import json
import logging
import re
import time
import uuid
from datetime import datetime, timedelta, timezone


SUPPORTED_EVENT_TYPES = (
    "RESERVATION_HOLD_CREATED",
    "RESERVATION_CONFIRMED",
    "RESERVATION_RELEASED",
    "RESERVATION_EXPIRED",
    "RESERVATION_REJECTED",
)
BRONZE_PATH = "/opt/nexa/data/bronze"
EVENT_SILVER_PATH = "/opt/nexa/data/silver_events"
QUARANTINE_PATH = "/opt/nexa/data/quarantine_events"
EVENT_SILVER_CHECKPOINT_PATH = "/opt/nexa/data/checkpoint/silver_events"
MAX_OCCURRED_AT_AHEAD = timedelta(minutes=5)
JAVA_INT_MIN = -(2**31)
JAVA_INT_MAX = 2**31 - 1
UTC_TIMESTAMP = re.compile(
    r"^(?P<prefix>\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2})"
    r"(?P<fraction>\.\d{1,9})?(?P<zone>Z|[+-]\d{2}:\d{2})$"
)
UTC_TIMESTAMP_WITHOUT_ZONE = re.compile(
    r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d{1,9})?$"
)
UTC = timezone.utc
logger = logging.getLogger("nexa.silver_events")

SILVER_COLUMNS = (
    "eventId",
    "eventType",
    "eventVersion",
    "occurredAt",
    "resourceId",
    "holderRef",
    "holdId",
    "expiresAt",
    "scheduledExpiresAt",
    "reason",
    "requestedUnits",
    "availableUnits",
    "canonical_json",
    "key",
    "topic",
    "partition",
    "offset",
    "timestamp",
)
QUARANTINE_COLUMNS = (
    "key",
    "value",
    "topic",
    "partition",
    "offset",
    "timestamp",
    "eventId",
    "quarantine_key",
    "quarantine_reason",
)


def _empty_result(record):
    return {
        "key": record.get("key"),
        "value": record.get("value"),
        "topic": record.get("topic"),
        "partition": record.get("partition"),
        "offset": record.get("offset"),
        "timestamp": record.get("timestamp"),
        "eventId": None,
        "eventType": None,
        "eventVersion": None,
        "occurredAt": None,
        "resourceId": None,
        "holderRef": None,
        "holdId": None,
        "expiresAt": None,
        "scheduledExpiresAt": None,
        "reason": None,
        "requestedUnits": None,
        "availableUnits": None,
        "canonical_json": None,
        "valid": False,
        "validation_error": None,
    }


def _invalid(result, reason):
    result["validation_error"] = reason
    return result


def _is_uuid(value):
    if not isinstance(value, str):
        return False
    try:
        uuid.UUID(value)
    except (ValueError, AttributeError):
        return False
    return True


def _canonical_uuid(value):
    return str(uuid.UUID(value))


def _parse_utc(value, field_name):
    if not isinstance(value, str):
        return (None, None, None), f"invalid_{field_name}_type"
    match = UTC_TIMESTAMP.fullmatch(value)
    if match is None:
        if UTC_TIMESTAMP_WITHOUT_ZONE.fullmatch(value):
            return (None, None, None), f"{field_name}_not_utc"
        return (None, None, None), f"invalid_{field_name}"
    fraction_digits = (match.group("fraction") or "")[1:]
    fraction_nanoseconds = int(fraction_digits.ljust(9, "0") or "0")
    microseconds = fraction_digits[:6].ljust(6, "0")
    zone = match.group("zone")
    zone_for_python = "+00:00" if zone == "Z" else zone
    python_value = match.group("prefix")
    if fraction_digits:
        python_value += f".{microseconds}"
    python_value += zone_for_python
    try:
        parsed = datetime.fromisoformat(python_value)
    except ValueError:
        return (None, None, None), f"invalid_{field_name}"
    if parsed.tzinfo is None or parsed.utcoffset() != timedelta(0):
        return (None, None, None), f"{field_name}_not_utc"
    parsed = parsed.astimezone(UTC)
    if fraction_nanoseconds % 1000 == 0:
        canonical = parsed.isoformat().replace("+00:00", "Z")
    else:
        canonical = parsed.strftime("%Y-%m-%dT%H:%M:%S")
        canonical += f".{fraction_nanoseconds:09d}Z"
    return (parsed, canonical, fraction_nanoseconds % 1000), None


def _timestamp_after(left, left_extra_nanoseconds, right, right_extra_nanoseconds):
    return (left, left_extra_nanoseconds) > (right, right_extra_nanoseconds)


def _source_timestamp(value):
    if not isinstance(value, datetime):
        return None
    if value.tzinfo is None:
        return value.replace(tzinfo=UTC)
    return value.astimezone(UTC)


def _required_uuid(payload, name):
    if name not in payload or payload[name] is None:
        return None, f"missing_{name}"
    if not isinstance(payload[name], str):
        return None, f"invalid_{name}_type"
    if not _is_uuid(payload[name]):
        return None, f"invalid_{name}"
    return _canonical_uuid(payload[name]), None


def _canonical_payload(event_type, typed, canonical_timestamps):
    fields = {
        "RESERVATION_HOLD_CREATED": ("holdId", "expiresAt"),
        "RESERVATION_CONFIRMED": ("holdId",),
        "RESERVATION_RELEASED": ("holdId",),
        "RESERVATION_EXPIRED": ("holdId", "scheduledExpiresAt"),
        "RESERVATION_REJECTED": ("reason", "requestedUnits", "availableUnits"),
    }[event_type]
    canonical = {}
    for field in fields:
        value = canonical_timestamps.get(field, typed[field])
        if isinstance(value, datetime):
            value = value.isoformat().replace("+00:00", "Z")
        canonical[field] = value
    return canonical


def validate_publication(record):
    """Validate one Bronze-shaped record without consulting the current clock."""
    result = _empty_result(record)
    raw_value = record.get("value")
    if not isinstance(raw_value, str):
        return _invalid(result, "invalid_raw_value_type")

    try:
        event = json.loads(raw_value, parse_constant=lambda value: (_ for _ in ()).throw(ValueError(value)))
    except (TypeError, ValueError, json.JSONDecodeError):
        return _invalid(result, "invalid_json")
    if not isinstance(event, dict):
        return _invalid(result, "json_not_object")

    event_id = event.get("eventId")
    if "eventId" not in event:
        return _invalid(result, "missing_event_id")
    if not isinstance(event_id, str):
        return _invalid(result, "invalid_event_id_type")
    if not _is_uuid(event_id):
        return _invalid(result, "invalid_event_id")

    event_type = event.get("eventType")
    if "eventType" not in event:
        return _invalid(result, "missing_event_type")
    if not isinstance(event_type, str):
        return _invalid(result, "invalid_event_type_type")
    if event_type not in SUPPORTED_EVENT_TYPES:
        return _invalid(result, "unsupported_event_type")

    event_version = event.get("eventVersion")
    if "eventVersion" not in event:
        return _invalid(result, "missing_event_version")
    if isinstance(event_version, bool) or not isinstance(event_version, int):
        return _invalid(result, "invalid_event_version_type")
    if event_version != 1:
        return _invalid(result, "unsupported_event_version")

    if "occurredAt" not in event:
        return _invalid(result, "missing_occurred_at")
    (occurred_at, occurred_at_canonical, occurred_at_extra_nanoseconds), error = _parse_utc(
        event["occurredAt"], "occurred_at"
    )
    if error:
        return _invalid(result, error)

    resource_id = event.get("resourceId")
    if "resourceId" not in event:
        return _invalid(result, "missing_resource_id")
    if not isinstance(resource_id, str):
        return _invalid(result, "invalid_resource_id_type")
    if not _is_uuid(resource_id):
        return _invalid(result, "invalid_resource_id")
    resource_id = _canonical_uuid(resource_id)

    key = record.get("key")
    if not isinstance(key, str) or not _is_uuid(key) or _canonical_uuid(key) != resource_id:
        return _invalid(result, "resource_key_mismatch")

    kafka_timestamp = _source_timestamp(record.get("timestamp"))
    if kafka_timestamp is None:
        return _invalid(result, "missing_kafka_timestamp")
    if _timestamp_after(
        occurred_at,
        occurred_at_extra_nanoseconds,
        kafka_timestamp + MAX_OCCURRED_AT_AHEAD,
        0,
    ):
        return _invalid(result, "occurred_at_after_kafka_timestamp")

    holder_ref = event.get("holderRef")
    if "holderRef" not in event:
        return _invalid(result, "missing_holder_ref")
    if not isinstance(holder_ref, str):
        return _invalid(result, "invalid_holder_ref_type")
    if not holder_ref.strip():
        return _invalid(result, "empty_holder_ref")

    payload = event.get("payload")
    if "payload" not in event:
        return _invalid(result, "missing_payload")
    if not isinstance(payload, dict):
        return _invalid(result, "invalid_payload_type")

    typed = {
        "holdId": None,
        "expiresAt": None,
        "scheduledExpiresAt": None,
        "reason": None,
        "requestedUnits": None,
        "availableUnits": None,
    }
    canonical_timestamps = {}

    if event_type != "RESERVATION_REJECTED":
        typed["holdId"], error = _required_uuid(payload, "holdId")
        if error:
            return _invalid(result, error)

    if event_type == "RESERVATION_HOLD_CREATED":
        if "expiresAt" not in payload:
            return _invalid(result, "missing_expires_at")
        (
            typed["expiresAt"],
            canonical_timestamps["expiresAt"],
            _,
        ), error = _parse_utc(payload["expiresAt"], "expires_at")
        if error:
            return _invalid(result, error)
    elif event_type == "RESERVATION_EXPIRED":
        if "scheduledExpiresAt" not in payload:
            return _invalid(result, "missing_scheduled_expires_at")
        (
            typed["scheduledExpiresAt"],
            canonical_timestamps["scheduledExpiresAt"],
            _,
        ), error = _parse_utc(
            payload["scheduledExpiresAt"], "scheduled_expires_at"
        )
        if error:
            return _invalid(result, error)
    elif event_type == "RESERVATION_REJECTED":
        if payload.get("reason") != "INSUFFICIENT_AVAILABILITY":
            return _invalid(result, "invalid_rejection_reason")
        requested_units = payload.get("requestedUnits")
        if "requestedUnits" not in payload:
            return _invalid(result, "missing_requested_units")
        if isinstance(requested_units, bool) or not isinstance(requested_units, int):
            return _invalid(result, "invalid_requested_units_type")
        if requested_units != 1:
            return _invalid(result, "invalid_requested_units")
        available_units = payload.get("availableUnits")
        if "availableUnits" not in payload:
            return _invalid(result, "missing_available_units")
        if isinstance(available_units, bool) or not isinstance(available_units, int):
            return _invalid(result, "invalid_available_units_type")
        if not JAVA_INT_MIN <= available_units <= JAVA_INT_MAX:
            return _invalid(result, "available_units_out_of_range")
        if available_units > 0:
            return _invalid(result, "invalid_available_units")
        typed["reason"] = payload["reason"]
        typed["requestedUnits"] = requested_units
        typed["availableUnits"] = available_units

    result.update(
        {
            "eventId": _canonical_uuid(event_id),
            "eventType": event_type,
            "eventVersion": event_version,
            "occurredAt": occurred_at,
            "resourceId": resource_id,
            "holderRef": holder_ref,
            **typed,
            "valid": True,
            "validation_error": None,
        }
    )
    canonical_content = {
        "eventId": result["eventId"],
        "eventType": event_type,
        "eventVersion": event_version,
        "occurredAt": occurred_at_canonical,
        "resourceId": resource_id,
        "holderRef": holder_ref,
        "payload": _canonical_payload(event_type, typed, canonical_timestamps),
    }
    result["canonical_json"] = json.dumps(
        canonical_content, sort_keys=True, separators=(",", ":"), ensure_ascii=False
    )
    return result


def _validation_schema():
    from pyspark.sql.types import (
        BooleanType,
        IntegerType,
        LongType,
        StringType,
        StructField,
        StructType,
        TimestampType,
    )

    return StructType(
        [
            StructField("key", StringType()),
            StructField("value", StringType()),
            StructField("topic", StringType()),
            StructField("partition", IntegerType()),
            StructField("offset", LongType()),
            StructField("timestamp", TimestampType()),
            StructField("eventId", StringType()),
            StructField("eventType", StringType()),
            StructField("eventVersion", IntegerType()),
            StructField("occurredAt", TimestampType()),
            StructField("resourceId", StringType()),
            StructField("holderRef", StringType()),
            StructField("holdId", StringType()),
            StructField("expiresAt", TimestampType()),
            StructField("scheduledExpiresAt", TimestampType()),
            StructField("reason", StringType()),
            StructField("requestedUnits", LongType()),
            StructField("availableUnits", LongType()),
            StructField("canonical_json", StringType()),
            StructField("valid", BooleanType()),
            StructField("validation_error", StringType()),
        ]
    )


def validate_bronze_df(bronze_df):
    from pyspark.sql.functions import col, udf

    @udf(_validation_schema())
    def validate_row(key, value, topic, partition, offset, timestamp):
        return validate_publication(
            {
                "key": key,
                "value": value,
                "topic": topic,
                "partition": partition,
                "offset": offset,
                "timestamp": timestamp,
            }
        )

    return bronze_df.select(
        validate_row(
            col("key"),
            col("value"),
            col("topic"),
            col("partition"),
            col("offset"),
            col("timestamp"),
        ).alias("validated")
    ).select("validated.*")


def assert_committed_delta_source(spark, path):
    from delta.tables import DeltaTable

    if not DeltaTable.isDeltaTable(spark, path):
        raise RuntimeError(f"No committed Delta source exists at {path}")


def _delta_table_exists(spark, path):
    from delta.tables import DeltaTable

    return DeltaTable.isDeltaTable(spark, path)


def _quarantine_columns(df, reason_column):
    from pyspark.sql.functions import col, concat_ws

    return df.select(
        *[
            col(column)
            for column in ("key", "value", "topic", "partition", "offset", "timestamp", "eventId")
        ],
        concat_ws(
            ":",
            col("topic"),
            col("partition").cast("string"),
            col("offset").cast("string"),
        ).alias("quarantine_key"),
        col(reason_column).alias("quarantine_reason"),
    ).select(*QUARANTINE_COLUMNS)


def _silver_columns(df):
    return df.select(*SILVER_COLUMNS)


def merge_insert_only(source_df, path, key_columns):
    from delta.tables import DeltaTable

    if source_df.isEmpty():
        return
    if not _delta_table_exists(source_df.sparkSession, path):
        source_df.limit(0).write.format("delta").mode("append").save(path)
    target = DeltaTable.forPath(source_df.sparkSession, path)
    condition = " AND ".join(
        f"target.`{column}` = source.`{column}`" for column in key_columns
    )
    (
        target.alias("target")
        .merge(source_df.alias("source"), condition)
        .whenNotMatchedInsertAll()
        .execute()
    )


def process_batch(batch_df, batch_id, silver_path, quarantine_path):
    from pyspark.sql import Window
    from pyspark.sql.functions import (
        col,
        lit,
        row_number,
    )

    validated = validate_bronze_df(batch_df).cache()
    silver_output = None
    quarantine_output = None
    try:
        invalid = validated.filter(~col("valid"))
        invalid_quarantine = _quarantine_columns(invalid, "validation_error")
        valid = validated.filter(col("valid"))

        if _delta_table_exists(batch_df.sparkSession, silver_path):
            existing = (
                batch_df.sparkSession.read.format("delta")
                .load(silver_path)
                .select(
                    col("eventId").alias("_existing_event_id"),
                    col("canonical_json").alias("_existing_content"),
                )
            )
            joined = valid.join(
                existing,
                valid.eventId == existing._existing_event_id,
                "left",
            )
            persisted_conflicts = joined.filter(
                col("_existing_event_id").isNotNull()
                & (col("canonical_json") != col("_existing_content"))
            )
            unseen = joined.filter(col("_existing_event_id").isNull()).select(
                *valid.columns
            )
        else:
            persisted_conflicts = valid.limit(0)
            unseen = valid

        ordering = [
            col("timestamp").asc_nulls_first(),
            col("topic").asc(),
            col("partition").asc(),
            col("offset").asc(),
        ]
        ranked = unseen.withColumn(
            "_candidate_rank",
            row_number().over(Window.partitionBy("eventId").orderBy(*ordering)),
        )
        winners = ranked.filter(col("_candidate_rank") == 1).drop("_candidate_rank")
        winner_content = winners.select(
            col("eventId").alias("_winner_event_id"),
            col("canonical_json").alias("_winner_content"),
        )
        batch_conflicts = (
            unseen.join(
                winner_content,
                unseen.eventId == winner_content._winner_event_id,
            )
            .filter(col("canonical_json") != col("_winner_content"))
            .select(*unseen.columns)
        )

        silver_output = _silver_columns(winners)
        persisted_conflict_output = _quarantine_columns(
            persisted_conflicts.withColumn(
                "_quarantine_reason", lit("conflicting_event_id")
            ),
            "_quarantine_reason",
        )
        batch_conflict_output = _quarantine_columns(
            batch_conflicts.withColumn("_quarantine_reason", lit("conflicting_event_id")),
            "_quarantine_reason",
        )
        quarantine_output = (
            invalid_quarantine
            .unionByName(persisted_conflict_output)
            .unionByName(batch_conflict_output)
            .dropDuplicates(["quarantine_key"])
        )

        # Materialize both classifications before either table changes. This
        # keeps a partial write retry based on one stable micro-batch view.
        silver_output.cache()
        quarantine_output.cache()
        silver_output.count()
        quarantine_output.count()
        merge_insert_only(silver_output, silver_path, ("eventId",))
        merge_insert_only(quarantine_output, quarantine_path, ("quarantine_key",))
    finally:
        if silver_output is not None:
            silver_output.unpersist()
        if quarantine_output is not None:
            quarantine_output.unpersist()
        validated.unpersist()


def start_event_silver_query(
    spark,
    bronze_path=BRONZE_PATH,
    silver_path=EVENT_SILVER_PATH,
    quarantine_path=QUARANTINE_PATH,
    checkpoint_path=EVENT_SILVER_CHECKPOINT_PATH,
    max_files_per_trigger=None,
):
    assert_committed_delta_source(spark, bronze_path)
    bronze_stream = spark.readStream.format("delta")
    if max_files_per_trigger is not None:
        bronze_stream = bronze_stream.option("maxFilesPerTrigger", max_files_per_trigger)
    source = bronze_stream.load(bronze_path)
    return (
        source.writeStream
        .foreachBatch(
            lambda batch_df, batch_id: process_batch(
                batch_df, batch_id, silver_path, quarantine_path
            )
        )
        .option("checkpointLocation", checkpoint_path)
        .start()
    )


def main():
    from pyspark.sql import SparkSession

    spark = (
        SparkSession.builder
        .appName("nexa-silver-events")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    query = None
    try:
        while not _delta_table_exists(spark, BRONZE_PATH):
            logger.warning("Waiting for a committed Bronze Delta table at %s", BRONZE_PATH)
            time.sleep(1)
        query = start_event_silver_query(spark)
        query.awaitTermination()
    except Exception:
        logger.exception("Event-level Silver stopped with an error")
        raise
    finally:
        if query is not None:
            query.stop()
        spark.stop()


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    main()
