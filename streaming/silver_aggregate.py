import logging

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    approx_count_distinct,
    col,
    count,
    from_json,
    get_json_object,
    lit,
    trim,
    to_timestamp,
    window,
    when,
)
from pyspark.sql.types import (
    IntegerType,
    MapType,
    StringType,
    StructField,
    StructType,
)


BRONZE_PATH = "/opt/nexa/data/bronze"
SILVER_PATH = "/opt/nexa/data/silver"
SILVER_CHECKPOINT_PATH = "/opt/nexa/data/checkpoint/silver"
WINDOW_DURATION = "5 minutes"
WATERMARK_DELAY = "5 minutes"
APPROX_RSD = 0.05
SUPPORTED_EVENT_TYPES = (
    "RESERVATION_HOLD_CREATED",
    "RESERVATION_CONFIRMED",
    "RESERVATION_RELEASED",
    "RESERVATION_EXPIRED",
    "RESERVATION_REJECTED",
)

EVENT_SCHEMA = StructType(
    [
        StructField("eventId", StringType()),
        StructField("eventType", StringType()),
        StructField("eventVersion", IntegerType()),
        StructField("occurredAt", StringType()),
        StructField("resourceId", StringType()),
        StructField("holderRef", StringType()),
        StructField("payload", MapType(StringType(), StringType())),
    ]
)

logger = logging.getLogger("nexa.silver")


def _missing(column_name: str):
    value = trim(col(column_name))
    return col(column_name).isNull() | (value == "")


def parse_and_validate(bronze_df: DataFrame) -> DataFrame:
    parsed = (
        bronze_df
        .withColumn("_raw_json", get_json_object(col("value"), "$"))
        .withColumn("_event", from_json(col("value"), EVENT_SCHEMA))
    )
    event_type = col("_event.eventType")
    hold_id = col("_event.payload").getItem("holdId")
    occurred_at = to_timestamp(col("_event.occurredAt"))

    validation_error = (
        when(col("_raw_json").isNull(), lit("invalid_json"))
        .when(_missing("_event.eventId"), lit("missing_event_id"))
        .when(_missing("_event.eventType"), lit("missing_event_type"))
        .when(~event_type.isin(*SUPPORTED_EVENT_TYPES), lit("unsupported_event_type"))
        .when(
            col("_event.eventVersion").isNull() | (col("_event.eventVersion") != 1),
            lit("unsupported_event_version"),
        )
        .when(occurred_at.isNull(), lit("invalid_occurred_at"))
        .when(_missing("_event.resourceId"), lit("missing_resource_id"))
        .when(_missing("_event.holderRef"), lit("missing_holder_ref"))
        .when(col("_event.payload").isNull(), lit("missing_payload"))
        .when(
            (event_type != "RESERVATION_REJECTED")
            & (hold_id.isNull() | (trim(hold_id) == "")),
            lit("missing_hold_id"),
        )
    )

    return parsed.select(
        col("key"),
        col("value"),
        col("topic"),
        col("partition"),
        col("offset"),
        col("timestamp"),
        col("_event.eventId").alias("eventId"),
        event_type.alias("eventType"),
        col("_event.eventVersion").alias("eventVersion"),
        occurred_at.alias("occurredAt"),
        col("_event.resourceId").alias("resourceId"),
        col("_event.holderRef").alias("holderRef"),
        validation_error.alias("validation_error"),
        validation_error.isNull().alias("valid"),
    )


def aggregate_events(events_df: DataFrame, with_watermark: bool = True) -> DataFrame:
    valid_events = events_df.filter(col("valid"))
    if with_watermark:
        valid_events = valid_events.withWatermark("occurredAt", WATERMARK_DELAY)

    deduplicated = valid_events.dropDuplicates(["eventId"])
    return (
        deduplicated.groupBy(window("occurredAt", WINDOW_DURATION), "eventType")
        .agg(
            count(lit(1)).cast("long").alias("event_count"),
            approx_count_distinct("holderRef", APPROX_RSD)
            .cast("long")
            .alias("distinct_users"),
        )
        .select(
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            col("eventType").alias("event_type"),
            col("event_count"),
            col("distinct_users"),
        )
    )


def main() -> None:
    spark = (
        SparkSession.builder
        .appName("nexa-silver-aggregate")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    silver_query = None
    invalid_query = None
    try:
        logger.warning(
            "Starting Silver aggregation bronze=%s silver=%s checkpoint=%s",
            BRONZE_PATH,
            SILVER_PATH,
            SILVER_CHECKPOINT_PATH,
        )

        bronze_stream = (
            spark.readStream
            .format("delta")
            .option("withEventTimeOrder", "true")
            .load(BRONZE_PATH)
        )
        parsed = parse_and_validate(bronze_stream)

        invalid_query = (
            parsed.filter(~col("valid"))
            .select("eventId", "validation_error", "value")
            .writeStream
            .format("console")
            .outputMode("append")
            .option("truncate", "false")
            .start()
        )

        silver_query = (
            aggregate_events(parsed)
            .writeStream
            .format("delta")
            .outputMode("append")
            .option("checkpointLocation", SILVER_CHECKPOINT_PATH)
            .start(SILVER_PATH)
        )
        silver_query.awaitTermination()
    except Exception:
        logger.exception("Silver aggregation stopped with an error")
        raise
    finally:
        for query in (silver_query, invalid_query):
            if query is not None:
                query.stop()
        spark.stop()


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    main()
