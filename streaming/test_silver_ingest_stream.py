from datetime import datetime
import json
import os
from tempfile import TemporaryDirectory
from unittest.mock import patch

from pyspark.sql import Row, SparkSession
from pyspark.sql.types import (
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from silver_ingest import (
    assert_committed_delta_source,
    process_batch,
    start_event_silver_query,
)


RESOURCE_ID = "11111111-1111-4111-8111-111111111111"
HOLD_ID = "22222222-2222-4222-8222-222222222222"
EVENT_1 = "33333333-3333-4333-8333-333333333333"
EVENT_2 = "44444444-4444-4444-8444-444444444444"
EVENT_3 = "55555555-5555-4555-8555-555555555555"
KAFKA_TIME = datetime(2026, 9, 20, 12, 0)
BRONZE_SCHEMA = StructType(
    [
        StructField("key", StringType()),
        StructField("value", StringType()),
        StructField("topic", StringType()),
        StructField("partition", IntegerType()),
        StructField("offset", LongType()),
        StructField("timestamp", TimestampType()),
    ]
)


def spark_session():
    spark = (
        SparkSession.builder
        .master("local[2]")
        .appName("nexa-silver-ingest-stream-tests")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    return spark


def event(event_id, event_type, occurred_at, payload):
    return json.dumps(
        {
            "eventId": event_id,
            "eventType": event_type,
            "eventVersion": 1,
            "occurredAt": occurred_at,
            "resourceId": RESOURCE_ID,
            "holderRef": "customer-42",
            "payload": payload,
        }
    )


def bronze_df(spark, values, offsets=None):
    offsets = offsets or list(range(41, 41 + len(values)))
    return spark.createDataFrame(
        [
            Row(
                key=RESOURCE_ID,
                value=value,
                topic="reservation.events.v1",
                partition=2,
                offset=offset,
                timestamp=KAFKA_TIME,
            )
            for value, offset in zip(values, offsets)
        ],
        BRONZE_SCHEMA,
    )


def append_bronze(spark, path, values, mode="append", offsets=None):
    bronze_df(spark, values, offsets).write.format("delta").mode(mode).save(path)


def read_delta(spark, path):
    return spark.read.format("delta").load(path)


def test_stream_keeps_old_events_across_split_reverse_order_snapshot():
    spark = spark_session()
    with TemporaryDirectory() as root:
        bronze_path = f"{root}/bronze"
        silver_path = f"{root}/silver_events"
        quarantine_path = f"{root}/quarantine_events"
        checkpoint_path = f"{root}/checkpoint"
        try:
            append_bronze(
                spark,
                bronze_path,
                [
                    event(EVENT_1, "RESERVATION_CONFIRMED", "2000-01-01T00:00:00Z", {"holdId": HOLD_ID}),
                    event(EVENT_2, "RESERVATION_CONFIRMED", "2026-09-20T12:02:00Z", {"holdId": HOLD_ID}),
                ],
                mode="overwrite",
                offsets=[41, 42],
            )
            append_bronze(
                spark,
                bronze_path,
                [event(EVENT_3, "RESERVATION_CONFIRMED", "2026-09-20T12:01:00Z", {"holdId": HOLD_ID})],
                offsets=[43],
            )

            query = start_event_silver_query(
                spark,
                bronze_path,
                silver_path,
                quarantine_path,
                checkpoint_path,
                max_files_per_trigger=1,
            )
            try:
                query.processAllAvailable()
                assert query.exception() is None
            finally:
                query.stop()

            rows = read_delta(spark, silver_path).select("eventId").collect()
            assert {row.eventId for row in rows} == {EVENT_1, EVENT_2, EVENT_3}
        finally:
            spark.stop()


def test_empty_input_and_all_invalid_input_do_not_create_silver_events():
    spark = spark_session()
    with TemporaryDirectory() as root:
        bronze_path = f"{root}/bronze"
        silver_path = f"{root}/silver_events"
        quarantine_path = f"{root}/quarantine_events"
        checkpoint_path = f"{root}/checkpoint"
        try:
            spark.createDataFrame([], BRONZE_SCHEMA).write.format("delta").save(bronze_path)
            query = start_event_silver_query(
                spark, bronze_path, silver_path, quarantine_path, checkpoint_path
            )
            try:
                query.processAllAvailable()
            finally:
                query.stop()
            assert not os.path.exists(silver_path)

            append_bronze(spark, bronze_path, ["not-json"], offsets=[51])
            query = start_event_silver_query(
                spark, bronze_path, silver_path, quarantine_path, checkpoint_path + "-invalid"
            )
            try:
                query.processAllAvailable()
                assert query.exception() is None
            finally:
                query.stop()
            assert not os.path.exists(silver_path)
            assert read_delta(spark, quarantine_path).count() == 1
        finally:
            spark.stop()


def test_restart_with_same_checkpoint_deduplicates_against_persisted_silver():
    spark = spark_session()
    with TemporaryDirectory() as root:
        bronze_path = f"{root}/bronze"
        silver_path = f"{root}/silver_events"
        quarantine_path = f"{root}/quarantine_events"
        checkpoint_path = f"{root}/checkpoint"
        duplicate = event(EVENT_1, "RESERVATION_CONFIRMED", "2026-09-20T12:00:00Z", {"holdId": HOLD_ID})
        try:
            append_bronze(spark, bronze_path, [duplicate], mode="overwrite", offsets=[41])
            query = start_event_silver_query(
                spark, bronze_path, silver_path, quarantine_path, checkpoint_path
            )
            query.processAllAvailable()
            query.stop()

            append_bronze(
                spark,
                bronze_path,
                [
                    duplicate,
                    event(EVENT_2, "RESERVATION_RELEASED", "2026-09-20T12:01:00Z", {"holdId": HOLD_ID}),
                ],
                offsets=[42, 43],
            )
            query = start_event_silver_query(
                spark, bronze_path, silver_path, quarantine_path, checkpoint_path
            )
            try:
                query.processAllAvailable()
                assert query.exception() is None
            finally:
                query.stop()

            assert read_delta(spark, silver_path).count() == 2
            assert not os.path.exists(quarantine_path)
        finally:
            spark.stop()


def test_stream_deduplicates_equivalent_json_and_quarantines_conflicts():
    spark = spark_session()
    with TemporaryDirectory() as root:
        bronze_path = f"{root}/bronze"
        silver_path = f"{root}/silver_events"
        quarantine_path = f"{root}/quarantine_events"
        checkpoint_path = f"{root}/checkpoint"
        first = event(EVENT_1, "RESERVATION_CONFIRMED", "2026-09-20T12:00:00Z", {"holdId": HOLD_ID})
        reordered = (
            '{"payload":{"holdId":"'
            + HOLD_ID
            + '"},"holderRef":"customer-42","resourceId":"'
            + RESOURCE_ID
            + '","occurredAt":"2026-09-20T12:00:00Z","eventVersion":1,'
            '"eventType":"RESERVATION_CONFIRMED","eventId":"'
            + EVENT_1
            + '"}'
        )
        conflict = event(
            EVENT_1,
            "RESERVATION_CONFIRMED",
            "2026-09-20T12:00:00Z",
            {"holdId": "66666666-6666-4666-8666-666666666666"},
        )
        try:
            append_bronze(
                spark,
                bronze_path,
                [first, reordered, conflict],
                mode="overwrite",
                offsets=[41, 42, 43],
            )
            query = start_event_silver_query(
                spark, bronze_path, silver_path, quarantine_path, checkpoint_path
            )
            query.processAllAvailable()
            query.stop()

            assert read_delta(spark, silver_path).count() == 1
            assert read_delta(spark, quarantine_path).count() == 1

            append_bronze(spark, bronze_path, [conflict], offsets=[44])
            query = start_event_silver_query(
                spark, bronze_path, silver_path, quarantine_path, checkpoint_path
            )
            try:
                query.processAllAvailable()
                assert query.exception() is None
            finally:
                query.stop()

            assert read_delta(spark, silver_path).count() == 1
            assert read_delta(spark, quarantine_path).count() == 2
        finally:
            spark.stop()


def test_retry_after_failure_between_output_writes_converges():
    spark = spark_session()
    with TemporaryDirectory() as root:
        silver_path = f"{root}/silver_events"
        quarantine_path = f"{root}/quarantine_events"
        valid = event(EVENT_1, "RESERVATION_CONFIRMED", "2026-09-20T12:00:00Z", {"holdId": HOLD_ID})
        try:
            batch = bronze_df(spark, [valid, "not-json"], offsets=[41, 42])
            original_merge = __import__("silver_ingest").merge_insert_only
            calls = {"count": 0}

            def fail_once(*args, **kwargs):
                calls["count"] += 1
                if calls["count"] == 2:
                    raise RuntimeError("simulated quarantine failure")
                return original_merge(*args, **kwargs)

            with patch("silver_ingest.merge_insert_only", side_effect=fail_once):
                try:
                    process_batch(batch, 0, silver_path, quarantine_path)
                except RuntimeError as exc:
                    assert str(exc) == "simulated quarantine failure"
                else:
                    raise AssertionError("partial output failure was ignored")

            process_batch(batch, 0, silver_path, quarantine_path)

            assert read_delta(spark, silver_path).count() == 1
            assert read_delta(spark, quarantine_path).count() == 1
        finally:
            spark.stop()


def test_empty_delta_log_is_not_a_ready_bronze_source():
    spark = spark_session()
    with TemporaryDirectory() as root:
        bronze_path = f"{root}/bronze"
        os.makedirs(f"{bronze_path}/_delta_log")
        try:
            try:
                assert_committed_delta_source(spark, bronze_path)
            except RuntimeError as exc:
                assert "committed Delta" in str(exc)
            else:
                raise AssertionError("empty _delta_log was accepted as a source")
        finally:
            spark.stop()


if __name__ == "__main__":
    for name, test in sorted(globals().items()):
        if name.startswith("test_"):
            test()
    print("NEXA_SILVER_INGEST_STREAM_TESTS_OK")
