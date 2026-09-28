from datetime import datetime
import json
import os
from tempfile import TemporaryDirectory
from unittest.mock import patch

from pyspark.sql import Row, SparkSession
from pyspark.sql.functions import col, lit, when
from pyspark.sql.types import (
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from silver_ingest import (
    SILVER_COLUMNS,
    assert_committed_delta_source,
    process_batch,
    start_event_silver_query,
    validate_bronze_df,
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
                [event(EVENT_2, "RESERVATION_CONFIRMED", "2026-09-20T12:02:00Z", {"holdId": HOLD_ID})],
                mode="overwrite",
                offsets=[42],
            )
            append_bronze(
                spark,
                bronze_path,
                [event(EVENT_1, "RESERVATION_CONFIRMED", "2000-01-01T00:00:00Z", {"holdId": HOLD_ID})],
                offsets=[41],
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


def test_legacy_canonical_values_survive_h4_upgrade_and_restart():
    spark = spark_session()
    with TemporaryDirectory() as root:
        bronze_path = f"{root}/bronze"
        silver_path = f"{root}/silver_events"
        quarantine_path = f"{root}/quarantine_events"
        checkpoint_path = f"{root}/checkpoint"
        legacy_values = [
            event(
                EVENT_1,
                "RESERVATION_CONFIRMED",
                "2026-09-20T12:00:00Z",
                {"holdId": HOLD_ID},
            ),
            event(
                EVENT_2,
                "RESERVATION_HOLD_CREATED",
                "2026-09-20T12:00:00.123Z",
                {
                    "holdId": HOLD_ID,
                    "expiresAt": "2026-09-20T12:15:00.123Z",
                },
            ),
            event(
                EVENT_3,
                "RESERVATION_EXPIRED",
                "2026-09-20T12:00:00.123456Z",
                {
                    "holdId": HOLD_ID,
                    "scheduledExpiresAt": "2026-09-20T12:15:00.123456Z",
                },
            ),
        ]
        legacy_canonical = {
            EVENT_1: (
                '{"eventId":"33333333-3333-4333-8333-333333333333",'
                '"eventType":"RESERVATION_CONFIRMED","eventVersion":1,'
                '"holderRef":"customer-42","occurredAt":"2026-09-20T12:00:00Z",'
                '"payload":{"holdId":"22222222-2222-4222-8222-222222222222"},'
                '"resourceId":"11111111-1111-4111-8111-111111111111"}'
            ),
            EVENT_2: (
                '{"eventId":"44444444-4444-4444-8444-444444444444",'
                '"eventType":"RESERVATION_HOLD_CREATED","eventVersion":1,'
                '"holderRef":"customer-42","occurredAt":"2026-09-20T12:00:00.123000Z",'
                '"payload":{"expiresAt":"2026-09-20T12:15:00.123000Z",'
                '"holdId":"22222222-2222-4222-8222-222222222222"},'
                '"resourceId":"11111111-1111-4111-8111-111111111111"}'
            ),
            EVENT_3: (
                '{"eventId":"55555555-5555-4555-8555-555555555555",'
                '"eventType":"RESERVATION_EXPIRED","eventVersion":1,'
                '"holderRef":"customer-42","occurredAt":"2026-09-20T12:00:00.123456Z",'
                '"payload":{"holdId":"22222222-2222-4222-8222-222222222222",'
                '"scheduledExpiresAt":"2026-09-20T12:15:00.123456Z"},'
                '"resourceId":"11111111-1111-4111-8111-111111111111"}'
            ),
        }
        equivalent_values = [
            event(
                EVENT_1,
                "RESERVATION_CONFIRMED",
                "2026-09-20T12:00:00.000000000Z",
                {"holdId": HOLD_ID},
            ),
            event(
                EVENT_2,
                "RESERVATION_HOLD_CREATED",
                "2026-09-20T12:00:00.123000000Z",
                {
                    "holdId": HOLD_ID,
                    "expiresAt": "2026-09-20T12:15:00.123000000Z",
                },
            ),
            event(
                EVENT_3,
                "RESERVATION_EXPIRED",
                "2026-09-20T12:00:00.123456000Z",
                {
                    "holdId": HOLD_ID,
                    "scheduledExpiresAt": "2026-09-20T12:15:00.123456000Z",
                },
            ),
        ]
        genuine_nanosecond_conflict = event(
            EVENT_2,
            "RESERVATION_HOLD_CREATED",
            "2026-09-20T12:00:00.123000001Z",
            {
                "holdId": HOLD_ID,
                "expiresAt": "2026-09-20T12:15:00.123000001Z",
            },
        )
        try:
            seeded = validate_bronze_df(
                bronze_df(spark, legacy_values, offsets=[501, 502, 503])
            )
            seeded = seeded.withColumn(
                "canonical_json",
                when(
                    col("eventId") == EVENT_1, lit(legacy_canonical[EVENT_1])
                ).when(
                    col("eventId") == EVENT_2, lit(legacy_canonical[EVENT_2])
                ).when(
                    col("eventId") == EVENT_3, lit(legacy_canonical[EVENT_3])
                ),
            )
            seeded.select(*SILVER_COLUMNS).write.format("delta").save(silver_path)
            before = (
                read_delta(spark, silver_path)
                .select(*SILVER_COLUMNS)
                .orderBy("offset")
                .collect()
            )

            append_bronze(
                spark,
                bronze_path,
                equivalent_values + [genuine_nanosecond_conflict],
                mode="overwrite",
                offsets=[601, 602, 603, 604],
            )
            query = start_event_silver_query(
                spark, bronze_path, silver_path, quarantine_path, checkpoint_path
            )
            try:
                query.processAllAvailable()
                assert query.exception() is None
            finally:
                query.stop()

            after_first_run = (
                read_delta(spark, silver_path)
                .select(*SILVER_COLUMNS)
                .orderBy("offset")
                .collect()
            )
            assert after_first_run == before
            assert [row.canonical_json for row in after_first_run] == [
                legacy_canonical[EVENT_1],
                legacy_canonical[EVENT_2],
                legacy_canonical[EVENT_3],
            ]
            quarantine = read_delta(spark, quarantine_path).select(
                "offset", "quarantine_reason"
            ).collect()
            assert [(row.offset, row.quarantine_reason) for row in quarantine] == [
                (604, "conflicting_event_id")
            ]

            process_batch(
                bronze_df(
                    spark,
                    equivalent_values + [genuine_nanosecond_conflict],
                    offsets=[601, 602, 603, 604],
                ),
                1,
                silver_path,
                quarantine_path,
            )
            query = start_event_silver_query(
                spark, bronze_path, silver_path, quarantine_path, checkpoint_path
            )
            try:
                query.processAllAvailable()
                assert query.exception() is None
            finally:
                query.stop()

            assert (
                read_delta(spark, silver_path)
                .select(*SILVER_COLUMNS)
                .orderBy("offset")
                .collect()
                == before
            )
            assert read_delta(spark, quarantine_path).count() == 1
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


def test_spark_validation_preserves_microseconds_and_integer_boundaries():
    spark = spark_session()
    with TemporaryDirectory() as root:
        try:
            values = [
                event(
                    EVENT_1,
                    "RESERVATION_CONFIRMED",
                    "2026-09-20T12:00:00.123456789Z",
                    {"holdId": HOLD_ID},
                ),
                event(
                    EVENT_2,
                    "RESERVATION_REJECTED",
                    "2026-09-20T12:00:00Z",
                    {
                        "reason": "INSUFFICIENT_AVAILABILITY",
                        "requestedUnits": 1,
                        "availableUnits": -(2**31),
                    },
                ),
            ]
            rows = (
                validate_bronze_df(bronze_df(spark, values))
                .orderBy("offset")
                .collect()
            )

            assert rows[0].valid is True
            assert rows[0].occurredAt == datetime(2026, 9, 20, 12, 0, 0, 123456)
            assert ".123456789Z" in rows[0].canonical_json
            assert rows[1].valid is True
            assert rows[1].availableUnits == -(2**31)
        finally:
            spark.stop()


def test_spark_validation_rejects_oversized_integer_before_serialization():
    spark = spark_session()
    with TemporaryDirectory() as root:
        try:
            value = event(
                EVENT_1,
                "RESERVATION_REJECTED",
                "2026-09-20T12:00:00Z",
                {
                    "reason": "INSUFFICIENT_AVAILABILITY",
                    "requestedUnits": 1,
                    "availableUnits": -(2**70),
                },
            )
            row = validate_bronze_df(bronze_df(spark, [value])).first()

            assert row.valid is False
            assert row.availableUnits is None
            assert row.validation_error == "available_units_out_of_range"
        finally:
            spark.stop()


def test_oversized_integer_is_quarantined_with_raw_input():
    spark = spark_session()
    with TemporaryDirectory() as root:
        silver_path = f"{root}/silver_events"
        quarantine_path = f"{root}/quarantine_events"
        value = event(
            EVENT_1,
            "RESERVATION_REJECTED",
            "2026-09-20T12:00:00Z",
            {
                "reason": "INSUFFICIENT_AVAILABILITY",
                "requestedUnits": 1,
                "availableUnits": -(2**70),
            },
        )
        try:
            process_batch(
                bronze_df(spark, [value], offsets=[71]),
                0,
                silver_path,
                quarantine_path,
            )

            assert not os.path.exists(silver_path)
            quarantine = read_delta(spark, quarantine_path).collect()
            assert len(quarantine) == 1
            assert quarantine[0].value == value
            assert quarantine[0].offset == 71
            assert quarantine[0].quarantine_reason == "available_units_out_of_range"
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


def test_deterministic_winner_keeps_exact_content_and_coordinates_after_repartition():
    spark = spark_session()
    with TemporaryDirectory() as root:
        silver_path = f"{root}/silver_events"
        quarantine_path = f"{root}/quarantine_events"
        winner = event(
            EVENT_1,
            "RESERVATION_CONFIRMED",
            "2026-09-20T12:00:00Z",
            {"holdId": HOLD_ID},
        )
        equivalent = winner
        conflict = event(
            EVENT_1,
            "RESERVATION_CONFIRMED",
            "2026-09-20T12:00:00Z",
            {"holdId": "66666666-6666-4666-8666-666666666666"},
        )
        try:
            batch = bronze_df(
                spark,
                [conflict, equivalent, winner],
                offsets=[105, 103, 101],
            ).repartition(2)
            process_batch(batch, 0, silver_path, quarantine_path)

            silver = read_delta(spark, silver_path).select(
                "eventType", "holdId", "canonical_json", "offset"
            ).collect()
            quarantine = read_delta(spark, quarantine_path).select("offset", "quarantine_reason").collect()
            assert [(row.eventType, row.holdId, row.offset) for row in silver] == [
                ("RESERVATION_CONFIRMED", HOLD_ID, 101)
            ]
            assert HOLD_ID in silver[0].canonical_json
            assert [(row.offset, row.quarantine_reason) for row in quarantine] == [
                (105, "conflicting_event_id")
            ]
        finally:
            spark.stop()


def test_stream_failure_after_silver_commits_replays_and_reclassifies_conflict():
    spark = spark_session()
    with TemporaryDirectory() as root:
        bronze_path = f"{root}/bronze"
        silver_path = f"{root}/silver_events"
        quarantine_path = f"{root}/quarantine_events"
        checkpoint_path = f"{root}/checkpoint"
        accepted = event(
            EVENT_1,
            "RESERVATION_CONFIRMED",
            "2026-09-20T12:00:00Z",
            {"holdId": HOLD_ID},
        )
        equivalent = accepted
        conflict = event(
            EVENT_1,
            "RESERVATION_CONFIRMED",
            "2026-09-20T12:00:00Z",
            {"holdId": "66666666-6666-4666-8666-666666666666"},
        )
        invalid = "not-json"
        try:
            append_bronze(
                spark,
                bronze_path,
                [accepted, equivalent, conflict, invalid],
                mode="overwrite",
                offsets=[81, 82, 83, 84],
            )
            module = __import__("silver_ingest")
            original_merge = module.merge_insert_only
            injected = {"failed": False}

            def fail_once(*args, **kwargs):
                if args[2] == ("eventId",):
                    return original_merge(*args, **kwargs)
                if not injected["failed"]:
                    injected["failed"] = True
                    raise RuntimeError("simulated quarantine failure")
                return original_merge(*args, **kwargs)

            with patch("silver_ingest.merge_insert_only", side_effect=fail_once):
                query = start_event_silver_query(
                    spark,
                    bronze_path,
                    silver_path,
                    quarantine_path,
                    checkpoint_path,
                )
                try:
                    try:
                        query.processAllAvailable()
                    except Exception as exc:
                        assert "simulated quarantine failure" in str(exc)
                    assert query.exception() is not None
                finally:
                    query.stop()

            silver_after_failure = read_delta(spark, silver_path).collect()
            assert len(silver_after_failure) == 1
            accepted_row = silver_after_failure[0]
            assert accepted_row.eventId == EVENT_1
            assert accepted_row.eventType == "RESERVATION_CONFIRMED"
            assert accepted_row.eventVersion == 1
            assert accepted_row.occurredAt == datetime(2026, 9, 20, 12, 0)
            assert accepted_row.resourceId == RESOURCE_ID
            assert accepted_row.holderRef == "customer-42"
            assert accepted_row.holdId == HOLD_ID
            assert accepted_row.canonical_json == json.dumps(
                {
                    "eventId": EVENT_1,
                    "eventType": "RESERVATION_CONFIRMED",
                    "eventVersion": 1,
                    "occurredAt": "2026-09-20T12:00:00Z",
                    "resourceId": RESOURCE_ID,
                    "holderRef": "customer-42",
                    "payload": {"holdId": HOLD_ID},
                },
                sort_keys=True,
                separators=(",", ":"),
            )
            assert accepted_row.key == RESOURCE_ID
            assert accepted_row.topic == "reservation.events.v1"
            assert accepted_row.partition == 2
            assert accepted_row.offset == 81
            assert accepted_row.timestamp == KAFKA_TIME
            assert not os.path.exists(quarantine_path)

            query = start_event_silver_query(
                spark,
                bronze_path,
                silver_path,
                quarantine_path,
                checkpoint_path,
            )
            try:
                query.processAllAvailable()
                assert query.exception() is None
            finally:
                query.stop()

            quarantine = read_delta(spark, quarantine_path).select(
                "value", "offset", "quarantine_reason"
            ).collect()
            assert {
                (row.value, row.offset, row.quarantine_reason) for row in quarantine
            } == {
                (conflict, 83, "conflicting_event_id"),
                (invalid, 84, "invalid_json"),
            }

            query = start_event_silver_query(
                spark,
                bronze_path,
                silver_path,
                quarantine_path,
                checkpoint_path,
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


def test_stream_replay_after_both_writes_before_batch_return_is_idempotent():
    spark = spark_session()
    with TemporaryDirectory() as root:
        bronze_path = f"{root}/bronze"
        silver_path = f"{root}/silver_events"
        quarantine_path = f"{root}/quarantine_events"
        checkpoint_path = f"{root}/checkpoint"
        accepted = event(
            EVENT_1,
            "RESERVATION_CONFIRMED",
            "2026-09-20T12:00:00Z",
            {"holdId": HOLD_ID},
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
                [accepted, conflict, "not-json"],
                mode="overwrite",
                offsets=[91, 92, 93],
            )
            module = __import__("silver_ingest")
            original_process = module.process_batch
            injected = {"failed": False}

            def fail_after_both_writes(*args, **kwargs):
                result = original_process(*args, **kwargs)
                if not injected["failed"]:
                    injected["failed"] = True
                    raise RuntimeError("simulated post-write failure")
                return result

            with patch("silver_ingest.process_batch", side_effect=fail_after_both_writes):
                query = start_event_silver_query(
                    spark,
                    bronze_path,
                    silver_path,
                    quarantine_path,
                    checkpoint_path,
                )
                try:
                    try:
                        query.processAllAvailable()
                    except Exception as exc:
                        assert "simulated post-write failure" in str(exc)
                    assert query.exception() is not None
                finally:
                    query.stop()

            assert read_delta(spark, silver_path).count() == 1
            assert read_delta(spark, quarantine_path).count() == 2

            query = start_event_silver_query(
                spark,
                bronze_path,
                silver_path,
                quarantine_path,
                checkpoint_path,
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
