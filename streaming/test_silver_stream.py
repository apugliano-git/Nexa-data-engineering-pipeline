from datetime import datetime
from tempfile import TemporaryDirectory

from pyspark.sql import Row, SparkSession

from silver_aggregate import aggregate_events, parse_and_validate


def spark_session():
    spark = (
        SparkSession.builder
        .master("local[2]")
        .appName("nexa-silver-stream-tests")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    return spark


def event(event_id, occurred_at, holder_ref="u-1"):
    return (
        '{'
        f'"eventId":"{event_id}",'
        '"eventType":"RESERVATION_HOLD_CREATED",'
        '"eventVersion":1,'
        f'"occurredAt":"{occurred_at}",'
        '"resourceId":"resource-1",'
        f'"holderRef":"{holder_ref}",'
        f'"payload":{{"holdId":"hold-{event_id}"}}'
        '}'
    )


def bronze_df(spark, values):
    return spark.createDataFrame(
        [
            Row(
                key="resource-1",
                value=value,
                topic="reservation.events.v1",
                partition=0,
                offset=index,
                timestamp=datetime(2026, 9, 15, 10, 0),
            )
            for index, value in enumerate(values)
        ]
    )


def append_bronze(spark, path, values, mode):
    bronze_df(spark, values).write.format("delta").mode(mode).save(path)


def start_silver_query(spark, bronze_path, silver_path, checkpoint_path):
    bronze_stream = (
        spark.readStream
        .format("delta")
        .load(bronze_path)
    )
    return (
        aggregate_events(parse_and_validate(bronze_stream))
        .writeStream
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", checkpoint_path)
        .start(silver_path)
    )


def read_silver(spark, path):
    return spark.read.format("delta").load(path).orderBy("window_start")


def test_initial_snapshot_is_aggregated_and_persisted():
    spark = spark_session()
    with TemporaryDirectory() as root:
        bronze_path = f"{root}/bronze"
        silver_path = f"{root}/silver"
        checkpoint_path = f"{root}/checkpoint"
        try:
            append_bronze(
                spark,
                bronze_path,
                [
                    event("e-1", "2026-09-15T10:01:00Z"),
                    event("e-2", "2026-09-15T10:02:00Z"),
                    event("e-3", "2026-09-15T10:11:00Z", "u-2"),
                ],
                "overwrite",
            )
            query = start_silver_query(spark, bronze_path, silver_path, checkpoint_path)
            try:
                query.processAllAvailable()
                assert query.exception() is None
            finally:
                query.stop()

            rows = read_silver(spark, silver_path).collect()
            assert len(rows) == 1
            assert rows[0].event_count == 2
            assert rows[0].distinct_users == 1
        finally:
            spark.stop()


def test_late_event_inside_watermark_is_kept_and_late_event_after_close_is_not():
    spark = spark_session()
    with TemporaryDirectory() as root:
        bronze_path = f"{root}/bronze"
        silver_path = f"{root}/silver"
        checkpoint_path = f"{root}/checkpoint"
        try:
            append_bronze(spark, bronze_path, [event("e-1", "2026-09-15T10:01:00Z")], "overwrite")
            query = start_silver_query(spark, bronze_path, silver_path, checkpoint_path)
            try:
                query.processAllAvailable()
                append_bronze(spark, bronze_path, [event("e-2", "2026-09-15T10:08:00Z")], "append")
                query.processAllAvailable()
                append_bronze(spark, bronze_path, [event("e-3", "2026-09-15T10:04:00Z")], "append")
                query.processAllAvailable()
                append_bronze(spark, bronze_path, [event("e-4", "2026-09-15T10:11:00Z", "u-2")], "append")
                query.processAllAvailable()
                append_bronze(spark, bronze_path, [event("e-5", "2026-09-15T10:02:00Z", "u-3")], "append")
                query.processAllAvailable()
                assert query.exception() is None
            finally:
                query.stop()

            rows = read_silver(spark, silver_path).collect()
            assert len(rows) == 1
            assert rows[0].event_count == 2
            assert rows[0].distinct_users == 1
        finally:
            spark.stop()


def test_restart_recovers_open_window_and_does_not_reinsert_finalized_rows():
    spark = spark_session()
    with TemporaryDirectory() as root:
        bronze_path = f"{root}/bronze"
        silver_path = f"{root}/silver"
        checkpoint_path = f"{root}/checkpoint"
        try:
            append_bronze(spark, bronze_path, [event("e-1", "2026-09-15T10:01:00Z")], "overwrite")
            query = start_silver_query(spark, bronze_path, silver_path, checkpoint_path)
            query.processAllAvailable()
            query.stop()

            query = start_silver_query(spark, bronze_path, silver_path, checkpoint_path)
            try:
                append_bronze(spark, bronze_path, [event("e-2", "2026-09-15T10:11:00Z")], "append")
                query.processAllAvailable()
                assert query.exception() is None
            finally:
                query.stop()

            query = start_silver_query(spark, bronze_path, silver_path, checkpoint_path)
            try:
                query.processAllAvailable()
                assert query.exception() is None
            finally:
                query.stop()

            rows = read_silver(spark, silver_path).collect()
            assert len(rows) == 1
            assert rows[0].event_count == 1
        finally:
            spark.stop()


if __name__ == "__main__":
    for test in (
        test_initial_snapshot_is_aggregated_and_persisted,
        test_late_event_inside_watermark_is_kept_and_late_event_after_close_is_not,
        test_restart_recovers_open_window_and_does_not_reinsert_finalized_rows,
    ):
        test()
    print("NEXA_SILVER_STREAM_TESTS_OK")
