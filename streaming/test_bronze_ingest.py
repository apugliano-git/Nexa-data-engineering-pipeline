from datetime import datetime, timezone

from pyspark.sql import Row, SparkSession

from bronze_ingest import to_bronze


def test_to_bronze_preserves_raw_value_and_kafka_metadata():
    spark = (
        SparkSession.builder
        .master("local[2]")
        .appName("nexa-bronze-projection-test")
        .getOrCreate()
    )
    try:
        source = spark.createDataFrame(
            [
                Row(
                    key=bytearray(b"resource-1"),
                    value=bytearray(
                        b'{"eventId":"event-1","payload":{"holdId":"hold-1"}}'
                    ),
                    topic="reservation.events.v1",
                    partition=2,
                    offset=41,
                    timestamp=datetime(2026, 9, 4, 12, 0, tzinfo=timezone.utc),
                )
            ]
        )

        row = to_bronze(source).first()

        assert row.asDict() == {
            "key": "resource-1",
            "value": '{"eventId":"event-1","payload":{"holdId":"hold-1"}}',
            "topic": "reservation.events.v1",
            "partition": 2,
            "offset": 41,
            "timestamp": datetime(2026, 9, 4, 12, 0),
        }
    finally:
        spark.stop()


if __name__ == "__main__":
    test_to_bronze_preserves_raw_value_and_kafka_metadata()
    print("NEXA_BRONZE_PROJECTION_TEST_OK")
