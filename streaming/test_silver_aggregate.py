from datetime import datetime
from unittest.mock import Mock, patch

from pyspark.sql import Row, SparkSession

from silver_aggregate import aggregate_events, main, parse_and_validate


def spark_session():
    return (
        SparkSession.builder
        .master("local[2]")
        .appName("nexa-silver-tests")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )


def bronze_rows(spark, values):
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


def event(event_id, event_type, occurred_at, holder_ref, hold_id="hold-1"):
    payload = f'"holdId":"{hold_id}"' if hold_id else '"reason":"INSUFFICIENT_AVAILABILITY"'
    return (
        '{'
        f'"eventId":"{event_id}",'
        f'"eventType":"{event_type}",'
        '"eventVersion":1,'
        f'"occurredAt":"{occurred_at}",'
        '"resourceId":"resource-1",'
        f'"holderRef":"{holder_ref}",'
        f'"payload":{{{payload}}}'
        '}'
    )


def test_parse_and_validate_accepts_rejected_without_hold_id():
    spark = spark_session()
    try:
        parsed = parse_and_validate(
            bronze_rows(
                spark,
                [
                    '{"eventId":"e-rejected","eventType":"RESERVATION_REJECTED",'
                    '"eventVersion":1,"occurredAt":"2026-09-15T10:01:00Z",'
                    '"resourceId":"resource-1","holderRef":"u-1",'
                    '"payload":{"reason":"INSUFFICIENT_AVAILABILITY",'
                    '"requestedUnits":1,"availableUnits":0}}'
                ],
            )
        )
        row = parsed.select("eventId", "eventType", "valid", "validation_error").first()
        assert row.asDict() == {
            "eventId": "e-rejected",
            "eventType": "RESERVATION_REJECTED",
            "valid": True,
            "validation_error": None,
        }
    finally:
        spark.stop()


def test_parse_and_validate_marks_invalid_json():
    spark = spark_session()
    try:
        row = parse_and_validate(bronze_rows(spark, ["not-json"]))
        result = row.select("valid", "validation_error").first()
        assert result.valid is False
        assert result.validation_error == "invalid_json"
    finally:
        spark.stop()


def test_parse_and_validate_rejects_missing_event_version():
    spark = spark_session()
    try:
        row = parse_and_validate(
            bronze_rows(
                spark,
                [
                    '{"eventId":"e-1","eventType":"RESERVATION_HOLD_CREATED",'
                    '"occurredAt":"2026-09-15T10:01:00Z","resourceId":"resource-1",'
                    '"holderRef":"u-1","payload":{"holdId":"hold-1"}}'
                ],
            )
        ).select("valid", "validation_error").first()
        assert row.asDict() == {"valid": False, "validation_error": "unsupported_event_version"}
    finally:
        spark.stop()


def test_aggregate_excludes_invalid_rows():
    spark = spark_session()
    try:
        parsed = parse_and_validate(
            bronze_rows(
                spark,
                [
                    event("e-valid", "RESERVATION_HOLD_CREATED", "2026-09-15T10:01:00Z", "u-1"),
                    "not-json",
                ],
            )
        )
        row = aggregate_events(parsed).first()
        assert row.event_count == 1
        assert row.distinct_users == 1
    finally:
        spark.stop()


def test_aggregate_counts_events_and_distinct_users_in_exact_tumbling_windows():
    spark = spark_session()
    try:
        parsed = parse_and_validate(
            bronze_rows(
                spark,
                [
                    event("e-1", "RESERVATION_HOLD_CREATED", "2026-09-15T10:04:59Z", "u-1"),
                    event("e-2", "RESERVATION_HOLD_CREATED", "2026-09-15T10:05:00Z", "u-2"),
                ],
            )
        )
        rows = (
            aggregate_events(parsed.filter("valid"), with_watermark=False)
            .orderBy("window_start")
            .collect()
        )
        assert [(row.window_start, row.window_end, row.event_count, row.distinct_users) for row in rows] == [
            (datetime(2026, 9, 15, 10, 0), datetime(2026, 9, 15, 10, 5), 1, 1),
            (datetime(2026, 9, 15, 10, 5), datetime(2026, 9, 15, 10, 10), 1, 1),
        ]
    finally:
        spark.stop()


def test_aggregate_deduplicates_event_id_before_counting():
    spark = spark_session()
    try:
        parsed = parse_and_validate(
            bronze_rows(
                spark,
                [
                    event("e-1", "RESERVATION_CONFIRMED", "2026-09-15T10:01:00Z", "u-1"),
                    event("e-1", "RESERVATION_CONFIRMED", "2026-09-15T10:01:00Z", "u-1"),
                ],
            )
        )
        row = aggregate_events(parsed.filter("valid"), with_watermark=False).first()
        assert row.event_count == 1
        assert row.distinct_users == 1
    finally:
        spark.stop()


def test_main_propagates_failure_from_either_stream_and_stops_both():
    active_spark = spark_session()
    try:
        for failed_stream in ("invalid", "silver"):
            failure = RuntimeError(f"{failed_stream} stream failed")
            spark = Mock()
            session = Mock()
            session.builder.appName.return_value.config.return_value.getOrCreate.return_value = spark
            spark.streams.awaitAnyTermination.side_effect = failure
            invalid_query = Mock()
            silver_query = Mock()
            parsed = Mock()
            aggregated = Mock()
            parsed.filter.return_value.select.return_value.writeStream.format.return_value.outputMode.return_value.option.return_value.start.return_value = invalid_query
            aggregated.writeStream.format.return_value.outputMode.return_value.option.return_value.start.return_value = silver_query
            # A failure in either registered query must escape main(). Real
            # Delta execution and recovery are covered by test_silver_stream.
            with (
                patch("silver_aggregate.SparkSession", session),
                patch("silver_aggregate.parse_and_validate", return_value=parsed),
                patch("silver_aggregate.aggregate_events", return_value=aggregated),
                patch("silver_aggregate.logger"),
            ):
                try:
                    main()
                except RuntimeError as exc:
                    assert exc is failure
                else:
                    raise AssertionError("main ignored a streaming-query failure")
                spark.streams.awaitAnyTermination.assert_called_once_with()
                invalid_query.stop.assert_called_once_with()
                silver_query.stop.assert_called_once_with()
                spark.stop.assert_called_once_with()
    finally:
        active_spark.stop()


if __name__ == "__main__":
    for test in (
        test_parse_and_validate_accepts_rejected_without_hold_id,
        test_parse_and_validate_marks_invalid_json,
        test_parse_and_validate_rejects_missing_event_version,
        test_aggregate_excludes_invalid_rows,
        test_aggregate_counts_events_and_distinct_users_in_exact_tumbling_windows,
        test_aggregate_deduplicates_event_id_before_counting,
        test_main_propagates_failure_from_either_stream_and_stops_both,
    ):
        test()
    print("NEXA_SILVER_PROJECTION_TESTS_OK")
