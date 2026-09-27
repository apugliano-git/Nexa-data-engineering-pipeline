from datetime import datetime, timedelta, timezone
import json

from silver_ingest import validate_publication


RESOURCE_ID = "11111111-1111-4111-8111-111111111111"
HOLD_ID = "22222222-2222-4222-8222-222222222222"
EVENT_ID = "33333333-3333-4333-8333-333333333333"
KAFKA_TIME = datetime(2026, 9, 20, 12, 0, tzinfo=timezone.utc)


def event(event_type, payload, **overrides):
    value = {
        "eventId": EVENT_ID,
        "eventType": event_type,
        "eventVersion": 1,
        "occurredAt": "2026-09-20T12:00:00Z",
        "resourceId": RESOURCE_ID,
        "holderRef": "customer-42",
        "payload": payload,
    }
    value.update(overrides)
    return value


def record(value, **overrides):
    source = {
        "key": RESOURCE_ID,
        "value": json.dumps(value) if not isinstance(value, str) else value,
        "topic": "reservation.events.v1",
        "partition": 2,
        "offset": 41,
        "timestamp": KAFKA_TIME,
    }
    source.update(overrides)
    return source


def validate(value, **overrides):
    return validate_publication(record(value, **overrides))


def test_accepts_all_five_ratchet_event_types():
    cases = [
        ("RESERVATION_HOLD_CREATED", {"holdId": HOLD_ID, "expiresAt": "2026-09-20T12:15:00Z"}),
        ("RESERVATION_CONFIRMED", {"holdId": HOLD_ID}),
        ("RESERVATION_RELEASED", {"holdId": HOLD_ID}),
        ("RESERVATION_EXPIRED", {"holdId": HOLD_ID, "scheduledExpiresAt": "2026-09-20T12:15:00Z"}),
        (
            "RESERVATION_REJECTED",
            {"reason": "INSUFFICIENT_AVAILABILITY", "requestedUnits": 1, "availableUnits": 0},
        ),
    ]

    results = [validate(event(event_type, payload)) for event_type, payload in cases]

    assert all(result["valid"] for result in results)
    assert results[-1]["holdId"] is None


def test_preserves_raw_input_and_source_coordinates():
    result = validate(event("RESERVATION_CONFIRMED", {"holdId": HOLD_ID}))

    assert result["valid"] is True
    assert result["value"] == json.dumps(event("RESERVATION_CONFIRMED", {"holdId": HOLD_ID}))
    assert result["topic"] == "reservation.events.v1"
    assert result["partition"] == 2
    assert result["offset"] == 41


def test_rejects_malformed_json_and_non_object_json():
    assert validate("not-json")["validation_error"] == "invalid_json"
    assert validate("[1, 2]")["validation_error"] == "json_not_object"


def test_rejects_missing_null_and_wrong_scalar_types():
    missing_id = event("RESERVATION_CONFIRMED", {"holdId": HOLD_ID})
    missing_id.pop("eventId")
    assert validate(missing_id)["validation_error"] == "missing_event_id"

    null_holder = event("RESERVATION_CONFIRMED", {"holdId": HOLD_ID}, holderRef=None)
    assert validate(null_holder)["validation_error"] == "invalid_holder_ref_type"

    string_version = event("RESERVATION_CONFIRMED", {"holdId": HOLD_ID}, eventVersion="1")
    assert validate(string_version)["validation_error"] == "invalid_event_version_type"

    string_units = event(
        "RESERVATION_REJECTED",
        {"reason": "INSUFFICIENT_AVAILABILITY", "requestedUnits": "1", "availableUnits": 0},
    )
    assert validate(string_units)["validation_error"] == "invalid_requested_units_type"


def test_rejects_unknown_type_and_version():
    assert validate(event("RESERVATION_UNKNOWN", {"holdId": HOLD_ID}))["validation_error"] == "unsupported_event_type"
    assert validate(event("RESERVATION_CONFIRMED", {"holdId": HOLD_ID}, eventVersion=2))["validation_error"] == "unsupported_event_version"


def test_validates_uuid_fields_key_and_holder_ref():
    assert validate(event("RESERVATION_CONFIRMED", {"holdId": HOLD_ID}, eventId="not-uuid"))["validation_error"] == "invalid_event_id"
    assert validate(event("RESERVATION_CONFIRMED", {"holdId": HOLD_ID}, resourceId="not-uuid"))["validation_error"] == "invalid_resource_id"
    assert validate(event("RESERVATION_CONFIRMED", {"holdId": HOLD_ID}, resourceId=RESOURCE_ID), key="99999999-9999-4999-8999-999999999999")["validation_error"] == "resource_key_mismatch"
    assert validate(
        event("RESERVATION_CONFIRMED", {"holdId": HOLD_ID}, holderRef="")
    )["validation_error"] == "empty_holder_ref"


def test_accepts_holder_ref_as_nonempty_non_uuid_string():
    result = validate(event("RESERVATION_CONFIRMED", {"holdId": HOLD_ID}, holderRef="user-123"))

    assert result["valid"] is True


def test_requires_explicit_utc_timestamps():
    missing_zone = event("RESERVATION_CONFIRMED", {"holdId": HOLD_ID}, occurredAt="2026-09-20T12:00:00")
    malformed = event("RESERVATION_CONFIRMED", {"holdId": HOLD_ID}, occurredAt="yesterday")

    assert validate(missing_zone)["validation_error"] == "occurred_at_not_utc"
    assert validate(malformed)["validation_error"] == "invalid_occurred_at"


def test_rejects_invalid_per_event_payloads():
    missing_expiration = event("RESERVATION_HOLD_CREATED", {"holdId": HOLD_ID})
    missing_schedule = event("RESERVATION_EXPIRED", {"holdId": HOLD_ID})
    bad_rejection = event(
        "RESERVATION_REJECTED",
        {"reason": "OTHER", "requestedUnits": 1, "availableUnits": 0},
    )
    wrong_requested_units = event(
        "RESERVATION_REJECTED",
        {"reason": "INSUFFICIENT_AVAILABILITY", "requestedUnits": 2, "availableUnits": 0},
    )

    assert validate(missing_expiration)["validation_error"] == "missing_expires_at"
    assert validate(missing_schedule)["validation_error"] == "missing_scheduled_expires_at"
    assert validate(bad_rejection)["validation_error"] == "invalid_rejection_reason"
    assert validate(wrong_requested_units)["validation_error"] == "invalid_requested_units"


def test_allows_compatible_additional_fields():
    result = validate(
        event(
            "RESERVATION_CONFIRMED",
            {"holdId": HOLD_ID, "futureField": {"kept": True}},
            futureEnvelopeField="allowed",
        )
    )

    assert result["valid"] is True


def test_applies_nexa_clock_policy_without_using_current_time():
    exactly_five_minutes_late = event(
        "RESERVATION_CONFIRMED",
        {"holdId": HOLD_ID},
        occurredAt="2026-09-20T12:05:00Z",
    )
    too_late = event(
        "RESERVATION_CONFIRMED",
        {"holdId": HOLD_ID},
        occurredAt="2026-09-20T12:05:01Z",
    )
    old_event = event(
        "RESERVATION_CONFIRMED",
        {"holdId": HOLD_ID},
        occurredAt="2000-01-01T00:00:00Z",
    )

    assert validate(exactly_five_minutes_late)["valid"] is True
    assert validate(too_late)["validation_error"] == "occurred_at_after_kafka_timestamp"
    assert validate(old_event)["valid"] is True


if __name__ == "__main__":
    for name, test in sorted(globals().items()):
        if name.startswith("test_"):
            test()
    print("NEXA_SILVER_INGEST_VALIDATION_TESTS_OK")
