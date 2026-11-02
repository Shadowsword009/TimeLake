
import pandas as pd

from src.timelake.validation import validate_data


def create_events():
    return pd.DataFrame({
        "event_id": ["EVT001", "EVT002", "EVT003", "EVT004", "EVT005", "EVT006"],
        "source_file": ["test.csv"] * 6,
        "customer_id": [
            "CUST001",
            "CUST999",
            "CUST001",
            "CUST001",
            "CUST001",
            "CUST001"
        ],
        "event_timestamp": [
            "2026-01-10",
            "2026-01-10",
            "invalid-date",
            "2026-01-10",
            "2026-01-10",
            "2026-01-10"
        ],
        "ingestion_timestamp": [
            "2026-01-11",
            "2026-01-11",
            "2026-01-11",
            "2026-01-11",
            "2026-01-11",
            "2026-01-11"
        ],
        "event_type": [
            "purchase",
            "purchase",
            "purchase",
            "unknown",
            "purchase",
            "login"
        ],
        "amount": [
            500,
            500,
            500,
            500,
            -100,
            100
        ]
    })


def test_valid_and_invalid_records(tmp_path):
    events = create_events()

    customers = pd.DataFrame({
        "customer_id": ["CUST001"],
        "signup_timestamp": ["2025-01-01"]
    })

    valid, rejected = validate_data(
        events,
        customers,
        str(tmp_path)
    )

    assert len(valid) == 1
    assert len(rejected) == 5

    assert valid.iloc[0]["event_id"] == "EVT001"


def test_rejection_reasons(tmp_path):
    events = create_events()

    customers = pd.DataFrame({
        "customer_id": ["CUST001"],
        "signup_timestamp": ["2025-01-01"]
    })

    valid, rejected = validate_data(
        events,
        customers,
        str(tmp_path)
    )

    reasons = dict(
        zip(
            rejected["event_id"],
            rejected["rejection_reason"]
        )
    )

    assert reasons["EVT002"] == "invalid_customer_reference"
    assert reasons["EVT003"] == "invalid_event_timestamp"
    assert reasons["EVT004"] == "invalid_event_type"
    assert reasons["EVT005"] == "negative_amount"
    assert reasons["EVT006"] == "invalid_amount_for_event_type"


def test_missing_event_id_is_rejected(tmp_path):
    events = create_events()
    events.loc[0, "event_id"] = None

    customers = pd.DataFrame({
        "customer_id": ["CUST001"],
        "signup_timestamp": ["2025-01-01"]
    })

    valid, rejected = validate_data(
        events,
        customers,
        str(tmp_path)
    )

    assert len(rejected) == 6
    assert "missing_event_id" in rejected.iloc[0]["rejection_reason"]


def test_rejected_records_are_quarantined(tmp_path):
    events = create_events()

    customers = pd.DataFrame({
        "customer_id": ["CUST001"],
        "signup_timestamp": ["2025-01-01"]
    })

    valid, rejected = validate_data(
        events,
        customers,
        str(tmp_path)
    )

    quarantine_file = tmp_path / "rejected_events.csv"

    assert quarantine_file.exists()

    stored_rejected = pd.read_csv(quarantine_file)

    assert len(stored_rejected) == len(rejected)
