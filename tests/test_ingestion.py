
import pandas as pd
import pytest

from src.timelake.ingestion import (
    ingest_data,
    load_csv,
    validate_columns,
    EVENT_COLUMNS,
    CUSTOMER_COLUMNS
)


def test_successful_ingestion(tmp_path):
    events = pd.DataFrame({
        "event_id": ["EVT001"],
        "source_file": ["test.csv"],
        "customer_id": ["CUST001"],
        "event_timestamp": ["2026-01-10"],
        "ingestion_timestamp": ["2026-01-11"],
        "event_type": ["purchase"],
        "amount": [500]
    })

    customers = pd.DataFrame({
        "customer_id": ["CUST001"],
        "signup_timestamp": ["2025-01-01"]
    })

    events.to_csv(tmp_path / "events.csv", index=False)
    customers.to_csv(tmp_path / "customers.csv", index=False)

    loaded_events, loaded_customers = ingest_data(str(tmp_path))

    assert len(loaded_events) == 1
    assert len(loaded_customers) == 1
    assert list(loaded_events.columns) == EVENT_COLUMNS
    assert list(loaded_customers.columns) == CUSTOMER_COLUMNS


def test_missing_file_fails(tmp_path):
    file_path = tmp_path / "missing.csv"

    with pytest.raises(FileNotFoundError):
        load_csv(str(file_path))


def test_empty_file_fails(tmp_path):
    file_path = tmp_path / "empty.csv"
    file_path.write_text("")

    with pytest.raises(ValueError):
        load_csv(str(file_path))


def test_missing_columns_fail():
    df = pd.DataFrame({
        "event_id": ["EVT001"]
    })

    with pytest.raises(ValueError, match="missing columns"):
        validate_columns(
            df,
            EVENT_COLUMNS,
            "events.csv"
        )


def test_missing_customer_file_fails(tmp_path):
    events = pd.DataFrame({
        "event_id": ["EVT001"],
        "source_file": ["test.csv"],
        "customer_id": ["CUST001"],
        "event_timestamp": ["2026-01-10"],
        "ingestion_timestamp": ["2026-01-11"],
        "event_type": ["purchase"],
        "amount": [500]
    })

    events.to_csv(tmp_path / "events.csv", index=False)

    with pytest.raises(FileNotFoundError):
        ingest_data(str(tmp_path))
