
import pandas as pd

from src.timelake.ingestion import ingest_data
from src.timelake.validation import validate_data
from src.timelake.transformation import (
    deduplicate_events,
    transform_events
)
from src.timelake.storage import write_partitioned_data


def test_full_refresh_pipeline(tmp_path):
    input_dir = tmp_path / "input"
    quarantine_dir = tmp_path / "quarantine"
    output_dir = tmp_path / "curated" / "events"

    input_dir.mkdir()

    customers = pd.DataFrame({
        "customer_id": ["CUST001", "CUST002"],
        "signup_timestamp": ["2025-01-01", "2025-01-02"]
    })

    events = pd.DataFrame({
        "event_id": ["EVT001", "EVT002", "EVT002"],
        "source_file": ["batch.csv"] * 3,
        "customer_id": ["CUST001", "CUST002", "CUST002"],
        "event_timestamp": [
            "2026-01-10 10:00:00",
            "2026-02-10 10:00:00",
            "2026-02-10 10:00:00"
        ],
        "ingestion_timestamp": [
            "2026-01-11 10:00:00",
            "2026-02-11 10:00:00",
            "2026-02-12 10:00:00"
        ],
        "event_type": ["purchase", "purchase", "purchase"],
        "amount": [500.0, 750.0, 750.0]
    })

    customers.to_csv(input_dir / "customers.csv", index=False)
    events.to_csv(input_dir / "events.csv", index=False)

    loaded_events, loaded_customers = ingest_data(str(input_dir))

    valid_events, rejected_events = validate_data(
        loaded_events,
        loaded_customers,
        str(quarantine_dir)
    )

    unique_events, duplicate_count = deduplicate_events(
        valid_events
    )

    transformed_events = transform_events(unique_events)

    file_count = write_partitioned_data(
        transformed_events,
        str(output_dir),
        max_rows_per_file=50
    )

    assert len(loaded_events) == 3
    assert len(valid_events) == 3
    assert len(rejected_events) == 0
    assert duplicate_count == 1
    assert len(transformed_events) == 2
    assert file_count == 2

    assert set(transformed_events["event_id"]) == {
        "EVT001",
        "EVT002"
    }

    assert (output_dir / "year=2026" / "month=1").exists()
    assert (output_dir / "year=2026" / "month=2").exists()
