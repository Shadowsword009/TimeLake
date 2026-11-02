
import sqlite3
import pandas as pd
import pytest

from src.timelake import incremental_pipeline


def read_latest_run(db_path):
    with sqlite3.connect(db_path) as connection:
        return connection.execute(
            """
            SELECT
                status,
                pipeline_type,
                input_records,
                valid_records,
                rejected_records,
                duplicate_records,
                output_records,
                parquet_files,
                error_message
            FROM pipeline_runs
            ORDER BY start_timestamp DESC
            LIMIT 1
            """
        ).fetchone()


def setup_pipeline(monkeypatch, new_events):
    events = pd.DataFrame({"event_id": ["EVT001", "EVT002", "EVT003", "EVT004"]})
    valid_events = pd.DataFrame({"event_id": ["EVT001", "EVT002", "EVT003"]})
    unique_events = pd.DataFrame({"event_id": ["EVT001", "EVT002"]})
    rejected_events = pd.DataFrame({"event_id": ["EVT004"]})

    monkeypatch.setattr(
        incremental_pipeline,
        "ingest_data",
        lambda input_dir: (events, pd.DataFrame())
    )

    monkeypatch.setattr(
        incremental_pipeline,
        "validate_data",
        lambda events, customers, quarantine_dir: (
            valid_events,
            rejected_events
        )
    )

    monkeypatch.setattr(
        incremental_pipeline,
        "deduplicate_events",
        lambda events: (unique_events, 1)
    )

    monkeypatch.setattr(
        incremental_pipeline,
        "filter_new_events",
        lambda events, db_path: new_events
    )

    monkeypatch.setattr(
        incremental_pipeline,
        "transform_events",
        lambda events: events.copy()
    )

    monkeypatch.setattr(
        incremental_pipeline,
        "mark_events_processed",
        lambda events, db_path: None
    )


def test_incremental_success_is_recorded(monkeypatch, tmp_path):
    run_db = str(tmp_path / "runs.db")
    new_events = pd.DataFrame({
        "event_id": ["EVT001", "EVT002"]
    })

    setup_pipeline(monkeypatch, new_events)

    monkeypatch.setattr(
        incremental_pipeline,
        "append_incremental_data",
        lambda events, output_dir: 2
    )

    incremental_pipeline.run_incremental_pipeline(
        run_db_path=run_db
    )

    record = read_latest_run(run_db)

    assert record == (
        "SUCCESS",
        "INCREMENTAL",
        4,
        3,
        1,
        1,
        2,
        2,
        None
    )


def test_incremental_no_new_events_is_recorded(monkeypatch, tmp_path):
    run_db = str(tmp_path / "runs.db")
    new_events = pd.DataFrame(columns=["event_id"])

    setup_pipeline(monkeypatch, new_events)

    incremental_pipeline.run_incremental_pipeline(
        run_db_path=run_db
    )

    record = read_latest_run(run_db)

    assert record == (
        "SUCCESS",
        "INCREMENTAL",
        4,
        3,
        1,
        1,
        0,
        0,
        None
    )


def test_incremental_storage_failure_is_recorded(monkeypatch, tmp_path):
    run_db = str(tmp_path / "runs.db")
    new_events = pd.DataFrame({
        "event_id": ["EVT001", "EVT002"]
    })

    setup_pipeline(monkeypatch, new_events)

    def failing_storage(events, output_dir):
        raise OSError("Simulated storage failure")

    monkeypatch.setattr(
        incremental_pipeline,
        "append_incremental_data",
        failing_storage
    )

    with pytest.raises(OSError, match="Simulated storage failure"):
        incremental_pipeline.run_incremental_pipeline(
            run_db_path=run_db
        )

    record = read_latest_run(run_db)

    assert record[0] == "FAILED"
    assert record[1] == "INCREMENTAL"
    assert record[2:8] == (4, 3, 1, 1, 0, 0)
    assert record[8] == "Simulated storage failure"
