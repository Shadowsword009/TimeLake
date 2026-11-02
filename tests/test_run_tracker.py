
import sqlite3
import pytest

from src.timelake.run_tracker import (
    start_run,
    complete_run,
    fail_run
)


def read_run(db_path, run_id):
    with sqlite3.connect(db_path) as connection:
        return connection.execute(
            """
            SELECT
                run_id,
                start_timestamp,
                end_timestamp,
                status,
                input_records,
                valid_records,
                rejected_records,
                duplicate_records,
                output_records,
                parquet_files,
                error_message,
                pipeline_type
            FROM pipeline_runs
            WHERE run_id = ?
            """,
            (run_id,)
        ).fetchone()


def test_start_run_creates_running_record(tmp_path):
    db_path = str(tmp_path / "runs.db")

    run_id = start_run(db_path)

    record = read_run(db_path, run_id)

    assert record is not None
    assert record[0] == run_id
    assert record[1] is not None
    assert record[3] == "RUNNING"
    assert record[11] == "FULL_REFRESH"


def test_complete_run_persists_metrics(tmp_path):
    db_path = str(tmp_path / "runs.db")

    run_id = start_run(db_path)

    complete_run(
        run_id,
        input_records=100,
        valid_records=90,
        rejected_records=10,
        duplicate_records=5,
        output_records=85,
        parquet_files=6,
        db_path=db_path
    )

    record = read_run(db_path, run_id)

    assert record[3] == "SUCCESS"
    assert record[1] is not None
    assert record[2] is not None
    assert record[4:10] == (100, 90, 10, 5, 85, 6)
    assert record[10] is None


def test_fail_run_persists_error(tmp_path):
    db_path = str(tmp_path / "runs.db")

    run_id = start_run(db_path)

    fail_run(
        run_id,
        error_message="Simulated storage failure",
        input_records=100,
        valid_records=90,
        rejected_records=10,
        db_path=db_path
    )

    record = read_run(db_path, run_id)

    assert record[3] == "FAILED"
    assert record[2] is not None
    assert record[4] == 100
    assert record[5] == 90
    assert record[6] == 10
    assert record[10] == "Simulated storage failure"


def test_complete_unknown_run_fails(tmp_path):
    db_path = str(tmp_path / "runs.db")

    with pytest.raises(ValueError, match="Unknown pipeline run"):
        complete_run(
            "unknown-run",
            input_records=0,
            valid_records=0,
            rejected_records=0,
            duplicate_records=0,
            output_records=0,
            parquet_files=0,
            db_path=db_path
        )

def test_existing_database_migrates_without_losing_records(tmp_path):
    db_path = str(tmp_path / "legacy_runs.db")

    with sqlite3.connect(db_path) as connection:
        connection.execute("""
            CREATE TABLE pipeline_runs (
                run_id TEXT PRIMARY KEY,
                start_timestamp TEXT NOT NULL,
                end_timestamp TEXT,
                status TEXT NOT NULL,
                input_records INTEGER DEFAULT 0,
                valid_records INTEGER DEFAULT 0,
                rejected_records INTEGER DEFAULT 0,
                duplicate_records INTEGER DEFAULT 0,
                output_records INTEGER DEFAULT 0,
                parquet_files INTEGER DEFAULT 0,
                error_message TEXT
            )
        """)

        connection.execute(
            """
            INSERT INTO pipeline_runs (
                run_id,
                start_timestamp,
                status,
                input_records
            )
            VALUES (?, ?, ?, ?)
            """,
            ("legacy-run-001", "2026-11-01T10:00:00+00:00", "SUCCESS", 100)
        )

    from src.timelake.run_tracker import initialize_run_store

    initialize_run_store(db_path)

    with sqlite3.connect(db_path) as connection:
        record = connection.execute(
            """
            SELECT run_id, input_records, pipeline_type
            FROM pipeline_runs
            WHERE run_id = ?
            """,
            ("legacy-run-001",)
        ).fetchone()

    assert record == ("legacy-run-001", 100, "FULL_REFRESH")
