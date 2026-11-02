
import os
import sqlite3
import uuid
from datetime import datetime, timezone


def get_timestamp():
    return datetime.now(timezone.utc).isoformat()


def initialize_run_store(db_path="metadata/pipeline_runs.db"):
    directory = os.path.dirname(db_path)

    if directory:
        os.makedirs(directory, exist_ok=True)

    with sqlite3.connect(db_path) as connection:
        connection.execute("""
            CREATE TABLE IF NOT EXISTS pipeline_runs (
                run_id TEXT PRIMARY KEY,
                start_timestamp TEXT NOT NULL,
                end_timestamp TEXT,
                status TEXT NOT NULL,
                pipeline_type TEXT NOT NULL DEFAULT 'FULL_REFRESH',
                input_records INTEGER DEFAULT 0,
                valid_records INTEGER DEFAULT 0,
                rejected_records INTEGER DEFAULT 0,
                duplicate_records INTEGER DEFAULT 0,
                output_records INTEGER DEFAULT 0,
                parquet_files INTEGER DEFAULT 0,
                error_message TEXT
            )
        """)

        columns = {
            row[1]
            for row in connection.execute(
                "PRAGMA table_info(pipeline_runs)"
            ).fetchall()
        }

        if "pipeline_type" not in columns:
            connection.execute("""
                ALTER TABLE pipeline_runs
                ADD COLUMN pipeline_type TEXT
                NOT NULL DEFAULT 'FULL_REFRESH'
            """)


def start_run(
    db_path="metadata/pipeline_runs.db",
    pipeline_type="FULL_REFRESH"
):
    if pipeline_type not in {"FULL_REFRESH", "INCREMENTAL"}:
        raise ValueError("Invalid pipeline type")

    initialize_run_store(db_path)

    run_id = str(uuid.uuid4())

    with sqlite3.connect(db_path) as connection:
        connection.execute(
            """
            INSERT INTO pipeline_runs (
                run_id,
                start_timestamp,
                status,
                pipeline_type
            )
            VALUES (?, ?, ?, ?)
            """,
            (
                run_id,
                get_timestamp(),
                "RUNNING",
                pipeline_type
            )
        )

    return run_id


def complete_run(
    run_id,
    input_records,
    valid_records,
    rejected_records,
    duplicate_records,
    output_records,
    parquet_files,
    db_path="metadata/pipeline_runs.db"
):
    initialize_run_store(db_path)

    with sqlite3.connect(db_path) as connection:
        cursor = connection.execute(
            """
            UPDATE pipeline_runs
            SET
                end_timestamp = ?,
                status = ?,
                input_records = ?,
                valid_records = ?,
                rejected_records = ?,
                duplicate_records = ?,
                output_records = ?,
                parquet_files = ?
            WHERE run_id = ?
            """,
            (
                get_timestamp(),
                "SUCCESS",
                input_records,
                valid_records,
                rejected_records,
                duplicate_records,
                output_records,
                parquet_files,
                run_id
            )
        )

        if cursor.rowcount != 1:
            raise ValueError(f"Unknown pipeline run: {run_id}")


def fail_run(
    run_id,
    error_message,
    input_records=0,
    valid_records=0,
    rejected_records=0,
    duplicate_records=0,
    output_records=0,
    parquet_files=0,
    db_path="metadata/pipeline_runs.db"
):
    initialize_run_store(db_path)

    with sqlite3.connect(db_path) as connection:
        cursor = connection.execute(
            """
            UPDATE pipeline_runs
            SET
                end_timestamp = ?,
                status = ?,
                input_records = ?,
                valid_records = ?,
                rejected_records = ?,
                duplicate_records = ?,
                output_records = ?,
                parquet_files = ?,
                error_message = ?
            WHERE run_id = ?
            """,
            (
                get_timestamp(),
                "FAILED",
                input_records,
                valid_records,
                rejected_records,
                duplicate_records,
                output_records,
                parquet_files,
                str(error_message),
                run_id
            )
        )

        if cursor.rowcount != 1:
            raise ValueError(f"Unknown pipeline run: {run_id}")
