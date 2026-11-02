import os
import tempfile
import pandas as pd
import duckdb

from src.timelake.transformation import transform_events
from src.timelake.storage import append_incremental_data
from src.timelake.incremental_pipeline import run_incremental_pipeline


with tempfile.TemporaryDirectory() as temp_dir:

    input_dir = os.path.join(temp_dir, "input")
    output_dir = os.path.join(temp_dir, "lake", "events")
    db_path = os.path.join(temp_dir, "metadata", "registry.db")
    quarantine_dir = os.path.join(temp_dir, "quarantine")

    os.makedirs(input_dir, exist_ok=True)

    customers = pd.DataFrame({
        "customer_id": ["CUST001"],
        "signup_timestamp": ["2025-01-01"]
    })

    customers.to_csv(
        os.path.join(input_dir, "customers.csv"),
        index=False
    )

    events = pd.DataFrame({
        "event_id": ["EVT901"],
        "source_file": ["recovery_test.csv"],
        "customer_id": ["CUST001"],
        "event_timestamp": ["2026-01-15 10:00:00"],
        "ingestion_timestamp": ["2026-02-10 12:00:00"],
        "event_type": ["purchase"],
        "amount": [500.0]
    })

    events.to_csv(
        os.path.join(input_dir, "events.csv"),
        index=False
    )

    print("\nSTEP 1: Simulate successful Parquet publishing")

    transformed = transform_events(events)

    append_incremental_data(
        transformed,
        output_dir
    )

    print("\nSTEP 2: Simulate failure before SQLite registration")
    print("Parquet exists, but event ID is not registered")

    print("\nSTEP 3: Retry the incremental pipeline")

    run_incremental_pipeline(
        input_dir,
        output_dir,
        db_path,
        quarantine_dir
    )

    print("\nSTEP 4: Verify final storage")

    connection = duckdb.connect()

    result = connection.execute(f"""
        SELECT
            COUNT(*),
            COUNT(DISTINCT event_id)
        FROM read_parquet(
            '{output_dir}/**/*.parquet',
            hive_partitioning = true
        )
    """).fetchone()

    print(f"Total records: {result[0]}")
    print(f"Unique event IDs: {result[1]}")

    connection.close()