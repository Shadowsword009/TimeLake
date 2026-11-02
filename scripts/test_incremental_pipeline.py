import os
import tempfile
import pandas as pd
import duckdb

from src.timelake.incremental_pipeline import run_incremental_pipeline


def create_events(event_ids):
    records = []

    for index, event_id in enumerate(event_ids):
        records.append({
            "event_id": event_id,
            "source_file": "test_batch.csv",
            "customer_id": f"CUST00{index + 1}",
            "event_timestamp": f"2026-01-{10 + index} 10:00:00",
            "ingestion_timestamp": f"2026-02-{10 + index} 12:00:00",
            "event_type": "purchase",
            "amount": 500.0
        })

    return pd.DataFrame(records)


with tempfile.TemporaryDirectory() as temp_dir:

    input_dir = os.path.join(temp_dir, "input")
    output_dir = os.path.join(temp_dir, "lake", "events")
    db_path = os.path.join(temp_dir, "metadata", "registry.db")
    quarantine_dir = os.path.join(temp_dir, "quarantine")

    os.makedirs(input_dir, exist_ok=True)

    customers = pd.DataFrame({
        "customer_id": ["CUST001", "CUST002", "CUST003"],
        "signup_timestamp": [
            "2025-01-01",
            "2025-01-02",
            "2025-01-03"
        ]
    })

    customers.to_csv(
        os.path.join(input_dir, "customers.csv"),
        index=False
    )

    print("\nFIRST BATCH")
    create_events(["EVT601", "EVT602"]).to_csv(
        os.path.join(input_dir, "events.csv"),
        index=False
    )

    run_incremental_pipeline(
        input_dir,
        output_dir,
        db_path,
        quarantine_dir
    )

    print("\nSECOND BATCH")
    create_events(["EVT602", "EVT603"]).to_csv(
        os.path.join(input_dir, "events.csv"),
        index=False
    )

    run_incremental_pipeline(
        input_dir,
        output_dir,
        db_path,
        quarantine_dir
    )

    print("\nREPEATING SECOND BATCH")

    run_incremental_pipeline(
        input_dir,
        output_dir,
        db_path,
        quarantine_dir
    )

    connection = duckdb.connect()

    result = connection.execute(f"""
        SELECT
            COUNT(*) AS total_records,
            COUNT(DISTINCT event_id) AS unique_events
        FROM read_parquet(
            '{output_dir}/**/*.parquet',
            hive_partitioning = true
        )
    """).fetchone()

    print("\nFINAL VERIFICATION")
    print("------------------")
    print(f"Total records: {result[0]}")
    print(f"Unique events: {result[1]}")

    connection.close()