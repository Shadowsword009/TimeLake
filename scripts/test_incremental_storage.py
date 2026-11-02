import os
import shutil
import pandas as pd
import duckdb

from src.timelake.storage import append_incremental_data


output_dir = "lake/incremental_test/events"

if os.path.exists(output_dir):
    shutil.rmtree(output_dir)

events = pd.DataFrame({
    "event_id": ["EVT501", "EVT502", "EVT503"],
    "customer_id": ["CUST001", "CUST002", "CUST003"],
    "event_timestamp": [
        "2026-01-10 10:00:00",
        "2026-01-15 11:00:00",
        "2026-02-05 12:00:00"
    ],
    "event_type": ["purchase", "login", "product_view"],
    "amount": [500.0, 0.0, 0.0],
    "year": [2026, 2026, 2026],
    "month": [1, 1, 2]
})

first_count = append_incremental_data(
    events,
    output_dir
)

second_count = append_incremental_data(
    events,
    output_dir
)

connection = duckdb.connect()
result = connection.execute(f"""
    SELECT COUNT(*), COUNT(DISTINCT event_id)
    FROM read_parquet(
        '{output_dir}/**/*.parquet',
        hive_partitioning = true
    )
""").fetchone()

print("\nIncremental storage verification")
print("--------------------------------")
print(f"First write files: {first_count}")
print(f"Retry write files: {second_count}")
print(f"Stored records: {result[0]}")
print(f"Unique event IDs: {result[1]}")

connection.close()