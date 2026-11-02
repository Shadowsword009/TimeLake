
import os
import shutil
import pandas as pd
import duckdb

from src.timelake.incremental import (
    filter_new_events,
    mark_events_processed
)
from src.timelake.transformation import transform_events
from src.timelake.storage import write_partitioned_data


db_path = "metadata/late_arrival_test.db"
output_dir = "lake/late_arrival_test/events"

if os.path.exists(db_path):
    os.remove(db_path)

if os.path.exists(output_dir):
    shutil.rmtree(output_dir)

events = pd.DataFrame({
    "event_id": ["EVT_LATE_001"],
    "source_file": ["late_batch.csv"],
    "customer_id": ["CUST001"],
    "event_timestamp": ["2026-01-28 10:00:00"],
    "ingestion_timestamp": ["2026-02-12 14:00:00"],
    "event_type": ["purchase"],
    "amount": [750.0]
})

events = transform_events(events)

new_events = filter_new_events(
    events,
    db_path
)

write_partitioned_data(
    new_events,
    output_dir
)

mark_events_processed(
    new_events,
    db_path
)

connection = duckdb.connect()
connection.execute("SET TimeZone = 'UTC'")

query = connection.execute(f"""
    SELECT
        event_id,
        CAST(event_timestamp AS VARCHAR) AS event_timestamp,
        CAST(ingestion_timestamp AS VARCHAR) AS ingestion_timestamp,
        year,
        month
    FROM read_parquet(
        '{output_dir}/**/*.parquet',
        hive_partitioning = true
    )
""")

columns = [column[0] for column in query.description]
rows = query.fetchall()

print("\nLate arrival verification")
print("-------------------------")
print("\t".join(columns))

for row in rows:
    print("\t".join(str(value) for value in row))

connection.close()
