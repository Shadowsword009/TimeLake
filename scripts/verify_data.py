import duckdb
duckdb.sql("SET TimeZone = 'UTC'")


query = """
SELECT
    COUNT(*) AS total_records,
    COUNT(DISTINCT event_id) AS unique_events,
    COUNT(DISTINCT month) AS monthly_partitions,
    MIN(event_timestamp) AS first_event,
    MAX(event_timestamp) AS last_event
FROM read_parquet(
    'lake/curated/events/**/*.parquet',
    hive_partitioning = true
)
"""

result = duckdb.sql(query).df()

print("\nData integrity verification")
print("---------------------------")
print(result.to_string(index=False))

if result.iloc[0]["total_records"] != 978:
    raise ValueError("Record count mismatch")

if result.iloc[0]["unique_events"] != 978:
    raise ValueError("Duplicate event IDs detected")

if result.iloc[0]["monthly_partitions"] != 6:
    raise ValueError("Partition count mismatch")

partition_check = duckdb.sql("""
    SELECT
        event_timestamp,
        event_date,
        year,
        month,
        EXTRACT(YEAR FROM event_timestamp) AS timestamp_year,
        EXTRACT(MONTH FROM event_timestamp) AS timestamp_month
    FROM read_parquet(
        'lake/curated/events/**/*.parquet',
        hive_partitioning = true
    )
    ORDER BY event_timestamp DESC
    LIMIT 5
""").df()

print("\nTimestamp and partition verification")
print(partition_check.to_string(index=False))

print("\nAll integrity checks passed")