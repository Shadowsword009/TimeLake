import duckdb


query = """
SELECT *
FROM read_parquet(
    'lake/curated/events/**/*.parquet',
    hive_partitioning = true
)
LIMIT 5
"""

result = duckdb.sql(query).df()

print(result.to_string())
print("\nColumn names:")
print(result.columns.tolist())

total_records = duckdb.sql("""
    SELECT COUNT(*) AS total
    FROM read_parquet(
        'lake/curated/events/**/*.parquet',
        hive_partitioning = true
    )
""").fetchall()[0][0]

print("\nTotal records:")
print(total_records)