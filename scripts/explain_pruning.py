import duckdb


connection = duckdb.connect()
connection.execute("SET TimeZone = 'UTC'")

data_path = "lake/curated/events/**/*.parquet"

filtered_query = f"""
SELECT COUNT(*), SUM(amount)
FROM read_parquet('{data_path}', hive_partitioning = true)
WHERE year = 2026 AND month = 1
"""

full_query = f"""
SELECT COUNT(*), SUM(amount)
FROM read_parquet('{data_path}', hive_partitioning = true)
"""

print("\nPartition-filtered query plan")
print("-----------------------------")
filtered_plan = connection.execute(
    "EXPLAIN ANALYZE " + filtered_query
).fetchdf()

print(filtered_plan.to_string(index=False))

print("\nFull-scan query plan")
print("--------------------")
full_plan = connection.execute(
    "EXPLAIN ANALYZE " + full_query
).fetchdf()

print(full_plan.to_string(index=False))

connection.close()