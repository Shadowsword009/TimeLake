import time
import statistics
import duckdb


connection = duckdb.connect()
connection.execute("SET TimeZone = 'UTC'")

partitioned_query = """
SELECT COUNT(*), SUM(amount)
FROM read_parquet(
    'lake/benchmark/events/**/*.parquet',
    hive_partitioning = true
)
WHERE year = 2026 AND month = 1
"""

unpartitioned_query = """
SELECT COUNT(*), SUM(amount)
FROM read_parquet(
    'lake/benchmark/unpartitioned/events.parquet'
)
WHERE year = 2026 AND month = 1
"""


def benchmark_query(name, query):
    connection.execute(query).fetchall()

    execution_times = []

    for _ in range(5):
        start = time.perf_counter()
        result = connection.execute(query).fetchall()
        execution_times.append(
            (time.perf_counter() - start) * 1000
        )

    average_time = statistics.mean(execution_times)
    median_time = statistics.median(execution_times)

    print(f"\n{name}")
    print("-" * len(name))
    print(f"Result: {result}")
    print(f"Average time: {average_time:.3f} ms")
    print(f"Median time: {median_time:.3f} ms")

    print("\nExecution plan:")
    plan = connection.execute(
        "EXPLAIN ANALYZE " + query
    ).fetchall()

    print(plan[0][1])


benchmark_query(
    "Monthly partitioned query",
    partitioned_query
)

benchmark_query(
    "Unpartitioned query",
    unpartitioned_query
)

connection.close()