import duckdb
import re


def main():
    connection = duckdb.connect()
    connection.execute("SET TimeZone = 'UTC'")

    query = """
    SELECT
        COUNT(*) AS total_events,
        SUM(amount) AS total_amount
    FROM read_parquet(
        'lake/compacted/events/**/*.parquet',
        hive_partitioning = true
    )
    WHERE year = 2026 AND month = 1
    """

    try:
        print("\nJanuary query on compacted dataset")
        print("----------------------------------")

        cursor = connection.execute(query)
        columns = [column[0] for column in cursor.description]
        result = dict(zip(columns, cursor.fetchone()))

        print(f"Events: {result['total_events']}")
        print(f"Total amount: {result['total_amount']:.2f}")

        plan = connection.execute(
            "EXPLAIN ANALYZE " + query
        ).fetchall()

        execution_plan = plan[0][1]

        print("\nExecution plan")
        print("--------------")
        print(execution_plan)

        files_scanned = re.search(
            r"Scanning Files:\s*(\d+)/(\d+)",
            execution_plan
        )

        if files_scanned:
            scanned = int(files_scanned.group(1))
            total = int(files_scanned.group(2))

            print("\nPartition pruning summary")
            print("-------------------------")
            print(f"Files scanned: {scanned}/{total}")
            print(f"Files skipped: {total - scanned}")

            if scanned >= total:
                raise ValueError("Partition pruning was not demonstrated")

        else:
            print("\nFile scan count not found in execution plan")

    finally:
        connection.close()


if __name__ == "__main__":
    main()
