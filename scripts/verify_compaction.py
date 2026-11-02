import glob
import os
import duckdb


MAX_SIZE_MB = 256
EXPECTED_RECORDS = 98012
EXPECTED_PARTITIONS = 6


def main():
    connection = duckdb.connect()
    connection.execute("SET TimeZone = 'UTC'")

    try:
        query = """
        SELECT
            COUNT(*) AS total_records,
            COUNT(DISTINCT event_id) AS unique_events,
            COUNT(DISTINCT month) AS monthly_partitions,
            CAST(MIN(event_timestamp) AS VARCHAR) AS first_event,
            CAST(MAX(event_timestamp) AS VARCHAR) AS last_event
        FROM read_parquet(
            'lake/compacted/events/**/*.parquet',
            hive_partitioning = true
        )
        """

        cursor = connection.execute(query)
        columns = [column[0] for column in cursor.description]
        result = dict(zip(columns, cursor.fetchone()))

        print("\nCompacted dataset verification")
        print("------------------------------")

        for key, value in result.items():
            print(f"{key}: {value}")

        if result["total_records"] != EXPECTED_RECORDS:
            raise ValueError("Record count mismatch")

        if result["unique_events"] != EXPECTED_RECORDS:
            raise ValueError("Duplicate event IDs detected")

        if result["monthly_partitions"] != EXPECTED_PARTITIONS:
            raise ValueError("Partition count mismatch")

        parquet_files = glob.glob(
            "lake/compacted/events/**/*.parquet",
            recursive=True
        )

        if not parquet_files:
            raise ValueError("No Parquet files found")

        max_size_bytes = MAX_SIZE_MB * 1024 * 1024

        print("\nParquet file sizes")
        print("------------------")

        for file_path in sorted(parquet_files):
            size_bytes = os.path.getsize(file_path)
            size_mb = size_bytes / (1024 * 1024)

            print(f"{file_path}: {size_mb:.2f} MB")

            if size_bytes > max_size_bytes:
                raise ValueError(
                    f"File exceeds {MAX_SIZE_MB} MB: {file_path}"
                )

        print(f"\nFiles verified: {len(parquet_files)}")
        print(f"Maximum allowed size: {MAX_SIZE_MB} MB")
        print("\nAll compaction checks passed")

    finally:
        connection.close()


if __name__ == "__main__":
    main()