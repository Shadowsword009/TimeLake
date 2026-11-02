import os
import sqlite3
import pandas as pd


def main():
    db_path = "metadata/pipeline_runs.db"

    if not os.path.exists(db_path):
        print("No pipeline run history found")
        return

    connection = sqlite3.connect(db_path)

    try:
        runs = pd.read_sql_query(
            """
            SELECT
                run_id,
                pipeline_type,
                start_timestamp,
                end_timestamp,
                status,
                input_records,
                valid_records,
                rejected_records,
                duplicate_records,
                output_records,
                parquet_files,
                error_message
            FROM pipeline_runs
            ORDER BY start_timestamp DESC
            LIMIT 10
            """,
            connection
        )

        if runs.empty:
            print("No pipeline runs found")
        else:
            print("\nRecent Pipeline Runs")
            print("====================")
            print(runs.to_string(index=False))

    finally:
        connection.close()


if __name__ == "__main__":
    main()