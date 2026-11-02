import duckdb


def run_query(query):
    connection = duckdb.connect()
    connection.execute("SET TimeZone = 'UTC'")

    result = connection.execute(query).df()

    connection.close()

    return result


def monthly_event_summary(data_path="lake/curated/events/**/*.parquet"):
    query = f"""
    SELECT
        year,
        month,
        COUNT(*) AS total_events,
        SUM(amount) AS total_amount
    FROM read_parquet(
        '{data_path}',
        hive_partitioning = true
    )
    GROUP BY year, month
    ORDER BY year, month
    """

    return run_query(query)


def monthly_filtered_query(
    month,
    data_path="lake/curated/events/**/*.parquet"
):
    query = f"""
    SELECT
        COUNT(*) AS total_events,
        SUM(amount) AS total_amount
    FROM read_parquet(
        '{data_path}',
        hive_partitioning = true
    )
    WHERE year = 2026
      AND month = {int(month)}
    """

    return run_query(query)