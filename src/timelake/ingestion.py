
import os
import pandas as pd


EVENT_COLUMNS = [
    "event_id",
    "source_file",
    "customer_id",
    "event_timestamp",
    "ingestion_timestamp",
    "event_type",
    "amount"
]

CUSTOMER_COLUMNS = [
    "customer_id",
    "signup_timestamp"
]


def load_csv(file_path):
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"File not found: {file_path}")

    if os.path.getsize(file_path) == 0:
        raise ValueError(f"Input file is empty: {file_path}")

    return pd.read_csv(file_path)


def validate_columns(df, required_columns, file_name):
    missing_columns = [
        column for column in required_columns
        if column not in df.columns
    ]

    if missing_columns:
        raise ValueError(
            f"{file_name} is missing columns: {missing_columns}"
        )


def ingest_data(input_dir="data/input"):
    events_path = os.path.join(input_dir, "events.csv")
    customers_path = os.path.join(input_dir, "customers.csv")

    events = load_csv(events_path)
    customers = load_csv(customers_path)

    validate_columns(events, EVENT_COLUMNS, "events.csv")
    validate_columns(customers, CUSTOMER_COLUMNS, "customers.csv")

    events = events[EVENT_COLUMNS].copy()
    customers = customers[CUSTOMER_COLUMNS].copy()

    print(f"Events loaded: {len(events)}")
    print(f"Customers loaded: {len(customers)}")

    return events, customers
