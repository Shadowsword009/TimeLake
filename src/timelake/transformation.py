
import pandas as pd


def deduplicate_events(events):
    events = events.copy()

    events["ingestion_timestamp"] = pd.to_datetime(
        events["ingestion_timestamp"],
        utc=True
    )

    events = events.sort_values(
        ["ingestion_timestamp", "event_id", "source_file"]
    )

    duplicate_count = events["event_id"].duplicated().sum()

    deduplicated = events.drop_duplicates(
        subset=["event_id"],
        keep="first"
    ).copy()

    return deduplicated, int(duplicate_count)


def transform_events(events):
    events = events.copy()

    events["event_timestamp"] = pd.to_datetime(
        events["event_timestamp"],
        utc=True
    )

    events["ingestion_timestamp"] = pd.to_datetime(
        events["ingestion_timestamp"],
        utc=True
    )

    events["amount"] = pd.to_numeric(
        events["amount"],
        errors="raise"
    )

    events["event_date"] = events["event_timestamp"].dt.date
    events["year"] = events["event_timestamp"].dt.year
    events["month"] = events["event_timestamp"].dt.month

    return events
