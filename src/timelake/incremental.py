import pandas as pd

from src.timelake.registry import (
    get_processed_ids,
    register_events
)


def filter_new_events(
    events,
    db_path="metadata/event_registry.db"
):
    processed_ids = get_processed_ids(db_path)

    new_events = events[
        ~events["event_id"].isin(processed_ids)
    ].drop_duplicates(
        subset=["event_id"],
        keep="first"
    ).copy()

    duplicate_count = len(events) - len(new_events)

    print("\nIncremental processing")
    print("----------------------")
    print(f"Incoming records: {len(events)}")
    print(f"New records: {len(new_events)}")
    print(f"Previously processed or repeated: {duplicate_count}")

    return new_events


def mark_events_processed(
    events,
    db_path="metadata/event_registry.db"
):
    if not events.empty:
        register_events(
            events["event_id"].tolist(),
            db_path
        )