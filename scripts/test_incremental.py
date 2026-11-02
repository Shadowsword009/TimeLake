import os
import tempfile
import pandas as pd

from src.timelake.incremental import (
    filter_new_events,
    mark_events_processed
)


with tempfile.TemporaryDirectory() as temp_dir:
    db_path = os.path.join(
        temp_dir,
        "incremental_test.db"
    )

    first_batch = pd.DataFrame({
        "event_id": ["EVT100", "EVT101", "EVT102"]
    })

    second_batch = pd.DataFrame({
        "event_id": ["EVT101", "EVT102", "EVT103", "EVT104"]
    })

    print("First batch")

    first_result = filter_new_events(
        first_batch,
        db_path
    )

    mark_events_processed(first_result, db_path)

    print("\nSecond batch")

    second_result = filter_new_events(
        second_batch,
        db_path
    )

    mark_events_processed(second_result, db_path)

    print("\nAccepted event IDs:")
    print(second_result["event_id"].tolist())