import os
import pandas as pd


ALLOWED_EVENT_TYPES = {
    "login",
    "product_view",
    "add_to_cart",
    "purchase",
    "cancellation"
}

ZERO_AMOUNT_EVENTS = {
    "login",
    "product_view"
}


def validate_data(events, customers, quarantine_dir="data/quarantine"):
    os.makedirs(quarantine_dir, exist_ok=True)

    events = events.copy().reset_index(drop=True)

    valid_customer_ids = set(
        customers["customer_id"].dropna()
    )

    customer_values = events["customer_id"].astype("string").str.strip()
    event_id_values = events["event_id"].astype("string").str.strip()

    event_time = pd.to_datetime(
        events["event_timestamp"],
        errors="coerce",
        utc=True
    )

    ingestion_time = pd.to_datetime(
        events["ingestion_timestamp"],
        errors="coerce",
        utc=True
    )

    amount = pd.to_numeric(
        events["amount"],
        errors="coerce"
    )

    rejection_checks = {
        "missing_event_id": (
            events["event_id"].isna()
            | event_id_values.eq("").fillna(True)
        ),
        "missing_customer_id": (
            events["customer_id"].isna()
            | customer_values.eq("").fillna(True)
        ),
        "invalid_customer_reference": (
            ~events["customer_id"].isin(valid_customer_ids)
            & events["customer_id"].notna()
            & customer_values.ne("").fillna(False)
        ),
        "invalid_event_timestamp": event_time.isna(),
        "invalid_ingestion_timestamp": ingestion_time.isna(),
        "invalid_event_type": ~events["event_type"].isin(
            ALLOWED_EVENT_TYPES
        ),
        "invalid_amount": amount.isna(),
        "negative_amount": amount.lt(0),
        "invalid_amount_for_event_type": (
            events["event_type"].isin(ZERO_AMOUNT_EVENTS)
            & amount.notna()
            & amount.ne(0)
        )
    }

    rejection_reasons = [
        [] for _ in range(len(events))
    ]

    for reason, mask in rejection_checks.items():
        for index in mask[mask].index:
            rejection_reasons[index].append(reason)

    events["rejection_reason"] = rejection_reasons

    valid_events = events[
        events["rejection_reason"].map(len) == 0
    ].copy()

    rejected_events = events[
        events["rejection_reason"].map(len) > 0
    ].copy()

    rejected_events["rejection_reason"] = (
        rejected_events["rejection_reason"].apply(
            lambda reasons: "|".join(reasons)
        )
    )

    rejected_path = os.path.join(
        quarantine_dir,
        "rejected_events.csv"
    )

    rejected_events.to_csv(
        rejected_path,
        index=False
    )

    valid_events = valid_events.drop(
        columns=["rejection_reason"]
    )

    print("\nValidation completed")
    print("--------------------")
    print(f"Input records: {len(events)}")
    print(f"Valid records: {len(valid_events)}")
    print(f"Rejected records: {len(rejected_events)}")

    return valid_events, rejected_events