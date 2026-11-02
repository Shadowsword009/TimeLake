
import os
import numpy as np
import pandas as pd


def generate_dataset(
    num_records=1000,
    customer_count=1000,
    start_date="2026-01-01",
    end_date="2026-06-30",
    seed=42,
    output_dir="data/input"
):


    rng = np.random.default_rng(seed)

    os.makedirs(output_dir, exist_ok=True)



    customers = pd.DataFrame({
        "customer_id": [
            f"CUST{i:05d}" for i in range(1, customer_count + 1)
        ],
        "signup_timestamp": pd.date_range(
            start=start_date,
            periods=customer_count,
            freq="h"
        ).astype(str)
    })



    start = pd.Timestamp(start_date)
    end = pd.Timestamp(end_date) + pd.Timedelta(days=1)

    total_seconds = int((end - start).total_seconds())

    timestamps = [
        start + pd.Timedelta(
            seconds=int(x)
        )
        for x in rng.integers(0, total_seconds, num_records)
    ]

    customer_ids = [
        f"CUST{x:05d}"
        for x in rng.integers(1, customer_count + 1, num_records)
    ]

    event_types = rng.choice(
        ["login", "product_view", "add_to_cart", "purchase", "cancellation"],
        size=num_records,
        p=[0.20, 0.30, 0.15, 0.25, 0.10]
    )

    amounts = []

    for event_type in event_types:
        if event_type in ["login", "product_view"]:
            amounts.append(0.0)
        elif event_type == "add_to_cart":
            amounts.append(round(float(rng.uniform(10, 500)), 2))
        elif event_type == "purchase":
            amounts.append(round(float(rng.uniform(100, 10000)), 2))
        else:
            amounts.append(round(float(rng.uniform(0, 10000)), 2))

    # Events can arrive up to 30 days after occurring.
    delays = rng.integers(0, 31, num_records)

    ingestion_timestamps = [
        timestamp + pd.Timedelta(days=int(delay))
        for timestamp, delay in zip(timestamps, delays)
    ]

    events = pd.DataFrame({
        
        "event_id": [
            f"EVT{i:08d}" for i in range(1, num_records + 1)
        ],
        "source_file": "events.csv",
        "customer_id": customer_ids,
        "event_timestamp": timestamps,
        "ingestion_timestamp": ingestion_timestamps,
        "event_type": event_types,
        "amount": amounts
    })


    # Missing customer IDs
    if num_records >= 20:
        missing_indices = rng.choice(
            num_records,
            size=max(1, num_records // 100),
            replace=False
        )

        events.loc[missing_indices, "customer_id"] = None

    # Invalid customer references
    if num_records >= 20:
        invalid_customer_indices = rng.choice(
            num_records,
            size=max(1, num_records // 200),
            replace=False
        )

        events.loc[
            invalid_customer_indices, "customer_id"
        ] = "CUST99999"

    # Negative transaction amounts
    if num_records >= 20:
        negative_indices = rng.choice(
            num_records,
            size=max(1, num_records // 200),
            replace=False
        )

        events.loc[negative_indices, "amount"] = -500.0

    # Invalid event type
    if num_records >= 20:
        invalid_type_index = int(rng.integers(0, num_records))

        events.loc[
            invalid_type_index, "event_type"
        ] = "unknown_event"

    # Business-rule violation:
    # A login event should have zero amount.
    if num_records >= 20:
        invalid_amount_index = int(rng.integers(0, num_records))

        events.loc[
            invalid_amount_index,
            ["event_type", "amount"]
        ] = ["login", 250.0]


    if num_records >= 20:
        duplicate_count = max(1, num_records // 100)

        duplicate_indices = rng.choice(
            num_records,
            size=duplicate_count,
            replace=False
        )

        duplicate_events = events.iloc[
            duplicate_indices
        ].copy()

        events = pd.concat(
            [events, duplicate_events],
            ignore_index=True
        )



    events = events.sample(
        frac=1,
        random_state=seed
    ).reset_index(drop=True)



    customers.to_csv(
        os.path.join(output_dir, "customers.csv"),
        index=False
    )

    events.to_csv(
        os.path.join(output_dir, "events.csv"),
        index=False
    )

    print("\nTimeLake Data Generation Completed")
    print("----------------------------------")
    print(f"Customers generated : {len(customers)}")
    print(f"Events generated    : {len(events)}")
    print(f"Output directory    : {output_dir}")

    return customers, events
