import os
import yaml
import pandas as pd

from src.timelake.generator import generate_dataset
from src.timelake.ingestion import ingest_data
from src.timelake.validation import validate_data
from src.timelake.transformation import (
    deduplicate_events,
    transform_events
)
from src.timelake.storage import write_partitioned_data


def main():
    with open("config/settings.yaml", "r") as file:
        settings = yaml.safe_load(file)

    generator_settings = settings["generator"]

    input_dir = "data/benchmark_input"
    quarantine_dir = "data/benchmark_quarantine"
    partitioned_dir = "lake/benchmark/events"
    unpartitioned_dir = "lake/benchmark/unpartitioned"

    generate_dataset(
        num_records=100000,
        customer_count=generator_settings["customer_count"],
        start_date=generator_settings["start_date"],
        end_date=generator_settings["end_date"],
        seed=generator_settings["random_seed"],
        output_dir=input_dir
    )

    events, customers = ingest_data(input_dir)

    valid_events, rejected_events = validate_data(
        events,
        customers,
        quarantine_dir
    )

    unique_events, duplicate_count = deduplicate_events(
        valid_events
    )

    transformed_events = transform_events(unique_events)

    partitioned_files = write_partitioned_data(
        transformed_events,
        output_dir=partitioned_dir,
        max_rows_per_file=5000
    )

    os.makedirs(unpartitioned_dir, exist_ok=True)

    unpartitioned_path = os.path.join(
        unpartitioned_dir,
        "events.parquet"
    )

    transformed_events.to_parquet(
        unpartitioned_path,
        engine="pyarrow",
        compression="snappy",
        index=False
    )

    print("\nBenchmark dataset created")
    print("-------------------------")
    print(f"Input records: {len(events)}")
    print(f"Rejected records: {len(rejected_events)}")
    print(f"Duplicates removed: {duplicate_count}")
    print(f"Final records: {len(transformed_events)}")
    print(f"Monthly partitioned files: {partitioned_files}")
    print("Unpartitioned files: 1")


if __name__ == "__main__":
    main()