
from src.timelake.ingestion import ingest_data
from src.timelake.validation import validate_data
from src.timelake.transformation import (
    deduplicate_events,
    transform_events
)
from src.timelake.storage import write_partitioned_data
from src.timelake.run_tracker import (
    start_run,
    complete_run,
    fail_run
)


def main():
    run_id = start_run()

    input_records = 0
    valid_records = 0
    rejected_records = 0
    duplicate_records = 0
    output_records = 0
    parquet_files = 0

    print(f"\nPipeline run ID: {run_id}")

    try:
        events, customers = ingest_data()
        input_records = len(events)

        valid_events, rejected_events = validate_data(
            events,
            customers
        )

        valid_records = len(valid_events)
        rejected_records = len(rejected_events)

        unique_events, duplicate_count = deduplicate_events(
            valid_events
        )

        duplicate_records = duplicate_count

        transformed_events = transform_events(unique_events)
        output_records = len(transformed_events)

        parquet_files = write_partitioned_data(
            transformed_events
        )

        complete_run(
            run_id=run_id,
            input_records=input_records,
            valid_records=valid_records,
            rejected_records=rejected_records,
            duplicate_records=duplicate_records,
            output_records=output_records,
            parquet_files=parquet_files
        )

        print("\nPipeline summary")
        print("----------------")
        print(f"Run ID: {run_id}")
        print("Status: SUCCESS")
        print(f"Input records: {input_records}")
        print(f"Valid records: {valid_records}")
        print(f"Rejected records: {rejected_records}")
        print(f"Duplicates removed: {duplicate_records}")
        print(f"Final records: {output_records}")
        print(f"Parquet files: {parquet_files}")

    except Exception as error:
        fail_run(
            run_id=run_id,
            error_message=error,
            input_records=input_records,
            valid_records=valid_records,
            rejected_records=rejected_records,
            duplicate_records=duplicate_records,
            output_records=output_records,
            parquet_files=parquet_files
        )

        print(f"\nPipeline failed: {error}")
        print(f"Run ID: {run_id}")

        raise


if __name__ == "__main__":
    main()
