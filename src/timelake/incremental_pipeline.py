
from src.timelake.ingestion import ingest_data
from src.timelake.validation import validate_data
from src.timelake.transformation import (
    deduplicate_events,
    transform_events
)
from src.timelake.incremental import (
    filter_new_events,
    mark_events_processed
)
from src.timelake.storage import append_incremental_data
from src.timelake.run_tracker import (
    start_run,
    complete_run,
    fail_run
)


def run_incremental_pipeline(
    input_dir="data/input",
    output_dir="lake/incremental/events",
    db_path="metadata/event_registry.db",
    quarantine_dir="data/incremental_quarantine",
    run_db_path="metadata/pipeline_runs.db"
):
    run_id = start_run(
        db_path=run_db_path,
        pipeline_type="INCREMENTAL"
    )

    input_records = 0
    valid_records = 0
    rejected_records = 0
    duplicate_records = 0
    output_records = 0
    parquet_files = 0

    print(f"\nIncremental pipeline run ID: {run_id}")

    try:
        events, customers = ingest_data(input_dir)
        input_records = len(events)

        valid_events, rejected_events = validate_data(
            events,
            customers,
            quarantine_dir
        )

        valid_records = len(valid_events)
        rejected_records = len(rejected_events)

        unique_events, duplicate_count = deduplicate_events(
            valid_events
        )

        duplicate_records = duplicate_count

        new_events = filter_new_events(
            unique_events,
            db_path
        )

        if new_events.empty:
            complete_run(
                run_id=run_id,
                input_records=input_records,
                valid_records=valid_records,
                rejected_records=rejected_records,
                duplicate_records=duplicate_records,
                output_records=0,
                parquet_files=0,
                db_path=run_db_path
            )

            print("\nNo new events to process")
            print(f"Run ID: {run_id}")
            print("Status: SUCCESS")
            return

        transformed_events = transform_events(new_events)

        parquet_files = append_incremental_data(
            transformed_events,
            output_dir
        )

        output_records = len(transformed_events)

        mark_events_processed(
            transformed_events,
            db_path
        )

        complete_run(
            run_id=run_id,
            input_records=input_records,
            valid_records=valid_records,
            rejected_records=rejected_records,
            duplicate_records=duplicate_records,
            output_records=output_records,
            parquet_files=parquet_files,
            db_path=run_db_path
        )

        print("\nIncremental pipeline summary")
        print("----------------------------")
        print(f"Run ID: {run_id}")
        print("Status: SUCCESS")
        print(f"Input records: {input_records}")
        print(f"Valid records: {valid_records}")
        print(f"Rejected records: {rejected_records}")
        print(f"Duplicates removed: {duplicate_records}")
        print(f"New records stored: {output_records}")
        print(f"Parquet files created: {parquet_files}")

    except Exception as error:
        fail_run(
            run_id=run_id,
            error_message=error,
            input_records=input_records,
            valid_records=valid_records,
            rejected_records=rejected_records,
            duplicate_records=duplicate_records,
            output_records=output_records,
            parquet_files=parquet_files,
            db_path=run_db_path
        )

        print(f"\nIncremental pipeline failed: {error}")
        print(f"Run ID: {run_id}")

        raise
