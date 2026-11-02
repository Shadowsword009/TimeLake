from src.timelake.file_sizing import write_size_aware_parquet
import os
import glob
import shutil
import pandas as pd
import pyarrow.parquet as pq
import hashlib


def calculate_batch_fingerprint(events):
    canonical = events.copy()

    canonical = canonical.reindex(
        sorted(canonical.columns),
        axis=1
    )

    canonical = canonical.sort_values(
        "event_id",
        key=lambda values: values.astype(str),
        kind="mergesort"
    ).reset_index(drop=True)

    schema = "|".join(canonical.columns.astype(str))

    content = canonical.to_json(
        orient="records",
        date_format="iso",
        date_unit="ns",
        double_precision=15,
        force_ascii=False
    )

    fingerprint = hashlib.sha256(
        f"{schema}\n{content}".encode("utf-8")
    ).hexdigest()

    return fingerprint


def write_partitioned_data(
    events,
    output_dir="lake/curated/events",
    max_rows_per_file=None,
    target_size_mb=128,
    max_size_mb=256,
    compression="snappy"
):
    if events.empty:
        raise ValueError("No valid records available for storage")

    if not {"year", "month"}.issubset(events.columns):
        raise ValueError("Missing partition columns")

    if max_rows_per_file is not None and max_rows_per_file <= 0:
        raise ValueError("Maximum rows per file must be positive")

    staging_dir = output_dir + ".staging"
    backup_dir = output_dir + ".backup"

    if os.path.exists(backup_dir):
        if not os.path.exists(output_dir):
            os.replace(backup_dir, output_dir)
        else:
            shutil.rmtree(backup_dir)

    if os.path.exists(staging_dir):
        shutil.rmtree(staging_dir)

    os.makedirs(staging_dir, exist_ok=True)

    file_count = 0

    try:
        for (year, month), group in events.groupby(["year", "month"]):
            partition_dir = os.path.join(
                staging_dir,
                f"year={year}",
                f"month={month}"
            )

            os.makedirs(partition_dir, exist_ok=True)

            if max_rows_per_file is not None:
                for start in range(0, len(group), max_rows_per_file):
                    chunk = group.iloc[
                        start:start + max_rows_per_file
                    ]

                    file_path = os.path.join(
                        partition_dir,
                        f"part-{file_count:05d}.parquet"
                    )

                    chunk.to_parquet(
                        file_path,
                        engine="pyarrow",
                        compression=compression,
                        index=False
                    )

                    file_count += 1

            else:
                written_files = write_size_aware_parquet(
                    group,
                    partition_dir,
                    prefix=f"part-{file_count:05d}",
                    target_size_mb=target_size_mb,
                    max_size_mb=max_size_mb,
                    compression=compression
                )

                file_count += len(written_files)

        parquet_files = glob.glob(
            os.path.join(staging_dir, "**", "*.parquet"),
            recursive=True
        )

        stored_records = sum(
            pq.ParquetFile(file).metadata.num_rows
            for file in parquet_files
        )

        if stored_records != len(events):
            raise ValueError("Staging record count mismatch")

        if os.path.exists(output_dir):
            os.replace(output_dir, backup_dir)

        try:
            os.replace(staging_dir, output_dir)
        except Exception:
            if os.path.exists(backup_dir):
                os.replace(backup_dir, output_dir)
            raise

        if os.path.exists(backup_dir):
            shutil.rmtree(backup_dir)

    except Exception:
        if os.path.exists(staging_dir):
            shutil.rmtree(staging_dir)

        if (
            os.path.exists(backup_dir)
            and not os.path.exists(output_dir)
        ):
            os.replace(backup_dir, output_dir)

        raise

    print("\nStorage completed")
    print("-----------------")
    print(f"Records stored: {stored_records}")
    print(f"Partitions created: {events.groupby(['year', 'month']).ngroups}")
    print(f"Parquet files created: {file_count}")

    return file_count



def append_incremental_data(
    events,
    output_dir="lake/incremental/events",
    max_rows_per_file=None,
    target_size_mb=128,
    max_size_mb=256,
    compression="snappy"
):
    if events.empty:
        raise ValueError("No new records available for storage")

    if not {"year", "month", "event_id"}.issubset(events.columns):
        raise ValueError("Missing required columns")

    if events["event_id"].duplicated().any():
        raise ValueError("Duplicate event IDs in batch")

    if max_rows_per_file is not None and max_rows_per_file <= 0:
        raise ValueError("Maximum rows per file must be positive")

    event_ids = sorted(events["event_id"].astype(str).tolist())

    batch_id = hashlib.sha256(
        "|".join(event_ids).encode()
    ).hexdigest()[:16]

    final_batch_dir = os.path.join(
        output_dir,
        f"batch_id={batch_id}"
    )

    staging_batch_dir = os.path.join(
        output_dir + ".staging",
        f"batch_id={batch_id}"
    )


    if os.path.exists(final_batch_dir):
        existing_files = glob.glob(
            os.path.join(final_batch_dir, "**", "*.parquet"),
            recursive=True
        )

        if not existing_files:
            raise ValueError("Published batch contains no Parquet files")

        existing_data = pd.concat(
            [pd.read_parquet(file) for file in existing_files],
            ignore_index=True
        )

        existing_event_ids = sorted(
            existing_data["event_id"].astype(str).tolist()
        )

        if existing_event_ids != event_ids:
            raise ValueError(
                "Existing batch event IDs do not match incoming events"
            )

        existing_fingerprint = calculate_batch_fingerprint(
            existing_data
        )

        incoming_fingerprint = calculate_batch_fingerprint(events)

        if existing_fingerprint != incoming_fingerprint:
            raise ValueError(
                "Batch content changed for previously published event IDs"
            )

        print(f"Batch {batch_id} already published")
        return len(existing_files)


    if os.path.exists(staging_batch_dir):
        shutil.rmtree(staging_batch_dir)

    os.makedirs(staging_batch_dir, exist_ok=True)

    file_count = 0
    written_records = 0

    try:
        for (year, month), group in events.groupby(["year", "month"]):
            partition_dir = os.path.join(
                staging_batch_dir,
                f"year={year}",
                f"month={month}"
            )

            os.makedirs(partition_dir, exist_ok=True)

            if max_rows_per_file is not None:
                for start in range(0, len(group), max_rows_per_file):
                    chunk = group.iloc[
                        start:start + max_rows_per_file
                    ]

                    file_path = os.path.join(
                        partition_dir,
                        f"part-{file_count:05d}.parquet"
                    )

                    chunk.to_parquet(
                        file_path,
                        engine="pyarrow",
                        compression=compression,
                        index=False
                    )

                    file_count += 1
                    written_records += len(chunk)

            else:
                written_files = write_size_aware_parquet(
                    group,
                    partition_dir,
                    prefix=f"part-{file_count:05d}",
                    target_size_mb=target_size_mb,
                    max_size_mb=max_size_mb,
                    compression=compression
                )

                file_count += len(written_files)
                written_records += sum(
                    pq.ParquetFile(file).metadata.num_rows
                    for file in written_files
                )

        if written_records != len(events):
            raise ValueError("Incremental storage record count mismatch")

        os.makedirs(output_dir, exist_ok=True)

        os.replace(staging_batch_dir, final_batch_dir)

    except Exception:
        if os.path.exists(staging_batch_dir):
            shutil.rmtree(staging_batch_dir)

        raise

    print("\nIncremental storage completed")
    print("-----------------------------")
    print(f"Batch ID: {batch_id}")
    print(f"Records stored: {written_records}")
    print(f"Parquet files created: {file_count}")

    return file_count
