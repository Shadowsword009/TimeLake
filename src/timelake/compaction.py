
import os
import glob
import shutil
import pandas as pd
import pyarrow.parquet as pq

from src.timelake.file_sizing import write_size_aware_parquet


def compact_partitioned_data(
    input_dir="lake/curated/events",
    output_dir="lake/compacted/events",
    target_rows_per_file=None,
    target_size_mb=128,
    max_size_mb=256,
    compression="snappy"
):
    if target_rows_per_file is not None and target_rows_per_file <= 0:
        raise ValueError("Target rows per file must be positive")

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

    total_before = 0
    total_after = 0
    files_before = 0
    files_after = 0

    try:
        partition_dirs = glob.glob(
            os.path.join(input_dir, "year=*", "month=*")
        )

        for partition_dir in partition_dirs:
            files = glob.glob(
                os.path.join(partition_dir, "*.parquet")
            )

            if not files:
                continue

            combined = pd.concat(
                [pd.read_parquet(file) for file in files],
                ignore_index=True
            )

            total_before += len(combined)
            files_before += len(files)

            relative_path = os.path.relpath(
                partition_dir,
                input_dir
            )

            target_partition = os.path.join(
                staging_dir,
                relative_path
            )

            os.makedirs(target_partition, exist_ok=True)

            if target_rows_per_file is not None:
                for index, start in enumerate(
                    range(0, len(combined), target_rows_per_file)
                ):
                    chunk = combined.iloc[
                        start:start + target_rows_per_file
                    ]

                    output_path = os.path.join(
                        target_partition,
                        f"compact-{index:05d}.parquet"
                    )

                    chunk.to_parquet(
                        output_path,
                        engine="pyarrow",
                        compression=compression,
                        index=False
                    )

            else:
                write_size_aware_parquet(
                    combined,
                    output_dir=target_partition,
                    prefix="compact",
                    target_size_mb=target_size_mb,
                    max_size_mb=max_size_mb,
                    compression=compression
                )

            written_files = glob.glob(
                os.path.join(target_partition, "*.parquet")
            )

            partition_records = sum(
                pq.ParquetFile(file).metadata.num_rows
                for file in written_files
            )

            if partition_records != len(combined):
                raise ValueError(
                    f"Record count mismatch in {relative_path}"
                )

            total_after += partition_records
            files_after += len(written_files)

        if total_before == 0:
            raise ValueError(
                "No Parquet records available for compaction"
            )

        if total_before != total_after:
            raise ValueError("Compaction record count mismatch")

        if os.path.exists(output_dir):
            os.replace(output_dir, backup_dir)

        try:
            os.replace(staging_dir, output_dir)
        except Exception:
            if os.path.exists(backup_dir):
                os.replace(backup_dir, output_dir)
            raise

        if os.path.exists(backup_dir):
            shutil.rmtree(backup_dir, ignore_errors=True)

    except Exception:
        if os.path.exists(staging_dir):
            shutil.rmtree(staging_dir)

        if (
            os.path.exists(backup_dir)
            and not os.path.exists(output_dir)
        ):
            os.replace(backup_dir, output_dir)

        raise

    print("\nCompaction completed")
    print("--------------------")
    print(f"Files before: {files_before}")
    print(f"Files after: {files_after}")
    print(f"Records before: {total_before}")
    print(f"Records after: {total_after}")

    return files_before, files_after
