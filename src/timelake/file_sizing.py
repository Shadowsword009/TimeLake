import io
import os
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq


def write_size_aware_parquet(
    events,
    output_dir,
    prefix="part",
    target_size_mb=128,
    max_size_mb=256,
    compression="snappy"
):
    if events.empty:
        raise ValueError("Cannot write an empty Parquet file")

    if target_size_mb <= 0 or max_size_mb <= 0:
        raise ValueError("File sizes must be positive")

    if target_size_mb > max_size_mb:
        raise ValueError("Target size cannot exceed maximum size")

    target_bytes = target_size_mb * 1024 * 1024
    max_bytes = max_size_mb * 1024 * 1024

    os.makedirs(output_dir, exist_ok=True)

    sample = events.iloc[:min(len(events), 1000)]

    buffer = io.BytesIO()

    pq.write_table(
        pa.Table.from_pandas(sample, preserve_index=False),
        buffer,
        compression=compression
    )

    estimated_bytes_per_row = max(
        buffer.tell() / len(sample),
        1
    )

    rows_per_file = max(
        1,
        int(target_bytes / estimated_bytes_per_row)
    )

    written_files = []
    file_index = 0

    def write_chunk(chunk):
        nonlocal file_index

        file_path = os.path.join(
            output_dir,
            f"{prefix}-{file_index:05d}.parquet"
        )

        table = pa.Table.from_pandas(
            chunk,
            preserve_index=False
        )

        pq.write_table(
            table,
            file_path,
            compression=compression
        )

        actual_size = os.path.getsize(file_path)

        if actual_size > max_bytes and len(chunk) > 1:
            os.remove(file_path)

            midpoint = len(chunk) // 2

            write_chunk(chunk.iloc[:midpoint])
            write_chunk(chunk.iloc[midpoint:])

        else:
            written_files.append(file_path)
            file_index += 1

    for start in range(0, len(events), rows_per_file):
        chunk = events.iloc[start:start + rows_per_file]
        write_chunk(chunk)

    return written_files