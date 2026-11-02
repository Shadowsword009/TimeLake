
import os
import pandas as pd
import pyarrow.parquet as pq
import pytest

from src.timelake.file_sizing import write_size_aware_parquet


def create_events(count):
    return pd.DataFrame({
        "event_id": [f"EVT{i:06d}" for i in range(count)],
        "customer_id": [f"CUST{i % 1000:05d}" for i in range(count)],
        "event_type": ["purchase"] * count,
        "amount": [float(i * 10) for i in range(count)]
    })


def test_size_aware_writer_creates_files(tmp_path):
    events = create_events(1000)

    files = write_size_aware_parquet(
        events,
        str(tmp_path),
        target_size_mb=0.01,
        max_size_mb=0.02
    )

    assert len(files) > 0
    assert all(os.path.exists(file) for file in files)


def test_size_aware_writer_preserves_records(tmp_path):
    events = create_events(1000)

    files = write_size_aware_parquet(
        events,
        str(tmp_path),
        target_size_mb=0.01,
        max_size_mb=0.02
    )

    stored_records = sum(
        pq.ParquetFile(file).metadata.num_rows
        for file in files
    )

    assert stored_records == len(events)


def test_size_aware_writer_rejects_empty_data(tmp_path):
    with pytest.raises(ValueError):
        write_size_aware_parquet(
            pd.DataFrame(),
            str(tmp_path)
        )


def test_size_aware_writer_rejects_invalid_sizes(tmp_path):
    events = create_events(10)

    with pytest.raises(ValueError):
        write_size_aware_parquet(
            events,
            str(tmp_path),
            target_size_mb=2,
            max_size_mb=1
        )
