import os
import pandas as pd
import pytest

from src.timelake.storage import (
    write_partitioned_data,
    append_incremental_data
)


@pytest.fixture
def sample_events():
    return pd.DataFrame({
        "event_id": ["EVT001", "EVT002", "EVT003"],
        "customer_id": ["CUST001", "CUST002", "CUST003"],
        "event_timestamp": pd.to_datetime([
            "2026-01-10",
            "2026-01-15",
            "2026-02-05"
        ], utc=True),
        "ingestion_timestamp": pd.to_datetime([
            "2026-01-11",
            "2026-01-16",
            "2026-02-06"
        ], utc=True),
        "event_type": ["purchase", "login", "purchase"],
        "amount": [500.0, 0.0, 750.0],
        "year": [2026, 2026, 2026],
        "month": [1, 1, 2]
    })


def test_full_refresh_storage(sample_events, tmp_path):
    output_dir = tmp_path / "curated" / "events"

    file_count = write_partitioned_data(
        sample_events,
        str(output_dir),
        max_rows_per_file=2
    )

    assert file_count == 2
    assert output_dir.exists()

    files = list(output_dir.rglob("*.parquet"))

    assert len(files) == 2


def test_incremental_storage(sample_events, tmp_path):
    output_dir = tmp_path / "incremental" / "events"

    file_count = append_incremental_data(
        sample_events,
        str(output_dir)
    )

    assert file_count == 2

    files = list(output_dir.rglob("*.parquet"))

    assert len(files) == 2


def test_incremental_retry_does_not_duplicate(
    sample_events,
    tmp_path
):
    output_dir = tmp_path / "incremental" / "events"

    first_count = append_incremental_data(
        sample_events,
        str(output_dir)
    )

    second_count = append_incremental_data(
        sample_events,
        str(output_dir)
    )

    assert first_count == second_count

    import pyarrow.parquet as pq

    total_records = sum(
        pq.ParquetFile(file).metadata.num_rows
        for file in output_dir.rglob("*.parquet")
    )

    assert total_records == 3


def test_empty_incremental_batch_fails(tmp_path):
    output_dir = tmp_path / "incremental" / "events"

    with pytest.raises(ValueError):
        append_incremental_data(
            pd.DataFrame(),
            str(output_dir)
        )


def test_duplicate_event_ids_fail(sample_events, tmp_path):
    output_dir = tmp_path / "incremental" / "events"

    duplicated = pd.concat(
        [sample_events, sample_events.iloc[[0]]],
        ignore_index=True
    )

    with pytest.raises(ValueError):
        append_incremental_data(
            duplicated,
            str(output_dir)
        )

def test_incremental_retry_rejects_changed_content(
    sample_events,
    tmp_path
):
    output_dir = tmp_path / "incremental" / "events"

    append_incremental_data(
        sample_events,
        str(output_dir)
    )

    changed_events = sample_events.copy()
    changed_events.loc[0, "amount"] = 999.0

    with pytest.raises(
        ValueError,
        match="Batch content changed"
    ):
        append_incremental_data(
            changed_events,
            str(output_dir)
        )


def test_incremental_retry_ignores_row_order(
    sample_events,
    tmp_path
):
    output_dir = tmp_path / "incremental" / "events"

    first_count = append_incremental_data(
        sample_events,
        str(output_dir)
    )

    reordered_events = sample_events.iloc[::-1].reset_index(
        drop=True
    )

    second_count = append_incremental_data(
        reordered_events,
        str(output_dir)
    )

    assert first_count == second_count