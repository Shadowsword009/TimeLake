import pandas as pd
import pyarrow.parquet as pq

from src.timelake.compaction import compact_partitioned_data


def test_compaction_reduces_file_count(tmp_path):
    input_dir = tmp_path / "input"
    output_dir = tmp_path / "compacted"

    partition_dir = input_dir / "year=2026" / "month=1"
    partition_dir.mkdir(parents=True)

    events = pd.DataFrame({
        "event_id": [f"EVT{i:03d}" for i in range(10)],
        "customer_id": [f"CUST{i:03d}" for i in range(10)],
        "amount": [100.0] * 10
    })

    for index in range(5):
        events.iloc[index * 2:(index + 1) * 2].to_parquet(
            partition_dir / f"part-{index}.parquet",
            index=False
        )

    files_before, files_after = compact_partitioned_data(
        str(input_dir),
        str(output_dir),
        target_rows_per_file=20
    )

    assert files_before == 5
    assert files_after == 1


def test_compaction_preserves_records(tmp_path):
    input_dir = tmp_path / "input"
    output_dir = tmp_path / "compacted"

    partition_dir = input_dir / "year=2026" / "month=1"
    partition_dir.mkdir(parents=True)

    events = pd.DataFrame({
        "event_id": [f"EVT{i:03d}" for i in range(10)],
        "amount": [100.0] * 10
    })

    events.to_parquet(
        partition_dir / "part-1.parquet",
        index=False
    )

    compact_partitioned_data(
        str(input_dir),
        str(output_dir),
        target_rows_per_file=20
    )

    output_files = list(output_dir.rglob("*.parquet"))

    result = pd.concat(
        [pd.read_parquet(file) for file in output_files],
        ignore_index=True
    )

    assert len(result) == 10
    assert result["event_id"].nunique() == 10

def test_compaction_restores_previous_output_on_failure(
    tmp_path,
    monkeypatch
):
    import os

    input_dir = tmp_path / "input"
    output_dir = tmp_path / "compacted"

    partition_dir = input_dir / "year=2026" / "month=1"
    partition_dir.mkdir(parents=True)

    events = pd.DataFrame({
        "event_id": ["EVT001", "EVT002"],
        "amount": [100.0, 200.0]
    })

    events.to_parquet(
        partition_dir / "part-1.parquet",
        index=False
    )

    previous_partition = output_dir / "year=2025" / "month=12"
    previous_partition.mkdir(parents=True)

    previous_file = previous_partition / "previous_version.txt"
    previous_file.write_text("previous dataset")

    original_replace = os.replace

    def failing_replace(source, destination):
        if str(source).endswith(".staging"):
            raise OSError("Simulated publishing failure")

        return original_replace(source, destination)

    monkeypatch.setattr(os, "replace", failing_replace)

    import pytest

    with pytest.raises(OSError):
        compact_partitioned_data(
            str(input_dir),
            str(output_dir),
            target_rows_per_file=10
        )

    assert previous_file.exists()
    assert previous_file.read_text() == "previous dataset"

    assert not os.path.exists(str(output_dir) + ".staging")
    assert not os.path.exists(str(output_dir) + ".backup")


def test_byte_aware_compaction_preserves_records_and_limits_file_size(tmp_path):
    import hashlib

    input_dir = tmp_path / "input"
    output_dir = tmp_path / "compacted"

    partition_dir = input_dir / "year=2026" / "month=1"
    partition_dir.mkdir(parents=True)

    events = pd.DataFrame({
        "event_id": [f"EVT{i:04d}" for i in range(300)],
        "amount": [float(i) for i in range(300)],
        "payload": [
            hashlib.sha256(str(i).encode()).hexdigest() * 8
            for i in range(300)
        ]
    })

    events.to_parquet(
        partition_dir / "part-1.parquet",
        index=False
    )

    files_before, files_after = compact_partitioned_data(
        str(input_dir),
        str(output_dir),
        target_size_mb=0.01,
        max_size_mb=0.02
    )

    output_files = list(output_dir.rglob("*.parquet"))

    result = pd.concat(
        [pd.read_parquet(file) for file in output_files],
        ignore_index=True
    )

    max_size_bytes = int(0.02 * 1024 * 1024)

    assert files_before == 1
    assert files_after == len(output_files)
    assert files_after > 1
    assert len(result) == 300
    assert result["event_id"].nunique() == 300
    assert all(
        file.stat().st_size <= max_size_bytes
        for file in output_files
    )
