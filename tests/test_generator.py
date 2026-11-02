
import pandas as pd

from src.timelake.generator import generate_dataset


def test_generator_creates_expected_counts(tmp_path):
    customers, events = generate_dataset(
        num_records=100,
        customer_count=20,
        seed=42,
        output_dir=str(tmp_path)
    )

    assert len(customers) == 20
    assert len(events) == 101


def test_generator_creates_expected_columns(tmp_path):
    customers, events = generate_dataset(
        num_records=100,
        customer_count=20,
        seed=42,
        output_dir=str(tmp_path)
    )

    assert list(customers.columns) == [
        "customer_id",
        "signup_timestamp"
    ]

    assert list(events.columns) == [
        "event_id",
        "source_file",
        "customer_id",
        "event_timestamp",
        "ingestion_timestamp",
        "event_type",
        "amount"
    ]


def test_generator_is_reproducible(tmp_path):
    _, first_events = generate_dataset(
        num_records=100,
        customer_count=20,
        seed=42,
        output_dir=str(tmp_path / "first")
    )

    _, second_events = generate_dataset(
        num_records=100,
        customer_count=20,
        seed=42,
        output_dir=str(tmp_path / "second")
    )

    pd.testing.assert_frame_equal(
        first_events,
        second_events
    )


def test_generator_includes_duplicate_events(tmp_path):
    _, events = generate_dataset(
        num_records=100,
        customer_count=20,
        seed=42,
        output_dir=str(tmp_path)
    )

    duplicate_count = events["event_id"].duplicated().sum()

    assert duplicate_count == 1


def test_generator_creates_late_arriving_events(tmp_path):
    _, events = generate_dataset(
        num_records=100,
        customer_count=20,
        seed=42,
        output_dir=str(tmp_path)
    )

    event_times = pd.to_datetime(
        events["event_timestamp"],
        utc=True
    )

    ingestion_times = pd.to_datetime(
        events["ingestion_timestamp"],
        utc=True
    )

    assert (ingestion_times >= event_times).all()
    assert (ingestion_times > event_times).any()


def test_generator_creates_input_files(tmp_path):
    generate_dataset(
        num_records=100,
        customer_count=20,
        seed=42,
        output_dir=str(tmp_path)
    )

    assert (tmp_path / "events.csv").exists()
    assert (tmp_path / "customers.csv").exists()
