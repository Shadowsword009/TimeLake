import sqlite3

from src.timelake.registry import (
    initialize_registry,
    get_processed_ids,
    register_events
)


def test_registry_creates_database_in_current_directory(
    tmp_path,
    monkeypatch
):
    monkeypatch.chdir(tmp_path)

    db_path = "event_registry.db"

    initialize_registry(db_path)

    assert (tmp_path / db_path).exists()


def test_register_and_retrieve_events(tmp_path):
    db_path = str(tmp_path / "metadata" / "event_registry.db")

    register_events(
        ["EVT001", "EVT002", "EVT003"],
        db_path
    )

    processed_ids = get_processed_ids(db_path)

    assert processed_ids == {
        "EVT001",
        "EVT002",
        "EVT003"
    }


def test_register_events_ignores_duplicates(tmp_path):
    db_path = str(tmp_path / "event_registry.db")

    register_events(
        ["EVT001", "EVT002"],
        db_path
    )

    register_events(
        ["EVT001", "EVT003"],
        db_path
    )

    processed_ids = get_processed_ids(db_path)

    assert processed_ids == {
        "EVT001",
        "EVT002",
        "EVT003"
    }