import os
import sqlite3


def initialize_registry(db_path="metadata/event_registry.db"):
    directory = os.path.dirname(db_path)

    if directory:
        os.makedirs(directory, exist_ok=True)

    with sqlite3.connect(db_path) as connection:
        connection.execute("""
            CREATE TABLE IF NOT EXISTS processed_events (
                event_id TEXT PRIMARY KEY,
                processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)


def get_processed_ids(db_path="metadata/event_registry.db"):
    initialize_registry(db_path)

    connection = sqlite3.connect(db_path)

    rows = connection.execute(
        "SELECT event_id FROM processed_events"
    ).fetchall()

    connection.close()

    return {row[0] for row in rows}


def register_events(event_ids, db_path="metadata/event_registry.db"):
    initialize_registry(db_path)

    connection = sqlite3.connect(db_path)

    connection.executemany(
        """
        INSERT OR IGNORE INTO processed_events (event_id)
        VALUES (?)
        """,
        [(event_id,) for event_id in event_ids]
    )

    connection.commit()
    connection.close()