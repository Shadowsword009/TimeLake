from src.timelake.registry import (
    initialize_registry,
    get_processed_ids,
    register_events
)


initialize_registry()

register_events(["EVT001", "EVT002", "EVT003"])

print("Processed IDs:")
print(sorted(get_processed_ids()))

register_events(["EVT002", "EVT003", "EVT004"])

print("\nAfter registering duplicates and a new event:")
print(sorted(get_processed_ids()))