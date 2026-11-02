from src.timelake.queries import (
    monthly_event_summary,
    monthly_filtered_query
)


print("\nMonthly summary")
print(monthly_event_summary().to_string(index=False))

print("\nJanuary query")
print(monthly_filtered_query(1).to_string(index=False))