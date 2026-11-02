import os
import shutil
import pandas as pd

from src.timelake.storage import write_partitioned_data


output_dir = "data/recovery_test/events"
staging_dir = output_dir + ".staging"
backup_dir = output_dir + ".backup"

for path in [output_dir, staging_dir, backup_dir]:
    if os.path.exists(path):
        shutil.rmtree(path)

os.makedirs(output_dir, exist_ok=True)

with open(os.path.join(output_dir, "previous_version.txt"), "w") as file:
    file.write("Previous dataset is safe")

events = pd.DataFrame({
    "event_id": ["EVT001"],
    "customer_id": ["CUST001"],
    "event_timestamp": pd.to_datetime(
        ["2026-01-01 10:00:00"],
        utc=True
    ),
    "year": [2026],
    "month": [1],
    "amount": [100.0]
})

original_replace = os.replace
failure_triggered = False


def simulate_failure(source, destination):
    global failure_triggered

    if (
        source == staging_dir
        and destination == output_dir
        and not failure_triggered
    ):
        failure_triggered = True
        raise OSError("Simulated publishing failure")

    return original_replace(source, destination)


os.replace = simulate_failure

try:
    write_partitioned_data(
        events,
        output_dir=output_dir,
        max_rows_per_file=50
    )
except OSError as error:
    print("\nExpected failure:", error)
finally:
    os.replace = original_replace

previous_file = os.path.join(
    output_dir,
    "previous_version.txt"
)

print("\nRecovery verification")
print("---------------------")
print("Previous dataset exists:", os.path.exists(previous_file))
print("Staging directory exists:", os.path.exists(staging_dir))
print("Backup directory exists:", os.path.exists(backup_dir))

if not os.path.exists(previous_file):
    raise RuntimeError("Previous dataset was not recovered")

if os.path.exists(staging_dir):
    raise RuntimeError("Staging directory was not cleaned")

if os.path.exists(backup_dir):
    raise RuntimeError("Backup directory was not restored correctly")

print("\nFailure recovery test passed")