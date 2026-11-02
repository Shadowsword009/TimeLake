# TimeLake

**A time-partitioned data lake with data validation, file sizing, compaction, incremental processing, and query pruning.**

## 1. Problem Statement

Design a partitioning and file-sizing strategy for a time-partitioned data lake. The solution should demonstrate partition pruning, avoid excessive small files, and document trade-offs for common query patterns.

TimeLake implements a local data lake pipeline using Python, Pandas, PyArrow, Parquet, DuckDB, and SQLite.

## 2. Solution Overview

The project demonstrates:

- CSV ingestion and schema validation.
- Data quality checks and quarantine of invalid records.
- Duplicate detection and removal.
- Time-based partitioning using event timestamps.
- Size-aware Parquet file writing.
- Compaction of small files.
- Incremental processing with an event registry.
- Idempotent batch writes and recovery handling.
- Query execution and partition-pruning verification.
- Pipeline run tracking and operational metrics.

## 3. Architecture

```text
                 Source CSV Files
                        |
                        v
                   Ingestion
                        |
                        v
                Data Validation
                  /         \
                 v           v
          Valid Records   Quarantine
                 |
                 v
             Deduplication
                 |
                 v
             Transformation
                 |
                 v
          Time Partitioning
           (Year / Month)
                 |
                 v
          Size-Aware Storage
                 |
                 v
          Curated Parquet Lake
                 |
          +------+------+
          |             |
          v             v
       Queries       Compaction
          |             |
          v             v
       DuckDB       Compacted Lake
          
       Incremental Processing
                 |
                 v
           Event Registry
                 |
                 v
          Idempotent Storage

          Pipeline Run Tracker
                 |
                 v
             SQLite
```

## 4. Technology Stack

| Component | Technology |
|---|---|
| Programming language | Python |
| Data processing | Pandas |
| Columnar storage | Apache Parquet |
| Parquet engine | PyArrow |
| Query engine | DuckDB |
| Metadata and tracking | SQLite |
| Testing | Pytest |

## 5. Project Structure

```text
TimeLake/
├── config/
│   └── settings.yaml
├── data/
│   ├── input/
│   ├── staging/
│   ├── quarantine/
│   ├── benchmark_input/
│   ├── benchmark_quarantine/
│   └── recovery_test/
├── docs/
│   ├── architecture.md
│   ├── data_dictionary.md
│   └── design_decisions.md
├── lake/
│   ├── curated/
│   ├── benchmark/
│   ├── compacted/
│   ├── incremental_test/
│   └── late_arrival_test/
├── metadata/
├── reports/
├── scripts/
├── src/
│   └── timelake/
├── tests/
├── .gitignore
├── pytest.ini
├── README.md
├── prompts.md
└── requirements.txt
```

The `data/`, `lake/`, `metadata/`, and `reports/` directories contain generated data and runtime outputs. They are excluded from version control.

`config/settings.yaml` is retained as a reference configuration. The current implementation uses function-level defaults rather than loading this file at runtime.

## 6. Data Model

### Events

| Column | Description |
|---|---|
| event_id | Unique event identifier |
| source_file | Source file name |
| customer_id | Customer reference |
| event_timestamp | Time the event occurred |
| ingestion_timestamp | Time the event entered the pipeline |
| event_type | Type of event |
| amount | Event amount |

### Customers

| Column | Description |
|---|---|
| customer_id | Unique customer identifier |
| signup_timestamp | Customer registration timestamp |

The event timestamp determines the storage partition. The ingestion timestamp is retained separately to support late-arriving event analysis.

## 7. Partitioning Strategy

The curated data is partitioned by event year and month.

```text
lake/curated/events/
├── year=2026/
│   ├── month=1/
│   ├── month=2/
│   ├── month=3/
│   ├── month=4/
│   ├── month=5/
│   └── month=6/
```

**Why monthly partitioning?**

- Supports common date-range queries.
- Enables partition pruning for month-specific queries.
- Avoids creating a separate partition for every day.
- Provides a practical balance between query efficiency and partition count.

The partition is derived from `event_timestamp`, not `ingestion_timestamp`. This ensures that late-arriving records are stored according to when the event actually occurred.

## 8. File Sizing and Compaction

The storage implementation uses size-aware Parquet writing.

| Parameter | Value |
|---|---:|
| Target file size | 128 MB |
| Advisory minimum file size | 32 MB |
| Maximum file size | 256 MB |
| Compression | Snappy |

The writer estimates compressed bytes per row and uses that estimate to determine batch sizes. If an output file exceeds the configured maximum, the batch is split further.

The minimum size is advisory rather than strictly enforced. Small files are handled through compaction.

Compaction combines smaller files within each partition while preserving the records and partition structure.

A single record may exceed the maximum target size by itself; the writer cannot split an individual row.

## 9. Incremental Processing and Recovery

TimeLake supports incremental ingestion using an SQLite event registry.

The registry tracks previously processed event IDs so that subsequent runs can identify new records.

The incremental workflow:

1. Ingests and validates incoming records.
2. Removes duplicates.
3. Checks the event registry.
4. Selects records that have not already been processed.
5. Writes the new records as an incremental batch.
6. Registers processed event IDs after successful publication.
7. Records the run outcome and metrics.

Batch identifiers are deterministic. If a previously published batch is retried with identical content, the existing output is reused. Changed content for the same published batch is rejected.

The implementation also includes recovery testing for failures between publishing data and updating the event registry.

## 10. Late-Arriving Events

Late-arriving events are supported by separating event time from ingestion time.

For example, an event that occurred in January but arrived in February is stored under:

```text
year=2026/month=1/
```

This keeps event-time queries accurate while retaining the ingestion timestamp for operational analysis.

## 11. Data Quality

The validation stage checks:

- Required event and customer identifiers.
- Valid customer references.
- Timestamp validity.
- Allowed event types.
- Numeric and non-negative amounts.
- Event-specific amount rules.

Invalid records are written to a quarantine location with rejection reasons. Valid records continue through deduplication and transformation.

## 12. Observability

Pipeline execution details are recorded in SQLite.

Tracked metrics include:

- Run ID and timestamps.
- Run status and pipeline type.
- Input, valid, rejected, duplicate, and output record counts.
- Number of Parquet files written.
- Error details for failed runs.

This provides a basic record of pipeline execution and failure recovery.

## 13. Setup and Execution

### Prerequisites

- Python 3.13 or a compatible Python version.
- Git.

### Clone the repository

```powershell
git clone https://github.com/Shadowsword009/TimeLake.git
cd TimeLake
```

### Create and activate a virtual environment

```powershell
python -m venv .venv
Set-ExecutionPolicy -Scope Process -ExecutionPolicy Bypass
.\.venv\Scripts\Activate.ps1
```

### Install dependencies

```powershell
pip install -r requirements.txt
```

### Generate sample data

```powershell
python -m scripts.generate_data
```

### Run the full-refresh pipeline

```powershell
python -m scripts.run_pipeline
```

### Run the test suite

```powershell
python -m pytest
```

### Run the benchmark

```powershell
python -m scripts.benchmark
python -m scripts.benchmark_queries
```

### Run compaction

```powershell
python -m scripts.compact
```

The benchmark and test scripts use separate generated-data locations so that they can be evaluated independently of the primary pipeline.

## 14. Benchmark Results

The benchmark uses a generated dataset covering six months.

### Data processing

| Metric | Result |
|---|---:|
| Input records | 101,000 |
| Rejected records | 2,005 |
| Duplicate records removed | 983 |
| Final valid unique records | 98,012 |
| Monthly partitions | 6 |
| Files before compaction | 24 |
| Files after compaction | 6 |

### January query comparison

The same January aggregation was executed against partitioned and unpartitioned Parquet data.

| Metric | Partitioned | Unpartitioned |
|---|---:|---:|
| Result count | 16,820 | 16,820 |
| Average execution time | 6.000 ms | 6.589 ms |
| Median execution time | 5.870 ms | 6.635 ms |
| Data read | 133.8 KB | 443.7 KB |
| Files scanned | 4 / 24 | 1 / 1 |

The partitioned query read approximately **69.9% less data** in this benchmark.

The measured average execution time was approximately 11.5% lower. These timings are from a small, warm-cache benchmark and should not be treated as a general performance guarantee.

### Compaction and pruning

Compaction reduced the benchmark output from 24 files to 6 while preserving all 98,012 records.

A January query against the compacted dataset scanned 1 of 6 files and read approximately 86.8 KB.

These results demonstrate the effect of time partitioning and file consolidation on the tested query pattern.

## 15. Testing

The project includes tests for:

- Data generation.
- Ingestion and schema validation.
- Data quality validation.
- File sizing.
- Parquet storage.
- Compaction.
- Incremental processing.
- Event registry behavior.
- Pipeline run tracking.
- Recovery and observability.

**Latest test result: 42 passed.**

Run the tests with:

```powershell
python -m pytest
```

## 16. Limitations and Future Improvements

Current limitations:

- Storage is implemented for a local filesystem.
- Concurrent writers are not coordinated.
- The event registry is maintained in SQLite.
- File-size estimation is based on a sample and actual compression can vary.
- The minimum file size is advisory, not strictly enforced.
- The current configuration YAML is not loaded by the application.
- Performance results are based on a generated dataset and a limited benchmark.

Potential improvements:

- Support object storage such as Amazon S3 or Azure Data Lake Storage.
- Add concurrent-writer coordination.
- Introduce configurable runtime settings.
- Evaluate partition sizing with larger datasets and additional query patterns.
- Add automated scheduling and monitoring.

## 17. AI-Assisted Development

AI tools were used during development for problem analysis, architectural discussions, implementation assistance, debugging, test design, benchmarking, and documentation.

The prompt history is provided in [`PROMPTS.md`](PROMPTS.md).

The implementation, design decisions, test results, and benchmark outputs were reviewed and validated during development. The repository includes the supporting code and documentation needed to inspect the solution.

## 18. Repository

GitHub: https://github.com/Shadowsword009/TimeLake
