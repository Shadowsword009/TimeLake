# TimeLake – Data Dictionary

## 1. Project Overview

TimeLake is a time-partitioned data lake prototype using synthetic e-commerce event data.

It demonstrates:
- CSV ingestion and data validation.
- Duplicate detection and event deduplication.
- Time-based partitioning.
- Apache Parquet storage with compression.
- Byte-aware file sizing and compaction.
- Incremental ingestion and idempotent processing.
- Query pruning and performance benchmarking.
- Pipeline execution tracking and recovery.

## 2. Source Data

### 2.1 Events Dataset

The source events dataset is stored in CSV format.

| Column | Data Type | Description |
|---|---|---|
| event_id | STRING | Unique event identifier |
| source_file | STRING | Name of the originating source file |
| customer_id | STRING | Customer identifier |
| event_timestamp | TIMESTAMP | Time when the event occurred |
| ingestion_timestamp | TIMESTAMP | Time when the event entered the ingestion system |
| event_type | STRING | Type of customer activity |
| amount | DOUBLE | Transaction amount |

### 2.2 Customers Dataset

Customer reference data is stored separately.

| Column | Data Type | Description |
|---|---|---|
| customer_id | STRING | Unique customer identifier |
| signup_timestamp | TIMESTAMP | Customer registration timestamp |

## 3. Target Data

The curated event dataset is stored in Apache Parquet format using Snappy compression.

| Column | Data Type | Description |
|---|---|---|
| event_id | STRING | Unique event identifier |
| source_file | STRING | Original source file |
| customer_id | STRING | Customer identifier |
| event_timestamp | TIMESTAMP | Event occurrence timestamp |
| ingestion_timestamp | TIMESTAMP | Event ingestion timestamp |
| event_type | STRING | Customer activity type |
| amount | DOUBLE | Transaction amount |
| event_date | DATE | Date derived from event_timestamp |
| year | INTEGER | Year derived from event_timestamp |
| month | INTEGER | Month derived from event_timestamp |

## 4. Primary Key and Deduplication

- event_id is the logical primary key.
- Duplicate event IDs are detected during processing.
- When duplicates exist, the record with the earliest ingestion_timestamp is retained.
- Incremental processing maintains a SQLite registry of previously processed event IDs.
- Batch fingerprints detect changes to previously published batch contents.

## 5. Supported Event Types

- login
- product_view
- add_to_cart
- purchase
- cancellation

## 6. Data Quality Rules

- event_id and customer_id are mandatory.
- customer_id must reference an existing customer.
- event_timestamp and ingestion_timestamp must be valid timestamps.
- event_type must belong to the supported event types.
- amount must be numeric and non-negative.
- login and product_view events must have an amount of zero.
- Invalid records are written to quarantine with rejection reasons.

## 7. Partitioning Strategy

The curated dataset uses event-time partitioning.

Partition hierarchy:

year=YYYY/month=MM/

Example:

lake/curated/events/
    year=2026/
        month=1/
        month=2/
        month=3/

Partition columns:
- year
- month

Both columns are derived from event_timestamp.

Late-arriving events are assigned to partitions according to their event_timestamp, not their ingestion_timestamp.

## 8. File Sizing and Compaction

- Storage format: Apache Parquet.
- Compression: Snappy.
- Target file size: 128 MB.
- Maximum file size: 256 MB.
- Minimum file size: 32 MB (advisory threshold for compaction).

The file-sizing algorithm estimates compressed bytes per row and determines an appropriate chunk size.

Actual file sizes are checked after writing. Oversized chunks are split recursively where possible.

Compaction combines smaller files within the same monthly partition while preserving records.

## 9. Incremental Processing

- Processed event IDs are tracked in SQLite.
- Previously processed events are excluded from subsequent batches.
- Batch identifiers are deterministic.
- Batch content is verified using SHA-256 fingerprints.
- Published batches are stored separately under batch-specific directories.
- A retry of an already published identical batch does not create duplicate files.

## 10. Pipeline Observability

Pipeline execution metadata is stored separately in SQLite.

Tracked attributes include:
- Run ID
- Pipeline type
- Start and end timestamps
- Execution status
- Input record count
- Valid record count
- Rejected record count
- Duplicate record count
- Output record count
- Parquet file count
- Error message

Run metadata is not embedded in the event Parquet schema.

## 11. Assumptions and Limitations

- All generated data is synthetic.
- Event timestamps are interpreted in UTC.
- Customer identifiers are validated against a customer reference dataset.
- Late-arriving events are assigned using event time.
- The prototype uses a local filesystem and SQLite metadata stores.
- Atomic publishing and recovery are implemented for local filesystem operations.
- Distributed concurrent writers and object-storage transaction guarantees are outside the current implementation scope.