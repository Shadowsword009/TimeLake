# TimeLake – Design Decisions and Trade-offs

## 1. Overview

This document explains the major architectural and implementation decisions made during the development of TimeLake.

The objective is to design a time-partitioned data lake that supports efficient analytical queries, avoids excessive small files, handles incremental ingestion, and maintains data integrity.

## 2. Partitioning Strategy

### Decision: Monthly partitioning using event time

The dataset is partitioned using:

- year
- month

The partition values are derived from event_timestamp.

Example:

```text
year=2026/
    month=1/
    month=2/
    month=3/
```

### Rationale

Monthly partitioning provides a balance between query pruning and partition management overhead.

Daily partitioning could create many small partitions when data volume is low.

Yearly partitioning would create fewer directories but could require scanning significantly more data for monthly analytical queries.

Monthly partitioning is therefore a practical compromise for the expected query patterns.

### Trade-offs

| Approach | Advantage | Disadvantage |
|---|---|---|
| Daily | Fine-grained pruning | More partitions and metadata overhead |
| Monthly | Balanced pruning and management | Less granular than daily |
| Yearly | Simple directory structure | More unnecessary data scanning |

### Late-arriving events

Events are assigned to partitions using event_timestamp rather than ingestion_timestamp.

This ensures that an event occurring in January but arriving in February is stored in the January partition.

## 3. File Format Selection

### Decision: Apache Parquet

Parquet was selected as the primary storage format.

### Rationale

- Columnar storage reduces unnecessary column reads.
- Supports compression.
- Supports metadata and row-group statistics.
- Integrates with DuckDB and PyArrow.
- Suitable for analytical workloads.

CSV was retained as the source ingestion format because it is simple and widely supported.

### Trade-off

Parquet is more suitable for analytical queries, but requires a schema-aware reader and writer compared with plain CSV.

## 4. Compression Strategy

### Decision: Snappy compression

Snappy was selected for Parquet files.

### Rationale

Snappy provides a practical balance between compression efficiency and read/write speed.

For analytical workloads, fast decompression is important because queries may repeatedly scan stored data.

### Trade-off

Snappy may produce larger files than compression algorithms prioritizing maximum compression ratios.

The selected approach favors practical query performance over maximum storage reduction.

## 5. File Sizing Strategy

### Decision: Byte-aware file sizing

TimeLake uses an estimated compressed bytes-per-row approach instead of relying exclusively on fixed row counts.

Configuration:

| Parameter | Value |
|---|---:|
| Target file size | 128 MB |
| Minimum file size | 32 MB |
| Maximum file size | 256 MB |

### Algorithm

1. Sample up to 1,000 records.
2. Write the sample to an in-memory Parquet buffer.
3. Estimate compressed bytes per row.
4. Calculate the initial rows-per-file value.
5. Write data in chunks.
6. Measure the actual output file size.
7. Split oversized chunks recursively.

### Why not fixed row counts?

Rows can have different storage requirements depending on their data types, string lengths, null values, and compression characteristics.

A fixed row count does not guarantee a predictable output file size.

Byte-aware sizing adapts the initial chunk size to the observed data.

### Trade-offs

- Sampling introduces estimation overhead.
- Actual compression ratios may differ from the sample.
- Recursive splitting adds additional writes for oversized chunks.
- A single unusually large record cannot be split further.
- The minimum size is an advisory compaction threshold rather than a strict write constraint.

## 6. Compaction Strategy

### Decision: Partition-preserving compaction

Compaction combines smaller files within their existing year/month partitions.

The shared byte-aware writer is used to generate appropriately sized output files.

### Rationale

Excessive small files increase file metadata overhead and can reduce analytical query efficiency.

Compaction reduces the number of files while retaining the original partition structure.

### Benchmark evidence

| Metric | Before | After |
|---|---:|---:|
| Parquet files | 24 | 6 |
| Records | 98,012 | 98,012 |
| Monthly partitions | 6 | 6 |

The benchmark reduced the file count by 75% without losing records.

## 7. Incremental Processing Strategy

### Decision: SQLite-based event registry

A SQLite registry tracks previously processed event IDs.

Before writing a new batch, incoming events are compared against the registry.

Previously processed events are excluded.

### Rationale

Reprocessing the complete dataset for every new batch would waste processing time and storage resources.

An event registry enables the pipeline to process only new records.

### Trade-offs

SQLite is simple and reliable for a local prototype, but it is not intended to coordinate distributed concurrent writers.

A production deployment could use a transactional metadata service or a distributed catalog.

## 8. Idempotency Strategy

### Decision: Deterministic batch identifiers and content fingerprints

The batch identifier is generated using SHA-256 over sorted event IDs.

A separate SHA-256 fingerprint is calculated from normalized batch content.

Normalization includes:
- Sorting columns.
- Sorting rows by event_id.
- Consistent JSON serialization.

### Retry behaviour

| Scenario | Behaviour |
|---|---|
| Same event IDs and same content | Accept as successful retry |
| Same event IDs but changed content | Reject |
| Same content with different row order | Accept |
| Different event IDs | Generate a different batch ID |

### Rationale

Checking only event IDs is insufficient because the contents associated with an existing event ID could change.

The content fingerprint protects against silently accepting modified batches during retries.

## 9. Storage Publication and Recovery

### Decision: Staging and backup directories

Full-refresh storage and compaction use staging and backup directories.

The process is:

1. Write the new dataset to a staging directory.
2. Verify stored record counts.
3. Move the existing output to a backup directory.
4. Publish the staging directory.
5. Remove the backup after successful publication.
6. Restore the previous output if publication fails.

Incremental storage publishes each deterministic batch directory separately.

### Rationale

Writing directly into the final output location could leave partially written datasets after a failure.

Staging allows validation before publication.

### Limitations

The implementation targets a local filesystem.

Filesystem publication and SQLite registry updates are not one distributed transaction.

Concurrent writers are not coordinated.

Object-storage atomicity and distributed transaction guarantees are outside the current implementation scope.

## 10. Query Engine Selection

### Decision: DuckDB

DuckDB was selected for querying Parquet datasets directly.

### Rationale

- Reads Parquet without requiring a separate database server.
- Supports Hive-style partition discovery.
- Supports partition pruning.
- Provides execution plans through EXPLAIN ANALYZE.
- Supports analytical aggregations.

### Benchmark evidence

A January query against the compacted dataset produced:

| Metric | Result |
|---|---:|
| Total files | 6 |
| Files scanned | 1 |
| Files skipped | 5 |
| Matching events | 16,820 |
| Data read | 86.8 KB |

DuckDB applied the month partition filter and scanned only the January file.

## 11. Observability Strategy

### Decision: SQLite-based pipeline run tracking

A separate SQLite database records pipeline execution details.

Tracked information includes:
- Run ID.
- Pipeline type.
- Start and end timestamps.
- Execution status.
- Input records.
- Valid records.
- Rejected records.
- Duplicate records.
- Output records.
- Parquet file count.
- Error message.

### Rationale

Pipeline observability makes execution outcomes measurable and helps diagnose failures.

It also provides an audit trail for full-refresh and incremental runs.

## 12. Testing Strategy

Pytest is used for automated unit and regression testing.

The test suite covers:

- Synthetic data generation.
- Data ingestion.
- Data validation.
- File sizing.
- Parquet storage.
- Incremental processing observability.
- Registry behaviour.
- Run tracking.
- Compaction.
- Storage recovery.

The current test suite contains 42 passing tests.

## 13. Known Limitations

The current prototype has the following limitations:

1. Input ingestion expects fixed CSV filenames.
2. SQLite registry operations are designed for a single-writer local environment.
3. Filesystem and SQLite operations do not share a distributed transaction.
4. Object-storage integration is not implemented.
5. File sizing relies on a representative sample.
6. Benchmark results are based on a synthetic dataset of approximately 98,000 valid events.
7. Large-scale distributed performance has not been evaluated.

## 14. Future Improvements

Potential future enhancements include:

- Object-storage integration.
- Distributed processing.
- Concurrent writer coordination.
- Schema evolution and version management.
- Automated compaction scheduling.
- Partition statistics and metadata manifests.
- Larger-scale performance benchmarking.
- Improved incremental batch manifests and recovery coordination.

## 15. Conclusion

TimeLake prioritizes predictable storage behaviour, time-based query pruning, data integrity, and recoverable incremental processing.

The selected architecture provides a practical local prototype while explicitly documenting the trade-offs and limitations that would need to be addressed in a distributed production environment.