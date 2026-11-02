# AI Prompt History
## TimeLake – Time-Partitioned Data Lake

The following prompts document the AI-assisted development process, from understanding the assignment and designing the architecture to implementation, testing, benchmarking and preparing the final project.

1. I have received a 72-hour data engineering coding challenge. The problem statement is to design a partitioning and file-sizing strategy for a time-partitioned data lake. Help me understand the requirements, expected deliverables and how the solution should demonstrate partition pruning and avoid excessive small files.

2. I want to implement this project using Python and locally runnable tools. Suggest a suitable technology stack that is simple to set up but still demonstrates proper data engineering concepts.

3. Help me design the architecture for TimeLake. I want to understand the complete flow from data generation and ingestion to validation, transformation, partitioning, Parquet storage and analytical querying.

4. Help me create a clean project folder structure. Separate source code, scripts, tests, configuration, sample data, generated lake outputs, metadata and documentation.

5. Let's start with synthetic data generation. Create a deterministic data generator for customer and event datasets. Include event timestamps, ingestion timestamps, event types and transaction amounts.

6. Introduce realistic data-quality issues into the generated dataset, including duplicate events, missing identifiers, invalid customer references, invalid event types and invalid transaction amounts.

7. Implement CSV ingestion for the event and customer datasets. Validate the required columns and handle missing or empty input files appropriately.

8. Help me implement a data validation module. Check mandatory fields, customer references, timestamps, event types and transaction amounts. Invalid records should be quarantined with clear rejection reasons.

9. Implement event deduplication using event_id. Make sure duplicate records are handled deterministically and the earliest ingestion timestamp is retained.

10. Implement the transformation layer. Convert timestamps to UTC, derive event_date, year and month, and prepare the dataset for time-based partitioning.

11. Help me implement partitioned Parquet storage using year and month derived from event_timestamp. Explain why event time should determine partition placement instead of ingestion time.

12. Design a file-sizing strategy for the Parquet writer. I want configurable target and maximum file sizes, compression and a way to estimate rows per file based on actual data characteristics.

13. Implement a byte-aware Parquet writer using PyArrow. It should estimate compressed bytes per row, divide data into chunks and recursively split files that exceed the maximum size.

14. Add safe publication to the storage layer. Use staging and backup directories, verify the written record counts and restore the previous dataset if publication fails.

15. Help me design a compaction strategy for small Parquet files. Compact files within the same time partition, preserve all records and verify the resulting dataset.

16. Implement the compaction module using the shared file-sizing logic. Support staging, backup and rollback so that failed compaction does not destroy the existing output.

17. I want to demonstrate partition pruning using DuckDB. Create a benchmark that compares a time-filtered query on partitioned Parquet data against an equivalent query on an unpartitioned Parquet file.

18. Generate a larger benchmark dataset with 100,000 base events. Measure query execution time, data read, files scanned and query results. Make sure both datasets return equivalent results.

19. Help me inspect DuckDB query plans using EXPLAIN ANALYZE. I want to demonstrate that monthly partition filters reduce the number of Parquet files scanned.

20. After compaction, verify that the number of files has decreased without losing records. Also demonstrate that partition pruning continues to work on the compacted dataset.

21. I want to support late-arriving events. Demonstrate a case where an event generated in January arrives in February and verify that it is stored in the January partition using event_timestamp.

22. Help me design an incremental ingestion workflow. It should identify previously processed event IDs, filter new events and write only new records without reprocessing the existing dataset.

23. Implement an SQLite event registry to track processed event IDs. Make registration safe against duplicate entries and ensure that the registry persists between pipeline executions.

24. Make incremental storage idempotent. Use deterministic batch identification and content fingerprints so that an identical retry is accepted while changed content for previously published event IDs is rejected.

25. What happens if a failure occurs after a batch is published but before the event registry is updated? Help me design a recovery mechanism that allows the same batch to be retried safely.

26. Add pipeline observability. Track run IDs, timestamps, pipeline types, execution status, input records, valid records, rejected records, duplicates, output records and generated Parquet file counts using SQLite.

27. Make sure failed pipeline executions are recorded with their error information. Also ensure that successful runs update their metrics only after the corresponding storage operation succeeds.

28. Help me write automated tests for the major components. Cover data generation, ingestion, validation, file sizing, storage, compaction, event registry, incremental processing and run tracking.

29. I encountered issues while running the tests. Help me understand the terminal errors and fix them without breaking existing functionality. Ensure that pytest collects only the intended tests and does not execute standalone demonstration scripts during collection.

30. Help me test storage recovery. Simulate failures during publication and verify that the previous valid dataset remains available.

31. Review the incremental pipeline for idempotency and recovery. Test identical retries, changed content, duplicate event IDs and failures between batch publication and registry updates.

32. Run the complete test suite and help me verify that all tests pass. Identify any warnings, environment-specific issues or test collection problems.

33. Help me document the source and target schemas. Include event fields, customer fields, derived partition columns, data types, validation rules and the purpose of each field.

34. Create an architecture document explaining the complete TimeLake processing flow, its components, storage layout and interactions between the full-refresh and incremental pipelines.

35. Help me document the major design decisions and trade-offs in TimeLake. Explain monthly partitioning, Parquet, Snappy compression, file sizing, compaction, incremental processing, idempotency, recovery and observability.

36. Help me prepare a README for GitHub. Include the problem statement, solution overview, architecture, technology stack, project structure, setup instructions, execution commands, benchmark results, testing instructions, design decisions and limitations. Make sure the documentation reflects the actual implementation.

37. Review the project structure and identify unnecessary or empty files. Check whether modules are actually used before recommending new abstractions or refactoring existing working code.

38. Help me prepare the .gitignore file. Identify generated CSV files, Parquet datasets, SQLite databases, Python caches, virtual environments, logs and temporary files that should not be committed, while preserving source code, tests and documentation.

39. Review the complete project before GitHub submission. Verify the project structure, test results, generated artifacts, documentation and repository contents.

40. Help me prepare the final GitHub repository. Explain how to initialize Git, stage the correct files, create meaningful commits, connect the remote repository and push the project.

41. Review the final submission against the YottaFlex guidelines. Check that the repository is publicly accessible, setup and verification instructions are clear, benchmark claims are supported by actual results and AI prompt history is included.

