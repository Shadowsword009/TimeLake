# AI Prompt History
## TimeLake – Time-Partitioned Data Lake

The following prompts document the AI-assisted development process, from understanding the assignment and designing the architecture to implementation, testing, benchmarking and preparing the final project.

1. I have received a 72-hour coding challenge from YottaFlex.ai. The problem is to design a partitioning and file-sizing strategy for a time-partitioned data lake. Help me understand what exactly they expect and what I should demonstrate in the solution.

2. I want to build this locally using Python. Suggest a technology stack that is practical for this challenge without introducing unnecessary infrastructure or complicated dependencies.

3. Before writing code, let's design the architecture. I want the pipeline to cover ingestion, validation, transformation, partitioning, Parquet storage and querying.

4. Help me organize the project into a proper folder structure. I want separate folders for source code, scripts, tests, data, lake outputs, metadata and documentation.

5. Let's start with generating synthetic data. Create customer and event datasets with realistic timestamps, event types and transaction amounts. The generator should produce repeatable results.

6. We also need some bad records to demonstrate data quality handling. Add duplicate events, missing identifiers, invalid customer references and invalid transaction amounts.

7. Now implement the ingestion module. It should read the CSV files and check whether the required columns are present. Handle missing and empty files properly.

8. Let's implement validation. Check customer references, timestamps, event types and amounts. I also want rejected records to be written to a separate quarantine folder with the reason for rejection.

9. Implement deduplication using event_id. If duplicates exist, retain the record with the earliest ingestion timestamp.

10. Now work on the transformation layer. Convert timestamps to UTC and derive the date, year and month fields required for partitioning.

11. Implement the Parquet storage layer. I want the output organized by year and month, based on event_timestamp. Explain why event time is a better partitioning key than ingestion time for this use case.

12. Let's work on file sizing. A fixed number of rows per file may not produce consistent file sizes, so I want a strategy based on estimated compressed data size.

13. Implement the size-aware Parquet writer using PyArrow. It should estimate the compressed bytes per row, calculate chunk sizes and split a batch further if the resulting file exceeds the maximum size.

14. What happens if writing succeeds but publishing the output fails? Add staging and backup handling so the previous valid dataset is not lost.

15. I also need a compaction strategy. The benchmark will produce several small files, so implement a way to combine files within the same partition without losing records.

16. Make compaction use the same file-sizing logic as the main storage layer. It should verify record counts and handle failures safely.

17. Now we need to demonstrate partition pruning. Use DuckDB to compare a query against partitioned Parquet files with an equivalent query against an unpartitioned file.

18. Generate a larger dataset with 100,000 base events so the benchmark is more meaningful. Compare the query results and measure execution time and data read.

19. I want to see actual evidence that partition pruning is happening. Use EXPLAIN ANALYZE and show how many files are scanned for a month-specific query.

20. Let's verify compaction as well. Check that the file count decreases, all records are preserved and partition pruning still works after compaction.

21. We should also demonstrate late-arriving data. Create an event that occurred in January but arrived in February and verify that it is stored in the January partition.

22. The assignment should demonstrate more than a full refresh. Let's add incremental processing so that new events can be processed without rewriting the entire dataset.

23. Use SQLite for an event registry. It should keep track of processed event IDs between runs and avoid registering the same event more than once.

24. Make incremental writes idempotent. If the same batch is submitted again, it should not create duplicate output. If the content changes for already published event IDs, reject it.

25. Consider a failure after the Parquet batch has been published but before the event registry is updated. How can we retry the batch without creating duplicate data?

26. Let's add pipeline observability. Track run IDs, timestamps, pipeline type, status and record counts. Store the execution details in SQLite.

27. Make sure failed runs are recorded too. I want the error message and run status to be persisted, and output metrics should only be updated after successful storage.

28. Now let's add automated tests for the individual modules. Cover generation, ingestion, validation, storage, file sizing, compaction, incremental processing and run tracking.

29. I'm encountering issues while running the tests. I'll share the terminal output. Help me identify the actual problem and fix it without unnecessarily changing working code.

30. Some standalone scripts have names beginning with test_. Pytest is collecting them along with the actual test suite. Configure test discovery so only the intended tests are collected.

31. Let's test storage recovery by simulating a failure during publication. Verify that the previous dataset remains intact.

32. Test the incremental pipeline more thoroughly. I want to verify identical retries, changed batch content, duplicate event IDs and recovery when registry registration fails after publishing.

33. Review the SQLite registry implementation. Make sure it handles database creation correctly, including when a database filename is provided without a parent directory.

34. Let's run the complete test suite. Check the number of passing tests and whether there are any warnings or collection issues.

35. Help me prepare the data dictionary. Document the source schemas, target schema, field descriptions, derived partition columns and validation rules.

36. Now prepare the architecture documentation. Explain how the components work together, including the full-refresh pipeline, incremental pipeline, storage and metadata tracking.

37. Document the design decisions and trade-offs. I want to explain monthly partitioning, Parquet, compression, file sizing, compaction, incremental processing, idempotency and recovery.

38. Let's review the implementation before finalizing it. Check for unnecessary modules and abstractions. I prefer keeping the working implementation straightforward rather than refactoring everything just for the sake of structure.

39. Help me prepare the README. It should explain the problem, architecture, technology stack, setup, execution, benchmark results, tests and limitations. Make sure it reflects what the code actually does.

40. Let's review the .gitignore. I don't want generated CSV files, Parquet outputs, SQLite databases, caches or the virtual environment uploaded to GitHub. Keep the source code, scripts, tests and documentation.
