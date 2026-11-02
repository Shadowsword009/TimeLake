from src.timelake.compaction import compact_partitioned_data


def main():
    compact_partitioned_data(
        input_dir="lake/benchmark/events",
        output_dir="lake/compacted/events",
        target_size_mb=128,
        max_size_mb=256,
        compression="snappy"
    )


if __name__ == "__main__":
    main()