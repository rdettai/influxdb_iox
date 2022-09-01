//! IOx compactor implementation.

#![deny(rustdoc::broken_intra_doc_links, rust_2018_idioms)]
#![warn(
    missing_copy_implementations,
    missing_docs,
    clippy::explicit_iter_loop,
    clippy::future_not_send,
    clippy::use_self,
    clippy::clone_on_ref_ptr
)]

pub mod compact;
pub(crate) mod compact_hot_partitions;
pub mod garbage_collector;
pub mod handler;
pub(crate) mod parquet_file_combining;
pub(crate) mod parquet_file_filtering;
pub(crate) mod parquet_file_lookup;
pub mod query;
pub mod server;
pub mod utils;

use crate::compact::{Compactor, PartitionCompactionCandidateWithInfo};
use data_types::CompactionLevel;
use metric::Attributes;
use snafu::{ResultExt, Snafu};
use std::sync::Arc;

#[derive(Debug, Snafu)]
#[allow(missing_copy_implementations, missing_docs)]
pub(crate) enum Error {
    #[snafu(display("{}", source))]
    Lookup {
        source: parquet_file_lookup::PartitionFilesFromPartitionError,
    },

    #[snafu(display("{}", source))]
    Combining {
        source: parquet_file_combining::Error,
    },

    #[snafu(display("{}", source))]
    Upgrading {
        source: iox_catalog::interface::Error,
    },
}

/// One compaction operation of one cold partition
pub(crate) async fn compact_cold_partition(
    compactor: &Compactor,
    partition: PartitionCompactionCandidateWithInfo,
) -> Result<(), Error> {
    let start_time = compactor.time_provider.now();
    let shard_id = partition.shard_id();

    let parquet_files_for_compaction =
        parquet_file_lookup::ParquetFilesForCompaction::for_partition(
            Arc::clone(&compactor.catalog),
            partition.id(),
        )
        .await
        .context(LookupSnafu)?;

    let to_compact = parquet_file_filtering::filter_cold_parquet_files(
        parquet_files_for_compaction,
        compactor.config.cold_input_size_threshold_bytes,
        compactor.config.cold_input_file_count_threshold,
        &compactor.parquet_file_candidate_gauge,
        &compactor.parquet_file_candidate_bytes,
    );

    let compact_result =
        if to_compact.len() == 1 && to_compact[0].compaction_level == CompactionLevel::Initial {
            // upgrade the one l0 file to l1, don't run compaction
            let mut repos = compactor.catalog.repositories().await;

            repos
                .parquet_files()
                .update_to_level_1(&[to_compact[0].id])
                .await
                .context(UpgradingSnafu)?;
            Ok(())
        } else {
            parquet_file_combining::compact_parquet_files(
                to_compact,
                partition,
                Arc::clone(&compactor.catalog),
                compactor.store.clone(),
                Arc::clone(&compactor.exec),
                Arc::clone(&compactor.time_provider),
                &compactor.compaction_input_file_bytes,
                compactor.config.max_desired_file_size_bytes,
                compactor.config.percentage_max_file_size,
                compactor.config.split_percentage,
            )
            .await
            .context(CombiningSnafu)
        };

    let attributes = Attributes::from([
        ("shard_id", format!("{}", shard_id).into()),
        ("partition_type", "cold".into()),
    ]);
    if let Some(delta) = compactor
        .time_provider
        .now()
        .checked_duration_since(start_time)
    {
        let duration = compactor.compaction_duration.recorder(attributes);
        duration.record(delta);
    }

    compact_result
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::handler::CompactorConfig;
    use arrow_util::assert_batches_sorted_eq;
    use backoff::BackoffConfig;
    use data_types::{ColumnType, CompactionLevel};
    use iox_query::exec::Executor;
    use iox_tests::util::{TestCatalog, TestParquetFileBuilder};
    use iox_time::{SystemProvider, TimeProvider};
    use parquet_file::storage::ParquetStorage;
    use std::time::Duration;

    #[tokio::test]
    async fn test_compact_cold_partition_many_files() {
        test_helpers::maybe_start_logging();
        let catalog = TestCatalog::new();

        // lp1 does not overlap with any other level 0
        let lp1 = vec![
            "table,tag1=WA field_int=1000i 10",
            "table,tag1=VT field_int=10i 20",
        ]
        .join("\n");

        // lp2 overlaps with lp3
        let lp2 = vec![
            "table,tag1=WA field_int=1000i 8000", // will be eliminated due to duplicate
            "table,tag1=VT field_int=10i 10000",
            "table,tag1=UT field_int=70i 20000",
        ]
        .join("\n");

        // lp3 overlaps with lp2
        let lp3 = vec![
            "table,tag1=WA field_int=1500i 8000", // latest duplicate and kept
            "table,tag1=VT field_int=10i 6000",
            "table,tag1=UT field_int=270i 25000",
        ]
        .join("\n");

        // lp4 does not overlap with any
        let lp4 = vec![
            "table,tag2=WA,tag3=10 field_int=1600i 28000",
            "table,tag2=VT,tag3=20 field_int=20i 26000",
        ]
        .join("\n");

        // lp5 overlaps with lp1
        let lp5 = vec![
            "table,tag2=PA,tag3=15 field_int=1601i 9",
            "table,tag2=OH,tag3=21 field_int=21i 25",
        ]
        .join("\n");

        // lp6 does not overlap with any
        let lp6 = vec![
            "table,tag2=PA,tag3=15 field_int=81601i 90000",
            "table,tag2=OH,tag3=21 field_int=421i 91000",
        ]
        .join("\n");

        let ns = catalog.create_namespace("ns").await;
        let shard = ns.create_shard(1).await;
        let table = ns.create_table("table").await;
        table.create_column("field_int", ColumnType::I64).await;
        table.create_column("tag1", ColumnType::Tag).await;
        table.create_column("tag2", ColumnType::Tag).await;
        table.create_column("tag3", ColumnType::Tag).await;
        table.create_column("time", ColumnType::Time).await;
        let partition = table.with_shard(&shard).create_partition("part").await;
        let time = Arc::new(SystemProvider::new());
        let time_38_hour_ago = (time.now() - Duration::from_secs(60 * 60 * 38)).timestamp_nanos();
        let config = make_compactor_config();
        let metrics = Arc::new(metric::Registry::new());
        let compactor = Compactor::new(
            vec![shard.shard.id],
            Arc::clone(&catalog.catalog),
            ParquetStorage::new(Arc::clone(&catalog.object_store)),
            Arc::new(Executor::new(1)),
            Arc::new(SystemProvider::new()),
            BackoffConfig::default(),
            config,
            Arc::clone(&metrics),
        );

        // parquet files that are all in the same partition

        // pf1 does not overlap with any other level 0
        let builder = TestParquetFileBuilder::default()
            .with_line_protocol(&lp1)
            .with_max_seq(3)
            .with_min_time(10)
            .with_max_time(20)
            .with_file_size_bytes(compactor.config.max_desired_file_size_bytes + 10)
            .with_creation_time(time_38_hour_ago);
        partition.create_parquet_file(builder).await;

        // pf2 overlaps with pf3
        let builder = TestParquetFileBuilder::default()
            .with_line_protocol(&lp2)
            .with_max_seq(5)
            .with_min_time(8_000)
            .with_max_time(20_000)
            .with_file_size_bytes(100) // small file
            .with_creation_time(time_38_hour_ago);
        partition.create_parquet_file(builder).await;

        // pf3 overlaps with pf2
        let builder = TestParquetFileBuilder::default()
            .with_line_protocol(&lp3)
            .with_max_seq(10)
            .with_min_time(6_000)
            .with_max_time(25_000)
            .with_file_size_bytes(100) // small file
            .with_creation_time(time_38_hour_ago);
        partition.create_parquet_file(builder).await;

        // pf4 does not overlap with any but is small
        let builder = TestParquetFileBuilder::default()
            .with_line_protocol(&lp4)
            .with_max_seq(18)
            .with_min_time(26_000)
            .with_max_time(28_000)
            .with_file_size_bytes(100) // small file
            .with_creation_time(time_38_hour_ago);
        partition.create_parquet_file(builder).await;

        // pf5 was created in a previous compaction cycle; overlaps with pf1
        let builder = TestParquetFileBuilder::default()
            .with_line_protocol(&lp5)
            .with_max_seq(1)
            .with_min_time(9)
            .with_max_time(25)
            .with_file_size_bytes(100) // small file
            .with_creation_time(time_38_hour_ago)
            .with_compaction_level(CompactionLevel::FileNonOverlapped);
        partition.create_parquet_file(builder).await;

        // pf6 was created in a previous compaction cycle; does not overlap with any
        let builder = TestParquetFileBuilder::default()
            .with_line_protocol(&lp6)
            .with_max_seq(20)
            .with_min_time(90000)
            .with_max_time(91000)
            .with_file_size_bytes(100) // small file
            .with_creation_time(time_38_hour_ago)
            .with_compaction_level(CompactionLevel::FileNonOverlapped);
        partition.create_parquet_file(builder).await;

        // should have 4 level-0 files before compacting
        let count = catalog.count_level_0_files(shard.shard.id).await;
        assert_eq!(count, 4);

        // ------------------------------------------------
        // Compact
        let candidates = compactor
            .cold_partitions_to_compact(compactor.config.max_number_partitions_per_shard)
            .await
            .unwrap();
        let mut candidates = compactor.add_info_to_partitions(&candidates).await.unwrap();

        assert_eq!(candidates.len(), 1);
        let c = candidates.pop_front().unwrap();

        compact_cold_partition(&compactor, c).await.unwrap();

        // Should have 3 non-soft-deleted files:
        //
        // - the level 1 file that didn't overlap with anything
        // - the two newly created after compacting and splitting pf1, pf2, pf3, pf4, pf5
        let mut files = catalog.list_by_table_not_to_delete(table.table.id).await;
        assert_eq!(files.len(), 3);
        let files_and_levels: Vec<_> = files
            .iter()
            .map(|f| (f.id.get(), f.compaction_level))
            .collect();
        assert_eq!(
            files_and_levels,
            vec![
                (6, CompactionLevel::FileNonOverlapped),
                (7, CompactionLevel::FileNonOverlapped),
                (8, CompactionLevel::FileNonOverlapped),
            ]
        );

        // ------------------------------------------------
        // Verify the parquet file content

        // Later compacted file
        let file1 = files.pop().unwrap();
        let batches = table.read_parquet_file(file1).await;
        assert_batches_sorted_eq!(
            &[
                "+-----------+------+------+------+-----------------------------+",
                "| field_int | tag1 | tag2 | tag3 | time                        |",
                "+-----------+------+------+------+-----------------------------+",
                "| 1600      |      | WA   | 10   | 1970-01-01T00:00:00.000028Z |",
                "| 20        |      | VT   | 20   | 1970-01-01T00:00:00.000026Z |",
                "| 270       | UT   |      |      | 1970-01-01T00:00:00.000025Z |",
                "+-----------+------+------+------+-----------------------------+",
            ],
            &batches
        );

        // Earlier compacted file
        let file0 = files.pop().unwrap();
        let batches = table.read_parquet_file(file0).await;
        assert_batches_sorted_eq!(
            &[
                "+-----------+------+------+------+--------------------------------+",
                "| field_int | tag1 | tag2 | tag3 | time                           |",
                "+-----------+------+------+------+--------------------------------+",
                "| 10        | VT   |      |      | 1970-01-01T00:00:00.000000020Z |",
                "| 10        | VT   |      |      | 1970-01-01T00:00:00.000006Z    |",
                "| 10        | VT   |      |      | 1970-01-01T00:00:00.000010Z    |",
                "| 1000      | WA   |      |      | 1970-01-01T00:00:00.000000010Z |",
                "| 1500      | WA   |      |      | 1970-01-01T00:00:00.000008Z    |",
                "| 1601      |      | PA   | 15   | 1970-01-01T00:00:00.000000009Z |",
                "| 21        |      | OH   | 21   | 1970-01-01T00:00:00.000000025Z |",
                "| 70        | UT   |      |      | 1970-01-01T00:00:00.000020Z    |",
                "+-----------+------+------+------+--------------------------------+",
            ],
            &batches
        );
    }

    #[tokio::test]
    async fn test_compact_cold_partition_one_level_0_without_overlap() {
        test_helpers::maybe_start_logging();
        let catalog = TestCatalog::new();

        // lp1 does not overlap with any other level 0 or level 1
        let lp1 = vec![
            "table,tag1=WA field_int=1000i 10",
            "table,tag1=VT field_int=10i 20",
        ]
        .join("\n");

        // lp6 does not overlap with any
        let lp6 = vec![
            "table,tag2=PA,tag3=15 field_int=81601i 90000",
            "table,tag2=OH,tag3=21 field_int=421i 91000",
        ]
        .join("\n");

        let ns = catalog.create_namespace("ns").await;
        let shard = ns.create_shard(1).await;
        let table = ns.create_table("table").await;
        table.create_column("field_int", ColumnType::I64).await;
        table.create_column("tag1", ColumnType::Tag).await;
        table.create_column("tag2", ColumnType::Tag).await;
        table.create_column("tag3", ColumnType::Tag).await;
        table.create_column("time", ColumnType::Time).await;
        let partition = table.with_shard(&shard).create_partition("part").await;
        let time = Arc::new(SystemProvider::new());
        let time_38_hour_ago = (time.now() - Duration::from_secs(60 * 60 * 38)).timestamp_nanos();
        let config = make_compactor_config();
        let metrics = Arc::new(metric::Registry::new());
        let compactor = Compactor::new(
            vec![shard.shard.id],
            Arc::clone(&catalog.catalog),
            ParquetStorage::new(Arc::clone(&catalog.object_store)),
            Arc::new(Executor::new(1)),
            Arc::new(SystemProvider::new()),
            BackoffConfig::default(),
            config,
            Arc::clone(&metrics),
        );

        // parquet files that are all in the same partition

        // pf1 does not overlap with any other level 0
        let builder = TestParquetFileBuilder::default()
            .with_line_protocol(&lp1)
            .with_max_seq(3)
            .with_min_time(10)
            .with_max_time(20)
            .with_file_size_bytes(compactor.config.max_desired_file_size_bytes + 10)
            .with_creation_time(time_38_hour_ago);
        partition.create_parquet_file(builder).await;

        // pf6 was created in a previous compaction cycle; does not overlap with any
        let builder = TestParquetFileBuilder::default()
            .with_line_protocol(&lp6)
            .with_max_seq(20)
            .with_min_time(90000)
            .with_max_time(91000)
            .with_file_size_bytes(100) // small file
            .with_creation_time(time_38_hour_ago)
            .with_compaction_level(CompactionLevel::FileNonOverlapped);
        partition.create_parquet_file(builder).await;

        // should have 1 level-0 file before compacting
        let count = catalog.count_level_0_files(shard.shard.id).await;
        assert_eq!(count, 1);

        // ------------------------------------------------
        // Compact
        let candidates = compactor
            .cold_partitions_to_compact(compactor.config.max_number_partitions_per_shard)
            .await
            .unwrap();
        let mut candidates = compactor.add_info_to_partitions(&candidates).await.unwrap();

        assert_eq!(candidates.len(), 1);
        let c = candidates.pop_front().unwrap();

        compact_cold_partition(&compactor, c).await.unwrap();

        // Should have 2 non-soft-deleted files:
        //
        // - the level 1 file that didn't overlap with anything
        // - the newly created level 1 file that was only upgraded from level 0
        let mut files = catalog.list_by_table_not_to_delete(table.table.id).await;
        assert_eq!(files.len(), 2);
        let files_and_levels: Vec<_> = files
            .iter()
            .map(|f| (f.id.get(), f.compaction_level))
            .collect();
        assert_eq!(
            files_and_levels,
            vec![
                (1, CompactionLevel::FileNonOverlapped),
                (2, CompactionLevel::FileNonOverlapped),
            ]
        );

        // ------------------------------------------------
        // Verify the parquet file content

        // Later compacted file
        let file1 = files.pop().unwrap();
        let batches = table.read_parquet_file(file1).await;
        assert_batches_sorted_eq!(
            &[
                "+-----------+------+------+-----------------------------+",
                "| field_int | tag2 | tag3 | time                        |",
                "+-----------+------+------+-----------------------------+",
                "| 421       | OH   | 21   | 1970-01-01T00:00:00.000091Z |",
                "| 81601     | PA   | 15   | 1970-01-01T00:00:00.000090Z |",
                "+-----------+------+------+-----------------------------+",
            ],
            &batches
        );

        // Earlier compacted file
        let file0 = files.pop().unwrap();
        let batches = table.read_parquet_file(file0).await;
        assert_batches_sorted_eq!(
            &[
                "+-----------+------+--------------------------------+",
                "| field_int | tag1 | time                           |",
                "+-----------+------+--------------------------------+",
                "| 10        | VT   | 1970-01-01T00:00:00.000000020Z |",
                "| 1000      | WA   | 1970-01-01T00:00:00.000000010Z |",
                "+-----------+------+--------------------------------+",
            ],
            &batches
        );
    }

    fn make_compactor_config() -> CompactorConfig {
        CompactorConfig {
            max_desired_file_size_bytes: 10_000,
            percentage_max_file_size: 30,
            split_percentage: 80,
            max_cold_concurrent_size_bytes: 90_000,
            max_number_partitions_per_shard: 1,
            min_number_recent_ingested_files_per_partition: 1,
            cold_input_size_threshold_bytes: 600 * 1024 * 1024,
            cold_input_file_count_threshold: 100,
            hot_multiple: 4,
            memory_budget_bytes: 100_000_000,
        }
    }
}
