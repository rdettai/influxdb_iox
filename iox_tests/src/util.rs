//! Utils of the tests

use arrow::{
    compute::{lexsort, SortColumn, SortOptions},
    record_batch::RecordBatch,
};
use data_types::{
    Column, ColumnSet, ColumnType, CompactionLevel, Namespace, NamespaceSchema, ParquetFile,
    ParquetFileParams, Partition, PartitionId, QueryPool, SequenceNumber, Shard, ShardId,
    ShardIndex, Table, TableId, TableSchema, Timestamp, Tombstone, TombstoneId, TopicMetadata,
};
use datafusion::physical_plan::metrics::Count;
use iox_catalog::{
    interface::{get_schema_by_id, get_table_schema_by_id, Catalog, PartitionRepo},
    mem::MemCatalog,
};
use iox_query::{exec::Executor, provider::RecordBatchDeduplicator, util::arrow_sort_key_exprs};
use iox_time::{MockProvider, Time, TimeProvider};
use mutable_batch_lp::test_helpers::lp_to_mutable_batch;
use object_store::{memory::InMemory, DynObjectStore};
use observability_deps::tracing::debug;
use once_cell::sync::Lazy;
use parquet_file::{metadata::IoxMetadata, storage::ParquetStorage, ParquetFilePath};
use schema::{
    selection::Selection,
    sort::{adjust_sort_key_columns, compute_sort_key, SortKey},
    Schema,
};
use std::sync::Arc;
use uuid::Uuid;

/// Global executor used by all test catalogs.
static GLOBAL_EXEC: Lazy<Arc<Executor>> = Lazy::new(|| Arc::new(Executor::new(1)));

/// Catalog for tests
#[derive(Debug)]
#[allow(missing_docs)]
pub struct TestCatalog {
    pub catalog: Arc<dyn Catalog>,
    pub metric_registry: Arc<metric::Registry>,
    pub object_store: Arc<DynObjectStore>,
    pub time_provider: Arc<MockProvider>,
    pub exec: Arc<Executor>,
}

impl TestCatalog {
    /// Initialize the catalog
    ///
    /// All test catalogs use the same [`Executor`]. Use [`with_exec`](Self::with_exec) if you need a special or
    /// dedicated executor.
    pub fn new() -> Arc<Self> {
        let exec = Arc::clone(&GLOBAL_EXEC);

        Self::with_exec(exec)
    }

    /// Initialize with given executor.
    pub fn with_exec(exec: Arc<Executor>) -> Arc<Self> {
        let metric_registry = Arc::new(metric::Registry::new());
        let catalog: Arc<dyn Catalog> = Arc::new(MemCatalog::new(Arc::clone(&metric_registry)));
        let object_store = Arc::new(InMemory::new());
        let time_provider = Arc::new(MockProvider::new(Time::from_timestamp(0, 0)));

        Arc::new(Self {
            metric_registry,
            catalog,
            object_store,
            time_provider,
            exec,
        })
    }

    /// Return the catalog
    pub fn catalog(&self) -> Arc<dyn Catalog> {
        Arc::clone(&self.catalog)
    }

    /// Return the catalog's metric registry
    pub fn metric_registry(&self) -> Arc<metric::Registry> {
        Arc::clone(&self.metric_registry)
    }

    /// Return the catalog's  object store
    pub fn object_store(&self) -> Arc<DynObjectStore> {
        Arc::clone(&self.object_store)
    }

    /// Return the mockable version of the catalog's time provider.
    ///
    /// If you need a generic time provider, use [`time_provider`](Self::time_provider) instead.
    pub fn mock_time_provider(&self) -> &MockProvider {
        self.time_provider.as_ref()
    }

    /// Return the catalog's time provider
    ///
    /// If you need to mock the time, use [`mock_time_provider`](Self::mock_time_provider) instead.
    pub fn time_provider(&self) -> Arc<dyn TimeProvider> {
        Arc::clone(&self.time_provider) as _
    }

    /// Return the catalog's executor
    pub fn exec(&self) -> Arc<Executor> {
        Arc::clone(&self.exec)
    }

    /// Create a shard in the catalog
    pub async fn create_shard(self: &Arc<Self>, shard_index: i32) -> Arc<Shard> {
        let mut repos = self.catalog.repositories().await;

        let topic = repos.topics().create_or_get("topic").await.unwrap();
        let shard_index = ShardIndex::new(shard_index);
        Arc::new(
            repos
                .shards()
                .create_or_get(&topic, shard_index)
                .await
                .unwrap(),
        )
    }

    /// Create a namesapce in the catalog
    pub async fn create_namespace(self: &Arc<Self>, name: &str) -> Arc<TestNamespace> {
        let mut repos = self.catalog.repositories().await;

        let topic = repos.topics().create_or_get("topic").await.unwrap();
        let query_pool = repos.query_pools().create_or_get("pool").await.unwrap();
        let namespace = repos
            .namespaces()
            .create(name, "1y", topic.id, query_pool.id)
            .await
            .unwrap();

        Arc::new(TestNamespace {
            catalog: Arc::clone(self),
            topic,
            query_pool,
            namespace,
        })
    }

    /// return tombstones of a given table
    pub async fn list_tombstones_by_table(self: &Arc<Self>, table_id: TableId) -> Vec<Tombstone> {
        self.catalog
            .repositories()
            .await
            .tombstones()
            .list_by_table(table_id)
            .await
            .unwrap()
    }

    /// return number of tombstones of a given table
    pub async fn count_tombstones_for_table(self: &Arc<Self>, table_id: TableId) -> usize {
        let ts = self
            .catalog
            .repositories()
            .await
            .tombstones()
            .list_by_table(table_id)
            .await
            .unwrap();
        ts.len()
    }

    /// return number of processed tombstones of a tombstones
    pub async fn count_processed_tombstones(self: &Arc<Self>, tombstone_id: TombstoneId) -> i64 {
        self.catalog
            .repositories()
            .await
            .processed_tombstones()
            .count_by_tombstone_id(tombstone_id)
            .await
            .unwrap()
    }

    /// List level 0 files
    pub async fn list_level_0_files(self: &Arc<Self>, shard_id: ShardId) -> Vec<ParquetFile> {
        self.catalog
            .repositories()
            .await
            .parquet_files()
            .level_0(shard_id)
            .await
            .unwrap()
    }

    /// Count level 0 files
    pub async fn count_level_0_files(self: &Arc<Self>, shard_id: ShardId) -> usize {
        let level_0 = self
            .catalog
            .repositories()
            .await
            .parquet_files()
            .level_0(shard_id)
            .await
            .unwrap();
        level_0.len()
    }

    /// List all non-deleted files
    pub async fn list_by_table_not_to_delete(
        self: &Arc<Self>,
        table_id: TableId,
    ) -> Vec<ParquetFile> {
        self.catalog
            .repositories()
            .await
            .parquet_files()
            .list_by_table_not_to_delete(table_id)
            .await
            .unwrap()
    }
}

/// A test namespace
#[derive(Debug)]
#[allow(missing_docs)]
pub struct TestNamespace {
    pub catalog: Arc<TestCatalog>,
    pub topic: TopicMetadata,
    pub query_pool: QueryPool,
    pub namespace: Namespace,
}

impl TestNamespace {
    /// Create a table in this namespace
    pub async fn create_table(self: &Arc<Self>, name: &str) -> Arc<TestTable> {
        let mut repos = self.catalog.catalog.repositories().await;

        let table = repos
            .tables()
            .create_or_get(name, self.namespace.id)
            .await
            .unwrap();

        Arc::new(TestTable {
            catalog: Arc::clone(&self.catalog),
            namespace: Arc::clone(self),
            table,
        })
    }

    /// Create a shard for this namespace
    pub async fn create_shard(self: &Arc<Self>, shard_index: i32) -> Arc<TestShard> {
        let mut repos = self.catalog.catalog.repositories().await;

        let shard = repos
            .shards()
            .create_or_get(&self.topic, ShardIndex::new(shard_index))
            .await
            .unwrap();

        Arc::new(TestShard {
            catalog: Arc::clone(&self.catalog),
            namespace: Arc::clone(self),
            shard,
        })
    }

    /// Get namespace schema for this namespace.
    pub async fn schema(&self) -> NamespaceSchema {
        let mut repos = self.catalog.catalog.repositories().await;
        get_schema_by_id(self.namespace.id, repos.as_mut())
            .await
            .unwrap()
    }
}

/// A test shard with its namespace in the catalog
#[derive(Debug)]
#[allow(missing_docs)]
pub struct TestShard {
    pub catalog: Arc<TestCatalog>,
    pub namespace: Arc<TestNamespace>,
    pub shard: Shard,
}

/// A test table of a namespace in the catalog
#[allow(missing_docs)]
#[derive(Debug)]
pub struct TestTable {
    pub catalog: Arc<TestCatalog>,
    pub namespace: Arc<TestNamespace>,
    pub table: Table,
}

impl TestTable {
    /// Attach a shard to the table
    pub fn with_shard(self: &Arc<Self>, shard: &Arc<TestShard>) -> Arc<TestTableBoundShard> {
        assert!(Arc::ptr_eq(&self.catalog, &shard.catalog));
        assert!(Arc::ptr_eq(&self.namespace, &shard.namespace));

        Arc::new(TestTableBoundShard {
            catalog: Arc::clone(&self.catalog),
            namespace: Arc::clone(&self.namespace),
            table: Arc::clone(self),
            shard: Arc::clone(shard),
        })
    }

    /// Create a column for the table
    pub async fn create_column(
        self: &Arc<Self>,
        name: &str,
        column_type: ColumnType,
    ) -> Arc<TestColumn> {
        let mut repos = self.catalog.catalog.repositories().await;

        let column = repos
            .columns()
            .create_or_get(name, self.table.id, column_type)
            .await
            .unwrap();

        Arc::new(TestColumn {
            catalog: Arc::clone(&self.catalog),
            namespace: Arc::clone(&self.namespace),
            table: Arc::clone(self),
            column,
        })
    }

    /// Get catalog schema.
    pub async fn catalog_schema(&self) -> TableSchema {
        let mut repos = self.catalog.catalog.repositories().await;

        get_table_schema_by_id(self.table.id, repos.as_mut())
            .await
            .unwrap()
    }

    /// Get schema for this table.
    pub async fn schema(&self) -> Schema {
        self.catalog_schema().await.try_into().unwrap()
    }

    /// Read the record batches from the specified Parquet File associated with this table.
    pub async fn read_parquet_file(&self, file: ParquetFile) -> Vec<RecordBatch> {
        let storage = ParquetStorage::new(self.catalog.object_store());

        // get schema
        let table_catalog_schema = self.catalog_schema().await;
        let column_id_lookup = table_catalog_schema.column_id_map();
        let table_schema = self.schema().await;
        let selection: Vec<_> = file
            .column_set
            .iter()
            .map(|id| *column_id_lookup.get(id).unwrap())
            .collect();
        let schema = table_schema.select_by_names(&selection).unwrap();

        let path: ParquetFilePath = (&file).into();
        let rx = storage.read_all(schema.as_arrow(), &path).unwrap();
        datafusion::physical_plan::common::collect(rx)
            .await
            .unwrap()
    }
}

/// A test column.
#[allow(missing_docs)]
pub struct TestColumn {
    pub catalog: Arc<TestCatalog>,
    pub namespace: Arc<TestNamespace>,
    pub table: Arc<TestTable>,
    pub column: Column,
}

/// A test catalog with specified namespace, shard, and table
#[allow(missing_docs)]
pub struct TestTableBoundShard {
    pub catalog: Arc<TestCatalog>,
    pub namespace: Arc<TestNamespace>,
    pub table: Arc<TestTable>,
    pub shard: Arc<TestShard>,
}

impl TestTableBoundShard {
    /// Creat a partition for the table
    pub async fn create_partition(self: &Arc<Self>, key: &str) -> Arc<TestPartition> {
        let mut repos = self.catalog.catalog.repositories().await;

        let partition = repos
            .partitions()
            .create_or_get(key.into(), self.shard.shard.id, self.table.table.id)
            .await
            .unwrap();

        Arc::new(TestPartition {
            catalog: Arc::clone(&self.catalog),
            namespace: Arc::clone(&self.namespace),
            table: Arc::clone(&self.table),
            shard: Arc::clone(&self.shard),
            partition,
        })
    }

    /// Creat a partition with a specified sort key for the table
    pub async fn create_partition_with_sort_key(
        self: &Arc<Self>,
        key: &str,
        sort_key: &[&str],
    ) -> Arc<TestPartition> {
        let mut repos = self.catalog.catalog.repositories().await;

        let partition = repos
            .partitions()
            .create_or_get(key.into(), self.shard.shard.id, self.table.table.id)
            .await
            .unwrap();

        let partition = repos
            .partitions()
            .update_sort_key(partition.id, sort_key)
            .await
            .unwrap();

        Arc::new(TestPartition {
            catalog: Arc::clone(&self.catalog),
            namespace: Arc::clone(&self.namespace),
            table: Arc::clone(&self.table),
            shard: Arc::clone(&self.shard),
            partition,
        })
    }

    /// Create a tombstone
    pub async fn create_tombstone(
        self: &Arc<Self>,
        sequence_number: i64,
        min_time: i64,
        max_time: i64,
        predicate: &str,
    ) -> Arc<TestTombstone> {
        let mut repos = self.catalog.catalog.repositories().await;

        let tombstone = repos
            .tombstones()
            .create_or_get(
                self.table.table.id,
                self.shard.shard.id,
                SequenceNumber::new(sequence_number),
                Timestamp::new(min_time),
                Timestamp::new(max_time),
                predicate,
            )
            .await
            .unwrap();

        Arc::new(TestTombstone {
            catalog: Arc::clone(&self.catalog),
            namespace: Arc::clone(&self.namespace),
            tombstone,
        })
    }
}

/// A test catalog with specified namespace, shard, table, partition
#[allow(missing_docs)]
#[derive(Debug)]
pub struct TestPartition {
    pub catalog: Arc<TestCatalog>,
    pub namespace: Arc<TestNamespace>,
    pub table: Arc<TestTable>,
    pub shard: Arc<TestShard>,
    pub partition: Partition,
}

impl TestPartition {
    /// Update sort key.
    pub async fn update_sort_key(self: &Arc<Self>, sort_key: SortKey) -> Arc<Self> {
        let partition = self
            .catalog
            .catalog
            .repositories()
            .await
            .partitions()
            .update_sort_key(
                self.partition.id,
                &sort_key.to_columns().collect::<Vec<_>>(),
            )
            .await
            .unwrap();

        Arc::new(Self {
            catalog: Arc::clone(&self.catalog),
            namespace: Arc::clone(&self.namespace),
            table: Arc::clone(&self.table),
            shard: Arc::clone(&self.shard),
            partition,
        })
    }

    /// Create a Parquet file in this partition in object storage and the catalog with attributes
    /// specified by the builder
    pub async fn create_parquet_file(
        self: &Arc<Self>,
        builder: TestParquetFileBuilder,
    ) -> TestParquetFile {
        let TestParquetFileBuilder {
            record_batch,
            table,
            schema,
            max_sequence_number,
            min_time,
            max_time,
            file_size_bytes,
            creation_time,
            compaction_level,
            to_delete,
            object_store_id,
            row_count,
        } = builder;

        let record_batch = record_batch.expect("A record batch is required");
        let table = table.expect("A table is required");
        let schema = schema.expect("A schema is required");
        assert_eq!(
            table, self.table.table.name,
            "Table name of line protocol and partition should have matched",
        );

        assert!(
            row_count.is_none(),
            "Cannot have both a record batch and a manually set row_count!"
        );
        let row_count = record_batch.num_rows();
        assert!(row_count > 0, "Parquet file must have at least 1 row");
        let (record_batch, sort_key) = sort_batch(record_batch, schema.clone());
        let record_batch = dedup_batch(record_batch, &sort_key);

        let object_store_id = object_store_id.unwrap_or_else(Uuid::new_v4);

        let metadata = IoxMetadata {
            object_store_id,
            creation_timestamp: now(),
            namespace_id: self.namespace.namespace.id,
            namespace_name: self.namespace.namespace.name.clone().into(),
            shard_id: self.shard.shard.id,
            table_id: self.table.table.id,
            table_name: self.table.table.name.clone().into(),
            partition_id: self.partition.id,
            partition_key: self.partition.partition_key.clone(),
            max_sequence_number,
            compaction_level: CompactionLevel::Initial,
            sort_key: Some(sort_key.clone()),
        };
        let real_file_size_bytes = create_parquet_file(
            ParquetStorage::new(Arc::clone(&self.catalog.object_store)),
            &metadata,
            record_batch.clone(),
        )
        .await;

        let builder = TestParquetFileBuilder {
            record_batch: Some(record_batch),
            table: Some(table),
            schema: Some(schema),
            max_sequence_number,
            min_time,
            max_time,
            file_size_bytes: Some(file_size_bytes.unwrap_or(real_file_size_bytes as u64)),
            creation_time,
            compaction_level,
            to_delete,
            object_store_id: Some(object_store_id),
            row_count: None, // will be computed from the record batch again
        };

        let result = self.create_parquet_file_catalog_record(builder).await;
        let mut repos = self.catalog.catalog.repositories().await;
        update_catalog_sort_key_if_needed(repos.partitions(), self.partition.id, sort_key).await;
        result
    }

    /// Only update the catalog with the builder's info, don't create anything in object storage.
    /// Record batch is not required in this case.
    pub async fn create_parquet_file_catalog_record(
        self: &Arc<Self>,
        builder: TestParquetFileBuilder,
    ) -> TestParquetFile {
        let TestParquetFileBuilder {
            record_batch,
            max_sequence_number,
            min_time,
            max_time,
            file_size_bytes,
            creation_time,
            compaction_level,
            to_delete,
            object_store_id,
            row_count,
            ..
        } = builder;

        let table_catalog_schema = self.table.catalog_schema().await;

        let (row_count, column_set) = if let Some(record_batch) = record_batch {
            let column_set = ColumnSet::new(record_batch.schema().fields().iter().map(|f| {
                table_catalog_schema
                    .columns
                    .get(f.name())
                    .expect("Column registered")
                    .id
            }));

            assert!(
                row_count.is_none(),
                "Cannot have both a record batch and a manually set row_count!"
            );

            (record_batch.num_rows(), column_set)
        } else {
            let column_set =
                ColumnSet::new(table_catalog_schema.columns.values().map(|col| col.id));
            (row_count.unwrap_or(0), column_set)
        };

        let parquet_file_params = ParquetFileParams {
            shard_id: self.shard.shard.id,
            namespace_id: self.namespace.namespace.id,
            table_id: self.table.table.id,
            partition_id: self.partition.id,
            object_store_id: object_store_id.unwrap_or_else(Uuid::new_v4),
            max_sequence_number,
            min_time: Timestamp::new(min_time),
            max_time: Timestamp::new(max_time),
            file_size_bytes: file_size_bytes.unwrap_or(0) as i64,
            row_count: row_count as i64,
            created_at: Timestamp::new(creation_time),
            compaction_level,
            column_set,
        };

        let mut repos = self.catalog.catalog.repositories().await;
        let parquet_file = repos
            .parquet_files()
            .create(parquet_file_params)
            .await
            .unwrap();

        if to_delete {
            repos
                .parquet_files()
                .flag_for_delete(parquet_file.id)
                .await
                .unwrap();
        }

        TestParquetFile {
            catalog: Arc::clone(&self.catalog),
            namespace: Arc::clone(&self.namespace),
            table: Arc::clone(&self.table),
            shard: Arc::clone(&self.shard),
            partition: Arc::clone(self),
            parquet_file,
        }
    }
}

/// A builder for creating parquet files within partitions.
#[derive(Debug, Clone)]
pub struct TestParquetFileBuilder {
    record_batch: Option<RecordBatch>,
    table: Option<String>,
    schema: Option<Schema>,
    max_sequence_number: SequenceNumber,
    min_time: i64,
    max_time: i64,
    file_size_bytes: Option<u64>,
    creation_time: i64,
    compaction_level: CompactionLevel,
    to_delete: bool,
    object_store_id: Option<Uuid>,
    row_count: Option<usize>,
}

impl Default for TestParquetFileBuilder {
    fn default() -> Self {
        Self {
            record_batch: None,
            table: None,
            schema: None,
            max_sequence_number: SequenceNumber::new(100),
            min_time: now().timestamp_nanos(),
            max_time: now().timestamp_nanos(),
            file_size_bytes: None,
            creation_time: 1,
            compaction_level: CompactionLevel::Initial,
            to_delete: false,
            object_store_id: None,
            row_count: None,
        }
    }
}

impl TestParquetFileBuilder {
    /// Specify the line protocol that should become the record batch in this parquet file.
    pub fn with_line_protocol(self, line_protocol: &str) -> Self {
        let (table, batch) = lp_to_mutable_batch(line_protocol);

        let schema = batch.schema(Selection::All).unwrap();
        let record_batch = batch.to_arrow(Selection::All).unwrap();

        self.with_record_batch(record_batch)
            .with_table(table)
            .with_schema(schema)
    }

    fn with_record_batch(mut self, record_batch: RecordBatch) -> Self {
        self.record_batch = Some(record_batch);
        self
    }

    fn with_table(mut self, table: String) -> Self {
        self.table = Some(table);
        self
    }

    fn with_schema(mut self, schema: Schema) -> Self {
        self.schema = Some(schema);
        self
    }

    /// Specify the maximum sequence number for the parquet file metadata.
    pub fn with_max_seq(mut self, max_seq: i64) -> Self {
        self.max_sequence_number = SequenceNumber::new(max_seq);
        self
    }

    /// Specify the minimum time for the parquet file metadata.
    pub fn with_min_time(mut self, min_time: i64) -> Self {
        self.min_time = min_time;
        self
    }

    /// Specify the maximum time for the parquet file metadata.
    pub fn with_max_time(mut self, max_time: i64) -> Self {
        self.max_time = max_time;
        self
    }

    /// Specify the file size, in bytes, for the parquet file metadata.
    pub fn with_file_size_bytes(mut self, file_size_bytes: u64) -> Self {
        self.file_size_bytes = Some(file_size_bytes);
        self
    }

    /// Specify the creation time for the parquet file metadata.
    pub fn with_creation_time(mut self, creation_time: i64) -> Self {
        self.creation_time = creation_time;
        self
    }

    /// Specify the compaction level for the parquet file metadata.
    pub fn with_compaction_level(mut self, compaction_level: CompactionLevel) -> Self {
        self.compaction_level = compaction_level;
        self
    }

    /// Specify whether the parquet file should be marked as deleted or not.
    pub fn with_to_delete(mut self, to_delete: bool) -> Self {
        self.to_delete = to_delete;
        self
    }

    /// Specify the number of rows in this parquet file. If line protocol/record batch are also
    /// set, this will panic! Only use this when you're not specifying any rows!
    pub fn with_row_count(mut self, row_count: usize) -> Self {
        self.row_count = Some(row_count);
        self
    }
}

async fn update_catalog_sort_key_if_needed(
    partitions_catalog: &mut dyn PartitionRepo,
    partition_id: PartitionId,
    sort_key: SortKey,
) {
    // Fetch the latest partition info from the catalog
    let partition = partitions_catalog
        .get_by_id(partition_id)
        .await
        .unwrap()
        .unwrap();

    // Similarly to what the ingester does, if there's an existing sort key in the catalog, add new
    // columns onto the end
    match partition.sort_key() {
        Some(catalog_sort_key) => {
            let new_sort_key = sort_key.to_columns().collect::<Vec<_>>();
            let (_metadata, update) = adjust_sort_key_columns(&catalog_sort_key, &new_sort_key);
            if let Some(new_sort_key) = update {
                let new_columns = new_sort_key.to_columns().collect::<Vec<_>>();
                debug!(
                    "Updating sort key from {:?} to {:?}",
                    catalog_sort_key.to_columns().collect::<Vec<_>>(),
                    &new_columns,
                );
                partitions_catalog
                    .update_sort_key(partition_id, &new_columns)
                    .await
                    .unwrap();
            }
        }
        None => {
            let new_columns = sort_key.to_columns().collect::<Vec<_>>();
            debug!("Updating sort key from None to {:?}", &new_columns);
            partitions_catalog
                .update_sort_key(partition_id, &new_columns)
                .await
                .unwrap();
        }
    }
}

/// Create parquet file and return file size.
async fn create_parquet_file(
    store: ParquetStorage,
    metadata: &IoxMetadata,
    record_batch: RecordBatch,
) -> usize {
    let stream = futures::stream::once(async { Ok(record_batch) });
    let (_meta, file_size) = store
        .upload(stream, metadata)
        .await
        .expect("persisting parquet file should succeed");
    file_size
}

/// A test parquet file of the catalog
#[allow(missing_docs)]
pub struct TestParquetFile {
    pub catalog: Arc<TestCatalog>,
    pub namespace: Arc<TestNamespace>,
    pub table: Arc<TestTable>,
    pub shard: Arc<TestShard>,
    pub partition: Arc<TestPartition>,
    pub parquet_file: ParquetFile,
}

impl TestParquetFile {
    /// Make the parquet file deletable
    pub async fn flag_for_delete(&self) {
        let mut repos = self.catalog.catalog.repositories().await;

        repos
            .parquet_files()
            .flag_for_delete(self.parquet_file.id)
            .await
            .unwrap()
    }

    /// Get Parquet file schema.
    pub async fn schema(&self) -> Arc<Schema> {
        let table_schema = self.table.catalog_schema().await;
        let column_id_lookup = table_schema.column_id_map();
        let selection: Vec<_> = self
            .parquet_file
            .column_set
            .iter()
            .map(|id| *column_id_lookup.get(id).unwrap())
            .collect();
        let table_schema: Schema = table_schema.clone().try_into().unwrap();
        Arc::new(table_schema.select_by_names(&selection).unwrap())
    }
}

/// A catalog test tombstone
#[allow(missing_docs)]
pub struct TestTombstone {
    pub catalog: Arc<TestCatalog>,
    pub namespace: Arc<TestNamespace>,
    pub tombstone: Tombstone,
}

impl TestTombstone {
    /// mark the tombstone proccesed
    pub async fn mark_processed(self: &Arc<Self>, parquet_file: &TestParquetFile) {
        assert!(Arc::ptr_eq(&self.catalog, &parquet_file.catalog));
        assert!(Arc::ptr_eq(&self.namespace, &parquet_file.namespace));

        let mut repos = self.catalog.catalog.repositories().await;

        repos
            .processed_tombstones()
            .create(parquet_file.parquet_file.id, self.tombstone.id)
            .await
            .unwrap();
    }
}

/// Return the current time
pub fn now() -> Time {
    Time::from_timestamp(0, 0)
}

/// Sort arrow record batch into arrow record batch and sort key.
fn sort_batch(record_batch: RecordBatch, schema: Schema) -> (RecordBatch, SortKey) {
    // calculate realistic sort key
    let sort_key = compute_sort_key(&schema, std::iter::once(&record_batch));

    // set up sorting
    let mut sort_columns = Vec::with_capacity(record_batch.num_columns());
    let mut reverse_index: Vec<_> = (0..record_batch.num_columns()).map(|_| None).collect();
    for (column_name, _options) in sort_key.iter() {
        let index = record_batch
            .schema()
            .column_with_name(column_name.as_ref())
            .unwrap()
            .0;
        reverse_index[index] = Some(sort_columns.len());
        sort_columns.push(SortColumn {
            values: Arc::clone(record_batch.column(index)),
            options: Some(SortOptions::default()),
        });
    }
    for (index, reverse_index) in reverse_index.iter_mut().enumerate() {
        if reverse_index.is_none() {
            *reverse_index = Some(sort_columns.len());
            sort_columns.push(SortColumn {
                values: Arc::clone(record_batch.column(index)),
                options: None,
            });
        }
    }

    // execute sorting
    let arrays = lexsort(&sort_columns, None).unwrap();

    // re-create record batch
    let arrays: Vec<_> = reverse_index
        .into_iter()
        .map(|index| {
            let index = index.unwrap();
            Arc::clone(&arrays[index])
        })
        .collect();
    let record_batch = RecordBatch::try_new(record_batch.schema(), arrays).unwrap();

    (record_batch, sort_key)
}

fn dedup_batch(record_batch: RecordBatch, sort_key: &SortKey) -> RecordBatch {
    let schema = record_batch.schema();
    let sort_keys = arrow_sort_key_exprs(sort_key, &schema);
    let mut deduplicator = RecordBatchDeduplicator::new(sort_keys, Count::default(), None);

    let mut batches = vec![deduplicator.push(record_batch).unwrap()];
    if let Some(batch) = deduplicator.finish().unwrap() {
        batches.push(batch);
    }

    RecordBatch::concat(&schema, &batches).unwrap()
}
