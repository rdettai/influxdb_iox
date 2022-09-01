//! This module is responsible for writing the given data to the specified
//! object store and reading it back.

use crate::{
    metadata::{IoxMetadata, IoxParquetMetaData},
    serialize::{self, CodecError, ROW_GROUP_WRITE_SIZE},
    ParquetFilePath,
};
use arrow::{
    datatypes::{Field, SchemaRef},
    error::ArrowError,
    record_batch::RecordBatch,
};
use bytes::Bytes;
use datafusion::{
    datasource::{listing::PartitionedFile, object_store::ObjectStoreUrl},
    execution::context::TaskContext,
    physical_plan::{
        execute_stream,
        file_format::{FileScanConfig, ParquetExec},
        stream::RecordBatchStreamAdapter,
        SendableRecordBatchStream, Statistics,
    },
    prelude::SessionContext,
};
use futures::{Stream, TryStreamExt};
use object_store::{DynObjectStore, ObjectMeta};
use observability_deps::tracing::*;
use predicate::Predicate;
use schema::selection::{select_schema, Selection};
use std::{num::TryFromIntError, sync::Arc, time::Duration};
use thiserror::Error;

/// Parquet row group read size
pub const ROW_GROUP_READ_SIZE: usize = 1024 * 1024;

// ensure read and write work well together
// Skip clippy due to <https://github.com/rust-lang/rust-clippy/issues/8159>.
#[allow(clippy::assertions_on_constants)]
const _: () = assert!(ROW_GROUP_WRITE_SIZE % ROW_GROUP_READ_SIZE == 0);
/// Errors returned during a Parquet "put" operation, covering [`RecordBatch`]
/// pull from the provided stream, encoding, and finally uploading the bytes to
/// the object store.
#[derive(Debug, Error)]
pub enum UploadError {
    /// A codec failure during serialisation.
    #[error(transparent)]
    Serialise(#[from] CodecError),

    /// An error during Parquet metadata conversion when attempting to
    /// instantiate a valid [`IoxParquetMetaData`] instance.
    #[error("failed to construct IOx parquet metadata: {0}")]
    Metadata(crate::metadata::Error),

    /// Uploading the Parquet file to object store failed.
    #[error("failed to upload to object storage: {0}")]
    Upload(#[from] object_store::Error),
}

/// Errors during Parquet file download & scan.
#[derive(Debug, Error)]
#[allow(clippy::large_enum_variant)]
pub enum ReadError {
    /// Error writing the bytes fetched from object store to the temporary
    /// parquet file on disk.
    #[error("i/o error writing downloaded parquet: {0}")]
    IO(#[from] std::io::Error),

    /// An error fetching Parquet file bytes from object store.
    #[error("failed to read data from object store: {0}")]
    ObjectStore(#[from] object_store::Error),

    /// An error reading the downloaded Parquet file.
    #[error("invalid parquet file: {0}")]
    Parquet(#[from] parquet::errors::ParquetError),

    /// Schema mismatch
    #[error("Schema mismatch (expected VS actual parquet file) for file '{path}': {source}")]
    SchemaMismatch {
        /// Path of the affected parquet file.
        path: object_store::path::Path,

        /// Source error
        source: ProjectionError,
    },

    /// Malformed integer data for row count
    #[error("Malformed row count integer")]
    MalformedRowCount(#[from] TryFromIntError),
}

/// The [`ParquetStorage`] type encapsulates [`RecordBatch`] persistence to an
/// underlying [`ObjectStore`].
///
/// [`RecordBatch`] instances are serialized to Parquet files, with IOx specific
/// metadata ([`IoxParquetMetaData`]) attached.
///
/// Code that interacts with Parquet files in object storage should utilise this
/// type that encapsulates the storage & retrieval implementation.
///
/// [`ObjectStore`]: object_store::ObjectStore
#[derive(Debug, Clone)]
pub struct ParquetStorage {
    /// Underlying object store.
    object_store: Arc<DynObjectStore>,
}

impl ParquetStorage {
    /// Initialise a new [`ParquetStorage`] using `object_store` as the
    /// persistence layer.
    pub fn new(object_store: Arc<DynObjectStore>) -> Self {
        Self { object_store }
    }

    /// Push `batches`, a stream of [`RecordBatch`] instances, to object
    /// storage.
    ///
    /// # Retries
    ///
    /// This method retries forever in the presence of object store errors. All
    /// other errors are returned as they occur.
    pub async fn upload<S>(
        &self,
        batches: S,
        meta: &IoxMetadata,
    ) -> Result<(IoxParquetMetaData, usize), UploadError>
    where
        S: Stream<Item = Result<RecordBatch, ArrowError>> + Send,
    {
        // Stream the record batches into a parquet file.
        //
        // It would be nice to stream the encoded parquet to disk for this and
        // eliminate the buffering in memory, but the lack of a streaming object
        // store put negates any benefit of spilling to disk.
        //
        // This is not a huge concern, as the resulting parquet files are
        // currently smallish on average.
        let (data, parquet_file_meta) = serialize::to_parquet_bytes(batches, meta).await?;

        // Read the IOx-specific parquet metadata from the file metadata
        let parquet_meta =
            IoxParquetMetaData::try_from(parquet_file_meta).map_err(UploadError::Metadata)?;
        debug!(
            ?meta.partition_id,
            ?parquet_meta,
            "IoxParquetMetaData coverted from Row Group Metadata (aka FileMetaData)"
        );

        // Derive the correct object store path from the metadata.
        let path = ParquetFilePath::from(meta).object_store_path();

        let file_size = data.len();
        let data = Bytes::from(data);

        // Retry uploading the file endlessly.
        //
        // This is abort-able by the user by dropping the upload() future.
        //
        // Cloning `data` is a ref count inc, rather than a data copy.
        while let Err(e) = self.object_store.put(&path, data.clone()).await {
            error!(error=%e, ?meta, "failed to upload parquet file to object storage");
            tokio::time::sleep(Duration::from_secs(1)).await;
        }

        Ok((parquet_meta, file_size))
    }

    /// Pull the Parquet-encoded [`RecordBatch`] at the file path derived from
    /// the provided [`ParquetFilePath`].
    ///
    /// The `selection` projection is pushed down to the Parquet deserializer.
    ///
    /// This impl fetches the associated Parquet file bytes from object storage,
    /// temporarily persisting them to a local temp file to feed to the arrow
    /// reader.
    ///
    /// No caching is performed by `read_filter()`, and each call to
    /// `read_filter()` will re-download the parquet file unless the underlying
    /// object store impl caches the fetched bytes.
    pub fn read_filter(
        &self,
        predicate: &Predicate,
        selection: Selection<'_>,
        schema: SchemaRef,
        path: &ParquetFilePath,
        file_size: usize,
    ) -> Result<SendableRecordBatchStream, ReadError> {
        let path = path.object_store_path();
        trace!(path=?path, "fetching parquet data for filtered read");

        // Compute final (output) schema after selection
        let schema = Arc::new(
            select_schema(selection, &schema)
                .as_ref()
                .clone()
                .with_metadata(Default::default()),
        );

        // create ParquetExec node
        let object_meta = ObjectMeta {
            location: path,
            // we don't care about the "last modified" field
            last_modified: Default::default(),
            size: file_size,
        };
        let expr = predicate.filter_expr();
        let base_config = FileScanConfig {
            object_store_url: ObjectStoreUrl::parse("iox://iox/").expect("valid object store URL"),
            file_schema: Arc::clone(&schema),
            file_groups: vec![vec![PartitionedFile {
                object_meta,
                partition_values: vec![],
                range: None,
                extensions: None,
            }]],
            statistics: Statistics::default(),
            projection: None,
            limit: None,
            table_partition_cols: vec![],
        };
        let exec = ParquetExec::new(base_config, expr, None);

        // set up "fake" DataFusion session
        let object_store = Arc::clone(&self.object_store);
        let session_ctx = SessionContext::new();
        let task_ctx = Arc::new(TaskContext::from(&session_ctx));
        task_ctx
            .runtime_env()
            .register_object_store("iox", "iox", object_store);

        Ok(Box::pin(RecordBatchStreamAdapter::new(
            Arc::clone(&schema),
            futures::stream::once(execute_stream(Arc::new(exec), task_ctx)).try_flatten(),
        )))
    }

    /// Read all data from the parquet file.
    pub fn read_all(
        &self,
        schema: SchemaRef,
        path: &ParquetFilePath,
        file_size: usize,
    ) -> Result<SendableRecordBatchStream, ReadError> {
        self.read_filter(
            &Predicate::default(),
            Selection::All,
            schema,
            path,
            file_size,
        )
    }
}

/// Error during projecting parquet file data to an expected schema.
#[derive(Debug, Error)]
#[allow(clippy::large_enum_variant)]
pub enum ProjectionError {
    /// Unknown field.
    #[error("Unknown field: {0}")]
    UnknownField(String),

    /// Field type mismatch
    #[error("Type mismatch, expected {expected:?} but got {actual:?}")]
    FieldTypeMismatch {
        /// Expected field.
        expected: Field,

        /// Actual field.
        actual: Field,
    },
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{ArrayRef, Int64Array, StringArray};
    use data_types::{CompactionLevel, NamespaceId, PartitionId, SequenceNumber, ShardId, TableId};
    use datafusion::common::DataFusionError;
    use iox_time::Time;
    use std::collections::HashMap;

    #[tokio::test]
    async fn test_upload_metadata() {
        let object_store: Arc<DynObjectStore> = Arc::new(object_store::memory::InMemory::default());

        let store = ParquetStorage::new(object_store);

        let meta = meta();
        let batch = RecordBatch::try_from_iter([("a", to_string_array(&["value"]))]).unwrap();

        // Serialize & upload the record batches.
        let (file_meta, _file_size) = upload(&store, &meta, batch.clone()).await;

        // Extract the various bits of metadata.
        let file_meta = file_meta.decode().expect("should decode parquet metadata");
        let got_iox_meta = file_meta
            .read_iox_metadata_new()
            .expect("should read IOx metadata from parquet meta");

        // Ensure the metadata in the file decodes to the same IOx metadata we
        // provided when uploading.
        assert_eq!(got_iox_meta, meta);
    }

    #[tokio::test]
    async fn test_simple_roundtrip() {
        let batch = RecordBatch::try_from_iter([("a", to_string_array(&["value"]))]).unwrap();
        let schema = batch.schema();

        assert_roundtrip(batch.clone(), Selection::All, schema, batch).await;
    }

    #[tokio::test]
    async fn test_selection() {
        let batch = RecordBatch::try_from_iter([
            ("a", to_string_array(&["value"])),
            ("b", to_int_array(&[1])),
            ("c", to_string_array(&["foo"])),
            ("d", to_int_array(&[2])),
        ])
        .unwrap();
        let schema = batch.schema();

        let expected_batch = RecordBatch::try_from_iter([
            ("d", to_int_array(&[2])),
            ("c", to_string_array(&["foo"])),
        ])
        .unwrap();
        assert_roundtrip(batch, Selection::Some(&["d", "c"]), schema, expected_batch).await;
    }

    #[tokio::test]
    async fn test_selection_unknown() {
        let batch = RecordBatch::try_from_iter([
            ("a", to_string_array(&["value"])),
            ("b", to_int_array(&[1])),
        ])
        .unwrap();
        let schema = batch.schema();

        let expected_batch = RecordBatch::try_from_iter([("b", to_int_array(&[1]))]).unwrap();
        assert_roundtrip(batch, Selection::Some(&["b", "c"]), schema, expected_batch).await;
    }

    #[tokio::test]
    async fn test_file_has_different_column_order() {
        let file_batch = RecordBatch::try_from_iter([
            ("a", to_string_array(&["value"])),
            ("b", to_int_array(&[1])),
        ])
        .unwrap();
        let schema_batch = RecordBatch::try_from_iter([
            ("b", to_int_array(&[1])),
            ("a", to_string_array(&["value"])),
        ])
        .unwrap();
        let schema = schema_batch.schema();
        assert_roundtrip(file_batch, Selection::All, schema, schema_batch).await;
    }

    #[tokio::test]
    async fn test_file_has_different_column_order_with_selection() {
        let batch = RecordBatch::try_from_iter([
            ("a", to_string_array(&["value"])),
            ("b", to_int_array(&[1])),
            ("c", to_string_array(&["foo"])),
            ("d", to_int_array(&[2])),
        ])
        .unwrap();
        let schema_batch = RecordBatch::try_from_iter([
            ("b", to_int_array(&[1])),
            ("d", to_int_array(&[2])),
            ("c", to_string_array(&["foo"])),
            ("a", to_string_array(&["value"])),
        ])
        .unwrap();
        let schema = schema_batch.schema();

        let expected_batch = RecordBatch::try_from_iter([
            ("d", to_int_array(&[2])),
            ("c", to_string_array(&["foo"])),
        ])
        .unwrap();
        assert_roundtrip(batch, Selection::Some(&["d", "c"]), schema, expected_batch).await;
    }

    #[tokio::test]
    async fn test_schema_check_fail_different_types() {
        let batch = RecordBatch::try_from_iter([("a", to_string_array(&["value"]))]).unwrap();
        let other_batch = RecordBatch::try_from_iter([("a", to_int_array(&[1]))]).unwrap();
        let schema = batch.schema();
        assert_schema_check_fail(
            other_batch,
            schema,
            "Arrow error: External error: Execution error: Failed to map column projection for field a. Incompatible data types Int64 and Utf8",
        ).await;
    }

    #[tokio::test]
    async fn test_schema_check_fail_different_names() {
        let batch = RecordBatch::try_from_iter([("a", to_string_array(&["value"]))]).unwrap();
        let other_batch = RecordBatch::try_from_iter([("b", to_string_array(&["value"]))]).unwrap();
        let schema = batch.schema();
        assert_schema_check_fail(
            other_batch,
            schema,
            "Arrow error: Invalid argument error: Column 'a' is declared as non-nullable but contains null values",
        ).await;
    }

    #[tokio::test]
    async fn test_schema_check_fail_unknown_column() {
        let batch = RecordBatch::try_from_iter([
            ("a", to_string_array(&["value"])),
            ("b", to_string_array(&["value"])),
        ])
        .unwrap();
        let other_batch = RecordBatch::try_from_iter([("a", to_string_array(&["value"]))]).unwrap();
        let schema = batch.schema();
        assert_schema_check_fail(
            other_batch,
            schema,
            "Arrow error: Invalid argument error: Column 'b' is declared as non-nullable but contains null values",
        ).await;
    }

    #[tokio::test]
    async fn test_schema_check_ignore_additional_metadata_in_mem() {
        let object_store: Arc<DynObjectStore> = Arc::new(object_store::memory::InMemory::default());

        let store = ParquetStorage::new(object_store);

        let meta = meta();
        let batch = RecordBatch::try_from_iter([("a", to_string_array(&["value"]))]).unwrap();
        let schema = batch.schema();

        // Serialize & upload the record batches.
        let (_iox_md, file_size) = upload(&store, &meta, batch).await;

        // add metadata to reference schema
        let schema = Arc::new(
            schema
                .as_ref()
                .clone()
                .with_metadata(HashMap::from([(String::from("foo"), String::from("bar"))])),
        );
        download(&store, &meta, Selection::All, schema, file_size)
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn test_schema_check_ignore_additional_metadata_in_file() {
        let object_store: Arc<DynObjectStore> = Arc::new(object_store::memory::InMemory::default());

        let store = ParquetStorage::new(object_store);

        let meta = meta();
        let batch = RecordBatch::try_from_iter([("a", to_string_array(&["value"]))]).unwrap();
        let schema = batch.schema();
        // add metadata to stored batch
        let batch = RecordBatch::try_new(
            Arc::new(
                schema
                    .as_ref()
                    .clone()
                    .with_metadata(HashMap::from([(String::from("foo"), String::from("bar"))])),
            ),
            batch.columns().to_vec(),
        )
        .unwrap();

        // Serialize & upload the record batches.
        let (_iox_md, file_size) = upload(&store, &meta, batch).await;

        download(&store, &meta, Selection::All, schema, file_size)
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn test_schema_check_ignores_extra_column_in_file() {
        let file_batch = RecordBatch::try_from_iter([
            ("a", to_string_array(&["value"])),
            ("b", to_string_array(&["value"])),
        ])
        .unwrap();
        let expected_batch =
            RecordBatch::try_from_iter([("a", to_string_array(&["value"]))]).unwrap();
        let schema = expected_batch.schema();
        assert_roundtrip(file_batch, Selection::All, schema, expected_batch).await;
    }

    #[tokio::test]
    async fn test_schema_check_ignores_type_for_unselected_column() {
        let file_batch = RecordBatch::try_from_iter([
            ("a", to_string_array(&["value"])),
            ("b", to_string_array(&["value"])),
        ])
        .unwrap();
        let schema_batch = RecordBatch::try_from_iter([
            ("a", to_string_array(&["value"])),
            ("b", to_int_array(&[1])),
        ])
        .unwrap();
        let schema = schema_batch.schema();
        let expected_batch =
            RecordBatch::try_from_iter([("a", to_string_array(&["value"]))]).unwrap();
        assert_roundtrip(file_batch, Selection::Some(&["a"]), schema, expected_batch).await;
    }

    fn to_string_array(strs: &[&str]) -> ArrayRef {
        let array: StringArray = strs.iter().map(|s| Some(*s)).collect();
        Arc::new(array)
    }

    fn to_int_array(vals: &[i64]) -> ArrayRef {
        let array: Int64Array = vals.iter().map(|v| Some(*v)).collect();
        Arc::new(array)
    }

    fn meta() -> IoxMetadata {
        IoxMetadata {
            object_store_id: Default::default(),
            creation_timestamp: Time::from_timestamp_nanos(42),
            namespace_id: NamespaceId::new(1),
            namespace_name: "bananas".into(),
            shard_id: ShardId::new(2),
            table_id: TableId::new(3),
            table_name: "platanos".into(),
            partition_id: PartitionId::new(4),
            partition_key: "potato".into(),
            max_sequence_number: SequenceNumber::new(11),
            compaction_level: CompactionLevel::FileNonOverlapped,
            sort_key: None,
        }
    }

    async fn upload(
        store: &ParquetStorage,
        meta: &IoxMetadata,
        batch: RecordBatch,
    ) -> (IoxParquetMetaData, usize) {
        let stream = futures::stream::iter([Ok(batch)]);
        store
            .upload(stream, meta)
            .await
            .expect("should serialize and store sucessfully")
    }

    async fn download<'a>(
        store: &ParquetStorage,
        meta: &IoxMetadata,
        selection: Selection<'_>,
        expected_schema: SchemaRef,
        file_size: usize,
    ) -> Result<RecordBatch, DataFusionError> {
        let path: ParquetFilePath = meta.into();
        let rx = store
            .read_filter(
                &Predicate::default(),
                selection,
                expected_schema,
                &path,
                file_size,
            )
            .expect("should read record batches from object store");
        let schema = rx.schema();
        datafusion::physical_plan::common::collect(rx)
            .await
            .map(|mut batches| {
                assert_eq!(batches.len(), 1);
                let batch = batches.remove(0);
                assert_eq!(batch.schema(), schema);
                batch
            })
    }

    async fn assert_roundtrip(
        upload_batch: RecordBatch,
        selection: Selection<'_>,
        expected_schema: SchemaRef,
        expected_batch: RecordBatch,
    ) {
        let object_store: Arc<DynObjectStore> = Arc::new(object_store::memory::InMemory::default());

        let store = ParquetStorage::new(object_store);

        // Serialize & upload the record batches.
        let meta = meta();
        let (_iox_md, file_size) = upload(&store, &meta, upload_batch).await;

        // And compare to the original input
        let actual_batch = download(&store, &meta, selection, expected_schema, file_size)
            .await
            .unwrap();
        assert_eq!(actual_batch, expected_batch);
    }

    async fn assert_schema_check_fail(
        persisted_batch: RecordBatch,
        expected_schema: SchemaRef,
        msg: &str,
    ) {
        let object_store: Arc<DynObjectStore> = Arc::new(object_store::memory::InMemory::default());

        let store = ParquetStorage::new(object_store);

        let meta = meta();
        let (_iox_md, file_size) = upload(&store, &meta, persisted_batch).await;

        let err = download(&store, &meta, Selection::All, expected_schema, file_size)
            .await
            .unwrap_err();

        // And compare to the original input
        assert_eq!(err.to_string(), msg);
    }
}
