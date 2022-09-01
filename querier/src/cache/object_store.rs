//! Cache for immutable object store entires.
use std::{collections::HashMap, mem::size_of_val, ops::Range, sync::Arc};

use async_trait::async_trait;
use backoff::{Backoff, BackoffConfig};
use bytes::Bytes;
use cache_system::{
    backend::policy::{
        lru::{LruPolicy, ResourcePool},
        PolicyBackend,
    },
    cache::{driver::CacheDriver, metrics::CacheWithMetrics, Cache},
    loader::{metrics::MetricsLoader, FunctionLoader},
    resource_consumption::FunctionEstimator,
};
use futures::{stream::BoxStream, StreamExt};
use iox_time::TimeProvider;
use object_store::{
    path::Path, Error as ObjectStoreError, GetResult, ListResult, MultipartId, ObjectMeta,
    ObjectStore,
};
use tokio::io::AsyncWrite;
use trace::span::Span;

use super::ram::RamSize;

const CACHE_ID: &str = "object_store";

async fn read_from_store(
    store: &dyn ObjectStore,
    path: &Path,
) -> Result<Option<Bytes>, ObjectStoreError> {
    let get_result = match store.get(path).await {
        Ok(get_result) => get_result,
        Err(ObjectStoreError::NotFound { .. }) => return Ok(None),
        Err(e) => return Err(e),
    };

    let data = match get_result.bytes().await {
        Ok(data) => data,
        Err(ObjectStoreError::NotFound { .. }) => return Ok(None),
        Err(e) => return Err(e),
    };

    Ok(Some(data))
}

type CacheT = Box<
    dyn Cache<
        K = Path,
        V = Option<Bytes>,
        GetExtra = ((), Option<Span>),
        PeekExtra = ((), Option<Span>),
    >,
>;

/// Cache for object store read operation.
///
/// This assumes that objects are written once and are NEVER modified afterwards. Deletions are NOT propagated into the
/// cache.
///
/// ["Not found"](ObjectStoreError::NotFound) results are cached forever, so make sure to only retrieve objects that
/// shall exist.
#[derive(Debug)]
pub struct ObjectStoreCache {
    // this is the virtual object store
    object_store: Arc<dyn ObjectStore>,
}

impl ObjectStoreCache {
    /// Create new empty cache.
    pub fn new(
        backoff_config: BackoffConfig,
        object_store: Arc<dyn ObjectStore>,
        time_provider: Arc<dyn TimeProvider>,
        metric_registry: &metric::Registry,
        ram_pool: Arc<ResourcePool<RamSize>>,
        testing: bool,
    ) -> Self {
        let object_store_captured = Arc::clone(&object_store);
        let loader = Box::new(FunctionLoader::new(move |key: Path, _extra: ()| {
            let backoff_config = backoff_config.clone();
            let object_store = Arc::clone(&object_store_captured);

            async move {
                Backoff::new(&backoff_config)
                    .retry_all_errors::<_, _, _, ObjectStoreError>(
                        "get object from object store",
                        || async {
                            let data = read_from_store(object_store.as_ref(), &key).await?;

                            Ok(data)
                        },
                    )
                    .await
                    .expect("retry forever")
            }
        }));
        let loader = Arc::new(MetricsLoader::new(
            loader,
            CACHE_ID,
            Arc::clone(&time_provider),
            metric_registry,
            testing,
        ));

        // add to memory pool
        let mut backend = PolicyBackend::new(Box::new(HashMap::new()), Arc::clone(&time_provider));
        backend.add_policy(LruPolicy::new(
            Arc::clone(&ram_pool),
            CACHE_ID,
            Arc::new(FunctionEstimator::new(|k: &Path, v: &Option<Bytes>| {
                RamSize(
                    size_of_val(k)
                        + k.as_ref().len()
                        + size_of_val(v)
                        + v.as_ref().map(|v| v.len()).unwrap_or_default(),
                )
            })),
        ));

        let cache = CacheDriver::new(loader, backend);
        let cache = Box::new(CacheWithMetrics::new(
            cache,
            CACHE_ID,
            time_provider,
            metric_registry,
        ));

        let object_store = Arc::new(CachedObjectStore {
            cache,
            inner: object_store,
        });

        Self { object_store }
    }

    /// Get object store.
    #[allow(dead_code)]
    pub fn object_store(&self) -> &Arc<dyn ObjectStore> {
        &self.object_store
    }
}

#[derive(Debug)]
struct CachedObjectStore {
    cache: CacheT,
    inner: Arc<dyn ObjectStore>,
}

impl CachedObjectStore {
    async fn get_data(&self, location: &Path) -> Result<Bytes, ObjectStoreError> {
        self.cache
            .get(location.clone(), ((), None))
            .await
            .ok_or_else(|| ObjectStoreError::NotFound {
                path: location.to_string(),
                source: String::from("not found").into(),
            })
    }
}

impl std::fmt::Display for CachedObjectStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "CachedObjectStore")
    }
}

#[async_trait]
impl ObjectStore for CachedObjectStore {
    async fn put(&self, _location: &Path, _bytes: Bytes) -> Result<(), ObjectStoreError> {
        Err(ObjectStoreError::NotImplemented)
    }

    async fn put_multipart(
        &self,
        _location: &Path,
    ) -> Result<(MultipartId, Box<dyn AsyncWrite + Unpin + Send>), ObjectStoreError> {
        Err(ObjectStoreError::NotImplemented)
    }

    async fn abort_multipart(
        &self,
        _location: &Path,
        _multipart_id: &MultipartId,
    ) -> Result<(), ObjectStoreError> {
        Err(ObjectStoreError::NotImplemented)
    }

    async fn get(&self, location: &Path) -> Result<GetResult, ObjectStoreError> {
        let data = self.get_data(location).await?;

        Ok(GetResult::Stream(
            futures::stream::once(async move { Ok(data) }).boxed(),
        ))
    }

    async fn get_range(
        &self,
        location: &Path,
        range: Range<usize>,
    ) -> Result<Bytes, ObjectStoreError> {
        let data = self.get_data(location).await?;

        if range.end > data.len() {
            return Err(ObjectStoreError::Generic {
                store: "CachedObjectStore",
                source: format!("Out of range: len={}, range end={}", data.len(), range.end).into(),
            });
        }
        if range.start > range.end {
            return Err(ObjectStoreError::Generic {
                store: "CachedObjectStore",
                source: format!("Invalid range: start={}, end={}", range.start, range.end).into(),
            });
        }

        Ok(data.slice(range))
    }

    async fn head(&self, location: &Path) -> Result<ObjectMeta, ObjectStoreError> {
        let data = self.get_data(location).await?;

        Ok(ObjectMeta {
            location: location.clone(),
            // nobody really cares about the "last modified" field and it is wasteful to issue a HEAD request just to
            // retrieve it.
            last_modified: Default::default(),
            size: data.len(),
        })
    }

    async fn delete(&self, _location: &Path) -> Result<(), ObjectStoreError> {
        Err(ObjectStoreError::NotImplemented)
    }

    async fn list(
        &self,
        prefix: Option<&Path>,
    ) -> Result<BoxStream<'_, Result<ObjectMeta, ObjectStoreError>>, ObjectStoreError> {
        self.inner.list(prefix).await
    }

    async fn list_with_delimiter(
        &self,
        prefix: Option<&Path>,
    ) -> Result<ListResult, ObjectStoreError> {
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy(&self, _from: &Path, _to: &Path) -> Result<(), ObjectStoreError> {
        Err(ObjectStoreError::NotImplemented)
    }

    async fn copy_if_not_exists(&self, _from: &Path, _to: &Path) -> Result<(), ObjectStoreError> {
        Err(ObjectStoreError::NotImplemented)
    }
}

#[cfg(test)]
mod tests {
    use assert_matches::assert_matches;
    use futures::TryStreamExt;
    use iox_time::SystemProvider;
    use metric::{Attributes, DurationHistogram, Metric};
    use object_store::memory::InMemory;
    use object_store_metrics::ObjectStoreMetrics;

    use crate::cache::ram::test_util::test_ram_pool;

    use super::*;

    #[tokio::test]
    async fn test() {
        // set up inner store with content
        let inner = Arc::new(InMemory::new());

        let path_1 = Path::from("foo");
        let bytes_1 = Bytes::from(b"data_foo" as &'static [u8]);
        inner.put(&path_1, bytes_1.clone()).await.unwrap();

        let path_2 = Path::from("bar/1");
        let bytes_2 = Bytes::from(b"data_bar/1" as &'static [u8]);
        inner.put(&path_2, bytes_2.clone()).await.unwrap();

        let path_3 = Path::from("bar/2");
        let bytes_3 = Bytes::from(b"data_bar/2" as &'static [u8]);
        inner.put(&path_3, bytes_3.clone()).await.unwrap();

        let path_4 = Path::from("baz");
        let bytes_4 = Bytes::from(b"data_baz" as &'static [u8]);

        // set up cache
        let metric_registry = metric::Registry::new();
        let time_provider = Arc::new(SystemProvider::new());
        let instrumented_store = ObjectStoreMetrics::new(
            Arc::clone(&inner) as _,
            Arc::clone(&time_provider) as _,
            &metric_registry,
        );
        let cache = ObjectStoreCache::new(
            BackoffConfig::default(),
            Arc::new(instrumented_store),
            time_provider,
            &metric_registry,
            test_ram_pool(),
            true,
        );
        let cached_store = cache.object_store();

        // ensure "hits" are cached
        assert_eq!(get_count_hit(&metric_registry), 0);
        assert_eq!(
            cached_store
                .get(&path_1)
                .await
                .unwrap()
                .bytes()
                .await
                .unwrap(),
            bytes_1,
        );
        assert_eq!(get_count_hit(&metric_registry), 1);
        assert_eq!(
            cached_store
                .get(&path_1)
                .await
                .unwrap()
                .bytes()
                .await
                .unwrap(),
            bytes_1,
        );
        assert_eq!(get_count_hit(&metric_registry), 1);

        // ensure "misses" are cached
        assert_eq!(get_count_miss(&metric_registry), 0);
        assert_matches!(
            cached_store.get(&path_4).await.unwrap_err(),
            ObjectStoreError::NotFound { .. }
        );
        assert_eq!(get_count_miss(&metric_registry), 1);
        assert_matches!(
            cached_store.get(&path_4).await.unwrap_err(),
            ObjectStoreError::NotFound { .. }
        );
        assert_eq!(get_count_miss(&metric_registry), 1);

        // changes don't invalidate the cache
        inner.delete(&path_1).await.unwrap();
        inner.put(&path_4, bytes_4.clone()).await.unwrap();
        assert_matches!(
            cached_store.get(&path_4).await.unwrap_err(),
            ObjectStoreError::NotFound { .. }
        );
        assert_eq!(
            cached_store
                .get(&path_1)
                .await
                .unwrap()
                .bytes()
                .await
                .unwrap(),
            bytes_1,
        );
        assert_eq!(get_count_hit(&metric_registry), 1);
        assert_eq!(get_count_miss(&metric_registry), 1);

        // list operations work but are uncached
        assert_eq!(list_count(&metric_registry), 0);
        assert_eq!(
            list(cached_store.as_ref()).await,
            vec![path_2.clone(), path_3.clone(), path_4.clone()]
        );
        assert_eq!(list_count(&metric_registry), 1);
        assert_eq!(
            list(cached_store.as_ref()).await,
            vec![path_2, path_3, path_4.clone()]
        );
        assert_eq!(list_count(&metric_registry), 2);

        // list with delimiter operations work but  are uncached
        assert_eq!(
            list_with_delimiter(cached_store.as_ref()).await,
            vec![Path::from("bar")]
        );
        assert_eq!(list_count(&metric_registry), 3);
        assert_eq!(
            list_with_delimiter(cached_store.as_ref()).await,
            vec![Path::from("bar")]
        );
        assert_eq!(list_count(&metric_registry), 4);

        // listing still does NOT invalidate the cache
        assert_matches!(
            cached_store.get(&path_4).await.unwrap_err(),
            ObjectStoreError::NotFound { .. }
        );
        assert_eq!(
            cached_store
                .get(&path_1)
                .await
                .unwrap()
                .bytes()
                .await
                .unwrap(),
            bytes_1,
        );
        assert_eq!(get_count_hit(&metric_registry), 1);
        assert_eq!(get_count_miss(&metric_registry), 1);
    }

    async fn list(store: &dyn ObjectStore) -> Vec<Path> {
        let mut paths: Vec<_> = store
            .list(None)
            .await
            .unwrap()
            .map_ok(|meta| meta.location)
            .try_collect()
            .await
            .unwrap();
        paths.sort();
        paths
    }

    async fn list_with_delimiter(store: &dyn ObjectStore) -> Vec<Path> {
        let mut paths = store
            .list_with_delimiter(None)
            .await
            .unwrap()
            .common_prefixes;
        paths.sort();
        paths
    }

    fn get_count_hit(metric_registry: &metric::Registry) -> u64 {
        metric_registry
            .get_instrument::<Metric<DurationHistogram>>("object_store_op_duration")
            .unwrap()
            .get_observer(&Attributes::from(&[("op", "get"), ("result", "success")]))
            .unwrap()
            .fetch()
            .sample_count()
    }

    fn get_count_miss(metric_registry: &metric::Registry) -> u64 {
        metric_registry
            .get_instrument::<Metric<DurationHistogram>>("object_store_op_duration")
            .unwrap()
            .get_observer(&Attributes::from(&[("op", "get"), ("result", "error")]))
            .unwrap()
            .fetch()
            .sample_count()
    }

    fn list_count(metric_registry: &metric::Registry) -> u64 {
        metric_registry
            .get_instrument::<Metric<DurationHistogram>>("object_store_op_duration")
            .unwrap()
            .get_observer(&Attributes::from(&[("op", "list"), ("result", "success")]))
            .unwrap()
            .fetch()
            .sample_count()
    }
}
