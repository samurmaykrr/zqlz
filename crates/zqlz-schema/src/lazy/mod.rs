//! Lazy loading schema cache
//!
//! Provides lazy loading functionality for large database schemas,
//! only fetching data when first accessed.

mod cache;

pub use cache::{
    CacheEntry, LazyCacheConfig, LazyCacheStats, LazySchemaCache, SharedLazySchemaCache,
    new_shared_cache, new_shared_cache_with_config,
};
