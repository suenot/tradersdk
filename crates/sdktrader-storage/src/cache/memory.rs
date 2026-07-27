//! Generic in-memory cache with TTL support.

use std::collections::HashMap;
use std::hash::Hash;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::RwLock;

/// Cache entry with value and optional expiration.
#[derive(Clone)]
struct CacheEntry<V> {
    value: V,
    expires_at: Option<Instant>,
}

impl<V> CacheEntry<V> {
    fn new(value: V, ttl: Option<Duration>) -> Self {
        Self {
            value,
            expires_at: ttl.map(|d| Instant::now() + d),
        }
    }

    fn is_expired(&self) -> bool {
        self.expires_at.map(|t| Instant::now() > t).unwrap_or(false)
    }
}

/// Generic in-memory cache with TTL support.
///
/// Thread-safe cache implementation using RwLock.
/// Supports optional TTL for entries.
///
/// # Example
/// ```rust,no_run
/// use sdktrader_storage::MemoryCache;
/// use std::time::Duration;
///
/// #[tokio::main]
/// async fn main() {
///     let cache: MemoryCache<String, f64> = MemoryCache::new();
///
///     // Set with TTL
///     cache.set("BTC".to_string(), 50000.0, Some(Duration::from_secs(60))).await;
///
///     // Get value
///     if let Some(price) = cache.get(&"BTC".to_string()).await {
///         println!("BTC price: {}", price);
///     }
/// }
/// ```
pub struct MemoryCache<K, V> {
    data: Arc<RwLock<HashMap<K, CacheEntry<V>>>>,
    max_size: Option<usize>,
    default_ttl: Option<Duration>,
}

impl<K, V> MemoryCache<K, V>
where
    K: Eq + Hash + Clone + Send + Sync,
    V: Clone + Send + Sync,
{
    /// Create a new cache with no size limit.
    pub fn new() -> Self {
        Self {
            data: Arc::new(RwLock::new(HashMap::new())),
            max_size: None,
            default_ttl: None,
        }
    }

    /// Create a new cache with a maximum size.
    pub fn with_max_size(max_size: usize) -> Self {
        Self {
            data: Arc::new(RwLock::new(HashMap::with_capacity(max_size))),
            max_size: Some(max_size),
            default_ttl: None,
        }
    }

    /// Create a new cache with a default TTL.
    pub fn with_default_ttl(default_ttl: Duration) -> Self {
        Self {
            data: Arc::new(RwLock::new(HashMap::new())),
            max_size: None,
            default_ttl: Some(default_ttl),
        }
    }

    /// Set a value in the cache with optional TTL.
    pub async fn set(&self, key: K, value: V, ttl: Option<Duration>) {
        let ttl = ttl.or(self.default_ttl);
        let entry = CacheEntry::new(value, ttl);

        let mut data = self.data.write().await;

        // Check size limit
        if let Some(max) = self.max_size {
            if data.len() >= max && !data.contains_key(&key) {
                // Remove expired entries first
                data.retain(|_, e| !e.is_expired());

                // If still over limit, remove oldest (first) entry
                if data.len() >= max {
                    if let Some(k) = data.keys().next().cloned() {
                        data.remove(&k);
                    }
                }
            }
        }

        data.insert(key, entry);
    }

    /// Get a value from the cache.
    ///
    /// Returns None if the key doesn't exist or the entry has expired.
    pub async fn get(&self, key: &K) -> Option<V> {
        let data = self.data.read().await;

        data.get(key).and_then(|entry| {
            if entry.is_expired() {
                None
            } else {
                Some(entry.value.clone())
            }
        })
    }

    /// Check if a key exists in the cache and is not expired.
    pub async fn contains(&self, key: &K) -> bool {
        let data = self.data.read().await;

        data.get(key)
            .map(|entry| !entry.is_expired())
            .unwrap_or(false)
    }

    /// Remove a value from the cache.
    pub async fn remove(&self, key: &K) -> Option<V> {
        let mut data = self.data.write().await;
        data.remove(key).map(|e| e.value)
    }

    /// Clear all entries from the cache.
    pub async fn clear(&self) {
        let mut data = self.data.write().await;
        data.clear();
    }

    /// Get the number of entries in the cache (including expired).
    pub async fn len(&self) -> usize {
        let data = self.data.read().await;
        data.len()
    }

    /// Check if the cache is empty.
    pub async fn is_empty(&self) -> bool {
        let data = self.data.read().await;
        data.is_empty()
    }

    /// Remove expired entries.
    pub async fn cleanup_expired(&self) {
        let mut data = self.data.write().await;
        data.retain(|_, entry| !entry.is_expired());
    }

    /// Get all keys (including expired).
    pub async fn keys(&self) -> Vec<K> {
        let data = self.data.read().await;
        data.keys().cloned().collect()
    }
}

impl<K, V> Default for MemoryCache<K, V>
where
    K: Eq + Hash + Clone + Send + Sync,
    V: Clone + Send + Sync,
{
    fn default() -> Self {
        Self::new()
    }
}

impl<K, V> Clone for MemoryCache<K, V> {
    fn clone(&self) -> Self {
        Self {
            data: Arc::clone(&self.data),
            max_size: self.max_size,
            default_ttl: self.default_ttl,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::time::sleep;

    #[tokio::test]
    async fn test_basic_operations() {
        let cache: MemoryCache<String, i32> = MemoryCache::new();

        cache.set("key1".to_string(), 42, None).await;
        assert_eq!(cache.get(&"key1".to_string()).await, Some(42));
        assert!(cache.contains(&"key1".to_string()).await);

        cache.remove(&"key1".to_string()).await;
        assert_eq!(cache.get(&"key1".to_string()).await, None);
    }

    #[tokio::test]
    async fn test_ttl_expiration() {
        let cache: MemoryCache<String, i32> = MemoryCache::new();

        cache
            .set("key1".to_string(), 42, Some(Duration::from_millis(50)))
            .await;

        assert_eq!(cache.get(&"key1".to_string()).await, Some(42));

        sleep(Duration::from_millis(100)).await;

        assert_eq!(cache.get(&"key1".to_string()).await, None);
    }

    #[tokio::test]
    async fn test_max_size() {
        let cache: MemoryCache<String, i32> = MemoryCache::with_max_size(2);

        cache.set("key1".to_string(), 1, None).await;
        cache.set("key2".to_string(), 2, None).await;
        cache.set("key3".to_string(), 3, None).await;

        assert!(cache.len().await <= 2);
    }
}
