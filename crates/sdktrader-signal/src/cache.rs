use std::collections::VecDeque;
use std::collections::HashMap;
use std::time::{SystemTime, UNIX_EPOCH};

use sha2::{Sha256, Digest};
use tracing::debug;

/// Cache statistics snapshot.
#[derive(Debug, Clone)]
pub struct CacheStats {
    /// Current number of entries in the cache
    pub size: usize,
    /// Maximum capacity of the cache
    pub max_size: usize,
    /// Number of cache hits (duplicate lookups that found a match)
    pub hits: u64,
    /// Number of cache misses (lookups that did not find a match)
    pub misses: u64,
    /// Hit rate (0.0 to 1.0)
    pub hit_rate: f64,
}

/// LRU message deduplication cache with TTL.
///
/// This is a direct port of the Python `MessageCache` class from
/// `grpc_signal_server.py`. It uses SHA-256 hashing for message
/// fingerprinting, LRU eviction when the cache is full, and
/// time-based expiration of old entries.
///
/// # Design
///
/// - `map`: HashMap<hash, index_in_order> for O(1) lookup
/// - `order`: VecDeque<CacheEntry> for insertion-order tracking (LRU eviction from front)
///
/// When a hash is looked up and found (cache hit), it is moved to the back
/// of the order queue (most recently used).
pub struct MessageCache {
    max_size: usize,
    ttl_seconds: u64,
    /// Maps hash -> timestamp_ms for O(1) lookup
    map: HashMap<String, i64>,
    /// Insertion order for LRU eviction (oldest at front)
    order: VecDeque<String>,
    hits: u64,
    misses: u64,
}

impl MessageCache {
    /// Create a new message cache.
    ///
    /// # Arguments
    /// * `max_size` - Maximum number of entries (default: 10000)
    /// * `ttl_seconds` - Time-to-live for entries in seconds (default: 2592000 = 30 days)
    pub fn new(max_size: usize, ttl_seconds: u64) -> Self {
        Self {
            max_size,
            ttl_seconds,
            map: HashMap::with_capacity(max_size),
            order: VecDeque::with_capacity(max_size),
            hits: 0,
            misses: 0,
        }
    }

    /// Compute SHA-256 hash of the message text.
    ///
    /// Matches the Python implementation:
    /// `hashlib.sha256(message_text.encode('utf-8')).hexdigest()`
    fn compute_hash(message_text: &str) -> String {
        let mut hasher = Sha256::new();
        hasher.update(message_text.as_bytes());
        hex::encode(hasher.finalize())
    }

    /// Get the current time in milliseconds since UNIX epoch.
    fn now_ms() -> i64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as i64
    }

    /// Check if a message is a duplicate (exists in cache and not expired).
    ///
    /// If the message is found and not expired, it is moved to the back
    /// of the LRU queue (refreshed). If it is found but expired, it is
    /// removed from the cache.
    ///
    /// # Returns
    /// `true` if the message is a duplicate, `false` otherwise.
    pub fn contains(&mut self, message_text: &str) -> bool {
        let msg_hash = Self::compute_hash(message_text);
        let current_time_ms = Self::now_ms();

        if let Some(&timestamp_ms) = self.map.get(&msg_hash) {
            let age_seconds = (current_time_ms - timestamp_ms) / 1000;
            if age_seconds < self.ttl_seconds as i64 {
                // Not expired - move to end (most recently used)
                self.order.retain(|h| h != &msg_hash);
                self.order.push_back(msg_hash);
                self.hits += 1;
                return true;
            } else {
                // Expired - remove
                self.map.remove(&msg_hash);
                self.order.retain(|h| h != &msg_hash);
            }
        }

        self.misses += 1;
        false
    }

    /// Add a message to the cache.
    ///
    /// If the cache is full, the oldest (least recently used) entry is
    /// evicted to make room.
    pub fn add(&mut self, message_text: &str) {
        let msg_hash = Self::compute_hash(message_text);
        let current_time_ms = Self::now_ms();

        // Evict oldest entries if cache is full
        while self.map.len() >= self.max_size {
            if let Some(oldest_hash) = self.order.pop_front() {
                debug!("Evicted oldest message from cache: {}...", &oldest_hash[..8.min(oldest_hash.len())]);
                self.map.remove(&oldest_hash);
            } else {
                break;
            }
        }

        // If the hash already exists, update timestamp and move to end
        if self.map.contains_key(&msg_hash) {
            self.order.retain(|h| h != &msg_hash);
        }

        self.map.insert(msg_hash.clone(), current_time_ms);
        self.order.push_back(msg_hash);
    }

    /// Remove expired entries from the cache.
    ///
    /// # Returns
    /// The number of entries removed.
    pub fn cleanup_expired(&mut self) -> usize {
        let current_time_ms = Self::now_ms();
        let mut removed = 0;

        // Collect expired hashes
        let expired: Vec<String> = self
            .map
            .iter()
            .filter(|(_, &ts)| (current_time_ms - ts) / 1000 >= self.ttl_seconds as i64)
            .map(|(hash, _)| hash.clone())
            .collect();

        // Remove them
        for hash in &expired {
            self.map.remove(hash);
            removed += 1;
        }

        // Remove from order queue
        if removed > 0 {
            self.order.retain(|h| !expired.contains(h));
            debug!("Cleaned up {} expired cache entries", removed);
        }

        removed
    }

    /// Get the current number of entries in the cache.
    pub fn size(&self) -> usize {
        self.map.len()
    }

    /// Get a snapshot of cache statistics.
    pub fn stats(&self) -> CacheStats {
        let total = self.hits + self.misses;
        CacheStats {
            size: self.map.len(),
            max_size: self.max_size,
            hits: self.hits,
            misses: self.misses,
            hit_rate: if total > 0 {
                self.hits as f64 / total as f64
            } else {
                0.0
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_add_and_contains() {
        let mut cache = MessageCache::new(100, 3600);

        // Message should not be in cache initially
        assert!(!cache.contains("hello world"));

        // Add and check
        cache.add("hello world");
        assert!(cache.contains("hello world"));

        // Different message should not be found
        assert!(!cache.contains("goodbye world"));
    }

    #[test]
    fn test_duplicate_detection() {
        let mut cache = MessageCache::new(100, 3600);

        cache.add("message A");
        cache.add("message B");

        // Both should be detected as duplicates
        assert!(cache.contains("message A"));
        assert!(cache.contains("message B"));

        // New message should not be a duplicate
        assert!(!cache.contains("message C"));
    }

    #[test]
    fn test_lru_eviction() {
        let mut cache = MessageCache::new(3, 3600);

        cache.add("msg1");
        cache.add("msg2");
        cache.add("msg3");
        assert_eq!(cache.size(), 3);

        // Adding a 4th message should evict the oldest (msg1)
        cache.add("msg4");
        assert_eq!(cache.size(), 3);

        // msg1 should have been evicted
        assert!(!cache.contains("msg1"));
        // msg2, msg3, msg4 should still be present
        assert!(cache.contains("msg2"));
        assert!(cache.contains("msg3"));
        assert!(cache.contains("msg4"));
    }

    #[test]
    fn test_ttl_expiry() {
        // Use a TTL of 0 seconds so entries expire immediately
        let mut cache = MessageCache::new(100, 0);

        cache.add("expiring message");

        // With TTL=0, `contains` will see the entry as expired
        // (age_seconds >= 0 which is >= ttl_seconds=0)
        // Actually the check is `age_seconds < ttl_seconds`, so with ttl=0,
        // the condition `age < 0` is always false -> always expired.
        assert!(!cache.contains("expiring message"));
    }

    #[test]
    fn test_cleanup_expired() {
        // TTL of 0 means everything expires immediately
        let mut cache = MessageCache::new(100, 0);

        cache.add("msg1");
        cache.add("msg2");
        cache.add("msg3");
        assert_eq!(cache.size(), 3);

        let removed = cache.cleanup_expired();
        assert_eq!(removed, 3);
        assert_eq!(cache.size(), 0);
    }

    #[test]
    fn test_stats() {
        let mut cache = MessageCache::new(100, 3600);

        // Initial stats
        let stats = cache.stats();
        assert_eq!(stats.size, 0);
        assert_eq!(stats.max_size, 100);
        assert_eq!(stats.hits, 0);
        assert_eq!(stats.misses, 0);
        assert_eq!(stats.hit_rate, 0.0);

        // Add a message and check it (1 miss from contains, then add, then 1 hit)
        cache.contains("test"); // miss
        cache.add("test");
        cache.contains("test"); // hit

        let stats = cache.stats();
        assert_eq!(stats.hits, 1);
        assert_eq!(stats.misses, 1);
        assert!((stats.hit_rate - 0.5).abs() < f64::EPSILON);
    }

    #[test]
    fn test_sha256_hash() {
        // Verify our hash matches Python's hashlib.sha256("test".encode('utf-8')).hexdigest()
        let hash = MessageCache::compute_hash("test");
        assert_eq!(
            hash,
            "9f86d081884c7d659a2feaa0c55ad015a3bf4f1b2b0b822cd15d6c15b0f00a08"
        );
    }

    #[test]
    fn test_readd_same_message() {
        let mut cache = MessageCache::new(100, 3600);

        cache.add("same message");
        cache.add("same message"); // re-adding should update timestamp, not duplicate

        assert_eq!(cache.size(), 1);
        assert!(cache.contains("same message"));
    }
}
