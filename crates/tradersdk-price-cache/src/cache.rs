use std::collections::HashMap;
use std::path::PathBuf;

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use tokio::sync::RwLock;
use tracing::{debug, info};

use crate::types::{PriceCacheData, PriceData};

/// Cache statistics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CacheStats {
    pub total_prices: usize,
    pub last_update: f64,
    pub cache_age_hours: f64,
    pub is_fresh: bool,
}

/// Thread-safe price cache with file persistence.
///
/// Stores price data in memory with `RwLock` for concurrent access,
/// and persists to a JSON file on disk using atomic writes (temp file + rename).
pub struct PriceCache {
    data: RwLock<PriceCacheData>,
    cache_file: PathBuf,
    freshness_hours: f64,
}

impl PriceCache {
    /// Create a new price cache.
    ///
    /// # Arguments
    /// * `cache_file` - Path to the JSON cache file on disk.
    /// * `freshness_hours` - Maximum age in hours before the cache is considered stale.
    pub fn new(cache_file: PathBuf, freshness_hours: f64) -> Self {
        Self {
            data: RwLock::new(PriceCacheData::default()),
            cache_file,
            freshness_hours,
        }
    }

    /// Load cache from a JSON file on disk.
    ///
    /// If the file does not exist, this is a no-op (the cache remains empty).
    /// Returns an error only if the file exists but cannot be read or parsed.
    pub async fn load(&self) -> Result<()> {
        if !self.cache_file.exists() {
            info!(
                "Cache file {:?} does not exist, starting empty",
                self.cache_file
            );
            return Ok(());
        }

        let contents = tokio::fs::read_to_string(&self.cache_file)
            .await
            .with_context(|| format!("Failed to read cache file {:?}", self.cache_file))?;

        let cache_data: PriceCacheData = serde_json::from_str(&contents)
            .with_context(|| format!("Failed to parse cache file {:?}", self.cache_file))?;

        let num_prices = cache_data.prices.len();
        let mut data = self.data.write().await;
        *data = cache_data;

        info!(
            num_prices,
            last_full_update = data.last_full_update,
            "Loaded cache from file"
        );

        Ok(())
    }

    /// Save cache to a JSON file using atomic write (temp file + rename).
    ///
    /// Writes to a `.tmp` file first, then renames to the target path
    /// to avoid corruption if the process is interrupted mid-write.
    pub async fn save(&self) -> Result<()> {
        let data = self.data.read().await;
        let json =
            serde_json::to_string_pretty(&*data).context("Failed to serialize cache data")?;
        drop(data);

        // Ensure parent directory exists
        if let Some(parent) = self.cache_file.parent() {
            tokio::fs::create_dir_all(parent)
                .await
                .with_context(|| format!("Failed to create cache directory {:?}", parent))?;
        }

        // Atomic write: write to temp file, then rename
        let tmp_file = self.cache_file.with_extension("tmp");
        tokio::fs::write(&tmp_file, json.as_bytes())
            .await
            .with_context(|| format!("Failed to write temp cache file {:?}", tmp_file))?;

        tokio::fs::rename(&tmp_file, &self.cache_file)
            .await
            .with_context(|| {
                format!(
                    "Failed to rename temp file {:?} -> {:?}",
                    tmp_file, self.cache_file
                )
            })?;

        debug!("Saved cache to {:?}", self.cache_file);
        Ok(())
    }

    /// Check if cache is fresh enough (updated within `freshness_hours`).
    pub async fn is_fresh(&self) -> bool {
        let data = self.data.read().await;

        if data.last_full_update == 0.0 {
            return false;
        }

        let now = current_unix_timestamp();
        let age_hours = (now - data.last_full_update) / 3600.0;
        let fresh = age_hours < self.freshness_hours;

        debug!(
            age_hours = format!("{:.2}", age_hours),
            freshness_hours = self.freshness_hours,
            fresh,
            "Cache freshness check"
        );

        fresh
    }

    /// Get the USD price for a symbol.
    ///
    /// Returns `None` if the symbol is not in the cache.
    pub async fn get_price(&self, symbol: &str) -> Option<f64> {
        let data = self.data.read().await;
        let key = symbol.to_uppercase();
        data.prices.get(&key).map(|p| p.price_usd)
    }

    /// Get full price data for a symbol.
    ///
    /// Returns `None` if the symbol is not in the cache.
    pub async fn get_price_data(&self, symbol: &str) -> Option<PriceData> {
        let data = self.data.read().await;
        let key = symbol.to_uppercase();
        data.prices.get(&key).cloned()
    }

    /// Get all prices as a symbol -> price_usd map.
    pub async fn get_all_prices(&self) -> HashMap<String, f64> {
        let data = self.data.read().await;
        data.prices
            .iter()
            .map(|(symbol, pd)| (symbol.clone(), pd.price_usd))
            .collect()
    }

    /// Update prices by merging with existing data.
    ///
    /// New entries overwrite existing ones for the same symbol.
    /// Also updates `last_full_update` to the current time.
    pub async fn update_prices(&self, prices: HashMap<String, PriceData>) {
        let mut data = self.data.write().await;
        let count = prices.len();

        for (symbol, price_data) in prices {
            data.prices.insert(symbol, price_data);
        }

        data.last_full_update = current_unix_timestamp();

        info!(
            new_prices = count,
            total_prices = data.prices.len(),
            "Updated prices"
        );
    }

    /// Get cache statistics.
    pub async fn stats(&self) -> CacheStats {
        let data = self.data.read().await;
        let now = current_unix_timestamp();
        let age_hours = if data.last_full_update > 0.0 {
            (now - data.last_full_update) / 3600.0
        } else {
            f64::INFINITY
        };

        CacheStats {
            total_prices: data.prices.len(),
            last_update: data.last_full_update,
            cache_age_hours: age_hours,
            is_fresh: data.last_full_update > 0.0 && age_hours < self.freshness_hours,
        }
    }
}

/// Get the current Unix timestamp as f64 seconds.
fn current_unix_timestamp() -> f64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs_f64()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_price_data(symbol: &str, price: f64) -> PriceData {
        PriceData {
            symbol: symbol.to_string(),
            price_usd: price,
            updated_at: current_unix_timestamp(),
            name: Some(format!("{} Coin", symbol)),
            market_cap_rank: Some(1),
            price_change_24h: Some(0.5),
        }
    }

    #[tokio::test]
    async fn test_get_set_price() {
        let cache = PriceCache::new(PathBuf::from("/tmp/test_cache_get_set.json"), 3.0);

        // Initially empty
        assert_eq!(cache.get_price("BTC").await, None);

        // Insert a price
        let mut prices = HashMap::new();
        prices.insert("BTC".to_string(), make_price_data("BTC", 42000.0));
        cache.update_prices(prices).await;

        // Should be found now
        assert_eq!(cache.get_price("BTC").await, Some(42000.0));

        // Case-insensitive lookup
        assert_eq!(cache.get_price("btc").await, Some(42000.0));
    }

    #[tokio::test]
    async fn test_get_price_data() {
        let cache = PriceCache::new(PathBuf::from("/tmp/test_cache_price_data.json"), 3.0);

        let mut prices = HashMap::new();
        prices.insert(
            "ETH".to_string(),
            PriceData {
                symbol: "ETH".to_string(),
                price_usd: 2200.0,
                updated_at: 1700000000.0,
                name: Some("Ethereum".to_string()),
                market_cap_rank: Some(2),
                price_change_24h: Some(1.5),
            },
        );
        cache.update_prices(prices).await;

        let data = cache.get_price_data("ETH").await.unwrap();
        assert_eq!(data.symbol, "ETH");
        assert_eq!(data.price_usd, 2200.0);
        assert_eq!(data.name, Some("Ethereum".to_string()));
        assert_eq!(data.market_cap_rank, Some(2));

        // Non-existent symbol
        assert!(cache.get_price_data("NONEXISTENT").await.is_none());
    }

    #[tokio::test]
    async fn test_get_all_prices() {
        let cache = PriceCache::new(PathBuf::from("/tmp/test_cache_all.json"), 3.0);

        let mut prices = HashMap::new();
        prices.insert("BTC".to_string(), make_price_data("BTC", 42000.0));
        prices.insert("ETH".to_string(), make_price_data("ETH", 2200.0));
        cache.update_prices(prices).await;

        let all = cache.get_all_prices().await;
        assert_eq!(all.len(), 2);
        assert_eq!(all["BTC"], 42000.0);
        assert_eq!(all["ETH"], 2200.0);
    }

    #[tokio::test]
    async fn test_update_merges_prices() {
        let cache = PriceCache::new(PathBuf::from("/tmp/test_cache_merge.json"), 3.0);

        // First batch
        let mut prices1 = HashMap::new();
        prices1.insert("BTC".to_string(), make_price_data("BTC", 42000.0));
        cache.update_prices(prices1).await;

        // Second batch with different symbol
        let mut prices2 = HashMap::new();
        prices2.insert("ETH".to_string(), make_price_data("ETH", 2200.0));
        cache.update_prices(prices2).await;

        // Both should exist
        assert_eq!(cache.get_price("BTC").await, Some(42000.0));
        assert_eq!(cache.get_price("ETH").await, Some(2200.0));

        // Update existing symbol with new price
        let mut prices3 = HashMap::new();
        prices3.insert("BTC".to_string(), make_price_data("BTC", 43000.0));
        cache.update_prices(prices3).await;

        assert_eq!(cache.get_price("BTC").await, Some(43000.0));
        assert_eq!(cache.get_price("ETH").await, Some(2200.0));
    }

    #[tokio::test]
    async fn test_freshness_check() {
        let cache = PriceCache::new(PathBuf::from("/tmp/test_cache_fresh.json"), 3.0);

        // Empty cache is not fresh
        assert!(!cache.is_fresh().await);

        // After update, cache should be fresh
        let prices = HashMap::new();
        cache.update_prices(prices).await;
        assert!(cache.is_fresh().await);
    }

    #[tokio::test]
    async fn test_stats() {
        let cache = PriceCache::new(PathBuf::from("/tmp/test_cache_stats.json"), 3.0);

        let stats = cache.stats().await;
        assert_eq!(stats.total_prices, 0);
        assert_eq!(stats.last_update, 0.0);
        assert!(!stats.is_fresh);

        let mut prices = HashMap::new();
        prices.insert("BTC".to_string(), make_price_data("BTC", 42000.0));
        cache.update_prices(prices).await;

        let stats = cache.stats().await;
        assert_eq!(stats.total_prices, 1);
        assert!(stats.last_update > 0.0);
        assert!(stats.is_fresh);
        assert!(stats.cache_age_hours < 0.01); // just updated
    }

    #[tokio::test]
    async fn test_save_and_load() {
        let cache_file = PathBuf::from("/tmp/test_tradersdk_price_cache_save_load.json");

        // Clean up from previous test runs
        let _ = tokio::fs::remove_file(&cache_file).await;

        // Create and populate cache
        let cache = PriceCache::new(cache_file.clone(), 3.0);
        let mut prices = HashMap::new();
        prices.insert(
            "BTC".to_string(),
            PriceData {
                symbol: "BTC".to_string(),
                price_usd: 42000.0,
                updated_at: 1700000000.0,
                name: Some("Bitcoin".to_string()),
                market_cap_rank: Some(1),
                price_change_24h: Some(-2.5),
            },
        );
        cache.update_prices(prices).await;
        cache.save().await.unwrap();

        // Load into a new cache instance
        let cache2 = PriceCache::new(cache_file.clone(), 3.0);
        cache2.load().await.unwrap();

        let data = cache2.get_price_data("BTC").await.unwrap();
        assert_eq!(data.symbol, "BTC");
        assert_eq!(data.price_usd, 42000.0);
        assert_eq!(data.name, Some("Bitcoin".to_string()));
        assert_eq!(data.market_cap_rank, Some(1));
        assert_eq!(data.price_change_24h, Some(-2.5));

        // Clean up
        let _ = tokio::fs::remove_file(&cache_file).await;
    }

    #[tokio::test]
    async fn test_load_nonexistent_file() {
        let cache = PriceCache::new(PathBuf::from("/tmp/nonexistent_price_cache.json"), 3.0);
        // Loading a nonexistent file should succeed (no-op)
        cache.load().await.unwrap();
        assert_eq!(cache.get_all_prices().await.len(), 0);
    }
}
