//! Price cache for quick access to latest prices.

use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;

/// Fast price cache for trading operations.
///
/// Stores latest prices with update timestamps.
/// Thread-safe and optimized for high-frequency reads.
pub struct PriceCache {
    prices: Arc<RwLock<HashMap<String, PriceEntry>>>,
}

/// Price entry with timestamp.
#[derive(Clone, Debug)]
pub struct PriceEntry {
    pub price: f64,
    pub timestamp: i64,
}

impl PriceCache {
    /// Create a new price cache.
    pub fn new() -> Self {
        Self {
            prices: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Get the price for a symbol.
    pub async fn get(&self, symbol: &str) -> Option<f64> {
        let prices = self.prices.read().await;
        prices.get(symbol).map(|e| e.price)
    }

    /// Get price with timestamp.
    pub async fn get_with_timestamp(&self, symbol: &str) -> Option<PriceEntry> {
        let prices = self.prices.read().await;
        prices.get(symbol).cloned()
    }

    /// Set the price for a symbol.
    pub async fn set(&self, symbol: &str, price: f64, timestamp: i64) {
        let mut prices = self.prices.write().await;
        prices.insert(symbol.to_string(), PriceEntry { price, timestamp });
    }

    /// Update price from ticker data.
    pub async fn update_from_ticker(&self, symbol: &str, price: f64) {
        let timestamp = chrono::Utc::now().timestamp_millis();
        self.set(symbol, price, timestamp).await;
    }

    /// Get all symbols in cache.
    pub async fn symbols(&self) -> Vec<String> {
        let prices = self.prices.read().await;
        prices.keys().cloned().collect()
    }

    /// Clear all prices.
    pub async fn clear(&self) {
        let mut prices = self.prices.write().await;
        prices.clear();
    }

    /// Get price age in milliseconds.
    pub async fn price_age_ms(&self, symbol: &str) -> Option<i64> {
        let prices = self.prices.read().await;
        prices
            .get(symbol)
            .map(|e| chrono::Utc::now().timestamp_millis() - e.timestamp)
    }

    /// Check if price is stale (older than threshold).
    pub async fn is_stale(&self, symbol: &str, max_age_ms: i64) -> bool {
        self.price_age_ms(symbol)
            .await
            .map(|age| age > max_age_ms)
            .unwrap_or(true)
    }
}

impl Default for PriceCache {
    fn default() -> Self {
        Self::new()
    }
}

impl Clone for PriceCache {
    fn clone(&self) -> Self {
        Self {
            prices: Arc::clone(&self.prices),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_price_cache() {
        let cache = PriceCache::new();

        cache.update_from_ticker("BTCUSDT", 50000.0).await;

        assert_eq!(cache.get("BTCUSDT").await, Some(50000.0));
        assert!(cache.get_with_timestamp("BTCUSDT").await.is_some());
        assert!(!cache.is_stale("BTCUSDT", 10000).await);
    }
}
