use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use tokio::sync::RwLock;
use tracing::{debug, info, warn};

use crate::types::{CacheInfo, LeverageCacheFile};

/// Manager for leverage cache data.
///
/// Provides thread-safe access to max leverage data for trading pairs.
/// Loads leverage information from a JSON cache file and supports
/// periodic reload by checking file modification time.
pub struct LeverageCacheManager {
    cache_file: PathBuf,
    refresh_interval_seconds: f64,
    default_leverage: u32,
    data: Arc<RwLock<LeverageCacheFile>>,
    file_mtime: Arc<RwLock<Option<f64>>>,
    last_refresh: Arc<RwLock<Option<f64>>>,
}

impl LeverageCacheManager {
    /// Create a new LeverageCacheManager.
    ///
    /// # Arguments
    /// * `cache_file` - Path to the JSON cache file
    /// * `refresh_interval_seconds` - How often to reload cache from file
    /// * `default_leverage` - Default leverage when symbol not found
    pub fn new(cache_file: PathBuf, refresh_interval_seconds: f64, default_leverage: u32) -> Self {
        Self {
            cache_file,
            refresh_interval_seconds,
            default_leverage,
            data: Arc::new(RwLock::new(LeverageCacheFile::default())),
            file_mtime: Arc::new(RwLock::new(None)),
            last_refresh: Arc::new(RwLock::new(None)),
        }
    }

    /// Load cache from file.
    ///
    /// Checks file modification time and only reloads if the file has changed.
    /// Returns `Ok(true)` if loaded successfully or file unchanged,
    /// `Ok(false)` if file not found, `Err` on parse/IO errors.
    pub async fn load(&self) -> anyhow::Result<bool> {
        if !self.cache_file.exists() {
            warn!(
                "Leverage cache file not found: {}",
                self.cache_file.display()
            );
            return Ok(false);
        }

        // Check if file has changed via modification time
        let metadata = tokio::fs::metadata(&self.cache_file).await?;
        let current_mtime = metadata
            .modified()?
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs_f64();

        {
            let stored_mtime = self.file_mtime.read().await;
            if let Some(prev_mtime) = *stored_mtime {
                if (current_mtime - prev_mtime).abs() < f64::EPSILON {
                    return Ok(true); // No changes
                }
            }
        }

        // Read and parse the file
        let contents = tokio::fs::read_to_string(&self.cache_file).await?;
        let parsed: LeverageCacheFile = serde_json::from_str(&contents)?;

        // Count symbols for logging
        let total_symbols: usize = parsed.exchanges.values().map(|ex| ex.symbols.len()).sum();

        // Update data under write lock
        {
            let mut data = self.data.write().await;
            *data = parsed;
        }
        {
            let mut mtime = self.file_mtime.write().await;
            *mtime = Some(current_mtime);
        }
        {
            let now = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs_f64();
            let mut lr = self.last_refresh.write().await;
            *lr = Some(now);
        }

        info!(
            "Loaded leverage cache: {} symbols from {}",
            total_symbols,
            self.cache_file.display()
        );

        Ok(true)
    }

    /// Get max leverage for a symbol on an exchange.
    ///
    /// Resolution order:
    /// 1. Exact match of `symbol` in the exchange's symbols map
    /// 2. Alternative format: remove `/` and `:` from symbol (e.g. "BTC/USDT:USDT" -> "BTCUSDTUSDT")
    /// 3. Base symbol fallback: extract base (before `/` or `USDT`), find any "{base}...USDT" key
    /// 4. Falls back to `default_leverage` if nothing found
    pub async fn get_max_leverage(&self, exchange: &str, symbol: &str) -> u32 {
        let exchange_lower = exchange.to_lowercase();
        let data = self.data.read().await;

        let exchange_data = match data.exchanges.get(&exchange_lower) {
            Some(ed) => ed,
            None => {
                debug!(
                    "No leverage found for {}:{}, using default {}x",
                    exchange, symbol, self.default_leverage
                );
                return self.default_leverage;
            }
        };

        let symbols = &exchange_data.symbols;

        // 1. Exact match
        if let Some(sd) = symbols.get(symbol) {
            if let Some(lev) = sd.max_leverage {
                if lev > 0 {
                    return lev;
                }
            }
        }

        // 2. Alternative format: remove "/" and ":"
        let alt_symbol: String = symbol.chars().filter(|c| *c != '/' && *c != ':').collect();
        if alt_symbol != symbol {
            if let Some(sd) = symbols.get(&alt_symbol) {
                if let Some(lev) = sd.max_leverage {
                    if lev > 0 {
                        return lev;
                    }
                }
            }
        }

        // 3. Base symbol fallback
        let base = if let Some(idx) = symbol.find('/') {
            &symbol[..idx]
        } else if let Some(idx) = symbol.find("USDT") {
            &symbol[..idx]
        } else {
            symbol
        };

        for (sym, sd) in symbols.iter() {
            if sym.starts_with(base) && sym.contains("USDT") {
                if let Some(lev) = sd.max_leverage {
                    if lev > 0 {
                        debug!("Found leverage for {} via {}: {}x", symbol, sym, lev);
                        return lev;
                    }
                }
            }
        }

        debug!(
            "No leverage found for {}:{}, using default {}x",
            exchange, symbol, self.default_leverage
        );
        self.default_leverage
    }

    /// Get the leverage that was actually set on exchange (from `set_leverages` section).
    ///
    /// Returns `None` if not found.
    pub async fn get_set_leverage(&self, exchange: &str, symbol: &str) -> Option<u32> {
        let exchange_lower = exchange.to_lowercase();
        let data = self.data.read().await;

        data.set_leverages
            .get(&exchange_lower)
            .and_then(|ex| ex.get(symbol))
            .and_then(|entry| entry.leverage)
    }

    /// Get all max leverages for an exchange.
    ///
    /// Returns a map of symbol -> max_leverage for all symbols that have a non-zero leverage.
    pub async fn get_all_leverages(&self, exchange: &str) -> HashMap<String, u32> {
        let exchange_lower = exchange.to_lowercase();
        let data = self.data.read().await;
        let mut result = HashMap::new();

        if let Some(exchange_data) = data.exchanges.get(&exchange_lower) {
            for (symbol, sd) in &exchange_data.symbols {
                if let Some(lev) = sd.max_leverage {
                    if lev > 0 {
                        result.insert(symbol.clone(), lev);
                    }
                }
            }
        }

        result
    }

    /// Check if cache has been loaded (i.e. has at least one exchange).
    pub async fn is_loaded(&self) -> bool {
        let data = self.data.read().await;
        !data.exchanges.is_empty()
    }

    /// Get cache info for status display.
    pub async fn cache_info(&self) -> CacheInfo {
        let data = self.data.read().await;
        let last_refresh = self.last_refresh.read().await;

        let exchanges: Vec<String> = data.exchanges.keys().cloned().collect();
        let total_symbols: usize = data.exchanges.values().map(|ex| ex.symbols.len()).sum();

        let last_refresh_str = last_refresh.map(|ts| {
            // Format as ISO 8601
            let secs = ts as i64;
            let nanos = ((ts - secs as f64) * 1_000_000_000.0) as u32;
            let dt = std::time::UNIX_EPOCH + std::time::Duration::new(secs as u64, nanos);
            format!("{:?}", dt)
        });

        CacheInfo {
            cache_file: self.cache_file.display().to_string(),
            exchanges,
            total_symbols,
            updated_at: data.updated_at.clone(),
            last_refresh: last_refresh_str,
            refresh_interval_seconds: self.refresh_interval_seconds,
            default_leverage: self.default_leverage,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use tempfile::NamedTempFile;

    const TEST_DATA: &str = r#"{
        "exchanges": {
            "bybit": {
                "symbols": {
                    "BTCUSDT": {"max_leverage": 100},
                    "ETHUSDT": {"max_leverage": 75},
                    "SOLUSDT": {"max_leverage": 50}
                }
            }
        },
        "set_leverages": {
            "bybit": {
                "BTCUSDT": {"leverage": 20}
            }
        },
        "updated_at": "2024-01-01T00:00:00Z"
    }"#;

    fn create_test_file() -> NamedTempFile {
        let mut file = NamedTempFile::new().expect("Failed to create temp file");
        file.write_all(TEST_DATA.as_bytes())
            .expect("Failed to write test data");
        file.flush().expect("Failed to flush");
        file
    }

    #[tokio::test]
    async fn test_load_from_file() {
        let file = create_test_file();
        let manager = LeverageCacheManager::new(file.path().to_path_buf(), 60.0, 10);

        let result = manager.load().await;
        assert!(result.is_ok());
        assert!(result.unwrap());
        assert!(manager.is_loaded().await);
    }

    #[tokio::test]
    async fn test_is_loaded_before_load() {
        let file = create_test_file();
        let manager = LeverageCacheManager::new(file.path().to_path_buf(), 60.0, 10);

        // Before loading, is_loaded should be false
        assert!(!manager.is_loaded().await);
    }

    #[tokio::test]
    async fn test_load_file_not_found() {
        let manager = LeverageCacheManager::new(
            PathBuf::from("/tmp/nonexistent_leverage_cache_test_file.json"),
            60.0,
            10,
        );

        let result = manager.load().await;
        assert!(result.is_ok());
        assert!(!result.unwrap()); // false because file not found
    }

    #[tokio::test]
    async fn test_get_max_leverage_exact_match() {
        let file = create_test_file();
        let manager = LeverageCacheManager::new(file.path().to_path_buf(), 60.0, 10);
        manager.load().await.unwrap();

        assert_eq!(manager.get_max_leverage("bybit", "BTCUSDT").await, 100);
        assert_eq!(manager.get_max_leverage("bybit", "ETHUSDT").await, 75);
        assert_eq!(manager.get_max_leverage("bybit", "SOLUSDT").await, 50);
    }

    #[tokio::test]
    async fn test_get_max_leverage_alternative_format() {
        let file = create_test_file();
        let manager = LeverageCacheManager::new(file.path().to_path_buf(), 60.0, 10);
        manager.load().await.unwrap();

        // "BTC/USDT:USDT" -> remove / and : -> "BTCUSDTUSDT"
        // This won't match BTCUSDT exactly, but will match via base symbol fallback
        // (base = "BTC", find "BTCUSDT" which starts with "BTC" and contains "USDT")
        assert_eq!(
            manager.get_max_leverage("bybit", "BTC/USDT:USDT").await,
            100
        );
    }

    #[tokio::test]
    async fn test_get_max_leverage_base_symbol_fallback() {
        let file = create_test_file();
        let manager = LeverageCacheManager::new(file.path().to_path_buf(), 60.0, 10);
        manager.load().await.unwrap();

        // "ETH/USDT:USDT" -> base = "ETH" -> finds "ETHUSDT"
        assert_eq!(manager.get_max_leverage("bybit", "ETH/USDT:USDT").await, 75);
    }

    #[tokio::test]
    async fn test_get_max_leverage_default() {
        let file = create_test_file();
        let manager = LeverageCacheManager::new(file.path().to_path_buf(), 60.0, 10);
        manager.load().await.unwrap();

        // Unknown symbol -> default leverage
        assert_eq!(manager.get_max_leverage("bybit", "UNKNOWNUSDT").await, 10);

        // Unknown exchange -> default leverage
        assert_eq!(manager.get_max_leverage("binance", "BTCUSDT").await, 10);
    }

    #[tokio::test]
    async fn test_get_max_leverage_case_insensitive_exchange() {
        let file = create_test_file();
        let manager = LeverageCacheManager::new(file.path().to_path_buf(), 60.0, 10);
        manager.load().await.unwrap();

        assert_eq!(manager.get_max_leverage("Bybit", "BTCUSDT").await, 100);
        assert_eq!(manager.get_max_leverage("BYBIT", "BTCUSDT").await, 100);
    }

    #[tokio::test]
    async fn test_get_set_leverage_found() {
        let file = create_test_file();
        let manager = LeverageCacheManager::new(file.path().to_path_buf(), 60.0, 10);
        manager.load().await.unwrap();

        assert_eq!(manager.get_set_leverage("bybit", "BTCUSDT").await, Some(20));
    }

    #[tokio::test]
    async fn test_get_set_leverage_not_found() {
        let file = create_test_file();
        let manager = LeverageCacheManager::new(file.path().to_path_buf(), 60.0, 10);
        manager.load().await.unwrap();

        assert_eq!(manager.get_set_leverage("bybit", "ETHUSDT").await, None);
        assert_eq!(manager.get_set_leverage("binance", "BTCUSDT").await, None);
    }

    #[tokio::test]
    async fn test_get_all_leverages() {
        let file = create_test_file();
        let manager = LeverageCacheManager::new(file.path().to_path_buf(), 60.0, 10);
        manager.load().await.unwrap();

        let all = manager.get_all_leverages("bybit").await;
        assert_eq!(all.len(), 3);
        assert_eq!(all.get("BTCUSDT"), Some(&100));
        assert_eq!(all.get("ETHUSDT"), Some(&75));
        assert_eq!(all.get("SOLUSDT"), Some(&50));
    }

    #[tokio::test]
    async fn test_get_all_leverages_unknown_exchange() {
        let file = create_test_file();
        let manager = LeverageCacheManager::new(file.path().to_path_buf(), 60.0, 10);
        manager.load().await.unwrap();

        let all = manager.get_all_leverages("binance").await;
        assert!(all.is_empty());
    }

    #[tokio::test]
    async fn test_cache_info() {
        let file = create_test_file();
        let manager = LeverageCacheManager::new(file.path().to_path_buf(), 60.0, 10);
        manager.load().await.unwrap();

        let info = manager.cache_info().await;
        assert_eq!(info.exchanges, vec!["bybit".to_string()]);
        assert_eq!(info.total_symbols, 3);
        assert_eq!(info.updated_at, Some("2024-01-01T00:00:00Z".to_string()));
        assert_eq!(info.refresh_interval_seconds, 60.0);
        assert_eq!(info.default_leverage, 10);
        assert!(info.last_refresh.is_some());
    }

    #[tokio::test]
    async fn test_reload_unchanged_file() {
        let file = create_test_file();
        let manager = LeverageCacheManager::new(file.path().to_path_buf(), 60.0, 10);

        // First load
        let result = manager.load().await.unwrap();
        assert!(result);

        // Second load should return true (unchanged)
        let result = manager.load().await.unwrap();
        assert!(result);
    }
}
