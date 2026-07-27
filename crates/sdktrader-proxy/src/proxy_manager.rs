//! Proxy rotation with cooldown timing.
//!
//! Proxy pool manager with cooldown tracking.

use std::collections::HashMap;
use std::time::{SystemTime, UNIX_EPOCH};

use rand::seq::SliceRandom;
use serde::Serialize;
use tracing::{debug, info};

/// Special marker for direct connection (no proxy).
pub const HOST_CONNECTION: &str = "HOST";

/// Statistics about the proxy pool.
#[derive(Debug, Clone, Serialize)]
pub struct ProxyStats {
    pub total_proxies: usize,
    pub available_proxies: usize,
    pub on_cooldown: usize,
    pub cooldown_ms: i64,
}

/// Manages proxy rotation with cooldown timing.
///
/// Proxies are filtered on creation: a proxy string is included if it contains
/// `:enabled` **or** does NOT contain `:disabled`.
///
/// When [`get_next_proxy`](ProxyManager::get_next_proxy) is called the manager
/// picks a random proxy whose cooldown has elapsed. If every proxy is still on
/// cooldown the least-recently-used proxy is returned instead.
pub struct ProxyManager {
    proxies: Vec<String>,
    last_used: HashMap<String, i64>,
    cooldown_ms: i64,
}

impl ProxyManager {
    /// Create a new `ProxyManager`.
    ///
    /// * `proxies` -- raw proxy strings (format `"protocol:host:port:user:pass:status"`).
    /// * `cooldown_ms` -- minimum milliseconds between uses of the same proxy.
    /// * `enable_host` -- if `true`, [`HOST_CONNECTION`] is appended to the pool.
    pub fn new(proxies: Vec<String>, cooldown_ms: i64, enable_host: bool) -> Self {
        let mut filtered: Vec<String> = proxies
            .into_iter()
            .filter(|p| p.contains(":enabled") || !p.contains(":disabled"))
            .collect();

        if enable_host {
            filtered.push(HOST_CONNECTION.to_string());
        }

        let proxy_count = filtered.iter().filter(|p| p.as_str() != HOST_CONNECTION).count();
        info!(
            "ProxyManager initialized with {} connections ({} proxies{}), cooldown: {}ms",
            filtered.len(),
            proxy_count,
            if enable_host { ", 1 host" } else { "" },
            cooldown_ms,
        );

        Self {
            proxies: filtered,
            last_used: HashMap::new(),
            cooldown_ms,
        }
    }

    /// Return the current timestamp in milliseconds since the UNIX epoch.
    fn now_ms() -> i64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system clock before UNIX epoch")
            .as_millis() as i64
    }

    /// Get the next available proxy.
    ///
    /// Picks a random proxy whose cooldown has elapsed.  If all proxies are on
    /// cooldown the least-recently-used one is returned (LRU fallback).
    ///
    /// Returns `None` only when the pool is empty.
    pub fn get_next_proxy(&mut self) -> Option<String> {
        if self.proxies.is_empty() {
            return None;
        }

        let now = Self::now_ms();

        // Collect proxies whose cooldown has elapsed.
        let available: Vec<&String> = self
            .proxies
            .iter()
            .filter(|p| {
                let last = self.last_used.get(p.as_str()).copied().unwrap_or(0);
                now - last >= self.cooldown_ms
            })
            .collect();

        if available.is_empty() {
            // LRU fallback -- pick the proxy used longest ago.
            let oldest = self
                .proxies
                .iter()
                .min_by_key(|p| self.last_used.get(p.as_str()).copied().unwrap_or(0));

            if let Some(proxy) = oldest {
                let last = self.last_used.get(proxy.as_str()).copied().unwrap_or(0);
                debug!(
                    "All proxies on cooldown, using oldest proxy {} (waited {}ms of {}ms)",
                    Self::get_proxy_info(proxy),
                    now - last,
                    self.cooldown_ms,
                );
                let proxy = proxy.clone();
                self.last_used.insert(proxy.clone(), now);
                return Some(proxy);
            }

            return None;
        }

        // Random selection among available proxies.
        let mut rng = rand::thread_rng();
        let selected = (*available.choose(&mut rng).unwrap()).clone();
        self.last_used.insert(selected.clone(), now);
        Some(selected)
    }

    /// Get a displayable summary of a proxy string.
    ///
    /// For normal proxies the format returned is `"protocol:host:port:status"`.
    /// For [`HOST_CONNECTION`] the string `"HOST"` is returned.
    pub fn get_proxy_info(proxy_str: &str) -> String {
        if proxy_str == HOST_CONNECTION {
            return HOST_CONNECTION.to_string();
        }

        let parts: Vec<&str> = proxy_str.split(':').collect();
        if parts.len() >= 3 {
            let protocol = parts[0];
            let host = parts[1];
            let port = parts[2];
            let status = if parts.len() > 5 {
                parts[parts.len() - 1]
            } else {
                "unknown"
            };
            format!("{protocol}:{host}:{port}:{status}")
        } else {
            proxy_str.to_string()
        }
    }

    /// Return pool statistics.
    pub fn stats(&self) -> ProxyStats {
        let now = Self::now_ms();
        let available_count = self
            .proxies
            .iter()
            .filter(|p| {
                let last = self.last_used.get(p.as_str()).copied().unwrap_or(0);
                now - last >= self.cooldown_ms
            })
            .count();

        ProxyStats {
            total_proxies: self.proxies.len(),
            available_proxies: available_count,
            on_cooldown: self.proxies.len() - available_count,
            cooldown_ms: self.cooldown_ms,
        }
    }

    /// Check whether a specific proxy is available (not on cooldown and present
    /// in the pool).
    pub fn is_available(&self, proxy: &str) -> bool {
        if !self.proxies.iter().any(|p| p == proxy) {
            return false;
        }
        let now = Self::now_ms();
        let last = self.last_used.get(proxy).copied().unwrap_or(0);
        now - last >= self.cooldown_ms
    }

    /// Return all proxies whose cooldown has elapsed.
    pub fn available_proxies(&self) -> Vec<String> {
        let now = Self::now_ms();
        self.proxies
            .iter()
            .filter(|p| {
                let last = self.last_used.get(p.as_str()).copied().unwrap_or(0);
                now - last >= self.cooldown_ms
            })
            .cloned()
            .collect()
    }

    /// Reset all cooldowns.
    pub fn reset_cooldowns(&mut self) {
        self.last_used.clear();
        info!("All proxy cooldowns reset");
    }

    /// Number of proxies in the pool (including HOST if enabled).
    pub fn len(&self) -> usize {
        self.proxies.len()
    }

    /// Returns `true` when the pool is empty.
    pub fn is_empty(&self) -> bool {
        self.proxies.is_empty()
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_proxies() -> Vec<String> {
        vec![
            "http:1.2.3.4:8080:user:pass:enabled".to_string(),
            "socks5:5.6.7.8:1080:user:pass:enabled".to_string(),
            "http:9.10.11.12:3128:user:pass:disabled".to_string(),
        ]
    }

    #[test]
    fn test_creation_filters_disabled() {
        let pm = ProxyManager::new(sample_proxies(), 1000, false);
        // The `:disabled` proxy must be excluded.
        assert_eq!(pm.len(), 2);
    }

    #[test]
    fn test_creation_with_host() {
        let pm = ProxyManager::new(sample_proxies(), 1000, true);
        // 2 enabled proxies + HOST
        assert_eq!(pm.len(), 3);
        assert!(pm.proxies.contains(&HOST_CONNECTION.to_string()));
    }

    #[test]
    fn test_get_next_proxy_returns_some() {
        let mut pm = ProxyManager::new(sample_proxies(), 0, false);
        let proxy = pm.get_next_proxy();
        assert!(proxy.is_some());
    }

    #[test]
    fn test_get_next_proxy_empty_pool() {
        let mut pm = ProxyManager::new(vec![], 0, false);
        assert!(pm.get_next_proxy().is_none());
    }

    #[test]
    fn test_cooldown_puts_proxy_on_cooldown() {
        // Use a very long cooldown so the proxy stays on cooldown.
        let mut pm = ProxyManager::new(
            vec!["http:1.2.3.4:8080:user:pass:enabled".to_string()],
            60_000,
            false,
        );
        let proxy = pm.get_next_proxy().unwrap();
        // The single proxy should now be on cooldown.
        assert!(!pm.is_available(&proxy));
    }

    #[test]
    fn test_fallback_to_lru_when_all_on_cooldown() {
        let mut pm = ProxyManager::new(
            vec![
                "http:a:1:u:p:enabled".to_string(),
                "http:b:2:u:p:enabled".to_string(),
            ],
            60_000,
            false,
        );

        // Use both proxies so all are on cooldown.
        let first = pm.get_next_proxy().unwrap();
        // Manually advance the second proxy's timestamp so we know which is "oldest".
        let second = pm
            .proxies
            .iter()
            .find(|p| p.as_str() != first)
            .unwrap()
            .clone();
        pm.last_used.insert(second.clone(), ProxyManager::now_ms());

        // Now both are on cooldown; the fallback should return `first` (the older one).
        let fallback = pm.get_next_proxy().unwrap();
        assert_eq!(fallback, first);
    }

    #[test]
    fn test_stats() {
        let pm = ProxyManager::new(sample_proxies(), 1000, false);
        let stats = pm.stats();
        assert_eq!(stats.total_proxies, 2);
        assert_eq!(stats.available_proxies, 2);
        assert_eq!(stats.on_cooldown, 0);
        assert_eq!(stats.cooldown_ms, 1000);
    }

    #[test]
    fn test_reset_cooldowns() {
        let mut pm = ProxyManager::new(
            vec!["http:a:1:u:p:enabled".to_string()],
            60_000,
            false,
        );
        let proxy = pm.get_next_proxy().unwrap();
        assert!(!pm.is_available(&proxy));
        pm.reset_cooldowns();
        assert!(pm.is_available(&proxy));
    }

    #[test]
    fn test_available_proxies() {
        let mut pm = ProxyManager::new(
            vec![
                "http:a:1:u:p:enabled".to_string(),
                "http:b:2:u:p:enabled".to_string(),
            ],
            60_000,
            false,
        );
        assert_eq!(pm.available_proxies().len(), 2);
        let _ = pm.get_next_proxy();
        assert_eq!(pm.available_proxies().len(), 1);
    }

    #[test]
    fn test_get_proxy_info_host() {
        assert_eq!(ProxyManager::get_proxy_info(HOST_CONNECTION), "HOST");
    }

    #[test]
    fn test_get_proxy_info_full_format() {
        let info = ProxyManager::get_proxy_info("http:1.2.3.4:8080:user:pass:enabled");
        assert_eq!(info, "http:1.2.3.4:8080:enabled");
    }

    #[test]
    fn test_get_proxy_info_short_format() {
        let info = ProxyManager::get_proxy_info("http:1.2.3.4:8080");
        assert_eq!(info, "http:1.2.3.4:8080:unknown");
    }

    #[test]
    fn test_is_empty() {
        let pm = ProxyManager::new(vec![], 0, false);
        assert!(pm.is_empty());
        let pm2 = ProxyManager::new(sample_proxies(), 0, false);
        assert!(!pm2.is_empty());
    }

    #[test]
    fn test_proxy_filtering_enabled_keyword() {
        let proxies = vec![
            "http:a:1:u:p:enabled".to_string(),
            "http:b:2:u:p:disabled".to_string(),
            "http:c:3:u:p".to_string(), // no status -- should be included
        ];
        let pm = ProxyManager::new(proxies, 0, false);
        // "enabled" is included, "disabled" is excluded, no-status is included
        assert_eq!(pm.len(), 2);
    }
}
