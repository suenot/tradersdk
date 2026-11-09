//! # tradersdk-leverage-cache
//!
//! JSON file-based max leverage cache for trading pairs per exchange.
//!
//! This crate provides a thread-safe, async manager for looking up maximum
//! leverage values from a JSON cache file. It supports:
//!
//! - Loading leverage data from a JSON cache file
//! - File modification time tracking to avoid unnecessary reloads
//! - Flexible symbol matching (exact, normalized, base symbol fallback)
//! - Thread-safe concurrent access via `tokio::sync::RwLock`
//!
//! # Example
//!
//! ```no_run
//! use std::path::PathBuf;
//! use tradersdk_leverage_cache::LeverageCacheManager;
//!
//! #[tokio::main]
//! async fn main() -> anyhow::Result<()> {
//!     let manager = LeverageCacheManager::new(
//!         PathBuf::from("leverage_cache.json"),
//!         60.0,  // refresh interval seconds
//!         10,    // default leverage
//!     );
//!     manager.load().await?;
//!
//!     let lev = manager.get_max_leverage("bybit", "BTCUSDT").await;
//!     println!("Max leverage: {}x", lev);
//!     Ok(())
//! }
//! ```

pub mod manager;
pub mod types;

pub use manager::LeverageCacheManager;
pub use types::{
    CacheInfo, ExchangeLeverageData, LeverageCacheFile, SetLeverageEntry, SymbolLeverageData,
};
