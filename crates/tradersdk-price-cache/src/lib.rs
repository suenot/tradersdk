//! Price cache with CoinGecko integration, rate limiting, and file persistence.
//!
//! This crate provides:
//! - [`PriceCache`] -- thread-safe in-memory price cache with JSON file persistence
//! - [`CoinGeckoClient`] -- rate-limited CoinGecko API client
//! - [`RateLimiter`] -- token-bucket rate limiter for API requests
//! - [`PriceData`] / [`PriceCacheData`] -- serializable price data types

pub mod cache;
pub mod coingecko;
pub mod rate_limiter;
pub mod types;

pub use cache::{CacheStats, PriceCache};
pub use coingecko::{CoinGeckoClient, CoinMarketData};
pub use rate_limiter::RateLimiter;
pub use types::{PriceCacheData, PriceData};
