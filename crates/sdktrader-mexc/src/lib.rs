//! MEXC exchange adapter.
//!
//! This module provides trading functionality for the MEXC exchange.
//!
//! # Architecture
//!
//! The MEXC adapter uses a service-based architecture:
//! - `trader.rs` - Main trader implementation using services (preferred)
//! - `client.rs` - Legacy implementation (kept for reference)
//! - `services/` - Dedicated services for REST and WebSocket operations
//!
//! # API Structure
//! MEXC has two different APIs with different authentication methods:
//! - Spot API v3: HMAC-SHA256 signature in URL, X-MEXC-APIKEY header
//! - Futures API v1: ApiKey, Request-Time, and Signature headers
//!
//! # Symbol Formats
//! - Spot: BTCUSDT
//! - Futures: BTC_USDT

pub mod config;
pub mod helpers;
mod client;
mod trader;
pub mod models;
pub mod services;

// Export the service-based trader as the main MexcTrader
pub use trader::MexcTrader;

// Also export legacy client if needed for backward compatibility
pub use client::MexcTrader as MexcTraderLegacy;

pub use models::*;
pub use services::{MarketStreamService, RestService, UserDataService};
