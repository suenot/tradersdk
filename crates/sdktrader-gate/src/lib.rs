//! Gate.io exchange adapter.
//!
//! This module provides trading functionality for the Gate.io exchange.
//!
//! # Architecture
//!
//! The Gate.io adapter uses a service-based architecture:
//! - `trader.rs` - Main trader implementation using services (preferred)
//! - `client.rs` - Legacy implementation (kept for reference)
//! - `services/` - Dedicated services for REST and WebSocket operations
//!
//! # Authentication
//! Gate.io API v4 uses HMAC-SHA512 signatures with the following headers:
//! - `KEY` - API key
//! - `SIGN` - HMAC-SHA512 signature
//! - `Timestamp` - Unix timestamp in seconds
//!
//! # Symbol Formats
//! - Spot: BTC_USDT
//! - Futures: BTC_USDT (same format for perpetual contracts)
//!
//! # WebSocket Trading
//! Gate.io supports WebSocket trading for order operations:
//! - ~2x faster than REST API
//! - Place, cancel, and amend orders via WebSocket
//! - Per-request authentication (no persistent auth handshake)

pub mod config;
pub mod helpers;
mod client;
mod trader;
pub mod models;
pub mod services;

// Export the service-based trader as the main GateTrader
pub use trader::GateTrader;

// Also export legacy client if needed for backward compatibility
pub use client::GateTrader as GateTraderLegacy;

pub use models::*;
pub use services::{MarketStreamService, RestService, UserDataService, WsTradingService};
