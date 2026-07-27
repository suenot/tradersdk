//! Hyperliquid exchange adapter.
//!
//! This module provides trading functionality for the Hyperliquid exchange.
//! Hyperliquid is a decentralized perpetual futures exchange on Arbitrum.
//!
//! # Architecture
//!
//! The Hyperliquid adapter uses a service-based architecture:
//! - `trader.rs` - Main trader implementation using services (preferred)
//! - `client.rs` - Legacy implementation (kept for reference)
//! - `services/` - Dedicated services for REST and WebSocket operations
//!
//! # Authentication
//! Hyperliquid uses Ethereum signatures (EIP-712) for authentication.
//! - `api_key` is your Ethereum wallet address
//! - `api_secret` is your private key (hex string, with or without 0x prefix)
//!
//! # Features
//! - Perpetual futures trading
//! - Spot trading
//! - Real-time WebSocket data feeds
//! - Order updates via WebSocket (no authentication required, uses wallet address)

pub mod config;
pub mod helpers;
mod client;
mod trader;
pub mod models;
pub mod services;

// Export the service-based trader as the main HyperliquidTrader
pub use trader::HyperliquidTrader;

// Also export legacy client if needed for backward compatibility
pub use client::HyperliquidTrader as HyperliquidTraderLegacy;

pub use models::*;
pub use services::{MarketStreamService, RestService, UserDataService};
