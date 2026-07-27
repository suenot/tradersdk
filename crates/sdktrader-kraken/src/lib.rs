//! Kraken exchange adapter.
//!
//! This module provides trading functionality for the Kraken exchange.
//! Supports both Spot and Futures markets.
//!
//! # API Endpoints
//! - Spot REST: https://api.kraken.com
//! - Futures REST: https://futures.kraken.com
//! - Spot WebSocket: wss://ws.kraken.com
//! - Futures WebSocket: wss://futures.kraken.com/ws/v1
//!
//! # Authentication
//! Kraken uses HMAC-SHA512 with nonce for API authentication.
//!
//! # Symbol Format
//! Kraken uses XBT instead of BTC (e.g., XBTUSD, XXBTZUSD).
//! The adapter handles automatic conversion between BTC and XBT formats.

mod client;

pub use client::KrakenTrader;
