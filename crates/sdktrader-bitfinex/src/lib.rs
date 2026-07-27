//! Bitfinex exchange adapter.
//!
//! This module provides trading functionality for the Bitfinex exchange.
//!
//! ## API Endpoints
//! - REST API v2: https://api-pub.bitfinex.com/v2
//! - Authenticated REST API: https://api.bitfinex.com/v2
//! - WebSocket: wss://api-pub.bitfinex.com/ws/2
//!
//! ## Authentication
//! - Uses HMAC-SHA384 signature

mod client;

pub use client::BitfinexTrader;
