//! KuCoin exchange adapter.
//!
//! This module provides trading functionality for the KuCoin exchange.
//! Supports both Spot and Futures (USDT-M perpetual) markets.
//!
//! KuCoin API requires passphrase in addition to API key and secret for authentication.

mod client;

pub use client::KucoinTrader;
