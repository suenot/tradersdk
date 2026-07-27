//! BitMart exchange adapter.
//!
//! This module provides trading functionality for the BitMart exchange.
//! BitMart uses underscore-separated symbol format (e.g., BTC_USDT).

mod client;

pub use client::BitmartTrader;
