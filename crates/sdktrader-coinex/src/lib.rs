//! CoinEx exchange adapter.
//!
//! This module provides trading functionality for the CoinEx exchange.
//! Supports both spot and perpetual futures trading via CoinEx API v2.
//!
//! API Documentation: https://docs.coinex.com/api/v2

mod client;

pub use client::CoinexTrader;
