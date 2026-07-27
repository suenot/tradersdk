//! Deribit exchange adapter.
//!
//! This module provides trading functionality for the Deribit exchange.
//! Deribit is a major derivatives exchange focusing on BTC and ETH options/futures.

mod client;
pub mod models;

pub use client::DeribitTrader;
pub use models::*;
