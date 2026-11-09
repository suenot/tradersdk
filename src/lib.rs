//! TraderSDK - High-performance Rust SDK for cryptocurrency trading.
//!
//! This library provides a unified interface for trading across multiple
//! cryptocurrency exchanges with support for:
//!
//! - Modular exchange crates (20 exchanges via tradersdk-providers)
//! - REST and WebSocket APIs
//! - Market and limit orders
//! - Position management
//! - Real-time order updates
//!
//! # Example
//!
//! ```rust,no_run
//! use tradersdk::prelude::*;
//!
//! #[tokio::main]
//! async fn main() -> anyhow::Result<()> {
//!     let credentials = ExchangeCredentials {
//!         api_key: "your_key".into(),
//!         api_secret: "your_secret".into(),
//!         passphrase: None,
//!         testnet: true,
//!     };
//!
//!     let trader = create_trader("bybit", credentials, MarketType::Swap)?;
//!
//!     let price = trader.get_current_price("BTCUSDT").await?;
//!     println!("BTC price: {}", price);
//!
//!     Ok(())
//! }
//! ```

pub mod exchanges;
pub mod models;

#[cfg(feature = "utils")]
pub use tradersdk_utils as utils;

#[cfg(feature = "storage")]
pub use tradersdk_storage as storage;

#[cfg(feature = "notifications")]
pub use tradersdk_notifications as notifications;

#[cfg(feature = "api")]
pub use tradersdk_api as api;

/// Prelude module for convenient imports.
pub mod prelude {
    pub use crate::exchanges::{create_trader, ExchangeCredentials, ExchangeError, ExchangeTrader};
    pub use crate::models::*;
    pub use tradersdk_core::utils::timestamp_ms;
}

// Re-export main types at crate root
pub use exchanges::{create_trader, ExchangeCredentials, ExchangeTrader};
pub use models::{
    AccountBalance, MarketType, OrderInfo, OrderResult, OrderSide, OrderStatus, OrderType,
    Orderbook, TickerInfo,
};
