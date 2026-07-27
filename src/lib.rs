//! SDKTrader - High-performance Rust SDK for cryptocurrency trading.
//!
//! This library provides a unified interface for trading across multiple
//! cryptocurrency exchanges with support for:
//!
//! - Modular exchange crates (20 exchanges via sdktrader-providers)
//! - REST and WebSocket APIs
//! - Market and limit orders
//! - Position management
//! - Real-time order updates
//!
//! # Example
//!
//! ```rust,no_run
//! use sdktrader::prelude::*;
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

pub mod models;
pub mod exchanges;

#[cfg(feature = "utils")]
pub use sdktrader_utils as utils;

#[cfg(feature = "storage")]
pub use sdktrader_storage as storage;

#[cfg(feature = "notifications")]
pub use sdktrader_notifications as notifications;

#[cfg(feature = "api")]
pub use sdktrader_api as api;

/// Prelude module for convenient imports.
pub mod prelude {
    pub use crate::models::*;
    pub use crate::exchanges::{
        create_trader,
        ExchangeTrader,
        ExchangeCredentials,
        ExchangeError,
    };
    pub use sdktrader_core::utils::timestamp_ms;
}

// Re-export main types at crate root
pub use exchanges::{create_trader, ExchangeTrader, ExchangeCredentials};
pub use models::{
    MarketType, OrderSide, OrderType, OrderStatus,
    OrderResult, OrderInfo, AccountBalance,
    TickerInfo, Orderbook,
};
