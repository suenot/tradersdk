//! Binance exchange adapter.
//!
//! This module provides a unified interface for trading on Binance exchange,
//! supporting both spot and futures (perpetual) markets.
//!
//! ## Module Structure
//!
//! ```text
//! sdktrader-binance/
//! ├── lib.rs           - Module exports and documentation
//! ├── config.rs        - API URLs, endpoints, constants
//! ├── helpers.rs       - Helper functions (parsing, formatting)
//! ├── models/          - API response models
//! │   ├── mod.rs       - Model exports
//! │   ├── rest.rs      - REST API response models
//! │   └── ws.rs        - WebSocket models
//! ├── services/        - API services
//! │   ├── mod.rs       - Service exports
//! │   ├── rest.rs      - REST API client
//! │   ├── ws_trading.rs    - WebSocket trading (orders)
//! │   ├── ws_stream.rs     - Public market data streams
//! │   └── ws_private.rs    - Private user data stream
//! └── trader.rs        - BinanceTrader + ExchangeTrader impl
//! ```

pub mod config;
pub mod helpers;
mod trader;
pub mod models;
pub mod services;

pub use trader::BinanceTrader;
pub use models::*;
pub use services::{MarketStreamService, RestService, UserDataService, WsTradingService};
