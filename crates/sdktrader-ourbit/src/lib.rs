//! Ourbit exchange adapter.
//!
//! Ourbit only supports FUTURES trading (no spot market).
//! All operations are performed via REST API (no WebSocket support).
//!
//! # Architecture
//!
//! - `config` - API URLs and endpoint constants
//! - `helpers` - Symbol formatting and conversion utilities
//! - `models` - Ourbit-specific data structures
//! - `services` - REST API service
//! - `trader` - Main trader implementation

pub mod config;
pub mod helpers;
pub mod models;
pub mod services;
mod trader;

pub use models::*;
pub use services::RestService;
pub use trader::OurbitTrader;
