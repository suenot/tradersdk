//! HTTP API module for exchange adapter testing.
//!
//! This module provides a REST API server compatible with the terminalus frontend.
//!
//! ## Endpoints
//!
//! - `GET /api/v1/health` - Health check
//! - `GET /api/v1/exchanges` - List supported exchanges
//! - `GET /api/v1/exchanges/methods` - List available test methods
//! - `GET /api/v1/exchanges/bots` - List bots from config.json
//! - `POST /api/v1/exchanges/test` - Test an exchange adapter

pub mod config;
pub mod handlers;
pub mod server;
pub mod types;

pub use config::Config;
pub use handlers::AppState;
pub use server::ApiServer;
pub use types::*;
