//! # tradersdk-warming
//!
//! Connection warming system for cryptocurrency exchange APIs.
//!
//! This crate provides:
//! - **Server time warming**: Query exchange server time endpoints to keep connections warm
//!   and measure latency.
//! - **Circuit breaker**: Automatic protection against cascading failures when an exchange
//!   endpoint becomes unresponsive.
//! - **Account detection**: Identify unique exchange accounts from bot configurations,
//!   grouping bots that share the same API credentials.
//!
//! ## Quick Start
//!
//! ```rust,no_run
//! use tradersdk_warming::warmer::ServerTimeWarmer;
//!
//! #[tokio::main]
//! async fn main() {
//!     let mut warmer = ServerTimeWarmer::new(5000);
//!     let response = warmer.warm_exchange("bybit").await;
//!     println!("Bybit server time: {:?}", response);
//! }
//! ```

pub mod account_detector;
pub mod endpoints;
pub mod types;
pub mod warmer;

// Re-export main types for convenience.
pub use account_detector::{AccountDetector, BotAccountInfo};
pub use types::{
    AccountStats, ConnectionMetrics, HealthSummary, ServerTimeResponse, UniqueAccount,
};
pub use warmer::{CircuitBreakerConfig, ServerTimeWarmer};
