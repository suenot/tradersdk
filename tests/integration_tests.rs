//! Main integration test file.
//!
//! This file includes all integration tests when the `integration` feature is enabled.
//!
//! # Running tests
//!
//! ## Dry-run tests (no API credentials needed)
//! ```bash
//! cargo test --features integration --test integration_tests
//! ```
//!
//! ## Live tests (requires API credentials in environment)
//! ```bash
//! # Set credentials for the exchange you want to test
//! export BINANCE_API_KEY="your_key"
//! export BINANCE_API_SECRET="your_secret"
//! export BINANCE_TESTNET="true"  # Use testnet for safety
//!
//! # Run live tests
//! cargo test --features live --test integration_tests -- --ignored
//! ```
//!
//! ## Test specific exchange
//! ```bash
//! cargo test --features "integration binance" --test integration_tests binance
//! ```

#[cfg(feature = "integration")]
mod integration;
