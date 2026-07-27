//! Integration tests for exchange providers.
//!
//! Run dry-run tests (no credentials):
//! ```bash
//! cargo test --features integration
//! ```
//!
//! Run live tests (with credentials from environment):
//! ```bash
//! cargo test --features live --ignored
//! ```

mod common;

#[cfg(feature = "bybit")]
mod test_bybit;

#[cfg(feature = "bingx")]
mod test_bingx;
