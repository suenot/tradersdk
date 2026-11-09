//! Exchange adapters for cryptocurrency trading.
//!
//! Re-exports from `tradersdk-providers` for backward compatibility.

pub use tradersdk_providers::ExchangeError;
pub use tradersdk_providers::ExchangeTrader;
pub use tradersdk_providers::{create_trader, supported_exchanges, ExchangeCredentials};

// Feature-gated exchange modules
#[cfg(feature = "bybit")]
pub use tradersdk_providers::bybit;

#[cfg(feature = "bingx")]
pub use tradersdk_providers::bingx;
