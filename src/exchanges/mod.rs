//! Exchange adapters for cryptocurrency trading.
//!
//! Re-exports from `sdktrader-providers` for backward compatibility.

pub use sdktrader_providers::{ExchangeCredentials, create_trader, supported_exchanges};
pub use sdktrader_providers::ExchangeTrader;
pub use sdktrader_providers::ExchangeError;

// Feature-gated exchange modules
#[cfg(feature = "bybit")]
pub use sdktrader_providers::bybit;

#[cfg(feature = "bingx")]
pub use sdktrader_providers::bingx;
