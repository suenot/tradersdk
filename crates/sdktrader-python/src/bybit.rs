//! Python wrapper for Bybit trader.

crate::macros::declare_py_trader!(
    PyBybitTrader,
    sdktrader_providers::bybit::BybitTrader,
    "Bybit"
);
