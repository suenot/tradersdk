//! Python wrapper for Bybit trader.

crate::macros::declare_py_trader!(
    PyBybitTrader,
    tradersdk_providers::bybit::BybitTrader,
    "Bybit"
);
