//! Python wrapper for BingX trader.

crate::macros::declare_py_trader!(
    PyBingxTrader,
    sdktrader_providers::bingx::BingxTrader,
    "BingX"
);
