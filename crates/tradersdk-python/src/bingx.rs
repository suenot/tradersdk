//! Python wrapper for BingX trader.

crate::macros::declare_py_trader!(
    PyBingxTrader,
    tradersdk_providers::bingx::BingxTrader,
    "BingX"
);
