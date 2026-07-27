//! Python bindings for sdktrader via PyO3.
//!
//! Exposes Rust-backed exchange traders to Python for HFT-grade performance.
//! Module name: `sdktrader_rs`

use pyo3::prelude::*;

#[macro_use]
mod macros;
mod models;
mod bingx;
mod bybit;

use bingx::PyBingxTrader;
use bybit::PyBybitTrader;

/// sdktrader_rs Python module.
#[pymodule]
fn sdktrader_rs(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<PyBingxTrader>()?;
    m.add_class::<PyBybitTrader>()?;

    // Module metadata
    m.add("__version__", env!("CARGO_PKG_VERSION"))?;
    m.add("__doc__", "High-performance Rust-backed exchange traders for Python")?;

    Ok(())
}
