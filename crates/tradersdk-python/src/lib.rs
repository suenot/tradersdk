//! Python bindings for tradersdk via PyO3.
//!
//! Exposes Rust-backed exchange traders to Python for HFT-grade performance.
//! Module name: `tradersdk`

use pyo3::prelude::*;

#[macro_use]
mod macros;
mod bingx;
mod bybit;
mod models;

use bingx::PyBingxTrader;
use bybit::PyBybitTrader;

/// tradersdk Python module.
#[pymodule]
fn tradersdk(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<PyBingxTrader>()?;
    m.add_class::<PyBybitTrader>()?;

    // Module metadata
    m.add("__version__", env!("CARGO_PKG_VERSION"))?;
    m.add(
        "__doc__",
        "High-performance Rust-backed exchange traders for Python",
    )?;

    Ok(())
}
