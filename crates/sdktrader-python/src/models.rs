//! Python-compatible model conversion via pythonize.
//!
//! Instead of manually constructing Python dicts field-by-field,
//! we use `pythonize` to automatically convert any Serialize type
//! to a Python object. This ensures all fields are always included
//! and the conversion stays in sync with Rust struct definitions.
//!
//! For types with computed fields (Orderbook, AccountBalance),
//! we pythonize the base struct and then add the extra fields.

use pyo3::prelude::*;
use pyo3::types::{PyDict, PyList};
use sdktrader_core::models::{
    AccountBalance, MarketType, OrderInfo, OrderResult, OrderSide, Orderbook, TickerInfo,
};

/// Convert MarketType from Python string.
pub fn parse_market_type(s: &str) -> PyResult<MarketType> {
    match s.to_lowercase().as_str() {
        "spot" => Ok(MarketType::Spot),
        "swap" | "futures" | "perp" => Ok(MarketType::Swap),
        _ => Err(pyo3::exceptions::PyValueError::new_err(format!(
            "Invalid market type: '{}'. Use 'spot' or 'swap'",
            s
        ))),
    }
}

/// Convert OrderSide from Python string.
pub fn parse_order_side(s: &str) -> PyResult<OrderSide> {
    match s.to_lowercase().as_str() {
        "buy" | "long" => Ok(OrderSide::Buy),
        "sell" | "short" => Ok(OrderSide::Sell),
        _ => Err(pyo3::exceptions::PyValueError::new_err(format!(
            "Invalid order side: '{}'. Use 'buy' or 'sell'",
            s
        ))),
    }
}

/// Convert any Serialize type to a Python dict via pythonize.
fn to_pydict<T: serde::Serialize>(py: Python<'_>, value: &T) -> PyResult<Py<PyDict>> {
    let obj = pythonize::pythonize(py, value).map_err(|e| {
        pyo3::exceptions::PyRuntimeError::new_err(format!("Serialization error: {}", e))
    })?;
    let dict = obj
        .cast::<PyDict>()
        .map_err(|_| pyo3::exceptions::PyRuntimeError::new_err("Expected dict from pythonize"))?;
    Ok(dict.clone().unbind())
}

/// Convert OrderResult to Python dict.
pub fn order_result_to_pydict(py: Python<'_>, result: &OrderResult) -> PyResult<Py<PyDict>> {
    to_pydict(py, result)
}

/// Convert AccountBalance to Python dict.
///
/// Adds computed `total` field to each balance entry for backward
/// compatibility with the previous manual dict construction.
pub fn account_balance_to_pydict(py: Python<'_>, balance: &AccountBalance) -> PyResult<Py<PyDict>> {
    let dict = to_pydict(py, balance)?;

    // Add computed `total` field to each nested balance dict.
    let bound = dict.bind(py);
    if let Some(balances_obj) = bound.get_item("balances")? {
        let balances_list = balances_obj.cast::<PyList>()?;
        for (i, item) in balances_list.iter().enumerate() {
            let bd = item.cast::<PyDict>()?;
            bd.set_item("total", balance.balances[i].total())?;
        }
    }

    Ok(dict)
}

/// Convert TickerInfo to Python dict.
pub fn ticker_to_pydict(py: Python<'_>, ticker: &TickerInfo) -> PyResult<Py<PyDict>> {
    to_pydict(py, ticker)
}

/// Convert OrderInfo to Python dict.
pub fn order_info_to_pydict(py: Python<'_>, info: &OrderInfo) -> PyResult<Py<PyDict>> {
    to_pydict(py, info)
}

/// Convert Orderbook to Python dict.
///
/// Adds computed `best_bid`, `best_ask`, and `spread` fields
/// for backward compatibility with the previous manual dict construction.
pub fn orderbook_to_pydict(py: Python<'_>, ob: &Orderbook) -> PyResult<Py<PyDict>> {
    let dict = to_pydict(py, ob)?;

    // Add computed fields that are not part of the serialized struct.
    let bound = dict.bind(py);
    bound.set_item("best_bid", ob.best_bid())?;
    bound.set_item("best_ask", ob.best_ask())?;
    bound.set_item("spread", ob.spread())?;

    Ok(dict)
}
