//! Macro to generate PyO3 exchange trader wrappers.
//!
//! Eliminates boilerplate duplication across exchange implementations.
//! Each exchange trader wraps a Rust `ExchangeTrader` impl behind `Arc<T>`
//! and exposes it to Python via `#[pyclass]`/`#[pymethods]`.

/// Generate a complete PyO3 wrapper for an exchange trader.
///
/// Usage:
/// ```ignore
/// declare_py_trader!(
///     PyBybitTrader,                            // Python class name
///     sdktrader_providers::bybit::BybitTrader,  // Full path to Rust trader type
///     "Bybit"                                   // Exchange display name
/// );
/// ```
macro_rules! declare_py_trader {
    (
        $py_name:ident,
        $rust_type:path,
        $display_name:expr
    ) => {
        use pyo3::prelude::*;
        use pyo3::types::PyDict;
        use std::sync::Arc;

        use sdktrader_core::ExchangeTrader;

        use crate::models::*;

        #[doc = concat!("Python-accessible ", $display_name, " trader backed by Rust.")]
        #[pyclass]
        pub struct $py_name {
            inner: Arc<$rust_type>,  // full path, e.g. sdktrader_providers::bybit::BybitTrader
        }

        #[pymethods]
        impl $py_name {
            #[doc = concat!("Create a new ", $display_name, " trader.")]
            ///
            /// Args:
            ///     api_key: API key
            ///     api_secret: API secret
            ///     testnet: Whether to use testnet
            ///     market_type: "spot" or "swap"
            #[new]
            fn new(api_key: &str, api_secret: &str, testnet: bool, market_type: &str) -> PyResult<Self> {
                let mt = parse_market_type(market_type)?;
                let trader = <$rust_type>::new(api_key, api_secret, testnet, mt);
                Ok(Self {
                    inner: Arc::new(trader),
                })
            }

            fn __repr__(&self) -> String {
                format!(
                    concat!(stringify!($py_name), "(exchange='{}', market_type='{}')"),
                    self.inner.exchange_name(),
                    format!("{:?}", self.inner.market_type())
                )
            }

            /// Get exchange name.
            fn exchange_name(&self) -> String {
                self.inner.exchange_name().to_string()
            }

            /// Get market type.
            fn market_type(&self) -> String {
                format!("{:?}", self.inner.market_type())
            }

            /// Get current price for a symbol.
            fn get_current_price<'py>(&self, py: Python<'py>, symbol: &str) -> PyResult<Bound<'py, PyAny>> {
                let trader = self.inner.clone();
                let symbol = symbol.to_string();
                pyo3_async_runtimes::tokio::future_into_py(py, async move {
                    trader
                        .get_current_price(&symbol)
                        .await
                        .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))
                })
            }

            /// Get ticker info.
            fn get_ticker<'py>(&self, py: Python<'py>, symbol: &str) -> PyResult<Bound<'py, PyAny>> {
                let trader = self.inner.clone();
                let symbol = symbol.to_string();
                pyo3_async_runtimes::tokio::future_into_py(py, async move {
                    let ticker = trader
                        .get_ticker(&symbol)
                        .await
                        .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
                    Python::attach(|py| ticker_to_pydict(py, &ticker))
                })
            }

            /// Get orderbook.
            fn get_orderbook<'py>(
                &self,
                py: Python<'py>,
                symbol: &str,
                depth: u32,
            ) -> PyResult<Bound<'py, PyAny>> {
                let trader = self.inner.clone();
                let symbol = symbol.to_string();
                pyo3_async_runtimes::tokio::future_into_py(py, async move {
                    let ob = trader
                        .get_orderbook(&symbol, depth)
                        .await
                        .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
                    Python::attach(|py| orderbook_to_pydict(py, &ob))
                })
            }

            /// Get account balance.
            fn get_balance<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
                let trader = self.inner.clone();
                pyo3_async_runtimes::tokio::future_into_py(py, async move {
                    let balance = trader
                        .get_balance()
                        .await
                        .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
                    Python::attach(|py| account_balance_to_pydict(py, &balance))
                })
            }

            /// Place a market order.
            fn place_market_order<'py>(
                &self,
                py: Python<'py>,
                symbol: &str,
                side: &str,
                quantity: f64,
            ) -> PyResult<Bound<'py, PyAny>> {
                let trader = self.inner.clone();
                let symbol = symbol.to_string();
                let order_side = parse_order_side(side)?;
                pyo3_async_runtimes::tokio::future_into_py(py, async move {
                    let result = trader
                        .place_market_order(&symbol, order_side, quantity)
                        .await
                        .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
                    Python::attach(|py| order_result_to_pydict(py, &result))
                })
            }

            /// Place a limit order.
            #[pyo3(signature = (symbol, side, quantity, price, reduce_only=false))]
            fn place_limit_order<'py>(
                &self,
                py: Python<'py>,
                symbol: &str,
                side: &str,
                quantity: f64,
                price: f64,
                reduce_only: bool,
            ) -> PyResult<Bound<'py, PyAny>> {
                let trader = self.inner.clone();
                let symbol = symbol.to_string();
                let order_side = parse_order_side(side)?;
                pyo3_async_runtimes::tokio::future_into_py(py, async move {
                    let result = trader
                        .place_limit_order(&symbol, order_side, quantity, price, reduce_only)
                        .await
                        .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
                    Python::attach(|py| order_result_to_pydict(py, &result))
                })
            }

            /// Cancel an order.
            fn cancel_order<'py>(
                &self,
                py: Python<'py>,
                symbol: &str,
                order_id: &str,
            ) -> PyResult<Bound<'py, PyAny>> {
                let trader = self.inner.clone();
                let symbol = symbol.to_string();
                let order_id = order_id.to_string();
                pyo3_async_runtimes::tokio::future_into_py(py, async move {
                    trader
                        .cancel_order(&symbol, &order_id)
                        .await
                        .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))
                })
            }

            /// Set leverage for a symbol (symbol=None for global).
            #[pyo3(signature = (symbol, leverage))]
            fn set_leverage<'py>(
                &self,
                py: Python<'py>,
                symbol: Option<&str>,
                leverage: u32,
            ) -> PyResult<Bound<'py, PyAny>> {
                let trader = self.inner.clone();
                let symbol = symbol.map(|s| s.to_string());
                pyo3_async_runtimes::tokio::future_into_py(py, async move {
                    trader
                        .set_leverage(symbol.as_deref(), leverage)
                        .await
                        .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))
                })
            }

            /// Close a position.
            fn close_position<'py>(&self, py: Python<'py>, symbol: &str) -> PyResult<Bound<'py, PyAny>> {
                let trader = self.inner.clone();
                let symbol = symbol.to_string();
                pyo3_async_runtimes::tokio::future_into_py(py, async move {
                    let result = trader
                        .close_position(&symbol)
                        .await
                        .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
                    Python::attach(|py| order_result_to_pydict(py, &result))
                })
            }

            /// Execute a fast market entry with optional TP/SL.
            #[pyo3(signature = (symbol, direction, usdt_amount, leverage, tp_percent=None, sl_percent=None, current_price=None))]
            #[allow(clippy::too_many_arguments)]
            fn execute_fast_entry<'py>(
                &self,
                py: Python<'py>,
                symbol: &str,
                direction: &str,
                usdt_amount: f64,
                leverage: u32,
                tp_percent: Option<f64>,
                sl_percent: Option<f64>,
                current_price: Option<f64>,
            ) -> PyResult<Bound<'py, PyAny>> {
                let trader = self.inner.clone();
                let symbol = symbol.to_string();
                let direction = direction.to_string();
                pyo3_async_runtimes::tokio::future_into_py(py, async move {
                    let result = trader
                        .execute_fast_entry(
                            &symbol, &direction, usdt_amount, leverage,
                            tp_percent, sl_percent, current_price,
                        )
                        .await
                        .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
                    Python::attach(|py| order_result_to_pydict(py, &result))
                })
            }

            /// Get order status and information.
            fn get_order_status<'py>(
                &self,
                py: Python<'py>,
                symbol: &str,
                order_id: &str,
            ) -> PyResult<Bound<'py, PyAny>> {
                let trader = self.inner.clone();
                let symbol = symbol.to_string();
                let order_id = order_id.to_string();
                pyo3_async_runtimes::tokio::future_into_py(py, async move {
                    let info = trader
                        .get_order_status(&symbol, &order_id)
                        .await
                        .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
                    Python::attach(|py| order_info_to_pydict(py, &info))
                })
            }

            /// Format a symbol according to exchange conventions.
            fn format_symbol(&self, symbol: &str) -> String {
                self.inner.format_symbol(symbol)
            }

            /// Test connection to the exchange.
            fn test_connection<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
                let trader = self.inner.clone();
                pyo3_async_runtimes::tokio::future_into_py(py, async move {
                    trader
                        .test_connection()
                        .await
                        .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))
                })
            }

            /// Get server time.
            fn get_server_time<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
                let trader = self.inner.clone();
                pyo3_async_runtimes::tokio::future_into_py(py, async move {
                    trader
                        .get_server_time()
                        .await
                        .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))
                })
            }

            /// Batch get prices for multiple symbols. Returns None for failed lookups.
            fn get_prices_batch<'py>(
                &self,
                py: Python<'py>,
                symbols: Vec<String>,
            ) -> PyResult<Bound<'py, PyAny>> {
                let trader = self.inner.clone();
                pyo3_async_runtimes::tokio::future_into_py(py, async move {
                    let mut results: Vec<(String, Option<f64>)> = Vec::with_capacity(symbols.len());
                    for symbol in &symbols {
                        match trader.get_current_price(symbol).await {
                            Ok(price) => results.push((symbol.clone(), Some(price))),
                            Err(_) => results.push((symbol.clone(), None)),
                        }
                    }
                    Python::attach(|py| -> PyResult<Py<PyDict>> {
                        let dict = pyo3::types::PyDict::new(py);
                        for (sym, price) in &results {
                            dict.set_item(sym, *price)?;
                        }
                        Ok(dict.into())
                    })
                })
            }
        }
    };
}

pub(crate) use declare_py_trader;
