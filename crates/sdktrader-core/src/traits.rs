//! Exchange trader trait - the unified interface for all exchanges.
//!
//! Unified trading interface implemented by every exchange adapter.

use crate::models::{
    AccountBalance, ExchangeCapabilities, MarketType, OrderInfo, OrderResult, OrderSide,
    Orderbook, TickerInfo, FundingRateEntry, OpenInterestEntry, OrderUpdateEvent,
    WsTestResult, OrderTestResult,
};
use anyhow::Result;
use async_trait::async_trait;
use tokio::sync::broadcast;
use tokio::task::JoinHandle;

/// Common trait for all exchange traders.
///
/// This trait provides a unified interface for interacting with different
/// cryptocurrency exchanges. All exchange implementations must implement
/// this trait.
///
/// # Example
/// ```rust,ignore
/// use sdktrader::exchanges::{ExchangeTrader, create_trader, ExchangeCredentials};
/// use sdktrader::models::MarketType;
///
/// async fn example() -> anyhow::Result<()> {
///     let creds = ExchangeCredentials::new("key", "secret");
///     let trader = create_trader("bybit", creds, MarketType::Swap)?;
///
///     let price = trader.get_current_price("BTCUSDT").await?;
///     println!("BTC price: {}", price);
///
///     Ok(())
/// }
/// ```
#[async_trait]
pub trait ExchangeTrader: Send + Sync {
    // ═══════════════════════════════════════════════════════════════════
    // BASIC INFO
    // ═══════════════════════════════════════════════════════════════════

    /// Get the exchange name.
    fn exchange_name(&self) -> &str;

    /// Get the market type.
    fn market_type(&self) -> MarketType;

    /// Get the capabilities matrix for this exchange.
    ///
    /// Returns what operations this exchange supports. Consumers should
    /// check capabilities before calling optional methods.
    fn capabilities(&self) -> ExchangeCapabilities {
        ExchangeCapabilities::default()
    }

    // ═══════════════════════════════════════════════════════════════════
    // CONNECTION & ACCOUNT
    // ═══════════════════════════════════════════════════════════════════

    /// Test the connection to the exchange.
    async fn test_connection(&self) -> Result<bool>;

    /// Get the current server time from the exchange.
    async fn get_server_time(&self) -> Result<i64>;

    /// Get account balance.
    async fn get_balance(&self) -> Result<AccountBalance>;

    // ═══════════════════════════════════════════════════════════════════
    // MARKET DATA
    // ═══════════════════════════════════════════════════════════════════

    /// Get current price for a symbol.
    async fn get_current_price(&self, symbol: &str) -> Result<f64>;

    /// Get ticker info including Open Interest and Funding Rate (REST API).
    async fn get_ticker(&self, _symbol: &str) -> Result<TickerInfo> {
        anyhow::bail!("get_ticker not implemented for {}", self.exchange_name())
    }

    /// Get orderbook via REST API.
    async fn get_orderbook(&self, _symbol: &str, _depth: u32) -> Result<Orderbook> {
        anyhow::bail!("get_orderbook not implemented for {}", self.exchange_name())
    }

    /// Get funding rate history.
    async fn get_funding_rate_history(&self, _symbol: &str, _limit: u32) -> Result<Vec<FundingRateEntry>> {
        anyhow::bail!("get_funding_rate_history not implemented for {}", self.exchange_name())
    }

    /// Get open interest history.
    async fn get_open_interest_history(&self, _symbol: &str, _interval: &str, _limit: u32) -> Result<Vec<OpenInterestEntry>> {
        anyhow::bail!("get_open_interest_history not implemented for {}", self.exchange_name())
    }

    // ═══════════════════════════════════════════════════════════════════
    // LEVERAGE
    // ═══════════════════════════════════════════════════════════════════

    /// Set leverage for a symbol.
    async fn set_leverage(&self, symbol: Option<&str>, leverage: u32) -> Result<bool>;

    // ═══════════════════════════════════════════════════════════════════
    // TRADING
    // ═══════════════════════════════════════════════════════════════════

    /// Execute a fast market entry with optional TP/SL.
    ///
    /// Places a market order and attaches take-profit / stop-loss in a single
    /// call on exchanges whose API supports it, which is meaningfully faster
    /// than submitting three separate orders.
    async fn execute_fast_entry(
        &self,
        symbol: &str,
        direction: &str,
        usdt_amount: f64,
        leverage: u32,
        tp_percent: Option<f64>,
        sl_percent: Option<f64>,
        current_price: Option<f64>,
    ) -> Result<OrderResult>;

    /// Place a market order.
    async fn place_market_order(
        &self,
        symbol: &str,
        side: OrderSide,
        quantity: f64,
    ) -> Result<OrderResult>;

    /// Place a limit order.
    async fn place_limit_order(
        &self,
        symbol: &str,
        side: OrderSide,
        quantity: f64,
        price: f64,
        reduce_only: bool,
    ) -> Result<OrderResult>;

    /// Cancel an order.
    async fn cancel_order(&self, symbol: &str, order_id: &str) -> Result<bool>;

    /// Get order status and information.
    async fn get_order_status(&self, _symbol: &str, _order_id: &str) -> Result<OrderInfo> {
        anyhow::bail!("get_order_status not implemented for {}", self.exchange_name())
    }

    /// Close a position.
    async fn close_position(&self, symbol: &str) -> Result<OrderResult>;

    // ═══════════════════════════════════════════════════════════════════
    // SYMBOL FORMATTING
    // ═══════════════════════════════════════════════════════════════════

    /// Get exchange-specific symbol format.
    ///
    /// Converts a symbol to the exchange format.
    /// Accepts formats: "BTC/USDT", "BTCUSDT", "BTC" -> "BTCUSDT"
    fn format_symbol(&self, base_symbol: &str) -> String {
        let symbol = base_symbol.to_uppercase();
        // Handle BTC/USDT format
        if symbol.contains('/') {
            return symbol.replace("/", "");
        }
        if symbol.ends_with("USDT") || symbol.ends_with("USD") {
            return symbol;
        }
        format!("{}USDT", symbol)
    }

    // ═══════════════════════════════════════════════════════════════════
    // WEBSOCKET TESTING
    // ═══════════════════════════════════════════════════════════════════

    /// Test WebSocket orderbook subscription.
    async fn test_ws_orderbook(&self, _symbol: &str, _depth: u32, _timeout_ms: u64) -> Result<WsTestResult> {
        anyhow::bail!("test_ws_orderbook not implemented for {}", self.exchange_name())
    }

    /// Test WebSocket trades subscription.
    async fn test_ws_trades(&self, _symbol: &str, _timeout_ms: u64) -> Result<WsTestResult> {
        anyhow::bail!("test_ws_trades not implemented for {}", self.exchange_name())
    }

    /// Test WebSocket ticker subscription (includes OI, Funding Rate for perpetuals).
    async fn test_ws_ticker(&self, _symbol: &str, _timeout_ms: u64) -> Result<WsTestResult> {
        anyhow::bail!("test_ws_ticker not implemented for {}", self.exchange_name())
    }

    // ═══════════════════════════════════════════════════════════════════
    // ORDER TESTING (DRY RUN)
    // ═══════════════════════════════════════════════════════════════════

    /// Test market order creation (dry run - validates without execution).
    async fn test_market_order(&self, symbol: &str, side: OrderSide, usdt_amount: f64) -> Result<OrderTestResult> {
        // Default: get price and calculate quantity
        let price = self.get_current_price(symbol).await?;
        let quantity = usdt_amount / price;

        Ok(OrderTestResult {
            valid: true,
            order_type: "MARKET".to_string(),
            side: side.to_string(),
            symbol: symbol.to_string(),
            quantity,
            price: Some(price),
            estimated_cost: usdt_amount,
            error: None,
        })
    }

    /// Test limit order creation (dry run - validates without execution).
    async fn test_limit_order(&self, symbol: &str, side: OrderSide, usdt_amount: f64, price: f64) -> Result<OrderTestResult> {
        let quantity = usdt_amount / price;

        Ok(OrderTestResult {
            valid: true,
            order_type: "LIMIT".to_string(),
            side: side.to_string(),
            symbol: symbol.to_string(),
            quantity,
            price: Some(price),
            estimated_cost: usdt_amount,
            error: None,
        })
    }

    // ═══════════════════════════════════════════════════════════════════
    // PRIVATE WEBSOCKET
    // ═══════════════════════════════════════════════════════════════════

    /// Subscribe to real-time order updates via private WebSocket.
    ///
    /// Spawns a background tokio task that maintains a persistent WebSocket connection
    /// to the exchange's private channel and publishes order update events.
    /// Auto-reconnects on disconnect with exponential backoff.
    ///
    /// Returns a JoinHandle that can be aborted for graceful shutdown.
    async fn subscribe_order_updates(
        &self,
        _sender: broadcast::Sender<OrderUpdateEvent>,
    ) -> Result<JoinHandle<()>> {
        anyhow::bail!("subscribe_order_updates not implemented for {}", self.exchange_name())
    }
}
