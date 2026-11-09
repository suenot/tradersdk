//! BingX REST API response models.
//!
//! This module contains all data structures for deserializing BingX API responses,
//! including both Swap (perpetual futures) and Spot market endpoints.

use serde::Deserialize;

/// Generic BingX API response wrapper.
///
/// All BingX REST API responses follow this structure with a code indicating
/// success (0) or an error code, along with optional data payload.
#[derive(Debug, Deserialize)]
pub struct BingxResponse<T> {
    /// Response code (0 = success, non-zero = error).
    pub code: i32,
    /// Human-readable message (may be empty on success).
    #[serde(default)]
    pub msg: String,
    /// Response data payload (None on error or empty response).
    pub data: Option<T>,
    /// Debug message for troubleshooting (usually present on errors).
    #[serde(rename = "debugMsg")]
    pub debug_msg: Option<String>,
    /// Server timestamp when response was generated.
    pub timestamp: Option<i64>,
}

impl<T> BingxResponse<T> {
    /// Check if the response indicates success.
    pub fn is_success(&self) -> bool {
        self.code == 0
    }

    /// Get a human-readable error message.
    ///
    /// Returns the message field if present, otherwise the debug message,
    /// or a generic error with the code.
    pub fn error_message(&self) -> String {
        if !self.msg.is_empty() {
            self.msg.clone()
        } else if let Some(debug) = &self.debug_msg {
            debug.clone()
        } else {
            format!("BingX error code: {}", self.code)
        }
    }
}

/// Server time response data.
///
/// Used to synchronize client time with BingX servers for request signing.
#[derive(Debug, Deserialize)]
pub struct ServerTimeData {
    /// Unix timestamp in milliseconds.
    #[serde(rename = "serverTime")]
    pub server_time: i64,
}

/// Swap (perpetual futures) account balance response wrapper.
#[derive(Debug, Deserialize)]
pub struct SwapBalanceData {
    /// Balance information for the swap account.
    pub balance: Option<SwapBalance>,
}

/// Detailed swap account balance information.
///
/// Contains margin, equity, and profit/loss information for perpetual futures trading.
#[derive(Debug, Deserialize)]
pub struct SwapBalance {
    /// Available margin for opening new positions.
    #[serde(rename = "availableMargin")]
    pub available_margin: Option<String>,
    /// Total wallet balance.
    pub balance: Option<String>,
    /// Account equity (balance + unrealized PnL).
    pub equity: Option<String>,
    /// Unrealized profit/loss from open positions.
    #[serde(rename = "unrealizedProfit")]
    pub unrealized_profit: Option<String>,
    /// Realized profit/loss from closed positions.
    #[serde(rename = "realisedProfit")]
    pub realised_profit: Option<String>,
    /// Margin currently used by open positions.
    #[serde(rename = "usedMargin")]
    pub used_margin: Option<String>,
    /// Margin frozen for pending orders.
    #[serde(rename = "freezedMargin")]
    pub freezed_margin: Option<String>,
}

/// Spot account balance response wrapper.
#[derive(Debug, Deserialize)]
pub struct SpotBalanceData {
    /// List of asset balances in the spot account.
    pub balances: Vec<SpotAsset>,
}

/// Individual spot asset balance.
///
/// Represents the balance of a single asset in the spot wallet.
#[derive(Debug, Deserialize)]
pub struct SpotAsset {
    /// Asset symbol (e.g., "BTC", "USDT").
    pub asset: String,
    /// Available balance for trading.
    pub free: String,
    /// Balance locked in open orders.
    pub locked: String,
}

/// Price data for a trading pair.
///
/// Used for fetching the current price of a symbol.
#[derive(Debug, Deserialize)]
pub struct PriceData {
    /// Trading pair symbol (e.g., "BTC-USDT").
    pub symbol: Option<String>,
    /// Current price as a string (preserves precision).
    pub price: String,
}

/// 24-hour ticker data for a trading pair.
///
/// Contains comprehensive market statistics for the last 24 hours.
#[derive(Debug, Deserialize)]
pub struct TickerData {
    /// Trading pair symbol.
    pub symbol: Option<String>,
    /// Last traded price.
    #[serde(rename = "lastPrice")]
    pub last_price: Option<String>,
    /// Best bid price.
    #[serde(rename = "bidPrice")]
    pub bid_price: Option<String>,
    /// Best ask price.
    #[serde(rename = "askPrice")]
    pub ask_price: Option<String>,
    /// Highest price in 24 hours.
    #[serde(rename = "highPrice")]
    pub high_price: Option<String>,
    /// Lowest price in 24 hours.
    #[serde(rename = "lowPrice")]
    pub low_price: Option<String>,
    /// 24-hour trading volume in base currency.
    pub volume: Option<String>,
    /// 24-hour trading volume in quote currency (turnover).
    #[serde(rename = "quoteVolume")]
    pub quote_volume: Option<String>,
    /// 24-hour price change percentage.
    #[serde(rename = "priceChangePercent")]
    pub price_change_percent: Option<String>,
    /// Current open interest (for perpetual futures).
    #[serde(rename = "openInterest")]
    pub open_interest: Option<String>,
}

/// Order book (depth) data.
///
/// Contains bid and ask price levels for a trading pair.
#[derive(Debug, Deserialize)]
pub struct OrderbookData {
    /// Bid (buy) orders - each entry is [price, quantity].
    pub bids: Vec<Vec<String>>,
    /// Ask (sell) orders - each entry is [price, quantity].
    pub asks: Vec<Vec<String>>,
    /// Timestamp when the orderbook snapshot was taken.
    #[serde(rename = "T")]
    pub timestamp: Option<i64>,
}

/// Order response data.
///
/// Returned when placing, querying, or cancelling orders.
#[derive(Debug, Deserialize)]
pub struct OrderData {
    /// Unique order identifier (can be string or number from API).
    #[serde(rename = "orderId")]
    pub order_id: Option<serde_json::Value>,
    /// Trading pair symbol.
    pub symbol: Option<String>,
    /// Order side ("BUY" or "SELL").
    pub side: Option<String>,
    /// Order type ("MARKET", "LIMIT", etc.).
    #[serde(rename = "type")]
    pub order_type: Option<String>,
    /// Order price (for limit orders).
    pub price: Option<String>,
    /// Original order quantity.
    #[serde(rename = "origQty")]
    pub orig_qty: Option<String>,
    /// Executed (filled) quantity.
    #[serde(rename = "executedQty")]
    pub executed_qty: Option<String>,
    /// Average fill price.
    #[serde(rename = "avgPrice")]
    pub avg_price: Option<String>,
    /// Order status ("NEW", "FILLED", "CANCELLED", etc.).
    pub status: Option<String>,
    /// Order creation timestamp.
    pub time: Option<i64>,
    /// Client-provided order identifier.
    #[serde(rename = "clientOrderId")]
    pub client_order_id: Option<String>,
}

impl OrderData {
    /// Extract order ID as a string.
    ///
    /// Handles both string and numeric order IDs from the API.
    pub fn order_id_string(&self) -> String {
        match &self.order_id {
            Some(serde_json::Value::String(s)) => s.clone(),
            Some(serde_json::Value::Number(n)) => n.to_string(),
            _ => String::new(),
        }
    }
}

/// Position data for perpetual futures.
///
/// Contains information about an open position including size, margin, and PnL.
#[derive(Debug, Clone, Deserialize)]
pub struct PositionData {
    /// Unique position identifier.
    #[serde(rename = "positionId")]
    pub position_id: Option<String>,
    /// Trading pair symbol.
    pub symbol: String,
    /// Position direction ("LONG" or "SHORT").
    #[serde(rename = "positionSide")]
    pub position_side: String,
    /// Position size (positive for long, negative for short).
    #[serde(rename = "positionAmt")]
    pub position_amt: String,
    /// Available amount that can be closed.
    #[serde(rename = "availableAmt")]
    pub available_amt: Option<String>,
    /// Whether the position uses isolated margin.
    pub isolated: Option<bool>,
    /// Average entry price.
    #[serde(rename = "avgPrice")]
    pub avg_price: String,
    /// Initial margin used to open the position.
    #[serde(rename = "initialMargin")]
    pub initial_margin: Option<String>,
    /// Current margin for the position.
    pub margin: Option<String>,
    /// Position leverage multiplier.
    pub leverage: i32,
    /// Unrealized profit/loss.
    #[serde(rename = "unrealizedProfit")]
    pub unrealized_profit: Option<String>,
    /// Realized profit/loss.
    #[serde(rename = "realisedProfit")]
    pub realised_profit: Option<String>,
    /// Liquidation price (None if no risk of liquidation).
    #[serde(rename = "liquidationPrice")]
    pub liquidation_price: Option<f64>,
    /// Current mark price.
    #[serde(rename = "markPrice")]
    pub mark_price: Option<String>,
    /// Risk rate as a percentage.
    #[serde(rename = "riskRate")]
    pub risk_rate: Option<String>,
    /// Last update timestamp.
    #[serde(rename = "updateTime")]
    pub update_time: Option<i64>,
}

/// Leverage setting response data.
///
/// Returned when setting or querying leverage for a symbol.
#[derive(Debug, Deserialize)]
pub struct LeverageData {
    /// Trading pair symbol.
    pub symbol: Option<String>,
    /// Leverage for long positions.
    #[serde(rename = "longLeverage")]
    pub long_leverage: Option<i32>,
    /// Leverage for short positions.
    #[serde(rename = "shortLeverage")]
    pub short_leverage: Option<i32>,
    /// Combined leverage (when long/short have same value).
    pub leverage: Option<i32>,
}

/// Response data for closing all positions on a symbol.
#[derive(Debug, Deserialize)]
pub struct CloseAllPositionsData {
    /// List of successfully closed position order IDs.
    pub success: Option<Vec<serde_json::Value>>,
    /// List of positions that failed to close.
    pub failed: Option<Vec<serde_json::Value>>,
}

/// Response data for closing a single position.
#[derive(Debug, Deserialize)]
pub struct ClosePositionData {
    /// Order ID for the close position order.
    #[serde(rename = "orderId")]
    pub order_id: Option<serde_json::Value>,
    /// Position identifier that was closed.
    #[serde(rename = "positionId")]
    pub position_id: Option<String>,
    /// Trading pair symbol.
    pub symbol: Option<String>,
    /// Order side for the close order.
    pub side: Option<String>,
    /// Order type (usually "MARKET" for position close).
    #[serde(rename = "type")]
    pub order_type: Option<String>,
    /// Position side that was closed ("LONG" or "SHORT").
    #[serde(rename = "positionSide")]
    pub position_side: Option<String>,
    /// Quantity that was closed.
    #[serde(rename = "origQty")]
    pub orig_qty: Option<String>,
}

/// Funding rate data for perpetual futures.
///
/// Funding rates are periodic payments between long and short positions.
#[derive(Debug, Deserialize)]
pub struct FundingRateData {
    /// Trading pair symbol.
    pub symbol: Option<String>,
    /// Current or historical funding rate (positive = longs pay shorts).
    #[serde(rename = "fundingRate")]
    pub funding_rate: String,
    /// Timestamp when the funding rate applies/applied.
    #[serde(rename = "fundingTime")]
    pub funding_time: i64,
}

/// Open interest data for perpetual futures.
///
/// Open interest represents the total number of outstanding contracts.
#[derive(Debug, Deserialize)]
pub struct OpenInterestData {
    /// Trading pair symbol.
    pub symbol: Option<String>,
    /// Total open interest in contracts.
    #[serde(rename = "openInterest")]
    pub open_interest: String,
}

/// Listen key data for WebSocket user data streams.
///
/// The listen key is required to authenticate private WebSocket connections.
#[derive(Debug, Deserialize)]
pub struct ListenKeyData {
    /// Listen key string for WebSocket authentication.
    #[serde(rename = "listenKey")]
    pub listen_key: Option<String>,
}
