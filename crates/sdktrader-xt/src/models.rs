//! XT.com-specific response models.
//!
//! This module contains all the data structures for parsing XT API responses.

use serde::{Deserialize, Serialize};

// ═══════════════════════════════════════════════════════════════════════════
// API Response Wrappers
// ═══════════════════════════════════════════════════════════════════════════

/// XT Spot API response wrapper.
/// Spot uses different field names than futures.
#[derive(Debug, Deserialize)]
pub struct XtSpotResponse<T> {
    /// Return code: 0 = success
    pub rc: i32,
    /// Message code
    pub mc: String,
    /// Message arguments
    #[serde(default)]
    pub ma: Vec<String>,
    /// Result data
    pub result: Option<T>,
}

impl<T> XtSpotResponse<T> {
    pub fn is_success(&self) -> bool {
        self.rc == 0
    }
}

/// XT Futures API response wrapper.
#[derive(Debug, Deserialize)]
pub struct XtFuturesResponse<T> {
    #[serde(rename = "returnCode")]
    pub return_code: i32,
    #[serde(rename = "msgInfo")]
    pub msg_info: String,
    pub error: Option<XtError>,
    pub result: Option<T>,
}

impl<T> XtFuturesResponse<T> {
    pub fn is_success(&self) -> bool {
        self.return_code == 0
    }
}

/// XT error details.
#[derive(Debug, Deserialize)]
pub struct XtError {
    pub code: String,
    pub msg: String,
}

// ═══════════════════════════════════════════════════════════════════════════
// Market Data Models - Spot
// ═══════════════════════════════════════════════════════════════════════════

/// Spot ticker response (24h).
#[derive(Debug, Deserialize)]
pub struct SpotTickerData {
    /// Symbol (e.g., "btc_usdt")
    pub s: String,
    /// Timestamp
    pub t: i64,
    /// Price change value (24h)
    #[serde(default)]
    pub cv: Option<String>,
    /// Price change rate (24h)
    #[serde(default)]
    pub cr: Option<String>,
    /// Open price
    #[serde(default)]
    pub o: Option<String>,
    /// Low price
    #[serde(default)]
    pub l: Option<String>,
    /// High price
    #[serde(default)]
    pub h: Option<String>,
    /// Close price (last)
    #[serde(default)]
    pub c: Option<String>,
    /// Base volume (quantity)
    #[serde(default)]
    pub q: Option<String>,
    /// Quote volume (turnover)
    #[serde(default)]
    pub v: Option<String>,
    /// Best ask price
    #[serde(default)]
    pub ap: Option<String>,
    /// Best bid price
    #[serde(default)]
    pub bp: Option<String>,
}

/// Spot orderbook response.
#[derive(Debug, Deserialize)]
pub struct SpotOrderbookData {
    /// Timestamp
    #[serde(default)]
    pub t: Option<i64>,
    /// Bids [[price, qty], ...]
    #[serde(default)]
    pub bids: Vec<Vec<String>>,
    /// Asks [[price, qty], ...]
    #[serde(default)]
    pub asks: Vec<Vec<String>>,
}

// ═══════════════════════════════════════════════════════════════════════════
// Market Data Models - Futures
// ═══════════════════════════════════════════════════════════════════════════

/// Futures aggregated ticker response.
/// This includes mark price, index price, funding rate info.
#[derive(Debug, Deserialize)]
pub struct FuturesAggTickerData {
    /// Timestamp
    pub t: i64,
    /// Symbol (e.g., "btc_usdt")
    pub s: String,
    /// Close price (last)
    pub c: String,
    /// High price
    pub h: String,
    /// Low price
    pub l: String,
    /// Base volume (amount)
    #[serde(default)]
    pub a: Option<String>,
    /// Quote volume (turnover)
    #[serde(default)]
    pub v: Option<String>,
    /// Open price
    #[serde(default)]
    pub o: Option<String>,
    /// Price change rate
    #[serde(default)]
    pub r: Option<String>,
    /// Index price
    #[serde(default)]
    pub i: Option<String>,
    /// Mark price
    #[serde(default)]
    pub m: Option<String>,
    /// Best bid price
    #[serde(default)]
    pub bp: Option<String>,
    /// Best ask price
    #[serde(default)]
    pub ap: Option<String>,
}

/// Futures funding rate response.
#[derive(Debug, Deserialize)]
pub struct FuturesFundingRateData {
    /// Symbol
    pub symbol: String,
    /// Funding rate
    #[serde(rename = "fundingRate")]
    pub funding_rate: String,
    /// Funding time (next settlement)
    #[serde(rename = "fundingTime")]
    pub funding_time: i64,
    /// Funding interval (e.g., 8 hours)
    #[serde(rename = "fundingInterval", default)]
    pub funding_interval: Option<i32>,
}

/// Futures funding rate history entry.
#[derive(Debug, Deserialize)]
pub struct FundingRateHistoryEntry {
    /// ID
    pub id: i64,
    /// Symbol
    pub symbol: String,
    /// Funding rate
    #[serde(rename = "fundingRate")]
    pub funding_rate: String,
    /// Timestamp
    #[serde(rename = "createdTime")]
    pub created_time: i64,
}

/// Futures funding rate history list.
#[derive(Debug, Deserialize)]
pub struct FundingRateHistoryResult {
    #[serde(rename = "hasPrev")]
    pub has_prev: bool,
    #[serde(rename = "hasNext")]
    pub has_next: bool,
    pub items: Vec<FundingRateHistoryEntry>,
}

/// Futures open interest response.
#[derive(Debug, Deserialize)]
pub struct FuturesOpenInterestData {
    /// Symbol
    pub symbol: String,
    /// Open interest (contracts)
    #[serde(rename = "openInterest")]
    pub open_interest: String,
    /// Open interest value (USDT)
    #[serde(rename = "openInterestValue", default)]
    pub open_interest_value: Option<String>,
    /// Timestamp
    #[serde(default)]
    pub timestamp: Option<i64>,
}

/// Futures orderbook response.
#[derive(Debug, Deserialize)]
pub struct FuturesOrderbookData {
    /// Timestamp
    pub t: i64,
    /// Symbol
    #[serde(default)]
    pub s: Option<String>,
    /// Bids [[price, qty], ...]
    #[serde(default)]
    pub b: Vec<Vec<String>>,
    /// Asks [[price, qty], ...]
    #[serde(default)]
    pub a: Vec<Vec<String>>,
}

// ═══════════════════════════════════════════════════════════════════════════
// Account Models
// ═══════════════════════════════════════════════════════════════════════════

/// Spot balance entry.
#[derive(Debug, Deserialize)]
pub struct SpotBalanceEntry {
    /// Currency code
    pub currency: String,
    /// Total balance
    #[serde(rename = "currencyId")]
    pub currency_id: i64,
    /// Available balance
    #[serde(rename = "availableAmount")]
    pub available_amount: String,
    /// Frozen balance
    #[serde(rename = "frozenAmount")]
    pub frozen_amount: String,
}

/// Futures balance entry.
#[derive(Debug, Deserialize)]
pub struct FuturesBalanceEntry {
    /// Currency/Coin
    pub coin: String,
    /// Wallet balance
    #[serde(rename = "walletBalance")]
    pub wallet_balance: String,
    /// Open order margin frozen
    #[serde(rename = "openOrderMarginFrozen")]
    pub open_order_margin_frozen: String,
    /// Position margin
    #[serde(rename = "isolatedMargin")]
    pub isolated_margin: String,
    /// Available balance
    #[serde(rename = "availableBalance")]
    pub available_balance: String,
}

/// Futures balance list response.
#[derive(Debug, Deserialize)]
pub struct FuturesBalanceList {
    #[serde(default)]
    pub items: Vec<FuturesBalanceEntry>,
}

// ═══════════════════════════════════════════════════════════════════════════
// Order Models
// ═══════════════════════════════════════════════════════════════════════════

/// Spot order response.
#[derive(Debug, Deserialize)]
pub struct SpotOrderResult {
    #[serde(rename = "orderId")]
    pub order_id: String,
}

/// Spot order info.
#[derive(Debug, Deserialize)]
pub struct SpotOrderInfo {
    #[serde(rename = "orderId")]
    pub order_id: String,
    #[serde(rename = "clientOrderId", default)]
    pub client_order_id: Option<String>,
    pub symbol: String,
    /// Order state: NEW, PARTIALLY_FILLED, FILLED, CANCELED, REJECTED, EXPIRED
    pub state: String,
    /// Side: BUY, SELL
    pub side: String,
    /// Type: LIMIT, MARKET
    #[serde(rename = "type")]
    pub order_type: String,
    /// Price
    #[serde(default)]
    pub price: Option<String>,
    /// Original quantity
    #[serde(rename = "origQty")]
    pub orig_qty: String,
    /// Executed quantity
    #[serde(rename = "executedQty")]
    pub executed_qty: String,
    /// Average fill price
    #[serde(rename = "avgPrice", default)]
    pub avg_price: Option<String>,
    /// Created time
    #[serde(rename = "time")]
    pub time: i64,
}

/// Futures order response.
#[derive(Debug, Deserialize)]
pub struct FuturesOrderResult {
    #[serde(rename = "orderId")]
    pub order_id: i64,
}

/// Futures order info.
#[derive(Debug, Deserialize)]
pub struct FuturesOrderInfo {
    #[serde(rename = "orderId")]
    pub order_id: i64,
    #[serde(rename = "clientOrderId", default)]
    pub client_order_id: Option<String>,
    pub symbol: String,
    /// Order state
    #[serde(rename = "orderStatus")]
    pub order_status: String,
    /// Position side: LONG, SHORT
    #[serde(rename = "positionSide")]
    pub position_side: String,
    /// Side: BUY, SELL
    #[serde(rename = "orderSide")]
    pub order_side: String,
    /// Type: LIMIT, MARKET
    #[serde(rename = "orderType")]
    pub order_type: String,
    /// Original quantity
    #[serde(rename = "origQty")]
    pub orig_qty: String,
    /// Executed quantity
    #[serde(rename = "executedQty")]
    pub executed_qty: String,
    /// Price
    #[serde(default)]
    pub price: Option<String>,
    /// Average fill price
    #[serde(rename = "avgPrice", default)]
    pub avg_price: Option<String>,
    /// Created time
    #[serde(rename = "createdTime")]
    pub created_time: i64,
}

// ═══════════════════════════════════════════════════════════════════════════
// Position Models
// ═══════════════════════════════════════════════════════════════════════════

/// Futures position info.
#[derive(Debug, Clone, Deserialize)]
pub struct FuturesPositionInfo {
    pub symbol: String,
    /// Position side: LONG, SHORT
    #[serde(rename = "positionSide")]
    pub position_side: String,
    /// Position type: CROSSED, ISOLATED
    #[serde(rename = "positionType")]
    pub position_type: String,
    /// Position size
    #[serde(rename = "positionSize")]
    pub position_size: String,
    /// Entry price
    #[serde(rename = "entryPrice")]
    pub entry_price: String,
    /// Isolated margin
    #[serde(rename = "isolatedMargin")]
    pub isolated_margin: String,
    /// Leverage
    pub leverage: i32,
    /// Unrealized PnL
    #[serde(rename = "unrealizedPnl", default)]
    pub unrealized_pnl: Option<String>,
    /// Mark price
    #[serde(rename = "markPrice", default)]
    pub mark_price: Option<String>,
}

/// Futures position list.
#[derive(Debug, Deserialize)]
pub struct FuturesPositionList {
    #[serde(default)]
    pub items: Vec<FuturesPositionInfo>,
}

// ═══════════════════════════════════════════════════════════════════════════
// WebSocket Models
// ═══════════════════════════════════════════════════════════════════════════

/// WebSocket subscription request (spot).
#[derive(Debug, Serialize)]
pub struct WsSpotSubscribeRequest {
    pub method: String,
    pub id: String,
    pub params: Vec<String>,
    #[serde(rename = "listenKey", skip_serializing_if = "Option::is_none")]
    pub listen_key: Option<String>,
}

/// WebSocket subscription request (futures).
#[derive(Debug, Serialize)]
pub struct WsFuturesSubscribeRequest {
    pub method: String,
    pub id: String,
    pub params: Vec<String>,
}

/// WebSocket message (generic).
#[derive(Debug, Deserialize)]
pub struct WsMessage {
    pub topic: Option<String>,
    pub event: Option<String>,
    pub data: Option<serde_json::Value>,
    /// Response ID (for subscription confirmations)
    pub id: Option<String>,
    /// Return code (for errors)
    pub code: Option<i32>,
    pub msg: Option<String>,
}

/// WebSocket token response (spot private).
#[derive(Debug, Deserialize)]
pub struct WsTokenResponse {
    #[serde(rename = "accessToken")]
    pub access_token: Option<String>,
    pub token: Option<String>,
    #[serde(rename = "refreshToken")]
    pub refresh_token: Option<String>,
}

/// WebSocket listen key response (futures private).
#[derive(Debug, Deserialize)]
pub struct WsListenKeyResponse {
    /// Listen key string
    pub result: String,
}

/// WebSocket order update (spot).
#[derive(Debug, Deserialize)]
pub struct WsSpotOrderUpdate {
    /// Event type: NEW, TRADE, CANCELED, etc.
    pub e: String,
    /// Symbol
    pub s: String,
    /// Order ID
    #[serde(rename = "i")]
    pub order_id: String,
    /// Client order ID
    #[serde(rename = "c", default)]
    pub client_order_id: Option<String>,
    /// Side: BUY, SELL
    #[serde(rename = "S")]
    pub side: String,
    /// Order type: LIMIT, MARKET
    #[serde(rename = "o")]
    pub order_type: String,
    /// Price
    #[serde(rename = "p")]
    pub price: String,
    /// Original quantity
    #[serde(rename = "q")]
    pub quantity: String,
    /// Executed quantity
    #[serde(rename = "z")]
    pub executed_qty: String,
    /// Order status
    #[serde(rename = "X")]
    pub status: String,
    /// Trade time
    #[serde(rename = "T")]
    pub trade_time: i64,
}

/// WebSocket order update (futures).
#[derive(Debug, Deserialize)]
pub struct WsFuturesOrderUpdate {
    /// Order ID
    #[serde(rename = "orderId")]
    pub order_id: i64,
    /// Client order ID
    #[serde(rename = "clientOrderId", default)]
    pub client_order_id: Option<String>,
    /// Symbol
    pub symbol: String,
    /// Order side: BUY, SELL
    #[serde(rename = "orderSide")]
    pub order_side: String,
    /// Order type: LIMIT, MARKET
    #[serde(rename = "orderType")]
    pub order_type: String,
    /// Price
    #[serde(default)]
    pub price: Option<String>,
    /// Original quantity
    #[serde(rename = "origQty")]
    pub orig_qty: String,
    /// Executed quantity
    #[serde(rename = "executedQty")]
    pub executed_qty: String,
    /// Average fill price
    #[serde(rename = "avgPrice", default)]
    pub avg_price: Option<String>,
    /// Order status
    #[serde(rename = "orderStatus")]
    pub order_status: String,
    /// Position side: LONG, SHORT
    #[serde(rename = "positionSide")]
    pub position_side: String,
    /// Commission
    #[serde(default)]
    pub fee: Option<String>,
    /// Fee currency
    #[serde(rename = "feeCurrency", default)]
    pub fee_currency: Option<String>,
    /// Created time
    #[serde(rename = "createdTime")]
    pub created_time: i64,
}

/// WebSocket trade data.
#[derive(Debug, Deserialize)]
pub struct WsTradeData {
    /// Symbol
    pub s: String,
    /// Trade ID
    pub i: String,
    /// Timestamp
    pub t: i64,
    /// Price
    pub p: String,
    /// Quantity
    pub q: String,
    /// Buy/Sell side indicator (true = buy is taker)
    pub b: bool,
}
