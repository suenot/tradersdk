//! WebSocket API models for Binance.

use serde::{Deserialize, Serialize};

// ═══════════════════════════════════════════════════════════════════════════
// WEBSOCKET REQUEST/RESPONSE
// ═══════════════════════════════════════════════════════════════════════════

/// WebSocket request for Binance API.
#[derive(Debug, Serialize)]
pub struct WsRequest {
    pub id: String,
    pub method: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub params: Option<serde_json::Value>,
}

/// WebSocket response from Binance API.
#[derive(Debug, Deserialize)]
pub struct WsResponse {
    pub id: String,
    pub status: i32,
    #[serde(default)]
    pub result: Option<serde_json::Value>,
    #[serde(default)]
    pub error: Option<WsError>,
    #[serde(rename = "rateLimits")]
    #[serde(default)]
    pub rate_limits: Vec<serde_json::Value>,
}

/// WebSocket error response.
#[derive(Debug, Deserialize)]
pub struct WsError {
    pub code: i32,
    pub msg: String,
}

/// WebSocket order response data.
#[derive(Debug, Deserialize)]
pub struct WsOrderData {
    #[serde(rename = "orderId")]
    pub order_id: Option<i64>,
    pub symbol: Option<String>,
    #[serde(rename = "clientOrderId")]
    pub client_order_id: Option<String>,
    pub status: Option<String>,
    #[serde(rename = "executedQty")]
    pub executed_qty: Option<String>,
    #[serde(rename = "avgPrice")]
    pub avg_price: Option<String>,
}

// ═══════════════════════════════════════════════════════════════════════════
// USER DATA STREAM EVENTS
// ═══════════════════════════════════════════════════════════════════════════

/// Spot execution report event.
#[derive(Debug, Deserialize)]
pub struct SpotExecutionReport {
    /// Event type ("executionReport").
    #[serde(rename = "e")]
    pub event_type: String,
    /// Event time.
    #[serde(rename = "E")]
    pub event_time: i64,
    /// Symbol.
    #[serde(rename = "s")]
    pub symbol: String,
    /// Client order ID.
    #[serde(rename = "c")]
    pub client_order_id: String,
    /// Side (BUY/SELL).
    #[serde(rename = "S")]
    pub side: String,
    /// Order type (MARKET/LIMIT).
    #[serde(rename = "o")]
    pub order_type: String,
    /// Time in force.
    #[serde(rename = "f")]
    pub time_in_force: String,
    /// Order quantity.
    #[serde(rename = "q")]
    pub quantity: String,
    /// Order price.
    #[serde(rename = "p")]
    pub price: String,
    /// Order ID.
    #[serde(rename = "i")]
    pub order_id: i64,
    /// Order status.
    #[serde(rename = "X")]
    pub status: String,
    /// Last executed quantity.
    #[serde(rename = "l")]
    pub last_qty: String,
    /// Cumulative filled quantity.
    #[serde(rename = "z")]
    pub filled_qty: String,
    /// Last executed price.
    #[serde(rename = "L")]
    pub last_price: String,
    /// Commission amount.
    #[serde(rename = "n")]
    pub commission: Option<String>,
    /// Commission asset.
    #[serde(rename = "N")]
    pub commission_asset: Option<String>,
    /// Transaction time.
    #[serde(rename = "T")]
    pub transaction_time: i64,
}

/// Futures order update event.
#[derive(Debug, Deserialize)]
pub struct FuturesOrderUpdate {
    /// Event type ("ORDER_TRADE_UPDATE").
    #[serde(rename = "e")]
    pub event_type: String,
    /// Event time.
    #[serde(rename = "E")]
    pub event_time: i64,
    /// Transaction time.
    #[serde(rename = "T")]
    pub transaction_time: i64,
    /// Order details.
    #[serde(rename = "o")]
    pub order: FuturesOrderDetail,
}

/// Futures order detail within ORDER_TRADE_UPDATE.
#[derive(Debug, Deserialize)]
pub struct FuturesOrderDetail {
    /// Symbol.
    #[serde(rename = "s")]
    pub symbol: String,
    /// Client order ID.
    #[serde(rename = "c")]
    pub client_order_id: String,
    /// Side (BUY/SELL).
    #[serde(rename = "S")]
    pub side: String,
    /// Order type.
    #[serde(rename = "o")]
    pub order_type: String,
    /// Time in force.
    #[serde(rename = "f")]
    pub time_in_force: String,
    /// Original quantity.
    #[serde(rename = "q")]
    pub quantity: String,
    /// Original price.
    #[serde(rename = "p")]
    pub price: String,
    /// Average price.
    #[serde(rename = "ap")]
    pub avg_price: String,
    /// Order ID.
    #[serde(rename = "i")]
    pub order_id: i64,
    /// Order status.
    #[serde(rename = "X")]
    pub status: String,
    /// Filled quantity.
    #[serde(rename = "z")]
    pub filled_qty: String,
    /// Commission.
    #[serde(rename = "n")]
    pub commission: Option<String>,
    /// Commission asset.
    #[serde(rename = "N")]
    pub commission_asset: Option<String>,
    /// Reduce only.
    #[serde(rename = "R")]
    pub reduce_only: bool,
    /// Position side.
    #[serde(rename = "ps")]
    pub position_side: String,
}

// ═══════════════════════════════════════════════════════════════════════════
// MARKET DATA STREAM MODELS
// ═══════════════════════════════════════════════════════════════════════════

/// Depth update event.
#[derive(Debug, Deserialize)]
pub struct DepthUpdate {
    /// Event type.
    #[serde(rename = "e")]
    pub event_type: Option<String>,
    /// Event time.
    #[serde(rename = "E")]
    pub event_time: Option<i64>,
    /// Symbol.
    #[serde(rename = "s")]
    pub symbol: Option<String>,
    /// Bids.
    #[serde(rename = "b")]
    pub bids: Option<Vec<Vec<String>>>,
    /// Asks.
    #[serde(rename = "a")]
    pub asks: Option<Vec<Vec<String>>>,
}

/// Aggregate trade event.
#[derive(Debug, Deserialize)]
pub struct AggTradeEvent {
    /// Event type.
    #[serde(rename = "e")]
    pub event_type: String,
    /// Event time.
    #[serde(rename = "E")]
    pub event_time: i64,
    /// Symbol.
    #[serde(rename = "s")]
    pub symbol: String,
    /// Aggregate trade ID.
    #[serde(rename = "a")]
    pub agg_trade_id: i64,
    /// Price.
    #[serde(rename = "p")]
    pub price: String,
    /// Quantity.
    #[serde(rename = "q")]
    pub quantity: String,
    /// First trade ID.
    #[serde(rename = "f")]
    pub first_trade_id: i64,
    /// Last trade ID.
    #[serde(rename = "l")]
    pub last_trade_id: i64,
    /// Trade time.
    #[serde(rename = "T")]
    pub trade_time: i64,
    /// Is buyer maker.
    #[serde(rename = "m")]
    pub is_buyer_maker: bool,
}

/// Mark price update event (futures).
#[derive(Debug, Deserialize)]
pub struct MarkPriceUpdate {
    /// Event type.
    #[serde(rename = "e")]
    pub event_type: String,
    /// Event time.
    #[serde(rename = "E")]
    pub event_time: i64,
    /// Symbol.
    #[serde(rename = "s")]
    pub symbol: String,
    /// Mark price.
    #[serde(rename = "p")]
    pub mark_price: String,
    /// Index price.
    #[serde(rename = "i")]
    pub index_price: Option<String>,
    /// Funding rate.
    #[serde(rename = "r")]
    pub funding_rate: Option<String>,
    /// Next funding time.
    #[serde(rename = "T")]
    pub next_funding_time: Option<i64>,
}

/// Ticker update event (spot).
#[derive(Debug, Deserialize)]
pub struct TickerUpdate {
    /// Event type.
    #[serde(rename = "e")]
    pub event_type: String,
    /// Event time.
    #[serde(rename = "E")]
    pub event_time: i64,
    /// Symbol.
    #[serde(rename = "s")]
    pub symbol: String,
    /// Last price.
    #[serde(rename = "c")]
    pub last_price: String,
    /// Price change.
    #[serde(rename = "p")]
    pub price_change: String,
    /// Price change percent.
    #[serde(rename = "P")]
    pub price_change_percent: String,
    /// High price.
    #[serde(rename = "h")]
    pub high_price: String,
    /// Low price.
    #[serde(rename = "l")]
    pub low_price: String,
    /// Volume.
    #[serde(rename = "v")]
    pub volume: String,
    /// Quote volume.
    #[serde(rename = "q")]
    pub quote_volume: String,
}
