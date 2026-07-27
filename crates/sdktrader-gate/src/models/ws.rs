//! Gate.io WebSocket models.

use serde::{Deserialize, Serialize};

/// WebSocket authentication object.
#[derive(Debug, Serialize)]
pub struct WsAuth {
    /// Authentication method (always "api_key").
    pub method: String,
    /// API key.
    #[serde(rename = "KEY")]
    pub key: String,
    /// HMAC-SHA512 signature.
    #[serde(rename = "SIGN")]
    pub sign: String,
}

/// WebSocket request structure for Gate.io.
#[derive(Debug, Serialize)]
pub struct WsRequest {
    /// Unix timestamp in seconds.
    pub time: i64,
    /// Optional request ID for tracking responses.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub id: Option<String>,
    /// Channel name (e.g., "spot.order_place", "futures.orders").
    pub channel: String,
    /// Event type (e.g., "subscribe", "api").
    pub event: String,
    /// Request payload (varies by channel).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub payload: Option<serde_json::Value>,
    /// Authentication object for private channels.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub auth: Option<WsAuth>,
}

/// WebSocket response structure from Gate.io.
#[derive(Debug, Deserialize)]
pub struct WsResponse {
    /// Unix timestamp in seconds.
    #[serde(rename = "time")]
    pub _time: Option<i64>,
    /// Unix timestamp in milliseconds.
    #[serde(rename = "time_ms")]
    pub _time_ms: Option<i64>,
    /// Request ID (echoed from request).
    #[serde(rename = "id")]
    pub _id: Option<String>,
    /// Channel name.
    #[serde(rename = "channel")]
    pub _channel: Option<String>,
    /// Event type.
    #[serde(rename = "event")]
    pub _event: Option<String>,
    /// Error object (if request failed).
    pub error: Option<WsError>,
    /// Result data (if request succeeded).
    pub result: Option<serde_json::Value>,
}

/// WebSocket error response.
#[derive(Debug, Deserialize)]
pub struct WsError {
    /// Error code.
    pub code: Option<i32>,
    /// Error message.
    pub message: Option<String>,
}

/// Order place/amend request parameters for Spot.
#[derive(Debug, Serialize)]
pub struct SpotOrderParams {
    /// Client order ID for tracking.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub text: Option<String>,
    /// Trading pair (e.g., "BTC_USDT").
    pub currency_pair: String,
    /// Order type ("limit", "market").
    #[serde(rename = "type")]
    pub order_type: String,
    /// Account type (e.g., "spot", "margin").
    #[serde(skip_serializing_if = "Option::is_none")]
    pub account: Option<String>,
    /// Order side ("buy", "sell").
    pub side: String,
    /// Order amount.
    pub amount: String,
    /// Order price (required for limit orders).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub price: Option<String>,
    /// Time in force ("gtc", "ioc", "poc", "fok").
    #[serde(skip_serializing_if = "Option::is_none")]
    pub time_in_force: Option<String>,
    /// Iceberg order amount.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub iceberg: Option<String>,
    /// Auto borrow for margin trading.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub auto_borrow: Option<bool>,
    /// Auto repay for margin trading.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub auto_repay: Option<bool>,
}

/// Order place/amend request parameters for Futures.
#[derive(Debug, Serialize)]
pub struct FuturesOrderParams {
    /// Contract name (e.g., "BTC_USDT").
    pub contract: String,
    /// Order size (positive for long, negative for short).
    pub size: i64,
    /// Order price ("0" for market orders).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub price: Option<String>,
    /// Time in force ("gtc", "ioc", "poc", "fok").
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tif: Option<String>,
    /// Client order ID for tracking.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub text: Option<String>,
    /// Reduce-only flag.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reduce_only: Option<bool>,
    /// Close position flag.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub close: Option<bool>,
    /// Iceberg order size.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub iceberg: Option<i64>,
    /// Auto size mode ("close_long", "close_short").
    #[serde(skip_serializing_if = "Option::is_none")]
    pub auto_size: Option<String>,
}

/// Order cancel request parameters.
#[derive(Debug, Serialize)]
pub struct OrderCancelParams {
    /// Order ID to cancel.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub order_id: Option<String>,
    /// Trading pair (for spot orders).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub currency_pair: Option<String>,
    /// Contract name (for futures orders).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub contract: Option<String>,
}
