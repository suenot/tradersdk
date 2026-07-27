//! Bybit WebSocket API models.
//!
//! This module contains data structures for Bybit WebSocket communication,
//! including authentication, order management, and message parsing.

use serde::{Deserialize, Serialize};

/// WebSocket authentication request.
///
/// Sent to authenticate a private WebSocket connection using API credentials.
/// The signature is generated using HMAC-SHA256 of the message "GET/realtime{expires}".
///
/// # Example
/// ```json
/// {
///     "reqId": "auth_1234567890",
///     "op": "auth",
///     "args": ["api_key", "expires_timestamp", "signature"]
/// }
/// ```
#[derive(Debug, Serialize)]
pub struct WsAuthRequest {
    /// Unique request identifier for tracking the response.
    #[serde(rename = "reqId")]
    pub req_id: String,

    /// Operation type, always "auth" for authentication.
    pub op: String,

    /// Authentication arguments: [api_key, expires_timestamp, signature].
    pub args: Vec<String>,
}

/// WebSocket authentication response.
///
/// Received after sending an authentication request.
/// Check `success` field to verify authentication status.
#[derive(Debug, Deserialize)]
pub struct WsAuthResponse {
    /// Whether authentication was successful.
    pub success: bool,

    /// Error message if authentication failed.
    #[serde(rename = "retMsg")]
    pub ret_msg: Option<String>,

    /// Connection ID assigned by the server (useful for debugging).
    #[serde(rename = "connId")]
    pub conn_id: Option<String>,

    /// Operation type that was processed.
    pub op: String,
}

/// WebSocket order request for trading operations.
///
/// Used for creating, canceling, and amending orders via WebSocket.
/// Includes authentication headers for request signing.
///
/// # Supported Operations
/// - `order.create`: Create a new order
/// - `order.cancel`: Cancel an existing order
/// - `order.amend`: Modify an existing order
#[derive(Debug, Serialize)]
pub struct WsOrderRequest {
    /// Unique request identifier for tracking the response.
    #[serde(rename = "reqId")]
    pub req_id: String,

    /// Authentication and timing headers.
    pub header: WsOrderHeader,

    /// Operation type: "order.create", "order.cancel", or "order.amend".
    pub op: String,

    /// Order parameters as JSON values.
    pub args: Vec<serde_json::Value>,
}

/// WebSocket order request header.
///
/// Contains authentication and timing information required for
/// signed WebSocket trading requests.
#[derive(Debug, Serialize)]
pub struct WsOrderHeader {
    /// Request timestamp in milliseconds.
    #[serde(rename = "X-BAPI-TIMESTAMP")]
    pub timestamp: String,

    /// Receive window in milliseconds (typically "5000").
    /// Requests are rejected if server time differs by more than this amount.
    #[serde(rename = "X-BAPI-RECV-WINDOW")]
    pub recv_window: String,
}

/// WebSocket order operation response.
///
/// Received after submitting an order operation (create/cancel/amend).
/// Contains the result status and order details on success.
#[derive(Debug, Deserialize)]
pub struct WsOrderResponse {
    /// Whether the operation was successful.
    pub success: bool,

    /// Error or success message.
    #[serde(rename = "retMsg")]
    pub ret_msg: Option<String>,

    /// Request ID echoed back for correlation.
    #[serde(rename = "reqId")]
    pub req_id: Option<String>,

    /// Return code (0 for success).
    #[serde(rename = "retCode")]
    pub ret_code: Option<i32>,

    /// Operation type that was processed.
    pub op: Option<String>,

    /// Order data returned on successful operations.
    pub data: Option<WsOrderData>,
}

/// WebSocket order data in operation response.
///
/// Contains identifiers for the order that was created, modified, or canceled.
#[derive(Debug, Deserialize)]
pub struct WsOrderData {
    /// System-generated order ID.
    #[serde(rename = "orderId")]
    pub order_id: Option<String>,

    /// Client-specified order link ID (if provided in the request).
    #[serde(rename = "orderLinkId")]
    pub order_link_id: Option<String>,
}

/// Generic WebSocket message for subscription data.
///
/// Used to parse incoming WebSocket messages for orderbook, trades,
/// and ticker subscriptions.
///
/// # Message Types
/// - `snapshot`: Initial full data snapshot
/// - `delta`: Incremental update to previous data
#[derive(Debug, Deserialize)]
pub struct WsMessage {
    /// Subscription topic (e.g., "orderbook.50.BTCUSDT", "publicTrade.BTCUSDT").
    pub topic: Option<String>,

    /// Message payload containing the subscription data.
    pub data: Option<serde_json::Value>,

    /// Message type: "snapshot" or "delta".
    #[serde(rename = "type")]
    pub msg_type: Option<String>,
}
