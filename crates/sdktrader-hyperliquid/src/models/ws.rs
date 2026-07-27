//! Hyperliquid WebSocket models.
//!
//! Contains models for WebSocket communication and EIP-712 signing.

use serde::{Deserialize, Serialize};

/// EIP-712 signature components.
#[derive(Debug, Clone, Serialize)]
pub struct Eip712Signature {
    pub r: String,
    pub s: String,
    pub v: u8,
}

impl Eip712Signature {
    /// Format as the signature object expected by Hyperliquid API.
    pub fn to_api_format(&self) -> serde_json::Value {
        serde_json::json!({
            "r": self.r,
            "s": self.s,
            "v": self.v
        })
    }
}

/// Order action for placing orders.
#[derive(Debug, Clone, Serialize)]
pub struct OrderAction {
    #[serde(rename = "type")]
    pub action_type: String,
    pub orders: Vec<OrderWire>,
    pub grouping: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub builder: Option<BuilderInfo>,
}

/// Order wire format for API.
#[derive(Debug, Clone, Serialize)]
pub struct OrderWire {
    /// Asset index.
    pub a: i32,
    /// Is buy.
    pub b: bool,
    /// Price.
    pub p: String,
    /// Size.
    pub s: String,
    /// Reduce only.
    pub r: bool,
    /// Order type.
    pub t: OrderTypeWire,
    /// Client order ID (optional).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub c: Option<String>,
}

/// Order type wire format.
#[derive(Debug, Clone, Serialize)]
pub struct OrderTypeWire {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub limit: Option<LimitOrderType>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub trigger: Option<TriggerOrderType>,
}

/// Limit order type.
#[derive(Debug, Clone, Serialize)]
pub struct LimitOrderType {
    pub tif: String, // "Gtc", "Ioc", "Alo"
}

/// Trigger order type.
#[derive(Debug, Clone, Serialize)]
pub struct TriggerOrderType {
    #[serde(rename = "isMarket")]
    pub is_market: bool,
    #[serde(rename = "triggerPx")]
    pub trigger_px: String,
    pub tpsl: String, // "tp" or "sl"
}

/// Builder fee info.
#[derive(Debug, Clone, Serialize)]
pub struct BuilderInfo {
    pub b: String,
    pub f: i32,
}

/// Cancel action.
#[derive(Debug, Clone, Serialize)]
pub struct CancelAction {
    #[serde(rename = "type")]
    pub action_type: String,
    pub cancels: Vec<CancelWire>,
}

/// Cancel wire format.
#[derive(Debug, Clone, Serialize)]
pub struct CancelWire {
    /// Asset index.
    pub a: i32,
    /// Order ID.
    pub o: i64,
}

/// Cancel by client order ID action.
#[derive(Debug, Clone, Serialize)]
pub struct CancelByCloidAction {
    #[serde(rename = "type")]
    pub action_type: String,
    pub cancels: Vec<CancelByCloidWire>,
}

/// Cancel by client order ID wire format.
#[derive(Debug, Clone, Serialize)]
pub struct CancelByCloidWire {
    pub asset: i32,
    pub cloid: String,
}

/// Set leverage action.
#[derive(Debug, Clone, Serialize)]
pub struct SetLeverageAction {
    #[serde(rename = "type")]
    pub action_type: String,
    pub asset: i32,
    #[serde(rename = "isCross")]
    pub is_cross: bool,
    pub leverage: i32,
}

/// WebSocket subscription message.
#[derive(Debug, Clone, Serialize)]
pub struct WsSubscription {
    pub method: String,
    pub subscription: WsSubscriptionData,
}

/// WebSocket subscription data.
#[derive(Debug, Clone, Serialize)]
pub struct WsSubscriptionData {
    #[serde(rename = "type")]
    pub sub_type: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub coin: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub user: Option<String>,
}

/// WebSocket message.
#[derive(Debug, Clone, Deserialize)]
pub struct WsMessage {
    pub channel: Option<String>,
    pub data: Option<serde_json::Value>,
}
