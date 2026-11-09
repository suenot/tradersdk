//! WebSocket and real-time event models.

use crate::models::{OrderSide, OrderStatus, OrderType};
use serde::{Deserialize, Serialize};

/// Real-time order update event from exchange private WebSocket.
///
/// Contains both the exchange timestamp (when the exchange processed the order)
/// and the local received timestamp for latency measurement.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(feature = "json-schema", derive(schemars::JsonSchema))]
pub struct OrderUpdateEvent {
    /// Exchange name.
    pub exchange: String,
    /// Order ID.
    pub order_id: String,
    /// Client order ID (optional).
    pub client_order_id: Option<String>,
    /// Trading symbol.
    pub symbol: String,
    /// Order status.
    pub status: OrderStatus,
    /// Order side.
    pub side: OrderSide,
    /// Order type.
    pub order_type: OrderType,
    /// Order price.
    pub price: f64,
    /// Order quantity.
    pub quantity: f64,
    /// Filled quantity.
    pub filled_quantity: f64,
    /// Average fill price.
    pub avg_fill_price: Option<f64>,
    /// Fee amount.
    pub fee: Option<f64>,
    /// Fee currency.
    pub fee_currency: Option<String>,
    /// Exchange's order creation/transaction timestamp (ms).
    pub exchange_timestamp: i64,
    /// Local timestamp when we received the update (ms).
    pub received_at: i64,
    /// Raw exchange response data.
    pub raw: Option<serde_json::Value>,
}

impl OrderUpdateEvent {
    /// Calculate latency between exchange timestamp and local receive time.
    pub fn latency_ms(&self) -> i64 {
        self.received_at - self.exchange_timestamp
    }

    /// Check if order is fully filled.
    pub fn is_filled(&self) -> bool {
        self.status == OrderStatus::Filled
    }

    /// Get remaining quantity.
    pub fn remaining_quantity(&self) -> f64 {
        self.quantity - self.filled_quantity
    }

    /// Get fill percentage.
    pub fn fill_percentage(&self) -> f64 {
        if self.quantity == 0.0 {
            return 0.0;
        }
        (self.filled_quantity / self.quantity) * 100.0
    }
}

impl Default for OrderUpdateEvent {
    fn default() -> Self {
        Self {
            exchange: String::new(),
            order_id: String::new(),
            client_order_id: None,
            symbol: String::new(),
            status: OrderStatus::New,
            side: OrderSide::Buy,
            order_type: OrderType::Market,
            price: 0.0,
            quantity: 0.0,
            filled_quantity: 0.0,
            avg_fill_price: None,
            fee: None,
            fee_currency: None,
            exchange_timestamp: 0,
            received_at: 0,
            raw: None,
        }
    }
}

/// Trade event from WebSocket.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(feature = "json-schema", derive(schemars::JsonSchema))]
pub struct TradeEvent {
    /// Trading symbol.
    pub symbol: String,
    /// Trade ID.
    pub trade_id: Option<String>,
    /// Trade price.
    pub price: f64,
    /// Trade quantity.
    pub quantity: f64,
    /// Trade side (buy/sell).
    pub side: String,
    /// Trade timestamp (ms).
    pub timestamp: i64,
}

impl TradeEvent {
    /// Check if trade is a buy.
    pub fn is_buy(&self) -> bool {
        self.side.to_lowercase() == "buy"
    }

    /// Get trade value.
    pub fn value(&self) -> f64 {
        self.price * self.quantity
    }
}

/// WebSocket connection test result.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(feature = "json-schema", derive(schemars::JsonSchema))]
pub struct WsTestResult {
    /// Whether connection was successful.
    pub connected: bool,
    /// Connection latency in ms.
    pub latency_ms: u64,
    /// Number of messages received during test.
    pub messages_received: u32,
    /// Type of subscription tested.
    pub subscription_type: String,
    /// Sample of received data.
    pub data_sample: Option<serde_json::Value>,
}

/// Order test result (dry run without real execution).
#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(feature = "json-schema", derive(schemars::JsonSchema))]
pub struct OrderTestResult {
    /// Whether order parameters are valid.
    pub valid: bool,
    /// Order type.
    pub order_type: String,
    /// Order side.
    pub side: String,
    /// Symbol.
    pub symbol: String,
    /// Quantity.
    pub quantity: f64,
    /// Price (for limit orders).
    pub price: Option<f64>,
    /// Estimated cost.
    pub estimated_cost: f64,
    /// Error message if invalid.
    pub error: Option<String>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_order_update_latency() {
        let event = OrderUpdateEvent {
            exchange_timestamp: 1000,
            received_at: 1050,
            ..Default::default()
        };

        assert_eq!(event.latency_ms(), 50);
    }

    #[test]
    fn test_order_update_fill_percentage() {
        let event = OrderUpdateEvent {
            quantity: 10.0,
            filled_quantity: 7.5,
            ..Default::default()
        };

        assert_eq!(event.fill_percentage(), 75.0);
        assert_eq!(event.remaining_quantity(), 2.5);
    }
}
