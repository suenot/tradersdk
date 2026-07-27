//! Order types, statuses, and results.

use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Order side (buy/sell).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "json-schema", derive(schemars::JsonSchema))]
#[serde(rename_all = "lowercase")]
pub enum OrderSide {
    Buy,
    Sell,
}

impl std::fmt::Display for OrderSide {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            OrderSide::Buy => write!(f, "BUY"),
            OrderSide::Sell => write!(f, "SELL"),
        }
    }
}

impl From<&str> for OrderSide {
    fn from(s: &str) -> Self {
        match s.to_lowercase().as_str() {
            "sell" | "short" => Self::Sell,
            _ => Self::Buy,
        }
    }
}

/// Order type.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "json-schema", derive(schemars::JsonSchema))]
#[serde(rename_all = "snake_case")]
pub enum OrderType {
    Market,
    Limit,
    StopMarket,
    StopLimit,
    TakeProfitMarket,
    TakeProfitLimit,
    TrailingStop,
}

impl std::fmt::Display for OrderType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            OrderType::Market => write!(f, "MARKET"),
            OrderType::Limit => write!(f, "LIMIT"),
            OrderType::StopMarket => write!(f, "STOP_MARKET"),
            OrderType::StopLimit => write!(f, "STOP_LIMIT"),
            OrderType::TakeProfitMarket => write!(f, "TAKE_PROFIT_MARKET"),
            OrderType::TakeProfitLimit => write!(f, "TAKE_PROFIT_LIMIT"),
            OrderType::TrailingStop => write!(f, "TRAILING_STOP"),
        }
    }
}

impl Default for OrderType {
    fn default() -> Self {
        Self::Market
    }
}

/// Order status.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "json-schema", derive(schemars::JsonSchema))]
#[serde(rename_all = "snake_case")]
pub enum OrderStatus {
    /// Order is pending (submitted but not yet accepted by exchange).
    Pending,
    /// Order is created but not yet filled.
    New,
    /// Order is partially filled.
    PartiallyFilled,
    /// Order is fully filled.
    Filled,
    /// Order was cancelled.
    Cancelled,
    /// Order was rejected.
    Rejected,
    /// Order expired.
    Expired,
}

impl OrderStatus {
    /// Check if order is in terminal state.
    pub fn is_final(&self) -> bool {
        matches!(
            self,
            OrderStatus::Filled | OrderStatus::Cancelled | OrderStatus::Rejected | OrderStatus::Expired
        )
    }

    /// Check if order is active.
    pub fn is_active(&self) -> bool {
        matches!(self, OrderStatus::Pending | OrderStatus::New | OrderStatus::PartiallyFilled)
    }
}

/// Time in force for orders.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[cfg_attr(feature = "json-schema", derive(schemars::JsonSchema))]
#[serde(rename_all = "snake_case")]
pub enum TimeInForce {
    /// Good Till Cancelled.
    #[default]
    GTC,
    /// Immediate Or Cancel.
    IOC,
    /// Fill Or Kill.
    FOK,
    /// Post Only (maker only).
    PostOnly,
    /// Good Till Crossing (same as PostOnly on some exchanges).
    GTX,
}

impl std::fmt::Display for TimeInForce {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TimeInForce::GTC => write!(f, "GTC"),
            TimeInForce::IOC => write!(f, "IOC"),
            TimeInForce::FOK => write!(f, "FOK"),
            TimeInForce::PostOnly => write!(f, "PostOnly"),
            TimeInForce::GTX => write!(f, "GTX"),
        }
    }
}

/// Result of an order execution.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(feature = "json-schema", derive(schemars::JsonSchema))]
pub struct OrderResult {
    /// Whether the order was successful.
    pub success: bool,
    /// Order ID assigned by exchange.
    pub order_id: Option<String>,
    /// Trading symbol.
    pub symbol: String,
    /// Order side.
    pub side: OrderSide,
    /// Order type.
    pub order_type: OrderType,
    /// Executed quantity.
    pub quantity: Option<f64>,
    /// Entry/average price.
    pub entry_price: Option<f64>,
    /// Position size after order.
    pub position_size: Option<f64>,
    /// Position value in USDT.
    pub position_usdt: Option<f64>,
    /// Error message if failed.
    pub error: Option<String>,
    /// Raw exchange response.
    pub raw_response: Option<HashMap<String, serde_json::Value>>,
}

impl OrderResult {
    /// Create a successful order result.
    pub fn success(
        order_id: String,
        symbol: String,
        side: OrderSide,
        quantity: f64,
        entry_price: f64,
    ) -> Self {
        let position_usdt = quantity * entry_price;
        Self {
            success: true,
            order_id: Some(order_id),
            symbol,
            side,
            order_type: OrderType::Market,
            quantity: Some(quantity),
            entry_price: Some(entry_price),
            position_size: Some(quantity),
            position_usdt: Some(position_usdt),
            error: None,
            raw_response: None,
        }
    }

    /// Create a failed order result.
    pub fn failure(symbol: String, side: OrderSide, error: String) -> Self {
        Self {
            success: false,
            order_id: None,
            symbol,
            side,
            order_type: OrderType::Market,
            quantity: None,
            entry_price: None,
            position_size: None,
            position_usdt: None,
            error: Some(error),
            raw_response: None,
        }
    }

    /// Helper to get order_id as a string reference.
    pub fn order_id(&self) -> &str {
        self.order_id.as_deref().unwrap_or("")
    }

    /// Helper to get executed quantity.
    pub fn executed_qty(&self) -> f64 {
        self.quantity.unwrap_or(0.0)
    }
}

/// Detailed order information.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(feature = "json-schema", derive(schemars::JsonSchema))]
pub struct OrderInfo {
    /// Order ID.
    pub order_id: String,
    /// Client order ID.
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
    /// Remaining quantity.
    pub remaining_quantity: f64,
    /// Average fill price.
    pub avg_price: Option<f64>,
    /// Order creation timestamp.
    pub timestamp: i64,
    /// Time in force.
    pub time_in_force: TimeInForce,
    /// Whether reduce-only.
    pub reduce_only: bool,
}

impl OrderInfo {
    /// Get fill percentage.
    pub fn fill_percentage(&self) -> f64 {
        if self.quantity == 0.0 {
            return 0.0;
        }
        (self.filled_quantity / self.quantity) * 100.0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_order_result_success() {
        let result = OrderResult::success(
            "123".to_string(),
            "BTCUSDT".to_string(),
            OrderSide::Buy,
            0.1,
            50000.0,
        );

        assert!(result.success);
        assert_eq!(result.order_id(), "123");
        assert_eq!(result.executed_qty(), 0.1);
        assert_eq!(result.position_usdt, Some(5000.0));
    }

    #[test]
    fn test_order_status_is_final() {
        assert!(OrderStatus::Filled.is_final());
        assert!(OrderStatus::Cancelled.is_final());
        assert!(!OrderStatus::New.is_final());
        assert!(!OrderStatus::PartiallyFilled.is_final());
    }
}
