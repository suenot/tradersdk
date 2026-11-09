//! Ourbit helper functions.
//!
//! Provides utility functions for symbol formatting, order status parsing,
//! and side/type conversions for the Ourbit futures exchange.

use tracing::warn;
use tradersdk_core::models::{OrderSide, OrderStatus, OrderType};

use crate::config::{
    INITIAL_RECONNECT_DELAY_SECS, MAX_RECONNECT_DELAY_SECS, ORDER_SIDE_CLOSE_LONG,
    ORDER_SIDE_CLOSE_SHORT, ORDER_SIDE_OPEN_LONG, ORDER_SIDE_OPEN_SHORT, ORDER_TYPE_FOK,
    ORDER_TYPE_IOC, ORDER_TYPE_LIMIT, ORDER_TYPE_MARKET, ORDER_TYPE_POST_ONLY,
};

/// Calculate next reconnect delay with exponential backoff (capped).
pub fn next_reconnect_delay(current_secs: u64) -> u64 {
    (current_secs * 2).min(MAX_RECONNECT_DELAY_SECS)
}

/// Get initial reconnect delay.
pub fn initial_reconnect_delay() -> u64 {
    INITIAL_RECONNECT_DELAY_SECS
}

/// Format symbol for Ourbit API.
///
/// Ourbit uses BTC_USDT format (underscore separator).
pub fn format_symbol(symbol: &str) -> String {
    let s = symbol.to_uppercase();

    if s.contains('/') {
        return s.replace('/', "_");
    }
    if s.contains('-') {
        return s.replace('-', "_");
    }
    if s.contains('_') {
        return s;
    }
    if s.ends_with("USDT") {
        let base = s.trim_end_matches("USDT");
        return format!("{}_USDT", base);
    }
    if s.ends_with("USDC") {
        let base = s.trim_end_matches("USDC");
        return format!("{}_USDC", base);
    }
    format!("{}_USDT", s)
}

/// Parse Ourbit numeric order state to OrderStatus.
pub fn parse_order_status(state: i32) -> OrderStatus {
    match state {
        1 => OrderStatus::New,
        2 => OrderStatus::PartiallyFilled,
        3 => OrderStatus::Filled,
        4 => OrderStatus::Cancelled,
        5 => OrderStatus::Rejected,
        _ => {
            warn!("Unknown Ourbit order state: {}", state);
            OrderStatus::New
        }
    }
}

/// Convert OrderSide and reduce_only flag to Ourbit side integer.
pub fn convert_side_to_ourbit(side: OrderSide, reduce_only: bool) -> i32 {
    match (side, reduce_only) {
        (OrderSide::Buy, false) => ORDER_SIDE_OPEN_LONG,
        (OrderSide::Buy, true) => ORDER_SIDE_CLOSE_SHORT,
        (OrderSide::Sell, false) => ORDER_SIDE_OPEN_SHORT,
        (OrderSide::Sell, true) => ORDER_SIDE_CLOSE_LONG,
    }
}

/// Convert Ourbit side integer to (OrderSide, reduce_only).
pub fn convert_side_from_ourbit(side: i32) -> (OrderSide, bool) {
    match side {
        ORDER_SIDE_OPEN_LONG => (OrderSide::Buy, false),
        ORDER_SIDE_CLOSE_SHORT => (OrderSide::Buy, true),
        ORDER_SIDE_OPEN_SHORT => (OrderSide::Sell, false),
        ORDER_SIDE_CLOSE_LONG => (OrderSide::Sell, true),
        _ => {
            warn!("Unknown Ourbit order side: {}, defaulting to Buy", side);
            (OrderSide::Buy, false)
        }
    }
}

/// Convert OrderType to Ourbit order type integer.
pub fn convert_order_type_to_ourbit(order_type: OrderType) -> i32 {
    match order_type {
        OrderType::Limit => ORDER_TYPE_LIMIT,
        OrderType::Market => ORDER_TYPE_MARKET,
        OrderType::StopMarket => ORDER_TYPE_MARKET,
        OrderType::StopLimit => ORDER_TYPE_LIMIT,
        OrderType::TakeProfitMarket => ORDER_TYPE_MARKET,
        OrderType::TakeProfitLimit => ORDER_TYPE_LIMIT,
        OrderType::TrailingStop => ORDER_TYPE_MARKET,
    }
}

/// Convert Ourbit order type integer to OrderType.
pub fn convert_order_type_from_ourbit(order_type: i32) -> OrderType {
    match order_type {
        ORDER_TYPE_LIMIT => OrderType::Limit,
        ORDER_TYPE_POST_ONLY => OrderType::Limit,
        ORDER_TYPE_IOC => OrderType::Market,
        ORDER_TYPE_FOK => OrderType::Market,
        ORDER_TYPE_MARKET => OrderType::Market,
        _ => {
            warn!(
                "Unknown Ourbit order type: {}, defaulting to Market",
                order_type
            );
            OrderType::Market
        }
    }
}

/// Parse price string to f64.
#[allow(dead_code)]
pub fn parse_price(s: Option<&str>) -> Option<f64> {
    s.and_then(|v| v.parse::<f64>().ok())
}

/// Parse quantity string to f64 with default.
#[allow(dead_code)]
pub fn parse_quantity(s: Option<&str>) -> f64 {
    s.and_then(|v| v.parse::<f64>().ok()).unwrap_or(0.0)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_format_symbol_btcusdt() {
        assert_eq!(format_symbol("BTCUSDT"), "BTC_USDT");
        assert_eq!(format_symbol("btcusdt"), "BTC_USDT");
    }

    #[test]
    fn test_format_symbol_base_only() {
        assert_eq!(format_symbol("BTC"), "BTC_USDT");
        assert_eq!(format_symbol("ETH"), "ETH_USDT");
    }

    #[test]
    fn test_format_symbol_slash_separator() {
        assert_eq!(format_symbol("BTC/USDT"), "BTC_USDT");
        assert_eq!(format_symbol("ETH/USDC"), "ETH_USDC");
    }

    #[test]
    fn test_format_symbol_dash_separator() {
        assert_eq!(format_symbol("BTC-USDT"), "BTC_USDT");
    }

    #[test]
    fn test_format_symbol_underscore_separator() {
        assert_eq!(format_symbol("BTC_USDT"), "BTC_USDT");
    }

    #[test]
    fn test_parse_order_status_all() {
        assert_eq!(parse_order_status(1), OrderStatus::New);
        assert_eq!(parse_order_status(2), OrderStatus::PartiallyFilled);
        assert_eq!(parse_order_status(3), OrderStatus::Filled);
        assert_eq!(parse_order_status(4), OrderStatus::Cancelled);
        assert_eq!(parse_order_status(5), OrderStatus::Rejected);
        assert_eq!(parse_order_status(0), OrderStatus::New);
    }

    #[test]
    fn test_side_conversion_roundtrip() {
        for &(side, reduce_only) in &[
            (OrderSide::Buy, false),
            (OrderSide::Buy, true),
            (OrderSide::Sell, false),
            (OrderSide::Sell, true),
        ] {
            let ourbit_side = convert_side_to_ourbit(side, reduce_only);
            let (converted_side, converted_reduce_only) = convert_side_from_ourbit(ourbit_side);
            assert_eq!(converted_side, side);
            assert_eq!(converted_reduce_only, reduce_only);
        }
    }

    #[test]
    fn test_next_reconnect_delay() {
        assert_eq!(next_reconnect_delay(1), 2);
        assert_eq!(next_reconnect_delay(30), 60);
        assert_eq!(next_reconnect_delay(60), 60);
    }

    #[test]
    fn test_initial_reconnect_delay() {
        assert_eq!(initial_reconnect_delay(), 1);
    }
}
