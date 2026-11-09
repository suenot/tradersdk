//! Helper functions for Binance exchange adapter.

use tracing::{info, warn};
use tradersdk_core::models::{OrderStatus, OrderUpdateEvent};

use crate::config::{INITIAL_RECONNECT_DELAY_SECS, MAX_RECONNECT_DELAY_SECS};

/// Calculate next reconnect delay with exponential backoff (capped).
pub fn next_reconnect_delay(current_secs: u64) -> u64 {
    (current_secs * 2).min(MAX_RECONNECT_DELAY_SECS)
}

/// Get initial reconnect delay.
pub fn initial_reconnect_delay() -> u64 {
    INITIAL_RECONNECT_DELAY_SECS
}

/// Parse a status string from Binance into OrderStatus.
pub fn parse_order_status(s: &str) -> OrderStatus {
    match s.to_uppercase().as_str() {
        "NEW" => OrderStatus::New,
        "PARTIALLY_FILLED" => OrderStatus::PartiallyFilled,
        "FILLED" => OrderStatus::Filled,
        "CANCELED" | "CANCELLED" => OrderStatus::Cancelled,
        "REJECTED" => OrderStatus::Rejected,
        "EXPIRED" | "EXPIRED_IN_MATCH" => OrderStatus::Expired,
        _ => {
            warn!("Unknown Binance order status: '{}'", s);
            OrderStatus::New
        }
    }
}

/// Log an order update event with exchange/local timestamp delta.
pub fn log_order_update(event: &OrderUpdateEvent) {
    let delta = event.received_at - event.exchange_timestamp;
    info!(
        "ORDER_UPDATE exchange={} order_id={} symbol={} status={:?} side={:?} type={:?} \
         filled={}/{} price={} avg_fill={} fee={} exchange_ts={} local_ts={} delta_ms={}",
        event.exchange,
        event.order_id,
        event.symbol,
        event.status,
        event.side,
        event.order_type,
        event.filled_quantity,
        event.quantity,
        event.price,
        event.avg_fill_price.unwrap_or(0.0),
        event.fee.unwrap_or(0.0),
        event.exchange_timestamp,
        event.received_at,
        delta,
    );
}

/// Format symbol for Binance API (uppercase, no special chars).
pub fn format_symbol(symbol: &str) -> String {
    symbol.to_uppercase().replace(['/', '-', ':'], "")
}

/// Parse price string to f64 with default.
pub fn parse_price(s: Option<&str>) -> Option<f64> {
    s.and_then(|v| v.parse::<f64>().ok())
}

/// Parse quantity string to f64 with default.
pub fn parse_quantity(s: Option<&str>) -> f64 {
    s.and_then(|v| v.parse::<f64>().ok()).unwrap_or(0.0)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_order_status() {
        assert!(matches!(parse_order_status("NEW"), OrderStatus::New));
        assert!(matches!(parse_order_status("FILLED"), OrderStatus::Filled));
        assert!(matches!(
            parse_order_status("PARTIALLY_FILLED"),
            OrderStatus::PartiallyFilled
        ));
        assert!(matches!(
            parse_order_status("CANCELED"),
            OrderStatus::Cancelled
        ));
        assert!(matches!(
            parse_order_status("CANCELLED"),
            OrderStatus::Cancelled
        ));
        assert!(matches!(
            parse_order_status("REJECTED"),
            OrderStatus::Rejected
        ));
        assert!(matches!(
            parse_order_status("EXPIRED"),
            OrderStatus::Expired
        ));
    }

    #[test]
    fn test_format_symbol() {
        assert_eq!(format_symbol("btc/usdt"), "BTCUSDT");
        assert_eq!(format_symbol("BTC-USDT"), "BTCUSDT");
        assert_eq!(format_symbol("BTC:USDT"), "BTCUSDT");
        assert_eq!(format_symbol("BTCUSDT"), "BTCUSDT");
    }

    #[test]
    fn test_next_reconnect_delay() {
        assert_eq!(next_reconnect_delay(1), 2);
        assert_eq!(next_reconnect_delay(2), 4);
        assert_eq!(next_reconnect_delay(16), 30); // capped
        assert_eq!(next_reconnect_delay(30), 30); // stays at max
    }
}
