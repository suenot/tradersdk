//! Helper functions for Bybit exchange adapter.

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

/// Parse a status string from Bybit into OrderStatus.
///
/// Bybit uses various status strings including:
/// - New, Open, Live, Created, Init, Accepted, Untriggered, Triggered -> New
/// - PartiallyFilled, Partial -> PartiallyFilled
/// - Filled, Closed -> Filled
/// - Cancelled, Canceled, Deactivated -> Cancelled
/// - Rejected, Fail, Failed -> Rejected
/// - Expired -> Expired
pub fn parse_order_status(s: &str) -> OrderStatus {
    match s.to_lowercase().as_str() {
        "new" | "open" | "live" | "created" | "init" | "accepted" | "untriggered" | "triggered" => {
            OrderStatus::New
        }
        "partially_filled" | "partiallyfilled" | "partial_filled" | "partial" => {
            OrderStatus::PartiallyFilled
        }
        "filled" | "closed" | "full_fill" | "full" => OrderStatus::Filled,
        "cancelled" | "canceled" | "cancel" | "deactivated" => OrderStatus::Cancelled,
        "rejected" | "fail" | "failed" => OrderStatus::Rejected,
        "expired" | "expire" => OrderStatus::Expired,
        _ => {
            warn!("Unknown Bybit order status: '{}'", s);
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

/// Format symbol for Bybit API.
///
/// Bybit expects uppercase symbols without separators.
/// Examples:
/// - "btc/usdt" -> "BTCUSDT"
/// - "BTC-USDT" -> "BTCUSDT"
/// - "BTC" -> "BTCUSDT" (appends USDT if not present)
pub fn format_symbol(symbol: &str) -> String {
    let symbol = symbol.to_uppercase().replace(['/', '-', ':'], "");

    if symbol.ends_with("USDT") || symbol.ends_with("USDC") {
        symbol
    } else {
        format!("{}USDT", symbol)
    }
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
        // Standard Bybit statuses
        assert!(matches!(parse_order_status("New"), OrderStatus::New));
        assert!(matches!(parse_order_status("Filled"), OrderStatus::Filled));
        assert!(matches!(
            parse_order_status("PartiallyFilled"),
            OrderStatus::PartiallyFilled
        ));
        assert!(matches!(
            parse_order_status("Cancelled"),
            OrderStatus::Cancelled
        ));
        assert!(matches!(
            parse_order_status("Rejected"),
            OrderStatus::Rejected
        ));
        assert!(matches!(
            parse_order_status("Expired"),
            OrderStatus::Expired
        ));

        // Alternative statuses
        assert!(matches!(parse_order_status("Open"), OrderStatus::New));
        assert!(matches!(parse_order_status("Live"), OrderStatus::New));
        assert!(matches!(
            parse_order_status("Untriggered"),
            OrderStatus::New
        ));
        assert!(matches!(parse_order_status("Triggered"), OrderStatus::New));
        assert!(matches!(parse_order_status("Closed"), OrderStatus::Filled));
        assert!(matches!(
            parse_order_status("Canceled"),
            OrderStatus::Cancelled
        ));
        assert!(matches!(
            parse_order_status("Deactivated"),
            OrderStatus::Cancelled
        ));
        assert!(matches!(
            parse_order_status("Failed"),
            OrderStatus::Rejected
        ));

        // Case insensitivity
        assert!(matches!(parse_order_status("new"), OrderStatus::New));
        assert!(matches!(parse_order_status("FILLED"), OrderStatus::Filled));
        assert!(matches!(
            parse_order_status("partiallyfilled"),
            OrderStatus::PartiallyFilled
        ));
    }

    #[test]
    fn test_format_symbol() {
        assert_eq!(format_symbol("btc/usdt"), "BTCUSDT");
        assert_eq!(format_symbol("BTC-USDT"), "BTCUSDT");
        assert_eq!(format_symbol("BTC:USDT"), "BTCUSDT");
        assert_eq!(format_symbol("BTCUSDT"), "BTCUSDT");
        assert_eq!(format_symbol("BTC"), "BTCUSDT");
        assert_eq!(format_symbol("eth"), "ETHUSDT");
        assert_eq!(format_symbol("ETH/USDC"), "ETHUSDC");
    }

    #[test]
    fn test_next_reconnect_delay() {
        assert_eq!(next_reconnect_delay(1), 2);
        assert_eq!(next_reconnect_delay(2), 4);
        assert_eq!(next_reconnect_delay(4), 8);
        assert_eq!(next_reconnect_delay(8), 16);
        assert_eq!(next_reconnect_delay(16), 30); // capped at MAX_RECONNECT_DELAY_SECS
        assert_eq!(next_reconnect_delay(30), 30); // stays at max
        assert_eq!(next_reconnect_delay(100), 30); // also capped
    }

    #[test]
    fn test_initial_reconnect_delay() {
        assert_eq!(initial_reconnect_delay(), INITIAL_RECONNECT_DELAY_SECS);
        assert_eq!(initial_reconnect_delay(), 1);
    }

    #[test]
    fn test_parse_price() {
        assert_eq!(parse_price(Some("123.45")), Some(123.45));
        assert_eq!(parse_price(Some("0.00001")), Some(0.00001));
        assert_eq!(parse_price(Some("invalid")), None);
        assert_eq!(parse_price(None), None);
        assert_eq!(parse_price(Some("")), None);
    }

    #[test]
    fn test_parse_quantity() {
        assert_eq!(parse_quantity(Some("100.5")), 100.5);
        assert_eq!(parse_quantity(Some("0.001")), 0.001);
        assert_eq!(parse_quantity(Some("invalid")), 0.0);
        assert_eq!(parse_quantity(None), 0.0);
        assert_eq!(parse_quantity(Some("")), 0.0);
    }
}
