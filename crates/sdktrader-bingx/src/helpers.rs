//! Helper functions for BingX exchange adapter.

use anyhow::Result;
use flate2::read::GzDecoder;
use sdktrader_core::models::{OrderStatus, OrderUpdateEvent};
use std::io::Read;
use tracing::{info, warn};

use super::config::{INITIAL_RECONNECT_DELAY_SECS, MAX_RECONNECT_DELAY_SECS};

/// Calculate next reconnect delay with exponential backoff (capped).
pub fn next_reconnect_delay(current_secs: u64) -> u64 {
    (current_secs * 2).min(MAX_RECONNECT_DELAY_SECS)
}

/// Get initial reconnect delay.
pub fn initial_reconnect_delay() -> u64 {
    INITIAL_RECONNECT_DELAY_SECS
}

/// Parse a status string from BingX into OrderStatus.
///
/// BingX uses various status strings including:
/// - NEW, PENDING, WORKING -> New
/// - PARTIALLY_FILLED -> PartiallyFilled
/// - FILLED -> Filled
/// - CANCELLED, CANCELED -> Cancelled
/// - REJECTED, FAILED -> Rejected
/// - EXPIRED -> Expired
pub fn parse_order_status(s: &str) -> OrderStatus {
    match s.to_uppercase().as_str() {
        "NEW" | "PENDING" | "WORKING" => OrderStatus::New,
        "PARTIALLY_FILLED" => OrderStatus::PartiallyFilled,
        "FILLED" => OrderStatus::Filled,
        "CANCELLED" | "CANCELED" => OrderStatus::Cancelled,
        "REJECTED" | "FAILED" => OrderStatus::Rejected,
        "EXPIRED" => OrderStatus::Expired,
        _ => {
            warn!("Unknown BingX order status: '{}'", s);
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

/// Format symbol for BingX API.
///
/// BingX expects symbols with hyphen separator (e.g., BTC-USDT).
/// Examples:
/// - "BTC/USDT" -> "BTC-USDT"
/// - "BTCUSDT" -> "BTC-USDT"
/// - "BTC-USDT" -> "BTC-USDT" (unchanged)
/// - "BTC" -> "BTC-USDT" (appends -USDT if not present)
pub fn format_symbol(base_symbol: &str) -> String {
    let symbol = base_symbol.to_uppercase();
    // Handle BTC/USDT format -> BTC-USDT
    if symbol.contains('/') {
        return symbol.replace('/', "-");
    }
    // Already in BingX format
    if symbol.contains('-') {
        return symbol;
    }
    // BTCUSDT -> BTC-USDT
    if symbol.ends_with("USDT") {
        let base = symbol.trim_end_matches("USDT");
        return format!("{}-USDT", base);
    }
    // BTCUSDC -> BTC-USDC
    if symbol.ends_with("USDC") {
        let base = symbol.trim_end_matches("USDC");
        return format!("{}-USDC", base);
    }
    // BTC -> BTC-USDT
    format!("{}-USDT", symbol)
}

/// Decompress gzip-compressed data from BingX WebSocket.
///
/// BingX sends gzip-compressed binary messages through WebSocket.
/// This function decompresses them to a UTF-8 string.
pub fn decompress(data: &[u8]) -> Result<String> {
    let mut decoder = GzDecoder::new(data);
    let mut s = String::new();
    decoder.read_to_string(&mut s)?;
    Ok(s)
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
        // Standard BingX statuses
        assert!(matches!(parse_order_status("NEW"), OrderStatus::New));
        assert!(matches!(parse_order_status("FILLED"), OrderStatus::Filled));
        assert!(matches!(
            parse_order_status("PARTIALLY_FILLED"),
            OrderStatus::PartiallyFilled
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

        // Alternative statuses
        assert!(matches!(parse_order_status("PENDING"), OrderStatus::New));
        assert!(matches!(parse_order_status("WORKING"), OrderStatus::New));
        assert!(matches!(
            parse_order_status("CANCELED"),
            OrderStatus::Cancelled
        ));
        assert!(matches!(
            parse_order_status("FAILED"),
            OrderStatus::Rejected
        ));

        // Case insensitivity
        assert!(matches!(parse_order_status("new"), OrderStatus::New));
        assert!(matches!(parse_order_status("filled"), OrderStatus::Filled));
        assert!(matches!(
            parse_order_status("partially_filled"),
            OrderStatus::PartiallyFilled
        ));

        // Unknown status defaults to New
        assert!(matches!(parse_order_status("UNKNOWN"), OrderStatus::New));
    }

    #[test]
    fn test_format_symbol() {
        // Basic symbol (appends -USDT)
        assert_eq!(format_symbol("BTC"), "BTC-USDT");
        assert_eq!(format_symbol("ETH"), "ETH-USDT");
        assert_eq!(format_symbol("eth"), "ETH-USDT");

        // BTCUSDT format -> BTC-USDT
        assert_eq!(format_symbol("BTCUSDT"), "BTC-USDT");
        assert_eq!(format_symbol("ETHUSDT"), "ETH-USDT");

        // BTC/USDT format -> BTC-USDT
        assert_eq!(format_symbol("BTC/USDT"), "BTC-USDT");
        assert_eq!(format_symbol("ETH/USDT"), "ETH-USDT");

        // Already in BingX format
        assert_eq!(format_symbol("BTC-USDT"), "BTC-USDT");
        assert_eq!(format_symbol("ETH-USDT"), "ETH-USDT");

        // USDC pairs
        assert_eq!(format_symbol("BTCUSDC"), "BTC-USDC");
        assert_eq!(format_symbol("ETH/USDC"), "ETH-USDC");
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

    #[test]
    fn test_decompress_valid_gzip() {
        use flate2::write::GzEncoder;
        use flate2::Compression;
        use std::io::Write;

        // Create gzip-compressed data
        let original = "Hello, BingX!";
        let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
        encoder.write_all(original.as_bytes()).unwrap();
        let compressed = encoder.finish().unwrap();

        // Decompress and verify
        let result = decompress(&compressed);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), original);
    }

    #[test]
    fn test_decompress_invalid_data() {
        let invalid_data = b"not gzip data";
        let result = decompress(invalid_data);
        assert!(result.is_err());
    }
}
