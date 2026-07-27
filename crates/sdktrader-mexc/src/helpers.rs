//! MEXC helper functions.

use sdktrader_core::models::{MarketType, OrderStatus};
use crate::config::MAX_RECONNECT_DELAY_SECS;

/// Calculate next reconnect delay with exponential backoff.
pub fn next_reconnect_delay(current: u64) -> u64 {
    (current * 2).min(MAX_RECONNECT_DELAY_SECS)
}

/// Parse order status from string.
pub fn parse_order_status(status: &str) -> OrderStatus {
    match status.to_uppercase().as_str() {
        "NEW" | "PENDING" | "OPEN" => OrderStatus::New,
        "PARTIALLY_FILLED" | "PARTIAL" => OrderStatus::PartiallyFilled,
        "FILLED" | "CLOSED" => OrderStatus::Filled,
        "CANCELED" | "CANCELLED" => OrderStatus::Cancelled,
        "REJECTED" => OrderStatus::Rejected,
        "EXPIRED" => OrderStatus::Expired,
        _ => OrderStatus::New,
    }
}

/// Format symbol for MEXC API.
/// Spot: BTCUSDT format
/// Futures: BTC_USDT format
pub fn format_symbol(base: &str, market_type: MarketType) -> String {
    let s = base.to_uppercase();

    // Handle BTC/USDT format
    if s.contains('/') {
        let cleaned = s.replace("/", "");
        if market_type == MarketType::Swap {
            // Convert BTCUSDT to BTC_USDT for futures
            if cleaned.ends_with("USDT") {
                let base = cleaned.trim_end_matches("USDT");
                return format!("{}_USDT", base);
            }
        }
        return cleaned;
    }

    if market_type == MarketType::Spot {
        // Spot: BTCUSDT format
        if s.ends_with("USDT") {
            s
        } else {
            format!("{}USDT", s)
        }
    } else {
        // Futures: BTC_USDT format
        if s.contains('_') {
            s
        } else if s.ends_with("USDT") {
            let base = s.trim_end_matches("USDT");
            format!("{}_USDT", base)
        } else {
            format!("{}_USDT", s)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_next_reconnect_delay() {
        assert_eq!(next_reconnect_delay(1), 2);
        assert_eq!(next_reconnect_delay(2), 4);
        assert_eq!(next_reconnect_delay(30), 60);
        assert_eq!(next_reconnect_delay(60), 60); // Capped at MAX
    }

    #[test]
    fn test_parse_order_status() {
        assert_eq!(parse_order_status("NEW"), OrderStatus::New);
        assert_eq!(parse_order_status("FILLED"), OrderStatus::Filled);
        assert_eq!(parse_order_status("CANCELED"), OrderStatus::Cancelled);
        assert_eq!(parse_order_status("PARTIALLY_FILLED"), OrderStatus::PartiallyFilled);
    }

    #[test]
    fn test_format_symbol_spot() {
        assert_eq!(format_symbol("BTCUSDT", MarketType::Spot), "BTCUSDT");
        assert_eq!(format_symbol("BTC", MarketType::Spot), "BTCUSDT");
        assert_eq!(format_symbol("BTC/USDT", MarketType::Spot), "BTCUSDT");
    }

    #[test]
    fn test_format_symbol_futures() {
        assert_eq!(format_symbol("BTCUSDT", MarketType::Swap), "BTC_USDT");
        assert_eq!(format_symbol("BTC", MarketType::Swap), "BTC_USDT");
        assert_eq!(format_symbol("BTC_USDT", MarketType::Swap), "BTC_USDT");
        assert_eq!(format_symbol("BTC/USDT", MarketType::Swap), "BTC_USDT");
    }
}
