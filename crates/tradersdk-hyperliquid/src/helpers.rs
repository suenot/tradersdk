//! Hyperliquid helper functions.

use tracing::warn;
use tradersdk_core::models::OrderStatus;

use crate::config::MAX_RECONNECT_DELAY_SECS;

/// Calculate next reconnect delay with exponential backoff (capped at MAX_RECONNECT_DELAY_SECS).
pub fn next_reconnect_delay(current_secs: u64) -> u64 {
    (current_secs * 2).min(MAX_RECONNECT_DELAY_SECS)
}

/// Parse a status string into OrderStatus.
pub fn parse_order_status(s: &str) -> OrderStatus {
    match s.to_lowercase().as_str() {
        "new" | "open" | "live" | "created" | "init" | "accepted" => OrderStatus::New,
        "partially_filled" | "partiallyfilled" | "partial_filled" | "partial" => {
            OrderStatus::PartiallyFilled
        }
        "filled" | "closed" | "full_fill" | "full" => OrderStatus::Filled,
        "cancelled" | "canceled" | "cancel" | "deactivated" => OrderStatus::Cancelled,
        "rejected" | "fail" | "failed" => OrderStatus::Rejected,
        "expired" | "expire" => OrderStatus::Expired,
        _ => {
            warn!("Unknown order status string: '{}'", s);
            OrderStatus::New
        }
    }
}

/// Convert symbol to Hyperliquid format (just the base asset).
/// Examples: "BTCUSDT" -> "BTC", "BTC/USDT" -> "BTC", "ETHPERP" -> "ETH"
pub fn to_hl_symbol(symbol: &str) -> String {
    let upper = symbol.to_uppercase();
    // Handle BTC/USDT format -> take base only
    if upper.contains('/') {
        return upper.split('/').next().unwrap_or(&upper).to_string();
    }
    // Strip USDT/USD/PERP suffix
    upper
        .strip_suffix("USDT")
        .or_else(|| upper.strip_suffix("USD"))
        .or_else(|| upper.strip_suffix("PERP"))
        .unwrap_or(&upper)
        .to_string()
}

/// Format quantity with proper decimals.
pub fn format_size(size: f64, decimals: i32) -> String {
    let factor = 10_f64.powi(decimals);
    let rounded = (size * factor).round() / factor;
    format!("{:.prec$}", rounded, prec = decimals as usize)
}

/// Format price (5 significant figures as per Hyperliquid spec).
pub fn format_price(price: f64) -> String {
    if price >= 1.0 {
        let digits = (price.log10().floor() as i32) + 1;
        let decimals = (5 - digits).max(0);
        format!("{:.prec$}", price, prec = decimals as usize)
    } else if price > 0.0 {
        // For prices < 1, count leading zeros after decimal
        let log_val = -price.log10().floor() as i32;
        let decimals = log_val + 5;
        format!("{:.prec$}", price, prec = decimals as usize)
    } else {
        "0".to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_next_reconnect_delay() {
        assert_eq!(next_reconnect_delay(1), 2);
        assert_eq!(next_reconnect_delay(2), 4);
        assert_eq!(next_reconnect_delay(16), 30); // Capped at MAX
        assert_eq!(next_reconnect_delay(30), 30); // Already at max
    }

    #[test]
    fn test_to_hl_symbol() {
        assert_eq!(to_hl_symbol("BTCUSDT"), "BTC");
        assert_eq!(to_hl_symbol("BTC/USDT"), "BTC");
        assert_eq!(to_hl_symbol("btc"), "BTC");
        assert_eq!(to_hl_symbol("ETHPERP"), "ETH");
        assert_eq!(to_hl_symbol("SOL"), "SOL");
    }

    #[test]
    fn test_format_size() {
        assert_eq!(format_size(1.23456789, 2), "1.23");
        assert_eq!(format_size(1.23456789, 4), "1.2346");
        assert_eq!(format_size(0.00123456, 6), "0.001235");
    }

    #[test]
    fn test_format_price() {
        assert_eq!(format_price(50000.0), "50000");
        assert_eq!(format_price(12345.6789), "12346");
        assert_eq!(format_price(1.23456), "1.2346");
        assert_eq!(format_price(0.00012345), "0.000123450");
    }

    #[test]
    fn test_parse_order_status() {
        assert_eq!(parse_order_status("open"), OrderStatus::New);
        assert_eq!(parse_order_status("filled"), OrderStatus::Filled);
        assert_eq!(parse_order_status("canceled"), OrderStatus::Cancelled);
        assert_eq!(parse_order_status("rejected"), OrderStatus::Rejected);
    }
}
