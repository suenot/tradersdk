//! Gate.io helper functions.

use hmac::{Hmac, Mac};
use sha2::Sha512;
use tracing::warn;
use tradersdk_core::models::{MarketType, OrderStatus};

use crate::config::MAX_RECONNECT_DELAY_SECS;

/// Calculate next reconnect delay with exponential backoff (capped).
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

/// Format symbol for Gate.io API.
/// Both Spot and Futures use BTC_USDT format.
pub fn format_symbol(base: &str, _market_type: MarketType) -> String {
    let s = base.to_uppercase();

    // Handle BTC/USDT format
    if s.contains('/') {
        return s.replace("/", "_");
    }

    // Both spot and futures use BTC_USDT format
    if s.contains('_') {
        s
    } else if s.ends_with("USDT") {
        let b = s.trim_end_matches("USDT");
        format!("{}_USDT", b)
    } else {
        format!("{}_USDT", s)
    }
}

/// Generate HMAC-SHA512 signature for Gate.io WebSocket authentication.
/// Sign string format: channel=<channel>&event=<event>&time=<time>
pub fn ws_auth_signature(api_secret: &str, channel: &str, event: &str, time: i64) -> String {
    let sign_string = format!("channel={}&event={}&time={}", channel, event, time);

    type HmacSha512 = Hmac<Sha512>;
    let mut mac =
        HmacSha512::new_from_slice(api_secret.as_bytes()).expect("HMAC can take key of any size");
    mac.update(sign_string.as_bytes());
    hex::encode(mac.finalize().into_bytes())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_next_reconnect_delay() {
        assert_eq!(next_reconnect_delay(1), 2);
        assert_eq!(next_reconnect_delay(2), 4);
        assert_eq!(next_reconnect_delay(15), 30);
        assert_eq!(next_reconnect_delay(30), 30); // Capped at MAX
    }

    #[test]
    fn test_parse_order_status() {
        assert_eq!(parse_order_status("open"), OrderStatus::New);
        assert_eq!(parse_order_status("filled"), OrderStatus::Filled);
        assert_eq!(parse_order_status("cancelled"), OrderStatus::Cancelled);
    }

    #[test]
    fn test_format_symbol() {
        assert_eq!(format_symbol("BTCUSDT", MarketType::Spot), "BTC_USDT");
        assert_eq!(format_symbol("BTC", MarketType::Spot), "BTC_USDT");
        assert_eq!(format_symbol("BTC_USDT", MarketType::Spot), "BTC_USDT");
        assert_eq!(format_symbol("BTC/USDT", MarketType::Spot), "BTC_USDT");
        assert_eq!(format_symbol("BTCUSDT", MarketType::Swap), "BTC_USDT");
    }
}
