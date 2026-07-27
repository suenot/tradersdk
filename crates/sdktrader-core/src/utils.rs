//! Utility functions for exchange operations.

use hmac::{Hmac, Mac};
use sha2::Sha256;

type HmacSha256 = Hmac<Sha256>;

/// Sign a message with HMAC-SHA256.
///
/// Used for authenticating API requests to exchanges.
///
/// # Example
/// ```
/// use sdktrader_core::utils::hmac_sign;
///
/// let signature = hmac_sign("secret_key", "message_to_sign");
/// assert_eq!(signature.len(), 64); // 32 bytes = 64 hex chars
/// ```
pub fn hmac_sign(secret: &str, message: &str) -> String {
    let mut mac = HmacSha256::new_from_slice(secret.as_bytes())
        .expect("HMAC can take key of any size");
    mac.update(message.as_bytes());

    let result = mac.finalize();
    hex::encode(result.into_bytes())
}

/// Sign a message with HMAC-SHA256 and return base64.
pub fn hmac_sign_base64(secret: &str, message: &str) -> String {
    let mut mac = HmacSha256::new_from_slice(secret.as_bytes())
        .expect("HMAC can take key of any size");
    mac.update(message.as_bytes());

    let result = mac.finalize();
    base64::Engine::encode(&base64::engine::general_purpose::STANDARD, result.into_bytes())
}

/// Get current timestamp in milliseconds.
pub fn timestamp_ms() -> i64 {
    chrono::Utc::now().timestamp_millis()
}

/// Get current timestamp in seconds.
pub fn timestamp_secs() -> i64 {
    chrono::Utc::now().timestamp()
}

/// Format quantity to exchange precision.
///
/// Truncates (not rounds) the quantity to avoid exceeding limits.
pub fn format_quantity(quantity: f64, precision: u8) -> String {
    let factor = 10_f64.powi(precision as i32);
    let truncated = (quantity * factor).floor() / factor;
    format!("{:.prec$}", truncated, prec = precision as usize)
}

/// Format price to exchange precision.
pub fn format_price(price: f64, precision: u8) -> String {
    format!("{:.prec$}", price, prec = precision as usize)
}

/// Parse symbol into base and quote currencies.
///
/// Handles formats like "BTCUSDT", "BTC/USDT", "BTC-USDT"
pub fn parse_symbol(symbol: &str) -> Option<(String, String)> {
    let symbol = symbol.to_uppercase();

    // Handle BTC/USDT or BTC-USDT format
    if symbol.contains('/') {
        let parts: Vec<&str> = symbol.split('/').collect();
        if parts.len() == 2 {
            return Some((parts[0].to_string(), parts[1].to_string()));
        }
    }

    if symbol.contains('-') {
        let parts: Vec<&str> = symbol.split('-').collect();
        if parts.len() == 2 {
            return Some((parts[0].to_string(), parts[1].to_string()));
        }
    }

    // Handle BTCUSDT format
    let quote_currencies = ["USDT", "USDC", "BUSD", "USD", "BTC", "ETH"];
    for quote in quote_currencies {
        if symbol.ends_with(quote) {
            let base = &symbol[..symbol.len() - quote.len()];
            if !base.is_empty() {
                return Some((base.to_string(), quote.to_string()));
            }
        }
    }

    None
}

/// Normalize symbol to standard format (BTCUSDT).
pub fn normalize_symbol(symbol: &str) -> String {
    let symbol = symbol.to_uppercase();
    symbol
        .replace("/", "")
        .replace("-", "")
        .replace("_", "")
}

/// Calculate slippage percentage.
pub fn calculate_slippage(expected_price: f64, actual_price: f64) -> f64 {
    if expected_price == 0.0 {
        return 0.0;
    }
    ((actual_price - expected_price) / expected_price).abs() * 100.0
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_hmac_sign() {
        let signature = hmac_sign("secret", "message");
        assert!(!signature.is_empty());
        // HMAC-SHA256 produces 64 hex characters
        assert_eq!(signature.len(), 64);
    }

    #[test]
    fn test_format_quantity() {
        assert_eq!(format_quantity(1.23456789, 4), "1.2345");
        assert_eq!(format_quantity(0.001, 8), "0.00100000");
    }

    #[test]
    fn test_parse_symbol() {
        assert_eq!(
            parse_symbol("BTCUSDT"),
            Some(("BTC".to_string(), "USDT".to_string()))
        );
        assert_eq!(
            parse_symbol("BTC/USDT"),
            Some(("BTC".to_string(), "USDT".to_string()))
        );
        assert_eq!(
            parse_symbol("BTC-USDT"),
            Some(("BTC".to_string(), "USDT".to_string()))
        );
        assert_eq!(
            parse_symbol("ETHBTC"),
            Some(("ETH".to_string(), "BTC".to_string()))
        );
    }

    #[test]
    fn test_normalize_symbol() {
        assert_eq!(normalize_symbol("BTC/USDT"), "BTCUSDT");
        assert_eq!(normalize_symbol("BTC-USDT"), "BTCUSDT");
        assert_eq!(normalize_symbol("btcusdt"), "BTCUSDT");
    }

    #[test]
    fn test_calculate_slippage() {
        assert_eq!(calculate_slippage(100.0, 101.0), 1.0);
        assert_eq!(calculate_slippage(100.0, 99.0), 1.0);
    }
}
