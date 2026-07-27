//! Exchange-specific symbol formatting.
//!
//! Ported from Python `position_exit_manager.py` methods
//! `_format_symbol_for_exchange` and `_extract_base_symbol`.
//! Pure computation — no exchange API calls.

use serde::{Deserialize, Serialize};
use std::fmt;

/// Supported exchanges for symbol formatting.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum Exchange {
    Bybit,
    Binance,
    BingX,
    Hyperliquid,
    OKX,
    Gate,
    MEXC,
    Bitget,
    Kraken,
    Kucoin,
    Deribit,
    Phemex,
    XT,
    HTX,
    Bitfinex,
    Bitmart,
    Coinex,
    Lbank,
    Weex,
    Ourbit,
}

impl fmt::Display for Exchange {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Exchange::Bybit => write!(f, "bybit"),
            Exchange::Binance => write!(f, "binance"),
            Exchange::BingX => write!(f, "bingx"),
            Exchange::Hyperliquid => write!(f, "hyperliquid"),
            Exchange::OKX => write!(f, "okx"),
            Exchange::Gate => write!(f, "gate"),
            Exchange::MEXC => write!(f, "mexc"),
            Exchange::Bitget => write!(f, "bitget"),
            Exchange::Kraken => write!(f, "kraken"),
            Exchange::Kucoin => write!(f, "kucoin"),
            Exchange::Deribit => write!(f, "deribit"),
            Exchange::Phemex => write!(f, "phemex"),
            Exchange::XT => write!(f, "xt"),
            Exchange::HTX => write!(f, "htx"),
            Exchange::Bitfinex => write!(f, "bitfinex"),
            Exchange::Bitmart => write!(f, "bitmart"),
            Exchange::Coinex => write!(f, "coinex"),
            Exchange::Lbank => write!(f, "lbank"),
            Exchange::Weex => write!(f, "weex"),
            Exchange::Ourbit => write!(f, "ourbit"),
        }
    }
}

impl Exchange {
    /// Parse exchange name from a case-insensitive string.
    ///
    /// Returns `None` for unrecognized names.
    #[allow(clippy::should_implement_trait)] // returns Option, not Result
    pub fn from_str(s: &str) -> Option<Self> {
        match s.to_lowercase().as_str() {
            "bybit" => Some(Exchange::Bybit),
            "binance" => Some(Exchange::Binance),
            "bingx" => Some(Exchange::BingX),
            "hyperliquid" => Some(Exchange::Hyperliquid),
            "okx" => Some(Exchange::OKX),
            "gate" => Some(Exchange::Gate),
            "mexc" => Some(Exchange::MEXC),
            "bitget" => Some(Exchange::Bitget),
            "kraken" => Some(Exchange::Kraken),
            "kucoin" => Some(Exchange::Kucoin),
            "deribit" => Some(Exchange::Deribit),
            "phemex" => Some(Exchange::Phemex),
            "xt" => Some(Exchange::XT),
            "htx" => Some(Exchange::HTX),
            "bitfinex" => Some(Exchange::Bitfinex),
            "bitmart" => Some(Exchange::Bitmart),
            "coinex" => Some(Exchange::Coinex),
            "lbank" => Some(Exchange::Lbank),
            "weex" => Some(Exchange::Weex),
            "ourbit" => Some(Exchange::Ourbit),
            _ => None,
        }
    }

    /// Format a base symbol (e.g. `"BTC"`) into the exchange-specific format.
    ///
    /// # Examples
    /// ```
    /// use sdktrader_trading::symbol::Exchange;
    ///
    /// assert_eq!(Exchange::Bybit.format_symbol("BTC"), "BTCUSDT");
    /// assert_eq!(Exchange::BingX.format_symbol("BTC"), "BTC-USDT");
    /// assert_eq!(Exchange::Hyperliquid.format_symbol("BTC"), "BTC");
    /// assert_eq!(Exchange::OKX.format_symbol("BTC"), "BTC-USDT-SWAP");
    /// assert_eq!(Exchange::Gate.format_symbol("BTC"), "BTC_USDT");
    /// ```
    pub fn format_symbol(&self, base: &str) -> String {
        let base_upper = base.to_uppercase();
        match self {
            Exchange::Bybit | Exchange::Binance | Exchange::Bitget => {
                format!("{}USDT", base_upper)
            }
            Exchange::BingX => format!("{}-USDT", base_upper),
            Exchange::Hyperliquid => base_upper,
            Exchange::OKX => format!("{}-USDT-SWAP", base_upper),
            Exchange::Gate | Exchange::MEXC => format!("{}_USDT", base_upper),
            // All other exchanges default to {BASE}USDT
            _ => format!("{}USDT", base_upper),
        }
    }
}

/// Extract the base symbol from any exchange-specific format.
///
/// Strips known suffixes in priority order:
/// - `-USDT-SWAP` (OKX)
/// - `-USDT` / `-USDC` (BingX, etc.)
/// - `_USDT` (Gate, MEXC)
/// - `USDT` / `USDC` (Bybit, Binance, etc.)
///
/// # Examples
/// ```
/// use sdktrader_trading::symbol::extract_base_symbol;
///
/// assert_eq!(extract_base_symbol("BTCUSDT"), "BTC");
/// assert_eq!(extract_base_symbol("BTC-USDT"), "BTC");
/// assert_eq!(extract_base_symbol("BTC_USDT"), "BTC");
/// assert_eq!(extract_base_symbol("BTC-USDT-SWAP"), "BTC");
/// assert_eq!(extract_base_symbol("BTC"), "BTC");
/// ```
pub fn extract_base_symbol(symbol: &str) -> &str {
    let s = symbol.trim();

    // Check longest suffixes first to avoid partial matches.
    // OKX: -USDT-SWAP
    if let Some(base) = strip_suffix_ci(s, "-USDT-SWAP") {
        return base;
    }
    // Dash-separated: -USDT, -USDC
    if let Some(base) = strip_suffix_ci(s, "-USDT") {
        return base;
    }
    if let Some(base) = strip_suffix_ci(s, "-USDC") {
        return base;
    }
    // Underscore-separated: _USDT
    if let Some(base) = strip_suffix_ci(s, "_USDT") {
        return base;
    }
    // No separator: USDT, USDC
    if let Some(base) = strip_suffix_ci(s, "USDT") {
        return base;
    }
    if let Some(base) = strip_suffix_ci(s, "USDC") {
        return base;
    }
    s
}

/// Case-insensitive suffix stripping that returns the original slice prefix.
fn strip_suffix_ci<'a>(s: &'a str, suffix: &str) -> Option<&'a str> {
    if s.len() >= suffix.len() && s[s.len() - suffix.len()..].eq_ignore_ascii_case(suffix) {
        let base = &s[..s.len() - suffix.len()];
        if base.is_empty() {
            None
        } else {
            Some(base)
        }
    } else {
        None
    }
}

/// Format a symbol for a specific exchange given the exchange name as a string.
///
/// If the exchange name is not recognized, defaults to `{BASE}USDT`.
///
/// The input `symbol` may be in any format — the base is extracted first.
///
/// # Examples
/// ```
/// use sdktrader_trading::symbol::format_symbol_for_exchange;
///
/// assert_eq!(format_symbol_for_exchange("SOLUSDT", "bybit"), "SOLUSDT");
/// assert_eq!(format_symbol_for_exchange("SOL-USDT", "bingx"), "SOL-USDT");
/// assert_eq!(format_symbol_for_exchange("SOLUSDT", "hyperliquid"), "SOL");
/// assert_eq!(format_symbol_for_exchange("SOL", "okx"), "SOL-USDT-SWAP");
/// ```
pub fn format_symbol_for_exchange(symbol: &str, exchange: &str) -> String {
    let base = extract_base_symbol(symbol);
    match Exchange::from_str(exchange) {
        Some(ex) => ex.format_symbol(base),
        None => format!("{}USDT", base.to_uppercase()),
    }
}

// ── Tests ────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    // ── Exchange::from_str ──────────────────────────────────────────

    #[test]
    fn test_exchange_from_str() {
        assert_eq!(Exchange::from_str("bybit"), Some(Exchange::Bybit));
        assert_eq!(Exchange::from_str("Bybit"), Some(Exchange::Bybit));
        assert_eq!(Exchange::from_str("BYBIT"), Some(Exchange::Bybit));
        assert_eq!(Exchange::from_str("bingx"), Some(Exchange::BingX));
        assert_eq!(
            Exchange::from_str("hyperliquid"),
            Some(Exchange::Hyperliquid)
        );
        assert_eq!(Exchange::from_str("okx"), Some(Exchange::OKX));
        assert_eq!(Exchange::from_str("gate"), Some(Exchange::Gate));
        assert_eq!(Exchange::from_str("mexc"), Some(Exchange::MEXC));
        assert_eq!(Exchange::from_str("bitget"), Some(Exchange::Bitget));
        assert_eq!(Exchange::from_str("kraken"), Some(Exchange::Kraken));
        assert_eq!(Exchange::from_str("kucoin"), Some(Exchange::Kucoin));
        assert_eq!(Exchange::from_str("deribit"), Some(Exchange::Deribit));
        assert_eq!(Exchange::from_str("phemex"), Some(Exchange::Phemex));
        assert_eq!(Exchange::from_str("xt"), Some(Exchange::XT));
        assert_eq!(Exchange::from_str("htx"), Some(Exchange::HTX));
        assert_eq!(Exchange::from_str("bitfinex"), Some(Exchange::Bitfinex));
        assert_eq!(Exchange::from_str("bitmart"), Some(Exchange::Bitmart));
        assert_eq!(Exchange::from_str("coinex"), Some(Exchange::Coinex));
        assert_eq!(Exchange::from_str("lbank"), Some(Exchange::Lbank));
        assert_eq!(Exchange::from_str("weex"), Some(Exchange::Weex));
        assert_eq!(Exchange::from_str("ourbit"), Some(Exchange::Ourbit));
        assert_eq!(Exchange::from_str("unknown"), None);
    }

    // ── Exchange::format_symbol ─────────────────────────────────────

    #[test]
    fn test_format_symbol_bybit() {
        assert_eq!(Exchange::Bybit.format_symbol("BTC"), "BTCUSDT");
        assert_eq!(Exchange::Bybit.format_symbol("sol"), "SOLUSDT");
    }

    #[test]
    fn test_format_symbol_binance() {
        assert_eq!(Exchange::Binance.format_symbol("ETH"), "ETHUSDT");
    }

    #[test]
    fn test_format_symbol_bingx() {
        assert_eq!(Exchange::BingX.format_symbol("SOL"), "SOL-USDT");
    }

    #[test]
    fn test_format_symbol_hyperliquid() {
        assert_eq!(Exchange::Hyperliquid.format_symbol("BTC"), "BTC");
    }

    #[test]
    fn test_format_symbol_okx() {
        assert_eq!(Exchange::OKX.format_symbol("BTC"), "BTC-USDT-SWAP");
    }

    #[test]
    fn test_format_symbol_gate() {
        assert_eq!(Exchange::Gate.format_symbol("BTC"), "BTC_USDT");
    }

    #[test]
    fn test_format_symbol_mexc() {
        assert_eq!(Exchange::MEXC.format_symbol("BTC"), "BTC_USDT");
    }

    #[test]
    fn test_format_symbol_bitget() {
        assert_eq!(Exchange::Bitget.format_symbol("BTC"), "BTCUSDT");
    }

    #[test]
    fn test_format_symbol_defaults() {
        // Exchanges that fall through to default {BASE}USDT
        assert_eq!(Exchange::Kraken.format_symbol("BTC"), "BTCUSDT");
        assert_eq!(Exchange::Kucoin.format_symbol("BTC"), "BTCUSDT");
        assert_eq!(Exchange::Deribit.format_symbol("BTC"), "BTCUSDT");
        assert_eq!(Exchange::Phemex.format_symbol("BTC"), "BTCUSDT");
        assert_eq!(Exchange::XT.format_symbol("BTC"), "BTCUSDT");
        assert_eq!(Exchange::HTX.format_symbol("BTC"), "BTCUSDT");
        assert_eq!(Exchange::Bitfinex.format_symbol("BTC"), "BTCUSDT");
        assert_eq!(Exchange::Bitmart.format_symbol("BTC"), "BTCUSDT");
        assert_eq!(Exchange::Coinex.format_symbol("BTC"), "BTCUSDT");
        assert_eq!(Exchange::Lbank.format_symbol("BTC"), "BTCUSDT");
        assert_eq!(Exchange::Weex.format_symbol("BTC"), "BTCUSDT");
        assert_eq!(Exchange::Ourbit.format_symbol("BTC"), "BTCUSDT");
    }

    // ── extract_base_symbol ─────────────────────────────────────────

    #[test]
    fn test_extract_base_btcusdt() {
        assert_eq!(extract_base_symbol("BTCUSDT"), "BTC");
    }

    #[test]
    fn test_extract_base_dash_usdt() {
        assert_eq!(extract_base_symbol("BTC-USDT"), "BTC");
    }

    #[test]
    fn test_extract_base_underscore_usdt() {
        assert_eq!(extract_base_symbol("BTC_USDT"), "BTC");
    }

    #[test]
    fn test_extract_base_okx_swap() {
        assert_eq!(extract_base_symbol("BTC-USDT-SWAP"), "BTC");
    }

    #[test]
    fn test_extract_base_usdc() {
        assert_eq!(extract_base_symbol("BTCUSDC"), "BTC");
        assert_eq!(extract_base_symbol("BTC-USDC"), "BTC");
    }

    #[test]
    fn test_extract_base_plain() {
        assert_eq!(extract_base_symbol("BTC"), "BTC");
        assert_eq!(extract_base_symbol("SOL"), "SOL");
    }

    #[test]
    fn test_extract_base_lowercase() {
        // Case-insensitive suffix stripping preserves original prefix case
        assert_eq!(extract_base_symbol("btcusdt"), "btc");
        assert_eq!(extract_base_symbol("sol-usdt"), "sol");
    }

    // ── format_symbol_for_exchange ──────────────────────────────────

    #[test]
    fn test_format_symbol_for_exchange_bybit() {
        assert_eq!(format_symbol_for_exchange("SOLUSDT", "bybit"), "SOLUSDT");
        assert_eq!(format_symbol_for_exchange("SOL-USDT", "bybit"), "SOLUSDT");
        assert_eq!(format_symbol_for_exchange("SOL", "bybit"), "SOLUSDT");
    }

    #[test]
    fn test_format_symbol_for_exchange_bingx() {
        assert_eq!(format_symbol_for_exchange("SOLUSDT", "bingx"), "SOL-USDT");
        assert_eq!(format_symbol_for_exchange("SOL", "bingx"), "SOL-USDT");
    }

    #[test]
    fn test_format_symbol_for_exchange_hyperliquid() {
        assert_eq!(format_symbol_for_exchange("SOLUSDT", "hyperliquid"), "SOL");
    }

    #[test]
    fn test_format_symbol_for_exchange_okx() {
        assert_eq!(format_symbol_for_exchange("SOL", "okx"), "SOL-USDT-SWAP");
    }

    #[test]
    fn test_format_symbol_for_exchange_gate() {
        assert_eq!(format_symbol_for_exchange("SOL", "gate"), "SOL_USDT");
    }

    #[test]
    fn test_format_symbol_for_exchange_unknown() {
        // Unknown exchange defaults to {BASE}USDT
        assert_eq!(format_symbol_for_exchange("SOL", "foobar"), "SOLUSDT");
    }

    #[test]
    fn test_roundtrip_extract_format() {
        let exchanges = [
            "bybit",
            "binance",
            "bitget",
            "bingx",
            "hyperliquid",
            "okx",
            "gate",
            "mexc",
        ];
        for ex in &exchanges {
            let formatted = format_symbol_for_exchange("BTC", ex);
            let base = extract_base_symbol(&formatted);
            assert_eq!(base, "BTC", "roundtrip failed for exchange {}", ex);
        }
    }
}
