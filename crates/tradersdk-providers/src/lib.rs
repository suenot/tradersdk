//! Exchange provider registry for tradersdk.
//!
//! Single import point for all exchange adapters. Provides:
//! - `ExchangeCredentials` for API authentication
//! - `create_trader()` factory function
//! - `supported_exchanges()` to list available exchanges
//! - Feature-gated re-exports of individual exchange crates

// Re-export core crate and its public API
pub use tradersdk_core;
pub use tradersdk_core::ExchangeError;
pub use tradersdk_core::ExchangeTrader;

// Feature-gated exchange crate re-exports
#[cfg(feature = "bybit")]
pub use tradersdk_bybit as bybit;

#[cfg(feature = "bingx")]
pub use tradersdk_bingx as bingx;

#[cfg(feature = "binance")]
pub use tradersdk_binance as binance;

#[cfg(feature = "gate")]
pub use tradersdk_gate as gate;

#[cfg(feature = "okx")]
pub use tradersdk_okx as okx;

#[cfg(feature = "bitget")]
pub use tradersdk_bitget as bitget;

#[cfg(feature = "mexc")]
pub use tradersdk_mexc as mexc;

#[cfg(feature = "hyperliquid")]
pub use tradersdk_hyperliquid as hyperliquid;

#[cfg(feature = "kraken")]
pub use tradersdk_kraken as kraken;

#[cfg(feature = "kucoin")]
pub use tradersdk_kucoin as kucoin;

#[cfg(feature = "deribit")]
pub use tradersdk_deribit as deribit;

#[cfg(feature = "phemex")]
pub use tradersdk_phemex as phemex;

#[cfg(feature = "xt")]
pub use tradersdk_xt as xt;

#[cfg(feature = "htx")]
pub use tradersdk_htx as htx;

#[cfg(feature = "bitfinex")]
pub use tradersdk_bitfinex as bitfinex;

#[cfg(feature = "bitmart")]
pub use tradersdk_bitmart as bitmart;

#[cfg(feature = "coinex")]
pub use tradersdk_coinex as coinex;

#[cfg(feature = "lbank")]
pub use tradersdk_lbank as lbank;

#[cfg(feature = "weex")]
pub use tradersdk_weex as weex;

#[cfg(feature = "ourbit")]
pub use tradersdk_ourbit as ourbit;

use anyhow::Result;
use std::sync::Arc;
use tradersdk_core::models::MarketType;

/// Exchange credentials for API authentication.
#[derive(Debug, Clone)]
pub struct ExchangeCredentials {
    /// API key.
    pub api_key: String,
    /// API secret.
    pub api_secret: String,
    /// Passphrase (required for some exchanges like OKX, Bitget).
    pub passphrase: Option<String>,
    /// Whether to use testnet.
    pub testnet: bool,
}

impl ExchangeCredentials {
    /// Create new credentials.
    pub fn new(api_key: &str, api_secret: &str) -> Self {
        Self {
            api_key: api_key.to_string(),
            api_secret: api_secret.to_string(),
            passphrase: None,
            testnet: false,
        }
    }

    /// Set passphrase.
    pub fn with_passphrase(mut self, passphrase: &str) -> Self {
        self.passphrase = Some(passphrase.to_string());
        self
    }

    /// Enable testnet.
    pub fn with_testnet(mut self, testnet: bool) -> Self {
        self.testnet = testnet;
        self
    }
}

/// Create a trader instance for the specified exchange.
pub fn create_trader(
    exchange: &str,
    credentials: ExchangeCredentials,
    market_type: MarketType,
) -> Result<Arc<dyn ExchangeTrader>> {
    match exchange.to_lowercase().as_str() {
        #[cfg(feature = "bybit")]
        "bybit" => Ok(Arc::new(bybit::BybitTrader::new(
            &credentials.api_key,
            &credentials.api_secret,
            credentials.testnet,
            market_type,
        ))),

        #[cfg(feature = "bingx")]
        "bingx" => Ok(Arc::new(bingx::BingxTrader::new(
            &credentials.api_key,
            &credentials.api_secret,
            credentials.testnet,
            market_type,
        ))),

        #[cfg(feature = "binance")]
        "binance" => Ok(Arc::new(binance::BinanceTrader::new(
            &credentials.api_key,
            &credentials.api_secret,
            credentials.testnet,
            market_type,
        ))),

        #[cfg(feature = "gate")]
        "gate" => Ok(Arc::new(gate::GateTrader::new(
            &credentials.api_key,
            &credentials.api_secret,
            credentials.testnet,
            market_type,
        ))),

        #[cfg(feature = "okx")]
        "okx" => Ok(Arc::new(okx::OkxTrader::new(
            &credentials.api_key,
            &credentials.api_secret,
            credentials.passphrase.as_deref(),
            credentials.testnet,
            market_type,
        ))),

        #[cfg(feature = "bitget")]
        "bitget" => Ok(Arc::new(bitget::BitgetTrader::new(
            &credentials.api_key,
            &credentials.api_secret,
            credentials.passphrase.as_deref(),
            credentials.testnet,
            market_type,
        ))),

        #[cfg(feature = "mexc")]
        "mexc" => Ok(Arc::new(mexc::MexcTrader::new(
            &credentials.api_key,
            &credentials.api_secret,
            credentials.testnet,
            market_type,
        ))),

        #[cfg(feature = "hyperliquid")]
        "hyperliquid" => Ok(Arc::new(hyperliquid::HyperliquidTrader::new(
            &credentials.api_key,
            &credentials.api_secret,
            credentials.testnet,
            market_type,
        ))),

        #[cfg(feature = "kraken")]
        "kraken" => Ok(Arc::new(kraken::KrakenTrader::new(
            &credentials.api_key,
            &credentials.api_secret,
            credentials.testnet,
            market_type,
        ))),

        #[cfg(feature = "kucoin")]
        "kucoin" => Ok(Arc::new(kucoin::KucoinTrader::new(
            &credentials.api_key,
            &credentials.api_secret,
            credentials.passphrase.as_deref(),
            credentials.testnet,
            market_type,
        ))),

        #[cfg(feature = "deribit")]
        "deribit" => Ok(Arc::new(deribit::DeribitTrader::new(
            &credentials.api_key,
            &credentials.api_secret,
            credentials.testnet,
            market_type,
        ))),

        #[cfg(feature = "phemex")]
        "phemex" => Ok(Arc::new(phemex::PhemexTrader::new(
            &credentials.api_key,
            &credentials.api_secret,
            credentials.testnet,
            market_type,
        ))),

        #[cfg(feature = "xt")]
        "xt" => Ok(Arc::new(xt::XtTrader::new(
            &credentials.api_key,
            &credentials.api_secret,
            credentials.testnet,
            market_type,
        ))),

        #[cfg(feature = "htx")]
        "htx" => Ok(Arc::new(htx::HtxTrader::new(
            &credentials.api_key,
            &credentials.api_secret,
            credentials.testnet,
            market_type,
        ))),

        #[cfg(feature = "bitfinex")]
        "bitfinex" => Ok(Arc::new(bitfinex::BitfinexTrader::new(
            &credentials.api_key,
            &credentials.api_secret,
            credentials.testnet,
            market_type,
        ))),

        #[cfg(feature = "bitmart")]
        "bitmart" => Ok(Arc::new(bitmart::BitmartTrader::new(
            &credentials.api_key,
            &credentials.api_secret,
            credentials.passphrase.as_deref(),
            credentials.testnet,
            market_type,
        ))),

        #[cfg(feature = "coinex")]
        "coinex" => Ok(Arc::new(coinex::CoinexTrader::new(
            &credentials.api_key,
            &credentials.api_secret,
            credentials.testnet,
            market_type,
        ))),

        #[cfg(feature = "lbank")]
        "lbank" => Ok(Arc::new(lbank::LbankTrader::new(
            &credentials.api_key,
            &credentials.api_secret,
            credentials.testnet,
            market_type,
        ))),

        #[cfg(feature = "weex")]
        "weex" => Ok(Arc::new(weex::WeexTrader::new(
            &credentials.api_key,
            &credentials.api_secret,
            credentials.passphrase.as_deref(),
            credentials.testnet,
            market_type,
        ))),

        #[cfg(feature = "ourbit")]
        "ourbit" => Ok(Arc::new(ourbit::OurbitTrader::new(
            &credentials.api_key,
            &credentials.api_secret,
            credentials.testnet,
            market_type,
        ))),

        _ => Err(anyhow::anyhow!("Unsupported exchange: {}", exchange)),
    }
}

/// Get list of supported exchanges.
#[allow(clippy::vec_init_then_push)] // each push is behind its own #[cfg]
pub fn supported_exchanges() -> Vec<&'static str> {
    let mut exchanges = Vec::new();

    #[cfg(feature = "bybit")]
    exchanges.push("bybit");

    #[cfg(feature = "bingx")]
    exchanges.push("bingx");

    #[cfg(feature = "binance")]
    exchanges.push("binance");

    #[cfg(feature = "gate")]
    exchanges.push("gate");

    #[cfg(feature = "okx")]
    exchanges.push("okx");

    #[cfg(feature = "bitget")]
    exchanges.push("bitget");

    #[cfg(feature = "mexc")]
    exchanges.push("mexc");

    #[cfg(feature = "hyperliquid")]
    exchanges.push("hyperliquid");

    #[cfg(feature = "kraken")]
    exchanges.push("kraken");

    #[cfg(feature = "kucoin")]
    exchanges.push("kucoin");

    #[cfg(feature = "deribit")]
    exchanges.push("deribit");

    #[cfg(feature = "phemex")]
    exchanges.push("phemex");

    #[cfg(feature = "xt")]
    exchanges.push("xt");

    #[cfg(feature = "htx")]
    exchanges.push("htx");

    #[cfg(feature = "bitfinex")]
    exchanges.push("bitfinex");

    #[cfg(feature = "bitmart")]
    exchanges.push("bitmart");

    #[cfg(feature = "coinex")]
    exchanges.push("coinex");

    #[cfg(feature = "lbank")]
    exchanges.push("lbank");

    #[cfg(feature = "weex")]
    exchanges.push("weex");

    #[cfg(feature = "ourbit")]
    exchanges.push("ourbit");

    exchanges
}
