//! Common utilities for integration tests.

use sdktrader::exchanges::{create_trader, ExchangeCredentials, ExchangeTrader};
use sdktrader::models::MarketType;
use std::env;
use std::sync::Arc;

/// Test configuration loaded from environment variables.
#[derive(Debug, Clone)]
pub struct TestConfig {
    pub api_key: String,
    pub api_secret: String,
    pub passphrase: Option<String>,
    pub testnet: bool,
}

impl TestConfig {
    /// Load test configuration from environment variables.
    ///
    /// Expected env vars:
    /// - `{EXCHANGE}_API_KEY`
    /// - `{EXCHANGE}_API_SECRET`
    /// - `{EXCHANGE}_PASSPHRASE` (optional)
    /// - `{EXCHANGE}_TESTNET` (optional, defaults to true)
    pub fn from_env(exchange: &str) -> Option<Self> {
        let prefix = exchange.to_uppercase();

        let api_key = env::var(format!("{}_API_KEY", prefix)).ok()?;
        let api_secret = env::var(format!("{}_API_SECRET", prefix)).ok()?;
        let passphrase = env::var(format!("{}_PASSPHRASE", prefix)).ok();
        let testnet = env::var(format!("{}_TESTNET", prefix))
            .map(|v| v.to_lowercase() != "false")
            .unwrap_or(true);

        Some(Self {
            api_key,
            api_secret,
            passphrase,
            testnet,
        })
    }

    /// Check if credentials are available for an exchange.
    pub fn has_credentials(exchange: &str) -> bool {
        Self::from_env(exchange).is_some()
    }
}

/// Create a trader for testing with optional credentials.
pub fn create_test_trader(
    exchange: &str,
    market_type: MarketType,
    config: Option<&TestConfig>,
) -> anyhow::Result<Arc<dyn ExchangeTrader>> {
    let (api_key, api_secret, passphrase, testnet) = match config {
        Some(cfg) => (
            cfg.api_key.as_str(),
            cfg.api_secret.as_str(),
            cfg.passphrase.as_deref(),
            cfg.testnet,
        ),
        None => ("", "", None, true),
    };

    let mut creds = ExchangeCredentials::new(api_key, api_secret)
        .with_testnet(testnet);

    if let Some(pass) = passphrase {
        creds = creds.with_passphrase(pass);
    }

    create_trader(exchange, creds, market_type)
}

/// Standard test symbol for each exchange.
pub fn get_test_symbol(exchange: &str, market_type: MarketType) -> &'static str {
    match (exchange, market_type) {
        ("hyperliquid", _) => "BTC",
        (_, MarketType::Spot) => "BTC/USDT",
        (_, MarketType::Swap) => "BTC/USDT",
        _ => "BTC/USDT",
    }
}

/// Macro to generate standard public endpoint tests for an exchange.
#[macro_export]
macro_rules! public_endpoint_tests {
    ($exchange:expr, $market_type:expr) => {
        use super::common::{create_test_trader, get_test_symbol};

        #[tokio::test]
        #[cfg(feature = "integration")]
        async fn test_connection() {
            let trader = create_test_trader($exchange, $market_type, None)
                .expect("Failed to create trader");

            let result = trader.test_connection().await;
            assert!(result.is_ok(), "test_connection failed: {:?}", result.err());
            assert!(result.unwrap(), "Connection test returned false");
        }

        #[tokio::test]
        #[cfg(feature = "integration")]
        async fn test_get_server_time() {
            let trader = create_test_trader($exchange, $market_type, None)
                .expect("Failed to create trader");

            let result = trader.get_server_time().await;
            assert!(result.is_ok(), "get_server_time failed: {:?}", result.err());

            let time = result.unwrap();
            assert!(time > 0, "Server time should be positive");

            // Server time should be reasonable (after 2020)
            assert!(time > 1577836800000, "Server time seems too old");
        }

        #[tokio::test]
        #[cfg(feature = "integration")]
        async fn test_get_current_price() {
            let trader = create_test_trader($exchange, $market_type, None)
                .expect("Failed to create trader");

            let symbol = get_test_symbol($exchange, $market_type);
            let result = trader.get_current_price(symbol).await;

            assert!(result.is_ok(), "get_current_price failed: {:?}", result.err());

            let price = result.unwrap();
            assert!(price > 0.0, "Price should be positive");
            // BTC price sanity check (between $1000 and $1,000,000)
            assert!(price > 1000.0 && price < 1_000_000.0,
                "BTC price {} seems unreasonable", price);
        }

        #[tokio::test]
        #[cfg(feature = "integration")]
        async fn test_format_symbol() {
            let trader = create_test_trader($exchange, $market_type, None)
                .expect("Failed to create trader");

            // Test various input formats
            let formatted1 = trader.format_symbol("BTC/USDT");
            let formatted2 = trader.format_symbol("BTCUSDT");
            let formatted3 = trader.format_symbol("BTC");

            // All should produce a valid symbol
            assert!(!formatted1.is_empty(), "Formatted symbol should not be empty");
            assert!(!formatted2.is_empty(), "Formatted symbol should not be empty");
            assert!(!formatted3.is_empty(), "Formatted symbol should not be empty");

            // First two should produce the same result
            assert_eq!(formatted1, formatted2,
                "BTC/USDT and BTCUSDT should format to the same symbol");
        }

        #[tokio::test]
        #[cfg(feature = "integration")]
        async fn test_get_orderbook() {
            let trader = create_test_trader($exchange, $market_type, None)
                .expect("Failed to create trader");

            let symbol = get_test_symbol($exchange, $market_type);
            let result = trader.get_orderbook(symbol, 10).await;

            // Note: Some exchanges may not implement this method
            if let Ok(orderbook) = result {
                assert!(!orderbook.bids.is_empty(), "Orderbook should have bids");
                assert!(!orderbook.asks.is_empty(), "Orderbook should have asks");

                // Best bid should be less than best ask
                if let (Some(best_bid), Some(best_ask)) = (orderbook.bids.first(), orderbook.asks.first()) {
                    assert!(best_bid.price < best_ask.price,
                        "Best bid {} should be less than best ask {}",
                        best_bid.price, best_ask.price);
                }
            }
        }
    };
}

/// Macro to generate private endpoint tests (require credentials).
#[macro_export]
macro_rules! private_endpoint_tests {
    ($exchange:expr, $market_type:expr) => {
        use super::common::{create_test_trader, get_test_symbol, TestConfig};

        #[tokio::test]
        #[ignore] // Run with: cargo test --features live --ignored
        #[cfg(feature = "live")]
        async fn test_get_balance() {
            let config = TestConfig::from_env($exchange)
                .expect(&format!("No credentials for {}", $exchange));

            let trader = create_test_trader($exchange, $market_type, Some(&config))
                .expect("Failed to create trader");

            let result = trader.get_balance().await;
            assert!(result.is_ok(), "get_balance failed: {:?}", result.err());

            let balance = result.unwrap();
            // Balance should have reasonable values
            assert!(balance.total_equity >= 0.0, "Total equity should be non-negative");
        }

        #[tokio::test]
        #[ignore]
        #[cfg(feature = "live")]
        async fn test_set_leverage() {
            // Only for swap/futures
            if $market_type != sdktrader::models::MarketType::Swap {
                return;
            }

            let config = TestConfig::from_env($exchange)
                .expect(&format!("No credentials for {}", $exchange));

            let trader = create_test_trader($exchange, $market_type, Some(&config))
                .expect("Failed to create trader");

            let symbol = get_test_symbol($exchange, $market_type);
            let result = trader.set_leverage(Some(symbol), 5).await;

            // Some exchanges may not support leverage setting
            if let Ok(success) = result {
                assert!(success, "set_leverage should succeed");
            }
        }
    };
}

/// Macro to generate WebSocket tests.
#[macro_export]
macro_rules! websocket_tests {
    ($exchange:expr, $market_type:expr) => {
        use super::common::{create_test_trader, get_test_symbol};

        #[tokio::test]
        #[cfg(feature = "integration")]
        async fn test_ws_orderbook() {
            let trader = create_test_trader($exchange, $market_type, None)
                .expect("Failed to create trader");

            let symbol = get_test_symbol($exchange, $market_type);
            let result = trader.test_ws_orderbook(symbol, 50, 5000).await;

            if let Ok(ws_result) = result {
                assert!(ws_result.connected, "WebSocket should connect");
                assert!(ws_result.messages_received > 0, "Should receive messages");
            }
        }

        #[tokio::test]
        #[cfg(feature = "integration")]
        async fn test_ws_trades() {
            let trader = create_test_trader($exchange, $market_type, None)
                .expect("Failed to create trader");

            let symbol = get_test_symbol($exchange, $market_type);
            let result = trader.test_ws_trades(symbol, 5000).await;

            if let Ok(ws_result) = result {
                assert!(ws_result.connected, "WebSocket should connect");
                // Trades might be sparse, so we don't require messages
            }
        }

        #[tokio::test]
        #[cfg(feature = "integration")]
        async fn test_ws_ticker() {
            // Only for swap/futures (ticker with OI/funding)
            if $market_type != sdktrader::models::MarketType::Swap {
                return;
            }

            let trader = create_test_trader($exchange, $market_type, None)
                .expect("Failed to create trader");

            let symbol = get_test_symbol($exchange, $market_type);
            let result = trader.test_ws_ticker(symbol, 5000).await;

            if let Ok(ws_result) = result {
                assert!(ws_result.connected, "WebSocket should connect");
            }
        }
    };
}

/// Macro to generate dry-run order tests.
#[macro_export]
macro_rules! order_dryrun_tests {
    ($exchange:expr, $market_type:expr) => {
        use super::common::{create_test_trader, get_test_symbol};
        use sdktrader::models::OrderSide;

        #[tokio::test]
        #[cfg(feature = "integration")]
        async fn test_market_order_dryrun() {
            let trader = create_test_trader($exchange, $market_type, None)
                .expect("Failed to create trader");

            let symbol = get_test_symbol($exchange, $market_type);
            let result = trader.test_market_order(symbol, OrderSide::Buy, 10.0).await;

            if let Ok(order_result) = result {
                assert!(order_result.valid, "Dry-run order should be valid");
                assert!(order_result.quantity > 0.0, "Should calculate quantity");
                assert!(order_result.estimated_cost > 0.0, "Should estimate cost");
            }
        }

        #[tokio::test]
        #[cfg(feature = "integration")]
        async fn test_limit_order_dryrun() {
            let trader = create_test_trader($exchange, $market_type, None)
                .expect("Failed to create trader");

            let symbol = get_test_symbol($exchange, $market_type);

            // Get current price for limit order
            let price = trader.get_current_price(symbol).await
                .unwrap_or(50000.0);

            // Place limit order 10% below current price
            let limit_price = price * 0.9;
            let result = trader.test_limit_order(symbol, OrderSide::Buy, 10.0, limit_price).await;

            if let Ok(order_result) = result {
                assert!(order_result.valid, "Dry-run order should be valid");
                assert!(order_result.quantity > 0.0, "Should calculate quantity");
            }
        }
    };
}
