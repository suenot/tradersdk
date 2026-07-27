//! API request handlers.

use std::sync::Arc;
use std::time::Instant;

use axum::{
    extract::{Json, State},
    http::StatusCode,
    response::IntoResponse,
};
use tokio::sync::RwLock;

use sdktrader_providers::{create_trader, supported_exchanges, ExchangeCredentials, ExchangeTrader};
use sdktrader_core::models::MarketType;

use crate::config::Config;
use crate::types::*;

/// Adapter methods available for testing.
pub const ADAPTER_METHODS: &[&str] = &[
    "test_connection",
    "get_server_time",
    "get_current_price",
    "get_balance",
    "set_leverage",
    "format_symbol",
    "get_orderbook",
    "test_ws_orderbook",
    "test_ws_trades",
    "test_market_order",
    "test_limit_order",
    "get_ticker",
    "test_ws_ticker",
    "get_funding_rate_history",
    "get_open_interest_history",
];

/// Application state.
pub struct AppState {
    pub config: RwLock<Config>,
    pub config_path: String,
}

impl AppState {
    pub fn new(config: Config, config_path: String) -> Self {
        Self {
            config: RwLock::new(config),
            config_path,
        }
    }
}

/// Health check handler.
pub async fn health_check() -> impl IntoResponse {
    Json(ApiResponse::success(HealthResponse {
        status: "healthy".to_string(),
        version: env!("CARGO_PKG_VERSION").to_string(),
        backend: "sdktrader-rs".to_string(),
    }))
}

/// List supported exchanges.
pub async fn list_exchanges() -> impl IntoResponse {
    Json(ApiResponse::success(supported_exchanges()))
}

/// List adapter methods.
pub async fn list_adapter_methods() -> impl IntoResponse {
    Json(ApiResponse::success(ADAPTER_METHODS))
}

/// List bots available for testing.
pub async fn list_bots_for_testing(
    State(state): State<Arc<AppState>>,
) -> impl IntoResponse {
    let config = state.config.read().await;

    let bots: Vec<BotTestInfo> = config
        .bots
        .iter()
        .map(|bot| BotTestInfo {
            name: bot.name.clone(),
            exchange: bot.exchange.clone(),
            market_type: bot.market_type.clone(),
            testnet: bot.testnet,
            has_api_key: !bot.api_key.is_empty(),
            has_api_secret: !bot.api_secret.is_empty(),
            has_passphrase: !bot.api_passphrase.is_empty(),
        })
        .collect();

    Json(ApiResponse::success(bots))
}

/// Test an exchange adapter.
pub async fn test_exchange(
    State(state): State<Arc<AppState>>,
    Json(request): Json<TestExchangeRequest>,
) -> impl IntoResponse {
    let config = state.config.read().await;

    // Find bot configuration
    let bot = match config.get_bot(&request.bot_name) {
        Some(bot) => bot.clone(),
        None => {
            return (
                StatusCode::NOT_FOUND,
                Json(ApiResponse::<TestExchangeResponse>::error(format!(
                    "Bot '{}' not found in configuration",
                    request.bot_name
                ))),
            );
        }
    };

    // Create credentials
    let credentials = ExchangeCredentials {
        api_key: bot.api_key.clone(),
        api_secret: bot.api_secret.clone(),
        passphrase: if bot.api_passphrase.is_empty() {
            None
        } else {
            Some(bot.api_passphrase.clone())
        },
        testnet: bot.testnet,
    };

    let has_credentials = !bot.api_key.is_empty() && !bot.api_secret.is_empty();

    // Parse market type
    let market_type = match bot.market_type.to_lowercase().as_str() {
        "spot" => MarketType::Spot,
        _ => MarketType::Swap,
    };

    // Create trader
    let trader = match create_trader(&bot.exchange, credentials, market_type) {
        Ok(t) => t,
        Err(e) => {
            return (
                StatusCode::BAD_REQUEST,
                Json(ApiResponse::<TestExchangeResponse>::error(format!(
                    "Failed to create trader: {}",
                    e
                ))),
            );
        }
    };

    // Determine which methods to test
    let methods_to_test: Vec<&str> = if request.methods.is_empty() {
        ADAPTER_METHODS.to_vec()
    } else {
        request
            .methods
            .iter()
            .filter(|m| ADAPTER_METHODS.contains(&m.as_str()))
            .map(|s| s.as_str())
            .collect()
    };

    // Run tests
    let mut tests = Vec::new();
    let symbol = &request.symbol;

    for method in methods_to_test {
        let result = run_test_method(&*trader, method, symbol, has_credentials, market_type).await;
        tests.push(result);
    }

    let overall_success = tests.iter().all(|t| t.success);

    let response = TestExchangeResponse {
        bot_name: bot.name,
        exchange: bot.exchange,
        testnet: bot.testnet,
        market_type: bot.market_type,
        has_credentials,
        tests,
        overall_success,
    };

    (StatusCode::OK, Json(ApiResponse::success(response)))
}

/// Run a single test method.
async fn run_test_method(
    trader: &dyn ExchangeTrader,
    method: &str,
    symbol: &str,
    has_credentials: bool,
    market_type: MarketType,
) -> TestResult {
    let start = Instant::now();

    let (success, result, error) = match method {
        "test_connection" => {
            match trader.test_connection().await {
                Ok(connected) => (true, Some(serde_json::json!({ "connected": connected })), None),
                Err(e) => (false, None, Some(e.to_string())),
            }
        }
        "get_server_time" => {
            match trader.get_server_time().await {
                Ok(time) => (true, Some(serde_json::json!({ "server_time": time })), None),
                Err(e) => (false, None, Some(e.to_string())),
            }
        }
        "get_current_price" => {
            match trader.get_current_price(symbol).await {
                Ok(price) => (true, Some(serde_json::json!({ "price": price, "symbol": symbol })), None),
                Err(e) => (false, None, Some(e.to_string())),
            }
        }
        "get_balance" => {
            if !has_credentials {
                (false, None, Some("Requires API credentials".to_string()))
            } else {
                match trader.get_balance().await {
                    Ok(balance) => (true, Some(serde_json::to_value(&balance).unwrap_or_default()), None),
                    Err(e) => (false, None, Some(e.to_string())),
                }
            }
        }
        "set_leverage" => {
            if !has_credentials {
                (false, None, Some("Requires API credentials".to_string()))
            } else if market_type == MarketType::Spot {
                (false, None, Some("Not available for spot market".to_string()))
            } else {
                match trader.set_leverage(Some(symbol), 10).await {
                    Ok(success) => (true, Some(serde_json::json!({ "leverage_set": success, "leverage": 10 })), None),
                    Err(e) => (false, None, Some(e.to_string())),
                }
            }
        }
        "format_symbol" => {
            let formatted = trader.format_symbol(symbol);
            (true, Some(serde_json::json!({ "input": symbol, "formatted": formatted })), None)
        }
        "get_orderbook" => {
            match trader.get_orderbook(symbol, 10).await {
                Ok(orderbook) => (true, Some(serde_json::to_value(&orderbook).unwrap_or_default()), None),
                Err(e) => (false, None, Some(e.to_string())),
            }
        }
        "test_ws_orderbook" => {
            match trader.test_ws_orderbook(symbol, 10, 5000).await {
                Ok(result) => (true, Some(serde_json::to_value(&result).unwrap_or_default()), None),
                Err(e) => (false, None, Some(e.to_string())),
            }
        }
        "test_ws_trades" => {
            match trader.test_ws_trades(symbol, 5000).await {
                Ok(result) => (true, Some(serde_json::to_value(&result).unwrap_or_default()), None),
                Err(e) => (false, None, Some(e.to_string())),
            }
        }
        "test_market_order" => {
            if !has_credentials {
                (false, None, Some("Requires API credentials".to_string()))
            } else {
                match trader.test_market_order(symbol, sdktrader_core::models::OrderSide::Buy, 100.0).await {
                    Ok(result) => (true, Some(serde_json::to_value(&result).unwrap_or_default()), None),
                    Err(e) => (false, None, Some(e.to_string())),
                }
            }
        }
        "test_limit_order" => {
            if !has_credentials {
                (false, None, Some("Requires API credentials".to_string()))
            } else {
                // Get current price for limit order
                let price = trader.get_current_price(symbol).await.unwrap_or(50000.0) * 0.95;
                match trader.test_limit_order(symbol, sdktrader_core::models::OrderSide::Buy, 100.0, price).await {
                    Ok(result) => (true, Some(serde_json::to_value(&result).unwrap_or_default()), None),
                    Err(e) => (false, None, Some(e.to_string())),
                }
            }
        }
        "get_ticker" => {
            match trader.get_ticker(symbol).await {
                Ok(ticker) => (true, Some(serde_json::to_value(&ticker).unwrap_or_default()), None),
                Err(e) => (false, None, Some(e.to_string())),
            }
        }
        "test_ws_ticker" => {
            if market_type == MarketType::Spot {
                (false, None, Some("Only available for perpetual/swap markets".to_string()))
            } else {
                match trader.test_ws_ticker(symbol, 5000).await {
                    Ok(result) => (true, Some(serde_json::to_value(&result).unwrap_or_default()), None),
                    Err(e) => (false, None, Some(e.to_string())),
                }
            }
        }
        "get_funding_rate_history" => {
            if market_type == MarketType::Spot {
                (false, None, Some("Only available for perpetual/swap markets".to_string()))
            } else {
                match trader.get_funding_rate_history(symbol, 10).await {
                    Ok(history) => (true, Some(serde_json::to_value(&history).unwrap_or_default()), None),
                    Err(e) => (false, None, Some(e.to_string())),
                }
            }
        }
        "get_open_interest_history" => {
            if market_type == MarketType::Spot {
                (false, None, Some("Only available for perpetual/swap markets".to_string()))
            } else {
                match trader.get_open_interest_history(symbol, "5m", 10).await {
                    Ok(history) => (true, Some(serde_json::to_value(&history).unwrap_or_default()), None),
                    Err(e) => (false, None, Some(e.to_string())),
                }
            }
        }
        _ => (false, None, Some(format!("Unknown method: {}", method))),
    };

    let duration_ms = start.elapsed().as_millis() as u64;

    TestResult {
        name: method.to_string(),
        success,
        duration_ms,
        result,
        error,
    }
}
