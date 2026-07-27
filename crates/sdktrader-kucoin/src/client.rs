//! KuCoin exchange trader implementation.
//!
//! KuCoin API documentation:
//! - Spot REST: https://api.kucoin.com
//! - Futures REST: https://api-futures.kucoin.com
//! - WebSocket: Dynamic endpoint obtained via POST /api/v1/bullet-public (or /api/v1/bullet-private for auth)
//!
//! Signature: HMAC-SHA256 with base64 encoding, requires passphrase.

use anyhow::{Context, Result};
use async_trait::async_trait;
use base64::{engine::general_purpose, Engine as _};
use futures_util::{SinkExt, StreamExt};
use reqwest::Client;
use sdktrader_core::models::{
    AccountBalance, Balance, ExchangeCapabilities, MarketType, OrderInfo, OrderResult, OrderSide,
    OrderStatus, OrderType, OrderUpdateEvent, Orderbook, OrderbookLevel, TickerInfo, TimeInForce,
    WsTestResult,
};
use sdktrader_core::utils::timestamp_ms;
use sdktrader_core::ExchangeTrader;
use serde::Deserialize;
use tokio::sync::broadcast;
use tokio::task::JoinHandle;
use tokio_tungstenite::{connect_async, tungstenite::protocol::Message};
use tracing::{debug, error, info, warn};

// Spot API
const SPOT_API_URL: &str = "https://api.kucoin.com";
// Futures API
const FUTURES_API_URL: &str = "https://api-futures.kucoin.com";

/// Maximum reconnect backoff delay in seconds.
const MAX_RECONNECT_DELAY_SECS: u64 = 30;

/// Initial reconnect delay in seconds.
const INITIAL_RECONNECT_DELAY_SECS: u64 = 1;

/// KuCoin exchange trader.
pub struct KucoinTrader {
    api_key: String,
    api_secret: String,
    passphrase: String,
    testnet: bool,
    market_type: MarketType,
    client: Client,
}

impl KucoinTrader {
    /// Create a new KuCoin trader.
    ///
    /// # Arguments
    /// * `api_key` - KuCoin API key
    /// * `api_secret` - KuCoin API secret
    /// * `passphrase` - KuCoin API passphrase (required for all authenticated calls)
    /// * `testnet` - Whether to use sandbox/testnet environment
    /// * `market_type` - Market type (Spot or Swap/Futures)
    pub fn new(
        api_key: &str,
        api_secret: &str,
        passphrase: Option<&str>,
        testnet: bool,
        market_type: MarketType,
    ) -> Self {
        Self {
            api_key: api_key.to_string(),
            api_secret: api_secret.to_string(),
            passphrase: passphrase.unwrap_or("").to_string(),
            testnet,
            market_type,
            client: Client::new(),
        }
    }

    /// Get base URL for API calls.
    fn base_url(&self) -> &str {
        match self.market_type {
            MarketType::Spot => SPOT_API_URL,
            MarketType::Swap => FUTURES_API_URL,
        }
    }

    /// Generate HMAC-SHA256 signature for KuCoin API.
    ///
    /// KuCoin signature: BASE64(HMAC-SHA256(timestamp + method + endpoint + body, secret))
    fn sign(&self, timestamp: &str, method: &str, endpoint: &str, body: &str) -> String {
        use hmac::{Hmac, Mac};
        use sha2::Sha256;

        type HmacSha256 = Hmac<Sha256>;

        let message = format!("{}{}{}{}", timestamp, method, endpoint, body);

        let mut mac = HmacSha256::new_from_slice(self.api_secret.as_bytes())
            .expect("HMAC can take key of any size");
        mac.update(message.as_bytes());

        let result = mac.finalize();
        general_purpose::STANDARD.encode(result.into_bytes())
    }

    /// Generate encrypted passphrase for API v2.
    ///
    /// KuCoin v2 requires: BASE64(HMAC-SHA256(passphrase, secret))
    fn encrypt_passphrase(&self) -> String {
        use hmac::{Hmac, Mac};
        use sha2::Sha256;

        type HmacSha256 = Hmac<Sha256>;

        let mut mac = HmacSha256::new_from_slice(self.api_secret.as_bytes())
            .expect("HMAC can take key of any size");
        mac.update(self.passphrase.as_bytes());

        let result = mac.finalize();
        general_purpose::STANDARD.encode(result.into_bytes())
    }

    /// Make an authenticated GET request.
    async fn auth_get(&self, endpoint: &str) -> Result<serde_json::Value> {
        let timestamp = timestamp_ms().to_string();
        let signature = self.sign(&timestamp, "GET", endpoint, "");
        let encrypted_passphrase = self.encrypt_passphrase();

        let url = format!("{}{}", self.base_url(), endpoint);

        let response = self
            .client
            .get(&url)
            .header("KC-API-KEY", &self.api_key)
            .header("KC-API-SIGN", &signature)
            .header("KC-API-TIMESTAMP", &timestamp)
            .header("KC-API-PASSPHRASE", &encrypted_passphrase)
            .header("KC-API-KEY-VERSION", "2")
            .header("Content-Type", "application/json")
            .send()
            .await
            .context("Failed to send GET request")?;

        let json: serde_json::Value = response.json().await?;

        // Check for KuCoin API errors
        if let Some(code) = json.get("code").and_then(|c| c.as_str()) {
            if code != "200000" {
                let msg = json
                    .get("msg")
                    .and_then(|m| m.as_str())
                    .unwrap_or("Unknown error");
                anyhow::bail!("KuCoin API error {}: {}", code, msg);
            }
        }

        Ok(json)
    }

    /// Make an authenticated POST request.
    async fn auth_post(
        &self,
        endpoint: &str,
        body: &serde_json::Value,
    ) -> Result<serde_json::Value> {
        let timestamp = timestamp_ms().to_string();
        let body_str = serde_json::to_string(body)?;
        let signature = self.sign(&timestamp, "POST", endpoint, &body_str);
        let encrypted_passphrase = self.encrypt_passphrase();

        let url = format!("{}{}", self.base_url(), endpoint);

        let response = self
            .client
            .post(&url)
            .header("KC-API-KEY", &self.api_key)
            .header("KC-API-SIGN", &signature)
            .header("KC-API-TIMESTAMP", &timestamp)
            .header("KC-API-PASSPHRASE", &encrypted_passphrase)
            .header("KC-API-KEY-VERSION", "2")
            .header("Content-Type", "application/json")
            .body(body_str)
            .send()
            .await
            .context("Failed to send POST request")?;

        let json: serde_json::Value = response.json().await?;

        if let Some(code) = json.get("code").and_then(|c| c.as_str()) {
            if code != "200000" {
                let msg = json
                    .get("msg")
                    .and_then(|m| m.as_str())
                    .unwrap_or("Unknown error");
                anyhow::bail!("KuCoin API error {}: {}", code, msg);
            }
        }

        Ok(json)
    }

    /// Make an authenticated DELETE request.
    async fn auth_delete(&self, endpoint: &str) -> Result<serde_json::Value> {
        let timestamp = timestamp_ms().to_string();
        let signature = self.sign(&timestamp, "DELETE", endpoint, "");
        let encrypted_passphrase = self.encrypt_passphrase();

        let url = format!("{}{}", self.base_url(), endpoint);

        let response = self
            .client
            .delete(&url)
            .header("KC-API-KEY", &self.api_key)
            .header("KC-API-SIGN", &signature)
            .header("KC-API-TIMESTAMP", &timestamp)
            .header("KC-API-PASSPHRASE", &encrypted_passphrase)
            .header("KC-API-KEY-VERSION", "2")
            .header("Content-Type", "application/json")
            .send()
            .await
            .context("Failed to send DELETE request")?;

        let json: serde_json::Value = response.json().await?;

        if let Some(code) = json.get("code").and_then(|c| c.as_str()) {
            if code != "200000" {
                let msg = json
                    .get("msg")
                    .and_then(|m| m.as_str())
                    .unwrap_or("Unknown error");
                anyhow::bail!("KuCoin API error {}: {}", code, msg);
            }
        }

        Ok(json)
    }

    /// Get public WebSocket token and endpoint.
    async fn get_public_ws_token(&self) -> Result<WsTokenResponse> {
        let endpoint = match self.market_type {
            MarketType::Spot => "/api/v1/bullet-public",
            MarketType::Swap => "/api/v1/bullet-public",
        };

        let url = format!("{}{}", self.base_url(), endpoint);

        let response = self
            .client
            .post(&url)
            .header("Content-Type", "application/json")
            .send()
            .await
            .context("Failed to get public WS token")?;

        let json: serde_json::Value = response.json().await?;

        if let Some(code) = json.get("code").and_then(|c| c.as_str()) {
            if code != "200000" {
                let msg = json
                    .get("msg")
                    .and_then(|m| m.as_str())
                    .unwrap_or("Unknown error");
                anyhow::bail!("Failed to get WS token: {} - {}", code, msg);
            }
        }

        let data = json
            .get("data")
            .ok_or_else(|| anyhow::anyhow!("No data in response"))?;

        let token = data
            .get("token")
            .and_then(|t| t.as_str())
            .ok_or_else(|| anyhow::anyhow!("No token in response"))?
            .to_string();

        let servers = data
            .get("instanceServers")
            .and_then(|s| s.as_array())
            .ok_or_else(|| anyhow::anyhow!("No servers in response"))?;

        let server = servers
            .first()
            .ok_or_else(|| anyhow::anyhow!("Empty servers list"))?;

        let endpoint = server
            .get("endpoint")
            .and_then(|e| e.as_str())
            .ok_or_else(|| anyhow::anyhow!("No endpoint in server"))?
            .to_string();

        let ping_interval = server
            .get("pingInterval")
            .and_then(|p| p.as_u64())
            .unwrap_or(30000);

        Ok(WsTokenResponse {
            token,
            endpoint,
            ping_interval,
        })
    }

    /// Get private WebSocket token and endpoint.
    async fn get_private_ws_token(&self) -> Result<WsTokenResponse> {
        let endpoint = match self.market_type {
            MarketType::Spot => "/api/v1/bullet-private",
            MarketType::Swap => "/api/v1/bullet-private",
        };

        let json = self.auth_post(endpoint, &serde_json::json!({})).await?;

        let data = json
            .get("data")
            .ok_or_else(|| anyhow::anyhow!("No data in response"))?;

        let token = data
            .get("token")
            .and_then(|t| t.as_str())
            .ok_or_else(|| anyhow::anyhow!("No token in response"))?
            .to_string();

        let servers = data
            .get("instanceServers")
            .and_then(|s| s.as_array())
            .ok_or_else(|| anyhow::anyhow!("No servers in response"))?;

        let server = servers
            .first()
            .ok_or_else(|| anyhow::anyhow!("Empty servers list"))?;

        let ws_endpoint = server
            .get("endpoint")
            .and_then(|e| e.as_str())
            .ok_or_else(|| anyhow::anyhow!("No endpoint in server"))?
            .to_string();

        let ping_interval = server
            .get("pingInterval")
            .and_then(|p| p.as_u64())
            .unwrap_or(30000);

        Ok(WsTokenResponse {
            token,
            endpoint: ws_endpoint,
            ping_interval,
        })
    }

    /// Build WebSocket URL with token.
    fn build_ws_url(token_response: &WsTokenResponse) -> String {
        let connect_id = uuid::Uuid::new_v4().to_string();
        format!(
            "{}?token={}&connectId={}",
            token_response.endpoint, token_response.token, connect_id
        )
    }

    /// Place order via REST API.
    async fn place_order_rest(
        &self,
        symbol: &str,
        side: OrderSide,
        order_type: OrderType,
        quantity: f64,
        price: Option<f64>,
        reduce_only: bool,
    ) -> Result<OrderResult> {
        let formatted_symbol = self.format_symbol(symbol);
        let client_oid = uuid::Uuid::new_v4().to_string();

        match self.market_type {
            MarketType::Spot => {
                // Spot order
                let mut order_body = serde_json::json!({
                    "clientOid": client_oid,
                    "symbol": formatted_symbol,
                    "side": match side {
                        OrderSide::Buy => "buy",
                        OrderSide::Sell => "sell",
                    },
                    "type": match order_type {
                        OrderType::Market => "market",
                        OrderType::Limit => "limit",
                        _ => "market",
                    },
                });

                // For market orders on spot, use "size" for base currency amount
                // or "funds" for quote currency amount
                match order_type {
                    OrderType::Market => {
                        // Use size (base currency amount)
                        order_body["size"] = serde_json::json!(format!("{:.8}", quantity));
                    }
                    OrderType::Limit => {
                        order_body["size"] = serde_json::json!(format!("{:.8}", quantity));
                        if let Some(p) = price {
                            order_body["price"] = serde_json::json!(format!("{:.8}", p));
                        }
                    }
                    _ => {
                        order_body["size"] = serde_json::json!(format!("{:.8}", quantity));
                    }
                }

                let response = self.auth_post("/api/v1/orders", &order_body).await?;

                let order_id = response
                    .get("data")
                    .and_then(|d| d.get("orderId"))
                    .and_then(|o| o.as_str())
                    .unwrap_or("")
                    .to_string();

                if order_id.is_empty() {
                    return Ok(OrderResult::failure(
                        formatted_symbol,
                        side,
                        "No order ID in response".to_string(),
                    ));
                }

                info!("KuCoin spot order placed: {}", order_id);

                Ok(OrderResult {
                    success: true,
                    order_id: Some(order_id),
                    symbol: formatted_symbol,
                    side,
                    order_type,
                    quantity: Some(quantity),
                    entry_price: price,
                    position_size: Some(quantity),
                    position_usdt: price.map(|p| quantity * p),
                    error: None,
                    raw_response: None,
                })
            }
            MarketType::Swap => {
                // Futures order
                let mut order_body = serde_json::json!({
                    "clientOid": client_oid,
                    "symbol": formatted_symbol,
                    "side": match side {
                        OrderSide::Buy => "buy",
                        OrderSide::Sell => "sell",
                    },
                    "type": match order_type {
                        OrderType::Market => "market",
                        OrderType::Limit => "limit",
                        _ => "market",
                    },
                    "size": quantity as i64, // Futures uses integer lot size
                    "leverage": "1", // Default leverage
                });

                if reduce_only {
                    order_body["reduceOnly"] = serde_json::json!(true);
                }

                if let (OrderType::Limit, Some(p)) = (order_type, price) {
                    order_body["price"] = serde_json::json!(format!("{:.8}", p));
                }

                let response = self.auth_post("/api/v1/orders", &order_body).await?;

                let order_id = response
                    .get("data")
                    .and_then(|d| d.get("orderId"))
                    .and_then(|o| o.as_str())
                    .unwrap_or("")
                    .to_string();

                if order_id.is_empty() {
                    return Ok(OrderResult::failure(
                        formatted_symbol,
                        side,
                        "No order ID in response".to_string(),
                    ));
                }

                info!("KuCoin futures order placed: {}", order_id);

                Ok(OrderResult {
                    success: true,
                    order_id: Some(order_id),
                    symbol: formatted_symbol,
                    side,
                    order_type,
                    quantity: Some(quantity),
                    entry_price: price,
                    position_size: Some(quantity),
                    position_usdt: price.map(|p| quantity * p),
                    error: None,
                    raw_response: None,
                })
            }
        }
    }
}

/// WebSocket token response.
#[derive(Debug, Clone)]
struct WsTokenResponse {
    token: String,
    endpoint: String,
    ping_interval: u64,
}

/// WebSocket message structure.
#[derive(Debug, Deserialize)]
struct WsMessage {
    #[serde(rename = "type")]
    msg_type: Option<String>,
    topic: Option<String>,
    subject: Option<String>,
    data: Option<serde_json::Value>,
}

/// Calculate next reconnect delay with exponential backoff (capped).
fn next_reconnect_delay(current_secs: u64) -> u64 {
    (current_secs * 2).min(MAX_RECONNECT_DELAY_SECS)
}

/// Parse order status from KuCoin string.
fn parse_order_status_str(s: &str) -> OrderStatus {
    match s.to_lowercase().as_str() {
        "open" | "active" | "new" => OrderStatus::New,
        "match" | "partial" => OrderStatus::PartiallyFilled,
        "done" | "filled" => OrderStatus::Filled,
        "cancelled" | "canceled" => OrderStatus::Cancelled,
        "failure" | "failed" => OrderStatus::Rejected,
        _ => {
            warn!("Unknown KuCoin order status: '{}'", s);
            OrderStatus::New
        }
    }
}

/// Log an order update event with exchange/local timestamp delta.
fn log_order_update(event: &OrderUpdateEvent) {
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

#[async_trait]
impl ExchangeTrader for KucoinTrader {
    fn exchange_name(&self) -> &str {
        "kucoin"
    }

    fn market_type(&self) -> MarketType {
        self.market_type
    }

    fn capabilities(&self) -> ExchangeCapabilities {
        sdktrader_core::exchange_capabilities! {
            spot: Supported,
            swap: Supported,
            create_market_order: Supported,
            create_limit_order: Supported,
            cancel_order: Supported,
            get_order_status: Supported,
            set_leverage: Supported,
            close_position: Supported,
            get_current_price: Supported,
            get_ticker: Supported,
            get_orderbook: Supported,
            get_balance: Supported,
            get_server_time: Supported,
            test_connection: Supported,
            execute_fast_entry: Supported,
            format_symbol: Supported,
            ws_orderbook: Supported,
            ws_trades: Supported,
            ws_ticker: Supported,
            ws_orders: Supported,
        }
    }

    async fn test_connection(&self) -> Result<bool> {
        let endpoint = match self.market_type {
            MarketType::Spot => "/api/v1/timestamp",
            MarketType::Swap => "/api/v1/timestamp",
        };

        let url = format!("{}{}", self.base_url(), endpoint);
        let response = self.client.get(&url).send().await?;

        Ok(response.status().is_success())
    }

    async fn get_server_time(&self) -> Result<i64> {
        let endpoint = match self.market_type {
            MarketType::Spot => "/api/v1/timestamp",
            MarketType::Swap => "/api/v1/timestamp",
        };

        let url = format!("{}{}", self.base_url(), endpoint);
        let response: serde_json::Value = self.client.get(&url).send().await?.json().await?;

        Ok(response["data"].as_i64().unwrap_or_else(timestamp_ms))
    }

    async fn get_balance(&self) -> Result<AccountBalance> {
        match self.market_type {
            MarketType::Spot => {
                let response = self.auth_get("/api/v1/accounts?type=trade").await?;

                let empty_vec = vec![];
                let accounts = response
                    .get("data")
                    .and_then(|d| d.as_array())
                    .unwrap_or(&empty_vec);

                let mut total_usdt = 0.0;
                let mut available_usdt = 0.0;
                let mut balances = Vec::new();

                for account in accounts {
                    let currency = account
                        .get("currency")
                        .and_then(|c| c.as_str())
                        .unwrap_or("");
                    let available = account
                        .get("available")
                        .and_then(|a| a.as_str())
                        .and_then(|s| s.parse::<f64>().ok())
                        .unwrap_or(0.0);
                    let holds = account
                        .get("holds")
                        .and_then(|h| h.as_str())
                        .and_then(|s| s.parse::<f64>().ok())
                        .unwrap_or(0.0);

                    if currency == "USDT" {
                        total_usdt = available + holds;
                        available_usdt = available;
                    }

                    balances.push(Balance {
                        asset: currency.to_string(),
                        free: available,
                        locked: holds,
                    });
                }

                Ok(AccountBalance {
                    total_balance: total_usdt,
                    available_balance: available_usdt,
                    currency: "USDT".to_string(),
                    margin_balance: None,
                    used_margin: None,
                    unrealized_pnl: None,
                    realized_pnl: None,
                    balances,
                })
            }
            MarketType::Swap => {
                let response = self
                    .auth_get("/api/v1/account-overview?currency=USDT")
                    .await?;

                let data = response.get("data").unwrap_or(&serde_json::Value::Null);

                let account_equity = data
                    .get("accountEquity")
                    .and_then(|e| e.as_f64())
                    .unwrap_or(0.0);
                let available_balance = data
                    .get("availableBalance")
                    .and_then(|a| a.as_f64())
                    .unwrap_or(0.0);
                let margin_balance = data
                    .get("marginBalance")
                    .and_then(|m| m.as_f64())
                    .unwrap_or(0.0);
                let unrealized_pnl = data
                    .get("unrealisedPNL")
                    .and_then(|u| u.as_f64())
                    .unwrap_or(0.0);

                Ok(AccountBalance {
                    total_balance: account_equity,
                    available_balance,
                    currency: "USDT".to_string(),
                    margin_balance: Some(margin_balance),
                    used_margin: Some(margin_balance - available_balance),
                    unrealized_pnl: Some(unrealized_pnl),
                    realized_pnl: None,
                    balances: vec![],
                })
            }
        }
    }

    async fn get_current_price(&self, symbol: &str) -> Result<f64> {
        let formatted_symbol = self.format_symbol(symbol);

        match self.market_type {
            MarketType::Spot => {
                let url = format!(
                    "{}/api/v1/market/orderbook/level1?symbol={}",
                    self.base_url(),
                    formatted_symbol
                );
                let response: serde_json::Value =
                    self.client.get(&url).send().await?.json().await?;

                let price = response
                    .get("data")
                    .and_then(|d| d.get("price"))
                    .and_then(|p| p.as_str())
                    .and_then(|s| s.parse().ok())
                    .unwrap_or(0.0);

                Ok(price)
            }
            MarketType::Swap => {
                let url = format!(
                    "{}/api/v1/ticker?symbol={}",
                    self.base_url(),
                    formatted_symbol
                );
                let response: serde_json::Value =
                    self.client.get(&url).send().await?.json().await?;

                let price = response
                    .get("data")
                    .and_then(|d| d.get("price"))
                    .and_then(|p| p.as_str())
                    .and_then(|s| s.parse().ok())
                    .unwrap_or(0.0);

                Ok(price)
            }
        }
    }

    async fn get_ticker(&self, symbol: &str) -> Result<TickerInfo> {
        let formatted_symbol = self.format_symbol(symbol);

        match self.market_type {
            MarketType::Spot => {
                let url = format!(
                    "{}/api/v1/market/stats?symbol={}",
                    self.base_url(),
                    formatted_symbol
                );
                let response: serde_json::Value =
                    self.client.get(&url).send().await?.json().await?;

                let data = response
                    .get("data")
                    .ok_or_else(|| anyhow::anyhow!("No ticker data"))?;

                Ok(TickerInfo {
                    symbol: formatted_symbol,
                    last_price: data["last"]
                        .as_str()
                        .and_then(|s| s.parse().ok())
                        .unwrap_or(0.0),
                    bid_price: data["buy"].as_str().and_then(|s| s.parse().ok()),
                    ask_price: data["sell"].as_str().and_then(|s| s.parse().ok()),
                    high_24h: data["high"].as_str().and_then(|s| s.parse().ok()),
                    low_24h: data["low"].as_str().and_then(|s| s.parse().ok()),
                    volume_24h: data["vol"].as_str().and_then(|s| s.parse().ok()),
                    turnover_24h: data["volValue"].as_str().and_then(|s| s.parse().ok()),
                    change_24h: data["changeRate"].as_str().and_then(|s| s.parse().ok()),
                    open_interest: None,
                    open_interest_value: None,
                    funding_rate: None,
                    next_funding_time: None,
                    mark_price: None,
                    index_price: None,
                    timestamp: data["time"].as_i64().unwrap_or_else(timestamp_ms),
                })
            }
            MarketType::Swap => {
                let url = format!(
                    "{}/api/v1/ticker?symbol={}",
                    self.base_url(),
                    formatted_symbol
                );
                let response: serde_json::Value =
                    self.client.get(&url).send().await?.json().await?;

                let data = response
                    .get("data")
                    .ok_or_else(|| anyhow::anyhow!("No ticker data"))?;

                // Get funding rate
                let funding_url = format!(
                    "{}/api/v1/funding-rate/{}/current",
                    self.base_url(),
                    formatted_symbol
                );
                let funding_response: serde_json::Value = self
                    .client
                    .get(&funding_url)
                    .send()
                    .await?
                    .json()
                    .await
                    .unwrap_or(serde_json::json!({}));

                let funding_rate = funding_response
                    .get("data")
                    .and_then(|d| d.get("value"))
                    .and_then(|v| v.as_f64());

                Ok(TickerInfo {
                    symbol: formatted_symbol,
                    last_price: data["price"]
                        .as_str()
                        .and_then(|s| s.parse().ok())
                        .unwrap_or(0.0),
                    bid_price: data["bestBidPrice"].as_str().and_then(|s| s.parse().ok()),
                    ask_price: data["bestAskPrice"].as_str().and_then(|s| s.parse().ok()),
                    high_24h: data["highPrice"].as_str().and_then(|s| s.parse().ok()),
                    low_24h: data["lowPrice"].as_str().and_then(|s| s.parse().ok()),
                    volume_24h: data["volumeOf24h"].as_f64(),
                    turnover_24h: data["turnoverOf24h"].as_f64(),
                    change_24h: data["priceChgPct"].as_f64(),
                    open_interest: data["openInterest"].as_str().and_then(|s| s.parse().ok()),
                    open_interest_value: None,
                    funding_rate,
                    next_funding_time: None,
                    mark_price: data["markPrice"].as_f64(),
                    index_price: data["indexPrice"].as_f64(),
                    timestamp: data["ts"].as_i64().unwrap_or_else(timestamp_ms),
                })
            }
        }
    }

    async fn get_orderbook(&self, symbol: &str, depth: u32) -> Result<Orderbook> {
        let formatted_symbol = self.format_symbol(symbol);

        match self.market_type {
            MarketType::Spot => {
                // KuCoin spot orderbook levels: level2_20, level2_100
                let level = if depth <= 20 { "20" } else { "100" };
                let url = format!(
                    "{}/api/v1/market/orderbook/level2_{}?symbol={}",
                    self.base_url(),
                    level,
                    formatted_symbol
                );
                let response: serde_json::Value =
                    self.client.get(&url).send().await?.json().await?;

                let data = response
                    .get("data")
                    .ok_or_else(|| anyhow::anyhow!("No orderbook data"))?;

                let bids: Vec<OrderbookLevel> = data["bids"]
                    .as_array()
                    .map(|arr| {
                        arr.iter()
                            .filter_map(|item| {
                                let price = item[0].as_str()?.parse().ok()?;
                                let quantity = item[1].as_str()?.parse().ok()?;
                                Some(OrderbookLevel { price, quantity })
                            })
                            .collect()
                    })
                    .unwrap_or_default();

                let asks: Vec<OrderbookLevel> = data["asks"]
                    .as_array()
                    .map(|arr| {
                        arr.iter()
                            .filter_map(|item| {
                                let price = item[0].as_str()?.parse().ok()?;
                                let quantity = item[1].as_str()?.parse().ok()?;
                                Some(OrderbookLevel { price, quantity })
                            })
                            .collect()
                    })
                    .unwrap_or_default();

                Ok(Orderbook {
                    symbol: formatted_symbol,
                    bids,
                    asks,
                    timestamp: data["time"].as_i64().unwrap_or_else(timestamp_ms),
                })
            }
            MarketType::Swap => {
                // KuCoin futures orderbook
                let url = format!(
                    "{}/api/v1/level2/depth{}?symbol={}",
                    self.base_url(),
                    depth.min(100),
                    formatted_symbol
                );
                let response: serde_json::Value =
                    self.client.get(&url).send().await?.json().await?;

                let data = response
                    .get("data")
                    .ok_or_else(|| anyhow::anyhow!("No orderbook data"))?;

                let bids: Vec<OrderbookLevel> = data["bids"]
                    .as_array()
                    .map(|arr| {
                        arr.iter()
                            .filter_map(|item| {
                                let price = item[0].as_f64()?;
                                let quantity = item[1].as_f64()?;
                                Some(OrderbookLevel { price, quantity })
                            })
                            .collect()
                    })
                    .unwrap_or_default();

                let asks: Vec<OrderbookLevel> = data["asks"]
                    .as_array()
                    .map(|arr| {
                        arr.iter()
                            .filter_map(|item| {
                                let price = item[0].as_f64()?;
                                let quantity = item[1].as_f64()?;
                                Some(OrderbookLevel { price, quantity })
                            })
                            .collect()
                    })
                    .unwrap_or_default();

                Ok(Orderbook {
                    symbol: formatted_symbol,
                    bids,
                    asks,
                    timestamp: data["ts"].as_i64().unwrap_or_else(timestamp_ms),
                })
            }
        }
    }

    async fn set_leverage(&self, symbol: Option<&str>, leverage: u32) -> Result<bool> {
        if self.market_type != MarketType::Swap {
            return Ok(true); // Spot doesn't need leverage
        }

        let symbol = symbol.ok_or_else(|| anyhow::anyhow!("Symbol required for leverage"))?;
        let formatted_symbol = self.format_symbol(symbol);

        // KuCoin futures: POST /api/v1/position/risk-limit-level/change
        // Note: KuCoin uses risk limit levels, not direct leverage setting
        // We'll need to get the position and adjust
        let body = serde_json::json!({
            "symbol": formatted_symbol,
            "level": leverage.min(100) // KuCoin max leverage varies by symbol
        });

        match self
            .auth_post("/api/v1/position/risk-limit-level/change", &body)
            .await
        {
            Ok(_) => {
                info!(
                    "KuCoin leverage set to {} for {}",
                    leverage, formatted_symbol
                );
                Ok(true)
            }
            Err(e) => {
                warn!("Failed to set leverage: {}", e);
                Ok(false)
            }
        }
    }

    async fn execute_fast_entry(
        &self,
        symbol: &str,
        direction: &str,
        usdt_amount: f64,
        leverage: u32,
        _tp_percent: Option<f64>,
        _sl_percent: Option<f64>,
        current_price: Option<f64>,
    ) -> Result<OrderResult> {
        let formatted_symbol = self.format_symbol(symbol);

        // Get current price if not provided
        let price = match current_price {
            Some(p) => p,
            None => self.get_current_price(&formatted_symbol).await?,
        };

        // Calculate quantity
        let quantity = if self.market_type == MarketType::Swap {
            usdt_amount / price * leverage as f64
        } else {
            usdt_amount / price
        };

        // Set leverage for futures
        if self.market_type == MarketType::Swap {
            if let Err(e) = self.set_leverage(Some(&formatted_symbol), leverage).await {
                error!("Failed to set leverage: {}", e);
            }
        }

        // Determine side
        let side = if direction.to_lowercase() == "long" {
            OrderSide::Buy
        } else {
            OrderSide::Sell
        };

        // Place order
        self.place_market_order(&formatted_symbol, side, quantity)
            .await
    }

    async fn place_market_order(
        &self,
        symbol: &str,
        side: OrderSide,
        quantity: f64,
    ) -> Result<OrderResult> {
        self.place_order_rest(symbol, side, OrderType::Market, quantity, None, false)
            .await
    }

    async fn place_limit_order(
        &self,
        symbol: &str,
        side: OrderSide,
        quantity: f64,
        price: f64,
        reduce_only: bool,
    ) -> Result<OrderResult> {
        self.place_order_rest(
            symbol,
            side,
            OrderType::Limit,
            quantity,
            Some(price),
            reduce_only,
        )
        .await
    }

    async fn cancel_order(&self, symbol: &str, order_id: &str) -> Result<bool> {
        let formatted_symbol = self.format_symbol(symbol);
        let _ = formatted_symbol; // Used for logging

        let endpoint = format!("/api/v1/orders/{}", order_id);
        match self.auth_delete(&endpoint).await {
            Ok(response) => {
                let cancelled_id = response
                    .get("data")
                    .and_then(|d| d.get("cancelledOrderIds"))
                    .and_then(|c| c.as_array())
                    .and_then(|arr| arr.first())
                    .and_then(|id| id.as_str());

                if cancelled_id.is_some() {
                    info!("KuCoin order cancelled: {}", order_id);
                    Ok(true)
                } else {
                    warn!("Order cancellation response unclear for {}", order_id);
                    Ok(false)
                }
            }
            Err(e) => {
                error!("Failed to cancel order {}: {}", order_id, e);
                Ok(false)
            }
        }
    }

    async fn get_order_status(&self, _symbol: &str, order_id: &str) -> Result<OrderInfo> {
        let endpoint = format!("/api/v1/orders/{}", order_id);
        let response = self.auth_get(&endpoint).await?;

        let data = response
            .get("data")
            .ok_or_else(|| anyhow::anyhow!("No order data"))?;

        let symbol = data["symbol"].as_str().unwrap_or("").to_string();
        let side_str = data["side"].as_str().unwrap_or("buy");
        let type_str = data["type"].as_str().unwrap_or("market");

        let size = data["size"]
            .as_str()
            .and_then(|s| s.parse::<f64>().ok())
            .unwrap_or(0.0);
        let filled_size = data["dealSize"]
            .as_str()
            .and_then(|s| s.parse::<f64>().ok())
            .unwrap_or(0.0);
        let price = data["price"]
            .as_str()
            .and_then(|s| s.parse::<f64>().ok())
            .unwrap_or(0.0);
        let deal_funds = data["dealFunds"]
            .as_str()
            .and_then(|s| s.parse::<f64>().ok())
            .unwrap_or(0.0);

        let avg_price = if filled_size > 0.0 {
            Some(deal_funds / filled_size)
        } else {
            None
        };

        let is_active = data["isActive"].as_bool().unwrap_or(false);
        let cancel_exist = data["cancelExist"].as_bool().unwrap_or(false);

        let status = if cancel_exist {
            OrderStatus::Cancelled
        } else if filled_size >= size && size > 0.0 {
            OrderStatus::Filled
        } else if filled_size > 0.0 {
            OrderStatus::PartiallyFilled
        } else if is_active {
            OrderStatus::New
        } else {
            OrderStatus::Filled
        };

        Ok(OrderInfo {
            order_id: order_id.to_string(),
            client_order_id: data["clientOid"].as_str().map(|s| s.to_string()),
            symbol,
            status,
            side: if side_str == "buy" {
                OrderSide::Buy
            } else {
                OrderSide::Sell
            },
            order_type: if type_str == "market" {
                OrderType::Market
            } else {
                OrderType::Limit
            },
            price,
            quantity: size,
            filled_quantity: filled_size,
            remaining_quantity: size - filled_size,
            avg_price,
            timestamp: data["createdAt"].as_i64().unwrap_or(0),
            time_in_force: TimeInForce::GTC,
            reduce_only: false,
        })
    }

    async fn close_position(&self, symbol: &str) -> Result<OrderResult> {
        if self.market_type != MarketType::Swap {
            return Ok(OrderResult::failure(
                symbol.to_string(),
                OrderSide::Sell,
                "Close position only available for futures".to_string(),
            ));
        }

        let formatted_symbol = self.format_symbol(symbol);

        // Get current position
        let endpoint = format!("/api/v1/position?symbol={}", formatted_symbol);
        let response = self.auth_get(&endpoint).await?;

        let data = response.get("data").unwrap_or(&serde_json::Value::Null);
        let current_qty = data["currentQty"].as_i64().unwrap_or(0);

        if current_qty == 0 {
            return Ok(OrderResult::failure(
                formatted_symbol,
                OrderSide::Sell,
                "No position to close".to_string(),
            ));
        }

        // Determine close side
        let (side, quantity) = if current_qty > 0 {
            (OrderSide::Sell, current_qty.unsigned_abs() as f64)
        } else {
            (OrderSide::Buy, current_qty.unsigned_abs() as f64)
        };

        // Place close order with reduceOnly
        self.place_order_rest(
            &formatted_symbol,
            side,
            OrderType::Market,
            quantity,
            None,
            true,
        )
        .await
    }

    fn format_symbol(&self, base: &str) -> String {
        let s = base.to_uppercase();

        // Normalize: remove / and - to get clean symbol
        let normalized = s.replace("/", "").replace("-", "");

        match self.market_type {
            MarketType::Spot => {
                // Spot uses BTC-USDT format
                if normalized.ends_with("USDT") {
                    let base_asset = normalized.trim_end_matches("USDT");
                    format!("{}-USDT", base_asset)
                } else {
                    format!("{}-USDT", normalized)
                }
            }
            MarketType::Swap => {
                // Futures uses BTCUSDTM format (no dash, M suffix for perpetual)
                if normalized.ends_with("M") {
                    normalized
                } else if normalized.ends_with("USDT") {
                    format!("{}M", normalized)
                } else {
                    format!("{}USDTM", normalized)
                }
            }
        }
    }

    async fn test_ws_orderbook(
        &self,
        symbol: &str,
        depth: u32,
        timeout_ms: u64,
    ) -> Result<WsTestResult> {
        let formatted_symbol = self.format_symbol(symbol);

        // Get public WebSocket token
        let token_response = self.get_public_ws_token().await?;
        let ws_url = Self::build_ws_url(&token_response);

        let start = std::time::Instant::now();
        let (mut ws_stream, _) = connect_async(&ws_url)
            .await
            .context("Failed to connect to KuCoin WebSocket")?;

        let connect_time = start.elapsed().as_millis() as u64;

        // Wait for welcome message
        if let Some(Ok(Message::Text(_))) = ws_stream.next().await {
            debug!("Received KuCoin WS welcome message");
        }

        // Subscribe to orderbook
        let sub_id = uuid::Uuid::new_v4().to_string();
        let topic = match self.market_type {
            MarketType::Spot => format!("/market/level2:{}", formatted_symbol),
            MarketType::Swap => format!(
                "/contractMarket/level2Depth{}:{}",
                depth.min(50),
                formatted_symbol
            ),
        };

        let subscribe_msg = serde_json::json!({
            "id": sub_id,
            "type": "subscribe",
            "topic": topic,
            "privateChannel": false,
            "response": true
        });

        ws_stream
            .send(Message::Text(subscribe_msg.to_string()))
            .await?;

        let mut messages_received = 0u32;
        let mut data_sample = None;
        let timeout = tokio::time::Duration::from_millis(timeout_ms);

        loop {
            tokio::select! {
                msg = ws_stream.next() => {
                    match msg {
                        Some(Ok(Message::Text(text))) => {
                            messages_received += 1;
                            if let Ok(parsed) = serde_json::from_str::<WsMessage>(&text) {
                                if parsed.msg_type.as_deref() == Some("message") && parsed.data.is_some() {
                                    data_sample = parsed.data;
                                    break;
                                }
                            }
                        }
                        Some(Ok(Message::Ping(data))) => {
                            let _ = ws_stream.send(Message::Pong(data)).await;
                        }
                        Some(Ok(Message::Close(_))) | Some(Err(_)) | None => break,
                        _ => {}
                    }
                }
                _ = tokio::time::sleep(timeout) => {
                    break;
                }
            }
        }

        let _ = ws_stream.close(None).await;

        Ok(WsTestResult {
            connected: messages_received > 0,
            latency_ms: connect_time,
            messages_received,
            subscription_type: "orderbook".to_string(),
            data_sample,
        })
    }

    async fn test_ws_trades(&self, symbol: &str, timeout_ms: u64) -> Result<WsTestResult> {
        let formatted_symbol = self.format_symbol(symbol);

        let token_response = self.get_public_ws_token().await?;
        let ws_url = Self::build_ws_url(&token_response);

        let start = std::time::Instant::now();
        let (mut ws_stream, _) = connect_async(&ws_url)
            .await
            .context("Failed to connect to KuCoin WebSocket")?;

        let connect_time = start.elapsed().as_millis() as u64;

        // Wait for welcome message
        if let Some(Ok(Message::Text(_))) = ws_stream.next().await {
            debug!("Received KuCoin WS welcome message");
        }

        // Subscribe to trades
        let sub_id = uuid::Uuid::new_v4().to_string();
        let topic = match self.market_type {
            MarketType::Spot => format!("/market/match:{}", formatted_symbol),
            MarketType::Swap => format!("/contractMarket/execution:{}", formatted_symbol),
        };

        let subscribe_msg = serde_json::json!({
            "id": sub_id,
            "type": "subscribe",
            "topic": topic,
            "privateChannel": false,
            "response": true
        });

        ws_stream
            .send(Message::Text(subscribe_msg.to_string()))
            .await?;

        let mut messages_received = 0u32;
        let mut data_sample = None;
        let timeout = tokio::time::Duration::from_millis(timeout_ms);

        loop {
            tokio::select! {
                msg = ws_stream.next() => {
                    match msg {
                        Some(Ok(Message::Text(text))) => {
                            messages_received += 1;
                            if let Ok(parsed) = serde_json::from_str::<WsMessage>(&text) {
                                if parsed.msg_type.as_deref() == Some("message") && parsed.data.is_some() {
                                    data_sample = parsed.data;
                                    break;
                                }
                            }
                        }
                        Some(Ok(Message::Ping(data))) => {
                            let _ = ws_stream.send(Message::Pong(data)).await;
                        }
                        Some(Ok(Message::Close(_))) | Some(Err(_)) | None => break,
                        _ => {}
                    }
                }
                _ = tokio::time::sleep(timeout) => {
                    break;
                }
            }
        }

        let _ = ws_stream.close(None).await;

        Ok(WsTestResult {
            connected: messages_received > 0,
            latency_ms: connect_time,
            messages_received,
            subscription_type: "trades".to_string(),
            data_sample,
        })
    }

    async fn test_ws_ticker(&self, symbol: &str, timeout_ms: u64) -> Result<WsTestResult> {
        let formatted_symbol = self.format_symbol(symbol);

        let token_response = self.get_public_ws_token().await?;
        let ws_url = Self::build_ws_url(&token_response);

        let start = std::time::Instant::now();
        let (mut ws_stream, _) = connect_async(&ws_url)
            .await
            .context("Failed to connect to KuCoin WebSocket")?;

        let connect_time = start.elapsed().as_millis() as u64;

        // Wait for welcome message
        if let Some(Ok(Message::Text(_))) = ws_stream.next().await {
            debug!("Received KuCoin WS welcome message");
        }

        // Subscribe to ticker
        let sub_id = uuid::Uuid::new_v4().to_string();
        let topic = match self.market_type {
            MarketType::Spot => format!("/market/ticker:{}", formatted_symbol),
            MarketType::Swap => format!("/contractMarket/tickerV2:{}", formatted_symbol),
        };

        let subscribe_msg = serde_json::json!({
            "id": sub_id,
            "type": "subscribe",
            "topic": topic,
            "privateChannel": false,
            "response": true
        });

        ws_stream
            .send(Message::Text(subscribe_msg.to_string()))
            .await?;

        let mut messages_received = 0u32;
        let mut data_sample = None;
        let timeout = tokio::time::Duration::from_millis(timeout_ms);

        loop {
            tokio::select! {
                msg = ws_stream.next() => {
                    match msg {
                        Some(Ok(Message::Text(text))) => {
                            messages_received += 1;
                            if let Ok(parsed) = serde_json::from_str::<WsMessage>(&text) {
                                if parsed.msg_type.as_deref() == Some("message") && parsed.data.is_some() {
                                    data_sample = parsed.data;
                                    break;
                                }
                            }
                        }
                        Some(Ok(Message::Ping(data))) => {
                            let _ = ws_stream.send(Message::Pong(data)).await;
                        }
                        Some(Ok(Message::Close(_))) | Some(Err(_)) | None => break,
                        _ => {}
                    }
                }
                _ = tokio::time::sleep(timeout) => {
                    break;
                }
            }
        }

        let _ = ws_stream.close(None).await;

        Ok(WsTestResult {
            connected: messages_received > 0,
            latency_ms: connect_time,
            messages_received,
            subscription_type: "ticker".to_string(),
            data_sample,
        })
    }

    async fn subscribe_order_updates(
        &self,
        sender: broadcast::Sender<OrderUpdateEvent>,
    ) -> Result<JoinHandle<()>> {
        let api_key = self.api_key.clone();
        let api_secret = self.api_secret.clone();
        let passphrase = self.passphrase.clone();
        let market_type = self.market_type;
        let exchange_name = "kucoin".to_string();

        // Clone self for getting tokens
        let client = self.client.clone();
        let base_url = self.base_url().to_string();

        let handle = tokio::spawn(async move {
            let mut reconnect_delay = INITIAL_RECONNECT_DELAY_SECS;

            loop {
                info!("[{}] Getting private WS token...", exchange_name);

                // Get private WebSocket token
                let token_result = get_private_ws_token_inner(
                    &client,
                    &base_url,
                    &api_key,
                    &api_secret,
                    &passphrase,
                )
                .await;

                let token_response = match token_result {
                    Ok(t) => t,
                    Err(e) => {
                        error!("[{}] Failed to get private WS token: {}", exchange_name, e);
                        tokio::time::sleep(tokio::time::Duration::from_secs(reconnect_delay)).await;
                        reconnect_delay = next_reconnect_delay(reconnect_delay);
                        continue;
                    }
                };

                let connect_id = uuid::Uuid::new_v4().to_string();
                let ws_url = format!(
                    "{}?token={}&connectId={}",
                    token_response.endpoint, token_response.token, connect_id
                );

                info!("[{}] Connecting to private WS...", exchange_name);

                match connect_async(&ws_url).await {
                    Ok((mut ws_stream, _)) => {
                        // Wait for welcome message
                        if let Some(Ok(Message::Text(text))) = ws_stream.next().await {
                            debug!("[{}] WS welcome: {}", exchange_name, text);
                        }

                        // Subscribe to order updates
                        let sub_id = uuid::Uuid::new_v4().to_string();
                        let topic = match market_type {
                            MarketType::Spot => "/spotMarket/tradeOrders".to_string(),
                            MarketType::Swap => "/contractMarket/tradeOrders".to_string(),
                        };

                        let subscribe_msg = serde_json::json!({
                            "id": sub_id,
                            "type": "subscribe",
                            "topic": topic,
                            "privateChannel": true,
                            "response": true
                        });

                        if let Err(e) = ws_stream
                            .send(Message::Text(subscribe_msg.to_string()))
                            .await
                        {
                            error!("[{}] Failed to subscribe: {}", exchange_name, e);
                            tokio::time::sleep(tokio::time::Duration::from_secs(reconnect_delay))
                                .await;
                            reconnect_delay = next_reconnect_delay(reconnect_delay);
                            continue;
                        }

                        info!("[{}] Subscribed to private order updates", exchange_name);
                        reconnect_delay = INITIAL_RECONNECT_DELAY_SECS;

                        let ping_interval_ms = token_response.ping_interval;
                        let mut ping_interval = tokio::time::interval(
                            tokio::time::Duration::from_millis(ping_interval_ms / 2),
                        );

                        loop {
                            tokio::select! {
                                msg = ws_stream.next() => {
                                    match msg {
                                        Some(Ok(Message::Text(text))) => {
                                            if let Ok(msg_json) = serde_json::from_str::<serde_json::Value>(&text) {
                                                // Skip ack messages
                                                if msg_json.get("type").and_then(|t| t.as_str()) == Some("ack") {
                                                    continue;
                                                }

                                                // Process order updates
                                                if let Some(data) = msg_json.get("data") {
                                                    let subject = msg_json.get("subject").and_then(|s| s.as_str()).unwrap_or("");

                                                    // Handle different order update types
                                                    if subject.contains("order") || msg_json.get("topic").and_then(|t| t.as_str()).map(|t| t.contains("Order")).unwrap_or(false) {
                                                        let event = OrderUpdateEvent {
                                                            exchange: exchange_name.clone(),
                                                            order_id: data.get("orderId").and_then(|v| v.as_str()).unwrap_or("").to_string(),
                                                            client_order_id: data.get("clientOid").and_then(|v| v.as_str()).map(|s| s.to_string()),
                                                            symbol: data.get("symbol").and_then(|v| v.as_str()).unwrap_or("").to_string(),
                                                            status: parse_order_status_str(
                                                                data.get("status").and_then(|v| v.as_str())
                                                                    .or_else(|| data.get("type").and_then(|v| v.as_str()))
                                                                    .unwrap_or("new")
                                                            ),
                                                            side: if data.get("side").and_then(|v| v.as_str()) == Some("buy") {
                                                                OrderSide::Buy
                                                            } else {
                                                                OrderSide::Sell
                                                            },
                                                            order_type: if data.get("orderType").and_then(|v| v.as_str()) == Some("market") {
                                                                OrderType::Market
                                                            } else {
                                                                OrderType::Limit
                                                            },
                                                            price: data.get("price").and_then(|v| v.as_str()).and_then(|s| s.parse().ok()).unwrap_or(0.0),
                                                            quantity: data.get("size").and_then(|v| v.as_str()).and_then(|s| s.parse().ok()).unwrap_or(0.0),
                                                            filled_quantity: data.get("filledSize").and_then(|v| v.as_str()).and_then(|s| s.parse().ok())
                                                                .or_else(|| data.get("matchSize").and_then(|v| v.as_str()).and_then(|s| s.parse().ok()))
                                                                .unwrap_or(0.0),
                                                            avg_fill_price: data.get("matchPrice").and_then(|v| v.as_str()).and_then(|s| s.parse().ok()),
                                                            fee: None,
                                                            fee_currency: None,
                                                            exchange_timestamp: data.get("ts").and_then(|v| v.as_i64())
                                                                .or_else(|| data.get("orderTime").and_then(|v| v.as_i64()))
                                                                .unwrap_or(0),
                                                            received_at: timestamp_ms(),
                                                            raw: Some(data.clone()),
                                                        };
                                                        log_order_update(&event);
                                                        let _ = sender.send(event);
                                                    }
                                                }
                                            }
                                        }
                                        Some(Ok(Message::Ping(data))) => {
                                            let _ = ws_stream.send(Message::Pong(data)).await;
                                        }
                                        Some(Ok(Message::Close(_))) | Some(Err(_)) | None => {
                                            warn!("[{}] Private WS disconnected", exchange_name);
                                            break;
                                        }
                                        _ => {}
                                    }
                                }
                                _ = ping_interval.tick() => {
                                    let ping_msg = serde_json::json!({
                                        "id": uuid::Uuid::new_v4().to_string(),
                                        "type": "ping"
                                    });
                                    if let Err(e) = ws_stream.send(Message::Text(ping_msg.to_string())).await {
                                        error!("[{}] Ping failed: {}", exchange_name, e);
                                        break;
                                    }
                                }
                            }
                        }
                    }
                    Err(e) => {
                        error!("[{}] Failed to connect private WS: {}", exchange_name, e);
                    }
                }

                warn!(
                    "[{}] Reconnecting in {}s...",
                    exchange_name, reconnect_delay
                );
                tokio::time::sleep(tokio::time::Duration::from_secs(reconnect_delay)).await;
                reconnect_delay = next_reconnect_delay(reconnect_delay);
            }
        });

        Ok(handle)
    }
}

/// Helper function to get private WS token (used in spawned task).
async fn get_private_ws_token_inner(
    client: &Client,
    base_url: &str,
    api_key: &str,
    api_secret: &str,
    passphrase: &str,
) -> Result<WsTokenResponse> {
    use hmac::{Hmac, Mac};
    use sha2::Sha256;

    type HmacSha256 = Hmac<Sha256>;

    let endpoint = "/api/v1/bullet-private";
    let timestamp = timestamp_ms().to_string();
    let body = "{}";

    // Generate signature
    let message = format!("{}POST{}{}", timestamp, endpoint, body);
    let mut mac =
        HmacSha256::new_from_slice(api_secret.as_bytes()).expect("HMAC can take key of any size");
    mac.update(message.as_bytes());
    let signature = general_purpose::STANDARD.encode(mac.finalize().into_bytes());

    // Encrypt passphrase
    let mut mac2 =
        HmacSha256::new_from_slice(api_secret.as_bytes()).expect("HMAC can take key of any size");
    mac2.update(passphrase.as_bytes());
    let encrypted_passphrase = general_purpose::STANDARD.encode(mac2.finalize().into_bytes());

    let url = format!("{}{}", base_url, endpoint);

    let response = client
        .post(&url)
        .header("KC-API-KEY", api_key)
        .header("KC-API-SIGN", &signature)
        .header("KC-API-TIMESTAMP", &timestamp)
        .header("KC-API-PASSPHRASE", &encrypted_passphrase)
        .header("KC-API-KEY-VERSION", "2")
        .header("Content-Type", "application/json")
        .body(body)
        .send()
        .await
        .context("Failed to get private WS token")?;

    let json: serde_json::Value = response.json().await?;

    if let Some(code) = json.get("code").and_then(|c| c.as_str()) {
        if code != "200000" {
            let msg = json
                .get("msg")
                .and_then(|m| m.as_str())
                .unwrap_or("Unknown error");
            anyhow::bail!("Failed to get private WS token: {} - {}", code, msg);
        }
    }

    let data = json
        .get("data")
        .ok_or_else(|| anyhow::anyhow!("No data in response"))?;

    let token = data
        .get("token")
        .and_then(|t| t.as_str())
        .ok_or_else(|| anyhow::anyhow!("No token in response"))?
        .to_string();

    let servers = data
        .get("instanceServers")
        .and_then(|s| s.as_array())
        .ok_or_else(|| anyhow::anyhow!("No servers in response"))?;

    let server = servers
        .first()
        .ok_or_else(|| anyhow::anyhow!("Empty servers list"))?;

    let ws_endpoint = server
        .get("endpoint")
        .and_then(|e| e.as_str())
        .ok_or_else(|| anyhow::anyhow!("No endpoint in server"))?
        .to_string();

    let ping_interval = server
        .get("pingInterval")
        .and_then(|p| p.as_u64())
        .unwrap_or(30000);

    Ok(WsTokenResponse {
        token,
        endpoint: ws_endpoint,
        ping_interval,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_format_symbol_spot() {
        let trader = KucoinTrader::new("", "", None, false, MarketType::Spot);
        assert_eq!(trader.format_symbol("BTC"), "BTC-USDT");
        assert_eq!(trader.format_symbol("BTCUSDT"), "BTC-USDT");
        assert_eq!(trader.format_symbol("BTC-USDT"), "BTC-USDT");
        assert_eq!(trader.format_symbol("eth"), "ETH-USDT");
        assert_eq!(trader.format_symbol("BTC/USDT"), "BTC-USDT");
    }

    #[test]
    fn test_format_symbol_futures() {
        let trader = KucoinTrader::new("", "", None, false, MarketType::Swap);
        assert_eq!(trader.format_symbol("BTC"), "BTCUSDTM");
        assert_eq!(trader.format_symbol("BTCUSDT"), "BTCUSDTM");
        assert_eq!(trader.format_symbol("BTCUSDTM"), "BTCUSDTM");
        assert_eq!(trader.format_symbol("eth"), "ETHUSDTM");
        assert_eq!(trader.format_symbol("BTC-USDT"), "BTCUSDTM");
    }

    #[test]
    fn test_parse_order_status() {
        assert_eq!(parse_order_status_str("open"), OrderStatus::New);
        assert_eq!(parse_order_status_str("active"), OrderStatus::New);
        assert_eq!(parse_order_status_str("done"), OrderStatus::Filled);
        assert_eq!(parse_order_status_str("cancelled"), OrderStatus::Cancelled);
        assert_eq!(
            parse_order_status_str("match"),
            OrderStatus::PartiallyFilled
        );
    }

    #[test]
    fn test_base_url() {
        let spot_trader = KucoinTrader::new("", "", None, false, MarketType::Spot);
        assert_eq!(spot_trader.base_url(), SPOT_API_URL);

        let futures_trader = KucoinTrader::new("", "", None, false, MarketType::Swap);
        assert_eq!(futures_trader.base_url(), FUTURES_API_URL);
    }
}
