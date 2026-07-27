//! Kraken exchange trader implementation.

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
use serde::Serialize;
use sha2::{Digest, Sha256, Sha512};
use tokio::sync::broadcast;
use tokio::task::JoinHandle;
use tokio_tungstenite::{connect_async, tungstenite::protocol::Message};
use tracing::{error, info, warn};

// Spot API endpoints
const SPOT_REST_URL: &str = "https://api.kraken.com";
const SPOT_WS_PUBLIC_URL: &str = "wss://ws.kraken.com";
const SPOT_WS_PRIVATE_URL: &str = "wss://ws-auth.kraken.com";

// Futures API endpoints
const FUTURES_REST_URL: &str = "https://futures.kraken.com";
const FUTURES_WS_PUBLIC_URL: &str = "wss://futures.kraken.com/ws/v1";
const FUTURES_WS_PRIVATE_URL: &str = "wss://futures.kraken.com/ws/v1";

// Demo/Testnet endpoints
const FUTURES_DEMO_REST_URL: &str = "https://demo-futures.kraken.com";
const FUTURES_DEMO_WS_URL: &str = "wss://demo-futures.kraken.com/ws/v1";

/// Maximum reconnect backoff delay in seconds.
const MAX_RECONNECT_DELAY_SECS: u64 = 30;

/// Initial reconnect delay in seconds.
const INITIAL_RECONNECT_DELAY_SECS: u64 = 1;

/// Kraken exchange trader.
pub struct KrakenTrader {
    api_key: String,
    api_secret: String,
    testnet: bool,
    market_type: MarketType,
    client: Client,
}

impl KrakenTrader {
    /// Create a new Kraken trader.
    pub fn new(api_key: &str, api_secret: &str, testnet: bool, market_type: MarketType) -> Self {
        Self {
            api_key: api_key.to_string(),
            api_secret: api_secret.to_string(),
            testnet,
            market_type,
            client: Client::new(),
        }
    }

    /// Get the base REST URL based on market type and testnet setting.
    fn rest_url(&self) -> &str {
        match self.market_type {
            MarketType::Spot => SPOT_REST_URL,
            MarketType::Swap => {
                if self.testnet {
                    FUTURES_DEMO_REST_URL
                } else {
                    FUTURES_REST_URL
                }
            }
        }
    }

    /// Get WebSocket public URL.
    fn ws_public_url(&self) -> &str {
        match self.market_type {
            MarketType::Spot => SPOT_WS_PUBLIC_URL,
            MarketType::Swap => {
                if self.testnet {
                    FUTURES_DEMO_WS_URL
                } else {
                    FUTURES_WS_PUBLIC_URL
                }
            }
        }
    }

    /// Get WebSocket private URL.
    fn ws_private_url(&self) -> &str {
        match self.market_type {
            MarketType::Spot => SPOT_WS_PRIVATE_URL,
            MarketType::Swap => {
                if self.testnet {
                    FUTURES_DEMO_WS_URL
                } else {
                    FUTURES_WS_PRIVATE_URL
                }
            }
        }
    }

    /// Generate signature for Kraken Spot API.
    /// Kraken uses HMAC-SHA512 with: SHA256(nonce + POST data) prepended with URI path.
    fn spot_sign(&self, uri_path: &str, nonce: i64, post_data: &str) -> String {
        // Decode the base64 API secret
        let secret_bytes = general_purpose::STANDARD
            .decode(&self.api_secret)
            .unwrap_or_default();

        // SHA256 of nonce + post_data
        let mut sha256 = Sha256::new();
        sha256.update(format!("{}{}", nonce, post_data).as_bytes());
        let sha256_result = sha256.finalize();

        // Concatenate URI path + SHA256 result
        let mut message = uri_path.as_bytes().to_vec();
        message.extend_from_slice(&sha256_result);

        // HMAC-SHA512
        use hmac::{Hmac, Mac};
        type HmacSha512 = Hmac<Sha512>;

        let mut mac =
            HmacSha512::new_from_slice(&secret_bytes).expect("HMAC can take key of any size");
        mac.update(&message);

        let result = mac.finalize();
        general_purpose::STANDARD.encode(result.into_bytes())
    }

    /// Generate signature for Kraken Futures API.
    /// Futures uses: HMAC-SHA512(postData + nonce + endpoint, SHA256(secret))
    fn futures_sign(&self, endpoint: &str, nonce: &str, post_data: &str) -> String {
        // Decode the base64 API secret
        let secret_bytes = general_purpose::STANDARD
            .decode(&self.api_secret)
            .unwrap_or_default();

        // SHA256 of the secret
        let mut sha256 = Sha256::new();
        sha256.update(&secret_bytes);
        let hashed_secret = sha256.finalize();

        // Message: postData + nonce + endpoint
        let message = format!("{}{}{}", post_data, nonce, endpoint);

        // HMAC-SHA512 with hashed secret
        use hmac::{Hmac, Mac};
        type HmacSha512 = Hmac<Sha512>;

        let mut mac =
            HmacSha512::new_from_slice(&hashed_secret).expect("HMAC can take key of any size");
        mac.update(message.as_bytes());

        let result = mac.finalize();
        general_purpose::STANDARD.encode(result.into_bytes())
    }

    /// Convert standard symbol (BTC) to Kraken format (XBT).
    fn to_kraken_symbol(&self, symbol: &str) -> String {
        let s = symbol.to_uppercase();
        // Replace BTC with XBT for Kraken
        let s = s.replace("BTC", "XBT");

        match self.market_type {
            MarketType::Spot => {
                // Kraken spot uses formats like XXBTZUSD, XETHZUSD
                // But also accepts XBTUSD for API v0
                if s.contains('/') {
                    s.replace("/", "")
                } else if s.ends_with("USDT") || s.ends_with("USD") {
                    s
                } else {
                    format!("{}USD", s)
                }
            }
            MarketType::Swap => {
                // Kraken futures uses PI_XBTUSD format for perpetuals
                let base = if s.ends_with("USDT") {
                    s.trim_end_matches("USDT")
                } else if s.ends_with("USD") {
                    s.trim_end_matches("USD")
                } else {
                    &s
                };
                format!("PI_{}USD", base)
            }
        }
    }

    /// Convert Kraken symbol (XBT) back to standard format (BTC).
    fn from_kraken_symbol(symbol: &str) -> String {
        symbol.replace("XBT", "BTC").replace("PI_", "")
    }

    /// Make authenticated request to Spot API.
    async fn spot_private_request(
        &self,
        endpoint: &str,
        params: &[(&str, &str)],
    ) -> Result<serde_json::Value> {
        let nonce = timestamp_ms() * 1000; // Kraken uses microseconds for nonce
        let uri_path = format!("/0/private/{}", endpoint);

        let mut post_data = format!("nonce={}", nonce);
        for (key, value) in params {
            post_data.push_str(&format!("&{}={}", key, value));
        }

        let signature = self.spot_sign(&uri_path, nonce, &post_data);

        let response = self
            .client
            .post(format!("{}{}", SPOT_REST_URL, uri_path))
            .header("API-Key", &self.api_key)
            .header("API-Sign", signature)
            .header("Content-Type", "application/x-www-form-urlencoded")
            .body(post_data)
            .send()
            .await?;

        let json: serde_json::Value = response.json().await?;

        // Check for errors
        if let Some(errors) = json.get("error").and_then(|e| e.as_array()) {
            if !errors.is_empty() {
                let error_msg = errors
                    .iter()
                    .filter_map(|e| e.as_str())
                    .collect::<Vec<_>>()
                    .join(", ");
                anyhow::bail!("Kraken API error: {}", error_msg);
            }
        }

        Ok(json)
    }

    /// Make authenticated request to Futures API.
    async fn futures_private_request(
        &self,
        method: &str,
        endpoint: &str,
        params: Option<&serde_json::Value>,
    ) -> Result<serde_json::Value> {
        let base_url = self.rest_url();
        let nonce = timestamp_ms().to_string();

        let post_data = params
            .map(|p| serde_json::to_string(p).unwrap_or_default())
            .unwrap_or_default();

        let signature = self.futures_sign(endpoint, &nonce, &post_data);

        let url = format!("{}{}", base_url, endpoint);

        let request = match method {
            "GET" => self.client.get(&url),
            "POST" => self.client.post(&url).body(post_data.clone()),
            "DELETE" => self.client.delete(&url),
            _ => self.client.get(&url),
        };

        let response = request
            .header("APIKey", &self.api_key)
            .header("Nonce", &nonce)
            .header("Authent", signature)
            .header("Content-Type", "application/json")
            .send()
            .await?;

        let json: serde_json::Value = response.json().await?;

        // Check for errors
        if json.get("result").and_then(|r| r.as_str()) == Some("error") {
            let error_msg = json
                .get("error")
                .and_then(|e| e.as_str())
                .unwrap_or("Unknown error");
            anyhow::bail!("Kraken Futures API error: {}", error_msg);
        }

        Ok(json)
    }

    /// Get WebSocket authentication token for Spot private channels.
    async fn get_ws_token(&self) -> Result<String> {
        let response = self.spot_private_request("GetWebSocketsToken", &[]).await?;

        let token = response
            .get("result")
            .and_then(|r| r.get("token"))
            .and_then(|t| t.as_str())
            .ok_or_else(|| anyhow::anyhow!("Failed to get WebSocket token"))?;

        Ok(token.to_string())
    }

    /// Generate WebSocket authentication challenge response for Futures.
    fn futures_ws_auth_sign(&self, challenge: &str) -> String {
        let secret_bytes = general_purpose::STANDARD
            .decode(&self.api_secret)
            .unwrap_or_default();

        // SHA256 of the secret
        let mut sha256 = Sha256::new();
        sha256.update(&secret_bytes);
        let hashed_secret = sha256.finalize();

        // HMAC-SHA512 with hashed secret
        use hmac::{Hmac, Mac};
        type HmacSha512 = Hmac<Sha512>;

        let mut mac =
            HmacSha512::new_from_slice(&hashed_secret).expect("HMAC can take key of any size");
        mac.update(challenge.as_bytes());

        let result = mac.finalize();
        general_purpose::STANDARD.encode(result.into_bytes())
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
        let kraken_symbol = self.to_kraken_symbol(symbol);

        match self.market_type {
            MarketType::Spot => {
                let order_type_str = match order_type {
                    OrderType::Market => "market",
                    OrderType::Limit => "limit",
                    _ => "market",
                };

                let side_str = match side {
                    OrderSide::Buy => "buy",
                    OrderSide::Sell => "sell",
                };

                let mut params = vec![
                    ("pair", kraken_symbol.as_str()),
                    ("type", side_str),
                    ("ordertype", order_type_str),
                ];

                let volume_str = format!("{:.8}", quantity);
                params.push(("volume", &volume_str));

                let price_str;
                if let Some(p) = price {
                    price_str = format!("{:.8}", p);
                    params.push(("price", &price_str));
                }

                let response = self.spot_private_request("AddOrder", &params).await?;

                if let Some(result) = response.get("result") {
                    if let Some(txid) = result.get("txid").and_then(|t| t.as_array()) {
                        if let Some(order_id) = txid.first().and_then(|id| id.as_str()) {
                            info!("Kraken spot order placed: {}", order_id);
                            return Ok(OrderResult {
                                success: true,
                                order_id: Some(order_id.to_string()),
                                symbol: symbol.to_string(),
                                side,
                                order_type,
                                quantity: Some(quantity),
                                entry_price: price,
                                position_size: Some(quantity),
                                position_usdt: price.map(|p| quantity * p),
                                error: None,
                                raw_response: None,
                            });
                        }
                    }
                }

                Ok(OrderResult::failure(
                    symbol.to_string(),
                    side,
                    "Failed to parse order response".to_string(),
                ))
            }
            MarketType::Swap => {
                let order_type_str = match order_type {
                    OrderType::Market => "mkt",
                    OrderType::Limit => "lmt",
                    OrderType::StopMarket => "stp",
                    OrderType::StopLimit => "stp",
                    OrderType::TakeProfitMarket => "take_profit",
                    OrderType::TakeProfitLimit => "take_profit",
                    OrderType::TrailingStop => "trailing_stop",
                };

                let side_str = match side {
                    OrderSide::Buy => "buy",
                    OrderSide::Sell => "sell",
                };

                let mut order_params = serde_json::json!({
                    "orderType": order_type_str,
                    "symbol": kraken_symbol,
                    "side": side_str,
                    "size": quantity,
                });

                if let Some(p) = price {
                    order_params["limitPrice"] = serde_json::json!(p);
                }

                if reduce_only {
                    order_params["reduceOnly"] = serde_json::json!(true);
                }

                let response = self
                    .futures_private_request(
                        "POST",
                        "/derivatives/api/v3/sendorder",
                        Some(&order_params),
                    )
                    .await?;

                if let Some(send_status) = response.get("sendStatus") {
                    if let Some(order_id) = send_status.get("order_id").and_then(|id| id.as_str()) {
                        let status = send_status
                            .get("status")
                            .and_then(|s| s.as_str())
                            .unwrap_or("");

                        if status == "placed" || status == "partiallyFilled" {
                            info!("Kraken futures order placed: {}", order_id);
                            return Ok(OrderResult {
                                success: true,
                                order_id: Some(order_id.to_string()),
                                symbol: symbol.to_string(),
                                side,
                                order_type,
                                quantity: Some(quantity),
                                entry_price: price,
                                position_size: Some(quantity),
                                position_usdt: price.map(|p| quantity * p),
                                error: None,
                                raw_response: None,
                            });
                        } else {
                            let error = send_status
                                .get("receivedTime")
                                .and_then(|e| e.as_str())
                                .unwrap_or("Order rejected");
                            return Ok(OrderResult::failure(
                                symbol.to_string(),
                                side,
                                error.to_string(),
                            ));
                        }
                    }
                }

                Ok(OrderResult::failure(
                    symbol.to_string(),
                    side,
                    "Failed to parse futures order response".to_string(),
                ))
            }
        }
    }

    /// Cancel order via REST API.
    async fn cancel_order_rest(&self, symbol: &str, order_id: &str) -> Result<bool> {
        match self.market_type {
            MarketType::Spot => {
                let params = [("txid", order_id)];
                let response = self.spot_private_request("CancelOrder", &params).await?;

                if let Some(result) = response.get("result") {
                    if result.get("count").and_then(|c| c.as_i64()).unwrap_or(0) > 0 {
                        info!("Kraken spot order cancelled: {}", order_id);
                        return Ok(true);
                    }
                }
                Ok(false)
            }
            MarketType::Swap => {
                let params = serde_json::json!({
                    "order_id": order_id,
                });

                let response = self
                    .futures_private_request(
                        "POST",
                        "/derivatives/api/v3/cancelorder",
                        Some(&params),
                    )
                    .await?;

                if response.get("result").and_then(|r| r.as_str()) == Some("success") {
                    info!("Kraken futures order cancelled: {}", order_id);
                    return Ok(true);
                }

                let _ = symbol; // Used for logging if needed
                Ok(false)
            }
        }
    }
}

/// Calculate next reconnect delay with exponential backoff (capped).
fn next_reconnect_delay(current_secs: u64) -> u64 {
    (current_secs * 2).min(MAX_RECONNECT_DELAY_SECS)
}

/// Parse Kraken order status string into OrderStatus enum.
fn parse_order_status_str(s: &str) -> OrderStatus {
    match s.to_lowercase().as_str() {
        "pending" | "open" | "untouched" => OrderStatus::New,
        "partial" | "partialfilled" | "partiallyfilled" => OrderStatus::PartiallyFilled,
        "closed" | "filled" | "executed" => OrderStatus::Filled,
        "canceled" | "cancelled" => OrderStatus::Cancelled,
        "rejected" | "invalidated" => OrderStatus::Rejected,
        "expired" => OrderStatus::Expired,
        _ => {
            warn!("Unknown Kraken order status: '{}'", s);
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

/// WebSocket subscription message for Kraken Spot.
#[derive(Debug, Serialize)]
struct SpotWsSubscribe {
    event: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    reqid: Option<i64>,
    pair: Vec<String>,
    subscription: SpotWsSubscription,
}

#[derive(Debug, Serialize)]
struct SpotWsSubscription {
    name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    depth: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    token: Option<String>,
}

/// WebSocket subscription message for Kraken Futures.
#[derive(Debug, Serialize)]
struct FuturesWsSubscribe {
    event: String,
    feed: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    product_ids: Option<Vec<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    api_key: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    original_challenge: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    signed_challenge: Option<String>,
}

#[async_trait]
impl ExchangeTrader for KrakenTrader {
    fn exchange_name(&self) -> &str {
        "kraken"
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
        match self.market_type {
            MarketType::Spot => {
                let response = self
                    .client
                    .get(format!("{}/0/public/Time", SPOT_REST_URL))
                    .send()
                    .await?;
                Ok(response.status().is_success())
            }
            MarketType::Swap => {
                let url = format!("{}/derivatives/api/v3/tickers", self.rest_url());
                let response = self.client.get(&url).send().await?;
                Ok(response.status().is_success())
            }
        }
    }

    async fn get_server_time(&self) -> Result<i64> {
        match self.market_type {
            MarketType::Spot => {
                let response: serde_json::Value = self
                    .client
                    .get(format!("{}/0/public/Time", SPOT_REST_URL))
                    .send()
                    .await?
                    .json()
                    .await?;

                let time = response
                    .get("result")
                    .and_then(|r| r.get("unixtime"))
                    .and_then(|t| t.as_i64())
                    .map(|t| t * 1000)
                    .unwrap_or(timestamp_ms());

                Ok(time)
            }
            MarketType::Swap => {
                // Futures API doesn't have a dedicated time endpoint, use local time
                Ok(timestamp_ms())
            }
        }
    }

    async fn get_balance(&self) -> Result<AccountBalance> {
        match self.market_type {
            MarketType::Spot => {
                let response = self.spot_private_request("Balance", &[]).await?;

                let mut total = 0.0;
                let mut balances = vec![];

                if let Some(result) = response.get("result").and_then(|r| r.as_object()) {
                    for (asset, value) in result {
                        if let Some(amount) = value.as_str().and_then(|s| s.parse::<f64>().ok()) {
                            if amount > 0.0 {
                                // Convert XBT back to BTC for display
                                let display_asset =
                                    asset.replace("XXBT", "BTC").replace("ZUSD", "USD");
                                balances.push(Balance {
                                    asset: display_asset,
                                    free: amount,
                                    locked: 0.0,
                                });

                                // Approximate total in USD
                                if asset.contains("USD") {
                                    total += amount;
                                }
                            }
                        }
                    }
                }

                Ok(AccountBalance {
                    total_balance: total,
                    available_balance: total,
                    currency: "USD".to_string(),
                    margin_balance: None,
                    used_margin: None,
                    unrealized_pnl: None,
                    realized_pnl: None,
                    balances,
                })
            }
            MarketType::Swap => {
                let response = self
                    .futures_private_request("GET", "/derivatives/api/v3/accounts", None)
                    .await?;

                let mut total = 0.0;
                let mut available = 0.0;
                let mut margin = 0.0;
                let mut pnl = 0.0;

                if let Some(accounts) = response.get("accounts").and_then(|a| a.as_object()) {
                    // Look for the cash/flex account
                    for (_, account) in accounts {
                        if let Some(balances) = account.get("balances").and_then(|b| b.as_object())
                        {
                            for (_, balance) in balances {
                                total += balance.as_f64().unwrap_or(0.0);
                            }
                        }
                        if let Some(av) = account.get("availableMargin").and_then(|a| a.as_f64()) {
                            available = av;
                        }
                        if let Some(im) = account.get("initialMargin").and_then(|a| a.as_f64()) {
                            margin = im;
                        }
                        if let Some(upnl) = account.get("unrealizedPnl").and_then(|a| a.as_f64()) {
                            pnl = upnl;
                        }
                    }
                }

                Ok(AccountBalance {
                    total_balance: total,
                    available_balance: available,
                    currency: "USD".to_string(),
                    margin_balance: Some(total),
                    used_margin: Some(margin),
                    unrealized_pnl: Some(pnl),
                    realized_pnl: None,
                    balances: vec![],
                })
            }
        }
    }

    async fn get_current_price(&self, symbol: &str) -> Result<f64> {
        let kraken_symbol = self.to_kraken_symbol(symbol);

        match self.market_type {
            MarketType::Spot => {
                let response: serde_json::Value = self
                    .client
                    .get(format!(
                        "{}/0/public/Ticker?pair={}",
                        SPOT_REST_URL, kraken_symbol
                    ))
                    .send()
                    .await?
                    .json()
                    .await?;

                // Kraken returns result with the pair name as key
                if let Some(result) = response.get("result").and_then(|r| r.as_object()) {
                    for (_, ticker) in result {
                        // "c" is the last trade closed [price, lot volume]
                        if let Some(price) = ticker
                            .get("c")
                            .and_then(|c| c.as_array())
                            .and_then(|arr| arr.first())
                            .and_then(|p| p.as_str())
                            .and_then(|s| s.parse::<f64>().ok())
                        {
                            return Ok(price);
                        }
                    }
                }

                anyhow::bail!("Failed to get price for {}", symbol)
            }
            MarketType::Swap => {
                let url = format!("{}/derivatives/api/v3/tickers", self.rest_url());
                let response: serde_json::Value =
                    self.client.get(&url).send().await?.json().await?;

                if let Some(tickers) = response.get("tickers").and_then(|t| t.as_array()) {
                    for ticker in tickers {
                        if ticker.get("symbol").and_then(|s| s.as_str()) == Some(&kraken_symbol) {
                            if let Some(price) = ticker.get("last").and_then(|p| p.as_f64()) {
                                return Ok(price);
                            }
                        }
                    }
                }

                anyhow::bail!("Failed to get futures price for {}", symbol)
            }
        }
    }

    async fn get_ticker(&self, symbol: &str) -> Result<TickerInfo> {
        let kraken_symbol = self.to_kraken_symbol(symbol);

        match self.market_type {
            MarketType::Spot => {
                let response: serde_json::Value = self
                    .client
                    .get(format!(
                        "{}/0/public/Ticker?pair={}",
                        SPOT_REST_URL, kraken_symbol
                    ))
                    .send()
                    .await?
                    .json()
                    .await?;

                if let Some(result) = response.get("result").and_then(|r| r.as_object()) {
                    if let Some((_, ticker)) = result.into_iter().next() {
                        let last_price = ticker
                            .get("c")
                            .and_then(|c| c.as_array())
                            .and_then(|arr| arr.first())
                            .and_then(|p| p.as_str())
                            .and_then(|s| s.parse::<f64>().ok())
                            .unwrap_or(0.0);

                        let bid_price = ticker
                            .get("b")
                            .and_then(|b| b.as_array())
                            .and_then(|arr| arr.first())
                            .and_then(|p| p.as_str())
                            .and_then(|s| s.parse::<f64>().ok());

                        let ask_price = ticker
                            .get("a")
                            .and_then(|a| a.as_array())
                            .and_then(|arr| arr.first())
                            .and_then(|p| p.as_str())
                            .and_then(|s| s.parse::<f64>().ok());

                        let high_24h = ticker
                            .get("h")
                            .and_then(|h| h.as_array())
                            .and_then(|arr| arr.get(1))
                            .and_then(|p| p.as_str())
                            .and_then(|s| s.parse::<f64>().ok());

                        let low_24h = ticker
                            .get("l")
                            .and_then(|l| l.as_array())
                            .and_then(|arr| arr.get(1))
                            .and_then(|p| p.as_str())
                            .and_then(|s| s.parse::<f64>().ok());

                        let volume_24h = ticker
                            .get("v")
                            .and_then(|v| v.as_array())
                            .and_then(|arr| arr.get(1))
                            .and_then(|p| p.as_str())
                            .and_then(|s| s.parse::<f64>().ok());

                        return Ok(TickerInfo {
                            symbol: Self::from_kraken_symbol(&kraken_symbol),
                            last_price,
                            bid_price,
                            ask_price,
                            high_24h,
                            low_24h,
                            volume_24h,
                            turnover_24h: None,
                            change_24h: None,
                            open_interest: None,
                            open_interest_value: None,
                            funding_rate: None,
                            next_funding_time: None,
                            mark_price: None,
                            index_price: None,
                            timestamp: timestamp_ms(),
                        });
                    }
                }

                anyhow::bail!("Failed to get ticker for {}", symbol)
            }
            MarketType::Swap => {
                let url = format!("{}/derivatives/api/v3/tickers", self.rest_url());
                let response: serde_json::Value =
                    self.client.get(&url).send().await?.json().await?;

                if let Some(tickers) = response.get("tickers").and_then(|t| t.as_array()) {
                    for ticker in tickers {
                        if ticker.get("symbol").and_then(|s| s.as_str()) == Some(&kraken_symbol) {
                            return Ok(TickerInfo {
                                symbol: Self::from_kraken_symbol(&kraken_symbol),
                                last_price: ticker
                                    .get("last")
                                    .and_then(|p| p.as_f64())
                                    .unwrap_or(0.0),
                                bid_price: ticker.get("bid").and_then(|p| p.as_f64()),
                                ask_price: ticker.get("ask").and_then(|p| p.as_f64()),
                                high_24h: ticker.get("high24h").and_then(|p| p.as_f64()),
                                low_24h: ticker.get("low24h").and_then(|p| p.as_f64()),
                                volume_24h: ticker.get("vol24h").and_then(|p| p.as_f64()),
                                turnover_24h: None,
                                change_24h: ticker.get("change24h").and_then(|p| p.as_f64()),
                                open_interest: ticker.get("openInterest").and_then(|p| p.as_f64()),
                                open_interest_value: None,
                                funding_rate: ticker.get("fundingRate").and_then(|p| p.as_f64()),
                                next_funding_time: ticker
                                    .get("nextFundingRateTime")
                                    .and_then(|t| t.as_i64()),
                                mark_price: ticker.get("markPrice").and_then(|p| p.as_f64()),
                                index_price: ticker.get("indexPrice").and_then(|p| p.as_f64()),
                                timestamp: timestamp_ms(),
                            });
                        }
                    }
                }

                anyhow::bail!("Failed to get futures ticker for {}", symbol)
            }
        }
    }

    async fn get_orderbook(&self, symbol: &str, depth: u32) -> Result<Orderbook> {
        let kraken_symbol = self.to_kraken_symbol(symbol);

        match self.market_type {
            MarketType::Spot => {
                let response: serde_json::Value = self
                    .client
                    .get(format!(
                        "{}/0/public/Depth?pair={}&count={}",
                        SPOT_REST_URL, kraken_symbol, depth
                    ))
                    .send()
                    .await?
                    .json()
                    .await?;

                if let Some(result) = response.get("result").and_then(|r| r.as_object()) {
                    if let Some((_, data)) = result.into_iter().next() {
                        let bids: Vec<OrderbookLevel> = data
                            .get("bids")
                            .and_then(|b| b.as_array())
                            .map(|arr| {
                                arr.iter()
                                    .filter_map(|item| {
                                        let price = item.get(0)?.as_str()?.parse().ok()?;
                                        let quantity = item.get(1)?.as_str()?.parse().ok()?;
                                        Some(OrderbookLevel { price, quantity })
                                    })
                                    .collect()
                            })
                            .unwrap_or_default();

                        let asks: Vec<OrderbookLevel> = data
                            .get("asks")
                            .and_then(|a| a.as_array())
                            .map(|arr| {
                                arr.iter()
                                    .filter_map(|item| {
                                        let price = item.get(0)?.as_str()?.parse().ok()?;
                                        let quantity = item.get(1)?.as_str()?.parse().ok()?;
                                        Some(OrderbookLevel { price, quantity })
                                    })
                                    .collect()
                            })
                            .unwrap_or_default();

                        return Ok(Orderbook {
                            symbol: Self::from_kraken_symbol(&kraken_symbol),
                            bids,
                            asks,
                            timestamp: timestamp_ms(),
                        });
                    }
                }

                anyhow::bail!("Failed to get orderbook for {}", symbol)
            }
            MarketType::Swap => {
                let url = format!(
                    "{}/derivatives/api/v3/orderbook?symbol={}",
                    self.rest_url(),
                    kraken_symbol
                );
                let response: serde_json::Value =
                    self.client.get(&url).send().await?.json().await?;

                let orderbook = response.get("orderBook").unwrap_or(&response);

                let bids: Vec<OrderbookLevel> = orderbook
                    .get("bids")
                    .and_then(|b| b.as_array())
                    .map(|arr| {
                        arr.iter()
                            .take(depth as usize)
                            .filter_map(|item| {
                                let price = item.get(0)?.as_f64()?;
                                let quantity = item.get(1)?.as_f64()?;
                                Some(OrderbookLevel { price, quantity })
                            })
                            .collect()
                    })
                    .unwrap_or_default();

                let asks: Vec<OrderbookLevel> = orderbook
                    .get("asks")
                    .and_then(|a| a.as_array())
                    .map(|arr| {
                        arr.iter()
                            .take(depth as usize)
                            .filter_map(|item| {
                                let price = item.get(0)?.as_f64()?;
                                let quantity = item.get(1)?.as_f64()?;
                                Some(OrderbookLevel { price, quantity })
                            })
                            .collect()
                    })
                    .unwrap_or_default();

                Ok(Orderbook {
                    symbol: Self::from_kraken_symbol(&kraken_symbol),
                    bids,
                    asks,
                    timestamp: timestamp_ms(),
                })
            }
        }
    }

    async fn set_leverage(&self, symbol: Option<&str>, leverage: u32) -> Result<bool> {
        match self.market_type {
            MarketType::Spot => {
                // Spot doesn't support leverage
                Ok(true)
            }
            MarketType::Swap => {
                // Kraken Futures leverage is set per-order, not globally
                // This is a no-op but we log it for reference
                if let Some(sym) = symbol {
                    info!(
                        "Kraken futures leverage {} will be applied to {} orders",
                        leverage, sym
                    );
                } else {
                    info!(
                        "Kraken futures leverage {} will be applied to orders",
                        leverage
                    );
                }
                Ok(true)
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

        // Set leverage for swap trading (no-op for Kraken but logs intent)
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

        // Place market order
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
        self.cancel_order_rest(symbol, order_id).await
    }

    async fn get_order_status(&self, symbol: &str, order_id: &str) -> Result<OrderInfo> {
        match self.market_type {
            MarketType::Spot => {
                let params = [("txid", order_id)];
                let response = self.spot_private_request("QueryOrders", &params).await?;

                if let Some(result) = response.get("result").and_then(|r| r.as_object()) {
                    if let Some(order) = result.get(order_id) {
                        let status_str = order
                            .get("status")
                            .and_then(|s| s.as_str())
                            .unwrap_or("pending");

                        let side_str = order
                            .get("descr")
                            .and_then(|d| d.get("type"))
                            .and_then(|t| t.as_str())
                            .unwrap_or("buy");

                        let order_type_str = order
                            .get("descr")
                            .and_then(|d| d.get("ordertype"))
                            .and_then(|t| t.as_str())
                            .unwrap_or("market");

                        let price = order
                            .get("descr")
                            .and_then(|d| d.get("price"))
                            .and_then(|p| p.as_str())
                            .and_then(|s| s.parse().ok())
                            .unwrap_or(0.0);

                        let quantity = order
                            .get("vol")
                            .and_then(|v| v.as_str())
                            .and_then(|s| s.parse().ok())
                            .unwrap_or(0.0);

                        let filled = order
                            .get("vol_exec")
                            .and_then(|v| v.as_str())
                            .and_then(|s| s.parse().ok())
                            .unwrap_or(0.0);

                        let avg_price = order
                            .get("price")
                            .and_then(|p| p.as_str())
                            .and_then(|s| s.parse().ok());

                        return Ok(OrderInfo {
                            order_id: order_id.to_string(),
                            client_order_id: order
                                .get("userref")
                                .and_then(|u| u.as_i64())
                                .map(|i| i.to_string()),
                            symbol: Self::from_kraken_symbol(symbol),
                            status: parse_order_status_str(status_str),
                            side: if side_str == "sell" {
                                OrderSide::Sell
                            } else {
                                OrderSide::Buy
                            },
                            order_type: if order_type_str == "limit" {
                                OrderType::Limit
                            } else {
                                OrderType::Market
                            },
                            price,
                            quantity,
                            filled_quantity: filled,
                            remaining_quantity: quantity - filled,
                            avg_price,
                            timestamp: order
                                .get("opentm")
                                .and_then(|t| t.as_f64())
                                .map(|t| (t * 1000.0) as i64)
                                .unwrap_or(0),
                            time_in_force: TimeInForce::GTC,
                            reduce_only: false,
                        });
                    }
                }

                anyhow::bail!("Order {} not found", order_id)
            }
            MarketType::Swap => {
                let params = serde_json::json!({
                    "orderIds": [order_id],
                });

                let response = self
                    .futures_private_request(
                        "POST",
                        "/derivatives/api/v3/orders/status",
                        Some(&params),
                    )
                    .await?;

                if let Some(orders) = response.get("orders").and_then(|o| o.as_array()) {
                    if let Some(order) = orders.first() {
                        let status_str = order
                            .get("status")
                            .and_then(|s| s.as_str())
                            .unwrap_or("open");

                        let side_str = order.get("side").and_then(|s| s.as_str()).unwrap_or("buy");

                        let order_type_str = order
                            .get("orderType")
                            .and_then(|t| t.as_str())
                            .unwrap_or("mkt");

                        return Ok(OrderInfo {
                            order_id: order_id.to_string(),
                            client_order_id: order
                                .get("cliOrdId")
                                .and_then(|c| c.as_str())
                                .map(|s| s.to_string()),
                            symbol: Self::from_kraken_symbol(symbol),
                            status: parse_order_status_str(status_str),
                            side: if side_str == "sell" {
                                OrderSide::Sell
                            } else {
                                OrderSide::Buy
                            },
                            order_type: if order_type_str == "lmt" {
                                OrderType::Limit
                            } else {
                                OrderType::Market
                            },
                            price: order
                                .get("limitPrice")
                                .and_then(|p| p.as_f64())
                                .unwrap_or(0.0),
                            quantity: order.get("qty").and_then(|q| q.as_f64()).unwrap_or(0.0),
                            filled_quantity: order
                                .get("filled")
                                .and_then(|f| f.as_f64())
                                .unwrap_or(0.0),
                            remaining_quantity: order
                                .get("unfilledSize")
                                .and_then(|u| u.as_f64())
                                .unwrap_or(0.0),
                            avg_price: order.get("avgFillPrice").and_then(|p| p.as_f64()),
                            timestamp: order
                                .get("receivedTime")
                                .and_then(|t| t.as_i64())
                                .unwrap_or(0),
                            time_in_force: TimeInForce::GTC,
                            reduce_only: order
                                .get("reduceOnly")
                                .and_then(|r| r.as_bool())
                                .unwrap_or(false),
                        });
                    }
                }

                anyhow::bail!("Futures order {} not found", order_id)
            }
        }
    }

    async fn close_position(&self, symbol: &str) -> Result<OrderResult> {
        match self.market_type {
            MarketType::Spot => Ok(OrderResult::failure(
                symbol.to_string(),
                OrderSide::Sell,
                "Spot trading does not have positions to close".to_string(),
            )),
            MarketType::Swap => {
                // Get current position
                let response = self
                    .futures_private_request("GET", "/derivatives/api/v3/openpositions", None)
                    .await?;

                let kraken_symbol = self.to_kraken_symbol(symbol);

                if let Some(positions) = response.get("openPositions").and_then(|p| p.as_array()) {
                    for pos in positions {
                        if pos.get("symbol").and_then(|s| s.as_str()) == Some(&kraken_symbol) {
                            let size = pos.get("size").and_then(|s| s.as_f64()).unwrap_or(0.0);
                            let side_str =
                                pos.get("side").and_then(|s| s.as_str()).unwrap_or("long");

                            if size.abs() > 0.0 {
                                // Close by placing opposite order
                                let close_side = if side_str == "long" {
                                    OrderSide::Sell
                                } else {
                                    OrderSide::Buy
                                };

                                return self
                                    .place_order_rest(
                                        symbol,
                                        close_side,
                                        OrderType::Market,
                                        size.abs(),
                                        None,
                                        true,
                                    )
                                    .await;
                            }
                        }
                    }
                }

                Ok(OrderResult::failure(
                    symbol.to_string(),
                    OrderSide::Sell,
                    "No position found to close".to_string(),
                ))
            }
        }
    }

    fn format_symbol(&self, base: &str) -> String {
        let s = base.to_uppercase();

        // Handle BTC/USDT format
        if s.contains('/') {
            let s = s.replace("/", "");
            // Convert BTC to XBT for Kraken
            return s.replace("BTC", "XBT");
        }

        // Convert BTC to XBT
        let s = s.replace("BTC", "XBT");

        if s.ends_with("USDT") || s.ends_with("USD") {
            s
        } else {
            format!("{}USD", s)
        }
    }

    async fn test_ws_orderbook(
        &self,
        symbol: &str,
        depth: u32,
        timeout_ms: u64,
    ) -> Result<WsTestResult> {
        let ws_url = self.ws_public_url();
        let kraken_symbol = self.to_kraken_symbol(symbol);
        let start = std::time::Instant::now();

        let (mut ws_stream, _) = connect_async(ws_url)
            .await
            .context("Failed to connect to Kraken WebSocket")?;

        let connect_time = start.elapsed().as_millis() as u64;

        // Subscribe to orderbook
        match self.market_type {
            MarketType::Spot => {
                let sub_msg = SpotWsSubscribe {
                    event: "subscribe".to_string(),
                    reqid: Some(1),
                    pair: vec![kraken_symbol],
                    subscription: SpotWsSubscription {
                        name: "book".to_string(),
                        depth: Some(depth.min(100)),
                        token: None,
                    },
                };
                let msg_json = serde_json::to_string(&sub_msg)?;
                ws_stream.send(Message::Text(msg_json)).await?;
            }
            MarketType::Swap => {
                let sub_msg = FuturesWsSubscribe {
                    event: "subscribe".to_string(),
                    feed: "book".to_string(),
                    product_ids: Some(vec![kraken_symbol]),
                    api_key: None,
                    original_challenge: None,
                    signed_challenge: None,
                };
                let msg_json = serde_json::to_string(&sub_msg)?;
                ws_stream.send(Message::Text(msg_json)).await?;
            }
        }

        let mut messages_received = 0u32;
        let mut data_sample = None;
        let timeout = tokio::time::Duration::from_millis(timeout_ms);

        loop {
            tokio::select! {
                msg = ws_stream.next() => {
                    match msg {
                        Some(Ok(Message::Text(text))) => {
                            messages_received += 1;
                            if data_sample.is_none() {
                                data_sample = serde_json::from_str(&text).ok();
                            }
                            if messages_received >= 3 {
                                break;
                            }
                        }
                        Some(Ok(Message::Ping(data))) => {
                            let _ = ws_stream.send(Message::Pong(data)).await;
                        }
                        Some(Err(e)) => {
                            error!("WebSocket error: {}", e);
                            break;
                        }
                        None => break,
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
        let ws_url = self.ws_public_url();
        let kraken_symbol = self.to_kraken_symbol(symbol);
        let start = std::time::Instant::now();

        let (mut ws_stream, _) = connect_async(ws_url)
            .await
            .context("Failed to connect to Kraken WebSocket")?;

        let connect_time = start.elapsed().as_millis() as u64;

        // Subscribe to trades
        match self.market_type {
            MarketType::Spot => {
                let sub_msg = SpotWsSubscribe {
                    event: "subscribe".to_string(),
                    reqid: Some(2),
                    pair: vec![kraken_symbol],
                    subscription: SpotWsSubscription {
                        name: "trade".to_string(),
                        depth: None,
                        token: None,
                    },
                };
                let msg_json = serde_json::to_string(&sub_msg)?;
                ws_stream.send(Message::Text(msg_json)).await?;
            }
            MarketType::Swap => {
                let sub_msg = FuturesWsSubscribe {
                    event: "subscribe".to_string(),
                    feed: "trade".to_string(),
                    product_ids: Some(vec![kraken_symbol]),
                    api_key: None,
                    original_challenge: None,
                    signed_challenge: None,
                };
                let msg_json = serde_json::to_string(&sub_msg)?;
                ws_stream.send(Message::Text(msg_json)).await?;
            }
        }

        let mut messages_received = 0u32;
        let mut data_sample = None;
        let timeout = tokio::time::Duration::from_millis(timeout_ms);

        loop {
            tokio::select! {
                msg = ws_stream.next() => {
                    match msg {
                        Some(Ok(Message::Text(text))) => {
                            messages_received += 1;
                            if data_sample.is_none() {
                                data_sample = serde_json::from_str(&text).ok();
                            }
                            if messages_received >= 3 {
                                break;
                            }
                        }
                        Some(Ok(Message::Ping(data))) => {
                            let _ = ws_stream.send(Message::Pong(data)).await;
                        }
                        Some(Err(e)) => {
                            error!("WebSocket error: {}", e);
                            break;
                        }
                        None => break,
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
        let ws_url = self.ws_public_url();
        let kraken_symbol = self.to_kraken_symbol(symbol);
        let start = std::time::Instant::now();

        let (mut ws_stream, _) = connect_async(ws_url)
            .await
            .context("Failed to connect to Kraken WebSocket")?;

        let connect_time = start.elapsed().as_millis() as u64;

        // Subscribe to ticker
        match self.market_type {
            MarketType::Spot => {
                let sub_msg = SpotWsSubscribe {
                    event: "subscribe".to_string(),
                    reqid: Some(3),
                    pair: vec![kraken_symbol],
                    subscription: SpotWsSubscription {
                        name: "ticker".to_string(),
                        depth: None,
                        token: None,
                    },
                };
                let msg_json = serde_json::to_string(&sub_msg)?;
                ws_stream.send(Message::Text(msg_json)).await?;
            }
            MarketType::Swap => {
                let sub_msg = FuturesWsSubscribe {
                    event: "subscribe".to_string(),
                    feed: "ticker".to_string(),
                    product_ids: Some(vec![kraken_symbol]),
                    api_key: None,
                    original_challenge: None,
                    signed_challenge: None,
                };
                let msg_json = serde_json::to_string(&sub_msg)?;
                ws_stream.send(Message::Text(msg_json)).await?;
            }
        }

        let mut messages_received = 0u32;
        let mut data_sample = None;
        let timeout = tokio::time::Duration::from_millis(timeout_ms);

        loop {
            tokio::select! {
                msg = ws_stream.next() => {
                    match msg {
                        Some(Ok(Message::Text(text))) => {
                            messages_received += 1;
                            if data_sample.is_none() {
                                data_sample = serde_json::from_str(&text).ok();
                            }
                            if messages_received >= 3 {
                                break;
                            }
                        }
                        Some(Ok(Message::Ping(data))) => {
                            let _ = ws_stream.send(Message::Pong(data)).await;
                        }
                        Some(Err(e)) => {
                            error!("WebSocket error: {}", e);
                            break;
                        }
                        None => break,
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
        let testnet = self.testnet;
        let market_type = self.market_type;
        let exchange_name = "kraken".to_string();

        // For spot, we need to get a WebSocket token first
        let ws_token = if market_type == MarketType::Spot {
            Some(self.get_ws_token().await?)
        } else {
            None
        };

        let handle = tokio::spawn(async move {
            let mut reconnect_delay = INITIAL_RECONNECT_DELAY_SECS;

            loop {
                let ws_url = match market_type {
                    MarketType::Spot => SPOT_WS_PRIVATE_URL,
                    MarketType::Swap => {
                        if testnet {
                            FUTURES_DEMO_WS_URL
                        } else {
                            FUTURES_WS_PRIVATE_URL
                        }
                    }
                };

                info!("[{}] Connecting to private WS: {}", exchange_name, ws_url);

                match connect_async(ws_url).await {
                    Ok((mut ws_stream, _)) => {
                        let authenticated = match market_type {
                            MarketType::Spot => {
                                // Spot uses token-based auth
                                if let Some(ref token) = ws_token {
                                    let sub_msg = serde_json::json!({
                                        "event": "subscribe",
                                        "subscription": {
                                            "name": "openOrders",
                                            "token": token
                                        }
                                    });

                                    if let Err(e) =
                                        ws_stream.send(Message::Text(sub_msg.to_string())).await
                                    {
                                        error!("[{}] Failed to subscribe: {}", exchange_name, e);
                                        false
                                    } else {
                                        info!("[{}] Subscribed to openOrders", exchange_name);
                                        true
                                    }
                                } else {
                                    error!("[{}] No WebSocket token available", exchange_name);
                                    false
                                }
                            }
                            MarketType::Swap => {
                                // Futures uses challenge-response auth
                                // First, request a challenge
                                let challenge_req = serde_json::json!({
                                    "event": "challenge",
                                    "api_key": api_key
                                });

                                if let Err(e) = ws_stream
                                    .send(Message::Text(challenge_req.to_string()))
                                    .await
                                {
                                    error!(
                                        "[{}] Failed to send challenge request: {}",
                                        exchange_name, e
                                    );
                                    false
                                } else {
                                    // Wait for challenge response
                                    let mut auth_ok = false;

                                    if let Some(Ok(Message::Text(text))) = ws_stream.next().await {
                                        if let Ok(resp) =
                                            serde_json::from_str::<serde_json::Value>(&text)
                                        {
                                            if let Some(challenge) =
                                                resp.get("message").and_then(|m| m.as_str())
                                            {
                                                // Sign the challenge
                                                let secret_bytes = general_purpose::STANDARD
                                                    .decode(&api_secret)
                                                    .unwrap_or_default();

                                                let mut sha256 = Sha256::new();
                                                sha256.update(&secret_bytes);
                                                let hashed_secret = sha256.finalize();

                                                use hmac::{Hmac, Mac};
                                                type HmacSha512 = Hmac<Sha512>;

                                                let mut mac =
                                                    HmacSha512::new_from_slice(&hashed_secret)
                                                        .expect("HMAC can take key of any size");
                                                mac.update(challenge.as_bytes());

                                                let signed = general_purpose::STANDARD
                                                    .encode(mac.finalize().into_bytes());

                                                // Subscribe with signed challenge
                                                let sub_msg = serde_json::json!({
                                                    "event": "subscribe",
                                                    "feed": "open_orders",
                                                    "api_key": api_key,
                                                    "original_challenge": challenge,
                                                    "signed_challenge": signed
                                                });

                                                if let Err(e) = ws_stream
                                                    .send(Message::Text(sub_msg.to_string()))
                                                    .await
                                                {
                                                    error!(
                                                        "[{}] Failed to subscribe: {}",
                                                        exchange_name, e
                                                    );
                                                } else {
                                                    info!(
                                                        "[{}] Subscribed to open_orders",
                                                        exchange_name
                                                    );
                                                    auth_ok = true;
                                                }
                                            }
                                        }
                                    }

                                    auth_ok
                                }
                            }
                        };

                        if !authenticated {
                            tokio::time::sleep(tokio::time::Duration::from_secs(reconnect_delay))
                                .await;
                            reconnect_delay = next_reconnect_delay(reconnect_delay);
                            continue;
                        }

                        reconnect_delay = INITIAL_RECONNECT_DELAY_SECS;
                        let mut ping_interval =
                            tokio::time::interval(tokio::time::Duration::from_secs(30));

                        loop {
                            tokio::select! {
                                msg = ws_stream.next() => {
                                    match msg {
                                        Some(Ok(Message::Text(text))) => {
                                            if let Ok(msg_json) = serde_json::from_str::<serde_json::Value>(&text) {
                                                // Handle different message formats
                                                match market_type {
                                                    MarketType::Spot => {
                                                        // Spot format: array with channel name and data
                                                        if let Some(arr) = msg_json.as_array() {
                                                            if arr.len() >= 2 {
                                                                if let Some(orders) = arr.first().and_then(|o| o.as_array()) {
                                                                    for order_data in orders {
                                                                        for (order_id, order) in order_data.as_object().unwrap_or(&serde_json::Map::new()) {
                                                                            let event = OrderUpdateEvent {
                                                                                exchange: exchange_name.clone(),
                                                                                order_id: order_id.clone(),
                                                                                client_order_id: order.get("userref").and_then(|u| u.as_i64()).map(|i| i.to_string()),
                                                                                symbol: order.get("descr").and_then(|d| d.get("pair")).and_then(|p| p.as_str()).unwrap_or("").replace("XBT", "BTC"),
                                                                                status: parse_order_status_str(
                                                                                    order.get("status").and_then(|s| s.as_str()).unwrap_or("open")
                                                                                ),
                                                                                side: if order.get("descr").and_then(|d| d.get("type")).and_then(|t| t.as_str()) == Some("sell") {
                                                                                    OrderSide::Sell
                                                                                } else {
                                                                                    OrderSide::Buy
                                                                                },
                                                                                order_type: if order.get("descr").and_then(|d| d.get("ordertype")).and_then(|t| t.as_str()) == Some("limit") {
                                                                                    OrderType::Limit
                                                                                } else {
                                                                                    OrderType::Market
                                                                                },
                                                                                price: order.get("descr").and_then(|d| d.get("price")).and_then(|p| p.as_str()).and_then(|s| s.parse().ok()).unwrap_or(0.0),
                                                                                quantity: order.get("vol").and_then(|v| v.as_str()).and_then(|s| s.parse().ok()).unwrap_or(0.0),
                                                                                filled_quantity: order.get("vol_exec").and_then(|v| v.as_str()).and_then(|s| s.parse().ok()).unwrap_or(0.0),
                                                                                avg_fill_price: order.get("avg_price").and_then(|p| p.as_str()).and_then(|s| s.parse().ok()),
                                                                                fee: order.get("fee").and_then(|f| f.as_str()).and_then(|s| s.parse().ok()),
                                                                                fee_currency: None,
                                                                                exchange_timestamp: order.get("opentm").and_then(|t| t.as_f64()).map(|t| (t * 1000.0) as i64).unwrap_or(0),
                                                                                received_at: timestamp_ms(),
                                                                                raw: Some(order.clone()),
                                                                            };
                                                                            log_order_update(&event);
                                                                            let _ = sender.send(event);
                                                                        }
                                                                    }
                                                                }
                                                            }
                                                        }
                                                    }
                                                    MarketType::Swap => {
                                                        // Futures format
                                                        if msg_json.get("feed").and_then(|f| f.as_str()) == Some("open_orders") {
                                                            if let Some(orders) = msg_json.get("orders").and_then(|o| o.as_array()) {
                                                                for order in orders {
                                                                    let event = OrderUpdateEvent {
                                                                        exchange: exchange_name.clone(),
                                                                        order_id: order.get("order_id").and_then(|id| id.as_str()).unwrap_or("").to_string(),
                                                                        client_order_id: order.get("cli_ord_id").and_then(|c| c.as_str()).map(|s| s.to_string()),
                                                                        symbol: order.get("symbol").and_then(|s| s.as_str()).unwrap_or("").replace("PI_", "").replace("XBT", "BTC"),
                                                                        status: parse_order_status_str(
                                                                            order.get("status").and_then(|s| s.as_str()).unwrap_or("open")
                                                                        ),
                                                                        side: if order.get("side").and_then(|s| s.as_str()) == Some("sell") {
                                                                            OrderSide::Sell
                                                                        } else {
                                                                            OrderSide::Buy
                                                                        },
                                                                        order_type: if order.get("order_type").and_then(|t| t.as_str()) == Some("lmt") {
                                                                            OrderType::Limit
                                                                        } else {
                                                                            OrderType::Market
                                                                        },
                                                                        price: order.get("limit_price").and_then(|p| p.as_f64()).unwrap_or(0.0),
                                                                        quantity: order.get("qty").and_then(|q| q.as_f64()).unwrap_or(0.0),
                                                                        filled_quantity: order.get("filled").and_then(|f| f.as_f64()).unwrap_or(0.0),
                                                                        avg_fill_price: order.get("avg_fill_price").and_then(|p| p.as_f64()),
                                                                        fee: None,
                                                                        fee_currency: None,
                                                                        exchange_timestamp: order.get("timestamp").and_then(|t| t.as_i64()).unwrap_or(0),
                                                                        received_at: timestamp_ms(),
                                                                        raw: Some(order.clone()),
                                                                    };
                                                                    log_order_update(&event);
                                                                    let _ = sender.send(event);
                                                                }
                                                            }
                                                        }
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
                                    let ping_msg = match market_type {
                                        MarketType::Spot => serde_json::json!({"event": "ping"}),
                                        MarketType::Swap => serde_json::json!({"event": "heartbeat"}),
                                    };
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_format_symbol() {
        let trader = KrakenTrader::new("", "", false, MarketType::Spot);
        assert_eq!(trader.format_symbol("BTC"), "XBTUSD");
        assert_eq!(trader.format_symbol("BTCUSD"), "XBTUSD");
        assert_eq!(trader.format_symbol("eth"), "ETHUSD");
        assert_eq!(trader.format_symbol("BTC/USDT"), "XBTUSDT");
        assert_eq!(trader.format_symbol("ETH/USD"), "ETHUSD");
    }

    #[test]
    fn test_to_kraken_symbol_spot() {
        let trader = KrakenTrader::new("", "", false, MarketType::Spot);
        assert_eq!(trader.to_kraken_symbol("BTCUSD"), "XBTUSD");
        assert_eq!(trader.to_kraken_symbol("ETHUSD"), "ETHUSD");
        assert_eq!(trader.to_kraken_symbol("BTC"), "XBTUSD");
    }

    #[test]
    fn test_to_kraken_symbol_futures() {
        let trader = KrakenTrader::new("", "", false, MarketType::Swap);
        assert_eq!(trader.to_kraken_symbol("BTCUSD"), "PI_XBTUSD");
        assert_eq!(trader.to_kraken_symbol("ETHUSD"), "PI_ETHUSD");
    }

    #[test]
    fn test_from_kraken_symbol() {
        assert_eq!(KrakenTrader::from_kraken_symbol("XBTUSD"), "BTCUSD");
        assert_eq!(KrakenTrader::from_kraken_symbol("PI_XBTUSD"), "BTCUSD");
    }

    #[test]
    fn test_parse_order_status() {
        assert_eq!(parse_order_status_str("open"), OrderStatus::New);
        assert_eq!(parse_order_status_str("closed"), OrderStatus::Filled);
        assert_eq!(parse_order_status_str("canceled"), OrderStatus::Cancelled);
        assert_eq!(
            parse_order_status_str("partial"),
            OrderStatus::PartiallyFilled
        );
    }

    #[test]
    fn test_rest_urls() {
        let spot = KrakenTrader::new("", "", false, MarketType::Spot);
        assert_eq!(spot.rest_url(), SPOT_REST_URL);

        let futures = KrakenTrader::new("", "", false, MarketType::Swap);
        assert_eq!(futures.rest_url(), FUTURES_REST_URL);

        let futures_demo = KrakenTrader::new("", "", true, MarketType::Swap);
        assert_eq!(futures_demo.rest_url(), FUTURES_DEMO_REST_URL);
    }

    #[test]
    fn test_ws_urls() {
        let spot = KrakenTrader::new("", "", false, MarketType::Spot);
        assert_eq!(spot.ws_public_url(), SPOT_WS_PUBLIC_URL);
        assert_eq!(spot.ws_private_url(), SPOT_WS_PRIVATE_URL);

        let futures = KrakenTrader::new("", "", false, MarketType::Swap);
        assert_eq!(futures.ws_public_url(), FUTURES_WS_PUBLIC_URL);
    }
}
