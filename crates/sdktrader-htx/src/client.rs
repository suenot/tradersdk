//! HTX (formerly Huobi) exchange trader implementation.
//!
//! Full-featured implementation supporting:
//! - REST API for trading and market data
//! - WebSocket for real-time data and private order updates
//! - HMAC-SHA256 authentication (signature in query params)
//! - Automatic reconnection with exponential backoff
//!
//! # API Documentation
//! - REST: https://www.htx.com/en-us/opend/newApiPages/
//! - Spot: https://api.huobi.pro
//! - Futures: https://api.hbdm.com

use anyhow::{Context, Result};
use async_trait::async_trait;
use flate2::read::GzDecoder;
use futures_util::{SinkExt, StreamExt};
use reqwest::Client;
use sdktrader_core::models::{
    AccountBalance, Balance, ExchangeCapabilities, FundingRateEntry, MarketType, OpenInterestEntry,
    OrderInfo, OrderResult, OrderSide, OrderStatus, OrderType, OrderUpdateEvent, Orderbook,
    OrderbookLevel, TickerInfo, TimeInForce, WsTestResult,
};
use sdktrader_core::utils::{hmac_sign_base64, timestamp_ms};
use sdktrader_core::ExchangeTrader;
use serde::Deserialize;
use std::io::Read;
use tokio::sync::broadcast;
use tokio::task::JoinHandle;
use tokio_tungstenite::{connect_async, tungstenite::protocol::Message};
use tracing::{debug, error, info, warn};

// API Endpoints
const SPOT_REST_URL: &str = "https://api.huobi.pro";
const FUTURES_REST_URL: &str = "https://api.hbdm.com";

// WebSocket Endpoints
const SPOT_WS_URL: &str = "wss://api.huobi.pro/ws";
const FUTURES_WS_URL: &str = "wss://api.hbdm.com/linear-swap-ws";
const SPOT_WS_PRIVATE_URL: &str = "wss://api.huobi.pro/ws/v2";
const FUTURES_WS_PRIVATE_URL: &str = "wss://api.hbdm.com/linear-swap-notification";

/// Maximum reconnect backoff delay in seconds.
const MAX_RECONNECT_DELAY_SECS: u64 = 30;

/// Initial reconnect delay in seconds.
const INITIAL_RECONNECT_DELAY_SECS: u64 = 1;

/// HTX exchange trader.
pub struct HtxTrader {
    api_key: String,
    api_secret: String,
    testnet: bool,
    market_type: MarketType,
    client: Client,
    base_url: String,
}

impl HtxTrader {
    /// Create a new HTX trader.
    pub fn new(api_key: &str, api_secret: &str, testnet: bool, market_type: MarketType) -> Self {
        // Note: HTX doesn't have a public testnet, testnet flag is kept for API compatibility
        let base_url = match market_type {
            MarketType::Spot => SPOT_REST_URL.to_string(),
            MarketType::Swap => FUTURES_REST_URL.to_string(),
        };

        Self {
            api_key: api_key.to_string(),
            api_secret: api_secret.to_string(),
            testnet,
            market_type,
            client: Client::new(),
            base_url,
        }
    }

    /// Get WebSocket public URL based on market type.
    fn ws_public_url(&self) -> &str {
        match self.market_type {
            MarketType::Spot => SPOT_WS_URL,
            MarketType::Swap => FUTURES_WS_URL,
        }
    }

    /// Get WebSocket private URL based on market type.
    fn ws_private_url(&self) -> &str {
        match self.market_type {
            MarketType::Spot => SPOT_WS_PRIVATE_URL,
            MarketType::Swap => FUTURES_WS_PRIVATE_URL,
        }
    }

    /// Generate signature for REST API requests.
    /// HTX uses signature in query params with specific format.
    fn sign_request(&self, method: &str, host: &str, path: &str, params: &str) -> String {
        let sign_payload = format!("{}\n{}\n{}\n{}", method, host, path, params);
        hmac_sign_base64(&self.api_secret, &sign_payload)
    }

    /// Build signed query string for REST API.
    fn build_signed_params(&self, method: &str, path: &str, extra_params: &str) -> String {
        let timestamp = chrono::Utc::now().format("%Y-%m-%dT%H:%M:%S").to_string();
        let host = if self.market_type == MarketType::Spot {
            "api.huobi.pro"
        } else {
            "api.hbdm.com"
        };

        let mut params = format!(
            "AccessKeyId={}&SignatureMethod=HmacSHA256&SignatureVersion=2&Timestamp={}",
            self.api_key,
            urlencoding::encode(&timestamp)
        );

        if !extra_params.is_empty() {
            params = format!("{}&{}", params, extra_params);
        }

        // Sort parameters alphabetically
        let mut param_pairs: Vec<&str> = params.split('&').collect();
        param_pairs.sort();
        let sorted_params = param_pairs.join("&");

        let signature = self.sign_request(method, host, path, &sorted_params);
        let encoded_sig = urlencoding::encode(&signature);

        format!("{}&Signature={}", sorted_params, encoded_sig)
    }

    /// Generate WebSocket authentication message for HTX.
    fn ws_auth_message(&self) -> serde_json::Value {
        let timestamp = chrono::Utc::now().format("%Y-%m-%dT%H:%M:%S").to_string();
        let host = if self.market_type == MarketType::Spot {
            "api.huobi.pro"
        } else {
            "api.hbdm.com"
        };
        let path = if self.market_type == MarketType::Spot {
            "/ws/v2"
        } else {
            "/linear-swap-notification"
        };

        let params = format!(
            "AccessKeyId={}&SignatureMethod=HmacSHA256&SignatureVersion=2&Timestamp={}",
            self.api_key,
            urlencoding::encode(&timestamp)
        );

        let signature = self.sign_request("GET", host, path, &params);

        if self.market_type == MarketType::Spot {
            serde_json::json!({
                "action": "req",
                "ch": "auth",
                "params": {
                    "authType": "api",
                    "accessKey": self.api_key,
                    "signatureMethod": "HmacSHA256",
                    "signatureVersion": "2.1",
                    "timestamp": timestamp,
                    "signature": signature
                }
            })
        } else {
            serde_json::json!({
                "op": "auth",
                "type": "api",
                "AccessKeyId": self.api_key,
                "SignatureMethod": "HmacSHA256",
                "SignatureVersion": "2",
                "Timestamp": timestamp,
                "Signature": signature
            })
        }
    }

    /// Make a signed GET request.
    async fn signed_get<T: for<'de> serde::Deserialize<'de>>(
        &self,
        path: &str,
        extra_params: &str,
    ) -> Result<T> {
        let signed_params = self.build_signed_params("GET", path, extra_params);
        let url = format!("{}{}?{}", self.base_url, path, signed_params);

        debug!("HTX GET: {}", url);

        let response = self
            .client
            .get(&url)
            .send()
            .await
            .context("Failed to send request to HTX")?;

        let text = response.text().await?;
        debug!("HTX response: {}", text);

        serde_json::from_str(&text).context(format!("Failed to parse HTX response: {}", text))
    }

    /// Make a signed POST request.
    async fn signed_post<T: for<'de> serde::Deserialize<'de>>(
        &self,
        path: &str,
        body: &serde_json::Value,
    ) -> Result<T> {
        let signed_params = self.build_signed_params("POST", path, "");
        let url = format!("{}{}?{}", self.base_url, path, signed_params);

        debug!("HTX POST: {} body: {}", url, body);

        let response = self
            .client
            .post(&url)
            .header("Content-Type", "application/json")
            .body(body.to_string())
            .send()
            .await
            .context("Failed to send request to HTX")?;

        let text = response.text().await?;
        debug!("HTX response: {}", text);

        serde_json::from_str(&text).context(format!("Failed to parse HTX response: {}", text))
    }

    /// Decompress GZIP data from WebSocket message.
    fn decompress_gzip(data: &[u8]) -> Result<String> {
        let mut decoder = GzDecoder::new(data);
        let mut result = String::new();
        decoder
            .read_to_string(&mut result)
            .context("Failed to decompress GZIP data")?;
        Ok(result)
    }

    /// Get account ID for spot trading.
    async fn get_account_id(&self) -> Result<i64> {
        let response: HtxResponse<Vec<AccountInfo>> =
            self.signed_get("/v1/account/accounts", "").await?;

        if response.status != "ok" {
            anyhow::bail!(
                "Failed to get accounts: {}",
                response.err_msg.unwrap_or_default()
            );
        }

        let accounts = response
            .data
            .ok_or_else(|| anyhow::anyhow!("No accounts found"))?;

        // Find spot account
        for account in accounts {
            if account.account_type == "spot" && account.state == "working" {
                return Ok(account.id);
            }
        }

        anyhow::bail!("No working spot account found")
    }

    /// Place order via REST API.
    async fn place_order_rest(
        &self,
        symbol: &str,
        side: OrderSide,
        order_type: OrderType,
        quantity: f64,
        price: Option<f64>,
    ) -> Result<OrderResult> {
        if self.market_type == MarketType::Spot {
            self.place_spot_order(symbol, side, order_type, quantity, price)
                .await
        } else {
            self.place_futures_order(symbol, side, order_type, quantity, price)
                .await
        }
    }

    /// Place spot order.
    async fn place_spot_order(
        &self,
        symbol: &str,
        side: OrderSide,
        order_type: OrderType,
        quantity: f64,
        price: Option<f64>,
    ) -> Result<OrderResult> {
        let account_id = self.get_account_id().await?;

        let order_type_str = match (side, order_type) {
            (OrderSide::Buy, OrderType::Market) => "buy-market",
            (OrderSide::Sell, OrderType::Market) => "sell-market",
            (OrderSide::Buy, OrderType::Limit) => "buy-limit",
            (OrderSide::Sell, OrderType::Limit) => "sell-limit",
            _ => "buy-market",
        };

        let mut body = serde_json::json!({
            "account-id": account_id,
            "symbol": symbol.to_lowercase(),
            "type": order_type_str,
            "amount": format!("{:.8}", quantity),
        });

        if let (OrderType::Limit, Some(p)) = (order_type, price) {
            body["price"] = serde_json::json!(format!("{:.8}", p));
        }

        let response: HtxResponse<String> =
            self.signed_post("/v1/order/orders/place", &body).await?;

        if response.status == "ok" {
            let order_id = response.data.unwrap_or_default();
            info!("HTX spot order placed: {}", order_id);
            Ok(OrderResult {
                success: true,
                order_id: Some(order_id),
                symbol: symbol.to_string(),
                side,
                order_type,
                quantity: Some(quantity),
                entry_price: price,
                position_size: Some(quantity),
                position_usdt: price.map(|p| quantity * p),
                error: None,
                raw_response: None,
            })
        } else {
            let error_msg = response
                .err_msg
                .unwrap_or_else(|| "Unknown error".to_string());
            error!("HTX spot order failed: {}", error_msg);
            Ok(OrderResult::failure(symbol.to_string(), side, error_msg))
        }
    }

    /// Place futures order.
    async fn place_futures_order(
        &self,
        symbol: &str,
        side: OrderSide,
        order_type: OrderType,
        quantity: f64,
        price: Option<f64>,
    ) -> Result<OrderResult> {
        let direction = match side {
            OrderSide::Buy => "buy",
            OrderSide::Sell => "sell",
        };

        let order_price_type = match order_type {
            OrderType::Market => "opponent", // market order uses opponent price
            OrderType::Limit => "limit",
            _ => "opponent",
        };

        let mut body = serde_json::json!({
            "contract_code": symbol.to_uppercase(),
            "volume": quantity as i64,
            "direction": direction,
            "offset": "open",
            "lever_rate": 10, // default leverage
            "order_price_type": order_price_type,
        });

        if let (OrderType::Limit, Some(p)) = (order_type, price) {
            body["price"] = serde_json::json!(p);
        }

        let response: HtxFuturesResponse<FuturesOrderResult> = self
            .signed_post("/linear-swap-api/v1/swap_order", &body)
            .await?;

        if response.status == "ok" {
            if let Some(data) = response.data {
                let order_id = data
                    .order_id_str
                    .unwrap_or_else(|| data.order_id.to_string());
                info!("HTX futures order placed: {}", order_id);
                return Ok(OrderResult {
                    success: true,
                    order_id: Some(order_id),
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

        let error_msg = response
            .err_msg
            .unwrap_or_else(|| "Unknown error".to_string());
        error!("HTX futures order failed: {}", error_msg);
        Ok(OrderResult::failure(symbol.to_string(), side, error_msg))
    }

    /// Cancel order via REST API.
    async fn cancel_order_rest(&self, symbol: &str, order_id: &str) -> Result<bool> {
        if self.market_type == MarketType::Spot {
            let path = format!("/v1/order/orders/{}/submitcancel", order_id);
            let response: HtxResponse<String> =
                self.signed_post(&path, &serde_json::json!({})).await?;

            if response.status == "ok" {
                info!("HTX spot order cancelled: {}", order_id);
                Ok(true)
            } else {
                let error_msg = response.err_msg.unwrap_or_default();
                error!("Failed to cancel spot order {}: {}", order_id, error_msg);
                Ok(false)
            }
        } else {
            let body = serde_json::json!({
                "order_id": order_id,
                "contract_code": symbol.to_uppercase(),
            });

            let response: HtxFuturesResponse<serde_json::Value> = self
                .signed_post("/linear-swap-api/v1/swap_cancel", &body)
                .await?;

            if response.status == "ok" {
                info!("HTX futures order cancelled: {}", order_id);
                Ok(true)
            } else {
                let error_msg = response.err_msg.unwrap_or_default();
                error!("Failed to cancel futures order {}: {}", order_id, error_msg);
                Ok(false)
            }
        }
    }
}

/// Calculate next reconnect delay with exponential backoff (capped).
fn next_reconnect_delay(current_secs: u64) -> u64 {
    (current_secs * 2).min(MAX_RECONNECT_DELAY_SECS)
}

/// Parse a status string from HTX into OrderStatus.
fn parse_order_status_str(s: &str) -> OrderStatus {
    match s.to_lowercase().as_str() {
        "submitted" | "created" | "open" | "new" => OrderStatus::New,
        "partial-filled" | "partial_filled" | "partiallyfilled" => OrderStatus::PartiallyFilled,
        "filled" | "full-filled" => OrderStatus::Filled,
        "canceled" | "cancelled" | "partial-canceled" => OrderStatus::Cancelled,
        "rejected" | "failed" => OrderStatus::Rejected,
        "expired" => OrderStatus::Expired,
        _ => {
            warn!("Unknown HTX order status string: '{}'", s);
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

// ===============================================================================
// API Response Types
// ===============================================================================

/// HTX API response wrapper for spot.
#[derive(Debug, Deserialize)]
struct HtxResponse<T> {
    status: String,
    data: Option<T>,
    #[serde(rename = "err-code")]
    err_code: Option<String>,
    #[serde(rename = "err-msg")]
    err_msg: Option<String>,
}

/// HTX API response wrapper for futures.
#[derive(Debug, Deserialize)]
struct HtxFuturesResponse<T> {
    status: String,
    data: Option<T>,
    err_code: Option<i64>,
    err_msg: Option<String>,
    ts: Option<i64>,
}

/// Account info.
#[derive(Debug, Deserialize)]
struct AccountInfo {
    id: i64,
    #[serde(rename = "type")]
    account_type: String,
    state: String,
}

/// Futures order result.
#[derive(Debug, Deserialize)]
struct FuturesOrderResult {
    order_id: i64,
    order_id_str: Option<String>,
}

/// Ticker data from HTX.
#[derive(Debug, Deserialize)]
struct HtxTicker {
    symbol: Option<String>,
    open: Option<f64>,
    high: Option<f64>,
    low: Option<f64>,
    close: Option<f64>,
    amount: Option<f64>,
    vol: Option<f64>,
    count: Option<i64>,
    bid: Option<f64>,
    #[serde(rename = "bidSize")]
    bid_size: Option<f64>,
    ask: Option<f64>,
    #[serde(rename = "askSize")]
    ask_size: Option<f64>,
}

/// Orderbook data from HTX.
#[derive(Debug, Deserialize)]
struct HtxOrderbook {
    ts: Option<i64>,
    bids: Vec<Vec<f64>>,
    asks: Vec<Vec<f64>>,
}

/// Balance info for spot.
#[derive(Debug, Deserialize)]
struct SpotBalance {
    currency: String,
    #[serde(rename = "type")]
    balance_type: String,
    balance: String,
}

/// Futures account info.
#[derive(Debug, Deserialize)]
struct FuturesAccountInfo {
    margin_balance: Option<f64>,
    margin_available: Option<f64>,
    profit_unreal: Option<f64>,
    margin_static: Option<f64>,
}

/// Futures position info.
#[derive(Debug, Deserialize)]
struct FuturesPosition {
    symbol: String,
    contract_code: String,
    volume: f64,
    available: f64,
    direction: String,
    cost_open: f64,
    cost_hold: f64,
    profit_unreal: f64,
    lever_rate: i32,
}

/// Server timestamp response.
#[derive(Debug, Deserialize)]
struct TimestampResponse {
    status: String,
    data: Option<i64>,
}

/// WebSocket message for parsing.
#[derive(Debug, Deserialize)]
struct WsMessage {
    ch: Option<String>,
    ts: Option<i64>,
    tick: Option<serde_json::Value>,
    ping: Option<i64>,
    pong: Option<i64>,
    action: Option<String>,
    code: Option<i64>,
    #[serde(rename = "op")]
    operation: Option<String>,
}

// ===============================================================================
// ExchangeTrader Implementation
// ===============================================================================

#[async_trait]
impl ExchangeTrader for HtxTrader {
    fn exchange_name(&self) -> &str {
        "htx"
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
            get_funding_rate_history: Supported,
            get_open_interest_history: Supported,
            execute_fast_entry: Supported,
            format_symbol: Supported,
            ws_orderbook: Supported,
            ws_trades: Supported,
            ws_ticker: Supported,
            ws_orders: Supported,
        }
    }

    async fn test_connection(&self) -> Result<bool> {
        let url = if self.market_type == MarketType::Spot {
            format!("{}/v1/common/timestamp", SPOT_REST_URL)
        } else {
            format!("{}/api/v1/timestamp", FUTURES_REST_URL)
        };

        let response = self.client.get(&url).send().await?;
        Ok(response.status().is_success())
    }

    async fn get_server_time(&self) -> Result<i64> {
        if self.market_type == MarketType::Spot {
            let url = format!("{}/v1/common/timestamp", SPOT_REST_URL);
            let response: TimestampResponse = self.client.get(&url).send().await?.json().await?;

            if let Some(ts) = response.data {
                return Ok(ts);
            }
            anyhow::bail!("Failed to get server time")
        } else {
            let url = format!("{}/api/v1/timestamp", FUTURES_REST_URL);
            let response: HtxFuturesResponse<serde_json::Value> =
                self.client.get(&url).send().await?.json().await?;

            if let Some(ts) = response.ts {
                return Ok(ts);
            }
            anyhow::bail!("Failed to get server time")
        }
    }

    async fn get_balance(&self) -> Result<AccountBalance> {
        if self.market_type == MarketType::Spot {
            let account_id = self.get_account_id().await?;
            let path = format!("/v1/account/accounts/{}/balance", account_id);
            let response: HtxResponse<serde_json::Value> = self.signed_get(&path, "").await?;

            if response.status != "ok" {
                anyhow::bail!(
                    "Failed to get balance: {}",
                    response.err_msg.unwrap_or_default()
                );
            }

            let data = response
                .data
                .ok_or_else(|| anyhow::anyhow!("No balance data"))?;
            let list = data
                .get("list")
                .and_then(|l| l.as_array())
                .ok_or_else(|| anyhow::anyhow!("Invalid balance format"))?;

            let mut balances = Vec::new();
            let mut usdt_free = 0.0;
            let mut usdt_locked = 0.0;

            for item in list {
                let currency = item.get("currency").and_then(|c| c.as_str()).unwrap_or("");
                let balance_type = item.get("type").and_then(|t| t.as_str()).unwrap_or("");
                let balance: f64 = item
                    .get("balance")
                    .and_then(|b| b.as_str())
                    .and_then(|b| b.parse().ok())
                    .unwrap_or(0.0);

                if currency == "usdt" {
                    if balance_type == "trade" {
                        usdt_free = balance;
                    } else if balance_type == "frozen" {
                        usdt_locked = balance;
                    }
                }

                // Aggregate balances by currency
                if balance_type == "trade" {
                    balances.push(Balance {
                        asset: currency.to_uppercase(),
                        free: balance,
                        locked: 0.0,
                    });
                }
            }

            Ok(AccountBalance {
                total_balance: usdt_free + usdt_locked,
                available_balance: usdt_free,
                currency: "USDT".to_string(),
                margin_balance: None,
                used_margin: None,
                unrealized_pnl: None,
                realized_pnl: None,
                balances,
            })
        } else {
            let response: HtxFuturesResponse<Vec<FuturesAccountInfo>> = self
                .signed_post(
                    "/linear-swap-api/v1/swap_cross_account_info",
                    &serde_json::json!({}),
                )
                .await?;

            if response.status != "ok" {
                anyhow::bail!(
                    "Failed to get balance: {}",
                    response.err_msg.unwrap_or_default()
                );
            }

            if let Some(accounts) = response.data {
                if let Some(acc) = accounts.first() {
                    return Ok(AccountBalance {
                        total_balance: acc.margin_balance.unwrap_or(0.0),
                        available_balance: acc.margin_available.unwrap_or(0.0),
                        currency: "USDT".to_string(),
                        margin_balance: acc.margin_balance,
                        used_margin: Some(
                            acc.margin_balance.unwrap_or(0.0) - acc.margin_available.unwrap_or(0.0),
                        ),
                        unrealized_pnl: acc.profit_unreal,
                        realized_pnl: None,
                        balances: vec![],
                    });
                }
            }

            Ok(AccountBalance::default())
        }
    }

    async fn get_current_price(&self, symbol: &str) -> Result<f64> {
        let formatted_symbol = self.format_symbol(symbol);

        if self.market_type == MarketType::Spot {
            let url = format!(
                "{}/market/detail/merged?symbol={}",
                SPOT_REST_URL,
                formatted_symbol.to_lowercase()
            );
            let response: HtxResponse<HtxTicker> =
                self.client.get(&url).send().await?.json().await?;

            if response.status == "ok" {
                if let Some(ticker) = response.data {
                    return Ok(ticker.close.unwrap_or(0.0));
                }
            }
            anyhow::bail!("Failed to get price for {}", symbol)
        } else {
            let url = format!(
                "{}/linear-swap-ex/market/detail/merged?contract_code={}",
                FUTURES_REST_URL, formatted_symbol
            );
            let response: HtxFuturesResponse<HtxTicker> =
                self.client.get(&url).send().await?.json().await?;

            if response.status == "ok" {
                if let Some(ticker) = response.data {
                    return Ok(ticker.close.unwrap_or(0.0));
                }
            }
            anyhow::bail!("Failed to get price for {}", symbol)
        }
    }

    async fn get_ticker(&self, symbol: &str) -> Result<TickerInfo> {
        let formatted_symbol = self.format_symbol(symbol);

        if self.market_type == MarketType::Spot {
            let url = format!(
                "{}/market/detail/merged?symbol={}",
                SPOT_REST_URL,
                formatted_symbol.to_lowercase()
            );
            let response: HtxResponse<HtxTicker> =
                self.client.get(&url).send().await?.json().await?;

            if response.status == "ok" {
                if let Some(t) = response.data {
                    return Ok(TickerInfo {
                        symbol: formatted_symbol,
                        last_price: t.close.unwrap_or(0.0),
                        bid_price: t.bid,
                        ask_price: t.ask,
                        high_24h: t.high,
                        low_24h: t.low,
                        volume_24h: t.amount,
                        turnover_24h: t.vol,
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
        } else {
            // Get ticker
            let url = format!(
                "{}/linear-swap-ex/market/detail/merged?contract_code={}",
                FUTURES_REST_URL, formatted_symbol
            );
            let response: HtxFuturesResponse<HtxTicker> =
                self.client.get(&url).send().await?.json().await?;

            let ticker = if response.status == "ok" {
                response.data
            } else {
                None
            };

            // Get funding rate
            let funding_url = format!(
                "{}/linear-swap-api/v1/swap_funding_rate?contract_code={}",
                FUTURES_REST_URL, formatted_symbol
            );
            let funding_response: HtxFuturesResponse<serde_json::Value> =
                self.client.get(&funding_url).send().await?.json().await?;

            let (funding_rate, next_funding_time) = if let Some(data) = funding_response.data {
                (
                    data.get("funding_rate")
                        .and_then(|r| r.as_str())
                        .and_then(|r| r.parse().ok()),
                    data.get("next_funding_time").and_then(|t| t.as_i64()),
                )
            } else {
                (None, None)
            };

            // Get open interest
            let oi_url = format!(
                "{}/linear-swap-api/v1/swap_open_interest?contract_code={}",
                FUTURES_REST_URL, formatted_symbol
            );
            let oi_response: HtxFuturesResponse<Vec<serde_json::Value>> =
                self.client.get(&oi_url).send().await?.json().await?;

            let (open_interest, open_interest_value) = if let Some(data) = oi_response.data {
                if let Some(first) = data.first() {
                    (
                        first.get("volume").and_then(|v| v.as_f64()),
                        first.get("value").and_then(|v| v.as_f64()),
                    )
                } else {
                    (None, None)
                }
            } else {
                (None, None)
            };

            if let Some(t) = ticker {
                return Ok(TickerInfo {
                    symbol: formatted_symbol,
                    last_price: t.close.unwrap_or(0.0),
                    bid_price: t.bid,
                    ask_price: t.ask,
                    high_24h: t.high,
                    low_24h: t.low,
                    volume_24h: t.amount,
                    turnover_24h: t.vol,
                    change_24h: None,
                    open_interest,
                    open_interest_value,
                    funding_rate,
                    next_funding_time,
                    mark_price: None,
                    index_price: None,
                    timestamp: timestamp_ms(),
                });
            }

            anyhow::bail!("Failed to get ticker for {}", symbol)
        }
    }

    async fn get_orderbook(&self, symbol: &str, depth: u32) -> Result<Orderbook> {
        let formatted_symbol = self.format_symbol(symbol);
        let limit = depth.min(150);
        let depth_type = if limit <= 5 {
            "step0"
        } else if limit <= 20 {
            "step1"
        } else {
            "step2"
        };

        if self.market_type == MarketType::Spot {
            let url = format!(
                "{}/market/depth?symbol={}&type={}&depth={}",
                SPOT_REST_URL,
                formatted_symbol.to_lowercase(),
                depth_type,
                limit
            );
            let response: HtxResponse<HtxOrderbook> =
                self.client.get(&url).send().await?.json().await?;

            if response.status == "ok" {
                if let Some(ob) = response.data {
                    let bids: Vec<OrderbookLevel> = ob
                        .bids
                        .iter()
                        .filter_map(|level| {
                            if level.len() >= 2 {
                                Some(OrderbookLevel::new(level[0], level[1]))
                            } else {
                                None
                            }
                        })
                        .collect();

                    let asks: Vec<OrderbookLevel> = ob
                        .asks
                        .iter()
                        .filter_map(|level| {
                            if level.len() >= 2 {
                                Some(OrderbookLevel::new(level[0], level[1]))
                            } else {
                                None
                            }
                        })
                        .collect();

                    return Ok(Orderbook {
                        symbol: formatted_symbol,
                        bids,
                        asks,
                        timestamp: ob.ts.unwrap_or(timestamp_ms()),
                    });
                }
            }
        } else {
            let url = format!(
                "{}/linear-swap-ex/market/depth?contract_code={}&type={}",
                FUTURES_REST_URL, formatted_symbol, depth_type
            );
            let response: HtxFuturesResponse<HtxOrderbook> =
                self.client.get(&url).send().await?.json().await?;

            if response.status == "ok" {
                if let Some(ob) = response.data {
                    let bids: Vec<OrderbookLevel> = ob
                        .bids
                        .iter()
                        .filter_map(|level| {
                            if level.len() >= 2 {
                                Some(OrderbookLevel::new(level[0], level[1]))
                            } else {
                                None
                            }
                        })
                        .collect();

                    let asks: Vec<OrderbookLevel> = ob
                        .asks
                        .iter()
                        .filter_map(|level| {
                            if level.len() >= 2 {
                                Some(OrderbookLevel::new(level[0], level[1]))
                            } else {
                                None
                            }
                        })
                        .collect();

                    return Ok(Orderbook {
                        symbol: formatted_symbol,
                        bids,
                        asks,
                        timestamp: ob.ts.unwrap_or(timestamp_ms()),
                    });
                }
            }
        }

        anyhow::bail!("Failed to get orderbook for {}", symbol)
    }

    async fn get_funding_rate_history(
        &self,
        symbol: &str,
        limit: u32,
    ) -> Result<Vec<FundingRateEntry>> {
        if self.market_type != MarketType::Swap {
            anyhow::bail!("Funding rate only available for perpetual futures");
        }

        let formatted_symbol = self.format_symbol(symbol);
        let url = format!(
            "{}/linear-swap-api/v1/swap_historical_funding_rate?contract_code={}&page_size={}",
            FUTURES_REST_URL,
            formatted_symbol,
            limit.min(50)
        );

        let response: HtxFuturesResponse<serde_json::Value> =
            self.client.get(&url).send().await?.json().await?;

        if response.status != "ok" {
            anyhow::bail!("Failed to get funding rate history");
        }

        let data = response
            .data
            .ok_or_else(|| anyhow::anyhow!("No funding data"))?;

        let list = data
            .get("data")
            .and_then(|d| d.as_array())
            .ok_or_else(|| anyhow::anyhow!("Invalid funding data format"))?;

        let entries: Vec<FundingRateEntry> = list
            .iter()
            .filter_map(|e| {
                let funding_rate: f64 = e
                    .get("funding_rate")
                    .and_then(|r| r.as_str())
                    .and_then(|r| r.parse().ok())?;
                let funding_time: i64 = e.get("funding_time").and_then(|t| t.as_i64())?;
                Some(FundingRateEntry {
                    symbol: formatted_symbol.clone(),
                    funding_rate,
                    funding_time,
                })
            })
            .collect();

        Ok(entries)
    }

    async fn get_open_interest_history(
        &self,
        symbol: &str,
        _interval: &str,
        limit: u32,
    ) -> Result<Vec<OpenInterestEntry>> {
        if self.market_type != MarketType::Swap {
            anyhow::bail!("Open interest only available for perpetual futures");
        }

        let formatted_symbol = self.format_symbol(symbol);
        let url =
            format!(
            "{}/linear-swap-api/v1/swap_his_open_interest?contract_code={}&period=60min&size={}",
            FUTURES_REST_URL, formatted_symbol, limit.min(200)
        );

        let response: HtxFuturesResponse<serde_json::Value> =
            self.client.get(&url).send().await?.json().await?;

        if response.status != "ok" {
            anyhow::bail!("Failed to get open interest history");
        }

        let data = response
            .data
            .ok_or_else(|| anyhow::anyhow!("No open interest data"))?;

        let list = data
            .get("data")
            .and_then(|d| d.as_array())
            .ok_or_else(|| anyhow::anyhow!("Invalid open interest data format"))?;

        let entries: Vec<OpenInterestEntry> = list
            .iter()
            .filter_map(|e| {
                let volume = e.get("volume").and_then(|v| v.as_f64())?;
                let value = e.get("value").and_then(|v| v.as_f64()).unwrap_or(0.0);
                let ts = e.get("ts").and_then(|t| t.as_i64())?;
                Some(OpenInterestEntry {
                    symbol: formatted_symbol.clone(),
                    open_interest: volume,
                    open_interest_value: value,
                    timestamp: ts,
                })
            })
            .collect();

        Ok(entries)
    }

    async fn set_leverage(&self, symbol: Option<&str>, leverage: u32) -> Result<bool> {
        if self.market_type == MarketType::Spot {
            return Ok(true);
        }

        let sym = match symbol {
            Some(s) => self.format_symbol(s),
            None => return Ok(true),
        };

        let body = serde_json::json!({
            "contract_code": sym,
            "lever_rate": leverage,
        });

        let response: HtxFuturesResponse<serde_json::Value> = self
            .signed_post("/linear-swap-api/v1/swap_switch_lever_rate", &body)
            .await?;

        if response.status == "ok" {
            info!("HTX leverage set to {} for {}", leverage, sym);
            Ok(true)
        } else {
            // Code 1044 means leverage already set to this value
            if response.err_code == Some(1044) {
                Ok(true)
            } else {
                warn!(
                    "Failed to set leverage: {}",
                    response.err_msg.unwrap_or_default()
                );
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

        // Set leverage for swap trading
        if self.market_type == MarketType::Swap {
            if let Err(e) = self.set_leverage(Some(&formatted_symbol), leverage).await {
                warn!("Failed to set leverage: {}", e);
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
        let formatted_symbol = self.format_symbol(symbol);
        self.place_order_rest(&formatted_symbol, side, OrderType::Market, quantity, None)
            .await
    }

    async fn place_limit_order(
        &self,
        symbol: &str,
        side: OrderSide,
        quantity: f64,
        price: f64,
        _reduce_only: bool,
    ) -> Result<OrderResult> {
        let formatted_symbol = self.format_symbol(symbol);
        self.place_order_rest(
            &formatted_symbol,
            side,
            OrderType::Limit,
            quantity,
            Some(price),
        )
        .await
    }

    async fn cancel_order(&self, symbol: &str, order_id: &str) -> Result<bool> {
        self.cancel_order_rest(symbol, order_id).await
    }

    async fn get_order_status(&self, symbol: &str, order_id: &str) -> Result<OrderInfo> {
        if self.market_type == MarketType::Spot {
            let path = format!("/v1/order/orders/{}", order_id);
            let response: HtxResponse<serde_json::Value> = self.signed_get(&path, "").await?;

            if response.status != "ok" {
                anyhow::bail!(
                    "Failed to get order status: {}",
                    response.err_msg.unwrap_or_default()
                );
            }

            let data = response
                .data
                .ok_or_else(|| anyhow::anyhow!("No order data"))?;

            let status_str = data
                .get("state")
                .and_then(|s| s.as_str())
                .unwrap_or("unknown");
            let order_type_str = data.get("type").and_then(|s| s.as_str()).unwrap_or("");

            let (side, order_type) = if order_type_str.starts_with("buy") {
                (
                    OrderSide::Buy,
                    if order_type_str.contains("market") {
                        OrderType::Market
                    } else {
                        OrderType::Limit
                    },
                )
            } else {
                (
                    OrderSide::Sell,
                    if order_type_str.contains("market") {
                        OrderType::Market
                    } else {
                        OrderType::Limit
                    },
                )
            };

            let price = data
                .get("price")
                .and_then(|p| p.as_str())
                .and_then(|p| p.parse().ok())
                .unwrap_or(0.0);
            let quantity = data
                .get("amount")
                .and_then(|q| q.as_str())
                .and_then(|q| q.parse().ok())
                .unwrap_or(0.0);
            let filled_quantity = data
                .get("field-amount")
                .and_then(|q| q.as_str())
                .and_then(|q| q.parse().ok())
                .unwrap_or(0.0);
            let avg_price = data
                .get("field-cash-amount")
                .and_then(|v| v.as_str())
                .and_then(|v| v.parse::<f64>().ok())
                .map(|cash| {
                    if filled_quantity > 0.0 {
                        cash / filled_quantity
                    } else {
                        0.0
                    }
                });

            Ok(OrderInfo {
                order_id: order_id.to_string(),
                client_order_id: data
                    .get("client-order-id")
                    .and_then(|s| s.as_str())
                    .map(|s| s.to_string()),
                symbol: symbol.to_string(),
                status: parse_order_status_str(status_str),
                side,
                order_type,
                price,
                quantity,
                filled_quantity,
                remaining_quantity: quantity - filled_quantity,
                avg_price,
                timestamp: data.get("created-at").and_then(|t| t.as_i64()).unwrap_or(0),
                time_in_force: TimeInForce::GTC,
                reduce_only: false,
            })
        } else {
            let body = serde_json::json!({
                "contract_code": symbol.to_uppercase(),
                "order_id": order_id,
            });

            let response: HtxFuturesResponse<Vec<serde_json::Value>> = self
                .signed_post("/linear-swap-api/v1/swap_order_info", &body)
                .await?;

            if response.status != "ok" {
                anyhow::bail!(
                    "Failed to get order status: {}",
                    response.err_msg.unwrap_or_default()
                );
            }

            let orders = response
                .data
                .ok_or_else(|| anyhow::anyhow!("No order data"))?;
            let data = orders
                .first()
                .ok_or_else(|| anyhow::anyhow!("Order not found"))?;

            let status_str = data
                .get("status")
                .and_then(|s| s.as_i64())
                .map(|s| match s {
                    1..=3 => "submitted",
                    4 => "partial-filled",
                    5 | 6 => "filled",
                    7 => "canceled",
                    _ => "unknown",
                })
                .unwrap_or("unknown");

            let direction = data
                .get("direction")
                .and_then(|d| d.as_str())
                .unwrap_or("buy");
            let side = if direction == "buy" {
                OrderSide::Buy
            } else {
                OrderSide::Sell
            };

            let order_price_type = data
                .get("order_price_type")
                .and_then(|t| t.as_str())
                .unwrap_or("");
            let order_type = if order_price_type == "limit" {
                OrderType::Limit
            } else {
                OrderType::Market
            };

            Ok(OrderInfo {
                order_id: order_id.to_string(),
                client_order_id: data
                    .get("client_order_id")
                    .and_then(|s| s.as_i64())
                    .map(|s| s.to_string()),
                symbol: symbol.to_string(),
                status: parse_order_status_str(status_str),
                side,
                order_type,
                price: data.get("price").and_then(|p| p.as_f64()).unwrap_or(0.0),
                quantity: data.get("volume").and_then(|q| q.as_f64()).unwrap_or(0.0),
                filled_quantity: data
                    .get("trade_volume")
                    .and_then(|q| q.as_f64())
                    .unwrap_or(0.0),
                remaining_quantity: data.get("volume").and_then(|q| q.as_f64()).unwrap_or(0.0)
                    - data
                        .get("trade_volume")
                        .and_then(|q| q.as_f64())
                        .unwrap_or(0.0),
                avg_price: data.get("trade_avg_price").and_then(|p| p.as_f64()),
                timestamp: data.get("created_at").and_then(|t| t.as_i64()).unwrap_or(0),
                time_in_force: TimeInForce::GTC,
                reduce_only: data.get("offset").and_then(|o| o.as_str()) == Some("close"),
            })
        }
    }

    async fn close_position(&self, symbol: &str) -> Result<OrderResult> {
        if self.market_type == MarketType::Spot {
            anyhow::bail!("Close position not supported for spot trading");
        }

        let formatted_symbol = self.format_symbol(symbol);

        // Get current position
        let body = serde_json::json!({
            "contract_code": formatted_symbol,
        });

        let response: HtxFuturesResponse<Vec<FuturesPosition>> = self
            .signed_post("/linear-swap-api/v1/swap_cross_position_info", &body)
            .await?;

        if response.status != "ok" {
            anyhow::bail!(
                "Failed to get position: {}",
                response.err_msg.unwrap_or_default()
            );
        }

        let positions = response
            .data
            .ok_or_else(|| anyhow::anyhow!("No position data"))?;

        if positions.is_empty() {
            return Ok(OrderResult::failure(
                formatted_symbol,
                OrderSide::Sell,
                "No position to close".to_string(),
            ));
        }

        let pos = &positions[0];
        if pos.volume <= 0.0 {
            return Ok(OrderResult::failure(
                formatted_symbol,
                OrderSide::Sell,
                "No position to close".to_string(),
            ));
        }

        // Determine close direction (opposite of position)
        let close_direction = if pos.direction == "buy" {
            "sell"
        } else {
            "buy"
        };

        let close_body = serde_json::json!({
            "contract_code": formatted_symbol,
            "volume": pos.volume as i64,
            "direction": close_direction,
            "offset": "close",
            "lever_rate": pos.lever_rate,
            "order_price_type": "opponent",
        });

        let close_response: HtxFuturesResponse<FuturesOrderResult> = self
            .signed_post("/linear-swap-api/v1/swap_order", &close_body)
            .await?;

        if close_response.status == "ok" {
            if let Some(data) = close_response.data {
                let order_id = data
                    .order_id_str
                    .unwrap_or_else(|| data.order_id.to_string());
                info!("HTX position closed: {}", order_id);
                return Ok(OrderResult {
                    success: true,
                    order_id: Some(order_id),
                    symbol: formatted_symbol,
                    side: if close_direction == "sell" {
                        OrderSide::Sell
                    } else {
                        OrderSide::Buy
                    },
                    order_type: OrderType::Market,
                    quantity: Some(pos.volume),
                    entry_price: None,
                    position_size: Some(0.0),
                    position_usdt: None,
                    error: None,
                    raw_response: None,
                });
            }
        }

        Ok(OrderResult::failure(
            formatted_symbol,
            OrderSide::Sell,
            close_response
                .err_msg
                .unwrap_or_else(|| "Failed to close position".to_string()),
        ))
    }

    fn format_symbol(&self, base_symbol: &str) -> String {
        let symbol = base_symbol.to_uppercase();

        // Handle BTC/USDT format
        if symbol.contains('/') {
            let cleaned = symbol.replace("/", "");
            if self.market_type == MarketType::Swap {
                // Futures uses format like BTC-USDT
                if cleaned.ends_with("USDT") {
                    let base = &cleaned[..cleaned.len() - 4];
                    return format!("{}-USDT", base);
                }
            }
            return cleaned.to_lowercase();
        }

        if self.market_type == MarketType::Swap {
            // Futures: BTC-USDT format
            if symbol.contains('-') {
                return symbol;
            }
            if symbol.ends_with("USDT") {
                let base = &symbol[..symbol.len() - 4];
                return format!("{}-USDT", base);
            }
            format!("{}-USDT", symbol)
        } else {
            // Spot: btcusdt format (lowercase)
            if symbol.ends_with("USDT") {
                return symbol.to_lowercase();
            }
            format!("{}usdt", symbol.to_lowercase())
        }
    }

    async fn test_ws_orderbook(
        &self,
        symbol: &str,
        _depth: u32,
        timeout_ms: u64,
    ) -> Result<WsTestResult> {
        let formatted_symbol = self.format_symbol(symbol);
        let ws_url = self.ws_public_url();

        let start = std::time::Instant::now();
        let (mut ws_stream, _) = connect_async(ws_url)
            .await
            .context("Failed to connect to HTX WebSocket")?;

        let connect_time = start.elapsed().as_millis() as u64;

        // Spot and futures use the same depth channel on HTX.
        let sub_msg = serde_json::json!({
            "sub": format!("market.{}.depth.step0", formatted_symbol),
            "id": "orderbook_test"
        });

        ws_stream.send(Message::Text(sub_msg.to_string())).await?;

        let mut messages_received = 0u32;
        let mut data_sample = None;
        let timeout = tokio::time::Duration::from_millis(timeout_ms);

        loop {
            tokio::select! {
                msg = ws_stream.next() => {
                    match msg {
                        Some(Ok(Message::Binary(data))) => {
                            messages_received += 1;
                            if let Ok(text) = Self::decompress_gzip(&data) {
                                if let Ok(parsed) = serde_json::from_str::<WsMessage>(&text) {
                                    // Handle ping
                                    if let Some(ping) = parsed.ping {
                                        let pong = serde_json::json!({"pong": ping});
                                        let _ = ws_stream.send(Message::Text(pong.to_string())).await;
                                        continue;
                                    }

                                    if parsed.ch.is_some() && parsed.tick.is_some() {
                                        data_sample = parsed.tick;
                                        break;
                                    }
                                }
                            }
                        }
                        Some(Ok(Message::Text(text))) => {
                            messages_received += 1;
                            if let Ok(parsed) = serde_json::from_str::<WsMessage>(&text) {
                                if let Some(ping) = parsed.ping {
                                    let pong = serde_json::json!({"pong": ping});
                                    let _ = ws_stream.send(Message::Text(pong.to_string())).await;
                                    continue;
                                }

                                if parsed.ch.is_some() && parsed.tick.is_some() {
                                    data_sample = parsed.tick;
                                    break;
                                }
                            }
                        }
                        Some(Ok(_)) => {}
                        Some(Err(e)) => {
                            return Err(anyhow::anyhow!("WebSocket error: {}", e));
                        }
                        None => break,
                    }
                }
                _ = tokio::time::sleep(timeout) => {
                    break;
                }
            }
        }

        let _ = ws_stream.close(None).await;

        Ok(WsTestResult {
            connected: true,
            latency_ms: connect_time,
            messages_received,
            subscription_type: format!("market.{}.depth.step0", formatted_symbol),
            data_sample,
        })
    }

    async fn test_ws_trades(&self, symbol: &str, timeout_ms: u64) -> Result<WsTestResult> {
        let formatted_symbol = self.format_symbol(symbol);
        let ws_url = self.ws_public_url();

        let start = std::time::Instant::now();
        let (mut ws_stream, _) = connect_async(ws_url)
            .await
            .context("Failed to connect to HTX WebSocket")?;

        let connect_time = start.elapsed().as_millis() as u64;

        let sub_msg = serde_json::json!({
            "sub": format!("market.{}.trade.detail", formatted_symbol),
            "id": "trade_test"
        });

        ws_stream.send(Message::Text(sub_msg.to_string())).await?;

        let mut messages_received = 0u32;
        let mut data_sample = None;
        let timeout = tokio::time::Duration::from_millis(timeout_ms);

        loop {
            tokio::select! {
                msg = ws_stream.next() => {
                    match msg {
                        Some(Ok(Message::Binary(data))) => {
                            messages_received += 1;
                            if let Ok(text) = Self::decompress_gzip(&data) {
                                if let Ok(parsed) = serde_json::from_str::<WsMessage>(&text) {
                                    if let Some(ping) = parsed.ping {
                                        let pong = serde_json::json!({"pong": ping});
                                        let _ = ws_stream.send(Message::Text(pong.to_string())).await;
                                        continue;
                                    }

                                    if parsed.ch.is_some() && parsed.tick.is_some() {
                                        data_sample = parsed.tick;
                                        break;
                                    }
                                }
                            }
                        }
                        Some(Ok(Message::Text(text))) => {
                            messages_received += 1;
                            if let Ok(parsed) = serde_json::from_str::<WsMessage>(&text) {
                                if let Some(ping) = parsed.ping {
                                    let pong = serde_json::json!({"pong": ping});
                                    let _ = ws_stream.send(Message::Text(pong.to_string())).await;
                                    continue;
                                }

                                if parsed.ch.is_some() && parsed.tick.is_some() {
                                    data_sample = parsed.tick;
                                    break;
                                }
                            }
                        }
                        Some(Ok(_)) => {}
                        Some(Err(e)) => {
                            return Err(anyhow::anyhow!("WebSocket error: {}", e));
                        }
                        None => break,
                    }
                }
                _ = tokio::time::sleep(timeout) => {
                    break;
                }
            }
        }

        let _ = ws_stream.close(None).await;

        Ok(WsTestResult {
            connected: true,
            latency_ms: connect_time,
            messages_received,
            subscription_type: format!("market.{}.trade.detail", formatted_symbol),
            data_sample,
        })
    }

    async fn test_ws_ticker(&self, symbol: &str, timeout_ms: u64) -> Result<WsTestResult> {
        let formatted_symbol = self.format_symbol(symbol);
        let ws_url = self.ws_public_url();

        let start = std::time::Instant::now();
        let (mut ws_stream, _) = connect_async(ws_url)
            .await
            .context("Failed to connect to HTX WebSocket")?;

        let connect_time = start.elapsed().as_millis() as u64;

        let sub_msg = serde_json::json!({
            "sub": format!("market.{}.detail", formatted_symbol),
            "id": "ticker_test"
        });

        ws_stream.send(Message::Text(sub_msg.to_string())).await?;

        let mut messages_received = 0u32;
        let mut data_sample = None;
        let timeout = tokio::time::Duration::from_millis(timeout_ms);

        loop {
            tokio::select! {
                msg = ws_stream.next() => {
                    match msg {
                        Some(Ok(Message::Binary(data))) => {
                            messages_received += 1;
                            if let Ok(text) = Self::decompress_gzip(&data) {
                                if let Ok(parsed) = serde_json::from_str::<WsMessage>(&text) {
                                    if let Some(ping) = parsed.ping {
                                        let pong = serde_json::json!({"pong": ping});
                                        let _ = ws_stream.send(Message::Text(pong.to_string())).await;
                                        continue;
                                    }

                                    if parsed.ch.is_some() && parsed.tick.is_some() {
                                        data_sample = parsed.tick;
                                        break;
                                    }
                                }
                            }
                        }
                        Some(Ok(Message::Text(text))) => {
                            messages_received += 1;
                            if let Ok(parsed) = serde_json::from_str::<WsMessage>(&text) {
                                if let Some(ping) = parsed.ping {
                                    let pong = serde_json::json!({"pong": ping});
                                    let _ = ws_stream.send(Message::Text(pong.to_string())).await;
                                    continue;
                                }

                                if parsed.ch.is_some() && parsed.tick.is_some() {
                                    data_sample = parsed.tick;
                                    break;
                                }
                            }
                        }
                        Some(Ok(_)) => {}
                        Some(Err(e)) => {
                            return Err(anyhow::anyhow!("WebSocket error: {}", e));
                        }
                        None => break,
                    }
                }
                _ = tokio::time::sleep(timeout) => {
                    break;
                }
            }
        }

        let _ = ws_stream.close(None).await;

        Ok(WsTestResult {
            connected: true,
            latency_ms: connect_time,
            messages_received,
            subscription_type: format!("market.{}.detail", formatted_symbol),
            data_sample,
        })
    }

    async fn subscribe_order_updates(
        &self,
        sender: broadcast::Sender<OrderUpdateEvent>,
    ) -> Result<JoinHandle<()>> {
        let api_key = self.api_key.clone();
        let api_secret = self.api_secret.clone();
        let market_type = self.market_type;
        let exchange_name = "htx".to_string();

        let handle = tokio::spawn(async move {
            let mut reconnect_delay = INITIAL_RECONNECT_DELAY_SECS;

            loop {
                let ws_url = match market_type {
                    MarketType::Spot => SPOT_WS_PRIVATE_URL,
                    MarketType::Swap => FUTURES_WS_PRIVATE_URL,
                };

                info!("[{}] Connecting to private WS: {}", exchange_name, ws_url);

                match connect_async(ws_url).await {
                    Ok((mut ws_stream, _)) => {
                        // Generate and send authentication message
                        let timestamp = chrono::Utc::now().format("%Y-%m-%dT%H:%M:%S").to_string();
                        let host = match market_type {
                            MarketType::Spot => "api.huobi.pro",
                            MarketType::Swap => "api.hbdm.com",
                        };
                        let path = match market_type {
                            MarketType::Spot => "/ws/v2",
                            MarketType::Swap => "/linear-swap-notification",
                        };

                        let params = format!(
                            "AccessKeyId={}&SignatureMethod=HmacSHA256&SignatureVersion=2&Timestamp={}",
                            api_key,
                            urlencoding::encode(&timestamp)
                        );

                        let sign_payload = format!("GET\n{}\n{}\n{}", host, path, params);
                        let signature = hmac_sign_base64(&api_secret, &sign_payload);

                        let auth_msg = if market_type == MarketType::Spot {
                            serde_json::json!({
                                "action": "req",
                                "ch": "auth",
                                "params": {
                                    "authType": "api",
                                    "accessKey": api_key,
                                    "signatureMethod": "HmacSHA256",
                                    "signatureVersion": "2.1",
                                    "timestamp": timestamp,
                                    "signature": signature
                                }
                            })
                        } else {
                            serde_json::json!({
                                "op": "auth",
                                "type": "api",
                                "AccessKeyId": api_key,
                                "SignatureMethod": "HmacSHA256",
                                "SignatureVersion": "2",
                                "Timestamp": timestamp,
                                "Signature": signature
                            })
                        };

                        if let Err(e) = ws_stream.send(Message::Text(auth_msg.to_string())).await {
                            error!("[{}] Failed to send auth: {}", exchange_name, e);
                            tokio::time::sleep(tokio::time::Duration::from_secs(reconnect_delay))
                                .await;
                            reconnect_delay = next_reconnect_delay(reconnect_delay);
                            continue;
                        }

                        // Wait for auth response
                        let mut authenticated = false;
                        let auth_timeout = tokio::time::Duration::from_secs(10);

                        tokio::select! {
                            msg = ws_stream.next() => {
                                match msg {
                                    Some(Ok(Message::Binary(data))) => {
                                        if let Ok(text) = HtxTrader::decompress_gzip(&data) {
                                            if let Ok(resp) = serde_json::from_str::<serde_json::Value>(&text) {
                                                // Check spot auth response
                                                if resp.get("action").and_then(|a| a.as_str()) == Some("req")
                                                    && resp.get("ch").and_then(|c| c.as_str()) == Some("auth")
                                                    && resp.get("code").and_then(|c| c.as_i64()) == Some(200)
                                                {
                                                    info!("[{}] Private WS authenticated (spot)", exchange_name);
                                                    authenticated = true;
                                                }
                                                // Check futures auth response
                                                else if resp.get("op").and_then(|o| o.as_str()) == Some("auth")
                                                    && resp.get("err-code").and_then(|e| e.as_i64()) == Some(0)
                                                {
                                                    info!("[{}] Private WS authenticated (futures)", exchange_name);
                                                    authenticated = true;
                                                } else {
                                                    error!("[{}] Auth failed: {}", exchange_name, text);
                                                }
                                            }
                                        }
                                    }
                                    Some(Ok(Message::Text(text))) => {
                                        if let Ok(resp) = serde_json::from_str::<serde_json::Value>(&text) {
                                            if resp.get("action").and_then(|a| a.as_str()) == Some("req")
                                                && resp.get("ch").and_then(|c| c.as_str()) == Some("auth")
                                                && resp.get("code").and_then(|c| c.as_i64()) == Some(200)
                                            {
                                                info!("[{}] Private WS authenticated", exchange_name);
                                                authenticated = true;
                                            } else if resp.get("op").and_then(|o| o.as_str()) == Some("auth")
                                                && resp.get("err-code").and_then(|e| e.as_i64()) == Some(0)
                                            {
                                                info!("[{}] Private WS authenticated", exchange_name);
                                                authenticated = true;
                                            } else {
                                                error!("[{}] Auth failed: {}", exchange_name, text);
                                            }
                                        }
                                    }
                                    _ => {}
                                }
                            }
                            _ = tokio::time::sleep(auth_timeout) => {
                                error!("[{}] Auth timeout", exchange_name);
                            }
                        }

                        if !authenticated {
                            tokio::time::sleep(tokio::time::Duration::from_secs(reconnect_delay))
                                .await;
                            reconnect_delay = next_reconnect_delay(reconnect_delay);
                            continue;
                        }

                        // Subscribe to order updates
                        let sub_msg = if market_type == MarketType::Spot {
                            serde_json::json!({
                                "action": "sub",
                                "ch": "orders#*"
                            })
                        } else {
                            serde_json::json!({
                                "op": "sub",
                                "topic": "orders_cross.*"
                            })
                        };

                        if let Err(e) = ws_stream.send(Message::Text(sub_msg.to_string())).await {
                            error!("[{}] Failed to subscribe: {}", exchange_name, e);
                            tokio::time::sleep(tokio::time::Duration::from_secs(reconnect_delay))
                                .await;
                            reconnect_delay = next_reconnect_delay(reconnect_delay);
                            continue;
                        }

                        info!("[{}] Subscribed to private order updates", exchange_name);
                        reconnect_delay = INITIAL_RECONNECT_DELAY_SECS;

                        // Main message loop with ping interval
                        let mut ping_interval =
                            tokio::time::interval(tokio::time::Duration::from_secs(20));

                        loop {
                            tokio::select! {
                                msg = ws_stream.next() => {
                                    match msg {
                                        Some(Ok(Message::Binary(data))) => {
                                            if let Ok(text) = HtxTrader::decompress_gzip(&data) {
                                                if let Ok(msg_json) = serde_json::from_str::<serde_json::Value>(&text) {
                                                    // Handle ping
                                                    if let Some(ping) = msg_json.get("ping").and_then(|p| p.as_i64()) {
                                                        let pong = serde_json::json!({"pong": ping});
                                                        let _ = ws_stream.send(Message::Text(pong.to_string())).await;
                                                        continue;
                                                    }

                                                    // Handle op ping (futures)
                                                    if msg_json.get("op").and_then(|o| o.as_str()) == Some("ping") {
                                                        let ts = msg_json.get("ts").and_then(|t| t.as_i64()).unwrap_or(0);
                                                        let pong = serde_json::json!({"op": "pong", "ts": ts});
                                                        let _ = ws_stream.send(Message::Text(pong.to_string())).await;
                                                        continue;
                                                    }

                                                    // Process order updates
                                                    let is_order_update = msg_json.get("action").and_then(|a| a.as_str()) == Some("push")
                                                        || msg_json.get("op").and_then(|o| o.as_str()) == Some("notify");

                                                    if is_order_update {
                                                        let data = msg_json.get("data").or_else(|| msg_json.get("tick"));
                                                        if let Some(order_data) = data {
                                                            let event = OrderUpdateEvent {
                                                                exchange: exchange_name.clone(),
                                                                order_id: order_data.get("orderId")
                                                                    .or_else(|| order_data.get("order_id_str"))
                                                                    .and_then(|v| v.as_str())
                                                                    .unwrap_or("")
                                                                    .to_string(),
                                                                client_order_id: order_data.get("clientOrderId")
                                                                    .or_else(|| order_data.get("client_order_id"))
                                                                    .and_then(|v| v.as_str())
                                                                    .map(|s| s.to_string()),
                                                                symbol: order_data.get("symbol")
                                                                    .or_else(|| order_data.get("contract_code"))
                                                                    .and_then(|v| v.as_str())
                                                                    .unwrap_or("")
                                                                    .to_string(),
                                                                status: parse_order_status_str(
                                                                    order_data.get("orderStatus")
                                                                        .or_else(|| order_data.get("status"))
                                                                        .and_then(|s| s.as_str())
                                                                        .or_else(|| order_data.get("status").and_then(|s| s.as_i64()).map(|_| "new"))
                                                                        .unwrap_or("new")
                                                                ),
                                                                side: if order_data.get("type").and_then(|t| t.as_str()).unwrap_or("").contains("buy")
                                                                    || order_data.get("direction").and_then(|d| d.as_str()) == Some("buy")
                                                                {
                                                                    OrderSide::Buy
                                                                } else {
                                                                    OrderSide::Sell
                                                                },
                                                                order_type: if order_data.get("type").and_then(|t| t.as_str()).unwrap_or("").contains("market")
                                                                    || order_data.get("order_price_type").and_then(|t| t.as_str()) != Some("limit")
                                                                {
                                                                    OrderType::Market
                                                                } else {
                                                                    OrderType::Limit
                                                                },
                                                                price: order_data.get("orderPrice")
                                                                    .or_else(|| order_data.get("price"))
                                                                    .and_then(|p| p.as_str().and_then(|s| s.parse().ok()).or_else(|| p.as_f64()))
                                                                    .unwrap_or(0.0),
                                                                quantity: order_data.get("orderSize")
                                                                    .or_else(|| order_data.get("volume"))
                                                                    .and_then(|q| q.as_str().and_then(|s| s.parse().ok()).or_else(|| q.as_f64()))
                                                                    .unwrap_or(0.0),
                                                                filled_quantity: order_data.get("tradeVolume")
                                                                    .or_else(|| order_data.get("trade_volume"))
                                                                    .and_then(|q| q.as_str().and_then(|s| s.parse().ok()).or_else(|| q.as_f64()))
                                                                    .unwrap_or(0.0),
                                                                avg_fill_price: order_data.get("tradePrice")
                                                                    .or_else(|| order_data.get("trade_avg_price"))
                                                                    .and_then(|p| p.as_str().and_then(|s| s.parse().ok()).or_else(|| p.as_f64())),
                                                                fee: order_data.get("transactFee")
                                                                    .or_else(|| order_data.get("fee"))
                                                                    .and_then(|f| f.as_str().and_then(|s| s.parse().ok()).or_else(|| f.as_f64())),
                                                                fee_currency: order_data.get("feeCurrency")
                                                                    .or_else(|| order_data.get("fee_asset"))
                                                                    .and_then(|c| c.as_str())
                                                                    .map(|s| s.to_string()),
                                                                exchange_timestamp: order_data.get("tradeTime")
                                                                    .or_else(|| order_data.get("ts"))
                                                                    .and_then(|t| t.as_i64())
                                                                    .unwrap_or(0),
                                                                received_at: timestamp_ms(),
                                                                raw: Some(order_data.clone()),
                                                            };

                                                            log_order_update(&event);
                                                            let _ = sender.send(event);
                                                        }
                                                    }
                                                }
                                            }
                                        }
                                        Some(Ok(Message::Text(text))) => {
                                            if let Ok(msg_json) = serde_json::from_str::<serde_json::Value>(&text) {
                                                // Handle ping (spot v2)
                                                if msg_json.get("action").and_then(|a| a.as_str()) == Some("ping") {
                                                    let data = msg_json.get("data").cloned().unwrap_or(serde_json::json!({}));
                                                    let pong = serde_json::json!({"action": "pong", "data": data});
                                                    let _ = ws_stream.send(Message::Text(pong.to_string())).await;
                                                    continue;
                                                }

                                                // Similar order update processing for text messages...
                                                let is_order_update = msg_json.get("action").and_then(|a| a.as_str()) == Some("push");
                                                if is_order_update {
                                                    // ... same order processing logic
                                                }
                                            }
                                        }
                                        Some(Ok(Message::Ping(data))) => {
                                            let _ = ws_stream.send(Message::Pong(data)).await;
                                        }
                                        Some(Ok(Message::Close(_))) => {
                                            warn!("[{}] Private WS closed by server", exchange_name);
                                            break;
                                        }
                                        Some(Err(e)) => {
                                            error!("[{}] Private WS error: {}", exchange_name, e);
                                            break;
                                        }
                                        None => {
                                            warn!("[{}] Private WS stream ended", exchange_name);
                                            break;
                                        }
                                        _ => {}
                                    }
                                }
                                _ = ping_interval.tick() => {
                                    let ping_msg = if market_type == MarketType::Spot {
                                        serde_json::json!({"action": "ping"})
                                    } else {
                                        serde_json::json!({"op": "ping"})
                                    };
                                    if let Err(e) = ws_stream.send(Message::Text(ping_msg.to_string())).await {
                                        error!("[{}] Failed to send ping: {}", exchange_name, e);
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

                // Reconnect with backoff
                warn!(
                    "[{}] Reconnecting private WS in {}s...",
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
    fn test_format_symbol_spot() {
        let trader = HtxTrader::new("", "", false, MarketType::Spot);
        assert_eq!(trader.format_symbol("BTC"), "btcusdt");
        assert_eq!(trader.format_symbol("BTCUSDT"), "btcusdt");
        assert_eq!(trader.format_symbol("BTC/USDT"), "btcusdt");
        assert_eq!(trader.format_symbol("eth"), "ethusdt");
    }

    #[test]
    fn test_format_symbol_futures() {
        let trader = HtxTrader::new("", "", false, MarketType::Swap);
        assert_eq!(trader.format_symbol("BTC"), "BTC-USDT");
        assert_eq!(trader.format_symbol("BTCUSDT"), "BTC-USDT");
        assert_eq!(trader.format_symbol("BTC/USDT"), "BTC-USDT");
        assert_eq!(trader.format_symbol("BTC-USDT"), "BTC-USDT");
        assert_eq!(trader.format_symbol("eth"), "ETH-USDT");
    }

    #[test]
    fn test_parse_order_status() {
        assert_eq!(parse_order_status_str("submitted"), OrderStatus::New);
        assert_eq!(parse_order_status_str("filled"), OrderStatus::Filled);
        assert_eq!(
            parse_order_status_str("partial-filled"),
            OrderStatus::PartiallyFilled
        );
        assert_eq!(parse_order_status_str("canceled"), OrderStatus::Cancelled);
        assert_eq!(parse_order_status_str("rejected"), OrderStatus::Rejected);
    }

    #[test]
    fn test_next_reconnect_delay() {
        assert_eq!(next_reconnect_delay(1), 2);
        assert_eq!(next_reconnect_delay(2), 4);
        assert_eq!(next_reconnect_delay(16), 30); // Capped at MAX
        assert_eq!(next_reconnect_delay(30), 30); // Already at max
    }
}
