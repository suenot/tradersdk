//! Gate.io exchange trader implementation.
//!
//! Gate.io API v4 implementation with HMAC-SHA512 signing.

use anyhow::{Context, Result};
use async_trait::async_trait;
use futures_util::{SinkExt, StreamExt};
use hmac::{Hmac, Mac};
use reqwest::Client;
use serde::{Deserialize, Serialize};
use sha2::Sha512;
use tokio::sync::broadcast;
use tokio::task::JoinHandle;
use tokio_tungstenite::{connect_async, tungstenite::protocol::Message};
use tracing::{debug, error, info, warn};
use tradersdk_core::models::{
    AccountBalance, ExchangeCapabilities, FundingRateEntry, MarketType, OpenInterestEntry,
    OrderInfo, OrderResult, OrderSide, OrderStatus, OrderType, OrderUpdateEvent, Orderbook,
    OrderbookLevel, TickerInfo, TimeInForce, WsTestResult,
};
use tradersdk_core::utils::timestamp_ms;
use tradersdk_core::ExchangeTrader;

const SPOT_MAINNET_URL: &str = "https://api.gateio.ws/api/v4";
const FUTURES_MAINNET_URL: &str = "https://api.gateio.ws/api/v4";
const SPOT_WS_URL: &str = "wss://api.gateio.ws/ws/v4/";
const FUTURES_WS_URL: &str = "wss://fx-ws.gateio.ws/v4/ws/usdt";

// WebSocket URLs for trading (order operations)
const SPOT_WS_TRADE_URL: &str = "wss://api.gateio.ws/ws/v4/";
const FUTURES_WS_TRADE_URL: &str = "wss://fx-ws.gateio.ws/v4/ws/usdt";

// Private WebSocket reconnection constants
const MAX_RECONNECT_DELAY_SECS: u64 = 30;
const INITIAL_RECONNECT_DELAY_SECS: u64 = 1;

/// Gate.io exchange trader.
pub struct GateTrader {
    api_key: String,
    api_secret: String,
    _testnet: bool,
    market_type: MarketType,
    client: Client,
    base_url: String,
}

impl GateTrader {
    /// Create a new Gate.io trader.
    pub fn new(api_key: &str, api_secret: &str, testnet: bool, market_type: MarketType) -> Self {
        let base_url = match market_type {
            MarketType::Spot => SPOT_MAINNET_URL,
            MarketType::Swap => FUTURES_MAINNET_URL,
        };

        Self {
            api_key: api_key.to_string(),
            api_secret: api_secret.to_string(),
            _testnet: testnet,
            market_type,
            client: Client::new(),
            base_url: base_url.to_string(),
        }
    }

    /// Generate Gate.io API v4 signature (HMAC-SHA512).
    ///
    /// Sign string format: method\nurl\nquery_string\nhashed_payload\ntimestamp
    fn sign(&self, method: &str, url: &str, query_string: &str, body: &str) -> (String, String) {
        use sha2::Digest;

        let timestamp = (timestamp_ms() / 1000).to_string();

        // Hash body/payload with SHA512 (empty string if no body)
        let body_hash = hex::encode(sha2::Sha512::digest(body.as_bytes()));

        // Create sign string: METHOD\n/api/v4/path\nquery_string\nhashed_payload\ntimestamp
        let sign_string = format!(
            "{}\n{}\n{}\n{}\n{}",
            method, url, query_string, body_hash, timestamp
        );

        debug!("Gate.io sign string: {}", sign_string.replace("\n", "\\n"));

        // HMAC-SHA512 signature
        type HmacSha512 = Hmac<Sha512>;
        let mut mac = HmacSha512::new_from_slice(self.api_secret.as_bytes())
            .expect("HMAC can take key of any size");
        mac.update(sign_string.as_bytes());
        let signature = hex::encode(mac.finalize().into_bytes());

        (signature, timestamp)
    }

    /// Make a signed GET request.
    async fn signed_get<T: for<'de> Deserialize<'de>>(
        &self,
        endpoint: &str,
        query: &str,
    ) -> Result<T> {
        let url_path = endpoint;
        let (signature, timestamp) = self.sign("GET", url_path, query, "");

        let full_url = if query.is_empty() {
            format!("{}{}", self.base_url, endpoint)
        } else {
            format!("{}{}?{}", self.base_url, endpoint, query)
        };

        let response = self
            .client
            .get(&full_url)
            .header("KEY", &self.api_key)
            .header("SIGN", signature)
            .header("Timestamp", timestamp)
            .header("Content-Type", "application/json")
            .send()
            .await
            .context("Failed to send request to Gate.io")?;

        let text = response.text().await?;
        debug!("Gate.io response: {}", text);

        serde_json::from_str(&text).context(format!("Failed to parse Gate.io response: {}", text))
    }

    /// Make a signed POST request.
    async fn signed_post<T: for<'de> Deserialize<'de>>(
        &self,
        endpoint: &str,
        body: &serde_json::Value,
    ) -> Result<T> {
        let url_path = endpoint;
        let body_str = serde_json::to_string(body)?;
        let (signature, timestamp) = self.sign("POST", url_path, "", &body_str);

        let response = self
            .client
            .post(format!("{}{}", self.base_url, endpoint))
            .header("KEY", &self.api_key)
            .header("SIGN", signature)
            .header("Timestamp", timestamp)
            .header("Content-Type", "application/json")
            .body(body_str)
            .send()
            .await
            .context("Failed to send request to Gate.io")?;

        let text = response.text().await?;
        debug!("Gate.io response: {}", text);

        serde_json::from_str(&text).context(format!("Failed to parse Gate.io response: {}", text))
    }

    /// Make a signed DELETE request.
    #[allow(dead_code)]
    async fn signed_delete<T: for<'de> Deserialize<'de>>(
        &self,
        endpoint: &str,
        query: &str,
    ) -> Result<T> {
        let url_path = endpoint;
        let (signature, timestamp) = self.sign("DELETE", url_path, query, "");

        let full_url = if query.is_empty() {
            format!("{}{}", self.base_url, endpoint)
        } else {
            format!("{}{}?{}", self.base_url, endpoint, query)
        };

        let response = self
            .client
            .delete(&full_url)
            .header("KEY", &self.api_key)
            .header("SIGN", signature)
            .header("Timestamp", timestamp)
            .header("Content-Type", "application/json")
            .send()
            .await
            .context("Failed to send request to Gate.io")?;

        let text = response.text().await?;
        debug!("Gate.io response: {}", text);

        serde_json::from_str(&text).context(format!("Failed to parse Gate.io response: {}", text))
    }

    /// Generate WebSocket authentication signature.
    /// Sign string format: channel=<channel>&event=<event>&time=<time>
    fn ws_auth_signature(&self, channel: &str, event: &str, time: i64) -> String {
        let sign_string = format!("channel={}&event={}&time={}", channel, event, time);

        type HmacSha512 = Hmac<Sha512>;
        let mut mac = HmacSha512::new_from_slice(self.api_secret.as_bytes())
            .expect("HMAC can take key of any size");
        mac.update(sign_string.as_bytes());
        hex::encode(mac.finalize().into_bytes())
    }

    /// Get WebSocket trade URL based on market type.
    fn ws_trade_url(&self) -> &str {
        match self.market_type {
            MarketType::Spot => SPOT_WS_TRADE_URL,
            MarketType::Swap => FUTURES_WS_TRADE_URL,
        }
    }

    /// Create and authenticate WebSocket connection for trading.
    async fn create_ws_trade_connection(
        &self,
    ) -> Result<
        tokio_tungstenite::WebSocketStream<
            tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>,
        >,
    > {
        let ws_url = self.ws_trade_url();
        let (ws_stream, _) = connect_async(ws_url)
            .await
            .context("Failed to connect to Gate.io trade WebSocket")?;

        info!("Gate.io WebSocket connected to {}", ws_url);

        // Gate.io doesn't require explicit auth handshake like Bybit
        // Auth is sent with each authenticated request
        Ok(ws_stream)
    }

    /// Send ping to keep WebSocket connection alive.
    #[allow(dead_code)]
    async fn send_ws_ping(
        ws_stream: &mut tokio_tungstenite::WebSocketStream<
            tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>,
        >,
        market_type: MarketType,
    ) -> Result<()> {
        let channel = match market_type {
            MarketType::Spot => "spot.ping",
            MarketType::Swap => "futures.ping",
        };

        let ping_msg = WsRequest {
            time: timestamp_ms() / 1000,
            id: None,
            channel: channel.to_string(),
            event: "ping".to_string(),
            payload: None,
            auth: None,
        };

        let ping_json = serde_json::to_string(&ping_msg)?;
        ws_stream.send(Message::Text(ping_json)).await?;
        Ok(())
    }

    /// Place order via WebSocket.
    async fn place_order_ws(
        &self,
        symbol: &str,
        side: OrderSide,
        order_type: OrderType,
        quantity: f64,
        price: Option<f64>,
        reduce_only: bool,
    ) -> Result<OrderResult> {
        // Create WS connection
        let mut ws_stream = self.create_ws_trade_connection().await?;

        let time = timestamp_ms() / 1000;
        let req_id = format!("order_{}", timestamp_ms());

        let (channel, event, payload) = if self.market_type == MarketType::Spot {
            // Spot order
            let side_str = match side {
                OrderSide::Buy => "buy",
                OrderSide::Sell => "sell",
            };

            let order_params = SpotOrderParams {
                text: Some(req_id.clone()),
                currency_pair: symbol.to_string(),
                order_type: match order_type {
                    OrderType::Market => "market".to_string(),
                    OrderType::Limit => "limit".to_string(),
                    _ => "limit".to_string(),
                },
                account: None,
                side: side_str.to_string(),
                amount: format!("{:.8}", quantity),
                price: price.map(|p| format!("{:.8}", p)),
                time_in_force: match order_type {
                    OrderType::Market => Some("ioc".to_string()),
                    OrderType::Limit => Some("gtc".to_string()),
                    _ => Some("gtc".to_string()),
                },
                iceberg: None,
                auto_borrow: None,
                auto_repay: None,
            };

            (
                "spot.order_place",
                "api",
                serde_json::to_value(order_params)?,
            )
        } else {
            // Futures order
            let size = match side {
                OrderSide::Buy => quantity as i64,
                OrderSide::Sell => -(quantity as i64),
            };

            let order_params = FuturesOrderParams {
                contract: symbol.to_string(),
                size,
                price: match order_type {
                    OrderType::Market => Some("0".to_string()),
                    OrderType::Limit => price.map(|p| format!("{:.8}", p)),
                    _ => price.map(|p| format!("{:.8}", p)),
                },
                tif: match order_type {
                    OrderType::Market => Some("ioc".to_string()),
                    OrderType::Limit => Some("gtc".to_string()),
                    _ => Some("gtc".to_string()),
                },
                text: Some(req_id.clone()),
                reduce_only: if reduce_only { Some(true) } else { None },
                close: None,
                iceberg: None,
                auto_size: None,
            };

            (
                "futures.order_place",
                "api",
                serde_json::to_value(order_params)?,
            )
        };

        // Generate auth signature
        let signature = self.ws_auth_signature(channel, event, time);
        let auth = WsAuth {
            method: "api_key".to_string(),
            key: self.api_key.clone(),
            sign: signature,
        };

        // Build request
        let order_req = WsRequest {
            time,
            id: Some(req_id.clone()),
            channel: channel.to_string(),
            event: event.to_string(),
            payload: Some(payload),
            auth: Some(auth),
        };

        let order_json = serde_json::to_string(&order_req)?;
        debug!("Sending Gate.io WS order: {}", order_json);

        // Send order
        ws_stream.send(Message::Text(order_json)).await?;

        // Wait for response
        let timeout = tokio::time::Duration::from_secs(10);
        let mut order_result = None;

        tokio::select! {
            msg = ws_stream.next() => {
                match msg {
                    Some(Ok(Message::Text(text))) => {
                        debug!("Gate.io order response: {}", text);
                        let resp: WsResponse = serde_json::from_str(&text)
                            .context("Failed to parse order response")?;

                        if let Some(error) = resp.error {
                            let error_msg = error.message.unwrap_or_else(|| "Unknown error".to_string());
                            order_result = Some(OrderResult::failure(
                                symbol.to_string(),
                                side,
                                format!("Code {}: {}", error.code.unwrap_or(0), error_msg),
                            ));
                        } else if let Some(result) = resp.result {
                            // Parse order ID from result
                            let order_id = if self.market_type == MarketType::Spot {
                                result["id"].as_str().map(|s| s.to_string())
                            } else {
                                result["id"].as_i64().map(|id| id.to_string())
                            };

                            if let Some(id) = order_id {
                                info!("Gate.io WS order placed: {}", id);
                                order_result = Some(OrderResult {
                                    success: true,
                                    order_id: Some(id),
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
                                order_result = Some(OrderResult::failure(
                                    symbol.to_string(),
                                    side,
                                    "No order ID in response".to_string(),
                                ));
                            }
                        }
                    }
                    Some(Ok(Message::Ping(data))) => {
                        let _ = ws_stream.send(Message::Pong(data)).await;
                    }
                    Some(Ok(_)) => {
                        order_result = Some(OrderResult::failure(
                            symbol.to_string(),
                            side,
                            "Unexpected message type".to_string(),
                        ));
                    }
                    Some(Err(e)) => {
                        order_result = Some(OrderResult::failure(
                            symbol.to_string(),
                            side,
                            format!("WebSocket error: {}", e),
                        ));
                    }
                    None => {
                        order_result = Some(OrderResult::failure(
                            symbol.to_string(),
                            side,
                            "WebSocket closed".to_string(),
                        ));
                    }
                }
            }
            _ = tokio::time::sleep(timeout) => {
                order_result = Some(OrderResult::failure(
                    symbol.to_string(),
                    side,
                    "Order timeout".to_string(),
                ));
            }
        }

        // Close connection
        let _ = ws_stream.close(None).await;

        order_result.ok_or_else(|| anyhow::anyhow!("No order result received"))
    }

    /// Cancel order via WebSocket.
    async fn cancel_order_ws(&self, symbol: &str, order_id: &str) -> Result<bool> {
        // Create WS connection
        let mut ws_stream = self.create_ws_trade_connection().await?;

        let time = timestamp_ms() / 1000;
        let req_id = format!("cancel_{}", timestamp_ms());

        let (channel, event, payload) = if self.market_type == MarketType::Spot {
            let cancel_params = OrderCancelParams {
                order_id: Some(order_id.to_string()),
                currency_pair: Some(symbol.to_string()),
                contract: None,
            };
            (
                "spot.order_cancel",
                "api",
                serde_json::to_value(cancel_params)?,
            )
        } else {
            let cancel_params = OrderCancelParams {
                order_id: Some(order_id.to_string()),
                currency_pair: None,
                contract: Some(symbol.to_string()),
            };
            (
                "futures.order_cancel",
                "api",
                serde_json::to_value(cancel_params)?,
            )
        };

        // Generate auth signature
        let signature = self.ws_auth_signature(channel, event, time);
        let auth = WsAuth {
            method: "api_key".to_string(),
            key: self.api_key.clone(),
            sign: signature,
        };

        // Build request
        let cancel_req = WsRequest {
            time,
            id: Some(req_id),
            channel: channel.to_string(),
            event: event.to_string(),
            payload: Some(payload),
            auth: Some(auth),
        };

        let cancel_json = serde_json::to_string(&cancel_req)?;
        debug!("Sending Gate.io WS cancel: {}", cancel_json);

        // Send cancel request
        ws_stream.send(Message::Text(cancel_json)).await?;

        // Wait for response
        let timeout = tokio::time::Duration::from_secs(10);
        let mut success = false;

        tokio::select! {
            msg = ws_stream.next() => {
                match msg {
                    Some(Ok(Message::Text(text))) => {
                        debug!("Gate.io cancel response: {}", text);
                        let resp: WsResponse = serde_json::from_str(&text)
                            .context("Failed to parse cancel response")?;

                        if resp.error.is_none() && resp.result.is_some() {
                            info!("Gate.io WS order cancelled: {}", order_id);
                            success = true;
                        } else if let Some(error) = resp.error {
                            let error_msg = error.message.unwrap_or_else(|| "Unknown error".to_string());
                            error!("Failed to cancel order {}: {}", order_id, error_msg);
                        }
                    }
                    Some(Ok(Message::Ping(data))) => {
                        let _ = ws_stream.send(Message::Pong(data)).await;
                    }
                    Some(Ok(_)) => error!("Unexpected message type during cancel"),
                    Some(Err(e)) => error!("WebSocket error during cancel: {}", e),
                    None => error!("WebSocket closed during cancel"),
                }
            }
            _ = tokio::time::sleep(timeout) => {
                error!("Cancel order timeout for {}", order_id);
            }
        }

        // Close connection
        let _ = ws_stream.close(None).await;

        Ok(success)
    }

    /// Amend (modify) order via WebSocket.
    #[allow(dead_code)]
    async fn amend_order_ws(
        &self,
        symbol: &str,
        order_id: &str,
        new_quantity: Option<f64>,
        new_price: Option<f64>,
    ) -> Result<bool> {
        // Create WS connection
        let mut ws_stream = self.create_ws_trade_connection().await?;

        let time = timestamp_ms() / 1000;
        let req_id = format!("amend_{}", timestamp_ms());

        let (channel, event, payload) = if self.market_type == MarketType::Spot {
            // For spot, we need to build amend params
            let mut amend_data = serde_json::json!({
                "order_id": order_id,
                "currency_pair": symbol,
            });

            if let Some(qty) = new_quantity {
                amend_data["amount"] = serde_json::json!(format!("{:.8}", qty));
            }
            if let Some(price) = new_price {
                amend_data["price"] = serde_json::json!(format!("{:.8}", price));
            }

            ("spot.order_amend", "api", amend_data)
        } else {
            // For futures
            let mut amend_data = serde_json::json!({
                "order_id": order_id,
            });

            if let Some(qty) = new_quantity {
                amend_data["size"] = serde_json::json!(qty as i64);
            }
            if let Some(price) = new_price {
                amend_data["price"] = serde_json::json!(format!("{:.8}", price));
            }

            ("futures.order_amend", "api", amend_data)
        };

        // Generate auth signature
        let signature = self.ws_auth_signature(channel, event, time);
        let auth = WsAuth {
            method: "api_key".to_string(),
            key: self.api_key.clone(),
            sign: signature,
        };

        // Build request
        let amend_req = WsRequest {
            time,
            id: Some(req_id),
            channel: channel.to_string(),
            event: event.to_string(),
            payload: Some(payload),
            auth: Some(auth),
        };

        let amend_json = serde_json::to_string(&amend_req)?;
        debug!("Sending Gate.io WS amend: {}", amend_json);

        // Send amend request
        ws_stream.send(Message::Text(amend_json)).await?;

        // Wait for response
        let timeout = tokio::time::Duration::from_secs(10);
        let mut success = false;

        tokio::select! {
            msg = ws_stream.next() => {
                match msg {
                    Some(Ok(Message::Text(text))) => {
                        debug!("Gate.io amend response: {}", text);
                        let resp: WsResponse = serde_json::from_str(&text)
                            .context("Failed to parse amend response")?;

                        if resp.error.is_none() && resp.result.is_some() {
                            info!("Gate.io WS order amended: {}", order_id);
                            success = true;
                        } else if let Some(error) = resp.error {
                            let error_msg = error.message.unwrap_or_else(|| "Unknown error".to_string());
                            error!("Failed to amend order {}: {}", order_id, error_msg);
                        }
                    }
                    Some(Ok(Message::Ping(data))) => {
                        let _ = ws_stream.send(Message::Pong(data)).await;
                    }
                    Some(Ok(_)) => error!("Unexpected message type during amend"),
                    Some(Err(e)) => error!("WebSocket error during amend: {}", e),
                    None => error!("WebSocket closed during amend"),
                }
            }
            _ = tokio::time::sleep(timeout) => {
                error!("Amend order timeout for {}", order_id);
            }
        }

        // Close connection
        let _ = ws_stream.close(None).await;

        Ok(success)
    }
}

// ============================================================================
// Internal response/request types
// ============================================================================

#[derive(Debug, Deserialize)]
struct SpotAccountBalance {
    currency: String,
    available: String,
    locked: String,
}

#[derive(Debug, Deserialize)]
struct FuturesAccountBalance {
    total: String,
    available: String,
    currency: String,
}

#[derive(Debug, Deserialize)]
struct SpotOrderResponse {
    id: String,
    currency_pair: String,
    #[serde(rename = "type")]
    order_type: String,
    side: String,
    amount: String,
    price: Option<String>,
    avg_deal_price: Option<String>,
    filled_total: Option<String>,
}

#[derive(Debug, Deserialize)]
struct FuturesOrderResponse {
    id: i64,
    contract: String,
    size: i64,
    price: Option<String>,
    fill_price: Option<String>,
}

/// WebSocket authentication object.
#[derive(Debug, Serialize)]
struct WsAuth {
    method: String,
    #[serde(rename = "KEY")]
    key: String,
    #[serde(rename = "SIGN")]
    sign: String,
}

/// WebSocket request structure for Gate.io.
#[derive(Debug, Serialize)]
struct WsRequest {
    time: i64,
    #[serde(skip_serializing_if = "Option::is_none")]
    id: Option<String>,
    channel: String,
    event: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    payload: Option<serde_json::Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    auth: Option<WsAuth>,
}

/// WebSocket response structure from Gate.io.
#[derive(Debug, Deserialize)]
struct WsResponse {
    #[serde(rename = "time")]
    _time: Option<i64>,
    #[serde(rename = "time_ms")]
    _time_ms: Option<i64>,
    #[serde(rename = "id")]
    _id: Option<String>,
    #[serde(rename = "channel")]
    _channel: Option<String>,
    #[serde(rename = "event")]
    _event: Option<String>,
    error: Option<WsError>,
    result: Option<serde_json::Value>,
}

/// WebSocket error response.
#[derive(Debug, Deserialize)]
struct WsError {
    code: Option<i32>,
    message: Option<String>,
}

/// Order place/amend request parameters for Spot.
#[derive(Debug, Serialize)]
struct SpotOrderParams {
    #[serde(skip_serializing_if = "Option::is_none")]
    text: Option<String>,
    currency_pair: String,
    #[serde(rename = "type")]
    order_type: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    account: Option<String>,
    side: String,
    amount: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    price: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    time_in_force: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    iceberg: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    auto_borrow: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    auto_repay: Option<bool>,
}

/// Order place/amend request parameters for Futures.
#[derive(Debug, Serialize)]
struct FuturesOrderParams {
    contract: String,
    size: i64,
    #[serde(skip_serializing_if = "Option::is_none")]
    price: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    tif: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    text: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    reduce_only: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    close: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    iceberg: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    auto_size: Option<String>,
}

/// Order cancel request parameters.
#[derive(Debug, Serialize)]
struct OrderCancelParams {
    #[serde(skip_serializing_if = "Option::is_none")]
    order_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    currency_pair: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    contract: Option<String>,
}

// ============================================================================
// Private WebSocket helper functions (inlined from private_ws module)
// ============================================================================

/// Calculate next reconnect delay with exponential backoff (capped).
fn next_reconnect_delay(current_secs: u64) -> u64 {
    (current_secs * 2).min(MAX_RECONNECT_DELAY_SECS)
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

/// Parse a status string from various exchanges into OrderStatus.
fn parse_order_status_str(s: &str) -> OrderStatus {
    match s.to_lowercase().as_str() {
        // Common
        "new" | "open" | "live" | "created" | "init" | "accepted" => OrderStatus::New,
        "partially_filled" | "partiallyfilled" | "partial_filled" | "partial" => {
            OrderStatus::PartiallyFilled
        }
        "filled" | "closed" | "full_fill" | "full" => OrderStatus::Filled,
        "cancelled" | "canceled" | "cancel" | "deactivated" => OrderStatus::Cancelled,
        "rejected" | "fail" | "failed" => OrderStatus::Rejected,
        "expired" | "expire" => OrderStatus::Expired,
        // Bybit specific
        "untriggered" => OrderStatus::New,
        "triggered" => OrderStatus::New,
        // Default
        _ => {
            warn!("Unknown order status string: '{}'", s);
            OrderStatus::New
        }
    }
}

// ============================================================================
// ExchangeTrader implementation
// ============================================================================

#[async_trait]
impl ExchangeTrader for GateTrader {
    fn exchange_name(&self) -> &str {
        "gate"
    }

    fn market_type(&self) -> MarketType {
        self.market_type
    }

    fn capabilities(&self) -> ExchangeCapabilities {
        tradersdk_core::exchange_capabilities! {
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
        let endpoint = if self.market_type == MarketType::Spot {
            "/spot/time"
        } else {
            "/futures/usdt/contracts"
        };

        let response = self
            .client
            .get(format!("{}{}", self.base_url, endpoint))
            .send()
            .await?;
        Ok(response.status().is_success())
    }

    async fn get_server_time(&self) -> Result<i64> {
        let response: serde_json::Value = self
            .client
            .get(format!("{}/spot/time", self.base_url))
            .send()
            .await?
            .json()
            .await?;
        Ok(response["server_time"].as_i64().unwrap_or(timestamp_ms()))
    }

    async fn get_balance(&self) -> Result<AccountBalance> {
        if self.market_type == MarketType::Spot {
            let balances: Vec<SpotAccountBalance> = self.signed_get("/spot/accounts", "").await?;

            let usdt = balances
                .iter()
                .find(|b| b.currency == "USDT")
                .map(|b| {
                    let available: f64 = b.available.parse().unwrap_or(0.0);
                    let locked: f64 = b.locked.parse().unwrap_or(0.0);
                    (available + locked, available)
                })
                .unwrap_or((0.0, 0.0));

            Ok(AccountBalance::simple(usdt.0, usdt.1, "USDT"))
        } else {
            let balances: Vec<FuturesAccountBalance> =
                self.signed_get("/futures/usdt/accounts", "").await?;

            let usdt = balances
                .iter()
                .find(|b| b.currency == "USDT")
                .map(|b| {
                    let total: f64 = b.total.parse().unwrap_or(0.0);
                    let available: f64 = b.available.parse().unwrap_or(0.0);
                    (total, available)
                })
                .unwrap_or((0.0, 0.0));

            Ok(AccountBalance::simple(usdt.0, usdt.1, "USDT"))
        }
    }

    async fn get_current_price(&self, symbol: &str) -> Result<f64> {
        let formatted = self.format_symbol(symbol);

        if self.market_type == MarketType::Spot {
            let response: Vec<serde_json::Value> = self
                .client
                .get(format!(
                    "{}/spot/tickers?currency_pair={}",
                    self.base_url, formatted
                ))
                .send()
                .await?
                .json()
                .await?;

            Ok(response
                .first()
                .and_then(|t| t["last"].as_str())
                .and_then(|s| s.parse().ok())
                .unwrap_or(0.0))
        } else {
            let response: Vec<serde_json::Value> = self
                .client
                .get(format!(
                    "{}/futures/usdt/tickers?contract={}",
                    self.base_url, formatted
                ))
                .send()
                .await?
                .json()
                .await?;

            Ok(response
                .first()
                .and_then(|t| t["last"].as_str())
                .and_then(|s| s.parse().ok())
                .unwrap_or(0.0))
        }
    }

    async fn set_leverage(&self, symbol: Option<&str>, leverage: u32) -> Result<bool> {
        if self.market_type == MarketType::Spot {
            return Ok(true);
        }

        let contract = symbol.ok_or_else(|| anyhow::anyhow!("Symbol required for leverage"))?;
        let formatted = self.format_symbol(contract);

        let body = serde_json::json!({
            "leverage": leverage.to_string()
        });

        let _response: serde_json::Value = self
            .signed_post(
                &format!("/futures/usdt/positions/{}/leverage", formatted),
                &body,
            )
            .await?;

        Ok(true)
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
        let formatted = self.format_symbol(symbol);
        let price = current_price.unwrap_or(self.get_current_price(&formatted).await?);

        if self.market_type == MarketType::Swap {
            if let Err(e) = self.set_leverage(Some(&formatted), leverage).await {
                error!("Failed to set leverage on Gate.io: {}", e);
            }
        }

        let quantity = if self.market_type == MarketType::Swap {
            usdt_amount / price * leverage as f64
        } else {
            usdt_amount / price
        };

        let side = if direction.to_lowercase() == "long" {
            OrderSide::Buy
        } else {
            OrderSide::Sell
        };

        self.place_market_order(&formatted, side, quantity).await
    }

    async fn place_market_order(
        &self,
        symbol: &str,
        side: OrderSide,
        quantity: f64,
    ) -> Result<OrderResult> {
        let formatted = self.format_symbol(symbol);

        // Use WebSocket by default (2x faster for HFT)
        self.place_order_ws(&formatted, side, OrderType::Market, quantity, None, false)
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
        let formatted = self.format_symbol(symbol);

        // Use WebSocket by default (2x faster for HFT)
        self.place_order_ws(
            &formatted,
            side,
            OrderType::Limit,
            quantity,
            Some(price),
            reduce_only,
        )
        .await
    }

    async fn cancel_order(&self, symbol: &str, order_id: &str) -> Result<bool> {
        let formatted = self.format_symbol(symbol);

        // Use WebSocket by default (2x faster for HFT)
        self.cancel_order_ws(&formatted, order_id).await
    }

    async fn get_order_status(&self, symbol: &str, order_id: &str) -> Result<OrderInfo> {
        let formatted = self.format_symbol(symbol);

        if self.market_type == MarketType::Spot {
            let order: SpotOrderResponse = self
                .signed_get(
                    &format!("/spot/orders/{}", order_id),
                    &format!("currency_pair={}", formatted),
                )
                .await?;

            let status = match order.order_type.as_str() {
                "closed" => OrderStatus::Filled,
                "cancelled" => OrderStatus::Cancelled,
                _ => OrderStatus::New,
            };

            let filled_quantity: f64 = order
                .filled_total
                .as_ref()
                .and_then(|s| s.parse().ok())
                .unwrap_or(0.0);
            let quantity: f64 = order.amount.parse().unwrap_or(0.0);

            Ok(OrderInfo {
                order_id: order.id,
                client_order_id: None,
                symbol: order.currency_pair,
                status,
                side: if order.side == "buy" {
                    OrderSide::Buy
                } else {
                    OrderSide::Sell
                },
                order_type: if order.order_type == "market" {
                    OrderType::Market
                } else {
                    OrderType::Limit
                },
                price: order
                    .price
                    .as_ref()
                    .and_then(|s| s.parse().ok())
                    .unwrap_or(0.0),
                quantity,
                filled_quantity,
                remaining_quantity: quantity - filled_quantity,
                avg_price: order.avg_deal_price.as_ref().and_then(|s| s.parse().ok()),
                timestamp: timestamp_ms(),
                time_in_force: TimeInForce::GTC,
                reduce_only: false,
            })
        } else {
            let order: FuturesOrderResponse = self
                .signed_get(&format!("/futures/usdt/orders/{}", order_id), "")
                .await?;

            let quantity = order.size.abs() as f64;

            Ok(OrderInfo {
                order_id: order.id.to_string(),
                client_order_id: None,
                symbol: order.contract,
                status: OrderStatus::New, // Would need to check actual status field
                side: if order.size > 0 {
                    OrderSide::Buy
                } else {
                    OrderSide::Sell
                },
                order_type: OrderType::Limit,
                price: order
                    .price
                    .as_ref()
                    .and_then(|s| s.parse().ok())
                    .unwrap_or(0.0),
                quantity,
                filled_quantity: 0.0, // Would need actual field
                remaining_quantity: quantity,
                avg_price: order.fill_price.as_ref().and_then(|s| s.parse().ok()),
                timestamp: timestamp_ms(),
                time_in_force: TimeInForce::GTC,
                reduce_only: false,
            })
        }
    }

    async fn close_position(&self, symbol: &str) -> Result<OrderResult> {
        if self.market_type == MarketType::Spot {
            anyhow::bail!("Close position not supported for spot trading");
        }

        let formatted = self.format_symbol(symbol);

        // Get current position
        let positions: Vec<serde_json::Value> =
            self.signed_get("/futures/usdt/positions", "").await?;

        let position = positions
            .iter()
            .find(|p| p["contract"].as_str() == Some(&formatted));

        if let Some(pos) = position {
            let size: i64 = pos["size"].as_i64().unwrap_or(0);

            if size == 0 {
                return Ok(OrderResult::failure(
                    formatted,
                    OrderSide::Sell,
                    "No position to close".to_string(),
                ));
            }

            // Close position by placing opposite order
            let close_size = -size; // Opposite sign to close

            let body = serde_json::json!({
                "contract": formatted,
                "size": close_size,
                "price": "0",
                "tif": "ioc",
                "reduce_only": true
            });

            let response: FuturesOrderResponse =
                self.signed_post("/futures/usdt/orders", &body).await?;

            let side = if size > 0 {
                OrderSide::Sell
            } else {
                OrderSide::Buy
            };

            return Ok(OrderResult {
                success: true,
                order_id: Some(response.id.to_string()),
                symbol: formatted,
                side,
                order_type: OrderType::Market,
                quantity: Some(size.abs() as f64),
                entry_price: response.fill_price.as_ref().and_then(|p| p.parse().ok()),
                position_size: Some(0.0),
                position_usdt: None,
                error: None,
                raw_response: None,
            });
        }

        Ok(OrderResult::failure(
            formatted,
            OrderSide::Sell,
            "Position not found".to_string(),
        ))
    }

    fn format_symbol(&self, base: &str) -> String {
        let s = base.to_uppercase();

        // Handle BTC/USDT format
        if s.contains('/') {
            return s.replace("/", "_");
        }

        if self.market_type == MarketType::Spot {
            // Spot: BTC_USDT format
            if s.contains('_') {
                s
            } else if s.ends_with("USDT") {
                let b = s.trim_end_matches("USDT");
                format!("{}_USDT", b)
            } else {
                format!("{}_USDT", s)
            }
        } else {
            // Futures: BTC_USDT format for perpetual contracts
            if s.contains('_') {
                s
            } else if s.ends_with("USDT") {
                let b = s.trim_end_matches("USDT");
                format!("{}_USDT", b)
            } else {
                format!("{}_USDT", s)
            }
        }
    }

    async fn get_orderbook(&self, symbol: &str, depth: u32) -> Result<Orderbook> {
        let formatted = self.format_symbol(symbol);
        let limit = depth.min(100);

        let response: serde_json::Value = if self.market_type == MarketType::Spot {
            self.client
                .get(format!(
                    "{}/spot/order_book?currency_pair={}&limit={}",
                    self.base_url, formatted, limit
                ))
                .send()
                .await?
                .json()
                .await?
        } else {
            self.client
                .get(format!(
                    "{}/futures/usdt/order_book?contract={}&limit={}",
                    self.base_url, formatted, limit
                ))
                .send()
                .await?
                .json()
                .await?
        };

        let bids: Vec<OrderbookLevel> = response["bids"]
            .as_array()
            .map(|arr| {
                arr.iter()
                    .filter_map(|level| {
                        let price: f64 = level["p"].as_str().or(level[0].as_str())?.parse().ok()?;
                        let qty: f64 = level["s"].as_str().or(level[1].as_str())?.parse().ok()?;
                        Some(OrderbookLevel {
                            price,
                            quantity: qty,
                        })
                    })
                    .collect()
            })
            .unwrap_or_default();

        let asks: Vec<OrderbookLevel> = response["asks"]
            .as_array()
            .map(|arr| {
                arr.iter()
                    .filter_map(|level| {
                        let price: f64 = level["p"].as_str().or(level[0].as_str())?.parse().ok()?;
                        let qty: f64 = level["s"].as_str().or(level[1].as_str())?.parse().ok()?;
                        Some(OrderbookLevel {
                            price,
                            quantity: qty,
                        })
                    })
                    .collect()
            })
            .unwrap_or_default();

        Ok(Orderbook {
            symbol: formatted,
            bids,
            asks,
            timestamp: timestamp_ms(),
        })
    }

    async fn test_ws_orderbook(
        &self,
        symbol: &str,
        _depth: u32,
        timeout_ms: u64,
    ) -> Result<WsTestResult> {
        let formatted = self.format_symbol(symbol);
        let ws_url = if self.market_type == MarketType::Spot {
            SPOT_WS_URL
        } else {
            FUTURES_WS_URL
        };

        let start = std::time::Instant::now();
        let (mut ws_stream, _) = connect_async(ws_url)
            .await
            .context("Failed to connect to Gate.io WebSocket")?;

        let connect_time = start.elapsed().as_millis() as u64;

        // Subscribe to orderbook
        let subscribe_msg = if self.market_type == MarketType::Spot {
            serde_json::json!({
                "time": timestamp_ms() / 1000,
                "channel": "spot.order_book",
                "event": "subscribe",
                "payload": [formatted, "20", "100ms"]
            })
        } else {
            serde_json::json!({
                "time": timestamp_ms() / 1000,
                "channel": "futures.order_book",
                "event": "subscribe",
                "payload": [formatted, "20", "100ms"]
            })
        };

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
                            if let Ok(parsed) = serde_json::from_str::<serde_json::Value>(&text) {
                                if parsed.get("result").is_some() || parsed.get("bids").is_some() {
                                    data_sample = Some(parsed);
                                    break;
                                }
                            }
                        }
                        Some(Ok(Message::Ping(data))) => {
                            let _ = ws_stream.send(Message::Pong(data)).await;
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
            subscription_type: "order_book".to_string(),
            data_sample,
        })
    }

    async fn test_ws_trades(&self, symbol: &str, timeout_ms: u64) -> Result<WsTestResult> {
        let formatted = self.format_symbol(symbol);
        let ws_url = if self.market_type == MarketType::Spot {
            SPOT_WS_URL
        } else {
            FUTURES_WS_URL
        };

        let start = std::time::Instant::now();
        let (mut ws_stream, _) = connect_async(ws_url)
            .await
            .context("Failed to connect to Gate.io WebSocket")?;

        let connect_time = start.elapsed().as_millis() as u64;

        let subscribe_msg = if self.market_type == MarketType::Spot {
            serde_json::json!({
                "time": timestamp_ms() / 1000,
                "channel": "spot.trades",
                "event": "subscribe",
                "payload": [formatted]
            })
        } else {
            serde_json::json!({
                "time": timestamp_ms() / 1000,
                "channel": "futures.trades",
                "event": "subscribe",
                "payload": [formatted]
            })
        };

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
                            if let Ok(parsed) = serde_json::from_str::<serde_json::Value>(&text) {
                                if parsed.get("result").is_some() {
                                    data_sample = Some(parsed);
                                    break;
                                }
                            }
                        }
                        Some(Ok(Message::Ping(data))) => {
                            let _ = ws_stream.send(Message::Pong(data)).await;
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
            subscription_type: "trades".to_string(),
            data_sample,
        })
    }

    async fn get_ticker(&self, symbol: &str) -> Result<TickerInfo> {
        let formatted = self.format_symbol(symbol);

        if self.market_type == MarketType::Spot {
            let response: Vec<serde_json::Value> = self
                .client
                .get(format!(
                    "{}/spot/tickers?currency_pair={}",
                    self.base_url, formatted
                ))
                .send()
                .await?
                .json()
                .await?;

            let ticker = response
                .first()
                .ok_or_else(|| anyhow::anyhow!("Ticker not found"))?;

            return Ok(TickerInfo {
                symbol: formatted,
                last_price: ticker["last"]
                    .as_str()
                    .and_then(|s| s.parse().ok())
                    .unwrap_or(0.0),
                bid_price: ticker["highest_bid"].as_str().and_then(|s| s.parse().ok()),
                ask_price: ticker["lowest_ask"].as_str().and_then(|s| s.parse().ok()),
                high_24h: ticker["high_24h"].as_str().and_then(|s| s.parse().ok()),
                low_24h: ticker["low_24h"].as_str().and_then(|s| s.parse().ok()),
                volume_24h: ticker["base_volume"].as_str().and_then(|s| s.parse().ok()),
                turnover_24h: ticker["quote_volume"].as_str().and_then(|s| s.parse().ok()),
                change_24h: ticker["change_percentage"]
                    .as_str()
                    .and_then(|s| s.parse().ok()),
                open_interest: None,
                open_interest_value: None,
                funding_rate: None,
                next_funding_time: None,
                mark_price: None,
                index_price: None,
                timestamp: timestamp_ms(),
            });
        }

        // Futures
        let response: Vec<serde_json::Value> = self
            .client
            .get(format!(
                "{}/futures/usdt/tickers?contract={}",
                self.base_url, formatted
            ))
            .send()
            .await?
            .json()
            .await?;

        let ticker = response
            .first()
            .ok_or_else(|| anyhow::anyhow!("Ticker not found"))?;

        Ok(TickerInfo {
            symbol: formatted,
            last_price: ticker["last"]
                .as_str()
                .and_then(|s| s.parse().ok())
                .unwrap_or(0.0),
            bid_price: ticker["highest_bid"].as_str().and_then(|s| s.parse().ok()),
            ask_price: ticker["lowest_ask"].as_str().and_then(|s| s.parse().ok()),
            high_24h: ticker["high_24h"].as_str().and_then(|s| s.parse().ok()),
            low_24h: ticker["low_24h"].as_str().and_then(|s| s.parse().ok()),
            volume_24h: ticker["volume_24h"].as_str().and_then(|s| s.parse().ok()),
            turnover_24h: ticker["volume_24h_usd"]
                .as_str()
                .and_then(|s| s.parse().ok()),
            change_24h: ticker["change_percentage"]
                .as_str()
                .and_then(|s| s.parse().ok()),
            open_interest: ticker["total_size"].as_str().and_then(|s| s.parse().ok()),
            open_interest_value: None,
            funding_rate: ticker["funding_rate"].as_str().and_then(|s| s.parse().ok()),
            next_funding_time: ticker["funding_next_apply"].as_i64().map(|t| t * 1000),
            mark_price: ticker["mark_price"].as_str().and_then(|s| s.parse().ok()),
            index_price: ticker["index_price"].as_str().and_then(|s| s.parse().ok()),
            timestamp: timestamp_ms(),
        })
    }

    async fn test_ws_ticker(&self, symbol: &str, timeout_ms: u64) -> Result<WsTestResult> {
        let formatted = self.format_symbol(symbol);
        let ws_url = if self.market_type == MarketType::Spot {
            SPOT_WS_URL
        } else {
            FUTURES_WS_URL
        };

        let start = std::time::Instant::now();
        let (mut ws_stream, _) = connect_async(ws_url)
            .await
            .context("Failed to connect to Gate.io WebSocket")?;

        let connect_time = start.elapsed().as_millis() as u64;

        let subscribe_msg = if self.market_type == MarketType::Spot {
            serde_json::json!({
                "time": timestamp_ms() / 1000,
                "channel": "spot.tickers",
                "event": "subscribe",
                "payload": [formatted]
            })
        } else {
            serde_json::json!({
                "time": timestamp_ms() / 1000,
                "channel": "futures.tickers",
                "event": "subscribe",
                "payload": [formatted]
            })
        };

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
                            if let Ok(parsed) = serde_json::from_str::<serde_json::Value>(&text) {
                                data_sample = Some(parsed);
                                break;
                            }
                        }
                        Some(Ok(Message::Ping(data))) => {
                            let _ = ws_stream.send(Message::Pong(data)).await;
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
            subscription_type: "tickers".to_string(),
            data_sample,
        })
    }

    async fn get_funding_rate_history(
        &self,
        symbol: &str,
        limit: u32,
    ) -> Result<Vec<FundingRateEntry>> {
        if self.market_type != MarketType::Swap {
            anyhow::bail!("Funding rate only available for futures");
        }

        let formatted = self.format_symbol(symbol);

        let response: Vec<serde_json::Value> = self
            .client
            .get(format!(
                "{}/futures/usdt/funding_rate?contract={}&limit={}",
                self.base_url,
                formatted,
                limit.min(1000)
            ))
            .send()
            .await?
            .json()
            .await?;

        let entries: Vec<FundingRateEntry> = response
            .iter()
            .filter_map(|e| {
                Some(FundingRateEntry {
                    symbol: formatted.clone(),
                    funding_rate: e["r"].as_str()?.parse().ok()?,
                    funding_time: e["t"].as_i64()? * 1000,
                })
            })
            .collect();

        Ok(entries)
    }

    async fn get_open_interest_history(
        &self,
        symbol: &str,
        _interval: &str,
        _limit: u32,
    ) -> Result<Vec<OpenInterestEntry>> {
        if self.market_type != MarketType::Swap {
            anyhow::bail!("Open interest only available for futures");
        }

        let formatted = self.format_symbol(symbol);

        // Gate.io doesn't have historical OI endpoint, return current OI
        let response: Vec<serde_json::Value> = self
            .client
            .get(format!(
                "{}/futures/usdt/tickers?contract={}",
                self.base_url, formatted
            ))
            .send()
            .await?
            .json()
            .await?;

        let ticker = response
            .first()
            .ok_or_else(|| anyhow::anyhow!("Ticker not found"))?;

        let oi: f64 = ticker["total_size"]
            .as_str()
            .and_then(|s| s.parse().ok())
            .unwrap_or(0.0);
        let price: f64 = ticker["last"]
            .as_str()
            .and_then(|s| s.parse().ok())
            .unwrap_or(0.0);

        Ok(vec![OpenInterestEntry {
            symbol: formatted,
            open_interest: oi,
            open_interest_value: oi * price,
            timestamp: timestamp_ms(),
        }])
    }

    async fn subscribe_order_updates(
        &self,
        sender: broadcast::Sender<OrderUpdateEvent>,
    ) -> Result<JoinHandle<()>> {
        let api_key = self.api_key.clone();
        let api_secret = self.api_secret.clone();
        let market_type = self.market_type;
        let exchange_name = "gate".to_string();

        let handle = tokio::spawn(async move {
            let mut reconnect_delay = INITIAL_RECONNECT_DELAY_SECS;

            loop {
                let ws_url = match market_type {
                    MarketType::Spot => SPOT_WS_TRADE_URL,
                    MarketType::Swap => FUTURES_WS_TRADE_URL,
                };

                info!("[{}] Connecting to private WS: {}", exchange_name, ws_url);

                match connect_async(ws_url).await {
                    Ok((mut ws_stream, _)) => {
                        // Gate.io uses per-request auth, subscribe with auth
                        let channel = match market_type {
                            MarketType::Spot => "spot.orders",
                            MarketType::Swap => "futures.orders",
                        };
                        let time = timestamp_ms() / 1000;
                        let sign_string =
                            format!("channel={}&event={}&time={}", channel, "subscribe", time);

                        type HmacSha512 = Hmac<Sha512>;
                        let signature = {
                            let mut mac = HmacSha512::new_from_slice(api_secret.as_bytes())
                                .expect("HMAC can take key of any size");
                            mac.update(sign_string.as_bytes());
                            hex::encode(mac.finalize().into_bytes())
                        };

                        let sub_msg = serde_json::json!({
                            "time": time,
                            "channel": channel,
                            "event": "subscribe",
                            "payload": ["!all"],
                            "auth": {
                                "method": "api_key",
                                "KEY": api_key,
                                "SIGN": signature
                            }
                        });

                        if let Err(e) = ws_stream.send(Message::Text(sub_msg.to_string())).await {
                            error!("[{}] Failed to subscribe: {}", exchange_name, e);
                            tokio::time::sleep(tokio::time::Duration::from_secs(reconnect_delay))
                                .await;
                            reconnect_delay = next_reconnect_delay(reconnect_delay);
                            continue;
                        }

                        info!(
                            "[{}] Subscribed to private order updates ({})",
                            exchange_name, channel
                        );
                        reconnect_delay = INITIAL_RECONNECT_DELAY_SECS;

                        let mut ping_interval =
                            tokio::time::interval(tokio::time::Duration::from_secs(15));

                        loop {
                            tokio::select! {
                                msg = ws_stream.next() => {
                                    match msg {
                                        Some(Ok(Message::Text(text))) => {
                                            if let Ok(msg_json) = serde_json::from_str::<serde_json::Value>(&text) {
                                                let event_type = msg_json.get("event").and_then(|v| v.as_str()).unwrap_or("");
                                                if event_type == "subscribe" || event_type == "unsubscribe" {
                                                    debug!("[{}] WS event: {}", exchange_name, text);
                                                    continue;
                                                }

                                                if event_type == "update" {
                                                    if let Some(result) = msg_json.get("result").and_then(|r| r.as_array()) {
                                                        for o in result {
                                                            let id = o.get("id").and_then(|v| v.as_str())
                                                                .or_else(|| o.get("id").and_then(|v| v.as_i64()).map(|_| ""))
                                                                .unwrap_or("");
                                                            let id_str = if id.is_empty() {
                                                                o.get("id").and_then(|v| v.as_i64()).map(|v| v.to_string()).unwrap_or_default()
                                                            } else {
                                                                id.to_string()
                                                            };

                                                            let create_time = o.get("create_time")
                                                                .and_then(|v| v.as_f64())
                                                                .map(|t| (t * 1000.0) as i64)
                                                                .or_else(|| o.get("create_time").and_then(|v| v.as_i64()))
                                                                .unwrap_or(0);

                                                            let event = OrderUpdateEvent {
                                                                exchange: exchange_name.clone(),
                                                                order_id: id_str,
                                                                client_order_id: o.get("text").and_then(|v| v.as_str()).map(|s| s.to_string()),
                                                                symbol: o.get("currency_pair")
                                                                    .or_else(|| o.get("contract"))
                                                                    .and_then(|v| v.as_str())
                                                                    .unwrap_or("").to_string(),
                                                                status: parse_order_status_str(
                                                                    o.get("status").or_else(|| o.get("finish_as"))
                                                                        .and_then(|v| v.as_str()).unwrap_or("open")
                                                                ),
                                                                side: if o.get("side").and_then(|v| v.as_str()) == Some("buy") { OrderSide::Buy } else { OrderSide::Sell },
                                                                order_type: if o.get("type").and_then(|v| v.as_str()) == Some("market") { OrderType::Market } else { OrderType::Limit },
                                                                price: o.get("price").and_then(|v| v.as_str()).and_then(|s| s.parse().ok()).unwrap_or(0.0),
                                                                quantity: o.get("amount").or_else(|| o.get("size"))
                                                                    .and_then(|v| v.as_str().or_else(|| v.as_f64().map(|_| "")))
                                                                    .and_then(|s| if s.is_empty() { None } else { s.parse().ok() })
                                                                    .or_else(|| o.get("amount").or_else(|| o.get("size")).and_then(|v| v.as_f64()))
                                                                    .unwrap_or(0.0),
                                                                filled_quantity: o.get("filled_total").or_else(|| o.get("left"))
                                                                    .and_then(|v| v.as_str()).and_then(|s| s.parse().ok()).unwrap_or(0.0),
                                                                avg_fill_price: o.get("avg_deal_price").and_then(|v| v.as_str()).and_then(|s| s.parse().ok()),
                                                                fee: o.get("fee").and_then(|v| v.as_str()).and_then(|s| s.parse().ok()),
                                                                fee_currency: o.get("fee_currency").and_then(|v| v.as_str()).map(|s| s.to_string()),
                                                                exchange_timestamp: create_time,
                                                                received_at: timestamp_ms(),
                                                                raw: Some(o.clone()),
                                                            };
                                                            log_order_update(&event);
                                                            let _ = sender.send(event);
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
                                    let ping_msg = serde_json::json!({"time": timestamp_ms() / 1000, "channel": "spot.ping"});
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
