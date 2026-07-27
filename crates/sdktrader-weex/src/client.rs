//! Weex exchange trader implementation.

use anyhow::{Context, Result};
use async_trait::async_trait;
use base64::{engine::general_purpose, Engine as _};
use futures_util::{SinkExt, StreamExt};
use reqwest::Client;
use sdktrader_core::models::{
    AccountBalance, ExchangeCapabilities, MarketType, OrderInfo, OrderResult, OrderSide,
    OrderStatus, OrderType, OrderUpdateEvent, Orderbook, OrderbookLevel, TickerInfo, WsTestResult,
};
use sdktrader_core::utils::timestamp_ms;
use sdktrader_core::ExchangeTrader;
use serde::{Deserialize, Serialize};
use tokio::sync::broadcast;
use tokio::task::JoinHandle;
use tokio_tungstenite::{connect_async, tungstenite::protocol::Message};
use tracing::{debug, error, info, warn};

const MAINNET_URL: &str = "https://api.weex.com";
const MAINNET_WS_PUBLIC_URL: &str = "wss://ws.weex.com/v2/ws/public";
const MAINNET_WS_PRIVATE_URL: &str = "wss://ws.weex.com/v2/ws/private";
const TESTNET_WS_PRIVATE_URL: &str = "wss://ws.weex.com/v2/ws/private";

/// Maximum reconnect backoff delay in seconds.
const MAX_RECONNECT_DELAY_SECS: u64 = 30;

/// Initial reconnect delay in seconds.
const INITIAL_RECONNECT_DELAY_SECS: u64 = 1;

/// Weex exchange trader.
pub struct WeexTrader {
    api_key: String,
    api_secret: String,
    passphrase: String,
    testnet: bool,
    market_type: MarketType,
    client: Client,
    base_url: String,
}

impl WeexTrader {
    /// Create a new Weex trader.
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
            base_url: MAINNET_URL.to_string(),
        }
    }

    /// Get WebSocket private URL based on testnet setting.
    fn ws_private_url(&self) -> &str {
        if self.testnet {
            TESTNET_WS_PRIVATE_URL
        } else {
            MAINNET_WS_PRIVATE_URL
        }
    }

    /// Get WebSocket public URL.
    fn ws_public_url(&self) -> &str {
        MAINNET_WS_PUBLIC_URL
    }

    /// Get instrument type for API calls.
    fn inst_type(&self) -> &str {
        match self.market_type {
            MarketType::Spot => "SPOT",
            MarketType::Swap => "USDT-FUTURES",
        }
    }

    /// Get product type for futures API calls.
    fn product_type(&self) -> &str {
        "USDT-FUTURES"
    }

    /// Generate HMAC-SHA256 signature for REST API.
    fn sign(&self, timestamp: i64, method: &str, request_path: &str, body: &str) -> String {
        use hmac::{Hmac, Mac};
        use sha2::Sha256;

        type HmacSha256 = Hmac<Sha256>;

        let message = format!("{}{}{}{}", timestamp, method, request_path, body);

        let mut mac = HmacSha256::new_from_slice(self.api_secret.as_bytes())
            .expect("HMAC can take key of any size");
        mac.update(message.as_bytes());

        let result = mac.finalize();
        general_purpose::STANDARD.encode(result.into_bytes())
    }

    /// Generate WebSocket authentication signature for Weex.
    /// Uses: timestamp + "GET" + "/user/verify" signed with HMAC SHA256 and Base64 encoded
    fn ws_auth_signature(&self, timestamp: i64) -> String {
        use hmac::{Hmac, Mac};
        use sha2::Sha256;

        type HmacSha256 = Hmac<Sha256>;

        let message = format!("{}GET/user/verify", timestamp);

        let mut mac = HmacSha256::new_from_slice(self.api_secret.as_bytes())
            .expect("HMAC can take key of any size");
        mac.update(message.as_bytes());

        let result = mac.finalize();
        general_purpose::STANDARD.encode(result.into_bytes())
    }

    /// Create authenticated headers for REST API requests.
    fn auth_headers(&self, method: &str, request_path: &str, body: &str) -> Vec<(String, String)> {
        let timestamp = timestamp_ms();
        let signature = self.sign(timestamp, method, request_path, body);

        vec![
            ("ACCESS-KEY".to_string(), self.api_key.clone()),
            ("ACCESS-SIGN".to_string(), signature),
            ("ACCESS-TIMESTAMP".to_string(), timestamp.to_string()),
            ("ACCESS-PASSPHRASE".to_string(), self.passphrase.clone()),
            ("Content-Type".to_string(), "application/json".to_string()),
            ("locale".to_string(), "en-US".to_string()),
        ]
    }

    /// Create and authenticate WebSocket connection for trading.
    async fn create_ws_private_connection(
        &self,
    ) -> Result<
        tokio_tungstenite::WebSocketStream<
            tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>,
        >,
    > {
        let ws_url = self.ws_private_url();
        let (mut ws_stream, _) = connect_async(ws_url)
            .await
            .context("Failed to connect to Weex private WebSocket")?;

        // Authenticate
        let timestamp = timestamp_ms() / 1000; // Weex uses seconds
        let signature = self.ws_auth_signature(timestamp);

        let auth_msg = WsLoginRequest {
            op: "login".to_string(),
            args: vec![WsLoginArgs {
                api_key: self.api_key.clone(),
                passphrase: self.passphrase.clone(),
                timestamp: timestamp.to_string(),
                sign: signature,
            }],
        };

        let auth_json = serde_json::to_string(&auth_msg)?;
        debug!("Sending WS auth: {}", auth_json);

        ws_stream.send(Message::Text(auth_json)).await?;

        // Wait for auth response
        let timeout = tokio::time::Duration::from_secs(5);
        tokio::select! {
            msg = ws_stream.next() => {
                match msg {
                    Some(Ok(Message::Text(text))) => {
                        debug!("Auth response: {}", text);
                        let auth_resp: WsLoginResponse = serde_json::from_str(&text)
                            .context("Failed to parse auth response")?;

                        if auth_resp.event != "login" || auth_resp.code != Some("0".to_string()) {
                            anyhow::bail!("WebSocket authentication failed: {:?}", auth_resp.msg);
                        }

                        info!("Weex WebSocket authenticated");
                    }
                    Some(Ok(Message::Ping(_))) => {
                        debug!("Received ping during auth");
                    }
                    Some(Ok(_)) => anyhow::bail!("Unexpected message type during auth"),
                    Some(Err(e)) => anyhow::bail!("WebSocket error during auth: {}", e),
                    None => anyhow::bail!("WebSocket closed during auth"),
                }
            }
            _ = tokio::time::sleep(timeout) => {
                anyhow::bail!("WebSocket authentication timeout");
            }
        }

        Ok(ws_stream)
    }

    /// Send ping to keep WebSocket connection alive.
    #[allow(dead_code)]
    async fn send_ws_ping(
        ws_stream: &mut tokio_tungstenite::WebSocketStream<
            tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>,
        >,
    ) -> Result<()> {
        ws_stream.send(Message::Text("ping".into())).await?;
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
        // Create WS connection and authenticate
        let mut ws_stream = self.create_ws_private_connection().await?;

        // Prepare order request
        let timestamp = timestamp_ms();
        let req_id = format!("order_{}", timestamp);

        let mut order_params = serde_json::json!({
            "instId": symbol,
            "tdMode": if reduce_only { "cross" } else { "cash" },
            "side": match side {
                OrderSide::Buy => "buy",
                OrderSide::Sell => "sell",
            },
            "orderType": match order_type {
                OrderType::Market => "market",
                OrderType::Limit => "limit",
                _ => "market",
            },
            "sz": format!("{:.8}", quantity),
        });

        // Add price for limit orders
        if let (OrderType::Limit, Some(p)) = (order_type, price) {
            order_params["px"] = serde_json::json!(format!("{:.8}", p));
        }

        let order_req = WsTradeRequest {
            id: req_id.clone(),
            op: "order".to_string(),
            args: vec![WsTradeArgs {
                inst_type: self.inst_type().to_string(),
                channel: "place-order".to_string(),
                data: order_params,
            }],
        };

        let order_json = serde_json::to_string(&order_req)?;
        debug!("Sending WS order: {}", order_json);

        // Send order
        ws_stream.send(Message::Text(order_json)).await?;

        // Wait for response
        let timeout = tokio::time::Duration::from_secs(10);
        let mut order_result = None;

        tokio::select! {
            msg = ws_stream.next() => {
                match msg {
                    Some(Ok(Message::Text(text))) => {
                        debug!("Order response: {}", text);
                        let order_resp: WsTradeResponse = serde_json::from_str(&text)
                            .context("Failed to parse order response")?;

                        if order_resp.code == "0" {
                            if let Some(data) = order_resp.data {
                                if let Some(order_id) = data.get("ordId").and_then(|v| v.as_str()) {
                                    info!("Weex WS order placed: {}", order_id);
                                    order_result = Some(OrderResult {
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
                        } else {
                            let error_msg = order_resp.msg.unwrap_or_else(|| "Unknown error".to_string());
                            order_result = Some(OrderResult::failure(
                                symbol.to_string(),
                                side,
                                error_msg,
                            ));
                        }
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
        let request_path = "/api/v2/mix/order/place-order";

        let side_str = match side {
            OrderSide::Buy => {
                if reduce_only {
                    "close_short"
                } else {
                    "open_long"
                }
            }
            OrderSide::Sell => {
                if reduce_only {
                    "close_long"
                } else {
                    "open_short"
                }
            }
        };

        let order_type_str = match order_type {
            OrderType::Market => "market",
            OrderType::Limit => "limit",
            _ => "market",
        };

        let mut body = serde_json::json!({
            "symbol": symbol,
            "productType": self.product_type(),
            "marginMode": "crossed",
            "marginCoin": "USDT",
            "size": format!("{:.8}", quantity),
            "side": side_str,
            "orderType": order_type_str,
        });

        if let (OrderType::Limit, Some(p)) = (order_type, price) {
            body["price"] = serde_json::json!(format!("{:.8}", p));
        }

        let body_str = serde_json::to_string(&body)?;
        let headers = self.auth_headers("POST", request_path, &body_str);

        let mut request = self
            .client
            .post(format!("{}{}", self.base_url, request_path));

        for (key, value) in headers {
            request = request.header(&key, &value);
        }

        let response: serde_json::Value = request.body(body_str).send().await?.json().await?;

        debug!("Order response: {:?}", response);

        if response["code"].as_str() == Some("00000") {
            let data = &response["data"];
            let order_id = data["orderId"].as_str().unwrap_or("").to_string();

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
            let error_msg = response["msg"]
                .as_str()
                .unwrap_or("Unknown error")
                .to_string();
            Ok(OrderResult::failure(symbol.to_string(), side, error_msg))
        }
    }

    /// Cancel order via WebSocket.
    async fn cancel_order_ws(&self, symbol: &str, order_id: &str) -> Result<bool> {
        // Create WS connection and authenticate
        let mut ws_stream = self.create_ws_private_connection().await?;

        // Prepare cancel request
        let timestamp = timestamp_ms();
        let req_id = format!("cancel_{}", timestamp);

        let cancel_params = serde_json::json!({
            "instId": symbol,
            "ordId": order_id,
        });

        let cancel_req = WsTradeRequest {
            id: req_id.clone(),
            op: "order".to_string(),
            args: vec![WsTradeArgs {
                inst_type: self.inst_type().to_string(),
                channel: "cancel-order".to_string(),
                data: cancel_params,
            }],
        };

        let cancel_json = serde_json::to_string(&cancel_req)?;
        debug!("Sending WS cancel: {}", cancel_json);

        // Send cancel request
        ws_stream.send(Message::Text(cancel_json)).await?;

        // Wait for response
        let timeout = tokio::time::Duration::from_secs(10);
        let mut success = false;

        tokio::select! {
            msg = ws_stream.next() => {
                match msg {
                    Some(Ok(Message::Text(text))) => {
                        debug!("Cancel response: {}", text);
                        let cancel_resp: WsTradeResponse = serde_json::from_str(&text)
                            .context("Failed to parse cancel response")?;

                        if cancel_resp.code == "0" {
                            info!("Weex WS order cancelled: {}", order_id);
                            success = true;
                        } else {
                            let error_msg = cancel_resp.msg.unwrap_or_else(|| "Unknown error".to_string());
                            error!("Failed to cancel order {}: {}", order_id, error_msg);
                        }
                    }
                    Some(Ok(Message::Ping(_))) => debug!("Received ping during cancel"),
                    Some(Ok(Message::Pong(_))) => debug!("Received pong during cancel"),
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

    /// Cancel order via REST API.
    async fn cancel_order_rest(&self, symbol: &str, order_id: &str) -> Result<bool> {
        let request_path = "/api/v2/mix/order/cancel-order";

        let body = serde_json::json!({
            "symbol": symbol,
            "productType": self.product_type(),
            "orderId": order_id,
        });

        let body_str = serde_json::to_string(&body)?;
        let headers = self.auth_headers("POST", request_path, &body_str);

        let mut request = self
            .client
            .post(format!("{}{}", self.base_url, request_path));

        for (key, value) in headers {
            request = request.header(&key, &value);
        }

        let response: serde_json::Value = request.body(body_str).send().await?.json().await?;

        debug!("Cancel response: {:?}", response);

        Ok(response["code"].as_str() == Some("00000"))
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
        // Create WS connection and authenticate
        let mut ws_stream = self.create_ws_private_connection().await?;

        // Prepare amend request
        let timestamp = timestamp_ms();
        let req_id = format!("amend_{}", timestamp);

        let mut amend_params = serde_json::json!({
            "instId": symbol,
            "ordId": order_id,
        });

        // Add new quantity if provided
        if let Some(qty) = new_quantity {
            amend_params["newSz"] = serde_json::json!(format!("{:.8}", qty));
        }

        // Add new price if provided
        if let Some(price) = new_price {
            amend_params["newPx"] = serde_json::json!(format!("{:.8}", price));
        }

        let amend_req = WsTradeRequest {
            id: req_id.clone(),
            op: "order".to_string(),
            args: vec![WsTradeArgs {
                inst_type: self.inst_type().to_string(),
                channel: "amend-order".to_string(),
                data: amend_params,
            }],
        };

        let amend_json = serde_json::to_string(&amend_req)?;
        debug!("Sending WS amend: {}", amend_json);

        // Send amend request
        ws_stream.send(Message::Text(amend_json)).await?;

        // Wait for response
        let timeout = tokio::time::Duration::from_secs(10);
        let mut success = false;

        tokio::select! {
            msg = ws_stream.next() => {
                match msg {
                    Some(Ok(Message::Text(text))) => {
                        debug!("Amend response: {}", text);
                        let amend_resp: WsTradeResponse = serde_json::from_str(&text)
                            .context("Failed to parse amend response")?;

                        if amend_resp.code == "0" {
                            info!("Weex WS order amended: {}", order_id);
                            success = true;
                        } else {
                            let error_msg = amend_resp.msg.unwrap_or_else(|| "Unknown error".to_string());
                            error!("Failed to amend order {}: {}", order_id, error_msg);
                        }
                    }
                    Some(Ok(Message::Ping(_))) => debug!("Received ping during amend"),
                    Some(Ok(Message::Pong(_))) => debug!("Received pong during amend"),
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

/// Calculate next reconnect delay with exponential backoff (capped).
fn next_reconnect_delay(current_secs: u64) -> u64 {
    (current_secs * 2).min(MAX_RECONNECT_DELAY_SECS)
}

/// Parse a status string from Weex into OrderStatus.
fn parse_order_status_str(s: &str) -> OrderStatus {
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

/// WebSocket login request for Weex.
#[derive(Debug, Serialize)]
struct WsLoginRequest {
    op: String,
    args: Vec<WsLoginArgs>,
}

#[derive(Debug, Serialize)]
struct WsLoginArgs {
    #[serde(rename = "apiKey")]
    api_key: String,
    passphrase: String,
    timestamp: String,
    sign: String,
}

/// WebSocket login response from Weex.
#[derive(Debug, Deserialize)]
struct WsLoginResponse {
    event: String,
    code: Option<String>,
    msg: Option<String>,
}

/// WebSocket trade request for Weex.
#[derive(Debug, Serialize)]
struct WsTradeRequest {
    id: String,
    op: String,
    args: Vec<WsTradeArgs>,
}

#[derive(Debug, Serialize)]
struct WsTradeArgs {
    #[serde(rename = "instType")]
    inst_type: String,
    channel: String,
    #[serde(flatten)]
    data: serde_json::Value,
}

/// WebSocket trade response from Weex.
#[derive(Debug, Deserialize)]
struct WsTradeResponse {
    #[allow(dead_code)]
    id: Option<String>,
    #[allow(dead_code)]
    op: Option<String>,
    code: String,
    msg: Option<String>,
    data: Option<serde_json::Value>,
}

/// WebSocket subscription request.
#[derive(Debug, Serialize)]
struct WsSubscribeRequest {
    op: String,
    args: Vec<WsSubscribeArgs>,
}

#[derive(Debug, Serialize)]
struct WsSubscribeArgs {
    #[serde(rename = "instType")]
    inst_type: String,
    channel: String,
    #[serde(rename = "instId")]
    inst_id: String,
}

#[async_trait]
impl ExchangeTrader for WeexTrader {
    fn exchange_name(&self) -> &str {
        "weex"
    }

    fn market_type(&self) -> MarketType {
        self.market_type
    }

    fn capabilities(&self) -> ExchangeCapabilities {
        sdktrader_core::exchange_capabilities! {
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
        let response = self
            .client
            .get(format!("{}/api/v2/public/time", self.base_url))
            .send()
            .await?;
        Ok(response.status().is_success())
    }

    async fn get_server_time(&self) -> Result<i64> {
        let response: serde_json::Value = self
            .client
            .get(format!("{}/api/v2/public/time", self.base_url))
            .send()
            .await?
            .json()
            .await?;
        Ok(response["data"]["serverTime"]
            .as_i64()
            .or_else(|| {
                response["data"]["serverTime"]
                    .as_str()
                    .and_then(|s| s.parse().ok())
            })
            .unwrap_or(timestamp_ms()))
    }

    async fn get_balance(&self) -> Result<AccountBalance> {
        let request_path = "/api/v2/mix/account/accounts";
        let query = format!("?productType={}", self.product_type());
        let full_path = format!("{}{}", request_path, query);

        let headers = self.auth_headers("GET", &full_path, "");

        let mut request = self.client.get(format!("{}{}", self.base_url, full_path));

        for (key, value) in headers {
            request = request.header(&key, &value);
        }

        let response: serde_json::Value = request.send().await?.json().await?;

        debug!("Balance response: {:?}", response);

        if response["code"].as_str() == Some("00000") {
            let data = response["data"]
                .as_array()
                .and_then(|arr| arr.first())
                .cloned()
                .unwrap_or(serde_json::json!({}));

            let available = data["available"]
                .as_str()
                .and_then(|s| s.parse().ok())
                .unwrap_or(0.0);
            let equity = data["accountEquity"]
                .as_str()
                .and_then(|s| s.parse().ok())
                .unwrap_or(0.0);
            let unrealized_pnl = data["unrealizedPL"].as_str().and_then(|s| s.parse().ok());

            Ok(AccountBalance {
                total_balance: equity,
                available_balance: available,
                currency: "USDT".to_string(),
                margin_balance: Some(equity),
                used_margin: data["locked"].as_str().and_then(|s| s.parse().ok()),
                unrealized_pnl,
                realized_pnl: None,
                balances: vec![],
            })
        } else {
            Ok(AccountBalance::simple(0.0, 0.0, "USDT"))
        }
    }

    async fn get_current_price(&self, symbol: &str) -> Result<f64> {
        let formatted_symbol = self.format_symbol(symbol);
        let url = match self.market_type {
            MarketType::Spot => format!(
                "{}/api/v2/spot/market/tickers?symbol={}",
                self.base_url, formatted_symbol
            ),
            MarketType::Swap => format!(
                "{}/api/v2/mix/market/ticker?symbol={}&productType={}",
                self.base_url,
                formatted_symbol,
                self.product_type()
            ),
        };

        let response: serde_json::Value = self.client.get(&url).send().await?.json().await?;

        let price = match self.market_type {
            MarketType::Spot => response["data"][0]["lastPr"]
                .as_str()
                .and_then(|s| s.parse().ok())
                .unwrap_or(0.0),
            MarketType::Swap => response["data"]["lastPr"]
                .as_str()
                .and_then(|s| s.parse().ok())
                .unwrap_or(0.0),
        };

        Ok(price)
    }

    async fn get_ticker(&self, symbol: &str) -> Result<TickerInfo> {
        let formatted_symbol = self.format_symbol(symbol);

        let url = match self.market_type {
            MarketType::Spot => format!(
                "{}/api/v2/spot/market/tickers?symbol={}",
                self.base_url, formatted_symbol
            ),
            MarketType::Swap => format!(
                "{}/api/v2/mix/market/ticker?symbol={}&productType={}",
                self.base_url,
                formatted_symbol,
                self.product_type()
            ),
        };

        let response: serde_json::Value = self.client.get(&url).send().await?.json().await?;

        let data = match self.market_type {
            MarketType::Spot => response["data"]
                .get(0)
                .cloned()
                .unwrap_or(serde_json::json!({})),
            MarketType::Swap => response["data"].clone(),
        };

        Ok(TickerInfo {
            symbol: formatted_symbol,
            last_price: data["lastPr"]
                .as_str()
                .and_then(|s| s.parse().ok())
                .unwrap_or(0.0),
            bid_price: data["bidPr"].as_str().and_then(|s| s.parse().ok()),
            ask_price: data["askPr"].as_str().and_then(|s| s.parse().ok()),
            volume_24h: data["baseVolume"]
                .as_str()
                .and_then(|s| s.parse().ok())
                .or_else(|| data["baseVol"].as_str().and_then(|s| s.parse().ok())),
            turnover_24h: data["quoteVolume"]
                .as_str()
                .and_then(|s| s.parse().ok())
                .or_else(|| data["quoteVol"].as_str().and_then(|s| s.parse().ok())),
            high_24h: data["high24h"].as_str().and_then(|s| s.parse().ok()),
            low_24h: data["low24h"].as_str().and_then(|s| s.parse().ok()),
            change_24h: data["change24h"].as_str().and_then(|s| s.parse().ok()),
            open_interest: data["openInterest"]
                .as_str()
                .and_then(|s| s.parse().ok())
                .or_else(|| data["holdingAmount"].as_str().and_then(|s| s.parse().ok())),
            open_interest_value: None,
            funding_rate: data["fundingRate"].as_str().and_then(|s| s.parse().ok()),
            next_funding_time: data["nextFundingTime"]
                .as_str()
                .and_then(|s| s.parse().ok()),
            mark_price: data["markPrice"].as_str().and_then(|s| s.parse().ok()),
            index_price: data["indexPrice"].as_str().and_then(|s| s.parse().ok()),
            timestamp: data["ts"]
                .as_str()
                .and_then(|s| s.parse().ok())
                .or_else(|| data["timestamp"].as_str().and_then(|s| s.parse().ok()))
                .unwrap_or(timestamp_ms()),
        })
    }

    async fn get_orderbook(&self, symbol: &str, depth: u32) -> Result<Orderbook> {
        let formatted_symbol = self.format_symbol(symbol);

        let url = match self.market_type {
            MarketType::Spot => format!(
                "{}/api/v2/spot/market/orderbook?symbol={}&limit={}",
                self.base_url, formatted_symbol, depth
            ),
            MarketType::Swap => format!(
                "{}/api/v2/mix/market/depth?symbol={}&productType={}&limit={}",
                self.base_url,
                formatted_symbol,
                self.product_type(),
                depth
            ),
        };

        let response: serde_json::Value = self.client.get(&url).send().await?.json().await?;

        let data = &response["data"];

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
            timestamp: data["ts"]
                .as_str()
                .and_then(|s| s.parse().ok())
                .or_else(|| data["timestamp"].as_str().and_then(|s| s.parse().ok()))
                .unwrap_or(timestamp_ms()),
        })
    }

    async fn set_leverage(&self, symbol: Option<&str>, leverage: u32) -> Result<bool> {
        let Some(symbol) = symbol else {
            return Ok(true);
        };

        let formatted_symbol = self.format_symbol(symbol);
        let request_path = "/api/v2/mix/account/set-leverage";

        let body = serde_json::json!({
            "symbol": formatted_symbol,
            "productType": self.product_type(),
            "marginCoin": "USDT",
            "leverage": leverage.to_string(),
            "holdSide": "long",
        });

        let body_str = serde_json::to_string(&body)?;
        let headers = self.auth_headers("POST", request_path, &body_str);

        let mut request = self
            .client
            .post(format!("{}{}", self.base_url, request_path));

        for (key, value) in headers {
            request = request.header(&key, &value);
        }

        let response: serde_json::Value =
            request.body(body_str.clone()).send().await?.json().await?;

        debug!("Set leverage response (long): {:?}", response);

        // Also set for short side
        let body = serde_json::json!({
            "symbol": formatted_symbol,
            "productType": self.product_type(),
            "marginCoin": "USDT",
            "leverage": leverage.to_string(),
            "holdSide": "short",
        });

        let body_str = serde_json::to_string(&body)?;
        let headers = self.auth_headers("POST", request_path, &body_str);

        let mut request = self
            .client
            .post(format!("{}{}", self.base_url, request_path));

        for (key, value) in headers {
            request = request.header(&key, &value);
        }

        let response2: serde_json::Value = request.body(body_str).send().await?.json().await?;

        debug!("Set leverage response (short): {:?}", response2);

        Ok(response["code"].as_str() == Some("00000")
            || response2["code"].as_str() == Some("00000"))
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
                error!("Failed to set leverage: {}", e);
            }
        }

        // Determine side
        let side = if direction.to_lowercase() == "long" {
            OrderSide::Buy
        } else {
            OrderSide::Sell
        };

        // Place order via REST API (more reliable under load)
        self.place_order_rest(
            &formatted_symbol,
            side,
            OrderType::Market,
            quantity,
            None,
            false,
        )
        .await
    }

    async fn place_market_order(
        &self,
        symbol: &str,
        side: OrderSide,
        quantity: f64,
    ) -> Result<OrderResult> {
        let formatted_symbol = self.format_symbol(symbol);

        // Try WebSocket first, fall back to REST
        match self
            .place_order_ws(
                &formatted_symbol,
                side,
                OrderType::Market,
                quantity,
                None,
                false,
            )
            .await
        {
            Ok(result) => Ok(result),
            Err(e) => {
                warn!("WS order failed, falling back to REST: {}", e);
                self.place_order_rest(
                    &formatted_symbol,
                    side,
                    OrderType::Market,
                    quantity,
                    None,
                    false,
                )
                .await
            }
        }
    }

    async fn place_limit_order(
        &self,
        symbol: &str,
        side: OrderSide,
        quantity: f64,
        price: f64,
        reduce_only: bool,
    ) -> Result<OrderResult> {
        let formatted_symbol = self.format_symbol(symbol);

        // Try WebSocket first, fall back to REST
        match self
            .place_order_ws(
                &formatted_symbol,
                side,
                OrderType::Limit,
                quantity,
                Some(price),
                reduce_only,
            )
            .await
        {
            Ok(result) => Ok(result),
            Err(e) => {
                warn!("WS order failed, falling back to REST: {}", e);
                self.place_order_rest(
                    &formatted_symbol,
                    side,
                    OrderType::Limit,
                    quantity,
                    Some(price),
                    reduce_only,
                )
                .await
            }
        }
    }

    async fn cancel_order(&self, symbol: &str, order_id: &str) -> Result<bool> {
        // Try WebSocket first, fall back to REST
        match self.cancel_order_ws(symbol, order_id).await {
            Ok(result) => Ok(result),
            Err(e) => {
                warn!("WS cancel failed, falling back to REST: {}", e);
                self.cancel_order_rest(symbol, order_id).await
            }
        }
    }

    async fn get_order_status(&self, symbol: &str, order_id: &str) -> Result<OrderInfo> {
        let formatted_symbol = self.format_symbol(symbol);
        let request_path = "/api/v2/mix/order/detail";
        let query = format!(
            "?symbol={}&productType={}&orderId={}",
            formatted_symbol,
            self.product_type(),
            order_id
        );
        let full_path = format!("{}{}", request_path, query);

        let headers = self.auth_headers("GET", &full_path, "");

        let mut request = self.client.get(format!("{}{}", self.base_url, full_path));

        for (key, value) in headers {
            request = request.header(&key, &value);
        }

        let response: serde_json::Value = request.send().await?.json().await?;

        debug!("Order status response: {:?}", response);

        if response["code"].as_str() != Some("00000") {
            anyhow::bail!("Failed to get order status: {:?}", response["msg"]);
        }

        let data = &response["data"];

        let status_str = data["state"]
            .as_str()
            .or_else(|| data["status"].as_str())
            .unwrap_or("new");

        let side_str = data["side"].as_str().unwrap_or("buy");
        let side = if side_str.contains("buy") || side_str.contains("long") {
            OrderSide::Buy
        } else {
            OrderSide::Sell
        };

        let order_type_str = data["orderType"].as_str().unwrap_or("market");
        let order_type = if order_type_str == "limit" {
            OrderType::Limit
        } else {
            OrderType::Market
        };

        let quantity: f64 = data["size"]
            .as_str()
            .and_then(|s| s.parse().ok())
            .or_else(|| data["sz"].as_str().and_then(|s| s.parse().ok()))
            .unwrap_or(0.0);

        let filled_quantity: f64 = data["fillSize"]
            .as_str()
            .and_then(|s| s.parse().ok())
            .or_else(|| data["fillSz"].as_str().and_then(|s| s.parse().ok()))
            .unwrap_or(0.0);

        Ok(OrderInfo {
            order_id: order_id.to_string(),
            client_order_id: data["clientOid"]
                .as_str()
                .or_else(|| data["clOrdId"].as_str())
                .map(|s| s.to_string()),
            symbol: formatted_symbol,
            status: parse_order_status_str(status_str),
            side,
            order_type,
            price: data["price"]
                .as_str()
                .and_then(|s| s.parse().ok())
                .or_else(|| data["px"].as_str().and_then(|s| s.parse().ok()))
                .unwrap_or(0.0),
            quantity,
            filled_quantity,
            remaining_quantity: quantity - filled_quantity,
            avg_price: data["avgPx"]
                .as_str()
                .and_then(|s| s.parse().ok())
                .or_else(|| data["priceAvg"].as_str().and_then(|s| s.parse().ok())),
            timestamp: data["cTime"]
                .as_str()
                .and_then(|s| s.parse().ok())
                .or_else(|| data["createTime"].as_str().and_then(|s| s.parse().ok()))
                .unwrap_or(0),
            time_in_force: Default::default(),
            reduce_only: data["reduceOnly"].as_bool().unwrap_or(false),
        })
    }

    async fn close_position(&self, symbol: &str) -> Result<OrderResult> {
        let formatted_symbol = self.format_symbol(symbol);
        let request_path = "/api/v2/mix/order/close-positions";

        let body = serde_json::json!({
            "symbol": formatted_symbol,
            "productType": self.product_type(),
            "holdSide": "long",
        });

        let body_str = serde_json::to_string(&body)?;
        let headers = self.auth_headers("POST", request_path, &body_str);

        let mut request = self
            .client
            .post(format!("{}{}", self.base_url, request_path));

        for (key, value) in headers {
            request = request.header(&key, &value);
        }

        let response: serde_json::Value =
            request.body(body_str.clone()).send().await?.json().await?;

        debug!("Close position response (long): {:?}", response);

        // Also close short position
        let body = serde_json::json!({
            "symbol": formatted_symbol,
            "productType": self.product_type(),
            "holdSide": "short",
        });

        let body_str = serde_json::to_string(&body)?;
        let headers = self.auth_headers("POST", request_path, &body_str);

        let mut request = self
            .client
            .post(format!("{}{}", self.base_url, request_path));

        for (key, value) in headers {
            request = request.header(&key, &value);
        }

        let response2: serde_json::Value = request.body(body_str).send().await?.json().await?;

        debug!("Close position response (short): {:?}", response2);

        if response["code"].as_str() == Some("00000") || response2["code"].as_str() == Some("00000")
        {
            Ok(OrderResult {
                success: true,
                order_id: response["data"]["orderId"]
                    .as_str()
                    .or_else(|| response2["data"]["orderId"].as_str())
                    .map(|s| s.to_string()),
                symbol: formatted_symbol,
                side: OrderSide::Sell,
                order_type: OrderType::Market,
                quantity: None,
                entry_price: None,
                position_size: None,
                position_usdt: None,
                error: None,
                raw_response: None,
            })
        } else {
            Ok(OrderResult::failure(
                formatted_symbol,
                OrderSide::Sell,
                format!(
                    "Failed to close position: {:?}",
                    response["msg"]
                        .as_str()
                        .or_else(|| response2["msg"].as_str())
                ),
            ))
        }
    }

    fn format_symbol(&self, base: &str) -> String {
        let s = base.to_uppercase();
        // Handle BTC/USDT format
        if s.contains('/') {
            return s.replace("/", "");
        }
        if s.ends_with("USDT") {
            s
        } else {
            format!("{}USDT", s)
        }
    }

    async fn test_ws_orderbook(
        &self,
        symbol: &str,
        depth: u32,
        timeout_ms: u64,
    ) -> Result<WsTestResult> {
        let formatted_symbol = self.format_symbol(symbol);
        let ws_url = self.ws_public_url();

        let start = std::time::Instant::now();
        let (mut ws_stream, _) = connect_async(ws_url)
            .await
            .context("Failed to connect to Weex public WebSocket")?;

        let connect_time = start.elapsed().as_millis() as u64;

        // Subscribe to orderbook
        let channel = if depth <= 5 {
            "books5"
        } else if depth <= 15 {
            "books15"
        } else {
            "books"
        };

        let sub_msg = WsSubscribeRequest {
            op: "subscribe".to_string(),
            args: vec![WsSubscribeArgs {
                inst_type: self.inst_type().to_string(),
                channel: channel.to_string(),
                inst_id: formatted_symbol.clone(),
            }],
        };

        ws_stream
            .send(Message::Text(serde_json::to_string(&sub_msg)?))
            .await?;

        let mut messages_received = 0u32;
        let mut data_sample = None;

        let timeout = tokio::time::Duration::from_millis(timeout_ms);
        let deadline = tokio::time::Instant::now() + timeout;

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
                _ = tokio::time::sleep_until(deadline) => {
                    break;
                }
            }
        }

        let _ = ws_stream.close(None).await;

        Ok(WsTestResult {
            connected: messages_received > 0,
            latency_ms: connect_time,
            messages_received,
            subscription_type: format!("orderbook_{}", channel),
            data_sample,
        })
    }

    async fn test_ws_trades(&self, symbol: &str, timeout_ms: u64) -> Result<WsTestResult> {
        let formatted_symbol = self.format_symbol(symbol);
        let ws_url = self.ws_public_url();

        let start = std::time::Instant::now();
        let (mut ws_stream, _) = connect_async(ws_url)
            .await
            .context("Failed to connect to Weex public WebSocket")?;

        let connect_time = start.elapsed().as_millis() as u64;

        // Subscribe to trades
        let sub_msg = WsSubscribeRequest {
            op: "subscribe".to_string(),
            args: vec![WsSubscribeArgs {
                inst_type: self.inst_type().to_string(),
                channel: "trade".to_string(),
                inst_id: formatted_symbol.clone(),
            }],
        };

        ws_stream
            .send(Message::Text(serde_json::to_string(&sub_msg)?))
            .await?;

        let mut messages_received = 0u32;
        let mut data_sample = None;

        let timeout = tokio::time::Duration::from_millis(timeout_ms);
        let deadline = tokio::time::Instant::now() + timeout;

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
                _ = tokio::time::sleep_until(deadline) => {
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
        let ws_url = self.ws_public_url();

        let start = std::time::Instant::now();
        let (mut ws_stream, _) = connect_async(ws_url)
            .await
            .context("Failed to connect to Weex public WebSocket")?;

        let connect_time = start.elapsed().as_millis() as u64;

        // Subscribe to ticker
        let sub_msg = WsSubscribeRequest {
            op: "subscribe".to_string(),
            args: vec![WsSubscribeArgs {
                inst_type: self.inst_type().to_string(),
                channel: "ticker".to_string(),
                inst_id: formatted_symbol.clone(),
            }],
        };

        ws_stream
            .send(Message::Text(serde_json::to_string(&sub_msg)?))
            .await?;

        let mut messages_received = 0u32;
        let mut data_sample = None;

        let timeout = tokio::time::Duration::from_millis(timeout_ms);
        let deadline = tokio::time::Instant::now() + timeout;

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
                _ = tokio::time::sleep_until(deadline) => {
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
        let testnet = self.testnet;
        let inst_type = self.inst_type().to_string();
        let exchange_name = "weex".to_string();

        let handle = tokio::spawn(async move {
            let mut reconnect_delay = INITIAL_RECONNECT_DELAY_SECS;

            loop {
                let ws_url = if testnet {
                    TESTNET_WS_PRIVATE_URL
                } else {
                    MAINNET_WS_PRIVATE_URL
                };
                info!("[{}] Connecting to private WS: {}", exchange_name, ws_url);

                match connect_async(ws_url).await {
                    Ok((mut ws_stream, _)) => {
                        // Authenticate
                        let timestamp = timestamp_ms() / 1000;
                        let message = format!("{}GET/user/verify", timestamp);
                        let signature = {
                            use hmac::{Hmac, Mac};
                            use sha2::Sha256;
                            type HmacSha256 = Hmac<Sha256>;
                            let mut mac = HmacSha256::new_from_slice(api_secret.as_bytes())
                                .expect("HMAC can take key of any size");
                            mac.update(message.as_bytes());
                            general_purpose::STANDARD.encode(mac.finalize().into_bytes())
                        };

                        let auth_msg = serde_json::json!({
                            "op": "login",
                            "args": [{
                                "apiKey": api_key,
                                "passphrase": passphrase,
                                "timestamp": timestamp.to_string(),
                                "sign": signature
                            }]
                        });

                        if let Err(e) = ws_stream.send(Message::Text(auth_msg.to_string())).await {
                            error!("[{}] Failed to send login: {}", exchange_name, e);
                            tokio::time::sleep(tokio::time::Duration::from_secs(reconnect_delay))
                                .await;
                            reconnect_delay = next_reconnect_delay(reconnect_delay);
                            continue;
                        }

                        let mut authenticated = false;
                        if let Some(Ok(Message::Text(text))) = ws_stream.next().await {
                            if let Ok(resp) = serde_json::from_str::<serde_json::Value>(&text) {
                                if resp.get("event").and_then(|v| v.as_str()) == Some("login")
                                    && resp.get("code").and_then(|v| v.as_str()) == Some("0")
                                {
                                    info!("[{}] Private WS authenticated", exchange_name);
                                    authenticated = true;
                                } else {
                                    error!("[{}] Login failed: {}", exchange_name, text);
                                }
                            }
                        }

                        if !authenticated {
                            tokio::time::sleep(tokio::time::Duration::from_secs(reconnect_delay))
                                .await;
                            reconnect_delay = next_reconnect_delay(reconnect_delay);
                            continue;
                        }

                        // Subscribe to orders
                        let sub_msg = serde_json::json!({
                            "op": "subscribe",
                            "args": [{"instType": inst_type, "channel": "orders", "instId": "default"}]
                        });

                        if let Err(e) = ws_stream.send(Message::Text(sub_msg.to_string())).await {
                            error!("[{}] Failed to subscribe: {}", exchange_name, e);
                            tokio::time::sleep(tokio::time::Duration::from_secs(reconnect_delay))
                                .await;
                            reconnect_delay = next_reconnect_delay(reconnect_delay);
                            continue;
                        }

                        info!("[{}] Subscribed to private order updates", exchange_name);
                        reconnect_delay = INITIAL_RECONNECT_DELAY_SECS;

                        let mut ping_interval =
                            tokio::time::interval(tokio::time::Duration::from_secs(25));

                        loop {
                            tokio::select! {
                                msg = ws_stream.next() => {
                                    match msg {
                                        Some(Ok(Message::Text(text))) => {
                                            if let Ok(msg_json) = serde_json::from_str::<serde_json::Value>(&text) {
                                                if msg_json.get("event").is_some() || msg_json.get("op").is_some() {
                                                    debug!("[{}] WS event: {}", exchange_name, text);
                                                    continue;
                                                }

                                                if let Some(data_arr) = msg_json.get("data").and_then(|d| d.as_array()) {
                                                    for o in data_arr {
                                                        let event = OrderUpdateEvent {
                                                            exchange: exchange_name.clone(),
                                                            order_id: o.get("orderId").and_then(|v| v.as_str()).unwrap_or("").to_string(),
                                                            client_order_id: o.get("clOrdId").and_then(|v| v.as_str()).map(|s| s.to_string()),
                                                            symbol: o.get("instId").and_then(|v| v.as_str()).unwrap_or("").to_string(),
                                                            status: parse_order_status_str(
                                                                o.get("status").and_then(|v| v.as_str()).unwrap_or("new")
                                                            ),
                                                            side: if o.get("side").and_then(|v| v.as_str()) == Some("buy") { OrderSide::Buy } else { OrderSide::Sell },
                                                            order_type: if o.get("ordType").and_then(|v| v.as_str()) == Some("market") { OrderType::Market } else { OrderType::Limit },
                                                            price: o.get("px").and_then(|v| v.as_str()).and_then(|s| s.parse().ok()).unwrap_or(0.0),
                                                            quantity: o.get("sz").and_then(|v| v.as_str()).and_then(|s| s.parse().ok()).unwrap_or(0.0),
                                                            filled_quantity: o.get("fillSz").and_then(|v| v.as_str()).and_then(|s| s.parse().ok()).unwrap_or(0.0),
                                                            avg_fill_price: o.get("avgPx").and_then(|v| v.as_str()).and_then(|s| s.parse().ok()),
                                                            fee: o.get("fee").and_then(|v| v.as_str()).and_then(|s| s.parse().ok()),
                                                            fee_currency: o.get("feeCcy").and_then(|v| v.as_str()).map(|s| s.to_string()),
                                                            exchange_timestamp: o.get("cTime").and_then(|v| v.as_str()).and_then(|s| s.parse().ok()).unwrap_or(0),
                                                            received_at: timestamp_ms(),
                                                            raw: Some(o.clone()),
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
                                    if let Err(e) = ws_stream.send(Message::Text("ping".into())).await {
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
        let trader = WeexTrader::new("", "", None, false, MarketType::Swap);
        assert_eq!(trader.format_symbol("BTC"), "BTCUSDT");
        assert_eq!(trader.format_symbol("BTCUSDT"), "BTCUSDT");
        assert_eq!(trader.format_symbol("eth"), "ETHUSDT");
        assert_eq!(trader.format_symbol("BTC/USDT"), "BTCUSDT");
        assert_eq!(trader.format_symbol("ETH/USDT"), "ETHUSDT");
    }

    #[test]
    fn test_inst_type() {
        let spot_trader = WeexTrader::new("", "", None, false, MarketType::Spot);
        assert_eq!(spot_trader.inst_type(), "SPOT");

        let swap_trader = WeexTrader::new("", "", None, false, MarketType::Swap);
        assert_eq!(swap_trader.inst_type(), "USDT-FUTURES");
    }

    #[test]
    fn test_ws_urls() {
        let mainnet = WeexTrader::new("", "", None, false, MarketType::Spot);
        assert_eq!(mainnet.ws_private_url(), MAINNET_WS_PRIVATE_URL);

        let testnet = WeexTrader::new("", "", None, true, MarketType::Spot);
        assert_eq!(testnet.ws_private_url(), TESTNET_WS_PRIVATE_URL);
    }

    #[test]
    fn test_parse_order_status() {
        assert_eq!(parse_order_status_str("new"), OrderStatus::New);
        assert_eq!(parse_order_status_str("filled"), OrderStatus::Filled);
        assert_eq!(parse_order_status_str("cancelled"), OrderStatus::Cancelled);
        assert_eq!(
            parse_order_status_str("partially_filled"),
            OrderStatus::PartiallyFilled
        );
    }
}
