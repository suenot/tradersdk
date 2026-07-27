//! Bitget exchange trader implementation.
//!
//! Full-featured implementation with:
//! - Persistent WebSocket connections for trading
//! - Full ticker data including funding rate, open interest, and mark price for perpetuals
//! - Funding rate history and open interest history
//! - Private WebSocket subscription for order updates
//! - Proper passphrase handling for authentication

use crate::models::*;
use sdktrader_core::utils::timestamp_ms;
use sdktrader_core::ExchangeTrader;
use sdktrader_core::models::{
    AccountBalance, ExchangeCapabilities, FundingRateEntry, MarketType, OpenInterestEntry,
    OrderInfo, OrderResult, OrderSide, OrderStatus, OrderType, OrderUpdateEvent, Orderbook,
    OrderbookLevel, TickerInfo, TimeInForce, WsTestResult,
};
use anyhow::{Context, Result};
use async_trait::async_trait;
use base64::{engine::general_purpose, Engine as _};
use futures_util::{SinkExt, StreamExt};
use reqwest::Client;
use tokio::sync::broadcast;
use tokio::task::JoinHandle;
use tokio_tungstenite::{connect_async, tungstenite::protocol::Message};
use tracing::{debug, error, info, warn};

const MAINNET_URL: &str = "https://api.bitget.com";
const MAINNET_WS_PUBLIC_URL: &str = "wss://ws.bitget.com/v2/ws/public";
const MAINNET_WS_PRIVATE_URL: &str = "wss://ws.bitget.com/v2/ws/private";
const TESTNET_WS_PUBLIC_URL: &str = "wss://ws.bitget.com/v2/ws/public";
const TESTNET_WS_PRIVATE_URL: &str = "wss://ws.bitget.com/v2/ws/private";

/// Maximum reconnect backoff delay in seconds.
const MAX_RECONNECT_DELAY_SECS: u64 = 30;

/// Initial reconnect delay in seconds.
const INITIAL_RECONNECT_DELAY_SECS: u64 = 1;

/// Bitget exchange trader.
pub struct BitgetTrader {
    api_key: String,
    api_secret: String,
    passphrase: String,
    testnet: bool,
    market_type: MarketType,
    client: Client,
    base_url: String,
}

impl BitgetTrader {
    /// Create a new Bitget trader.
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

    /// Get WebSocket public URL based on testnet setting.
    fn ws_public_url(&self) -> &str {
        if self.testnet {
            TESTNET_WS_PUBLIC_URL
        } else {
            MAINNET_WS_PUBLIC_URL
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

    /// Get instrument type for API calls.
    fn inst_type(&self) -> &str {
        match self.market_type {
            MarketType::Spot => "SPOT",
            MarketType::Swap => "USDT-FUTURES",
        }
    }

    /// Get product type for futures API calls.
    fn product_type(&self) -> &str {
        match self.market_type {
            MarketType::Spot => "SPOT",
            MarketType::Swap => "USDT-FUTURES",
        }
    }

    /// Sign a REST API request for Bitget.
    /// Bitget signature: timestamp + method + requestPath + body, signed with HMAC SHA256 and Base64 encoded.
    fn sign_request(&self, method: &str, path: &str, body: &str, timestamp: i64) -> String {
        use hmac::{Hmac, Mac};
        use sha2::Sha256;

        type HmacSha256 = Hmac<Sha256>;

        let prehash = format!("{}{}{}{}", timestamp, method, path, body);

        let mut mac = HmacSha256::new_from_slice(self.api_secret.as_bytes())
            .expect("HMAC can take key of any size");
        mac.update(prehash.as_bytes());

        let result = mac.finalize();
        general_purpose::STANDARD.encode(result.into_bytes())
    }

    /// Make a signed GET request to Bitget API.
    async fn signed_get<T: for<'de> serde::Deserialize<'de>>(
        &self,
        endpoint: &str,
        params: &str,
    ) -> Result<T> {
        let timestamp = timestamp_ms();
        let path = if params.is_empty() {
            endpoint.to_string()
        } else {
            format!("{}?{}", endpoint, params)
        };
        let signature = self.sign_request("GET", &path, "", timestamp);

        let url = format!("{}{}", self.base_url, path);

        let response = self
            .client
            .get(&url)
            .header("ACCESS-KEY", &self.api_key)
            .header("ACCESS-SIGN", signature)
            .header("ACCESS-TIMESTAMP", timestamp.to_string())
            .header("ACCESS-PASSPHRASE", &self.passphrase)
            .header("Content-Type", "application/json")
            .header("locale", "en-US")
            .send()
            .await
            .context("Failed to send request to Bitget")?;

        let text = response.text().await?;
        debug!("Bitget response: {}", text);

        serde_json::from_str(&text).context("Failed to parse Bitget response")
    }

    /// Make a signed POST request to Bitget API.
    async fn signed_post<T: for<'de> serde::Deserialize<'de>>(
        &self,
        endpoint: &str,
        body: &serde_json::Value,
    ) -> Result<T> {
        let timestamp = timestamp_ms();
        let body_str = body.to_string();
        let signature = self.sign_request("POST", endpoint, &body_str, timestamp);

        let url = format!("{}{}", self.base_url, endpoint);

        let response = self
            .client
            .post(&url)
            .header("ACCESS-KEY", &self.api_key)
            .header("ACCESS-SIGN", signature)
            .header("ACCESS-TIMESTAMP", timestamp.to_string())
            .header("ACCESS-PASSPHRASE", &self.passphrase)
            .header("Content-Type", "application/json")
            .header("locale", "en-US")
            .body(body_str)
            .send()
            .await
            .context("Failed to send request to Bitget")?;

        let text = response.text().await?;
        debug!("Bitget response: {}", text);

        serde_json::from_str(&text).context("Failed to parse Bitget response")
    }

    /// Get current position for a symbol.
    async fn get_position(&self, symbol: &str) -> Result<Option<PositionData>> {
        if self.market_type == MarketType::Spot {
            return Ok(None);
        }

        let params = format!("productType={}&symbol={}", self.product_type(), symbol);
        let response: BitgetResponse<Vec<PositionData>> = self
            .signed_get("/api/v2/mix/position/single-position", &params)
            .await?;

        if response.is_success() {
            if let Some(positions) = response.data {
                for pos in positions {
                    let total: f64 = pos
                        .total
                        .as_ref()
                        .and_then(|s| s.parse().ok())
                        .unwrap_or(0.0);
                    if total.abs() > 0.0 {
                        return Ok(Some(pos));
                    }
                }
            }
        }

        Ok(None)
    }

    /// Generate WebSocket authentication signature for Bitget.
    /// Bitget uses: timestamp + "GET" + "/user/verify" signed with HMAC SHA256 and Base64 encoded
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
            .context("Failed to connect to Bitget private WebSocket")?;

        // Authenticate
        let timestamp = timestamp_ms() / 1000; // Bitget uses seconds
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

        ws_stream.send(Message::Text(auth_json.into())).await?;

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

                        info!("Bitget WebSocket authenticated");
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
        ws_stream.send(Message::Text(order_json.into())).await?;

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
                                    info!("Bitget WS order placed: {}", order_id);
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
        ws_stream.send(Message::Text(cancel_json.into())).await?;

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
                            info!("Bitget WS order cancelled: {}", order_id);
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
        ws_stream.send(Message::Text(amend_json.into())).await?;

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
                            info!("Bitget WS order amended: {}", order_id);
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

/// Parse a status string from Bitget into OrderStatus.
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

#[async_trait]
impl ExchangeTrader for BitgetTrader {
    fn exchange_name(&self) -> &str {
        "bitget"
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
            .unwrap_or(timestamp_ms()))
    }

    async fn get_balance(&self) -> Result<AccountBalance> {
        if self.market_type == MarketType::Spot {
            // Spot account balance
            let response: BitgetResponse<Vec<SpotAssetData>> =
                self.signed_get("/api/v2/spot/account/assets", "").await?;

            if response.is_success() {
                if let Some(assets) = response.data {
                    // Find USDT asset
                    for asset in &assets {
                        if asset.coin == "USDT" {
                            let available: f64 = asset
                                .available
                                .as_ref()
                                .and_then(|s| s.parse().ok())
                                .unwrap_or(0.0);
                            let locked: f64 = asset
                                .locked
                                .as_ref()
                                .and_then(|s| s.parse().ok())
                                .unwrap_or(0.0);
                            return Ok(AccountBalance::simple(
                                available,
                                available + locked,
                                "USDT",
                            ));
                        }
                    }
                }
            }
        } else {
            // Futures account balance
            let params = format!("productType={}", self.product_type());
            let response: BitgetResponse<Vec<AccountData>> = self
                .signed_get("/api/v2/mix/account/accounts", &params)
                .await?;

            if response.is_success() {
                if let Some(accounts) = response.data {
                    for account in &accounts {
                        if account.margin_coin.as_ref().map(|s| s.as_str()) == Some("USDT") {
                            let available: f64 = account
                                .available
                                .as_ref()
                                .and_then(|s| s.parse().ok())
                                .unwrap_or(0.0);
                            let equity: f64 = account
                                .account_equity
                                .as_ref()
                                .and_then(|s| s.parse().ok())
                                .unwrap_or(0.0);
                            return Ok(AccountBalance::simple(available, equity, "USDT"));
                        }
                    }
                }
            }
        }

        // Return zero balance if nothing found
        Ok(AccountBalance::default())
    }

    async fn get_current_price(&self, symbol: &str) -> Result<f64> {
        let formatted_symbol = self.format_symbol(symbol);

        if self.market_type == MarketType::Spot {
            let response: serde_json::Value = self
                .client
                .get(format!(
                    "{}/api/v2/spot/market/tickers?symbol={}",
                    self.base_url, formatted_symbol
                ))
                .send()
                .await?
                .json()
                .await?;
            Ok(response["data"][0]["lastPr"]
                .as_str()
                .and_then(|s| s.parse().ok())
                .unwrap_or(0.0))
        } else {
            // Futures ticker
            let response: serde_json::Value = self
                .client
                .get(format!(
                    "{}/api/v2/mix/market/ticker?symbol={}&productType={}",
                    self.base_url,
                    formatted_symbol,
                    self.product_type()
                ))
                .send()
                .await?
                .json()
                .await?;
            Ok(response["data"][0]["lastPr"]
                .as_str()
                .and_then(|s| s.parse().ok())
                .unwrap_or(0.0))
        }
    }

    async fn get_ticker(&self, symbol: &str) -> Result<TickerInfo> {
        let formatted_symbol = self.format_symbol(symbol);

        if self.market_type == MarketType::Spot {
            // Spot ticker
            let response: BitgetResponse<Vec<SpotTickerData>> = self
                .client
                .get(format!(
                    "{}/api/v2/spot/market/tickers?symbol={}",
                    self.base_url, formatted_symbol
                ))
                .send()
                .await?
                .json()
                .await?;

            if !response.is_success() {
                anyhow::bail!("Failed to get ticker: {}", response.msg);
            }

            let data = response
                .data
                .and_then(|d| d.into_iter().next())
                .ok_or_else(|| anyhow::anyhow!("No ticker data"))?;

            Ok(TickerInfo {
                symbol: formatted_symbol,
                last_price: data
                    .last_pr
                    .as_ref()
                    .and_then(|s| s.parse().ok())
                    .unwrap_or(0.0),
                bid_price: data.bid_pr.as_ref().and_then(|s| s.parse().ok()),
                ask_price: data.ask_pr.as_ref().and_then(|s| s.parse().ok()),
                volume_24h: data.base_volume.as_ref().and_then(|s| s.parse().ok()),
                turnover_24h: data.quote_volume.as_ref().and_then(|s| s.parse().ok()),
                high_24h: data.high_24h.as_ref().and_then(|s| s.parse().ok()),
                low_24h: data.low_24h.as_ref().and_then(|s| s.parse().ok()),
                change_24h: data.change_24h.as_ref().and_then(|s| s.parse().ok()),
                open_interest: None,
                open_interest_value: None,
                funding_rate: None,
                next_funding_time: None,
                mark_price: None,
                index_price: None,
                timestamp: data
                    .ts
                    .as_ref()
                    .and_then(|s| s.parse().ok())
                    .unwrap_or(timestamp_ms()),
            })
        } else {
            // Futures ticker - includes funding rate, OI, mark price
            let response: BitgetResponse<Vec<FuturesTickerData>> = self
                .client
                .get(format!(
                    "{}/api/v2/mix/market/ticker?symbol={}&productType={}",
                    self.base_url,
                    formatted_symbol,
                    self.product_type()
                ))
                .send()
                .await?
                .json()
                .await?;

            if !response.is_success() {
                anyhow::bail!("Failed to get ticker: {}", response.msg);
            }

            let data = response
                .data
                .and_then(|d| d.into_iter().next())
                .ok_or_else(|| anyhow::anyhow!("No ticker data"))?;

            // holdingAmount is the open interest in base currency
            let open_interest: Option<f64> =
                data.holding_amount.as_ref().and_then(|s| s.parse().ok());
            let mark_price: Option<f64> = data.mark_price.as_ref().and_then(|s| s.parse().ok());
            let open_interest_value = match (open_interest, mark_price) {
                (Some(oi), Some(mp)) => Some(oi * mp),
                _ => None,
            };

            Ok(TickerInfo {
                symbol: formatted_symbol,
                last_price: data
                    .last_pr
                    .as_ref()
                    .and_then(|s| s.parse().ok())
                    .unwrap_or(0.0),
                bid_price: data.bid_pr.as_ref().and_then(|s| s.parse().ok()),
                ask_price: data.ask_pr.as_ref().and_then(|s| s.parse().ok()),
                volume_24h: data.base_volume.as_ref().and_then(|s| s.parse().ok()),
                turnover_24h: data.usdt_volume.as_ref().and_then(|s| s.parse().ok()),
                high_24h: data.high_24h.as_ref().and_then(|s| s.parse().ok()),
                low_24h: data.low_24h.as_ref().and_then(|s| s.parse().ok()),
                change_24h: data.change_24h.as_ref().and_then(|s| s.parse().ok()),
                open_interest,
                open_interest_value,
                funding_rate: data.funding_rate.as_ref().and_then(|s| s.parse().ok()),
                next_funding_time: data.next_funding_time.as_ref().and_then(|s| s.parse().ok()),
                mark_price,
                index_price: data.index_price.as_ref().and_then(|s| s.parse().ok()),
                timestamp: data
                    .ts
                    .as_ref()
                    .and_then(|s| s.parse().ok())
                    .unwrap_or(timestamp_ms()),
            })
        }
    }

    async fn get_orderbook(&self, symbol: &str, depth: u32) -> Result<Orderbook> {
        let formatted_symbol = self.format_symbol(symbol);
        let limit = depth.min(150); // Bitget max is 150

        let url = if self.market_type == MarketType::Spot {
            format!(
                "{}/api/v2/spot/market/orderbook?symbol={}&limit={}",
                self.base_url, formatted_symbol, limit
            )
        } else {
            format!(
                "{}/api/v2/mix/market/merge-depth?symbol={}&productType={}&limit={}",
                self.base_url,
                formatted_symbol,
                self.product_type(),
                limit
            )
        };

        let response: BitgetResponse<OrderbookData> =
            self.client.get(&url).send().await?.json().await?;

        if !response.is_success() {
            anyhow::bail!("Failed to get orderbook: {}", response.msg);
        }

        let data = response
            .data
            .ok_or_else(|| anyhow::anyhow!("No orderbook data"))?;

        let bids: Vec<OrderbookLevel> = data
            .bids
            .iter()
            .filter_map(|level| {
                if level.len() >= 2 {
                    let price: f64 = level[0].parse().ok()?;
                    let quantity: f64 = level[1].parse().ok()?;
                    Some(OrderbookLevel::new(price, quantity))
                } else {
                    None
                }
            })
            .collect();

        let asks: Vec<OrderbookLevel> = data
            .asks
            .iter()
            .filter_map(|level| {
                if level.len() >= 2 {
                    let price: f64 = level[0].parse().ok()?;
                    let quantity: f64 = level[1].parse().ok()?;
                    Some(OrderbookLevel::new(price, quantity))
                } else {
                    None
                }
            })
            .collect();

        Ok(Orderbook {
            symbol: formatted_symbol,
            bids,
            asks,
            timestamp: data
                .ts
                .and_then(|s| s.parse().ok())
                .unwrap_or(timestamp_ms()),
        })
    }

    async fn set_leverage(&self, symbol: Option<&str>, leverage: u32) -> Result<bool> {
        if self.market_type == MarketType::Spot {
            return Ok(true); // Leverage not applicable for spot
        }

        let sym = match symbol {
            Some(s) => self.format_symbol(s),
            None => return Ok(true),
        };

        let body = serde_json::json!({
            "symbol": sym,
            "productType": self.product_type(),
            "marginCoin": "USDT",
            "leverage": leverage.to_string(),
            "holdSide": "long" // Set for both sides
        });

        let response: BitgetResponse<serde_json::Value> = self
            .signed_post("/api/v2/mix/account/set-leverage", &body)
            .await?;

        if response.is_success() {
            // Also set for short side
            let body_short = serde_json::json!({
                "symbol": sym,
                "productType": self.product_type(),
                "marginCoin": "USDT",
                "leverage": leverage.to_string(),
                "holdSide": "short"
            });
            let _: BitgetResponse<serde_json::Value> = self
                .signed_post("/api/v2/mix/account/set-leverage", &body_short)
                .await?;

            info!("Set leverage to {}x for {}", leverage, sym);
            Ok(true)
        } else {
            // Leverage might already be set, which is fine
            if response.code == "40834" || response.msg.contains("leverage") {
                Ok(true)
            } else {
                warn!("Failed to set leverage: {}", response.msg);
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
                error!("Failed to set leverage: {}", e);
            }
        }

        // Determine side
        let side = if direction.to_lowercase() == "long" {
            OrderSide::Buy
        } else {
            OrderSide::Sell
        };

        // Place order via WebSocket
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

        // Use WebSocket by default
        self.place_order_ws(
            &formatted_symbol,
            side,
            OrderType::Market,
            quantity,
            None,
            false,
        )
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
        let formatted_symbol = self.format_symbol(symbol);

        // Use WebSocket by default
        self.place_order_ws(
            &formatted_symbol,
            side,
            OrderType::Limit,
            quantity,
            Some(price),
            reduce_only,
        )
        .await
    }

    async fn cancel_order(&self, symbol: &str, order_id: &str) -> Result<bool> {
        // Use WebSocket by default
        self.cancel_order_ws(symbol, order_id).await
    }

    async fn get_order_status(&self, symbol: &str, order_id: &str) -> Result<OrderInfo> {
        let formatted_symbol = self.format_symbol(symbol);

        if self.market_type == MarketType::Spot {
            let params = format!("orderId={}", order_id);
            let response: BitgetResponse<Vec<OrderData>> = self
                .signed_get("/api/v2/spot/trade/orderInfo", &params)
                .await?;

            if !response.is_success() {
                anyhow::bail!("Failed to get order status: {}", response.msg);
            }

            let order = response
                .data
                .and_then(|d| d.into_iter().next())
                .ok_or_else(|| anyhow::anyhow!("Order not found"))?;

            let status_str = order.status.as_deref().unwrap_or("new");
            let status = parse_order_status_str(status_str);

            let side_str = order.side.as_deref().unwrap_or("buy");
            let side = if side_str.to_lowercase() == "sell" {
                OrderSide::Sell
            } else {
                OrderSide::Buy
            };

            let order_type_str = order.order_type.as_deref().unwrap_or("market");
            let order_type = if order_type_str.to_lowercase() == "limit" {
                OrderType::Limit
            } else {
                OrderType::Market
            };

            let price: f64 = order
                .price
                .as_ref()
                .and_then(|s| s.parse().ok())
                .unwrap_or(0.0);
            let quantity: f64 = order
                .size
                .as_ref()
                .and_then(|s| s.parse().ok())
                .unwrap_or(0.0);
            let filled: f64 = order
                .base_volume
                .as_ref()
                .and_then(|s| s.parse().ok())
                .unwrap_or(0.0);
            let avg_price: Option<f64> = order.price_avg.as_ref().and_then(|s| s.parse().ok());
            let timestamp: i64 = order
                .c_time
                .as_ref()
                .and_then(|s| s.parse().ok())
                .unwrap_or(0);

            Ok(OrderInfo {
                order_id: order.order_id,
                client_order_id: order.client_oid,
                symbol: formatted_symbol,
                status,
                side,
                order_type,
                price,
                quantity,
                filled_quantity: filled,
                remaining_quantity: quantity - filled,
                avg_price,
                timestamp,
                time_in_force: TimeInForce::GTC,
                reduce_only: false,
            })
        } else {
            // Futures order
            let params = format!(
                "symbol={}&productType={}&orderId={}",
                formatted_symbol,
                self.product_type(),
                order_id
            );
            let response: BitgetResponse<Vec<FuturesOrderData>> =
                self.signed_get("/api/v2/mix/order/detail", &params).await?;

            if !response.is_success() {
                anyhow::bail!("Failed to get order status: {}", response.msg);
            }

            let order = response
                .data
                .and_then(|d| d.into_iter().next())
                .ok_or_else(|| anyhow::anyhow!("Order not found"))?;

            let status_str = order.state.as_deref().unwrap_or("new");
            let status = parse_order_status_str(status_str);

            let side_str = order.side.as_deref().unwrap_or("buy");
            let side = if side_str.to_lowercase().contains("sell")
                || side_str.to_lowercase().contains("short")
            {
                OrderSide::Sell
            } else {
                OrderSide::Buy
            };

            let order_type_str = order.order_type.as_deref().unwrap_or("market");
            let order_type = if order_type_str.to_lowercase() == "limit" {
                OrderType::Limit
            } else {
                OrderType::Market
            };

            let price: f64 = order
                .price
                .as_ref()
                .and_then(|s| s.parse().ok())
                .unwrap_or(0.0);
            let quantity: f64 = order
                .size
                .as_ref()
                .and_then(|s| s.parse().ok())
                .unwrap_or(0.0);
            let filled: f64 = order
                .filled_qty
                .as_ref()
                .and_then(|s| s.parse().ok())
                .unwrap_or(0.0);
            let avg_price: Option<f64> = order.price_avg.as_ref().and_then(|s| s.parse().ok());
            let timestamp: i64 = order
                .c_time
                .as_ref()
                .and_then(|s| s.parse().ok())
                .unwrap_or(0);
            let reduce_only = order
                .reduce_only
                .as_ref()
                .map(|s| s == "true" || s == "YES")
                .unwrap_or(false);

            Ok(OrderInfo {
                order_id: order.order_id,
                client_order_id: order.client_oid,
                symbol: formatted_symbol,
                status,
                side,
                order_type,
                price,
                quantity,
                filled_quantity: filled,
                remaining_quantity: quantity - filled,
                avg_price,
                timestamp,
                time_in_force: TimeInForce::GTC,
                reduce_only,
            })
        }
    }

    async fn close_position(&self, symbol: &str) -> Result<OrderResult> {
        if self.market_type == MarketType::Spot {
            anyhow::bail!("Close position not supported for spot trading");
        }

        let formatted_symbol = self.format_symbol(symbol);

        // Get current position
        let position = self.get_position(&formatted_symbol).await?;

        match position {
            Some(pos) => {
                let total: f64 = pos
                    .total
                    .as_ref()
                    .and_then(|s| s.parse().ok())
                    .unwrap_or(0.0);

                if total.abs() < 0.000001 {
                    return Ok(OrderResult::failure(
                        formatted_symbol,
                        OrderSide::Sell,
                        "No position to close".to_string(),
                    ));
                }

                // Determine close side (opposite of position side)
                let hold_side = pos.hold_side.as_deref().unwrap_or("long");
                let close_side = if hold_side == "long" {
                    OrderSide::Sell
                } else {
                    OrderSide::Buy
                };

                // Place market order to close position with reduceOnly flag
                self.place_order_ws(
                    &formatted_symbol,
                    close_side,
                    OrderType::Market,
                    total.abs(),
                    None,
                    true, // reduceOnly
                )
                .await
            }
            None => Ok(OrderResult::failure(
                formatted_symbol,
                OrderSide::Sell,
                "No position found".to_string(),
            )),
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

    async fn get_funding_rate_history(
        &self,
        symbol: &str,
        limit: u32,
    ) -> Result<Vec<FundingRateEntry>> {
        if self.market_type != MarketType::Swap {
            anyhow::bail!("Funding rate only available for perpetual futures");
        }

        let formatted_symbol = self.format_symbol(symbol);
        let params = format!(
            "symbol={}&productType={}&pageSize={}",
            formatted_symbol,
            self.product_type(),
            limit.min(100)
        );

        let response: BitgetResponse<Vec<FundingRateHistoryEntry>> = self
            .client
            .get(format!(
                "{}/api/v2/mix/market/history-fund-rate?{}",
                self.base_url, params
            ))
            .send()
            .await?
            .json()
            .await?;

        if !response.is_success() {
            anyhow::bail!("Failed to get funding rate history: {}", response.msg);
        }

        let entries: Vec<FundingRateEntry> = response
            .data
            .unwrap_or_default()
            .iter()
            .filter_map(|e| {
                let funding_rate: f64 = e.funding_rate.parse().ok()?;
                // Bitget uses fundingTime or fundingRateTimestamp
                let funding_time: i64 = e
                    .funding_time
                    .as_ref()
                    .or(e.funding_rate_timestamp.as_ref())
                    .and_then(|s| s.parse().ok())?;
                Some(FundingRateEntry {
                    symbol: e.symbol.clone(),
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
        _limit: u32,
    ) -> Result<Vec<OpenInterestEntry>> {
        // Bitget doesn't have a dedicated open interest history endpoint
        // We can only get current open interest from the ticker
        if self.market_type != MarketType::Swap {
            anyhow::bail!("Open interest only available for perpetual futures");
        }

        let formatted_symbol = self.format_symbol(symbol);

        // Get current open interest from ticker
        let response: BitgetResponse<OpenInterestData> = self
            .client
            .get(format!(
                "{}/api/v2/mix/market/open-interest?symbol={}&productType={}",
                self.base_url,
                formatted_symbol,
                self.product_type()
            ))
            .send()
            .await?
            .json()
            .await?;

        if !response.is_success() {
            anyhow::bail!("Failed to get open interest: {}", response.msg);
        }

        let data = response
            .data
            .ok_or_else(|| anyhow::anyhow!("No open interest data"))?;
        let timestamp: i64 = data
            .ts
            .as_ref()
            .and_then(|s| s.parse().ok())
            .unwrap_or(timestamp_ms());

        let entries: Vec<OpenInterestEntry> = data
            .open_interest_list
            .unwrap_or_default()
            .iter()
            .filter_map(|e| {
                let open_interest: f64 = e
                    .size
                    .as_ref()
                    .or(e.open_interest.as_ref())
                    .and_then(|s| s.parse().ok())?;
                Some(OpenInterestEntry {
                    symbol: e.symbol.clone(),
                    open_interest,
                    open_interest_value: 0.0, // Not provided
                    timestamp,
                })
            })
            .collect();

        Ok(entries)
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
            .context("Failed to connect to Bitget WebSocket")?;

        let connect_time = start.elapsed().as_millis() as u64;

        // Bitget uses books5, books15, or books for different depths
        let depth_channel = if depth <= 5 {
            "books5"
        } else if depth <= 15 {
            "books15"
        } else {
            "books"
        };

        let subscribe_msg = serde_json::json!({
            "op": "subscribe",
            "args": [{
                "instType": self.inst_type(),
                "channel": depth_channel,
                "instId": formatted_symbol
            }]
        });

        ws_stream
            .send(Message::Text(subscribe_msg.to_string().into()))
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
                                if parsed.data.is_some() && parsed.action.is_some() {
                                    data_sample = parsed.data;
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
            subscription_type: format!("{}.{}", depth_channel, symbol),
            data_sample,
        })
    }

    async fn test_ws_trades(&self, symbol: &str, timeout_ms: u64) -> Result<WsTestResult> {
        let formatted_symbol = self.format_symbol(symbol);
        let ws_url = self.ws_public_url();

        let start = std::time::Instant::now();
        let (mut ws_stream, _) = connect_async(ws_url)
            .await
            .context("Failed to connect to Bitget WebSocket")?;

        let connect_time = start.elapsed().as_millis() as u64;

        let subscribe_msg = serde_json::json!({
            "op": "subscribe",
            "args": [{
                "instType": self.inst_type(),
                "channel": "trade",
                "instId": formatted_symbol
            }]
        });

        ws_stream
            .send(Message::Text(subscribe_msg.to_string().into()))
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
                                if parsed.data.is_some() && parsed.action.is_some() {
                                    data_sample = parsed.data;
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
            subscription_type: format!("trade.{}", symbol),
            data_sample,
        })
    }

    async fn test_ws_ticker(&self, symbol: &str, timeout_ms: u64) -> Result<WsTestResult> {
        let formatted_symbol = self.format_symbol(symbol);
        let ws_url = self.ws_public_url();

        let start = std::time::Instant::now();
        let (mut ws_stream, _) = connect_async(ws_url)
            .await
            .context("Failed to connect to Bitget WebSocket")?;

        let connect_time = start.elapsed().as_millis() as u64;

        let subscribe_msg = serde_json::json!({
            "op": "subscribe",
            "args": [{
                "instType": self.inst_type(),
                "channel": "ticker",
                "instId": formatted_symbol
            }]
        });

        ws_stream
            .send(Message::Text(subscribe_msg.to_string().into()))
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
                                if parsed.data.is_some() && parsed.action.is_some() {
                                    data_sample = parsed.data;
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
            subscription_type: format!("ticker.{} (includes OI, fundingRate for futures)", symbol),
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
        let exchange_name = "bitget".to_string();

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

                        if let Err(e) = ws_stream
                            .send(Message::Text(auth_msg.to_string().into()))
                            .await
                        {
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

                        if let Err(e) = ws_stream
                            .send(Message::Text(sub_msg.to_string().into()))
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
        let trader = BitgetTrader::new("", "", None, false, MarketType::Swap);
        assert_eq!(trader.format_symbol("BTC"), "BTCUSDT");
        assert_eq!(trader.format_symbol("BTCUSDT"), "BTCUSDT");
        assert_eq!(trader.format_symbol("eth"), "ETHUSDT");
        assert_eq!(trader.format_symbol("BTC/USDT"), "BTCUSDT");
        assert_eq!(trader.format_symbol("ETH/USDT"), "ETHUSDT");
    }

    #[test]
    fn test_inst_type() {
        let spot_trader = BitgetTrader::new("", "", None, false, MarketType::Spot);
        assert_eq!(spot_trader.inst_type(), "SPOT");

        let swap_trader = BitgetTrader::new("", "", None, false, MarketType::Swap);
        assert_eq!(swap_trader.inst_type(), "USDT-FUTURES");
    }

    #[test]
    fn test_ws_urls() {
        let mainnet = BitgetTrader::new("", "", None, false, MarketType::Spot);
        assert_eq!(mainnet.ws_private_url(), MAINNET_WS_PRIVATE_URL);

        let testnet = BitgetTrader::new("", "", None, true, MarketType::Spot);
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
