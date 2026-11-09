//! OKX exchange trader implementation.
//!
//! Full-featured implementation with:
//! - REST API for market data (ticker, orderbook, funding rate, open interest)
//! - WebSocket for trading (place/cancel/amend orders)
//! - Private WebSocket subscription for order updates
//! - Public WebSocket testing for orderbook, trades, ticker
//! - Testnet support via x-simulated-trading header

use crate::models::*;
use anyhow::{Context, Result};
use async_trait::async_trait;
use base64::{engine::general_purpose::STANDARD as BASE64, Engine as _};
use futures_util::{SinkExt, StreamExt};
use hmac::{Hmac, Mac};
use reqwest::Client;
use sha2::Sha256;
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

const MAINNET_URL: &str = "https://www.okx.com";
const TESTNET_URL: &str = "https://www.okx.com"; // OKX uses same URL with demo trading header
const MAINNET_WS_PUBLIC_URL: &str = "wss://ws.okx.com:8443/ws/v5/public";
const TESTNET_WS_PUBLIC_URL: &str = "wss://wspap.okx.com:8443/ws/v5/public?brokerId=9999";
const MAINNET_WS_TRADE_URL: &str = "wss://ws.okx.com:8443/ws/v5/private";
const TESTNET_WS_TRADE_URL: &str = "wss://wspap.okx.com:8443/ws/v5/private?brokerId=9999";

/// Maximum reconnect backoff delay in seconds.
const MAX_RECONNECT_DELAY_SECS: u64 = 30;
/// Initial reconnect delay in seconds.
const INITIAL_RECONNECT_DELAY_SECS: u64 = 1;

/// OKX exchange trader.
pub struct OkxTrader {
    api_key: String,
    api_secret: String,
    passphrase: String,
    testnet: bool,
    market_type: MarketType,
    client: Client,
    base_url: String,
}

impl OkxTrader {
    /// Create a new OKX trader.
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
            base_url: if testnet { TESTNET_URL } else { MAINNET_URL }.to_string(),
        }
    }

    fn sign(&self, timestamp: &str, method: &str, path: &str, body: &str) -> String {
        let message = format!("{}{}{}{}", timestamp, method, path, body);

        type HmacSha256 = Hmac<Sha256>;
        let mut mac = HmacSha256::new_from_slice(self.api_secret.as_bytes())
            .expect("HMAC can take key of any size");
        mac.update(message.as_bytes());

        BASE64.encode(mac.finalize().into_bytes())
    }

    fn timestamp_iso(&self) -> String {
        chrono::Utc::now()
            .format("%Y-%m-%dT%H:%M:%S%.3fZ")
            .to_string()
    }

    /// Get WebSocket trade URL based on testnet setting.
    fn ws_trade_url(&self) -> &str {
        if self.testnet {
            TESTNET_WS_TRADE_URL
        } else {
            MAINNET_WS_TRADE_URL
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

    /// Get instrument type string for API calls.
    fn inst_type(&self) -> &str {
        match self.market_type {
            MarketType::Spot => "SPOT",
            MarketType::Swap => "SWAP",
        }
    }

    /// Get current funding rate for a symbol.
    async fn get_funding_rate(&self, symbol: &str) -> Result<Option<(f64, i64)>> {
        if self.market_type != MarketType::Swap {
            return Ok(None);
        }

        let formatted_symbol = self.format_symbol(symbol);
        let url = format!(
            "{}/api/v5/public/funding-rate?instId={}",
            self.base_url, formatted_symbol
        );

        let response: OkxResponse<Vec<OkxFundingRateData>> =
            self.client.get(&url).send().await?.json().await?;

        if let Some(data) = response.data {
            if let Some(fr) = data.first() {
                let rate: f64 = fr.funding_rate.parse().unwrap_or(0.0);
                let next_time: i64 = fr
                    .next_funding_time
                    .as_ref()
                    .and_then(|s| s.parse().ok())
                    .unwrap_or(0);
                return Ok(Some((rate, next_time)));
            }
        }
        Ok(None)
    }

    /// Get mark price for a symbol.
    async fn get_mark_price(&self, symbol: &str) -> Result<Option<f64>> {
        if self.market_type != MarketType::Swap {
            return Ok(None);
        }

        let formatted_symbol = self.format_symbol(symbol);
        let url = format!(
            "{}/api/v5/public/mark-price?instId={}",
            self.base_url, formatted_symbol
        );

        let response: OkxResponse<Vec<OkxMarkPriceData>> =
            self.client.get(&url).send().await?.json().await?;

        if let Some(data) = response.data {
            if let Some(mp) = data.first() {
                return Ok(mp.mark_px.parse().ok());
            }
        }
        Ok(None)
    }

    /// Get index price for a symbol.
    async fn get_index_price(&self, symbol: &str) -> Result<Option<f64>> {
        if self.market_type != MarketType::Swap {
            return Ok(None);
        }

        // For swap, the index instrument is like BTC-USD (without -SWAP)
        let formatted_symbol = self.format_symbol(symbol);
        let index_inst = formatted_symbol.replace("-SWAP", "");
        let url = format!(
            "{}/api/v5/market/index-tickers?instId={}",
            self.base_url, index_inst
        );

        let response: OkxResponse<Vec<OkxIndexTickerData>> =
            self.client.get(&url).send().await?.json().await?;

        if let Some(data) = response.data {
            if let Some(it) = data.first() {
                return Ok(it.idx_px.parse().ok());
            }
        }
        Ok(None)
    }

    /// Get current open interest for a symbol.
    async fn get_open_interest(&self, symbol: &str) -> Result<Option<(f64, Option<f64>)>> {
        if self.market_type != MarketType::Swap {
            return Ok(None);
        }

        let formatted_symbol = self.format_symbol(symbol);
        let url = format!(
            "{}/api/v5/public/open-interest?instType=SWAP&instId={}",
            self.base_url, formatted_symbol
        );

        let response: OkxResponse<Vec<OkxOpenInterestData>> =
            self.client.get(&url).send().await?.json().await?;

        if let Some(data) = response.data {
            if let Some(oi) = data.first() {
                let oi_amount: f64 = oi.oi.parse().unwrap_or(0.0);
                let oi_value: Option<f64> = oi.oi_usd.as_ref().and_then(|s| s.parse().ok());
                return Ok(Some((oi_amount, oi_value)));
            }
        }
        Ok(None)
    }

    /// Get current position for a symbol.
    async fn get_position(&self, symbol: &str) -> Result<Option<OkxPositionData>> {
        if self.market_type == MarketType::Spot {
            return Ok(None);
        }

        let formatted_symbol = self.format_symbol(symbol);
        let timestamp = self.timestamp_iso();
        let path = format!(
            "/api/v5/account/positions?instType=SWAP&instId={}",
            formatted_symbol
        );
        let signature = self.sign(&timestamp, "GET", &path, "");

        let response: OkxResponse<Vec<OkxPositionData>> = self
            .client
            .get(format!("{}{}", self.base_url, path))
            .header("OK-ACCESS-KEY", &self.api_key)
            .header("OK-ACCESS-SIGN", signature)
            .header("OK-ACCESS-TIMESTAMP", timestamp)
            .header("OK-ACCESS-PASSPHRASE", &self.passphrase)
            .header("x-simulated-trading", if self.testnet { "1" } else { "0" })
            .send()
            .await?
            .json()
            .await?;

        if let Some(data) = response.data {
            if let Some(pos) = data.first() {
                let size: f64 = pos.pos.parse().unwrap_or(0.0);
                if size.abs() > 0.0 {
                    return Ok(Some(pos.clone()));
                }
            }
        }
        Ok(None)
    }

    /// Generate WebSocket authentication signature.
    fn ws_auth_signature(&self, timestamp: &str) -> String {
        let sign_str = format!("{}GET/users/self/verify", timestamp);
        type HmacSha256 = Hmac<Sha256>;
        let mut mac = HmacSha256::new_from_slice(self.api_secret.as_bytes())
            .expect("HMAC can take key of any size");
        mac.update(sign_str.as_bytes());
        BASE64.encode(mac.finalize().into_bytes())
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
        let (mut ws_stream, _) = connect_async(ws_url)
            .await
            .context("Failed to connect to OKX trade WebSocket")?;

        // Authenticate
        let timestamp = (timestamp_ms() / 1000).to_string(); // OKX uses seconds
        let signature = self.ws_auth_signature(&timestamp);

        let auth_msg = WsLoginRequest {
            op: "login".to_string(),
            args: vec![WsLoginArgs {
                api_key: self.api_key.clone(),
                passphrase: self.passphrase.clone(),
                timestamp: timestamp.clone(),
                sign: signature,
            }],
        };

        let auth_json = serde_json::to_string(&auth_msg)?;
        debug!("Sending WS login: {}", auth_json);

        ws_stream.send(Message::Text(auth_json)).await?;

        // Wait for auth response
        let timeout = tokio::time::Duration::from_secs(5);
        tokio::select! {
            msg = ws_stream.next() => {
                match msg {
                    Some(Ok(Message::Text(text))) => {
                        debug!("Login response: {}", text);
                        let login_resp: WsLoginResponse = serde_json::from_str(&text)
                            .context("Failed to parse login response")?;

                        if login_resp.code != "0" {
                            anyhow::bail!("WebSocket login failed: code={}, msg={:?}",
                                login_resp.code, login_resp.msg);
                        }

                        info!("OKX WebSocket authenticated, connId: {:?}", login_resp.conn_id);
                    }
                    Some(Ok(Message::Ping(_))) | Some(Ok(Message::Pong(_))) => {
                        debug!("Received ping/pong during login");
                    }
                    Some(Ok(_)) => anyhow::bail!("Unexpected message type during login"),
                    Some(Err(e)) => anyhow::bail!("WebSocket error during login: {}", e),
                    None => anyhow::bail!("WebSocket closed during login"),
                }
            }
            _ = tokio::time::sleep(timeout) => {
                anyhow::bail!("WebSocket login timeout");
            }
        }

        Ok(ws_stream)
    }

    /// Send ping to keep WebSocket connection alive.
    async fn send_ws_ping(
        ws_stream: &mut tokio_tungstenite::WebSocketStream<
            tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>,
        >,
    ) -> Result<()> {
        // OKX uses string "ping", not JSON
        ws_stream.send(Message::Text("ping".to_string())).await?;
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
        let mut ws_stream = self.create_ws_trade_connection().await?;

        // Prepare order request
        let timestamp = timestamp_ms();
        let req_id = format!("order_{}", timestamp);

        let (td_mode, pos_side) = match self.market_type {
            MarketType::Spot => ("cash".to_string(), "".to_string()),
            MarketType::Swap => (
                "cross".to_string(),
                match side {
                    OrderSide::Buy => "long".to_string(),
                    OrderSide::Sell => "short".to_string(),
                },
            ),
        };

        let mut order_params = serde_json::json!({
            "instId": symbol,
            "tdMode": td_mode,
            "side": match side {
                OrderSide::Buy => "buy",
                OrderSide::Sell => "sell",
            },
            "ordType": match order_type {
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

        // Add posSide for swap
        if self.market_type == MarketType::Swap {
            order_params["posSide"] = serde_json::json!(pos_side);
        }

        // Add reduceOnly if needed
        if reduce_only {
            order_params["reduceOnly"] = serde_json::json!(true);
        }

        let order_req = WsOrderRequest {
            id: req_id.clone(),
            op: "order".to_string(),
            args: vec![order_params],
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

                        // Handle pong response
                        if text == "pong" {
                            error!("Received pong instead of order response");
                            order_result = Some(OrderResult::failure(
                                symbol.to_string(),
                                side,
                                "Received pong instead of order response".to_string(),
                            ));
                        } else {
                            let order_resp: WsOrderResponse = serde_json::from_str(&text)
                                .context("Failed to parse order response")?;

                            if order_resp.code == "0" {
                                if let Some(data) = order_resp.data {
                                    if let Some(first) = data.first() {
                                        if let Some(order_id) = &first.ord_id {
                                            info!("OKX WS order placed: {}", order_id);
                                            order_result = Some(OrderResult {
                                                success: true,
                                                order_id: Some(order_id.clone()),
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
                            } else {
                                let error_msg = order_resp.msg
                                    .unwrap_or_else(|| format!("Error code: {}", order_resp.code));
                                order_result = Some(OrderResult::failure(
                                    symbol.to_string(),
                                    side,
                                    error_msg,
                                ));
                            }
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
        let mut ws_stream = self.create_ws_trade_connection().await?;

        // Prepare cancel request
        let timestamp = timestamp_ms();
        let req_id = format!("cancel_{}", timestamp);

        let cancel_params = serde_json::json!({
            "instId": symbol,
            "ordId": order_id,
        });

        let cancel_req = WsOrderRequest {
            id: req_id.clone(),
            op: "cancel-order".to_string(),
            args: vec![cancel_params],
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
                        if text == "pong" {
                            error!("Received pong during cancel");
                        } else {
                            debug!("Cancel response: {}", text);
                            let cancel_resp: WsOrderResponse = serde_json::from_str(&text)
                                .context("Failed to parse cancel response")?;

                            if cancel_resp.code == "0" {
                                info!("OKX WS order cancelled: {}", order_id);
                                success = true;
                            } else {
                                let error_msg = cancel_resp.msg
                                    .unwrap_or_else(|| format!("Error code: {}", cancel_resp.code));
                                error!("Failed to cancel order {}: {}", order_id, error_msg);
                            }
                        }
                    }
                    Some(Ok(Message::Ping(_))) | Some(Ok(Message::Pong(_))) => {
                        debug!("Received ping/pong during cancel");
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
    async fn amend_order_ws(
        &self,
        symbol: &str,
        order_id: &str,
        new_quantity: Option<f64>,
        new_price: Option<f64>,
    ) -> Result<bool> {
        // Create WS connection and authenticate
        let mut ws_stream = self.create_ws_trade_connection().await?;

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

        let amend_req = WsOrderRequest {
            id: req_id.clone(),
            op: "amend-order".to_string(),
            args: vec![amend_params],
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
                        if text == "pong" {
                            error!("Received pong during amend");
                        } else {
                            debug!("Amend response: {}", text);
                            let amend_resp: WsOrderResponse = serde_json::from_str(&text)
                                .context("Failed to parse amend response")?;

                            if amend_resp.code == "0" {
                                info!("OKX WS order amended: {}", order_id);
                                success = true;
                            } else {
                                let error_msg = amend_resp.msg
                                    .unwrap_or_else(|| format!("Error code: {}", amend_resp.code));
                                error!("Failed to amend order {}: {}", order_id, error_msg);
                            }
                        }
                    }
                    Some(Ok(Message::Ping(_))) | Some(Ok(Message::Pong(_))) => {
                        debug!("Received ping/pong during amend");
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

/// Calculate next reconnect delay with exponential backoff (capped).
fn next_reconnect_delay(current_secs: u64) -> u64 {
    (current_secs * 2).min(MAX_RECONNECT_DELAY_SECS)
}

/// Parse a status string from OKX into OrderStatus.
fn parse_order_status_str(s: &str) -> OrderStatus {
    match s.to_lowercase().as_str() {
        "live" | "created" | "init" | "accepted" => OrderStatus::New,
        "partially_filled" | "partiallyfilled" | "partial_filled" | "partial" => {
            OrderStatus::PartiallyFilled
        }
        "filled" | "closed" | "full_fill" | "full" => OrderStatus::Filled,
        "cancelled" | "canceled" | "cancel" | "deactivated" => OrderStatus::Cancelled,
        "rejected" | "fail" | "failed" => OrderStatus::Rejected,
        "expired" | "expire" => OrderStatus::Expired,
        _ => {
            tracing::warn!("Unknown order status string: '{}'", s);
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
impl ExchangeTrader for OkxTrader {
    fn exchange_name(&self) -> &str {
        "okx"
    }

    fn market_type(&self) -> MarketType {
        self.market_type
    }

    fn capabilities(&self) -> ExchangeCapabilities {
        tradersdk_core::exchange_capabilities! {
            spot: Supported,
            swap: Supported,
            futures: Supported,
            option: Supported,
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
            .get(format!("{}/api/v5/public/time", self.base_url))
            .send()
            .await?;

        Ok(response.status().is_success())
    }

    async fn get_server_time(&self) -> Result<i64> {
        let response: serde_json::Value = self
            .client
            .get(format!("{}/api/v5/public/time", self.base_url))
            .send()
            .await?
            .json()
            .await?;

        let time = response["data"][0]["ts"]
            .as_str()
            .and_then(|s| s.parse::<i64>().ok())
            .ok_or_else(|| anyhow::anyhow!("Failed to get server time"))?;

        Ok(time)
    }

    async fn get_balance(&self) -> Result<AccountBalance> {
        let timestamp = self.timestamp_iso();
        let path = "/api/v5/account/balance";
        let signature = self.sign(&timestamp, "GET", path, "");

        let response: serde_json::Value = self
            .client
            .get(format!("{}{}", self.base_url, path))
            .header("OK-ACCESS-KEY", &self.api_key)
            .header("OK-ACCESS-SIGN", signature)
            .header("OK-ACCESS-TIMESTAMP", timestamp)
            .header("OK-ACCESS-PASSPHRASE", &self.passphrase)
            .header("x-simulated-trading", if self.testnet { "1" } else { "0" })
            .send()
            .await?
            .json()
            .await?;

        let available = response["data"][0]["details"]
            .as_array()
            .and_then(|arr| arr.iter().find(|d| d["ccy"] == "USDT"))
            .and_then(|d| d["availBal"].as_str())
            .and_then(|s| s.parse::<f64>().ok())
            .unwrap_or(0.0);

        Ok(AccountBalance::simple(available, available, "USDT"))
    }

    async fn get_current_price(&self, symbol: &str) -> Result<f64> {
        let formatted_symbol = self.format_symbol(symbol);

        let response: serde_json::Value = self
            .client
            .get(format!(
                "{}/api/v5/market/ticker?instId={}",
                self.base_url, formatted_symbol
            ))
            .send()
            .await?
            .json()
            .await?;

        let price = response["data"][0]["last"]
            .as_str()
            .and_then(|s| s.parse::<f64>().ok())
            .ok_or_else(|| anyhow::anyhow!("Failed to get price"))?;

        Ok(price)
    }

    async fn get_ticker(&self, symbol: &str) -> Result<TickerInfo> {
        let formatted_symbol = self.format_symbol(symbol);

        let response: serde_json::Value = self
            .client
            .get(format!(
                "{}/api/v5/market/ticker?instId={}",
                self.base_url, formatted_symbol
            ))
            .send()
            .await?
            .json()
            .await?;

        let data = &response["data"][0];

        let last_price: f64 = data["last"]
            .as_str()
            .and_then(|s| s.parse().ok())
            .unwrap_or(0.0);

        // Get additional data for swap markets (funding rate, mark price, etc.)
        let (funding_rate, next_funding_time): (Option<f64>, Option<i64>) = if self.market_type
            == MarketType::Swap
        {
            match self.get_funding_rate(&formatted_symbol).await {
                Ok(Some((rate, time))) => (Some(rate), if time > 0 { Some(time) } else { None }),
                _ => (None, None),
            }
        } else {
            (None, None)
        };

        let (open_interest, open_interest_value) = if self.market_type == MarketType::Swap {
            self.get_open_interest(&formatted_symbol)
                .await
                .ok()
                .flatten()
                .map(|(oi, oi_val)| (Some(oi), oi_val))
                .unwrap_or((None, None))
        } else {
            (None, None)
        };

        let mark_price = if self.market_type == MarketType::Swap {
            self.get_mark_price(&formatted_symbol).await.ok().flatten()
        } else {
            None
        };

        let index_price = if self.market_type == MarketType::Swap {
            self.get_index_price(&formatted_symbol).await.ok().flatten()
        } else {
            None
        };

        Ok(TickerInfo {
            symbol: formatted_symbol,
            last_price,
            bid_price: data["bidPx"].as_str().and_then(|s| s.parse().ok()),
            ask_price: data["askPx"].as_str().and_then(|s| s.parse().ok()),
            high_24h: data["high24h"].as_str().and_then(|s| s.parse().ok()),
            low_24h: data["low24h"].as_str().and_then(|s| s.parse().ok()),
            volume_24h: data["vol24h"].as_str().and_then(|s| s.parse().ok()),
            turnover_24h: data["volCcy24h"].as_str().and_then(|s| s.parse().ok()),
            change_24h: None,
            open_interest,
            open_interest_value,
            funding_rate,
            next_funding_time,
            mark_price,
            index_price,
            timestamp: timestamp_ms(),
        })
    }

    async fn get_orderbook(&self, symbol: &str, depth: u32) -> Result<Orderbook> {
        let formatted_symbol = self.format_symbol(symbol);
        let limit = depth.min(400); // OKX max is 400

        let response: serde_json::Value = self
            .client
            .get(format!(
                "{}/api/v5/market/books?instId={}&sz={}",
                self.base_url, formatted_symbol, limit
            ))
            .send()
            .await?
            .json()
            .await?;

        let data = &response["data"][0];

        let bids: Vec<OrderbookLevel> = data["bids"]
            .as_array()
            .map(|arr| {
                arr.iter()
                    .filter_map(|level| {
                        if let Some(arr) = level.as_array() {
                            if arr.len() >= 2 {
                                let price = arr[0].as_str()?.parse().ok()?;
                                let quantity = arr[1].as_str()?.parse().ok()?;
                                return Some(OrderbookLevel::new(price, quantity));
                            }
                        }
                        None
                    })
                    .collect()
            })
            .unwrap_or_default();

        let asks: Vec<OrderbookLevel> = data["asks"]
            .as_array()
            .map(|arr| {
                arr.iter()
                    .filter_map(|level| {
                        if let Some(arr) = level.as_array() {
                            if arr.len() >= 2 {
                                let price = arr[0].as_str()?.parse().ok()?;
                                let quantity = arr[1].as_str()?.parse().ok()?;
                                return Some(OrderbookLevel::new(price, quantity));
                            }
                        }
                        None
                    })
                    .collect()
            })
            .unwrap_or_default();

        let timestamp = data["ts"]
            .as_str()
            .and_then(|s| s.parse().ok())
            .unwrap_or_else(timestamp_ms);

        Ok(Orderbook {
            symbol: formatted_symbol,
            bids,
            asks,
            timestamp,
        })
    }

    async fn set_leverage(&self, symbol: Option<&str>, leverage: u32) -> Result<bool> {
        if self.market_type == MarketType::Spot {
            return Ok(true);
        }

        let symbol = symbol.ok_or_else(|| anyhow::anyhow!("Symbol required"))?;
        let formatted_symbol = self.format_symbol(symbol);
        let timestamp = self.timestamp_iso();
        let path = "/api/v5/account/set-leverage";
        let body = serde_json::json!({
            "instId": formatted_symbol,
            "lever": leverage.to_string(),
            "mgnMode": "cross"
        });
        let body_str = body.to_string();
        let signature = self.sign(&timestamp, "POST", path, &body_str);

        let _response: serde_json::Value = self
            .client
            .post(format!("{}{}", self.base_url, path))
            .header("OK-ACCESS-KEY", &self.api_key)
            .header("OK-ACCESS-SIGN", signature)
            .header("OK-ACCESS-TIMESTAMP", timestamp)
            .header("OK-ACCESS-PASSPHRASE", &self.passphrase)
            .header("Content-Type", "application/json")
            .header("x-simulated-trading", if self.testnet { "1" } else { "0" })
            .body(body_str)
            .send()
            .await?
            .json()
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
        let formatted_symbol = self.format_symbol(symbol);

        let price = match current_price {
            Some(p) => p,
            None => self.get_current_price(&formatted_symbol).await?,
        };

        let quantity = usdt_amount / price * leverage as f64;

        if self.market_type == MarketType::Swap {
            let _ = self.set_leverage(Some(&formatted_symbol), leverage).await;
        }

        let side = if direction.to_lowercase() == "long" {
            OrderSide::Buy
        } else {
            OrderSide::Sell
        };

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
        let formatted_symbol = self.format_symbol(symbol);
        // Use WebSocket by default
        self.cancel_order_ws(&formatted_symbol, order_id).await
    }

    async fn get_order_status(&self, symbol: &str, order_id: &str) -> Result<OrderInfo> {
        let formatted_symbol = self.format_symbol(symbol);
        let timestamp = self.timestamp_iso();
        let path = format!(
            "/api/v5/trade/order?instId={}&ordId={}",
            formatted_symbol, order_id
        );
        let signature = self.sign(&timestamp, "GET", &path, "");

        let response: serde_json::Value = self
            .client
            .get(format!("{}{}", self.base_url, path))
            .header("OK-ACCESS-KEY", &self.api_key)
            .header("OK-ACCESS-SIGN", signature)
            .header("OK-ACCESS-TIMESTAMP", timestamp)
            .header("OK-ACCESS-PASSPHRASE", &self.passphrase)
            .header("x-simulated-trading", if self.testnet { "1" } else { "0" })
            .send()
            .await?
            .json()
            .await?;

        let data = &response["data"][0];

        let status_str = data["state"].as_str().unwrap_or("live");
        let status = parse_order_status_str(status_str);

        let side_str = data["side"].as_str().unwrap_or("buy");
        let side = if side_str == "sell" {
            OrderSide::Sell
        } else {
            OrderSide::Buy
        };

        let order_type_str = data["ordType"].as_str().unwrap_or("market");
        let order_type = if order_type_str == "limit" {
            OrderType::Limit
        } else {
            OrderType::Market
        };

        let price = data["px"]
            .as_str()
            .and_then(|s| s.parse().ok())
            .unwrap_or(0.0);
        let quantity = data["sz"]
            .as_str()
            .and_then(|s| s.parse().ok())
            .unwrap_or(0.0);
        let filled_quantity = data["fillSz"]
            .as_str()
            .and_then(|s| s.parse().ok())
            .unwrap_or(0.0);
        let avg_price = data["avgPx"].as_str().and_then(|s| s.parse().ok());
        let timestamp_val = data["cTime"]
            .as_str()
            .and_then(|s| s.parse().ok())
            .unwrap_or(0);

        Ok(OrderInfo {
            order_id: order_id.to_string(),
            client_order_id: data["clOrdId"].as_str().map(|s| s.to_string()),
            symbol: formatted_symbol,
            status,
            side,
            order_type,
            price,
            quantity,
            filled_quantity,
            remaining_quantity: quantity - filled_quantity,
            avg_price,
            timestamp: timestamp_val,
            time_in_force: TimeInForce::GTC,
            reduce_only: false,
        })
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
                let size: f64 = pos.pos.parse().context("Failed to parse position size")?;
                if size.abs() < 0.000001 {
                    return Ok(OrderResult::failure(
                        formatted_symbol,
                        OrderSide::Sell,
                        "No position to close".to_string(),
                    ));
                }

                // Determine close side based on position side
                // In OKX: posSide can be "long", "short", or "net"
                // For net mode, pos > 0 means long, pos < 0 means short
                let close_side = if pos.pos_side == "long" || (pos.pos_side == "net" && size > 0.0)
                {
                    OrderSide::Sell
                } else {
                    OrderSide::Buy
                };

                // Place market order to close position with reduceOnly flag
                self.place_order_ws(
                    &formatted_symbol,
                    close_side,
                    OrderType::Market,
                    size.abs(),
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

    async fn get_funding_rate_history(
        &self,
        symbol: &str,
        limit: u32,
    ) -> Result<Vec<FundingRateEntry>> {
        if self.market_type != MarketType::Swap {
            anyhow::bail!("Funding rate only available for perpetual futures");
        }

        let formatted_symbol = self.format_symbol(symbol);
        let limit = limit.min(100); // OKX max is 100

        let url = format!(
            "{}/api/v5/public/funding-rate-history?instId={}&limit={}",
            self.base_url, formatted_symbol, limit
        );

        let response: OkxResponse<Vec<OkxFundingRateHistoryData>> =
            self.client.get(&url).send().await?.json().await?;

        if let Some(data) = response.data {
            let entries: Vec<FundingRateEntry> = data
                .iter()
                .filter_map(|e| {
                    // Use realizedRate which is the actual settled rate
                    let funding_rate: f64 = e.realized_rate.parse().ok()?;
                    let funding_time: i64 = e.funding_time.parse().ok()?;
                    Some(FundingRateEntry {
                        symbol: e.inst_id.clone(),
                        funding_rate,
                        funding_time,
                    })
                })
                .collect();
            return Ok(entries);
        }

        anyhow::bail!("Failed to get funding history: {}", response.msg)
    }

    async fn get_open_interest_history(
        &self,
        symbol: &str,
        interval: &str,
        limit: u32,
    ) -> Result<Vec<OpenInterestEntry>> {
        if self.market_type != MarketType::Swap {
            anyhow::bail!("Open interest only available for perpetual futures");
        }

        // OKX uses /api/v5/rubik/stat/contracts/open-interest-volume for historical OI
        // Convert interval format: 5m -> 5m, 1h -> 1H, 1d -> 1D
        let period = match interval.to_lowercase().as_str() {
            "5m" => "5m",
            "1h" => "1H",
            "1d" | "1day" => "1D",
            _ => "5m",
        };

        // Extract currency from symbol (e.g., BTC-USDT-SWAP -> BTC)
        let formatted_symbol = self.format_symbol(symbol);
        let currency = formatted_symbol
            .split('-')
            .next()
            .unwrap_or("BTC")
            .to_uppercase();

        let url = format!(
            "{}/api/v5/rubik/stat/contracts/open-interest-volume?ccy={}&period={}",
            self.base_url, currency, period
        );

        let response: serde_json::Value = self.client.get(&url).send().await?.json().await?;

        if response["code"].as_str() != Some("0") {
            anyhow::bail!(
                "Failed to get open interest history: {}",
                response["msg"].as_str().unwrap_or("Unknown error")
            );
        }

        let data = response["data"]
            .as_array()
            .ok_or_else(|| anyhow::anyhow!("Invalid response format"))?;

        let entries: Vec<OpenInterestEntry> = data
            .iter()
            .take(limit as usize)
            .filter_map(|e| {
                if let Some(arr) = e.as_array() {
                    // Format: [timestamp, oi_value_usd, volume_usd]
                    if arr.len() >= 2 {
                        let timestamp: i64 = arr[0].as_str()?.parse().ok()?;
                        let oi_value: f64 = arr[1].as_str()?.parse().ok()?;
                        return Some(OpenInterestEntry {
                            symbol: formatted_symbol.clone(),
                            open_interest: 0.0, // OKX returns OI in USD value, not contracts
                            open_interest_value: oi_value,
                            timestamp,
                        });
                    }
                }
                None
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
            .context("Failed to connect to OKX WebSocket")?;

        let connect_time = start.elapsed().as_millis() as u64;

        // OKX orderbook channels: books (400 depth), books5 (5 depth), bbo-tbt (BBO)
        let channel = if depth <= 5 { "books5" } else { "books" };

        let subscribe_msg = serde_json::json!({
            "op": "subscribe",
            "args": [{
                "channel": channel,
                "instId": formatted_symbol
            }]
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
                            // Skip subscription confirmations
                            if text.contains("\"event\":") {
                                continue;
                            }
                            if let Ok(parsed) = serde_json::from_str::<WsPublicMessage>(&text) {
                                if parsed.data.is_some() {
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
            subscription_type: format!("{}.{}", channel, symbol),
            data_sample,
        })
    }

    async fn test_ws_trades(&self, symbol: &str, timeout_ms: u64) -> Result<WsTestResult> {
        let formatted_symbol = self.format_symbol(symbol);
        let ws_url = self.ws_public_url();

        let start = std::time::Instant::now();
        let (mut ws_stream, _) = connect_async(ws_url)
            .await
            .context("Failed to connect to OKX WebSocket")?;

        let connect_time = start.elapsed().as_millis() as u64;

        let subscribe_msg = serde_json::json!({
            "op": "subscribe",
            "args": [{
                "channel": "trades",
                "instId": formatted_symbol
            }]
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
                            // Skip subscription confirmations
                            if text.contains("\"event\":") {
                                continue;
                            }
                            if let Ok(parsed) = serde_json::from_str::<WsPublicMessage>(&text) {
                                if parsed.data.is_some() {
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
            subscription_type: format!("trades.{}", symbol),
            data_sample,
        })
    }

    async fn test_ws_ticker(&self, symbol: &str, timeout_ms: u64) -> Result<WsTestResult> {
        let formatted_symbol = self.format_symbol(symbol);
        let ws_url = self.ws_public_url();

        let start = std::time::Instant::now();
        let (mut ws_stream, _) = connect_async(ws_url)
            .await
            .context("Failed to connect to OKX WebSocket")?;

        let connect_time = start.elapsed().as_millis() as u64;

        let subscribe_msg = serde_json::json!({
            "op": "subscribe",
            "args": [{
                "channel": "tickers",
                "instId": formatted_symbol
            }]
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
                            // Skip subscription confirmations
                            if text.contains("\"event\":") {
                                continue;
                            }
                            if let Ok(parsed) = serde_json::from_str::<WsPublicMessage>(&text) {
                                if parsed.data.is_some() {
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
            subscription_type: format!("tickers.{} (includes last, bid, ask, vol)", symbol),
            data_sample,
        })
    }

    fn format_symbol(&self, base_symbol: &str) -> String {
        let symbol = base_symbol.to_uppercase();
        // Handle BTC/USDT format
        let base = if symbol.contains('/') {
            symbol.split('/').next().unwrap_or(&symbol).to_string()
        } else if symbol.contains('-') {
            // Already in OKX format
            return if self.market_type == MarketType::Swap && !symbol.ends_with("-SWAP") {
                format!("{}-SWAP", symbol)
            } else {
                symbol
            };
        } else if symbol.ends_with("USDT") {
            symbol.trim_end_matches("USDT").to_string()
        } else {
            symbol
        };

        match self.market_type {
            MarketType::Spot => format!("{}-USDT", base),
            MarketType::Swap => format!("{}-USDT-SWAP", base),
        }
    }

    async fn subscribe_order_updates(
        &self,
        sender: broadcast::Sender<OrderUpdateEvent>,
    ) -> Result<JoinHandle<()>> {
        let api_key = self.api_key.clone();
        let api_secret = self.api_secret.clone();
        let passphrase = self.passphrase.clone();
        let testnet = self.testnet;
        let exchange_name = "okx".to_string();

        let handle = tokio::spawn(async move {
            let mut reconnect_delay = INITIAL_RECONNECT_DELAY_SECS;

            loop {
                let ws_url = if testnet {
                    TESTNET_WS_TRADE_URL
                } else {
                    MAINNET_WS_TRADE_URL
                };

                info!("[{}] Connecting to private WS: {}", exchange_name, ws_url);

                match connect_async(ws_url).await {
                    Ok((mut ws_stream, _)) => {
                        // Authenticate with login
                        let timestamp = (timestamp_ms() / 1000).to_string();
                        let sign_str = format!("{}GET/users/self/verify", timestamp);
                        type HmacSha256 = Hmac<Sha256>;
                        let signature = {
                            let mut mac = HmacSha256::new_from_slice(api_secret.as_bytes())
                                .expect("HMAC can take key of any size");
                            mac.update(sign_str.as_bytes());
                            BASE64.encode(mac.finalize().into_bytes())
                        };

                        let auth_msg = serde_json::json!({
                            "op": "login",
                            "args": [{
                                "apiKey": api_key,
                                "passphrase": passphrase,
                                "timestamp": timestamp,
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

                        // Wait for login response
                        let mut authenticated = false;
                        if let Some(Ok(Message::Text(text))) = ws_stream.next().await {
                            if let Ok(resp) = serde_json::from_str::<serde_json::Value>(&text) {
                                let event = resp.get("event").and_then(|v| v.as_str());
                                let code = resp.get("code").and_then(|v| v.as_str());
                                if event == Some("login") && code == Some("0") {
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
                            "args": [{"channel": "orders", "instType": "ANY"}]
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
                                                // Skip op responses (subscribe confirmations etc.)
                                                if msg_json.get("event").is_some() || msg_json.get("op").is_some() {
                                                    debug!("[{}] WS event: {}", exchange_name, text);
                                                    continue;
                                                }

                                                // Process order data
                                                if let Some(data_arr) = msg_json.get("data").and_then(|d| d.as_array()) {
                                                    for o in data_arr {
                                                        let event = OrderUpdateEvent {
                                                            exchange: exchange_name.clone(),
                                                            order_id: o.get("ordId").and_then(|v| v.as_str()).unwrap_or("").to_string(),
                                                            client_order_id: o.get("clOrdId").and_then(|v| v.as_str()).map(|s| s.to_string()),
                                                            symbol: o.get("instId").and_then(|v| v.as_str()).unwrap_or("").to_string(),
                                                            status: parse_order_status_str(
                                                                o.get("state").and_then(|v| v.as_str()).unwrap_or("live")
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
                                    if let Err(e) = ws_stream.send(Message::Text("ping".to_string())).await {
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
    fn test_format_symbol_spot() {
        let trader = OkxTrader::new("", "", None, false, MarketType::Spot);

        assert_eq!(trader.format_symbol("BTC"), "BTC-USDT");
        assert_eq!(trader.format_symbol("BTCUSDT"), "BTC-USDT");
        assert_eq!(trader.format_symbol("BTC/USDT"), "BTC-USDT");
        assert_eq!(trader.format_symbol("BTC-USDT"), "BTC-USDT");
    }

    #[test]
    fn test_format_symbol_swap() {
        let trader = OkxTrader::new("", "", None, false, MarketType::Swap);

        assert_eq!(trader.format_symbol("BTC"), "BTC-USDT-SWAP");
        assert_eq!(trader.format_symbol("BTCUSDT"), "BTC-USDT-SWAP");
        assert_eq!(trader.format_symbol("BTC/USDT"), "BTC-USDT-SWAP");
        assert_eq!(trader.format_symbol("BTC-USDT"), "BTC-USDT-SWAP");
        assert_eq!(trader.format_symbol("BTC-USDT-SWAP"), "BTC-USDT-SWAP");
    }

    #[test]
    fn test_parse_order_status() {
        assert_eq!(parse_order_status_str("live"), OrderStatus::New);
        assert_eq!(parse_order_status_str("filled"), OrderStatus::Filled);
        assert_eq!(parse_order_status_str("cancelled"), OrderStatus::Cancelled);
        assert_eq!(
            parse_order_status_str("partially_filled"),
            OrderStatus::PartiallyFilled
        );
    }
}
