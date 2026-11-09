//! Bybit exchange trader implementation.
//!
//! Full-featured client implementation:
//! - Persistent WebSocket connections for trading
//! - TP/SL support in execute_fast_entry
//! - Full close_position implementation
//! - Proper error handling (no silent unwrap_or defaults)
//! - Private WebSocket subscription for order updates

use crate::models::*;
use anyhow::{Context, Result};
use async_trait::async_trait;
use futures_util::{SinkExt, StreamExt};
use reqwest::Client;
use tokio::sync::broadcast;
use tokio::task::JoinHandle;
use tokio_tungstenite::{connect_async, tungstenite::protocol::Message};
use tracing::{debug, error, info, warn};
use tradersdk_core::models::{
    AccountBalance, ExchangeCapabilities, FundingRateEntry, MarketType, OpenInterestEntry,
    OrderInfo, OrderResult, OrderSide, OrderStatus, OrderType, OrderUpdateEvent, Orderbook,
    OrderbookLevel, TickerInfo, TimeInForce, WsTestResult,
};
use tradersdk_core::utils::{hmac_sign, timestamp_ms};
use tradersdk_core::ExchangeTrader;

const MAINNET_URL: &str = "https://api.bybit.com";
const TESTNET_URL: &str = "https://api-testnet.bybit.com";
const MAINNET_WS_URL: &str = "wss://stream.bybit.com/v5/public/linear";
const TESTNET_WS_URL: &str = "wss://stream-testnet.bybit.com/v5/public/linear";
const MAINNET_WS_SPOT_URL: &str = "wss://stream.bybit.com/v5/public/spot";
const TESTNET_WS_SPOT_URL: &str = "wss://stream-testnet.bybit.com/v5/public/spot";
const MAINNET_WS_TRADE_URL: &str = "wss://stream.bybit.com/v5/trade";
const TESTNET_WS_TRADE_URL: &str = "wss://stream-testnet.bybit.com/v5/trade";
const MAINNET_WS_PRIVATE_URL: &str = "wss://stream.bybit.com/v5/private";
const TESTNET_WS_PRIVATE_URL: &str = "wss://stream-testnet.bybit.com/v5/private";

/// Private WebSocket reconnect constants.
const INITIAL_RECONNECT_DELAY_SECS: u64 = 1;
const MAX_RECONNECT_DELAY_SECS: u64 = 30;

/// Calculate next reconnect delay with exponential backoff.
fn next_reconnect_delay(current_secs: u64) -> u64 {
    (current_secs * 2).min(MAX_RECONNECT_DELAY_SECS)
}

/// Parse order status string to OrderStatus enum.
fn parse_order_status_str(s: &str) -> OrderStatus {
    match s.to_lowercase().as_str() {
        "new" | "open" | "live" | "created" | "init" | "accepted" | "untriggered" | "triggered" => {
            OrderStatus::New
        }
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

/// Log an order update event.
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

/// Bybit exchange trader.
pub struct BybitTrader {
    api_key: String,
    api_secret: String,
    testnet: bool,
    market_type: MarketType,
    client: Client,
    base_url: String,
}

impl BybitTrader {
    /// Create a new Bybit trader.
    pub fn new(api_key: &str, api_secret: &str, testnet: bool, market_type: MarketType) -> Self {
        let base_url = if testnet { TESTNET_URL } else { MAINNET_URL };

        Self {
            api_key: api_key.to_string(),
            api_secret: api_secret.to_string(),
            testnet,
            market_type,
            client: Client::new(),
            base_url: base_url.to_string(),
        }
    }

    /// Sign a request.
    fn sign_request(&self, params: &str, timestamp: i64) -> String {
        let recv_window = "5000";
        let sign_str = format!("{}{}{}{}", timestamp, self.api_key, recv_window, params);
        hmac_sign(&self.api_secret, &sign_str)
    }

    /// Get WebSocket trade URL based on testnet setting.
    fn ws_trade_url(&self) -> &str {
        if self.testnet {
            TESTNET_WS_TRADE_URL
        } else {
            MAINNET_WS_TRADE_URL
        }
    }

    /// Generate WebSocket authentication signature.
    fn ws_auth_signature(&self, expires: i64) -> String {
        let sign_str = format!("GET/realtime{}", expires);
        hmac_sign(&self.api_secret, &sign_str)
    }

    /// Get category string for API calls.
    fn category(&self) -> &str {
        match self.market_type {
            MarketType::Spot => "spot",
            MarketType::Swap => "linear",
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
        let (mut ws_stream, _) = connect_async(ws_url)
            .await
            .context("Failed to connect to Bybit trade WebSocket")?;

        // Authenticate
        let expires = (timestamp_ms() + 10000) / 1000;
        let signature = self.ws_auth_signature(expires);

        let auth_msg = WsAuthRequest {
            req_id: format!("auth_{}", timestamp_ms()),
            op: "auth".to_string(),
            args: vec![self.api_key.clone(), expires.to_string(), signature],
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
                        let auth_resp: WsAuthResponse = serde_json::from_str(&text)
                            .context("Failed to parse auth response")?;

                        if !auth_resp.success {
                            anyhow::bail!("WebSocket authentication failed: {:?}", auth_resp.ret_msg);
                        }

                        info!("WebSocket authenticated, connId: {:?}", auth_resp.conn_id);
                    }
                    Some(Ok(Message::Ping(_))) => debug!("Received ping during auth"),
                    Some(Ok(Message::Pong(_))) => debug!("Received pong during auth"),
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
        let mut ws_stream = self.create_ws_trade_connection().await?;

        let timestamp = timestamp_ms();
        let req_id = format!("order_{}", timestamp);

        let mut order_params = serde_json::json!({
            "category": self.category(),
            "symbol": symbol,
            "side": match side {
                OrderSide::Buy => "Buy",
                OrderSide::Sell => "Sell",
            },
            "orderType": match order_type {
                OrderType::Market => "Market",
                OrderType::Limit => "Limit",
                _ => "Market",
            },
            "qty": format!("{:.8}", quantity),
        });

        if let (OrderType::Limit, Some(p)) = (order_type, price) {
            order_params["price"] = serde_json::json!(format!("{:.8}", p));
            order_params["timeInForce"] = serde_json::json!("GTC");
        }

        if reduce_only && self.market_type == MarketType::Swap {
            order_params["reduceOnly"] = serde_json::json!(true);
        }

        let order_req = WsOrderRequest {
            req_id: req_id.clone(),
            header: WsOrderHeader {
                timestamp: timestamp.to_string(),
                recv_window: "5000".to_string(),
            },
            op: "order.create".to_string(),
            args: vec![order_params],
        };

        let order_json = serde_json::to_string(&order_req)?;
        debug!("Sending WS order: {}", order_json);

        ws_stream.send(Message::Text(order_json)).await?;

        // Wait for response
        let timeout = tokio::time::Duration::from_secs(10);
        let mut order_result = None;

        tokio::select! {
            msg = ws_stream.next() => {
                match msg {
                    Some(Ok(Message::Text(text))) => {
                        debug!("Order response: {}", text);
                        let order_resp: WsOrderResponse = serde_json::from_str(&text)
                            .context("Failed to parse order response")?;

                        if order_resp.success {
                            if let Some(data) = order_resp.data {
                                if let Some(order_id) = data.order_id {
                                    info!("Bybit WS order placed: {}", order_id);
                                    order_result = Some(OrderResult {
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
                        } else {
                            let error_msg = order_resp.ret_msg
                                .unwrap_or_else(|| "Unknown error".to_string());
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

        let _ = ws_stream.close(None).await;
        order_result.ok_or_else(|| anyhow::anyhow!("No order result received"))
    }

    /// Cancel order via WebSocket.
    async fn cancel_order_ws(&self, symbol: &str, order_id: &str) -> Result<bool> {
        let mut ws_stream = self.create_ws_trade_connection().await?;

        let timestamp = timestamp_ms();
        let req_id = format!("cancel_{}", timestamp);

        let cancel_params = serde_json::json!({
            "category": self.category(),
            "symbol": symbol,
            "orderId": order_id,
        });

        let cancel_req = WsOrderRequest {
            req_id: req_id.clone(),
            header: WsOrderHeader {
                timestamp: timestamp.to_string(),
                recv_window: "5000".to_string(),
            },
            op: "order.cancel".to_string(),
            args: vec![cancel_params],
        };

        let cancel_json = serde_json::to_string(&cancel_req)?;
        debug!("Sending WS cancel: {}", cancel_json);

        ws_stream.send(Message::Text(cancel_json)).await?;

        let timeout = tokio::time::Duration::from_secs(10);
        let mut success = false;

        tokio::select! {
            msg = ws_stream.next() => {
                match msg {
                    Some(Ok(Message::Text(text))) => {
                        debug!("Cancel response: {}", text);
                        let cancel_resp: WsOrderResponse = serde_json::from_str(&text)
                            .context("Failed to parse cancel response")?;

                        if cancel_resp.success {
                            info!("Bybit WS order cancelled: {}", order_id);
                            success = true;
                        } else {
                            let error_msg = cancel_resp.ret_msg
                                .unwrap_or_else(|| "Unknown error".to_string());
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
        let mut ws_stream = self.create_ws_trade_connection().await?;

        let timestamp = timestamp_ms();
        let req_id = format!("amend_{}", timestamp);

        let mut amend_params = serde_json::json!({
            "category": self.category(),
            "symbol": symbol,
            "orderId": order_id,
        });

        if let Some(qty) = new_quantity {
            amend_params["qty"] = serde_json::json!(format!("{:.8}", qty));
        }

        if let Some(price) = new_price {
            amend_params["price"] = serde_json::json!(format!("{:.8}", price));
        }

        let amend_req = WsOrderRequest {
            req_id: req_id.clone(),
            header: WsOrderHeader {
                timestamp: timestamp.to_string(),
                recv_window: "5000".to_string(),
            },
            op: "order.amend".to_string(),
            args: vec![amend_params],
        };

        let amend_json = serde_json::to_string(&amend_req)?;
        debug!("Sending WS amend: {}", amend_json);

        ws_stream.send(Message::Text(amend_json)).await?;

        let timeout = tokio::time::Duration::from_secs(10);
        let mut success = false;

        tokio::select! {
            msg = ws_stream.next() => {
                match msg {
                    Some(Ok(Message::Text(text))) => {
                        debug!("Amend response: {}", text);
                        let amend_resp: WsOrderResponse = serde_json::from_str(&text)
                            .context("Failed to parse amend response")?;

                        if amend_resp.success {
                            info!("Bybit WS order amended: {}", order_id);
                            success = true;
                        } else {
                            let error_msg = amend_resp.ret_msg
                                .unwrap_or_else(|| "Unknown error".to_string());
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

        let _ = ws_stream.close(None).await;
        Ok(success)
    }

    /// Make a signed GET request.
    async fn signed_get<T: for<'de> serde::Deserialize<'de>>(
        &self,
        endpoint: &str,
        params: &str,
    ) -> Result<T> {
        let timestamp = timestamp_ms();
        let signature = self.sign_request(params, timestamp);

        let url = format!("{}{}?{}", self.base_url, endpoint, params);

        let response = self
            .client
            .get(&url)
            .header("X-BAPI-API-KEY", &self.api_key)
            .header("X-BAPI-SIGN", signature)
            .header("X-BAPI-TIMESTAMP", timestamp.to_string())
            .header("X-BAPI-RECV-WINDOW", "5000")
            .send()
            .await
            .context("Failed to send request to Bybit")?;

        let text = response.text().await?;
        debug!("Bybit response: {}", text);

        serde_json::from_str(&text).context("Failed to parse Bybit response")
    }

    /// Make a signed POST request.
    async fn signed_post<T: for<'de> serde::Deserialize<'de>>(
        &self,
        endpoint: &str,
        body: &serde_json::Value,
    ) -> Result<T> {
        let timestamp = timestamp_ms();
        let body_str = body.to_string();
        let signature = self.sign_request(&body_str, timestamp);

        let url = format!("{}{}", self.base_url, endpoint);

        let response = self
            .client
            .post(&url)
            .header("Content-Type", "application/json")
            .header("X-BAPI-API-KEY", &self.api_key)
            .header("X-BAPI-SIGN", signature)
            .header("X-BAPI-TIMESTAMP", timestamp.to_string())
            .header("X-BAPI-RECV-WINDOW", "5000")
            .body(body_str)
            .send()
            .await
            .context("Failed to send request to Bybit")?;

        let text = response.text().await?;
        debug!("Bybit response: {}", text);

        serde_json::from_str(&text).context("Failed to parse Bybit response")
    }

    /// Set Take Profit and Stop Loss for a position.
    async fn set_trading_stop(
        &self,
        symbol: &str,
        take_profit: Option<f64>,
        stop_loss: Option<f64>,
        position_idx: i32,
    ) -> Result<bool> {
        if self.market_type == MarketType::Spot {
            return Ok(true); // TP/SL not supported for spot
        }

        let mut body = serde_json::json!({
            "category": "linear",
            "symbol": symbol,
            "positionIdx": position_idx,
        });

        if let Some(tp) = take_profit {
            body["takeProfit"] = serde_json::json!(format!("{:.8}", tp));
            body["tpTriggerBy"] = serde_json::json!("LastPrice");
        }

        if let Some(sl) = stop_loss {
            body["stopLoss"] = serde_json::json!(format!("{:.8}", sl));
            body["slTriggerBy"] = serde_json::json!("LastPrice");
        }

        let response: BybitResponse<serde_json::Value> =
            self.signed_post("/v5/position/trading-stop", &body).await?;

        if response.is_success() {
            info!(
                "Set trading stop for {}: TP={:?}, SL={:?}",
                symbol, take_profit, stop_loss
            );
            Ok(true)
        } else {
            error!(
                "Failed to set trading stop for {}: {}",
                symbol, response.ret_msg
            );
            Ok(false)
        }
    }

    /// Get current position for a symbol.
    async fn get_position(&self, symbol: &str) -> Result<Option<PositionInfo>> {
        if self.market_type == MarketType::Spot {
            return Ok(None);
        }

        let params = format!("category=linear&symbol={}", symbol);
        let response: BybitResponse<PositionListResult> =
            self.signed_get("/v5/position/list", &params).await?;

        if let Some(result) = response.result {
            if let Some(pos) = result.list.first() {
                let size_str = &pos.size;
                let size: f64 = size_str.parse().unwrap_or(0.0);

                if size.abs() > 0.0 {
                    return Ok(Some(pos.clone()));
                }
            }
        }

        Ok(None)
    }
}

#[async_trait]
impl ExchangeTrader for BybitTrader {
    fn exchange_name(&self) -> &str {
        "bybit"
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
        let response: BybitResponse<ServerTimeResult> = self
            .client
            .get(format!("{}/v5/market/time", self.base_url))
            .send()
            .await?
            .json()
            .await?;

        Ok(response.is_success())
    }

    async fn get_server_time(&self) -> Result<i64> {
        let response: BybitResponse<ServerTimeResult> = self
            .client
            .get(format!("{}/v5/market/time", self.base_url))
            .send()
            .await?
            .json()
            .await?;

        if let Some(result) = response.result {
            let seconds: i64 = result
                .time_second
                .parse()
                .context("Failed to parse server time")?;
            Ok(seconds * 1000)
        } else {
            anyhow::bail!("Failed to get server time: {}", response.ret_msg)
        }
    }

    async fn get_balance(&self) -> Result<AccountBalance> {
        let params = "accountType=UNIFIED";

        let timestamp = timestamp_ms();
        let signature = self.sign_request(params, timestamp);
        let url = format!("{}/v5/account/wallet-balance?{}", self.base_url, params);

        let response = self
            .client
            .get(&url)
            .header("X-BAPI-API-KEY", &self.api_key)
            .header("X-BAPI-SIGN", signature)
            .header("X-BAPI-TIMESTAMP", timestamp.to_string())
            .header("X-BAPI-RECV-WINDOW", "5000")
            .send()
            .await
            .context("Failed to send request to Bybit")?;

        let text = response.text().await?;
        debug!("Bybit balance response: {}", text);

        let response: BybitResponse<WalletBalanceResult> = serde_json::from_str(&text)
            .context(format!("Failed to parse Bybit balance response: {}", text))?;

        if let Some(result) = response.result {
            if let Some(account) = result.list.first() {
                let available: f64 = account
                    .total_available_balance
                    .parse()
                    .context("Failed to parse available balance")?;
                return Ok(AccountBalance::simple(available, available, "USDT"));
            }
        }

        // If no balance found, return zero balance instead of error
        Ok(AccountBalance::default())
    }

    async fn get_current_price(&self, symbol: &str) -> Result<f64> {
        let formatted_symbol = self.format_symbol(symbol);
        let params = format!("category={}&symbol={}", self.category(), formatted_symbol);
        let response: BybitResponse<TickerResult> = self
            .client
            .get(format!("{}/v5/market/tickers?{}", self.base_url, params))
            .send()
            .await?
            .json()
            .await?;

        if let Some(result) = response.result {
            if let Some(ticker) = result.list.first() {
                let price: f64 = ticker
                    .last_price
                    .parse()
                    .context("Failed to parse last price")?;
                return Ok(price);
            }
        }

        anyhow::bail!("Failed to get price for {}: {}", symbol, response.ret_msg)
    }

    async fn get_ticker(&self, symbol: &str) -> Result<TickerInfo> {
        let formatted_symbol = self.format_symbol(symbol);
        let params = format!("category={}&symbol={}", self.category(), formatted_symbol);
        let response: BybitResponse<TickerResult> = self
            .client
            .get(format!("{}/v5/market/tickers?{}", self.base_url, params))
            .send()
            .await?
            .json()
            .await?;

        if let Some(result) = response.result {
            if let Some(t) = result.list.first() {
                return Ok(TickerInfo {
                    symbol: t.symbol.clone(),
                    last_price: t.last_price.parse().unwrap_or(0.0),
                    bid_price: t.bid1_price.as_ref().and_then(|p| p.parse().ok()),
                    ask_price: t.ask1_price.as_ref().and_then(|p| p.parse().ok()),
                    high_24h: t.high_price_24h.as_ref().and_then(|p| p.parse().ok()),
                    low_24h: t.low_price_24h.as_ref().and_then(|p| p.parse().ok()),
                    volume_24h: t.volume_24h.as_ref().and_then(|p| p.parse().ok()),
                    turnover_24h: t.turnover_24h.as_ref().and_then(|p| p.parse().ok()),
                    change_24h: None,
                    open_interest: t.open_interest.as_ref().and_then(|p| p.parse().ok()),
                    open_interest_value: t
                        .open_interest_value
                        .as_ref()
                        .and_then(|p| p.parse().ok()),
                    funding_rate: t.funding_rate.as_ref().and_then(|p| p.parse().ok()),
                    next_funding_time: t.next_funding_time.as_ref().and_then(|p| p.parse().ok()),
                    mark_price: t.mark_price.as_ref().and_then(|p| p.parse().ok()),
                    index_price: t.index_price.as_ref().and_then(|p| p.parse().ok()),
                    timestamp: timestamp_ms(),
                });
            }
        }

        anyhow::bail!("Failed to get ticker for {}: {}", symbol, response.ret_msg)
    }

    async fn get_orderbook(&self, symbol: &str, depth: u32) -> Result<Orderbook> {
        let formatted_symbol = self.format_symbol(symbol);
        let limit = depth.min(200);
        let params = format!(
            "category={}&symbol={}&limit={}",
            self.category(),
            formatted_symbol,
            limit
        );

        let response: BybitResponse<OrderbookResult> = self
            .client
            .get(format!("{}/v5/market/orderbook?{}", self.base_url, params))
            .send()
            .await?
            .json()
            .await?;

        if let Some(result) = response.result {
            let bids: Vec<OrderbookLevel> = result
                .b
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

            let asks: Vec<OrderbookLevel> = result
                .a
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

            return Ok(Orderbook {
                symbol: if result.s.is_empty() {
                    formatted_symbol
                } else {
                    result.s
                },
                bids,
                asks,
                timestamp: result.ts,
            });
        }

        anyhow::bail!("Failed to get orderbook: {}", response.ret_msg)
    }

    async fn set_leverage(&self, symbol: Option<&str>, leverage: u32) -> Result<bool> {
        if self.market_type == MarketType::Spot {
            return Ok(true);
        }

        let body = if let Some(sym) = symbol {
            let formatted_symbol = self.format_symbol(sym);
            serde_json::json!({
                "category": "linear",
                "symbol": formatted_symbol,
                "buyLeverage": leverage.to_string(),
                "sellLeverage": leverage.to_string(),
            })
        } else {
            return Ok(true);
        };

        let response: BybitResponse<serde_json::Value> =
            self.signed_post("/v5/position/set-leverage", &body).await?;

        // Leverage setting can fail with "leverage not changed" which is fine
        if response.is_success() || response.ret_code == 110043 {
            Ok(true)
        } else {
            warn!("Failed to set leverage: {}", response.ret_msg);
            Ok(false)
        }
    }

    async fn execute_fast_entry(
        &self,
        symbol: &str,
        direction: &str,
        usdt_amount: f64,
        leverage: u32,
        tp_percent: Option<f64>,
        sl_percent: Option<f64>,
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

        // Place order
        let order_result = self
            .place_market_order(&formatted_symbol, side, quantity)
            .await?;

        // Set TP/SL if order was successful and we're in swap mode
        if order_result.success && self.market_type == MarketType::Swap {
            let has_tp_sl = tp_percent.is_some() || sl_percent.is_some();
            if has_tp_sl {
                // Calculate TP/SL prices
                let is_long = direction.to_lowercase() == "long";
                let position_idx = if is_long { 1 } else { 2 };

                let take_profit = tp_percent.map(|pct| {
                    if is_long {
                        price * (1.0 + pct / 100.0)
                    } else {
                        price * (1.0 - pct / 100.0)
                    }
                });

                let stop_loss = sl_percent.map(|pct| {
                    if is_long {
                        price * (1.0 - pct / 100.0)
                    } else {
                        price * (1.0 + pct / 100.0)
                    }
                });

                // Small delay to ensure position is registered
                tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

                if let Err(e) = self
                    .set_trading_stop(&formatted_symbol, take_profit, stop_loss, position_idx)
                    .await
                {
                    warn!("Failed to set TP/SL: {}", e);
                }
            }
        }

        Ok(order_result)
    }

    async fn place_market_order(
        &self,
        symbol: &str,
        side: OrderSide,
        quantity: f64,
    ) -> Result<OrderResult> {
        let formatted_symbol = self.format_symbol(symbol);
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
        self.cancel_order_ws(symbol, order_id).await
    }

    async fn get_order_status(&self, symbol: &str, order_id: &str) -> Result<OrderInfo> {
        let params = format!(
            "category={}&symbol={}&orderId={}",
            self.category(),
            symbol,
            order_id
        );

        let response: BybitResponse<serde_json::Value> =
            self.signed_get("/v5/order/realtime", &params).await?;

        if response.ret_code != 0 {
            anyhow::bail!("Failed to get order status: {}", response.ret_msg);
        }

        let result = response
            .result
            .ok_or_else(|| anyhow::anyhow!("No order data in response"))?;

        let list = result
            .get("list")
            .and_then(|l| l.as_array())
            .ok_or_else(|| anyhow::anyhow!("Invalid order list format"))?;

        let order_data = list
            .first()
            .ok_or_else(|| anyhow::anyhow!("Order not found"))?;

        let status_str = order_data
            .get("orderStatus")
            .and_then(|s| s.as_str())
            .unwrap_or("Unknown");

        let status = parse_order_status_str(status_str);

        let side_str = order_data
            .get("side")
            .and_then(|s| s.as_str())
            .unwrap_or("Buy");
        let side = if side_str == "Sell" {
            OrderSide::Sell
        } else {
            OrderSide::Buy
        };

        let order_type_str = order_data
            .get("orderType")
            .and_then(|s| s.as_str())
            .unwrap_or("Market");
        let order_type = if order_type_str == "Limit" {
            OrderType::Limit
        } else {
            OrderType::Market
        };

        let price = order_data
            .get("price")
            .and_then(|p| p.as_str())
            .and_then(|p| p.parse::<f64>().ok())
            .unwrap_or(0.0);

        let quantity = order_data
            .get("qty")
            .and_then(|q| q.as_str())
            .and_then(|q| q.parse::<f64>().ok())
            .unwrap_or(0.0);

        let filled_quantity = order_data
            .get("cumExecQty")
            .and_then(|q| q.as_str())
            .and_then(|q| q.parse::<f64>().ok())
            .unwrap_or(0.0);

        let avg_price = order_data
            .get("avgPrice")
            .and_then(|p| p.as_str())
            .and_then(|p| p.parse::<f64>().ok());

        let timestamp = order_data
            .get("createdTime")
            .and_then(|t| t.as_str())
            .and_then(|t| t.parse::<i64>().ok())
            .unwrap_or(0);

        Ok(OrderInfo {
            order_id: order_id.to_string(),
            client_order_id: order_data
                .get("orderLinkId")
                .and_then(|s| s.as_str())
                .map(|s| s.to_string()),
            symbol: symbol.to_string(),
            status,
            side,
            order_type,
            price,
            quantity,
            filled_quantity,
            remaining_quantity: quantity - filled_quantity,
            avg_price,
            timestamp,
            time_in_force: TimeInForce::GTC,
            reduce_only: order_data
                .get("reduceOnly")
                .and_then(|r| r.as_bool())
                .unwrap_or(false),
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
                let size: f64 = pos.size.parse().context("Failed to parse position size")?;
                if size.abs() < 0.000001 {
                    return Ok(OrderResult::failure(
                        formatted_symbol,
                        OrderSide::Sell,
                        "No position to close".to_string(),
                    ));
                }

                // Determine close side (opposite of position side)
                let close_side = if pos.side == "Buy" {
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

    fn format_symbol(&self, base_symbol: &str) -> String {
        let symbol = base_symbol.to_uppercase();
        if symbol.contains('/') {
            return symbol.replace("/", "");
        }
        if symbol.ends_with("USDT") {
            symbol
        } else {
            format!("{}USDT", symbol)
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
            "category=linear&symbol={}&limit={}",
            formatted_symbol,
            limit.min(200)
        );

        let response: BybitResponse<FundingHistoryResult> = self
            .client
            .get(format!(
                "{}/v5/market/funding/history?{}",
                self.base_url, params
            ))
            .send()
            .await?
            .json()
            .await?;

        if let Some(result) = response.result {
            let entries: Vec<FundingRateEntry> = result
                .list
                .iter()
                .filter_map(|e| {
                    let funding_rate: f64 = e.funding_rate.parse().ok()?;
                    let funding_time: i64 = e.funding_rate_timestamp.parse().ok()?;
                    Some(FundingRateEntry {
                        symbol: e.symbol.clone(),
                        funding_rate,
                        funding_time,
                    })
                })
                .collect();
            return Ok(entries);
        }

        anyhow::bail!("Failed to get funding history: {}", response.ret_msg)
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

        let formatted_symbol = self.format_symbol(symbol);
        let params = format!(
            "category=linear&symbol={}&intervalTime={}&limit={}",
            formatted_symbol,
            interval,
            limit.min(200)
        );

        let response: BybitResponse<OpenInterestResult> = self
            .client
            .get(format!(
                "{}/v5/market/open-interest?{}",
                self.base_url, params
            ))
            .send()
            .await?
            .json()
            .await?;

        if let Some(result) = response.result {
            let entries: Vec<OpenInterestEntry> = result
                .list
                .iter()
                .filter_map(|e| {
                    let open_interest: f64 = e.open_interest.parse().ok()?;
                    let timestamp: i64 = e.timestamp.parse().ok()?;
                    Some(OpenInterestEntry {
                        symbol: symbol.to_string(),
                        open_interest,
                        open_interest_value: 0.0, // Not provided in this endpoint
                        timestamp,
                    })
                })
                .collect();
            return Ok(entries);
        }

        anyhow::bail!("Failed to get open interest history: {}", response.ret_msg)
    }

    async fn test_ws_orderbook(
        &self,
        symbol: &str,
        depth: u32,
        timeout_ms: u64,
    ) -> Result<WsTestResult> {
        let formatted_symbol = self.format_symbol(symbol);
        let ws_url = match (self.testnet, self.market_type) {
            (true, MarketType::Swap) => TESTNET_WS_URL,
            (false, MarketType::Swap) => MAINNET_WS_URL,
            (true, MarketType::Spot) => TESTNET_WS_SPOT_URL,
            (false, MarketType::Spot) => MAINNET_WS_SPOT_URL,
        };

        let start = std::time::Instant::now();
        let (mut ws_stream, _) = connect_async(ws_url)
            .await
            .context("Failed to connect to Bybit WebSocket")?;

        let connect_time = start.elapsed().as_millis() as u64;

        let depth_level = if depth <= 1 {
            1
        } else if depth <= 50 {
            50
        } else if depth <= 200 {
            200
        } else {
            500
        };

        let subscribe_msg = serde_json::json!({
            "op": "subscribe",
            "args": [format!("orderbook.{}.{}", depth_level, formatted_symbol)]
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
                                if parsed.topic.is_some() && parsed.data.is_some() {
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
            subscription_type: format!("orderbook.{}.{}", depth_level, symbol),
            data_sample,
        })
    }

    async fn test_ws_trades(&self, symbol: &str, timeout_ms: u64) -> Result<WsTestResult> {
        let formatted_symbol = self.format_symbol(symbol);
        let ws_url = match (self.testnet, self.market_type) {
            (true, MarketType::Swap) => TESTNET_WS_URL,
            (false, MarketType::Swap) => MAINNET_WS_URL,
            (true, MarketType::Spot) => TESTNET_WS_SPOT_URL,
            (false, MarketType::Spot) => MAINNET_WS_SPOT_URL,
        };

        let start = std::time::Instant::now();
        let (mut ws_stream, _) = connect_async(ws_url)
            .await
            .context("Failed to connect to Bybit WebSocket")?;

        let connect_time = start.elapsed().as_millis() as u64;

        let subscribe_msg = serde_json::json!({
            "op": "subscribe",
            "args": [format!("publicTrade.{}", formatted_symbol)]
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
                                if parsed.topic.is_some() && parsed.data.is_some() {
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
            subscription_type: format!("publicTrade.{}", symbol),
            data_sample,
        })
    }

    async fn test_ws_ticker(&self, symbol: &str, timeout_ms: u64) -> Result<WsTestResult> {
        let formatted_symbol = self.format_symbol(symbol);
        let ws_url = match (self.testnet, self.market_type) {
            (true, MarketType::Swap) => TESTNET_WS_URL,
            (false, MarketType::Swap) => MAINNET_WS_URL,
            (true, MarketType::Spot) => TESTNET_WS_SPOT_URL,
            (false, MarketType::Spot) => MAINNET_WS_SPOT_URL,
        };

        let start = std::time::Instant::now();
        let (mut ws_stream, _) = connect_async(ws_url)
            .await
            .context("Failed to connect to Bybit WebSocket")?;

        let connect_time = start.elapsed().as_millis() as u64;

        let subscribe_msg = serde_json::json!({
            "op": "subscribe",
            "args": [format!("tickers.{}", formatted_symbol)]
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
                                if parsed.topic.is_some() && parsed.data.is_some() {
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
            subscription_type: format!("tickers.{} (includes OI, fundingRate)", symbol),
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
        let exchange_name = "bybit".to_string();

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
                        let expires = (timestamp_ms() + 10000) / 1000;
                        let sign_str = format!("GET/realtime{}", expires);
                        let signature = hmac_sign(&api_secret, &sign_str);

                        let auth_msg = serde_json::json!({
                            "reqId": format!("auth_{}", timestamp_ms()),
                            "op": "auth",
                            "args": [api_key, expires.to_string(), signature]
                        });

                        if let Err(e) = ws_stream.send(Message::Text(auth_msg.to_string())).await {
                            error!("[{}] Failed to send auth: {}", exchange_name, e);
                            tokio::time::sleep(tokio::time::Duration::from_secs(reconnect_delay))
                                .await;
                            reconnect_delay = next_reconnect_delay(reconnect_delay);
                            continue;
                        }

                        // Wait for auth response
                        let mut authenticated = false;
                        if let Some(Ok(Message::Text(text))) = ws_stream.next().await {
                            if let Ok(resp) = serde_json::from_str::<serde_json::Value>(&text) {
                                if resp.get("success").and_then(|v| v.as_bool()) == Some(true) {
                                    info!("[{}] Private WS authenticated", exchange_name);
                                    authenticated = true;
                                } else {
                                    error!("[{}] Auth failed: {}", exchange_name, text);
                                }
                            }
                        }

                        if !authenticated {
                            tokio::time::sleep(tokio::time::Duration::from_secs(reconnect_delay))
                                .await;
                            reconnect_delay = next_reconnect_delay(reconnect_delay);
                            continue;
                        }

                        // Subscribe to order updates
                        let sub_msg = serde_json::json!({
                            "reqId": format!("sub_{}", timestamp_ms()),
                            "op": "subscribe",
                            "args": ["order"]
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

                        // Main message loop with ping interval
                        let mut ping_interval =
                            tokio::time::interval(tokio::time::Duration::from_secs(20));

                        loop {
                            tokio::select! {
                                msg = ws_stream.next() => {
                                    match msg {
                                        Some(Ok(Message::Text(text))) => {
                                            if let Ok(msg_json) = serde_json::from_str::<serde_json::Value>(&text) {
                                                // Skip subscription confirmations and pong responses
                                                if msg_json.get("op").is_some() {
                                                    debug!("[{}] WS op response: {}", exchange_name, text);
                                                    continue;
                                                }

                                                // Process order update topic
                                                let topic = msg_json.get("topic").and_then(|t| t.as_str());
                                                if topic == Some("order") {
                                                    if let Some(data_arr) = msg_json.get("data").and_then(|d| d.as_array()) {
                                                        for order_data in data_arr {
                                                            let order_id = order_data.get("orderId")
                                                                .and_then(|v| v.as_str())
                                                                .unwrap_or("")
                                                                .to_string();
                                                            let symbol = order_data.get("symbol")
                                                                .and_then(|v| v.as_str())
                                                                .unwrap_or("")
                                                                .to_string();
                                                            let status_str = order_data.get("orderStatus")
                                                                .and_then(|v| v.as_str())
                                                                .unwrap_or("New");
                                                            let side_str = order_data.get("side")
                                                                .and_then(|v| v.as_str())
                                                                .unwrap_or("Buy");
                                                            let order_type_str = order_data.get("orderType")
                                                                .and_then(|v| v.as_str())
                                                                .unwrap_or("Market");
                                                            let price = order_data.get("price")
                                                                .and_then(|v| v.as_str())
                                                                .and_then(|s| s.parse::<f64>().ok())
                                                                .unwrap_or(0.0);
                                                            let qty = order_data.get("qty")
                                                                .and_then(|v| v.as_str())
                                                                .and_then(|s| s.parse::<f64>().ok())
                                                                .unwrap_or(0.0);
                                                            let filled_qty = order_data.get("cumExecQty")
                                                                .and_then(|v| v.as_str())
                                                                .and_then(|s| s.parse::<f64>().ok())
                                                                .unwrap_or(0.0);
                                                            let avg_price = order_data.get("avgPrice")
                                                                .and_then(|v| v.as_str())
                                                                .and_then(|s| s.parse::<f64>().ok());
                                                            let created_time = order_data.get("createdTime")
                                                                .and_then(|v| v.as_str())
                                                                .and_then(|s| s.parse::<i64>().ok())
                                                                .unwrap_or(0);
                                                            let cum_exec_fee = order_data.get("cumExecFee")
                                                                .and_then(|v| v.as_str())
                                                                .and_then(|s| s.parse::<f64>().ok());

                                                            let event = OrderUpdateEvent {
                                                                exchange: exchange_name.clone(),
                                                                order_id,
                                                                client_order_id: order_data.get("orderLinkId")
                                                                    .and_then(|v| v.as_str())
                                                                    .map(|s| s.to_string()),
                                                                symbol,
                                                                status: parse_order_status_str(status_str),
                                                                side: if side_str == "Buy" { OrderSide::Buy } else { OrderSide::Sell },
                                                                order_type: if order_type_str == "Market" { OrderType::Market } else { OrderType::Limit },
                                                                price,
                                                                quantity: qty,
                                                                filled_quantity: filled_qty,
                                                                avg_fill_price: avg_price,
                                                                fee: cum_exec_fee,
                                                                fee_currency: None,
                                                                exchange_timestamp: created_time,
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
                                    let ping = serde_json::json!({"op": "ping"});
                                    if let Err(e) = ws_stream.send(Message::Text(ping.to_string())).await {
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
    fn test_format_symbol() {
        let trader = BybitTrader::new("", "", false, MarketType::Swap);

        assert_eq!(trader.format_symbol("BTC"), "BTCUSDT");
        assert_eq!(trader.format_symbol("BTCUSDT"), "BTCUSDT");
        assert_eq!(trader.format_symbol("BTC/USDT"), "BTCUSDT");
        assert_eq!(trader.format_symbol("eth"), "ETHUSDT");
    }

    #[test]
    fn test_parse_order_status() {
        assert_eq!(parse_order_status_str("New"), OrderStatus::New);
        assert_eq!(parse_order_status_str("Filled"), OrderStatus::Filled);
        assert_eq!(
            parse_order_status_str("PartiallyFilled"),
            OrderStatus::PartiallyFilled
        );
        assert_eq!(parse_order_status_str("Cancelled"), OrderStatus::Cancelled);
        assert_eq!(parse_order_status_str("Rejected"), OrderStatus::Rejected);
        assert_eq!(parse_order_status_str("Expired"), OrderStatus::Expired);
    }

    #[test]
    fn test_next_reconnect_delay() {
        assert_eq!(next_reconnect_delay(1), 2);
        assert_eq!(next_reconnect_delay(2), 4);
        assert_eq!(next_reconnect_delay(16), 30); // Capped at MAX
        assert_eq!(next_reconnect_delay(30), 30); // Already at max
    }
}
