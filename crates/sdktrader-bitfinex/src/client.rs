//! Bitfinex exchange trader implementation.

use anyhow::{Context, Result};
use async_trait::async_trait;
use futures_util::{SinkExt, StreamExt};
use reqwest::Client;
use sdktrader_core::models::{
    AccountBalance, Balance, ExchangeCapabilities, MarketType, OrderInfo, OrderResult, OrderSide,
    OrderStatus, OrderType, OrderUpdateEvent, Orderbook, OrderbookLevel, TickerInfo, TimeInForce,
    WsTestResult,
};
use sdktrader_core::utils::timestamp_ms;
use sdktrader_core::ExchangeTrader;
use tokio::sync::broadcast;
use tokio::task::JoinHandle;
use tokio_tungstenite::{connect_async, tungstenite::protocol::Message};
use tracing::{debug, error, info, warn};

// Bitfinex API endpoints
const PUBLIC_REST_URL: &str = "https://api-pub.bitfinex.com/v2";
const AUTH_REST_URL: &str = "https://api.bitfinex.com/v2";
const WS_URL: &str = "wss://api-pub.bitfinex.com/ws/2";
const AUTH_WS_URL: &str = "wss://api.bitfinex.com/ws/2";

/// Maximum reconnect backoff delay in seconds.
const MAX_RECONNECT_DELAY_SECS: u64 = 30;

/// Initial reconnect delay in seconds.
const INITIAL_RECONNECT_DELAY_SECS: u64 = 1;

/// Bitfinex exchange trader.
pub struct BitfinexTrader {
    api_key: String,
    api_secret: String,
    testnet: bool,
    market_type: MarketType,
    client: Client,
}

impl BitfinexTrader {
    /// Create a new Bitfinex trader.
    pub fn new(api_key: &str, api_secret: &str, testnet: bool, market_type: MarketType) -> Self {
        Self {
            api_key: api_key.to_string(),
            api_secret: api_secret.to_string(),
            testnet,
            market_type,
            client: Client::new(),
        }
    }

    /// Get public REST API base URL.
    fn public_url(&self) -> &str {
        PUBLIC_REST_URL
    }

    /// Get authenticated REST API base URL.
    fn auth_url(&self) -> &str {
        AUTH_REST_URL
    }

    /// Get WebSocket URL.
    fn ws_url(&self) -> &str {
        WS_URL
    }

    /// Get authenticated WebSocket URL.
    fn auth_ws_url(&self) -> &str {
        AUTH_WS_URL
    }

    /// Generate HMAC-SHA384 signature for Bitfinex authentication.
    /// Signature: HMAC-SHA384(api_secret, payload_base64)
    fn sign(&self, payload: &str) -> String {
        use hmac::{Hmac, Mac};
        use sha2::Sha384;

        type HmacSha384 = Hmac<Sha384>;

        let mut mac = HmacSha384::new_from_slice(self.api_secret.as_bytes())
            .expect("HMAC can take key of any size");
        mac.update(payload.as_bytes());

        let result = mac.finalize();
        hex::encode(result.into_bytes())
    }

    /// Create authentication headers for REST API.
    fn auth_headers(&self, path: &str, body: &str) -> Vec<(&'static str, String)> {
        let nonce = timestamp_ms().to_string();
        let signature_payload = format!("/api/v2{}{}{}", path, nonce, body);
        let signature = self.sign(&signature_payload);

        vec![
            ("bfx-nonce", nonce),
            ("bfx-apikey", self.api_key.clone()),
            ("bfx-signature", signature),
        ]
    }

    /// Make authenticated POST request.
    async fn auth_post<T: serde::de::DeserializeOwned>(
        &self,
        path: &str,
        body: serde_json::Value,
    ) -> Result<T> {
        let body_str = serde_json::to_string(&body)?;
        let headers = self.auth_headers(path, &body_str);

        let mut request = self
            .client
            .post(format!("{}{}", self.auth_url(), path))
            .header("Content-Type", "application/json")
            .body(body_str);

        for (key, value) in headers {
            request = request.header(key, value);
        }

        let response = request.send().await?;
        let status = response.status();
        let text = response.text().await?;

        if !status.is_success() {
            anyhow::bail!("Bitfinex API error: {} - {}", status, text);
        }

        serde_json::from_str(&text).context("Failed to parse Bitfinex response")
    }

    /// Create WebSocket authentication payload.
    fn ws_auth_payload(&self) -> serde_json::Value {
        let nonce = (timestamp_ms() * 1000).to_string(); // microseconds
        let auth_payload = format!("AUTH{}", nonce);
        let signature = self.sign(&auth_payload);

        serde_json::json!({
            "event": "auth",
            "apiKey": self.api_key,
            "authSig": signature,
            "authNonce": nonce,
            "authPayload": auth_payload,
            "filter": ["trading", "wallet"]
        })
    }

    /// Create and authenticate WebSocket connection for trading.
    async fn create_ws_private_connection(
        &self,
    ) -> Result<
        tokio_tungstenite::WebSocketStream<
            tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>,
        >,
    > {
        let ws_url = self.auth_ws_url();
        let (mut ws_stream, _) = connect_async(ws_url)
            .await
            .context("Failed to connect to Bitfinex private WebSocket")?;

        // Wait for initial info message
        let timeout = tokio::time::Duration::from_secs(5);
        tokio::select! {
            msg = ws_stream.next() => {
                match msg {
                    Some(Ok(Message::Text(text))) => {
                        debug!("WS initial message: {}", text);
                        // Should receive {"event":"info","version":2,...}
                    }
                    Some(Ok(_)) => {}
                    Some(Err(e)) => anyhow::bail!("WebSocket error: {}", e),
                    None => anyhow::bail!("WebSocket closed"),
                }
            }
            _ = tokio::time::sleep(timeout) => {
                anyhow::bail!("WebSocket info timeout");
            }
        }

        // Authenticate
        let auth_msg = self.ws_auth_payload();
        let auth_json = serde_json::to_string(&auth_msg)?;
        debug!("Sending WS auth: {}", auth_json);

        ws_stream.send(Message::Text(auth_json)).await?;

        // Wait for auth response
        tokio::select! {
            msg = ws_stream.next() => {
                match msg {
                    Some(Ok(Message::Text(text))) => {
                        debug!("Auth response: {}", text);
                        let resp: serde_json::Value = serde_json::from_str(&text)?;

                        if resp.get("event").and_then(|v| v.as_str()) == Some("auth") {
                            if resp.get("status").and_then(|v| v.as_str()) == Some("OK") {
                                info!("Bitfinex WebSocket authenticated");
                            } else {
                                let msg = resp.get("msg").and_then(|v| v.as_str()).unwrap_or("Unknown");
                                anyhow::bail!("WebSocket authentication failed: {}", msg);
                            }
                        }
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

    /// Place order via WebSocket.
    async fn place_order_ws(
        &self,
        symbol: &str,
        side: OrderSide,
        order_type: OrderType,
        quantity: f64,
        price: Option<f64>,
        _reduce_only: bool,
    ) -> Result<OrderResult> {
        // Create WS connection and authenticate
        let mut ws_stream = self.create_ws_private_connection().await?;

        // Prepare order request
        // Bitfinex order format: [0, "on", null, {...}]
        let cid = timestamp_ms();
        let amount = match side {
            OrderSide::Buy => quantity,
            OrderSide::Sell => -quantity,
        };

        let order_type_str = match order_type {
            OrderType::Market => "EXCHANGE MARKET",
            OrderType::Limit => "EXCHANGE LIMIT",
            _ => "EXCHANGE MARKET",
        };

        let mut order_data = serde_json::json!({
            "cid": cid,
            "type": order_type_str,
            "symbol": symbol,
            "amount": format!("{:.8}", amount),
        });

        // Add price for limit orders
        if let (OrderType::Limit, Some(p)) = (order_type, price) {
            order_data["price"] = serde_json::json!(format!("{:.8}", p));
        }

        let order_msg = serde_json::json!([0, "on", null, order_data]);
        let order_json = serde_json::to_string(&order_msg)?;
        debug!("Sending WS order: {}", order_json);

        // Send order
        ws_stream.send(Message::Text(order_json)).await?;

        // Wait for response
        let timeout = tokio::time::Duration::from_secs(10);
        let order_result;

        loop {
            tokio::select! {
                msg = ws_stream.next() => {
                    match msg {
                        Some(Ok(Message::Text(text))) => {
                            debug!("Order response: {}", text);

                            // Parse response - could be notification or order update
                            if let Ok(resp) = serde_json::from_str::<serde_json::Value>(&text) {
                                // Check for notification [0, "n", [...]]
                                if let Some(arr) = resp.as_array() {
                                    if arr.len() >= 3 {
                                        if let Some(msg_type) = arr.get(1).and_then(|v| v.as_str()) {
                                            match msg_type {
                                                "n" => {
                                                    // Notification: [0, "n", [timestamp, type, msg_id, null, [data], code, status, text]]
                                                    if let Some(n_data) = arr.get(2).and_then(|v| v.as_array()) {
                                                        let status = n_data.get(6).and_then(|v| v.as_str()).unwrap_or("");
                                                        let text = n_data.get(7).and_then(|v| v.as_str()).unwrap_or("");

                                                        if status == "SUCCESS" {
                                                            // Get order ID from notification data
                                                            if let Some(order_arr) = n_data.get(4).and_then(|v| v.as_array()) {
                                                                let order_id = order_arr.first()
                                                                    .and_then(|v| v.as_i64())
                                                                    .map(|id| id.to_string())
                                                                    .unwrap_or_default();

                                                                info!("Bitfinex WS order placed: {}", order_id);
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
                                                                break;
                                                            }
                                                        } else if status == "ERROR" {
                                                            order_result = Some(OrderResult::failure(
                                                                symbol.to_string(),
                                                                side,
                                                                text.to_string(),
                                                            ));
                                                            break;
                                                        }
                                                    }
                                                }
                                                "on" | "ou" | "oc" => {
                                                    // Order new/update/cancel - already have the order
                                                    continue;
                                                }
                                                _ => continue,
                                            }
                                        }
                                    }
                                }
                            }
                        }
                        Some(Ok(Message::Ping(data))) => {
                            let _ = ws_stream.send(Message::Pong(data)).await;
                        }
                        Some(Ok(_)) => continue,
                        Some(Err(e)) => {
                            order_result = Some(OrderResult::failure(
                                symbol.to_string(),
                                side,
                                format!("WebSocket error: {}", e),
                            ));
                            break;
                        }
                        None => {
                            order_result = Some(OrderResult::failure(
                                symbol.to_string(),
                                side,
                                "WebSocket closed".to_string(),
                            ));
                            break;
                        }
                    }
                }
                _ = tokio::time::sleep(timeout) => {
                    order_result = Some(OrderResult::failure(
                        symbol.to_string(),
                        side,
                        "Order timeout".to_string(),
                    ));
                    break;
                }
            }
        }

        // Close connection
        let _ = ws_stream.close(None).await;

        order_result.ok_or_else(|| anyhow::anyhow!("No order result received"))
    }

    /// Cancel order via WebSocket.
    async fn cancel_order_ws(&self, _symbol: &str, order_id: &str) -> Result<bool> {
        // Create WS connection and authenticate
        let mut ws_stream = self.create_ws_private_connection().await?;

        // Parse order_id to i64
        let order_id_num: i64 = order_id.parse().context("Invalid order ID")?;

        // Prepare cancel request: [0, "oc", null, {"id": order_id}]
        let cancel_msg = serde_json::json!([0, "oc", null, {"id": order_id_num}]);
        let cancel_json = serde_json::to_string(&cancel_msg)?;
        debug!("Sending WS cancel: {}", cancel_json);

        // Send cancel request
        ws_stream.send(Message::Text(cancel_json)).await?;

        // Wait for response
        let timeout = tokio::time::Duration::from_secs(10);
        let mut success = false;

        loop {
            tokio::select! {
                msg = ws_stream.next() => {
                    match msg {
                        Some(Ok(Message::Text(text))) => {
                            debug!("Cancel response: {}", text);

                            if let Ok(resp) = serde_json::from_str::<serde_json::Value>(&text) {
                                if let Some(arr) = resp.as_array() {
                                    if arr.len() >= 3 {
                                        if let Some(msg_type) = arr.get(1).and_then(|v| v.as_str()) {
                                            if msg_type == "n" {
                                                if let Some(n_data) = arr.get(2).and_then(|v| v.as_array()) {
                                                    let status = n_data.get(6).and_then(|v| v.as_str()).unwrap_or("");
                                                    if status == "SUCCESS" {
                                                        info!("Bitfinex WS order cancelled: {}", order_id);
                                                        success = true;
                                                        break;
                                                    } else if status == "ERROR" {
                                                        let text = n_data.get(7).and_then(|v| v.as_str()).unwrap_or("Unknown");
                                                        error!("Failed to cancel order {}: {}", order_id, text);
                                                        break;
                                                    }
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
                        Some(Ok(_)) => continue,
                        Some(Err(e)) => {
                            error!("WebSocket error during cancel: {}", e);
                            break;
                        }
                        None => {
                            error!("WebSocket closed during cancel");
                            break;
                        }
                    }
                }
                _ = tokio::time::sleep(timeout) => {
                    error!("Cancel order timeout for {}", order_id);
                    break;
                }
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

/// Parse a status string from Bitfinex into OrderStatus.
fn parse_order_status_str(s: &str) -> OrderStatus {
    match s.to_uppercase().as_str() {
        "ACTIVE" | "PARTIALLY FILLED" => OrderStatus::PartiallyFilled,
        "EXECUTED" => OrderStatus::Filled,
        "CANCELED" | "CANCELLED" => OrderStatus::Cancelled,
        "POSTONLY CANCELED" => OrderStatus::Cancelled,
        "INSUFFICIENT MARGIN" | "RSN_DUST" | "RSN_PAUSE" => OrderStatus::Rejected,
        _ => {
            if s.starts_with("EXECUTED") {
                OrderStatus::Filled
            } else if s.starts_with("CANCELED") || s.starts_with("CANCELLED") {
                OrderStatus::Cancelled
            } else {
                warn!("Unknown order status string: '{}'", s);
                OrderStatus::New
            }
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
impl ExchangeTrader for BitfinexTrader {
    fn exchange_name(&self) -> &str {
        "bitfinex"
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
        let response = self
            .client
            .get(format!("{}/platform/status", self.public_url()))
            .send()
            .await?;

        if response.status().is_success() {
            let data: serde_json::Value = response.json().await?;
            // Response: [1] means operative, [0] means maintenance
            if let Some(arr) = data.as_array() {
                if let Some(status) = arr.first().and_then(|v| v.as_i64()) {
                    return Ok(status == 1);
                }
            }
        }
        Ok(false)
    }

    async fn get_server_time(&self) -> Result<i64> {
        // Bitfinex doesn't have a dedicated time endpoint, use local time
        Ok(timestamp_ms())
    }

    async fn get_balance(&self) -> Result<AccountBalance> {
        // Get wallet balances
        let wallets: Vec<Vec<serde_json::Value>> = self
            .auth_post("/auth/r/wallets", serde_json::json!({}))
            .await?;

        let mut total_balance = 0.0;
        let mut available_balance = 0.0;
        let mut balances = Vec::new();

        // Wallet format: [WALLET_TYPE, CURRENCY, BALANCE, UNSETTLED_INTEREST, AVAILABLE_BALANCE, ...]
        for wallet in wallets {
            if wallet.len() >= 5 {
                let wallet_type = wallet.first().and_then(|v| v.as_str()).unwrap_or("");
                let currency = wallet.get(1).and_then(|v| v.as_str()).unwrap_or("");
                let balance = wallet.get(2).and_then(|v| v.as_f64()).unwrap_or(0.0);
                let available = wallet.get(4).and_then(|v| v.as_f64()).unwrap_or(balance);

                // For trading, use "exchange" wallet
                if wallet_type == "exchange" || wallet_type == "margin" {
                    if currency == "USD" || currency == "UST" {
                        total_balance += balance;
                        available_balance += available;
                    }

                    balances.push(Balance {
                        asset: currency.to_string(),
                        free: available,
                        locked: balance - available,
                    });
                }
            }
        }

        Ok(AccountBalance {
            total_balance,
            available_balance,
            currency: "USD".to_string(),
            margin_balance: Some(total_balance),
            used_margin: Some(total_balance - available_balance),
            unrealized_pnl: None,
            realized_pnl: None,
            balances,
        })
    }

    async fn get_current_price(&self, symbol: &str) -> Result<f64> {
        let formatted_symbol = self.format_symbol(symbol);
        let response: serde_json::Value = self
            .client
            .get(format!("{}/ticker/{}", self.public_url(), formatted_symbol))
            .send()
            .await?
            .json()
            .await?;

        // Ticker format: [BID, BID_SIZE, ASK, ASK_SIZE, DAILY_CHANGE, DAILY_CHANGE_RELATIVE,
        //                 LAST_PRICE, VOLUME, HIGH, LOW]
        if let Some(arr) = response.as_array() {
            if let Some(last_price) = arr.get(6).and_then(|v| v.as_f64()) {
                return Ok(last_price);
            }
        }

        anyhow::bail!("Failed to get price for {}", formatted_symbol)
    }

    async fn get_ticker(&self, symbol: &str) -> Result<TickerInfo> {
        let formatted_symbol = self.format_symbol(symbol);
        let response: serde_json::Value = self
            .client
            .get(format!("{}/ticker/{}", self.public_url(), formatted_symbol))
            .send()
            .await?
            .json()
            .await?;

        // Ticker format: [BID, BID_SIZE, ASK, ASK_SIZE, DAILY_CHANGE, DAILY_CHANGE_RELATIVE,
        //                 LAST_PRICE, VOLUME, HIGH, LOW]
        if let Some(arr) = response.as_array() {
            return Ok(TickerInfo {
                symbol: formatted_symbol,
                last_price: arr.get(6).and_then(|v| v.as_f64()).unwrap_or(0.0),
                bid_price: arr.first().and_then(|v| v.as_f64()),
                ask_price: arr.get(2).and_then(|v| v.as_f64()),
                volume_24h: arr.get(7).and_then(|v| v.as_f64()),
                turnover_24h: None,
                high_24h: arr.get(8).and_then(|v| v.as_f64()),
                low_24h: arr.get(9).and_then(|v| v.as_f64()),
                change_24h: arr.get(5).and_then(|v| v.as_f64()).map(|v| v * 100.0), // Convert to percentage
                open_interest: None,
                open_interest_value: None,
                funding_rate: None,
                next_funding_time: None,
                mark_price: None,
                index_price: None,
                timestamp: timestamp_ms(),
            });
        }

        anyhow::bail!("Failed to get ticker for {}", formatted_symbol)
    }

    async fn get_orderbook(&self, symbol: &str, depth: u32) -> Result<Orderbook> {
        let formatted_symbol = self.format_symbol(symbol);
        let precision = "P0"; // Highest precision
        let len = depth.min(100); // Max 100 levels

        let response: serde_json::Value = self
            .client
            .get(format!(
                "{}/book/{}/{}?len={}",
                self.public_url(),
                formatted_symbol,
                precision,
                len
            ))
            .send()
            .await?
            .json()
            .await?;

        let mut bids = Vec::new();
        let mut asks = Vec::new();

        // Book format: [[PRICE, COUNT, AMOUNT], ...]
        // If AMOUNT > 0, it's a bid; if AMOUNT < 0, it's an ask
        if let Some(arr) = response.as_array() {
            for level in arr {
                if let Some(level_arr) = level.as_array() {
                    if level_arr.len() >= 3 {
                        let price = level_arr.first().and_then(|v| v.as_f64()).unwrap_or(0.0);
                        let amount = level_arr.get(2).and_then(|v| v.as_f64()).unwrap_or(0.0);

                        if amount > 0.0 {
                            bids.push(OrderbookLevel {
                                price,
                                quantity: amount,
                            });
                        } else {
                            asks.push(OrderbookLevel {
                                price,
                                quantity: amount.abs(),
                            });
                        }
                    }
                }
            }
        }

        // Sort bids descending by price
        bids.sort_by(|a, b| {
            b.price
                .partial_cmp(&a.price)
                .unwrap_or(std::cmp::Ordering::Equal)
        });
        // Sort asks ascending by price
        asks.sort_by(|a, b| {
            a.price
                .partial_cmp(&b.price)
                .unwrap_or(std::cmp::Ordering::Equal)
        });

        Ok(Orderbook {
            symbol: formatted_symbol,
            bids,
            asks,
            timestamp: timestamp_ms(),
        })
    }

    async fn set_leverage(&self, _symbol: Option<&str>, _leverage: u32) -> Result<bool> {
        // Bitfinex doesn't have explicit leverage setting for spot
        // For margin trading, leverage is implicit
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

        // Use WebSocket for order placement
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

        // Use WebSocket for order placement
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
        // Use WebSocket for order cancellation
        self.cancel_order_ws(symbol, order_id).await
    }

    async fn get_order_status(&self, _symbol: &str, order_id: &str) -> Result<OrderInfo> {
        // Get order history and find the order
        let order_id_num: i64 = order_id.parse().context("Invalid order ID")?;

        let orders: Vec<Vec<serde_json::Value>> = self
            .auth_post(
                "/auth/r/orders/hist",
                serde_json::json!({
                    "id": [order_id_num]
                }),
            )
            .await?;

        if let Some(order) = orders.first() {
            // Order format: [ID, GID, CID, SYMBOL, MTS_CREATE, MTS_UPDATE, AMOUNT, AMOUNT_ORIG,
            //                TYPE, TYPE_PREV, MTS_TIF, _, FLAGS, STATUS, _, _, PRICE, PRICE_AVG,
            //                PRICE_TRAILING, PRICE_AUX_LIMIT, _, _, _, NOTIFY, HIDDEN, PLACED_ID, ...]
            let symbol = order
                .get(3)
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();
            let amount = order.get(6).and_then(|v| v.as_f64()).unwrap_or(0.0);
            let amount_orig = order.get(7).and_then(|v| v.as_f64()).unwrap_or(0.0);
            let status_str = order.get(13).and_then(|v| v.as_str()).unwrap_or("");
            let price = order.get(16).and_then(|v| v.as_f64()).unwrap_or(0.0);
            let avg_price = order.get(17).and_then(|v| v.as_f64());
            let timestamp = order.get(4).and_then(|v| v.as_i64()).unwrap_or(0);

            let (side, quantity, filled_quantity) = if amount_orig > 0.0 {
                (
                    OrderSide::Buy,
                    amount_orig.abs(),
                    (amount_orig - amount).abs(),
                )
            } else {
                (
                    OrderSide::Sell,
                    amount_orig.abs(),
                    (amount_orig - amount).abs(),
                )
            };

            return Ok(OrderInfo {
                order_id: order_id.to_string(),
                client_order_id: order.get(2).and_then(|v| v.as_i64()).map(|v| v.to_string()),
                symbol,
                status: parse_order_status_str(status_str),
                side,
                order_type: OrderType::Limit, // Default
                price,
                quantity,
                filled_quantity,
                remaining_quantity: amount.abs(),
                avg_price,
                timestamp,
                time_in_force: TimeInForce::GTC,
                reduce_only: false,
            });
        }

        anyhow::bail!("Order not found: {}", order_id)
    }

    async fn close_position(&self, symbol: &str) -> Result<OrderResult> {
        // Get current position and close it with market order
        Ok(OrderResult::failure(
            symbol.to_string(),
            OrderSide::Sell,
            "Not implemented - use place_market_order with reduce_only".to_string(),
        ))
    }

    fn format_symbol(&self, base: &str) -> String {
        // Already in Bitfinex format: a `t`/`f` prefix followed by an
        // uppercase pair, e.g. tBTCUSD or fUSD. Checked before uppercasing,
        // otherwise the prefix is indistinguishable from a ticker starting
        // with T or F (TRX, FTM).
        if let Some(rest) = base.strip_prefix('t').or_else(|| base.strip_prefix('f')) {
            if rest.len() >= 3 && rest.chars().all(|c| c.is_ascii_uppercase() || c == ':') {
                return base.to_string();
            }
        }

        let s = base.to_uppercase();

        // Handle BTC/USDT format
        if s.contains('/') {
            let parts: Vec<&str> = s.split('/').collect();
            if parts.len() == 2 {
                // Bitfinex uses tBTCUSD format
                return format!("t{}{}", parts[0], parts[1].replace("USDT", "UST"));
            }
        }

        // Handle BTCUSDT format
        if s.ends_with("USDT") {
            let base_asset = s.trim_end_matches("USDT");
            return format!("t{}UST", base_asset);
        }

        // Handle BTCUSD format
        if s.ends_with("USD") {
            return format!("t{}", s);
        }

        // Default: add t prefix and USD suffix
        format!("t{}USD", s)
    }

    async fn test_ws_orderbook(
        &self,
        symbol: &str,
        depth: u32,
        timeout_ms: u64,
    ) -> Result<WsTestResult> {
        let formatted_symbol = self.format_symbol(symbol);
        let start = std::time::Instant::now();

        let (mut ws_stream, _) = connect_async(self.ws_url())
            .await
            .context("Failed to connect to Bitfinex WebSocket")?;

        // Wait for info message
        let _ = ws_stream.next().await;

        // Subscribe to orderbook
        let subscribe_msg = serde_json::json!({
            "event": "subscribe",
            "channel": "book",
            "symbol": formatted_symbol,
            "prec": "P0",
            "len": depth.min(100).to_string()
        });

        ws_stream
            .send(Message::Text(subscribe_msg.to_string()))
            .await?;

        let timeout = tokio::time::Duration::from_millis(timeout_ms);
        let mut messages_received = 0u32;
        let mut data_sample = None;

        loop {
            tokio::select! {
                msg = ws_stream.next() => {
                    match msg {
                        Some(Ok(Message::Text(text))) => {
                            messages_received += 1;
                            if let Ok(data) = serde_json::from_str::<serde_json::Value>(&text) {
                                // Check if it's data message (array with channel_id)
                                if data.is_array() && data.as_array().map(|a| a.len() > 1).unwrap_or(false) {
                                    if data_sample.is_none() {
                                        data_sample = Some(data);
                                    }
                                    break;
                                }
                            }
                        }
                        Some(Ok(_)) => continue,
                        Some(Err(e)) => {
                            let _ = ws_stream.close(None).await;
                            anyhow::bail!("WebSocket error: {}", e);
                        }
                        None => break,
                    }
                }
                _ = tokio::time::sleep(timeout) => break,
            }
        }

        let _ = ws_stream.close(None).await;

        Ok(WsTestResult {
            connected: true,
            latency_ms: start.elapsed().as_millis() as u64,
            messages_received,
            subscription_type: "orderbook".to_string(),
            data_sample,
        })
    }

    async fn test_ws_trades(&self, symbol: &str, timeout_ms: u64) -> Result<WsTestResult> {
        let formatted_symbol = self.format_symbol(symbol);
        let start = std::time::Instant::now();

        let (mut ws_stream, _) = connect_async(self.ws_url())
            .await
            .context("Failed to connect to Bitfinex WebSocket")?;

        // Wait for info message
        let _ = ws_stream.next().await;

        // Subscribe to trades
        let subscribe_msg = serde_json::json!({
            "event": "subscribe",
            "channel": "trades",
            "symbol": formatted_symbol
        });

        ws_stream
            .send(Message::Text(subscribe_msg.to_string()))
            .await?;

        let timeout = tokio::time::Duration::from_millis(timeout_ms);
        let mut messages_received = 0u32;
        let mut data_sample = None;

        loop {
            tokio::select! {
                msg = ws_stream.next() => {
                    match msg {
                        Some(Ok(Message::Text(text))) => {
                            messages_received += 1;
                            if let Ok(data) = serde_json::from_str::<serde_json::Value>(&text) {
                                // Check if it's data message
                                if data.is_array() {
                                    if let Some(arr) = data.as_array() {
                                        if arr.len() > 1 && !arr.get(1).map(|v| v.as_str() == Some("hb")).unwrap_or(false) {
                                            if data_sample.is_none() {
                                                data_sample = Some(data);
                                            }
                                            break;
                                        }
                                    }
                                }
                            }
                        }
                        Some(Ok(_)) => continue,
                        Some(Err(e)) => {
                            let _ = ws_stream.close(None).await;
                            anyhow::bail!("WebSocket error: {}", e);
                        }
                        None => break,
                    }
                }
                _ = tokio::time::sleep(timeout) => break,
            }
        }

        let _ = ws_stream.close(None).await;

        Ok(WsTestResult {
            connected: true,
            latency_ms: start.elapsed().as_millis() as u64,
            messages_received,
            subscription_type: "trades".to_string(),
            data_sample,
        })
    }

    async fn test_ws_ticker(&self, symbol: &str, timeout_ms: u64) -> Result<WsTestResult> {
        let formatted_symbol = self.format_symbol(symbol);
        let start = std::time::Instant::now();

        let (mut ws_stream, _) = connect_async(self.ws_url())
            .await
            .context("Failed to connect to Bitfinex WebSocket")?;

        // Wait for info message
        let _ = ws_stream.next().await;

        // Subscribe to ticker
        let subscribe_msg = serde_json::json!({
            "event": "subscribe",
            "channel": "ticker",
            "symbol": formatted_symbol
        });

        ws_stream
            .send(Message::Text(subscribe_msg.to_string()))
            .await?;

        let timeout = tokio::time::Duration::from_millis(timeout_ms);
        let mut messages_received = 0u32;
        let mut data_sample = None;

        loop {
            tokio::select! {
                msg = ws_stream.next() => {
                    match msg {
                        Some(Ok(Message::Text(text))) => {
                            messages_received += 1;
                            if let Ok(data) = serde_json::from_str::<serde_json::Value>(&text) {
                                // Check if it's data message
                                if data.is_array() {
                                    if let Some(arr) = data.as_array() {
                                        if arr.len() > 1 && !arr.get(1).map(|v| v.as_str() == Some("hb")).unwrap_or(false) {
                                            if data_sample.is_none() {
                                                data_sample = Some(data);
                                            }
                                            break;
                                        }
                                    }
                                }
                            }
                        }
                        Some(Ok(_)) => continue,
                        Some(Err(e)) => {
                            let _ = ws_stream.close(None).await;
                            anyhow::bail!("WebSocket error: {}", e);
                        }
                        None => break,
                    }
                }
                _ = tokio::time::sleep(timeout) => break,
            }
        }

        let _ = ws_stream.close(None).await;

        Ok(WsTestResult {
            connected: true,
            latency_ms: start.elapsed().as_millis() as u64,
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
        let exchange_name = "bitfinex".to_string();

        let handle = tokio::spawn(async move {
            let mut reconnect_delay = INITIAL_RECONNECT_DELAY_SECS;

            loop {
                info!(
                    "[{}] Connecting to private WS: {}",
                    exchange_name, AUTH_WS_URL
                );

                match connect_async(AUTH_WS_URL).await {
                    Ok((mut ws_stream, _)) => {
                        // Wait for info message
                        if let Some(Ok(Message::Text(_))) = ws_stream.next().await {
                            debug!("[{}] Received info message", exchange_name);
                        }

                        // Authenticate
                        let nonce = (timestamp_ms() * 1000).to_string();
                        let auth_payload = format!("AUTH{}", nonce);

                        let signature = {
                            use hmac::{Hmac, Mac};
                            use sha2::Sha384;
                            type HmacSha384 = Hmac<Sha384>;

                            let mut mac = HmacSha384::new_from_slice(api_secret.as_bytes())
                                .expect("HMAC can take key of any size");
                            mac.update(auth_payload.as_bytes());
                            hex::encode(mac.finalize().into_bytes())
                        };

                        let auth_msg = serde_json::json!({
                            "event": "auth",
                            "apiKey": api_key,
                            "authSig": signature,
                            "authNonce": nonce,
                            "authPayload": auth_payload,
                            "filter": ["trading"]
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
                                if resp.get("event").and_then(|v| v.as_str()) == Some("auth") {
                                    if resp.get("status").and_then(|v| v.as_str()) == Some("OK") {
                                        info!("[{}] Private WS authenticated", exchange_name);
                                        authenticated = true;
                                    } else {
                                        let msg = resp
                                            .get("msg")
                                            .and_then(|v| v.as_str())
                                            .unwrap_or("Unknown");
                                        error!("[{}] Auth failed: {}", exchange_name, msg);
                                    }
                                }
                            }
                        }

                        if !authenticated {
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
                                            if let Ok(data) = serde_json::from_str::<serde_json::Value>(&text) {
                                                // Check for order updates: [0, "on"|"ou"|"oc", [...]]
                                                if let Some(arr) = data.as_array() {
                                                    if arr.len() >= 3 {
                                                        if let Some(msg_type) = arr.get(1).and_then(|v| v.as_str()) {
                                                            match msg_type {
                                                                "on" | "ou" | "oc" => {
                                                                    // Order update
                                                                    if let Some(order_arr) = arr.get(2).and_then(|v| v.as_array()) {
                                                                        // Order format: [ID, GID, CID, SYMBOL, ...]
                                                                        let order_id = order_arr.first()
                                                                            .and_then(|v| v.as_i64())
                                                                            .map(|id| id.to_string())
                                                                            .unwrap_or_default();
                                                                        let symbol = order_arr.get(3)
                                                                            .and_then(|v| v.as_str())
                                                                            .unwrap_or("")
                                                                            .to_string();
                                                                        let amount = order_arr.get(6)
                                                                            .and_then(|v| v.as_f64())
                                                                            .unwrap_or(0.0);
                                                                        let amount_orig = order_arr.get(7)
                                                                            .and_then(|v| v.as_f64())
                                                                            .unwrap_or(0.0);
                                                                        let order_type_str = order_arr.get(8)
                                                                            .and_then(|v| v.as_str())
                                                                            .unwrap_or("");
                                                                        let status_str = order_arr.get(13)
                                                                            .and_then(|v| v.as_str())
                                                                            .unwrap_or("");
                                                                        let price = order_arr.get(16)
                                                                            .and_then(|v| v.as_f64())
                                                                            .unwrap_or(0.0);
                                                                        let avg_price = order_arr.get(17)
                                                                            .and_then(|v| v.as_f64());
                                                                        let timestamp = order_arr.get(5)
                                                                            .and_then(|v| v.as_i64())
                                                                            .unwrap_or(0);

                                                                        let (side, quantity, filled_quantity) = if amount_orig > 0.0 {
                                                                            (OrderSide::Buy, amount_orig.abs(), (amount_orig - amount).abs())
                                                                        } else {
                                                                            (OrderSide::Sell, amount_orig.abs(), (amount_orig - amount).abs())
                                                                        };

                                                                        let order_type = if order_type_str.contains("MARKET") {
                                                                            OrderType::Market
                                                                        } else {
                                                                            OrderType::Limit
                                                                        };

                                                                        let status = match msg_type {
                                                                            "on" => OrderStatus::New,
                                                                            "oc" => parse_order_status_str(status_str),
                                                                            "ou" => {
                                                                                if filled_quantity > 0.0 && filled_quantity < quantity {
                                                                                    OrderStatus::PartiallyFilled
                                                                                } else if filled_quantity >= quantity {
                                                                                    OrderStatus::Filled
                                                                                } else {
                                                                                    OrderStatus::New
                                                                                }
                                                                            }
                                                                            _ => OrderStatus::New,
                                                                        };

                                                                        let event = OrderUpdateEvent {
                                                                            exchange: exchange_name.clone(),
                                                                            order_id,
                                                                            client_order_id: order_arr.get(2)
                                                                                .and_then(|v| v.as_i64())
                                                                                .map(|id| id.to_string()),
                                                                            symbol,
                                                                            status,
                                                                            side,
                                                                            order_type,
                                                                            price,
                                                                            quantity,
                                                                            filled_quantity,
                                                                            avg_fill_price: avg_price,
                                                                            fee: None,
                                                                            fee_currency: None,
                                                                            exchange_timestamp: timestamp,
                                                                            received_at: timestamp_ms(),
                                                                            raw: Some(data.clone()),
                                                                        };

                                                                        log_order_update(&event);
                                                                        let _ = sender.send(event);
                                                                    }
                                                                }
                                                                "hb" => {
                                                                    // Heartbeat - ignore
                                                                    debug!("[{}] Heartbeat received", exchange_name);
                                                                }
                                                                _ => {
                                                                    debug!("[{}] Unknown message type: {}", exchange_name, msg_type);
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
                                    // Bitfinex expects ping as JSON: {"event": "ping"}
                                    let ping_msg = serde_json::json!({"event": "ping"});
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
        let trader = BitfinexTrader::new("", "", false, MarketType::Spot);

        // Test BTC -> tBTCUSD
        assert_eq!(trader.format_symbol("BTC"), "tBTCUSD");

        // Test BTCUSDT -> tBTCUST
        assert_eq!(trader.format_symbol("BTCUSDT"), "tBTCUST");

        // Test BTCUSD -> tBTCUSD
        assert_eq!(trader.format_symbol("BTCUSD"), "tBTCUSD");

        // Test BTC/USDT -> tBTCUST
        assert_eq!(trader.format_symbol("BTC/USDT"), "tBTCUST");

        // Test BTC/USD -> tBTCUSD
        assert_eq!(trader.format_symbol("BTC/USD"), "tBTCUSD");

        // Test already formatted
        assert_eq!(trader.format_symbol("tBTCUSD"), "tBTCUSD");
        assert_eq!(trader.format_symbol("fUSD"), "fUSD");

        // Tickers starting with t/f are not Bitfinex-prefixed symbols
        assert_eq!(trader.format_symbol("trx"), "tTRXUSD");
        assert_eq!(trader.format_symbol("ftmusdt"), "tFTMUST");

        // Test lowercase
        assert_eq!(trader.format_symbol("eth"), "tETHUSD");
        assert_eq!(trader.format_symbol("ethusdt"), "tETHUST");
    }

    #[test]
    fn test_parse_order_status() {
        assert_eq!(
            parse_order_status_str("ACTIVE"),
            OrderStatus::PartiallyFilled
        );
        assert_eq!(parse_order_status_str("EXECUTED"), OrderStatus::Filled);
        assert_eq!(
            parse_order_status_str("EXECUTED @ 50000.0"),
            OrderStatus::Filled
        );
        assert_eq!(parse_order_status_str("CANCELED"), OrderStatus::Cancelled);
        assert_eq!(parse_order_status_str("CANCELLED"), OrderStatus::Cancelled);
        assert_eq!(
            parse_order_status_str("POSTONLY CANCELED"),
            OrderStatus::Cancelled
        );
        assert_eq!(
            parse_order_status_str("INSUFFICIENT MARGIN"),
            OrderStatus::Rejected
        );
    }

    #[test]
    fn test_next_reconnect_delay() {
        assert_eq!(next_reconnect_delay(1), 2);
        assert_eq!(next_reconnect_delay(2), 4);
        assert_eq!(next_reconnect_delay(16), 30); // Capped at MAX_RECONNECT_DELAY_SECS
        assert_eq!(next_reconnect_delay(30), 30); // Already at max
    }
}
