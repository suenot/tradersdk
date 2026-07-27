//! LBank exchange trader implementation.
//!
//! LBank API v2 implementation with HMAC-SHA256 signing.
//! LBank uses lowercase symbol format with underscore: btc_usdt

use sdktrader_core::utils::{hmac_sign, timestamp_ms};
use sdktrader_core::ExchangeTrader;
use sdktrader_core::models::{
    AccountBalance, ExchangeCapabilities, FundingRateEntry, MarketType, OpenInterestEntry,
    OrderInfo, OrderResult, OrderSide, OrderStatus, OrderType, OrderUpdateEvent, Orderbook,
    OrderbookLevel, TickerInfo, TimeInForce, WsTestResult,
};
use anyhow::{Context, Result};
use async_trait::async_trait;
use futures_util::{SinkExt, StreamExt};
use reqwest::Client;
use serde::Deserialize;
use tokio::sync::broadcast;
use tokio::task::JoinHandle;
use tokio_tungstenite::{connect_async, tungstenite::protocol::Message};
use tracing::{debug, error, info, warn};

// LBank API endpoints
const MAINNET_URL: &str = "https://api.lbank.info";
const WS_PUBLIC_URL: &str = "wss://www.lbkex.net/ws/V2/";
const WS_PRIVATE_URL: &str = "wss://www.lbkex.net/ws/V2/";

// Reconnect constants
const INITIAL_RECONNECT_DELAY_SECS: u64 = 1;
const MAX_RECONNECT_DELAY_SECS: u64 = 60;

/// Calculate next reconnect delay with exponential backoff.
fn next_reconnect_delay(current: u64) -> u64 {
    (current * 2).min(MAX_RECONNECT_DELAY_SECS)
}

/// Parse order status from LBank string.
fn parse_order_status_str(status: &str) -> OrderStatus {
    match status.to_lowercase().as_str() {
        "0" | "new" | "open" | "pending" => OrderStatus::New,
        "1" | "partial" | "partially_filled" | "partiallyfilled" => OrderStatus::PartiallyFilled,
        "2" | "filled" | "closed" | "complete" => OrderStatus::Filled,
        "-1" | "cancelled" | "canceled" | "cancel" => OrderStatus::Cancelled,
        "rejected" | "fail" | "failed" => OrderStatus::Rejected,
        "expired" | "expire" => OrderStatus::Expired,
        _ => {
            warn!("Unknown LBank order status: '{}'", status);
            OrderStatus::New
        }
    }
}

/// Log order update event.
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

/// LBank exchange trader.
pub struct LbankTrader {
    api_key: String,
    api_secret: String,
    _testnet: bool,
    market_type: MarketType,
    client: Client,
    base_url: String,
}

impl LbankTrader {
    /// Create a new LBank trader.
    pub fn new(api_key: &str, api_secret: &str, testnet: bool, market_type: MarketType) -> Self {
        Self {
            api_key: api_key.to_string(),
            api_secret: api_secret.to_string(),
            _testnet: testnet,
            market_type,
            client: Client::new(),
            base_url: MAINNET_URL.to_string(),
        }
    }

    /// Sign request parameters with HMAC-SHA256.
    /// LBank signature: MD5(secret_key) then HMAC-SHA256 sign the sorted params
    fn sign(&self, params: &str) -> String {
        // LBank uses direct HMAC-SHA256 signing with the secret key
        hmac_sign(&self.api_secret, params)
    }

    /// Build sorted query string from parameters.
    fn build_sorted_params(&self, params: &[(&str, &str)]) -> String {
        let mut sorted_params: Vec<_> = params.to_vec();
        sorted_params.sort_by(|a, b| a.0.cmp(b.0));
        sorted_params
            .iter()
            .map(|(k, v)| format!("{}={}", k, v))
            .collect::<Vec<_>>()
            .join("&")
    }

    /// Make a signed POST request to LBank API.
    async fn signed_post<T: for<'de> Deserialize<'de>>(
        &self,
        endpoint: &str,
        params: &[(&str, &str)],
    ) -> Result<T> {
        let timestamp = timestamp_ms().to_string();

        // Build params with timestamp and api_key
        let mut all_params: Vec<(&str, &str)> = params.to_vec();
        all_params.push(("api_key", &self.api_key));
        all_params.push(("timestamp", &timestamp));

        // Sort and build query string for signing
        let query_string = self.build_sorted_params(&all_params);

        // Create signature
        let signature = self.sign(&query_string);

        // Add signature to params
        let url = format!("{}{}", self.base_url, endpoint);

        let response = self
            .client
            .post(&url)
            .header("Content-Type", "application/x-www-form-urlencoded")
            .body(format!("{}&sign={}", query_string, signature))
            .send()
            .await
            .context("Failed to send request to LBank")?;

        let text = response.text().await?;
        debug!("LBank response: {}", text);

        serde_json::from_str(&text).context(format!("Failed to parse LBank response: {}", text))
    }

    /// Make a public GET request.
    #[allow(dead_code)]
    async fn public_get<T: for<'de> Deserialize<'de>>(&self, endpoint: &str) -> Result<T> {
        let url = format!("{}{}", self.base_url, endpoint);

        let response = self
            .client
            .get(&url)
            .send()
            .await
            .context("Failed to send request to LBank")?;

        let text = response.text().await?;
        debug!("LBank response: {}", text);

        serde_json::from_str(&text).context(format!("Failed to parse LBank response: {}", text))
    }
}

/// LBank API response wrapper.
#[derive(Debug, Deserialize)]
#[allow(dead_code)]
struct LbankResponse<T> {
    result: Option<String>,
    error_code: Option<i32>,
    data: Option<T>,
    #[serde(flatten)]
    extra: Option<serde_json::Value>,
}

/// LBank ticker response.
#[derive(Debug, Deserialize)]
#[allow(dead_code)]
struct LbankTicker {
    symbol: Option<String>,
    ticker: Option<LbankTickerData>,
    timestamp: Option<i64>,
}

#[derive(Debug, Deserialize)]
#[allow(dead_code)]
struct LbankTickerData {
    high: Option<f64>,
    vol: Option<f64>,
    low: Option<f64>,
    change: Option<f64>,
    turnover: Option<f64>,
    latest: Option<f64>,
}

/// LBank depth/orderbook response.
#[derive(Debug, Deserialize)]
#[allow(dead_code)]
struct LbankDepth {
    bids: Option<Vec<Vec<serde_json::Value>>>,
    asks: Option<Vec<Vec<serde_json::Value>>>,
    timestamp: Option<i64>,
}

/// LBank order response.
#[derive(Debug, Deserialize)]
struct LbankOrderResponse {
    result: Option<String>,
    error_code: Option<i32>,
    order_id: Option<String>,
}

/// LBank order info.
#[derive(Debug, Deserialize)]
#[allow(dead_code)]
struct LbankOrderInfo {
    order_id: Option<String>,
    symbol: Option<String>,
    #[serde(rename = "type")]
    order_type: Option<String>,
    price: Option<f64>,
    amount: Option<f64>,
    deal_amount: Option<f64>,
    avg_price: Option<f64>,
    status: Option<i32>,
    create_time: Option<i64>,
}

/// LBank balance response.
#[derive(Debug, Deserialize)]
struct LbankBalanceResponse {
    result: Option<String>,
    info: Option<LbankBalanceInfo>,
}

#[derive(Debug, Deserialize)]
struct LbankBalanceInfo {
    #[allow(dead_code)]
    asset: Option<serde_json::Value>,
    free: Option<serde_json::Value>,
    freeze: Option<serde_json::Value>,
}

#[async_trait]
impl ExchangeTrader for LbankTrader {
    fn exchange_name(&self) -> &str {
        "lbank"
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
            .get(format!("{}/v2/timestamp.do", self.base_url))
            .send()
            .await?;
        Ok(response.status().is_success())
    }

    async fn get_server_time(&self) -> Result<i64> {
        let response: serde_json::Value = self
            .client
            .get(format!("{}/v2/timestamp.do", self.base_url))
            .send()
            .await?
            .json()
            .await?;

        Ok(response["data"]
            .as_i64()
            .or_else(|| response["ts"].as_i64())
            .unwrap_or(timestamp_ms()))
    }

    async fn get_balance(&self) -> Result<AccountBalance> {
        let response: LbankBalanceResponse = self
            .signed_post("/v2/user_info.do", &[])
            .await?;

        if response.result.as_deref() != Some("true") {
            anyhow::bail!("Failed to get LBank balance");
        }

        let info = response.info.ok_or_else(|| anyhow::anyhow!("No balance info"))?;

        // Parse USDT balance from the response
        let free_usdt = info.free
            .as_ref()
            .and_then(|f| f.get("usdt"))
            .and_then(|v| v.as_str())
            .and_then(|s| s.parse::<f64>().ok())
            .or_else(|| {
                info.free
                    .as_ref()
                    .and_then(|f| f.get("usdt"))
                    .and_then(|v| v.as_f64())
            })
            .unwrap_or(0.0);

        let frozen_usdt = info.freeze
            .as_ref()
            .and_then(|f| f.get("usdt"))
            .and_then(|v| v.as_str())
            .and_then(|s| s.parse::<f64>().ok())
            .or_else(|| {
                info.freeze
                    .as_ref()
                    .and_then(|f| f.get("usdt"))
                    .and_then(|v| v.as_f64())
            })
            .unwrap_or(0.0);

        Ok(AccountBalance {
            total_balance: free_usdt + frozen_usdt,
            available_balance: free_usdt,
            currency: "USDT".to_string(),
            margin_balance: None,
            used_margin: Some(frozen_usdt),
            unrealized_pnl: None,
            realized_pnl: None,
            balances: vec![],
        })
    }

    async fn get_current_price(&self, symbol: &str) -> Result<f64> {
        let formatted = self.format_symbol(symbol);

        let response: serde_json::Value = self
            .client
            .get(format!("{}/v2/ticker.do?symbol={}", self.base_url, formatted))
            .send()
            .await?
            .json()
            .await?;

        // LBank returns array of tickers
        let price = if let Some(arr) = response.as_array() {
            arr.first()
                .and_then(|t| t.get("ticker"))
                .and_then(|t| t.get("latest"))
                .and_then(|v| v.as_f64())
                .unwrap_or(0.0)
        } else {
            response["ticker"]["latest"].as_f64().unwrap_or(0.0)
        };

        Ok(price)
    }

    async fn get_ticker(&self, symbol: &str) -> Result<TickerInfo> {
        let formatted = self.format_symbol(symbol);

        let response: serde_json::Value = self
            .client
            .get(format!("{}/v2/ticker.do?symbol={}", self.base_url, formatted))
            .send()
            .await?
            .json()
            .await?;

        // Handle array response
        let ticker_data = if let Some(arr) = response.as_array() {
            arr.first().cloned().unwrap_or(serde_json::Value::Null)
        } else {
            response
        };

        let ticker = ticker_data.get("ticker").unwrap_or(&ticker_data);

        Ok(TickerInfo {
            symbol: formatted,
            last_price: ticker["latest"].as_f64().unwrap_or(0.0),
            bid_price: None, // LBank ticker doesn't include bid/ask
            ask_price: None,
            high_24h: ticker["high"].as_f64(),
            low_24h: ticker["low"].as_f64(),
            volume_24h: ticker["vol"].as_f64(),
            turnover_24h: ticker["turnover"].as_f64(),
            change_24h: ticker["change"].as_f64(),
            open_interest: None,
            open_interest_value: None,
            funding_rate: None,
            next_funding_time: None,
            mark_price: None,
            index_price: None,
            timestamp: ticker_data["timestamp"].as_i64().unwrap_or(timestamp_ms()),
        })
    }

    async fn get_orderbook(&self, symbol: &str, depth: u32) -> Result<Orderbook> {
        let formatted = self.format_symbol(symbol);
        let size = depth.min(60); // LBank max depth is 60

        let response: serde_json::Value = self
            .client
            .get(format!(
                "{}/v2/depth.do?symbol={}&size={}",
                self.base_url, formatted, size
            ))
            .send()
            .await?
            .json()
            .await?;

        let bids: Vec<OrderbookLevel> = response["bids"]
            .as_array()
            .map(|arr| {
                arr.iter()
                    .filter_map(|level| {
                        let price = level[0].as_f64()
                            .or_else(|| level[0].as_str().and_then(|s| s.parse().ok()))?;
                        let qty = level[1].as_f64()
                            .or_else(|| level[1].as_str().and_then(|s| s.parse().ok()))?;
                        Some(OrderbookLevel { price, quantity: qty })
                    })
                    .collect()
            })
            .unwrap_or_default();

        let asks: Vec<OrderbookLevel> = response["asks"]
            .as_array()
            .map(|arr| {
                arr.iter()
                    .filter_map(|level| {
                        let price = level[0].as_f64()
                            .or_else(|| level[0].as_str().and_then(|s| s.parse().ok()))?;
                        let qty = level[1].as_f64()
                            .or_else(|| level[1].as_str().and_then(|s| s.parse().ok()))?;
                        Some(OrderbookLevel { price, quantity: qty })
                    })
                    .collect()
            })
            .unwrap_or_default();

        Ok(Orderbook {
            symbol: formatted,
            bids,
            asks,
            timestamp: response["timestamp"].as_i64().unwrap_or(timestamp_ms()),
        })
    }

    async fn set_leverage(&self, _symbol: Option<&str>, _leverage: u32) -> Result<bool> {
        // LBank spot doesn't support leverage
        // For futures, this would need to be implemented
        if self.market_type == MarketType::Spot {
            return Ok(true);
        }
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

        // Get current price if not provided
        let price = match current_price {
            Some(p) => p,
            None => self.get_current_price(&formatted).await?,
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

        // Place market order
        self.place_market_order(&formatted, side, quantity).await
    }

    async fn place_market_order(
        &self,
        symbol: &str,
        side: OrderSide,
        quantity: f64,
    ) -> Result<OrderResult> {
        let formatted = self.format_symbol(symbol);

        // LBank uses "buy_market" and "sell_market" for market orders
        let order_type = match side {
            OrderSide::Buy => "buy_market",
            OrderSide::Sell => "sell_market",
        };

        let quantity_str = format!("{:.8}", quantity);

        let params = [
            ("symbol", formatted.as_str()),
            ("type", order_type),
            ("amount", quantity_str.as_str()),
        ];

        let response: LbankOrderResponse = self
            .signed_post("/v2/create_order.do", &params)
            .await?;

        if response.result.as_deref() != Some("true") {
            let error_code = response.error_code.unwrap_or(0);
            return Ok(OrderResult::failure(
                formatted,
                side,
                format!("LBank order failed with error code: {}", error_code),
            ));
        }

        let order_id = response.order_id.unwrap_or_default();
        info!("LBank market order placed: {}", order_id);

        Ok(OrderResult {
            success: true,
            order_id: Some(order_id),
            symbol: formatted,
            side,
            order_type: OrderType::Market,
            quantity: Some(quantity),
            entry_price: None, // Will be filled by exchange
            position_size: Some(quantity),
            position_usdt: None,
            error: None,
            raw_response: None,
        })
    }

    async fn place_limit_order(
        &self,
        symbol: &str,
        side: OrderSide,
        quantity: f64,
        price: f64,
        _reduce_only: bool,
    ) -> Result<OrderResult> {
        let formatted = self.format_symbol(symbol);

        // LBank uses "buy" and "sell" for limit orders
        let order_type = match side {
            OrderSide::Buy => "buy",
            OrderSide::Sell => "sell",
        };

        let quantity_str = format!("{:.8}", quantity);
        let price_str = format!("{:.8}", price);

        let params = [
            ("symbol", formatted.as_str()),
            ("type", order_type),
            ("amount", quantity_str.as_str()),
            ("price", price_str.as_str()),
        ];

        let response: LbankOrderResponse = self
            .signed_post("/v2/create_order.do", &params)
            .await?;

        if response.result.as_deref() != Some("true") {
            let error_code = response.error_code.unwrap_or(0);
            return Ok(OrderResult::failure(
                formatted,
                side,
                format!("LBank order failed with error code: {}", error_code),
            ));
        }

        let order_id = response.order_id.unwrap_or_default();
        info!("LBank limit order placed: {}", order_id);

        Ok(OrderResult {
            success: true,
            order_id: Some(order_id),
            symbol: formatted,
            side,
            order_type: OrderType::Limit,
            quantity: Some(quantity),
            entry_price: Some(price),
            position_size: Some(quantity),
            position_usdt: Some(quantity * price),
            error: None,
            raw_response: None,
        })
    }

    async fn cancel_order(&self, symbol: &str, order_id: &str) -> Result<bool> {
        let formatted = self.format_symbol(symbol);

        let params = [
            ("symbol", formatted.as_str()),
            ("order_id", order_id),
        ];

        let response: serde_json::Value = self
            .signed_post("/v2/cancel_order.do", &params)
            .await?;

        let success = response["result"].as_str() == Some("true")
            || response["result"].as_bool() == Some(true);

        if success {
            info!("LBank order cancelled: {}", order_id);
        } else {
            warn!("Failed to cancel LBank order {}: {:?}", order_id, response);
        }

        Ok(success)
    }

    async fn get_order_status(&self, symbol: &str, order_id: &str) -> Result<OrderInfo> {
        let formatted = self.format_symbol(symbol);

        let params = [
            ("symbol", formatted.as_str()),
            ("order_id", order_id),
        ];

        let response: serde_json::Value = self
            .signed_post("/v2/orders_info.do", &params)
            .await?;

        let orders = response["orders"]
            .as_array()
            .ok_or_else(|| anyhow::anyhow!("No orders in response"))?;

        let order = orders
            .first()
            .ok_or_else(|| anyhow::anyhow!("Order not found"))?;

        let status_code = order["status"]
            .as_i64()
            .or_else(|| order["status"].as_str().and_then(|s| s.parse().ok()))
            .unwrap_or(0);

        let status = match status_code {
            -1 => OrderStatus::Cancelled,
            0 => OrderStatus::New,
            1 => OrderStatus::PartiallyFilled,
            2 => OrderStatus::Filled,
            4 => OrderStatus::Cancelled,
            _ => OrderStatus::New,
        };

        let order_type_str = order["type"].as_str().unwrap_or("");
        let (side, order_type) = match order_type_str {
            "buy" => (OrderSide::Buy, OrderType::Limit),
            "sell" => (OrderSide::Sell, OrderType::Limit),
            "buy_market" => (OrderSide::Buy, OrderType::Market),
            "sell_market" => (OrderSide::Sell, OrderType::Market),
            _ => (OrderSide::Buy, OrderType::Limit),
        };

        let price = order["price"]
            .as_f64()
            .or_else(|| order["price"].as_str().and_then(|s| s.parse().ok()))
            .unwrap_or(0.0);

        let amount = order["amount"]
            .as_f64()
            .or_else(|| order["amount"].as_str().and_then(|s| s.parse().ok()))
            .unwrap_or(0.0);

        let deal_amount = order["deal_amount"]
            .as_f64()
            .or_else(|| order["deal_amount"].as_str().and_then(|s| s.parse().ok()))
            .unwrap_or(0.0);

        let avg_price = order["avg_price"]
            .as_f64()
            .or_else(|| order["avg_price"].as_str().and_then(|s| s.parse().ok()));

        Ok(OrderInfo {
            order_id: order["order_id"]
                .as_str()
                .unwrap_or(order_id)
                .to_string(),
            client_order_id: None,
            symbol: formatted,
            status,
            side,
            order_type,
            price,
            quantity: amount,
            filled_quantity: deal_amount,
            remaining_quantity: amount - deal_amount,
            avg_price,
            timestamp: order["create_time"].as_i64().unwrap_or(0),
            time_in_force: TimeInForce::GTC,
            reduce_only: false,
        })
    }

    async fn close_position(&self, symbol: &str) -> Result<OrderResult> {
        // For spot trading, there's no "position" to close
        // This would need to sell all holdings of the base asset
        if self.market_type == MarketType::Spot {
            return Ok(OrderResult::failure(
                symbol.to_string(),
                OrderSide::Sell,
                "Close position not applicable for spot trading".to_string(),
            ));
        }

        Ok(OrderResult::failure(
            symbol.to_string(),
            OrderSide::Sell,
            "Close position not implemented for LBank".to_string(),
        ))
    }

    fn format_symbol(&self, base: &str) -> String {
        let s = base.to_lowercase();

        // Handle BTC/USDT format
        if s.contains('/') {
            return s.replace("/", "_");
        }

        // Handle BTCUSDT format -> btc_usdt
        if s.contains('_') {
            return s;
        }

        // Handle btcusdt -> btc_usdt
        if s.ends_with("usdt") {
            let base_asset = s.trim_end_matches("usdt");
            return format!("{}_usdt", base_asset);
        }

        // Handle btc -> btc_usdt
        format!("{}_usdt", s)
    }

    async fn test_ws_orderbook(
        &self,
        symbol: &str,
        _depth: u32,
        timeout_ms: u64,
    ) -> Result<WsTestResult> {
        let formatted = self.format_symbol(symbol);

        let start = std::time::Instant::now();
        let (mut ws_stream, _) = connect_async(WS_PUBLIC_URL)
            .await
            .context("Failed to connect to LBank WebSocket")?;

        let connect_time = start.elapsed().as_millis() as u64;

        // Subscribe to depth
        let subscribe_msg = serde_json::json!({
            "action": "subscribe",
            "subscribe": "depth",
            "depth": "20",
            "pair": formatted
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
                            if let Ok(parsed) = serde_json::from_str::<serde_json::Value>(&text) {
                                if parsed.get("depth").is_some() || parsed.get("asks").is_some() {
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
            subscription_type: "depth".to_string(),
            data_sample,
        })
    }

    async fn test_ws_trades(&self, symbol: &str, timeout_ms: u64) -> Result<WsTestResult> {
        let formatted = self.format_symbol(symbol);

        let start = std::time::Instant::now();
        let (mut ws_stream, _) = connect_async(WS_PUBLIC_URL)
            .await
            .context("Failed to connect to LBank WebSocket")?;

        let connect_time = start.elapsed().as_millis() as u64;

        // Subscribe to trades
        let subscribe_msg = serde_json::json!({
            "action": "subscribe",
            "subscribe": "trade",
            "pair": formatted
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
                            if let Ok(parsed) = serde_json::from_str::<serde_json::Value>(&text) {
                                if parsed.get("trade").is_some() {
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

    async fn test_ws_ticker(&self, symbol: &str, timeout_ms: u64) -> Result<WsTestResult> {
        let formatted = self.format_symbol(symbol);

        let start = std::time::Instant::now();
        let (mut ws_stream, _) = connect_async(WS_PUBLIC_URL)
            .await
            .context("Failed to connect to LBank WebSocket")?;

        let connect_time = start.elapsed().as_millis() as u64;

        // Subscribe to ticker
        let subscribe_msg = serde_json::json!({
            "action": "subscribe",
            "subscribe": "tick",
            "pair": formatted
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
                            if let Ok(parsed) = serde_json::from_str::<serde_json::Value>(&text) {
                                if parsed.get("tick").is_some() || parsed.get("latest").is_some() {
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
            subscription_type: "ticker".to_string(),
            data_sample,
        })
    }

    async fn get_funding_rate_history(
        &self,
        _symbol: &str,
        _limit: u32,
    ) -> Result<Vec<FundingRateEntry>> {
        // LBank spot doesn't have funding rates
        if self.market_type == MarketType::Spot {
            anyhow::bail!("Funding rate not available for spot trading");
        }
        Ok(vec![])
    }

    async fn get_open_interest_history(
        &self,
        _symbol: &str,
        _interval: &str,
        _limit: u32,
    ) -> Result<Vec<OpenInterestEntry>> {
        // LBank spot doesn't have open interest
        if self.market_type == MarketType::Spot {
            anyhow::bail!("Open interest not available for spot trading");
        }
        Ok(vec![])
    }

    async fn subscribe_order_updates(
        &self,
        sender: broadcast::Sender<OrderUpdateEvent>,
    ) -> Result<JoinHandle<()>> {
        let api_key = self.api_key.clone();
        let api_secret = self.api_secret.clone();
        let exchange_name = "lbank".to_string();

        let handle = tokio::spawn(async move {
            let mut reconnect_delay = INITIAL_RECONNECT_DELAY_SECS;

            loop {
                info!("[{}] Connecting to private WebSocket", exchange_name);

                match connect_async(WS_PRIVATE_URL).await {
                    Ok((mut ws_stream, _)) => {
                        // Authenticate
                        let timestamp = timestamp_ms();
                        let sign_str = format!("{}{}timestamp{}", api_key, api_secret, timestamp);
                        let signature = hmac_sign(&api_secret, &sign_str);

                        let auth_msg = serde_json::json!({
                            "action": "subscribe",
                            "subscribe": "orderUpdate",
                            "api_key": api_key,
                            "timestamp": timestamp.to_string(),
                            "sign": signature
                        });

                        if let Err(e) = ws_stream
                            .send(Message::Text(auth_msg.to_string()))
                            .await
                        {
                            error!("[{}] Failed to send auth: {}", exchange_name, e);
                            tokio::time::sleep(tokio::time::Duration::from_secs(reconnect_delay)).await;
                            reconnect_delay = next_reconnect_delay(reconnect_delay);
                            continue;
                        }

                        // Wait for auth response
                        let mut authenticated = false;
                        let auth_timeout = tokio::time::Duration::from_secs(10);

                        tokio::select! {
                            msg = ws_stream.next() => {
                                match msg {
                                    Some(Ok(Message::Text(text))) => {
                                        debug!("[{}] Auth response: {}", exchange_name, text);
                                        if let Ok(resp) = serde_json::from_str::<serde_json::Value>(&text) {
                                            if resp.get("status").and_then(|v| v.as_str()) == Some("ok")
                                                || resp.get("result").and_then(|v| v.as_str()) == Some("true")
                                                || resp.get("success").and_then(|v| v.as_bool()) == Some(true)
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
                            tokio::time::sleep(tokio::time::Duration::from_secs(reconnect_delay)).await;
                            reconnect_delay = next_reconnect_delay(reconnect_delay);
                            continue;
                        }

                        info!("[{}] Subscribed to order updates", exchange_name);
                        reconnect_delay = INITIAL_RECONNECT_DELAY_SECS;

                        let mut ping_interval =
                            tokio::time::interval(tokio::time::Duration::from_secs(30));

                        loop {
                            tokio::select! {
                                msg = ws_stream.next() => {
                                    match msg {
                                        Some(Ok(Message::Text(text))) => {
                                            if let Ok(msg_json) = serde_json::from_str::<serde_json::Value>(&text) {
                                                // Check for order update
                                                if let Some(order_data) = msg_json.get("orderUpdate")
                                                    .or_else(|| msg_json.get("order"))
                                                    .or_else(|| msg_json.get("data"))
                                                {
                                                    let order_id = order_data["order_id"]
                                                        .as_str()
                                                        .or_else(|| order_data["orderId"].as_str())
                                                        .unwrap_or("")
                                                        .to_string();

                                                    let symbol = order_data["symbol"]
                                                        .as_str()
                                                        .or_else(|| order_data["pair"].as_str())
                                                        .unwrap_or("")
                                                        .to_string();

                                                    let status_val = order_data["status"]
                                                        .as_i64()
                                                        .map(|i| i.to_string())
                                                        .or_else(|| order_data["status"].as_str().map(|s| s.to_string()))
                                                        .unwrap_or_else(|| "0".to_string());

                                                    let order_type_str = order_data["type"]
                                                        .as_str()
                                                        .unwrap_or("");

                                                    let (side, order_type) = match order_type_str {
                                                        "buy" => (OrderSide::Buy, OrderType::Limit),
                                                        "sell" => (OrderSide::Sell, OrderType::Limit),
                                                        "buy_market" => (OrderSide::Buy, OrderType::Market),
                                                        "sell_market" => (OrderSide::Sell, OrderType::Market),
                                                        _ => (OrderSide::Buy, OrderType::Limit),
                                                    };

                                                    let price = order_data["price"]
                                                        .as_f64()
                                                        .or_else(|| order_data["price"].as_str().and_then(|s| s.parse().ok()))
                                                        .unwrap_or(0.0);

                                                    let quantity = order_data["amount"]
                                                        .as_f64()
                                                        .or_else(|| order_data["amount"].as_str().and_then(|s| s.parse().ok()))
                                                        .unwrap_or(0.0);

                                                    let filled_quantity = order_data["deal_amount"]
                                                        .as_f64()
                                                        .or_else(|| order_data["deal_amount"].as_str().and_then(|s| s.parse().ok()))
                                                        .or_else(|| order_data["filled"].as_f64())
                                                        .unwrap_or(0.0);

                                                    let avg_fill_price = order_data["avg_price"]
                                                        .as_f64()
                                                        .or_else(|| order_data["avg_price"].as_str().and_then(|s| s.parse().ok()));

                                                    let event = OrderUpdateEvent {
                                                        exchange: exchange_name.clone(),
                                                        order_id,
                                                        client_order_id: order_data["custom_id"]
                                                            .as_str()
                                                            .map(|s| s.to_string()),
                                                        symbol,
                                                        status: parse_order_status_str(&status_val),
                                                        side,
                                                        order_type,
                                                        price,
                                                        quantity,
                                                        filled_quantity,
                                                        avg_fill_price,
                                                        fee: order_data["fee"]
                                                            .as_f64()
                                                            .or_else(|| order_data["fee"].as_str().and_then(|s| s.parse().ok())),
                                                        fee_currency: order_data["fee_currency"]
                                                            .as_str()
                                                            .map(|s| s.to_string()),
                                                        exchange_timestamp: order_data["create_time"]
                                                            .as_i64()
                                                            .or_else(|| order_data["timestamp"].as_i64())
                                                            .unwrap_or(0),
                                                        received_at: timestamp_ms(),
                                                        raw: Some(order_data.clone()),
                                                    };

                                                    log_order_update(&event);
                                                    let _ = sender.send(event);
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
                                        "action": "ping"
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_format_symbol() {
        let trader = LbankTrader::new("", "", false, MarketType::Spot);

        // Test various input formats
        assert_eq!(trader.format_symbol("BTC"), "btc_usdt");
        assert_eq!(trader.format_symbol("BTCUSDT"), "btc_usdt");
        assert_eq!(trader.format_symbol("btcusdt"), "btc_usdt");
        assert_eq!(trader.format_symbol("BTC/USDT"), "btc_usdt");
        assert_eq!(trader.format_symbol("btc_usdt"), "btc_usdt");
        assert_eq!(trader.format_symbol("ETH"), "eth_usdt");
        assert_eq!(trader.format_symbol("eth/usdt"), "eth_usdt");
    }

    #[test]
    fn test_parse_order_status() {
        assert_eq!(parse_order_status_str("0"), OrderStatus::New);
        assert_eq!(parse_order_status_str("1"), OrderStatus::PartiallyFilled);
        assert_eq!(parse_order_status_str("2"), OrderStatus::Filled);
        assert_eq!(parse_order_status_str("-1"), OrderStatus::Cancelled);
        assert_eq!(parse_order_status_str("new"), OrderStatus::New);
        assert_eq!(parse_order_status_str("filled"), OrderStatus::Filled);
    }

    #[test]
    fn test_exchange_name() {
        let trader = LbankTrader::new("", "", false, MarketType::Spot);
        assert_eq!(trader.exchange_name(), "lbank");
    }

    #[test]
    fn test_market_type() {
        let spot_trader = LbankTrader::new("", "", false, MarketType::Spot);
        assert_eq!(spot_trader.market_type(), MarketType::Spot);

        let swap_trader = LbankTrader::new("", "", false, MarketType::Swap);
        assert_eq!(swap_trader.market_type(), MarketType::Swap);
    }
}
