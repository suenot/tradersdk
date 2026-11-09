//! BitMart exchange trader implementation.

use anyhow::{Context, Result};
use async_trait::async_trait;
use futures_util::{SinkExt, StreamExt};
use reqwest::Client;
use serde::Serialize;
use tokio::sync::broadcast;
use tokio::task::JoinHandle;
use tokio_tungstenite::{connect_async, tungstenite::protocol::Message};
use tracing::{debug, error, info, warn};
use tradersdk_core::models::{
    AccountBalance, Balance, ExchangeCapabilities, MarketType, OrderInfo, OrderResult, OrderSide,
    OrderStatus, OrderType, OrderUpdateEvent, Orderbook, OrderbookLevel, TickerInfo, TimeInForce,
    WsTestResult,
};
use tradersdk_core::utils::timestamp_ms;
use tradersdk_core::ExchangeTrader;

// BitMart API endpoints
const MAINNET_URL: &str = "https://api-cloud.bitmart.com";
const MAINNET_WS_PUBLIC_URL: &str = "wss://ws-manager-compress.bitmart.com/api?protocol=1.1";
const MAINNET_WS_PRIVATE_URL: &str = "wss://ws-manager-compress.bitmart.com/user?protocol=1.1";

// Contract (Futures) endpoints
const CONTRACT_WS_PUBLIC_URL: &str = "wss://openapi-ws.bitmart.com/api?protocol=1.1";
const CONTRACT_WS_PRIVATE_URL: &str = "wss://openapi-ws.bitmart.com/user?protocol=1.1";

/// Maximum reconnect backoff delay in seconds.
const MAX_RECONNECT_DELAY_SECS: u64 = 30;

/// Initial reconnect delay in seconds.
const INITIAL_RECONNECT_DELAY_SECS: u64 = 1;

/// BitMart exchange trader.
pub struct BitmartTrader {
    api_key: String,
    api_secret: String,
    memo: String, // BitMart uses memo instead of passphrase
    testnet: bool,
    market_type: MarketType,
    client: Client,
    base_url: String,
}

impl BitmartTrader {
    /// Create a new BitMart trader.
    ///
    /// # Arguments
    /// * `api_key` - BitMart API key
    /// * `api_secret` - BitMart API secret
    /// * `memo` - BitMart API memo (required for authentication)
    /// * `testnet` - Whether to use testnet (BitMart doesn't have a public testnet)
    /// * `market_type` - Market type (Spot or Swap)
    pub fn new(
        api_key: &str,
        api_secret: &str,
        memo: Option<&str>,
        testnet: bool,
        market_type: MarketType,
    ) -> Self {
        Self {
            api_key: api_key.to_string(),
            api_secret: api_secret.to_string(),
            memo: memo.unwrap_or("").to_string(),
            testnet,
            market_type,
            client: Client::new(),
            base_url: MAINNET_URL.to_string(),
        }
    }

    /// Get WebSocket public URL based on market type.
    fn ws_public_url(&self) -> &str {
        match self.market_type {
            MarketType::Spot => MAINNET_WS_PUBLIC_URL,
            MarketType::Swap => CONTRACT_WS_PUBLIC_URL,
        }
    }

    /// Get WebSocket private URL based on market type.
    fn ws_private_url(&self) -> &str {
        match self.market_type {
            MarketType::Spot => MAINNET_WS_PRIVATE_URL,
            MarketType::Swap => CONTRACT_WS_PRIVATE_URL,
        }
    }

    /// Generate signature for REST API authentication.
    /// BitMart signature: HMAC-SHA256(timestamp + "#" + memo + "#" + queryString)
    fn sign_request(&self, timestamp: i64, query_string: &str) -> String {
        use hmac::{Hmac, Mac};
        use sha2::Sha256;

        type HmacSha256 = Hmac<Sha256>;

        let message = format!("{}#{}#{}", timestamp, self.memo, query_string);

        let mut mac = HmacSha256::new_from_slice(self.api_secret.as_bytes())
            .expect("HMAC can take key of any size");
        mac.update(message.as_bytes());

        let result = mac.finalize();
        hex::encode(result.into_bytes())
    }

    /// Generate WebSocket authentication signature.
    /// BitMart WS signature: HMAC-SHA256(timestamp + "#" + memo + "#" + "bitmart.WebSocket")
    fn ws_auth_signature(&self, timestamp: i64) -> String {
        use hmac::{Hmac, Mac};
        use sha2::Sha256;

        type HmacSha256 = Hmac<Sha256>;

        let message = format!("{}#{}#bitmart.WebSocket", timestamp, self.memo);

        let mut mac = HmacSha256::new_from_slice(self.api_secret.as_bytes())
            .expect("HMAC can take key of any size");
        mac.update(message.as_bytes());

        let result = mac.finalize();
        hex::encode(result.into_bytes())
    }

    /// Make authenticated GET request.
    async fn signed_get(&self, endpoint: &str) -> Result<serde_json::Value> {
        let timestamp = timestamp_ms();
        let url = format!("{}{}", self.base_url, endpoint);
        let signature = self.sign_request(timestamp, "");

        let response = self
            .client
            .get(&url)
            .header("X-BM-KEY", &self.api_key)
            .header("X-BM-SIGN", &signature)
            .header("X-BM-TIMESTAMP", timestamp.to_string())
            .send()
            .await?
            .json()
            .await?;

        Ok(response)
    }

    /// Make authenticated POST request.
    async fn signed_post(
        &self,
        endpoint: &str,
        body: &serde_json::Value,
    ) -> Result<serde_json::Value> {
        let timestamp = timestamp_ms();
        let url = format!("{}{}", self.base_url, endpoint);
        let body_str = serde_json::to_string(body)?;
        let signature = self.sign_request(timestamp, &body_str);

        let response = self
            .client
            .post(&url)
            .header("X-BM-KEY", &self.api_key)
            .header("X-BM-SIGN", &signature)
            .header("X-BM-TIMESTAMP", timestamp.to_string())
            .header("Content-Type", "application/json")
            .body(body_str)
            .send()
            .await?
            .json()
            .await?;

        Ok(response)
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
            .context("Failed to connect to BitMart private WebSocket")?;

        // Authenticate
        let timestamp = timestamp_ms();
        let signature = self.ws_auth_signature(timestamp);

        let auth_msg = serde_json::json!({
            "op": "login",
            "args": [self.api_key.clone(), timestamp.to_string(), signature]
        });

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
                        let auth_resp: serde_json::Value = serde_json::from_str(&text)
                            .context("Failed to parse auth response")?;

                        // BitMart returns {"event":"login"} on success
                        if auth_resp.get("event").and_then(|v| v.as_str()) == Some("login") {
                            if auth_resp.get("errorCode").is_some() {
                                let error_msg = auth_resp.get("errorMessage")
                                    .and_then(|v| v.as_str())
                                    .unwrap_or("Unknown error");
                                anyhow::bail!("WebSocket authentication failed: {}", error_msg);
                            }
                            info!("BitMart WebSocket authenticated");
                        } else {
                            anyhow::bail!("Unexpected auth response: {:?}", auth_resp);
                        }
                    }
                    Some(Ok(Message::Binary(data))) => {
                        // BitMart sends compressed data
                        let text = decompress_gzip(&data)?;
                        debug!("Auth response (decompressed): {}", text);
                        let auth_resp: serde_json::Value = serde_json::from_str(&text)?;

                        if auth_resp.get("event").and_then(|v| v.as_str()) == Some("login") {
                            if auth_resp.get("errorCode").is_some() {
                                let error_msg = auth_resp.get("errorMessage")
                                    .and_then(|v| v.as_str())
                                    .unwrap_or("Unknown error");
                                anyhow::bail!("WebSocket authentication failed: {}", error_msg);
                            }
                            info!("BitMart WebSocket authenticated");
                        }
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

    /// Place order via REST API.
    async fn place_order_rest(
        &self,
        symbol: &str,
        side: OrderSide,
        order_type: OrderType,
        quantity: f64,
        price: Option<f64>,
    ) -> Result<OrderResult> {
        let endpoint = match self.market_type {
            MarketType::Spot => "/spot/v2/submit_order",
            MarketType::Swap => "/contract/private/submit-order",
        };

        let mut body = serde_json::json!({
            "symbol": symbol,
            "side": match side {
                OrderSide::Buy => "buy",
                OrderSide::Sell => "sell",
            },
            "type": match order_type {
                OrderType::Market => "market",
                OrderType::Limit => "limit",
                _ => "market",
            },
            "size": format!("{:.8}", quantity),
        });

        // Add price for limit orders
        if let (OrderType::Limit, Some(p)) = (order_type, price) {
            body["price"] = serde_json::json!(format!("{:.8}", p));
        }

        // Add notional for spot market buy orders
        if self.market_type == MarketType::Spot
            && order_type == OrderType::Market
            && side == OrderSide::Buy
        {
            if let Some(p) = price {
                body["notional"] = serde_json::json!(format!("{:.8}", quantity * p));
            }
        }

        let response = self.signed_post(endpoint, &body).await?;
        debug!("Order response: {:?}", response);

        // Check for success
        let code = response.get("code").and_then(|v| v.as_i64()).unwrap_or(-1);

        if code == 1000 {
            let data = response.get("data").unwrap_or(&serde_json::Value::Null);
            let order_id = data
                .get("order_id")
                .and_then(|v| v.as_str())
                .or_else(|| data.get("order_id").and_then(|v| v.as_i64()).map(|_| ""))
                .unwrap_or("");

            info!("BitMart order placed: {}", order_id);
            Ok(OrderResult {
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
            })
        } else {
            let error_msg = response
                .get("message")
                .and_then(|v| v.as_str())
                .unwrap_or("Unknown error")
                .to_string();

            error!("BitMart order failed: {}", error_msg);
            Ok(OrderResult::failure(symbol.to_string(), side, error_msg))
        }
    }

    /// Cancel order via REST API.
    async fn cancel_order_rest(&self, symbol: &str, order_id: &str) -> Result<bool> {
        let endpoint = match self.market_type {
            MarketType::Spot => "/spot/v3/cancel_order",
            MarketType::Swap => "/contract/private/cancel-order",
        };

        let body = serde_json::json!({
            "symbol": symbol,
            "order_id": order_id,
        });

        let response = self.signed_post(endpoint, &body).await?;
        debug!("Cancel response: {:?}", response);

        let code = response.get("code").and_then(|v| v.as_i64()).unwrap_or(-1);

        if code == 1000 {
            info!("BitMart order cancelled: {}", order_id);
            Ok(true)
        } else {
            let error_msg = response
                .get("message")
                .and_then(|v| v.as_str())
                .unwrap_or("Unknown error");
            error!("Failed to cancel order {}: {}", order_id, error_msg);
            Ok(false)
        }
    }
}

/// Decompress gzip data from BitMart WebSocket.
fn decompress_gzip(data: &[u8]) -> Result<String> {
    use flate2::read::GzDecoder;
    use std::io::Read;

    let mut decoder = GzDecoder::new(data);
    let mut decompressed = String::new();
    decoder.read_to_string(&mut decompressed)?;
    Ok(decompressed)
}

/// Calculate next reconnect delay with exponential backoff (capped).
fn next_reconnect_delay(current_secs: u64) -> u64 {
    (current_secs * 2).min(MAX_RECONNECT_DELAY_SECS)
}

/// Parse a status string from BitMart into OrderStatus.
fn parse_order_status_str(s: &str) -> OrderStatus {
    match s.to_lowercase().as_str() {
        "new" | "open" | "1" | "2" => OrderStatus::New,
        "partially_filled" | "partiallyfilled" | "partial_filled" | "3" | "5" => {
            OrderStatus::PartiallyFilled
        }
        "filled" | "closed" | "4" | "6" => OrderStatus::Filled,
        "cancelled" | "canceled" | "cancel" | "8" => OrderStatus::Cancelled,
        "rejected" | "fail" | "failed" | "7" => OrderStatus::Rejected,
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

/// WebSocket subscription request.
#[derive(Debug, Serialize)]
struct WsSubscribeRequest {
    op: String,
    args: Vec<String>,
}

#[async_trait]
impl ExchangeTrader for BitmartTrader {
    fn exchange_name(&self) -> &str {
        "bitmart"
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
            .get(format!("{}/system/time", self.base_url))
            .send()
            .await?;
        Ok(response.status().is_success())
    }

    async fn get_server_time(&self) -> Result<i64> {
        let response: serde_json::Value = self
            .client
            .get(format!("{}/system/time", self.base_url))
            .send()
            .await?
            .json()
            .await?;

        Ok(response["data"]["server_time"]
            .as_i64()
            .unwrap_or(timestamp_ms()))
    }

    async fn get_balance(&self) -> Result<AccountBalance> {
        let endpoint = match self.market_type {
            MarketType::Spot => "/spot/v1/wallet",
            MarketType::Swap => "/contract/private/assets-detail",
        };

        let response = self.signed_get(endpoint).await?;
        debug!("Balance response: {:?}", response);

        let code = response.get("code").and_then(|v| v.as_i64()).unwrap_or(-1);

        if code != 1000 {
            let error_msg = response
                .get("message")
                .and_then(|v| v.as_str())
                .unwrap_or("Unknown error");
            anyhow::bail!("Failed to get balance: {}", error_msg);
        }

        let data = response.get("data").unwrap_or(&serde_json::Value::Null);

        let mut total = 0.0;
        let mut available = 0.0;
        let mut balances = vec![];

        if self.market_type == MarketType::Spot {
            if let Some(wallet) = data.get("wallet").and_then(|v| v.as_array()) {
                for item in wallet {
                    let asset = item.get("id").and_then(|v| v.as_str()).unwrap_or("");
                    let free: f64 = item
                        .get("available")
                        .and_then(|v| v.as_str())
                        .and_then(|s| s.parse().ok())
                        .unwrap_or(0.0);
                    let locked: f64 = item
                        .get("frozen")
                        .and_then(|v| v.as_str())
                        .and_then(|s| s.parse().ok())
                        .unwrap_or(0.0);

                    if asset.to_uppercase() == "USDT" {
                        total = free + locked;
                        available = free;
                    }

                    if free > 0.0 || locked > 0.0 {
                        balances.push(Balance {
                            asset: asset.to_string(),
                            free,
                            locked,
                        });
                    }
                }
            }
        } else {
            // Futures balance
            total = data
                .get("equity")
                .and_then(|v| v.as_str())
                .and_then(|s| s.parse().ok())
                .unwrap_or(0.0);
            available = data
                .get("available_balance")
                .and_then(|v| v.as_str())
                .and_then(|s| s.parse().ok())
                .unwrap_or(0.0);
        }

        Ok(AccountBalance {
            total_balance: total,
            available_balance: available,
            currency: "USDT".to_string(),
            margin_balance: if self.market_type == MarketType::Swap {
                Some(total)
            } else {
                None
            },
            used_margin: None,
            unrealized_pnl: None,
            realized_pnl: None,
            balances,
        })
    }

    async fn get_current_price(&self, symbol: &str) -> Result<f64> {
        let formatted_symbol = self.format_symbol(symbol);

        let endpoint = match self.market_type {
            MarketType::Spot => format!("/spot/quotation/v3/ticker?symbol={}", formatted_symbol),
            MarketType::Swap => format!("/contract/public/details?symbol={}", formatted_symbol),
        };

        let response: serde_json::Value = self
            .client
            .get(format!("{}{}", self.base_url, endpoint))
            .send()
            .await?
            .json()
            .await?;

        let price = if self.market_type == MarketType::Spot {
            response["data"]["last"]
                .as_str()
                .and_then(|s| s.parse().ok())
                .unwrap_or(0.0)
        } else {
            response["data"]["last_price"]
                .as_str()
                .and_then(|s| s.parse().ok())
                .unwrap_or(0.0)
        };

        Ok(price)
    }

    async fn get_ticker(&self, symbol: &str) -> Result<TickerInfo> {
        let formatted_symbol = self.format_symbol(symbol);

        let endpoint = match self.market_type {
            MarketType::Spot => format!("/spot/quotation/v3/ticker?symbol={}", formatted_symbol),
            MarketType::Swap => format!("/contract/public/details?symbol={}", formatted_symbol),
        };

        let response: serde_json::Value = self
            .client
            .get(format!("{}{}", self.base_url, endpoint))
            .send()
            .await?
            .json()
            .await?;

        let data = &response["data"];

        if self.market_type == MarketType::Spot {
            Ok(TickerInfo {
                symbol: formatted_symbol,
                last_price: data["last"]
                    .as_str()
                    .and_then(|s| s.parse().ok())
                    .unwrap_or(0.0),
                bid_price: data["best_bid"].as_str().and_then(|s| s.parse().ok()),
                ask_price: data["best_ask"].as_str().and_then(|s| s.parse().ok()),
                volume_24h: data["v_24h"].as_str().and_then(|s| s.parse().ok()),
                turnover_24h: data["qv_24h"].as_str().and_then(|s| s.parse().ok()),
                high_24h: data["high_24h"].as_str().and_then(|s| s.parse().ok()),
                low_24h: data["low_24h"].as_str().and_then(|s| s.parse().ok()),
                change_24h: data["fluctuation"].as_str().and_then(|s| s.parse().ok()),
                open_interest: None,
                open_interest_value: None,
                funding_rate: None,
                next_funding_time: None,
                mark_price: None,
                index_price: None,
                timestamp: data["ts"].as_i64().unwrap_or(timestamp_ms()),
            })
        } else {
            // Futures ticker
            Ok(TickerInfo {
                symbol: formatted_symbol,
                last_price: data["last_price"]
                    .as_str()
                    .and_then(|s| s.parse().ok())
                    .unwrap_or(0.0),
                bid_price: data["best_bid"].as_str().and_then(|s| s.parse().ok()),
                ask_price: data["best_ask"].as_str().and_then(|s| s.parse().ok()),
                volume_24h: data["volume_24h"].as_str().and_then(|s| s.parse().ok()),
                turnover_24h: data["turnover_24h"].as_str().and_then(|s| s.parse().ok()),
                high_24h: data["high_24h"].as_str().and_then(|s| s.parse().ok()),
                low_24h: data["low_24h"].as_str().and_then(|s| s.parse().ok()),
                change_24h: data["price_change_percent_24h"]
                    .as_str()
                    .and_then(|s| s.parse().ok()),
                open_interest: data["open_interest"].as_str().and_then(|s| s.parse().ok()),
                open_interest_value: data["open_interest_value"]
                    .as_str()
                    .and_then(|s| s.parse().ok()),
                funding_rate: data["funding_rate"].as_str().and_then(|s| s.parse().ok()),
                next_funding_time: data["next_funding_time"].as_i64(),
                mark_price: data["index_price"].as_str().and_then(|s| s.parse().ok()),
                index_price: data["index_price"].as_str().and_then(|s| s.parse().ok()),
                timestamp: data["timestamp"].as_i64().unwrap_or(timestamp_ms()),
            })
        }
    }

    async fn get_orderbook(&self, symbol: &str, depth: u32) -> Result<Orderbook> {
        let formatted_symbol = self.format_symbol(symbol);

        let endpoint = match self.market_type {
            MarketType::Spot => format!(
                "/spot/quotation/v3/books?symbol={}&limit={}",
                formatted_symbol, depth
            ),
            MarketType::Swap => format!(
                "/contract/public/depth?symbol={}&size={}",
                formatted_symbol, depth
            ),
        };

        let response: serde_json::Value = self
            .client
            .get(format!("{}{}", self.base_url, endpoint))
            .send()
            .await?
            .json()
            .await?;

        let data = &response["data"];

        let bids: Vec<OrderbookLevel> = data["bids"]
            .as_array()
            .map(|arr| {
                arr.iter()
                    .filter_map(|item| {
                        let (price, quantity) = if item.is_array() {
                            // Array format: [price, quantity]
                            (
                                item[0].as_str()?.parse().ok()?,
                                item[1].as_str()?.parse().ok()?,
                            )
                        } else {
                            // Object format: {price, quantity}
                            (
                                item["price"].as_str()?.parse().ok()?,
                                item["amount"]
                                    .as_str()
                                    .or(item["quantity"].as_str())?
                                    .parse()
                                    .ok()?,
                            )
                        };
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
                        let (price, quantity) = if item.is_array() {
                            (
                                item[0].as_str()?.parse().ok()?,
                                item[1].as_str()?.parse().ok()?,
                            )
                        } else {
                            (
                                item["price"].as_str()?.parse().ok()?,
                                item["amount"]
                                    .as_str()
                                    .or(item["quantity"].as_str())?
                                    .parse()
                                    .ok()?,
                            )
                        };
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
                .as_i64()
                .or_else(|| data["timestamp"].as_i64())
                .unwrap_or(timestamp_ms()),
        })
    }

    async fn set_leverage(&self, symbol: Option<&str>, leverage: u32) -> Result<bool> {
        if self.market_type == MarketType::Spot {
            return Ok(true); // No leverage for spot
        }

        let symbol = symbol.ok_or_else(|| anyhow::anyhow!("Symbol required for leverage"))?;
        let formatted_symbol = self.format_symbol(symbol);

        let body = serde_json::json!({
            "symbol": formatted_symbol,
            "leverage": leverage.to_string(),
            "open_type": "cross", // or "isolated"
        });

        let response = self
            .signed_post("/contract/private/submit-leverage", &body)
            .await?;
        debug!("Set leverage response: {:?}", response);

        let code = response.get("code").and_then(|v| v.as_i64()).unwrap_or(-1);

        if code == 1000 {
            info!(
                "BitMart leverage set to {}x for {}",
                leverage, formatted_symbol
            );
            Ok(true)
        } else {
            let error_msg = response
                .get("message")
                .and_then(|v| v.as_str())
                .unwrap_or("Unknown error");
            error!("Failed to set leverage: {}", error_msg);
            Ok(false)
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
        let formatted_symbol = self.format_symbol(symbol);
        self.cancel_order_rest(&formatted_symbol, order_id).await
    }

    async fn get_order_status(&self, symbol: &str, order_id: &str) -> Result<OrderInfo> {
        let formatted_symbol = self.format_symbol(symbol);

        let endpoint = match self.market_type {
            MarketType::Spot => format!(
                "/spot/v4/query/order?symbol={}&orderId={}",
                formatted_symbol, order_id
            ),
            MarketType::Swap => format!(
                "/contract/private/order?symbol={}&order_id={}",
                formatted_symbol, order_id
            ),
        };

        let response = self.signed_get(&endpoint).await?;
        debug!("Order status response: {:?}", response);

        let code = response.get("code").and_then(|v| v.as_i64()).unwrap_or(-1);

        if code != 1000 {
            let error_msg = response
                .get("message")
                .and_then(|v| v.as_str())
                .unwrap_or("Unknown error");
            anyhow::bail!("Failed to get order status: {}", error_msg);
        }

        let data = response.get("data").unwrap_or(&serde_json::Value::Null);

        let status_str = data
            .get("state")
            .or_else(|| data.get("status"))
            .and_then(|v| v.as_str())
            .unwrap_or("new");

        let side_str = data.get("side").and_then(|v| v.as_str()).unwrap_or("buy");

        let type_str = data
            .get("type")
            .and_then(|v| v.as_str())
            .unwrap_or("market");

        Ok(OrderInfo {
            order_id: order_id.to_string(),
            client_order_id: data
                .get("client_order_id")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string()),
            symbol: formatted_symbol,
            status: parse_order_status_str(status_str),
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
            price: data
                .get("price")
                .and_then(|v| v.as_str())
                .and_then(|s| s.parse().ok())
                .unwrap_or(0.0),
            quantity: data
                .get("size")
                .or_else(|| data.get("quantity"))
                .and_then(|v| v.as_str())
                .and_then(|s| s.parse().ok())
                .unwrap_or(0.0),
            filled_quantity: data
                .get("filled_size")
                .or_else(|| data.get("filled_qty"))
                .and_then(|v| v.as_str())
                .and_then(|s| s.parse().ok())
                .unwrap_or(0.0),
            remaining_quantity: data
                .get("unfilled_size")
                .and_then(|v| v.as_str())
                .and_then(|s| s.parse().ok())
                .unwrap_or(0.0),
            avg_price: data
                .get("price_avg")
                .or_else(|| data.get("avg_price"))
                .and_then(|v| v.as_str())
                .and_then(|s| s.parse().ok()),
            timestamp: data
                .get("create_time")
                .or_else(|| data.get("created_at"))
                .and_then(|v| v.as_i64())
                .unwrap_or(timestamp_ms()),
            time_in_force: TimeInForce::GTC,
            reduce_only: false,
        })
    }

    async fn close_position(&self, symbol: &str) -> Result<OrderResult> {
        if self.market_type == MarketType::Spot {
            return Ok(OrderResult::failure(
                symbol.to_string(),
                OrderSide::Sell,
                "Close position not supported for spot".to_string(),
            ));
        }

        let formatted_symbol = self.format_symbol(symbol);

        // Get current position first
        let response = self
            .signed_get(&format!(
                "/contract/private/position?symbol={}",
                formatted_symbol
            ))
            .await?;

        let data = response.get("data").unwrap_or(&serde_json::Value::Null);
        let position_size: f64 = data
            .get("current_qty")
            .or_else(|| data.get("vol"))
            .and_then(|v| v.as_str())
            .and_then(|s| s.parse().ok())
            .unwrap_or(0.0);

        if position_size.abs() < 0.000001 {
            return Ok(OrderResult::failure(
                formatted_symbol.clone(),
                OrderSide::Sell,
                "No position to close".to_string(),
            ));
        }

        // Determine close side (opposite of position)
        let side = if position_size > 0.0 {
            OrderSide::Sell
        } else {
            OrderSide::Buy
        };

        // Place market order to close
        self.place_order_rest(
            &formatted_symbol,
            side,
            OrderType::Market,
            position_size.abs(),
            None,
        )
        .await
    }

    /// Format symbol for BitMart (uses underscore separator: BTC_USDT).
    fn format_symbol(&self, base: &str) -> String {
        let s = base.to_uppercase();

        // Already has underscore
        if s.contains('_') {
            return s;
        }

        // Handle BTC/USDT format
        if s.contains('/') {
            return s.replace("/", "_");
        }

        // Handle BTCUSDT format - convert to BTC_USDT
        if s.ends_with("USDT") {
            let base_coin = &s[..s.len() - 4];
            return format!("{}_USDT", base_coin);
        }

        if s.ends_with("USD") {
            let base_coin = &s[..s.len() - 3];
            return format!("{}_USD", base_coin);
        }

        // Default: add _USDT suffix
        format!("{}_USDT", s)
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
            .context("Failed to connect to BitMart WebSocket")?;

        let connect_latency = start.elapsed().as_millis() as u64;

        // Subscribe to orderbook
        let channel = match self.market_type {
            MarketType::Spot => format!("spot/depth{}:{}", depth.min(50), formatted_symbol),
            MarketType::Swap => format!("futures/depth{}:{}", depth.min(50), formatted_symbol),
        };

        let sub_msg = serde_json::json!({
            "op": "subscribe",
            "args": [channel]
        });

        ws_stream.send(Message::Text(sub_msg.to_string())).await?;

        let mut messages_received = 0u32;
        let mut data_sample = None;
        let timeout = tokio::time::Duration::from_millis(timeout_ms);
        let deadline = tokio::time::Instant::now() + timeout;

        loop {
            tokio::select! {
                msg = ws_stream.next() => {
                    match msg {
                        Some(Ok(Message::Text(text))) => {
                            if let Ok(json) = serde_json::from_str::<serde_json::Value>(&text) {
                                if json.get("data").is_some() {
                                    messages_received += 1;
                                    if data_sample.is_none() {
                                        data_sample = Some(json);
                                    }
                                }
                            }
                        }
                        Some(Ok(Message::Binary(data))) => {
                            if let Ok(text) = decompress_gzip(&data) {
                                if let Ok(json) = serde_json::from_str::<serde_json::Value>(&text) {
                                    if json.get("data").is_some() {
                                        messages_received += 1;
                                        if data_sample.is_none() {
                                            data_sample = Some(json);
                                        }
                                    }
                                }
                            }
                        }
                        Some(Ok(Message::Ping(data))) => {
                            let _ = ws_stream.send(Message::Pong(data)).await;
                        }
                        Some(Err(_)) | None => break,
                        _ => {}
                    }
                }
                _ = tokio::time::sleep_until(deadline) => {
                    break;
                }
            }

            if messages_received >= 3 {
                break;
            }
        }

        let _ = ws_stream.close(None).await;

        Ok(WsTestResult {
            connected: true,
            latency_ms: connect_latency,
            messages_received,
            subscription_type: "orderbook".to_string(),
            data_sample,
        })
    }

    async fn test_ws_trades(&self, symbol: &str, timeout_ms: u64) -> Result<WsTestResult> {
        let formatted_symbol = self.format_symbol(symbol);
        let ws_url = self.ws_public_url();

        let start = std::time::Instant::now();
        let (mut ws_stream, _) = connect_async(ws_url)
            .await
            .context("Failed to connect to BitMart WebSocket")?;

        let connect_latency = start.elapsed().as_millis() as u64;

        // Subscribe to trades
        let channel = match self.market_type {
            MarketType::Spot => format!("spot/trade:{}", formatted_symbol),
            MarketType::Swap => format!("futures/trade:{}", formatted_symbol),
        };

        let sub_msg = serde_json::json!({
            "op": "subscribe",
            "args": [channel]
        });

        ws_stream.send(Message::Text(sub_msg.to_string())).await?;

        let mut messages_received = 0u32;
        let mut data_sample = None;
        let timeout = tokio::time::Duration::from_millis(timeout_ms);
        let deadline = tokio::time::Instant::now() + timeout;

        loop {
            tokio::select! {
                msg = ws_stream.next() => {
                    match msg {
                        Some(Ok(Message::Text(text))) => {
                            if let Ok(json) = serde_json::from_str::<serde_json::Value>(&text) {
                                if json.get("data").is_some() {
                                    messages_received += 1;
                                    if data_sample.is_none() {
                                        data_sample = Some(json);
                                    }
                                }
                            }
                        }
                        Some(Ok(Message::Binary(data))) => {
                            if let Ok(text) = decompress_gzip(&data) {
                                if let Ok(json) = serde_json::from_str::<serde_json::Value>(&text) {
                                    if json.get("data").is_some() {
                                        messages_received += 1;
                                        if data_sample.is_none() {
                                            data_sample = Some(json);
                                        }
                                    }
                                }
                            }
                        }
                        Some(Ok(Message::Ping(data))) => {
                            let _ = ws_stream.send(Message::Pong(data)).await;
                        }
                        Some(Err(_)) | None => break,
                        _ => {}
                    }
                }
                _ = tokio::time::sleep_until(deadline) => {
                    break;
                }
            }

            if messages_received >= 3 {
                break;
            }
        }

        let _ = ws_stream.close(None).await;

        Ok(WsTestResult {
            connected: true,
            latency_ms: connect_latency,
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
            .context("Failed to connect to BitMart WebSocket")?;

        let connect_latency = start.elapsed().as_millis() as u64;

        // Subscribe to ticker
        let channel = match self.market_type {
            MarketType::Spot => format!("spot/ticker:{}", formatted_symbol),
            MarketType::Swap => format!("futures/ticker:{}", formatted_symbol),
        };

        let sub_msg = serde_json::json!({
            "op": "subscribe",
            "args": [channel]
        });

        ws_stream.send(Message::Text(sub_msg.to_string())).await?;

        let mut messages_received = 0u32;
        let mut data_sample = None;
        let timeout = tokio::time::Duration::from_millis(timeout_ms);
        let deadline = tokio::time::Instant::now() + timeout;

        loop {
            tokio::select! {
                msg = ws_stream.next() => {
                    match msg {
                        Some(Ok(Message::Text(text))) => {
                            if let Ok(json) = serde_json::from_str::<serde_json::Value>(&text) {
                                if json.get("data").is_some() {
                                    messages_received += 1;
                                    if data_sample.is_none() {
                                        data_sample = Some(json);
                                    }
                                }
                            }
                        }
                        Some(Ok(Message::Binary(data))) => {
                            if let Ok(text) = decompress_gzip(&data) {
                                if let Ok(json) = serde_json::from_str::<serde_json::Value>(&text) {
                                    if json.get("data").is_some() {
                                        messages_received += 1;
                                        if data_sample.is_none() {
                                            data_sample = Some(json);
                                        }
                                    }
                                }
                            }
                        }
                        Some(Ok(Message::Ping(data))) => {
                            let _ = ws_stream.send(Message::Pong(data)).await;
                        }
                        Some(Err(_)) | None => break,
                        _ => {}
                    }
                }
                _ = tokio::time::sleep_until(deadline) => {
                    break;
                }
            }

            if messages_received >= 3 {
                break;
            }
        }

        let _ = ws_stream.close(None).await;

        Ok(WsTestResult {
            connected: true,
            latency_ms: connect_latency,
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
        let memo = self.memo.clone();
        let market_type = self.market_type;
        let exchange_name = "bitmart".to_string();

        let handle = tokio::spawn(async move {
            let mut reconnect_delay = INITIAL_RECONNECT_DELAY_SECS;

            loop {
                let ws_url = match market_type {
                    MarketType::Spot => MAINNET_WS_PRIVATE_URL,
                    MarketType::Swap => CONTRACT_WS_PRIVATE_URL,
                };

                info!("[{}] Connecting to private WS: {}", exchange_name, ws_url);

                match connect_async(ws_url).await {
                    Ok((mut ws_stream, _)) => {
                        // Authenticate
                        let timestamp = timestamp_ms();
                        let signature = {
                            use hmac::{Hmac, Mac};
                            use sha2::Sha256;
                            type HmacSha256 = Hmac<Sha256>;
                            let message = format!("{}#{}#bitmart.WebSocket", timestamp, memo);
                            let mut mac = HmacSha256::new_from_slice(api_secret.as_bytes())
                                .expect("HMAC can take key of any size");
                            mac.update(message.as_bytes());
                            hex::encode(mac.finalize().into_bytes())
                        };

                        let auth_msg = serde_json::json!({
                            "op": "login",
                            "args": [api_key.clone(), timestamp.to_string(), signature]
                        });

                        if let Err(e) = ws_stream.send(Message::Text(auth_msg.to_string())).await {
                            error!("[{}] Failed to send login: {}", exchange_name, e);
                            tokio::time::sleep(tokio::time::Duration::from_secs(reconnect_delay))
                                .await;
                            reconnect_delay = next_reconnect_delay(reconnect_delay);
                            continue;
                        }

                        // Wait for auth response
                        let mut authenticated = false;
                        if let Some(msg_result) = ws_stream.next().await {
                            match msg_result {
                                Ok(Message::Text(text)) => {
                                    if let Ok(resp) =
                                        serde_json::from_str::<serde_json::Value>(&text)
                                    {
                                        if resp.get("event").and_then(|v| v.as_str())
                                            == Some("login")
                                            && resp.get("errorCode").is_none()
                                        {
                                            info!("[{}] Private WS authenticated", exchange_name);
                                            authenticated = true;
                                        } else {
                                            error!("[{}] Login failed: {}", exchange_name, text);
                                        }
                                    }
                                }
                                Ok(Message::Binary(data)) => {
                                    if let Ok(text) = decompress_gzip(&data) {
                                        if let Ok(resp) =
                                            serde_json::from_str::<serde_json::Value>(&text)
                                        {
                                            if resp.get("event").and_then(|v| v.as_str())
                                                == Some("login")
                                                && resp.get("errorCode").is_none()
                                            {
                                                info!(
                                                    "[{}] Private WS authenticated",
                                                    exchange_name
                                                );
                                                authenticated = true;
                                            }
                                        }
                                    }
                                }
                                _ => {}
                            }
                        }

                        if !authenticated {
                            tokio::time::sleep(tokio::time::Duration::from_secs(reconnect_delay))
                                .await;
                            reconnect_delay = next_reconnect_delay(reconnect_delay);
                            continue;
                        }

                        // Subscribe to order updates
                        let channel = match market_type {
                            MarketType::Spot => "spot/user/order:*",
                            MarketType::Swap => "futures/order",
                        };

                        let sub_msg = serde_json::json!({
                            "op": "subscribe",
                            "args": [channel]
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
                            tokio::time::interval(tokio::time::Duration::from_secs(15));

                        loop {
                            tokio::select! {
                                msg = ws_stream.next() => {
                                    match msg {
                                        Some(Ok(Message::Text(text))) => {
                                            process_order_message(&text, &exchange_name, &sender);
                                        }
                                        Some(Ok(Message::Binary(data))) => {
                                            if let Ok(text) = decompress_gzip(&data) {
                                                process_order_message(&text, &exchange_name, &sender);
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
                                    // BitMart expects "ping" message
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

/// Process order update message from WebSocket.
fn process_order_message(
    text: &str,
    exchange_name: &str,
    sender: &broadcast::Sender<OrderUpdateEvent>,
) {
    if let Ok(msg_json) = serde_json::from_str::<serde_json::Value>(text) {
        // Skip subscription confirmations and pongs
        if msg_json.get("event").is_some() || text == "pong" {
            debug!("[{}] WS event: {}", exchange_name, text);
            return;
        }

        // Process order data
        if let Some(data) = msg_json.get("data") {
            let orders = if data.is_array() {
                data.as_array().unwrap().clone()
            } else {
                vec![data.clone()]
            };

            for o in orders {
                let event = OrderUpdateEvent {
                    exchange: exchange_name.to_string(),
                    order_id: o
                        .get("order_id")
                        .or_else(|| o.get("orderId"))
                        .and_then(|v| v.as_str())
                        .unwrap_or("")
                        .to_string(),
                    client_order_id: o
                        .get("client_order_id")
                        .and_then(|v| v.as_str())
                        .map(|s| s.to_string()),
                    symbol: o
                        .get("symbol")
                        .and_then(|v| v.as_str())
                        .unwrap_or("")
                        .to_string(),
                    status: parse_order_status_str(
                        o.get("state")
                            .or_else(|| o.get("status"))
                            .and_then(|v| v.as_str())
                            .unwrap_or("new"),
                    ),
                    side: if o.get("side").and_then(|v| v.as_str()) == Some("buy") {
                        OrderSide::Buy
                    } else {
                        OrderSide::Sell
                    },
                    order_type: if o.get("type").and_then(|v| v.as_str()) == Some("market") {
                        OrderType::Market
                    } else {
                        OrderType::Limit
                    },
                    price: o
                        .get("price")
                        .and_then(|v| v.as_str())
                        .and_then(|s| s.parse().ok())
                        .unwrap_or(0.0),
                    quantity: o
                        .get("size")
                        .or_else(|| o.get("quantity"))
                        .and_then(|v| v.as_str())
                        .and_then(|s| s.parse().ok())
                        .unwrap_or(0.0),
                    filled_quantity: o
                        .get("filled_size")
                        .or_else(|| o.get("filled_qty"))
                        .and_then(|v| v.as_str())
                        .and_then(|s| s.parse().ok())
                        .unwrap_or(0.0),
                    avg_fill_price: o
                        .get("price_avg")
                        .or_else(|| o.get("avg_price"))
                        .and_then(|v| v.as_str())
                        .and_then(|s| s.parse().ok()),
                    fee: o
                        .get("fee")
                        .and_then(|v| v.as_str())
                        .and_then(|s| s.parse().ok()),
                    fee_currency: o
                        .get("fee_currency")
                        .and_then(|v| v.as_str())
                        .map(|s| s.to_string()),
                    exchange_timestamp: o
                        .get("create_time")
                        .or_else(|| o.get("ms_t"))
                        .and_then(|v| v.as_i64())
                        .unwrap_or(0),
                    received_at: timestamp_ms(),
                    raw: Some(o.clone()),
                };
                log_order_update(&event);
                let _ = sender.send(event);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_format_symbol() {
        let trader = BitmartTrader::new("", "", None, false, MarketType::Swap);
        assert_eq!(trader.format_symbol("BTC"), "BTC_USDT");
        assert_eq!(trader.format_symbol("BTCUSDT"), "BTC_USDT");
        assert_eq!(trader.format_symbol("BTC_USDT"), "BTC_USDT");
        assert_eq!(trader.format_symbol("eth"), "ETH_USDT");
        assert_eq!(trader.format_symbol("BTC/USDT"), "BTC_USDT");
        assert_eq!(trader.format_symbol("ETH/USDT"), "ETH_USDT");
    }

    #[test]
    fn test_ws_urls() {
        let spot = BitmartTrader::new("", "", None, false, MarketType::Spot);
        assert_eq!(spot.ws_public_url(), MAINNET_WS_PUBLIC_URL);
        assert_eq!(spot.ws_private_url(), MAINNET_WS_PRIVATE_URL);

        let swap = BitmartTrader::new("", "", None, false, MarketType::Swap);
        assert_eq!(swap.ws_public_url(), CONTRACT_WS_PUBLIC_URL);
        assert_eq!(swap.ws_private_url(), CONTRACT_WS_PRIVATE_URL);
    }

    #[test]
    fn test_parse_order_status() {
        assert_eq!(parse_order_status_str("new"), OrderStatus::New);
        assert_eq!(parse_order_status_str("4"), OrderStatus::Filled);
        assert_eq!(parse_order_status_str("8"), OrderStatus::Cancelled);
        assert_eq!(parse_order_status_str("3"), OrderStatus::PartiallyFilled);
    }

    #[test]
    fn test_signature() {
        let trader = BitmartTrader::new(
            "test_key",
            "test_secret",
            Some("test_memo"),
            false,
            MarketType::Spot,
        );
        let timestamp = 1234567890000i64;
        let signature = trader.sign_request(timestamp, "");
        // Just verify it produces a hex string of correct length (64 chars for SHA256)
        assert_eq!(signature.len(), 64);
        assert!(signature.chars().all(|c| c.is_ascii_hexdigit()));
    }

    #[test]
    fn test_ws_signature() {
        let trader = BitmartTrader::new(
            "test_key",
            "test_secret",
            Some("test_memo"),
            false,
            MarketType::Spot,
        );
        let timestamp = 1234567890000i64;
        let signature = trader.ws_auth_signature(timestamp);
        assert_eq!(signature.len(), 64);
        assert!(signature.chars().all(|c| c.is_ascii_hexdigit()));
    }
}
