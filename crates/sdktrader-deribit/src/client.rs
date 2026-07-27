//! Deribit exchange trader implementation.
//!
//! Full-featured implementation for Deribit derivatives exchange:
//! - HMAC-SHA256 authentication with deri-hmac-sha256 header format
//! - REST API for trading operations
//! - WebSocket support for real-time data
//! - Support for perpetuals, futures, and options

use crate::models::*;
use anyhow::{Context, Result};
use async_trait::async_trait;
use futures_util::{SinkExt, StreamExt};
use reqwest::Client;
use sdktrader_core::models::{
    AccountBalance, ExchangeCapabilities, FundingRateEntry, MarketType, OpenInterestEntry,
    OrderInfo, OrderResult, OrderSide, OrderStatus, OrderType, OrderUpdateEvent, Orderbook,
    OrderbookLevel, TickerInfo, TimeInForce, WsTestResult,
};
use sdktrader_core::utils::{hmac_sign, timestamp_ms};
use sdktrader_core::ExchangeTrader;
use tokio::sync::broadcast;
use tokio::task::JoinHandle;
use tokio_tungstenite::{connect_async, tungstenite::protocol::Message};
use tracing::{debug, error, info, warn};

const MAINNET_URL: &str = "https://www.deribit.com";
const TESTNET_URL: &str = "https://test.deribit.com";
const MAINNET_WS_URL: &str = "wss://www.deribit.com/ws/api/v2";
const TESTNET_WS_URL: &str = "wss://test.deribit.com/ws/api/v2";

/// API version for Deribit.
const API_VERSION: &str = "v2";

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
        "open" | "untriggered" => OrderStatus::New,
        "partially_filled" => OrderStatus::PartiallyFilled,
        "filled" | "closed" => OrderStatus::Filled,
        "cancelled" | "canceled" => OrderStatus::Cancelled,
        "rejected" => OrderStatus::Rejected,
        "expired" => OrderStatus::Expired,
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

/// Deribit exchange trader.
pub struct DeribitTrader {
    client_id: String,
    client_secret: String,
    testnet: bool,
    market_type: MarketType,
    client: Client,
    base_url: String,
}

impl DeribitTrader {
    /// Create a new Deribit trader.
    ///
    /// Note: Deribit uses client_id and client_secret (OAuth2 style) instead of
    /// api_key/api_secret. The api_key parameter maps to client_id and
    /// api_secret maps to client_secret.
    pub fn new(api_key: &str, api_secret: &str, testnet: bool, market_type: MarketType) -> Self {
        let base_url = if testnet { TESTNET_URL } else { MAINNET_URL };

        Self {
            client_id: api_key.to_string(),
            client_secret: api_secret.to_string(),
            testnet,
            market_type,
            client: Client::new(),
            base_url: base_url.to_string(),
        }
    }

    /// Get the WebSocket URL based on testnet setting.
    fn ws_url(&self) -> &str {
        if self.testnet {
            TESTNET_WS_URL
        } else {
            MAINNET_WS_URL
        }
    }

    /// Sign a request using deri-hmac-sha256 format.
    ///
    /// Deribit signature format:
    /// signature = HMAC-SHA256(timestamp + "\n" + nonce + "\n" + method + "\n" + request_path + "\n" + body + "\n")
    fn sign_request(
        &self,
        method: &str,
        request_path: &str,
        body: &str,
    ) -> (String, String, String) {
        let timestamp = timestamp_ms().to_string();
        let nonce = uuid::Uuid::new_v4().to_string();

        // Build the string to sign
        // Format: METHOD\nREQUEST_PATH\nBODY\n
        let request_data = format!("{}\n{}\n{}\n", method, request_path, body);
        // Format: TIMESTAMP\nNONCE\nREQUEST_DATA
        let string_to_sign = format!("{}\n{}\n{}", timestamp, nonce, request_data);

        let signature = hmac_sign(&self.client_secret, &string_to_sign);

        (timestamp, nonce, signature)
    }

    /// Build the Authorization header value.
    fn build_auth_header(&self, timestamp: &str, nonce: &str, signature: &str) -> String {
        format!(
            "deri-hmac-sha256 id={},ts={},sig={},nonce={}",
            self.client_id, timestamp, signature, nonce
        )
    }

    /// Make a public GET request.
    async fn public_get<T: for<'de> serde::Deserialize<'de>>(
        &self,
        endpoint: &str,
        params: &[(&str, &str)],
    ) -> Result<T> {
        let request_path = format!("/api/{}/public/{}", API_VERSION, endpoint);
        let url = if params.is_empty() {
            format!("{}{}", self.base_url, request_path)
        } else {
            let query = params
                .iter()
                .map(|(k, v)| format!("{}={}", k, urlencoding::encode(v)))
                .collect::<Vec<_>>()
                .join("&");
            format!("{}{}?{}", self.base_url, request_path, query)
        };

        debug!("Deribit public GET: {}", url);

        let response = self
            .client
            .get(&url)
            .send()
            .await
            .context("Failed to send request to Deribit")?;

        let text = response.text().await?;
        debug!("Deribit response: {}", text);

        serde_json::from_str(&text).context(format!("Failed to parse Deribit response: {}", text))
    }

    /// Make a private GET request.
    async fn private_get<T: for<'de> serde::Deserialize<'de>>(
        &self,
        endpoint: &str,
        params: &[(&str, &str)],
    ) -> Result<T> {
        let request_path = if params.is_empty() {
            format!("/api/{}/private/{}", API_VERSION, endpoint)
        } else {
            let query = params
                .iter()
                .map(|(k, v)| format!("{}={}", k, urlencoding::encode(v)))
                .collect::<Vec<_>>()
                .join("&");
            format!("/api/{}/private/{}?{}", API_VERSION, endpoint, query)
        };

        let (timestamp, nonce, signature) = self.sign_request("GET", &request_path, "");
        let auth_header = self.build_auth_header(&timestamp, &nonce, &signature);

        let url = format!("{}{}", self.base_url, request_path);
        debug!("Deribit private GET: {}", url);

        let response = self
            .client
            .get(&url)
            .header("Authorization", auth_header)
            .send()
            .await
            .context("Failed to send request to Deribit")?;

        let text = response.text().await?;
        debug!("Deribit response: {}", text);

        serde_json::from_str(&text).context(format!("Failed to parse Deribit response: {}", text))
    }

    /// Get the currency for the current market type (BTC or ETH for futures/options).
    fn get_currency(&self) -> &str {
        // Default to BTC, can be extended based on symbol
        "BTC"
    }

    /// Get the instrument kind based on market type.
    fn get_kind(&self) -> &str {
        match self.market_type {
            MarketType::Swap => "future", // perpetuals are kind=future with settlement_period=perpetual
            MarketType::Spot => "spot",
        }
    }

    /// Get current position for a symbol.
    async fn get_position(&self, symbol: &str) -> Result<Option<PositionData>> {
        let formatted_symbol = self.format_symbol(symbol);
        let params = [("instrument_name", formatted_symbol.as_str())];

        let response: DeribitResponse<PositionData> =
            self.private_get("get_position", &params).await?;

        if response.is_success() {
            if let Some(pos) = response.result {
                if pos.size.abs() > 0.0 {
                    return Ok(Some(pos));
                }
            }
        }

        Ok(None)
    }

    /// Create and authenticate WebSocket connection.
    async fn create_ws_connection(
        &self,
    ) -> Result<
        tokio_tungstenite::WebSocketStream<
            tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>,
        >,
    > {
        let ws_url = self.ws_url();
        let (mut ws_stream, _) = connect_async(ws_url)
            .await
            .context("Failed to connect to Deribit WebSocket")?;

        // Authenticate using client_credentials grant type
        let auth_request = WsAuthRequest {
            jsonrpc: "2.0".to_string(),
            id: 1,
            method: "public/auth".to_string(),
            params: WsAuthParams {
                grant_type: "client_credentials".to_string(),
                client_id: self.client_id.clone(),
                client_secret: self.client_secret.clone(),
            },
        };

        let auth_json = serde_json::to_string(&auth_request)?;
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

                        if let Some(err) = auth_resp.error {
                            anyhow::bail!("WebSocket authentication failed: {} (code: {})", err.message, err.code);
                        }

                        if let Some(result) = auth_resp.result {
                            info!("WebSocket authenticated, token type: {}", result.token_type);
                        }
                    }
                    Some(Ok(Message::Ping(data))) => {
                        ws_stream.send(Message::Pong(data)).await?;
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

    /// Place an order via REST API.
    async fn place_order_rest(
        &self,
        symbol: &str,
        side: OrderSide,
        order_type: OrderType,
        amount: f64,
        price: Option<f64>,
        reduce_only: bool,
    ) -> Result<OrderResult> {
        let formatted_symbol = self.format_symbol(symbol);

        // Build endpoint based on side
        let endpoint = match side {
            OrderSide::Buy => "buy",
            OrderSide::Sell => "sell",
        };

        // Build params
        let amount_str = format!("{:.8}", amount);
        let mut params: Vec<(&str, &str)> = vec![
            ("instrument_name", &formatted_symbol),
            ("amount", &amount_str),
        ];

        let type_str = match order_type {
            OrderType::Market => "market",
            OrderType::Limit => "limit",
            _ => "market",
        };
        params.push(("type", type_str));

        let price_str;
        if let (OrderType::Limit, Some(p)) = (order_type, price) {
            price_str = format!("{:.8}", p);
            params.push(("price", &price_str));
        }

        if reduce_only {
            params.push(("reduce_only", "true"));
        }

        let response: DeribitResponse<DeribitOrderResult> =
            self.private_get(endpoint, &params).await?;

        if response.is_success() {
            if let Some(result) = response.result {
                let order = result.order;
                info!("Deribit order placed: {}", order.order_id);
                return Ok(OrderResult {
                    success: true,
                    order_id: Some(order.order_id),
                    symbol: symbol.to_string(),
                    side,
                    order_type,
                    quantity: Some(amount),
                    entry_price: order.average_price,
                    position_size: Some(order.filled_amount),
                    position_usdt: order.average_price.map(|p| order.filled_amount * p),
                    error: None,
                    raw_response: None,
                });
            }
        }

        let error_msg = if let Some(err) = response.error {
            format!("{} (code: {})", err.message, err.code)
        } else {
            "Unknown error".to_string()
        };

        Ok(OrderResult::failure(symbol.to_string(), side, error_msg))
    }
}

#[async_trait]
impl ExchangeTrader for DeribitTrader {
    fn exchange_name(&self) -> &str {
        "deribit"
    }

    fn market_type(&self) -> MarketType {
        self.market_type
    }

    fn capabilities(&self) -> ExchangeCapabilities {
        sdktrader_core::exchange_capabilities! {
            swap: Supported,
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
        let response: DeribitResponse<ServerTimeResult> = self.public_get("get_time", &[]).await?;

        Ok(response.is_success())
    }

    async fn get_server_time(&self) -> Result<i64> {
        let response: DeribitResponse<ServerTimeResult> = self.public_get("get_time", &[]).await?;

        response.result.ok_or_else(|| {
            let err_msg = response
                .error
                .map_or("Unknown error".to_string(), |e| e.message);
            anyhow::anyhow!("Failed to get server time: {}", err_msg)
        })
    }

    async fn get_balance(&self) -> Result<AccountBalance> {
        let currency = self.get_currency();
        let params = [("currency", currency), ("extended", "true")];

        let response: DeribitResponse<AccountSummaryResult> =
            self.private_get("get_account_summary", &params).await?;

        if let Some(result) = response.result {
            return Ok(AccountBalance::simple(
                result.available_funds,
                result.equity,
                &result.currency,
            ));
        }

        // If no balance found, return zero balance
        Ok(AccountBalance::default())
    }

    async fn get_current_price(&self, symbol: &str) -> Result<f64> {
        let formatted_symbol = self.format_symbol(symbol);
        let params = [("instrument_name", formatted_symbol.as_str())];

        let response: DeribitResponse<TickerResult> = self.public_get("ticker", &params).await?;

        if let Some(result) = response.result {
            if let Some(price) = result.last_price {
                return Ok(price);
            }
            if let Some(price) = result.mark_price {
                return Ok(price);
            }
        }

        let err_msg = response
            .error
            .map_or("No price available".to_string(), |e| e.message);
        anyhow::bail!("Failed to get price for {}: {}", symbol, err_msg)
    }

    async fn get_ticker(&self, symbol: &str) -> Result<TickerInfo> {
        let formatted_symbol = self.format_symbol(symbol);
        let params = [("instrument_name", formatted_symbol.as_str())];

        let response: DeribitResponse<TickerResult> = self.public_get("ticker", &params).await?;

        if let Some(t) = response.result {
            let (high_24h, low_24h, volume_24h) = if let Some(stats) = &t.stats {
                (stats.high, stats.low, stats.volume)
            } else {
                (None, None, None)
            };

            return Ok(TickerInfo {
                symbol: t.instrument_name,
                last_price: t.last_price.unwrap_or(0.0),
                bid_price: t.best_bid_price,
                ask_price: t.best_ask_price,
                high_24h,
                low_24h,
                volume_24h,
                turnover_24h: t.stats.as_ref().and_then(|s| s.volume_usd),
                change_24h: t.stats.as_ref().and_then(|s| s.price_change),
                open_interest: t.open_interest,
                open_interest_value: None,
                funding_rate: t.current_funding,
                next_funding_time: None,
                mark_price: t.mark_price,
                index_price: t.index_price,
                timestamp: t.timestamp,
            });
        }

        let err_msg = response
            .error
            .map_or("No ticker data".to_string(), |e| e.message);
        anyhow::bail!("Failed to get ticker for {}: {}", symbol, err_msg)
    }

    async fn get_orderbook(&self, symbol: &str, depth: u32) -> Result<Orderbook> {
        let formatted_symbol = self.format_symbol(symbol);
        let depth_str = depth.to_string();
        let params = [
            ("instrument_name", formatted_symbol.as_str()),
            ("depth", depth_str.as_str()),
        ];

        let response: DeribitResponse<OrderbookResult> =
            self.public_get("get_order_book", &params).await?;

        if let Some(result) = response.result {
            let bids: Vec<OrderbookLevel> = result
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

            let asks: Vec<OrderbookLevel> = result
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
                symbol: result.instrument_name,
                bids,
                asks,
                timestamp: result.timestamp,
            });
        }

        let err_msg = response
            .error
            .map_or("No orderbook data".to_string(), |e| e.message);
        anyhow::bail!("Failed to get orderbook: {}", err_msg)
    }

    async fn set_leverage(&self, _symbol: Option<&str>, _leverage: u32) -> Result<bool> {
        // Deribit uses portfolio margin or per-instrument leverage set at account level
        // Leverage is typically controlled via position sizing, not explicit API call
        // Return true as leverage is handled differently
        Ok(true)
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
        // For Deribit perpetuals and futures, amount is typically in USD
        // For inverse contracts: position_size = USD_amount
        // For linear contracts: position_size = USD_amount / price
        let quantity = if formatted_symbol.contains("PERPETUAL") || formatted_symbol.contains("-") {
            // For inverse perpetuals like BTC-PERPETUAL, amount is in USD
            usdt_amount * leverage as f64
        } else {
            // Fallback calculation
            usdt_amount / price * leverage as f64
        };

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

        // Note: Deribit doesn't have native TP/SL like some other exchanges
        // Would need to place separate stop orders if needed
        if order_result.success && (tp_percent.is_some() || sl_percent.is_some()) {
            warn!("TP/SL orders are not automatically placed for Deribit. Consider placing separate stop orders.");
        }

        Ok(order_result)
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

    async fn cancel_order(&self, _symbol: &str, order_id: &str) -> Result<bool> {
        let params = [("order_id", order_id)];

        let response: DeribitResponse<CancelOrderResult> =
            self.private_get("cancel", &params).await?;

        if response.is_success() {
            info!("Deribit order cancelled: {}", order_id);
            Ok(true)
        } else {
            let err_msg = response
                .error
                .map_or("Unknown error".to_string(), |e| e.message);
            error!("Failed to cancel order {}: {}", order_id, err_msg);
            Ok(false)
        }
    }

    async fn get_order_status(&self, _symbol: &str, order_id: &str) -> Result<OrderInfo> {
        let params = [("order_id", order_id)];

        let response: DeribitResponse<GetOrderStateResult> =
            self.private_get("get_order_state", &params).await?;

        if let Some(order) = response.result {
            let side = if order.direction == "buy" {
                OrderSide::Buy
            } else {
                OrderSide::Sell
            };

            let order_type = match order.order_type.as_str() {
                "limit" | "stop_limit" | "take_limit" => OrderType::Limit,
                _ => OrderType::Market,
            };

            let time_in_force = match order.time_in_force.as_str() {
                "fill_or_kill" => TimeInForce::FOK,
                "immediate_or_cancel" => TimeInForce::IOC,
                _ => TimeInForce::GTC,
            };

            return Ok(OrderInfo {
                order_id: order.order_id,
                client_order_id: order.label,
                symbol: order.instrument_name,
                status: parse_order_status_str(&order.order_state),
                side,
                order_type,
                price: order.price.unwrap_or(0.0),
                quantity: order.amount,
                filled_quantity: order.filled_amount,
                remaining_quantity: order.amount - order.filled_amount,
                avg_price: order.average_price,
                timestamp: order.creation_timestamp,
                time_in_force,
                reduce_only: order.reduce_only,
            });
        }

        let err_msg = response
            .error
            .map_or("Order not found".to_string(), |e| e.message);
        anyhow::bail!("Failed to get order status: {}", err_msg)
    }

    async fn close_position(&self, symbol: &str) -> Result<OrderResult> {
        let formatted_symbol = self.format_symbol(symbol);

        // Get current position
        let position = self.get_position(&formatted_symbol).await?;

        match position {
            Some(pos) => {
                if pos.size.abs() < 0.000001 {
                    return Ok(OrderResult::failure(
                        formatted_symbol,
                        OrderSide::Sell,
                        "No position to close".to_string(),
                    ));
                }

                // Determine close side (opposite of position direction)
                let close_side = if pos.direction == "buy" {
                    OrderSide::Sell
                } else {
                    OrderSide::Buy
                };

                // Place market order to close position with reduce_only flag
                self.place_order_rest(
                    &formatted_symbol,
                    close_side,
                    OrderType::Market,
                    pos.size.abs(),
                    None,
                    true, // reduce_only
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

        // If already in Deribit format (e.g., BTC-PERPETUAL, ETH-27DEC24-2000-C)
        if symbol.contains("-") {
            return symbol;
        }

        // Handle common formats
        if symbol.contains("/") {
            // BTC/USD -> BTC-PERPETUAL
            let parts: Vec<&str> = symbol.split('/').collect();
            if let Some(base) = parts.first() {
                return format!("{}-PERPETUAL", base);
            }
        }

        // Handle BTCUSDT or similar -> BTC-PERPETUAL
        if symbol.ends_with("USDT") || symbol.ends_with("USD") {
            let base = symbol.trim_end_matches("USDT").trim_end_matches("USD");
            return format!("{}-PERPETUAL", base);
        }

        // Handle just BTC -> BTC-PERPETUAL
        if symbol == "BTC" || symbol == "ETH" || symbol == "SOL" {
            return format!("{}-PERPETUAL", symbol);
        }

        // Default: assume it's a base currency for perpetual
        format!("{}-PERPETUAL", symbol)
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
        let now = timestamp_ms();
        let month_ago = now - 30 * 24 * 60 * 60 * 1000;

        let start_str = month_ago.to_string();
        let end_str = now.to_string();

        let params = [
            ("instrument_name", formatted_symbol.as_str()),
            ("start_timestamp", start_str.as_str()),
            ("end_timestamp", end_str.as_str()),
        ];

        let response: DeribitResponse<FundingRateHistoryResult> =
            self.public_get("get_funding_rate_history", &params).await?;

        if let Some(result) = response.result {
            let entries: Vec<FundingRateEntry> = result
                .iter()
                .take(limit as usize)
                .map(|e| FundingRateEntry {
                    symbol: formatted_symbol.clone(),
                    funding_rate: e.interest_8h,
                    funding_time: e.timestamp,
                })
                .collect();
            return Ok(entries);
        }

        let err_msg = response
            .error
            .map_or("No funding rate data".to_string(), |e| e.message);
        anyhow::bail!("Failed to get funding history: {}", err_msg)
    }

    async fn get_open_interest_history(
        &self,
        symbol: &str,
        _interval: &str,
        _limit: u32,
    ) -> Result<Vec<OpenInterestEntry>> {
        // Deribit doesn't have a historical open interest endpoint
        // We can only get current OI from ticker
        let formatted_symbol = self.format_symbol(symbol);

        let ticker = self.get_ticker(&formatted_symbol).await?;

        let entries = vec![OpenInterestEntry {
            symbol: formatted_symbol,
            open_interest: ticker.open_interest.unwrap_or(0.0),
            open_interest_value: 0.0,
            timestamp: ticker.timestamp,
        }];

        Ok(entries)
    }

    async fn test_ws_orderbook(
        &self,
        symbol: &str,
        depth: u32,
        timeout_ms: u64,
    ) -> Result<WsTestResult> {
        let formatted_symbol = self.format_symbol(symbol);
        let ws_url = self.ws_url();

        let start = std::time::Instant::now();
        let (mut ws_stream, _) = connect_async(ws_url)
            .await
            .context("Failed to connect to Deribit WebSocket")?;

        let connect_time = start.elapsed().as_millis() as u64;

        // Subscribe to orderbook
        let group = if depth <= 10 { "10" } else { "none" };
        let channel = format!("book.{}.{}.100ms", formatted_symbol, group);

        let subscribe_msg = serde_json::json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "public/subscribe",
            "params": {
                "channels": [channel]
            }
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
                                if parsed.params.is_some() {
                                    if let Some(params) = parsed.params {
                                        data_sample = params.data;
                                        break;
                                    }
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
            subscription_type: format!("book.{}", formatted_symbol),
            data_sample,
        })
    }

    async fn test_ws_trades(&self, symbol: &str, timeout_ms: u64) -> Result<WsTestResult> {
        let formatted_symbol = self.format_symbol(symbol);
        let ws_url = self.ws_url();

        let start = std::time::Instant::now();
        let (mut ws_stream, _) = connect_async(ws_url)
            .await
            .context("Failed to connect to Deribit WebSocket")?;

        let connect_time = start.elapsed().as_millis() as u64;

        // Subscribe to trades
        let channel = format!("trades.{}.100ms", formatted_symbol);

        let subscribe_msg = serde_json::json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "public/subscribe",
            "params": {
                "channels": [channel]
            }
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
                                if let Some(params) = parsed.params {
                                    if params.data.is_some() {
                                        data_sample = params.data;
                                        break;
                                    }
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
            subscription_type: format!("trades.{}", formatted_symbol),
            data_sample,
        })
    }

    async fn test_ws_ticker(&self, symbol: &str, timeout_ms: u64) -> Result<WsTestResult> {
        let formatted_symbol = self.format_symbol(symbol);
        let ws_url = self.ws_url();

        let start = std::time::Instant::now();
        let (mut ws_stream, _) = connect_async(ws_url)
            .await
            .context("Failed to connect to Deribit WebSocket")?;

        let connect_time = start.elapsed().as_millis() as u64;

        // Subscribe to ticker
        let channel = format!("ticker.{}.100ms", formatted_symbol);

        let subscribe_msg = serde_json::json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "public/subscribe",
            "params": {
                "channels": [channel]
            }
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
                                if let Some(params) = parsed.params {
                                    if params.data.is_some() {
                                        data_sample = params.data;
                                        break;
                                    }
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
            subscription_type: format!("ticker.{} (includes OI, fundingRate)", formatted_symbol),
            data_sample,
        })
    }

    async fn subscribe_order_updates(
        &self,
        sender: broadcast::Sender<OrderUpdateEvent>,
    ) -> Result<JoinHandle<()>> {
        let client_id = self.client_id.clone();
        let client_secret = self.client_secret.clone();
        let testnet = self.testnet;
        let exchange_name = "deribit".to_string();

        let handle = tokio::spawn(async move {
            let mut reconnect_delay = INITIAL_RECONNECT_DELAY_SECS;

            loop {
                let ws_url = if testnet {
                    TESTNET_WS_URL
                } else {
                    MAINNET_WS_URL
                };

                info!("[{}] Connecting to private WS: {}", exchange_name, ws_url);

                match connect_async(ws_url).await {
                    Ok((mut ws_stream, _)) => {
                        // Authenticate using client_credentials
                        let auth_request = serde_json::json!({
                            "jsonrpc": "2.0",
                            "id": 1,
                            "method": "public/auth",
                            "params": {
                                "grant_type": "client_credentials",
                                "client_id": client_id,
                                "client_secret": client_secret
                            }
                        });

                        if let Err(e) = ws_stream
                            .send(Message::Text(auth_request.to_string()))
                            .await
                        {
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
                                if resp.get("result").is_some() && resp.get("error").is_none() {
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
                            "jsonrpc": "2.0",
                            "id": 2,
                            "method": "private/subscribe",
                            "params": {
                                "channels": [
                                    "user.orders.any.any.raw",
                                    "user.trades.any.any.raw"
                                ]
                            }
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

                        // Main message loop with heartbeat
                        let mut heartbeat_interval =
                            tokio::time::interval(tokio::time::Duration::from_secs(15));

                        loop {
                            tokio::select! {
                                msg = ws_stream.next() => {
                                    match msg {
                                        Some(Ok(Message::Text(text))) => {
                                            if let Ok(msg_json) = serde_json::from_str::<serde_json::Value>(&text) {
                                                // Handle heartbeat response
                                                if msg_json.get("id").is_some() {
                                                    debug!("[{}] WS response: {}", exchange_name, text);
                                                    continue;
                                                }

                                                // Process subscription data
                                                if let Some(params) = msg_json.get("params") {
                                                    let channel = params.get("channel").and_then(|c| c.as_str());

                                                    if let Some(ch) = channel {
                                                        if ch.starts_with("user.orders") {
                                                            if let Some(data) = params.get("data") {
                                                                // Parse order notification
                                                                let order_id = data.get("order_id")
                                                                    .and_then(|v| v.as_str())
                                                                    .unwrap_or("")
                                                                    .to_string();
                                                                let symbol = data.get("instrument_name")
                                                                    .and_then(|v| v.as_str())
                                                                    .unwrap_or("")
                                                                    .to_string();
                                                                let status_str = data.get("order_state")
                                                                    .and_then(|v| v.as_str())
                                                                    .unwrap_or("open");
                                                                let side_str = data.get("direction")
                                                                    .and_then(|v| v.as_str())
                                                                    .unwrap_or("buy");
                                                                let order_type_str = data.get("order_type")
                                                                    .and_then(|v| v.as_str())
                                                                    .unwrap_or("market");
                                                                let price = data.get("price")
                                                                    .and_then(|v| v.as_f64())
                                                                    .unwrap_or(0.0);
                                                                let amount = data.get("amount")
                                                                    .and_then(|v| v.as_f64())
                                                                    .unwrap_or(0.0);
                                                                let filled = data.get("filled_amount")
                                                                    .and_then(|v| v.as_f64())
                                                                    .unwrap_or(0.0);
                                                                let avg_price = data.get("average_price")
                                                                    .and_then(|v| v.as_f64());
                                                                let created_time = data.get("creation_timestamp")
                                                                    .and_then(|v| v.as_i64())
                                                                    .unwrap_or(0);
                                                                let commission = data.get("commission")
                                                                    .and_then(|v| v.as_f64());

                                                                let event = OrderUpdateEvent {
                                                                    exchange: exchange_name.clone(),
                                                                    order_id,
                                                                    client_order_id: data.get("label")
                                                                        .and_then(|v| v.as_str())
                                                                        .map(|s| s.to_string()),
                                                                    symbol,
                                                                    status: parse_order_status_str(status_str),
                                                                    side: if side_str == "buy" { OrderSide::Buy } else { OrderSide::Sell },
                                                                    order_type: if order_type_str == "market" { OrderType::Market } else { OrderType::Limit },
                                                                    price,
                                                                    quantity: amount,
                                                                    filled_quantity: filled,
                                                                    avg_fill_price: avg_price,
                                                                    fee: commission,
                                                                    fee_currency: None,
                                                                    exchange_timestamp: created_time,
                                                                    received_at: timestamp_ms(),
                                                                    raw: Some(data.clone()),
                                                                };

                                                                log_order_update(&event);
                                                                let _ = sender.send(event);
                                                            }
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
                                _ = heartbeat_interval.tick() => {
                                    // Send heartbeat/test request
                                    let heartbeat = serde_json::json!({
                                        "jsonrpc": "2.0",
                                        "id": timestamp_ms(),
                                        "method": "public/test",
                                        "params": {}
                                    });
                                    if let Err(e) = ws_stream.send(Message::Text(heartbeat.to_string())).await {
                                        error!("[{}] Failed to send heartbeat: {}", exchange_name, e);
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
        let trader = DeribitTrader::new("", "", false, MarketType::Swap);

        assert_eq!(trader.format_symbol("BTC"), "BTC-PERPETUAL");
        assert_eq!(trader.format_symbol("BTCUSDT"), "BTC-PERPETUAL");
        assert_eq!(trader.format_symbol("BTC/USD"), "BTC-PERPETUAL");
        assert_eq!(trader.format_symbol("ETH"), "ETH-PERPETUAL");
        assert_eq!(trader.format_symbol("BTC-PERPETUAL"), "BTC-PERPETUAL");
        assert_eq!(
            trader.format_symbol("BTC-27DEC24-100000-C"),
            "BTC-27DEC24-100000-C"
        );
    }

    #[test]
    fn test_parse_order_status() {
        assert_eq!(parse_order_status_str("open"), OrderStatus::New);
        assert_eq!(parse_order_status_str("filled"), OrderStatus::Filled);
        assert_eq!(parse_order_status_str("cancelled"), OrderStatus::Cancelled);
        assert_eq!(parse_order_status_str("rejected"), OrderStatus::Rejected);
        assert_eq!(parse_order_status_str("expired"), OrderStatus::Expired);
        assert_eq!(parse_order_status_str("untriggered"), OrderStatus::New);
    }

    #[test]
    fn test_next_reconnect_delay() {
        assert_eq!(next_reconnect_delay(1), 2);
        assert_eq!(next_reconnect_delay(2), 4);
        assert_eq!(next_reconnect_delay(16), 30); // Capped at MAX
        assert_eq!(next_reconnect_delay(30), 30); // Already at max
    }
}
