//! BingX exchange trader implementation.
//!
//! Full-featured implementation with:
//! - Swap and Spot market support
//! - Testnet support
//! - Full close_position implementation
//! - Private WebSocket subscription for order updates
//! - Proper error handling with BingX error codes

use super::models::*;
use sdktrader_core::utils::{hmac_sign, timestamp_ms};
use sdktrader_core::ExchangeTrader;
use sdktrader_core::models::{
    AccountBalance, ExchangeCapabilities, FundingRateEntry, MarketType, OpenInterestEntry,
    OrderInfo, OrderResult, OrderSide, OrderStatus, OrderType, OrderUpdateEvent, Orderbook,
    OrderbookLevel, TickerInfo, TimeInForce, WsTestResult,
};
use anyhow::{Context, Result};
use async_trait::async_trait;
use flate2::read::GzDecoder;
use futures_util::{SinkExt, StreamExt};
use reqwest::Client;
use std::io::Read;
use tokio::sync::broadcast;
use tokio::task::JoinHandle;
use tokio_tungstenite::{connect_async, tungstenite::protocol::Message};
use tracing::{debug, error, info, warn};

// Mainnet URLs
const MAINNET_URL: &str = "https://open-api.bingx.com";
const MAINNET_WS_SWAP_URL: &str = "wss://open-api-swap.bingx.com/swap-market";
const MAINNET_WS_SPOT_URL: &str = "wss://open-api-ws.bingx.com/market";
const MAINNET_WS_USER_URL: &str = "wss://open-api-ws.bingx.com/market";

// Testnet URLs (VST = Virtual Standard Trading)
const TESTNET_URL: &str = "https://open-api-vst.bingx.com";
const TESTNET_WS_SWAP_URL: &str = "wss://open-api-vst.bingx.com/swap-market";

/// Maximum reconnect backoff delay in seconds.
const MAX_RECONNECT_DELAY_SECS: u64 = 30;

/// Initial reconnect delay in seconds.
const INITIAL_RECONNECT_DELAY_SECS: u64 = 1;

/// BingX exchange trader.
pub struct BingxTrader {
    api_key: String,
    api_secret: String,
    testnet: bool,
    market_type: MarketType,
    client: Client,
    base_url: String,
}

impl BingxTrader {
    /// Create a new BingX trader.
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

    fn sign(&self, params: &str) -> String {
        hmac_sign(&self.api_secret, params)
    }

    fn decompress(&self, data: &[u8]) -> Result<String> {
        let mut decoder = GzDecoder::new(data);
        let mut s = String::new();
        decoder.read_to_string(&mut s)?;
        Ok(s)
    }

    /// Get WebSocket URL for public data.
    fn ws_public_url(&self) -> &str {
        match (self.testnet, self.market_type) {
            (true, MarketType::Swap) => TESTNET_WS_SWAP_URL,
            (false, MarketType::Swap) => MAINNET_WS_SWAP_URL,
            (_, MarketType::Spot) => MAINNET_WS_SPOT_URL,
        }
    }

    /// Get API path prefix based on market type.
    fn api_path(&self) -> &str {
        match self.market_type {
            MarketType::Swap => "/openApi/swap/v2",
            MarketType::Spot => "/openApi/spot/v1",
        }
    }

    /// Make a signed GET request.
    async fn signed_get<T: for<'de> serde::Deserialize<'de>>(
        &self,
        endpoint: &str,
        params: &str,
    ) -> Result<BingxResponse<T>> {
        let timestamp = timestamp_ms();
        let full_params = if params.is_empty() {
            format!("timestamp={}", timestamp)
        } else {
            format!("{}&timestamp={}", params, timestamp)
        };
        let signature = self.sign(&full_params);

        let url = format!(
            "{}{}?{}&signature={}",
            self.base_url, endpoint, full_params, signature
        );

        let response = self
            .client
            .get(&url)
            .header("X-BX-APIKEY", &self.api_key)
            .send()
            .await
            .context("Failed to send request to BingX")?;

        let text = response.text().await?;
        debug!("BingX response: {}", text);

        serde_json::from_str(&text).context(format!("Failed to parse BingX response: {}", text))
    }

    /// Make a signed POST request.
    async fn signed_post<T: for<'de> serde::Deserialize<'de>>(
        &self,
        endpoint: &str,
        params: &str,
    ) -> Result<BingxResponse<T>> {
        let timestamp = timestamp_ms();
        let full_params = if params.is_empty() {
            format!("timestamp={}", timestamp)
        } else {
            format!("{}&timestamp={}", params, timestamp)
        };
        let signature = self.sign(&full_params);

        let url = format!(
            "{}{}?{}&signature={}",
            self.base_url, endpoint, full_params, signature
        );

        let response = self
            .client
            .post(&url)
            .header("X-BX-APIKEY", &self.api_key)
            .send()
            .await
            .context("Failed to send request to BingX")?;

        let text = response.text().await?;
        debug!("BingX response: {}", text);

        serde_json::from_str(&text).context(format!("Failed to parse BingX response: {}", text))
    }

    /// Make a signed DELETE request.
    async fn signed_delete<T: for<'de> serde::Deserialize<'de>>(
        &self,
        endpoint: &str,
        params: &str,
    ) -> Result<BingxResponse<T>> {
        let timestamp = timestamp_ms();
        let full_params = if params.is_empty() {
            format!("timestamp={}", timestamp)
        } else {
            format!("{}&timestamp={}", params, timestamp)
        };
        let signature = self.sign(&full_params);

        let url = format!(
            "{}{}?{}&signature={}",
            self.base_url, endpoint, full_params, signature
        );

        let response = self
            .client
            .delete(&url)
            .header("X-BX-APIKEY", &self.api_key)
            .send()
            .await
            .context("Failed to send request to BingX")?;

        let text = response.text().await?;
        debug!("BingX response: {}", text);

        serde_json::from_str(&text).context(format!("Failed to parse BingX response: {}", text))
    }

    /// Get current position for a symbol.
    pub async fn get_position(&self, symbol: &str) -> Result<Option<PositionData>> {
        if self.market_type == MarketType::Spot {
            return Ok(None);
        }

        let params = format!("symbol={}", symbol);
        let response: BingxResponse<Vec<PositionData>> = self
            .signed_get("/openApi/swap/v2/user/positions", &params)
            .await?;

        if !response.is_success() {
            anyhow::bail!("BingX API error: {}", response.error_message());
        }

        if let Some(positions) = response.data {
            for pos in positions {
                let size: f64 = pos.position_amt.parse().unwrap_or(0.0);
                if size.abs() > 0.000001 {
                    return Ok(Some(pos));
                }
            }
        }

        Ok(None)
    }
}

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

/// Parse a status string from BingX into OrderStatus.
fn parse_order_status_str(s: &str) -> OrderStatus {
    match s.to_uppercase().as_str() {
        "NEW" | "PENDING" | "WORKING" => OrderStatus::New,
        "PARTIALLY_FILLED" => OrderStatus::PartiallyFilled,
        "FILLED" => OrderStatus::Filled,
        "CANCELLED" | "CANCELED" => OrderStatus::Cancelled,
        "REJECTED" | "FAILED" => OrderStatus::Rejected,
        "EXPIRED" => OrderStatus::Expired,
        _ => {
            warn!("Unknown BingX order status: '{}'", s);
            OrderStatus::New
        }
    }
}

#[async_trait]
impl ExchangeTrader for BingxTrader {
    fn exchange_name(&self) -> &str {
        "bingx"
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
        let mut retries = 3;
        loop {
            let result = self
                .client
                .get(format!("{}/openApi/swap/v2/server/time", self.base_url))
                .send()
                .await;

            match result {
                Ok(response) => return Ok(response.status().is_success()),
                Err(e) => {
                    if retries <= 1 {
                        return Err(e.into());
                    }
                    retries -= 1;
                    tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;
                }
            }
        }
    }

    async fn get_server_time(&self) -> Result<i64> {
        let response: BingxResponse<ServerTimeData> = self
            .client
            .get(format!("{}/openApi/swap/v2/server/time", self.base_url))
            .send()
            .await?
            .json()
            .await?;

        if !response.is_success() {
            anyhow::bail!("BingX API error: {}", response.error_message());
        }

        response
            .data
            .map(|d| d.server_time)
            .ok_or_else(|| anyhow::anyhow!("Failed to get server time"))
    }

    async fn get_balance(&self) -> Result<AccountBalance> {
        match self.market_type {
            MarketType::Swap => {
                let response: BingxResponse<SwapBalanceData> =
                    self.signed_get("/openApi/swap/v2/user/balance", "").await?;

                if !response.is_success() {
                    anyhow::bail!("BingX API error: {}", response.error_message());
                }

                if let Some(data) = response.data {
                    if let Some(balance) = data.balance {
                        let available = balance
                            .available_margin
                            .as_ref()
                            .and_then(|s| s.parse::<f64>().ok())
                            .unwrap_or(0.0);
                        let equity = balance
                            .equity
                            .as_ref()
                            .and_then(|s| s.parse::<f64>().ok())
                            .unwrap_or(available);
                        let used_margin = balance
                            .used_margin
                            .as_ref()
                            .and_then(|s| s.parse::<f64>().ok());
                        let unrealized_pnl = balance
                            .unrealized_profit
                            .as_ref()
                            .and_then(|s| s.parse::<f64>().ok());
                        let realized_pnl = balance
                            .realised_profit
                            .as_ref()
                            .and_then(|s| s.parse::<f64>().ok());

                        return Ok(AccountBalance {
                            total_balance: equity,
                            available_balance: available,
                            currency: "USDT".to_string(),
                            margin_balance: Some(equity),
                            used_margin,
                            unrealized_pnl,
                            realized_pnl,
                            balances: vec![],
                        });
                    }
                }

                Ok(AccountBalance::default())
            }
            MarketType::Spot => {
                let response: BingxResponse<SpotBalanceData> =
                    self.signed_get("/openApi/spot/v1/account/balance", "").await?;

                if !response.is_success() {
                    anyhow::bail!("BingX API error: {}", response.error_message());
                }

                let mut total_usdt = 0.0;
                let mut available_usdt = 0.0;
                let mut balances = vec![];

                if let Some(data) = response.data {
                    for asset in data.balances {
                        let free: f64 = asset.free.parse().unwrap_or(0.0);
                        let locked: f64 = asset.locked.parse().unwrap_or(0.0);

                        if asset.asset == "USDT" {
                            total_usdt = free + locked;
                            available_usdt = free;
                        }

                        if free > 0.0 || locked > 0.0 {
                            balances.push(sdktrader_core::models::Balance {
                                asset: asset.asset,
                                free,
                                locked,
                            });
                        }
                    }
                }

                Ok(AccountBalance {
                    total_balance: total_usdt,
                    available_balance: available_usdt,
                    currency: "USDT".to_string(),
                    margin_balance: None,
                    used_margin: None,
                    unrealized_pnl: None,
                    realized_pnl: None,
                    balances,
                })
            }
        }
    }

    async fn get_current_price(&self, symbol: &str) -> Result<f64> {
        let formatted_symbol = self.format_symbol(symbol);
        let endpoint = match self.market_type {
            MarketType::Swap => "/openApi/swap/v2/quote/price",
            MarketType::Spot => "/openApi/spot/v1/ticker/price",
        };

        let response: BingxResponse<PriceData> = self
            .client
            .get(format!(
                "{}{}?symbol={}",
                self.base_url, endpoint, formatted_symbol
            ))
            .send()
            .await?
            .json()
            .await?;

        if !response.is_success() {
            anyhow::bail!("BingX API error: {}", response.error_message());
        }

        let price = response
            .data
            .and_then(|d| d.price.parse::<f64>().ok())
            .ok_or_else(|| anyhow::anyhow!("Failed to get price for {}", symbol))?;

        Ok(price)
    }

    async fn set_leverage(&self, symbol: Option<&str>, leverage: u32) -> Result<bool> {
        if self.market_type == MarketType::Spot {
            return Ok(true);
        }

        let symbol = symbol.ok_or_else(|| anyhow::anyhow!("Symbol required for leverage"))?;
        let formatted_symbol = self.format_symbol(symbol);
        let params = format!("symbol={}&side=BOTH&leverage={}", formatted_symbol, leverage);

        let response: BingxResponse<LeverageData> = self
            .signed_post("/openApi/swap/v2/trade/leverage", &params)
            .await?;

        if !response.is_success() {
            // 80014 = leverage not changed (already at requested level)
            if response.code == 80014 {
                return Ok(true);
            }
            warn!("Failed to set leverage: {}", response.error_message());
            return Ok(false);
        }

        info!(
            "BingX leverage set to {}x for {}",
            leverage, formatted_symbol
        );
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

        let quantity = if self.market_type == MarketType::Swap {
            usdt_amount / price * leverage as f64
        } else {
            usdt_amount / price
        };

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
        let side_str = match side {
            OrderSide::Buy => "BUY",
            OrderSide::Sell => "SELL",
        };

        let endpoint = match self.market_type {
            MarketType::Swap => "/openApi/swap/v2/trade/order",
            MarketType::Spot => "/openApi/spot/v1/trade/order",
        };

        let params = format!(
            "symbol={}&side={}&type=MARKET&quantity={:.8}",
            formatted_symbol, side_str, quantity
        );

        let response: BingxResponse<OrderData> = self.signed_post(endpoint, &params).await?;

        if !response.is_success() {
            return Ok(OrderResult::failure(
                formatted_symbol,
                side,
                response.error_message(),
            ));
        }

        let order_id = response
            .data
            .map(|d| d.order_id_string())
            .unwrap_or_default();

        info!("BingX market order placed: {}", order_id);

        Ok(OrderResult {
            success: true,
            order_id: Some(order_id),
            symbol: formatted_symbol,
            side,
            order_type: OrderType::Market,
            quantity: Some(quantity),
            entry_price: None,
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
        let formatted_symbol = self.format_symbol(symbol);
        let side_str = match side {
            OrderSide::Buy => "BUY",
            OrderSide::Sell => "SELL",
        };

        let endpoint = match self.market_type {
            MarketType::Swap => "/openApi/swap/v2/trade/order",
            MarketType::Spot => "/openApi/spot/v1/trade/order",
        };

        let params = format!(
            "symbol={}&side={}&type=LIMIT&quantity={:.8}&price={:.8}",
            formatted_symbol, side_str, quantity, price
        );

        let response: BingxResponse<OrderData> = self.signed_post(endpoint, &params).await?;

        if !response.is_success() {
            return Ok(OrderResult::failure(
                formatted_symbol,
                side,
                response.error_message(),
            ));
        }

        let order_id = response
            .data
            .map(|d| d.order_id_string())
            .unwrap_or_default();

        info!("BingX limit order placed: {}", order_id);

        Ok(OrderResult {
            success: true,
            order_id: Some(order_id),
            symbol: formatted_symbol,
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
        let formatted_symbol = self.format_symbol(symbol);

        let endpoint = match self.market_type {
            MarketType::Swap => "/openApi/swap/v2/trade/order",
            MarketType::Spot => "/openApi/spot/v1/trade/cancel",
        };

        let params = format!("symbol={}&orderId={}", formatted_symbol, order_id);

        let response: BingxResponse<serde_json::Value> =
            self.signed_delete(endpoint, &params).await?;

        if !response.is_success() {
            warn!("Failed to cancel order {}: {}", order_id, response.error_message());
            return Ok(false);
        }

        info!("BingX order cancelled: {}", order_id);
        Ok(true)
    }

    async fn get_order_status(&self, symbol: &str, order_id: &str) -> Result<OrderInfo> {
        let formatted_symbol = self.format_symbol(symbol);

        let endpoint = match self.market_type {
            MarketType::Swap => "/openApi/swap/v2/trade/order",
            MarketType::Spot => "/openApi/spot/v1/trade/query",
        };

        let params = format!("symbol={}&orderId={}", formatted_symbol, order_id);

        let response: BingxResponse<OrderData> = self.signed_get(endpoint, &params).await?;

        if !response.is_success() {
            anyhow::bail!("BingX API error: {}", response.error_message());
        }

        let data = response
            .data
            .ok_or_else(|| anyhow::anyhow!("Order not found"))?;

        let status_str = data.status.as_deref().unwrap_or("NEW");
        let status = parse_order_status_str(status_str);

        let side_str = data.side.as_deref().unwrap_or("BUY");
        let side = if side_str == "SELL" {
            OrderSide::Sell
        } else {
            OrderSide::Buy
        };

        let order_type_str = data.order_type.as_deref().unwrap_or("MARKET");
        let order_type = if order_type_str == "LIMIT" {
            OrderType::Limit
        } else {
            OrderType::Market
        };

        let price = data
            .price
            .as_ref()
            .and_then(|p| p.parse::<f64>().ok())
            .unwrap_or(0.0);

        let quantity = data
            .orig_qty
            .as_ref()
            .and_then(|q| q.parse::<f64>().ok())
            .unwrap_or(0.0);

        let filled_quantity = data
            .executed_qty
            .as_ref()
            .and_then(|q| q.parse::<f64>().ok())
            .unwrap_or(0.0);

        let avg_price = data.avg_price.as_ref().and_then(|p| p.parse::<f64>().ok());

        let timestamp = data.time.unwrap_or(0);

        Ok(OrderInfo {
            order_id: order_id.to_string(),
            client_order_id: data.client_order_id,
            symbol: formatted_symbol,
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
            reduce_only: false,
        })
    }

    async fn close_position(&self, symbol: &str) -> Result<OrderResult> {
        if self.market_type == MarketType::Spot {
            anyhow::bail!("Close position not supported for spot trading");
        }

        let formatted_symbol = self.format_symbol(symbol);

        // First try to get position info
        let position = self.get_position(&formatted_symbol).await?;

        match position {
            Some(pos) => {
                let size: f64 = pos.position_amt.parse().context("Failed to parse position size")?;
                if size.abs() < 0.000001 {
                    return Ok(OrderResult::failure(
                        formatted_symbol,
                        OrderSide::Sell,
                        "No position to close".to_string(),
                    ));
                }

                // BingX has a one-click close all positions endpoint for a symbol
                let params = format!("symbol={}", formatted_symbol);

                let response: BingxResponse<CloseAllPositionsData> = self
                    .signed_post("/openApi/swap/v2/trade/closeAllPositions", &params)
                    .await?;

                if !response.is_success() {
                    return Ok(OrderResult::failure(
                        formatted_symbol,
                        OrderSide::Sell,
                        response.error_message(),
                    ));
                }

                // Get the order IDs from the response
                let order_ids = response
                    .data
                    .and_then(|d| d.success)
                    .unwrap_or_default();

                let order_id = order_ids
                    .first()
                    .map(|v| match v {
                        serde_json::Value::String(s) => s.clone(),
                        serde_json::Value::Number(n) => n.to_string(),
                        _ => String::new(),
                    })
                    .unwrap_or_default();

                info!("BingX position closed for {}: order_id={}", formatted_symbol, order_id);

                let close_side = if pos.position_side == "LONG" {
                    OrderSide::Sell
                } else {
                    OrderSide::Buy
                };

                Ok(OrderResult {
                    success: true,
                    order_id: Some(order_id),
                    symbol: formatted_symbol,
                    side: close_side,
                    order_type: OrderType::Market,
                    quantity: Some(size.abs()),
                    entry_price: None,
                    position_size: Some(0.0),
                    position_usdt: None,
                    error: None,
                    raw_response: None,
                })
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
        // Handle BTC/USDT format -> BTC-USDT
        if symbol.contains('/') {
            return symbol.replace('/', "-");
        }
        // Already in BingX format
        if symbol.contains('-') {
            return symbol;
        }
        // BTCUSDT -> BTC-USDT
        if symbol.ends_with("USDT") {
            let base = symbol.trim_end_matches("USDT");
            return format!("{}-USDT", base);
        }
        // BTC -> BTC-USDT
        format!("{}-USDT", symbol)
    }

    async fn get_orderbook(&self, symbol: &str, depth: u32) -> Result<Orderbook> {
        let formatted_symbol = self.format_symbol(symbol);
        let limit = depth.min(100);

        let endpoint = match self.market_type {
            MarketType::Swap => "/openApi/swap/v2/quote/depth",
            MarketType::Spot => "/openApi/spot/v1/market/depth",
        };

        let response: BingxResponse<OrderbookData> = self
            .client
            .get(format!(
                "{}{}?symbol={}&limit={}",
                self.base_url, endpoint, formatted_symbol, limit
            ))
            .send()
            .await?
            .json()
            .await?;

        if !response.is_success() {
            anyhow::bail!("BingX API error: {}", response.error_message());
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
                    let qty: f64 = level[1].parse().ok()?;
                    Some(OrderbookLevel {
                        price,
                        quantity: qty,
                    })
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
                    let qty: f64 = level[1].parse().ok()?;
                    Some(OrderbookLevel {
                        price,
                        quantity: qty,
                    })
                } else {
                    None
                }
            })
            .collect();

        Ok(Orderbook {
            symbol: formatted_symbol,
            bids,
            asks,
            timestamp: data.timestamp.unwrap_or_else(timestamp_ms),
        })
    }

    async fn test_ws_orderbook(
        &self,
        symbol: &str,
        depth: u32,
        timeout_ms: u64,
    ) -> Result<WsTestResult> {
        let formatted_symbol = self.format_symbol(symbol);
        let start = std::time::Instant::now();
        let ws_url = self.ws_public_url();

        let (mut ws_stream, _) =
            connect_async(ws_url).await.context("Failed to connect to BingX WebSocket")?;

        let connect_time = start.elapsed().as_millis() as u64;

        // Subscribe to orderbook - BingX uses different format
        let level = if depth <= 5 {
            5
        } else if depth <= 10 {
            10
        } else {
            20
        };
        let subscribe_msg = serde_json::json!({
            "id": "orderbook_sub",
            "reqType": "sub",
            "dataType": format!("{}@depth{}@500ms", formatted_symbol, level)
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
                            if text == "Ping" {
                                let _ = ws_stream.send(Message::Text("Pong".to_string())).await;
                                continue;
                            }
                            if let Ok(parsed) = serde_json::from_str::<serde_json::Value>(&text) {
                                if parsed.get("data").is_some() {
                                    data_sample = Some(parsed);
                                    break;
                                }
                                if data_sample.is_none() {
                                    data_sample = Some(parsed);
                                }
                            }
                        }
                        Some(Ok(Message::Binary(bin))) => {
                            messages_received += 1;
                            if let Ok(text) = self.decompress(&bin) {
                                if text == "Ping" {
                                    let _ = ws_stream.send(Message::Text("Pong".to_string())).await;
                                    continue;
                                }
                                if let Ok(parsed) = serde_json::from_str::<serde_json::Value>(&text) {
                                    if parsed.get("data").is_some() {
                                        data_sample = Some(parsed);
                                        break;
                                    }
                                    if data_sample.is_none() {
                                        data_sample = Some(parsed);
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
            subscription_type: format!("{}@depth{}", formatted_symbol, level),
            data_sample,
        })
    }

    async fn test_ws_trades(&self, symbol: &str, timeout_ms: u64) -> Result<WsTestResult> {
        let formatted_symbol = self.format_symbol(symbol);
        let start = std::time::Instant::now();
        let ws_url = self.ws_public_url();

        let (mut ws_stream, _) =
            connect_async(ws_url).await.context("Failed to connect to BingX WebSocket")?;

        let connect_time = start.elapsed().as_millis() as u64;

        // Subscribe to trades
        let subscribe_msg = serde_json::json!({
            "id": "trade_sub",
            "reqType": "sub",
            "dataType": format!("{}@trade", formatted_symbol)
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
                            if text == "Ping" {
                                let _ = ws_stream.send(Message::Text("Pong".to_string())).await;
                                continue;
                            }
                            if let Ok(parsed) = serde_json::from_str::<serde_json::Value>(&text) {
                                if parsed.get("data").is_some() {
                                    data_sample = Some(parsed);
                                    break;
                                }
                                if data_sample.is_none() {
                                    data_sample = Some(parsed);
                                }
                            }
                        }
                        Some(Ok(Message::Binary(bin))) => {
                            messages_received += 1;
                            if let Ok(text) = self.decompress(&bin) {
                                if text == "Ping" {
                                    let _ = ws_stream.send(Message::Text("Pong".to_string())).await;
                                    continue;
                                }
                                if let Ok(parsed) = serde_json::from_str::<serde_json::Value>(&text) {
                                    if parsed.get("data").is_some() {
                                        data_sample = Some(parsed);
                                        break;
                                    }
                                    if data_sample.is_none() {
                                        data_sample = Some(parsed);
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
            subscription_type: format!("{}@trade", formatted_symbol),
            data_sample,
        })
    }

    async fn get_ticker(&self, symbol: &str) -> Result<TickerInfo> {
        let formatted_symbol = self.format_symbol(symbol);

        let endpoint = match self.market_type {
            MarketType::Swap => "/openApi/swap/v2/quote/ticker",
            MarketType::Spot => "/openApi/spot/v1/ticker/24hr",
        };

        let response: BingxResponse<TickerData> = self
            .client
            .get(format!(
                "{}{}?symbol={}",
                self.base_url, endpoint, formatted_symbol
            ))
            .send()
            .await?
            .json()
            .await?;

        if !response.is_success() {
            anyhow::bail!("BingX API error: {}", response.error_message());
        }

        let data = response
            .data
            .ok_or_else(|| anyhow::anyhow!("No ticker data"))?;

        Ok(TickerInfo {
            symbol: formatted_symbol,
            last_price: data
                .last_price
                .as_ref()
                .and_then(|s| s.parse().ok())
                .unwrap_or(0.0),
            bid_price: data.bid_price.as_ref().and_then(|s| s.parse().ok()),
            ask_price: data.ask_price.as_ref().and_then(|s| s.parse().ok()),
            high_24h: data.high_price.as_ref().and_then(|s| s.parse().ok()),
            low_24h: data.low_price.as_ref().and_then(|s| s.parse().ok()),
            volume_24h: data.volume.as_ref().and_then(|s| s.parse().ok()),
            turnover_24h: data.quote_volume.as_ref().and_then(|s| s.parse().ok()),
            change_24h: data.price_change_percent.as_ref().and_then(|s| s.parse().ok()),
            open_interest: data.open_interest.as_ref().and_then(|s| s.parse().ok()),
            open_interest_value: None,
            funding_rate: None, // Need separate API call
            next_funding_time: None,
            mark_price: None,
            index_price: None,
            timestamp: timestamp_ms(),
        })
    }

    async fn test_ws_ticker(&self, symbol: &str, timeout_ms: u64) -> Result<WsTestResult> {
        let formatted_symbol = self.format_symbol(symbol);
        let start = std::time::Instant::now();
        let ws_url = self.ws_public_url();

        let (mut ws_stream, _) =
            connect_async(ws_url).await.context("Failed to connect to BingX WebSocket")?;

        let connect_time = start.elapsed().as_millis() as u64;

        // Subscribe to ticker
        let subscribe_msg = serde_json::json!({
            "id": "ticker_sub",
            "reqType": "sub",
            "dataType": format!("{}@ticker", formatted_symbol)
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
                            if text == "Ping" {
                                let _ = ws_stream.send(Message::Text("Pong".to_string())).await;
                                continue;
                            }
                            if let Ok(parsed) = serde_json::from_str::<serde_json::Value>(&text) {
                                if parsed.get("data").is_some() {
                                    data_sample = Some(parsed);
                                    break;
                                }
                                if data_sample.is_none() {
                                    data_sample = Some(parsed);
                                }
                            }
                        }
                        Some(Ok(Message::Binary(bin))) => {
                            messages_received += 1;
                            if let Ok(text) = self.decompress(&bin) {
                                if text == "Ping" {
                                    let _ = ws_stream.send(Message::Text("Pong".to_string())).await;
                                    continue;
                                }
                                if let Ok(parsed) = serde_json::from_str::<serde_json::Value>(&text) {
                                    if parsed.get("data").is_some() {
                                        data_sample = Some(parsed);
                                        break;
                                    }
                                    if data_sample.is_none() {
                                        data_sample = Some(parsed);
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
            subscription_type: format!("{}@ticker", formatted_symbol),
            data_sample,
        })
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
        let response: BingxResponse<Vec<FundingRateData>> = self
            .client
            .get(format!(
                "{}/openApi/swap/v2/quote/fundingRate?symbol={}&limit={}",
                self.base_url, formatted_symbol, limit.min(100)
            ))
            .send()
            .await?
            .json()
            .await?;

        if !response.is_success() {
            anyhow::bail!("BingX API error: {}", response.error_message());
        }

        let entries: Vec<FundingRateEntry> = response
            .data
            .unwrap_or_default()
            .iter()
            .filter_map(|e| {
                Some(FundingRateEntry {
                    symbol: e
                        .symbol
                        .clone()
                        .unwrap_or_else(|| formatted_symbol.clone()),
                    funding_rate: e.funding_rate.parse().ok()?,
                    funding_time: e.funding_time,
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
            anyhow::bail!("Open interest only available for perpetual futures");
        }

        let formatted_symbol = self.format_symbol(symbol);

        // BingX provides current OI only, not history
        let response: BingxResponse<OpenInterestData> = self
            .client
            .get(format!(
                "{}/openApi/swap/v2/quote/openInterest?symbol={}",
                self.base_url, formatted_symbol
            ))
            .send()
            .await?
            .json()
            .await?;

        if !response.is_success() {
            anyhow::bail!("BingX API error: {}", response.error_message());
        }

        let oi = response
            .data
            .and_then(|d| d.open_interest.parse().ok())
            .unwrap_or(0.0);

        Ok(vec![OpenInterestEntry {
            symbol: formatted_symbol,
            open_interest: oi,
            open_interest_value: 0.0,
            timestamp: timestamp_ms(),
        }])
    }

    async fn subscribe_order_updates(
        &self,
        sender: broadcast::Sender<OrderUpdateEvent>,
    ) -> Result<JoinHandle<()>> {
        let api_key = self.api_key.clone();
        let api_secret = self.api_secret.clone();
        let testnet = self.testnet;
        let exchange_name = "bingx".to_string();
        let base_url = self.base_url.clone();

        let handle = tokio::spawn(async move {
            let client = Client::new();
            let mut reconnect_delay = INITIAL_RECONNECT_DELAY_SECS;

            loop {
                // Create listenKey
                let timestamp = timestamp_ms();
                let params = format!("timestamp={}", timestamp);
                let signature = hmac_sign(&api_secret, &params);
                let listen_key_url = format!(
                    "{}/openApi/user/auth/userDataStream?{}&signature={}",
                    base_url, params, signature
                );

                let listen_key_resp = client
                    .post(&listen_key_url)
                    .header("X-BX-APIKEY", &api_key)
                    .send()
                    .await;

                let listen_key = match listen_key_resp {
                    Ok(resp) => {
                        let text = resp.text().await.unwrap_or_default();
                        match serde_json::from_str::<serde_json::Value>(&text) {
                            Ok(json) => {
                                // listenKey can be at top level or in data
                                json.get("listenKey")
                                    .or_else(|| json.get("data").and_then(|d| d.get("listenKey")))
                                    .and_then(|v| v.as_str())
                                    .map(|s| s.to_string())
                                    .unwrap_or_default()
                            }
                            Err(_) => String::new(),
                        }
                    }
                    Err(e) => {
                        error!("[{}] Failed to create listenKey: {}", exchange_name, e);
                        tokio::time::sleep(tokio::time::Duration::from_secs(reconnect_delay)).await;
                        reconnect_delay = next_reconnect_delay(reconnect_delay);
                        continue;
                    }
                };

                if listen_key.is_empty() {
                    error!("[{}] Empty listenKey received", exchange_name);
                    tokio::time::sleep(tokio::time::Duration::from_secs(reconnect_delay)).await;
                    reconnect_delay = next_reconnect_delay(reconnect_delay);
                    continue;
                }

                let ws_url = if testnet {
                    format!(
                        "wss://open-api-vst.bingx.com/market?listenKey={}",
                        listen_key
                    )
                } else {
                    format!(
                        "{}?listenKey={}",
                        MAINNET_WS_USER_URL, listen_key
                    )
                };

                info!("[{}] Connecting to user data stream", exchange_name);

                match connect_async(&ws_url).await {
                    Ok((mut ws_stream, _)) => {
                        info!("[{}] Connected to user data stream", exchange_name);
                        reconnect_delay = INITIAL_RECONNECT_DELAY_SECS;

                        // BingX listenKey needs to be renewed every 30 minutes
                        let mut keepalive_interval =
                            tokio::time::interval(tokio::time::Duration::from_secs(30 * 60));
                        keepalive_interval.tick().await;

                        loop {
                            tokio::select! {
                                msg = ws_stream.next() => {
                                    match msg {
                                        Some(Ok(Message::Text(text))) => {
                                            // BingX may send Ping as text
                                            if text == "Ping" {
                                                let _ = ws_stream.send(Message::Text("Pong".to_string())).await;
                                                continue;
                                            }
                                            if let Ok(ev) = serde_json::from_str::<serde_json::Value>(&text) {
                                                process_order_event(&ev, &exchange_name, &sender);
                                            }
                                        }
                                        Some(Ok(Message::Binary(data))) => {
                                            // BingX sends gzip compressed data
                                            let mut decoder = GzDecoder::new(&data[..]);
                                            let mut text = String::new();
                                            if decoder.read_to_string(&mut text).is_ok() {
                                                if text == "Ping" {
                                                    let _ = ws_stream.send(Message::Text("Pong".to_string())).await;
                                                    continue;
                                                }
                                                if let Ok(ev) = serde_json::from_str::<serde_json::Value>(&text) {
                                                    process_order_event(&ev, &exchange_name, &sender);
                                                }
                                            }
                                        }
                                        Some(Ok(Message::Ping(data))) => {
                                            let _ = ws_stream.send(Message::Pong(data)).await;
                                        }
                                        Some(Ok(Message::Close(_))) | Some(Err(_)) | None => {
                                            warn!("[{}] User data stream disconnected", exchange_name);
                                            break;
                                        }
                                        _ => {}
                                    }
                                }
                                _ = keepalive_interval.tick() => {
                                    // Renew listenKey
                                    let ts = timestamp_ms();
                                    let params = format!("timestamp={}", ts);
                                    let sig = hmac_sign(&api_secret, &params);
                                    let keepalive_url = format!(
                                        "{}/openApi/user/auth/userDataStream?{}&signature={}&listenKey={}",
                                        base_url, params, sig, listen_key
                                    );
                                    match client.put(&keepalive_url).header("X-BX-APIKEY", &api_key).send().await {
                                        Ok(resp) if resp.status().is_success() => {
                                            debug!("[{}] listenKey renewed", exchange_name);
                                        }
                                        _ => {
                                            warn!("[{}] listenKey renewal failed, reconnecting...", exchange_name);
                                            break;
                                        }
                                    }
                                }
                            }
                        }
                    }
                    Err(e) => {
                        error!(
                            "[{}] Failed to connect user data stream: {}",
                            exchange_name, e
                        );
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

/// Process order event from WebSocket.
fn process_order_event(
    ev: &serde_json::Value,
    exchange_name: &str,
    sender: &broadcast::Sender<OrderUpdateEvent>,
) {
    let event_type = ev.get("e").and_then(|v| v.as_str()).unwrap_or("");

    // BingX uses ORDER_TRADE_UPDATE for futures, executionReport for spot
    if event_type == "ORDER_TRADE_UPDATE" || event_type == "executionReport" {
        let o = ev.get("o").unwrap_or(ev);

        // Extract order_id which can be string or number
        let order_id = o
            .get("i")
            .map(|v| match v {
                serde_json::Value::String(s) => s.clone(),
                serde_json::Value::Number(n) => n.to_string(),
                _ => String::new(),
            })
            .unwrap_or_default();

        let event = OrderUpdateEvent {
            exchange: exchange_name.to_string(),
            order_id,
            client_order_id: o.get("c").and_then(|v| v.as_str()).map(|s| s.to_string()),
            symbol: o
                .get("s")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string(),
            status: parse_order_status_str(o.get("X").and_then(|v| v.as_str()).unwrap_or("NEW")),
            side: if o.get("S").and_then(|v| v.as_str()) == Some("BUY") {
                OrderSide::Buy
            } else {
                OrderSide::Sell
            },
            order_type: if o.get("o").and_then(|v| v.as_str()) == Some("MARKET") {
                OrderType::Market
            } else {
                OrderType::Limit
            },
            price: o
                .get("p")
                .and_then(|v| v.as_str())
                .and_then(|s| s.parse().ok())
                .unwrap_or(0.0),
            quantity: o
                .get("q")
                .and_then(|v| v.as_str())
                .and_then(|s| s.parse().ok())
                .unwrap_or(0.0),
            filled_quantity: o
                .get("z")
                .and_then(|v| v.as_str())
                .and_then(|s| s.parse().ok())
                .unwrap_or(0.0),
            avg_fill_price: o
                .get("ap")
                .and_then(|v| v.as_str())
                .and_then(|s| s.parse().ok()),
            fee: o
                .get("n")
                .and_then(|v| v.as_str())
                .and_then(|s| s.parse().ok()),
            fee_currency: o.get("N").and_then(|v| v.as_str()).map(|s| s.to_string()),
            exchange_timestamp: ev.get("T").and_then(|v| v.as_i64()).unwrap_or(0),
            received_at: timestamp_ms(),
            raw: Some(ev.clone()),
        };

        log_order_update(&event);
        let _ = sender.send(event);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_format_symbol() {
        let trader = BingxTrader::new("", "", false, MarketType::Swap);

        assert_eq!(trader.format_symbol("BTC"), "BTC-USDT");
        assert_eq!(trader.format_symbol("BTCUSDT"), "BTC-USDT");
        assert_eq!(trader.format_symbol("BTC/USDT"), "BTC-USDT");
        assert_eq!(trader.format_symbol("BTC-USDT"), "BTC-USDT");
        assert_eq!(trader.format_symbol("eth"), "ETH-USDT");
    }

    #[test]
    fn test_parse_order_status() {
        assert_eq!(parse_order_status_str("NEW"), OrderStatus::New);
        assert_eq!(parse_order_status_str("FILLED"), OrderStatus::Filled);
        assert_eq!(
            parse_order_status_str("PARTIALLY_FILLED"),
            OrderStatus::PartiallyFilled
        );
        assert_eq!(parse_order_status_str("CANCELLED"), OrderStatus::Cancelled);
        assert_eq!(parse_order_status_str("REJECTED"), OrderStatus::Rejected);
        assert_eq!(parse_order_status_str("EXPIRED"), OrderStatus::Expired);
    }

    #[test]
    fn test_next_reconnect_delay() {
        assert_eq!(next_reconnect_delay(1), 2);
        assert_eq!(next_reconnect_delay(2), 4);
        assert_eq!(next_reconnect_delay(16), 30); // Capped at MAX
        assert_eq!(next_reconnect_delay(30), 30); // Already at max
    }
}
