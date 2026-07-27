//! XT.com exchange trader implementation.
//!
//! Full-featured implementation supporting:
//! - Spot and futures (perpetual swap) trading
//! - REST API with HMAC-SHA256 authentication
//! - WebSocket for market data (orderbook, trades, ticker)
//! - Private WebSocket for order updates
//! - Funding rate and open interest data
//!
//! API documentation: https://doc.xt.com/

use crate::models::*;
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
use tokio::sync::broadcast;
use tokio::task::JoinHandle;
use tokio_tungstenite::{connect_async, tungstenite::protocol::Message};
use tracing::{debug, error, info, warn};

// ═══════════════════════════════════════════════════════════════════════════
// API Endpoints
// ═══════════════════════════════════════════════════════════════════════════

const SPOT_URL: &str = "https://sapi.xt.com";
const FUTURES_URL: &str = "https://fapi.xt.com";

// WebSocket URLs
const SPOT_WS_PUBLIC_URL: &str = "wss://stream.xt.com/public";
const SPOT_WS_PRIVATE_URL: &str = "wss://stream.xt.com/private";
const FUTURES_WS_PUBLIC_URL: &str = "wss://fstream.xt.com/ws/market";
const FUTURES_WS_PRIVATE_URL: &str = "wss://fstream.xt.com/ws/user";

/// Private WebSocket reconnect constants.
const INITIAL_RECONNECT_DELAY_SECS: u64 = 1;
const MAX_RECONNECT_DELAY_SECS: u64 = 30;

/// Calculate next reconnect delay with exponential backoff.
fn next_reconnect_delay(current_secs: u64) -> u64 {
    (current_secs * 2).min(MAX_RECONNECT_DELAY_SECS)
}

/// Parse order status string to OrderStatus enum.
fn parse_order_status_str(s: &str) -> OrderStatus {
    match s.to_uppercase().as_str() {
        "NEW" | "OPEN" | "CREATED" | "INIT" | "ACCEPTED" => OrderStatus::New,
        "PARTIALLY_FILLED" | "PARTIALLYFILLED" | "PARTIAL_FILLED" | "PARTIAL" => {
            OrderStatus::PartiallyFilled
        }
        "FILLED" | "CLOSED" | "FULL_FILL" | "FULL" => OrderStatus::Filled,
        "CANCELLED" | "CANCELED" | "CANCEL" => OrderStatus::Cancelled,
        "REJECTED" | "FAIL" | "FAILED" => OrderStatus::Rejected,
        "EXPIRED" | "EXPIRE" => OrderStatus::Expired,
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

// ═══════════════════════════════════════════════════════════════════════════
// XtTrader Implementation
// ═══════════════════════════════════════════════════════════════════════════

/// XT.com exchange trader.
///
/// Supports both spot and futures (perpetual swap) trading with full
/// authentication and WebSocket capabilities.
pub struct XtTrader {
    api_key: String,
    api_secret: String,
    _testnet: bool,
    market_type: MarketType,
    client: Client,
    base_url: String,
}

impl XtTrader {
    /// Create a new XT trader.
    pub fn new(api_key: &str, api_secret: &str, testnet: bool, market_type: MarketType) -> Self {
        let base_url = match market_type {
            MarketType::Spot => SPOT_URL,
            MarketType::Swap => FUTURES_URL,
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

    fn ws_public_url(&self) -> &str {
        match self.market_type {
            MarketType::Spot => SPOT_WS_PUBLIC_URL,
            MarketType::Swap => FUTURES_WS_PUBLIC_URL,
        }
    }

    fn ws_private_url(&self) -> &str {
        match self.market_type {
            MarketType::Spot => SPOT_WS_PRIVATE_URL,
            MarketType::Swap => FUTURES_WS_PRIVATE_URL,
        }
    }

    fn sign_spot_request(
        &self,
        method: &str,
        path: &str,
        query_or_body: Option<&str>,
        timestamp: i64,
    ) -> String {
        let recv_window = "5000";
        let mut payload_string = format!(
            "xt-validate-algorithms=HmacSHA256&xt-validate-appkey={}&xt-validate-recvwindow={}&xt-validate-timestamp={}",
            self.api_key, recv_window, timestamp
        );

        payload_string.push_str(&format!("#{}#{}", method, path));
        if let Some(qb) = query_or_body {
            if !qb.is_empty() {
                payload_string.push_str(&format!("#{}", qb));
            }
        }

        hmac_sign(&self.api_secret, &payload_string)
    }

    fn sign_futures_request(
        &self,
        method: &str,
        path: &str,
        query_or_body: Option<&str>,
        timestamp: i64,
    ) -> String {
        let mut payload_string = format!(
            "xt-validate-appkey={}&xt-validate-timestamp={}",
            self.api_key, timestamp
        );

        if method == "GET" {
            payload_string.push_str(&format!("#{}", path));
            if let Some(query) = query_or_body {
                if !query.is_empty() {
                    payload_string.push_str(&format!("#{}", query));
                }
            }
        } else {
            payload_string.push_str(&format!("#{}", path));
            if let Some(body) = query_or_body {
                payload_string.push_str(&format!("#{}", body));
            }
        }

        hmac_sign(&self.api_secret, &payload_string)
    }

    async fn signed_spot_get<T: for<'de> serde::Deserialize<'de>>(
        &self,
        endpoint: &str,
        params: &str,
    ) -> Result<XtSpotResponse<T>> {
        let timestamp = timestamp_ms();
        let path = format!("/v4{}", endpoint);
        let signature = self.sign_spot_request("GET", &path, Some(params), timestamp);

        let url = if params.is_empty() {
            format!("{}{}", self.base_url, path)
        } else {
            format!("{}{}?{}", self.base_url, path, params)
        };

        let response = self
            .client
            .get(&url)
            .header("Content-Type", "application/json")
            .header("xt-validate-algorithms", "HmacSHA256")
            .header("xt-validate-appkey", &self.api_key)
            .header("xt-validate-timestamp", timestamp.to_string())
            .header("xt-validate-recvwindow", "5000")
            .header("xt-validate-signature", signature)
            .send()
            .await
            .context("Failed to send request to XT")?;

        let text = response.text().await?;
        debug!("XT spot response: {}", text);

        serde_json::from_str(&text).context(format!("Failed to parse XT spot response: {}", text))
    }

    async fn signed_spot_post<T: for<'de> serde::Deserialize<'de>>(
        &self,
        endpoint: &str,
        body: &serde_json::Value,
    ) -> Result<XtSpotResponse<T>> {
        let timestamp = timestamp_ms();
        let path = format!("/v4{}", endpoint);
        let body_str = body.to_string();
        let signature = self.sign_spot_request("POST", &path, Some(&body_str), timestamp);

        let url = format!("{}{}", self.base_url, path);

        let response = self
            .client
            .post(&url)
            .header("Content-Type", "application/json")
            .header("xt-validate-algorithms", "HmacSHA256")
            .header("xt-validate-appkey", &self.api_key)
            .header("xt-validate-timestamp", timestamp.to_string())
            .header("xt-validate-recvwindow", "5000")
            .header("xt-validate-signature", signature)
            .body(body_str)
            .send()
            .await
            .context("Failed to send request to XT")?;

        let text = response.text().await?;
        debug!("XT spot response: {}", text);

        serde_json::from_str(&text).context(format!("Failed to parse XT spot response: {}", text))
    }

    async fn signed_spot_delete<T: for<'de> serde::Deserialize<'de>>(
        &self,
        endpoint: &str,
        params: &str,
    ) -> Result<XtSpotResponse<T>> {
        let timestamp = timestamp_ms();
        let path = format!("/v4{}", endpoint);
        let signature = self.sign_spot_request("DELETE", &path, Some(params), timestamp);

        let url = if params.is_empty() {
            format!("{}{}", self.base_url, path)
        } else {
            format!("{}{}?{}", self.base_url, path, params)
        };

        let response = self
            .client
            .delete(&url)
            .header("Content-Type", "application/json")
            .header("xt-validate-algorithms", "HmacSHA256")
            .header("xt-validate-appkey", &self.api_key)
            .header("xt-validate-timestamp", timestamp.to_string())
            .header("xt-validate-recvwindow", "5000")
            .header("xt-validate-signature", signature)
            .send()
            .await
            .context("Failed to send request to XT")?;

        let text = response.text().await?;
        debug!("XT spot response: {}", text);

        serde_json::from_str(&text).context(format!("Failed to parse XT spot response: {}", text))
    }

    async fn signed_futures_get<T: for<'de> serde::Deserialize<'de>>(
        &self,
        endpoint: &str,
        params: &str,
    ) -> Result<XtFuturesResponse<T>> {
        let timestamp = timestamp_ms();
        let signature = self.sign_futures_request("GET", endpoint, Some(params), timestamp);

        let url = if params.is_empty() {
            format!("{}{}", self.base_url, endpoint)
        } else {
            format!("{}{}?{}", self.base_url, endpoint, params)
        };

        let response = self
            .client
            .get(&url)
            .header("Content-Type", "application/json")
            .header("xt-validate-appkey", &self.api_key)
            .header("xt-validate-timestamp", timestamp.to_string())
            .header("xt-validate-signature", signature)
            .send()
            .await
            .context("Failed to send request to XT")?;

        let text = response.text().await?;
        debug!("XT futures response: {}", text);

        serde_json::from_str(&text)
            .context(format!("Failed to parse XT futures response: {}", text))
    }

    async fn signed_futures_post<T: for<'de> serde::Deserialize<'de>>(
        &self,
        endpoint: &str,
        body: &serde_json::Value,
    ) -> Result<XtFuturesResponse<T>> {
        let timestamp = timestamp_ms();
        let body_str = body.to_string();
        let signature = self.sign_futures_request("POST", endpoint, Some(&body_str), timestamp);

        let url = format!("{}{}", self.base_url, endpoint);

        let response = self
            .client
            .post(&url)
            .header("Content-Type", "application/json")
            .header("xt-validate-appkey", &self.api_key)
            .header("xt-validate-timestamp", timestamp.to_string())
            .header("xt-validate-signature", signature)
            .body(body_str)
            .send()
            .await
            .context("Failed to send request to XT")?;

        let text = response.text().await?;
        debug!("XT futures response: {}", text);

        serde_json::from_str(&text)
            .context(format!("Failed to parse XT futures response: {}", text))
    }

    async fn get_position(&self, symbol: &str) -> Result<Option<FuturesPositionInfo>> {
        if self.market_type == MarketType::Spot {
            return Ok(None);
        }

        let params = format!("symbol={}", symbol);
        let response: XtFuturesResponse<FuturesPositionList> = self
            .signed_futures_get("/future/user/v1/position/list", &params)
            .await?;

        if let Some(result) = response.result {
            for pos in result.items {
                let size: f64 = pos.position_size.parse().unwrap_or(0.0);
                if size.abs() > 0.0 {
                    return Ok(Some(pos));
                }
            }
        }

        Ok(None)
    }

    async fn get_futures_listen_key(&self) -> Result<String> {
        let response: XtFuturesResponse<String> = self
            .signed_futures_get("/future/user/v1/user/listen-key", "")
            .await?;

        response
            .result
            .ok_or_else(|| anyhow::anyhow!("Failed to get listen key"))
    }

    async fn get_spot_ws_token(&self) -> Result<String> {
        let body = serde_json::json!({});
        let response: XtSpotResponse<WsTokenResponse> =
            self.signed_spot_post("/ws-token", &body).await?;

        if let Some(result) = response.result {
            if let Some(token) = result.access_token.or(result.token) {
                return Ok(token);
            }
        }

        anyhow::bail!("Failed to get WebSocket token")
    }
}

#[async_trait]
impl ExchangeTrader for XtTrader {
    fn exchange_name(&self) -> &str {
        "xt"
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
        let url = match self.market_type {
            MarketType::Spot => format!("{}/v4/public/time", self.base_url),
            MarketType::Swap => {
                format!("{}/future/market/v1/public/symbol/list", self.base_url)
            }
        };

        let response = self.client.get(&url).send().await?;
        Ok(response.status().is_success())
    }

    async fn get_server_time(&self) -> Result<i64> {
        if self.market_type == MarketType::Spot {
            let response: XtSpotResponse<serde_json::Value> = self
                .client
                .get(format!("{}/v4/public/time", self.base_url))
                .send()
                .await?
                .json()
                .await?;

            if let Some(result) = response.result {
                if let Some(ts) = result.get("serverTime").and_then(|v| v.as_i64()) {
                    return Ok(ts);
                }
            }
        } else {
            // Futures doesn't have a direct time endpoint, use symbol list timestamp
            let _response: serde_json::Value = self
                .client
                .get(format!(
                    "{}/future/market/v1/public/symbol/list",
                    self.base_url
                ))
                .send()
                .await?
                .json()
                .await?;

            // Just return current time as fallback
            return Ok(timestamp_ms());
        }

        Ok(timestamp_ms())
    }

    async fn get_balance(&self) -> Result<AccountBalance> {
        if self.api_key.is_empty() {
            return Ok(AccountBalance::default());
        }

        match self.market_type {
            MarketType::Spot => {
                let response: XtSpotResponse<Vec<SpotBalanceEntry>> =
                    self.signed_spot_get("/balances", "").await?;

                if let Some(balances) = response.result {
                    for bal in &balances {
                        if bal.currency.to_uppercase() == "USDT" {
                            let available: f64 = bal.available_amount.parse().unwrap_or(0.0);
                            let frozen: f64 = bal.frozen_amount.parse().unwrap_or(0.0);
                            return Ok(AccountBalance::simple(available, available + frozen, "USDT"));
                        }
                    }
                }
                Ok(AccountBalance::default())
            }
            MarketType::Swap => {
                let response: XtFuturesResponse<FuturesBalanceList> = self
                    .signed_futures_get("/future/user/v1/balance/list", "")
                    .await?;

                if let Some(result) = response.result {
                    for bal in &result.items {
                        if bal.coin.to_uppercase() == "USDT" {
                            let available: f64 = bal.available_balance.parse().unwrap_or(0.0);
                            let wallet: f64 = bal.wallet_balance.parse().unwrap_or(0.0);
                            return Ok(AccountBalance::simple(available, wallet, "USDT"));
                        }
                    }
                }
                Ok(AccountBalance::default())
            }
        }
    }

    async fn get_current_price(&self, symbol: &str) -> Result<f64> {
        let formatted_symbol = self.format_symbol(symbol);

        match self.market_type {
            MarketType::Spot => {
                let response: XtSpotResponse<Vec<SpotTickerData>> = self
                    .client
                    .get(format!(
                        "{}/v4/public/ticker/price?symbol={}",
                        self.base_url, formatted_symbol
                    ))
                    .send()
                    .await?
                    .json()
                    .await?;

                if let Some(tickers) = response.result {
                    if let Some(ticker) = tickers.first() {
                        if let Some(price) = &ticker.c {
                            return Ok(price.parse().unwrap_or(0.0));
                        }
                    }
                }
                anyhow::bail!("Failed to get price for {}", symbol)
            }
            MarketType::Swap => {
                let response: XtFuturesResponse<FuturesAggTickerData> = self
                    .client
                    .get(format!(
                        "{}/future/market/v1/public/q/agg-ticker?symbol={}",
                        self.base_url, formatted_symbol
                    ))
                    .send()
                    .await?
                    .json()
                    .await?;

                if let Some(ticker) = response.result {
                    return Ok(ticker.c.parse().unwrap_or(0.0));
                }
                anyhow::bail!("Failed to get price for {}", symbol)
            }
        }
    }

    async fn get_ticker(&self, symbol: &str) -> Result<TickerInfo> {
        let formatted_symbol = self.format_symbol(symbol);
        let timestamp = timestamp_ms();

        match self.market_type {
            MarketType::Spot => {
                let response: XtSpotResponse<Vec<SpotTickerData>> = self
                    .client
                    .get(format!(
                        "{}/v4/public/ticker/24h?symbol={}",
                        self.base_url, formatted_symbol
                    ))
                    .send()
                    .await?
                    .json()
                    .await?;

                if let Some(tickers) = response.result {
                    if let Some(t) = tickers.first() {
                        return Ok(TickerInfo {
                            symbol: formatted_symbol,
                            last_price: t.c.as_ref().and_then(|s| s.parse().ok()).unwrap_or(0.0),
                            bid_price: t.bp.as_ref().and_then(|s| s.parse().ok()),
                            ask_price: t.ap.as_ref().and_then(|s| s.parse().ok()),
                            high_24h: t.h.as_ref().and_then(|s| s.parse().ok()),
                            low_24h: t.l.as_ref().and_then(|s| s.parse().ok()),
                            volume_24h: t.q.as_ref().and_then(|s| s.parse().ok()),
                            turnover_24h: t.v.as_ref().and_then(|s| s.parse().ok()),
                            change_24h: t.cr.as_ref().and_then(|s| s.parse().ok()),
                            open_interest: None,
                            open_interest_value: None,
                            funding_rate: None,
                            next_funding_time: None,
                            mark_price: None,
                            index_price: None,
                            timestamp,
                        });
                    }
                }
                anyhow::bail!("Failed to get ticker for {}", symbol)
            }
            MarketType::Swap => {
                let ticker_response: XtFuturesResponse<FuturesAggTickerData> = self
                    .client
                    .get(format!(
                        "{}/future/market/v1/public/q/agg-ticker?symbol={}",
                        self.base_url, formatted_symbol
                    ))
                    .send()
                    .await?
                    .json()
                    .await?;

                let funding_response: XtFuturesResponse<FuturesFundingRateData> = self
                    .client
                    .get(format!(
                        "{}/future/market/v1/public/q/funding-rate?symbol={}",
                        self.base_url, formatted_symbol
                    ))
                    .send()
                    .await?
                    .json()
                    .await?;

                let oi_response: XtFuturesResponse<FuturesOpenInterestData> = self
                    .client
                    .get(format!(
                        "{}/future/market/v1/public/contract/open-interest?symbol={}",
                        self.base_url, formatted_symbol
                    ))
                    .send()
                    .await?
                    .json()
                    .await?;

                if let Some(t) = ticker_response.result {
                    let mut ticker = TickerInfo {
                        symbol: formatted_symbol,
                        last_price: t.c.parse().unwrap_or(0.0),
                        bid_price: t.bp.as_ref().and_then(|s| s.parse().ok()),
                        ask_price: t.ap.as_ref().and_then(|s| s.parse().ok()),
                        high_24h: Some(t.h.parse().unwrap_or(0.0)),
                        low_24h: Some(t.l.parse().unwrap_or(0.0)),
                        volume_24h: t.a.as_ref().and_then(|s| s.parse().ok()),
                        turnover_24h: t.v.as_ref().and_then(|s| s.parse().ok()),
                        change_24h: t.r.as_ref().and_then(|s| s.parse().ok()),
                        open_interest: None,
                        open_interest_value: None,
                        funding_rate: None,
                        next_funding_time: None,
                        mark_price: t.m.as_ref().and_then(|s| s.parse().ok()),
                        index_price: t.i.as_ref().and_then(|s| s.parse().ok()),
                        timestamp: t.t,
                    };

                    if let Some(f) = funding_response.result {
                        ticker.funding_rate = Some(f.funding_rate.parse().unwrap_or(0.0));
                        ticker.next_funding_time = Some(f.funding_time);
                    }

                    if let Some(oi) = oi_response.result {
                        ticker.open_interest = Some(oi.open_interest.parse().unwrap_or(0.0));
                        ticker.open_interest_value =
                            oi.open_interest_value.as_ref().and_then(|s| s.parse().ok());
                    }

                    return Ok(ticker);
                }
                anyhow::bail!("Failed to get ticker for {}", symbol)
            }
        }
    }

    async fn get_orderbook(&self, symbol: &str, depth: u32) -> Result<Orderbook> {
        let formatted_symbol = self.format_symbol(symbol);
        let limit = depth.min(500);

        match self.market_type {
            MarketType::Spot => {
                let response: XtSpotResponse<SpotOrderbookData> = self
                    .client
                    .get(format!(
                        "{}/v4/public/depth?symbol={}&limit={}",
                        self.base_url, formatted_symbol, limit
                    ))
                    .send()
                    .await?
                    .json()
                    .await?;

                if let Some(result) = response.result {
                    let timestamp = result.t.unwrap_or_else(timestamp_ms);

                    let bids: Vec<OrderbookLevel> = result
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

                    let asks: Vec<OrderbookLevel> = result
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

                    return Ok(Orderbook {
                        symbol: formatted_symbol,
                        bids,
                        asks,
                        timestamp,
                    });
                }
                anyhow::bail!("Failed to get orderbook for {}", symbol)
            }
            MarketType::Swap => {
                let response: XtFuturesResponse<FuturesOrderbookData> = self
                    .client
                    .get(format!(
                        "{}/future/market/v1/public/q/depth?symbol={}&level={}",
                        self.base_url, formatted_symbol, limit
                    ))
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
                        symbol: result.s.unwrap_or(formatted_symbol),
                        bids,
                        asks,
                        timestamp: result.t,
                    });
                }
                anyhow::bail!("Failed to get orderbook for {}", symbol)
            }
        }
    }

    async fn get_funding_rate_history(&self, symbol: &str, limit: u32) -> Result<Vec<FundingRateEntry>> {
        if self.market_type != MarketType::Swap {
            anyhow::bail!("Funding rate only available for perpetual futures");
        }

        let formatted_symbol = self.format_symbol(symbol);
        let params = format!("symbol={}&limit={}", formatted_symbol, limit.min(200));

        let response: XtFuturesResponse<FundingRateHistoryResult> = self
            .client
            .get(format!(
                "{}/future/market/v1/public/q/funding-rate-record?{}",
                self.base_url, params
            ))
            .send()
            .await?
            .json()
            .await?;

        if let Some(result) = response.result {
            let entries: Vec<FundingRateEntry> = result
                .items
                .iter()
                .filter_map(|e| {
                    let funding_rate: f64 = e.funding_rate.parse().ok()?;
                    Some(FundingRateEntry {
                        symbol: e.symbol.clone(),
                        funding_rate,
                        funding_time: e.created_time,
                    })
                })
                .collect();
            return Ok(entries);
        }

        anyhow::bail!("Failed to get funding history")
    }

    async fn get_open_interest_history(&self, symbol: &str, _interval: &str, _limit: u32) -> Result<Vec<OpenInterestEntry>> {
        if self.market_type != MarketType::Swap {
            anyhow::bail!("Open interest only available for perpetual futures");
        }

        let formatted_symbol = self.format_symbol(symbol);

        let response: XtFuturesResponse<FuturesOpenInterestData> = self
            .client
            .get(format!(
                "{}/future/market/v1/public/contract/open-interest?symbol={}",
                self.base_url, formatted_symbol
            ))
            .send()
            .await?
            .json()
            .await?;

        if let Some(oi) = response.result {
            let entry = OpenInterestEntry {
                symbol: oi.symbol,
                open_interest: oi.open_interest.parse().unwrap_or(0.0),
                open_interest_value: oi
                    .open_interest_value
                    .as_ref()
                    .and_then(|s| s.parse().ok())
                    .unwrap_or(0.0),
                timestamp: oi.timestamp.unwrap_or_else(timestamp_ms),
            };
            return Ok(vec![entry]);
        }

        anyhow::bail!("Failed to get open interest")
    }

    async fn set_leverage(&self, symbol: Option<&str>, leverage: u32) -> Result<bool> {
        if self.market_type == MarketType::Spot {
            return Ok(true);
        }

        let sym = match symbol {
            Some(s) => self.format_symbol(s),
            None => return Ok(true),
        };

        let body = serde_json::json!({
            "symbol": sym,
            "leverage": leverage,
            "positionSide": "LONG"
        });

        let response: XtFuturesResponse<serde_json::Value> = self
            .signed_futures_post("/future/user/v1/position/adjust-leverage", &body)
            .await?;

        if response.is_success() {
            let body_short = serde_json::json!({
                "symbol": sym,
                "leverage": leverage,
                "positionSide": "SHORT"
            });
            let _ = self
                .signed_futures_post::<serde_json::Value>(
                    "/future/user/v1/position/adjust-leverage",
                    &body_short,
                )
                .await;
            Ok(true)
        } else {
            warn!("Failed to set leverage: {}", response.msg_info);
            Ok(false)
        }
    }

    async fn execute_fast_entry(&self, symbol: &str, direction: &str, usdt_amount: f64, leverage: u32, _tp_percent: Option<f64>, _sl_percent: Option<f64>, current_price: Option<f64>) -> Result<OrderResult> {
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
            if let Err(e) = self.set_leverage(Some(&formatted_symbol), leverage).await {
                warn!("Failed to set leverage: {}", e);
            }
        }
        let side = if direction.to_lowercase() == "long" { OrderSide::Buy } else { OrderSide::Sell };
        self.place_market_order(&formatted_symbol, side, quantity).await
    }

    async fn place_market_order(&self, symbol: &str, side: OrderSide, quantity: f64) -> Result<OrderResult> {
        let formatted_symbol = self.format_symbol(symbol);
        info!("XT market order: {} {} {}", formatted_symbol, side, quantity);

        match self.market_type {
            MarketType::Spot => {
                let body = serde_json::json!({
                    "symbol": formatted_symbol,
                    "side": match side { OrderSide::Buy => "BUY", OrderSide::Sell => "SELL" },
                    "type": "MARKET",
                    "quantity": format!("{:.8}", quantity),
                });

                let response: XtSpotResponse<SpotOrderResult> = self.signed_spot_post("/order", &body).await?;

                if response.is_success() {
                    if let Some(result) = response.result {
                        return Ok(OrderResult {
                            success: true,
                            order_id: Some(result.order_id),
                            symbol: formatted_symbol,
                            side,
                            order_type: OrderType::Market,
                            quantity: Some(quantity),
                            entry_price: None,
                            position_size: Some(quantity),
                            position_usdt: None,
                            error: None,
                            raw_response: None,
                        });
                    }
                }
                Ok(OrderResult::failure(formatted_symbol, side, response.mc))
            }
            MarketType::Swap => {
                let position_side = match side {
                    OrderSide::Buy => "LONG",
                    OrderSide::Sell => "SHORT",
                };
                let body = serde_json::json!({
                    "symbol": formatted_symbol,
                    "orderSide": match side { OrderSide::Buy => "BUY", OrderSide::Sell => "SELL" },
                    "orderType": "MARKET",
                    "origQty": format!("{:.8}", quantity),
                    "positionSide": position_side,
                });

                let response: XtFuturesResponse<FuturesOrderResult> = self
                    .signed_futures_post("/future/trade/v1/order/create", &body)
                    .await?;

                if response.is_success() {
                    if let Some(result) = response.result {
                        return Ok(OrderResult {
                            success: true,
                            order_id: Some(result.order_id.to_string()),
                            symbol: formatted_symbol,
                            side,
                            order_type: OrderType::Market,
                            quantity: Some(quantity),
                            entry_price: None,
                            position_size: Some(quantity),
                            position_usdt: None,
                            error: None,
                            raw_response: None,
                        });
                    }
                }
                Ok(OrderResult::failure(formatted_symbol, side, response.msg_info))
            }
        }
    }

    async fn place_limit_order(&self, symbol: &str, side: OrderSide, quantity: f64, price: f64, _reduce_only: bool) -> Result<OrderResult> {
        let formatted_symbol = self.format_symbol(symbol);
        info!("XT limit order: {} {} {} @ {}", formatted_symbol, side, quantity, price);

        match self.market_type {
            MarketType::Spot => {
                let body = serde_json::json!({
                    "symbol": formatted_symbol,
                    "side": match side { OrderSide::Buy => "BUY", OrderSide::Sell => "SELL" },
                    "type": "LIMIT",
                    "quantity": format!("{:.8}", quantity),
                    "price": format!("{:.8}", price),
                    "timeInForce": "GTC",
                });
                let response: XtSpotResponse<SpotOrderResult> = self.signed_spot_post("/order", &body).await?;
                if response.is_success() {
                    if let Some(result) = response.result {
                        return Ok(OrderResult {
                            success: true,
                            order_id: Some(result.order_id),
                            symbol: formatted_symbol,
                            side,
                            order_type: OrderType::Limit,
                            quantity: Some(quantity),
                            entry_price: Some(price),
                            position_size: Some(quantity),
                            position_usdt: Some(quantity * price),
                            error: None,
                            raw_response: None,
                        });
                    }
                }
                Ok(OrderResult::failure(formatted_symbol, side, response.mc))
            }
            MarketType::Swap => {
                let position_side = match side { OrderSide::Buy => "LONG", OrderSide::Sell => "SHORT" };
                let body = serde_json::json!({
                    "symbol": formatted_symbol,
                    "orderSide": match side { OrderSide::Buy => "BUY", OrderSide::Sell => "SELL" },
                    "orderType": "LIMIT",
                    "origQty": format!("{:.8}", quantity),
                    "price": format!("{:.8}", price),
                    "positionSide": position_side,
                    "timeInForce": "GTC",
                });
                let response: XtFuturesResponse<FuturesOrderResult> = self.signed_futures_post("/future/trade/v1/order/create", &body).await?;
                if response.is_success() {
                    if let Some(result) = response.result {
                        return Ok(OrderResult {
                            success: true,
                            order_id: Some(result.order_id.to_string()),
                            symbol: formatted_symbol,
                            side,
                            order_type: OrderType::Limit,
                            quantity: Some(quantity),
                            entry_price: Some(price),
                            position_size: Some(quantity),
                            position_usdt: Some(quantity * price),
                            error: None,
                            raw_response: None,
                        });
                    }
                }
                Ok(OrderResult::failure(formatted_symbol, side, response.msg_info))
            }
        }
    }

    async fn cancel_order(&self, symbol: &str, order_id: &str) -> Result<bool> {
        let formatted_symbol = self.format_symbol(symbol);
        match self.market_type {
            MarketType::Spot => {
                let endpoint = format!("/order/{}", order_id);
                let response: XtSpotResponse<serde_json::Value> = self.signed_spot_delete(&endpoint, "").await?;
                Ok(response.is_success())
            }
            MarketType::Swap => {
                let body = serde_json::json!({
                    "orderId": order_id.parse::<i64>().unwrap_or(0),
                    "symbol": formatted_symbol,
                });
                let response: XtFuturesResponse<serde_json::Value> = self.signed_futures_post("/future/trade/v1/order/cancel", &body).await?;
                Ok(response.is_success())
            }
        }
    }

    async fn get_order_status(&self, symbol: &str, order_id: &str) -> Result<OrderInfo> {
        let formatted_symbol = self.format_symbol(symbol);
        match self.market_type {
            MarketType::Spot => {
                let endpoint = format!("/order/{}", order_id);
                let response: XtSpotResponse<SpotOrderInfo> = self.signed_spot_get(&endpoint, "").await?;
                if let Some(order) = response.result {
                    let side = if order.side == "BUY" { OrderSide::Buy } else { OrderSide::Sell };
                    let order_type = if order.order_type == "MARKET" { OrderType::Market } else { OrderType::Limit };
                    return Ok(OrderInfo {
                        order_id: order.order_id,
                        client_order_id: order.client_order_id,
                        symbol: order.symbol,
                        status: parse_order_status_str(&order.state),
                        side,
                        order_type,
                        price: order.price.and_then(|p| p.parse().ok()).unwrap_or(0.0),
                        quantity: order.orig_qty.parse().unwrap_or(0.0),
                        filled_quantity: order.executed_qty.parse().unwrap_or(0.0),
                        remaining_quantity: order.orig_qty.parse().unwrap_or(0.0) - order.executed_qty.parse().unwrap_or(0.0),
                        avg_price: order.avg_price.and_then(|p| p.parse().ok()),
                        timestamp: order.time,
                        time_in_force: TimeInForce::GTC,
                        reduce_only: false,
                    });
                }
                anyhow::bail!("Order not found")
            }
            MarketType::Swap => {
                let params = format!("orderId={}&symbol={}", order_id.parse::<i64>().unwrap_or(0), formatted_symbol);
                let response: XtFuturesResponse<FuturesOrderInfo> = self.signed_futures_get("/future/trade/v1/order/detail", &params).await?;
                if let Some(order) = response.result {
                    let side = if order.order_side == "BUY" { OrderSide::Buy } else { OrderSide::Sell };
                    let order_type = if order.order_type == "MARKET" { OrderType::Market } else { OrderType::Limit };
                    return Ok(OrderInfo {
                        order_id: order.order_id.to_string(),
                        client_order_id: order.client_order_id,
                        symbol: order.symbol,
                        status: parse_order_status_str(&order.order_status),
                        side,
                        order_type,
                        price: order.price.and_then(|p| p.parse().ok()).unwrap_or(0.0),
                        quantity: order.orig_qty.parse().unwrap_or(0.0),
                        filled_quantity: order.executed_qty.parse().unwrap_or(0.0),
                        remaining_quantity: order.orig_qty.parse().unwrap_or(0.0) - order.executed_qty.parse().unwrap_or(0.0),
                        avg_price: order.avg_price.and_then(|p| p.parse().ok()),
                        timestamp: order.created_time,
                        time_in_force: TimeInForce::GTC,
                        reduce_only: false,
                    });
                }
                anyhow::bail!("Order not found")
            }
        }
    }

    async fn close_position(&self, symbol: &str) -> Result<OrderResult> {
        if self.market_type == MarketType::Spot {
            anyhow::bail!("Close position not supported for spot trading");
        }
        let formatted_symbol = self.format_symbol(symbol);
        let position = self.get_position(&formatted_symbol).await?;
        match position {
            Some(pos) => {
                let size: f64 = pos.position_size.parse().context("Failed to parse position size")?;
                if size.abs() < 0.000001 {
                    return Ok(OrderResult::failure(formatted_symbol, OrderSide::Sell, "No position to close".to_string()));
                }
                let (close_side, position_side) = if pos.position_side == "LONG" {
                    (OrderSide::Sell, "LONG")
                } else {
                    (OrderSide::Buy, "SHORT")
                };
                let body = serde_json::json!({
                    "symbol": formatted_symbol,
                    "orderSide": match close_side { OrderSide::Buy => "BUY", OrderSide::Sell => "SELL" },
                    "orderType": "MARKET",
                    "origQty": format!("{:.8}", size.abs()),
                    "positionSide": position_side,
                    "reduceOnly": true,
                });
                let response: XtFuturesResponse<FuturesOrderResult> = self.signed_futures_post("/future/trade/v1/order/create", &body).await?;
                if response.is_success() {
                    if let Some(result) = response.result {
                        return Ok(OrderResult {
                            success: true,
                            order_id: Some(result.order_id.to_string()),
                            symbol: formatted_symbol,
                            side: close_side,
                            order_type: OrderType::Market,
                            quantity: Some(size.abs()),
                            entry_price: None,
                            position_size: Some(0.0),
                            position_usdt: None,
                            error: None,
                            raw_response: None,
                        });
                    }
                }
                Ok(OrderResult::failure(formatted_symbol, close_side, response.msg_info))
            }
            None => Ok(OrderResult::failure(formatted_symbol, OrderSide::Sell, "No position found".to_string())),
        }
    }

    fn format_symbol(&self, base_symbol: &str) -> String {
        let s = base_symbol.to_lowercase();
        if s.contains('/') { return s.replace('/', "_"); }
        if s.contains('-') { return s.replace('-', "_"); }
        if s.contains('_') { return s; }
        if s.len() > 4 && s.ends_with("usdt") {
            let base = s.trim_end_matches("usdt");
            return format!("{}_usdt", base);
        }
        if s.len() > 4 && s.ends_with("usdc") {
            let base = s.trim_end_matches("usdc");
            return format!("{}_usdc", base);
        }
        if s.len() > 3 && s.ends_with("btc") {
            let base = s.trim_end_matches("btc");
            return format!("{}_btc", base);
        }
        format!("{}_usdt", s)
    }

    async fn test_ws_orderbook(&self, symbol: &str, depth: u32, timeout_ms: u64) -> Result<WsTestResult> {
        let formatted_symbol = self.format_symbol(symbol);
        let ws_url = self.ws_public_url();
        let start = std::time::Instant::now();
        let (mut ws_stream, _) = connect_async(ws_url).await.context("Failed to connect to XT WebSocket")?;
        let connect_time = start.elapsed().as_millis() as u64;
        let levels = if depth <= 5 { 5 } else if depth <= 10 { 10 } else if depth <= 20 { 20 } else { 50 };
        let subscribe_msg = match self.market_type {
            MarketType::Spot => serde_json::json!({"method": "subscribe", "id": format!("{}", timestamp_ms()), "params": [format!("depth@{},{}", formatted_symbol, levels)]}),
            MarketType::Swap => serde_json::json!({"method": "SUBSCRIBE", "id": format!("{}", timestamp_ms()), "params": [format!("depth@{},{}", formatted_symbol, levels)]}),
        };
        ws_stream.send(Message::Text(subscribe_msg.to_string())).await?;
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
                        Some(Ok(Message::Ping(data))) => { let _ = ws_stream.send(Message::Pong(data)).await; }
                        Some(Ok(_)) => {}
                        Some(Err(e)) => { return Err(anyhow::anyhow!("WebSocket error: {}", e)); }
                        None => break,
                    }
                }
                _ = tokio::time::sleep(timeout) => { break; }
            }
        }
        let _ = ws_stream.close(None).await;
        Ok(WsTestResult { connected: true, latency_ms: connect_time, messages_received, subscription_type: format!("depth@{}", symbol), data_sample })
    }

    async fn test_ws_trades(&self, symbol: &str, timeout_ms: u64) -> Result<WsTestResult> {
        let formatted_symbol = self.format_symbol(symbol);
        let ws_url = self.ws_public_url();
        let start = std::time::Instant::now();
        let (mut ws_stream, _) = connect_async(ws_url).await.context("Failed to connect to XT WebSocket")?;
        let connect_time = start.elapsed().as_millis() as u64;
        let subscribe_msg = match self.market_type {
            MarketType::Spot => serde_json::json!({"method": "subscribe", "id": format!("{}", timestamp_ms()), "params": [format!("trade@{}", formatted_symbol)]}),
            MarketType::Swap => serde_json::json!({"method": "SUBSCRIBE", "id": format!("{}", timestamp_ms()), "params": [format!("trade@{}", formatted_symbol)]}),
        };
        ws_stream.send(Message::Text(subscribe_msg.to_string())).await?;
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
                                if parsed.topic.is_some() && parsed.data.is_some() { data_sample = parsed.data; break; }
                            }
                        }
                        Some(Ok(Message::Ping(data))) => { let _ = ws_stream.send(Message::Pong(data)).await; }
                        Some(Ok(_)) => {}
                        Some(Err(e)) => { return Err(anyhow::anyhow!("WebSocket error: {}", e)); }
                        None => break,
                    }
                }
                _ = tokio::time::sleep(timeout) => { break; }
            }
        }
        let _ = ws_stream.close(None).await;
        Ok(WsTestResult { connected: true, latency_ms: connect_time, messages_received, subscription_type: format!("trade@{}", symbol), data_sample })
    }

    async fn test_ws_ticker(&self, symbol: &str, timeout_ms: u64) -> Result<WsTestResult> {
        let formatted_symbol = self.format_symbol(symbol);
        let ws_url = self.ws_public_url();
        let start = std::time::Instant::now();
        let (mut ws_stream, _) = connect_async(ws_url).await.context("Failed to connect to XT WebSocket")?;
        let connect_time = start.elapsed().as_millis() as u64;
        let subscribe_msg = match self.market_type {
            MarketType::Spot => serde_json::json!({"method": "subscribe", "id": format!("{}", timestamp_ms()), "params": [format!("ticker@{}", formatted_symbol)]}),
            MarketType::Swap => serde_json::json!({"method": "SUBSCRIBE", "id": format!("{}", timestamp_ms()), "params": [format!("agg_ticker@{}", formatted_symbol)]}),
        };
        ws_stream.send(Message::Text(subscribe_msg.to_string())).await?;
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
                                if parsed.topic.is_some() && parsed.data.is_some() { data_sample = parsed.data; break; }
                            }
                        }
                        Some(Ok(Message::Ping(data))) => { let _ = ws_stream.send(Message::Pong(data)).await; }
                        Some(Ok(_)) => {}
                        Some(Err(e)) => { return Err(anyhow::anyhow!("WebSocket error: {}", e)); }
                        None => break,
                    }
                }
                _ = tokio::time::sleep(timeout) => { break; }
            }
        }
        let _ = ws_stream.close(None).await;
        let subscription_type = match self.market_type {
            MarketType::Spot => format!("ticker@{}", symbol),
            MarketType::Swap => format!("agg_ticker@{} (includes mark/index price)", symbol),
        };
        Ok(WsTestResult { connected: true, latency_ms: connect_time, messages_received, subscription_type, data_sample })
    }

    async fn subscribe_order_updates(&self, sender: broadcast::Sender<OrderUpdateEvent>) -> Result<JoinHandle<()>> {
        let api_key = self.api_key.clone();
        let api_secret = self.api_secret.clone();
        let market_type = self.market_type;
        let base_url = self.base_url.clone();
        let exchange_name = "xt".to_string();

        let handle = tokio::spawn(async move {
            let mut reconnect_delay = INITIAL_RECONNECT_DELAY_SECS;
            loop {
                let listen_key = match market_type {
                    MarketType::Spot => {
                        let client = Client::new();
                        let timestamp = timestamp_ms();
                        let path = "/v4/ws-token";
                        let recv_window = "5000";
                        let payload_string = format!(
                            "xt-validate-algorithms=HmacSHA256&xt-validate-appkey={}&xt-validate-recvwindow={}&xt-validate-timestamp={}#POST#{}",
                            api_key, recv_window, timestamp, path
                        );
                        let signature = hmac_sign(&api_secret, &payload_string);
                        let url = format!("{}{}", SPOT_URL, path);
                        match client.post(&url).header("Content-Type", "application/json").header("xt-validate-algorithms", "HmacSHA256").header("xt-validate-appkey", &api_key).header("xt-validate-timestamp", timestamp.to_string()).header("xt-validate-recvwindow", recv_window).header("xt-validate-signature", signature).body("{}").send().await {
                            Ok(resp) => {
                                if let Ok(text) = resp.text().await {
                                    if let Ok(json) = serde_json::from_str::<XtSpotResponse<WsTokenResponse>>(&text) {
                                        if let Some(result) = json.result { result.access_token.or(result.token) } else { None }
                                    } else { None }
                                } else { None }
                            }
                            Err(e) => { error!("[{}] Failed to get WS token: {}", exchange_name, e); None }
                        }
                    }
                    MarketType::Swap => {
                        let client = Client::new();
                        let timestamp = timestamp_ms();
                        let endpoint = "/future/user/v1/user/listen-key";
                        let payload_string = format!("xt-validate-appkey={}&xt-validate-timestamp={}#{}", api_key, timestamp, endpoint);
                        let signature = hmac_sign(&api_secret, &payload_string);
                        let url = format!("{}{}", base_url, endpoint);
                        match client.get(&url).header("Content-Type", "application/json").header("xt-validate-appkey", &api_key).header("xt-validate-timestamp", timestamp.to_string()).header("xt-validate-signature", signature).send().await {
                            Ok(resp) => {
                                if let Ok(text) = resp.text().await {
                                    if let Ok(json) = serde_json::from_str::<XtFuturesResponse<String>>(&text) { json.result } else { None }
                                } else { None }
                            }
                            Err(e) => { error!("[{}] Failed to get listen key: {}", exchange_name, e); None }
                        }
                    }
                };
                let listen_key = match listen_key {
                    Some(key) => key,
                    None => {
                        error!("[{}] No listen key available, retrying...", exchange_name);
                        tokio::time::sleep(tokio::time::Duration::from_secs(reconnect_delay)).await;
                        reconnect_delay = next_reconnect_delay(reconnect_delay);
                        continue;
                    }
                };
                let ws_url = match market_type { MarketType::Spot => SPOT_WS_PRIVATE_URL, MarketType::Swap => FUTURES_WS_PRIVATE_URL };
                info!("[{}] Connecting to private WS: {}", exchange_name, ws_url);
                match connect_async(ws_url).await {
                    Ok((mut ws_stream, _)) => {
                        let sub_msg = match market_type {
                            MarketType::Spot => serde_json::json!({"method": "subscribe", "id": format!("{}", timestamp_ms()), "params": ["order"], "listenKey": listen_key}),
                            MarketType::Swap => serde_json::json!({"method": "SUBSCRIBE", "id": format!("{}", timestamp_ms()), "params": [format!("order@{}", listen_key)]}),
                        };
                        if let Err(e) = ws_stream.send(Message::Text(sub_msg.to_string())).await {
                            error!("[{}] Failed to subscribe: {}", exchange_name, e);
                            tokio::time::sleep(tokio::time::Duration::from_secs(reconnect_delay)).await;
                            reconnect_delay = next_reconnect_delay(reconnect_delay);
                            continue;
                        }
                        info!("[{}] Subscribed to private order updates", exchange_name);
                        reconnect_delay = INITIAL_RECONNECT_DELAY_SECS;
                        let mut ping_interval = tokio::time::interval(tokio::time::Duration::from_secs(20));
                        loop {
                            tokio::select! {
                                msg = ws_stream.next() => {
                                    match msg {
                                        Some(Ok(Message::Text(text))) => {
                                            if let Ok(msg_json) = serde_json::from_str::<WsMessage>(&text) {
                                                if msg_json.id.is_some() && msg_json.topic.is_none() { debug!("[{}] WS subscription confirmed", exchange_name); continue; }
                                                let topic = msg_json.topic.as_deref();
                                                if topic == Some("order") {
                                                    if let Some(data) = msg_json.data {
                                                        let event = match market_type {
                                                            MarketType::Spot => {
                                                                if let Ok(order) = serde_json::from_value::<WsSpotOrderUpdate>(data.clone()) {
                                                                    Some(OrderUpdateEvent { exchange: exchange_name.clone(), order_id: order.order_id.clone(), client_order_id: order.client_order_id.clone(), symbol: order.s.clone(), status: parse_order_status_str(&order.status), side: if order.side == "BUY" { OrderSide::Buy } else { OrderSide::Sell }, order_type: if order.order_type == "MARKET" { OrderType::Market } else { OrderType::Limit }, price: order.price.parse().unwrap_or(0.0), quantity: order.quantity.parse().unwrap_or(0.0), filled_quantity: order.executed_qty.parse().unwrap_or(0.0), avg_fill_price: None, fee: None, fee_currency: None, exchange_timestamp: order.trade_time, received_at: timestamp_ms(), raw: Some(data) })
                                                                } else { None }
                                                            }
                                                            MarketType::Swap => {
                                                                if let Ok(order) = serde_json::from_value::<WsFuturesOrderUpdate>(data.clone()) {
                                                                    Some(OrderUpdateEvent { exchange: exchange_name.clone(), order_id: order.order_id.to_string(), client_order_id: order.client_order_id.clone(), symbol: order.symbol.clone(), status: parse_order_status_str(&order.order_status), side: if order.order_side == "BUY" { OrderSide::Buy } else { OrderSide::Sell }, order_type: if order.order_type == "MARKET" { OrderType::Market } else { OrderType::Limit }, price: order.price.as_ref().and_then(|p| p.parse().ok()).unwrap_or(0.0), quantity: order.orig_qty.parse().unwrap_or(0.0), filled_quantity: order.executed_qty.parse().unwrap_or(0.0), avg_fill_price: order.avg_price.as_ref().and_then(|p| p.parse().ok()), fee: order.fee.as_ref().and_then(|f| f.parse().ok()), fee_currency: order.fee_currency.clone(), exchange_timestamp: order.created_time, received_at: timestamp_ms(), raw: Some(data) })
                                                                } else { None }
                                                            }
                                                        };
                                                        if let Some(event) = event { log_order_update(&event); let _ = sender.send(event); }
                                                    }
                                                }
                                            }
                                        }
                                        Some(Ok(Message::Ping(data))) => { let _ = ws_stream.send(Message::Pong(data)).await; }
                                        Some(Ok(Message::Close(_))) => { warn!("[{}] Private WS closed by server", exchange_name); break; }
                                        Some(Err(e)) => { error!("[{}] Private WS error: {}", exchange_name, e); break; }
                                        None => { warn!("[{}] Private WS stream ended", exchange_name); break; }
                                        _ => {}
                                    }
                                }
                                _ = ping_interval.tick() => {
                                    let ping_msg = match market_type { MarketType::Spot => "ping", MarketType::Swap => r#"{"method":"PING"}"# };
                                    if let Err(e) = ws_stream.send(Message::Text(ping_msg.to_string())).await { error!("[{}] Failed to send ping: {}", exchange_name, e); break; }
                                }
                            }
                        }
                    }
                    Err(e) => { error!("[{}] Failed to connect private WS: {}", exchange_name, e); }
                }
                warn!("[{}] Reconnecting private WS in {}s...", exchange_name, reconnect_delay);
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

    fn create_test_trader() -> XtTrader { XtTrader::new("test_key", "test_secret", false, MarketType::Spot) }
    fn create_futures_trader() -> XtTrader { XtTrader::new("test_key", "test_secret", false, MarketType::Swap) }

    #[test]
    fn test_format_symbol() {
        let trader = create_test_trader();
        assert_eq!(trader.format_symbol("BTC/USDT"), "btc_usdt");
        assert_eq!(trader.format_symbol("BTC-USDT"), "btc_usdt");
        assert_eq!(trader.format_symbol("BTCUSDT"), "btc_usdt");
        assert_eq!(trader.format_symbol("btc_usdt"), "btc_usdt");
        assert_eq!(trader.format_symbol("BTC"), "btc_usdt");
        assert_eq!(trader.format_symbol("ETH/BTC"), "eth_btc");
        assert_eq!(trader.format_symbol("ETHBTC"), "eth_btc");
        assert_eq!(trader.format_symbol("ETHUSDT"), "eth_usdt");
    }

    #[test]
    fn test_exchange_name() { assert_eq!(create_test_trader().exchange_name(), "xt"); }

    #[test]
    fn test_market_type() {
        assert_eq!(create_test_trader().market_type(), MarketType::Spot);
        assert_eq!(create_futures_trader().market_type(), MarketType::Swap);
    }

    #[test]
    fn test_base_url_selection() {
        assert_eq!(create_test_trader().base_url, SPOT_URL);
        assert_eq!(create_futures_trader().base_url, FUTURES_URL);
    }

    #[test]
    fn test_parse_order_status() {
        assert_eq!(parse_order_status_str("NEW"), OrderStatus::New);
        assert_eq!(parse_order_status_str("FILLED"), OrderStatus::Filled);
        assert_eq!(parse_order_status_str("PARTIALLY_FILLED"), OrderStatus::PartiallyFilled);
        assert_eq!(parse_order_status_str("CANCELLED"), OrderStatus::Cancelled);
    }

    #[test]
    fn test_next_reconnect_delay() {
        assert_eq!(next_reconnect_delay(1), 2);
        assert_eq!(next_reconnect_delay(2), 4);
        assert_eq!(next_reconnect_delay(16), 30);
        assert_eq!(next_reconnect_delay(30), 30);
    }

    #[test]
    fn test_ws_urls() {
        assert_eq!(create_test_trader().ws_public_url(), SPOT_WS_PUBLIC_URL);
        assert_eq!(create_test_trader().ws_private_url(), SPOT_WS_PRIVATE_URL);
        assert_eq!(create_futures_trader().ws_public_url(), FUTURES_WS_PUBLIC_URL);
        assert_eq!(create_futures_trader().ws_private_url(), FUTURES_WS_PRIVATE_URL);
    }
}
