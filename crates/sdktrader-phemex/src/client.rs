//! Phemex exchange trader implementation.
//!
//! Full-featured implementation based on CCXT phemex.ts with improvements:
//! - REST API for trading and market data
//! - WebSocket connections for real-time data
//! - Testnet support
//! - HMAC-SHA256 authentication
//! - Private WebSocket subscription for order updates

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

const MAINNET_URL: &str = "https://api.phemex.com";
const TESTNET_URL: &str = "https://testnet-api.phemex.com";
const MAINNET_WS_URL: &str = "wss://ws.phemex.com";
const TESTNET_WS_URL: &str = "wss://testnet-ws.phemex.com";

/// Private WebSocket reconnect constants.
const INITIAL_RECONNECT_DELAY_SECS: u64 = 1;
const MAX_RECONNECT_DELAY_SECS: u64 = 30;

/// Value scales for Phemex API conversions.
const PRICE_SCALE_EP: i32 = 4; // For inverse contracts (BTCUSD etc.)
const RATIO_SCALE_ER: i32 = 8; // For ratios (funding rates)
const VALUE_SCALE_EV: i32 = 8; // For values (balances, quantities)

/// Calculate next reconnect delay with exponential backoff.
fn next_reconnect_delay(current_secs: u64) -> u64 {
    (current_secs * 2).min(MAX_RECONNECT_DELAY_SECS)
}

/// Parse order status string to OrderStatus enum.
fn parse_order_status_str(s: &str) -> OrderStatus {
    match s.to_lowercase().as_str() {
        "new" | "created" | "init" | "untriggered" | "triggered" => OrderStatus::New,
        "partially_filled" | "partiallyfilled" | "partial" => OrderStatus::PartiallyFilled,
        "filled" | "closed" | "full" => OrderStatus::Filled,
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

/// Convert from Phemex scaled value (Ep, Ev, Er) to float.
fn from_ep(value: i64, scale: i32) -> f64 {
    let divisor = 10_f64.powi(scale);
    value as f64 / divisor
}

/// Convert to Phemex scaled value.
fn to_ep(value: f64, scale: i32) -> i64 {
    let multiplier = 10_f64.powi(scale);
    (value * multiplier).round() as i64
}

/// Phemex exchange trader.
pub struct PhemexTrader {
    api_key: String,
    api_secret: String,
    testnet: bool,
    market_type: MarketType,
    client: Client,
    base_url: String,
}

impl PhemexTrader {
    /// Create a new Phemex trader.
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

    /// Sign a request using HMAC-SHA256.
    /// Phemex signature: HMAC-SHA256(requestPath + queryString + expiry + body)
    fn sign_request(
        &self,
        request_path: &str,
        query_string: &str,
        expiry: i64,
        body: &str,
    ) -> String {
        let sign_str = format!("{}{}{}{}", request_path, query_string, expiry, body);
        hmac_sign(&self.api_secret, &sign_str)
    }

    /// Get WebSocket URL based on testnet setting.
    fn ws_url(&self) -> &str {
        if self.testnet {
            TESTNET_WS_URL
        } else {
            MAINNET_WS_URL
        }
    }

    /// Generate WebSocket authentication signature.
    fn ws_auth_signature(&self, expiry: i64) -> String {
        let sign_str = format!("{}{}", self.api_key, expiry);
        hmac_sign(&self.api_secret, &sign_str)
    }

    /// Check if using linear perpetuals (USDT-settled).
    fn is_linear(&self) -> bool {
        self.market_type == MarketType::Swap
    }

    /// Make a public GET request.
    async fn public_get<T: for<'de> serde::Deserialize<'de>>(
        &self,
        endpoint: &str,
        params: &str,
    ) -> Result<T> {
        let url = if params.is_empty() {
            format!("{}{}", self.base_url, endpoint)
        } else {
            format!("{}{}?{}", self.base_url, endpoint, params)
        };

        debug!("Phemex public GET: {}", url);

        let response = self
            .client
            .get(&url)
            .send()
            .await
            .context("Failed to send request to Phemex")?;

        let text = response.text().await?;
        debug!("Phemex response: {}", text);

        serde_json::from_str(&text).context("Failed to parse Phemex response")
    }

    /// Make a signed GET request.
    async fn signed_get<T: for<'de> serde::Deserialize<'de>>(
        &self,
        endpoint: &str,
        params: &str,
    ) -> Result<T> {
        let expiry = (timestamp_ms() / 1000) + 60; // 60 seconds expiry
        let query_string = if params.is_empty() {
            String::new()
        } else {
            format!("?{}", params)
        };
        let signature = self.sign_request(endpoint, &query_string, expiry, "");

        let url = format!("{}{}{}", self.base_url, endpoint, query_string);

        let response = self
            .client
            .get(&url)
            .header("x-phemex-access-token", &self.api_key)
            .header("x-phemex-request-expiry", expiry.to_string())
            .header("x-phemex-request-signature", signature)
            .send()
            .await
            .context("Failed to send request to Phemex")?;

        let text = response.text().await?;
        debug!("Phemex signed GET response: {}", text);

        serde_json::from_str(&text).context("Failed to parse Phemex response")
    }

    /// Make a signed POST request.
    async fn signed_post<T: for<'de> serde::Deserialize<'de>>(
        &self,
        endpoint: &str,
        body: &serde_json::Value,
    ) -> Result<T> {
        let expiry = (timestamp_ms() / 1000) + 60;
        let body_str = body.to_string();
        let signature = self.sign_request(endpoint, "", expiry, &body_str);

        let url = format!("{}{}", self.base_url, endpoint);

        let response = self
            .client
            .post(&url)
            .header("Content-Type", "application/json")
            .header("x-phemex-access-token", &self.api_key)
            .header("x-phemex-request-expiry", expiry.to_string())
            .header("x-phemex-request-signature", signature)
            .body(body_str)
            .send()
            .await
            .context("Failed to send request to Phemex")?;

        let text = response.text().await?;
        debug!("Phemex signed POST response: {}", text);

        serde_json::from_str(&text).context("Failed to parse Phemex response")
    }

    /// Make a signed PUT request.
    async fn signed_put<T: for<'de> serde::Deserialize<'de>>(
        &self,
        endpoint: &str,
        params: &str,
    ) -> Result<T> {
        let expiry = (timestamp_ms() / 1000) + 60;
        let query_string = if params.is_empty() {
            String::new()
        } else {
            format!("?{}", params)
        };
        let signature = self.sign_request(endpoint, &query_string, expiry, "");

        let url = format!("{}{}{}", self.base_url, endpoint, query_string);

        let response = self
            .client
            .put(&url)
            .header("x-phemex-access-token", &self.api_key)
            .header("x-phemex-request-expiry", expiry.to_string())
            .header("x-phemex-request-signature", signature)
            .send()
            .await
            .context("Failed to send request to Phemex")?;

        let text = response.text().await?;
        debug!("Phemex signed PUT response: {}", text);

        serde_json::from_str(&text).context("Failed to parse Phemex response")
    }

    /// Make a signed DELETE request.
    async fn signed_delete<T: for<'de> serde::Deserialize<'de>>(
        &self,
        endpoint: &str,
        params: &str,
    ) -> Result<T> {
        let expiry = (timestamp_ms() / 1000) + 60;
        let query_string = if params.is_empty() {
            String::new()
        } else {
            format!("?{}", params)
        };
        let signature = self.sign_request(endpoint, &query_string, expiry, "");

        let url = format!("{}{}{}", self.base_url, endpoint, query_string);

        let response = self
            .client
            .delete(&url)
            .header("x-phemex-access-token", &self.api_key)
            .header("x-phemex-request-expiry", expiry.to_string())
            .header("x-phemex-request-signature", signature)
            .send()
            .await
            .context("Failed to send request to Phemex")?;

        let text = response.text().await?;
        debug!("Phemex signed DELETE response: {}", text);

        serde_json::from_str(&text).context("Failed to parse Phemex response")
    }

    /// Place an order via REST API.
    async fn place_order_rest(
        &self,
        symbol: &str,
        side: OrderSide,
        order_type: OrderType,
        quantity: f64,
        price: Option<f64>,
        reduce_only: bool,
    ) -> Result<OrderResult> {
        let side_str = match side {
            OrderSide::Buy => "Buy",
            OrderSide::Sell => "Sell",
        };

        let ord_type = match order_type {
            OrderType::Market => "Market",
            OrderType::Limit => "Limit",
            _ => "Market",
        };

        let cl_ord_id = format!("SDK{}", timestamp_ms());

        if self.is_linear() {
            // Linear perpetual (USDT-settled) - use g-orders endpoint
            let mut body = serde_json::json!({
                "symbol": symbol,
                "side": side_str,
                "ordType": ord_type,
                "orderQtyRq": format!("{:.8}", quantity),
                "clOrdID": cl_ord_id,
            });

            if let (OrderType::Limit, Some(p)) = (order_type, price) {
                body["priceRp"] = serde_json::json!(format!("{:.8}", p));
                body["timeInForce"] = serde_json::json!("GoodTillCancel");
            }

            if reduce_only {
                body["reduceOnly"] = serde_json::json!(true);
            }

            let response: PhemexResponse<OrderData> = self.signed_post("/g-orders", &body).await?;

            if response.is_success() {
                if let Some(data) = response.data {
                    let order_id = data.order_id.unwrap_or_default();
                    info!("Phemex order placed: {}", order_id);
                    return Ok(OrderResult {
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

            return Ok(OrderResult::failure(symbol.to_string(), side, response.msg));
        } else {
            // Spot trading - use spot/orders endpoint
            let mut body = serde_json::json!({
                "symbol": symbol,
                "side": side_str,
                "ordType": ord_type,
                "qtyType": "ByBase",
                "baseQtyEv": to_ep(quantity, VALUE_SCALE_EV),
                "clOrdID": cl_ord_id,
            });

            if let (OrderType::Limit, Some(p)) = (order_type, price) {
                body["priceEp"] = serde_json::json!(to_ep(p, VALUE_SCALE_EV));
                body["timeInForce"] = serde_json::json!("GoodTillCancel");
            }

            let response: PhemexResponse<OrderData> =
                self.signed_post("/spot/orders", &body).await?;

            if response.is_success() {
                if let Some(data) = response.data {
                    let order_id = data.order_id.unwrap_or_default();
                    info!("Phemex spot order placed: {}", order_id);
                    return Ok(OrderResult {
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

            return Ok(OrderResult::failure(symbol.to_string(), side, response.msg));
        }
    }

    /// Get current position for a symbol (futures only).
    async fn get_position(&self, symbol: &str) -> Result<Option<PositionInfo>> {
        if self.market_type == MarketType::Spot {
            return Ok(None);
        }

        let params = format!("symbol={}", symbol);
        let response: PhemexResponse<AccountPositionsData> = self
            .signed_get("/g-accounts/accountPositions", &params)
            .await?;

        if let Some(data) = response.data {
            for pos in data.positions {
                if pos.symbol == symbol {
                    let size = if let Some(size_rq) = &pos.size_rq {
                        size_rq.parse::<f64>().unwrap_or(0.0)
                    } else if let Some(size) = pos.size {
                        size as f64
                    } else {
                        0.0
                    };

                    if size.abs() > 0.000001 {
                        return Ok(Some(pos));
                    }
                }
            }
        }

        Ok(None)
    }

    /// Set Take Profit and Stop Loss for a position (futures only).
    async fn set_trading_stop(
        &self,
        symbol: &str,
        take_profit: Option<f64>,
        stop_loss: Option<f64>,
        pos_side: &str,
    ) -> Result<bool> {
        if self.market_type == MarketType::Spot {
            return Ok(true); // TP/SL not supported for spot
        }

        // Phemex uses separate conditional orders for TP/SL
        // This is a simplified implementation
        if let Some(tp) = take_profit {
            let side = if pos_side == "Buy" { "Sell" } else { "Buy" };
            let body = serde_json::json!({
                "symbol": symbol,
                "side": side,
                "ordType": "LimitIfTouched",
                "triggerType": "ByMarkPrice",
                "stopPxRp": format!("{:.8}", tp),
                "priceRp": format!("{:.8}", tp),
                "orderQtyRq": "0", // Close all
                "reduceOnly": true,
                "closeOnTrigger": true,
            });

            let _response: PhemexResponse<serde_json::Value> =
                self.signed_post("/g-orders", &body).await?;
        }

        if let Some(sl) = stop_loss {
            let side = if pos_side == "Buy" { "Sell" } else { "Buy" };
            let body = serde_json::json!({
                "symbol": symbol,
                "side": side,
                "ordType": "Stop",
                "triggerType": "ByMarkPrice",
                "stopPxRp": format!("{:.8}", sl),
                "orderQtyRq": "0", // Close all
                "reduceOnly": true,
                "closeOnTrigger": true,
            });

            let _response: PhemexResponse<serde_json::Value> =
                self.signed_post("/g-orders", &body).await?;
        }

        info!(
            "Set trading stop for {}: TP={:?}, SL={:?}",
            symbol, take_profit, stop_loss
        );
        Ok(true)
    }
}

#[async_trait]
impl ExchangeTrader for PhemexTrader {
    fn exchange_name(&self) -> &str {
        "phemex"
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
        // Use public endpoint to test connection
        let response: PhemexResultResponse<serde_json::Value> = self
            .client
            .get(format!("{}/v1/md/ticker/24hr?symbol=BTCUSD", self.base_url))
            .send()
            .await?
            .json()
            .await?;

        Ok(response.is_success())
    }

    async fn get_server_time(&self) -> Result<i64> {
        // Phemex doesn't have a dedicated server time endpoint
        // We'll use the timestamp from a ticker response
        let response: PhemexResultResponse<SpotTickerData> = self
            .client
            .get(format!("{}/v1/md/ticker/24hr?symbol=BTCUSD", self.base_url))
            .send()
            .await?
            .json()
            .await?;

        if let Some(result) = response.result {
            if let Some(ts) = result.timestamp {
                // Phemex returns nanosecond timestamps
                return Ok(ts / 1_000_000);
            }
        }

        // Fallback to local time
        Ok(timestamp_ms())
    }

    async fn get_balance(&self) -> Result<AccountBalance> {
        if self.market_type == MarketType::Spot {
            let response: PhemexResponse<SpotWalletData> =
                self.signed_get("/spot/wallets", "").await?;

            if let Some(data) = response.data {
                // Find USDT balance
                for wallet in data.wallets {
                    if wallet.currency == "USDT" {
                        let total = from_ep(wallet.balance_ev, VALUE_SCALE_EV);
                        let locked = from_ep(
                            wallet.locked_trading_balance_ev + wallet.locked_withdraw_ev,
                            VALUE_SCALE_EV,
                        );
                        let available = total - locked;
                        return Ok(AccountBalance::simple(available, total, "USDT"));
                    }
                }
            }

            return Ok(AccountBalance::default());
        } else {
            // Futures account
            let response: PhemexResponse<AccountPositionsData> = self
                .signed_get("/g-accounts/accountPositions", "currency=USDT")
                .await?;

            if let Some(data) = response.data {
                let account = data.account;
                let available = if let Some(avail) = &account.avail_balance_rv {
                    avail.parse().unwrap_or(0.0)
                } else if let Some(avail) = account.avail_balance_ev {
                    from_ep(avail, VALUE_SCALE_EV)
                } else {
                    0.0
                };

                let total = if let Some(bal) = &account.account_balance_rv {
                    bal.parse().unwrap_or(available)
                } else if let Some(bal) = account.account_balance_ev {
                    from_ep(bal, VALUE_SCALE_EV)
                } else {
                    available
                };

                return Ok(AccountBalance::simple(available, total, &account.currency));
            }

            Ok(AccountBalance::default())
        }
    }

    async fn get_current_price(&self, symbol: &str) -> Result<f64> {
        let formatted_symbol = self.format_symbol(symbol);

        if self.is_linear() {
            // Linear perpetual
            let params = format!("symbol={}", formatted_symbol);
            let response: PhemexResultResponse<PerpTickerData> =
                self.public_get("/v2/md/v2/ticker/24hr", &params).await?;

            if let Some(result) = response.result {
                if let Some(price_str) = result.close_rp {
                    let price: f64 = price_str.parse().context("Failed to parse price")?;
                    return Ok(price);
                }
            }
        } else {
            // Spot
            let params = format!("symbol={}", formatted_symbol);
            let response: PhemexResultResponse<SpotTickerData> =
                self.public_get("/v1/md/spot/ticker/24hr", &params).await?;

            if let Some(result) = response.result {
                if let Some(last_ep) = result.last_ep {
                    return Ok(from_ep(last_ep, VALUE_SCALE_EV));
                }
            }
        }

        anyhow::bail!("Failed to get price for {}", symbol)
    }

    async fn get_ticker(&self, symbol: &str) -> Result<TickerInfo> {
        let formatted_symbol = self.format_symbol(symbol);

        if self.is_linear() {
            let params = format!("symbol={}", formatted_symbol);
            let response: PhemexResultResponse<PerpTickerData> =
                self.public_get("/v2/md/v2/ticker/24hr", &params).await?;

            if let Some(t) = response.result {
                let last_price = t
                    .close_rp
                    .as_ref()
                    .and_then(|p| p.parse().ok())
                    .unwrap_or(0.0);
                let timestamp = t
                    .timestamp
                    .as_ref()
                    .and_then(|ts| ts.parse::<i64>().ok())
                    .map(|ts| ts / 1_000_000)
                    .unwrap_or_else(timestamp_ms);

                return Ok(TickerInfo {
                    symbol: formatted_symbol,
                    last_price,
                    bid_price: None,
                    ask_price: None,
                    high_24h: t.high_rp.as_ref().and_then(|p| p.parse().ok()),
                    low_24h: t.low_rp.as_ref().and_then(|p| p.parse().ok()),
                    volume_24h: t.volume_rq.as_ref().and_then(|p| p.parse().ok()),
                    turnover_24h: t.turnover_rv.as_ref().and_then(|p| p.parse().ok()),
                    change_24h: None,
                    open_interest: t.open_interest_rv.as_ref().and_then(|p| p.parse().ok()),
                    open_interest_value: None,
                    funding_rate: t.funding_rate_rr.as_ref().and_then(|p| p.parse().ok()),
                    next_funding_time: None,
                    mark_price: t.mark_price_rp.as_ref().and_then(|p| p.parse().ok()),
                    index_price: t.index_price_rp.as_ref().and_then(|p| p.parse().ok()),
                    timestamp,
                });
            }
        } else {
            let params = format!("symbol={}", formatted_symbol);
            let response: PhemexResultResponse<SpotTickerData> =
                self.public_get("/v1/md/spot/ticker/24hr", &params).await?;

            if let Some(t) = response.result {
                let timestamp = t
                    .timestamp
                    .map(|ts| ts / 1_000_000)
                    .unwrap_or_else(timestamp_ms);

                return Ok(TickerInfo {
                    symbol: formatted_symbol,
                    last_price: t.last_ep.map(|v| from_ep(v, VALUE_SCALE_EV)).unwrap_or(0.0),
                    bid_price: t.bid_ep.map(|v| from_ep(v, VALUE_SCALE_EV)),
                    ask_price: t.ask_ep.map(|v| from_ep(v, VALUE_SCALE_EV)),
                    high_24h: t.high_ep.map(|v| from_ep(v, VALUE_SCALE_EV)),
                    low_24h: t.low_ep.map(|v| from_ep(v, VALUE_SCALE_EV)),
                    volume_24h: t.volume_ev.map(|v| from_ep(v, VALUE_SCALE_EV)),
                    turnover_24h: t.turnover_ev.map(|v| from_ep(v, VALUE_SCALE_EV)),
                    change_24h: None,
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

    async fn get_orderbook(&self, symbol: &str, _depth: u32) -> Result<Orderbook> {
        let formatted_symbol = self.format_symbol(symbol);

        let params = format!("symbol={}", formatted_symbol);
        let response: PhemexResultResponse<OrderbookData> = if self.is_linear() {
            self.public_get("/v2/md/v2/orderbook", &params).await?
        } else {
            self.public_get("/v1/md/orderbook", &params).await?
        };

        if let Some(result) = response.result {
            let bids: Vec<OrderbookLevel> = result
                .book
                .bids
                .iter()
                .filter_map(|level| {
                    if level.len() >= 2 {
                        let price = match &level[0] {
                            serde_json::Value::Number(n) => n.as_f64(),
                            serde_json::Value::String(s) => s.parse().ok(),
                            _ => None,
                        }?;
                        let quantity = match &level[1] {
                            serde_json::Value::Number(n) => n.as_f64(),
                            serde_json::Value::String(s) => s.parse().ok(),
                            _ => None,
                        }?;
                        Some(OrderbookLevel::new(price, quantity))
                    } else {
                        None
                    }
                })
                .collect();

            let asks: Vec<OrderbookLevel> = result
                .book
                .asks
                .iter()
                .filter_map(|level| {
                    if level.len() >= 2 {
                        let price = match &level[0] {
                            serde_json::Value::Number(n) => n.as_f64(),
                            serde_json::Value::String(s) => s.parse().ok(),
                            _ => None,
                        }?;
                        let quantity = match &level[1] {
                            serde_json::Value::Number(n) => n.as_f64(),
                            serde_json::Value::String(s) => s.parse().ok(),
                            _ => None,
                        }?;
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
                timestamp: result.timestamp,
            });
        }

        anyhow::bail!("Failed to get orderbook for {}", symbol)
    }

    async fn set_leverage(&self, symbol: Option<&str>, leverage: u32) -> Result<bool> {
        if self.market_type == MarketType::Spot {
            return Ok(true);
        }

        let sym = symbol.ok_or_else(|| anyhow::anyhow!("Symbol required for leverage setting"))?;
        let formatted_symbol = self.format_symbol(sym);

        let params = format!("symbol={}&leverageRr={}", formatted_symbol, leverage);
        let response: PhemexResponse<serde_json::Value> =
            self.signed_put("/g-positions/leverage", &params).await?;

        if response.is_success() {
            info!("Set leverage for {} to {}x", formatted_symbol, leverage);
            Ok(true)
        } else {
            // Leverage not changed is not an error
            if response.msg.contains("not changed") || response.code == 11004 {
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
                let pos_side = if is_long { "Buy" } else { "Sell" };

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
                    .set_trading_stop(&formatted_symbol, take_profit, stop_loss, pos_side)
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

    async fn place_limit_order(
        &self,
        symbol: &str,
        side: OrderSide,
        quantity: f64,
        price: f64,
        reduce_only: bool,
    ) -> Result<OrderResult> {
        let formatted_symbol = self.format_symbol(symbol);
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

    async fn cancel_order(&self, symbol: &str, order_id: &str) -> Result<bool> {
        let formatted_symbol = self.format_symbol(symbol);

        if self.is_linear() {
            let params = format!("symbol={}&orderID={}", formatted_symbol, order_id);
            let response: PhemexResponse<serde_json::Value> =
                self.signed_delete("/g-orders/cancel", &params).await?;

            if response.is_success() {
                info!("Phemex order cancelled: {}", order_id);
                return Ok(true);
            } else {
                error!("Failed to cancel order {}: {}", order_id, response.msg);
                return Ok(false);
            }
        } else {
            let params = format!("symbol={}&orderID={}", formatted_symbol, order_id);
            let response: PhemexResponse<serde_json::Value> =
                self.signed_delete("/spot/orders", &params).await?;

            if response.is_success() {
                info!("Phemex spot order cancelled: {}", order_id);
                return Ok(true);
            } else {
                error!("Failed to cancel spot order {}: {}", order_id, response.msg);
                return Ok(false);
            }
        }
    }

    async fn get_order_status(&self, symbol: &str, order_id: &str) -> Result<OrderInfo> {
        let formatted_symbol = self.format_symbol(symbol);

        if self.is_linear() {
            let params = format!("symbol={}&orderID={}", formatted_symbol, order_id);
            let response: PhemexResponse<Vec<OrderData>> = self
                .signed_get("/api-data/g-futures/orders/by-order-id", &params)
                .await?;

            if let Some(orders) = response.data {
                if let Some(order) = orders.first() {
                    let status_str = order.ord_status.as_deref().unwrap_or("New");
                    let status = parse_order_status_str(status_str);

                    let side_str = order.side.as_deref().unwrap_or("Buy");
                    let side = if side_str == "Sell" {
                        OrderSide::Sell
                    } else {
                        OrderSide::Buy
                    };

                    let order_type_str = order.ord_type.as_deref().unwrap_or("Market");
                    let order_type = if order_type_str == "Limit" {
                        OrderType::Limit
                    } else {
                        OrderType::Market
                    };

                    let price = order
                        .price_rp
                        .as_ref()
                        .and_then(|p| p.parse::<f64>().ok())
                        .unwrap_or(0.0);

                    let quantity = order
                        .order_qty_rq
                        .as_ref()
                        .and_then(|q| q.parse::<f64>().ok())
                        .unwrap_or(0.0);

                    let filled_quantity = order
                        .cum_qty_rq
                        .as_ref()
                        .and_then(|q| q.parse::<f64>().ok())
                        .unwrap_or(0.0);

                    let avg_price = order
                        .avg_price_rp
                        .as_ref()
                        .and_then(|p| p.parse::<f64>().ok());

                    let timestamp = order.create_time_ns.map(|ns| ns / 1_000_000).unwrap_or(0);

                    return Ok(OrderInfo {
                        order_id: order_id.to_string(),
                        client_order_id: order.cl_ord_id.clone(),
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
                    });
                }
            }
        }

        anyhow::bail!("Order not found: {}", order_id)
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
                let size = if let Some(size_rq) = &pos.size_rq {
                    size_rq.parse::<f64>().unwrap_or(0.0)
                } else if let Some(s) = pos.size {
                    s as f64
                } else {
                    0.0
                };

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
                self.place_order_rest(
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

        // Handle BTC/USDT format
        if symbol.contains('/') {
            return symbol.replace("/", "");
        }

        // For linear perpetuals, use BTCUSDT format
        if self.is_linear() {
            if symbol.ends_with("USDT") {
                symbol
            } else {
                format!("{}USDT", symbol)
            }
        } else {
            // For spot, use sBTCUSDT format
            if symbol.starts_with('s') && symbol.ends_with("USDT") {
                symbol
            } else if symbol.ends_with("USDT") {
                format!("s{}", symbol)
            } else {
                format!("s{}USDT", symbol)
            }
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
        // Phemex uses custom symbol format for funding rate history
        let fr_symbol = format!(".{}FR8H", formatted_symbol);

        let params = format!("symbol={}&limit={}", fr_symbol, limit.min(200));
        let response: PhemexResponse<FundingRateHistoryData> = self
            .public_get("/v2/api-data/public/data/funding-rate-history", &params)
            .await?;

        if let Some(data) = response.data {
            let entries: Vec<FundingRateEntry> = data
                .rows
                .iter()
                .map(|e| {
                    let funding_rate = if let Some(rr) = &e.funding_rate_rr {
                        rr.parse().unwrap_or(0.0)
                    } else if let Some(er) = e.funding_rate_er {
                        from_ep(er, RATIO_SCALE_ER)
                    } else {
                        0.0
                    };

                    FundingRateEntry {
                        symbol: formatted_symbol.clone(),
                        funding_rate,
                        funding_time: e.funding_time,
                    }
                })
                .collect();
            return Ok(entries);
        }

        anyhow::bail!("Failed to get funding history for {}", symbol)
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

        // Phemex doesn't have a dedicated open interest history endpoint
        // We can only get current open interest from ticker
        let ticker = self.get_ticker(symbol).await?;

        if let Some(oi) = ticker.open_interest {
            return Ok(vec![OpenInterestEntry {
                symbol: symbol.to_string(),
                open_interest: oi,
                open_interest_value: ticker.open_interest_value.unwrap_or(0.0),
                timestamp: ticker.timestamp,
            }]);
        }

        anyhow::bail!("Failed to get open interest for {}", symbol)
    }

    async fn test_ws_orderbook(
        &self,
        symbol: &str,
        _depth: u32,
        timeout_ms: u64,
    ) -> Result<WsTestResult> {
        let formatted_symbol = self.format_symbol(symbol);
        let ws_url = self.ws_url();

        let start = std::time::Instant::now();
        let (mut ws_stream, _) = connect_async(ws_url)
            .await
            .context("Failed to connect to Phemex WebSocket")?;

        let connect_time = start.elapsed().as_millis() as u64;

        // Subscribe to orderbook
        let subscribe_msg = serde_json::json!({
            "id": 1,
            "method": "orderbook.subscribe",
            "params": [formatted_symbol]
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
                                // Check if it's orderbook data (has book field)
                                if parsed.get("book").is_some() {
                                    data_sample = Some(parsed);
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
            subscription_type: format!("orderbook.{}", symbol),
            data_sample,
        })
    }

    async fn test_ws_trades(&self, symbol: &str, timeout_ms: u64) -> Result<WsTestResult> {
        let formatted_symbol = self.format_symbol(symbol);
        let ws_url = self.ws_url();

        let start = std::time::Instant::now();
        let (mut ws_stream, _) = connect_async(ws_url)
            .await
            .context("Failed to connect to Phemex WebSocket")?;

        let connect_time = start.elapsed().as_millis() as u64;

        // Subscribe to trades
        let subscribe_msg = serde_json::json!({
            "id": 1,
            "method": "trade.subscribe",
            "params": [formatted_symbol]
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
                                // Check if it's trade data (has trades field)
                                if parsed.get("trades").is_some() {
                                    data_sample = Some(parsed);
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
        let ws_url = self.ws_url();

        let start = std::time::Instant::now();
        let (mut ws_stream, _) = connect_async(ws_url)
            .await
            .context("Failed to connect to Phemex WebSocket")?;

        let connect_time = start.elapsed().as_millis() as u64;

        // Subscribe to ticker
        let subscribe_msg = serde_json::json!({
            "id": 1,
            "method": "tick.subscribe",
            "params": [formatted_symbol]
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
                                // Check if it's ticker data
                                if parsed.get("tick").is_some() || parsed.get("lastEp").is_some() {
                                    data_sample = Some(parsed);
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
            subscription_type: format!("tick.{} (includes OI, fundingRate)", symbol),
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
        let exchange_name = "phemex".to_string();

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
                        // Authenticate
                        let expiry = (timestamp_ms() / 1000) + 120;
                        let sign_str = format!("{}{}", api_key, expiry);
                        let signature = hmac_sign(&api_secret, &sign_str);

                        let auth_msg = serde_json::json!({
                            "id": timestamp_ms(),
                            "method": "user.auth",
                            "params": ["API", api_key, signature, expiry]
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
                                if resp.get("error").map(|e| e.is_null()).unwrap_or(true) {
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
                            "id": timestamp_ms(),
                            "method": "aop.subscribe",
                            "params": []
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
                                                // Check for order updates
                                                if let Some(orders) = msg_json.get("orders").and_then(|o| o.as_array()) {
                                                    for order_data in orders {
                                                        let order_id = order_data.get("orderID")
                                                            .and_then(|v| v.as_str())
                                                            .unwrap_or("")
                                                            .to_string();
                                                        let symbol = order_data.get("symbol")
                                                            .and_then(|v| v.as_str())
                                                            .unwrap_or("")
                                                            .to_string();
                                                        let status_str = order_data.get("ordStatus")
                                                            .and_then(|v| v.as_str())
                                                            .unwrap_or("New");
                                                        let side_str = order_data.get("side")
                                                            .and_then(|v| v.as_str())
                                                            .unwrap_or("Buy");
                                                        let order_type_str = order_data.get("ordType")
                                                            .and_then(|v| v.as_str())
                                                            .unwrap_or("Market");
                                                        let price = order_data.get("priceRp")
                                                            .and_then(|v| v.as_str())
                                                            .and_then(|s| s.parse::<f64>().ok())
                                                            .unwrap_or(0.0);
                                                        let qty = order_data.get("orderQtyRq")
                                                            .and_then(|v| v.as_str())
                                                            .and_then(|s| s.parse::<f64>().ok())
                                                            .unwrap_or(0.0);
                                                        let filled_qty = order_data.get("cumQtyRq")
                                                            .and_then(|v| v.as_str())
                                                            .and_then(|s| s.parse::<f64>().ok())
                                                            .unwrap_or(0.0);
                                                        let avg_price = order_data.get("avgPriceRp")
                                                            .and_then(|v| v.as_str())
                                                            .and_then(|s| s.parse::<f64>().ok());
                                                        let transact_time = order_data.get("transactTimeNs")
                                                            .and_then(|v| v.as_i64())
                                                            .map(|ns| ns / 1_000_000)
                                                            .unwrap_or(0);
                                                        let exec_fee = order_data.get("execFeeRv")
                                                            .and_then(|v| v.as_str())
                                                            .and_then(|s| s.parse::<f64>().ok());

                                                        let event = OrderUpdateEvent {
                                                            exchange: exchange_name.clone(),
                                                            order_id,
                                                            client_order_id: order_data.get("clOrdID")
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
                                                            fee: exec_fee,
                                                            fee_currency: Some("USDT".to_string()),
                                                            exchange_timestamp: transact_time,
                                                            received_at: timestamp_ms(),
                                                            raw: Some(order_data.clone()),
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
                                    let ping = serde_json::json!({"id": timestamp_ms(), "method": "server.ping", "params": []});
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
    fn test_format_symbol_linear() {
        let trader = PhemexTrader::new("", "", false, MarketType::Swap);

        assert_eq!(trader.format_symbol("BTC"), "BTCUSDT");
        assert_eq!(trader.format_symbol("BTCUSDT"), "BTCUSDT");
        assert_eq!(trader.format_symbol("BTC/USDT"), "BTCUSDT");
        assert_eq!(trader.format_symbol("eth"), "ETHUSDT");
    }

    #[test]
    fn test_format_symbol_spot() {
        let trader = PhemexTrader::new("", "", false, MarketType::Spot);

        assert_eq!(trader.format_symbol("BTC"), "sBTCUSDT");
        assert_eq!(trader.format_symbol("BTCUSDT"), "sBTCUSDT");
        assert_eq!(trader.format_symbol("sBTCUSDT"), "sBTCUSDT");
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
    }

    #[test]
    fn test_from_ep() {
        assert_eq!(from_ep(100000000, 8), 1.0);
        assert_eq!(from_ep(50000000, 8), 0.5);
        assert_eq!(from_ep(10000, 4), 1.0);
    }

    #[test]
    fn test_to_ep() {
        assert_eq!(to_ep(1.0, 8), 100000000);
        assert_eq!(to_ep(0.5, 8), 50000000);
        assert_eq!(to_ep(1.0, 4), 10000);
    }

    #[test]
    fn test_next_reconnect_delay() {
        assert_eq!(next_reconnect_delay(1), 2);
        assert_eq!(next_reconnect_delay(2), 4);
        assert_eq!(next_reconnect_delay(16), 30); // Capped at MAX
        assert_eq!(next_reconnect_delay(30), 30); // Already at max
    }
}
