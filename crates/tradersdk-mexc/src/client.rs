//! MEXC exchange trader implementation.
//!
//! MEXC API v3 implementation with HMAC-SHA256 signing.

use anyhow::{Context, Result};
use async_trait::async_trait;
use futures_util::{SinkExt, StreamExt};
use reqwest::Client;
use serde::Deserialize;
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

const SPOT_MAINNET_URL: &str = "https://api.mexc.com";
const FUTURES_MAINNET_URL: &str = "https://contract.mexc.com";
const SPOT_WS_URL: &str = "wss://wbs.mexc.com/ws";
const FUTURES_WS_URL: &str = "wss://contract.mexc.com/ws";

// Private WebSocket constants
const INITIAL_RECONNECT_DELAY_SECS: u64 = 1;
const MAX_RECONNECT_DELAY_SECS: u64 = 60;

/// Calculate next reconnect delay with exponential backoff.
fn next_reconnect_delay(current: u64) -> u64 {
    (current * 2).min(MAX_RECONNECT_DELAY_SECS)
}

/// Parse order status from string.
fn parse_order_status_str(status: &str) -> OrderStatus {
    match status.to_uppercase().as_str() {
        "NEW" | "PENDING" | "OPEN" => OrderStatus::New,
        "PARTIALLY_FILLED" | "PARTIAL" => OrderStatus::PartiallyFilled,
        "FILLED" | "CLOSED" => OrderStatus::Filled,
        "CANCELED" | "CANCELLED" => OrderStatus::Cancelled,
        "REJECTED" => OrderStatus::Rejected,
        "EXPIRED" => OrderStatus::Expired,
        _ => OrderStatus::New,
    }
}

/// Log order update event.
fn log_order_update(event: &OrderUpdateEvent) {
    info!(
        "[{}] Order update: {} {} {} @ {} | filled: {}/{} | status: {:?}",
        event.exchange,
        event.symbol,
        event.side,
        event.order_type,
        event.price,
        event.filled_quantity,
        event.quantity,
        event.status
    );
}

/// MEXC exchange trader.
pub struct MexcTrader {
    api_key: String,
    api_secret: String,
    _testnet: bool,
    market_type: MarketType,
    client: Client,
    base_url: String,
}

impl MexcTrader {
    /// Create a new MEXC trader.
    pub fn new(api_key: &str, api_secret: &str, testnet: bool, market_type: MarketType) -> Self {
        let base_url = match market_type {
            MarketType::Spot => SPOT_MAINNET_URL,
            MarketType::Swap => FUTURES_MAINNET_URL,
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

    /// Sign query parameters with HMAC-SHA256.
    fn sign(&self, query: &str) -> String {
        hmac_sign(&self.api_secret, query)
    }

    /// Make a signed GET request (for spot API).
    async fn signed_get_spot<T: for<'de> Deserialize<'de>>(
        &self,
        endpoint: &str,
        params: &str,
    ) -> Result<T> {
        let timestamp = timestamp_ms();
        let query = if params.is_empty() {
            format!("timestamp={}", timestamp)
        } else {
            format!("{}&timestamp={}", params, timestamp)
        };
        let signature = self.sign(&query);
        let url = format!(
            "{}{}?{}&signature={}",
            self.base_url, endpoint, query, signature
        );

        let response = self
            .client
            .get(&url)
            .header("X-MEXC-APIKEY", &self.api_key)
            .header("Content-Type", "application/json")
            .send()
            .await
            .context("Failed to send request to MEXC")?;

        let text = response.text().await?;
        debug!("MEXC response: {}", text);

        serde_json::from_str(&text).context(format!("Failed to parse MEXC response: {}", text))
    }

    /// Make a signed POST request (for spot API).
    async fn signed_post_spot<T: for<'de> Deserialize<'de>>(
        &self,
        endpoint: &str,
        params: &str,
    ) -> Result<T> {
        let timestamp = timestamp_ms();
        let query = if params.is_empty() {
            format!("timestamp={}", timestamp)
        } else {
            format!("{}&timestamp={}", params, timestamp)
        };
        let signature = self.sign(&query);
        let url = format!(
            "{}{}?{}&signature={}",
            self.base_url, endpoint, query, signature
        );

        let response = self
            .client
            .post(&url)
            .header("X-MEXC-APIKEY", &self.api_key)
            .header("Content-Type", "application/json")
            .send()
            .await
            .context("Failed to send request to MEXC")?;

        let text = response.text().await?;
        debug!("MEXC response: {}", text);

        serde_json::from_str(&text).context(format!("Failed to parse MEXC response: {}", text))
    }

    /// Make a signed DELETE request (for spot API).
    async fn signed_delete_spot<T: for<'de> Deserialize<'de>>(
        &self,
        endpoint: &str,
        params: &str,
    ) -> Result<T> {
        let timestamp = timestamp_ms();
        let query = if params.is_empty() {
            format!("timestamp={}", timestamp)
        } else {
            format!("{}&timestamp={}", params, timestamp)
        };
        let signature = self.sign(&query);
        let url = format!(
            "{}{}?{}&signature={}",
            self.base_url, endpoint, query, signature
        );

        let response = self
            .client
            .delete(&url)
            .header("X-MEXC-APIKEY", &self.api_key)
            .header("Content-Type", "application/json")
            .send()
            .await
            .context("Failed to send request to MEXC")?;

        let text = response.text().await?;
        debug!("MEXC response: {}", text);

        serde_json::from_str(&text).context(format!("Failed to parse MEXC response: {}", text))
    }

    /// Make a signed request for futures API.
    async fn signed_request_futures<T: for<'de> Deserialize<'de>>(
        &self,
        method: &str,
        endpoint: &str,
        params: &str,
    ) -> Result<T> {
        let timestamp = timestamp_ms();
        let request_param = format!("{}{}{}", self.api_key, timestamp, params);
        let signature = self.sign(&request_param);

        let url = if params.is_empty() {
            format!("{}{}", self.base_url, endpoint)
        } else {
            format!("{}{}?{}", self.base_url, endpoint, params)
        };

        let response = match method {
            "GET" => {
                self.client
                    .get(&url)
                    .header("ApiKey", &self.api_key)
                    .header("Request-Time", timestamp.to_string())
                    .header("Signature", signature)
                    .header("Content-Type", "application/json")
                    .send()
                    .await?
            }
            "POST" => {
                self.client
                    .post(&url)
                    .header("ApiKey", &self.api_key)
                    .header("Request-Time", timestamp.to_string())
                    .header("Signature", signature)
                    .header("Content-Type", "application/json")
                    .send()
                    .await?
            }
            "DELETE" => {
                self.client
                    .delete(&url)
                    .header("ApiKey", &self.api_key)
                    .header("Request-Time", timestamp.to_string())
                    .header("Signature", signature)
                    .header("Content-Type", "application/json")
                    .send()
                    .await?
            }
            _ => anyhow::bail!("Unsupported HTTP method: {}", method),
        };

        let text = response.text().await?;
        debug!("MEXC futures response: {}", text);

        serde_json::from_str(&text)
            .context(format!("Failed to parse MEXC futures response: {}", text))
    }
}

#[derive(Debug, Deserialize)]
struct SpotBalanceResponse {
    balances: Vec<SpotBalance>,
}

#[derive(Debug, Deserialize)]
struct SpotBalance {
    asset: String,
    free: String,
    locked: String,
}

#[derive(Debug, Deserialize)]
struct SpotOrderResponse {
    #[serde(rename = "symbol")]
    _symbol: String,
    #[serde(rename = "orderId")]
    order_id: String,
    #[serde(rename = "executedQty")]
    executed_qty: Option<String>,
    #[serde(rename = "cummulativeQuoteQty")]
    cummulative_quote_qty: Option<String>,
    price: Option<String>,
    status: Option<String>,
    side: Option<String>,
    #[serde(rename = "type")]
    order_type: Option<String>,
    #[serde(rename = "origQty")]
    orig_qty: Option<String>,
    time: Option<i64>,
}

#[derive(Debug, Deserialize)]
struct FuturesResponse<T> {
    success: bool,
    #[serde(rename = "code")]
    _code: Option<i32>,
    data: Option<T>,
    message: Option<String>,
}

#[derive(Debug, Deserialize)]
struct FuturesBalance {
    currency: String,
    #[serde(rename = "availableBalance")]
    available_balance: f64,
    #[serde(rename = "frozenBalance")]
    frozen_balance: f64,
}

#[derive(Debug, Deserialize)]
struct FuturesOrderResult {
    #[serde(rename = "orderId")]
    order_id: i64,
    #[serde(rename = "symbol")]
    _symbol: String,
    #[serde(rename = "dealAvgPrice")]
    deal_avg_price: Option<f64>,
    #[serde(rename = "dealVol")]
    deal_vol: Option<f64>,
}

#[derive(Debug, Deserialize)]
struct FuturesPosition {
    symbol: String,
    #[serde(rename = "holdVol")]
    hold_vol: f64,
    #[serde(rename = "positionType")]
    position_type: i32, // 1: long, 2: short
    #[serde(rename = "openAvgPrice")]
    _open_avg_price: f64,
}

#[derive(Debug, Deserialize)]
struct FuturesOrderInfo {
    #[serde(rename = "orderId")]
    order_id: i64,
    #[serde(rename = "symbol")]
    _symbol: String,
    side: i32,
    #[serde(rename = "orderType")]
    order_type: i32,
    price: f64,
    vol: f64,
    #[serde(rename = "dealVol")]
    deal_vol: f64,
    #[serde(rename = "dealAvgPrice")]
    deal_avg_price: Option<f64>,
    state: i32,
    #[serde(rename = "createTime")]
    create_time: i64,
}

#[async_trait]
impl ExchangeTrader for MexcTrader {
    fn exchange_name(&self) -> &str {
        "mexc"
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
        let endpoint = if self.market_type == MarketType::Spot {
            "/api/v3/ping"
        } else {
            "/api/v1/contract/ping"
        };

        let response = self
            .client
            .get(format!("{}{}", self.base_url, endpoint))
            .send()
            .await?;
        Ok(response.status().is_success())
    }

    async fn get_server_time(&self) -> Result<i64> {
        if self.market_type == MarketType::Spot {
            let response: serde_json::Value = self
                .client
                .get(format!("{}/api/v3/time", self.base_url))
                .send()
                .await?
                .json()
                .await?;
            Ok(response["serverTime"].as_i64().unwrap_or(timestamp_ms()))
        } else {
            let response: serde_json::Value = self
                .client
                .get(format!("{}/api/v1/contract/ping", self.base_url))
                .send()
                .await?
                .json()
                .await?;
            Ok(response["data"].as_i64().unwrap_or(timestamp_ms()))
        }
    }

    async fn get_balance(&self) -> Result<AccountBalance> {
        if self.market_type == MarketType::Spot {
            let response: SpotBalanceResponse = self.signed_get_spot("/api/v3/account", "").await?;

            let usdt = response
                .balances
                .iter()
                .find(|b| b.asset == "USDT")
                .map(|b| {
                    let free: f64 = b.free.parse().unwrap_or(0.0);
                    let locked: f64 = b.locked.parse().unwrap_or(0.0);
                    (free + locked, free)
                })
                .ok_or_else(|| anyhow::anyhow!("USDT balance not found in MEXC spot account"))?;

            Ok(AccountBalance {
                total_balance: usdt.0,
                available_balance: usdt.1,
                currency: "USDT".to_string(),
                margin_balance: None,
                used_margin: None,
                unrealized_pnl: None,
                realized_pnl: None,
                balances: vec![],
            })
        } else {
            let response: FuturesResponse<Vec<FuturesBalance>> = self
                .signed_request_futures("GET", "/api/v1/private/account/assets", "")
                .await?;

            let usdt = response
                .data
                .and_then(|balances| {
                    balances
                        .iter()
                        .find(|b| b.currency == "USDT")
                        .map(|b| (b.available_balance + b.frozen_balance, b.available_balance))
                })
                .ok_or_else(|| anyhow::anyhow!("USDT balance not found in MEXC futures account"))?;

            Ok(AccountBalance {
                total_balance: usdt.0,
                available_balance: usdt.1,
                currency: "USDT".to_string(),
                margin_balance: Some(usdt.0),
                used_margin: Some(usdt.0 - usdt.1),
                unrealized_pnl: None,
                realized_pnl: None,
                balances: vec![],
            })
        }
    }

    async fn get_current_price(&self, symbol: &str) -> Result<f64> {
        let formatted = self.format_symbol(symbol);

        if self.market_type == MarketType::Spot {
            let response: serde_json::Value = self
                .client
                .get(format!(
                    "{}/api/v3/ticker/price?symbol={}",
                    self.base_url, formatted
                ))
                .send()
                .await?
                .json()
                .await?;

            Ok(response["price"]
                .as_str()
                .and_then(|s| s.parse().ok())
                .unwrap_or(0.0))
        } else {
            let response: serde_json::Value = self
                .client
                .get(format!(
                    "{}/api/v1/contract/ticker?symbol={}",
                    self.base_url, formatted
                ))
                .send()
                .await?
                .json()
                .await?;

            Ok(response["data"]["lastPrice"].as_f64().unwrap_or(0.0))
        }
    }

    async fn set_leverage(&self, symbol: Option<&str>, leverage: u32) -> Result<bool> {
        if self.market_type == MarketType::Spot {
            return Ok(true);
        }

        let contract = symbol.ok_or_else(|| anyhow::anyhow!("Symbol required for leverage"))?;
        let formatted = self.format_symbol(contract);

        // MEXC futures leverage is set per position side
        let params = format!(
            "symbol={}&leverage={}&openType=1&positionType=1",
            formatted, leverage
        );

        let _: FuturesResponse<serde_json::Value> = self
            .signed_request_futures("POST", "/api/v1/private/position/change_leverage", &params)
            .await?;

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
        let formatted = self.format_symbol(symbol);
        let price = current_price.unwrap_or(self.get_current_price(&formatted).await?);

        if self.market_type == MarketType::Swap {
            if let Err(e) = self.set_leverage(Some(&formatted), leverage).await {
                error!("Failed to set leverage on MEXC: {}", e);
            }
        }

        let quantity = if self.market_type == MarketType::Swap {
            usdt_amount / price * leverage as f64
        } else {
            usdt_amount / price
        };

        let side = if direction.to_lowercase() == "long" {
            OrderSide::Buy
        } else {
            OrderSide::Sell
        };

        let result = self.place_market_order(&formatted, side, quantity).await?;

        // Handle TP/SL if provided and order was successful
        if result.success && self.market_type == MarketType::Swap {
            let is_long = direction.to_lowercase() == "long";

            if let Some(tp) = tp_percent {
                let tp_price = if is_long {
                    price * (1.0 + tp / 100.0)
                } else {
                    price * (1.0 - tp / 100.0)
                };
                info!("MEXC TP price calculated: {:.8}", tp_price);
                // TODO: Implement place_plan_order for MEXC
            }

            if let Some(sl) = sl_percent {
                let sl_price = if is_long {
                    price * (1.0 - sl / 100.0)
                } else {
                    price * (1.0 + sl / 100.0)
                };
                info!("MEXC SL price calculated: {:.8}", sl_price);
                // TODO: Implement place_plan_order for MEXC
            }
        }

        Ok(result)
    }

    async fn place_market_order(
        &self,
        symbol: &str,
        side: OrderSide,
        quantity: f64,
    ) -> Result<OrderResult> {
        let formatted = self.format_symbol(symbol);

        if self.market_type == MarketType::Spot {
            let side_str = match side {
                OrderSide::Buy => "BUY",
                OrderSide::Sell => "SELL",
            };

            let params = format!(
                "symbol={}&side={}&type=MARKET&quantity={:.8}",
                formatted, side_str, quantity
            );

            let response: SpotOrderResponse =
                self.signed_post_spot("/api/v3/order", &params).await?;

            info!("MEXC spot order placed: {}", response.order_id);

            let executed_qty: f64 = response
                .executed_qty
                .as_ref()
                .and_then(|s| s.parse().ok())
                .unwrap_or(0.0);

            let quote_qty: f64 = response
                .cummulative_quote_qty
                .as_ref()
                .and_then(|s| s.parse().ok())
                .unwrap_or(0.0);

            let entry_price = if executed_qty > 0.0 {
                Some(quote_qty / executed_qty)
            } else if response
                .status
                .as_ref()
                .map(|s| !s.is_empty())
                .unwrap_or(false)
            {
                // If not fully filled yet, use None
                None
            } else {
                None
            };

            Ok(OrderResult {
                success: true,
                order_id: Some(response.order_id),
                symbol: formatted,
                side,
                order_type: OrderType::Market,
                quantity: Some(executed_qty),
                entry_price,
                position_size: Some(executed_qty),
                position_usdt: Some(quote_qty),
                error: None,
                raw_response: None,
            })
        } else {
            // Futures market order
            let (side_num, open_type) = match side {
                OrderSide::Buy => (1, 1),  // 1: open long
                OrderSide::Sell => (2, 1), // 2: open short (as sell = going short)
            };

            let params = format!(
                "symbol={}&side={}&openType={}&type=5&vol={}",
                formatted, side_num, open_type, quantity
            );

            let response: FuturesResponse<FuturesOrderResult> = self
                .signed_request_futures("POST", "/api/v1/private/order/submit", &params)
                .await?;

            if !response.success {
                return Ok(OrderResult::failure(
                    formatted,
                    side,
                    response
                        .message
                        .unwrap_or_else(|| "Unknown error".to_string()),
                ));
            }

            let order_data = response
                .data
                .ok_or_else(|| anyhow::anyhow!("No order data"))?;

            info!("MEXC futures order placed: {}", order_data.order_id);

            Ok(OrderResult {
                success: true,
                order_id: Some(order_data.order_id.to_string()),
                symbol: formatted,
                side,
                order_type: OrderType::Market,
                quantity: Some(quantity),
                entry_price: order_data.deal_avg_price,
                position_size: order_data.deal_vol,
                position_usdt: order_data.deal_avg_price.map(|p| quantity * p),
                error: None,
                raw_response: None,
            })
        }
    }

    async fn place_limit_order(
        &self,
        symbol: &str,
        side: OrderSide,
        quantity: f64,
        price: f64,
        reduce_only: bool,
    ) -> Result<OrderResult> {
        let formatted = self.format_symbol(symbol);

        if self.market_type == MarketType::Spot {
            let side_str = match side {
                OrderSide::Buy => "BUY",
                OrderSide::Sell => "SELL",
            };

            let params = format!(
                "symbol={}&side={}&type=LIMIT&quantity={:.8}&price={:.8}&timeInForce=GTC",
                formatted, side_str, quantity, price
            );

            let response: SpotOrderResponse =
                self.signed_post_spot("/api/v3/order", &params).await?;

            Ok(OrderResult {
                success: true,
                order_id: Some(response.order_id),
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
        } else {
            let (side_num, open_type) = if reduce_only {
                match side {
                    OrderSide::Buy => (4, 2),  // close short
                    OrderSide::Sell => (3, 2), // close long
                }
            } else {
                match side {
                    OrderSide::Buy => (1, 1),  // open long
                    OrderSide::Sell => (2, 1), // open short
                }
            };

            let params = format!(
                "symbol={}&side={}&openType={}&type=1&vol={}&price={}",
                formatted, side_num, open_type, quantity as i64, price
            );

            let response: FuturesResponse<FuturesOrderResult> = self
                .signed_request_futures("POST", "/api/v1/private/order/submit", &params)
                .await?;

            if !response.success {
                return Ok(OrderResult::failure(
                    formatted,
                    side,
                    response
                        .message
                        .unwrap_or_else(|| "Unknown error".to_string()),
                ));
            }

            let order_data = response
                .data
                .ok_or_else(|| anyhow::anyhow!("No order data"))?;

            Ok(OrderResult {
                success: true,
                order_id: Some(order_data.order_id.to_string()),
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
    }

    async fn cancel_order(&self, symbol: &str, order_id: &str) -> Result<bool> {
        let formatted = self.format_symbol(symbol);

        if self.market_type == MarketType::Spot {
            let params = format!("symbol={}&orderId={}", formatted, order_id);
            let _: serde_json::Value = self.signed_delete_spot("/api/v3/order", &params).await?;
        } else {
            let params = format!("symbol={}&orderId={}", formatted, order_id);
            let _: FuturesResponse<serde_json::Value> = self
                .signed_request_futures("POST", "/api/v1/private/order/cancel", &params)
                .await?;
        }

        Ok(true)
    }

    async fn get_order_status(&self, symbol: &str, order_id: &str) -> Result<OrderInfo> {
        let formatted = self.format_symbol(symbol);

        if self.market_type == MarketType::Spot {
            let params = format!("symbol={}&orderId={}", formatted, order_id);
            let response: SpotOrderResponse =
                self.signed_get_spot("/api/v3/order", &params).await?;

            let status = parse_order_status_str(response.status.as_deref().unwrap_or("NEW"));
            let side = if response.side.as_deref() == Some("BUY") {
                OrderSide::Buy
            } else {
                OrderSide::Sell
            };
            let order_type = if response.order_type.as_deref() == Some("MARKET") {
                OrderType::Market
            } else {
                OrderType::Limit
            };

            let orig_qty: f64 = response
                .orig_qty
                .as_ref()
                .and_then(|s| s.parse().ok())
                .unwrap_or(0.0);
            let filled_qty: f64 = response
                .executed_qty
                .as_ref()
                .and_then(|s| s.parse().ok())
                .unwrap_or(0.0);
            let price: f64 = response
                .price
                .as_ref()
                .and_then(|s| s.parse().ok())
                .unwrap_or(0.0);

            Ok(OrderInfo {
                order_id: response.order_id,
                client_order_id: None,
                symbol: formatted,
                status,
                side,
                order_type,
                price,
                quantity: orig_qty,
                filled_quantity: filled_qty,
                remaining_quantity: orig_qty - filled_qty,
                avg_price: if filled_qty > 0.0 {
                    response
                        .cummulative_quote_qty
                        .as_ref()
                        .and_then(|s| s.parse::<f64>().ok())
                        .map(|q| q / filled_qty)
                } else {
                    None
                },
                timestamp: response.time.unwrap_or(0),
                time_in_force: TimeInForce::GTC,
                reduce_only: false,
            })
        } else {
            let params = format!("symbol={}&orderId={}", formatted, order_id);
            let response: FuturesResponse<FuturesOrderInfo> = self
                .signed_request_futures("GET", "/api/v1/private/order/get", &params)
                .await?;

            let order_data = response
                .data
                .ok_or_else(|| anyhow::anyhow!("No order data"))?;

            let status = match order_data.state {
                1 => OrderStatus::New,
                2 => OrderStatus::PartiallyFilled,
                3 => OrderStatus::Filled,
                4 => OrderStatus::Cancelled,
                _ => OrderStatus::New,
            };

            let side = if order_data.side == 1 || order_data.side == 4 {
                OrderSide::Buy
            } else {
                OrderSide::Sell
            };

            let order_type = if order_data.order_type == 5 {
                OrderType::Market
            } else {
                OrderType::Limit
            };

            Ok(OrderInfo {
                order_id: order_data.order_id.to_string(),
                client_order_id: None,
                symbol: formatted,
                status,
                side,
                order_type,
                price: order_data.price,
                quantity: order_data.vol,
                filled_quantity: order_data.deal_vol,
                remaining_quantity: order_data.vol - order_data.deal_vol,
                avg_price: order_data.deal_avg_price,
                timestamp: order_data.create_time,
                time_in_force: TimeInForce::GTC,
                reduce_only: order_data.side == 3 || order_data.side == 4,
            })
        }
    }

    async fn close_position(&self, symbol: &str) -> Result<OrderResult> {
        if self.market_type == MarketType::Spot {
            anyhow::bail!("Close position not supported for spot trading");
        }

        let formatted = self.format_symbol(symbol);

        // Get current positions
        let response: FuturesResponse<Vec<FuturesPosition>> = self
            .signed_request_futures(
                "GET",
                "/api/v1/private/position/open_positions",
                &format!("symbol={}", formatted),
            )
            .await?;

        let positions = response.data.unwrap_or_default();
        let position = positions
            .iter()
            .find(|p| p.symbol == formatted && p.hold_vol > 0.0);

        if let Some(pos) = position {
            // Close position with market order
            let (side_num, close_side) = if pos.position_type == 1 {
                // Long position - close by selling
                (3, OrderSide::Sell) // 3: close long
            } else {
                // Short position - close by buying
                (4, OrderSide::Buy) // 4: close short
            };

            let params = format!(
                "symbol={}&side={}&openType=2&type=5&vol={}",
                formatted, side_num, pos.hold_vol as i64
            );

            let order_response: FuturesResponse<FuturesOrderResult> = self
                .signed_request_futures("POST", "/api/v1/private/order/submit", &params)
                .await?;

            if !order_response.success {
                return Ok(OrderResult::failure(
                    formatted,
                    close_side,
                    order_response
                        .message
                        .unwrap_or_else(|| "Unknown error".to_string()),
                ));
            }

            let order_data = order_response
                .data
                .ok_or_else(|| anyhow::anyhow!("No order data"))?;

            return Ok(OrderResult {
                success: true,
                order_id: Some(order_data.order_id.to_string()),
                symbol: formatted,
                side: close_side,
                order_type: OrderType::Market,
                quantity: Some(pos.hold_vol),
                entry_price: order_data.deal_avg_price,
                position_size: Some(0.0),
                position_usdt: None,
                error: None,
                raw_response: None,
            });
        }

        Ok(OrderResult::failure(
            formatted,
            OrderSide::Sell,
            "No position to close".to_string(),
        ))
    }

    fn format_symbol(&self, base: &str) -> String {
        let s = base.to_uppercase();

        // Handle BTC/USDT format
        if s.contains('/') {
            return s.replace("/", "");
        }

        if self.market_type == MarketType::Spot {
            // Spot: BTCUSDT format
            if s.ends_with("USDT") {
                s
            } else {
                format!("{}USDT", s)
            }
        } else {
            // Futures: BTC_USDT format
            if s.contains('_') {
                s
            } else if s.ends_with("USDT") {
                let b = s.trim_end_matches("USDT");
                format!("{}_USDT", b)
            } else {
                format!("{}_USDT", s)
            }
        }
    }

    async fn get_orderbook(&self, symbol: &str, depth: u32) -> Result<Orderbook> {
        let formatted = self.format_symbol(symbol);
        let limit = depth.min(100);

        let response: serde_json::Value = if self.market_type == MarketType::Spot {
            self.client
                .get(format!(
                    "{}/api/v3/depth?symbol={}&limit={}",
                    self.base_url, formatted, limit
                ))
                .send()
                .await?
                .json()
                .await?
        } else {
            self.client
                .get(format!(
                    "{}/api/v1/contract/depth/{}?limit={}",
                    self.base_url, formatted, limit
                ))
                .send()
                .await?
                .json()
                .await?
        };

        let data = if self.market_type == MarketType::Spot {
            &response
        } else {
            &response["data"]
        };

        let bids: Vec<OrderbookLevel> = data["bids"]
            .as_array()
            .map(|arr| {
                arr.iter()
                    .filter_map(|level| {
                        let price: f64 = level[0].as_str()?.parse().ok()?;
                        let qty: f64 = level[1].as_str()?.parse().ok()?;
                        Some(OrderbookLevel {
                            price,
                            quantity: qty,
                        })
                    })
                    .collect()
            })
            .unwrap_or_default();

        let asks: Vec<OrderbookLevel> = data["asks"]
            .as_array()
            .map(|arr| {
                arr.iter()
                    .filter_map(|level| {
                        let price: f64 = level[0].as_str()?.parse().ok()?;
                        let qty: f64 = level[1].as_str()?.parse().ok()?;
                        Some(OrderbookLevel {
                            price,
                            quantity: qty,
                        })
                    })
                    .collect()
            })
            .unwrap_or_default();

        Ok(Orderbook {
            symbol: formatted,
            bids,
            asks,
            timestamp: timestamp_ms(),
        })
    }

    async fn test_ws_orderbook(
        &self,
        symbol: &str,
        _depth: u32,
        timeout_ms: u64,
    ) -> Result<WsTestResult> {
        let formatted = self.format_symbol(symbol);
        let ws_url = if self.market_type == MarketType::Spot {
            SPOT_WS_URL
        } else {
            FUTURES_WS_URL
        };

        let start = std::time::Instant::now();
        let (mut ws_stream, _) = connect_async(ws_url)
            .await
            .context("Failed to connect to MEXC WebSocket")?;

        let connect_time = start.elapsed().as_millis() as u64;

        // Subscribe to orderbook
        let subscribe_msg = if self.market_type == MarketType::Spot {
            serde_json::json!({
                "method": "SUBSCRIPTION",
                "params": [format!("spot@public.limit.depth.v3.api@{}@20", formatted)]
            })
        } else {
            serde_json::json!({
                "method": "sub.depth",
                "param": {
                    "symbol": formatted
                }
            })
        };

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
                                if parsed.get("data").is_some() || parsed.get("bids").is_some() {
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
        let ws_url = if self.market_type == MarketType::Spot {
            SPOT_WS_URL
        } else {
            FUTURES_WS_URL
        };

        let start = std::time::Instant::now();
        let (mut ws_stream, _) = connect_async(ws_url)
            .await
            .context("Failed to connect to MEXC WebSocket")?;

        let connect_time = start.elapsed().as_millis() as u64;

        let subscribe_msg = if self.market_type == MarketType::Spot {
            serde_json::json!({
                "method": "SUBSCRIPTION",
                "params": [format!("spot@public.deals.v3.api@{}", formatted)]
            })
        } else {
            serde_json::json!({
                "method": "sub.deal",
                "param": {
                    "symbol": formatted
                }
            })
        };

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
                                if parsed.get("data").is_some() {
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

    async fn get_ticker(&self, symbol: &str) -> Result<TickerInfo> {
        let formatted = self.format_symbol(symbol);

        if self.market_type == MarketType::Spot {
            let response: serde_json::Value = self
                .client
                .get(format!(
                    "{}/api/v3/ticker/24hr?symbol={}",
                    self.base_url, formatted
                ))
                .send()
                .await?
                .json()
                .await?;

            return Ok(TickerInfo {
                symbol: formatted,
                last_price: response["lastPrice"]
                    .as_str()
                    .and_then(|s| s.parse().ok())
                    .unwrap_or(0.0),
                bid_price: response["bidPrice"].as_str().and_then(|s| s.parse().ok()),
                ask_price: response["askPrice"].as_str().and_then(|s| s.parse().ok()),
                high_24h: response["highPrice"].as_str().and_then(|s| s.parse().ok()),
                low_24h: response["lowPrice"].as_str().and_then(|s| s.parse().ok()),
                volume_24h: response["volume"].as_str().and_then(|s| s.parse().ok()),
                turnover_24h: response["quoteVolume"]
                    .as_str()
                    .and_then(|s| s.parse().ok()),
                change_24h: response["priceChangePercent"]
                    .as_str()
                    .and_then(|s| s.parse().ok()),
                open_interest: None,
                open_interest_value: None,
                funding_rate: None,
                next_funding_time: None,
                mark_price: None,
                index_price: None,
                timestamp: timestamp_ms(),
            });
        }

        // Futures
        let response: serde_json::Value = self
            .client
            .get(format!(
                "{}/api/v1/contract/ticker?symbol={}",
                self.base_url, formatted
            ))
            .send()
            .await?
            .json()
            .await?;

        let data = &response["data"];

        Ok(TickerInfo {
            symbol: formatted,
            last_price: data["lastPrice"].as_f64().unwrap_or(0.0),
            bid_price: data["bid1"].as_f64(),
            ask_price: data["ask1"].as_f64(),
            high_24h: data["high24Price"].as_f64(),
            low_24h: data["low24Price"].as_f64(),
            volume_24h: data["volume24"].as_f64(),
            turnover_24h: data["amount24"].as_f64(),
            change_24h: data["riseFallRate"].as_f64(),
            open_interest: data["holdVol"].as_f64(),
            open_interest_value: None,
            funding_rate: data["fundingRate"].as_f64(),
            next_funding_time: data["nextSettleTime"].as_i64(),
            mark_price: data["fairPrice"].as_f64(),
            index_price: data["indexPrice"].as_f64(),
            timestamp: timestamp_ms(),
        })
    }

    async fn test_ws_ticker(&self, symbol: &str, timeout_ms: u64) -> Result<WsTestResult> {
        let formatted = self.format_symbol(symbol);
        let ws_url = if self.market_type == MarketType::Spot {
            SPOT_WS_URL
        } else {
            FUTURES_WS_URL
        };

        let start = std::time::Instant::now();
        let (mut ws_stream, _) = connect_async(ws_url)
            .await
            .context("Failed to connect to MEXC WebSocket")?;

        let connect_time = start.elapsed().as_millis() as u64;

        let subscribe_msg = if self.market_type == MarketType::Spot {
            serde_json::json!({
                "method": "SUBSCRIPTION",
                "params": [format!("spot@public.miniTicker.v3.api@{}@UTC+0", formatted)]
            })
        } else {
            serde_json::json!({
                "method": "sub.ticker",
                "param": {
                    "symbol": formatted
                }
            })
        };

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
                                data_sample = Some(parsed);
                                break;
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
        symbol: &str,
        limit: u32,
    ) -> Result<Vec<FundingRateEntry>> {
        if self.market_type != MarketType::Swap {
            anyhow::bail!("Funding rate only available for futures");
        }

        let formatted = self.format_symbol(symbol);

        let response: serde_json::Value = self
            .client
            .get(format!(
                "{}/api/v1/contract/funding_rate/history?symbol={}&page_num=1&page_size={}",
                self.base_url,
                formatted,
                limit.min(100)
            ))
            .send()
            .await?
            .json()
            .await?;

        let entries: Vec<FundingRateEntry> = response["data"]["resultList"]
            .as_array()
            .map(|arr| {
                arr.iter()
                    .filter_map(|e| {
                        Some(FundingRateEntry {
                            symbol: formatted.clone(),
                            funding_rate: e["fundingRate"].as_f64()?,
                            funding_time: e["settleTime"].as_i64()?,
                        })
                    })
                    .collect()
            })
            .unwrap_or_default();

        Ok(entries)
    }

    async fn get_open_interest_history(
        &self,
        symbol: &str,
        _interval: &str,
        _limit: u32,
    ) -> Result<Vec<OpenInterestEntry>> {
        if self.market_type != MarketType::Swap {
            anyhow::bail!("Open interest only available for futures");
        }

        let formatted = self.format_symbol(symbol);

        // MEXC doesn't have historical OI endpoint, return current OI
        let response: serde_json::Value = self
            .client
            .get(format!(
                "{}/api/v1/contract/ticker?symbol={}",
                self.base_url, formatted
            ))
            .send()
            .await?
            .json()
            .await?;

        let data = &response["data"];
        let oi: f64 = data["holdVol"].as_f64().unwrap_or(0.0);
        let price: f64 = data["lastPrice"].as_f64().unwrap_or(0.0);

        Ok(vec![OpenInterestEntry {
            symbol: formatted,
            open_interest: oi,
            open_interest_value: oi * price,
            timestamp: timestamp_ms(),
        }])
    }

    async fn subscribe_order_updates(
        &self,
        sender: broadcast::Sender<OrderUpdateEvent>,
    ) -> Result<JoinHandle<()>> {
        let api_key = self.api_key.clone();
        let market_type = self.market_type;
        let exchange_name = "mexc".to_string();

        let handle = tokio::spawn(async move {
            let client = Client::new();
            let mut reconnect_delay = INITIAL_RECONNECT_DELAY_SECS;

            loop {
                // Create listenKey
                let listen_key_url = if market_type == MarketType::Spot {
                    format!("{}/api/v3/userDataStream", SPOT_MAINNET_URL)
                } else {
                    format!("{}/api/v1/private/user/listen-key", FUTURES_MAINNET_URL)
                };

                let listen_key_resp = client
                    .post(&listen_key_url)
                    .header("X-MEXC-APIKEY", &api_key)
                    .send()
                    .await;

                let listen_key = match listen_key_resp {
                    Ok(resp) => {
                        let text = resp.text().await.unwrap_or_default();
                        match serde_json::from_str::<serde_json::Value>(&text) {
                            Ok(json) => json
                                .get("listenKey")
                                .and_then(|v| v.as_str())
                                .map(|s| s.to_string())
                                .unwrap_or_default(),
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
                    error!("[{}] Empty listenKey", exchange_name);
                    tokio::time::sleep(tokio::time::Duration::from_secs(reconnect_delay)).await;
                    reconnect_delay = next_reconnect_delay(reconnect_delay);
                    continue;
                }

                let ws_url = if market_type == MarketType::Spot {
                    format!("{}?listenKey={}", SPOT_WS_URL, listen_key)
                } else {
                    format!("{}?listenKey={}", FUTURES_WS_URL, listen_key)
                };

                info!("[{}] Connecting to user data stream", exchange_name);

                match connect_async(&ws_url).await {
                    Ok((mut ws_stream, _)) => {
                        info!("[{}] Connected to user data stream", exchange_name);
                        reconnect_delay = INITIAL_RECONNECT_DELAY_SECS;

                        let mut keepalive_interval =
                            tokio::time::interval(tokio::time::Duration::from_secs(30 * 60));
                        keepalive_interval.tick().await;

                        loop {
                            tokio::select! {
                                msg = ws_stream.next() => {
                                    match msg {
                                        Some(Ok(Message::Text(text))) => {
                                            if let Ok(ev) = serde_json::from_str::<serde_json::Value>(&text) {
                                                let event_type = ev.get("e").and_then(|v| v.as_str()).unwrap_or("");

                                                if event_type == "executionReport" {
                                                    let event = OrderUpdateEvent {
                                                        exchange: exchange_name.clone(),
                                                        order_id: ev.get("i").and_then(|v| v.as_str())
                                                            .or_else(|| ev.get("i").and_then(|v| v.as_i64()).map(|_| ""))
                                                            .unwrap_or("").to_string(),
                                                        client_order_id: ev.get("c").and_then(|v| v.as_str()).map(|s| s.to_string()),
                                                        symbol: ev.get("s").and_then(|v| v.as_str()).unwrap_or("").to_string(),
                                                        status: parse_order_status_str(
                                                            ev.get("X").and_then(|v| v.as_str()).unwrap_or("NEW")
                                                        ),
                                                        side: if ev.get("S").and_then(|v| v.as_str()) == Some("BUY") { OrderSide::Buy } else { OrderSide::Sell },
                                                        order_type: if ev.get("o").and_then(|v| v.as_str()) == Some("MARKET") { OrderType::Market } else { OrderType::Limit },
                                                        price: ev.get("p").and_then(|v| v.as_str()).and_then(|s| s.parse().ok()).unwrap_or(0.0),
                                                        quantity: ev.get("q").and_then(|v| v.as_str()).and_then(|s| s.parse().ok()).unwrap_or(0.0),
                                                        filled_quantity: ev.get("z").and_then(|v| v.as_str()).and_then(|s| s.parse().ok()).unwrap_or(0.0),
                                                        avg_fill_price: ev.get("L").and_then(|v| v.as_str()).and_then(|s| s.parse().ok()),
                                                        fee: ev.get("n").and_then(|v| v.as_str()).and_then(|s| s.parse().ok()),
                                                        fee_currency: ev.get("N").and_then(|v| v.as_str()).map(|s| s.to_string()),
                                                        exchange_timestamp: ev.get("T").and_then(|v| v.as_i64()).unwrap_or(0),
                                                        received_at: timestamp_ms(),
                                                        raw: Some(ev.clone()),
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
                                            warn!("[{}] User data stream disconnected", exchange_name);
                                            break;
                                        }
                                        _ => {}
                                    }
                                }
                                _ = keepalive_interval.tick() => {
                                    let put_resp = client
                                        .put(&listen_key_url)
                                        .header("X-MEXC-APIKEY", &api_key)
                                        .send()
                                        .await;
                                    match put_resp {
                                        Ok(resp) if resp.status().is_success() => {
                                            debug!("[{}] listenKey renewed", exchange_name);
                                        }
                                        _ => {
                                            warn!("[{}] listenKey renewal failed", exchange_name);
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
