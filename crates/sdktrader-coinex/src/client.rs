//! CoinEx exchange trader implementation.
//!
//! CoinEx API v2 documentation: https://docs.coinex.com/api/v2
//!
//! Authentication:
//! - V2 API uses SHA256 signature
//! - Signature = SHA256(METHOD + /v2/ + path + body(for POST) + timestamp + secret)
//! - Headers: X-COINEX-KEY, X-COINEX-SIGN, X-COINEX-TIMESTAMP

use sdktrader_core::utils::timestamp_ms;
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
use sha2::{Sha256, Digest};
use tokio::sync::broadcast;
use tokio::task::JoinHandle;
use tokio_tungstenite::{connect_async, tungstenite::protocol::Message};
use tracing::{debug, error, info, warn};

// ===============================================================================
// CONSTANTS
// ===============================================================================

const SPOT_API_URL: &str = "https://api.coinex.com";
const FUTURES_API_URL: &str = "https://api.coinex.com";

// WebSocket URLs
const SPOT_WS_URL: &str = "wss://socket.coinex.com/v2/spot";
const FUTURES_WS_URL: &str = "wss://socket.coinex.com/v2/futures";

// Private WebSocket reconnect constants
const INITIAL_RECONNECT_DELAY_SECS: u64 = 1;
const MAX_RECONNECT_DELAY_SECS: u64 = 30;

// ===============================================================================
// HELPER FUNCTIONS
// ===============================================================================

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

/// Parse a status string from CoinEx into OrderStatus.
fn parse_order_status_str(s: &str) -> OrderStatus {
    match s.to_lowercase().as_str() {
        "open" | "not_deal" | "part_deal" => OrderStatus::New,
        "done" | "filled" => OrderStatus::Filled,
        "cancel" | "canceled" | "cancelled" => OrderStatus::Cancelled,
        "rejected" => OrderStatus::Rejected,
        _ => {
            warn!("Unknown order status string: '{}'", s);
            OrderStatus::New
        }
    }
}

/// Create SHA256 signature for CoinEx API v2.
fn create_signature(method: &str, path: &str, body: &str, timestamp: &str, secret: &str) -> String {
    // Format: METHOD + path + body + timestamp + secret
    let prepared = format!("{}{}{}{}{}", method, path, body, timestamp, secret);
    let mut hasher = Sha256::new();
    hasher.update(prepared.as_bytes());
    let result = hasher.finalize();
    hex::encode(result)
}

// ===============================================================================
// COINEX TRADER
// ===============================================================================

/// CoinEx exchange trader.
pub struct CoinexTrader {
    api_key: String,
    api_secret: String,
    #[allow(dead_code)]
    testnet: bool,
    market_type: MarketType,
    client: Client,
    base_url: String,
}

impl CoinexTrader {
    /// Create a new CoinEx trader.
    pub fn new(api_key: &str, api_secret: &str, testnet: bool, market_type: MarketType) -> Self {
        // CoinEx does not have a public testnet, so we use mainnet URLs
        let base_url = match market_type {
            MarketType::Spot => SPOT_API_URL,
            MarketType::Swap => FUTURES_API_URL,
        };

        if testnet {
            warn!("CoinEx does not have a public testnet. Using mainnet URLs.");
        }

        Self {
            api_key: api_key.to_string(),
            api_secret: api_secret.to_string(),
            testnet,
            market_type,
            client: Client::new(),
            base_url: base_url.to_string(),
        }
    }

    /// Sign a request for CoinEx API v2.
    fn sign(&self, method: &str, path: &str, body: &str, timestamp: &str) -> String {
        create_signature(method, path, body, timestamp, &self.api_secret)
    }

    /// Make a public GET request.
    async fn public_get<T: for<'de> Deserialize<'de>>(
        &self,
        endpoint: &str,
        params: &str,
    ) -> Result<T> {
        let url = if params.is_empty() {
            format!("{}{}", self.base_url, endpoint)
        } else {
            format!("{}{}?{}", self.base_url, endpoint, params)
        };

        let response = self
            .client
            .get(&url)
            .send()
            .await
            .context("Failed to send request to CoinEx")?;

        let text = response.text().await?;
        debug!("CoinEx response: {}", text);

        let parsed: CoinexResponse<T> = serde_json::from_str(&text)
            .context("Failed to parse CoinEx response")?;

        if parsed.code != 0 {
            anyhow::bail!("CoinEx API error: {} - {}", parsed.code, parsed.message);
        }

        parsed.data.ok_or_else(|| anyhow::anyhow!("No data in response"))
    }

    /// Make a signed GET request.
    async fn signed_get<T: for<'de> Deserialize<'de>>(
        &self,
        endpoint: &str,
        params: &str,
    ) -> Result<T> {
        let timestamp = timestamp_ms().to_string();
        let path = if params.is_empty() {
            endpoint.to_string()
        } else {
            format!("{}?{}", endpoint, params)
        };
        let signature = self.sign("GET", &path, "", &timestamp);

        let url = format!("{}{}", self.base_url, path);

        let response = self
            .client
            .get(&url)
            .header("Content-Type", "application/json")
            .header("Accept", "application/json")
            .header("X-COINEX-KEY", &self.api_key)
            .header("X-COINEX-SIGN", &signature)
            .header("X-COINEX-TIMESTAMP", &timestamp)
            .send()
            .await
            .context("Failed to send request to CoinEx")?;

        let text = response.text().await?;
        debug!("CoinEx response: {}", text);

        let parsed: CoinexResponse<T> = serde_json::from_str(&text)
            .context("Failed to parse CoinEx response")?;

        if parsed.code != 0 {
            anyhow::bail!("CoinEx API error: {} - {}", parsed.code, parsed.message);
        }

        parsed.data.ok_or_else(|| anyhow::anyhow!("No data in response"))
    }

    /// Make a signed POST request.
    async fn signed_post<T: for<'de> Deserialize<'de>>(
        &self,
        endpoint: &str,
        body: &serde_json::Value,
    ) -> Result<T> {
        let timestamp = timestamp_ms().to_string();
        let body_str = serde_json::to_string(body)?;
        let signature = self.sign("POST", endpoint, &body_str, &timestamp);

        let url = format!("{}{}", self.base_url, endpoint);

        let response = self
            .client
            .post(&url)
            .header("Content-Type", "application/json")
            .header("Accept", "application/json")
            .header("X-COINEX-KEY", &self.api_key)
            .header("X-COINEX-SIGN", &signature)
            .header("X-COINEX-TIMESTAMP", &timestamp)
            .body(body_str)
            .send()
            .await
            .context("Failed to send request to CoinEx")?;

        let text = response.text().await?;
        debug!("CoinEx response: {}", text);

        let parsed: CoinexResponse<T> = serde_json::from_str(&text)
            .context("Failed to parse CoinEx response")?;

        if parsed.code != 0 {
            anyhow::bail!("CoinEx API error: {} - {}", parsed.code, parsed.message);
        }

        parsed.data.ok_or_else(|| anyhow::anyhow!("No data in response"))
    }

    /// Get WebSocket URL based on market type.
    fn ws_url(&self) -> &str {
        match self.market_type {
            MarketType::Spot => SPOT_WS_URL,
            MarketType::Swap => FUTURES_WS_URL,
        }
    }

    /// Format symbol for CoinEx (e.g., BTCUSDT).
    fn format_coinex_symbol(&self, symbol: &str) -> String {
        let symbol = symbol.to_uppercase();
        // Handle BTC/USDT format
        if symbol.contains('/') {
            return symbol.replace("/", "");
        }
        if symbol.ends_with("USDT") || symbol.ends_with("USD") {
            return symbol;
        }
        format!("{}USDT", symbol)
    }
}

// ===============================================================================
// RESPONSE STRUCTURES
// ===============================================================================

/// Generic CoinEx API response wrapper.
#[derive(Debug, Deserialize)]
struct CoinexResponse<T> {
    code: i32,
    message: String,
    data: Option<T>,
}

/// Server time response.
#[derive(Debug, Deserialize)]
struct ServerTimeResponse {
    timestamp: i64,
}

/// Ticker response for spot.
#[allow(dead_code)]
#[derive(Debug, Deserialize)]
struct SpotTickerResponse {
    market: String,
    last: String,
    open: String,
    high: String,
    low: String,
    volume: String,
    value: String,
    #[serde(default)]
    sell: String,
    #[serde(default)]
    buy: String,
}

/// Ticker response for futures.
#[allow(dead_code)]
#[derive(Debug, Deserialize)]
struct FuturesTickerResponse {
    market: String,
    last: String,
    open: String,
    high: String,
    low: String,
    volume: String,
    value: String,
    #[serde(default)]
    sell: String,
    #[serde(default)]
    buy: String,
    #[serde(default)]
    open_interest: Option<String>,
    #[serde(default)]
    open_interest_value: Option<String>,
    #[serde(default)]
    index_price: Option<String>,
    #[serde(default)]
    mark_price: Option<String>,
    #[serde(default)]
    funding_rate: Option<String>,
    #[serde(default)]
    next_funding_time: Option<i64>,
}

/// Balance entry for spot.
#[derive(Debug, Deserialize)]
struct SpotBalanceEntry {
    ccy: String,
    available: String,
    frozen: String,
}

/// Balance entry for futures.
#[allow(dead_code)]
#[derive(Debug, Deserialize)]
struct FuturesBalanceEntry {
    ccy: String,
    available: String,
    frozen: String,
    #[serde(default)]
    margin: Option<String>,
    #[serde(default)]
    transferrable: Option<String>,
    #[serde(default)]
    unrealized_pnl: Option<String>,
}

/// Order response.
#[allow(dead_code)]
#[derive(Debug, Deserialize)]
struct OrderResponse {
    order_id: i64,
    market: String,
    market_type: String,
    side: String,
    #[serde(rename = "type")]
    order_type: String,
    amount: String,
    price: String,
    #[serde(default)]
    client_id: Option<String>,
    #[serde(default)]
    filled_amount: Option<String>,
    #[serde(default)]
    filled_value: Option<String>,
    #[serde(default)]
    unfilled_amount: Option<String>,
    #[serde(default)]
    avg_price: Option<String>,
    created_at: i64,
    #[serde(default)]
    updated_at: Option<i64>,
}

/// Orderbook entry.
#[allow(dead_code)]
#[derive(Debug, Deserialize)]
struct OrderbookEntry {
    price: String,
    amount: String,
}

/// Orderbook response.
#[allow(dead_code)]
#[derive(Debug, Deserialize)]
struct OrderbookResponseData {
    bids: Vec<Vec<String>>,
    asks: Vec<Vec<String>>,
    #[serde(default)]
    last: Option<String>,
    #[serde(default)]
    updated_at: Option<i64>,
}

/// Funding rate entry.
#[allow(dead_code)]
#[derive(Debug, Deserialize)]
struct FundingRateResponseData {
    market: String,
    funding_rate: String,
    funding_time: i64,
}

/// Funding rate history entry.
#[derive(Debug, Deserialize)]
struct FundingRateHistoryEntry {
    market: String,
    funding_rate: String,
    funding_time: i64,
}

/// Position response for futures.
#[allow(dead_code)]
#[derive(Debug, Deserialize)]
struct PositionResponse {
    position_id: i64,
    market: String,
    side: String,
    open_interest: String,
    #[serde(default)]
    avg_entry_price: Option<String>,
    #[serde(default)]
    leverage: Option<String>,
    #[serde(default)]
    unrealized_pnl: Option<String>,
    #[serde(default)]
    realized_pnl: Option<String>,
    #[serde(default)]
    liq_price: Option<String>,
}

// ===============================================================================
// EXCHANGE TRADER IMPLEMENTATION
// ===============================================================================

#[async_trait]
impl ExchangeTrader for CoinexTrader {
    fn exchange_name(&self) -> &str {
        "coinex"
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
        let endpoint = "/v2/ping";

        let response = self
            .client
            .get(format!("{}{}", self.base_url, endpoint))
            .send()
            .await?;

        Ok(response.status().is_success())
    }

    async fn get_server_time(&self) -> Result<i64> {
        let endpoint = "/v2/time";

        let response: ServerTimeResponse = self.public_get(endpoint, "").await?;

        Ok(response.timestamp)
    }

    async fn get_balance(&self) -> Result<AccountBalance> {
        let endpoint = if self.market_type == MarketType::Spot {
            "/v2/assets/spot/balance"
        } else {
            "/v2/assets/futures/balance"
        };

        if self.market_type == MarketType::Spot {
            let balances: Vec<SpotBalanceEntry> = self.signed_get(endpoint, "").await?;

            // Find USDT balance
            let usdt_balance = balances
                .iter()
                .find(|b| b.ccy == "USDT")
                .map(|b| {
                    let available = b.available.parse::<f64>().unwrap_or(0.0);
                    let frozen = b.frozen.parse::<f64>().unwrap_or(0.0);
                    (available, frozen)
                })
                .unwrap_or((0.0, 0.0));

            Ok(AccountBalance {
                total_balance: usdt_balance.0 + usdt_balance.1,
                available_balance: usdt_balance.0,
                currency: "USDT".to_string(),
                margin_balance: None,
                used_margin: Some(usdt_balance.1),
                unrealized_pnl: None,
                realized_pnl: None,
                balances: vec![],
            })
        } else {
            let balances: Vec<FuturesBalanceEntry> = self.signed_get(endpoint, "").await?;

            // Find USDT balance
            let usdt_balance = balances
                .iter()
                .find(|b| b.ccy == "USDT")
                .map(|b| {
                    let available = b.available.parse::<f64>().unwrap_or(0.0);
                    let frozen = b.frozen.parse::<f64>().unwrap_or(0.0);
                    let margin = b.margin.as_ref().and_then(|m| m.parse::<f64>().ok()).unwrap_or(0.0);
                    let unrealized_pnl = b.unrealized_pnl.as_ref().and_then(|p| p.parse::<f64>().ok());
                    (available, frozen, margin, unrealized_pnl)
                })
                .unwrap_or((0.0, 0.0, 0.0, None));

            Ok(AccountBalance {
                total_balance: usdt_balance.0 + usdt_balance.1 + usdt_balance.2,
                available_balance: usdt_balance.0,
                currency: "USDT".to_string(),
                margin_balance: Some(usdt_balance.2),
                used_margin: Some(usdt_balance.1),
                unrealized_pnl: usdt_balance.3,
                realized_pnl: None,
                balances: vec![],
            })
        }
    }

    async fn get_current_price(&self, symbol: &str) -> Result<f64> {
        let formatted_symbol = self.format_coinex_symbol(symbol);
        let endpoint = if self.market_type == MarketType::Spot {
            "/v2/spot/ticker"
        } else {
            "/v2/futures/ticker"
        };

        let params = format!("market={}", formatted_symbol);

        let tickers: Vec<serde_json::Value> = self.public_get(endpoint, &params).await?;

        if let Some(ticker) = tickers.first() {
            let price_str = ticker.get("last")
                .and_then(|v| v.as_str())
                .ok_or_else(|| anyhow::anyhow!("No price in ticker"))?;

            return price_str.parse().context("Failed to parse price");
        }

        anyhow::bail!("No ticker data for {}", formatted_symbol)
    }

    async fn get_ticker(&self, symbol: &str) -> Result<TickerInfo> {
        let formatted_symbol = self.format_coinex_symbol(symbol);
        let endpoint = if self.market_type == MarketType::Spot {
            "/v2/spot/ticker"
        } else {
            "/v2/futures/ticker"
        };

        let params = format!("market={}", formatted_symbol);

        let tickers: Vec<serde_json::Value> = self.public_get(endpoint, &params).await?;

        if let Some(ticker) = tickers.first() {
            let last_price = ticker.get("last")
                .and_then(|v| v.as_str())
                .and_then(|s| s.parse().ok())
                .unwrap_or(0.0);

            let high_24h = ticker.get("high")
                .and_then(|v| v.as_str())
                .and_then(|s| s.parse().ok());

            let low_24h = ticker.get("low")
                .and_then(|v| v.as_str())
                .and_then(|s| s.parse().ok());

            let volume_24h = ticker.get("volume")
                .and_then(|v| v.as_str())
                .and_then(|s| s.parse().ok());

            let turnover_24h = ticker.get("value")
                .and_then(|v| v.as_str())
                .and_then(|s| s.parse().ok());

            let bid_price = ticker.get("buy")
                .and_then(|v| v.as_str())
                .and_then(|s| s.parse().ok());

            let ask_price = ticker.get("sell")
                .and_then(|v| v.as_str())
                .and_then(|s| s.parse().ok());

            let open_price: f64 = ticker.get("open")
                .and_then(|v| v.as_str())
                .and_then(|s| s.parse().ok())
                .unwrap_or(0.0);

            let change_24h = if open_price > 0.0 {
                Some(((last_price - open_price) / open_price) * 100.0)
            } else {
                None
            };

            // Futures-specific fields
            let (open_interest, open_interest_value, funding_rate, next_funding_time, mark_price, index_price) =
                if self.market_type == MarketType::Swap {
                    (
                        ticker.get("open_interest").and_then(|v| v.as_str()).and_then(|s| s.parse().ok()),
                        ticker.get("open_interest_value").and_then(|v| v.as_str()).and_then(|s| s.parse().ok()),
                        ticker.get("funding_rate").and_then(|v| v.as_str()).and_then(|s| s.parse().ok()),
                        ticker.get("next_funding_time").and_then(|v| v.as_i64()),
                        ticker.get("mark_price").and_then(|v| v.as_str()).and_then(|s| s.parse().ok()),
                        ticker.get("index_price").and_then(|v| v.as_str()).and_then(|s| s.parse().ok()),
                    )
                } else {
                    (None, None, None, None, None, None)
                };

            return Ok(TickerInfo {
                symbol: formatted_symbol,
                last_price,
                bid_price,
                ask_price,
                high_24h,
                low_24h,
                volume_24h,
                turnover_24h,
                change_24h,
                open_interest,
                open_interest_value,
                funding_rate,
                next_funding_time,
                mark_price,
                index_price,
                timestamp: timestamp_ms(),
            });
        }

        anyhow::bail!("No ticker data for {}", formatted_symbol)
    }

    async fn get_orderbook(&self, symbol: &str, depth: u32) -> Result<Orderbook> {
        let formatted_symbol = self.format_coinex_symbol(symbol);
        let endpoint = if self.market_type == MarketType::Spot {
            "/v2/spot/depth"
        } else {
            "/v2/futures/depth"
        };

        let limit = depth.min(50); // CoinEx max depth is 50
        let params = format!("market={}&limit={}&interval=0", formatted_symbol, limit);

        let response: OrderbookResponseData = self.public_get(endpoint, &params).await?;

        let bids: Vec<OrderbookLevel> = response.bids
            .iter()
            .filter_map(|level| {
                if level.len() >= 2 {
                    let price = level[0].parse().ok()?;
                    let qty = level[1].parse().ok()?;
                    Some(OrderbookLevel { price, quantity: qty })
                } else {
                    None
                }
            })
            .collect();

        let asks: Vec<OrderbookLevel> = response.asks
            .iter()
            .filter_map(|level| {
                if level.len() >= 2 {
                    let price = level[0].parse().ok()?;
                    let qty = level[1].parse().ok()?;
                    Some(OrderbookLevel { price, quantity: qty })
                } else {
                    None
                }
            })
            .collect();

        Ok(Orderbook {
            symbol: formatted_symbol,
            bids,
            asks,
            timestamp: response.updated_at.unwrap_or_else(|| timestamp_ms()),
        })
    }

    async fn set_leverage(&self, symbol: Option<&str>, leverage: u32) -> Result<bool> {
        if self.market_type == MarketType::Spot {
            return Ok(true);
        }

        let symbol = symbol.ok_or_else(|| anyhow::anyhow!("Symbol required for leverage"))?;
        let formatted_symbol = self.format_coinex_symbol(symbol);

        let endpoint = "/v2/futures/adjust-position-leverage";
        let body = serde_json::json!({
            "market": formatted_symbol,
            "leverage": leverage.to_string(),
            "margin_mode": "cross"
        });

        let _response: serde_json::Value = self.signed_post(endpoint, &body).await?;

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
        let formatted_symbol = self.format_coinex_symbol(symbol);

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
                error!("Failed to set leverage: {}", e);
            }
        }

        let side = if direction.to_lowercase() == "long" {
            OrderSide::Buy
        } else {
            OrderSide::Sell
        };

        self.place_market_order(&formatted_symbol, side, quantity).await
    }

    async fn place_market_order(
        &self,
        symbol: &str,
        side: OrderSide,
        quantity: f64,
    ) -> Result<OrderResult> {
        let formatted_symbol = self.format_coinex_symbol(symbol);

        let endpoint = if self.market_type == MarketType::Spot {
            "/v2/spot/order"
        } else {
            "/v2/futures/order"
        };

        let side_str = match side {
            OrderSide::Buy => "buy",
            OrderSide::Sell => "sell",
        };

        let market_type = if self.market_type == MarketType::Spot {
            "SPOT"
        } else {
            "FUTURES"
        };

        let body = serde_json::json!({
            "market": formatted_symbol,
            "market_type": market_type,
            "side": side_str,
            "type": "market",
            "amount": format!("{:.8}", quantity),
        });

        let response: OrderResponse = match self.signed_post(endpoint, &body).await {
            Ok(r) => r,
            Err(e) => {
                return Ok(OrderResult::failure(
                    formatted_symbol,
                    side,
                    e.to_string(),
                ));
            }
        };

        let filled_qty: f64 = response.filled_amount
            .as_ref()
            .and_then(|s| s.parse().ok())
            .unwrap_or(quantity);

        let avg_price: Option<f64> = response.avg_price
            .as_ref()
            .and_then(|s| s.parse().ok());

        info!("CoinEx order placed: {}", response.order_id);

        Ok(OrderResult {
            success: true,
            order_id: Some(response.order_id.to_string()),
            symbol: formatted_symbol,
            side,
            order_type: OrderType::Market,
            quantity: Some(filled_qty),
            entry_price: avg_price,
            position_size: Some(filled_qty),
            position_usdt: avg_price.map(|p| filled_qty * p),
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
        let formatted_symbol = self.format_coinex_symbol(symbol);

        let endpoint = if self.market_type == MarketType::Spot {
            "/v2/spot/order"
        } else {
            "/v2/futures/order"
        };

        let side_str = match side {
            OrderSide::Buy => "buy",
            OrderSide::Sell => "sell",
        };

        let market_type = if self.market_type == MarketType::Spot {
            "SPOT"
        } else {
            "FUTURES"
        };

        let body = serde_json::json!({
            "market": formatted_symbol,
            "market_type": market_type,
            "side": side_str,
            "type": "limit",
            "amount": format!("{:.8}", quantity),
            "price": format!("{:.8}", price),
        });

        let response: OrderResponse = match self.signed_post(endpoint, &body).await {
            Ok(r) => r,
            Err(e) => {
                return Ok(OrderResult::failure(
                    formatted_symbol,
                    side,
                    e.to_string(),
                ));
            }
        };

        info!("CoinEx limit order placed: {}", response.order_id);

        Ok(OrderResult {
            success: true,
            order_id: Some(response.order_id.to_string()),
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
        let formatted_symbol = self.format_coinex_symbol(symbol);

        let endpoint = if self.market_type == MarketType::Spot {
            "/v2/spot/cancel-order"
        } else {
            "/v2/futures/cancel-order"
        };

        let market_type = if self.market_type == MarketType::Spot {
            "SPOT"
        } else {
            "FUTURES"
        };

        let body = serde_json::json!({
            "market": formatted_symbol,
            "market_type": market_type,
            "order_id": order_id.parse::<i64>().unwrap_or(0),
        });

        let _response: serde_json::Value = self.signed_post(endpoint, &body).await?;

        info!("CoinEx order cancelled: {}", order_id);
        Ok(true)
    }

    async fn get_order_status(&self, symbol: &str, order_id: &str) -> Result<OrderInfo> {
        let formatted_symbol = self.format_coinex_symbol(symbol);

        let endpoint = if self.market_type == MarketType::Spot {
            "/v2/spot/order-status"
        } else {
            "/v2/futures/order-status"
        };

        let market_type = if self.market_type == MarketType::Spot {
            "SPOT"
        } else {
            "FUTURES"
        };

        let params = format!("market={}&market_type={}&order_id={}", formatted_symbol, market_type, order_id);

        let response: OrderResponse = self.signed_get(endpoint, &params).await?;

        let status = parse_order_status_str(&response.order_type);

        let side = match response.side.as_str() {
            "buy" => OrderSide::Buy,
            _ => OrderSide::Sell,
        };

        let order_type = match response.order_type.as_str() {
            "limit" => OrderType::Limit,
            _ => OrderType::Market,
        };

        let quantity: f64 = response.amount.parse().unwrap_or(0.0);
        let filled_quantity: f64 = response.filled_amount
            .as_ref()
            .and_then(|s| s.parse().ok())
            .unwrap_or(0.0);
        let price: f64 = response.price.parse().unwrap_or(0.0);
        let avg_price: Option<f64> = response.avg_price.as_ref().and_then(|s| s.parse().ok());

        Ok(OrderInfo {
            order_id: response.order_id.to_string(),
            client_order_id: response.client_id,
            symbol: response.market,
            status,
            side,
            order_type,
            price,
            quantity,
            filled_quantity,
            remaining_quantity: quantity - filled_quantity,
            avg_price,
            timestamp: response.created_at,
            time_in_force: TimeInForce::GTC,
            reduce_only: false,
        })
    }

    async fn close_position(&self, symbol: &str) -> Result<OrderResult> {
        if self.market_type == MarketType::Spot {
            anyhow::bail!("Close position not supported for spot trading");
        }

        let formatted_symbol = self.format_coinex_symbol(symbol);

        // Get position info first
        let endpoint = "/v2/futures/pending-position";
        let params = format!("market={}", formatted_symbol);

        let positions: Vec<PositionResponse> = self.signed_get(endpoint, &params).await?;

        if let Some(position) = positions.first() {
            let qty: f64 = position.open_interest.parse().unwrap_or(0.0);

            if qty.abs() < 0.0001 {
                return Ok(OrderResult::failure(
                    formatted_symbol,
                    OrderSide::Sell,
                    "No position to close".to_string(),
                ));
            }

            // CoinEx close position endpoint
            let close_endpoint = "/v2/futures/close-position";
            let body = serde_json::json!({
                "market": formatted_symbol,
                "position_id": position.position_id,
                "type": "market"
            });

            let response: OrderResponse = self.signed_post(close_endpoint, &body).await?;

            let side = if position.side == "long" {
                OrderSide::Sell
            } else {
                OrderSide::Buy
            };

            return Ok(OrderResult {
                success: true,
                order_id: Some(response.order_id.to_string()),
                symbol: formatted_symbol,
                side,
                order_type: OrderType::Market,
                quantity: Some(qty),
                entry_price: None,
                position_size: Some(0.0),
                position_usdt: None,
                error: None,
                raw_response: None,
            });
        }

        Ok(OrderResult::failure(
            formatted_symbol,
            OrderSide::Sell,
            "Position not found".to_string(),
        ))
    }

    async fn get_funding_rate_history(
        &self,
        symbol: &str,
        limit: u32,
    ) -> Result<Vec<FundingRateEntry>> {
        if self.market_type != MarketType::Swap {
            anyhow::bail!("Funding rate only available for futures");
        }

        let formatted_symbol = self.format_coinex_symbol(symbol);
        let endpoint = "/v2/futures/funding-rate-history";
        let params = format!("market={}&limit={}", formatted_symbol, limit.min(1000));

        let response: Vec<FundingRateHistoryEntry> = self.public_get(endpoint, &params).await?;

        let entries: Vec<FundingRateEntry> = response
            .iter()
            .filter_map(|e| {
                Some(FundingRateEntry {
                    symbol: e.market.clone(),
                    funding_rate: e.funding_rate.parse().ok()?,
                    funding_time: e.funding_time,
                })
            })
            .collect();

        Ok(entries)
    }

    async fn get_open_interest_history(
        &self,
        _symbol: &str,
        _interval: &str,
        _limit: u32,
    ) -> Result<Vec<OpenInterestEntry>> {
        // CoinEx doesn't have a public open interest history endpoint
        anyhow::bail!("Open interest history not available on CoinEx")
    }

    async fn test_ws_orderbook(
        &self,
        symbol: &str,
        depth: u32,
        timeout_ms: u64,
    ) -> Result<WsTestResult> {
        let ws_url = self.ws_url();
        let formatted_symbol = self.format_coinex_symbol(symbol);

        let start = std::time::Instant::now();
        let (mut ws_stream, _) = connect_async(ws_url)
            .await
            .context("Failed to connect to CoinEx WebSocket")?;

        let connect_time = start.elapsed().as_millis() as u64;

        // Subscribe to depth
        let subscribe_msg = serde_json::json!({
            "method": "depth.subscribe",
            "params": {
                "market_list": [
                    {
                        "market": formatted_symbol,
                        "limit": depth.min(50),
                        "interval": "0",
                        "merge": false
                    }
                ]
            },
            "id": 1
        });

        ws_stream
            .send(Message::Text(serde_json::to_string(&subscribe_msg)?))
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
                                if parsed.get("method").and_then(|v| v.as_str()) == Some("depth.update") {
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
            subscription_type: format!("depth.subscribe:{}", formatted_symbol),
            data_sample,
        })
    }

    async fn test_ws_trades(&self, symbol: &str, timeout_ms: u64) -> Result<WsTestResult> {
        let ws_url = self.ws_url();
        let formatted_symbol = self.format_coinex_symbol(symbol);

        let start = std::time::Instant::now();
        let (mut ws_stream, _) = connect_async(ws_url)
            .await
            .context("Failed to connect to CoinEx WebSocket")?;

        let connect_time = start.elapsed().as_millis() as u64;

        // Subscribe to deals (trades)
        let subscribe_msg = serde_json::json!({
            "method": "deals.subscribe",
            "params": {
                "market_list": [formatted_symbol]
            },
            "id": 1
        });

        ws_stream
            .send(Message::Text(serde_json::to_string(&subscribe_msg)?))
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
                                if parsed.get("method").and_then(|v| v.as_str()) == Some("deals.update") {
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
            subscription_type: format!("deals.subscribe:{}", formatted_symbol),
            data_sample,
        })
    }

    async fn test_ws_ticker(&self, symbol: &str, timeout_ms: u64) -> Result<WsTestResult> {
        let ws_url = self.ws_url();
        let formatted_symbol = self.format_coinex_symbol(symbol);

        let start = std::time::Instant::now();
        let (mut ws_stream, _) = connect_async(ws_url)
            .await
            .context("Failed to connect to CoinEx WebSocket")?;

        let connect_time = start.elapsed().as_millis() as u64;

        // Subscribe to ticker
        let subscribe_msg = serde_json::json!({
            "method": "state.subscribe",
            "params": {
                "market_list": [formatted_symbol]
            },
            "id": 1
        });

        ws_stream
            .send(Message::Text(serde_json::to_string(&subscribe_msg)?))
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
                                if parsed.get("method").and_then(|v| v.as_str()) == Some("state.update") {
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
            subscription_type: format!("state.subscribe:{}", formatted_symbol),
            data_sample,
        })
    }

    async fn subscribe_order_updates(
        &self,
        sender: broadcast::Sender<OrderUpdateEvent>,
    ) -> Result<JoinHandle<()>> {
        let api_key = self.api_key.clone();
        let api_secret = self.api_secret.clone();
        let market_type = self.market_type;
        let exchange_name = "coinex".to_string();

        let handle = tokio::spawn(async move {
            let mut reconnect_delay = INITIAL_RECONNECT_DELAY_SECS;

            loop {
                let ws_url = match market_type {
                    MarketType::Spot => SPOT_WS_URL,
                    MarketType::Swap => FUTURES_WS_URL,
                };

                match connect_async(ws_url).await {
                    Ok((mut ws_stream, _)) => {
                        info!("[{}] Connected to user data stream", exchange_name);
                        reconnect_delay = INITIAL_RECONNECT_DELAY_SECS;

                        // Authenticate
                        let timestamp = timestamp_ms().to_string();
                        let sign_str = format!("{}#{}", api_key, timestamp);
                        let mut hasher = Sha256::new();
                        hasher.update(format!("{}{}", sign_str, api_secret).as_bytes());
                        let signature = hex::encode(hasher.finalize());

                        let auth_msg = serde_json::json!({
                            "method": "server.sign",
                            "params": {
                                "access_id": api_key,
                                "signed_str": signature,
                                "timestamp": timestamp
                            },
                            "id": 1
                        });

                        if let Err(e) = ws_stream.send(Message::Text(serde_json::to_string(&auth_msg).unwrap())).await {
                            error!("[{}] Failed to send auth message: {}", exchange_name, e);
                            continue;
                        }

                        // Subscribe to order updates
                        let subscribe_msg = serde_json::json!({
                            "method": "order.subscribe",
                            "params": {},
                            "id": 2
                        });

                        if let Err(e) = ws_stream.send(Message::Text(serde_json::to_string(&subscribe_msg).unwrap())).await {
                            error!("[{}] Failed to subscribe to orders: {}", exchange_name, e);
                            continue;
                        }

                        // Ping interval
                        let mut ping_interval = tokio::time::interval(tokio::time::Duration::from_secs(30));
                        ping_interval.tick().await;

                        loop {
                            tokio::select! {
                                msg = ws_stream.next() => {
                                    match msg {
                                        Some(Ok(Message::Text(text))) => {
                                            if let Ok(event_json) = serde_json::from_str::<serde_json::Value>(&text) {
                                                let method = event_json.get("method").and_then(|v| v.as_str()).unwrap_or("");

                                                if method == "order.update" {
                                                    if let Some(data) = event_json.get("data") {
                                                        let event = OrderUpdateEvent {
                                                            exchange: exchange_name.clone(),
                                                            order_id: data.get("order_id").and_then(|v| v.as_i64()).map(|v| v.to_string()).unwrap_or_default(),
                                                            client_order_id: data.get("client_id").and_then(|v| v.as_str()).map(|s| s.to_string()),
                                                            symbol: data.get("market").and_then(|v| v.as_str()).unwrap_or("").to_string(),
                                                            status: parse_order_status_str(
                                                                data.get("status").and_then(|v| v.as_str()).unwrap_or("open")
                                                            ),
                                                            side: if data.get("side").and_then(|v| v.as_str()) == Some("buy") { OrderSide::Buy } else { OrderSide::Sell },
                                                            order_type: if data.get("type").and_then(|v| v.as_str()) == Some("market") { OrderType::Market } else { OrderType::Limit },
                                                            price: data.get("price").and_then(|v| v.as_str()).and_then(|s| s.parse().ok()).unwrap_or(0.0),
                                                            quantity: data.get("amount").and_then(|v| v.as_str()).and_then(|s| s.parse().ok()).unwrap_or(0.0),
                                                            filled_quantity: data.get("filled_amount").and_then(|v| v.as_str()).and_then(|s| s.parse().ok()).unwrap_or(0.0),
                                                            avg_fill_price: data.get("avg_price").and_then(|v| v.as_str()).and_then(|s| s.parse().ok()),
                                                            fee: data.get("base_fee").and_then(|v| v.as_str()).and_then(|s| s.parse().ok()),
                                                            fee_currency: data.get("ccy").and_then(|v| v.as_str()).map(|s| s.to_string()),
                                                            exchange_timestamp: data.get("updated_at").and_then(|v| v.as_i64()).unwrap_or(0),
                                                            received_at: timestamp_ms(),
                                                            raw: Some(event_json.clone()),
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
                                            warn!("[{}] User data stream closed", exchange_name);
                                            break;
                                        }
                                        Some(Err(e)) => {
                                            error!("[{}] User data stream error: {}", exchange_name, e);
                                            break;
                                        }
                                        None => {
                                            warn!("[{}] User data stream ended", exchange_name);
                                            break;
                                        }
                                        _ => {}
                                    }
                                }
                                _ = ping_interval.tick() => {
                                    let ping_msg = serde_json::json!({
                                        "method": "server.ping",
                                        "params": {},
                                        "id": 100
                                    });
                                    if let Err(e) = ws_stream.send(Message::Text(serde_json::to_string(&ping_msg).unwrap())).await {
                                        warn!("[{}] Failed to send ping: {}", exchange_name, e);
                                        break;
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
                    "[{}] Reconnecting user data stream in {}s...",
                    exchange_name, reconnect_delay
                );
                tokio::time::sleep(tokio::time::Duration::from_secs(reconnect_delay)).await;
                reconnect_delay = next_reconnect_delay(reconnect_delay);
            }
        });

        Ok(handle)
    }
}
