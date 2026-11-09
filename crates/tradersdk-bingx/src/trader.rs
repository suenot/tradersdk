//! BingX exchange trader implementation.
//!
//! Service-based implementation following the same pattern as bybit/trader.rs.
//! Delegates all functionality to dedicated services:
//! - RestService: REST API calls
//! - MarketStreamService: Public market data streams
//! - UserDataService: Private user data streams

use anyhow::{Context, Result};
use async_trait::async_trait;
use tokio::sync::broadcast;
use tokio::task::JoinHandle;
use tracing::{info, warn};
use tradersdk_core::models::{
    AccountBalance, Balance, ExchangeCapabilities, FundingRateEntry, MarketType, OpenInterestEntry,
    OrderInfo, OrderResult, OrderSide, OrderType, OrderUpdateEvent, Orderbook, OrderbookLevel,
    TickerInfo, TimeInForce, WsTestResult,
};
use tradersdk_core::utils::timestamp_ms;
use tradersdk_core::ExchangeTrader;

use super::config::endpoints;
use super::helpers::{format_symbol, parse_order_status};
use super::models::*;
use super::services::{MarketStreamService, RestService, UserDataService};

/// BingX exchange trader.
///
/// Uses service-based architecture for clean separation of concerns:
/// - `rest`: REST API service for signed and public requests
/// - `ws_stream`: WebSocket service for market data streams
pub struct BingxTrader {
    rest: RestService,
    ws_stream: MarketStreamService,
    testnet: bool,
    market_type: MarketType,
}

impl BingxTrader {
    /// Create a new BingX trader.
    ///
    /// # Arguments
    /// * `api_key` - BingX API key
    /// * `api_secret` - BingX API secret
    /// * `testnet` - Whether to use testnet (true) or mainnet (false)
    /// * `market_type` - Market type (Spot or Swap/Perpetual)
    pub fn new(api_key: &str, api_secret: &str, testnet: bool, market_type: MarketType) -> Self {
        Self {
            rest: RestService::new(api_key, api_secret, testnet, market_type),
            ws_stream: MarketStreamService::new(testnet, market_type),
            testnet,
            market_type,
        }
    }

    /// Get current position for a symbol.
    ///
    /// Returns `None` for spot trading or if no position exists.
    async fn get_position(&self, symbol: &str) -> Result<Option<PositionData>> {
        if self.market_type == MarketType::Spot {
            return Ok(None);
        }

        let params = format!("symbol={}", symbol);
        let response: BingxResponse<Vec<PositionData>> = self
            .rest
            .signed_get(endpoints::SWAP_POSITIONS, &params)
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

#[async_trait]
impl ExchangeTrader for BingxTrader {
    fn exchange_name(&self) -> &str {
        "bingx"
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
        let mut retries = 3;
        loop {
            let url = format!("{}{}", self.rest.base_url(), endpoints::SWAP_SERVER_TIME);
            let result = self.rest.client().get(&url).send().await;

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
        let url = format!("{}{}", self.rest.base_url(), endpoints::SWAP_SERVER_TIME);
        let response: BingxResponse<ServerTimeData> = self.rest.public_get(&url).await?;

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
                    self.rest.signed_get(endpoints::SWAP_BALANCE, "").await?;

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
                    self.rest.signed_get(endpoints::SPOT_BALANCE, "").await?;

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
                            balances.push(Balance {
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
        let formatted_symbol = format_symbol(symbol);
        let endpoint = match self.market_type {
            MarketType::Swap => endpoints::SWAP_PRICE,
            MarketType::Spot => endpoints::SPOT_PRICE,
        };

        let url = format!(
            "{}{}?symbol={}",
            self.rest.base_url(),
            endpoint,
            formatted_symbol
        );
        let response: BingxResponse<PriceData> = self.rest.public_get(&url).await?;

        if !response.is_success() {
            anyhow::bail!("BingX API error: {}", response.error_message());
        }

        let price = response
            .data
            .and_then(|d| d.price.parse::<f64>().ok())
            .ok_or_else(|| anyhow::anyhow!("Failed to get price for {}", symbol))?;

        Ok(price)
    }

    async fn get_ticker(&self, symbol: &str) -> Result<TickerInfo> {
        let formatted_symbol = format_symbol(symbol);
        let endpoint = match self.market_type {
            MarketType::Swap => endpoints::SWAP_TICKER,
            MarketType::Spot => endpoints::SPOT_TICKER,
        };

        let url = format!(
            "{}{}?symbol={}",
            self.rest.base_url(),
            endpoint,
            formatted_symbol
        );
        let response: BingxResponse<TickerData> = self.rest.public_get(&url).await?;

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
            change_24h: data
                .price_change_percent
                .as_ref()
                .and_then(|s| s.parse().ok()),
            open_interest: data.open_interest.as_ref().and_then(|s| s.parse().ok()),
            open_interest_value: None,
            funding_rate: None, // Need separate API call
            next_funding_time: None,
            mark_price: None,
            index_price: None,
            timestamp: timestamp_ms(),
        })
    }

    async fn get_orderbook(&self, symbol: &str, depth: u32) -> Result<Orderbook> {
        let formatted_symbol = format_symbol(symbol);
        let limit = depth.min(100);
        let endpoint = match self.market_type {
            MarketType::Swap => endpoints::SWAP_DEPTH,
            MarketType::Spot => endpoints::SPOT_DEPTH,
        };

        let url = format!(
            "{}{}?symbol={}&limit={}",
            self.rest.base_url(),
            endpoint,
            formatted_symbol,
            limit
        );
        let response: BingxResponse<OrderbookData> = self.rest.public_get(&url).await?;

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

    async fn set_leverage(&self, symbol: Option<&str>, leverage: u32) -> Result<bool> {
        if self.market_type == MarketType::Spot {
            return Ok(true);
        }

        let symbol = symbol.ok_or_else(|| anyhow::anyhow!("Symbol required for leverage"))?;
        let formatted_symbol = format_symbol(symbol);
        let params = format!(
            "symbol={}&side=BOTH&leverage={}",
            formatted_symbol, leverage
        );

        let response: BingxResponse<LeverageData> = self
            .rest
            .signed_post(endpoints::SWAP_LEVERAGE, &params)
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
        let formatted_symbol = format_symbol(symbol);

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
        let formatted_symbol = format_symbol(symbol);
        let side_str = match side {
            OrderSide::Buy => "BUY",
            OrderSide::Sell => "SELL",
        };

        let endpoint = match self.market_type {
            MarketType::Swap => endpoints::SWAP_ORDER,
            MarketType::Spot => endpoints::SPOT_ORDER,
        };

        let params = format!(
            "symbol={}&side={}&type=MARKET&quantity={:.8}",
            formatted_symbol, side_str, quantity
        );

        let response: BingxResponse<OrderData> = self.rest.signed_post(endpoint, &params).await?;

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
        let formatted_symbol = format_symbol(symbol);
        let side_str = match side {
            OrderSide::Buy => "BUY",
            OrderSide::Sell => "SELL",
        };

        let endpoint = match self.market_type {
            MarketType::Swap => endpoints::SWAP_ORDER,
            MarketType::Spot => endpoints::SPOT_ORDER,
        };

        let params = format!(
            "symbol={}&side={}&type=LIMIT&quantity={:.8}&price={:.8}",
            formatted_symbol, side_str, quantity, price
        );

        let response: BingxResponse<OrderData> = self.rest.signed_post(endpoint, &params).await?;

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
        let formatted_symbol = format_symbol(symbol);

        let endpoint = match self.market_type {
            MarketType::Swap => endpoints::SWAP_ORDER,
            MarketType::Spot => endpoints::SPOT_CANCEL,
        };

        let params = format!("symbol={}&orderId={}", formatted_symbol, order_id);

        let response: BingxResponse<serde_json::Value> =
            self.rest.signed_delete(endpoint, &params).await?;

        if !response.is_success() {
            warn!(
                "Failed to cancel order {}: {}",
                order_id,
                response.error_message()
            );
            return Ok(false);
        }

        info!("BingX order cancelled: {}", order_id);
        Ok(true)
    }

    async fn get_order_status(&self, symbol: &str, order_id: &str) -> Result<OrderInfo> {
        let formatted_symbol = format_symbol(symbol);

        let endpoint = match self.market_type {
            MarketType::Swap => endpoints::SWAP_ORDER,
            MarketType::Spot => endpoints::SPOT_QUERY,
        };

        let params = format!("symbol={}&orderId={}", formatted_symbol, order_id);

        let response: BingxResponse<OrderData> = self.rest.signed_get(endpoint, &params).await?;

        if !response.is_success() {
            anyhow::bail!("BingX API error: {}", response.error_message());
        }

        let data = response
            .data
            .ok_or_else(|| anyhow::anyhow!("Order not found"))?;

        let status_str = data.status.as_deref().unwrap_or("NEW");
        let status = parse_order_status(status_str);

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

        let formatted_symbol = format_symbol(symbol);

        // First try to get position info
        let position = self.get_position(&formatted_symbol).await?;

        match position {
            Some(pos) => {
                let size: f64 = pos
                    .position_amt
                    .parse()
                    .context("Failed to parse position size")?;
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
                    .rest
                    .signed_post(endpoints::SWAP_CLOSE_ALL_POSITIONS, &params)
                    .await?;

                if !response.is_success() {
                    return Ok(OrderResult::failure(
                        formatted_symbol,
                        OrderSide::Sell,
                        response.error_message(),
                    ));
                }

                // Get the order IDs from the response
                let order_ids = response.data.and_then(|d| d.success).unwrap_or_default();

                let order_id = order_ids
                    .first()
                    .map(|v| match v {
                        serde_json::Value::String(s) => s.clone(),
                        serde_json::Value::Number(n) => n.to_string(),
                        _ => String::new(),
                    })
                    .unwrap_or_default();

                info!(
                    "BingX position closed for {}: order_id={}",
                    formatted_symbol, order_id
                );

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
        format_symbol(base_symbol)
    }

    async fn get_funding_rate_history(
        &self,
        symbol: &str,
        limit: u32,
    ) -> Result<Vec<FundingRateEntry>> {
        if self.market_type != MarketType::Swap {
            anyhow::bail!("Funding rate only available for perpetual futures");
        }

        let formatted_symbol = format_symbol(symbol);
        let url = format!(
            "{}{}?symbol={}&limit={}",
            self.rest.base_url(),
            endpoints::SWAP_FUNDING_RATE,
            formatted_symbol,
            limit.min(100)
        );
        let response: BingxResponse<Vec<FundingRateData>> = self.rest.public_get(&url).await?;

        if !response.is_success() {
            anyhow::bail!("BingX API error: {}", response.error_message());
        }

        let entries: Vec<FundingRateEntry> = response
            .data
            .unwrap_or_default()
            .iter()
            .filter_map(|e| {
                Some(FundingRateEntry {
                    symbol: e.symbol.clone().unwrap_or_else(|| formatted_symbol.clone()),
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

        let formatted_symbol = format_symbol(symbol);

        // BingX provides current OI only, not history
        let url = format!(
            "{}{}?symbol={}",
            self.rest.base_url(),
            endpoints::SWAP_OPEN_INTEREST,
            formatted_symbol
        );
        let response: BingxResponse<OpenInterestData> = self.rest.public_get(&url).await?;

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

    async fn test_ws_orderbook(
        &self,
        symbol: &str,
        depth: u32,
        timeout_ms: u64,
    ) -> Result<WsTestResult> {
        self.ws_stream
            .test_ws_orderbook(symbol, depth, timeout_ms)
            .await
    }

    async fn test_ws_trades(&self, symbol: &str, timeout_ms: u64) -> Result<WsTestResult> {
        self.ws_stream.test_ws_trades(symbol, timeout_ms).await
    }

    async fn test_ws_ticker(&self, symbol: &str, timeout_ms: u64) -> Result<WsTestResult> {
        self.ws_stream.test_ws_ticker(symbol, timeout_ms).await
    }

    async fn subscribe_order_updates(
        &self,
        sender: broadcast::Sender<OrderUpdateEvent>,
    ) -> Result<JoinHandle<()>> {
        let user_data_service = UserDataService::new(
            self.rest.api_key(),
            self.rest.api_secret(),
            self.testnet,
            Some(self.rest.base_url()),
        );

        user_data_service.subscribe_order_updates(sender)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_format_symbol() {
        assert_eq!(format_symbol("BTC"), "BTC-USDT");
        assert_eq!(format_symbol("BTCUSDT"), "BTC-USDT");
        assert_eq!(format_symbol("BTC/USDT"), "BTC-USDT");
        assert_eq!(format_symbol("BTC-USDT"), "BTC-USDT");
        assert_eq!(format_symbol("eth"), "ETH-USDT");
    }

    #[test]
    fn test_trader_creation() {
        let trader = BingxTrader::new("key", "secret", true, MarketType::Swap);
        assert_eq!(trader.exchange_name(), "bingx");
        assert_eq!(trader.market_type(), MarketType::Swap);
    }
}
