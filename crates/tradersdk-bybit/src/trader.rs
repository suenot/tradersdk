//! Bybit exchange trader implementation.
//!
//! Service-based implementation following the same pattern as binance/trader.rs.
//! Delegates all functionality to dedicated services:
//! - RestService: REST API calls
//! - WsTradingService: WebSocket trading operations
//! - MarketStreamService: Public market data streams
//! - UserDataService: Private user data streams

use anyhow::{Context, Result};
use async_trait::async_trait;
use tokio::sync::broadcast;
use tokio::task::JoinHandle;
use tracing::warn;
use tradersdk_core::models::{
    AccountBalance, ExchangeCapabilities, FundingRateEntry, MarketType, OpenInterestEntry,
    OrderInfo, OrderResult, OrderSide, OrderType, OrderUpdateEvent, Orderbook, OrderbookLevel,
    TickerInfo, TimeInForce, WsTestResult,
};
use tradersdk_core::utils::timestamp_ms;
use tradersdk_core::ExchangeTrader;

use crate::config::endpoints;
use crate::helpers::{format_symbol, parse_order_status};
use crate::models::{
    BybitResponse, FundingHistoryResult, OpenInterestResult, OrderbookResult, PositionInfo,
    PositionListResult, ServerTimeResult, TickerResult, WalletBalanceResult,
};
use crate::services::{MarketStreamService, RestService, UserDataService, WsTradingService};

/// Bybit exchange trader.
///
/// Uses service-based architecture for clean separation of concerns:
/// - `rest`: REST API service for signed and public requests
/// - `ws_trading`: WebSocket service for low-latency order operations
/// - `ws_stream`: WebSocket service for market data streams
pub struct BybitTrader {
    rest: RestService,
    ws_trading: WsTradingService,
    ws_stream: MarketStreamService,
    testnet: bool,
    market_type: MarketType,
}

impl BybitTrader {
    /// Create a new Bybit trader.
    ///
    /// # Arguments
    /// * `api_key` - Bybit API key
    /// * `api_secret` - Bybit API secret
    /// * `testnet` - Whether to use testnet (true) or mainnet (false)
    /// * `market_type` - Market type (Spot or Swap/Linear perpetual)
    pub fn new(api_key: &str, api_secret: &str, testnet: bool, market_type: MarketType) -> Self {
        Self {
            rest: RestService::new(api_key, api_secret, testnet, market_type),
            ws_trading: WsTradingService::new(api_key, api_secret, testnet, market_type),
            ws_stream: MarketStreamService::new(testnet, market_type),
            testnet,
            market_type,
        }
    }

    /// Get current position for a symbol.
    ///
    /// Returns `None` for spot trading or if no position exists.
    async fn get_position(&self, symbol: &str) -> Result<Option<PositionInfo>> {
        if self.market_type == MarketType::Spot {
            return Ok(None);
        }

        let params = format!("category=linear&symbol={}", symbol);
        let response: BybitResponse<PositionListResult> = self
            .rest
            .signed_get(endpoints::POSITION_LIST, &params)
            .await?;

        if let Some(result) = response.result {
            if let Some(pos) = result.list.first() {
                let size: f64 = pos.size.parse().unwrap_or(0.0);
                if size.abs() > 0.0 {
                    return Ok(Some(pos.clone()));
                }
            }
        }

        Ok(None)
    }

    /// Set Take Profit and Stop Loss for a position.
    ///
    /// Only available for perpetual futures (Swap market type).
    async fn set_trading_stop(
        &self,
        symbol: &str,
        take_profit: Option<f64>,
        stop_loss: Option<f64>,
        position_idx: i32,
    ) -> Result<bool> {
        if self.market_type == MarketType::Spot {
            return Ok(true); // TP/SL not supported for spot
        }

        let mut body = serde_json::json!({
            "category": "linear",
            "symbol": symbol,
            "positionIdx": position_idx,
        });

        if let Some(tp) = take_profit {
            body["takeProfit"] = serde_json::json!(format!("{:.8}", tp));
            body["tpTriggerBy"] = serde_json::json!("LastPrice");
        }

        if let Some(sl) = stop_loss {
            body["stopLoss"] = serde_json::json!(format!("{:.8}", sl));
            body["slTriggerBy"] = serde_json::json!("LastPrice");
        }

        let response: BybitResponse<serde_json::Value> = self
            .rest
            .signed_post(endpoints::TRADING_STOP, &body)
            .await?;

        if response.is_success() {
            tracing::info!(
                "Set trading stop for {}: TP={:?}, SL={:?}",
                symbol,
                take_profit,
                stop_loss
            );
            Ok(true)
        } else {
            tracing::error!(
                "Failed to set trading stop for {}: {}",
                symbol,
                response.ret_msg
            );
            Ok(false)
        }
    }
}

#[async_trait]
impl ExchangeTrader for BybitTrader {
    fn exchange_name(&self) -> &str {
        "bybit"
    }

    fn market_type(&self) -> MarketType {
        self.market_type
    }

    fn capabilities(&self) -> ExchangeCapabilities {
        tradersdk_core::exchange_capabilities! {
            spot: Supported,
            swap: Supported,
            futures: Supported,
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
        let response: BybitResponse<ServerTimeResult> =
            self.rest.public_get(endpoints::SERVER_TIME).await?;

        Ok(response.is_success())
    }

    async fn get_server_time(&self) -> Result<i64> {
        let response: BybitResponse<ServerTimeResult> =
            self.rest.public_get(endpoints::SERVER_TIME).await?;

        if let Some(result) = response.result {
            let seconds: i64 = result
                .time_second
                .parse()
                .context("Failed to parse server time")?;
            Ok(seconds * 1000)
        } else {
            anyhow::bail!("Failed to get server time: {}", response.ret_msg)
        }
    }

    async fn get_balance(&self) -> Result<AccountBalance> {
        let params = "accountType=UNIFIED";
        let response: BybitResponse<WalletBalanceResult> = self
            .rest
            .signed_get(endpoints::WALLET_BALANCE, params)
            .await?;

        if let Some(result) = response.result {
            if let Some(account) = result.list.first() {
                let available: f64 = account
                    .total_available_balance
                    .parse()
                    .context("Failed to parse available balance")?;
                return Ok(AccountBalance::simple(available, available, "USDT"));
            }
        }

        // If no balance found, return zero balance instead of error
        Ok(AccountBalance::default())
    }

    async fn get_current_price(&self, symbol: &str) -> Result<f64> {
        let formatted_symbol = format_symbol(symbol);
        let params = self
            .rest
            .with_category(&format!("symbol={}", formatted_symbol));
        let response: BybitResponse<TickerResult> = self
            .rest
            .public_get_with_params(endpoints::TICKERS, &params)
            .await?;

        if let Some(result) = response.result {
            if let Some(ticker) = result.list.first() {
                let price: f64 = ticker
                    .last_price
                    .parse()
                    .context("Failed to parse last price")?;
                return Ok(price);
            }
        }

        anyhow::bail!("Failed to get price for {}: {}", symbol, response.ret_msg)
    }

    async fn get_ticker(&self, symbol: &str) -> Result<TickerInfo> {
        let formatted_symbol = format_symbol(symbol);
        let params = self
            .rest
            .with_category(&format!("symbol={}", formatted_symbol));
        let response: BybitResponse<TickerResult> = self
            .rest
            .public_get_with_params(endpoints::TICKERS, &params)
            .await?;

        if let Some(result) = response.result {
            if let Some(t) = result.list.first() {
                return Ok(TickerInfo {
                    symbol: t.symbol.clone(),
                    last_price: t.last_price.parse().unwrap_or(0.0),
                    bid_price: t.bid1_price.as_ref().and_then(|p| p.parse().ok()),
                    ask_price: t.ask1_price.as_ref().and_then(|p| p.parse().ok()),
                    high_24h: t.high_price_24h.as_ref().and_then(|p| p.parse().ok()),
                    low_24h: t.low_price_24h.as_ref().and_then(|p| p.parse().ok()),
                    volume_24h: t.volume_24h.as_ref().and_then(|p| p.parse().ok()),
                    turnover_24h: t.turnover_24h.as_ref().and_then(|p| p.parse().ok()),
                    change_24h: None,
                    open_interest: t.open_interest.as_ref().and_then(|p| p.parse().ok()),
                    open_interest_value: t
                        .open_interest_value
                        .as_ref()
                        .and_then(|p| p.parse().ok()),
                    funding_rate: t.funding_rate.as_ref().and_then(|p| p.parse().ok()),
                    next_funding_time: t.next_funding_time.as_ref().and_then(|p| p.parse().ok()),
                    mark_price: t.mark_price.as_ref().and_then(|p| p.parse().ok()),
                    index_price: t.index_price.as_ref().and_then(|p| p.parse().ok()),
                    timestamp: timestamp_ms(),
                });
            }
        }

        anyhow::bail!("Failed to get ticker for {}: {}", symbol, response.ret_msg)
    }

    async fn get_orderbook(&self, symbol: &str, depth: u32) -> Result<Orderbook> {
        let formatted_symbol = format_symbol(symbol);
        let limit = depth.min(200);
        let params = self
            .rest
            .with_category(&format!("symbol={}&limit={}", formatted_symbol, limit));

        let response: BybitResponse<OrderbookResult> = self
            .rest
            .public_get_with_params(endpoints::ORDERBOOK, &params)
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
                symbol: if result.s.is_empty() {
                    formatted_symbol
                } else {
                    result.s
                },
                bids,
                asks,
                timestamp: result.ts,
            });
        }

        anyhow::bail!("Failed to get orderbook: {}", response.ret_msg)
    }

    async fn set_leverage(&self, symbol: Option<&str>, leverage: u32) -> Result<bool> {
        if self.market_type == MarketType::Spot {
            return Ok(true);
        }

        let formatted_symbol = match symbol {
            Some(sym) => format_symbol(sym),
            None => return Ok(true),
        };

        let body = serde_json::json!({
            "category": "linear",
            "symbol": formatted_symbol,
            "buyLeverage": leverage.to_string(),
            "sellLeverage": leverage.to_string(),
        });

        let response: BybitResponse<serde_json::Value> = self
            .rest
            .signed_post(endpoints::SET_LEVERAGE, &body)
            .await?;

        // Leverage setting can fail with "leverage not changed" (110043) which is fine
        if response.is_success() || response.ret_code == 110043 {
            Ok(true)
        } else {
            warn!("Failed to set leverage: {}", response.ret_msg);
            Ok(false)
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
        let formatted_symbol = format_symbol(symbol);

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

        // Place order via WebSocket trading service
        let order_result = self
            .place_market_order(&formatted_symbol, side, quantity)
            .await?;

        // Set TP/SL if order was successful and we're in swap mode
        if order_result.success && self.market_type == MarketType::Swap {
            let has_tp_sl = tp_percent.is_some() || sl_percent.is_some();
            if has_tp_sl {
                // Calculate TP/SL prices
                let is_long = direction.to_lowercase() == "long";
                let position_idx = if is_long { 1 } else { 2 };

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
                    .set_trading_stop(&formatted_symbol, take_profit, stop_loss, position_idx)
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
        let formatted_symbol = format_symbol(symbol);

        // Use WebSocket trading service for low-latency execution
        self.ws_trading
            .place_order_ws(
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
        let formatted_symbol = format_symbol(symbol);

        // Use WebSocket trading service for low-latency execution
        self.ws_trading
            .place_order_ws(
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
        // Use WebSocket trading service for low-latency cancellation
        self.ws_trading.cancel_order_ws(symbol, order_id).await
    }

    async fn get_order_status(&self, symbol: &str, order_id: &str) -> Result<OrderInfo> {
        let params = format!(
            "category={}&symbol={}&orderId={}",
            self.rest.category(),
            symbol,
            order_id
        );

        let response: BybitResponse<serde_json::Value> = self
            .rest
            .signed_get(endpoints::ORDER_REALTIME, &params)
            .await?;

        if response.ret_code != 0 {
            anyhow::bail!("Failed to get order status: {}", response.ret_msg);
        }

        let result = response
            .result
            .ok_or_else(|| anyhow::anyhow!("No order data in response"))?;

        let list = result
            .get("list")
            .and_then(|l| l.as_array())
            .ok_or_else(|| anyhow::anyhow!("Invalid order list format"))?;

        let order_data = list
            .first()
            .ok_or_else(|| anyhow::anyhow!("Order not found"))?;

        let status_str = order_data
            .get("orderStatus")
            .and_then(|s| s.as_str())
            .unwrap_or("Unknown");

        let status = parse_order_status(status_str);

        let side_str = order_data
            .get("side")
            .and_then(|s| s.as_str())
            .unwrap_or("Buy");
        let side = if side_str == "Sell" {
            OrderSide::Sell
        } else {
            OrderSide::Buy
        };

        let order_type_str = order_data
            .get("orderType")
            .and_then(|s| s.as_str())
            .unwrap_or("Market");
        let order_type = if order_type_str == "Limit" {
            OrderType::Limit
        } else {
            OrderType::Market
        };

        let price = order_data
            .get("price")
            .and_then(|p| p.as_str())
            .and_then(|p| p.parse::<f64>().ok())
            .unwrap_or(0.0);

        let quantity = order_data
            .get("qty")
            .and_then(|q| q.as_str())
            .and_then(|q| q.parse::<f64>().ok())
            .unwrap_or(0.0);

        let filled_quantity = order_data
            .get("cumExecQty")
            .and_then(|q| q.as_str())
            .and_then(|q| q.parse::<f64>().ok())
            .unwrap_or(0.0);

        let avg_price = order_data
            .get("avgPrice")
            .and_then(|p| p.as_str())
            .and_then(|p| p.parse::<f64>().ok());

        let timestamp = order_data
            .get("createdTime")
            .and_then(|t| t.as_str())
            .and_then(|t| t.parse::<i64>().ok())
            .unwrap_or(0);

        Ok(OrderInfo {
            order_id: order_id.to_string(),
            client_order_id: order_data
                .get("orderLinkId")
                .and_then(|s| s.as_str())
                .map(|s| s.to_string()),
            symbol: symbol.to_string(),
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
            reduce_only: order_data
                .get("reduceOnly")
                .and_then(|r| r.as_bool())
                .unwrap_or(false),
        })
    }

    async fn close_position(&self, symbol: &str) -> Result<OrderResult> {
        if self.market_type == MarketType::Spot {
            anyhow::bail!("Close position not supported for spot trading");
        }

        let formatted_symbol = format_symbol(symbol);

        // Get current position
        let position = self.get_position(&formatted_symbol).await?;

        match position {
            Some(pos) => {
                let size: f64 = pos.size.parse().context("Failed to parse position size")?;
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
                self.ws_trading
                    .place_order_ws(
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
        let params = format!(
            "category=linear&symbol={}&limit={}",
            formatted_symbol,
            limit.min(200)
        );

        let response: BybitResponse<FundingHistoryResult> = self
            .rest
            .public_get_with_params(endpoints::FUNDING_HISTORY, &params)
            .await?;

        if let Some(result) = response.result {
            let entries: Vec<FundingRateEntry> = result
                .list
                .iter()
                .filter_map(|e| {
                    let funding_rate: f64 = e.funding_rate.parse().ok()?;
                    let funding_time: i64 = e.funding_rate_timestamp.parse().ok()?;
                    Some(FundingRateEntry {
                        symbol: e.symbol.clone(),
                        funding_rate,
                        funding_time,
                    })
                })
                .collect();
            return Ok(entries);
        }

        anyhow::bail!("Failed to get funding history: {}", response.ret_msg)
    }

    async fn get_open_interest_history(
        &self,
        symbol: &str,
        interval: &str,
        limit: u32,
    ) -> Result<Vec<OpenInterestEntry>> {
        if self.market_type != MarketType::Swap {
            anyhow::bail!("Open interest only available for perpetual futures");
        }

        let formatted_symbol = format_symbol(symbol);
        let params = format!(
            "category=linear&symbol={}&intervalTime={}&limit={}",
            formatted_symbol,
            interval,
            limit.min(200)
        );

        let response: BybitResponse<OpenInterestResult> = self
            .rest
            .public_get_with_params(endpoints::OPEN_INTEREST, &params)
            .await?;

        if let Some(result) = response.result {
            let entries: Vec<OpenInterestEntry> = result
                .list
                .iter()
                .filter_map(|e| {
                    let open_interest: f64 = e.open_interest.parse().ok()?;
                    let timestamp: i64 = e.timestamp.parse().ok()?;
                    Some(OpenInterestEntry {
                        symbol: symbol.to_string(),
                        open_interest,
                        open_interest_value: 0.0, // Not provided in this endpoint
                        timestamp,
                    })
                })
                .collect();
            return Ok(entries);
        }

        anyhow::bail!("Failed to get open interest history: {}", response.ret_msg)
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
        let user_data_service =
            UserDataService::new(self.rest.api_key(), self.rest.api_secret(), self.testnet);

        user_data_service.subscribe_order_updates(sender)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_format_symbol() {
        assert_eq!(format_symbol("BTC"), "BTCUSDT");
        assert_eq!(format_symbol("BTCUSDT"), "BTCUSDT");
        assert_eq!(format_symbol("BTC/USDT"), "BTCUSDT");
        assert_eq!(format_symbol("eth"), "ETHUSDT");
        assert_eq!(format_symbol("ETH-USDT"), "ETHUSDT");
    }

    #[test]
    fn test_trader_creation() {
        let trader = BybitTrader::new("key", "secret", true, MarketType::Swap);
        assert_eq!(trader.exchange_name(), "bybit");
        assert_eq!(trader.market_type(), MarketType::Swap);
    }
}
