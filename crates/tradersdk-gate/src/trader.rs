//! Gate.io service-based trader implementation.
//!
//! This is the preferred implementation using dedicated services for different operations:
//! - `RestService` - REST API operations (balance, prices, orderbook, etc.)
//! - `WsTradingService` - WebSocket trading (orders ~2x faster than REST)
//! - `MarketStreamService` - Public WebSocket market data
//! - `UserDataService` - Private WebSocket order updates

use anyhow::Result;
use async_trait::async_trait;
use tokio::sync::broadcast;
use tokio::task::JoinHandle;
use tracing::error;
use tradersdk_core::models::{
    AccountBalance, ExchangeCapabilities, FundingRateEntry, MarketType, OpenInterestEntry,
    OrderInfo, OrderResult, OrderSide, OrderStatus, OrderType, OrderUpdateEvent, Orderbook,
    OrderbookLevel, TickerInfo, TimeInForce, WsTestResult,
};
use tradersdk_core::utils::timestamp_ms;
use tradersdk_core::ExchangeTrader;

use crate::config::endpoints;
use crate::helpers::{format_symbol, parse_order_status};
use crate::models::{
    FuturesAccountBalance, FuturesOrderResponse, SpotAccountBalance, SpotOrderResponse,
};
use crate::services::{MarketStreamService, RestService, UserDataService, WsTradingService};

/// Gate.io exchange trader using service-based architecture.
///
/// # Architecture
///
/// The trader owns four specialized services:
/// - `rest_service` - For REST API calls (balance, prices, orderbook)
/// - `ws_trading_service` - For fast WebSocket trading (place/cancel orders)
/// - `market_stream_service` - For public market data streams
/// - `user_data_service` - For private order update streams
///
/// # Trading
///
/// By default, order operations use WebSocket (`ws_trading_service`) as it's
/// approximately 2x faster than REST for latency-sensitive trading.
pub struct GateTrader {
    rest_service: RestService,
    ws_trading_service: WsTradingService,
    market_stream_service: MarketStreamService,
    user_data_service: UserDataService,
    market_type: MarketType,
}

impl GateTrader {
    /// Create a new Gate.io trader with service-based architecture.
    ///
    /// # Arguments
    /// * `api_key` - Gate.io API key
    /// * `api_secret` - Gate.io API secret
    /// * `testnet` - Whether to use testnet (currently not supported by Gate.io)
    /// * `market_type` - Spot or Swap (futures)
    pub fn new(api_key: &str, api_secret: &str, _testnet: bool, market_type: MarketType) -> Self {
        Self {
            rest_service: RestService::new(api_key, api_secret, market_type),
            ws_trading_service: WsTradingService::new(api_key, api_secret, market_type),
            market_stream_service: MarketStreamService::new(market_type),
            user_data_service: UserDataService::new(api_key, api_secret, market_type),
            market_type,
        }
    }

    /// Format symbol for Gate.io API.
    /// Both Spot and Futures use BTC_USDT format.
    fn fmt_symbol(&self, symbol: &str) -> String {
        format_symbol(symbol, self.market_type)
    }
}

#[async_trait]
impl ExchangeTrader for GateTrader {
    fn exchange_name(&self) -> &str {
        "gate"
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
            endpoints::spot::TIME
        } else {
            endpoints::futures::CONTRACTS
        };

        let response: serde_json::Value = self.rest_service.public_get(endpoint).await?;
        Ok(!response.is_null())
    }

    async fn get_server_time(&self) -> Result<i64> {
        let response: serde_json::Value =
            self.rest_service.public_get(endpoints::spot::TIME).await?;
        Ok(response["server_time"].as_i64().unwrap_or(timestamp_ms()))
    }

    async fn get_balance(&self) -> Result<AccountBalance> {
        if self.market_type == MarketType::Spot {
            let balances: Vec<SpotAccountBalance> = self
                .rest_service
                .signed_get(endpoints::spot::ACCOUNTS, "")
                .await?;

            let usdt = balances
                .iter()
                .find(|b| b.currency == "USDT")
                .map(|b| {
                    let available: f64 = b.available.parse().unwrap_or(0.0);
                    let locked: f64 = b.locked.parse().unwrap_or(0.0);
                    (available + locked, available)
                })
                .unwrap_or((0.0, 0.0));

            Ok(AccountBalance::simple(usdt.0, usdt.1, "USDT"))
        } else {
            let balances: Vec<FuturesAccountBalance> = self
                .rest_service
                .signed_get(endpoints::futures::ACCOUNTS, "")
                .await?;

            let usdt = balances
                .iter()
                .find(|b| b.currency == "USDT")
                .map(|b| {
                    let total: f64 = b.total.parse().unwrap_or(0.0);
                    let available: f64 = b.available.parse().unwrap_or(0.0);
                    (total, available)
                })
                .unwrap_or((0.0, 0.0));

            Ok(AccountBalance::simple(usdt.0, usdt.1, "USDT"))
        }
    }

    async fn get_current_price(&self, symbol: &str) -> Result<f64> {
        let formatted = self.fmt_symbol(symbol);

        if self.market_type == MarketType::Spot {
            let query = format!("currency_pair={}", formatted);
            let response: Vec<serde_json::Value> = self
                .rest_service
                .public_get_with_query(endpoints::spot::TICKERS, &query)
                .await?;

            Ok(response
                .first()
                .and_then(|t| t["last"].as_str())
                .and_then(|s| s.parse().ok())
                .unwrap_or(0.0))
        } else {
            let query = format!("contract={}", formatted);
            let response: Vec<serde_json::Value> = self
                .rest_service
                .public_get_with_query(endpoints::futures::TICKERS, &query)
                .await?;

            Ok(response
                .first()
                .and_then(|t| t["last"].as_str())
                .and_then(|s| s.parse().ok())
                .unwrap_or(0.0))
        }
    }

    async fn set_leverage(&self, symbol: Option<&str>, leverage: u32) -> Result<bool> {
        if self.market_type == MarketType::Spot {
            return Ok(true);
        }

        let contract = symbol.ok_or_else(|| anyhow::anyhow!("Symbol required for leverage"))?;
        let formatted = self.fmt_symbol(contract);

        let body = serde_json::json!({
            "leverage": leverage.to_string()
        });

        let _response: serde_json::Value = self
            .rest_service
            .signed_post(
                &format!("{}/{}/leverage", endpoints::futures::POSITIONS, formatted),
                &body,
            )
            .await?;

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
        let formatted = self.fmt_symbol(symbol);
        let price = current_price.unwrap_or(self.get_current_price(&formatted).await?);

        if self.market_type == MarketType::Swap {
            if let Err(e) = self.set_leverage(Some(&formatted), leverage).await {
                error!("Failed to set leverage on Gate.io: {}", e);
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

        self.place_market_order(&formatted, side, quantity).await
    }

    async fn place_market_order(
        &self,
        symbol: &str,
        side: OrderSide,
        quantity: f64,
    ) -> Result<OrderResult> {
        let formatted = self.fmt_symbol(symbol);
        // Use WebSocket by default (2x faster for HFT)
        self.ws_trading_service
            .place_market_order(&formatted, side, quantity)
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
        let formatted = self.fmt_symbol(symbol);
        // Use WebSocket by default (2x faster for HFT)
        self.ws_trading_service
            .place_limit_order(&formatted, side, quantity, price, reduce_only)
            .await
    }

    async fn cancel_order(&self, symbol: &str, order_id: &str) -> Result<bool> {
        let formatted = self.fmt_symbol(symbol);
        // Use WebSocket by default (2x faster for HFT)
        self.ws_trading_service
            .cancel_order(&formatted, order_id)
            .await
    }

    async fn get_order_status(&self, symbol: &str, order_id: &str) -> Result<OrderInfo> {
        let formatted = self.fmt_symbol(symbol);

        if self.market_type == MarketType::Spot {
            let query = format!("currency_pair={}", formatted);
            let order: SpotOrderResponse = self
                .rest_service
                .signed_get(&format!("{}/{}", endpoints::spot::ORDERS, order_id), &query)
                .await?;

            let status = parse_order_status(&order.order_type);
            let filled_quantity: f64 = order
                .filled_total
                .as_ref()
                .and_then(|s| s.parse().ok())
                .unwrap_or(0.0);
            let quantity: f64 = order.amount.parse().unwrap_or(0.0);

            Ok(OrderInfo {
                order_id: order.id,
                client_order_id: None,
                symbol: order.currency_pair,
                status,
                side: if order.side == "buy" {
                    OrderSide::Buy
                } else {
                    OrderSide::Sell
                },
                order_type: if order.order_type == "market" {
                    OrderType::Market
                } else {
                    OrderType::Limit
                },
                price: order
                    .price
                    .as_ref()
                    .and_then(|s| s.parse().ok())
                    .unwrap_or(0.0),
                quantity,
                filled_quantity,
                remaining_quantity: quantity - filled_quantity,
                avg_price: order.avg_deal_price.as_ref().and_then(|s| s.parse().ok()),
                timestamp: timestamp_ms(),
                time_in_force: TimeInForce::GTC,
                reduce_only: false,
            })
        } else {
            let order: FuturesOrderResponse = self
                .rest_service
                .signed_get(&format!("{}/{}", endpoints::futures::ORDERS, order_id), "")
                .await?;

            let quantity = order.size.abs() as f64;

            Ok(OrderInfo {
                order_id: order.id.to_string(),
                client_order_id: None,
                symbol: order.contract,
                status: OrderStatus::New,
                side: if order.size > 0 {
                    OrderSide::Buy
                } else {
                    OrderSide::Sell
                },
                order_type: OrderType::Limit,
                price: order
                    .price
                    .as_ref()
                    .and_then(|s| s.parse().ok())
                    .unwrap_or(0.0),
                quantity,
                filled_quantity: 0.0,
                remaining_quantity: quantity,
                avg_price: order.fill_price.as_ref().and_then(|s| s.parse().ok()),
                timestamp: timestamp_ms(),
                time_in_force: TimeInForce::GTC,
                reduce_only: false,
            })
        }
    }

    async fn close_position(&self, symbol: &str) -> Result<OrderResult> {
        if self.market_type == MarketType::Spot {
            anyhow::bail!("Close position not supported for spot trading");
        }

        let formatted = self.fmt_symbol(symbol);

        // Get current position
        let positions: Vec<serde_json::Value> = self
            .rest_service
            .signed_get(endpoints::futures::POSITIONS, "")
            .await?;

        let position = positions
            .iter()
            .find(|p| p["contract"].as_str() == Some(&formatted));

        if let Some(pos) = position {
            let size: i64 = pos["size"].as_i64().unwrap_or(0);

            if size == 0 {
                return Ok(OrderResult::failure(
                    formatted,
                    OrderSide::Sell,
                    "No position to close".to_string(),
                ));
            }

            // Close position by placing opposite order
            let close_size = -size;

            let body = serde_json::json!({
                "contract": formatted,
                "size": close_size,
                "price": "0",
                "tif": "ioc",
                "reduce_only": true
            });

            let response: FuturesOrderResponse = self
                .rest_service
                .signed_post(endpoints::futures::ORDERS, &body)
                .await?;

            let side = if size > 0 {
                OrderSide::Sell
            } else {
                OrderSide::Buy
            };

            return Ok(OrderResult {
                success: true,
                order_id: Some(response.id.to_string()),
                symbol: formatted,
                side,
                order_type: OrderType::Market,
                quantity: Some(size.abs() as f64),
                entry_price: response.fill_price.as_ref().and_then(|p| p.parse().ok()),
                position_size: Some(0.0),
                position_usdt: None,
                error: None,
                raw_response: None,
            });
        }

        Ok(OrderResult::failure(
            formatted,
            OrderSide::Sell,
            "Position not found".to_string(),
        ))
    }

    fn format_symbol(&self, base: &str) -> String {
        self.fmt_symbol(base)
    }

    async fn get_orderbook(&self, symbol: &str, depth: u32) -> Result<Orderbook> {
        let formatted = self.fmt_symbol(symbol);
        let limit = depth.min(100);

        let response: serde_json::Value = if self.market_type == MarketType::Spot {
            let query = format!("currency_pair={}&limit={}", formatted, limit);
            self.rest_service
                .public_get_with_query(endpoints::spot::ORDER_BOOK, &query)
                .await?
        } else {
            let query = format!("contract={}&limit={}", formatted, limit);
            self.rest_service
                .public_get_with_query(endpoints::futures::ORDER_BOOK, &query)
                .await?
        };

        let parse_level = |level: &serde_json::Value| -> Option<OrderbookLevel> {
            let price: f64 = level["p"].as_str().or(level[0].as_str())?.parse().ok()?;
            let qty: f64 = level["s"].as_str().or(level[1].as_str())?.parse().ok()?;
            Some(OrderbookLevel {
                price,
                quantity: qty,
            })
        };

        let bids: Vec<OrderbookLevel> = response["bids"]
            .as_array()
            .map(|arr| arr.iter().filter_map(parse_level).collect())
            .unwrap_or_default();

        let asks: Vec<OrderbookLevel> = response["asks"]
            .as_array()
            .map(|arr| arr.iter().filter_map(parse_level).collect())
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
        depth: u32,
        timeout_ms: u64,
    ) -> Result<WsTestResult> {
        self.market_stream_service
            .test_ws_orderbook(symbol, depth, timeout_ms)
            .await
    }

    async fn test_ws_trades(&self, symbol: &str, timeout_ms: u64) -> Result<WsTestResult> {
        self.market_stream_service
            .test_ws_trades(symbol, timeout_ms)
            .await
    }

    async fn get_ticker(&self, symbol: &str) -> Result<TickerInfo> {
        let formatted = self.fmt_symbol(symbol);

        if self.market_type == MarketType::Spot {
            let query = format!("currency_pair={}", formatted);
            let response: Vec<serde_json::Value> = self
                .rest_service
                .public_get_with_query(endpoints::spot::TICKERS, &query)
                .await?;

            let ticker = response
                .first()
                .ok_or_else(|| anyhow::anyhow!("Ticker not found"))?;

            return Ok(TickerInfo {
                symbol: formatted,
                last_price: ticker["last"]
                    .as_str()
                    .and_then(|s| s.parse().ok())
                    .unwrap_or(0.0),
                bid_price: ticker["highest_bid"].as_str().and_then(|s| s.parse().ok()),
                ask_price: ticker["lowest_ask"].as_str().and_then(|s| s.parse().ok()),
                high_24h: ticker["high_24h"].as_str().and_then(|s| s.parse().ok()),
                low_24h: ticker["low_24h"].as_str().and_then(|s| s.parse().ok()),
                volume_24h: ticker["base_volume"].as_str().and_then(|s| s.parse().ok()),
                turnover_24h: ticker["quote_volume"].as_str().and_then(|s| s.parse().ok()),
                change_24h: ticker["change_percentage"]
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
        let query = format!("contract={}", formatted);
        let response: Vec<serde_json::Value> = self
            .rest_service
            .public_get_with_query(endpoints::futures::TICKERS, &query)
            .await?;

        let ticker = response
            .first()
            .ok_or_else(|| anyhow::anyhow!("Ticker not found"))?;

        Ok(TickerInfo {
            symbol: formatted,
            last_price: ticker["last"]
                .as_str()
                .and_then(|s| s.parse().ok())
                .unwrap_or(0.0),
            bid_price: ticker["highest_bid"].as_str().and_then(|s| s.parse().ok()),
            ask_price: ticker["lowest_ask"].as_str().and_then(|s| s.parse().ok()),
            high_24h: ticker["high_24h"].as_str().and_then(|s| s.parse().ok()),
            low_24h: ticker["low_24h"].as_str().and_then(|s| s.parse().ok()),
            volume_24h: ticker["volume_24h"].as_str().and_then(|s| s.parse().ok()),
            turnover_24h: ticker["volume_24h_usd"]
                .as_str()
                .and_then(|s| s.parse().ok()),
            change_24h: ticker["change_percentage"]
                .as_str()
                .and_then(|s| s.parse().ok()),
            open_interest: ticker["total_size"].as_str().and_then(|s| s.parse().ok()),
            open_interest_value: None,
            funding_rate: ticker["funding_rate"].as_str().and_then(|s| s.parse().ok()),
            next_funding_time: ticker["funding_next_apply"].as_i64().map(|t| t * 1000),
            mark_price: ticker["mark_price"].as_str().and_then(|s| s.parse().ok()),
            index_price: ticker["index_price"].as_str().and_then(|s| s.parse().ok()),
            timestamp: timestamp_ms(),
        })
    }

    async fn test_ws_ticker(&self, symbol: &str, timeout_ms: u64) -> Result<WsTestResult> {
        self.market_stream_service
            .test_ws_ticker(symbol, timeout_ms)
            .await
    }

    async fn get_funding_rate_history(
        &self,
        symbol: &str,
        limit: u32,
    ) -> Result<Vec<FundingRateEntry>> {
        if self.market_type != MarketType::Swap {
            anyhow::bail!("Funding rate only available for futures");
        }

        let formatted = self.fmt_symbol(symbol);
        let query = format!("contract={}&limit={}", formatted, limit.min(1000));

        let response: Vec<serde_json::Value> = self
            .rest_service
            .public_get_with_query(endpoints::futures::FUNDING_RATE, &query)
            .await?;

        let entries: Vec<FundingRateEntry> = response
            .iter()
            .filter_map(|e| {
                Some(FundingRateEntry {
                    symbol: formatted.clone(),
                    funding_rate: e["r"].as_str()?.parse().ok()?,
                    funding_time: e["t"].as_i64()? * 1000,
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
            anyhow::bail!("Open interest only available for futures");
        }

        let formatted = self.fmt_symbol(symbol);

        // Gate.io doesn't have historical OI endpoint, return current OI
        let query = format!("contract={}", formatted);
        let response: Vec<serde_json::Value> = self
            .rest_service
            .public_get_with_query(endpoints::futures::TICKERS, &query)
            .await?;

        let ticker = response
            .first()
            .ok_or_else(|| anyhow::anyhow!("Ticker not found"))?;

        let oi: f64 = ticker["total_size"]
            .as_str()
            .and_then(|s| s.parse().ok())
            .unwrap_or(0.0);
        let price: f64 = ticker["last"]
            .as_str()
            .and_then(|s| s.parse().ok())
            .unwrap_or(0.0);

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
        self.user_data_service.subscribe_order_updates(sender).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_trader_new() {
        let trader = GateTrader::new("test_key", "test_secret", false, MarketType::Spot);
        assert_eq!(trader.exchange_name(), "gate");
        assert_eq!(trader.market_type(), MarketType::Spot);
    }

    #[test]
    fn test_format_symbol() {
        let trader = GateTrader::new("key", "secret", false, MarketType::Spot);
        assert_eq!(trader.format_symbol("BTCUSDT"), "BTC_USDT");
        assert_eq!(trader.format_symbol("BTC"), "BTC_USDT");
        assert_eq!(trader.format_symbol("BTC_USDT"), "BTC_USDT");
        assert_eq!(trader.format_symbol("BTC/USDT"), "BTC_USDT");
    }

    #[test]
    fn test_format_symbol_futures() {
        let trader = GateTrader::new("key", "secret", false, MarketType::Swap);
        assert_eq!(trader.format_symbol("BTCUSDT"), "BTC_USDT");
        assert_eq!(trader.format_symbol("ETH"), "ETH_USDT");
    }
}
