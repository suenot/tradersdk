//! Binance exchange trader implementation.

use anyhow::{Context, Result};
use async_trait::async_trait;
use tokio::sync::broadcast;
use tokio::task::JoinHandle;
use tradersdk_core::models::{
    AccountBalance, ExchangeCapabilities, FundingRateEntry, MarketType, OpenInterestEntry,
    OrderInfo, OrderResult, OrderSide, OrderStatus, OrderType, OrderUpdateEvent, Orderbook,
    OrderbookLevel, TickerInfo, TimeInForce, WsTestResult,
};
use tradersdk_core::utils::timestamp_ms;
use tradersdk_core::ExchangeTrader;

use crate::config::endpoints;
use crate::helpers::{format_symbol, parse_order_status};
use crate::models::{OrderResponse, ServerTimeResponse, TickerPriceResponse};
use crate::services::{MarketStreamService, RestService, UserDataService, WsTradingService};

/// Binance exchange trader.
pub struct BinanceTrader {
    rest: RestService,
    ws_trading: WsTradingService,
    ws_stream: MarketStreamService,
    testnet: bool,
    market_type: MarketType,
}

impl BinanceTrader {
    /// Create a new Binance trader.
    pub fn new(api_key: &str, api_secret: &str, testnet: bool, market_type: MarketType) -> Self {
        Self {
            rest: RestService::new(api_key, api_secret, testnet, market_type),
            ws_trading: WsTradingService::new(api_key, api_secret, testnet, market_type),
            ws_stream: MarketStreamService::new(testnet, market_type),
            testnet,
            market_type,
        }
    }
}

#[async_trait]
impl ExchangeTrader for BinanceTrader {
    fn exchange_name(&self) -> &str {
        "binance"
    }

    fn market_type(&self) -> MarketType {
        self.market_type
    }

    fn capabilities(&self) -> ExchangeCapabilities {
        tradersdk_core::exchange_capabilities! {
            spot: Supported,
            swap: Supported,
            futures: Supported,
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
        let endpoint = self.rest.ping_endpoint();
        let response = self
            .rest
            .client()
            .get(format!("{}{}", self.rest.base_url(), endpoint))
            .send()
            .await?;

        Ok(response.status().is_success())
    }

    async fn get_server_time(&self) -> Result<i64> {
        let endpoint = self.rest.time_endpoint();
        let response: ServerTimeResponse = self.rest.public_get(endpoint).await?;
        Ok(response.server_time)
    }

    async fn get_balance(&self) -> Result<AccountBalance> {
        let endpoint = self.rest.balance_endpoint();
        let response: serde_json::Value = self.rest.signed_get(endpoint, "").await?;

        // Parse balance based on market type
        let available = if self.market_type == MarketType::Spot {
            // For spot, look for USDT in balances array
            response["balances"]
                .as_array()
                .and_then(|arr| arr.iter().find(|b| b["asset"] == "USDT"))
                .and_then(|b| b["free"].as_str())
                .and_then(|s| s.parse::<f64>().ok())
                .unwrap_or(0.0)
        } else {
            // For futures, look for USDT in balance array
            response
                .as_array()
                .and_then(|arr| arr.iter().find(|b| b["asset"] == "USDT"))
                .and_then(|b| b["availableBalance"].as_str())
                .and_then(|s| s.parse::<f64>().ok())
                .unwrap_or(0.0)
        };

        Ok(AccountBalance {
            total_balance: available,
            available_balance: available,
            currency: "USDT".to_string(),
            margin_balance: None,
            used_margin: None,
            unrealized_pnl: None,
            realized_pnl: None,
            balances: vec![],
        })
    }

    async fn get_current_price(&self, symbol: &str) -> Result<f64> {
        let endpoint = self.rest.ticker_price_endpoint();
        let params = format!("symbol={}", symbol);
        let response: TickerPriceResponse =
            self.rest.public_get_with_params(endpoint, &params).await?;

        response.price.parse().context("Failed to parse price")
    }

    async fn get_ticker(&self, symbol: &str) -> Result<TickerInfo> {
        if self.market_type == MarketType::Spot {
            // Spot ticker
            let endpoint = self.rest.ticker_24hr_endpoint();
            let params = format!("symbol={}", symbol);
            let response: serde_json::Value =
                self.rest.public_get_with_params(endpoint, &params).await?;

            return Ok(TickerInfo {
                symbol: symbol.to_string(),
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

        // Futures - need multiple endpoints
        let base_url = self.rest.base_url();

        let ticker: serde_json::Value = self
            .rest
            .client()
            .get(format!(
                "{}{}?symbol={}",
                base_url,
                endpoints::FUTURES_TICKER_24HR,
                symbol
            ))
            .send()
            .await?
            .json()
            .await?;

        let premium: serde_json::Value = self
            .rest
            .client()
            .get(format!(
                "{}{}?symbol={}",
                base_url,
                endpoints::FUTURES_PREMIUM_INDEX,
                symbol
            ))
            .send()
            .await?
            .json()
            .await?;

        let oi: serde_json::Value = self
            .rest
            .client()
            .get(format!(
                "{}{}?symbol={}",
                base_url,
                endpoints::FUTURES_OPEN_INTEREST,
                symbol
            ))
            .send()
            .await?
            .json()
            .await?;

        Ok(TickerInfo {
            symbol: symbol.to_string(),
            last_price: ticker["lastPrice"]
                .as_str()
                .and_then(|s| s.parse().ok())
                .unwrap_or(0.0),
            bid_price: ticker["bidPrice"].as_str().and_then(|s| s.parse().ok()),
            ask_price: ticker["askPrice"].as_str().and_then(|s| s.parse().ok()),
            high_24h: ticker["highPrice"].as_str().and_then(|s| s.parse().ok()),
            low_24h: ticker["lowPrice"].as_str().and_then(|s| s.parse().ok()),
            volume_24h: ticker["volume"].as_str().and_then(|s| s.parse().ok()),
            turnover_24h: ticker["quoteVolume"].as_str().and_then(|s| s.parse().ok()),
            change_24h: ticker["priceChangePercent"]
                .as_str()
                .and_then(|s| s.parse().ok()),
            open_interest: oi["openInterest"].as_str().and_then(|s| s.parse().ok()),
            open_interest_value: None,
            funding_rate: premium["lastFundingRate"]
                .as_str()
                .and_then(|s| s.parse().ok()),
            next_funding_time: premium["nextFundingTime"].as_i64(),
            mark_price: premium["markPrice"].as_str().and_then(|s| s.parse().ok()),
            index_price: premium["indexPrice"].as_str().and_then(|s| s.parse().ok()),
            timestamp: timestamp_ms(),
        })
    }

    async fn get_orderbook(&self, symbol: &str, depth: u32) -> Result<Orderbook> {
        let endpoint = self.rest.depth_endpoint();
        let limit = depth.min(1000);
        let params = format!("symbol={}&limit={}", symbol, limit);
        let response: serde_json::Value =
            self.rest.public_get_with_params(endpoint, &params).await?;

        let bids: Vec<OrderbookLevel> = response["bids"]
            .as_array()
            .map(|arr| {
                arr.iter()
                    .filter_map(|level| {
                        let price = level[0].as_str()?.parse().ok()?;
                        let qty = level[1].as_str()?.parse().ok()?;
                        Some(OrderbookLevel {
                            price,
                            quantity: qty,
                        })
                    })
                    .collect()
            })
            .unwrap_or_default();

        let asks: Vec<OrderbookLevel> = response["asks"]
            .as_array()
            .map(|arr| {
                arr.iter()
                    .filter_map(|level| {
                        let price = level[0].as_str()?.parse().ok()?;
                        let qty = level[1].as_str()?.parse().ok()?;
                        Some(OrderbookLevel {
                            price,
                            quantity: qty,
                        })
                    })
                    .collect()
            })
            .unwrap_or_default();

        Ok(Orderbook {
            symbol: symbol.to_string(),
            bids,
            asks,
            timestamp: response["T"].as_i64().unwrap_or_else(timestamp_ms),
        })
    }

    async fn set_leverage(&self, symbol: Option<&str>, leverage: u32) -> Result<bool> {
        if self.market_type == MarketType::Spot {
            return Ok(true);
        }

        let symbol = symbol.ok_or_else(|| anyhow::anyhow!("Symbol required for leverage"))?;
        let params = format!("symbol={}&leverage={}", symbol, leverage);

        let _response: serde_json::Value = self
            .rest
            .signed_post(endpoints::FUTURES_LEVERAGE, &params)
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
            if let Err(e) = self.set_leverage(Some(&formatted_symbol), leverage).await {
                tracing::error!("Failed to set leverage: {}", e);
            }
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

        // Use WebSocket by default
        self.ws_trading
            .place_order(
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

        // Use WebSocket by default
        self.ws_trading
            .place_order(
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
        // Use WebSocket by default
        self.ws_trading.cancel_order(symbol, order_id).await
    }

    async fn get_order_status(&self, symbol: &str, order_id: &str) -> Result<OrderInfo> {
        let endpoint = self.rest.order_endpoint();
        let params = format!("symbol={}&orderId={}", symbol, order_id);
        let response: OrderResponse = self.rest.signed_get(endpoint, &params).await?;

        let status = response
            .status
            .as_ref()
            .map(|s| parse_order_status(s))
            .unwrap_or(OrderStatus::New);

        let side = match response.side.as_deref() {
            Some("BUY") => OrderSide::Buy,
            _ => OrderSide::Sell,
        };

        let order_type = match response.order_type.as_deref() {
            Some("LIMIT") => OrderType::Limit,
            _ => OrderType::Market,
        };

        let quantity: f64 = response
            .orig_qty
            .as_ref()
            .and_then(|s| s.parse().ok())
            .unwrap_or(0.0);

        let filled_quantity: f64 = response
            .executed_qty
            .as_ref()
            .and_then(|s| s.parse().ok())
            .unwrap_or(0.0);

        let price: f64 = response
            .price
            .as_ref()
            .and_then(|s| s.parse().ok())
            .unwrap_or(0.0);

        let avg_price: Option<f64> = response.avg_price.as_ref().and_then(|s| s.parse().ok());

        Ok(OrderInfo {
            order_id: response.order_id.to_string(),
            client_order_id: None,
            symbol: response.symbol,
            status,
            side,
            order_type,
            price,
            quantity,
            filled_quantity,
            remaining_quantity: quantity - filled_quantity,
            avg_price,
            timestamp: response.time.unwrap_or_else(timestamp_ms),
            time_in_force: TimeInForce::GTC,
            reduce_only: false,
        })
    }

    async fn close_position(&self, symbol: &str) -> Result<OrderResult> {
        if self.market_type == MarketType::Spot {
            anyhow::bail!("Close position not supported for spot trading");
        }

        // Get position info first
        let params = format!("symbol={}", symbol);
        let positions: Vec<serde_json::Value> = self
            .rest
            .signed_get(endpoints::FUTURES_POSITION_RISK, &params)
            .await?;

        if let Some(position) = positions.first() {
            let qty: f64 = position["positionAmt"]
                .as_str()
                .and_then(|s| s.parse().ok())
                .unwrap_or(0.0);

            if qty.abs() < 0.0001 {
                return Ok(OrderResult::failure(
                    symbol.to_string(),
                    OrderSide::Sell,
                    "No position to close".to_string(),
                ));
            }

            let side = if qty > 0.0 {
                OrderSide::Sell
            } else {
                OrderSide::Buy
            };

            return self.place_market_order(symbol, side, qty.abs()).await;
        }

        Ok(OrderResult::failure(
            symbol.to_string(),
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

        let params = format!("symbol={}&limit={}", symbol, limit.min(1000));
        let response: Vec<serde_json::Value> = self
            .rest
            .public_get_with_params(endpoints::FUTURES_FUNDING_RATE, &params)
            .await?;

        let entries: Vec<FundingRateEntry> = response
            .iter()
            .filter_map(|e| {
                Some(FundingRateEntry {
                    symbol: e["symbol"].as_str()?.to_string(),
                    funding_rate: e["fundingRate"].as_str()?.parse().ok()?,
                    funding_time: e["fundingTime"].as_i64()?,
                })
            })
            .collect();

        Ok(entries)
    }

    async fn get_open_interest_history(
        &self,
        symbol: &str,
        interval: &str,
        limit: u32,
    ) -> Result<Vec<OpenInterestEntry>> {
        if self.market_type != MarketType::Swap {
            anyhow::bail!("Open interest only available for futures");
        }

        let params = format!(
            "symbol={}&period={}&limit={}",
            symbol,
            interval,
            limit.min(500)
        );
        let response: Vec<serde_json::Value> = self
            .rest
            .public_get_with_params(endpoints::FUTURES_OPEN_INTEREST_HIST, &params)
            .await?;

        let entries: Vec<OpenInterestEntry> = response
            .iter()
            .filter_map(|e| {
                Some(OpenInterestEntry {
                    symbol: e["symbol"].as_str()?.to_string(),
                    open_interest: e["sumOpenInterest"].as_str()?.parse().ok()?,
                    open_interest_value: e["sumOpenInterestValue"]
                        .as_str()?
                        .parse()
                        .ok()
                        .unwrap_or(0.0),
                    timestamp: e["timestamp"].as_i64()?,
                })
            })
            .collect();

        Ok(entries)
    }

    async fn test_ws_orderbook(
        &self,
        symbol: &str,
        depth: u32,
        timeout_ms: u64,
    ) -> Result<WsTestResult> {
        self.ws_stream
            .test_orderbook(symbol, depth, timeout_ms)
            .await
    }

    async fn test_ws_trades(&self, symbol: &str, timeout_ms: u64) -> Result<WsTestResult> {
        self.ws_stream.test_trades(symbol, timeout_ms).await
    }

    async fn test_ws_ticker(&self, symbol: &str, timeout_ms: u64) -> Result<WsTestResult> {
        self.ws_stream.test_ticker(symbol, timeout_ms).await
    }

    async fn subscribe_order_updates(
        &self,
        sender: broadcast::Sender<OrderUpdateEvent>,
    ) -> Result<JoinHandle<()>> {
        let user_data_service = UserDataService::new(
            self.rest.api_key(),
            self.rest.base_url(),
            self.testnet,
            self.market_type,
        );

        Ok(user_data_service.subscribe(sender))
    }
}
