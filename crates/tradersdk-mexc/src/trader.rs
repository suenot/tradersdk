//! MEXC trader implementation using service-based architecture.
//!
//! This module provides a clean, modular implementation of the MEXC trader
//! that delegates to specialized services for REST API calls, market data
//! streaming, and user data streaming.

use anyhow::Result;
use async_trait::async_trait;
use tokio::sync::broadcast;
use tokio::task::JoinHandle;
use tracing::{error, info};

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
    FuturesBalance, FuturesOrderInfo, FuturesOrderResult, FuturesPosition, FuturesResponse,
    SpotBalanceResponse, SpotOrderResponse,
};
use crate::services::{MarketStreamService, RestService, UserDataService};

/// MEXC exchange trader using service-based architecture.
///
/// This trader owns instances of:
/// - `RestService`: Handles all REST API calls (both spot and futures)
/// - `MarketStreamService`: Handles public WebSocket streams (orderbook, trades, ticker)
/// - `UserDataService`: Handles private WebSocket streams (order updates)
///
/// All ExchangeTrader trait methods are implemented by delegating to the appropriate service.
pub struct MexcTrader {
    rest_service: RestService,
    market_stream: MarketStreamService,
    user_data: UserDataService,
    market_type: MarketType,
}

impl MexcTrader {
    /// Create a new MEXC trader with service-based architecture.
    ///
    /// # Arguments
    /// * `api_key` - MEXC API key
    /// * `api_secret` - MEXC API secret
    /// * `testnet` - Whether to use testnet (ignored - MEXC doesn't have testnet)
    /// * `market_type` - Spot or Swap (futures) market
    ///
    /// # Note
    /// MEXC does not have a testnet environment, so the `testnet` parameter is ignored.
    pub fn new(api_key: &str, api_secret: &str, _testnet: bool, market_type: MarketType) -> Self {
        Self {
            rest_service: RestService::new(api_key, api_secret, market_type),
            market_stream: MarketStreamService::new(market_type),
            user_data: UserDataService::new(api_key, market_type),
            market_type,
        }
    }

    /// Get a reference to the REST service.
    pub fn rest_service(&self) -> &RestService {
        &self.rest_service
    }

    /// Get a reference to the market stream service.
    pub fn market_stream_service(&self) -> &MarketStreamService {
        &self.market_stream
    }

    /// Get a reference to the user data service.
    pub fn user_data_service(&self) -> &UserDataService {
        &self.user_data
    }
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
            endpoints::spot::PING
        } else {
            endpoints::futures::PING
        };

        let response: serde_json::Value = self.rest_service.public_get(endpoint).await?;
        // For spot, ping returns {} on success
        // For futures, ping returns {"success": true, "data": timestamp}
        Ok(response.is_object())
    }

    async fn get_server_time(&self) -> Result<i64> {
        if self.market_type == MarketType::Spot {
            let response: serde_json::Value =
                self.rest_service.public_get(endpoints::spot::TIME).await?;
            Ok(response["serverTime"].as_i64().unwrap_or(timestamp_ms()))
        } else {
            let response: serde_json::Value = self
                .rest_service
                .public_get(endpoints::futures::PING)
                .await?;
            Ok(response["data"].as_i64().unwrap_or(timestamp_ms()))
        }
    }

    async fn get_balance(&self) -> Result<AccountBalance> {
        if self.market_type == MarketType::Spot {
            let response: SpotBalanceResponse = self
                .rest_service
                .signed_get_spot(endpoints::spot::ACCOUNT, "")
                .await?;

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
                .rest_service
                .signed_request_futures("GET", endpoints::futures::ASSETS, "")
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
        let formatted = format_symbol(symbol, self.market_type);

        if self.market_type == MarketType::Spot {
            let endpoint = format!("{}?symbol={}", endpoints::spot::TICKER_PRICE, formatted);
            let response: serde_json::Value = self.rest_service.public_get(&endpoint).await?;

            Ok(response["price"]
                .as_str()
                .and_then(|s| s.parse().ok())
                .unwrap_or(0.0))
        } else {
            let endpoint = format!("{}?symbol={}", endpoints::futures::TICKER, formatted);
            let response: serde_json::Value = self.rest_service.public_get(&endpoint).await?;

            Ok(response["data"]["lastPrice"].as_f64().unwrap_or(0.0))
        }
    }

    async fn get_ticker(&self, symbol: &str) -> Result<TickerInfo> {
        let formatted = format_symbol(symbol, self.market_type);

        if self.market_type == MarketType::Spot {
            let endpoint = format!("{}?symbol={}", endpoints::spot::TICKER_24HR, formatted);
            let response: serde_json::Value = self.rest_service.public_get(&endpoint).await?;

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
        let endpoint = format!("{}?symbol={}", endpoints::futures::TICKER, formatted);
        let response: serde_json::Value = self.rest_service.public_get(&endpoint).await?;

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

    async fn get_orderbook(&self, symbol: &str, depth: u32) -> Result<Orderbook> {
        let formatted = format_symbol(symbol, self.market_type);
        let limit = depth.min(100);

        let response: serde_json::Value = if self.market_type == MarketType::Spot {
            let endpoint = format!(
                "{}?symbol={}&limit={}",
                endpoints::spot::DEPTH,
                formatted,
                limit
            );
            self.rest_service.public_get(&endpoint).await?
        } else {
            let endpoint = format!(
                "{}/{}?limit={}",
                endpoints::futures::DEPTH,
                formatted,
                limit
            );
            self.rest_service.public_get(&endpoint).await?
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

    async fn get_funding_rate_history(
        &self,
        symbol: &str,
        limit: u32,
    ) -> Result<Vec<FundingRateEntry>> {
        if self.market_type != MarketType::Swap {
            anyhow::bail!("Funding rate only available for futures");
        }

        let formatted = format_symbol(symbol, self.market_type);

        let endpoint = format!(
            "{}?symbol={}&page_num=1&page_size={}",
            endpoints::futures::FUNDING_RATE_HISTORY,
            formatted,
            limit.min(100)
        );

        let response: serde_json::Value = self.rest_service.public_get(&endpoint).await?;

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

        let formatted = format_symbol(symbol, self.market_type);

        // MEXC doesn't have historical OI endpoint, return current OI
        let endpoint = format!("{}?symbol={}", endpoints::futures::TICKER, formatted);
        let response: serde_json::Value = self.rest_service.public_get(&endpoint).await?;

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

    async fn set_leverage(&self, symbol: Option<&str>, leverage: u32) -> Result<bool> {
        if self.market_type == MarketType::Spot {
            return Ok(true);
        }

        let contract = symbol.ok_or_else(|| anyhow::anyhow!("Symbol required for leverage"))?;
        let formatted = format_symbol(contract, self.market_type);

        // MEXC futures leverage is set per position side
        let params = format!(
            "symbol={}&leverage={}&openType=1&positionType=1",
            formatted, leverage
        );

        let _: FuturesResponse<serde_json::Value> = self
            .rest_service
            .signed_request_futures("POST", endpoints::futures::CHANGE_LEVERAGE, &params)
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
        let formatted = format_symbol(symbol, self.market_type);
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
        let formatted = format_symbol(symbol, self.market_type);

        if self.market_type == MarketType::Spot {
            let side_str = match side {
                OrderSide::Buy => "BUY",
                OrderSide::Sell => "SELL",
            };

            let params = format!(
                "symbol={}&side={}&type=MARKET&quantity={:.8}",
                formatted, side_str, quantity
            );

            let response: SpotOrderResponse = self
                .rest_service
                .signed_post_spot(endpoints::spot::ORDER, &params)
                .await?;

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
                .rest_service
                .signed_request_futures("POST", endpoints::futures::ORDER_SUBMIT, &params)
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
        let formatted = format_symbol(symbol, self.market_type);

        if self.market_type == MarketType::Spot {
            let side_str = match side {
                OrderSide::Buy => "BUY",
                OrderSide::Sell => "SELL",
            };

            let params = format!(
                "symbol={}&side={}&type=LIMIT&quantity={:.8}&price={:.8}&timeInForce=GTC",
                formatted, side_str, quantity, price
            );

            let response: SpotOrderResponse = self
                .rest_service
                .signed_post_spot(endpoints::spot::ORDER, &params)
                .await?;

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
                .rest_service
                .signed_request_futures("POST", endpoints::futures::ORDER_SUBMIT, &params)
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
        let formatted = format_symbol(symbol, self.market_type);

        if self.market_type == MarketType::Spot {
            let params = format!("symbol={}&orderId={}", formatted, order_id);
            let _: serde_json::Value = self
                .rest_service
                .signed_delete_spot(endpoints::spot::ORDER, &params)
                .await?;
        } else {
            let params = format!("symbol={}&orderId={}", formatted, order_id);
            let _: FuturesResponse<serde_json::Value> = self
                .rest_service
                .signed_request_futures("POST", endpoints::futures::ORDER_CANCEL, &params)
                .await?;
        }

        Ok(true)
    }

    async fn get_order_status(&self, symbol: &str, order_id: &str) -> Result<OrderInfo> {
        let formatted = format_symbol(symbol, self.market_type);

        if self.market_type == MarketType::Spot {
            let params = format!("symbol={}&orderId={}", formatted, order_id);
            let response: SpotOrderResponse = self
                .rest_service
                .signed_get_spot(endpoints::spot::ORDER, &params)
                .await?;

            let status = parse_order_status(response.status.as_deref().unwrap_or("NEW"));
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
                .rest_service
                .signed_request_futures("GET", endpoints::futures::ORDER_GET, &params)
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

        let formatted = format_symbol(symbol, self.market_type);

        // Get current positions
        let params = format!("symbol={}", formatted);
        let response: FuturesResponse<Vec<FuturesPosition>> = self
            .rest_service
            .signed_request_futures("GET", endpoints::futures::POSITIONS, &params)
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
                .rest_service
                .signed_request_futures("POST", endpoints::futures::ORDER_SUBMIT, &params)
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
        format_symbol(base, self.market_type)
    }

    // === WebSocket Methods - Delegated to Services ===

    async fn test_ws_orderbook(
        &self,
        symbol: &str,
        depth: u32,
        timeout_ms: u64,
    ) -> Result<WsTestResult> {
        self.market_stream
            .test_orderbook(symbol, depth, timeout_ms)
            .await
    }

    async fn test_ws_trades(&self, symbol: &str, timeout_ms: u64) -> Result<WsTestResult> {
        self.market_stream.test_trades(symbol, timeout_ms).await
    }

    async fn test_ws_ticker(&self, symbol: &str, timeout_ms: u64) -> Result<WsTestResult> {
        self.market_stream.test_ticker(symbol, timeout_ms).await
    }

    async fn subscribe_order_updates(
        &self,
        sender: broadcast::Sender<OrderUpdateEvent>,
    ) -> Result<JoinHandle<()>> {
        self.user_data.subscribe_order_updates(sender).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new_trader_spot() {
        let trader = MexcTrader::new("key", "secret", false, MarketType::Spot);
        assert_eq!(trader.exchange_name(), "mexc");
        assert_eq!(trader.market_type(), MarketType::Spot);
    }

    #[test]
    fn test_new_trader_futures() {
        let trader = MexcTrader::new("key", "secret", false, MarketType::Swap);
        assert_eq!(trader.exchange_name(), "mexc");
        assert_eq!(trader.market_type(), MarketType::Swap);
    }

    #[test]
    fn test_testnet_ignored() {
        // MEXC doesn't have testnet, so both should be the same
        let trader1 = MexcTrader::new("key", "secret", true, MarketType::Spot);
        let trader2 = MexcTrader::new("key", "secret", false, MarketType::Spot);
        assert_eq!(trader1.exchange_name(), trader2.exchange_name());
    }

    #[test]
    fn test_format_symbol_spot() {
        let trader = MexcTrader::new("key", "secret", false, MarketType::Spot);
        assert_eq!(trader.format_symbol("BTC"), "BTCUSDT");
        assert_eq!(trader.format_symbol("BTCUSDT"), "BTCUSDT");
        assert_eq!(trader.format_symbol("BTC/USDT"), "BTCUSDT");
    }

    #[test]
    fn test_format_symbol_futures() {
        let trader = MexcTrader::new("key", "secret", false, MarketType::Swap);
        assert_eq!(trader.format_symbol("BTC"), "BTC_USDT");
        assert_eq!(trader.format_symbol("BTCUSDT"), "BTC_USDT");
        assert_eq!(trader.format_symbol("BTC_USDT"), "BTC_USDT");
        assert_eq!(trader.format_symbol("BTC/USDT"), "BTC_USDT");
    }

    #[test]
    fn test_service_accessors() {
        let trader = MexcTrader::new("key", "secret", false, MarketType::Spot);
        assert_eq!(trader.rest_service().market_type(), MarketType::Spot);
        // Other services don't expose market_type directly but are still accessible
        let _ = trader.market_stream_service();
        let _ = trader.user_data_service();
    }
}
