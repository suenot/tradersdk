//! Ourbit service-based trader implementation.

use anyhow::Result;
use async_trait::async_trait;
use tokio::sync::broadcast;
use tokio::task::JoinHandle;
use tracing::{debug, error};
use tradersdk_core::models::{
    AccountBalance, ExchangeCapabilities, FundingRateEntry, MarketType, OpenInterestEntry,
    OrderInfo, OrderResult, OrderSide, OrderType, OrderUpdateEvent, Orderbook, OrderbookLevel,
    TickerInfo, TimeInForce, WsTestResult,
};
use tradersdk_core::utils::timestamp_ms;
use tradersdk_core::ExchangeTrader;

use crate::config::endpoints;
use crate::helpers::{
    convert_order_type_from_ourbit, convert_order_type_to_ourbit, convert_side_from_ourbit,
    convert_side_to_ourbit, format_symbol, parse_order_status,
};
use crate::models::{OurbitBalance, OurbitOrder, OurbitPosition, OurbitResponse, OurbitTicker};
use crate::services::RestService;

/// Ourbit exchange trader using service-based architecture.
pub struct OurbitTrader {
    rest_service: RestService,
}

impl OurbitTrader {
    /// Create a new Ourbit trader with service-based architecture.
    pub fn new(api_key: &str, api_secret: &str, _testnet: bool, _market_type: MarketType) -> Self {
        Self {
            rest_service: RestService::new(api_key, api_secret),
        }
    }

    fn fmt_symbol(&self, symbol: &str) -> String {
        format_symbol(symbol)
    }
}

#[async_trait]
impl ExchangeTrader for OurbitTrader {
    fn exchange_name(&self) -> &str {
        "ourbit"
    }

    fn market_type(&self) -> MarketType {
        MarketType::Swap
    }

    fn capabilities(&self) -> ExchangeCapabilities {
        tradersdk_core::exchange_capabilities! {
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
            execute_fast_entry: Supported,
            format_symbol: Supported,
            ws_orderbook: NotSupported,
            ws_trades: NotSupported,
            ws_ticker: NotSupported,
            ws_orders: NotSupported,
            get_funding_rate_history: NotSupported,
            get_open_interest_history: NotSupported,
        }
    }

    async fn test_connection(&self) -> Result<bool> {
        let response: OurbitResponse<Vec<OurbitTicker>> =
            self.rest_service.public_get(endpoints::TICKER).await?;
        Ok(response.success)
    }

    async fn get_server_time(&self) -> Result<i64> {
        Ok(timestamp_ms())
    }

    async fn get_balance(&self) -> Result<AccountBalance> {
        let response: OurbitResponse<Vec<OurbitBalance>> =
            self.rest_service.signed_get(endpoints::ASSETS, "").await?;

        if !response.success {
            anyhow::bail!(
                "Failed to get balance: {}",
                response.message.unwrap_or_default()
            );
        }

        let balances = response.data.unwrap_or_default();
        let usdt = balances
            .iter()
            .find(|b| b.asset == "USDT")
            .map(|b| {
                let balance: f64 = b.balance.parse().unwrap_or(0.0);
                let available: f64 = b.available_balance.parse().unwrap_or(0.0);
                (balance, available)
            })
            .unwrap_or((0.0, 0.0));

        Ok(AccountBalance::simple(usdt.0, usdt.1, "USDT"))
    }

    async fn get_current_price(&self, symbol: &str) -> Result<f64> {
        let formatted = self.fmt_symbol(symbol);

        let response: OurbitResponse<Vec<OurbitTicker>> =
            self.rest_service.public_get(endpoints::TICKER).await?;

        if !response.success {
            anyhow::bail!(
                "Failed to get ticker: {}",
                response.message.unwrap_or_default()
            );
        }

        let tickers = response.data.unwrap_or_default();
        let ticker = tickers
            .iter()
            .find(|t| t.symbol == formatted)
            .ok_or_else(|| anyhow::anyhow!("Ticker not found for {}", formatted))?;

        ticker
            .last_price
            .parse()
            .map_err(|_| anyhow::anyhow!("Failed to parse price"))
    }

    async fn set_leverage(&self, symbol: Option<&str>, leverage: u32) -> Result<bool> {
        let contract = symbol.ok_or_else(|| anyhow::anyhow!("Symbol required for leverage"))?;
        let formatted = self.fmt_symbol(contract);

        let body = serde_json::json!({
            "symbol": formatted,
            "leverage": leverage
        });

        let response: OurbitResponse<serde_json::Value> = self
            .rest_service
            .signed_post(endpoints::LEVERAGE, &body)
            .await?;

        if !response.success {
            error!(
                "Failed to set leverage: {}",
                response.message.unwrap_or_default()
            );
            return Ok(false);
        }

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

        if let Err(e) = self.set_leverage(Some(&formatted), leverage).await {
            error!("Failed to set leverage on Ourbit: {}", e);
        }

        let quantity = usdt_amount / price * leverage as f64;

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
        let ourbit_side = convert_side_to_ourbit(side, false);
        let ourbit_type = convert_order_type_to_ourbit(OrderType::Market);

        let body = serde_json::json!({
            "symbol": formatted,
            "vol": quantity.to_string(),
            "side": ourbit_side,
            "type": ourbit_type,
            "openType": 1
        });

        debug!("Ourbit place market order: {:?}", body);

        let response: OurbitResponse<OurbitOrder> = self
            .rest_service
            .signed_post(endpoints::ORDER_SUBMIT, &body)
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

        let order = response
            .data
            .ok_or_else(|| anyhow::anyhow!("No order data in response"))?;

        Ok(OrderResult {
            success: true,
            order_id: Some(order.order_id),
            symbol: formatted,
            side,
            order_type: OrderType::Market,
            quantity: Some(quantity),
            entry_price: order.avg_price.as_ref().and_then(|p| p.parse().ok()),
            position_size: None,
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
        reduce_only: bool,
    ) -> Result<OrderResult> {
        let formatted = self.fmt_symbol(symbol);
        let ourbit_side = convert_side_to_ourbit(side, reduce_only);
        let ourbit_type = convert_order_type_to_ourbit(OrderType::Limit);

        let body = serde_json::json!({
            "symbol": formatted,
            "vol": quantity.to_string(),
            "price": price.to_string(),
            "side": ourbit_side,
            "type": ourbit_type,
            "openType": 1
        });

        debug!("Ourbit place limit order: {:?}", body);

        let response: OurbitResponse<OurbitOrder> = self
            .rest_service
            .signed_post(endpoints::ORDER_SUBMIT, &body)
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

        let order = response
            .data
            .ok_or_else(|| anyhow::anyhow!("No order data in response"))?;

        Ok(OrderResult {
            success: true,
            order_id: Some(order.order_id),
            symbol: formatted,
            side,
            order_type: OrderType::Limit,
            quantity: Some(quantity),
            entry_price: Some(price),
            position_size: None,
            position_usdt: None,
            error: None,
            raw_response: None,
        })
    }

    async fn cancel_order(&self, _symbol: &str, order_id: &str) -> Result<bool> {
        let body = serde_json::json!([order_id]);

        debug!("Ourbit cancel order: {:?}", body);

        let response: OurbitResponse<serde_json::Value> = self
            .rest_service
            .signed_post(endpoints::ORDER_CANCEL, &body)
            .await?;

        if !response.success {
            error!(
                "Failed to cancel order: {}",
                response.message.unwrap_or_default()
            );
            return Ok(false);
        }

        Ok(true)
    }

    async fn get_order_status(&self, _symbol: &str, order_id: &str) -> Result<OrderInfo> {
        let endpoint = format!("{}/{}", endpoints::ORDER_GET, order_id);

        let response: OurbitResponse<OurbitOrder> =
            self.rest_service.signed_get(&endpoint, "").await?;

        if !response.success {
            anyhow::bail!(
                "Failed to get order: {}",
                response.message.unwrap_or_default()
            );
        }

        let order = response
            .data
            .ok_or_else(|| anyhow::anyhow!("No order data in response"))?;

        let status = parse_order_status(order.state);
        let (side, reduce_only) = convert_side_from_ourbit(order.side);
        let order_type = convert_order_type_from_ourbit(order.order_type);

        let quantity: f64 = order.vol.parse().unwrap_or(0.0);
        let filled_quantity: f64 = order
            .deal_vol
            .as_ref()
            .and_then(|s| s.parse().ok())
            .unwrap_or(0.0);

        Ok(OrderInfo {
            order_id: order.order_id,
            client_order_id: None,
            symbol: order.symbol,
            status,
            side,
            order_type,
            price: order
                .price
                .as_ref()
                .and_then(|s| s.parse().ok())
                .unwrap_or(0.0),
            quantity,
            filled_quantity,
            remaining_quantity: quantity - filled_quantity,
            avg_price: order.avg_price.as_ref().and_then(|s| s.parse().ok()),
            timestamp: timestamp_ms(),
            time_in_force: TimeInForce::GTC,
            reduce_only,
        })
    }

    async fn close_position(&self, symbol: &str) -> Result<OrderResult> {
        let formatted = self.fmt_symbol(symbol);

        let response: OurbitResponse<Vec<OurbitPosition>> = self
            .rest_service
            .signed_get(endpoints::POSITIONS, "")
            .await?;

        if !response.success {
            anyhow::bail!(
                "Failed to get positions: {}",
                response.message.unwrap_or_default()
            );
        }

        let positions = response.data.unwrap_or_default();
        let position = positions.iter().find(|p| p.symbol == formatted);

        if let Some(pos) = position {
            let size: f64 = pos.position_amt.parse().unwrap_or(0.0);

            if size.abs() < 0.000001 {
                return Ok(OrderResult::failure(
                    formatted,
                    OrderSide::Sell,
                    "No position to close".to_string(),
                ));
            }

            let (side, ourbit_side) = if pos.position_type == 1 {
                (OrderSide::Sell, 4)
            } else {
                (OrderSide::Buy, 2)
            };

            let body = serde_json::json!({
                "symbol": formatted,
                "vol": size.abs().to_string(),
                "side": ourbit_side,
                "type": 5,
                "openType": 1
            });

            let response: OurbitResponse<OurbitOrder> = self
                .rest_service
                .signed_post(endpoints::ORDER_SUBMIT, &body)
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

            let order = response
                .data
                .ok_or_else(|| anyhow::anyhow!("No order data in response"))?;

            return Ok(OrderResult {
                success: true,
                order_id: Some(order.order_id),
                symbol: formatted,
                side,
                order_type: OrderType::Market,
                quantity: Some(size.abs()),
                entry_price: order.avg_price.as_ref().and_then(|p| p.parse().ok()),
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

        let endpoint = format!("{}/{}/{}", endpoints::ORDERBOOK, formatted, limit);

        let response: OurbitResponse<serde_json::Value> =
            self.rest_service.public_get(&endpoint).await?;

        if !response.success {
            anyhow::bail!(
                "Failed to get orderbook: {}",
                response.message.unwrap_or_default()
            );
        }

        let data = response.data.unwrap_or_default();

        let parse_level = |level: &serde_json::Value| -> Option<OrderbookLevel> {
            let price: f64 = level[0]
                .as_str()
                .or(level["price"].as_str())?
                .parse()
                .ok()?;
            let qty: f64 = level[1].as_str().or(level["vol"].as_str())?.parse().ok()?;
            Some(OrderbookLevel {
                price,
                quantity: qty,
            })
        };

        let bids: Vec<OrderbookLevel> = data["bids"]
            .as_array()
            .map(|arr| arr.iter().filter_map(parse_level).collect())
            .unwrap_or_default();

        let asks: Vec<OrderbookLevel> = data["asks"]
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
        _symbol: &str,
        _depth: u32,
        _timeout_ms: u64,
    ) -> Result<WsTestResult> {
        anyhow::bail!("Ourbit does not support WebSocket API")
    }

    async fn test_ws_trades(&self, _symbol: &str, _timeout_ms: u64) -> Result<WsTestResult> {
        anyhow::bail!("Ourbit does not support WebSocket API")
    }

    async fn get_ticker(&self, symbol: &str) -> Result<TickerInfo> {
        let formatted = self.fmt_symbol(symbol);

        let response: OurbitResponse<Vec<OurbitTicker>> =
            self.rest_service.public_get(endpoints::TICKER).await?;

        if !response.success {
            anyhow::bail!(
                "Failed to get ticker: {}",
                response.message.unwrap_or_default()
            );
        }

        let tickers = response.data.unwrap_or_default();
        let ticker = tickers
            .iter()
            .find(|t| t.symbol == formatted)
            .ok_or_else(|| anyhow::anyhow!("Ticker not found for {}", formatted))?;

        Ok(TickerInfo {
            symbol: formatted,
            last_price: ticker.last_price.parse().unwrap_or(0.0),
            bid_price: ticker.bid1.as_ref().and_then(|s| s.parse().ok()),
            ask_price: ticker.ask1.as_ref().and_then(|s| s.parse().ok()),
            high_24h: None,
            low_24h: None,
            volume_24h: ticker.volume24.as_ref().and_then(|s| s.parse().ok()),
            turnover_24h: None,
            change_24h: ticker.rise_fall_rate.as_ref().and_then(|s| s.parse().ok()),
            open_interest: None,
            open_interest_value: None,
            funding_rate: None,
            next_funding_time: None,
            mark_price: None,
            index_price: None,
            timestamp: timestamp_ms(),
        })
    }

    async fn test_ws_ticker(&self, _symbol: &str, _timeout_ms: u64) -> Result<WsTestResult> {
        anyhow::bail!("Ourbit does not support WebSocket API")
    }

    async fn get_funding_rate_history(
        &self,
        _symbol: &str,
        _limit: u32,
    ) -> Result<Vec<FundingRateEntry>> {
        anyhow::bail!("Funding rate history not available for Ourbit")
    }

    async fn get_open_interest_history(
        &self,
        _symbol: &str,
        _interval: &str,
        _limit: u32,
    ) -> Result<Vec<OpenInterestEntry>> {
        anyhow::bail!("Open interest history not available for Ourbit")
    }

    async fn subscribe_order_updates(
        &self,
        _sender: broadcast::Sender<OrderUpdateEvent>,
    ) -> Result<JoinHandle<()>> {
        anyhow::bail!("Ourbit does not support WebSocket API for order updates")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_trader_new() {
        let trader = OurbitTrader::new("test_key", "test_secret", false, MarketType::Spot);
        assert_eq!(trader.exchange_name(), "ourbit");
        assert_eq!(trader.market_type(), MarketType::Swap);
    }

    #[test]
    fn test_format_symbol() {
        let trader = OurbitTrader::new("key", "secret", false, MarketType::Swap);
        assert_eq!(trader.format_symbol("BTCUSDT"), "BTC_USDT");
        assert_eq!(trader.format_symbol("BTC"), "BTC_USDT");
        assert_eq!(trader.format_symbol("BTC_USDT"), "BTC_USDT");
        assert_eq!(trader.format_symbol("BTC/USDT"), "BTC_USDT");
    }

    #[test]
    fn test_market_type_always_swap() {
        let trader_spot = OurbitTrader::new("key", "secret", false, MarketType::Spot);
        assert_eq!(trader_spot.market_type(), MarketType::Swap);

        let trader_swap = OurbitTrader::new("key", "secret", false, MarketType::Swap);
        assert_eq!(trader_swap.market_type(), MarketType::Swap);
    }
}
