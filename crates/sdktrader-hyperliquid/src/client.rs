//! Hyperliquid exchange trader implementation (legacy monolithic client).
//!
//! This is the legacy implementation kept for backward compatibility.
//! Prefer using the service-based `HyperliquidTrader` from `trader.rs` instead.

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
use tokio::sync::broadcast;
use tokio::task::JoinHandle;
use tokio_tungstenite::{connect_async, tungstenite::protocol::Message};
use tracing::{debug, error, info, warn};

#[cfg(feature = "signing")]
use ethers::signers::{LocalWallet, Signer};
#[cfg(feature = "signing")]
use ethers::types::H256;

use crate::config::{
    MAINNET_URL, TESTNET_URL, MAINNET_WS_URL, TESTNET_WS_URL,
    INITIAL_RECONNECT_DELAY_SECS, MAX_RECONNECT_DELAY_SECS, FUNDING_INTERVAL_MS,
};

/// Calculate next reconnect delay with exponential backoff (capped).
fn next_reconnect_delay(current_secs: u64) -> u64 {
    (current_secs * 2).min(MAX_RECONNECT_DELAY_SECS)
}

/// Parse a status string into OrderStatus.
fn parse_order_status_str(s: &str) -> OrderStatus {
    match s.to_lowercase().as_str() {
        "new" | "open" | "live" | "created" | "init" | "accepted" => OrderStatus::New,
        "partially_filled" | "partiallyfilled" | "partial_filled" | "partial" => {
            OrderStatus::PartiallyFilled
        }
        "filled" | "closed" | "full_fill" | "full" => OrderStatus::Filled,
        "cancelled" | "canceled" | "cancel" | "deactivated" => OrderStatus::Cancelled,
        "rejected" | "fail" | "failed" => OrderStatus::Rejected,
        "expired" | "expire" => OrderStatus::Expired,
        _ => {
            warn!("Unknown order status string: '{}'", s);
            OrderStatus::New
        }
    }
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

/// Asset info with index for order placement.
#[derive(Debug, Clone)]
struct AssetInfo {
    name: String,
    index: i32,
    sz_decimals: i32,
}

/// Hyperliquid exchange trader (legacy monolithic implementation).
///
/// Uses wallet address as api_key and private key as api_secret.
pub struct HyperliquidTrader {
    wallet_address: String,
    private_key: String,
    testnet: bool,
    market_type: MarketType,
    client: Client,
    base_url: String,
    asset_cache: std::sync::RwLock<Option<Vec<AssetInfo>>>,
}

impl HyperliquidTrader {
    pub fn new(api_key: &str, api_secret: &str, testnet: bool, market_type: MarketType) -> Self {
        let base_url = if testnet { TESTNET_URL } else { MAINNET_URL };
        let private_key = api_secret
            .strip_prefix("0x")
            .unwrap_or(api_secret)
            .to_string();

        Self {
            wallet_address: api_key.to_string(),
            private_key,
            testnet,
            market_type,
            client: Client::new(),
            base_url: base_url.to_string(),
            asset_cache: std::sync::RwLock::new(None),
        }
    }

    fn ws_url(&self) -> &str {
        if self.testnet { TESTNET_WS_URL } else { MAINNET_WS_URL }
    }

    fn to_hl_symbol(&self, symbol: &str) -> String {
        let upper = symbol.to_uppercase();
        if upper.contains('/') {
            return upper.split('/').next().unwrap_or(&upper).to_string();
        }
        upper
            .strip_suffix("USDT")
            .or_else(|| upper.strip_suffix("USD"))
            .or_else(|| upper.strip_suffix("PERP"))
            .unwrap_or(&upper)
            .to_string()
    }

    async fn ensure_asset_cache(&self) -> Result<()> {
        {
            let cache = self.asset_cache.read().unwrap();
            if cache.is_some() { return Ok(()); }
        }
        let response: serde_json::Value = self.client
            .post(format!("{}/info", self.base_url))
            .json(&serde_json::json!({"type": "meta"}))
            .send().await?.json().await?;
        let universe = response["universe"].as_array()
            .ok_or_else(|| anyhow::anyhow!("Invalid meta response: missing universe"))?;
        let assets: Vec<AssetInfo> = universe.iter().enumerate()
            .filter_map(|(i, asset)| {
                let name = asset["name"].as_str()?.to_string();
                let sz_decimals = asset["szDecimals"].as_i64()? as i32;
                Some(AssetInfo { name, index: i as i32, sz_decimals })
            }).collect();
        let mut cache = self.asset_cache.write().unwrap();
        *cache = Some(assets);
        Ok(())
    }

    async fn get_asset_index(&self, symbol: &str) -> Result<i32> {
        self.ensure_asset_cache().await?;
        let hl_symbol = self.to_hl_symbol(symbol);
        let cache = self.asset_cache.read().unwrap();
        if let Some(assets) = cache.as_ref() {
            for asset in assets {
                if asset.name == hl_symbol { return Ok(asset.index); }
            }
        }
        Err(anyhow::anyhow!("Asset not found: {} (tried {})", symbol, hl_symbol))
    }

    async fn get_sz_decimals(&self, symbol: &str) -> Result<i32> {
        self.ensure_asset_cache().await?;
        let hl_symbol = self.to_hl_symbol(symbol);
        let cache = self.asset_cache.read().unwrap();
        if let Some(assets) = cache.as_ref() {
            for asset in assets {
                if asset.name == hl_symbol { return Ok(asset.sz_decimals); }
            }
        }
        Ok(4)
    }

    fn format_size(&self, size: f64, decimals: i32) -> String {
        let factor = 10_f64.powi(decimals);
        let rounded = (size * factor).round() / factor;
        format!("{:.prec$}", rounded, prec = decimals as usize)
    }

    fn format_price(&self, price: f64) -> String {
        if price >= 1.0 {
            let digits = (price.log10().floor() as i32) + 1;
            let decimals = (5 - digits).max(0);
            format!("{:.prec$}", price, prec = decimals as usize)
        } else if price > 0.0 {
            let log_val = -price.log10().floor() as i32;
            let decimals = log_val + 5;
            format!("{:.prec$}", price, prec = decimals as usize)
        } else {
            "0".to_string()
        }
    }

    #[cfg(feature = "signing")]
    async fn sign_l1_action(
        &self, action: &serde_json::Value, nonce: i64, vault_address: Option<&str>,
    ) -> Result<serde_json::Value> {
        use ethers::core::utils::keccak256;
        let action_bytes = serde_json::to_vec(action)?;
        let mut data_to_hash = action_bytes.clone();
        let nonce_bytes = nonce.to_be_bytes();
        data_to_hash.extend_from_slice(&[0u8; 5]);
        data_to_hash.extend_from_slice(&nonce_bytes);
        if let Some(vault) = vault_address {
            data_to_hash.push(0x01);
            let vault_bytes = hex::decode(vault.strip_prefix("0x").unwrap_or(vault))?;
            data_to_hash.extend_from_slice(&vault_bytes);
        } else {
            data_to_hash.push(0x00);
        }
        let action_hash = keccak256(&data_to_hash);
        let source = if self.testnet { "b" } else { "a" };
        let wallet: LocalWallet = self.private_key.parse::<LocalWallet>().context("Invalid private key")?;
        let domain_type_hash = keccak256(b"EIP712Domain(string name,string version,uint256 chainId,address verifyingContract)");
        let name_hash = keccak256(b"Exchange");
        let version_hash = keccak256(b"1");
        let mut domain_data = Vec::new();
        domain_data.extend_from_slice(&domain_type_hash);
        domain_data.extend_from_slice(&name_hash);
        domain_data.extend_from_slice(&version_hash);
        domain_data.extend_from_slice(&[0u8; 24]);
        domain_data.extend_from_slice(&1337u64.to_be_bytes());
        domain_data.extend_from_slice(&[0u8; 32]);
        let domain_separator = keccak256(&domain_data);
        let type_hash = keccak256(b"Agent(string source,bytes32 connectionId)");
        let mut struct_data = Vec::new();
        struct_data.extend_from_slice(&type_hash);
        struct_data.extend_from_slice(&keccak256(source.as_bytes()));
        struct_data.extend_from_slice(&action_hash);
        let struct_hash = keccak256(&struct_data);
        let mut message_data = Vec::new();
        message_data.extend_from_slice(&[0x19, 0x01]);
        message_data.extend_from_slice(&domain_separator);
        message_data.extend_from_slice(&struct_hash);
        let message_hash = keccak256(&message_data);
        let signature = wallet.sign_hash(H256::from(message_hash))?;
        let mut r_bytes = [0u8; 32];
        let mut s_bytes = [0u8; 32];
        signature.r.to_big_endian(&mut r_bytes);
        signature.s.to_big_endian(&mut s_bytes);
        Ok(serde_json::json!({
            "r": format!("0x{}", hex::encode(r_bytes)),
            "s": format!("0x{}", hex::encode(s_bytes)),
            "v": signature.v as u8
        }))
    }

    #[cfg(not(feature = "signing"))]
    async fn sign_l1_action(
        &self, _action: &serde_json::Value, _nonce: i64, _vault_address: Option<&str>,
    ) -> Result<serde_json::Value> {
        Err(anyhow::anyhow!(
            "Hyperliquid trading requires the 'signing' feature flag. \
             Enable it with: cargo build --features signing"
        ))
    }

    async fn place_order_internal(
        &self, symbol: &str, side: OrderSide, order_type: OrderType,
        quantity: f64, price: Option<f64>, reduce_only: bool,
    ) -> Result<OrderResult> {
        let asset_index = self.get_asset_index(symbol).await?;
        let sz_decimals = self.get_sz_decimals(symbol).await?;
        let is_buy = matches!(side, OrderSide::Buy);
        let sz = self.format_size(quantity, sz_decimals);
        let px = match order_type {
            OrderType::Market => {
                let current_price = self.get_current_price(symbol).await?;
                let slippage = 0.05;
                let price_with_slippage = if is_buy { current_price * (1.0 + slippage) } else { current_price * (1.0 - slippage) };
                self.format_price(price_with_slippage)
            }
            OrderType::Limit => match price {
                Some(p) => self.format_price(p),
                None => return Err(anyhow::anyhow!("Limit order requires a price")),
            },
            _ => return Err(anyhow::anyhow!("Unsupported order type: {:?}", order_type)),
        };
        let order_type_obj = match order_type {
            OrderType::Market => serde_json::json!({"limit": {"tif": "Ioc"}}),
            OrderType::Limit => serde_json::json!({"limit": {"tif": "Gtc"}}),
            _ => return Err(anyhow::anyhow!("Unsupported order type")),
        };
        let order = serde_json::json!({"a": asset_index, "b": is_buy, "p": px, "s": sz, "r": reduce_only, "t": order_type_obj});
        let action = serde_json::json!({"type": "order", "orders": [order], "grouping": "na"});
        let nonce = timestamp_ms();
        let signature = self.sign_l1_action(&action, nonce, None).await?;
        let request = serde_json::json!({"action": action, "nonce": nonce, "signature": signature});
        debug!("Hyperliquid order request: {:?}", request);
        let response: serde_json::Value = self.client.post(format!("{}/exchange", self.base_url)).json(&request).send().await?.json().await?;
        debug!("Hyperliquid order response: {:?}", response);
        let status = response["status"].as_str().unwrap_or("");
        if status != "ok" {
            let error_msg = response["response"]["data"]["statuses"][0]["error"].as_str()
                .or_else(|| response.get("error").and_then(|e| e.as_str()))
                .unwrap_or("Unknown error").to_string();
            return Ok(OrderResult::failure(symbol.to_string(), side, error_msg));
        }
        let statuses = response["response"]["data"]["statuses"].as_array()
            .ok_or_else(|| anyhow::anyhow!("Invalid response: missing statuses"))?;
        if let Some(first_status) = statuses.first() {
            let order_id = first_status["resting"]["oid"].as_i64()
                .or_else(|| first_status["filled"]["oid"].as_i64())
                .map(|id| id.to_string());
            let avg_price = first_status["filled"]["avgPx"].as_str().and_then(|s| s.parse::<f64>().ok());
            let filled_sz = first_status["filled"]["totalSz"].as_str().and_then(|s| s.parse::<f64>().ok()).unwrap_or(0.0);
            if let Some(error) = first_status["error"].as_str() {
                return Ok(OrderResult::failure(symbol.to_string(), side, error.to_string()));
            }
            let raw_map = response.as_object().map(|obj| obj.iter().map(|(k, v)| (k.clone(), v.clone())).collect());
            return Ok(OrderResult {
                success: true, order_id, symbol: symbol.to_string(), side, order_type,
                quantity: Some(quantity), entry_price: avg_price.or(price),
                position_size: Some(filled_sz), position_usdt: avg_price.map(|p| filled_sz * p),
                error: None, raw_response: raw_map,
            });
        }
        Ok(OrderResult::failure(symbol.to_string(), side, "No order status in response".to_string()))
    }

    #[allow(dead_code)]
    async fn get_open_orders(&self, symbol: Option<&str>) -> Result<Vec<serde_json::Value>> {
        let response: serde_json::Value = self.client
            .post(format!("{}/info", self.base_url))
            .json(&serde_json::json!({"type": "openOrders", "user": self.wallet_address}))
            .send().await?.json().await?;
        let orders = response.as_array().cloned().unwrap_or_default();
        if let Some(sym) = symbol {
            let hl_symbol = self.to_hl_symbol(sym);
            Ok(orders.into_iter().filter(|o| o["coin"].as_str() == Some(&hl_symbol)).collect())
        } else {
            Ok(orders)
        }
    }

    async fn get_positions(&self) -> Result<Vec<serde_json::Value>> {
        let response: serde_json::Value = self.client
            .post(format!("{}/info", self.base_url))
            .json(&serde_json::json!({"type": "clearinghouseState", "user": self.wallet_address}))
            .send().await?.json().await?;
        let positions = response["assetPositions"].as_array().cloned().unwrap_or_default();
        Ok(positions)
    }
}

#[async_trait]
impl ExchangeTrader for HyperliquidTrader {
    fn exchange_name(&self) -> &str { "hyperliquid" }
    fn market_type(&self) -> MarketType { self.market_type }

    fn capabilities(&self) -> ExchangeCapabilities {
        sdktrader_core::exchange_capabilities! {
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
        let response = self.client.post(format!("{}/info", self.base_url))
            .json(&serde_json::json!({"type": "meta"})).send().await?;
        Ok(response.status().is_success())
    }

    async fn get_server_time(&self) -> Result<i64> {
        let response: serde_json::Value = self.client.post(format!("{}/info", self.base_url))
            .json(&serde_json::json!({"type": "exchangeStatus"})).send().await?.json().await?;
        response["time"].as_str().and_then(|s| s.parse::<i64>().ok())
            .or_else(|| response["time"].as_i64())
            .ok_or_else(|| anyhow::anyhow!("Failed to get server time"))
    }

    async fn get_balance(&self) -> Result<AccountBalance> {
        let response: serde_json::Value = self.client.post(format!("{}/info", self.base_url))
            .json(&serde_json::json!({"type": "clearinghouseState", "user": self.wallet_address}))
            .send().await?.json().await?;
        let account_value = response["marginSummary"]["accountValue"].as_str().and_then(|s| s.parse::<f64>().ok()).unwrap_or(0.0);
        let total_margin_used = response["marginSummary"]["totalMarginUsed"].as_str().and_then(|s| s.parse::<f64>().ok()).unwrap_or(0.0);
        let available = account_value - total_margin_used;
        Ok(AccountBalance::simple(available, account_value, "USDC"))
    }

    async fn get_current_price(&self, symbol: &str) -> Result<f64> {
        let hl_symbol = self.to_hl_symbol(symbol);
        let response: serde_json::Value = self.client.post(format!("{}/info", self.base_url))
            .json(&serde_json::json!({"type": "allMids"})).send().await?.json().await?;
        response[&hl_symbol].as_str().and_then(|s| s.parse::<f64>().ok())
            .ok_or_else(|| anyhow::anyhow!("Price not found for {} (tried {})", symbol, hl_symbol))
    }

    async fn get_ticker(&self, symbol: &str) -> Result<TickerInfo> {
        let hl_symbol = self.to_hl_symbol(symbol);
        let response: serde_json::Value = self.client.post(format!("{}/info", self.base_url))
            .json(&serde_json::json!({"type": "metaAndAssetCtxs"})).send().await?.json().await?;
        let meta_array = response.as_array().ok_or_else(|| anyhow::anyhow!("Invalid metaAndAssetCtxs response"))?;
        if meta_array.len() < 2 { return Err(anyhow::anyhow!("Invalid metaAndAssetCtxs response: missing data")); }
        let universe = meta_array[0]["universe"].as_array().ok_or_else(|| anyhow::anyhow!("Missing universe"))?;
        let asset_ctxs = meta_array[1].as_array().ok_or_else(|| anyhow::anyhow!("Missing asset contexts"))?;
        let asset_idx = universe.iter().position(|a| a["name"].as_str() == Some(&hl_symbol))
            .ok_or_else(|| anyhow::anyhow!("Asset not found: {}", hl_symbol))?;
        let ctx = &asset_ctxs[asset_idx];
        let mid_px = ctx["midPx"].as_str().and_then(|s| s.parse::<f64>().ok()).unwrap_or(0.0);
        let mark_px = ctx["markPx"].as_str().and_then(|s| s.parse::<f64>().ok());
        let oracle_px = ctx["oraclePx"].as_str().and_then(|s| s.parse::<f64>().ok());
        let funding_rate = ctx["funding"].as_str().and_then(|s| s.parse::<f64>().ok());
        let open_interest = ctx["openInterest"].as_str().and_then(|s| s.parse::<f64>().ok());
        let day_ntl_vlm = ctx["dayNtlVlm"].as_str().and_then(|s| s.parse::<f64>().ok());
        let prev_day_px = ctx["prevDayPx"].as_str().and_then(|s| s.parse::<f64>().ok());
        let change_24h = prev_day_px.map(|prev| if prev > 0.0 { ((mid_px - prev) / prev) * 100.0 } else { 0.0 });
        let open_interest_value = match (open_interest, mark_px) { (Some(oi), Some(price)) => Some(oi * price), _ => None };
        let now = timestamp_ms();
        let next_funding_time = ((now / FUNDING_INTERVAL_MS) + 1) * FUNDING_INTERVAL_MS;
        let (bid_price, ask_price) = if let Some(impact_pxs) = ctx["impactPxs"].as_array() {
            (impact_pxs.first().and_then(|v| v.as_str()).and_then(|s| s.parse().ok()),
             impact_pxs.get(1).and_then(|v| v.as_str()).and_then(|s| s.parse().ok()))
        } else { (None, None) };
        Ok(TickerInfo {
            symbol: symbol.to_string(), last_price: mid_px, bid_price, ask_price,
            high_24h: None, low_24h: None, volume_24h: day_ntl_vlm, turnover_24h: day_ntl_vlm,
            change_24h, open_interest, open_interest_value, funding_rate,
            next_funding_time: Some(next_funding_time), mark_price: mark_px,
            index_price: oracle_px, timestamp: timestamp_ms(),
        })
    }

    async fn get_orderbook(&self, symbol: &str, _depth: u32) -> Result<Orderbook> {
        let hl_symbol = self.to_hl_symbol(symbol);
        let response: serde_json::Value = self.client.post(format!("{}/info", self.base_url))
            .json(&serde_json::json!({"type": "l2Book", "coin": hl_symbol})).send().await?.json().await?;
        let levels = &response["levels"];
        let bids: Vec<OrderbookLevel> = levels[0].as_array().map(|arr| arr.iter().filter_map(|level| {
            let price = level["px"].as_str()?.parse().ok()?;
            let qty = level["sz"].as_str()?.parse().ok()?;
            Some(OrderbookLevel { price, quantity: qty })
        }).collect()).unwrap_or_default();
        let asks: Vec<OrderbookLevel> = levels[1].as_array().map(|arr| arr.iter().filter_map(|level| {
            let price = level["px"].as_str()?.parse().ok()?;
            let qty = level["sz"].as_str()?.parse().ok()?;
            Some(OrderbookLevel { price, quantity: qty })
        }).collect()).unwrap_or_default();
        Ok(Orderbook { symbol: symbol.to_string(), bids, asks, timestamp: timestamp_ms() })
    }

    async fn get_funding_rate_history(&self, symbol: &str, limit: u32) -> Result<Vec<FundingRateEntry>> {
        let hl_symbol = self.to_hl_symbol(symbol);
        let end_time = timestamp_ms();
        let start_time = end_time - (limit as i64 * FUNDING_INTERVAL_MS);
        let response: serde_json::Value = self.client.post(format!("{}/info", self.base_url))
            .json(&serde_json::json!({"type": "fundingHistory", "coin": hl_symbol, "startTime": start_time}))
            .send().await?.json().await?;
        let entries: Vec<FundingRateEntry> = response.as_array().map(|arr| arr.iter().filter_map(|e| {
            Some(FundingRateEntry { symbol: symbol.to_string(), funding_rate: e["fundingRate"].as_str()?.parse().ok()?, funding_time: e["time"].as_i64()? })
        }).take(limit as usize).collect()).unwrap_or_default();
        Ok(entries)
    }

    async fn get_open_interest_history(&self, symbol: &str, _interval: &str, _limit: u32) -> Result<Vec<OpenInterestEntry>> {
        let ticker = self.get_ticker(symbol).await?;
        Ok(vec![OpenInterestEntry { symbol: symbol.to_string(), open_interest: ticker.open_interest.unwrap_or(0.0), open_interest_value: ticker.open_interest_value.unwrap_or(0.0), timestamp: ticker.timestamp }])
    }

    async fn set_leverage(&self, symbol: Option<&str>, leverage: u32) -> Result<bool> {
        let Some(sym) = symbol else { return Ok(true); };
        let asset_index = self.get_asset_index(sym).await?;
        let action = serde_json::json!({"type": "updateLeverage", "asset": asset_index, "isCross": true, "leverage": leverage});
        let nonce = timestamp_ms();
        let signature = self.sign_l1_action(&action, nonce, None).await?;
        let request = serde_json::json!({"action": action, "nonce": nonce, "signature": signature});
        let response: serde_json::Value = self.client.post(format!("{}/exchange", self.base_url)).json(&request).send().await?.json().await?;
        let status = response["status"].as_str().unwrap_or("");
        if status == "ok" { info!("Hyperliquid: set leverage {} for {}", leverage, sym); Ok(true) }
        else { let error = response["response"]["data"]["error"].as_str().unwrap_or("Unknown error"); warn!("Failed to set leverage for {}: {}", sym, error); Ok(false) }
    }

    async fn execute_fast_entry(&self, symbol: &str, direction: &str, usdt_amount: f64, leverage: u32, _tp_percent: Option<f64>, _sl_percent: Option<f64>, current_price: Option<f64>) -> Result<OrderResult> {
        let price = match current_price { Some(p) => p, None => self.get_current_price(symbol).await? };
        let quantity = (usdt_amount * leverage as f64) / price;
        let side = if direction.to_lowercase() == "long" { OrderSide::Buy } else { OrderSide::Sell };
        if let Err(e) = self.set_leverage(Some(symbol), leverage).await { warn!("Failed to set leverage: {}", e); }
        self.place_order_internal(symbol, side, OrderType::Market, quantity, None, false).await
    }

    async fn place_market_order(&self, symbol: &str, side: OrderSide, quantity: f64) -> Result<OrderResult> {
        self.place_order_internal(symbol, side, OrderType::Market, quantity, None, false).await
    }

    async fn place_limit_order(&self, symbol: &str, side: OrderSide, quantity: f64, price: f64, reduce_only: bool) -> Result<OrderResult> {
        self.place_order_internal(symbol, side, OrderType::Limit, quantity, Some(price), reduce_only).await
    }

    async fn cancel_order(&self, symbol: &str, order_id: &str) -> Result<bool> {
        let asset_index = self.get_asset_index(symbol).await?;
        let oid: i64 = order_id.parse().context("Invalid order ID")?;
        let action = serde_json::json!({"type": "cancel", "cancels": [{"a": asset_index, "o": oid}]});
        let nonce = timestamp_ms();
        let signature = self.sign_l1_action(&action, nonce, None).await?;
        let request = serde_json::json!({"action": action, "nonce": nonce, "signature": signature});
        let response: serde_json::Value = self.client.post(format!("{}/exchange", self.base_url)).json(&request).send().await?.json().await?;
        let status = response["status"].as_str().unwrap_or("");
        if status == "ok" { info!("Hyperliquid: cancelled order {} for {}", order_id, symbol); Ok(true) }
        else { let error = response["response"]["data"]["statuses"][0].as_str().unwrap_or("Unknown error"); warn!("Failed to cancel order {}: {}", order_id, error); Ok(false) }
    }

    async fn get_order_status(&self, symbol: &str, order_id: &str) -> Result<OrderInfo> {
        let oid: i64 = order_id.parse().context("Invalid order ID")?;
        let response: serde_json::Value = self.client.post(format!("{}/info", self.base_url))
            .json(&serde_json::json!({"type": "orderStatus", "user": self.wallet_address, "oid": oid}))
            .send().await?.json().await?;
        let order = &response["order"];
        let status_str = response["status"].as_str().unwrap_or("unknown");
        let side = if order["side"].as_str() == Some("B") { OrderSide::Buy } else { OrderSide::Sell };
        let order_type = if order["orderType"].as_str() == Some("Market") { OrderType::Market } else { OrderType::Limit };
        let price = order["limitPx"].as_str().and_then(|s| s.parse().ok()).unwrap_or(0.0);
        let quantity = order["origSz"].as_str().or_else(|| order["sz"].as_str()).and_then(|s| s.parse().ok()).unwrap_or(0.0);
        let filled_quantity = order["filled"].as_str().and_then(|s| s.parse().ok()).unwrap_or(0.0);
        let timestamp = order["timestamp"].as_i64().unwrap_or(0);
        Ok(OrderInfo {
            order_id: order_id.to_string(), client_order_id: order["cloid"].as_str().map(|s| s.to_string()),
            symbol: symbol.to_string(), status: parse_order_status_str(status_str), side, order_type,
            price, quantity, filled_quantity, remaining_quantity: quantity - filled_quantity,
            avg_price: order["avgPx"].as_str().and_then(|s| s.parse().ok()), timestamp,
            time_in_force: TimeInForce::GTC, reduce_only: order["reduceOnly"].as_bool().unwrap_or(false),
        })
    }

    async fn close_position(&self, symbol: &str) -> Result<OrderResult> {
        let hl_symbol = self.to_hl_symbol(symbol);
        let positions = self.get_positions().await?;
        let position = positions.iter().find(|p| p["position"]["coin"].as_str() == Some(&hl_symbol));
        let Some(pos) = position else {
            return Ok(OrderResult::failure(symbol.to_string(), OrderSide::Sell, "No position found".to_string()));
        };
        let size: f64 = pos["position"]["szi"].as_str().and_then(|s| s.parse().ok()).unwrap_or(0.0);
        if size.abs() < 0.000001 {
            return Ok(OrderResult::failure(symbol.to_string(), OrderSide::Sell, "Position size is zero".to_string()));
        }
        let (side, quantity) = if size > 0.0 { (OrderSide::Sell, size) } else { (OrderSide::Buy, size.abs()) };
        self.place_order_internal(symbol, side, OrderType::Market, quantity, None, true).await
    }

    fn format_symbol(&self, base_symbol: &str) -> String { self.to_hl_symbol(base_symbol) }

    async fn test_ws_orderbook(&self, symbol: &str, _depth: u32, timeout_ms: u64) -> Result<WsTestResult> {
        let hl_symbol = self.to_hl_symbol(symbol);
        let ws_url = self.ws_url();
        let start = std::time::Instant::now();
        let (mut ws_stream, _) = connect_async(ws_url).await.context("Failed to connect to Hyperliquid WebSocket")?;
        let connect_time = start.elapsed().as_millis() as u64;
        let subscribe_msg = serde_json::json!({"method": "subscribe", "subscription": {"type": "l2Book", "coin": hl_symbol}});
        ws_stream.send(Message::Text(subscribe_msg.to_string())).await?;
        let mut messages_received = 0u32;
        let mut data_sample = None;
        let timeout = tokio::time::Duration::from_millis(timeout_ms);
        loop {
            tokio::select! {
                msg = ws_stream.next() => {
                    match msg {
                        Some(Ok(Message::Text(text))) => { messages_received += 1; if let Ok(parsed) = serde_json::from_str::<serde_json::Value>(&text) { if parsed.get("data").is_some() || parsed["channel"] == "l2Book" { data_sample = Some(parsed); break; } } }
                        Some(Ok(Message::Ping(data))) => { let _ = ws_stream.send(Message::Pong(data)).await; }
                        Some(Ok(_)) => {} Some(Err(e)) => { return Err(anyhow::anyhow!("WebSocket error: {}", e)); } None => break,
                    }
                }
                _ = tokio::time::sleep(timeout) => { break; }
            }
        }
        let _ = ws_stream.close(None).await;
        Ok(WsTestResult { connected: true, latency_ms: connect_time, messages_received, subscription_type: format!("l2Book.{}", hl_symbol), data_sample })
    }

    async fn test_ws_trades(&self, symbol: &str, timeout_ms: u64) -> Result<WsTestResult> {
        let hl_symbol = self.to_hl_symbol(symbol);
        let ws_url = self.ws_url();
        let start = std::time::Instant::now();
        let (mut ws_stream, _) = connect_async(ws_url).await.context("Failed to connect to Hyperliquid WebSocket")?;
        let connect_time = start.elapsed().as_millis() as u64;
        let subscribe_msg = serde_json::json!({"method": "subscribe", "subscription": {"type": "trades", "coin": hl_symbol}});
        ws_stream.send(Message::Text(subscribe_msg.to_string())).await?;
        let mut messages_received = 0u32;
        let mut data_sample = None;
        let timeout = tokio::time::Duration::from_millis(timeout_ms);
        loop {
            tokio::select! {
                msg = ws_stream.next() => {
                    match msg {
                        Some(Ok(Message::Text(text))) => { messages_received += 1; if let Ok(parsed) = serde_json::from_str::<serde_json::Value>(&text) { if parsed.get("data").is_some() { data_sample = Some(parsed); break; } } }
                        Some(Ok(Message::Ping(data))) => { let _ = ws_stream.send(Message::Pong(data)).await; }
                        Some(Ok(_)) => {} Some(Err(e)) => { return Err(anyhow::anyhow!("WebSocket error: {}", e)); } None => break,
                    }
                }
                _ = tokio::time::sleep(timeout) => { break; }
            }
        }
        let _ = ws_stream.close(None).await;
        Ok(WsTestResult { connected: true, latency_ms: connect_time, messages_received, subscription_type: format!("trades.{}", hl_symbol), data_sample })
    }

    async fn test_ws_ticker(&self, symbol: &str, timeout_ms: u64) -> Result<WsTestResult> {
        let hl_symbol = self.to_hl_symbol(symbol);
        let ws_url = self.ws_url();
        let start = std::time::Instant::now();
        let (mut ws_stream, _) = connect_async(ws_url).await.context("Failed to connect to Hyperliquid WebSocket")?;
        let connect_time = start.elapsed().as_millis() as u64;
        let subscribe_msg = serde_json::json!({"method": "subscribe", "subscription": {"type": "activeAssetData", "coin": hl_symbol}});
        ws_stream.send(Message::Text(subscribe_msg.to_string())).await?;
        let mut messages_received = 0u32;
        let mut data_sample = None;
        let timeout = tokio::time::Duration::from_millis(timeout_ms);
        loop {
            tokio::select! {
                msg = ws_stream.next() => {
                    match msg {
                        Some(Ok(Message::Text(text))) => { messages_received += 1; if let Ok(parsed) = serde_json::from_str::<serde_json::Value>(&text) { if parsed.get("data").is_some() || parsed.get("channel").is_some() { data_sample = Some(parsed); break; } } }
                        Some(Ok(Message::Ping(data))) => { let _ = ws_stream.send(Message::Pong(data)).await; }
                        Some(Ok(_)) => {} Some(Err(e)) => { return Err(anyhow::anyhow!("WebSocket error: {}", e)); } None => break,
                    }
                }
                _ = tokio::time::sleep(timeout) => { break; }
            }
        }
        let _ = ws_stream.close(None).await;
        Ok(WsTestResult { connected: true, latency_ms: connect_time, messages_received, subscription_type: format!("activeAssetData.{} (includes OI, fundingRate)", hl_symbol), data_sample })
    }

    async fn subscribe_order_updates(&self, sender: broadcast::Sender<OrderUpdateEvent>) -> Result<JoinHandle<()>> {
        let wallet_address = self.wallet_address.clone();
        let testnet = self.testnet;
        let exchange_name = "hyperliquid".to_string();
        let handle = tokio::spawn(async move {
            let mut reconnect_delay = INITIAL_RECONNECT_DELAY_SECS;
            loop {
                let ws_url = if testnet { TESTNET_WS_URL } else { MAINNET_WS_URL };
                info!("[{}] Connecting to WS: {}", exchange_name, ws_url);
                match connect_async(ws_url).await {
                    Ok((mut ws_stream, _)) => {
                        let sub_msg = serde_json::json!({"method": "subscribe", "subscription": {"type": "orderUpdates", "user": wallet_address}});
                        if let Err(e) = ws_stream.send(Message::Text(sub_msg.to_string())).await {
                            error!("[{}] Failed to subscribe: {}", exchange_name, e);
                            tokio::time::sleep(tokio::time::Duration::from_secs(reconnect_delay)).await;
                            reconnect_delay = next_reconnect_delay(reconnect_delay);
                            continue;
                        }
                        info!("[{}] Subscribed to order updates for {}", exchange_name, wallet_address);
                        reconnect_delay = INITIAL_RECONNECT_DELAY_SECS;
                        let mut ping_interval = tokio::time::interval(tokio::time::Duration::from_secs(30));
                        loop {
                            tokio::select! {
                                msg = ws_stream.next() => {
                                    match msg {
                                        Some(Ok(Message::Text(text))) => {
                                            if let Ok(msg_json) = serde_json::from_str::<serde_json::Value>(&text) {
                                                let channel = msg_json.get("channel").and_then(|v| v.as_str()).unwrap_or("");
                                                if channel == "orderUpdates" {
                                                    if let Some(data_arr) = msg_json.get("data").and_then(|d| d.as_array()) {
                                                        for o in data_arr {
                                                            let order_obj = o.get("order").unwrap_or(o);
                                                            let status_str = o.get("status").and_then(|v| v.as_str())
                                                                .or_else(|| order_obj.get("status").and_then(|v| v.as_str())).unwrap_or("open");
                                                            let event = OrderUpdateEvent {
                                                                exchange: exchange_name.clone(),
                                                                order_id: order_obj.get("oid").and_then(|v| v.as_i64()).map(|v| v.to_string())
                                                                    .or_else(|| order_obj.get("oid").and_then(|v| v.as_str()).map(|s| s.to_string())).unwrap_or_default(),
                                                                client_order_id: order_obj.get("cloid").and_then(|v| v.as_str()).map(|s| s.to_string()),
                                                                symbol: order_obj.get("coin").and_then(|v| v.as_str()).unwrap_or("").to_string(),
                                                                status: parse_order_status_str(status_str),
                                                                side: if order_obj.get("side").and_then(|v| v.as_str()) == Some("B") { OrderSide::Buy } else { OrderSide::Sell },
                                                                order_type: if order_obj.get("orderType").and_then(|v| v.as_str()) == Some("Market") { OrderType::Market } else { OrderType::Limit },
                                                                price: order_obj.get("limitPx").and_then(|v| v.as_str()).and_then(|s| s.parse().ok()).unwrap_or(0.0),
                                                                quantity: order_obj.get("sz").and_then(|v| v.as_str()).and_then(|s| s.parse().ok()).unwrap_or(0.0),
                                                                filled_quantity: order_obj.get("filled").and_then(|v| v.as_str())
                                                                    .or_else(|| order_obj.get("totalSz").and_then(|v| v.as_str())).and_then(|s| s.parse().ok()).unwrap_or(0.0),
                                                                avg_fill_price: order_obj.get("avgPx").and_then(|v| v.as_str()).and_then(|s| s.parse().ok()),
                                                                fee: None, fee_currency: None,
                                                                exchange_timestamp: o.get("statusTimestamp").and_then(|v| v.as_i64()).unwrap_or_else(timestamp_ms),
                                                                received_at: timestamp_ms(), raw: Some(o.clone()),
                                                            };
                                                            log_order_update(&event);
                                                            let _ = sender.send(event);
                                                        }
                                                    }
                                                } else if channel == "subscriptionResponse" { debug!("[{}] Subscription confirmed: {}", exchange_name, text); }
                                            }
                                        }
                                        Some(Ok(Message::Ping(data))) => { let _ = ws_stream.send(Message::Pong(data)).await; }
                                        Some(Ok(Message::Close(_))) | Some(Err(_)) | None => { warn!("[{}] WS disconnected", exchange_name); break; }
                                        _ => {}
                                    }
                                }
                                _ = ping_interval.tick() => {
                                    let ping = serde_json::json!({"method": "ping"});
                                    if let Err(e) = ws_stream.send(Message::Text(ping.to_string())).await { error!("[{}] Ping failed: {}", exchange_name, e); break; }
                                }
                            }
                        }
                    }
                    Err(e) => { error!("[{}] Failed to connect WS: {}", exchange_name, e); }
                }
                warn!("[{}] Reconnecting in {}s...", exchange_name, reconnect_delay);
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
    fn test_format_symbol() {
        let trader = HyperliquidTrader::new("", "", false, MarketType::Swap);
        assert_eq!(trader.format_symbol("BTCUSDT"), "BTC");
        assert_eq!(trader.format_symbol("BTC/USDT"), "BTC");
        assert_eq!(trader.format_symbol("BTC"), "BTC");
        assert_eq!(trader.format_symbol("ETHPERP"), "ETH");
    }

    #[test]
    fn test_to_hl_symbol() {
        let trader = HyperliquidTrader::new("", "", false, MarketType::Swap);
        assert_eq!(trader.to_hl_symbol("BTCUSDT"), "BTC");
        assert_eq!(trader.to_hl_symbol("BTC/USDT"), "BTC");
        assert_eq!(trader.to_hl_symbol("btc"), "BTC");
    }

    #[test]
    fn test_format_price() {
        let trader = HyperliquidTrader::new("", "", false, MarketType::Swap);
        assert_eq!(trader.format_price(50000.0), "50000");
        assert_eq!(trader.format_price(12345.6789), "12346");
        assert_eq!(trader.format_price(1.23456), "1.2346");
        assert_eq!(trader.format_price(0.00012345), "0.000123450");
    }

    #[test]
    fn test_format_size() {
        let trader = HyperliquidTrader::new("", "", false, MarketType::Swap);
        assert_eq!(trader.format_size(1.23456789, 2), "1.23");
        assert_eq!(trader.format_size(1.23456789, 4), "1.2346");
        assert_eq!(trader.format_size(0.00123456, 6), "0.001235");
    }

    #[test]
    fn test_parse_order_status() {
        assert_eq!(parse_order_status_str("open"), OrderStatus::New);
        assert_eq!(parse_order_status_str("filled"), OrderStatus::Filled);
        assert_eq!(parse_order_status_str("canceled"), OrderStatus::Cancelled);
    }
}
