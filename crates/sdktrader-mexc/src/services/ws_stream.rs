//! MEXC public market data WebSocket service.
//!
//! MEXC has different WebSocket URLs and subscription formats for spot vs futures:
//! - Spot WS: wss://wbs.mexc.com/ws with "SUBSCRIPTION" method
//! - Futures WS: wss://contract.mexc.com/ws with "sub.*" methods

use anyhow::{Context, Result};
use futures_util::{SinkExt, StreamExt};
use tokio_tungstenite::{connect_async, tungstenite::protocol::Message};

use crate::config::{FUTURES_WS_URL, SPOT_WS_URL};
use crate::helpers::format_symbol;
use sdktrader_core::models::{MarketType, WsTestResult};

/// MEXC public market data stream service.
pub struct MarketStreamService {
    market_type: MarketType,
}

impl MarketStreamService {
    /// Create a new market stream service.
    pub fn new(market_type: MarketType) -> Self {
        Self { market_type }
    }

    /// Get WebSocket URL based on market type.
    fn ws_url(&self) -> &str {
        match self.market_type {
            MarketType::Spot => SPOT_WS_URL,
            MarketType::Swap => FUTURES_WS_URL,
        }
    }

    /// Test WebSocket orderbook subscription.
    ///
    /// Spot subscription format: `{"method": "SUBSCRIPTION", "params": ["spot@public.limit.depth.v3.api@BTCUSDT@20"]}`
    /// Futures subscription format: `{"method": "sub.depth", "param": {"symbol": "BTC_USDT"}}`
    pub async fn test_orderbook(
        &self,
        symbol: &str,
        _depth: u32,
        timeout_ms: u64,
    ) -> Result<WsTestResult> {
        let formatted = format_symbol(symbol, self.market_type);
        let ws_url = self.ws_url();

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
                                // Check for depth data in response
                                // Spot: {"d": {"bids": [...], "asks": [...]}}
                                // Futures: {"data": {"bids": [...], "asks": [...]}}
                                if parsed.get("data").is_some() || parsed.get("d").is_some() || parsed.get("bids").is_some() {
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

    /// Test WebSocket trades subscription.
    ///
    /// Spot subscription format: `{"method": "SUBSCRIPTION", "params": ["spot@public.deals.v3.api@BTCUSDT"]}`
    /// Futures subscription format: `{"method": "sub.deal", "param": {"symbol": "BTC_USDT"}}`
    pub async fn test_trades(&self, symbol: &str, timeout_ms: u64) -> Result<WsTestResult> {
        let formatted = format_symbol(symbol, self.market_type);
        let ws_url = self.ws_url();

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
                                // Check for trade data
                                // Spot: {"d": {"deals": [...]}}
                                // Futures: {"data": [...]}
                                if parsed.get("data").is_some() || parsed.get("d").is_some() {
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

    /// Test WebSocket ticker subscription.
    ///
    /// Spot subscription format: `{"method": "SUBSCRIPTION", "params": ["spot@public.miniTicker.v3.api@BTCUSDT@UTC+0"]}`
    /// Futures subscription format: `{"method": "sub.ticker", "param": {"symbol": "BTC_USDT"}}`
    pub async fn test_ticker(&self, symbol: &str, timeout_ms: u64) -> Result<WsTestResult> {
        let formatted = format_symbol(symbol, self.market_type);
        let ws_url = self.ws_url();

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
                                // Any valid JSON response with data is considered a ticker response
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
}
