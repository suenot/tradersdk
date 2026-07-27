//! Public WebSocket stream service for Binance market data.

use sdktrader_core::models::{MarketType, WsTestResult};
use anyhow::{Context, Result};
use futures_util::{SinkExt, StreamExt};
use tokio_tungstenite::{connect_async, tungstenite::protocol::Message};

use crate::config::{
    FUTURES_WS_MAINNET, FUTURES_WS_TESTNET, SPOT_WS_MAINNET, SPOT_WS_TESTNET,
};

/// Market data stream service for Binance.
pub struct MarketStreamService {
    testnet: bool,
    market_type: MarketType,
}

impl MarketStreamService {
    /// Create a new market stream service.
    pub fn new(testnet: bool, market_type: MarketType) -> Self {
        Self { testnet, market_type }
    }

    /// Get WebSocket URL for market data.
    fn ws_url(&self) -> &str {
        match (self.market_type, self.testnet) {
            (MarketType::Swap, false) => FUTURES_WS_MAINNET,
            (MarketType::Swap, true) => FUTURES_WS_TESTNET,
            (MarketType::Spot, false) => SPOT_WS_MAINNET,
            (MarketType::Spot, true) => SPOT_WS_TESTNET,
        }
    }

    /// Test orderbook WebSocket stream.
    pub async fn test_orderbook(
        &self,
        symbol: &str,
        depth: u32,
        timeout_ms: u64,
    ) -> Result<WsTestResult> {
        let ws_url = self.ws_url();
        let symbol_lower = symbol.to_lowercase();

        let depth_level = if depth <= 5 {
            "5"
        } else if depth <= 10 {
            "10"
        } else {
            "20"
        };

        let stream = format!("{}@depth{}@100ms", symbol_lower, depth_level);
        let ws_full_url = format!("{}/{}", ws_url, stream);

        let start = std::time::Instant::now();
        let (mut ws_stream, _) = connect_async(&ws_full_url)
            .await
            .context("Failed to connect to Binance WebSocket")?;

        let connect_time = start.elapsed().as_millis() as u64;

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
                                if parsed.get("b").is_some() || parsed.get("bids").is_some() {
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
            subscription_type: stream,
            data_sample,
        })
    }

    /// Test trades WebSocket stream.
    pub async fn test_trades(&self, symbol: &str, timeout_ms: u64) -> Result<WsTestResult> {
        let ws_url = self.ws_url();
        let symbol_lower = symbol.to_lowercase();
        let stream = format!("{}@aggTrade", symbol_lower);
        let ws_full_url = format!("{}/{}", ws_url, stream);

        let start = std::time::Instant::now();
        let (mut ws_stream, _) = connect_async(&ws_full_url)
            .await
            .context("Failed to connect to Binance WebSocket")?;

        let connect_time = start.elapsed().as_millis() as u64;

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
                                if parsed.get("p").is_some() {
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
            subscription_type: stream,
            data_sample,
        })
    }

    /// Test ticker WebSocket stream.
    pub async fn test_ticker(&self, symbol: &str, timeout_ms: u64) -> Result<WsTestResult> {
        let ws_url = self.ws_url();
        let symbol_lower = symbol.to_lowercase();

        // For futures use markPrice stream which includes funding
        let stream = if self.market_type == MarketType::Swap {
            format!("{}@markPrice@1s", symbol_lower)
        } else {
            format!("{}@ticker", symbol_lower)
        };
        let ws_full_url = format!("{}/{}", ws_url, stream);

        let start = std::time::Instant::now();
        let (mut ws_stream, _) = connect_async(&ws_full_url)
            .await
            .context("Failed to connect to Binance WebSocket")?;

        let connect_time = start.elapsed().as_millis() as u64;

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
            subscription_type: stream,
            data_sample,
        })
    }
}
