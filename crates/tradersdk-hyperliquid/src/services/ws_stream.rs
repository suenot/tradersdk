//! Hyperliquid public market data WebSocket service.

use anyhow::{Context, Result};
use futures_util::{SinkExt, StreamExt};
use tokio_tungstenite::{connect_async, tungstenite::protocol::Message};

use crate::config::{MAINNET_WS_URL, TESTNET_WS_URL};
use crate::helpers::to_hl_symbol;
use tradersdk_core::models::WsTestResult;

/// Hyperliquid public market data stream service.
pub struct MarketStreamService {
    testnet: bool,
}

impl MarketStreamService {
    pub fn new(testnet: bool) -> Self {
        Self { testnet }
    }

    fn ws_url(&self) -> &str {
        if self.testnet {
            TESTNET_WS_URL
        } else {
            MAINNET_WS_URL
        }
    }

    /// Test WebSocket orderbook subscription (l2Book).
    pub async fn test_ws_orderbook(
        &self,
        symbol: &str,
        _depth: u32,
        timeout_ms: u64,
    ) -> Result<WsTestResult> {
        let hl_symbol = to_hl_symbol(symbol);
        let ws_url = self.ws_url();

        let start = std::time::Instant::now();

        let (mut ws_stream, _) = connect_async(ws_url)
            .await
            .context("Failed to connect to Hyperliquid WebSocket")?;

        let connect_time = start.elapsed().as_millis() as u64;

        // Subscribe to l2Book
        let subscribe_msg = serde_json::json!({
            "method": "subscribe",
            "subscription": {
                "type": "l2Book",
                "coin": hl_symbol
            }
        });

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
                                // Check for data or subscriptionResponse
                                if parsed.get("data").is_some() || parsed["channel"] == "l2Book" {
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
            subscription_type: format!("l2Book.{}", hl_symbol),
            data_sample,
        })
    }

    /// Test WebSocket trades subscription.
    pub async fn test_ws_trades(&self, symbol: &str, timeout_ms: u64) -> Result<WsTestResult> {
        let hl_symbol = to_hl_symbol(symbol);
        let ws_url = self.ws_url();

        let start = std::time::Instant::now();
        let (mut ws_stream, _) = connect_async(ws_url)
            .await
            .context("Failed to connect to Hyperliquid WebSocket")?;

        let connect_time = start.elapsed().as_millis() as u64;

        // Subscribe to trades
        let subscribe_msg = serde_json::json!({
            "method": "subscribe",
            "subscription": {
                "type": "trades",
                "coin": hl_symbol
            }
        });

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
                                if parsed.get("data").is_some() {
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
            subscription_type: format!("trades.{}", hl_symbol),
            data_sample,
        })
    }

    /// Test WebSocket ticker/asset data subscription (activeAssetData).
    pub async fn test_ws_ticker(&self, symbol: &str, timeout_ms: u64) -> Result<WsTestResult> {
        let hl_symbol = to_hl_symbol(symbol);
        let ws_url = self.ws_url();

        let start = std::time::Instant::now();
        let (mut ws_stream, _) = connect_async(ws_url)
            .await
            .context("Failed to connect to Hyperliquid WebSocket")?;

        let connect_time = start.elapsed().as_millis() as u64;

        // Subscribe to activeAssetData (contains funding, OI etc)
        let subscribe_msg = serde_json::json!({
            "method": "subscribe",
            "subscription": {
                "type": "activeAssetData",
                "coin": hl_symbol
            }
        });

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
                                if parsed.get("data").is_some() || parsed.get("channel").is_some() {
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
            subscription_type: format!("activeAssetData.{} (includes OI, fundingRate)", hl_symbol),
            data_sample,
        })
    }
}
