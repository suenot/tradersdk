//! Public WebSocket stream service for Bybit market data.

use anyhow::{Context, Result};
use futures_util::{SinkExt, StreamExt};
use tokio_tungstenite::{connect_async, tungstenite::protocol::Message};
use tradersdk_core::models::{MarketType, WsTestResult};

use crate::config::{MAINNET_WS_SPOT_URL, MAINNET_WS_URL, TESTNET_WS_SPOT_URL, TESTNET_WS_URL};
use crate::models::WsMessage;

/// Market data stream service for Bybit.
pub struct MarketStreamService {
    testnet: bool,
    market_type: MarketType,
}

impl MarketStreamService {
    /// Create a new market stream service.
    pub fn new(testnet: bool, market_type: MarketType) -> Self {
        Self {
            testnet,
            market_type,
        }
    }

    /// Get WebSocket URL for market data.
    pub fn get_ws_url(&self) -> &str {
        match (self.market_type, self.testnet) {
            (MarketType::Swap, false) => MAINNET_WS_URL,
            (MarketType::Swap, true) => TESTNET_WS_URL,
            (MarketType::Spot, false) => MAINNET_WS_SPOT_URL,
            (MarketType::Spot, true) => TESTNET_WS_SPOT_URL,
        }
    }

    /// Format symbol to Bybit format (e.g., "BTC" -> "BTCUSDT").
    pub fn format_symbol(&self, base_symbol: &str) -> String {
        let symbol = base_symbol.to_uppercase();
        if symbol.contains('/') {
            return symbol.replace("/", "");
        }
        if symbol.ends_with("USDT") {
            symbol
        } else {
            format!("{}USDT", symbol)
        }
    }

    /// Test orderbook WebSocket stream.
    pub async fn test_ws_orderbook(
        &self,
        symbol: &str,
        depth: u32,
        timeout_ms: u64,
    ) -> Result<WsTestResult> {
        let formatted_symbol = self.format_symbol(symbol);
        let ws_url = self.get_ws_url();

        let start = std::time::Instant::now();
        let (mut ws_stream, _) = connect_async(ws_url)
            .await
            .context("Failed to connect to Bybit WebSocket")?;

        let connect_time = start.elapsed().as_millis() as u64;

        let depth_level = if depth <= 1 {
            1
        } else if depth <= 50 {
            50
        } else if depth <= 200 {
            200
        } else {
            500
        };

        let subscribe_msg = serde_json::json!({
            "op": "subscribe",
            "args": [format!("orderbook.{}.{}", depth_level, formatted_symbol)]
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
                            if let Ok(parsed) = serde_json::from_str::<WsMessage>(&text) {
                                if parsed.topic.is_some() && parsed.data.is_some() {
                                    data_sample = parsed.data;
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
            subscription_type: format!("orderbook.{}.{}", depth_level, symbol),
            data_sample,
        })
    }

    /// Test trades WebSocket stream.
    pub async fn test_ws_trades(&self, symbol: &str, timeout_ms: u64) -> Result<WsTestResult> {
        let formatted_symbol = self.format_symbol(symbol);
        let ws_url = self.get_ws_url();

        let start = std::time::Instant::now();
        let (mut ws_stream, _) = connect_async(ws_url)
            .await
            .context("Failed to connect to Bybit WebSocket")?;

        let connect_time = start.elapsed().as_millis() as u64;

        let subscribe_msg = serde_json::json!({
            "op": "subscribe",
            "args": [format!("publicTrade.{}", formatted_symbol)]
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
                            if let Ok(parsed) = serde_json::from_str::<WsMessage>(&text) {
                                if parsed.topic.is_some() && parsed.data.is_some() {
                                    data_sample = parsed.data;
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
            subscription_type: format!("publicTrade.{}", symbol),
            data_sample,
        })
    }

    /// Test ticker WebSocket stream.
    pub async fn test_ws_ticker(&self, symbol: &str, timeout_ms: u64) -> Result<WsTestResult> {
        let formatted_symbol = self.format_symbol(symbol);
        let ws_url = self.get_ws_url();

        let start = std::time::Instant::now();
        let (mut ws_stream, _) = connect_async(ws_url)
            .await
            .context("Failed to connect to Bybit WebSocket")?;

        let connect_time = start.elapsed().as_millis() as u64;

        let subscribe_msg = serde_json::json!({
            "op": "subscribe",
            "args": [format!("tickers.{}", formatted_symbol)]
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
                            if let Ok(parsed) = serde_json::from_str::<WsMessage>(&text) {
                                if parsed.topic.is_some() && parsed.data.is_some() {
                                    data_sample = parsed.data;
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
            subscription_type: format!("tickers.{} (includes OI, fundingRate)", symbol),
            data_sample,
        })
    }
}
