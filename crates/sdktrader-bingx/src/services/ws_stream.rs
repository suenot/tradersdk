//! Public WebSocket stream service for BingX market data.
//!
//! BingX sends data in gzip-compressed Binary messages, so we need to decompress them.

use sdktrader_core::models::{MarketType, WsTestResult};
use anyhow::{Context, Result};
use futures_util::{SinkExt, StreamExt};
use tokio_tungstenite::{connect_async, tungstenite::protocol::Message};

use super::super::config::{
    MAINNET_WS_SPOT_URL, MAINNET_WS_SWAP_URL, TESTNET_WS_SWAP_URL,
};
use super::super::helpers::{decompress, format_symbol};

/// Market data stream service for BingX.
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
    pub fn get_ws_url(&self) -> &str {
        match (self.market_type, self.testnet) {
            (MarketType::Swap, false) => MAINNET_WS_SWAP_URL,
            (MarketType::Swap, true) => TESTNET_WS_SWAP_URL,
            (MarketType::Spot, _) => MAINNET_WS_SPOT_URL, // No spot testnet
        }
    }

    /// Format symbol to BingX format (e.g., "BTC" -> "BTC-USDT").
    pub fn format_symbol(&self, base_symbol: &str) -> String {
        format_symbol(base_symbol)
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
            .context("Failed to connect to BingX WebSocket")?;

        let connect_time = start.elapsed().as_millis() as u64;

        // BingX depth levels: 5, 10, 20
        let level = if depth <= 5 {
            5
        } else if depth <= 10 {
            10
        } else {
            20
        };

        let subscribe_msg = serde_json::json!({
            "id": "orderbook_sub",
            "reqType": "sub",
            "dataType": format!("{}@depth{}@500ms", formatted_symbol, level)
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
                            if text == "Ping" {
                                let _ = ws_stream.send(Message::Text("Pong".to_string())).await;
                                continue;
                            }
                            if let Ok(parsed) = serde_json::from_str::<serde_json::Value>(&text) {
                                if parsed.get("data").is_some() {
                                    data_sample = Some(parsed);
                                    break;
                                }
                                if data_sample.is_none() {
                                    data_sample = Some(parsed);
                                }
                            }
                        }
                        Some(Ok(Message::Binary(bin))) => {
                            messages_received += 1;
                            if let Ok(text) = decompress(&bin) {
                                if text == "Ping" {
                                    let _ = ws_stream.send(Message::Text("Pong".to_string())).await;
                                    continue;
                                }
                                if let Ok(parsed) = serde_json::from_str::<serde_json::Value>(&text) {
                                    if parsed.get("data").is_some() {
                                        data_sample = Some(parsed);
                                        break;
                                    }
                                    if data_sample.is_none() {
                                        data_sample = Some(parsed);
                                    }
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
            subscription_type: format!("{}@depth{}", formatted_symbol, level),
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
            .context("Failed to connect to BingX WebSocket")?;

        let connect_time = start.elapsed().as_millis() as u64;

        let subscribe_msg = serde_json::json!({
            "id": "trade_sub",
            "reqType": "sub",
            "dataType": format!("{}@trade", formatted_symbol)
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
                            if text == "Ping" {
                                let _ = ws_stream.send(Message::Text("Pong".to_string())).await;
                                continue;
                            }
                            if let Ok(parsed) = serde_json::from_str::<serde_json::Value>(&text) {
                                if parsed.get("data").is_some() {
                                    data_sample = Some(parsed);
                                    break;
                                }
                                if data_sample.is_none() {
                                    data_sample = Some(parsed);
                                }
                            }
                        }
                        Some(Ok(Message::Binary(bin))) => {
                            messages_received += 1;
                            if let Ok(text) = decompress(&bin) {
                                if text == "Ping" {
                                    let _ = ws_stream.send(Message::Text("Pong".to_string())).await;
                                    continue;
                                }
                                if let Ok(parsed) = serde_json::from_str::<serde_json::Value>(&text) {
                                    if parsed.get("data").is_some() {
                                        data_sample = Some(parsed);
                                        break;
                                    }
                                    if data_sample.is_none() {
                                        data_sample = Some(parsed);
                                    }
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
            subscription_type: format!("{}@trade", formatted_symbol),
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
            .context("Failed to connect to BingX WebSocket")?;

        let connect_time = start.elapsed().as_millis() as u64;

        let subscribe_msg = serde_json::json!({
            "id": "ticker_sub",
            "reqType": "sub",
            "dataType": format!("{}@ticker", formatted_symbol)
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
                            if text == "Ping" {
                                let _ = ws_stream.send(Message::Text("Pong".to_string())).await;
                                continue;
                            }
                            if let Ok(parsed) = serde_json::from_str::<serde_json::Value>(&text) {
                                if parsed.get("data").is_some() {
                                    data_sample = Some(parsed);
                                    break;
                                }
                                if data_sample.is_none() {
                                    data_sample = Some(parsed);
                                }
                            }
                        }
                        Some(Ok(Message::Binary(bin))) => {
                            messages_received += 1;
                            if let Ok(text) = decompress(&bin) {
                                if text == "Ping" {
                                    let _ = ws_stream.send(Message::Text("Pong".to_string())).await;
                                    continue;
                                }
                                if let Ok(parsed) = serde_json::from_str::<serde_json::Value>(&text) {
                                    if parsed.get("data").is_some() {
                                        data_sample = Some(parsed);
                                        break;
                                    }
                                    if data_sample.is_none() {
                                        data_sample = Some(parsed);
                                    }
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
            subscription_type: format!("{}@ticker", formatted_symbol),
            data_sample,
        })
    }
}
