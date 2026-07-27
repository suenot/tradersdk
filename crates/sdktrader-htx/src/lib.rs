//! HTX (formerly Huobi) exchange adapter.
//!
//! This module provides trading functionality for the HTX exchange.
//! Supports both Spot and USDT-M Futures (Swap) markets.
//!
//! # API Endpoints
//! - Spot: https://api.huobi.pro
//! - Futures: https://api.hbdm.com
//! - WebSocket Spot: wss://api.huobi.pro/ws
//! - WebSocket Futures: wss://api.hbdm.com/linear-swap-ws

mod client;

pub use client::HtxTrader;
