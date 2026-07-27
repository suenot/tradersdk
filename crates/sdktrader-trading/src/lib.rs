//! Trading logic: PnL calculators, symbol formatting, position sizing, exit grid.
//!
//! This crate contains the pure-computation core of the trading system.
//! It does NOT perform any exchange API calls.

pub mod pnl;
pub mod symbol;
pub mod position;
pub mod exit;

pub use pnl::{
    calculate_pnl, calculate_aggregate_pnl, calculate_fifo_pnl,
    PnlResult, FillInfo, FifoMatch, FifoPnlResult,
};
pub use symbol::{
    Exchange, extract_base_symbol, format_symbol_for_exchange,
};
pub use position::{
    calculate_position_size, calculate_split_orders, SplitOrderConfig,
};
pub use exit::{
    calculate_limit_orders_grid, GridRule, GridOrder,
};
