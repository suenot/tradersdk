//! Trading logic: PnL calculators, symbol formatting, position sizing, exit grid.
//!
//! This crate contains the pure-computation core of the trading system.
//! It does NOT perform any exchange API calls.

pub mod exit;
pub mod pnl;
pub mod position;
pub mod symbol;

pub use exit::{calculate_limit_orders_grid, GridOrder, GridRule};
pub use pnl::{
    calculate_aggregate_pnl, calculate_fifo_pnl, calculate_pnl, FifoMatch, FifoPnlResult, FillInfo,
    PnlResult,
};
pub use position::{calculate_position_size, calculate_split_orders, SplitOrderConfig};
pub use symbol::{extract_base_symbol, format_symbol_for_exchange, Exchange};
