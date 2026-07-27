//! Notification system for trading alerts.
//!
//! Provides Telegram and webhook notification channels
//! with a manager for routing to multiple channels.

mod base;
mod manager;
mod telegram;
mod webhook;

pub use base::*;
pub use manager::*;
pub use telegram::*;
pub use webhook::*;
