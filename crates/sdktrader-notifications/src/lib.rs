//! Notification system for trading alerts.
//!
//! Provides Telegram and webhook notification channels
//! with a manager for routing to multiple channels.

mod base;
mod telegram;
mod webhook;
mod manager;

pub use base::*;
pub use telegram::*;
pub use webhook::*;
pub use manager::*;
