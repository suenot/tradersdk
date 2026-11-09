//! MEXC service modules for REST and WebSocket operations.

mod rest;
mod ws_private;
mod ws_stream;

pub use rest::RestService;
pub use ws_private::UserDataService;
pub use ws_stream::MarketStreamService;
