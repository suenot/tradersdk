//! Exchange error types.

use thiserror::Error;

/// Errors that can occur when interacting with exchanges.
#[derive(Error, Debug)]
pub enum ExchangeError {
    /// Network error during API call.
    #[error("Network error: {0}")]
    Network(#[from] reqwest::Error),

    /// Authentication failed.
    #[error("Authentication error: {0}")]
    Authentication(String),

    /// Invalid API response.
    #[error("Invalid response: {0}")]
    InvalidResponse(String),

    /// Rate limit exceeded.
    #[error("Rate limit exceeded, retry after {retry_after_ms}ms")]
    RateLimited { retry_after_ms: u64 },

    /// Order rejected by exchange.
    #[error("Order rejected: {0}")]
    OrderRejected(String),

    /// Insufficient funds.
    #[error("Insufficient funds: {0}")]
    InsufficientFunds(String),

    /// Symbol not found.
    #[error("Symbol not found: {0}")]
    SymbolNotFound(String),

    /// Invalid order parameters.
    #[error("Invalid order: {0}")]
    InvalidOrder(String),

    /// Position not found.
    #[error("Position not found: {0}")]
    PositionNotFound(String),

    /// WebSocket error.
    #[error("WebSocket error: {0}")]
    WebSocket(String),

    /// Exchange-specific error.
    #[error("{exchange} error ({code}): {message}")]
    ExchangeSpecific {
        exchange: String,
        code: i32,
        message: String,
    },

    /// Generic error.
    #[error("{0}")]
    Other(String),
}

impl ExchangeError {
    /// Create an authentication error.
    pub fn auth(msg: impl Into<String>) -> Self {
        Self::Authentication(msg.into())
    }

    /// Create an invalid response error.
    pub fn invalid_response(msg: impl Into<String>) -> Self {
        Self::InvalidResponse(msg.into())
    }

    /// Create a rate limited error.
    pub fn rate_limited(retry_after_ms: u64) -> Self {
        Self::RateLimited { retry_after_ms }
    }

    /// Create an order rejected error.
    pub fn order_rejected(msg: impl Into<String>) -> Self {
        Self::OrderRejected(msg.into())
    }

    /// Create an insufficient funds error.
    pub fn insufficient_funds(msg: impl Into<String>) -> Self {
        Self::InsufficientFunds(msg.into())
    }

    /// Create a symbol not found error.
    pub fn symbol_not_found(symbol: impl Into<String>) -> Self {
        Self::SymbolNotFound(symbol.into())
    }

    /// Create an invalid order error.
    pub fn invalid_order(msg: impl Into<String>) -> Self {
        Self::InvalidOrder(msg.into())
    }

    /// Create a WebSocket error.
    pub fn websocket(msg: impl Into<String>) -> Self {
        Self::WebSocket(msg.into())
    }

    /// Create an exchange-specific error.
    pub fn exchange_error(exchange: &str, code: i32, message: impl Into<String>) -> Self {
        Self::ExchangeSpecific {
            exchange: exchange.to_string(),
            code,
            message: message.into(),
        }
    }

    /// Check if error is retryable.
    pub fn is_retryable(&self) -> bool {
        matches!(
            self,
            Self::Network(_) | Self::RateLimited { .. } | Self::WebSocket(_)
        )
    }

    /// Check if error is due to rate limiting.
    pub fn is_rate_limited(&self) -> bool {
        matches!(self, Self::RateLimited { .. })
    }

    /// Get retry delay if rate limited.
    pub fn retry_after(&self) -> Option<u64> {
        match self {
            Self::RateLimited { retry_after_ms } => Some(*retry_after_ms),
            _ => None,
        }
    }
}

impl From<String> for ExchangeError {
    fn from(s: String) -> Self {
        Self::Other(s)
    }
}

impl From<&str> for ExchangeError {
    fn from(s: &str) -> Self {
        Self::Other(s.to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_error_is_retryable() {
        let rate_limited = ExchangeError::rate_limited(1000);
        assert!(rate_limited.is_retryable());
        assert!(rate_limited.is_rate_limited());
        assert_eq!(rate_limited.retry_after(), Some(1000));

        let auth_error = ExchangeError::auth("invalid key");
        assert!(!auth_error.is_retryable());
    }
}
