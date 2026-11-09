use serde::Serialize;

/// Response from a server time query to an exchange.
#[derive(Debug, Clone, Serialize)]
pub struct ServerTimeResponse {
    /// Whether the query succeeded.
    pub success: bool,
    /// Server timestamp in milliseconds (if successful).
    pub server_time: Option<i64>,
    /// Round-trip latency in milliseconds.
    pub latency_ms: f64,
    /// Error message (if failed).
    pub error_message: Option<String>,
    /// Exchange name.
    pub exchange: String,
}

/// Aggregated connection metrics for a single exchange.
#[derive(Debug, Clone, Default, Serialize)]
pub struct ConnectionMetrics {
    /// Exchange name.
    pub exchange: String,
    /// Total number of queries made.
    pub total_queries: u64,
    /// Number of successful queries.
    pub successful_queries: u64,
    /// Number of failed queries.
    pub failed_queries: u64,
    /// Average latency in milliseconds across all successful queries.
    pub average_latency_ms: f64,
    /// Timestamp of last successful query (epoch ms).
    pub last_success_time: Option<i64>,
    /// Last error message encountered.
    pub last_error: Option<String>,
    /// Health score from 0.0 to 100.0.
    pub health_score: f64,
    /// Number of consecutive failures.
    pub consecutive_failures: u32,
    /// Whether the circuit breaker is currently open.
    pub circuit_breaker_open: bool,
}

/// A unique exchange account identified by API credentials.
#[derive(Debug, Clone, Serialize)]
pub struct UniqueAccount {
    /// Exchange name.
    pub exchange: String,
    /// Hashed account identifier (first 16 hex chars of SHA-256).
    pub account_id: String,
    /// Names of bots that share this account.
    pub bot_names: Vec<String>,
}

/// Summary of health across all monitored exchanges.
#[derive(Debug, Clone, Serialize)]
pub struct HealthSummary {
    /// Total number of exchanges being monitored.
    pub total_exchanges: usize,
    /// Number of healthy exchanges (health_score >= 50).
    pub healthy_exchanges: usize,
    /// Number of unhealthy exchanges (health_score < 50).
    pub unhealthy_exchanges: usize,
    /// Number of exchanges with open circuit breakers.
    pub circuit_breakers_open: usize,
    /// Average health score across all exchanges.
    pub average_health_score: f64,
}

/// Statistics about detected accounts.
#[derive(Debug, Clone, Serialize)]
pub struct AccountStats {
    /// Total number of unique accounts.
    pub total_accounts: usize,
    /// Total number of bots.
    pub total_bots: usize,
    /// Number of accounts per exchange.
    pub accounts_per_exchange: std::collections::HashMap<String, usize>,
}
