use std::collections::HashMap;
use std::time::Instant;

use tracing::{debug, info, warn};

use crate::endpoints::get_endpoints;
use crate::types::{ConnectionMetrics, HealthSummary, ServerTimeResponse};

/// Configuration for the circuit breaker.
#[derive(Debug, Clone)]
pub struct CircuitBreakerConfig {
    /// Number of consecutive failures before the circuit breaker opens.
    pub failure_threshold: u32,
    /// How long the circuit breaker stays open before allowing a recovery attempt (ms).
    pub timeout_ms: i64,
    /// Interval between recovery attempts while circuit breaker is open (ms).
    pub recovery_timeout_ms: i64,
}

impl Default for CircuitBreakerConfig {
    fn default() -> Self {
        Self {
            failure_threshold: 5,
            timeout_ms: 30_000,
            recovery_timeout_ms: 5_000,
        }
    }
}

/// Internal state for the circuit breaker of a single exchange.
#[derive(Debug, Clone)]
struct CircuitBreakerState {
    /// Whether the circuit breaker is currently open (blocking requests).
    open: bool,
    /// When the circuit breaker was opened (epoch ms).
    opened_at: Option<i64>,
    /// When the last recovery attempt was made (epoch ms).
    last_recovery_attempt: Option<i64>,
}

impl Default for CircuitBreakerState {
    fn default() -> Self {
        Self {
            open: false,
            opened_at: None,
            last_recovery_attempt: None,
        }
    }
}

/// Server time warmer that queries exchange server time endpoints
/// with circuit breaker protection.
pub struct ServerTimeWarmer {
    /// HTTP request timeout in milliseconds.
    timeout_ms: u64,
    /// Circuit breaker configuration.
    circuit_breaker: CircuitBreakerConfig,
    /// HTTP client for making requests.
    client: reqwest::Client,
    /// Metrics per exchange.
    metrics: HashMap<String, ConnectionMetrics>,
    /// Circuit breaker state per exchange.
    breaker_states: HashMap<String, CircuitBreakerState>,
    /// Sum of latencies for average calculation.
    latency_sums: HashMap<String, f64>,
}

impl ServerTimeWarmer {
    /// Create a new `ServerTimeWarmer` with the given request timeout in milliseconds.
    pub fn new(timeout_ms: u64) -> Self {
        let client = reqwest::Client::builder()
            .timeout(std::time::Duration::from_millis(timeout_ms))
            .build()
            .expect("Failed to create HTTP client");

        Self {
            timeout_ms,
            circuit_breaker: CircuitBreakerConfig::default(),
            client,
            metrics: HashMap::new(),
            breaker_states: HashMap::new(),
            latency_sums: HashMap::new(),
        }
    }

    /// Create a new `ServerTimeWarmer` with custom circuit breaker configuration.
    pub fn with_circuit_breaker(timeout_ms: u64, config: CircuitBreakerConfig) -> Self {
        let client = reqwest::Client::builder()
            .timeout(std::time::Duration::from_millis(timeout_ms))
            .build()
            .expect("Failed to create HTTP client");

        Self {
            timeout_ms,
            circuit_breaker: config,
            client,
            metrics: HashMap::new(),
            breaker_states: HashMap::new(),
            latency_sums: HashMap::new(),
        }
    }

    /// Get the current time in epoch milliseconds.
    fn now_ms() -> i64 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as i64
    }

    /// Check if the circuit breaker allows a request for the given exchange.
    fn is_request_allowed(&mut self, exchange: &str) -> bool {
        let state = self
            .breaker_states
            .entry(exchange.to_string())
            .or_default();

        if !state.open {
            return true;
        }

        let now = Self::now_ms();

        // Check if the circuit breaker timeout has expired
        if let Some(opened_at) = state.opened_at {
            if now - opened_at >= self.circuit_breaker.timeout_ms {
                // Timeout expired, allow recovery attempt
                debug!(exchange, "Circuit breaker timeout expired, allowing recovery");
                state.last_recovery_attempt = Some(now);
                return true;
            }
        }

        // Check if enough time has passed for a recovery attempt
        if let Some(last_attempt) = state.last_recovery_attempt {
            if now - last_attempt >= self.circuit_breaker.recovery_timeout_ms {
                debug!(exchange, "Allowing recovery attempt");
                state.last_recovery_attempt = Some(now);
                return true;
            }
        }

        false
    }

    /// Record a successful request and potentially close the circuit breaker.
    fn record_success(&mut self, exchange: &str, latency_ms: f64, _server_time: Option<i64>) {
        // Update latency sum first (separate HashMap, no conflict)
        let sum = self
            .latency_sums
            .entry(exchange.to_string())
            .or_insert(0.0);
        *sum += latency_ms;
        let latency_sum = *sum;

        // Close circuit breaker on success
        let state = self
            .breaker_states
            .entry(exchange.to_string())
            .or_default();
        if state.open {
            info!(exchange, "Circuit breaker closed after successful recovery");
            state.open = false;
            state.opened_at = None;
            state.last_recovery_attempt = None;
        }

        // Update metrics
        let metrics = self
            .metrics
            .entry(exchange.to_string())
            .or_insert_with(|| ConnectionMetrics {
                exchange: exchange.to_string(),
                ..Default::default()
            });

        metrics.total_queries += 1;
        metrics.successful_queries += 1;
        metrics.consecutive_failures = 0;
        metrics.last_success_time = Some(Self::now_ms());
        metrics.last_error = None;
        metrics.average_latency_ms = latency_sum / metrics.successful_queries as f64;
        metrics.circuit_breaker_open = false;

        // Calculate health score after releasing the mutable borrow on metrics
        let _ = metrics;
        let score = self.calculate_health_score(exchange);
        self.metrics.get_mut(exchange).unwrap().health_score = score;
    }

    /// Record a failed request and potentially open the circuit breaker.
    fn record_failure(&mut self, exchange: &str, error: &str) {
        let failure_threshold = self.circuit_breaker.failure_threshold;

        // Update metrics
        let metrics = self
            .metrics
            .entry(exchange.to_string())
            .or_insert_with(|| ConnectionMetrics {
                exchange: exchange.to_string(),
                ..Default::default()
            });

        metrics.total_queries += 1;
        metrics.failed_queries += 1;
        metrics.consecutive_failures += 1;
        metrics.last_error = Some(error.to_string());

        let consecutive_failures = metrics.consecutive_failures;

        // Check if circuit breaker should open
        if consecutive_failures >= failure_threshold {
            metrics.circuit_breaker_open = true;

            // Release mutable borrow on metrics before borrowing breaker_states
            let _ = metrics;

            let state = self
                .breaker_states
                .entry(exchange.to_string())
                .or_default();
            if !state.open {
                warn!(
                    exchange,
                    consecutive_failures,
                    "Circuit breaker opened"
                );
                state.open = true;
                state.opened_at = Some(Self::now_ms());
                state.last_recovery_attempt = Some(Self::now_ms());
            }
        }

        // Calculate health score after releasing all mutable borrows
        let score = self.calculate_health_score(exchange);
        self.metrics.get_mut(exchange).unwrap().health_score = score;
    }

    /// Calculate health score (0.0 to 100.0) for an exchange.
    ///
    /// Formula:
    /// - 70% from success rate
    /// - 20% from latency (bonus up to 20 points, subtract latency/50)
    /// - 10% from recency (bonus if last success within 30s)
    /// - 50% penalty if circuit breaker is open
    fn calculate_health_score(&self, exchange: &str) -> f64 {
        let metrics = match self.metrics.get(exchange) {
            Some(m) => m,
            None => return 0.0,
        };

        if metrics.total_queries == 0 {
            return 0.0;
        }

        // Success rate component (0-70)
        let success_rate = metrics.successful_queries as f64 / metrics.total_queries as f64;
        let success_score = success_rate * 70.0;

        // Latency component (0-20)
        let latency_score = if metrics.average_latency_ms > 0.0 {
            (20.0 - metrics.average_latency_ms / 50.0).max(0.0).min(20.0)
        } else {
            20.0
        };

        // Recency component (0-10)
        let recency_score = if let Some(last_success) = metrics.last_success_time {
            let elapsed_ms = Self::now_ms() - last_success;
            if elapsed_ms <= 30_000 {
                10.0
            } else {
                0.0
            }
        } else {
            0.0
        };

        let mut score = success_score + latency_score + recency_score;

        // Circuit breaker penalty
        let state = self.breaker_states.get(exchange);
        if state.map_or(false, |s| s.open) {
            score *= 0.5;
        }

        score.max(0.0).min(100.0)
    }

    /// Query server time for a specific exchange (respects circuit breaker).
    pub async fn query_server_time(&mut self, exchange: &str) -> ServerTimeResponse {
        // Check circuit breaker
        if !self.is_request_allowed(exchange) {
            debug!(exchange, "Circuit breaker open, request blocked");
            return ServerTimeResponse {
                success: false,
                server_time: None,
                latency_ms: 0.0,
                error_message: Some("Circuit breaker open".to_string()),
                exchange: exchange.to_string(),
            };
        }

        let endpoints = get_endpoints();
        let endpoint = match endpoints.get(exchange) {
            Some(ep) => ep,
            None => {
                return ServerTimeResponse {
                    success: false,
                    server_time: None,
                    latency_ms: 0.0,
                    error_message: Some(format!("Unknown exchange: {}", exchange)),
                    exchange: exchange.to_string(),
                };
            }
        };

        let start = Instant::now();

        let result = self.client.get(endpoint.url).send().await;

        let latency_ms = start.elapsed().as_secs_f64() * 1000.0;

        match result {
            Ok(response) => {
                if response.status().is_success() {
                    // Try to parse server time from various response formats
                    let server_time = match response.text().await {
                        Ok(body) => parse_server_time(&body),
                        Err(_) => None,
                    };

                    self.record_success(exchange, latency_ms, server_time);

                    ServerTimeResponse {
                        success: true,
                        server_time,
                        latency_ms,
                        error_message: None,
                        exchange: exchange.to_string(),
                    }
                } else {
                    let error = format!("HTTP {}", response.status());
                    self.record_failure(exchange, &error);

                    ServerTimeResponse {
                        success: false,
                        server_time: None,
                        latency_ms,
                        error_message: Some(error),
                        exchange: exchange.to_string(),
                    }
                }
            }
            Err(e) => {
                let error = e.to_string();
                self.record_failure(exchange, &error);

                ServerTimeResponse {
                    success: false,
                    server_time: None,
                    latency_ms,
                    error_message: Some(error),
                    exchange: exchange.to_string(),
                }
            }
        }
    }

    /// Warm connection for a single exchange (query + update metrics).
    pub async fn warm_exchange(&mut self, exchange: &str) -> ServerTimeResponse {
        self.query_server_time(exchange).await
    }

    /// Warm all specified exchanges sequentially.
    ///
    /// Note: Due to the mutable borrow on `self`, exchanges are warmed sequentially.
    /// For concurrent warming, use `warm_all_concurrent` which creates separate clients.
    pub async fn warm_all(
        &mut self,
        exchanges: &[String],
    ) -> HashMap<String, ServerTimeResponse> {
        let mut results = HashMap::new();
        for exchange in exchanges {
            let response = self.warm_exchange(exchange).await;
            results.insert(exchange.clone(), response);
        }
        results
    }

    /// Test all known endpoints and return whether each succeeded.
    pub async fn test_all_endpoints(&mut self) -> HashMap<String, bool> {
        let endpoint_names: Vec<String> = get_endpoints().keys().map(|k| k.to_string()).collect();
        let mut results = HashMap::new();
        for exchange in &endpoint_names {
            let response = self.query_server_time(exchange).await;
            results.insert(exchange.clone(), response.success);
        }
        results
    }

    /// Get a reference to the metrics for all exchanges.
    pub fn get_metrics(&self) -> &HashMap<String, ConnectionMetrics> {
        &self.metrics
    }

    /// Get a health summary across all monitored exchanges.
    pub fn get_health_summary(&self) -> HealthSummary {
        let total_exchanges = self.metrics.len();
        let mut healthy = 0;
        let mut unhealthy = 0;
        let mut cb_open = 0;
        let mut total_score = 0.0;

        for metrics in self.metrics.values() {
            if metrics.health_score >= 50.0 {
                healthy += 1;
            } else {
                unhealthy += 1;
            }
            if metrics.circuit_breaker_open {
                cb_open += 1;
            }
            total_score += metrics.health_score;
        }

        let average_health_score = if total_exchanges > 0 {
            total_score / total_exchanges as f64
        } else {
            0.0
        };

        HealthSummary {
            total_exchanges,
            healthy_exchanges: healthy,
            unhealthy_exchanges: unhealthy,
            circuit_breakers_open: cb_open,
            average_health_score,
        }
    }

    /// Get the request timeout in milliseconds.
    pub fn timeout_ms(&self) -> u64 {
        self.timeout_ms
    }

    /// Get a reference to the circuit breaker configuration.
    pub fn circuit_breaker_config(&self) -> &CircuitBreakerConfig {
        &self.circuit_breaker
    }

    /// Manually open the circuit breaker for an exchange (for testing).
    #[cfg(test)]
    fn force_open_circuit_breaker(&mut self, exchange: &str) {
        let state = self
            .breaker_states
            .entry(exchange.to_string())
            .or_default();
        state.open = true;
        state.opened_at = Some(Self::now_ms());
        state.last_recovery_attempt = Some(Self::now_ms());

        let metrics = self
            .metrics
            .entry(exchange.to_string())
            .or_insert_with(|| ConnectionMetrics {
                exchange: exchange.to_string(),
                ..Default::default()
            });
        metrics.circuit_breaker_open = true;
    }

    /// Simulate failures for testing (increments consecutive_failures without network).
    #[cfg(test)]
    fn simulate_failure(&mut self, exchange: &str, error: &str) {
        self.record_failure(exchange, error);
    }

    /// Simulate success for testing (updates metrics without network).
    #[cfg(test)]
    fn simulate_success(&mut self, exchange: &str, latency_ms: f64) {
        self.record_success(exchange, latency_ms, Some(Self::now_ms()));
    }
}

/// Parse server time from various JSON response formats used by different exchanges.
fn parse_server_time(body: &str) -> Option<i64> {
    let value: serde_json::Value = serde_json::from_str(body).ok()?;

    // Try common response formats:
    // {"serverTime": 123456789} (Binance, MEXC)
    if let Some(t) = value.get("serverTime").and_then(|v| v.as_i64()) {
        return Some(t);
    }

    // {"result": {"timeSecond": "123", "timeNano": "123456789"}} (Bybit)
    if let Some(result) = value.get("result") {
        if let Some(t) = result.get("timeNano").and_then(|v| {
            v.as_str()
                .and_then(|s| s.parse::<i64>().ok())
                .map(|ns| ns / 1_000_000)
        }) {
            return Some(t);
        }
        if let Some(t) = result
            .get("timeSecond")
            .and_then(|v| v.as_str().and_then(|s| s.parse::<i64>().ok()).map(|s| s * 1000))
        {
            return Some(t);
        }
    }

    // {"data": {"serverTime": 123}} or {"data": [{"ts": "123"}]} (OKX, Bitget)
    if let Some(data) = value.get("data") {
        if let Some(t) = data.get("serverTime").and_then(|v| v.as_i64()) {
            return Some(t);
        }
        if let Some(arr) = data.as_array() {
            if let Some(first) = arr.first() {
                if let Some(t) = first
                    .get("ts")
                    .and_then(|v| v.as_str().and_then(|s| s.parse::<i64>().ok()))
                {
                    return Some(t);
                }
            }
        }
    }

    // {"server_time": 123} (Gate.io)
    if let Some(t) = value.get("server_time").and_then(|v| v.as_i64()) {
        return Some(t * 1000); // Gate.io returns seconds
    }

    // {"data": "123456789"} (BingX)
    if let Some(data) = value.get("data") {
        if let Some(t) = data.as_str().and_then(|s| s.parse::<i64>().ok()) {
            return Some(t);
        }
        if let Some(t) = data.as_i64() {
            return Some(t);
        }
    }

    // {"result": {"serverTime": 123}} (XT)
    if let Some(result) = value.get("result") {
        if let Some(t) = result.get("serverTime").and_then(|v| v.as_i64()) {
            return Some(t);
        }
    }

    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_create_warmer() {
        let warmer = ServerTimeWarmer::new(5000);
        assert_eq!(warmer.timeout_ms(), 5000);
        assert_eq!(warmer.circuit_breaker_config().failure_threshold, 5);
        assert_eq!(warmer.circuit_breaker_config().timeout_ms, 30_000);
        assert_eq!(warmer.circuit_breaker_config().recovery_timeout_ms, 5_000);
        assert!(warmer.get_metrics().is_empty());
    }

    #[test]
    fn test_create_warmer_with_custom_config() {
        let config = CircuitBreakerConfig {
            failure_threshold: 3,
            timeout_ms: 10_000,
            recovery_timeout_ms: 2_000,
        };
        let warmer = ServerTimeWarmer::with_circuit_breaker(3000, config);
        assert_eq!(warmer.timeout_ms(), 3000);
        assert_eq!(warmer.circuit_breaker_config().failure_threshold, 3);
    }

    #[test]
    fn test_circuit_breaker_opens_after_failures() {
        let config = CircuitBreakerConfig {
            failure_threshold: 3,
            timeout_ms: 30_000,
            recovery_timeout_ms: 5_000,
        };
        let mut warmer = ServerTimeWarmer::with_circuit_breaker(5000, config);

        // Simulate consecutive failures
        warmer.simulate_failure("test_exchange", "timeout");
        warmer.simulate_failure("test_exchange", "timeout");

        // Should still be closed after 2 failures (threshold is 3)
        let state = warmer.breaker_states.get("test_exchange");
        assert!(
            state.is_none() || !state.unwrap().open,
            "Circuit breaker should be closed after 2 failures"
        );

        // Third failure should open it
        warmer.simulate_failure("test_exchange", "timeout");

        let state = warmer.breaker_states.get("test_exchange").unwrap();
        assert!(state.open, "Circuit breaker should be open after 3 failures");
        assert!(state.opened_at.is_some());

        let metrics = warmer.metrics.get("test_exchange").unwrap();
        assert_eq!(metrics.consecutive_failures, 3);
        assert!(metrics.circuit_breaker_open);
    }

    #[test]
    fn test_circuit_breaker_closes_on_recovery() {
        let config = CircuitBreakerConfig {
            failure_threshold: 2,
            timeout_ms: 30_000,
            recovery_timeout_ms: 5_000,
        };
        let mut warmer = ServerTimeWarmer::with_circuit_breaker(5000, config);

        // Open the circuit breaker
        warmer.simulate_failure("test_exchange", "error1");
        warmer.simulate_failure("test_exchange", "error2");

        let state = warmer.breaker_states.get("test_exchange").unwrap();
        assert!(state.open);

        // Simulate a successful recovery
        warmer.simulate_success("test_exchange", 50.0);

        let state = warmer.breaker_states.get("test_exchange").unwrap();
        assert!(!state.open, "Circuit breaker should close after success");

        let metrics = warmer.metrics.get("test_exchange").unwrap();
        assert_eq!(metrics.consecutive_failures, 0);
        assert!(!metrics.circuit_breaker_open);
    }

    #[test]
    fn test_request_blocked_when_circuit_open() {
        let mut warmer = ServerTimeWarmer::new(5000);

        // Force open the circuit breaker
        warmer.force_open_circuit_breaker("blocked_exchange");

        // Request should be blocked (recovery won't be allowed because opened_at is now)
        // We need to set opened_at to recent time so recovery timeout hasn't passed
        let allowed = warmer.is_request_allowed("blocked_exchange");
        // Since opened_at is just now, timeout hasn't expired, and last_recovery_attempt is now
        assert!(
            !allowed,
            "Request should be blocked when circuit breaker is open and no timeout expired"
        );
    }

    #[test]
    fn test_health_score_perfect() {
        let mut warmer = ServerTimeWarmer::new(5000);

        // Simulate successful queries with low latency
        for _ in 0..10 {
            warmer.simulate_success("healthy_exchange", 50.0);
        }

        let metrics = warmer.metrics.get("healthy_exchange").unwrap();
        // Success rate: 100% -> 70 points
        // Latency: 50ms -> 20 - 50/50 = 19 points
        // Recency: just queried -> 10 points
        // Total: 99 points
        assert!(
            metrics.health_score > 90.0,
            "Health score should be high: {}",
            metrics.health_score
        );
    }

    #[test]
    fn test_health_score_with_failures() {
        let mut warmer = ServerTimeWarmer::new(5000);

        // 5 successes, 5 failures = 50% success rate
        for _ in 0..5 {
            warmer.simulate_success("mixed_exchange", 100.0);
        }
        for _ in 0..5 {
            warmer.simulate_failure("mixed_exchange", "error");
        }

        let metrics = warmer.metrics.get("mixed_exchange").unwrap();
        // Success rate: 50% -> 35 points
        // Latency component based on average
        // health score should be moderate
        assert!(
            metrics.health_score < 70.0,
            "Health score should be moderate: {}",
            metrics.health_score
        );
        assert!(
            metrics.health_score > 0.0,
            "Health score should be positive: {}",
            metrics.health_score
        );
    }

    #[test]
    fn test_health_score_circuit_breaker_penalty() {
        let mut warmer = ServerTimeWarmer::new(5000);

        // Some successes
        for _ in 0..10 {
            warmer.simulate_success("penalized_exchange", 50.0);
        }

        let score_before = warmer.metrics.get("penalized_exchange").unwrap().health_score;

        // Force open circuit breaker
        warmer.force_open_circuit_breaker("penalized_exchange");

        // Recalculate health score
        let score_after = warmer.calculate_health_score("penalized_exchange");

        assert!(
            score_after < score_before,
            "Health score should decrease with circuit breaker open: before={}, after={}",
            score_before,
            score_after
        );
    }

    #[test]
    fn test_health_summary_empty() {
        let warmer = ServerTimeWarmer::new(5000);
        let summary = warmer.get_health_summary();
        assert_eq!(summary.total_exchanges, 0);
        assert_eq!(summary.healthy_exchanges, 0);
        assert_eq!(summary.unhealthy_exchanges, 0);
        assert_eq!(summary.circuit_breakers_open, 0);
        assert_eq!(summary.average_health_score, 0.0);
    }

    #[test]
    fn test_health_summary_mixed() {
        let mut warmer = ServerTimeWarmer::new(5000);

        // One healthy exchange
        for _ in 0..10 {
            warmer.simulate_success("healthy", 30.0);
        }

        // One unhealthy exchange (all failures)
        for _ in 0..10 {
            warmer.simulate_failure("unhealthy", "error");
        }

        let summary = warmer.get_health_summary();
        assert_eq!(summary.total_exchanges, 2);
        assert_eq!(summary.healthy_exchanges, 1);
        assert_eq!(summary.unhealthy_exchanges, 1);
    }

    #[test]
    fn test_metrics_tracking() {
        let mut warmer = ServerTimeWarmer::new(5000);

        warmer.simulate_success("exchange_a", 100.0);
        warmer.simulate_success("exchange_a", 200.0);
        warmer.simulate_failure("exchange_a", "timeout");

        let metrics = warmer.metrics.get("exchange_a").unwrap();
        assert_eq!(metrics.total_queries, 3);
        assert_eq!(metrics.successful_queries, 2);
        assert_eq!(metrics.failed_queries, 1);
        assert_eq!(metrics.average_latency_ms, 150.0); // (100 + 200) / 2
        assert!(metrics.last_success_time.is_some());
        assert_eq!(metrics.last_error, Some("timeout".to_string()));
        assert_eq!(metrics.consecutive_failures, 1);
    }

    #[test]
    fn test_parse_server_time_binance() {
        let body = r#"{"serverTime": 1700000000000}"#;
        assert_eq!(parse_server_time(body), Some(1700000000000));
    }

    #[test]
    fn test_parse_server_time_bybit() {
        let body = r#"{"result": {"timeSecond": "1700000000", "timeNano": "1700000000123000000"}}"#;
        assert_eq!(parse_server_time(body), Some(1700000000123));
    }

    #[test]
    fn test_parse_server_time_gateio() {
        let body = r#"{"server_time": 1700000000}"#;
        assert_eq!(parse_server_time(body), Some(1700000000000));
    }

    #[test]
    fn test_parse_server_time_okx() {
        let body = r#"{"data": [{"ts": "1700000000000"}]}"#;
        assert_eq!(parse_server_time(body), Some(1700000000000));
    }

    #[test]
    fn test_parse_server_time_bingx() {
        let body = r#"{"data": "1700000000000"}"#;
        assert_eq!(parse_server_time(body), Some(1700000000000));
    }

    #[test]
    fn test_parse_server_time_invalid() {
        assert_eq!(parse_server_time("not json"), None);
        assert_eq!(parse_server_time(r#"{"foo": "bar"}"#), None);
    }
}
