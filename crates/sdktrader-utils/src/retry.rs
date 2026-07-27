//! Retry utilities with exponential backoff.

use std::future::Future;
use std::time::Duration;
use tokio::time::sleep;
use tracing::warn;

/// Configuration for retry behavior.
#[derive(Debug, Clone)]
pub struct RetryConfig {
    /// Maximum number of retry attempts.
    pub max_retries: u32,
    /// Initial delay between retries (ms).
    pub base_delay_ms: u64,
    /// Maximum delay between retries (ms).
    pub max_delay_ms: u64,
    /// Exponential backoff multiplier.
    pub multiplier: f64,
    /// Random jitter factor (0.0 - 1.0).
    pub jitter: f64,
}

impl Default for RetryConfig {
    fn default() -> Self {
        Self {
            max_retries: 3,
            base_delay_ms: 100,
            max_delay_ms: 10000,
            multiplier: 2.0,
            jitter: 0.1,
        }
    }
}

impl RetryConfig {
    /// Create a new retry config.
    pub fn new(max_retries: u32) -> Self {
        Self {
            max_retries,
            ..Default::default()
        }
    }

    /// Set base delay.
    pub fn with_base_delay(mut self, delay_ms: u64) -> Self {
        self.base_delay_ms = delay_ms;
        self
    }

    /// Set max delay.
    pub fn with_max_delay(mut self, delay_ms: u64) -> Self {
        self.max_delay_ms = delay_ms;
        self
    }

    /// Set multiplier.
    pub fn with_multiplier(mut self, multiplier: f64) -> Self {
        self.multiplier = multiplier;
        self
    }

    /// Calculate delay for a given attempt.
    pub fn delay_for_attempt(&self, attempt: u32) -> Duration {
        let delay = self.base_delay_ms as f64 * self.multiplier.powi(attempt as i32);
        let delay = delay.min(self.max_delay_ms as f64);

        // Add jitter
        let jitter_range = delay * self.jitter;
        let jitter = rand_jitter() * jitter_range;
        let final_delay = delay + jitter;

        Duration::from_millis(final_delay as u64)
    }
}

/// Simple pseudo-random jitter (deterministic for testing).
fn rand_jitter() -> f64 {
    use std::time::SystemTime;
    let nanos = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .map(|d| d.subsec_nanos())
        .unwrap_or(0);
    (nanos % 1000) as f64 / 1000.0
}

/// Retry a fallible async operation with exponential backoff.
///
/// # Example
/// ```rust,no_run
/// use sdktrader_utils::{retry_with_backoff, RetryConfig};
///
/// async fn example() -> Result<(), Box<dyn std::error::Error>> {
///     let config = RetryConfig::new(3);
///
///     let result = retry_with_backoff(&config, || async {
///         // Some operation that might fail
///         Ok::<_, Box<dyn std::error::Error>>(42)
///     }).await?;
///
///     assert_eq!(result, 42);
///     Ok(())
/// }
/// ```
pub async fn retry_with_backoff<F, Fut, T, E>(
    config: &RetryConfig,
    mut operation: F,
) -> Result<T, E>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<T, E>>,
    E: std::fmt::Display,
{
    let mut attempt = 0;

    loop {
        match operation().await {
            Ok(result) => return Ok(result),
            Err(e) if attempt < config.max_retries => {
                let delay = config.delay_for_attempt(attempt);
                warn!(
                    "Attempt {} failed: {}. Retrying in {:?}...",
                    attempt + 1,
                    e,
                    delay
                );
                sleep(delay).await;
                attempt += 1;
            }
            Err(e) => {
                warn!(
                    "All {} attempts failed. Last error: {}",
                    config.max_retries + 1,
                    e
                );
                return Err(e);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_delay_calculation() {
        let config = RetryConfig {
            base_delay_ms: 100,
            max_delay_ms: 10000,
            multiplier: 2.0,
            jitter: 0.0,
            ..Default::default()
        };

        // With 0 jitter, delays should be predictable
        let delay0 = config.delay_for_attempt(0);
        let delay1 = config.delay_for_attempt(1);
        let delay2 = config.delay_for_attempt(2);

        assert!(delay0.as_millis() >= 100 && delay0.as_millis() <= 110);
        assert!(delay1.as_millis() >= 200 && delay1.as_millis() <= 220);
        assert!(delay2.as_millis() >= 400 && delay2.as_millis() <= 440);
    }

    #[tokio::test]
    async fn test_retry_success_first_try() {
        let config = RetryConfig::new(3);
        let mut calls = 0;

        let result = retry_with_backoff(&config, || {
            calls += 1;
            async { Ok::<_, &str>(42) }
        })
        .await;

        assert_eq!(result, Ok(42));
        assert_eq!(calls, 1);
    }
}
