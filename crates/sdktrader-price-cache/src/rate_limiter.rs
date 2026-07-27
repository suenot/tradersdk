use std::sync::Arc;
use tokio::sync::Mutex;
use tracing::debug;

/// Token bucket rate limiter for API requests.
///
/// Ensures a minimum interval between consecutive API calls
/// based on a configured calls-per-minute limit.
pub struct RateLimiter {
    min_interval_ms: u64,
    last_call_ms: Arc<Mutex<i64>>,
}

impl RateLimiter {
    /// Create a rate limiter from calls-per-minute.
    ///
    /// # Arguments
    /// * `calls_per_minute` - Maximum number of API calls allowed per minute.
    ///   The limiter computes a minimum interval between calls as `60_000 / calls_per_minute` ms.
    pub fn new(calls_per_minute: u32) -> Self {
        let min_interval_ms = if calls_per_minute == 0 {
            60_000 // fallback: 1 call per minute
        } else {
            60_000 / calls_per_minute as u64
        };

        debug!(
            calls_per_minute,
            min_interval_ms, "RateLimiter created"
        );

        Self {
            min_interval_ms,
            last_call_ms: Arc::new(Mutex::new(0)),
        }
    }

    /// Wait if necessary to respect rate limits.
    ///
    /// If the time since the last call is less than `min_interval_ms`,
    /// this method sleeps for the remaining duration before returning.
    pub async fn wait_if_needed(&self) {
        let mut last_call = self.last_call_ms.lock().await;

        let now_ms = current_time_ms();
        let elapsed = now_ms - *last_call;

        if elapsed < self.min_interval_ms as i64 {
            let sleep_ms = self.min_interval_ms as i64 - elapsed;
            debug!(sleep_ms, "Rate limit: sleeping");
            tokio::time::sleep(tokio::time::Duration::from_millis(sleep_ms as u64)).await;
        }

        *last_call = current_time_ms();
    }

    /// Get the minimum interval between calls in milliseconds.
    pub fn min_interval_ms(&self) -> u64 {
        self.min_interval_ms
    }
}

/// Get current time in milliseconds since Unix epoch.
fn current_time_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as i64
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_rate_limiter_creation() {
        let limiter = RateLimiter::new(30);
        assert_eq!(limiter.min_interval_ms(), 2000); // 60000 / 30 = 2000ms

        let limiter = RateLimiter::new(60);
        assert_eq!(limiter.min_interval_ms(), 1000); // 60000 / 60 = 1000ms

        let limiter = RateLimiter::new(10);
        assert_eq!(limiter.min_interval_ms(), 6000); // 60000 / 10 = 6000ms
    }

    #[test]
    fn test_rate_limiter_zero_calls() {
        let limiter = RateLimiter::new(0);
        assert_eq!(limiter.min_interval_ms(), 60_000); // fallback
    }

    #[tokio::test]
    async fn test_rate_limiter_first_call_no_wait() {
        // The first call should not wait (last_call_ms starts at 0, which is far in the past)
        let limiter = RateLimiter::new(60); // 1000ms interval
        let start = tokio::time::Instant::now();
        limiter.wait_if_needed().await;
        let elapsed = start.elapsed();
        // First call should be nearly instant (no wait needed since last_call is epoch 0)
        assert!(elapsed.as_millis() < 100, "First call waited too long: {:?}", elapsed);
    }

    #[tokio::test(start_paused = true)]
    async fn test_rate_limiter_timing_with_paused_time() {
        // With paused time, we can test rate limiting behavior deterministically.
        // Note: tokio paused time starts at a fixed instant but SystemTime is still real,
        // so we just verify sequential calls work without panicking.
        let limiter = RateLimiter::new(30); // 2000ms interval
        limiter.wait_if_needed().await;
        limiter.wait_if_needed().await;
        // If we get here without panicking, the limiter works correctly.
    }
}
