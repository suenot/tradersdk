use std::time::{SystemTime, UNIX_EPOCH};

use tracing::info;

use crate::cache::MessageCache;
use crate::types::{SignalMessage, SignalResponse, SignalStats};

/// Processes incoming signals: deduplication + dispatch.
///
/// This is the core processing pipeline, ported from Python's
/// `TelegramMessageServicer`. It receives `SignalMessage` values,
/// checks them against the deduplication cache, and returns a
/// `SignalResponse` indicating whether the message was new or duplicate.
///
/// This struct is transport-agnostic: it does not know about gRPC,
/// HTTP, or any other transport. The caller is responsible for
/// receiving messages and sending responses.
pub struct SignalProcessor {
    cache: MessageCache,
    total_received: u64,
    deduplicated: u64,
    start_time_ms: i64,
}

impl SignalProcessor {
    /// Create a new signal processor.
    ///
    /// # Arguments
    /// * `cache_max_size` - Maximum number of entries in the dedup cache
    /// * `cache_ttl_seconds` - Time-to-live for cache entries in seconds
    pub fn new(cache_max_size: usize, cache_ttl_seconds: u64) -> Self {
        Self {
            cache: MessageCache::new(cache_max_size, cache_ttl_seconds),
            total_received: 0,
            deduplicated: 0,
            start_time_ms: Self::now_ms(),
        }
    }

    /// Create a new signal processor with default settings.
    ///
    /// Default: max_size=10000, ttl=2592000 (30 days).
    pub fn with_defaults() -> Self {
        Self::new(10_000, 2_592_000)
    }

    /// Get the current time in milliseconds since UNIX epoch.
    fn now_ms() -> i64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as i64
    }

    /// Process an incoming signal message.
    ///
    /// Checks whether the message is a duplicate. If it is, increments
    /// the dedup counter and returns a response with `deduplicated: true`.
    /// Otherwise, adds the message to the cache and returns a response
    /// with `deduplicated: false`.
    ///
    /// # Returns
    /// A `SignalResponse` indicating whether the message was accepted
    /// as new or was recognized as a duplicate.
    pub fn process(&mut self, msg: &SignalMessage) -> SignalResponse {
        self.total_received += 1;
        let server_timestamp_ms = Self::now_ms();

        // Check for duplicate
        if self.cache.contains(&msg.message_text) {
            self.deduplicated += 1;
            info!(
                message_id = %msg.message_id,
                city = %msg.city,
                ip = %msg.ip,
                "Duplicate message detected"
            );
            return SignalResponse {
                success: true,
                message: "Message deduplicated".to_string(),
                server_timestamp_ms,
                deduplicated: true,
            };
        }

        // Add to cache
        self.cache.add(&msg.message_text);

        info!(
            message_id = %msg.message_id,
            city = %msg.city,
            ip = %msg.ip,
            source = %msg.source,
            "New message received"
        );

        SignalResponse {
            success: true,
            message: "Message received and processed".to_string(),
            server_timestamp_ms,
            deduplicated: false,
        }
    }

    /// Get a snapshot of current statistics.
    pub fn stats(&self) -> SignalStats {
        let cache_stats = self.cache.stats();
        let uptime_ms = Self::now_ms() - self.start_time_ms;

        SignalStats {
            total_messages_received: self.total_received,
            deduplicated_count: self.deduplicated,
            cache_size: cache_stats.size,
            cache_max_size: cache_stats.max_size,
            cache_hit_rate: cache_stats.hit_rate,
            uptime_seconds: uptime_ms as f64 / 1000.0,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::SignalMessage;

    fn make_msg(text: &str) -> SignalMessage {
        SignalMessage {
            message_text: text.to_string(),
            message_id: format!("test_{}", text.len()),
            published_at_ms: 1700000000000,
            detected_at_ms: 1700000000100,
            source: "test".to_string(),
            city: "localhost".to_string(),
            ip: "127.0.0.1".to_string(),
        }
    }

    #[test]
    fn test_new_message_accepted() {
        let mut processor = SignalProcessor::new(100, 3600);
        let msg = make_msg("brand new message");

        let resp = processor.process(&msg);
        assert!(resp.success);
        assert!(!resp.deduplicated);
        assert_eq!(resp.message, "Message received and processed");
        assert!(resp.server_timestamp_ms > 0);
    }

    #[test]
    fn test_duplicate_rejected() {
        let mut processor = SignalProcessor::new(100, 3600);
        let msg = make_msg("duplicate me");

        // First time: accepted
        let resp1 = processor.process(&msg);
        assert!(!resp1.deduplicated);

        // Second time: deduplicated
        let resp2 = processor.process(&msg);
        assert!(resp2.deduplicated);
        assert!(resp2.success);
        assert_eq!(resp2.message, "Message deduplicated");
    }

    #[test]
    fn test_different_messages_accepted() {
        let mut processor = SignalProcessor::new(100, 3600);

        let resp1 = processor.process(&make_msg("message A"));
        let resp2 = processor.process(&make_msg("message B"));

        assert!(!resp1.deduplicated);
        assert!(!resp2.deduplicated);
    }

    #[test]
    fn test_stats_tracking() {
        let mut processor = SignalProcessor::new(100, 3600);

        processor.process(&make_msg("msg1"));
        processor.process(&make_msg("msg2"));
        processor.process(&make_msg("msg1")); // duplicate

        let stats = processor.stats();
        assert_eq!(stats.total_messages_received, 3);
        assert_eq!(stats.deduplicated_count, 1);
        assert_eq!(stats.cache_size, 2);
        assert_eq!(stats.cache_max_size, 100);
        assert!(stats.uptime_seconds >= 0.0);
    }

    #[test]
    fn test_with_defaults() {
        let processor = SignalProcessor::with_defaults();
        let stats = processor.stats();
        assert_eq!(stats.cache_max_size, 10_000);
        assert_eq!(stats.total_messages_received, 0);
    }
}
