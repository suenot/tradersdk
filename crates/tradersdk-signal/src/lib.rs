//! # tradersdk-signal
//!
//! Signal service: message deduplication, cache, and signal types.
//!
//! This crate ports the core logic from the Python `signal_service/grpc_signal_server.py`
//! into a transport-agnostic Rust library. It provides:
//!
//! - **Message types** (`SignalMessage`, `SignalResponse`, `SignalStats`) for the signal pipeline
//! - **LRU deduplication cache** (`MessageCache`) with SHA-256 hashing and TTL expiry
//! - **Signal processor** (`SignalProcessor`) that ties dedup + stats together
//!
//! The crate intentionally avoids any transport dependency (no tonic/gRPC, no HTTP).
//! Consumers can wrap `SignalProcessor` with any transport layer they choose.
//!
//! ## Quick start
//!
//! ```rust
//! use tradersdk_signal::{SignalProcessor, SignalMessage};
//!
//! let mut processor = SignalProcessor::with_defaults();
//!
//! let msg = SignalMessage {
//!     message_text: "BTC listing on Binance".to_string(),
//!     message_id: "tg_123".to_string(),
//!     published_at_ms: 1700000000000,
//!     detected_at_ms: 1700000000100,
//!     source: "telegram".to_string(),
//!     city: "Frankfurt".to_string(),
//!     ip: "10.0.0.1".to_string(),
//! };
//!
//! let resp = processor.process(&msg);
//! assert!(!resp.deduplicated);
//!
//! // Second time is a duplicate
//! let resp2 = processor.process(&msg);
//! assert!(resp2.deduplicated);
//! ```

pub mod cache;
pub mod processor;
pub mod types;

// Re-export main types for convenience
pub use cache::{CacheStats, MessageCache};
pub use processor::SignalProcessor;
pub use types::{SignalMessage, SignalResponse, SignalStats};

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_signal_message_serialization_roundtrip() {
        let msg = SignalMessage {
            message_text: "BTC listing announcement".to_string(),
            message_id: "tg_42".to_string(),
            published_at_ms: 1700000000000,
            detected_at_ms: 1700000000100,
            source: "telegram".to_string(),
            city: "Frankfurt".to_string(),
            ip: "10.0.0.1".to_string(),
        };

        let json = serde_json::to_string(&msg).unwrap();
        let deserialized: SignalMessage = serde_json::from_str(&json).unwrap();

        assert_eq!(deserialized.message_text, msg.message_text);
        assert_eq!(deserialized.message_id, msg.message_id);
        assert_eq!(deserialized.published_at_ms, msg.published_at_ms);
        assert_eq!(deserialized.detected_at_ms, msg.detected_at_ms);
        assert_eq!(deserialized.source, msg.source);
        assert_eq!(deserialized.city, msg.city);
        assert_eq!(deserialized.ip, msg.ip);
    }

    #[test]
    fn test_signal_response_serialization_roundtrip() {
        let resp = SignalResponse {
            success: true,
            message: "Message received and processed".to_string(),
            server_timestamp_ms: 1700000000200,
            deduplicated: false,
        };

        let json = serde_json::to_string(&resp).unwrap();
        let deserialized: SignalResponse = serde_json::from_str(&json).unwrap();

        assert_eq!(deserialized.success, resp.success);
        assert_eq!(deserialized.message, resp.message);
        assert_eq!(deserialized.server_timestamp_ms, resp.server_timestamp_ms);
        assert_eq!(deserialized.deduplicated, resp.deduplicated);
    }

    #[test]
    fn test_signal_stats_default() {
        let stats = SignalStats::default();
        assert_eq!(stats.total_messages_received, 0);
        assert_eq!(stats.deduplicated_count, 0);
        assert_eq!(stats.cache_size, 0);
        assert_eq!(stats.cache_max_size, 0);
        assert_eq!(stats.cache_hit_rate, 0.0);
        assert_eq!(stats.uptime_seconds, 0.0);
    }

    #[test]
    fn test_signal_stats_serialization_roundtrip() {
        let stats = SignalStats {
            total_messages_received: 42,
            deduplicated_count: 10,
            cache_size: 32,
            cache_max_size: 10000,
            cache_hit_rate: 0.238,
            uptime_seconds: 3600.5,
        };

        let json = serde_json::to_string(&stats).unwrap();
        let deserialized: SignalStats = serde_json::from_str(&json).unwrap();

        assert_eq!(
            deserialized.total_messages_received,
            stats.total_messages_received
        );
        assert_eq!(deserialized.deduplicated_count, stats.deduplicated_count);
        assert_eq!(deserialized.cache_size, stats.cache_size);
        assert_eq!(deserialized.cache_max_size, stats.cache_max_size);
        assert!((deserialized.cache_hit_rate - stats.cache_hit_rate).abs() < f64::EPSILON);
        assert!((deserialized.uptime_seconds - stats.uptime_seconds).abs() < f64::EPSILON);
    }
}
