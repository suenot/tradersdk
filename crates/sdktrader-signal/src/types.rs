use serde::{Deserialize, Serialize};

/// Incoming signal message from external parser.
///
/// Mirrors the protobuf `TelegramMessage` from the Python signal service,
/// but is transport-agnostic (works with gRPC, HTTP, WebSocket, etc.).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SignalMessage {
    /// Raw message text content
    pub message_text: String,
    /// Unique message identifier (e.g. "grpc_1234567890")
    pub message_id: String,
    /// Timestamp when the message was originally published (ms since epoch)
    pub published_at_ms: i64,
    /// Timestamp when the message was detected by the parser (ms since epoch)
    pub detected_at_ms: i64,
    /// Source transport method: "grpc", "grpc_stream", "telegram", etc.
    pub source: String,
    /// City of the parser node
    pub city: String,
    /// IP address of the parser node
    pub ip: String,
}

/// Response after processing a signal.
///
/// Returned to the caller indicating whether the message was accepted
/// as new or was recognized as a duplicate.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SignalResponse {
    /// Whether the message was processed successfully
    pub success: bool,
    /// Human-readable status message
    pub message: String,
    /// Server-side timestamp when the response was generated (ms since epoch)
    pub server_timestamp_ms: i64,
    /// Whether the message was identified as a duplicate
    pub deduplicated: bool,
}

/// Signal server statistics.
///
/// Aggregated counters for monitoring the signal processing pipeline.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct SignalStats {
    /// Total number of messages received (including duplicates)
    pub total_messages_received: u64,
    /// Number of messages that were identified as duplicates
    pub deduplicated_count: u64,
    /// Current number of entries in the deduplication cache
    pub cache_size: usize,
    /// Maximum capacity of the deduplication cache
    pub cache_max_size: usize,
    /// Cache hit rate (0.0 to 1.0)
    pub cache_hit_rate: f64,
    /// Time since the processor was started (seconds)
    pub uptime_seconds: f64,
}
