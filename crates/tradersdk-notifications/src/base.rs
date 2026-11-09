//! Base notification types.

use serde::{Deserialize, Serialize};

/// Notification severity level.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
pub enum NotificationLevel {
    Debug,
    #[default]
    Info,
    Warning,
    Error,
    Critical,
}

impl NotificationLevel {
    /// Get emoji for level.
    pub fn emoji(&self) -> &'static str {
        match self {
            NotificationLevel::Debug => "\u{1f50d}",
            NotificationLevel::Info => "\u{2139}\u{fe0f}",
            NotificationLevel::Warning => "\u{26a0}\u{fe0f}",
            NotificationLevel::Error => "\u{274c}",
            NotificationLevel::Critical => "\u{1f6a8}",
        }
    }
}

/// Notification message.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NotificationMessage {
    /// Message title.
    pub title: String,
    /// Message body.
    pub body: String,
    /// Notification level.
    pub level: NotificationLevel,
    /// Timestamp (ms).
    pub timestamp: i64,
    /// Optional metadata.
    pub metadata: Option<serde_json::Value>,
}

impl NotificationMessage {
    /// Create a new notification message.
    pub fn new(title: &str, body: &str) -> Self {
        Self {
            title: title.to_string(),
            body: body.to_string(),
            level: NotificationLevel::Info,
            timestamp: chrono::Utc::now().timestamp_millis(),
            metadata: None,
        }
    }

    /// Set notification level.
    pub fn with_level(mut self, level: NotificationLevel) -> Self {
        self.level = level;
        self
    }

    /// Set metadata.
    pub fn with_metadata(mut self, metadata: serde_json::Value) -> Self {
        self.metadata = Some(metadata);
        self
    }

    /// Create an info notification.
    pub fn info(title: &str, body: &str) -> Self {
        Self::new(title, body).with_level(NotificationLevel::Info)
    }

    /// Create a warning notification.
    pub fn warning(title: &str, body: &str) -> Self {
        Self::new(title, body).with_level(NotificationLevel::Warning)
    }

    /// Create an error notification.
    pub fn error(title: &str, body: &str) -> Self {
        Self::new(title, body).with_level(NotificationLevel::Error)
    }

    /// Create a critical notification.
    pub fn critical(title: &str, body: &str) -> Self {
        Self::new(title, body).with_level(NotificationLevel::Critical)
    }

    /// Format as plain text.
    pub fn to_text(&self) -> String {
        format!("{} {}\n\n{}", self.level.emoji(), self.title, self.body)
    }
}

/// Result of sending a notification.
#[derive(Debug, Clone)]
pub struct NotificationResult {
    /// Whether sending was successful.
    pub success: bool,
    /// Response message or error.
    pub message: Option<String>,
    /// Message ID if assigned by service.
    pub message_id: Option<String>,
}

impl NotificationResult {
    /// Create a successful result.
    pub fn success() -> Self {
        Self {
            success: true,
            message: None,
            message_id: None,
        }
    }

    /// Create a successful result with message ID.
    pub fn success_with_id(id: &str) -> Self {
        Self {
            success: true,
            message: None,
            message_id: Some(id.to_string()),
        }
    }

    /// Create a failure result.
    pub fn failure(message: &str) -> Self {
        Self {
            success: false,
            message: Some(message.to_string()),
            message_id: None,
        }
    }
}
