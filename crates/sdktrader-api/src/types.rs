//! API request and response types.

use serde::{Deserialize, Serialize};

/// Generic API response wrapper.
#[derive(Debug, Serialize)]
pub struct ApiResponse<T: Serialize> {
    pub success: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub data: Option<T>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

impl<T: Serialize> ApiResponse<T> {
    pub fn success(data: T) -> Self {
        Self {
            success: true,
            data: Some(data),
            error: None,
        }
    }

    pub fn error(message: impl Into<String>) -> Self {
        Self {
            success: false,
            data: None,
            error: Some(message.into()),
        }
    }
}

/// Request to test an exchange adapter.
#[derive(Debug, Deserialize)]
pub struct TestExchangeRequest {
    pub bot_name: String,
    #[serde(default = "default_symbol")]
    pub symbol: String,
    #[serde(default)]
    pub methods: Vec<String>,
}

fn default_symbol() -> String {
    "BTCUSDT".to_string()
}

/// Result of a single test.
#[derive(Debug, Serialize)]
pub struct TestResult {
    pub name: String,
    pub success: bool,
    pub duration_ms: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub result: Option<serde_json::Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

/// Response from exchange testing.
#[derive(Debug, Serialize)]
pub struct TestExchangeResponse {
    pub bot_name: String,
    pub exchange: String,
    pub testnet: bool,
    pub market_type: String,
    pub has_credentials: bool,
    pub tests: Vec<TestResult>,
    pub overall_success: bool,
}

/// Bot info for testing page.
#[derive(Debug, Serialize)]
pub struct BotTestInfo {
    pub name: String,
    pub exchange: String,
    pub market_type: String,
    pub testnet: bool,
    pub has_api_key: bool,
    pub has_api_secret: bool,
    pub has_passphrase: bool,
}

/// Health check response.
#[derive(Debug, Serialize)]
pub struct HealthResponse {
    pub status: String,
    pub version: String,
    pub backend: String,
}
