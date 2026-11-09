//! HTTP API server setup.

use std::net::SocketAddr;
use std::sync::Arc;

use axum::{
    routing::{get, post},
    Router,
};
use tower_http::cors::{Any, CorsLayer};
use tower_http::trace::TraceLayer;

use crate::config::Config;
use crate::handlers::{self, AppState};

/// API Server.
pub struct ApiServer {
    host: String,
    port: u16,
    config_path: String,
}

impl ApiServer {
    /// Create a new API server.
    pub fn new(host: impl Into<String>, port: u16, config_path: impl Into<String>) -> Self {
        Self {
            host: host.into(),
            port,
            config_path: config_path.into(),
        }
    }

    /// Build the router with all routes.
    fn build_router(state: Arc<AppState>) -> Router {
        // CORS configuration
        let cors = CorsLayer::new()
            .allow_origin(Any)
            .allow_methods(Any)
            .allow_headers(Any);

        // API v1 routes
        let api_v1 = Router::new()
            .route("/health", get(handlers::health_check))
            .route("/exchanges", get(handlers::list_exchanges))
            .route("/exchanges/methods", get(handlers::list_adapter_methods))
            .route("/exchanges/bots", get(handlers::list_bots_for_testing))
            .route("/exchanges/test", post(handlers::test_exchange));

        Router::new()
            .nest("/api/v1", api_v1)
            .layer(cors)
            .layer(TraceLayer::new_for_http())
            .with_state(state)
    }

    /// Start the server.
    pub async fn start(&self) -> anyhow::Result<()> {
        // Load configuration
        let config = Config::load(&self.config_path).unwrap_or_else(|e| {
            tracing::warn!(
                "Failed to load config from '{}': {}. Using default.",
                self.config_path,
                e
            );
            Config::default()
        });

        tracing::info!("Loaded {} bot(s) from configuration", config.bots.len());

        let state = Arc::new(AppState::new(config, self.config_path.clone()));
        let app = Self::build_router(state);

        let addr: SocketAddr = format!("{}:{}", self.host, self.port).parse()?;

        tracing::info!("TraderSDK API server starting on http://{}", addr);

        let listener = tokio::net::TcpListener::bind(addr).await?;
        axum::serve(listener, app).await?;

        Ok(())
    }
}
