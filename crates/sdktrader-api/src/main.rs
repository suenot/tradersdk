//! Binary entry point for the SDKTrader API server.

use sdktrader_api::ApiServer;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Initialize tracing
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "info,sdktrader=debug".into()),
        )
        .init();

    let host = std::env::var("HOST").unwrap_or_else(|_| "0.0.0.0".into());
    let port: u16 = std::env::var("PORT")
        .ok()
        .and_then(|p| p.parse().ok())
        .unwrap_or(8085);
    let config_path = std::env::var("CONFIG_PATH").unwrap_or_else(|_| "config.json".into());

    let server = ApiServer::new(host, port, config_path);
    server.start().await
}
