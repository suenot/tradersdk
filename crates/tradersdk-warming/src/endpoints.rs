use std::collections::HashMap;

/// Configuration for a server time endpoint.
pub struct EndpointConfig {
    /// Full URL of the server time API.
    pub url: &'static str,
    /// HTTP method (GET or POST).
    pub method: &'static str,
}

/// Get server time endpoints for all supported exchanges.
///
/// Returns a map from exchange name to its endpoint configuration.
pub fn get_endpoints() -> HashMap<&'static str, EndpointConfig> {
    let mut endpoints = HashMap::new();

    endpoints.insert(
        "bybit",
        EndpointConfig {
            url: "https://api.bybit.com/v5/market/time",
            method: "GET",
        },
    );

    endpoints.insert(
        "binance",
        EndpointConfig {
            url: "https://api.binance.com/api/v3/time",
            method: "GET",
        },
    );

    endpoints.insert(
        "bingx",
        EndpointConfig {
            url: "https://open-api.bingx.com/openApi/swap/v2/server/time",
            method: "GET",
        },
    );

    endpoints.insert(
        "mexc",
        EndpointConfig {
            url: "https://api.mexc.com/api/v3/time",
            method: "GET",
        },
    );

    endpoints.insert(
        "xt",
        EndpointConfig {
            url: "https://sapi.xt.com/v4/public/time",
            method: "GET",
        },
    );

    endpoints.insert(
        "okx",
        EndpointConfig {
            url: "https://www.okx.com/api/v5/public/time",
            method: "GET",
        },
    );

    endpoints.insert(
        "gateio",
        EndpointConfig {
            url: "https://api.gateio.ws/api/v4/spot/time",
            method: "GET",
        },
    );

    endpoints.insert(
        "bitget",
        EndpointConfig {
            url: "https://api.bitget.com/api/v2/public/time",
            method: "GET",
        },
    );

    endpoints
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_all_endpoints_present() {
        let endpoints = get_endpoints();
        let expected = [
            "bybit", "binance", "bingx", "mexc", "xt", "okx", "gateio", "bitget",
        ];
        for name in &expected {
            assert!(
                endpoints.contains_key(name),
                "Missing endpoint for {}",
                name
            );
        }
        assert_eq!(endpoints.len(), expected.len());
    }

    #[test]
    fn test_all_endpoints_are_get() {
        let endpoints = get_endpoints();
        for (name, config) in &endpoints {
            assert_eq!(config.method, "GET", "{} should use GET", name);
        }
    }

    #[test]
    fn test_endpoints_have_https() {
        let endpoints = get_endpoints();
        for (name, config) in &endpoints {
            assert!(
                config.url.starts_with("https://"),
                "{} URL should start with https://",
                name
            );
        }
    }
}
