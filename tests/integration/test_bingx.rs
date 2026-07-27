//! Integration tests for BingX exchange.

use sdktrader::models::MarketType;

mod public_spot {
    use super::*;
    crate::public_endpoint_tests!("bingx", MarketType::Spot);
}

mod public_swap {
    use super::*;
    crate::public_endpoint_tests!("bingx", MarketType::Swap);
}

mod private_spot {
    use super::*;
    crate::private_endpoint_tests!("bingx", MarketType::Spot);
}

mod private_swap {
    use super::*;
    crate::private_endpoint_tests!("bingx", MarketType::Swap);
}

mod websocket {
    use super::*;
    crate::websocket_tests!("bingx", MarketType::Swap);
}

mod order_dryrun {
    use super::*;
    crate::order_dryrun_tests!("bingx", MarketType::Swap);
}
