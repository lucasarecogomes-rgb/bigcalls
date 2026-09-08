use std::sync::{Arc, Mutex};

use axum::{body::Body, http::Request, routing::get, Router};
use serde_json::json;
use tokio::{
    net::TcpListener,
    task::JoinHandle,
    time::{sleep, timeout},
};

use super::*;

const MINT: &str = "6p6xgHyF7AeE6TZkSmFsko444wqoP15icUSqi2jfGiPN";

fn candidate() -> TokenCandidate {
    TokenCandidate {
        contract_address: MINT.into(),
        discovered_at: Utc::now(),
        source: "pump.fun".into(),
        provider: "pumpportal".into(),
        name: None,
        symbol: None,
        metadata_uri: None,
        transaction_signature: None,
        transaction_user: None,
        bonding_curve_address: None,
    }
}

fn token() -> Value {
    json!({"address": MINT, "name": " Example ", "symbol": " EX ",
        "price": {"price": "0.25", "volume_5m": "1200.5", "volume_1h": 5000},
        "liquidity": "30000", "circulating_supply": "1000000", "total_supply": "2000000",
        "decimals": 6, "creation_timestamp": 1700000000, "holder_count": 100,
        "pool": {"creation_timestamp": 1700100000}})
}

#[test]
fn normalizes_documented_fields_and_calculates_cap_from_circulating_supply() {
    let snapshot = normalize_token(&token(), MINT).unwrap();
    assert_eq!(snapshot.contract_address, MINT);
    assert_eq!(snapshot.symbol.as_deref(), Some("EX"));
    assert_eq!(snapshot.name.as_deref(), Some("Example"));
    assert_eq!(snapshot.price_usd, Some(0.25));
    assert_eq!(snapshot.market_cap_usd, Some(250000.0));
    assert_eq!(snapshot.liquidity_usd, Some(30000.0));
    assert_eq!(snapshot.volume_5m_usd, Some(1200.5));
    assert_eq!(snapshot.volume_1h_usd, Some(5000.0));
    assert_eq!(snapshot.created_at.unwrap().timestamp(), 1700000000);
    assert_eq!(snapshot.source.as_deref(), Some("gmgn"));
    assert!(snapshot.holders.is_none());
    assert!(snapshot.top_10_holder_pct.is_none());
    assert!(snapshot.mint_authority_revoked.is_none());
}

#[test]
fn missing_invalid_or_nonfinite_values_stay_none() {
    let data = json!({"address": MINT, "name": " ", "symbol": false,
        "price": {"price": "NaN", "volume_5m": -1, "volume_1h": "not-a-number"},
        "liquidity": "Infinity", "circulating_supply": "1000000",
        "creation_timestamp": 1700000000000_i64, "open_timestamp": 1700000000,
        "pool": {"creation_timestamp": 1700000000}});
    let snapshot = normalize_token(&data, MINT).unwrap();
    assert!(snapshot.name.is_none() && snapshot.symbol.is_none());
    assert!(snapshot.price_usd.is_none() && snapshot.market_cap_usd.is_none());
    assert!(snapshot.volume_5m_usd.is_none() && snapshot.volume_1h_usd.is_none());
    assert!(snapshot.liquidity_usd.is_none() && snapshot.created_at.is_none());
    let minimal = normalize_token(&json!({"address": MINT}), MINT).unwrap();
    assert!(minimal.price_usd.is_none() && minimal.market_cap_usd.is_none());
    assert!(minimal.created_at.is_none());
}

#[test]
fn zero_is_data_and_total_supply_is_not_a_market_cap_fallback() {
    let snapshot = normalize_token(
        &json!({"address": MINT,
        "price": {"price": 0, "volume_5m": "0", "volume_1h": 0},
        "total_supply": 1000, "pool": {"liquidity": "0"}}),
        MINT,
    )
    .unwrap();
    assert_eq!(snapshot.price_usd, Some(0.0));
    assert_eq!(snapshot.volume_5m_usd, Some(0.0));
    assert_eq!(snapshot.volume_1h_usd, Some(0.0));
    assert_eq!(snapshot.liquidity_usd, Some(0.0));
    assert!(snapshot.market_cap_usd.is_none());
    let overflow = normalize_token(
        &json!({"address": MINT,
        "price": {"price": "1e308"}, "circulating_supply": "1e308"}),
        MINT,
    )
    .unwrap();
    assert!(overflow.market_cap_usd.is_none());
}

#[test]
fn rejects_wrong_token_and_preserves_existing_analyze_input_shape() {
    assert_eq!(
        normalize_token(&json!({"address": "another-token"}), MINT).unwrap_err(),
        MarketDataError::AddressMismatch
    );
    assert_eq!(
        normalize_token(&Value::Null, MINT).unwrap_err(),
        MarketDataError::InvalidResponse
    );
    let request: crate::AnalysisRequest =
        serde_json::from_value(json!({"market": {"contractAddress": MINT}})).unwrap();
    assert!(request.market.price_usd.is_none());
    assert!(serde_json::to_value(&request.market)
        .unwrap()
        .get("priceUsd")
        .is_none());
    assert!(request.social.is_empty());
}

struct MockServer {
    endpoint: String,
    requests: Arc<Mutex<Vec<(String, HeaderMap)>>>,
    task: JoinHandle<()>,
}

impl Drop for MockServer {
    fn drop(&mut self) {
        self.task.abort();
    }
}

async fn mock(status: StatusCode, headers: HeaderMap, body: String, delay: Duration) -> MockServer {
    let requests = Arc::new(Mutex::new(Vec::new()));
    let recorded = requests.clone();
    let app = Router::new().route(
        "/v1/token/info",
        get(move |request: Request<Body>| {
            let (recorded, headers, body) = (recorded.clone(), headers.clone(), body.clone());
            async move {
                recorded
                    .lock()
                    .unwrap()
                    .push((request.uri().to_string(), request.headers().clone()));
                sleep(delay).await;
                (status, headers, body)
            }
        }),
    );
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("http://{}/v1/token/info", listener.local_addr().unwrap());
    let task = tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    MockServer {
        endpoint,
        requests,
        task,
    }
}

fn provider(server: &MockServer) -> GmgnMarketDataProvider {
    let mut provider = GmgnMarketDataProvider::new("fake-test-key".into()).unwrap();
    provider.endpoint = server.endpoint.clone();
    provider
}

#[tokio::test]
async fn sends_documented_read_only_auth_with_fresh_request_ids() {
    let server = mock(
        StatusCode::OK,
        HeaderMap::new(),
        json!({"code": 0, "data": token()}).to_string(),
        Duration::ZERO,
    )
    .await;
    let mut provider = provider(&server);
    for _ in 0..2 {
        assert_eq!(
            provider.fetch_market(&candidate()).await.unwrap().price_usd,
            Some(0.25)
        );
    }
    let requests = server.requests.lock().unwrap();
    assert_eq!(requests.len(), 2);
    let mut ids = Vec::new();
    for (uri, headers) in requests.iter() {
        assert_eq!(headers["x-apikey"], "fake-test-key");
        assert!(!headers.contains_key("x-signature"));
        let url = reqwest::Url::parse(&format!("http://localhost{uri}")).unwrap();
        assert_eq!(url.path(), "/v1/token/info");
        let params: std::collections::HashMap<_, _> = url.query_pairs().collect();
        assert_eq!(params["chain"], "sol");
        assert_eq!(params["address"], MINT);
        assert!((params["timestamp"].parse::<i64>().unwrap() - Utc::now().timestamp()).abs() <= 5);
        ids.push(Uuid::parse_str(&params["client_id"]).unwrap());
    }
    assert_ne!(ids[0], ids[1]);
}

#[tokio::test]
async fn handles_http_business_and_malformed_response_errors() {
    for (status, body, expected) in [
        (
            StatusCode::UNAUTHORIZED,
            "{}",
            MarketDataError::Authentication,
        ),
        (StatusCode::FORBIDDEN, "{}", MarketDataError::Authentication),
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            "upstream error",
            MarketDataError::Http(500),
        ),
        (StatusCode::NOT_FOUND, "{}", MarketDataError::Http(404)),
        (
            StatusCode::OK,
            r#"{"code":123,"message":"untrusted message"}"#,
            MarketDataError::ProviderRejected,
        ),
        (StatusCode::OK, "not-json", MarketDataError::InvalidResponse),
        (
            StatusCode::OK,
            r#"{"code":0,"data":null}"#,
            MarketDataError::InvalidResponse,
        ),
    ] {
        let server = mock(status, HeaderMap::new(), body.into(), Duration::ZERO).await;
        assert_eq!(
            provider(&server)
                .fetch_market(&candidate())
                .await
                .unwrap_err(),
            expected
        );
    }
}

#[tokio::test]
async fn timeout_and_invalid_candidates_do_not_panic_or_leak_credentials() {
    let server = mock(
        StatusCode::OK,
        HeaderMap::new(),
        "{}".into(),
        Duration::from_secs(1),
    )
    .await;
    let mut provider = provider(&server);
    let mut invalid = candidate();
    invalid.contract_address = "invalid/?token".into();
    assert_eq!(
        provider.fetch_market(&invalid).await.unwrap_err(),
        MarketDataError::InvalidCandidate
    );
    assert!(server.requests.lock().unwrap().is_empty());
    provider.client = Client::builder()
        .timeout(Duration::from_millis(30))
        .build()
        .unwrap();
    let error = provider.fetch_market(&candidate()).await.unwrap_err();
    assert_eq!(error, MarketDataError::Timeout);
    assert!(!format!("{error:?}").contains("fake-test-key"));
    assert!(matches!(
        GmgnMarketDataProvider::new("".into()),
        Err(MarketDataError::InvalidCredentials)
    ));
}

#[tokio::test]
async fn respects_rate_limit_cooldown_before_sending_another_request() {
    for (status, body) in [
        (StatusCode::TOO_MANY_REQUESTS, "non-JSON 429 response"),
        (
            StatusCode::OK,
            r#"{"code":429,"error":"RATE_LIMIT_BANNED"}"#,
        ),
    ] {
        let mut headers = HeaderMap::new();
        headers.insert(RETRY_AFTER, HeaderValue::from_static("60"));
        let server = mock(status, headers, body.into(), Duration::ZERO).await;
        let mut provider = provider(&server);
        assert!(matches!(
            provider.fetch_market(&candidate()).await,
            Err(MarketDataError::RateLimited { .. })
        ));
        assert!(timeout(
            Duration::from_millis(30),
            provider.fetch_market(&candidate())
        )
        .await
        .is_err());
        assert_eq!(server.requests.lock().unwrap().len(), 1);
    }
}

#[test]
fn rate_limit_uses_latest_reset_including_retry_after_http_date() {
    let now = DateTime::from_timestamp(1700000000, 0).unwrap();
    let mut headers = HeaderMap::new();
    headers.insert("x-ratelimit-reset", HeaderValue::from_static("1700000020"));
    headers.insert(RETRY_AFTER, HeaderValue::from_static("10"));
    assert_eq!(
        rate_cooldown(&headers, Some(&json!({"reset_at": 1700000040})), now),
        Duration::from_secs(41)
    );
    let date = (now + chrono::Duration::seconds(50)).to_rfc2822();
    headers.insert(RETRY_AFTER, HeaderValue::from_str(&date).unwrap());
    assert_eq!(rate_cooldown(&headers, None, now), Duration::from_secs(51));
    assert_eq!(
        rate_cooldown(&HeaderMap::new(), None, now),
        Duration::from_secs(61)
    );
}

#[tokio::test]
#[ignore = "requires GMGN_API_KEY; makes one read-only live request"]
async fn live_read_only_token_info() {
    let key = std::env::var("GMGN_API_KEY").expect("set GMGN_API_KEY for this explicit live test");
    let mut provider = GmgnMarketDataProvider::new(key).unwrap();
    let snapshot = provider.fetch_market(&candidate()).await.unwrap();
    assert_eq!(snapshot.contract_address, MINT);
    assert!(snapshot.price_usd.is_some());
    println!("Live GMGN fields: price={}, market_cap={}, liquidity={}, volume_5m={}, volume_1h={}, created_at={}",
        snapshot.price_usd.is_some(), snapshot.market_cap_usd.is_some(), snapshot.liquidity_usd.is_some(),
        snapshot.volume_5m_usd.is_some(), snapshot.volume_1h_usd.is_some(), snapshot.created_at.is_some());
}
