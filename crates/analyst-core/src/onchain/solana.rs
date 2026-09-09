use super::{OnChainError, OnChainProvider, OnChainSnapshot};
use reqwest::Client;
use serde_json::{json, Value};
use std::time::Duration;
use tokio::time::Instant;

const TOKEN: &str = "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA";
const TOKEN_2022: &str = "TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb";

pub struct SolanaRpcProvider {
    client: Client,
    endpoint: reqwest::Url,
    next_request: Instant,
}

impl SolanaRpcProvider {
    pub fn new(endpoint: &str) -> Result<Self, OnChainError> {
        let endpoint = reqwest::Url::parse(endpoint).map_err(|_| OnChainError::Configuration)?;
        if !matches!(endpoint.scheme(), "https" | "http") {
            return Err(OnChainError::Configuration);
        }
        Ok(Self {
            client: Client::builder()
                .connect_timeout(Duration::from_secs(5))
                .timeout(Duration::from_secs(10))
                .redirect(reqwest::redirect::Policy::none())
                .build()
                .map_err(|_| OnChainError::Configuration)?,
            endpoint,
            next_request: Instant::now(),
        })
    }
}

impl OnChainProvider for SolanaRpcProvider {
    async fn fetch_batch(
        &mut self,
        addresses: &[String],
    ) -> Result<Vec<OnChainSnapshot>, OnChainError> {
        if addresses.is_empty() {
            return Ok(Vec::new());
        }
        if addresses.len() > 100 {
            return Err(OnChainError::Configuration);
        }
        if Instant::now() < self.next_request {
            return Err(OnChainError::Unavailable);
        }
        // One query for the accepted subset of the existing market batch. No retries.
        self.next_request = Instant::now() + Duration::from_secs(1);
        let response = self
            .client
            .post(self.endpoint.clone())
            .json(&json!({
                "jsonrpc": "2.0", "id": 1, "method": "getMultipleAccounts",
                "params": [addresses, {"encoding": "jsonParsed", "commitment": "confirmed"}]
            }))
            .send()
            .await
            .map_err(|_| OnChainError::Transport)?;
        if !response.status().is_success() {
            let retry = response
                .headers()
                .get("retry-after")
                .and_then(|v| v.to_str().ok())
                .and_then(|v| v.parse::<u64>().ok())
                .unwrap_or(60)
                .clamp(60, 86400);
            self.next_request = Instant::now() + Duration::from_secs(retry);
            return Err(OnChainError::Unavailable);
        }
        let body: Value = response
            .json()
            .await
            .map_err(|_| OnChainError::InvalidResponse)?;
        if body.get("error").is_some() {
            self.next_request = Instant::now() + Duration::from_secs(60);
            return Err(OnChainError::Unavailable);
        }
        normalize(&body, addresses)
    }
}

fn normalize(body: &Value, addresses: &[String]) -> Result<Vec<OnChainSnapshot>, OnChainError> {
    let rows = body
        .pointer("/result/value")
        .and_then(Value::as_array)
        .filter(|rows| rows.len() == addresses.len())
        .ok_or(OnChainError::InvalidResponse)?;
    let slot = body.pointer("/result/context/slot").and_then(Value::as_u64);
    Ok(addresses
        .iter()
        .zip(rows)
        .map(|(address, row)| {
            let owner = row.get("owner").and_then(Value::as_str);
            let is_mint = row.pointer("/data/parsed/type").and_then(Value::as_str) == Some("mint")
                && matches!(owner, Some(TOKEN | TOKEN_2022));
            let reported_extensions = if is_mint {
                row.pointer("/data/parsed/info/extensions")
                    .and_then(Value::as_array)
                    .and_then(|items| {
                        items
                            .iter()
                            .map(|item| {
                                item.get("extension")
                                    .and_then(Value::as_str)
                                    .filter(|s| !s.is_empty())
                                    .map(str::to_owned)
                            })
                            .collect::<Option<Vec<_>>>()
                    })
            } else {
                None
            };
            OnChainSnapshot {
                source: "solana-rpc:getMultipleAccounts".into(),
                commitment: Some("confirmed".into()),
                contract_address: address.clone(),
                slot,
                token_program: if is_mint {
                    owner.map(str::to_owned)
                } else {
                    None
                },
                reported_extensions,
            }
        })
        .collect())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    #[ignore = "requires SOLANA_RPC_URL; one read-only mainnet mint request"]
    async fn live_read_only_mint_context() {
        let mut provider =
            SolanaRpcProvider::new(&std::env::var("SOLANA_RPC_URL").unwrap()).unwrap();
        let mint = "So11111111111111111111111111111111111111112".to_string();
        let rows = provider
            .fetch_batch(std::slice::from_ref(&mint))
            .await
            .unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].contract_address, mint);
        assert_eq!(rows[0].token_program.as_deref(), Some(TOKEN));
        assert!(rows[0].slot.is_some());
    }

    #[test]
    fn normalizes_only_reported_mint_context_in_request_order() {
        let body = json!({"result":{"context":{"slot":42},"value":[
            {"owner":TOKEN_2022,"data":{"parsed":{"type":"mint","info":{
                "extensions":[{"extension":"transferFeeConfig","state":{}},{"extension":"permanentDelegate"}]
            }}}},
            {"owner":TOKEN,"data":{"parsed":{"type":"mint","info":{}}}},
            null,
            {"owner":TOKEN_2022,"data":{"parsed":{"type":"account","info":{"extensions":[]}}}}
        ]}});
        let rows = normalize(&body, &["a".into(), "b".into(), "c".into(), "d".into()]).unwrap();
        assert_eq!(
            rows[0].reported_extensions,
            Some(vec!["transferFeeConfig".into(), "permanentDelegate".into()])
        );
        assert_eq!(rows[0].slot, Some(42));
        assert_eq!(rows[1].token_program.as_deref(), Some(TOKEN));
        assert!(rows[1].reported_extensions.is_none());
        assert!(rows[2].token_program.is_none());
        assert!(rows[3].reported_extensions.is_none());
        assert_eq!(rows[3].contract_address, "d");
        assert!(normalize(&body, &[]).is_err());
        assert!(normalize(&json!({}), &["a".into()]).is_err());
    }

    #[tokio::test]
    async fn http_contract_and_cooldown() {
        use axum::{routing::post, Json, Router};
        let app = Router::new().route(
            "/",
            post(|Json(body): Json<Value>| async move {
                assert_eq!(body["method"], "getMultipleAccounts");
                assert_eq!(body["params"][0], json!(["a", "b"]));
                assert_eq!(body["params"][1]["encoding"], "jsonParsed");
                (axum::http::StatusCode::TOO_MANY_REQUESTS, "limited")
            }),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let mut provider =
            SolanaRpcProvider::new(&format!("http://{}", listener.local_addr().unwrap())).unwrap();
        let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        assert!(provider.fetch_batch(&[]).await.unwrap().is_empty());
        assert!(matches!(
            provider.fetch_batch(&["a".into(), "b".into()]).await,
            Err(OnChainError::Unavailable)
        ));
        assert!(provider.next_request > Instant::now() + Duration::from_secs(50));
        assert!(matches!(
            provider.fetch_batch(&["c".into()]).await,
            Err(OnChainError::Unavailable)
        ));
        server.abort();
    }
}
