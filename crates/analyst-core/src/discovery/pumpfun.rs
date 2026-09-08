//! Pump.fun creation events delivered by the third-party PumpPortal feed.

use std::time::Duration;

use anyhow::{bail, Context, Result};
use chrono::Utc;
use futures_util::{SinkExt, StreamExt};
use serde_json::Value;
use tokio::{net::TcpStream, time::timeout};
use tokio_tungstenite::{connect_async, tungstenite::Message, MaybeTlsStream, WebSocketStream};

use super::{TokenCandidate, TokenSource};

const ENDPOINT: &str = "wss://pumpportal.fun/api/data";
const CONNECT_TIMEOUT: Duration = Duration::from_secs(15);
const READ_TIMEOUT: Duration = Duration::from_secs(90);

type Socket = WebSocketStream<MaybeTlsStream<TcpStream>>;

pub struct PumpFunSource {
    endpoint: String,
    socket: Option<Socket>,
}

impl Default for PumpFunSource {
    fn default() -> Self {
        Self {
            endpoint: ENDPOINT.into(),
            socket: None,
        }
    }
}

impl PumpFunSource {
    pub fn new() -> Self {
        Self::default()
    }

    async fn receive(&mut self) -> Result<TokenCandidate> {
        if self.socket.is_none() {
            let (mut socket, _) = timeout(CONNECT_TIMEOUT, connect_async(&self.endpoint))
                .await
                .context("PumpPortal connection timed out")?
                .context("PumpPortal connection failed")?;
            timeout(
                CONNECT_TIMEOUT,
                socket.send(Message::Text(r#"{"method":"subscribeNewToken"}"#.into())),
            )
            .await
            .context("PumpPortal subscription timed out")??;
            self.socket = Some(socket);
        }

        let socket = self.socket.as_mut().context("PumpPortal socket missing")?;
        loop {
            let message = timeout(READ_TIMEOUT, socket.next())
                .await
                .context("PumpPortal stream idle for 90 seconds")?
                .context("PumpPortal stream ended")??;
            match message {
                Message::Text(text) => {
                    if let Some(candidate) = normalize_event(text.as_ref())? {
                        return Ok(candidate);
                    }
                }
                Message::Ping(_) => {
                    // Tungstenite queues the matching pong automatically.
                    socket.flush().await?;
                }
                Message::Close(_) => bail!("PumpPortal closed the connection"),
                _ => {}
            }
        }
    }
}

impl TokenSource for PumpFunSource {
    async fn next_candidate(&mut self) -> Result<TokenCandidate> {
        let result = self.receive().await;
        if result.is_err() {
            // Drop the old connection before the runtime retries this source.
            self.socket = None;
        }
        result
    }
}

fn normalize_event(text: &str) -> Result<Option<TokenCandidate>> {
    let event: Value = serde_json::from_str(text).context("Invalid PumpPortal event JSON")?;
    if event.get("error").is_some_and(|error| !error.is_null()) {
        bail!("PumpPortal reported a provider error");
    }

    // The creation subscription also carries other launchpads (e.g. bonk).
    if event.get("txType").and_then(Value::as_str) != Some("create")
        || event.get("pool").and_then(Value::as_str) != Some("pump")
    {
        return Ok(None);
    }
    let Some(contract_address) = text_field(&event, "mint") else {
        return Ok(None);
    };

    Ok(Some(TokenCandidate {
        contract_address,
        discovered_at: Utc::now(),
        source: "pump.fun".into(),
        provider: "pumpportal".into(),
        name: text_field(&event, "name"),
        symbol: text_field(&event, "symbol"),
        metadata_uri: text_field(&event, "uri"),
        transaction_signature: text_field(&event, "signature"),
        transaction_user: text_field(&event, "traderPublicKey"),
        bonding_curve_address: text_field(&event, "bondingCurveKey"),
    }))
}

fn text_field(event: &Value, key: &str) -> Option<String> {
    event
        .get(key)
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_owned)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use tokio::net::TcpListener;
    use tokio_tungstenite::accept_async;

    const MINT: &str = "5eM84hw58miUmLuqqLA2eTUqL8s5wv71q2ReNc99pump";

    fn creation() -> Value {
        json!({
            "txType": "create", "pool": "pump", "mint": MINT,
            "name": " Example ", "symbol": " EX ", "uri": "ipfs://metadata",
            "signature": "transaction-signature", "traderPublicKey": "transaction-user",
            "bondingCurveKey": "curve-address", "marketCapSol": 25
        })
    }

    #[test]
    fn normalizes_identity_and_metadata_without_inventing_market_or_creation_time() {
        let before = Utc::now();
        let candidate = normalize_event(&creation().to_string()).unwrap().unwrap();
        assert_eq!(candidate.contract_address, MINT);
        assert_eq!(candidate.source, "pump.fun");
        assert_eq!(candidate.provider, "pumpportal");
        assert_eq!(candidate.name.as_deref(), Some("Example"));
        assert_eq!(candidate.symbol.as_deref(), Some("EX"));
        assert_eq!(candidate.metadata_uri.as_deref(), Some("ipfs://metadata"));
        assert_eq!(
            candidate.transaction_user.as_deref(),
            Some("transaction-user")
        );
        assert!(candidate.discovered_at >= before && candidate.discovered_at <= Utc::now());
        let stored = serde_json::to_value(candidate).unwrap();
        assert!(stored.get("createdAt").is_none());
        assert!(stored.get("marketCapUsd").is_none());
        assert!(stored.get("creator").is_none());
    }

    #[test]
    fn ignores_other_launchpads_trades_acknowledgements_and_missing_mints() {
        for message in [
            json!({"message": "Successfully subscribed to token creation events."}),
            json!({"txType": "buy", "pool": "pump", "mint": MINT}),
            json!({"txType": "create", "pool": "bonk", "mint": MINT}),
            json!({"txType": "create", "mint": MINT}),
            json!({"txType": "create", "pool": "pump"}),
            json!({"txType": "create", "pool": "pump", "mint": "  "}),
        ] {
            assert!(normalize_event(&message.to_string()).unwrap().is_none());
        }
    }

    #[test]
    fn tolerates_missing_or_invalid_optional_metadata() {
        let candidate = normalize_event(
            &json!({"txType": "create", "pool": "pump", "mint": MINT,
                "name": null, "symbol": " ", "uri": 123})
            .to_string(),
        )
        .unwrap()
        .unwrap();
        assert!(candidate.name.is_none());
        assert!(candidate.symbol.is_none());
        assert!(candidate.metadata_uri.is_none());
        assert!(candidate.transaction_signature.is_none());
    }

    #[test]
    fn reports_invalid_json_and_provider_errors() {
        assert!(normalize_event("not json").is_err());
        assert!(normalize_event(r#"{"error":"subscription rejected"}"#).is_err());
    }

    #[tokio::test]
    async fn subscribes_handles_ping_and_reconnects_after_close() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("ws://{}", listener.local_addr().unwrap());
        let server = tokio::spawn(async move {
            for connection in 0..2 {
                let (stream, _) = listener.accept().await.unwrap();
                let mut socket = accept_async(stream).await.unwrap();
                let subscription = socket.next().await.unwrap().unwrap();
                assert_eq!(
                    serde_json::from_str::<Value>(subscription.to_text().unwrap()).unwrap(),
                    json!({"method": "subscribeNewToken"})
                );
                if connection == 0 {
                    socket.send(Message::Close(None)).await.unwrap();
                } else {
                    socket
                        .send(Message::Ping(vec![1, 2, 3].into()))
                        .await
                        .unwrap();
                    assert_eq!(
                        socket.next().await.unwrap().unwrap(),
                        Message::Pong(vec![1, 2, 3].into())
                    );
                    let mut other_pool = creation();
                    other_pool["pool"] = json!("bonk");
                    socket
                        .send(Message::Text(other_pool.to_string().into()))
                        .await
                        .unwrap();
                    socket
                        .send(Message::Text(creation().to_string().into()))
                        .await
                        .unwrap();
                }
            }
        });
        let mut source = PumpFunSource {
            endpoint,
            socket: None,
        };
        timeout(Duration::from_secs(5), async {
            assert!(source.next_candidate().await.is_err());
            assert!(source.socket.is_none());
            assert_eq!(
                source.next_candidate().await.unwrap().contract_address,
                MINT
            );
            server.await.unwrap();
        })
        .await
        .unwrap();
    }
}
