use analyst_core::{
    discovery::TokenCandidate,
    market::{MarketDataError, MarketDataProvider},
    JsonlStore, MarketSnapshot,
};
use chrono::{DateTime, Utc};
use serde::Serialize;
use tokio::sync::mpsc::Receiver;
use tracing::warn;

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct MarketEnrichmentRecord {
    discovered_at: DateTime<Utc>,
    fetched_at: DateTime<Utc>,
    market: MarketSnapshot,
}

pub async fn run(
    mut provider: impl MarketDataProvider,
    mut candidates: Receiver<TokenCandidate>,
    store: JsonlStore,
) -> anyhow::Result<()> {
    while let Some(candidate) = candidates.recv().await {
        match provider.fetch_market(&candidate).await {
            Ok(market) => {
                let record = MarketEnrichmentRecord {
                    discovered_at: candidate.discovered_at,
                    fetched_at: Utc::now(),
                    market,
                };
                // Persistence failure stops only this worker, not discovery or HTTP.
                store.append(&record).await?;
            }
            Err(MarketDataError::Authentication) => {
                // Repeating rejected credentials for every mint serves no purpose.
                return Err(MarketDataError::Authentication.into());
            }
            Err(error) => {
                warn!(error = %error, "market enrichment failed; candidate remains in discovery history");
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::VecDeque;
    use tokio::sync::mpsc;

    struct MockProvider(VecDeque<Result<MarketSnapshot, MarketDataError>>);

    impl MarketDataProvider for MockProvider {
        async fn fetch_market(
            &mut self,
            _: &TokenCandidate,
        ) -> Result<MarketSnapshot, MarketDataError> {
            self.0.pop_front().expect("unexpected repeat API call")
        }
    }

    fn candidate() -> TokenCandidate {
        TokenCandidate {
            contract_address: "So11111111111111111111111111111111111111112".into(),
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

    #[tokio::test]
    async fn provider_error_does_not_prevent_enriching_the_next_candidate() {
        let candidate = candidate();
        let (tx, rx) = mpsc::channel(2);
        tx.send(candidate.clone()).await.unwrap();
        tx.send(candidate.clone()).await.unwrap();
        drop(tx);
        let provider = MockProvider(VecDeque::from([
            Err(MarketDataError::Timeout),
            Ok(MarketSnapshot {
                contract_address: candidate.contract_address.clone(),
                price_usd: Some(0.5),
                source: Some("mock".into()),
                ..MarketSnapshot::default()
            }),
        ]));
        let path = std::env::temp_dir().join(format!(
            "bigcalls-market-{}-{}.jsonl",
            std::process::id(),
            Utc::now().timestamp_nanos_opt().unwrap()
        ));
        run(provider, rx, JsonlStore::new(&path)).await.unwrap();
        let history = tokio::fs::read_to_string(&path).await.unwrap();
        tokio::fs::remove_file(&path).await.unwrap();
        assert_eq!(history.lines().count(), 1);
        let record: serde_json::Value = serde_json::from_str(history.trim()).unwrap();
        assert_eq!(record["market"]["priceUsd"], 0.5);
        assert_eq!(
            record["market"]["contractAddress"],
            candidate.contract_address
        );
        assert!(record.get("ai").is_none());
        assert!(record["market"]["marketCapUsd"].is_null());
    }

    #[tokio::test]
    async fn authentication_failure_stops_further_requests() {
        let (tx, rx) = mpsc::channel(2);
        tx.send(candidate()).await.unwrap();
        tx.send(candidate()).await.unwrap();
        drop(tx);
        let provider = MockProvider(VecDeque::from([Err(MarketDataError::Authentication)]));
        let error = run(provider, rx, JsonlStore::new(std::env::temp_dir()))
            .await
            .unwrap_err();
        assert_eq!(
            error.downcast_ref::<MarketDataError>(),
            Some(&MarketDataError::Authentication)
        );
    }

    #[tokio::test]
    async fn storage_failure_returns_an_error() {
        let (tx, rx) = mpsc::channel(1);
        tx.send(candidate()).await.unwrap();
        drop(tx);
        let provider = MockProvider(VecDeque::from([Ok(MarketSnapshot::default())]));
        assert!(run(provider, rx, JsonlStore::new(std::env::temp_dir()))
            .await
            .is_err());
    }
}
