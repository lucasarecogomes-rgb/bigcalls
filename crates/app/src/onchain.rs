use crate::market::{MarketEnrichmentRecord, PrefilterStatus};
use analyst_core::{onchain::OnChainProvider, record_id, JsonlStore};
use chrono::Utc;
use serde_json::json;
use std::collections::HashSet;
use tokio::sync::mpsc::Receiver;
use tracing::warn;

pub async fn run(
    mut provider: impl OnChainProvider,
    mut batches: Receiver<Vec<MarketEnrichmentRecord>>,
    store: JsonlStore,
) -> anyhow::Result<()> {
    while let Some(records) = batches.recv().await {
        process_batch(&mut provider, records, &store).await?;
    }
    Ok(())
}

async fn process_batch(
    provider: &mut impl OnChainProvider,
    records: Vec<MarketEnrichmentRecord>,
    store: &JsonlStore,
) -> anyhow::Result<()> {
    let mut seen = HashSet::new();
    // Defense at the stage boundary, even if a caller sends a rejected record.
    let accepted: Vec<_> = records
        .into_iter()
        .filter(|r| {
            r.status == PrefilterStatus::Accepted
                && !r.prefilter.rejected
                && seen.insert(r.market.contract_address.clone())
        })
        .collect();
    if accepted.is_empty() {
        return Ok(());
    }
    let addresses: Vec<_> = accepted
        .iter()
        .map(|r| r.market.contract_address.clone())
        .collect();
    match provider.fetch_batch(&addresses).await {
        Ok(snapshots) => {
            let mut saved = HashSet::new();
            for snapshot in snapshots {
                let Some(input) = accepted
                    .iter()
                    .find(|r| r.market.contract_address == snapshot.contract_address)
                else {
                    continue;
                };
                if !saved.insert(snapshot.contract_address.clone()) {
                    continue;
                }
                store
                    .append(&json!({
                        "marketRecordId": record_id(input),
                        "marketFetchedAt": input.fetched_at,
                        "observedAt": Utc::now(),
                        "onChain": snapshot
                    }))
                    .await?;
            }
        }
        Err(error) => {
            warn!(error = %error, "on-chain batch unavailable; accepted market history remains preserved")
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use analyst_core::{
        onchain::{OnChainError, OnChainSnapshot},
        MarketSnapshot, PrefilterResult,
    };

    struct Spy(Vec<Vec<String>>);
    impl OnChainProvider for Spy {
        async fn fetch_batch(
            &mut self,
            addresses: &[String],
        ) -> Result<Vec<OnChainSnapshot>, OnChainError> {
            self.0.push(addresses.to_vec());
            Ok(addresses
                .iter()
                .map(|address| OnChainSnapshot {
                    source: "test".into(),
                    commitment: None,
                    contract_address: address.clone(),
                    slot: None,
                    token_program: None,
                    reported_extensions: None,
                })
                .collect())
        }
    }

    struct FailOnce(bool);
    impl OnChainProvider for FailOnce {
        async fn fetch_batch(
            &mut self,
            _: &[String],
        ) -> Result<Vec<OnChainSnapshot>, OnChainError> {
            if !self.0 {
                self.0 = true;
                Err(OnChainError::Transport)
            } else {
                Ok(vec![OnChainSnapshot {
                    source: "test".into(),
                    commitment: None,
                    contract_address: "second".into(),
                    slot: None,
                    token_program: None,
                    reported_extensions: None,
                }])
            }
        }
    }

    #[tokio::test]
    async fn failed_batch_does_not_stop_the_worker() {
        let path = std::env::temp_dir().join(format!(
            "bigcalls-onchain-failure-{}.jsonl",
            record_id(&Utc::now())
        ));
        let (tx, rx) = tokio::sync::mpsc::channel(2);
        tx.send(vec![record(PrefilterStatus::Accepted, false, "first")])
            .await
            .unwrap();
        tx.send(vec![record(PrefilterStatus::Accepted, false, "second")])
            .await
            .unwrap();
        drop(tx);
        run(FailOnce(false), rx, JsonlStore::new(&path))
            .await
            .unwrap();
        let history = tokio::fs::read_to_string(&path).await.unwrap();
        assert_eq!(history.lines().count(), 1);
        assert!(history.contains("second"));
        tokio::fs::remove_file(path).await.unwrap();
    }

    fn record(status: PrefilterStatus, rejected: bool, address: &str) -> MarketEnrichmentRecord {
        MarketEnrichmentRecord {
            discovered_at: Utc::now(),
            fetched_at: Utc::now(),
            status,
            market: MarketSnapshot {
                contract_address: address.into(),
                ..MarketSnapshot::default()
            },
            prefilter: PrefilterResult {
                rejected,
                reasons: vec![],
                warnings: vec![],
            },
        }
    }

    #[tokio::test]
    async fn rejected_and_inconsistent_records_never_reach_provider_or_history() {
        let path = std::env::temp_dir().join(format!(
            "bigcalls-onchain-{}.jsonl",
            analyst_core::record_id(&Utc::now())
        ));
        let store = JsonlStore::new(&path);
        let mut spy = Spy(vec![]);
        process_batch(
            &mut spy,
            vec![record(PrefilterStatus::Rejected, true, "rejected")],
            &store,
        )
        .await
        .unwrap();
        assert!(spy.0.is_empty());
        assert!(!path.exists());
        process_batch(
            &mut spy,
            vec![
                record(PrefilterStatus::Accepted, false, "accepted"),
                record(PrefilterStatus::Rejected, false, "rejected"),
                record(PrefilterStatus::Accepted, true, "inconsistent"),
            ],
            &store,
        )
        .await
        .unwrap();
        assert_eq!(spy.0, vec![vec!["accepted".to_string()]]);
        let history = tokio::fs::read_to_string(&path).await.unwrap();
        assert_eq!(history.lines().count(), 1);
        let row: serde_json::Value = serde_json::from_str(&history).unwrap();
        assert_eq!(row["onChain"]["contractAddress"], "accepted");
        assert!(row["onChain"]["reportedExtensions"].is_null());
        assert!(row.get("market").is_none());
        assert!(row.get("status").is_none()); // No new decision.
        tokio::fs::remove_file(path).await.unwrap();
    }
}
