use analyst_core::{
    discovery::TokenCandidate,
    market::{MarketDataError, MarketDataProvider},
    JsonlStore, MarketSnapshot,
};
use chrono::{DateTime, Utc};
use serde::Serialize;
use std::{collections::HashSet, time::Duration};
use tokio::{
    sync::mpsc::Receiver,
    time::{timeout_at, Instant},
};
use tracing::{info, warn};

const BATCH_SIZE: usize = 80;
const BATCH_WINDOW: Duration = Duration::from_secs(5);

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct MarketEnrichmentRecord {
    discovered_at: DateTime<Utc>,
    fetched_at: DateTime<Utc>,
    market: MarketSnapshot,
}

pub async fn run(
    provider: impl MarketDataProvider,
    candidates: Receiver<TokenCandidate>,
    store: JsonlStore,
) -> anyhow::Result<()> {
    run_batches(provider, candidates, store, BATCH_WINDOW).await
}

async fn run_batches(
    mut provider: impl MarketDataProvider,
    mut candidates: Receiver<TokenCandidate>,
    store: JsonlStore,
    window: Duration,
) -> anyhow::Result<()> {
    while let Some(first) = candidates.recv().await {
        let mut batch = vec![first];
        let deadline = Instant::now() + window;
        while batch.len() < BATCH_SIZE {
            match timeout_at(deadline, candidates.recv()).await {
                Ok(Some(candidate)) => batch.push(candidate),
                _ => break,
            }
        }
        // Exactly one bulk call; never fall back to token/info for misses.
        match provider.fetch_markets(&batch).await {
            Ok(markets) => {
                let mut saved = HashSet::new();
                let fetched_at = Utc::now();
                for market in markets {
                    let Some(candidate) = batch
                        .iter()
                        .find(|c| c.contract_address.trim() == market.contract_address)
                    else {
                        continue;
                    };
                    if !saved.insert(market.contract_address.clone()) {
                        continue;
                    }
                    let record = MarketEnrichmentRecord {
                        discovered_at: candidate.discovered_at,
                        fetched_at,
                        market,
                    };
                    store.append(&record).await?;
                }
                info!(
                    candidates = batch.len(),
                    enriched = saved.len(),
                    "market batch completed; unmatched candidates remain in discovery history"
                );
            }
            Err(MarketDataError::Authentication | MarketDataError::BatchUnsupported) => {
                return Err(anyhow::anyhow!(
                    "market batch worker stopped: authentication or batch capability unavailable"
                ));
            }
            Err(error) => {
                warn!(error = %error, candidates = batch.len(), "market batch failed; candidates remain in discovery history");
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{
        collections::VecDeque,
        sync::{Arc, Mutex},
    };
    use tokio::sync::mpsc;

    struct MockProvider {
        calls: Arc<Mutex<Vec<usize>>>,
        errors: VecDeque<Option<MarketDataError>>,
        omit_last: bool,
    }

    impl MarketDataProvider for MockProvider {
        async fn fetch_market(
            &mut self,
            _: &TokenCandidate,
        ) -> Result<MarketSnapshot, MarketDataError> {
            panic!("automatic single-token fallback is forbidden");
        }
        async fn fetch_markets(
            &mut self,
            candidates: &[TokenCandidate],
        ) -> Result<Vec<MarketSnapshot>, MarketDataError> {
            self.calls.lock().unwrap().push(candidates.len());
            if let Some(Some(error)) = self.errors.pop_front() {
                return Err(error);
            }
            Ok(candidates
                .iter()
                .take(candidates.len() - usize::from(self.omit_last))
                .map(|c| MarketSnapshot {
                    contract_address: c.contract_address.clone(),
                    price_usd: Some(0.5),
                    source: Some("mock".into()),
                    ..MarketSnapshot::default()
                })
                .collect())
        }
    }

    fn candidate(index: usize) -> TokenCandidate {
        TokenCandidate {
            contract_address: format!("test-mint-{index}"),
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

    fn path() -> std::path::PathBuf {
        std::env::temp_dir().join(format!(
            "bigcalls-batch-{}-{}.jsonl",
            std::process::id(),
            Utc::now().timestamp_nanos_opt().unwrap()
        ))
    }

    #[tokio::test]
    async fn one_hundred_candidates_use_two_calls_without_fallback_for_misses() {
        let (tx, rx) = mpsc::channel(100);
        for index in 0..100 {
            tx.send(candidate(index)).await.unwrap();
        }
        drop(tx);
        let calls = Arc::new(Mutex::new(Vec::new()));
        let provider = MockProvider {
            calls: calls.clone(),
            errors: VecDeque::new(),
            omit_last: true,
        };
        let file = path();
        run_batches(provider, rx, JsonlStore::new(&file), BATCH_WINDOW)
            .await
            .unwrap();
        assert_eq!(*calls.lock().unwrap(), vec![80, 20]);
        let history = tokio::fs::read_to_string(&file).await.unwrap();
        tokio::fs::remove_file(&file).await.unwrap();
        assert_eq!(history.lines().count(), 98);
        let record: serde_json::Value =
            serde_json::from_str(history.lines().next().unwrap()).unwrap();
        assert_eq!(record["market"]["priceUsd"], 0.5);
        assert!(record.get("ai").is_none());
    }

    #[tokio::test]
    async fn failed_batch_does_not_prevent_the_next_batch() {
        let (tx, rx) = mpsc::channel(81);
        for index in 0..81 {
            tx.send(candidate(index)).await.unwrap();
        }
        drop(tx);
        let calls = Arc::new(Mutex::new(Vec::new()));
        let provider = MockProvider {
            calls: calls.clone(),
            errors: VecDeque::from([Some(MarketDataError::Timeout)]),
            omit_last: false,
        };
        let file = path();
        run_batches(provider, rx, JsonlStore::new(&file), BATCH_WINDOW)
            .await
            .unwrap();
        assert_eq!(*calls.lock().unwrap(), vec![80, 1]);
        let history = tokio::fs::read_to_string(&file).await.unwrap();
        tokio::fs::remove_file(&file).await.unwrap();
        assert_eq!(history.lines().count(), 1);
    }

    #[tokio::test]
    async fn timer_flushes_small_batch_and_idle_worker_makes_no_requests() {
        let (tx, rx) = mpsc::channel(1);
        let calls = Arc::new(Mutex::new(Vec::new()));
        let provider = MockProvider {
            calls: calls.clone(),
            errors: VecDeque::new(),
            omit_last: true,
        };
        let task = tokio::spawn(run_batches(
            provider,
            rx,
            JsonlStore::new(path()),
            Duration::from_millis(30),
        ));
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert!(calls.lock().unwrap().is_empty());
        tx.send(candidate(0)).await.unwrap();
        tokio::time::timeout(Duration::from_secs(2), async {
            while calls.lock().unwrap().is_empty() {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
        drop(tx);
        task.await.unwrap().unwrap();
        assert_eq!(*calls.lock().unwrap(), vec![1]);
    }

    #[tokio::test]
    async fn authentication_and_storage_failures_stop_only_the_worker() {
        for error in [Some(MarketDataError::Authentication), None] {
            let (tx, rx) = mpsc::channel(1);
            tx.send(candidate(0)).await.unwrap();
            drop(tx);
            let provider = MockProvider {
                calls: Arc::new(Mutex::new(Vec::new())),
                errors: VecDeque::from([error]),
                omit_last: false,
            };
            assert!(run_batches(
                provider,
                rx,
                JsonlStore::new(std::env::temp_dir()),
                BATCH_WINDOW
            )
            .await
            .is_err());
        }
    }
}
