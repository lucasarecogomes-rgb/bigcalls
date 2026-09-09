use analyst_core::{
    discovery::TokenCandidate,
    market::{MarketDataError, MarketDataProvider},
    prefilter, AnalystConfig, JsonlStore, MarketSnapshot, PrefilterResult,
};
use chrono::{DateTime, Utc};
use serde::Serialize;
use std::{collections::HashSet, time::Duration};
use tokio::{
    sync::mpsc::{Receiver, Sender},
    time::{timeout_at, Instant},
};
use tracing::{info, warn};

const BATCH_SIZE: usize = 80;
const BATCH_WINDOW: Duration = Duration::from_secs(5);

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct MarketEnrichmentRecord {
    pub(crate) discovered_at: DateTime<Utc>,
    pub(crate) fetched_at: DateTime<Utc>,
    pub(crate) market: MarketSnapshot,
    pub(crate) status: PrefilterStatus,
    pub(crate) prefilter: PrefilterResult,
}

#[derive(Serialize, PartialEq, Eq)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub(crate) enum PrefilterStatus {
    Accepted,
    Rejected,
}

pub async fn run(
    provider: impl MarketDataProvider,
    candidates: Receiver<TokenCandidate>,
    store: JsonlStore,
    config: AnalystConfig,
    onchain: Option<Sender<Vec<MarketEnrichmentRecord>>>,
) -> anyhow::Result<()> {
    run_batches(provider, candidates, store, config, BATCH_WINDOW, onchain).await
}

async fn run_batches(
    mut provider: impl MarketDataProvider,
    mut candidates: Receiver<TokenCandidate>,
    store: JsonlStore,
    config: AnalystConfig,
    window: Duration,
    onchain: Option<Sender<Vec<MarketEnrichmentRecord>>>,
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
                let mut accepted = Vec::new();
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
                    let prefilter = prefilter(&market, &config);
                    let status = if prefilter.rejected {
                        PrefilterStatus::Rejected
                    } else {
                        PrefilterStatus::Accepted
                    };
                    let record = MarketEnrichmentRecord {
                        discovered_at: candidate.discovered_at,
                        fetched_at,
                        market,
                        status,
                        prefilter,
                    };
                    store.append(&record).await?;
                    if record.prefilter.rejected {
                        continue;
                    }
                    if record.status == PrefilterStatus::Accepted {
                        accepted.push(record);
                    }
                }
                if let Some(sink) = &onchain {
                    if !accepted.is_empty() && sink.try_send(accepted).is_err() {
                        warn!(
                            "on-chain queue unavailable; accepted market history remains preserved"
                        );
                    }
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

    struct SnapshotProvider(Vec<MarketSnapshot>);

    impl MarketDataProvider for SnapshotProvider {
        async fn fetch_market(
            &mut self,
            _: &TokenCandidate,
        ) -> Result<MarketSnapshot, MarketDataError> {
            panic!("point queries must not run during prefiltering");
        }

        async fn fetch_markets(
            &mut self,
            _: &[TokenCandidate],
        ) -> Result<Vec<MarketSnapshot>, MarketDataError> {
            Ok(std::mem::take(&mut self.0))
        }
    }

    #[tokio::test]
    async fn persists_raw_snapshots_and_existing_prefilter_decisions() {
        // Non-default config proves the worker uses the supplied AnalystConfig.
        let config = AnalystConfig {
            min_liquidity_usd: 100.0,
            max_top10_pct: 90.0,
            max_creator_pct: 40.0,
            max_sniper_pct: 60.0,
        };
        let mut markets = vec![
            MarketSnapshot::default(), // Missing data alone is accepted.
            MarketSnapshot {
                liquidity_usd: Some(100.0),
                top_10_holder_pct: Some(90.0),
                creator_holder_pct: Some(40.0),
                sniper_holder_pct: Some(60.0),
                market_cap_usd: Some(0.0), // No market-cap threshold.
                mint_authority_revoked: Some(false),
                freeze_authority_revoked: Some(false), // Warnings only.
                ..MarketSnapshot::default()
            },
            MarketSnapshot {
                liquidity_usd: Some(99.0),
                ..MarketSnapshot::default()
            },
            MarketSnapshot {
                top_10_holder_pct: Some(91.0),
                ..MarketSnapshot::default()
            },
            MarketSnapshot {
                creator_holder_pct: Some(41.0),
                ..MarketSnapshot::default()
            },
            MarketSnapshot {
                sniper_holder_pct: Some(61.0),
                ..MarketSnapshot::default()
            },
            MarketSnapshot {
                liquidity_usd: Some(99.0),
                top_10_holder_pct: Some(91.0),
                creator_holder_pct: Some(41.0),
                sniper_holder_pct: Some(61.0),
                ..MarketSnapshot::default()
            },
        ];
        let (tx, rx) = mpsc::channel(8);
        for (index, market) in markets.iter_mut().enumerate() {
            let candidate = candidate(index);
            market.contract_address = candidate.contract_address.clone();
            market.source = Some("gmgn:trenches".into());
            tx.send(candidate).await.unwrap();
        }
        tx.send(candidate(7)).await.unwrap(); // No GMGN match: no prefilter record.
        drop(tx);
        let mut returned = markets.clone();
        returned.push(markets[0].clone()); // Duplicate provider row.
        returned.push(MarketSnapshot {
            contract_address: "unrelated-mint".into(),
            ..MarketSnapshot::default()
        });
        let file = path();
        let (onchain_tx, mut onchain_rx) = mpsc::channel(1);
        run_batches(
            SnapshotProvider(returned),
            rx,
            JsonlStore::new(&file),
            config.clone(),
            BATCH_WINDOW,
            Some(onchain_tx),
        )
        .await
        .unwrap();
        let accepted = onchain_rx.recv().await.unwrap();
        assert_eq!(accepted.len(), 2);
        assert!(accepted
            .iter()
            .all(|r| r.status == PrefilterStatus::Accepted && !r.prefilter.rejected));
        assert!(onchain_rx.recv().await.is_none());
        let history = tokio::fs::read_to_string(&file).await.unwrap();
        tokio::fs::remove_file(&file).await.unwrap();
        let records: Vec<serde_json::Value> = history
            .lines()
            .map(|line| serde_json::from_str(line).unwrap())
            .collect();
        assert_eq!(records.len(), 7);
        for (index, (record, market)) in records.iter().zip(&markets).enumerate() {
            assert_eq!(record["market"], serde_json::to_value(market).unwrap());
            assert_eq!(
                record["prefilter"],
                serde_json::to_value(prefilter(market, &config)).unwrap()
            );
            assert_eq!(
                record["status"],
                if index < 2 { "ACCEPTED" } else { "REJECTED" }
            );
            assert_eq!(record["prefilter"]["rejected"], index >= 2);
            assert_eq!(
                record["prefilter"]["reasons"].as_array().unwrap().len(),
                if index < 2 {
                    0
                } else if index == 6 {
                    4
                } else {
                    1
                }
            );
            assert!(record.get("ai").is_none());
            assert!(record.get("social").is_none());
        }
        assert_eq!(
            records[0]["prefilter"]["warnings"]
                .as_array()
                .unwrap()
                .len(),
            2
        );
        assert_eq!(
            records[1]["prefilter"]["warnings"]
                .as_array()
                .unwrap()
                .len(),
            2
        );
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
        run_batches(
            provider,
            rx,
            JsonlStore::new(&file),
            AnalystConfig::default(),
            BATCH_WINDOW,
            None,
        )
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
        run_batches(
            provider,
            rx,
            JsonlStore::new(&file),
            AnalystConfig::default(),
            BATCH_WINDOW,
            None,
        )
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
            AnalystConfig::default(),
            Duration::from_millis(30),
            None,
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
                AnalystConfig::default(),
                BATCH_WINDOW,
                None
            )
            .await
            .is_err());
        }
    }
}
