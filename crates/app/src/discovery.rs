use std::{
    collections::{HashSet, VecDeque},
    time::Duration,
};

use analyst_core::{discovery::TokenSource, JsonlStore};
use anyhow::{Context, Result};
use tracing::warn;

const RECENT_CANDIDATE_LIMIT: usize = 10_000;

pub async fn run(mut source: impl TokenSource, store: JsonlStore) -> Result<()> {
    let mut recent = RecentCandidates::default();
    let mut retry_delay = Duration::from_secs(1);

    loop {
        let candidate = match source.next_candidate().await {
            Ok(candidate) => candidate,
            Err(error) => {
                warn!(
                    error = ?error,
                    retry_seconds = retry_delay.as_secs(),
                    "token discovery source failed; reconnecting"
                );
                tokio::time::sleep(retry_delay).await;
                retry_delay = (retry_delay * 2).min(Duration::from_secs(60));
                continue;
            }
        };
        retry_delay = Duration::from_secs(1);

        let key = (candidate.source.clone(), candidate.contract_address.clone());
        if recent.keys.contains(&key) {
            continue;
        }

        store
            .append(&candidate)
            .await
            .context("failed to persist discovered token")?;
        recent.remember(key);
    }
}

#[derive(Default)]
struct RecentCandidates {
    keys: HashSet<(String, String)>,
    order: VecDeque<(String, String)>,
}

impl RecentCandidates {
    fn remember(&mut self, key: (String, String)) {
        if self.order.len() == RECENT_CANDIDATE_LIMIT {
            if let Some(oldest) = self.order.pop_front() {
                self.keys.remove(&oldest);
            }
        }
        self.keys.insert(key.clone());
        self.order.push_back(key);
    }
}

#[cfg(test)]
mod tests {
    use std::{future::pending, path::PathBuf, time::SystemTime};

    use analyst_core::discovery::TokenCandidate;
    use chrono::Utc;
    use tokio::sync::oneshot;

    use super::*;

    struct MockSource {
        candidates: VecDeque<TokenCandidate>,
        drained: Option<oneshot::Sender<()>>,
    }

    impl TokenSource for MockSource {
        async fn next_candidate(&mut self) -> Result<TokenCandidate> {
            if let Some(candidate) = self.candidates.pop_front() {
                return Ok(candidate);
            }
            if let Some(drained) = self.drained.take() {
                let _ = drained.send(());
            }
            pending().await
        }
    }

    fn candidate() -> TokenCandidate {
        TokenCandidate {
            contract_address: "So11111111111111111111111111111111111111112".into(),
            discovered_at: Utc::now(),
            source: "pump.fun".into(),
            provider: "pumpportal".into(),
            name: Some("Example token".into()),
            symbol: Some("EXAMPLE".into()),
            metadata_uri: Some("https://example.com/token.json".into()),
            transaction_signature: Some("creation-transaction".into()),
            transaction_user: Some("transaction-user".into()),
            bonding_curve_address: None,
        }
    }

    fn history_path() -> PathBuf {
        let timestamp = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::env::temp_dir().join(format!(
            "bigcalls-discovery-{}-{timestamp}.jsonl",
            std::process::id()
        ))
    }

    #[tokio::test]
    async fn persists_candidates_once_per_source_and_contract() {
        let first = candidate();
        let mut another_source = first.clone();
        another_source.source = "another-source".into();
        let (drained_tx, drained_rx) = oneshot::channel();
        let source = MockSource {
            candidates: VecDeque::from([first.clone(), first.clone(), another_source.clone()]),
            drained: Some(drained_tx),
        };
        let path = history_path();
        let task = tokio::spawn(run(source, JsonlStore::new(&path)));

        let drained = tokio::time::timeout(Duration::from_secs(5), drained_rx).await;
        task.abort();
        let _ = task.await;
        drained.unwrap().unwrap();

        let history = tokio::fs::read_to_string(&path).await.unwrap();
        tokio::fs::remove_file(&path).await.unwrap();
        let records: Vec<serde_json::Value> = history
            .lines()
            .map(|line| serde_json::from_str(line).unwrap())
            .collect();
        assert_eq!(
            records,
            vec![
                serde_json::to_value(first).unwrap(),
                serde_json::to_value(another_source).unwrap(),
            ]
        );
    }

    #[tokio::test]
    async fn storage_failure_stops_discovery() {
        let source = MockSource {
            candidates: VecDeque::from([candidate()]),
            drained: None,
        };
        // A directory cannot be opened as the JSONL history file.
        let result = run(source, JsonlStore::new(std::env::temp_dir())).await;
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("failed to persist discovered token"));
    }

    #[test]
    fn recent_history_is_bounded() {
        let mut recent = RecentCandidates::default();
        for index in 0..=RECENT_CANDIDATE_LIMIT {
            recent.remember(("pump.fun".into(), index.to_string()));
        }

        assert_eq!(recent.keys.len(), RECENT_CANDIDATE_LIMIT);
        assert!(!recent.keys.contains(&("pump.fun".into(), "0".into())));
        assert!(recent
            .keys
            .contains(&("pump.fun".into(), RECENT_CANDIDATE_LIMIT.to_string())));
    }
}
