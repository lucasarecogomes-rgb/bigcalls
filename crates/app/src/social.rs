//! Local exact-contract correlation. JSONL remains the source of truth.
use analyst_core::{JsonlStore, SocialIngestRecord};
use anyhow::Result;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::{
    collections::{BTreeMap, HashSet},
    path::PathBuf,
    sync::Arc,
};
use tokio::{
    fs::File,
    io::{AsyncBufReadExt, AsyncSeekExt, BufReader, SeekFrom},
    sync::Notify,
};
use tracing::warn;

struct Tail {
    path: PathBuf,
    offset: u64,
}

impl Tail {
    fn new(path: PathBuf) -> Self {
        Self { path, offset: 0 }
    }

    async fn read_new(&mut self) -> Result<Vec<(u64, Value)>> {
        let file = match File::open(&self.path).await {
            Ok(file) => file,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(vec![]),
            Err(error) => return Err(error.into()),
        };
        anyhow::ensure!(
            file.metadata().await?.len() >= self.offset,
            "correlation history was truncated"
        );
        let mut reader = BufReader::new(file);
        reader.seek(SeekFrom::Start(self.offset)).await?;
        let mut records = Vec::new();
        loop {
            let mut line = Vec::new();
            let size = reader.read_until(b'\n', &mut line).await?;
            if size == 0 || line.last() != Some(&b'\n') {
                break;
            }
            let offset = self.offset;
            self.offset += size as u64;
            match serde_json::from_slice(&line) {
                Ok(value) => records.push((offset, value)),
                Err(_) => warn!(offset, "invalid correlation history line skipped"),
            }
        }
        Ok(records)
    }
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct AcceptedToken {
    contract_address: String,
    market_history_offset: u64,
    market_fetched_at: String,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct SocialContext<'a> {
    token: &'a AcceptedToken,
    market_history_path: &'a std::path::Path,
    social_history_path: &'a std::path::Path,
    match_method: &'static str,
    social_events: &'a [SocialIngestRecord],
}

pub(crate) struct Correlator {
    markets: Tail,
    social: Tail,
    output: JsonlStore,
    accepted: BTreeMap<u64, AcceptedToken>,
    events: BTreeMap<String, Vec<SocialIngestRecord>>,
    latest: BTreeMap<u64, Value>,
}

impl Correlator {
    pub(crate) async fn open(markets: PathBuf, social: PathBuf, output: PathBuf) -> Result<Self> {
        let mut latest = BTreeMap::new();
        for (_, context) in Tail::new(output.clone()).read_new().await? {
            // The output is append-only revisions, keyed by accepted history location.
            if context["marketHistoryPath"] == serde_json::to_value(&markets)?
                && context["socialHistoryPath"] == serde_json::to_value(&social)?
            {
                if let Some(offset) = context
                    .pointer("/token/marketHistoryOffset")
                    .and_then(Value::as_u64)
                {
                    latest.insert(offset, context);
                }
            }
        }
        Ok(Self {
            markets: Tail::new(markets),
            social: Tail::new(social),
            output: JsonlStore::new(output),
            accepted: BTreeMap::new(),
            events: BTreeMap::new(),
            latest,
        })
    }

    pub(crate) async fn reconcile(&mut self) -> Result<()> {
        let mut changed = HashSet::new();
        // Load social first so startup produces a complete context for prior acceptances.
        for (_, value) in self.social.read_new().await? {
            let Ok(record) = serde_json::from_value::<SocialIngestRecord>(value) else {
                continue;
            };
            let Some(address) = record
                .event
                .contract_address
                .as_deref()
                .map(str::trim)
                .filter(|s| !s.is_empty())
            else {
                continue;
            };
            changed.insert(address.to_owned());
            self.events
                .entry(address.to_owned())
                .or_default()
                .push(record);
        }
        for (offset, value) in self.markets.read_new().await? {
            // Explicit acceptance is required. Legacy, rejected or inconsistent rows never enter.
            if value["status"] != "ACCEPTED"
                || value.pointer("/prefilter/rejected") != Some(&Value::Bool(false))
            {
                continue;
            }
            let Some(address) = value
                .pointer("/market/contractAddress")
                .and_then(Value::as_str)
                .map(str::trim)
                .filter(|s| !s.is_empty())
            else {
                continue;
            };
            let Some(fetched_at) = value["fetchedAt"].as_str() else {
                continue;
            };
            changed.insert(address.to_owned());
            self.accepted.insert(
                offset,
                AcceptedToken {
                    contract_address: address.to_owned(),
                    market_history_offset: offset,
                    market_fetched_at: fetched_at.to_owned(),
                },
            );
        }
        for (offset, token) in &self.accepted {
            if !changed.contains(&token.contract_address) {
                continue;
            }
            let context = serde_json::to_value(SocialContext {
                token,
                market_history_path: &self.markets.path,
                social_history_path: &self.social.path,
                match_method: "contractAddress",
                social_events: self
                    .events
                    .get(&token.contract_address)
                    .map(Vec::as_slice)
                    .unwrap_or(&[]),
            })?;
            if self.latest.get(offset) != Some(&context) {
                self.output.append(&context).await?;
                self.latest.insert(*offset, context);
            }
        }
        Ok(())
    }
}

pub(crate) async fn run(
    markets: PathBuf,
    social: PathBuf,
    output: PathBuf,
    notify: Arc<Notify>,
) -> Result<()> {
    let mut correlator = Correlator::open(markets, social, output).await?;
    loop {
        correlator.reconcile().await?;
        // Notifications coalesce; unread durable lines do not get dropped.
        notify.notified().await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    struct Fixture {
        root: PathBuf,
        market: PathBuf,
        social: PathBuf,
        output: PathBuf,
    }
    impl Fixture {
        async fn new() -> Self {
            let root = std::env::temp_dir().join(format!(
                "bigcalls-social-{}-{}",
                std::process::id(),
                chrono::Utc::now().timestamp_nanos_opt().unwrap()
            ));
            tokio::fs::create_dir_all(&root).await.unwrap();
            Self {
                market: root.join("market.jsonl"),
                social: root.join("j7.jsonl"),
                output: root.join("contexts.jsonl"),
                root,
            }
        }
        async fn correlator(&self) -> Correlator {
            Correlator::open(
                self.market.clone(),
                self.social.clone(),
                self.output.clone(),
            )
            .await
            .unwrap()
        }
        async fn contexts(&self) -> Vec<Value> {
            Tail::new(self.output.clone())
                .read_new()
                .await
                .unwrap()
                .into_iter()
                .map(|(_, v)| v)
                .collect()
        }
        async fn cleanup(self) {
            tokio::fs::remove_dir_all(self.root).await.unwrap();
        }
    }

    fn accepted() -> Value {
        json!({"status":"ACCEPTED", "prefilter":{"rejected":false},
            "fetchedAt":"2026-09-09T00:00:00Z", "market":{"contractAddress":"MintA", "symbol":"SAME"}})
    }
    fn event(address: Option<&str>) -> Value {
        json!({"id":"j7-original-id", "receivedAt":"2026-09-09T00:00:01Z", "event":{
            "source":"j7", "contractAddress":address, "ticker":"SAME", "text":"original text",
            "author":"original author", "tweetUrl":"https://x.com/example/status/123", "rawLinks":[]
        }})
    }
    async fn append(path: &PathBuf, value: &Value) {
        // Notification-enabled stores guarantee readers see the complete line.
        JsonlStore::new(path)
            .with_notify(Arc::new(Notify::new()))
            .append(value)
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn same_contract_event_before_acceptance_preserves_event_and_provenance() {
        let f = Fixture::new().await;
        append(&f.social, &event(Some("MintA"))).await;
        let mut c = f.correlator().await;
        c.reconcile().await.unwrap();
        assert!(f.contexts().await.is_empty());
        append(&f.market, &accepted()).await;
        c.reconcile().await.unwrap();
        let contexts = f.contexts().await;
        assert_eq!(contexts.len(), 1);
        let context = &contexts[0];
        assert_eq!(context["matchMethod"], "contractAddress");
        assert_eq!(context["token"]["contractAddress"], "MintA");
        assert_eq!(context["socialEvents"][0]["id"], "j7-original-id");
        let parsed: SocialIngestRecord = serde_json::from_value(event(Some("MintA"))).unwrap();
        assert_eq!(
            context["socialEvents"][0],
            serde_json::to_value(parsed).unwrap()
        );
        assert_eq!(
            context["socialHistoryPath"],
            serde_json::to_value(&f.social).unwrap()
        );
        f.cleanup().await;
    }

    #[tokio::test]
    async fn event_after_acceptance_updates_empty_context_without_invention() {
        let f = Fixture::new().await;
        append(&f.market, &accepted()).await;
        let mut c = f.correlator().await;
        c.reconcile().await.unwrap();
        let initial = f.contexts().await;
        assert_eq!(initial[0]["socialEvents"], json!([]));
        assert!(initial[0].get("sentiment").is_none());
        assert!(initial[0].get("score").is_none());
        append(&f.social, &event(Some("MintA"))).await;
        c.reconcile().await.unwrap();
        let contexts = f.contexts().await;
        assert_eq!(contexts.len(), 2);
        assert_eq!(contexts[1]["socialEvents"].as_array().unwrap().len(), 1);
        assert_eq!(contexts[0]["token"], contexts[1]["token"]);
        f.cleanup().await;
    }

    #[tokio::test]
    async fn rejected_inconsistent_and_legacy_records_never_correlate() {
        let f = Fixture::new().await;
        append(&f.social, &event(Some("MintA"))).await;
        for (status, rejected) in [
            ("REJECTED", true),
            ("REJECTED", false),
            ("ACCEPTED", true),
            ("", false),
        ] {
            let mut row = accepted();
            row["status"] = json!(status);
            row["prefilter"]["rejected"] = json!(rejected);
            append(&f.market, &row).await;
        }
        let mut c = f.correlator().await;
        c.reconcile().await.unwrap();
        append(&f.social, &event(Some("MintA"))).await;
        c.reconcile().await.unwrap();
        assert!(c.accepted.is_empty());
        assert!(f.contexts().await.is_empty());
        f.cleanup().await;
    }

    #[tokio::test]
    async fn equal_ticker_without_same_contract_never_matches() {
        let f = Fixture::new().await;
        append(&f.market, &accepted()).await;
        for address in [Some("MintB"), None, Some("minta"), Some("")] {
            append(&f.social, &event(address)).await;
        }
        let mut c = f.correlator().await;
        c.reconcile().await.unwrap();
        assert_eq!(f.contexts().await[0]["socialEvents"], json!([]));
        f.cleanup().await;
    }

    #[tokio::test]
    async fn restart_replays_both_histories_without_duplicate_revisions() {
        let f = Fixture::new().await;
        append(&f.social, &event(Some("MintA"))).await;
        append(&f.market, &accepted()).await;
        f.correlator().await.reconcile().await.unwrap();
        f.correlator().await.reconcile().await.unwrap();
        assert_eq!(f.contexts().await.len(), 1);
        // Receipt duplicated by J7 is preserved as a separate receipt, not a new interpretation.
        append(&f.social, &event(Some("MintA"))).await;
        f.correlator().await.reconcile().await.unwrap();
        let contexts = f.contexts().await;
        assert_eq!(contexts.len(), 2);
        assert_eq!(contexts[1]["socialEvents"].as_array().unwrap().len(), 2);
        f.cleanup().await;
    }

    #[tokio::test]
    async fn notifications_coalesce_without_losing_durable_lines_and_partial_lines_wait() {
        use tokio::io::AsyncWriteExt;
        let f = Fixture::new().await;
        let notify = Arc::new(Notify::new());
        let store = JsonlStore::new(&f.social).with_notify(notify.clone());
        store.append(&event(Some("MintA"))).await.unwrap();
        store.append(&event(Some("MintB"))).await.unwrap();
        tokio::time::timeout(std::time::Duration::from_secs(1), notify.notified())
            .await
            .unwrap();
        let mut tail = Tail::new(f.social.clone());
        assert_eq!(tail.read_new().await.unwrap().len(), 2);
        let mut file = tokio::fs::OpenOptions::new()
            .append(true)
            .open(&f.social)
            .await
            .unwrap();
        file.write_all(b"{\"test\":true}").await.unwrap();
        file.flush().await.unwrap();
        assert!(tail.read_new().await.unwrap().is_empty());
        file.write_all(b"\n").await.unwrap();
        file.flush().await.unwrap();
        assert_eq!(tail.read_new().await.unwrap().len(), 1);
        drop(file);
        f.cleanup().await;
    }
}
