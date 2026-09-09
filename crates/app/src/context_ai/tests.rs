use super::*;
use analyst_core::{context::MarketObservation, MarketSnapshot, PrefilterResult, Verdict};
use serde_json::json;
use std::sync::atomic::{AtomicUsize, Ordering};

struct Spy {
    calls: Arc<AtomicUsize>,
    fail_first: bool,
}
impl ContextEngine for Spy {
    fn model(&self) -> &str {
        "test-model"
    }
    async fn analyze(&self, _: &AnalysisContext) -> Result<AiDecision> {
        let n = self.calls.fetch_add(1, Ordering::SeqCst);
        if self.fail_first && n == 0 {
            anyhow::bail!("mock OpenAI failure");
        }
        tokio::task::yield_now().await;
        Ok(AiDecision {
            verdict: Verdict::Observe,
            confidence: 60,
            narrative: None,
            thesis: "limited evidence".into(),
            positives: vec![],
            risks: vec![],
            missing_data: vec!["onChain".into()],
            next_checks: vec![],
        })
    }
}
struct Fixture {
    root: PathBuf,
    contexts: PathBuf,
    markets: PathBuf,
    history: PathBuf,
    attempts: PathBuf,
    context: AnalysisContext,
}
impl Fixture {
    async fn new() -> Self {
        let root = std::env::temp_dir().join(format!(
            "bigcalls-ai-{}-{}",
            std::process::id(),
            Utc::now().timestamp_nanos_opt().unwrap()
        ));
        tokio::fs::create_dir_all(&root).await.unwrap();
        let markets = root.join("markets.jsonl");
        let m = MarketObservation {
            discovered_at: Utc::now(),
            fetched_at: Utc::now(),
            status: "ACCEPTED".into(),
            market: MarketSnapshot {
                contract_address: "MintA".into(),
                ..MarketSnapshot::default()
            },
            prefilter: PrefilterResult {
                rejected: false,
                reasons: vec![],
                warnings: vec![],
            },
        };
        JsonlStore::new(&markets).append_durable(&m).await.unwrap();
        let context = AnalysisContext::assemble(
            &m,
            HistoryReference {
                path: markets.clone(),
                offset: 0,
            },
            None,
            None,
        )
        .unwrap();
        Self {
            contexts: root.join("contexts.jsonl"),
            history: root.join("history.jsonl"),
            attempts: root.join("attempts.jsonl"),
            markets,
            root,
            context,
        }
    }
    async fn append(&self, context: &AnalysisContext) -> u64 {
        let offset = tokio::fs::metadata(&self.contexts)
            .await
            .map(|m| m.len())
            .unwrap_or(0);
        JsonlStore::new(&self.contexts)
            .append_durable(context)
            .await
            .unwrap();
        offset
    }
    async fn service(
        &self,
        calls: &Arc<AtomicUsize>,
        fail_first: bool,
    ) -> Arc<AnalysisService<Spy>> {
        Arc::new(
            AnalysisService::open(
                Some(Spy {
                    calls: calls.clone(),
                    fail_first,
                }),
                self.contexts.clone(),
                self.markets.clone(),
                self.history.clone(),
                self.attempts.clone(),
            )
            .await
            .unwrap(),
        )
    }
    fn revision(&self) -> AnalysisContext {
        let mut c = self.context.clone();
        c.social_events.push(
            serde_json::from_value(json!({"id":"new-event","receivedAt":"2026-09-09T01:00:00Z",
            "event":{"source":"j7","contractAddress":"MintA","text":"new evidence"}}))
            .unwrap(),
        );
        c
    }
    async fn cleanup(self) {
        tokio::fs::remove_dir_all(self.root).await.unwrap();
    }
}

#[tokio::test]
async fn shared_service_deduplicates_concurrent_calls_and_allows_distinct_revision() {
    let f = Fixture::new().await;
    let calls = Arc::new(AtomicUsize::new(0));
    let first = f.append(&f.context).await;
    let duplicate = f.append(&f.context).await;
    let second = f.append(&f.revision()).await;
    let service = f.service(&calls, false).await;
    let (a, b) = tokio::join!(
        service.analyze_offset(first),
        service.analyze_offset(duplicate)
    );
    assert_eq!(a.unwrap().context_hash, b.unwrap().context_hash);
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    service.analyze_offset(second).await.unwrap();
    assert_eq!(calls.load(Ordering::SeqCst), 2);
    let records = strict_history::<ContextAnalysisRecord>(&f.history)
        .await
        .unwrap();
    assert_eq!(records.len(), 2);
    assert_ne!(records[0].context_hash, records[1].context_hash);
    assert_eq!(records[0].model, "test-model");
    assert_eq!(records[0].contract_address, "MintA");
    f.cleanup().await;
}

#[tokio::test]
async fn restart_uses_analysis_history_even_without_attempt_entries() {
    let f = Fixture::new().await;
    let calls = Arc::new(AtomicUsize::new(0));
    let offset = f.append(&f.context).await;
    f.service(&calls, false)
        .await
        .analyze_offset(offset)
        .await
        .unwrap();
    // Recover successful history independently of the separate attempt journal.
    tokio::fs::remove_file(&f.attempts).await.unwrap();
    let record = f
        .service(&calls, false)
        .await
        .analyze_offset(offset)
        .await
        .unwrap();
    assert_eq!(record.context_hash, context_hash(&f.context).unwrap());
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert_eq!(
        strict_history::<ContextAnalysisRecord>(&f.history)
            .await
            .unwrap()
            .len(),
        1
    );
    f.cleanup().await;
}

#[tokio::test]
async fn failed_openai_request_is_not_a_fake_analysis_and_not_resent_after_restart() {
    let f = Fixture::new().await;
    let calls = Arc::new(AtomicUsize::new(0));
    let offset = f.append(&f.context).await;
    let service = f.service(&calls, true).await;
    assert!(matches!(
        service.analyze_offset(offset).await,
        Err(AnalysisError::Provider)
    ));
    assert!(!f.history.exists());
    assert!(matches!(
        service.analyze_offset(offset).await,
        Err(AnalysisError::AlreadyAttempted)
    ));
    assert!(matches!(
        f.service(&calls, false).await.analyze_offset(offset).await,
        Err(AnalysisError::AlreadyAttempted)
    ));
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    f.cleanup().await;
}

#[tokio::test]
async fn disabled_automatic_worker_makes_zero_calls_even_for_unread_contexts() {
    let f = Fixture::new().await;
    let calls = Arc::new(AtomicUsize::new(0));
    f.append(&f.context).await;
    assert!(!auto_enabled(None).unwrap());
    assert!(!auto_enabled(Some("false")).unwrap());
    assert!(auto_enabled(Some("true")).unwrap());
    assert!(auto_enabled(Some("yes")).is_err());
    run(
        false,
        f.service(&calls, false).await,
        Tail::new(f.contexts.clone()),
        Arc::new(Notify::new()),
    )
    .await
    .unwrap();
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    assert!(!f.history.exists());
    f.cleanup().await;
}

async fn wait_calls(calls: &AtomicUsize, n: usize) {
    tokio::time::timeout(Duration::from_secs(3), async {
        while calls.load(Ordering::SeqCst) < n {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn automatic_worker_skips_old_and_rejected_contexts_and_survives_provider_failure() {
    let f = Fixture::new().await;
    let calls = Arc::new(AtomicUsize::new(0));
    f.append(&f.context).await;
    let tail = new_context_tail(f.contexts.clone()).await.unwrap();
    let service = f.service(&calls, true).await;
    let notify = Arc::new(Notify::new());
    let mut rejected = f.context.clone();
    rejected.prefilter.rejected = true;
    f.append(&rejected).await;
    let first_new = f.revision();
    f.append(&first_new).await;
    let task = tokio::spawn(run(true, service.clone(), tail, notify.clone()));
    wait_calls(&calls, 1).await;
    {
        let mut state = service.state.lock().await;
        assert!(state.next_request > Instant::now() + Duration::from_secs(50));
        state.next_request = Instant::now(); // Advance cooldown only inside this test.
    }
    assert!(!f.history.exists());
    let mut next = first_new.clone();
    next.social_events[0].event.text = Some("another revision".into());
    f.append(&next).await;
    notify.notify_one();
    wait_calls(&calls, 2).await;
    let completed = service.state.lock().await.completed.len();
    assert_eq!(completed, 1);
    assert_eq!(
        strict_history::<ContextAnalysisRecord>(&f.history)
            .await
            .unwrap()
            .len(),
        1
    );
    task.abort();
    let _ = task.await;
    // Restart tail starts at EOF, so it does not backfill any old or failed context.
    let mut restarted = new_context_tail(f.contexts.clone()).await.unwrap();
    assert!(restarted.read_new().await.unwrap().is_empty());
    f.cleanup().await;
}

#[tokio::test]
async fn authoritative_rejected_market_and_invalid_offset_cannot_trigger_ai() {
    let f = Fixture::new().await;
    let calls = Arc::new(AtomicUsize::new(0));
    let offset = f.append(&f.context).await;
    let mut m: MarketObservation = read_at(&f.markets, 0).await.unwrap();
    m.status = "REJECTED".into();
    // Fixture simulates an untrusted context claiming acceptance over a rejected source.
    tokio::fs::write(
        &f.markets,
        format!("{}\n", serde_json::to_string(&m).unwrap()),
    )
    .await
    .unwrap();
    let service = f.service(&calls, false).await;
    assert!(matches!(
        service.analyze_offset(offset).await,
        Err(AnalysisError::Ineligible)
    ));
    assert!(matches!(
        service.analyze_offset(offset + 1).await,
        Err(AnalysisError::InvalidReference)
    ));
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    f.cleanup().await;
}

#[tokio::test]
async fn damaged_attempt_journal_disables_ai_instead_of_risking_a_duplicate() {
    let f = Fixture::new().await;
    tokio::fs::write(&f.attempts, b"{\"contextHash\":")
        .await
        .unwrap();
    assert!(AnalysisService::<Spy>::open(
        None,
        f.contexts.clone(),
        f.markets.clone(),
        f.history.clone(),
        f.attempts.clone()
    )
    .await
    .is_err());
    f.cleanup().await;
}
