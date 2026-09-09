use super::*;
use analyst_core::{MarketSnapshot, PrefilterResult};
use serde_json::json;

const TIME: &str = "2026-09-09T00:00:00Z";

struct Fixture {
    root: PathBuf,
    market: PathBuf,
    onchain: PathBuf,
    social: PathBuf,
    receipts: PathBuf,
    output: PathBuf,
}

impl Fixture {
    async fn new() -> Self {
        let root = std::env::temp_dir().join(format!(
            "bigcalls-context-{}-{}",
            std::process::id(),
            chrono::Utc::now().timestamp_nanos_opt().unwrap()
        ));
        tokio::fs::create_dir_all(&root).await.unwrap();
        Self {
            market: root.join("market.jsonl"),
            onchain: root.join("onchain.jsonl"),
            social: root.join("social.jsonl"),
            receipts: root.join("receipts.jsonl"),
            output: root.join("contexts.jsonl"),
            root,
        }
    }
    async fn assembler(&self) -> Assembler {
        Assembler::open(
            self.market.clone(),
            self.onchain.clone(),
            self.social.clone(),
            self.receipts.clone(),
            self.output.clone(),
        )
        .await
        .unwrap()
    }
    async fn contexts(&self) -> Vec<AnalysisContext> {
        Tail::new(self.output.clone())
            .read_new()
            .await
            .unwrap()
            .into_iter()
            .map(|(_, value)| serde_json::from_value(value).unwrap())
            .collect()
    }
    fn social(&self, mint: &str, messages: &[&str]) -> Value {
        json!({"token":{"contractAddress":mint,"marketHistoryOffset":0,"marketFetchedAt":TIME},
            "marketHistoryPath":self.market,"socialHistoryPath":self.receipts,"matchMethod":"contractAddress",
            "socialEvents":messages.iter().enumerate().map(|(i, text)| json!({
                "id":format!("j7-{i}"),"receivedAt":"2026-09-09T00:01:00Z",
                "event":{"contractAddress":mint,"source":"j7","text":text,"author":"original",
                "detectedAt":"2026-09-09T00:00:30Z","tweetUrl":"https://x.com/example/status/123"}
            })).collect::<Vec<_>>()})
    }
    async fn cleanup(self) {
        tokio::fs::remove_dir_all(self.root).await.unwrap();
    }
}

fn market() -> Value {
    serde_json::to_value(MarketObservation {
        discovered_at: TIME.parse().unwrap(),
        fetched_at: TIME.parse().unwrap(),
        status: "ACCEPTED".into(),
        market: MarketSnapshot {
            contract_address: "MintA".into(),
            symbol: Some("SAME".into()),
            price_usd: Some(0.0),
            source: Some("gmgn:trenches".into()),
            ..MarketSnapshot::default()
        },
        prefilter: PrefilterResult {
            rejected: false,
            reasons: vec![],
            warnings: vec!["liquidez ausente".into()],
        },
    })
    .unwrap()
}

fn onchain(mint: &str) -> Value {
    json!({"marketRecordId":"opaque-original-id","marketFetchedAt":TIME,"observedAt":"2026-09-09T00:00:10Z",
        "onChain":{"contractAddress":mint,"source":"solana-rpc:getMultipleAccounts","commitment":"confirmed",
        "slot":42,"tokenProgram":"original-program","reportedExtensions":[]}})
}

async fn append(path: &PathBuf, value: &Value) {
    JsonlStore::new(path)
        .with_notify(Arc::new(Notify::new()))
        .append(value)
        .await
        .unwrap();
}

#[tokio::test]
async fn accepted_combines_market_onchain_social_and_preserves_provenance() {
    let f = Fixture::new().await;
    append(&f.market, &market()).await;
    append(&f.onchain, &onchain("MintA")).await;
    append(&f.social, &f.social("MintA", &["first"])).await;
    f.assembler().await.reconcile().await.unwrap();
    let rows = f.contexts().await;
    assert_eq!(rows.len(), 1);
    let c = &rows[0];
    assert_eq!(serde_json::to_value(&c.market).unwrap(), market()["market"]);
    assert_eq!(
        serde_json::to_value(&c.prefilter).unwrap(),
        market()["prefilter"]
    );
    assert_eq!(c.on_chain.as_ref().unwrap().slot, Some(42));
    assert_eq!(c.social_events[0].id, "j7-0");
    assert_eq!(c.social_events[0].event.text.as_deref(), Some("first"));
    assert_eq!(
        c.social_events[0].event.detected_at.unwrap().to_rfc3339(),
        "2026-09-09T00:00:30+00:00"
    );
    assert_eq!(c.provenance.market.history.path, f.market);
    assert_eq!(
        c.provenance.on_chain.as_ref().unwrap().market_record_id,
        "opaque-original-id"
    );
    assert_eq!(
        c.provenance
            .on_chain
            .as_ref()
            .unwrap()
            .observed_at
            .to_rfc3339(),
        "2026-09-09T00:00:10+00:00"
    );
    assert_eq!(
        c.provenance.social.as_ref().unwrap().social_history_path,
        f.receipts
    );
    assert!(!c.missing_data.contains(&"onChain".into()));
    assert!(!c.missing_data.contains(&"socialEvents".into()));
    assert!(!c
        .missing_data
        .contains(&"onChain.reportedExtensions".into()));
    f.cleanup().await;
}

#[tokio::test]
async fn accepted_without_onchain_keeps_none() {
    let f = Fixture::new().await;
    append(&f.market, &market()).await;
    append(&f.social, &f.social("MintA", &["first"])).await;
    f.assembler().await.reconcile().await.unwrap();
    let c = f.contexts().await.remove(0);
    assert!(c.on_chain.is_none() && c.provenance.on_chain.is_none());
    assert!(c.missing_data.contains(&"onChain".into()));
    assert_eq!(c.social_events.len(), 1);
    f.cleanup().await;
}

#[tokio::test]
async fn accepted_without_social_has_empty_events() {
    let f = Fixture::new().await;
    append(&f.market, &market()).await;
    append(&f.onchain, &onchain("MintA")).await;
    f.assembler().await.reconcile().await.unwrap();
    let c = f.contexts().await.remove(0);
    assert!(c.social_events.is_empty() && c.provenance.social.is_none());
    assert!(c.missing_data.contains(&"socialEvents".into()));
    f.cleanup().await;
}

#[tokio::test]
async fn later_social_revision_appends_without_overwriting_original() {
    let f = Fixture::new().await;
    append(&f.market, &market()).await;
    let mut a = f.assembler().await;
    a.reconcile().await.unwrap();
    append(&f.social, &f.social("MintA", &["first"])).await;
    a.reconcile().await.unwrap();
    append(&f.social, &f.social("MintA", &["first", "second"])).await;
    a.reconcile().await.unwrap();
    a.reconcile().await.unwrap();
    let rows = f.contexts().await;
    assert_eq!(rows.len(), 3);
    assert!(rows[0].social_events.is_empty());
    assert_eq!(rows[1].social_events.len(), 1);
    assert_eq!(rows[2].social_events.len(), 2);
    assert_eq!(
        rows[0].provenance.market.history,
        rows[2].provenance.market.history
    );
    assert!(
        rows[2].provenance.social.as_ref().unwrap().history.offset
            > rows[1].provenance.social.as_ref().unwrap().history.offset
    );
    f.cleanup().await;
}

#[tokio::test]
async fn rejected_inconsistent_and_legacy_never_generate_contexts() {
    let f = Fixture::new().await;
    for (status, rejected) in [
        ("REJECTED", true),
        ("REJECTED", false),
        ("ACCEPTED", true),
        ("", false),
    ] {
        let mut m = market();
        m["status"] = json!(status);
        m["prefilter"]["rejected"] = json!(rejected);
        append(&f.market, &m).await;
    }
    append(&f.onchain, &onchain("MintA")).await;
    append(&f.social, &f.social("MintA", &["first"])).await;
    f.assembler().await.reconcile().await.unwrap();
    assert!(f.contexts().await.is_empty());
    f.cleanup().await;
}

#[tokio::test]
async fn other_mint_or_other_market_time_onchain_never_matches() {
    let f = Fixture::new().await;
    append(&f.market, &market()).await;
    append(&f.onchain, &onchain("MintB")).await;
    let mut wrong_time = onchain("MintA");
    wrong_time["marketFetchedAt"] = json!("2026-09-09T00:05:00Z");
    append(&f.onchain, &wrong_time).await;
    f.assembler().await.reconcile().await.unwrap();
    assert!(f.contexts().await[0].on_chain.is_none());
    f.cleanup().await;
}

#[tokio::test]
async fn other_mint_or_inconsistent_social_cannot_replace_matching_revision() {
    let f = Fixture::new().await;
    append(&f.market, &market()).await;
    append(&f.social, &f.social("MintB", &["wrong"])).await;
    let mut a = f.assembler().await;
    a.reconcile().await.unwrap();
    assert!(f.contexts().await[0].social_events.is_empty());
    append(&f.social, &f.social("MintA", &["correct"])).await;
    for field in ["mint", "eventMint", "time", "offset", "path"] {
        let mut row = f.social("MintA", &["wrong"]);
        match field {
            "mint" => row["token"]["contractAddress"] = json!("MintB"),
            "eventMint" => row["socialEvents"][0]["event"]["contractAddress"] = json!("MintB"),
            "time" => row["token"]["marketFetchedAt"] = json!("2026-09-09T00:05:00Z"),
            "offset" => row["token"]["marketHistoryOffset"] = json!(999),
            _ => row["marketHistoryPath"] = json!("different.jsonl"),
        }
        append(&f.social, &row).await;
    }
    a.reconcile().await.unwrap();
    let rows = f.contexts().await;
    assert_eq!(rows.len(), 2);
    assert_eq!(
        rows[1].social_events[0].event.text.as_deref(),
        Some("correct")
    );
    f.cleanup().await;
}

#[tokio::test]
async fn missing_data_is_deterministic_and_zero_is_not_missing_or_a_risk() {
    let f = Fixture::new().await;
    append(&f.market, &market()).await;
    let mut o = onchain("MintA");
    o["onChain"]["reportedExtensions"] = Value::Null;
    append(&f.onchain, &o).await;
    f.assembler().await.reconcile().await.unwrap();
    let c = f.contexts().await.remove(0);
    assert_eq!(c.market.price_usd, Some(0.0));
    assert!(c.market.liquidity_usd.is_none());
    assert!(c.on_chain.as_ref().unwrap().reported_extensions.is_none());
    assert_eq!(
        c.missing_data,
        vec![
            "token.name",
            "market.marketCapUsd",
            "market.liquidityUsd",
            "market.volume5mUsd",
            "market.volume1hUsd",
            "market.holders",
            "market.top10HolderPct",
            "market.creatorHolderPct",
            "market.sniperHolderPct",
            "market.bundledHolderPct",
            "market.mintAuthorityRevoked",
            "market.freezeAuthorityRevoked",
            "market.createdAt",
            "onChain.reportedExtensions",
            "socialEvents"
        ]
    );
    assert!(!c.prefilter.rejected);
    assert_eq!(c.prefilter.warnings, vec!["liquidez ausente"]);
    f.cleanup().await;
}

#[tokio::test]
async fn restart_rebuilds_without_duplicate_context_and_uses_latest_social() {
    let f = Fixture::new().await;
    append(&f.market, &market()).await;
    append(&f.social, &f.social("MintA", &["first"])).await;
    append(&f.social, &f.social("MintA", &["first", "second"])).await;
    f.assembler().await.reconcile().await.unwrap();
    f.assembler().await.reconcile().await.unwrap();
    let rows = f.contexts().await;
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].social_events.len(), 2);
    f.cleanup().await;
}

#[tokio::test]
async fn ambiguous_onchain_reference_is_not_associated_even_with_legacy_duplicate() {
    let f = Fixture::new().await;
    append(&f.market, &market()).await;
    let mut legacy = market();
    legacy.as_object_mut().unwrap().remove("status");
    append(&f.market, &legacy).await;
    append(&f.onchain, &onchain("MintA")).await;
    f.assembler().await.reconcile().await.unwrap();
    let rows = f.contexts().await;
    assert_eq!(rows.len(), 1);
    assert!(rows[0].on_chain.is_none());
    f.cleanup().await;
}

#[tokio::test]
async fn later_onchain_revises_context_and_store_wakes_both_consumers() {
    let f = Fixture::new().await;
    let social_notify = Arc::new(Notify::new());
    let context_notify = Arc::new(Notify::new());
    JsonlStore::new(&f.market)
        .with_notify(social_notify.clone())
        .with_notify(context_notify.clone())
        .append(&market())
        .await
        .unwrap();
    for notify in [social_notify, context_notify] {
        tokio::time::timeout(std::time::Duration::from_secs(1), notify.notified())
            .await
            .unwrap();
    }
    let mut a = f.assembler().await;
    a.reconcile().await.unwrap();
    append(&f.onchain, &onchain("MintA")).await;
    a.reconcile().await.unwrap();
    let rows = f.contexts().await;
    assert_eq!(rows.len(), 2);
    assert!(rows[0].on_chain.is_none() && rows[1].on_chain.is_some());
    f.cleanup().await;
}
