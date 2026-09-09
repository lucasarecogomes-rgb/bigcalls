use super::*;
use crate::{
    context::{HistoryReference, MarketObservation},
    MarketSnapshot, PrefilterResult,
};

fn context() -> AnalysisContext {
    AnalysisContext::assemble(
        &MarketObservation {
            discovered_at: "2026-09-09T00:00:00Z".parse().unwrap(),
            fetched_at: "2026-09-09T00:00:00Z".parse().unwrap(),
            status: "ACCEPTED".into(),
            market: MarketSnapshot {
                contract_address: "MintA".into(),
                ..MarketSnapshot::default()
            },
            prefilter: PrefilterResult {
                rejected: false,
                reasons: vec![],
                warnings: vec!["liquidez ausente".into()],
            },
        },
        HistoryReference {
            path: "market.jsonl".into(),
            offset: 0,
        },
        None,
        None,
    )
    .unwrap()
}
fn decision() -> Value {
    json!({"verdict":"OBSERVE","confidence":60,"narrative":null,"thesis":"Evidencias limitadas",
        "positives":[],"risks":[],"missingData":["onChain"],"nextChecks":[]})
}
fn response(value: &Value) -> Value {
    json!({"status":"completed","output":[{"type":"message","role":"assistant","content":[{"type":"output_text","text":value.to_string()}]}]})
}

#[test]
fn context_request_uses_strict_schema_and_exact_evidence_payload() {
    let a = AiAnalyst::new("test".into(), "existing-model".into());
    let c = context();
    let request = a.context_request(&c).unwrap();
    assert_eq!(request["model"], "existing-model");
    assert_eq!(request["text"]["format"]["strict"], true);
    assert_eq!(
        request["text"]["format"]["schema"]["additionalProperties"],
        false
    );
    assert_eq!(
        request["text"]["format"]["schema"]["required"]
            .as_array()
            .unwrap()
            .len(),
        8
    );
    assert_eq!(request["store"], false);
    assert!(request.get("tools").is_none());
    let sent: Value =
        serde_json::from_str(request["input"][0]["content"].as_str().unwrap()).unwrap();
    assert_eq!(sent, serde_json::to_value(&c).unwrap());
    let mut rejected = c.clone();
    rejected.prefilter.rejected = true;
    assert!(a.context_request(&rejected).is_err());
    assert_eq!(context_hash(&c).unwrap(), context_hash(&c.clone()).unwrap());
}

#[test]
fn structured_decisions_are_validated_without_filling_missing_fields() {
    assert_eq!(
        parse_response(&response(&decision())).unwrap().confidence,
        60
    );
    for confidence in [json!(101), json!(-1), json!(0.5), json!("60")] {
        let mut d = decision();
        d["confidence"] = confidence;
        assert!(parse_response(&response(&d)).is_err());
    }
    for key in ["narrative", "risks", "nextChecks"] {
        let mut d = decision();
        d.as_object_mut().unwrap().remove(key);
        assert!(parse_response(&response(&d)).is_err());
    }
    let mut d = decision();
    d["extra"] = json!(true);
    assert!(parse_response(&response(&d)).is_err());
    let mut d = decision();
    d["verdict"] = json!("BUY");
    assert!(parse_response(&response(&d)).is_err());
    let mut r = response(&decision());
    r["status"] = json!("incomplete");
    assert!(parse_response(&r).is_err());
    r = response(&decision());
    r["output"][0]["content"][0] = json!({"type":"refusal","refusal":"no"});
    assert!(parse_response(&r).is_err());
    r = response(&decision());
    r["output"][0]["content"][0]["text"] = json!("not JSON");
    assert!(parse_response(&r).is_err());
}

#[tokio::test]
async fn existing_analyst_sends_responses_contract_and_handles_http_failure() {
    use axum::{routing::post, Json, Router};
    let app = Router::new()
        .route(
            "/ok",
            post(|Json(body): Json<Value>| async move {
                assert_eq!(body["text"]["format"]["type"], "json_schema");
                Json(response(&decision()))
            }),
        )
        .route(
            "/fail",
            post(|| async { axum::http::StatusCode::TOO_MANY_REQUESTS }),
        );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let base = format!("http://{}", listener.local_addr().unwrap());
    let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    let a = AiAnalyst::new("test-only".into(), "existing-model".into());
    assert_eq!(
        a.analyze_context_at(&context(), &format!("{base}/ok"))
            .await
            .unwrap()
            .confidence,
        60
    );
    assert!(a
        .analyze_context_at(&context(), &format!("{base}/fail"))
        .await
        .is_err());
    server.abort();
}
