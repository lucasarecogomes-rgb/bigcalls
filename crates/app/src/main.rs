mod context;
mod discovery;
mod history;
mod market;
mod onchain;
mod social;

use std::{env, net::SocketAddr, sync::Arc};

use analyst_core::discovery::pumpfun::PumpFunSource;
use analyst_core::market::gmgn::GmgnMarketDataProvider;
use analyst_core::{
    prefilter, record_id, AiAnalyst, AnalysisRecord, AnalysisRequest, AnalystConfig, JsonlStore,
    SocialEvent, SocialIngestRecord,
};
use anyhow::Context;
use axum::{
    extract::State,
    http::StatusCode,
    response::IntoResponse,
    routing::{get, post},
    Json, Router,
};
use chrono::Utc;
use serde::Serialize;
use tracing::{info, warn};

#[derive(Clone)]
struct AppState {
    ai: Option<AiAnalyst>,
    analysis_store: JsonlStore,
    social_store: JsonlStore,
    config: AnalystConfig,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct HealthResponse {
    ok: bool,
    service: &'static str,
    ai_enabled: bool,
    mode: &'static str,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    dotenvy::dotenv().ok();
    init_logging();

    let discovery_enabled = env::var("PUMPFUN_DISCOVERY_ENABLED")
        .unwrap_or_else(|_| "false".into())
        .parse::<bool>()
        .context("PUMPFUN_DISCOVERY_ENABLED must be true or false")?;

    let bind_addr: SocketAddr = env::var("APP_BIND_ADDR")
        .unwrap_or_else(|_| "127.0.0.1:8790".into())
        .parse()?;

    let ai = env::var("OPENAI_API_KEY")
        .ok()
        .filter(|value| !value.trim().is_empty())
        .map(|api_key| {
            let model = env::var("OPENAI_MODEL").unwrap_or_else(|_| "gpt-5.6".into());
            AiAnalyst::new(api_key, model)
        });

    let social_notify = Arc::new(tokio::sync::Notify::new());
    let context_notify = Arc::new(tokio::sync::Notify::new());
    let social_history_path =
        env::var("SOCIAL_HISTORY_PATH").unwrap_or_else(|_| "data/social-history.jsonl".into());
    let state = Arc::new(AppState {
        ai,
        analysis_store: JsonlStore::new(
            env::var("ANALYSIS_HISTORY_PATH")
                .unwrap_or_else(|_| "data/analysis-history.jsonl".into()),
        ),
        social_store: JsonlStore::new(&social_history_path).with_notify(social_notify.clone()),
        config: AnalystConfig::from_env(),
    });

    let app = Router::new()
        .route("/health", get(health))
        .route("/webhooks/social/j7", post(ingest_social))
        .route("/analyze", post(analyze))
        .with_state(state.clone());

    let listener = tokio::net::TcpListener::bind(bind_addr).await?;
    let context_wake = context_notify.clone();
    let context_social_history = social_history_path.clone();
    let context_task = tokio::spawn(async move {
        if let Err(error) = context::run(
            "data/market-snapshots.jsonl".into(),
            "data/onchain-snapshots.jsonl".into(),
            "data/social-contexts.jsonl".into(),
            context_social_history.into(),
            "data/analysis-contexts.jsonl".into(),
            context_wake,
        )
        .await
        {
            warn!(error = %error, "analysis context assembly stopped; source histories remain preserved");
        }
    });
    let social_wake = social_notify.clone();
    let social_output_wake = context_notify.clone();
    let social_task = tokio::spawn(async move {
        if let Err(error) = social::run(
            "data/market-snapshots.jsonl".into(),
            social_history_path.into(),
            "data/social-contexts.jsonl".into(),
            social_wake,
            social_output_wake,
        )
        .await
        {
            warn!(error = %error, "social correlation stopped; source histories remain preserved");
        }
    });
    let market_provider = if discovery_enabled {
        env::var("GMGN_API_KEY")
            .ok()
            .filter(|key| !key.trim().is_empty())
            .and_then(|key| match GmgnMarketDataProvider::new(key) {
                Ok(provider) => Some(provider),
                Err(error) => {
                    warn!(error = %error, "market provider disabled; discovery remains available");
                    None
                }
            })
    } else {
        None
    };
    let (onchain_sink, onchain_task) = if market_provider.is_some() {
        env::var("SOLANA_RPC_URL").ok().filter(|url| !url.trim().is_empty())
            .and_then(|url| match analyst_core::onchain::solana::SolanaRpcProvider::new(&url) {
                Ok(provider) => {
                    let (tx, rx) = tokio::sync::mpsc::channel(8);
                    let onchain_wake = context_notify.clone();
                    let task = tokio::spawn(async move {
                        if let Err(error) = onchain::run(provider, rx, JsonlStore::new("data/onchain-snapshots.jsonl").with_notify(onchain_wake)).await {
                            warn!(error = %error, "on-chain worker stopped; market enrichment and HTTP remain available");
                        }
                    });
                    Some((tx, task))
                }
                Err(error) => { warn!(error = %error, "on-chain provider disabled"); None }
            }).unzip()
    } else {
        (None, None)
    };
    let (market_sink, market_task) = market_provider.map(|provider| {
        let (tx, rx) = tokio::sync::mpsc::channel(256);
        let config = state.config.clone();
        let task = tokio::spawn(async move {
            info!("GMGN read-only market enrichment enabled");
            if let Err(error) = market::run(provider, rx, JsonlStore::new("data/market-snapshots.jsonl").with_notify(social_notify).with_notify(context_notify), config, onchain_sink).await {
                warn!(error = %error, "market enrichment stopped; discovery and HTTP remain available");
            }
        });
        (tx, task)
    }).unzip();
    let discovery_task = discovery_enabled.then(|| {
        tokio::spawn(async move {
            info!("Pump.fun discovery enabled via PumpPortal");
            if let Err(error) = discovery::run(
                PumpFunSource::new(),
                JsonlStore::new("data/token-candidates.jsonl"),
                market_sink,
            )
            .await
            {
                warn!(error = ?error, "discovery stopped; HTTP service remains available");
            }
        })
    });
    info!(%bind_addr, "BIGCALLS iniciado em modo de analise local");
    let result = axum::serve(listener, app).await;
    if let Some(task) = discovery_task {
        task.abort();
    }
    if let Some(task) = market_task {
        task.abort();
    }
    if let Some(task) = onchain_task {
        task.abort();
    }
    social_task.abort();
    context_task.abort();
    result?;
    Ok(())
}

async fn health(State(state): State<Arc<AppState>>) -> Json<HealthResponse> {
    Json(HealthResponse {
        ok: true,
        service: "bigcalls",
        ai_enabled: state.ai.is_some(),
        mode: "analysis-only",
    })
}

async fn ingest_social(
    State(state): State<Arc<AppState>>,
    Json(event): Json<SocialEvent>,
) -> Result<(StatusCode, Json<SocialIngestRecord>), ApiError> {
    let record = SocialIngestRecord {
        id: record_id(&event),
        received_at: Utc::now(),
        event,
    };

    state
        .social_store
        .append(&record)
        .await
        .map_err(ApiError::internal)?;

    Ok((StatusCode::ACCEPTED, Json(record)))
}

async fn analyze(
    State(state): State<Arc<AppState>>,
    Json(request): Json<AnalysisRequest>,
) -> Result<Json<AnalysisRecord>, ApiError> {
    if request.market.contract_address.trim().is_empty() {
        return Err(ApiError::bad_request(
            "market.contractAddress e obrigatorio",
        ));
    }

    let pre = prefilter(&request.market, &state.config);
    let ai = if pre.rejected {
        None
    } else if let Some(client) = &state.ai {
        match client.analyze(&request.market, &request.social, &pre).await {
            Ok(decision) => Some(decision),
            Err(error) => {
                warn!(error = ?error, "analise IA falhou; entrada sera preservada no historico");
                None
            }
        }
    } else {
        None
    };

    let record = AnalysisRecord {
        id: record_id(&request),
        analyzed_at: Utc::now(),
        market: request.market,
        social: request.social,
        prefilter: pre,
        ai,
    };

    state
        .analysis_store
        .append(&record)
        .await
        .map_err(ApiError::internal)?;

    Ok(Json(record))
}

fn init_logging() {
    let filter = tracing_subscriber::EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| "info,reqwest=warn".into());
    tracing_subscriber::fmt().with_env_filter(filter).init();
}

struct ApiError {
    status: StatusCode,
    message: String,
}

impl ApiError {
    fn bad_request(message: impl Into<String>) -> Self {
        Self {
            status: StatusCode::BAD_REQUEST,
            message: message.into(),
        }
    }

    fn internal(error: impl std::fmt::Display) -> Self {
        Self {
            status: StatusCode::INTERNAL_SERVER_ERROR,
            message: error.to_string(),
        }
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> axum::response::Response {
        (
            self.status,
            Json(serde_json::json!({
                "ok": false,
                "error": self.message,
            })),
        )
            .into_response()
    }
}
