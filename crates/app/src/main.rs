use std::{env, net::SocketAddr, sync::Arc};

use analyst_core::{
    prefilter, record_id, AiAnalyst, AnalysisRecord, AnalysisRequest, AnalystConfig, JsonlStore,
    SocialEvent, SocialIngestRecord,
};
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

    let state = Arc::new(AppState {
        ai,
        analysis_store: JsonlStore::new(
            env::var("ANALYSIS_HISTORY_PATH")
                .unwrap_or_else(|_| "data/analysis-history.jsonl".into()),
        ),
        social_store: JsonlStore::new(
            env::var("SOCIAL_HISTORY_PATH")
                .unwrap_or_else(|_| "data/social-history.jsonl".into()),
        ),
        config: AnalystConfig::from_env(),
    });

    let app = Router::new()
        .route("/health", get(health))
        .route("/webhooks/social/j7", post(ingest_social))
        .route("/analyze", post(analyze))
        .with_state(state);

    let listener = tokio::net::TcpListener::bind(bind_addr).await?;
    info!(%bind_addr, "BIGCALLS iniciado em modo de analise local");
    axum::serve(listener, app).await?;
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
        return Err(ApiError::bad_request("market.contractAddress e obrigatorio"));
    }

    let pre = prefilter(&request.market, &state.config);
    let ai = if pre.rejected {
        None
    } else if let Some(client) = &state.ai {
        match client
            .analyze(&request.market, &request.social, &pre)
            .await
        {
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
