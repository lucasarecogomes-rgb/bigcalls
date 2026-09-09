use crate::history::Tail;
use analyst_core::{
    context::{AnalysisContext, HistoryReference, MarketObservation},
    context_ai::{context_hash, ContextAnalysisRecord},
    AiAnalyst, AiDecision, JsonlStore,
};
use anyhow::Result;
use chrono::Utc;
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use std::{
    collections::{HashMap, HashSet},
    future::Future,
    path::{Path, PathBuf},
    sync::Arc,
    time::Duration,
};
use tokio::{
    io::{AsyncBufReadExt, AsyncReadExt, AsyncSeekExt, BufReader, SeekFrom},
    sync::{Mutex, Notify},
    time::Instant,
};
use tracing::warn;

// Test seam only; the production implementation is the existing AiAnalyst.
pub(crate) trait ContextEngine: Send {
    fn model(&self) -> &str;
    fn analyze(&self, context: &AnalysisContext)
        -> impl Future<Output = Result<AiDecision>> + Send;
}
impl ContextEngine for AiAnalyst {
    fn model(&self) -> &str {
        self.model()
    }
    async fn analyze(&self, context: &AnalysisContext) -> Result<AiDecision> {
        self.analyze_context(context).await
    }
}

#[derive(Debug)]
pub(crate) enum AnalysisError {
    InvalidReference,
    Ineligible,
    AlreadyAttempted,
    Unavailable,
    Provider,
    Storage,
}
impl std::fmt::Display for AnalysisError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::InvalidReference => "invalid context history offset or record",
            Self::Ineligible => "context lacks a consistent ACCEPTED market observation",
            Self::AlreadyAttempted => "context already attempted; no automatic resend",
            Self::Unavailable => "context AI unavailable; check OPENAI_API_KEY and startup logs",
            Self::Provider => "OpenAI context analysis failed; no decision persisted",
            Self::Storage => "AI history storage failed; no resend will be attempted",
        })
    }
}

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct Attempt {
    context_hash: String,
    context: HistoryReference,
    attempted_at: chrono::DateTime<Utc>,
    model: String,
}

struct State<T> {
    engine: Option<T>,
    attempted: HashSet<String>,
    completed: HashMap<String, ContextAnalysisRecord>,
    next_request: Instant,
}

pub(crate) struct AnalysisService<T = AiAnalyst> {
    contexts: PathBuf,
    markets: PathBuf,
    history: JsonlStore,
    attempts: JsonlStore,
    state: Mutex<State<T>>,
}

async fn strict_history<T: DeserializeOwned>(path: &Path) -> Result<Vec<T>> {
    let text = match tokio::fs::read_to_string(path).await {
        Ok(text) => text,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(vec![]),
        Err(e) => return Err(e.into()),
    };
    // Fail closed: silently skipping a damaged cost journal could resend a paid request.
    anyhow::ensure!(
        text.is_empty() || text.ends_with('\n'),
        "incomplete AI journal"
    );
    text.lines()
        .map(|line| serde_json::from_str(line).map_err(Into::into))
        .collect()
}

async fn read_at<T: DeserializeOwned>(path: &Path, offset: u64) -> Result<T> {
    let mut file = tokio::fs::File::open(path).await?;
    if offset > 0 {
        file.seek(SeekFrom::Start(offset - 1)).await?;
        let mut byte = [0];
        file.read_exact(&mut byte).await?;
        anyhow::ensure!(byte[0] == b'\n', "offset must identify a line start");
    }
    file.seek(SeekFrom::Start(offset)).await?;
    let mut reader = BufReader::new(file);
    let mut line = String::new();
    reader.read_line(&mut line).await?;
    anyhow::ensure!(line.ends_with('\n'), "context line is incomplete");
    Ok(serde_json::from_str(&line)?)
}

impl<T: ContextEngine> AnalysisService<T> {
    pub(crate) async fn open(
        engine: Option<T>,
        contexts: PathBuf,
        markets: PathBuf,
        history: PathBuf,
        attempts: PathBuf,
    ) -> Result<Self> {
        let records = strict_history::<ContextAnalysisRecord>(&history).await?;
        let mut attempted: HashSet<_> = strict_history::<Attempt>(&attempts)
            .await?
            .into_iter()
            .map(|a| a.context_hash)
            .collect();
        let mut completed = HashMap::new();
        for record in records {
            anyhow::ensure!(
                record.decision.confidence <= 100,
                "invalid persisted AI confidence"
            );
            attempted.insert(record.context_hash.clone());
            completed.insert(record.context_hash.clone(), record);
        }
        Ok(Self {
            contexts,
            markets,
            history: JsonlStore::new(history),
            attempts: JsonlStore::new(attempts),
            state: Mutex::new(State {
                engine,
                attempted,
                completed,
                next_request: Instant::now(),
            }),
        })
    }

    pub(crate) async fn analyze_offset(
        &self,
        offset: u64,
    ) -> std::result::Result<ContextAnalysisRecord, AnalysisError> {
        let context: AnalysisContext = read_at(&self.contexts, offset)
            .await
            .map_err(|_| AnalysisError::InvalidReference)?;
        let reference = &context.provenance.market.history;
        if reference.path != self.markets {
            return Err(AnalysisError::Ineligible);
        }
        let market: MarketObservation = read_at(&self.markets, reference.offset)
            .await
            .map_err(|_| AnalysisError::Ineligible)?;
        if market.status != "ACCEPTED"
            || market.prefilter.rejected
            || context.prefilter.rejected
            || context.token.contract_address != market.market.contract_address
            || context.provenance.market.fetched_at != market.fetched_at
            || serde_json::to_value(&context.market).ok()
                != serde_json::to_value(&market.market).ok()
            || serde_json::to_value(&context.prefilter).ok()
                != serde_json::to_value(&market.prefilter).ok()
        {
            return Err(AnalysisError::Ineligible);
        }
        let hash = context_hash(&context).map_err(|_| AnalysisError::InvalidReference)?;
        // Shared across manual HTTP and auto worker: maximum one request in flight.
        let mut state = self.state.lock().await;
        if let Some(record) = state.completed.get(&hash) {
            return Ok(record.clone());
        }
        if state.attempted.contains(&hash) {
            return Err(AnalysisError::AlreadyAttempted);
        }
        let model = state
            .engine
            .as_ref()
            .ok_or(AnalysisError::Unavailable)?
            .model()
            .to_owned();
        tokio::time::sleep_until(state.next_request).await;
        let reference = HistoryReference {
            path: self.contexts.clone(),
            offset,
        };
        state.attempted.insert(hash.clone());
        self.attempts
            .append_durable(&Attempt {
                context_hash: hash.clone(),
                context: reference.clone(),
                attempted_at: Utc::now(),
                model: model.clone(),
            })
            .await
            .map_err(|_| AnalysisError::Storage)?;
        let result = state.engine.as_ref().unwrap().analyze(&context).await;
        let decision = match result {
            Ok(decision) if decision.confidence <= 100 => decision,
            _ => {
                state.next_request = Instant::now() + Duration::from_secs(60);
                return Err(AnalysisError::Provider);
            }
        };
        let record = ContextAnalysisRecord {
            context: reference,
            contract_address: context.token.contract_address.clone(),
            analyzed_at: Utc::now(),
            model,
            context_hash: hash.clone(),
            decision,
        };
        self.history
            .append_durable(&record)
            .await
            .map_err(|_| AnalysisError::Storage)?;
        state.completed.insert(hash, record.clone());
        Ok(record)
    }
}

pub(crate) fn auto_enabled(value: Option<&str>) -> Result<bool> {
    match value {
        None | Some("false") => Ok(false),
        Some("true") => Ok(true),
        _ => anyhow::bail!("AI_AUTO_ANALYSIS_ENABLED must be true or false"),
    }
}

/// Captured before context assembly starts; existing history is manual-only.
pub(crate) async fn new_context_tail(path: PathBuf) -> Result<Tail> {
    let offset = match tokio::fs::metadata(&path).await {
        Ok(metadata) => metadata.len(),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => 0,
        Err(e) => return Err(e.into()),
    };
    Ok(Tail::from_offset(path, offset))
}

pub(crate) async fn run<T: ContextEngine>(
    enabled: bool,
    service: Arc<AnalysisService<T>>,
    mut tail: Tail,
    notify: Arc<Notify>,
) -> Result<()> {
    if !enabled {
        return Ok(());
    }
    loop {
        for (offset, _) in tail.read_new().await? {
            if let Err(error) = service.analyze_offset(offset).await {
                warn!(offset, error = %error, "context AI did not produce a new analysis; other pipeline stages continue");
            }
        }
        notify.notified().await;
    }
}

#[cfg(test)]
mod tests;
