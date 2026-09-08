use std::{
    env,
    path::{Path, PathBuf},
    sync::Arc,
};

use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use reqwest::Client;
use serde::{Deserialize, Serialize};
use serde_json::json;
use sha2::{Digest, Sha256};
use tokio::{
    fs::{self, OpenOptions},
    io::AsyncWriteExt,
    sync::Mutex,
};

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SocialEvent {
    pub source: Option<String>,
    pub event_type: Option<String>,
    pub author: Option<String>,
    pub username: Option<String>,
    pub text: Option<String>,
    pub raw_text: Option<String>,
    pub contract_address: Option<String>,
    pub ticker: Option<String>,
    pub token_name: Option<String>,
    pub tweet_url: Option<String>,
    pub dex_url: Option<String>,
    pub token_url: Option<String>,
    #[serde(default)]
    pub raw_links: Vec<String>,
    pub detected_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, Deserialize, Serialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct MarketSnapshot {
    pub contract_address: String,
    pub symbol: Option<String>,
    pub name: Option<String>,
    pub market_cap_usd: Option<f64>,
    pub liquidity_usd: Option<f64>,
    pub volume_5m_usd: Option<f64>,
    pub volume_1h_usd: Option<f64>,
    pub holders: Option<u64>,
    pub top_10_holder_pct: Option<f64>,
    pub creator_holder_pct: Option<f64>,
    pub sniper_holder_pct: Option<f64>,
    pub bundled_holder_pct: Option<f64>,
    pub mint_authority_revoked: Option<bool>,
    pub freeze_authority_revoked: Option<bool>,
    pub created_at: Option<DateTime<Utc>>,
    pub source: Option<String>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct AnalysisRequest {
    pub market: MarketSnapshot,
    #[serde(default)]
    pub social: Vec<SocialEvent>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum Verdict {
    Ignore,
    Observe,
    Research,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AiDecision {
    pub verdict: Verdict,
    pub confidence: u8,
    pub narrative: Option<String>,
    pub thesis: String,
    #[serde(default)]
    pub positives: Vec<String>,
    #[serde(default)]
    pub risks: Vec<String>,
    #[serde(default)]
    pub missing_data: Vec<String>,
    #[serde(default)]
    pub next_checks: Vec<String>,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct AnalysisRecord {
    pub id: String,
    pub analyzed_at: DateTime<Utc>,
    pub market: MarketSnapshot,
    pub social: Vec<SocialEvent>,
    pub prefilter: PrefilterResult,
    pub ai: Option<AiDecision>,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SocialIngestRecord {
    pub id: String,
    pub received_at: DateTime<Utc>,
    pub event: SocialEvent,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct PrefilterResult {
    pub rejected: bool,
    pub reasons: Vec<String>,
    pub warnings: Vec<String>,
}

#[derive(Debug, Clone)]
pub struct AnalystConfig {
    pub min_liquidity_usd: f64,
    pub max_top10_pct: f64,
    pub max_creator_pct: f64,
    pub max_sniper_pct: f64,
}

impl Default for AnalystConfig {
    fn default() -> Self {
        Self {
            min_liquidity_usd: 5_000.0,
            max_top10_pct: 80.0,
            max_creator_pct: 35.0,
            max_sniper_pct: 50.0,
        }
    }
}

impl AnalystConfig {
    pub fn from_env() -> Self {
        let defaults = Self::default();
        Self {
            min_liquidity_usd: env_f64("MIN_LIQUIDITY_USD", defaults.min_liquidity_usd),
            max_top10_pct: env_f64("MAX_TOP10_HOLDERS_PCT", defaults.max_top10_pct),
            max_creator_pct: env_f64("MAX_CREATOR_HOLDER_PCT", defaults.max_creator_pct),
            max_sniper_pct: env_f64("MAX_SNIPER_PCT", defaults.max_sniper_pct),
        }
    }
}

fn env_f64(key: &str, default: f64) -> f64 {
    env::var(key)
        .ok()
        .and_then(|value| value.trim().parse::<f64>().ok())
        .unwrap_or(default)
}

pub fn prefilter(snapshot: &MarketSnapshot, cfg: &AnalystConfig) -> PrefilterResult {
    let mut reasons = Vec::new();
    let mut warnings = Vec::new();

    if let Some(value) = snapshot.liquidity_usd {
        if value < cfg.min_liquidity_usd {
            reasons.push(format!("liquidez muito baixa: ${value:.0}"));
        }
    } else {
        warnings.push("liquidez ausente".into());
    }

    if let Some(value) = snapshot.top_10_holder_pct {
        if value > cfg.max_top10_pct {
            reasons.push(format!("concentracao top 10 excessiva: {value:.1}%"));
        }
    } else {
        warnings.push("concentracao top 10 ausente".into());
    }

    if let Some(value) = snapshot.creator_holder_pct {
        if value > cfg.max_creator_pct {
            reasons.push(format!("creator concentra {value:.1}%"));
        }
    }

    if let Some(value) = snapshot.sniper_holder_pct {
        if value > cfg.max_sniper_pct {
            reasons.push(format!("snipers concentram {value:.1}%"));
        }
    }

    if matches!(snapshot.mint_authority_revoked, Some(false)) {
        warnings.push("mint authority ainda ativa".into());
    }

    if matches!(snapshot.freeze_authority_revoked, Some(false)) {
        warnings.push("freeze authority ainda ativa".into());
    }

    PrefilterResult {
        rejected: !reasons.is_empty(),
        reasons,
        warnings,
    }
}

#[derive(Clone)]
pub struct AiAnalyst {
    client: Client,
    api_key: String,
    model: String,
}

impl AiAnalyst {
    pub fn new(api_key: String, model: String) -> Self {
        Self {
            client: Client::new(),
            api_key,
            model,
        }
    }

    pub async fn analyze(
        &self,
        market: &MarketSnapshot,
        social: &[SocialEvent],
        prefilter: &PrefilterResult,
    ) -> Result<AiDecision> {
        let input = json!({
            "market": market,
            "social": social,
            "prefilter": prefilter,
        });

        let prompt = format!(
            r#"You are the reasoning layer of a personal Solana memecoin analyst running locally.
Your job is analysis only. Interpret market context, narrative, social activity and basic on-chain risk data.
The deterministic prefilter exists only to remove obvious trash/rug conditions. Do not turn the remaining decision into a rigid fixed score.
Treat market data as the starting point, then evaluate narrative and social evidence around it.
Pay attention to whether a narrative is emerging, who is influencing it, whether attention looks organic or coordinated, how the social evidence relates to market behavior, and which important facts are still missing.
Do not invent metrics or events. Missing data must be listed explicitly.
Return ONLY valid JSON with this exact shape:
{{"verdict":"IGNORE|OBSERVE|RESEARCH","confidence":0,"narrative":null,"thesis":"...","positives":[],"risks":[],"missingData":[],"nextChecks":[]}}

INPUT:
{}"#,
            serde_json::to_string_pretty(&input)?
        );

        let response = self
            .client
            .post("https://api.openai.com/v1/responses")
            .bearer_auth(&self.api_key)
            .json(&json!({
                "model": self.model,
                "input": prompt,
            }))
            .send()
            .await
            .context("OpenAI request failed")?
            .error_for_status()
            .context("OpenAI returned an error status")?;

        let body: serde_json::Value = response.json().await?;
        let text = extract_output_text(&body).context("OpenAI response had no output text")?;
        let cleaned = text
            .trim()
            .trim_start_matches("```json")
            .trim_start_matches("```")
            .trim_end_matches("```")
            .trim();

        serde_json::from_str(cleaned).context("AI returned invalid decision JSON")
    }
}

fn extract_output_text(value: &serde_json::Value) -> Option<String> {
    if let Some(text) = value.get("output_text").and_then(|value| value.as_str()) {
        return Some(text.to_owned());
    }

    for item in value.get("output")?.as_array()? {
        for content in item
            .get("content")
            .and_then(|value| value.as_array())
            .into_iter()
            .flatten()
        {
            if let Some(text) = content.get("text").and_then(|value| value.as_str()) {
                return Some(text.to_owned());
            }
        }
    }

    None
}

#[derive(Clone)]
pub struct JsonlStore {
    path: PathBuf,
    lock: Arc<Mutex<()>>,
}

impl JsonlStore {
    pub fn new(path: impl AsRef<Path>) -> Self {
        Self {
            path: path.as_ref().to_path_buf(),
            lock: Arc::new(Mutex::new(())),
        }
    }

    pub async fn append<T: Serialize>(&self, value: &T) -> Result<()> {
        let _guard = self.lock.lock().await;

        if let Some(parent) = self.path.parent() {
            fs::create_dir_all(parent).await?;
        }

        let mut file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.path)
            .await?;

        let mut line = serde_json::to_vec(value)?;
        line.push(b'\n');
        file.write_all(&line).await?;
        Ok(())
    }
}

pub fn record_id<T: Serialize>(value: &T) -> String {
    let mut hasher = Sha256::new();
    hasher.update(serde_json::to_vec(value).unwrap_or_default());
    hasher.update(Utc::now().timestamp_millis().to_le_bytes());
    format!("{:x}", hasher.finalize())
}
