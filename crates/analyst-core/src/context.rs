//! Typed evidence assembly only: no provider access, decisions or interpretation.
use crate::{onchain::OnChainSnapshot, MarketSnapshot, PrefilterResult, SocialIngestRecord};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::path::PathBuf;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct HistoryReference {
    pub path: PathBuf,
    pub offset: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TokenIdentity {
    pub contract_address: String,
    pub name: Option<String>,
    pub symbol: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct MarketObservation {
    pub discovered_at: DateTime<Utc>,
    pub fetched_at: DateTime<Utc>,
    pub market: MarketSnapshot,
    pub status: String,
    pub prefilter: PrefilterResult,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct OnChainRecord {
    pub market_record_id: String,
    pub market_fetched_at: DateTime<Utc>,
    pub observed_at: DateTime<Utc>,
    pub on_chain: OnChainSnapshot,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SocialTokenReference {
    pub contract_address: String,
    pub market_history_offset: u64,
    pub market_fetched_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SocialContextRecord {
    pub token: SocialTokenReference,
    pub market_history_path: PathBuf,
    pub social_history_path: PathBuf,
    pub match_method: String,
    pub social_events: Vec<SocialIngestRecord>,
}

impl SocialContextRecord {
    pub fn matches(&self, observation: &MarketObservation, market_ref: &HistoryReference) -> bool {
        let mint = observation.market.contract_address.trim();
        self.token.contract_address.trim() == mint
            && self.token.market_fetched_at == observation.fetched_at
            && self.token.market_history_offset == market_ref.offset
            && self.market_history_path == market_ref.path
            && self.match_method == "contractAddress"
            && self
                .social_events
                .iter()
                .all(|e| e.event.contract_address.as_deref().map(str::trim) == Some(mint))
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct MarketProvenance {
    pub history: HistoryReference,
    pub discovered_at: DateTime<Utc>,
    pub fetched_at: DateTime<Utc>,
    pub source: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct OnChainProvenance {
    pub history: HistoryReference,
    /// Opaque historical ID; never recompute record_id (it includes wall-clock time).
    pub market_record_id: String,
    pub market_fetched_at: DateTime<Utc>,
    pub observed_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SocialProvenance {
    pub history: HistoryReference,
    pub social_history_path: PathBuf,
    pub match_method: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ContextProvenance {
    pub market: MarketProvenance,
    pub on_chain: Option<OnChainProvenance>,
    pub social: Option<SocialProvenance>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AnalysisContext {
    pub token: TokenIdentity,
    pub market: MarketSnapshot,
    pub prefilter: PrefilterResult,
    pub on_chain: Option<OnChainSnapshot>,
    pub social_events: Vec<SocialIngestRecord>,
    pub provenance: ContextProvenance,
    pub missing_data: Vec<String>,
}

impl AnalysisContext {
    /// The caller must resolve ambiguous on-chain market references before assembly.
    pub fn assemble(
        observation: &MarketObservation,
        market_ref: HistoryReference,
        onchain: Option<(&OnChainRecord, HistoryReference)>,
        social: Option<(&SocialContextRecord, HistoryReference)>,
    ) -> Option<Self> {
        if observation.status != "ACCEPTED"
            || observation.prefilter.rejected
            || observation.market.contract_address.trim().is_empty()
        {
            return None;
        }
        let mint = observation.market.contract_address.trim();
        let onchain = onchain.filter(|(r, _)| {
            r.on_chain.contract_address.trim() == mint
                && r.market_fetched_at == observation.fetched_at
        });
        let social = social.filter(|(r, _)| r.matches(observation, &market_ref));
        let mut context = Self {
            token: TokenIdentity {
                contract_address: observation.market.contract_address.clone(),
                name: observation.market.name.clone(),
                symbol: observation.market.symbol.clone(),
            },
            market: observation.market.clone(),
            prefilter: observation.prefilter.clone(),
            on_chain: onchain.as_ref().map(|(r, _)| r.on_chain.clone()),
            social_events: social
                .as_ref()
                .map(|(r, _)| r.social_events.clone())
                .unwrap_or_default(),
            provenance: ContextProvenance {
                market: MarketProvenance {
                    history: market_ref,
                    discovered_at: observation.discovered_at,
                    fetched_at: observation.fetched_at,
                    source: observation.market.source.clone(),
                },
                on_chain: onchain.map(|(r, history)| OnChainProvenance {
                    history,
                    market_record_id: r.market_record_id.clone(),
                    market_fetched_at: r.market_fetched_at,
                    observed_at: r.observed_at,
                }),
                social: social.map(|(r, history)| SocialProvenance {
                    history,
                    social_history_path: r.social_history_path.clone(),
                    match_method: r.match_method.clone(),
                }),
            },
            missing_data: vec![],
        };
        context.missing_data = context.missing_fields();
        Some(context)
    }

    fn missing_fields(&self) -> Vec<String> {
        let m = &self.market;
        let mut missing = Vec::new();
        for (field, absent) in [
            ("token.name", self.token.name.is_none()),
            ("token.symbol", self.token.symbol.is_none()),
            ("market.priceUsd", m.price_usd.is_none()),
            ("market.marketCapUsd", m.market_cap_usd.is_none()),
            ("market.liquidityUsd", m.liquidity_usd.is_none()),
            ("market.volume5mUsd", m.volume_5m_usd.is_none()),
            ("market.volume1hUsd", m.volume_1h_usd.is_none()),
            ("market.holders", m.holders.is_none()),
            ("market.top10HolderPct", m.top_10_holder_pct.is_none()),
            ("market.creatorHolderPct", m.creator_holder_pct.is_none()),
            ("market.sniperHolderPct", m.sniper_holder_pct.is_none()),
            ("market.bundledHolderPct", m.bundled_holder_pct.is_none()),
            (
                "market.mintAuthorityRevoked",
                m.mint_authority_revoked.is_none(),
            ),
            (
                "market.freezeAuthorityRevoked",
                m.freeze_authority_revoked.is_none(),
            ),
            ("market.createdAt", m.created_at.is_none()),
            ("market.source", m.source.is_none()),
        ] {
            if absent {
                missing.push(field.to_owned());
            }
        }
        if let Some(onchain) = &self.on_chain {
            for (field, absent) in [
                ("onChain.tokenProgram", onchain.token_program.is_none()),
                (
                    "onChain.reportedExtensions",
                    onchain.reported_extensions.is_none(),
                ),
                ("onChain.slot", onchain.slot.is_none()),
                ("onChain.commitment", onchain.commitment.is_none()),
            ] {
                if absent {
                    missing.push(field.to_owned());
                }
            }
        } else {
            missing.push("onChain".into());
        }
        if self.social_events.is_empty() {
            missing.push("socialEvents".into());
        }
        missing
    }
}
