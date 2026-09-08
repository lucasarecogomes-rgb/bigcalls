//! Token discovery produces evidence for later enrichment, never an analyst verdict.

use std::future::Future;

use anyhow::Result;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

pub mod pumpfun;

/// Identity and metadata observed at discovery time, without inferred market data.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TokenCandidate {
    pub contract_address: String,
    /// Local receipt time, not the token's on-chain creation time.
    pub discovered_at: DateTime<Utc>,
    pub source: String,
    pub provider: String,
    pub name: Option<String>,
    pub symbol: Option<String>,
    pub metadata_uri: Option<String>,
    pub transaction_signature: Option<String>,
    /// Transaction user reported by the provider; not necessarily the creator.
    pub transaction_user: Option<String>,
    pub bonding_curve_address: Option<String>,
}

/// A source owns its transport and normalization. The runtime owns persistence.
/// After an error, the next call must be able to reconnect/resume the source.
pub trait TokenSource: Send {
    fn next_candidate(&mut self) -> impl Future<Output = Result<TokenCandidate>> + Send;
}
