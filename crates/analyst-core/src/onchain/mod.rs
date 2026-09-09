//! Read-only mint context. No market duplication or risk decisions.
use serde::{Deserialize, Serialize};
use std::future::Future;

pub mod solana;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct OnChainSnapshot {
    pub source: String,
    pub commitment: Option<String>,
    pub contract_address: String,
    pub slot: Option<u64>,
    pub token_program: Option<String>,
    /// Names reported by the RPC parser, not a claim of complete risk coverage.
    pub reported_extensions: Option<Vec<String>>,
}

pub trait OnChainProvider: Send {
    fn fetch_batch(
        &mut self,
        addresses: &[String],
    ) -> impl Future<Output = Result<Vec<OnChainSnapshot>, OnChainError>> + Send;
}

/// Never includes RPC URLs, credentials, response bodies or transport errors.
#[derive(Debug, thiserror::Error)]
pub enum OnChainError {
    #[error("invalid on-chain provider configuration")]
    Configuration,
    #[error("on-chain request failed or timed out")]
    Transport,
    #[error("on-chain provider unavailable or rate limited")]
    Unavailable,
    #[error("invalid on-chain response")]
    InvalidResponse,
}
