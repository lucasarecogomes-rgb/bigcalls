//! Read-only market enrichment, independent of discovery transports and AI.

use std::{future::Future, time::Duration};

use crate::{discovery::TokenCandidate, MarketSnapshot};

pub mod gmgn;

pub trait MarketDataProvider: Send {
    fn fetch_market(
        &mut self,
        candidate: &TokenCandidate,
    ) -> impl Future<Output = Result<MarketSnapshot, MarketDataError>> + Send;
}

/// Safe to log: excludes credentials, request headers and raw provider bodies.
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum MarketDataError {
    #[error("GMGN_API_KEY is missing or invalid")]
    InvalidCredentials,
    #[error("market provider client could not be initialized")]
    Configuration,
    #[error("invalid Solana contract address")]
    InvalidCandidate,
    #[error("market provider request timed out")]
    Timeout,
    #[error("market provider connection failed")]
    Transport,
    #[error("market provider rejected authentication; check credentials and IPv4 access")]
    Authentication,
    #[error("market provider rate limited requests; cooldown {retry_after:?}")]
    RateLimited { retry_after: Duration },
    #[error("market provider returned HTTP {0}")]
    Http(u16),
    #[error("market provider rejected the request")]
    ProviderRejected,
    #[error("market provider returned missing or invalid token data")]
    InvalidResponse,
    #[error("market provider returned a different token address")]
    AddressMismatch,
}
