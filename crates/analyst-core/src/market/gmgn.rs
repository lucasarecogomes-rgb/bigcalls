//! Official GMGN token-info endpoint. No website scraping or trading routes.
//! References and field semantics are recorded in README.md.

use std::{
    net::{IpAddr, Ipv4Addr},
    time::Duration,
};

use chrono::{DateTime, Utc};
use reqwest::{
    header::{HeaderMap, HeaderValue, RETRY_AFTER},
    Client, StatusCode,
};
use serde_json::Value;
use tokio::time::{sleep_until, Instant};
use uuid::Uuid;

use super::{MarketDataError, MarketDataProvider};
use crate::{discovery::TokenCandidate, MarketSnapshot};

const ENDPOINT: &str = "https://openapi.gmgn.ai/v1/token/info";
const TRENCHES_ENDPOINT: &str = "https://openapi.gmgn.ai/v1/trenches";
mod trenches;
const REQUEST_INTERVAL: Duration = Duration::from_millis(250);
const ERROR_COOLDOWN: Duration = Duration::from_secs(5);
const DEFAULT_RATE_COOLDOWN: Duration = Duration::from_secs(60);

pub struct GmgnMarketDataProvider {
    client: Client,
    endpoint: String,
    trenches_endpoint: String,
    next_request_at: Instant,
}

impl GmgnMarketDataProvider {
    pub fn new(api_key: String) -> Result<Self, MarketDataError> {
        if api_key.trim().is_empty() {
            return Err(MarketDataError::InvalidCredentials);
        }
        let mut key = HeaderValue::from_str(api_key.trim())
            .map_err(|_| MarketDataError::InvalidCredentials)?;
        key.set_sensitive(true);
        let mut headers = HeaderMap::new();
        headers.insert("x-apikey", key);
        let client = Client::builder()
            .default_headers(headers)
            .user_agent("bigcalls/0.1.0")
            .local_address(IpAddr::V4(Ipv4Addr::UNSPECIFIED))
            .redirect(reqwest::redirect::Policy::none())
            .connect_timeout(Duration::from_secs(5))
            .timeout(Duration::from_secs(10))
            .build()
            .map_err(|_| MarketDataError::Configuration)?;
        Ok(Self {
            client,
            endpoint: ENDPOINT.into(),
            trenches_endpoint: TRENCHES_ENDPOINT.into(),
            next_request_at: Instant::now(),
        })
    }

    async fn request(
        &mut self,
        candidate: &TokenCandidate,
    ) -> Result<MarketSnapshot, MarketDataError> {
        let address = candidate.contract_address.trim();
        if !valid_solana_address(address) {
            return Err(MarketDataError::InvalidCandidate);
        }
        let envelope = self.request_json(false, Some(address)).await?;
        normalize_token(&envelope, address)
    }

    async fn request_json(
        &mut self,
        batch: bool,
        address: Option<&str>,
    ) -> Result<Value, MarketDataError> {
        sleep_until(self.next_request_at).await;
        self.next_request_at = Instant::now() + REQUEST_INTERVAL;
        let request = if batch {
            self.client
                .post(&self.trenches_endpoint)
                .json(&trenches::request_body())
        } else {
            self.client.get(&self.endpoint)
        };
        let mut query = vec![
            ("chain", "sol".to_owned()),
            ("timestamp", Utc::now().timestamp().to_string()),
            ("client_id", Uuid::new_v4().to_string()),
        ];
        if let Some(address) = address {
            query.push(("address", address.to_owned()));
        }
        let response = request
            .query(&query)
            .send()
            .await
            .map_err(transport_error)?;
        let status = response.status();
        let headers = response.headers().clone();
        if status == StatusCode::UNAUTHORIZED || status == StatusCode::FORBIDDEN {
            return Err(MarketDataError::Authentication);
        }
        if !status.is_success() && status != StatusCode::TOO_MANY_REQUESTS {
            return Err(MarketDataError::Http(status.as_u16()));
        }
        // Read the body under the client's timeout. Preserve a 429 even if its
        // body is malformed or cannot be read: headers still determine cooldown.
        let body = response.bytes().await;
        let envelope = body
            .as_ref()
            .ok()
            .and_then(|bytes| serde_json::from_slice::<Value>(bytes).ok());
        let limited = status == StatusCode::TOO_MANY_REQUESTS
            || envelope.as_ref().is_some_and(|value| {
                value.get("code").and_then(integer) == Some(429)
                    || matches!(
                        value.get("error").and_then(Value::as_str),
                        Some(
                            "RATE_LIMIT_EXCEEDED"
                                | "RATE_LIMIT_BANNED"
                                | "ERROR_RATE_LIMIT_BLOCKED"
                        )
                    )
            });
        if limited {
            return Err(MarketDataError::RateLimited {
                retry_after: rate_cooldown(&headers, envelope.as_ref(), Utc::now()),
            });
        }
        if let Err(error) = body {
            return Err(transport_error(error));
        }
        let envelope = envelope.ok_or(MarketDataError::InvalidResponse)?;
        match envelope.get("code").and_then(integer) {
            Some(0) => {}
            Some(_) => return Err(MarketDataError::ProviderRejected),
            None => return Err(MarketDataError::InvalidResponse),
        }
        envelope
            .get("data")
            .cloned()
            .ok_or(MarketDataError::InvalidResponse)
    }

    fn apply_cooldown<T>(&mut self, result: &Result<T, MarketDataError>) {
        let cooldown = match result {
            Err(MarketDataError::RateLimited { retry_after }) => Some(*retry_after),
            Err(
                MarketDataError::Timeout
                | MarketDataError::Transport
                | MarketDataError::Http(_)
                | MarketDataError::ProviderRejected
                | MarketDataError::InvalidResponse,
            ) => Some(ERROR_COOLDOWN),
            _ => None,
        };
        if let Some(delay) = cooldown {
            self.next_request_at = Instant::now() + delay;
        }
    }
}

impl MarketDataProvider for GmgnMarketDataProvider {
    async fn fetch_market(
        &mut self,
        candidate: &TokenCandidate,
    ) -> Result<MarketSnapshot, MarketDataError> {
        let result = self.request(candidate).await;
        self.apply_cooldown(&result);
        result
    }

    async fn fetch_markets(
        &mut self,
        candidates: &[TokenCandidate],
    ) -> Result<Vec<MarketSnapshot>, MarketDataError> {
        if candidates.is_empty() {
            return Ok(Vec::new());
        }
        if !candidates
            .iter()
            .any(|candidate| valid_solana_address(candidate.contract_address.trim()))
        {
            return Err(MarketDataError::InvalidCandidate);
        }
        let result = self
            .request_json(true, None)
            .await
            .and_then(|data| trenches::normalize(&data, candidates));
        self.apply_cooldown(&result);
        result
    }
}

fn transport_error(error: reqwest::Error) -> MarketDataError {
    if error.is_timeout() {
        MarketDataError::Timeout
    } else {
        MarketDataError::Transport
    }
}

fn valid_solana_address(address: &str) -> bool {
    (32..=44).contains(&address.len())
        && address.bytes().all(|byte| {
            b"123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz".contains(&byte)
        })
}

fn normalize_token(
    data: &Value,
    requested_address: &str,
) -> Result<MarketSnapshot, MarketDataError> {
    let address = text(data.get("address")).ok_or(MarketDataError::InvalidResponse)?;
    if address != requested_address {
        return Err(MarketDataError::AddressMismatch);
    }
    let price = &data["price"];
    let price_usd = number(price.get("price"));
    // GMGN documents circulating_supply in human token units. Do not substitute
    // total_supply (FDV) or divide by decimals a second time.
    let market_cap_usd = price_usd
        .zip(number(data.get("circulating_supply")))
        .map(|(price, supply)| price * supply)
        .filter(|value| value.is_finite());
    let created_at = data
        .get("creation_timestamp")
        .and_then(integer)
        .filter(|timestamp| *timestamp > 0 && *timestamp <= Utc::now().timestamp())
        .and_then(|timestamp| DateTime::from_timestamp(timestamp, 0));
    Ok(MarketSnapshot {
        contract_address: address,
        symbol: text(data.get("symbol")),
        name: text(data.get("name")),
        price_usd,
        market_cap_usd,
        liquidity_usd: number(data.get("liquidity"))
            .or_else(|| number(data["pool"].get("liquidity"))),
        volume_5m_usd: number(price.get("volume_5m")),
        volume_1h_usd: number(price.get("volume_1h")),
        created_at,
        source: Some("gmgn".into()),
        ..MarketSnapshot::default()
    })
}

fn text(value: Option<&Value>) -> Option<String> {
    value
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_owned)
}

fn number(value: Option<&Value>) -> Option<f64> {
    let value = value?;
    value
        .as_f64()
        .or_else(|| value.as_str()?.trim().parse().ok())
        .filter(|number| number.is_finite() && *number >= 0.0)
}

fn integer(value: &Value) -> Option<i64> {
    value
        .as_i64()
        .or_else(|| value.as_str()?.trim().parse().ok())
}

fn rate_cooldown(headers: &HeaderMap, envelope: Option<&Value>, now: DateTime<Utc>) -> Duration {
    let seconds_until = |timestamp: i64| timestamp.saturating_sub(now.timestamp()).max(0) as u64;
    let reset = headers
        .get("x-ratelimit-reset")
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.parse::<i64>().ok())
        .map(seconds_until);
    let body_reset = envelope
        .and_then(|value| value.get("reset_at"))
        .and_then(integer)
        .map(seconds_until);
    let retry = headers
        .get(RETRY_AFTER)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| {
            value.parse::<u64>().ok().or_else(|| {
                DateTime::parse_from_rfc2822(value)
                    .ok()
                    .map(|date| seconds_until(date.timestamp()))
            })
        });
    // Respect the latest published reset and add the provider's recommended
    // one-second buffer. Bound implausible server values to avoid Instant overflow.
    let seconds = [reset, body_reset, retry]
        .into_iter()
        .flatten()
        .max()
        .unwrap_or(DEFAULT_RATE_COOLDOWN.as_secs())
        .min(86400);
    Duration::from_secs(seconds.saturating_add(1))
}

#[cfg(test)]
mod tests;
