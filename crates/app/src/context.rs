use crate::history::Tail;
#[cfg(test)]
mod tests;
use analyst_core::{
    context::{
        AnalysisContext, HistoryReference, MarketObservation, OnChainRecord, SocialContextRecord,
    },
    JsonlStore,
};
use anyhow::Result;
use serde_json::Value;
use std::{collections::BTreeMap, path::PathBuf, sync::Arc};
use tokio::sync::Notify;

pub(crate) struct Assembler {
    market_tail: Tail,
    onchain_tail: Tail,
    social_tail: Tail,
    social_history: PathBuf,
    output: JsonlStore,
    markets: BTreeMap<u64, MarketObservation>,
    onchain: BTreeMap<(String, chrono::DateTime<chrono::Utc>), (u64, OnChainRecord)>,
    social: BTreeMap<u64, Vec<(u64, SocialContextRecord)>>,
    identity_counts: BTreeMap<(String, chrono::DateTime<chrono::Utc>), usize>,
    latest: BTreeMap<u64, Value>,
}

impl Assembler {
    pub(crate) async fn open(
        market: PathBuf,
        onchain: PathBuf,
        social: PathBuf,
        social_history: PathBuf,
        output: PathBuf,
    ) -> Result<Self> {
        let mut latest = BTreeMap::new();
        for (_, value) in Tail::new(output.clone()).read_new().await? {
            if let Ok(context) = serde_json::from_value::<AnalysisContext>(value.clone()) {
                if context.provenance.market.history.path == market {
                    latest.insert(context.provenance.market.history.offset, value);
                }
            }
        }
        Ok(Self {
            market_tail: Tail::new(market),
            onchain_tail: Tail::new(onchain),
            social_tail: Tail::new(social),
            social_history,
            output: JsonlStore::new(output),
            markets: BTreeMap::new(),
            onchain: BTreeMap::new(),
            social: BTreeMap::new(),
            identity_counts: BTreeMap::new(),
            latest,
        })
    }

    pub(crate) async fn reconcile(&mut self) -> Result<()> {
        for (offset, value) in self.market_tail.read_new().await? {
            // Count even legacy/ineligible rows so an ambiguous historical reference cannot match.
            if let (Some(mint), Ok(time)) = (
                value
                    .pointer("/market/contractAddress")
                    .and_then(Value::as_str),
                serde_json::from_value::<chrono::DateTime<chrono::Utc>>(value["fetchedAt"].clone()),
            ) {
                *self
                    .identity_counts
                    .entry((mint.trim().to_owned(), time))
                    .or_default() += 1;
            }
            if let Ok(record) = serde_json::from_value::<MarketObservation>(value) {
                self.markets.insert(offset, record);
            }
        }
        for (offset, value) in self.onchain_tail.read_new().await? {
            if let Ok(record) = serde_json::from_value::<OnChainRecord>(value) {
                self.onchain.insert(
                    (
                        record.on_chain.contract_address.trim().to_owned(),
                        record.market_fetched_at,
                    ),
                    (offset, record),
                );
            }
        }
        for (offset, value) in self.social_tail.read_new().await? {
            if let Ok(record) = serde_json::from_value::<SocialContextRecord>(value) {
                if record.market_history_path == self.market_tail.path
                    && record.social_history_path == self.social_history
                {
                    self.social
                        .entry(record.token.market_history_offset)
                        .or_default()
                        .push((offset, record));
                }
            }
        }
        // Existing on-chain IDs include wall-clock time. The persisted mint/time pair
        // must identify ONE market row, counting rejected rows too; never guess.
        for (offset, market) in &self.markets {
            let key = (
                market.market.contract_address.trim().to_owned(),
                market.fetched_at,
            );
            let onchain = if self.identity_counts.get(&key) == Some(&1) {
                self.onchain.get(&key)
            } else {
                None
            };
            let market_ref = HistoryReference {
                path: self.market_tail.path.clone(),
                offset: *offset,
            };
            let social = self.social.get(offset).and_then(|rows| {
                rows.iter()
                    .rev()
                    .find(|(_, r)| r.matches(market, &market_ref))
            });
            let Some(context) = AnalysisContext::assemble(
                market,
                HistoryReference {
                    path: self.market_tail.path.clone(),
                    offset: *offset,
                },
                onchain.map(|(offset, record)| {
                    (
                        record,
                        HistoryReference {
                            path: self.onchain_tail.path.clone(),
                            offset: *offset,
                        },
                    )
                }),
                social.map(|(offset, record)| {
                    (
                        record,
                        HistoryReference {
                            path: self.social_tail.path.clone(),
                            offset: *offset,
                        },
                    )
                }),
            ) else {
                continue;
            };
            let value = serde_json::to_value(&context)?;
            if self.latest.get(offset) != Some(&value) {
                self.output.append(&context).await?;
                self.latest.insert(*offset, value);
            }
        }
        Ok(())
    }
}

pub(crate) async fn run(
    market: PathBuf,
    onchain: PathBuf,
    social: PathBuf,
    social_history: PathBuf,
    output: PathBuf,
    notify: Arc<Notify>,
) -> Result<()> {
    let mut assembler = Assembler::open(market, onchain, social, social_history, output).await?;
    loop {
        assembler.reconcile().await?;
        notify.notified().await;
    }
}
