use super::*;
use serde_json::json;
use std::collections::HashSet;

pub(super) fn request_body() -> Value {
    // Exactly the official client's request structure, without safety presets,
    // numeric filters or sorting. Platform restriction identifies our universe.
    let section = json!({
        "filters": ["offchain", "onchain"], "launchpad_platform_v2": true,
        "limit": 80,
        "launchpad_platform": ["Pump.fun", "pump_mayhem", "pump_mayhem_agent", "pump_agent"],
        "quote_address_type": [4, 5, 3, 1, 13, 0]
    });
    json!({"version": "v2", "new_creation": section,
        "near_completion": section, "completed": section})
}

pub(super) fn normalize(
    data: &Value,
    candidates: &[TokenCandidate],
) -> Result<Vec<MarketSnapshot>, MarketDataError> {
    // Official docs call the middle category `pump`; the live API also returns
    // `near_completion`. Both represent the same documented lifecycle stage.
    let categories = ["new_creation", "pump", "near_completion", "completed"];
    if !categories
        .iter()
        .any(|key| data.get(key).is_some_and(Value::is_array))
    {
        return Err(MarketDataError::InvalidResponse);
    }
    let wanted: HashSet<_> = candidates
        .iter()
        .map(|c| c.contract_address.trim())
        .filter(|address| valid_solana_address(address))
        .collect();
    let mut seen = HashSet::new();
    let mut snapshots = Vec::new();
    for category in categories {
        let Some(rows) = data.get(category) else {
            continue;
        };
        let rows = rows.as_array().ok_or(MarketDataError::InvalidResponse)?;
        for row in rows {
            let Some(address) = text(row.get("address")) else {
                continue;
            };
            if !wanted.contains(address.as_str()) || !seen.insert(address.clone()) {
                continue;
            }
            if row
                .get("chain")
                .and_then(Value::as_str)
                .is_some_and(|chain| chain != "sol")
            {
                continue;
            }
            snapshots.push(MarketSnapshot {
                contract_address: address,
                symbol: text(row.get("symbol")),
                name: text(row.get("name")),
                price_usd: number(row.get("price")),
                // Both aliases are documented USD fields; the live RankItem uses market_cap.
                market_cap_usd: number(row.get("market_cap"))
                    .or_else(|| number(row.get("usd_market_cap"))),
                liquidity_usd: number(row.get("liquidity")),
                volume_1h_usd: number(row.get("volume_1h")),
                // No documented 5m window in Trenches. Never substitute 24h volume.
                volume_5m_usd: None,
                created_at: row
                    .get("created_timestamp")
                    .and_then(integer)
                    .filter(|ts| *ts > 0 && *ts <= Utc::now().timestamp())
                    .and_then(|ts| DateTime::from_timestamp(ts, 0)),
                holders: row
                    .get("holder_count")
                    .and_then(integer)
                    .and_then(|n| u64::try_from(n).ok()),
                top_10_holder_pct: percentage(row.get("top_10_holder_rate")),
                creator_holder_pct: percentage(row.get("creator_balance_rate")),
                mint_authority_revoked: flag(row.get("renounced_mint")),
                freeze_authority_revoked: flag(row.get("renounced_freeze_account")),
                // Bundle trading volume, sniper counts and top-70 holdings are
                // not equivalent to overall bundled/sniper holder percentages.
                source: Some("gmgn:trenches".into()),
                ..MarketSnapshot::default()
            });
        }
    }
    Ok(snapshots)
}

fn percentage(value: Option<&Value>) -> Option<f64> {
    number(value)
        .filter(|ratio| *ratio <= 1.0)
        .map(|ratio| ratio * 100.0)
}

fn flag(value: Option<&Value>) -> Option<bool> {
    let value = value?;
    value.as_bool().or_else(|| match integer(value) {
        Some(0) => Some(false),
        Some(1) => Some(true),
        _ => None,
    })
}

#[cfg(test)]
mod tests {
    use super::super::tests::{candidate, MINT};
    use super::*;

    #[test]
    fn matches_mints_and_only_maps_documented_equivalent_metrics() {
        let row = json!({"address": MINT, "name": "Example", "symbol": "EX", "price": "0.5",
            "market_cap": 500, "liquidity": 100, "volume_1h": "50", "volume_24h": 999,
            "volume_5m": 123, "created_timestamp": 1700000000, "holder_count": "12",
            "top_10_holder_rate": "0.2", "creator_balance_rate": 0.05, "dev_team_hold_rate": 0.9,
            "sniper_count": 4, "top70_sniper_hold_rate": 0.4, "bundler_trader_amount_rate": 0.6,
            "renounced_mint": "1", "renounced_freeze_account": 0});
        let data =
            json!({"new_creation": [row, {"address": "unrequested"}], "near_completion": [row]});
        let records = normalize(&data, &[candidate()]).unwrap();
        assert_eq!(records.len(), 1);
        let record = &records[0];
        assert_eq!(record.price_usd, Some(0.5));
        assert_eq!(record.market_cap_usd, Some(500.0));
        assert_eq!(record.liquidity_usd, Some(100.0));
        assert_eq!(record.volume_1h_usd, Some(50.0));
        assert!(record.volume_5m_usd.is_none());
        assert_eq!(record.holders, Some(12));
        assert_eq!(record.top_10_holder_pct, Some(20.0));
        assert_eq!(record.creator_holder_pct, Some(5.0));
        assert_eq!(record.mint_authority_revoked, Some(true));
        assert_eq!(record.freeze_authority_revoked, Some(false));
        assert!(record.sniper_holder_pct.is_none() && record.bundled_holder_pct.is_none());
        assert_eq!(record.created_at.unwrap().timestamp(), 1700000000);
    }

    #[test]
    fn missing_ambiguous_and_invalid_data_are_not_invented() {
        let data = json!({"pump": [{"address": MINT, "usd_market_cap": "12",
            "top_10_holder_rate": 20, "creator_balance_rate": -1, "dev_team_hold_rate": 0.4,
            "renounced_mint": "unknown", "created_timestamp": 0, "open_timestamp": 1700000000}]});
        let records = normalize(&data, &[candidate()]).unwrap();
        let record = &records[0];
        assert_eq!(record.market_cap_usd, Some(12.0));
        assert!(record.price_usd.is_none() && record.volume_1h_usd.is_none());
        assert!(record.top_10_holder_pct.is_none() && record.creator_holder_pct.is_none());
        assert!(record.mint_authority_revoked.is_none() && record.created_at.is_none());
        assert!(normalize(&json!({"new_creation": []}), &[candidate()])
            .unwrap()
            .is_empty());
        assert!(normalize(&json!({"new_creation": {}}), &[candidate()]).is_err());
    }
}
