# BIGCALLS

Personal Solana AI analyst. Local-only, analysis-only.

## Scope

BIGCALLS is the base for the MVP we defined:

1. receive/discover new Solana tokens;
2. collect and normalize basic market/on-chain metrics;
3. reject only obvious trash/rug conditions;
4. correlate social activity and narratives;
5. let the LLM interpret context instead of replacing analysis with fixed scores;
6. classify opportunities as `IGNORE`, `OBSERVE` or `RESEARCH`;
7. preserve local analysis history for future memory/context.

It does not manage wallets, execute trades, manage positions, provide subscriptions, admin panels, Telegram UI or multi-user product features.

## Current components

- `crates/app`: local HTTP runtime.
- `crates/analyst-core`: token discovery interfaces/adapters, normalized inputs, basic prefilter, AI reasoning and local JSONL history.
- `j7-bridge`: local browser collector for J7Tracker social events.

## API

`GET /health`

Returns the local service status.

`POST /webhooks/social/j7`

Receives a J7 social event. The event is preserved as social evidence. A social event by itself is not treated as sufficient evidence for a token decision; it must later be correlated with market/on-chain context.

`POST /analyze`

Receives `{"market": { ... }, "social": []}` and runs the obvious-trash prefilter before optional LLM interpretation. `market` is required; `social` defaults to an empty list.

## Pump.fun discovery

Set `PUMPFUN_DISCOVERY_ENABLED=true` in `.env` and run `cargo run -p app`.
Discovery is disabled by default so the existing local API can still run independently.

The runtime starts one background `TokenSource`, implemented by `PumpFunSource` in
`crates/analyst-core/src/discovery/pumpfun.rs`. The adapter subscribes to
`subscribeNewToken` at `wss://pumpportal.fun/api/data`, using
[PumpPortal's creation feed](https://pumpportal.fun/data-api/real-time/).
PumpPortal is a third-party provider. Creation subscriptions are documented as free;
the endpoint was also verified without an API key. Only events with `txType: "create"`
and `pool: "pump"` are accepted because the feed also carries other launchpads.

The flow is:

`PumpPortal creation event -> PumpFunSource -> TokenCandidate -> data/token-candidates.jsonl`

Each JSONL record contains `contractAddress`, `discoveredAt` (local UTC receipt time),
`source: "pump.fun"`, `provider: "pumpportal"`, and optional `name`, `symbol`,
`metadataUri`, `transactionSignature`, `transactionUser` and `bondingCurveAddress`.
Missing metadata remains `null`. The transaction user is not assumed to be the creator.
Metadata URIs are recorded without fetching them. Discovery does not infer creation time
or market metrics, call the LLM, run the analysis prefilter, score/rank tokens, or execute trades.

The runtime suppresses duplicate source/mint pairs within the last 10,000 persisted
candidates in this process. This bounded cache survives reconnects, but not restarts;
the history remains append-only. Source failures retry with delays from 1 to 60 seconds
and resubscribe on a single connection. Connection attempts time out after 15 seconds;
an idle stream is reopened after 90 seconds. Storage errors stop discovery and are logged,
while the HTTP API stays available.

This is a live feed, without backfill: events during downtime can be missed, and
[PumpPortal reports events at processed commitment](https://pumpportal.fun/FAQ/).
Candidates are observations for future market/on-chain enrichment, not confirmed
analysis decisions. `/analyze` and the J7 ingestion flow retain their existing contracts.

## Market enrichment (GMGN)

`TokenSource` remains independent of `MarketDataProvider`. Automatic enrichment
now uses `fetch_markets` (one batch request). The existing `fetch_market` /
`token/info` implementation remains available for explicit future point queries;
the worker never invokes it or falls back to it for missing candidates.

With discovery enabled, configure your own `GMGN_API_KEY` in local `.env`.
Credentials are the only provider setting. No private key is needed. The fixed
PumpPortal endpoint, J7, candidate JSONL retention and `/analyze` are unchanged.

### Verified official Trenches contract

Verified on 2026-09-08 against GMGN's official client/docs at commit
`aa4d29a9b7d1aaaac3d6acd72c13f17575605348`:

- [Route, request body and auth](https://github.com/GMGNAI/gmgn-skills/blob/aa4d29a9b7d1aaaac3d6acd72c13f17575605348/src/client/OpenApiClient.ts):
  `POST https://openapi.gmgn.ai/v1/trenches?chain=sol&timestamp=<unix-seconds>&client_id=<uuid>`,
  with `X-APIKEY` and a JSON body. This POST queries market data; it does not trade.
- [Timestamp/UUID](https://github.com/GMGNAI/gmgn-skills/blob/aa4d29a9b7d1aaaac3d6acd72c13f17575605348/src/client/signer.ts):
  fresh UUID per request, clock within five seconds of the server.
- [Parameters, limits and field semantics](https://github.com/GMGNAI/gmgn-skills/blob/aa4d29a9b7d1aaaac3d6acd72c13f17575605348/skills/gmgn-market/SKILL.md):
  three categories, documented maximum 80 results per category, route weight 3
  in the documented rate-20/capacity-20 limiter.

The body uses `version: "v2"` and `new_creation`, `near_completion`, `completed`
sections, each with `limit: 80`, `filters: ["offchain", "onchain"]`,
`launchpad_platform_v2: true`, and the official Solana quote-address types.
Only the documented Pump.fun platform family is requested:
`Pump.fun`, `pump_mayhem`, `pump_mayhem_agent`, `pump_agent`.
There are no safety presets, market thresholds, custom sorting or ranking.

**Observed differences:** the live API returned 60 rows per category despite
requesting 80, and used `near_completion` where documentation calls the response
key `pump`. The adapter supports both category names. Live rows use `market_cap`,
documented as USD in the shared RankItem reference, while the Trenches table also
documents `usd_market_cap`. Both aliases are accepted. Do not treat the
documented 80 as a guaranteed number of results.

### Batching and coverage

`Pump.fun -> persist TokenCandidate -> buffer (up to 80 / 5 seconds) -> one Trenches request -> exact mint join -> MarketSnapshot`

The buffer flushes when full or five seconds after its first candidate. It makes
no requests while empty. The existing 256-entry input queue stays nonblocking:
a full queue skips enrichment for that candidate but preserves its discovery record.

Trenches is a recent-token feed, **not a bulk lookup accepting our mint list**.
The adapter joins only exact requested Solana addresses, deduplicates repeated
rows and ignores all unrelated tokens. Only matched snapshots append to
`data/market-snapshots.jsonl`, with discovery and fetch times.
Missing candidates remain in `data/token-candidates.jsonl`. There are no
automatic per-token fallbacks, retries, pagination guesses or refresh loops.
Feed coverage is partial: indexing lag, bursts and downtime can cause misses.

For 100 candidates already queued: **100 calls before, 2 calls now** (80 + 20),
a 98% request reduction. This is not a promise to enrich all 100: only feed matches
produce snapshots. With arrivals spread out, cost is one call per nonempty
five-second window (or full batch); sparse arrivals can still mean one call per
candidate. On limiter weight, two Trenches calls cost 6 units versus 100 token-info
calls at weight 1. No extra requests are issued to fill coverage gaps.

A real read-only cross-source check collected 3 new PumpPortal mints and matched
all 3 in one Trenches request. This demonstrates matching, not complete feed coverage.

### Metrics mapped

| MarketSnapshot field | Documented Trenches/RankItem field |
| --- | --- |
| `contractAddress`, `name`, `symbol` | `address`, `name`, `symbol` |
| `priceUsd` | `price` (USD) |
| `marketCapUsd` | `market_cap`, or `usd_market_cap` |
| `liquidityUsd` | `liquidity` |
| `volume1hUsd` | `volume_1h`, only when present; absent in the live sample |
| `createdAt` | `created_timestamp` (Unix seconds), never open/completion/discovery time |
| `holders` | `holder_count` |
| `top10HolderPct` | `top_10_holder_rate * 100` |
| `creatorHolderPct` | `creator_balance_rate * 100`, not the broader dev-team metric |
| `mintAuthorityRevoked` | `renounced_mint` |
| `freezeAuthorityRevoked` | `renounced_freeze_account` |

`volume5mUsd` stays `None`: the Trenches documentation does not establish that
window. Neither 24h volume nor a generic interval volume is substituted.
`sniperHolderPct` stays `None`: sniper wallet counts and the top-70 subset's
holdings are not overall sniper holdings.
`bundledHolderPct` stays `None`: bundler trading-volume ratios are not holding
ratios. No new fields are created for scores or unrelated risk/social metrics.

Missing, malformed, negative/non-finite numbers and invalid ratios stay `None`;
zero is preserved. Ratios must be 0–1 before conversion to percent. No LLM or
analysis prefilter is invoked. Snapshot source is `gmgn:trenches`.

### Failure handling and verification

The provider retains its 5-second connection and 10-second overall timeouts,
minimum 250 ms request spacing, and shared error/cooldown handling.
Rate limits honor the latest `X-RateLimit-Reset`, `reset_at`, or `Retry-After`
plus a one-second buffer (61 seconds by default; implausible values capped at one
day plus buffer). No requests occur during cooldown. A failed batch is logged
and left in candidate history; subsequent batches can proceed.
Authentication or market-history storage failure stops only the market worker.

Offline checks: `cargo fmt --check`, `cargo check --workspace`,
`cargo test --workspace`. Explicit read-only tests require `GMGN_API_KEY` in the
process environment:

```bash
cargo test --workspace live_read_only_trenches -- --ignored --nocapture
cargo test --workspace live_read_only_token_info -- --ignored --nocapture
```

The second command tests the preserved point-query adapter; it is not an
automatic fallback. No real or demo API key is embedded in repository files.

## Market snapshot

The MVP input is intentionally small and can grow as collectors are implemented:

```json
{
  "contractAddress": "SOLANA_MINT",
  "symbol": "TOKEN",
  "name": "Token Name",
  "marketCapUsd": 250000,
  "liquidityUsd": 45000,
  "volume5mUsd": 12000,
  "volume1hUsd": 90000,
  "holders": 850,
  "top10HolderPct": 26.0,
  "creatorHolderPct": 1.8,
  "sniperHolderPct": 7.0,
  "bundledHolderPct": 3.0,
  "mintAuthorityRevoked": true,
  "freezeAuthorityRevoked": true,
  "source": "collector"
}
```

The prefilter exists only to remove obvious bad conditions. It should not become a rigid trading strategy. Narrative strength, timing, influence, social context and missing information belong to the AI reasoning layer.

## AI output

When `OPENAI_API_KEY` is configured, the analyst returns:

```json
{
  "verdict": "IGNORE|OBSERVE|RESEARCH",
  "confidence": 0,
  "narrative": null,
  "thesis": "...",
  "positives": [],
  "risks": [],
  "missingData": [],
  "nextChecks": []
}
```

The model is explicitly instructed not to invent missing data and not to make decisions from fixed score thresholds alone.

## Local storage

Analysis records are appended to `data/analysis-history.jsonl` by default. `data/` stays out of Git.

For the MVP this is simpler and safer than carrying the previous PostgreSQL/multi-user product stack. A structured database can be introduced later when the history/queries justify it.

## Configuration

```bash
cp .env.example .env
```

Then configure the local bind address and, when desired, the LLM API key/model.

## Run

```bash
cargo check
cargo run -p app
```

The service binds to `127.0.0.1:8790` by default.

## J7 collector

The existing J7 browser collector was preserved because it is useful for the social/narrative layer.

```bash
cd j7-bridge/extension
npm install
npm run check
npm run build
```

Load the extension as unpacked in Chrome/Edge and keep J7Tracker open. The extension forwards normalized social events to the local BIGCALLS backend.

The optional relay remains in `j7-bridge/bridge-server`.

## Next implementation priorities

1. holder/concentration/basic rug collector;
2. correlation of token + social events;
3. narrative discovery and influence analysis;
4. LLM context assembly;
5. local historical memory and reevaluation of observed tokens, including market retries/refreshes.

This repository is intentionally a small base for those steps, not a finished trading bot.
