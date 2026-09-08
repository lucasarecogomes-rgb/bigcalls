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

1. market/liquidity/volume collector to enrich discovered candidates;
2. holder/concentration/basic rug collector;
3. correlation of token + social events;
4. narrative discovery and influence analysis;
5. LLM context assembly;
6. local historical memory and reevaluation of observed tokens.

This repository is intentionally a small base for those steps, not a finished trading bot.
