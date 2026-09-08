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
- `crates/analyst-core`: normalized inputs, basic prefilter, AI reasoning and local JSONL history.
- `j7-bridge`: local browser collector for J7Tracker social events.

## API

`GET /health`

Returns the local service status.

`POST /webhooks/social/j7`

Receives a J7 social event. The event is preserved as social evidence. A social event by itself is not treated as sufficient evidence for a token decision; it must later be correlated with market/on-chain context.

`POST /analyze/market`

Receives a normalized market snapshot and runs the obvious-trash prefilter before optional LLM interpretation.

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

1. new-token discovery on Solana;
2. market/liquidity/volume collector;
3. holder/concentration/basic rug collector;
4. correlation of token + social events;
5. narrative discovery and influence analysis;
6. LLM context assembly;
7. local historical memory and reevaluation of observed tokens.

This repository is intentionally a small base for those steps, not a finished trading bot.
