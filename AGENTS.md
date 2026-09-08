# AGENTS.md — BIGCALLS

## Project purpose

BIGCALLS is a personal, local-only Solana memecoin AI analyst. Its job is to reduce manual research by collecting market, on-chain and social context, then letting an LLM interpret narratives, trends, influence and opportunity quality.

This repository is intentionally small. Do not rebuild the old trading-bot/product stack.

## Non-negotiable scope

- Analysis only for now.
- No wallet management.
- No trade execution.
- No position manager.
- No Jupiter execution client.
- No Telegram trading UI.
- No admin panel, subscriptions, multi-user/auth product layer or commercial SaaS features.
- Do not introduce bankroll/capital assumptions into prompts, filters or logic unless explicitly requested later.
- Do not reintroduce deleted legacy modules just because they existed in repository history.

## Architecture rule

Use code for deterministic work and AI for interpretation.

Code owns:
- data collection;
- normalization;
- storage/history;
- basic metrics;
- basic obvious-trash/rug checks;
- correlation of token identifiers across sources;
- scheduling/monitoring infrastructure.

AI owns:
- market context interpretation;
- narrative recognition;
- trend interpretation;
- social/influencer context;
- relationship between social attention and market behavior;
- uncertainty and missing-data reasoning;
- final analyst verdict.

Do not replace AI reasoning with a giant fixed scoring system.

## Analysis philosophy

The deterministic prefilter must remain small and conservative. It exists only to remove obvious junk/rug conditions and avoid wasting expensive analysis.

Do not invent arbitrary rigid rules merely to make implementation easier. If a threshold is used, keep it configurable and explain what risk it removes.

The analyst verdicts are:
- `IGNORE`
- `OBSERVE`
- `RESEARCH`

Never invent missing market, on-chain or social facts.

## Intended flow

Keep the system conceptually close to:

`token discovery -> market context -> basic risk/on-chain context -> social/narrative context -> AI analysis -> local history/memory`

Market context should be available before the AI makes a token decision. Social events alone are evidence, not a sufficient token decision trigger.

Avoid adding extra ranking/selection layers unless measurements show they are needed. Optimize incrementally rather than prematurely complicating the pipeline.

## Discovery and providers

Provider integrations must be behind small interfaces so sources can be replaced without rewriting the core.

Useful conceptual interfaces:
- `TokenSource`
- `MarketDataProvider`
- `OnChainProvider`
- `SocialProvider`

Current direction:
- Pump.fun can be used as an initial token-discovery universe instead of scanning the entire Solana chain.
- Prefer structured data/APIs over making the AI visually operate dashboards.
- GMGN can be evaluated as a structured market/on-chain data source, but must not become a hard architectural dependency.
- J7 is an initial social/narrative source, not the primary token-discovery mechanism.

Do not call the LLM for every raw chain or J7 event. Assemble useful context first.

## Current repository structure

Only these active project areas are expected unless a new module is clearly justified:

- `crates/app` — local HTTP/runtime layer.
- `crates/analyst-core` — normalized inputs, basic prefilter, AI reasoning and local history.
- `j7-bridge` — browser/relay collector for J7 social events.

Root configuration currently includes:
- `.env.example`
- `.gitignore`
- `Cargo.toml`
- `README.md`

## Storage

Keep the MVP local and simple. JSONL is acceptable while history is append-oriented.

If querying/correlation becomes awkward, prefer SQLite for this personal single-user project before considering a server database such as PostgreSQL.

## Cost discipline

The Solana/memecoin firehose is noisy. Avoid architectures that make paid API or LLM calls for every token/event.

Use cheap deterministic collection/filtering first and spend expensive API/LLM calls only when there is enough context to justify them.

Cost optimization must not turn into an unnecessary extra strategy/scoring layer.

## Implementation rules for Codex

Before changing architecture:
1. inspect the current repository state;
2. preserve the small personal-analysis scope;
3. prefer the smallest coherent change;
4. keep provider-specific code isolated;
5. do not silently add trading/execution behavior;
6. do not add bankroll assumptions;
7. do not add a large scoring framework;
8. keep configuration in environment variables where appropriate;
9. update README/AGENTS.md when an architectural decision materially changes;
10. run relevant checks/tests before considering a change complete.

When requirements are ambiguous, favor the existing BIGCALLS architecture and avoid speculative feature expansion.

User workflow preference: always commit completed project changes after the relevant checks pass. Do not commit changes with compilation errors. Include only files belonging to the completed task.

## Implemented discovery layer

- `analyst-core::discovery::TokenSource` yields normalized `TokenCandidate` records, separate from `MarketSnapshot` and analysis requests.
- `PumpFunSource` uses the third-party PumpPortal live creation feed, accepting only `txType=create` and `pool=pump` events. Keep provider transport and event fields inside that adapter.
- `PUMPFUN_DISCOVERY_ENABLED=true` enables one background collector in `crates/app`; it appends to `data/token-candidates.jsonl` using the existing JSONL store. Discovery is disabled by default.
- Candidates preserve local discovery time and available identity/metadata; do not infer creation time, market metrics or creator identity from the transaction user.
- This stage does not call the LLM, invoke analysis filters, rank candidates or trade. Market enrichment and social correlation remain later steps.
- Recent duplicate suppression is bounded and in memory. Reconnects resubscribe but do not backfill missing events; provider failures must not take down the HTTP API.

## Implemented market enrichment

- Keep `TokenSource` separate from `MarketDataProvider`. `analyst-core::market` maps candidates to `MarketSnapshot`; optional `priceUsd` is the only analysis-type addition.
- The first adapter uses the official GMGN `GET https://openapi.gmgn.ai/v1/token/info` with `X-APIKEY`, Unix-second `timestamp`, and a fresh UUID `client_id`. Only `GMGN_API_KEY` is configurable; no private key or trading endpoint is used. See README for the verified official references and field mapping.
- When discovery and credentials are available, one market worker consumes persisted, deduplicated candidates through a bounded nonblocking queue. Successful snapshots append to `data/market-snapshots.jsonl`, with local fetch time. Missing data stays `None`; market cap is derived only from returned price and circulating supply.
- Rate limits/timeouts/provider errors must not stop discovery or HTTP. Do not send repeated requests during cooldown. Authentication failure stops market requests; candidates remain preserved. No automatic per-token retries, ranking, thresholds, social processing or AI calls are part of this stage.
- Preserve the fixed PumpPortal address and the current storage/retention strategy of `data/token-candidates.jsonl`; evaluate any changes to those separately later.
