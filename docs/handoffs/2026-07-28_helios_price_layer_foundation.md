# Handoff — Helios Guidance Report price-feed layer: foundation + AMS settle parser

**Date:** 2026-07-28
**Brief:** `clients/Contracts/Helios/CLAUDE_CODE_BRIEF_price_feeds.md` (+ `Price_Series_Register.md`, 29 series)
**Scope this session:** brief steps 1 (storage) + start of 2 (Tier A core), per Tore's two rulings below.

## Tore's rulings this session
1. **History backfill:** ingest the existing 2000→ delayed strips (yfinance/ibkr) into the new layer at a **lower quality_rank** (`NEWS_INDICATIVE`, `can_republish=FALSE`); official settles overlay from ~2025 as `SETTLE_OFFICIAL`. Keep the depth, flag the provenance.
2. **First increment:** storage migrations + the AMS settlement-block parser.

## What shipped (applied + verified)
- **Migrations 156/157/158** (applied to prod `rlc_commodities`; additive-only, reversible by DROP):
  - `156` `reference.price_quality_rank` — the 7-rank vocabulary, HIGHER ordinal = better; min-rank inheritance is `MIN(rank_ordinal)`.
  - `157` `silver.price_mark` (canonical per-obs marks, natural PK, `tenor` never NULL, `can_republish` §9 flag) + `silver.curve_snapshot` (full strips w/ vol/OI for the thin-OI guard).
  - `158` `gold.curve_term` (IFV term stack) + **deferred constraint-trigger tie-out**: `SUM(term_value) == DERIVED_* headline in price_mark` (curve_key=series_key), epsilon `GREATEST(0.01, |v|*1e-4)`, fails loud at COMMIT. **Verified**: 60+25+15=100 commits; 85≠100 raises.
- **AMS settlement collector** `src/agents/collectors/us/ams_settlement_collector.py`:
  - Route: MARS API does **not** expose the settlement block for slug 3192 (only Header/Detail) → PDF fallback (brief's stated route). Slugs 3192/2850/2771 carry an **identical** block; 3192 primary.
  - Lands 6 series × 7 contracts: `ZC ZS ZW ZO KE MWE` — register #1-4 (ZC/ZW/KE/MWE) + free ZS/oats. **ZL absent** (CME route, as brief says). Units cents/bu, `SETTLE_OFFICIAL`, `can_republish=TRUE` (USDA public domain).
  - Writes BOTH a `price_mark` CONTRACT row and a `curve_snapshot` leg per contract (idempotent upserts).
  - Unit test `tests/test_ams_settlement_parser.py` (6 tests, PASS) against fixture `tests/fixtures/ams_settlement/ams_3192_illinois.txt`.
  - Live run: 84 rows landed; logged to `core.collection_status` as `ams_grain_settlement`.
  - Registered: `COLLECTOR_MAP` (dispatcher) + `RELEASE_SCHEDULES` (daily 17:30 ET). Dispatcher resolution verified.

## Known-broken / unverified / NOT done
- **Migrations await Tore's review.** They are applied (to unblock the parser) but the brief says freeze-after-review. Two explicit review points:
  - the tie-out **linkage convention** `curve_key == series_key` (+ same obs_date/tenor);
  - tie-out enforced **from the term side only** — a `DERIVED_*` headline with NO backing terms is currently allowed (mark-side enforcement deliberately deferred so official collectors that write no terms aren't blocked).
- **Sysgraph declaration NOT added** for `ams_grain_settlement` (brief DoD item). Migration 146 = `sys.system_graph`; needs a node/edge decl. Open.
- **Historical backfill NOT done** — ruling 1 above is a separate, reviewable data migration (yfinance/ibkr `futures_daily_settlement` → `price_mark`/`curve_snapshot` at `NEWS_INDICATIVE`). Next obvious step; gives the "as much history as possible" depth.
- One sanity chart per curve for Desktop (brief DoD) not yet rendered.

## Next steps (brief sequence)
1. Historical backfill migration (ruling 1) — 25yr depth into the new layer.
2. Remaining Tier A core: CME ZL (#5), Bursa FCPO official strip (#6), ZCE/DCE EOD (#7/8), Euronext ECO (#9), ICE canola (#10), **FX daily (#11 — no daily FX table exists at all, fully net-new)**, EIA WTI/Brent backfill (#12, silver table empty), AMS DCO ams_3618 (#13), EPA EMTS RIN (#14).
3. Curve module `src/curves/` methods 1-2, then parity chains.

## Notes for whoever picks this up
- Existing price infra is largely **fastmarkets-licensed** (`bronze.credit_prices`, `bronze.feedstock_prices`) = `can_republish=FALSE`; the whole point of this layer is free/official/publishable equivalents.
- `src/scheduler/` (singular) is orphaned; current scheduler is `src/schedulers/` (plural). Execution goes dispatcher → `src/dispatcher/collector_registry.py`.
- `db_query.py` appends `LIMIT` and is single-statement only — use a psycopg2 script (see scratchpad `test_price_layer.py`) for multi-statement/DDL checks.
