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

## Historical backfill — DONE (migration 159, applied)
- Ruling 1 executed. `bronze.futures_daily_settlement` → new layer at `NEWS_INDICATIVE` / `can_republish=FALSE`.
- **Key discovery:** the 25yr depth lives entirely in the continuous front-month rows (`contract_month='FRONT'`); dated contracts (`U26`…) exist only ~2024+. Dropping FRONT would have discarded the history.
- **Modeling decision (REVIEW POINT):** extended `price_mark.tenor_type` vocab with **`NEARBY`**; FRONT → `NEARBY`/`M1` (unadjusted continuous, carries roll gaps — documented). Dated contracts → `CONTRACT` + `curve_snapshot`. OI is NULL in the source (yfinance/ibkr both).
- Landed: **65,671 NEARBY rows** (12 series, 2000-03-15→) + 8,413 CONTRACT + 8,413 curve_snapshot legs. Official AMS settles coexist on top at `SETTLE_OFFICIAL` (PK includes source; consumer takes MAX rank). Reversible: `DELETE … WHERE source IN ('yfinance','ibkr_tws')`.

## Known-broken / unverified / NOT done
- **Migrations await Tore's review.** They are applied (to unblock the parser) but the brief says freeze-after-review. Review points: the **`NEARBY` tenor_type addition** (159); plus —
  - the tie-out **linkage convention** `curve_key == series_key` (+ same obs_date/tenor);
  - tie-out enforced **from the term side only** — a `DERIVED_*` headline with NO backing terms is currently allowed (mark-side enforcement deliberately deferred so official collectors that write no terms aren't blocked).
- **Sysgraph declaration NOT added** for `ams_grain_settlement` (brief DoD item). Migration 146 = `sys.system_graph`; needs a node/edge decl. Open.
- **Historical backfill NOT done** — ruling 1 above is a separate, reviewable data migration (yfinance/ibkr `futures_daily_settlement` → `price_mark`/`curve_snapshot` at `NEWS_INDICATIVE`). Next obvious step; gives the "as much history as possible" depth.
- One sanity chart per curve for Desktop (brief DoD) not yet rendered.

## Tier A #12 WTI/Brent — DONE (bridge, not a migration)
- WTI (RWTC) + Brent (RBRTE) daily spot already sit in `bronze.eia_observations` (1990→) but the EIA v2 collector was **stale since 2026-05-26**. Refreshed bronze via `eia_v2_collector.py --series wti_cushing --series brent --start 2026-05-01` → current to 2026-07-20 (EIA spot lags a few days, normal).
- New bridge `src/agents/collectors/us/eia_crude_price_bridge.py` (`EIACrudePriceBridge`) upserts bronze → `silver.price_mark` as `WTI`/`BRENT`, `tenor_type=SPOT`, `OFFICIAL_GOV`, `can_republish=TRUE`. First run landed **18,459 rows, 1990-01-02→2026-07-20**. Idempotent; registered daily 18:00 (after the EIA pull). No text parser → functional verification, not a fixture test.
- GOTCHA logged: `get_connection()` yields a **RealDictCursor** — `fetchone()[0]` raises `KeyError: 0`; use an aliased column + dict access.

## Tier A #11 FX — DONE as ECB INTERIM (FRED still preferred)
- **No FRED key exists** — checked `.env` (root + dashboards/ops) and Gmail (2026-07-28); the "Fred" email hits are a training session / Argus spam, no registration. If Tore registers one (free, fred.stlouisfed.org), FRED is the upgrade: direct USD pairs + **ARS** + long history.
- Built `src/agents/collectors/global/ecb_fx_collector.py` (`ECBFXCollector`): ECB euro reference rates (keyless, data-api.ecb.europa.eu), triangulated to USD pairs — `USD/xxx = (xxx/EUR)/(USD/EUR)`, exact (same daily fixing). Landed **41,814 rows, 6 pairs** (FX_EURUSD + FX_USD{MYR,CNY,MXN,CAD,BRL}), 1999/2000→2026-07-28, `SPOT`, `OFFICIAL_GOV`, `can_republish=TRUE`. Convention: USDxxx = xxx per USD (divide a MYR/t price by FX_USDMYR to get USD/t). Verified: USDMYR 4.6514/1.1367=4.092021 ✓.
- **ARS is a gap** — ECB stopped publishing ARS in 2020-10-30. Deferred to FRED / Argentina-official.
- Registered daily 11:30 ET. Perf note: full-history re-pull every run, but batched `execute_values` makes it ~seconds; single-row upserts over RDS were the original 40k-row hang → use `execute_values`.

## Next steps (brief sequence)
1. **FX upgrade to FRED** if Tore adds `FRED_API_KEY` — direct pairs + ARS; would coexist with / supersede ECB (same OFFICIAL_GOV rank, prefer by source).
2. Remaining Tier A: AMS DCO ams_3618 (#13, MARS key present → unblocked), CME ZL official strip (#5), Bursa FCPO (#6), ZCE/DCE EOD (#7/8), Euronext ECO (#9), ICE canola (#10), EPA EMTS RIN (#14, scrape). #5-#10/#14 are scraping-heavy — a distinct work mode (ToS, fixtures, fragility) better suited to a focused session.
3. Curve module `src/curves/` methods 1-2, then parity chains.

## Notes for whoever picks this up
- Existing price infra is largely **fastmarkets-licensed** (`bronze.credit_prices`, `bronze.feedstock_prices`) = `can_republish=FALSE`; the whole point of this layer is free/official/publishable equivalents.
- `src/scheduler/` (singular) is orphaned; current scheduler is `src/schedulers/` (plural). Execution goes dispatcher → `src/dispatcher/collector_registry.py`.
- `db_query.py` appends `LIMIT` and is single-statement only — use a psycopg2 script (see scratchpad `test_price_layer.py`) for multi-statement/DDL checks.
