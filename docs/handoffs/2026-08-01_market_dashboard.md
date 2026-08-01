# Handoff 2026-08-01 — Market Dashboard (build session)

## ADDENDUM (same session, later): RLC roll convention + Bollinger Bands
**Ruled by Tore:** roll futures on the FIRST BUSINESS DAY OF THE CONTRACT
MONTH (front = nearest contract with delivery month strictly after the
current calendar month) — avoids notice-period trading where a few cash
players price local S&D/basis, not futures. Implemented across strip /
perf table / candlestick / curves (`_RLC_FRONT_FILTER` in db.py); yfinance
FRONT rows are now only the pre-2026-03-03 historical splice (`roll`
column, captioned). On 7/31 this moved ZS front X26→Q26 (18¢ difference).
Candlestick gained Bollinger Bands (20d, 2σ, on by default). Notion session
note written under RLC OS. Memory: reference_futures_roll_convention.md.

## What shipped (all committed + pushed on `helios-price-layer-foundation`)

**New Streamlit app `dashboards/market/`** — launch `scripts/launch_market_dashboard.bat` (port 8510):
- **Price strip on every page**: 13 symbols off `silver.futures_price` FRONT rows,
  badged "settle, as of <date>", per-symbol stale chips (FCPO shows ~145d — correct),
  day changes are SAME-CONTRACT (front-month rolls were contaminating diffs: DC
  "jumped" +12% on 7/31 purely from the N26→U26 roll).
- **Markets page**: 1D/5D/30D/YTD performance table, candlestick + 10d MA,
  forward curves with 1-week and 4-week ghost curves.
- **Projection Comparison page**: reads `gold.projection_comparison_long`
  (migration 165). Coverage chips per source, overlay chart (realized / Mine
  green #3C7D22 / LLM / USDA with WASDE revision-path checkbox), divergence
  table. Mixed units standardize to 1000 MT or are dropped with a banner.
- **Series Explorer**: any table in `dashboards/data/series_registry.py`,
  Line / YoY% / Seasonal modes, freshness chip, CSV download.

**Migration 165** (`gold.projection_comparison_long` + `_coverage`), applied live:
- 4-branch UNION (user / llm / usda / realized), conservative explicit unit
  conversion; TONS verified as SHORT tons two independent ways (MY2023 crush).
- Realized deduped per month before summing — ERS_OCY republishes NASS soy
  crush to the pound; naive SUM exactly doubled crush.
- Realized-vs-USDA now ties to the digit (62,195.8 vs 62,196 KMT MY2023 crush).

**`scripts/harvest_user_estimates.py`** — green-cell (#3C7D22) harvest from
`models/Oilseeds/*/*_bal*_sheets.xls[mx]` into `silver.user_sd_estimate`:
- Commodity from IN-SHEET title (not tab name), filename/title cross-check.
- START-year MY convention (2024 = 2024/25). Idempotent re-runs; new vintage
  only when workbook values changed; scoped is_current flip via
  `silver.mark_previous_estimates_not_current()`.
- Harvested live: **22 commodities, US books only** (generated country books
  have zero green cells — expected), MYs out to 2045.

## Verified facts that contradict prior assumptions
- `silver.futures_price.contract_date` is **NULL on every row** — weekly.py's
  `ORDER BY contract_date` was dead code. Front month = collector's synthetic
  `FRONT` rows (all symbols except FCPO).
- `gold.futures_daily_validated.overall_validation` = NEEDS_REVIEW on **all**
  74,410 rows — the flag is uninformative; no validation marker shown.
- `bronze.cme_settlements` doesn't exist; that collector is a 0-row placeholder.

## Known-broken / needs Tore — status after Tore's review (same day)
1. **`us_lauric_oils_bal_sheets.xlsm`** (the **US** book specifically): tabs
   AND sheet titles are still the US soybean complex — stale clone. The
   country lauric books (Philippines/Indonesia copra, Indonesia/Malaysia
   palm) are real and fine. Loader skips the US book loudly; Tore will fix
   its tabs during his formula pass, then harvest picks it up. **OPEN, with
   Tore.**
2. **Legacy CSV rows: RESOLVED 2026-08-01.** They were demo scaffolding
   loaded 2026-01-30 from `domain_knowledge/balance_sheets/oilseeds/
   us_soybeans.csv`. Backed up to `data/exports/
   user_sd_estimate_legacy_rows_backup_2026-08-01.csv` (source CSV also
   still in domain_knowledge), then deleted. `silver.user_sd_estimate` now
   contains only harvested rows, all START-year convention — the schema
   comment's END-year claim no longer describes any live row.
3. **US canola book uncached greens: PENDING TORE** — open + save
   `models/Oilseeds/United States/us_canola_balance_sheets.xlsx` (NOT the
   Canadian book), then re-run `scripts/harvest_user_estimates.py`.
4. **Negative meal stocks MY2031+: CLOSED per Tore** — at this stage only
   formula integrity matters (a−b=c); values get fixed when projections are
   re-imported under the real methodology. Harvest faithfully, don't gate.
5. Commodity slug `canola/rapeseed_oil` (from title "CANOLA/RAPESEED OIL") is
   ugly; rename the sheet title or add an alias if it bothers.
6. WASDE vintage depth is 2026-only (labeled in the UI). LLM book still ~2 rows
   — forecast generation is the next workstream for the comparison page.
7. FCPO stale since 2026-03-09; no RIN price data exists anywhere (Helios
   register #14 unbuilt).

## Not done / deferred
- `--monthly` harvest into `silver.monthly_expectation` (stub exits; grammar known).
- Feed/Food Grains .xlsm books (corn, wheat) — same grammar, add to glob after
  inspecting one.
- Cash-vs-futures row and fuels mini-panel on Markets page (phase 1.5 stretch).

## Next-session prompt (updated after Tore's review)
> Read docs/handoffs/2026-08-01_market_dashboard.md. Build usda_comp tabs
> across the balance sheet workbooks from gold.psd_wasde_vintages — extend
> the wasde_comp pattern in the US soybean complex book; decide Python-
> written (generated country books) vs VBA/ODBC (hand-maintained US .xlsm)
> per file type. Re-run scripts/harvest_user_estimates.py first to pick up
> the US canola book if Tore has re-saved it. Then: LLM forecast generation
> into core.forecasts is the following workstream.
