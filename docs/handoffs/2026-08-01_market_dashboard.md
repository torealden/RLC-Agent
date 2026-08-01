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

## Known-broken / needs Tore
1. **`us_lauric_oils_bal_sheets.xlsm` is a stale soybean-complex clone** (tabs
   AND sheet titles still say SOYBEAN). Loader skips it loudly. Rebuild the
   workbook, then re-run the harvest.
2. **3 legacy CSV rows** in `silver.user_sd_estimate` (soybeans, estimate_date
   2026-01-30) have **ambiguous MY convention** (schema comment says END-year,
   values don't tie cleanly either way; look like demo data). Consider deleting
   them: `DELETE FROM silver.user_sd_estimate WHERE estimate_date='2026-01-30'`.
3. **us_canola_balance_sheets.xlsx: 21 green cells have no cached value** —
   open + save in Excel, re-run harvest.
4. **Meal sheet ending stocks go NEGATIVE from MY2031** in the soybean complex
   workbook (down to -817 by 2039) — harvested faithfully; that's in the model.
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

## Next-session prompt
> Read docs/handoffs/2026-08-01_market_dashboard.md. Launch the market
> dashboard (scripts/launch_market_dashboard.bat), review the three pages in a
> browser, then either (a) extend the harvest to the corn/wheat books, or
> (b) wire LLM forecast generation into core.forecasts so the comparison
> page's LLM column fills in.
