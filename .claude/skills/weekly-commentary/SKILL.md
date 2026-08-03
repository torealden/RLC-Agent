---
name: weekly-commentary
description: RLC market commentary discipline — ranked, non-circular price-driver attribution and the curve-vs-inventory divergence check. Use when writing weekly/periodic market commentary, answering "why did X move", or preparing the market section of any client note.
---

# RLC Weekly Commentary

Encodes the two disciplines RLC commentary must always follow, plus the house
conventions. This skill is about HOW to reason and present; per-client format,
brand, and prose rules stay in each client's own process
(memory: feedback_client_process_separation).

## 0. Scope first

- Commodity (or complex) and window (usually week-over-week).
- Audience: **internal** or **client**. If client: Fastmarkets-derived data
  and anything ruled confidential (per-facility profitability, market-field
  calibration) must NOT appear — check before writing, not after.
- All marketing years written "2025/26" style. Units come from the source's
  own labels, never assumed (memory: feedback_units_source_vs_display,
  reference_us_oilseed_unit_convention).

## 1. Gather evidence (query, don't recall)

Run the queries — do not quote numbers from memory or from this file.
Verified-live sources (2026-08-03; re-verify freshness each use):

| What | Source |
|---|---|
| Futures settles, full curve | `gold.futures_daily_validated` (symbol, contract_month, settlement, open_interest) |
| Managed-money positioning + 1-yr percentile | `gold.cftc_sentiment`; extremes in `gold.cftc_mm_extremes`; history in `silver.cftc_position_history` |
| USDA S&D incl. stocks, by vintage | `gold.psd_wasde_vintages` (cite the `vintage`, e.g. WASDE_JUL_26) |
| Monthly crush/oil stocks actuals | `silver.monthly_realized` |
| Ethanol weekly | `gold.eia_ethanol_weekly` |
| Liquid fuel stocks | `gold.us_liquid_fuel_stocks_monthly` |
| Cash/spot marks | `gold.price_mark_best`, `silver.cash_price` |
| Analyst frameworks & thresholds | KG: `get_kg_context(<node>)` — e.g. `cftc.cot`, `corn`, `soybean_oil` |

Do NOT reference `gold.curve_term` (empty shell as of 2026-08-03) or
`gold.cftc_<commodity>_positioning` views (do not exist despite older docs).
If curve_term is populated later, prefer it and update this table.

## 2. Driver attribution — ranked and non-circular

Decompose the move into drivers across these categories:

- **Fundamental**: supply/demand news, S&D revisions, stocks reports, weather
- **Macro**: USD, energy complex, rates/risk appetite
- **Positioning/technical**: managed-money extremes (use the percentile from
  `gold.cftc_sentiment`, and KG rule: >90th percentile net long = liquidation
  risk), option expiry, index roll
- **Policy/geopolitical**: RVO/SRE, tariffs, export bans, sanctions

Rules:
1. **Rank drivers by estimated magnitude.** A list without ranking is an
   inventory, not an attribution. State rough shares or at least an order.
2. **No circular attribution.** "Fund buying" is not a driver unless you name
   the catalyst that made funds buy. If no catalyst is identifiable, say so
   explicitly ("positioning-led, no clear fundamental trigger") — that is
   itself information.
3. **Every driver gets one piece of queryable evidence** (a number, a report,
   a dated event). No evidence → move it to "possible but unverified".
4. Check the KG for standing frameworks BEFORE attributing — several
   thresholds (price architecture floors, breakevens, seasonal norms) are
   already encoded and beat ad-hoc reasoning.

## 3. Curve-vs-inventory divergence check

For each commodity covered:

1. Build the front of the curve from `gold.futures_daily_validated`: front
   month vs 2nd/3rd deferred, latest trade_date. Express the spread in native
   units AND as % annualized. Classify: carry (contango) / inversion
   (backwardation) / flat. Respect the roll convention — front rolls first
   business day of contract month (memory: reference_futures_roll_convention).
2. Pull the stocks trajectory: latest stocks level and direction vs prior
   period and vs seasonal norm (PSD ending stocks by vintage, monthly_realized
   for crush-complex stocks, EIA weeklies for energy).
3. **Confront them.** Inversion + building stocks, or carry + draining
   stocks, is a DIVERGENCE: flag it explicitly and offer the candidate
   explanations (hidden/invisible stocks, logistics premium, positioning
   distortion, data lag) rather than smoothing over it. Agreement is also
   worth one sentence — it raises confidence in the balance.
4. Watch the weekend month-end artifact on USDA month-end stocks
   (memory: reference_weekend_month_end_stock_artifact) before calling a
   surprise draw/build.

## 4. Output shape (internal default)

1. **Lead with the move and the top driver** — one sentence each.
2. Ranked driver table or short list, each with its evidence.
3. Curve/inventory paragraph: structure, spread, divergence flag if any.
4. What would change the view + upcoming releases (WASDE, stocks, COT dates
   from the data-update schedule in CLAUDE.md).
5. Every number carries source + vintage/date. Anything modeled or assumed is
   labeled in the same sentence ("not sourced", "my read").

Client versions inherit 1–4 but format/brand/prose per that client's process.

## Quality gate (before delivering)

- [ ] Drivers ranked; no unranked laundry lists
- [ ] No circular attribution anywhere
- [ ] Curve vs stocks confronted for every commodity covered, divergences flagged
- [ ] All numbers queried this session, with source + vintage cited
- [ ] MY format "2025/26"; units from source labels
- [ ] Client audience: Fastmarkets + confidential layers absent
