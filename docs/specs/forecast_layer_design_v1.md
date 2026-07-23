# Forecast Layer — Design v1

**Status:** DESIGN ONLY (ledger session 6a). No code, no migrations. Numbered decisions +
an explicit not-verified list. The build is 6b, gated on session 5.
**Date:** 2026-07-23
**Owners:** Claude Code (storage, writers, callables) · Claude Desktop (balance-sheet formulas).
**Depends on:** `flat_file_contract.md` (the locked seam), `reference_vintage_rank_ladder.md`
(rank = order, `vintage` = identity), migration 149 (PSD/WASDE on the ladder).

---

## 0. Verified state (checked this session, not inherited)

Everything below was queried against the live DB / read from code today. Where it corrects a
prior claim, that is called out — per the verify-before-asserting rule.

| Claim under test | Finding | Verdict |
|---|---|---|
| "1–9 forecast band confirmed free" | `min(vintage_rank)` across **all four** tables carrying the column is **10** (`silver.wheat_series`). Nothing in the DB emits below 10. | **TRUE at the floor.** |
| "…nothing in the estate is a forecast except in 1–9" | **False framing.** Three forecast/model vintages already live *above* 10: `MODEL`=30 (`tallow_balance`, `build_tallow_biofuel_use.py`), `FORECAST_SEASONAL`=40 and `RESIDUAL`=50 (`write_oils_supply_flat_files.py`). The *band* 1–9 is free; the *convention* "forecasts go at 1–9" is **not yet true**. | **Corrected — see D3, D8.** |
| "silver.*_series tables" (plural) | Only **`silver.wheat_series`** exists. Wheat is the sole instance of the pattern; every other commodity's flat file is still fed by ad-hoc silver tables (`tallow_balance`, `oil` builders, etc.). | **Wheat is the pilot, not a family.** |
| `core.forecasts` exists for book (a) | Exists, **3 rows** (a stub). Columns include `forecast_date`, `target_date`, `confidence_low`, `confidence_high`, `analyst`, `marketing_year`, `notes`. Date-grain, already has band columns. Scaffold around it: `core.forecast_actual_pairs`, `core.forecast_feedback`, `core.forecasts_historical`, `core.v_forecast_accuracy_by_commodity`, `core.v_forecasts_latest`. | **Book (a)'s home is real and near-empty — good.** |
| `silver.fuel_production_forecast` shape | 902 rows keyed on `is_forecast boolean` — cannot say *which* forecast, *when made*, or *with what band*. | The anti-pattern D1–D4 replace. |
| Writer dedup key | `write_wheat_flat_files.py` dedups `DISTINCT ON (…, vintage_rank)` keeping latest `release_date`. | Forces **D2**: within one key, rank must be unique. |

**Live ladder in `silver.wheat_series` (the reference vocabulary):**
`WINTER_SEEDINGS`=10, `PROSPECTIVE`=20, `ACREAGE`=30, `CROP_PROD_MAY..AUG`=45–48,
`WASDE_CURRENT`=50, `FINAL`=90, `ACTUAL`=99.

**Live ladder in `silver.tallow_balance`:** `MODEL`=30, `SLAUGHTER_DERIVED`=60, `CENSUS_CIR`=80,
`CIR`=85, `CENSUS`=90, `NASS_FATS_OILS`=90, `CIR`=95, `EIA`=95. (The 90/90 and 95/95 collisions
and `CIR` at both 85 and 95 are the "holds by luck" problem — resolved in D7.)

---

## 1. The two books (D1)

**D1. Two structurally separate books. They never share a table.**

| | Book (b) — **mechanical model** | Book (a) — **LLM balance-sheet** |
|---|---|---|
| Home | rows in `silver.<commodity>_series`, ranks **1–9** | `core.forecasts` (+ existing scaffold) |
| Grain | flat-file grain (`commodity, class, series, MY, period`) | date grain (`forecast_date`, `target_date`) |
| Feeds the sheets? | **Yes** — flows through the flat files into the production balance sheets via `MAXIFS(vintage_rank)` | **No** — never written to a flat file, never touches a sheet |
| Purpose | the house forecast (model + Tore's judgment) that RLC actually publishes | an independent LLM forecast, scored against realized to answer "is the LLM beating Tore" |
| Bands | `value_low`/`value_high` columns (D4) | native `confidence_low`/`confidence_high` |

**Why the hard wall:** if the LLM forecast and the model/Tore forecast shared a table, the
comparison that the whole endpoint exists for (`project_symbiotic_forecasting`) would be
self-referential — the LLM could see, or be blended into, the number it is being graded against.
Different tables, different grain, one-way: book (b) → sheets; book (a) → scoreboard only.
`core.forecast_actual_pairs` + `v_forecast_accuracy_by_commodity` already exist to score book (a)
against realized without ever reading book (b).

**Scope note:** book (b)'s home is `silver.<commodity>_series`. Today only `wheat_series` exists.
The forecast rows land there first; other `*_series` tables are created as the flat-file
migration reaches each commodity (that migration is separate, pre-existing work — not this layer).
Until a commodity has a `*_series` table, its forecast has nowhere in book (b) to live; that is a
sequencing fact, not a blocker for the design.

---

## 2. Integer ranks, revisions via columns (D2)

**D2. Ranks are integers. A rank is a forecast *stage*, never a revision or a timestamp.**

- **No decimals.** `1.10` *is* `1.1` in every numeric store and in Excel — decimals collide
  silently. Stages get whole numbers with gaps for inserts (the estate's existing convention).
- **A revision of the same forecast is not a new rank.** It reuses the same `(vintage,
  vintage_rank)` and moves `release_date` / `revision` forward. The writer's
  `DISTINCT ON (…vintage_rank) ORDER BY release_date DESC` (flat_file_contract §7) then keeps the
  latest revision automatically. Re-forecasting corn production in March vs April = same rank,
  newer `release_date`.
- **Two forecasts shown side by side = two ranks.** `MODEL`=3 and `MODEL_ADJUSTED`=6 coexist in
  the same file for the same key because they are two *different* estimates a reader compares —
  not two versions of one estimate.
- **Uniqueness invariant (from the dedup key):** within any `(commodity, class, series,
  marketing_year, period)`, `vintage_rank` is **unique**. Distinct vintages that can co-occur for
  one key must get distinct ranks. The forecast band obeys this by construction — every stage in
  D3 has its own number. This is also what D7 fixes for the actuals band.

---

## 3. The 1–9 vocabulary (D3)

**D3. Ranks 1–9 are the mechanical-forecast band. Assigned stages, sparse, with insert gaps.**

| rank | vintage | what it is | who/what writes it |
|-----:|---------|-----------|--------------------|
| **1** | `MODEL_BASE` | raw callable output — a pure function of (data, assumptions), zero human judgment. The audit anchor: reproducible from inputs alone. | `forecast.run` (D5) |
| **3** | `MODEL` | the house model forecast — `MODEL_BASE` after calibration/blending the model owner considers standard. The default published forecast when Tore adds nothing. | `forecast.run` or model owner |
| **6** | `MODEL_ADJUSTED` | Tore's judgment applied on top of `MODEL`. Present only when he overrides. Sitting beside `MODEL` at a higher rank is what makes "did the human add value" measurable directly in the file. | analyst |
| **9** | `PRE_SEASON` | the last forecast before real survey data begins — the top of the forecast band, the number that stands until `WINTER_SEEDINGS`(10) supersedes it. | model or analyst |

Gaps **2, 4, 5, 7, 8** are reserved for inserts (e.g. a second adjustment pass, an ensemble
member) without renumbering.

**The automatic handoff.** Because the whole forecast band is **below** `WINTER_SEEDINGS`(10),
`MAXIFS(vintage_rank)` upgrades with zero formula change as real data arrives:

```
MODEL_BASE(1) → MODEL(3) → MODEL_ADJUSTED(6) → PRE_SEASON(9)
   → WINTER_SEEDINGS(10) → PROSPECTIVE(20) → ACREAGE(30) → CROP_PROD_*(45–48)
   → WASDE_CURRENT(50) / PSD_WASDE(61–79) → FINAL(90) → ACTUAL(99)
```

The forecast is the floor by design: **any** real report — even the earliest January seedings —
outranks and replaces it. That is the correct epistemics (a survey beats a model) and it means a
stale forecast can never mask a fresh actual.

**What does NOT go in 1–9:** the LLM book (that is book (a), `core.forecasts`, no rank at all).
And gap-filler vintages that are model-derived but only compete with actuals in commodities that
have no survey ladder (`FORECAST_SEASONAL`=40, `RESIDUAL`=50 in oils) stay where they are for now —
see D8.

---

## 4. Bands are mandatory (D4)

**D4. A far-horizon point estimate without a band must be hard to publish — enforced at storage.**

- **Mechanism:** `silver.<commodity>_series` gains two nullable numeric columns
  `value_low`, `value_high`, projected to the flat file as **two trailing columns** (append-only;
  keys 1–8 and `value` keep their positions — flat_file_contract §2 stays a stable seam, bumped to
  v1.1). Desktop's formulas are unaffected until they choose to read the band.
- **Enforcement (the "hard to skip" part):** a CHECK constraint —
  `vintage_rank BETWEEN 1 AND 9  ⇒  value_low IS NOT NULL AND value_high IS NOT NULL
   AND value_low <= value <= value_high`.
  A forecast-band row **cannot be inserted** without a band that brackets the point. Actuals
  (rank ≥ 10) leave the band columns null. The DB, not a convention, is the gate.
- **Ruled by Tore 2026-07-23: hard gate, fail loud, never silent.** He'd rather a legitimate
  band-less forecast throw a loud insert error we handle at build time than let any point estimate
  slip through unbanded. Equality (`value_low = value = value_high`) is permitted so a deliberately
  tight or degenerate band is *expressible* — but a NULL never is. This makes the near-certain cases
  (e.g. crop-progress week 1, where the interval is trivially small) cost one explicit tight band
  rather than an exemption, while the cases that matter most (price forecasts) are forced to carry a
  real interval. No `is_forecast`-style boolean escape hatch, and no nullable-band shortcut — the
  whole point of `silver.fuel_production_forecast`'s failure is that a boolean can't refuse a
  band-less publish.
- **Why columns, not sibling `_lo`/`_hi` series:** sibling series (three rows per forecast) let the
  band go silently missing — exactly what D4 forbids — and can't be CHECK-constrained as a unit.
  Columns keep the forecast atomic (one row = point + band) and mirror `core.forecasts`
  (`confidence_low/high`), so both books express uncertainty the same way.
- **Band semantics** (state in each commodity's `_meta`, don't assume): default is a
  scenario/subjective interval, not a fixed confidence level — document the convention per series so
  the number is not read as a p-value it isn't.

---

## 5. Model callables as pure functions (D5)

**D5. Every mechanical forecast is produced by a callable that is a pure function of
`(data, assumptions)`; a `forecast.run` records the invocation.**

- **Purity:** the callable reads its inputs from arguments only — no `now()`, no hidden global,
  no ambient "latest". Same `(data, assumptions)` → byte-identical output, forever. This is what
  makes `MODEL_BASE`(1) reproducible and auditable and is the precondition for a client-facing
  forecast MCP. Fits the existing `src/kg/callables/` + `callable_invoker` pattern (which already
  logs to `core.kg_callable_invocation`); the forecast callables are new members of that family.
  Current count is **2** callables (`implied_feedstock_value`, `weather_yield`) — per the handoff
  the real gap for this layer is callables, not storage.
- **`core.forecast_run`** (new in 6b) records, per invocation:
  `run_id, callable, callable_version, assumptions jsonb, input_snapshot_ref, produced_vintage,
  produced_rank, target_keys, retain boolean, created_at`.
  - `assumptions jsonb` — the full assumption set (yields, mix, price deck…), so any published
    forecast row traces back to the exact assumptions that made it. Free to capture now, expensive
    to reconstruct later.
  - `retain boolean` — **the publish gate.** `retain=true` runs write their output rows into
    `silver.<commodity>_series` (they become the book). `retain=false` runs are scenario/preview
    explorations: logged for provenance, but their numbers never enter the book or the flat files.
    Lets Tore run ten what-ifs without polluting the forecast of record.
- **Provenance link:** each retained forecast row carries the `run_id` (a column on
  `silver.<commodity>_series`, or a join table if we keep the series schema lean) so
  row → run → assumptions is one hop.

---

## 6. Book (a) — the LLM scoreboard (D6)

**D6. The LLM book stays in `core.forecasts` and is only ever compared to realized, never to book (b).**

No change to `core.forecasts` shape is required by this design — it already has date grain, bands,
and an `analyst`/`source` field to distinguish `LLM` from any other author. Scoring uses the
existing `core.forecast_actual_pairs` + `core.v_forecast_accuracy_by_commodity`. The only rule this
design *adds*: **nothing in book (a) is ever joined into a flat file or a `silver.*_series` row.**
The wall is enforced by keeping them in different schemas with different grain and writing a
one-line check into 6b's build (assert no `source='LLM'` row ever appears in any `*_series` table).

---

## 7. Rank-ladder reconciliation (D7 — the open-for-Tore item, resolved here)

The handoff flagged: `MODEL`=30 above `PROSPECTIVE`=20; rank 90 shared by CENSUS+NASS_FATS_OILS;
95 by CIR+EIA; `CIR` at both 85 and 95. "Not double-counting, but by luck."

**D7. The ladder is a namespaced vocabulary (rank = order, `vintage` = identity) with one hard
invariant: within a single key, rank is unique.** Resolution, by band:

1. **`MODEL`=30 in `tallow_balance` / `build_tallow_biofuel_use.py` is mis-banded.** It is a
   mechanical model output sitting where `ACREAGE` sits in the grain ladder. It doesn't
   double-count today only because tallow has no rank-10/20 vintages to be wrongly overridden — luck.
   **Relocate it to the forecast band (`MODEL`=3) in 6b.** Low urgency, but it is the same word
   meaning the same thing, so it should carry the same rank across the estate.
2. **Duplicate ranks among actuals (90/90, 95/95, 85-vs-95 `CIR`)** violate the uniqueness
   invariant *if and only if* the two vintages ever land on the same `(series, period)`. Verified
   they currently don't collide, but that is not guaranteed by construction. **6b/session-3 fix:**
   give co-occurring actuals distinct ranks with a deterministic tie-order (e.g. CENSUS above NASS,
   or vice-versa — Tore's call on which source wins), and never reuse a rank for two identities
   (`CIR` picks one number). This is a data-cleanup, not a schema change.
3. **The forecast band never has this problem** because D3 assigns every stage its own number and
   D2 forbids sharing.

**Decision for Tore (one, small):** in item 2, when CENSUS and NASS both report the same series/period,
which vintage should win the higher rank? Everything else in D7 is mechanical.

---

## 8. What stays put, on purpose (D8)

**D8. The three above-10 forecast-ish vintages are not one class — reading the code, they split
three ways. (Revised 2026-07-23 after Tore asked whether we can just roll them down; the honest
answer is "two of the three, and the third is a different bug.")**

| current | what it actually is (from the writer) | correct home | roll-down safe? |
|---|---|---|---|
| `MODEL`=30 (`tallow_balance`, `build_tallow_biofuel_use.py`) | genuine mechanical model output | **`MODEL`=3** (D3 band) | **Yes — behavior-neutral.** It is already the floor in tallow (nothing at 4–29); `MAXIFS` picks it for gap periods and actuals(60–95) override it, identically at 30 or 3. |
| `FORECAST_SEASONAL`=40 (`write_oils_supply_flat_files.py`, soybean oil) | genuine 2-MY forward forecast (`trailing-3MY avg × 5yr seasonal`) | **forecast band 1–9** | **Yes — behavior-neutral.** Only on `non_biofuel_use[*]`, whose only other vintage is `RESIDUAL_ACTUAL`=90. Forward periods have only the forecast; history has only the actual. 40→low is invisible to `MAXIFS`. |
| `RESIDUAL`=50 (same writer, **canola** oil) | **not a forecast** — a balance identity `production+imports−biofuel−exports` | **split, not rolled** | **No.** |

**The real finding is an inconsistency, not a mis-rank.** Soybean oil does non-bio use correctly:
`RESIDUAL_ACTUAL`=90 for historical months (a *derived actual*) **plus** `FORECAST_SEASONAL`=40 for
the forward months. **Canola collapses both into one `RESIDUAL`=50** and never separates the
actual-derived history from the forward projection. Rolling canola `RESIDUAL` to 1–9 would relabel
historical, actual-input balance residuals as "forecasts" — and then book (a)/scoring would grade a
residual identity against the same actuals it was computed from. That is worse than leaving it at 50.

**Decisions:**
1. **Roll down the two genuine forecasts** (tallow `MODEL`→3; soy `FORECAST_SEASONAL`→forecast band)
   in 6b. Both verified behavior-neutral above, so this is pure consistency cleanup, cheap and safe.
2. **Fix canola to match soy's split** — historical residual → `RESIDUAL_ACTUAL`=90, forward residual
   → forecast band — rather than roll it. This is a small modeling fix (`sbo_nonbio_series` already
   has the pattern to copy), not a rank move. **Left for Tore's review before touching code**, per his
   ask; the one call for him is confirming the split direction (treat canola exactly like soy).

Going-forward rule unchanged: **1–9 is the canonical forecast home for all new forecast series.**

---

## 9. Not verified / assumptions / open

Label-in-the-same-breath, per the project rule. None of these block the *design*; each is a thing
6b must check before it writes code.

- **[ ] Flat-file schema append is truly non-breaking — and test it on the RIGHT workbook.** D4 adds
  `value_low`/`value_high` as trailing columns; I asserted Desktop's `MAXIFS/SUMIFS` bind only to
  keys 1–8 + `value`. **Do not test this on soy.** Verified this session (sys graph): the soy oil
  flat file `us_soybean_oil_supply_demand.xlsx` is consumed by `us_soybean_complex_bal_sheets.xlsm`
  via **positional external cell links** (`xlsx_extlink`, 0 `flat_file_series` edges) — the *old*
  direct-link path, not the `vintage_rank`/`MAXIFS` contract. An append test there would exercise the
  wrong mechanism and give false confidence. **Run the append test on a genuine SUMIFS-contract
  consumer — the wheat pilot** (`us_wheat_production.xlsx` per `flat_file_contract.md`): add the two
  columns, open the consumer, confirm nothing shifts. Corollary: the spec's "no formula change ever"
  promise holds *only* for workbooks already on the contract; soy's balance sheet (positional links)
  must be migrated to the contract before it benefits — consistent with "only 2 of 71 oils workbooks
  use it."
- **[ ] Soy is a live forecast→flat-file→balance-sheet example, but not of this contract.** It proves
  the plumbing runs (a `FORECAST_SEASONAL` vintage reaches a linked balance sheet); it proves nothing
  about `MAXIFS` auto-upgrade or band columns (its link is positional and its forecast is band-less).
- **[ ] CHECK-constraint enforceability of bands.** The `rank 1–9 ⇒ band present` constraint assumes
  every current and future forecast writer can supply a band. If any mechanical model genuinely
  produces a point with no defensible interval, the constraint blocks it. Not surveyed across the
  (currently 2) callables.
- **[ ] `run_id` on `silver.*_series` vs. a side table.** I recommend a column for one-hop
  provenance, but have not checked whether the flat-file writer's `DISTINCT ON` / passthrough
  tolerates an extra non-key column cleanly. Decide in 6b.
- **[ ] Band semantics per series.** "Scenario interval vs confidence interval" is asserted as a
  documentation task; no convention is actually written for any series yet.
- **[ ] Only `wheat_series` exists.** The design generalizes over `silver.<commodity>_series`, but
  that family is one table today. Every non-wheat forecast has no book-(b) home until that
  commodity's `*_series` migration lands. Sequencing risk, not a design flaw — but real.
- **[ ] D7 item 2 collision is "verified not colliding" as of a prior session, re-confirm before
  changing ranks.** Don't renumber actuals on trust.
- **[ ] `silver.fuel_production_forecast` conversion path.** D1–D4 imply this 902-row
  `is_forecast`-boolean table becomes rank-banded rows, but the mapping (which rows are which
  vintage/rank, whether they carry bands) is unspecified here — a 6b task.

---

## 10. Summary of decisions

- **D1** — two books, never one table: book (b) mechanical → `silver.*_series` ranks 1–9 → sheets;
  book (a) LLM → `core.forecasts` date-grain → scoreboard only.
- **D2** — integer ranks = stages; revisions ride `release_date`/`revision`; rank unique within a key.
- **D3** — 1–9 vocabulary: `MODEL_BASE`(1), `MODEL`(3), `MODEL_ADJUSTED`(6), `PRE_SEASON`(9);
  forecast band is the floor, handoff to survey ladder is automatic via `MAXIFS`.
- **D4** — bands mandatory: `value_low`/`value_high` columns + CHECK constraint on rank 1–9.
- **D5** — pure `(data, assumptions)` callables; `core.forecast_run` carries `assumptions jsonb` +
  `retain` publish gate; forecast row → `run_id` → assumptions.
- **D6** — book (a) unchanged; hard rule: no LLM row ever enters a `*_series`/flat file.
- **D7** — namespaced ladder, unique-rank-within-key invariant; relocate tallow `MODEL`→3;
  de-collide actuals in 6b (one small Tore decision: CENSUS vs NASS tie-order).
- **D8** — the three above-10 vintages split three ways: roll tallow `MODEL`→3 and soy
  `FORECAST_SEASONAL`→band (both behavior-neutral); **split** canola `RESIDUAL`(50) like soy rather
  than roll it (it's a balance identity, not a forecast). Canola split direction awaits Tore's review.
