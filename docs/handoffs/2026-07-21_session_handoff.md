# Session Handoff — 2026-07-21

**Purpose:** this file exists so the next session starts from a written record rather than a
context summary. Summaries keep conclusions and drop evidence, which is backwards for this work.
Read this, then verify anything it asserts against live data before acting on it.

---

## 1. What shipped (all committed and pushed to `origin/main`)

| Commit | What |
|---|---|
| `8b62f169` | Helios/Pepsi deliverables inventory + internal spreadsheet game plan |
| `9f92df8d` | Pepsi coverage tracker: all 5 SOW complexes, per-complex sheet sets, A–E tiering |
| `79012e6a` | Palm corrected to a full crush complex with two oils; lauric template spec |
| `b4161b6c` | Flat-file **wide render** (balance-sheet shaped) so the mirror tabs can die |
| `ebca65f5` | **Two-sided EIA rake** — biodiesel and RD family raked separately |
| `39d18d55` | VBA `SoyOilRepointToFlatFile.bas` — retire `eia_data.xlsm`, drop mirror tabs |
| `954239d8` | SBO history to 2007, 2011–15 gap fill, (then-wrong) stocks split |
| `c4e33725` | **Stocks = NASS crude + once refined** (Tore's correction) |
| `594d8a86` | **Helios API live** — climate risk index into `bronze.helios_climate_risk` |

---

## 2. State by workstream

### Feedstock / rake — LARGELY FIXED
- The rake now honours EIA's published **BD/RD split** where EIA publishes one (soybean, canola,
  corn oil — and only those). **89 two-sided months tie exactly, worst |diff| 0.0000.**
- Withheld months where EIA published only the combined total are split pro-rata on the same MY's
  published bd:rd ratio (`EIA_TOTAL_SPLIT`, 29 months). Corn Oil Apr-2024 was the entire 340 mil lb
  "EIA inconsistency" — a withheld month, not an inconsistency.
- Tallow and UCO remain **RLC-canonical / exempt** — EIA is not canon there (standing ruling).
- CY2024 SBO now reads BD 7,399.0 / RD-family 5,921.0 / total 13,320.0 against EIA's identical
  7,399 / 5,921 / 13,320.
- A **binding per-month tie-out** now lives in `rake_feedstock_vintage_aware.py`. Its absence is
  what let a 1,789 mil lb error hide behind a correct grand total.

### Flat files / balance-sheet link — BUILT, NOT YET APPLIED
- Flat file now emits `*_wide` tabs: months down (Oct→Sep), MYs across, 16-row blocks, million
  pounds. Grid anchored so **column B = MY1990/91 and column AI = MY2023/24 — identical to the
  balance sheet**, so columns map 1:1 and only rows differ.
- `_wide_index` tab publishes every block's row anchors. **Read it; never count rows.**
- `SoyOilRepointToFlatFile.bas` is written and verified by simulation but **HAS NOT BEEN RUN.**
  Run order: `RepointSoyOilPreview` → `RepointSoyOilApply` → `RepointSoyOilCleanup`.

### SBO history — HOLE CLOSED
- `non_biofuel_use` marketing years **2007–2027 contiguous** (was missing 2011–2014).
- The 2011/12–2014/15 hole was **never an ingest failure**: Census killed the CIR Fats & Oils
  program Jul-2011, NASS started the monthly crush report May-2015. No monthly source exists.
  Filled from ERS **annual** — production by seasonal share (a flow), stocks by seasonal level index
  anchored to published MY-end (a level), non-bio from total-dom-use minus biofuel.
  Flagged `MODELED_GAPFILL` rank 60: above forecast (40), below every actual (85–95).
- `production` extended to Oct-2007 via ERS — verified **ratio 1.000** against NASS on the
  2015–2018 overlap, so it is the same series, not a splice.
- `stocks` = NASS **crude + once refined** (onsite & offsite), 337 months. Reproduces ERS exactly:
  108 overlapping months, worst difference 0.01 mil lb.
- Residual after the stocks fix: MY2024/25 **15,154.5**, MY2025/26 **15,530.8** mil lb — both on
  Tore's ~15 billion reference.

### Helios — CONNECTED
- `bronze.helios_climate_risk`: **226,736 rows, 88 commodity × country pairs, 2021-07-01 →
  2028-07-21**, 64,264 forecast days.
- Four dimensions (too hot / cold / wet / dry) + composite `wapr`, with `wapr_hist_avg` alongside.
- Two gotchas, both documented in the script: country must be the **two-letter code** (`us`, not
  `United States`); an explicit **User-Agent is required** or the edge returns 403 where curl works.
- Bronze only. **Not integrated** — our scenario layer is calibrated to classical inputs and this
  composite is not a drop-in.

---

## 3. OPEN DECISIONS — need Tore, blocking downstream work

1. **Wheat scope.** Wheat is a live workstream in the 2026-07-13 meeting record but appears
   **nowhere in SOW No. 1** (five vegetable-oil complexes). Amend SOW1, write a change order, or
   stop building. Unpaid scope on a $1,500/mo production fee otherwise.
2. **Reference-series citation conflict.** Three of the five contracted reference series (RSO FOB
   Dutch, sunoil six ports, CPO CIF Rotterdam) are **private assessments** we cannot republish
   under SOW §9's government/exchange-only rule. Determines who owns the "market" column in the
   weekly data file. Must be settled during build-out, not after.
3. **Helios v1 vs v3.** Docs reference a `v3 daily_risk` POST; `/v3/daily-risk` 404s on the live
   host. We built on v1. Ask which is canonical before building further.
4. **Dual non-bio.** Carry `non_biofuel_use` (derived residual, closes the sheet) *and*
   `non_biofuel_use_independent` (mechanical forecast, for comparison)? Recommended internally,
   residual only client-facing.

---

## 4. KNOWN BROKEN / UNVERIFIED — do not assume these are fine

- **`silver.monthly_realized` attribute `oil_stocks` is the ONCE-REFINED series alone** (~1/6 of
  total). The flat-file writer now bypasses silver for stocks, but **anything else reading
  `oil_stocks` understates stocks ~6×.** Needs a collector/mapping fix. Tore's balance sheet is
  unaffected — its stocks line already sums `'[2]NASS Crush'!AD + AF`.
- **Biofuel has no forecast.** The raked allocator stops Apr-2026, so MY2025/26 is seven months
  masquerading as a year and MY2026/27 reads **0.0**, while non-bio forecasts to Sep-2028. Either
  forecast biofuel to match or explicitly blank the forecast years so the hole is visible.
- **`bronze.historical_feedstock_allocation` still feeds `eia_data.xlsm`** and disagrees with
  `gold.bbd_feedstock_raked` (+647 mil lb on CY2024 SBO). Retire it as a balance-sheet input.
- **Fats totals do not tie EIA** — Tallow 7,271 vs our 4,138; Yellow Grease 7,387 vs 8,591;
  Poultry 213 vs 620. Partly the deliberate RLC-canonical exemption; **the tallow gap of 3,133 mil
  lb is larger than the exemption explains** and has not been investigated.
- **`SoyOilRepointToFlatFile.bas` has never been executed.**
- **PSD attributes 140 (Industrial Dom. Cons.) / 149 (Food Use Dom. Cons.) are not ingested.**
  `fsi_consumption` and `feed_dom_consumption` are **zero rows populated for every vegetable oil**,
  so we have no biofuel/non-biofuel split outside the US. The attributes exist and are populated at
  USDA — one collector change gets industrial vs food for every oil × every country, and USDA is
  §9-citable.

---

## 5. Research findings to carry (agent-sourced, 2026-07-21)

- Indonesian FAME is **effectively 100% palm**; USDA GAIN shows UCO and POME literally **zero**.
- **PFAD, POME oil and UCO are exported, not domestically consumed** — PFAD exports 3.0–3.5 MMT/yr.
  They belong on our sheets as **export lines, not domestic biofuel use**.
- **Indonesia went to B50 on 1 July 2026**; **Malaysia to B15 on 1 June 2026**. Both postdate every
  GAIN report.
- USDA estimates **1.6–1.7 MMT/yr of CPO was improperly exported as "POME"** in 2022–2024 —
  historical Indonesian CPO exports understated, POME overstated.
- GAIN's mandate **allocation is a quota, not output** — running ~1 MMT CPO above forecast
  production. Use production for balance sheets.
- Conversion: **1 MT CPO = 1,087 L FAME**, validated against GAIN's own published table.
- "Industrial Dom. Cons." **≠ biofuel** — it also holds oleochemicals. Malaysia's 3.1 MMT industrial
  palm use is *majority oleochemical*. A three-way split is required: biofuel / oleochemical /
  food-feed-waste.

---

## 6. Next sessions (agreed plan — one task per session, `/clear` between)

1. **KG design** — node/edge model for code + database + spreadsheet lineage. Design only, no code.
2. **KG extractors + validate pass** — build off the locked design.
3. **Cleanup** — run the repoint macro, fix `silver.oil_stocks`, blank the biofuel forecast hole.
4. **Forecast layer** — *after* 3. Tiered by series type; freeze vintages in `core.forecasts`;
   50%/90% bands from the start.
5. **Helios validation** — index vs the 2012 drought / 2019 wet commentary archive.
6. **Non-bio everywhere** — needs the KG and the PSD 140/149 ingest first.

---

## 7. Post-mortem — why today produced so many corrections

Most errors were **not** context-length failures. The pattern was seeing a signal and concluding
instead of running the one query that would falsify it:

| Claim | Reality |
|---|---|
| "175 actual months, no gap" | The 175 *contained* a 48-month hole. The number itself was the evidence. |
| "Palm has no seed, crush, or meal" | Palm is a full crush complex with two oils. Inferred from "tree crop". |
| "NASS and ERS stocks are different concepts, 4.7–6.5×" | Same concept. I was missing the crude component. |
| Annual tie-out showed 6 feedstock-years "OFF" | The *check* was wrong, not the rake. |

Two of those happened when context was short, so shorter sessions alone will not fix this. The fix
is the verification rule now in `CLAUDE.md` §Verify before asserting, plus putting checks **in the
code** rather than in anyone's head — the binding tie-out in `rake_feedstock_vintage_aware.py` is
the model to copy.
