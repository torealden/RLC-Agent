# Rat-Hole Action Plan

_Companion to `rat_hole_inventory.md`. Inventory shows 31.8 GB across 36,112 files — this is the strategic ranking and execution plan._

## Headline numbers
- **Total**: 31.8 GB, 36,112 files across 22 directories
- **High-value (act now)**: ~3.5 GB, ~1,500 files in 5 directories
- **Long-form documents for KG** (consulting reports, outlook PPTs): ~7 GB, ~280 files
- **Large but low-priority** (recovery noise + bank research): ~17 GB, ~30,000 files
- **Already in current pipeline or trivially redundant**: ~3 GB

---

## Tier 1 — GOLDMINES (act on these in this order)

### 🥇 1. `D:\HigbyBarrett` (1.4 GB, 1,097 files) — RLC analytical DNA archive
- 324 .docx + 301 .xlsx + 177 .pdf
- HB Weekly Reports + Quarterly Insights + Balance Sheets + Cash Prices + Soybean Complex Futures Prices
- Most recent: **July 13, 2023** — covers years of methodology development
- The KG was built FROM this content (batches 001-009 quote HB extensively); this is the **source archive** for everything we've already extracted, plus a lot more

**Action**:
- A1: Build a `scripts/ingest_hb_archive.py` that walks the directory, identifies report types (Weekly / Quarterly / Balance Sheet / Cash Prices) by filename pattern
- A2: For each report type, design a KG extraction batch (HB Weekly batches 022+, HB Quarterly Insights batches, Balance Sheet schema)
- A3: Quote the most recent reports highest-priority — newer methodology supersedes older
- **Estimated KG yield**: 50-100 new nodes, 200+ contexts spread across 10+ batches. Doubles current KG size.

### 🥈 2. `D:\Plant Lists` (43.3 MB, 26 files) — directly extends facility database
- *World Crushing Plants List.xlsx* (May 2021) — likely covers the 78 we have + more
- *Bob's Plant List.xlsx* (May 2021) — Bob = the analyst whose framework underlies the KG
- *RD Comparison List.xlsx* (Dec 2021) + *Renewable Diesel Projects Tracker* (Aug 2020)
- *RD Feedstock Build Up.xlsx* (Apr 2021) — feedstock-by-plant allocation
- *Beef and Pork Slaughter Plants.xlsx* (Jul 2020) — seeds the **rendered-fats facility expansion**
- *Biodiesel Magazine Biodiesel Plant List* — Winter 2019 + Winter 2022 PDFs
- *Chinese Soybean Crushing Plants.pdf* — international expansion seed
- 9 PNG diagrams labeled "Refiners A/B", "Beef", "Ethanol" — visual maps for KG

**Action**:
- B1: Compare *World Crushing Plants List* against `reference.oilseed_crush_facilities` — what plants/columns are missing
- B2: If superior, re-base our reference table from this file (preserve our IA enrichments)
- B3: Build sister tables `reference.biodiesel_facilities`, `reference.rd_facilities`, `reference.beef_pork_slaughter_plants` from the matching files
- B4: Parse Biodiesel Magazine PDFs (annual, structured tables) — each is ~150 plants, structured layout
- **Estimated yield**: 200-400 new facility rows across 4-6 reference tables. Direct phase-2 facility-agent fuel.

### 🥉 3. `D:\Forecast Measurement` (7.0 MB, 224 files) — bootstraps the LLM-vs-human comparison
- *Forecast data and accuracy assessment v02.xlsm* (Oct 2019) — the methodology workbook
- *MI Forecast Evaluation.xlsx* (May 2020) — institutional forecast accuracy framework
- *Forecast Accuracy Instructions.docx* — your written methodology
- 196 daily CSVs (e.g., `20200203_VegOils_SBO.csv`) — actual historical forecasts captured daily
- *One/Two/Three Month Error.JPG* — visualization templates

**Action**:
- C1: Read the .xlsm + Instructions to extract the methodology → writeup as `docs/specs/forecast_accuracy_methodology.md`
- C2: Build `core.forecasts_historical` table; bulk-ingest the 196 daily CSVs as the seed dataset
- C3: Wire into the existing `project_forecast_comparison.md` endpoint — these become the historical baseline that the LLM forecasts get scored against
- **Strategic value**: lets us claim "validated against multi-year historical forecast accuracy framework" instead of starting from zero. Bootstraps the symbiotic forecasting endpoint immediately.

### 🥈 4. `D:\Switch Over\Biomass-Based Diesel\Plant Model Project` (5.7 MB, 7 files) — IFV calibration
- *Biodiesel Plant Profitability.xlsx* (1.9 MB, Nov 2022)
- *Long-Term Plant Profitability Model Inputs.xlsx* (1.0 MB)
- *Long-Term Plant Profitability.xlsx* (1.0 MB)
- *Braya - Profitability Comparison Detail.xlsx* — anchors the Cresta/Braya KG batch (012)
- *Diamond Green Diesel - Naptha-LPG from Corn Oil.pdf* — process spec for DGD

**Action**:
- D1: Read all four xlsx → distill into the per-facility XLSX template (§12 step 5 of the Iowa Crush Agent spec — your "secret sauce")
- D2: Use *Long-Term Plant Profitability Model Inputs* as the input schema for the IFV kg_callable wrapper (instead of inventing one)
- D3: Cross-reference *Braya Profitability* against KG node `come_by_chance_refinery` (already in KG from batch 012)
- **Strategic value**: this is where the per-facility financial model lives. Makes §12.5 a 2-day job instead of a 2-week design exercise.

### 🥈 5. `D:\Biomass-Based Diesel` (23.6 MB, 106 files) — BBD weekly report fuel
- *EMTS DATA.xlsx* (2.1 MB, Dec 2020) + *EMTS Forecast.xlsx* (1.5 MB, Jan 2021) — RIN data + projections
- *LCFS California's Low Carbon Fuel Standard.xlsb* (4.5 MB) + *CA Quarterly LCFS Data.xlsx* (3.1 MB)
- *Biodiesel and RD Forward Curve Forecast.xlsx*
- *RIN Balance Sheet Forecast.xlsx*
- *Mandate Projections.xlsx*
- *biodiesel Renewable Diesel Imports Exports.xlsb*

**Action**:
- E1: Each xlsx becomes a silver view in the BBD-related schema we're building for the weekly report
- E2: *EMTS Forecast* + *RIN Balance Sheet Forecast* feed Section 03 (Credit Stack) and Section 09 (Market Signals) of the weekly
- E3: *LCFS Forecasting Model* could become a kg_callable on its own — historical CARB credit price model
- **Strategic value**: the data layer for sections 03, 04, 05 of the BBD weekly report is mostly here, ready to plumb.

---

## Tier 2 — HIGH-VALUE for KG (long-form consulting/outlook documents)

### 6. `D:\Multi-Client Report` (3.9 GB, 116 files) — 49 PPTX of consulting deliverables
- Each PPTX is 80-180 MB (heavy graphic content) — likely 20-50 slides per deck
- Examples: *Multi Client Report PBF*, *Feedstock and Fuel Outlook P66*, *Bakersfield CA Feedstock Analysis*, *LCFS Q4 2019 Multi Client*, *Future Feedstock Outlook 13 slides*, *LOW CI Feedstock Outlook 64 slides*, *Virgin Vegetable and Crop Oils Outlook 52 slides*, *Canadian RFS Jan 2020*

**Action**:
- F1: Pick the 5 most analytically rich (suggest: PBF, Bakersfield, LCFS Q4 2019, Future Feedstock Outlook, LOW CI Feedstock Outlook)
- F2: Each → KG batch following the HOBO pattern (batches 022a-e or so)
- F3: PBF and Bakersfield connect directly to existing KG nodes `pbf_chalmette_rd` and West Coast feedstock framework
- **Time estimate**: 5 batches × ~2 hours each = 10 hours of focused KG work. Can be parallelized.

### 7. `D:\2020 Renewable Fuel Feedstock Outlook` (2.3 GB, 75 files) — comprehensive RFS outlook
- *Renewable Fuels and Feedstocks Outlook Rev Q3-2020.pdf* (660 MB!) — single largest analytical document in the rat hole
- Monthly Feedstock Outlook Reports Aug-Dec 2020 (~250 MB each)
- *US Oilseed and Fats Projections - 2018-2040.xlsx* — long-term projection
- *Outlook Report Spreadsheets.zip* — backup data
- *Basis Worksheet 2.xlsx* (16.5 MB) — basis modeling

**Action**:
- G1: The 660 MB PDF is structurally the predecessor of HOBO Section 3 (Demand & Supply Fundamentals). Extract sections individually following batch 009 pattern.
- G2: *US Oilseed and Fats Projections 2018-2040* is structurally similar to the Suncor batch (013). Compare and merge if they overlap.
- G3: *Basis Worksheet 2* could feed `silver.facility_basis_proxy` for the Iowa Crush Agent's Open Question #1.

### 8. `D:\Misc` (1.0 GB, 23 files) — client-engagement reports
- *Holly BOD RC.pptx* (110 MB) — Holly Frontier board presentation
- *Vitol Vegetable Oil Reports* (Aug 2020) — Vitol commodity desk
- *Feedstock and Fuel Outlook P66* — Phillips 66
- *States with LCFS.pptx* (128 MB) — state policy comparison
- *Reuters History of Grain Stocks Polls - 0324.xlsx* (Mar 2024) — most recent file

**Action**:
- H1: Holly + Vitol + P66 are client-engagement reports just like HOBO. Each → KG batch using the HOBO pattern.
- H2: *Reuters History of Grain Stocks Polls* is unique — sentiment / consensus data not currently in our KG. Worth its own small batch.

---

## Tier 3 — MEDIUM-VALUE (use selectively)

### 9. `D:\Tableau Library` (1.4 GB, 904 files) — Jacobsen weekly report ancestor
- 370 .twb + 69 .twbx (Tableau workbook files) + 353 .xlsx (source data)
- *MI Report Commentary.zip* (39 MB) — likely all the prose commentary that fed the Tableau dashboards
- *Jacobsen Biofuels Master 4-13-2020.pdf* (37 MB) — comprehensive snapshot
- *LCFS Credit Breakout By Feedstock v4.xlsx* — direct input for our Section 03

**Action**:
- I1: Don't open the Tableau .twb/.twbx files (we're not using Tableau). Use them as VISUAL REFERENCE for what the BBD weekly should look like — open in Tableau Reader if needed.
- I2: Extract the 353 .xlsx data files; the LCFS / FSA acres / GMM Forecasts ones are immediately useful.
- I3: *MI Report Commentary.zip* — unzip and ingest the prose to KG as historical analyst commentary. Could be 500+ documents.

### 10. `D:\Soybean Spreadsheets` (1.1 GB, 3,814 files) — mixed bag, some gold
- 276 .py files = legacy collector scripts; review for ideas, don't import wholesale
- 353 .xlsx = the soy data archive
- *TradeUpdater.xlsm* (Mar 2024, 47 KB) — likely the precursor to the current `TradeUpdaterSQL.bas`
- *Oilseed PSD Data.xlsx* (37 MB) — historical PSD snapshot
- *GTT_sourcer.ipynb* (116 MB!) — Jupyter notebook, possibly the global trade tracker

**Action**:
- J1: Read the .py files for archaeology — don't import, but flag interesting collectors not currently replicated
- J2: Sample-read 5-10 of the bigger .xlsx files (`usoilseedbal`, `wldcrush`, `wldweather`) to see if they fill any historical gaps in current bronze
- J3: *TradeUpdater.xlsm* — read it; if it's the precursor of current TradeUpdaterSQL.bas, archive only

### 11. `D:\Feed Grain Spreadsheets` + `D:\Old Balance Sheets` (~50 MB total) — historical balance sheets
- *usethanolbal.xlsx* (16.4 MB), *fgusbal.xlsx*, *sbwldbal - old.xlsx*, *whusbal - old.xlsx*
- These look like older versions of current bronze data

**Action**:
- K1: Spot-check whether any contains historical data not in current bronze (pre-2014 NASS, etc.)
- K2: If so, ingest the deltas; if not, archive

### 12. Seagate Recovery — `Document\xlsx`, `xlsm`, `csv` (combined 5.6 GB) — 2014-2019 model archive
- The xlsx ones include `wldsuntrade.xlsx` (624 MB!), `wldlautrade.xlsx` (143 MB) — world trade by commodity
- The xlsm ones include `cotcharts2.xlsm` (82 MB), `Wheat_Data_Weekly_Export_Sales.xlsm` (58 MB), `JCI Price File.xlsm` — Jacobsen/Informa style models
- The csv ones include 3 copies of `Oilseeds.csv` (54 MB each — likely PSD historical)
- Many "lost file name" copies — recovery noise

**Action**:
- L1: Filter to files with REAL names (not "lost file name (NNN)")
- L2: From the named files, prioritize the price files (`JCI Price File`, `Oil-Price-History`) and trade files (`wldsuntrade`, `wldlautrade`)
- L3: Dedupe (`Oilseeds.csv` × 3 identical files)
- L4: Ingest historical prices and trade flows that aren't in current bronze

---

## Tier 4 — LOW-VALUE / SKIP

### 13. `D:\Investment Research` (2.2 GB, 2,915 files) — bank research
- 2,874 PDFs from GS, JPM, MS, HSBC, Citi etc.
- Mostly macro/equity research, not commodity-focused
- Some commodity reports embedded (e.g., GS Energy Daily) but signal-to-noise is low

**Action**:
- M1: **DO NOT systematically ingest.** Use as visual style reference only.
- M2: Sample 10 commodity-specific PDFs for chart/visual ideas for the BBD weekly

### 14. `D:\Seagate Recovery\Document\doc` + `\docx` (3.7 GB, 12,848 files) — old Word documents
- Many recoverable Word reports from 2005-2018
- Big chunk are "lost file name" recovered files
- Real names: UBS Oil Weekly archive 2005-2006 (multiple 7-8 MB .doc files)

**Action**:
- N1: Filter to non-"lost file name" files, sort by date, read the recent ones
- N2: UBS Oil Weekly 2005-2006 → seed batch for "historical RD outlook 2005" if we want a long-term backtest

### 15. `D:\Seagate Recovery\Document\pdf` (2.3 GB, 2,072 files) — recovered PDFs
- 4 files at 512 MB are likely concatenated archives, not single PDFs — would need binary inspection
- Real PDFs include `USDA Major World Crop Areas Book.pdf` (36 MB) — useful but already mostly in our crop_maps domain knowledge

**Action**:
- O1: Cherry-pick the named PDFs only. Skip "lost file name" PDFs.

### 16. `D:\Seagate Recovery\Document\ppt`, `pptm`, `pptx`, `ppsm` (3.4 GB) — old presentations
- 2003-2018 era. Some named files have value (Tom Scott 2014, NAWG 2014, KCG Oilseed Complex training, Cypriano ABRA Brazilian Rendering 2018, Swisher NRA Global Update)

**Action**:
- P1: Extract the 6-8 named presentations with relevant titles, ingest to KG as a single "historical analyst presentations" batch

### 17. `D:\Seagate Recovery\Document\txt` (1.3 GB, 5,973 files) — text files
- *posded.txt* (14 MB), *STATES.TXT* (7 MB) — likely DTN-style cash price reports?
- *mbox.txt* — looks like email archive
- Most files small and numbered

**Action**:
- Q1: Skip unless you confirm one specific file is needed

### 18. `D:\Macro Econ Data`, `D:\Jake Dropbox Files`, `D:\Old USB Files` — minor

**Action**: archive, no work

---

## Recommended execution sequence

Given the scope, I'd batch the work as follows. Each "phase" is roughly a focused 1-3 day chunk.

### Phase 1 — IMMEDIATE WINS (1-2 days)
**Goal**: extract the highest leverage data with smallest effort.
1. **Plant Lists ingestion** (Tier 1 #2) — 4-6 new reference tables, 200-400 facility rows. ~half day.
2. **Forecast Measurement → forecasts_historical** (Tier 1 #3) — bootstrap the symbiotic forecasting endpoint. ~half day.
3. **BBD folder → silver views** (Tier 1 #5) — feeds the BBD weekly report immediately. ~half day.

### Phase 2 — IFV/SECRET SAUCE (2-3 days)
4. **Plant Model Project files** (Tier 1 #4) — extract per-facility XLSX template logic into the IFV kg_callable spec + a per-facility XLSX clone procedure. Closes spec §12 step 5.

### Phase 3 — KG EXPANSION (3-5 days, can parallelize)
5. **HB Archive** (Tier 1 #1) — large, multi-batch. Build a script that walks the directory and routes by report type. 5-10 KG batches.
6. **Multi-Client Reports** (Tier 2 #6) — pick top 5 PPTs, ingest each as a batch.
7. **2020 Outlook** (Tier 2 #7) — extract the 660 MB master report in sections.
8. **Misc client engagements** (Tier 2 #8) — Holly + Vitol + P66 + States with LCFS.

### Phase 4 — TABLEAU + HISTORICAL (1-2 days)
9. **Tableau Library .xlsx files** (Tier 3 #9) — extract the 353 source xlsx, ingest selected ones.
10. **Historical balance sheets** (Tier 3 #11) — fill any gaps in current bronze.

### Phase 5 — TRIAGE THE NOISE (variable)
11. **Seagate Recovery named files** (Tier 4 #14-16) — cherry-pick the named files with real titles.
12. **Investment Research** — sample for visual style only.

---

## Decisions I need from you

1. **Bob's Plant List vs current workbook** — I'll compare the columns and rows. If Bob's list is superior, do you want to **re-base** `reference.oilseed_crush_facilities` from it (preserving the IA enrichments and KG node_keys we've established)?

2. **HB Archive ingestion scope** — there are ~325 docx + 300 xlsx HB files. Do you want me to ingest ALL of them eventually, or pick the most-recent N (e.g., last 50 weeks) plus all Quarterly Insights?

3. **Per-facility XLSX template** — once I read the *Long-Term Plant Profitability Model Inputs.xlsx*, I'll either: (a) adapt it as the per-facility template for the Iowa Crush Agent system, or (b) tell you it's not the right structure and propose an alternative. Want me to make that call autonomously, or report back first?

4. **Investment Research / Seagate noise** — confirm we skip systematic ingestion of these and only cherry-pick named files when needed?

5. **Where to start now** — Phase 1 has three half-day tasks. My instinct: start with **Plant Lists** since it directly extends work we just did. Sound right, or do you want a different starting point?
