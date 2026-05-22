# data/raw — ingest plan

*Generated 2026-05-22. Companion to `data_raw_ingest_inventory.md` (file-level listing).*

**Scope:** 685 files / 1,400 MB across 37 entities. Plan groups them into priority tiers and identifies what becomes new bronze tables, what extends existing tables, and what's analytical-snapshot-only (no ingest needed).

---

## Tier 1 — Net-new tables, high analytical value (~6 entities)

These unlock genuinely new analyses we don't have today.

### 1.1 FAO Population (OA)
- **File:** `oilseeds_fats_greases/Population_E_All_Data.zip` (1.4 MB)
- **Bronze target:** `bronze.fao_population` (country × year × element × value)
- **Effort:** 1 hour. Single CSV, ~245 countries × 75 years × 5 elements ≈ 90k rows.
- **Unlocks:** Per-capita normalizers for any country, total/rural/urban splits.

### 1.2 FAO Production Crops + Livestock (QCL)
- **File:** `oilseeds_fats_greases/Production_Crops_Livestock_E_All_Data.zip` (24 MB)
- **Bronze target:** `bronze.fao_qcl` (area × item × element × year × value)
- **Effort:** 2 hours. ~245 countries × 313 items × 20 elements × 60 years ≈ 9M rows, but heavily sparse — likely 2-3M actual rows.
- **Unlocks:** Livestock counts for **gCAU/pCAU multiplier inputs** (the Table 30 / "High protein" framework you noted). Filter to species items (Cattle 866, Pigs, Chickens 1057, etc.) × Element 5111/5112/5114 (Stocks) = inputs to country-level animal-unit aggregation.

### 1.3 FAO Food Balance Sheets (FBS)
- **File:** Not on disk yet — quarterly-pull reminder set for Jun 15.
- **Bronze target:** `bronze.fao_fbs`
- **Effort:** 2 hours after download.
- **Unlocks:** Per-country S&D equation (production + imports − exports = feed / food / seed / waste / processing). The canonical FAO "domestic disappearance" framework.

### 1.4 FAO Commodity Balances Non-Food (CB)
- **File:** `oilseeds_fats_greases/CommodityBalances_(non-food)_(2010-)_E_All_Data.zip` (339 KB)
- **Bronze target:** `bronze.fao_commodity_balances_nonfood`
- **Effort:** 30 min. Tiny file.
- **Unlocks:** Industrial use of crops where it's broken out separately from FBS.

### 1.5 USDA ERS Feed Grains Yearbook
- **File:** `Feed Grains Yearbook Tables - Jul 24.xlsx` + Outlook xlsx monthly files
- **Bronze target:** `bronze.ers_feed_grains_yearbook` (table 1-35 normalized)
- **Effort:** 4 hours. Multi-sheet xlsx, each table has its own shape. Worth doing carefully because Table 30 (gCAU/pCAU) is load-bearing.
- **Unlocks:** US-side gCAU/pCAU authoritative, cross-check vs FAO-derived for everyone else.

### 1.6 USDA ERS Oil Crops Yearbook (24-year history)
- **Files:** `oilseeds_fats_greases/Oil Crops Yearbooks/YearbookAllTables_{2002..2025}.zip` (24 zips)
- **Bronze target:** `bronze.ers_oil_crops_yearbook`
- **Effort:** 4 hours. Need to handle schema drift across 24 years of yearbook formats.
- **Unlocks:** 24 years of authoritative US oilseed S&D + crush from ERS, complementing the FAS PSD path. Pre-1999 oil crops S&D not currently in our system.

---

## Tier 2 — Extend existing tables, fills historical gaps (~5 entities)

### 2.1 FGIS Inspections — pre-2014 backfill
- **Files:** `cross_commodity/CY1990.csv` through `CY2025.csv` (39 files, 294 MB)
- **Bronze target:** Extends existing `bronze.fgis_inspections` (currently 534K rows back to 1990 per CLAUDE.md — verify if pre-2014 is in there or if these CY files extend it)
- **Effort:** 1-2 hours validate + bulk-load anything missing.

### 2.2 ERS Wheat / Feed Grains numbered table CSVs
- **Files:** `food_grains/01_Wheat_*.csv` through `25_*.csv`, `feed_grains/*.csv`
- **Bronze target:** Same as 1.5/1.6 (the numbered breakouts ARE the yearbook tables exported individually). Likely redundant with the master xlsx ingest.
- **Effort:** Use these only if the master xlsx ingest hits format issues; cleaner data shape.

### 2.3 NASS Cattle on Feed Database (COFD)
- **Files:** `livestock/cofd_*.csv`, `cofd1225.pdf` (cattle on feed monthly + historical tables)
- **Bronze target:** `bronze.nass_cattle_on_feed` (NEW table)
- **Effort:** 2 hours. Feeds into gCAU calculation directly (cattle on feed is the largest single category).
- **Unlocks:** US cattle-on-feed history, which currently isn't in any bronze table.

### 2.4 Canada AAFC Crop Production
- **Files:** `Canada - Crop Production - Aug 25.xlsx`
- **Bronze target:** Extends `bronze.canada_statscan` or new `bronze.canada_aafc`
- **Effort:** 1 hour.

### 2.5 CONAB monthly safra xlsx archive
- **Files:** `CONAB - {Crop} - {Month YY}.xlsx` (already have 10+ months)
- **Bronze target:** Extends existing `bronze.conab_production`
- **Effort:** 2 hours. Tricky because CONAB xlsx schema varies by report and our existing collector hit 404 on automated path; bulk-load these manual pulls.

---

## Tier 3 — Reference snapshots, parse-only (no recurring ingest)

These are point-in-time analyst inputs, not series we need refreshed.

| Entity | Files | Action |
|---|---|---|
| EIA BBD Capacity / Feedstock Use (snapshots) | `BBD Feedstock Use - EIA - Aug 25.xlsx`, `BBD US Capacity - EIA - Aug 25.xlsx`, `Biodiesel Capacity - EIA - Aug 25.xlsx`, `Capacity - EIA - Aug 25.xlsx` | Parse to `reference.eia_bbd_capacity_snapshots`, dated rows |
| WASDE PDFs (37 files, 33 MB) | Monthly WASDE PDF archive 2024-2026 | Index for LLM retrieval; don't re-parse (WASDE collector handles current) |
| Crop Production / Acreage / Crop Progress PDFs | 30+ monthly NASS PDFs | Same — index for retrieval, NASS collector handles current data |
| Bloomberg pre-report surveys | `Bloomberg* Acreage`, `Bloomber Survey` | Note as analyst-curated input, no recurring ingest |
| Attache reports | `Attache Reports/` (subfolder) | Tier-2 candidate for KG extraction; not bronze-table-shaped |
| MMN reports, MN/MO permits, Drought monitor PDFs | Various | Index only |
| 2012-13 / 2020-21 corn summary markdown | Historical analyst writeups | Already in repo, don't re-ingest |

---

## Tier 4 — Big-file ingests deferred

### 4.1 FAO Trade Detailed Matrix (TM) — 409 MB
- **Per your note (2026-05-22): FAO TM is the cross-check, not the canonical bilateral trade source.** Primary will be each country's own Census-equivalent (Census US, StatCan, ComexStat, INDEC, IBGE — collectors exist for several).
- **Bronze target if/when ingested:** `bronze.fao_trade_matrix`
- **Recommendation:** **Defer.** Ingest when we want explicit reconciliation of "what Brazil says they exported to China" vs "what FAO TM reports for that flow." Until then, the file just sits on disk.

### 4.2 FAO Trade Crops + Livestock (TCL) — 89 MB
- Same logic as TM — backup/cross-check rather than canonical.
- **Defer** to the same future reconciliation pass.

### 4.3 MN State Air Permits bulk (94 MB across Title V archive)
- Already in flight via Ollama PDF extraction pipeline (per `project_state_air_permits_llm.md`)
- The `my_neighborhood_sites.csv` (75.5 MB) is the MN MPCA bulk export — should ingest as `bronze.mn_facility_sites` so the air-permit extraction can join against it.

---

## Effort summary

| Tier | Entities | Net new bronze tables | Estimated effort | Unlocks |
|------|---------:|----------------------:|-----------------:|---------|
| Tier 1 | 6 | 6 new tables | ~14 hours | gCAU/pCAU, per-country S&D, 24yr oil crops |
| Tier 2 | 5 | 1 new (COFD) + 4 extends | ~8 hours | Pre-2014 trade, cattle on feed, Canada |
| Tier 3 | ~10 | 1 reference table | ~2 hours | EIA BBD capacity snapshots |
| Tier 4 | 2 (deferred) + 1 (in-flight) | 1 in-flight | ~6 hours when triggered | Trade cross-validation |
| **Total active** | **21 entities** | **8 new tables** | **~24 hours** | |

---

## Recommended execution order (post-Helios meeting)

1. **FAO Population (OA)** — 1 hr, trivial, unblocks per-capita math
2. **FAO QCL livestock filter** — focus the QCL ingest on livestock stocks first (the gCAU/pCAU inputs), defer full crop production rows to a second pass
3. **USDA ERS Feed Grains Yearbook Table 30 (gCAU/pCAU)** — 2 hr; this + (1) + (2) = the aggregation-sheet substrate Tore described
4. **ERS Oil Crops Yearbook 24-year backfill** — 4 hr; biggest single historical unlock
5. **NASS Cattle on Feed (COFD)** — 2 hr; feeds gCAU directly
6. **FAO Food Balance Sheets** — wait for Jun 15 quarterly pull, then ingest
7. **Canada AAFC + CONAB xlsx bulk-load** — fold into existing tables
8. **EIA BBD capacity snapshots** — reference table for the BBD models

The first three (FAO Population + QCL + ERS Feed Grains Yearbook) together = ~7 hours of work that lights up the aggregation sheets you described as the next analytical artifact.

---

## Conventions to lock in before starting

- **All FAO bronze tables**: long format (area_code, area, item_code, item, element_code, element, year, value, unit, flag, year_code).
- **All bronze.fao_* tables** include `source_zip` text column = the filename of the ZIP it came from, so we can detect when a fresher snapshot supersedes.
- **Refresh strategy**: when a new ZIP is dropped in data/raw/, the ingest script truncates and reloads (FAO ZIPs are always full-history dumps, no incremental). Old `source_zip` value tells us last refresh date.
- **Brazil rows**: ingested as-is (no MY adjustment). Per `reference_brazil_my_alignment.md`, analytical layer handles US-vs-Brazil calendar alignment.

---

*Inventory regenerable via `python scripts/_inventory_data_raw.py`. Re-run after each new download batch to track coverage.*
