# WASDE historical vintage backfill — source ruling + ingestion spec (v1)

*2026-08-03. Source hunt complete, archive secured locally, ingestion not yet built.*

## Decision: primary source

**USDA OCE "Consolidated Historical WASDE Report Data"** — machine-readable CSV of every
WASDE report *as published at release time*, April 2010 → present. This is exactly the
by-release-month pre-compiled dataset we hoped existed; no PDF scraping needed for 2010+.

- Landing page: https://www.usda.gov/oce/commodity-markets/wasde/historical-wasde-report-data
- File pattern: `https://www.usda.gov/sites/default/files/documents/oce-wasde-report-data-YYYY-MM.csv`
- 2010-04→2020-12 ship as two ZIPs (one consolidated CSV each); 2021-01+ are per-month files.
- Refreshed the day after each WASDE release (forward collection is already covered by the
  mig-166 PSD/WASDE vintage collector; this source is for **backfill only**).

## What is on disk (verified 2026-08-03)

`data/raw/wasde_historical/` — fetched by `scripts/download_wasde_historical.py`:

| Item | Coverage |
|---|---|
| 2 ZIPs (committed to git — see WAF note) | Apr 2010 – Dec 2020 |
| `monthly_csv/` 66 files (gitignored, re-downloadable) | Jan 2021 – Jul 2026 |

**Verification run** (`scratchpad/verify_wasde_archive.py`, rerun any time):
- **193 reports, WASDE #481 (Apr 2010) → #673 (Jul 2026), zero gaps in the report-number
  sequence.** 948,901 rows total, ~4,916 rows/report, 52 distinct report tables.
- Month gaps at **2013-10, 2019-01, 2025-10 are government shutdowns** — the WasdeNumber
  sequence is contiguous across them, so those reports never existed (verified in-data,
  not assumed; cf. `reference_govt_shutdown_data_handling`).
- **Tie-out vs our own collection**: July 2026 CSV US corn vs `gold.psd_wasde_vintages`
  `WASDE_JUL_26` — production MY25/26 432.34 MMT ↔ 432,342 (1000 MT); MY26/27 406.42 ↔
  406,419; ending stocks 51.31 ↔ 51,306 and 45.46 ↔ 45,464. Matches at WASDE-published
  rounding (see precision caveat).

## CSV schema (long format, quoted)

`WasdeNumber, ReportDate, ReportTitle, Attribute, ReliabilityProjection, Commodity,
Region, MarketYear, ProjEstFlag, AnnualQuarterFlag, Value, Unit, ReleaseDate,
ReleaseTime, ForecastYear, ForecastMonth`

- `ProjEstFlag`: `Proj.` / `Est.` / blank (blank = closed-year actual in that release).
- 43 regions: all countries the country books need (Brazil, Argentina, China, EU, Russia,
  Ukraine, Canada, India, …) plus aggregates (World, Total Foreign, Major Exporters…).
- 62 commodity labels, **messy** — case variants (`Beef`/`BEEF`), naming variants
  (`Oilseed, Soybean` vs `Soybean Meal` vs `Meal, Soybean`, `Rice, milled` vs `RICE,
  milled`). Normalization map required at transform time.
- Reliability-appendix rows have empty `MarketYear` and units `Years`/`Percent` — filter
  on `MarketYear <> ''` for balance-sheet ingestion.

## Ingestion plan (next session)

1. **Bronze**: `bronze.wasde_historical` — CSV columns verbatim + `source_file`,
   standard lineage. One-shot loader over the three directories; idempotent on
   (wasde_number, report_title, attribute, commodity, region, market_year, unit).
2. **Transform to vintage rows**: map into the `gold.psd_wasde_vintages` shape with
   `vintage = WASDE_<MON>_<YY>`, `psd_cycle = release month`, following the exact
   conventions mig 166 stamps on live pulls. Rank within the 61–90 PSD/WASDE band by the
   same maturity logic observed in live rows (new-crop Proj. 63, old-crop Est. 67,
   FINAL 90). Since HIGHER rank wins (`reference_vintage_rank_ladder`), FINAL still
   heads every closed MY; backfilled monthlies sit beneath as the vintage history the
   comp tabs read. **Confirm rank assignment with Tore before loading** — it must
   reproduce mig-166 semantics, not invent a parallel one.
3. **Units**: US grain tables are Million Bushels (native WASDE precision); world tables
   are MMT at 2 dp. Store source units in bronze (`feedback_units_source_vs_display`);
   convert at transform. **Precision caveat**: 2-dp MMT ↔ PSD 1000 MT differs by up to
   ±5 thousand MT; bushel-converted values carry the same rounding. Backfilled rows are
   WASDE-published precision, slightly coarser than API-pulled PSD rows. Label the
   source so nobody chases phantom sub-0.01-MMT "revisions".
4. **Scope for v1**: US + world-table countries for corn, soybeans (+ oil, meal), wheat
   (+ classes), sorghum, rice, cotton — the commodities the comp tabs cover. Livestock/
   dairy/sugar tables ride along in bronze; transform later if wanted.

## Pre-2010 (deferred, phase 2)

- **ESMIS archive** (moved from Cornell to https://esmis.nal.usda.gov): every WASDE
  release 1973→present as PDF/TXT — parseable but a scraping project, not a download.
- **AgManager.info (K-State)** compiled US-only WASDE history 1973+ for corn, sorghum,
  wheat, soybeans — spreadsheet form, US S&D only, no world/country rows.
- Recommendation: don't. 2010+ covers 16 years of vintages; the comp-tab use case rarely
  reaches further back. Revisit only if forecast-evaluation work demands deeper history.

## Access gotchas (cost a few loops — recorded so nobody repeats them)

- usda.gov WAF returns 403 to non-browser clients for **HTML pages and .zip files**;
  bare `.csv` GETs pass with a browser User-Agent. The two ZIPs were retrieved from the
  Wayback Machine (`web/20241112034933id_/`) and are **committed to git** (5.2 MB total)
  because USDA blocks re-downloading them.
- Errata reposts rename the file with an **uppercase** `-V2`/`-V3` suffix
  (2026-05 and 2026-06 are V2). Lowercase 404s. The downloader tries suffixes and keeps
  the highest version.
- Wayback CDX queries against `usda.gov*` time out; use `matchType=prefix` on the full
  document path.
