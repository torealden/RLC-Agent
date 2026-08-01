# Handoff 2026-08-01 (second session) — usda_comp tabs + PSD collector fixes

## What this session was
Next-session prompt from `2026-08-01_market_dashboard.md`: build usda_comp
tabs across the balance sheet workbooks from `gold.psd_wasde_vintages`,
extending the wasde_comp pattern from the US soybean complex book.

## What shipped

### 1. Two real collection bugs found and fixed (usda_wasde collector)
Both fixes in `src/agents/collectors/us/usda_wasde_collector.py`:

- **Wrong country codes, silently returning nothing.** `PSD_COUNTRY_CODES` /
  `DEFAULT_WASDE_COUNTRIES` used ISO-style codes (CN, EU, RU, UA, AU...).
  PSD's API wants its own FIPS-style codes (CH, E4, RS, UP, AS...). Result:
  every monthly pull from March to August 2026 landed data ONLY for the 7
  countries whose codes coincide (US, BR, AR, CA, IN, ID, MY). China, EU,
  Russia, Ukraine, Australia, Mexico, Japan etc. sat at the 2026-03-15 full
  backfill. **Fix:** codes corrected (verified against /api/psd/countries),
  and `DEFAULT_WASDE_COUNTRIES = ["all"]` — one call per commodity/MY
  returns every country, immune to code drift.
- **Commodity list stopped short.** The monthly default list had 14
  commodities; rapeseed_meal, sunflowerseed(+meal), cottonseed complex,
  peanuts, palm_kernel_oil were only in the March full pull — stale since.
  Also PSD_COMMODITY_CODES had NO entries for peanut_meal, peanut_oil,
  copra, copra_meal, coconut_oil, palm_kernel, palm_kernel_meal (codes
  verified against /api/psd/commodities and added, also to
  `scripts/backfill_fas_psd.py`). Default list now carries the full oilseed
  complex membership (29 commodities).
- **Confirmed NOT in PSD at all:** flaxseed, safflower, corn oil — those
  three US books can never get a usda_comp from this source.

### 2. Backfills run (bronze.fas_psd, report_date 2026-08-01)
- Stale commodities × all countries, MY2024-2026.
- New commodities (copra complex, peanut products, palm kernel) × all
  countries, MY2000-2026 (FINAL history + active vintages).
- Majors × all countries, MY2024-2026 (this is what un-stales China/EU/
  Russia/Ukraine books; the lucky-7 countries got exact-duplicate rows,
  deduped afterwards — see §4).

### 3. `scripts/build_usda_comp_tabs.py` (NEW)
Builds/refreshes a `usda_comp` tab in every eligible balance-sheet workbook.
Per-file-type engine ruling (the open decision from the prior handoff):
- **Generated country books** (.xlsx outside `United States/`): openpyxl —
  same engine copy_legacy_monthly_blocks.py already uses on these files.
- **Hand-maintained US books** (all of `United States/`): Excel COM —
  preserves everything openpyxl would drop; macros force-disabled on open.
  (Checked: the US .xlsm books other than the soybean complex contain no
  VBA project at all — .xlsm extension is precautionary.)
- us_soybean_complex keeps its hand-built wasde_comp + Ctrl+Shift+W VBA,
  untouched.

Design notes:
- Layout mirrors wasde_comp: B/E USDA current vintage (two active MYs),
  C/F Δ-from-prior formulas, D/G RLC links into the member sheet's MY
  columns, I/J prior-vintage values. Formula rows (Total Supply, Other
  Domestic Use, Total Demand, Stocks-to-Use) are formulas in all four
  value columns.
- Member sheets are identified by IN-SHEET TITLE (A2), never tab name —
  seven rapeseed country books still carry soy_* tab names from the
  template clone.
- MY→column resolved from row-3 labels ('2025/26'); rows by scanning
  column-A labels down to the first ALL-CAPS monthly-section banner.
- **Units are never assumed.** Row-label unit text is authoritative;
  a magnitude cross-check vs the newest FINAL PSD values confirms it
  where the sheet has data, and can veto only on strong evidence
  (PSD value ≥ 500). No unit source → loud skip. The Δ-vs-source
  ambiguity is real: a ~10% USDA-vs-sheet source difference is
  indistinguishable from the short-tons factor on small denominators.
- Backs up every book to `models/Oilseeds/Archive/` before writing
  (models/ is gitignored — no git safety net).
- Rerun cadence: after each WASDE (the scheduled collector pull), run
  `python scripts/build_usda_comp_tabs.py`. Idempotent; rewrites the tab.

### 4. Duplicate-vintage cleanup — smaller problem than assumed
Assumption going in: an off-WASDE-day pull just re-snapshots July values →
fake "WASDE_AUG_26" vintage with Δ=0. **Verified wrong for the majors**:
PSD revises continuously between WASDEs — corn US MY2025 ending stocks
moved 54,481 → 51,306 KMT between the 7/10 and 8/1 pulls, and PSD's own
`month` attribute advanced 6 → 7. So Aug-1 vintages carry real intra-month
revisions and their deltas are genuine. Only 184 bronze rows were exact
zero-information duplicates (every field identical incl. PSD month);
those were deleted. Residual caveat: vintage names label the PULL month —
"WASDE_AUG_26" as of 8/1 means "PSD as of Aug 1", not the Aug 12 WASDE;
the scheduled WASDE-day pull supersedes it within the month by design.

## Coverage after final pass (2026-08-01)
- **34 books written** with usda_comp tabs (all country books + US canola,
  coconut (oil only), cottonseed, palm complex, peanut, sunflower).
- 1 locked: argentina_soybean (open in Tore's Excel session — rerun
  `python scripts/build_usda_comp_tabs.py --only argentina_soybean` after
  closing it).
- 5 skipped by design: corn oil / flaxseed / safflower (not in PSD),
  lauric (being rebuilt), soybean complex (existing VBA wasde_comp).
- US copra, copra_meal, palm_kernel_meal have no US rows in PSD — those
  member blocks appear automatically if PSD ever carries them.

## Verified
- Brazil soy comp ties to `gold.psd_wasde_vintages` to the digit
  (WASDE_JUL_26 rank 66 vs JUN_26; production 180,000 / exports 115,000 /
  ending 37,688 KMT; area 48,500 → 48.5 M ha).
- China soy comp (previously starved by the country-code bug) ties exactly:
  WASDE_AUG_26 (imports 113,000 / crush 109,000 / ending 44,369) vs
  WASDE_MAR_26 (112,000 / 108,000 / 44,388); 2026/27 correctly shows no
  prior (MY didn't exist in the March pull).
- US canola comp in million pounds: unit snap production MY2024 sheet
  4,864 vs PSD 2,220 × 2.20462 ✓.
- Single-vintage members (Indonesia palm kernel/cake) correctly render
  "Δ (no prior)" with blank delta columns.
- VBA-survival check on COM-written .xlsm: no vbaProject existed before or
  after (backups compared). COM uses DispatchEx (separate instance), so it
  did not touch Tore's live Excel session (lauric rebuild was open
  throughout).

## Known-broken / needs Tore
1. **us_cottonseed seed sheet data is a mishmash** — Production row holds
   ~12,066 (≈ US cotton production in thousand bales), while the stock
   rows sit at PSD's thousand-tonne magnitudes (365.9 vs PSD 363), all
   under a "(million pounds)" label. The usda_comp seed block was built
   with label units (USDA columns are internally consistent); the RLC
   link columns expose the bad sheet values. Meal/oil members fine.
2. **Argentina peanut book** has scattered values ~10% off PSD in the oil
   sheet (weak short-tons snap ignored, label thousand tonnes used).
   Probably a different source vintage — worth a look.
3. **Mixed country codes in bronze.fas_psd**: stray ISO-coded rows (CN,
   AU, ZA, plus SF/ZA both present for South Africa) from an old
   mis-coded pull sit alongside the PSD-coded history (CH, AS, SF). The
   view partitions by country_code so they form tiny orphan ladders.
   Harmless to the comp tabs (builder uses PSD codes) but worth a
   cleanup migration.
4. **US coconut / lauric / palm-kernel-meal comps** appear automatically
   next builder run if PSD carries US rows for those commodities (copra
   complex data landed this session; US may legitimately have none for
   some members).
5. wasde_comp in the soybean book still shows June/July — Tore refreshes
   it with Ctrl+Shift+W as usual; it reads the same repaired view. Note
   it will show "August vs July" until the Aug 12 WASDE (the Aug vintage
   is the Aug 1 PSD pull, which does carry real intra-month revisions).
6. **argentina_soybean book has NO usda_comp yet** — it was open in
   Excel (lock predates the session) and failed both write passes.
   Rerun `python scripts/build_usda_comp_tabs.py --only argentina_soybean`
   after closing it.

## Not done / deferred
- Feed/Food grains books (corn, wheat) — same builder grammar would work;
  they already have hand-built wasde_comp tabs + VBA. Decide whether to
  migrate them to the generated tab or leave VBA.
- View-level duplicate-vintage collapse (see §4).
- LLM forecast generation into core.forecasts — **next workstream** (was
  already queued behind this one).

## Next-session prompt
> Read docs/handoffs/2026-08-01_usda_comp_tabs.md. Start the LLM forecast
> generation workstream: forecasts into core.forecasts parallel to the
> balance sheets (see memory project_forecast_layer.md and
> project_symbiotic_forecasting.md), so the Projection Comparison page's
> LLM book fills in. The usda_comp tabs and gold.projection_comparison_long
> give the realized/USDA/RLC scaffolding to score against.
