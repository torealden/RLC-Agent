# Handoff — 2026-07-29/30 session

Two workstreams this session: (A) **Helios price-feed layer** (early), (B) **legacy balance-sheet monthly copy** (main, current). Full detail in memory: `project_helios_price_layer`, `project_legacy_monthly_copy`, `project_model_build_tracker`.

## A. Helios price-feed layer — SHIPPED (branch `helios-price-layer-foundation`, pushed)
- **#13 DCO** (`ams_dco_collector`, MARS API, 8 regions), **#11 FX FRED** (`fred_fx_collector`, key in .env), **#12 WTI/Brent** (earlier), **RSO** (Dutch Mill rapeseed via compliant Barchart CSV loader).
- Storage: migs 156-164 (`price_mark`/`curve_snapshot`/`curve_term` + tie-out, `price_mark_best` best-rank view, `price_series` catalog + unit guard, `PLACEHOLDER` rank, `price_source` provenance).
- **§B CME inventory** delivered (`clients/Contracts/Helios/CME_settled_veg_oil_futures_inventory.md`) — RSO/UCO/UCOME/South-Asia Fastmarkets suite; provenance = `ASSESSED_PENDING_LICENSE`, can_republish FALSE.
- Route reality: CME ToS-blocks scraping, Barchart `/proxies/` robots-disallowed → compliant path = human CSV export → loader. Barchart subscription does NOT grant republication.
- **NEXT (Helios):** UCO/UCOME CSVs from Tore → 2-line ROOT_MAP add; scraping fleet #5-10/#14; curve module `src/curves/`. Price-feed build tracker: `clients/Contracts/Helios/PRICE_FEED_BUILD_TRACKER.md`.

## B. Legacy balance-sheet monthly copy — MOSTLY DONE
Copy monthly blocks from `C:/Users/torem/RLC Dropbox/Tore Alden/Soybean Spreadsheets - Copy/wld<crop>bal.xlsx` → new per-country 3-tab workbooks, re-bucketed to US MY. Engine: `scripts/copy_legacy_monthly_blocks.py` (general, verified). Prompt: `docs/specs/legacy_monthly_copy_prompt_v1.md`.

**DONE (all 0 genuine mismatches, 816 formulas preserved, 0 dirty):**
- **Rapeseed/canola — 9:** Australia, Canada(CANOLA token, Aug-Jul), China, EU, India, Japan, Mexico, Russia, Ukraine.
- **Soybean — 9:** Paraguay, Uruguay, EU, Canada, Ukraine, Russia, India, Japan, Mexico. (Argentina/Brazil/China done by Tore; US special — skipped.)
- **Coconut/copra:** Indonesia (new); Philippines = Tore's filled template.
- **Peanut:** Argentina/China — done by Tore (Mar-source pasted positionally to Sep/Oct convention; do NOT re-bucket).
- Europe dir → Archive (EU canonical). 10 annual `*_complex_balance_sheet.xlsx` archived.

**⚠️ KNOWN / UNVERIFIED / NEXT:**
- **CRUSH-BLOCK BUG — FOUND & FIXED (Tore caught it via India rapeseed crush).** `read_block` read ALL year-labeled columns; Tore keeps analysis columns (trailing avg, stocks-to-use) to the RIGHT of some blocks with their own year labels after a gap, so that second table silently OVERWROTE the real series for overlapping years → plausible-looking garbage (India crush showed 3900/0.2). Values were never fabricated (all real source cells) — pulled from the wrong sub-table. **My earlier "336/336 faithful" was a self-consistent verification failure** (checked output vs the same buggy reader — the exact verify-before-asserting trap). **FIX:** `read_block` now stops at the first year-row gap. **Blast radius** (structural signature scan across all copied blocks): ONLY **India rapeseed + Canada canola crush** had it → both re-run clean (India crush max 6634→550). All other crush blocks + imports/exports/prod/stocks/meal/oil verified unaffected; big crush maxes (China 10190, Brazil, Argentina, EU) are legitimate/Tore's own files. **Push-ahead approved.** LESSON: verify copies against RAW source cells + a mass-balance sanity (crush ≤ production), never against the same reader.
- **PALM — ready, just run next session.** `malaysia_palm_complex_bal_sheets.xlsx` (Tore's, 4 tabs: palm_oil/palm_kernel/pk_meal/pk_oil) is CORRECT: oils/meal Oct-Sep, kernel Sep-Aug. My earlier "May-first / bad layout" was a BUG in throwaway inspection scripts (month list lacked abbreviations) — the real engine handles it. Add `palm_blocks()` (source `PALM OIL`/`PALM KERNEL`/`PALM KERNEL CAKE`→pk_meal/`PALM KERNEL OIL` — note source typo "PRODUCITON"; base_country="Malaysia", template=Malaysia file) and run **Indonesia** (Malaysia = filled template). Source `wldlaubal` "Indonesia Palm Complex", Oct-first.
- **Tracker:** `models/_build_tracker/` — step-1 (Template) checked for built combos in Master Matrix (now 130), group tabs, per-commodity workbooks. Added 7 built combos that were outside Part C scope (rapeseed India/Mexico; soybean Canada/Japan/Mexico/Russia/Ukraine). **May find more oilseed country/commodity combos** — tracker is the starter kit, extend as needed. NB: re-running `build_model_build_tracker.py` WIPES manual checks — edit directly or add a pre-check-built feature.
- Model workbooks + tracker xlsx are WORKING FILES (not git-committed); scripts + migrations + specs are committed/pushed.

**GOTCHAS:** wldsoybal has a chartsheet openpyxl chokes on → monkeypatch `openpyxl.chartsheet.custom.CustomChartsheetView.scale=Integer(allow_none=True)`. Git Bash `/c/` paths don't resolve in Windows Python — use `C:/...`. Tabs (soy_balance_sheet etc.) MUST NOT be renamed programmatically — 816 formulas depend on them (Tore renames on open).
