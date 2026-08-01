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

**Post-review update (Tore, same session):** ALL books are (or will be)
hand-maintained / set up like the US sheets. Engine default changed to
**COM for every book** (openpyxl kept as `--engine openpyxl` escape hatch
for headless runs on still-generated shells). Per-book VBA duplication of
WASDECompUpdater was considered and recommended against: the .xlsx books
can't host macros, and 37 macro copies would need maintaining — one
`python scripts/build_usda_comp_tabs.py` run after each WASDE refreshes
everything COM-safely. If in-book Ctrl+Shift+W refresh is wanted for
specific .xlsm books later, the VBA route stays open. Also ruled: no-PSD
books (corn oil, flaxseed, safflower) get a NOTE-ONLY usda_comp tab
(done, verified) rather than nothing; no fabricated #N/A rows in bronze.

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

### 4. Duplicate cleanup + a vintage-labeling bug found via Tore's pushback
184 bronze rows from the Aug-1 pulls were exact zero-information
duplicates of the 7/10 rows (every field identical incl. PSD month) and
were deleted. The interesting part is the rest: corn US MY2025 values
DIFFER between the 6/11, 7/10 and 8/1 pulls, and PSD's own `month`
attribute runs one behind the pull date (Jun-11 pull → month 5, Jul-10 →
month 6, Aug-1 → month 7). First read was "PSD revises between WASDEs";
**Tore corrected this — PSD normally only updates at WASDE** — and the
data fits his story better: the scheduled **noon-ET WASDE-day pull races
the release and captures the PRIOR cycle**. Which means ladder vintages
for the continuously-pulled countries are systematically mislabeled one
month late (WASDE_JUL_26 ≈ June WASDE values). Inference, not yet proven —
verify via PSD `month` attribute semantics across commodities/countries.
**Fix candidates (own session):** label vintages from the PSD month
attribute instead of pull date (migration on the 149 view), and/or move
the scheduled pull from 12:00 to later in the day. Until then, treat comp
vintage names as pull labels, one cycle stale for the majors' history.

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
1. **us_cottonseed seed sheet units** — Tore is on it (thinks bales are
   used for yield-ish lines, million pounds elsewhere; will fix and
   confirm). Rerun `--only cottonseed` after; the unit snap will confirm
   the fix. (Found because Production holds ~12,066 ≈ cotton K-bales
   while stocks sit at tonne magnitudes under a million-pounds label.)
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
> Read docs/handoffs/2026-08-01_usda_comp_tabs.md. FIRST (small): resolve
> the WASDE-day pull race from §4 — verify PSD's `month` attribute
> semantics on bronze.fas_psd, then either relabel vintages from that
> attribute (migration on the 149 view) or move the scheduled pull past
> the release, and rerun build_usda_comp_tabs.py (also picks up the
> argentina_soybean book and Tore's cottonseed unit fix). THEN the main
> workstream: LLM forecast generation into core.forecasts parallel to the
> balance sheets (memory: project_forecast_layer.md,
> project_symbiotic_forecasting.md), so the Projection Comparison page's
> LLM book fills in.
