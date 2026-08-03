# Handoff 2026-08-03 — Curve engine (prompt A) + CLAUDE.md DB-inventory hard lock (prompt B)

Both queued prompts from `2026-08-02_wasde_pull_race_fix.md` addendum, executed in order per Tore.
Commits: `e69e9388` (curve engine), `74b9d1a9` (CLAUDE.md lock), both pushed to main.

## A. Derived-curve engine — gold.curve_term is no longer empty

### Pre-build verification (correcting the record)
- The 07-28 handoff contradicted itself on the history backfill ("DONE" and "NOT done"). **Verified
  in DB: DONE** — mig 159 landed 65k NEARBY + dated CONTRACT rows at NEWS_INDICATIVE. The MEMORY.md
  index line "backfill ruling PENDING" was stale (fixed this session); the memory file body was
  already correct.
- Much had landed since the 07-28 handoff that the queued prompt didn't know: FRED FX (key exists
  now), `gold.price_mark_best` (mig 160, THE consumer path), `reference.price_series` catalog +
  unit-guard trigger (mig 161), PLACEHOLDER rank (mig 162), RSO/BDO loader (mig 164). The engine
  was built against that state, not the prompt's.
- Found + fixed a standing hole: mig 159 was one-shot, so delayed board marks in `price_mark` froze
  at 2026-07-27 while bronze kept collecting daily.

### What shipped
1. **`futures_price_mark_bridge`** (`src/agents/collectors/market/futures_price_mark_bridge.py`) —
   daily bronze→price_mark/curve_snapshot sync, identical mapping to mig 159, 14-day trailing
   window with DO UPDATE (revision heal). Registered daily 17:45 ET (after yfinance 17:15).
   First run: 1,516 rows, price_mark current to 07-31.
2. **`src/curves/`** (`specs.py` + `engine.py`) — parity-chain engine:
   - Board legs from `gold.price_mark_best` (official displaces delayed automatically), **dated
     contracts only** (brief §D.2 aggregator prohibition — never NEARBY/FRONT), zero-volume tenors
     refused (OI is NULL in the delayed source → volume is the §D.1 proxy).
   - Writes term rows + `DERIVED_*` headline (`series_key == curve_key`, tenor = delivery window
     'YYYY-MM', tenor_type WINDOW) in ONE transaction; the mig-158 deferred tie-out validates at
     COMMIT. Trigger untouched.
   - First curve: **`BRSBO_FOB_PARITY`** (register #19/#27) = board (ZL cents/lb × 22.046226 →
     USD/t) + fob_spread + basis_residual. 10 obs_dates × 6 windows × 3 terms = 180 term rows live.
     Cataloged in `reference.price_series` (unit guard active).
   - Registered `curve_builder` daily 18:15 ET; dispatcher restarted, resolution verified.
3. **Tie-out proven adversarially**: in-txn +5 USD/t on one board term → `CheckViolation` at COMMIT
   with the exact sum/headline/epsilon; rollback leaves the stack tying. Unit tests (6) cover
   contract-code→window mapping and stack arithmetic: `tests/test_curve_engine.py`.

### Review points for Tore (methods, per the brief)
1. **fob_spread = 0 placeholder.** No in-house BR FOB series exists to calibrate (checked
   bronze.feedstock_prices — no Brazil/Paranaguá region). Intended calibrator = CEPEA collector
   (register #19, unbuilt). Rule the initial spread or accept 0-until-CEPEA.
2. **basis_residual = 0 until CEPEA lands** — then the residual series becomes the RLC-built BR
   basis (register #27).
3. **can_republish = FALSE** on the headline while the board leg is delayed yfinance. When the CME
   ZL official collector (register #5) lands, the engine picks up the better mark automatically but
   the flag flip is a decision, not automatic.
4. **No band is stored** — migs 157/158 have no band columns, so the brief's "no bandless curve
   leaves gold" rule is currently unsatisfiable in storage. Needs a storage + method ruling
   (band columns on price_mark vs a separate table; and what a defensible band even is while the
   spread terms are placeholders).
5. FX term: none in this chain (USD/t from a USD parent). FX enters with the first local-currency
   chain (DCE P ← FCPO), which is blocked anyway — FCPO feed is stale (ibkr, dead since 03-09).

## B. CLAUDE.md DB-inventory hard lock

- **`scripts/generate_claude_md_db_inventory.py`** rewrites a marked region ("Current Database
  Stats") from information_schema: per-schema table/view counts + distinct collector count.
- **`scripts/check_claude_md_db_drift.py`** exits 1 if ANY schema-qualified object named anywhere
  in CLAUDE.md doesn't exist in the DB (tables/views/matviews/functions), or the generated region
  is stale. Exit 2 = DB unreachable (reported, never treated as drift, soft-passes the hook).
- **Wired three ways**: daily dispatcher job `claude_md_drift_check` 07:30 ET → `system_alert` CNS
  event on drift (surfaces in get_briefing); pre-commit hook (installed `.git/hooks/pre-commit`,
  versioned `scripts/hooks/pre-commit` — re-install after clone) blocking commits that touch
  CLAUDE.md; generator as the repair tool.
- **Drift fixed was 22 phantom objects, not 3.** Beyond the known CFTC positioning views:
  bronze.usda_nass, eia_ethanol/petroleum, epa_rfs_rin_generation/transaction, weather_observations,
  weather_emails, usda_ams_cash_prices, usda_ers_data, cme_settlements, ndvi_data, wheat_tenders;
  silver.crop_progress/crop_condition/ethanol_weekly; gold.corn/soybean/wheat_condition_latest,
  ethanol_production_summary. Each replaced with its verified real counterpart or removed.
  Stale counts were materially wrong: actual bronze 107 (doc said 89), gold 193 views (180),
  59 collectors (41).
- **Proven**: fake `gold.totally_fake_view_xyz` → checker exit 1, pre-commit blocked the commit,
  CNS event id 1743 written (demo event deleted after). Clean pass exit 0; the real commit
  `74b9d1a9` ran through the hook live.
- Windows gotcha fixed: hook pipes staged content via stdin; cp1252 default mangled the em-dash in
  the region markers → read `sys.stdin.buffer` and decode UTF-8 explicitly.

## Known-broken / unverified
1. **First scheduled runs unverified** — bridge (17:45), curve_builder (18:15), drift check
   (tomorrow 07:30) have only run manually. Check `core.collection_status` tomorrow.
2. **Drift check's first scheduled run may report a stale region** if any DDL/collector lands
   between now and then — that's design working, not a bug; run the generator.
3. **Sysgraph declarations** still missing for ams_grain_settlement (prior gap) AND the three new
   jobs — the brief's DoD item remains open estate-wide.
4. Curve sanity chart for Desktop (brief DoD) not rendered.
5. The engine derives only dates where the parent has dated CONTRACT marks (~2024+). No deep
   history for the derived curve until/unless Tore wants NEARBY-based history (would violate §D.2
   — recommend against).
6. FCPO (ibkr) dead since 2026-03-09; RSO single-day; both block the palm/rapeseed parity chains.

## Next-session prompt
> Read docs/handoffs/2026-08-03_curve_engine_claude_md_lock.md. Verify the cheapest items first:
> did the three scheduled jobs (futures_price_mark_bridge 17:45, curve_builder 18:15,
> claude_md_drift_check 07:30) run clean in core.collection_status? Then pick ONE:
> (a) historical WASDE vintage backfill source hunt (Cornell Mann / OCE — check for a pre-compiled
> by-release-month dataset first), (b) USDACompUpdater.bas in-book VBA, (c) LLM forecast generation
> into core.forecasts (memory: project_forecast_layer.md, project_symbiotic_forecasting.md),
> (d) client-MCP scoping spec, or (e) second derived curve + Tore's fob_spread/band rulings from
> this handoff's review points.
