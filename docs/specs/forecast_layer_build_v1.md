# Forecast Layer — Build v1 (ledger 6b)

**Status:** BUILT + verified, 2026-07-24. Implements `forecast_layer_design_v1.md` (D1–D8).
**Owners:** Claude Code (storage, writers, guards) · Claude Desktop (balance-sheet formulas).
**Depends on:** `flat_file_contract.md` (now v1.1), `reference_vintage_rank_ladder.md`, migration 149.

This is the BUILD half of session 6. The DESIGN (6a) is `forecast_layer_design_v1.md`; read it first
for the *why*. This doc is *what shipped*, the *verification evidence*, and the *known-gap / not-verified*
list the next session opens against.

---

## 0. The gate that had to pass first (design §9, the cheapest way to be wrong)

**D4 append test — PASS.** Could appending `value_low`/`value_high` as trailing columns to a flat file
shift any value a balance-sheet formula computes? If yes, all of D4 needed rethinking.

Ran the **real §4 contract formula** — `SUMIFS(value, keys…, vintage_rank = MAXIFS(rank, keys…))`,
whole-column refs — against the live 1,218-row wheat flat file (`us_wheat_production.xlsx`), on four
keys each carrying **5 distinct vintage ranks** (genuine MAXIFS discrimination). Recalculated in Excel,
appended `value_low`/`value_high` as columns N/O with band values filled on **every** data row,
recalculated again:

| key | expected (max-rank value) | before | after |
|---|---|---|---|
| SRW/production/2025 | 352,916,000 | 352,916,000 | 352,916,000 |
| HRW/production/2025 | 804,443,000 | 804,443,000 | 804,443,000 |
| SRW/production/2023 | 449,017,000 | 449,017,000 | 449,017,000 |
| HRW/production/2022 | 530,966,000 | 530,966,000 | 530,966,000 |

**Zero shift.** Backed by three structural facts, not just the one run: the flat file has **no ListObject
tables** (no structured-ref auto-expansion), **no defined-name ranges** spanning the used area, and the
binding is by **explicit column letter** (A–I) while the append lands at N/O. Trailing-column bands are a
non-breaking seam change. `flat_file_contract` bumped to **v1.1**.

(Corrected a stale premise while here: `us_wheat_balance_sheet.xlsm` is an untouched LLM draft with
**no** SUMIFS/external links yet — its `link_demo` tab shows the intended SUMPRODUCT/MAX binding. Wheat
is genuinely un-wired; the eventual balance sheet mirrors corn/soy — annual block on top, monthly blocks
below — and is Desktop's half to build. The append test still validates the *seam*, which is what D4 is about.)

---

## 1. What shipped

| # | Artifact | Where |
|---|---|---|
| 150 | `core.forecast_run` — provenance ledger; `assumptions jsonb` + `retain` publish gate; `produced_rank 1..9` CHECK | migration |
| 151 | `silver.wheat_series` `value_low`/`value_high` + `run_id` FK + hard band CHECK | migration |
| 152 | tallow `MODEL` 30→3 (644 rows) + in-migration 0-collision guard | migration |
| — | `src/forecast/guards.py` `assert_no_maxrank_collision` — the standing D7 guard | new module |
| — | `build_tallow_biofuel_use.py` — writes rank 3; `vintage_rank` in upsert; guard call | code |
| — | `write_wheat_flat_files.py` — projects band columns (v1.1); guard call | code |
| — | `write_oils_supply_flat_files.py` — `canola_nonbio_series` two-vintage split | code |

### Storage (D4/D5) — migrations 150, 151

- **`core.forecast_run`**: one row per mechanical-forecast invocation. `retain=false` = scenario/what-if,
  logged for provenance but **never** enters the book or a flat file; `retain=true` publishes into
  `silver.<commodity>_series`. `assumptions jsonb` captures the full assumption set so any published row
  traces back to the exact inputs. `produced_rank` CHECK-constrained to 1..9.
- **`silver.wheat_series` band + CHECK**: `value_low`/`value_high` (projected to the flat file as trailing
  columns), `run_id` (DB-only, one-hop provenance to `forecast_run`), and the **hard band gate**:
  `rank 1..9 ⇒ value + bracketing band present, NULL never`. Actuals (rank ≥10) unconstrained.

**Verified live (rolled back, pilot table untouched):** the CHECK **rejects** a band-less rank-3 row, a
value-outside-band row, and a NULL-point rank-3 row; **accepts** a valid banded forecast, a tight band
(`low=value=high`), and all actuals. `forecast_run` insert works (uuid/jsonb/retain); `produced_rank=10`
rejected.

### Vocabulary (D7/D8) — migration 152 + guard

- **tallow `MODEL` 30→3.** Verified behavior-neutral on the live table before moving: **0 rows at ranks
  4–29** (MODEL is the floor either way), actuals 60–95 still override, **no consumer hardcodes 30**, 644
  rows moved, **0 MAXIFS collisions** after. Builder now writes 3 and carries `vintage_rank` in its upsert
  `DO UPDATE SET` so a re-run can't silently leave rows at 30.
- **standing collision guard** (`assert_no_maxrank_collision`): raises `MaxRankCollision` if any key
  carries >1 vintage at its max rank (which would make `DISTINCT ON` drop one and `SUMIFS` double-count).
  Wired into the wheat writer and tallow builder. **Verified:** returns 0 (clean) on both live tables;
  **raises** on an injected collision (rolled back). This is the D7-item-2 "guard, not a hope."

### Canola split (D8 decision 2) — `write_oils_supply_flat_files.py`

Canola `non_biofuel_use` was a single collapsed `RESIDUAL`(50) — actual-derived history and forward in one
vintage, and in fact **no forward projection at all** (it stopped at the biofuel frontier). New
`canola_nonbio_series()` mirrors soy's structure: **`RESIDUAL_ACTUAL`(90)** for actual-input history +
**`FORECAST_SEASONAL`(40)** forward (current-MY remainder + 2 MYs, trailing-3MY-avg × 5yr seasonal).

**Verified:** canola non-bio now 90 over 2024-01..2026-04 + 40 over 2026-05..2028-09 — **same forward
horizon as soy**, disjoint periods (no collision). Soy unchanged. Canola's ≥0 clamp (import-dominant,
disappearance incomplete — Tore's ruling) retained on the actual history.

---

## 2. Decisions taken this session

- **Biofuel gap — DEFERRED (Tore, 2026-07-24).** The 495 forward `#VALUE!` in the soyoil balance sheet
  (the ~17-month un-forecast biofuel gap) stay **loud** — they are an honest signal, not a bug. Closing
  the gap is the **first real D5 forecast callable** and gets its own focused session, not a bolt-on to
  clear cosmetic errors. (Also: the soyoil balance sheet reads the flat file positionally, so a fill needs
  the positional wiring handled too — another reason it's its own piece.)
- **D8 decision 1 (roll soy/canola `FORECAST_SEASONAL` 40 → 1–9 band) — DEFERRED, with rationale.** The D4
  hard gate requires every 1–9 row to carry a band; soybean_oil/canola_oil have no `silver.*_series` table
  or band CHECK yet, so moving them into 1–9 now would create **unbanded band-rank rows**, violating D4. At
  40 vs a 1–9 rank the MAXIFS result is **identical** (forward carries only the forecast), so the roll is
  cosmetic and waits for the oils `*_series` migration. **Gate beats parameter.** Canola matches soy at 40.

---

## 3. Not verified / known gaps — the next session opens against the cheapest of these

- **[ ] Tallow rank-3 MODEL is UNBANDED.** `silver.tallow_balance` is not a `*_series` table and has no
  band CHECK, so the relocated rank-3 MODEL carries no `value_low`/`value_high`. The D4 band guarantee
  holds **today only on `silver.wheat_series`** (the pilot). Documented, not silent. Closes when tallow
  migrates to the `*_series` pattern.
- **[ ] Oils forecast rows (`FORECAST_SEASONAL`=40) are unbanded and above the 1–9 band.** Same root cause:
  no `silver.*_series` table for the oils. The 40→band roll and band columns both wait on that migration.
- **[ ] The forecast band is empty everywhere.** `core.forecast_run` has 0 published rows and
  `silver.wheat_series` has 0 rows at rank 1–9. The storage + gate exist; **no forecast has been produced
  yet.** The real gap (per the design and the session-5 handoff) is **callables, not storage** — currently
  2 callables (`implied_feedstock_value`, `weather_yield`), 0 forecast callables. **This is the next
  session: build the first D5 forecast callable (the biofuel-feedstock-use forecast to close the gap).**
- **[ ] `run_id` passthrough not exercised.** The column + FK exist; no writer has yet written a forecast
  row with a `run_id`. The wheat writer's `DISTINCT ON` tolerates the extra non-key column (it's not in
  `COLS`), but the retain=true → series-row → run_id path is untested end-to-end.
- **[ ] `silver.fuel_production_forecast` (902 `is_forecast`-boolean rows) not converted.** D1–D4 imply it
  becomes rank-banded rows; the mapping is unspecified and untouched this session.
- **[ ] Band semantics per series undocumented.** No `_meta` convention written yet for any forecast series.
- **[ ] The collision guard is not wired into the oils writer.** It's a SQL guard over a single silver
  table; the oils writer builds from ad-hoc tables + in-memory rows. The canola change introduces no
  same-rank collision (verified: 90 and 40 periods disjoint), but an in-memory guard for the oils writer is
  a follow-up.
- **[ ] Migration tracker is 68 behind.** Many pre-tracker migrations (incl. 149) were applied manually and
  never recorded; a blanket `apply_migrations.py apply` would be dangerous. 150/151/152 were applied via
  single-version `apply <v> --yes` and **are** recorded. Reconciling the tracker is separate work.
