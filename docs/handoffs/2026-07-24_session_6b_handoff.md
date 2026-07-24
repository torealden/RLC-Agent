# Session handoff — 2026-07-24 (ledger 6b: forecast layer BUILD)

Read this, then **verify what it claims before acting on it.** Full detail + evidence is in
`docs/specs/forecast_layer_build_v1.md`; this is the short state + next-opener.

Session 6b built the forecast layer's **storage + vocabulary**. Six commits, all verified live. The one
gated decision (biofuel gap) and one design deviation (D8-dec-1) are both settled below.

---

## 1. What shipped (commits after `35f1b77e`)

| Commit | Artifact |
|---|---|
| `9b1631f2` | migrations 150 (`core.forecast_run` + retain gate) + 151 (`wheat_series` bands + `run_id` + hard band CHECK) |
| `c1664e8f` | migration 152 (tallow `MODEL` 30→3) + `src/forecast/guards.py` standing collision guard + wiring |
| `2deaf456` | canola non-bio two-vintage split (`RESIDUAL_ACTUAL`90 + `FORECAST_SEASONAL`40, like soy) |
| `5ca6380c` | wheat flat file → contract v1.1 (band columns projected) |

**D4 gate PASSED** first: the real §4 `SUMIFS(MAXIFS())` contract on the live 1,218-row wheat flat file,
4 keys × 5 vintage ranks, recalculated in Excel before/after a 13→15 column append with bands on every
row — **zero shift**. Structural backing: no ListObject tables, no defined-name ranges, binding by
explicit column letter. Trailing-column bands are non-breaking; `flat_file_contract` → v1.1.

---

## 2. State by deliverable (all DONE, verified)

- **Storage (150/151).** `core.forecast_run` (retain gate: false=scenario-only, true=publish;
  `assumptions jsonb`). `silver.wheat_series` gains `value_low`/`value_high` (projected to flat file),
  `run_id` FK (DB-only), and a **hard band CHECK** (rank 1..9 ⇒ value + bracketing band, NULL never).
  Verified rolled-back: rejects band-less / inverted / null-point forecasts; accepts valid + tight bands +
  all actuals.
- **Tallow `MODEL` 30→3 (152).** Behavior-neutral (0 rows at 4–29, actuals 60–95 override, no consumer
  hardcodes 30, 644 rows moved, 0 collisions after). Builder writes 3 + `vintage_rank` in upsert.
- **Collision guard.** `src/forecast/guards.py::assert_no_maxrank_collision` — 0 (clean) on both live
  tables, **raises** on injected collision. Wired into wheat writer + tallow builder.
- **Canola split.** `RESIDUAL_ACTUAL`(90) history + `FORECAST_SEASONAL`(40) forward, same horizon as soy
  (2026-05..2028-09), disjoint periods. Soy unchanged.

---

## 3. Decisions settled (Tore, 2026-07-24)

- **Biofuel gap → DEFERRED.** 495 forward `#VALUE!` stay loud (honest signal). Closing them = the first
  real D5 forecast callable, in its own session — **that is the next session** (see §5).
- **D8-decision-1 (roll soy/canola `FORECAST_SEASONAL` 40 → 1–9 band) → DEFERRED, with rationale.** Oils
  have no `*_series` table/band CHECK, so 1–9 rows there would be unbanded (violates D4's hard gate). The
  roll is MAXIFS-identical (cosmetic) and waits for the oils `*_series` migration. **Gate beats parameter.**

---

## 4. Known broken / unverified — do NOT assume fixed

- [ ] **The forecast band is EMPTY.** 0 rows in `core.forecast_run`; 0 rows at rank 1–9 anywhere. Storage +
      gate exist; **no forecast produced yet.** The real gap is callables (2 today, 0 forecast callables).
- [ ] **`run_id` passthrough untested** end-to-end (retain=true → series row → run_id). Column/FK exist.
- [ ] **Tallow rank-3 MODEL + oils `FORECAST_SEASONAL`=40 are UNBANDED** — not `*_series` tables, no CHECK.
      D4 band guarantee holds today only on `silver.wheat_series`.
- [ ] **`silver.fuel_production_forecast` (902 `is_forecast`-boolean rows) not converted** to rank-banded.
- [ ] **Migration tracker 68 behind** (149 incl.); 150/151/152 applied via single-version `apply` and ARE
      recorded. Blanket `apply` is unsafe. Reconciling the tracker is separate work.
- [ ] **Carried from session 5, still open:** 495 forward `#VALUE!` (biofuel gap, now explicitly deferred);
      `RepointSoyOilCleanup` must NOT run; tallow 3,133 mil lb vs EIA uninvestigated; PSD 140/149 not
      ingested; `us_grain_crush.xlsm` ethanol tabs stale.

---

## 5. Open the next session with this

**Build the first D5 forecast callable — the biofuel-feedstock-use forecast** over the ~17-month gap
(May 2026–Sep 2027). A pure `(data, assumptions)` callable (fits `src/kg/callables/` + `callable_invoker`),
logged to `core.forecast_run` with `retain=true` and full `assumptions jsonb`, producing banded rows
(`value_low`/`value_high`) — the concrete thing that (a) clears the 495 forward `#VALUE!`, (b) proves the
retain → `silver.*_series` → `run_id` path end-to-end, and (c) is the first real entry in the forecast
book. Note the soyoil balance sheet reads the flat file **positionally**, so clearing the 495 also needs
the positional wiring handled, not just forecast-band rows. Cheapest first check: does soybean_oil even
have a `silver.*_series` home for a retained forecast row, or does that table need creating first?
(Per the design, only `wheat_series` exists today — likely a prerequisite.)
