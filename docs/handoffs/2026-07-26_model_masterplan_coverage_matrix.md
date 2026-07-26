# Handoff — 2026-07-26: model completion master plan + live coverage matrix

Read, then **verify before acting.** This was a framing/planning session, not a data-build session.
It produced the drawings for the rest of the RLC spreadsheet models and a living tracker for them.

## 0. What this session was

Tore asked for a game plan covering (1) the Helios/Pepsi forecasts and (2) a formalized, replicable
process for building a full country's balance-sheet set — plus the master list of country×commodity
sheets + prices to finish the project. Mid-session **the Tuesday deadline was explicitly dropped**:

> "Build as if we have no deadline, but deliver the prices/commodities Helios needs for Pepsi first.
> No shortcuts in building… we will do prices last, so get the fundamental supply and demand data
> done and then come back and do a full pass across all country/commodity combinations for pricing."

Confirmed: **"the report" = the Helios weekly veg-oils report.** SOW commodity/country coverage is
**final**; Helios is young / not deep on ags, so expect a stream of small bespoke Pepsi levers later
— build those as they come, keep the base tracker for the *regular* report only.

## 1. What shipped (durable, committed)

| Artifact | What | Commit |
|---|---|---|
| `docs/specs/rlc_model_completion_masterplan_v1.md` | The framing above the two Helios specs. **Part A** full-quality report outline (each section tagged with its data dependency); **Part B** the country×complex build SOP (B0–B9); **Part C** master matrix (Tier A/B/C/D across all complexes); **Part D** phased finish plan P0→P·FINAL. | `94047019` |
| `docs/specs/rlc_model_coverage_matrix.html` | Visual coverage tracker in the Lake, Field & Grain design system. Self-contained — opens in any browser. | `67e48534`, `5e1ce985` |
| `scripts/build_coverage_matrix_html.py` | **Live generator** — the HTML is derived, not hand-typed. | `5e1ce985` |
| `docs/SESSION_LEDGER.md` | New workstream "Model completion master plan (the framing)". | — |

## 2. The governing rulings (carry these forward)

1. **No shortcuts; Helios/Pepsi set first; fundamentals before prices.**
2. **Prices are a single final pass** (P·FINAL) across every closed fundamental sheet — one shared
   guidance-price engine, designed + back-tested once, not per-country as we go.
3. **The report defines the data, not vice-versa** — Part A is the requirements spec.
4. **Build country-major via the SOP** (Part B): flat-file (machine-written) → vintage-rank workbook
   (analyst judgment lives here) → crush link → identity tie-out → trade-loop close → forecast
   callables → recalc gate. **The SOP STOPS before price.**

## 3. The live coverage tracker — how it works (verified this session)

`python scripts/build_coverage_matrix_html.py` →

- **Imports** the coverage universe from `build_pepsi_coverage_tracker.py` (single source of truth;
  no scope re-definition).
- **Probes** each `models/Oilseeds/<country>/` folder for workbooks matching the complex →
  `empty` (no file) / `partial` (file present).
- **`VERIFIED_CLOSED`** (in the generator) is the only curated input: `("Soybean","United States")`,
  `("Corn Oil","United States")` — because "forecast-closed + tied out" is a ledger fact file
  presence can't prove.
- **Live counts (2026-07-26):** Tier A **2 done / 1 partial / 11 empty of 14**; 4 importers; 5
  rollups; 9 stubs.

**Workflow to keep it current:** build a country → drop workbooks in its folder → re-run the
generator → cell flips empty→partial; after recalc/tie-out passes, add the (complex,country) pair to
`VERIFIED_CLOSED` → partial→done.

**Note the hand-count fixes** the generator surfaced: it's **14 Tier-A builds, not 13** (the hand
draft dropped Russia from rapeseed), and oilseed sheet-sets are **5, not 6**. Trust the generator
over any number I typed in prose.

## 4. Next session — recommended: P0.5 (tagged NEXT in the grid)

**Stand up the 5 Tier-C world rollups from `bronze.fas_psd`** (palm, rape, sun, soy oil, corn oil).
Cheapest work, highest immediate return — a directional read on all five complexes with zero manual
sheet-building. When a rollup workbook lands under `models/Oilseeds/World/`, the generator auto-flips
the rollup note from "pending" to "live" (that detection is already wired: it checks the World
folder). After P0.5, P1 is the Helios oils in window-order: **sunflower first** (6–7 mo window is
live now), then palm (B50 draw), rape, soy BR/AR + corn oil.

## 5. Known-broken / unverified — do NOT assume

- [ ] **Counts beyond the oils are estimates**, not derived. Only the five oil complexes have a real
  tracker + live folder probe. P2–P6 cell counts in the master plan are order-of-magnitude, labeled.
- [ ] **US "🟡 built" ≠ verified closed.** 38 workbooks exist in `models/Oilseeds/United States/`; only
  US soy oil + US DCO are ledger-verified. The generator correctly shows only those two as `done`.
  Do not upgrade others to `done` without a recalc/tie-out pass.
- [ ] **The generator only probes `models/Oilseeds/`.** Grains/fats/biofuels/fuels live in other
  `models/` subtrees and are NOT in this tracker — it is the veg-oil (Helios) tracker by design. A
  broader tracker is future work, not done.
- [ ] **`partial` is coarse** — it means "≥1 matching workbook present," not "N of M sheets built."
  Brazil soy shows partial off one workbook. Fine for now; sheet-level granularity is deferred.
- [ ] **Price layer is unbuilt and carries real model risk** — Block C (basis to the quoted series)
  and Block B (S/U→price mapping, never fitted for any complex) are the hard, unproven parts sitting
  in P·FINAL. "Prices last" is not "prices easy."
- [ ] **Artifact URL doesn't resolve in a plain browser** (`claude.ai/code/artifact/...` → Page not
  found). Use the local file `docs/specs/rlc_model_coverage_matrix.html` or the inline render. For
  Felipe, hand off the local self-contained HTML.
- [ ] **Meal workstream (ledger 6g–6i) is still open** and unrelated to this session — it's the next
  data-build item if Tore wants to advance US completion (P0) rather than start P0.5.
