# Claude Desktop Handoff — Design the BBD Feedstock Allocation System

**From:** Claude Code · **To:** Claude Desktop · **Seam:** the canonical schema + flat-file contract.
**Your job:** *design the system that guides the flow of feedstock data* — the architecture, the
method decision, the canonical schema, the outputs, and the acceptance criteria. Code implements
the plumbing (ETL, allocator, writers) against your design. Same division as the wheat build; the
canonical table + flat file are the contract between us.

---

## 1. What we're doing and why

We audited the biomass-based-diesel (BBD) feedstock-consumption modelling and found it has drifted
into **two disconnected pipelines** plus **facility-table sprawl**. We want a single, clean,
canonical feedstock-allocation system: facility + production + policy data → per-facility →
national/PADD feedstock consumption by commodity, **reconciled to EIA actuals**, feeding balance
sheets, the three-way comparison, the dashboard, the weekly Notion update, and the client Feedstock
Report. This runs in parallel with finishing wheat, so design it **phased** — we have limited
bandwidth.

## 2. Current state (the audit findings you're designing against)

**Two parallel computations of "feedstock consumption," not connected:**

| | `gold.feedstock_allocation` | `scripts/national_feedstock_consumption.py` |
|---|---|---|
| method | economic, **margin-ranked** allocation (`src/engines/feedstock_allocation/allocator.py`) | bottom-up **assumed-mix** rollup |
| persisted? | Yes — 42,689 rows, per-facility, monthly 2006–2025 | **No — print-only** |
| last run | **2026-05-26, on the pre-cleanup facility master** | this session |
| feeds | national/PADD views → weekly Notion, dashboard, daily-ops doc, `bbd_model_v1` | **nothing** |
| state | stale + dirty (old master) | just calibrated (soy overshoot +5.40→+2.66) |

This session the mix-rollup path was heavily improved: facility-master cleanup (idled 2024–25
shakeout zombies, merged dups, killed phantom BP 2,020 mmgy → 110, excluded international +
non-lipid), real per-facility mixes, and an **EIA-derived fats-inclusive default** (`derive_defaults`)
that fixes the RD soy/canola split via `RD veg = total − biodiesel` (EIA redacts canola/corn-oil
by plant type). **None of it reached the shipped `gold.feedstock_allocation`.**

**Facility-table sprawl:** `reference.biofuel_facilities` (the one we cleaned/use) vs
`biodiesel_facilities` vs `renewable_diesel_facilities` vs `facility_capacity_biodiesel/renewable_diesel`.
Overlapping; must collapse to one canonical master.

## 3. What you need to DESIGN (the decisions that are yours)

1. **Method reconciliation — the central decision.** Design intent was: economic allocator = the
   eventual "bible," mix-rollup = the baseline it must reproduce/beat. Decide and specify the target:
   which is the canonical consumption number, and how the other relates (our lean: **allocator =
   canonical per-facility economic layer; mix-rollup = the reconciliation/validation layer; both tie
   to EIA**). Define how the calibrated mixes + `derive_defaults` priors feed the allocator so the two
   converge instead of diverge.
2. **The canonical data-flow.** Sources → medallion transforms → **one** canonical output → consumers.
   Name the canonical output (table + a LONG flat file per the wheat contract), its schema/grain
   (facility × period × feedstock_code × fuel_type, plus national/PADD rollups), and the reconciliation
   step to EIA.
3. **Facility-master consolidation.** One canonical table (`reference.biofuel_facilities`), the others
   deprecated/migrated. Specify the required fields (capacity, operating status, eligible_feedstocks,
   assumed_mix link, geo/PADD, pathway/CI) — the two load-bearing signals are **capacity + operating
   status**.
4. **Outputs/consumers + internal-vs-client separation.** Balance sheets (oilseed-complex soy-oil
   demand line), three-way comparison (spreadsheet vs LLM vs EIA/EPA), dashboard, weekly Notion,
   client Feedstock Report. Client-facing must **never** surface fastmarkets-sourced rows.

## 4. Critical requirements (non-negotiable)

- **EIA = canon.** The output must reconcile to `bronze.eia_feedstock_monthly`. Reconcile *up* toward
  EIA; where EIA splits BD vs RD, use it; use the `total − biodiesel` workaround for redacted veg.
- **Reuse the wheat flat-file contract** (`docs/specs/flat_file_contract.md`) — LONG default, stable
  key columns, period/vintage keys, writer-owned files, dynamic `SUMIFS`/`MAXIFS` reads. Don't invent
  a new seam.
- **Medallion discipline** — bronze raw, silver canonical, gold analytics. One canonical path, no
  parallel divergent computations.
- **Don't break what ships.** The weekly Notion + dashboard read `gold.feedstock_allocation` today;
  the transition must keep them working (or cut them over deliberately).
- **Anti-staleness is a design requirement, not an afterthought.** The canonical number must be
  re-derived automatically when the facility master or inputs change (scheduled + change-triggered),
  and flag when it's stale — this whole audit exists because it silently went 5 weeks stale.
- **Preserve per-facility granularity + provenance** (`run_id`, `scenario`) — the FFA vision is
  facility-level agents; keep eligibility-vs-mix distinct (soy-*eligible* ≠ soy-*used*).

## 5. What you should PRODUCE

A **system design doc** (the spec Code builds against): the canonical data-flow (a diagram/spec),
the method-reconciliation decision, the facility-master consolidation plan, the canonical output
schema + flat-file layout, the EIA-reconciliation step, the anti-staleness mechanism, and **phased
acceptance criteria** (Phase 1: re-run allocator on the cleaned master + reconcile to EIA; later
phases: full convergence, consumer cutover). Keep it phased — we're doing this alongside wheat.

## 6. Coordination

Notion = shared source of truth. Code owns implementation; you own the design/contract. Flag any
place you need Code to surface current schema/behavior before you can finalize a decision — I'll
pull it. Scope realistically against the wheat work in flight.
