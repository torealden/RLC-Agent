# Path to a Complete, Tested US Model

**Drafted 2026-06-20.** Reconciles the question "do we finish Iowa agents first, or
collect all national facility data first?" against the locked Notion roadmaps
(Master Roadmap 2026-05-02, Phase Two Facility Agent Architecture) and defines the
concrete path to a fully set-up and tested **US** analytical model — including where
the biotracker and VaR actually sit.

---

## 1. The fork, and the recommendation

Tore's instinct (this session): **collect all facility info nationwide first**, then
model by industry (crush first), then build the quarterly risk budget per
facility/company. Reasoning: multi-state networks (ADM, Cargill, Bunge) make
state-by-state setup artificial — decisions about a network need all its nodes visible.

The locked roadmaps say the opposite: **Iowa-first depth** — build the full vertical
stack for ~17 Iowa crushers, validate vs NASS, then replicate.

**These only conflict if you treat "data" and "modeling" as one thing. Split them:**

- **Data acquisition → breadth-first is correct (and cheap).** The permit pipeline now
  works end-to-end (288 IA facilities drained on local GPUs, ~free). Running it for
  other states is just GPU-hours. And Tore's multi-state argument is right: you cannot
  model ADM-Iowa in isolation from ADM's network. So **acquire the national facility
  census now**, before modeling.
- **Economic modeling / agents → depth-first is correct.** You do not stand up 500
  facility agents at once. Build the **oilseed_crush** model fully (best-understood
  industry, math largely exists), validate against NASS monthly crush, *then* replicate
  the pattern to the next industry. Building breadth-first here would mean scaling an
  unproven model.

**Synthesis: breadth-first on acquisition, depth-first on modeling.** Concretely — when
you model the Iowa crush vertical, pull in the out-of-state facilities of any
multi-state operator whose Iowa node you model, so each *company* network is whole even
while the *industry* depth is Iowa-first. That directly answers the multi-state worry
without abandoning the de-risking value of a narrow first vertical.

This is a refinement of the locked roadmap, not a replacement: Phase Two still stages
modeling Iowa→IN/NE→industries→country; we're just front-loading the cheap national
*data* sweep and making company-networks the unit of completeness.

---

## 2. Where we are today (honest inventory)

**Done / working:**
- Permit acquisition + extraction pipeline (acquirer → bronze → parse_spine best-of-N →
  chunked retry for large permits → organized archive). 288 IA facilities, 8,847 units.
- Medallion DB: ~89 bronze / ~93 silver / ~180 gold; 41 collectors.
- US balance sheets (the spreadsheet/S&D side) — mostly built; Tore can run the US
  model on US balance sheets today.
- IFV math is BUILT and registered: `src/agents/facility/hefa_economics.py` +
  `src/kg/callables/implied_feedstock_value.py` (`implied_feedstock_value_v1`).
- KG: 436 nodes / 395 edges / 336 contexts + kg_callable layer + forecast_book scaffold.

**Partial / scaffolded:**
- Facility economic models beyond IFV/HEFA (crush margin per-facility) — pieces exist.
- Forecast tracking (`deployment/forecast_tracker.py`, `core.forecasts`) — scaffold.
- Facility agent L1–L4 (strategic/daily/signal/aggregation) — designed, not built.

**Not built:**
- National facility census (only IA permits drained so far).
- Facility *state* layer, catchment topology, decision-execution shell (Phase Two's 5 gaps).
- Symbiotic reconciliation (facility roll-up vs balance sheets) end-to-end.
- **BioTrack** railcar detection — Planning only (YOLOv8/OCR design exists, no data feed).
- **VaR / quarterly risk budget** — scoped as a SEPARATE product (decision IFVS-003;
  David leads; shared backend eventually). No spec page yet.

---

## 3. What "US model completely set up and tested" means

Two sides that must meet:
- **Top-down (spreadsheets):** US commodity balance sheets / S&D forecasts. ~Ready.
- **Bottom-up (facilities):** per-facility economics → aggregate to industry flows.
- **Reconciliation (symbiotic):** the two sides forecast in parallel and reconcile
  against realized data (NASS crush, EIA production, Census trade). The gap between them
  is itself signal.

"Tested" = run the bottom-up crush aggregate against realized NASS monthly crush for a
backtest window and measure error (roadmap target: ±5%). That is the definitive SWOT
test of the system for domestic use, exactly as Tore framed it.

---

## 4. Phased plan

### Phase A — National facility census (breadth, cheap, do now)
Run the permit/acquisition pipeline beyond Iowa for RLC's focus industries.
- A1. Pick state order by RLC relevance: IL, NE, IN, MN, IA-done, then OH/MO/SD/ND/KS,
  then crush/RD/SAF states outside the belt (CA, LA, ND for RD; WA, etc.).
- A2. Generalize the acquirer per state air-permit portal (IA DNR was the first client;
  each state agency differs — this is the real work, not the GPU time).
- A3. Drain → bronze → publish to `permits/<industry>/<state>/<facility>/`.
- A4. **Company-network rollup:** link facilities to parent operators (ADM, Cargill,
  Bunge, Green Plains, Chevron/REG, Valero, ...) so multi-state networks are queryable
  as a unit. (SEC/EDGAR operator data from project_public_filings_extraction can enrich.)
- Output: national facility census with equipment lists + operator graph.

### Phase B — Oilseed crush economic model (depth, the proving vertical)
- B1. Per-facility crush economics: capacity (from permit throughput) × crush margin
  (board crush + basis) → facility crush volume + P&L. Seasonality from the spreadsheet
  work IS the baseline (per Phase Two).
- B2. Facility state layer + strategic plan (L1 quarterly Opus memo: coverage ratios,
  basis ceilings, hedge ratios) for the IA-17 + their out-of-state network nodes.
- B3. Aggregate facility crush → monthly US crush; **backtest vs NASS** (±5% target).
- B4. Reconcile against the top-down balance sheet crush line. Measure the gap.
- This is the GO/NO-GO test for the whole bottom-up approach.

### Phase C — Replicate the pattern to the next industries
In RLC-relevance order: biodiesel → renewable_diesel/SAF (IFV math already exists here,
so this may actually be the easiest second vertical) → ethanol/corn-wet-milling →
livestock/render. Each reuses the B-pattern: permit→economics→agent→aggregate→reconcile.

### Phase D — Symbiotic reconciliation + forecast tracking (the "model" proper)
- D1. Wire facility aggregates + balance sheets into `core.forecasts` / forecast_book.
- D2. LLM forecasts each monthly series in parallel; reconcile vs realized as it lands;
  track accuracy (human vs LLM vs USDA — the forecast-comparison endpoint).
- D3. This is "US model complete": both sides live, reconciling, accuracy-tracked.

### Phase E — Extensions (NOT critical path to a tested US model)
- **BioTrack (catchment):** rail-car detection feeds *which facility draws feedstock
  from where*. Improves the catchment-topology gap, but the US model is testable without
  it (use proxies: trade flows, basis, capacity-weighted catchment). Honest call: this is
  the most speculative + expensive piece (needs a camera/data feed, CV stack). **Defer
  until after Phase B proves the model** — don't let it block.
- **VaR / quarterly risk budget:** the risk product on top of the facility P&L + IFV +
  forecast distribution. It's the natural home for "quarterly risk budget per
  facility/company." Per IFVS-003 it's a separate product (David leads). It needs Phase B
  facility P&L + Phase D forecast distributions as inputs, so it's genuinely downstream.

---

## 5. Where biotracker and VaR sit (direct answer)

Tore asked for a path "including the biotracker and VaR." Honestly:
- **Neither is on the critical path to a *tested* US model.** The definitive domestic
  SWOT test (Phase B backtest vs NASS, Phase D reconciliation) can be done without them.
- **VaR** is the right *next product* once Phase B/D give it inputs, and it's the
  formalization of the "quarterly risk budget" Tore wants. Sequence it right after a
  working crush vertical + forecast distributions.
- **BioTrack** is a catchment-accuracy enhancement, not a foundation. It's also the
  highest-uncertainty build (data feed unsolved). Recommend: prototype only after Phase B,
  and only if catchment proxies prove insufficient.

If the goal is "tested US model fastest," the order is **A → B → D**, then **VaR**, then
**BioTrack** as accuracy polish.

---

## 6. Is this unique among publicly available services? (honest)

**Partly. The moat is the synthesis, not the data.**
- The **facility data + equipment lists** are NOT a moat — anyone can parse permits; you
  just did it unusually cheaply (local GPUs). Cost advantage, not defensibility.
- Incumbents do *pieces*: S&P Global Commodity Insights / Platts, Argus, OPIS, Stratas,
  LMC International, DTN/ProExporter have facility databases, capacity tracking, crush/RD
  margin models, and price assessments. These are expensive, mostly aggregate or
  consulting-grade, and **not** agent-based, not real-time facility simulation, and not
  published as an implied-feedstock-value index.
- What's genuinely differentiated: **facility-agent simulation + the Implied Feedstock
  Value framework + the sentiment/market-field layer + symbiotic reconciliation,
  published as a PRA-style index.** That *combination*, at an accessible price, appears to
  have no direct public equivalent.
- **The honest caution:** none of the individual layers are hard for a well-funded
  incumbent (S&P, Argus) to build if they decide to. The defensibility is speed, focus,
  the IFV framing as a recognized benchmark, and being first to publish it as an index.
  Don't assume the data layer is the moat — it isn't.

Net: the product vision is real and rare in public form. Build it like the moat is the
integration + the index brand, because it is.

---

## 7. Open decisions for Tore
1. **Confirm the breadth/depth split** (national data sweep now; crush-depth modeling
   first, company-networks whole). If yes, Phase A acquirer-generalization is the next build.
2. **State order for Phase A** — propose IL, NE, IN, MN next (crush/biofuel density).
3. **Second modeling vertical** — biodiesel/RD (IFV math exists) vs ethanol (more
   facilities, more data). I lean RD/SAF: the hard math is already built and it's the
   commercial story (IFVS, feedstock report).
4. **VaR ownership** — Notion says David leads. Does that still hold, and does he need
   the Phase B/D outputs defined as his input contract?
5. **BioTrack** — confirm it's OK to defer to post-Phase-B (recommended).

---

_This plan is a refinement of the locked Master Roadmap + Phase Two architecture, not a
replacement. Sync decisions back to Notion RLC OS per the dual-Claude coordination pattern._
