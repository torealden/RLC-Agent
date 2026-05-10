# Cargill — Climate / ESG Extraction

> **Sources:**
> 1. Cargill, *Impact Report 2025* (December 2025), 117 p. — at
>    `CARGILL/climate/cargill_climate_impact_report_dec_2025.pdf`
> 2. Cargill, *Climate Related Financial Risk Disclosure California
>    SB 261* (2025), 5 p. — at
>    `CARGILL/climate/cargill_climate_related_financial_risk_disclosure_california_sb_261.pdf`
>
> Both extracted to text + parsed 2026-05-10. Page references below
> are to the Impact Report unless otherwise noted.

---

## Corporate snapshot (2025)

| Metric | Value |
|---|---|
| Founded | 1865 (single grain warehouse, Iowa) |
| Employees | 155,000+ |
| Countries | 70 |
| Markets served | 125 |
| Annual revenue | **$154 billion** |
| Ownership | Private, family-owned (no SEC ticker) |
| HQ | Wayzata, Minnesota |
| Reporting cadence | Fiscal year June–May (switched from calendar year starting FY24) |

**Employee distribution by region:**
- Asia/Pacific: 33%
- North America: 29%
- Latin America: 25%
- Europe / Middle East / Africa: 13%

**The "Connecting the global supply chain" framework** (the
infographic we used for icon references):
1. **Source and trade** — partner with farmers/ranchers; originate,
   source, store, trade commodities; provide insights & risk mgmt
2. **Make and transport** — process meat/egg/protein/salt/oils/
   starches/cocoa/sweeteners; formulate animal feed; create
   nature-derived bio-based products & biofuels; transport
3. **Deliver for customers** — sell to manufacturers, farmers,
   ranchers, foodservice, retailers, consumers

---

## Climate goals + progress (the headline numbers)

| Goal | Target | Status as of 2025 |
|---|---|---|
| Scope 1+2 absolute emissions | −10% by 2025 (vs FY2017 baseline) | **−20.9% achieved — 2x the goal** |
| Scope 3 supply-chain emissions | −30% by 2030 (per ton of product, vs FY2017 baseline) | In progress, no headline % yet |

**FY25 climate-related investments + actions:**
- $69M invested in efficiency + sustainability capex
- 105 MW new renewable capacity contracted (+14% portfolio)
- 100+ renewable energy projects across 30 countries
- 1.21M MT CO2-equiv reduction from renewable electricity mix
- 7.7M cumulative farmer trainings since 2017
- 2.5M acres engaged in regenerative ag in North America since 2020
- 91 billion liters of water restored in water-stressed regions
- $110M invested in local communities

### Scope 1+2 reduction trajectory
| Year | Reduction vs FY17 |
|---|---|
| CY2021 | 5.5% |
| CY2022 | 10.9% |
| CY2023 | 16.1% |
| FY2024 | 15.8% |
| FY2025 | **20.9%** |

(Goal: 10% by 2025 — exceeded)

---

## Marquee renewable deal — VPPA Oklahoma

**December 2024:** Cargill signed a 15-year **virtual power
purchase agreement (VPPA)** with an 85 MW solar project located
in Oklahoma (within the Southwest Power Pool / SPP).

- Commercial operation expected: 2026
- Expected CO2 reduction: 1.54 million MT total / ~103,000 MT/year
- Will contribute emissions reductions at Cargill's largest US sites:
  - **Blair, Nebraska**
  - **Dodge City, Kansas**
  - **Schuyler, Nebraska**
  - **Friona, Texas**
  - **Wichita, Kansas**

(Note: Blair, NE and Wichita, KS are Cargill's massive corn wet
milling / bioindustrial complexes; Schuyler is beef packing; Dodge
City and Friona are beef. This is one of the few public listings of
which Cargill US sites are "the largest" — useful for our DB if we
add Cargill operator rows.)

---

## Cargill's South American soy supply chain (highly relevant to our work)

This is the public-data snapshot of Cargill's soy origination in the
five South American producing countries — useful as a cross-check on
our `gold.brazil_soybean_production`, our Argentinian S&D, and the
`bronze.fas_psd` data.

### Infrastructure (2024-2025)
| Facility type | Count |
|---|---|
| Elevators | 14 |
| Processing plants | 13 |
| Ports | 12 |
| Administrative offices | 42 |
| Commercial offices | 5+ |

### Industry-wide soy production by country (CY2024, mil MT)
| Country | Production | Source |
|---|---|---|
| Brazil | 147.4 | CONAB |
| Argentina | 48.2 | MAGYP |
| Paraguay | 10.0 | INBIO-UGP |
| Uruguay | 3.2 | MGAP |
| Bolivia | 2.0 | ANAPO |

### Cargill supplier counts + direct/indirect mix
| Country | ~Suppliers | Direct % | Indirect % | Polygon-mapped (direct) | DCF % (2020 ref) |
|---|---|---|---|---|---|
| Argentina | 5,550 | 62% | 38% | 98.28% | 99.96% |
| Bolivia | 100 | 35% | 65% | 99.84% | 96.78% |
| Brazil | 15,000 | 58% | 42% | 100% | 99.29% |
| Paraguay | 1,850 | 47% | 53% | 99.96% | 99.62% |
| Uruguay | 800 | 71% | 29% | 100% | 99.95% |

### Methodology for deforestation-/conversion-free (DCF) calculation
- **Direct suppliers:** polygon farm boundaries + historical satellite analysis
  - Brazil: PRODES (national space institute deforestation system) + INCRA-SIGEF + Federal SICAR
  - Other 4 countries: University of Maryland deforestation layer
  - All 5: USGS historical satellite imagery
- **Indirect suppliers:** municipal/regional aggregation
  - <1% sector deforestation in past 5 yrs = "negligible risk" → counted as DCF
  - ≥1% = "at risk" → required individual indirect supplier verification

### Implications for our market field
- Cargill operates in **all five major South American soy origins** —
  meaning their Atlantic price signal incorporates harvest, basis,
  and currency dynamics from the five-country block. This is the
  canonical "competitor" reference for any RLC cross-product
  forecasting against Brazilian export pace.
- Their 2020 DCF reference date is now industry-standard for the
  Cerrado / Mato Grosso EUDR compliance work — when EUDR enforcement
  bites, a Cargill-style polygon mapping is the table-stakes baseline.

---

## Climate risk framework (TCFD-aligned, per SB 261)

Cargill assesses climate risks across **three time horizons** to
2050:

| Horizon | Years | Primary risk theme |
|---|---|---|
| Short term | 0-3 yrs | Regulatory transition; chronic water stress in protein business |
| Medium term | 4-10 yrs | Increasing physical risk exposure; water stress |
| Long term | 11-30 yrs | Scenario divergence (2/3/4°C warming) |

### Scenarios analyzed
- **Low warming:** 2°C
- **Intermediate warming:** 3°C
- **High warming:** 4°C

### Identified short-term physical risk
> "Chronic water stress – particularly within the protein business
> – remains a key area of concern."

This is meaningful for RLC because Cargill's protein business
includes the same beef/poultry/turkey processors we track in our
multi-industry IA seed (Hormel, JBS, Tyson, etc., share value
chains with Cargill protein).

### Identified short-term transition risk
> "Transition risks are primarily regulatory, driven by carbon
> reduction schemes and evolving disclosure requirements."

Specific regulations they're tracking:
- California SB 253 (GHG disclosure)
- California SB 261 (climate financial risk — this very document)
- EU Corporate Sustainability Reporting Directive (CSRD)
- EU Regulation on Deforestation-free Products (EUDR)
- Corporate Sustainability Due Diligence Directive (CSDDD)
- Australian Corporations Act 2001 climate disclosure amendments

---

## Governance (per SB 261)

**Board / management oversight chain:**
- CEO = Board Chair (combined role) — **Brian Sikes** (since Jan 2023)
- CSO reports to CEO — leads ESG strategy
- Quoted exec: **David Webster** — *Specialized Portfolio and Chief
  Risk Officer*

### ESG Governance Committee (cross-functional)
Members:
- CEO
- Chief Sustainability Officer (chair)
- Chief Financial Officer
- General Counsel
- VP of Corporate Audit

Responsibilities:
- Approve/reject ESG hurdle criteria for major CapEx + JV
  (mergers/acquisitions/divestitures/JVs) over thresholds
- Monitor regulatory + customer + science trends
- Recommend new ESG policies, programs, disclosures

### Enterprise Risk Management (ERM)
Company-wide approach with:
- Risk ownership embedded across businesses
- Regular leadership + BOD review
- Holistic internal/external risk view
- Integration into strategy + investment decisions

---

## Strategic NGO + multi-stakeholder partnerships

| Partner | Topic |
|---|---|
| World Resources Institute (WRI) | Climate, land use, water |
| CARE | WASH, food access, community |
| Solidaridad | Sustainable farming, regenerative ag |
| EIT Food Accelerator Network (Europe) | Foodtech innovation |
| BestPrep | Education, food access |
| Cargill Cares Councils | Local community engagement |

EIT Food's Impact Funding Framework projects: estimated 20M MT CO2e
reduction in Europe over 15 years (Cargill is one of multiple
corporate partners).

---

## Brand programs worth knowing

- **Cargill SustainConnect™** — Australia canola/barley regen-ag
  program. 7 science-based interventions: conservation tillage,
  legumes, cover crops, nitrogen optimization, fertilizer
  substitutions. Farmer Tim Gainsford (NSW) quoted as participant.
- **AminoPlus** (this is actually AGP's brand — confirming Cargill
  doesn't claim it). [scratch — wrong company]
- (Plus the legacy Cargill commodity-trading + protein brands not in
  this document.)

---

## What this means for our calibration / FIC work

1. **Cargill is a benchmark for soy origination methodology.** Their
   polygon mapping + DCF calculation is the industry reference. When
   we build similar capabilities for biofuel feedstock traceability
   (UCO, tallow), we should adopt the same framework where possible.

2. **The five US sites singled out** (Blair, Dodge City, Schuyler,
   Friona, Wichita) tell us where Cargill's largest US energy
   consumption is. These are the priority sites for a Cargill plant
   inventory — and for understanding Cargill's marginal cost
   structure in beef/corn-wet-milling.

3. **Cargill's $154B revenue + 13 SA processing plants + 12 ports**
   is a benchmark for AGP. AGP processes ~5.5M acres of soy/year —
   Cargill operates at multiple multiples of that scope.

4. **The 2°/3°/4° scenario framework + 0-3 / 4-10 / 11-30 year
   horizons** is the structure most ag operators are now using
   for climate-driven scenario planning. Worth adopting in our
   own market-field forecasting framework.

5. **EU EUDR** — when this bites in late 2025/2026, Cargill's
   polygon coverage gives them a competitive advantage over
   smaller operators that haven't done the work. Worth noting in
   our biofuel feedstock supply analysis.

---

## Open queue (future Cargill mining)

- **SoyInfo Center book on Cargill soy** (1940-2020, 6.8 MB)
  downloaded but not yet text-extracted. Same playbook as ADM/AGP:
  extract → mine → curate.
- **Cargill US plant inventory** — these climate docs surface 5
  named US sites (Blair, Dodge City, Schuyler, Friona, Wichita).
  Need to enumerate the rest from public sources before we can
  attempt our own facility-level Cargill model.
- **Brian Sikes** confirmation as CEO+Chair (since Jan 2023). Add
  to executive ledger if Cargill enters our operator set.

---

## Cross-document validation

The two docs are consistent:
- SB 261 disclosure cites the Impact Report for water strategy
  detail, climate goals, and supply-chain emissions data.
- SB 261 is a 5-page regulatory summary; Impact Report is the
  117-page comprehensive narrative. SB 261 reads like a
  TCFD-formatted index INTO the Impact Report.

This is the standard pattern for the new wave of climate disclosure
regulations: a short regulator-friendly TCFD-formatted disclosure
that points at a long-form sustainability report.
