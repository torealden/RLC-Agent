# Round Lake Companies
**Brief for Helios AI — May 22, 2026**

---

## What RLC covers

Round Lake Companies is a specialist analytical firm in the biomass-based diesel (BBD) feedstock complex — soybean oil, canola oil, palm oil, used cooking oil, edible and inedible tallow, yellow grease, distillers corn oil, poultry fat — and the regulatory credit stack that prices them: D4 RINs, 45Z, LCFS, Oregon CFP, Washington CFS, Canadian CFR. We sit one layer beneath the event-driven commodity-analytics world. Most coverage of feedstocks is at the price/event layer ("UCO rallied 6¢ on the China headline"); we work in the layer that determines what the price *can be*, given the regulatory mechanics and the production-cost surface.

The firm's signature lens is the **Implied Feedstock Value framework**: invert the renewable-diesel revenue stack (ULSD + D4 RIN + LCFS credit + 45Z PTC, less OPEX and capital recovery) to derive the upper bound a producer can pay for feedstock at a given moment. It's the structural number that decides which feedstock wins the bid, which plant is in the money, and where marginal supply has to come from. The methodology is productized as an executable engine, not a one-off spreadsheet.

## What's running today

- **Data layer.** Medallion PostgreSQL architecture: 89 bronze tables, 93 silver tables, 180 gold views. 40+ automated daily collectors covering USDA FAS PSD, NASS crop progress and condition, FAS weekly export sales, CFTC COT (56,000+ rows back to 1986), Census trade with HS-code resolution, EIA petroleum and biofuels, EPA RFS RIN generation, CARB LCFS pathways (892 certified), CONAB Brazil, Statistics Canada, IBGE, INDEC, FGIS export inspections, MPOB Malaysia, weather observations (~155,000 hourly). Self-monitoring dispatcher with event log and freshness tracking.
- **Knowledge graph.** 436 nodes, 395 edges, 336 expert contexts, 181 sources. Encodes analyst-level rules: seasonal patterns, regulatory mechanics, cross-market causal links, positioning thresholds, refinery economics. Three layers: narrative `kg_context`, executable `kg_callable`, and a forecast book in `core.forecasts` that persists every prediction with scenario inputs.
- **Engines.** Pure-function math for HEFA (renewable diesel / SAF / biodiesel), oilseed crush, and the IFV scenario engine — the latter handles five 45Z policy branches (extension through 2031, expiry in 2027, ILUC removed, domestic-feedstock restriction, none). Unit-tested, callable from Python and from the KG.
- **Facility layer.** 2,001 multi-industry US facilities geocoded — crush, ethanol, biodiesel, RD, ag processing — joined to a 217,000-segment North American rail network, 150 major US ports, and 30 marine highway routes. State Title V air permits parsed via local LLM produce facility-level equipment lists and capacities.
- **Reconciliation models.** Feedstock-allocation flows cross-checked against EIA Tables 2b/2c and EPA EMTS RIN generation, with month-by-month tie-out. Defensible to refiners, obligated parties, and trading desks.

## Where RLC and Helios don't overlap

Helios's coverage strength is in the soft-commodity event layer: fruit, vegetable, cocoa, coffee, sugar, and the climate-risk and geopolitical signals that move those markets. RLC's coverage strength is in the BBD-feedstock mechanism layer: the regulatory math, the facility-level production surface, and the credit-stack arithmetic that determines feedstock equilibrium prices. Events happen on top of mechanism; mechanism determines the magnitude and durability of an event's impact. The two operate on adjacent surfaces of the same problem.

This is not a competitive overlap. It is a complementary one. A credible climate-event signal materially sharpens our forecast horizon; a credible IFV level materially sharpens the price-impact estimate that follows an event. Each side does what the other side does not.

## Where the work is heading

- **Index-provider role.** Publish forward IFV levels as PRA-style benchmarks — daily indicative for spot, weekly settlements for forward (T+1, T+3, T+6, T+12 months). Position alongside Argus, OPIS, and Platts in feedstock pricing, where no incumbent does the structural math.
- **Symbiotic forecasting.** Parallel LLM forecasts of every monthly data series, reconciled against realized data when it lands. Forecast accuracy becomes a tracked asset, not a memory.
- **Facility-agent simulation.** One agent per facility, each with its own buy / sell / utilize / idle decision loop informed by KG context and live margins. Industry-activity forecasts emerge from aggregating agent decisions across the production graph — crush → refining → biofuel → renderer — rather than from regression on lagged inventories.

## Why we should stay in touch

The natural relationship is data exchange and analytical referral. RLC produces structured outputs — IFV levels, capacity-adjusted balance sheets, facility decision data, regulatory-stack scenarios — that improve the precision of any event-impact analysis. In the other direction, Helios's event-detection signals improve the structural forecasts we publish. There is no version of this where the two firms get in each other's way.

---

*Tore Alden — Founder, Round Lake Companies — The Feedstock Report — Cape Coral, FL — toremalden@gmail.com — rlccompanies.com*
