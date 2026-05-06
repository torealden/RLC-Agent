# The Market Field

A unified analytical layer for agricultural commodity markets.

---

## What it is

The Market Field is the substrate that ties facilities, market
participants, and information sources into one continuously updating
state. It is how Round Lakes Commodities replicates the way information,
capital, and physical commodity flows interact in a real market —
including the parts most analytical platforms ignore.

Most commodity intelligence platforms answer two questions well:

- **Where is the capital positioned?** (CFTC reports, open interest,
  managed-money net positions)
- **What do the fundamentals look like?** (USDA balance sheets,
  production estimates, weather data)

Both are necessary. Neither is sufficient. The third leg of the
analytical stool — **how participants interpret and react to the news
that reaches them** — is where most platforms fall silent. The Market
Field is built to fill that gap, and to integrate all three legs into
a single coherent state.

## Why it matters

Market participants do not operate on the published consensus. They
operate on their own information graph: which stories crossed their
desk, which colleagues forwarded what, which facilities they are
networked into, what their own commercial position predisposes them to
believe. A piece of information that reaches AGP's Eagle Grove plant
this morning will not reach an unrelated soybean crusher in Indiana
the same way, at the same speed, or with the same emphasis. That
asymmetry is where the alpha lives.

The Market Field models that asymmetry explicitly. By tracking how
information propagates through the real network of facilities,
counterparties, regulators, and trade channels, we can:

- Detect price moves before they show up on the tape (sentiment leads
  prices in markets where information diffusion is slow)
- Quantify when a market is approaching a regime change (consensus
  collapsing into polarization is a phase transition with measurable
  signatures)
- Spot mispricings between facilities or sub-regions where the network
  has not yet caught up to the fundamentals
- Generalize the same framework across commodity markets globally

## Architecture

The Field is two layers stacked on the existing data and knowledge
infrastructure.

```
+---------------------------------------------------------------+
| Decision layer:  facility agents (buy / sell / hold / hedge)  |
+---------------------------------------------------------------+
| Layer 2 — Three-legged stool                                  |
|                                                               |
|   Positioning  +  Fundamentals  +  Sentiment    ->  action    |
|   (CFTC, OI)     (KG, S&D)        (Field below)               |
+---------------------------------------------------------------+
| Layer 1 — Sentiment dynamics on the facility graph            |
|                                                               |
|   Each facility holds a sentiment vector across topics.       |
|   News and data enter as exogenous forcing.                   |
|   Sentiment propagates through edges weighted by              |
|   organisational, geographic, and trade ties.                 |
+---------------------------------------------------------------+
| Foundation:  facility reference, knowledge graph, balance     |
|              sheets, weather, basis levels, kg_callables      |
+---------------------------------------------------------------+
```

## Layer 1 — sentiment dynamics

Each facility holds a vector of sentiment scores, one per topic. For
the initial Iowa oilseed-crush deployment, the topic set is structured
into four categories:

| # | Topic | Category | Time-varying weight |
|---|---|---|---|
| 1 | Weather and growing conditions | Inputs | none |
| 2 | Soybean supply (input availability) | Inputs | none |
| 3 | Veg oil demand (RD, SAF, biofuel feedstock, food, industrial) | Outputs | scaled by oil_share |
| 4 | Meal and livestock feed demand | Outputs | scaled by (1 − oil_share) |
| 5 | Policy — Federal (45Z, RFS, CFR, EPA) | Policy | none |
| 6 | Policy — State and local | Policy | none |
| 7 | Policy — Industry mandates and voluntary standards (CORSIA, ReFuelEU, corporate sustainability commitments) | Policy | none |
| 8 | Competitor activity (capacity, M&A, plant openings and closures) | Industry | none |

The output-side weighting reflects an economic reality specific to
oilseed crushing: meal and oil are joint products of the same bushel,
but their relative contribution to crush revenue varies materially
over time. Pre-2020, soybean oil contributed roughly 30 percent of
crush revenue and meal carried the economics of the plant; oil was
a near-byproduct that crushers worked to dispose of, which is much
of the reason soybean oil is in nearly every processed food today.
Post-2020, with renewable-diesel capacity buildout pulling vegetable
oil hard, oil share has risen toward 50 percent and the economics
have flipped.

A 45Z headline matters more to a crusher when oil revenue is half
the bushel than when it is a quarter. A meal export embargo matters
more when meal carries the plant's economics. The Field captures
this by scaling the influence of each output-side topic by the
current oil-share, computed daily from the soybean oil and soybean
meal price series joined to per-bushel yield. The result is a model
that breathes with the same incentives the operators do.

Topics are extensible. Each market gets its own topic taxonomy,
calibrated to what its participants actually pay attention to.

For each facility *i*, each topic *k*, each day *t*, the sentiment
score evolves according to a discrete-time update rule built from
four well-studied components:

- **Decay / inertia.** Sentiment from yesterday persists, attenuated.
  Captures memory and the fact that beliefs do not turn over instantly.
- **Local exogenous forcing.** New stories or new data arriving at
  the focal facility enter as an additive shock. Each headline is
  classified for sentiment polarity and intensity at ingest.
- **Network influence.** Each facility is influenced by the sentiment
  of its neighbours, weighted by edge strength. Same-company ties
  carry the most weight; same-region, same-industry, and explicit
  trade ties carry progressively less.
- **Weak-tie randomness.** A small probability that a story jumps to
  an unrelated facility. Captures the empirical reality of
  Granovetter's strength-of-weak-ties: novel information often
  travels through unexpected channels.

Sentiment lives in `[-1, +1]`. It decays gradually toward neutrality
in the absence of new information. It can also **reverse instantly**
when a contradicting event arrives — the bombing that ended the
oil-bullish sentiment cycle around the Iran conflict is the canonical
example. Reversal is detected by opposing-polarity news above a
magnitude threshold.

## Local vs national, peer vs hierarchy

Two distinctions that surface naturally and matter for accuracy:

**Local stories** (a fire at one plant, a regional dryness forecast,
a county-level zoning decision) enter only the focal facility's
exogenous term and propagate from there. The network does the work of
deciding who else hears about it and how quickly.

**National stories** (a major geopolitical event, a federal rule
change, a USDA report) enter every facility's exogenous term
simultaneously with the same intensity. The network does not need to
diffuse information that is already universal — modelling it as
diffusing produces double-counting.

**Peer ties** (lateral, same-level): bidirectional, weighted by
relationship strength.

**Chain-of-command ties**: a separate hierarchy stacked on top of
the lateral network. Boss-to-analyst messages flow down with high
influence weight (orders, priorities). Analyst-to-boss messages flow
up only for novel or extreme content (no point reporting what the
boss already knew). Distinct from peer edges, and important: it is
how information aggregates and gets acted on within an organisation.

## Phase transitions

Opinion dynamics on networks have a deep mathematical literature
documenting that they undergo **phase transitions** as parameters
cross critical thresholds. The system can sit in a consensus regime
(everyone aligned), a polarized regime (two stable camps), or a
fragmented regime (no stable groupings). The transitions between
these regimes are not gradual — they are sudden, mathematical
discontinuities, the same kind that govern a liquid becoming a
vapour.

The coupling strength of the network, the density of connections,
and the relative influence of stubborn versus malleable actors are
what move a market between phases. The Field tracks these
parameters explicitly. When the system approaches a critical
threshold — early-warning indicators include rising correlation
between facilities, falling entropy in the sentiment distribution,
and slowing recovery from small perturbations — that is itself a
trading signal.

This is where the framework draws on the broader systemic
phase-transition literature: the same physics that explains why
markets can absorb stress for years and then crack abruptly applies
inside this layer.

## Generalisation across markets

The mathematics is market-agnostic. Each market gets its own
instantiation:

- A node set: the facilities relevant to that market
- An edge weighting scheme: the same five-tier ladder of
  organisational, geographic, industry, trade, and weak ties
- A topic taxonomy: 4–7 topics calibrated to what participants in
  that market care about
- A news-source mix: market-specific trade press plus general news

Iowa oilseed crush is the first market. The framework is designed
from day one to drop into European rapeseed, Brazilian soy, US
wheat, biofuel feedstock chains, or any other commodity market
where facility-level granularity exists.

## Status

The data foundations are built. The facility reference table, the
knowledge graph (391 nodes, 274 edges, 268 contexts), the basis-field
infrastructure, the weather pipeline, and the per-facility geocoding
are all in place. The Market Field's Layer 1 — the sentiment dynamics
on the facility graph — is the next major construction phase.

Layer 2, the integration into facility agent decisions, follows once
Layer 1 is producing daily sentiment vectors that survive sanity
checks against historical events.

## What this is not

It is not a sentiment-of-headlines aggregator. Plenty of those exist.
The differentiator is the **network propagation** and the
**three-legged integration** — sentiment as one input to the same
decision function that consumes positioning and fundamentals, with
the propagation governed by the actual structure of the market, not
a generic news-feed score.

It is also not a replacement for the existing positioning and
fundamentals analytics. It augments them. The three legs are
separately valuable; the Field's contribution is making them
analyzable as a coupled system rather than three independent screens.

---

*Prepared by Round Lakes Commodities. The conceptual framework
described here is shareable; the calibration parameters, edge
taxonomies, news-source weighting, and topic-mixture coefficients
are confidential and constitute the operational core of the system.*
