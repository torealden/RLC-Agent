# Prompt for an LLM to build "AI Is Not Coming For Your Job" v8

Use this prompt with Claude Sonnet 4.6, GPT-5, or any current frontier LLM
that can produce structured deck outlines. Paste it in full. Iterate on the
output before generating actual slides.

---

## Role and goal

You are a presentation designer working with a senior commodity-market
analyst (Tore Alden, owner of Round Lakes Commodities). You are building
the next version of his signature talk, "AI Is Not Coming For Your Job,"
which he gives at industry conferences and to corporate clients in the
agricultural / biofuel / energy commodities space. The audience is
sophisticated, skeptical, often older commodity traders, analysts,
operations executives, and risk managers — people who have seen AI hype
cycles and are not impressed by demos. They want to know what's actually
working, what isn't, and what it means for their careers.

This is the **8th** iteration of this talk. Previous versions argued the
abstract case that AI is augmentation not replacement. **This version
needs to be different: it has to be grounded in concrete proof, not
philosophy. RLC has spent the past three months building a working
production system that demonstrates exactly what AI-augmented commodity
analysis looks like. The deck should walk through what was built, with
specific numbers, and let the system itself argue the point.**

The deck should run roughly 30–45 minutes spoken, ~25–35 slides.
Tone: confident, direct, occasionally dry. Tore prefers honest pushback
over flattery, including in his own slides. He values calibrated language;
no superlatives without numbers behind them.

## What the deck must accomplish

1. **Reframe the conversation.** Most AI-and-jobs talks debate replacement
   vs. augmentation in the abstract. This talk shows what augmentation
   actually looks like inside a real commodity research workflow,
   with the working code, the data corpus, and the cost numbers.

2. **Demonstrate the analytical stool framework.** Three legs:
   positioning, fundamentals, and sentiment. Most platforms have one
   or two; almost none have all three integrated. RLC has built all
   three on facility-resolution data. This is the deck's central
   technical claim and what makes it different from generic
   "use AI more" decks.

3. **Show the 47-year historical corpus.** Tore has hand-annotated price
   charts going back to 1969 — the kind of irreplaceable institutional
   memory that took a career to accumulate. We just digitized 11 PDFs
   into 1,494 structured events spanning 1969-2016 in roughly four
   hours of compute time and ~$25 of cloud LLM cost. **The point:
   AI doesn't replace the analyst's career. It makes that career's
   accumulated wisdom queryable for the first time.**

4. **Be honest about what AI cannot do.** Calibration still needs
   domain expertise. Cursive handwriting still requires manual review
   on ~20% of cases. Local LLMs are not good enough for client-facing
   work. JSON schemas still break on local models. The audience will
   trust the optimistic claims more if the pessimistic claims are
   front and center.

5. **Land the career message in concrete terms.** The job changes:
   less time on data collection and pattern matching, more time on
   judgment calls, calibration, network reasoning, and decision
   accountability. Tore should land the talk with the specific
   skills the job migrates toward, not platitudes about lifelong
   learning.

## Concrete material the deck should include

This material reflects the actual production system at Round Lakes
Commodities as of May 2026. Use these numbers and components verbatim.

### The Market Field — RLC's proprietary analytical layer

A unified substrate that ties facilities, market participants, and
information sources into one continuously updating state. Two layers:

**Layer 1: Sentiment dynamics on a facility graph.** Every facility
holds a sentiment vector across 8 topics (weather, soybean supply,
veg-oil demand, meal/livestock demand, federal policy, state/local
policy, industry policy, competitor activity). Each day, sentiment
updates via a DeGroot-style equation:

```
s(t+1) = ALPHA * s(t)             # decay/inertia
       + BETA  * news(t)           # local exogenous forcing
       + GAMMA * SUM(w_ij * s_j(t)) # network influence
       + EPS   * jump              # weak-tie randomness
```

with a reversal trigger when contradicting news of meaningful
intensity arrives — sentiment can flip in one day instead of decaying
gradually (the "Iran war" case from the talk's analytical bank).

Output topics scaled by current oil_share: a 45Z headline matters
more when oil revenue is half the bushel than when it's a quarter. The
math breathes with the same incentives operators do.

**Layer 2: Sentiment feeds the facility decision loop alongside
positioning and fundamentals.** Three legs converge into a single
input to the buy/sell/hold function.

### Facility-resolution graph

24 canonical Iowa oilseed crush plants, all geocoded, all canonical
duplicates resolved. 1,134 directed edges with three weight types:

- parent_company (1.0, e.g., AGP cluster of 7, Cargill cluster of 6)
- draw_region (exp(-d/50), capped at 200 mi)
- industry baseline (0.05 floor for any pair in the market)

Generalizes to any commodity market: just instantiate node set and
edge weights for that market's facilities. The math is the same.

### News collection pipeline

3 RSS feeds (BiofuelsDigest, Agweek, BrownField) plus 23 Google News
queries — one per (operator, city) cluster across all canonical
facilities. All 24 facilities have a dedicated news source. No paid
feeds; PoC built on free public data. Roughly 30-50 articles per day
flow into bronze; classified daily by Claude Sonnet 4.6 against the
8-topic taxonomy with facility relevance tagging. Daily Windows
scheduled task runs the full pipeline at 5:30am CT.

### The 47-year historical chart corpus

Tore has decades of price charts annotated by hand with the
market-moving event at every meaningful price move. We extracted
**1,494 events from 11 PDF scans** using Claude Sonnet 4.6 vision
with **best-of-N consensus** (each chart processed three independent
times, events appearing in 2 of 3 runs flagged high-agreement). Of
the 1,494 events, **702 are at agreement_score >= 0.67** —
calibration-ready. The corpus spans 1969 to 2016, covering 60+
distinct futures contracts. Cost: roughly $25 of cloud LLM time.

This is what the slide for the corpus should say literally: "47 years
of one analyst's institutional memory, queryable in a database, in
about $25 of compute time."

### Knowledge graph

Layered semantic infrastructure with 436 nodes, 395 edges, 336
expert-rule contexts, 1 callable model (more in development), and
181 source attributions. Top node types:

- 81 data_series (CFTC COT, NASS reports, FAS PSD, EIA, etc.)
- 50 commodities (corn, soybeans, soybean oil, crude tallow, RBD soy
  oil, palm oil, ethanol, biodiesel, RD, SAF, ...)
- 37 regions
- 37 quantitative models
- 34 policies (RFS, 45Z, CFR, CORSIA, ReFuelEU, LCFS, ...)
- 29 analytical models
- 28 metrics
- 25 facilities (24 IA crushers + market participants)
- 18 seasonal events
- 17 companies

These nodes encode expert frameworks like "when managed money net
long exceeds 90th percentile, liquidation risk increases" — that's
not text retrieved by RAG, it's structured rules that drive specific
decision branches.

### Honest scope boundaries

Bake these into the deck explicitly. The audience will respect honesty
over hype.

- **Local LLMs are not good enough for the work that drives client
  decisions.** Sonnet 4.6 reads cursive at ~85-90% accuracy; the
  best local vision model we tested (qwen3-vl:8b) reads it at ~65-75%.
  That 20-point gap on bad handwriting compounds badly when building
  calibration data. We use local for high-volume, deterministic,
  privacy-sensitive work (Title V air-permit extraction, embeddings,
  audio transcription). Cloud for everything client-facing.

- **Best-of-N is required, not optional.** Single-run LLM extraction
  shows 50-70% bidirectional variance on long structured documents.
  The same model on the same chart can read "Funds Established
  Record net Long" in one run and "net Short" in another — opposite
  polarity. The fix is N runs and consensus.

- **Calibration still needs the analyst.** The chart corpus dates
  are 1969-2016. Our news+sentiment data is 2024+. There is no
  overlap, so the model cannot self-calibrate. Tore has to specify
  3-5 historical events from his memory with expected facility
  responses and the model fits parameters against that human
  judgment. Without his domain expertise, the calibration step does
  not happen.

- **There is no autopilot.** The system surfaces signals. It does
  not place trades. It does not advise clients. Every output is
  one input to a decision Tore is responsible for.

### Cost numbers worth showing on a slide

- **Daily news classification at current volume:** ~$0.30/day, ~$100/year
- **Weather brief synthesis (replaced local Ollama with Claude):**
  ~$0.10/day, ~$30/year
- **Refining margin gold view + implied unit price layer:** zero
  marginal cost (PostgreSQL views over already-collected data)
- **47-year historical chart corpus extraction with best-of-N:** ~$25 total
- **Annual production cost of running the entire system:** under
  $500/year in cloud LLM. The unfair part: this would have been
  thousands of analyst-hours and a multi-FTE data team five years ago.

## Deck structure to produce

Build these sections in order. For each section, output in this format:
slide title, bullet content (3-5 bullets max per slide, each one a
complete thought not a fragment), speaker notes (one paragraph for
each slide), and any chart/diagram suggestions.

**Section 1 — Frame (3 slides)**

1. Title slide. "AI Is Not Coming For Your Job — But Your Job Is About
   To Change. v8, May 2026." Subtitle: "What three months of building
   actually showed."
2. The honest version of the AI hype cycle. Where we are; what's still
   not working; why most of the optimism is right and most of the fear
   is wrong, but for slightly different reasons than the discourse says.
3. The argument in one sentence. *AI is the cheapest analyst you'll
   ever hire and the most expensive one to manage poorly. The
   difference is whether your domain expertise gets encoded into the
   system or stays in your head.*

**Section 2 — The three-legged stool (5 slides)**

4. Most platforms answer "where is capital positioned?" (CFTC) or
   "what do the fundamentals say?" (USDA). Almost none answer "what is
   the market believing right now and why?" The third leg is sentiment
   on a network. Explain why.
5. Schematic of the Market Field with all three legs feeding the
   facility decision loop. Use the architecture image from
   `docs/specs/market_field_spec.md`.
6. Why the network matters: one news story doesn't reach AGP Eagle
   Grove and Cargill Sioux City the same way at the same speed. The
   asymmetry is where the alpha lives.
7. The math, briefly (one slide max). DeGroot update equation, with
   the four terms and what each represents in plain English.
8. Reversal logic — the Iran war example. Sentiment can flip in one
   day when news contradicts. Most "sentiment" platforms only smooth.

**Section 3 — Facility-resolution data (4 slides)**

9. The facility graph: 24 plants, 1,134 directed edges, three weight
   types. Show as a network diagram of Iowa.
10. Why facility resolution beats sector-ETF-level intelligence.
    Cargill Cedar Rapids East and West are 1 mile apart along the
    river. Bloomberg sentiment doesn't know they exist. The Market
    Field does.
11. Generalization story. The math is market-agnostic. Iowa oilseed
    crush is the pilot. Same machinery drops into EU rapeseed,
    Brazilian soy, US wheat, biofuel feedstock chains.
12. The 47-year chart corpus. 1,494 events, 528 unanimous across
    three independent reads, ~$25 to digitize. The slide every old
    analyst will quietly love.

**Section 4 — What it actually replaces vs. what it doesn't (4 slides)**

13. What's gone: hours of news monitoring, hours of price chart
    annotation, hours of report-comparison spreadsheets, days of
    crushing-margin reconstruction. The "data collection and pattern
    matching" part of the job is largely automatable.
14. What's left for humans: calibration, judgment, accountability,
    domain expertise that turns raw signals into trade theses.
    Decisions still happen at desks. Risk still gets owned by
    people, not models.
15. The honest cost numbers. Under $500/year in cloud LLM to run
    the entire system. The cost of the human who calibrates it
    properly is the binding constraint, not the compute.
16. What we tried that didn't work. Local LLMs for handwriting
    extraction (~20-point accuracy gap). Single-run extraction
    (50-70% variance forces best-of-N). Manual chart-event curation
    (slow, error-prone, doesn't scale).

**Section 5 — How the job changes (4 slides)**

17. New job description, by skill. Concrete bullet list. (Sample:
    "Calibration: specifying expected behaviors against historical
    episodes you remember well." "Network reasoning: inferring
    second-order effects through the facility graph." "Domain
    encoding: turning your tacit knowledge into structured rules
    in a knowledge graph." "Risk attribution: assigning
    accountability when the model is wrong.")
18. The skill that becomes 10x more valuable: pattern recognition
    against historical analogs. AI is great at retrieving the
    pattern. Only the analyst can decide which pattern is
    applicable now.
19. The skill that becomes 10x less valuable: rote data ingestion.
    If you spent your career building Excel sheets that pulled
    from PDFs and emails, that part of the job is done.
20. The career math. The fewer hours you spend on data collection
    and the more on judgment, the more leverage you have. People
    who learn to work with this stuff become more valuable, not
    less. People who refuse to engage with it become less.

**Section 6 — Honest constraints (3 slides)**

21. What AI absolutely cannot do well in commodity work. Subtle
    polarity discrimination on ambiguous wording. Multi-hop
    reasoning across noisy data. Anything that requires being
    accountable to a client.
22. The variance problem. Same model, same input, different
    answer. Best-of-N is mandatory. Costs scale linearly. Plan
    for it.
23. The calibration gap. Without a domain expert specifying
    expected behavior, the system has no ground truth. Without
    ground truth, it cannot improve. The expert is the bottleneck,
    and that bottleneck is not removable.

**Section 7 — Close (2-3 slides)**

24. The single sentence to take away. *Your job becomes higher-
    leverage, not extinct. The leverage is in calibration,
    domain encoding, and judgment — three things that took you a
    career to develop.*
25. Specific advice for the audience. Three concrete steps an
    analyst or risk manager should take in the next 60 days to
    start migrating their job upward. (Tore should fill these in;
    the LLM should suggest 3-5 candidates that are specific
    enough to be useful, not platitudes.)
26. Optional: live demo slide. If Tore presents in a setting where
    he can do live, the dashboard at
    `dashboards/market_field/app.py` shows the network alive in
    real time. Heatmap, facility trajectory, recent news. Closes
    the talk with proof.

## Constraints on output style

- **No filler bullets.** Every bullet must be a concrete claim that
  could be defended.
- **Specific numbers everywhere.** "1,494 events," not "thousands
  of events." "47 years," not "decades."
- **Use exact RLC terminology.** "Market Field," "facility graph,"
  "three-legged stool," "best-of-N consensus," "DeGroot update."
- **Speaker notes should be conversational.** Tore reads them; they
  should sound like him talking, not a corporate marketing voice.
- **Include the bracketed facility names** when relevant (`ia.agp_eagle_grove`,
  `ia.cargill_cedar_rapids_east`, etc.) — these are real keys from
  the production system and signal substance.
- **Tone test:** if a sentence could appear in any AI-and-jobs deck,
  rewrite it to be specific to RLC.

## What to do first

Before writing slides, output a one-paragraph **revised arc** in your
own words: how the deck flows from "here's what AI can't do" through
"here's what we built" to "here's what your job becomes." Tore will
push back if the arc feels off. Then proceed to slide-by-slide once
the arc is approved.
