# Helios meeting — rehearsal Q&A
**Friday 2026-05-22 — 45-min Zoom — Francisco Martin-Rayo (Helios AI)**

---

## Meeting frame (anchor in your first 5 minutes)

*Updated 2026-05-22 per Tore + Claude-Content. Joao is Tore's friend
outside work and a former direct report at Fastmarkets — the intro was a
friend intro with a side of professional hygiene, not a strategic overlap
flag. Francisco is coming in curious (Joao primed him on the palm
oil / biofuels angle and he was "really excited"), not adversarial.*

> "Joao said you were doing something interesting and we should know each
> other. He's been right about a lot of things over the years, so here we
> are. My instinct on how to use 45 minutes — let me show you what we're
> building, you show me what you're building, and we figure out from there
> whether we're adjacent, overlapping, or just both in commodities. Sound
> right?"

What this does:
- References Joao + the friend dynamic explicitly — sets the social contract.
- Vouches for Joao without overselling.
- Proposes a simple structure ("show me, I'll show you") with an open
  outcome ("adjacent, overlapping, or just both") — names the question
  lightly without making it the centerpiece.
- Ends with a question — gives Francisco agency to redirect if he wants.

If Francisco opens with "what are you hoping to get out of this," keep
the same relaxed energy: "Honestly, mostly curiosity about what you're
seeing in softs. Joao gave me the bullet-point version and I want the
analyst-to-analyst version. Plus a working relationship if it makes sense
when we're done."

## Warm-up beat (before the demo)

Before screen share, when Francisco asks about your background (he probably will), the natural one-liner is:

> "Left FM about a year ago. Before FM I was at The Jacobsen — they hired me to build an analytics business to complement their existing PRA. Joao came up under me at FM as our Brazil analyst, and we've stayed in touch — Joao and I tend to see the same things the same way."

What this does in 25 seconds: places you on the Jacobsen → FM → independent arc (analytics + PRA experience signals you understand what an index provider has to be), and establishes Joao as a peer relationship not just a vendor relationship. Don't recite the whole career — Francisco doesn't need SmithBarney through Informa unless he asks.

## Demo walkthrough — the 12 minutes you control

Five beats, ~2.5 min each. Don't go past 12 min — leave the rest for him.

1. **IFV calculator.** Show one number. UCO HEFA in California. Explain it in 90 seconds: "this is what a producer can pay for feedstock given the full credit stack — RIN, LCFS, 45Z, less OPEX." Don't dwell on the math; come back if he asks.

   *Add the education-revenue framing inside this beat:* "We're going to feature this widget on the website as both lead generation and as the entry point to the analytical service. Plus an educational layer underneath — when a new vegetable-oil buyer joins Unilever, Unilever pays for that person to take a class on the market. The education revenue is a bonus; what it really is, is a funnel into the analytical service. The kid who took the class becomes the buyer who trusts the IFV number five years later." This was the strongest single business moment of the rehearsal. Most analytical firms treat education as marketing spend. Framing it as revenue line + funnel is the version Francisco actually responded to.

2. **Policy scenario tab.** Same UCO bid, run the five 45Z scenarios. Replacement phrasing for "first-class argument": *"Policy is a dial you turn, not a fact baked into the model. Same engine, different policy world."* If asked how you chose the five scenarios: *"They're CARB's own scenario set for evaluating LCFS — not arbitrary, not my opinion. As we develop the system we'll continue to build out policy nodes."*

3. **Multi-industry facility map.** Pan to Iowa. "2,001 facilities — crushers, biofuel plants, food manufacturing, oleo-chemical, **and transportation: rail terminals, barge terminals, sea terminals**." Slow down for half a beat on the transportation layer — most analytics firms don't model the logistics layer, and including it signals depth. The map isn't the product; the structured data underneath is.

4. **KG depth viewer.** Pick one node — `rd_price_stack` or `feedstock_sensitivity_rule`. 436 nodes / 395 edges / 336 contexts. Hand-curated from primary sources, not LLM hallucination.

5. **Three-way balance sheet comparison.** Spreadsheet vs LLM forecast vs canonical USDA. **Caveat honestly:** "Facility agents aren't fully deployed yet, so today the product is mine vs USDA. The LLM column lights up once the facility-agent layer is running."

   *When/if asked why "USDA is easy to beat":* **lead with the mechanism, not the conclusion.** "USDA analysts know they move markets, so they walk revisions in over three reports instead of one. The arbitrage is being early to the direction — that's where I've built a career." Lands as observation → insight, not as boast → justification.

End with: *"That's the operational substrate. The product surface — IFV indices, the Feedstock Report, the eventual forward forecasts — sits on top of all of this."* Then stop. Don't sell.

---

## Anticipated questions, recommended framing

Grouped by what he's probably trying to figure out.

### Cluster A — "Is this overlapping with what we do?"

*Rehearsal note (2026-05-22): these questions in the friendly-CEO frame are unlikely to come up the way Cluster D and the substantive palm/SBO/policy probes do. Keep them ready but don't anchor to them.*


**Q: How is your coverage scope different from ours?**
You cover the event layer for softs (fruit, vegetable, cocoa, coffee, sugar). I work in the mechanism layer for BBD feedstocks (UCO, tallow, yellow grease, DCO, soybean oil, canola oil) and the regulatory stack around them. Different commodity verticals, different analytical layer. The places we might brush against each other are palm oil, where you might cover the climate-event signal and we'd cover the BBD-substitution effect; and SAF, where you might cover aviation-demand signals and we'd cover the feedstock-side production economics. Both of those look more like data exchange than competition.

**Q: Could either of us extend into the other's space?**
Sure, in theory. In practice it would take you years to build the regulatory-mechanics layer for BBD because the 45Z PTC formula alone has five live policy scenarios and the CARB LCFS pathway database has 892 certified pathways with CI scores that change quarterly. We've been building that for a year and it's only just usable. Going the other direction, RLC isn't going to build hyperspectral climate-risk for cocoa — that's not what we know how to do, and the customer base is wrong for our distribution. Neither expansion is plausible without dedicating a team and three years.

**Q: Why didn't Argus or OPIS build the IFV framework?**
They have the prices but not the math. The IFV framework requires modeling the regulatory stack — RFS, LCFS, 45Z PTC — as a coupled system, and then running it scenario-conditionally. Argus and OPIS publish the price components; nobody productizes the inversion that solves for feedstock value. It's a category gap, not an oversight on their part. It's the kind of gap a smaller, specialist firm can occupy before incumbents notice.

### Cluster B — "Is this real or is it a deck?"

**Q: How much of this is built versus planned?**
The medallion data architecture, the 40+ collectors, the KG, the IFV engine, the multi-industry facility map, the rail and port network, the three-way balance sheet comparison — all built and running. Air-permit extraction works for Iowa oilseed crush; biofuels and slaughter are the next industries. The forward-index publication is planned, not built. The facility-agent simulation is bones-in-place, not running. Anything else where you want to know which side of the line it's on, ask and I'll tell you.

**Q: Is this you, or do you have a team?**
Mostly me, with two analytical contractors and Claude-Opus running large chunks of the build-out as a coding partner. The Feedstock Report has a subscriber base and a webinar audience that's been running for years. RLC is operationally self-sustaining; the engineering velocity is what's unusual for a firm of this size.

**Q: What does the customer side look like today?**
The Feedstock Report (weekly research). Higby Barrett (analytics partnership). Two consulting engagements in BBD that closed in Q1. The widget+essay series we're publishing next month is the lead funnel for the next cohort. Customer mix is obligated parties, refiners, integrated oils/fats traders, and ag-focused funds.

### Cluster C — "What do you want from this meeting?"

**Q: What would 'working relationship' look like?**
Three places it could go. (1) Data exchange — your event signals improve our forecast horizon; our IFV levels improve your event-impact precision. (2) Mutual referral when a prospect needs a thing we don't do. (3) Co-authored content when there's a story that genuinely sits across both layers — palm oil substitution into BBD feedstocks is the obvious one. None of these require a deal structure or a JV. They require the two firms knowing each other.

**Q: What about a paid partnership or data license?**
Open to it if there's something specific you're missing on the BBD side. The IFV engine could be wrapped as a feed; the CARB pathway data could be licensed. But I don't think you should buy something just to have it. Tell me what's hard for you to cover and I'll tell you if we have it.

**Q: Why did Joao think we should talk if there's no deal?**
Joao watches the space and he sees two firms doing related things and didn't want either of us to find out about the other six months later in a competitive context. He's right that we should know each other; he wasn't necessarily implying a transaction.

### Cluster D — Probable curious-CEO questions (Francisco is not adversarial)

**Q: What's the moat?** *(load-bearing — this is the analytically honest answer Francisco actually responds to)*

The math isn't the moat — anyone with a regulatory expert and a spreadsheet can replicate the IFV equations. The real moat is two things working together: **a management philosophy of intellectual honesty, and twenty-eight years of relationships built on it.**

I tell every analyst that's ever worked for me: I will never be angry when we're wrong. We're trying to predict the unpredictable; we will be wrong a lot. The only thing that matters is that the client knows *why* we think what we think, so they can judge for themselves. **I have never lost a client over a wrong call.** I get to have spirited debates with retainer clients I've had for decades. That's the actual moat — it's a *relationship structure*, not a data structure. The data is the substrate that lets the relationship exist at this scale, but the data alone wouldn't sell anything.

Most analytical shops sell certainty. They build cultures where being wrong is career-ending, so analysts shade toward consensus to protect themselves, and the publication says nothing new. I'm selling intellectual honesty. Those are different products.

**Q: Why publish the framework publicly if it's your edge?**

Because the framework being public is the asset. The widget and the essay establish RLC as the firm that authored the way the industry should think about feedstock pricing. Once anchored, the paid product is the *forward* index — what implied UCO value will be at T+3, T+6, T+12. The retrospective spot calculation is a credentialing artifact, not the revenue product.

**Q: Where do you see this in 5 years?**

The aspirational anchor is Sparks Companies — Willard Sparks built an institution that carried his analytical voice without him being in every conversation, and most boutique firms never make that transition. Sparks combined founder depth with a publishing cadence customers couldn't get elsewhere, then got acquired on their own terms once the brand was credentialed. That's the structural play I'm running. Worked at Informa, worked at Fastmarkets — I've been adjacent to that lineage my whole career.

**Q: Should you be raising venture money?** *(Tore brought this up himself in rehearsal — Francisco's response gave him a useful frame)*

Probably not for the analytical business — money doesn't accelerate analytical judgment, and most VC-backed analytics firms break exactly there. **Small specific raise for the index-provider build-out (IOSCO, audit, regulator relations) is the one piece where outside capital might help**, because PRAs sell partly on looking institutional. Strategic investor (CME, ICE, Bloomberg) more likely than financial VC. But not now. Build the analyst-credibility first, decide on the institutional capital question in 12-24 months from a position of credentialing.

**Q: What does the customer side look like today?**

The Feedstock Report (weekly research subscriber base). Higby Barrett (analytics partnership). Two consulting engagements in BBD that closed in Q1. The IFV widget + essay series we're publishing next month is the lead funnel. Customer mix is obligated parties, refiners, integrated oils/fats traders, and ag-focused funds.

**Q: How much is built vs planned?** *(give the honest version)*

Built and running: medallion data architecture, 40+ collectors, KG, IFV engine, multi-industry facility map (2,001 facilities), rail and port network, the dispatcher, the air-permit extraction pipeline (Iowa oilseed crush is done), the three-way comparison machinery (Tore + USDA — LLM column pending facility-agent deployment). Planned: forward-index publication; facility-agent simulation (bones-in-place, not running); world coverage extension beyond US/Iowa.

---

## What to NOT say

- Don't quote dollar figures for the eventual forward-index product, even ballpark. You don't have a price yet and a guess will anchor him.
- Don't name specific Feedstock Report subscribers or consulting clients. He'll respect the discretion.
- Don't mention the Helios demo dashboard (`dashboards/helios_demo/app.py`) explicitly — it's a private internal artifact named for the meeting, not a product. If he asks how he can try the system, point to the public IFVS widget timeline (next month).
- Don't reference HOBO, the consulting study, or any specific consulting work by name. The capabilities you cite are the same capabilities; the attribution is internal-only.
- Don't get into the Market Field framework. It's proprietary, trademark-pending, and not part of the disambiguation conversation.
- Don't say "we're not competitive" defensively. Say "here's what we do, here's what you do, here's where they touch" and let him reach the conclusion.

## What to ASK Francisco

You have 12 min of demo, ~30 min of conversation. Spend at least half the conversation listening. Useful questions:

1. **"What's the biggest analytical gap you're working to close right now?"** — Tells you where Helios sees its own weakness. Could reveal a coordination opportunity. Could also reveal he sees BBD as that gap, in which case you have a more interesting conversation.
2. **"When you talk to refiners and obligated parties — if you do — what do they ask for that you don't currently cover?"** — Probes whether Helios has tried to extend into the BBD/refiner customer base and what stopped them.
3. **"Where do you see the climate-risk-for-softs space going in 24 months?"** — Gives him room to talk about his own vision. You learn whether his roadmap brushes against yours.
4. **"What does your relationship with Joao look like?"** — Useful context. Helps you understand why Joao made this intro and whether there's a pattern of intros Joao routes.
5. **"If we wanted to send the occasional client referral your way, who's the right person to route to?"** — Concrete, low-stakes, future-relationship-establishing. Closes the meeting forward, not backward.

## Closing

> "Send me the leave-behind doc — I've got a one-pager that lays out the analytical surface and the roadmap. Happy to keep this open ended; if something comes up where it makes sense to talk again, the door's open."

End on time. Don't run over. Don't ask for a follow-up unless he asks first.

---

*Length: ~1,600 words / 4 pages. Cheat-sheet, not script. Read once before the meeting; refer back if a hot question lands.*
