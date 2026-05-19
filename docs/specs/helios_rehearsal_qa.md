# Helios meeting — rehearsal Q&A
**Friday 2026-05-22 — 45-min Zoom — Francisco Martin-Rayo (Helios AI)**

---

## Meeting frame (anchor in your first 5 minutes)

> "Joao introduced us because there's a perception that Helios and RLC might be working in overlapping territory. I don't think we are, but the only way to know is to walk through what each of us actually does. I'll show you what we've built, you tell me what you've built, and at the end we'll know whether there's anything to coordinate on or whether we're just two firms in the same neighborhood doing different jobs."

This is the only framing line that has to land word-for-word. Everything after it can be conversational. Three things this does:
- Names the elephant (perceived overlap) instead of letting Francisco worry about it.
- Refuses the "are you a competitor" frame and replaces it with "are we adjacent or overlapping."
- Implicitly promises the meeting won't pivot to a sales pitch or M&A pitch. He'll relax.

If Francisco opens with "what are you hoping to get out of this," answer with the same frame, shorter: "Clarity on whether what we do is actually the same thing as what you do. If it isn't, a working relationship as adjacent firms."

## Demo walkthrough — the 12 minutes you control

Five beats, ~2.5 min each. Don't go past 12 min — leave the rest for him.

1. **IFV calculator.** Show one number. UCO HEFA in California. Explain it in 90 seconds: "this is what a producer can pay for feedstock given the full credit stack — RIN, LCFS, 45Z, less OPEX." Move on. Don't dwell on the math; you can come back if he asks.
2. **Policy scenario tab.** Same UCO bid, run the five 45Z scenarios. "What you're seeing is the engine doing what an analyst spreadsheet would do, but with policy as a first-class argument." The point isn't the numbers — it's that the engine handles regulatory branching natively.
3. **Multi-industry facility map.** Pan to Iowa. "2,001 facilities, rail, ports, marine highways. The map isn't the product; the structured data underneath is. When a 45Z change hits, we know which specific facilities are exposed."
4. **KG depth viewer.** Pick one node — `rd_price_stack` or `feedstock_sensitivity_rule`. Show the contexts and edges. "This is the analytical structure that drives our forecasts. 436 nodes, 395 edges. Not LLM hallucination; this is hand-curated from primary sources."
5. **Three-way balance sheet comparison.** Spreadsheet vs LLM forecast vs canonical EIA/EPA. "We're tracking forecast accuracy as a measured asset. Every prediction the engine makes goes into a forecast book and gets reconciled when the realized data lands."

End with: *"That's the operational substrate. The product surface — IFV indices, the Feedstock Report, the eventual forward forecasts — sits on top of all of this."* Then stop. Don't sell.

---

## Anticipated questions, recommended framing

Grouped by what he's probably trying to figure out.

### Cluster A — "Is this overlapping with what we do?"

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

### Cluster D — Hot questions (the ones Francisco might probe with)

**Q: You're not raising. Then how did all this get funded?**
Personal capital plus revenue from The Feedstock Report and adjacent consulting. The system was built incrementally and the infrastructure cost is dominated by Claude API spend, not headcount.

**Q: What's the moat? The 436 KG nodes are presumably not patented.**
The math isn't the moat — anyone with a regulatory expert and a spreadsheet can get to the same equations. The moat is two things working together: the data-and-engine pipeline that produces the IFV number daily without manual intervention, and the analyst credibility from twenty-eight years in feedstocks that lets the number be trusted by people who actually trade these markets. Either alone is replaceable. Together they're slow to copy.

**Q: Why publish the framework publicly if it's your edge?**
Because the framework being public is the asset. The widget and the essay establish RLC as the firm that authored the way the industry should think about feedstock pricing. Once that's anchored, the paid product is the *forward* index — what implied UCO value will be at T+3, T+6, T+12. The retrospective spot calculation is a credentialing artifact, not the revenue product.

**Q: What stops Helios from building IFV next quarter?**
Nothing technical. Practically — same answer as the "could either of us extend" question. You'd be entering a category where there's already a published framework, an established analyst voice, and a productized engine. The right move for a competitor is probably to license rather than rebuild.

**Q: Sounds like you've thought about acquisition. What would change your mind?**
Not currently considering it. If something changed, it would be capital-need driven (which is not the situation) or distribution-leverage driven (where the right partner has feedstock-customer reach we don't, which Helios largely doesn't). Honest answer: I want to know what RLC looks like as a fully-built independent firm before I think about how it fits inside someone else's roadmap.

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
