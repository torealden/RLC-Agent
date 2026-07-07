# Tallow non-bio NOWCAST (2011→present) — Code→Desktop open item

**From:** Claude Code (implementation) | **To:** Claude Desktop (methodology) | **Owner:** Tore
**Date:** 2026-07-07 | **Parent:** Tallow Ruling Doc + Addendum A (2026-07-06)
**Status:** OPEN — blocks only the *modern* `non_bio_trend` leg of `silver.tallow_balance`.
Everything else (production, trade, CIR-era segmentation, code plumbing, diagnostics) is
building in parallel and does not depend on this ruling.

---

## The gap

Addendum A pins the non-bio **segmentation structure and shares** from CIR M311K, and the
fits reproduce the ruling exactly (validated 2026-07-07):

| Param | Computed (2005–2010) | Ruled |
|---|---|---|
| CIR tallow production (c55+c59) | 5.16–5.69B | 5.2–5.7B ✓ |
| `ibft_share` (c59/c64) | 0.543–0.591 | ~0.55–0.60 ✓ |
| `EDIBLE_SHARE` (c55/(c55+c59), pooled) | 0.335 | 0.32–0.35 ✓ |
| Feed IBFT (c235) 2005→2010 | 1193M→332M | 1193M→332M ✓ (exact, the BSE two-chapter signature) |

**But the CIR consumption block dies 2011-07.** The allocator needs `non_bio_trend` monthly
for **2011→2025** (really 2015+, where real NASS production begins). The ruling docs specify:
- §4: the *identity* (non_bio_use = non_bio_trend + feed_use; feed = elastic residual).
- §7: the *forecast* method (2025→2050: exporter model, policy module).
- **Neither specifies the 2011→present nowcast of `non_bio_trend`.** That gap sets how far
  RLC tallow biofuel falls below EIA's 8.65B, so it is load-bearing for the public number.

## What Code proposes as the default (pending your ruling)

1. **oleo (c233) + other-inedible (c239+c240+c245)**: carry the 2007–2010 CIR-fitted share
   **constant forward** as a share of IBFT supply (IBFT-allocated via `ibft_share`). Slow-moving,
   holds its share of a growing production base. This is the intent behind Tore's earlier
   "~9.9%-of-production" shorthand and honors A7 ("anchor shares, not totals").
2. **feed (c235-seeded)**: elastic residual per §4, seeded at the post-BSE floor (~332M, 2010
   exit level), bid down toward `FEED_FLOOR` as RD demand rises 2021+. `FEED_FLOOR` = parameter.
3. **EBFT**: A5 disappearance identity → food use; EBFT-to-biofuel ≈ small.

Lands 2024 biofuel-available ≈ **5B** (IBFT 3.88 + net imports 1.58 − oleo/other ~0.6 −
feed floor ~0.3 + small EBFT), vs EIA 8.65B — matches the ruled "~5B biofuel."

## Decisions requested

1. **Confirm or replace the constant-share carry** for oleo+other-inedible 2011→present. If you
   want a trend (e.g. oleochemical demand growth) rather than flat share, specify the slope/source.
2. **`FEED_FLOOR` value** (or a rule to derive it). Post-BSE 2010 exit was 332M IBFT feed; RD
   has bid feed down further since 2021 — what is the structural minimum?
3. **Anchoring base for the share**: share of IBFT *production*, or IBFT *supply* (prod + net
   imports)? Imports are large and inedible-heavy post-2020; this changes the level materially.
4. **EBFT biofuel** in the modern era: hold ≈0 (all food), or model a small growing share?

Until ruled, Code stores the modern `non_bio_trend` leg with a `PENDING_DESKTOP` methodology
flag in `_meta` and uses the default above so the pipeline runs end-to-end; the guardrail is
not fed to a live allocator re-run (that is held for UCO regardless, per Ruling §5).
