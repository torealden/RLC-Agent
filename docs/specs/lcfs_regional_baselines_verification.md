# LCFS-style regional baseline CI — verification notes

*Task #65. Researched 2026-05-22 ahead of IFVS spec lock with Claude-Content.*

The IFVS spec (notion.so/365ead023dee813daee1e31b22219327, §5.2) lists 2026 baseline CI values for the four LCFS-style regional programs. This document verifies each against the published regulations.

## TL;DR

| Region | IFVS spec value | What the source says | Status |
|---|---:|---|---|
| California (LCFS) | **88.62** | CARB declining schedule, diesel pool 2026 | ✅ Confirmed in earlier research |
| Oregon (CFP) | **94.32** | OR-GREET 4.0 baseline = 104.92 (2026 reference); annual target probably ~91-92 by linear interp | ⚠️ Spec value is between baseline and target — needs clarification of *which* number to use |
| Washington (CFS) | **91.45** | Not verified — Ecology publishes schedule in WAC 173-424 | ⚠️ Unverified |
| British Columbia (LCFS) | **88.42** | Not verified — Ministry of Energy publishes schedule in B.C. Reg 234/2012 | ⚠️ Unverified |

## What I found

### Oregon CFP — important nuance discovered

Oregon has **two different "CI" numbers** that both look like baselines but mean different things:

1. **Reference baseline CI** — the unaltered fossil-diesel CI used for credit-value-per-MJ math
   - 2025 = 101.74 g/MJ (pre-OR-GREET 4.0)
   - **2026 = 104.92 g/MJ** (post-OR-GREET 4.0 adoption Jan 2025; the *increase* reflects new NOx research)

2. **Annual compliance target CI** — the declining schedule fuels must beat to avoid generating deficits
   - 2025 = ~91.6 g/MJ (10% reduction from baseline)
   - 2030 = ~82 g/MJ (20% reduction)
   - 2035 = ~64.7 g/MJ (37% reduction)
   - **2026 = somewhere between, ~91 g/MJ via linear interpolation**

The IFVS spec uses the term "baseline CI" in the formula `(CI_baseline_region − CI_fuel) × MJ/gal × credit_price`. **Which number Claude-Content meant matters a lot:**

- If "baseline" = reference baseline (104.92), credits are calculated against the unaltered fossil CI. This is NOT how LCFS-style credit math works in practice — actual credits use the annual target.
- If "baseline" = annual target standard (~91 for OR 2026), Claude-Content's value of 94.32 is close but ~3 g/MJ too high.

**This same ambiguity applies to all four regional programs.** California uses the annual target for credit math (88.62 for 2026 diesel pool, per CARB). Probably the same convention should hold for Oregon / Washington / BC, in which case all three spec values may be slightly off but in the right ballpark.

### Washington CFS

Not researched in detail. Program structure mirrors California's LCFS — declining annual standards with separate gasoline and diesel pools. Authority: WAC 173-424 (Department of Ecology). Worth a focused 20-minute lookup against the actual rule.

### British Columbia LCFS

Not researched. Program is older than US LCFS-style programs (originated 2010). Authority: B.C. Reg 234/2012 (Greenhouse Gas Reduction Act). BC has more aggressive long-term targets than US programs.

## Recommendation for the IFVS spec

Two options for Claude-Content to choose between in the Decision Log:

**Option A — use annual compliance target (what credit math actually uses):**
- CA 2026 diesel: **88.62** ✅
- OR 2026 diesel: **~91.5** (recalculate from OAR 340-253 schedule)
- WA 2026 diesel: needs lookup
- BC 2026 diesel: needs lookup

**Option B — use reference baseline (cleaner mental model but doesn't match credit-math reality):**
- CA 2026 diesel: ~95 (pre-declining baseline) — but CARB doesn't really publish this separately
- OR 2026 diesel: 104.92 (OR-GREET 4.0)
- WA / BC: would need their reference-baseline values

**My recommendation: Option A.** The IFVS widget shows users the credit value per gallon — that value uses the annual target in the actual formula, not the reference baseline. The "baseline" terminology in the IFVS spec should be renamed "annual target CI" or "compliance standard CI" to avoid future confusion.

## Open action items

1. **Pull OR diesel 2026 target from OAR 340-253-8010** (actual regulation). Probably available in PDF form on Oregon DEQ's site.
2. **Pull WA diesel 2026 standard from WAC 173-424-610** schedule table.
3. **Pull BC diesel 2026 standard from B.C. Reg 234/2012**, Schedule 4.
4. **Propose terminology rename** in IFVS spec — "regional baseline CI" → "regional annual compliance target CI" or similar.

Each lookup is 15-30 minutes of focused work in the right document. Not urgent for the IFVS spec lock if Claude-Content is OK using the current numbers as placeholders flagged as "to-verify."

---

*Annotated in the Notion IFVS spec Decision Log as IFVS-014 (pending).*
