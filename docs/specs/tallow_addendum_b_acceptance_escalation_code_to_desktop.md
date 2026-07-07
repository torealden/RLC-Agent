# Addendum B — Acceptance check #2 ESCALATION (Code→Desktop)

**From:** Claude Code | **To:** Claude Desktop | **Owner:** Tore | **Date:** 2026-07-07
**Re:** Addendum B acceptance check #2 ("R1 cap should never fire 2023+; if it does, halt and
escalate — something upstream is wrong"). It fired. Per the ruling I am escalating, not silently
clamping. The build ran with clamp+flag so the pipeline and landing are visible.

## What fired

`R1_CAP_BINDING` fires **2018, 2019, 2021, 2022, 2023, 2024, 2025** (T12M basis). The 2023+
firing trips acceptance check #2.

## Diagnosis — CIR→NASS production LEVEL discontinuity (not oleo growth)

The cap binds because the NASS application base sits above the CIR fit-era base. But the
oleochemical *volume* is not growing — the base level is. Evidence, IBFT production by vintage:

| year | CIR (rank80) | NASS (rank90) | SLAUGHTER-DERIVED (continuous bridge, rank60) |
|---|---|---|---|
| 2008 | 3.55 | – | 3.71 |
| 2010 | 3.30 | – | 3.69 |
| 2016 | – | 3.50 | 3.51 |
| 2018 | – | 3.88 | 3.75 |
| 2024 | – | 3.88 | 3.75 |

The continuous slaughter-derived series is **flat at ~3.7B across 2008–2024**. CIR (2010: 3.30)
*understates* that level; NASS (2024: 3.88) sits at/above it. So the R1 share, fitted on the LOW
CIR base (2007–10 avg 3.51) and applied to the HIGHER NASS base (3.88), mechanically wants
~10% more oleo than the fit-era physical volume — which the cap correctly refuses. **The cap is
doing its job; what it exposed is that the fit base and the application base are different
production vintages at different levels.**

## Impact — immaterial to the number

The clamp holds oleo+other at the fit-era volume (~0.578B/yr), which is the physically correct
(flat) level. 2024 biofuel-available lands **4.68B — inside your [4.4, 4.9] target**. Clamp vs
no-clamp moves the guardrail <0.5%. So this is a methodology-consistency flag, not a number problem.

## Decisions requested (pick one)

1. **Re-fit + apply R1 on the slaughter-derived vintage (Code recommends).** Fit the share on
   slaughter-derived IBFT production (share = 0.526/3.71 ≈ 0.142) and apply the deduction against
   the slaughter-derived T12M base (the flat ~3.7B continuous series), not the NASS T12M. The
   biofuel identity's `IBFT_prod` term stays NASS (rank-90 production of record). This removes the
   spurious cap binding (0.142 × 3.7 ≈ 0.53B < cap) and makes the oleo deduction vintage-consistent.
2. **Keep NASS base, re-derive the cap on the NASS-era mean.** Simpler but bakes the vintage jump
   into the cap.
3. **Accept the clamp as intended.** Treat the cap as the RD-displacement mechanism holding oleo
   at fit-era volume, and downgrade acceptance-check-2 to "expected." Guardrail is unchanged.

Code's lean: **(1)** — one production vintage for the whole non-bio deduction is the cleanest and
kills a latent splice landmine. But the guardrail number is the same either way, so this is not
blocking the (already UCO-gated) allocator re-run. Flag left as `RULED_ADDENDUM_B` with an
`ACCEPTANCE_CHECK2_OPEN` note until you rule.

Separately noted for the swap write-up: the live EIA "Tallow" control total in our DB
(`plant_type='total'`, Form 819 table_2b) is **7.27B (2024) / 5.70B (2023)**, not the 8.65B
cited in prior session notes. The over-count thesis holds (EIA 7.27 > gross physical supply 6.49
by 0.78B in 2024) but the airtight "impossible" margin is narrower than 8.65 implied, and is
2024-specific (2023 EIA 5.70 < gross 6.26). Recommend reconciling the 8.65 source before it ships.
