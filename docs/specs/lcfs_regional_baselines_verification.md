# LCFS-style regional baseline CI — verification notes

*Task #65. Original research 2026-05-22; verified against primary sources 2026-05-25.*

The IFVS spec (notion.so/365ead023dee813daee1e31b22219327, §5.2) lists 2026
baseline CI values for the four LCFS-style regional programs. **Three of the
four spec values are wrong** — they don't match either the reference
baseline or the annual compliance target as published.

## TL;DR

| Region | IFVS spec value | Verified 2026 annual target | Reference baseline | Status |
|---|---:|---:|---:|---|
| California (LCFS) | **88.62** | **88.62** ✅ | ~95 (pre-decline) | Spec value matches |
| Oregon (CFP) | **94.32** | **86.89** | 104.92 (OR-GREET 4.0) | Spec value WRONG — 7.4 g/MJ off |
| Washington (CFS) | **91.45** | **93.10** | 100.11 (2017) | Spec value WRONG — 1.6 g/MJ off |
| British Columbia (LCFS) | **88.42** | ~79.6 (computed: 100.21 × (1−0.206)) | 100.21 | Spec value WRONG — 8.8 g/MJ off |

For credit math (`(baseline − fuel_CI) × MJ/gal × credit_price`), the
*annual compliance target* is the right number, not the reference baseline.
That's the CARB convention and what the verified columns above use.

## Primary sources

| Region | Source | Authority |
|---|---|---|
| Oregon | OAR 340-253-8010 Table 2 + DEQ Clean Fuels Forecast 2026 | Oregon DEQ |
| Washington | WAC 173-424-900 Table 1 | Washington Department of Ecology |
| BC | Low Carbon Fuels Act (Regulation 295/2023) + 2026-04-29 RLCF-012 bulletin | BC Ministry of Energy |
| California | LCFS regulation §95484 declining schedule | CARB |

## Annual diesel-pool standards

### Oregon CFP

OR-GREET 4.0 raised the reference baseline from 101.74 to **104.92 gCO2e/MJ**
effective 2026. The annual standards (% reduction from that baseline):

| Year | % reduction | Standard (gCO2e/MJ) |
|---:|---:|---:|
| 2025 | 15.1% | 88.87 |
| **2026** | **17.2%** | **86.89** |
| 2027 | 19.1% | 84.92 |
| 2028 | 21.0% | 82.94 |
| 2029 | 22.8% | 80.97 |
| 2030 | 24.7% | 78.99 |
| 2035 | 38.3% | 64.72 |

### Washington CFS

Baseline (2017 reference): **100.11 gCO2e/MJ**.

| Year | % reduction | Standard (gCO2e/MJ) |
|---:|---:|---:|
| 2025 | 5.0% | 95.10 |
| **2026** | **7.0%** | **93.10** |
| 2027 | 9.0% | 91.10 |
| 2028 | 11.0% | 89.10 |
| 2029 | 13.0% | 87.10 |
| 2030 | 15.0% | 85.09 |
| 2035 | 20.0% | 80.09 |

### British Columbia LCFS

Baseline diesel-class CI: **100.21 gCO2e/MJ** (per BC LCFA technical
regulation). The 2026-04-29 RLCF-012 update revised the schedule to reach
30% reduction by 2030 (was 20% in the prior version).

| Year | % reduction | Standard (gCO2e/MJ, computed) |
|---:|---:|---:|
| 2024 | 16.0% | 84.18 |
| 2025 | 18.3% | 81.87 |
| **2026** | **20.6%** | **79.57** |
| 2027 | 23.0% | 77.16 |
| 2028 | 25.3% | 74.86 |
| 2029 | 27.7% | 72.45 |
| 2030 | 30.0% | 70.15 |

### California LCFS

CARB declining schedule. 2026 diesel-pool annual target: **88.62 gCO2e/MJ**.
Spec value matches.

## What changed in the IFVS spec

The IFVS hefa_economics calibration uses:

```python
LCFS_CI_TARGETS_2026 = {
    'CA': 88.62,   # OK
    'OR': 94.32,   # should be 86.89
    'WA': 91.45,   # should be 93.10
    'BC': 88.42,   # should be 79.57
}
```

Net IFV impact of corrected values, soybean oil-derived RD at HEFA fuel CI
~ 50 gCO2e/MJ, $0.20/credit (illustrative):

- **CA:** unchanged
- **OR:** widens the credit window — fuel beats target by (86.89 − 50) =
  36.89 g/MJ instead of (94.32 − 50) = 44.32 g/MJ. So the **OR credit per
  MJ goes DOWN** by ~17%. Implied bid for OR-routed gallons falls.
- **WA:** narrows by 1.6 g/MJ — slightly more credit value than spec
  assumed. Modest positive for WA.
- **BC:** dramatic widening of the gap — fuel beats target by 29.6 g/MJ
  instead of 38.4 g/MJ. **BC credit per MJ down ~23%.** Material for any
  BC-routed forecasts.

## Recommendation for the spec lock

1. Update `LCFS_CI_TARGETS_2026` in `src/agents/facility/hefa_economics.py`
   to the verified values: CA 88.62, OR 86.89, WA 93.10, BC 79.57.
2. Rename the constant from `*_BASELINES_*` to `*_TARGETS_*` to remove the
   "baseline vs target" ambiguity that caused this. The annual compliance
   target is what enters credit math, not the reference baseline.
3. Re-run unit tests against the new values — likely several IFV outputs
   shift by 5-20% depending on region weighting.
4. Update the IFVS spec Decision Log entry IFVS-014 from "pending" to
   "resolved" with the verified table above.

## Sources

- [Oregon CFP standards (Stillwater 2025)](https://stillwaterpublications.com/stillwater-cfp-101/)
- [Washington WAC 173-424-900 Tables](https://app.leg.wa.gov/WAC/default.aspx?cite=173-424-900)
- [BC LCFA Technical Regulation 295/2023](https://www.bclaws.gov.bc.ca/civix/document/id/complete/statreg/295_2023)
- [BC RLCF-012 2026-04-29 Approved Carbon Intensities](https://www2.gov.bc.ca/assets/gov/farming-natural-resources-and-industry/electricity-alternative-energy/transportation/renewable-low-carbon-fuels/rlcf012_approved_carbon_intensities_current_29apr2026.pdf)
- [IETA Business Brief: BC LCFS Sept 2025](https://www.ieta.org/uploads/wp-content/2025/09/IETA_Business_Brief-BC-Low-Carbon-Fuel-Standard-September_2025.pdf)
- [IETA Business Brief: Oregon CFP Sept 2025](https://www.ieta.org/uploads/wp-content/2025/09/IETA_Business_Brief-Oregon-clean-fuel-Sept-2025Final.pdf)
- [IETA Business Brief: Washington CFS Sept 2025](https://www.ieta.org/uploads/wp-content/2025/09/IETA_Business_Brief-Washington-CFS-19SeptemberFinal.pdf)
