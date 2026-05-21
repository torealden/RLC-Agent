# US Renewable Diesel Production — July 2026 Forecast

**Forecast ID:** `rd_us_monthly_2026-07_v1`
**Made on:** 2026-05-21
**Target:** July 2026 (single month)
**Series:** US Biofuels Plant Net Production of Renewable Diesel
**Source:** EIA Monthly Biofuels (table `bronze.eia_monthly_biofuels`, `fuel_type='renewable_diesel'`, `attribute='production'`)

---

## Point forecast

| Field | Value |
|---|---|
| **Central** | **5,800 MBBL** (thousand barrels) |
| Lower confidence | 5,500 MBBL |
| Upper confidence | 6,100 MBBL |
| Width | ±300 MBBL (~5% either side) |

In gallon-equivalent: 5,800 MBBL × 42 gal/bbl = **243.6 million gallons**.

## Anchors

| Observation | Value | Source |
|---|---|---|
| July 2025 actual | 6,224 MBBL | EIA |
| Trailing-12 mean (Feb 2025 – Jan 2026) | 5,675 MBBL | EIA |
| Trailing-6 mean (Aug 2025 – Jan 2026) | 5,562 MBBL | EIA |
| YoY trend last 6 months | -7.1% (range -1.2% to -14.7%) | EIA |
| Most recent print (Jan 2026) | 4,682 MBBL (-9.8% YoY) | EIA |

## Reasoning

The headline takeaway from the data: US RD production has been running **5–12% below year-ago** for six consecutive months. The peak monthly print of 6,889 MBBL (Dec 2024) hasn't been touched in 2025 or early 2026. The narrative of "RD capacity keeps growing so production keeps growing" stopped being correct sometime in mid-2024 — the industry hit a soft ceiling around 6,500 MBBL/month.

For July 2026, three forces compete:

1. **Headwind: idle capacity.** Chevron REG idled Ralston, IA and Madison, WI in March 2024 (50 mmgy = ~115 MBBL/mo offline). The 10-K confirmed both, the CARB pathway database has no active LCFS pathway for either — closure is real, not a paper idle. See `project_reg_ralston_madison_idle.md`.

2. **Headwind: feedstock cost pressure.** UCO at ~42¢/lb and SBO at ~48¢/lb are near year-highs. At 7.7 lb/gal yield, that's $3.25/gal feedstock cost on SBO — leaving only $2-3/gal for OPEX, capital recovery, and margin out of a ~$5.50-6.50/gal eff_sell price stack. Marginal facilities operate near breakeven.

3. **Tailwind: 45Z still active.** Under the `extension_2031` scenario the 45Z PTC continues to subsidize low-CI fuel at ~$0.60-1.00/gal for waste-oil pathways. That's the only thing keeping output where it is. If the policy lapses (the `expiry_2027` scenario in the IFV kg_callable), Q3 2026 production drops 15-25%.

**Methodology:**

- Take July 2025 baseline = 6,224 MBBL.
- Apply trailing-6 YoY trend of -7%: `6,224 × 0.93 = 5,788 MBBL` central.
- The -7% already reflects the Chevron REG idle (it occurred in March 2024, baked into the YoY shift), so no further subtraction needed.
- Round to **5,800 MBBL central**.
- Range: ±5% to absorb (a) D4 RIN volatility, (b) feedstock price swings, (c) unplanned refinery turnaround at any of the four largest operators.

## What would shift this

| Trigger | Direction | Magnitude |
|---|---|---|
| 45Z policy clarification (uncertainty resolved positively) | + | +200-400 MBBL |
| D4 RIN drops below $1.20 | − | −300-500 MBBL |
| SBO spikes above $55¢/lb | − | −150-300 MBBL |
| New idle announcement at DGD / Marathon / Valero / Phillips 66 | − | −200-600 MBBL depending on size |
| UCO supply tightens (e.g., import restriction enforcement) | − | −100-200 MBBL |
| BBD blend mandate increase announcement | + | +200-400 MBBL |

## How this gets graded

The actual July 2026 print will appear in `bronze.eia_monthly_biofuels` around **mid-September 2026** (EIA monthly is typically a 7-week lag). At that point:

1. Pull actual via:
   ```sql
   SELECT value FROM bronze.eia_monthly_biofuels
   WHERE fuel_type='renewable_diesel' AND attribute='production'
     AND period_month = '2026-07-01';
   ```
2. Compare against forecast row `forecast_id='rd_us_monthly_2026-07_v1'` in `core.forecasts`.
3. Record reconciliation in `core.forecasts` source `eia_release_actual` (mirroring the soybean crush 2026-05-01 reconciliation already in the table).

## Re-evaluation triggers (before July)

- **2026-06-13 (next WASDE):** does USDA's WASDE include any RD-relevant feedstock supply revision?
- **2026-06-15 (next EIA monthly biofuels release):** May 2026 actual lands — calibrate the May print against my implicit ~5,750 MBBL working assumption.
- **Mid-June OPIS print:** D4 RIN settle — if it has dropped below $1.20 or risen above $1.65 from current ~$1.45, revise.
- **Any major operator announcement:** turnaround schedule, idle extension, capacity expansion.

If any of those triggers fires, write a `_v2` revision and link it to `_v1`.

---

*Forecast methodology: anchored to 12-month rolling baseline + YoY trend + capacity adjustment + bounded by feedstock-economics realism. This is a "human-style" forecast — the IFV kg_callable can be invoked separately for a price-stack-driven sensitivity grid.*

*Persisted via `python scripts/write_forecast_july_2026_rd.py`.*
