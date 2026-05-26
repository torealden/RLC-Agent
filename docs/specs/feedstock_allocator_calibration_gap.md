# Feedstock Allocator — Calibration Gap (2026-05-26)

After completing historical ingest + running allocator across Jan 2010 –
Sep 2025, found a systematic ~40% under-allocation vs EIA reported totals.

## The numbers

For 2024 specifically:

| Metric | Allocator | EIA reported | Gap |
|---|---|---|---|
| Total feedstock (mil lbs) | 23,376 | ~40,000* | -42% |
| Total biofuel gal (mil) | 2,980 | 4,865 (BD+RD) | -39% |
| SBO feedstock | 7,139 | 13,236 | -46% |
| Tallow (EBFT+IBFT) | 5,226 | 7,165 | -27% |
| Corn Oil | 1,252 | 4,330 | -71% |

*EIA reported feedstock sum is approximate — adds the major reported
categories from `bronze.eia_feedstock_monthly` (plant_type='total').

## Root cause

`src/engines/feedstock_allocation/allocator.py::_estimate_supply` uses
hard-coded national supply totals that are now too low for the 2023+
era of rapid RD growth:

```python
annual_supply_mil_lbs = {
    'SBO':  12000,   # ~24B lbs US production, ~50% to biofuel
    'CO':    1500,   # Canola oil
    'DCO':   4000,   # Distillers corn oil
    'EBFT':  1400,
    'IBFT':  2600,
    'CWG':   1500,
    'PF':    1200,
    'YG':    2500,
    'UCO':   3000,
}
# Total = ~30,000 mil lbs/year supply cap
# After 10% buffer: ~27,000 mil lbs/year usable
```

EIA-reported demand in 2024 is ~40,000 mil lbs/year, so the cap binds and
the allocator stops allocating once supply is exhausted. This is *exactly*
what we'd expect from a 60% allocation rate.

## Why it didn't blow up sooner

The allocator had only ever run for Jan 2025 (142 records) before this
session. Single-month spot runs don't surface systematic bias because
nobody compared the totals against EIA monthly actuals.

## Fix options

**Option A — Calibrate `_estimate_supply` from EIA actuals.** Replace
hard-coded annual totals with monthly EIA Form 819 values (now that
we have 237 months of history in bronze). This is the cleanest path
and turns the historical data into a load-bearing calibration source.

**Option B — Populate `silver.feedstock_supply` directly.** The allocator
already prefers `silver.feedstock_supply` over the estimate fallback.
Build a pipeline that reads `bronze.eia_feedstock_monthly` + applies
PADD-level distribution, writes to silver. Cleaner separation of
concerns vs Option A.

**Option C — Let the allocator over-allocate beyond supply.** Less
defensible — supply caps exist for a reason (don't allocate more
yellow grease than physically exists). Don't do this.

## Recommended path

Option B. Build `silver.feedstock_supply` populator that takes EIA
totals and distributes to PADDs by facility-capacity-weighted shares.
This integrates the historical ingest work directly into the calibration
of forward allocations.

Estimated effort: 2-3 hours including verification.

## Other findings from this session

- Allocator's tallow guardrail was extended to read from 3 eras (Form
  819 2022+, EIA old report 2012-2021, USDA F&O 2006-2011). Pre-2022
  reads are biodiesel-only, but RD tallow was negligible until ~2020
  so this is a reasonable proxy.
- Allocator does NOT use the EIA plant_type splits for SBO / Canola Oil
  / Corn Oil as guardrails — only for tallow. June 2025 SBO data is
  nearly 50/50 BD/RD, so this is real divergence risk in the BD-vs-RD
  feedstock split. Worth extending the tallow guardrail pattern to
  the major vegetable oils after the demo.
- 2022 has 10/12 months of Form 819 data (Nov-Dec 2022 missing). No
  Wayback snapshots exist for early 2023 to fill the gap.
- 2021-2022 vegetable oil BD/RD splits are genuinely unavailable —
  Form 819 didn't introduce table_2c plant_type splits until 2023.

## Files touched this session

- `src/tools/eia_biofuels_collector.py` — db_config + Tallow normalization
- `src/agents/collectors/us/eia_biofuels_form819_collector.py` — new
- `src/dispatcher/collector_registry.py` — registered
- `src/schedulers/master_scheduler.py` — scheduled monthly
- `src/engines/feedstock_allocation/allocator.py` — 3-era tallow guardrail
- `scripts/backfill_eia_form819.py` — new (local + Wayback)
- `scripts/ingest_usda_fo_biodiesel_history.py` — new (2006-2011 from xlsm)
- `scripts/write_allocation_to_eia_data.py` — skip-if-missing fix
- `database/schemas/037_tallow_grade_split.sql` — applied to RDS

## Bronze coverage achieved

- 237 months: Jan 2006 → Sep 2025
- 3,274 rows
- 25 feedstock types
- Source split: 464 rows USDA F&O (2006-2011) + 1,600 rows EIA old report
  (2012-2020) + 1,210 rows Form 819 (2021+)
