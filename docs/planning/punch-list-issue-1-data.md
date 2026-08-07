# Data Punch List — Issue 1
### Split by what blocks today and what does not

**Read the split first.** Tier 1 blocks publication. Tier 2 is the banked staircase piece
and can wait. Do not let Tier 2 work delay Tier 1.

**Global filter:** any pull feeding free content applies `WHERE is_proprietary = false`.
Enforce at the query, not at write time.

---
---

# TIER 1 — BLOCKS ISSUE 1

## 1.1 Freshness — run this first, before anything else

Run `get_data_freshness` across all collectors, then check:

- `gold.transformation_status`
- `gold.unresolved_discrepancies`
- `silver.verification_discrepancies`

**Report stale or failed collectors before pulling anything.** If a Tier 1 series is
stale, that changes what can publish and it needs to be known now, not at 9pm.

## 1.2 Credit stack — the In Focus core

| Table | Pull | Blocking? |
|---|---|---|
| `bronze.credit_prices` | Full recent history, all series, `is_proprietary = false` | **YES** |
| — D4 RIN | Daily/weekly, last 24 months minimum | **YES** |
| — LCFS California | Same | **YES** |
| — Oregon CFP, Washington CFS | Same | Nice to have |
| `gold.price_mark_best` | Current marks | **YES** |
| `gold.price_series_status` | Freshness by series | **YES** |

**Report explicitly:** which credit series exist, their date coverage, their last
observation date, and which are flagged proprietary. If D4 or LCFS California is
proprietary-only, say so immediately — that changes the article.

## 1.3 Feedstock prices — for the ¢/lb comparison

| Table | Pull |
|---|---|
| `silver.feedstock_prices_consolidated` | Current levels, all feedstocks, flag proprietary |
| `bronze.feedstock_prices` | Underlying |
| `bronze.feedstock_profitability` | Existing margin work |

**Deliverable:** current inter-feedstock spreads, non-proprietary sources only. The
article needs to compare a 1.33 ¢/lb credit step against real spreads. Without this the
key sentence cannot be written.

## 1.4 Feedstock CI — for the 45Z leg

| Table | Pull |
|---|---|
| `silver.lcfs_pathway_ci_summary` | CI by feedstock/pathway |
| `silver.lcfs_pathway_ci` | Detail |
| `gold.pathway_summary` | Rollup |
| `bronze.epa_pathway_detail` | EPA pathways |

**Caution to carry into the analysis:** LCFS CI is not 45Z emissions rate. Different
model, different boundary, and LCFS includes indirect land use change while 45Z now
excludes it. Use these for distribution shape and relative ordering only. **Do not convert
an LCFS CI into a 45Z staircase position.**

## 1.5 The Signal — D4 generation pace

| Table | Pull |
|---|---|
| `gold.rin_generation_summary` | D4, monthly, current year |
| `gold.d4_bbd_trend` | Full |
| `silver.rfs_volume_projections` | 2026 requirement. Note `scenario`, `snapshot_label`, `measure` |
| `gold.rin_annual_balance` | Carryover |

**Corrected 2026-08-07 — `silver.rfs_volume_projections` is the WRONG table.** It holds a
single snapshot, `mandate_projections_2020_12` (`snapshot_date` 2020-12-11, source
`Mandate Projections.xlsx`), with only Lower/Upper scenarios. There is no Set 2 figure in
it and its 2026 D4 projection (2.53–3.03 bn RINs) is already exceeded by first-half
actuals alone. Do not use it as a denominator.

**The live Set 2 volumes are in `reference.biofuel_policy_timeline`**, `policy_name =
'RFS2_RVO'`, sourced to the EPA Federal Register, covering 2025–2027. **Unit trap:** D4
and D6 are denominated in `billion_gal`; D3, D5 and TOTAL in `billion_rins`. Compare
gallons to gallons — the monthly EMTS export carries `Volume (Gal.)` alongside `RINs`.

2026 D4 RVO = **3.35 bn gal** (2025: 2.95; 2027: 3.50).

**Deliverable:** D4 generation year to date against the 2026 requirement, with share of
year elapsed. One table, one chart.

## 1.6 New table — `silver.credit_45z_value`

Does not exist. Create it. Schema is in the decision log at D30. Populate from the
staircase formula (D38) for each feedstock CI in 1.4, under all three transfer scenarios
(0.88 / 0.90 / 0.93).

**Non-negotiable implementation details:**
- `decimal.Decimal` with explicit `ROUND_HALF_UP`. Never Python's `round()`. No floats.
- Store both rounded and unrounded emissions factor.
- Native unit kg CO₂e/MMBtu is the calculation field. g/MJ is display only, one-way,
  `CONVERSION = Decimal("1.05505585262")`.
- Applicable amount $1.09 (PWA) / $0.22 (non-PWA), 2026, per IRB 2026-29.

---
---

# TIER 2 — DOES NOT BLOCK ISSUE 1

## 2.1 GREET runner

Separate handoff: `code-handoff-greet-runner.md`. Multi-day build, five phases with
checkpoints. **Start phase 1 today** so it downloads and hashes in the background, then
leave it.

## 2.2 New table — `silver.greet_runs`

Schema in the GREET handoff. Blocked on the runner.

## 2.3 Archetype validation

Against `gold.renewable_fuel_plants`, `gold.facility_capacity`, CARB and EPA pathway
tables. Count facilities matching each archetype on region, scale band, feedstock mix.
Flag any archetype with fewer than three matches. **Output stays internal.**

## 2.4 Import-haircut piece

Full pull list already written: `feedstock-report-data-pull-list.md`, section Pull A.
Banked for a later issue.

## 2.5a Manual values render as collected data — governance hole

`reports.feedstock_price_dashboard_snapshot.is_manual_entry` and the same column on
`feedstock_credit_stack_snapshot` are written by `snapshot.load_manual_csv`, but
`render.py` references `is_manual_entry` **zero times**. A hand-entered number therefore
renders identically to a collector-sourced one, with a whitelisted citation beside it,
permanently and invisibly.

Found 2026-08-07 while deciding whether to hand-key credit prices into Issue 0. Not
today's problem — Issue 0 ships with no manual values — but it is a standing hole: the
next person who keys a number in has no marker on the output.

**Fix:** render manual entries with a visible marker on the same apparatus as the
carried-row dagger (a second symbol plus a legend line), and make the gate refuse a
manual value whose source is not on the citation whitelist. Decide whether manual values
are permitted in free-mode output at all.

## 2.5b Series-history integrity behind the "52-wk range" column

The dashboard prints a column headed **52-wk range** computed as min/max over whatever
prints exist in the trailing 52 weeks, with no coverage floor. Soybean Oil currently has
11 prints spanning 10 weeks, so its range (69.86–78.54) is labelled as something it is
not. Root cause is a **region rename**, not missing data: AMS 3511 soybean oil ran under
region `minneapolis` from 2025-02-17 to 2026-05-18, then switched to
`minnesota`/`iowa`/`indiana_ohio`/`illinois` from 2026-05-18. The dashboard points at
`illinois`, which only exists after the rename.

**Guard SHIPPED 2026-08-07** (`RANGE_MIN_WEEKS = 40`): any series whose history spans
fewer than 40 weeks now renders "—" instead of a mislabelled range, and logs a warning
naming the span and print count. This is the generalizable half — it fires for *any*
renamed or newly-added series, not just soybean oil, so the next silent truncation
cannot print a false 52-week label.

**Still open:** bridge the pre-rename region to its successor so soybean oil has a
genuine 52 weeks again (`minneapolis` 2025-02-17→2026-05-18 + `minnesota` 2026-05-18→).
Until then SBO simply has no range column. Deliberately not done on ship day — it is a
data-lineage change and deserves its own verification.

Same failure class as the cadence collapse: a header asserting something the data does
not support.

## 2.5 Forecast tournament infrastructure

`silver.monthly_expectation` has no author column and cannot hold three competing entries
per cell. Either add `forecaster` or route everything through `record_forecast` and
`gold.forecast_vs_actual`. Small, not urgent.

---
---

# What to report back, in this order

1. Freshness results. Anything stale or failed.
2. Whether D4 and LCFS California exist non-proprietary, with coverage dates.
3. Current inter-feedstock spreads.
4. Distinct values in `rfs_volume_projections` and which is the live Set 2 figure.
5. Whether anything in Tier 1 is missing entirely rather than just stale.

Item 5 is the one that changes the plan. Report it first if it happens.
