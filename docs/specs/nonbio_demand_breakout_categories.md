# Non-Biofuel Demand Breakout — Balance-Sheet Category Spec

**For:** Claude Desktop (workbook build) — the exact demand-side line items now emitted in the
feedstock supply/demand flat files, so the balance sheets can link to them.
**Status:** live in the flat files as of 2026-07-16 (`write_oils_supply_flat_files.py`,
`write_fats_supply_flat_files.py`).
**Shares source of truth:** `reference.nonbio_enduse_shares_monthly` (migration 145) — SEASONAL
calendar-month shares; the flat annual `reference.nonbio_enduse_shares` (migration 144) is the
fallback/level reference. Query either for the weights.

**Seasonal shape (2026-07-17):** components are no longer a flat percentage every month. The
month-of-year pattern is derived from the 2006-2011 Census monthlies (SBO from
`us_oilseed_crush.xlsm`, tallow from `bronze.census_cir_fats`) and applied as a seasonal index
**around the ruled annual level** — so the annual level is unchanged (e.g. SBO salad/cooking still
averages 64.2%) but it now breathes by month (salad/cooking ranges ~60.7% Aug to ~67.5% May;
baking/frying inversely, peaking summer). Each (commodity, month) still sums to 1.0, so the sheet
closes every month. Rebuild with `python scripts/build_nonbio_seasonal_shares.py`. Single-bucket
feedstocks (DCO, UCO/YG) and cottonseed have no seasonal source → flat across months.

---

## Demand side — uniform structure for every feedstock

Each commodity's `*_demand` tab now carries three groups. **All values are raw pounds (LB), monthly**
(`period_type='cal_month'`, `period='Mnn'`).

### 1. Biofuel use (from the allocator/rake — 4 separate fuel lines + total)
| series | meaning |
|---|---|
| `biofuel_use_biodiesel` | biodiesel feedstock use |
| `biofuel_use_renewable_diesel` | renewable diesel feedstock use |
| `biofuel_use_coprocessing` | co-processing feedstock use |
| `biofuel_use_saf` | sustainable aviation fuel feedstock use |
| `biofuel_use_total` | sum of the four above |

Co-processing and SAF are **two separate lines** (kept distinct per Tore's ruling). They are
present for every commodity, including the feedstocks that had only biodiesel/RD before.

### 2. Non-biofuel use — total + component breakout
- `non_biofuel_use` — the aggregate closing line (unchanged derivation; see each commodity below).
- `nonbiofuel_use_<end_use>` — the component split. **Components sum to `non_biofuel_use` exactly**
  (shares sum to 1.0), so the sheet closes on either the lump or the sum of parts — never both.

**Vintage flag on the component rows:**
- `NONBIO_MEASURED` — the split is a real 2006–2011 Census end-use measurement (held forward).
- `NONBIO_MODELED` — analog/assumption. **Present as an assumption on the sheet, not as measured.**

### 3. Closure identity
`total_supply = biofuel_use_total + non_biofuel_use + exports ± Δ ending_stocks`
Link the component lines for detail; use `non_biofuel_use` (or the component sum) as the demand leg.
Do **not** also add the individual `nonbiofuel_use_*` lines to `non_biofuel_use` — that double-counts.

---

## Per-commodity component line items

### Soybean oil (`soybean_oil`) — MEASURED (Census 2006–2011), ~95% edible / ~5% industrial
| series | share |
|---|---:|
| `nonbiofuel_use_salad_cooking_oil` | 64.2% |
| `nonbiofuel_use_baking_frying_fats` | 30.7% |
| `nonbiofuel_use_margarine` | 2.7% |
| `nonbiofuel_use_other_inedible` | 1.5% |
| `nonbiofuel_use_resins_plastics` | 0.4% |
| `nonbiofuel_use_other_edible` | 0.3% |
| `nonbiofuel_use_paint_varnish` | 0.2% |

Also emits `food_use` (NASS refined edible use) as an informational NASS line — **not** part of the
closing sum. `non_biofuel_use` derivation: NASS refined edible (disappearance).

### Canola oil (`canola_oil`) — MODELED (analog of soybean-oil edible shares)
| series | share |
|---|---:|
| `nonbiofuel_use_salad_cooking_oil` | 65.8% |
| `nonbiofuel_use_baking_frying_fats` | 31.5% |
| `nonbiofuel_use_margarine` | 2.8% |

`non_biofuel_use` derivation: residual (production + imports − biofuel − exports, clamped ≥ 0).

### Tallow (`tallow`) — MEASURED (Census inedible tallow & grease, feed-dominated)
| series | share |
|---|---:|
| `nonbiofuel_use_feed` | 73.3% |
| `nonbiofuel_use_fatty_acids` | 17.4% |
| `nonbiofuel_use_other_inedible` | 9.3% |

`non_biofuel_use` derivation: modeled (mirrors `silver.tallow_balance.non_bio_use`).

### Poultry fat / white grease / yellow grease — MODELED (analog of the tallow & grease pool)
Same three lines as tallow (`nonbiofuel_use_feed` 73.3%, `_fatty_acids` 17.4%, `_other_inedible` 9.3%).
`non_biofuel_use` derivation: disappearance (NASS processing_use − biofuel).

### DCO (`dco`) — MODELED (≈ all biofuel; residual is feed)
| series | share |
|---|---:|
| `nonbiofuel_use_feed` | 100% |

`non_biofuel_use` derivation: residual. In the current period DCO is biofuel-dominant, so the non-bio
residual is small.

### UCO/YG (`uco_yg`) — MODELED (post-consumer; residual is oleochemical/feed)
| series | share |
|---|---:|
| `nonbiofuel_use_oleochemical_feed` | 100% |

`non_biofuel_use` derivation: residual. UCO is biofuel-dominant, so non-bio residual is ~0 currently.

### Cottonseed oil (`cottonseed_oil`) — MEASURED, essentially all edible
Shares seeded in `reference.nonbio_enduse_shares` (salad/cooking 81.8%, baking/frying 17.2%,
other edible 1.0%) but **no CSO flat file is written yet** — wire this when a CSO supply/demand
workbook is added.

---

## Known coverage gap (flag on the SBO sheet)

The SBO `non_biofuel_use` total (and therefore its component split) is currently bounded to
**2025-01 .. 2026-05** because its source series, NASS `oil_refined_edible_use`, is only loaded for
those 17 months. The breakout is correct; the underlying total is short on history. Extending it
(e.g. a residual-based non-bio total for pre-2025 history) is a separate data decision, not part of
this breakout. Tallow/PF/CWG/YG/DCO/canola non-bio totals run further back (disappearance/residual).
