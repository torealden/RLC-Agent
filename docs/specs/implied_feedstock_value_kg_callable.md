# Implied Feedstock Value — kg_callable Spec (v3 — HOBO-anchored, layered with crush_economics)

> **Purpose**: codify the signature analytical model from section 05 of the BBD weekly report as an executable kg_callable so it runs every week with auditable inputs and the same logic.
>
> **Anchored to the HOBO Renewables price stack** (KG node `rd_price_stack`, context `price_stack_decomposition`). The structural form, default values, and sensitivity calibration come from the HOBO Section 8 framework.

## Architectural layering (per Iowa Crush Agent spec review 2026-04-26)

```
┌──────────────────────────────────────────────────────────────┐
│  src/agents/facility/crush_economics.py   ← THE MATH (pure)  │
│    - implied_feedstock_value_marginal()                      │
│    - implied_feedstock_value_full_cost()                     │
│    - compute_crush_margin_per_bu()                           │
│    - days_of_coverage()                                      │
│    - CrushParams, MarketSnapshot, FixedCostsPerBushel,       │
│      ImpliedValueBreakdown                                   │
│    - SOYBEAN_DEFAULT_PARAMS (KG-validated)                   │
└──────────┬─────────────────────────────────┬────────────────┘
           │                                 │
           ▼                                 ▼
┌─────────────────────────────┐  ┌────────────────────────────────────┐
│ buyer_agent.py (daily)      │  │ kg/callables/implied_feedstock_    │
│  imports crush_economics    │  │   value.py  (THIS SPEC)            │
│  reads facility XLSX +      │  │  imports crush_economics           │
│   facility_state DB         │  │  resolves inputs from DB+KG        │
│  calls run() once/day       │  │  adds policy_scenario branching    │
└─────────────────────────────┘  │  adds W/W deltas                   │
                                 │  adds cash_compare mode            │
                                 │  adds scenario_grid mode           │
                                 │  writes to core.forecasts          │
                                 └────────────────────────────────────┘
```

**Single source of truth**: `crush_economics.py` is the pure mathematical core, fully unit-tested (`tests/agents/facility/test_crush_economics.py`, 12 tests passing). Both the daily buyer agent and this kg_callable wrapper consume it. Changes to the math go in one place; both consumers pick them up.

**Status**: spec ready; core math implemented and tested 2026-04-27.
**Target file**: `src/kg/callables/implied_feedstock_value.py` (the wrapper — not yet implemented)
**KG anchor nodes**: `rd_price_stack`, `saf_price_stack`, `feedstock_sensitivity_rule`, `cfpc_45z` (4 scenarios), `base_case_margins` (best/base/worst), `hefa_opex_structure`, `bbd_margin_model`, `ci_value_framework`, `lcfs_credit_framework`, `crushing_plant_yield_matrix`, `crushing_plant_opex_structure`

---

## 1. The Model — HOBO Price Stack

For a given **(refined fuel, region, feedstock, as_of_date)** the model computes effective selling price by adding the four-component HOBO stack, then backs into a per-lb feedstock bid by subtracting non-feedstock costs and dividing by yield.

### 1.1 Effective Selling Price (per gal of refined fuel)

```
effective_selling_price_per_gal =
    base_refined_product_price        # ULSD for RD/BD; jet for SAF
  + d4_rin_value                      # = d4_rin_price × equivalence_ratio (RD=1.7, BD=1.5, SAF=1.7)
  + lcfs_value                        # = lcfs_credit_$/MT × pathway_ci_advantage_g/MJ × conversion_factor
  + cfpc_45z_value                    # CI-dependent, varies with policy_scenario (§1.4)
```

**Calibration anchors from KG node `rd_price_stack` (California 2025 illustrative):**

| Component | Default value | Source / range |
|---|---|---|
| Base ULSD | $2.50/gal | EIA spot or OPIS |
| D4 RIN value | $2.55/gal | 1.7 × $1.50/RIN |
| LCFS value | $0.50–0.75/gal | $60–70/MT credit, 55 g/MJ CI advantage |
| 45Z value | $0.65–1.00/gal | CI-dependent — see §1.4 |
| **Effective selling price** | **$5.50–6.50/gal** | sum of above |

### 1.2 Production Cost (per gal)

```
production_cost_per_gal =
    feedstock_cost                    # = feedstock_price_$/lb × yield_lb_per_gal
  + opex_per_gal                      # variable: H2, methanol, utilities, labor
  + fixed_cost_per_gal                # capex amortization + insurance + property
```

**HOBO calibration** (`feedstock_sensitivity_rule.feedstock_cost_is_everything`):
- Feedstock = **70–80% of cash cost**
- $0.05/lb feedstock change → $0.35–0.40/gal margin change
- 1¢/lb feedstock = $0.08/gal margin
- Implies HEFA yield ≈ **7.5–8.0 lb feedstock per gal RD** (use 7.7 default)
- OPEX defaults: BD ~$0.40, RD ~$0.30–0.50, SAF ~$1.10/gal
- Fixed: $0.10/gal default until plant-specific data available

### 1.3 Implied Feedstock Bid

```
implied_bid_per_lb_feedstock =
    [ effective_selling_price_per_gal
      − opex_per_gal
      − fixed_cost_per_gal
      − target_gross_margin_per_gal ]
    ÷ yield_lb_per_gal
```

This is the **breakeven** bid (when target_margin = 0) or the **practical bid** (when a target margin is enforced).

### 1.4 45Z Policy Scenario (first-class input)

Per KG node `cfpc_45z`, four scenarios materially reshape the stack. The model takes a `policy_scenario` argument:

| Scenario | 45Z handling | When to use |
|---|---|---|
| `extension_2031` (bullish) | Full credit through 2031, ILUC remains | Base case for 2026 reports |
| `expiry_2027` (bearish, SAF cliff) | Credit set to 0 for as_of_date > 2027-12-31 | Stress test post-cliff |
| `iluc_removed` | Recompute pathway CI without ILUC; narrows waste-vs-crop differential | Scenario test for soybean oil competitiveness |
| `domestic_restriction` | Apply CI penalty (or set to 0) for non-domestic feedstock pathways | Favors Midwest US plants like HOBO |
| `none` | 45Z = 0 in cost stack | Pre-IRA baseline / counterfactual |

The `breakdown_per_gal` output (§3) **always returns 45Z value separately** so subscribers see the policy sensitivity at a glance.

### 1.5 Mode Variants

| Mode | Behavior |
|---|---|
| `breakeven` | target_margin = 0; returns ceiling bid |
| `target_margin` | target_margin = user-supplied (e.g., HOBO base case $0.50/gal IL) |
| `cash_compare` | Computes breakeven AND compares to observed cash; flags margin opportunity vs compression |
| `scenario_grid` | Runs all 4 policy scenarios + best/base/worst margin cases (per `base_case_margins`) — returns a 4×3 grid |

---

## 2. Inputs

### 2.1 Required parameters (function signature)

```python
def run(
    fuel: str,                  # 'biodiesel' | 'renewable_diesel' | 'saf'
    region: str,                # 'gulf' | 'midwest' | 'west_coast' | 'pnw' | 'rocky_mtn'
    feedstock_code: str,        # canonical from silver.bbd_feedstock_dim
    as_of_date: date,           # the trading day to compute for
    mode: str = 'breakeven',    # 'breakeven' | 'target_margin' | 'cash_compare' | 'scenario_grid'
    policy_scenario: str = 'extension_2031',  # see §1.4 (default: HOBO bullish base)
    target_margin_per_gal: float = 0.0,       # used when mode='target_margin'
    observed_cash_per_lb: float | None = None, # used when mode='cash_compare'
) -> dict
```

### 2.2 Data inputs (auto-resolved from DB; failures fall back to KG defaults)

| Input | Source | Fallback |
|---|---|---|
| `refined_product_price_per_gal` | OPIS/Argus subscription if available; else `silver.eia_spot_prices_daily` for ULSD as proxy | KG node `rd_price_stack` historical avg |
| `d4_rin_price_per_rin` | OPIS subscription; or compute from EMTS-implied | KG node `rin_oversupply_model` |
| `equiv_value_d4` (RINs per gal) | constant by fuel: BD=1.5, RD=1.7, SAF=1.7 | n/a |
| `lcfs_credit_price_per_mt` | CARB weekly average (publicly reported) | KG node `lcfs_credit_framework` |
| `lcfs_pathway_ci_score` | CARB pathway lookup keyed by (fuel, feedstock_code, region) | KG node `ci_value_framework` |
| `lcfs_baseline_ci` | constant (= 95.61 g/MJ for diesel substitute, 89.0 for jet) | n/a |
| `cfpc_45z_value_per_gal` | derived from CI score per IRS guidance — needs 45Z lookup table | KG node `cfpc_45z` |
| `yield_gal_per_lb_feedstock` | `silver.feedstock_yield_ref` table (TBD) | KG conventions: SBO 7.7 lb/gal BD; UCO 7.5; tallow 7.7 lb/gal RD via HEFA |
| `variable_cost_per_gal` | `silver.bbd_production_cost_ref` table (TBD) | KG node `bbd_margin_model` defaults: BD ~$0.40, RD ~$0.55, SAF ~$1.10 |
| `fixed_cost_per_gal` | facility-specific from `core.facilities` capacity & capex | conservative $0.10/gal default |

**Data plumbing required**:
- `silver.feedstock_yield_ref` — small reference table, ~30 rows ((fuel, feedstock) → yield). I'll seed from KG conventions.
- `silver.bbd_production_cost_ref` — variable + fixed cost benchmarks by fuel × technology. Initial seed from KG; updated as we get plant-specific data.
- `silver.lcfs_pathway_ci_ref` — CARB pathway CI scores by (fuel, feedstock, region). Manually seeded; updated when CARB publishes amendments.

### 2.3 KG fallback chain

If a DB input is missing, the callable looks up the corresponding KG node via `src.kg.callable_invoker.lookup_kg_default()` and uses the historical mean from `kg_context`. Every fallback is logged in `warnings[]` so the analyst knows which inputs came from defaults vs. live data.

---

## 3. Output

```python
{
  'implied_bid_per_lb': 0.4521,           # headline number, $/lb feedstock
  'implied_bid_per_short_ton': 904.20,
  'yield_lb_per_gal': 7.7,                # HOBO default for HEFA RD; varies by fuel/feedstock
  'policy_scenario': 'extension_2031',    # which 45Z branch was used
  'breakdown_per_gal': {
      # The HOBO four-component price stack — supply side
      'base_refined_product':    2.50,    # ULSD or jet
      'd4_rin_value':            2.55,    # = $1.50 × 1.7 (equiv ratio)
      'lcfs_value':              0.62,    # = $65/MT × 55 g/MJ × conversion
      'cfpc_45z_value':          0.85,    # CI-dependent, varies by scenario
      'effective_selling_price': 6.52,    # sum of the above
      # Cost side
      'opex':                   -0.40,    # variable OPEX (HEFA default)
      'fixed_cost':             -0.10,
      'target_margin':           0.00,    # nonzero only when mode='target_margin'
      'net_available_for_feedstock': 6.02, # (effective_selling - opex - fixed - target_margin)
  },
  # Built-in HOBO scenario context for the report
  'margin_case': 'base',                  # 'best' | 'base' | 'worst' (per base_case_margins KG node)
  'inputs_used': {...},                   # echoed for audit
  'fallback_inputs': [...],               # which inputs came from KG default vs live DB
  'wow_delta': {                          # if as_of_date − 7d data available
      'implied_bid_per_lb':              +0.018,
      'd4_rin_value_per_gal':            +0.025,
      'lcfs_value_per_gal':              -0.011,
      'cfpc_45z_value_per_gal':           0.000,
      'base_refined_product_per_gal':    +0.050,
      'feedstock_basis_change_per_lb':   -0.003,
  },
  'cash_compare': {                       # populated only when mode='cash_compare'
      'observed_per_lb':            0.4400,
      'implied_minus_observed':     +0.0121,
      'interpretation': 'Implied bid 1.2 c/lb above observed — margin opportunity vs spot',
  },
  'scenario_grid': {                      # populated only when mode='scenario_grid'
      # 4 policy scenarios × 3 margin cases (HOBO best/base/worst)
      'extension_2031':       {'best': ..., 'base': ..., 'worst': ...},
      'expiry_2027':          {'best': ..., 'base': ..., 'worst': ...},
      'iluc_removed':         {'best': ..., 'base': ..., 'worst': ...},
      'domestic_restriction': {'best': ..., 'base': ..., 'worst': ...},
  },
  'reasoning':  '<HOBO-style narrative: "RD GoM base case at $X.XX/gal under extension_2031;
                  D4 contributes 38% of revenue; feedstock-cost-is-everything implies $Y.YY/lb
                  ceiling, observed UCO at $Z.ZZ → margin opportunity / compression of N c/gal">',
  'confidence': 0.78,
  'warnings':   [...],
}
```

The `reasoning` string is composed automatically from the breakdown — for the weekly report it's pasted directly into section 05 with W/W deltas highlighted.

---

## 4. Self-Exploration (`self_explore()`)

Same pattern as `weather_yield.py`. Sweeps:

- **RIN sweep**: D4 from $0.50 to $2.00 in 0.10 steps → implied bid sensitivity
- **LCFS sweep**: $30 to $200/MT in $10 steps
- **45Z scenario grid**: extension / sunset / reduced multiplier
- **Feedstock CI sweep**: ±10 g/MJ around current pathway score (matters for WC RD economics)
- **Variable cost sweep**: ±$0.10/gal

Returns sensitivities (e.g., "+1c/RIN = +1.5c/lb feedstock bid for SBO Gulf BD") and breakpoints (e.g., "BD economics break below $0.40 D4 if SBO is at $0.55/lb").

---

## 5. Forecast-book integration

When invoked from the weekly forecast loop (`src.kg.forecast_book`), the callable writes:

- One row per `(fuel, region, feedstock, as_of_date, mode)` to `core.forecasts`
- The full breakdown to `core.forecast_components` (so we can backtest each component independently)
- W/W deltas computed automatically

Backtest hook: every Friday the prior week's `cash_compare` calls are evaluated against the now-observed cash market. Hit/miss logged to `reports.calls_register` automatically.

---

## 6. Edge cases & guards

- **Missing CI pathway**: fall back to `feedstock_category` average; downgrade confidence by 0.2.
- **Negative implied bid**: sanity flag — possible if RIN+LCFS collapse below variable cost. Returned with a `warnings[]` entry; not silently zero.
- **45Z uncertainty**: when `--mode=conservative`, set 45Z value to 0 (treats credit as not bankable). Useful for stress testing post-2027 sunset scenarios.
- **Region/feedstock combos that don't exist** (e.g., palm oil West Coast RD — CARB doesn't allow): explicit pre-check, return `{'error': 'pathway_not_certified'}` rather than computing a meaningless number.
- **Stale subscription data**: if OPIS/Argus feed > 5 days old, prefer EIA proxy and flag.

---

## 7. Implementation sequence (after spec approval)

1. **Reference tables** — create `silver.feedstock_yield_ref`, `silver.bbd_production_cost_ref`, `silver.lcfs_pathway_ci_ref`. Seed from KG nodes. (~1 day)
2. **Core `run()`** — mathematical heart, no DB calls; takes all inputs as args. Pure function, fully unit-testable. (~half day)
3. **DB resolver** — `_resolve_inputs()` helper that pulls each input from its source view/table with KG fallback. (~half day)
4. **`self_explore()`** — sweep machinery. (~half day)
5. **Forecast-book wiring** — register in `src/kg/callable_invoker.py`; add to weekly forecast loop. (~half day)
6. **Backtest harness** — replay last 12 months of cash data through the model, log to `reports.calls_register`. (~1 day)
7. **Section 05 chart** — single matplotlib plot of implied vs cash with W/W deltas marked. (~half day)

**Total: ~5 days of focused work** once inputs/spec are locked.

---

## 8. Open questions for review

1. **Cost reference scope** — one variable-cost benchmark per (fuel, technology), or per-region? My instinct: per (fuel, technology) is enough for MVP; regional differentiation is mostly logistics + utilities.
2. ~~45Z handling~~ — **resolved by HOBO anchor**: 4 first-class policy scenarios (extension_2031 / expiry_2027 / iluc_removed / domestic_restriction). Default to `extension_2031` (HOBO bullish base); always emit 45Z value as a separate breakdown line for clarity.
3. **CI scoring source** — CARB pathway database has gaps for newer feedstocks (UCO from China). Use CARB defaults, or our own CI estimate where CARB has no certified pathway? The latter is editorially riskier but probably what subscribers want. *My recommendation: own estimate, with confidence flag and "CARB-pending" annotation.*
4. **Region scope for MVP** — Gulf BD, Gulf RD, Midwest BD, West Coast RD covers ~80% of the action. Add SAF West Coast and Midwest RD in v2? *My recommendation: include SAF West Coast in MVP — HOBO's signature deliverable depends on it.*
5. **Cash spread normalization** — implied bid in $/lb vs cash in $/lb is straightforward, but BD futures are quoted in $/gal at notional 7.5 lb/gal. Output both $/lb and $/gal? *My recommendation: yes, both — cheap addition.*
6. **Margin case default** — `mode=scenario_grid` returns the 4×3 grid (4 policy × best/base/worst margin cases per HOBO `base_case_margins`). For the standard weekly run, do you want `base` as the headline number with the grid in a sidebar, or something else?
7. **Feedstock-specific OPEX** — HOBO defaults assume soft soybean-oil-grade processing. Tougher feedstocks (high FFA tallow, contaminated UCO) raise OPEX 5-15 c/gal. Do you want a feedstock_quality_factor multiplier on OPEX, or hold OPEX constant in MVP? *My recommendation: hold constant in MVP; add quality factor in v2 once we have HOBO actuals.*

---

**Once you approve, the implementation order in §7 is straightforward — none of it requires new collectors. Everything plumbs from data we already have (or KG defaults).**
