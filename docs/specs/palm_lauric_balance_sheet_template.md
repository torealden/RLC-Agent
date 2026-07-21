# Palm & Lauric Oils — Balance Sheet Template

**Source template:** `RLC Dropbox\RLC Team Folder\RLC-Models\Archive\Oilseeds\World Lauric Oils Balance Sheets.xlsx`
(36 sheets; the structural models are `Malaysia Palm Complex`, `Indonesia Palm Complex`,
`Philippines Copra Complex`, `Indonesia Copra Complex`).
**Documented:** 2026-07-21 · **Purpose:** the structure to replicate in `models/` for palm, palm
kernel oil, and coconut oil.

> ⚠️ **Numbers in the source are The Jacobsen's forecasts and estimates** (bold cells in the
> original). Replicate the *structure*; do not carry their values into anything client-facing —
> same private-assessment constraint as the veg-oil reference series (SOW §9).

---

## The correction this template forces

I previously specced palm in the Pepsi coverage tracker as *"a plantation crop with no seed, no
crush, and no meal"* and gave it a 4-sheet set. **That is wrong.** Palm is a full crush complex —
it just has *two* oils instead of one:

```
        Fresh Fruit Bunches (FFB)
                  │
          ┌───────┴────────┐
      mesocarp            kernel  ──── the "seed"
          │                 │
     PALM OIL (CPO)      crushed
                            │
                    ┌───────┴────────┐
              PALM KERNEL OIL   PALM KERNEL CAKE
                   (PKO)            (PKC) ── the "meal"
```

So palm carries **four** balance sheets, not one: **Palm Oil · Palm Kernel · Palm Kernel Oil ·
Palm Kernel Cake**, plus an area-and-yield block. Palm kernel maps to Seed S&D, PKO is a *second*
oil sheet with no analogue in the soy/rape/sun complexes, and PKC is the meal.

The copra/coconut complex is the same shape with three sheets: **Copra · Copra Meal · Coconut Oil**.

---

## Marketing year and units

| | |
|---|---|
| **Marketing year** | **October 1 – September 30** ("Beginning Stocks (October 1)" / "Ending Stocks (September 30)"; copra sheets say "Carryin"/"Carryout") |
| **Area units** | 1,000 hectares |
| **Volume units** | 1,000 tonnes |
| **Yield** | tonnes per hectare |
| **Price** | Malaysian futures in **Ringgit/tonne**; copra/coconut in **US$/tonne** |
| **MY label** | two-digit split-year, `93/94` … `61/62` |

---

## 1. Palm complex — the four balance sheets

### 1a. Area & Yield (1,000 hectares)
```
Immature Planted Area
Mature Planted Area
Total Planted Area
Oil Yield (Tonnes/Hectare)
```

### 1b. Palm Oil S&D (1,000 tonnes)
```
Beginning Stocks (October 1)
Production
Imports
Total Supply
Domestic Usage
    Industrial Usage            <- biodiesel/oleochemical draw lands here
    Food, Feed & Waste Usage
Exports
Total Use
Ending Stocks (September 30)
Stocks/Use
Futures Price (Ringgit/Tonne)
```
The **Industrial / Food-Feed-Waste** split inside Domestic Usage is where Indonesia's B40/B50
mandate is modeled. That is the single most important line in the palm complex for the Pepsi
guidance price — it is the mechanism by which mandate policy removes export availability.

### 1c. Palm Kernel S&D (1,000 tonnes) — the "seed"
```
Beginning Stocks (October 1)
Production
Imports
Total Supply
Crush/Domestic Usage            <- the crush line
Exports
Total Use
Ending Stocks (September 30)
Stocks/Use
```

### 1d. Palm Kernel Cake S&D (1,000 tonnes) — the "meal"
```
Beginning Stocks (October 1)
Production
Imports
Total Supply
Domestic Usage
Exports
Total Use
Ending Stocks (September 30)
Stocks/Use
```

### 1e. Palm Kernel Oil S&D (1,000 tonnes) — the second oil
```
Beginning Stocks (October 1)
Production
Imports
Total Supply
Domestic Usage
Exports
Total Use
Ending Stocks (September 30)
Stocks/Use
Futures Price (Ringgit/Tonne)
```

---

## 2. Copra / coconut complex — three balance sheets

### 2a. Copra S&D (1,000 hectares / 1,000 tonnes)
```
Trees
Total Mature Area
Yield (Tonnes per Hectare)
Carryin (October 1)
Production
Imports
Total Supply
Crush
Exports
Residual                        <- explicit residual line; palm sheets have none
Total Use
Carryout (September 30)
Stocks-to-Usage
Average Price (Dollars per Ton)
```

### 2b. Copra Meal S&D (1,000 tonnes)
```
Crush (October - September)     <- carried down from the copra sheet, drives production
Carryin (October 1)
Production
Imports
Total Supply
Domestic Usage
Exports
Total Use
Carryout (September 30)
Average Price (50%, Dollars per Ton)
```

### 2c. Coconut Oil S&D (1,000 tonnes)
```
Carryin (October 1)
Production
Imports
Total Supply
Domestic Usage
    Industrial Usage            (source workbook misspells this "Industrail Usage" — fix in ours)
    Feed Usage
    Food Usage
Exports
Total Use
Carryout (September 30)
Average Price (50%, Dollars per Ton)
```
Note the coconut oil domestic split is **three-way** (Industrial / Feed / Food) against palm's
two-way (Industrial / Food-Feed-Waste). Keep the difference — it is not an inconsistency to
normalize away; coconut oil has a genuine feed channel that palm reports lump in.

---

## 3. Monthly detail blocks (below each annual sheet)

Every annual balance sheet is backed by monthly blocks laid out **MY-column-major**: rows are
marketing years (`93/94` down), columns are `Oct … Sep`, with a `Total` (or `Average` for rates)
column. This is the opposite orientation from our flat files (rows ascending by period) — the
conversion belongs in the writer, not by reshaping the model.

Blocks present for palm:
```
Implied Fresh Fruit Bunch Production (1,000 Tonnes)     -> Total
Palm Oil Extraction Rate (%)                            -> Average      (OER — the yield driver)
Palm Oil Production (1,000 Tonnes)                      -> Total
Palm Oil Month-over-Month Change in Production          -> (no total)
Palm Oil Imports (1,000 Tonnes)                         -> Total
Palm Oil Exports (1,000 Tonnes)                         -> Total
Palm Oil Domestic Use (1,000 Tonnes)                    -> Total
Palm Oil End-of-Month Stocks (1,000 Tonnes)             -> (level, dated)
Malaysia Palm Oil Futures (Continuous Third Month)      -> Marketing-Year Average
Palm Kernel Production (1,000 Tonnes)                   -> Total
```

**FFB → OER → CPO is the production identity.** Implied FFB × extraction rate = palm oil
production. That chain is what makes a palm production forecast auditable rather than asserted, and
it is the analogue of area × yield in the row crops.

The copra sheets carry the same treatment for Copra Imports, Copra Exports, and so on.

---

## 4. Supporting sheets worth replicating

| Sheet | What it does |
|---|---|
| `MPOB Report Breakdown` | Monthly MPOB actuals — Production, Exports, Imports, Closing Stocks, **Domestic Usage derived as a residual** off the prior month's closing stocks. MPOB is §9-citable |
| `Malaysian Cargo Surveys` | High-frequency export tracking (ITS/AmSpec/SGS) — the early-warning series between MPOB releases |
| `Malaysian Palm USDA Comp` | Our numbers vs USDA PSD — the reconciliation check |
| `SBO-PO spread` | Soybean oil vs palm oil spread — the substitution signal, and directly feeds the cross-commodity knock-on scenario promised to Helios |
| `SE Asia Palm Complex` | Malaysia + Indonesia rollup |

The `MPOB Report Breakdown` residual method is worth flagging: domestic usage is *derived*, not
reported. Same pattern as the SBO non-biofuel residual — and it carries the same risk, that every
error in production, trade, or stocks accumulates into the residual line.

---

## 5. What this means for the Pepsi build

1. **Tracker sheet set for palm is wrong and must change** — from `Plantation · Oil S&D · Trade ·
   Stocks` to `Plantation · Seed S&D (palm kernel) · Crush · Oil S&D (CPO) · Kernel Oil S&D (PKO) ·
   Meal S&D (PKC) · Trade · Stocks`. Palm is the *largest* per-country build of the five complexes,
   not the smallest.
2. **A "Kernel Oil S&D" sheet type has to exist** — no other complex has a second oil, so it cannot
   be folded into `Oil S&D` without losing PKO.
3. **Marketing year differs.** Palm and copra run **Oct–Sep**, same as US oils. Don't inherit the
   soybean-complex MY assumptions blindly for the other complexes.
4. **Copra / coconut oil is NOT in SOW No. 1** (five complexes: palm, rapeseed, sunflower, soybean,
   corn oil). Build it into the models library for lauric substitution and the feedstock work — but
   it is not a Pepsi deliverable and should not consume six-week build time.
5. **The Industrial-Usage line is the B50 modeling hook.** Wire it explicitly rather than treating
   Indonesian domestic use as one lump.
