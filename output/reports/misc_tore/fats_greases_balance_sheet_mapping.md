# Fats & Greases Balance Sheet Template Mapping

## Template: us_choice_white_grease_balance.xlsx (CWG)
276 rows, 16 blocks per commodity, Oct-Sep marketing year.

## External File References
- `[1]` = `us_oilseed_crush.xlsm` → sheet `NASS Low CI` (production + stocks)
- `[2]` = `us_fats_greases_trade.xlsx` (imports + exports per commodity)
- `[3]` = Another balance sheet file (marketing year headers in row 3)
- `[4]` = `feedstock_allocation_output.xlsx` → sheet `Allocation` (biofuel feedstock demand)
- `[5]` = `us_livestock_slaughter.xlsx` (slaughter + live weight data)
- `[6]` = `us_fats_greases_prices.xlsx` (cash prices)

## Per-Commodity Configuration

### Choice White Grease (CWG) — TEMPLATE DONE
| Block | [1] NASS Col | [2] Trade Sheet | [4] Alloc Col | [5] Livestock | [6] Price |
|-------|-------------|-----------------|---------------|---------------|-----------|
| Production | B (prod), C (use), D (stocks) | — | — | — | — |
| Imports | — | CWG Imports | — | — | — |
| Exports | — | CWG Exports | — | — | — |
| Biodiesel | — | — | G (WhGr) | — | — |
| Renew Diesel | — | — | P (WhGr) | — | — |
| Co-Processing | — | — | AH (WhGr) | — | — |
| SAF | — | — | Y (WhGr) | — | — |
| Stocks | D (stocks) | — | — | — | — |
| Slaughter | — | — | — | B (hog head) | — |
| Live Weight | — | — | — | D (hog lbs) | — |
| Price Chicago | — | — | — | — | CWG MO River (col B) |

### Inedible Tallow
| Block | [1] NASS Col | [2] Trade Sheet | [4] Alloc Col | [5] Livestock |
|-------|-------------|-----------------|---------------|---------------|
| Production | AE (prod), AF (use), AG (stocks) | — | — | — |
| Imports | — | Inedible Tallow Imports | — | — |
| Exports | — | Inedible Tallow Exports | — | — |
| Biodiesel | — | — | F (Tallow) | — |
| Renew Diesel | — | — | O (Tallow) | — |
| Co-Processing | — | — | AG (Tallow) | — |
| SAF | — | — | X (Tallow) | — |
| Stocks | AG (stocks) | — | — | — |
| Slaughter | — | — | — | I (cattle head) |
| Live Weight | — | — | — | K (cattle lbs) |

### Edible Tallow
| Block | [1] NASS Col | [2] Trade Sheet | [4] Alloc Col | [5] Livestock |
|-------|-------------|-----------------|---------------|---------------|
| Production | AA (prod), AB (use), AC (stocks) | — | — | — |
| Imports | — | Edible Tallow Imports | — | — |
| Exports | — | Edible Tallow Exports | — | — |
| Biodiesel | — | — | F (Tallow)* | — |
| Slaughter | — | — | — | I (cattle head) |
*Note: EIA doesn't split edible/inedible tallow in feedstock data. May need to allocate.

### Yellow Grease
| Block | [1] NASS Col | [2] Trade Sheet | [4] Alloc Col | [5] Livestock |
|-------|-------------|-----------------|---------------|---------------|
| Production | AM (prod), AN (use), AO (stocks) | — | — | — |
| Imports | — | Yellow Grease Imports | — | — |
| Exports | — | Yellow Grease Exports | — | — |
| Biodiesel | — | — | H (YelGr) | — |
| Renew Diesel | — | — | Q (YelGr) | — |
| Co-Processing | — | — | AI (YelGr) | — |
| SAF | — | — | Z (YelGr) | — |
| Stocks | AO (stocks) | — | — | — |

### Poultry Fat
| Block | [1] NASS Col | [2] Trade Sheet | [4] Alloc Col | [5] Livestock |
|-------|-------------|-----------------|---------------|---------------|
| Production | T (prod), U (use), V (stocks) | — | — | — |
| Imports | — | Poultry Fat Imports | — | — |
| Exports | — | Poultry Fat Exports | — | — |
| Biodiesel | — | — | E (Poultry) | — |
| Renew Diesel | — | — | N (Poultry) | — |
| Co-Processing | — | — | AF (Poultry) | — |
| SAF | — | — | W (Poultry) | — |
| Stocks | V (stocks) | — | — | — |
| Slaughter | — | — | — | P (chickens head) |
| Live Weight | — | — | — | R (chickens lbs) |

### Lard
| Block | [1] NASS Col | [2] Trade Sheet | [4] Alloc Col | [5] Livestock |
|-------|-------------|-----------------|---------------|---------------|
| Production | I (prod), J (use), K (stocks) | — | — | — |
| Imports | — | Lard Imports | — | — |
| Exports | — | Lard Exports | — | — |
| Biodiesel | — | — | (none — lard not in alloc) | — |
| Stocks | K (stocks) | — | — | — |
| Slaughter | — | — | — | B (hog head) |
| Live Weight | — | — | — | D (hog lbs) |

### UCO (to build with user later)
| Block | [1] NASS Col | [2] Trade Sheet | [4] Alloc Col |
|-------|-------------|-----------------|---------------|
| Production | (collection model — separate) | — | — |
| Imports | — | UCO Imports | — |
| Exports | — | UCO Exports | — |
| Biodiesel | — | — | (in YelGr/UCO combined) |

### Other Grease
| Block | [1] NASS Col | [2] Trade Sheet |
|-------|-------------|-----------------|
| Production | P (prod), Q (use), R (stocks) | — |
| (no trade sheets yet) | — | — |

## Balance Sheet Formula Patterns (from CWG template)

### Production (rows 25-36): `='[1]NASS Low CI'!{COL}{ROW}/1000000`
- COL = production column for the commodity
- ROW = 437 + (MY_offset * 12) + month_offset
- Oct of MY 2015/16 = row 437

### Imports/Exports (rows 57-68 / 73-84): `='[2]{Sheet}'!{COL}$217/1000`
- Sheet = commodity-specific import/export sheet
- COL steps by 12 per marketing year (AW, BI, BU, CG, CS, DE, DQ, EC, EO, FA, FM)
- Row 217 = world total row
- /1000 converts from 000 lbs to million lbs

### Allocation (rows 121-132 etc.): `='[4]Allocation'!${COL}{ROW}`
- COL = feedstock column (fixed per commodity)
- ROW = 5-13 for Jan-Sep, 14-25 for Oct-Sep of next MY block

### Stocks (rows 201-212): `='[1]NASS Low CI'!{COL}{ROW}/1000000`
- COL = stocks column for the commodity
- Same row mapping as production
