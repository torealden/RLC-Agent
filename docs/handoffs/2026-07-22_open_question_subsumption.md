# Open questions — does something else already provide this data?

23 workbooks still marked `?`. Compared against 133 kept and 81 archive-bound workbooks under `models/`.

**Signal 1 — identical file.** Same sha256 somewhere else. Settled.

**Signal 2 — tab-name containment.** What share of this file's tabs appear by name in another workbook. Evidence only: a matching tab name does not prove matching coverage or vintage. Verify before archiving anything on this alone.

## `models/Biofuels/us_biodiesel_balance_sheets.xlsx`

1 tabs · 0.2 MB

| covers | shared tabs | candidate | verdict | example tabs |
|---:|---:|---|---|---|
| 100% | 1 | `models/Feed Grains/us_corn_balance_sheet_backup_20260713_171323.xlsm` | ARCHIVE_OK | balancesheet |
| 100% | 1 | `models/Feed Grains/us_corn_balance_sheet_backup_20260713_160827.xlsm` | ARCHIVE_OK | balancesheet |
| 100% | 1 | `models/Feed Grains/us_corn_balance_sheet_backup_20260713_160014.xlsm` | ARCHIVE_OK | balancesheet |
| 100% | 1 | `models/Feed Grains/us_corn_balance_sheet_backup_20260713_155820.xlsm` | ARCHIVE_OK | balancesheet |

## `models/Biofuels/us_renewable_diesel_balance_sheets (version 1).xlsx`

1 tabs · 0.2 MB

| covers | shared tabs | candidate | verdict | example tabs |
|---:|---:|---|---|---|
| 100% | 1 | `models/Feed Grains/us_corn_balance_sheet_backup_20260713_171323.xlsm` | ARCHIVE_OK | balancesheet |
| 100% | 1 | `models/Feed Grains/us_corn_balance_sheet_backup_20260713_160827.xlsm` | ARCHIVE_OK | balancesheet |
| 100% | 1 | `models/Feed Grains/us_corn_balance_sheet_backup_20260713_160014.xlsm` | ARCHIVE_OK | balancesheet |
| 100% | 1 | `models/Feed Grains/us_corn_balance_sheet_backup_20260713_155820.xlsm` | ARCHIVE_OK | balancesheet |

## `models/Biofuels/us_renewable_diesel_balance_sheets.xlsx`

1 tabs · 0.2 MB

| covers | shared tabs | candidate | verdict | example tabs |
|---:|---:|---|---|---|
| 100% | 1 | `models/Feed Grains/us_corn_balance_sheet_backup_20260713_171323.xlsm` | ARCHIVE_OK | balancesheet |
| 100% | 1 | `models/Feed Grains/us_corn_balance_sheet_backup_20260713_160827.xlsm` | ARCHIVE_OK | balancesheet |
| 100% | 1 | `models/Feed Grains/us_corn_balance_sheet_backup_20260713_160014.xlsm` | ARCHIVE_OK | balancesheet |
| 100% | 1 | `models/Feed Grains/us_corn_balance_sheet_backup_20260713_155820.xlsm` | ARCHIVE_OK | balancesheet |

## `models/Biofuels/us_sustainable_aviation_fuel_balance_sheets.xlsx`

1 tabs · 0.2 MB

_No workbook shares a meaningful share of these tab names — likely genuinely standalone._

## `models/Cotton/us_cotton_production.xlsx`

5 tabs · 0.0 MB

| covers | shared tabs | candidate | verdict | example tabs |
|---:|---:|---|---|---|
| 100% | 5 | `models/Oilseeds/United States/us_sunflower_production.xlsx` | IS_CANONICAL | areaharvested, areaplanted, meta, production, yield |
| 100% | 5 | `models/Oilseeds/United States/us_soybean_production.xlsx` | IS_CANONICAL | areaharvested, areaplanted, meta, production, yield |
| 100% | 5 | `models/Oilseeds/United States/us_safflower_production.xlsx` | IS_CANONICAL | areaharvested, areaplanted, meta, production, yield |
| 100% | 5 | `models/Oilseeds/United States/us_peanut_production.xlsx` | IS_CANONICAL | areaharvested, areaplanted, meta, production, yield |

## `models/Cotton/us_cotton_trade.xlsx`

2 tabs · 0.5 MB

_No workbook shares a meaningful share of these tab names — likely genuinely standalone._

## `models/Cotton/world_cotton_trade.xlsx`

6 tabs · 1.4 MB

_No workbook shares a meaningful share of these tab names — likely genuinely standalone._

## `models/Data/US Corn Exports - 01201025.xlsx`

1 tabs · 0.0 MB

**Byte-identical copies:**
- `models/Spreadsheet Samples/Data Samples/Feed Grains/US Corn Exports - 01201025.xlsx` — ARCHIVE_OK

| covers | shared tabs | candidate | verdict | example tabs |
|---:|---:|---|---|---|
| 100% | 1 | `models/Spreadsheet Samples/Data Samples/Feed Grains/US Corn Exports - 01201025.xlsx` | ARCHIVE_OK | standardquery31849 |

## `models/Data/US Corn Imports - 01201025.xlsx`

1 tabs · 0.0 MB

**Byte-identical copies:**
- `models/Spreadsheet Samples/Data Samples/Feed Grains/US Corn Imports - 01201025.xlsx` — ARCHIVE_OK

| covers | shared tabs | candidate | verdict | example tabs |
|---:|---:|---|---|---|
| 100% | 1 | `models/Spreadsheet Samples/Data Samples/Feed Grains/US Corn Imports - 01201025.xlsx` | ARCHIVE_OK | standardquery59138 |

## `models/Data/US Feed Grains Outlook - Dec 25.xlsx`

36 tabs · 0.9 MB

**Byte-identical copies:**
- `models/Spreadsheet Samples/Data Samples/Feed Grains/US Feed Grains Outlook - Dec 25.xlsx` — ARCHIVE_OK

| covers | shared tabs | candidate | verdict | example tabs |
|---:|---:|---|---|---|
| 100% | 36 | `models/Spreadsheet Samples/Data Samples/Feed Grains/US Feed Grains Outlook - Dec 25.xlsx` | ARCHIVE_OK | contents, fgyearbooktable01, fgyearbooktable02, fgyearbooktable03, fgyearbooktable04, fgyearbooktable05 |
| 3% | 1 | `models/Spreadsheet Samples/Data Samples/Oilseeds/oiltables.xlsx` | ARCHIVE_OK | contents |

## `models/Data/oiltables.xlsx`

12 tabs · 0.2 MB

**Byte-identical copies:**
- `models/Spreadsheet Samples/Data Samples/Oilseeds/oiltables.xlsx` — ARCHIVE_OK

| covers | shared tabs | candidate | verdict | example tabs |
|---:|---:|---|---|---|
| 100% | 12 | `models/Spreadsheet Samples/Data Samples/Oilseeds/oiltables.xlsx` | ARCHIVE_OK | contents, figure1, figure2, figure3, figure4, table1 |
| 50% | 6 | `models/Spreadsheet Samples/Data Samples/Wheat/CGC October Exports.xlsx` | ARCHIVE_OK | table1, table10, table2, table3, table8, table9 |
| 8% | 1 | `models/Spreadsheet Samples/Data Samples/Feed Grains/US Feed Grains Outlook - Dec 25.xlsx` | ARCHIVE_OK | contents |

## `models/Fats and Greases/us_animal_fat_prices.xlsx`

10 tabs · 0.1 MB

_No workbook shares a meaningful share of these tab names — likely genuinely standalone._

## `models/Feed Grains/cir_m311k1107.xlsx`

10 tabs · 0.1 MB

| covers | shared tabs | candidate | verdict | example tabs |
|---:|---:|---|---|---|
| 40% | 4 | `models/Spreadsheet Samples/Data Samples/Wheat/CGC October Exports.xlsx` | ARCHIVE_OK | table1, table2, table3, table4 |
| 30% | 3 | `models/Spreadsheet Samples/Data Samples/Oilseeds/oiltables.xlsx` | ARCHIVE_OK | table1, table2, table3 |

## `models/Feed Grains/corn_processing_report.xlsx`

2 tabs · 0.0 MB

| covers | shared tabs | candidate | verdict | example tabs |
|---:|---:|---|---|---|
| 50% | 1 | `models/Macro/Commitment of Traders Sheets.xlsx` | IS_CANONICAL | data |

## `models/Feed Grains/us_corn_processing.xlsx`

1 tabs · 0.0 MB

_No workbook shares a meaningful share of these tab names — likely genuinely standalone._

## `models/Feed Grains/us_grain_crush.xlsm`

5 tabs · 0.2 MB

| covers | shared tabs | candidate | verdict | example tabs |
|---:|---:|---|---|---|
| 20% | 1 | `models/Population/population_by_country.xlsx` | IS_CANONICAL | meta |
| 20% | 1 | `models/Oilseeds/us_soybean_oil_supply_demand.xlsx` | IS_CANONICAL | meta |
| 20% | 1 | `models/Oilseeds/us_canola_oil_supply_demand.xlsx` | IS_CANONICAL | meta |
| 20% | 1 | `models/Oilseeds/United States/us_sunflower_production.xlsx` | IS_CANONICAL | meta |

## `models/Food Grains/keypath_proof_1.xlsx`

3 tabs · 0.0 MB

| covers | shared tabs | candidate | verdict | example tabs |
|---:|---:|---|---|---|
| 67% | 2 | `models/Oilseeds/United States/us_sunflower_production.xlsx` | IS_CANONICAL | meta, production |
| 67% | 2 | `models/Oilseeds/United States/us_soybean_production.xlsx` | IS_CANONICAL | meta, production |
| 67% | 2 | `models/Oilseeds/United States/us_safflower_production.xlsx` | IS_CANONICAL | meta, production |
| 67% | 2 | `models/Oilseeds/United States/us_peanut_production.xlsx` | IS_CANONICAL | meta, production |

## `models/Food Grains/us_food_grain_production.xlsx`

5 tabs · 0.1 MB

| covers | shared tabs | candidate | verdict | example tabs |
|---:|---:|---|---|---|
| 20% | 1 | `models/Population/population_by_country.xlsx` | IS_CANONICAL | meta |
| 20% | 1 | `models/Oilseeds/us_soybean_oil_supply_demand.xlsx` | IS_CANONICAL | meta |
| 20% | 1 | `models/Oilseeds/us_canola_oil_supply_demand.xlsx` | IS_CANONICAL | meta |
| 20% | 1 | `models/Oilseeds/United States/us_sunflower_production.xlsx` | IS_CANONICAL | meta |

## `models/Food Grains/world_wheat_trade.xlsx`

14 tabs · 3.2 MB

_No workbook shares a meaningful share of these tab names — likely genuinely standalone._

## `models/Oilseeds/Brazil/brazil_soy_complex_monthly.xlsx`

2 tabs · 0.0 MB

| covers | shared tabs | candidate | verdict | example tabs |
|---:|---:|---|---|---|
| 100% | 2 | `models/Oilseeds/Brazil/brazil_conab_monthly.xlsx` | IS_CANONICAL | meta, soycomplex |
| 50% | 1 | `models/Population/population_by_country.xlsx` | IS_CANONICAL | meta |
| 50% | 1 | `models/Oilseeds/us_soybean_oil_supply_demand.xlsx` | IS_CANONICAL | meta |
| 50% | 1 | `models/Oilseeds/us_canola_oil_supply_demand.xlsx` | IS_CANONICAL | meta |

## `models/Oilseeds/United States/us_oilseed_production.xlsx`

13 tabs · 0.0 MB

_No workbook shares a meaningful share of these tab names — likely genuinely standalone._

## `models/Oilseeds/United States/us_soy_crush.xlsm`

3 tabs · 0.6 MB

| covers | shared tabs | candidate | verdict | example tabs |
|---:|---:|---|---|---|
| 67% | 2 | `models/Oilseeds/United States/us_oilseed_crush.xlsm` | IS_CANONICAL | censuscrush, nopauscrush |
| 67% | 2 | `models/Oilseeds/Archive/us_oilseed_crush.backup_20260612_045932.xlsm` | ARCHIVE_OK | censuscrush, nopauscrush |

## `models/Oilseeds/United States/us_veg_oil_prices.xlsx`

5 tabs · 0.1 MB

| covers | shared tabs | candidate | verdict | example tabs |
|---:|---:|---|---|---|
| 20% | 1 | `models/Population/population_by_country.xlsx` | IS_CANONICAL | meta |
| 20% | 1 | `models/Oilseeds/us_soybean_oil_supply_demand.xlsx` | IS_CANONICAL | meta |
| 20% | 1 | `models/Oilseeds/us_canola_oil_supply_demand.xlsx` | IS_CANONICAL | meta |
| 20% | 1 | `models/Oilseeds/United States/us_sunflower_production.xlsx` | IS_CANONICAL | meta |
