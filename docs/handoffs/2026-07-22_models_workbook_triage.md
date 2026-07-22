# `models/` workbook triage — 2026-07-22

Generated from system-graph scan 10. **Your rule:** anything in `models/` that is not a
balance sheet or a known input (flat file) can move to `models/Archive`.

Mark each row: `K` keep · `A` archive · `?` unsure. I'll persist your answers into
`sys.declaration` so no future scan re-proposes them.

Columns: **in** = workbooks that link INTO this one · **out** = external links this one
makes · **vba** = macro-enabled · **sh** = sheet count.

Buckets I pre-assigned (heuristic, override freely):

| | meaning |
|---|---|
| `A` | already named archive/backup — almost certainly move |
| `B` | filename looks like a balance sheet — almost certainly keep |
| `L` | something links to it or it links out — likely a known input |
| `?` | neither — **these are the ones that need your eye** |

**237 workbooks** — `?`=100 · `L`=23 · `B`=42 · `A`=72

## `models/`  (6)

| | file | in | out | vba | sh | MB |
|---|---|---:|---:|:--:|---:|---:|
| `?` | File Index-Mapping.xlsx | 0 | 0 |  | 1 | 0.0 |
| `?` | HS Codes.xlsx | 0 | 0 |  | 2 | 0.0 |
| `?` | commodities_config.xlsx | 0 | 0 |  | 4 | 0.0 |
| `?` | hb_price_forecast.xlsx | 0 | 0 |  | 3 | 0.0 |
| `?` | rlc_prices.xlsx | 0 | 0 |  | 18 | 6.1 |
| `B` | us_oilseed_balance_sheet_templates.xlsx | 0 | 0 |  | 4 | 0.0 |

## `models\AnimalUnits/`  (1)

| | file | in | out | vba | sh | MB |
|---|---|---:|---:|:--:|---:|---:|
| `?` | animal_units_by_country.xlsx | 0 | 0 |  | 8 | 0.1 |

## `models\Archive/`  (28)

| | file | in | out | vba | sh | MB |
|---|---|---:|---:|:--:|---:|---:|
| `A` | Protein Meal and Veg Oil Balance Sheet Templates.xlsx | 2 | 0 |  | 2 | 0.2 |
| `A` | Soybean Crop Conditions (Tore Alden's conflicted copy 2025-08-06).xlsx | 0 | 0 |  | 14 | 0.2 |
| `A` | US Oilseed Balance Sheet - Duplicate move to Archive and Delete.xlsx | 19 | 0 |  | 30 | 5.9 |
| `A` | US Oilseed Balance Sheets (version 1).xlsx | 19 | 0 |  | 30 | 5.9 |
| `A` | US Oilseed Balance Sheets - Final.xlsx | 19 | 0 |  | 30 | 5.9 |
| `A` | World Rapeseed Trade.xlsx | 1 | 2 |  | 55 | 22.9 |
| `A` | archive_tabs.xlsx | 1 | 0 |  | 1 | 0.0 |
| `A` | us_biodiesel_bal_sheets.xlsx | 1 | 0 |  | 1 | 0.0 |
| `A` | us_coconut_bal_sheets.xlsm | 4 | 0 | Y | 5 | 0.2 |
| `A` | us_edible_tallow_balance.xlsx | 5 | 0 |  | 1 | 0.1 |
| `A` | us_fats_greases_trade_backup_20260504_050250.xlsm | 0 | 0 | Y | 16 | 0.8 |
| `A` | us_fats_greases_trade_backup_my_20260504_050833.xlsm | 0 | 0 | Y | 16 | 0.7 |
| `A` | us_fats_greases_trade_backup_my_20260505_051545.xlsm | 0 | 0 | Y | 16 | 1.4 |
| `A` | us_fuel_trade.xlsx | 0 | 0 |  | 16 | 2.0 |
| `A` | us_grain_crush.xlsx | 0 | 0 |  | 2 | 0.0 |
| `A` | us_grain_crush_nass_data.xlsx | 0 | 0 |  | 1 | 0.0 |
| `A` | us_inedible_tallow_balance.xlsx | 5 | 0 |  | 1 | 0.1 |
| `A` | us_minor_veg_oils_trade_old (2).xlsm | 0 | 0 | Y | 12 | 0.2 |
| `A` | us_minor_veg_oils_trade_old.xlsm | 0 | 0 | Y | 12 | 0.2 |
| `A` | us_renewable_diesel_bal_sheets.xlsx | 0 | 0 |  | 1 | 0.0 |
| `A` | us_saf_bal_sheets.xlsx | 0 | 0 |  | 1 | 0.0 |
| `A` | us_soybean_complex_bal_sheets_backup_20260713_124240.xlsm | 4 | 0 | Y | 5 | 0.5 |
| `A` | us_soybean_complex_bal_sheets_backup_20260713_124853.xlsm | 4 | 0 | Y | 5 | 0.5 |
| `A` | us_soybean_complex_bal_sheets_backup_20260713_154754.xlsm | 4 | 0 | Y | 5 | 0.5 |
| `A` | us_soybean_complex_bal_sheets_backup_20260713_160833.xlsm | 4 | 0 | Y | 5 | 0.5 |
| `A` | us_tallow_complex_balance.xlsm | 6 | 0 | Y | 6 | 2.2 |
| `A` | us_uco_bal_sheets.xlsx | 1 | 0 |  | 1 | 0.0 |
| `A` | us_yellow_grease_balance.xlsx | 5 | 0 |  | 1 | 0.1 |

## `models\Archive\stale_united_states_dup_20260713/`  (3)

| | file | in | out | vba | sh | MB |
|---|---|---:|---:|:--:|---:|---:|
| `A` | us_uco_supply_demand.xlsx | 0 | 0 |  | 3 | 0.1 |
| `A` | us_white_grease_supply_demand.xlsx | 0 | 0 |  | 3 | 0.1 |
| `A` | us_yellow_grease_supply_demand.xlsx | 0 | 0 |  | 3 | 0.0 |

## `models\Biofuels/`  (22)

| | file | in | out | vba | sh | MB |
|---|---|---:|---:|:--:|---:|---:|
| `?` | eia_us_biofuel_capacity.xlsx | 0 | 0 |  |  | 0.0 |
| `?` | epa_pathway_plants.xlsx | 0 | 0 |  |  | 0.0 |
| `?` | feedstock_allocation_model.xlsx | 0 | 0 |  |  | 0.0 |
| `?` | feedstock_allocation_output.xlsx | 0 | 0 |  |  | 0.0 |
| `?` | us_credit_prices.xlsx | 0 | 0 |  |  | 0.0 |
| `?` | us_fuel_prices.xlsx | 0 | 0 |  |  | 0.0 |
| `L` | RD Feedstock Build Up.xlsx | 1 | 0 |  |  | 0.0 |
| `L` | eia_data.xlsm | 1 | 47 |  |  | 0.0 |
| `L` | fuel_value_breakout.xlsx | 0 | 1 |  |  | 0.0 |
| `L` | rfs_data.xlsm | 1 | 6 |  |  | 0.0 |
| `L` | rfs_mandates.xlsx | 1 | 0 |  |  | 0.0 |
| `L` | rfs_predictions.xlsx | 1 | 0 |  |  | 0.0 |
| `L` | us_fuel_trade.xlsm | 0 | 11 |  |  | 0.0 |
| `L` | us_liquid_fuel_and_biofuel_production.xlsx | 0 | 17 |  |  | 0.0 |
| `B` | ethanol template archive after alternative balance sheet complete.xlsx | 4 | 0 |  |  | 0.0 |
| `B` | us_bbd_combined_bal_sheets.xlsx | 0 | 0 |  |  | 0.0 |
| `B` | us_biodiesel_balance_sheets.xlsx | 2 | 0 |  |  | 0.0 |
| `B` | us_distillate_fuel_oil_bal_sheets.xlsx | 0 | 0 |  |  | 0.0 |
| `B` | us_ethanol_balance_sheet.xlsx | 3 | 0 |  |  | 0.0 |
| `B` | us_renewable_diesel_balance_sheets (version 1).xlsx | 2 | 0 |  |  | 0.0 |
| `B` | us_renewable_diesel_balance_sheets.xlsx | 2 | 0 |  |  | 0.0 |
| `B` | us_sustainable_aviation_fuel_balance_sheets.xlsx | 2 | 0 |  |  | 0.0 |

## `models\Biofuels\archive/`  (1)

| | file | in | out | vba | sh | MB |
|---|---|---:|---:|:--:|---:|---:|
| `A` | us_corn_grind_template_unfinished.xlsx | 0 | 0 |  | 1 | 0.0 |

## `models\Biofuels\biofuel_charts/`  (1)

| | file | in | out | vba | sh | MB |
|---|---|---:|---:|:--:|---:|---:|
| `L` | us_biofuel_charts.xlsx | 3 | 0 |  |  | 0.0 |

## `models\Cotton/`  (3)

| | file | in | out | vba | sh | MB |
|---|---|---:|---:|:--:|---:|---:|
| `?` | us_cotton_production.xlsx | 0 | 0 |  | 5 | 0.0 |
| `?` | us_cotton_trade.xlsx | 0 | 0 |  | 2 | 0.5 |
| `?` | world_cotton_trade.xlsx | 0 | 0 |  | 6 | 1.4 |

## `models\Data/`  (4)

| | file | in | out | vba | sh | MB |
|---|---|---:|---:|:--:|---:|---:|
| `?` | US Corn Exports - 01201025.xlsx | 0 | 0 |  | 1 | 0.0 |
| `?` | US Corn Imports - 01201025.xlsx | 0 | 0 |  | 1 | 0.0 |
| `?` | US Feed Grains Outlook - Dec 25.xlsx | 0 | 0 |  | 36 | 0.9 |
| `?` | oiltables.xlsx | 0 | 0 |  | 12 | 0.2 |

## `models\Fats and Greases/`  (23)

| | file | in | out | vba | sh | MB |
|---|---|---:|---:|:--:|---:|---:|
| `?` | us_animal_fat_prices.xlsx | 0 | 0 |  |  | 0.0 |
| `L` | us_fats_greases_prices.xlsx | 0 | 12 |  |  | 0.0 |
| `L` | us_fats_greases_trade.xlsm | 0 | 23 |  |  | 0.0 |
| `L` | us_livestock_slaughter.xlsx | 0 | 17 |  |  | 0.0 |
| `B` | us_choice_white_grease_balance.xlsx | 5 | 0 |  |  | 0.0 |
| `B` | us_dco_supply_demand.xlsx | 0 | 0 |  |  | 0.0 |
| `B` | us_distillers_corn_oil_balance.xlsx | 7 | 0 |  |  | 0.0 |
| `B` | us_fat_and_grease_balance_sheets.xlsx | 0 | 0 |  |  | 0.0 |
| `B` | us_lard_balance.xlsx | 4 | 0 |  |  | 0.0 |
| `B` | us_poultry_fat_balance.xlsx | 5 | 0 |  |  | 0.0 |
| `B` | us_poultry_fat_supply_demand.xlsx | 0 | 0 |  |  | 0.0 |
| `B` | us_tallow_complex_balance.xlsx | 6 | 0 |  |  | 0.0 |
| `B` | us_tallow_supply_demand.xlsx | 0 | 1 |  |  | 0.0 |
| `B` | us_uco_supply_demand.xlsx | 0 | 0 |  |  | 0.0 |
| `B` | us_used_cooking_oil_balance.xlsx | 5 | 0 |  |  | 0.0 |
| `B` | us_white_grease_supply_demand.xlsx | 0 | 0 |  |  | 0.0 |
| `B` | us_yellow_grease_supply_demand.xlsx | 0 | 0 |  |  | 0.0 |
| `A` | us_choice_white_grease_balance.xlsx.bak_20260526_141049 | 5 | 0 |  | 1 | 0.1 |
| `A` | us_distillers_corn_oil_balance.xlsx.bak_20260526_141048 | 7 | 0 |  | 3 | 0.3 |
| `A` | us_poultry_fat_balance.xlsx.bak_20260526_141049 | 5 | 0 |  | 1 | 0.1 |
| `A` | us_tallow_complex_balance.xlsx.bak_20260526_141049 | 5 | 0 |  | 4 | 0.4 |
| `A` | us_used_cooking_oil_balance.xlsx.bak_20260526_141049 | 5 | 0 |  | 3 | 0.3 |
| `A` | us_used_cooking_oil_balance.xlsx.bak_20260526_141050 | 5 | 0 |  | 3 | 0.2 |

## `models\Feed Grains/`  (17)

| | file | in | out | vba | sh | MB |
|---|---|---:|---:|:--:|---:|---:|
| `?` | cir_m311k1107.xlsx | 0 | 0 |  | 10 | 0.1 |
| `?` | corn_processing_report.xlsx | 0 | 0 |  | 2 | 0.0 |
| `?` | us_feed_grain_production.xlsx | 0 | 0 |  | 7 | 0.1 |
| `?` | us_grains_trade.xlsx | 0 | 0 |  | 10 | 2.2 |
| `?` | us_sorghum_production.xlsx | 0 | 0 |  | 5 | 0.0 |
| `?` | world_corn_trade.xlsx | 0 | 0 |  | 10 | 2.2 |
| `L` | us_corn_processing.xlsx | 0 | 1 |  | 1 | 0.0 |
| `L` | us_corn_production.xlsx | 0 | 1 |  | 15 | 0.2 |
| `L` | us_grain_crush.xlsm | 0 | 5 | Y | 5 | 0.2 |
| `L` | us_grains_trade.xlsm | 0 | 1 | Y | 10 | 1.9 |
| `B` | us_corn_balance_sheet.xlsm | 4 | 0 | Y | 2 | 0.2 |
| `B` | us_corn_co_products_balance_sheets.xlsx | 0 | 0 |  | 13 | 0.0 |
| `A` | us_corn_balance_sheet_backup_20260713_155756.xlsm | 4 | 0 | Y | 2 | 0.2 |
| `A` | us_corn_balance_sheet_backup_20260713_155820.xlsm | 4 | 0 | Y | 2 | 0.2 |
| `A` | us_corn_balance_sheet_backup_20260713_160014.xlsm | 4 | 0 | Y | 2 | 0.2 |
| `A` | us_corn_balance_sheet_backup_20260713_160827.xlsm | 4 | 0 | Y | 2 | 0.2 |
| `A` | us_corn_balance_sheet_backup_20260713_171323.xlsm | 4 | 0 | Y | 2 | 0.2 |

## `models\Food Grains/`  (9)

| | file | in | out | vba | sh | MB |
|---|---|---:|---:|:--:|---:|---:|
| `?` | keypath_proof_1.xlsx | 0 | 0 |  | 3 | 0.0 |
| `?` | us_food_grain_production.xlsx | 0 | 0 |  | 5 | 0.1 |
| `?` | us_wheat_milling.xlsx | 0 | 0 |  | 2 | 0.0 |
| `?` | us_wheat_production.xlsx | 0 | 0 |  | 2 | 0.1 |
| `?` | world_wheat_trade.xlsx | 0 | 0 |  | 14 | 3.2 |
| `B` | us_wheat_balance_sheet.xlsm | 0 | 0 | Y | 6 | 0.1 |
| `B` | us_wheat_co_products_balance_sheets.xlsx | 0 | 0 |  | 11 | 0.0 |
| `A` | us_wheat_balance_sheet_backup_20260713_160829.xlsm | 0 | 0 | Y | 6 | 0.1 |
| `A` | us_wheat_balance_sheet_backup_20260713_171525.xlsm | 0 | 0 | Y | 6 | 0.1 |

## `models\Macro/`  (2)

| | file | in | out | vba | sh | MB |
|---|---|---:|---:|:--:|---:|---:|
| `?` | Commitment of Traders Sheets.xlsx | 0 | 0 |  | 15 | 1.0 |
| `L` | World Macro Economic and Population Data.xlsx | 2 | 0 |  | 12 | 0.5 |

## `models\Oilseeds/`  (10)

| | file | in | out | vba | sh | MB |
|---|---|---:|---:|:--:|---:|---:|
| `?` | _Pepsi_Coverage_Tracker.xlsx | 0 | 0 |  | 1 | 0.0 |
| `?` | cash_prices.xlsx | 0 | 0 |  | 4 | 0.0 |
| `?` | usdareports.xlsx | 0 | 0 |  | 1 | 0.1 |
| `?` | world_palm_trade.xlsm | 0 | 0 | Y | 14 | 1.1 |
| `?` | world_palm_trade.xlsx | 0 | 0 |  | 12 | 0.9 |
| `?` | world_rapeseed_trade.xlsx | 0 | 0 |  | 12 | 2.7 |
| `?` | world_soybean_trade.xlsx | 0 | 0 |  | 72 | 16.2 |
| `?` | world_sunflower_trade.xlsx | 0 | 0 |  | 6 | 1.3 |
| `B` | us_canola_oil_supply_demand.xlsx | 0 | 0 |  | 3 | 0.1 |
| `B` | us_soybean_oil_supply_demand.xlsx | 0 | 0 |  | 3 | 0.1 |

## `models\Oilseeds\Archive/`  (27)

| | file | in | out | vba | sh | MB |
|---|---|---:|---:|:--:|---:|---:|
| `A` | trade_sheets_template.xlsx | 0 | 0 |  | 6 | 3.1 |
| `A` | us_canola_balance_sheets.xlsx.bak_20260526_134653 | 3 | 0 |  | 4 | 0.3 |
| `A` | us_canola_balance_sheets.xlsx.bak_20260526_134708 | 3 | 0 |  | 4 | 0.2 |
| `A` | us_canola_balance_sheets.xlsx.bak_20260526_141046 | 3 | 0 |  | 4 | 0.2 |
| `A` | us_coconut_balance_sheets.xlsm.bak_20260527_122348 | 3 | 0 | Y | 4 | 0.2 |
| `A` | us_coconut_balance_sheets.xlsm.bak_20260527_122411 | 3 | 0 | Y | 5 | 0.1 |
| `A` | us_coconut_balance_sheets.xlsm.bak_20260527_130227 | 3 | 0 | Y | 5 | 0.1 |
| `A` | us_corn_oil_balance_sheets.xlsx.bak_20260526_134708 | 4 | 0 |  | 4 | 0.3 |
| `A` | us_corn_oil_balance_sheets.xlsx.bak_20260526_141033_revert | 4 | 0 |  | 4 | 0.2 |
| `A` | us_corn_oil_balance_sheets_01.xlsx | 5 | 0 |  | 4 | 0.3 |
| `A` | us_cottonseed_balance_sheets.xlsx.bak_20260526_134709 | 3 | 0 |  | 4 | 0.3 |
| `A` | us_cottonseed_balance_sheets.xlsx.bak_20260526_141047 | 3 | 0 |  | 4 | 0.2 |
| `A` | us_cottonseed_balance_sheets_01.xlsx | 4 | 0 |  | 4 | 0.3 |
| `A` | us_fats_oils_nass_data.xlsx | 0 | 0 |  | 1 | 0.0 |
| `A` | us_minor_oilseed_trade.xlsx | 0 | 0 |  | 8 | 4.1 |
| `A` | us_nass_crush_fats_oils_master.xlsx | 0 | 0 |  | 1 | 0.0 |
| `A` | us_oilseed_crush.backup_20260612_045932.xlsm | 1 | 0 | Y | 10 | 1.1 |
| `A` | us_oilseed_crushing_capacity.xlsm.bak-20260425-043956 | 0 | 0 | Y | 4 | 0.0 |
| `A` | us_palm_complex_balance_sheets.xlsm.bak_20260526_134709 | 4 | 0 | Y | 6 | 0.3 |
| `A` | us_palm_complex_balance_sheets.xlsm.bak_20260526_141047 | 4 | 0 | Y | 6 | 0.2 |
| `A` | us_palm_complex_balance_sheets.xlsm.bak_20260527_122349 | 5 | 0 | Y | 6 | 0.3 |
| `A` | us_palm_complex_balance_sheets.xlsm.bak_20260527_122412 | 5 | 0 | Y | 7 | 0.2 |
| `A` | us_palm_complex_balance_sheets.xlsm.bak_20260527_130228 | 5 | 0 | Y | 7 | 0.3 |
| `A` | us_peanut_bal_sheets.xlsm.bak_20260527_120807 | 3 | 0 | Y | 5 | 0.2 |
| `A` | us_peanut_bal_sheets.xlsm.bak_20260527_130135 | 3 | 0 | Y | 6 | 0.1 |
| `A` | us_sunflower_balance_sheets.xlsx.bak_20260526_134710 | 3 | 0 |  | 4 | 0.3 |
| `A` | us_sunflower_balance_sheets.xlsx.bak_20260526_141048 | 3 | 0 |  | 4 | 0.2 |

## `models\Oilseeds\Brazil/`  (3)

| | file | in | out | vba | sh | MB |
|---|---|---:|---:|:--:|---:|---:|
| `?` | brazil_conab_monthly.xlsx | 0 | 0 |  | 2 | 0.2 |
| `?` | brazil_soy_complex_monthly.xlsx | 0 | 0 |  | 2 | 0.0 |
| `B` | brazil_soybean_complex_bal_sheets.xlsx | 0 | 0 |  | 5 | 0.3 |

## `models\Oilseeds\United States/`  (31)

| | file | in | out | vba | sh | MB |
|---|---|---:|---:|:--:|---:|---:|
| `?` | us_canola_production.xlsx | 0 | 0 |  |  | 0.0 |
| `?` | us_flaxseed_production.xlsx | 0 | 0 |  |  | 0.0 |
| `?` | us_minor_oilseed_trade.xlsm | 0 | 0 |  |  | 0.0 |
| `?` | us_mustard_production.xlsx | 0 | 0 |  |  | 0.0 |
| `?` | us_oilseed_complex_trade.xlsm | 0 | 0 |  |  | 0.0 |
| `?` | us_oilseed_crushing_capacity.xlsm | 0 | 0 |  |  | 0.0 |
| `?` | us_oilseed_production.xlsx | 0 | 0 |  |  | 0.0 |
| `?` | us_peanut_production.xlsx | 0 | 0 |  |  | 0.0 |
| `?` | us_protein_meal_prices.xlsx | 0 | 0 |  |  | 0.0 |
| `?` | us_safflower_production.xlsx | 0 | 0 |  |  | 0.0 |
| `?` | us_sunflower_production.xlsx | 0 | 0 |  |  | 0.0 |
| `?` | us_veg_oil_prices.xlsx | 0 | 0 |  |  | 0.0 |
| `L` | us_oilseed_crush.xlsm | 1 | 1 |  |  | 0.0 |
| `L` | us_protein_meal_consumption.xlsx | 11 | 0 |  |  | 0.0 |
| `L` | us_soy_complex_trade.xlsm | 0 | 7 |  |  | 0.0 |
| `L` | us_soy_crush.xlsm | 0 | 7 |  |  | 0.0 |
| `L` | us_soybean_production.xlsx | 0 | 1 |  |  | 0.0 |
| `L` | us_veg_oil_domestic_consumption.xlsx | 12 | 0 |  |  | 0.0 |
| `B` | us_canola_balance_sheets.xlsx | 4 | 0 |  |  | 0.0 |
| `B` | us_canola_oil_supply_demand.xlsx | 0 | 0 |  |  | 0.0 |
| `B` | us_coconut_balance_sheets.xlsm | 4 | 0 |  |  | 0.0 |
| `B` | us_corn_oil_balance_sheets.xlsx | 4 | 0 |  |  | 0.0 |
| `B` | us_cottonseed_balance_sheets.xlsx | 4 | 0 |  |  | 0.0 |
| `B` | us_flaxseed_balance_sheets.xlsx | 2 | 0 |  |  | 0.0 |
| `B` | us_lauric_oils_bal_sheets.xlsm | 3 | 0 |  |  | 0.0 |
| `B` | us_palm_complex_balance_sheets.xlsm | 4 | 0 |  |  | 0.0 |
| `B` | us_peanut_bal_sheets.xlsm | 3 | 0 |  |  | 0.0 |
| `B` | us_safflower_balance_sheets.xlsx | 2 | 0 |  |  | 0.0 |
| `B` | us_soybean_complex_bal_sheets.xlsm | 5 | 1 |  |  | 0.0 |
| `B` | us_soybean_oil_supply_demand.xlsx | 0 | 1 |  |  | 0.0 |
| `B` | us_sunflower_balance_sheets.xlsx | 4 | 0 |  |  | 0.0 |

## `models\Population/`  (1)

| | file | in | out | vba | sh | MB |
|---|---|---:|---:|:--:|---:|---:|
| `?` | population_by_country.xlsx | 0 | 0 |  | 3 | 0.2 |

## `models\Spreadsheet Samples\Data Samples/`  (1)

| | file | in | out | vba | sh | MB |
|---|---|---:|---:|:--:|---:|---:|
| `?` | Report List.xlsx | 0 | 0 |  | 1 | 0.0 |

## `models\Spreadsheet Samples\Data Samples\Cross Commodity/`  (1)

| | file | in | out | vba | sh | MB |
|---|---|---:|---:|:--:|---:|---:|
| `?` | Food Expendatures - 0925.xlsx | 0 | 0 |  | 1 | 0.1 |

## `models\Spreadsheet Samples\Data Samples\Feed Grains/`  (3)

| | file | in | out | vba | sh | MB |
|---|---|---:|---:|:--:|---:|---:|
| `?` | US Corn Exports - 01201025.xlsx | 0 | 0 |  | 1 | 0.0 |
| `?` | US Corn Imports - 01201025.xlsx | 0 | 0 |  | 1 | 0.0 |
| `?` | US Feed Grains Outlook - Dec 25.xlsx | 0 | 0 |  | 36 | 0.9 |

## `models\Spreadsheet Samples\Data Samples\Oilseeds/`  (4)

| | file | in | out | vba | sh | MB |
|---|---|---:|---:|:--:|---:|---:|
| `?` | Misc Soy Trade Flow Pre Nov WASDE.xlsx | 0 | 0 |  | 1 | 0.0 |
| `?` | SBM Exports.xlsx | 0 | 0 |  | 1 | 0.0 |
| `?` | SBO Exports.xlsx | 0 | 0 |  | 1 | 0.0 |
| `?` | oiltables.xlsx | 0 | 0 |  | 12 | 0.2 |

## `models\Spreadsheet Samples\Data Samples\Wheat/`  (1)

| | file | in | out | vba | sh | MB |
|---|---|---:|---:|:--:|---:|---:|
| `?` | CGC October Exports.xlsx | 0 | 0 |  | 19 | 0.3 |

## `models\per_facility/`  (34)

| | file | in | out | vba | sh | MB |
|---|---|---:|---:|:--:|---:|---:|
| `?` | ia.adm_des_moines.xlsx | 0 | 0 |  | 10 | 0.1 |
| `?` | ia.ag_processing_eagle_grove.xlsx | 0 | 0 |  | 10 | 0.1 |
| `?` | ia.ag_processing_emmetsburg.xlsx | 0 | 0 |  | 10 | 0.1 |
| `?` | ia.ag_processing_manning.xlsx | 0 | 0 |  | 10 | 0.1 |
| `?` | ia.ag_processing_mason_city.xlsx | 0 | 0 |  | 10 | 0.1 |
| `?` | ia.ag_processing_sergeant_bluff.xlsx | 0 | 0 |  | 10 | 0.1 |
| `?` | ia.ag_processing_sheldon.xlsx | 0 | 0 |  | 10 | 0.1 |
| `?` | ia.agp_algona.xlsx | 0 | 0 |  | 10 | 0.1 |
| `?` | ia.agp_eagle_grove.xlsx | 0 | 0 |  | 10 | 0.1 |
| `?` | ia.agp_emmetsburg.xlsx | 0 | 0 |  | 10 | 0.1 |
| `?` | ia.agp_manning.xlsx | 0 | 0 |  | 10 | 0.1 |
| `?` | ia.agp_mason_city.xlsx | 0 | 0 |  | 10 | 0.1 |
| `?` | ia.agp_sergeant_bluff.xlsx | 0 | 0 |  | 10 | 0.1 |
| `?` | ia.agp_sheldon.xlsx | 0 | 0 |  | 10 | 0.1 |
| `?` | ia.bunge_council_bluffs.xlsx | 0 | 0 |  | 10 | 0.1 |
| `?` | ia.cargill_cedar_rapids_east.xlsx | 0 | 0 |  | 10 | 0.1 |
| `?` | ia.cargill_cedar_rapids_v006r3.xlsx | 0 | 0 |  | 10 | 0.1 |
| `?` | ia.cargill_cedar_rapids_v010r3.xlsx | 0 | 0 |  | 10 | 0.1 |
| `?` | ia.cargill_cedar_rapids_v044r4.xlsx | 0 | 0 |  | 10 | 0.1 |
| `?` | ia.cargill_cedar_rapids_west.xlsx | 0 | 0 |  | 10 | 0.1 |
| `?` | ia.cargill_des_moines.xlsx | 0 | 0 |  | 10 | 0.1 |
| `?` | ia.cargill_eddyville_v004r3.xlsx | 0 | 0 |  | 10 | 0.1 |
| `?` | ia.cargill_eddyville_v006r1.xlsx | 0 | 0 |  | 10 | 0.1 |
| `?` | ia.cargill_fort_dodge.xlsx | 0 | 0 |  | 10 | 0.1 |
| `?` | ia.cargill_iowa_falls.xlsx | 0 | 0 |  | 10 | 0.1 |
| `?` | ia.cargill_sioux_city.xlsx | 0 | 0 |  | 10 | 0.1 |
| `?` | ia.cf_processing_creston.xlsx | 0 | 0 |  | 10 | 0.1 |
| `?` | ia.maple_river_energy_llc_galva.xlsx | 0 | 0 |  | 10 | 0.1 |
| `?` | ia.platinum_alta.xlsx | 0 | 0 |  | 10 | 0.1 |
| `?` | ia.shell_rock_shell_rock.xlsx | 0 | 0 |  | 10 | 0.1 |
| `?` | ia.soy_energy_llc_marcus.xlsx | 0 | 0 |  | 10 | 0.1 |
| `?` | ia.tri_city_energy_llc_keokuk.xlsx | 0 | 0 |  | 10 | 0.1 |
| `?` | ia.west_central_cooperative_ralston.xlsx | 0 | 0 |  | 10 | 0.1 |
| `?` | ia.white_river_creston.xlsx | 0 | 0 |  | 10 | 0.1 |

## `models\templates/`  (1)

| | file | in | out | vba | sh | MB |
|---|---|---:|---:|:--:|---:|---:|
| `?` | per_facility_profitability_v1.xlsx | 0 | 0 |  | 10 | 0.1 |
