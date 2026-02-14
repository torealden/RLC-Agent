# EIA Series ID Reference for Biofuels and Petroleum Market Analysis

The U.S. Energy Information Administration provides **over 150 relevant series IDs** covering fuel ethanol, biodiesel, renewable diesel, gasoline, diesel, natural gas, and related commodities. This reference compiles the complete inventory for commodity market analysis, organized for data collection systems serving food grain, feed grain, oilseed, livestock, and biofuel clients. Critical finding: while EIA excels at petroleum and biofuel production tracking, **feedstock inputs like corn for ethanol must be sourced from USDA**, representing a significant data architecture consideration.

---

## EIA API v2 structure and access methods

The EIA Open Data API v2 (current version 2.1.10) requires a free API key from eia.gov/opendata and uses the base URL `https://api.eia.gov/v2/`. Series IDs follow a structured naming convention: **[PREFIX].[PRODUCT_CODE]_[ACTIVITY]_[GEOGRAPHY]_[UNITS].[FREQUENCY]**. The prefix denotes data category (PET=Petroleum, NG=Natural Gas, ELEC=Electricity), while frequency suffixes indicate periodicity (D=daily, W=weekly, M=monthly, A=annual).

Two access methods exist: the **legacy series ID translation** at `/v2/seriesid/[SERIES_ID]?api_key=YOUR_KEY` and the newer **hierarchical route system** such as `/v2/petroleum/sum/sndw/data` for weekly petroleum supply data. Key query parameters include `data[]=value` for data columns, `facets[field][]=value` for filtering, `frequency=weekly|monthly|annual`, and `start/end` dates in RFC 3339 format. Maximum return is 5,000 rows per request with pagination via `offset`.

### Primary API routes for biofuels and petroleum

| Route | Description |
|-------|-------------|
| `/v2/petroleum/sum/sndw/` | Weekly petroleum supply summary (includes ethanol) |
| `/v2/petroleum/sum/snd/` | Monthly supply and disposition |
| `/v2/petroleum/stoc/wstk/` | Weekly stocks |
| `/v2/petroleum/move/exp/` and `/imp/` | Exports and imports |
| `/v2/petroleum/pnp/wprode/` | Weekly ethanol production |
| `/v2/natural-gas/pri/sum/` | Natural gas price summary |
| `/v2/natural-gas/stor/sum/` | Natural gas storage |

---

## Fuel ethanol series IDs

Ethanol data resides in the petroleum section using product code **EPOOXE**. Weekly data derives from the Weekly Petroleum Status Report (Wednesdays); monthly data from Petroleum Supply Monthly with ~60-day lag.

### Weekly production and stocks

| Series ID | Description | Units |
|-----------|-------------|-------|
| `PET.W_EPOOXE_YOP_NUS_MBBLD.W` | U.S. Fuel Ethanol Plant Production | Thousand Barrels/Day |
| `PET.W_EPOOXE_YOP_R10_MBBLD.W` | PADD 1 (East Coast) Production | Thousand Barrels/Day |
| `PET.W_EPOOXE_YOP_R20_MBBLD.W` | PADD 2 (Midwest) Production | Thousand Barrels/Day |
| `PET.W_EPOOXE_YOP_R30_MBBLD.W` | PADD 3 (Gulf Coast) Production | Thousand Barrels/Day |
| `PET.W_EPOOXE_YOP_R40_MBBLD.W` | PADD 4 (Rocky Mountain) Production | Thousand Barrels/Day |
| `PET.W_EPOOXE_YOP_R50_MBBLD.W` | PADD 5 (West Coast) Production | Thousand Barrels/Day |
| `PET.W_EPOOXE_SAE_NUS_MBBL.W` | U.S. Ending Stocks | Thousand Barrels |
| `PET.W_EPOOXE_SAE_R10_MBBL.W` | PADD 1 Ending Stocks | Thousand Barrels |
| `PET.W_EPOOXE_SAE_R20_MBBL.W` | PADD 2 Ending Stocks | Thousand Barrels |
| `PET.W_EPOOXE_SAE_R30_MBBL.W` | PADD 3 Ending Stocks | Thousand Barrels |
| `PET.W_EPOOXE_SAE_R40_MBBL.W` | PADD 4 Ending Stocks | Thousand Barrels |
| `PET.W_EPOOXE_SAE_R50_MBBL.W` | PADD 5 Ending Stocks | Thousand Barrels |

### Monthly production, consumption, and trade

| Series ID | Description | Frequency | Units |
|-----------|-------------|-----------|-------|
| `PET.M_EPOOXE_YOP_NUS_1.M` | U.S. Oxygenate Plant Production | Monthly | Thousand Barrels |
| `PET.M_EPOOXE_YNP_NUS_MBBL.M` | Biofuels Plant Net Production | Monthly | Thousand Barrels |
| `PET.MFESTUS1.M` | U.S. Ending Stocks | Monthly | Thousand Barrels |
| `PET.M_EPOOXE_VPP_NUS_MBBL.M` | U.S. Product Supplied (consumption proxy) | Monthly | Thousand Barrels |
| `PET.M_EPOOXE_VPP_NUS_MBBLD.M` | Product Supplied | Monthly | Thousand Barrels/Day |
| `PET.MFERIUS1.M` | Refinery & Blender Net Inputs | Monthly | Thousand Barrels |
| `PET.M_EPOOXE_EEX_NUS-Z00_MBBL.M` | U.S. Exports | Monthly | Thousand Barrels |
| `PET.M_EPOOXE_EEX_NUS-Z00_MBBLD.M` | U.S. Exports | Monthly | Thousand Barrels/Day |
| `PET.MFEIMUS1.M` | U.S. Imports | Monthly | Thousand Barrels |
| `PET.MFEIMUS2.A` | U.S. Imports | Annual | Thousand Barrels/Day |

### Inter-PADD movements (net receipts)

| Series ID | Description |
|-----------|-------------|
| `PET.M_EPOOXE_VNR_R10-Z0P_MBBL.M` | PADD 1 Net Receipts |
| `PET.M_EPOOXE_VNR_R20-Z0P_MBBL.M` | PADD 2 Net Receipts |
| `PET.M_EPOOXE_VNR_R30-Z0P_MBBL.M` | PADD 3 Net Receipts |
| `PET.M_EPOOXE_VNR_R40-Z0P_MBBL.M` | PADD 4 Net Receipts |
| `PET.M_EPOOXE_VNR_R50-Z0P_MBBL.M` | PADD 5 Net Receipts |

**Important note:** EIA does not publish dedicated fuel ethanol price series. Ethanol prices must be obtained from CBOT futures or OPIS.

---

## Biodiesel and renewable diesel series IDs

Biodiesel uses product code **EPOORDB** while renewable diesel uses **EPOORDO**. Combined biodiesel/renewable diesel data uses **EPOORD**. Data collected via Form EIA-819.

### Biodiesel production

| Series ID | Description | Frequency | Units |
|-----------|-------------|-----------|-------|
| `PET.M_EPOORDB_YNP_NUS_MBBL.M` | U.S. Biodiesel Net Production | Monthly | Thousand Barrels |
| `PET.M_EPOORDB_YNP_R10_MBBL.M` | PADD 1 Biodiesel Production | Monthly | Thousand Barrels |
| `PET.M_EPOORDB_YNP_R20_MBBL.M` | PADD 2 Biodiesel Production | Monthly | Thousand Barrels |
| `PET.M_EPOORDB_YNP_R30_MBBL.M` | PADD 3 Biodiesel Production | Monthly | Thousand Barrels |
| `PET.M_EPOORDB_YNP_R40_MBBL.M` | PADD 4 Biodiesel Production | Monthly | Thousand Barrels |
| `PET.M_EPOORDB_YNP_R50_MBBL.M` | PADD 5 Biodiesel Production | Monthly | Thousand Barrels |

### Biodiesel trade

| Series ID | Description | Units |
|-----------|-------------|-------|
| `PET.M_EPOORDB_IM0_NUS-Z00_MBBL.M` | Biodiesel Imports (All Countries) | Thousand Barrels |
| `PET.M_EPOORDB_IM0_NUS-NCA_MBBL.M` | Biodiesel Imports from Canada | Thousand Barrels |
| `PET.M_EPOORDB_EEX_NUS-Z00_MBBL.M` | Biodiesel Exports (All Countries) | Thousand Barrels |
| `PET.M_EPOORDB_EEX_NUS-NCA_MBBL.M` | Biodiesel Exports to Canada | Thousand Barrels |

### Combined biodiesel/renewable diesel blending and stocks

| Series ID | Description | Geography | Units |
|-----------|-------------|-----------|-------|
| `PET.M_EPOORD_YIR_NUS_MBBL.M` | Refinery & Blenders Net Input | U.S. | Thousand Barrels |
| `PET.M_EPOORD_YIR_R10_MBBL.M` | Refinery & Blenders Net Input | PADD 1 | Thousand Barrels |
| `PET.M_EPOORD_YIR_R20_MBBL.M` | Refinery & Blenders Net Input | PADD 2 | Thousand Barrels |
| `PET.M_EPOORD_YIR_R30_MBBL.M` | Refinery & Blenders Net Input | PADD 3 | Thousand Barrels |
| `PET.M_EPOORD_YIR_R50_MBBL.M` | Refinery & Blenders Net Input | PADD 5 | Thousand Barrels |
| `PET.M_EPOORD_SAE_NUS_MBBL.M` | Total Ending Stocks | U.S. | Thousand Barrels |
| `PET.M_EPOORD_SKR_NUS_MBBL.M` | Stocks at Refineries | U.S. | Thousand Barrels |

### Renewable diesel-specific stocks

| Series ID | Description | Units |
|-----------|-------------|-------|
| `PET.M_EPOORDO_SAE_NUS_MBBL.M` | Renewable Diesel Total Stocks | Thousand Barrels |
| `PET.M_EPOORDO_SKR_NUS_MBBL.M` | Renewable Diesel Refinery Stocks | Thousand Barrels |
| `PET.M_EPOORDO_SKB_NUS_MBBL.M` | Renewable Diesel Bulk Terminal Stocks | Thousand Barrels |
| `PET.M_EPOORDO_SKP_NUS_MBBL.M` | Renewable Diesel Pipeline Stocks | Thousand Barrels |

### State Energy Data System (SEDS) biofuels codes

| SEDS Code | Description |
|-----------|-------------|
| BDPRPUS | Biodiesel production, U.S. total |
| BDPRP[XX] | Biodiesel production by state (XX=state code) |
| B1PRPUS | Renewable diesel production, U.S. total |
| B1PRP[XX] | Renewable diesel production by state |

---

## Sustainable aviation fuel data limitations

**SAF does not have dedicated EIA series IDs.** SAF production and consumption are aggregated within the "Other Biofuels" category in Monthly Energy Review Table 10.4c, which combines renewable jet fuel, renewable heating oil, renewable naphtha/gasoline, and biobutanol. Annual plant-level SAF capacity is available in the "U.S. Renewable Diesel Fuel and Other Biofuels Plant Production Capacity" report. For SAF-specific volumes, alternative sources include EPA RFS RIN transaction data (approximately **15.8 million gallons** SAF generated in 2022) and California LCFS data for state-level SAF tracking.

---

## Gasoline series IDs

### Production

| Series ID | Frequency | Description | Units |
|-----------|-----------|-------------|-------|
| `PET.WGFRPUS2.W` | Weekly | U.S. Finished Motor Gasoline Production | Thousand Barrels/Day |
| `PET.MGFRPUS2.M` | Monthly | U.S. Finished Motor Gasoline Production | Thousand Barrels/Day |
| `PET.WGFRPP11.W` | Weekly | PADD 1 Production | Thousand Barrels/Day |
| `PET.WGFRPP21.W` | Weekly | PADD 2 Production | Thousand Barrels/Day |
| `PET.WGFRPP31.W` | Weekly | PADD 3 Production | Thousand Barrels/Day |
| `PET.WGFRPP41.W` | Weekly | PADD 4 Production | Thousand Barrels/Day |
| `PET.WGFRPP51.W` | Weekly | PADD 5 Production | Thousand Barrels/Day |

### Stocks (most-watched series)

| Series ID | Frequency | Description | Units |
|-----------|-----------|-------------|-------|
| `PET.WGTSTUS1.W` | Weekly | **U.S. Total Gasoline Stocks** | Thousand Barrels |
| `PET.WGFSTUS1.W` | Weekly | U.S. Finished Motor Gasoline Stocks | Thousand Barrels |
| `PET.WBCSTUS1.W` | Weekly | U.S. Gasoline Blending Components Stocks | Thousand Barrels |
| `PET.WGTSTP11.W` | Weekly | PADD 1 (East Coast) Total Stocks | Thousand Barrels |
| `PET.WGTSTP21.W` | Weekly | PADD 2 (Midwest) Total Stocks | Thousand Barrels |
| `PET.WGTSTP31.W` | Weekly | PADD 3 (Gulf Coast) Total Stocks | Thousand Barrels |
| `PET.WGTSTP41.W` | Weekly | PADD 4 (Rocky Mountain) Total Stocks | Thousand Barrels |
| `PET.WGTSTP51.W` | Weekly | PADD 5 (West Coast) Total Stocks | Thousand Barrels |
| `PET.WGTST1A1.W` | Weekly | New England (Sub-PADD 1A) Stocks | Thousand Barrels |
| `PET.WGTST1B1.W` | Weekly | Central Atlantic (Sub-PADD 1B) Stocks | Thousand Barrels |
| `PET.WGTST1C1.W` | Weekly | Lower Atlantic (Sub-PADD 1C) Stocks | Thousand Barrels |

### Consumption (product supplied)

| Series ID | Frequency | Description | Units |
|-----------|-----------|-------------|-------|
| `PET.WGFUPUS2.W` | Weekly | **U.S. Finished Motor Gasoline Product Supplied** | Thousand Barrels/Day |
| `PET.MGFUPUS2.M` | Monthly | U.S. Finished Motor Gasoline Product Supplied | Thousand Barrels/Day |
| `PET.WGTUPUS2.W` | Weekly | U.S. Total Motor Gasoline Product Supplied | Thousand Barrels/Day |

### Retail prices

| Series ID | Frequency | Description | Units |
|-----------|-----------|-------------|-------|
| `PET.EMM_EPM0_PTE_NUS_DPG.W` | Weekly | U.S. All Grades All Formulations Retail | $/Gallon |
| `PET.EMM_EPM0_PTE_R10_DPG.W` | Weekly | PADD 1 All Grades Retail | $/Gallon |
| `PET.EMM_EPM0_PTE_R20_DPG.W` | Weekly | PADD 2 All Grades Retail | $/Gallon |
| `PET.EMM_EPM0_PTE_R30_DPG.W` | Weekly | PADD 3 All Grades Retail | $/Gallon |
| `PET.EMM_EPM0_PTE_R40_DPG.W` | Weekly | PADD 4 All Grades Retail | $/Gallon |
| `PET.EMM_EPM0_PTE_R50_DPG.W` | Weekly | PADD 5 All Grades Retail | $/Gallon |
| `PET.EMM_EPMR_PTE_NUS_DPG.W` | Weekly | U.S. Regular Grade Retail | $/Gallon |
| `PET.EMM_EPMM_PTE_NUS_DPG.W` | Weekly | U.S. Midgrade Retail | $/Gallon |
| `PET.EMM_EPMP_PTE_NUS_DPG.W` | Weekly | U.S. Premium Grade Retail | $/Gallon |

### Spot prices

| Series ID | Frequency | Description | Units |
|-----------|-----------|-------------|-------|
| `PET.EER_EPMRU_PF4_Y35NY_DPG.D` | Daily | **NY Harbor Conventional Regular Spot FOB** | $/Gallon |
| `PET.EER_EPMRU_PF4_RGC_DPG.D` | Daily | **Gulf Coast Conventional Regular Spot FOB** | $/Gallon |
| `PET.EER_EPMRR_PF4_Y05LA_DPG.D` | Daily | Los Angeles RBOB Regular Spot FOB | $/Gallon |

### Imports and exports

| Series ID | Frequency | Description | Units |
|-----------|-----------|-------------|-------|
| `PET.WGTIMUS2.W` | Weekly | U.S. Total Gasoline Imports | Thousand Barrels/Day |
| `PET.WGFIMUS2.W` | Weekly | U.S. Finished Motor Gasoline Imports | Thousand Barrels/Day |
| `PET.WGTEXUS2.W` | Weekly | U.S. Total Gasoline Exports | Thousand Barrels/Day |
| `PET.WGFEXUS2.W` | Weekly | U.S. Finished Motor Gasoline Exports | Thousand Barrels/Day |
| `PET.MGTIMUS1.M` | Monthly | U.S. Total Gasoline Imports | Thousand Barrels |

---

## Diesel and distillate fuel oil series IDs

### Production

| Series ID | Frequency | Description | Units |
|-----------|-----------|-------------|-------|
| `PET.WDIRPUS2.W` | Weekly | U.S. Distillate Fuel Oil Production | Thousand Barrels/Day |
| `PET.MDIRPUS2.M` | Monthly | U.S. Distillate Fuel Oil Production | Thousand Barrels/Day |
| `PET.WDIRPP11.W` | Weekly | PADD 1 Production | Thousand Barrels/Day |
| `PET.WDIRPP21.W` | Weekly | PADD 2 Production | Thousand Barrels/Day |
| `PET.WDIRPP31.W` | Weekly | PADD 3 Production | Thousand Barrels/Day |
| `PET.WDIRPP41.W` | Weekly | PADD 4 Production | Thousand Barrels/Day |
| `PET.WDIRPP51.W` | Weekly | PADD 5 Production | Thousand Barrels/Day |

### Stocks

| Series ID | Frequency | Description | Units |
|-----------|-----------|-------------|-------|
| `PET.WDISTUS1.W` | Weekly | **U.S. Total Distillate Fuel Oil Stocks** | Thousand Barrels |
| `PET.MDISTUS1.M` | Monthly | U.S. Total Distillate Stocks | Thousand Barrels |
| `PET.WDISTP11.W` | Weekly | PADD 1 Stocks | Thousand Barrels |
| `PET.WDISTP21.W` | Weekly | PADD 2 Stocks | Thousand Barrels |
| `PET.WDISTP31.W` | Weekly | PADD 3 Stocks | Thousand Barrels |
| `PET.WDISTP41.W` | Weekly | PADD 4 Stocks | Thousand Barrels |
| `PET.WDISTP51.W` | Weekly | PADD 5 Stocks | Thousand Barrels |
| `PET.WDIST1A1.W` | Weekly | New England (Sub-PADD 1A) Stocks | Thousand Barrels |
| `PET.WDIST1B1.W` | Weekly | Central Atlantic (Sub-PADD 1B) Stocks | Thousand Barrels |
| `PET.WDIST1C1.W` | Weekly | Lower Atlantic (Sub-PADD 1C) Stocks | Thousand Barrels |

### Ultra-low sulfur diesel (ULSD) stocks by sulfur content

| Series ID | Description | Units |
|-----------|-------------|-------|
| `PET.WD0ST_NUS_1.W` | ULSD Stocks (≤15 ppm sulfur) | Thousand Barrels |
| `PET.WD10ST_NUS_1.W` | Distillate Stocks (>15 to 500 ppm sulfur) | Thousand Barrels |
| `PET.WD50ST_NUS_1.W` | Distillate Stocks (>500 ppm sulfur) | Thousand Barrels |

### Consumption

| Series ID | Frequency | Description | Units |
|-----------|-----------|-------------|-------|
| `PET.WDIUPUS2.W` | Weekly | **U.S. Distillate Product Supplied** | Thousand Barrels/Day |
| `PET.MDIUPUS2.M` | Monthly | U.S. Distillate Product Supplied | Thousand Barrels/Day |

### Prices

| Series ID | Frequency | Description | Units |
|-----------|-----------|-------------|-------|
| `PET.EMD_EPD0_PTE_NUS_DPG.W` | Weekly | **U.S. On-Highway Diesel Retail Price** | $/Gallon |
| `PET.EMD_EPD0_PTE_R10_DPG.W` | Weekly | PADD 1 On-Highway Diesel Retail | $/Gallon |
| `PET.EMD_EPD0_PTE_R20_DPG.W` | Weekly | PADD 2 On-Highway Diesel Retail | $/Gallon |
| `PET.EMD_EPD0_PTE_R30_DPG.W` | Weekly | PADD 3 On-Highway Diesel Retail | $/Gallon |
| `PET.EMD_EPD0_PTE_R40_DPG.W` | Weekly | PADD 4 On-Highway Diesel Retail | $/Gallon |
| `PET.EMD_EPD0_PTE_R50_DPG.W` | Weekly | PADD 5 On-Highway Diesel Retail | $/Gallon |
| `PET.EER_EPD2DXL0_PF4_Y35NY_DPG.D` | Daily | **NY Harbor ULSD Spot Price FOB** | $/Gallon |
| `PET.EER_EPD2DXL0_PF4_RGC_DPG.D` | Daily | **Gulf Coast ULSD Spot Price FOB** | $/Gallon |
| `PET.EER_EPD2DC_PF4_Y05LA_DPG.D` | Daily | Los Angeles ULSD Spot Price FOB | $/Gallon |

### Trade

| Series ID | Frequency | Description | Units |
|-----------|-----------|-------------|-------|
| `PET.WDIIMUS2.W` | Weekly | U.S. Distillate Imports | Thousand Barrels/Day |
| `PET.WDIEXUS2.W` | Weekly | U.S. Distillate Exports | Thousand Barrels/Day |
| `PET.MDIIMUS1.M` | Monthly | U.S. Distillate Imports | Thousand Barrels |
| `PET.MDIEXUS1.M` | Monthly | U.S. Distillate Exports | Thousand Barrels |

---

## Naphtha series IDs

Naphtha data is limited; EIA tracks primarily "Naphtha for Petrochemical Feedstock Use." **Renewable naphtha is NOT separately tracked.**

| Series ID | Frequency | Description | Units |
|-----------|-----------|-------------|-------|
| `PET.MPNRX_NUS_1.M` | Monthly | U.S. Refinery Net Production of Naphtha | Thousand Barrels |
| `PET.MNFSTUS1.M` | Monthly | U.S. Naphtha Total Stocks | Thousand Barrels |
| `PET.MNFRSUS1.M` | Monthly | U.S. Naphtha Refinery Stocks | Thousand Barrels |
| `PET.MNFUPUS2.M` | Monthly | U.S. Naphtha Product Supplied | Thousand Barrels/Day |

---

## Natural gas series IDs

Natural gas series use the **NG prefix** with format `NG.[SERIES_CODE].[FREQUENCY]`.

### Henry Hub spot and futures prices

| Series ID | Frequency | Description | Units |
|-----------|-----------|-------------|-------|
| `NG.RNGWHHD.D` | Daily | **Henry Hub Natural Gas Spot Price** | $/MMBtu |
| `NG.RNGWHHD.W` | Weekly | Henry Hub Spot Price (Weekly Avg) | $/MMBtu |
| `NG.RNGWHHD.M` | Monthly | Henry Hub Spot Price (Monthly Avg) | $/MMBtu |
| `NG.RNGWHHD.A` | Annual | Henry Hub Spot Price (Annual Avg) | $/MMBtu |
| `NG.RNGC1.D` | Daily | Natural Gas Futures Contract 1 | $/MMBtu |
| `NG.RNGC2.D` | Daily | Natural Gas Futures Contract 2 | $/MMBtu |
| `NG.RNGC3.D` | Daily | Natural Gas Futures Contract 3 | $/MMBtu |
| `NG.RNGC4.D` | Daily | Natural Gas Futures Contract 4 | $/MMBtu |

### Production

| Series ID | Frequency | Description | Units |
|-----------|-----------|-------------|-------|
| `NG.N9010US2.M` | Monthly | U.S. Gross Withdrawals | Million Cubic Feet |
| `NG.N9050US2.M` | Monthly | U.S. Marketed Production | Million Cubic Feet |
| `NG.N9070US2.M` | Monthly | **U.S. Dry Natural Gas Production** | Million Cubic Feet |
| `NG.N9060US2.M` | Monthly | NGPL Production, Gaseous Equivalent | Million Cubic Feet |
| `NG.N9010US2.A` | Annual | U.S. Gross Withdrawals | Million Cubic Feet |
| `NG.N9050US2.A` | Annual | U.S. Marketed Production | Million Cubic Feet |

State-level production: Replace "US" with state code (TX, PA, LA, OK, etc.)
- `NG.N9010TX2.M` = Texas Gross Withdrawals
- `NG.N9050PA2.M` = Pennsylvania Marketed Production

### Consumption by sector (relevant to fertilizer/ammonia inputs)

| Series ID | Frequency | Description | Units |
|-----------|-----------|-------------|-------|
| `NG.N9140US2.M` | Monthly | **Total U.S. Natural Gas Consumption** | Million Cubic Feet |
| `NG.N3010US2.M` | Monthly | Residential Consumption | Million Cubic Feet |
| `NG.N3020US2.M` | Monthly | Commercial Consumption | Million Cubic Feet |
| `NG.N3035US2.M` | Monthly | **Industrial Consumption** (includes fertilizer) | Million Cubic Feet |
| `NG.N3045US2.M` | Monthly | Electric Power Consumption | Million Cubic Feet |
| `NG.N3025US2.M` | Monthly | Vehicle Fuel Consumption | Million Cubic Feet |
| `NG.N9160US2.M` | Monthly | Lease and Plant Fuel | Million Cubic Feet |
| `NG.N9170US2.M` | Monthly | Pipeline & Distribution Use | Million Cubic Feet |

**For fertilizer/ammonia-specific context:** Industrial natural gas consumption (`NG.N3035US2.M`) includes feedstock for ammonia/fertilizer production. Key state series for major ammonia producing regions:
- `NG.N3035TX2.M` – Texas Industrial Consumption
- `NG.N3035LA2.M` – Louisiana Industrial Consumption
- `NG.N3035OK2.M` – Oklahoma Industrial Consumption

Note: EIA does not separately break out natural gas consumption specifically for ammonia production.

### Prices by sector

| Series ID | Frequency | Description | Units |
|-----------|-----------|-------------|-------|
| `NG.N3050US3.M` | Monthly | Citygate Price | $/Mcf |
| `NG.N3010US3.M` | Monthly | Residential Price | $/Mcf |
| `NG.N3020US3.M` | Monthly | Commercial Price | $/Mcf |
| `NG.N3035US3.M` | Monthly | **Industrial Price** | $/Mcf |
| `NG.N3045US3.M` | Monthly | Electric Power Price | $/Mcf |

### Storage

| Series ID | Frequency | Description | Units |
|-----------|-----------|-------------|-------|
| `NG.NW2_EPG0_SWO_R48_BCF.W` | Weekly | **Working Gas in Storage (Lower 48)** | Bcf |
| `NG.N5020US2.M` | Monthly | Working Gas in Storage | Million Cubic Feet |
| `NG.N5010US2.M` | Monthly | Base Gas | Million Cubic Feet |
| `NG.N5030US2.M` | Monthly | Total Gas in Storage | Million Cubic Feet |
| `NG.N5050US2.M` | Monthly | Storage Injections | Million Cubic Feet |
| `NG.N5060US2.M` | Monthly | Storage Withdrawals | Million Cubic Feet |
| `NG.N5290US2.M` | Monthly | Total Storage Capacity | Million Cubic Feet |

### Trade (LNG and pipeline)

| Series ID | Frequency | Description | Units |
|-----------|-----------|-------------|-------|
| `NG.N9100US2.M` | Monthly | Total Imports (Volume) | Million Cubic Feet |
| `NG.N9102US2.M` | Monthly | Pipeline Imports | Million Cubic Feet |
| `NG.N9103US2.M` | Monthly | LNG Imports | Million Cubic Feet |
| `NG.N9130US2.M` | Monthly | Total Exports | Million Cubic Feet |
| `NG.N9132US2.M` | Monthly | Pipeline Exports | Million Cubic Feet |
| `NG.N9133US2.M` | Monthly | **LNG Exports** | Million Cubic Feet |
| `NG.N9100US3.M` | Monthly | Import Price | $/Mcf |
| `NG.N9130US3.M` | Monthly | Export Price | $/Mcf |
| `NG.N9133US3.M` | Monthly | LNG Export Price | $/Mcf |

---

## Propane and LPG series IDs

Propane is critical for agricultural drying and rural fuel uses.

### Production

| Series ID | Frequency | Description | Units |
|-----------|-----------|-------------|-------|
| `PET.WPRFPUS2.W` | Weekly | U.S. Refinery Production of Propane | Thousand Barrels/Day |
| `PET.MPRFPUS2.M` | Monthly | U.S. Refinery & Blender Net Production | Thousand Barrels/Day |
| `PET.MPLNPUS2.M` | Monthly | Natural Gas Plant Propane Production | Thousand Barrels/Day |

### Stocks

| Series ID | Frequency | Description | Units |
|-----------|-----------|-------------|-------|
| `PET.W_EPLLPZ_SAE_NUS_MBBL.W` | Weekly | **U.S. Propane/Propylene Total Stocks** | Thousand Barrels |
| `PET.WPRSTUS1.W` | Weekly | U.S. Propane Stocks (Excl. Propylene) | Thousand Barrels |
| `PET.W_EPLLPZ_SAE_R10_MBBL.W` | Weekly | PADD 1 (East Coast) Stocks | Thousand Barrels |
| `PET.W_EPLLPZ_SAE_R20_MBBL.W` | Weekly | **PADD 2 (Midwest) Stocks** | Thousand Barrels |
| `PET.W_EPLLPZ_SAE_R30_MBBL.W` | Weekly | PADD 3 (Gulf Coast) Stocks | Thousand Barrels |
| `PET.W_EPLLPZ_SAE_R4N5_MBBL.W` | Weekly | PADDs 4&5 Stocks | Thousand Barrels |
| `PET.W_EPLLPZ_SAE_R1X_MBBL.W` | Weekly | New England (PADD 1A) Stocks | Thousand Barrels |
| `PET.MPRST_NUS_1.M` | Monthly | U.S. Monthly Propane Stocks | Thousand Barrels |

### Prices

| Series ID | Frequency | Description | Units |
|-----------|-----------|-------------|-------|
| `PET.EER_EPLLPA_PF4_Y44MB_DPG.D` | Daily | **Mont Belvieu, TX Propane Spot Price** | $/Gallon |
| `PET.EER_EPLLPA_PF4_Y44MB_DPG.W` | Weekly | Mont Belvieu Propane Spot (Weekly) | $/Gallon |
| `PET.W_EPLLPA_PRS_NUS_DPG.W` | Weekly | **U.S. Residential Propane Price** | $/Gallon |
| `PET.M_EPLLPA_PRS_NUS_DPG.M` | Monthly | U.S. Residential Propane Price | $/Gallon |
| `PET.W_EPLLPA_PWR_NUS_DPG.W` | Weekly | U.S. Wholesale Propane Price | $/Gallon |
| `PET.W_EPLLPA_PRS_R20_DPG.W` | Weekly | **Midwest (PADD 2) Residential Propane** | $/Gallon |
| `PET.W_EPLLPA_PRS_SMN_DPG.W` | Weekly | Minnesota Residential Propane | $/Gallon |
| `PET.W_EPLLPA_PRS_SWI_DPG.W` | Weekly | Wisconsin Residential Propane | $/Gallon |
| `PET.W_EPLLPA_PRS_SIA_DPG.W` | Weekly | Iowa Residential Propane | $/Gallon |

Note: Regional residential propane prices collected October–March only (heating season).

### Consumption and trade

| Series ID | Frequency | Description | Units |
|-----------|-----------|-------------|-------|
| `PET.WPRUP_NUS_4.W` | Weekly | U.S. Propane Product Supplied | Thousand Barrels/Day |
| `PET.MPRUP_NUS_2.M` | Monthly | U.S. Propane Product Supplied | Thousand Barrels/Day |
| `PET.MPLIM_NUS_1.M` | Monthly | Propane Imports | Thousand Barrels/Day |
| `PET.WPREXUS2.W` | Weekly | Propane Exports | Thousand Barrels/Day |
| `PET.MPLEXP_NUS_2.M` | Monthly | Propane Exports | Thousand Barrels/Day |

---

## Heating oil series IDs

### Stocks

| Series ID | Frequency | Description | Units |
|-----------|-----------|-------------|-------|
| `PET.W_EPD0_SAE_NUS_MBBL.W` | Weekly | U.S. Total Distillate Stocks | Thousand Barrels |
| `PET.W_EPD0_SAE_R10_MBBL.W` | Weekly | PADD 1 (East Coast) Distillate Stocks | Thousand Barrels |
| `PET.W_EPD0_SAE_R1X_MBBL.W` | Weekly | **New England (PADD 1A) Distillate Stocks** | Thousand Barrels |
| `PET.W_EPD0_SAE_R1Y_MBBL.W` | Weekly | Central Atlantic (PADD 1B) Distillate Stocks | Thousand Barrels |
| `PET.W_EPD0_SAE_R1Z_MBBL.W` | Weekly | Lower Atlantic (PADD 1C) Distillate Stocks | Thousand Barrels |

### Prices

| Series ID | Frequency | Description | Units |
|-----------|-----------|-------------|-------|
| `PET.EER_EPD2F_PF4_Y35NY_DPG.D` | Daily | **NY Harbor No. 2 Heating Oil Spot Price** | $/Gallon |
| `PET.EER_EPD2F_PF4_Y35NY_DPG.W` | Weekly | NY Harbor Heating Oil Spot (Weekly) | $/Gallon |
| `PET.EER_EPD2F_PF4_Y35NY_DPG.M` | Monthly | NY Harbor Heating Oil Spot (Monthly) | $/Gallon |
| `PET.EER_EPD2F_PE2_Y35NY_DPG.D` | Daily | NY Harbor Heating Oil Futures (Contract 1) | $/Gallon |
| `PET.W_EPD2F_PRS_NUS_DPG.W` | Weekly | **U.S. Residential Heating Oil Price** | $/Gallon |
| `PET.M_EPD2F_PRS_NUS_DPG.M` | Monthly | U.S. Residential Heating Oil Price | $/Gallon |
| `PET.W_EPD2F_PWR_NUS_DPG.W` | Weekly | U.S. Wholesale Heating Oil Price | $/Gallon |
| `PET.W_EPD2F_PRS_R1X_DPG.W` | Weekly | New England Residential Heating Oil | $/Gallon |
| `PET.W_EPD2F_PWR_R1X_DPG.W` | Weekly | New England Wholesale Heating Oil | $/Gallon |

---

## Jet fuel series IDs (conventional, for SAF comparison)

### Production and stocks

| Series ID | Frequency | Description | Units |
|-----------|-----------|-------------|-------|
| `PET.WKJRPUS2.W` | Weekly | U.S. Kerosene-Type Jet Fuel Production | Thousand Barrels/Day |
| `PET.MKJRPUS2.M` | Monthly | U.S. Jet Fuel Production | Thousand Barrels/Day |
| `PET.WKJST_NUS_1.W` | Weekly | **U.S. Jet Fuel Stocks** | Thousand Barrels |
| `PET.W_EPJK_SAE_NUS_MBBL.W` | Weekly | U.S. Jet Fuel Ending Stocks | Thousand Barrels |
| `PET.W_EPJK_SAE_R10_MBBL.W` | Weekly | PADD 1 Jet Fuel Stocks | Thousand Barrels |
| `PET.W_EPJK_SAE_R20_MBBL.W` | Weekly | PADD 2 Jet Fuel Stocks | Thousand Barrels |
| `PET.W_EPJK_SAE_R30_MBBL.W` | Weekly | PADD 3 Jet Fuel Stocks | Thousand Barrels |
| `PET.MKJST_NUS_1.M` | Monthly | U.S. Monthly Jet Fuel Stocks | Thousand Barrels |

### Consumption

| Series ID | Frequency | Description | Units |
|-----------|-----------|-------------|-------|
| `PET.WKJUPUS2.W` | Weekly | **U.S. Jet Fuel Product Supplied** | Thousand Barrels/Day |
| `PET.MKJUPUS2.M` | Monthly | U.S. Jet Fuel Product Supplied | Thousand Barrels/Day |
| `PET.W_EPJK_VSD_NUS_DAYS.W` | Weekly | Jet Fuel Days of Supply | Days |

### Prices

| Series ID | Frequency | Description | Units |
|-----------|-----------|-------------|-------|
| `PET.EER_EPJK_PF4_RGC_DPG.D` | Daily | **Gulf Coast Jet Fuel Spot Price FOB** | $/Gallon |
| `PET.EER_EPJK_PF4_RGC_DPG.W` | Weekly | Gulf Coast Jet Fuel Spot (Weekly) | $/Gallon |
| `PET.EER_EPJK_PTG_NUS_DPG.D` | Daily | U.S. Kerosene-Type Jet Fuel Retail | $/Gallon |
| `PET.EMM_EPJK_PTG_NUS_DPG.M` | Monthly | U.S. Jet Fuel Retail Price | $/Gallon |

### Trade

| Series ID | Frequency | Description | Units |
|-----------|-----------|-------------|-------|
| `PET.W_EPJK_IM0_NUS_MBBLPD.W` | Weekly | U.S. Jet Fuel Imports | Thousand Barrels/Day |
| `PET.W_EPJK_EX0_NUS-Z00_MBBLPD.W` | Weekly | U.S. Jet Fuel Exports | Thousand Barrels/Day |
| `PET.MKJIM_NUS_1.M` | Monthly | U.S. Jet Fuel Imports | Thousand Barrels/Day |
| `PET.MKJEX_NUS_1.M` | Monthly | U.S. Jet Fuel Exports | Thousand Barrels/Day |

---

## Biofuel feedstock tracking via Form EIA-819

EIA tracks feedstock consumption for biodiesel and renewable diesel production through Form EIA-819 (Monthly Biofuels Report). Data available at https://www.eia.gov/biofuels/update/ and https://www.eia.gov/dnav/pet/pet_pnp_feedbiofuel_dcu_nus_m.htm

### Feedstocks tracked by EIA

| Feedstock Category | Specific Items | Units | Frequency |
|-------------------|----------------|-------|-----------|
| Vegetable Oils | Soybean oil, Canola oil, Corn oil, Palm oil | Million pounds | Monthly |
| Waste Oils/Fats | Yellow grease (includes UCO), Tallow, White grease, Poultry fat | Million pounds | Monthly |
| Agriculture/Forestry | Corn, Grain sorghum, Agricultural residues, Dedicated energy crops | Million pounds | Monthly |
| Recycled Materials | Municipal solid waste, Yard and food waste | Million pounds | Monthly |
| Other | Oil from algae, Biogas | Million pounds | Monthly |

---

## Critical data gaps requiring alternative sources

EIA has significant gaps in feedstock and co-product tracking that require USDA data sources:

### Not tracked by EIA

| Data Need | Alternative Source | Report/Location |
|-----------|-------------------|-----------------|
| **Corn consumed for ethanol** (bushels) | USDA NASS | Grain Crushings and Co-Products Production |
| **DDGS production** | USDA NASS | Grain Crushings Report |
| **Corn oil from ethanol plants** | USDA NASS | Grain Crushings Report |
| **Glycerin production** (biodiesel co-product) | Not systematically tracked | — |
| Feedstock prices | USDA ERS / CME Group | Agricultural Prices / Futures |
| Natural gas for ammonia specifically | Not available | Only industrial total tracked |
| Farm-specific energy consumption | USDA ERS | Farm Business Economics |
| Biodiesel/renewable diesel prices | Not published by EIA | OPIS, industry sources |
| SAF-specific volumes | EPA / California LCFS | RFS RIN data, LCFS reports |

### Key alternative data sources

- **USDA NASS Grain Crushings Report:** https://usda.library.cornell.edu/concern/publications/n583xt96p (corn for ethanol, DDGS, corn oil)
- **USDA ERS U.S. Bioenergy Statistics:** https://www.ers.usda.gov/data-products/us-bioenergy-statistics/
- **USDA-EIA Biofuels Data Sources Matrix:** https://www.ers.usda.gov/about-ers/partnerships/strengthening-statistics-through-the-icars/biofuels-data-sources

---

## Summary reference table for most-watched series

| Commodity | Key Series ID | Description | Frequency |
|-----------|--------------|-------------|-----------|
| Ethanol | `PET.W_EPOOXE_YOP_NUS_MBBLD.W` | U.S. Production | Weekly |
| Ethanol | `PET.W_EPOOXE_SAE_NUS_MBBL.W` | U.S. Stocks | Weekly |
| Biodiesel | `PET.M_EPOORDB_YNP_NUS_MBBL.M` | U.S. Production | Monthly |
| Renewable Diesel | `PET.M_EPOORDO_SAE_NUS_MBBL.M` | U.S. Stocks | Monthly |
| Gasoline | `PET.WGTSTUS1.W` | U.S. Total Stocks | Weekly |
| Gasoline | `PET.WGFUPUS2.W` | U.S. Product Supplied | Weekly |
| Gasoline | `PET.EMM_EPM0_PTE_NUS_DPG.W` | U.S. Retail Price | Weekly |
| Diesel | `PET.WDISTUS1.W` | U.S. Total Stocks | Weekly |
| Diesel | `PET.EMD_EPD0_PTE_NUS_DPG.W` | U.S. Retail Price | Weekly |
| Natural Gas | `NG.RNGWHHD.D` | Henry Hub Spot Price | Daily |
| Natural Gas | `NG.N3035US2.M` | Industrial Consumption | Monthly |
| Natural Gas | `NG.NW2_EPG0_SWO_R48_BCF.W` | Working Gas Storage | Weekly |
| Propane | `PET.W_EPLLPZ_SAE_NUS_MBBL.W` | U.S. Total Stocks | Weekly |
| Propane | `PET.W_EPLLPA_PRS_R20_DPG.W` | Midwest Residential Price | Weekly |
| Jet Fuel | `PET.WKJUPUS2.W` | U.S. Product Supplied | Weekly |
| Jet Fuel | `PET.EER_EPJK_PF4_RGC_DPG.D` | Gulf Coast Spot Price | Daily |

---

## Conclusion

This reference identifies **over 150 EIA series IDs** across the requested commodity categories, providing a comprehensive foundation for RLC's data collection systems. The EIA API v2 enables programmatic access via both legacy series ID translation and hierarchical routes. Key implementation considerations include: weekly petroleum data releases on Wednesdays with monthly revisions providing final figures; natural gas using different naming conventions (NG prefix) than petroleum (PET prefix); and significant feedstock data gaps—particularly corn for ethanol and co-products—requiring integration with USDA NASS data sources. For SAF tracking, the absence of dedicated series IDs means relying on aggregated "Other Biofuels" data or alternative sources like EPA RFS RIN data and California LCFS reports until EIA expands coverage of this emerging fuel category.