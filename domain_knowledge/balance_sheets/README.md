# Balance Sheet CSV Files

This directory contains user S&D (Supply & Demand) estimate CSV files that are loaded into the `silver.user_sd_estimate` table for variance tracking against realized monthly data.

## Directory Structure

```
balance_sheets/
├── feed_grains/          # Corn, sorghum, barley
│   ├── us_corn.csv
│   └── us_sorghum.csv
├── oilseeds/             # Soybeans, canola, sunflower
│   ├── us_soybeans.csv
│   └── brazil_soybeans.csv
├── food_grains/          # Wheat, rice
│   └── us_wheat.csv
├── fats_greases/         # Soybean oil, UCO, tallow
│   └── us_soybean_oil.csv
├── biofuels/             # Ethanol, biodiesel, SAF
│   └── us_ethanol.csv
└── macro/                # World totals, regional aggregates
    └── world_corn.csv
```

## CSV Format

### Required Columns
- `commodity` - Commodity name (corn, soybeans, wheat, etc.)
- `marketing_year` - Marketing year as integer (e.g., 2025 for MY 2024/25)

### Optional Columns
- `country` - Country name (default: "United States")
- `unit` - Unit of measure (default: "mil bu")

### Supply Side Columns
- `area_planted` - Planted area
- `area_harvested` - Harvested area
- `yield` - Yield per unit area
- `beginning_stocks` - Carryover from prior MY
- `production` - Total production
- `imports` - Total imports
- `total_supply` - Sum of beg stocks + production + imports

### Demand Side Columns
- `crush` - Oilseed crush (soybeans, canola)
- `feed_residual` - Feed & residual use (corn, sorghum)
- `fsi` - Food, Seed, Industrial (non-ethanol)
- `ethanol` - Corn used for fuel ethanol
- `domestic_use` - Total domestic use
- `exports` - Total exports
- `total_use` - Total use/disappearance

### Ending Columns
- `ending_stocks` - Carryout to next MY
- `stocks_use_ratio` - Stocks/Use percentage

### Metadata Columns
- `notes` - Any notes or assumptions

## Example CSV: US Corn

```csv
commodity,country,marketing_year,area_planted,area_harvested,yield,beginning_stocks,production,imports,total_supply,feed_residual,fsi,ethanol,domestic_use,exports,total_use,ending_stocks,stocks_use_ratio,unit,notes
corn,United States,2024,91.0,83.0,177.3,1803,14720,25,16548,5650,1490,5450,12590,2150,14740,1808,12.3,mil bu,Pre-harvest estimate
corn,United States,2025,93.0,85.5,181.0,1808,15475,25,17308,5800,1500,5500,12800,2400,15200,2108,13.9,mil bu,Trend yield assumption
```

## Example CSV: US Soybeans

```csv
commodity,country,marketing_year,area_planted,area_harvested,yield,beginning_stocks,production,imports,total_supply,crush,exports,domestic_use,total_use,ending_stocks,stocks_use_ratio,unit,notes
soybeans,United States,2024,86.5,85.5,50.2,264,4290,20,4574,2295,1780,95,4170,404,9.7,mil bu,Pre-harvest estimate
soybeans,United States,2025,87.0,86.0,52.0,404,4472,20,4896,2350,1850,100,4300,596,13.9,mil bu,Trend yield assumption
```

## Loading Data

Use the balance sheet loader script:

```bash
# Load a specific file
python src/agents/loaders/balance_sheet_loader.py --file oilseeds/us_soybeans.csv

# Load all CSV files
python src/agents/loaders/balance_sheet_loader.py --all

# List available CSV files
python src/agents/loaders/balance_sheet_loader.py --list
```

## Notes

1. **Marketing Year Convention**: Use the ending year of the marketing year (e.g., 2025 for MY 2024/25).

2. **Units**: Default is "mil bu" (million bushels). Specify "1000 MT" for metric tons.

3. **Version Control**: Each time a CSV is loaded, it marks previous estimates for that commodity as "not current" and creates new records dated today.

4. **Variance Tracking**: Loaded estimates are compared against realized monthly data from NOPA, Fats & Oils, Census, etc. through the `gold.sd_variance_tracker` view.

5. **Monthly Projections**: For monthly expectations (remaining months in MY), use the `silver.monthly_expectation` table directly or a separate monthly CSV format.
