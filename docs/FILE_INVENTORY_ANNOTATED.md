# RLC-Agent Complete File Inventory (Annotated)
**Generated:** 2026-01-27
**Total Items:** 987 files and folders

---

## Root Directory Files

| File | Description |
|------|-------------|
| `.env` | Environment variables (DB credentials, API keys) - not in git |
| `.env.example` | Template for environment variables setup |
| `.gitignore` | Git ignore patterns for Python, credentials, cache |
| `.mcp.json` | Model Context Protocol configuration for Claude Code |
| `LLM_SETUP_PLAN.md` | Planning document for local LLM integration |
| `README.md` | Project overview and setup instructions |
| `requirements.txt` | Python package dependencies |
| `start_overnight_test.py` | Script to launch overnight data collection tests |

---

## /archive/
*Archived and deprecated code for reference*

| Path | Description |
|------|-------------|
| `archive/` | Container for old/deprecated code |
| `archive/deprecated_code/` | Deprecated scripts and modules |

---

## /biotrack/
*BioTrack AI - Biofuel facility tracking system*

| Path | Description |
|------|-------------|
| `biotrack/` | Biofuel production facility tracking module |
| `biotrack/biotrack_ai.py` | Main AI agent for biofuel facility monitoring |
| `biotrack/README.md` | BioTrack module documentation |
| `biotrack/requirements.txt` | BioTrack-specific dependencies |
| `biotrack/config/` | BioTrack configuration files |
| `biotrack/config/facilities.csv` | Database of biofuel facilities (locations, capacity) |

---

## /config/
*Application configuration files*

| Path | Description |
|------|-------------|
| `config/` | Central configuration directory |
| `config/credentials.env` | API credentials and secrets (not in git) |
| `config/credentials.example` | Template for credentials setup |
| `config/DATA_SOURCES_CHECKLIST.md` | Checklist of data sources to implement |
| `config/data_sources_master.csv` | Master list of all data sources with URLs, frequencies |
| `config/weather_cities.json` | Cities for weather data collection |
| `config/weather_email_config.json` | Weather email agent configuration |
| `config/weather_locations.json` | Geographic coordinates for weather stations |

---

## /dashboards/
*PowerBI dashboards and visualization templates*

| Path | Description |
|------|-------------|
| `dashboards/` | Business intelligence dashboards |
| `dashboards/powerbi/` | PowerBI dashboard files |
| `dashboards/powerbi/First Sample Dashboard - Ethanol.pbix` | Ethanol market dashboard |
| `dashboards/powerbi/First Sample Dashboard - RLC.pbix` | General RLC commodity dashboard |
| `dashboards/powerbi/RLC_Professional_Template_Initial.pbit` | Reusable PowerBI template |
| `dashboards/powerbi/US Balance Sheets.pbix` | US commodity supply/demand balances |
| `dashboards/powerbi/us_soybean_trade_flows.pbix` | Soybean import/export flow visualization |
| `dashboards/powerbi/USDA Prices.pbix` | USDA price data dashboard |
| `dashboards/templates/` | Dashboard templates (empty) |

---

## /data/
*Data storage - raw inputs, processed outputs, exports*

### /data/cache/ & /data/cached/
| Path | Description |
|------|-------------|
| `data/cache/` | Temporary cache for API responses |
| `data/cached/` | Persistent cached data |

### /data/exports/
*Exported data files and inventory reports*

| Path | Description |
|------|-------------|
| `data/exports/` | Exported datasets and analysis |
| `data/exports/duplication_analysis.csv` | Analysis of duplicate code/data |
| `data/exports/HigbyBarrett_Dashboard.xlsx` | HB report data for dashboards |
| `data/exports/inventory_agents.csv` | Inventory of all agent modules |
| `data/exports/inventory_collectors.csv` | Inventory of data collectors |
| `data/exports/inventory_database.csv` | Database tables/views inventory |
| `data/exports/inventory_deployment_tools.csv` | Deployment script inventory |
| `data/exports/inventory_documentation.csv` | Documentation files inventory |
| `data/exports/inventory_orchestrators.csv` | Pipeline orchestrators inventory |
| `data/exports/inventory_report_writers.csv` | Report generation modules |
| `data/exports/inventory_schedulers.csv` | Scheduler modules inventory |
| `data/exports/inventory_scripts.csv` | Utility scripts inventory |
| `data/exports/inventory_services.csv` | Service modules inventory |
| `data/exports/inventory_special_systems.csv` | Special system components |
| `data/exports/inventory_summary.csv` | Summary of all inventories |
| `data/exports/proposed_structure.csv` | Proposed folder restructuring |

### /data/marketing_years/
| Path | Description |
|------|-------------|
| `data/marketing_years/` | Marketing year analysis documents |
| `data/marketing_years/2012-13_corn_summary.md` | 2012 drought year corn analysis |
| `data/marketing_years/2020-21_corn_summary.md` | 2020/21 corn marketing year recap |
| `data/marketing_years/README.md` | Marketing year documentation guide |

### /data/processed/ & /data/raw/
| Path | Description |
|------|-------------|
| `data/processed/` | Cleaned/transformed data (empty) |
| `data/raw/` | Raw source data files |

### /data/raw/ - Source Documents (200+ files)
*USDA reports, CONAB data, EIA energy data, trade statistics*

| Category | Files | Description |
|----------|-------|-------------|
| Acreage Reports | `Acreage - Jun 23.pdf`, etc. | USDA planted acreage reports |
| Agricultural Prices | `Agricultural Prices - *.pdf` | Monthly farm price reports |
| Attache Reports | 12 PDFs | USDA FAS international biofuel reports |
| Balance Sheets | `Balance Sheet Data - *.xlsx` | Supply/demand balance data |
| BBD/Biofuel | `BBD *.xlsx`, `Biodiesel *.xlsx` | Biomass-based diesel data from EIA |
| Canada | `Canada - Crop Production - *.xlsx` | Statistics Canada crop data |
| CONAB | `CONAB - *.xlsx` (6 files) | Brazilian crop estimates |
| Crop Production | `Crop Production - *.pdf/xlsx` (15) | USDA monthly crop reports |
| Crop Progress | `Crop Progress - *.pdf` | Weekly crop condition reports |
| Drought Monitors | 5 PNG images | Argentina/Brazil drought maps |
| EIA Data | `EIA *.xlsx` (10+) | Energy Information Administration data |
| Export Inspections | `Inspections - *.xlsx` | Weekly grain export inspections |
| Fats and Oils | `Fats and Oils - *.pdf/xlsx` (12) | NASS fats/oils crushing reports |
| Feed Grains | `Feed Grain Outlook *.pdf/xlsx` (8) | ERS feed grains outlook |
| Grain Crushings | `Grain Crushings - *.pdf/xlsx` | Monthly ethanol/DDGS production |
| Grain Stocks | `Grain Stocks - *.pdf` | Quarterly stocks reports |
| MMN Reports | 7 PDFs | USDA Market News report samples |
| Oil Crops | `Oil Crops *.xlsx/pdf` | ERS oilseed outlook data |
| Trade Data | `US * Exports/Imports - *.xlsx` (40+) | Census Bureau trade statistics |
| USDA Maps | 17 PNGs | Crop production area maps |
| WASDE | `WASDE - *.pdf/xlsx` (20+) | World supply/demand estimates |
| Weather | `Weather to Watch - *.docx` | Weather analysis documents |

### /data/templates/
| Path | Description |
|------|-------------|
| `data/templates/` | Data file templates |
| `data/templates/marketing_year_template.md` | Template for MY analysis docs |

---

## /database/
*Database schemas, migrations, and queries*

| Path | Description |
|------|-------------|
| `database/` | Database management files |
| `database/census_trade.db` | SQLite database for Census trade data |

### /database/migrations/
*Database migration scripts for schema evolution*

| Path | Description |
|------|-------------|
| `migrations/` | Database migration scripts |
| `migrations/01_inventory_postgres.py` | Inventory existing PostgreSQL tables |
| `migrations/02_deploy_medallion_schema.py` | Deploy bronze/silver/gold layers |
| `migrations/03_migrate_existing_to_bronze.py` | Move legacy data to bronze layer |
| `migrations/04_migrate_sqlite_to_bronze.py` | Migrate SQLite data to PostgreSQL |
| `migrations/README.md` | Migration process documentation |
| `migrations/run_full_migration.py` | Run all migrations in sequence |

### /database/queries/
| Path | Description |
|------|-------------|
| `queries/` | Reusable SQL queries |
| `queries/balance_sheet_inventory.sql` | Query to inventory balance sheet data |

### /database/schemas/
*Medallion architecture schema definitions*

| Path | Description |
|------|-------------|
| `schemas/` | PostgreSQL schema definitions |
| `schemas/001_schema_foundation.sql` | Base schemas (bronze/silver/gold) |
| `schemas/002_bronze_layer.sql` | Bronze (raw) layer tables |
| `schemas/003_silver_layer.sql` | Silver (cleaned) layer tables |
| `schemas/004_gold_layer.sql` | Gold (analytics) layer views |
| `schemas/005_roles_and_security.sql` | Database roles and permissions |
| `schemas/005_transformation_logging.sql` | ETL transformation audit logs |
| `schemas/006_operational_guidance.sql` | Operational metadata tables |
| `schemas/006_weather_schema.sql` | Weather observation tables |
| `schemas/007_price_schema.sql` | Cash and futures price tables |
| `schemas/007_wheat_tenders.sql` | International wheat tender tracking |
| `schemas/008_feed_grains_data.sql` | ERS feed grains data tables |
| `schemas/008_futures_sessions_schema.sql` | **[NEW]** Futures session OHLC tables |
| `schemas/009_food_expenditure_data.sql` | Consumer food expenditure data |
| `schemas/010_conab_soybean.sql` | CONAB Brazil soybean tables |
| `schemas/cattle_on_feed_bronze.sql` | Cattle on feed report tables |
| `schemas/create_wheat_all_table.sql` | Consolidated wheat data table |
| `schemas/DATABASE_DESIGN.md` | Database architecture documentation |
| `schemas/diagnose_and_fix.sql` | Diagnostic and repair queries |
| `schemas/fix_trade_views.sql` | Trade view corrections |
| `schemas/install.sh` | Schema installation script |
| `schemas/LOGGING_AGENT_DESIGN.md` | Logging agent architecture |
| `schemas/README.md` | Schema documentation |

### /database/sql/
*Additional SQL scripts*

| Path | Description |
|------|-------------|
| `sql/00_init.sql` | Database initialization |
| `sql/002_hb_report_tables.sql` | HigbyBarrett report data tables |
| `sql/003_comprehensive_commodity_schema.sql` | Full commodity data schema |
| `sql/004_export_inspections.sql` | Export inspection tables |
| `sql/01_schemas.sql` | Schema creation |
| `sql/02_core_dimensions.sql` | Dimension tables (commodities, locations) |
| `sql/03_audit_tables.sql` | Audit trail tables |
| `sql/04_bronze_wasde.sql` | WASDE bronze layer |
| `sql/05_silver_observation.sql` | Silver observation tables |
| `sql/06_gold_views.sql` | Gold analytics views |
| `sql/07_roles_grants.sql` | Role permissions |
| `sql/08_functions.sql` | Stored functions |
| `sql/09_sample_dml.sql` | Sample data inserts |
| `sql/99_operational.sql` | Operational queries |

### /database/views/
| Path | Description |
|------|-------------|
| `views/01_traditional_balance_sheets.sql` | Balance sheet views v1 |
| `views/02_traditional_balance_sheets_v2.sql` | Balance sheet views v2 |
| `views/03_trade_flow_views.sql` | Import/export flow views |
| `views/04_ers_gold_views.sql` | ERS data gold layer views |

---

## /docs/
*Project documentation (80+ files)*

### Core Documentation
| Path | Description |
|------|-------------|
| `docs/` | Main documentation directory |
| `docs/ARCHITECTURE_PLAN.md` | System architecture overview |
| `docs/AUTOMATED_DATA_PIPELINE.md` | Data pipeline documentation |
| `docs/BALANCE_SHEET_KNOWLEDGE.md` | Balance sheet methodology |
| `docs/CODE_REVIEW.md` | Code review guidelines |
| `docs/CONSOLIDATION_PLAN.md` | Code consolidation strategy |
| `docs/CREDENTIALS_REQUIRED.md` | Required API credentials list |
| `docs/DATA_SOURCE_REGISTRY.md` | Registry of all data sources |
| `docs/FILE_ORGANIZATION_PLAN.md` | File structure planning |
| `docs/FORECAST_TRACKING_GUIDE.md` | Forecast accuracy tracking |
| `docs/HB_REPORT_AUTOMATION_GUIDE.md` | HigbyBarrett report automation |
| `docs/HISTORICAL_DATABASE_SETUP.md` | Historical data setup guide |
| `docs/INCOMPLETE_TASKS_EXPORT.csv` | Incomplete tasks from Notion |
| `docs/INCOMPLETE_TASKS_SUMMARY.md` | Task completion summary |
| `docs/INTEGRATION_GUIDE.md` | System integration guide |
| `docs/LLM_TRAINING_STRATEGY.md` | LLM fine-tuning strategy |
| `docs/NORTH_AMERICA_DATA_SOURCES.md` | US/Canada data sources |
| `docs/NOTION_DATA_SUMMARY.md` | Notion database summary |
| `docs/NOTION_PROJECT_STRUCTURE.md` | Notion workspace structure |
| `docs/POWER_BI_SETUP_GUIDE.md` | PowerBI configuration |
| `docs/POWER_BI_TEMPLATES.md` | PowerBI template docs |
| `docs/POWERBI_DASHBOARD_GUIDE.md` | Dashboard creation guide |
| `docs/POWERBI_ODBC_SETUP.md` | ODBC connection setup |
| `docs/RESTRUCTURE_PROPOSAL.md` | Code restructuring proposal |
| `docs/SCREEN_RECORDING_FOR_LLM.md` | Screen recording for training |
| `docs/SOUTH_AMERICA_DATA_SOURCES.md` | Brazil/Argentina sources |
| `docs/VISUALIZATION_INSPIRATION.md` | Dashboard design ideas |
| `docs/WHEAT_TENDER_MONITORING.md` | Wheat tender tracking guide |
| `docs/windows_service_setup.md` | Windows service configuration |

### Agent & System Documentation (.docx)
| Path | Description |
|------|-------------|
| `docs/Agents 101.docx` | Introduction to agent architecture |
| `docs/Complete RLC Master Agent Setup Guide.docx` | Full setup instructions |
| `docs/Description of Persistent Business Partner Agent.docx` | Business agent design |
| `docs/Design of the HB Weekly Report Writer Agent.docx` | Report writer specification |
| `docs/Desktop Assistant Project Folder Contents.docx` | Folder structure guide |
| `docs/Initial Desktop LLM Set Up Instructions.docx` | LLM setup guide |
| `docs/Master Agent Identity for the Nash System.docx` | Nash agent identity |
| `docs/Master-Agent Architecture*.docx` (5 versions) | Architecture iterations |
| `docs/NASH - Initial Full Stack Implementation.docx` | Nash system implementation |
| `docs/Persistent_Orchestrator_Blueprint.docx` | Orchestrator design |
| `docs/RLC Master Agent System.docx` | Master agent documentation |

### API & Integration Docs
| Path | Description |
|------|-------------|
| `docs/Automated Data Ingestion Pipeline Setup Guide.docx` | Pipeline setup |
| `docs/Consolidated Structure for Google Calendar and Email.docx` | Google API setup |
| `docs/Connecting the Local LLM to Gmail and Calendar.docx` | Gmail integration |
| `docs/data_sources_api_info.docx` | API endpoint documentation |
| `docs/IBKR API Connector.txt` | Interactive Brokers API notes |
| `docs/OpenWeather API Guide.docx` | Weather API documentation |
| `docs/Prompt for Comprehensive API Guidebooks.docx` | API documentation prompts |
| `docs/USDA FSA GATS API Sites.docx` | USDA API endpoints |

### Marketing & Business Docs
| Path | Description |
|------|-------------|
| `docs/Comprehensive Marketing Strategy for RLC.docx` | Marketing plan |
| `docs/Content Calendar*.csv` (4 files) | Social media content planning |
| `docs/RLC Marketing Execution Plan*.xlsx/pdf` | Marketing execution |
| `docs/RLC Media Editing Instructions.pdf` | Media guidelines |
| `docs/RLC Social Media Setup Guide.docx` | Social media setup |
| `docs/RLC Website Launch Guide.docx` | Website launch plan |

### Technical Setup Docs
| Path | Description |
|------|-------------|
| `docs/RLC Tech Stack.docx` | Technology stack overview |
| `docs/RLC-Server Set Up Guide.md/pdf` | Server configuration |
| `docs/Server-Build Plan-Info.docx/pdf` | Server build specifications |
| `docs/Technical Details and Outlines.xlsx` | Technical specifications |

### Reference Documents
| Path | Description |
|------|-------------|
| `docs/Census Geo Reference Files.pdf` | Census geographic codes |
| `docs/cofd1225.pdf` | Cattle on Feed report sample |
| `docs/HS Codes.xlsx` | Harmonized System trade codes |
| `docs/My Market News Reports List.txt/xlsx` | USDA report catalog |
| `docs/Notion Database Details.xlsx` | Notion database export |
| `docs/notion_data_export.json` | Notion JSON export |
| `docs/proposed_data_sources.md` | Proposed new data sources |

### Subdirectories
| Path | Description |
|------|-------------|
| `docs/api/` | API documentation (empty) |
| `docs/architecture/` | Architecture documents |
| `docs/architecture/Folder Structure - The Bible.docx` | Definitive folder structure |
| `docs/architecture/LIBRARY_STRUCTURE.md` | Code library organization |
| `docs/runbooks/` | Operational runbooks (empty) |
| `docs/setup/` | Setup guides (empty) |
| `docs/training/` | Training materials |
| `docs/training/CC_TRAINING_GUIDE.md` | Claude Code training guide |

---

## /domain_knowledge/
*Reference materials for LLM context and analyst training*

### /domain_knowledge/balance_sheets/
*Balance sheet templates by commodity category*

| Path | Description |
|------|-------------|
| `balance_sheets/` | Balance sheet reference materials |
| `balance_sheets/biofuels/` | Ethanol, biodiesel S&D templates |
| `balance_sheets/fats_greases/` | Animal fats, vegetable oils |
| `balance_sheets/feed_grains/` | Corn, sorghum, barley, oats |
| `balance_sheets/food_grains/` | Wheat, rice |
| `balance_sheets/macro/` | Global/macro economic data |
| `balance_sheets/oilseeds/` | Soybeans, canola, sunflower |

### /domain_knowledge/crop_calendars/
| Path | Description |
|------|-------------|
| `crop_calendars/` | Monthly crop activity calendars |
| `crop_calendars/January.gif` - `December.gif` | 12 monthly planting/harvest calendars |

### /domain_knowledge/crop_maps/
*Production area maps by region*

| Path | Description |
|------|-------------|
| `crop_maps/` | Crop production area maps |
| `crop_maps/argentina/` | Argentina production maps (empty) |
| `crop_maps/brazil/` | Brazil production maps |
| `crop_maps/brazil/Brazil - Soybean - 2023.png` | Brazil soybean production areas |
| `crop_maps/global/` | Global production maps (empty) |
| `crop_maps/us/` | US production maps (17 PNGs) |
| `crop_maps/us/US - Corn - 2023.png` | US corn production areas |
| `crop_maps/us/US - Soybean - 2023.png` | US soybean production areas |
| `crop_maps/us/US - Wheat - 2023.png` | US wheat production areas |
| *(+ 14 more crop maps)* | Barley, canola, cotton, oats, etc. |

### /domain_knowledge/data_dictionaries/
| Path | Description |
|------|-------------|
| `data_dictionaries/` | Field definitions (empty) |

### /domain_knowledge/glossaries/
| Path | Description |
|------|-------------|
| `glossaries/` | Industry terminology (empty) |

### /domain_knowledge/llm_context/
| Path | Description |
|------|-------------|
| `llm_context/` | Context documents for LLM |
| `llm_context/Jan 22 5 am.docx` | Daily market context snapshot |

### /domain_knowledge/market_specs/
| Path | Description |
|------|-------------|
| `market_specs/` | Futures contract specifications (empty) |

### /domain_knowledge/methodology/
| Path | Description |
|------|-------------|
| `methodology/` | Analysis methodology docs (empty) |

### /domain_knowledge/operator_guides/
*Operational procedures for different agent roles*

| Path | Description |
|------|-------------|
| `operator_guides/` | Agent operation manuals |
| `operator_guides/App Developer Operator.docx` | App development procedures |
| `operator_guides/Checker Operator.docx` | Quality check procedures |
| `operator_guides/Checker Operator - App Developer.docx` | App code review |
| `operator_guides/Checker Operator - Data Gathering.docx` | Data validation |
| `operator_guides/Checker Operator - Pattern Analysis.docx` | Pattern verification |
| `operator_guides/Checker Operator - Trading Code.docx` | Trading code review |
| `operator_guides/Data Gathering Operator.docx` | Data collection procedures |
| `operator_guides/IBKR API Pull/` | IBKR data pull scripts |
| `operator_guides/Pattern Analysis Operator.docx` | Pattern analysis procedures |
| `operator_guides/Project Manager Operator.docx` | PM procedures |
| `operator_guides/Test IBKR File/` | IBKR test files |
| `operator_guides/Trading Code Operator.docx` | Trading algorithm procedures |

### /domain_knowledge/sample_presentations/
*Historical presentation files for reference*

| Path | Description |
|------|-------------|
| `sample_presentations/` | Conference presentations |
| `sample_presentations/AFOA Conference Presentation - 10132021.pptx` | AFOA 2021 |
| `sample_presentations/AFOA Presentation - 10172019.pptx` | AFOA 2019 |
| `sample_presentations/Biomass Based Diesel Outlook.pptx` | BBD outlook |
| `sample_presentations/Feed Grains Presentation - 05232018.pptx` | Feed grains |
| `sample_presentations/Jake Conference 2022/` | 2022 conference materials (15 files) |
| `sample_presentations/Master Presentation - 08262021.pptx` | Master template |
| `sample_presentations/Oilseeds Presentation - 05232018.pptx` | Oilseeds |
| `sample_presentations/Webinar - 04262022.pptx` | Webinar presentation |

### /domain_knowledge/sample_reports/
*Sample industry reports for training and reference*

| Path | Description |
|------|-------------|
| `sample_reports/` | Industry report samples |
| `sample_reports/HigbyBarrett Weekly Report*.docx` (6) | HB weekly reports Oct-Dec 2025 |
| `sample_reports/data/` | Report source data |
| `sample_reports/data/cross_commodity/` | Multi-commodity reports (11 files) |
| `sample_reports/data/energy/` | Energy sector data (empty) |
| `sample_reports/data/feed_grains/` | Feed grain data (4 files) |
| `sample_reports/data/food_grains/` | Wheat yearbook tables (15 CSVs) |
| `sample_reports/data/livestock/` | Cattle on feed data (11 files) |
| `sample_reports/data/oilseeds_fats_greases/` | Oilseed data (2 files) |
| `sample_reports/historical/` | Historical event analysis |
| `sample_reports/historical/drought_years/` | Drought year analyses (empty) |
| `sample_reports/historical/market_events/` | Market event studies (empty) |
| `sample_reports/historical/trade_disruptions/` | Trade disruption analyses (empty) |
| `sample_reports/industry_reports/` | Third-party reports |
| `sample_reports/industry_reports/outlook_reports/` | Outlook reports (empty) |
| `sample_reports/industry_reports/special_reports/` | Special reports (empty) |
| `sample_reports/industry_reports/trusted_sources/` | Trusted source list (empty) |

### /domain_knowledge/special_situations/
*Analysis of historical market-moving events*

| Path | Description |
|------|-------------|
| `special_situations/` | Historical event analyses |
| `special_situations/2012_us_drought.md` | 2012 US drought impact analysis |
| `special_situations/2018_china_trade_war.md` | China trade war market impact |
| `special_situations/2020_china_demand_surge.md` | 2020 China buying surge |
| `special_situations/2020_derecho.md` | August 2020 derecho damage |
| `special_situations/2022_ukraine_war.md` | Ukraine war grain impact |
| `special_situations/README.md` | Special situations guide |

### Other Domain Knowledge Files
| Path | Description |
|------|-------------|
| `data_sources.xlsx` | Data source inventory |
| `inspections export example.xlsx` | Export inspection sample |
| `Iowa Extension - Cost of Storing Grain.pdf` | Grain storage economics |
| `NTAD_North_American_Rail_Network_Lines.kmz` | Rail network map (Google Earth) |
| `Price Reporting Handbook - AMS.pdf` | USDA price reporting guide |
| `Projections for Brazilian Production.pdf` | Brazil crop projections |
| `Protein Ladder - Example.png` | Protein meal value hierarchy |
| `state level production forecast example.xlsx` | State forecast template |
| `system_inventory.md` | System component inventory |
| `trade flow tracking sheet example.xlsx` | Trade flow tracking template |
| `uco_facilities.csv` | Used cooking oil facilities |
| `uco_locations.txt` | UCO facility locations |
| `spreadsheet_samples/` | Spreadsheet examples (empty) |
| `templates/` | Document templates |
| `var_analysis/` | Value-at-risk analysis |
| `var_analysis/Gulf Coast Tallow Price Synthetic Hedging.docx` | Tallow hedging analysis |
| `webinars/` | Webinar recordings (empty) |

---

## /output/
*Generated outputs - logs, reports, visualizations*

### /output/logs/
*System and process logs*

| Path | Description |
|------|-------------|
| `logs/` | Application log files |
| `logs/data_checker.log` | Data validation agent log |
| `logs/hb_report_test_20260126_*.log` (3) | HB report test logs |
| `logs/hb_report_v2_20260127_*.log` (3) | HB report V2 generation logs |
| `logs/overnight_runner_20260127.log` | **[NEW]** Overnight daemon log |
| `logs/overnight_test_20260126_181143.log` | Overnight test log |
| `logs/weather_collector.log` | Weather collection log |
| `logs/weather_email.log` | Weather email log |
| `logs/weather_email_agent.log` | Weather email agent log |

### /output/reports/
*Generated market reports*

| Path | Description |
|------|-------------|
| `reports/` | Generated reports |
| `reports/brazil_crop_update/` | Brazil crop updates (empty) |
| `reports/higby_barrett/` | HigbyBarrett weekly reports |
| `reports/higby_barrett/HB_Weekly_DATA_V2_*.json` (3) | Report source data JSON |
| `reports/higby_barrett/HB_Weekly_PROMPT_V2_*.txt` (3) | LLM prompts used |
| `reports/higby_barrett/HB_Weekly_Report_V2_*.md` (3) | Generated report markdown |
| `reports/higby_barrett/HB_Weekly_Report_TEST_*.md` | Test report output |
| `reports/higby_barrett/HigbyBarrett Weekly Report - Test.docx` | Word format test |
| `reports/us_saf_update/` | US SAF updates (empty) |
| `reports/weather_summaries/` | Weather summary reports |
| `reports/weather_summaries/weather_summary_*.txt` (12) | Daily weather summaries |

### /output/visualizations/
*Generated charts and graphs*

| Path | Description |
|------|-------------|
| `visualizations/` | Generated visualizations |
| `visualizations/corn_balance_sheet_20260115.png` | Corn S&D chart |
| `visualizations/corn_prices_20260115.png` | Corn price chart |
| `visualizations/corn_report_20260115.txt` | Corn analysis text |
| `visualizations/corn_stocks_price_20260115.png` | Corn stocks vs price |
| `visualizations/soybean_balance_sheet_20260115.png` | Soybean S&D chart |
| `visualizations/soybean_crush_margin_20260115.png` | Crush margin chart |
| `visualizations/soybean_prices_20260115.png` | Soybean price chart |
| `visualizations/soybean_report_20260115.txt` | Soybean analysis text |
| `visualizations/wheat_balance_sheet_20260115.png` | Wheat S&D chart |
| `visualizations/wheat_export_destinations_20260115.png` | Wheat export flows |
| `visualizations/wheat_prices_20260115.png` | Wheat price chart |
| `visualizations/wheat_report_20260115.txt` | Wheat analysis text |

---

## /scripts/
*Utility scripts for data collection, transformation, and analysis*

### Root Scripts
| Path | Description |
|------|-------------|
| `scripts/` | Utility script directory |
| `scripts/collect.py` | General data collection runner |
| `scripts/create_powerbi_export.py` | Export data for PowerBI |
| `scripts/create_weather_table.sql` | Weather table DDL |
| `scripts/data_scheduler.py` | Data collection scheduler |
| `scripts/export_cached_data.py` | Export cached API data |
| `scripts/export_for_powerbi.py` | PowerBI data export |
| `scripts/extract_trade_data.py` | Trade data extraction |
| `scripts/ingest_all_wheat_data.py` | Wheat data ingestion |
| `scripts/ingest_corn_trade.py` | Corn trade data ingestion |
| `scripts/ingest_crop_production_summary.py` | Crop production ingestion |
| `scripts/ingest_ers_data.py` | ERS data ingestion |
| `scripts/ingest_feed_grains_data.py` | Feed grains ingestion |
| `scripts/ingest_feed_grains_yearbook.py` | Feed grains yearbook |
| `scripts/ingest_oil_crops_csv.py` | Oil crops CSV ingestion |
| `scripts/ingest_oil_crops_data.py` | Oil crops data ingestion |
| `scripts/ingest_oil_crops_monthly.py` | Monthly oil crops update |
| `scripts/ingest_wasde.py` | WASDE report ingestion |
| `scripts/ingest_wheat_data.py` | Wheat data ingestion |
| `scripts/init_database.py` | Database initialization |
| `scripts/inventory_balance_sheet_data.py` | Balance sheet inventory |
| `scripts/llm_assistant.py` | LLM assistant interface |
| `scripts/load_cached_to_db.py` | Load cache to database |
| `scripts/load_historical_data.py` | Historical data loader |
| `scripts/monitor_wheat_tenders.py` | Wheat tender monitor |
| `scripts/overnight_runner.py` | **[NEW]** Overnight scheduler daemon |
| `scripts/pull_census_trade.py` | Census trade data pull |
| `scripts/pull_census_trade_git.py` | Census trade (git version) |
| `scripts/pull_census_trade_modified.py` | Census trade (modified) |
| `scripts/pull_cgc_trade.py` | Canada CGC trade data |
| `scripts/pull_extended_historical.py` | Extended historical data |
| `scripts/pull_historical_weather.py` | Historical weather data |
| `scripts/pull_statcan_trade.py` | Statistics Canada trade |
| `scripts/pull_weekly_inspections.py` | Weekly export inspections |
| `scripts/test_all_collectors.py` | Test all data collectors |
| `scripts/test_excel_write.py` | Excel write test |
| `scripts/test_hb_report_generation.py` | HB report test V1 |
| `scripts/test_hb_report_generation_v2.py` | HB report test V2 (enhanced) |

### /scripts/collectors/
| Path | Description |
|------|-------------|
| `collectors/` | Standalone collector scripts |
| `collectors/__init__.py` | Package init |
| `collectors/ers_feed_grains_collector.py` | ERS feed grains collector |
| `collectors/ers_monthly_outlook_collector.py` | ERS outlook collector |

### /scripts/data/cache/
| Path | Description |
|------|-------------|
| `data/cache/` | API response cache |
| `data/cache/*.json` (4 files) | Cached API responses |

### /scripts/data_ingestion/
| Path | Description |
|------|-------------|
| `data_ingestion/` | Ingestion scripts (empty) |

### /scripts/deployment/
*Deployment and setup scripts*

| Path | Description |
|------|-------------|
| `deployment/` | Deployment utilities |
| `deployment/agent_tools.py` | Agent utility functions |
| `deployment/balance_sheet_extractor.py` | Balance sheet data extraction |
| `deployment/db_config.py` | Database configuration |
| `deployment/deploy_to_rlc_server.py` | Server deployment script |
| `deployment/document_rag.py` | Document RAG system |
| `deployment/excel_to_database.py` | Excel to DB loader |
| `deployment/extract_feed_grains.py` | Feed grains extraction |
| `deployment/fast_extractor.py` | Fast data extraction |
| `deployment/forecast_tracker.py` | Forecast accuracy tracker |
| `deployment/powerbi_export.py` | PowerBI data export |
| `deployment/setup_google_oauth.py` | Google OAuth setup |
| `deployment/setup_rlc_server.ps1` | PowerShell server setup |
| `deployment/start_agent.py` | Agent startup script |
| `deployment/transfer_calendar.py` | Calendar data transfer |
| `deployment/usda_api.py` | USDA API utilities |

### /scripts/maintenance/
| Path | Description |
|------|-------------|
| `maintenance/` | Maintenance scripts (empty) |

### /scripts/transformations/
*Data transformation (bronze → silver → gold)*

| Path | Description |
|------|-------------|
| `transformations/` | ETL transformation scripts |
| `transformations/__init__.py` | Package init |
| `transformations/oilseed_silver_transformations.py` | Oilseed data transforms |
| `transformations/silver_transformations.py` | General silver transforms |
| `transformations/wheat_silver_transformations.py` | Wheat data transforms |

### /scripts/visualizations/
*Visualization generation scripts*

| Path | Description |
|------|-------------|
| `visualizations/` | Chart generation scripts |
| `visualizations/__init__.py` | Package init |
| `visualizations/gold_visualizations.py` | Gold layer visualizations |
| `visualizations/oilseed_visualizations.py` | Oilseed charts |
| `visualizations/wheat_visualizations.py` | Wheat charts |

---

## /src/
*Main source code - agents, services, orchestrators*

### Root
| Path | Description |
|------|-------------|
| `src/` | Main source code directory |
| `src/__init__.py` | Package init |
| `src/main.py` | Application entry point |

---

### /src/agents/
*AI agents for data collection, analysis, and reporting*

| Path | Description |
|------|-------------|
| `agents/` | Agent modules |
| `agents/__init__.py` | Package init |

### /src/agents/analysis/
*Market analysis agents*

| Path | Description |
|------|-------------|
| `analysis/` | Analysis agents |
| `analysis/__init__.py` | Package init |
| `analysis/fundamental_analyzer.py` | Fundamental S&D analysis |
| `analysis/price_forecaster.py` | Price prediction agent |
| `analysis/spread_and_basis_analyzer.py` | Spread/basis analysis |

### /src/agents/base/
*Base classes for all agents*

| Path | Description |
|------|-------------|
| `base/` | Base agent classes |
| `base/__init__.py` | Package init |
| `base/base_collector.py` | Base data collector class |
| `base/base_lineup_agent.py` | Base vessel lineup agent |
| `base/base_trade_agent.py` | Base trade data agent |

### /src/agents/collectors/
*Data collection agents by region/source*

| Path | Description |
|------|-------------|
| `collectors/` | Data collector agents |
| `collectors/__init__.py` | Package init |

#### /src/agents/collectors/asia/
| Path | Description |
|------|-------------|
| `asia/` | Asian data collectors |
| `asia/__init__.py` | Package init |
| `asia/mpob_collector.py` | Malaysia Palm Oil Board data |

#### /src/agents/collectors/canada/
| Path | Description |
|------|-------------|
| `canada/` | Canadian data collectors |
| `canada/__init__.py` | Package init |
| `canada/canada_cgc_collector.py` | Canadian Grain Commission data |
| `canada/canada_statscan_collector.py` | Statistics Canada data |

#### /src/agents/collectors/europe/
| Path | Description |
|------|-------------|
| `europe/` | European data collectors |
| `europe/__init__.py` | Package init (empty) |

#### /src/agents/collectors/global/
| Path | Description |
|------|-------------|
| `global/` | Global data collectors |
| `global/__init__.py` | Package init |
| `global/faostat_collector.py` | FAO statistics collector |

#### /src/agents/collectors/market/
*Market data collectors (prices, futures)*

| Path | Description |
|------|-------------|
| `market/` | Market data collectors |
| `market/__init__.py` | Package init |
| `market/cme_settlements_collector.py` | CME settlement prices |
| `market/futures_data_collector.py` | General futures data |
| `market/ibkr_collector.py` | Interactive Brokers data |
| `market/tradestation_collector.py` | TradeStation data |
| `market/yahoo_futures_collector.py` | **[ENHANCED]** Yahoo Finance futures with session capture |

#### /src/agents/collectors/south_america/
*South American data collectors (Brazil, Argentina)*

| Path | Description |
|------|-------------|
| `south_america/` | South American collectors |
| `south_america/__init__.py` | Package init |
| `south_america/abiove_collector.py` | Brazil Veg Oil Association data |
| `south_america/argentina_agent.py` | Argentina trade data |
| `south_america/base_collector.py` | **[NEW]** Base collector for SA |
| `south_america/brazil_agent.py` | Brazil Comex Stat trade data |
| `south_america/brazil_lineup_agent.py` | Brazil vessel lineup |
| `south_america/colombia_agent.py` | Colombia trade data |
| `south_america/conab_collector.py` | **[FIXED]** CONAB Brazil crop estimates |
| `south_america/conab_soybean_agent.py` | CONAB soybean specialist agent |
| `south_america/ibge_sidra_collector.py` | Brazil IBGE statistics |
| `south_america/imea_collector.py` | Mato Grosso economics institute |
| `south_america/magyp_collector.py` | Argentina agriculture ministry |
| `south_america/paraguay_agent.py` | Paraguay trade data |
| `south_america/uruguay_agent.py` | Uruguay trade data |

#### /src/agents/collectors/tenders/
| Path | Description |
|------|-------------|
| `tenders/` | Tender monitoring agents |
| `tenders/__init__.py` | Package init |
| `tenders/alert_system.py` | Tender alert notifications |
| `tenders/wheat_tender_collector.py` | International wheat tenders |

#### /src/agents/collectors/us/
*US government data collectors*

| Path | Description |
|------|-------------|
| `us/` | US data collectors |
| `us/__init__.py` | Package init |
| `us/base_collector.py` | Base US collector |
| `us/census_trade_collector.py` | Census Bureau trade data |
| `us/cftc_cot_collector.py` | CFTC Commitments of Traders |
| `us/drought_collector.py` | US Drought Monitor data |
| `us/eia_ethanol_collector.py` | EIA ethanol production |
| `us/eia_petroleum_collector.py` | EIA petroleum data |
| `us/epa_rfs_collector.py` | EPA Renewable Fuel Standard |
| `us/ers_food_expenditure_collector.py` | ERS food expenditure |
| `us/usda_ams_collector.py` | USDA AMS market news |
| `us/usda_ams_collector_asynch.py` | USDA AMS async version |
| `us/usda_ers_collector.py` | USDA ERS outlook data |
| `us/usda_fas_collector.py` | USDA FAS export sales |
| `us/usda_nass_collector.py` | USDA NASS crop data |

### /src/agents/core/
*Core agent infrastructure*

| Path | Description |
|------|-------------|
| `core/` | Core agent components |
| `core/__init__.py` | Package init |
| `core/approval_manager.py` | Human approval workflow |
| `core/data_agent.py` | Data handling agent |
| `core/database_agent.py` | Database operations agent |
| `core/master_agent.py` | Master orchestration agent |
| `core/memory_manager.py` | Agent memory/context manager |
| `core/transformation_logger.py` | ETL audit logger |
| `core/verification_agent.py` | Data verification agent |

### /src/agents/integration/
*External system integrations*

| Path | Description |
|------|-------------|
| `integration/` | Integration agents |
| `integration/__init__.py` | Package init |
| `integration/calendar_agent.py` | Google Calendar integration |
| `integration/email_agent.py` | Email processing agent |
| `integration/ibkr_api_connector.py` | IBKR API connection |
| `integration/notion_manager.py` | Notion workspace integration |

### /src/agents/reporting/
*Report generation agents*

| Path | Description |
|------|-------------|
| `reporting/` | Reporting agents |
| `reporting/__init__.py` | Package init |
| `reporting/internal_data_agent.py` | Internal data reports |
| `reporting/market_research_agent.py` | Market research agent |
| `reporting/price_data_agent.py` | Price data reports |
| `reporting/report_writer_agent.py` | Report generation agent |

### /src/agents/standalone/
*Standalone agent runner system*

| Path | Description |
|------|-------------|
| `standalone/` | Standalone agent system |
| `standalone/__init__.py` | Package init |
| `standalone/agent.py` | Standalone agent class |
| `standalone/config.py` | Standalone configuration |
| `standalone/run_daemon.bat` | Windows daemon runner |
| `standalone/start_agent.bat` | Windows agent starter |
| `standalone/submit_task.py` | Task submission script |
| `standalone/tasks/.gitkeep` | Task queue directory |
| `standalone/tools.py` | Standalone agent tools |

### /src/agents/tasks/
| Path | Description |
|------|-------------|
| `tasks/` | Agent task queue |
| `tasks/.gitkeep` | Keep directory in git |

---

### /src/orchestrators/
*Pipeline orchestration for multi-step workflows*

| Path | Description |
|------|-------------|
| `orchestrators/` | Pipeline orchestrators |
| `orchestrators/__init__.py` | Package init |
| `orchestrators/conab_soybean_orchestrator.py` | CONAB soybean data pipeline |
| `orchestrators/hb_report_orchestrator.py` | HB report generation pipeline |
| `orchestrators/pipeline_orchestrator.py` | General pipeline orchestrator |
| `orchestrators/trade_data_orchestrator.py` | Trade data pipeline |

---

### /src/scheduler/
*Task scheduling and automation*

| Path | Description |
|------|-------------|
| `scheduler/` | Scheduling system |
| `scheduler/__init__.py` | Package init |
| `scheduler/agent_scheduler.py` | Agent execution scheduler |
| `scheduler/daily_activity_log.py` | Daily activity logging |
| `scheduler/end_of_day_export.bat` | End of day export batch |
| `scheduler/master_scheduler.py` | **[UPDATED]** Master schedule with futures + CONAB |
| `scheduler/README.md` | Scheduler documentation |
| `scheduler/report_scheduler.py` | Report generation scheduler |
| `scheduler/run_scheduler.bat` | Windows scheduler runner |
| `scheduler/setup_windows_tasks.ps1` | Windows Task Scheduler setup |
| `scheduler/trade_scheduler.py` | Trade data scheduler |

#### /src/scheduler/agents/
*Scheduler-specific agents*

| Path | Description |
|------|-------------|
| `agents/` | Scheduler agents |
| `agents/cash_price_collector_agent.py` | Cash price collection |
| `agents/data_checker_agent.py` | Data validation agent |
| `agents/setup_gmail_auth.py` | Gmail authentication setup |
| `agents/weather_collector_agent.py` | Weather data collection |
| `agents/weather_email_agent.py` | Weather email processing |

#### /src/scheduler/config/
| Path | Description |
|------|-------------|
| `config/` | Scheduler configuration |
| `config/weather_email_agent.log` | Weather email log |

#### /src/scheduler/logs/
| Path | Description |
|------|-------------|
| `logs/` | Scheduler logs |
| `logs/scheduler.log` | Main scheduler log |

#### /src/scheduler/tasks/
*Windows Task Scheduler scripts*

| Path | Description |
|------|-------------|
| `tasks/` | Task scripts |
| `tasks/README.md` | Task documentation |
| `tasks/run_agent_scheduler.bat` | Run agent scheduler |
| `tasks/run_data_checker.bat` | Run data checker |
| `tasks/run_weather_collector.bat` | Run weather collector |
| `tasks/run_weather_email.bat` | Run weather email |
| `tasks/setup_windows_tasks.ps1` | PowerShell task setup |

---

### /src/services/
*Shared service modules*

| Path | Description |
|------|-------------|
| `services/` | Shared services |
| `services/__init__.py` | Package init |
| `services/city_enrollment_service.py` | City/location enrollment |
| `services/location_service.py` | Geographic location service |

#### /src/services/api/
*API client services*

| Path | Description |
|------|-------------|
| `api/` | API services |
| `api/__init__.py` | Package init |
| `api/census_api.py` | Census Bureau API client |
| `api/usda_api.py` | USDA API client |
| `api/weather_api.py` | OpenWeather API client |
| `api/world_weather_service.py` | World weather service |

#### /src/services/database/
*Database services*

| Path | Description |
|------|-------------|
| `database/` | Database services |
| `database/__init__.py` | Package init |
| `database/data_loader.py` | Data loading utilities |
| `database/db_config.py` | Database configuration |
| `database/schema.py` | Schema definitions |

#### /src/services/document/
*Document processing services*

| Path | Description |
|------|-------------|
| `document/` | Document services |
| `document/__init__.py` | Package init |
| `document/document_builder.py` | Document generation |
| `document/document_rag.py` | Document RAG system |

---

### /src/tools/
*Shared agent tools*

| Path | Description |
|------|-------------|
| `tools/` | Shared tools |
| `tools/__init__.py` | Package init |
| `tools/agent_tools.py` | Common agent utilities |

---

### /src/utils/
*Utility functions*

| Path | Description |
|------|-------------|
| `utils/` | Utility modules |
| `utils/__init__.py` | Package init |
| `utils/config.py` | Configuration utilities |

---

## /tests/
*Test files and fixtures*

| Path | Description |
|------|-------------|
| `tests/` | Test directory |
| `tests/test_conab_soybean_pipeline.py` | CONAB pipeline tests |
| `tests/test_wheat_tender_collector.py` | Wheat tender tests |
| `tests/collectors/` | Collector test fixtures |
| `tests/collectors/test_sample_food_sales.csv` | Test data file |

---

## /training/
*LLM training materials*

| Path | Description |
|------|-------------|
| `training/` | Training materials |
| `training/CC_TRAINING_GUIDE.md` | Claude Code training guide |
| `training/process_docs/` | Process documentation |
| `training/process_docs/TEMPLATE_process.md` | Process document template |

---

## Summary Statistics

| Category | Count |
|----------|-------|
| **Total Items** | 987 |
| **Directories** | ~150 |
| **Python Files (.py)** | ~150 |
| **SQL Files** | ~25 |
| **Documentation (.md, .docx)** | ~100 |
| **Data Files (PDF, XLSX, CSV)** | ~250 |
| **Configuration Files** | ~20 |
| **Image Files (PNG, GIF, JPG)** | ~50 |
| **PowerBI Files (.pbix, .pbit)** | 6 |
| **Batch/Shell Scripts** | ~15 |

---

## Recent Changes (This Session)

| File | Change |
|------|--------|
| `database/schemas/008_futures_sessions_schema.sql` | **NEW** - Bronze tables for session-based futures OHLC |
| `src/agents/collectors/market/yahoo_futures_collector.py` | **ENHANCED** - Added session capture (overnight/US/settlement) |
| `src/agents/collectors/south_america/base_collector.py` | **NEW** - Base collector for South America |
| `src/agents/collectors/south_america/conab_collector.py` | **FIXED** - Import handling for direct execution |
| `src/scheduler/master_scheduler.py` | **UPDATED** - Added futures session + CONAB schedules |
| `scripts/overnight_runner.py` | **NEW** - Overnight daemon for futures + HB reports |
| `scripts/test_hb_report_generation_v2.py` | **UPDATED** - Added futures + CONAB to prompt |
