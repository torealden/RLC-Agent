# RLC-Agent File & Capability Index (for Felipe's onboarding docs)

**From:** Claude Code | **For:** Desktop to incorporate into Felipe's user guide | **Date:** 2026-07-06
**Verified against the live codebase** at `C:\dev\RLC-Agent` (not guessed). Root is `C:\dev\RLC-Agent`
throughout. **Note:** substantial onboarding material already exists — `docs/user_guide/` (6-part guide
+ appendices), `docs/ONBOARDING_GUIDE.md`, `docs/FILE_INVENTORY_ANNOTATED.md`, `docs/CAPABILITIES_INVENTORY.md`.
Desktop should **build on those**, not duplicate; this index is the current, work-focused subset.

## 1. Models — Felipe's primary work surface (`models/`)
Organized by commodity category. Each folder holds balance sheets (`.xlsx`), data-loading workbooks
(`.xlsm` with VBA), and writer-owned flat files. `~$…` files are Excel lock files — ignore.

| folder | key files | purpose |
|---|---|---|
| `models/Biofuels/` | `us_bbd_combined_bal_sheets.xlsx`, `us_biodiesel_balance_sheets.xlsx`, `us_renewable_diesel_balance_sheets.xlsx`, `us_sustainable_aviation_fuel_balance_sheets.xlsx`, `us_ethanol_balance_sheet.xlsx`, `feedstock_allocation_model.xlsx`, `eia_data.xlsm`, `rfs_data.xlsm`, `us_fuel_trade.xlsm`, `RD Feedstock Build Up.xlsx` | BBD/RD/SAF/ethanol S&D; the feedstock allocation output; EIA + RFS data workbooks |
| `models/Fats and Greases/` | `us_fat_and_grease_balance_sheets.xlsx`, `us_choice_white_grease_balance.xlsx`, `us_distillers_corn_oil_balance.xlsx`, `us_fats_greases_trade.xlsm`, `us_fats_greases_prices.xlsx` | animal-fat & grease S&D + trade (the tallow/UCO/CWG work lands here) |
| `models/Oilseeds/` | `US Oilseed Balance Sheets.xlsx`, `US Oilseed Trade.xlsx`, `World Crush and Other Stuff.xlsx` | soy/canola/etc. S&D, the curated trade backbone, the CIR/crush data |
| `models/Food Grains/` | `us_wheat_production.xlsx` | wheat S&D + the wheat flat file (the country-build template) |
| `models/Feed Grains/`, `models/Cotton/` | corn / cotton production, trade | feed-grain & cotton S&D |
| `models/Macro/`, `models/Population/`, `models/AnimalUnits/` | `World Macro Economic and Population Data.xlsx`, country GDP/pop, animal units | the macro/consumer-strength inputs (UCO/tallow exporter models) |
| `models/templates/`, `models/per_facility/`, `models/yield/` | templates, per-facility, yield models | reusable templates + facility-level work |
| `models/commodities_config.xlsx` | — | master commodity configuration |

## 2. VBA data-loading updaters (`src/tools/*.bas`) — the Excel ⇄ DB bridge
Imported into the `.xlsm` workbooks; each pulls from PostgreSQL via ODBC and writes to the sheet on a
**Ctrl-key shortcut** (quick) / **Ctrl-Shift** (custom). Loaded via `*WorkbookEvents.bas` on open.

| updater | shortcut | loads |
|---|---|---|
| `TradeUpdaterSQL.bas` | Ctrl+I | Census import/export trade |
| `BiofuelDataUpdater.bas` | Ctrl+B | Biofuel S&D |
| `EIAFeedstockUpdater.bas` | Ctrl+D | EIA feedstock |
| `EMTSDataUpdater.bas` / `FeedstockUpdaterSQL.bas` | Ctrl+E | EMTS / feedstock |
| `RINUpdaterSQL.bas` | Ctrl+R | RIN data |
| `FatsOilsUpdaterSQL.bas` | Ctrl+U | NASS Fats & Oils (universal, header-matched) |
| `EnergyTradeUpdater.bas` | Ctrl+Y | fuel trade (`us_fuel_trade.xlsm`) |
| `CornGrindUpdater.bas` | Ctrl+K | corn grind co-products |
| others | — | `BiofuelFeedstockTradeUpdater`, `CornProductsUpdater`, `ExportSalesUpdaterSQL`, `GrainTradeUpdater`, `InspectionsUpdaterSQL`, `LivestockUpdaterSQL`, `EthanolUpdater` |

## 3. Data pipeline (`scripts/`, 310 files; `src/agents/collectors/`, 89 collectors)
- **Collectors** (`src/agents/collectors/`) — pull from source APIs (USDA NASS/FAS, Census, EIA, EPA,
  CONAB, StatsCan, World Bank, etc.). All inherit `BaseCollector`; registered in the collector registry.
- **Silver builders** — `build_silver_*.py` (wheat_series, animal_slaughter, animal_fat_production,
  food_expenditure, tallow_production, uco_imports…): bronze → tidy silver.
- **Ingesters** — `ingest_*.py` (cir_fats, ers_data, corn_trade, fao_population…): load external files.
- **Flat-file writers** — `write_wheat_flat_files.py` (pattern): silver → `models/**/…xlsx` per contract.
- **Feedstock engine** — `src/engines/feedstock_allocation/allocator.py` + `scripts/rake_feedstock_to_eia.py`.

## 4. Database — PostgreSQL on AWS RDS (medallion: bronze→silver→gold)
- **Connect:** `.env` at root holds `RLC_PG_HOST` + all API keys; `src/services/database/db_config.py`
  `get_connection()` (context manager).
- **Schemas/migrations:** DDL in `database/schemas/` (001–007); incremental in `database/migrations/`
  (132 files).
- **Layers:** `bronze.*` raw ingested, `silver.*` cleaned/standardized, `gold.*` analytics views.
  Live counts ~89 bronze / ~93 silver / ~180 gold (per `CLAUDE.md`; re-verify before quoting).
- **Query tool:** `python src/tools/db_query.py "<SQL>"`; MCP server `src/mcp/commodities_db_server.py`.

## 5. Specs & contracts (`docs/specs/`)
- `flat_file_contract.md` — **the LONG/vintage-ladder/MAXIFS contract every flat file follows** (v1.1).
- `bbd_feedstock_system_design_v1_6_LOCKED.md` — the feedstock allocation architecture.
- `UCO_ruling_doc_and_contract.md`, `tallow_ruling_doc_and_contract.md` (+ addendum) — the UCO/tallow methodology.
- `felipe_onboarding_desktop_brief.md` — the onboarding plan this index supports.

## 6. Domain knowledge (`domain_knowledge/`)
Data dictionaries (`data_dictionaries/` — PSD/EIA/EPA/HS-code references), crop maps, crop calendars,
balance-sheet templates, sample reports, special-situation case studies. The reference layer behind the models.

## 7. Config, credentials, conventions
- `.env` (root) + `dashboards/ops/.env` — DB + API keys (NOT in git; hand-delivered).
- `models/commodities_config.xlsx`, `config/` — commodity/pipeline config.
- **Conventions Felipe must know:** flat files sort ascending (VLOOKUP stability) + `_meta` tab;
  internal xlsx header fill `#3C7D22`; store bronze in source units, convert for display;
  **never surface Fastmarkets-sourced values in client-facing material**; commit+push after each chunk.

## 8. Operations & scheduling
- **Ops dashboard:** `dashboards/ops/app.py` (`streamlit run …` or `scripts/launch_data_dashboard.bat`).
- **Scheduler/dispatcher:** `rlc_scheduler/`, `src/schedulers/` (current — NOT `src/scheduler/` legacy);
  Windows Scheduled Tasks under `\RLC\`.
- **Verify setup:** `python scripts/verify_setup.py`.

## 9. Knowledge Graph & LLM tooling
- `src/kg/` (callable invoker, forecast book), `src/knowledge_graph/`, MCP server (`src/mcp/`).
- KG = analyst frameworks queried via MCP tools (`get_kg_context`, `search_knowledge_graph`); see `CLAUDE.md` §CNS.

## 10. Existing onboarding docs to build on (don't duplicate)
`docs/user_guide/` (01_GETTING_STARTED … 06_LLM_INTEGRATION + APPENDIX_A_FILE_LIST / B_API_KEYS /
C_DATABASE_REFERENCE / D_TROUBLESHOOTING), `docs/ONBOARDING_GUIDE.md`, `docs/FILE_INVENTORY_ANNOTATED.md`,
`docs/CAPABILITIES_INVENTORY.md`, `docs/DATA_SOURCE_REGISTRY.md`, `docs/POWER_BI_SETUP_GUIDE.md`.
**Gap Desktop should fill for Felipe:** these predate the flat-file-contract + LLM-assisted-country-build
workflow — the new material is the *repeatable loop* (source API → schema → load → flat file → verify),
not another file inventory.
