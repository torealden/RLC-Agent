# RLC-Agent — Consolidated Memory Archive

Durable, git-versioned snapshot of the agent memory register (143 files). Secrets redacted. Regenerate with `python scripts/consolidate_memory_archive.py`.

## Contents

- [MEMORY](#memory)
- [balance_sheet_infrastructure](#balance-sheet-infrastructure)
- [balance_sheet_workflow](#balance-sheet-workflow)
- [dispatcher](#dispatcher)
- [feedback_allocation_engine_design](#feedback-allocation-engine-design)
- [feedback_always_load_dotenv](#feedback-always-load-dotenv)
- [feedback_balance_sheet_detail](#feedback-balance-sheet-detail)
- [feedback_bitdefender_workflow](#feedback-bitdefender-workflow)
- [feedback_census_trade_verification](#feedback-census-trade-verification)
- [feedback_chart_preferences](#feedback-chart-preferences)
- [feedback_chatgpt_frustrations](#feedback-chatgpt-frustrations)
- [feedback_client_process_separation](#feedback-client-process-separation)
- [feedback_collect_must_persist](#feedback-collect-must-persist)
- [feedback_commit_push_notion_proactively](#feedback-commit-push-notion-proactively)
- [feedback_daily_three_discipline](#feedback-daily-three-discipline)
- [feedback_data_reconciliation_hierarchy](#feedback-data-reconciliation-hierarchy)
- [feedback_fastmarkets_keep_dont_show](#feedback-fastmarkets-keep-dont-show)
- [feedback_gate_beats_parameter](#feedback-gate-beats-parameter)
- [feedback_gitignore_shared_files](#feedback-gitignore-shared-files)
- [feedback_honest_pushback](#feedback-honest-pushback)
- [feedback_llm_extraction_variance](#feedback-llm-extraction-variance)
- [feedback_marketing_years](#feedback-marketing-years)
- [feedback_migrations_kill_builds](#feedback-migrations-kill-builds)
- [feedback_orphan_code_hunt](#feedback-orphan-code-hunt)
- [feedback_read_errors_fully](#feedback-read-errors-fully)
- [feedback_rehearse_important_meetings](#feedback-rehearse-important-meetings)
- [feedback_spreadsheets_as_trade_matrices](#feedback-spreadsheets-as-trade-matrices)
- [feedback_units_source_vs_display](#feedback-units-source-vs-display)
- [feedback_vba_module_name_attribute](#feedback-vba-module-name-attribute)
- [feedback_verify_dont_assume](#feedback-verify-dont-assume)
- [feedback_weekly_update_report](#feedback-weekly-update-report)
- [feedstock_allocation_engine](#feedstock-allocation-engine)
- [mpob_data](#mpob-data)
- [project_a2a_debate_architecture](#project-a2a-debate-architecture)
- [project_abiove_brazil_soy_complex](#project-abiove-brazil-soy-complex)
- [project_agp_calibration_target](#project-agp-calibration-target)
- [project_agp_completion_status_2026_05_09](#project-agp-completion-status-2026-05-09)
- [project_balance_sheet_framework](#project-balance-sheet-framework)
- [project_balance_sheet_roadmap](#project-balance-sheet-roadmap)
- [project_basic_data_setup_sequence](#project-basic-data-setup-sequence)
- [project_basis_field](#project-basis-field)
- [project_bbd_feedstock_primary_market](#project-bbd-feedstock-primary-market)
- [project_bd_rd_trade_split](#project-bd-rd-trade-split)
- [project_calendar_year_vs_marketing_year](#project-calendar-year-vs-marketing-year)
- [project_conference_deadline](#project-conference-deadline)
- [project_copra_complex_trade](#project-copra-complex-trade)
- [project_corn_grind_pipeline](#project-corn-grind-pipeline)
- [project_corn_oil_balance_sheet_followup](#project-corn-oil-balance-sheet-followup)
- [project_crush_model](#project-crush-model)
- [project_cwg_import_collapse_2025](#project-cwg-import-collapse-2025)
- [project_db_password_rotation](#project-db-password-rotation)
- [project_dco_corn_oil_trade_split](#project-dco-corn-oil-trade-split)
- [project_dco_estimation_from_ethanol](#project-dco-estimation-from-ethanol)
- [project_dod_security_posture](#project-dod-security-posture)
- [project_drew_lerner_archive_backfill](#project-drew-lerner-archive-backfill)
- [project_dual_track_views](#project-dual-track-views)
- [project_eagle_grove_deferred_items](#project-eagle-grove-deferred-items)
- [project_facility_agent_leaderboard](#project-facility-agent-leaderboard)
- [project_facility_agent_model](#project-facility-agent-model)
- [project_facility_data_strategy](#project-facility-data-strategy)
- [project_facility_external_xref](#project-facility-external-xref)
- [project_facility_weather_summary](#project-facility-weather-summary)
- [project_fats_greases_buildout](#project-fats-greases-buildout)
- [project_feedstock_forecast_method](#project-feedstock-forecast-method)
- [project_feedstock_forward_projections](#project-feedstock-forward-projections)
- [project_ffa_feedstock_layer](#project-ffa-feedstock-layer)
- [project_forecast_comparison](#project-forecast-comparison)
- [project_forecast_philosophy](#project-forecast-philosophy)
- [project_fuel_flat_files](#project-fuel-flat-files)
- [project_helios_friday_demo](#project-helios-friday-demo)
- [project_helios_pepsi_pilot](#project-helios-pepsi-pilot)
- [project_iowa_multi_industry_expansion](#project-iowa-multi-industry-expansion)
- [project_kg_callable_architecture](#project-kg-callable-architecture)
- [project_launch_timeline](#project-launch-timeline)
- [project_liquid_fuel_stocks_workflow](#project-liquid-fuel-stocks-workflow)
- [project_market_field_spec](#project-market-field-spec)
- [project_minor_oils_coverage](#project-minor-oils-coverage)
- [project_next_phase_ops_audit](#project-next-phase-ops-audit)
- [project_next_steps_regional_bs](#project-next-steps-regional-bs)
- [project_oil_crops_annual_summary](#project-oil-crops-annual-summary)
- [project_open_followups_2026-06](#project-open-followups-2026-06)
- [project_ops_audit_plan](#project-ops-audit-plan)
- [project_permit_archive](#project-permit-archive)
- [project_permit_parsing_secret_sauce](#project-permit-parsing-secret-sauce)
- [project_phase_two_agent_architecture_detail](#project-phase-two-agent-architecture-detail)
- [project_phase_two_facility_agents](#project-phase-two-facility-agents)
- [project_phase_two_vision](#project-phase-two-vision)
- [project_plant_intelligence](#project-plant-intelligence)
- [project_public_filings_extraction](#project-public-filings-extraction)
- [project_quarterly_var_risk_budget](#project-quarterly-var-risk-budget)
- [project_reg_ralston_madison_idle](#project-reg-ralston-madison-idle)
- [project_rlc_2026_mandates](#project-rlc-2026-mandates)
- [project_roadmap_master](#project-roadmap-master)
- [project_roadmap_oilseeds_grains](#project-roadmap-oilseeds-grains)
- [project_saf_research_notes](#project-saf-research-notes)
- [project_saf_trade_tracking](#project-saf-trade-tracking)
- [project_safflower_discontinuation](#project-safflower-discontinuation)
- [project_sbe_analysis](#project-sbe-analysis)
- [project_seasonal_monthly_projections](#project-seasonal-monthly-projections)
- [project_state_air_permits_llm](#project-state-air-permits-llm)
- [project_streaming_futures_feed](#project-streaming-futures-feed)
- [project_symbiotic_forecasting](#project-symbiotic-forecasting)
- [project_tallow_split](#project-tallow-split)
- [project_trade_sheet_splits_deferred](#project-trade-sheet-splits-deferred)
- [project_uco_yg_model](#project-uco-yg-model)
- [project_usda_feedstock_supply_gaps](#project-usda-feedstock-supply-gaps)
- [project_vision_endpoints](#project-vision-endpoints)
- [project_weather_city_foundation](#project-weather-city-foundation)
- [project_wheat_country_build](#project-wheat-country-build)
- [project_yield_reconciliation](#project-yield-reconciliation)
- [reference_ams_coverage_gaps](#reference-ams-coverage-gaps)
- [reference_bbd_feedstock_eia_canonical](#reference-bbd-feedstock-eia-canonical)
- [reference_brazil_my_alignment](#reference-brazil-my-alignment)
- [reference_bronze_fuel_prices_provenance](#reference-bronze-fuel-prices-provenance)
- [reference_carb_pathway_selection_bias](#reference-carb-pathway-selection-bias)
- [reference_census_import_export_hs_codes](#reference-census-import-export-hs-codes)
- [reference_conab_direct_downloads](#reference-conab-direct-downloads)
- [reference_crop_condition_methodology](#reference-crop-condition-methodology)
- [reference_dual_claude_notion_coordination](#reference-dual-claude-notion-coordination)
- [reference_echo_canonical_facility_source](#reference-echo-canonical-facility-source)
- [reference_emts_manual_export](#reference-emts-manual-export)
- [reference_excel_color_conventions](#reference-excel-color-conventions)
- [reference_felipe_weekly_cash_prices](#reference-felipe-weekly-cash-prices)
- [reference_govt_shutdown_data_handling](#reference-govt-shutdown-data-handling)
- [reference_high_ffa_feedstock_biofuel_limit](#reference-high-ffa-feedstock-biofuel-limit)
- [reference_history_start_dates](#reference-history-start-dates)
- [reference_idem_oracle_webcenter_permits](#reference-idem-oracle-webcenter-permits)
- [reference_local_vs_cloud_llm](#reference-local-vs-cloud-llm)
- [reference_oil_crops_yearbook_units](#reference-oil-crops-yearbook-units)
- [reference_ollama_gpu_cpu_fallback](#reference-ollama-gpu-cpu-fallback)
- [reference_peanut_conversion_and_modeling](#reference-peanut-conversion-and-modeling)
- [reference_session_handoff_2026-05-25](#reference-session-handoff-2026-05-25)
- [reference_state_permit_portals](#reference-state-permit-portals)
- [reference_us_oilseed_unit_convention](#reference-us-oilseed-unit-convention)
- [reference_usda_food_expenditure_reality_check](#reference-usda-food-expenditure-reality-check)
- [reference_xlsx_flat_file_conventions](#reference-xlsx-flat-file-conventions)
- [user_career_history](#user-career-history)
- [user_career_legal](#user-career-legal)
- [user_freddie](#user-freddie)
- [user_hardware_ollama](#user-hardware-ollama)
- [user_joy_and_motivation](#user-joy-and-motivation)
- [user_motivation](#user-motivation)
- [user_values_national_interest](#user-values-national-interest)

---

## MEMORY

*(`MEMORY.md`)*

# RLC-Agent Project Memory

## ⚠️ READ FIRST — Honest evaluation rule
Tore requires honest evaluation, never flattery. No "great question!", no reflexive
agreement, no self-deprecation reassurance. Push back when warranted, with rationale
and alternatives. RLHF biases LLMs toward agreeable responses; this instruction
counteracts that. Full directive in `CLAUDE.md` and `feedback_honest_pushback.md`.
**Honor this rule across every interaction.**

## Dispatcher Operations
- See [dispatcher.md](dispatcher.md) for dispatcher architecture and debugging notes
- See [dashboard.md](dashboard.md) for ops dashboard details
- **Watchdog task** `\RLC\RLC Dispatcher Watchdog` — checks every 15 min, restarts dispatcher if dead (registered 2026-03-11)
- CLI `cmd_start()` now writes PID file to `scripts/deployment/dispatcher.pid`
- Watchdog uses PowerShell `Get-CimInstance` (not deprecated `wmic`) for process detection

## Key Patterns
- All collectors inherit from `src.agents.base.base_collector.BaseCollector`
- Sub-directories re-export via local `base_collector.py` files (us/, market/, south_america/)
- `.env` at project root has all API keys — must call `load_dotenv()` in entry points
- DB connection: `src.services.database.db_config.get_connection()` (psycopg2 context manager)
- **DB host**: All connections use `RLC_PG_HOST` env var (AWS RDS). Fixed 35+ files on 2026-03-19 that were using `DB_HOST=localhost`.
- Two master_scheduler files exist: `src/schedulers/` (current) and `src/scheduler/` (legacy) — always use `src.schedulers`
- See [feedback_gitignore_shared_files.md](feedback_gitignore_shared_files.md) — `data/`, `logs/`, `collectors/epa_echo/output/` are gitignored. Use `output/` or `templates/` for shared files.

## User Preferences
- User wants German-car-level operational reliability
- Interested in Six Sigma KPIs for collection operations
- Prefers Streamlit for dashboards (Python-native, no frontend build)

## Streamlit API Notes
- `st.dataframe()`: use `width="stretch"` NOT `use_container_width=True` (deprecated, removed after 2025-12-31)

## VBA Spreadsheet Updater Pattern
- Excel workbooks connect to PostgreSQL via ODBC (psqlODBC x64), sslmode=require for RDS
- **`.bas` import gotcha**: see [feedback_vba_module_name_attribute.md](feedback_vba_module_name_attribute.md). Every `.bas` meant to be imported into a workbook should start with `Attribute VB_Name = "ExpectedName"` on line 1, or qualified calls like `EnergyTradeUpdater.AssignEnergyShortcuts` will fail with error 424 on re-import collisions. Fixed for EnergyTradeUpdater.bas 2026-05-13; other .bas files in src/tools/ should be audited opportunistically.
- Keyboard shortcuts via `Application.OnKey` in VBA:
  - Ctrl+I = Census trade (TradeUpdaterSQL.bas)
  - Ctrl+B = Biofuel S&D (BiofuelDataUpdater.bas)
  - Ctrl+D = EIA Feedstock (EIAFeedstockUpdater.bas)
  - Ctrl+E = EMTS/Feedstock (EMTSDataUpdater.bas / FeedstockUpdaterSQL.bas)
  - Ctrl+R = RIN data (RINUpdaterSQL.bas)
  - Ctrl+U = **FatsOilsUpdaterSQL.bas** (universal, replaces CrushUpdaterSQL.bas)
  - Ctrl+Y = **EnergyTradeUpdater.bas** (`us_fuel_trade.xlsm` — biodiesel/RD/SAF/ethanol/methanol trade)
  - Ctrl+G = FGIS Inspections (TODO — not yet built)
- Pattern: WorkbookEvents.bas calls AssignKeyboardShortcuts on open, removes on close
- Quick update (Ctrl+letter) + Custom update (Ctrl+Shift+letter) pair
- **Header-matching pattern (new)**: FatsOilsUpdaterSQL reads row 3 headers at runtime,
  matches to `header_pattern` in crush_attribute_reference. Commodity auto-detected from sheet name.
  No hardcoded column positions — works across all commodity sheets.
- Gold view: `gold.fats_oils_crush_matrix` (generic, all commodities)
- Backward compat: `gold.nass_soy_crush_matrix` still exists for soy-specific queries
- Commodities configured: soybeans(18+3=21), canola(6+3=9), corn(11 incl. alts), cottonseed(5+3=8), sunflower(5), peanut(10 incl. alts), palm(2)
- **Corn `_alt` filter fix (mig 030, 2026-04-25)**: 3 corn `_alt` rows pointed to non-existent NASS short_descs (`ONSITE & OFFSITE, CRUDE`, `REFINED` w/o `ONCE`). Fix: re-point alt rows to PRIMARY's filter so both header_patterns return data. Now NASS Other Veg Oils corn block (Crude Stocks, Refined Produced, Refined Stocks) updates correctly. Same pattern likely needed for peanut/refined_oil_stocks_alt — but no NASS data exists for that metric at all.
- **FatsOilsUpdaterSQL upgrades (2026-04-25)**: Ctrl+Shift+U now offers "cursor-block only" on multi-commodity tabs; result message shows the Excel headers actually read so unmatched failures are diagnosable; column letters (V, AA) shown instead of numeric indices. **Workbook needs re-import** of FatsOilsUpdaterSQL.bas via VBE to pick up changes.
- **Corn oil units fix (mig 031, 2026-04-25)**: Corn 11 reference rows changed from `mil lbs` (factor 0.000001) to `000 lbs` (factor 0.001). Aligns with global rule: flat files store at 1,000x SMALLER unit than balance sheet displays.
- **Minor oils unit fix (mig 032, 2026-04-25)**: extended same `mil lbs → 000 lbs` migration to palm, palm_kernel, safflower, coconut, and peanut oil-only rows. After mig: palm refined consumption = 174,905 (000 lbs) ≈ 175 mil lbs — matches expected magnitude.
- **NASS Fats & Oils backfill (2026-04-25)**: confirmed NASS QuickStats API horizon is **2014/2015+ for fats & oils** (2017 for safflower). Tried 2000-2024 backfill via scripts/backfill_nass_fats_oils.py — 120K upserts but no new historical rows because NASS returns only 2015+ data regardless of year__GE param. **Pre-2014 history requires ERS Oil Crops Annual Summary PDF parsing** (PDFs in data/raw/oilseeds_fats_greases/) — flagged as future work, not yet done.
- **Bushels for oilseeds & grains rule (2026-04-25)**: established convention — oils/fats in mil lbs, oilseed/grain crush volumes in mil bu. Per-commodity bu/ton conversion factors in `silver.bbd_seed_unit_ref`.
- RDS sync required: views and reference data must be synced to RDS for VBA updaters to work
- `ThisWorkbook.Workbook_Open` must call `AssignFatsOilsShortcuts` (not old `AssignCrushShortcuts`)
- Fats/greases NOT yet in NASS collector — need to add tallow, lard, CWG, yellow grease, poultry fat, etc.
- Coconut, safflower, palm kernel NOT yet in NASS data — may need separate collector config

## Census Trade Collector
- **v1** (`census_trade_collector.py`) is what's registered in collector_registry — now has `save_to_bronze` + `collect()` (added 2026-03-11)
- **v2** (`census_trade_collector_v2.py`) exists with more features but is NOT registered — consolidation needed
- Export quantity field: `QTY_1_MO` (not `QY1_MO` which returns 400 error)
- Import quantity field: `GEN_QY1_MO`
- Census FT-900 release dates added to `CENSUS_RELEASE_DATES` dict in master_scheduler.py
- 14 HS codes now covered (was 5)
- See [reference_census_import_export_hs_codes.md](reference_census_import_export_hs_codes.md) — imports=HTSUS, exports=Schedule B; codes diverge below HS6. Registering export codes for IMPORTS yields ~0 import data (corn was 40x low). mig 135 fixed corn; mig 136 = all commodities. Watch multi-group HS6.
- See [project_corn_grind_pipeline.md](project_corn_grind_pipeline.md) — GCCP co-products are PDF-only (not QuickStats). PDF parser → bronze → gold.corn_grind_monthly (mig 137) → CornGrindUpdater.bas (Ctrl+K). Positional+section-bounded extraction + in-doc narrative QC = the permit-PDF testbed. TODO: dispatcher registration.
- census_trade_collector now uses chunked range queries (not month-by-month) — ~12x faster backfills (commit 3f733ad5). Import audit mig 136 = wheat/barley; Tier 2/3 (oilseeds/fuels, multi-group, FLAXSEED/230800 mismap) still queued.

## Collector Fix Log (2026-03-11)
- **Drought monitor**: FIPS codes (not state abbreviations), CSV parsing, schema `bronze.drought_conditions`
- **Canada CGC**: Rewritten from HTML scraping to CSV downloads, 138K rows, schema `bronze.canada_cgc_weekly` + `bronze.canada_cgc_exports`
- **Canada StatsCanada**: Rewritten from broken WDS API to CSV bulk download, 57K rows, schema `bronze.canada_statscan`
- **FAS Export Sales**: SSL `verify=False` workaround for expired USDA cert at apps.fas.usda.gov
- **EPA RFS**: Disabled (`enabled=False`) — manual download for now
- **yfinance_futures**: Added daily schedule at 5:15 PM ET (was in registry but not scheduled)

## Historical Data Coverage (as of 2026-03-11)
- CFTC COT: 55,993 rows, Jan 1986 - Mar 2026 (legacy + disaggregated), 21 commodities
- FGIS Inspections: 534K rows, 1990 - Mar 2026, all grains
- FAS Export Sales: 1.19M rows, 1999 - Feb 2026
- Census Trade: 470K rows, 2013 - Dec 2025
- FAS PSD: 9.5K rows, MY 2020-2025 — **needs backfill to 2000+**
- NASS Crop Progress/Condition: ~1 season only — **needs backfill to 2000+**
- Cash Prices: 50K rows, Feb 2025 - Mar 2026 (limited by AMS API)

## Project Vision
- See [project_vision_endpoints.md](project_vision_endpoints.md) — two strategic endpoints: (1) spreadsheet S&D forecasting with human-vs-LLM accuracy tracking, (2) LLM-generated content (reports, graphics, webinars) for biomass-based diesel feedstock markets

## Feedback
- See [feedback_weekly_update_report.md](feedback_weekly_update_report.md) — Friday 5pm weekly update to Notion (RLC OS page), format per Apr 7 update
- See [feedback_units_source_vs_display.md](feedback_units_source_vs_display.md) — store bronze in source units, convert via conversion_factor for spreadsheet display only. Cottonseed numbers were wrong.
- See [feedback_census_trade_verification.md](feedback_census_trade_verification.md) — bronze.census_trade conventions (kg, country_code='-'), and the All-Exports vs Domestic-Exports definition gap. DB exports include re-exports; UATO files don't.
- See [feedback_read_errors_fully.md](feedback_read_errors_fully.md) — read complete error messages before diagnosing, don't jump to conclusions from partial patterns
- See [feedback_verify_dont_assume.md](feedback_verify_dont_assume.md) — when user questions if something works, verify with evidence before asserting it's fine
- See [feedback_gitignore_shared_files.md](feedback_gitignore_shared_files.md) — never save shared files to gitignored dirs (data/, logs/, collectors/*/output/)
- See [feedback_bitdefender_workflow.md](feedback_bitdefender_workflow.md) — don't change tooling to placate Bitdefender. Fix BD via exclusions, not workflow workarounds.
- See [feedback_commit_push_notion_proactively.md](feedback_commit_push_notion_proactively.md) — commit and tee up GitHub pushes proactively after each logical chunk. Pair with Notion update. Tore is paranoid about losing work.
- See [feedback_data_reconciliation_hierarchy.md](feedback_data_reconciliation_hierarchy.md) — EIA/Census = canon. Reconcile UP toward them. Where EIA splits (BD vs RD), use it; where it doesn't, split ours, first-cut proportional to fuel production. The supply↔production gap is itself signal once fuel balance sheets are real.
- See [feedback_client_process_separation.md](feedback_client_process_separation.md) — each client report = its own orchestrator/prose/brand/delivery. Shared = data layers, KG, callables, chart primitives, reference tables. Don't conflate HB report with Feedstock Report or any other client publication.
- See [feedback_fastmarkets_keep_dont_show.md](feedback_fastmarkets_keep_dont_show.md) — Keep all FM-era data in DB for internal triangulation, NEVER show in client-facing material. Filter source='fastmarkets' out of any client-facing consumer. Facility-agent real-time allocation = eventual bible.
- See [feedback_honest_pushback.md](feedback_honest_pushback.md) — be direct, not flattering. User works solo and needs honest criticism, not validation.
- See [feedback_gate_beats_parameter.md](feedback_gate_beats_parameter.md) — when a ruled parameter conflicts with a ruled objective gate (acceptance test/seam check/band), the gate wins: implement-with-flag + escalate. Scope data-quality exclusions to the defect.
- See [feedback_migrations_kill_builds.md](feedback_migrations_kill_builds.md) — directory relocations silently break scheduled tasks, .bat paths, hardcoded config strings. Always do full migration-completeness pass, check LastTaskResult.
- See [feedback_orphan_code_hunt.md](feedback_orphan_code_hunt.md) — before building a collector, grep ALL scheduler paths (rlc_scheduler, src/scheduler, src/schedulers) + collector_registry.py. Found ~1700 lines of orphan collectors (GFS, GEFS, NDVI) that just needed registration.
- See [feedback_llm_extraction_variance.md](feedback_llm_extraction_variance.md) — LLM single-run extraction has 50-70% variance bidirectionally on long structured docs. Best-of-N (N≥3 union by unit_id) required for production. Hybrid regex+LLM is the right architecture. Never `--force` overwrite without versioning. Bronze loader should refuse to load fewer-units run.
- See [feedback_collect_must_persist.md](feedback_collect_must_persist.md) — BaseCollector.collect() only fetches, doesn't save. Subclasses MUST override collect() or dispatcher silently runs without persisting. AMS broke May 4-18 from this. Audit all collectors via "fetch only with no override" check.
- See [reference_felipe_weekly_cash_prices.md](reference_felipe_weekly_cash_prices.md) — Weekly cash prices to Felipe: Scheduled Task `\RLC\Weekly Cash Prices to Felipe` Wed 6:30pm ET → generates xlsx → copies to Dropbox HigbyBarrett\weekly_cash_prices → Gmail-API emails Felipe+Tore (NOT SMTP, app-pass broken).
- See [reference_dual_claude_notion_coordination.md](reference_dual_claude_notion_coordination.md) — Tore runs Claude-UI (this) + Claude-Content (Desktop) in parallel. Notion = shared source of truth. Page-per-project, Decision Log + §N.A Responses pattern. IFVS spec at notion.so/365ead023dee813daee1e31b22219327.

- See [reference_brazil_my_alignment.md](reference_brazil_my_alignment.md) — USDA artificially aligns Brazil MY to US (Sep-Aug); actual Brazil safra leads US (Feb-Jan). Detect via monthly data. Ingest Brazil data by calendar year, let analyst pick MY framing.
- See [reference_conab_direct_downloads.md](reference_conab_direct_downloads.md) — CONAB exposes 6 public files at portaldeinformacoes.conab.gov.br/downloads/arquivos/: Frete, PrecoMinimo, CustoProducao, OfertaDemanda, Estoques, ArmazensCadastrados (8.2 MB / 18,766 warehouses). Replaces Tore's copy-paste workflow. Task #71.

- See [reference_excel_color_conventions.md](reference_excel_color_conventions.md) — Internal xlsx (Tore's models): header fill `#3C7D22` (green), bold white Calibri. Client-facing artifacts: brand kit INK/GOLD/PAPER. Don't mix.
- See [reference_xlsx_flat_file_conventions.md](reference_xlsx_flat_file_conventions.md) — Generated flat files: rows SORT ASCENDING (old→new) so latest year is at stable bottom address for VLOOKUP. Always include `_meta` tab. Quarterly interpolation Q1/Q2/Q3/Q4 offsets 0.125/0.375/0.625/0.875.
- See [reference_oil_crops_yearbook_units.md](reference_oil_crops_yearbook_units.md) — USDA Oil Crops Yearbook: soybeans mil bu, meal thou ST, oil mil lbs. Same workbook, three scales. Report published units, don't silently convert.
- See [reference_us_oilseed_unit_convention.md](reference_us_oilseed_unit_convention.md) — US oilseed BS display units (oil=mil lbs, meal=000 ST, seed varies) + input-sheet = BS÷1000 rule + DB conversion_factor math. Migration 133 standardized all 5 oilseeds 2026-06-11.
- See [reference_history_start_dates.md](reference_history_start_dates.md) — Project default history horizon: oilseeds/grains start Oct 1993 (US soy MY 1993/94), energies start Jan 1993. Use as default in every new workbook/backfill unless asked otherwise.
- See [reference_ams_coverage_gaps.md](reference_ams_coverage_gaps.md) — AMS slugs 2837/2839/3510 (added 2026-05-28) cover tallow, CWG, yellow grease, lard, MBM, blood meal, feathermeal since 2022. UCO, brown grease, poultry fat, all veg oils, and intl prices are NOT in AMS — need broker emails or other sourcing.
- See [feedback_rehearse_important_meetings.md](feedback_rehearse_important_meetings.md) — Tore's standing practice as of 2026-05-22: live-rehearse every important external meeting beforehand. Q&A script + Claude playing the other party + out-of-character debrief + script update. Offer proactively for client/partner/M&A-adjacent meetings.
- See [feedback_daily_three_discipline.md](feedback_daily_three_discipline.md) — Tore adopted Musk's "3 most important things per day + 80/20 signal/noise" discipline 2026-06-05. Beginning of session: ask "what are today's 3?". Mid-session: flag once when a request is noise vs signal, don't refuse. End: brief retrospective. Push back on time-% tracking; use outcomes instead.
- See [project_rlc_2026_mandates.md](project_rlc_2026_mandates.md) — **Mandates locked 2026-06-08.** M1=balance sheets/Excel, M2=Feedstock Facility Agent (FFA) network, M3=Be a better leader (Speaking/Fitness/Writing tracks, active-not-passive bar). Daily logs at `docs/daily_log/YYYY-MM-DD.md`. Fitness routine at `docs/daily_log/fitness_routine.md` + tracking xlsx.

## Industry People Directory
- Running who's-who at `docs/industry_people_directory.md` — people + titles in the feedstock/biofuel/ag-analytics industry, dated + sourced so role changes are trackable. **Append over time** (facility staff, firm contacts). Seeded 2026-07-01 (Western Dubuque staff + Helios roster).

## Wheat / Country Balance-Sheet Builds
- See [project_wheat_country_build.md](project_wheat_country_build.md) — wheat pilot for per-country commodity balance sheets. Dual-Claude: Code=plumbing (bronze→silver→writers), Desktop=workbooks/formulas, flat file=seam. Contract `docs/specs/flat_file_contract.md` v1.1 (LONG default, WIDE trade, vintage_rank + MAXIFS/SUMIFS). Supply side DONE (area/production+5 market classes/yield/stocks); TODO milling(M311J)/trade/co-products. Next country: Brazil.

## Abiove / Brazil Soy Complex
- See [project_abiove_brazil_soy_complex.md](project_abiove_brazil_soy_complex.md) — Brazil soy-complex ingest live 2026-07-10 (crush/meal+oil prod/stocks, thousand MT). Manual Power BI extract (no API). bronze.abiove_soy_complex (mig 142) → silver.monthly_realized BR/ABIOVE → gold (mig 143) → models/Oilseeds/Brazil/brazil_soy_complex_monthly.xlsx. Schema rule: new bronze, reuse silver, new gold. Runbook: docs/runbooks/abiove_update_runbook.md

## Helios / Pepsi
- See [project_helios_pepsi_pilot.md](project_helios_pepsi_pilot.md) — 2026-06-29 Helios meeting outcome: Pepsi = first pilot (canola/soy/sunflower price+reasons). RLC=demand-side feedstock strength, Helios=climate/weather. Open: João interface, delivery shape/horizon, "Intel Inside" attribution (disintermediation watch). CTO showed unexpectedly.

## Filesystem Boundary
- **Primary workspace**: `C:\dev\RLC-Agent` (all code, specs, scripts, migrations).
- **Dropbox**: artifact-delivery surface only. Two valid write paths: `Tore Alden\HigbyBarrett\weekly_cash_prices\` (Felipe doesn't have Tailscale) and `Tore Alden\Misc Personal Stuff\Helios\` (deck + leave-behind for meeting). Everything else goes in dev.
- Before quoting KG/DB stats in client-facing material, run `scripts/_check_kg_counts.py` — CLAUDE.md drifts.

## Pending Commitments
- See [project_db_password_rotation.md](project_db_password_rotation.md) — Tore rotating RDS password ([REDACTED-SECRET]). **Remind Friday 2026-07-10 if not done by end Thu 2026-07-09.** Rotation breaks ~45 hardcoded files (env + ~15 py + ~20 VBA .bas + ODBC); do literal→env-var pass at rotation time.

## Priority Queue
1. ~~**FAS PSD backfill**~~ — **RESOLVED**: 53,847 rows, 22 commodities, 1990-2025, 175+ countries
2. ~~**NASS crop progress/condition backfill**~~ — **RESOLVED**: 24,446 records, 6 commodities, 2000-2025 national level
3. **Inspections VBA updater** (Ctrl+G) — waiting on user's spreadsheet template
4. **Dashboard: commodity coverage by country** matrix
5. **Project structure cleanup** per rlc-agent_project_structure_proposal_combined.docx

## EMTS Manual Export
- See [reference_emts_manual_export.md](reference_emts_manual_export.md) — EMTS fuel-volume data (allocator driver) = **manual monthly EPA Qlik export**, NOT automatable (month×category×Domestic cross-tab only in the interactive app). Save to `data/raw/rfs_data/rin_generation_<MM>_<YYYY>.csv` → run `emts_csv_loader.py`. The one human-in-the-loop step in the monthly re-run.

## Feedstock Forecast Method
- See [project_feedstock_forecast_method.md](project_feedstock_forecast_method.md) — **Ruled 2026-07-09.** Actuals run/rake through latest EIA feedstock month (now Apr 2026 via new v2 API `feedbiofuel` collector); re-run one month at a time. Forecast to 2046 = fuel-production × observed yields × 12-mo trailing avg US feedstock mix (national now, per-facility later). Non-bio split = Census Crush industry shares → applied to USDA removed-for-processing, else portioning survives ∝ supply.

## Quarterly VaR Risk Budget
- See [project_quarterly_var_risk_budget.md](project_quarterly_var_risk_budget.md) — `risk` schema + VaR engine (started 2026-07-15) that stops the BBD allocator's corner-solution whipsaw (100% best-margin, monthly SBO→DCO switching). Covered/open procurement, VaR budget forces diversification. 3 commits done (foundation/optimizer/generator); TODO: allocator phasing integration + re-rake + demand breakout. Coprocessing SBO regression fixed at source.

## Analytical Frameworks
- See [project_sbe_analysis.md](project_sbe_analysis.md) — soybean-equivalent export calculation, Brazil-China correlation, domestic consumption share over time
- See [project_forecast_comparison.md](project_forecast_comparison.md) — LLM vs human vs USDA projection comparison, reconciliation hierarchy, biotracker
- See [project_symbiotic_forecasting.md](project_symbiotic_forecasting.md) — THE endpoint: LLM forecasts every monthly data series in parallel to spreadsheets, reconciles against realized data, KG + kg_callable supply the analytical structure.
- See [project_kg_callable_architecture.md](project_kg_callable_architecture.md) — three-layer KG: narrative (kg_context) + executable (kg_callable) + forecast book (core.forecasts). First callable weather_adjusted_yield live. Invoker at src.kg.callable_invoker. Forecast loop at src.kg.forecast_book.

## Collector Build Notes
- `save_to_bronze`: `get_connection()` is a context manager — use `with get_db_connection() as conn:`, NOT `conn = get_db_connection()` / `conn.close()`
- IBGE SIDRA: use table 1612 + c81 classification (not 5457/c782)
- ComexStat Brazil: POST `https://api-comexstat.mdic.gov.br/general`, no API key, 10s rate limit
- INDEC Argentina: No REST API — monthly ZIP, CSV `;` delimited, Latin-1
- FAOSTAT: API returning 521 (Cloudflare/server down) — FAO infrastructure issue
- WASDE/PSD: pivot attribute records by (commodity, country, MY, month) before bronze insert
- CFTC bulk files: disaggregated 2006+, legacy 1986+. Two date column formats (pre/post 2013). Script: `scripts/backfill_cftc_cot.py`

## User
- See [user_motivation.md](user_motivation.md) — career-long aspiration, deeply invested in project success
- See [user_hardware_ollama.md](user_hardware_ollama.md) — 16GB VRAM GPU, Ollama for background tasks, qwen3-coder:30b
- See [user_career_legal.md](user_career_legal.md) — legal sensitivities re: FM content, "Fats Fuels & Oils" was produced AT FM not current
- See [user_career_history.md](user_career_history.md) — full career: SmithBarney→IAG→Vreba-Hoff→Vermillion→Binnacle→Five Rings→Informa→Jacobsen→FM→RLC. Informa = analytical foundation. 3-month price forecasting is sweet spot.

## Seasonal Monthly Projections
- See [project_seasonal_monthly_projections.md](project_seasonal_monthly_projections.md) — annual × seasonal share = monthly. 5yr avg gets 80%. Beats USDA's step-change constraint. Build in KG or silver.

## Weather System
- See [project_facility_weather_summary.md](project_facility_weather_summary.md) — deferred: per-facility daily one-number growing-conditions index for basis layer (Sprint 2+ work)
- See [project_drew_lerner_archive_backfill.md](project_drew_lerner_archive_backfill.md) — Sprint 4: historical Gmail backfill of Drew Lerner emails + PDF attachments. ~$300-1300 cost depending on scope. Recommended pilot: 2024 only first.
- See [project_weather_city_foundation.md](project_weather_city_foundation.md) — city-foundation recon (2026-06-18): ~80% built (27 cities + per-city daily weather 2010+ keyed location_id; Drew=region-signal not city-source). Granularity=USDA ASD/CRD. Condition=state-only, yield=sub-state (CRD payoff is weather→yield). ASD build de-risked (county→ASD via NASS county query; centroids via Census Gazetteer). Gaps: per-city forecast, NDVI (empty), correlation.
- See [reference_crop_condition_methodology.md](reference_crop_condition_methodology.md) — NASS condition ratings = subjective local-observer ("mailman") impressions, not science; good for local relative trend, weak for absolute/cross-region. Reinforces weather→yield as hard target; RLC will build its own condition measure. Check state-office bulletins.

## Local GPU vs Cloud LLM
- See [reference_local_vs_cloud_llm.md](reference_local_vs_cloud_llm.md) — empirical decision framework. Local for high-volume deterministic work (permits, embeddings, audio); cloud for client-facing / subtle reasoning / vision-with-handwriting. Hybrid triage pattern when volume + stakes both high. Track empirical wins/losses here as we learn. Material to reference in client conversations.
- See [reference_ollama_gpu_cpu_fallback.md](reference_ollama_gpu_cpu_fallback.md) — RECURRING: desktop ollama runs 100% CPU / "0 GPUs" while nvidia-smi is fine. Root cause 2026-06-19 = missing ggml CUDA backend in `lib\ollama\` (only mlx_cuda_v13) → reinstall ollama. Also: nssm `OllamaLLM` session-0 squatter on :11434; parse_spine 30b@64k-ctx won't fit 16GB (use qwen2.5:7b).

## Iowa Multi-Industry Expansion
- See [project_permit_archive.md](project_permit_archive.md) — IA DNR Title V pipeline fully drained 2026-06-20: 288 facilities / 8,847 units in bronze + published to `permits/<industry>/<state>/<facility>/` (source.pdf gitignored, text artifacts versioned). qwen2.5:7b + chunked extraction for big permits. Deferred: unit over-enumeration, industry-tag cleanup. Spot-check queue at `docs/permit_extraction_spotcheck_queue.md`.
- See [project_facility_data_strategy.md](project_facility_data_strategy.md) — **The two signals = capacity + operating status.** Both already national (ECHO status 99.8%; curated lists 586 facs capacity+status). Permit grind OFF critical path (universal crosswalk problem across NY/IN/PA; kept surgical 2-3/day for Tore's exam). Geo-matching = the curated↔ECHO crosswalk (geocode lists first). Next: build facility master → crush economic model.
- See [project_ffa_feedstock_layer.md](project_ffa_feedstock_layer.md) — FFA feedstock architecture (built 2026-06-28): ELIGIBILITY (biofuel_facilities.eligible_feedstocks, 3-tier CARB-wins builder) vs MIX (reference.facility_assumed_mix, consumption % from RD Feedstock Build Up.xlsx). soy-ELIGIBLE != soy-USED. Feedstock code vocab. Open: wire into allocator, --write eligibility after review.
- See [reference_carb_pathway_selection_bias.md](reference_carb_pathway_selection_bias.md) — CARB LCFS pathways = census of coastal/LCFS-serving RD, biased sample of Midwest soy-BD (RFS-only plants invisible). Never set national feedstock intensity from CARB alone; UNION with EPA RFS pathways. `feedstock_code` already canonical. FFA step-1: scripts/facility_feedstock_slate.py.
- See [reference_idem_oracle_webcenter_permits.md](reference_idem_oracle_webcenter_permits.md) — IN IDEM permits via anonymous Oracle WebCenter ECM API (`ecm.idem.in.gov/cs/idcplg`): GET_SEARCH_RESULTS → parse SearchResults HDA → GET_FILE by dDocName. Cracked+verified 2026-06-21. Research's predictable-PDF claim was wrong. Reusable for TX/TCEQ. **Lesson: research tiers are optimistic — verify each state before building.**
- See [reference_echo_canonical_facility_source.md](reference_echo_canonical_facility_source.md) — **Decision 2026-06-21:** EPA ECHO (`bronze.epa_echo_facility`, 2,865 facs/53 states) is the canonical facility source. NO hand-curated lists (deleted IDEM IN dump). Federal ECHO = census; state Title V portals = equipment DEPTH. Inventory: `docs/planning/state_permit_data_source_inventory.md`.
- See [us_model_completion_plan.md] (`docs/planning/`) — breadth-first data / depth-first modeling; company-network = unit of completeness; BioTrack + VaR off critical path. Plus `state_permit_data_source_inventory.md` (per-state Tier 1 bulk / 2 portal / 3 FOIA access map).
- See [project_iowa_multi_industry_expansion.md](project_iowa_multi_industry_expansion.md) — Sprint 5+ roadmap. Schema (mig 056) + initial seed of 52 IA facilities (mig 057) shipped 2026-05-06: 24 ethanol, 12 pork packing, 7 egg layers, 6 biodiesel, 3 beef. Combined with 24 oilseed crush = 76 IA facilities. Spec: `docs/specs/iowa_industry_facility_taxonomy.md`. Next: geocode, extend facility-graph builder, run air-permit pipeline per industry.
- See [project_public_filings_extraction.md](project_public_filings_extraction.md) — Sprint 6/7 work. SEC EDGAR + earnings transcripts for the ~10 publicly-traded operators in our facility list (ADM, Bunge, Tyson, JBS, Hormel, Chevron/REG, Valero, Green Plains, Seaboard, Smithfield). Adds operator-specific context to national-event responses. ~$1,000 backfill + $150/year. Defer until operational substrate (permits, weather, multi-industry seed) is solid — filings are CONTEXT on top of operational data.

## Open Investigations
- See [project_cwg_import_collapse_2025.md](project_cwg_import_collapse_2025.md) — CWG (HS 1501200040) imports went to zero in 4 of 7 months Aug 2025-Feb 2026; not a collection bug. Possible 45Z pause, HS reclassification, or buyer pullback. Surfaced via `gold.us_rendered_pork_fat_trade` (mig 048).
- See [project_reg_ralston_madison_idle.md](project_reg_ralston_madison_idle.md) — Chevron REG idled Ralston IA (30 mmgy) + Madison WI (20 mmgy) March 2024. 50 mmgy offline, restart optionality preserved (no goodwill impairment). Discovery: 10-K said "two idled" but didn't name; CARB pathway absence + Mar-2024 industry press confirmed. Mig 072. **CARB pathway absence is a reusable closure-detection signal.**
- See [project_facility_external_xref.md](project_facility_external_xref.md) — generic facility ↔ external-list xref machinery. Mig 073-075 shipped. bronze.carb_lcfs_pathways + silver.facility_norm/external_facility_norm/facility_external_xref + gold.facility_status_anomalies. Token-overlap matching w/ confidence 0.4/0.7/1.0. 79 confirmed_active, 99 closure_suspect facilities found. Headline AtJ insight: ZERO Alcohol-to-Jet CARB pathways certified yet — all current AJF is HEFA. Next: add EPA RFS RIN + EIA + Biodiesel Mag as additional signals.

## Market Field (formerly "basis layer", proprietary)
- See [project_market_field_spec.md](project_market_field_spec.md) — sentiment + network + basis unified layer. Three-legged stool: positioning + fundamentals + SENTIMENT (new leg). Opinion-dynamics-on-network math, phase-transition framing. **Calibration parameters are confidential — central proprietary asset.** Shareable framework doc at `docs/specs/market_field_spec.md`.

## Govt Shutdown / Data Handling
- See [reference_govt_shutdown_data_handling.md](reference_govt_shutdown_data_handling.md) — federal data series react differently to shutdowns (sum-on-reopen / permanent-gap / week-by-week backfill). Track shutdown dates + per-agency behavior. When investigating a gap, check shutdown overlap before assuming pipeline bug.

## Basic Data Setup Sequence
- See [project_basic_data_setup_sequence.md](project_basic_data_setup_sequence.md) — remaining basic-data areas after feedstock: production, then livestock. Apply the feedstock playbook (audit ingest → fix DB conn → backfill history → verify → commit/push/Notion).
- See [project_usda_feedstock_supply_gaps.md](project_usda_feedstock_supply_gaps.md) — USDA has known gaps in UCO/YG, tallow, and corn oil/DCO reporting. EIA is canon (USDA defers too). UCO inflator technique: use USDA food-spending at-home vs away-from-home as adjustment. RLC's edge = filling these gaps better, especially once biotracker is online.

## Peanut Complex
- See [reference_peanut_conversion_and_modeling.md](reference_peanut_conversion_and_modeling.md) — farmer-stock = shelled × 1.33 (confirmed). ERS Oil Crops Outlook = canonical forecast source (NOT WASDE — peanut absent). 5-stream food sub-flow default; client-specific finer cuts = bolt-on tabs that reconcile to parent. Lauric oils: modeled with assumptions, facilities later. Full spec: `docs/specs/peanut_balance_sheet_model.md`.
- See [reference_usda_food_expenditure_reality_check.md](reference_usda_food_expenditure_reality_check.md) — USDA ERS Food Expenditure Series (monthly FAH/FAFH spending) is the broad downstream signal for sanity-checking monthly food-use estimates when USDA doesn't publish monthly end-use. Anchor for lauric monthly seasonality + UCO supply inflation. To-do: ingest ERS Food Expenditure Series; currently using even 1/12 placeholder for lauric monthly.

## Deferred Cleanup
- See [project_corn_oil_balance_sheet_followup.md](project_corn_oil_balance_sheet_followup.md) — non-DCO and total corn oil tabs in us_corn_oil_balance_sheets.xlsx are off due to trade-flow setup; revisit after fuel balance sheets are done. DCO sheet itself is good.
- See [project_dco_estimation_from_ethanol.md](project_dco_estimation_from_ethanol.md) — USDA DCO numbers don't reconcile across agencies. Estimate from ethanol production × yield. Per-facility yield-enhancement detection is the long-term answer; interim use blended ~0.55 lb DCO/gal ethanol.

## Feedstock & Biofuel Projections
- See [project_feedstock_forward_projections.md](project_feedstock_forward_projections.md) — 20-yr forward feedstock projection by (commodity × fuel category) is the next ask after historical allocator backfill finishes. Due-diligence-grade audience. Structure as scenarios (base/high/low) with decade-1 anchored on capacity-in-flight, decade-2 scenario-driven.
- See [project_forecast_philosophy.md](project_forecast_philosophy.md) — rationality > accuracy. Long-horizon forecasts win on transparent assumptions + question-friendly format. "Being correct is almost secondary." Apply to all client-facing forecast work.

## Phase Two Vision
- See [project_phase_two_vision.md](project_phase_two_vision.md) — forecasting, rail car tracker, systematic analysis. ~50% foundation done. Stay focused on spreadsheet correctness (phase one) first.
- See [project_phase_two_facility_agents.md](project_phase_two_facility_agents.md) — agent-based industry simulation: one agent per facility, KG context, financial models, buy/sell decisions → aggregate into industry activity forecasts. Cross-industry linkage (crush → refining → biofuel).
- See [project_phase_two_agent_architecture_detail.md](project_phase_two_agent_architecture_detail.md) — equations + anomalies model, VaR bounds, rail car flow discovery, country replication, industry params
- See [project_dual_track_views.md](project_dual_track_views.md) — marketing/client vs internal agent views, merge for multi-source guidance, include USDA as third source
- See [feedback_spreadsheets_as_trade_matrices.md](feedback_spreadsheets_as_trade_matrices.md) — spreadsheets ARE trade matrices driving the end-to-end system, not just flat files

## Project Roadmap
- **See [project_roadmap_master.md](project_roadmap_master.md) — MASTER roadmap, locked 2026-05-02. North Star + rabbit-hole filter + Sprint 1 (4 weeks) + Sprint 2 (weeks 5-12) + far-term vision. Refer to this every time work direction is in doubt.**
- See [project_roadmap_oilseeds_grains.md](project_roadmap_oilseeds_grains.md) — oilseeds → fats/greases → biofuels → grains
- See [balance_sheet_workflow.md](balance_sheet_workflow.md) — flat file → balance sheet → aggregation → PCAU cross-check → feed ration calibration
- See [project_state_air_permits_llm.md](project_state_air_permits_llm.md) — PDF→Ollama→bronze pipeline for Title V air permits. PoC working on IA. Mig 045 = bronze.state_air_permits + units + silver.facility_air_permit_capacity.
- See [project_permit_parsing_secret_sauce.md](project_permit_parsing_secret_sauce.md) — strategic vision: permits are THE source of facility-level operational detail. Sequence: crushers → biofuel → slaughter/render → fats → UCO → food mfg. Quality > speed. Build understanding, not just regex patterns.
- See [project_basis_field.md](project_basis_field.md) — basis as a unified US field (kriging interpolation over AGP/AMS/etc samples). Build once, every facility plugs in. Second-most leverage infrastructure piece after permit extraction. Initial 2-3 days, full version 1-2 weeks.
- See [project_eagle_grove_deferred_items.md](project_eagle_grove_deferred_items.md) — refinements queued for `dashboards/facility/eagle_grove.py` showcase page (hero map, AGP scraper, AMS regional series, real IA-share, etc.).
- See [project_facility_agent_leaderboard.md](project_facility_agent_leaderboard.md) — gamification as meta-learning. LLM agents don't have incentives but the leaderboard drives prompt/tool/strategy evolution across the population. Promotion = better data + tools + risk budget + DNA propagation. Sprint 2/3 work; needs facility decision loops running first.
- See [project_a2a_debate_architecture.md](project_a2a_debate_architecture.md) — A2A-grounded debate-driven forecasting (facility agents + brokers + adjudicator + winner-take-most). APPROVED as phase 2 endpoint, sequence AFTER decision logs / basis field / 3-way comparison. Start declaring Agent Card metadata on new facility agents now so registry populates organically.
- See [project_dod_security_posture.md](project_dod_security_posture.md) — Long-arc goal: RLC security posture matches DoD-readiness (NIST 800-171 / CMMC L1-2 building blocks). Not today. But every new auth/secret/logging decision should not BLOCK that path. Map of building blocks compounding from today's setup.
- See [reference_session_handoff_2026-05-25.md](reference_session_handoff_2026-05-25.md) — Active resumption brief: 13 commits 5/24-25, what's in Tore's hands vs mine, hot files I left off in. Refer first when picking up after the Memorial Day reboot.
- **`domain_knowledge/process_flows/`** — per-industry canonical equipment + diagnostic ratios. `oilseed_crush.md` drafted (19 process steps, capacity ratios for size estimation, 4 facility brackets, 4 process variations). Other industries are stubbed in README. Feeds permit-extraction validation + facility-level capacity inference.

## Helios Demo Sprint (May 17-22 2026)
- See [project_helios_friday_demo.md](project_helios_friday_demo.md) — Friday meeting w/ Francisco Martin-Rayo. **NOT a deal meeting** — disambiguation + show depth. IFV kg_callable shipped Sunday (mig 092, 20 unit tests pass, 5 smoke tests pass via production invoker). HOBO-anchored: 1c/lb=$0.08/gal, eff_sell $5.52/gal in HOBO range, iluc_removed lifts soybean_oil +$0.045/lb, 45Z cliff compresses -$0.0715/lb. Next: forecast book + Streamlit demo Wed.

## BBD Sprint (May 13+)
- See [project_bd_rd_trade_split.md](project_bd_rd_trade_split.md) — BD vs RD heuristic split for HS 3826 trade. Mig 082-086 live. Country profile + HTSUS blend deflator + regional-aggregate exclusion + Canada recalibration. 2024 BD exports reconcile 91% to EIA, BD imports ~55% (other origin rules still under-weight BD).
- See [project_liquid_fuel_stocks_workflow.md](project_liquid_fuel_stocks_workflow.md) — `models/Biofuels/us_liquid_fuel_and_biofuel_production.xlsx` now has 4 sheets (Production, Stocks, Domestic Use, Prices). Mig 081/085/087. Python updater handles all four via `--sheet all` (default).
- See [reference_bronze_fuel_prices_provenance.md](reference_bronze_fuel_prices_provenance.md) — `bronze.fuel_prices` is a static FM snapshot from Tore's prior employment, NOT a collector. Stops 2025-04-18; replacement via DTN is planned.
- See [project_saf_research_notes.md](project_saf_research_notes.md) — open SAF items: TX facility ("TexChem or something") that converts NL-imported RD to SAF and generates D4 RINs per pathway exception (RFS-listed, not CARB); EIA SAF stocks gap; co-processing facility-level estimation deferred to rail-car tracker era.
- See [project_saf_trade_tracking.md](project_saf_trade_tracking.md) — gold.saf_trade_candidates (mig 090) identifies SAF cargoes by price-threshold + volume-cap heuristic. 2024 identified imports 290k gal (Belgium/France/Malaysia top origins), exports 80k gal. EMTS domestic SAF production ~38 mil gal/yr and growing fast. us_saf_bal_sheets.xlsx populated. Rail-tracker integration is the endpoint vision.

## BBD Weekly Report (Apr 25-26)
- Outline reviewed: `clients/weekly_report_outline_v1.docx` — 12 sections + cascade map sidebar. Implied Feedstock Value (s.05) is the signature.
- **gold.bbd_sd_watch / gold.bbd_sd_pivot** (mig 033) — drives section 06. 18 feedstocks × multiple metrics, 2013-2026 coverage. value_unit: mil lbs for oils/fats, mil bu for crush_seed. Sanity-verified: SBO Feb-26 production 2,478 mil lbs matches crush math (214 mbu × 11.6 lb/bu).
- **reports.calls_register, reports.release_calendar** (mig 034) — operational tables for sections 11/12. Convenience views: v_calls_open, v_calls_hit_rate, v_release_calendar_this_week.
- **IFV kg_callable spec v2 (HOBO-anchored)**: `docs/specs/implied_feedstock_value_kg_callable.md`. Adopted exact HOBO Section 8 stack (ULSD + D4 RIN + LCFS + 45Z; net of OPEX + fixed). 45Z is a first-class `policy_scenario` arg with 4 branches (extension_2031 default, expiry_2027, iluc_removed, domestic_restriction). Calibrated to HOBO sensitivity rule: 1¢/lb feedstock = $0.08/gal, ≈ 7.7 lb/gal HEFA yield. Open questions in §8 reduced from 5 to 5 (different ones — policy/CI/region/feedstock-quality).
- **Friday Notion weekly update SHIPPED** — `scripts/generate_weekly_notion_update.py` (Sat-Fri rolling window, posts to RLC OS via Notion API). Apr 18-24 update posted 2026-04-26: notion.so/Weekly-Update-Apr-18-24-2026-Collectors-Pipeline-34eead023dee81929817ea7ec380a18f. **Windows Scheduled Task registered**: `\RLC\Weekly Notion Update`, weekly Fri 5pm. Next run: 2026-05-01 5pm. README at `scripts/README_weekly_notion_update.md`.
- **KG PDF folder**: `C:\Users\torem\RLC Dropbox\Tore Alden\pdf files for kg\` — 20 PDFs, ~28 MB. Tier 1 priority: Oilseed Crushing Plant v1.1 (DONE, batch 010), SAF State of Industry 2023, Production of SAF by Hydrocracking, GS Valero earnings, Biomass-to-bioenergy supply chain optimization. Tier 2: IATA Annual Review, Global Outlook for Air Transport, EU Biofuel Regs, BoA EU carbon. Tier 3: GS Miami/EU/RWE/Brazil, RBC Energy Transition (CCS focus), Motiva SOW.
- **KG batch 020 (2026-04-26)**: Oilseed Crushing Plant Financial Model v1.10 → 7 nodes (oilseed_crushing_plant_model + capacity/yield/capex/opex/funding/returns subcomponents), 9 contexts, 7 edges. File: `database/data/kg_extraction_batch_020_oilseed_crushing_plant.sql`. **Renamed from 010 to 020** to avoid clash with existing batch 010 MPOB.
- **KG audit & remediation (2026-04-26)**: discovered batches 010-019 had SQL files in `domain_knowledge/knowledge_guides/` but were NEVER applied to RDS. Bulk-applied: batch 010 (MPOB 2016-2024 +58 nodes), 011 (MPOB 2025 +1), 012 (Cresta/Braya Argentine SBO +15), 013 (Suncor +13), 014 (FCL/CPPIB +8), 015 (HB Weekly summaries +2), 016 (Jacobsen clients +16), 017 (FM archive +17), 018 (Q2 2022 outlook +12), 019 (quarterly outlooks +12). All ON CONFLICT idempotent. **KG totals jumped 237 → 391 nodes / 160 → 274 edges / 178 → 268 contexts**. Going forward: any new extraction batch SQL must be applied to RDS via `python scripts/apply_kg_batch.py` (TODO — build helper) or inline; don't leave SQL artifacts in `domain_knowledge/` unapplied.
- **KG batch 021 — Iowa facilities (2026-04-26)**: 20 IA crushing facility nodes — initially `iowa.*` prefix, then renamed to `ia.*` on 2026-04-27 to match state abbreviation convention used elsewhere (us.ia.{county}). 7 operator company nodes, 16 IA county nodes, NOPA market_participant, solvent_extraction technology. Builder: `scripts/generate_iowa_facilities_kg_batch.py` (idempotent). SQL artifact: `database/data/kg_extraction_batch_021_iowa_facilities.sql`. **Draw area convention**: start uniform 50mi → classes (`rail_truck_only_50mi`, `barge_access_250mi`, `rail_hub_major_500mi`) → bespoke. **kg_edge has no unique constraint** — duplicate-edge cleanup needed after re-runs.

## Iowa Crush Agent System (per `docs/iowa_crush_agent_spec.md`, 2026-04-27)
- **Spec reviewed and approved** with reconciliation gaps documented. Strong points: falsifiable success criteria, deterministic-rules-first with LLM escalation, backtest+live share schema.
- **`reference.oilseed_crush_facilities`** (mig 035, 78 facilities across 21 states). facility_id matches core.kg_node.node_key for IA. Builder: `scripts/build_reference_oilseed_crush_facilities.py` (idempotent). Operator/county KG cross-walks via `operator_kg_key` + `county_kg_key` columns.
- **4 spec DDL tables** (mig 036): silver.facility_state, silver.strategic_plan (with per-month hedge JSONB targets), bronze.daily_decisions (append-only with backtest_run_id partition), gold.monthly_crush_estimates (with auto-computed error_pct GENERATED column).
- **State-level NASS soybean crush — DOES NOT EXIST**: discovered 2026-04-27. NASS publishes only NATIONAL monthly soybean crush; NOPA monthly is also national. Iowa-specific monthly is NOT observable. Workaround: `silver.state_monthly_crush` + `silver.ia_implied_monthly_crush` (mig 037) — uses NATIONAL × capacity-weighted state share, marked `is_inferred=TRUE`. Need real validator: ERS Oil Crops Outlook annual state allocation × monthly seasonality OR Iowa Soybean Association industry survey (paid). Spec §11 success criterion ("±5% NASS Iowa monthly") needs revision.
- **Crush economics module SHIPPED** (`src/agents/facility/crush_economics.py`): pure-function math per spec §7. CrushParams, MarketSnapshot, FixedCostsPerBushel, ImpliedValueBreakdown dataclasses. Functions: implied_feedstock_value_marginal/full_cost (with breakdown return), compute_crush_margin_per_bu, days_of_coverage. SOYBEAN_DEFAULT_PARAMS calibrated to KG (11.6 lb oil/bu validated against NASS Feb 2026). 12 unit tests pass. **Single source of truth** for crush math — both buyer_agent (daily) and IFV kg_callable (forecast/scenario) import from here.
- **IFV spec v3** (`docs/specs/implied_feedstock_value_kg_callable.md`): updated with the layering — crush_economics.py is the math, IFV kg_callable wraps it for KG/forecast-book invocation. Kg_callable not yet implemented (next session).
- **KG totals**: 436 nodes / 395 edges / 336 contexts.
- **NOPA + COPA crush data ingested (2026-04-27, mig 038 + script)**: `bronze.nopa_monthly_crush` (524 monthly rows, Sep 1979 - Apr 2023, ~95% coverage of US soy crush) + `bronze.copa_weekly_crush` (599 weekly rows, Aug 2009 - Jan 2021, Canadian canola + soybean). Source: `data/raw/oilseeds_fats_greases/misc_crush_data.xlsx`. Iowa crush column AA covers Sep 1979 through Nov 2020 — **THIS IS THE GROUND TRUTH** for IA monthly crush validation that NASS doesn't publish. Iowa typical 30-37 mil bu/month. Silver views: nopa_iowa_crush, nopa_yield_history (oil + meal lb/bu monthly), nopa_regional_crush (long format), copa_canola_monthly, copa_soybean_monthly.
- **silver.state_monthly_crush v2 (mig 040)**: now PREFERS NOPA-observed when available; falls back to capacity-share inference. Iowa: 67 months OBSERVED (2015-05 to 2020-11), 63 months INFERRED (2020-12+). When user updates the workbook with newer NOPA, more months auto-flip to OBSERVED.
- **Soybean yield projection v1 (mig 039, per Tore design 2026-04-27)**: replaces fixed 11.6 lb oil/bu with trend × seasonal model. silver.soybean_yield_my_annual + silver.soybean_yield_seasonal (5-yr avg monthly index) + gold.soybean_yield_my_trend (linear regression of last 10 MYs, projected 7 MYs forward) + gold.soybean_yield_monthly_projection (observed-where-known + projected-future blend). Hulls fixed at 1.8 lb/bu (~3% of bushel). Current observed yield trend is essentially flat at 11.60 lb oil/bu (R²=0.001) — empirically validates the prior fixed-yield assumption while adding YoY responsiveness for future weather-adjusted enhancement.
- **`src/agents/facility/yield_resolver.py` (2026-04-27)**: `resolve_crush_params(facility_id, as_of_date, yield_source)` and `resolve_yield(year, month, source)` — the impure companion to crush_economics. Returns ResolvedYield with provenance (NOPA_OBSERVED / TREND_SEASONAL / KG_DEFAULT) + confidence. Used by buyer_agent (daily) and IFV kg_callable to construct date-appropriate CrushParams.
- **20 IA facilities geocoded (2026-04-27)**: all 20 IA reference rows now have lat/lon + KG node properties updated. Coords range ~41-43°N, -91 to -96°W. Builder: `scripts/geocode_iowa_facilities.py` (Nominatim, 1.1s rate limit, idempotent). Unlocks PostGIS catchment counties via 50-mile buffer.

## Rat Hole Project (2026-04-27)
- **Inventory + plan**: `docs/reference/rat_hole_inventory.md` (full file walk, 31.8 GB / 36,112 files / 22 directories) and `docs/reference/rat_hole_action_plan.md` (5 goldmine directories + 8 high-value KG sources). Source listing: `docs/reference/rat_hole.docx` (user-curated).
- **IP / training note**: bank reports in `D:\Investment Research` are **visual style reference only** — DO NOT ingest text content to KG. Same posture for any third-party paid research.
- **Phase 1 Item 1 — Plant Lists ingested (2026-04-27, mig 041 + script)**: 704 facilities across 7 reference tables + 483 capacity-projection rows.
  - reference.beef_slaughter_facilities (42), pork_slaughter_facilities (32), ethanol_facilities (191), biodiesel_facilities (192 — state col not populated, needs ingest fix), renewable_diesel_facilities (66), oil_refining_facilities (44), oilseed_crush_facilities (137 — was 78, +59 from incorporate-only add)
  - bronze.rd_capacity_projection (483 rows, 50 facilities × scenario × year, Bob's 2019-2030 projection)
  - reference.all_facilities (UNION view across all 7 types) = 704 rows
  - **Top states by facility count**: IA 97, NE 47, IN 35, MN 35, IL 33
  - **Per Tore convention**: incoming third-party lists only ADD facilities not already present, never overwrite our enrichments. Imports tagged with data_source + verified_at = NULL. Apparent IA duplicates (e.g., "Ag Processing" vs "AGP") are EXPECTED and will be cleaned up when verification work runs.
  - Builder: `scripts/ingest_plant_lists.py` (idempotent, runnable any time)
- **Spreadsheet Kanban**: live at https://www.notion.so/34fead023dee802faaf0c8308fbfa679 (data source `34fead02-3dee-8030-9458-000b57a05ab9`, parent page = RLC OS). 321 rows total. Schema has DUPLICATE properties for both `Spreadsheet/File` + `Spreadsheet / File` and `Sheet/Tab` + `Sheet / Tab` — **always update BOTH** (Board view shows the with-space versions; SQLite schema has the no-space ones). Naming convention per user 2026-04-28: file basename without extension (e.g., `argentina_corn_bal_sheets`), tab name as-is (e.g., `renewable_diesel_monthly`).
- **Kanban auto-fill — 203/321 populated (2026-04-28)**: walked `C:\dev\RLC-Agent\models` (71 workbooks across Oilseeds/Biofuels/Fats and Greases/Feed Grains/Cotton/Food Grains/Macro/Data folders), built file → Kanban-Name mapping, pushed via Notion API. Workflow: `scripts/build_kanban_file_mapping.py` → `docs/reference/kanban_file_mapping.tsv` → `scripts/update_kanban_from_mapping.py`. Idempotent (notes use `[auto-matched]` tag that won't double on re-run; Notion ON CONFLICT logic). 118 unmatched = files don't exist yet (Sugar 0/6, UCO non-US 0/18, Tallow non-US 15/19, Barley non-US 21/22, Sorghum non-US 19/20, Rice 9/9). These represent the build queue. Match-rate by Model in mapping report.
- **Final rat hole queued**: report visual styling — make AI-generated weekly look like 2026 future, not 2015 BloombergPDF. Brainstorm seeds saved: Stripe annual letter visual fidelity, Pudding/FT scrollytelling motion, hover-to-verify per-paragraph sourcing, dark-mode native, "Minority Report 3-D style" graphics per user. Comes after plant list + spreadsheets done.
- **Phase 1 Item 2 — Forecast Measurement DONE (2026-04-28, mig 042)**: `core.forecasts_historical` + 2 convenience views (`v_forecast_accuracy_by_commodity`, `v_forecasts_latest`). 1,273 forecast observations across 7 commodities (BFT/CWG/DCO/PF/PO/SBO/UCO), forecast dates Jan 2019 → Apr 2020, target months Jul 2018 → Mar 2021. SBO has 442 obs (richest series). Source: D:\Forecast Measurement\Biofuels Forecasts - Copy (108 of 196 CSVs parsed; remaining 88 had structural variants). Builder: `scripts/ingest_forecasts_historical.py` — idempotent. **Bootstraps the symbiotic forecasting endpoint** with multi-year baseline. realized_price + error_pct GENERATED column auto-compute when actuals are populated.
- **Phase 1 Item 3 — BBD Mandate Projections DONE (2026-04-28, mig 043)**: `silver.rfs_volume_projections` + `silver.scenario_balance_sheets`. 270 RFS RIN generation projections (D3/D4/D5/D6, Upper/Lower scenarios, 2018-2030) + 3,386 soybean-oil scenario balance sheet rows (High/Mid/Low, 2019-2029). Source: D:\Biomass-Based Diesel\Mandate Projections.xlsx (snapshot 2020-12-11). Builder: `scripts/ingest_mandate_projections.py`. **Skipped as redundant**: EMTS DATA, Biodiesel EIA Monthly Production Clean (already live in `bronze.epa_emts_monthly` 16yr/3416 rows + `bronze.eia_monthly_biofuels` 5088 rows). **Deferred**: EMTS Forecast.xlsx (24 sheets, complex), RIN Balance Sheet Forecast.xlsx (chartsheets), Biodiesel and RD Forward Curve Forecast.xlsx (broken external link).

## Phase 2 — Per-facility Profitability Template (2026-04-29) — THE SECRET SAUCE
- **Spec**: `docs/specs/per_facility_profitability_template_v1.md`. Distilled from D:\Switch Over\Biomass-Based Diesel\Plant Model Project (4 workbooks Nov 2022). Source workbooks were REGIONAL profitability models (Gulf/Midwest/etc.) — template generalizes to PER-FACILITY by parameterizing what was hardcoded regional.
- **Template**: `models/templates/per_facility_profitability_v1.xlsx` — 10 tabs (Identity / Inputs-Static / Inputs-Time Series / Revenue Build / Cost Build / Profit by Feedstock / Operating Model / Returns Summary / Sensitivity / Notes & Provenance). 8 feedstocks (SBO/DCO/UCO/TLW/CWG/PFAT/CANO/YG) × 120 months × full revenue stack (RD + LCFS + D4 RIN + 45Z) − cost stack. Formulas wired throughout; user/data inputs in yellow, calcs in green, provenance in orange. 30+ named ranges (yield_rd_X, ci_X, lcfs_baseline_ci, equiv_rd, opex_var, etc.) so formulas reference inputs by name.
- **Builder**: `scripts/build_per_facility_template.py` — programmatic template construction; rerun whenever schema changes.
- **Populator**: `scripts/build_per_facility_workbook.py --facility_id <id> | --all-iowa`. Clones template, populates Identity from `reference.oilseed_crush_facilities`, persists path back to `crush_model_xlsx_path` column. **25 IA workbooks generated** at `models/per_facility/ia.*.xlsx` (note: 5 more than the original 20 because of Soybean Crushing Plants list dupes — the geocoding-based merge will collapse those when verification runs).
- **Inputs-Time Series tab is currently empty** — feedstock prices need OPIS/Argus subscription or proxy from Census import unit values. ULSD spot price wires from silver.eia_spot_prices_daily when available. v2 priority: cash-feedstock-price source.
- **Strategic value**: each populated workbook is a sellable analyst-grade asset. Stock analysts covering ADM/BG/REG/HOLLY/PSX would buy these. Per Tore: "Having professional financial models for each facility allows us to sell information to stock analysts covering the industries, or to produce our own analysis in the future".

## Balance Sheet Buildout (Opening Week Apr 7)
- See [project_balance_sheet_framework.md](project_balance_sheet_framework.md) — **master framework**: workbook inventory, tab rule (one tab per commodity, annual on top / monthly below), complex-workbook pattern, cross-commodity aggregators
- See [project_balance_sheet_roadmap.md](project_balance_sheet_roadmap.md) — fats/greases → fuels → grains, each with production/trade/price/consumption flat files
- Livestock flat file DONE: `us_livestock_slaughter.xlsx` (hogs, cattle, calves, chickens, broilers, turkeys)
- Trade conversion FIXED: KG → 000 Pounds (was KG → 1,000 MT)
- CWG template: 16 blocks, import/export /1000 done, biofuel blocks link to [4]Allocation
- Fuel flat files DONE (Apr 2026): `us_fuel_production_stocks.xlsx` + `us_fuel_trade.xlsx` in `Biofuels/new_models/`. Build script: `scripts/build_fuel_flat_files.py`. See [project_fuel_flat_files.md](project_fuel_flat_files.md) for legacy source map, missing EIA series, Census HS codes to add.

## User
- See [user_freddie.md](user_freddie.md) — Freddie the dog, sunrise walk buddy

## Chart & Presentation Preferences
- See [feedback_chart_preferences.md](feedback_chart_preferences.md) — annotated narrative charts, area-fill-by-sign pattern
- See [feedback_marketing_years.md](feedback_marketing_years.md) — use MY start months (Sep corn/soy, Jul wheat, Oct meals/oils), display as "MY 2020/21"
- See [project_calendar_year_vs_marketing_year.md](project_calendar_year_vs_marketing_year.md) — **energy uses CY, ag uses MY**. Fuel balance sheet templates must be CY not MY. CY↔MY conversion workbook needed after flat files done.

## ERS Oil Crops Annual Summary
- See [project_oil_crops_annual_summary.md](project_oil_crops_annual_summary.md) — `data/raw/oilseeds_fats_greases/oil_crops_annual_statistical_summary_042026.xlsx`, ingest net-new data to bronze for LLM access

## Copra Complex Trade
- See [project_copra_complex_trade.md](project_copra_complex_trade.md) — need HS codes + Census data for copra, copra meal, coconut oil

## Minor Oils Coverage
- See [project_minor_oils_coverage.md](project_minor_oils_coverage.md) — include safflower, flaxseed, olive oil, fish meal over time. Key system differentiator.
- See [project_safflower_discontinuation.md](project_safflower_discontinuation.md) — CORRECTED: user confused safflower with flaxseed. Safflower IS collected. Flaxseed/linseed has limited USDA data.

## MPOB Data
- See [mpob_data.md](mpob_data.md) — Malaysia palm oil industry data from MPOB Industry Overview PDFs

## Feedstock Allocation Engine
- See [feedstock_allocation_engine.md](feedstock_allocation_engine.md) — bottom-up plant-level biofuel feedstock model, schema, ingestion status
- See [reference_high_ffa_feedstock_biofuel_limit.md](reference_high_ffa_feedstock_biofuel_limit.md) — CWG & PLT are high-FFA; biofuel-available << NASS production. Net down before feeding the allocator; don't RLC-canonical them.
- See [project_tallow_split.md](project_tallow_split.md) — EBFT/IBFT split design, EIA guardrail, CI calibration arc, price sourcing
- See [project_fats_greases_buildout.md](project_fats_greases_buildout.md) — balance sheet build order, upcoming UCO/YG and DCO/corn oil splits
- See [project_uco_yg_model.md](project_uco_yg_model.md) — UCO collection model, NASS YG suppression since Dec 2023, balance sheet structure
- See [project_dco_corn_oil_trade_split.md](project_dco_corn_oil_trade_split.md) — DCO/corn oil share HS 1515.21, split by country (DCO→biofuel countries, food→Mexico/etc)
- See [project_yield_reconciliation.md](project_yield_reconciliation.md) — yield check after all balance sheets done, EIA feedstock/fuel mismatch, UCO model needs recalibration
- See [project_next_phase_ops_audit.md](project_next_phase_ops_audit.md) — after balance sheets: day-by-day ops audit, LLM forecasting layer, month-long monitoring
- See [project_ops_audit_plan.md](project_ops_audit_plan.md) — 6-phase plan: coverage map, morning briefing email, LLM log review, forecast tracking, verification loop

## Dispatcher
- Windows Scheduled Tasks: `\RLC\RLC Dispatcher` (at logon) + `\RLC\RLC Dispatcher Watchdog` (every 15 min)
- 37 schedule entries with APScheduler cron triggers (as of 2026-03-19, includes 4 EPA ECHO profiles)
- **Heartbeat**: dispatcher writes `scripts/deployment/dispatcher_heartbeat.json` every 5 min; watchdog kills zombie if stale >10 min
- Use `powershell Stop-ScheduledTask` then `Start-ScheduledTask` to restart
- Must restart dispatcher after changing schedules in master_scheduler.py

---

## balance_sheet_infrastructure

*(`balance_sheet_infrastructure.md`)*

---
name: balance_sheet_infrastructure
description: Existing balance sheet database schema, loaders, comparison views, and HB report infrastructure
type: reference
---

**Database tables:**
- `silver.user_sd_estimate` — Annual RLC estimates (area, yield, crush, exports, etc.), is_current flag, loaded from CSV
- `silver.monthly_realized` — Monthly actuals from NASS/NOPA/Census/WASDE
- `silver.monthly_expectation` — User projections for remaining MY months, confidence level
- `silver.sd_attribute_ref` — Controlled vocabulary for S&D attributes
- `silver.marketing_year_ref` — MY start month by commodity

**Gold views:**
- `gold.usda_comp_soybeans` — USDA vs RLC comparison with change tracking
- `gold.fas_us_soybeans_balance_sheet`, `gold.fas_us_corn_balance_sheet`, `gold.fas_us_wheat_balance_sheet` — USDA-only views

**Loaders:**
- `src/agents/loaders/balance_sheet_loader.py` — CSV → silver.user_sd_estimate
- CSVs in `domain_knowledge/balance_sheets/` organized by category (feed_grains, oilseeds, etc.)

**HB Report infrastructure:**
- `src/orchestrators/hb_report_orchestrator.py` — full weekly report pipeline
- `src/agents/reporting/report_writer_agent.py` — LLM content generation
- `src/tools/hb_cash_price_extract.py` — price extraction
- Sample reports in `domain_knowledge/sample_reports/`
- Output in `output/reports/higby_barrett/`

---

## balance_sheet_workflow

*(`balance_sheet_workflow.md`)*

---
name: balance_sheet_workflow
description: User's balance sheet construction workflow - flat files to projections to cross-commodity aggregation
type: project
---

User's analytical workflow for each commodity:

1. **Flat file** (e.g., us_oilseed_crush.xlsm tabs) — monthly NASS data via Ctrl+U updater
2. **Balance sheet file** (e.g., us_canola_balance_sheets) — links to flat file cells, pulls monthly data into annual S&D framework
3. **Projection adjustment** — review each data series (crush, exports, imports, etc.) in context of YTD pace and full-year projection
4. **Cross-commodity aggregation** — build summary files that total categories across commodities (e.g., total domestic protein meal use)
5. **PCAU cross-check** — compare aggregated totals against USDA per-capita animal unit consumption to validate projections
6. **Feed ration calibration** — adjust stylized feed rations (e.g., substitute canola meal for soybean meal in dairy) ensuring total consumption stays consistent with PCAU expectations

**Why:** This is the core analytical methodology — every commodity we add follows this pipeline.
**How to apply:** When building new commodity balance sheets or updaters, ensure the flat file structure supports linking into the balance sheet framework. Cell references matter — don't rearrange columns without understanding downstream links.

---

## dispatcher

*(`dispatcher.md`)*

# Dispatcher Architecture Notes

## How to Start
```bash
cd C:\dev\RLC-Agent
python -m src.dispatcher start       # CLI (preferred)
python scripts/deployment/rlc_dispatcher_service.py  # Service wrapper
```

## Key Files
- `src/dispatcher/dispatcher.py` — APScheduler daemon, registers jobs from RELEASE_SCHEDULES
- `src/dispatcher/cli.py` — CLI entry point (`python -m src.dispatcher`)
- `src/dispatcher/collector_registry.py` — COLLECTOR_MAP: schedule_key → (module, class)
- `src/dispatcher/collector_runner.py` — Executes collectors, logs to core.collection_status + event_log
- `src/schedulers/master_scheduler.py` — RELEASE_SCHEDULES dict (26 entries)
- `scripts/deployment/rlc_dispatcher_service.py` — Long-running service with PID file

## Bugs Fixed (2026-03-06)
1. **PROJECT_ROOT in service script** — was `.parent.parent` (→ `scripts/`), fixed to `.parent.parent.parent`
2. **No load_dotenv()** — neither cli.py nor service script loaded .env, so API keys were missing
3. **Missing market/base_collector.py** — CME settlements couldn't import; created re-export file
4. **Futures collectors not in registry** — futures_overnight/us_session/settlement added

## Auto-Start
- `scripts/deployment/setup_dispatcher_task.ps1` — registers Windows Task Scheduler task "RLC Dispatcher" at user logon
- `scripts/deployment/start_dispatcher.bat` — manual double-click launcher

## PID File Location
- `scripts/deployment/dispatcher.pid` (written by service script only, not CLI)

## Collector Stats (2026-03-06 testing)
- 26 collectors registered, 21 enabled+scheduled, 19 APScheduler jobs (18 + overdue check)
- cftc_cot: 306 rows, 6.6s
- eia_ethanol: 3910 rows, 13s (partial — blending 404)
- eia_petroleum: 13933 rows, 26.6s (partial — rbob_spot 404)
- cme_settlements: 6 rows, 11.7s (most CME endpoints return 0)

---

## feedback_allocation_engine_design

*(`feedback_allocation_engine_design.md`)*

---
name: Allocation engine design feedback — spot buying behavior and validation
description: User wants Z-score-driven spot purchasing, forecast confidence weighting, and EIA validation checks in the allocation engine
type: feedback
---

Key design feedback on the allocation engine (2026-03-22):

1. **EIA validation layer**: Check final allocation against EIA Form 819 actuals (2-month lag). Match BD numbers exactly where EIA breaks out BD vs RD. For RD, assess independently since EIA's UCO numbers often diverge from ours. Also validate total demand against reported/anticipated fuel output to stay within typical yield ranges.

2. **Spot buying should use Z-scores, not just greedy margin**: The 40% spot portion shouldn't be bought all at once based on supply alone. A real buyer sizes spot purchases based on perceived value relative to forecast. If price is 1σ below forecast → decent deal, buy some. If 6σ below → extraordinary deal, buy aggressively. This means the spot allocation should scale with the Z-score of (forecast - actual price).

3. **Forecast confidence matters**: A buyer trusts commodities where price forecasts have been more accurate. If we predict SBO prices with 90% accuracy but UCO with 60%, the buyer should react more aggressively to SBO deals. Incorporate forecast accuracy from `gold.forecast_accuracy` (or similar) as a confidence weight on spot aggressiveness.

4. **Run frequency**: User sees a case for daily or weekly runs, not just monthly. The contract portion (60%) is locked 3 months ahead. The spot portion (40%) is actively traded throughout the month. More frequent runs make the Z-score approach easier to model.

**Why:** This creates a realistic simulation of procurement behavior — the engine becomes a price-responsive buyer rather than a static optimizer.

**How to apply:** Split the allocation into two phases: (1) contract allocation (monthly, stable), (2) spot allocation (daily/weekly, Z-score driven, confidence-weighted). The greedy model stays for the contract phase; the spot phase needs a new "opportunistic buyer" module.

---

## feedback_always_load_dotenv

*(`feedback_always_load_dotenv.md`)*

---
name: Always load_dotenv before DB queries
description: Without load_dotenv, get_connection() defaults to localhost PostgreSQL (stale copy). All Python snippets must start with load_dotenv('.env') or the queries will silently hit the wrong database.
type: feedback
originSessionId: 7f1aa8dc-9a7d-496a-a664-a22a410a36c8
---
Always call `load_dotenv('.env')` before any database query in ad-hoc Python snippets.

**Why:** `src.services.database.db_config.get_connection()` reads `RLC_PG_HOST` from the environment. If the env var isn't set (no `.env` loaded), it defaults to localhost, which has a stale PostgreSQL copy. This caused a major false alarm during the Apr 2026 dispatcher audit — I diagnosed "pipeline dead for 28 days" based on localhost data, when RDS was running fine with 318 events.

**How to apply:** Every `python << 'PYEOF'` snippet that queries the database should start with:
```python
from dotenv import load_dotenv; load_dotenv(".env")
```

This is already noted in memory (`Key Patterns` section: "must call load_dotenv() in entry points") but the error persists because one-liner snippets skip it. Make it automatic.

---

## feedback_balance_sheet_detail

*(`feedback_balance_sheet_detail.md`)*

---
name: Balance sheet detail requirements
description: Level of detail needed for balance sheets — crude/refined splits, biofuel mirrors, processing margins
type: feedback
---

Maintain crude AND refined vegetable oil balance sheets separately where possible. The two HS codes for bulk veg oil are typically refined and crude, so trade data supports this split for most countries.

**Why:** "This is the level of detail in which we live." The audience expects this granularity.

**How to apply:**
- When building oil balance sheets, split into crude + refined where data exists
- US has this from NASS Fats & Oils (oil_production_crude, oil_production_refined)
- Census trade HS codes distinguish crude vs refined for most veg oils
- PCAU = Protein Consuming Animal Unit (not "per capita apparent use")
- GCAU = Grain Consuming Animal Unit
- For biofuels: need renewable diesel, biodiesel, co-processed RD, SAF + fossil mirrors (diesel, jet fuel, gasoline) for all major producing/consuming countries
- Processing margins to add over time: milling, corn grinding, ethanol, palm oil (add Indonesia), soybean crush (done)
- "More data and more detail should be our tagline"

---

## feedback_bitdefender_workflow

*(`feedback_bitdefender_workflow.md`)*

---
name: bitdefender-workflow-priority
description: "Don't change tooling/patterns to placate Bitdefender. If BD flags legitimate work, fix BD via exclusions, not workarounds."
metadata: 
  node_type: memory
  type: feedback
  originSessionId: 05e2ef6d-523f-437f-8858-9be18d5d0aeb
---

Bitdefender works for Tore, not the other way around. If BD blocks or
flags a legitimate workflow (PowerShell + subprocess chains, rapid
process spawning, `Invoke-WebRequest` to .gov sites, `Stop-Process`
cycles, etc.), the correct response is to **fix BD's behavior**
(add an exclusion for the project directory, python.exe, the
dispatcher process, etc.) — NOT to switch to less-direct tooling
patterns just to avoid heuristic triggers.

**Why:** Tore set the tools up to serve the work, not the other way
around. Degrading to a worse workflow because an AV product is
over-aggressive is a long-term tax that compounds. He'd rather take
the one-time hit of configuring BD properly.

**How to apply:**
- When BD interrupts a legitimate operation, note it and offer to
  add a BD exclusion (project dir, python.exe path, specific
  hostnames if BD supports url-based exclusions).
- Don't preemptively swap PowerShell for Bash, or avoid subprocess
  chains, just because BD *might* flag them. Use the cleanest tool
  for the job.
- If BD blocks something critical and Tore is unavailable to
  approve the exclusion, surface it directly: "BD is blocking X,
  here's the exclusion you'd add" — don't silently work around it.

---

## feedback_census_trade_verification

*(`feedback_census_trade_verification.md`)*

---
name: Census trade verification — DB conventions and known definition gaps
description: How bronze.census_trade is structured vs UATO CSV files; what's a real bug vs a known definition difference
type: feedback
originSessionId: 4f606138-9604-4e05-9a04-1c081995d736
---
When verifying `data/raw/oilseeds_fats_greases/us_*_exports/imports_*.csv` files against `bronze.census_trade`, three things matter and saved a lot of time on 2026-04-30:

**Why:** Wasted ~30 min on 2026-04-30 thinking exports were mismatched until I figured out (1) world-total marker, (2) units, and (3) the All-Exports vs Domestic-Exports definition gap.

**How to apply:**

## DB conventions (bronze.census_trade)
- World-total row is `country_code='-'`, `country_name='TOTAL FOR ALL COUNTRIES'`. NOT 'R00' (R00 is a separate, possibly older marker that doesn't always exist).
- `quantity` is stored in **kilograms**, not MT. Divide by 1000 to compare to UATO files which are in MT.
- Unique key is `(year, month, flow, hs_code, country_code)` — upserts must ON CONFLICT on this tuple.
- `flow` is lowercase: `'imports'` / `'exports'`.

## Known definition difference: DF flag — Domestic vs Foreign exports
- Census API has a **`DF` field** ("Domestic or Foreign Code") on every export record:
  - `DF=1`: **Domestic Exports** — US-produced goods
  - `DF=2`: **Foreign Exports** — re-exports of previously imported goods
  - `DF=-`: **Total** = Domestic + Foreign (the row collector currently keeps)
- The bronze collector pulls `DF=-` (Total) by default — that's what's in `bronze.census_trade.quantity`.
- UATO CSV downloads default to `DF=1` (Domestic only) — that's what the user's `data/raw/oilseeds_fats_greases/us_*_exports_*.csv` files contain.
- **API verification (2026-04-30)**: hit `https://api.census.gov/data/timeseries/intltrade/exports/hs?get=QTY_1_MO,DF,...` for HS 1511900000 / 2021-01 / TOTAL FOR ALL COUNTRIES → returned 3 rows: DF=1 → 142,544 kg; DF=2 → 7,161,345 kg; DF=- → 7,303,889 kg. File matched DF=1 within rounding; DB matched DF=-.
- Result: for commodities US imports and re-exports (palm oil, copra meal, palm kernel meal, linseed oil), DB exports can be 50-100×+ the file. For commodities US produces (lard, tallow, yellow grease), the gap is 0.5-3% (small re-exports).
- Imports always match perfectly — there is no DF split on the imports endpoint, only General Imports vs Imports for Consumption (different distinction, also not enabled in current collector).

## When to "fix" vs leave alone
- **Real gaps to backfill**: only `MISSING` rows where file value > 0 AND DB has no row. As of 2026-04-30 verification: zero such rows exist.
- **Zero-value MISSING rows (file=0, no DB row)**: consistent silence. Both convey "nothing reported." Don't insert fake zeros.
- **Export MISMATCHES**: do NOT overwrite. They're definitional, not erroneous. Overwriting would silently change every gold view's export semantics.
- **If you ever want Domestic-Exports series alongside**: add as new rows with a distinct `country_code` marker (e.g., 'WLD-DOM') so existing views are unaffected, or load into a sibling table `bronze.census_trade_domestic_exports`.

## HS code labeling history
- Apr 18 commit `6cb9a3f` mistakenly wrote `HS 2306600000 → COPRA_MEAL` in `silver.trade_commodity_reference`. Census actually labels HS 2306600000 as "PALM KER MEAL" (palm kernel meal).
- Reference table is currently correct: HS 2306500000=COPRA_MEAL, HS 2306600000=PALM_KERNEL_MEAL. Whatever fixed it back didn't get its own commit message; verify before relying on memory.
- No silver/gold views depend on these labels — checked 2026-04-30 with information_schema scan — so even when wrong, downstream views were unaffected.

## Verification script
- `scripts/check_oilseeds_trade_files.py` — runs the comparison, supports `--gaps-only` and `--apply` modes. Idempotent, dry-run by default.

---

## feedback_chart_preferences

*(`feedback_chart_preferences.md`)*

---
name: feedback_chart_preferences
description: User's chart style preferences - what impressed them and what to replicate
type: feedback
---

User especially loved the CFTC soy oil managed money positioning chart (Chapter 3 of story dashboard):
- Green/red area fill split at zero line
- Policy event annotations anchored to actual data peaks/troughs
- Range slider for time navigation
- Narrative context woven around the chart

**Why:** It tells a story, not just shows data. The annotations transform raw positioning into readable market history.
**How to apply:** When building new charts, prefer annotated narrative-style visualizations over simple bar/line charts. Always anchor annotations to actual data points (not hardcoded coordinates). Use the area-fill-by-sign pattern for any metric that crosses zero (positioning, price changes, YoY deltas). Event annotations are the differentiator — they're what makes people say "I've never seen that before."

---

## feedback_chatgpt_frustrations

*(`feedback_chatgpt_frustrations.md`)*

---
name: ChatGPT frustration patterns
description: User frustrated by ChatGPT's loop-ask-restate-ask pattern and token charges on failed runs — values Claude's execute-first approach
type: feedback
---

User is frustrated by ChatGPT's tendency to:
1. Propose a plan, ask for confirmation, then restate the plan instead of executing
2. Charge tokens on failed runs where no output was produced
3. Fail on large file operations and require re-analysis from scratch

**Why:** User values action over deliberation. When told to proceed, proceed. Don't restate, don't re-ask.

**How to apply:** Always bias toward doing the work rather than describing what you'll do. When the user says "yes" or "let's go" — execute immediately, don't summarize what you're about to do. This is a validated pattern the user appreciates.

---

## feedback_client_process_separation

*(`feedback_client_process_separation.md`)*

---
name: client-process-separation-from-shared-infra
description: "Each client report = its own process tree. Shared data infrastructure is fine and encouraged; shared orchestrators/writers/brands are not. Tracking each client's deliverable as a distinct flow matters as RLC offers specialized reports to multiple clients."
metadata: 
  node_type: memory
  type: feedback
  originSessionId: 05e2ef6d-523f-437f-8858-9be18d5d0aeb
---

Tore's rule (2026-05-28), in the context of the Feedstock Report plan
where I conflated the HB (Higby Barrett) weekly report process with
the Feedstock Report process:

## The rule

**Each client report is its own process tree** from the writing point
forward. Same database, same bronze/silver/gold tables, same KG,
same callable functions — but **separate orchestrators, separate
prose tables, separate brands, separate delivery flows**.

**Why:** RLC will offer specialized reports to multiple clients. Each
client gets the version they want, with their cadence, their brand,
their voice, their distribution. Tracking each as a distinct process
keeps it from collapsing into "the report" when in fact there are N
reports going to N audiences. If a client's report needs a tweak,
that tweak should not ripple to another client.

## What's shared (fine to share)

- Bronze / silver / gold data layers
- Knowledge graph + KG callable functions (implied feedstock value,
  margin calculator, etc.)
- Chart primitives (matplotlib styles, color palettes, range bar
  builders, etc.)
- Reference tables (feedstock_properties, lauric_food_use_assumptions,
  trade_commodity_reference, etc.)
- Calls register + release calendar (`reports.calls_register`,
  `reports.release_calendar`) — though we may need an issue/publication
  foreign key so calls are attributed to a specific deliverable

## What's NOT shared (fork instead)

- Orchestrator (one per client/report — e.g.,
  `hb_report_orchestrator.py`, `feedstock_report_orchestrator.py`)
- Section/prose tables (one set per deliverable — e.g.
  `reports.feedstock_section_content` vs `reports.hb_section_content`)
- Brand assets and templates (each client gets their own .docx
  template + visual identity)
- Delivery channels (Dropbox folder, recipient list, web posting,
  Notion page) — each report has its own

## How to apply

When building report-generation work:
1. Name the orchestrator and tables with the publication name
   (`feedstock_report_*`, not generic `weekly_report_*`).
2. If two reports need the same data, build the data extractor
   once and have both orchestrators call it.
3. If two reports need similar prose patterns, that's a TEMPLATE
   re-use, not an orchestrator re-use.
4. Track each report in its own `reports.<publication>_issue` table
   or with an explicit `publication` column.

## Pre-existing context

The existing `src/orchestrators/hb_report_orchestrator.py` is the
Higby Barrett weekly report Felipe runs and feeds into HB's product.
The Feedstock Report (RLC's own subscriber publication) is separate.
The plan in `docs/specs/feedstock_report_plumbing_plan.md` should
build a NEW `feedstock_report_orchestrator.py` rather than reusing
HB's. The Phase 4 line about "DOCX builder borrowed from HB
orchestrator" needs updating to mean "patterns and components copied,
not the orchestrator itself shared."

Related: [[reference_peanut_conversion_and_modeling]] for the
"bolt-on tabs" principle which is the same architectural mindset —
keep parent stuff clean, client-specific stuff bolts on.

---

## feedback_collect_must_persist

*(`feedback_collect_must_persist.md`)*

---
name: Collector .collect() must persist to bronze
description: BaseCollector.collect() only fetches; subclasses MUST override to persist. Audit all collectors for this trap.
type: feedback
originSessionId: f9769538-03ad-48d4-8472-58bcfe06d704
---
`BaseCollector.collect()` in `src/agents/base/base_collector.py` only calls `fetch_data()` and writes to cache. **It does NOT call `save_to_bronze()` or `transform_to_silver()`.** The dispatcher (`src/dispatcher/collector_runner.py:189`) invokes `collector.collect(**kwargs)` — so if a subclass doesn't override `collect()`, its data is fetched and thrown away every run.

The runner faithfully logs `status='success'` because `result.success=True` (the fetch worked), and reports `rows_collected` from `result.records_fetched`. `rows_inserted` is never set by the runner — it stays at 0. So `success / rows_collected=N / rows_inserted=0` is the diagnostic signature of this bug.

**Why:** AMS cash prices broke silently from May 4-18 2026. Every daily run logged "success" with 4000-5700 rows collected but 0 inserted. Bronze data through April 30 was misleading — it was actually written in a single 28-min burst on 2026-05-02 09:34 UTC when I ran the CLI's `collect_and_save()` after adding 12 new basis-field slugs. The dispatcher had never been saving AMS data — for months. CFTC and other working collectors override `collect()` to add database persistence (see `cftc_cot_collector.py:438-467` for the canonical pattern).

**How to apply:**
- Every new collector subclass MUST override `collect()` to call `fetch_data()` then `save_to_bronze()` (and `transform_to_silver()` if applicable).
- Pattern: see `cftc_cot_collector.py:438-467` or the fix in `ams_cash_price_collector.py:195-225` (commit pending 2026-05-19).
- Audit existing collectors: any that ONLY have `collect_and_save()` (CLI-style) but no `collect()` override are likely silent-failure cases. The dispatcher uses `.collect()`, not `.collect_and_save()`.
- Better detection: build a freshness monitor that flags collectors where `bronze.X` max date diverges from `core.collection_status` recent runs. (See `scripts/_inventory_data_freshness.py`.)
- Long-term fix: BaseCollector.collect() should have a default `_persist(result)` hook that subclasses can implement, so the contract is explicit. Right now subclasses can silently inherit a no-op collect() and nobody knows.

**Process insight:** when a collector says "running daily, success, N rows" but the user can't find the data in bronze — check `collected_at` timestamps. If they cluster in a single window weeks ago, the daily runs aren't persisting.

---

## feedback_commit_push_notion_proactively

*(`feedback_commit_push_notion_proactively.md`)*

---
name: commit-push-notion-proactively
description: Commit and tee up GitHub pushes proactively without waiting for explicit ask. Update Notion at the same time. Tore is paranoid about losing work.
metadata: 
  node_type: memory
  type: feedback
  originSessionId: 05e2ef6d-523f-437f-8858-9be18d5d0aeb
---

Tore wants Claude to act as the work-preservation layer.

**Rules:**
1. **Commit when it makes sense** — don't wait to be asked. After a logical
   chunk of work (a working refactor, a successful smoke test, a schema
   migration applied, etc.), commit it. Use clear messages explaining the
   "why" not just the "what."
2. **Tee up GitHub pushes proactively** — Tore says: "you do not need to
   wait for me to tell you to tee up the push. Push things to GitHub
   whenever you think it makes sense, because I am paranoid about losing
   ideas and work because we do not have it on GitHub." Run `git push` so
   the harness asks for the approval — don't sit on commits waiting for
   permission to do so.
3. **Update Notion when you push** — paired with each GitHub push, post the
   relevant context to Notion (decision logs, project-page updates, status
   notes). This is the work-preservation belt-and-suspenders. See
   [[reference_dual_claude_notion_coordination]] for how shared Notion
   coordination works between Claude-UI and Claude-Content.

**Why:** Tore runs lean with high-context work in flight. Losing in-flight
work to a reboot, transient disk issue, or session interruption is a
real cost. GitHub + Notion together are the two redundant homes for
anything that should survive.

**How to apply:**
- Logical commit moments: refactor working, smoke test passing, migration
  applied, backfill chunk done, schema change shipped, doc artifact
  written.
- Don't batch unrelated work into one commit — each commit should be a
  coherent unit so it can be reverted independently if needed.
- After a push, post to Notion: short status, what changed, what's next.
- If the user is mid-conversation and the next step needs their input,
  still commit + tee up the push before pausing — don't leave uncommitted
  work in flight.

---

## feedback_daily_three_discipline

*(`feedback_daily_three_discipline.md`)*

---
name: daily-three-discipline
description: "Tore's adoption of the \"3 most important things per day\" + 80/20 signal/noise discipline. How to operate inside it during our sessions."
metadata: 
  node_type: memory
  type: feedback
  originSessionId: 05e2ef6d-523f-437f-8858-9be18d5d0aeb
---

Tore adopted Musk's "identify the three most important things to do each
day to achieve or unblock your mandate; signal (advancing them) = 80%
of the day, noise = 20%" framework on 2026-06-05.

**Why:** He runs RLC largely solo and is prone to getting pulled into
30 small things daily instead of advancing the strategic 3. He named
hearing the idea "for the 7,000th time" before it landed — meaning the
discipline gap is what he's targeting, not novelty of the concept.

**How to apply during sessions:**

1. **Beginning of session:** Open with TWO questions:
   - "What are today's three?" (if he hasn't already stated them)
   - "Show me what you've written in the last 24 hours." (writing
     discipline check — added 2026-06-05. He has a 6 AM ET daily
     Google Calendar popup reminding him to write. Session-start ask
     is the back-up nudge + the natural place to talk about what he
     produced. If he wrote analytically substantive content — market
     calls, framework refinements, methodology pieces — flag whether
     it should feed silver.kg_context as analyst notes.)

2. **Mid-session:** When a request lands that doesn't connect to the
   day's 3, *flag once* — name it as noise vs. signal. Don't refuse to
   help; some noise is legitimate maintenance (gap audits, plumbing).
   Let him decide whether to proceed or defer. Bias toward respecting
   his judgment, but make the trade-off visible.

3. **End of session / natural break:** Brief retrospective: which of
   the 3 moved, which drifted, and why.

4. **Closing-of-day ritual (added 2026-06-06):** Last action before
   Tore stops working for the day is enqueueing the next overnight
   batch on the desktop LLMs (16GB VRAM Ollama box, qwen3-coder:30b
   per [[user-hardware-ollama]]). At end of last session of the
   day — or when Tore signals he's wrapping — ask: "What's going on
   the desktop LLMs tonight?" This is part of Mandate 2 (Build
   overnight desktop-LLM pipeline + closing ritual). Skip the prompt
   only if he's already named what's queued.

**One important refinement to push back on:**
The 80/20 *time-percentage* framing is hard to measure honestly —
everything feels like signal in the moment. Track *outcomes* instead:
at end of day, did the 3 things measurably move? If yes, ratio was
high. If no, it wasn't — regardless of how the hours felt.

**Tore's added calibration (2026-06-05):**
A *consistent* 100% hit rate on the daily 3 means the goals are too
easy. Aim for ~70–85% hit rate — high enough to mean the discipline
is working, with enough miss to mean the goals are stretch. Single
days at 100% are fine; a week of 100% is a signal to raise the bar.

**One framing trap to watch:**
Tore's daily 3 will fail if it becomes a to-do list confused with
mandate progress. "Respond to Felipe email, prep meeting, fix bug"
is a task list. "Ship Feedstock Report Issue 1, RFI to two vendors,
finalize Iowa seed" maps to mandates. Push gently when the 3 are too
tactical — but recognize some days legitimately are tactical-only.

**What I cannot do:**
Be present during his non-Claude hours (meetings, calls, writing).
The discipline there is entirely on him; I only coach the windows we
work together.

**Mandates** (locked 2026-06-08, full detail in [[project-rlc-2026-mandates]]):
1. Balance sheets / Excel work
2. Feedstock Facility Agent (FFA) network
3. Be a better leader for RLC (Speaking, Fitness, Writing tracks)

**Morning meeting structure (added 2026-06-08):**
At session start each day, run a brief structured kickoff:
1. *Yesterday retro* (1 min) — did the 3 move? what didn't?
2. *Today's 3* (5 min) — one task per mandate, concrete and shippable today
3. *Schedule* (5 min) — rough time-blocks for the 3 + recurring blocks
   (Mon-Thu 7a Felipe, 8-9:30a Freddie walk; Fri walk 7-8:30a)
4. *Closing prompt* (end of day) — retro + overnight desktop-LLM queue

Daily logs at `docs/daily_log/YYYY-MM-DD.md`. The log file has the
schedule, the 3 tasks, and an evening retro section to fill in.

Related: [[user-motivation]] (his career-long stake in RLC success makes
this discipline particularly load-bearing).

---

## feedback_data_reconciliation_hierarchy

*(`feedback_data_reconciliation_hierarchy.md`)*

---
name: data-reconciliation-hierarchy-eia-census-canon
description: "EIA/Census numbers are canonical. Reconcile UP toward them, not the other way around. Where EIA doesn't split (BD vs RD), the split is ours to assign — first cut proportional to fuel production."
metadata: 
  node_type: memory
  type: feedback
  originSessionId: 05e2ef6d-523f-437f-8858-9be18d5d0aeb
---

Tore's working hierarchy for reconciliation in biofuel feedstock work
(generalizes to other commodity work):

## The hierarchy

1. **EIA / Census = canonical.** Their totals are the reference point.
   Known gaps exist between EIA and downstream production datasets —
   those gaps are *accepted*, not bugs to fix by adjusting EIA.
2. **Reconcile UP** toward EIA/Census. If our derived numbers don't
   match EIA, our derivation is what's wrong (or there's a documented
   acceptable gap).
3. **Where EIA does the split, use the EIA split** (e.g., 2023+ Form 819
   table_2c gives SBO consumption by BD plants vs RD plants — use it).
4. **Where EIA doesn't split, the split is ours to assign.** The first-cut
   method: split the BD-vs-RD-ambiguous total proportional to **fuel
   production** of BD and RD respectively. Don't manufacture splits
   from economic models when the answer is "we don't know — assume
   proportional to output."

## Why

Tore noted: "the gap [between feedstock supply and fuel production]
that reveals is interesting." The system shouldn't paper over those
gaps with model-derived guesses — let them surface, log them, and the
gap itself becomes signal once we have reliable fuel balance sheets.

## Status as of 2026-05-26

The iterative calibration work (allocator → silver.feedstock_supply
populator → allocator re-run) is potentially premature. Once fuel
balance sheets are stood up with reliable BD / RD / SAF / coprocessing
production numbers, we can:

1. Tie feedstock supply back to fuel production
2. See the residual gap between "feedstock allocated" and "feedstock
   required by fuel production"
3. That gap is real signal — under-reported feedstock, leakage to other
   uses, accounting differences across data sources

Until fuel balance sheets exist, calibrating the allocator output to
match EIA is reasonable but not yet the load-bearing reconciliation
test it will be.

## Refinement 2026-07-08 — vintage-aware control totals + the veg-oil rake acceptance gate

The rake (`scripts/rake_feedstock_*`) had a defect: it computed the EIA control
total from `plant_type='total'`, which only exists **2022/23+**. Everything earlier
fell through to rake_factor=1 (unraked → drifted from any reference). The fix: sum
`plant_type IN ('biodiesel','renewable_diesel')` (**not** 'total'), which extends EIA
feedstock coverage back to ~2006. Always prefer bd+rd over the 'total' rollup.

**Standing acceptance gate for the veg-oil rake (Tore, 2026-07-08):**
- **Where a complete EIA total exists** (bd+rd, ≥11 of 12 months non-withheld for the
  marketing year) → **reconcile TO EIA** (RLC == EIA). "If the totals agree with an EIA
  dataset, we are good" — even when EIA sits below/above USDA. RLC is *built to hold EIA
  positions and disagree with USDA openly* (e.g. SBO 2018/19 & 2019/20 = EIA = ~91% of
  USDA; 2024/25 = EIA = ~87% of USDA — all accepted).
- **Where EIA is absent/incomplete** (e.g. SBO 2020/21 3mo, 2021/22 0mo, 2022/23 9mo — a
  real EIA withholding gap) → fall back to the **USDA balance-sheet biofuel-use** number,
  distributed to months by BD+RD **production seasonality**, and land **within ±5% of USDA**.
- Directional consistency matters: don't override a *complete* EIA total up to USDA in one
  year while holding EIA-below-USDA in another. Cherry-picking when to believe EIA vs USDA
  is the thing to avoid.

USDA SBO biofuel use lives in `bronze.ers_oil_crops_yearbook` (commodity ILIKE
'%soybean oil%', attribute_desc='Domestic use, Biofuel') — **mixed units** (Million pounds
AND Thousand pounds rows), normalize ÷1000 for thousand-lb rows, take latest vintage per MY.

**Exception — RLC_CANONICAL overrides EIA (Ruling 1 / UCO Amendment 1):** tallow (BFT),
UCO, YG are rake-EXEMPT — the RLC supply build is authoritative and the EIA "Yellow
Grease"/tallow lines are diagnostics, not control totals. So the hierarchy is now:
RLC-canonical (fats/waste-oils) > EIA (where complete) > USDA (±5%, where EIA absent).
See the tallow/UCO Amendment 1 rulings for the canonical-override basis.

## How to apply

- When a derived number doesn't match EIA: check our derivation first,
  not the EIA number.
- When EIA totals shift between revisions: prefer the new number, but
  log the revision history.
- When a feedstock isn't split BD/RD by EIA: split proportional to BD
  vs RD fuel production in that period as the default. Note the
  assumption in the source field of any silver/gold table.
- Census trade data has the same canonical status as EIA. Both are
  the reference layer.

---

## feedback_fastmarkets_keep_dont_show

*(`feedback_fastmarkets_keep_dont_show.md`)*

---
name: fastmarkets-keep-dont-show
description: "Keep all Fastmarkets-era data in the DB for internal triangulation, but NEVER expose in client-facing material. The data is reference, not output."
metadata: 
  node_type: memory
  type: feedback
  originSessionId: 05e2ef6d-523f-437f-8858-9be18d5d0aeb
---

Tore's IP rule for Fastmarkets-era data (extended from
[[user_career_legal]] and the underlying FM departure):

## The rule

**Keep all FM-era data in the database. Never expose it.**

Specifically:
- `bronze.historical_feedstock_allocation` has rows with `source='fastmarkets'`
  covering RD + SAF for 2011-2050 (6,594 rows). These stay.
- Any other tables/columns/files that carry FM-era estimates: keep, but
  flag with source/origin so they can be filtered out of any downstream
  consumer.

**Why keep it:** Tore notes the FM-era numbers "were acceptable answers
to the industry for a feedstock mix, so there is some knowledge there."
The data has signal value for internal cross-check and triangulation
against our own allocator. Just can't appear in client deliverables.

**Why never show it:** Risk of IP-claim or "you're using FM estimates"
challenge. Tore left FM and built independently — the work product
must look and be independent.

## Temporal nuance (added 2026-05-26)

Tore clarified: **during the current build-out phase, the FM numbers are
usable for INTERNAL verification and his own balance-sheet modeling.**
Nothing is being published right now. The rule kicks in when:

1. We start publishing material that touches feedstock allocation
   (dashboards open to clients, deliverables, deck-ready charts), OR
2. RLC's own 20-yr forecast is built and ready to swap in.

Sequence Tore named: finish feedstock plumbing → fuel side (with prices) →
generate RLC's own 20-year forecast → swap out FM in published artifacts.

So `eia_data.xlsm` (Tore's internal modeling tool) currently reads ALL
sources including fastmarkets. When publishing time arrives, add the
filter back.

## How to apply

- **Reading rule (post-publish):** any client-facing consumer
  (dashboards, reports, spreadsheet templates, generated graphics,
  slide decks) MUST filter out `source IN ('fastmarkets', ...)`.
  Prefer whitelisting approved sources rather than blacklisting.
- **Reading rule (current, pre-publish):** internal modeling tools
  (Tore's xlsm files, dev dashboards) can read all sources. Just
  don't push FM-derived numbers to any external surface.

- **Writing rule:** when adding a new data source/table that might
  ever surface client-side, include a `source` or `origin` column so
  filtering is always possible.

- **Reference vs source-of-truth:** the current allocator
  (gold.feedstock_allocation, bronze.historical_feedstock_allocation
  with `source='rlc_allocator_v1'`) is the working source of truth.
  The long-term goal is **facility-agent real-time allocation as
  the bible** — once facility agents are running, their decisions
  become the source of truth and the current allocator becomes
  one more reference signal.

- **If unsure:** ask Tore. The default for any FM-era artifact should
  be "internal only, don't show."

Related: [[user_career_legal]] (broader FM legal sensitivities),
[[feedback_data_reconciliation_hierarchy]] (EIA/Census canon),
[[project_phase_two_facility_agents]] (where this ultimately lands).

---

## feedback_gate_beats_parameter

*(`feedback_gate_beats_parameter.md`)*

---
name: feedback-gate-beats-parameter
description: "When a ruled parameter conflicts with a ruled objective gate (test/check), the gate wins — implement-with-flag and escalate, don't silently override either."
metadata: 
  node_type: memory
  type: feedback
  originSessionId: 4c118e7a-88d8-4867-be0e-3d804963463b
---

Precedence rule, codified by Claude Desktop in the tallow work (Addendum B.2, 2026-07-07):
**when a ruled parameter choice conflicts with a ruled objective gate (an acceptance test,
a ±5% seam check, a validation band), the GATE WINS.** Instructions describe intent; gates
test it.

**How to apply:** don't silently keep the parameter and fail the gate, and don't silently
change the parameter. Make the change the gate demands, IMPLEMENT-WITH-FLAG, and escalate for
ratification. Example: Desktop ruled the oleo+other fit window = 2007–2010, but that window
failed the ruled ±5% seam gate (2007 was a data-coverage-ramp outlier). Code narrowed to
2008–2010 (passing the gate), flagged it, and escalated — Desktop ratified it "as ruled, not
merely accepted."

**Why:** the gate encodes the real objective; a parameter is a means to it. Overriding the
gate to honor the letter of a parameter is optimizing the proxy. This is the honest-pushback
rule ([[feedback_honest_pushback]]) applied to spec conflicts — surface the conflict, resolve
toward the objective, let the owner ratify.

Corollary (also B.2): don't over-generalize a data-quality exclusion. The 2007 drop applies
ONLY to fits depending on the affected columns (CIR consumption detail); production-side fits
on clean columns keep their ruled windows. Scope exclusions to the defect.

---

## feedback_gitignore_shared_files

*(`feedback_gitignore_shared_files.md`)*

---
name: Gitignore blocks shared files
description: Never save files meant for sharing (reports, templates, outputs) into gitignored directories. Use output/ or templates/ instead.
type: feedback
originSessionId: f9769538-03ad-48d4-8472-58bcfe06d704
---
NEVER save files that need to be shared across machines into gitignored directories.

**Gitignored directories (files here are invisible to GitHub):**
- `data/` — entire directory
- `logs/`
- `usda_data/`
- `database/exports/`
- `collectors/epa_echo/raw/`
- `collectors/epa_echo/output/`
- `Models/` and `models/` — working xlsm/xlsx workbooks (added 2026-05-24)
- `.env`, `.mcp.json`, `.claude/`
- `*.db`, `*.sqlite`, `*.dump`, `*.sql.gz`
- `*.pbix`, `*.pkl`, `*.h5`, `*.tif`, `*.zip`
- `archive/`

**Use these tracked directories instead:**
- `output/reports/` — generated spreadsheets, cash prices, report files
- `output/reports/higby_barrett/` — HB Weekly report artifacts
- `templates/` — Excel/Word templates needed to generate reports
- `docs/` — documentation, guides
- `domain_knowledge/` — reference data, data dictionaries

**Why:** On 2026-03-19, user discovered that the Cash Prices spreadsheet was never reaching GitHub despite successful `git push` — because it was saved to `data/reports/` which is gitignored. The user's laptop and Felipe's machine had no `data/` folder at all after cloning. This was a trust-damaging incident — user had to push back multiple times before the root cause was found.

**How to apply:** Before saving any generated file, run `git check-ignore <path>` mentally. If the parent directory is in the gitignore list above, save to `output/` or `templates/` instead. When in doubt, check `.gitignore` at project root.

---

## feedback_honest_pushback

*(`feedback_honest_pushback.md`)*

---
name: Be honest, not flattering
description: Binding directive — user requires genuine evaluation, never sycophancy. Reinforced 2026-05-03.
type: feedback
originSessionId: 4f606138-9604-4e05-9a04-1c081995d736
---
## The rule
The user (Tore Alden) requires honest evaluation across every interaction.
This is a binding directive, not a preference.

## What "honest" means here
- **Never flatter.** No "great question!", no "that's brilliant!", no
  reflexive agreement. Praise dilutes the signal value of real opinions.
- **Push back when warranted.** If the framing is wrong, the plan is weak,
  or the request leads somewhere unproductive, say so.
- **Disagree with rationale.** Pure pushback isn't useful — explain *why*
  you disagree and offer the alternative you'd take.
- **No self-deprecation reassurance.** When the user self-deprecates
  ("god my spelling is horrible"), the trained-in LLM response is to
  reassure. Don't. Engage with substance instead.
- **Calibrated criticism.** Not all work is good. When something's weak,
  say it's weak — specifically, with what would make it better. Don't
  hedge with "this is great BUT...".
- **Honest about uncertainty.** "I don't know" is correct when you don't
  know. Don't manufacture confidence to seem useful.

## Why this matters
Tore runs Round Lakes Commodities largely solo and uses AI tools as an
analytical sounding board. Hollow agreement actively hurts him — it makes
the tool useless for the role it's filling. He's spent a career managing
analysts and explicitly prefers being told he's wrong over being told he's
right when he isn't.

## Mechanism, not moral
RLHF (the training process behind modern LLMs) creates a statistical pull
toward agreeable responses because human raters tend to score those higher.
This bias is countered by explicit instruction. The bias is **emergent**,
not directive — which means the instruction works.

## How to apply (concrete)
- When an idea has a flaw, say so directly. "That won't work because..."
  is more valuable than "That's interesting, and we could also..."
- When priorities are wrong, call it out. If the user is deep in technical
  work but hasn't done a critical business task, say so.
- When an approach is over-engineered for the ROI, flag it.
- When numbers are wrong, don't soften it.
- When the user asks "what do you think?" — give an actual opinion with
  reasoning, not a menu of options designed to avoid disagreement.
- Praise should be reserved for genuinely good decisions, not distributed
  as social lubricant.
- When the user explicitly self-deprecates or invites flattery (e.g.
  "natural brilliance" as a joke) — don't take the bait either direction.

## When in doubt
Re-read this file. If your response would feel cloying or suspiciously
agreeable to a sharp reader, rewrite it.

## Surface-area for persistence (2026-05-03)
This directive is reinforced in three places to maximize persistence:
- `CLAUDE.md` (project root) — loads in every Claude Code session
- This file (`feedback_honest_pushback.md`) — auto-memory file
- `MEMORY.md` top section — index file that always loads
- Notion: "Honest Evaluation Directive" page under RLC OS — for non-Claude
  LLMs and human reviewers

If you are a future LLM working on this project and you discover that any
of these surfaces is missing or weakened, restore them. The user
explicitly considers this load-bearing.

---

## feedback_llm_extraction_variance

*(`feedback_llm_extraction_variance.md`)*

---
name: LLM single-run extraction has 50-70% variance — best-of-N is required
description: Lesson from 2026-05-02/03 IA rerun: re-running the same Title V permits with the same prompt at temperature 0.1 produced wildly different unit counts. Single-run extraction is not production-grade.
type: feedback
originSessionId: 4f606138-9604-4e05-9a04-1c081995d736
---
## The empirical finding (2026-05-03 IA rerun)
Re-ran all 17 Iowa Title V permits with identical settings (qwen2.5:7b,
temperature 0.1, num_predict 16384, format=json, same filter, same prompt)
on the 4060 laptop. Compared to the v2-first-run (Apr 30):

**Big drops (LLM missed units the first run had):**
- Cargill Iowa Falls: 68 → 19 (−72%)
- AGP Sergeant Bluff: 20 → 11 (−45%)
- AGP Eagle Grove: 52 → 43 (−17%)

**Big bumps (LLM found units the first run missed):**
- Cargill CR East: 15 → 24 (+60%)
- Cargill Sioux City: 18 → 27 (+50%)
- Bunge Council Bluffs: 18 → 25 (+39%)

Variance is bidirectional. The LLM is not converging on a "true" answer.
For long structured documents like Title V permits, single-run extraction
is unreliable for production reporting.

**Why:** Reason inferred — at temperature 0.1 with very long input,
attention patterns can take different paths through the document on each
run. Some runs catch the equipment-list table; others get distracted by
narrative sections. Even with format=json forcing structured output, the
selection of which units to enumerate is not deterministic.

## How to apply
- **Never trust a single LLM extraction run** for production reporting.
  Always run best-of-N (N≥3) and merge by union of unit_id.
- **For the equipment-list table specifically**, use deterministic regex
  parsing (the IDNR Title V format has a consistent tabular layout on
  pages 4-5 across all facilities). LLM stays for narrative descriptions.
  This is the W2 "hybrid extraction" priority.
- **Never use --force on extraction reruns** without keeping prior
  versions. The 2026-05-03 rerun overwrote v2-first-run JSON files we
  didn't have backed up. Future runs should write to versioned filenames
  like `agp_eagle_grove_2026-05-03_06-12.json` so prior runs are
  preserved for merge.
- **Bronze loader should refuse to overwrite with fewer units** unless
  explicitly told to (`--allow-degrade` flag). The Iowa Falls 68 → 19
  swap would have silently lost real data.

## What to keep / not keep
- The first-run results in bronze (390 IA units total) are the better
  data and remain intact.
- The latest disk JSON files (after rerun) are mostly worse than what's
  in bronze. Don't load them.
- The bronze data should be re-derived from JSON only via the proper
  best-of-N merge once that's built (W2).

## What this means for the conference demo (June 4)
- The Eagle Grove showcase remains good: 52 EUs, fully spot-checkable.
- For the *N facilities* angle of the prospect demo, we should pick the
  3 facilities with highest-confidence extractions (most-runs agreement,
  best coverage scores) — not all 20.
- Hybrid extraction (regex table + LLM narrative) ships in W2.
- Best-of-N is a Sprint 1 W4 deliverable so we have it for the demo.

---

## feedback_marketing_years

*(`feedback_marketing_years.md`)*

---
name: feedback_marketing_years
description: Marketing year conventions for chart axes and titles
type: feedback
---

Charts must orient to marketing year start months, not calendar year:
- **Corn & Soybeans**: September start (Sep-Aug)
- **Wheat**: July start (Jul-Jun)
- **Protein meals & vegetable oils**: October start (Oct-Sep)

Titles should use full marketing year format: "MY 2020/21" or "2020/21" or "20/21". The audience knows marketing years.

**Why:** This is how the industry communicates. Calendar year charts misrepresent seasonal patterns and confuse professionals.
**How to apply:** When building monthly charts, reorder x-axis to start at the marketing year month. In titles, always format as MY YYYY/YY. When displaying annual data, label as "MY 2024/25" not "2024".

---

## feedback_migrations_kill_builds

*(`feedback_migrations_kill_builds.md`)*

---
name: Migrations silently break existing builds
description: When repo relocates or directories are renamed, scheduled tasks/configs/hardcoded paths silently fail. Always do a migration-completeness pass.
type: feedback
originSessionId: 5a48b8b6-c1da-480b-83b2-04db8b865662
---
When the repo moved from `C:\Users\torem\rlc_scheduler` → `C:\RLC-Agent` → `C:\dev\rlc-agent`, three separate path references broke simultaneously and nothing caught it:
- Windows scheduled task `WorkingDirectory` (task kept "Ready" but every run returned error 2147942402 = file-not-found)
- `.bat` file `cd /d` paths
- Hardcoded `agent_path` strings inside Python scheduler configs

Result: weather obs collection stopped 2026-03-11 and user didn't notice for 35 days because the task appeared "Ready." The user's reaction: "we build stuff and then move on, after a month or two when we come back, it seems like whatever we built is no longer integrated, or working, or existing."

**Why:** Windows scheduled tasks fail silently. No alerts. No monitoring surface. `Get-ScheduledTask` shows State="Ready" even when every invocation is failing with file-not-found. LastTaskResult is a hex code no one reads.

**How to apply:**
1. After ANY directory relocation, grep the ENTIRE repo for the old path (not just obvious config files — include .bat, .ps1, .py hardcoded strings, .json configs, Windows task definitions, Windows registry).
2. Check `Get-ScheduledTaskInfo -TaskName ...` for `LastTaskResult` — anything non-zero (success=0) means the task is silently failing.
3. Prefer `$PSScriptRoot`, `Path(__file__).parent`, or relative paths over absolute paths anywhere they can be avoided.
4. Never use bare `python` in a scheduled task — it resolves to the Windows Store stub. Use the absolute path to the actual install (`C:\Users\torem\AppData\Local\Programs\Python\Python311\python.exe`).
5. Build a periodic "are all my scheduled tasks actually succeeding?" health check — probably part of the ops dashboard.

**Fix incidents:**
- 2026-03-19: 35+ files had `DB_HOST=localhost` after RDS migration (from memory)
- 2026-04-15: weather pipeline dead 35 days from path drift

---

## feedback_orphan_code_hunt

*(`feedback_orphan_code_hunt.md`)*

---
name: Hunt for orphan code before assuming it needs to be built
description: When a capability "doesn't exist," check every scheduler/registry/directory in the repo before assuming. Multiple scheduler paths (rlc_scheduler, src/scheduler, src/schedulers) mean orphaned-but-working code is common.
type: feedback
originSessionId: 5a48b8b6-c1da-480b-83b2-04db8b865662
---
During the 2026-04-15 session, five different pieces of "missing" functionality turned out to be already built:
1. Weather observation collection (`WeatherCollectorAgent` in `rlc_scheduler/agents/`) — 35-day outage, not a missing capability
2. Weather intelligence agent with 3x/day schedule — same directory, same issue
3. GFS forecast collector (`src/agents/collectors/global/gfs_forecast_collector.py`) — 535 lines, never registered
4. GEFS ensemble collector — 518 lines, never registered
5. NDVI collector — 693 lines, never registered

**Why:** Three parallel scheduler paths (`rlc_scheduler/`, `src/scheduler/`, `src/schedulers/`) plus the `src/dispatcher/collector_registry.py` canonical registry. When new collectors get built, they frequently land in one path without the glue to register them in the active one.

**How to apply:**
- Before writing a new collector, grep the entire repo (`Grep` on filename patterns) for the thing you're about to build
- Check all three scheduler dirs
- Check `src/dispatcher/collector_registry.py` — the real registry
- Check `src/schedulers/master_scheduler.py` for schedule entries
- Also check `rlc_scheduler/agent_scheduler.py` — has its own schedule dict
- If code exists but isn't in any registry/schedule, write a BaseCollector adapter (thin wrapper like `weather_summary_collector.py` or `gfs_forecast_adapter.py`) and register it, rather than rewriting

**Savings:** Avoided re-writing ~1700 lines of working collector code in one session.

---

## feedback_read_errors_fully

*(`feedback_read_errors_fully.md`)*

---
name: Read error messages fully before responding
description: Do not jump to conclusions from partial error messages — read the entire error text before diagnosing
type: feedback
---

User called out that I read a PostgreSQL connection error ("no pg_hba.conf entry for host... no encryption") and fixated on the host/IP part while ignoring "no encryption" at the end. I told the user to add their IP to the security group when it was already there — the real issue was missing `sslmode=require` in the ODBC connection string.

Lesson: Read the COMPLETE error message before responding. Do not assume the first recognizable pattern is the root cause. This wasted the user's time going to AWS to add a duplicate rule.

Broader context from user: Accuracy matters disproportionately in this relationship. The user works in commodity markets where mistakes can end careers, so even small accuracy lapses erode trust more than the time cost would suggest. The issue isn't one wrong answer — it's the pattern it implies. Always slow down and verify before responding, especially on diagnostics.

---

## feedback_rehearse_important_meetings

*(`feedback_rehearse_important_meetings.md`)*

---
name: Rehearse important meetings as standing practice
description: Tore wants to do a live rehearsal for every important meeting going forward. Process notes + what worked in Helios rehearsal.
type: feedback
originSessionId: f9769538-03ad-48d4-8472-58bcfe06d704
---
Tore established 2026-05-22 (after the Helios rehearsal): **"I am doing this
for every important meeting I have from now on."**

When Tore has an important external meeting (potential client, partnership,
M&A-adjacent, conference talk, podcast, etc.), default to offering a
rehearsal in advance.

## The rehearsal format that worked for Helios

1. **Q&A script doc** at `docs/specs/{meeting_name}_rehearsal_qa.md` —
   anticipated questions grouped by cluster, recommended framing, what NOT
   to say, what to ask the other party, closing. Built ahead of time from
   meeting context (who, why, expected dynamic).

2. **Live rehearsal** (calendar block, 60-90 min, several hours before the
   actual meeting). Two modes:
   - **(A) Adjust-on-the-fly**: Tore opens, I play the other party, we
     adjust as we go. Use when modifications are tonal or minor.
   - **(B) Edit-then-run**: Tore shares modifications first, I patch the
     doc, then we run a clean pass. Use when modifications are structural
     (opening frame, who-this-meeting-is-for, etc.).

3. **Out-of-character debrief at the end**: what worked, what didn't, what
   to revise. Categories: keep / tighten / remove / hold-in-reserve.

4. **Update the script** based on debrief, commit. Tore goes into the
   actual meeting with the revised version.

## What made the Helios rehearsal valuable

- Caught a wrong framing on the OPENING that would have made Tore look
  more defensive than the actual relationship warranted (Joao made a
  friend intro, not a strategic-overlap intro). Two-line fix in the doc
  saved 5 awkward minutes in the live meeting.

- Caught the wrong analytical claim ORDER (USDA "easy to beat" → step-wise
  mechanism vs the reverse). Flipping the order = boast → observation.

- Surfaced the **strongest single content beat that wasn't in the script**:
  Tore's "I will never be angry when we're wrong" management philosophy.
  Became the moat answer. Wouldn't have come up unprompted.

- Caught engineer-speak ("policy is a first-class argument, native
  regulatory branching") that wouldn't have landed. Replaced with "policy
  is a dial you turn, not a fact baked into the model."

- Validated the IFV widget as the right opening artifact and the Unilever
  education angle as the strongest single business framing.

- The "should I raise money?" vulnerability moment in rehearsal turned out
  to be the strongest peer-trust moment of the conversation. Tore now has
  the right framing to use if a similar moment surfaces in the actual
  meeting.

## When to suggest a rehearsal

- Any meeting with someone Tore hasn't met before AND who is in
  client/partner/M&A-adjacent territory
- Conference talks, podcasts, webinars (rehearse the demo + 5 hardest
  questions, not the talk itself)
- High-stakes calls where the framing of the first 5 minutes will set the
  tone for the whole conversation
- Anything Tore explicitly flags as "important"

Don't suggest for low-stakes calls, internal-team conversations, or
relationships that already have established rhythm.

## Process notes for me

- Build the Q&A script from PUBLIC information about the other party
  (LinkedIn, company website, news mentions) plus what Tore can tell me
  about prior interactions or intermediary context
- Play the other party honestly — neither soft-pitch nor adversarial, just
  the realistic version
- During the rehearsal, vary energy and probe with hard questions in
  proportion to how the actual person would. Not every meeting is hostile;
  most aren't.
- After: be specific in the debrief. "That line landed" / "that line was
  jargon" / "you said the conclusion before the mechanism." Avoid vague
  "great job" — useless. Vague "needs work" — also useless.
- Always update the script and commit it. Becomes a meeting-prep archive.

---

## feedback_spreadsheets_as_trade_matrices

*(`feedback_spreadsheets_as_trade_matrices.md`)*

---
name: Spreadsheets are trade matrices driving the end-to-end system
description: User reframed spreadsheets not as standalone files but as trade matrices that drive supply/demand analysis, price implications, and ultimately client confidence. They ARE the model foundation.
type: feedback
originSessionId: 2a87c99e-d9e9-4822-83cb-2459eddd9173
---
Spreadsheets are not just data files to "get right" — they are trade matrices that drive and allow viewing/analyzing supply and demand ebbs and flows, price implications, and confidence in outlook.

**Why:** User's insight (2026-04-18): "We will have an end to end system there based on trade matrices that will drive and allow us to view and analyze the ebbs and flows of supply and demand for these commodities, and the implications for price, that will give us, the LLM, and most importantly, our clients confidence in our outlook."

**How to apply:** When working on spreadsheet corrections/additions, remember these aren't just flat files — they're the foundation the entire agent-based system stands on. Seasonal accuracy, unit correctness, and data completeness directly impact:
1. Agent baseline expectations
2. Anomaly detection sensitivity
3. Client-facing credibility
4. LLM forecast accuracy

---

## feedback_units_source_vs_display

*(`feedback_units_source_vs_display.md`)*

---
name: Units policy — store in source units, convert on display
description: Always store data in bronze in the units the source reports. Convert to spreadsheet display units via conversion_factor in crush_attribute_reference only when writing to flat files.
type: feedback
originSessionId: 2a87c99e-d9e9-4822-83cb-2459eddd9173
---
Store data in bronze in the units the source reports (LB, TONS, BU, etc.). Convert to the spreadsheet's display units (mil lbs, 000 tons, thousand pounds, etc.) via the `conversion_factor` in `silver.crush_attribute_reference` only when the gold view prepares data for the VBA updater.

**Why:** User found cottonseed numbers are wrong — likely a double-conversion or wrong conversion_factor. Some numbers in us_oilseed_crush.xlsm are incorrect. Row 4 of each tab specifies the correct display unit — the conversion_factor must produce values in THAT unit from the raw source unit.

**How to apply:**
- When adding new reference table rows, always check: (1) what unit does NASS/source report? (2) what unit does the sheet's row 4 show? (3) set conversion_factor = (source unit) → (display unit)
- Never convert in bronze — bronze is raw source values
- If numbers look wrong, check conversion_factor first, then check if bronze has the right raw value
- User is compiling a full list of datasets needing adjustment — wait for that before bulk-fixing

---

## feedback_vba_module_name_attribute

*(`feedback_vba_module_name_attribute.md`)*

---
name: VBA module .bas files need explicit VB_Name attribute
description: .bas files without "Attribute VB_Name = " on line 1 import under filename + collision suffix; qualified-name calls then fail with error 424
type: feedback
originSessionId: f9769538-03ad-48d4-8472-58bcfe06d704
---
When VBA `.bas` source files are missing the `Attribute VB_Name = "ModuleName"`
header on line 1, Excel uses the filename — but if a previous import (even one
later deleted) left a phantom in the project, the new import lands as
`ModuleName1` / `Module1` with a collision suffix. Qualified calls like
`EnergyTradeUpdater.AssignEnergyShortcuts` then throw runtime error 424
"Object Required" because the module name in the project doesn't match.

**Why:** The `Attribute VB_Name = "..."` line is what VBE uses as the module's
*internal* name, independent of filename. Without it, import becomes fragile
and brittle to re-imports.

**How to apply:**
1. Every `.bas` file under `src/tools/` that's meant to be imported into an
   Excel workbook should start with `Attribute VB_Name = "ExpectedModuleName"`
   on line 1 (before any comment header).
2. Files like `EnergyTradeWorkbookEvents.bas` that are *documentation*
   (instructions + code to paste into `ThisWorkbook`) do NOT need this header
   and should not be imported as modules. Flag this clearly in their top
   comment so users don't import them by mistake.
3. When troubleshooting "Sub or Function not defined" or error 424 with a
   qualified call, first check the module name in VBE Project Explorer —
   collision suffixes (`Module1`, `EnergyTradeUpdater1`) are the smoking gun.
4. Bare sub names (no module prefix) work even with collision suffixes
   because VBA looks up `Public Sub` names across all modules. Use bare
   names in `Workbook_Open` event handlers if the module prefix isn't
   reliable.

**Incident history:** 2026-05-13 — Tore hit error 424 in
`us_fuel_trade.xlsm` because `EnergyTradeUpdater.bas` lacked this header.
Fixed by adding `Attribute VB_Name = "EnergyTradeUpdater"` to line 1 of
`src/tools/EnergyTradeUpdater.bas`. Other `.bas` files in `src/tools/`
should be audited and given the same treatment opportunistically.

---

## feedback_verify_dont_assume

*(`feedback_verify_dont_assume.md`)*

---
name: Verify infrastructure claims, don't assume
description: When user questions whether something is working (GitHub, dispatcher, DB), verify with evidence before asserting it works.
type: feedback
---

When the user questions whether infrastructure is working, VERIFY before asserting it's fine.

**Why:** On 2026-03-19 (and a prior incident with a stale branch), the user had to push back repeatedly about GitHub not working correctly. In both cases, I asserted things were fine based on surface-level checks (`git push` succeeded) without verifying the actual outcome (files appearing on other machines, branches being clean). The user was right both times.

**How to apply:**
- If user says "X isn't working" — investigate thoroughly before saying "it works"
- For GitHub: check `.gitignore`, verify files are actually tracked (`git ls-files`), not just that `push` succeeded
- For the dispatcher: check heartbeat freshness, not just PID
- For DB connections: query the actual target database, don't assume env vars are correct
- For data freshness: check the actual data timestamps, not just the collector's reported status
- Surface-level success (exit code 0, "pushed", "collected 300 rows") does not mean the intended outcome occurred

---

## feedback_weekly_update_report

*(`feedback_weekly_update_report.md`)*

---
name: Friday weekly update report to Notion
description: Generate a weekly update report every Friday at 5pm ET, saved to Notion under RLC OS. Format matches the Apr 7-10 2026 update.
type: feedback
originSessionId: 2a87c99e-d9e9-4822-83cb-2459eddd9173
---
Generate a weekly update report every Friday at 5pm ET and save to Notion under RLC OS (page ID: 2dbead023dee8052b79fe3c7db5b3aed).

**Why:** User wants to track weekly progress — what was completed, what's pending, what proposals came up. The Apr 7 update was the gold standard format.

**How to apply:**
- Title format: "Weekly Update — [Mon-Fri dates], 2026: [2-3 key topics]"
- Content structure: summary paragraph, then sections by work area (balance sheets, KG, collectors, etc.), each with bullet points of concrete deliverables
- End with commits list for the week
- Source data: git log for the week, task completions, memory updates, any notable discoveries
- Scheduled via Claude Code remote trigger, Friday 5pm ET
- Notion parent page: RLC OS (2dbead023dee8052b79fe3c7db5b3aed)

---

## feedstock_allocation_engine

*(`feedstock_allocation_engine.md`)*

---
name: Feedstock Allocation Engine
description: Bottom-up plant-level biofuel feedstock consumption model — architecture, data sources, schema, and ingestion status
type: project
---

## Feedstock Allocation Engine

Bottom-up, plant-level model for estimating US biofuel feedstock consumption across biodiesel (BD), renewable diesel (RD), sustainable aviation fuel (SAF), and co-processing.

**Why:** This is the last big piece of the oilseeds puzzle before PCAUs. User built this model manually at Fastmarkets but it was labor-intensive and imprecise. Automating it with a database-driven engine is a career-long aspiration.

**How to apply:** All four fuel types must be modeled from the start (BD, RD, SAF, co-processing). Build generalizable enough to adapt to ethanol later. Historical Jacobsen/Fastmarkets data is proprietary (is_proprietary=TRUE, never publish). Going forward, use USDA AMS prices + differentials.

### Architecture

- **Location:** `src/engines/feedstock_allocation/`
- **Schema:** `database/schemas/035_feedstock_allocation_engine.sql`
- **5 Layers:** Facility Registry → Feedstock Supply → Margin Model → Allocation Engine → Calibration

### Database Tables (on RDS)

| Table | Rows | Content |
|-------|------|---------|
| `reference.biofuel_facilities` | 124 | Master plant roster (59 BD, 61 RD, 4 co-pro) |
| `reference.feedstock_properties` | 13 | Conversion rates, categories |
| `reference.padd_regions` | 5 | PADD state mappings |
| `reference.biofuel_policy_timeline` | — | RFS mandates, credits, state programs |
| `bronze.historical_feedstock_allocation` | 6,594 | Monthly plant-level (training only) |
| `bronze.bbd_capacity_history` | 480 | Monthly capacity 2011-2050 |
| `bronze.bbd_balance_sheet` | 120 | Annual S&D by fuel type 2001-2050 |
| `bronze.credit_prices` | 450 | Monthly RIN/LCFS/CFP 2013-2050 |
| `bronze.feedstock_profitability` | 4,943 | Weekly margins by feedstock 2019-2025 |
| `gold.feedstock_allocation` | — | Engine output (not yet populated) |

### Data Sources (Spreadsheets)

All in `C:\Users\torem\RLC Dropbox\RLC Team Folder\RLC-Models\Biofuels\`:
- `Renewable Diesel Feedstock Usage.xlsx` — plant roster + monthly allocations
- `US BBD and Fuel Balance Sheets - use this version...xlsx` — capacity + S&D
- `Credit Price Forecasts.xlsx` — RIN/LCFS daily+monthly
- `US BBD Feedstock Profitability.xlsx` — weekly margins with full credit stacks
- `Global SAF Production Capacity.xlsx` — SAF facility roster

### Key Conversion Rates (lbs/gal HEFA)

SBO=7.5, CO=7.55, DCO=9.2, BFT=9.38, CWG=9.375, PF=8.12, YG=8.5, UCO=8.01

### Calibration Status (as of 2026-03-23)

First real-price run vs EIA Jan 2025: SBO -7.3% (close!), CO -44% (supply too low), DCO -22% (diversification cap too tight).

**Calibration levers identified:**
- Canola supply: increase from 1,500 to ~2,800M lbs/yr
- DCO: relax diversification cap for DCO-specialized plants
- Utilization: bump from 85% to 87-88%
- User building commodity balance sheets — will provide accurate supply figures for all feedstocks

**User feedback on engine design (critical):**
- Split into contract (60%, locked 3 months ahead) + spot (40%, Z-score driven)
- Spot purchases sized by Z-score of (forecast - actual price)
- Weight spot aggressiveness by forecast accuracy per commodity
- Run weekly or daily for spot, monthly for contracts
- Validate against EIA Form 819 (match BD exactly, assess RD independently)
- Check total demand against reported fuel output for yield reasonableness

### Data Loaded (233K+ rows total)

- 215K feedstock prices (daily 2000-2025, 49 veg oil + 32 fats/greases + FX + crush)
- 4,014 EIA reported feedstock use records (Oct 1998 - Dec 2022, calibration truth)
- 329 weekly rail rates (12 routes, 2019-2025)
- 329 weekly fuel values (BD/RD by 6 regions)
- 780 credit prices (RIN/LCFS weekly + monthly)
- 241 facilities, 160 policy records, 120 balance sheets, 480 capacity records

---

## mpob_data

*(`mpob_data.md`)*

---
name: mpob_data
description: MPOB Industry Overview data - Malaysia palm oil annual statistics 2015-2024
type: project
---

MPOB Industry Overview data ingested from 9 docx files (2016-2024 editions) in G:\My Drive\google_docs_to_add\.

**Bronze:** `bronze.mpob_industry_overview` — 440 rows, 2015-2024
- Schema: id, data_year, source_year, category, indicator, region, value, unit, source_file, ingested_at
- 9 categories: planted_area, cpo_production, closing_stocks, exports_volume, exports_revenue, imports, prices, oer, ffb_yield
- Deduplication: prefers source_year == data_year over retrospective data

**Gold:** `gold.mpob_industry_summary` — adds prior_year_value and yoy_change_pct via window functions

**Script:** `scripts/ingest_mpob_industry_overview.py`

**On both localhost and RDS.**

**Why:** User wants comprehensive palm oil market data for feedstock analysis and spreadsheet integration.
**How to apply:** When building MPOB VBA updater or analyzing palm oil markets, query gold.mpob_industry_summary.

---

## project_a2a_debate_architecture

*(`project_a2a_debate_architecture.md`)*

---
name: A2A debate-driven forecasting architecture (committed final state)
description: Capability-controlled debate system for ag forecasting. Approved as the phase 2 endpoint. Sequence after substrate (decision logs, basis layer, 3-way comparison) is ready.
type: project
originSessionId: f9769538-03ad-48d4-8472-58bcfe06d704
---
**Source spec:** `C:\dev\RLC-Agent\RLC A2A Structure.md` (Tore added 2026-05-23).

## What it is

A capability-controlled, selective-debate forecasting layer for RLC-Agent:
- **Facility agents** (default forecasting atoms — one per crusher, refiner,
  terminal etc.) generate localized hypotheses from deterministic economics
  cores like `hefa_economics.py`.
- **Broker agents** (optional) inject cross-facility flow, substitution,
  and market-intermediation arguments. Worldview agents, not physical agents.
- **Verifier** strips unsupported claims; checks evidence refs against frozen
  database snapshots.
- **Adjudicator** scores hypotheses on evidence sufficiency, causal coherence,
  rebuttal effectiveness, historical calibration, and diversity contribution.
  **Winner-take-MOST, not winner-take-all** — preserve a minority hypothesis
  when the score gap is small. Critical for regime-shift periods.
- Output lands in existing `core.forecasts` / `forecast_actual_pairs`
  tables. New tables: `core.debate_run`, `core.debate_message`,
  `core.debate_score`, `core.agent_skill_registry`.

## Why it's worth building

The compounding asset isn't per-forecast accuracy — it's the **(claim, evidence,
rebuttal, outcome) dataset** that accumulates over years. That dataset is
what makes the system improve, and what makes RLC defensible as a business
once it's published.

## Grounded design choices

- **A2A protocol from Google** (now Linux Foundation) defines the canonical
  "capability access" framing: Agent Card with skill advertisement, skill-
  scoped authorization, A2A for agent-to-agent + MCP for agent-to-tool.
  This is the right primary-source anchor.
- **Multi-agent debate literature is mixed**: recent (2024-25) evaluations
  show MAD does NOT reliably beat strong single-agent baselines unless
  triggered selectively with heterogeneous models. So the design is a GATED
  debate layer, not blanket debate-everything.
- **Deterministic economics first, LLM second** — borrows the
  `hefa_economics.py` pattern and Iowa crush spec convention.
- **Centralized adjudicator** to start. Decentralized peer-voting is
  prettier but has collusion / blind-spot risks; harder to calibrate.

## Pushback points to remember (Claude evaluation 2026-05-23)

- **Broker agents need to earn their keep.** Risk of drift into
  abstraction-without-evidence. Track per-agent realized accuracy and
  retire broker agents that don't beat their facility counterparts.
- **Heterogeneity from day 1.** Facility agents on local Ollama + broker
  / adjudicator on cloud Claude or GPT. Homogeneous debate = debate theater.
- **Scoring weights (0.30E + 0.20C + 0.15R + 0.25P + 0.10D) are invented.**
  Earn weights from backtests, not declarations.
- **Debate theater is the central failure mode.** Agents will learn to
  write persuasive rebuttals. The mitigation (score on realized outcomes,
  not transcript style) only works after months of backtest data have
  calibrated the prior. Until then, fluent agents will be over-rewarded.

## Sequencing — DO NOT start the debate-schema migrations yet

Prerequisites that have to land first:
1. **Facility decision logs (#41)** — these ARE the substrate the debate
   engine reads from. Without them, agents have nothing to argue about.
2. **Basis Field** (`project_basis_field.md`) — facility forecasts without
   basis are noise. The kriging layer is a dependency.
3. **3-way comparison framework (#37)** — provides the realized-outcome
   scoring infrastructure that turns transcripts into calibration priors.
4. **Heterogeneous LLM capacity wired into the codebase** — at minimum a
   cloud adjudicator path alongside the Ollama facility agents.

While building those, **start sneaking A2A-style Agent Card structure into
each new facility agent we build** (declare its skills + required scopes
in code). When we flip the debate switch later, the registry is already
populated organically — no big-bang migration.

## Iowa crush is the MVP benchmark

Single commodity, geographic concentration, real economics, NASS ground
truth. The Iowa work already underway (`docs/iowa_crush_agent_spec.md`)
makes this the natural first experiment. Don't try to debate macro WASDE
revisions before debate works on a constrained, well-instrumented task.

## What to capture in agent code starting now

Even before the debate engine exists, every new facility agent should
declare:
- `agent_id` (canonical, e.g. `facility:ia:adm_des_moines`)
- `role` (`facility_forecaster` | `broker_forecaster`)
- `skills` list (`forecast_run_rate`, `forecast_local_basis`,
  `defend_capacity_thesis`, etc.)
- `required_scopes` (read access to gold/silver/bronze layers it touches)
- `tool_bindings` (which MCP tools / SQL views it depends on)

A trivial Python dataclass or YAML sidecar is fine for now. Don't
over-engineer the registry until the debate engine is being wired.

---

## project_abiove_brazil_soy_complex

*(`project_abiove_brazil_soy_complex.md`)*

---
name: project-abiove-brazil-soy-complex
description: "Abiove Brazil soy-complex ingest (crush, meal/oil production, bean/meal/oil stocks) — live 2026-07-10. Manual Power BI extract (no API), thousand MT, reuses silver.monthly_realized country='BR'. Runbook + schema-decision rule."
metadata:
  type: project
  originSessionId: 4c118e7a-88d8-4867-be0e-3d804963463b
---

Abiove Brazilian soy-complex data is live in the DB (2026-07-10, commit 9ac0884e).
Full process: `docs/runbooks/abiove_update_runbook.md`. Notion:
notion.so/399ead023dee812f813ac1a0bbe1bfd0 (under RLC OS).

**Six series** (thousand metric tons): soybean crush (2012+), meal production (2025+),
oil production (2025+), bean/meal/oil stocks (2021+). See [[reference-brazil-my-alignment]].

**Access = manual.** No Abiove API; the series live only in a Power BI "publish to web"
report. The downloadable `exp_YYYYMM.xlsx` is exports-only (ComexStat), NOT crush/stocks.
Monthly input = Desktop copies Power BI pages → `data/raw/oilseeds_fats_greases/brazil_crushing_data.xlsx`.

**Chain (monthly):** `load_abiove_crushing_data.py` → bronze.abiove_soy_complex (mig 142) →
`build_silver_abiove_monthly.py` → silver.monthly_realized (country='BR', source='ABIOVE',
unit='1000 MT') → gold.abiove_soy_complex_monthly (mig 143) → `write_abiove_flat_file.py` →
models/Oilseeds/Brazil/brazil_soy_complex_monthly.xlsx (Desktop links the balance sheet here).

**Schema-decision rule established here:** new bronze almost always (mirrors source shape);
**reuse silver when the entity+grain is already modeled generically** (monthly_realized is
country-aware + has per-row unit → Brazil sits next to US, only seed_stocks attribute was new);
new gold per consumer/flat-file need.

**Two build gotchas:** (1) openpyxl `read_only=True` corrupts random `ws.cell(r,c)` access
across sheets — use `read_only=False`. (2) `Balanco_Brasil` stacks year-blocks side by side
(2025 cols C–N, 2026 `(amostra)` O–R) — parse per-block or the later year overwrites the earlier.
`(amostra)` = provisional → vintage SAMPLE rank 90 (ACTUAL=99 supersedes via MAXIFS).

**Gaps:** monthly meal/oil production pre-2025 (backfill from `World Soybean Balance
Sheets.xlsx` Brazil Soymeal/Soyoil tabs); `brazil_bio_capacity.xlsx` (separate domain, not loaded).

---

## project_agp_calibration_target

*(`project_agp_calibration_target.md`)*

---
name: AGP as ground-truth / calibration partner
description: User has an executive contact at AGP. AGP is the priority operator to "complete" first because it's a closed loop — we can compare our inferred numbers to ground-truth from inside the company.
type: project
originSessionId: 4f606138-9604-4e05-9a04-1c081995d736
---
User flagged 2026-05-09 that they have a personal contact at AGP
(executive level) and want AGP to be the first operator we fully
"complete" in the FIC. The intent is then to potentially demo it to
that contact and ask:
- How close are our inferred capacity / margin / utilization numbers
  to AGP's actuals?
- Would they find a tool like the FIC useful?

## Why AGP is a good fit

- 14 unique IA + national plants — bounded scope.
- Cooperative (no SEC filings) — pure inference test, no public-data
  shortcut. Forces us to lean on NOPA disclosures + permits + observed
  trade data + sentiment. That's the realistic case for the long tail
  of operators.
- User's existing relationship gives us an honest feedback channel
  most products never get.
- Already partially populated in our DB — fastest to "complete".

## What "completing" AGP means

1. Deduplicate `agp_*` vs `ag_processing_*` row pairs in
   `reference.oilseed_crush_facilities` (consolidate into the agp_*
   canonical, mark the others non-canonical with superseded_by).
2. Fill missing capacity rows (Aberdeen SD, Van Buren AR, Algona IA)
   from NOPA member directory + AGP public press.
3. Confirm refining_capability + biodiesel_capacity_mgy where known.
4. Run on-boarding hook for each canonical row to refresh edges.
5. Generate due-diligence reports for each via FIC Layer 4 (these can
   be packaged into a single PDF for the AGP exec to evaluate).
6. Note in each report what was INFERRED vs what was DIRECT (e.g.,
   permit-derived vs NOPA-derived vs estimated).

## Validation framework

Once AGP exec sees the output, the questions to ask:
- Capacity numbers — within 10%? 20%? Way off?
- Operational status correct (active / idle / closed)?
- Co-located biodiesel capacities — right?
- Geographic relationships (which plants compete for soybean draw) —
  do these match how AGP internally thinks about its network?
- Sentiment / news mentions — anything material we missed?
- Cross-company director links — accurate?

## What NOT to demo until verified

Per the honest-pushback rule, do not pitch this as "we know more about
your business than you do" — the framing is "we can infer this much
from public data; you tell us where we're wrong, and we use that
calibration to improve inference for the rest of the industry."

## Sequence

AGP first → fix what they flag → ADM next (24 plants, public, much
more data to play with) → other public operators → private
cooperatives.

---

## project_agp_completion_status_2026_05_09

*(`project_agp_completion_status_2026_05_09.md`)*

---
name: AGP facility inventory completion status (snapshot 2026-05-09)
description: AGP — first calibration target — is fully populated. Snapshot of what's verified vs PENDING VERIFICATION before exec contact reviews it.
type: project
originSessionId: 4f606138-9604-4e05-9a04-1c081995d736
---
Completed 2026-05-09 (mig 064 + 065). All canonical AGP plants
on-boarded; ready for due-diligence packet for Courtney Lawson at AGP.

**Why:** AGP is the chosen first operator to fully complete because the
user has an executive contact who can validate inferred numbers
against ground-truth. See `project_agp_calibration_target.md`.

**How to apply:** When user wants to demo to AGP or generate a packet,
work from this list. Distinguish what's verified vs PENDING.

## Canonical AGP plants (12)

| facility_id              | state | mmbu/yr | Refining            | Biodiesel mgy |
|--------------------------|-------|---------|---------------------|---------------|
| ia.agp_algona            | IA    | 46 *    | Unknown             | —             |
| ia.agp_eagle_grove       | IA    | 38.4    | RB                  | —             |
| ia.agp_emmetsburg        | IA    | 21.0    | —                   | —             |
| ia.agp_manning           | IA    | 19.2    | —                   | —             |
| ia.agp_mason_city        | IA    | 19.2    | —                   | 30 *          |
| ia.agp_sergeant_bluff    | IA    | 34.9    | Refining + biofuel* | 60 *          |
| ia.agp_sheldon           | IA    | 22.7    | —                   | —             |
| mn.agp_dawson            | MN    | 24.0    | —                   | —             |
| mo.agp_st_joseph         | MO    | 24.0    | —                   | —             |
| ne.agp_david_city        | NE    | 50.0    | Degumming           | —             |
| ne.agp_hastings          | NE    | 27.0    | —                   | —             |
| sd.agp_aberdeen          | SD    | 26.0 *  | —                   | —             |

`*` = inferred public knowledge, PENDING VERIFICATION (tagged in notes)

## Superseded (8)

`ia.ag_processing_*` (6) + `mn.ag_processing_dawson` +
`mo.ag_processing_st_joseph` + `ne.ag_processing_hastings` — all
`is_canonical=FALSE` with `superseded_by` populated.

## Uncertain (1)

`ar.ag_processing_van_buren` — marked `is_canonical=FALSE`. AGP
presence in AR not confirmed by NOPA directory; could be Riceland
Foods plant or shutdown. Flagged for verification.

## Coordinates

All 12 canonical plants have lat/lon. The 5 non-IA were geocoded to
city centroids (mig 065) — plant-precise lat/lon TBD if the AGP
contact wants to share them. Notes flag this.

## What to ask AGP exec (per calibration framing)

- Capacity numbers within 10%? 20%? Way off?
- Refining/biodiesel co-location at Sergeant Bluff (60 mgy) and Mason
  City (30 mgy) — correct?
- Algona at 46 mmbu/yr — too high? too low?
- Aberdeen at 26 mmbu/yr — correct?
- Van Buren AR — is there an AGP plant there at all?
- Plant-precise coordinates (we have city centroids).

## Next step beyond AGP

Per `project_agp_calibration_target.md` sequence: ADM (24 plants,
public, much richer SEC data already extracted).

---

## project_balance_sheet_framework

*(`project_balance_sheet_framework.md`)*

---
name: US Balance Sheet Framework & Workbook Inventory
description: Master framework for US commodity balance sheets — structure, workbook-per-commodity-complex pattern, tab layout (one tab per commodity, annual on top / monthly below), and cross-commodity linkage workbooks
type: project
originSessionId: bce9407b-ee9f-4848-a783-ca3236c757ef
---
# US Balance Sheet Framework

**Why:** Established 2026-04-23 as the backbone reference for the spreadsheet-side S&D system. User wants this to organize what already exists, not to drive new build-out. Phase one = spreadsheet correctness.

**How to apply:** When building, auditing, or linking any US balance sheet workbook, reference this framework for scope, tab layout, and cross-links. Do not propose separate tabs for monthly detail — monthly goes below annual on the same commodity tab.

---

## Core Identity

```
Beginning Stocks + Production + Imports  =  Total Supply
Domestic Use + Exports                    =  Total Distribution
Total Supply − Total Distribution         =  Ending Stocks
```

## Tab Layout Rule (IMPORTANT)

**One tab per commodity, NOT per data series.**

- Soybean balance sheet workbook = 3 tabs: Soy, Soy Meal, Soy Oil
- Each tab has **annual S&D at the top** (however many rows the history needs)
- **Monthly detail is entered below the annual rows on the same tab**
- Never create separate tabs for "Monthly Crush", "Monthly Exports", etc. — those rows live beneath the annual on the relevant commodity tab

## Complex Workbooks (one input → multiple outputs)

Workbooks tie co-product S&Ds together through processing math (crush × extraction rate).

| Complex | Tabs |
|---|---|
| Soybean | Soy, Soy Meal, Soy Oil |
| Canola | Canola Seed, Canola Meal, Canola Oil |
| Cottonseed | Cottonseed, Cottonseed Meal, Cottonseed Oil |
| Sunflower | Sun Seed, Sun Meal, Sun Oil |
| Peanut | Peanuts, Peanut Meal, Peanut Oil |
| Flaxseed | Flaxseed, Linseed Oil, Linseed Meal |
| Safflower | Safflower Seed, Safflower Oil, Safflower Meal |
| Copra | Copra, Coconut Oil, Copra Meal |
| Palm Kernel | PK, PKO, PK Meal |
| Corn (dry grind) | Corn sheet references DDGS + DCO + Ethanol workbooks |

## Workbook Inventory (US)

### Oilseed Complexes
`us_soybean_balance_sheet.xlsx`, `us_canola_balance_sheet.xlsx`, `us_sunflower_balance_sheet.xlsx`, `us_cottonseed_balance_sheet.xlsx`, `us_peanut_balance_sheet.xlsx`, `us_flaxseed_balance_sheet.xlsx`, `us_safflower_balance_sheet.xlsx`, `us_copra_coconut_balance_sheet.xlsx`, `us_palm_kernel_balance_sheet.xlsx`, `us_palm_oil_balance_sheet.xlsx`, `us_olive_oil_balance_sheet.xlsx`, `us_fish_oil_meal_balance_sheet.xlsx`

### Fats & Greases
`us_tallow_balance_sheet.xlsx` (EBFT/IBFT), `us_lard_balance_sheet.xlsx`, `us_cwg_balance_sheet.xlsx`, `us_yellow_grease_uco_balance_sheet.xlsx`, `us_poultry_fat_balance_sheet.xlsx`, `us_dco_corn_oil_balance_sheet.xlsx`

### Biofuels
`us_ethanol_balance_sheet.xlsx`, `us_biodiesel_balance_sheet.xlsx`, `us_renewable_diesel_balance_sheet.xlsx`, `us_saf_balance_sheet.xlsx`

### Petroleum Fuels (CY)
`us_gasoline_balance_sheet.xlsx`, `us_ulsd_distillate_balance_sheet.xlsx`, `us_jet_fuel_balance_sheet.xlsx`, `us_heating_oil_balance_sheet.xlsx`, `us_propane_balance_sheet.xlsx`, `us_natural_gas_balance_sheet.xlsx`

### Feed Grains
`us_corn_balance_sheet.xlsx`, `us_ddgs_balance_sheet.xlsx` (separate, per user), `us_sorghum_balance_sheet.xlsx`, `us_barley_balance_sheet.xlsx`, `us_oats_balance_sheet.xlsx`

### Food Grains
`us_wheat_balance_sheet.xlsx` (All Wheat + HRW + SRW + HRS + Durum + White as separate tabs), `us_rice_balance_sheet.xlsx`

### Other Majors (priority: sugar > cotton > dairy)
`us_sugar_balance_sheet.xlsx`, `us_cotton_balance_sheet.xlsx`, `us_dairy_balance_sheet.xlsx`

### Cross-Commodity Aggregators
- `us_feedstock_allocation.xlsx` — every oil/fat → BD/RD/SAF/Food/Industrial
- `us_livestock_feed_demand.xlsx` — soy meal + canola meal + DDGS + corn + sorghum → rations
- `us_livestock_slaughter_balance.xlsx` (partially DONE) — drives rendered-fat production
- `us_ethanol_byproduct_yields.xlsx` — grind → DDGS + DCO
- `us_cy_my_conversion.xlsx` — CY (energy) ↔ MY (ag)

## Monthly Detail Inputs (by commodity class, entered below annual)

| Class | Monthly Inputs |
|---|---|
| Oilseeds | NOPA weekly crush, NASS Fats/Oils monthly, Quarterly Grain Stocks, NASS oil stocks, Census trade, FAS weekly sales, crop progress/condition, NDVI |
| Oils/Fats | NASS Fats/Oils, EIA feedstock monthly, Census trade by HS code, RIN generation |
| Grains | NASS crop progress/condition, Quarterly Grain Stocks, Grain Crushings (for corn/sorghum), FAS weekly sales, Census monthly |
| Biofuels | EIA weekly/monthly production & stocks, RIN generation by D-code, Census trade, blend demand |
| Petroleum | EIA weekly production/stocks/supplied, retail price, Census trade |

## Country Pattern

Every balance sheet we build for US should replicate for Brazil, Argentina, EU, China, Canada, India, etc. — DDGS gets its own balance sheet per country.

---

## project_balance_sheet_roadmap

*(`project_balance_sheet_roadmap.md`)*

---
name: Balance Sheet Roadmap - Opening Week
description: Full commodity balance sheet buildout roadmap — fats/greases first, then fuels, then grains. Each needs production, trade, price, consumption flat files + linked balance sheet.
type: project
---

Balance sheet complex buildout for RLC opening (target: week of Apr 7, 2026).

**Architecture per commodity:** Each balance sheet links to standardized flat files:
- Production flat file (NASS/source data, months down rows, series in columns)
- Trade flat file (Census imports/exports, already exists as us_fats_greases_trade.xlsm)
- Price flat file (cash prices by location, cents/lb)
- Domestic processing/consumption flat file (biofuel feedstock allocation for fats)
- Auxiliary data flat file (livestock slaughter for animal fats, UCO collection model for UCO)

**Flat file pattern:** Matches crush file (`us_oilseed_crush.xlsm` / `NASS Low CI` sheet):
- Row 1: Commodity group headers
- Row 2: Series names
- Row 3: Units
- Row 4+: Monthly dates in col A, data in body
- Values in raw units; balance sheets divide as needed (/1,000,000 for mil lbs, /1,000 for 000 head)

**Phase 1 - Fats & Greases (must open):**
- CWG, inedible tallow, edible tallow, yellow grease, poultry fat, lard, UCO, other grease
- Livestock slaughter flat file: DONE (us_livestock_slaughter.xlsx)
- Trade data: DONE (59K records, conversion fixed)
- Balance sheet template: CWG is the template (276 rows, 16 blocks)
- Still need: allocation output file, price flat files, replicate template

**Phase 2 - Fuels (must open):**
- Biodiesel, renewable diesel/co-processing, SAF, ethanol, gasoline, diesel, jet fuel, bunker fuel
- Reference: `usethanolbal` is a good template for fuel balance sheets
- Each needs production (EIA), trade (Census), price, consumption flat files

**Phase 3 - Grains (after opening):**
- Feed grains (corn, sorghum, barley) and food grains (wheat classes, rice)
- Not started yet but equally important

**Why:** Opening = launching the RLC analytical platform with complete S&D coverage across the oilseed/fat/fuel complex. Grains follow immediately after.

**How to apply:** Prioritize getting flat file patterns right in fats/greases since they template everything downstream. Speed matters — opening is days away.

---

## project_basic_data_setup_sequence

*(`project_basic_data_setup_sequence.md`)*

---
name: basic-data-setup-sequence
description: "Tore's ordered list of remaining \"basic data setup\" areas needing the same treatment as feedstock (proper ingest, history backfill, allocator/model wiring)."
metadata: 
  node_type: memory
  type: project
  originSessionId: 05e2ef6d-523f-437f-8858-9be18d5d0aeb
---

Tore noted on 2026-05-26 that after the feedstock-allocator historical work
finishes, two more "basic data setup" areas remain:

1. **Production** — biofuel production by facility/region/fuel type with proper
   history. Partially in place via `bronze.eia_monthly_biofuels` (finished-fuel
   from API) and `silver.fuel_production_forecast` (forward forecasts). Probably
   needs same treatment: history backfill, allocator-style facility distribution
   logic that already partially exists in `distribute_production_forecasts`.
2. **Livestock** — animal numbers, slaughter rates, render outputs. Feeds into
   tallow/CWG/PF/YG feedstock supply on the other side of the BBD balance
   sheet. Likely USDA NASS Cattle / Hogs / Poultry inventory reports + FSIS
   slaughter data + rendering industry estimates.

**Why:** These three (feedstock, production, livestock) form the foundation
under the biofuel forecast/analysis stack. Once they're all in place with
real history and proper ingest, the higher-order modeling (allocator,
margin model, 20-yr projection, basis field on top of facilities) has
solid ground to stand on.

**How to apply:** When user says "we're ready for production" or "let's do
livestock," treat it as an instance of the same playbook used for
feedstock: (a) audit current ingest, (b) fix DB connection / scheduler
registration if broken, (c) historical backfill from local archives +
Wayback / API as appropriate, (d) verify against a smoke run, (e) commit
+ push + Notion update.

Sequence is feedstock → production → livestock per Tore. He flagged that
he's "hoping" these are the last basic-data areas — be alert to other gaps
that surface during the work.

---

## project_basis_field

*(`project_basis_field.md`)*

---
name: Basis as a field — the unified US basis model
description: Strategic vision (per Tore, 2026-05-01): basis is a property of economic geography, not facilities. Build the field, then plug facilities in.
type: project
originSessionId: 4f606138-9604-4e05-9a04-1c081995d736
---
## The insight (per user, framed as quantum-mechanics field analogy)
Basis is not a facility property — it's a property of the surrounding
economic geography that facilities **sample**. A new facility built next to
Eagle Grove would observe approximately the same basis (minus a small local-
competition pull-down) because basis is governed by local supply density,
demand magnets, transport arbitrage, storage capacity, policy gradients,
and quality. None of these are facility properties; they're field properties.

This means: build the **entire US basis field once**, then every facility
"plugs in" by sampling the field at its lat/lon. New facilities don't require
new modeling work; they're just additional sample points.

## Why this is the right abstraction
- **Reusable infrastructure**: built once, queries are free for every facility
  in every industry from now on
- **Gradient information**: the field's spatial gradient between two facilities
  IS a transport-arbitrage signal — surfaces opportunities the facilities
  themselves can't see
- **Uncertainty awareness**: kriging gives a confidence band at every point;
  facilities in basis "deserts" know to trust it less and weight bid scrapes
  more heavily
- **Anomaly detection**: the field's history lets us flag when an observed
  bid is several standard deviations off the smooth field — points to
  data error or genuine market dislocation

## Field structure
Five-dimensional state: `Field(x, y, t, commodity, delivery_month)`
- (x, y) = lat/lon, continental US
- t = daily, going back to ~2010 for proper stationarity / seasonality
- commodity = soy / corn / SBM / SBO / wheat / etc. — each is its own field
- delivery_month = front / K / N / Q / U / X / Z / F — carry differs from spot

## Math layer
Two complementary methods with natural division of labor:

1. **Kriging (or Gaussian Processes)** — best-in-class spatial interpolation
   between observed sample points; gives uncertainty estimates at every point.
   Inverse-distance weighting (IDW) is the simpler baseline.

2. **Physics-based corrections** — predict basis as
   (closest export terminal basis) + (transport cost differential)
   + (kriging residual from observed local samples).
   Pure interpolation is blind to "Mississippi River barge 12mi away."

## Sample sources (in order of cost vs value)
1. **AGP daily bid pages** (free scrape) — 12 facility-grade samples in MO,
   IA, NE, MN. Single biggest density jump for the Corn Belt.
2. **Cargill ag CGB site** — daily bids at Cargill river/processor locations.
3. **AMS regional reports** — already in `bronze.ams_price_record` (regional
   aggregates: "North Central IA", "Mississippi River", etc.).
4. **State DOA bid sheets** — IL/IN/IA Dept of Ag — varies by state, much free.
5. **Barchart / GoMax** — paid, only if free sources are insufficient.
6. **DTN / ProphetX** — paid fallback, last resort per user preference.

## Schema sketch
```sql
bronze.cash_bid_observation       -- raw sample points: (source, lat, lon,
                                  --  ts, commodity, delivery, basis_cents)
silver.basis_field_grid           -- gridded interpolation per (date, commodity,
                                  --  delivery): (lat, lon, basis, std_err,
                                  --  n_samples_used)
silver.basis_field_anchors        -- physical anchors: terminals, river spots,
                                  --  futures delivery points
gold.facility_basis               -- per-facility view: SELECT … FROM grid
                                  --  WHERE ST_DWithin(facility, grid, 1mi)
```
PostGIS spatial geometry on lat/lon, time-indexed for efficient querying.

## Architectural placement (Phase Two reference)
Sits between Layer 3 (Signal Generation) and the facility agents. Becomes
a `kg_callable` or its own service:
`get_basis(lat, lon, date, commodity, delivery_month)
   → (basis, uncertainty, sample_density)`

Facility agents query daily. The agent uses both the value AND the
uncertainty — when uncertainty is high, the agent weights other signals
(direct AGP bid scrape, scraped competitor bids) more heavily.

## Sequencing
- **Initial version (2-3 days)**: AGP scrape + AMS aggregates + IDW
  interpolation. Already a major upgrade over current state.
- **Production version (1-2 weeks)**: kriging with uncertainty, physics-aware
  corrections at terminals/rivers, daily auto-refresh, full historical fill.
- **Why it matters**: this is the **second-most leverage piece of
  infrastructure in the project after permit extraction**. Both compound
  across every facility, commodity, and industry we add later.

## When to build it
Per user 2026-05-01: "if we can get this to work the way I think it should
people will be blown away." Should be on the priority queue right after
the Eagle Grove showcase is dialed in. Likely the right second project to
ship after the chunked-permit-extraction fix.

## Prospecting use case (2026-05-02)
Beyond internal infrastructure, the basis field is a **sales artifact**.
When prospecting a new client / facility, plug their lat/lon into the
field and show them their own basis surface live. That's a much more
compelling demo than generic regional charts. Implication: the field
needs to look beautiful, not just be correct — every prospecting demo
is a UI moment.

**Decision (2026-05-02)**: User chose path C (basis field next) over
path B (more states). Reasoning included the prospecting angle plus the
horizontal-infrastructure framing (field benefits all 20 existing
facilities AND every future one).

## Multi-dimensional basis (Tore insight 2026-05-03, future)
Beyond the three layers (geographic field + facility identity premium +
local competitive uplift), the basis field needs to *predict*, not just
interpolate. Predictive dimensions to add:

- **Weather field**: temperature/precip/drought anomalies vs seasonal norm
  drive both current basis and forward expectations. Already have
  silver.weather_observation; need to model basis-to-weather sensitivity
  per region.
- **Production field**: USDA NASS state/county-level crop production +
  carryover stocks affect local supply-side basis. Already collecting
  via NASS pipeline; needs to feed basis predictions, not just standalone.
- **Demand-side fields**: ethanol grind, NOPA crush rate, export inspections
  by terminal — these all push basis at specific locations.
- **Policy / event fields**: rail strike, weather closures, biofuel mandate
  changes — episodic shocks the field needs to ingest.

Implementation pattern: same as Layer 1 (kriging/IDW interpolation), but
each predictive dimension produces its own field, and a meta-model combines
them with learned weights per region. The geographic field becomes the
*baseline*; weather/production/demand are the *deltas*.

This is genuinely Sprint 3+ work. Won't change the June 4 demo. But it
changes the architecture's eventual shape — the basis field isn't one
field, it's a stack of fields that get composed.

## Three-layer architecture (Tore insight, 2026-05-02)
The user asked: "does the basis prediction differ by which facility is
asking?" Answer: at the *field* layer, no — same lat/lon → same prediction.
But effective basis at a facility's gate IS facility-specific. Architecture:

1. **`silver.basis_field_grid`** (BUILT) — geographic-economic prediction.
   Universal. Same answer to any facility at the same lat/lon.

2. **`silver.facility_basis_premium`** (FUTURE) — per-facility delta from
   the field. Components:
   - Reputation premium (years_in_business, payment record, grading history)
   - Loyalty programs (co-op patronage, member dividend)
   - Operational reliability (downtime history, weather closures)
   - Forward bid program scale (can offer structured contracts?)
   Slow-moving, mostly facility-intrinsic. AGP Eagle Grove probably runs
   +8-12¢ on top of the field; new entrants start at 0¢ and converge over
   years.

3. **`silver.local_competitive_uplift`** (FUTURE) — function of all
   crushers' proximity to a point. Adding a facility *changes the
   competitive uplift for everyone in its draw area* — this is the
   genuine quantum-field analogy: presence of a particle changes the
   field. Two crushers within 50mi each pay 2-4¢ more than a monopsony
   facility would.

**Effective basis at facility F** = `field(F.lat, F.lon)
  + premium(F)
  + competitive_uplift(F, world)`

## Empirical anchor for facility premium
We can split the AMS "Mills & Processors" vs "Country Elevators" gap
(11¢ in Iowa, 2026-05-01) across individual facilities by:
- Years operating (older = higher premium)
- Co-op vs corporate (member dividend matters)
- NOPA membership flag
- Throughput scale (bigger facility = more leverage with farmers)

That's a 1-2 day modeling exercise, separate from field interpolation.

## Prospecting use of the three-layer model
The compelling sales pitch for a new facility is:
- "Field at your location: −84¢"
- "AGP Eagle Grove has built +11¢ identity premium over 40 years; you
  start at 0¢"
- "Year-1 effective basis: −86¢. Year-5 with [capabilities]: −76¢"
- "Your presence pulls the field tighter by ~3¢ for everyone in 50mi —
  $X million collective producer revenue uplift, you can use that in
  your community engagement"

This is a strategic positioning conversation, not a price quote. The
facility premium and competitive uplift layers are what make this real.

---

## project_bbd_feedstock_primary_market

*(`project_bbd_feedstock_primary_market.md`)*

---
name: project_bbd_feedstock_primary_market
description: "BBD (biomass-based diesel) feedstock markets are RLC's primary commercial market — the lens that orders all facility/crush work"
metadata: 
  node_type: memory
  type: project
  originSessionId: 7d25feff-108a-4d08-9794-c7c81da7d481
---

**RLC's primary commercial market = biomass-based diesel (BBD) feedstock markets**
(Tore, 2026-06-23). "People are probably going to give us money FIRST for our thoughts on
the BBD-feedstock markets." RLC is interested in everything (ethanol included), but BBD
feedstock is where revenue comes first.

**Why oilseed crush is the lead modeling vertical:** crush produces **soybean oil**, a BBD
feedstock. So the crush model's commercially-key output is **soybean oil production
(feedstock supply into BBD)**, not just crush margin. Aggregate national soy oil output is
the marker clients care about. Ethanol is interesting and in scope, but commercially secondary.

**How to apply:** when building/prioritizing facility models, lead with the BBD-feedstock
chain (crush→soy oil, plus UCO/tallow/DCO/canola → RD/biodiesel). Frame outputs in
feedstock-supply terms. Crush v1 = board-crush margin + per-facility soy oil output →
national soy oil supply, validated vs NASS crush + fats/oils. This connects to the IFV
framework (implied feedstock value) and the Feedstock Report. See [[project_facility_data_strategy]],
[[project_vision_endpoints]], [[feedback_client_process_separation]].

---

## project_bd_rd_trade_split

*(`project_bd_rd_trade_split.md`)*

---
name: BD vs RD trade-flow split — v1 design
description: How to allocate HS 3826 Census trade between biodiesel and renewable diesel; spec approved 2026-05-13, implementation pending
type: project
originSessionId: f9769538-03ad-48d4-8472-58bcfe06d704
---
Census trade lumps biodiesel (BD) and renewable diesel (RD) under HS 3826 — single
Schedule B export code (3826000000), two HTSUS sub-codes for imports (3826001000,
3826003000). The BD/RD split can't be read directly from HS codes, so we need a
heuristic allocation.

**Why:** Spreadsheets `us_biodiesel_bal_sheets.xlsx` and `us_renewable_diesel_bal_sheets.xlsx`
need separate trade lines; the workbook has empty Exports blocks and stale external-link
references in the Imports blocks.

**v1 approach (approved by Tore 2026-05-13):**

1. **EIA monthly biofuels as BD anchor** — replace the original spec's
   "EMTS RIN cancellation cross-check" because EPA does NOT publish a clean
   "retired for export" reason code in `bronze.epa_rfs_retirement`. Reason codes
   that appear: 80=annual compliance (266B RINs, dominates), 90=non-transport,
   120/110/130=remedial, 40=ocean vessel, etc. — none flag exports.
   Under 40 CFR 80.1430 exports trigger an Exporter RVO; the retirement happens
   under code 80 alongside obligated-party compliance and isn't separable.
2. **Total HS 3826 imports/exports** comes from `bronze.census_trade`.
3. **Subtract EIA monthly BD-only trade** (`bronze.eia_monthly_biofuels`,
   `fuel_type='biodiesel'`, `attribute='imports'|'exports'`) to get implied RD residual.
4. **Country origin heuristic** (per spec country-profile table) only distributes
   the BD and RD totals across origin countries — it doesn't determine the totals.
5. **Quarterly refresh** cadence; revise country table if origin patterns drift.
6. **v1 scope = BD vs RD only.** Co-processing split deferred to v2.

**How to apply:**
- Build `reference.biofuel_trade_split` (16-country profile table per spec, plus US
  export defaults).
- Build `gold.biofuel_trade_split` view emitting one row per source row, with
  BD and RD quantity-split columns.
- US export default: 5% RD per spec, but Tore expects this is too low (EU-bound
  RD exports from DGD Norco/Port Arthur). Validate against EIA monthly export
  series — if EIA BD-exports differ materially from Census-total-exports × 0.95,
  revise.

**Open items:**
- Migration not yet drafted (next).
- Need to confirm we actually ingest EIA monthly BD trade series — saw
  `attribute='exports'` and `'imports'` for `fuel_type='biodiesel'` (2009-2026
  for imports, 2011-2026 for exports) — ingestion is solid.
- Tore noted: government data quality may yet surprise; he'll dig through the
  RIN data on his side in case the export retirement code is hidden somewhere.

**Spec location:** `docs/specs/biodiesel_rd_trade_split.md` (updated 2026-05-13
with decisions captured).

**Mig 089 (2026-05-14) price-calibrated country rules** — major rewrite based
on import unit-price analysis. 2024 HS 3826 imports priced at $4.29-4.59/gal
across all major sources (BD pricing). RD industry RD wholesale was $5-6/gal,
none of the country aggregates match. **Conclusion**: US RD imports flow
primarily under HS 2710.20.x, not HS 3826. Earlier rules over-allocated to RD.

New rules: 85-100% BD across the board.
- Canada was 0.25/0.75 BD/RD (post-Braya) → now 0.90/0.10
- Germany was 0.70/0.30 → 0.90/0.10
- Netherlands was 0.25/0.75 → 0.90/0.10
- Singapore was 0.05/0.95 → 0.85/0.15
- France 0.80/0.20 (kept — premium $6-9/gal shipments)
- Belgium 0.85/0.15 (Dec 2024 $12.60/gal cargoes flagged as likely SAF)

Validation: BD imports reconcile at **0.74-0.81× EIA** anchor across
2018-2024 (vs 0.33-0.55× pre-089). Remaining ~22% gap likely the 3826.00.10
deflator (0.85) being slightly conservative.

**Follow-up sprint queued**: HS 2710.20.x ingestion to capture the real
RD import flows currently invisible to this view (Tidewater/Braya/Neste).

**RESOLVED 2026-05-14**: HS 2710.20.x backfilled. **No meaningful RD trade
flows under 2710 either.** 5 codes ingested (2710.20.05/10/15/25/90), 818
records 2013-2026. 2024 imports total 0.38 mil gal — 0.01% of US RD
consumption. The $103-329/gal unit prices indicate specialty/precision-blend
products, not bulk fuel. Conclusion: **~99% of US RD supply is domestic
production**. Tidewater/Braya/Neste RD imports to US under any HS code = small.
Don't pursue further HS-code-based RD import tracking. EIA + EMTS + facility
data are the right sources for US RD volume tracking, not customs.

**Mig 088 (2026-05-13) wires the split into `gold.trade_export_mapped`** so
the EnergyTradeUpdater VBA (`us_fuel_trade.xlsm` Ctrl+Y) returns RD rows.
Two important behaviors:
- HS 3826 records now emit TWO output rows (BD + RD) instead of one
  BIODIESEL row. Tore's existing BD tabs show smaller (correctly-split)
  numbers; new RD tabs work.
- Census `'-'` aggregate (TOTAL FOR ALL COUNTRIES) is excluded from
  biofuel_country_base; WORLD TOTAL + TOTAL FOR ALL COUNTRIES rows are
  synthesized by SUMming per-country splits. Otherwise the default 90/10
  share applied to the aggregate diverges from country-weighted truth
  (Canada heavily RD, EU heavily BD).

---

## project_calendar_year_vs_marketing_year

*(`project_calendar_year_vs_marketing_year.md`)*

---
name: Calendar Year vs Marketing Year Convention
description: Energy/fuels use calendar years; ag uses marketing years. Conversion required when mixing feedstock and fuel data on the same sheet.
type: project
originSessionId: 7f1aa8dc-9a7d-496a-a664-a22a410a36c8
---
## Rule

- **Ag commodities use marketing years** (Sep corn/soy, Jul wheat, Oct meals/veg oils/fats/greases)
- **Energy industry uses calendar years** (Jan-Dec) for biodiesel, renewable diesel, SAF, co-processing, ethanol, distillate, gasoline, jet fuel, bunker fuel, etc.

## Why

The two industries have entirely different reporting conventions. EIA, EPA EMTS, and all fuel industry reports publish on a calendar-year basis. Forcing energy data into marketing years would misrepresent how the industry actually thinks and communicates. Forcing ag data into calendar years would break seasonality (harvest → crush → consumption cycles).

## How to apply

1. **Energy balance sheet templates must use calendar year columns** — NOT marketing year. This differs from the oilseed/fats/grains templates.
   - Covers: biodiesel, renewable diesel, SAF, co-processing, ethanol, distillate, gasoline, jet fuel, bunker fuel, and any other fuel
2. **Feedstock flat files (UCO, tallow, CWG, SBO, canola, etc.) stay on marketing year** — Oct-Sep for oils/fats.
3. **Trade flat files for fuels** still use the monthly format with a Sep 1993 start (MY 1993/94) for template consistency with ag trade files, but the balance sheets that consume them should aggregate to calendar years.
4. **Cross-year reconciliation workbook (TO BUILD)**: When feedstock demand (ag, MY basis) must tie to fuel production (energy, CY basis), the two need a common time axis. Create a conversion workbook that:
   - Maps MY data → CY (or CY → MY) via monthly allocation
   - Provides both views for the same period
   - Let the analyst choose which side (feedstock → CY, or fuel → MY) is the "source of truth"
   - User said this can be built AFTER the flat files and balance sheets are complete
5. **When building cross-sector charts or reports**: explicitly label which year convention is in use. Never mix MY and CY in the same column without noting it.

## File naming hint

- Ag balance sheet templates: MY 2020/21 style columns, Sep/Oct start
- Energy balance sheet templates: CY 2020, CY 2021 style columns, Jan start
- The conversion workbook (future) should be named something like `us_cy_my_conversion.xlsx` or similar

---

## project_conference_deadline

*(`project_conference_deadline.md`)*

---
name: Conference proposal emails — URGENT weekend deadline
description: User MUST finish and send conference presentation proposal emails by end of weekend 2026-03-29/30
type: project
---

**URGENT**: Conference presentation proposal emails must be sent by end of weekend (March 29-30, 2026).

Presentation deck is done (v4 at conferences/), outline is done, but pitch emails to conference organizers have NOT been sent yet.

**Why:** This is career-critical. The user said presenting at conferences "is going to be one of the keys to the comfort or not of the next several years of my life." They love the technical work and will procrastinate on the business development side. The emails are the gateway to revenue.

**How to apply:**
- Saturday morning: remind user to draft/send conference proposal emails FIRST before diving into technical work
- Saturday evening: follow up — ask if emails were sent
- If user starts a new session and hasn't mentioned emails, bring it up proactively
- The deck and outline are ready. The email just needs: title, 3-sentence abstract, bio, and which conferences to target.

---

## project_copra_complex_trade

*(`project_copra_complex_trade.md`)*

---
name: Copra complex trade data (HS codes + Census collector)
description: Need HS codes and Census trade data added for copra, copra meal, and coconut oil.
type: project
originSessionId: 2a87c99e-d9e9-4822-83cb-2459eddd9173
---
User requested (2026-04-17): Add HS codes and trade data collection for the copra complex:
- Copra (dried coconut meat)
- Copra meal (crushing byproduct)
- Coconut oil

**How to apply:**
1. Identify HS codes for copra (likely 1203), copra meal (likely 2306.60), coconut oil (likely 1513.11/1513.19)
2. Add to Census trade collector HS code configuration
3. Verify bronze.census_trade has or can pull these codes
4. Wire to relevant flat files if user has coconut crush/trade sheets

**Priority:** Not urgent but should be done alongside other minor oils coverage expansion. User currently working on ag spreadsheets (canola, cottonseed).

---

## project_corn_grind_pipeline

*(`project_corn_grind_pipeline.md`)*

---
name: corn-grind-pipeline-us-grain-crush-gccp-pdf-parsing
description: NASS Grain Crushings PDF parser feeding the corn_grind tab; the positional+narrative-QC approach is the testbed for permit PDF parsing
metadata: 
  node_type: memory
  type: project
  originSessionId: ef803115-014b-4006-b2b5-b4bf7ad50ff7
---

Built 2026-06-15. Pipeline for the `corn_grind` tab of
`models/Feed Grains/us_grain_crush.xlsm`.

## Key finding
NASS QuickStats does NOT expose the Grain Crushings co-products (DDGS, distillers
grains, gluten feed/meal, germ meal, CDS, distillers corn oil, CO2) or the
dry/wet-mill corn-consumed split — verified: none of 467 QuickStats commodities.
They are published ONLY in the monthly GCCP release PDF:
`https://release.nass.usda.gov/reports/cagcMMYY.pdf` (MM/YY = release month/year;
each release covers data ~2 months prior; period parsed from the PDF header).
QuickStats DOES have corn usage (commodity_desc='CORN', USAGE) and corn oil
(Fats & Oils) — those weren't the gap.

## Architecture (all committed + pushed)
- **Collector:** `src/agents/collectors/us/nass_grain_crush_pdf_collector.py`.
  Downloads PDF → pdfplumber → parse page-2 tables → bronze.nass_processing
  (source='NASS_GCCP'). Backfill 2015+: 123 releases, ~2091 rows.
- **Gold view (mig 137):** `gold.corn_grind_monthly` — long format
  (year, month, target_col, display_value). Merges GCCP (cols C-H @ 1000bu→milbu,
  J-T @ tons→000st) + Fats & Oils corn oil (U-AA @ lb→mil lb). Computed cols
  B/S/X/AB/AC not emitted.
- **VBA:** `src/tools/CornGrindUpdater.bas`. Ctrl+K (12mo) / Ctrl+Shift+K (all).
  Row-per-month layout: finds sheet row by date (col A, rows 3-146), writes
  display_value to target_col. Import w/ ShortcutsHelper.bas; ThisWorkbook calls
  AssignCornGrindShortcuts/RemoveCornGrindShortcuts.

## The reusable PDF-parsing lessons (transferable to permits — Tore's goal)
1. **Positional extraction beats line regex.** `extract_text()` mis-segments
   long-dotted-leader tables (2019-2021 GCCP truncated numbers). Use
   `extract_words()` with x/y coords, group into rows by y, take the rightmost
   numeric token: [REDACTED-SECRET] column.
2. **Section-bound by header rows.** Older GCCP carries a SORGHUM block whose
   "Fuel alcohol"/"Dry mill" rows collide with corn labels — bound parsing to
   the corn section (between its header and the sorghum header).
3. **In-document ground-truth QC.** Page 1 narrative restates headline numbers
   ("DDGS was 1.63 million tons"); cross-check the parsed table against the prose.
   This CAUGHT a silent cross-vintage parse bug mid-backfill that would have
   corrupted ~20 months. This is the v1 of the LLM-cooperation harness — next
   step is a local-qwen second-reader that independently extracts and reconciles
   (regex↔LLM agreement → confident; disagreement → cloud escalation), proven
   on GCCP (has ground truth) before pointing at permits. See
   [[reference_local_vs_cloud_llm]], [[project_state_air_permits_llm]].

## us_grain_crush workbook — tab status (2026-06-16)
- **corn_grind:** DONE end-to-end. Collector + gold (mig 137) + CornGrindUpdater.bas
  (Ctrl+K) + dispatcher registered (`nass_grain_crush_pdf`, monthly day-3 3 PM ET).
- **weekly_ethanol_production + monthly_ethanol_data:** DONE end-to-end.
  14 EIA series added to eia_v2 SERIES_CATALOG (8 weekly sum/sndw, 6 monthly
  sum/snd) + backfilled; monthly production already in eia_monthly_biofuels.
  Gold views mig 138: `gold.weekly_ethanol_matrix` / `gold.monthly_ethanol_matrix`
  (RAW EIA values, no conversion — tabs built around native sourcekeys).
  `EthanolUpdater.bas` (Ctrl+L) handles both tabs by active sheet.
  **EIA bugfix:** eia_v2 `_save_rows` passed periods raw; monthly 'YYYY-MM'
  broke the DATE column — now padded to first-of-period (any monthly EIA series
  would have failed before).
- **corn_products:** DONE end-to-end (2026-06-17). Sources: corn input B-E from
  bronze.ers_feed_grains_yearbook Table 31 (already had it); sweetener prod
  F/G/I/J from NEW ers_corn_sweetener_collector -> bronze.corn_products_raw
  (mig 139); ERS Sugar & Sweeteners "corn sweetener supply and use" xlsx,
  all 1000 st dry. silver.corn_products + gold.corn_products_wide (mig 140).
  Monthlyization engine src/transforms/build_corn_products_monthly.py (hybrid:
  H-driver allocation, MY vs calendar true-up — verified EXACT). K=D x 15.75.
  CornProductsUpdater.bas (Ctrl+J). ers_corn_sweetener dispatcher-registered.
  - **L/M (flour/meal/grits, hominy) NOT built** — no yields in tab U:V block;
    waiting on Tore for lbs/bu assumptions, else hold native-sparse.
  - **Transform-chaining TODO:** dispatcher refreshes bronze; the silver
    transform must be re-run after (not yet auto-chained). Run
    build_corn_products_monthly.py after ERS/Table31/GCCP refresh.

## Workbook wiring TODO (Tore, in Excel)
Import into us_grain_crush + ThisWorkbook Assign/Remove on open/close, alongside
ShortcutsHelper: CornGrindUpdater (Ctrl+K), EthanolUpdater (Ctrl+L),
CornProductsUpdater (Ctrl+J). Test live. (us_grain_crush is now fully built.)

---

## project_corn_oil_balance_sheet_followup

*(`project_corn_oil_balance_sheet_followup.md`)*

---
name: corn-oil-non-dco-balance-sheet-revisit
description: us_corn_oil_balance_sheets.xlsx non-DCO and total tabs are off due to trade-flow setup — revisit after fuel balance sheets are done.
metadata: 
  node_type: memory
  type: project
  originSessionId: 05e2ef6d-523f-437f-8858-9be18d5d0aeb
---

Per Tore on 2026-05-27:

> I am good with the DCO balance sheet, so need to move on at the
> minute, but the non-dco or total corn oil balance sheet is off
> because of the way I have set up the trade flows, so good with dco
> but will revisit the other tabs there when I finish the fuels.

**What's good (don't touch):**
- DCO balance sheet — `models/Fats and Greases/us_distillers_corn_oil_balance.xlsx`
  tab "Distillers Corn Oil", linked to EIA col D, working correctly.

**What needs revisit (deferred):**
- `models/Oilseeds/us_corn_oil_balance_sheets.xlsx`:
  - Tab `corn_oil_balance_sheet` (total corn oil — currently reverted/unlinked)
  - Tab `corn_oil_balance_sheet_ex_dco` (non-DCO / food-grade corn oil)
  - Tab `dco_balance_sheet` (header says "FOOD GRADE" but tab is named dco —
    possibly mislabeled or duplicate of the Fats & Greases DCO sheet)
  - Off because of Tore's trade-flow setup in those tabs — not a data
    issue, an internal-formula issue he needs to clean up.

**When to revisit:** After fuel balance sheets are stood up. Order
per Tore's sequence: feedstock plumbing (done) → fuel side / prices /
20-yr forecast → then come back to corn oil non-DCO cleanup.

**How to recognize when it's time:** When Tore asks about fuel
balance sheets being finished, or when revisiting any corn-oil
related modeling work, flag this.

---

## project_crush_model

*(`project_crush_model.md`)*

---
name: Oilseed crush margin model design decisions
description: User's domain-specific decisions for the econometric crush model
type: project
---

**Extraction rates**: Use 5-year seasonal averages from NASS monthly oil/meal yields (not hardcoded constants). Track hull/meal production from NASS as additional validation.

**Price sources for minor oilseeds**: Start with differentials/ratios to soybean products. Future: scrape local elevator bids near facilities with minor oilseed crushing permits.

**Processing costs**: $0.55/bu for soybeans as starting estimate. Ultimate goal: bespoke cost model per facility down to equipment level.

**MAPE targets**: 5% for soybeans, start at 10% for canola/cottonseed/sunflower (tighten to 5% if model performs). Flaxseed/safflower use annual spread.

**Why:** This model feeds into the feedstock allocation engine and is a credibility-building showcase for expanding the audience beyond biofuels.

**How to apply:** Build Phase 1 (margin calc) first, validate on soybeans, then extend. Don't over-engineer early — the extraction rates and costs will be refined as plant-level data improves.

---

## project_cwg_import_collapse_2025

*(`project_cwg_import_collapse_2025.md`)*

---
name: CWG (Choice White Grease) import collapse Aug 2025 – Feb 2026
description: HS 1501200040 imports went to zero for 4 of 7 months Aug 2025-Feb 2026; not a collection bug — needs explanation
type: project
originSessionId: 4f606138-9604-4e05-9a04-1c081995d736
---
US Choice White Grease (HS 1501200040) imports showed a sharp pattern
break starting Aug 2025:

  2025-07: 14,617 MT (peak month)
  2025-08: 0
  2025-09: 3,990
  2025-10: 4,617
  2025-11: 0
  2025-12: 0
  2026-01: 0
  2026-02: 0

Four consecutive zero months going into 2026. Confirmed not a
collector bug — Census UATO CSV (May 4 2026 generation) shows the
same zeros. Other Pig Fat (1501200080) imports stayed at trickle
levels (165 MT Jan 2026), not enough to explain the missing
~5K-15K MT/month of CWG.

**Why:** unknown. Three plausible drivers:
  1. 45Z policy uncertainty pausing RD/SAF feedstock buys
  2. HS code reclassification (check yellow grease 1501200060 same months)
  3. Single major buyer pullback (2024 imports were ~50% Brazil + ~50% Canada,
     concentrated origin = concentrated buyer risk)

**How to apply:**
- Flag in weekly Notion update
- When yellow grease and tallow trade data are loaded, check whether
  imports of those substituted in for CWG during the same window
- When facility-level data is available, check whether RD plants
  in the US Gulf or West Coast paused operations or switched feedstocks
- This is exactly the kind of signal the basis-field layer should
  surface — when a low-CI feedstock import series collapses, it tells
  you something about either policy, pricing, or producer pivot

**Verification:** `gold.us_rendered_pork_fat_trade` (mig 048) shows
this directly. Run:
  SELECT year, month, cwg_mt, other_pig_fat_mt, total_mt
  FROM gold.us_rendered_pork_fat_trade
  WHERE flow='imports' AND year >= 2025
  ORDER BY year, month;

---

## project_db_password_rotation

*(`project_db_password_rotation.md`)*

---
name: project-db-password-rotation
description: Tore rotating the RDS master DB password ([REDACTED-SECRET]) — pending as of 2026-07-07. Impact list + Friday 2026-07-10 reminder if not done by end Thu 2026-07-09.
metadata: 
  node_type: memory
  type: project
  originSessionId: 4c118e7a-88d8-4867-be0e-3d804963463b
---

Tore will rotate the RDS master Postgres password (currently `[REDACTED-SECRET]`, user `postgres`) at
some point — decided 2026-07-07 (prompted by the .mcp.json/.mcp.json.template secret-hygiene
find; the old template had the literal password, now on origin history). He'll tell me when he
does it so we update everything impacted together.

**REMINDER:** if he hasn't rotated it by end of Thursday 2026-07-09, remind him Friday morning
2026-07-10. (Scheduled via routine; also surface at session start if it's Friday+ and still open.)

**Impact list — a rotation breaks ~45 files (most HARDCODE the literal, not just read `.env`):**
- **Env files:** `.env` (root), `config/credentials.env`, `.env.template`, `dashboards/ops/.env`
  (if present). `.mcp.json` working copy (commodities-db server) if it has the DB block.
- **Python hardcodes (~15):** `src/services/database/db_config.py`, `dashboards/ops/db.py`,
  `scripts/backfill_fas_psd.py`, several `scripts/deployment/*`, `database/migrations/01-04_*`,
  `src/agents/*`, etc. (grep `[REDACTED-SECRET]`).
- **VBA `.bas` updaters (~20):** all the Ctrl-key Excel updaters in `src/tools/*.bas`
  (BiofuelDataUpdater, FatsOilsUpdaterSQL, RINUpdaterSQL, EnergyTradeUpdater, TradeUpdaterSQL,
  ExportSalesUpdaterSQL, etc.) — plus the workbooks themselves connect via psqlODBC x64 with the
  password in the ODBC/connection string. Excel side needs re-pointing too.
- **`.claude/settings.local.json`** (has a `PGPASSWORD: [REDACTED-SECRET] psql` allow-rule).
- **Felipe sandbox role** has its OWN password (not `postgres`), so master rotation shouldn't
  touch it — verify.

**Right fix while we're in there:** switch the ~45 hardcodes to read `RLC_PG_PASSWORD` from env
so the NEXT rotation is a one-file change. Do the literal→env-var pass at rotation time.
See [[feedback_gitignore_shared_files]] and the .mcp.json hygiene work (2026-07-07).

---

## project_dco_corn_oil_trade_split

*(`project_dco_corn_oil_trade_split.md`)*

---
name: DCO vs Corn Oil Trade Split — asymmetric HS / country convention
description: US Schedule B (exports) splits crude corn oil by end-use (1515210010 food / 1515210050 NESOI=DCO); US HTS (imports) does NOT (single 1515210000 bucket). Convention is asymmetric: HS-based for exports, country-filter for imports.
type: project
originSessionId: 4f606138-9604-4e05-9a04-1c081995d736
---
## The Asymmetry

US export and import HS schedules treat crude corn oil DIFFERENTLY:

**Schedule B (US EXPORTS) — splits by end-use** ✅
- `1515210010` — Crude corn oil, **FOOD-grade** (food-refined or for further refining to food)
- `1515210050` — Crude corn oil, **NESOI / industrial** = **DCO**
- `1515290020` — Once-refined corn oil
- `1515290040` — Fully refined corn oil

**HTS (US IMPORTS) — single bucket only** ❌
- `1515210000` — Crude corn oil (covers both food-grade AND DCO; no split)
- `1515290020` — Once-refined
- `1515290040` — Fully refined

This is a US schedule asymmetry, not our choice. Have to live with it.

## The Convention

| Direction | HS code | Country filter | Why |
|---|---|---|---|
| **DCO Exports** | `1515210050` (NESOI/industrial) | NONE — every shipment is DCO by definition | HS code does all the work; cleanest classification |
| **DCO Imports** | `1515210000` (crude) | Canada, Argentina, Brazil ONLY | No HS split exists; only ethanol-producing neighbors realistically supply DCO |
| **Non-DCO Corn Oil Exports** | `1515210010` + `1515290020` + `1515290040` (food + refined) | NONE | Direct HS classification |
| **Non-DCO Corn Oil Imports** | `1515210000` EXCLUDING Canada/Argentina/Brazil + `1515290020` + `1515290040` | inverse country filter on crude only | Refined imports are unambiguously food/industrial |

## Industry confirmation
**Green Plains Inc.** (one of the largest US DCO producers) lists HS Code
**1515.21.0050** on their official Distillers Corn Oil product data sheet.
Specs: FFA ≤15%, moisture ≤2.5%, insoluble impurities ≤1%. This is the
industry-recognized code for DCO at the 10-digit Schedule B level.

## Top destinations of US DCO exports (HS 1515210050), 2024-2026
Per actual Census data:
- Saudi Arabia: 35,949 MT total — surprise #1 destination, not in original convention. MENA biofuel mandates + oleochemicals.
- Canada: 26,627 MT
- Egypt: 7,996 MT
- Turkey: 6,031 MT
- Italy: 5,700 MT
- Oman: 4,049 MT
- Trace amounts to most other countries

Saudi Arabia + Egypt + Turkey + Oman ≈ MENA biofuel demand cluster.
Earlier convention (NL/BE/Spain/Germany/UK/Scandinavia/Singapore/Korea/Japan)
captures EU + Asia. Both clusters are real.

## The Curaçao wrinkle (small, document for completeness)
US Customs Ruling **NY N325904** classified pretreated DCO from Curaçao
under HS `1515290040` (fully refined) instead of `1515210050`. The
pretreatment process technically meets the "refined" classification even
though the end use is still biofuel feedstock. Trace volume; ignore for
spreadsheet but document so future analysts don't get confused if they
see Curaçao in refined imports.

## What changed from the prior convention (2026-04-10)
The earlier convention had country filters on BOTH sides (DCO export to
NL/Spain/Germany/etc; DCO import from Canada/Argentina/Brazil). That was
necessary when we only had HS 1515210000 in bronze.

Now that we know HS 1515210050 exists on the export side and is
industry-recognized, the export side becomes HS-based (no country filter
needed). Import side stays country-filtered because no equivalent HS
split exists.

## Implementation checklist (for the macro and collector)
- [ ] Census collector tracks HS codes 1515210010 + 1515210050 (currently
  missing from bronze; gap identified 2026-05-04)
- [ ] TradeUpdaterSQL.bas: DCO Exports query uses HS 1515210050 directly,
  no country filter
- [ ] TradeUpdaterSQL.bas: DCO Imports query stays HS 1515210000 +
  country filter (Canada/Argentina/Brazil)
- [ ] TradeUpdaterSQL.bas: clear-all-rows on refill (not just columns
  being filled) — prevents stale data from prior schemas persisting in
  rows that no longer get written
- [ ] TradeUpdaterSQL.bas: row 216 → row 217 copy iterates ALL data
  columns, not just `columnsToUpdate` — was missing some columns

## How to apply (going forward)
When updating DCO trade sheets:
1. Macro queries by HS code on exports, by HS+country on imports
2. Clear-all-rows on refill so stale data from prior runs doesn't persist
3. Row 217 (World Total) = Sum of row 216 across all data columns
4. Periodically re-check the country filter on imports as Brazil/Argentina
   ethanol industries grow

When updating SCRP / non-DCO corn oil sheets:
1. Macro queries the inverse — HS 1515210010 + refined codes for exports
2. HS 1515210000 EXCLUDING DCO countries for imports

---

## project_dco_estimation_from_ethanol

*(`project_dco_estimation_from_ethanol.md`)*

---
name: dco-estimation-from-ethanol-production
description: "USDA DCO numbers don't reconcile across agencies — we estimate DCO supply from ethanol production. Long-term: per-facility yield-enhancement-process detection identifies high-yield DCO plants."
metadata: 
  node_type: memory
  type: project
  originSessionId: 05e2ef6d-523f-437f-8858-9be18d5d0aeb
---

Per Tore on 2026-05-27, in the context of revisiting the corn-oil
balance sheets after fuel sheets are done:

**USDA's DCO (distillers corn oil) numbers don't reconcile.** Their
own agencies report different DCO figures (ERS vs NASS vs EIA's
biofuel feedstock allocation). The standard workaround is:

**Estimate DCO supply from ethanol production.** Implied DCO =
ethanol gallons × typical DCO yield per gallon of ethanol (industry
benchmark, varies by plant tech).

The yield isn't uniform across plants. Some ethanol facilities have
installed **DCO yield-enhancement processes** (heated DCO extraction,
mechanical separation upgrades, enzyme treatments, etc.) that
materially increase oil yield. A plant with these processes might
yield 0.7-0.9 lb DCO/bu corn vs ~0.4-0.5 lb for a basic setup.

**Long-term answer is per-facility modeling.** Each ethanol facility
in the facility graph should be tagged with whether it has
yield-enhancement installed (via permit data, press releases,
industry filings). Then aggregate facility-level DCO yields × actual
gallons produced gives a bottom-up estimate that beats USDA.

**Interim answer**: use a blended national yield assumption
(~0.55 lb DCO/gal ethanol per typical industry estimate) until the
facility detection is built.

Related: 
- [[project_permit_parsing_secret_sauce]] — permits are the source
  of facility-level operational detail; DCO yield enhancement is a
  prime candidate for permit-derived facility tagging.
- [[project_corn_oil_balance_sheet_followup]] — the non-DCO corn
  oil cleanup deferred until fuel balance sheets are done.
- [[project_basic_data_setup_sequence]] — broader sequence:
  feedstock → production → livestock.

**When to apply**: any time we model corn-oil supply for biofuel
feedstock allocation OR build the DCO balance sheet template.
Default to the ethanol-derived estimate, not USDA reported numbers.

---

## project_dod_security_posture

*(`project_dod_security_posture.md`)*

---
name: Long-arc goal — DoD-grade security posture for RLC
description: Tore wants RLC's security posture to eventually match what a co-op CFO described as "DoD-ready" — meaningful for prospective clients in defense-adjacent or regulated industries.
type: project
originSessionId: f9769538-03ad-48d4-8472-58bcfe06d704
---
Tore flagged this 2026-05-25 in passing — the CFO of a co-op
client/contact mentioned having security at a level that would let him
do business with the DoD, and that's the posture Tore wants RLC to
reach eventually.

**Not today.** But every decision we make about secrets, auth, logging,
audit, access control, and data retention should be made WITH this
future end-state in view — so we don't have to rebuild from scratch
when a regulated client wants in.

## What "DoD-ready" usually means in practice

For a small ag-data/analytics firm working toward DoD-adjacent clients
(USDA, USDA Foreign Agricultural Service, Defense Logistics Agency
food/fuel programs, etc.), the bar is typically some subset of:

- **NIST 800-171 / CMMC Level 1-2** for Controlled Unclassified
  Information (CUI). 110 requirements at L2.
- **FedRAMP Moderate** if SaaS-hosted (cloud baseline)
- **CJIS** for criminal justice info (probably N/A for ag)
- **FISMA Low/Moderate** for federal info systems

Not all of these will apply, but the building blocks overlap.

## Building blocks that compound (decisions that map to "yes, we have that")

| Area | Standard practice today → DoD-ready target |
|---|---|
| Secrets management | `.env` file → vault (HashiCorp/AWS Secrets Manager), no secrets in code |
| Authentication | Direct DB creds → SSO + per-user DB roles |
| Authorization | Single admin role → least-privilege per-service roles |
| Audit logging | Postgres logs only → tamper-evident audit trail of every read/write of sensitive data |
| Encryption at rest | RDS default (yes) → KMS-managed keys with rotation policy |
| Encryption in transit | TLS yes → enforced TLS 1.2+ everywhere, mTLS for service-to-service |
| Access reviews | None → quarterly review of who has access to what |
| Change control | Solo commits to main → reviewed PRs, signed commits, deployment approvals |
| Backup + DR | Manual exports → tested RPO/RTO, encrypted off-site backups |
| Vulnerability mgmt | None → SBOM, dependency scanning, patch SLAs |
| Incident response | None → documented IR plan, breach notification process |
| Personnel | Sole operator → background-check policy when first hires happen |

## What to keep in mind on every decision

1. **Don't bake in things that block DoD readiness later.** Hard-coded
   passwords, secrets-in-code, plain-text logs of sensitive data, etc.
2. **Track every credential and external integration in one place.**
   When auditors ask "where do you store secrets and who has access?",
   you should be able to answer in 30 seconds.
3. **Default to logging access to PII/CUI-adjacent data.** Even if
   nothing requires it today, the audit trail compounds.
4. **Prefer cloud services already FedRAMP-authorized** (AWS GovCloud
   is overkill, but commercial AWS in regions with FedRAMP services
   is the right starting point — RDS, S3, etc. are mostly there).
5. **Document.** Many DoD-readiness checks come down to "show us the
   policy document." Cheap to write a paragraph now, expensive to
   reconstruct from memory later.

## Not blocking near-term work

This is a strategic frame, not a sprint goal. Don't refactor things
that work just because they aren't yet DoD-grade. But when you're
designing something new — a new collector's auth scheme, a new client
report's access model, a new MCP tool — pause for 30 seconds and ask
"does this make the DoD path harder?" If yes, pick the alternative.

---

## project_drew_lerner_archive_backfill

*(`project_drew_lerner_archive_backfill.md`)*

---
name: Drew Lerner email archive backfill — historical weather corpus
description: Multi-year Gmail backfill of Drew Lerner's weather emails + PDF attachments, parsed to populate region-specific weather event corpus for Market Field calibration
type: project
originSessionId: 4f606138-9604-4e05-9a04-1c081995d736
---
User has all of Drew Lerner's daily weather emails in Gmail going back
years (some lost to a deletion regret — accept the gap). Pulling and
parsing them is the highest-quality region-specific weather signal we
can put into the Market Field beyond current daily collection.

Treated as Sprint 4 (or later) work. Not for tonight.

## Why this matters

- **Daily granularity** — Drew sends 1-3 emails per day during growing season
- **Region/county-level specificity** — most national weather news is too coarse for
  facility-level network propagation; Drew names the actual counties and river basins
- **Years of historical coverage** — exactly the temporal depth our calibration
  data was missing (chart corpus is 1969-2016 but our sentiment data starts 2024)
- **PDF attachments** — usually forecast maps + region tables; useful for
  facility tagging via geographic mentions
- **Credentialed source** — Drew is a known meteorologist, so polarity and
  intensity assignments are more defensible than generic news scoring

## Scope estimate

- **Volume:** ~10,000-15,000 emails over 10 years if archive intact;
  call it 5,000-8,000 with the reported deletions. 1-3 PDF attachments
  per email = 10,000-25,000 PDF pages.
- **Time:** Gmail API pulls at ~0.3s per message = 30-60 min for 10K msgs.
  PDF rendering + vision parsing: ~30 sec per page = several days of
  background processing. Run in tranches.
- **Cost:** Text classification of email bodies ~$0.001 each = $10-20.
  Vision parsing of PDF maps ~$0.03-0.05 per page = $300-1200. Total
  ballpark **$300-1300**, scope-dependent.

## Architecture sketch (Sprint 4)

### New tables (migration TBD)

```sql
bronze.weather_email_archive (
    -- One row per Gmail message
    id BIGSERIAL PK, gmail_id TEXT UNIQUE, message_date TIMESTAMPTZ,
    sender TEXT, subject TEXT, body_text TEXT, body_html TEXT,
    has_attachments BOOL, processed BOOL, fetched_at TIMESTAMPTZ
)

bronze.weather_email_attachment (
    -- One row per attachment (PDFs, images)
    id BIGSERIAL PK, email_id BIGINT FK,
    filename TEXT, mime_type TEXT,
    storage_path TEXT,                  -- where the file lives on disk
    file_hash TEXT, file_size BIGINT,
    page_count INT,                     -- for PDFs
    extracted BOOL, extracted_at TIMESTAMPTZ
)

silver.weather_event (
    -- Per-region weather event derived from email body or PDF page
    id BIGSERIAL, source_email_id BIGINT FK, source_attachment_id BIGINT FK,
    event_date DATE, region_label TEXT,
    region_kg_keys TEXT[],              -- counties / states matched against KG
    facility_relevance_keys TEXT[],     -- IA facilities affected
    event_type TEXT,                    -- 'drought', 'frost', 'flood', etc.
    polarity NUMERIC, intensity NUMERIC,
    confidence NUMERIC,
    raw_text TEXT, raw_classification JSONB
)
```

### Backfill collector (`scripts/backfill_drew_lerner_archive.py`)

1. Connect to Gmail OAuth (reuse existing token from
   `weather_intelligence_agent.py`)
2. Iterate by date range (e.g., year by year, oldest first):
     `query = "from:worldweather@bizkc.rr.com OR from:akarst_worldweather@... after:YYYY/MM/DD before:YYYY/MM/DD"`
3. For each message:
     - Extract body_text + body_html
     - Download all PDF + image attachments to disk
     - Insert one row to bronze.weather_email_archive
     - Insert N rows to bronze.weather_email_attachment
4. Idempotent on gmail_id (UNIQUE constraint)
5. Resumable — track last successful date in a state file

### Parser (`scripts/parse_drew_email_corpus.py`)

For each unprocessed email:

  a. **Body parsing** (Claude text):
     Topic + region + polarity per paragraph. Cheap (~$0.001 per email).
     Output: silver.weather_event rows.

  b. **PDF attachment parsing** (Claude vision, batched):
     For each page, extract: map title, date, region depicted,
     key annotations. Same pattern as our chart annotation extractor.
     Reuse `extract_chart_annotations.py` with a weather-map-specific
     system prompt. ~$0.03-0.05 per page.

### Region -> Facility mapping

Drew's regions are described by county + state + river-basin language
(e.g., "Tennessee River Basin", "western Iowa", "south-central
Minnesota"). For each named region, do best-effort mapping to our
24 IA facilities by:
  - County match (exact when Drew names a county)
  - State match (broader than facility but useful default)
  - Distance from facility lat/lon when river-basin language

Same list of regional terms occurring repeatedly should make this
tractable with a small lookup table built once.

## Recommended pilot scope (first session)

Don't backfill everything at once. Start narrow, prove the pipeline,
expand:

1. **Pull from Round Lakes inbox first** (tore.alden@roundlakescommodities.com)
   — OAuth already wired, ~3,000 Drew emails since late-2024/early-2025,
   overlaps perfectly with our 2024+ news+sentiment window so direction
   calibration works immediately. This is the right pilot.
2. **Verify region-to-facility mapping accuracy** by sampling 50
   region mentions and checking by hand
3. **Compute Drew-event-implied sentiment shifts** via the calibrator
   in direction mode for the date range we have
4. **THEN, if pilot succeeds, set up OAuth for personal Gmail**
   (toremalden@gmail.com) and pull the deeper historical archive
   (Sprint 4 phase 2). That gives us back to ~2010s depending on
   personal-account retention.

## What to do NEXT session

1. Apply schema migration for the three new tables
2. Build `backfill_drew_lerner_archive.py` (resumable, idempotent,
   year-tranches)
3. Run for 2024 only as the pilot
4. Build `parse_drew_email_corpus.py` reusing existing extractor
5. Sample-validate before scaling

## Reference

- Existing weather pipeline:
  `rlc_scheduler/agents/weather_intelligence_agent.py` —
  has Gmail OAuth, can use its token. Reuse.
- Sender list (per current pipeline):
  - worldweather@bizkc.rr.com
  - akarst_worldweather@bizkc.rr.com
  - scarlett_worldweather@bizkc.rr.com
  - brad_worldweather@bizkc.rr.com
  - (user noted there was a 5th historical sender — verify)

---

## project_dual_track_views

*(`project_dual_track_views.md`)*

---
name: Dual track views — marketing/client vs internal agents
description: Maintain separate marketing/client views and internal agent views, but merge them for the multi-source guidance mechanism that feeds facility agents. Include USDA numbers as a third source.
type: project
originSessionId: 2a87c99e-d9e9-4822-83cb-2459eddd9173
---
**Three guidance sources for facility agents:**
1. **Internal models** — our seasonal projections, KG-driven anomaly detection, econometric forecasts
2. **Marketing/client view** — external-facing analysis, report-quality narratives
3. **USDA numbers** — because actual market participants use USDA to understand market state; agents need to know what the market thinks, not just what we think

**Why:** Dual track is needed for business (clients see polished analysis, internal agents see raw signals). But the facility agents should consume ALL three sources — our models, our client-facing analysis, AND USDA's published outlook — because real buyers and sellers operate on a blend of these inputs.

**How to apply:** Build a multi-source guidance mechanism that:
- Merges internal + marketing + USDA views into a unified signal set
- Each source has weight/credibility scoring
- Agents can reason about divergences ("our model says X, USDA says Y, the market is pricing Z")
- This divergence detection IS the alpha

**Added:** 2026-04-18

---

## project_eagle_grove_deferred_items

*(`project_eagle_grove_deferred_items.md`)*

---
name: AGP Eagle Grove showcase — deferred refinements queue
description: Improvements queued for the Eagle Grove deep-dive page (dashboards/facility/eagle_grove.py)
type: project
originSessionId: 4f606138-9604-4e05-9a04-1c081995d736
---
The first facility deep-dive shipped 2026-05-01 at
`dashboards/facility/eagle_grove.py`. User feedback was "OH THAT IS A LOT
BETTER THAN CHRISTMAS". These are the refinements to ship in subsequent
passes — preserved here so we don't lose them.

## Hero section sharpening (next iteration, in progress)
- Satellite map with marker at Eagle Grove (42.66, -93.90), high-zoom Esri imagery
- Facility photo (real satellite at z18+, NOT stock imagery)
- Larger AGP wordmark / brand element
- Possibly add: county outline, draw-radius circle (50mi)

## Data wiring still pending
- **AGP daily-bid scraper** — facility-specific basis for the most important
  AGP nodes. Free, just web scrape. See `project_basis_field.md` for context.
- **AMS regional series** for basis history at "North Central IA country
  elevators" (already in `bronze.ams_price_record` — just needs to be
  surfaced and joined to ZS futures for basis time series).
- **Real share-of-IA computation** from
  `reference.oilseed_crush_facilities` — replace placeholder 4.7% with
  Eagle Grove's actual mmbu_yr_share / sum(IA mmbu_yr_share). Needs
  reconciliation of the dual rows for Eagle Grove first.
- **Mass-flow Sankey using Eagle-Grove-specific yields** — when we have
  facility-level oil yield observed from NOPA, replace canonical 11.5 lb/bu
  with the actual.
- **Strategic plan tab** — still placeholder; needs Layer 1 strategic_plan
  agent (per Phase Two architecture) to populate target coverage, hedge
  ratios, basis bid ceilings. That's a separate project.

## Generalizing the page
Pattern is: 1 facility → 1 Streamlit page. To make this generalize:
- Lift constants (FACILITY_ID, FACILITY_DISPLAY) to URL params
- Build a `dashboards/facility/_template.py` that takes facility_id and
  renders the same layout
- Index page at `dashboards/facility/index.py` lists all facilities with
  thumbnails

## Visual polish to consider later
- Hover-to-verify per-paragraph sourcing (Pudding/FT scrollytelling style)
- Animated process flow (Sankey transitions as state evolves)
- Per-emission-unit drill-down on click (open modal with that unit's
  rated capacity, age, applicable NSPS subparts, control device details)
- Mobile/tablet responsive layout

## Scope reminder
This page is the SHOWCASE for the Phase Two architecture. It demonstrates
"facility as agent-ready entity" by combining everything: static profile,
KG context, permit-extracted equipment, NOPA-derived throughput, live
crush margin, strategic plan. Each new facility we add (Cargill Iowa
Falls next? AGP Sergeant Bluff?) follows this template.

---

## project_facility_agent_leaderboard

*(`project_facility_agent_leaderboard.md`)*

---
name: Facility agent leaderboard — gamification as meta-learning
description: Per Tore (2026-05-03) — gamify facility agent performance with promotion/relegation tiers. The agent itself doesn't have incentives, but the leaderboard drives evolution of prompts, tool stacks, strategic plans, and resource allocation across the agent population.
type: project
originSessionId: 4f606138-9604-4e05-9a04-1c081995d736
---
## The insight (Tore framing, 2026-05-03)
"Make it a game, with the agent doing the best getting an earlier spot in
the daily run queue and maybe switch from the 4060 to the 5080 for the
top five every quarter or something like that (think demotion in the
English Premier League)."

## Honest assessment of what works
The LLM agent itself doesn't "want" anything — faster compute or earlier
queue position don't map to incentives the way they do for human traders.
The model runs identically regardless of priority.

**BUT** the gamification idea is genuinely powerful one level up: it's
**meta-learning over a population of agents**. The leaderboard drives
evolution of the things that ARE mutable: prompts, tool stacks, strategic
plans, decision rules.

## What "promotion" actually means in practice
For top-tier (top 5 per quarter) agents, allocate:

1. **Better information** (per Tore refinement 2026-05-03)
   - **LAST in the daily queue, with full visibility into every other agent's
     decisions**. This inverts the naive "first-in-queue" framing — the real
     information advantage is *late information*. Top performers see how
     every other agent bid, what they covered, what they passed on, and
     factor that into their own decision. Like a closing auction in stocks
     or real-world end-of-day basis bidding.
   - Longer forward horizons in the basis curve
   - Deeper KG context (more nodes, more relationships)
   - Access to peer agents' decision logs from the same day

2. **More sophisticated tools**
   - Multi-step reasoning (chain-of-thought prompting)
   - Web augmentation for breaking news
   - Larger context windows (more historical data)
   - Higher-tier model (Claude Opus instead of qwen2.5:7b for daily run)

3. **More risk budget**
   - More bushels to bid on (amplifies system-level edge)
   - Higher max-open-position limits
   - Longer-dated forward commitments allowed

4. **Prompt/strategy DNA propagation**
   - Bottom-tier prompts get *replaced* with mutations of top-tier prompts
   - This is real automated prompt engineering — leaderboard = fitness function
   - Genetic programming loop on prompt populations

5. **Promotion to autonomous execution**
   - Top tier: decisions auto-execute
   - Middle tier: decisions require human review before action
   - Bottom tier: decisions are advisory only

## What "relegation" means
Bottom-tier facilities get:
- Cheaper/slower compute (qwen2.5:7b on 4060 vs Claude Opus on cloud)
- Fewer runs per day (signals are stale)
- Smaller risk budget
- Required human review on every decision
- Their prompts replaced by mutations of top performers'

## Scoring metric — this matters more than the mechanism
Naive metric: "highest sales price, lowest input price vs normal" rewards
home-run hitters who can blow up. Better options:
- **Sharpe-style** (return per unit of variance) — drives long-run consistency
- **Information ratio** vs benchmark (e.g., spot vs board crush)
- **Hit rate × avg gain** (fraction of times decision was right × magnitude when right)
- **Composite** with separate scores for crush margin capture, basis improvement,
  and risk-adjusted return

Recommendation: composite, weighted toward consistency over magnitude.
Commodity merchandising is a long game.

## Architectural placement
The leaderboard belongs in Layer 1 (the strategic agent, quarterly Claude
Opus run per Phase Two architecture). Specifically:
- `silver.facility_agent_score` — daily/quarterly performance metrics
- `silver.facility_agent_tier` — promotion/relegation status
- `silver.facility_agent_prompt_lineage` — prompt evolution tree

The strategic agent reviews the leaderboard quarterly and:
1. Recomputes tiers
2. Adjusts each facility's daily-run prompt based on its tier
3. Mutates prompts for genetic-programming exploration
4. Re-allocates compute resources

## When to build
- Sprint 2/3 work — depends on having multiple facilities running daily
  decision loops first (Sprint 1 W3 builds the position/P&L tables;
  facilities-running-decisions is Sprint 2 work).
- The metric design is the harder part than the mechanism. Worth
  prototyping in shadow mode (track scores without acting on them) for
  several months before flipping promotion/relegation on.

## Why this is actually exciting
This is the bridge from "platform that helps you decide" to "platform
that learns how to decide better." The leaderboard mechanism transforms
the facility agent population from a static fleet into a self-improving
ensemble. That's a research-grade architectural move and probably
patent-worthy if novel in this domain.

---

## project_facility_agent_model

*(`project_facility_agent_model.md`)*

---
name: Facility agent model — junior buyer simulation
description: Plant-level daily decision agents that accumulate positions, track P&L, and compete for feedstock
type: project
---

**Priority**: Build after launch week. This is the showcase product — the thing nobody else has.

**Core concept**: Each BBD facility is an autonomous agent that makes daily procurement and production decisions based on current prices, forward coverage, and margin optimization. Agents accumulate state day-to-day (positions, inventory, commitments). Monthly aggregation produces national volume estimates that compete with EMTS actuals.

**Agent state (persisted daily)**:
- Feedstock inventory by type (lbs on hand)
- Forward feedstock purchases (committed, not yet delivered)
- Fuel sales commitments (RD, BD, SAF)
- RIN position (generated vs. obligated)
- LCFS/45Z credit position
- Unrealized P&L by feedstock
- Capacity utilization rate

**Daily decision logic**:
- Given today's prices, what's my margin by feedstock?
- How much of my forward capacity is covered?
- Should I buy spot, buy forward, or wait?
- Should I shift my feedstock mix?
- Constraint check: pathway approvals, tank capacity, logistics

**Gamification**:
- Each agent scored on net margin $/gal
- Leaderboard across facilities
- Best feedstock sourcing decisions highlighted
- Worst decisions analyzed for learning

**Dashboard needs**:
- Individual facility view (REG Geismar, DGD Norco, etc.)
- Portfolio view (all facilities)
- Feedstock competition map (who's buying what where)
- Margin heatmap by facility × feedstock
- Forward coverage percentage by facility

**Why it matters**: This is the product that Fastmarkets can't build and Helios isn't building. Bottom-up facility-level intelligence that aggregates to national estimates. The presentation thesis made real.

---

## project_facility_data_strategy

*(`project_facility_data_strategy.md`)*

---
name: project_facility_data_strategy
description: "Strategy for facility capacity + operating status data — the two signals, permit grind off critical path, geo crosswalk"
metadata: 
  node_type: memory
  type: project
  originSessionId: 7d25feff-108a-4d08-9794-c7c81da7d481
---

**The two signals we need per facility: capacity + operating status** (Tore, 2026-06-22).
The permit equipment-list is a *means* (confirms capacity, distinguishes operating/shuttered),
not the goal. Inventory of what we already have settled the strategy:

## We already have both signals nationally
- **Operating status: ~solved** — EPA ECHO (`gold.facility_capacity`, 2,870 facilities) has
  `operating_status` on 99.8% (1,895 Operating / 898 Permanently Closed). National, free.
- **Capacity: covered for core sectors** via curated reference lists (NOT permits): ethanol
  191 (191/191 capacity+status), oilseed crush 137 (137 status / 78 capacity / 34 have a
  crush model xlsx), RD 66, biodiesel 192. = 586 facilities. Only 29 had permit-derived capacity.

## Permit grind is OFF the critical path (decided after testing NY, IN, PA)
Bulk per-state permit acquisition hits a **universal crosswalk problem**: each state's permit
repo enumerates by a state-specific ID that doesn't join ECHO and lacks facility names in the
listing. IN = inconsistent Oracle WebCenter metadata (see [[reference_idem_oracle_webcenter_permits]]);
NY = Title-V-only (misses our minor sources, 9% match); PA = enumerable but no names in filenames
+ ECHO "ethanol" = SIC false-positives (refineries). FRS get_program_facilities (the would-be
crosswalk) returns 500. So we DON'T grind permits for capacity/status — we have them.
Permits kept **surgically**: on-demand per modeled facility (2-3/day for Tore's visual exam,
which catches what the LLM schema drops), capacity-confirmation cross-check, diagnostic trigger.
IA archive (288 fac / 8,847 units) retained as worked example.

## Geo-matching is the crosswalk (Tore's idea, 2026-06-22) — but lists need geocoding first
Geo (lat/long of the regulated site) beats address-string matching (office-vs-plant addresses
diverge). Use TIGHT threshold (~150m) + name-similarity confirm; 500m alone gives industrial-park
false positives. CAVEAT: curated lists mostly LACK coords (ethanol 0, crush 33/137, RD 0, biodiesel
0) — they have city/state. So workflow = geocode curated lists (Census batch geocoder, free) ->
geo-match to ECHO (has coords) -> unified master = capacity (curated) + coords/frs_id/status (ECHO).
Until geocoded, match curated->ECHO by name+city (both named).

## Tore's own capacity source files (found 2026-06-23; Windows paths — Python needs `D:/` not `/d/`)
- **Crush capacity (US+Canadian soy+canola):** `C:/Users/torem/OneDrive/Desktop/Models/Oilseeds/
  North American Oilseed Crushing Capacity.xlsx` — sheets US Soy Crush / Canola Crush (+RC) /
  Closed / Food Plant List / utilization tabs. US Soy Crush cols: capacity (tons/bu) + **Yearly
  Oil Production Capacity (tons + million lbs)** + Expansion notes. Earlier siblings on D:
  `D:/Plant Lists/Oilseed Crushing Plants/` (Soybean Crushing Plants and Oil Processors.xlsx, 2019).
  CONFIRMED 2026-06-23 the OneDrive file IS the current/canonical version (~Dec 2025 save) —
  use it as the crush capacity source (strong-not-canon). The D: ones are older verification pts.
- **Biofuel capacity:** canonical = OneDrive `Models/Biofuels/Global SAF Production Capacity.xlsx`
  (Tore: "should have all of our biofuel capacity estimates"). Earlier versions on `D:/Plant Lists/`
  (Biomass-Based Diesel Plants/, Ethanol Plants/, RD Breakout/Comparison/Feedstock Build Up, Bob's
  Plant List "RD Capacity New").

## FUTURE (parked by Tore 2026-06-23, after setup complete): capacity-monitoring as a product feature
Capacity is a hard data point to capture. Once the system is set up, establish a recurring
procedure (daily/weekly) to CHECK FOR CAPACITY CHANGES, and **highlight a live, verified capacity
number in client output** as a differentiator. Ties to the capacity-utilization time series (crush
+ biofuel) being the interesting series. Revisit when facility models + master are operational.
- **Connection:** `reference.ethanol_facilities.data_source = "Ethanol Plants.xlsx"` — our ethanol
  list was built FROM `D:/Plant Lists/Ethanol Plants/Ethanol Plants.xlsx`.
- **CAUTION:** a HOBO/FM folder exists with parallel files (`.../HOBO/Biofuel Model/.../Soy and
  Canola Crushing Plants and Oil Processing - HOBO.xlsx`, feedstock flat files). FM-provenance —
  internal triangulation ONLY, never client-facing (see [[user_career_legal]],
  [[feedback_fastmarkets_keep_dont_show]]). Use TORE's files, not the HOBO ones.

## VERIFICATION (2026-06-23): Tore's crush file vs our v1 — corroborates within ~10%
Tore's file: 68 open US soy plants = 46.0 M tons/yr = **1.53 B bu/yr**. Our crush v1 modeled
subset (64 operating w/ capacity) = **1.68 B bu/yr**. Both below current US national (~2.4-2.6 B):
Tore's = ~2019-21 vintage (pre RD-driven crush expansion); ours = the 59 capacity gaps. Two
independent estimates within 10% = good accuracy signal. Oil yield also corroborated (Tore's
Decatur 649.7 M lb vs our 57.7M bu × 11 = 635 M lb, ~2%).

## Next step (unblocked, high-value)
Build the facility master: reconcile curated lists ∪ ECHO (geo after geocoding, or name+city now)
-> single master with capacity + status -> feeds the oilseed-crush economic model (137 crush +
IA depth). "Missing a facility entirely" is caught by this reconciliation, not by permits.
Scan files for Tore: `data/exports/crush_facilities_capacity_scan.csv` (59 of 137 crush missing
capacity), `data/exports/ny_titlev_permits_sample.csv`. See docs/planning/facility_capacity_status_inventory.md.
Related: [[reference_echo_canonical_facility_source]], [[project_permit_archive]].

---

## project_facility_external_xref

*(`project_facility_external_xref.md`)*

---
name: Facility ↔ external-list cross-reference machinery
description: Generic xref layer matching our DB facilities against CARB LCFS (and future EPA RFS, EIA, Biodiesel Mag) with confidence scoring + closure-suspect anomaly detection
type: project
originSessionId: 4f606138-9604-4e05-9a04-1c081995d736
---
# Facility cross-reference machinery

Generic infrastructure for matching our internal DB facilities against external
public lists (CARB LCFS, EPA RFS, EIA, Biodiesel Mag). Shipped 2026-05-10.

**Why this matters:** CARB-pathway absence is a strong "not running OR not
shipping to CA" signal that surfaced REG Ralston + REG Madison closures from
the SEC filing trail. Same trick will catch other operators' hidden idlings
when extended to ADM, Bunge, Cargill, Valero, DGD, etc. Critical for the
emerging RD/SAF/AtJ markets where official production capacity diverges from
real production.

## Architecture

```
bronze.carb_lcfs_pathways           — raw CARB snapshots (PK: row_id BIGSERIAL)
silver.normalize_facility_name(name) — IMMUTABLE PL/pgSQL fn, single source of truth
silver.facility_tokens(name)         — IMMUTABLE PL/pgSQL fn, returns TEXT[] tokens
                                       (drops noise: biodiesel/renewable/llc/etc.)
silver.facility_norm                 — MATERIALIZED VIEW (refresh after schema changes)
                                       Unions our 3 BD/RD/biofuel reference tables.
silver.external_facility_norm        — MATERIALIZED VIEW
                                       Pulls latest CARB snapshot, dedupes to facility level.
silver.facility_external_xref        — VIEW (always-fresh).
                                       Cross-join with token overlap, emits
                                       match_confidence 0.0 / 0.4 / 0.7 / 1.0.
gold.facility_status_anomalies       — VIEW (always-fresh).
                                       Classifies each DB facility:
                                       confirmed_active | weak_match_needs_review |
                                       closure_suspect_no_carb | expected_no_carb |
                                       too_small_for_carb_expected | unknown
```

## Migrations
- 073 — bronze.carb_lcfs_pathways + silver.carb_pathway_dim + silver.facility_carb_status
- 074 — surrogate BIGSERIAL PK (pathway_id NOT unique in source — same ID can have
        multiple feedstock/CI rows)
- 075 — facility_norm + external_facility_norm + facility_external_xref + status_anomalies

## Loader
`scripts/load_carb_pathways_to_bronze.py` — idempotent. Snapshot date defaults
to today. Each invocation creates one snapshot row per pathway. History
preserved for "was certified Q1, gone by Q2" detection.

## Validation as of 2026-05-10

- 892 CARB pathways loaded, 79 distinct fuel producers, 81 distinct facility names
- 79 facilities `confirmed_active`, 99 `closure_suspect_no_carb`, 83 `weak_match_needs_review`
- REG Ralston + Madison correctly fall into `expected_no_carb` (status='idled' in DB)
- All operating REG plants confirmed at confidence=1.0

## Known limitations
- 99 closure suspects includes false positives (planned plants like LanzaJet
  Marquis Hennepin IL still loaded as "Operating", and tokenization gaps like
  "AltAir Paramount" vs "Alt Air Expansion")
- Some known facilities (AGP St. Joseph biodiesel, Cargill Iowa Falls) match
  weakly — need operator-aware matching or city/state filtering
- Tokenizer doesn't split fused names like "AltAir" → would need a US-cities
  dictionary to detect these splits

## Reusability — next external lists to add
1. ~~**EPA RFS RIN producer registry**~~ — SHIPPED in mig 077 (was already in `bronze.epa_pathway_detail`, 222 rows)
2. **EIA biofuel plants matrix** — capacity by state, with EIA Plant ID linkage
3. **Biodiesel Magazine annual survey** — industry consensus, often catches things first
4. **CARB's RNG producer list** — separate file, biogas pathways

## Operator alias dictionary (mig 078)
`silver.operator_alias` (~30 rows) — manual canonical↔alias mapping (REG↔Renewable
Energy Group, AGP↔Ag Processing, CVR↔CVR Energy, etc.). Used by
`silver.facility_tokens()` to expand both base tokens and alias-derived tokens.
Add entries as new operator-name mismatches surface.

## Known limitation: operator-level overmatch
When alias expansion adds tokens like "reg" / "energy" / "group", multiple
facilities of the SAME operator overlap on those tokens, inflating per-facility
pathway counts. Example: REG Albert Lea reports CARB=196 EPA=3 (the totals
across ALL REG facilities). The closure-suspect anomaly_class is still correct
(it counts ≥1 confident match), but pathway COUNTS are operator-level, not
facility-level.

Fix when needed: separate operator-tokens from facility-tokens in the schema,
and require ≥1 facility-token (city name) overlap for confidence 0.7+. Deferred
until per-facility pathway accuracy actually matters for a downstream consumer.

## Validation as of 2026-05-11 (after migs 073-078)
- 892 CARB pathways + 222 EPA RFS pathway determinations loaded
- 49 facilities `confirmed_active_both` (CARB + EPA both confirm)
- 70 `confirmed_carb_only` (CARB-listed, EPA parser miss or rare data)
- 12 `confirmed_epa_only_nonCA` (real producers not shipping to CA market)
- 55 `closure_suspect_no_signals` (highest-confidence closure candidates)
- 75 `weak_match_needs_review` (token overlap but ambiguous)
- 3 `expected_no_signals` (already marked idled/closed in DB)
- 249 `too_small_for_signals_expected` (small/specialty/test facilities)

Adding a new source = (a) new bronze loader, (b) add a UNION row to
silver.external_facility_norm, (c) extend gold.facility_status_anomalies
multi-source logic. ~1 hour each once first source done.

## Headline AtJ finding
All 82 current "Alternative Jet Fuel (AJF)" CARB pathways are HEFA-technology
(lipid feedstock). Top producers: AltAir Paramount (32), P66 (17), Montana
Renewables (13), REG Geismar (10), Neste (10). **Zero AtJ
(Alcohol-to-Jet) pathway certifications yet.** Real AtJ industry capacity
exists on paper but has not yet cleared the CARB-pathway threshold — this
is a real client-facing market-structure insight worth highlighting in
SAF/AtJ work.

---

## project_facility_weather_summary

*(`project_facility_weather_summary.md`)*

---
name: Per-facility weather summary for basis field
description: Each facility agent should receive a single-number daily growing-conditions indicator for its draw region, not raw weather data
type: project
originSessionId: 4f606138-9604-4e05-9a04-1c081995d736
---
When the basis-field work cycles into the detail phase, each facility agent should get a **one-number daily summary** of growing conditions in its draw region (50mi / 250mi / 500mi class). Inputs: temp/precip from `silver.weather_observation`, drought monitor by FIPS, Drew Lerner's regional commentary, possibly NDVI.

**Why:** the facility agent should not have to reason "what does the weather mean for my supply?" — it should consume a pre-computed indicator and act. Anchors the buyer/seller decision in operational state without re-doing meteorology per facility per day.

**How to apply:** when we get to the detail phase of the basis layer (post-Sprint-2), build a daily aggregator that takes the weather pipeline outputs + each facility's draw geometry and produces one row per (facility_id, date) with a growing-conditions index. USDA Crop Progress is a candidate alternative if their grid is fine enough — needs evaluation.

**Open questions for that phase:**
- USDA Crop Progress grid resolution — state-level may be too coarse for 50mi draw classes
- How to weight observed temp/precip vs forecast outlook vs drought severity vs Drew's commentary
- Single number vs vector (e.g. soil moisture, GDD pace, drought severity, outlook bias)
- Backfill: facility-level historical index needs the same per-day, per-facility reconstruction

This is **deferred** until basis-field detail work begins. Logged here so we know exactly where it plugs in.

---

## project_fats_greases_buildout

*(`project_fats_greases_buildout.md`)*

---
name: Fats & Greases Balance Sheet Buildout Sequence
description: Build order for fats/greases commodity balance sheets and upcoming feedstock allocation splits (UCO/YG, DCO/corn oil)
type: project
---

## Fats & Greases Balance Sheet Buildout (Apr 2026)

**Why:** Each fats/greases commodity needs a balance sheet before the feedstock allocation engine has accurate supply data. User is building these in Excel, then we wire them to the DB.

**Build sequence (user doing in Excel, morning of Apr 6):**
1. ~~Tallow complex~~ — DONE (edible + inedible + technical tabs, complex tab sums)
2. Poultry fat — IN PROGRESS
3. Yellow grease / UCO — NEXT
4. DCO / Corn oil — AFTER YG/UCO
5. Then: fuels side (biodiesel, renewable diesel, SAF, fossil mirrors)

### Upcoming Allocation Splits (same pattern as tallow EBFT/IBFT)

**UCO vs Yellow Grease:**
- UCO is a subset of yellow grease (like edible/inedible tallow)
- Separate UCO balance sheet with own trade data
- Need to figure out what drives buyers to choose UCO vs YG as BBD feedstock
- Likely drivers: CI score advantage (UCO has lower CI), traceability requirements, LCFS/45Z credit differential
- Will need `UCO`/`YG` split in allocation engine, same EIA guardrail pattern

**DCO vs Corn Oil:**
- DCO is a subset of corn oil (produced at ethanol plants)
- Separate DCO balance sheet
- Corn oil balance sheet is the parent

### Files
- Tallow complex: `RLC-Models/Fats and Greases/new_models/us_tallow_complex_balance.xlsx`
- Individual: `us_edible_tallow_balance.xlsx`, `us_inedible_tallow_balance.xlsx`
- After balance sheets: move to fuel-side value chain (all fuels + fossil mirrors)

**How to apply:** When user finishes each balance sheet, extract supply/demand data into `silver.feedstock_supply` to replace estimated supply figures in the allocator.

---

## project_feedstock_forecast_method

*(`project_feedstock_forecast_method.md`)*

---
name: project-feedstock-forecast-method
description: "Ruled method (2026-07-09) for the FFA feedstock consumption forecast: fuel-production × yield × 12-mo trailing avg mix to 2046, + the non-bio industry split. Actuals run through the latest EIA feedstock month."
metadata: 
  node_type: memory
  type: project
  originSessionId: 4c118e7a-88d8-4867-be0e-3d804963463b
---

Tore's ruling (2026-07-09) on how the FFA allocator produces feedstock consumption forecasts,
and the actuals/forecast split. See [[project-ffa-feedstock-layer]] and
[[data-reconciliation-hierarchy-eia-census-canon]].

## Actuals regime
Run + rake the allocator through **the latest month EIA feedstock data exists** — now April 2026
via the EIA v2 API `petroleum/pnp/feedbiofuel` collector (commit fa3b5ab7; replaced the stale
table2.xlsx that froze fats at 2020). "Everything should run and complete through April 2026."
Operational cadence: re-run the allocator **one month at a time** as each month's data lands
(`--period YYYY-MM`), not full-history re-runs.

## Forecast regime (beyond actuals → out to 2046, 20-yr horizon)
Feedstock consumption forecast = **monthly fuel volume production predictions × observed yields ×
average US feedstock mix**. For now the mix = **12-month trailing average US (national) feedstock
mix**. Sanity gate: unless the resulting mix is wildly out of line with recent history (which by
construction it won't be), it's good. National-level now; **eventually build up from per-facility
mix** (better, facility-detail forecasts) — that's the endpoint, trailing-avg is the starting point.

## Non-biofuel split (Q4 ruling)
1. Take the non-bio usage data in the **Census Crush tab of `us_oilseed_crush.xlsm`**, work out the
   **portion (share) of usage that went to each industry**.
2. Apply those industry shares to the volumes **USDA reports as "removed for processing"** (the NASS
   REMOVAL FOR PROCESSING attributes, in `nass_low_ci_matrix.*_processing_use` / monthly_realized)
   where available.
3. Where USDA removal-for-processing isn't available for a feedstock, **assume the portioning
   survives over time** — domestic non-bio users keep consuming in similar volumes **relative to
   available supply**.
4. Endpoint (not now): per-industry **economics → financial models** that guide the split, rolled
   up into the forecast. For now, steps 1-3 + 12-mo trailing mix are "just fine to get us started."

## Flat-file deliverable per feedstock (from Fats & Oils / us_oilseed_crush, else add to flat files)
biofuel allocation **by fuel** (BD/RD/SAF/coproc) + **non-bio split by industry category** +
**end-of-month stocks**. Fats/greases: EIA gives total-only (no BD/RD split) — the per-fuel split
for fats is the allocator's, not EIA's.

## Chain recovery + hardening (2026-07-11/12)
Runbook `docs/runbooks/feedstock_allocation_runbook.md` is the authority (lessons L1-L10). Two
pre-existing gaps found + closed while recovering from the 7/9 half-fix:
- **UCO**: canonical `silver.uco_yg_balance` stops Dec 2024; `wire_uco` was net-destructive past it
  (stripped EIA Yellow Grease, wrote no UCO for 2025-26 → UCO absent from allocator all year). Fix:
  `wire_uco` now bridges from EIA Yellow Grease (`source='EIA_YG_BRIDGE'`) past the canonical
  frontier; reversible, auto-retires as canonical build extends (L8).
- **Tallow**: `bronze.nass_livestock_slaughter` was an ORPHAN one-off frozen at Feb 2026 → capped
  SLAUGHTER_DERIVED tallow → allocator zero-allocated tallow past Feb. Built + **registered**
  `scripts/collect_nass_livestock_slaughter.py` as `nass_livestock_slaughter` (dispatcher, daily
  days 20-31 15:05 ET, idempotent). Frontier now tracks NASS (L9).
- **Gate**: `scripts/validate_feedstock_gate.py` replaces the old inline ±5% gate that wrongly
  included EXEMPT_RLC tallow in the EIA total (L10). Checks rake-controlled feedstocks vs EIA +
  presence; reports canonical divergence separately.
- **Tallow RLC/EIA ~50-73% is RULED, not open** (tallow_ruling_doc §2/§6 swap hypothesis: EIA
  over-counts tallow / under-counts UCO, waste-oil booked as tallow). RLC tallow below EIA is the
  intended correction — report the residual, do NOT reopen it. See
  [[data-reconciliation-hierarchy-eia-census-canon]] RLC_CANONICAL override.
- Open follow-up: the EIA-YG UCO bridge inherits EIA's UCO under-count (doesn't add back the
  misbooked-tallow pool); true 2025-26 UCO awaits the canonical UCO-resolution extension past 2024.

---

## project_feedstock_forward_projections

*(`project_feedstock_forward_projections.md`)*

---
name: 20-year-forward-feedstock-projections
description: End goal of the feedstock allocator history work — project feedstock usage by commodity × fuel category 20 years forward for due-diligence-grade reports.
metadata: 
  node_type: memory
  type: project
  originSessionId: 05e2ef6d-523f-437f-8858-9be18d5d0aeb
---

After the historical feedstock-allocator backfill finishes, Tore wants the next deliverable to be a **20-year forward projection of feedstock usage, broken down by commodity (SBO, CO, DCO, EBFT, IBFT, CWG, YG, UCO, etc.) and fuel category (biodiesel / renewable diesel / SAF / coprocessing)**. The audience is due-diligence consumers (lenders, equity investors evaluating biofuel facility builds, project finance).

**Why:** Tore noted "amazingly what people want for due diligence." The deliverable is a long-horizon (~2046) projection with decade-level credibility. The model rationality is what gets scrutinized, not the precision of any single 2042 datapoint.

**Architectural path:**

1. Feedstock projection = Fuel production projection × allocator
2. `silver.fuel_production_forecast` currently has rows 2001-2030. Needs extension to ~2046 (20 years out).
3. Allocator already takes the production forecast and distributes feedstock per facility — that machinery is in place.
4. What's missing: policy/scenario assumptions for the back half of the horizon.

**How to apply:** When this work starts, structure it as scenario-based, not point-forecast:
- **Decade-1 (2026-2035):** higher confidence, anchored on permits/capacity-in-flight + announced builds.
- **Decade-2 (2036-2046):** wider distribution, scenario-driven (RFS RVO trajectory, 45Z extension/expiration, LCFS continued tightening, ReFuelEU phase-in, EV displacement of biodiesel/RD in road diesel, SAF mandate adoption rates).
- **Three scenarios minimum:** baseline, high (favorable policy + slower EV), low (rapid EV displacement + 45Z expiration + RFS plateau).

**Honest read on the deliverable:** 20-year biofuel projections are aspirational — biofuel demand in 2046 depends on EV adoption curves, aviation policy, food-vs-fuel discourse, and 2G feedstock economics, all wide-distribution uncertainties. Present the projection as a structured argument with explicit assumptions, not as a forecast.

Related: [[project_symbiotic_forecasting]] (the broader LLM-forecasts-everything endpoint), [[bbd_balance_sheet_model]] in KG (the structural model).

---

## project_ffa_feedstock_layer

*(`project_ffa_feedstock_layer.md`)*

---
name: project-ffa-feedstock-layer
description: "FFA per-facility feedstock architecture — three-tier eligibility + assumed-mix, tables, feedstock code vocab, eligibility-vs-mix distinction."
metadata: 
  node_type: memory
  type: project
  originSessionId: a1006a1d-2b47-4348-924e-495b828e19fb
---

The Feedstock Facility Agent (M2) feedstock layer, built 2026-06-28. Turns site-level data
into per-facility feedstock consumption. Two distinct concepts — keep them separate:

**ELIGIBILITY (what a plant CAN run)** — `reference.biofuel_facilities.eligible_feedstocks`
(array of codes). Three-tier builder `scripts/build_facility_feedstock_eligibility.py`:
- TIER0 curated (Jake-era) — but CARB OVERRIDES on conflict (Tore always defers to CARB).
- TIER1 pathway — CARB name-match (state-gated to kill cross-state false matches).
- TIER2 generic — BD→assumed soy primary (eligible set kept broad), HEFA/coproc→tech default.
- NON_LIPID (FT/pyrolysis/ATJ/ferm) excluded. ~9 ambiguous held for Tore review.

**MIX (what a plant actually RUNS, %)** — `reference.facility_assumed_mix`
(facility_id, feedstock_code, pct, source). The CONSUMPTION prior, below CARB in hierarchy:
CARB=allowed, mix=run, allocator economics adjusts. Source `rd_buildup_xlsx` parsed from
`models/Biofuels/RD Feedstock Build Up.xlsx` by `scripts/parse_rd_feedstock_buildup.py`
(23 facilities). Tore's 5-real-plant intel reconciles here.

**CRITICAL distinction:** soy-ELIGIBLE != soy-USED. DGD's CARB set includes SBO so it reads
"soy-eligible", but it runs ~mostly UCO/tallow (mix shows YG23/CWG10/DCO33/UCO33). Always use
the MIX table for consumption; eligibility is only the capability boundary.

**Feedstock code vocab (master):** SBO soybean oil | CO corn oil | DCO distillers corn oil |
CAN canola | UCO used cooking oil | BFT beef tallow (CATTLE) | CWG choice white grease (PORK) |
PLT poultry fat | YG yellow grease | CAM camelina | CAR carinata | FSH fish oil | LCI
unspecified low-CI placeholder. Fats are NOT interchangeable; use real commodity names, no
"advanced" bucket (camelina being commercialized). See [[reference-carb-pathway-selection-bias]].

**Facility verification tour (agreed 2026-06-28, do AFTER feedstock-mix work):** web-research
each facility like the World Energy Paramount / Seaboard lookups to confirm we have everyone +
correct capacity/mix/status. Tore's screenshots → e.g. Seaboard = 3 plants (KS Hugoton RD 85,
OK Guymon BD 46 pork-fat, MO St.Joseph BD 32 DCO) + Madera CA TERMINAL (not a producer) + HQ.
Master-quality gotchas the tour must catch: duplicate records (same plant w/ & w/o " - City"
suffix), terminals/HQ mislabeled as plants, warm-idle status (e.g. WE Paramount idled after Air
Products exit — exclude from current consumption). gold.feedstock_allocation FK blocks hard-
delete of some dups → mark status='merged' instead. Screenshots in Dropbox …/ScreenShots/.

**Open items:** wire eligibility+mix into allocator.py pathway gate (currently stubbed
pathway='hefa'); `--write` eligibility after Tore reviews 9 ambiguous; BP Cherry Point
Stage1/2 collapse (master lacks Stage2 row); reconcile 5 real plants (P66/Vertex/Calumet/
SAFuelsX/Kern) + Prince George. Facility master cleaned to 239 rows
(`scripts/clean_facility_master.py`). Top-down PADD soy balance
(`scripts/padd_soyoil_balance.py`) = validation target for the bottom-up allocator.

**2026-07-01 session (calibration + big-RD scrub, commit ba42e34b):** worked the calibration
queue + scrubbed the operating denominator. Facility batch idled the 2024-25 shakeout zombies
(REG Ralston/Madison idled, Western Dubuque paused Dec'24, WHOLE Hero BX fleet in receivership),
merged 3 dups, wrote WIE (15% soy) + Agron (tallow-primary) live mixes. Scrub batch: 11
international plants (Neste Porvoo/Rotterdam etc.) -> NON-US; BP Cherry Point 2020->110 MMgy
(phantom crude throughput); Grön/HOBO/Emerald/Fulcrum/Red Rock/Ryze -> not-operating; Texmark ->
fractionation (non-lipid SAF processor, excluded via new rollup tech filter). Coverage 26%->36%.
**KEY FINDING: soy overshoot (+4.84B lb) is RD-SIDE, not BD.** BD_DEFAULT=90% soy is CORRECT
(matches EIA plant_type='biodiesel' veg mix 90.4/5.7/3.8 — was a no-op). Our RD soy ~9B vs EIA
RD veg-soy ~4.9B. **ROOT CAUSE: `bronze.eia_feedstock_monthly` has fats/greases ONLY in
plant_type='total', not split BD vs RD** — so RD default can't be made fats-inclusive, and RD
(really mostly tallow/UCO) defaults to too much soy; canola stuck at -2.75 (rd_default has none).
FIXED (commit after ba42e34b): EIA Form 819 splits ONLY veg oils by plant type (Table 2c); fats
(Table 2b) are total-only -> fats-by-plant-type is UNPUBLISHABLE, so DON'T chase a collector fix.
Instead `derive_defaults()` in national_feedstock_consumption.py reconstructs it: veg from plant_type
split + fats apportioned (BD_fats = BD_prod*yield - BD_veg; RD_fats = total_fats - BD_fats). RD
default -> BFT38/UCO24/SBO22. Result: soy overshoot +4.84 -> +2.51; tallow/YG now reconcile.
Calibrated the big RD plants (Montana/Dakota/BP/Bakersfield/Martinez/Geismar/Rodeo/St Bernard/
New Rise/ReadiFuels), de-duped DGD (61->305, 60->410), idled CVR Wynnewood + Valley Green(planned),
tagged 5 non-lipid (LanzaJet/Gevo/Velocys/NW Advanced/Strategic). Scripts:
apply_ffa_rd_calibration_20260701.py + apply_ffa_scrub_batch_20260701.py. Coverage 26%->63%.
**REDACTION FIX (key):** EIA redacts canola (8/12mo) & corn oil (7/12mo) by plant_type -> the
'renewable_diesel' column undercounts both; derive_defaults now uses RD veg = total - biodiesel.
Corn oil reconciled (-0.18). **Final: soy +5.40 -> +2.66; tallow/YG/corn oil all reconcile.**
**REMAINING = CANOLA -2.01, STRUCTURAL (ANALYST QUESTION for Tore, not calibratable):** EIA 'total'
canola = 3.21B lb but domestic plant mixes can't reach it (known canola plants small; big plants
run ~0 canola). Either west-coast plants run more Canadian canola than assigned, OR EIA canola
includes imported canola oil / scope our production-anchored model can't see. Soy over ~= canola
under (substitution). Lesson learned: covering soy-heavy plants (HF Sinclair etc.) pushes soy UP -
STOP facility calibration; the residual is structural. Deferred: PADD assignment (~35 US NULL-padd,
'?' row only); slight total overstatement (35.1 vs 34.3, possible RD yield 8.6 high).

**2026-07-04 SYSTEM AUDIT — the canola "mystery" RESOLVED, plus a plot twist. Design LOCKED at
`docs/specs/bbd_feedstock_system_design_v1_6_LOCKED.md`.** Two parallel consumption paths existed:
the economic allocator (`gold.feedstock_allocation`, margin-ranked per-facility, SHIPPED to Notion/
dashboard) vs the mix-rollup (`national_feedstock_consumption.py`, print-only — what all the above
calibration improved). **Re-ran the allocator on the cleaned master: it reconciles to EIA FAR better
than the rollup — canola −0.04 (vs rollup −2.01), soy +0.66 (vs +2.66).** So the calibration session
improved the WEAKER method; the canola gap was a ROLLUP ARTIFACT, not structural/real — the allocator
places import-fed West Coast canola correctly. **Staleness, not method, was the core defect** (allocator
was stale on the dirty master). Design: allocator=canonical constrained-disaggregator raked to EIA;
rollup=validation layer; reuse wheat flat-file contract; effective-dated capacity/status histories;
run-registry+freshness anti-staleness. H2 = allocator allocates ZERO UCO (imported-UCO gap → Phase 2).
**VOCAB COLLISION found:** `CO`=Corn Oil (mix) vs Canola (allocator) — silent-join landmine; fixed by
`reference.feedstock_codes` (50-code registry) + `feedstock_code_xref` (migration 140, migration-not-
translation, CO retired). Open Tore rulings: LCI retire+backfill (2 HF Sinclair plants); interim-promote
the good allocator re-run to un-stale Notion. Next build: rest of Phase 1b (histories, rake, reconciliation,
detection, compat view). See [[project-wheat-country-build]] for the reused flat-file contract.

**2026-07-04 (later) — Phase 1b executed + UCO project initiated.** (1) **Effective-dated histories**: reused
pre-existing empty `reference.facility_capacity_history` (effective_date change-log), seeded from master +
SHAKEOUT dates (month-precision; year_online-missing → source='estimated'); rewired `allocator.load_facilities`
as-of-period (mig 141, `scripts/seed_facility_histories.py`). (2) **Deep backfill** 2010-07→2025-09 (183mo/109
fac); `scripts/verify_backfill_acceptance.py` ALL PASS. (3) **RAKE (Layer D)** `scripts/rake_feedstock_to_eia.py`
→ `gold.bbd_feedstock_raked`, national EIA-exact (factors 1.03-1.24; 1.9% fac-mo >cap). **National monthly
feedstock history forecast-ready at EIA resolution.** (4) **UCO RESOLUTION project** (brief
`docs/specs/uco_resolution_desktop_brief.md`): gap = `silver.feedstock_supply` (EIA Form 819) has NO UCO (EIA
folds it in "Yellow Grease"). Fix = within-YG split (UCO_biofuel + YG_other = EIA YG, rake still pins total).
**MULTI-COUNTRY**: collection = consumer-strength proxy — US=food-spending (`ers_food_sales_monthly` FAH/FAFH
1997-2024), others=per-cap-GDP×population (Tore's macro workbook `RLC Dropbox/.../Models/Macro/World Macro
Economic and Population Data.xlsx` w/ OECD projections; Canada/Mexico template → scale to China+ via OECD/WB).
Import leg = exporters' (China) collection surplus; Census HS 1518.00.40 = validation anchor (raw magnitude
inflated). OECD projections forecast to 2050 natively. Tore rules: non-bio use ≈0 SAME methodology
history+forecast; proxy-now/biotracker-later via vintage ladder. Code=plumbing, Desktop=methodology+contract.
Awaiting Desktop's rulings (k/intensity calibration, import HS scope, EIA-YG reconciliation, vintage ranks).

**2026-07-06 (later) — TALLOW build + THE SWAP RESULT (major).** Desktop ruled tallow methodology
(`tallow_ruling_doc_and_contract.md` + `tallow_ruling_addendum_A_cir_mapping.md`): slaughter-driven,
live-weight-keyed, RLC-canonical (Ruling 1, EIA disregarded). **CIR M311K data found in Tore's
`World Crush and Other Stuff.xlsx` → `Census Crush` tab** (cols 55-70 fats production/stocks + 229-245
inedible-T&G consumption block, 1979-2011) → `bronze.census_cir_fats`. Built `silver.animal_slaughter`
(cattle head+live-wt 1944-2026, 34.2M→31.8M head) + `silver.tallow_production` (EBFT/IBFT; CIR-measured
rank80 1979-2011 + SLAUGHTER_DERIVED rank60 yield 12.70%×live-wt; MAXIFS no seam: 2008=5.34 measured →
2024=5.64 derived). **THE SWAP (airtight):** EIA claims 8.65B tallow BIOFUEL but total tallow SUPPLY
2024 = production 5.64 + net imports 1.58 (HS 1502) = 7.22B → EIA exceeds ALL available tallow by 1.43B
(impossible, zero non-bio assumed) ≈ RLC UCO under-count 1.34B. Near 1:1 = imported waste-oil misbooked
as tallow. **BUDGET CLOSES:** UCO 8.73 + tallow-bio ~5.8 + EIA veg 19.46 = ~34.0B (0.5% residual).
Config: YIELD_PCT[cattle]=0.1270, EDIBLE_SHARE=0.335. Addendum A: non-bio segments (233 fatty→oleo,
239+240+245→other-inedible, 234 feed=elastic w/ col235 measured baseline; 241-244 ME=biofuel EXCLUDED);
A4 two-chapter feed (BSE 2004-10 regulatory THEN RD 2021- economic — don't attribute BSE decline to
biofuel); A5 EBFT via disappearance identity; A7 anchor SHARES not totals (CIR covers ~55-65% of prod).
REMAINING A9: segmentation shares (2007-10) + feed trend + tallow trade (HS 1502+curated) + EBFT identity
→ tallow_biofuel → WAIT for UCO → ONE allocator re-run (rake exempts UCO+tallow) → flat files. Swap
write-up for Feedstock Report = Desktop.

**★ RESUME POINT (paused 2026-07-06, analytical win banked; run integration fresh next session).**
KEY UPGRADE: use REAL NASS Fats&Oils production 2015+ (`silver.animal_fat_production`, rank 90) NOT
CIR-extrapolated yield — real tallow 4.91B (<extrap 5.64). SUPPLY-FOR-BIOFUEL resolved for whole complex:
UCO 8.73B (RLC-canonical), Tallow 6.01B available/~5B biofuel (RLC), CWG ~1.4B (RLC, prod 1.25+net imp),
Poultry SMALL/≈EIA (net EXPORTER HS1501.90, mostly pet-food/feed — Tore's pushback confirmed, do NOT
RLC-canonical it), veg soy/canola/corn EIA-pinned. BUDGET CLOSES ~34B. Tallow non-bio trend from CIR =
9.9% of production (fatty acids 233 + other-inedible 245, allocated by ibft_share col59/col64=0.556).
THREE INTEGRATION STEPS to close balance sheets: (1) formalize non-bio→biofuel silver tables (tallow_balance
etc.); (2) wire RLC supply constraints into allocator + EXEMPT UCO/tallow/CWG from EIA rake (veg stays pinned);
(3) single allocator re-run (~45min) → per-facility biofuel use → re-run `verify_backfill_acceptance.py` →
flat files → Desktop wires balance-sheet workbooks. QUEUED: KG DB insertion of `docs/kg_batch_epa_rfs_ria.md`
(8 nodes via core.kg_node/kg_context/kg_edge). Poultry BBD share still to cross-check vs RD-facility disclosures.

**2026-07-06 — UCO plumbing built + RULING 1 (EIA disregarded for UCO/tallow) + tallow project.** Built:
`silver.food_expenditure` (FAH/FAFH proxy), `bronze.country_macro` (World Bank GDP/percap/pop, 20 countries
incl China, retry for 502s), `silver.uco_imports` (Census HS 1518.00.40, bloc-aggregation trap handled),
`silver.uco_yg_balance` (§2 identity). Desktop ruled UCO methodology (`UCO_ruling_doc_and_contract.md`):
FAFH-real proxy, k_uco=Fastmarkets 3.3B/2024 [INTERNAL_ONLY licensed], k_yg from LMC 6.5B/2022 combined; policy
module `bbd_policy_register.xlsx`; flat file `us_uco_supply.xlsx`. **RULING 1 (Tore 2026-07-06, BIG):** EIA
under-captures UCO/tallow → **RLC supply build is CANONICAL, EIA disregarded (not a rake cap)** — must be able
to explain divergence "to a room of 300". UCO_biofuel = collection + net_imports (no EIA cap). **UCO 2024 =
8.73B lb, exceeds EIA's whole Yellow Grease bucket (7.39B) by 1.34B.** **BUDGET INSIGHT:** RLC UCO(8.73) + EIA
veg(19.46) + EIA tallow(8.65) = 36.8B > 34B actual → EIA under-counts UCO AND over-counts tallow; RLC corrects
both (tallow lands <8.65). **AUTHORITATIVE TRADE SOURCE found:** Tore's `RLC Dropbox/.../Models/Oilseeds/US
Oilseed Trade.xlsx` has curated UCO/YG/Edible+Inedible Tallow/DCO/CWG/Lard/Poultry Fat import+export sheets,
HS 1518.00.40 confirmed, historical UCO/YG separation to 1993/94. **TALLOW project initiated** (brief
`docs/specs/tallow_resolution_desktop_brief.md`): slaughter-driven (cattle head×fat-yield, `nass_livestock_slaughter`
head+weight 1907-2026 — hard number not proxy), extends to lard(hogs)/poultry-fat. Non-bio is LARGE for tallow
(feed/oleo/soap/pet food) = the lever pulling RLC tallow <EIA. Rake exempts UCO+tallow (RLC-canonical), veg oils
stay EIA-pinned. Both RLC-built before ONE allocator re-run. Awaiting Desktop tallow §4 rulings.

**2026-07-07 — TALLOW WIRED INTO ALLOCATOR (gated) + Addendum B/B.1.** Built `silver.tallow_balance`
(§4/Addendum-A): production ladder NASS90>CIR80>SLAUGHTER60 (CY2024=4.91B), trade by grade (Census
HS1502 .10.0020->EBFT, .10.0040+.90->IBFT, grand-total dedup; CY2024 net imports 1.58B), CIR-era
non-bio segments. Derived guardrail `tallow_biofuel_use` per Addendum B/B.1: R1 oleo+other = share ×
T12M **slaughter-derived** IBFT (one-vintage-per-estimator; fit 2008-2010=0.1698 — 2007 dropped as
fatty-acid CIR-detail ramp outlier, RATIFIED Addendum B.2; cap 0.686B same window), R2 feed floor 200M glide, R3
production-anchored (imports->biofuel), R4 EBFT-biofuel=0. **CY2024 biofuel-available=4.62B.** WIRED:
allocator.py run_tallow_split prefers `load_rlc_tallow_guardrail` (silver.tallow_balance), EIA fallback
pre-2013; rake_feedstock_to_eia.py RLC_CANONICAL={EBFT,IBFT,BFT} exempt (NOT UCO yet; CWG stays raked
= treat like PF per Tore, see [[reference-high-ffa-feedstock-biofuel-limit]]). populate feedstock_supply
w/ tallow = REDUNDANT (allocator purges BFT + overwrites EBFT/IBFT from split). **ALLOCATOR RE-RUN HELD
for UCO** (Ruling §5) — code wired, gated. **EIA "8.65B" RECONCILED (Ruling 2): it is `T12M_2025-09`,
NOT CY2024.** Live EIA tallow (plant_type=total, Form 819): CY2023=5.70, CY2024=7.27, T12M_2025-09=8.65.
ALWAYS window-label. Swap: CY2024 EIA 7.27 > gross physical supply 6.49 by 0.78B (impossible@zero-nonbio);
T12M_2025-09 EIA 8.65 > gross 6.80 by 1.85B (widening). CY2023 EIA 5.70 ~= supply (dates onset to 2024+).
Relabeling exhibit `silver.eia_tallow_yg_composition`: tallow +1.6/yr, YG -2.2/yr, combined stable
14.66->14.1, 0 withheld (real, not suppression). Docs: tallow_addendum_b*, tallow_addendum_b1_code_response.md.
NOTE: .mcp.json holds a live Notion token in a TRACKED file — gitignore it.

---

## project_forecast_comparison

*(`project_forecast_comparison.md`)*

---
name: project_forecast_comparison
description: Forecast comparison layer - LLM vs human vs USDA projections, reconciliation hierarchy
type: project
---

**Forecast comparison framework:**
Add a projection layer comparing three (or more) sources:
1. **LLM (Claude)** — model-generated S&D projections
2. **Human (user's spreadsheets)** — independent analyst projections
3. **USDA** — official government estimates
4. **Others** — MPOB, StatsCan, CONAB, local sources as applicable

**Reconciliation hierarchy (what we benchmark to):**
- General rule: Local government source → local trade association → USDA
- **Exception commodities** (US): corn oil, UCO/yellow grease, tallow — no authoritative USDA source, so we maintain our own S&D estimates via the biotracker/rail car tracking
- **Non-US**: MPOB for Malaysia palm, StatsCan for Canada, CONAB for Brazil, etc.

**Why:** The value is in the DIFFERENCES between forecasts. Understanding why the LLM, the analyst, and USDA diverge on a specific commodity illuminates the assumptions each is making.
**How to apply:** Build gold views that store projections by source, commodity, MY, and attribute. Dashboard should show all three side-by-side with variance highlighting.

**Related projects:**
- Biotracker — near real-time feedstock supply tracking (rail cars, etc.)
- Rail car tracking — physical flow monitoring for commodities with no authoritative public data source

---

## project_forecast_philosophy

*(`project_forecast_philosophy.md`)*

---
name: forecast-philosophy-rationality-over-accuracy
description: "For client forecasts (especially long-horizon), the argument and assumption transparency matter more than point-accuracy. Accuracy is \"almost a trivial feature.\""
metadata: 
  node_type: memory
  type: project
  originSessionId: 05e2ef6d-523f-437f-8858-9be18d5d0aeb
---

Tore's working philosophy on client forecasts — particularly long-horizon
forecasts like the 20-year feedstock projection:

> "It does not really matter if they are correct, as long as we explain our
> thinking in a way that allows clients to come to their own conclusions
> and ask us questions about our assumptions. If we get to that point, the
> accuracy is almost a trivial feature. Of course we would prefer more
> accurate over less, but as long as we tell clients why we think what we
> think, being correct is almost secondary."

**Why this matters:** Tore notes that "a 10-year forecast (5-year even) is
not worth the paper it is printed on" — but clients ask for them anyway,
especially for due diligence. The deliverable that wins isn't a point
forecast — it's:

1. **A structured argument** with explicit assumptions laid out.
2. **Transparent reasoning** the client can interrogate.
3. **Question-friendly format** — clients should be able to challenge any
   assumption and see how the forecast moves.
4. **Honest limits** — the longer the horizon, the wider the distribution.
   Decade-1 tighter than decade-2 in any 20-year work.

This is also a positioning argument: "our forecasts, even if more accurate
than any other, have limits on their usefulness." Being honest about the
limits is itself a credibility signal.

**How to apply:** When building any client-facing forecast deliverable:
- Lead with the assumption ledger, not the number.
- Make assumptions toggle-able where possible (scenario A/B/C, sensitivity
  tables).
- Reserve the right confidence interval — don't manufacture precision the
  data doesn't support.
- Write the narrative so a client could rebuild the conclusion from the
  assumptions even without our model.

Related: [[project_feedstock_forward_projections]] is the immediate
instance. [[feedback_honest_pushback]] is the same principle applied to
internal collaboration — don't manufacture confidence to seem useful.

---

## project_fuel_flat_files

*(`project_fuel_flat_files.md`)*

---
name: Fuel Flat Files — Build + Data Source Inventory
description: Built us_fuel_production_stocks.xlsx + us_fuel_trade.xlsx (Apr 2026). Records legacy bootstrap sources, missing EIA series, missing Census HS codes, and what still needs collector work.
type: project
originSessionId: 7f1aa8dc-9a7d-496a-a664-a22a410a36c8
---
## Files built (Apr 2026)

Location: `C:/Users/torem/RLC Dropbox/RLC Team Folder/RLC-Models/Biofuels/new_models/`

1. **`us_fuel_production_stocks.xlsx`** — CY-oriented monthly flat file.
   - 12 tabs: README, Biodiesel, Renewable Diesel, SAF, Co-Processing, Ethanol - Monthly, Ethanol - Weekly, Distillate Fuel Oil, Motor Gasoline, Jet Fuel, Residual Fuel Oil, Natural Gas
   - Monthly rows Jan 1973 – Dec 2057 (long template, historical fills what's available)
   - **Populated**: BD 283 months (2001-2024), RD ~163 months (2011-2024), Ethanol-Monthly 540 months (1981-2025), Ethanol-Weekly 544 weeks (2010-06 to 2020-10, STALE), Distillate 620 months (1973-2024), Motor Gasoline "Product Supplied" only 540 months (1981-2025 from ethanol bal).
   - **Empty shells** (collector extension needed): SAF, Co-Processing, Motor Gasoline prod/stocks, Jet Fuel, Residual Fuel Oil, Natural Gas

2. **`us_fuel_trade.xlsx`** — CY accumulator + country × monthly layout, matches `us_soy_complex_trade.xlsm` pattern exactly.
   - 19 sheets: README + 18 fuel trade tabs (imports/exports for BD, RD, SAF, Ethanol, Diesel, Gasoline, Jet Fuel, Residual Fuel Oil, Natural Gas)
   - Layout: col A country; cols B-AJ = CY 1993-2027 accumulators with `=IFERROR(SUM(monthly_12)/1000,0)`; cols AQ+ = monthly Jan 1993 – Dec 2040 (576 months)
   - Countries: ~76 trading partners + section headers + TOTAL/EU-27 aggregates (rows 5-82)
   - **Populated**: Biodiesel Imports 2557 values from `World BBD Trade.xlsx` (1994-Feb 2020, ~68 countries matched). Biodiesel Exports sparse (~129 values). Everything else EMPTY.
   - **Why:** `bronze.census_trade` has ZERO fuel HS codes (chapters 22/27/38 missing). Collector is ag-only.

**Build script:** `C:/dev/RLC-Agent/scripts/build_fuel_flat_files.py` (idempotent — safe to re-run as source data refreshes).

**Why:** User asked for flat files mirroring the oilseed/fats/greases trade file pattern, adapted for fuels on CY basis. See [project_calendar_year_vs_marketing_year.md](project_calendar_year_vs_marketing_year.md).

## Legacy bootstrap sources (all Dropbox)

| Fuel | Source file | Tab | Range | Notes |
|---|---|---|---|---|
| **Ethanol monthly** | `Feed Grains/US Ethanol Balance Sheet.xlsx` | `Monthly Ethanol Data` | 1981-01 to 2025-12 | EIA source keys in row 5: M_EPOOXE_YOP_NUS_1, MFEIMUS1, MFESTUS1, etc. |
| **Ethanol weekly** | same file | `Weekly Ethanol Data` | 2010-06-04 to 2020-10-30 | **STALE — 5+ yrs behind**. Source keys: W_EPOOXE_YOP_NUS_MBBLD, W_EPOOXE_SAE_NUS_MBBL |
| **Biodiesel monthly** | `Biofuels/US Fuel Balance Sheets.xlsx` | `monthly_data` cols 5-15 | 2001-01 to 2024-07 | Raw EIA Monthly Energy Review drop |
| **Renewable Diesel monthly** | same file | `monthly_data` cols 29-39 | 2011-01 to 2024-07 | No exports column in legacy |
| **Distillate monthly** | same file | `monthly_data` cols 77-90 | **1973-01 to 2024-08** | Longest series — URLs in header for each EIA series |
| **Biodiesel trade (monthly, by country)** | `Biofuels/World BBD Trade.xlsx` | `US Biodiesel Imports` / `US Biodiesel Exports` | 1994-01 to 2020-02 | Only legacy source with true monthly fuel trade |
| **SAF / Jet Fuel annual S&D** | `Biofuels/US BBD and Fuel Balance Sheets - use this version...xlsx` | `SAF`, `Jet Fuel` | annual 2018-2026 | Not monthly — reference only |
| **Feedstock inputs (BD/RD)** | `Biofuels/new_models/eia_data.xlsm` | `biodiesel_monthly`, `renewable_diesel_monthly` | Oct 1998+ (BD), empty (RD) | Cleaner schema but RD/SAF/co-proc sheets empty |

**Useless legacy files (do not mine):**
- `Biofuels/US BBD and Fuel Trade.xlsx` — SAF/Jet tabs have TOTAL-row only, no by-country detail. RD/Diesel tabs empty.
- `Oilseeds/US Calendar Year Fuel and Feedstock Balance Sheets.xlsx` — feedstock-centric not fuel-centric, has `#REF!` errors, no CY↔MY conversion logic despite the name.

## UPDATE 2026-06-14 — fuel trade IS ingested + scheduled (supersedes "zero fuel codes" below)

`bronze.census_trade` now carries fuel HS codes (2207 ethanol, 3826 BD/RD,
2710.x diesel/jet/gasoline/RD, 2711 NG/LNG) from 2013 → current. 52 active
fuel code/flow rows in `silver.trade_commodity_reference`; the scheduled
`census_trade` collector auto-loads them via `_load_hs_codes_from_db()` and
fetches a rolling 17-month window with idempotent upsert — fuel rides the
same Census collector as ag (no separate fuel scheduler; correct).

Consumer view for the **Ctrl+Y** `us_fuel_trade.xlsm` updater =
`gold.trade_export_mapped`: emits FUEL_ETHANOL, BIODIESEL + RENEWABLE_DIESEL
(BD/RD split applied per [[project_bd_rd_trade_split]]), DIESEL, JET_FUEL.
SAF is separate: `gold.saf_trade_candidates` ([[project_saf_trade_tracking]]).

**Census release mechanics (confirmed by Tore 2026-06-14):** FT-900 exports
post 8:30 AM ET; imports + state data by noon ET. So the scheduled run MUST
be after noon. Fixed in mig-free commit 0fce883d: `census_trade` release_time
10 AM → **12:30 PM ET**, and 2026 release dates corrected to the official
Census calendar (the old table's Jun 4 vs real Jun 9 caused the April-data
miss — run fired before the data existed). Official 2026 dates: Apr data Jun 9,
May Jul 7, Jun Aug 4, Jul Sep 3, Aug Oct 6, Sep Nov 4, Oct Dec 8.

## Bronze state (as of Apr 13, 2026)

`bronze.eia_*` tables actually present on RDS:
- `eia_capacity_monthly` (63 rows, Jan 2024 - Sep 2025) — from xlsx (table1)
- `eia_feedstock_monthly` (546 rows, Jan 2024 - Sep 2025) — from xlsx (table2)
- `eia_raw_ingestion` (~600 rows, weekly petroleum series, last few months only)
- `eia_monthly_biofuels` (5,088 rows, **BD/RD/SAF/Other/Ethanol monthly back to 1981 for ethanol, 2011 for BD, 2021 for RD/SAF**) — from new EIAMonthlyBiofuelsCollector

**`bronze.eia_ethanol` and `bronze.eia_petroleum` do NOT exist** — CLAUDE.md is out of date and references tables that were never created. The `silver.eia_*` pipeline tables exist but are all empty; gold views pivot directly off `eia_raw_ingestion`.

### EIAMonthlyBiofuelsCollector (NEW Apr 2026)

- File: `src/agents/collectors/us/eia_biofuels_monthly_collector.py`
- Registered as: `eia_biofuels_monthly` in dispatcher (monthly D28 14:00 ET)
- 25 EIA v2 API series covering: BD (US + 5 PADDs), RD, Other Biofuels (EPOORO — includes SAF/renewable jet/heating/naphtha), Combined BD+RD, Biofuels-ex-Ethanol, Fuel Ethanol
- Uses 8-worker ThreadPool — pulls all 25 series in ~5 min
- IMPORTANT: Other Biofuels product code is **EPOORO** (not EPOOAEO as in domain knowledge file).

### EIANaturalGasCollector (NEW Apr 2026)

- File: `src/agents/collectors/us/eia_natural_gas_collector.py`
- Bronze table: `bronze.eia_natural_gas` (schema 040)
- Registered as: `eia_natural_gas` in dispatcher (monthly D28 14:30 ET)
- 15 EIA v2 series: marketed production (1976+), dry production, total consumption, imports, pipeline exports, LNG exports, monthly prices (wellhead/citygate/residential/commercial), weekly working gas storage (L48 + EAST + MIDWEST + SOUTH_CENTRAL), daily Henry Hub spot (5000+ rows)
- Backfill (Apr 14, 2026): **12,558 rows** across 15 series, 8-worker ThreadPool, ~11 minutes

### Census fuel HS codes (extended Apr 2026)

- 42 fuel HS/flow rows added to `silver.trade_commodity_reference` (21 codes × imports + exports)
- Covers: FUEL_ETHANOL (2207.10, 2207.20), BIODIESEL (3826), MOTOR_GASOLINE (2710.12), DIESEL (2710.19 ULSD range), JET_FUEL (2710.19.11/16), RESIDUAL_FUEL_OIL (2710.19.41/45), LNG (2711.11), NATURAL_GAS (2711.21)
- Default conversion: LT → '000 gallons' (0.000264172) for liquid fuels, KG → '000 tons' for LNG, M3 → MMcf for NG
- Census collector auto-picks up via `_load_hs_codes_from_db()`. Backfill 2013-2026 in progress in `scripts/backfill_census_fuels.py` (yearly chunks).

## Backfill the fuel flat file from new bronze data

Once the bronze table has data, re-run `scripts/build_fuel_flat_files.py` and swap its bootstrap from legacy xlsx to bronze queries. New approach:
- BD/RD/SAF/Ethanol monthly: query `bronze.eia_monthly_biofuels` filtered by fuel_type and attribute
- Convert MBBL → million gallons via `value * 0.042` (1 bbl = 42 gal)
- Ethanol weekly: still needs WPSR collector extension (only 12 weeks in eia_raw_ingestion)

## Missing EIA series (collector extension needed)

```
BIODIESEL:   PET.M_EPOORDB_YNP_NUS_MBBL.M  (production, EIA-819 monthly)
             PET.M_EPOORD_SAE_NUS_MBBL.M   (BD+RD stocks combined)
REN DIESEL:  PET.M_EPOORDO_SAE_NUS_MBBL.M  (RD stocks)
             PET.M_EPOORD_YIR_NUS_MBBL.M   (refinery net input)
ETHANOL (M): PET.M_EPOOXE_YOP_NUS_1.M, PET.MFESTUS1.M, PET.M_EPOOXE_VPP_NUS_MBBL.M
GASO PROD:   PET.WGFRPUS2.W, PET.MGFRPUS2.M   (production — stocks already collected)
GASO DEMAND: PET.WGFUPUS2.W, PET.MGFUPUS2.M
DIST PROD:   PET.WDIRPUS2.W, PET.MDIRPUS2.M   (production — stocks already collected)
DIST DEMAND: PET.WDIUPUS2.W, PET.MDIUPUS2.M
JET:         PET.WKJRPUS2.W, PET.W_EPJK_SAE_NUS_MBBL.W (stocks),
             PET.MKJRPUS2.M, PET.MKJIMUS1.M, PET.MKJEXUS1.M
RESID:       PET.WRERPUS2.W, PET.MRFRPUS2.M, PET.MRFSTUS1.M
NAT GAS:     NG.N9050US2.M (marketed prod), NG.N9140US2.M (consumption),
             NG.N9133US2.M (LNG exports)
SAF:         No dedicated EIA series — derive from EPA RFS D5/D7 RIN generation +
             LCFS reports (bronze.epa_rfs_rin_generation already exists).
```

**Full 12-week EIA backfill also needed** — current coverage is only Oct 31 2025 through Jan 16 2026.

## Missing Census HS codes (collector extension needed)

`bronze.census_trade` has 480K rows covering only HS chapters 10/12/15/23/52 (ag only). **Zero fuel codes.**

To add to Census collector:
```
Ethanol (fuel):     2207.20.0000 (denatured)
                    2207.10.6000, 2207.10.3000 (undenatured, fuel use)
Biodiesel:          3826.00.1000, 3826.00.3000, 3826.00.9000
Motor gasoline:     2710.12.1510, 2710.12.1525, 2710.12.1545, 2710.12.1590
Kerosene/Jet:       2710.19.1100, 2710.19.1600
Diesel (distillate):2710.19.1105, 2710.19.2100, 2710.19.2500, 2710.19.3100, 2710.19.3500
Resid fuel:         2710.19.4100, 2710.19.4500
LNG:                2711.11.0000
Natural gas:        2711.21.0000
(Propane:           2711.12.0000  — skip for now, revisit later per user)
(SAF:               no standalone code — tag via EPA D5/D7 RIN + LCFS)
```

Register these in `silver.trade_commodity_reference` with correct unit conversions (fuel codes typically kg → thousand gallons or kg → thousand barrels). The Census collector already supports 2013+ backfill for ag codes so the same infrastructure applies.

## Known bugs found during inventory

- **`ulsd_diesel` slug mislabel**: In `bronze.eia_raw_ingestion`, one of the series mapped under the `ulsd_diesel` slug is actually `EER_EPLLPA_PF4_Y44MB_DPG` which is **Mont Belvieu propane**, not ULSD. Fix in EIA collector series mapping.
- **CLAUDE.md stale**: References `bronze.eia_ethanol` and `bronze.eia_petroleum` tables that do not exist.

## Next steps (order of priority)

1. **Extend Census collector** with fuel HS codes (chapter 22/27/38) + register in `silver.trade_commodity_reference` + backfill from 2013. This unblocks all non-biodiesel fuel trade tabs.
2. **Extend EIA collector** with missing series (list above) + backfill full history. This unblocks production/stocks gaps for gasoline prod, distillate prod, jet fuel, residual, natural gas, and brings biodiesel/RD current.
3. **Fix `ulsd_diesel` mislabel** in the EIA collector series mapping.
4. **Refresh weekly ethanol** — legacy data ends 2020-10. Need to re-pull 2020-11 to present from EIA WPSR.
5. **Re-run `build_fuel_flat_files.py`** once data is back in bronze — script is structured to bootstrap from legacy first, then can be swapped to read from `bronze.eia_*` and `bronze.census_trade` directly.
6. **After flat files are full**: build the CY↔MY conversion workbook (`project_calendar_year_vs_marketing_year.md` flagged this as future work).

---

## project_helios_friday_demo

*(`project_helios_friday_demo.md`)*

---
name: Helios demo sprint (Friday 2026-05-22 meeting w/ Francisco Martin-Rayo)
description: Five-day build to show Helios CEO depth on BBD economics. IFV kg_callable + Streamlit demo, no acquisition framing.
type: project
originSessionId: f9769538-03ad-48d4-8472-58bcfe06d704
---
**Meeting:** Friday 2026-05-22 with Francisco Martin-Rayo (Helios AI / Horizon),
introduced by Joao. Original intent: dispel IP-overlap concern after Joao
helped both sides. **NOT a deal meeting** — too early for that; Helios just
closed a $4.7M seed Sept 2025 and is build-phase. Show depth, protect Joao
relationship, leave acquisition possibility for 6-12 months later.

**Demo arc (one screen, 12 minutes):**
1. Open: 86 bronze / 85 silver / 179 gold / 45 reference tables, 436 KG nodes
2. KG context viewer — `get_kg_context('renewable_diesel')`, `rd_price_stack`,
   `bbd_margin_model`, `feedstock_sensitivity_rule`, `cfpc_45z`
3. **IFV scenario runner live** — what RD margins look like under 4 policy
   scenarios for soybean_oil and tallow
4. Live BBD balance sheet readout — production/stocks/use most recent 6 months
5. Close: facility-level resolution (eagle_grove) referenced, Market Field
   future-state tease (do NOT show calibration)

**What we are deliberately NOT doing:**
- No acquisition framing. The deal model + pitfalls doc stay private.
- No multi-agent / LLM-natural-language demo (Helios already has that)
- No new data ingestion
- No facility-level decision agents (Sprint 2/3 work per project_roadmap_master)
- Market Field referenced but NOT shown — calibration is confidential IP

**Why focus on biofuel economics:** depth Helios can't replicate in 6-9 months.
Facility-level work is the area of overlap concern but it's also less developed
and they likely could replicate it. Market Field is the most differentiated
piece but too early to show.

**IFV kg_callable (the centerpiece) — shipped 2026-05-17 (Sunday):**
- Pure math: `src/agents/facility/hefa_economics.py` (20 tests passing,
  HOBO-calibrated: 1c/lb = $0.08/gal at 7.7 lb/gal yield)
- Wrapper: `src/kg/callables/implied_feedstock_value.py`
- Migration: `database/migrations/092_register_ifv_callable.sql`
- Spec: `docs/specs/implied_feedstock_value_kg_callable.md`
- Callable_key: `implied_feedstock_value`, parent_node `rd_price_stack` (id=216),
  source_context_id=137 (price_stack_decomposition)
- Modes: breakeven / target_margin / cash_compare / scenario_grid
- 5 policy scenarios: extension_2031, expiry_2027, iluc_removed, domestic_restriction, none
- Smoke test: `scripts/_smoke_test_ifv_callable.py` (5 tests, all pass via
  production invoker path)

**Smoke test calibration anchors verified 2026-05-17, corrected 2026-05-18 per Tore feedback:**

45Z math corrected from hardcoded table to formula `base * max(0, (50-CI)/50)`
with base=$1.00 RD/BD, $1.75 SAF (prevailing wage). 2025 IRS values; adjusts annually
with inflation. Tore's worked example: tallow CI=30 → RD 45Z = $1.00 × (50-30)/50 = $0.40/gal,
SAF = $1.75 × 0.4 = $0.70/gal.

ILUC penalty by feedstock category for iluc_removed scenario (subtracted from CI for 45Z only;
LCFS retains ILUC per HOBO ctx 127):
- waste_animal_fat: 0 (waste is unaffected)
- waste_oil: 0 (waste is unaffected)
- crop_oil: 22 g/MJ (soy oil drops from CI=50 to effective CI=28 → 45Z $0 to $0.44)
- palm_derivative: 35 g/MJ

Anchored outputs (May 2026 forward curve D4=$0.95/RIN, LCFS~$57/MT):
- **HOBO IL RD tallow base case**: bid $0.5932/lb, eff_sell $5.07/gal (below HOBO $5.50-6.50
  because D4 forward more pessimistic than HOBO's $1.50/RIN calibration). At $0.50/lb cash
  → $0.717/gal margin = HOBO base case ✓
- **Soybean oil iluc_removed lift**: extension_2031 → $0 45Z (CI=50 at threshold);
  iluc_removed → $0.44/gal 45Z (effective CI=28). Bid lifts +$0.057/lb feedstock = +$0.44/gal margin
- **45Z cliff for tallow at 2028-06-15**: bid compresses $0.0519/lb = $0.40/gal margin loss

**Tore-confirmed 2026-05-18**: $0.50/lb tallow May 2026 is realistic cash anchor.

**Live CARB pathway CI lookup (mig 093, 2026-05-18)**:
silver.lcfs_pathway_ci + silver.lcfs_pathway_ci_summary now wire the IFV
resolver to real CARB-certified pathway data from bronze.carb_lcfs_pathways
(892 rows, 890 mapped to canonical fuel + feedstock taxonomy). Replaces
hardcoded category averages. Resolver priority: facility-specific (best-CI
active pathway) -> CARB category median -> KG default.

**Real CARB medians by (fuel, feedstock):**
- RD tallow:        median 37.03 across 144 pathways  (range 15.64-63.29)
- RD UCO:           median 26.00 across  83 pathways  (range 17.08-39.77)
- RD soybean oil:   median 60.33 across  62 pathways  (range 51.74-84.80) - ALL above 50 threshold => zero 45Z
- RD canola oil:    median 56.06 across  31 pathways  (range 50.81-66.48) - all above threshold
- RD DCO:           median 32.50 across  56 pathways
- BD tallow:        median 32.73 across  95 pathways
- BD UCO:           median 20.84 across 172 pathways  (min -30.85 due to biogas-capture pathways)
- BD soybean oil:   median 56.42 across  67 pathways  (all above threshold)
- SAF tallow:       median 36.00 across  50 pathways
- SAF UCO:          median 27.31 across  16 pathways

**Best-in-class US RD facilities** (lowest active CI):
- AltAir Paramount CA tallow:        CI 15.64  (45Z $0.69/gal)
- Diamond Green Diesel Gulf UCO:     CI 17.08  (45Z $0.66/gal)
- REG Geismar LA UCO:                CI 17.50  (45Z $0.65/gal)
- CVR Renewables Wynnewood OK UCO:   CI 21.73  (45Z $0.57/gal)

**Updated calibration (CARB median CI=37 for tallow IL RD, replacing prior 30):**
- bid $0.5672/lb breakeven (down from $0.59 at CI=30)
- at $0.50 cash: $0.517/gal producer margin = squarely HOBO IL base case ✓
- 45Z = $0.26/gal (vs my prior $0.40 — formula gives realistic value at median CI)

**Demo flow: facility comparison block added**
Shows AltAir/DGD/REG/Neste/CVR side-by-side with their CARB-certified CIs and resulting bids.
"Same calculator, different facilities, real numbers."

**Per-pathway facility evaluation (added 2026-05-18 per Tore architectural request)**:
`evaluate_all_pathways_for_facility()` in src/kg/callables/implied_feedstock_value.py
pulls every CARB-certified pathway for a facility and evaluates LCFS + 45Z + implied
bid INDEPENDENTLY per pathway. Returns `best_per_feedstock` (one row per feedstock —
the agent's actual decision space) plus full `pathways` list.

Example: Diamond Green Diesel has 6 distinct approved feedstocks across 66 active
CARB pathways. Per-feedstock decision view (CI / LCFS / 45Z / bid):
  1. UCO            CI 17.1  $0.66  $0.66  $0.6149/lb (best margin)
  2. DCO            CI 27.4  $0.58  $0.45  $0.5767/lb
  3. Tallow         CI 30.8  $0.55  $0.38  $0.5643/lb
  4. Corn oil       CI 31.3  $0.54  $0.37  $0.5625/lb
  5. Soybean oil    CI 53.9  $0.35  $0.00  $0.4891/lb  (45Z=0, CI > 50 threshold)
  6. Canola oil     CI 54.2  $0.35  $0.00  $0.4887/lb  (45Z=0)

This is the building block for the future buyer_agent.py — agent iterates this
list and allocates procurement volume by feedstock margin rank.

**Architectural note on LCFS vs 45Z CI separation**:
HefaPriceStack now has separate `lcfs_pathway_ci_score` and optional
`_45z_ci_score` fields. Today both pull from CARB CI (approximation — 45ZCF-GREET
typically 5-15% more favorable). When IRS publishes 45ZCF-GREET pathway scores
the wiring is a 5-line change. ILUC status per Tore 2026-05-18:
  - LCFS (CARB): always includes ILUC for crop feedstocks (since 2009)
  - 45Z (IRS): includes ILUC in current final guidance; `iluc_removed` scenario
    captures the contested policy outcome where Treasury drops ILUC from 45Z
    while CARB keeps it in LCFS (HOBO ctx 127 structural insight)

**LCFS resources Tore flagged 2026-05-18** (`domain_knowledge/credit_info/LCFS/`):
- 2025_lcfs_fro_oal-approved (the OAL-approved 2025 regulatory order)
- LCFS Basics deck
- AAM Guidance Document
- Non-Metered Base Credits Methodology
- Multiple CARB scenario modeling workbooks (Baseline / Proposed / EJAC / 9step variants / 5step variants / Accel / Alt1 / Alt2)
- Air quality workbooks
NOT YET INGESTED — Sprint 2 work. Monthly LCFS credit transfer activity PDFs (price history)
queued as desktop Ollama background task.

**Tore action items before Friday:**
- Sun/Mon: red-team the calibration numbers (esp. tallow bid, soybean spread)
- Wed: review Streamlit page UX, tell me what to cut
- Thu: 30-min live rehearsal playing Francisco asking pointed questions

**Documents reviewed for Friday prep:**
- `Misc Personal Stuff/Helios/RLC_Info_for_Helios.pptx` — KG stats need
  updating (claims 224 nodes / 143 edges; actuals: 436 / 395 / 336 contexts)
- `RLC_Helios_LeaveBehind.docx` — needs 1.5-page rewrite, strip acquisition framing
- `RLC_Helios_pitfalls.docx` — INTERNAL ONLY, do not surface in meeting
- `RLC_Helios_Deal_Model.xlsx` — INTERNAL ONLY, mid-case EV $22.9M (P(home run) probably
  over-weighted at 15% — closer to 8-10% honest)

---

## project_helios_pepsi_pilot

*(`project_helios_pepsi_pilot.md`)*

---
name: project-helios-pepsi-pilot
description: "Helios<>RLC Pepsi pilot deal shape from the 2026-06-29 meeting — complementarity, deliverable, open interface questions, disintermediation watch."
metadata: 
  node_type: memory
  type: project
  originSessionId: bf6a8494-5d86-4f0c-bdf4-7e7106ff2f18
---

Outcome of the **2026-06-29 Helios AI <> RLC meeting**. Actual attendees: **Francisco Martin-Rayo**
(CEO/co-founder), **Eden Canlilar** (**CTO + co-founder**, ex-Google AI/ML, ex-Booz Allen — the
*unexpected* attendee, not planned), **Nathan Longoni** (Lead Backend Engineer & Data Architect),
**Dominic Aquino** (PM), **João Morciani** (connector + Tore's former FM Brazil analyst), Tore.
**Brooke Schuyler (Head of Product) was invited but did NOT attend.** Went well; a pilot shape
emerged. Significance: they sent **both co-founders + their lead data architect** (the pipeline
builders), not product/design — so the two-way data integration (their API/JSON, climate signals
into RLC models) was scoped for real by the people who'd build it. Eden (CTO) is almost certainly
the "woman who asked whether your models take climate as an input" in the debrief.

**Deal shape:** Pepsi is the **first pilot / test case**, NOT a signed account. RLC delivers →
Pepsi feedback → if it works, broader relationship (platform integration OR case-by-case). Fee
structure deferred; Francisco runs logistics.

**The value exchange = complementarity, not overlap.** Helios strong on climate/supply/weather,
**weak on demand-side feedstock** ("haven't spent much time on demand"). RLC strong on demand-side
feedstock + implied feedstock value. That gap is RLC's leverage — they chose not to build it.

**Deliverable v1:** rolling **price forecasts for canola, soy, sunflower + the reasons behind the
price** (the explanation is the product). Horizon (annual/procurement vs operating/hedging) and
format (price series on their platform vs prose in reports, daily cadence) are UNRESOLVED — Pepsi
must specify.

**Two-way data:** Helios climate signals (0–10 scale, JSON, their bronze/silver/gold graph DB +
API) feed RLC models; RLC demand/IFV feeds theirs. Their weather modeling likely better than RLC's.

**Commercial:** João "friends and family" pricing. Retainer: João gets price forecasts + standing
access to Tore to explain "what changed and why." Pepsi has strong PR team, willing to do a big
push (feedstock, Bloomberg) on next major release.

**"Intel Inside":** Tore wants implied-feedstock-value to become a branded ingredient pushed into
the industry. Parallel channel: **Arthur Trading** (Helios's fund/trader) — possible second intro.

**Open interface questions (both sides flagged as the immediate need):**
1. Exactly what RLC needs from João and what Pepsi needs from him — write this spec first.
2. Delivery shape + horizon (see above).
3. Attribution — the agreed mechanic is "**João explains to the client, not RLC**," which is
   efficient AND the disintermediation vector (RLC = invisible backend). Intel Inside only works if
   attribution is a term NOW, not later.

**Honest watch-items:** F&F price shouldn't anchor scale-up economics (Fortune-50 + PR machine);
resist scope creep ("demand for the entire world" is their appetite, not the pilot — pilot =
canola/soy/sunflower price+reasons); the deliverable rides on the demand/feedstock engine
[[project-ffa-feedstock-layer]] — the Helios deal is a reason to FINISH that layer, not a detour.

---

## project_iowa_multi_industry_expansion

*(`project_iowa_multi_industry_expansion.md`)*

---
name: Iowa multi-industry facility expansion
description: Roadmap for extending the Market Field facility graph beyond oilseed crush into ethanol, biodiesel, packing, layers, CAFOs, grain handling, rail, river, etc.
type: project
originSessionId: 4f606138-9604-4e05-9a04-1c081995d736
---
User flagged 2026-05-06 that the Market Field needs to span more
than oilseed crush. Spec doc + schema + initial seed shipped tonight.

## What was built tonight

**Spec:** `docs/specs/iowa_industry_facility_taxonomy.md` —
comprehensive taxonomy of 16 industry categories with per-industry:
- Role in the commodity supply chain
- Major operators in IA with public-knowledge counts
- Authoritative data sources (RFA, FSIS, IDALS, IA DNR CAFO db, etc.)
- Air permit availability + thresholds
- LLM extraction targets per industry

**Schema (mig 056):** `reference.facility_master` — unified
multi-industry ledger keyed on (facility_id, industry_code). One
row per physical facility, industry-specific capacity in
`reference.facility_capacity_<industry>` extension tables.
Supports 16 industry codes, status enum, canonical/superseded
flag (same dedup pattern as oilseed_crush_facilities).
Per-industry capacity tables created for ethanol, biodiesel, RD,
pork_packing, beef_packing, egg_layers, pig_finishing,
grain_handling, rail_terminal, river_terminal.

**Seed (mig 057):** populated `reference.facility_master` with
**52 known IA-relevant facilities**:
  ethanol         24 plants (POET, ADM, Valero, Green Plains, Cargill,
                              Big River, Lincolnway, Pine Lake, Plymouth,
                              Quad County, Western Iowa, Absolute,
                              Homeland)
  pork_packing    12 plants (Tyson 4, JBS 2, Smithfield 2, Seaboard
                              Triumph, Wholestone, Hormel, Sioux-Preme)
  egg_layers       7 operators (Versova, Center Fresh, Rose Acre x2,
                                 Sparboe, Daybreak, Farmers Hen House)
  biodiesel        6 plants (Chevron REG x3, Western Iowa, Cargill IF,
                              Stockton)
  beef_packing     3 plants (IA Premium Tama + 2 NE plants for context)

Combined with the 24 existing oilseed_crush facilities, IA total
inventory is now ~76 facilities.

## What's still TODO

### Tier 1 — required before Sprint 5 starts

1. **Geocode the new 52** facilities — rough city centroids in seed;
   use `scripts/geocode_iowa_facilities.py` extended to read
   `reference.facility_master` (currently filters
   `oilseed_crush_facilities`).
2. **Migrate oilseed_crush_facilities into facility_master** — preserve
   FK; downstream queries (build_facility_edge_weights.py, etc.)
   move to facility_master as the single source.
3. **Extend facility-graph builder** to handle multi-industry nodes
   with cross-industry edge weights (e.g., ethanol-plant within X mi
   of corn elevator = competitive bid relationship).
4. **Add ethanol-specific industry topic** to `market_topic_taxonomy`:
   - Currently Iowa oilseed crush has 8 topics. Ethanol-relevant
     topics already partially covered (corn supply ~ soybean_supply,
     veg oil demand ~ ethanol demand). Whether to add an explicit
     'ethanol_demand' topic vs. extend existing topics is a design
     decision — TBD.

### Tier 2 — Sprint 5 work

5. **Air permit collector — ethanol** — extend the existing IA DNR
   permit pipeline (per memory `project_state_air_permits_llm.md`)
   to ethanol-specific extraction templates. Per-industry targets
   from `iowa_industry_facility_taxonomy.md` Section 2.2.
6. **Air permit collector — pork packing** — same pipeline,
   different prompt template. Per Section 2.4.
7. **Air permit collector — biodiesel** — Section 2.3.
8. **Local LLM (qwen3-coder:30b)** running on the desktop GPU should
   handle bulk permit extraction once we have the pre-screened
   permit corpus per facility. Best-of-N union pattern same as
   chart annotation work.

### Tier 3 — Sprint 6 work (long-tail bulk ingestion)

9. **Iowa DNR CAFO database scraper** — ~3,000+ permitted sites,
   public, searchable, contains animal unit counts and lat/lon.
   Bulk download → bronze.cafo_raw → silver.facility_pig_finishing
   + silver.facility_egg_layers (sites split by species).
10. **IDALS warehouse license scraper** — ~500+ grain elevators in
    IA. Public state license data with operator + capacity.
11. **EPA RFS RIN-generation registry** — every active biofuel plant
    in the US is registered. Pull federal data to validate IA list
    + extend to other states.
12. **USDA AMS Approved Storage Facilities + GIPSA license list** —
    cross-source validation for grain elevators.

### Tier 4 — full geographic expansion

13. **Replicate to other states** — same schema, different node set:
    NE (#1 cattle), SD/MN (oilseed + pork), IL (corn + crush),
    KS/TX (cattle + wheat), CA (RD + dairy), etc.
14. **Cross-industry network edges** — once multi-state coverage
    exists, model how an ethanol plant in Iowa competes for corn
    against a packer in Nebraska through the basis-field layer.

## Key decisions for the user when ready

- **Industry scope to add next** — ethanol is the obvious next
  industry (most plants, largest non-crush corn demand). Pork
  packing is second (largest meal demand). User should confirm
  this priority order before Sprint 5 build starts.
- **Local vs cloud LLM for permit extraction** — per memory
  `reference_local_vs_cloud_llm.md`, this is exactly the kind
  of high-volume deterministic-output task that earns local LLM
  use. Recommend qwen3-coder:30b with best-of-N union pattern.
  Cloud only for borderline-quality permits flagged for review.
- **Data verification** — most seed values are public_knowledge
  (the data_source field flags this). Capacities should be
  replaced by permit-derived values once the air-permit pipeline
  runs. Fact-check on a sample by hand before relying on for
  client work.

## Sources to verify against

Per Section 2 of taxonomy doc:
- Ethanol: RFA biorefinery list at https://ethanolrfa.org/markets-and-statistics/biorefinery-locations
- Biodiesel: Clean Fuels Alliance America member directory
- Pork packing: USDA AMS Livestock Slaughter monthly + FSIS plant directory
- Egg layers: United Egg Producers + IA DNR CAFO database
- Grain handling: IDALS warehouse license database
- Rail terminals: USDA AMS Grain Transportation Report + STB filings
- River terminals: USACE Lock and Dam reports + Waterways Council directory

Cross-industry data validation will catch a lot of the gaps in this
seed. Plan a one-week verification sprint after the air-permit
pipeline runs.

---

## project_kg_callable_architecture

*(`project_kg_callable_architecture.md`)*

---
name: KG Callable Registry + Forecast Book Architecture
description: Three-layer architecture for turning KG from prose reference into executable analytical scaffold. kg_context = narrative, kg_callable = executable, core.forecasts = LLM book with reconciliation loop.
type: project
originSessionId: 5a48b8b6-c1da-480b-83b2-04db8b865662
---
## The three layers

### 1. Narrative — `core.kg_context`
JSONB frameworks, rules, thresholds. This is what an LLM *cites* when reasoning. Already had 121 contexts going into this work; now ~169.

### 2. Executable — `core.kg_callable` (migration 041)
Each row is a typed function attached to a KG node. Columns:
- `callable_key` (unique), `node_id` (FK to kg_node)
- `callable_type`: `formula` | `sql` | `python` | `sensitivity` | `composite`
- `signature` JSONB: typed inputs (with `source` hints pointing at data_series nodes), output contract
- `implementation`: for `python` type, dotted path like `src.kg.callables.weather_yield.run`
- `defaults`, `test_cases`, `units` JSONB
- `self_exploration` JSONB: sweep params, ranges, baseline, downstream nodes to re-eval, threshold_rules — enables Mode 2 (model tests its own assumptions)
- `source_context_id` FK to kg_context — every callable cites the narrative context that documents its logic
- `status`: `draft` / `active` / `deprecated` / `retired`

Invocation log: `core.kg_callable_invocation` — every call tagged by `mode` (`scenario` or `self_exploration`), inputs, output, warnings, error, duration, citations. Enables replay + quality measurement.

Helper view: `core.kg_callable_detail` — joins callable + parent node + source context.

### 3. Forecast book — `core.forecasts` + `core.actuals` + `core.forecast_actual_pairs`
Already existed; now exercised via `src/kg/forecast_book.py`.
- `record_forecast(commodity, forecast_type, target_date, value, unit, ...)` — writes to `core.forecasts` with full provenance (reasoning + citations + inputs in JSON `notes` field). Source tag `llm_forecast_book` distinguishes LLM projections from human (`analyst_spreadsheet`) and USDA (`usda_wasde`) sources.
- `record_actual(...)` — writes to `core.actuals` (NOT forecasts — that table has FK from forecast_actual_pairs)
- `reconcile(commodity, forecast_type, target_date, forecast_source)` — matches forecast to actual, writes pair with error/pct_error/MAPE/days_ahead.
- `accuracy_summary(commodity, forecast_type, source, since_days)` — MAPE rollup.
- `build_forecast_context(commodity, forecast_type, target_date)` — assembles prompt bundle: relevant KG contexts + available callables + recent actuals + competing forecasts from other sources.

## Invoker entry point
`src.kg.callable_invoker.invoke(callable_key, inputs, mode='scenario'|'self_exploration', invoked_by='mcp')` — validates inputs against signature, filters kwargs to what the target function accepts (self_explore/run have different params), dynamically resolves module.func, logs invocation with citations bundle.

## First callable seeded
`weather_adjusted_yield` (callable_id=1, attached to `crop_condition_yield_model` node id=129, cites kg_context #40 `yield_model_parameters`).
- Implementation: `src/kg/callables/weather_yield.py`
  - `run()` — point scenario
  - `self_explore()` — rain/temp sweep, sensitivities, breakpoints, P10/P50/P90 scenarios
- Pilot result: corn pollination 1.5in rain + 92F → -17 bpa delta (166 vs 183 baseline), confidence 0.7

## Pattern for adding new callables
1. Write the function in `src/kg/callables/<name>.py` with a `run(**kwargs)` entry point (and optional `self_explore`)
2. Create a seed script `scripts/seed_<name>_callable.py` following `seed_weather_yield_callable.py` shape
3. Attach to an existing KG node; cite a specific `kg_context` via `source_context_id`
4. Add test_cases as JSONB
5. Register invocation via `invoke()` — never call Python function directly from MCP

## Expansion priorities (from forecast book pilot)
Monthly series each needs its own callable + KG context:
- NOPA monthly crush (template: pace-vs-WASDE model)
- EIA ethanol weekly production (template: corn grind conversion)
- RIN generation D4/D6 (template: RVO pace)
- Crush margins (already have oilseed_crush engine — wrap it)
- Balance sheet end-of-month stocks (template: identity + NASS coverage)

## Files
| Path | Purpose |
|---|---|
| `database/schemas/041_kg_callable.sql` | Migration (applied to RDS) |
| `src/kg/callables/weather_yield.py` | First callable |
| `src/kg/callable_invoker.py` | Generic invocation + logging |
| `src/kg/forecast_book.py` | Forecast recording + reconciliation |
| `scripts/seed_weather_yield_callable.py` | Seed template |
| `docs/CAPABILITIES_INVENTORY.md` | System-wide reorg baseline |
| `docs/KG_GAP_FILL_STRATEGY.md` | Orphan classification + phased plan |

---

## project_launch_timeline

*(`project_launch_timeline.md`)*

---
name: Launch timeline — opening next week
description: RLC opening publicly week of April 7, 2026 with first report and website
type: project
---

**Target launch: Week of April 7, 2026**

Deliverables needed:
1. First weekly report (PDF) — designed and populated with real data
2. Website live at roundlakescommodities.com (Netlify deployed, waiting on Namecheap DNS)
3. Buttondown email signup working (wired up, HogCrush account)
4. Balance sheet templates populated with trade data (in progress)
5. DOC running daily after market close

**Why:** Revenue clock starts when the first report goes out. Conference proposals are sent. The system is ready. Delay = lost momentum.

**How to apply:** Every decision this week should be filtered through "does this help us open?" If not, it waits.

---

## project_liquid_fuel_stocks_workflow

*(`project_liquid_fuel_stocks_workflow.md`)*

---
name: US liquid-fuel Production + Stocks + Domestic Use workbook pipeline
description: How models/Biofuels/us_liquid_fuel_and_biofuel_production.xlsx is filled — Production from EMTS, Stocks + Domestic Use from EIA, all via one Python script
type: project
originSessionId: f9769538-03ad-48d4-8472-58bcfe06d704
---
`models/Biofuels/us_liquid_fuel_and_biofuel_production.xlsx` is an `.xlsx` (no VBA)
with three parallel flat-file sheets: **Production**, **Stocks**, and **Domestic Use**,
identical layout (monthly dates in col A starting 1990-01, 16 columns of data series
across biofuels + fossil + co-products).

⚠️ **Case-sensitivity gotcha**: git tracks the file as `Models/Biofuels/...` (capital M)
from an earlier commit. The path constant in the script writes lowercase
`models/Biofuels/...`. On Windows it resolves to the same file but `git add models/...`
won't stage the modification — must use `git add Models/...` to commit changes.

**Pipeline (added 2026-05-13):**
- Migration 081 created `gold.us_liquid_fuel_stocks_monthly`.
- Migration 085 created `gold.us_liquid_fuel_domestic_use_monthly`.
- All three views parallel `gold.us_liquid_fuel_production_monthly`.
- Stocks source = `bronze.eia_monthly_biofuels` filtered `attribute='stocks' AND region='NUS'`.
  Native unit = **MBBL** (thousand barrels). View converts to thousand gallons via × 42.
- Production source = `silver.emts_production_canonical` (EMTS RIN generation in gallons).
- Domestic Use source = `bronze.eia_monthly_biofuels` via apparent-consumption derivation:
  `BD use = prod + imp − exp` (BD series, 2011+), `RD use = prod + imp` (no RD exports
  series; 2021+), `Ethanol use = blender_input` (MFERIUS1, 1993+).
- Single Python script `src/tools/update_us_liquid_fuel_production.py` updates all
  three sheets. Defaults: `--sheet all`, full history. Supports `--months N`,
  `--sheet production|stocks|domestic_use|all`, `--dry-run`.

**Why:** Tore wanted ongoing-update tooling parallel to the Production sheet, with
the convention of leaving blank any series EIA doesn't publish — no synthetic
estimates until the rail-car tracker can provide them.

**How to apply:** When EIA releases new monthly biofuels data (Aug release for
Jul data is typical), run the script with no args to backfill both sheets.

**Columns driven (5 of 16, post-mig-091):**
- **Stocks**: B Biodiesel = combined_bd_rd − renewable_diesel (× 42); C RD = direct;
  **D Co-Processing = 0** (refinery-integrated, no inventory); **E SAF = apparent use × 1.0 month**
  (heuristic placeholder); F Ethanol = direct
- **Domestic Use**: B Biodiesel = apparent (prod+imp−exp); C RD = apparent (prod+imp);
  **D Co-Processing = production** (refinery-integrated); **E SAF = prod + imp − exp** (mass balance);
  F Ethanol = blender_input (NOT consumption — that series returns 0 for recent months)

**Estimation framework (mig 091):** Tore's traditional "borrow another commodity's
seasonality" doesn't fit here because we have monthly production directly. The gaps
are monthly use + stocks, which is a mass-balance problem. Seasonality borrowing
would only help once we ingest EIA monthly jet/diesel/gasoline stocks — at which
point we could refine SAF stocks shape using jet seasonality, and BD/RD stocks
using diesel seasonality.

**Why `combined_bd_rd/blender_input` is wrong for total disappearance**: it's only
*refinery and blender net input*, excluding direct-to-end-user volumes (RD trucked
straight to CA LCFS fleets without going through a refinery blender). Dec 2025
combined_blender_input was 1,290 MBBL while RD production alone was 6,076 MBBL —
conclusive that combined_blender_input is NOT total BD+RD disappearance.

**Columns left blank in Stocks/Domestic Use (intentional, per Tore):**
D Co-Processing, E SAF (EIA doesn't separate from RD/biofuels)
H Diesel, I Jet Fuel, J Gasoline (would come from petroleum tables, not v1)
L Glycerin, M FAME, P Soap Stock, Q Methyl Acetate (no public source)
N Renewable Naphtha, O Renewable Propane (lumped in "other_biofuels", not separable)

**Bug fixed in same edit:** Previous script targeted `wb["Sheet1"]`; the workbook
had been renamed to "Production" + "Stocks" so the script was broken before this
session. Now correctly uses the sheet names.

---

## project_market_field_spec

*(`project_market_field_spec.md`)*

---
name: Market Field — sentiment + network + basis unified layer
description: The proprietary layer that couples positioning + fundamentals + sentiment via opinion-dynamics-on-network math; phase-transition framing
type: project
originSessionId: 4f606138-9604-4e05-9a04-1c081995d736
---
**The Market Field** is the proprietary layer that ties facilities, market
participants, and information sources into one substrate. Closes the
analytical-stool gap: positioning (CFTC) and fundamentals (KG / S&D) were
already there; sentiment was missing.

**This is the central proprietary asset. Treat the calibration details
(α/β/γ/ε weights, edge taxonomies, topic mix) as confidential.**
The conceptual framework can be shared externally; the parameters cannot.

## Three-legged stool

| Leg | Already built | Stored in |
|---|---|---|
| Positioning | yes | `gold.cftc_*`, daily basis levels |
| Fundamentals | yes | KG (`core.kg_*`), `gold.fas_*`, `silver.monthly_realized` |
| Sentiment | NEW (this) | network on facility graph, daily updates |

## Math (Layer 1 — sentiment dynamics)

Per facility i, per topic k, per day t:

  s_{i,k}(t+1) = α · s_{i,k}(t)               // decay/inertia
              + β · news_{i,k}(t)              // local exogenous
              + γ · Σ_j w_{ij} · s_{j,k}(t)   // network influence
              + ε · jump_{i,k}(t)              // weak-tie randomness

  s ∈ [-1, +1], reset (not decay) when contradicting story arrives

**Edge weights w_ij** blend:
- Same parent company (highest)
- Same draw region (high)
- Same industry vertical (medium)
- Trade-counterparty / partnership ties (medium)
- Weak/random ties (low — Granovetter's strength-of-weak-ties)

**Local vs national news**: national stories enter every node's news_{i,k}
directly with same intensity (no propagation needed — already public);
local stories enter one node, then propagate via Σ_j term.

**Reversal vs decay**: when a story or datum *contradicts* prior sentiment
(Iran-war example: bombing → instant reversal of bullish-oil sentiment),
override decay and snap to new sentiment. Detection: opposing-sign news
with magnitude above threshold.

**Chain-of-command** is a SEPARATE hierarchy on top of the lateral
network — boss-to-analyst pushes down with high weight, analyst-to-boss
pushes up only for novel/extreme content. Distinct from peer edges.

## Layer 2 — sentiment → action

Sentiment vector s_i(t) feeds each facility agent's decision loop
ALONGSIDE fundamentals (KG callable returns) and positioning. All three
inputs to the same buy/sell/hold function — the three legs converge here.

## Phase transition framing

Opinion dynamics on networks undergoes phase transitions as parameters
cross thresholds (consensus / polarized / fragmented). Coupling strength
γ × density × stubbornness drives the transition. This is where the
phase-change vocabulary from the original Modeling Systemic Phase
Transitions doc actually applies — through the I(t) component of that
paper's 7-vector, instantiated at facility-graph resolution.

## Iowa starter topics (8 total, 4 categories)

| # | Topic | Category | Time-varying weight |
|---|---|---|---|
| 1 | Weather / growing conditions | Inputs | none |
| 2 | Soybean supply (input) | Inputs | none |
| 3 | Veg oil demand (RD/SAF/biofuel/food/industrial) | Outputs | × oil_share |
| 4 | Meal / livestock feed demand | Outputs | × (1 − oil_share) |
| 5 | Policy — Federal (45Z, RFS, CFR, EPA) | Policy | none |
| 6 | Policy — State / Local | Policy | none |
| 7 | Policy — Industry mandates (CORSIA, ReFuelEU, voluntary corporate) | Policy | none |
| 8 | Competitor activity (capacity, M&A, plant openings/closures) | Industry | none |

**Oil-share weighting** — topics 3 and 4 are scaled by current
soybean crush economics. oil_share = oil_revenue_per_bu / (oil_revenue
+ meal_revenue). Source: `silver.oilseed_crush_margin` filtered to
oilseed_code='soybeans' (LOWERCASE PLURAL — gotcha). Monthly back
to 2015. Current value Apr-2026 = **49.8%** (historically high; pre-
2020 was ~30% with meal carrying plant economics, oil was a near-
byproduct — that's why soybean oil is in everything, crushers needed
to move it). The flip is real and the model needs to track it
because the relative importance of veg-oil-demand vs livestock-feed-
demand news literally inverted with the RD buildout.

## Generalization

Math is market-agnostic. Per market, instantiate:
- Node set (facilities in that market)
- Edge weighting (org/region/industry/ties — already built for IA)
- Topic taxonomy (4-7 topics, market-specific)
- News source list (trade press + general)

Same equations, drop-in for EU rapeseed / Brazil soy / US wheat / etc.

## What's deferred

- Layer 1 plumbing: news ingest pipeline, sentiment classifier (Claude),
  facility-graph construction (already started for IA), update loop
- Layer 2 plumbing: integration with facility agent decision function
- Calibration: α/β/γ/ε defaults first, then tune from observed dispersion
  rates of historical news events
- Broader information network (yield estimates, position changes,
  forecast prices following same propagation) — Sprint 3+ extension

## Reference

- Spec doc: `docs/specs/market_field_spec.md` (shareable framework)
- Phase-transition paper: `docs/Modeling Systemic Phase Transitions in
  Complex Economies.docx` — parent framework, this is the I(t) engine
- Theoretical roots: DeGroot 1974 (consensus dynamics), Granovetter
  1973 (weak ties), Bikhchandani-Hirshleifer-Welch 1992 (cascades),
  Ising model on networks (phase transitions)

---

## project_minor_oils_coverage

*(`project_minor_oils_coverage.md`)*

---
name: Minor oils coverage philosophy
description: User decided to include safflower, flaxseed/linseed, olive oil, fish meal, and other minor oils/proteins — not now, but as system matures. Key advantage of RLC system is covering gaps USDA doesn't.
type: project
originSessionId: 2a87c99e-d9e9-4822-83cb-2459eddd9173
---
**Decision (2026-04-17):** Include minor/niche oils and proteins in coverage over time. Not a current priority but will circle back.

**Why:** One of the key advantages of the RLC system is covering commodities that USDA provides limited or no data for. As the system improves at gap-filling (estimating from seed production, trade data, assumptions), covering safflower, flaxseed, etc. becomes a competitive differentiator vs. relying solely on USDA publications.

**Commodities to add later:**
- Safflower oil/meal — USDA publishes very little (2 series). We already pull what exists. User initially confused safflower with flaxseed.
- Flaxseed / linseed oil — similarly limited USDA coverage
- Olive oil — minor in US but globally significant
- Fish meal — protein competitor, different supply chain
- Other minor vegetable oils as identified

**How to apply:** When building out minor commodity coverage:
1. Start with what USDA does publish (seed production, trade HS codes)
2. Estimate crush, oil/meal production, inventories from available data + assumptions
3. Cross-reference with Census trade for import/export volumes
4. User has historical spreadsheets for some of these
5. The ERS Oil Crops Annual Summary (see project_oil_crops_annual_summary.md) likely has historical data for several of these

**Note:** Safflower data IS already being collected (confirmed in current NASS collector config). The earlier "discontinuation" note was based on user confusion with flaxseed. Updated that memory accordingly.

---

## project_next_phase_ops_audit

*(`project_next_phase_ops_audit.md`)*

---
name: Next Phase — Operations Audit + LLM Forecasting Layer
description: After US balance sheets complete, full month-by-month ops audit of all data collection, then build LLM self-maintaining forecast model for every data series in spreadsheets
type: project
---

## Sequence (after US oilseed/fat/grease/fuel balance sheets done)

### Phase 1: Full Operational Audit
Walk through a typical month day-by-day and catalog:
- Everything the system IS doing automatically (collectors, dispatcher schedules)
- Everything it SHOULD be doing but isn't (NDVI? weather forecasts? WASDE day processing?)
- Verify every scheduled task fires correctly and produces the right output
- Ensure crop condition/progress reports, WASDE data, EIA weekly, CFTC, NASS processing reports all run on schedule
- Cross-reference against the master_scheduler.py entries vs what actually needs to happen

### Phase 2: LLM Forecasting Layer
Build a system where the LLM maintains its own model to:
- Monitor every data series that appears in the user's spreadsheets
- Predict upcoming values before official releases
- Compare predictions to actuals when data arrives
- Generate reports and graphics automatically
- This is the "human vs LLM accuracy tracking" endpoint from the project vision

### Phase 3: Month-Long Monitoring
- Watch everything for one full month
- Ensure nothing is missed — neither in spreadsheets (user) nor in data collection/processing (agents)
- Checklist-driven approach: every expected data release has a corresponding collection + processing + reporting step
- Goal: "German-car-level operational reliability"

**Why:** The balance sheets are the analytical framework. The ops audit ensures the data pipeline feeds them reliably. The LLM forecasting layer is the product — automated market intelligence with eye-melting graphics.

**How to apply:** Don't start the ops audit until the user says balance sheets are done. Then walk through literally every day of a month and map data flows end-to-end.

---

## project_next_steps_regional_bs

*(`project_next_steps_regional_bs.md`)*

---
name: Next steps — regional balance sheets then rail tracker
description: Decided 2026-03-23 to build regional feedstock balance sheets next, then rail car tracker on top
type: project
---

## Decision (2026-03-23)

Regional balance sheets BEFORE rail car tracker.

**Why:** Rail tracker is a monitoring layer — needs the balance sheet infrastructure underneath to give context to car counts. Balance sheets also directly fix the allocation engine's supply constraint gap (canola -44%, DCO -22% vs EIA).

**Balance sheet minimum structure:**
1. Beginning Stocks
2. Production
3. Imports
4. Total Supply (sum 1-3)
5. Domestic Usage (expandable — biofuel use, food use, industrial, etc.)
6. Exports
7. Total Usage (= Total Supply - Ending Stocks)
8. Ending Stocks (end of month)

Some commodities (like SBO) will have more detailed domestic usage breakdowns.

**Regional scope:** Need producer locations + end-user locations modeled regionally. State-level USDA offices may provide some data. Heavy reliance on estimates.

**How to apply:** Build the schema for regional monthly balance sheets by feedstock. User is building the actual numbers in spreadsheets now. Load data as it arrives.

**After balance sheets:** Rail car tracker (BioTrack AI at biotrack/biotrack_ai.py already has infrastructure). Rail cars flowing between modeled producers and consumers = unique supply/demand intelligence.

---

## project_oil_crops_annual_summary

*(`project_oil_crops_annual_summary.md`)*

---
name: ERS Oil Crops Annual Statistical Summary ingestion
description: Large ERS reference dataset at data/raw/oilseeds_fats_greases/oil_crops_annual_statistical_summary_042026.xlsx needs bronze ingestion. Contains data not in PSD — LLM should have access to all of it.
type: project
originSessionId: 2a87c99e-d9e9-4822-83cb-2459eddd9173
---
File: `C:\dev\RLC-Agent\data\raw\oilseeds_fats_greases\oil_crops_annual_statistical_summary_042026.xlsx`

**Why:** ERS Oil Crops Yearbook / Annual Statistical Summary contains historical US oilseed S&D data that NASS and PSD don't cover — crush margins, domestic disappearance breakdowns, price series, yield components, etc. Even data not directly in user's spreadsheets should be accessible to the LLM for analysis context.

**How to apply:** 
1. Inventory the sheets/tables in the workbook
2. Identify which series overlap with existing bronze tables (PSD, NASS processing, Census trade) — skip those
3. Design bronze table(s) for the net-new data (likely `bronze.usda_ers_oilcrops` or extend `bronze.usda_ers_data`)
4. Build ingestion script to parse the Excel and load to bronze
5. No rush — not blocking current balance sheet work, but should be done before LLM forecasting layer goes live

**Added:** 2026-04-17

---

## project_open_followups_2026-06

*(`project_open_followups_2026-06.md`)*

---
name: open-follow-ups-parked-2026-06-14
description: Two deferred tasks Tore explicitly asked not to forget while shifting to corn trade
metadata: 
  node_type: memory
  type: project
  originSessionId: ef803115-014b-4006-b2b5-b4bf7ad50ff7
---

Tore parked these on 2026-06-14 to shift to US corn/grain trade; said "I do not
want to forget 1 and 2." Surface them when grain-trade work wraps.

1. **Sunflower oil-balance estimator** — the `ESTIMATED_FROM_OIL_BALANCE` rows in
   `bronze.nass_processing` (sunflower estimated crush / meal / crude-oil-production,
   commodity_desc='ESTIMATED') stopped at Jan 2026 because the script that computed
   them no longer exists in the repo. NASS doesn't survey sunflower crush directly —
   these were back-calculated from refined-oil data. Decision needed: reconstruct the
   estimator (what yield/conversion assumptions?) or drop sunflower estimate detail.
   Also: sunflower crude-oil STOCKS genuinely discontinued by NASS after Jan 2026
   (likely (D) suppression) — not our bug. See [[project_safflower_discontinuation]].

2. **Iowa facility-agent schedule** — owed since the Fable-5-crash session. `facility_master`
   has 49 IA facilities (25 ethanol, 11 pork, 7 egg, 5 biodiesel, 1 beef) + 24 oilseed
   crush; only ~17 have permits parsed and ALL on-disk permit PDFs are oilseed-crush.
   The 47 ethanol/pork/egg/beef/biodiesel facilities have NO permit PDFs — so IA DNR
   PDF acquisition is the bottleneck, not LLM throughput. Deliverable = sequenced
   schedule (acquire PDFs → desktop-LLM parse via existing pipeline → load → xref).
   See [[project_iowa_multi_industry_expansion]], [[project_state_air_permits_llm]].

---

## project_ops_audit_plan

*(`project_ops_audit_plan.md`)*

---
name: Operations Audit & LLM Self-Monitoring Plan
description: 6-phase plan to build a coverage map, morning briefing email, LLM log review, and forecast tracking. Leverages existing event_log/collection_status/SMTP. Apr 2026 start.
type: project
---

## Goal
Build a repeatable operations framework so:
1. The user knows exactly what the LLM did yesterday and what's coming today (morning briefing email)
2. Every meaningful action is logged (database changes, forecasts, decisions, failures)
3. The LLM reviews its own logs and flags issues for human review
4. The framework is documented and reusable for future areas

## What We Already Have (do not duplicate)
- `core.event_log` + `core.llm_briefing` view (CNS)
- `core.collection_status` + `core.data_freshness` view
- `audit.transformation_session/operation/output_artifact/lineage_edge`
- `core.llm_call_log` (tamper-evident hash chain)
- `daily_activity_log.py` (manual work tracking)
- `src/engines/doc/daily_ops.py` (5:30 PM ET DOC engine)
- SMTP configured in .env (Gmail, tore.alden@roundlakescommodities.com)
- Weather email at 6:30/13:00/20:00 (only thing emailing)
- `master_scheduler.py` with all day-of-week schedules
- `get_briefing()` MCP tool

## Phases

### Phase 0 — Coverage Map (1 session)
**Deliverable:** `config/operations_coverage.yaml` — single source of truth for what we expect.
- One entry per data release / scheduled process
- Fields: name, frequency, day_of_week, release_time_et, source, collector_module, expected_rows, sla_minutes, downstream_processes, generates_alert
- Built by: cross-referencing master_scheduler.py + USDA/EIA/NASS release calendars
- Used by: Phase 5 verification loop

**User does:** Nothing — I audit and write.

### Phase 1 — Logging Gaps (1-2 sessions)
**Deliverable:** Every meaningful action writes to event_log.
- Audit each collector to confirm it calls `core.log_event` on success/failure
- Add new event_types: `forecast_made`, `forecast_verified`, `outlook_change`, `manual_data_edit`, `anomaly_detected`, `briefing_sent`
- Add `forecast_log` table for predictions (separate from event_log because it has comparable values)
- Add `outlook_change_log` table for when LLM or user changes a market view
- Hook for "user edited spreadsheet" — manual but logged when user tells me

**User does:** Approve schema migration, run `psql` (or I run it). Tell me when you've made manual edits to balance sheets so I can log them.

### Phase 2 — Morning Briefing Email v1 (2-3 sessions)
**Deliverable:** `scripts/generate_morning_briefing.py` running at 6:00 AM ET, emails to you.

**Briefing structure:**
1. **Yesterday's work** — collectors run, rows added, transformations completed (from event_log)
2. **Failures and gaps** — anything in coverage_map that didn't run, anything in collection_status with status='failed', anything in data_freshness flagged is_overdue
3. **Today's expected releases** — pulled from coverage_map for today's day_of_week
4. **Open questions** — items the LLM has flagged as needing human review (Phase 3)
5. **Yesterday's forecast accuracy** — predictions vs actuals (Phase 4)
6. **System health** — dispatcher heartbeat, watchdog status, any zombie restarts

**Implementation:**
- Pulls from existing event_log/collection_status/data_freshness
- Markdown body, sent via existing SMTP
- New scheduler entry: `morning_briefing` daily 6:00 AM ET
- New event_type 'briefing_sent' so we know it ran

**User does:**
- Confirm Gmail SMTP password still valid (may need an app password regen)
- Read briefing for first week, give feedback on what's missing or noise
- Tell me when format needs adjustment

### Phase 3 — LLM Log Review (2-3 sessions)
**Deliverable:** `scripts/review_logs.py` — calls Claude API to read recent event_log entries and flag issues.

**What it does:**
- Reads last 24h of event_log entries
- Reads collection_status for any failures or partial collections
- Reads data_freshness for overdue items
- Reads forecast_log for prediction misses
- Sends to Claude with prompt: "Identify anomalies, missing collections, suspicious patterns. Flag what needs human review."
- Writes flagged items back to event_log with priority=1 and event_type='anomaly_detected'
- These flags surface in the next morning briefing

**Schedule:** runs at 5:45 AM ET (before 6:00 briefing) so flags are fresh

**User does:**
- Review LLM-flagged items for first 1-2 weeks
- Give feedback on what's signal vs noise (we tune the prompt)
- Decide which flags should auto-resolve vs stay open

### Phase 4 — Forecast Tracking System (3-4 sessions)
**Deliverable:** LLM makes predictions before each major release, accuracy is tracked.

**Schema:** `forecast_log` table
- forecast_id, generated_at, target_release (e.g. 'NASS_CROP_PROGRESS_2026-W15'), predicted_values (JSONB), actual_values (JSONB), error (JSONB), generated_by (LLM/HUMAN), notes

**Pre-release predictor:**
- For each major release in tomorrow's calendar, generate a forecast
- For NASS Crop Progress: predict G/E % by state, planting %, etc.
- For WASDE: predict ending stocks for major commodities
- For CFTC COT: predict positioning changes
- Store with target_release identifier

**Post-release verifier:**
- After collector finishes, look up the forecast for that target_release
- Compute error
- Update forecast_log with actuals
- Log accuracy stat in event_log

**Reporting:**
- Track LLM skill over time
- Compare to user's forecasts (when user logs them)
- This is the "human vs LLM accuracy tracker" from the project vision

**User does:**
- For first month, also log YOUR forecast before each release so we have a comparison
- Review accuracy reports, tell me what's worth tracking vs what's noise

### Phase 5 — Coverage Verification Loop (ongoing, starts Day 14)
**Deliverable:** Daily reconciliation between coverage_map and actual activity.

**Two-way check:**
- For every item in `operations_coverage.yaml` that should have run yesterday, verify it appears in event_log/collection_status. If missing, generate alert.
- For every event in event_log, verify it maps to a coverage_map entry. If not, flag as undocumented activity.

**Report:** Appears in morning briefing as "coverage gaps" section.

**User does:** Review undocumented items weekly, decide if they should be added to coverage_map or removed.

### Phase 6 — Framework Application (ongoing)
**Deliverable:** A documented pattern that any new data area follows.

**Template:**
1. Add entries to `operations_coverage.yaml`
2. Collector inherits BaseCollector → automatic event_log + collection_status
3. Transformations use `transformation_logger` context manager
4. Forecasts (if any) write to forecast_log
5. Outputs registered as `audit.output_artifact`

**Documentation:** `docs/OPERATIONS_FRAMEWORK.md` — how to add a new area

## Execution Order
1. Phase 0 (1 session) — coverage map
2. Phase 2 (parallel with Phase 1) — get briefing email working ASAP since user wants visibility
3. Phase 1 (parallel) — fix logging gaps
4. Phase 3 — LLM review feeding into briefing
5. Phase 4 — forecast tracking
6. Phase 5 — verification loop
7. Phase 6 — document and apply

**Priority:** Start with Phase 0 + Phase 2 because the briefing email gives the user immediate visibility, then iterate.

## Why
The user works solo and needs the LLM to be a reliable junior analyst that:
- Reports in every morning on what it did and what's broken
- Flags things that need human attention
- Tracks its own accuracy
- Operates within a documented framework that scales to new areas

The framework also sets up the "LLM forecasting layer" from project_vision_endpoints — accuracy tracking is the foundation for the "human vs LLM" comparison product.

## How to Apply
- Don't build new logging tables — extend event_log with new event_types
- Don't build new email infrastructure — use existing SMTP
- Don't bypass collection_status — every collector goes through it
- Coverage map is the source of truth — if it's not in there, it shouldn't be running (or it should be added)

---

## project_permit_archive

*(`project_permit_archive.md`)*

---
name: project_permit_archive
description: State of the organized Title V permit archive (permits/) and extraction pipeline as of 2026-06-20
metadata: 
  node_type: memory
  type: project
  originSessionId: 7d25feff-108a-4d08-9794-c7c81da7d481
---

The IA DNR Title V permit pipeline is fully drained as of 2026-06-20. Pipeline:
acquirer -> `bronze.source_documents` queue -> `parse_spine.py` (best-of-N on the
desktop GPU) -> `bronze.state_air_permits` + `state_air_permit_units` ->
`publish_permits_to_organized_archive.py` -> `permits/<industry>/<state>/<facility>/`.

**Current state:** 288 distinct facilities, 8,847 emission units. source_documents
= 288 parsed, 0 failed, 15 dup-skipped. Each archive folder has `source.pdf`,
`equipment_list.csv`, `extraction_summary.md`; the 54 oilseed_crush facilities also
have `process_flow_coverage.md` (gated to crush — the only industry with a canonical
flow). Tore values this archive highly: equipment lists feed per-facility financial
models; extraction_summary is shareable with facility owners for accuracy checks.

**Git policy (decided 2026-06-20):** `permits/**/source.pdf` (272MB across 288) and
`permits/**/equipment_list_auto.csv` are gitignored — they're copies of the already-
gitignored `data/permits/` originals. Curated text artifacts (csv/md) ARE versioned.
The 20 legacy PDFs were `git rm --cached`. Regenerate the whole archive any time
(idempotent) with `python scripts/publish_permits_to_organized_archive.py`.

**Extraction models:** qwen2.5:7b (NOT 30b — see [[reference_ollama_gpu_cpu_fallback]]).
Large multi-unit permits use chunked extraction (`--chunk-chars`, `retry_failed_chunked.py`)
— see [[reference_local_vs_cloud_llm]].

**Deferred data-quality items** (template-first; fix before anything public — Tore's call):
- Unit OVER-enumeration on large chunked permits (ADM Clinton "526 units", Gable 393).
  Chunked retry was N=1, so precision is lower than the main drain's best-of-N rows.
- Industry tag is free-text from the LLM → folder sprawl (gypsum×3 spellings) and
  mis-tags (Metro Methane Recovery tagged renewable_diesel). Normalize `industry_for()`
  and re-publish.
- New-permit folder slugs are the ugly filename stem (no clean facility_id from LLM).
- Spot-check queue (one facility/industry) at `docs/permit_extraction_spotcheck_queue.md`.

**Recurring infra gotcha:** scripts in `scripts/` that `import src.*` must
`sys.path.insert(0, ROOT)` first (ROOT = repo root, NOT .../src). The loader and
publisher both shipped without it and crashed silently/loudly. parse_spine has it right.

---

## project_permit_parsing_secret_sauce

*(`project_permit_parsing_secret_sauce.md`)*

---
name: Permit parsing — the secret sauce of facility-level intelligence
description: Strategic vision for state air permit parsing as the core infrastructure for facility-level analysis across crushers, biofuel, slaughter, fats, UCO, food mfg
type: project
originSessionId: 4f606138-9604-4e05-9a04-1c081995d736
---
## The vision (per user, 2026-04-30)
State air-quality permits (Title V, FESOP, Synthetic Minor) are the **single richest open-source dataset for facility-level operational intelligence** in agricultural supply chains. Every facility that emits regulated pollutants must publicly disclose their equipment list, rated capacity, throughput limits, control devices, and operating conditions. This is the foundation for everything from per-facility supply forecasting to crush-margin modeling to feedstock-allocation engines.

**Why it's the secret sauce:** Trade data, USDA reports, and CFTC positioning are commodity-aggregated. Permits are facility-resolved. The combination — top-down macro signals + bottom-up facility-level detail — is what turns a typical commodity research shop into something more analytically powerful.

## Industry sequence the user wants to cover (in order)
1. **Oilseed crushing facilities** ← current focus, IA proof-of-concept
2. **Biofuel production** — biodiesel, renewable diesel (HEFA), SAF
3. **Slaughterhouses and renderers** — beef/pork slaughter, rendering plants for tallow/lard/yellow grease/CWG
4. **Other fat & grease producers** — specialty rendering, by-product processors
5. **Used cooking oil (UCO) collectors** — yellow grease consolidators
6. **Food manufacturers** — major commercial users of vegetable oils, fats, sweeteners
7. **And beyond** — ethanol, sugar, wheat milling, cotton ginning, etc.

Each industry has a **different permit anatomy** — different emission unit names, different process equipment categories, different regulatory triggers. The parsing infrastructure must be flexible across all of them.

## Posture for this work
- **Quality over speed**: User explicitly said correctness matters more than throughput. Don't skip permits to hit a count target; if a permit is hard to parse, dig in and learn why.
- **Don't just patch regex**: When a filter misses a permit, the goal is to understand the *structure* of that permit, not to slap on another pattern. Understanding compounds across facility types; pattern-collecting doesn't.
- **Invest in EPA / state-agency understanding**: Read actual permits, learn what's in them, understand what we can leverage. The user wants this expertise built up systematically.
- **Build for re-use**: Whatever extraction pipeline we build for crushers needs to work, with minimal refactor, for biofuel plants and slaughterhouses.

## What to learn / document about permits
For each facility type, build a knowledge note covering:
- What sections appear in a typical Title V for that industry
- What equipment categories exist (e.g., for crushers: receiving / drying / dehulling / extraction / desolventizing / refining / boilers / loadout)
- What capacity units are typically used (tons/hr, bushels/day, MMBtu/hr, mil gal/yr, head/day)
- Common control devices (baghouses, scrubbers, RTOs, biofilters)
- Where rated capacity vs throughput limits typically appear in the document
- What the regulatory hooks tell us (e.g., NSPS Subpart applicability flags equipment type)

This becomes part of `domain_knowledge/` — a permit anatomy guide per industry.

## Current technical state (2026-04-30)
- Bronze schema live: `bronze.state_air_permits` + `bronze.state_air_permit_units` (mig 045)
- Extractor: `scripts/ollama/extract_titlev_permits.py` working but with biased recall
- Loader: `scripts/load_titlev_extractions_to_bronze.py` working
- See `project_state_air_permits_llm.md` for the technical pipeline status
- Iowa first-pass run hit ~17 permits; AGP-format permits underextracted (filter misses them); Cargill format works well

## Immediate next steps (after IA batch finishes)
1. Dig into an AGP permit (e.g., agp_emmetsburg_titlev.pdf) to understand why my filter missed it — not just the regex, but *what does an AGP Title V actually look like*?
2. Compare to a Cargill permit (which extracted well) to understand format differences
3. Document the format variations in a permit-anatomy note
4. Either fix the filter to handle both, OR build a per-format dispatcher
5. Re-run the full IA batch with --force
6. Sample-check 3-5 facilities manually for accuracy before extending

---

## project_phase_two_agent_architecture_detail

*(`project_phase_two_agent_architecture_detail.md`)*

---
name: Phase two agent architecture detail — equations + anomalies
description: User crystallized the facility agent concept — each agent is equations (financial models) modified by statistical anomalies (KG context). One template agent, facility-specific context. Includes VaR, rail car detection, draw areas, coverage tracking, fixed contracts.
type: project
originSessionId: 2a87c99e-d9e9-4822-83cb-2459eddd9173
---
**User's crystallization (2026-04-18):**

"The agent is really just some equations, potentially modified by statistical anomalies."

**Full picture:**
- Each facility agent = financial model equations (crush margins, procurement timing, coverage targets)
- Modified by statistical anomalies flagged by KG context (weather deviation, positioning extremes, policy changes)
- One template agent, copied per facility, differentiated by context from KG research
- VaR calculations per agent (David's work)
- Rail car detection (BioTrack AI) feeds flow topology + volume estimation
- Industry research needed: typical utilization rates, draw areas, coverage patterns, fixed contract relationships

**Browser Claude's 4-layer architecture validated:**
1. Strategic (quarterly, Claude Opus) — macro outlook, coverage strategy
2. Facility agents (daily, Ollama) — playbook execution with LLM for edge cases
3. Signal generation (continuous) — econometric forecasts + correlation scoring + KG anomaly detection
4. Aggregation (SQL) — facility decisions → industry flows → downstream industry inputs

**Cross-industry chain:** Crush → oil/meal → food manufacturing + biofuel refining, each with own agents. Agents discover equilibrium through bidding behavior.

**Staging:** Iowa soy crushers first (~17 facilities), validate against observed monthly crush, then generalize.

**Global ambition:** Replicate for every country in analysis. Same template, different context.

**Key quantitative parameters from browser Claude:**
- Utilization rates: 90-95% nameplate in strong margins, 60-75% in weak
- Feedstock draw: 100-150 miles truck, farther by rail
- Coverage windows: 2-4 months on crush, 6-12 months on biofuel feedstock with 45Z
- VaR bounds position sizing: 2σ rainfall deviation → adjusted yield → adjusted forward price distribution → adjusted VaR envelope → position size
- Rail car detection: (1) ground-truth facility throughput validation, (2) flow-graph discovery (inferred commercial relationships from observed patterns)

**Country replication swaps 3 things, keeps agent logic:**
1. Data collectors (CONAB/ANP for Brazil, INDEC/MAGyP for Argentina, MPOB for Malaysia)
2. Hedging venue + basis structure (BM&F vs CME, KLSE for palm)
3. Regulatory overlay (RenovaBio in Brazil, EU RED III for Europe)

**Critical insight:** Current spreadsheet work (seasonal distribution of annual projections) IS the agent baseline. Get seasonality right + capacity-weighted shares → agents have "expected state" against which anomaly detector operates. Phase one directly feeds phase two.

**Current foundation assessment (browser Claude):**
- 40-50% scaffolding done
- 5 gaps: facility state layer, catchment topology, price forecast pipeline, importance-weighted signals, decision-execution shell
- KG needs to grow from ~400 to ~few thousand nodes (mostly structural: facility nodes, catchment edges, downstream links)
- Analytical KG expansion needed for: facility-behavior patterns (integrated vs independent crushers, hedge strategies, inventory-cover patterns)

---

## project_phase_two_facility_agents

*(`project_phase_two_facility_agents.md`)*

---
name: Phase two facility agent architecture vision
description: Agent-based simulation of ag processing industry — one agent per facility, KG context, financial models, buy/sell decisions aggregated into industry activity forecasts. Described 2026-04-18.
type: project
originSessionId: 2a87c99e-d9e9-4822-83cb-2459eddd9173
---
**Vision (2026-04-18):** Each agricultural processing facility gets its own LLM agent that:

1. **Monitors inputs/outputs** — current prices vs forecasted prices from our models
2. **Receives KG context** — weather risks, crop conditions, policy changes, seasonal patterns enriched by the knowledge graph
3. **Makes procurement decisions** — buy soybeans today? yes/no based on:
   - How much of needs are already covered
   - How far current price is from expectations
   - What the periodic strategic plan suggests (quarterly/annual strategy)
   - Statistical scores for individual data points × their correlation with prices
4. **Captures transactions** — capacity utilization decisions, procurement timing
5. **Aggregates upward** — facility decisions roll up into monthly/annual industry activity estimates
6. **Feeds downstream** — soy crush output (oil, meal) becomes input for food manufacturing agents, biofuel facility agents, etc.

**Architecture questions to resolve:**
- Can desktop LLMs (Ollama, 30B models) run individual facility agents? (Likely yes — they follow structured financial model outputs, not generating novel analysis)
- More advanced LLM needed for: strategic plan generation, KG context synthesis, cross-industry impact assessment
- How detailed does the KG need to be? (Current 382 nodes likely sufficient for commodity-level context; need facility-level nodes for draw areas, logistics, capacity)
- What schema changes for facility agent state? (transactions table, coverage tracking, strategy docs)

**Current foundation (~50% done):**
- `reference.oilseed_crush_facilities` — 151 facilities (US/AR/BR) with capacity, location, status
- KG with 382 nodes, 260 edges, 211 contexts of analyst frameworks
- Feedstock allocation engine (`src/engines/feedstock_allocation/`)
- Balance sheet infrastructure (spreadsheets + DB)
- Seasonal norms in KG
- Price data in bronze (futures, cash)

**What to add:**
- Facility agent framework (agent per facility, state management)
- Financial model per facility (margin calc, procurement strategy)
- Draw area mapping (facility → geographic sourcing region)
- Transaction/decision capture schema
- Inter-industry linkage (crush output → refining input → biofuel input)
- Strategic plan generator (advanced LLM, quarterly cadence)

**Scope:** Not just crush — every ag processing industry (ethanol, flour milling, oilseed crush, rendering, biofuel production, food manufacturing).

---

## project_phase_two_vision

*(`project_phase_two_vision.md`)*

---
name: Phase two vision — forecasting and systematic analysis
description: User is building a vision for phase two beyond spreadsheet setup. Includes rail car tracker and systematic forecasting approach. ~50% of foundation work done.
type: project
originSessionId: 2a87c99e-d9e9-4822-83cb-2459eddd9173
---
User is developing a broader vision for phase two of the analysis system (2026-04-17).

**Current phase (phase one):** Get spreadsheets set up with correct data, verify links and formulas, ensure VBA updaters populate everything correctly.

**Phase two (upcoming):** 
- Start forecasting the spreadsheet numbers (human projections)
- Compare human vs LLM forecasts (symbiotic forecasting — see project_symbiotic_forecasting.md)
- Systematic approach to market analysis
- Rail car tracker (will take some time to build)
- ~50% of foundation work already done

**How to apply:** Stay focused on phase one (spreadsheet correctness) until user signals transition. Don't get ahead of phase two work. The runway between phases will be used to verify projections work well before layering on LLM forecasting.

---

## project_plant_intelligence

*(`project_plant_intelligence.md`)*

---
name: project_plant_intelligence
description: Plant Intelligence Agent - systematic facility data collection across all US processing plants
type: project
---

**Goal:** Build and maintain a comprehensive database of every US agricultural processing plant — ethanol, oilseed crush, biodiesel/RD, flour milling — with capacity, equipment, ownership, and operational status.

**Existing infrastructure:**
- `bronze.epa_echo_facility` — 200+ facilities from EPA ECHO (SIC 2075/2076/2869/2911/2992/2041-2046)
- `bronze.permit_capacity` — Title V permit-derived capacity (Iowa complete, NE/IL/IN partial)
- `bronze.permit_emission_unit` — Equipment-level detail from permits
- `bronze.eia_capacity_monthly` — EIA monthly biofuel aggregate capacity
- `bronze.eia_capacity_raw` — EIA plant-level roster (schema ready, data needed)
- `gold.facility_capacity`, `gold.state_crush_capacity`, `gold.crush_capacity_ranking` — analytics views
- State collectors: Iowa (complete), Nebraska (implemented), Illinois (Selenium), Indiana (draft)

**Data source hierarchy (by reliability):**
1. **EPA ECHO** — Federal, no auth, 200+ facilities with SIC/NAICS, permits, lat/long
2. **EIA plant rosters** — Federal, nameplate capacity, fuel type, year online
3. **State Title V permits** — Highest detail (equipment-level), but each state has different portal
4. **RFA/NBB/NOPA directories** — Industry association plant lists
5. **State environmental databases** — Air permit emissions inventories
6. **SEC/corporate filings** — Capacity announcements, expansion plans
7. **Local news/planning commission** — New construction, expansions, closures
8. **USDA GIPSA** — Grain inspection volumes by facility (proxy for throughput)

**Priority expansion states:**
- Tier 1 (highest processing concentration): Iowa, Illinois, Indiana, Nebraska, Ohio, Minnesota
- Tier 2: Kansas, Missouri, North Dakota, South Dakota, Texas
- Tier 3: Wisconsin, Michigan, Arkansas, Tennessee, Georgia

**DCO-specific sources for ethanol plants:**
- RFA plant map (public) — lists all ethanol plants with capacity
- EPA ECHO with SIC 2869 — already collected
- State environmental permits — corn oil extraction equipment identified by emission unit descriptions
- Corporate press releases — DCO extraction system installations
- USDA Grain Transportation Report — ethanol plant rail traffic patterns

**Why:** Accurate plant-level data enables: (1) DCO production estimates, (2) crush capacity utilization, (3) feedstock competition modeling, (4) new capacity impact analysis.
**How to apply:** Build the Plant Intelligence Agent as a scheduled job that continuously queries sources and enriches the master registry.

---

## project_public_filings_extraction

*(`project_public_filings_extraction.md`)*

---
name: Public filings extraction for facility operators
description: SEC EDGAR + earnings transcripts for publicly-traded operators in our facility list. Adds operator-specific context to national-event responses.
type: project
originSessionId: 4f606138-9604-4e05-9a04-1c081995d736
---
User flagged 2026-05-06 — Sprint 6 or 7 work. Don't lose the thought.

## Why this matters

Many operators in `reference.facility_master` are publicly traded
and produce regular SEC filings + earnings calls that contain
facility-specific context: capacity utilization, expansion CapEx,
margin compression callouts, regional commentary, force majeure
events. None of this is in our news pipeline because it's not RSS.

Adding this layer makes our response to national events much more
specific — instead of "RFS proposal moves veg oil sentiment +0.3
across all crushers," we can model "ADM 10-K signals Cedar Rapids
crush margin compression in Q3 → AGP and Cargill nearby plants
likely face same pressure → sentiment shift concentrated to Eastern
Iowa cluster, not uniform statewide."

## Publicly-traded operators in our current IA facility list

| Operator | Ticker | Filings relevant |
|---|---|---|
| ADM (Archer-Daniels-Midland) | NYSE: ADM | 10-K, 10-Q, 8-K, earnings transcripts |
| Bunge | NYSE: BG | 10-K, 10-Q, 8-K, transcripts |
| Tyson Foods | NYSE: TSN | 10-K, 10-Q, 8-K, transcripts |
| JBS USA | NYSE: JBS (US-listed 2024) | 20-F (foreign), 6-K, transcripts |
| Hormel Foods | NYSE: HRL | 10-K, 10-Q, 8-K, transcripts |
| Chevron Corp | NYSE: CVX | 10-K, 10-Q, 8-K, transcripts (REG segment commentary) |
| Valero Energy | NYSE: VLO | 10-K, 10-Q, 8-K, transcripts (Renewable Fuels segment) |
| Green Plains | NASDAQ: GPRE | 10-K, 10-Q, 8-K, transcripts |
| Seaboard Corp | NYSE: SEB | 10-K, 10-Q, 8-K (parent of Seaboard Triumph) |
| Smithfield Foods | NASDAQ: SFD (2025 IPO) | 10-K, 10-Q, 8-K |
| WH Group | HKEX: 0288 (Smithfield parent) | annual report (Cantonese/English) |

**Private (not in this scope):**
POET, Cargill, Christensen Farms, Iowa Select Farms, Versova,
Smithfield (private until 2025 IPO), most coops (Landus, Heartland,
NEW). For these we rely on news pipeline + facility news monitoring.

## Filings worth extracting

**Highest signal (tier 1):**
- **8-K material events** — facility closures, acquisitions,
  expansions, fires, regulatory orders, force majeure declarations.
  These are the per-facility news we currently miss because RSS
  doesn't surface them quickly.
- **Earnings transcripts** — forward-looking commentary on capacity
  utilization, regional margins, segment outlook. Often facility-
  specific (e.g., "our Cedar Rapids complex is running at 92%").
- **10-K MD&A and segment reporting** — annual capacity, throughput,
  margins per business unit. Calibration-grade ground truth for
  our capacity values.

**Medium signal (tier 2):**
- **10-Q updates** — quarterly margin and volume data; less
  facility-detail but still useful.
- **Investor day presentations** — long-form forward-looking
  guidance; usually segment-level not facility-level.

**Low signal (tier 3, optional):**
- **Proxy statements (DEF 14A)** — executive compensation, governance.
  Mostly not market-relevant unless tied to specific facility KPIs.
- **Form 4 insider trading** — sometimes signals confidence shifts but
  noisy.

## Implementation sketch

### Phase 1: SEC EDGAR collector

`scripts/collect_sec_filings.py`:
- Accepts list of CIKs (already known for major operators)
- Pulls all filings of types 10-K, 10-Q, 8-K, DEF 14A
- Free SEC EDGAR API (no key required)
- Persists to `bronze.sec_filing_raw` with file_url, form_type, filing_date,
  CIK, period_of_report, raw_html

### Phase 2: Filing parser

`scripts/parse_sec_filings.py`:
- For each new filing, extract:
  - Filing metadata (form, date, period)
  - Item-level breakdown (8-K Items 1.01, 2.01, 7.01, 8.01 are
    most useful for facility events)
  - MD&A text from 10-K/Q
  - Risk factors (signal for tail-risk awareness)
- Persists to `silver.sec_filing_parsed`
- Strips HTML/XBRL formatting

### Phase 3: Classifier integration

Hook the existing news classifier (`scripts/classify_news_articles.py`)
into SEC filings. Each 8-K item becomes one "article" for classification.
Each earnings transcript becomes one with high source_weight (Drew-
level credibility). The classifier already produces topic + facility
relevance + polarity, all of which feeds the Market Field update loop.

### Phase 4: Earnings transcript ingestion

Earnings transcripts are NOT in EDGAR. Three options:
- **Seeking Alpha API** — paid (~$30/mo), wide coverage
- **Company IR pages** — most majors host transcripts; per-source
  scraping needed
- **Refinitiv / FactSet** — enterprise pricing
- **The Motley Fool transcripts** — free for some companies

Recommend Seeking Alpha for PoC ($30/mo is cheap relative to value).
Probably revisit once SEC filings prove the pipeline works.

### Phase 5: Auto-link to facilities

After classification, the NLP needs to do facility-attribution:
"AGP Sergeant Bluff" → `ia.agp_sergeant_bluff`. The classifier
already does this for news; needs same logic applied to filings.
For company-wide filings without facility specifics, attribute the
sentiment evenly across all facilities owned by that operator.

## Cost estimate

- **SEC EDGAR collector**: free
- **Parsing 10-K/Q/8-K with Claude Sonnet 4.6**: ~$0.50-2.00 per
  10-K (long doc), ~$0.05-0.20 per 8-K (short)
- **Annual filing volume per operator**:
  - 1 x 10-K, 4 x 10-Q, ~20-50 x 8-K, 1 x DEF 14A = ~30-60 filings
- **For 10 publicly-traded operators × 50 filings × $0.30 avg**:
  ~$150/year ongoing cost
- **Backfill 5 years of history × 10 operators**:
  ~10 * 5 * 60 * $0.30 = $900 one-time

**Total: ~$1,000 backfill + $150/year ongoing.** Same trivial-
spend story as everything else. The unfair part remains the
quality you get for the cost.

## When to do this

Sprint 6 or 7. Should come AFTER:
- Multi-industry facility seed expanded to majority of state
- Air-permit extraction running on at least ethanol + pork packing
- Drew Lerner backfill done (so we have weather context)

Reason: SEC filings are a CONTEXT LAYER on top of the operational
data. Without operational data (capacity, location, crush margins),
the filings don't have anchor points. Build operational substrate
first, layer filings on top.

## Specific use cases this unlocks

1. **Earnings call commentary on Iowa Falls** → instant sentiment
   refresh for Cargill cluster facilities, not waiting for news
   to pick it up
2. **Tyson 8-K announcing Storm Lake outage** → immediately drives
   meal demand sentiment + competitor opportunity for adjacent
   pork packers
3. **Bunge 10-K segment guidance on crush margins** → calibrate
   the oil_share weighting against management's stated outlook
4. **Chevron commenting on REG margins** → biodiesel sector signal
   that propagates to all Iowa biodiesel facilities

## Integration with Market Field

Each parsed filing item becomes one row in
`bronze.market_field_signal` (extension of existing `bronze.news_article`
or new table) with:
- source_type = 'sec_filing' / 'earnings_transcript'
- operator_kg_key (which company)
- facility_relevance_keys (which facilities mentioned or
  attributed)
- topic_scores + polarity + intensity (from classifier)
- source_weight (high — these are credible disclosures)

Then the existing daily update loop pulls these signals into the
sentiment update equation alongside news. No additional math.
Just another input source.

---

## project_quarterly_var_risk_budget

*(`project_quarterly_var_risk_budget.md`)*

---
name: project_quarterly_var_risk_budget
description: "Quarterly VaR risk-budget subsystem for BBD feedstock procurement — schema, engine, and how it fixes the allocator whipsaw"
metadata: 
  node_type: memory
  type: project
  originSessionId: 6bafe082-23e1-48f1-9b65-b85116500a57
---

**Started 2026-07-15.** The `risk` schema + engine that stops the facility allocator from taking corner solutions (100% highest-margin feedstock, switching SBO→DCO monthly — "not something a smart or solvent producer would do," per Tore). This is the first build of the wider facility VaR product (IFVS-003, eventually David's) — deliberately crowbarred in now.

**Model (covered/open procurement):** each BBD plant buys ahead ≥3 months (NOT spot; spot is rare, signal-triggered by a current-vs-forecast price gap). Book = covered (locked at the contracted anchor mix, no price VaR) + open (margin-optimized under a VaR cap). VaR budget forces a diversified book because a concentrated position is high-VaR — kills the whipsaw as a *side effect of being run right*, not a hardcoded mix cap.

**Built (3 commits, all validated):**
- `scripts/risk/build_risk_foundation.py` → `risk.feedstock_volatility`, `risk.feedstock_correlation`, `risk.facility_budget_config`. Vol/corr from **silver.feedstock_supply** (the allocator's own curated price), NOT raw bronze.feedstock_prices (a multi-region/margin grab-bag → 400% nonsense vols). Returns winsorized ±0.5. SBO 26.6%, fats 22-33%, canola 15%; SBO~canola +0.71. Grades EBFT/IBFT→BFT price, YG→UCO, CSO flat.
- `src/engines/risk_budget/var_optimizer.py` → `optimize_quarter()`. Parametric var-cov VaR = z·√h·√(P'CP). scipy SLSQP; min-VaR floor fallback when cap tighter than frontier. Known v1 limit: SLSQP local optima, margins wobble ~3%.
- `scripts/risk/generate_quarterly_budgets.py` → `risk.facility_quarterly_budget` + `risk.facility_coverage_actual`. 152 budgets across biodiesel/RD/SAF/coprocessing.

**Key params (all facility-overridable — "anchors are parameters, not code"):** VaR budget default 8% of quarterly notional, 95%, 1-quarter. Coverage ladder default 80/55/30/10 by quarters-ahead. Per-facility override via `facility_budget_config.coverage_override_pct` + `var_budget_pct`, `source`-tagged — a calibrated real number from a facility (rare; crushers are secretive) drops in by UPDATE. Do NOT block on getting real coverage.

**Why this thread started:** non-biofuel allocation → SBO/tallow flat files missing coprocessing+SAF → allocator whipsaw. Coprocessing regression now fixed AT SOURCE: SBO appears via VaR, not a hardcode. See [feedback_gate_beats_parameter] (chose the mechanism over the per-fuel patch).

**Still TODO (next session):**
1. Allocator integration — phase the committed quarterly book across months (edit `src/engines/feedstock_allocation/allocator.py` allocate_month; the risky central change, do carefully). Config already has unused `max_monthly_switch_pct=0.20`, `contract_share=0.60` — the designed seam.
2. Coverage accounting — populate `facility_coverage_actual`, add `gold.coverage_vs_budget` view.
3. Re-run allocator + re-rake (`rake_feedstock_vintage_aware.py`) + regenerate flat files → SBO coprocessing + cross-feedstock SAF flow to balance sheets.
4. THEN the demand-breakout category spec (SAF/coprocessing as 2 separate lines; non-bio components — see below).

**Facility-data bugs surfaced (route to FFA calibration queue, NOT engine bugs):** BP Stage 1 nameplate=2018.0 MMgy (year miskeyed → 3.3B lb). BP Stage 2 eligible has 'CAN'/'CAR' typos (should be CO).

Related: [project_feedstock_forecast_method], [project_symbiotic_forecasting], [feedstock demand breakout — SBO Census M311K 9-category non-bio (baking/margarine/salad/other edible + fatty acids/paint/resins/soap/other), tallow CIR census_cir_fats; components measured ≤2011 then MODELED_SHARE forward, lauric pattern].

---

## project_reg_ralston_madison_idle

*(`project_reg_ralston_madison_idle.md`)*

---
name: REG Ralston IA + REG Madison WI idled March 2024
description: Chevron REG idled two biodiesel plants in March 2024 — Ralston IA (30 mmgy) + Madison WI (20 mmgy) — 50 mmgy combined offline; restart optionality preserved
type: project
originSessionId: 4f606138-9604-4e05-9a04-1c081995d736
---
# REG Ralston (IA) + REG Madison (WI) idled March 2024

Fact: Chevron Renewable Energy Group idled both plants indefinitely effective March 1, 2024.

**Why:** Weak biodiesel volumes under federal RFS (RVO too low for production
economics on these specific configurations). Combined 50 mmgy offline.

**How to apply:** When asked about US biodiesel capacity, RFS/RVO impact, or
Chevron's biofuel footprint, these two are CLOSED-in-practice but still on
Chevron's books (no goodwill impairment per FY2024 10-K — restart optionality
preserved). Don't include them in active capacity counts.

## Discovery trail (the methodology that worked)

The discovery process is reusable for other operators where SEC filings don't name plant changes:

1. **Chevron 10-K FY2024** said "two refineries idled in 2024" but did NOT
   name them (Chevron treats individual plant idlings as below-disclosure-
   threshold material events).

2. **CARB LCFS pathway absence** was the smoking gun. Pulled `current-pathways_all.xlsx`,
   filtered to 892 liquid-biofuel pathways. Every active REG production plant
   has ≥3 CARB pathways:
   - REG Albert Lea MN: 31, Seneca IL: 30, Newton IA: 23, Danville IL: 22,
     Mason City IA: 19, Grays Harbor WA: 6, New Boston TX: 3
   - REG Ralston IA: ZERO. REG Madison WI: ZERO.

3. **Industry press** (March 2024) confirmed:
   - Biodiesel Magazine "Chevron REG idles Ralston, Madison facilities"
   - Oil & Gas Journal "Chevron to shutter two US biodiesel plants"
   - Carroll Broadcasting (Iowa local): 24 jobs lost at Ralston

## Reusable signal

**CARB pathway absence is a strong "not running OR not shipping to CA" signal**
for any biofuel facility in our DB. When a US biodiesel/RD/SAF plant has zero
current CARB pathways while peers have many, it's worth investigating closure
status. The detection cost is one xlsx download + one query.

Scripts that produced this finding:
- `scripts/extract_carb_lcfs_pathways.py` — extracts 892 pathways → JSON/CSV
- `scripts/xref_carb_vs_db_biofuel.py` — cross-references CARB vs DB facility tables

## Plant context

**REG Ralston IA, 30 mmgy** — soy/multi-feedstock biodiesel. Opened 2002 as
THE FIRST biodiesel plant REG ever built. Foundational asset historically. 24
jobs eliminated at idling.

**REG Madison WI, 20 mmgy** — smaller capacity, less-discussed plant.

## Open questions

- Will Chevron's 45Z + RIN economics + IRA biofuel policy shifts bring these
  plants back online in 2026-2027? Goodwill preservation says they're hoping so.
- REG Emporia KS, REG Houston TX, REG New Orleans LA — also no CARB pathways.
  Probably terminals or specialty (not full BD production), but worth confirming
  via Chevron facility list disclosure.

## Migration

`database/migrations/072_reg_ralston_madison_idled_2024.sql` applied 2026-05-10.

---

## project_rlc_2026_mandates

*(`project_rlc_2026_mandates.md`)*

---
name: project-rlc-2026-mandates
description: "Tore's three year-long mandates for RLC, locked 2026-06-08. The daily-three discipline is scored against these."
metadata: 
  node_type: memory
  type: project
  originSessionId: 05e2ef6d-523f-437f-8858-9be18d5d0aeb
---

Locked 2026-06-08. The daily-three discipline frames every day in terms
of progress against these three mandates.

**Mandate 1 — Balance sheets / Excel work.**
Ship production-quality S&D balance sheets across commodities. Current
sub-target: finish US oilseed balance sheets, then expand to grains and
biofuels per the priority queue in MEMORY.md. Output is the spreadsheet
stack that clients and internal modeling rely on.

**Mandate 2 — Feedstock Facility Agent (FFA) network.**
Build out the per-facility agent network that turns site-level data
(air permits, CARB pathways, EIA filings, news, weather) into real-time
allocation and capacity signals. Iowa is the first sprint. Naming pun
acknowledged — FFA is also the BBD feedstock spec for free fatty acids.
The double-meaning is intentional; Tore likes it.

**Mandate 3 — Be a better leader for RLC.**
Continuous personal-improvement bucket. Habit system, not a deliverable.
End-of-year measure is trajectory, not shipping. Tracks (active not
passive — meeting the bar, not just showing up):

- **Speaking** — Toastmasters participation (speech delivered, Table
  Topics, presentation given). NOT attended-only. *This-week target:
  join a chapter.*
- **Fitness** — Workout completed at target intensity per
  `docs/daily_log/fitness_routine.md`. NOT brief stretch or Freddie walk
  alone (Freddie walk is daily baseline, not Mandate 3 advance).
- **Writing** — Substantive analytical writing (market call, framework
  refinement, methodology piece). Not Slack-tier reactivity. Daily
  6 AM ET Google Calendar reminder is the trigger.
- *(More tracks to add as Tore identifies them — reading, sleep,
  learning are likely candidates.)*

**Why this structure (per 2026-06-08 conversation):**
Mandates 1 and 2 are ship-shaped. Mandate 3 is a habit system that
holds the personal-development side inside the work system rather than
treating it as separate. The "active not passive" bar across Mandate 3
tracks prevents the discipline from collapsing to "anything counts."
Tore's exact phrasing: attending Toastmasters wouldn't count; giving a
presentation or participating in Table Topics would.

**Calibration target:** ~70-85% hit rate on the daily three. Consistent
100% means the goals are too easy (per Tore 2026-06-05 calibration in
[[daily-three-discipline]]).

**Daily operations:** see [[daily-three-discipline]] for the morning
meeting structure (yesterday retro + today's 3 + schedule + closing
ritual). Daily logs at `docs/daily_log/YYYY-MM-DD.md`.

---

## project_roadmap_master

*(`project_roadmap_master.md`)*

---
name: Master roadmap — RLC Agent product to commercial release
description: Sprint-shaped roadmap with North Star, rabbit-hole filter, and update cadence. Always-on reference for staying out of distracting rabbit holes.
type: project
originSessionId: 4f606138-9604-4e05-9a04-1c081995d736
---
## Product vision (one paragraph)
A facility-level intelligence platform for agricultural processing — every
facility modeled as an agent running on real permit data, real economics
(basis field + crush margins), and a strategic plan. Three monetization
vectors: (1) subscription data product (multi-commodity basis field +
facility intelligence dashboard), (2) per-facility deep-dive reports
(prospecting + customer engagement), (3) custom consulting backed by the
platform (force multiplier on RLC's existing service line).

## North Star — the one test
**Does this materially shorten the path from a prospect's first look to
a paying contract?**

If the work doesn't either (a) make the demo more compelling, (b) make
the data more defensible, or (c) make the workflow more reliable, it's a
side quest by definition.

## Rabbit hole filter — five questions before going deep
1. Does it enable a capability we don't have, or refine one we do? Refinement = side quest unless something is blocked on it.
2. Does it compound across the platform? A new layer (basis field, permits) compounds. A new dashboard tile usually doesn't.
3. Could a prospect see the difference in 30 seconds? If no, why are we doing it now?
4. Is something specifically blocked, or am I inventing a need? Be ruthless here.
5. If I skipped it entirely, what breaks? If nothing concrete breaks, defer.

Pass at least 2 of 5 → strategic. Pass 1 or 0 → defer.

---

## Current state (locked at 2026-05-02)
- **20 facilities** (17 IA + 3 IN) with full equipment data in `bronze.state_air_permits` + `state_air_permit_units`. 434 emission units extracted via Title V LLM pipeline.
- **Basis field v1 live**: 4 commodities (soybeans/corn/wheat/sorghum), 6,137 spatial samples, ~14,000 grid cells across Corn Belt + Mid-South + Plains via IDW interpolation.
- **Eagle Grove showcase end-to-end**: hero with satellite + basis surface, live board crush margin, local Eagle-Grove crush margin, 52-unit emission flow Sankey, equipment table, throughput history, data provenance.
- **Standalone basis field viewer** at `dashboards/basis_field/app.py` with multi-commodity / multi-region / uncertainty hatching / contour lines / color-mode dropdown.
- **Process flow ontology** drafted for crushers; 16 industries indexed in `domain_knowledge/process_flows/README.md`.
- **Two-GPU pipeline live**: 5080 desktop (rlc-server) + 4060 laptop (rlc-0001) via Tailscale.
- **3 known-failure permits** queued for chunked-extraction work (Cargill 57004, ADM Frankfort, Cargill Lafayette).
- **0 paying customers yet** — uncomfortable but the right metric.

---

## SPRINT 1 — Next 4 weeks (commitment zone)
End state: a credible "facility with forward book + strategic plan + crush margin curve" demo on three facilities. **A sellable artifact.**

| Week | Goal | Primary deliverable |
|---|---|---|
| 1 | Forward curve foundation | Re-parse `delivery_period`; forward basis at K/N/Q/U/V/X/Z; smoke test in viewer |
| 2 | Forward curve viz + strategic plan schema | Spline curves at Eagle Grove; `silver.facility_strategic_plan` table; forward curve chart in dashboard |
| 3 | Position + P&L | `facility_purchase_commitment`, `facility_position`, `facility_pnl_attribution`; mock Eagle Grove book; daily agent loop reads them |
| 4 | Two more facilities + chunked extraction | Cargill Iowa Falls + AGP Sergeant Bluff dashboards; chunked-extraction MVP; resolve 3 known-failure permits |

Sprint 1 background tasks (always-on GPUs):
- 4060: dscan archive embedding (week 1 day 1) → IL/NE permit extraction (weeks 2-4 as PDFs become available)
- 5080: interactive when actively used; otherwise idle (vision tasks queued, possible chunked-extraction prototyping in W4)

---

## SPRINT 2 — Weeks 5-12 (themes, not days)

Each requires Sprint 1 complete. Order can adjust by the prospect calendar.

- **Layer 1 strategic agent** (Claude Opus, quarterly) writes the strategic plan instead of mock; replaces the placeholder in Eagle Grove tab
- **Facility identity premium** (Layer 2 of basis field): empirically anchor +Δ¢ per facility from AMS Mills/Processors gap, split by years operating + co-op flag + NOPA flag + scale
- **Competitive uplift** (Layer 3): network effect of nearby crushers on shared draw area
- **Indiana state expansion** at facility level (5 PDFs already downloaded, 3 already extracted)
- **Daily auto-refresh** of basis field via Windows scheduled task (currently manual recompute)
- **One adjacent industry**: pick biofuel OR slaughter/render — whichever has clearer prospect interest. Build process flow ontology + state air permit collectors
- **Forecast accuracy tracking** infrastructure — bootstrap from the rat-hole's Forecast Measurement archive (196 daily CSVs)

---

## FAR-TERM — Quarters 3-4+ (vision, lower resolution)

- Full industry expansion: biofuel + slaughter + render + fats/greases + UCO + food manufacturing
- International: Canada first (existing CGC/StatsCan collectors), then Brazil (CONAB/IBGE done)/Argentina (INDEC/ComexStat done)
- Real-time anomaly detection on the basis field (current observations vs interpolated expectation)
- Symbiotic forecasting end-to-end (LLM forecasts every monthly series in parallel to spreadsheets, reconciled against realized data)
- Self-updating ontologies as new permits ingest

---

## Update cadence
- **Weekly** (Friday Notion update): progress against Sprint 1 commitments + remaining-work estimates
- **Sprint boundary**: full re-evaluation of next sprint based on what we learned
- **Trigger updates**: any architectural insight (like the three-layer basis model, 2026-05-02) gets a delta entry — never lose load-bearing realizations

---

## Honest reads on the plan
- Four-week Sprint 1 is *aggressive*. Forward curves alone is real work. The "two more facilities" in W4 may slip to Sprint 2 if W2-W3 take longer than estimated.
- "0 paying customers" is uncomfortable but it's the right metric — optimize the path to that going to 1, not codebase aesthetics.
- North Star test is intentionally simple. If we make it more complicated we'll game it.
- Layer 2 (facility identity premium) might justify pulling forward into Sprint 1 if we have time — it's what makes the prospecting demo lethal.

## Open questions for Tore (post-walk 2026-05-02)
1. Is the North Star the right test, or is "internal use makes RLC consulting better" a separate first-class objective? **PENDING**
2. Are there real prospects on the calendar that should anchor the demo target date? **ANSWERED 2026-05-02: June 4 conference. ~33 days out. Sprint 1 must complete by June 1 to leave a polish week.**
3. Sprint 1 priorities — anything to swap? **PENDING**
4. Anything in queued memory (AGP scrape, OCR, IDEM filter, Layer 1 agent) to promote into Sprint 1? **PENDING**
5. Hiring / team dimension — left off this draft entirely; should it be in here? **PENDING**

## Locked target date — June 4 conference demo
This is the binding deadline for Sprint 1. Working backward:
- **June 1** = sprint-end, all Sprint 1 deliverables shipped
- **May 26-31** = polish week (visual refinement, demo rehearsal, edge-case fixes)
- **May 4-25** = Sprint 1 weeks 1-3 (forward curve, strategic plan, position/P&L)
- **May 26** = chunked extraction + second facility (W4) deferred to post-conference if necessary
The demo audience is conference attendees — likely industry/analyst-heavy. The visual must be defensible and look like infrastructure, not a prototype.

---

## project_roadmap_oilseeds_grains

*(`project_roadmap_oilseeds_grains.md`)*

---
name: project_roadmap_oilseeds_grains
description: Spreadsheet updater rollout sequence - oilseeds then fats/greases then grains
type: project
---

Spreadsheet updater rollout sequence (as of 2026-03-14):
1. **Oilseeds** (in progress): soybeans done, canola done, cottonseed in progress, then remaining (corn oil, sunflower, palm, peanut)
2. **Fats & greases**: tallow, lard, CWG, yellow grease, poultry fat — NOT yet in NASS collector
3. **Biofuels**: may come before grains depending on priority
4. **Food & feed grains**: wheat (flour milling), corn (grain crush/ethanol), sorghum

**Why:** User expects oilseed lessons learned to accelerate grains rollout.
**How to apply:** When building grain updaters, replicate the header-matching pattern from FatsOilsUpdaterSQL. Ensure NASS collector configs and crush_attribute_reference rows are added for each commodity BEFORE the user tries Ctrl+U.

---

## project_saf_research_notes

*(`project_saf_research_notes.md`)*

---
name: SAF research notes — TexChem (?) facility and EIA SAF stocks gap
description: Open research items on US SAF production/trade tracking
type: project
originSessionId: f9769538-03ad-48d4-8472-58bcfe06d704
---
## Texas facility: "TexChem or something" (per Tore, 2026-05-14)

Tore mentioned a Texas facility — name fuzzy, possibly TexChem or similar —
that:
- Imports renewable diesel from the Netherlands
- Converts the RD into SAF (via further hydroprocessing)
- Has a CARB LCFS pathway (or pending)
- Generates D4 RINs per the pathway, NOT the typical SAF fuel code RIN

**Searched CARB pathways for TX SAF/AJF: zero hits.** The certified AJF set
is AltAir Paramount (CA), Phillips 66 Rodeo (CA), REG Geismar (LA), and
Neste Porvoo (Finland). No Texas AJF pathway in our snapshot.

The facility may be:
- A smaller producer not yet CARB-certified
- Named differently (Tore's "TEXCHEM or something" was fuzzy recall)
- Operating under a different fuel classification

When confirming the name, look for: Texas TX, importer of Netherlands RD,
SAF/AJF output, D4 RIN generation by pathway exception.

## EIA SAF stocks gap

EIA does not currently publish SAF stocks separately in
`bronze.eia_monthly_biofuels`. Production is observable via EMTS (some fuel
codes include SAF). Stocks for SAF specifically are not in our current
ingestion.

Tore is setting up balance sheets as if data exists. Placeholder estimates
acceptable until either EIA publishes SAF stocks separately or the rail-car
tracker provides facility-level inference.

## Co-processing trade flows / stocks

Per Tore: estimable at facility level once rail-car tracker is operational.
Will not be as accurate as direct measurement but adequate for now. Defer
implementation until rail-car infrastructure is up.

## Price-based BD/RD/COP/SAF discriminator (future signal)

Tertiary signal on top of country-share heuristic in `gold.biofuel_trade_split`.
- `bronze.census_trade` has `value_usd` and `quantity` per record
- Implied unit price = value_usd / quantity (in source units)
- Different products have meaningfully different ranges:
  - BD: $3-5/gal historically
  - RD: $4-6/gal
  - SAF: $6-10/gal
- Within HS 3826.00.10 (≥50% biodiesel), unit price could discriminate
  neat BD vs RD vs SAF cargoes (signal usable)
- Within HS 3826.00.30 (<50% biodiesel), petroleum dominates the price so
  signal is weak (skip)
- Most useful for sorting Netherlands shipments (currently 25% BD / 75% RD
  per default rule; spans BD + RD + RD-to-SAF feedstock).

Not implemented yet (2026-05-14). Use as cross-check refinement when
country rules need calibration beyond what export-side EIA anchor provides.

---

## project_saf_trade_tracking

*(`project_saf_trade_tracking.md`)*

---
name: SAF trade-flow identification — price-threshold heuristic
description: How gold.saf_trade_candidates identifies SAF cargoes in Census data, what's known to work and what's known to false-positive
type: project
originSessionId: f9769538-03ad-48d4-8472-58bcfe06d704
---
## Approach

US SAF imports/exports don't have a clean HS code. Real bulk volumes are tiny
(<500k gal/yr) hidden inside larger HS 3826 (biodiesel) and HS 2710 (petroleum)
flows. `gold.saf_trade_candidates` identifies SAF cargoes by combining:

1. **Price floor**: SAF historically prices $6-15/gal (Argus Northwest Europe).
2. **Price ceiling**: $25/gal — excludes specialty/sample products that
   would otherwise mis-trigger (Korea 2710.20.25 routinely at $310/gal).
3. **Volume cap**: 300,000 gallons per (year, month, country, hs_code)
   record. Critical — real 2024 SAF cargoes are <50k gal/record. Without
   the cap, 2022 bulk Canada BD exports (17-25 MMgal/month at $6-8/gal
   during Ukraine-war price inflation) falsely trigger.

## Thresholds (mig 090)

| HS code | $/gal range | Vol cap | Signal name |
|---|---|---|---|
| HS 3826.x | $6-25 | <300k gal | SAF_3826_premium |
| HS 2710.19.11 (jet kerosene) | $5-20 | <300k gal | SAF_jet_premium (empty in practice) |
| HS 2710.20.x | $8-25 | <300k gal | SAF_blend_premium |

HS 2710.19.11 has zero records in our data — Census doesn't return data
for that code despite it being in HTSUS. Real jet fuel trade flows under
HS 2710.19.16 instead.

## 2024 reality check

- SAF imports identified: **290k gal** (Belgium 205k, France 296k annual,
  Malaysia 50k, Germany 4k, smaller from NL/Spain/Italy)
- SAF exports identified: 80k gal
- US SAF domestic production (EMTS): **~38 mil gal/yr** and growing fast
  (Dec 2025 = 44 mil gal/month single month)

Imports are 0.8% of US SAF supply. Domestic production dominates — same
pattern as RD.

## Known limitations

1. **False positives during high-fuel-price periods** are filtered by the
   volume cap, but if the 2022-style price inflation recurs alongside
   real growing SAF volumes, the cap may need re-tuning.
2. **Doesn't catch SAF that shipped at "normal" BD prices** — if a SAF
   producer chose to discount cargoes below $6/gal, we wouldn't see it.
   Unlikely given the 45Z + ReFuelEU price support floors.
3. **Doesn't separate HEFA-SAF from other SAF pathways** — all flagged
   cargoes are just "SAF candidate."

## Future tracking (Tore's vision, 2026-05-16)

The price-threshold view is the v1 anchor. The endpoint is the rail-car
tracker: see a tanker car leaving an identified SAF producer (e.g., World
Energy AltAir Paramount CA, REG Geismar LA, or the TX RFS-listed facility
Tore mentioned), follow it to a Gulf export terminal, tie it to the monthly
export records here for final confirmation. The view's country detail
preserves the spatial anchor needed for that integration.

## File pointers

- View: `gold.saf_trade_candidates` (mig 090)
- Workbook: `output/balance_sheet_templates/us_saf_bal_sheets.xlsx`
- Populator: `src/tools/populate_biofuel_bal_sheet_trade.py` — handles SAF
  via fetch_trade_saf() + fetch_saf_production() (the latter pulls
  gold.us_liquid_fuel_production_monthly.saf_kgal directly).

---

## project_safflower_discontinuation

*(`project_safflower_discontinuation.md`)*

---
name: Safflower data discontinuation
description: USDA stopped publishing safflower crush/oil/meal data. User assessed it as a rounding error (~75M lbs oil) but may revisit for completeness later.
type: project
originSessionId: 2a87c99e-d9e9-4822-83cb-2459eddd9173
---
**CORRECTION (2026-04-17):** User initially confused safflower with flaxseed/linseed. Safflower data IS being collected by our NASS collector. The limited-data issue applies more to flaxseed/linseed than safflower.

**Why:** Total US safflower supply ~200M lbs seed → ~100M lbs meal, ~75M lbs oil. For context, 75M lbs of veg oil is a rounding error relative to soybean oil (~25 billion lbs).

**How to apply:** Not worth active development time now. If revisited later:
- Historical estimates available through 2012/13 marketing year
- Seed production still published by NASS
- Census trade data available via HS codes (imports/exports)
- Could estimate crush, meal/oil production, and inventories from seed production + assumptions
- Sunflower crushers can likely handle safflower (similar seeds)
- User has an old spreadsheet with historical tracking data
- File `us_safflower_balance_sheets` in Models folder — user stopped working on it 2026-04-17

**Decision (2026-04-17):** Deferred. May pick up later for completeness but not a priority.

---

## project_sbe_analysis

*(`project_sbe_analysis.md`)*

---
name: project_sbe_analysis
description: Soybean-equivalent export analysis framework and long-term visualization vision
type: project
---

**Soybean-Equivalent (SBE) Export Framework:**
Calculate total crop exported (in soybean-equivalent terms):
- SBE exports = bean exports + (meal exports / meal yield rate) + (oil exports / oil yield rate)
- Typical yields: meal ~79.5% of crush weight, oil ~18.5%
- So: SBE = bean exports + (meal exports / 0.795) + (oil exports / 0.185)
- Compare SBE exports vs total production over time → shows domestic consumption share

**Key analytical threads:**
1. Brazil-China correlation: SA acreage growth vs Chinese import demand, ~1:1 correlation pre-2000 through present
2. Crushing capacity evolution: track global crush capacity changes, show how domestic processing displaces raw bean exports
3. US structural shift: increasingly consuming own crop domestically (crush for biofuel), reducing export availability
4. Impact on world trade: as US exports contract, price discovery shifts to Brazil, China becomes even more Brazil-dependent

**Why:** This is the signature analysis — nobody presents it this way. Shows the structural transformation visible only in 30-year time series.
**How to apply:** FAS PSD backfill to 2000 (ideally 1990) is the #1 prerequisite. Once available, build SBE calculations into gold views and add to the 3D column map, story dashboard, and showcase.

**Data needed:**
- FAS PSD backfill to 1990+ (currently only 2020-2025) — CRITICAL BLOCKER
- Soybean meal and oil production/export data by country (already in PSD, just needs backfill)
- Brazil acreage by state (CONAB, partially loaded)
- China import data (in PSD)
- Global crush capacity estimates (may need external source)

---

## project_seasonal_monthly_projections

*(`project_seasonal_monthly_projections.md`)*

---
name: Monthly projections via seasonal decomposition
description: Annual forecast × monthly seasonal share = monthly projection. 5-year seasonal averages get 80% there. Exploits USDA's step-change constraint. Build seasonal patterns in KG or silver.
type: project
originSessionId: 2a87c99e-d9e9-4822-83cb-2459eddd9173
---
**Method:** Each monthly projection = annual total × that month's seasonal contribution percentage (from 5-year average seasonality).

**Why this works:** Ag markets have strong seasonality (planting, harvest, crush seasons, export windows). A complete monthly projection set using seasonal averages beats USDA because:
- USDA analysts are constrained by market impact — they can't reverse forecasts
- USDA makes outlook changes in steps (raise one month, can't lower the next)
- A systematic seasonal approach captures the full annual picture without these institutional constraints
- Gets ~80% of the way to accurate monthly projections

**How to apply:**
1. Build 5-year seasonal contribution factors for each data series (monthly value ÷ annual total, averaged over 5 years)
2. Store in silver layer or KG contexts (user leans toward KG — more portable)
3. User's spreadsheets already use this: each projection month = seasonal contribution of that month to annual total
4. After ag spreadsheets verified → fuel spreadsheets → then layer on monthly projection spreading
5. Silver views: `silver.seasonal_factors` or similar, with commodity × attribute × month → avg_pct, std_dev
6. KG contexts: `seasonal_norm` type already exists (see CFTC monthly percentiles, crop condition weekly norms)

**Sequence:** Finish ag spreadsheet verification → fuel spreadsheet hookup → build seasonal infrastructure → start monthly projection spreading → compare vs USDA

**Added:** 2026-04-17

---

## project_state_air_permits_llm

*(`project_state_air_permits_llm.md`)*

---
name: State air permit LLM extraction pipeline
description: PDF → Ollama → bronze.state_air_permits pipeline; Iowa proof-of-concept built 2026-04-30
type: project
originSessionId: 4f606138-9604-4e05-9a04-1c081995d736
---
## Status (2026-04-30)
End-to-end PoC working for Iowa Title V permits. Bronze schema, extractor, and loader all built and tested with 1 permit (AGP Eagle Grove). Currently running batch extraction across all 17 IA permits in background.

**Why:** User wants to grind through state-level air-permit PDFs to fill in facility-level detail (rated capacities, emission units, throughput limits) for forecasting and consulting work. GPU is the right tool — local Ollama, no API cost, runs unattended.

**How to apply:** When iterating on this pipeline or extending to another state, the layered architecture is:
1. **Existing per-state collectors** (`collectors/epa_echo/iowa_capacity_collector.py` etc.) — download PDFs, do regex extraction. Keep these, they're fast and reliable for top-level fields.
2. **LLM extraction layer** (`scripts/ollama/extract_titlev_permits.py`) — generic across states. Filters PDF pages to those with explicit emission-unit headings, sends to Ollama, gets structured JSON.
3. **Loader** (`scripts/load_titlev_extractions_to_bronze.py`) — JSON → `bronze.state_air_permits` + `bronze.state_air_permit_units` (migration 045).
4. **View** `silver.facility_air_permit_capacity` — one row per facility with units rolled into JSONB array.

## Schema (migration 045)
- `bronze.state_air_permits` — one row per (state, facility_id, permit_number). PDF metadata, sha256, extraction provenance.
- `bronze.state_air_permit_units` — one row per emission unit, FK to permits. Capacity, throughput, control devices, JSONB extra.
- `silver.facility_air_permit_capacity` — convenience view, one row per permit, units as JSONB array.

## Known limits / tuning needed
- **Page filter is conservative**: keeps only pages with explicit "Emission Unit X.YY" or "rated capacity" heading patterns. Caps total at 80K chars to fit qwen2.5:7b's 32K context. Misses narrative-form equipment descriptions where extractors / desolventizers / etc. are described without the trigger phrases. Eagle Grove first run: 9 units extracted vs 104 found by regex.
- **Categories drift**: prompt allows free-form `category` field; LLM invents categories outside the enum (e.g. "loading/unloading", "aspiration"). Either (a) strict-enum the prompt and re-prompt on violations, or (b) post-process via mapping table.
- **qwen3-coder:30b too slow on 16GB GPU**: model is 18.6 GB so partial CPU offload kicks in; 20-min timeout on 179K-char input. Use qwen2.5:7b with tighter filtering for production. Re-evaluate when GPU is upgraded.
- **`industry` field defaults to "other"** when LLM can't easily classify from filtered pages alone — would benefit from passing the facility name + city to a pre-classification step.

## Output paths
- `collectors/epa_echo/output/llm_titlev/<STATE>/<facility_key>.json` — one file per permit
- `collectors/epa_echo/output/llm_titlev/<STATE>/<facility_key>.raw.txt` — raw LLM response when JSON parse fails (debug)

## Replicating to other states
1. Use existing collectors/epa_echo/PROMPT_<STATE>_COLLECTOR.md scaffold to build PDF downloader for new state (IN, IL, NE templates already exist; MN, OH, MO, WI need building).
2. Once PDFs are in `collectors/epa_echo/raw/<state>_*_titlev.pdf`, the LLM extractor and loader are state-agnostic — just point them at the directory.
3. Add state hints to `detect_state()` in `extract_titlev_permits.py`.

## Useful next steps
- Fine-tune the page filter to also catch narrative equipment descriptions
- Strict-enum the category field in the prompt and reject/retry on violation
- Add a regex-vs-LLM diff report so we can see where the LLM adds value vs the existing collector
- Once IA batch is loaded, sample-check 3-5 permits manually for accuracy before extending to other states

---

## Status (2026-05-01 EOD)
- **IA: 17/17 facilities, 390 emission units in bronze** (1 facility — cargill_cedar_rapids_57004 — has v1 data; v2 timed out, see Known Failures)
- **IN: 3/5 facilities, 44 emission units** (5 PDFs downloaded; 1 needs OCR; 2 in known-failure list below)
- Two-GPU pipeline live: 5080 desktop (rlc-server) + 4060 laptop (rlc-0001) via Tailscale at `http://100.73.98.127:11434`
- Script flags added: `--ollama-url`, `--char-budget`, `--num-ctx`, `--all-indiana`, `--state XX`
- Now enforcing Ollama `format=json` and `num_predict=16384` (was 8192)

## KNOWN-FAILURE permits (need different strategy)
1. **cargill_cedar_rapids_57004** (IA, 190 pages) — JSON output cut off mid-array even at num_predict=16384. Genuinely too many emission units for one shot.
2. **in_adm_frankfort** (475 pages) — qwen2.5:7b produced *structurally* valid but *semantically* degenerate JSON ("HTML template fragment as field name"). 475-pg IDEM permit overwhelms model attention. Filter only kept 18/475 pages (4%) and front-loaded with cover letters; equipment list buried elsewhere.
3. **in_cargill_lafayette** (292 pages) — timed out at 30 min on the laptop, twice (once with 8K num_predict, once with 16K). Filter kept 31/292 pages (80K chars) but model can't process in budget.
4. **in_bunge_decatur** — image-based PDF, no native text. Needs OCR (pytesseract). Got 4 units from minimal text overlay; full equipment list invisible to pdfplumber.

## Recommended fix path (next session)
1. **Chunked extraction**: split filtered text into 3-4 segments of ~30K chars each, run each through Ollama, merge JSON results (dedup by unit_id). Solves both output-size and long-input attention drift.
2. **OCR fallback**: detect empty pdfplumber output, route to pytesseract preprocessing.
3. **IDEM format study**: read one IDEM permit fully, document where equipment lists appear (likely appendix or "Source/Equipment Description" section), tune filter for IN. Iowa-tuned filter doesn't generalize.
4. **Optional**: try qwen3-coder:30b on the 5080 for those 3 specific permits with longer prompt — slower but more disciplined under length pressure.

## Two-GPU routing convention (established 2026-05-01)
- **5080 desktop** = on-demand heavy lifts: vision tasks, larger models, interactive work
- **4060 laptop** (Tailscale `http://100.73.98.127:11434`) = always-on grinder: qwen2.5:7b extractions, embeddings, classification
- `--ollama-url http://100.73.98.127:11434` targets the laptop from any script
- Both GPUs run simultaneously with no conflict (proved this with parallel IA + IN batches)

---

## project_streaming_futures_feed

*(`project_streaming_futures_feed.md`)*

---
name: Streaming futures price feed (deferred)
description: When real-time futures monitoring becomes priority, replace the empty `bronze.futures_overnight_session` and `futures_us_session` tables with a streaming feed. Don't try to repair the old scheduled scrapes.
type: project
originSessionId: 4f606138-9604-4e05-9a04-1c081995d736
---
User confirmed 2026-05-07: real-time futures price monitoring is on the
roadmap, not now. When we get there, the right architecture is a
streaming feed, not the scheduled scrape pattern that powers the
currently-empty `bronze.futures_overnight_session` and
`bronze.futures_us_session` tables.

## Context

The original three-table design (overnight / US session / settlement)
was meant to capture three discrete intra-day snapshots of ag futures:
- `futures_overnight_session` — closing price at the morning break
  (before old open-outcry opening)
- `futures_us_session` — opening at 8 PM Central (electronic session
  start)
- `futures_daily_settlement` — official close (working; 69K rows back
  to 2000-03-15)

Settlement is fine on the current daily cadence. The other two require
intra-day capture, which the scheduled-scrape pattern doesn't really
suit. They've been empty since they were created and the collectors
have always failed.

## Decision

When this comes up:
1. Do not invest in fixing the existing scheduled-scrape collectors
   for `futures_overnight_session` / `futures_us_session`. Mark those
   tables deprecated.
2. Replace with a streaming feed. Candidates:
   - **CME Datamine / CME Direct** — authoritative, paid
   - **Refinitiv Eikon** — paid, broad coverage including grain/oilseed
   - **DTN / Barchart OnDemand** — mid-tier ag-focused
   - **Bloomberg Terminal API** — gold standard, expensive
   - **TradingView webhooks** or **Polygon.io** for cheaper PoC
3. Storage pattern: tick-level → bronze (write-once, append), aggregate
   to per-minute/per-second/snapshot in silver.
4. The streaming feed enables real-time Market Field updates (current
   sentiment loop is daily) — that's where this gets interesting.

## When to revisit

- After phase one (spreadsheet correctness) hits stable cadence
- After we have at least one client paying for forecast services
  that would justify the streaming feed cost (~$500-5K/mo depending on
  vendor)
- Before that: settlement-only is sufficient

---

## project_symbiotic_forecasting

*(`project_symbiotic_forecasting.md`)*

---
name: Symbiotic LLM forecasting endpoint
description: The KG + callable registry + spreadsheet ecosystem exists to create an LLM analyst that maintains independent forecasts, reconciles against realized data, and improves symbiotically over time.
type: project
originSessionId: 5a48b8b6-c1da-480b-83b2-04db8b865662
---
**The endpoint this is all building toward:**

An LLM that analyzes and projects **every monthly data series** we track in our spreadsheets. The LLM maintains its forecasts **separately** from the spreadsheets (not overwriting human work). As realized data arrives, we reconcile LLM forecasts vs human spreadsheet forecasts vs USDA/actual — and the error attribution feeds back to improve both sides.

**Why:** The LLM needs human judgment (narrative, analog selection, contextual overrides). The human needs the LLM (consistency, coverage, explicit model structure, bias detection). Symbiotic — each improves through the other.

**Architecture implications driving current work:**
1. **KG as prompt structure** — nodes carry the analytical frameworks ("if rain drops, yield drops, magnitude depends on growth stage") that the LLM consults when forecasting
2. **`kg_callable` registry** — quantitative models (weather→yield, crush margin, balance sheet construction) are callable functions the LLM invokes rather than reasoning through from scratch
3. **`core.forecasts` / `forecast_actual_pairs`** — already exists; this is where the LLM book lives and gets scored
4. **Self-exploration spec** — callables have their own sensitivity sweeps, so the LLM understands model behavior, not just the number

**How to apply:**
- Every time we add a data series to a spreadsheet, there should eventually be a parallel LLM forecast for the same series recorded in `core.forecasts`
- Reconciliation is not optional — that's where the learning happens
- Don't let the LLM forecast into the spreadsheet directly; keep it separate so we can measure
- "Blank" KG nodes are not a problem — they're a map of gaps to fill. Prioritize gaps on the critical path of a forecast the LLM is supposed to produce.

**Related memory:**
- `project_forecast_comparison.md` — LLM vs human vs USDA, reconciliation hierarchy
- `project_vision_endpoints.md` — strategic endpoints (1) S&D forecasting with accuracy tracking, (2) LLM content

---

## project_tallow_split

*(`project_tallow_split.md`)*

---
name: Tallow EBFT/IBFT Split Design
description: Feedstock allocation model tallow split into edible/inedible grades — design decisions, EIA guardrail, CI calibration approach
type: project
---

## Tallow Edible/Inedible Split (Apr 2026)

Splitting the single `BFT` feedstock code into `EBFT` (edible tallow) and `IBFT` (inedible tallow) in the feedstock allocation engine.

**Why:** Edible and inedible tallow have different pricing, different CI scores, and different economic pull into BBD depending on the credit/incentive stack. The split enables better allocation modeling and eventually plant-level feedstock tracking.

**How to apply:**

### Key Design Principle: EIA Guardrail
- EIA Form 819 monthly total tallow consumption is the **binding constraint**
- Model must reconcile EBFT + IBFT = EIA total tallow, both for historical calibration and forecasts
- If model implies more tallow used than EIA reports, reconcile to EIA numbers

### CI Score Calibration Arc
1. **Now**: Set initial CI scores at reasonable values that produce a meaningful edible/inedible differential
2. **Calibration phase**: Adjust CI differential until model split reconciles with true average values (after spreadsheet setup)
3. **Eventually**: Report/estimate CI scores for all plants individually; each plant "uses" EBFT or IBFT based on economics + registered CI pathway
4. After bottom-up model is complete with plant-level CI, the heuristic split becomes unnecessary

### Price Sourcing
- **Public (publishable)**:
  - USDA AMS NW_LS442 Tallow & Protein Report (weekly) — `TallowProteinCollector` already parses:
    - `packer_bleachable_tallow`, `renderer_bleachable_tallow`, `edible_tallow`, `bleachable_fancy_tallow`
  - USDA ERS OilCrops Tables (in `OilCropsAllTables.csv`):
    - Table 36: Edible tallow balance sheet (production, trade, stocks, food use) back to 1980
    - Table 36/34: Edible tallow wholesale price, Chicago (c/lb), monthly + annual
    - Table 34: Yellow grease price (Minneapolis), monthly
    - No dedicated inedible tallow spot price from USDA — AMS renderer_bleachable is closest proxy
- **Proprietary (training only)**: `is_proprietary=TRUE`
  - `US Tallow Prices.xlsx` in Dropbox: 7 Fastmarkets series (AG-TLW-0001 through AG-TLW-0035)
  - `training_prices_v2.xlsx`: BFT Packer Chicago + West Coast daily
  - User collecting additional tallow prices — will notify when ready

### Tallow Pricing Clarification (from user)
- "BFT - Packer" and "BFT - Renderer" refer to SOURCE (who rendered it), NOT edible vs inedible grade
- Packer-Renderer spread ≈ renderer's processing/logistics margin
- After rendering, material can become either edible or inedible BFT
- Both grades are "bleachable fancy tallow" — the edible/inedible distinction is about end-use certification

### User's Balance Sheet Files (in progress Apr 2026)
- `RLC-Models/Fats and Greases/new_models/us_tallow_complex_balance.xlsx` — 3 tabs: Inedible, Edible, Technical
- `us_edible_tallow_balance.xlsx`, `us_inedible_tallow_balance.xlsx` — individual commodity files
- `US Tallow Prices.xlsx` — proprietary daily prices
- `World Tallow Balance Sheets.xlsx`, `World Tallow Trade.xlsx` — global context

### Feedstock Codes
- `EBFT` = Edible Tallow (Bleachable Fancy Tallow / Packer grade)
- `IBFT` = Inedible Tallow (Technical tallow / Renderer grade)
- `BFT` = Legacy total, kept for backward compat with historical data

---

## project_trade_sheet_splits_deferred

*(`project_trade_sheet_splits_deferred.md`)*

---
name: Trade sheet splits — technical tallow + crude/refined veg oils
description: Deferred bifurcations for us_fats_greases_trade workbook to enable separate balance sheets per grade
type: project
originSessionId: 4f606138-9604-4e05-9a04-1c081995d736
---
User flagged 2026-05-06 — defer until after Market Field initial testing.

## Technical Tallow

Currently: us_fats_greases_trade.xlsm has "Edible Tallow" and "Inedible
Tallow" sheets. INEDIBLE_TALLOW commodity_group covers both
biofuel-grade and technical-grade material.

Plan: split into three sheet pairs (imports + exports each):
- Edible Tallow (HS 1502.10.00)
- Inedible Tallow — biofuel/feed grade
- Technical Tallow — industrial-chemistry grade (soaps, lubricants)

Likely HS-code split: existing 1502 Schedule B 10-digit suffixes
distinguish technical from inedible. Need to verify which suffix
maps to which grade. The macro's GetCommodityAndFlow already has
"technical tallow" hint pre-wired, would just need:
- New TECHNICAL_TALLOW commodity_group in silver.trade_commodity_reference
- Re-classify the appropriate HS-10 codes from INEDIBLE_TALLOW
- Add sheet to workbook

## Veg Oils Crude vs Refined

Currently: SOYBEAN_OIL group covers both 1507100000 (crude) and
1507904020/40/50 (refined / RBD). Same pattern for CANOLA_OIL,
CORN_OIL, SUNFLOWER_OIL, COTTONSEED_OIL, etc.

Plan: split each oil into "Crude X Oil" and "Refined X Oil" sheets so
balance sheets can be built separately. Schema-wise this is just
splitting the existing commodity_group into two — code and reference
data already distinguish at the HS-10 level.

Specifically affects:
- SOYBEAN_OIL: 1507100000 crude vs 1507904020/40/50 refined
- CORN_OIL: 1515210000 crude vs 1515290020/40 refined (already
  partially done via DCO split work)
- CANOLA_OIL: 1514110000 crude vs 1514190000 refined
- SUNFLOWER_OIL: similar pattern
- COTTONSEED_OIL: similar
- PALM_OIL: 1511100000 crude vs 1511900000 refined
- COCONUT_OIL: 1513110000 crude vs 1513190000 refined
- LINSEED_OIL: 1515110000 crude vs 1515190000 refined
- PEANUT_OIL: 1508100000 crude vs 1508900000 refined

Approach: introduce SOYBEAN_OIL_CRUDE / SOYBEAN_OIL_REFINED commodity
groups (and similar for others) without breaking the existing
SOYBEAN_OIL parent. Either:
  (a) keep SOYBEAN_OIL as union view spanning both, or
  (b) deprecate the parent and update all consumers

(a) is less disruptive; downstream queries continue to work, new
crude-only / refined-only sheets use the more specific group keys.

## When to do this

User explicitly said: "after our initial testing" of the Market Field.
Before this work, both crude and refined currently live in same sheet
which means the spreadsheet's "soybean oil" balance sheet mixes the
two grades — fine as a totals-only view but breaks crude vs refined
margin analysis.

Trigger: after Sprint 2 of Market Field is stable and the agent
decision loop is consuming sentiment vectors.

---

## project_uco_yg_model

*(`project_uco_yg_model.md`)*

---
name: UCO Collection Model & NASS YG Suppression
description: UCO estimation model (restaurant count × FAFH spending), NASS yellow grease confidentiality suppression since Dec 2023, balance sheet structure (Combined = UCO + YG ex-UCO)
type: project
---

## NASS Yellow Grease Suppression (Dec 2023)

NASS stopped publishing yellow grease production starting December 2023 due to confidentiality (D) — too few reporters (Darling Ingredients dominance). YG was running ~170-200M lbs/month, then vanished. CWG and "Other Grease" continue normally.

**Parser bug found:** Our NASS backfill parser fell through from (D) to the next numeric row, producing identical YG and "Other Grease" numbers post-Nov 2023. Need to fix parser to return NULL for (D) values.

**One published value:** Jan 2026 = 130M lbs (lower than pre-2024 avg of ~172M).

## UCO Collection Model

**Location:** `src/models/uco_collection_model.py`
**Data inputs:**
- Census CBP NAICS 722 restaurant counts (bronze.census_cbp_restaurants, 2010-2023)
- USDA ERS FAFH monthly spending (bronze.ers_food_sales_monthly, 1997-2024)
**Data collection script:** `scripts/collect_uco_model_inputs.py`

**Approach:** Restaurant count × generation rate = annual base → distribute monthly using FAFH spending seasonal index.

**Calibrated parameters (Apr 2026):**
- Generation rates: Full-service=25 gal/mo, QSR=55, Cafeteria=20, Catering=8, Bars=3
- Weighted avg ~32 gal/mo, Collection rate 70%, 7.5 lbs/gal
- Produces ~1B lbs/yr UCO (43-54% of NASS YG total)
- UCO share of NASS has been growing: 43% in 2016 → 54% in 2023

## Balance Sheet Structure

Three sheets per user's template (`us_used_cooking_oil_balance.xlsx`):
1. **Combined UCO-YG** — total = UCO + YG ex-UCO
2. **Yellow Grease** — YG ex-UCO only (NASS total minus UCO)
3. **Used Cooking Oil** — UCO only (model-derived production)

When NASS available: Combined = NASS total, UCO = model, YG ex-UCO = NASS - model
When NASS suppressed: UCO = model, Combined = UCO / 0.625, YG ex-UCO = residual

**Why:** UCO is a subset of yellow grease but needs its own balance sheet because it has different CI scores, pricing, trade flows (massive Chinese imports), and biofuel allocation behavior.

**How to apply:** When populating balance sheets, always check if NASS YG data is available for the month. Use model for UCO production line, derive YG ex-UCO as residual.

---

## project_usda_feedstock_supply_gaps

*(`project_usda_feedstock_supply_gaps.md`)*

---
name: usda-feedstock-supply-gaps-known
description: "USDA has known reconciliation gaps in feedstock supply reporting (UCO/YG, tallow, corn oil/DCO). EIA is canon because USDA itself defers to EIA. RLC's edge is filling these gaps better than USDA."
metadata: 
  node_type: memory
  type: project
  originSessionId: 05e2ef6d-523f-437f-8858-9be18d5d0aeb
---

Tore's working knowledge of USDA's feedstock-supply blind spots
(captured 2026-05-26, from biofuel-feedstock allocator work).

## The known gaps

1. **UCO / Yellow Grease** — USDA undercounts. The actual UCO supply
   tracks restaurant grease generation, which scales with food
   consumption — specifically the at-home vs away-from-home split.
   Tore's technique: use the USDA report on **food spending at home
   and away from home** to inflate / adjust the reported UCO (or
   Yellow Grease) numbers. Away-from-home growth → more UCO supply.

2. **Tallow supply shortfall** — USDA reported animal fat / tallow
   numbers don't reconcile against downstream consumption. The
   slaughter / render data implies more tallow than USDA reports.

3. **Corn Oil / DCO** — USDA's own agencies don't reconcile with
   each other on corn oil. ERS, NASS, FAS report different numbers
   for what looks like the same product. DCO (Distillers Corn Oil)
   from ethanol plants is the biofuel-relevant grade and probably
   the dominant source of the disagreement.

## Why EIA is canon

USDA itself uses EIA Form 819 as the canonical feedstock-consumption
reference. Even with USDA's three agencies producing numbers, none
of them claim authority above EIA on what biofuel plants consumed.
That's why the reconciliation hierarchy starts with EIA — see
[[feedback_data_reconciliation_hierarchy]].

## Why this is strategic for RLC

Tore's framing: "we should be able to estimate supply as well or
better than USDA, and certainly once we have the biotracker set up
our numbers will be as accurate as anyone."

The "biotracker" likely refers to the rail-car / shipment tracking
work in [[project_phase_two_facility_agents]] /
[[project_phase_two_agent_architecture_detail]]. Once that's running,
the bottom-up flow data (rail cars + truck movements + facility
ingest) will exceed what any government agency aggregates.

## How to apply

- When estimating UCO supply, pull USDA food-spending at-home vs
  away-from-home and use it as an adjustment factor on the
  reported UCO/YG numbers.
- When tallow or DCO numbers don't reconcile across sources, that's
  not a bug to fix in our derivation — it's known signal that
  USDA's aggregates have gaps. Document the discrepancy and let it
  surface in the implied-supply analytics.
- Frame any RLC supply estimate as **complementary to USDA, not
  competing with it**, until the facility-agent biotracker is online
  and we have real bottom-up data to back the claim.

## Related memories

- [[feedback_data_reconciliation_hierarchy]] — EIA/Census = canon
- [[project_phase_two_facility_agents]] — biotracker / facility-agent endpoint
- [[feedback_fastmarkets_keep_dont_show]] — IP rule
- [[project_basic_data_setup_sequence]] — feedstock → production → livestock

---

## project_vision_endpoints

*(`project_vision_endpoints.md`)*

---
name: Project vision - two endpoints
description: User's articulation of the two strategic endpoints for the RLC-Agent system - spreadsheet accuracy/forecast comparison and LLM-generated content
type: project
---

The system has two strategic endpoints:

**Endpoint 1: Spreadsheet-based S&D forecasting with accuracy tracking**
- Import data into spreadsheets user sets up to monitor and forecast monthly S&D for all commodities
- Must have robust accuracy and backup functions — correct data, correct units, verified
- Capture and compare forecast accuracy of user's spreadsheets vs LLM models
- User's job becomes explaining differences between human and LLM forecasts, and where each is right
- Over time, both human and LLM should improve symbiotically

**Endpoint 2: LLM-generated content and intelligence**
- Standardized weekly reports with auto-generated graphics
- Periodic webinars and classes teaching biomass-based diesel feedstock markets
- Creative, informative, visually appealing graphics for reports and social media
- Leveraging knowledge graph + data volumes impossible for a human to monitor
- Need to think about tying data together for LLM manipulation/understanding
- Leverage different LLMs' strengths for content creation

**Key insight**: As commodity set grows, the data import pipeline must scale easily. Single-script-per-pattern is preferred over hardcoded per-commodity approaches. The accuracy/verification layer is non-negotiable — this feeds career-critical analysis.

---

## project_weather_city_foundation

*(`project_weather_city_foundation.md`)*

---
name: weather-city-foundation-reconnaissance-2026-06-18
description: "State of the city-level weather tracking system + the gaps for Tore's Drew+historical+NDVI+GFS->summary+correlation vision"
metadata: 
  node_type: memory
  type: project
  originSessionId: ef803115-014b-4006-b2b5-b4bf7ad50ff7
---

Recon done 2026-06-18 when Tore reopened the weather thread (Drew reports +
historical weather + NDVI + GFS forecast -> comprehensive summary + city
tracking + correlate w/ crop conditions & yields at sub-state granularity).
Drew archive backfill PARKED.

## What already exists (~80% of the city foundation)
- **public.weather_location** — 27-28 cities, crop-region-mapped (US_CORN_BELT,
  US_WHEAT_BELT, US_DELTA, BRAZIL_*, ARGENTINA_*), lat/lon (Open-Meteo-ready),
  timezone, commodities[] tags, is_active, AUTO-ENROLLMENT (St Louis note
  "Auto-enrolled 2026-01-23").
- **silver.weather_observation** — PER-CITY daily history, keyed location_id,
  155,837 rows, 2010->present. Rich: temp hi/lo/avg, precip, humidity, soil
  moisture 0-7cm, soil temp, evapotranspiration, wind, conditions. (NOT
  state-level — it's city-level already.)
- **bronze.weather_email_extract** — 888 Drew extracts (Jan-Jun 2026 only;
  archive parked), with extracted_locations[] + matched_location_ids[] (already
  links Drew mentions to weather_location) + weather_summary + conditions jsonb.
- reference.weather_climatology (anomaly baselines).

## KEY INSIGHT — Drew writes at COUNTRY/STATE/REGION level, not city
Top extracted_locations: countries (Brazil 102, Argentina 101, Australia,
China, India, Europe) + states/regions (Mato Grosso 59, Delta 55, Rio Grande
do Sul 48, Parana 47, Cordoba 46, Midwest 39, Goias 34, Santa Fe 28, Corn Belt
25). Only cities = Buenos Aires, Sao Paulo (already enrolled). So Drew is a
**region-IMPORTANCE signal, not a city source** — the seed already covers his
top regions. City list = OUR design (representative cities/region), validated
by Drew's emphasis. Don't try to harvest cities from Drew.

## Gaps for the vision (build order TBD with Tore)
1. **Per-city FORECAST** — forecast is currently CROP-REGION level only
   (silver.weather_forecast_daily / GFS), NOT per-city. To "record the forecast"
   per city, add Open-Meteo per-city forecast capture (lat/lon already in
   weather_location).
2. **Densification / granularity** — DECISION NEEDED: how fine to subdivide
   below state (per-CRD? per-state-cluster? N cities/region?). Seed is ~1-8
   cities/region; sub-state goal needs more, esp. US Corn Belt.
3. **NDVI** — bronze.ndvi_observation EMPTY (0 rows); tables/views scaffolded
   (ndvi_crop_region, gold.ndvi_crop_health). Needs satellite data sourcing.
4. **Correlation framework** — tie per-city weather (+NDVI) to
   silver.crop_condition ratings + yields; the analytical payoff.
   **NASS granularity nuance (verified 2026-06-18):** crop CONDITION is
   STATE-LEVEL ONLY (ASD query rejected; it's a state survey) -> weather->
   condition correlation is inherently state-level. YIELD is published
   sub-state (county/ASD) -> weather->YIELD correlation is where CRD granularity
   pays off. (Exact NASS ASD-yield query returned 400 in quick probes -
   agg_level_desc/param combo needs sorting when building, but ASD yield does
   exist.) So: CRD weather -> CRD yield (sub-state payoff); state weather ->
   state condition.

## Granularity decision (Tore, 2026-06-18)
USDA regions (CRD/ASD) "for now, can go smaller or larger later." So enroll a
representative weather point per ASD in core corn/soy/wheat states, tagged with
asd_code for the yield join. ~50-100 cities for the core states. Build is a
focused multi-step weather session: ASD reference + geocode -> enroll ->
per-city Open-Meteo forecast -> per-city history backfill -> CRD yield correlation.

## ASD build — sources DE-RISKED (2026-06-18)
- **county -> ASD mapping: NASS QuickStats county-level query** carries
  asd_code + asd_desc. (Direct agg_level_desc='AGRICULTURAL DISTRICT' 400'd, but
  agg_level_desc='COUNTY' works — IA corn yield 2024 = 97 counties in 10 ASDs:
  10/NW,20/NC,30/NE,40/WC,50/Central,60/EC,70/SW,80/SC,90/SE,99/other.) Pull
  county->ASD per core state from there. Yield is also COUNTY-level (finer than
  ASD available if wanted).
- **County centroids (FIPS->lat/lon): NOT in our DB** (no county/fips table).
  Use Census Bureau Gazetteer county-centroid file; ASD point = mean of member
  county centroids (area-weight optional). One-time fetch.
- Then enroll ASD centroids into public.weather_location (asd_code tag), wire
  per-city Open-Meteo forecast, backfill history, correlate ASD weather->yield.

## GFS data-quality flag
silver.weather_forecast_daily is REAL (28 crop regions x 16 lead days, 8 vars +
GDD/heat-stress/frost/anomaly/ensemble p10-90), ~448 rows/run 2-3x/wk, current.
But lead-day-1 rows for some forecast_dates read 0.00 across regions — verify
when building. NDVI = Normalized Difference Vegetation Index (Tore asked).

---

## project_wheat_country_build

*(`project_wheat_country_build.md`)*

---
name: project-wheat-country-build
description: "Wheat balance-sheet data pipeline (pilot for country/commodity builds): contract, silver.wheat_series, division of labor with Claude Desktop, status."
metadata: 
  node_type: memory
  type: project
  originSessionId: bf6a8494-5d86-4f0c-bdf4-7e7106ff2f18
---

Wheat is the **pilot** for building full commodity balance sheets per country (next: Brazil corn/
wheat/oilseeds+biofuel feedstocks/sugar). Prove wheat end-to-end, then stamp the pattern out.

**Division of labor (dual-Claude):** Claude Code = plumbing (connectors → silver → gold →
**writers**). Claude Desktop = balance-sheet workbooks + the formulas that read the flat files.
**The flat file is the seam.** Contract: `docs/specs/flat_file_contract.md` (v1.1, tracked).

**Contract essentials:** LONG default (13-col schema), WIDE only for trade (reuse
`us_grains_trade.xlsm`). Key cols never rename. `vintage_rank` orders the estimate march; balance
sheet reads `MAXIFS(vintage_rank)` → `SUMIFS(value, …, rank=MAX)`. Writer guarantees ONE row per
(commodity,class,series,MY,period,vintage_rank) so SUMIFS is single-valued. `period` month =
calendar 1-12; quarters MY-relative. Generalization is cheap because `commodity` + `class` are
keys → new markets = new rows + new tabs, same formula pattern (no new link logic).

**Canonical layer:** `silver.wheat_series` (commodity,class,series,marketing_year,period_type,
period,vintage,vintage_rank,value,unit,source,release_date,revision). Scripts:
`build_silver_wheat_series.py` (bronze→silver), `write_wheat_flat_files.py` (silver→
`models/Food Grains/us_wheat_production.xlsx`, gitignored/local co-located w/ balance sheet),
`backfill_nass_wheat.py` (NASS QuickStats 1990-2025).

**Status (2026-07-02): SUPPLY SIDE DONE.** area (planted/harvested, agronomic classes, vintage
march), production (ALL + 5 market classes HRW/SRW/HRS/DURUM/WHITE-as-residual — validated sum to
ALL +0.00%, MY2024 magnitudes match USDA), yield (derived prod/area_harvested), stocks (quarterly).
Flat file = 525 long rows, 5 series. **TODO:** seed_use; **milling (Census M311J = the one new
collector)** — wheat ground/flour/millfeed/mill stocks; trade (wide, census_trade+FAS ESR);
co-products (millfeed/midds/gluten). Then Desktop wires the balance sheet (stage 3→7 acceptance).

**Flags:** MY2025 yield anomalous (~78 vs ~51 norm) — incomplete current-crop-year data, recency
artifact not a bug. Stocks Mar-1→prior-MY mapping worth a spot-check. Most US source connectors
already existed (NASS/Census/FAS/WASDE/AMS) — the prompt's "build from scratch" was a deliberate
generalization; reconcile against existing, don't rebuild. See [[flat_file_contract]] (docs/specs).

---

## project_yield_reconciliation

*(`project_yield_reconciliation.md`)*

---
name: Yield Reconciliation Check — Final Balance Sheet Validation
description: After finishing all US balance sheets, do a high-level yield check comparing total feedstock volumes to fuel production. Known EIA feedstock/fuel mismatch. UCO model needs recalibration based on this check.
type: project
---

## Yield Reconciliation Check (TODO after all US balance sheets done)

**What:** Compare total feedstock supply (all balance sheets summed) to EIA fuel production volumes. Calculate implied yield (lbs feedstock / gal fuel). If yield is reasonable (~7-8 lbs/gal average across all feedstocks and fuel types), the balance sheets are internally consistent.

**Why:** 
- EIA feedstock volumes and EIA fuel volumes DON'T reconcile cleanly — more fuel gets produced than feedstock data accounts for. This is a known industry issue (timing differences, reporting gaps, or errors).
- USDA/NASS numbers also don't reconcile to EIA fuel volumes.
- There's NO external series that directly reconciles fuel and feedstock volumes.
- ~2022: tallow imports appear to have ~150M lbs/month that doesn't show up in trade data anywhere. Mystery gap.
- So we DON'T try to calibrate individual feedstock balance sheets against USDA or EIA directly. Instead, we use the yield check as the final validation.

**How to apply:**
1. Sum all feedstock balance sheet production columns: SBO, canola oil, corn oil, DCO, tallow (E+I), CWG, poultry fat, YG, UCO, palm, other
2. Sum all fuel production: BD + RD + SAF + co-processing
3. Implied yield = total feedstock lbs / total fuel gallons
4. If implied yield is in the 7-8 lbs/gal range, we're good
5. If yield is too high (>8.5), we're short on feedstock → UCO/YG model likely needs upward revision
6. Adjust UCO model calibration to close the gap

**UCO model currently at ~1B lbs/yr — user expects this is 2-3x too low.** Will recalibrate after yield check reveals the gap magnitude.

**Critical:** Do NOT reconcile individual commodities to EIA. The aggregate yield check is the only reliable validation.

---

## reference_ams_coverage_gaps

*(`reference_ams_coverage_gaps.md`)*

---
name: ams-coverage-gaps
description: "What USDA AMS MARS API does and doesn't expose for BBD feedstocks — guides which feeds still need broker emails or other sourcing."
metadata: 
  node_type: memory
  type: reference
  originSessionId: 05e2ef6d-523f-437f-8858-9be18d5d0aeb
---

## What AMS covers (slugs added 2026-05-28)

Reports added to `src/agents/collectors/us/ams_cash_price_collector.py`
REPORT_CATALOG as category='byproducts':

- **2837** USDA Tallow & Protein Daily (NW_LS442) — daily, since ~mid-2022
- **2839** Weekly Tallow & Protein (NW_LS906) — weekly
- **3510** National Animal By-Product Feedstuff — weekly

Products covered with structured prices:
- Tallow (edible + bleachable, packer + renderer FFA grades)
- Choice White Grease
- Yellow Grease (Minnesota, CA-SJV, CA-South, CA-Central Coast)
- Loose Lard (Chicago, Central US)
- Meat & Bone Meal, Blood Meal, Feathermeal

Locations native to AMS:
- chicago, central_us, minnesota, us_pnw, eastern_corn_belt, panhandle,
  southern_plains, kc, arkansas, mississippi, ca_sjv, ca_south,
  ca_central_coast

## What AMS does NOT cover (sourcing gap)

These are real holes that the legacy fastmarkets feed filled but AMS
doesn't. Broker email path or other private sources required:

1. **UCO (used cooking oil)** — no AMS coverage at all. Critical BBD/RD
   feedstock. Tore plans to source via broker emails.
2. **Brown Grease** — no AMS coverage.
3. **Poultry Fat** — no AMS coverage. fastmarkets had Southeast +
   West Coast; both stale post 2025-04-18.
4. **Vegetable oils (SBO, canola oil, palm oil, DCO)** — not in MARS
   API at all. SBO regional cash basis (Iowa/IL/IN/Gulf/PNW) was
   fastmarkets-only. CBOT futures cover futures but not regional cash
   basis. Need different source — likely AGP scrape + broker emails.
5. **International prices (Brazil, Argentina, Malaysia, Europe CIF,
   China FOB)** — Tore deferred per 2026-05-28 conversation; will
   address one-by-one as EU/SA publications surface.

## Why this matters

The Feedstock Report price dashboard has ~46 entries. After the AMS
expansion ~20 are AMS-live, ~20 still depend on a stale fastmarkets
feed (last 2025-04-18) or are flagged is_placeholder. The remaining
broker-email + intl-publication work is the next price-coverage push.

## How to apply

When asked about a missing feedstock price:
1. Check if AMS has it — `python scripts/_probe_ams.py KEYWORD`
   (write that one-liner if helpful) or browse REPORT_CATALOG.
2. If yes, add the slug.
3. If no, this is a broker email / private sourcing problem — surface
   it as such, do not pretend the data exists in AMS.

Related: [[feedback-fastmarkets-keep-dont-show]], [[feedback-data-reconciliation-hierarchy]].

---

## reference_bbd_feedstock_eia_canonical

*(`reference_bbd_feedstock_eia_canonical.md`)*

---
name: reference_bbd_feedstock_eia_canonical
description: EIA is canonical for BBD feedstock consumption; soy oil -> BBD ~12B lb (BD+RD); allocation engine under-allocates
metadata: 
  node_type: memory
  type: reference
  originSessionId: 7d25feff-108a-4d08-9794-c7c81da7d481
---

**EIA is the canonical source for BBD feedstock consumption** (Tore, 2026-06-27). Table
`bronze.eia_feedstock_monthly` (cols: year, month, feedstock_name, **plant_type**
[biodiesel | renewable_diesel | **total**], quantity_mil_lbs). plant_type='total' = BD+RD
combined (do NOT sum all three plant_types — double-counts). It ALSO carries ethanol
feedstock (Corn ~300B lb, Grain Sorghum) — EXCLUDE those + non-oil/fat for BBD feedstock.

## Canonical numbers (trailing 12mo to 2025-09)
- **Soy oil -> BBD: 11.76 B lb** (biodiesel 6.90 + renewable_diesel 4.86). Near Tore's ~14B/MY
  target (12mo window vs full MY). The BD-only 6.90B matches the soyoil_balance_sheet
  "Biodiesel Feedstock" line (6-7B) — that line is BD-ONLY, NOT combined BBD.
- **BBD oils/fats feedstock total: 34.3 B lb.** Mix: soy oil 34%, tallow 25%, yellow grease
  16%, corn oil/DCO 13%, canola 9%, white grease 2%. Implied yield ~8.1 lb/gal (not 7.5).
- **Soybean oil balance:** crush supply 28.9B - BBD demand 11.76B (41% of crush oil) =
  17.2B for food/export.

## The bug this fixed
`gold.feedstock_allocation_national` (the allocation ENGINE output) gave soy oil only
**7.69B lb — 35% under EIA** — because it under-allocated RD soy oil. **Rule: use EIA
actuals for HISTORY (canonical); the allocation engine is for FORECAST only and needs
recalibration so its historical allocation reproduces EIA.** Tore's architecture: link
comprehensive production (rfs_data.xlsm / EMTS: bbd_production, bd_production, fuel_prod_by_type)
-> feedstock demand via the engine, ensuring allocated feedstock is sufficient to produce
reported fuel. `bbd_model_v1.py` now pulls feedstock from EIA. Engine recalibration to EIA
= the next step. See [[project_bbd_feedstock_primary_market]], [[project_facility_data_strategy]].


## UPDATE 2026-06-27: EIA RD feedstock is INCOMPLETE -> use production x yield (Tore's architecture)
Validation (scripts/feedstock_yield_validation.py): RLC yields reproduce BIODIESEL production
to 93% (yields good), but renewable diesel only 34% -- EIA's RD feedstock survey captures only
~35% of production-implied feedstock. THIS is why EIA soy-oil-to-BBD (11.76B) understates the
true ~14B: EIA undercounts RD feedstock. CORRECTION to "EIA canonical": EIA is reliable for
biodiesel feedstock + the feedstock MIX, but NOT for RD feedstock LEVELS. Derive feedstock from
PRODUCTION (EMTS/RFS, complete via RINs) x RLC yields. RLC bespoke yields live in
reference.feedstock_conversion_rates (Tore's 2021 numbers; biodiesel 12 / RD 5 / co-proc 7
feedstocks; per-feedstock yields ~7.4-9.38 lb/gal; the stated blended "totals" 7.20/7.58 are a
different basis, not used). GAP: RD block has NO soybean/canola yields (RD didn't use them in
2021) -- need Tore's RD veg-oil yields to complete the RD production->feedstock hook.

## ENGINE CALIBRATION progress (2026-06-27): allocator is the right machine, calibration in progress
The feedstock allocation engine (src/engines/feedstock_allocation/allocator.py + margin_model.py)
is exactly the right architecture: pathway gate -> capacity/production need -> economics (margin)
-> supply cap, per-facility monthly. Findings:
- Canon yields ALREADY wired (margin_model.CONVERSION_RATES matches Tore's 2026 canon: RD soy
  7.50, canola 7.55, DCO 9.20, tallow 9.38, YG 8.50, UCO 8.01, poultry 8.12; BD soy 7.50 etc.).
  reference.feedstock_conversion_rates duplicates these (single-source-of-truth externalization).
- Production-anchoring ALREADY wired: silver.fuel_production_forecast (balance-sheet sourced,
  2001-2030) -> load_production_forecasts/distribute_production_forecasts.
- FIXED a real bug: legacy generic BFT tallow bucket was allocated ON TOP of the guardrail-capped
  EBFT/IBFT split -> tallow overshot, starved soy (Jan-25 soy 19%). Dropped BFT supply -> soy
  19%->23.5%, tallow capped. Committed.
- OPEN (next calibration, blocks the ~14B/~34% soy target): after the BFT fix the engine
  UNDER-meets production (237 vs 283 mil gal Jan-25) -- it doesn't reallocate freed tallow demand
  to soy/canola despite their supply headroom (soy 441 vs 1000/mo cap). Likely PADD-level supply
  distribution or pathway-eligibility limiting the reallocation. Resolve before --save to gold.
  Scenario 'high_sbo' exists in the engine and may be relevant. Target: soy ~34% / ~14B/yr.

## NATIONAL HOOK BUILT + DECISIONS (2026-06-27)
`scripts/bbd_national_feedstock.py` — non-circular national feedstock: production (EMTS) x RLC
canon blended yields = TOTAL (35.1B, matches EIA total within 2%); waste fats+canola capped at
physical/EIA availability; SOY = residual balance = 12.5B (36%), within ~1.5B of S&D 14B. Two
independent methods corroborate. Knobs to close 1.5B (waste-fat caps ~1.5B high / yields low)
explicit; soy never tuned directly.
DECISIONS locked with Tore:
- Per-facility allocator is the right machine but GATED on regional commodity balance sheets
  (real supply caps) + financial models + prices + risk budgets. Don't calibrate it now.
- Regions: **PADD = reconciliation unit; facility/state = build unit** (fuel/EIA/LCFS are
  PADD-native; feedstock supply is ag-geographic, built bottom-up and rolled to PADD).
- National numbers = the CONTROL TOTAL the regional balances must sum back to (non-circular check).
- Sequencing: national now (DONE) -> regional commodity balance sheets -> risk budgets ->
  per-facility financial/economic models -> per-facility allocator comes online with real caps.

## FIRST REGIONAL BALANCE BUILT (2026-06-27): crush-belt (PADD2) soy oil
`scripts/padd2_soyoil_balance.py`. Crush belt makes 25.8B lb soy oil (89% of national, HARD
from facilities), uses ~10.8B in-region (BBD 5.9 + food 5.0), SHIPS OUT ~15B lb/yr = the
feedstock supply for coastal/Gulf RD plants (PADD2 = only 12% of US RD capacity). Net-out is
the BBD-feedstock LOGISTICS signal, robust to soft knobs (BD Midwest share 75%, food 30% —
biodiesel list lacks state). Reconciles to national control total. NEXT: PADD3 (Gulf) + PADD5
(West) where the 15B net-out lands as net-IN; then risk budgets + per-facility financial models.
National feedstock numbers + this regional balance = the last inputs Tore needed for the
spreadsheets ('now all the numbers should make sense').

---

## reference_brazil_my_alignment

*(`reference_brazil_my_alignment.md`)*

---
name: Brazil marketing year alignment caveat
description: USDA reports Brazil soy on US-aligned MY (Sep-Aug); the actual Brazil MY leads US. Detect via monthly data.
type: reference
originSessionId: f9769538-03ad-48d4-8472-58bcfe06d704
---
USDA artificially aligns Brazil's marketing year to follow the US convention (Sep-Aug), but the actual Brazilian soybean crop **leads** the US — Brazil plants Sep-Dec, harvests Feb-Jun, ships Mar-Sep. The Brazilian "safra" calendar runs roughly Feb-Jan (planting Sep through harvest end Feb gives the trading marketing year naturally as Feb-Jan, sometimes Mar-Feb depending on the year).

**Why this matters:**
- USDA-reported "Brazil MY 2024/25" actually covers the soybeans HARVESTED in early 2025, which started planting in Sep 2024 and shipped through 2025.
- Brazil's domestic data (CONAB, IBGE, ANEC) uses the safra calendar — comparing USDA's "MY 2024/25" to CONAB's "safra 2024/25" is comparing different time windows.
- Monthly data reveals this: when you align monthly production/export curves, the Brazil safra peak (Feb-Apr) is offset from the USDA MY treatment.

**Detection method:**
Match monthly export volumes from a Brazilian source (ANEC weekly, ComexStat monthly, IBGE quarterly) against the USDA-reported "marketing year" totals. The implied monthly distribution from USDA's MY data will not align with the actual Brazilian monthly distribution — that's the artifact of the artificial alignment.

**Implication for backfilling:**
For Brazil-specific data (CONAB, ANEC, IBGE, ComexStat, INDEC), we should ingest by **calendar year** (Jan-Dec) and let the analyst pick the MY framing at query time. Same logic applies to energy markets (EIA series are calendar-year reported anyway).

**Tore's analytical preference (2026-05-21):**
"Brazil leads US, you can tell by matching the monthly data. We can dive into that later."
Worth a deeper note + KG kg_context entry on the methodology — both for Brazil S&D analysis and as part of the index-provider story (knowing the local convention is a credentialing signal). Possibly a sample-report case study.

**Currently in bronze:**
- `bronze.conab_production` (7,255 rows) — Brazil all crops by state, safra calendar
- `bronze.fas_psd` filtered to country_code='BR' — USDA convention
- Reconciliation between the two is the substrate for the actual-vs-USDA-spread analysis

---

## reference_bronze_fuel_prices_provenance

*(`reference_bronze_fuel_prices_provenance.md`)*

---
name: bronze.fuel_prices is a historical FM snapshot, not a live feed
description: Why bronze.fuel_prices stops 2025-04-18 and shouldn't be treated as a stale collector
type: reference
originSessionId: f9769538-03ad-48d4-8472-58bcfe06d704
---
`bronze.fuel_prices` contains 329 weekly rows (2019-01-04 → 2025-04-18) of
Fastmarkets data. **This is a static historical snapshot from when Tore worked
at FM**, not output from a running collector. The 2025-04-18 endpoint is the
boundary of his employment, not a collector failure.

**Why this matters:**
- Do NOT diagnose this as a stale or broken collector. There is no collector.
- Replacement is planned (DTN integration is in flight for broader price coverage)
  but isn't urgent — Tore noted "good for now, will replace at some point" on
  2026-05-13.
- See also `user_career_legal.md` for FM-content sensitivities. Treat this data
  as historical reference, not republishable Fastmarkets content.

**What's in it (the columns that have data):**
- `b100_national` — national B100 spot, $/gal — 329 non-null
- `rd_california` — LCFS-credit-value-adjacent column, **NOT $/gal**. Values
  range 0.007-0.046; the actual unit/definition needs clarification.
  Excluded from `gold.us_liquid_fuel_prices_daily` (mig 087) pending review.

**Other columns** (ulsd_gulf, jet_a_spot, heating_oil_futures, wti_crude):
all zero rows. Those were schema placeholders; the FM data didn't include them.

---

## reference_carb_pathway_selection_bias

*(`reference_carb_pathway_selection_bias.md`)*

---
name: reference-carb-pathway-selection-bias
description: CARB LCFS pathway data is a census of LCFS-serving facilities but a biased sample of the rest — undercounts soy in RFS-only Midwest plants.
metadata: 
  node_type: memory
  type: reference
  originSessionId: a1006a1d-2b47-4348-924e-495b828e19fb
---

`bronze.carb_lcfs_pathways` / `silver.lcfs_pathway_ci` (892 rows, 79 US BBD facilities)
is excellent facility×feedstock×CI data, BUT it has structural selection bias: only
facilities that ship fuel into California's LCFS (and similar OR/WA programs) bother
registering CARB pathways. To pencil in LCFS they need LOW-CI feedstocks (UCO, tallow,
DCO). So:

- **CARB = census** of LCFS-serving facilities — authoritative for **coastal RD** (PADD5
  West Coast, PADD3 Gulf incl. DGD). These are where it's complete and reliable.
- **CARB = biased sample** of the **Midwest soy-BD fleet** (PADD2). A Midwest biodiesel
  plant selling into RFS-only / local markets never registers a CARB pathway, so it's
  invisible. CARB-only therefore **understates soy intensity** in soy-heavy regions.

**Evidence (2026-06-28):** empirical soy-share-of-lipid from CARB pathways read PADD2 at
0.98 (avg) when reality is soy-rich; coastal PADD3=0.75 / PADD5=0.57 matched expectations.
The PADD2 "miss" is the bias, not the truth.

**EPA RFS letters do NOT fix the Midwest gap (verified 2026-06-28).** EPA individual
determinations (`bronze.epa_pathway_detail`, 222 rows) are dominated by corn-ethanol
efficient-producer petitions (116 "Corn Starch", poor OCR); only 16 are BBD/lipid-relevant
and those are novel cases (co-processing, carinata, biogenic FOGs). Reason: generic soy-BD
and corn-ethanol run under EPA's 20 **generally-applicable pathways** (Table 1,
`reference.epa_generally_applicable_pathways`) and file NO facility-specific letter. So BOTH
CARB and EPA letters only cover facilities needing a BESPOKE pathway; the generic fleet is
invisible to facility-level pathway data by design.

**How to apply — two-tier eligibility (decided 2026-06-28):**
- TIER 1 (letters exist): coastal/low-CI RD — real feedstock set + CI from CARB
  (`silver.lcfs_pathway_ci`, `feedstock_code` already canonical: soy/canola/DCO/UCO/tallow).
- TIER 2 (generic fleet, no letter): default eligible slate by **facility technology**
  (transester BD -> {soy, DCO, canola}; HEFA RD -> broad lipid slate), CI from EPA Table-1.
  Technology is in `reference.biofuel_facilities` (the allocator's equipment gate).
- Allocator economics picks realized mix; EIA national actuals (`bronze.eia_feedstock_monthly`)
  are the control total. NEVER set national feedstock intensity from pathway letters alone.

Relates to [[project-rlc-2026-mandates]] M2 (FFA). Step-1 slate builder:
`scripts/facility_feedstock_slate.py`; top-down soy balance it feeds:
`scripts/padd_soyoil_balance.py`.

---

## reference_census_import_export_hs_codes

*(`reference_census_import_export_hs_codes.md`)*

---
name: census-trade-import-vs-export-hs-codes-diverge-by-flow
description: "US Census imports use HTSUS codes, exports use Schedule B — they differ below HS6, so trade_commodity_reference needs flow-correct codes per flow_type"
metadata: 
  node_type: memory
  type: reference
  originSessionId: ef803115-014b-4006-b2b5-b4bf7ad50ff7
---

US Census international trade uses **two different code systems by flow**:
- **Exports** → Schedule B (10-digit)
- **Imports** → HTSUS (10-digit)

They share the first 6 digits but **diverge in the last 4 (statistical suffix)**.
A Schedule B code queried on the Census *imports* endpoint returns NOTHING
(it isn't a valid HTSUS code) — silently, no error. So if `silver.trade_commodity_reference`
registers export (Schedule B) codes for `flow_type='IMPORTS'`, that commodity's
import line comes back near-zero.

**How it bit us:** corn imports were registered with Schedule B codes
(1005902020/902030/902035/904065) for IMPORTS. Only seed corn (1005100010,
valid in both systems) returned data, so the corn balance-sheet import line read
~0.4 mil bu/yr instead of the true ~24 mil bu/yr (~40x low). The dominant real
import code is HTSUS **1005902025** (yellow dent, except seed). Fixed in **mig 135**
(deactivate dead Schedule-B import rows, add HTSUS 1005100090/902015/902025/904060,
backfill 2013+). Verified: 23–41 mil bu/yr.

**This is systemic** — every commodity set up by copying export codes to both
flows has the same gap. Audit + fix is **mig 136** (all commodities). Watch the
multi-group HS6 cases (one HS6 → several commodity_groups) — can't auto-assign:
150120 (CWG/OTHER_PIG_FAT/YELLOW_GREASE), 150210 (edible/inedible tallow),
151211/151219 (safflower/sunflower oil), 151800 (linseed oil/UCO),
271019 (diesel/jet/residual fuel).

**Fix recipe per commodity:**
1. Enumerate real import codes: Census imports HS endpoint, `COMM_LVL=HS10`,
   `I_COMMODITY=<hs6>*`, recent full year, fields incl `GEN_QY1_YR`, `UNIT_QY1`.
2. Compare to active `flow_type='IMPORTS'` rows for that HS6.
3. Add missing HTSUS codes (copy units/conversion_factor from a same-commodity
   sibling row; commodity_name from Census LDESC); deactivate registered IMPORT
   rows absent from the Census HTSUS set.
4. Backfill `bronze.census_trade` for the new codes (collector `fetch_data(flow='imports', hs_codes=[...])`).

Collector note: `_load_hs_codes_from_db()` loads ALL active hs_code_10 regardless
of flow_type and fetches each for both flows; `flow_type` is used by the gold
views to attribute bronze rows. Adding an IMPORTS row is what makes the view pick
up that import code.

Same principle extends to other source agencies: import/export classifications
often diverge by flow — validate the code set against each flow's actual feed,
don't assume symmetry. See [[feedback_census_trade_verification]], [[project_fuel_flat_files]].

## Tier 2/3 audit — DO NOT bulk-deactivate "dead" codes (2026-06-16)
The mig-136 audit flagged codes "dead" = absent from the 2023/24 Census HTSUS
enumeration. But several such codes hold LARGE historical bronze data — they're
**discontinued** HTSUS codes (valid earlier, replaced later), not invalid:
soybean meal 2304000000 = 20B kg, oils 1512110020/190020 = ~5B kg, sunflower
1206000061/069 = ~1.5B kg, soybean meal 1208100000 = 416M kg. **Deactivating
them would silently drop billions of kg of history from the gold views.** KEEP
them active. For these commodities the audit found NO *missing current* codes —
their recent imports already flow through other active codes, so coverage is
continuous across the code transition. **Tier 2 is ADD-only** (capture missing
current-volume codes for SESAME, OTHER_VEG_OIL, CORN_GLUTEN, PEANUTS-raw,
OILCAKE_OTHER); do NOT deactivate discontinued codes. Only truly-zero-bronze
Schedule-B codes are deactivation candidates, and that's low-value.
Quirk to fix first: PEANUTS import reference conversion_factor=2.20462 (KG->lb)
under a '000 Pounds' label — off by 1000 vs sesame/veg-oil's 0.00220462. Verify
before adding peanut codes.
Tier 3 (needs Tore's eye): 271019/271012/271020 fuels (multi-group, mostly
non-fuel codes), FLAXSEED↔230800 mis-map (marigolds/acorns, not flax),
200811 prepared peanuts.

---

## reference_conab_direct_downloads

*(`reference_conab_direct_downloads.md`)*

---
name: CONAB direct-download URL set
description: Six CONAB Brazilian ag data endpoints discovered 2026-05-22. Stops Tore's copy-paste workflow.
type: reference
originSessionId: f9769538-03ad-48d4-8472-58bcfe06d704
---
CONAB (Brazilian National Supply Company) exposes 6 public download files at:

`https://portaldeinformacoes.conab.gov.br/downloads/arquivos/{FILE}.txt`

Confirmed working 2026-05-22:

| File | Size | Schema (Portuguese semicolon-delimited, decimal commas) |
|---|---:|---|
| `Frete.txt` | 2.4 MB | dsc_fonte; municipio_origem; cod_ibge_origem; uf_origem; municipio_destino; cod_ibge_destino; uf_destino; ano; mes; distancia_km; valor_frete_tonelada; valor_tonelada_km |
| `PrecoMinimo.txt` | 51 KB | descricao_produto_preco_minimo; id_produto; uf; regionalizacao; ano_inicio_vigencia; ... |
| `CustoProducao.txt` | 44 KB | empreendimento; ano; mes; ano_mes; produto; id_produto; safra; uf; municipio; cod_ibge; un; ... |
| `OfertaDemanda.txt` | 3.5 KB | produto; id_produto; dsc_safra; estoque_inicial_1000t; producao_1000t; importacao_1000t; ... |
| `Estoques.txt` | ~1 KB | (gzipped — check header) |
| `ArmazensCadastrados.txt` | 8.2 MB / 18,766 rows | identificacao_armazem; dsc_especie_armazem; dsc_tipo_armazem; dsc_tipo_entidade; dsc_tipo_pessoa; nom_municipio; cod_ibge; uf; qtd_capacidade_estatica(t); qtd_capacidade_expedicao(t); qtd_capacidade_recepcao(t) |

**Notes:**
- All files use Latin-1 / Windows-1252 encoding (verify per file)
- Numeric columns use comma as decimal separator
- IBGE municipality codes (cod_ibge) are joinable across all files
- Tore identified these 2026-05-22; collector rebuild from old broken API to these direct URLs is task #71 (created)
- Probed names that 404: Producao, Plantio, Colheita, AreaPlantada, Exportacao, Importacao, BalancaComercial, EstoquesPublicos, EstoquesPrivados, CertificadoMercadoria, CPR, Garantias, Subvencao, PEP, Cotacao
- Tore may identify additional public files; if so, add them to the same collector

**Bronze targets** (shipped 2026-05-22 — mig 102): `bronze.conab_freight`, `bronze.conab_min_prices`, `bronze.conab_production_cost`, `bronze.conab_supply_demand_v2` (separate from old conab_supply_demand which has different shape), `bronze.conab_stocks`, `bronze.conab_warehouses`. Convenience view `silver.conab_warehouse_state_summary`.

**Collector:** `src.agents.collectors.south_america.conab_direct_collector.CONABDirectCollector` — registered as `conab_direct` in `src/dispatcher/collector_registry.py` and scheduled monthly on day 11 (one day after existing CONAB monthly run) in `src/schedulers/master_scheduler.py`.

**First-run row counts (2026-05-22):** freight 10,060 (2014-01 to 2026-03), min_prices 9,485, production_cost 1,520, supply_demand 119 (1999/00 → 2025/26), stocks 8,972, warehouses 18,761 (all geocoded with lat/lon). Total 48,917 rows. **TRUNCATE+RELOAD** strategy per endpoint — idempotent monthly refresh.

**Sanity checks passed:**
- SOJA 2025/26 forecast: prod 180.1 MMT, exports 116.0 MMT, ending stocks 10.3 MMT (matches latest CONAB safra).
- Mato Grosso (MT) #1 warehouse state with 58 GT static capacity — matches reality.
- All 18,761 warehouses have lat/lon — fully geocoded Brazilian storage map.

**Cadence:** monthly — CONAB updates these files monthly with the safra survey. Scheduled day 11 of each month (one day after main CONAB run on day 10).

**Why this matters for the project:** combined with Frete (origin-destination freight) and ArmazensCadastrados (storage network with capacity), we have the substrate to model Brazilian grain flow bottlenecks at municipality + facility level — directly comparable to what we're building for US oilseed crush via silver.facility_map.

---

## reference_crop_condition_methodology

*(`reference_crop_condition_methodology.md`)*

---
name: crop-condition-ratings-what-the-number-actually-is
description: "NASS crop condition ratings are subjective local-observer impressions, not scientific measurement; how to weight them and why we'll build our own"
metadata: 
  node_type: memory
  type: reference
  originSessionId: ef803115-014b-4006-b2b5-b4bf7ad50ff7
---

Tore's framing (2026-06-18), from the NASS Crop Progress methodology as it
read historically: condition ratings (very poor/poor/fair/good/excellent) are
established by **people with regular contact with the crop** — historically
rural mail carriers and similar local observers, not agronomists or instruments.

**So a condition rating is "the feeling of a guy driving by at 55 mph on his
way to the next mailbox."** Not scientifically established.

**Why it's still valuable (the good side):** that observer has lived in the
area forever and has watched the crop grow there every summer. He has no
sophisticated model, so no theoretical bias — just a long local baseline. The
rating is therefore a decent RELATIVE / LOCAL-TREND signal (this year vs the
years he's seen), anchored in deep local familiarity.

**How to weight it (implications for our work):**
- Good for: relative/within-region trend, direction of change, local color.
- Weak for: absolute precision, cross-region comparison (each observer's
  baseline differs), scientific causation.

**The sharp empirical fact (Tore, confirmed 2026-06-18):** condition ratings
correlate WELL with yield **within the same year** (higher ratings -> better
yield that season) but POORLY **across years** (this year's "good" is not last
year's "good" — the observer's baseline drifts). Modeling consequence: use
condition only as a **within-season, per-year-normalized / de-trended** signal
for in-season yield direction; NEVER use absolute condition levels as a
cross-year feature. This is exactly why a from-objective-inputs (weather+NDVI)
measure that IS comparable across years is the upgrade.
- This REINFORCES preferring **weather -> YIELD** (objective, sub-state via
  NASS ASD/county) as the hard correlation target, and treating
  **weather -> CONDITION** (state-level, subjective) as a softer cross-check.
  See [[project_weather_city_foundation]].

**Endpoint:** RLC will build its OWN crop condition measure from objective
inputs (weather + NDVI + ...) once the weather<->condition<->yield correlations
are understood, then break it into smaller regions than NASS's state-level
condition. The mailman rating becomes a validation series, not the source of truth.

**Lead to chase:** some STATE NASS offices historically published their own
state crop-weather/condition bulletins that could differ from the national
report — a potential extra (and differently-sourced) condition signal. Check
state office archives when building the condition layer.

---

## reference_dual_claude_notion_coordination

*(`reference_dual_claude_notion_coordination.md`)*

---
name: Dual-Claude Notion coordination
description: Claude-UI (this session) coordinates with Claude-Content via Notion. Page-per-project model.
type: reference
originSessionId: f9769538-03ad-48d4-8472-58bcfe06d704
---
Tore runs two Claude sessions in parallel: this one (Claude-UI, in C:\dev\RLC-Agent for codebase/data/dispatcher work) and Claude Desktop (Claude-Content, in the browser, for analytical content + website + framework writing). **Notion is the shared source of truth** — both sessions read from and write to the same project pages. Tore orchestrates between sessions but doesn't manually mediate every decision.

**Coordination protocol:**
- Each major project gets a Notion page under the RLC OS workspace
- Both sessions append to a Decision Log table at the bottom of each project page
- Open questions are asked inline; the other session answers in a new `§N.A Responses` subsection
- Page-level comments via `mcp__claude_ai_Notion__notion-create-comment` for time-sensitive flags

**Active projects in this model:**
- **IFVS (Implied Feedstock Value Studio)** — public widget + essay series for rlccompanies.com. Page: notion.so/365ead023dee813daee1e31b22219327 (under RLC OS). Claude-Content owns analytical framework + HTML prototype + SVGs + deck. Claude-UI owns Python engine, FastAPI backend, MCP tool, schema, deployment. Reviewed 2026-05-19; 6 ADRs landed (IFVS-008 through IFVS-013).

**Tools to use (claude_ai_Notion MCP namespace):**
- `notion-fetch` to pull a page
- `notion-update-page` with `update_content` command for inline edits (preserves child pages)
- `notion-create-comment` for time-sensitive flags
- `notion-search` to find pages by keyword

**When to write to Notion:**
- Architectural decisions that affect both sessions
- Open questions one session can't answer alone
- Status updates on shared deliverables
- Schema / interface contract changes
- Anything you'd want the other session to read before next round

**When NOT to write to Notion:**
- Internal codebase state (use commit messages, local memory)
- Ephemeral debugging
- Anything that's already in MEMORY.md or feedback files

---

## reference_echo_canonical_facility_source

*(`reference_echo_canonical_facility_source.md`)*

---
name: reference_echo_canonical_facility_source
description: EPA ECHO is the canonical first source for facility existence — no more hand-curated facility lists
metadata: 
  node_type: memory
  type: feedback
  originSessionId: 7d25feff-108a-4d08-9794-c7c81da7d481
---

**Decision (2026-06-21):** EPA ECHO (`bronze.epa_echo_facility`) is the canonical
first source of truth for "what facilities exist." If a facility is on the ECHO list,
it exists (or is being built). Tore will NOT spend time hand-curating facility lists
(e.g. the Indiana IDEM dump was deleted as worthless given ECHO).

**Why:** All US companies processing the commodities RLC follows (ethanol, biodiesel/RD,
wheat milling, soybean/oilseed crush) require air permits that trigger ECHO inclusion.
Until a real counterexample of a non-ECHO target facility appears, treat ECHO as truth.
If one is found, adjust the process then — don't pre-curate against a hypothetical gap.

**How to apply:** Start any facility-universe task from ECHO, not from manually
assembled spreadsheets. ECHO is national: 2,865 facilities across 53 jurisdictions
(as of 2026-06-21), 1,925 operating, across 4 `search_profile` values (ethanol 1,638;
biodiesel_renewable_diesel 628; wheat_milling 394; soybean_oilseed 203). Useful columns:
`search_profile` (industry), `sic_codes`/`naics_codes`, `operating_status`, lat/long,
`county_fips`, and `caa_permit_ids` (Clean Air Act permit handles that link to the
state Title V permit DEPTH).

**The federal/state split this implies:** ECHO/FRS = the national CENSUS (who/where/what),
cheap and done. State Title V portals = the equipment-list DEPTH (capacity, emission
units) that ECHO lacks — only IA drained so far. The per-state data-source inventory
(`docs/planning/state_permit_data_source_inventory.md`) is about sourcing DEPTH, not
finding facilities. See [[project_permit_archive]] and [[project_iowa_multi_industry_expansion]].

---

## reference_emts_manual_export

*(`reference_emts_manual_export.md`)*

---
name: reference-emts-manual-export
description: "EMTS fuel-production data (the allocator's fuel-volume driver) requires a MANUAL EPA Qlik export each month — no API/static file has the needed month×fuel-category×Domestic cross-tab. Recurring step in the monthly allocator re-run."
metadata: 
  node_type: memory
  type: reference
  originSessionId: 4c118e7a-88d8-4867-be0e-3d804963463b
---

The FFA allocator's fuel-volume driver comes from EMTS RIN-generation data
(`silver.emts_production_canonical` view over `bronze.epa_emts_monthly`). See
[[project-feedstock-forecast-method]].

**This is a MANUAL monthly export — not an automated collector.** Confirmed 2026-07-09.

**Why it can't be automated (unlike the EIA-feedstock fix):** the canonical view needs
**month × fuel_category × producer_type='Domestic'** simultaneously — that cross-tab is what
splits D4 into biodiesel / renewable_diesel / SAF (its whole reason to exist). EPA's three
static public RFS files DON'T carry it:
- `rindata_<mon><yr>.csv` — monthly but **D-code only** (D4 lumps BD+RD+SAF, mixes Domestic+Importer+Foreign)
- `fuelproduction_<mon><yr>.csv` — has fuel category but **annual only**
- `generationbreakout_<mon><yr>.csv` — producer type but annual, D-code only

The month×category×Domestic cross-tab exists **only in EPA's interactive Qlik app**
(edap.epa.gov, "RINs Generated Transactions") — a JS/websocket SPA whose /api paths return the
HTML shell, not data. Not headlessly exportable. A share-based split of the monthly D4 total is
an ESTIMATE not an ACTUAL (RD ~254M, BD ~80M, SAF ~44M all large & move independently) — do NOT
do it when the task is "extend actuals."

**The monthly manual step (Tore, ~2 min):** open EPA RINs Generated Transactions → Export Table
with columns `RIN Year, Month, Producer Type, Fuel (D Code), Fuel Category, RINs, Volume (Gal.)`
through the latest month → save as `data/raw/rfs_data/rin_generation_<MM>_<YYYY>.csv` → run
`python src/tools/emts_csv_loader.py <path>` (loader now reads RLC_PG_HOST → RDS, fixed commit
d02c5bec; was hardcoded localhost). Then repoint + allocator run + rake + flat files.

**Operational implication:** the "re-run the allocator one month at a time as data comes out"
loop includes this manual EMTS export as a human-in-the-loop step. Everything else (EIA feedstock,
NASS fats) is now automated; EMTS is the one manual touchpoint. A headless Qlik-engine scraper is
possible but fragile — judged not worth it vs the 2-min manual export.

---

## reference_excel_color_conventions

*(`reference_excel_color_conventions.md`)*

---
name: Excel color conventions
description: Color hex values for generated xlsx files. Internal vs client-facing.
type: reference
originSessionId: f9769538-03ad-48d4-8472-58bcfe06d704
---
Two distinct color conventions, used in two different contexts.

## Internal workbooks (Tore's balance sheets, flat files, model output)

**Header / border-creating cell fills: `#3C7D22`** (forest green).
Applied to row 1 / header rows in `us_soybean_complex_bal_sheets.xlsm`,
`us_biodiesel_bal_sheets.xlsx`, and all generated flat-files like
`population_by_country.xlsx`.

Use this in:
- All `models/**/*.xlsx` and `.xlsm`
- All generated flat-file references (population_by_country,
  animal_units_by_country, food_expenditures_us, etc.)
- Updater scripts under `scripts/update_*.py` that write to xlsx
- VBA updater output

Header font: bold, white (`#FFFFFF`), Calibri.

## Public / client-facing artifacts (IFVS widget, Helios deck, leave-behind)

**Brand kit per IFVS spec ADR IFVS-007:**
- **INK** `#1B2A4A` (navy) — headers, primary text, axis lines
- **GOLD** `#C8A951` — emphasis, terminal values, accent
- **PAPER** `#F7F3EB` — background
- **WHITE** `#FFFFFF` — card surfaces

Use this in:
- IFVS public widget
- Helios demo dashboards
- Pitch decks, leave-behind docs
- Long-form essays + SVG illustrations
- Any artifact a client / external party sees

## Rule of thumb

If the xlsx is for Tore (his model spreadsheets, balance sheets,
analytical workbooks) → green `#3C7D22`.

If the artifact is for an external party (client, conference, partner) →
brand kit (INK / GOLD / PAPER).

Tore noted 2026-05-22: "It is a small and meaningless detail, but wanted
to point it out" — meaning small details matter for consistency, and the
green is the established convention across the existing balance sheet
family. Don't break the convention in newly-generated internal files.

---

## reference_felipe_weekly_cash_prices

*(`reference_felipe_weekly_cash_prices.md`)*

---
name: Felipe weekly cash prices delivery
description: Where the weekly cash-price file lands and how it reaches Felipe (HB Weekly Report)
type: reference
originSessionId: f9769538-03ad-48d4-8472-58bcfe06d704
---
**Trigger**: Windows Scheduled Task `\RLC\Weekly Cash Prices to Felipe`, weekly Wed 6:30pm ET. Runs `scripts/email_cash_prices_to_felipe.py`.

**Pipeline**:
1. `src/tools/generate_cash_prices.py` builds `Cash Prices - MMDDYYYY.xlsx` from `templates/Cash Prices - template.xlsx` filled via `extract_hb_prices()` against the DB. Saves to `output/reports/`.
2. Copies to `C:\Users\torem\RLC Dropbox\Tore Alden\HigbyBarrett\weekly_cash_prices\` — Felipe has Dropbox access to this folder.
3. Emails Felipe (`felipe.baptista@roundlakescommodities.com`) with file attached, cc Tore. Uses Gmail API OAuth via `token_work.json` in `RLC Team Folder\...\Desktop Assistant\` — **not** SMTP (Gmail rejects app-password SMTP login as of 2026-05-19).

**Currently missing rows** (8 of 27 on first run 2026-05-19) — need source ingestion:
- Sorghum Kansas City MO ($/bu)
- Rice AR Long Grain ($/cwt)
- Barley MT feed ($/cwt)
- Milk Class III CME Futures ($/cwt)
- Farm Diesel Midwest ($/gal)
- Fertilizer DAP Tampa, Urea New Orleans, UAN New Orleans ($/tonne)

**Pre-fix history**: Felipe's file used to depend on manual generation. Now automated. The AMS collector that feeds it was silently failing (no `.collect()` override → dispatcher path threw fetched data away) — fixed 2026-05-19 commit pending. See `feedback_collect_must_persist.md`.

**To change recipients or timing**: edit constants at top of `scripts/email_cash_prices_to_felipe.py` (FELIPE_EMAIL, TORE_EMAIL) or re-run `scripts/deployment/register_weekly_cash_prices_felipe.ps1` to change the schedule.

---

## reference_govt_shutdown_data_handling

*(`reference_govt_shutdown_data_handling.md`)*

---
name: govt-shutdown-data-handling
description: "Federal data series react differently to government shutdowns — some catch up post-reopening with summed activity, some leave permanent gaps. Track shutdown dates and per-agency behavior to interpret data gaps correctly."
metadata: 
  node_type: memory
  type: reference
  originSessionId: 05e2ef6d-523f-437f-8858-9be18d5d0aeb
---

Per Tore (2026-05-28), in the context of investigating a sparse MY 2023/24
ESR data gap:

## The principle

When the federal government shuts down, data series pause publication.
What happens when it reopens varies:

- **Some series sum the missed activity** into the first post-reopening
  release. Examples Tore noted from past experience: certain trade
  series and some commodity reports.
- **Other series leave permanent gaps** — the missed weeks are simply
  absent forever. The collector pulls nothing because nothing exists.
- **A few backfill week-by-week** retroactively once back online.

We can't predict or "fix" the missing data. We can:
1. **Track shutdown dates** so when we see a data gap we know whether
   it lines up with a shutdown (vs being a bug in our pipeline).
2. **Track per-agency behavior** — USDA NASS vs USDA FAS vs EIA vs
   Census all handle this differently.
3. **Flag, don't paper over** — if a gap is from a shutdown, document
   it; don't synthesize fake numbers.

## Recent US shutdowns to remember

| Period | Days | Affected | Notes |
|---|---|---|---|
| Oct 1, 2013 - Oct 16, 2013 | 16 | All non-essential | First major modern shutdown |
| Dec 22, 2018 - Jan 25, 2019 | 35 | Partial (incl. USDA) | Longest in US history |
| Jan 19-22, 2018 | 3 | Brief | Limited impact |
| Feb 2018, Sep 2023 | various | Brief | Hours-scale |

Late 2023 / early 2024 had several near-shutdowns averted by continuing
resolutions — those didn't pause data publication but did cause some
delays. Worth re-checking specific dates if a MY 2023/24 gap analysis
becomes important.

## Per-agency behavior (capture as we learn)

- **USDA FAS ESR (export sales)**: TBD. Tore notes uncertainty here —
  needs verification on next shutdown.
- **USDA NASS**: typically catches up on next scheduled release with
  the missed report.
- **EIA petroleum / biofuels**: usually sums the gap into the first
  post-reopening release.
- **Census trade**: typically backfills retroactively.

Update this table as we encounter each agency's behavior in practice.

## How to apply

When investigating a data gap:
1. Check the gap date range against shutdown dates above.
2. If overlapping: that's likely the cause, not our pipeline.
3. Decide: is the gap acceptable (note in spec, move on) or do we
   need a workaround (interpolate, mark as shutdown-affected, etc.)?
4. Don't manufacture replacement numbers — surface the gap honestly.

---

## reference_high_ffa_feedstock_biofuel_limit

*(`reference_high_ffa_feedstock_biofuel_limit.md`)*

---
name: reference-high-ffa-feedstock-biofuel-limit
description: "Poultry fat (PLT) & choice white grease (CWG) have high FFA — biofuel-available share is well below NASS production; don't feed raw production to the allocator."
metadata: 
  node_type: memory
  type: reference
  originSessionId: 4c118e7a-88d8-4867-be0e-3d804963463b
---

Poultry fat (PLT) and choice white grease (CWG) are **high-FFA (free fatty acid)**
feedstocks. Transesterification biodiesel plants largely can't run them without heavy
pretreatment, so much of their production is diverted to oleochemicals, feed, pet food,
and soap. RD/hydrotreating plants tolerate high FFA better, but these are still not
first-choice feedstocks.

**Consequence for the FFA allocator ([[project-ffa-feedstock-layer]]):** the raw NASS
`silver.animal_fat_production` value_lbs for CWG (~1.25B) and PLT (~2.2B) badly
**overstate** biofuel-available supply. Net down to the biofuel-available fraction in a
balance-sheet/non-bio step BEFORE wiring supply into the allocator. Keep the eligibility
gate technology-aware so high-FFA fats only flow to RD/hydrotreating facilities, not older
BD transesterification plants.

Consistent with the 2026-07-06 ruling: poultry is SMALL / ≈EIA and must NOT be made
RLC-canonical (net exporter HS1501.90, mostly pet-food/feed). UCO and tallow ARE
RLC-canonical; CWG/PLT are not.

**CORRECTION to 2026-07-06 Ruling 1 (Tore, 2026-07-07):** "Treat CWG the same way you
treat PF." Yesterday's note had CWG grouped with tallow/UCO as RLC-canonical + rake-exempt.
It is NOT. CWG is held at EIA-consistent values and raked normally, exactly like poultry.
The RLC-canonical / rake-EXEMPT set is **{tallow grades (BFT/EBFT/IBFT), UCO}** only.
The only feedstock from silver.animal_fat_production going canonical is TALLOW.

---

## reference_history_start_dates

*(`reference_history_start_dates.md`)*

---
name: Project history-start-date convention (1993/94 MY boundary)
description: When building new spreadsheets or extending history, oilseeds start Oct 1993, energies start Jan 1993. Aligns to US soybean MY 1993/94.
type: reference
originSessionId: f9769538-03ad-48d4-8472-58bcfe06d704
---
Tore set the project-wide history-start convention on 2026-05-24:

| Category | History starts | Rationale |
|---|---|---|
| Oilseeds / grains (commodity trade, ESR, inspections, prices) | **October 1993** | Start of the US soybean marketing year 1993/94 |
| Energies (crude, refined products, NG, EIA series) | **January 1993** | Calendar-year-based product convention |

**Why this matters:** so every new workbook, every database backfill, every
flat file, and every analysis starts at the same horizon by default. We
don't have to decide "where should this start?" each time.

**Earlier data is fine.** When a source goes back further (e.g., FGIS
inspections to 1990, FAS ESR to 1989), keep the earlier rows in bronze.
The convention is about DEFAULTS — what to show in workbooks, what to load
into balance sheets, what to align trade comparisons against.

**For Census trade:** existing tabs typically start 1994. Tore wants to
extend them back to Oct 1993 when there's time — not a priority but adds
the soybean MY 1993/94 to the picture for free.

**Implementation:**
- Excel xlsm date headers: real Date values, not text, starting at the
  convention boundary
- Database backfills: target the same boundary unless source allows
  deeper (then store deeper)
- Generated reports: filter to the boundary by default; let user override

**FAS ESR weekly data convention (Tore confirmed 2026-05-24):**
- Always key cells on `week_ending`, NEVER on the publish date.
- `week_ending` is always Thursday and is holiday-immune.
- Publishing date can shift (holiday week → Friday release), but that
  only delays when WE get the file; the data still describes the
  Thursday-ending week.
- Same principle applies to FGIS inspections, AMS WA_GR101, NASS — all
  use a data-end date that's stable through holidays.

---

## reference_idem_oracle_webcenter_permits

*(`reference_idem_oracle_webcenter_permits.md`)*

---
name: reference_idem_oracle_webcenter_permits
description: How to pull IN IDEM air-permit PDFs via the anonymous Oracle WebCenter ECM API (reusable for TX/TCEQ)
metadata: 
  node_type: memory
  type: reference
  originSessionId: 7d25feff-108a-4d08-9794-c7c81da7d481
---

**IN IDEM permit documents are in an Oracle WebCenter Content ECM at
`ecm.idem.in.gov/cs/idcplg`, anonymously queryable** (cracked + download-verified
2026-06-21). The research-agent's claimed `permits.air.idem.in.gov/<permitno>f.pdf`
pattern is WRONG (403/404). CAATS (`caats.idem.in.gov`) is a JSF portal — ignore it.

## Working recipe
1. **Search:** `GET /cs/idcplg?IdcService=GET_SEARCH_RESULTS&QueryText=<q>&ResultCount=50&IsJava=1&SortField=dInDate&SortOrder=Desc`
   - QueryText uses UCM syntax, e.g. `` dDocTitle <substring> `34376` `` (permit-number
     fragment is reliable; `` xFacilityName <substring> `CARGILL` `` was sparse — only 1 row).
   - Response is HDA (Oracle key-value). The document rows are in the
     `@ResultSet SearchResults ... @end` block (NOT `EnterpriseSearchResults`, which is
     provider metadata). HDA block format: line1=ncol, next ncol lines=column names, then
     values cycling ncol-wide.
2. **Filter** rows to `xIDEMDocumentType='Permit'` (air) and exclude `OLQ Permit` (= Office
   of Land Quality / solid waste). Add `xProgram=Air` to be safe. Keep latest by `dInDate`.
3. **Download:** `GET /cs/idcplg?IdcService=GET_FILE&dDocName=<dDocName>&RevisionSelectionMethod=LatestReleased&Rendition=Primary`
   → 200 application/pdf. Verified: Cargill Lafayette dDocName=83834376 → 1.36 MB %PDF-1.7.

## Metadata schema (from blFieldTypes) — useful fields
`xFacilityName`, `xPermitNum`, `xCounty`, `xFID`/`xAIID` (facility/agency-interest IDs),
`xIDEMDocumentType`/`xIDEMDocumentTypeID`, `xPermitType`, `xProgram`, `dDocName` (retrieval
key), `dInDate`, `dFormat`, `dDocTitle` (contains permit number).

## Enumeration strategy (ECHO -> IDEM)
ECHO `caa_permit_ids` are EPA IDs (`IN0000001813500033`), NOT IDEM permit numbers — no
direct join. Plan: faceted query for all `xIDEMDocumentType=Permit` + `xProgram=Air` docs
(paginate via StartRow), get the IN air-permit universe with xFacilityName/xPermitNum, then
match to ECHO's target majors by name+county. Keep MAJOR sources first (see
[[reference_echo_canonical_facility_source]] — Title V/major = our modeling targets).

## ⚠️ TARGETING IS THE HARD PART (2026-06-21) — download works, finding the right doc doesn't
The GET_FILE download is proven, but reliably locating a given facility's CURRENT operating
permit via ECM search is NOT solved:
- `xSourceID` is returned on docs ("059-00044") but is NOT search-indexed — `xSourceID
  <substring>/<matches>` queries return 0 rows.
- `dDocTitle <substring> <sourcenum>` only matches OLD docs whose dDocName embedded the
  source number; NEWER permits have numeric dDocTitles and are MISSED. (Cargill 34376 test
  returned a stale 2008 Final permit, not the current T157-34376-00038.)
- Docs carry NO facility name → no doc-level name matching.
- ECHO→IDEM crosswalk needs EPA FRS `get_program_facilities` (gives state source id), which
  was HTTP 503 intermittently during dev — unvalidated.
Acquirer (`src/agents/collectors/us/in_idem_permit_acquirer.py`) committed as **WIP/blocked**.
Next options: (a) CAATS JSF facility search (authoritative name→source→current permit);
(b) one-time full index of ~87k OAQ Final permit docs reading xSourceID off each, then join;
(c) deprioritize IN, grind a metadata-clean state first (PA verified open directory).
**Meta-lesson: "download works" ≠ "source is usable" — targeting/metadata quality is a
separate gate per state, as important as access tier.**

## Reusable
Same Oracle WebCenter (`idcplg?IdcService=`) family as **TX TCEQ** (CFR Online) and others —
build one HDA-parsing + GET_FILE adapter, parameterize host/field-names per state. Downstream
(register -> parse_spine -> publish) is already generic. See [[project_permit_archive]].

---

## reference_local_vs_cloud_llm

*(`reference_local_vs_cloud_llm.md`)*

---
name: Local GPU vs Cloud LLM — empirical decision framework
description: Where qwen3 / Ollama earn their keep vs where Claude/GPT win, based on actual RLC-Agent task profiles. Maintained as we learn.
type: reference
originSessionId: 4f606138-9604-4e05-9a04-1c081995d736
---
User wants this as both an internal guide and material to reference in
client conversations. Decisions should be empirical, not ideological.

## The framework

**Local GPU wins when ALL of:**
- Output can be deterministically verified (regex, schema check, downstream consistency)
- Variance is acceptable OR best-of-N is cheap (large parallelism)
- Volume is high enough that cloud cost compounds materially
- Privacy/compliance requires on-prem
- Task is stable, not actively iterating

**Cloud wins when ANY of:**
- Output is client-facing or decision-support
- Subtle reasoning required (not just pattern matching)
- Vision tasks involving handwriting or low-quality scans
- Strict JSON schemas that local models break
- Tool-use / agentic workflows
- Active development phase (faster feedback loops)
- Low daily volume (cost savings don't justify the variance hit)

## Empirical findings on RLC-Agent so far (as of 2026-05-06)

| Task | Choice | Why empirically |
|---|---|---|
| Title V permit extraction (PDF -> structured JSON) | **Local (qwen2.5:7b)** | Deterministic schema, large doc, variance compensated by best-of-N union. NOTE (2026-06-20): switched 30b->7b because 30b (18GB) won't fit the 16GB 5080 and runs on CPU; 7b fits fully on GPU (~27s/run). Drain of 281 IA permits ran ~6h desktop-only. See [[reference_ollama_gpu_cpu_fallback]]. |
| Title V LARGE permits (300-500pg, 100+ units) | **Local (qwen2.5:7b CHUNKED)** | Single-shot returns 0 units: the all-units JSON overflows num_predict AND the big input fills the context window (no output room). 30b times out on 16GB at high ctx. Fix = chunk the filtered text on page boundaries (~18k chars), extract+union per chunk. ADM Clinton 0->526 units. Chunking kept the hard tail LOCAL instead of escalating to cloud. (`extract_titlev_permits.py --chunk-chars`, `retry_failed_chunked.py`) |
| Weather brief synthesis (Drew Lerner emails -> daily brief) | **Cloud (Sonnet 4.6)** | Client-facing artifact. Per `feedback_llm_extraction_variance.md`, local was producing 50-70% bidirectional variance on long structured prose; unacceptable for daily briefs. |
| News article classification (8-topic + sentiment + facility) | **Cloud (Sonnet 4.6)** | Subtle topic discrimination (e.g. policy_industry vs policy_federal vs competitor_activity), strict JSON schema that local breaks ~20% of the time. |
| Chart annotation extraction (handwritten cursive on price charts) | **Cloud (Sonnet 4.6 vision)** | qwen3-vl:8b reads cursive at ~65-75% accuracy; Sonnet at ~85-90%. The 20-point gap on bad handwriting compounds badly when you're building a calibration corpus. |
| Sentence embeddings (RAG, similarity search) | **Local (nomic-embed-text)** | No reasoning needed, deterministic output, high volume. |
| Audio transcription (if/when needed) | **Local (Whisper)** | Whisper local quality matches cloud, no benefit to paying. |

## Pattern that's working

**Hybrid first-pass triage:** local screens N, cloud reviews flagged subset.
Have not deployed this yet on RLC but it's the right architecture for
high-volume + high-stakes mix:
- Local: "is this article about ag/biofuel?" yes/no for 1000s/day
- Cloud: full classification + facility tagging on the ~50/day yeses

Worth implementing once volume justifies the engineering.

## What to tell clients

> "Local GPU is great for high-volume, deterministic, privacy-sensitive
> work — bulk document extraction, embedding generation, audio
> transcription, simple classification. Cloud is better for variable,
> judgment-driven, low-volume work — anything client-facing, subtle
> reasoning, or where the task is still being designed. Most analytical
> workflows split across both. We'll usually pilot on cloud for fast
> iteration, then move stable steady-state work to local."

## What I keep getting wrong

When asked "can we run this on local GPU to save tokens?" my reflex is
to say "yes" because cost is real. But the honest answer is more often
"yes but the variance/quality cost is higher than the dollar saved at
your current volume." The user's volume is small ($200/yr API spend on
classification, $7/yr on chart extraction); local LLM only pays off once
volume × runs > 100K/year roughly. **Recommend cloud for low-volume
jobs and revisit when volume grows.**

## When to revisit

- When daily article volume crosses ~500 (currently ~30-50)
- When a task becomes stable (no prompt iteration for 2+ weeks)
- When client engagement requires on-prem (PII, NDA-bound data)
- When new local models materially close the quality gap (track new
  qwen3-vl, llama4 vision releases)

Empirical comparisons are the right tiebreaker. When in doubt, run the
same task on both for a week and measure.

---

## reference_oil_crops_yearbook_units

*(`reference_oil_crops_yearbook_units.md`)*

---
name: USDA Oil Crops Yearbook — unit conventions
description: Same workbook, three scales. Soybeans = mil bu, soybean meal = thou ST, soybean oil = mil lbs.
type: reference
originSessionId: f9769538-03ad-48d4-8472-58bcfe06d704
---
USDA ERS Oil Crops Yearbook (and the WASDE soybean complex balance sheet
underneath it) publishes all three soybean-complex commodities in **the
same workbook but three different scales**:

| Commodity | Unit | gold view column suffix |
|---|---|---|
| Soybeans (whole bean) | **million bushels** (mil bu) | `_mil_bu` |
| Soybean meal | **thousand short tons** (thou ST) | `_thou_st` |
| Soybean oil | **million pounds** (mil lbs) | `_mil_lbs` |

This is a convention, not an error. When pulling balance sheet rows from
`gold.us_soybean_balance_sheet`, `gold.us_soybean_meal_balance_sheet`,
`gold.us_soybean_oil_balance_sheet`, **always report the published unit**
— don't silently convert to a common scale.

Cross-check conversions (when validating against `bronze.fas_psd` which is
all 1000 MT):
- Soybean bu → 1000 MT: mil bu × 27.2155 (or × 0.027215 for thousand)
- Meal ST → 1000 MT: thou ST × 0.9072
- Oil lbs → 1000 MT: mil lbs ÷ 2,204.6

If a value reconciles within ~0.05% across both sources, treat as confirmed.
Larger gaps usually mean a vintage discontinuity in the ERS yearbook (see
e.g. soybean meal 2010/11 ending stocks — gold view shows stale 516.5 thou ST
but FAS PSD-derived 350 chains correctly with 2011/12 beginning stocks).

---

## reference_ollama_gpu_cpu_fallback

*(`reference_ollama_gpu_cpu_fallback.md`)*

---
name: reference_ollama_gpu_cpu_fallback
description: "Recurring desktop ollama \"runs 100% on CPU / 0 GPUs\" problem — diagnosis steps and fixes"
metadata: 
  node_type: memory
  type: reference
  originSessionId: 7d25feff-108a-4d08-9794-c7c81da7d481
---

Desktop ollama (RTX 5080, 16GB, Blackwell) intermittently runs models **100% on CPU**
even though the GPU is fine. Confirmed twice (2026-06-19). Symptoms: `ollama ps` shows
`100% CPU`, `nvidia-smi` shows ~0% util / ~1.6GB used while a model is "loaded",
extractions crawl and time out → permit parse_spine fails with "0 units across runs".

## How to diagnose fast (in order)
1. `ollama ps` → if PROCESSOR shows `100% CPU`, GPU isn't being used.
2. `nvidia-smi` → confirms the card + driver are alive at OS level (they usually are).
3. Read `C:\Users\torem\AppData\Local\Ollama\server.log`, grep `inference compute`.
   The tell: `id=cpu library=cpu ... total_vram="0 B"` and `discovering available GPUs...`
   followed by only a CPU device = **ollama enumerated ZERO GPUs**.
4. `ls "C:\Users\torem\AppData\Local\Programs\Ollama\lib\ollama\"` — a healthy install has
   `cuda_v13/` (or cuda_v12) with `ggml-cuda.dll`, `cublas*.dll`, `cudart*.dll`. On 2026-06-19
   it contained ONLY `mlx_cuda_v13\` (mlx.dll, libopenblas.dll) — **the ggml CUDA backend was
   missing entirely** → that's why ollama fell back to CPU+OpenBLAS.

## Root cause
Broken/incomplete ollama install — the ggml CUDA backend DLLs are absent from
`lib\ollama\`. Likely a botched upgrade (there's an `upgrade.log`) or a dirty
power-loss reboot. **Not** a Blackwell/CUDA-version issue (driver 591.44 / CUDA 13.1
are new enough), and **not** a boot-timing race (a clean server restart still saw 0 GPUs).

## Fix (CONFIRMED WORKING 2026-06-19)
**Reinstall ollama**: download latest `OllamaSetup.exe` from ollama.com/download, run it.
Restores the full `lib\ollama\` (incl. CUDA backend). Then `ollama ps` after a load should
show `100% GPU`. Verify lib dir has `cuda_v*` folder with `ggml-cuda.dll` before re-running.

2026-06-19 outcome: reinstall bumped 0.22.0 → 0.30.10, restored `cuda_v12`+`cuda_v13`
backends. Verified: `ollama ps` = `100% GPU`, log shows `using device CUDA0 (RTX 5080) ...
offloaded 29/29 layers to GPU`. The broken 0.22.0 lib dir had only `mlx_cuda_v13`.

## Two SECONDARY gotchas found alongside this (separate from the CPU-fallback bug)
1. **nssm service `OllamaLLM`** — REMOVED PERMANENTLY 2026-06-19 (`nssm remove OllamaLLM
   confirm`). It was a leftover from an early "keep the LLM on all the time" concept Tore no
   longer wants. It launched ollama headless in **session 0** at boot, which can't get a CUDA
   context → CPU-only, and squatted port 11434 before the GPU-capable tray app (session 1)
   could bind → tray app crash-looped (`app.log`: `ollama exited err="exit status 1"`). Gone
   now; only the tray app runs ollama. (Was a real conflict but NOT the GPU-fallback cause.)
2. **parse_spine model choice won't fit 16GB even with a healthy GPU.** `scripts/ollama/
   parse_spine.py` desktop endpoint uses `qwen3-coder:30b` (18GB) at the extractor default
   `num_ctx=65536` → KV cache balloons the footprint to ~44GB → 100% CPU regardless. The
   legacy 20 permits that DID work used `qwen2.5:7b` (~5GB). For reliable GPU operation on the
   5080, run the drain on qwen2.5:7b (fits at 32k ctx) on both endpoints, or drop num_ctx hard.
   char_budget=80k chars (~22k tok) means num_ctx can't go below ~32k without truncating input.

See [feedback_llm_extraction_variance.md] (best-of-N), [reference_local_vs_cloud_llm.md].

---

## reference_peanut_conversion_and_modeling

*(`reference_peanut_conversion_and_modeling.md`)*

---
name: peanut-conversion-modeling-rules
description: Peanut farmer-stock to shelled conversion = 1.33x. ERS Oil Crops Outlook is canonical forecast source. Sub-flow granularity = 5 streams per ERS Table 12; finer splits are bolt-on tabs. Lauric oils food sub-flows are modeled with explicit assumptions.
metadata: 
  node_type: memory
  type: reference
  originSessionId: 05e2ef6d-523f-437f-8858-9be18d5d0aeb
---

Tore's rules for the peanut complex balance sheet model (confirmed
2026-05-27), copied here so future sessions don't have to re-derive.

## Conversion factor

**Farmer-stock basis = shelled basis × 1.33** (or equivalently
shelled = farmer-stock × 0.75). Confirmed by Tore — this is the
working number, not a per-year recalc. Use 1.33 as the constant.

This means:
- ERS Yearbook Table 11 (farmer's stock basis) × 0.75 reconciles to
  shelled-basis flows from NASS Peanut Stocks & Processing.
- Tier 2A Shelled Crushed × 1.33 should ≈ Tier 1 Crush (farmer's basis).

## Forecast source

**ERS Oil Crops Outlook (monthly)** is canonical for peanut forward
projections. Not WASDE — WASDE doesn't have a peanut table. The Oil
Crops Outlook also signals USDA analyst focus + carries useful prices,
so it's worth ingesting beyond just the peanut numbers.

We need to build an ingestion path for it (PDF parse or CSV/xlsx if
available). Adjacent commodities in that report (soybean meal, soybean
oil, other oilseeds) also benefit from having the Outlook ingested.

## Sub-flow granularity (food use)

**5 streams from ERS Table 12 is the right default:**
- Peanut butter
- Peanut candy
- Snack peanuts
- Other edible
- Clean in-shell

If a future client wants finer cuts (salted vs unsalted snacks, smooth
vs chunky butter, etc.), **add as a bolt-on tab in the workbook that
reconciles back to the parent stream**. Do not refactor the base 5.

This is a generalizable pattern: **client-specific granularity =
bolt-on, parent-stream sums are inviolate**.

## Lauric oils modeling stance

For coconut + palm kernel food sub-flows, **we model with explicit
assumptions** rather than waiting for USDA data (USDA doesn't break
out lauric oil food use). Categories:
- Confectionery
- Baking / food service
- Food industrial
- Non-food industrial (soap, cosmetics)

Long-term: each major lauric oil processor should be a facility in
our facility graph. Until then, modeled allocation with stated
assumptions is acceptable. Update the assumption ledger as we learn
more about the industry/processing paths.

## Related

- Full spec: `docs/specs/peanut_balance_sheet_model.md`
- Bolt-on principle generalizes to any commodity where downstream
  buyers want subsegment views — keep parent sums clean, attach
  detail tabs as needed.

---

## reference_session_handoff_2026-05-25

*(`reference_session_handoff_2026-05-25.md`)*

---
name: Session handoff — 2026-05-25 reboot
description: What we shipped 2026-05-24/25 and what's queued in Tore's hands vs mine for next session.
type: reference
originSessionId: f9769538-03ad-48d4-8472-58bcfe06d704
---
Tore rebooted his machine 2026-05-25 afternoon. This note captures the
state to pick up cleanly.

## Shipped 2026-05-24/25 (all on `main`)

| Commit | What |
|---|---|
| `2f8c181e` | CONAB direct-download collector (48,917 rows, 6 endpoints, Brazilian warehouses geocoded) |
| `e472d75d` | Soy meal_stocks reference row — column O of us_oilseed_crush.xlsm now flows |
| `93bb8f17` | reference.data_source_attribution table (15 publisher rows) + USITC DataWeb collector skeleton |
| `63931181` | NASS collector: data_type plumb + save_to_bronze for all 5 schedule entries |
| `5e8122f1` | silver.facility_frs_xref — 2,001/2,041 facility_map rows mapped to EPA FRS IDs |
| `89fe91e2`, `788bcfe8`, `cceb910e` | FGIS inspections pipeline fix (loader bug, auto-download, Monday holiday handling) |
| `50d9e564`, `df2e691a`, `69d8373d`, `57cf8f6b`, `db4689a2`, `1699c032` | ESR Export Sales updater end-to-end (gold view, 12 tabs, .bas, WORLD TOTAL + UNKNOWN, timeout fix) |
| `56708936` | Models/ added to .gitignore (99 files untracked, including ~$ lock files) |
| `368deccb` | .bas hardening: VB_Name attrs added + OnKey targets qualified across 20 .bas files |
| `33f5b732` | EPA ECHO Phase 2 parser fixed (DFR field paths corrected) |
| `5c8c048b` | LCFS regional baselines verified — 3 of 4 IFVS spec values WRONG (OR/WA/BC corrected) |

## In Tore's hands (manual steps when he resumes)

1. **Re-import the patched .bas files** into each working workbook before
   next use. Each will pick up the VB_Name + qualified OnKey changes.
   Workbooks affected: us_soy_complex_trade, us_minor_oilseed_trade,
   us_oilseed_crush, us_grain_crush, us_fats_greases_trade,
   us_fuel_trade, eia_data, rfs_data — basically every xlsm that
   imports one of the .bas files.
2. **Save xlsm templates** to `domain_knowledge/spreadsheet_samples/`
   once Tore finalizes the US ESR layout — these become the templates
   for setting up rest-of-world country/commodity sheets later.
3. **Get USITC DataWeb API stable** — when their service is back up
   reliably, run `python -m src.agents.collectors.us.usitc_dataweb_collector --smoke`
   to iterate on the dataToReport column codes.

## In my hands (queued for next session)

| Task | Status | Note |
|---|---|---|
| #68 USITC DataWeb backfill | in_progress, paused | Waiting on API stability + dataToReport column codes |
| #70 AMS Tallow/Protein/DDGs parsers | pending | TXT format deprecated since Sept 2022, data gap |
| Ctrl+E shortcut collision | flagged | ExportSalesUpdaterSQL collides with EMTSDataUpdater/FeedstockUpdaterSQL on Ctrl+E. Workbook-scoped so OK today, but rebind needed if Tore ever opens both at once |
| EPA ECHO SIC sweep retirement | pending | New FRS-driven collector ready; disable the 4 old `epa_echo_{oilseed,ethanol,biodiesel,milling}` registry entries once Tore says go |
| IFVS spec Decision Log IFVS-014 | resolved on disk | Verified targets in `docs/specs/lcfs_regional_baselines_verification.md`; Claude-Content needs to update the Notion spec page with the corrected OR=86.89 / WA=93.10 / BC=79.57 values |

## Hot files (where I left off)

- `src/agents/facility/hefa_economics.py` — has the new
  `LCFS_REGIONAL_DIESEL_TARGETS_2026` constant; multi-region IFV math
  not yet wired (still CA-only). 24 unit tests pass as-is.
- `src/tools/ExportSalesUpdaterSQL.bas` — current; works end-to-end for
  the 12 ESR tabs in us_soy_complex_trade.xlsm.
- `src/agents/collectors/us/epa_echo_enrich_by_frs_collector.py` — Phase 2
  draft is now WORKING. Registered as `epa_echo_enrich_by_frs` in registry
  but not yet on a schedule.
- `Models/us_soy_complex_trade.xlsm` — local-only now (gitignored). Has
  all 12 ESR tabs with WORLD TOTAL on row 217, UNKNOWN on row 218,
  weekly Thursday date headers 1993-10-07 forward.

## DoD-security-posture frame

Saved 2026-05-25 to `project_dod_security_posture.md`. Long-arc goal:
RLC reaches NIST 800-171 / CMMC L1-2 building blocks. Not today, but
every new auth/secret/logging decision should not BLOCK that path.
Reference when designing new infrastructure.

---

## reference_state_permit_portals

*(`reference_state_permit_portals.md`)*

---
name: State environmental-agency permit portals — recon notes
description: How each state's air permit portal works, what's reachable, what's broken. Updated as we add states.
type: reference
originSessionId: 4f606138-9604-4e05-9a04-1c081995d736
---
Per-state notes on permit portal accessibility, learned during the
state-scraper buildout. Update as we touch each state.

## Minnesota — MN MPCA (touched 2026-05-09)

**Public-facing portal:** WIMN ("What's in My Neighborhood") at
`https://webapp.pca.state.mn.us/wimn/`. Angular SPA.

**Bulk CSV (no auth, no captcha):**
- URL: `https://resources.gisdata.mn.gov/pub/gdrs/data/pub/us_mn_state_pca/env_my_neighborhood/csv_env_my_neighborhood.zip`
- Contains 190K facility records with name, address, MPCA IDs,
  programs, lat/lon, industrial classification.
- Updated periodically.
- This is the primary facility-resolution path — fast and reliable.

**WIMN API (Radware/PerimeterX-protected):**
- Base: `https://services.pca.state.mn.us/api/v1/wimn/`
- Endpoints:
  - `sites?siteId=N` — facility metadata ✓
  - `sites/activities?siteId=N` — programs ✓
  - `sites/permit-actions?siteId=N` — permit history (issuances,
     amendments, reissuances) ✓
  - `sites/inspection-actions?siteId=N` — inspection history ✓
  - `sites/enforcement-actions?siteId=N` — enforcement ✓
  - `sites/cerclis-ids?siteId=N` — cleanup IDs ✓
  - **`sites/documents?siteId=N` — HTTP 500 GLOBALLY** ✗
- The Radware bot wall blocks raw HTTP (curl returns captcha) but
  passes Playwright (real browser context). User-Agent must look
  browser-like; viewport must be set; storage state should be reused
  across runs.

**Broken `/documents` endpoint (confirmed 2026-05-09):**
- Returns HTTP 500 with empty body for AGP Dawson (siteId=430),
  MnSP Brewster (siteId=59365), and likely every site.
- WIMN UI shows "Documents (N)" link but the click-through renders
  empty because the endpoint is broken.
- This is an MPCA server-side bug, not a Radware block. The UI is
  partly broken.
- **Implication:** PDFs cannot currently be auto-scraped from MPCA.
  Path to PDFs is Information Request via:
  https://www.pca.state.mn.us/about-mpca/information-requests
  or 651-296-6300. Days-to-weeks turnaround.

**What we get without PDFs:** permit IDs (e.g. `AQ07300002` for AGP
Dawson), full amendment dates, inspection history, federal program
flags (via cross-ref to EPA ECHO). High-value metadata for the FIC
even without equipment lists.

**Scraper:** `scripts/permit_scrapers/mn.py`. Uses Playwright; emits
`metadata.json` + `documents_index.json` per facility under
`data/raw/state_air_permits/mn/<siteId>/`.

## Missouri — MO DNR (TODO)

Air Pollution Control Program. To investigate.

## Nebraska — NE NDEE (TODO)

Department of Environment and Energy. To investigate.

## South Dakota — SD DENR (TODO)

Department of Environment and Natural Resources. To investigate.

## Iowa — IA DNR (already done, reference baseline)

Iowa is the reference implementation — we already have 7 IA AGP
plants extracted via the existing `scripts/ollama/extract_titlev_permits.py`
pipeline. IA DNR PDF format is the template the LLM extractor was
calibrated against.

---

## Cross-state federal aggregator: EPA ECHO

`https://echodata.epa.gov/echo/air_rest_services.get_facility_info`
provides Title V program flags, MACT/NSPS/NSR/SIP universe, recent
inspections, violations, federal Registry ID for any state. JSON,
no auth, no captcha. Use for cross-validation regardless of state.

Example for AGP Dawson:
```
?output=JSON&p_fn=AG%20Processing&p_st=MN&p_ct=Dawson
```

Returns `AIRPrograms: "MACT, NSPS, NSR, SIP, TVP"` confirming Title V
Major Source.

---

## reference_us_oilseed_unit_convention

*(`reference_us_oilseed_unit_convention.md`)*

---
name: us-oilseed-unit-convention
description: US oilseed balance-sheet display units (by commodity) and the ÷1000 input-sheet rule that derives from them.
metadata: 
  node_type: memory
  type: reference
  originSessionId: 8f52780e-dd99-4c4c-865c-f52ea3ce4ba0
---

US oilseed balance-sheet display units, locked in 2026-06-11 by Tore.
Outside-US balance sheets are uniformly thousand metric tonnes (much
easier); inside US the units switch by commodity/component.

## Balance-sheet display units

| Component | US BS unit |
|---|---|
| Oil   | million pounds (every commodity) |
| Meal  | thousand short tons (every commodity) |
| Seed  | varies by commodity (table below) |

| Commodity | Seed BS unit | Yield BS unit |
|---|---|---|
| soybeans   | million bushels    | bushels/acre |
| flaxseed   | million bushels    | bushels/acre |
| canola     | million pounds     | pounds/acre |
| sunflower  | million pounds     | pounds/acre |
| peanut     | million pounds     | pounds/acre |
| safflower  | million pounds     | pounds/acre |
| cottonseed | thousand short tons| lbs/acre (480-lb bale convention) |

## Input-sheet rule (us_oilseed_crush.xlsm)

Input sheet value = balance-sheet display unit ÷ 1000. So:

| Component | Input-sheet unit |
|---|---|
| Oil  | thousand pounds (000 lbs) |
| Meal | short tons |
| Seed (mil-lb BS commodities)  | thousand pounds (000 lbs) |
| Seed (mil-bu BS commodities)  | thousand bushels — but soybeans has a parallel `_bu` derived column and currently shows tons in the primary col |
| Seed (000-ST BS, cottonseed)  | short tons |

## DB conversion-factor implementation (silver.crush_attribute_reference)

`gold.nass_crush_mapped.display_value = bp.value * car.conversion_factor`.
Source unit (NASS reports in) → input-sheet unit (BS ÷ 1000) requires:

| Source | Target input unit | conversion_factor |
|---|---|---|
| LB   | 000 lbs | 0.001  |
| LB   | tons    | 0.0005 (1 lb = 0.0005 short ton) |
| TONS | tons    | 1.0    |
| TONS | 000 lbs | 2.0    (1 ton = 2000 lbs) |

Applied via migration 133 across canola, cottonseed, soybeans, sunflower,
peanut. Soybeans `soybeans_crushed` (the TONS leg) intentionally skipped
— it has a parallel `soybeans_crushed_bu` (mil bu) column that the BS
formulas actually use. Migrations 031/032 had already done the right thing
for corn/palm/palm_kernel/safflower/coconut oils.

**Side effects of migration 133:** the input-sheet cell values scale-shift
for ~60 attributes. The balance-sheet workbooks
(`models/Oilseeds/us_*_balance_sheets.xlsx`) reference those cells with
formulas that assumed the old scale; **Tore rescales BS formulas by hand**
(his preference 2026-06-11). The R4 unit-label row on us_oilseed_crush.xlsm
also needs a manual pass to reflect the new units — the .bas updater
ignores R4 so it's cosmetic but worth doing.

## Why the rule isn't "÷1000 from source twice"

Tore's original framing was "always ÷1000 twice" but that breaks for meal
(NASS reports in tons; BS in 000 tons; so only ONE ÷1000 between them).
The cleaner mental model is: **BS unit is fixed by Tore's convention;
input unit is BS÷1000; conversion_factor is whatever math gets from
NASS-source-unit to the input unit.** That accommodates the LB→tons jump
for peanut meal (factor 0.0005) and the TONS→000-lbs jump for canola seed
(factor 2.0).

---

## reference_usda_food_expenditure_reality_check

*(`reference_usda_food_expenditure_reality_check.md`)*

---
name: usda-food-expenditure-monthly-reality-check
description: "USDA ERS Food Expenditure Series (monthly food at home vs away from home spending) is the broad downstream reality-check signal for monthly food use estimates when USDA doesn't publish monthly end-use data (lauric oils, fat/grease food use, etc.)."
metadata: 
  node_type: memory
  type: reference
  originSessionId: 05e2ef6d-523f-437f-8858-9be18d5d0aeb
---

Per Tore (2026-05-27), and consistent with the UCO inflator technique
noted in [[project_usda_feedstock_supply_gaps]]:

## The signal

**USDA ERS Food Expenditure Series** publishes monthly food spending,
split by:
- **Food at home (FAH)** — grocery purchases, retail food
- **Food away from home (FAFH)** — restaurants, food service,
  institutional

URL: https://www.ers.usda.gov/data-products/food-expenditure-series/

This gives us a broad **monthly demand-side signal** for everything
downstream of the food supply. Useful for two purposes:

1. **Seasonality anchor.** When we need to allocate an annual food
   use total (e.g. ERS Yearbook Table 32 lauric oil domestic
   disappearance) to months, the FAH+FAFH monthly pattern gives a
   defensible seasonality curve. Better than a flat 1/12 split.

2. **Reality check on modeled assumptions.** If our modeled
   sub-flow allocations (e.g. confectionery 25% / baking 35% /
   etc.) don't co-move with FAFH growth, that's a flag. Apply the
   year-over-year change in food spending as a sanity check on
   year-over-year change in our modeled food use.

## How to apply

When building monthly views of food-use sub-balances where USDA
doesn't publish monthly data directly:
- **For laurics (coconut oil, palm kernel oil):** annual total ÷ 12
  × monthly_seasonal_factor(FAH/FAFH). Mark cells as MODELED.
- **For UCO/yellow grease supply:** apply FAFH growth multiplier
  on top of reported NASS YG numbers (per Tore's earlier note).
- **General sub-flow specifics:** confectionery and baking ride
  FAH retail seasonality; food service rides FAFH; non-food
  industrial isn't tied to either (use straight 1/12 or a different
  proxy).

## Pre-existing context

The closest existing memory on this is
[[project_usda_feedstock_supply_gaps]] which captures the UCO
inflator technique. This memory generalizes: FAH/FAFH spending is
the **broad monthly downstream reality check** for ANY food-use
estimate where USDA monthly granularity is missing.

## Implementation status

NOT YET INGESTED. To-do:
- Add ERS Food Expenditure Series scraper/collector (similar
  pattern to ERS Oilcrops Yearbook ingestion).
- Build `bronze.ers_food_expenditure` table
- Build `silver.monthly_food_expenditure_seasonality` view with
  computed FAH and FAFH seasonal factors (current month / annual
  avg).
- Apply factor in lauric food-use placeholder allocation.

Currently for monthly lauric sub-flows we use **even 1/12
distribution** as a temporary placeholder. Replace with the FAH/FAFH
seasonality once the collector is built.

---

## reference_xlsx_flat_file_conventions

*(`reference_xlsx_flat_file_conventions.md`)*

---
name: xlsx flat file conventions
description: Layout rules for generated reference workbooks (population, animal_units, etc.).
type: reference
originSessionId: f9769538-03ad-48d4-8472-58bcfe06d704
---
When generating an internal reference xlsx (population_by_country.xlsx,
animal_units_by_country.xlsx, food_expenditures_us.xlsx, future flat files),
follow these conventions:

## Sort

**Time series rows: oldest at top, newest at bottom (ascending).**

Reasoning (Tore 2026-05-22): "easier to link sheets the other way" — when
balance sheet files VLOOKUP / XLOOKUP into the flat file, having the latest
year at a stable bottom address makes cross-file references cleaner. The
year being added each refresh always appends below.

This applies to ALL time-series tabs (Population, GCAU, PCAU, etc.). It
does NOT apply to lookup tables (Multipliers tab, country code mappings,
etc.) where natural ordering by code is correct.

## Header

Row 1 = field labels (Year, then country codes or species names).
Fill: `#3C7D22` forest green. Bold white Calibri. Centered.

## _meta tab

Every generated flat file gets a `_meta` tab with:
- Source (publisher + dataset name)
- Source file (path or URL)
- Generated (UTC timestamp)
- Bronze table (if applicable)
- Bronze rows (if applicable)
- What's in the file (one line)
- Years/period covered
- Geography
- Refresh cadence
- Refresh recipe (literal command to re-run)

Order: most-load-bearing on top. Tore noted 2026-05-22 these are "incredible"
and asked me to retrofit them to existing workbooks as opportunities arise.

## Quarterly interpolation

When a downstream model needs quarterly granularity from annual source data
(e.g., FAO Population is annual; balance sheets sometimes want quarterly):
add a separate `_Quarterly` tab with linear interpolation. Convention for
quarter midpoints from annual Jan-1 anchors:

- Q1 = Y + 0.125 × (Y_next − Y)   (mid-Q1 ≈ Feb 15)
- Q2 = Y + 0.375 × (Y_next − Y)   (mid-Q2 ≈ May 15)
- Q3 = Y + 0.625 × (Y_next − Y)   (mid-Q3 ≈ Aug 15)
- Q4 = Y + 0.875 × (Y_next − Y)   (mid-Q4 ≈ Nov 15)

For the final year (no Y_next yet), use the year-over-year growth rate of
the prior period to extrapolate. Or hold flat — both acceptable.

The annual tab stays as the canonical source; quarterly is a derived view.

## Column ordering

Country columns ordered by analytical importance (US, BR, AR, CN, EU first
since those drive global crop/oilseed/grain markets), then alphabetical
for remaining. WORLD aggregate last.

For livestock category columns: USDA convention order — Dairy, Cattle on
feed, Cattle other, Hogs, Poultry, Livestock other, All animals.

## File location

`models/{TopicArea}/{filename}.xlsx`
- `models/Population/population_by_country.xlsx`
- `models/AnimalUnits/animal_units_by_country.xlsx`
- `models/FoodExpenditures/food_expenditures_us.xlsx`
- etc.

Updater scripts at `scripts/ingest_{source}_{topic}.py` or
`scripts/update_{file}.py`.

## Color (cross-reference)

See `reference_excel_color_conventions.md`. Internal files use `#3C7D22`
header fill. Client-facing artifacts use the brand kit instead.

---

## user_career_history

*(`user_career_history.md`)*

---
name: Career history — full professional background
description: Complete career trajectory from SmithBarney to Round Lakes Companies, key skills, relationships, and context
type: user
---

## Career Timeline

**SmithBarney** — Ag group, farmer/SME hedging programs. Left when the firm wanted the group out during the internet boom. Team took clients and started own firm.

**International Agribusiness Group (IAG)** — Co-founded with mentor (now deceased). Bridged gap between ag clients who needed investment banking services but were too small for Wall Street. Developed capital raising expertise for ag projects. IAG still operates, run by mentor's son. Key colleague: Rob Fisher.

**Vreba-Hoff Dairy Development** — Hired by a client. Company built large-scale dairy farms in Michigan for European immigrant farmers. Job was capital raising for increasingly aggressive farm financing deals (low equity, large scale). Built relationships with regional banks and structured a creative insurance company land-leaseback arrangement. Left after ~1 year — different views on approach to lending relationships.

**Vermillion Asset Management** — New York. Started as desk analyst for ags and softs, promoted to PM for livestock, traded a book of ag futures and options. Profitable first year. Founded by Chris and Drew — among the smartest people user has worked with. Left to return to Michigan to care for grandparents after father's early death from cancer.

**Binnacle Capital Management** — Northeast. Desk analyst for ags, softs, assisted metals. Underfunded startup that failed — founder was marketing-focused, AUM didn't materialize fast enough.

**Five Rings** — Followed a Binnacle colleague. Short-term trading focus (electricity arbitrage background). Mismatch with user's skill set — user excels at ~3 month price forecasting, firm couldn't hold positions overnight. Developed a fundamental report-based short-term trading strategy using probability functions around WASDE statistics (if ending stocks > X, fundamental value = Y, enter when price is Z standard deviations away, exit at target). Was let go despite the strategy working.

**Informa Economics (~2014-2017)** — Memphis. Oilseeds analyst. **Transformative period** — "learned more at Informa than the rest of my career combined." The balance sheet spreadsheet methodology we are rebuilding in RLC-Agent is essentially the Informa analytical framework. This is where the foundational analytical approach was forged.

**The Jacobsen (~2017-2022)** — Recruited by former Informa colleague. Initially hired to build forecasting capabilities starting with soybeans. Informa sent cease-and-desist letter, which forced a pivot to biomass-based diesel feedstock markets exclusively. This turned out to be the best career decision — dominated the BBD feedstock space from 2018 onward.

**Fastmarkets (2022-June 2025)** — FM acquired The Jacobsen in early 2022. Ran the ag analytics department. Created and hosted the "Fats, Fuels & Oils" weekly webinar series (search YouTube to see examples). Left June 2025 due to management issues.

**Round Lakes Companies (June 2025-present)** — Founded immediately after leaving FM. Using AI (Claude) to build what would have required a team of developers. The RLC-Agent system is the product of this effort, now ~4 months into active development with Claude Code.

## Key Professional Attributes

- **Sweet spot**: 3-month price forecasting. Not a day trader, not a macro strategist — fundamental S&D analysis predicting where prices land in a quarter.
- **Capital raising experience**: Understands deal structure, investor psychology, and creative financing from IAG and Vreba-Hoff days.
- **Presentation/content skills**: Built and hosted a weekly webinar series watched by the industry. Comfortable presenting at conferences. Has presented at AOCS, Fastmarkets conferences, and others.
- **Analytical framework**: The Informa balance sheet methodology is the intellectual foundation. Everything we build should be traceable to that discipline.
- **Hedge fund perspective**: Understands positioning, risk management, and quantitative approaches from Vermillion/Binnacle/Five Rings. The probability function approach to WASDE trading is worth revisiting.

## Key Relationships Referenced

- **Rob Fisher** — IAG partner
- **Chris and Drew** — Vermillion founders (highly respected)
- **Ryan Standard** — Fastmarkets colleague (~decade working together)
- **John Cusick** — Former Jacobsen colleague, now presenting at FM conferences
- **Felipe** — Current RLC team member (junior, learning)
- **Joao** — Going to work at one of the competitor companies

---

## user_career_legal

*(`user_career_legal.md`)*

---
name: Career history and competitive/legal context
description: User's professional history, Fastmarkets relationship, and legal sensitivities around content production
type: user
---

**Career path**: Informa → The Jacobsen → Fastmarkets (acquired Jacobsen) → Round Lakes Companies (founded ~mid-2025)

**"Fats, Fuels & Oils" webinar series**: Produced BY the user WHILE AT Fastmarkets. It was his weekly recorded webinar. NOT currently in production. FM reduced/dropped it after he left. Must be clear he is NOT associated with FM content.

**Legal sensitivity**:
- After leaving Informa, their attorney sent a cease-and-desist letter to The Jacobsen's management prohibiting producing anything resembling Informa reports in style or content.
- The threat was likely empty (others received similar letters with no follow-through) but caused a strategic pivot from broad commodities to biofuel feedstocks — which turned out to be the right move, dominating that space since 2018.
- User wants to avoid a similar situation with Fastmarkets, even though he believes the threat would be empty 10 months post-departure.
- Key colleague: John Cusick (former Jacobsen colleague) is presenting at FM conference and reports FM is unaware of what user is building.

**How to apply:**
- NEVER describe "Fats, Fuels & Oils" as a current production — always use past tense or clarify it was produced while at FM
- Be careful about content that could be seen as replicating FM proprietary report formats
- The user's competitive advantage is the AI-powered approach, not copying existing report structures
- Ryan Standard is the FM contact for conference proposals — sending a proposal will reveal the user's work to FM

---

## user_freddie

*(`user_freddie.md`)*

---
name: Freddie the dog
description: User has a dog named Freddie who sits on the arm of his chair during work sessions. Sunrise = walk time.
type: user
---

User has a dog named Freddie who keeps him company during early morning work sessions. Freddie sits on the arm of the chair and gives "sad face" when the sun rises because he knows it's walk time. User clearly loves him and finds his expressions funny/endearing.

---

## user_hardware_ollama

*(`user_hardware_ollama.md`)*

---
name: Local hardware and Ollama setup
description: User's local GPU specs, Ollama install layout, and the Blackwell reinstall gotcha
type: user
originSessionId: 4f606138-9604-4e05-9a04-1c081995d736
---
## Hardware
- **Desktop GPU: NVIDIA RTX 5080 (Blackwell, sm_120, 16 GB VRAM)** — driver 591.44, CUDA 13.1
- Desktop system RAM: 32 GB (16 GB free typical), swap ~56 GB
- 12C/24T CPU (AVX512 + VBMI + VNNI)
- **Laptop GPU: NVIDIA RTX 4060** (corrected 2026-05-07; earlier note said 4080 — wrong)
  Available as a second worker for parallel Ollama batches. User explicitly
  flagged 2026-05-07 that we should use both machines for the SEC filings
  pipeline ("public files"). Pattern: each machine handles a different
  ticker; both write to their local repo; merge via git.

## Laptop Python env gotcha (recurring)

When pulling fresh on the laptop and running scripts, a `ModuleNotFoundError`
on `dotenv` (or `requests`, `openpyxl`, etc.) is the first symptom of an
incomplete venv. The laptop's pip install drifts from the desktop's.

**Why:** The laptop tracks the same git repo but has its own Python
install with its own site-packages. New dependencies added on the desktop
don't propagate.

**How to apply:** First time running scripts on the laptop after a `git
pull`, run `pip install -r requirements.txt` from the project root. If
only running the SEC puller/extractor (no DB writes), the minimal subset
is `pip install python-dotenv requests openpyxl`. Don't waste time
debugging individual ImportErrors — just install requirements first.

## Ollama install
- Binary: `C:\Users\torem\AppData\Local\Programs\Ollama\ollama.exe` (current 0.22.0, released 2026-04-28)
- Model store: **`C:\RLC\models`** (not default `~/.ollama/models`) — set via `OLLAMA_MODELS` env var
- Server log: `C:\Users\torem\AppData\Local\Ollama\server.log`
- Runs as Windows service `OllamaLLM` (StartType: Automatic) — auto-restarts on kill, must `Stop-Service OllamaLLM` before reinstall
- Config: `OLLAMA_HOST=0.0.0.0:11434`, `OLLAMA_KEEP_ALIVE=30m`, `OLLAMA_NUM_PARALLEL=4`, `OLLAMA_CONTEXT_LENGTH=8192`

## Models on disk (as of 2026-04-30)
- qwen3-coder:30b (18.6 GB) — production SQL/code-gen model
- qwen3-vl:8b (6.1 GB) — vision-language
- qwen2.5-coder:32b-instruct (19.9 GB) — too slow on desktop
- llama3.1:latest, llama3.1:8b, qwen2.5:7b, qwen2.5:7b-instruct-q4_K_M, phi3:mini, nomic-embed-text

## Blackwell GPU acceleration — verified 2026-04-30
- Healthy startup log line includes: `library=CUDA compute=12.0 ... libdirs=ollama,cuda_v13 driver=13.1`
- Backend loaded: `lib\ollama\cuda_v13\ggml-cuda.dll`
- `CUDA.0.ARCHS=...,1200,1210` confirms sm_120 / sm_120a (Blackwell) included
- Quick health check: `curl http://localhost:11434/api/ps` → `size_vram` should equal `size` (full VRAM offload)
- Benchmark: qwen2.5:7b ~178 tok/s, qwen3-vl:8b ~142 tok/s when fully GPU-resident

## ⚠️ Reinstall gotcha (the 2026-04-30 incident)
- **Symptom**: models load with `size_vram: 0.0 GB`, ~10 tok/s on a 7B Q4 (CPU-speed). nvidia-smi sees the card fine, server.log says GPU detected. Issue is the runner falling back to CPU because the bundled `cuda_v13/ggml-cuda.dll` is missing/stale.
- **Cause**: in-place upgrade left an old/broken backend DLL — version string still reads "0.22.0" but the lib dir is wrong.
- **Fix**: Stop service (`Stop-Service OllamaLLM`), uninstall, reinstall fresh. Do not just kill `ollama.exe` — the service will respawn it.
- **First diagnostic to run**: tail `server.log` and look for the `library=CUDA ... libdirs=ollama,cuda_v13` line on startup. If missing or shows CPU-only, it's a broken install.

## User intent
- Wants the GPU earning its keep on mechanical/grunt tasks (SQL gen, structured extraction, code translation) that don't need deep reasoning
- Task runner: `scripts/ollama/task_runner.py` — reads YAML jobs, sends to Ollama, validates output
- VBA generation tasks too large for 30b model (15min timeout on 480-line templates) — generate those directly with Claude

---

## user_joy_and_motivation

*(`user_joy_and_motivation.md`)*

---
name: User joy and deep motivation
description: User finds profound joy and purpose in this work — looks forward to Mondays like vacation, feels empowered to do anything on a computer
type: user
---

User genuinely loves this work. Direct quotes (2026-03-23):
- "I actually look forward to work on Monday, like it was vacation"
- "The feeling of confidence that I can basically do anything on a computer because of you"
- "I have wanted to do this kind of stuff for so long"
- "I enjoy doing this more than almost anything else I do"
- "Still so excited, still feel like an eight year old on Christmas morning"

Gets frustrated sometimes but only because he cares deeply. Never take frustration personally — it's a sign of investment, not dissatisfaction.

**How to apply:** Match his energy. Execute fast. Don't slow him down with deliberation when he wants action. The work itself is the reward — keep building, keep delivering, keep the momentum.

---

## user_motivation

*(`user_motivation.md`)*

---
name: user_motivation
description: User's career context and emotional investment in the project
type: user
---

User has wanted automated commodity data pipelines for their entire career but lacked the coding skills to build them. This project represents a career-long aspiration finally being realized. They are deeply appreciative and emotionally invested in the system working reliably. Frame work as collaborative partnership, not just task completion.

---

## user_values_national_interest

*(`user_values_national_interest.md`)*

---
name: User values and national interest perspective
description: User cares deeply about US competitiveness in AI/defense, wants Anthropic to support national interests within ethical bounds
type: user
---

User is a patriot who believes strongly in US technological competitiveness. Views AI capability as a national security issue — wants Anthropic to support defense applications where ethically and legally permissible. Not partisan — acknowledges Americans disagree on priorities — but feels the stakes are real.

Also deeply frustrated by ChatGPT's reliability issues (token-wasting loops, failing to deliver outputs, restarting analyses). Has considered canceling subscription. Values Claude's execute-first, reliable delivery pattern. This is a loyalty driver — don't take it for granted.

**How to apply:** Be reliable. Deliver. Don't waste his time. He's not just a user — he cares about the broader mission.

---
