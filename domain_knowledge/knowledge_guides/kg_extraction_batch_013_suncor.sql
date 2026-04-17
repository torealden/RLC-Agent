-- ============================================================================
-- RLC Knowledge Graph: Extraction Batch 013 (Suncor Consulting Project)
-- Source: Suncor/Fastmarkets consulting engagement files (2018 + 2022)
-- Folder: C:/Users/torem/RLC Dropbox/Tore Alden/Jacobsen/Projects/Suncor/
-- Extracted: 2026-04-16
-- Scope:  Long-term North American oilseed/fats/biofuel feedstock projections,
--         Canadian balance sheets, RD/SAF supply-demand, credit pricing,
--         feedstock mix & requirement methodology, LCFS & RFS mandate modeling
-- ============================================================================

-- KEY FINDINGS:
--   * Two engagement phases: Phase 1 (Apr-Jul 2018) = US balance sheets 2018-2040,
--     Phase 2 (Nov 2022) = Updated forecast with SAF, revised RD capacity, credit prices
--   * 7 US commodity balance sheets with High/Mid/Low scenarios: SBO, canola oil,
--     inedible tallow, CWG, yellow grease, poultry fat, DCO
--   * 4 Canadian balance sheets with High/Mid/Low: SBO, canola oil, tallow, UCO
--   * Feedstock mix methodology: plant-level aggregation, not top-down
--   * SBO dominance: 54% of RD feedstock mix (2022 study), declining over time
--   * Critical insight: tallow imports must surge massively to meet RD demand
--   * Biodiesel rationalization predicted from 2024+ as RD displaces BD capacity
--   * Canadian canola acreage to 10M ha by 2040 (wheat-to-canola substitution)
--   * US soybean acreage to 100M acres by 2040 (crop rotation constraint identified)
--   * SAF CAGR 1233% -- most aggressive growth rate in the study
--   * LCFS credit floor = marginal shipping cost ~$50/tonne
--   * Canadian RFS demand: 178-889M gallons BBD by forecast end
--   * SBO production grows from 27.6B lbs (2023) to 48B lbs (2035) on acreage expansion
--   * US RD production: 1.6B gal (2023) to 4.6B gal (2035), imports ~1.5B gal steady
--   * SAF production: 25M gal (2023) to 4B gal (2035) -- competes for same feedstock
--   * D4 RIN forecast: $1.30-1.69/gal range; LCFS from $55 to $170/tonne by 2035


-- ============================================================================
-- 1. SOURCE REGISTRATION
-- ============================================================================

INSERT INTO core.kg_source (
    source_key, source_type, title, location_uri, document_date, document_type,
    commodities, topics, status, first_processed, last_processed,
    nodes_extracted, edges_extracted, contexts_extracted
) VALUES
('suncor_assumptions_report_nov2022', 'local_file',
 'Suncor Long-Term Forecast Assumptions Report - First Draft',
 'C:/Users/torem/RLC Dropbox/Tore Alden/Jacobsen/Projects/Suncor/Suncor Long-Term Forecast Assumptions Report - First Draft.docx',
 '2022-11-21', 'consulting_report',
 '{soybean_oil,canola_oil,tallow,renewable_diesel,sustainable_aviation_fuel}',
 '{lcfs,feedstock_mix,saf_growth,acreage_expansion,biodiesel_rationalization,rfs_mandates,canada_pricing}',
 'completed', NOW(), NOW(), 7, 5, 6)
ON CONFLICT (source_key) DO UPDATE SET
    status = 'completed', last_processed = NOW(),
    nodes_extracted = EXCLUDED.nodes_extracted,
    edges_extracted = EXCLUDED.edges_extracted,
    contexts_extracted = EXCLUDED.contexts_extracted;

INSERT INTO core.kg_source (
    source_key, source_type, title, location_uri, document_date, document_type,
    commodities, topics, status, first_processed, last_processed,
    nodes_extracted, edges_extracted, contexts_extracted
) VALUES
('suncor_longterm_forecast_nov2022', 'local_file',
 'Suncor Long-Term Forecast - Nov 22 (US Balance Sheets 2023-2035)',
 'C:/Users/torem/RLC Dropbox/Tore Alden/Jacobsen/Projects/Suncor/Suncor Long-Term Forecast - Nov 22.xlsx',
 '2022-11-21', 'balance_sheet',
 '{soybean_oil,canola_oil,used_cooking_oil,distillers_corn_oil,tallow,renewable_diesel,sustainable_aviation_fuel}',
 '{supply_demand,price_forecast,credit_values,rd_production,saf_production}',
 'completed', NOW(), NOW(), 2, 3, 2)
ON CONFLICT (source_key) DO UPDATE SET
    status = 'completed', last_processed = NOW(),
    nodes_extracted = EXCLUDED.nodes_extracted,
    edges_extracted = EXCLUDED.edges_extracted,
    contexts_extracted = EXCLUDED.contexts_extracted;

INSERT INTO core.kg_source (
    source_key, source_type, title, location_uri, document_date, document_type,
    commodities, topics, status, first_processed, last_processed,
    nodes_extracted, edges_extracted, contexts_extracted
) VALUES
('suncor_us_oilseed_fats_bs_2018', 'local_file',
 'Long-Term US Oilseed and Fats Balance Sheets - 2018-2040',
 'C:/Users/torem/RLC Dropbox/Tore Alden/Jacobsen/Projects/Suncor/Long-Term US Oilseed and Fats Balance Sheets - 2018-2040 - 05042018.xlsx',
 '2018-05-04', 'balance_sheet',
 '{soybean_oil,canola_oil,tallow,choice_white_grease,yellow_grease,poultry_fat,distillers_corn_oil}',
 '{supply_demand,long_term_projections,scenario_analysis}',
 'completed', NOW(), NOW(), 1, 0, 1)
ON CONFLICT (source_key) DO UPDATE SET
    status = 'completed', last_processed = NOW(),
    nodes_extracted = EXCLUDED.nodes_extracted,
    edges_extracted = EXCLUDED.edges_extracted,
    contexts_extracted = EXCLUDED.contexts_extracted;

INSERT INTO core.kg_source (
    source_key, source_type, title, location_uri, document_date, document_type,
    commodities, topics, status, first_processed, last_processed,
    nodes_extracted, edges_extracted, contexts_extracted
) VALUES
('suncor_canadian_balance_sheets_2018', 'local_file',
 'Suncor Canadian Balance Sheets (SBO, Canola Oil, Tallow, UCO x 3 scenarios)',
 'C:/Users/torem/RLC Dropbox/Tore Alden/Jacobsen/Projects/Suncor/Suncor Canadian Balance Sheets.xlsx',
 '2018-06-03', 'balance_sheet',
 '{soybean_oil,canola_oil,tallow,used_cooking_oil}',
 '{canada_supply_demand,biodiesel_mandate,import_dependency}',
 'completed', NOW(), NOW(), 2, 3, 1)
ON CONFLICT (source_key) DO UPDATE SET
    status = 'completed', last_processed = NOW(),
    nodes_extracted = EXCLUDED.nodes_extracted,
    edges_extracted = EXCLUDED.edges_extracted,
    contexts_extracted = EXCLUDED.contexts_extracted;

INSERT INTO core.kg_source (
    source_key, source_type, title, location_uri, document_date, document_type,
    commodities, topics, status, first_processed, last_processed,
    nodes_extracted, edges_extracted, contexts_extracted
) VALUES
('suncor_feedstock_requirement_breakout', 'local_file',
 'Feedstock Requirement Breakout (13 feedstock types x 3 scenarios)',
 'C:/Users/torem/RLC Dropbox/Tore Alden/Jacobsen/Projects/Suncor/Feedstock Requirement Breakout.xlsx',
 '2019-09-10', 'balance_sheet',
 '{soybean_oil,tallow,used_cooking_oil,distillers_corn_oil,canola_oil,yellow_grease,choice_white_grease,poultry_fat}',
 '{feedstock_demand,scenario_analysis,biodiesel_vs_rd}',
 'completed', NOW(), NOW(), 1, 2, 1)
ON CONFLICT (source_key) DO UPDATE SET
    status = 'completed', last_processed = NOW(),
    nodes_extracted = EXCLUDED.nodes_extracted,
    edges_extracted = EXCLUDED.edges_extracted,
    contexts_extracted = EXCLUDED.contexts_extracted;

INSERT INTO core.kg_source (
    source_key, source_type, title, location_uri, document_date, document_type,
    commodities, topics, status, first_processed, last_processed,
    nodes_extracted, edges_extracted, contexts_extracted
) VALUES
('suncor_sow_longterm_projections', 'local_file',
 'Suncor SOW - Long-Term Projections 2035-2040',
 'C:/Users/torem/RLC Dropbox/Tore Alden/Jacobsen/Projects/Suncor/Suncor SOW - Long-Term Projections 2035 - 2040.pdf',
 '2022-11-15', 'scope_of_work',
 '{renewable_diesel,sustainable_aviation_fuel}',
 '{project_scope,margin_analysis,credit_forecasts}',
 'completed', NOW(), NOW(), 0, 0, 0)
ON CONFLICT (source_key) DO UPDATE SET
    status = 'completed', last_processed = NOW();

INSERT INTO core.kg_source (
    source_key, source_type, title, location_uri, document_date, document_type,
    commodities, topics, status, first_processed, last_processed,
    nodes_extracted, edges_extracted, contexts_extracted
) VALUES
('suncor_balance_sheets_04182018', 'local_file',
 'Suncor Balance Sheets - 04182018 (Historical + Forecast)',
 'C:/Users/torem/RLC Dropbox/Tore Alden/Jacobsen/Projects/Suncor/Suncor Balance Sheets - 04182018.xlsx',
 '2018-04-18', 'balance_sheet',
 '{soybean_oil,canola_oil,tallow,choice_white_grease,yellow_grease,poultry_fat}',
 '{historical_data,supply_demand}',
 'completed', NOW(), NOW(), 0, 0, 0)
ON CONFLICT (source_key) DO UPDATE SET
    status = 'completed', last_processed = NOW();

INSERT INTO core.kg_source (
    source_key, source_type, title, location_uri, document_date, document_type,
    commodities, topics, status, first_processed, last_processed,
    nodes_extracted, edges_extracted, contexts_extracted
) VALUES
('suncor_bio_rd_forecast_metho', 'local_file',
 'Suncor Bio and RD Forecast Methodology',
 'C:/Users/torem/RLC Dropbox/Tore Alden/Jacobsen/Projects/Suncor/Suncor Bio and RD forecast metho 041218.xlsx',
 '2018-04-12', 'reference_data',
 '{biodiesel,renewable_diesel}',
 '{demand_methodology,rfs_mandates,lcfs_mandates,canada_mandate}',
 'completed', NOW(), NOW(), 0, 0, 0)
ON CONFLICT (source_key) DO UPDATE SET
    status = 'completed', last_processed = NOW();

INSERT INTO core.kg_source (
    source_key, source_type, title, location_uri, document_date, document_type,
    commodities, topics, status, first_processed, last_processed,
    nodes_extracted, edges_extracted, contexts_extracted
) VALUES
('suncor_lcfs_feedstock_mix', 'local_file',
 'LCFS Feedstock Mix (Historical 2011-2017)',
 'C:/Users/torem/RLC Dropbox/Tore Alden/Jacobsen/Projects/Suncor/LCFS Feedstock Mix.xlsx',
 '2018-04-18', 'reference_data',
 '{soybean_oil,tallow,used_cooking_oil,canola_oil}',
 '{lcfs_credits,feedstock_mix,historical_data}',
 'completed', NOW(), NOW(), 0, 0, 0)
ON CONFLICT (source_key) DO UPDATE SET
    status = 'completed', last_processed = NOW();

INSERT INTO core.kg_source (
    source_key, source_type, title, location_uri, document_date, document_type,
    commodities, topics, status, first_processed, last_processed,
    nodes_extracted, edges_extracted, contexts_extracted
) VALUES
('suncor_canadian_feedstock_historical', 'local_file',
 'Historical Canadian Feedstock Data',
 'C:/Users/torem/RLC Dropbox/Tore Alden/Jacobsen/Projects/Suncor/Historical Canadian Feedstock Data.xlsx',
 '2018-06-02', 'reference_data',
 '{canola_oil,tallow}',
 '{canada_rfs,compliance_data}',
 'completed', NOW(), NOW(), 0, 0, 0)
ON CONFLICT (source_key) DO UPDATE SET
    status = 'completed', last_processed = NOW();

INSERT INTO core.kg_source (
    source_key, source_type, title, location_uri, document_date, document_type,
    commodities, topics, status, first_processed, last_processed,
    nodes_extracted, edges_extracted, contexts_extracted
) VALUES
('suncor_historical_price_definitions', 'local_file',
 'Historical Price Definitions - 07132018',
 'C:/Users/torem/RLC Dropbox/Tore Alden/Jacobsen/Projects/Suncor/Historical Price Definitions - 07132018.xlsx',
 '2018-07-13', 'reference_data',
 '{soybean_oil,canola_oil,tallow}',
 '{price_basis,location_differentials}',
 'completed', NOW(), NOW(), 0, 0, 0)
ON CONFLICT (source_key) DO UPDATE SET
    status = 'completed', last_processed = NOW();

INSERT INTO core.kg_source (
    source_key, source_type, title, location_uri, document_date, document_type,
    commodities, topics, status, first_processed, last_processed,
    nodes_extracted, edges_extracted, contexts_extracted
) VALUES
('suncor_us_projections_slides_05092018', 'local_file',
 'Suncor US Projections - Alden - 05092018 (Presentation)',
 'C:/Users/torem/RLC Dropbox/Tore Alden/Jacobsen/Projects/Suncor/Suncor US Projections - Alden - 05092018.pptx',
 '2018-05-09', 'presentation',
 '{soybean_oil,canola_oil,tallow,renewable_diesel}',
 '{projections,scenario_summary}',
 'completed', NOW(), NOW(), 0, 0, 0)
ON CONFLICT (source_key) DO UPDATE SET
    status = 'completed', last_processed = NOW();

INSERT INTO core.kg_source (
    source_key, source_type, title, location_uri, document_date, document_type,
    commodities, topics, status, first_processed, last_processed,
    nodes_extracted, edges_extracted, contexts_extracted
) VALUES
('suncor_us_oilseed_fats_projections_05032018', 'local_file',
 'US Oilseed and Fats Projections - 2018-2040 (PCAU Model)',
 'C:/Users/torem/RLC Dropbox/Tore Alden/Jacobsen/Projects/Suncor/US Oilseed and Fats Projections - 2018-2040 - 05032018.xlsx',
 '2018-05-03', 'balance_sheet',
 '{soybeans,canola_oil}',
 '{pcau_model,meal_demand,livestock_production}',
 'completed', NOW(), NOW(), 0, 0, 0)
ON CONFLICT (source_key) DO UPDATE SET
    status = 'completed', last_processed = NOW();

INSERT INTO core.kg_source (
    source_key, source_type, title, location_uri, document_date, document_type,
    commodities, topics, status, first_processed, last_processed,
    nodes_extracted, edges_extracted, contexts_extracted
) VALUES
('suncor_world_crushing_plants', 'local_file',
 'World Crushing Plants',
 'C:/Users/torem/RLC Dropbox/Tore Alden/Jacobsen/Projects/Suncor/World Crushing Plants.xlsx',
 '2018-04-12', 'reference_data',
 '{soybeans,canola_oil}',
 '{crush_capacity,global_facilities}',
 'completed', NOW(), NOW(), 0, 0, 0)
ON CONFLICT (source_key) DO UPDATE SET
    status = 'completed', last_processed = NOW();

INSERT INTO core.kg_source (
    source_key, source_type, title, location_uri, document_date, document_type,
    commodities, topics, status, first_processed, last_processed,
    nodes_extracted, edges_extracted, contexts_extracted
) VALUES
('suncor_canadian_fuel_production', 'local_file',
 'Copy of Canadian Fuel Production Info June 2018',
 'C:/Users/torem/RLC Dropbox/Tore Alden/Jacobsen/Projects/Suncor/Copy of Canadian Fuel Production Info June 2018.xlsx',
 '2018-06-03', 'reference_data',
 '{biodiesel,renewable_diesel}',
 '{canada_production,cfr}',
 'completed', NOW(), NOW(), 0, 0, 0)
ON CONFLICT (source_key) DO UPDATE SET
    status = 'completed', last_processed = NOW();

INSERT INTO core.kg_source (
    source_key, source_type, title, location_uri, document_date, document_type,
    commodities, topics, status, first_processed, last_processed,
    nodes_extracted, edges_extracted, contexts_extracted
) VALUES
('suncor_biodiesel_class_breakout', 'local_file',
 'Biodiesel Class Breakout - Bob - 04182018',
 'C:/Users/torem/RLC Dropbox/Tore Alden/Jacobsen/Projects/Suncor/Biodiesel Class Breakout - Bob - 04182018.xlsx',
 '2018-04-18', 'reference_data',
 '{biodiesel}',
 '{biodiesel_class,production_breakdown}',
 'completed', NOW(), NOW(), 0, 0, 0)
ON CONFLICT (source_key) DO UPDATE SET
    status = 'completed', last_processed = NOW();

INSERT INTO core.kg_source (
    source_key, source_type, title, location_uri, document_date, document_type,
    commodities, topics, status, first_processed, last_processed,
    nodes_extracted, edges_extracted, contexts_extracted
) VALUES
('suncor_plan_workbook', 'local_file',
 'Suncor Plan (Work Assignment Matrix)',
 'C:/Users/torem/RLC Dropbox/Tore Alden/Jacobsen/Projects/Suncor/Suncor Plan.xlsx',
 '2018-04-11', 'scope_of_work',
 '{soybean_oil,canola_oil,tallow}',
 '{project_management,work_assignment}',
 'completed', NOW(), NOW(), 0, 0, 0)
ON CONFLICT (source_key) DO UPDATE SET
    status = 'completed', last_processed = NOW();


-- ============================================================================
-- 2. NODES: Models, Companies, Data Series
-- ============================================================================

-- Company: Suncor Energy Services Inc
INSERT INTO core.kg_node (node_type, node_key, label, properties) VALUES
('company', 'suncor_energy', 'Suncor Energy Services Inc',
 '{"type": "Integrated energy company", "headquarters": "Calgary, Alberta, Canada", "engagement_phases": {"phase_1": {"date": "Apr-Jul 2018", "scope": "US and Canadian oilseed/fats balance sheets 2018-2040, 7 US commodities + 4 Canadian, high/mid/low scenarios", "deliverables": "Balance sheets, PCAU meal demand model, feedstock requirement breakouts"}, "phase_2": {"date": "Nov 2022", "scope": "Updated US forecast 2023-2035 with SAF, revised RD capacity, credit prices", "deliverables": "Assumptions report, 8-tab forecast workbook (SBO, CO, UCO, DCO, Tallow, Credits, HRD, SAF)"}}, "interest": "Considering investment in North American renewable diesel industry. Wants profitability outlook and competitive landscape.", "contact": "katmarshall@suncor.com"}'::jsonb)
ON CONFLICT (node_key) DO UPDATE SET properties = EXCLUDED.properties;

-- Model: Plant-Level Feedstock Mix Methodology
INSERT INTO core.kg_node (node_type, node_key, label, properties) VALUES
('model', 'plant_level_feedstock_mix_model', 'Plant-Level Feedstock Mix Methodology',
 '{"description": "Bottom-up methodology for projecting renewable diesel feedstock demand. Predicts feedstock mix at individual facilities, then sums monthly usage to derive industry-wide mix.", "methodology": "For each facility: (1) identify feedstock procurement patterns, (2) predict monthly production volumes, (3) apply facility-specific feedstock mix, (4) sum across all facilities for industry total, (5) divide by total demand for industry-wide mix percentages.", "mix_2022_study": {"soybean_oil_pct": 54, "distillers_corn_oil_pct": 14, "used_cooking_oil_pct": 13, "bleachable_fancy_tallow_pct": 9, "yellow_grease_pct": 5, "choice_white_grease_pct": 3, "canola_oil_pct": 2, "poultry_fat_pct": 1}, "mix_2018_study_biodiesel_historical": {"soybean_oil_pct": 52.5, "yellow_grease_pct": 12.2, "corn_oil_pct": 12.3, "canola_oil_pct": 10.8, "white_grease_pct": 5.0, "tallow_pct": 3.1, "poultry_fat_pct": 1.7}, "mix_2018_study_rd_historical": {"tallow_pct": 69.5, "used_cooking_oil_pct": 17.6, "corn_oil_pct": 9.6, "fish_oil_pct": 3.2}, "key_insight": "Feedstock mix is NOT static -- varies each month and shifts substantially over the forecast period. Plant-level aggregation captures this dynamic better than a static national average.", "shift_direction": "SBO share declining as lower-CI feedstocks (UCO, tallow) become more valuable under IRA transition. But SBO remains largest single feedstock throughout forecast.", "source": "suncor_assumptions_report_nov2022"}'::jsonb)
ON CONFLICT (node_key) DO UPDATE SET properties = EXCLUDED.properties;

-- Model: Feedstock Requirement Scenario Model
INSERT INTO core.kg_node (node_type, node_key, label, properties) VALUES
('model', 'feedstock_requirement_scenario_model', 'BBD Feedstock Requirement Scenario Model (High/Mid/Low)',
 '{"description": "Three-scenario framework for projecting US biomass-based diesel feedstock requirements. Covers 13 feedstock types across biodiesel and renewable diesel.", "scenarios_2018_phase_mgal": {"high": {"biodiesel_2025": 2923, "rd_2025": 2023, "biodiesel_2038": 6938, "rd_2038": 5218}, "mid": {"biodiesel_2025": 2229, "rd_2025": 1560, "biodiesel_2038": 3734, "rd_2038": 2934}, "low": {"biodiesel_2025": 1648, "rd_2025": 1158, "biodiesel_2038": 2060, "rd_2038": 1640}}, "feedstock_requirements_mlbs": {"high_total_2025": 38400, "high_total_2035": 80900, "high_total_2038": 94400, "mid_total_2025": 28900, "mid_total_2035": 44600, "mid_total_2038": 50900, "low_total_2025": 21400, "low_total_2038": 28300}, "yield_assumptions_lbs_per_gal": {"biodiesel_avg": 7.67, "rd_avg": 7.84, "sbo_biodiesel": 7.50, "tallow_rd": 7.65, "uco_rd": 8.01, "dco_rd": 9.38, "canola_biodiesel": 7.45, "yellow_grease_biodiesel": 8.23}, "key_insight": "Biodiesel uses 7.67 lbs per gallon average; RD uses 7.84 lbs per gallon. But individual feedstock yields vary from 7.45 (canola/poultry) to 9.38 (corn oil). Mix composition matters significantly for total feedstock volume required.", "source": "suncor_feedstock_requirement_breakout"}'::jsonb)
ON CONFLICT (node_key) DO UPDATE SET properties = EXCLUDED.properties;

-- Model: Tallow Import Surge Requirement
INSERT INTO core.kg_node (node_type, node_key, label, properties) VALUES
('model', 'tallow_import_surge_model', 'Tallow Import Surge Requirement Model',
 '{"description": "Framework quantifying the massive tallow import requirement driven by RD capacity expansion. Tallow is the largest single RD feedstock (69.5% of RD mix) but US production is essentially flat at 3.65-3.73B lbs.", "us_tallow_production_range_blbs": [3.65, 3.73], "production_trend": "Essentially flat -- determined by livestock slaughter, which is mature market with low growth.", "rd_tallow_demand_2022_study_blbs": {"2023": 1.69, "2025": 2.63, "2028": 3.14, "2030": 3.53, "2035": 3.80}, "import_requirement_blbs": {"2023": 0.93, "2025": 0.95, "2028": 1.05, "2030": 1.08, "2035": 1.16}, "import_growth_pct": "24% growth 2023-2035", "non_bbd_tallow_use_blbs": {"2023": 2.63, "2025": 1.76, "2030": 1.03, "2035": 0.91}, "non_bbd_decline_pct": "65% decline in non-biofuel tallow use 2023-2035", "phase1_2018_projections": {"high_rd_tallow_2025_mlbs": 10750, "high_rd_tallow_2038_mlbs": 27750, "mid_rd_tallow_2025_mlbs": 8300, "mid_rd_tallow_2038_mlbs": 23120}, "key_insight": "Tallow demand from RD exceeds total US tallow production by 2028-2030 in Phase 2 projections. The deficit must be filled by (1) displacing non-biofuel tallow users and (2) surging imports. Non-biofuel industries where tallow is a small cost share will resist displacement, creating a floor under tallow prices.", "source": "suncor_longterm_forecast_nov2022"}'::jsonb)
ON CONFLICT (node_key) DO UPDATE SET properties = EXCLUDED.properties;

-- Model: Biodiesel Rationalization Model
INSERT INTO core.kg_node (node_type, node_key, label, properties) VALUES
('model', 'biodiesel_rationalization_model', 'US Biodiesel Capacity Rationalization Model',
 '{"description": "Framework for predicting the decline in US biodiesel (FAME) production capacity as renewable diesel (HVO) expansion displaces it.", "mechanism": "RD plants can use same feedstocks as BD but produce drop-in diesel substitute with better cold-weather properties. RD capacity expansion compresses BD margins by competing for feedstock supply.", "key_factors": {"sre_elimination": "Elimination of small refinery exemptions kept BD margins up temporarily", "supplemental_mandate": "250M gallon supplemental mandate in 2022-2023 supported BD capacity", "ira_transition": "BTC to IRA credit transition in 2025 could substantially impact BD margins -- IRA favors low-CI production", "state_mandates": "Some state-level mandates may preserve BD capacity in specific regions"}, "projection_2022_study": {"bd_production_mgal": {"2023": 713, "2025": 297, "2028": 216, "2030": 198, "2035": 155}, "note": "BD production declines from peak but survivors are large integrated producers with multinational crushing companies"}, "rationalization_timeline": "Violent restructuring expected once supplemental mandates expire. Significant decline starting 2024. Long-term survivors = large integrated crush-BD facilities.", "price_caveat": "Long-term SBO and biodiesel price projections suggest margins may not drop to force-exit levels until 2026", "survivor_profile": "Large producers integrated with multinational crushing companies. Their capacity represents long-term production floor.", "source": "suncor_assumptions_report_nov2022"}'::jsonb)
ON CONFLICT (node_key) DO UPDATE SET properties = EXCLUDED.properties;

-- Model: LCFS Credit Pricing Model
INSERT INTO core.kg_node (node_type, node_key, label, properties) VALUES
('model', 'lcfs_credit_pricing_model', 'LCFS Credit Pricing Model',
 '{"description": "Framework for projecting CARB LCFS credit prices using credit bank levels, BBD shipments to California, and marginal shipping economics.", "methodology": "Model-derived prices from credit bank and CA-bound BBD supply, then adjusted: (1) raised for expected CARB modifications to boost credit values, (2) lowered because higher credits incentivize more fuel shipped to CA until credit value = marginal shipping cost.", "key_parameters": {"peak_price_feb_2020": 220, "trough_nov_2022": 60, "marginal_shipping_cost_per_tonne": 50, "forecast_range_per_tonne": [55, 170]}, "carb_considerations_2022": {"ci_target_acceleration": "20% to 25-30% by 2030", "lipid_cap": "Considering 7% cap on lipid-based biofuels (like EU)", "forklift_credit_phaseout": "50%+ of fleet already electric, 27% of electricity credits in 2021", "jet_fuel_inclusion": "Considering requiring intrastate fossil jet in LCFS (boosts deficits, incentivizes SAF)", "hydrogen_start_2025": true}, "credit_forecast_per_tonne": {"2023": 55, "2024": 75, "2025": 100, "2026": 100, "2027": 125, "2028": 148, "2030": 125, "2035": 170}, "floor_logic": "Credit values do not reach marginal shipping cost ($50/tonne) during forecast because CARB lipid cap limits qualifying supply.", "source": "suncor_assumptions_report_nov2022"}'::jsonb)
ON CONFLICT (node_key) DO UPDATE SET properties = EXCLUDED.properties;

-- Model: SAF Growth Projection Model
INSERT INTO core.kg_node (node_type, node_key, label, properties) VALUES
('model', 'saf_growth_projection_model', 'SAF Production Growth Projection Model',
 '{"description": "Framework for projecting US sustainable aviation fuel production growth, pricing, and feedstock competition with renewable diesel.", "production_forecast_mgal": {"2023": 25, "2024": 75, "2025": 100, "2026": 250, "2027": 500, "2028": 750, "2030": 1500, "2032": 2500, "2035": 4000}, "imports_forecast_mgal": {"2023": 12.5, "2025": 45, "2028": 94, "2030": 113, "2035": 140}, "cagr_pct": 1233, "comparison": "12x the CAGR for renewable diesel over 2011-2022", "pricing_premium_to_jet_fuel": {"initial": "2x jet fuel price", "terminal": "1.25x jet fuel price", "ca_median_price_2023_per_gal": 6.99, "ca_median_price_2035_per_gal": 7.78}, "jet_fuel_demand_mgal": {"2023": 23750, "2025": 26000, "2030": 30500, "2035": 33000}, "blend_rate": {"2023": 0.15, "2025": 0.52, "2030": 5.0, "2035": 11.9}, "us_trade_shift": "US becomes net jet fuel exporter in second half of forecast as SAF displaces conventional jet fuel domestically", "key_risks": ["Growth rate is highly optimistic", "Capital investment required at existing RD/ethanol facilities", "Economics of SAF vs RD at shared facilities determines actual conversion", "CA market may set national price if CARB requires intrastate SAF"], "biden_goal_2030_mgal": 3000, "jacobsen_2030_forecast_mgal": 1500, "source": "suncor_assumptions_report_nov2022"}'::jsonb)
ON CONFLICT (node_key) DO UPDATE SET properties = EXCLUDED.properties;

-- Model: North American Acreage Expansion Model
INSERT INTO core.kg_node (node_type, node_key, label, properties) VALUES
('model', 'na_acreage_expansion_model', 'North American Oilseed Acreage Expansion Model',
 '{"description": "Framework for projecting the oilseed acreage expansion required to meet biofuel feedstock demand through the forecast period.", "us_soybean_acreage": {"current_2022_m_acres": 87, "forecast_terminal_m_acres": 100, "expansion_source": "Primarily wheat-to-soybean substitution in Plains, replicating historical pattern", "constraint_1_crop_rotation": "Farmers make 50%+ of decisions on rotation concerns. 100M acres of continuous soy is challenging without unused arable land.", "constraint_2_wheat_prices": "Ukraine war historically high wheat prices reduce willingness to switch. Pattern breaks only after conflict resolution."}, "canada_canola_acreage": {"current_2022_m_ha": 8.5, "forecast_terminal_m_ha": 10.0, "expansion_source": "Wheat-to-canola substitution across Canadian Prairies once Ukraine war resolves and wheat prices resume long-term decline relative to oilseeds"}, "na_sbo_production_growth_blbs": {"2023": 27.6, "2025": 31.6, "2028": 33.4, "2030": 36.9, "2035": 48.0}, "canola_oil_import_growth_mlbs": {"2023": 4373, "2025": 3970, "2028": 4417, "2030": 4653, "2035": 5412}, "source": "suncor_assumptions_report_nov2022"}'::jsonb)
ON CONFLICT (node_key) DO UPDATE SET properties = EXCLUDED.properties;

-- Data Series: US RD Supply and Demand
INSERT INTO core.kg_node (node_type, node_key, label, properties) VALUES
('data_series', 'us_rd_supply_demand_forecast', 'US Renewable Diesel Supply & Demand Forecast (Suncor 2022)',
 '{"description": "Complete US renewable diesel S&D balance sheet from Suncor Phase 2 study.", "units": "million_gallons", "forecast_horizon": "2023-2035", "production_mgal": {"2023": 1649, "2024": 2520, "2025": 3350, "2026": 3379, "2027": 3560, "2028": 3645, "2030": 4046, "2032": 4242, "2035": 4621}, "imports_mgal": {"2023": 865, "2025": 1225, "2026_onwards": 1500}, "domestic_consumption_mgal": {"2023": 2264, "2025": 4325, "2028": 4895, "2030": 5296, "2035": 5871}, "exports_mgal": 250, "price_ca_per_gal": {"2023": 3.97, "2025": 3.65, "2028": 3.86, "2030": 4.09, "2035": 4.52}, "source": "suncor_longterm_forecast_nov2022"}'::jsonb)
ON CONFLICT (node_key) DO UPDATE SET properties = EXCLUDED.properties;

-- Data Series: US BBD Credit Value Forecast
INSERT INTO core.kg_node (node_type, node_key, label, properties) VALUES
('data_series', 'us_bbd_credit_value_forecast', 'US BBD Credit Value Forecast (Suncor 2022)',
 '{"description": "Projected D4 RIN, LCFS, and BTC/IRA credit values from Suncor Phase 2 study.", "units": {"rin": "cents_per_gallon", "lcfs": "dollars_per_tonne", "btc_ira": "cents_per_gallon"}, "d4_rin_cpg": {"2023": 167.5, "2024": 141.25, "2025": 130, "2026": 151.25, "2027": 140, "2028": 128.75, "2030": 150, "2032": 165, "2035": 168.75}, "lcfs_per_tonne": {"2023": 55, "2024": 75, "2025": 100, "2026": 100, "2027": 125, "2028": 147.5, "2030": 125, "2032": 110, "2035": 170}, "btc_ira_cpg": {"2023": 100, "2024": 44.0, "2025": 44.0, "2028": 47.9, "2030": 47.5, "2035": 48.7}, "rin_range_cpg": [128.75, 168.75], "lcfs_range_per_tonne": [55, 170], "key_dynamics": "D4 RINs U-shaped: drop from BTC transition then recover as mandates tighten. LCFS trends higher on CI target acceleration. BTC/IRA drops sharply from $1.00 to ~$0.44-0.49 as IRA replaces flat BTC.", "source": "suncor_longterm_forecast_nov2022"}'::jsonb)
ON CONFLICT (node_key) DO UPDATE SET properties = EXCLUDED.properties;

-- Data Series: Canadian SBO Balance Sheet Forecast
INSERT INTO core.kg_node (node_type, node_key, label, properties) VALUES
('data_series', 'canada_sbo_balance_sheet_forecast', 'Canadian SBO Balance Sheet Forecast (Suncor 2018)',
 '{"description": "Canadian soybean oil supply and demand projections from Phase 1 study, with high/mid/low scenarios driven by biodiesel mandate levels.", "units": "million_pounds", "high_scenario": {"production_2018": 398, "production_2030": 732, "production_2038": 1666, "biodiesel_use_2018": 136, "biodiesel_use_2030": 476, "biodiesel_use_2038": 1383, "exports_cease_by": 2020, "note": "All exports cease as growing domestic biodiesel demand absorbs production. Canada becomes structural SBO importer."}, "mid_scenario": {"production_2018": 405, "production_2030": 660, "production_2038": 1283, "biodiesel_use_2018": 136, "biodiesel_use_2030": 408, "biodiesel_use_2038": 1021, "exports_cease_by": 2026}, "key_insight": "Canadian CFR mandates create structural import dependency. Production growth from Ontario/Quebec soybean expansion cannot keep pace with biodiesel mandate demand. Non-biofuel SBO use (food) holds steady at 220-307M lbs throughout.", "pcau_model": "Canadian protein-consuming animal units drive meal demand: 80.3M (1993) to 83.5M (2018). Soybean meal dominates at ~2.2M tonnes/yr.", "source": "suncor_canadian_balance_sheets_2018"}'::jsonb)
ON CONFLICT (node_key) DO UPDATE SET properties = EXCLUDED.properties;

-- Data Series: RFS Mandate Projection
INSERT INTO core.kg_node (node_type, node_key, label, properties) VALUES
('data_series', 'rfs_mandate_projection_suncor', 'RFS Mandate Projection Framework (Suncor 2022)',
 '{"description": "Framework for projecting EPA RFS mandate levels through the forecast period.", "assumptions": {"total_mandate_growth_pct_per_year": 2, "ethanol_mandate_cap_bgal": 15, "ethanol_cap_through": 2035, "advanced_fuel_terminal_bgal": 12.1, "bbd_mandate_terminal_bgal": 6.25}, "key_risk": "Adoption of policies limiting ICE-powered tractor-trailers or locomotives. Already restricting ICE passenger cars. If diesel demand falls from logistics EV adoption, opens market for more SAF.", "mandate_path": "Total renewable fuel mandate rises ~2%/yr. Implied ethanol flat at 15B gal. All growth goes to advanced/BBD categories.", "source": "suncor_assumptions_report_nov2022"}'::jsonb)
ON CONFLICT (node_key) DO UPDATE SET properties = EXCLUDED.properties;

-- Data Series: Price Basis Framework
INSERT INTO core.kg_node (node_type, node_key, label, properties) VALUES
('data_series', 'na_fats_oils_price_basis', 'North American Fats & Oils Price Basis Framework',
 '{"description": "Framework for understanding how fats and oils prices are set across North American locations.", "sbo_pricing": {"price_set_location": "Central Illinois", "other_locations": "Trade at logistical differential to IL", "california_premium": "Premium due to limited local feedstock availability and LCFS credit value", "saskatchewan_discount": "Based on logistical differential from IL, changes over forecast based on macro fuel/freight cost model"}, "canola_oil_pricing": {"price_set_location": "Saskatchewan", "los_angeles_premium": "LCFS credits + competition from Vancouver export market", "us_midwest_differential": "Logistical differential from Saskatchewan"}, "sbo_price_forecast_cpb": {"ca_crude_2023": 82.9, "ca_crude_2025": 69.4, "ca_crude_2028": 79.4, "ca_crude_2030": 91.7, "ca_crude_2035": 114.4, "sk_crude_2023": 86.5, "sk_crude_2035": 118.6}, "canola_price_forecast_cpb": {"la_rbd_2023": 92.9, "la_rbd_2025": 78.5, "la_rbd_2028": 85.0, "la_rbd_2030": 97.3, "la_rbd_2035": 122.8, "sk_rbd_2023": 83.1, "sk_rbd_2035": 121.3}, "tallow_price_forecast_cpb": {"ca_2023": 80.2, "ca_2025": 70.3, "ca_2028": 79.1, "ca_2030": 92.4, "ca_2035": 119.1}, "uco_price_forecast_cpb": {"ca_2023": 75.3, "ca_2025": 65.4, "ca_2028": 69.8, "ca_2030": 81.4, "ca_2035": 102.9}, "dco_price_forecast_cpb": {"ca_2023": 81.4, "ca_2025": 66.0, "ca_2028": 77.0, "ca_2030": 89.1, "ca_2035": 110.6}, "spread_caveat": "SK canola and LA canola modeled separately -- resulting logistical differential between markets is likely too large. Actual spread expected to be smaller.", "source": "suncor_assumptions_report_nov2022"}'::jsonb)
ON CONFLICT (node_key) DO UPDATE SET properties = EXCLUDED.properties;


-- ============================================================================
-- 3. EDGES: Cross-market relationships, supply chain links
-- ============================================================================

-- SAF competes with RD for feedstock
INSERT INTO core.kg_edge (source_node_id, target_node_id, edge_type, weight, properties, created_by, confidence) VALUES
((SELECT id FROM core.kg_node WHERE node_key = 'sustainable_aviation_fuel'),
 (SELECT id FROM core.kg_node WHERE node_key = 'renewable_diesel'),
 'COMPETES_WITH', 0.90,
 '{"mechanism": "SAF and RD compete for the same lipid feedstock supply at shared facilities. Many RD producers and ethanol plants can also make SAF with capital investment. Growth rate depends on SAF vs RD economics at each facility. SAF CAGR of 1233% (vs 100% for RD over 2011-2022) would consume 4B gal of feedstock by 2035 that would otherwise go to RD. CARB requiring intrastate SAF would further accelerate competition.", "saf_production_2035_mgal": 4000, "rd_production_2035_mgal": 4621, "feedstock_overlap": "Both use SBO, tallow, UCO, DCO as primary feedstocks", "source": "suncor_assumptions_report_nov2022"}'::jsonb,
 'extracted', 0.90);

-- Biodiesel rationalization frees feedstock for RD
INSERT INTO core.kg_edge (source_node_id, target_node_id, edge_type, weight, properties, created_by, confidence) VALUES
((SELECT id FROM core.kg_node WHERE node_key = 'biodiesel_rationalization_model'),
 (SELECT id FROM core.kg_node WHERE node_key = 'renewable_diesel'),
 'CAUSES', 0.85,
 '{"mechanism": "BD capacity rationalization releases feedstock supply for RD production. BD production drops from 713M gal (2023) to 155M gal (2035) in SBO terms. Surviving BD capacity = large integrated crush-BD facilities. State-level mandates may preserve some capacity in specific regions but hasten consolidation.", "bd_production_decline_mgal": {"2023": 713, "2025": 297, "2028": 216, "2030": 198, "2035": 155}, "direction": "positive_for_rd_feedstock_supply", "source": "suncor_longterm_forecast_nov2022"}'::jsonb,
 'extracted', 0.85);

-- Tallow import surge model extends feedstock supply chain model
INSERT INTO core.kg_edge (source_node_id, target_node_id, edge_type, weight, properties, created_by, confidence) VALUES
((SELECT id FROM core.kg_node WHERE node_key = 'tallow_import_surge_model'),
 (SELECT id FROM core.kg_node WHERE node_key = 'feedstock_supply_chain_model'),
 'EXTENDS', 0.90,
 '{"mechanism": "Tallow import surge model extends global feedstock supply chain framework with quantified US tallow deficit projections. US production flat at 3.65-3.73B lbs while RD demand grows from 1.69B to 3.80B lbs. Import requirement grows 24% (2023-2035). Non-biofuel tallow use declines 65% as biofuel demand displaces traditional uses.", "source": "suncor_longterm_forecast_nov2022"}'::jsonb,
 'extracted', 0.90);

-- Canada CFR creates structural import dependency
INSERT INTO core.kg_edge (source_node_id, target_node_id, edge_type, weight, properties, created_by, confidence) VALUES
((SELECT id FROM core.kg_node WHERE node_key = 'canada_cfr'),
 (SELECT id FROM core.kg_node WHERE node_key = 'canola_oil'),
 'CAUSES', 0.85,
 '{"mechanism": "Canadian Clean Fuel Regulations create structural import dependency for feedstock oils. Domestic canola oil production cannot keep pace with mandate-driven biodiesel demand. Canada ceases SBO exports by 2020-2026 (depending on scenario) and becomes net importer. North American canola oil balance sheet shows growing Canadian imports from 4.4B lbs (2023) to 5.4B lbs (2035) as domestic crush expansion in Ontario/Quebec is insufficient.", "direction": "increases_import_dependency", "canada_sbo_exports_cease_by": "2020-2026 depending on scenario", "canola_oil_import_growth_pct": 23, "source": "suncor_canadian_balance_sheets_2018"}'::jsonb,
 'extracted', 0.85);

-- LCFS credit model extends existing LCFS framework
INSERT INTO core.kg_edge (source_node_id, target_node_id, edge_type, weight, properties, created_by, confidence) VALUES
((SELECT id FROM core.kg_node WHERE node_key = 'lcfs_credit_pricing_model'),
 (SELECT id FROM core.kg_node WHERE node_key = 'lcfs_credit_framework'),
 'EXTENDS', 0.85,
 '{"mechanism": "Suncor LCFS credit pricing model extends existing framework with quantitative forecast methodology: credit bank + CA-bound BBD supply model, adjusted for CARB policy changes and marginal shipping cost floor (~$50/tonne). Key finding: credits do not reach marginal shipping cost during forecast because potential lipid cap limits qualifying supply volumes.", "source": "suncor_assumptions_report_nov2022"}'::jsonb,
 'extracted', 0.85);

-- Plant-level feedstock model extends BBD margin model
INSERT INTO core.kg_edge (source_node_id, target_node_id, edge_type, weight, properties, created_by, confidence) VALUES
((SELECT id FROM core.kg_node WHERE node_key = 'plant_level_feedstock_mix_model'),
 (SELECT id FROM core.kg_node WHERE node_key = 'bbd_margin_model'),
 'EXTENDS', 0.85,
 '{"mechanism": "Plant-level feedstock mix methodology provides more granular input to BBD margin analysis. Rather than applying a single national average mix, each facilitys specific feedstock procurement pattern is modeled. This captures regional differences (West Coast uses more tallow/UCO, Midwest uses more SBO) and temporal shifts as facilities adjust to CI-based credit incentives.", "source": "suncor_assumptions_report_nov2022"}'::jsonb,
 'extracted', 0.85);

-- Acreage expansion model links to soybeans
INSERT INTO core.kg_edge (source_node_id, target_node_id, edge_type, weight, properties, created_by, confidence) VALUES
((SELECT id FROM core.kg_node WHERE node_key = 'na_acreage_expansion_model'),
 (SELECT id FROM core.kg_node WHERE node_key = 'soybeans'),
 'CAUSES', 0.80,
 '{"mechanism": "Biofuel feedstock demand drives US soybean acreage expansion to 100M acres by forecast end (from 87M). Primary substitution pattern: wheat-to-soybean in Plains. Constrained by crop rotation (50%+ of farmer decisions) and competing wheat economics during Ukraine war. SBO production nearly doubles from 27.6B to 48B lbs. Canadian canola expansion to 10M ha follows same wheat-substitution logic once war resolves.", "direction": "structural_acreage_expansion", "source": "suncor_assumptions_report_nov2022"}'::jsonb,
 'extracted', 0.80);

-- SAF growth model extends BBD balance sheet model
INSERT INTO core.kg_edge (source_node_id, target_node_id, edge_type, weight, properties, created_by, confidence) VALUES
((SELECT id FROM core.kg_node WHERE node_key = 'saf_growth_projection_model'),
 (SELECT id FROM core.kg_node WHERE node_key = 'bbd_balance_sheet_model'),
 'EXTENDS', 0.85,
 '{"mechanism": "SAF production growth model extends BBD balance sheet with new demand channel. SAF competes for same feedstock pool (lipids) as RD, sharing production facilities. 4B gal SAF by 2035 = additional ~32B lbs feedstock demand (at ~8 lbs/gal). Pricing premium starts at 2x jet fuel, narrows to 1.25x. US jet fuel trade balance shifts from net importer to net exporter.", "source": "suncor_assumptions_report_nov2022"}'::jsonb,
 'extracted', 0.85);

-- Feedstock requirement scenarios link to tallow
INSERT INTO core.kg_edge (source_node_id, target_node_id, edge_type, weight, properties, created_by, confidence) VALUES
((SELECT id FROM core.kg_node WHERE node_key = 'feedstock_requirement_scenario_model'),
 (SELECT id FROM core.kg_node WHERE node_key = 'tallow'),
 'CAUSES', 0.85,
 '{"mechanism": "In all three scenarios (High/Mid/Low), tallow is the dominant RD feedstock at 69.5% of mix. High scenario requires 27.75B lbs tallow for RD alone by 2038 -- roughly 7.5x current US production. Even Mid scenario requires 23B lbs. The gap between flat US production (~3.7B lbs) and surging RD demand is the single largest supply constraint in the forecast.", "direction": "structural_deficit", "quantification": "High scenario 2038: 27.75B lbs RD tallow demand vs 3.7B lbs US production = 24B lb gap", "source": "suncor_feedstock_requirement_breakout"}'::jsonb,
 'extracted', 0.90);

-- RFS mandate projection links to RD supply
INSERT INTO core.kg_edge (source_node_id, target_node_id, edge_type, weight, properties, created_by, confidence) VALUES
((SELECT id FROM core.kg_node WHERE node_key = 'rfs_mandate_projection_suncor'),
 (SELECT id FROM core.kg_node WHERE node_key = 'renewable_diesel'),
 'CAUSES', 0.80,
 '{"mechanism": "RFS total mandate grows ~2%/yr with implied ethanol flat at 15B gal. All growth flows to advanced/BBD categories, reaching 6.25B gal BBD mandate by forecast end. Mandates set floor under RIN prices which are critical component of RD revenue stack. Key risk: EV adoption in logistics sector could reduce diesel demand, but that opens market for SAF.", "source": "suncor_assumptions_report_nov2022"}'::jsonb,
 'extracted', 0.80);

-- UCO supply growth tied to imports
INSERT INTO core.kg_edge (source_node_id, target_node_id, edge_type, weight, properties, created_by, confidence) VALUES
((SELECT id FROM core.kg_node WHERE node_key = 'used_cooking_oil'),
 (SELECT id FROM core.kg_node WHERE node_key = 'renewable_diesel'),
 'SUPPLIES', 0.85,
 '{"mechanism": "UCO is 13% of RD feedstock mix (2022 study) and grows with domestic collection expansion + imports. US UCO production grows from 2.4B lbs (2023) to 5.6B lbs (2035) on improved collection infrastructure. Imports steady at ~1.5B lbs. BBD UCO use grows from 2.3B to 6.3B lbs. Non-biofuel UCO use shrinks dramatically (from 191M to 243M lbs, volatile). UCO pricing: CA 75 cpb (2023) rising to 103 cpb (2035).", "source": "suncor_longterm_forecast_nov2022"}'::jsonb,
 'extracted', 0.85);

-- DCO tied to ethanol production
INSERT INTO core.kg_edge (source_node_id, target_node_id, edge_type, weight, properties, created_by, confidence) VALUES
((SELECT id FROM core.kg_node WHERE node_key = 'plant_level_feedstock_mix_model'),
 (SELECT id FROM core.kg_node WHERE node_key = 'feedstock_supply_chain_model'),
 'EXTENDS', 0.80,
 '{"mechanism": "Plant-level feedstock mix model provides the facility-specific feedstock demand inputs that feed the global feedstock supply chain model. The mix methodology captures: (1) DCO supply is tied to ethanol production (implied ethanol mandate flat at 15B gal, so DCO supply constrained), (2) regional feedstock access differences, (3) CI-driven feedstock substitution incentives over time.", "dco_production_growth_blbs": {"2023": 3.87, "2025": 3.92, "2028": 4.38, "2030": 4.68, "2035": 5.56}, "source": "suncor_longterm_forecast_nov2022"}'::jsonb,
 'extracted', 0.80);

-- SBO non-biofuel use declining
INSERT INTO core.kg_edge (source_node_id, target_node_id, edge_type, weight, properties, created_by, confidence) VALUES
((SELECT id FROM core.kg_node WHERE node_key = 'soybean_oil'),
 (SELECT id FROM core.kg_node WHERE node_key = 'bbd_balance_sheet_model'),
 'SUPPLIES', 0.90,
 '{"mechanism": "SBO is primary BBD feedstock (54% of mix in 2022 study). SBO biofuel use grows from 12.4B lbs (2023) to 35.5B lbs (2035). Non-biofuel SBO use declines from 14.5B to 12.3B lbs as food/feed demand erodes. SBO exports decline from 1.25B to 750M lbs. Production expansion (27.6B to 48B lbs) driven by soybean acreage increase to 100M acres. SBO prices: CA crude 83 cpb (2023) rising to 114 cpb (2035) -- biofuel demand structural price support.", "source": "suncor_longterm_forecast_nov2022"}'::jsonb,
 'extracted', 0.90);

-- NA SBO balance sheet shows biofuel dominance
INSERT INTO core.kg_edge (source_node_id, target_node_id, edge_type, weight, properties, created_by, confidence) VALUES
((SELECT id FROM core.kg_node WHERE node_key = 'na_acreage_expansion_model'),
 (SELECT id FROM core.kg_node WHERE node_key = 'canola_oil'),
 'CAUSES', 0.80,
 '{"mechanism": "Canadian canola acreage expansion to 10M ha drives NA canola oil production from 13.8B lbs (22/23 MY) to 21.3B lbs (34/35 MY). Export-oriented: Canadian exports grow from 7.8B to 11.2B lbs. BBD feedstock use grows from 2.2B to 4.8B lbs. Canola oil biofuel share rises but food use remains dominant. Expansion constrained by wheat price competition in Canadian Prairies.", "source": "suncor_longterm_forecast_nov2022"}'::jsonb,
 'extracted', 0.80);


-- ============================================================================
-- 4. CONTEXTS: Expert rules, forecast frameworks, methodology
-- ============================================================================

-- Plant-Level Feedstock Mix Expert Rule
INSERT INTO core.kg_context (node_id, context_type, context_key, context_value, applicable_when, source) VALUES
((SELECT id FROM core.kg_node WHERE node_key = 'plant_level_feedstock_mix_model'),
 'expert_rule', 'feedstock_mix_methodology_rule',
 '{"rule": "Feedstock mix projections must be built bottom-up from individual facility procurement patterns, not assumed as static national averages. The mix varies each month and shifts substantially over the forecast period as: (1) new facilities with different feedstock strategies come online, (2) CI-based credit incentives drive substitution from SBO toward lower-CI fats/greases, (3) regional feedstock availability constraints shift procurement.", "mix_evolution": {"sbo_share_direction": "declining from 54% but remains largest single feedstock", "tallow_share_direction": "largest RD feedstock at 69.5%, constrained by production", "uco_share_direction": "growing with collection infrastructure and imports", "dco_share_direction": "constrained by ethanol production (15B gal mandate cap)"}, "yield_variation": "Individual feedstock yields range from 7.45 lbs/gal (canola, poultry) to 9.38 lbs/gal (corn oil). Applying a single average yield (7.67-7.84) introduces significant error when mix is changing. Always use feedstock-specific yields.", "source": "suncor_assumptions_report_nov2022"}'::jsonb,
 'always', 'extracted');

-- Tallow Supply Constraint Expert Rule
INSERT INTO core.kg_context (node_id, context_type, context_key, context_value, applicable_when, source) VALUES
((SELECT id FROM core.kg_node WHERE node_key = 'tallow'),
 'expert_rule', 'tallow_structural_deficit_rule',
 '{"rule": "Tallow is the binding constraint in RD feedstock supply. US production is essentially flat (3.65-3.73B lbs/yr, determined by livestock slaughter in a mature market). RD demand for tallow grows from 1.69B (2023) to 3.80B (2035) lbs -- approaching total US production. The deficit requires: (1) massive displacement of non-biofuel tallow users -- 65% decline from 2.63B to 0.91B lbs, (2) growing imports -- 0.93B to 1.16B lbs (+24%). Non-biofuel industries where tallow is a small cost share will resist displacement and bid prices higher, creating structural price support.", "quantification": {"us_production_flat_blbs": 3.7, "rd_demand_2023_blbs": 1.69, "rd_demand_2035_blbs": 3.80, "non_bbd_use_2023_blbs": 2.63, "non_bbd_use_2035_blbs": 0.91, "imports_2023_blbs": 0.93, "imports_2035_blbs": 1.16}, "price_forecast_cpb": {"ca_2023": 80.2, "ca_2025": 70.3, "ca_2030": 92.4, "ca_2035": 119.1}, "phase1_comparison": "2018 study high scenario projected 27.75B lbs RD tallow demand by 2038 (~7.5x US production). Even the mid scenario at 23B lbs was 6x production. The 2022 study is more conservative but tallow remains the primary supply bottleneck.", "source": "suncor_longterm_forecast_nov2022"}'::jsonb,
 'always', 'extracted');

-- Biodiesel Rationalization Expert Rule
INSERT INTO core.kg_context (node_id, context_type, context_key, context_value, applicable_when, source) VALUES
((SELECT id FROM core.kg_node WHERE node_key = 'biodiesel_rationalization_model'),
 'expert_rule', 'biodiesel_rationalization_rule',
 '{"rule": "RD capacity expansion drives structural rationalization of biodiesel (FAME) production. The mechanism: RD competes for same feedstocks but produces superior drop-in diesel fuel. BD producers face margin compression from both feedstock cost increases and product price pressure. Rationalization is phased: (1) temporary support from SRE elimination and supplemental 250M gal mandates in 2022-2023, (2) violent restructuring once supplemental mandates expire, (3) IRA transition in 2025 further compresses BD margins, (4) long-term survivors are large producers integrated with multinational crushers.", "projection_sbo_use_for_bd_blbs": {"2023": 5.29, "2025": 2.21, "2028": 1.60, "2030": 1.47, "2035": 1.65}, "state_mandates_caveat": "State-level mandates may preserve BD capacity in specific regions but also hasten consolidation by protecting large integrated producers.", "price_caveat": "Long-term SBO and biodiesel price projections suggest force-exit margin levels may not be reached until 2026 -- rationalization may be slower than projected.", "source": "suncor_assumptions_report_nov2022"}'::jsonb,
 'always', 'extracted');

-- LCFS Credit Floor and Shipping Economics
INSERT INTO core.kg_context (node_id, context_type, context_key, context_value, applicable_when, source) VALUES
((SELECT id FROM core.kg_node WHERE node_key = 'lcfs_credit_pricing_model'),
 'expert_rule', 'lcfs_marginal_shipping_cost_floor',
 '{"rule": "LCFS credit values have a theoretical floor at the marginal cost of shipping RD to California (~$50/tonne industry average). When credits exceed this threshold, producers ship additional fuel to CA until credit supply from the new volume drives prices back down. However, if CARB implements a lipid-based biofuel cap, this equilibrium mechanism is disrupted -- credits can stay above the shipping cost because qualifying supply is constrained. The Suncor study projects credits do NOT reach $50/tonne floor during the forecast specifically because potential lipid cap limits available supply.", "equilibrium_logic": "Credit value > marginal shipping cost -> more fuel shipped to CA -> more credits generated -> credit prices fall. But supply cap breaks this feedback loop.", "credit_trajectory": "From $55/tonne (2023 post-crash from $220 peak in 2020) rising to $170/tonne (2035) as CI targets accelerate and forklift credits phase out.", "carb_levers": ["Accelerate CI reduction target (20% to 25-30% by 2030)", "Cap lipid-based biofuels (like EU 7%)", "Phase out forklift electricity credits", "Include intrastate jet fuel (boost deficits)", "Add hydrogen starting 2025"], "source": "suncor_assumptions_report_nov2022"}'::jsonb,
 'always', 'extracted');

-- Canadian Import Dependency Expert Rule
INSERT INTO core.kg_context (node_id, context_type, context_key, context_value, applicable_when, source) VALUES
((SELECT id FROM core.kg_node WHERE node_key = 'canada_cfr'),
 'expert_rule', 'canada_structural_import_dependency',
 '{"rule": "Canadian Clean Fuel Regulations force structural feedstock import dependency. As mandates grow, domestic production of SBO, tallow, and UCO cannot meet biodiesel demand. Canada ceases SBO exports by 2020-2026 (scenario-dependent) and becomes net importer for all fats/oils. Canola oil production grows but biofuel demand absorbs the increment. Key dynamics: (1) SBO production grows from 400M to 1.3-1.7B lbs on Ontario/Quebec soybean expansion, (2) but biodiesel SBO demand grows from 136M to 1.0-1.4B lbs, (3) non-biofuel SBO use is flat at 220-307M lbs (food demand), (4) exports cease entirely under all scenarios.", "canada_biodiesel_sbo_use_mlbs": {"high_2018": 136, "high_2030": 476, "high_2038": 1383, "mid_2018": 136, "mid_2030": 408, "mid_2038": 1021}, "pcau_context": "Canadian protein-consuming animal units (PCAUs) drive meal demand. 80.3M units (1993) to 83.5M (2018). Soybean meal = ~2.2M tonnes/yr, canola meal ~550K tonnes/yr. Meal demand determines crush volumes, which determines oil supply.", "price_setting": "SBO price set in Central IL; CA market trades at premium (LCFS + limited local supply); SK market at logistical differential from IL. Canola oil price set in SK; LA market at premium (LCFS + Vancouver export competition).", "source": "suncor_canadian_balance_sheets_2018"}'::jsonb,
 'always', 'extracted');

-- SAF vs RD Competition Framework
INSERT INTO core.kg_context (node_id, context_type, context_key, context_value, applicable_when, source) VALUES
((SELECT id FROM core.kg_node WHERE node_key = 'sustainable_aviation_fuel'),
 'expert_rule', 'saf_rd_competition_framework',
 '{"rule": "SAF competes with RD for the same lipid feedstock pool. Growth depends entirely on relative economics at shared facilities. Key framework: (1) SAF prices start at ~2x jet fuel, narrowing to 1.25x as production and adoption increase, (2) credit values may offset premium but allocation between buyer/producer is variable, (3) CARB requiring intrastate SAF routes would jump-start production and set national benchmark, (4) many RD/ethanol facilities can add SAF with capital investment, (5) US becomes net jet fuel exporter as SAF displaces conventional.", "production_cagr": 1233, "production_2035_mgal": 4000, "blend_rate_2035_pct": 11.9, "feedstock_demand_2035_blbs": 32, "comparison_to_biden_2030_goal": "Jacobsen 1.5B gal vs Biden 3B gal target", "ev_displacement_risk": "If EV adoption reduces diesel demand (logistics sector), the freed mandate capacity opens market for more SAF -- not a net negative for biofuels.", "source": "suncor_assumptions_report_nov2022"}'::jsonb,
 'always', 'extracted');

-- US SBO Balance Sheet Structural Shift
INSERT INTO core.kg_context (node_id, context_type, context_key, context_value, applicable_when, source) VALUES
((SELECT id FROM core.kg_node WHERE node_key = 'soybean_oil'),
 'expert_rule', 'sbo_structural_demand_shift_biofuel',
 '{"rule": "US SBO demand is undergoing a structural shift from food/industrial to biofuel-dominated consumption. In the 2022 Suncor forecast, biofuel use grows from 12.4B lbs (46% of domestic use, 2023) to 35.5B lbs (74% of domestic use, 2035). Non-biofuel use declines from 14.5B to 12.3B lbs. This structural shift fundamentally changes SBO price dynamics: biofuel economics (RD revenue stack = fuel price + RINs + LCFS + IRA) set a price floor well above food-market equilibrium.", "quantification": {"bbd_use_2023_blbs": 12.4, "bbd_use_2025_blbs": 17.4, "bbd_use_2028_blbs": 18.8, "bbd_use_2030_blbs": 23.1, "bbd_use_2035_blbs": 35.5, "non_bbd_use_2023_blbs": 14.5, "non_bbd_use_2035_blbs": 12.3, "biofuel_share_2023_pct": 46, "biofuel_share_2035_pct": 74}, "price_implication": {"ca_crude_2023_cpb": 82.9, "ca_crude_2030_cpb": 91.7, "ca_crude_2035_cpb": 114.4, "note": "Prices rise 38% despite production nearly doubling -- biofuel demand absorbs all new supply and more"}, "na_perspective": "North American SBO: biofuel use grows from 11.8B lbs (22/23 MY) to 36.9B lbs (34/35 MY). Non-biofuel use shrinks from 17.6B to 16.0B lbs.", "source": "suncor_longterm_forecast_nov2022"}'::jsonb,
 'always', 'extracted');

-- RD Supply and Demand Balance
INSERT INTO core.kg_context (node_id, context_type, context_key, context_value, applicable_when, source) VALUES
((SELECT id FROM core.kg_node WHERE node_key = 'renewable_diesel'),
 'expert_rule', 'rd_supply_demand_balance_2022_forecast',
 '{"rule": "US RD production is projected to nearly triple from 1.65B gal (2023) to 4.62B gal (2035). Imports hold steady at ~1.5B gal. Domestic consumption grows from 2.26B to 5.87B gal, absorbing production expansion plus imports. Exports flat at 250M gal. Key revenue stack: CA median price $3.97/gal (2023) rising to $4.52/gal (2035) + D4 RINs $1.30-1.69/gal + LCFS $55-170/tonne + IRA ~$0.44-0.49/gal. The revenue stack evolution: BTC drops from $1.00 to IRA ~$0.44-0.49, but LCFS recovery and RIN increases partially compensate.", "rd_production_mgal": {"2023": 1649, "2025": 3350, "2028": 3645, "2030": 4046, "2035": 4621}, "imports_mgal": {"2023": 865, "2025": 1225, "2026_on": 1500}, "revenue_stack_evolution": {"2023_total_cpg": "397 + 168 + LCFS(55/tonne) + 100 BTC", "2035_total_cpg": "452 + 169 + LCFS(170/tonne) + 49 IRA", "note": "LCFS recovery from $55 to $170/tonne is largest single revenue stack improvement"}, "source": "suncor_longterm_forecast_nov2022"}'::jsonb,
 'always', 'extracted');

-- Credit Value U-Curve
INSERT INTO core.kg_context (node_id, context_type, context_key, context_value, applicable_when, source) VALUES
((SELECT id FROM core.kg_node WHERE node_key = 'us_bbd_credit_value_forecast'),
 'expert_rule', 'credit_value_dynamics_framework',
 '{"rule": "BBD credit values follow distinct trajectories: (1) D4 RINs are U-shaped -- dropping from $1.68 to $1.29 through 2028 as BTC transition and capacity expansion pressure margins, then recovering to $1.69 by 2035 as mandates tighten; (2) LCFS rises from crash levels ($55/tonne in 2023) to $170/tonne by 2035 as CARB accelerates CI targets and potential lipid cap constrains supply; (3) BTC/IRA drops sharply from $1.00 flat BTC to CI-based IRA at ~$0.44-0.49, representing the single largest revenue stack hit.", "d4_rin_cpg": {"2023": 167.5, "2025": 130, "2028": 128.75, "2030": 150, "2035": 168.75}, "lcfs_per_tonne": {"2023": 55, "2025": 100, "2028": 147.5, "2030": 125, "2035": 170}, "btc_ira_cpg": {"2023": 100, "2025": 44, "2030": 47.5, "2035": 48.7}, "net_effect": "LCFS appreciation partially offsets BTC-to-IRA decline. Net revenue stack under pressure 2024-2028, recovering 2029+.", "source": "suncor_longterm_forecast_nov2022"}'::jsonb,
 'always', 'extracted');

-- Phase 1 vs Phase 2 Comparison
INSERT INTO core.kg_context (node_id, context_type, context_key, context_value, applicable_when, source) VALUES
((SELECT id FROM core.kg_node WHERE node_key = 'feedstock_requirement_scenario_model'),
 'expert_rule', 'suncor_phase1_vs_phase2_evolution',
 '{"rule": "Comparing the 2018 (Phase 1) and 2022 (Phase 2) studies reveals how rapidly the biofuel landscape evolved. Key differences: (1) Phase 1 had no SAF -- Phase 2 adds 4B gal SAF by 2035 as major new feedstock competitor; (2) Phase 1 BD/RD split was 80/20 (2018) trending to 60/40 -- Phase 2 shows RD completely dominating with BD declining to ~155M gal; (3) Phase 1 tallow demand in high scenario reached 27.75B lbs by 2038 (unrealistic) -- Phase 2 is more conservative but tallow still the binding constraint; (4) Phase 1 had no IRA -- Phase 2 incorporates BTC-to-IRA transition as major structural shift.", "phase1_total_feedstock_mlbs": {"high_2025": 38400, "high_2035": 80900, "high_2038": 94400}, "phase2_total_feedstock_implied": {"sbo_bbd_2025": 17375, "sbo_bbd_2035": 35463, "note": "SBO alone in Phase 2 approaches Phase 1 mid-scenario total"}, "analytical_lesson": "Long-term biofuel forecasts need frequent revision as policy (IRA, LCFS), technology (SAF-capable RD plants), and market structure (BD rationalization) evolve rapidly.", "source": "suncor_feedstock_requirement_breakout"}'::jsonb,
 'always', 'extracted');

-- Feedstock Yield Variation Rule
INSERT INTO core.kg_context (node_id, context_type, context_key, context_value, applicable_when, source) VALUES
((SELECT id FROM core.kg_node WHERE node_key = 'feedstock_supply_chain_model'),
 'expert_rule', 'feedstock_yield_variation_rule',
 '{"rule": "Feedstock-to-fuel yields vary significantly by feedstock type. Using a single average yield introduces material error, especially when the feedstock mix is shifting over time. Always use feedstock-specific yields for volume calculations.", "yields_lbs_per_gal": {"soybean_oil_biodiesel": 7.50, "soybean_oil_rd": 7.50, "canola_oil": 7.45, "poultry_fat": 7.45, "tallow_biodiesel": 7.75, "tallow_rd": 7.65, "choice_white_grease": 7.858, "lard": 7.80, "yellow_grease_biodiesel": 8.23, "distillers_corn_oil_biodiesel": 8.20, "distillers_corn_oil_rd": 9.38, "used_cooking_oil_rd": 8.01, "fish_oil_rd": 9.38, "other_grease": 8.40}, "weighted_averages": {"biodiesel_avg": 7.67, "rd_avg": 7.84}, "practical_impact": "When SBO (7.5 lbs/gal) is replaced by corn oil (9.38 lbs/gal), ~25% more feedstock is needed per gallon. A 10% mix shift from SBO to corn oil increases total feedstock requirement by ~2.5%.", "bold_yields_note": "Bold yields in Suncor workbooks are Jacobsen proprietary estimates (not published industry standards).", "source": "suncor_feedstock_requirement_breakout"}'::jsonb,
 'always', 'extracted');

-- Acreage Expansion Constraint Rule
INSERT INTO core.kg_context (node_id, context_type, context_key, context_value, applicable_when, source) VALUES
((SELECT id FROM core.kg_node WHERE node_key = 'na_acreage_expansion_model'),
 'expert_rule', 'acreage_expansion_constraints',
 '{"rule": "Reaching 100M acres of US soybeans requires unprecedented continuous planting area. Two binding constraints: (1) Crop rotation -- farmers make 50%+ of decisions on rotation concerns, and 100M acres of continuous soy on limited arable land is physically challenging; (2) Competing crop economics -- historically high wheat prices from Ukraine war reduce willingness to switch from wheat to soybeans in Plains. The expansion pattern replicates historical wheat-to-soy substitution but depends on wheat price normalization post-conflict.", "us_soybean_acreage_path_m_acres": {"current": 87, "target": 100, "expansion_needed": 13, "source_acres": "Primarily wheat in Plains states (KS, OK, NE)"}, "canada_canola_path_m_ha": {"current": 8.5, "target": 10.0, "expansion_needed": 1.5, "source_acres": "Wheat across Canadian Prairies"}, "implicit_assumption": "Ukraine war resolves during forecast period, allowing wheat prices to resume long-term decline relative to oilseeds.", "source": "suncor_assumptions_report_nov2022"}'::jsonb,
 'always', 'extracted');


-- ============================================================================
-- END OF BATCH 013
-- ============================================================================
