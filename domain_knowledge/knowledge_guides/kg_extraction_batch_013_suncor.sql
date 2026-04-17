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
--   * Critical insight: tallow imports must surge massively (5-8B lbs) to meet RD demand
--   * Biodiesel rationalization predicted from 2024+ as RD displaces BD capacity
--   * Canadian canola acreage to 10M ha by 2040 (wheat-to-canola substitution)
--   * US soybean acreage to 100M acres by 2040 (crop rotation constraint identified)
--   * SAF CAGR 1233% — most aggressive growth rate in the study
--   * LCFS credit floor = marginal shipping cost ~$50/tonne
--   * Canadian RFS demand: 178-889M gallons BBD by forecast end
-- ============================================================================


-- ============================================================================
-- 1. SOURCE REGISTRATION
-- ============================================================================

INSERT INTO core.kg_source (source_key, source_type, title, author, date_produced, file_path, status, extraction_batch, notes) VALUES
('suncor_lt_forecast_assumptions_nov22', 'consulting_report', 'Suncor Long-Term Forecast Assumptions Report - First Draft', 'Fastmarkets (Jacobsen)', '2022-11-21',
 'Dropbox/Jacobsen/Projects/Suncor/Suncor Long-Term Forecast Assumptions Report - First Draft.docx', 'completed', 'batch_013',
 'Cover letter + qualitative assumptions for Nov 2022 study update. LCFS changes, feedstock mix, SAF, RFS mandates, acreage, biodiesel rationalization, Canada pricing.')
ON CONFLICT (source_key) DO UPDATE SET status = 'completed';

INSERT INTO core.kg_source (source_key, source_type, title, author, date_produced, file_path, status, extraction_batch, notes) VALUES
('suncor_lt_forecast_nov22_xlsx', 'consulting_workbook', 'Suncor Long-Term Forecast - Nov 22', 'Fastmarkets (Jacobsen)', '2022-11-21',
 'Dropbox/Jacobsen/Projects/Suncor/Suncor Long-Term Forecast - Nov 22.xlsx', 'completed', 'batch_013',
 '8 sheets: SBO, CO, UCO, DCO, Tallow, Credit Values, HRD, SAF. US balance sheets 2023-2035+.')
ON CONFLICT (source_key) DO UPDATE SET status = 'completed';

INSERT INTO core.kg_source (source_key, source_type, title, author, date_produced, file_path, status, extraction_batch, notes) VALUES
('suncor_us_bs_2018_2040', 'consulting_workbook', 'Long-Term US Oilseed and Fats Balance Sheets 2018-2040', 'Jacobsen', '2018-05-04',
 'Dropbox/Jacobsen/Projects/Suncor/Long-Term US Oilseed and Fats Balance Sheets - 2018-2040 - 05042018.xlsx', 'completed', 'batch_013',
 '28 sheets. 7 commodity BS with High/Mid/Low scenarios + price forecasts. SBO, canola, tallow, CWG, YG, poultry fat, DCO.')
ON CONFLICT (source_key) DO UPDATE SET status = 'completed';

INSERT INTO core.kg_source (source_key, source_type, title, author, date_produced, file_path, status, extraction_batch, notes) VALUES
('suncor_us_bs_2018_2040_slides', 'consulting_workbook', 'Long-Term US Oilseed and Fats Balance Sheets 2018-2040 (Slides version)', 'Jacobsen', '2018-05-04',
 'Dropbox/Jacobsen/Projects/Suncor/Copy of Long-Term US Oilseed and Fats Balance Sheets - 2018-2040 - Slides.xlsx', 'completed', 'batch_013',
 '7 commodity BS presentation version with 2024-2037 forecast horizon. Updated projections vs May 2018 original.')
ON CONFLICT (source_key) DO UPDATE SET status = 'completed';

INSERT INTO core.kg_source (source_key, source_type, title, author, date_produced, file_path, status, extraction_batch, notes) VALUES
('suncor_us_projections_2018_2040', 'consulting_workbook', 'US Oilseed and Fats Projections 2018-2040', 'Jacobsen', '2018-05-03',
 'Dropbox/Jacobsen/Projects/Suncor/US Oilseed and Fats Projections - 2018-2040 - 05032018.xlsx', 'completed', 'batch_013',
 'PCAU-driven meal demand model, total meal/oil/fats annual. Historical back to 93/94.')
ON CONFLICT (source_key) DO UPDATE SET status = 'completed';

INSERT INTO core.kg_source (source_key, source_type, title, author, date_produced, file_path, status, extraction_batch, notes) VALUES
('suncor_balance_sheets_apr18', 'consulting_workbook', 'Suncor Balance Sheets (April 2018)', 'Jacobsen', '2018-04-18',
 'Dropbox/Jacobsen/Projects/Suncor/Suncor Balance Sheets.xlsx', 'completed', 'batch_013',
 '10 sheets: Tallow, DCO, YG, WG, Poultry Fat, SBO, Canola Oil, Palm Oil, Coconut Oil, PKO. Historical 2013-2022 + forecasts.')
ON CONFLICT (source_key) DO UPDATE SET status = 'completed';

INSERT INTO core.kg_source (source_key, source_type, title, author, date_produced, file_path, status, extraction_batch, notes) VALUES
('suncor_canadian_bs', 'consulting_workbook', 'Suncor Canadian Balance Sheets', 'Jacobsen', '2018-04-18',
 'Dropbox/Jacobsen/Projects/Suncor/Suncor Canadian Balance Sheets.xlsx', 'completed', 'batch_013',
 '17 sheets: PCAU + 4 commodities (SBO, canola oil, tallow, UCO) x High/Mid/Low + Cal Yr BS. Canada soybean complex with crush/export/meal.')
ON CONFLICT (source_key) DO UPDATE SET status = 'completed';

INSERT INTO core.kg_source (source_key, source_type, title, author, date_produced, file_path, status, extraction_batch, notes) VALUES
('suncor_feedstock_requirement', 'consulting_workbook', 'Feedstock Requirement Breakout', 'Jacobsen', '2018-04-18',
 'Dropbox/Jacobsen/Projects/Suncor/Feedstock Requirement Breakout.xlsx', 'completed', 'batch_013',
 'BD + RD feedstock requirement scenarios with 13 feedstock types, individual yields (7.45-9.38 gal/lb), breakout by forecast period.')
ON CONFLICT (source_key) DO UPDATE SET status = 'completed';

INSERT INTO core.kg_source (source_key, source_type, title, author, date_produced, file_path, status, extraction_batch, notes) VALUES
('suncor_bio_rd_methodology', 'consulting_workbook', 'Suncor Bio and RD Forecast Methodology', 'Jacobsen', '2018-04-12',
 'Dropbox/Jacobsen/Projects/Suncor/Suncor Bio and RD forecast metho 041218.xlsx', 'completed', 'batch_013',
 'BBD demand methodology: RFS D4/D5, LCFS, CAN RFS, provincial schemes. 3-5, 5-10, 10-20 year horizons. CARB projections.')
ON CONFLICT (source_key) DO UPDATE SET status = 'completed';

INSERT INTO core.kg_source (source_key, source_type, title, author, date_produced, file_path, status, extraction_batch, notes) VALUES
('suncor_lcfs_feedstock_mix', 'consulting_workbook', 'LCFS Feedstock Mix', 'Jacobsen', '2018-04-18',
 'Dropbox/Jacobsen/Projects/Suncor/LCFS Feedstock Mix.xlsx', 'completed', 'batch_013',
 'Historical LCFS credit generation by fuel type 2011-2017 Q3. Biodiesel, RD, ethanol, CNG, electricity credits.')
ON CONFLICT (source_key) DO UPDATE SET status = 'completed';

INSERT INTO core.kg_source (source_key, source_type, title, author, date_produced, file_path, status, extraction_batch, notes) VALUES
('suncor_historical_can_feedstock', 'consulting_workbook', 'Historical Canadian Feedstock Data', 'Jacobsen / Environment Canada', '2018-04-18',
 'Dropbox/Jacobsen/Projects/Suncor/Historical Canadian Feedstock Data.xlsx', 'completed', 'batch_013',
 'Canadian RFS compliance data 2013-2014. Fuel production/import volumes, ethanol/BBD by feedstock, provincial breakdown.')
ON CONFLICT (source_key) DO UPDATE SET status = 'completed';

INSERT INTO core.kg_source (source_key, source_type, title, author, date_produced, file_path, status, extraction_batch, notes) VALUES
('suncor_price_definitions', 'consulting_workbook', 'Historical Price Definitions', 'Jacobsen', '2018-07-13',
 'Dropbox/Jacobsen/Projects/Suncor/Historical Price Definitions - 07132018.xlsx', 'completed', 'batch_013',
 'Price basis definitions: SBO=Crude CIL (GX_GR117), Tallow=BFT Chicago, DCO=Crude Corn Oil CIL, YG=Illinois.')
ON CONFLICT (source_key) DO UPDATE SET status = 'completed';

INSERT INTO core.kg_source (source_key, source_type, title, author, date_produced, file_path, status, extraction_batch, notes) VALUES
('suncor_world_crushing_plants', 'consulting_workbook', 'World Crushing Plants', 'Jacobsen', '2018-04-18',
 'Dropbox/Jacobsen/Projects/Suncor/World Crushing Plants.xlsx', 'completed', 'batch_013',
 'Global oilseed crushing plant database.')
ON CONFLICT (source_key) DO UPDATE SET status = 'completed';

INSERT INTO core.kg_source (source_key, source_type, title, author, date_produced, file_path, status, extraction_batch, notes) VALUES
('suncor_plan', 'consulting_workbook', 'Suncor Plan (work assignment)', 'Jacobsen', '2018-04-18',
 'Dropbox/Jacobsen/Projects/Suncor/Suncor Plan.xlsx', 'completed', 'batch_013',
 'Workload assignment: Tore (BS + drivers for SBO, canola, palm, rape, tropical), Ryan (tallow, DCO, YG, CWG, PF pricing/hedging), John C (global waste).')
ON CONFLICT (source_key) DO UPDATE SET status = 'completed';

INSERT INTO core.kg_source (source_key, source_type, title, author, date_produced, file_path, status, extraction_batch, notes) VALUES
('suncor_can_fuel_production', 'consulting_workbook', 'Canadian Fuel Production Info June 2018', 'Jacobsen', '2018-06-01',
 'Dropbox/Jacobsen/Projects/Suncor/Copy of Canadian Fuel Production Info June 2018.xlsx', 'completed', 'batch_013',
 'Canadian fuel production data for balance sheet construction.')
ON CONFLICT (source_key) DO UPDATE SET status = 'completed';

INSERT INTO core.kg_source (source_key, source_type, title, author, date_produced, file_path, status, extraction_batch, notes) VALUES
('suncor_broiler_production', 'consulting_workbook', 'World Broiler Production - Mark Jordan', 'Mark Jordan (Jacobsen)', '2018-04-06',
 'Dropbox/Jacobsen/Projects/Suncor/World Broiler Production - Mark Jordan - 04062018.xlsx', 'completed', 'batch_013',
 'Global broiler production data supporting poultry fat supply projections.')
ON CONFLICT (source_key) DO UPDATE SET status = 'completed';

INSERT INTO core.kg_source (source_key, source_type, title, author, date_produced, file_path, status, extraction_batch, notes) VALUES
('suncor_biodiesel_class_breakout', 'consulting_workbook', 'Biodiesel Class Breakout', 'Bob (Jacobsen)', '2018-04-18',
 'Dropbox/Jacobsen/Projects/Suncor/Biodiesel Class Breakout - Bob - 04182018.xlsx', 'completed', 'batch_013',
 'Biodiesel production by feedstock class.')
ON CONFLICT (source_key) DO UPDATE SET status = 'completed';


-- ============================================================================
-- 2. NODES: New consulting/analytical model nodes
-- ============================================================================

-- Long-term feedstock study model (the overall consulting product)
INSERT INTO core.kg_node (node_type, node_key, label, properties) VALUES
('analytical_model', 'suncor_feedstock_study', 'Suncor North American Feedstock Study',
 '{"description": "Consulting engagement for Suncor Energy assessing long-term (2018-2040) North American BBD feedstock S&D, prices, and margins. Two phases: Phase 1 (Apr-Jul 2018) US/Canada balance sheets, Phase 2 (Nov 2022) updated forecast adding SAF. Produced 7 US commodity BS (SBO, canola oil, tallow, CWG, YG, poultry fat, DCO) + 4 Canadian BS (SBO, canola oil, tallow, UCO), each with High/Mid/Low scenarios.",
  "client": "Suncor Energy Services Inc",
  "producer": "Fastmarkets (The Jacobsen)",
  "engagement_dates": ["2018-04", "2022-11"],
  "commodities_covered": ["soybean_oil", "canola_oil", "inedible_tallow", "choice_white_grease", "yellow_grease", "poultry_fat", "distillers_corn_oil", "used_cooking_oil", "palm_oil", "coconut_oil", "palm_kernel_oil"],
  "geographies": ["US", "Canada"],
  "forecast_horizon": "2018-2040",
  "scenario_structure": "High/Mid/Low for each commodity and geography",
  "methodology": "Plant-level feedstock demand aggregation, PCAU-driven meal demand, individual facility feedstock mix projection",
  "key_outputs": ["balance_sheets", "price_forecasts", "feedstock_mix", "credit_value_forecasts", "margin_analysis", "acreage_projections"]}'::jsonb)
ON CONFLICT (node_key) DO UPDATE SET properties = EXCLUDED.properties;

-- Feedstock mix model
INSERT INTO core.kg_node (node_type, node_key, label, properties) VALUES
('analytical_model', 'feedstock_mix_methodology', 'Plant-Level Feedstock Mix Methodology',
 '{"description": "Bottom-up methodology for projecting industry-wide BBD feedstock demand. Projects feedstock mix at individual RD/biodiesel facilities, multiplies by monthly production, sums by feedstock type, divides by total to derive industry-wide mix.",
  "source": "Suncor Long-Term Forecast Assumptions Report (Nov 2022)",
  "average_mix_2022_study": {"soybean_oil_pct": 54, "distillers_corn_oil_pct": 14, "used_cooking_oil_pct": 13, "bleachable_fancy_tallow_pct": 9, "yellow_grease_pct": 5, "choice_white_grease_pct": 3, "canola_oil_pct": 2, "poultry_fat_pct": 1},
  "note": "Mix varies monthly and shifts substantially from beginning to end of forecast period. SBO share declines as capacity shifts toward lower-CI feedstocks.",
  "feedstock_yields_gal_per_lb": {"canola_oil": 7.45, "corn_oil": 8.2, "soybean_oil": 7.5, "poultry_fat": 7.45, "tallow": 7.75, "white_grease": 7.858, "yellow_grease": 8.23, "other_grease": 8.4},
  "rd_yield_gal_per_lb": {"corn_oil": 9.38, "fish_oil": 9.38, "tallow": 7.65, "uco": 8.01}}'::jsonb)
ON CONFLICT (node_key) DO UPDATE SET properties = EXCLUDED.properties;

-- BBD demand forecast model
INSERT INTO core.kg_node (node_type, node_key, label, properties) VALUES
('analytical_model', 'bbd_demand_forecast_methodology', 'BBD Demand Forecast Methodology (Suncor)',
 '{"description": "Multi-layer mandate-driven BBD demand model combining US RFS D4/D5, LCFS, Canadian RFS, and provincial schemes to derive total North American BBD demand in billion gallons.",
  "demand_layers": ["RFS2_D4_gallons", "RFS2_D5_gallons", "LCFS_incremental", "CAN_RFS", "provincial_schemes"],
  "d4_assumption_2022": "EPA raises total renewable fuel mandate ~2%/yr, implied ethanol stays at 15B gal through 2035, BBD mandate grows to 6.25B gal",
  "d5_methodology": "See D5 worksheet — cellulosic/advanced RIN surplus converted to gallon equivalents",
  "lcfs_assumption": "CARB modifications to raise credit prices; marginal shipping cost ~$50/tonne sets floor",
  "can_rfs_range_bn_gal": {"low": 0.178, "high": 0.889},
  "high_scenario_2037_total_bn_gal": 5.5,
  "source": "Suncor Bio and RD forecast metho 041218.xlsx + Assumptions Report Nov 2022"}'::jsonb)
ON CONFLICT (node_key) DO UPDATE SET properties = EXCLUDED.properties;

-- SAF model
INSERT INTO core.kg_node (node_type, node_key, label, properties) VALUES
('analytical_model', 'saf_growth_model_suncor', 'SAF Growth Model (Suncor Study)',
 '{"description": "SAF supply/demand projections from Nov 2022 Suncor study. Predicts rapid expansion as RD/ethanol facilities add SAF capability. US becomes net exporter of jet fuel in second half of forecast period.",
  "2021_baseline_gal_per_month": 500000,
  "2022_projected_gal_per_month": 600000,
  "cagr_forecast_period_pct": 1233,
  "biden_goal_2030_bn_gal": 3.0,
  "jacobsen_vs_biden": "More conservative than 3B gal by 2030 target",
  "pricing_framework": "SAF priced as premium to jet fuel. Premium starts ~2x jet fuel, narrows to ~1.25x by forecast end. California likely price-setting market.",
  "key_driver": "Economics of SAF vs RD vs ethanol at existing facilities",
  "carb_intrastate_saf": "If CARB requires intrastate SAF, provides stable market to jump-start production",
  "trade_balance_impact": "US becomes net jet fuel exporter as domestic SAF supply grows",
  "source": "Suncor Long-Term Forecast Assumptions Report Nov 2022"}'::jsonb)
ON CONFLICT (node_key) DO UPDATE SET properties = EXCLUDED.properties;

-- Biodiesel rationalization model
INSERT INTO core.kg_node (node_type, node_key, label, properties) VALUES
('analytical_model', 'biodiesel_rationalization_model', 'US Biodiesel Capacity Rationalization Model',
 '{"description": "Thesis that RD capacity growth drives violent restructuring of biodiesel capacity starting 2024. Large integrated crushers survive; small independents exit.",
  "trigger_timeline": "Significant decline in BD production starting 2024 as supplemental mandates expire",
  "margin_impact": "IRA transition from blenders tax credit to production credit (2025) substantially impacts BD margins",
  "survivors": "Facilities integrated with multinational crushing companies",
  "SRE_factor": "Elimination of small refinery exemptions temporarily supports BD margins",
  "supplemental_mandate": "250M gal supplemental in 2022-2023 delays rationalization",
  "state_policy_effect": "State-level policies could limit decline but hasten consolidation to large producers",
  "price_signal_timing": "Soy oil and BD price projections suggest margins may not force rationalization until 2026",
  "source": "Suncor Long-Term Forecast Assumptions Report Nov 2022"}'::jsonb)
ON CONFLICT (node_key) DO UPDATE SET properties = EXCLUDED.properties;

-- Acreage expansion model
INSERT INTO core.kg_node (node_type, node_key, label, properties) VALUES
('analytical_model', 'na_oilseed_acreage_expansion_model', 'North American Oilseed Acreage Expansion Model',
 '{"description": "Long-term acreage projections driven by BBD/SAF feedstock demand growth. US soybeans to 100M acres, Canada canola to 10M hectares by forecast end.",
  "us_soybean_target_acres_m": 100,
  "canada_canola_target_ha_m": 10,
  "us_constraint": "Crop rotation — farmers make 50%+ of acreage decisions on rotation. 100M continuous soy acres challenging with limited unused arable land.",
  "us_historical_analog": "Last major soy expansion came from Plains wheat-to-soy substitution",
  "us_risk": "High wheat prices (Ukraine war) limit wheat-to-soy switching in near term",
  "canada_driver": "Oilseeds steal acreage from wheat across Prairies as wheat prices resume long-term downtrend",
  "canada_risk": "Ukraine-driven wheat prices delay canola expansion",
  "assumption": "Ukraine conflict does not persist through full forecast period",
  "source": "Suncor Long-Term Forecast Assumptions Report Nov 2022"}'::jsonb)
ON CONFLICT (node_key) DO UPDATE SET properties = EXCLUDED.properties;

-- Canadian pricing framework
INSERT INTO core.kg_node (node_type, node_key, label, properties) VALUES
('analytical_model', 'canada_feedstock_pricing_model', 'Canadian Feedstock Pricing Framework',
 '{"description": "Pricing basis for Canadian oilseed/fats markets relative to US reference points. SBO set in CIL, canola oil set in Saskatchewan.",
  "sbo_reference": "Central Illinois (CIL) — prices in Saskatchewan = CIL + logistical differential",
  "canola_oil_reference": "Saskatchewan sets domestic price. LA market based on reported price series. Differential to SK varies with LCFS credit value and Vancouver export competition.",
  "differential_dynamics": "Logistical differential changes throughout forecast based on macro fuel and freight cost predictions",
  "california_premium": "Limited feedstock availability and LCFS credits create premium to SK price",
  "vancouver_competition": "Export market in Vancouver competes with domestic use for Canadian canola oil",
  "pricing_caveat": "Implied differential between SK and LA may be too large due to separate models used for each market",
  "price_definitions": {"soybean_oil": "Crude SBO - Central Illinois (USDA GX_GR117)", "tallow": "Bleachable Fancy Tallow - Renderer, Chicago", "dco": "Crude Corn Oil - Central Illinois", "yellow_grease": "Illinois"},
  "source": "Suncor Assumptions Report Nov 2022 + Historical Price Definitions Jul 2018"}'::jsonb)
ON CONFLICT (node_key) DO UPDATE SET properties = EXCLUDED.properties;

-- LCFS credit pricing model
INSERT INTO core.kg_node (node_type, node_key, label, properties) VALUES
('analytical_model', 'lcfs_credit_pricing_model_suncor', 'LCFS Credit Pricing Model (Suncor Study)',
 '{"description": "Model predicting LCFS credit values using credit bank, BBD shipments to California, and other variables. Three-step adjustment: (1) model-derived prices, (2) upward adjustment for assumed CARB program modifications, (3) downward adjustment for marginal shipping cost floor.",
  "peak_credit_value": "$220/tonne (Feb 2020)",
  "nov_2022_value": "$60/tonne",
  "marginal_shipping_cost_floor": "$50/tonne — industry average. Credits above this level incentivize RD shipments to CA.",
  "carb_modifications_considered": ["electric forklift credit phase-out", "hydrogen inclusion (2025+)", "intrastate fossil jet fuel inclusion", "crop-based biofuel cap", "land use change reevaluation"],
  "forklift_context": "Electric forklifts lower total cost of ownership even without LCFS credits. 50%+ fleet already electric. Generated 27% of electricity credits in 2021.",
  "lipid_cap_risk": "CARB evaluating limits on lipid-based (vegetable oil) derived BBD to prevent food vs fuel conflict",
  "forecast_behavior": "Projected credit values do not reach marginal shipping cost floor during forecast period due to lipid caps limiting qualifying supply",
  "source": "Suncor Long-Term Forecast Assumptions Report Nov 2022"}'::jsonb)
ON CONFLICT (node_key) DO UPDATE SET properties = EXCLUDED.properties;

-- Suncor Energy (the client)
INSERT INTO core.kg_node (node_type, node_key, label, properties) VALUES
('company', 'suncor_energy', 'Suncor Energy',
 '{"description": "Major Canadian integrated energy company headquartered in Calgary, Alberta. Engaged Fastmarkets/Jacobsen for renewable diesel feedstock study (2018, 2022). Considering investment in North American renewable diesel industry.",
  "headquarters": "Calgary, Alberta, Canada",
  "sector": "integrated_energy",
  "rd_interest": "Evaluating RD investment — commissioned profitability outlook and competitive landscape assessment",
  "study_scope": "BBD feedstock S&D, prices, RD/SAF S&D and prices, credit value projections, gross margin analysis",
  "contact_email_2022": "katmarshall@suncor.com"}'::jsonb)
ON CONFLICT (node_key) DO UPDATE SET properties = EXCLUDED.properties;

-- Canadian soybean complex
INSERT INTO core.kg_node (node_type, node_key, label, properties) VALUES
('commodity', 'canada_soybeans', 'Canadian Soybeans',
 '{"description": "Canadian soybean complex — rapidly expanding acreage from 1.87M ha (2013) projected to 4.7M+ ha by 2027 (High scenario). Production from 5.4 MMT (2013) to projected 14.5 MMT.",
  "crush_trajectory_000t": {"2013": 1525, "2017": 2200, "2024_high": 2750, "2027_high": 3050},
  "export_growth_000t": {"2013": 3448, "2017": 5450, "2027_high": 10400},
  "yield_range_t_ha": {"low": 2.71, "high": 3.09},
  "ending_stocks_000t": 350,
  "source": "Suncor Canadian Balance Sheets.xlsx"}'::jsonb)
ON CONFLICT (node_key) DO UPDATE SET properties = EXCLUDED.properties;

-- Canadian canola oil balance sheet node
INSERT INTO core.kg_node (node_type, node_key, label, properties) VALUES
('data_series', 'canada_canola_oil_balance_sheet', 'Canadian Canola Oil Balance Sheet',
 '{"description": "Canada canola oil S&D projected under High/Mid/Low scenarios. Production ~4.1B lbs/yr, majority exported (2.4-3.2B lbs). Domestic BBD use grows from near zero to 340-658M lbs (High) by 2024.",
  "production_mlb_2024_high": 4149,
  "exports_mlb_2024_high": 3036,
  "domestic_bbd_mlb_2024_high": 658,
  "non_bbd_domestic_mlb_2024": 600,
  "bbd_growth_driver": "Canadian RFS and provincial clean fuel mandates",
  "key_tension": "BBD demand absorbs exports — domestic BBD use rises while exports decline",
  "source": "Suncor Canadian Balance Sheets.xlsx"}'::jsonb)
ON CONFLICT (node_key) DO UPDATE SET properties = EXCLUDED.properties;

-- Canadian tallow balance sheet node
INSERT INTO core.kg_node (node_type, node_key, label, properties) VALUES
('data_series', 'canada_tallow_balance_sheet', 'Canadian Tallow Balance Sheet',
 '{"description": "Canada tallow S&D projected under scenarios. Production ~260-280M lbs/yr, growing with cattle herd. Critical: imports must surge from zero to 55-190M lbs to meet growing BBD demand.",
  "production_mlb_2024": 272,
  "imports_mlb_range_2024": {"low": 30, "mid": 55, "high": 155},
  "domestic_bbd_mlb_2024_high": 234,
  "domestic_bbd_mlb_2024_mid": 136,
  "non_bbd_domestic_mlb_2024": 191,
  "key_insight": "Canada historically near self-sufficient in tallow but BBD mandate growth forces import dependency",
  "source": "Suncor Canadian Balance Sheets.xlsx"}'::jsonb)
ON CONFLICT (node_key) DO UPDATE SET properties = EXCLUDED.properties;

-- Canadian UCO balance sheet node
INSERT INTO core.kg_node (node_type, node_key, label, properties) VALUES
('data_series', 'canada_uco_balance_sheet', 'Canadian UCO Balance Sheet',
 '{"description": "Canada UCO S&D. Domestic production ~100-110M lbs/yr, growing ~1%/yr. Imports must surge from zero to 40-200M lbs under BBD mandate scenarios.",
  "production_mlb_2024": 108,
  "imports_mlb_range_2024": {"low": 40, "mid": 70, "high": 165},
  "domestic_bbd_mlb_2024_high": 234,
  "domestic_bbd_mlb_2024_mid": 136,
  "non_bbd_domestic_mlb_2024": 38,
  "key_insight": "UCO is very small domestic supply base — any meaningful BBD mandate requires massive import growth",
  "source": "Suncor Canadian Balance Sheets.xlsx"}'::jsonb)
ON CONFLICT (node_key) DO UPDATE SET properties = EXCLUDED.properties;

-- RD feedstock requirement composition
INSERT INTO core.kg_node (node_type, node_key, label, properties) VALUES
('data_series', 'rd_feedstock_composition_2018', 'Renewable Diesel Feedstock Composition (2018 Suncor Study)',
 '{"description": "Historical and projected RD feedstock requirements by type. Tallow dominates RD feedstock at ~70%, UCO ~18%, corn oil ~10%, fish oil ~3%.",
  "rd_feedstock_shares": {"tallow_pct": 69.5, "uco_pct": 17.6, "corn_oil_pct": 9.6, "fish_oil_pct": 3.2, "other_pct": 0.04},
  "rd_total_feedstock_mlb_2018_high": 4000,
  "rd_total_feedstock_mlb_2025_high": 16000,
  "rd_total_feedstock_mlb_2037_high": 32900,
  "key_insight": "RD feedstock requirements grow 8x from 2018 to 2037 in high scenario. Tallow alone requires 22B lbs by 2037 — far exceeding US production of ~5-7B lbs, necessitating massive imports.",
  "source": "Feedstock Requirement Breakout.xlsx"}'::jsonb)
ON CONFLICT (node_key) DO UPDATE SET properties = EXCLUDED.properties;

-- BD feedstock composition
INSERT INTO core.kg_node (node_type, node_key, label, properties) VALUES
('data_series', 'bd_feedstock_composition_2018', 'Biodiesel Feedstock Composition (2018 Suncor Study)',
 '{"description": "Historical and projected biodiesel feedstock requirements by type. SBO dominates at ~52%, with YG ~12%, DCO ~12%, canola ~11%.",
  "bd_feedstock_shares_2018": {"soybean_oil_pct": 52.5, "yellow_grease_pct": 12.2, "distillers_corn_oil_pct": 12.3, "canola_oil_pct": 10.8, "white_grease_pct": 5.0, "tallow_pct": 3.1, "poultry_fat_pct": 1.7, "other_grease_pct": 0.9, "other_pct": 0.3, "lard_pct": 0.6, "sunflower_oil_pct": 0.3},
  "bd_yield_gal_per_lb": 7.67,
  "source": "Feedstock Requirement Breakout.xlsx"}'::jsonb)
ON CONFLICT (node_key) DO UPDATE SET properties = EXCLUDED.properties;

-- Canadian RFS demand series
INSERT INTO core.kg_node (node_type, node_key, label, properties) VALUES
('data_series', 'canada_rfs_bbd_demand', 'Canadian RFS BBD Demand Projections',
 '{"description": "Canadian renewable fuel standard BBD demand projections from Suncor study. Federal 2% mandate expanding. Provincial schemes add incremental demand.",
  "2018_demand_bn_gal": 0.178,
  "methodology_assumption": "Market moves from 2% to 5% across 5 years",
  "high_case_2037_bn_gal": 0.889,
  "provincial_schemes": "Not quantified in base model but expected to add",
  "historical_compliance_2013_14": {"ethanol_m3_2013": 2838322, "ethanol_m3_2014": 2961024, "bbd_m3_2013": 585075, "bbd_m3_2014": 605389},
  "canadian_bbd_feedstock_2013": {"animal_material_pct": 42, "oilseeds_pct": 44, "other_veg_oils_pct": 11, "soy_oil_pct": 3},
  "source": "Suncor Bio and RD forecast metho 041218.xlsx + Historical Canadian Feedstock Data.xlsx"}'::jsonb)
ON CONFLICT (node_key) DO UPDATE SET properties = EXCLUDED.properties;


-- ============================================================================
-- 3. EDGES: Relationships
-- ============================================================================

-- Suncor study links to existing nodes
INSERT INTO core.kg_edge (source_node_key, target_node_key, edge_type, properties) VALUES
('suncor_feedstock_study', 'renewable_diesel', 'ANALYZES',
 '{"context": "Study provides long-term feedstock S&D outlook for Suncor RD investment decision. Covers feedstock supply, demand, prices, and margins.", "source": "batch_013"}'::jsonb)
ON CONFLICT DO NOTHING;

INSERT INTO core.kg_edge (source_node_key, target_node_key, edge_type, properties) VALUES
('suncor_feedstock_study', 'sustainable_aviation_fuel', 'ANALYZES',
 '{"context": "2022 update added SAF supply/demand/pricing projections. SAF CAGR 1233% — competes with RD for feedstock.", "source": "batch_013"}'::jsonb)
ON CONFLICT DO NOTHING;

INSERT INTO core.kg_edge (source_node_key, target_node_key, edge_type, properties) VALUES
('suncor_feedstock_study', 'feedstock_supply_chain_model', 'EXTENDS',
 '{"context": "Suncor study provides 20-year projections using same supply chain logic as the feedstock supply chain model but with plant-level granularity.", "source": "batch_013"}'::jsonb)
ON CONFLICT DO NOTHING;

-- Feedstock mix methodology links
INSERT INTO core.kg_edge (source_node_key, target_node_key, edge_type, properties) VALUES
('feedstock_mix_methodology', 'bbd_balance_sheet_model', 'FEEDS',
 '{"context": "Plant-level feedstock mix projections feed into industry-wide BBD balance sheet. Each facility has individual feedstock assumptions.", "source": "batch_013"}'::jsonb)
ON CONFLICT DO NOTHING;

-- Acreage model links
INSERT INTO core.kg_edge (source_node_key, target_node_key, edge_type, properties) VALUES
('na_oilseed_acreage_expansion_model', 'canola_oil', 'SUPPLIES',
 '{"context": "Canadian canola acreage expansion to 10M ha is required to meet projected canola oil demand growth. Wheat-to-canola substitution is the primary mechanism.", "source": "batch_013"}'::jsonb)
ON CONFLICT DO NOTHING;

INSERT INTO core.kg_edge (source_node_key, target_node_key, edge_type, properties) VALUES
('na_oilseed_acreage_expansion_model', 'soybeans', 'SUPPLIES',
 '{"context": "US soybean acreage to 100M acres by 2040 — challenged by crop rotation constraint and limited unused arable land.", "source": "batch_013"}'::jsonb)
ON CONFLICT DO NOTHING;

-- RD feedstock competition
INSERT INTO core.kg_edge (source_node_key, target_node_key, edge_type, properties) VALUES
('renewable_diesel', 'tallow', 'CONSUMES',
 '{"context": "Tallow is 70% of RD feedstock in 2018 study. By 2037, tallow demand for RD alone reaches 22B lbs (High scenario) — far exceeding US production of 5-7B lbs, requiring massive imports.", "quantified": true, "rd_tallow_demand_2037_mlb_high": 22050, "us_tallow_production_2037_mlb": 7288, "import_requirement_mlb": 14762, "source": "batch_013"}'::jsonb)
ON CONFLICT DO NOTHING;

INSERT INTO core.kg_edge (source_node_key, target_node_key, edge_type, properties) VALUES
('renewable_diesel', 'used_cooking_oil', 'CONSUMES',
 '{"context": "UCO is 18% of RD feedstock. By 2037, RD UCO demand reaches 5.8B lbs (High). Import-dependent.", "source": "batch_013"}'::jsonb)
ON CONFLICT DO NOTHING;

-- SAF competes with RD for feedstock
INSERT INTO core.kg_edge (source_node_key, target_node_key, edge_type, properties) VALUES
('sustainable_aviation_fuel', 'renewable_diesel', 'COMPETES_WITH',
 '{"context": "SAF competes with RD and ethanol for feedstock at shared facilities. Growth depends on SAF vs RD economics. Many RD facilities announcing SAF capability addition.", "source": "batch_013"}'::jsonb)
ON CONFLICT DO NOTHING;

-- Biodiesel rationalization
INSERT INTO core.kg_edge (source_node_key, target_node_key, edge_type, properties) VALUES
('biodiesel_rationalization_model', 'renewable_diesel', 'CAUSED_BY',
 '{"context": "RD capacity growth drives violent restructuring of biodiesel capacity. Integrated crusher-biodiesel plants survive; small independents exit. Starting 2024.", "source": "batch_013"}'::jsonb)
ON CONFLICT DO NOTHING;

-- Canadian policy drives demand
INSERT INTO core.kg_edge (source_node_key, target_node_key, edge_type, properties) VALUES
('canada_cfr', 'canada_tallow_balance_sheet', 'CAUSES',
 '{"context": "Canadian clean fuel mandates drive tallow demand from near-zero BBD use to 136-234M lbs/yr by 2024, forcing Canada from tallow self-sufficiency to import dependency.", "source": "batch_013"}'::jsonb)
ON CONFLICT DO NOTHING;

INSERT INTO core.kg_edge (source_node_key, target_node_key, edge_type, properties) VALUES
('canada_cfr', 'canada_canola_oil_balance_sheet', 'CAUSES',
 '{"context": "Canadian RFS/CFR mandates redirect canola oil from export to domestic BBD use. Exports decline as BBD consumption absorbs available supply.", "source": "batch_013"}'::jsonb)
ON CONFLICT DO NOTHING;

INSERT INTO core.kg_edge (source_node_key, target_node_key, edge_type, properties) VALUES
('canada_cfr', 'canada_uco_balance_sheet', 'CAUSES',
 '{"context": "Canadian BBD mandates overwhelm tiny domestic UCO supply base (~110M lbs/yr), requiring 40-200M lbs imports depending on scenario.", "source": "batch_013"}'::jsonb)
ON CONFLICT DO NOTHING;

-- LCFS credit model links
INSERT INTO core.kg_edge (source_node_key, target_node_key, edge_type, properties) VALUES
('lcfs_credit_pricing_model_suncor', 'lcfs_credit_framework', 'EXTENDS',
 '{"context": "Suncor study model quantifies LCFS credit floor at ~$50/tonne marginal shipping cost. Three-step adjustment: model base, CARB modification upside, shipping cost cap.", "source": "batch_013"}'::jsonb)
ON CONFLICT DO NOTHING;

-- Suncor → BBD margin model
INSERT INTO core.kg_edge (source_node_key, target_node_key, edge_type, properties) VALUES
('suncor_feedstock_study', 'bbd_margin_model', 'EXTENDS',
 '{"context": "Suncor study provides projected average gross margins for RD producers, incorporating feedstock costs, fuel revenues, and credit values.", "source": "batch_013"}'::jsonb)
ON CONFLICT DO NOTHING;

-- Canadian soybeans to Canada SBO balance sheet
INSERT INTO core.kg_edge (source_node_key, target_node_key, edge_type, properties) VALUES
('canada_soybeans', 'canola_oil', 'COMPETES_WITH',
 '{"context": "Canadian soybean and canola compete for acreage on the Prairies. Both expanding at wheat expense. SBO crush growing from 1.5 to 3.0 MMT.", "source": "batch_013"}'::jsonb)
ON CONFLICT DO NOTHING;

-- Suncor links to existing HEFA
INSERT INTO core.kg_edge (source_node_key, target_node_key, edge_type, properties) VALUES
('suncor_energy', 'hefa_technology', 'EVALUATES',
 '{"context": "Suncor commissioned this study to evaluate RD investment opportunity. Study provides feedstock S&D, margin outlook, and competitive landscape for HEFA technology.", "source": "batch_013"}'::jsonb)
ON CONFLICT DO NOTHING;


-- ============================================================================
-- 4. CONTEXTS: Analyst-level frameworks and rules
-- ============================================================================

INSERT INTO core.kg_context (node_key, context_type, context_key, content, source_key, confidence) VALUES
('tallow', 'expert_rule', 'tallow_import_surge_requirement',
 'US tallow production grows ~1.5%/yr (livestock herd expansion) from ~5B to ~7.3B lbs by 2037. But RD tallow demand grows from ~2.7B lbs (2018) to 10.9-22.1B lbs (Mid/High 2037). The deficit requires tallow imports to surge from ~270M lbs (2018) to 5.7-8.4B lbs (2025 Mid/High) and 9.2-21.8B lbs (2037 Mid/High). This is the single most important feedstock supply constraint in the RD buildout. Non-biodiesel tallow use shrinks from 3.1B lbs to near zero as BBD absorbs all domestic production plus imports.',
 'suncor_us_bs_2018_2040', 0.90)
ON CONFLICT DO NOTHING;

INSERT INTO core.kg_context (node_key, context_type, context_key, content, source_key, confidence) VALUES
('soybean_oil', 'forecast_framework', 'sbo_bbd_demand_trajectory',
 'SBO is the largest single BBD feedstock at 54% of mix (2022 study). In the 2022 Nov study, SBO BBD use grows from 12.4B lbs (2023) to 35.5B lbs (2035), a CAGR of ~9%. Total SBO production must grow from 27.6B to 48.0B lbs to meet total demand. Non-biodiesel SBO use declines from 14.5B to 12.3B lbs as food/industrial loses share to BBD. Price range projects 31-54 cents/lb (2025 High) rising to 53-86 cents/lb (2035 High).',
 'suncor_lt_forecast_nov22_xlsx', 0.85)
ON CONFLICT DO NOTHING;

INSERT INTO core.kg_context (node_key, context_type, context_key, content, source_key, confidence) VALUES
('canola_oil', 'forecast_framework', 'canola_oil_import_dependency',
 'US canola oil is structurally import-dependent. Domestic production only ~1.8-2.1B lbs/yr but imports grow from 5.5B to 8.9B lbs (2037 High). Total BBD use of canola oil grows from 1.5B lbs (2024) to 5.4B lbs (2037 High). Non-BBD canola use also grows steadily (~5-6.3B lbs) as food/industrial demand expands. Price range: 36-44 to 37-50 cents/lb across forecast. Canada is virtually the only source of US canola oil imports.',
 'suncor_us_bs_2018_2040_slides', 0.85)
ON CONFLICT DO NOTHING;

INSERT INTO core.kg_context (node_key, context_type, context_key, content, source_key, confidence) VALUES
('yellow_grease', 'forecast_framework', 'yg_import_surge',
 'US yellow grease production is relatively flat at 3.0-3.5B lbs/yr. BBD demand grows from 2.7B lbs (2024) to 13.8B lbs (2037 High). Like tallow, the deficit is met by massive imports growing from near zero (2018) to 4.1B lbs (2025 High) and 10.0B lbs (2037 High). Non-biodiesel YG use shrinks from 817M lbs (2018) to negative levels by 2028 in projections, meaning ALL domestic production plus imports are absorbed by BBD.',
 'suncor_us_bs_2018_2040_slides', 0.85)
ON CONFLICT DO NOTHING;

INSERT INTO core.kg_context (node_key, context_type, context_key, content, source_key, confidence) VALUES
('feedstock_supply_chain_model', 'expert_rule', 'plant_level_aggregation_method',
 'The Jacobsen/Fastmarkets methodology for forecasting industry feedstock demand works bottom-up: (1) assign individual feedstock mix to each RD/biodiesel facility based on location, technology, and contracts, (2) project monthly production at each facility, (3) multiply production x mix to get monthly feedstock demand per plant, (4) sum across all plants to get industry totals, (5) divide each feedstock total by grand total to get industry-wide mix percentages. This contrasts with top-down approaches that assume a fixed national mix.',
 'suncor_lt_forecast_assumptions_nov22', 0.90)
ON CONFLICT DO NOTHING;

INSERT INTO core.kg_context (node_key, context_type, context_key, content, source_key, confidence) VALUES
('lcfs_credit_framework', 'expert_rule', 'lcfs_marginal_shipping_cost_floor',
 'The LCFS credit value has a practical floor at the marginal cost of shipping RD to California (~$50/tonne as of 2022). Below this level, producers stop shipping to CA because the credit revenue does not cover transportation. CARB modifications (forklift phase-out, hydrogen inclusion, intrastate jet fuel) aim to lift credit values, but added revenue incentivizes MORE RD shipments to CA, which caps the upside. If CARB imposes lipid-based biofuel caps, the qualifying supply constraint could lift credits above the shipping cost floor.',
 'suncor_lt_forecast_assumptions_nov22', 0.85)
ON CONFLICT DO NOTHING;

INSERT INTO core.kg_context (node_key, context_type, context_key, content, source_key, confidence) VALUES
('renewable_diesel', 'expert_rule', 'rd_displaces_biodiesel_capacity',
 'Renewable diesel capacity growth drives biodiesel capacity rationalization in two phases: (1) 2022-2024: SRE elimination and supplemental mandates temporarily support BD margins, (2) 2024+: supplemental mandates expire and IRA transition from blenders credit to production credit hits BD producers. Surviving BD capacity concentrates at large facilities integrated with multinational crushers. State-level policies may slow the decline but accelerate consolidation. BD production share falls as RD production grows ~10x.',
 'suncor_lt_forecast_assumptions_nov22', 0.85)
ON CONFLICT DO NOTHING;

INSERT INTO core.kg_context (node_key, context_type, context_key, content, source_key, confidence) VALUES
('sustainable_aviation_fuel', 'forecast_framework', 'saf_pricing_premium_trajectory',
 'SAF prices start at ~2x jet fuel price, narrowing to ~1.25x by forecast end as production scales. Pricing does NOT include credit values (RINs, LCFS). Credit allocation between producer and airline varies by contract — some airlines get full credit value, some get none. California likely price-setting market. If CARB requires intrastate SAF, stable demand base could accelerate production expansion.',
 'suncor_lt_forecast_assumptions_nov22', 0.85)
ON CONFLICT DO NOTHING;

INSERT INTO core.kg_context (node_key, context_type, context_key, content, source_key, confidence) VALUES
('canada_cfr', 'forecast_framework', 'canada_feedstock_import_dependency',
 'Canadian clean fuel mandates create structural import dependency across all BBD feedstocks. Before mandates, Canada was roughly self-sufficient in tallow (~260M lbs production, ~260M lbs domestic use) and exported canola oil (2.4-3.1B lbs). Under mandate scenarios: (a) tallow imports grow from zero to 55-190M lbs by 2024, (b) canola oil exports decline as domestic BBD absorbs supply, (c) UCO imports grow from zero to 40-200M lbs, (d) SBO imports remain small (~20M lbs) but domestic BBD use grows from ~6M lbs (2017) to 181-340M lbs (2024). Canada becomes a net feedstock IMPORTER as mandates grow.',
 'suncor_canadian_bs', 0.90)
ON CONFLICT DO NOTHING;

INSERT INTO core.kg_context (node_key, context_type, context_key, content, source_key, confidence) VALUES
('acreage_rules_of_thumb', 'expert_rule', 'crop_rotation_constraint_100m_soy',
 'Reaching 100M acres of continuous US soybean area faces a hard constraint: farmers make at least 50% of acreage decisions based on crop rotation concerns. With limited unused arable land, the expansion must come from other crops. Historical precedent: last major soy acreage expansion came from Plains wheat-to-soy substitution. But high wheat prices (Ukraine war) reduce willingness to switch. The war-driven wheat premium must subside before the next wave of soy expansion.',
 'suncor_lt_forecast_assumptions_nov22', 0.85)
ON CONFLICT DO NOTHING;

INSERT INTO core.kg_context (node_key, context_type, context_key, content, source_key, confidence) VALUES
('bbd_margin_model', 'forecast_framework', 'ira_transition_impact_bd_margins',
 'The 2025 transition from blenders tax credit to IRA production credit (45Z) has asymmetric impact: RD producers benefit because production credit rewards lower-CI feedstocks, while BD producers lose because blenders credit was feedstock-agnostic. This policy shift is one of the triggers for BD capacity rationalization, as BD margins compress while RD margins may be maintained or improved through CI optimization.',
 'suncor_lt_forecast_assumptions_nov22', 0.85)
ON CONFLICT DO NOTHING;

INSERT INTO core.kg_context (node_key, context_type, context_key, content, source_key, confidence) VALUES
('feedstock_mix_methodology', 'expert_rule', 'feedstock_mix_shifts_over_forecast',
 'The industry-wide feedstock mix is not static — it shifts substantially from forecast beginning to end. In the 2022 study, the average mix is 54% SBO / 14% DCO / 13% UCO / 9% BFT / 5% YG / 3% CWG / 2% canola / <1% poultry fat. But monthly variation is large, and the composition changes as (a) new RD facilities with different feedstock flexibility come online, (b) CI-score optimization shifts toward lower-CI feedstocks (UCO, waste fats), (c) feedstock price signals alter economic mix. Plant-level methodology captures these shifts; top-down approaches miss them.',
 'suncor_lt_forecast_assumptions_nov22', 0.90)
ON CONFLICT DO NOTHING;


-- ============================================================================
-- 5. UPDATE existing nodes with Suncor-derived reinforcement
-- ============================================================================

-- Reinforce tallow node
UPDATE core.kg_node
SET properties = properties || jsonb_build_object(
    'suncor_study_reinforcement', 'Suncor study (2018+2022) projects US tallow BBD use growing from 2.4B lbs (2018) to 11.6-13.7B lbs (2025 Mid/High) driven by RD. US production only grows to 5.5B lbs by 2025. Import surge of 1.4-8.4B lbs required. Tallow is 70% of RD feedstock mix by weight.'
),
last_reinforced = NOW()
WHERE node_key = 'tallow';

-- Reinforce canola_oil node
UPDATE core.kg_node
SET properties = properties || jsonb_build_object(
    'suncor_study_reinforcement', 'Suncor study projects US canola oil BBD use growing from 1.5B lbs (2024) to 5.4B lbs (2037 High). Canada is sole import source. Canadian domestic BBD mandates compete with US exports — tension between Canadian CFR mandates absorbing canola oil domestically vs US import demand. Canada canola acreage to 10M ha projected.'
),
last_reinforced = NOW()
WHERE node_key = 'canola_oil';

-- Reinforce used_cooking_oil node
UPDATE core.kg_node
SET properties = properties || jsonb_build_object(
    'suncor_study_reinforcement', 'Suncor study: UCO is 18% of RD feedstock. US YG (proxy for UCO) production flat at ~3B lbs/yr but BBD demand grows to 13.8B lbs by 2037 (High). Canada UCO production only ~110M lbs/yr but BBD mandates require 40-200M lbs imports. Global UCO supply chain must scale dramatically.'
),
last_reinforced = NOW()
WHERE node_key = 'used_cooking_oil';

-- Reinforce renewable_diesel node
UPDATE core.kg_node
SET properties = properties || jsonb_build_object(
    'suncor_study_reinforcement', 'Suncor study projects RD production growing from ~510M gal (2018) to 2,400-3,040M gal (2031 High/Mid). Total BBD feedstock requirement grows from 19.4B lbs (2018) to 54.4B lbs (2031 High). Study methodology uses plant-level feedstock mix aggregation. SBO dominates at 54% of mix, tallow dominates RD-specific feedstock at 70%.'
),
last_reinforced = NOW()
WHERE node_key = 'renewable_diesel';

-- Reinforce canada_cfr node
UPDATE core.kg_node
SET properties = properties || jsonb_build_object(
    'suncor_study_reinforcement', 'Suncor study provides quantified Canadian BBD demand trajectory: 178M gal (2018) to 889M gal (high case long-term). Canadian balance sheets show mandate growth forces structural import dependency for tallow (55-190M lbs), UCO (40-200M lbs), and redirects canola oil exports to domestic BBD use. Historical 2013-2014 compliance data shows animal material was 42% of BBD feedstock, oilseeds 44%.'
),
last_reinforced = NOW()
WHERE node_key = 'canada_cfr';

-- Reinforce feedstock_supply_chain_model node
UPDATE core.kg_node
SET properties = properties || jsonb_build_object(
    'suncor_study_reinforcement', 'Suncor study operationalizes feedstock supply chain model with 20-year projections: 7 US + 4 Canadian balance sheets x 3 scenarios. Demonstrates that ALL major fats/greases (tallow, YG, CWG, poultry fat) face same structural pattern: domestic production grows slowly (~1.5%/yr on livestock herd) while BBD demand grows 5-10x, forcing massive import dependency. Only SBO has sufficient production growth potential via acreage expansion.'
),
last_reinforced = NOW()
WHERE node_key = 'feedstock_supply_chain_model';


-- ============================================================================
-- END OF BATCH 013
-- ============================================================================
