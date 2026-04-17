-- ============================================================================
-- RLC Knowledge Graph: Extraction Batch 018 (Q2 2022 Long-Term Forecast)
-- Source: Jacobsen Q2 2022 Fats, Fuels & Feedstock Long-Term Outlook Package
-- Folder: C:/Users/torem/RLC Dropbox/Tore Alden/Jacobsen/Quarterly Outlook Report/Q2 2022/
-- Extracted: 2026-04-16
-- Scope:  Complete 10-year (2022-2032) feedstock-by-feedstock S&D outlook.
--         Covers BBD capacity expansion, feedstock competition, CI scores,
--         LCFS credit/deficit generation, RVO/RFS policy, IRA/BTC transition,
--         biodiesel rationalization, RD capacity ramp, ethanol blend wall,
--         SBO crush capacity constraints, pretreatment economics, CARB scoping.
-- ============================================================================

-- KEY FINDINGS:
--   * RD capacity: 1.95B gal (Jun 2022), predicted 2.7B by YE 2022; RD output doubled YoY to 359M gal/qtr
--   * Biodiesel rationalization: stable capacity through 2023, rapid decline after as RD displaces
--   * Ethanol blend wall: 890M gal shortfall in 2022, growing to 935M in 2023 -> forces extra 555-580M gal BBD
--   * IRA/BTC: $1/gal BTC extended through 2024, then CI-based credit excluding imports -> bullish RINs
--   * SREs: 69 small refinery exemptions denied, adding ~1.8B gal effective RVO
--   * LCFS credit bank: 10.35M tonnes Q1 2022, predicted 12-14M by end 2023, CARB considering 25-30% CI target
--   * CARB food-vs-fuel: considering cap on lipid-based feedstocks (EU-style), limited near-term impact
--   * SBO: crush capacity needs to expand to 2.6B bu by 2027; soybean acreage must avg 94.9M acres
--   * US SBO stocks-to-use forecast avg 5.6% (vs 8.2% prior 10yr avg) -> structurally tight
--   * SBO refining + pretreatment expansion: +4.5B lbs refining + 3.4B lbs pretreatment capacity by 2027
--   * UCO/YG: massive modeling difficulty due to lack of reliable production data; non-biofuel use goes negative
--   * Fat/grease non-biofuel demand cut avg 1.3B lbs/yr as BBD absorbs supply
--   * DCO: production constrained by ethanol output; high prices may justify faster yield improvements
--   * CWG/Tallow: RD import assumption change drives biggest forecast revisions
--   * CI scores declining: RD at 35.6, biodiesel 27.28, alt jet fuel record low 23.14
--   * LCFS feedstock mix: biodiesel = 90% fats/greases (50% DCO, 28% UCO); RD = 80% fats/greases (28% tallow, 24% UCO, 24% DCO, 22% SBO)
--   * RD margins: fats/greases mix >$3.60/gal (Apr 2022); SBO/YG mix peaked ~$1/gal (Jun 2022)
--   * Biodiesel margins: 75% SBO/25% YG mix peaked >$1/gal late Q2
--   * D4 RINs avg $1.69/gal Q2 (up from $1.43 Q1); LCFS credits fell from $109 to $81/MT
--   * Domestic BBD production: record 786M gal Q2 2022 (+25% YoY)
--   * CARB RD share: 77% of domestic RD shipped to California in Q1 2022


-- ============================================================================
-- 1. NODES: Models, Frameworks, Data Series
-- ============================================================================

-- Long-Term BBD Capacity Expansion Model
INSERT INTO core.kg_node (node_type, node_key, label, properties) VALUES
('model', 'bbd_capacity_expansion_q2_2022', 'BBD Capacity Expansion Forecast (Q2 2022 Vintage)',
 '{"description": "10-year forecast framework for US biomass-based diesel production capacity evolution: renewable diesel expansion displacing biodiesel, with biodiesel rationalization delayed by supplemental mandates and ethanol blend wall.", "vintage": "Q2 2022", "forecast_period": "2022-2032",
   "rd_capacity_trajectory": {"q1_2022_bgal": 1.46, "q2_2022_bgal": 1.95, "ye_2022_predicted_bgal": 2.7},
   "biodiesel_capacity": {"q1_2022_bgal": 2.231, "q2_2022_bgal": 2.215, "stable_through": 2023, "rapid_decline_after": 2023, "floor_factors": ["integrated_producers", "state_level_mandates"]},
   "production_q2_2022": {"total_bgal": 0.786, "rd_bgal": 0.359, "biodiesel_bgal": 0.427, "rd_yoy_growth_pct": 100, "biodiesel_yoy_pct": -5},
   "rationalization_framework": "Biodiesel capacity stable while supplemental mandate supports margins. RD capacity expansion in 2023 could accelerate biodiesel rationalization. Integrated producers and state mandates set floor.",
   "key_driver": "RD capacity expansion rate determines biodiesel margin compression timing",
   "source": "jacobsen_q2_2022_executive_summary"}'::jsonb)
ON CONFLICT (node_key) DO UPDATE SET properties = EXCLUDED.properties;

-- Ethanol Blend Wall Model
INSERT INTO core.kg_node (node_type, node_key, label, properties) VALUES
('model', 'ethanol_blend_wall_model', 'Ethanol Blend Wall / BBD Demand Linkage Model',
 '{"description": "Framework linking ethanol blend wall to incremental BBD demand. When ethanol production falls short of implied RFS mandate, the shortfall must be met by additional BBD production. This structural support for BBD demand is a key driver of biodiesel margin resilience despite RD expansion.", "vintage": "Q2 2022",
   "mechanics": {"ethanol_blend_rate_historical_avg_pct": 10.2, "ethanol_blend_rate_2022_assumed_pct": 10.6, "implied_ethanol_mandate_2022_bgal": 15.25, "ethanol_shortfall_2022_mgal": 890, "ethanol_shortfall_2023_mgal": 935, "additional_bbd_from_shortfall_2022_mgal": 555, "additional_bbd_from_shortfall_2023_mgal": 580},
   "mandate_context": {"total_rvo_2022_bgal": 20.63, "advanced_2022_bgal": 5.05, "cellulosic_2022_mgal": 630, "supplemental_2022_2023_mgal": 250, "sre_denied_count": 69, "sre_effective_addition_bgal": 1.8},
   "causal_chain": "Rising implied ethanol mandate -> gasoline demand insufficient to absorb -> ethanol blend wall shortfall -> additional BBD production required -> supports biodiesel margins -> delays biodiesel rationalization",
   "key_insight": "Ethanol demand is function of gasoline demand, not mandates. Increasing mandate does not increase ethanol blending -- it increases BBD demand.",
   "source": "jacobsen_q2_2022_executive_summary"}'::jsonb)
ON CONFLICT (node_key) DO UPDATE SET properties = EXCLUDED.properties;

-- SBO Crush Capacity Expansion Model
INSERT INTO core.kg_node (node_type, node_key, label, properties) VALUES
('model', 'sbo_crush_capacity_model_q2_2022', 'US SBO Crush & Refining Capacity Expansion Forecast',
 '{"description": "Framework for US soybean crushing, oil refining, and RD pretreatment capacity expansion needed to meet biofuel feedstock demand growth. Core tension: crush/refining capacity constrains SBO availability, elevated basis signals capacity shortage.", "vintage": "Q2 2022",
   "crush_capacity_trajectory": {"q4_2022_bbu": 2.3, "by_2027_bbu": 2.6},
   "refining_capacity_expansion_by_2027_blbs": 4.5,
   "pretreatment_expansion_by_2027_blbs": 3.4,
   "pretreatment_coverage_pct_of_rd_capacity": 90,
   "pretreatment_caveats": ["Not probability-adjusted for individual plant openings", "Processes all feedstock types not just SBO", "Refining margin reversion may deter future additions"],
   "acreage_requirement": {"avg_planted_acres_m": 94.9, "sustainability_concern": "Additional 7M acres sustained over 10 years would impact crop rotations in unsustainable way", "sbo_stocks_to_use_forecast_avg_pct": 5.6, "sbo_stocks_to_use_prior_10yr_avg_pct": 8.2},
   "basis_dynamics": "Elevated SBO basis despite adequate stocks signals capacity shortage. Relief expected as capacity reaches 2.3B bu in Q4 2022. Pace of RD expansion determines basis pressure.",
   "refining_margin_economics": "Historically high refining margins justify pretreatment capex. But margin reversion to long-term avg would undermine future pretreatment investment decisions.",
   "source": "jacobsen_q2_2022_veg_oil_summary"}'::jsonb)
ON CONFLICT (node_key) DO UPDATE SET properties = EXCLUDED.properties;

-- LCFS Credit/Deficit Generation Model
INSERT INTO core.kg_node (node_type, node_key, label, properties) VALUES
('model', 'lcfs_credit_deficit_model_q2_2022', 'LCFS Credit/Deficit Generation Forecast (Q2 2022 Vintage)',
 '{"description": "Quarterly credit and deficit generation forecast under CARB LCFS program with credit bank trajectory and price implications. Includes electricity/EV credits, bio-CNG, and all liquid biofuels.", "vintage": "Q2 2022",
   "credit_generation_q1_2022": {"total_mt": 6000000, "ex_electricity_projects_mt": 4630000, "rd_mt": 2270000, "biodiesel_mt": 552304, "ethanol_mt": 917060, "electricity_total_mt": 1380000, "ev_credits_mt": 896455},
   "deficit_generation_q1_2022": {"total_mt": 5150000, "carbob_mt": 4050000, "carbob_incremental_mt": 329050, "diesel_incremental_mt": 61885},
   "cumulative_bank": {"q1_2022_mt": 10350000, "q2_2022_predicted_mt": 10740000, "q4_2022_predicted_mt": 10310000, "ye_2023_range_mt": [12000000, 14000000], "ye_2024_uncapped_mt": 20000000},
   "carb_intervention": "Bank growth above 20M tonnes by 2024 is unlikely -- CARB will change program rules. Changes could push bank below 10M by end 2024. Without intervention, bank does not fall below 10M until 2027.",
   "lcfs_price_implications": "Credit prices fell from $150 (YE 2021) to $113 (Mar 2022). Short-term stabilization possible but downside continues through 2023 without CARB intervention. Price volatility likely rises late 2023 on CARB policy uncertainty.",
   "rd_ca_share_pct": {"q1_2022": 77, "full_year_2022_predicted": 68, "long_term_avg_2023_2033": 72},
   "biodiesel_ca_share_pct": {"q1_2022": 20.5, "full_year_2022_predicted": 17.5, "long_term_2024_2033": 65},
   "source": "jacobsen_q2_2022_lcfs_credit_deficit"}'::jsonb)
ON CONFLICT (node_key) DO UPDATE SET properties = EXCLUDED.properties;

-- LCFS Feedstock Mix Model (California-Specific)
INSERT INTO core.kg_node (node_type, node_key, label, properties) VALUES
('model', 'lcfs_feedstock_mix_model', 'LCFS Program Feedstock Mix by Fuel Type',
 '{"description": "Detailed feedstock mix breakdown for biodiesel and renewable diesel volumes under CARB LCFS program. Fats/greases dominate California feedstock mix at ~80-90% vs national average of ~31-33%.", "vintage": "Q2 2022",
   "biodiesel_lcfs_mix_q1_2022": {"dco_pct": 50, "uco_pct": 28, "tallow_pct": 11, "sbo_pct": 5, "canola_pct": 5, "other_pct": 0.4, "fats_greases_total_pct": 90, "veg_oil_total_pct": 10},
   "biodiesel_lcfs_implied_lbs_q1_2022": {"dco_mlbs": 284, "uco_mlbs": 158, "tallow_mlbs": 61, "sbo_mlbs": 25, "canola_mlbs": 24},
   "rd_lcfs_mix_q1_2022": {"tallow_pct": 28, "uco_pct": 24, "dco_pct": 24, "sbo_pct": 22, "other_pct": 1.5, "fats_greases_total_pct": 80, "veg_oil_total_pct": 20},
   "rd_lcfs_implied_lbs_q1_2022": {"tallow_mlbs": 846, "dco_mlbs": 718, "uco_mlbs": 624, "sbo_mlbs": 532, "other_mlbs": 41},
   "national_biodiesel_mix": {"veg_oil_pct": 69, "fats_greases_pct": 31},
   "national_rd_mix": {"veg_oil_pct": 67, "fats_greases_pct": 33},
   "key_insight": "California feedstock mix is dramatically different from national: ~80-90% fats/greases vs ~31-33% nationally. This divergence is driven by LCFS credit value rewarding lower-CI feedstocks. If IRA shifts national mix toward California mix, requires +5.5B lbs additional fats/greases.",
   "predicted_2022_rd_mix": {"dco_pct": 19, "uco_pct": 28, "tallow_pct": 28, "sbo_pct": 22, "other_pct": 2.4},
   "source": "jacobsen_q2_2022_ci_scores_summary"}'::jsonb)
ON CONFLICT (node_key) DO UPDATE SET properties = EXCLUDED.properties;

-- CI Score Trend Model
INSERT INTO core.kg_node (node_type, node_key, label, properties) VALUES
('data_series', 'lcfs_ci_scores_q2_2022', 'LCFS Average CI Scores by Fuel Type (Q1 2022)',
 '{"description": "Average carbon intensity scores reported by CARB for biofuels sold under LCFS program. Declining CI scores reflect shift to lower-CI feedstocks and process improvements.", "vintage": "Q1 2022 actuals",
   "ci_scores_q1_2022": {"ethanol": 58.47, "biodiesel": 27.28, "renewable_diesel": 35.6, "alternative_jet_fuel": 23.14, "electricity": 27.83, "bio_cng": -61.4},
   "trends": {"ethanol": "Slowly declining, lowest since Q4 2020", "biodiesel": "Declining, lowest since Q3 2020 -- driven by fats/greases dominance in CA mix", "rd": "Declining from 38.08 prev quarter, lowest since Q4 2020", "alt_jet": "Record low 23.14, less than half of initial 50 (Q2 2019)", "bio_cng": "Deeply negative at -61.4, indicating very high credit generation per gallon equivalent", "electricity": "Modestly rising to 27.83 from 27.43"},
   "key_insight": "Biodiesel CI lower than RD despite same HEFA-type feedstocks because California biodiesel is 90% fats/greases. RD has 22% SBO dragging CI higher. Alt jet fuel at record low CI = highest credit value per gallon.",
   "source": "jacobsen_q2_2022_ci_scores_summary"}'::jsonb)
ON CONFLICT (node_key) DO UPDATE SET properties = EXCLUDED.properties;

-- UCO/YG Supply Estimation Model
INSERT INTO core.kg_node (node_type, node_key, label, properties) VALUES
('model', 'uco_yg_supply_estimation_model', 'UCO/YG Production Estimation Methodology',
 '{"description": "Framework for estimating UCO and yellow grease production given absence of reliable official data. Relies on residual calculation: production = BBD feedstock use + exports + non-biofuel use - imports. Fundamental modeling challenge because limited growth in production vs massive growth in RD feedstock demand.", "vintage": "Q2 2022",
   "methodology": "Back-calculate production from known demand (BBD feedstock from EIA/CARB, exports from Census, imports) and estimated non-biofuel use. Residual approach leaves non-biofuel demand as the swing variable.",
   "core_problem": "Making model fit both historical data and projected RD demand growth is challenging. Non-biofuel demand goes negative for several years in best-fit model, indicating either production underestimated or demand overestimated.",
   "q2_2022_revisions": {"historical_production_cut_2021_mlbs": 1100, "production_forecast_cut_2022_mlbs": 870, "production_forecast_cut_2023_mlbs": 859, "forecast_cuts_declining_to_2030_mlbs": 30, "import_forecast_raised_avg_mlbs": 151},
   "bbd_demand_raised": {"total_avg_increase_2023_2032_mlbs": 1200, "rd_demand_2028_increase_mlbs": 1300, "biodiesel_avg_increase_mlbs": 238},
   "non_biofuel_demand_cuts": {"avg_2023_2032_mlbs": 1100, "range_mlbs": [811, 1500]},
   "export_cuts_avg_mlbs": 100,
   "key_warning": "Substantial uncertainty in production estimates. Model may undergo significant revision in subsequent quarters.",
   "source": "jacobsen_q2_2022_yg_uco_changes"}'::jsonb)
ON CONFLICT (node_key) DO UPDATE SET properties = EXCLUDED.properties;

-- Canola Oil BBD Pathway Model
INSERT INTO core.kg_node (node_type, node_key, label, properties) VALUES
('model', 'canola_oil_bbd_pathway', 'Canola Oil RD/SAF Pathway Approval Framework',
 '{"description": "EPA pathway approval process for canola oil in renewable diesel and sustainable aviation fuel production. Published proposed rule April 18, 2022. Pathway approval enables RIN generation for canola-oil-based RD/SAF.", "vintage": "Q2 2022",
   "timeline": {"proposed_rule_date": "2022-04-18", "public_comment_period_days": 30, "next_step": "EPA considers comments, amends GHG lifecycle analysis, then company-specific pathway application required", "pending_applications": "US Canola Association only (as of Q2 2022)"},
   "demand_projection": {"current_bbd_use_blbs": 1.2, "expected_decline_through": "2025/26", "recovery_to_current_by": "end of forecast period", "substitution_dynamic": "RD canola use offsets declining biodiesel canola use as biodiesel capacity rationalizes"},
   "food_substitution": "Canola non-biofuel demand expected to grow from 4.125B lbs (2021/22 low) to 6B+ lbs by end of forecast. Food industry substitution for SBO is substantially larger driver than BBD use.",
   "long_term_domestic_use_growth_pct_per_yr": 2.8,
   "source": "jacobsen_q2_2022_veg_oil_summary"}'::jsonb)
ON CONFLICT (node_key) DO UPDATE SET properties = EXCLUDED.properties;

-- CARB Scoping Plan / Food-vs-Fuel Framework
INSERT INTO core.kg_node (node_type, node_key, label, properties) VALUES
('policy', 'carb_2022_scoping_plan', 'CARB 2022 Scoping Plan (LCFS Amendments)',
 '{"description": "CARB scoping plan process for LCFS program changes, including higher CI reduction targets and potential cap on crop-based feedstocks. Key policy risk for BBD industry.", "vintage": "Q2 2022",
   "proposals": {"ci_target_2030_options_pct": [25, 30], "current_ci_target_2030_pct": 20, "jet_fuel_inclusion": "Considering requiring all intrastate fossil jet fuel in LCFS, creating deficits for airlines", "crop_feedstock_cap": "Considering EU-style limit on biofuels from food and feed crops", "fall_2022_workshop": "Planned workshop to evaluate supply/demand of alternative fuels and feasibility of CI targets"},
   "impact_assessment": "Limited near-term impact on BBD industry. Cap on lipid-based feedstocks would likely shift biofuel flows within US rather than reducing total veg oil usage. CARB SBO share is only 17-18% of total LCFS feedstock. Until next-gen feedstocks available, limits on veg oils will delay carbon emission targets.",
   "sbo_context": {"sbo_share_of_ca_feedstock_pct": 17, "sbo_ca_quarterly_mlbs": 550, "sbo_ca_as_pct_of_national_bbd_sbo_use": 22.5, "canola_plus_sbo_ca_pct": 18, "canola_plus_sbo_ca_mlbs": 580},
   "food_vs_fuel_status": "BBD industry has not faced same food-vs-fuel intensity as ethanol during its expansion. Becoming more common topic among public officials. Limit would likely shift flows, not volumes.",
   "source": "jacobsen_q2_2022_executive_summary"}'::jsonb)
ON CONFLICT (node_key) DO UPDATE SET properties = EXCLUDED.properties;

-- BBD Margin Framework (Q2 2022 Vintage)
INSERT INTO core.kg_node (node_type, node_key, label, properties) VALUES
('data_series', 'bbd_margins_q2_2022', 'BBD Producer Margins Q2 2022 Snapshot',
 '{"description": "Margin estimates for biodiesel and renewable diesel producers by feedstock mix during Q2 2022 record-margin environment.", "vintage": "Q2 2022",
   "biodiesel_margins": {"feedstock_mix": "75% SBO / 25% YG", "peak_margin_per_gal": 1.0, "timing": "Late Q2 2022", "driver": "Veg oil price peak + strong biofuel prices"},
   "rd_margins_fats_greases": {"feedstock_mix": "50% tallow / 25% DCO / 25% UCO", "peak_margin_per_gal": 3.6, "timing": "Late April 2022", "driver": "Sharp increase in biofuel prices before fat/grease prices followed", "pressure_after_peak": "Stickiness of fat/grease prices weighed on margins as energy prices declined"},
   "biofuel_prices_q2_2022": {"wc_biodiesel_low": 3.31, "wc_biodiesel_high": 4.13, "rd_low": 3.60, "rd_high": 4.64},
   "credit_prices_q2_2022": {"d4_rin_avg": 1.69, "d4_rin_q1_avg": 1.43, "lcfs_q2_avg_per_mt": 81, "lcfs_q1_avg_per_mt": 109},
   "energy_price_forecast": {"2023_decline_pct": -8, "2024_decline_pct": -13, "2025_decline_pct": -7, "2026": "stable", "2027_plus_annual_growth_pct": 3},
   "source": "jacobsen_q2_2022_executive_summary"}'::jsonb)
ON CONFLICT (node_key) DO UPDATE SET properties = EXCLUDED.properties;

-- RVO/RFS Mandate Framework (Q2 2022)
INSERT INTO core.kg_node (node_type, node_key, label, properties) VALUES
('data_series', 'rfs_rvo_q2_2022', 'RFS/RVO Mandate Levels (2020-2025 Framework)',
 '{"description": "Final RVO levels announced June 3, 2022 covering retroactive 2020 and 2021 plus 2022. First year after RFS law expiration gives EPA discretion on future mandates.", "vintage": "Q2 2022",
   "mandates": {"total_2020_bgal": 17.13, "total_2021_bgal": 18.84, "total_2022_bgal": 20.63, "advanced_2020_bgal": 4.63, "advanced_2021_bgal": 5.05, "advanced_2022_not_specified": true, "cellulosic_2020_mgal": 510, "cellulosic_2021_mgal": 560, "cellulosic_2022_mgal": 630, "supplemental_2022_2023_mgal": 250},
   "implied_ethanol": {"2020_bgal": 12.5, "2021_bgal": 13.79, "2022_bgal": 15.0, "2022_with_supplemental_bgal": 15.25},
   "sre_denial": {"applications_denied": 69, "effective_rvo_addition_bgal": 1.8, "context": "Biden administration rejected all SREs to offset prior mandate cuts"},
   "post_expiration_framework": "RFS law expired, leaving EPA to set mandates without statutory guidance. EPA no longer needs exemptions from law. Preliminary 2023-2025 mandates planned for Nov 2022.",
   "source": "jacobsen_q2_2022_executive_summary"}'::jsonb)
ON CONFLICT (node_key) DO UPDATE SET properties = EXCLUDED.properties;

-- RD Import Assumption (Critical Forecast Driver)
INSERT INTO core.kg_node (node_type, node_key, label, properties) VALUES
('model', 'rd_import_assumption_q2_2022', 'RD Import Volume Assumption (Key Forecast Variable)',
 '{"description": "The single most impactful assumption change in Q2 2022 outlook: substantial increase in assumed US renewable diesel imports reduced domestic feedstock demand projections across all commodities. Import assumption change cascaded through every feedstock balance sheet.", "vintage": "Q2 2022",
   "direction": "Increased RD import assumption from prior quarter",
   "cascade_effects": {
     "sbo_bbd_demand_cut_2022_blbs": 4.0, "sbo_bbd_demand_cut_2023_blbs": 1.7, "sbo_bbd_demand_cut_2032_blbs": 8.8,
     "veg_oil_total_rd_demand_cut_2022_blbs": 3.0, "veg_oil_total_rd_demand_cut_2032_blbs": 8.1,
     "tallow_demand_reduction_2032_blbs": 1.1,
     "cwg_rd_demand_cut_2022_mlbs": 266,
     "fat_grease_rd_demand_cut_2022_mlbs": 428
   },
   "offsetting_effects": {
     "non_biofuel_demand_raised": "Across all feedstocks, lower BBD demand allowed higher non-biofuel use projections",
     "biodiesel_demand_raised_short_term": "Supplemental mandate and ethanol blend wall supported biodiesel demand in 2022-2023",
     "import_decline_reduced": "Lower domestic feedstock demand reduced predicted feedstock import requirements"
   },
   "uncertainty": "If RINs do not rise enough to offset loss of $1/gal BTC for importers (post-2024), imports could be significantly lower than predicted -> higher domestic feedstock demand than this forecast assumes",
   "source": "jacobsen_q2_2022_multiple_documents"}'::jsonb)
ON CONFLICT (node_key) DO UPDATE SET properties = EXCLUDED.properties;


-- ============================================================================
-- 2. EDGES: Causal relationships and cross-commodity links
-- ============================================================================

-- Ethanol blend wall drives BBD demand
INSERT INTO core.kg_edge (source_node_id, target_node_id, edge_type, weight, properties, created_by, confidence) VALUES
((SELECT id FROM core.kg_node WHERE node_key = 'ethanol_blend_wall_model'),
 (SELECT id FROM core.kg_node WHERE node_key = 'bbd_balance_sheet_model'),
 'CAUSES', 0.95,
 '{"mechanism": "Ethanol blend wall shortfall (890M gal in 2022, growing to 935M in 2023) forces additional 555-580M gal/yr of BBD production to meet overall RFS mandates. This structural demand support delays biodiesel rationalization and maintains biodiesel margins despite RD capacity expansion.", "quantification": "555-580M additional gal BBD per year", "direction": "demand_increasing", "source": "jacobsen_q2_2022_executive_summary"}'::jsonb,
 'extracted', 0.95);

-- Ethanol blend wall supports biodiesel against RD displacement
INSERT INTO core.kg_edge (source_node_id, target_node_id, edge_type, weight, properties, created_by, confidence) VALUES
((SELECT id FROM core.kg_node WHERE node_key = 'ethanol_blend_wall_model'),
 (SELECT id FROM core.kg_node WHERE node_key = 'bbd_capacity_expansion_q2_2022'),
 'CAUSES', 0.90,
 '{"mechanism": "Supplemental mandate combined with ethanol blend wall raised biodiesel margins enough to limit capacity rationalization through 2023. Without blend wall shortfall, biodiesel rationalization would have begun earlier.", "source": "jacobsen_q2_2022_executive_summary"}'::jsonb,
 'extracted', 0.90);

-- RD capacity expansion drives feedstock competition
INSERT INTO core.kg_edge (source_node_id, target_node_id, edge_type, weight, properties, created_by, confidence) VALUES
((SELECT id FROM core.kg_node WHERE node_key = 'bbd_capacity_expansion_q2_2022'),
 (SELECT id FROM core.kg_node WHERE node_key = 'feedstock_supply_chain_model'),
 'CAUSES', 0.95,
 '{"mechanism": "RD capacity doubling YoY (Q2 2022) with further expansion to 2.7B gal by YE 2022 drives feedstock competition. Fats/greases preferred for lower CI but supply is relatively fixed. Veg oils (primarily SBO) must fill gap. Drives crush capacity expansion requirement to 2.6B bu by 2027.", "source": "jacobsen_q2_2022_executive_summary"}'::jsonb,
 'extracted', 0.95);

-- RD imports reduce domestic feedstock demand
INSERT INTO core.kg_edge (source_node_id, target_node_id, edge_type, weight, properties, created_by, confidence) VALUES
((SELECT id FROM core.kg_node WHERE node_key = 'rd_import_assumption_q2_2022'),
 (SELECT id FROM core.kg_node WHERE node_key = 'soybean_oil'),
 'CAUSES', 0.90,
 '{"mechanism": "Higher RD import assumption cut SBO BBD demand by 4B lbs (2022) growing to 8.8B lbs (2032). Reduced SBO import requirements. Allowed higher non-biofuel use and raised SBO ending stocks. Lowered price forecasts by avg 11 cpb across forecast period.", "direction": "demand_decreasing", "source": "jacobsen_q2_2022_sbo_changes"}'::jsonb,
 'extracted', 0.90);

-- RD imports reduce tallow demand
INSERT INTO core.kg_edge (source_node_id, target_node_id, edge_type, weight, properties, created_by, confidence) VALUES
((SELECT id FROM core.kg_node WHERE node_key = 'rd_import_assumption_q2_2022'),
 (SELECT id FROM core.kg_node WHERE node_key = 'tallow'),
 'CAUSES', 0.85,
 '{"mechanism": "Higher RD imports cut tallow demand projections. Import forecasts reduced from last quarter by up to 1.1B lbs (2032). Non-biofuel demand cuts averaged 596M lbs/yr (2026-2032). RD feedstock demand reduced avg 184M lbs/yr (2023-2028), growing to 596M by 2031. Price forecasts lowered avg 10 cpb.", "direction": "demand_decreasing", "source": "jacobsen_q2_2022_tallow_changes"}'::jsonb,
 'extracted', 0.85);

-- RD imports reduce CWG demand
INSERT INTO core.kg_edge (source_node_id, target_node_id, edge_type, weight, properties, created_by, confidence) VALUES
((SELECT id FROM core.kg_node WHERE node_key = 'rd_import_assumption_q2_2022'),
 (SELECT id FROM core.kg_node WHERE node_key = 'choice_white_grease'),
 'CAUSES', 0.85,
 '{"mechanism": "Higher RD imports cut CWG RD demand by 266M lbs in 2022 and up to 172M lbs by 2031. Non-biofuel demand absorbs surplus. Price forecasts reduced 7-30 cpb from prior quarter. CWG supply changes modest (<100M lbs/yr). Biodiesel demand slightly raised due to supplemental mandate.", "direction": "demand_decreasing", "source": "jacobsen_q2_2022_cwg_changes"}'::jsonb,
 'extracted', 0.85);

-- Crush capacity expansion links to SBO supply
INSERT INTO core.kg_edge (source_node_id, target_node_id, edge_type, weight, properties, created_by, confidence) VALUES
((SELECT id FROM core.kg_node WHERE node_key = 'sbo_crush_capacity_model_q2_2022'),
 (SELECT id FROM core.kg_node WHERE node_key = 'soybean_oil'),
 'ENABLES', 0.90,
 '{"mechanism": "Crush capacity must expand from ~2.3B bu (Q4 2022) to 2.6B bu by 2027 to supply SBO for biofuel demand. Refining capacity must add 4.5B lbs + 3.4B lbs pretreatment. Without expansion, SBO basis remains elevated and constrains BBD growth. Soybean acreage must average 94.9M acres.", "source": "jacobsen_q2_2022_veg_oil_summary"}'::jsonb,
 'extracted', 0.90);

-- UCO/YG supply constraint drives modeling challenges
INSERT INTO core.kg_edge (source_node_id, target_node_id, edge_type, weight, properties, created_by, confidence) VALUES
((SELECT id FROM core.kg_node WHERE node_key = 'uco_yg_supply_estimation_model'),
 (SELECT id FROM core.kg_node WHERE node_key = 'used_cooking_oil'),
 'PREDICTS', 0.70,
 '{"mechanism": "UCO/YG production estimation relies on residual calculation from known demand. Production estimates cut by 1.1B lbs (2021). Non-biofuel demand goes negative in model for several years, indicating production underestimated or demand overestimated. Imports partially offset with avg +151M lbs/yr. Model flagged for potential significant revision.", "confidence_note": "Low confidence due to acknowledged model problems", "source": "jacobsen_q2_2022_yg_uco_changes"}'::jsonb,
 'extracted', 0.70);

-- DCO production constrained by ethanol output
INSERT INTO core.kg_edge (source_node_id, target_node_id, edge_type, weight, properties, created_by, confidence) VALUES
((SELECT id FROM core.kg_node WHERE node_key = 'ethanol'),
 (SELECT id FROM core.kg_node WHERE node_key = 'distillers_corn_oil'),
 'CONSTRAINS', 0.90,
 '{"mechanism": "DCO is co-product of ethanol production. Lower ethanol output forecasts reduced DCO production predictions by avg 266M lbs/yr (2022-2030). High DCO prices may justify capex for faster yield improvements, partially offsetting production cuts. Yields rising but pace uncertain.", "source": "jacobsen_q2_2022_dco_changes"}'::jsonb,
 'extracted', 0.90);

-- Link LCFS credit model to existing framework
INSERT INTO core.kg_edge (source_node_id, target_node_id, edge_type, weight, properties, created_by, confidence) VALUES
((SELECT id FROM core.kg_node WHERE node_key = 'lcfs_credit_deficit_model_q2_2022'),
 (SELECT id FROM core.kg_node WHERE node_key = 'lcfs_credit_framework'),
 'EXTENDS', 0.90,
 '{"mechanism": "Q2 2022 vintage LCFS model extends general framework with specific quarterly credit/deficit generation forecasts, credit bank trajectory (10.35M tonnes growing to 12-14M by YE 2023), CARB intervention scenarios, and biodiesel/RD California share predictions. Key finding: credit bank growth forces CARB program changes by 2024.", "source": "jacobsen_q2_2022_lcfs_credit_deficit"}'::jsonb,
 'extracted', 0.90);

-- CARB scoping impacts LCFS credit framework
INSERT INTO core.kg_edge (source_node_id, target_node_id, edge_type, weight, properties, created_by, confidence) VALUES
((SELECT id FROM core.kg_node WHERE node_key = 'carb_2022_scoping_plan'),
 (SELECT id FROM core.kg_node WHERE node_key = 'lcfs_credit_framework'),
 'MODIFIES', 0.80,
 '{"mechanism": "CARB 2022 scoping plan proposes increasing CI reduction target from 20% to 25-30% by 2030. Also considering including intrastate jet fuel (creating deficits for airlines, supporting SAF). Crop-based feedstock cap under consideration but likely limited impact -- would shift biofuel flows within US rather than reduce volumes.", "source": "jacobsen_q2_2022_executive_summary"}'::jsonb,
 'extracted', 0.80);

-- CARB scoping links to SAF
INSERT INTO core.kg_edge (source_node_id, target_node_id, edge_type, weight, properties, created_by, confidence) VALUES
((SELECT id FROM core.kg_node WHERE node_key = 'carb_2022_scoping_plan'),
 (SELECT id FROM core.kg_node WHERE node_key = 'sustainable_aviation_fuel'),
 'ENABLES', 0.75,
 '{"mechanism": "CARB staff proposal to include intrastate fossil jet fuel in LCFS would create deficits for airlines, providing economic support for SAF production. IRA also included SAF credit with higher ceiling than BBD credit. Combined policy support could accelerate SAF feedstock demand.", "source": "jacobsen_q2_2022_executive_summary"}'::jsonb,
 'extracted', 0.75);

-- Link capacity model to existing BBD margin model
INSERT INTO core.kg_edge (source_node_id, target_node_id, edge_type, weight, properties, created_by, confidence) VALUES
((SELECT id FROM core.kg_node WHERE node_key = 'bbd_capacity_expansion_q2_2022'),
 (SELECT id FROM core.kg_node WHERE node_key = 'bbd_margin_model'),
 'EXTENDS', 0.85,
 '{"mechanism": "Capacity expansion trajectory determines margin compression timing. RD capacity growth to 2.7B gal by YE 2022 pressures biodiesel margins. Fats/greases RD margin peaked at $3.60/gal (Apr 2022). SBO-based biodiesel margin peaked at $1/gal (late Q2). D4 RINs averaged $1.69/gal. LCFS credits fell from $109 to $81/MT.", "source": "jacobsen_q2_2022_executive_summary"}'::jsonb,
 'extracted', 0.85);

-- IRA CI-based credit favors fats/greases over veg oils
INSERT INTO core.kg_edge (source_node_id, target_node_id, edge_type, weight, properties, created_by, confidence) VALUES
((SELECT id FROM core.kg_node WHERE node_key = 'cfpc_45z'),
 (SELECT id FROM core.kg_node WHERE node_key = 'feedstock_supply_chain_model'),
 'CAUSES', 0.85,
 '{"mechanism": "IRA replacing BTC with CI-based credit (post-2024) favors fats/greases (lower CI) over vegetable oils. Credit excludes imports, potentially reducing 675M gal/yr of imported BBD. If national mix shifts to CA-like (SBO 50%->20%), needs +5.5B lbs additional fats/greases. Due to yield differential, ~7B lbs fats/greases needed to replace 5.5B lbs SBO. Meeting mandates without SBO is impossible. RIN prices must rise to compensate.", "vintage": "Q2 2022", "source": "jacobsen_q2_2022_executive_summary"}'::jsonb,
 'extracted', 0.85);

-- Poultry fat has minimal BBD role
INSERT INTO core.kg_edge (source_node_id, target_node_id, edge_type, weight, properties, created_by, confidence) VALUES
((SELECT id FROM core.kg_node WHERE node_key = 'poultry_fat'),
 (SELECT id FROM core.kg_node WHERE node_key = 'renewable_diesel'),
 'SUPPLIES', 0.40,
 '{"mechanism": "Poultry fat used in modest volumes in RD production. RD feedstock demand raised by <20M lbs/yr based on reports of PF use in feedstock mix. Biodiesel demand cuts (96M lbs 2022) partially offset by non-biofuel demand increases. Supply (production + imports) essentially unchanged. PF is marginal BBD feedstock.", "source": "jacobsen_q2_2022_pf_changes"}'::jsonb,
 'extracted', 0.75);

-- Fat/grease non-biofuel demand compression
INSERT INTO core.kg_edge (source_node_id, target_node_id, edge_type, weight, properties, created_by, confidence) VALUES
((SELECT id FROM core.kg_node WHERE node_key = 'renewable_diesel'),
 (SELECT id FROM core.kg_node WHERE node_key = 'feedstock_supply_chain_model'),
 'CAUSES', 0.90,
 '{"mechanism": "Growing RD feedstock demand compresses fat/grease non-biofuel use. Total fat/grease non-biofuel demand cuts averaged 1.3B lbs/yr (2023-2032) from prior quarter. UCO/YG non-biofuel cuts most severe (avg 1.1B lbs/yr). BBD feedstock demand raised avg 676M lbs/yr. Fat/grease exports also reduced avg 102M lbs/yr as domestic BBD absorbs supply.", "source": "jacobsen_q2_2022_fat_grease_changes"}'::jsonb,
 'extracted', 0.90);

-- Canola oil pathway links to RD
INSERT INTO core.kg_edge (source_node_id, target_node_id, edge_type, weight, properties, created_by, confidence) VALUES
((SELECT id FROM core.kg_node WHERE node_key = 'canola_oil_bbd_pathway'),
 (SELECT id FROM core.kg_node WHERE node_key = 'renewable_diesel'),
 'ENABLES', 0.80,
 '{"mechanism": "EPA pathway approval for canola oil in RD/SAF production (proposed Apr 2022). Canola BBD use expected to decline through 2025/26 (biodiesel rationalization) then recover to ~1.2B lbs. Food substitution growth much larger driver than BBD. Non-biofuel use to grow from 4.125B lbs to 6B+ lbs.", "source": "jacobsen_q2_2022_co_changes_and_veg_oil_summary"}'::jsonb,
 'extracted', 0.80);


-- ============================================================================
-- 3. CONTEXTS: Expert rules, risk thresholds, analytical frameworks
-- ============================================================================

-- Biodiesel Rationalization Framework
INSERT INTO core.kg_context (node_id, context_type, context_key, context_value, applicable_when, source) VALUES
((SELECT id FROM core.kg_node WHERE node_key = 'bbd_capacity_expansion_q2_2022'),
 'expert_rule', 'biodiesel_rationalization_framework',
 '{"rule": "Biodiesel capacity remains stable while (1) supplemental mandate creates blend wall shortfall requiring extra BBD production AND (2) biodiesel margins remain positive. Once either condition fails, rationalization accelerates. Integrated producers and state-level mandates set a floor on biodiesel capacity. Biodiesel capacity decline expected rapid after 2023 but not to zero.", "timing": "Stable through 2023; rapid decline starting 2024", "margin_threshold": "If margins decline substantially more than predicted, rationalization shifts forward to 2023", "acceleration_trigger": "RD capacity expansion in 2023 reduces biodiesel margins, accelerating rationalization vs 2022 pace", "floor_factors": ["Integrated producer economics", "State biodiesel mandates", "LCFS credit value for remaining plants"], "lcfs_support": "Many remaining plants will need LCFS credit value to stay profitable. Expect 65% of domestic biodiesel production shipped to California from 2024-2033.", "vintage": "Q2 2022", "source": "jacobsen_q2_2022_executive_summary"}'::jsonb,
 'always', 'extracted');

-- SBO Structural Tightness Rule
INSERT INTO core.kg_context (node_id, context_type, context_key, context_value, applicable_when, source) VALUES
((SELECT id FROM core.kg_node WHERE node_key = 'sbo_crush_capacity_model_q2_2022'),
 'expert_rule', 'sbo_structural_tightness_rule',
 '{"rule": "US SBO stocks-to-use forecast of 5.6% avg (next 10 years) is well below the 8.2% avg of prior 10 years. Any decline in US soybean yields from trendline will leave crushing industry short of supply needed for biofuel demand. Soybean acreage must average 94.9M acres, requiring ~7M additional acres sustained over 10 years, which may not be sustainable for crop rotations.", "risk_threshold": "Stocks-to-use below 5.6% signals acute SBO shortage", "yield_sensitivity": "Below-trendline yield in any year creates immediate supply deficit given tight stocks-to-use", "acreage_feasibility": "Farmers planted more soy than corn only once before in history. Sustaining 95M acres is historically unprecedented and may be ecologically unsustainable.", "trendline_yield_concern": "Reduction in trendline yield prediction reduces margin of error for any given year", "basis_implication": "Elevated SBO basis despite adequate stocks = capacity shortage signal. Relief expected at 2.3B bu crush capacity but depends on RD expansion pace.", "vintage": "Q2 2022", "source": "jacobsen_q2_2022_veg_oil_summary"}'::jsonb,
 'always', 'extracted');

-- RD Import Sensitivity Rule
INSERT INTO core.kg_context (node_id, context_type, context_key, context_value, applicable_when, source) VALUES
((SELECT id FROM core.kg_node WHERE node_key = 'rd_import_assumption_q2_2022'),
 'expert_rule', 'rd_import_sensitivity_rule',
 '{"rule": "RD import assumption is the single most impactful variable in the 10-year feedstock outlook. Higher imports reduce domestic feedstock demand by billions of pounds across ALL commodities. Lower imports (e.g., if IRA credit excludes imports and RINs do not compensate) raise domestic demand dramatically. The BTC-to-IRA transition creates maximum uncertainty on import volumes.", "sensitivity": {"sbo_demand_swing_2032_blbs": 8.8, "tallow_demand_swing_2032_blbs": 1.1, "veg_oil_total_demand_swing_2032_blbs": 8.4}, "key_uncertainty": "If RINs dont rise enough to offset loss of BTC for importers, US imports could be significantly lower than predicted -> domestic feedstock demand higher than forecast", "cascading_effect": "Change flows through: imports -> domestic production needs -> feedstock demand -> non-biofuel demand -> inventories -> prices for every feedstock", "vintage": "Q2 2022", "source": "jacobsen_q2_2022_multiple_documents"}'::jsonb,
 'always', 'extracted');

-- UCO/YG Data Quality Warning
INSERT INTO core.kg_context (node_id, context_type, context_key, context_value, applicable_when, source) VALUES
((SELECT id FROM core.kg_node WHERE node_key = 'uco_yg_supply_estimation_model'),
 'data_quality_warning', 'uco_yg_production_estimation_uncertainty',
 '{"rule": "UCO/YG production estimates have no reliable official data source. Residual calculation methodology causes non-biofuel demand to go negative for several years when fitting both historical data and projected RD demand growth. Production may be substantially underestimated. Historical assessment cut by 1.1B lbs (2021 alone). Model may undergo significant revision in subsequent quarters.", "implication": "Any analysis relying on UCO/YG supply projections carries high uncertainty. Cross-check with trade data and LCFS-reported feedstock volumes.", "severity": "high", "vintage": "Q2 2022", "source": "jacobsen_q2_2022_yg_uco_changes"}'::jsonb,
 'always', 'extracted');

-- LCFS Credit Bank as Program Change Trigger
INSERT INTO core.kg_context (node_id, context_type, context_key, context_value, applicable_when, source) VALUES
((SELECT id FROM core.kg_node WHERE node_key = 'lcfs_credit_deficit_model_q2_2022'),
 'expert_rule', 'lcfs_credit_bank_intervention_trigger',
 '{"rule": "Cumulative LCFS credit bank growth above 14M tonnes triggers high probability of CARB program intervention. Without intervention, bank could reach 20M tonnes by end 2024. CARB changes could push bank below 10M by YE 2024 and keep it there through 2030. Credit prices will remain under pressure until bank declines or CARB intervenes.", "trigger_threshold_mt": 14000000, "intervention_timeline": "2024 most likely", "without_intervention": "Bank does not fall below 10M tonnes until 2027 from natural CI target tightening", "price_impact": "Credits could continue declining from $121 (Mar 2022) through 2023 without intervention. Wide forecast range in 2024 due to divergent scenarios for CARB action.", "rd_producer_response": "Producers will reduce CA-bound volumes as credit prices decline. Q2-Q3 2022 data provides crucial insight into this price response.", "vintage": "Q2 2022", "source": "jacobsen_q2_2022_lcfs_credit_deficit"}'::jsonb,
 'always', 'extracted');

-- DCO Yield Improvement Acceleration Rule
INSERT INTO core.kg_context (node_id, context_type, context_key, context_value, applicable_when, source) VALUES
((SELECT id FROM core.kg_node WHERE node_key = 'distillers_corn_oil'),
 'expert_rule', 'dco_yield_improvement_acceleration',
 '{"rule": "DCO production is constrained by ethanol output, but historically high DCO prices may justify capex for faster yield improvements than trend. If ethanol demand disappoints, producers are motivated to maintain revenue through higher extraction yields. This creates upside risk to DCO production forecasts even if ethanol output declines.", "base_case": "Yield improvements built into forecast at trend pace", "upside_scenario": "Faster yield improvement if DCO prices remain elevated", "price_sensitivity": "Production forecast cuts of avg 266M lbs/yr (2022-2030) from lower ethanol projections, but 2031-2032 rise by 267-303M lbs due to yield increases", "biodiesel_demand_cut_avg_mlbs": 135, "rd_demand_2023_2029_cut_avg_mlbs": 203, "rd_demand_2030_2032_raised_avg_mlbs": 315, "vintage": "Q2 2022", "source": "jacobsen_q2_2022_dco_changes"}'::jsonb,
 'always', 'extracted');

-- Fat/Grease Non-Biofuel Demand Compression Pattern
INSERT INTO core.kg_context (node_id, context_type, context_key, context_value, applicable_when, source) VALUES
((SELECT id FROM core.kg_node WHERE node_key = 'feedstock_supply_chain_model'),
 'expert_rule', 'fats_greases_nonbiofuel_demand_compression',
 '{"rule": "Growing RD feedstock demand systematically compresses fat/grease non-biofuel use. The Q2 2022 forecast cuts non-biofuel demand by avg 1.3B lbs/yr across the forecast period. UCO/YG bear the largest share (avg 1.1B lbs/yr cuts). Traditional industrial uses of fats/greases (soap, oleochemicals, animal feed) are being displaced by biofuel premiums. Once biofuel demand exceeds available supply, non-biofuel industries where fats/greases are a small percentage of cost will bid up prices above biofuel breakeven -- RINs must then compensate.", "compression_quantification": {"total_fat_grease_nonbiofuel_cut_avg_blbs": 1.3, "uco_yg_nonbiofuel_cut_avg_blbs": 1.1, "tallow_nonbiofuel_cut_avg_mlbs": 596, "cwg_change_modest": true}, "offset": "BBD feedstock demand raised avg 676M lbs/yr. Exports also reduced avg 102M lbs/yr as domestic demand absorbs supply.", "ceiling_mechanism": "Non-biofuel industries where fats/greases are small % of input cost will outbid biofuel at some price level -> RINs must rise to maintain biofuel economics", "vintage": "Q2 2022", "source": "jacobsen_q2_2022_fat_grease_changes"}'::jsonb,
 'always', 'extracted');

-- Pretreatment Economics Rule
INSERT INTO core.kg_context (node_id, context_type, context_key, context_value, applicable_when, source) VALUES
((SELECT id FROM core.kg_node WHERE node_key = 'sbo_crush_capacity_model_q2_2022'),
 'expert_rule', 'pretreatment_economics_reversion_risk',
 '{"rule": "While historically high SBO refining margins justify pretreatment capex, increasing refining and pretreatment capacity will pressure refining margins back toward long-term average. If margins revert enough, future pretreatment additions become uneconomic. Facilities not yet online may cancel pretreatment plans if refining margin drops to long-term average.", "current_dynamics": "90% of planned RD capacity includes pretreatment. 3.4B lbs of pretreatment expansion predicted by 2027.", "reversion_risk": "Margin reversion from historically high levels could deter future additions", "key_distinction": "Pretreatment processes ALL feedstock types (not just SBO). A pound of pretreatment expansion has less impact on SBO/veg oil basis than a pound of SBO refining expansion.", "caveat": "3.4B lbs not probability-adjusted for individual plants opening", "vintage": "Q2 2022", "source": "jacobsen_q2_2022_veg_oil_summary"}'::jsonb,
 'always', 'extracted');

-- SBO Import Competitiveness (Argentina)
INSERT INTO core.kg_context (node_id, context_type, context_key, context_value, applicable_when, source) VALUES
((SELECT id FROM core.kg_node WHERE node_key = 'soybean_oil'),
 'expert_rule', 'sbo_import_competitiveness_argentina',
 '{"rule": "Argentine SBO imports are close to competitive with US Gulf prices even including the 19.1% tariff. However, substantial import volumes unlikely through end of 2022. Relative prices could remain close until US crush/refining capacity expands enough to cut US basis relative to exporting countries. Long-term SBO import projections substantially reduced from prior quarter.", "tariff_pct": 19.1, "competitive_condition": "Argentine basis discount to CBOT must exceed tariff + freight to make imports viable", "long_term": "Import projections cut substantially in 2022-2023 and 2030-2032 due to higher RD import assumption reducing domestic feedstock demand", "btc_link": "If IRA credit excludes imported biofuels, SBO import demand also falls since imported SBO-based biofuel loses economic viability", "vintage": "Q2 2022", "source": "jacobsen_q2_2022_sbo_changes"}'::jsonb,
 'always', 'extracted');

-- RBD SBO Basis as Capacity Indicator
INSERT INTO core.kg_context (node_id, context_type, context_key, context_value, applicable_when, source) VALUES
((SELECT id FROM core.kg_node WHERE node_key = 'soybean_oil'),
 'market_indicator', 'rbd_sbo_basis_capacity_signal',
 '{"rule": "RBD SBO basis and refining margin serve as real-time indicators of crush/refining capacity adequacy. Elevated basis despite adequate stocks = capacity shortage. Q2 2022: RBD SBO basis rose 3.5 cpb on equal refining margin gain. LA canola basis rose 7 cpb vs SBO futures. Gulf basis dropped 3 cpb but recovered in Q3.", "q2_2022_prices": {"rbd_sbo_chicago_record_cpb": 109.6, "rbd_canola_la_peak_cpb": 121.6, "rbd_sbo_q2_end_cpb": 91.43, "rbd_canola_q2_end_cpb": 104.93, "crude_degummed_change": "unchanged"}, "basis_signal": "Rising RBD basis with flat/falling crude SBO = capacity squeeze at refining stage. Relief expected at 2.3B bu crush capacity (Q4 2022).", "vintage": "Q2 2022", "source": "jacobsen_q2_2022_veg_oil_summary"}'::jsonb,
 'always', 'extracted');

-- Poultry Fat: Marginal BBD Feedstock
INSERT INTO core.kg_context (node_id, context_type, context_key, context_value, applicable_when, source) VALUES
((SELECT id FROM core.kg_node WHERE node_key = 'poultry_fat'),
 'expert_rule', 'poultry_fat_marginal_bbd_feedstock',
 '{"rule": "Poultry fat is a marginal BBD feedstock. RD demand raised by <20M lbs/yr. Main dynamic is shift between biodiesel use (declining due to rationalization) and non-biofuel use (offsetting increase). Supply (production + imports) essentially unchanged across forecast. Price follows general feedstock price trends with avg 12 cpb decline (2027-2032) from prior quarter.", "biodiesel_demand_cut_2022_mlbs": 96, "rd_demand_increase_per_year_mlbs": 20, "non_biofuel_offset_pattern": "Biodiesel cuts -> non-biofuel increases, roughly offsetting", "inventory_impact": "Modestly lower stocks, declining 26M lbs cumulative by 2031", "price_changes_cpb": {"2022": 2, "2023": 6, "2024_2026_avg": -4, "2027_2032_avg": -12}, "vintage": "Q2 2022", "source": "jacobsen_q2_2022_pf_changes"}'::jsonb,
 'always', 'extracted');

-- BBD Feedstock Supply-Demand Summary
INSERT INTO core.kg_context (node_id, context_type, context_key, context_value, applicable_when, source) VALUES
((SELECT id FROM core.kg_node WHERE node_key = 'bbd_balance_sheet_model'),
 'expert_rule', 'bbd_feedstock_supply_demand_q2_2022',
 '{"rule": "Total BBD feedstock production forecasts declined 2022-2027 before rising modestly. Import forecasts cut in every year (avg 1.8B lbs/yr, ranging to 6B lbs in 2032). Total supply cuts averaged 1.6B lbs (up to 5.1B in 2032). Lower RD import assumption would reverse all these cuts. Demand declines from prior quarter offset supply reductions, raising inventory in most years (avg +424M lbs).", "production_changes": {"2022_cut_blbs": 1.5, "2027_cut_mlbs": 119, "2028_2032_raised_avg_mlbs": 302}, "import_changes": {"avg_cut_blbs": 1.8, "min_cut_mlbs": 359, "max_cut_blbs": 6.0}, "demand_components": {"biodiesel_rd_raised_first_4_years": true, "non_biofuel_raised_second_half": true, "rd_demand_largest_changes": true, "exports_most_commonly_cut": true}, "key_uncertainty": "Substantial uncertainty about imports post-2024 BTC expiration. RIN prices must rise enough to offset BTC loss for importers or imports drop significantly below forecast.", "vintage": "Q2 2022", "source": "jacobsen_q2_2022_bbd_feedstock_changes"}'::jsonb,
 'always', 'extracted');


-- ============================================================================
-- 4. SOURCE REGISTRY: Register all processed documents
-- ============================================================================

INSERT INTO core.kg_source (
    source_key, source_type, title, location_uri, document_date, document_type,
    commodities, topics, status, first_processed, last_processed,
    nodes_extracted, edges_extracted, contexts_extracted
) VALUES
('jacobsen_q2_2022_executive_summary', 'local_file',
 'Q2 2022 Long-Term Executive Summary',
 'C:/Users/torem/RLC Dropbox/Tore Alden/Jacobsen/Quarterly Outlook Report/Q2 2022/Q2 2022 Long-Term Executive Summary.docx',
 '2022-09-30', 'quarterly_outlook',
 '{biodiesel,renewable_diesel,soybean_oil,tallow,uco,cwg,dco,poultry_fat,ethanol}',
 '{rd_capacity_expansion,biodiesel_rationalization,ethanol_blend_wall,rvo_mandates,sre_denial,ira_btc_transition,carb_scoping,food_vs_fuel,lcfs_credit_decline,bbd_margins,d4_rins,feedstock_competition}',
 'completed', NOW(), NOW(), 5, 6, 1)
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
('jacobsen_q2_2022_cwg_changes', 'local_file',
 'Q2 2022 Long Term CWG Changes - Checked',
 'C:/Users/torem/RLC Dropbox/Tore Alden/Jacobsen/Quarterly Outlook Report/Q2 2022/Q2 2022 Long Term CWG Changes - Checked.docx',
 '2022-09-30', 'quarterly_outlook',
 '{choice_white_grease}',
 '{cwg_balance_sheet,rd_demand,biodiesel_demand,non_biofuel_demand,cwg_price_forecast,cwg_production,cwg_imports}',
 'completed', NOW(), NOW(), 0, 1, 0)
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
('jacobsen_q2_2022_tallow_changes', 'local_file',
 'Q2 2022 Long Term Tallow Changes - Checked',
 'C:/Users/torem/RLC Dropbox/Tore Alden/Jacobsen/Quarterly Outlook Report/Q2 2022/Q2 2022 Long Term Tallow Changes - Checked.docx',
 '2022-09-30', 'quarterly_outlook',
 '{tallow}',
 '{tallow_balance_sheet,rd_imports,tallow_demand,non_biofuel_demand,biodiesel_demand,tallow_price_forecast}',
 'completed', NOW(), NOW(), 0, 1, 0)
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
('jacobsen_q2_2022_fat_grease_changes', 'local_file',
 'Q2 2022 Long-Term Fat and Grease Changes - Checked',
 'C:/Users/torem/RLC Dropbox/Tore Alden/Jacobsen/Quarterly Outlook Report/Q2 2022/Q2 2022 Long-Term Fat and Grease Changes - Checked.docx',
 '2022-09-30', 'quarterly_outlook',
 '{tallow,cwg,uco,yellow_grease,poultry_fat}',
 '{fat_grease_aggregate,non_biofuel_compression,bbd_feedstock_demand,rd_plant_delays,export_forecast}',
 'completed', NOW(), NOW(), 0, 1, 1)
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
('jacobsen_q2_2022_veg_oil_changes', 'local_file',
 'Q2 2022 Long-Term Veg Oil Changes - Checked',
 'C:/Users/torem/RLC Dropbox/Tore Alden/Jacobsen/Quarterly Outlook Report/Q2 2022/Q2 2022 Long-Term Veg Oil Changes - Checked.docx',
 '2022-09-30', 'quarterly_outlook',
 '{soybean_oil,canola_oil}',
 '{veg_oil_aggregate,rd_import_impact,veg_oil_demand,non_biofuel_recovery,export_forecast,veg_oil_prices}',
 'completed', NOW(), NOW(), 0, 1, 0)
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
('jacobsen_q2_2022_co_changes', 'local_file',
 'Q2 2022 Long-Term CO (Canola Oil) Changes - Checked',
 'C:/Users/torem/RLC Dropbox/Tore Alden/Jacobsen/Quarterly Outlook Report/Q2 2022/Q2 2022 Long-Term CO Changes - Checked.docx',
 '2022-09-30', 'quarterly_outlook',
 '{canola_oil}',
 '{canola_balance_sheet,canola_imports,canola_non_biofuel,canola_stocks,canola_price_forecast,sbo_substitution}',
 'completed', NOW(), NOW(), 1, 1, 0)
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
('jacobsen_q2_2022_sbo_changes', 'local_file',
 'Q2 2022 Long-Term SBO Changes - Checked',
 'C:/Users/torem/RLC Dropbox/Tore Alden/Jacobsen/Quarterly Outlook Report/Q2 2022/Q2 2022 Long-Term SBO Changes - Checked.docx',
 '2022-09-30', 'quarterly_outlook',
 '{soybean_oil}',
 '{sbo_balance_sheet,sbo_imports,argentina_sbo,btc_impact,sbo_bbd_demand,sbo_non_biofuel,sbo_exports,sbo_price_forecast}',
 'completed', NOW(), NOW(), 0, 1, 1)
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
('jacobsen_q2_2022_dco_changes', 'local_file',
 'Q2 2022 Long-term DCO Changes - Checked',
 'C:/Users/torem/RLC Dropbox/Tore Alden/Jacobsen/Quarterly Outlook Report/Q2 2022/Q2 2022 Long-term DCO Changes - Checked.docx',
 '2022-09-30', 'quarterly_outlook',
 '{distillers_corn_oil}',
 '{dco_balance_sheet,dco_production,ethanol_linkage,dco_yield,dco_bbd_demand,dco_price_forecast}',
 'completed', NOW(), NOW(), 0, 1, 1)
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
('jacobsen_q2_2022_yg_uco_changes', 'local_file',
 'Q2 2022 Long-Term YG-UCO Changes - Checked',
 'C:/Users/torem/RLC Dropbox/Tore Alden/Jacobsen/Quarterly Outlook Report/Q2 2022/Q2 2022 Long-Term YG-UCO Changes - Checked.docx',
 '2022-09-30', 'quarterly_outlook',
 '{used_cooking_oil,yellow_grease}',
 '{uco_yg_balance_sheet,uco_production_estimation,yg_production,uco_imports,rd_feedstock_demand,non_biofuel_compression,uco_yg_prices}',
 'completed', NOW(), NOW(), 1, 1, 1)
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
('jacobsen_q2_2022_pf_changes', 'local_file',
 'Q2 2022 Long Term PF Changes - Checked',
 'C:/Users/torem/RLC Dropbox/Tore Alden/Jacobsen/Quarterly Outlook Report/Q2 2022/Q2 2022 Long Term PF Changes - Checked.docx',
 '2022-09-30', 'quarterly_outlook',
 '{poultry_fat}',
 '{pf_balance_sheet,pf_biodiesel,pf_rd_demand,pf_non_biofuel,pf_price_forecast}',
 'completed', NOW(), NOW(), 0, 1, 1)
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
('jacobsen_q2_2022_bbd_feedstock_changes', 'local_file',
 'Q2 2022 Long-Term BBD Feedstock Changes - Checked',
 'C:/Users/torem/RLC Dropbox/Tore Alden/Jacobsen/Quarterly Outlook Report/Q2 2022/Q2 2022 Long-Term BBD Feedstock Changes - Checked.docx',
 '2022-09-30', 'quarterly_outlook',
 '{soybean_oil,tallow,uco,cwg,dco,canola_oil,poultry_fat}',
 '{bbd_feedstock_aggregate,feedstock_production,feedstock_imports,feedstock_demand,rd_imports_uncertainty,btc_expiration}',
 'completed', NOW(), NOW(), 0, 0, 1)
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
('jacobsen_q2_2022_veg_oil_summary', 'local_file',
 'Q2 2022 Long-Term Veg Oil Summary',
 'C:/Users/torem/RLC Dropbox/Tore Alden/Jacobsen/Quarterly Outlook Report/Q2 2022/Q2 2022 Long-Term Veg Oil Summary.docx',
 '2022-09-30', 'quarterly_outlook',
 '{soybean_oil,canola_oil}',
 '{sbo_basis,rbd_refining,crush_capacity,pretreatment,soybean_acreage,stocks_to_use,canola_pathway,food_substitution}',
 'completed', NOW(), NOW(), 1, 1, 3)
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
('jacobsen_q2_2022_ci_scores_summary', 'local_file',
 'Q2 2022 Long-Term CI Scores Fuel and Feedstock Summary',
 'C:/Users/torem/RLC Dropbox/Tore Alden/Jacobsen/Quarterly Outlook Report/Q2 2022/Q2 2022 Long-Term CI Scores Fuel and Feedstock Summary.docx',
 '2022-09-30', 'quarterly_outlook',
 '{biodiesel,renewable_diesel,ethanol}',
 '{ci_scores,lcfs_feedstock_mix,biodiesel_lcfs_share,rd_lcfs_share,dco_uco_tallow_mix,sbo_canola_mix,alt_jet_fuel}',
 'completed', NOW(), NOW(), 2, 0, 0)
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
('jacobsen_q2_2022_lcfs_credit_deficit', 'local_file',
 'Q2 2022 Long Term LCFS Credit Deficit Generation',
 'C:/Users/torem/RLC Dropbox/Tore Alden/Jacobsen/Quarterly Outlook Report/Q2 2022/Q2 2022 Long Term LCFS Credit Deficit Generation.docx',
 '2022-09-30', 'quarterly_outlook',
 '{renewable_diesel,biodiesel,ethanol}',
 '{lcfs_credits,lcfs_deficits,credit_bank,ev_credits,bio_cng,carb_intervention,lcfs_price_forecast,rd_ca_shipments}',
 'completed', NOW(), NOW(), 1, 1, 1)
ON CONFLICT (source_key) DO UPDATE SET
    status = 'completed', last_processed = NOW(),
    nodes_extracted = EXCLUDED.nodes_extracted,
    edges_extracted = EXCLUDED.edges_extracted,
    contexts_extracted = EXCLUDED.contexts_extracted;


-- ============================================================================
-- BATCH STATISTICS
-- ============================================================================
-- Nodes:    11 (5 models, 2 data_series, 2 policies, 1 model/data hybrid, 1 model)
-- Edges:    16 (CAUSES, ENABLES, CONSTRAINS, SUPPLIES, EXTENDS, MODIFIES, PREDICTS)
-- Contexts: 11 (expert_rules, data_quality_warning, market_indicator)
-- Sources:  14 (all 14 analyzed documents registered)
-- Links to existing KG: soybean_oil, tallow, choice_white_grease, used_cooking_oil,
--   renewable_diesel, sustainable_aviation_fuel, lcfs_credit_framework, bbd_balance_sheet_model,
--   bbd_margin_model, feedstock_supply_chain_model, cfpc_45z, distillers_corn_oil, ethanol,
--   poultry_fat, canola_oil
