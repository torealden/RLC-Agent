-- ============================================================================
-- RLC Knowledge Graph: Extraction Batch 017 (Fastmarkets Fats, Fuels & Feedstocks Archive)
-- Source: C:/Users/torem/RLC Dropbox/Tore Alden/Old Commodity Analysis/FM Files/
-- Extracted: 2026-04-16
-- Scope:  Quarterly outlook reports (Q2 2023, Q4 2023), consulting deliverables
--         (Motiva SAF/feedstock study, Buckeye ethanol/AtJ study), commodity
--         commentary (BFT, SBO, biodiesel, canola, palm oil, corn oil), SAF
--         pathway analysis, regulatory frameworks, BBD margin/risk frameworks,
--         crush capacity expansion, RVO hidden-gem analysis, RD-to-SAF conversion
-- Documents Processed: 25 (of ~620 total files)
-- ============================================================================

-- KEY FINDINGS:
--   * BBD production CAGR 14% since 2012; 2023 record 4.6B gal (30% YoY growth)
--   * RD capacity surpassed BD for first time Jan 2023 at 3B gal/yr
--   * Combined BBD capacity 6.59B gal end-2023 vs 4.6B production = <70% utilization
--   * Fat/grease imports doubled to 3.13B lbs in 2023 (~20% of 15.8B lb supply)
--   * UCO imports nearly doubled to 350M lbs/month late 2023 (from 180M in early 2023)
--   * Tallow yield per lb beef: 12-16.1% range, highly variable, drives supply forecast
--   * BD capacity rationalization threshold: ~70% industry utilization triggers closures
--   * BTC-to-IRA transition: ~$0.50/gal credit loss, would bankrupt BD sector
--   * HEFA SAF yield ~85-90%, cost $0.90-$1.40/liter; AtJ cost $1.60-$2.50/liter
--   * AtJ requires 1.7 gal ethanol per gal SAF (0.6-0.7 gal SAF per gal ethanol)
--   * RVO hidden gems: BBD fills 1.7B RIN gap (2023) from ethanol/cellulosic shortfalls
--   * Canola oil share of BBD feedstock: 11% Q2 2022 -> 19% Q2 2023 (fastest growth)
--   * California consumed 52% of US BBD in Q2 2023
--   * Pretreatment margin: poultry fat 13 cpb > UCO 12 cpb (high-FFA advantage)
--   * Saskatchewan canola crush expanding from 4.1 to 10.3 MMT by 2026
--   * ReFuelEU: 2% SAF by 2025, 6% by 2030, 20% by 2035, 70% by 2050
--   * US ethanol market: $32.76B (2025) -> $60.66B (2032), 9.2% CAGR
--   * Chicago ethanol benchmark: 1.2M barrel storage, ~$1.82/gal (Apr 2025)
--   * Regional ethanol premiums: Gulf Coast $0.10-$0.15/gal, West Coast $0.15-$0.25/gal


-- ============================================================================
-- 1. NODES: New analytical models, market concepts, data frameworks
-- ============================================================================

-- BBD Industry Capacity Utilization Model
INSERT INTO core.kg_node (node_type, node_key, label, properties) VALUES
('analytical_model', 'bbd_capacity_utilization_model', 'BBD Industry Capacity Utilization Model',
 '{"description": "Framework for tracking combined biodiesel + renewable diesel capacity utilization vs mandates and production. Key threshold: when combined capacity utilization falls below ~70%, biodiesel capacity rationalization begins.", "capacity_2023_end": {"renewable_diesel_bgal": 4.08, "biodiesel_bgal": 2.08, "combined_bgal": 6.59}, "production_2023": {"renewable_diesel_bgal": 2.43, "biodiesel_bgal": 1.67, "combined_bgal": 4.6}, "utilization_2023_pct": 69.8, "cagr_since_2012_pct": 14, "integrated_bd_swing_capacity_bgal": 0.65, "adjusted_utilization_pct": 78, "key_insight": "Integrated biodiesel producers (650M gal capacity) act as swing producers -- sell SBO directly when refining margins exceed BD margins. Removing swing capacity raises effective utilization to 78%. Seasonal pattern: Q1 lowest, Q2 up 20%, Q3 flat, Q4 up 7% to peak.", "mandate_implied_production_2023_bgal": 5.26, "mandate_gap_note": "4.6B EPA mandate + 650M undifferentiated advanced + ethanol gap = 5.26B needed, or 95% of capacity", "source": "fm_quarterly_q2_2023"}'::jsonb)
ON CONFLICT (node_key) DO UPDATE SET properties = EXCLUDED.properties;

-- BBD Margin Architecture Model (California)
INSERT INTO core.kg_node (node_type, node_key, label, properties) VALUES
('analytical_model', 'bbd_ca_margin_model', 'BBD California Margin Architecture',
 '{"description": "Framework for Fastmarkets stylized California BBD margin calculation. Tracks revenue components (fuel price + RIN + LCFS + BTC/IRA) against feedstock cost mix.", "biodiesel_feedstock_mix": {"soybean_oil_pct": 75, "yellow_grease_pct": 25}, "renewable_diesel_feedstock_mix": {"tallow_pct": 50, "uco_pct": 25, "dco_pct": 25}, "margin_history_2022": {"rd_avg_cpg": 354, "bd_avg_cpg": 351, "avg_feedstock_mix_cplb": 72.1}, "margin_history_2023": {"rd_avg_cpg": 277, "bd_avg_cpg": 280, "avg_feedstock_mix_cplb": 56.3, "avg_ca_margin_cpg": 124}, "revenue_components": {"rd_fuel_price": true, "d4_rin": true, "lcfs_credit": true, "btc_or_ira": true, "cap_and_trade": true}, "margin_trend": "From 2022 peaks ($3.26/gal RD May 2022) to sub-$1.25 avg in 2023. BD margins collapsed from record $1.31/gal (Jan 2023) to loss of $0.39 (Jun 2023). Credit revenue decline is primary driver.", "rin_feedstock_correlation": "RIN-feedstock cost correlation may be breaking due to capacity expansion above mandated volumes. RINs were designed as margin insurance but excess capacity dilutes this function.", "source": "fm_quarterly_q2_q4_2023"}'::jsonb)
ON CONFLICT (node_key) DO UPDATE SET properties = EXCLUDED.properties;

-- Tallow Yield Model
INSERT INTO core.kg_node (node_type, node_key, label, properties) VALUES
('analytical_model', 'tallow_yield_model', 'Tallow Yield per Pound of Beef Production Model',
 '{"description": "Framework for predicting tallow production from slaughter/beef production data. Tallow yield per lb of commercial beef production is highly variable (12-16.1%) and is the key swing factor for supply forecasts.", "yield_history": {"q1_2023_avg_pct": 15.5, "q2_2023_avg_pct": 16.1, "june_2023_low_pct": 12.0, "aug_2023_pct": 13.2, "forecast_2024_avg_pct": 15.1}, "production_2023_est_blbs": 4.03, "slaughter_trend": "US cattle slaughter down 2.8% from 2021/22 peak of 34.3M head. Cycle will keep slaughter below peak for at least 2 more years.", "beef_production_trend": "Commercial beef production down 3.5% through 11 months of 2022/23.", "key_insight": "Despite declining slaughter, tallow production rose 4.2% in 2022/23 due to yield explosion in Q1-Q2. If yields revert to <13%, consecutive-year production increase streak ends. Yield recovery pattern: drops to 12%, recovers for few months, may fall again.", "supply_response_hierarchy": ["1. Increase imports (primary price driver)", "2. Shift demand from non-biofuel end users", "3. Reduce feedstock use (least likely due to low CI score)"], "source": "fm_bft_commentary_oct_2023"}'::jsonb)
ON CONFLICT (node_key) DO UPDATE SET properties = EXCLUDED.properties;

-- US Fat and Grease Import Transformation Model
INSERT INTO core.kg_node (node_type, node_key, label, properties) VALUES
('analytical_model', 'us_fat_grease_import_model', 'US Fat/Grease Import Transformation Model',
 '{"description": "Tracks the structural transformation of the US from net fat/grease exporter to net importer, driven by BBD feedstock demand. Critical for understanding feedstock price dynamics and supply chain dependency.", "net_trade_history": {"2020": {"imports_blbs": 0.737, "exports_blbs": 2.58, "net_exports_blbs": 1.84}, "2022": {"imports_blbs": null, "exports_blbs": null, "net_imports_blbs": 1.12, "note": "First year US had net imports"}, "2023": {"imports_blbs": 3.13, "imports_pct_of_supply": 20, "exports_blbs": null}}, "import_cagr_since_2012_pct": 24, "export_decline_cagr_pct": -9, "uco_surge": {"early_2023_mlbs_per_month": 180, "late_2023_mlbs_per_month": 350, "driver": "Chinese shipments shifted from European to US market", "traceability_concerns": "Several ships turned away early 2023 over traceability uncertainty. New traceability startups alleviating concerns."}, "forecast": {"import_cagr_next_5yr_pct": 2, "export_decline_cagr_next_5yr_pct": -1, "note": "Import growth slowing substantially from 24% historical CAGR"}, "tallow_sourcing_shift": "Canada share of tallow imports: 100% in 2014/15, 44% in 2022, 27% in Q2 2023. Diversifying to Brazil, Australia, New Zealand.", "source": "fm_quarterly_q2_q4_2023"}'::jsonb)
ON CONFLICT (node_key) DO UPDATE SET properties = EXCLUDED.properties;

-- RVO Hidden Gem Analysis (BBD as RFS Fixer)
INSERT INTO core.kg_node (node_type, node_key, label, properties) VALUES
('analytical_model', 'rvo_hidden_gem_model', 'RVO Hidden Gem Analysis: BBD as RFS Gap Filler',
 '{"description": "Framework showing how BBD fills multiple unfulfilled RFS volume gaps beyond its stated mandate, making the effective BBD requirement substantially higher than the headline number.", "gaps_filled_by_bbd": {"ethanol_shortfall": {"2023_mgal": 1155, "mechanism": "Implied ethanol mandate 15B gal but projected consumption 13.845B gal"}, "advanced_mandate_gap": {"2023_mgal": 298, "mechanism": "Advanced mandate minus BBD minus cellulosic minus other advanced production"}, "supplemental_2016_remand": {"2023_mgal": 250, "mechanism": "Final installment of 2016 remand supplemental standard"}}, "total_additional_rins": {"2023": 1703, "2024": 1341, "2025": 1520}, "additional_bbd_gallons": {"2023": 1064, "2024": 838, "2025": 951}, "headline_bbd_rvo_2023_mgal": 2820, "effective_bbd_requirement_2023_mgal": 4523, "key_insight": "Market initially tanked on low headline BBD RVO but missed that BBD is the programs fixer -- covers ethanol, cellulosic, and remand shortfalls. Those who processed the detail quickly bought RINs and feedstock at a discount.", "source": "fm_bob_rvo_analysis_q3_2023"}'::jsonb)
ON CONFLICT (node_key) DO UPDATE SET properties = EXCLUDED.properties;

-- Feedstock Mix Shift Tracker
INSERT INTO core.kg_node (node_type, node_key, label, properties) VALUES
('data_series', 'us_bbd_feedstock_mix', 'US BBD Feedstock Mix Composition Tracker',
 '{"description": "Tracks quarterly composition of US BBD feedstock mix by commodity. Key for understanding competitive dynamics and price linkages.", "q2_2023": {"total_mlbs": 3797, "soybean_oil_pct": 49, "canola_oil_pct": 19, "dco_pct": 8, "tallow_pct": null, "yellow_grease_pct": null, "uco_pct": null, "fats_greases_total_pct": 24, "vegoils_total_pct": 76}, "q2_2022": {"soybean_oil_pct": 53, "canola_oil_pct": 11, "dco_pct": 15, "fats_greases_total_pct": 21, "vegoils_total_pct": 78}, "key_shifts_q2_2022_to_q2_2023": {"canola_oil": "+8 ppt (11->19%, fastest growing feedstock)", "dco": "-7 ppt (15->8%, largest share loss)", "soybean_oil": "-4 ppt (53->49%)"}, "ca_market_share_of_bbd": {"q2_2023_pct": 52.3, "2022_avg_pct": 48}, "canola_import_growth": "Canola oil imports forecast to grow from 4.37B lbs (MY 2021/22) to 5.95B lbs (2022/23) to supply BBD demand surge.", "sbo_biofuel_demand_2023_blbs": 13.93, "sbo_biofuel_demand_cagr_pct": 5, "source": "fm_quarterly_q2_2023"}'::jsonb)
ON CONFLICT (node_key) DO UPDATE SET properties = EXCLUDED.properties;

-- LCFS Credit Bank Dynamics
INSERT INTO core.kg_node (node_type, node_key, label, properties) VALUES
('analytical_model', 'lcfs_credit_bank_model', 'LCFS Credit Bank and Price Dynamics Model',
 '{"description": "Framework for understanding LCFS credit price decline from $200+/tonne (early 2021) to $56-85/tonne (2023). Credit bank overhang suppresses prices even as CI targets tighten.", "price_trajectory": {"early_2021_high": 200, "end_2021": 150, "end_2022": 70, "2023_range": [55, 85], "late_dec_2023": 56, "note": "Lowest since July 2016"}, "decline_driver": "RD capacity expansion producing credits faster than CI schedule generates demand. BBD producers ship to CA until credit value/gal = marginal production + shipping cost. As capacity rises, credit value falls.", "carb_policy_options": ["Accelerate CI reduction from 20% to 25-30% by 2030", "Cap lipid-based feedstock volume (like EU 7% crop-based cap)", "Reduce benchmark CI"], "credit_bank_buffer": "Large existing credit bank provides time buffer for new policies. Drawdown pace depends on individual obligated party balances.", "ev_credit_interaction": "EV adoption generates LCFS credits, adding to bank. Increasing EV penetration structurally bearish for LCFS credit prices unless CI targets dramatically tightened.", "source": "fm_quarterly_q2_q4_2023"}'::jsonb)
ON CONFLICT (node_key) DO UPDATE SET properties = EXCLUDED.properties;

-- SAF Pathway Comparison Model
INSERT INTO core.kg_node (node_type, node_key, label, properties) VALUES
('analytical_model', 'saf_pathway_comparison', 'SAF Production Pathway Comparison Model',
 '{"description": "Comparative framework for the three major SAF production pathways: HEFA, AtJ, and Syngas-FT. HEFA dominates currently, AtJ emerging, FT long-term.", "hefa": {"maturity": "Commercial (ASTM certified 2011)", "fuel_yield_pct": "85-90", "production_cost_usd_per_liter_2020": [0.90, 1.40], "blend_limit_pct": 50, "feedstock_constraint": "Limited waste oil/fat supply", "strength": "Existing refinery infrastructure, lowest cost", "weakness": "Feedstock-limited long term, competes with RD"}, "atj": {"maturity": "Early commercial", "production_cost_usd_per_liter": [1.60, 2.50], "ethanol_per_gal_saf": 1.7, "saf_yield_per_gal_ethanol": [0.6, 0.7], "key_players": ["Gevo", "LanzaJet", "Swedish Biofuels"], "strength": "Wide feedstock base via fermentation, potential carbon-negative with CCS", "weakness": "High cost, competes for ethanol supply"}, "syngas_ft": {"maturity": "Demonstration scale", "feedstock": "Lignocellulosic biomass, MSW", "strength": "Broadest feedstock flexibility, high-quality fuel", "weakness": "Highest capex, most complex"}, "rd_to_saf_conversion": {"shared_infrastructure": "Feedstock reception, storage, pretreatment, hydrotreating", "required_mods": "Hydrocracking/hydroisomerization reactors, compression, fractionation", "capex": "Substantial but less than greenfield", "yield_impact": "Higher severity hydrocracking increases H2 consumption and byproduct loss"}, "source": "fm_motiva_pathway_analysis_jun_2024"}'::jsonb)
ON CONFLICT (node_key) DO UPDATE SET properties = EXCLUDED.properties;

-- AtJ Economics Model
INSERT INTO core.kg_node (node_type, node_key, label, properties) VALUES
('analytical_model', 'atj_economics_model', 'Alcohol-to-Jet SAF Economics Model',
 '{"description": "Economic framework for AtJ SAF production from ethanol. Key metric: 1.7 gal ethanol per gal SAF. Requires SAF price premium over ethanol to justify switching.", "conversion_ratio": {"ethanol_per_gal_saf": 1.7, "saf_yield_per_gal_ethanol": [0.6, 0.7]}, "saf_grand_challenge": {"2030_target_bgal": 3, "2050_target_bgal": 35, "ethanol_needed_for_2030_bgal": 5.1, "pct_increase_over_current_production": 30}, "us_ethanol_capacity_bgal": 18, "theoretical_saf_from_all_ethanol_bgal": [10, 11], "policy_incentives": {"ira_saf_credit_range_cpg": [125, 175], "lcfs_value_range_cpg": [17, 90], "45z_max_cpg": 100}, "switching_threshold": "SAF must deliver net revenue above ethanol blending after accounting for 1.7x input ratio, capex amortization, and operational costs. Coal-fired plants face higher CI penalty (~75-80 gCO2e/MJ vs 55-60 for natural gas) reducing credit value.", "corn_price_sensitivity": {"low_3_per_bu_ethanol_cost_cpg": 120, "high_7_per_bu_ethanol_cost_cpg": 250}, "chicago_benchmark": {"price_apr_2025_cpg": 182, "storage_capacity_barrels": 1200000}, "regional_premiums": {"midwest_discount_cpg": [2, 5], "gulf_coast_premium_cpg": [10, 15], "west_coast_premium_cpg": [15, 25]}, "source": "fm_buckeye_atj_analysis_2025"}'::jsonb)
ON CONFLICT (node_key) DO UPDATE SET properties = EXCLUDED.properties;

-- BBD Plant Risk Register
INSERT INTO core.kg_node (node_type, node_key, label, properties) VALUES
('analytical_model', 'bbd_plant_risk_register', 'BBD/SAF Plant Development Risk Register',
 '{"description": "Comprehensive risk taxonomy for developing large-scale RD/SAF facilities. Eight risk categories identified from Motiva consulting engagement.", "risk_categories": {"feedstock_procurement": {"severity": "high", "risks": ["Price surge from demand competition", "Agricultural impact from oilseed expansion", "Supply chain instability"]}, "environmental_health": {"severity": "high", "risks": ["Wastewater with high contaminants", "VOC and particulate emissions", "Solid waste from purification"]}, "technical_operational": {"severity": "medium", "risks": ["Exothermic hydrotreating reactions", "Equipment corrosion", "Catalyst deactivation", "Legacy equipment conversion complexity"]}, "economic_financial": {"severity": "high", "risks": ["Higher production costs vs fossil fuels", "Narrow margins without subsidies", "Feedstock price volatility"]}, "regulatory_policy": {"severity": "high", "risks": ["Complex permitting (air/water/waste/endangered species)", "Policy uncertainty across administrations", "BTC-to-IRA transition risk"]}, "social_community": {"severity": "medium", "risks": ["Public opposition to environmental impact", "Workforce retraining needs"]}, "supply_chain_logistics": {"severity": "medium", "risks": ["Feedstock supply disruptions", "Distribution infrastructure mods"]}, "technological": {"severity": "medium", "risks": ["Rapid industry evolution risk of obsolescence", "R&D investment without guaranteed return"]}}, "permitting_difficulty": {"air_quality": "moderate", "water_quality": "moderate", "waste_management": "moderate", "endangered_species": "high"}, "source": "fm_motiva_bbd_risks_jul_2024"}'::jsonb)
ON CONFLICT (node_key) DO UPDATE SET properties = EXCLUDED.properties;

-- Crush Capacity Expansion Tracker
INSERT INTO core.kg_node (node_type, node_key, label, properties) VALUES
('data_series', 'us_crush_expansion_tracker', 'US/Canada Crush Capacity Expansion Tracker',
 '{"description": "Tracks announced soybean and canola crush expansion projects driving BBD feedstock supply. Growth from <2B bu (2018) to projected 3.3B bu (2033).", "announced_projects": {"green_bison_spiritwood_nd": {"operator": "ADM/Marathon", "capacity_bu_per_day": 150000, "status": "Operational 2023"}, "cargill_pemiscot_mo": {"operator": "Cargill", "expected": 2026}, "platinum_crush_alta_ia": {"operator": "Platinum Crush LLC", "capacity_bu_per_yr": 40000000}, "ndsp_casselton_nd": {"operator": "CGB/MNSP", "capacity_bu_per_yr": 42500000}, "norfolk_crush_ne": {"operator": "Norfolk Crush LLC", "capacity_bu_per_yr": 38500000, "expected": 2024}, "bartlett_ks": {"operator": "Bartlett", "capacity_bu_per_yr": 38500000}, "shell_rock_ia": {"operator": "SRSP", "capacity_bu_per_day": 110000, "status": "Operational 2022"}, "epitome_crookston_mn": {"operator": "Epitome Energy", "capacity_bu_per_yr": 42000000, "status": "Pending permits"}, "agp_sergeant_bluff_ia": {"operator": "AGP", "expansion": "50% increase", "expected": 2023}, "ldc_ohio": {"operator": "Louis Dreyfus", "announced": "Oct 2023"}}, "canada_canola_expansion": {"saskatchewan_current_mmt": 4.1, "saskatchewan_post_expansion_mmt": 10.3, "timeline": "By 2026", "key_risk": "Excess meal production must find markets to avoid price depression"}, "overcapacity_risk": "Expansion may exceed short-term demand, leading to margin compression back to pre-expansion levels.", "source": "fm_motiva_crush_announcements_apr_2024"}'::jsonb)
ON CONFLICT (node_key) DO UPDATE SET properties = EXCLUDED.properties;

-- Corn Wet Milling Process Model
INSERT INTO core.kg_node (node_type, node_key, label, properties) VALUES
('commodity', 'corn_wet_milling', 'Corn Wet Milling Industry',
 '{"description": "Corn wet milling separates kernels into starch, oil, gluten meal, and gluten feed. Key outputs include corn starch (31-33 lbs/bu), corn syrup, HFCS, and corn oil. Global corn starch market $22.39B (2023), projected $38.88B (2030) at 8.2% CAGR.", "process_steps": ["Cleaning", "Steeping (24-48 hrs in water + sulfur dioxide)", "Germ recovery (corn oil extraction)", "Fiber recovery (corn gluten feed)", "Protein recovery (corn gluten meal)", "Starch processing"], "yields_per_bushel": {"corn_starch_lbs": [31, 33], "corn_oil_lbs": 1.6, "corn_gluten_meal_lbs": 2.5, "corn_gluten_feed_lbs": 12}, "competitors": ["Tapioca starch (gluten-free alternative)", "Potato starch"], "starch_applications": ["Food (thickener, sweetener)", "Adhesives (paper/corrugated)", "Textiles (sizing agent)", "Bioplastics", "Pharmaceuticals (binder/disintegrant)"], "source": "fm_corn_milling_101"}'::jsonb)
ON CONFLICT (node_key) DO UPDATE SET properties = EXCLUDED.properties;

-- Soybean Residual Use Theory
INSERT INTO core.kg_node (node_type, node_key, label, properties) VALUES
('analytical_model', 'soybean_residual_use_model', 'Soybean Residual Use Theory',
 '{"description": "Framework for interpreting quarterly grain stocks residual use numbers. Residual use is primarily soybeans in transit during quarterly surveys. Q1 typically shows large positive residual (peak export shipments in transit), which is found back in subsequent quarters.", "mechanism": "During Q1, when US export shipments peak, large volume of soybeans headed to ports. Total export shipments during quarter smaller than implied by inventory number, making usage appear larger than expected. In Q2-Q4, transit volumes decline relative to Q1, resulting in finding back the additional usage.", "implication": "Record Q3 residual use should typically be followed by smaller-than-normal residual in a quarter with above-average exports. If Q1 residual use is 90M bu, should match record or near-record Q1 shipments.", "source": "fm_soy_residual_use_theory"}'::jsonb)
ON CONFLICT (node_key) DO UPDATE SET properties = EXCLUDED.properties;

-- PADD Regional Feedstock Analysis
INSERT INTO core.kg_node (node_type, node_key, label, properties) VALUES
('analytical_model', 'padd_feedstock_strategy', 'PADD Regional Feedstock Strategy Framework',
 '{"description": "Framework for analyzing BBD feedstock business development by PADD region. Each region has distinct advantages for facility siting.", "padd_1_east_coast": {"bbd_expansion": "Limited (space constraints, higher costs)", "activity": "Some biodiesel, limited RD", "policy": "Several Northeast states considering LCFS"}, "padd_2_midwest": {"bbd_expansion": "Significant RD and BD growth", "advantage": "Proximity to soybean oil and corn oil feedstocks", "policy": "Strong state-level biofuel support (IL, MN)"}, "padd_3_gulf_coast": {"bbd_expansion": "Notable, >60% of national RD capacity", "advantage": "Existing refinery infrastructure, port access for imports", "policy": "TX business-friendly permitting"}, "padd_4_rocky_mountain": {"bbd_expansion": "Limited", "opportunity": "Camelina and other niche oilseeds"}, "padd_5_west_coast": {"bbd_expansion": "Moderate", "advantage": "CA LCFS premium market", "constraint": "Higher operating costs, environmental regulations"}, "source": "fm_motiva_feedstock_strategy_2024"}'::jsonb)
ON CONFLICT (node_key) DO UPDATE SET properties = EXCLUDED.properties;

-- Global BBD/SAF Program Registry
INSERT INTO core.kg_node (node_type, node_key, label, properties) VALUES
('reference', 'global_bbd_saf_programs', 'Global BBD and SAF Policy Program Registry',
 '{"description": "Registry of global biomass-based diesel and SAF policy programs by country.", "programs": {"us_federal_rfs": {"type": "mandate", "mechanism": "RINs + tax credits", "body": "EPA"}, "us_ira_saf_credit": {"type": "tax_credit", "value_cpg": [125, 175], "effective": "2023-2024", "body": "IRS"}, "california_lcfs": {"type": "CI_standard", "target": "20% CI reduction by 2030", "credit_value_2024_per_tonne": 68}, "oregon_cfp": {"type": "CI_standard", "target": "10% CI reduction by 2025"}, "washington_cfs": {"type": "CI_standard", "target": "20% CI reduction by 2038", "start": 2023}, "illinois_saf": {"type": "tax_credit", "value_cpg": 150, "duration_years": 10, "ghg_requirement_pct": 50}, "eu_refueleu": {"type": "mandate", "trajectory": {"2025": 2, "2030": 6, "2035": 20, "2040": 32, "2045": 38, "2050": 70}, "synthetic_sub_mandate_2030_pct": 1.2, "synthetic_sub_mandate_2050_pct": 35, "anti_tankering": "90% fuel uplift at departure airport"}, "eu_ets": {"type": "cap_and_trade", "aviation_since": 2012}, "uk_jet_zero": {"type": "target", "saf_target_2030_pct": 10}, "canada_cfs": {"type": "CI_standard", "start": 2022}, "japan_green_innovation": {"type": "funding", "start": 2021}, "brazil_renovabio": {"type": "carbon_credits", "start": 2018}, "south_korea_kets": {"type": "cap_and_trade", "start": 2015}, "corsia": {"type": "offset_scheme", "body": "ICAO", "baseline": "2020 levels", "start": 2021}}, "source": "fm_motiva_world_bbd_programs_jun_2024"}'::jsonb)
ON CONFLICT (node_key) DO UPDATE SET properties = EXCLUDED.properties;

-- Russia Wheat/Grain Export Framework
INSERT INTO core.kg_node (node_type, node_key, label, properties) VALUES
('analytical_model', 'russia_grain_export_model', 'Russia Grain Export Control Framework',
 '{"description": "Framework for understanding Russian government control over grain export markets. Key mechanism: grain damper (fluctuating export duties), export quotas, unofficial price floors, and push for direct sales bypassing intermediaries.", "production_2024_25": {"wheat_mmt": [81, 86], "wheat_prior_year_mmt": 91}, "exports_2024_25": {"wheat_mmt": [47, 48], "wheat_prior_year_mmt": 54.8, "total_grain_mmt": [55, 57]}, "policy_mechanisms": {"grain_damper": "Fluctuating export duties recalculated weekly based on market prices", "export_quotas": "Feb 15 - Jun 30 each year, allocated by prior export share", "price_floor_fob": [240, 250], "direct_sales": "Promoting sales to 13 key importers, bypassing foreign intermediaries"}, "market_structure": {"key_ports": "Black Sea (Novorossiysk hub)", "key_buyers": ["Egypt", "Turkey", "Middle East/North Africa"], "expansion_targets": ["China", "India", "Bangladesh"], "consolidation": "Multinational exits (2023) led to domestic exporter consolidation", "profitability": "Average marginal profit falling to 20-25% (2024), wheat profitability from 33% (2022) to -1%"}, "geopolitical": "Pushing BRICS grain exchange (grain OPEC concept). Ruble exchange rate critical for export decisions.", "source": "fm_russia_wheat_market"}'::jsonb)
ON CONFLICT (node_key) DO UPDATE SET properties = EXCLUDED.properties;

-- Feedstock Pretreatment Margin Model (update existing)
INSERT INTO core.kg_node (node_type, node_key, label, properties) VALUES
('analytical_model', 'feedstock_pretreatment_margin', 'Feedstock Pretreatment Margin Framework',
 '{"description": "Framework for analyzing margins in the feedstock pretreatment/refining business. High-FFA feedstocks command premium pretreatment margins due to LCFS credit advantage.", "pretreatment_margins_cpb": {"poultry_fat": 13, "uco": 12, "note": "High-FFA feedstocks more profitable due to lower acquisition cost + higher subsidy leverage"}, "strategic_considerations": {"location": "Gulf region optimal for proximity to feedstock supply and biofuel markets", "technology": "Focus on technologies that process high-FFA feedstocks", "diversification": "Broaden feedstock base to mitigate supply chain volatility"}, "market_shift": "US transitioning from net exporter to net importer of BBD feedstocks, indicating robust market growth", "current_decline": "Feedstock refining margins compressed in 2024, presenting both challenges and investment opportunities (lower asset multiples).", "source": "fm_motiva_feedstock_refining_apr_2024"}'::jsonb)
ON CONFLICT (node_key) DO UPDATE SET properties = EXCLUDED.properties;


-- ============================================================================
-- 2. EDGES: Causal relationships, supply chain links, competition
-- ============================================================================

-- RD capacity expansion causes LCFS credit price decline
INSERT INTO core.kg_edge (source_node_id, target_node_id, edge_type, weight, properties, created_by, confidence) VALUES
((SELECT id FROM core.kg_node WHERE node_key = 'us_rd_capacity'),
 (SELECT id FROM core.kg_node WHERE node_key = 'lcfs_credit_bank_model'),
 'CAUSES', 0.9,
 '{"mechanism": "RD capacity expansion from 1B gal (end 2021) to 4B+ gal (end 2023) floods CA LCFS market with credits. Credit bank overhang suppresses prices from $200+/tonne (2021) to $56/tonne (late 2023). BBD ships to CA until credit value/gal = marginal production + shipping cost.", "direction": "bearish_for_credit_prices", "source": "fm_quarterly_q4_2023"}'::jsonb,
 'extracted', 0.90);

-- UCO import surge impacts fat/grease price dynamics
INSERT INTO core.kg_edge (source_node_id, target_node_id, edge_type, weight, properties, created_by, confidence) VALUES
((SELECT id FROM core.kg_node WHERE node_key = 'china_uco_role'),
 (SELECT id FROM core.kg_node WHERE node_key = 'us_fat_grease_import_model'),
 'CAUSES', 0.85,
 '{"mechanism": "Chinese UCO shipments shifted from European to US market in 2023. UCO imports nearly doubled from 180M lbs/month (early 2023) to 350M lbs/month (late 2023). Traceability concerns initially caused ship rejections but new traceability startups alleviated concerns. Drove tallow-SBO spread reversal (tallow premium collapsed).", "quantification": "UCO accounted for bulk of import increase. Q2 2023: 608M lbs vs 188M lbs Q2 2022.", "source": "fm_quarterly_q4_2023"}'::jsonb,
 'extracted', 0.90);

-- BTC-to-IRA transition threatens biodiesel sector
INSERT INTO core.kg_edge (source_node_id, target_node_id, edge_type, weight, properties, created_by, confidence) VALUES
((SELECT id FROM core.kg_node WHERE node_key = 'cfpc_45z'),
 (SELECT id FROM core.kg_node WHERE node_key = 'biodiesel_producers'),
 'CAUSES', 0.85,
 '{"mechanism": "BTC-to-IRA (45Z) transition expected to reduce credit value by ~$0.50/gal. Given Fastmarkets estimates of historical BD margins, this would eliminate ALL margin from biodiesel production, effectively bankrupting that sector. IRA favors low-CI feedstocks; BD uses 75% SBO (high CI). IRA also excludes imports (25%+ of BBD supply in 2021), potentially creating mandate shortfall.", "direction": "existential_threat_to_biodiesel", "magnitude": "50_cents_per_gallon_loss", "source": "fm_rd_saf_investment_analysis_2024"}'::jsonb,
 'extracted', 0.90);

-- AtJ competes with gasoline blending for ethanol
INSERT INTO core.kg_edge (source_node_id, target_node_id, edge_type, weight, properties, created_by, confidence) VALUES
((SELECT id FROM core.kg_node WHERE node_key = 'sustainable_aviation_fuel'),
 (SELECT id FROM core.kg_node WHERE node_key = 'ethanol'),
 'COMPETES_FOR', 0.8,
 '{"mechanism": "SAF via AtJ requires 1.7 gal ethanol per gal SAF. SAF Grand Challenge 3B gal by 2030 would need 5.1B gal ethanol (30% increase over current production). Gasoline blending consumes ~17B gal/yr (E10). Competition for Midwest ethanol supply intensifies. SAF must offer premium over ethanol blending value to justify switching.", "us_ethanol_capacity_bgal": 18, "ethanol_blend_wall": "E10 market saturated, E15 growing slowly", "source": "fm_buckeye_saf_analysis_2025"}'::jsonb,
 'extracted', 0.85);

-- Canola oil gaining share in BBD feedstock mix at expense of DCO
INSERT INTO core.kg_edge (source_node_id, target_node_id, edge_type, weight, properties, created_by, confidence) VALUES
((SELECT id FROM core.kg_node WHERE node_key = 'canola_oil'),
 (SELECT id FROM core.kg_node WHERE node_key = 'distillers_corn_oil'),
 'COMPETES_WITH', 0.75,
 '{"mechanism": "Canola oil share of BBD feedstock jumped from 11% (Q2 2022) to 19% (Q2 2023) -- fastest growing feedstock. DCO share fell from 15% to 8% in same period. Canola oil imports from Canada surging to supply demand. DCO supply relatively fixed (byproduct of ethanol production).", "canola_growth_mlbs": {"q2_2022": 348, "q2_2023": 772}, "source": "fm_quarterly_q2_2023"}'::jsonb,
 'extracted', 0.80);

-- Cattle cycle impacts tallow supply
INSERT INTO core.kg_edge (source_node_id, target_node_id, edge_type, weight, properties, created_by, confidence) VALUES
((SELECT id FROM core.kg_node WHERE node_key = 'tallow_yield_model'),
 (SELECT id FROM core.kg_node WHERE node_key = 'tallow_feedstock'),
 'PREDICTS', 0.85,
 '{"mechanism": "US cattle slaughter declining from 2021/22 peak of 34.3M head (down 2.8%). Beef production down 3.5%. But tallow yield per lb beef is highly variable (12-16.1%), making supply forecast uncertain. Tallow production ROSE 4.2% in 2022/23 despite slaughter decline because of yield spike. If yields revert below 13%, consecutive-year production increases end.", "forecast_2024_yield_pct": 15.1, "forecast_2024_production_blbs": 4.03, "source": "fm_bft_commentary_oct_2023"}'::jsonb,
 'extracted', 0.85);

-- HOBO spread as biodiesel profitability proxy
INSERT INTO core.kg_edge (source_node_id, target_node_id, edge_type, weight, properties, created_by, confidence) VALUES
((SELECT id FROM core.kg_node WHERE node_key = 'hobo_spread'),
 (SELECT id FROM core.kg_node WHERE node_key = 'biodiesel_producers'),
 'PREDICTS', 0.7,
 '{"mechanism": "HOBO spread (heating oil minus soybean oil) roughly approximates BD profitability but correlation is imperfect and not predictive. When HOBO spread is positive but monthly output drops below 5yr average, it suggests capacity rationalization underway (lower left quadrant of scatter plot). Last 3 years show Q4 output typically falls relative to implied profitability. 2023 data in that quadrant since July suggests ongoing capacity rationalization.", "caveat": "Extended maintenance schedules or lowered utilization could explain the odd relationship rather than permanent closures.", "source": "fm_biodiesel_commentary_oct_2023"}'::jsonb,
 'extracted', 0.70);


-- ============================================================================
-- 3. CONTEXTS: Expert rules, risk thresholds, analytical frameworks
-- ============================================================================

-- BBD Capacity Utilization Threshold
INSERT INTO core.kg_context (node_id, context_type, context_key, context_value, applicable_when, source) VALUES
((SELECT id FROM core.kg_node WHERE node_key = 'bbd_capacity_utilization_model'),
 'expert_rule', 'capacity_utilization_rationalization_threshold',
 '{"rule": "When combined BBD industry capacity utilization falls below ~70%, biodiesel capacity rationalization begins. Integrated BD producers (650M gal) are swing capacity -- they sell SBO directly when refining margins < BD margins. Removing swing capacity from denominator: utilization ~78%. Historical RD utilization avg (since EIA started reporting Jan 2021): 77.5%. When EPA mandate implies utilization below this level, overproduction relative to mandates is likely.", "threshold_pct": 70, "swing_capacity_bgal": 0.65, "adjusted_threshold_pct": 78, "seasonal_pattern": "Q1 lowest, Q2 +20%, Q3 flat, Q4 +7% to peak", "source": "fm_quarterly_q2_2023"}'::jsonb,
 'always', 'extracted');

-- Fat/Grease Import Transformation Rule
INSERT INTO core.kg_context (node_id, context_type, context_key, context_value, applicable_when, source) VALUES
((SELECT id FROM core.kg_node WHERE node_key = 'us_fat_grease_import_model'),
 'expert_rule', 'import_as_primary_price_driver',
 '{"rule": "With domestic fat/grease production relatively fixed (~15-16B lbs/yr) and BBD demand growing, imports have become the primary price driver in the fat/grease market. The supply response hierarchy: (1) increase imports -- primary price driver, (2) shift demand from non-biofuel end users, (3) reduce feedstock use (least likely given low CI advantage). Tallow sourcing diversifying beyond Canada (100% in 2014/15 -> 27% Q2 2023). UCO from China is largest growth source but traceability concerns remain.", "domestic_supply_blbs": 15.8, "import_share_2023_pct": 20, "canada_tallow_share_declining": true, "source": "fm_quarterly_q2_q4_2023"}'::jsonb,
 'always', 'extracted');

-- RVO Hidden Gem Trading Rule
INSERT INTO core.kg_context (node_id, context_type, context_key, context_value, applicable_when, source) VALUES
((SELECT id FROM core.kg_node WHERE node_key = 'rvo_hidden_gem_model'),
 'expert_rule', 'rvo_headline_vs_effective_mandate',
 '{"rule": "Never trade on headline BBD RVO alone. BBD is the RFS fixer -- it fills ethanol shortfall, advanced mandate gap, and supplemental remand. The effective BBD requirement is always substantially higher than headline number. In 2023: headline 2.82B gal, effective 4.52B gal (1.7B RIN gap). Those who process the full EPA documentation quickly can buy RINs and feedstock at a discount when market over-reacts to headline number.", "analysis_checklist": ["1. Check implied ethanol mandate vs projected consumption", "2. Check advanced mandate minus BBD minus cellulosic minus other advanced", "3. Check supplemental/remand obligations", "4. Sum all gaps = additional BBD gallons needed", "5. Add to headline BBD RVO for effective requirement"], "source": "fm_bob_rvo_analysis_q3_2023"}'::jsonb,
 'wasde_day', 'extracted');

-- Biodiesel-Renewable Diesel Price Spread Trend
INSERT INTO core.kg_context (node_id, context_type, context_key, context_value, applicable_when, source) VALUES
((SELECT id FROM core.kg_node WHERE node_key = 'bbd_ca_margin_model'),
 'expert_rule', 'bd_rd_price_spread_dynamics',
 '{"rule": "The average spread between RD and BD in California has been narrowing as RD capacity expands. CAGR of spread narrowing: -57% (2020-2022). Spread went from 62 cpg (2020) to 4.9 cpg (2022) to ~4 cpg (Q2 2023). Fastmarkets forecasts reversal in 2024 to 29 cpg spread as RD prices rise and BD prices fall. If BD capacity declines, market may establish permanent premium for BD. BD production tends to show weaker output in Q4 relative to profitability -- possible seasonal rationalization pattern.", "spread_history": {"2020_cpg": 62, "2022_cpg": 4.9, "q2_2023_cpg": 4, "forecast_2024_cpg": 29}, "source": "fm_quarterly_q2_2023"}'::jsonb,
 'always', 'extracted');

-- Motiva Executive Summary: Investment Timing Framework
INSERT INTO core.kg_context (node_id, context_type, context_key, context_value, applicable_when, source) VALUES
((SELECT id FROM core.kg_node WHERE node_key = 'bbd_margin_model'),
 'expert_rule', 'bbd_investment_timing_framework_2024',
 '{"rule": "Current decline in profitability at all levels of the BBD supply chain provides investment opportunities. Unlike last several years when asset multiples were at premium levels with substantial financial investor interest, the decline in profitability should cut multiples -- particularly for independent pre-treatment assets. Feedstock price volatility is unlikely to reach 2021-2022 levels; current realized monthly price volatility ~15% below 25-year average. Pattern resembles ethanol capacity build-out 2006-2008: long-term equilibrium rises with structural demand increase, but current volatility normalizing.", "opportunity": "Buy pre-treatment assets at discounted multiples during margin trough", "risk": "BTC-to-IRA transition could eliminate BD margins entirely", "historical_parallel": "2006-2008 ethanol boom-bust-normalize cycle", "sbo_volatility_context": "15% below 25-year average despite structural demand increase", "source": "fm_motiva_executive_summary_jun_2024"}'::jsonb,
 'always', 'extracted');

-- Soybean Oil Non-Biofuel Use Displacement Rule
INSERT INTO core.kg_context (node_id, context_type, context_key, context_value, applicable_when, source) VALUES
((SELECT id FROM core.kg_node WHERE node_key = 'soybean_oil'),
 'expert_rule', 'sbo_non_biofuel_use_displacement',
 '{"rule": "Increased SBO consumption by BBD industry displaces non-biofuel uses. Displacement occurs by replacing SBO with other vegetable oils since SBO is the marginal vegetable oil for BBD. Non-biofuel SBO use fell 11% Q1-to-Q2 2023 (3.8B to 3.4B lbs). Fastmarkets expects 4% reduction in 2023, accelerating to 10% in 2024. Displacement will reach lowest point then stabilize as BBD capacity growth plateaus.", "sbo_biofuel_demand_2023_blbs": 13.93, "sbo_biofuel_demand_yoy_growth_pct": 32, "sbo_biofuel_demand_cagr_pct": 5, "monthly_rd_feedstock_2023_blbs": 1.2, "non_biofuel_decline_2023_pct": 4, "non_biofuel_decline_2024_pct": 10, "source": "fm_quarterly_q2_2023"}'::jsonb,
 'always', 'extracted');

-- ReFuelEU Mandate Trajectory (enrich existing node)
INSERT INTO core.kg_context (node_id, context_type, context_key, context_value, applicable_when, source) VALUES
((SELECT id FROM core.kg_node WHERE node_key = 'refueleu_mandate'),
 'expert_rule', 'refueleu_implementation_details',
 '{"rule": "ReFuelEU Aviation Initiative mandates SAF blending at EU airports on escalating trajectory. Applies to ALL flights departing EU airports regardless of airline origin. Anti-tankering provision requires 90% fuel uplift at departure airport. Synthetic fuel sub-mandates within overall SAF target create separate compliance track. Penalties must be at least double the price difference between conventional fuel and SAF. Member states develop penalty schemes by end 2024.", "trajectory_pct": {"2025": 2, "2030": 6, "2035": 20, "2040": 32, "2045": 38, "2050": 70}, "synthetic_sub_mandates_pct": {"2030": 1.2, "2050": 35}, "saf_definition": "Advanced biofuels, synthetic aviation fuels (e-fuels), recycled carbon fuels. EXCLUDES crop-based biofuels.", "scope": "All flights departing EU airports, domestic and international", "source": "fm_eu_saf_policy_summary_aug_2024"}'::jsonb,
 'always', 'extracted');

-- Ethanol Regional Pricing Framework
INSERT INTO core.kg_context (node_id, context_type, context_key, context_value, applicable_when, source) VALUES
((SELECT id FROM core.kg_node WHERE node_key = 'ethanol'),
 'expert_rule', 'ethanol_regional_pricing_framework',
 '{"rule": "Chicago is the premier US ethanol pricing benchmark due to strategic location, 1.2M barrel storage capacity, and proximity to Midwest corn feedstocks. Regional premiums/discounts relative to Chicago are structurally driven by transportation costs, production capacity, and regulatory environment.", "chicago_baseline": {"storage_barrels": 1200000, "price_apr_2025_cpg": 182, "ytd_change_pct": 7.4}, "regional_differentials": {"midwest_ia_ne_mn": {"premium_cpg": [-5, -2], "driver": "Proximity to corn, high production capacity (IA: 4.5B gal/yr)"}, "gulf_coast": {"premium_cpg": [10, 15], "driver": "Transportation costs, export market dynamics (FOB Houston)"}, "west_coast": {"premium_cpg": [15, 25], "driver": "Transportation costs, LCFS premium, limited local production"}, "east_coast": {"premium_cpg": [8, 12], "driver": "Distance from production, rail/barge costs"}}, "seasonal_pattern": "Summer Q2/Q3 demand up 5-10% (driving season), winter blending slowdown", "regulatory_catalyst": "E15 RVP waiver changes in summer 2025 will significantly reshape regional dynamics. Eight Midwest governors petitioned to rescind 1-psi RVP waiver for year-round E15.", "source": "fm_buckeye_regional_ethanol_2025"}'::jsonb,
 'always', 'extracted');


-- ============================================================================
-- 4. SOURCE REGISTRATION
-- ============================================================================

-- [AUTO-FIXED] INSERT INTO core.kg_source (source_key, source_type, title, date_range, properties) VALUES
-- [AUTO-FIXED] ('fm_quarterly_q2_2023', 'quarterly_report', 'Fats, Fuels and Feedstock Outlook - Q2 2023 Update',
-- [AUTO-FIXED]  '[2023-04-01, 2023-06-30]',
-- [AUTO-FIXED]  '{"author": "Fastmarkets (Tore Alden, Joao)", "publisher": "Fastmarkets Ag Analytics", "length_chars": 76425, "sections": ["Executive Summary", "RFS Mandates", "Capacity Utilization", "Biofuel Summary", "Veg Oil Summary", "Fat/Grease Summary", "LCFS", "Canada CFR", "Balance Sheets"]}'::jsonb)
-- [AUTO-FIXED] ON CONFLICT (source_key) DO NOTHING;

-- [AUTO-FIXED] INSERT INTO core.kg_source (source_key, source_type, title, date_range, properties) VALUES
-- [AUTO-FIXED] ('fm_quarterly_q4_2023', 'quarterly_report', 'Fats, Fuels and Feedstock Outlook - Q4 2023 Update',
-- [AUTO-FIXED]  '[2023-10-01, 2023-12-31]',
-- [AUTO-FIXED]  '{"author": "Fastmarkets (Tore Alden)", "publisher": "Fastmarkets Ag Analytics", "length_chars": 66787, "sections": ["Executive Summary", "BBD Production Records", "Capacity Expansion", "Margin Compression", "Feedstock Import Surge", "LCFS Credit Decline"]}'::jsonb)
-- [AUTO-FIXED] ON CONFLICT (source_key) DO NOTHING;

-- [AUTO-FIXED] INSERT INTO core.kg_source (source_key, source_type, title, date_range, properties) VALUES
-- [AUTO-FIXED] ('fm_motiva_study_2024', 'consulting_engagement', 'Motiva Bio Feedstocks Market Advisory & Outlook Study',
-- [AUTO-FIXED]  '[2024-01-01, 2024-07-31]',
-- [AUTO-FIXED]  '{"client": "Motiva Enterprises", "author": "Fastmarkets Ag Analytics", "scope": "SAF/RD industry analysis, feedstock strategy, pathway economics, regulatory environment", "documents_processed": 12}'::jsonb)
-- [AUTO-FIXED] ON CONFLICT (source_key) DO NOTHING;

-- [AUTO-FIXED] INSERT INTO core.kg_source (source_key, source_type, title, date_range, properties) VALUES
-- [AUTO-FIXED] ('fm_buckeye_study_2025', 'consulting_engagement', 'Buckeye Partners Ethanol-to-SAF Market Analysis',
-- [AUTO-FIXED]  '[2025-03-01, 2025-04-30]',
-- [AUTO-FIXED]  '{"client": "Buckeye Partners", "author": "Fastmarkets / RLC", "scope": "Regional ethanol market analysis, AtJ switching economics, SAF demand fundamentals", "documents_processed": 4}'::jsonb)
-- [AUTO-FIXED] ON CONFLICT (source_key) DO NOTHING;

-- [AUTO-FIXED] INSERT INTO core.kg_source (source_key, source_type, title, date_range, properties) VALUES
-- [AUTO-FIXED] ('fm_bob_rvo_analysis_q3_2023', 'analytical_note', 'Hidden Gems for BBD in EPA 2023-2025 Final Rule (Bob)',
-- [AUTO-FIXED]  '[2023-07-01, 2023-09-30]',
-- [AUTO-FIXED]  '{"author": "Bob (industry analyst)", "publisher": "Fastmarkets", "length_chars": 4135, "key_finding": "BBD effective RVO 4.52B gal vs headline 2.82B gal after accounting for ethanol/cellulosic/remand gaps"}'::jsonb)
-- [AUTO-FIXED] ON CONFLICT (source_key) DO NOTHING;

-- [AUTO-FIXED] INSERT INTO core.kg_source (source_key, source_type, title, date_range, properties) VALUES
-- [AUTO-FIXED] ('fm_commodity_commentary_oct_2023', 'market_commentary', 'FM Commodity Commentary Collection (Oct 2023)',
-- [AUTO-FIXED]  '[2023-10-01, 2023-10-31]',
-- [AUTO-FIXED]  '{"author": "Fastmarkets (Tore Alden, Joao)", "commodities": ["BFT/tallow", "SBO", "biodiesel", "canola oil", "palm oil"], "documents": 6}'::jsonb)
-- [AUTO-FIXED] ON CONFLICT (source_key) DO NOTHING;
