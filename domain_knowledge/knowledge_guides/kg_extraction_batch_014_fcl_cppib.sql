-- ============================================================================
-- RLC Knowledge Graph: Extraction Batch 014 (FCL + CPPIB Consulting Projects)
-- Source: Jacobsen/Fastmarkets consulting project files (2022)
--   FCL: Federated Co-operatives Limited — Canola Crush Margin Study
--   CPPIB: Canada Pension Plan Investment Board — US Renewable Diesel Margin Analysis
-- Extracted: 2026-04-16
-- Scope: Canola crush economics, Canadian market structure, RD margin frameworks,
--         feedstock competition, protein meal displacement, credit value mechanics
-- ============================================================================

-- KEY FINDINGS:
--   * FCL: Canola crush margin methodology for Saskatchewan (seed - (oil + meal)),
--     10-year forecast $95-$344/tonne range, avg $200 (above 5yr avg $142).
--   * Canola oil share 72-87% (vs soy 30-40%) — crushing for oil vs meal paradigm shift.
--   * Saskatchewan crush capacity expansion: 4.6M → 10.3M tonnes (2022-2026).
--   * Canola meal dairy inclusion: 6-17 lbs/cow/day, optimal 60% canola meal + 40% DDGs.
--   * Protein meal oversupply risk is #1 threat to new crush facilities.
--   * CPPIB: US RD margin 3-scenario model (base/high/low): $2.31 / $3.10 / $1.81 per gal avg.
--   * HOBO spread = highest correlation to D4 RIN values.
--   * SBO as marginal feedstock: CI parity unlikely because RINs don't vary by CI.
--   * BTC → IRA credit transition: CI >50 = $0 credit for veg oil RD producers.
--   * Alcohol-to-jet (ATJ) corn-based SAF at ~$2.50/gal feedstock vs $5.70 for lipid SAF.
--   * RD fixed costs 40¢/gal, variable 32.5¢/gal; biodiesel fixed 35¢, variable 30¢.
--   * Conversion: SBO 7.5 lbs/gal, yellow grease 8.2 lbs/gal (BD) / 8.5 lbs/gal (RD).
-- ============================================================================


-- ============================================================================
-- 1. NODES: Companies, commodities, regions
-- ============================================================================

-- FCL (Federated Co-operatives Limited)
INSERT INTO core.kg_node (node_type, node_key, label, properties) VALUES
('company', 'fcl_federated_cooperatives', 'Federated Co-operatives Limited (FCL)',
 '{"type": "cooperative", "headquarters": "Saskatoon, Saskatchewan, Canada", "project": "Proposed Regina canola crushing plant co-located with renewable diesel facility", "crush_capacity_planned_tonnes": 1100000, "planned_opening": 2026, "study_date": "2022-09", "consultant": "The Jacobsen (Fastmarkets)", "strategy": "Vertically integrated canola crush + RD — canola oil as internal feedstock for co-located RD plant, meal as secondary revenue stream", "risk_profile": "Exposed to protein meal displacement as NA crush capacity expands for oil demand"}'::jsonb)
ON CONFLICT (node_key) DO UPDATE SET properties = EXCLUDED.properties;

-- CPPIB
INSERT INTO core.kg_node (node_type, node_key, label, properties) VALUES
('company', 'cppib', 'Canada Pension Plan Investment Board (CPPIB)',
 '{"type": "institutional_investor", "headquarters": "Toronto, Ontario, Canada", "engagement": "Commissioned Fastmarkets to assess US renewable diesel margin outlook for potential investment", "study_date": "2022-11", "consultant": "Fastmarkets (The Jacobsen)", "focus": "RD margin trajectory through 2030 — 3 scenarios", "investment_thesis_type": "infrastructure_renewable_energy"}'::jsonb)
ON CONFLICT (node_key) DO UPDATE SET properties = EXCLUDED.properties;

-- Saskatchewan as a region node
INSERT INTO core.kg_node (node_type, node_key, label, properties) VALUES
('region', 'saskatchewan', 'Saskatchewan, Canada',
 '{"role": "Canada largest canola-producing province, >50% of national output", "avg_production_2018_2020_mmt": 11, "drought_2021_output_mmt": 6.6, "drought_2021_reduction_pct": 40, "crush_capacity_2022_mmt": 4.6, "crush_capacity_2024_mmt": 9.3, "crush_capacity_2026_mmt": 10.3, "logistical_advantage": "Adjacent to 75% of US canola crushing capacity (ND/MN border)", "vulnerability": "Drought risk — 2021 reduced output to 60% of average, doubled canola prices", "dairy_herd_small": "Relative to Ontario/Quebec — disadvantage for domestic meal placement"}'::jsonb)
ON CONFLICT (node_key) DO UPDATE SET properties = EXCLUDED.properties;

-- Canola seed node
INSERT INTO core.kg_node (node_type, node_key, label, properties) VALUES
('commodity', 'canola_seed', 'Canola Seed',
 '{"canadian_production_mmt": 20, "canadian_exports_mmt": 10, "primary_exporters": ["Canada"], "primary_importers": ["China", "Japan", "Mexico", "US"], "crush_capacity_canada_mmt": 11, "planned_expansion_mmt": 5.75, "price_basis": "Saskatchewan FOB (Statistics Canada), ICE Canola futures (Winnipeg)", "price_relationship": "Spread to Central Illinois soybeans used for forecasting", "drought_sensitivity": "2021 Saskatchewan drought cut output 40%, doubled prices in 2022"}'::jsonb)
ON CONFLICT (node_key) DO UPDATE SET properties = EXCLUDED.properties;

-- Canola meal node
INSERT INTO core.kg_node (node_type, node_key, label, properties) VALUES
('commodity', 'canola_meal', 'Canola Meal',
 '{"protein_content_pct": 36, "comparison_to_soy_meal": "Lower protein content, requires amino acid supplementation in non-dairy rations", "primary_use": "Dairy cow feed — niche market with sticky demand", "dairy_inclusion_range_lbs_day": {"low": 6, "avg": 10, "max": 17}, "optimal_blend": "60% canola meal + 40% DDGs yields 104 lbs milk/cow/day", "price_basis": "Velva, ND (USDA) adjusted by freight to Saskatchewan", "canada_is_largest_exporter": true, "exports_to_us_avg_mmt": 3.4, "exports_to_china_avg_mmt": 1.5, "exports_to_mexico_avg_mmt": 0.023, "aquaculture_inclusion_omnivorous_pct": "8-60", "aquaculture_inclusion_carnivorous_pct": "8-38", "storability": "Limited — shorter shelf life than oil, forces crushers to clear market quickly", "oversupply_risk": "As NA crushes for oil, excess meal will pressure prices to historically low levels"}'::jsonb)
ON CONFLICT (node_key) DO UPDATE SET properties = EXCLUDED.properties;

-- DDGs node
INSERT INTO core.kg_node (node_type, node_key, label, properties) VALUES
('commodity', 'ddgs', 'Dried Distillers Grains (DDGs)',
 '{"source": "Ethanol production by-product", "per_bushel_corn_yield_lbs": 17.5, "per_bushel_corn_ethanol_gal": 2.8, "replacement_ratio": "1 MT DDGs replaces 1.22 MT corn+soybean meal mix", "energy_source": "Fat, digestible fiber, and crude protein (not starch)", "dairy_benefits": "Equal or improved milk production vs corn/SBM rations, improved feed conversion", "china_ban_2014": "Banned US DDGs imports due to unapproved GMO corn strain + anti-dumping tariff 33.8% + anti-subsidy 10-10.7%", "strategic_pairing": "DDGs + canola meal blend (40/60) optimal for dairy crude protein"}'::jsonb)
ON CONFLICT (node_key) DO UPDATE SET properties = EXCLUDED.properties;


-- ============================================================================
-- 2. NODES: Analytical models and frameworks
-- ============================================================================

-- Canola crush margin model
INSERT INTO core.kg_node (node_type, node_key, label, properties) VALUES
('model', 'canola_crush_margin_model', 'Canola Crush Margin Model (Saskatchewan)',
 '{"formula": "Crush Margin = (Canola Oil Value × Oil Yield) + (Canola Meal Value × Meal Yield) - Canola Seed Cost", "oil_yield_per_tonne": 0.42, "meal_yield_per_tonne": 0.58, "oil_share_pct_range": {"low": 72, "high": 87}, "soybean_oil_share_pct_range": {"low": 30, "high": 40}, "price_series_used": {"canola_seed": "Saskatchewan (Statistics Canada)", "canola_meal": "Velva ND (USDA) + freight adjustment", "canola_oil": "Midwest USDA / Los Angeles Jacobsen, adjusted to Saskatchewan"}, "projection_methods": ["Monthly shipping differential method", "Volume-weighted average across demand segments"], "historical_5yr_avg_usd_tonne": 142, "2021_estimated_usd_tonne": 279, "forecast_range_2023_2033": {"low": 95, "high": 344, "avg": 200}, "volume_weighted_range_2023_2033": {"low": 128, "high": 334, "avg": 207}, "key_sensitivities": {"per_10_other_domestic_price": "0.40 $/tonne margin change", "per_10_other_country_export": "3.48 $/tonne margin change", "per_10_us_export_price": "2.28 $/tonne margin change"}, "critical_insight": "Historically high margins supported by sharp increase in canola oil value from RD feedstock demand. If biofuel policy changes, veg oil prices could collapse while NA oilseed capacity is still expanding — the primary downside risk."}'::jsonb)
ON CONFLICT (node_key) DO UPDATE SET properties = EXCLUDED.properties;

-- RD margin model (CPPIB study)
INSERT INTO core.kg_node (node_type, node_key, label, properties) VALUES
('model', 'rd_margin_model_cppib', 'US Renewable Diesel Margin Model (CPPIB Study)',
 '{"formula": "Margin = Fuel Revenue + Credit Revenue (RIN + LCFS + BTC + CnT) + Byproduct Revenue - Feedstock Cost - Fixed Costs - Variable Costs", "cost_structure_per_gal": {"fixed_costs_rd": 0.40, "variable_costs_rd": 0.325, "fixed_costs_bd": 0.35, "variable_costs_bd": 0.30}, "byproducts": {"rd_naphtha_per_gal": 0.14, "bd_glycerin_range_per_lb": {"low": 0.10, "high": 0.28}}, "conversion_rates_lbs_per_gal": {"soybean_oil": 7.5, "yellow_grease_bd": 8.2, "yellow_grease_rd": 8.5}, "credit_values_assumed": {"btc": 1.00, "cnt_per_gal": 0.16, "lcfs": "variable by CI pathway", "rin_d4": "modeled from HOBO spread + D6"}, "scenario_results_avg_margin_per_gal": {"base": 2.31, "high": 3.10, "low": 1.81}, "scenario_results_range_per_gal": {"base": {"low": 1.27, "high": 3.40}, "high": {"low": 1.88, "high": 4.41}, "low": {"low": 0.11, "high": 2.94}}, "feedstock_mix_assumption": "SBO ~55% of RD feedstock, ~45% of total BBD feedstock during forecast", "california_assumption": "100% sold to CA market (stylized — actual will be lower)", "key_insight": "RIN prices act as margin insurance — when revenue falls or costs rise, RINs adjust to maintain production at mandate levels. Only fails if large idle capacity can switch to low-CI feedstock."}'::jsonb)
ON CONFLICT (node_key) DO UPDATE SET properties = EXCLUDED.properties;

-- Protein meal displacement model
INSERT INTO core.kg_node (node_type, node_key, label, properties) VALUES
('model', 'protein_meal_displacement_model', 'Protein Meal Displacement Framework',
 '{"thesis": "As NA crushers shift from crushing-for-meal to crushing-for-oil due to RD demand, excess protein meal will pressure prices. This is the single largest risk to new crush facility margins.", "historical_paradigm": "Crushers matched crushing volume to meal demand because (1) meal cannot be stored as long as oil, (2) meal was larger share of product value", "new_paradigm": "RD demand drives oil value so high that crushers will crush for oil, accepting meal price decline", "canola_meal_displacement_markets": ["Canadian domestic dairy (+25% max from higher inclusion rates)", "US Midwest dairy (IN, MI, MN, WI — 3.9M to 6.7M tonnes potential)", "Mexico (6.6M dairy herd, 10.9M tonnes theoretical demand, only 850K current canola use)", "China (1.5M tonnes avg, but political risk)", "Southeast Asia aquaculture (long-term, high logistics cost)"], "soybean_meal_competition": "US soy crush expansion adds hundreds of millions bushels, pushing 450M bu of additional SBM into market", "mitigation_strategies": ["Strategic dairy partnerships in Mexico (pre-opening)", "DDG+canola meal premix partnerships with ethanol plants", "Ontario/Quebec domestic dairy relationships", "Asian aquaculture market development"], "price_clearing_mechanism": "Canola meal will need to buy its way into export markets at prices below historical average"}'::jsonb)
ON CONFLICT (node_key) DO UPDATE SET properties = EXCLUDED.properties;


-- ============================================================================
-- 3. EDGES: Cross-market relationships
-- ============================================================================

-- Canola oil → renewable diesel (feedstock supply)
INSERT INTO core.kg_edge (source_node_id, target_node_id, edge_type, weight, properties, created_by, confidence) VALUES
((SELECT id FROM core.kg_node WHERE node_key = 'canola_oil'),
 (SELECT id FROM core.kg_node WHERE node_key = 'renewable_diesel'),
 'SUPPLIES', 0.85,
 '{"mechanism": "Canola oil is a key RD feedstock, especially in Canada where co-located crush+RD facilities (e.g. FCL Regina) vertically integrate supply. Lower CI than SBO makes it attractive under LCFS. Canada CFR also provides incentive.", "vertical_integration_model": "FCL proposed co-located canola crush + RD plant in Regina — oil transfers internally, reducing feedstock logistics costs", "ci_advantage": "Canola oil CI scores lower than SBO for BD pathways, though federal RD pathway for canola not yet approved (as of 2022)", "source_documents": "FCL Canola Crush Margin Study (2022)"}'::jsonb,
 'extracted', 0.85);

-- RD demand → crush-for-oil paradigm shift
INSERT INTO core.kg_edge (source_node_id, target_node_id, edge_type, weight, properties, created_by, confidence) VALUES
((SELECT id FROM core.kg_node WHERE node_key = 'renewable_diesel'),
 (SELECT id FROM core.kg_node WHERE node_key = 'canola_crush_margin_model'),
 'CAUSES', 0.90,
 '{"mechanism": "Growth in RD feedstock demand drives vegetable oil prices higher, shifting crush economics from crushing-for-meal to crushing-for-oil. This supports historically high canola crush margins ($200/tonne avg forecast vs $142 historical) but creates protein meal oversupply risk.", "direction": "positive_for_oil_value_negative_for_meal_value", "paradigm_shift": "Oil share of canola crush value already 72-87%. RD demand will push soy crushers to crush for oil too, flooding protein meal markets.", "source_documents": "FCL Study (2022)"}'::jsonb,
 'extracted', 0.90);

-- Canola meal competes with soybean meal
INSERT INTO core.kg_edge (source_node_id, target_node_id, edge_type, weight, properties, created_by, confidence) VALUES
((SELECT id FROM core.kg_node WHERE node_key = 'canola_meal'),
 (SELECT id FROM core.kg_node WHERE node_key = 'soybean_meal'),
 'COMPETES_WITH', 0.90,
 '{"mechanism": "Canola meal competes with soybean meal in dairy feed rations. Lower protein content (36% vs ~47%) requires amino acid supplementation in non-dairy applications, limiting substitution. However, canola meal has dairy-specific advantages: improves milk production by ~1 liter/cow/day. US soy crush expansion will flood market with additional SBM, intensifying competition in export markets.", "substitution_ratio_dairy": "Direct but at different inclusion rates", "canola_advantage": "Dairy niche, Saskatchewan-to-Midwest logistics", "soy_advantage": "Higher protein, more versatile across species, dominant global supply", "source_documents": "FCL Study (2022)"}'::jsonb,
 'extracted', 0.90);

-- Saskatchewan → canola seed (supply)
INSERT INTO core.kg_edge (source_node_id, target_node_id, edge_type, weight, properties, created_by, confidence) VALUES
((SELECT id FROM core.kg_node WHERE node_key = 'saskatchewan'),
 (SELECT id FROM core.kg_node WHERE node_key = 'canola_seed'),
 'SUPPLIES', 0.95,
 '{"mechanism": "Saskatchewan accounts for >50% of Canadian canola production (avg 11M tonnes 2018-2020). Province is adjacent to 75% of US canola crush capacity. As crush capacity expands from 4.6M to 10.3M tonnes, competition for Saskatchewan seed supply will intensify, reducing export availability.", "drought_risk": "2021 drought cut Saskatchewan output to ~6.6M tonnes (~60% of average), nearly matching crush capacity and doubling prices", "source_documents": "FCL Study (2022)"}'::jsonb,
 'extracted', 0.95);

-- HOBO spread → D4 RIN prices (highest correlation)
INSERT INTO core.kg_edge (source_node_id, target_node_id, edge_type, weight, properties, created_by, confidence) VALUES
((SELECT id FROM core.kg_node WHERE node_key = 'soybean_oil'),
 (SELECT id FROM core.kg_node WHERE node_key = 'rd_margin_model_cppib'),
 'CAUSES', 0.90,
 '{"mechanism": "The HOBO spread (soybean oil price minus heating oil price) has the highest correlation to D4 RIN values. The spread reflects BBD production profitability. However, feedstock costs play a more prominent role than the revenue side in determining D4 values. SBO is the marginal feedstock — when SBO prices rise, D4 RINs must rise to maintain margin insurance.", "variable_name": "HOBO_spread", "correlation": "Highest of all variables in D4 model", "source_documents": "CPPIB RD Margin Study (2022)"}'::jsonb,
 'extracted', 0.90);

-- Canada CFR → canola oil demand
INSERT INTO core.kg_edge (source_node_id, target_node_id, edge_type, weight, properties, created_by, confidence) VALUES
((SELECT id FROM core.kg_node WHERE node_key = 'canada_cfr'),
 (SELECT id FROM core.kg_node WHERE node_key = 'canola_oil'),
 'CAUSES', 0.80,
 '{"mechanism": "Canadian Clean Fuel Regulations create domestic demand pull for canola oil as BBD feedstock, supporting canola oil prices and crush margins. Combined with US RFS/LCFS incentives, creates dual-market pull for Canadian canola oil production.", "direction": "positive_demand_pull", "source_documents": "FCL Study (2022)"}'::jsonb,
 'extracted', 0.80);

-- Protein meal displacement → crusher feasibility
INSERT INTO core.kg_edge (source_node_id, target_node_id, edge_type, weight, properties, created_by, confidence) VALUES
((SELECT id FROM core.kg_node WHERE node_key = 'protein_meal_displacement_model'),
 (SELECT id FROM core.kg_node WHERE node_key = 'crusher_feasibility_model'),
 'RISK_FACTOR', 0.90,
 '{"mechanism": "Protein meal displacement is identified as the single largest risk to new crush facility margins. As NA crushes for oil, meal must be marketed into declining-price environment. Having a plan to handle excess meal is a critical risk mitigation factor — the FCL study modeled meal price sensitivity per $10 changes across three demand segments.", "sensitivity_per_10_domestic": "$0.40/tonne margin impact", "sensitivity_per_10_export_other": "$3.48/tonne margin impact", "sensitivity_per_10_us_export": "$2.28/tonne margin impact", "source_documents": "FCL Study (2022)"}'::jsonb,
 'extracted', 0.90);

-- SBO as marginal feedstock → CI parity unlikely
INSERT INTO core.kg_edge (source_node_id, target_node_id, edge_type, weight, properties, created_by, confidence) VALUES
((SELECT id FROM core.kg_node WHERE node_key = 'soybean_oil'),
 (SELECT id FROM core.kg_node WHERE node_key = 'feedstock_supply_chain_model'),
 'SUPPLIES', 0.90,
 '{"mechanism": "SBO is the only feedstock that can increase supply in response to rising demand (fats/greases are by-products of more valuable commodities with fixed supply). As marginal feedstock, SBO sets price floor — CI parity between veg oil and F&G is unlikely because (1) RINs dont vary by CI score, (2) fats/greases capped by animal slaughter and restaurant volumes, (3) SBO retains premium as the supply-elastic feedstock.", "ci_parity_thesis_rejected": "Unless ALL credits become CI-based AND spare capacity can switch to low-CI feedstock, feedstocks will NOT trade at CI parity", "sbo_share_of_feedstock": "~55% of RD feedstock, ~45% of total BBD", "source_documents": "CPPIB Study (2022)"}'::jsonb,
 'extracted', 0.90);

-- SAF competes with RD for feedstock
INSERT INTO core.kg_edge (source_node_id, target_node_id, edge_type, weight, properties, created_by, confidence) VALUES
((SELECT id FROM core.kg_node WHERE node_key = 'sustainable_aviation_fuel'),
 (SELECT id FROM core.kg_node WHERE node_key = 'renewable_diesel'),
 'COMPETES_WITH', 0.85,
 '{"mechanism": "SAF production from lipids competes directly with RD for the same feedstock pool. SAF generates 1.6 D4 RINs/gal (vs 1.0 for RD), qualifies for LCFS, and receives higher IRA credits. SAF prices $2+/gal above RD. SAF producers can outbid RD producers for feedstock unless offset by ATJ economics.", "atj_alternative": "Alcohol-to-jet converts ethanol to SAF at ~$2.50/gal feedstock cost (corn-based) vs ~$5.70/gal for lipid-based SAF/RD. ATJ may limit feedstock competition pressure from lipid SAF growth.", "biden_2030_goal_gal": 3000000000, "implied_additional_feedstock_lbs": 22500000000, "source_documents": "CPPIB Study (2022)"}'::jsonb,
 'extracted', 0.85);

-- DDGs + canola meal synergy
INSERT INTO core.kg_edge (source_node_id, target_node_id, edge_type, weight, properties, created_by, confidence) VALUES
((SELECT id FROM core.kg_node WHERE node_key = 'ddgs'),
 (SELECT id FROM core.kg_node WHERE node_key = 'canola_meal'),
 'SUBSTITUTES', 0.80,
 '{"mechanism": "DDGs and canola meal are complementary in dairy cow rations. Optimal blend is 60% canola meal + 40% DDGs for crude protein, yielding 104 lbs milk/cow/day. An ethanol plant near the FCL facility creates strategic partnership opportunity for premixed feed products.", "dairy_inclusion": "6-17 lbs/cow/day canola meal range, combined with DDGs", "source_documents": "FCL Study (2022), DDG-Cow Research"}'::jsonb,
 'extracted', 0.80);

-- D6 RIN → D4 RIN (blend wall arbitrage)
INSERT INTO core.kg_edge (source_node_id, target_node_id, edge_type, weight, properties, created_by, confidence) VALUES
((SELECT id FROM core.kg_node WHERE node_key = 'ethanol'),
 (SELECT id FROM core.kg_node WHERE node_key = 'rd_margin_model_cppib'),
 'CAUSES', 0.85,
 '{"mechanism": "When ethanol production falls short of implied mandate (blend wall), BBD producers generate excess D4 RINs and sell them to cover D6 shortfall. This D4/D6 arbitrage lifts both credit values. D6 RIN prices are therefore a critical input to D4 RIN forecasting. Lower corn prices → lower D6 → lower D4 (all else equal).", "ethanol_blend_wall": "~10% blending rate × gasoline consumption. EV adoption shrinks denominator.", "source_documents": "CPPIB Study (2022)"}'::jsonb,
 'extracted', 0.85);

-- BTC → IRA credit transition risk
INSERT INTO core.kg_edge (source_node_id, target_node_id, edge_type, weight, properties, created_by, confidence) VALUES
((SELECT id FROM core.kg_node WHERE node_key = 'rd_margin_model_cppib'),
 (SELECT id FROM core.kg_node WHERE node_key = 'renewable_diesel'),
 'RISK_FACTOR', 0.85,
 '{"mechanism": "BTC ($1/gal flat) expires end of 2024, replaced by IRA credits using CI-based formula: (50 - fuel_CI) / 50 × $1. For SBO-based RD (CI ~58), credit value drops to $0. For canola oil BD (CI ~53-55), also likely $0. Only low-CI feedstocks (fats/greases) retain credit value. However, Jacobsen argues RIN prices will rise to offset revenue loss because SBO production is essential to meeting mandates.", "ci_threshold": 50, "sbo_avg_ci_lcfs": 58, "canola_oil_bd_avg_ci": "53-55", "jacobsen_thesis": "RIN prices are margin insurance — they must rise to replace lost BTC/IRA revenue if SBO remains the majority feedstock", "carbon_sequestration_offset": "Some facilities investing in CCS pipelines (Midwest→Gulf) to lower CI below 50 threshold", "source_documents": "CPPIB Study (2022)"}'::jsonb,
 'extracted', 0.85);


-- ============================================================================
-- 4. CONTEXTS: Expert rules and analytical frameworks
-- ============================================================================

-- Canola crush margin calculation methodology
INSERT INTO core.kg_context (node_id, context_type, context_key, context_value, applicable_when, source) VALUES
((SELECT id FROM core.kg_node WHERE node_key = 'canola_crush_margin_model'),
 'expert_rule', 'canola_crush_margin_methodology',
 '{"rule": "Canola crush margin = (canola oil price × oil yield/tonne) + (canola meal price × meal yield/tonne) - canola seed price. Canola oil accounts for 72-87% of product value (oil share), far higher than soybean crush (30-40%). This means canola crush margins are overwhelmingly driven by oil value, not meal value.", "yields": {"oil_yield_per_tonne": 0.42, "meal_yield_per_tonne": 0.58}, "price_basis_methodology": {"canola_seed": "Saskatchewan price = Central Illinois SBM price + historical SK-IL spread", "canola_oil": "LA Jacobsen price adjusted to Midwest, then Midwest-to-SK logistics differential", "canola_meal": "Velva ND (USDA) + FCL freight differential to Saskatchewan"}, "two_projection_methods": {"shipping_differential": "Monthly projection based on market-to-SK freight spreads — range $95-$344, avg $190", "volume_weighted": "Demand-segment-weighted average prices by market — range $128-$334, avg $207"}, "source_documents": "FCL Study (2022)"}'::jsonb,
 'always', 'extracted');

-- Crushing-for-oil paradigm shift
INSERT INTO core.kg_context (node_id, context_type, context_key, context_value, applicable_when, source) VALUES
((SELECT id FROM core.kg_node WHERE node_key = 'crusher_feasibility_model'),
 'expert_rule', 'crushing_for_oil_paradigm_shift',
 '{"rule": "Historically, NA crushers matched crushing volume to protein meal demand because: (1) meal cannot be stored as long as oil, (2) meal was a larger share of total product value. RD demand is flipping this — crushers will crush to match oil demand, accepting meal price declines. This is the most significant risk to any new crush facility.", "canola_specifics": "Canola oil share already 72-87%, so canola crushers always had less flexibility. The paradigm shift primarily affects soy crushers who will start crushing for oil, flooding the SBM market and creating knock-on competition for canola meal.", "meal_storability_constraint": "Crushers cannot store meal as long as oil. If meal production exceeds demand, prices must fall to clear the market — no storage buffer.", "risk_mitigation_required": "Any new crush facility MUST have a meal disposition plan. Strategic partnerships with dairies, export relationships, or premix feed products.", "us_soy_expansion": "~450M bushels additional US soy crush capacity coming online, all driven by oil demand from RD", "source_documents": "FCL Study (2022), CPPIB Study (2022)"}'::jsonb,
 'always', 'extracted');

-- RD margin insurance from RINs
INSERT INTO core.kg_context (node_id, context_type, context_key, context_value, applicable_when, source) VALUES
((SELECT id FROM core.kg_node WHERE node_key = 'rd_margin_model_cppib'),
 'expert_rule', 'rin_as_margin_insurance',
 '{"rule": "RIN values function as margin insurance for BBD producers. When revenue falls or costs rise, D4 RIN values must increase to maintain production at mandate levels. The only scenario where this fails is if large idle capacity can profitably switch to low-CI feedstock, displacing SBO without production decline. This is unlikely given fat/grease supply constraints.", "hobo_spread_correlation": "HOBO spread (SBO minus heating oil) has highest correlation to D4 RIN values of any variable. Feedstock costs play more prominent role than revenue side.", "d6_d4_linkage": "When ethanol blend wall binds, BBD producers fill D6 shortfall by selling excess D4 RINs. This D4/D6 arbitrage lifts both credit values.", "btc_to_ira_thesis": "Even if IRA credits are $0 for SBO-based producers (CI>50), RINs must rise to replace lost revenue — otherwise production falls below mandates, which drives RINs higher until equilibrium.", "source_documents": "CPPIB Study (2022)"}'::jsonb,
 'always', 'extracted');

-- CI parity rejection framework
INSERT INTO core.kg_context (node_id, context_type, context_key, context_value, applicable_when, source) VALUES
((SELECT id FROM core.kg_node WHERE node_key = 'feedstock_supply_chain_model'),
 'expert_rule', 'ci_parity_rejection_framework',
 '{"rule": "Feedstocks will NOT trade at CI parity despite the incentive from LCFS. Three structural barriers: (1) RINs — the largest credit component — do not vary by CI score, maintaining a wedge between low-CI and high-CI feedstock values. (2) Fat/grease supply is inelastic — they are by-products of more valuable commodities (slaughter, restaurants). Producers will not kill more cattle to produce more tallow. (3) SBO is the ONLY feedstock that can grow supply in response to price signals (via more acreage/crush). As the marginal feedstock, SBO sets the price floor.", "ira_credit_impact": "IRA credits (2025+) are CI-based nationally, which increases CI relevance. But SBO remains essential to meeting mandates, so CI parity still unlikely.", "uceo_ceiling": "UCO prices cannot exceed the value of the original fat/oil they derive from — supply response has diminishing returns above economic viability threshold of new UCO sources", "two_developments_to_watch": "(1) RD production surpassing biodiesel (happened 2022) increases CI importance. (2) IRA national CI-based credit increases CI importance. But neither sufficient for full CI parity.", "source_documents": "CPPIB Study (2022)"}'::jsonb,
 'always', 'extracted');

-- Canadian canola meal export market development
INSERT INTO core.kg_context (node_id, context_type, context_key, context_value, applicable_when, source) VALUES
((SELECT id FROM core.kg_node WHERE node_key = 'canola_meal'),
 'expert_rule', 'canola_meal_export_market_priority',
 '{"rule": "With Canadian canola crush capacity expanding from 11M to ~16.75M tonnes, meal disposition is critical. Market priority ranking by accessibility and growth potential:", "priority_1_us_midwest": {"market": "IN, MI, MN, WI dairy", "current_demand_mmt": 3.9, "max_demand_17lb_inclusion_mmt": 6.7, "canadian_share_of_feed_pct": 88, "challenge": "Mature market, high existing inclusion rates, hard to increase further"}, "priority_2_mexico": {"market": "Mexico dairy (6.6M head)", "current_canola_imports_mmt": 0.023, "theoretical_max_demand_mmt": 10.9, "actual_feed_use_mmt": 0.85, "opportunity": "Huge gap between theoretical and actual. Best long-term growth market. Requires strategic dairy farm partnerships.", "strategy": "Pre-opening relationship building, brokering meal before facility opens"}, "priority_3_china": {"market": "China", "current_imports_mmt": 1.5, "political_risk": "High — regulatory difficulties, political tension", "outlook": "Not attractive to target for growth but likely continues at historical rates"}, "priority_4_se_asia_aquaculture": {"market": "Thailand, SE Asia aquaculture", "inclusion_rates": "Omnivorous 8-60%, carnivorous 8-38%", "challenge": "High logistics cost, competition from local fishmeal and EU rapeseed meal"}, "source_documents": "FCL Study (2022)"}'::jsonb,
 'always', 'extracted');

-- RD cost structure benchmarks
INSERT INTO core.kg_context (node_id, context_type, context_key, context_value, applicable_when, source) VALUES
((SELECT id FROM core.kg_node WHERE node_key = 'rd_margin_model_cppib'),
 'benchmark', 'rd_bd_cost_structure_benchmarks_2022',
 '{"renewable_diesel": {"fixed_costs_per_gal": 0.40, "variable_costs_per_gal": 0.325, "total_non_feedstock_per_gal": 0.725, "naphtha_byproduct_per_gal": 0.14, "naphtha_yield_pct": "7-9%"}, "biodiesel": {"fixed_costs_per_gal": 0.35, "variable_costs_per_gal": 0.30, "total_non_feedstock_per_gal": 0.65, "glycerin_byproduct_per_lb": {"avg": 0.10, "2021_high": 0.28}, "glycerin_yield_per_gal": "~1 lb"}, "conversion_rates_lbs_per_gal": {"sbo_rd": 7.5, "sbo_bd": 7.5, "yellow_grease_bd": 8.2, "yellow_grease_rd": 8.5, "note": "Fats/greases require more lbs per gallon for RD than BD. Difference more significant for some feedstocks."}, "feedstock_spec_difference": "RD producers typically take crude feedstocks and pre-treat in-house. BD producers prefer refined feedstocks. Co-located crush+BD facilities have optionality to sell crude, refined, or biodiesel.", "integrated_facility_advantage": "Co-located soy crush + BD facilities have optionality advantage and eliminate logistics costs, improving margins vs independent producers", "source_documents": "CPPIB Study (2022)", "vintage": "2022 — costs may shift with industry maturation"}'::jsonb,
 'always', 'extracted');

-- Acreage allocation framework (RD era)
INSERT INTO core.kg_context (node_id, context_type, context_key, context_value, applicable_when, source) VALUES
((SELECT id FROM core.kg_node WHERE node_key = 'rd_margin_model_cppib'),
 'expert_rule', 'us_acreage_allocation_rd_era',
 '{"rule": "RD buildout drives soy acreage expansion at the expense of corn, the inverse of the ethanol buildout era. Base case projects US soy area growing to 94M acres (from ~87.5M) and corn declining to 84.5M (from 88.6M). Total acreage is essentially fixed — CRP releases limited.", "yield_drag": {"corn_continuous_pct": "5-15%", "soy_continuous_pct": "7.5-10%"}, "ethanol_era_analog": "During ethanol buildout, farmers extended corn rotation until yield drag forced switch. Jacobsen expects opposite — extended soybean rotation with eventual soy yield drag.", "corn_price_support": "Reduced corn area supports corn prices at higher levels. Projected avg ~$5/bu across forecast (below 2022 but above historical avg).", "corn_stocks_to_use": "Rises from 9.2% (2021/22) to avg 11% across forecast", "low_margin_case_divergence": "If soy yield drag limits acreage shift, soy area stays near 87.5M. Production falls 260-560M bu below base case, crushing volumes decline 50M bu/yr, SBO stocks drop below 2B lbs — triggering higher feedstock costs.", "source_documents": "CPPIB Study (2022)"}'::jsonb,
 'always', 'extracted');

-- Saskatchewan canola supply-crush balance
INSERT INTO core.kg_context (node_id, context_type, context_key, context_value, applicable_when, source) VALUES
((SELECT id FROM core.kg_node WHERE node_key = 'saskatchewan'),
 'expert_rule', 'saskatchewan_canola_supply_crush_balance',
 '{"rule": "Saskatchewan canola production (~11M tonnes avg) must now support crush capacity expanding from 4.6M to 10.3M tonnes plus seed exports (~500K to US, rest to other markets). By 2026, there will be limited excess supply for exports. Drought is the critical vulnerability — 2021 cut output to 60% of average, nearly matching crush capacity alone.", "supply_tightening_timeline": {"2022": "4.6M crush capacity vs 11M production — ample surplus", "2024": "9.3M crush vs production — surplus narrowing", "2026": "10.3M crush vs production — very tight, exports must decline"}, "drought_scenario": "In a 2021-type drought (6.6M tonnes), even current (2022) capacity nearly exhausts supply. Post-expansion, a drought would create severe seed rationing.", "price_signal": "2022 canola prices doubled from 5yr avg due to 2021 drought + global veg oil shortage. As capacity expands, price volatility from supply shocks will increase.", "source_documents": "FCL Study (2022)"}'::jsonb,
 'always', 'extracted');

-- Food vs fuel policy risk
INSERT INTO core.kg_context (node_id, context_type, context_key, context_value, applicable_when, source) VALUES
((SELECT id FROM core.kg_node WHERE node_key = 'renewable_diesel'),
 'expert_rule', 'food_vs_fuel_policy_risk_framework',
 '{"rule": "Government policy restricting vegetable oil use in BBD is the tail risk for RD margins. EU already proposing 7% cap on veg oil BBD. California CARB considering similar limits. However, because fat/grease supply is fixed, veg oil caps would reduce total BBD production below mandate levels.", "eu_scenario": "Veg oil cap → BBD supply contraction → carbon goals missed unless offset by faster EV adoption. Jacobsen believes grid not robust enough for rapid EV shift.", "california_scenario": "Veg oil cap in CA alone → RD produced from veg oil moves to non-CA markets. Unless EPA mirrors CA policy, net impact on US feedstock mix is minimal. Loss of LCFS credits partially offset by higher RIN prices.", "industry_maturation_thesis": "Jacobsen believes RD industry will reach steady state meeting mandates, then shift to SAF as EV adoption grows for ground transport. SAF becomes the residual demand growth driver, not RD.", "ethanol_precedent": "Ethanol industry faced identical food-vs-fuel criticism but matured and continues operating profitably despite sugar-based ethanol receiving more favorable treatment under current policy.", "source_documents": "FCL Study (2022), CPPIB Study (2022)"}'::jsonb,
 'always', 'extracted');


-- ============================================================================
-- 5. REINFORCE EXISTING NODES: Update with FCL/CPPIB context
-- ============================================================================

-- Update existing canola_oil node with crush economics context
UPDATE core.kg_node
SET properties = properties || jsonb_build_object(
    'fcl_study_2022', jsonb_build_object(
        'oil_share_of_canola_crush_pct', '72-87% (vs 30-40% for soy)',
        'price_basis', 'Midwest USDA / LA Jacobsen, adjusted to Saskatchewan',
        'rd_co_location_model', 'FCL proposed co-located crush+RD plant in Regina — internal feedstock transfer',
        'ci_status_2022', 'Federal RD pathway for canola oil not yet approved; BD pathway CI 53-55'
    )
),
last_reinforced = NOW()
WHERE node_key = 'canola_oil';

-- Update existing soybean_meal node with displacement context
UPDATE core.kg_node
SET properties = properties || jsonb_build_object(
    'fcl_study_2022', jsonb_build_object(
        'displacement_risk', 'US soy crush expansion (~450M bu additional capacity) will flood SBM into market as crushers shift to crushing-for-oil',
        'competitive_pressure_on_canola_meal', 'Additional SBM competes with canola meal in export markets, forcing canola meal to price discount to win share',
        'south_america_impact', 'Argentine SBM exports also under pressure as global crush margins shift'
    )
),
last_reinforced = NOW()
WHERE node_key = 'soybean_meal';

-- Update existing soybeans node with acreage framework
UPDATE core.kg_node
SET properties = properties || jsonb_build_object(
    'cppib_study_2022', jsonb_build_object(
        'acreage_expansion_driven_by_rd', 'Base case: US soy area to 94M acres (from 87.5M) driven by RD oil demand',
        'crush_expansion', 'US crushing to 2.6B bu by end of forecast (from 2.2B in 2021/22)',
        'sbo_2b_lb_threshold', 'Anecdotally, 2B lbs SBO inventory = inflection between rising and falling SBO prices',
        'yield_drag_risk', 'Extended soy rotation → 7.5-10% yield drag per year of continuous soybeans'
    )
),
last_reinforced = NOW()
WHERE node_key = 'soybeans';


-- ============================================================================
-- 6. SOURCE REGISTRY: Register all processed documents
-- ============================================================================

INSERT INTO core.kg_source (
    source_key, source_type, title, location_uri, document_date, document_type,
    commodities, topics, status, first_processed, last_processed,
    nodes_extracted, edges_extracted, contexts_extracted
) VALUES
('docx_fcl_crush_margin_report_final', 'local_file', 'FCL Canola Crush Margin Report - Final Draft (Sep 26 2022)',
 'C:/Users/torem/RLC Dropbox/Tore Alden/Jacobsen/Projects/FCL/FCL Canola Crush Margin Report - Final Draft.docx',
 '2022-09-26', 'consulting_report',
 '{canola_seed,canola_oil,canola_meal,soybean_meal,renewable_diesel,ddgs}',
 '{canola_crush_margins,saskatchewan_crush_expansion,protein_meal_displacement,dairy_feed_rations,crushing_for_oil,mexico_export_opportunity,food_vs_fuel}',
 'completed', NOW(), NOW(), 5, 6, 5)
ON CONFLICT (source_key) DO UPDATE SET
    status = 'completed', last_processed = NOW(),
    commodities = EXCLUDED.commodities, topics = EXCLUDED.topics;

INSERT INTO core.kg_source (
    source_key, source_type, title, location_uri, document_date, document_type,
    commodities, topics, status, first_processed, last_processed,
    nodes_extracted, edges_extracted, contexts_extracted
) VALUES
('docx_fcl_crush_margin_report_draft', 'local_file', 'Canola Crush Margin Report - Draft (Sep 2 2022)',
 'C:/Users/torem/RLC Dropbox/Tore Alden/Jacobsen/Projects/FCL/Canola Crush Margin Rerport - Final Draft.docx',
 '2022-09-02', 'consulting_report',
 '{canola_seed,canola_oil,canola_meal,soybean_meal,renewable_diesel}',
 '{canola_crush_margins,earlier_draft_of_final}',
 'completed', NOW(), NOW(), 0, 0, 0)
ON CONFLICT (source_key) DO UPDATE SET status = 'completed', last_processed = NOW();

INSERT INTO core.kg_source (
    source_key, source_type, title, location_uri, document_date, document_type,
    commodities, topics, status, first_processed, last_processed,
    nodes_extracted, edges_extracted, contexts_extracted
) VALUES
('docx_fcl_report_review_discussion', 'local_file', 'Jacobsen Report Review External Discussion 2022.08.19',
 'C:/Users/torem/RLC Dropbox/Tore Alden/Jacobsen/Projects/FCL/Jacobsen Report Review External Discussion 2022.08.19.docx',
 '2022-08-24', 'consulting_report',
 '{canola_seed,canola_oil,canola_meal}',
 '{earlier_draft_review_discussion}',
 'completed', NOW(), NOW(), 0, 0, 0)
ON CONFLICT (source_key) DO UPDATE SET status = 'completed', last_processed = NOW();

INSERT INTO core.kg_source (
    source_key, source_type, title, location_uri, document_date, document_type,
    commodities, topics, status, first_processed, last_processed,
    nodes_extracted, edges_extracted, contexts_extracted
) VALUES
('docx_fcl_scope_of_work', 'local_file', 'FCL Scope of Work',
 'C:/Users/torem/RLC Dropbox/Tore Alden/Jacobsen/Projects/FCL/Scope of Work.docx',
 '2022-07-29', 'scope_document',
 '{canola_meal,canola_oil,canola_seed}',
 '{scope_10yr_outlook_canola_crush_margins}',
 'completed', NOW(), NOW(), 0, 0, 0)
ON CONFLICT (source_key) DO UPDATE SET status = 'completed', last_processed = NOW();

INSERT INTO core.kg_source (
    source_key, source_type, title, location_uri, document_date, document_type,
    commodities, topics, status, first_processed, last_processed,
    nodes_extracted, edges_extracted, contexts_extracted
) VALUES
('docx_fcl_ryan_canola_draft', 'local_file', 'Ryan - New Canola Draft',
 'C:/Users/torem/RLC Dropbox/Tore Alden/Jacobsen/Projects/FCL/Ryan - New Canola Draft.docx',
 '2022-08-15', 'research_note',
 '{canola_seed,canola_oil,canola_meal,soybean_oil,soybean_meal}',
 '{canadian_canola_industry_overview,saskatchewan_expansion,renewable_diesel_feedstock_shift}',
 'completed', NOW(), NOW(), 0, 0, 0)
ON CONFLICT (source_key) DO UPDATE SET status = 'completed', last_processed = NOW();

INSERT INTO core.kg_source (
    source_key, source_type, title, location_uri, document_date, document_type,
    commodities, topics, status, first_processed, last_processed,
    nodes_extracted, edges_extracted, contexts_extracted
) VALUES
('docx_fcl_aquaculture_diet', 'local_file', 'Aquaculture Diet Research',
 'C:/Users/torem/RLC Dropbox/Tore Alden/Jacobsen/Projects/FCL/Aquaculture Diet.docx',
 '2022-08-26', 'research_note',
 '{canola_meal}',
 '{aquaculture_feed_inclusion_rates,fish_species_canola_tolerance}',
 'completed', NOW(), NOW(), 0, 0, 0)
ON CONFLICT (source_key) DO UPDATE SET status = 'completed', last_processed = NOW();

INSERT INTO core.kg_source (
    source_key, source_type, title, location_uri, document_date, document_type,
    commodities, topics, status, first_processed, last_processed,
    nodes_extracted, edges_extracted, contexts_extracted
) VALUES
('docx_fcl_ddg_cow_research', 'local_file', 'DDG-Cow Research',
 'C:/Users/torem/RLC Dropbox/Tore Alden/Jacobsen/Projects/FCL/DDG-Cow Research.docx',
 '2022-08-15', 'research_note',
 '{ddgs,canola_meal,corn,soybean_meal}',
 '{ddg_dairy_inclusion,canola_meal_ddg_blend,china_ddg_ban,california_dairy_byproducts}',
 'completed', NOW(), NOW(), 0, 0, 0)
ON CONFLICT (source_key) DO UPDATE SET status = 'completed', last_processed = NOW();

INSERT INTO core.kg_source (
    source_key, source_type, title, location_uri, document_date, document_type,
    commodities, topics, status, first_processed, last_processed,
    nodes_extracted, edges_extracted, contexts_extracted
) VALUES
('xlsx_fcl_crush_margin_forecast', 'local_file', 'Canadian Crush Margin Forecast - July 2022',
 'C:/Users/torem/RLC Dropbox/Tore Alden/Jacobsen/Projects/FCL/Canadian Crush Margin Forecast - July 2022.xlsx',
 '2022-07-01', 'model_spreadsheet',
 '{canola_seed,canola_oil,canola_meal}',
 '{crush_margin_projections_2022_2033}',
 'completed', NOW(), NOW(), 0, 0, 0)
ON CONFLICT (source_key) DO UPDATE SET status = 'completed', last_processed = NOW();

INSERT INTO core.kg_source (
    source_key, source_type, title, location_uri, document_date, document_type,
    commodities, topics, status, first_processed, last_processed,
    nodes_extracted, edges_extracted, contexts_extracted
) VALUES
('xlsx_fcl_crush_margins_linked', 'local_file', 'FCL Crush Margins - Linked',
 'C:/Users/torem/RLC Dropbox/Tore Alden/Jacobsen/Projects/FCL/FCL Crush Margins - Linked.xlsx',
 '2022-11-21', 'model_spreadsheet',
 '{canola_seed,canola_oil,canola_meal}',
 '{crush_margin_model_with_assumptions,meal_freight_20_per_tonne,oil_freight_2.5_cents_lb}',
 'completed', NOW(), NOW(), 0, 0, 0)
ON CONFLICT (source_key) DO UPDATE SET status = 'completed', last_processed = NOW();

INSERT INTO core.kg_source (
    source_key, source_type, title, location_uri, document_date, document_type,
    commodities, topics, status, first_processed, last_processed,
    nodes_extracted, edges_extracted, contexts_extracted
) VALUES
('docx_cppib_rd_margin_analysis', 'local_file', 'CPPIB - US Renewable Diesel Margin Analysis (Nov 2022)',
 'C:/Users/torem/RLC Dropbox/Tore Alden/Jacobsen/Projects/CPPBI/CPPIB - First Draft.docx',
 '2022-11-08', 'consulting_report',
 '{renewable_diesel,soybean_oil,yellow_grease,tallow,uco,canola_oil,ethanol,saf}',
 '{rd_margin_model_3_scenarios,hobo_spread_rin_correlation,ci_parity_rejection,btc_ira_transition,saf_feedstock_competition,atj_corn_saf,acreage_allocation_rd_era,food_vs_fuel_policy,ev_adoption_rd_risk,california_lcfs_credit_forecast}',
 'completed', NOW(), NOW(), 2, 6, 5)
ON CONFLICT (source_key) DO UPDATE SET
    status = 'completed', last_processed = NOW(),
    commodities = EXCLUDED.commodities, topics = EXCLUDED.topics;


-- ============================================================================
-- END OF BATCH 014 (FCL + CPPIB consulting projects)
-- ============================================================================
-- Touch summary:
--   NODES:
--     * 8 NEW nodes created: fcl_federated_cooperatives, cppib, saskatchewan,
--       canola_seed, canola_meal, ddgs, canola_crush_margin_model, rd_margin_model_cppib,
--       protein_meal_displacement_model
--     * 3 EXISTING nodes updated: canola_oil, soybean_meal, soybeans
--   EDGES:
--     * 12 NEW edges:
--       - canola_oil → renewable_diesel (SUPPLIES)
--       - renewable_diesel → canola_crush_margin_model (CAUSES)
--       - canola_meal ↔ soybean_meal (COMPETES_WITH)
--       - saskatchewan → canola_seed (SUPPLIES)
--       - soybean_oil → rd_margin_model (CAUSES / HOBO spread)
--       - canada_cfr → canola_oil (CAUSES)
--       - protein_meal_displacement → crusher_feasibility (RISK_FACTOR)
--       - soybean_oil → feedstock_supply_chain_model (SUPPLIES / marginal feedstock)
--       - saf ↔ renewable_diesel (COMPETES_WITH)
--       - ddgs → canola_meal (SUBSTITUTES)
--       - ethanol → rd_margin_model (CAUSES / blend wall)
--       - rd_margin_model → renewable_diesel (RISK_FACTOR / BTC→IRA)
--   CONTEXTS:
--     * 9 NEW contexts:
--       - canola_crush_margin_methodology
--       - crushing_for_oil_paradigm_shift
--       - rin_as_margin_insurance
--       - ci_parity_rejection_framework
--       - canola_meal_export_market_priority
--       - rd_bd_cost_structure_benchmarks_2022
--       - us_acreage_allocation_rd_era
--       - saskatchewan_canola_supply_crush_balance
--       - food_vs_fuel_policy_risk_framework
--   SOURCES:
--     * 10 documents registered in core.kg_source
