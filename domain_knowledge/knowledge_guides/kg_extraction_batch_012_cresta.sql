-- ============================================================================
-- RLC Knowledge Graph: Extraction Batch 012 (Cresta / Braya Feedstock Study)
-- Source: Cresta / Braya consulting project files (Fastmarkets, 2022)
--         Argentine EPA-Certified SBO Feedstock Availability Study
--         Braya Competitive Analysis / Refining Study
-- Folder: C:/Users/torem/RLC Dropbox/Tore Alden/Jacobsen/Projects/Cresta/
-- Extracted: 2026-04-16
-- Scope:  Argentine SBO supply chain, EPA certification, crush capacity,
--         export tax structure, biodiesel economics, Come-by-Chance refinery
--         conversion feasibility, RIN/LCFS credit forecasting, RD margin
--         comparison by region, feedstock-to-fuel cost chain
-- ============================================================================

-- KEY FINDINGS:
--   * Argentina: 49 crush plants, 62 MMT capacity, ~42 MMT actual crush, ~8.5 MMT SBO output
--   * EPA certification: 95% of farmland qualifies but only 30% of farmers enrolled
--   * Certified SBO premium: ~3% over crude degummed (~2.25 cpb / $50/MT at 2022 prices)
--   * Monthly exportable EPA-certified SBO: 210K-630K MT (30-90% enrollment scenarios)
--   * Argentine export tax: 33% soy/SBO (post-Mar 2022), 30% biodiesel
--   * Biodiesel capacity: 4.43B liters (3.87 MMT), ~65% idle capacity
--   * Come-by-Chance refinery (Newfoundland) converting crude oil to RD using Arg SBO
--   * RD from Arg SBO to CA market outbids all alternative SBO end uses
--   * RBD refining capacity: 1.75 MMT (crushers) + 383K MT (standalone)
--   * Crude-to-neutralized SBO refining cost: 0.25-0.40 cpb
--   * Crude-to-RBD SBO refining cost: 0.50-0.75 cpb
--   * SBO refining spread long-term avg: ~1.25 cpb (vs sunflower ~1.75 cpb)
--   * Argentine farmer soy hoarding: ending stocks 44% of use (2020/21) vs 19% 5yr avg
--   * IRA replacing BTC: excludes imports, favors low-CI feedstocks, bullish RINs
--   * SBO = ~50% of US BBD feedstock (2021); shift to CA-like 20% would need +5.5B lbs


-- ============================================================================
-- 1. NODES: Regions, Facilities, Companies, Policies
-- ============================================================================

-- Region: Argentina (if not exists)
INSERT INTO core.kg_node (node_type, node_key, label, properties) VALUES
('region', 'argentina', 'Argentina',
 '{"context": "World largest SBO exporter, top-3 soybean producer. 49 crush plants, 62 MMT crush capacity. Up River hub in Rosario accounts for ~75% of capacity and ~80% of ag exports. Soybean complex is primary source of foreign reserves via export taxes. Farmers hoard soybeans as inflation hedge (ending stocks 44% of use in 2020/21 vs 19% 5yr avg). Export tax structure favors value-added products historically but differential narrowed 2020-2022. EPA certification (CARBIO) enables Argentine SBO to qualify for US RFS RIN generation.", "soybean_production_avg_10yr_mmt": 50, "crush_capacity_mmt": 62, "crush_actual_avg_mmt": 42, "sbo_production_avg_mmt": 8.5, "sbo_export_avg_mmt": 5.0, "biodiesel_capacity_mmt": 3.87, "rbd_refining_capacity_mmt": 2.13}'::jsonb)
ON CONFLICT (node_key) DO UPDATE SET properties = EXCLUDED.properties;

-- Facility: Come-by-Chance Refinery (Braya)
INSERT INTO core.kg_node (node_type, node_key, label, properties) VALUES
('facility', 'come_by_chance_refinery', 'Come-by-Chance Refinery (Braya Renewable Fuels)',
 '{"location": "Newfoundland, Canada", "owner": "Braya Renewable Fuels (backed by Cresta Fund Management)", "conversion": "Crude oil refinery converting to renewable diesel using vegetable oil feedstock (primarily Argentine EPA-certified SBO)", "feedstock_strategy": "Source EPA-certified SBO from Argentina via Atlantic shipping. Argentinas proximity to Atlantic and Rosario port infrastructure make it competitive source.", "competitive_advantage": "RD shipped to California market generates RIN + LCFS + BTC/IRA revenue stack. Margin analysis (2020-Sep 2022) showed Come-by-Chance outbids all alternative Argentine SBO end uses every month.", "status_2022": "Under consideration for conversion investment", "key_risk": "Argentine soybean production variability (La Nina drought), export tax volatility, EPA farmer enrollment expansion pace"}'::jsonb)
ON CONFLICT (node_key) DO UPDATE SET properties = EXCLUDED.properties;

-- Policy: CARBIO EPA Certification Program
INSERT INTO core.kg_node (node_type, node_key, label, properties) VALUES
('policy', 'carbio_epa_certification', 'CARBIO EPA Certification Program (Argentina)',
 '{"description": "Alternative renewable biomass tracking program approved by EPA in January 2015. Run by Camara Argentina de Biocombustibles (CARBIO) with Peterson Control Union as independent surveyor.", "mechanism": "Satellite imagery + waybill tracking to verify soybeans grown on land cultivated before Dec 19, 2007 (go areas vs no-go areas). Waybill zip codes compared against approved go-area database. Crushing plants must segregate certified from non-certified SBO.", "enrollment_2022": "~30% of Argentine farmers have submitted sufficient paperwork", "theoretical_maximum": "~90% of farmland would qualify (95% cultivated before 2007)", "premium_estimate_2022": "~3% over crude degummed SBO price (~2.25 cpb / ~$50/MT)", "certified_sbean_premium": "Less than $10/MT (~26 cpb breakeven for crushers)", "monthly_supply_range_mt": {"low_30pct_enrollment": 210000, "high_90pct_enrollment": 630000}, "minimum_proven_supply": "Based on 2016 US biodiesel imports (442,400 MT/yr), minimum 38,300-82,500 MT/month proven EPA-certified supply", "eu_competition": "REDII requires deforestation-free since 2008 but different standard than EPA. EU may designate SBO as high-risk feedstock by 2030.", "barrier_to_enrollment": "Low -- 20 days lead time for farm evaluation. Cost is documentation/satellite image comparison. EPA premium provides economic incentive for crushers to pay premium for certified beans."}'::jsonb)
ON CONFLICT (node_key) DO UPDATE SET properties = EXCLUDED.properties;

-- Policy: Argentine Export Tax Structure
-- (Reinforce existing node argentina_export_tax with comprehensive data)
INSERT INTO core.kg_node (node_type, node_key, label, properties) VALUES
('policy', 'argentina_export_tax', 'Argentine Differential Export Tax',
 '{"structure_2022": {"soybeans": "33%", "soybean_oil": "33%", "soybean_meal": "31%", "biodiesel_nominal": "30%", "biodiesel_effective": "23.08%"}, "history": {"pre_2015": {"soybeans": "35%", "soybean_oil": "32%"}, "macri_2015": "Eliminated taxes on wheat/corn/sunflower/meat/fish. Cut soy from 35% to 30%, SBO from 32% to 27%.", "macri_2019": "4 pesos/dollar + 18% fixed = effective 24.7% for soybeans and SBO, 19.74% for biodiesel", "fernandez_2019": "Fixed rate: soybeans/SBO 30%, biodiesel 27% (effective 21.25%)", "fernandez_2020": "Reinstated differential: soybeans 33%, SBO 31%, biodiesel 29% (effective 23.08%)", "fernandez_2022_mar": "SBO raised to 33% (in line with soybeans), biodiesel raised to 30%"}, "impact_on_supply": "Tax rate changes drive farmer selling behavior. Rate increases -> early sales. Rate decreases -> delayed sales (farmers wait for better offers). Disruptions are short-lived -- sales resume once new rate implemented.", "risk_assessment": "Rates near decade highs in 2022 so upside risk minimal. Downside risk (cuts) could temporarily delay farmer sales.", "foreign_reserves": "Export taxes are critical source of foreign reserves for Argentine government -- guarantees continued export focus.", "countervailing_duty_link": "Lower biodiesel tax rate was cited by US Trade Representative as effective subsidy -- contributed to 72% CVD + 75% AD duties on Argentine biodiesel imports to US (2017)."}'::jsonb)
ON CONFLICT (node_key) DO UPDATE SET properties = EXCLUDED.properties;

-- Region: Rosario Up River Hub
INSERT INTO core.kg_node (node_type, node_key, label, properties) VALUES
('region', 'rosario_upriver', 'Rosario Up River Crushing/Port Hub',
 '{"location": "Rosario, Santa Fe, Argentina", "significance": "Center of Argentine crushing industry. ~75% of national crush capacity. 32 terminals handling ~80% of Argentine ag exports. Located on Parana River.", "infrastructure": "Integrated crush-port-biodiesel facilities. Crushers source soybeans from domestic producers and Paraguayan imports via Parana waterway.", "risk_parana_river": "77-year low water levels (2022) due to multi-year La Nina drought in Brazil. Dredging underway by Belgian firm. Despite draft restrictions, operations continued -- largest cargo in June 2022 lineup was 40,000 MT SBO shipment.", "logistics_advantage": "Soybean farmers proximity to elevators and crushers means transportation costs are minimal and dont impact marketing decisions. Rising fuel costs have minimal impact on soybean supply to crushers.", "june_2022_lineup": "424,000 MT SBO being loaded or waiting to load"}'::jsonb)
ON CONFLICT (node_key) DO UPDATE SET properties = EXCLUDED.properties;

-- Company nodes for major Argentine crushers
INSERT INTO core.kg_node (node_type, node_key, label, properties) VALUES
('company', 'renova_sa', 'Renova S.A. (Argentina)',
 '{"type": "Soybean crusher and biodiesel producer", "ownership": "JV Viterra S.A. and Vicentin SAIC (formed 2007)", "headquarters": "Bahia Blanca, Buenos Aires", "employees": 800, "annual_sales_usd_m": 265, "facilities": {"san_lorenzo": {"type": "biodiesel", "capacity_sbo_refine_tpd": 900, "capacity_biodiesel_tpd": 1450, "location": "Parana River"}, "timbues": {"type": "crushing + port", "note": "One of largest crush facilities in world. Port: 3000 t/hr grains, 1000 t/hr veg oil.", "location": "Coronda River"}}}'::jsonb)
ON CONFLICT (node_key) DO UPDATE SET properties = EXCLUDED.properties;

INSERT INTO core.kg_node (node_type, node_key, label, properties) VALUES
('company', 'molinos_agro', 'Molinos Agro S.A. (Argentina)',
 '{"type": "Soybean crusher and exporter", "spun_from": "Molinos Rio de la Plata S.A. (2016)", "headquarters": "Buenos Aires", "employees": 600, "annual_sales_usd_b": 2.5, "crush_capacity_tpd": 20000, "sbo_output_tpd": 4000, "location": "Parana River, Santa Fe", "note": "Operates exclusively in Argentina. Integrated crush-port facility, exports to 50+ countries."}'::jsonb)
ON CONFLICT (node_key) DO UPDATE SET properties = EXCLUDED.properties;

INSERT INTO core.kg_node (node_type, node_key, label, properties) VALUES
('company', 'terminal6_industrial', 'Terminal 6 Industrial S.A. (Argentina)',
 '{"type": "Soybean crusher and biodiesel producer", "ownership": "JV AGD and Bunge", "founded": 1985, "location": "Parana River", "facilities": {"plant_1_1998": {"crush_capacity_tpd": 9000}, "plant_2_2005": {"crush_capacity_tpd": 11000}, "biodiesel": {"capacity_tpd": 1500, "sbo_refined_tpd": 2000}}}'::jsonb)
ON CONFLICT (node_key) DO UPDATE SET properties = EXCLUDED.properties;

INSERT INTO core.kg_node (node_type, node_key, label, properties) VALUES
('company', 'ldc_argentina', 'LDC Argentina S.A. (Louis Dreyfus)',
 '{"type": "Soybean crusher and biodiesel producer", "parent": "Louis Dreyfus Group (est. 1851)", "argentina_operations_since": 1925, "parent_employees": 17000, "parent_revenue_usd_b": 51.3, "facilities": {"timbues": {"crush_capacity_tpd": 12000}, "general_lagos": {"crush_capacity_tpd": 8000}, "biodiesel": {"capacity_tpa": 600000, "sbo_refined_tpd": 2000}}, "epa_note": "Established third-party verified deforestation-free soybean supply chain -- primary source for EPA-certified SBO."}'::jsonb)
ON CONFLICT (node_key) DO UPDATE SET properties = EXCLUDED.properties;

-- Data Series: Argentine SBO Basis
INSERT INTO core.kg_node (node_type, node_key, label, properties) VALUES
('data_series', 'argentina.sbo_basis', 'Argentine SBO Basis vs CBOT',
 '{"description": "Basis for soybean oil at Argentine Up River ports relative to nearby CBOT SBO futures.", "units": "cents_per_pound", "historical_range": {"normal_low": -0.6, "normal_high": 0.6, "recent_extreme_low": -12.0, "recent_extreme_high": 6.0}, "note": "Historically ranged 2-4 cpb. Became substantially more volatile 2020-2022, ranging from 12 cents below to 6 cents above CBOT. As Argentine production recovers from drought and BBD feedstock demand grows in US, basis could show substantial discount to CBOT.", "forward_curve": "Spot ~+0.6 cpb, 6-month forward ~-0.6 cpb (long-term averages)", "epa_premium_additional": "~3% over crude degummed, or ~2.25 cpb at 2022 price levels"}'::jsonb)
ON CONFLICT (node_key) DO UPDATE SET properties = EXCLUDED.properties;

-- Data Series: Argentine SBO Refining Margins
INSERT INTO core.kg_node (node_type, node_key, label, properties) VALUES
('data_series', 'argentina.sbo_refining_margin', 'Argentine SBO Refining Cost/Margin Structure',
 '{"description": "Cost to refine crude SBO through various stages in Argentina.", "units": "cents_per_pound", "refining_steps": {"crude_to_degummed": {"cost_range_cpb": [0.15, 0.25], "note": "First step, removes phospholipids"}, "degummed_to_neutralized": {"cost_range_cpb": [0.10, 0.15], "note": "Chemical refining to remove FFAs. Metals removal makes oil suitable for RD production."}, "crude_to_neutralized_total": {"cost_range_cpb": [0.25, 0.40], "note": "Specification suitable for renewable diesel production"}, "neutralized_to_rbd": {"cost_range_cpb": [0.25, 0.35], "note": "Bleach, dewax, deodorize"}, "crude_to_rbd_total": {"cost_range_cpb": [0.50, 0.75]}}, "refining_spread_longterm_avg": {"sbo": 1.25, "sunflower": 1.75, "spread_delta": 0.50}, "competitive_dynamics": "RBD capacity mostly dedicated to sunflower oil due to consumer preference and higher margins. Converting to SBO would require sustained >0.5 cpb increase in SBO refining spread to offset sunflower premium. Ukraine war temporarily widened sunflower spread to 10.4 cpb.", "rbd_export_share": "Only ~3% of total Argentine SBO exports are RBD. Crude degummed = ~98% of exports."}'::jsonb)
ON CONFLICT (node_key) DO UPDATE SET properties = EXCLUDED.properties;

-- Argentine Biodiesel Industry
INSERT INTO core.kg_node (node_type, node_key, label, properties) VALUES
('commodity', 'argentina_biodiesel', 'Argentine Biodiesel (SME)',
 '{"description": "Argentine soybean methyl ester (SME) biodiesel industry.", "plants_2022": 33, "capacity_billion_liters": 4.43, "capacity_mmt": 3.87, "idle_capacity_pct": 65, "production_2021_mmt": 1.72, "production_peak_mmt": 2.87, "production_peak_year": 2017, "pre_pandemic_5yr_avg_mmt": 2.3, "sbo_yield_pct": 96, "domestic_mandate": {"current_2022": "7.5%", "history": "9-10% (2015-2019), 4.8% (2020 pandemic), 5% (Jul 2021 new law, can drop to 3%), 7.5% (Jun 2022 increase)", "domestic_demand_2022_mmt": 0.39, "peak_domestic_mmt": 1.2, "peak_year": 2017}, "export_2021_mmt": 1.2, "export_peak_mmt": 1.65, "export_peak_year": 2017, "us_tariffs": "72% CVD + 75% AD duties since 2017 -- effectively closed US market", "eu_quota": "1.36B liters (1.19 MMT) duty-free annual quota (agreed 2019)", "eu_outlook": "Exports to fulfill quota near-term, then slowly decline due to advanced feedstock incentives and RD preference", "economics_2022": {"sbo_cost_mt": 1825, "domestic_biodiesel_price_may_pesos": 182143, "domestic_biodiesel_price_may_usd": 1552, "eu_fame_price_mt": 1915, "export_tax_rate_pct": 30, "domestic_margin_longterm_avg_cpg": -4, "domestic_margin_ex_pandemic_cpg": 20}}'::jsonb)
ON CONFLICT (node_key) DO UPDATE SET properties = EXCLUDED.properties;

-- Argentine Soybean Complex Balance Sheet Model
INSERT INTO core.kg_node (node_type, node_key, label, properties) VALUES
('model', 'argentina_soybean_complex_model', 'Argentine Soybean Complex S&D Model',
 '{"description": "Framework for Argentine soybean, meal, and oil supply/demand analysis.", "key_parameters": {"production_10yr_avg_mmt": 50, "production_peak_mmt": 61, "production_peak_year": 2016, "crush_capacity_mmt": 62, "crush_actual_avg_mmt": 42, "crush_utilization_pct": 63, "sbo_yield_pct": 19.4, "sbo_production_capacity_mmt": 12.4, "sbo_production_avg_mmt": 7.5, "sbo_forecast_avg_mmt": 8.6, "domestic_nonbiofuel_sbo_mmt": 0.325, "sbo_export_avg_mmt": 5.0, "sbo_export_forecast_mmt": 6.5}, "crush_industry_structure": {"total_plants": 49, "soybean_plants": 47, "total_oilseed_capacity_mmt": 69.5, "soybean_capacity_mmt": 63.5, "sunflower_capacity_mmt": 5.0, "other_oilseed_mmt": 1.0, "extraction_types": {"chemical_only_pct": 85.6, "physical_only_tpa": 139500, "dual_process_mmt": 9.24}}, "concentration": "Top 8 companies = ~80% of installed capacity. Top 14 = 96%. Seven of top 13 are also largest biodiesel producers (68% of BD capacity).", "farmer_behavior": {"inflation_hedge": "Farmers hold soybeans as dollar-denominated hedge against inflation. Ending stocks 44% of use in 2020/21 vs 19% 5yr avg.", "tax_sensitivity": "Behind inflation, tax rates are most important factor for sell timing. Rate increases -> early sales, rate decreases -> delayed sales.", "soy_dollar_program": "Government had to offer special exchange rate (soy dollar) to incentivize farmer sales in 2022. Incentive was significantly larger than EPA premium."}}'::jsonb)
ON CONFLICT (node_key) DO UPDATE SET properties = EXCLUDED.properties;

-- RD Regional Margin Comparison Model
INSERT INTO core.kg_node (node_type, node_key, label, properties) VALUES
('model', 'rd_regional_margin_comparison', 'RD Regional Gross Margin Comparison Model',
 '{"description": "Framework for comparing gross margin of renewable diesel facilities across US regions and Come-by-Chance, all shipping to California market.", "methodology": "Weekly avg profitability Jan 2020 - Sep 2022. Revenue = LA RD price + RINs + LCFS credits + BTC + Cap-n-Trade. Costs = feedstock (region-specific mix, adjusted for shipping) + fuel freight to LA.", "regions": ["Come-by-Chance (Newfoundland)", "Midwest US", "US Gulf Coast", "West Coast US"], "feedstock_assumptions": {"cbc": "EPA-certified Argentine SBO", "midwest": "Regional average mix based on operating facility analysis", "gulf_coast": "Regional average mix based on operating facility analysis", "west_coast": "Stylized: 50% tallow, 25% UCO, 25% DCO (limited operating facilities during period)"}, "key_finding": "Come-by-Chance showed highest relative profitability per tonne of SBO in EVERY month over the last two years vs all alternative Argentine SBO end uses (India export, other export destinations, domestic biodiesel). Could outbid other end users for EPA-certified SBO supply.", "shipping_cost_methodology": "Rail costs adjusted from Q4 2017 base using AAR All-Inclusive Index Less Fuel inflation factor. Feedstock shipping uses estimated long-term avg IL-to-CA veg oil rail freight.", "revenue_components": ["RD price (LA market)", "D4 RINs", "LCFS credits", "Blenders Tax Credit ($1/gal)", "Cap-and-Trade credits"]}'::jsonb)
ON CONFLICT (node_key) DO UPDATE SET properties = EXCLUDED.properties;

-- IRA BTC Transition Impact Model
INSERT INTO core.kg_node (node_type, node_key, label, properties) VALUES
('model', 'ira_btc_transition_model', 'IRA/BTC Transition Impact on BBD Economics',
 '{"description": "Framework for analyzing the transition from $1/gal Blenders Tax Credit to IRA CI-based credits.", "btc_mechanics": "BTC value belongs to blender; sometimes split with producer but no standard split (not 50/50). Makes impact calculation difficult.", "ira_key_change": "IRA credits EXCLUDE imported biofuels, unlike BTC. Favors low-CI feedstocks.", "import_impact": "2021 US BBD imports = 675M gal (25%+ of total supply). Loss of BTC on imports -> decline in import volumes -> mandates shortfall -> RIN prices must rise.", "feedstock_shift_scenario": "If national feedstock mix shifts to California-like (SBO from 50% to 20%): needs +5.5B lbs additional fats/greases (2021), growing to +9B lbs by 2024. Due to yield differential, need ~7B lbs fats/greases to replace 5.5B lbs SBO.", "production_impact": "5.5B lb SBO decline = 740M gal less BBD (2021), growing to 1.22B gal by 2024. Impossible to meet mandates without SBO.", "conclusion": "SBO remains primary US BBD feedstock until advanced feedstocks commercially viable. RIN prices must rise to offset reduced IRA credit revenue vs BTC for SBO-based production.", "fats_greases_ceiling": "If all US fats/greases shifted to BBD, non-biofuel industries (where fats/greases are small % of cost) would bid up prices above biofuel breakeven -> RINs must compensate."}'::jsonb)
ON CONFLICT (node_key) DO UPDATE SET properties = EXCLUDED.properties;


-- ============================================================================
-- 2. EDGES: Causal relationships, supply chain links, competition
-- ============================================================================

-- Argentina supplies global SBO
INSERT INTO core.kg_edge (source_node_id, target_node_id, edge_type, weight, properties, created_by, confidence) VALUES
((SELECT id FROM core.kg_node WHERE node_key = 'argentina'),
 (SELECT id FROM core.kg_node WHERE node_key = 'soybean_oil'),
 'SUPPLIES', 1.0,
 '{"mechanism": "Argentina is world largest SBO exporter. 49 crush plants, avg 42 MMT crush, ~8.5 MMT SBO output, ~5-6.5 MMT exported. Up River Rosario hub handles 75% of crush capacity and 80% of ag exports. Top export destinations: India (~50%), Bangladesh, Peru. Crude degummed = 98% of exports. EPA certification (CARBIO) enables qualifying for US RFS.", "volume_mmt": {"production": 8.5, "export": 5.0, "export_forecast": 6.5}, "source": "cresta_study_2022"}'::jsonb,
 'extracted', 0.95);

-- EPA certification causally enables RIN generation from Argentine SBO
INSERT INTO core.kg_edge (source_node_id, target_node_id, edge_type, weight, properties, created_by, confidence) VALUES
((SELECT id FROM core.kg_node WHERE node_key = 'carbio_epa_certification'),
 (SELECT id FROM core.kg_node WHERE node_key = 'soybean_oil'),
 'ENABLES', 0.9,
 '{"mechanism": "CARBIO EPA certification enables Argentine SBO to qualify as renewable biomass under RFS, allowing RIN generation when converted to biofuel. Without certification, Argentine SBO cannot generate RINs and loses $50+/MT value advantage. Only 30% of farmers enrolled (2022) but 90% of farmland qualifies -- enrollment is demand-driven.", "premium_cpb": 2.25, "premium_pct": 3, "enrollment_current_pct": 30, "enrollment_potential_pct": 90, "source": "cresta_study_2022"}'::jsonb,
 'extracted', 0.90);

-- Argentine export tax impacts SBO supply availability
INSERT INTO core.kg_edge (source_node_id, target_node_id, edge_type, weight, properties, created_by, confidence) VALUES
((SELECT id FROM core.kg_node WHERE node_key = 'argentina_export_tax'),
 (SELECT id FROM core.kg_node WHERE node_key = 'argentina'),
 'CAUSES', 0.8,
 '{"mechanism": "Export tax changes drive farmer selling behavior. Rate increases trigger early sales (avoid lower future offers). Rate decreases delay sales (wait for better offers from crushers). Disruptions are short-lived -- sales resume once new rate implemented. Behind inflation, tax rates are most important factor for Argentine farmer sell timing. However, market sets world SBO price and tax burden falls on seller, so impact on SBO prices/availability is minimal.", "direction": "farmer_selling_timing_disruption", "magnitude": "short_term_disruption_only", "risk_level_2022": "Rates at decade highs, so upside risk minimal. Downside (cuts) could temporarily delay farmer sales.", "source": "cresta_study_2022"}'::jsonb,
 'extracted', 0.85);

-- Argentine inflation drives farmer soybean hoarding
INSERT INTO core.kg_node (node_type, node_key, label, properties) VALUES
('concept', 'argentina_farmer_soy_hoarding', 'Argentine Farmer Soybean Hoarding Behavior',
 '{"mechanism": "Argentine farmers hold soybeans as dollar-denominated inflation hedge because crushers/exporters sell in USD. Inflation rate (6% to 100%+ annualized) is primary driver of sell timing. Results in ending stocks 44% of use (2020/21) vs 19% 5yr avg. Buffer stocks allow crushers to maintain volumes during production shortfalls -- e.g., despite 12 MMT production drop over 2 years, 2021 crush was above 5yr avg and rose 4 MMT from prior year.", "implication_for_crush": "Farmer hoarding smooths crush volumes across crop years. La Nina drought reduced production but crush remained steady.", "policy_precedent": "Government soy dollar program (2022) offered special exchange rate to incentivize sales -- incentive was significantly larger than EPA premium.", "source": "cresta_study_2022"}'::jsonb)
ON CONFLICT (node_key) DO UPDATE SET properties = EXCLUDED.properties;

INSERT INTO core.kg_edge (source_node_id, target_node_id, edge_type, weight, properties, created_by, confidence) VALUES
((SELECT id FROM core.kg_node WHERE node_key = 'argentina_farmer_soy_hoarding'),
 (SELECT id FROM core.kg_node WHERE node_key = 'argentina'),
 'CAUSES', 0.85,
 '{"mechanism": "Farmer soybean hoarding in Argentina smooths crush volumes across crop years. Despite production variability (50 MMT avg, 43-61 MMT range), crush remains steady at ~42 MMT because farmers hold 19-44% of annual use in buffer stocks. Critical for understanding Argentine SBO supply reliability -- production shortfalls do NOT proportionally reduce SBO output.", "direction": "supply_smoothing", "quantification": "12 MMT production decline over 2 years, but crush rose 4 MMT and was above 5yr avg", "source": "cresta_study_2022"}'::jsonb,
 'extracted', 0.90);

-- Come-by-Chance competes with traditional Argentine SBO export destinations
INSERT INTO core.kg_edge (source_node_id, target_node_id, edge_type, weight, properties, created_by, confidence) VALUES
((SELECT id FROM core.kg_node WHERE node_key = 'come_by_chance_refinery'),
 (SELECT id FROM core.kg_node WHERE node_key = 'argentina_biodiesel'),
 'COMPETES_WITH', 0.85,
 '{"mechanism": "Come-by-Chance refinery sourcing EPA-certified Argentine SBO competes with (1) Indian/Asian crude SBO importers, (2) Argentine domestic biodiesel producers, (3) EU biodiesel market. Revenue stack analysis (RD price + RINs + LCFS + BTC to CA market) showed Come-by-Chance had highest profitability per tonne of SBO in EVERY month Jan 2020 - Sep 2022. Could outbid all alternative end uses. Only potential competition: integrated Argentine biodiesel producers with domestic mandate economics (avg +20 cpg ex-pandemic) may resist, potentially requiring additional ~1 cpb premium.", "competitive_ranking": "1st every month in 2020-2022 analysis period", "source": "cresta_braya_study_2022"}'::jsonb,
 'extracted', 0.85);

-- US anti-dumping duties freed up Argentine SBO for export market
INSERT INTO core.kg_edge (source_node_id, target_node_id, edge_type, weight, properties, created_by, confidence) VALUES
((SELECT id FROM core.kg_node WHERE node_key = 'argentina_biodiesel'),
 (SELECT id FROM core.kg_node WHERE node_key = 'soybean_oil'),
 'SUPPLIES', 0.75,
 '{"mechanism": "US CVD (72%) + AD (75%) duties since 2017 effectively closed US market for Argentine biodiesel. Volume previously used for US biodiesel exports (442K MT peak year 2016) freed SBO supply for export market. Domestic mandate reduction (10% -> 5-7.5%) further released 450-600K MT SBO annually. Argentine biodiesel industry has 65% idle capacity.", "direction": "positive_for_sbo_export_supply", "freed_volume_mmt": {"from_us_market_closure": 0.44, "from_mandate_reduction": 0.45}, "source": "cresta_study_2022"}'::jsonb,
 'extracted', 0.80);

-- EU REDII soybean oil high-risk feedstock risk
INSERT INTO core.kg_edge (source_node_id, target_node_id, edge_type, weight, properties, created_by, confidence) VALUES
((SELECT id FROM core.kg_node WHERE node_key = 'argentina_biodiesel'),
 (SELECT id FROM core.kg_node WHERE node_key = 'renewable_diesel'),
 'COMPETES_WITH', 0.70,
 '{"mechanism": "EU REDII caps crop-based biofuels at 2020 levels (max 7%). Commission considering classifying SBO as high-risk feedstock (phase-out by 2030). Individual member states moving ahead -- France banned palm oil biofuels (2020), Germany/Austria/Netherlands banning palm biodiesel (2022-2023). Rising EU preference for renewable diesel (HVO) over biodiesel (FAME) due to cold-weather properties and lower CI. Long-term decline in EU demand for Argentine SME frees additional SBO for export market.", "eu_quota": "1.36B liters (1.19 MMT) duty-free annual quota for Argentine biodiesel", "eu_redii_target": "32% renewable energy by 2030, 14% transport sector, considering B10 mandate", "source": "cresta_study_2022"}'::jsonb,
 'extracted', 0.80);

-- Parana River water levels impact SBO exports
INSERT INTO core.kg_edge (source_node_id, target_node_id, edge_type, weight, properties, created_by, confidence) VALUES
((SELECT id FROM core.kg_node WHERE node_key = 'rosario_upriver'),
 (SELECT id FROM core.kg_node WHERE node_key = 'soybean_oil'),
 'RISK_FACTOR', 0.7,
 '{"mechanism": "Parana River water levels (driven by Brazil rainfall) impact shipping from Rosario Up River hub. 77-year low in 2022 from multi-year La Nina. Draft restrictions reduce cargo sizes but do not halt operations -- June 2022 lineup showed 424K MT SBO loading/waiting. Government consolidated port management and contracted Belgian dredging. Risk is delay/cost, not supply cutoff.", "severity": "moderate_logistics_risk", "mitigation": "Government dredging contracts, operations continue at reduced draft", "source": "cresta_study_2022"}'::jsonb,
 'extracted', 0.75);

-- IRA transition favors low-CI feedstocks over SBO
INSERT INTO core.kg_edge (source_node_id, target_node_id, edge_type, weight, properties, created_by, confidence) VALUES
((SELECT id FROM core.kg_node WHERE node_key = 'ira_btc_transition_model'),
 (SELECT id FROM core.kg_node WHERE node_key = 'soybean_oil'),
 'CAUSES', 0.85,
 '{"mechanism": "IRA replacing BTC shifts economics against SBO (higher CI than fats/greases). IRA excludes imports (was 25% of supply). Forces domestic production expansion or RIN price increase. But impossible to replace SBO entirely -- if all US fats/greases went to BBD, non-biofuel industries would outbid biofuel for supply. SBO remains primary US BBD feedstock. RINs must rise to compensate SBO-based producers for lower IRA credits vs $1 BTC.", "direction": "structurally_negative_but_not_eliminative", "quantification": "SBO displacement from 50% to 20% of US BBD feedstock needs +5.5B lbs fats/greases (2021), growing to +9B lbs by 2024", "source": "cresta_braya_study_2022"}'::jsonb,
 'extracted', 0.85);

-- Link Come-by-Chance to HEFA technology
INSERT INTO core.kg_edge (source_node_id, target_node_id, edge_type, weight, properties, created_by, confidence) VALUES
((SELECT id FROM core.kg_node WHERE node_key = 'come_by_chance_refinery'),
 (SELECT id FROM core.kg_node WHERE node_key = 'hefa_technology'),
 'USES', 0.90,
 '{"mechanism": "Come-by-Chance refinery conversion from crude oil to renewable diesel using HEFA process with vegetable oil feedstock (primarily Argentine EPA-certified SBO). Similar to HOBO facility but Atlantic-facing with South American feedstock strategy.", "source": "cresta_braya_study_2022"}'::jsonb,
 'extracted', 0.90);

-- Link to existing feedstock supply chain model
INSERT INTO core.kg_edge (source_node_id, target_node_id, edge_type, weight, properties, created_by, confidence) VALUES
((SELECT id FROM core.kg_node WHERE node_key = 'argentina_soybean_complex_model'),
 (SELECT id FROM core.kg_node WHERE node_key = 'feedstock_supply_chain_model'),
 'EXTENDS', 0.85,
 '{"mechanism": "Argentine soybean complex model extends the global feedstock supply chain model with Argentina-specific dynamics: farmer hoarding behavior, export tax structure, CARBIO EPA certification, Rosario Up River logistics, biodiesel idle capacity freeing SBO supply.", "source": "cresta_study_2022"}'::jsonb,
 'extracted', 0.85);

-- Link to existing BBD margin model
INSERT INTO core.kg_edge (source_node_id, target_node_id, edge_type, weight, properties, created_by, confidence) VALUES
((SELECT id FROM core.kg_node WHERE node_key = 'rd_regional_margin_comparison'),
 (SELECT id FROM core.kg_node WHERE node_key = 'bbd_margin_model'),
 'EXTENDS', 0.85,
 '{"mechanism": "Regional margin comparison model extends BBD margin model with geographic dimension: Come-by-Chance vs Midwest vs Gulf Coast vs West Coast. All shipping to CA market. Revenue stack: RD price + RINs + LCFS + BTC + Cap-n-Trade. Cost: region-specific feedstock mix + shipping. Come-by-Chance using Argentine SBO was most profitable in every month analyzed (Jan 2020 - Sep 2022).", "source": "cresta_braya_study_2022"}'::jsonb,
 'extracted', 0.85);

-- Link IRA model to existing RIN oversupply model
INSERT INTO core.kg_edge (source_node_id, target_node_id, edge_type, weight, properties, created_by, confidence) VALUES
((SELECT id FROM core.kg_node WHERE node_key = 'ira_btc_transition_model'),
 (SELECT id FROM core.kg_node WHERE node_key = 'rin_oversupply_model'),
 'EXTENDS', 0.80,
 '{"mechanism": "IRA/BTC transition analysis extends RIN pricing framework. Loss of BTC on imports reduces supply by 675M gal/yr (25% of total), creating mandate shortfall that must be resolved via RIN price increases. RINs conceptualized as margin stabilizers that rise when production falls short of mandates.", "source": "cresta_braya_study_2022"}'::jsonb,
 'extracted', 0.80);

-- Link to existing crusher feasibility model
INSERT INTO core.kg_edge (source_node_id, target_node_id, edge_type, weight, properties, created_by, confidence) VALUES
((SELECT id FROM core.kg_node WHERE node_key = 'argentina_soybean_complex_model'),
 (SELECT id FROM core.kg_node WHERE node_key = 'crusher_feasibility_model'),
 'EXTENDS', 0.80,
 '{"mechanism": "Argentine crush industry data provides international comparison point for crusher feasibility analysis. 63% utilization rate on 62 MMT capacity. Chemical vs physical extraction economics. SBO refining cost chain (crude to degummed to neutralized to RBD) with cost estimates at each step.", "source": "cresta_braya_study_2022"}'::jsonb,
 'extracted', 0.80);

-- Sunflower oil competes with SBO for refining capacity in Argentina
INSERT INTO core.kg_edge (source_node_id, target_node_id, edge_type, weight, properties, created_by, confidence) VALUES
((SELECT id FROM core.kg_node WHERE node_key = 'sunflower_oil'),
 (SELECT id FROM core.kg_node WHERE node_key = 'soybean_oil'),
 'COMPETES_WITH', 0.75,
 '{"mechanism": "In Argentina, sunflower oil competes with SBO for RBD refining capacity. Sunflower refining margin avg ~1.75 cpb vs SBO ~1.25 cpb, so Argentine refiners preferentially allocate capacity to sunflower. Consumer preference for sunflower oil in domestic food market. Converting sunflower capacity to SBO would require sustained >0.5 cpb increase in SBO refining spread. Ukraine war temporarily widened sunflower spread from 3.4 to 10.4 cpb.", "geography": "argentina", "source": "cresta_braya_study_2022"}'::jsonb,
 'extracted', 0.75);


-- ============================================================================
-- 3. CONTEXTS: Expert rules, risk thresholds, analytical frameworks
-- ============================================================================

-- EPA Certification Supply Framework
INSERT INTO core.kg_context (node_id, context_type, context_key, context_value, applicable_when, source) VALUES
((SELECT id FROM core.kg_node WHERE node_key = 'carbio_epa_certification'),
 'expert_rule', 'epa_certified_sbo_supply_framework',
 '{"rule": "EPA-certified Argentine SBO supply is demand-driven, not capacity-constrained. Only 30% of farmers enrolled but 90% of farmland qualifies. Key constraint is the 20-day lead time for new farm evaluation + documentation/satellite verification. Premium of ~3% (2.25 cpb at 2022 prices) provides economic incentive.", "supply_tiers": {"proven_minimum_mt_per_month": 38300, "current_30pct_enrollment_mt_per_month": 210000, "potential_90pct_enrollment_mt_per_month": 630000}, "premium_economics": {"epa_sbo_premium_pct": 3, "epa_sbo_premium_cpb": 2.25, "epa_sbean_premium_per_mt": 10, "epa_sbean_premium_per_bu": 0.26, "breakeven_logic": "Crushers profit from EPA premium as long as certified soybean premium < 26 cpb ($9.75/MT)"}, "scaling_dynamics": "Robust demand incentivizes additional infrastructure for tracking programs, expanding the total pool. CARBIO system is scalable -- main bottleneck is farmer documentation submission, not processing capacity.", "competition_for_certified_supply": "No other export destination requires field-level tracing. EU REDII (deforestation-free since 2008) is different standard. EU may designate SBO as high-risk feedstock by 2030, which would REDUCE competition for EPA-certified supply.", "source": "cresta_study_2022"}'::jsonb,
 'always', 'extracted');

-- Argentine SBO Refining Cost Chain
INSERT INTO core.kg_context (node_id, context_type, context_key, context_value, applicable_when, source) VALUES
((SELECT id FROM core.kg_node WHERE node_key = 'argentina.sbo_refining_margin'),
 'expert_rule', 'sbo_refining_cost_chain_argentina',
 '{"rule": "Argentine SBO refining follows a staged cost chain. Key insight: the specification needed for renewable diesel feedstock (neutralized/low-FFA) is achieved partway through the RBD process, so full RBD capacity is NOT required.", "cost_chain_cpb": {"crude_to_degummed": [0.15, 0.25], "degummed_to_neutralized": [0.10, 0.15], "total_crude_to_rd_spec": [0.25, 0.40], "total_crude_to_rbd": [0.50, 0.75]}, "capacity_hierarchy_mmt": {"crude_degummed": 11.3, "rbd_crusher": 1.75, "rbd_standalone": 0.383, "chemical_refining_for_low_ffa": 3.0}, "competitive_dynamics": {"sunflower_vs_sbo_refining_spread_delta_cpb": 0.50, "conversion_incentive_threshold": "SBO refining spread must rise >0.5 cpb above normal to justify converting sunflower capacity", "rbd_sbo_share_of_total_rbd": "less than 3%", "rbd_sbo_production_mmt": 0.3}, "key_insight": "Integrated biodiesel producers likely already have chemical refining capacity for low-FFA feedstock (~3 MMT). This capacity is directly applicable for RD-spec SBO without additional investment.", "source": "cresta_braya_study_2022"}'::jsonb,
 'always', 'extracted');

-- Argentine Farmer Hoarding as Supply Buffer
INSERT INTO core.kg_context (node_id, context_type, context_key, context_value, applicable_when, source) VALUES
((SELECT id FROM core.kg_node WHERE node_key = 'argentina_farmer_soy_hoarding'),
 'expert_rule', 'farmer_hoarding_supply_buffer',
 '{"rule": "Argentine farmers hold soybeans as dollar-denominated inflation hedge. This creates a unique supply buffer that smooths crush volumes across crop years, making Argentine SBO supply more reliable than production variability suggests.", "quantification": {"stocks_to_use_2020_21_pct": 44, "stocks_to_use_prior_year_pct": 26, "stocks_to_use_5yr_avg_pct": 19, "inflation_range_last_20yr": "6% to 100%+"}, "supply_smoothing_evidence": "Despite production falling 12 MMT over two years, 2021 crush was above 5yr avg and rose 4 MMT from prior year.", "implication": "For feedstock sourcing, Argentine SBO availability is more closely tied to crush capacity (~42 MMT steady) than to annual production (~50 MMT avg but volatile).", "sell_timing_hierarchy": ["1. Inflation rate (primary driver)", "2. Export tax rate changes", "3. Price level (including EPA premium)"], "policy_risk": "Government can force sales via programs like soy dollar exchange rate incentive, but EPA premium alone may not overcome inflation-driven hoarding.", "source": "cresta_study_2022"}'::jsonb,
 'always', 'extracted');

-- RIN as Margin Stabilizer Framework
INSERT INTO core.kg_context (node_id, context_type, context_key, context_value, applicable_when, source) VALUES
((SELECT id FROM core.kg_node WHERE node_key = 'ira_btc_transition_model'),
 'expert_rule', 'rin_margin_stabilizer_framework',
 '{"rule": "RINs are conceptualized as margin stabilizers -- they rise when biofuel production falls short of mandates and fall when production exceeds RVOs. The IRA transition amplifies this function.", "d4_rin_pricing_model": "Spread between heating oil and SBO prices + projected D6 RIN = primary D4 RIN components. Adjusted for ethanol blend wall risk and fundamental/policy developments.", "ira_transition_dynamics": {"btc_value": "$1/gal to blender (sometimes split with producer, no standard split)", "ira_excludes_imports": true, "imports_share_2021": "25%+ (675M gal)", "sbo_share_us_bbd_feedstock": "~50%", "ca_market_sbo_share": "less than 20%"}, "feedstock_shift_impossibility": {"sbo_50pct_to_20pct_gap_2021_blbs": 5.5, "fats_greases_needed_blbs": 7.0, "sbo_50pct_to_20pct_gap_2024_blbs": 9.0, "bbg_production_loss_2021_mgal": 740, "bbg_production_loss_2024_mgal": 1220}, "fats_greases_ceiling": "Non-biofuel industries (where fats/greases are small cost share) would outbid BBD producers if entire supply redirected. RIN prices must compensate.", "conclusion": "SBO remains irreplaceable as primary BBD feedstock. RIN values structurally bullish under IRA transition.", "source": "cresta_braya_study_2022"}'::jsonb,
 'always', 'extracted');

-- Come-by-Chance Competitive Position
INSERT INTO core.kg_context (node_id, context_type, context_key, context_value, applicable_when, source) VALUES
((SELECT id FROM core.kg_node WHERE node_key = 'come_by_chance_refinery'),
 'expert_rule', 'come_by_chance_competitive_position',
 '{"rule": "Come-by-Chance refinery using Argentine EPA-certified SBO and selling RD to California market has the highest relative profitability per tonne of SBO vs all alternative end uses. Demonstrated in every month Jan 2020 - Sep 2022.", "competitive_hierarchy": ["1. Come-by-Chance -> CA RD market (highest every month)", "2. Argentine biodiesel -> EU export (volatile, depends on destination incentives)", "3. Argentine SBO -> India/Asia crude export", "4. Argentine SBO -> domestic non-biofuel use (minimal volume)"], "margin_advantage_source": "Revenue stack = RD price + RINs + LCFS + BTC/IRA + Cap-n-Trade. Argentine SBO competitive because of low basis + EPA certification premium (3%) is small vs total revenue stack.", "risks": ["Argentine production shortfall (La Nina drought)", "Export tax volatility (short-term disruption only)", "Parana River water levels (logistics, not supply cutoff)", "EPA farmer enrollment pace", "IRA credit value for SBO vs lower-CI feedstocks"], "risk_mitigation": "Farmer hoarding buffer smooths crush volumes. 65% idle biodiesel capacity means declining domestic SBO demand. Sufficient margin to outbid alternative end users.", "source": "cresta_braya_study_2022"}'::jsonb,
 'always', 'extracted');

-- LCFS Credit Value Dynamics
INSERT INTO core.kg_context (node_id, context_type, context_key, context_value, applicable_when, source) VALUES
((SELECT id FROM core.kg_node WHERE node_key = 'come_by_chance_refinery'),
 'expert_rule', 'lcfs_carb_policy_evolution_2022',
 '{"rule": "CARB considering multiple LCFS changes (2022 timeline) that would impact BBD economics: (1) Accelerate CI reduction target from 20% to 25-30% by 2030, (2) Cap lipid-based feedstock volume (like EU 7% crop-based cap). These changes would raise LCFS credit prices but reduce credit generation.", "lipid_cap_impact": "7% cap on vegetable-oil-derived BBD would require replacing 1.4B lbs feedstock (2021), or 1.8B lbs fats/greases due to yield differential.", "credit_bank_buffer": "Large existing credit bank provides time buffer -- new policy would not cause massive immediate demand shift. Drawdown pace depends on individual obligated party credit balances.", "equilibrium_logic": "BBD producers ship to CA until credit value per gallon = marginal cost of production + shipping. Capacity expansion continues to pressure credit values regardless of CI target changes.", "food_vs_fuel": "Renewed food vs fuel debate from 2021-2022 inflation acceleration driving policy consideration.", "source": "cresta_braya_study_2022"}'::jsonb,
 'always', 'extracted');

-- Argentine Biodiesel Export Tax as Subsidy (US Trade Case)
INSERT INTO core.kg_context (node_id, context_type, context_key, context_value, applicable_when, source) VALUES
((SELECT id FROM core.kg_node WHERE node_key = 'argentina_export_tax'),
 'expert_rule', 'export_tax_differential_as_subsidy',
 '{"rule": "The differential between Argentine soybean (33%) and biodiesel (effective 23%) export tax rates was cited by US Trade Representative as an effective subsidy for biodiesel industry, leading to 72% countervailing duties + 75% anti-dumping duties in 2017.", "tax_history_effective_rates": {"macri_2019": {"soybeans": 24.7, "soybean_oil": 24.7, "biodiesel": 19.74}, "fernandez_2020": {"soybeans": 33, "soybean_oil": 31, "biodiesel_nominal": 29, "biodiesel_effective": 23.08}, "fernandez_2022": {"soybeans": 33, "soybean_oil": 33, "biodiesel_nominal": 30}}, "us_trade_impact": "US imports of Argentine biodiesel peaked at 442,400 MT in 2016 (last full year before duties). Monthly range 36,000-78,250 MT. Duties effectively closed US market -- no expected reversal.", "political_economy": "Government relies on export taxes for foreign reserves. Tax policy correlates with economic crisis cycle -- recessions drive increases. Current decade-high rates suggest limited further upside risk.", "farmer_impact": "Tax changes are 2nd most important factor (after inflation) for farmer sell timing.", "source": "cresta_study_2022"}'::jsonb,
 'always', 'extracted');

-- Argentine Crush Industry Concentration
INSERT INTO core.kg_context (node_id, context_type, context_key, context_value, applicable_when, source) VALUES
((SELECT id FROM core.kg_node WHERE node_key = 'argentina_soybean_complex_model'),
 'expert_rule', 'argentine_crush_industry_structure',
 '{"rule": "Argentine crush industry is highly concentrated. Top 8 = ~80% capacity, top 14 = 96%. Seven of top 13 crushers are also largest biodiesel producers (68% of BD capacity). Most large operations are integrated crush-port-biodiesel on Parana River.", "top_5_crushers": {"renova_sa": {"owner": "JV Viterra/Vicentin", "biodiesel_tpd": 1450, "sbo_refine_tpd": 900, "note": "Largest biodiesel facility"}, "cargill": {"plants": 3, "crush_tpd": [2200, 6000, 13000], "port_terminals": 5}, "molinos_agro": {"crush_tpd": 20000, "sbo_output_tpd": 4000, "note": "Largest single-facility crush capacity"}, "terminal_6": {"owner": "JV AGD/Bunge", "crush_tpd": 20000, "biodiesel_tpd": 1500, "sbo_refined_tpd": 2000}, "ldc_argentina": {"crush_tpd": 20000, "biodiesel_tpa": 600000, "epa_certified": true, "note": "Third-party verified deforestation-free supply chain"}}, "extraction_types": {"chemical_only_pct_of_capacity": 85.6, "physical_only_tpa": 139500, "dual_process_mmt": 9.24}, "epa_implication": "Integrated crush-port-biodiesel operators in Rosario are already familiar with EPA certification process and likely have compliance procedures in place.", "source": "cresta_study_2022"}'::jsonb,
 'always', 'extracted');

-- Argentine SBO Demand Hierarchy
INSERT INTO core.kg_context (node_id, context_type, context_key, context_value, applicable_when, source) VALUES
((SELECT id FROM core.kg_node WHERE node_key = 'soybean_oil'),
 'expert_rule', 'argentine_sbo_demand_hierarchy',
 '{"rule": "Argentine SBO demand follows a clear hierarchy: exports dominate, biodiesel second, domestic food last. Understanding the hierarchy is critical for assessing EPA-certified SBO availability.", "demand_breakdown_mmt": {"total_production": 8.5, "exports": 5.0, "biodiesel_sbo_use": 1.7, "domestic_nonbiofuel": 0.325}, "export_destinations": {"india_share_pct": 50, "india_avg_mmt": 2.5, "bangladesh_mmt": 0.5, "peru_mmt": 0.325, "crude_degummed_share_pct": 98}, "key_insight": "Domestic non-biofuel demand (325K MT) is so small relative to production that no domestic demand increase could interrupt EPA-certified SBO supply. Export demand is the primary competitor, but RD revenue stack can outbid traditional export markets.", "biodiesel_threat_assessment": "65% idle capacity, US market closed by duties, EU demand declining long-term. Biodiesel is shrinking source of SBO demand, freeing more for export.", "source": "cresta_study_2022", "geography": "argentina"}'::jsonb,
 'always', 'extracted');


-- ============================================================================
-- 4. SOURCE REGISTRY: Register all processed documents
-- ============================================================================

INSERT INTO core.kg_source (
    source_key, source_type, title, location_uri, document_date, document_type,
    commodities, topics, status, first_processed, last_processed,
    nodes_extracted, edges_extracted, contexts_extracted
) VALUES
('cresta_feedstock_study_final_10202022', 'local_file',
 'Argentine EPA Certified SBO Feedstock Availability - Final Draft - 10202022',
 'C:/Users/torem/RLC Dropbox/Tore Alden/Jacobsen/Projects/Cresta/Argentine EPA Certified SBO Feedstock Availability - Final Draft - 10202022.docx',
 '2022-10-19', 'consulting_report',
 '{soybean_oil,soybeans,biodiesel}',
 '{epa_certification,carbio,argentina_crush,export_tax,sbo_supply,biodiesel_mandate,parana_river,rosario,la_nina,farmer_hoarding,india_sbo_imports}',
 'completed', NOW(), NOW(), 8, 7, 5)
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
('braya_refining_study_10252022', 'local_file',
 'Braya - Argentine SBO Refining Study - First Draft',
 'C:/Users/torem/RLC Dropbox/Tore Alden/Jacobsen/Projects/Cresta/Braya - Argentine SBO Refining Study - First Draft.docx',
 '2022-10-25', 'consulting_report',
 '{soybean_oil,renewable_diesel}',
 '{refining_cost_chain,rbd_capacity,degummed_neutralized,chemical_vs_physical_extraction,sunflower_competition,regional_margin_comparison,lcfs,rin,btc,ira,come_by_chance}',
 'completed', NOW(), NOW(), 4, 6, 4)
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
('braya_credit_price_forecast_10252022', 'local_file',
 'Braya - Follow-Up Project - Credit Price Forecast',
 'C:/Users/torem/RLC Dropbox/Tore Alden/Jacobsen/Projects/Cresta/Braya - Follow-Up Project - Credit Price Forecast.docx',
 '2022-10-25', 'consulting_report',
 '{renewable_diesel,biodiesel}',
 '{rin_pricing,lcfs_credit,btc_ira_transition,feedstock_substitution,fats_greases_ceiling,carb_policy}',
 'completed', NOW(), NOW(), 1, 2, 2)
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
('cresta_top5_crushers', 'local_file',
 'Top 5 Crusher Profiles',
 'C:/Users/torem/RLC Dropbox/Tore Alden/Jacobsen/Projects/Cresta/Top 5 Crusher Profiles.docx',
 '2022-06-03', 'reference_data',
 '{soybeans,soybean_oil,biodiesel}',
 '{argentina_crushers,renova,cargill,molinos,terminal6,ldc,crush_capacity}',
 'completed', NOW(), NOW(), 4, 0, 1)
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
('cresta_sow_05232022', 'local_file',
 'Argentine EPA Certified SBO Feedstock Study - SOW',
 'C:/Users/torem/RLC Dropbox/Tore Alden/Jacobsen/Projects/Cresta/Argentine EPA Certified SBO Feedstock Study - SOW.docx',
 '2022-05-23', 'scope_of_work',
 '{soybean_oil}',
 '{project_scope,feedstock_study}',
 'completed', NOW(), NOW(), 0, 0, 0)
ON CONFLICT (source_key) DO UPDATE SET
    status = 'completed', last_processed = NOW();

INSERT INTO core.kg_source (
    source_key, source_type, title, location_uri, document_date, document_type,
    commodities, topics, status, first_processed, last_processed,
    nodes_extracted, edges_extracted, contexts_extracted
) VALUES
('cresta_followup_sow_09222022', 'local_file',
 'Follow-Up Project Scope of Work - Sep 22',
 'C:/Users/torem/RLC Dropbox/Tore Alden/Jacobsen/Projects/Cresta/Follow-Up Project Scope of Work - Sep 22.docx',
 '2022-09-22', 'scope_of_work',
 '{soybean_oil,renewable_diesel}',
 '{refining_study,margin_comparison,credit_forecasts}',
 'completed', NOW(), NOW(), 0, 0, 0)
ON CONFLICT (source_key) DO UPDATE SET
    status = 'completed', last_processed = NOW();

INSERT INTO core.kg_source (
    source_key, source_type, title, location_uri, document_date, document_type,
    commodities, topics, status, first_processed, last_processed,
    nodes_extracted, edges_extracted, contexts_extracted
) VALUES
('cresta_epa_paperwork_translated', 'local_file',
 'EPA-Certification Paperwork - Translated (COFCO)',
 'C:/Users/torem/RLC Dropbox/Tore Alden/Jacobsen/Projects/Cresta/EPA-Certification Paperwork - Translated.docx',
 '2022-10-21', 'reference_data',
 '{soybeans}',
 '{epa_certification,cofco,waybill,segregation,go_areas}',
 'completed', NOW(), NOW(), 0, 0, 0)
ON CONFLICT (source_key) DO UPDATE SET
    status = 'completed', last_processed = NOW();

INSERT INTO core.kg_source (
    source_key, source_type, title, location_uri, document_date, document_type,
    commodities, topics, status, first_processed, last_processed,
    nodes_extracted, edges_extracted, contexts_extracted
) VALUES
('cresta_misc_commentary', 'local_file',
 'Misc Commentary from Initial Report',
 'C:/Users/torem/RLC Dropbox/Tore Alden/Jacobsen/Projects/Cresta/Misc Commentary from Initial Report.docx',
 '2022-05-26', 'notes',
 '{soybean_oil}',
 '{rfs,epa_certification,biomass_tracking}',
 'completed', NOW(), NOW(), 0, 0, 0)
ON CONFLICT (source_key) DO UPDATE SET
    status = 'completed', last_processed = NOW();


-- ============================================================================
-- END OF BATCH 012
-- ============================================================================
