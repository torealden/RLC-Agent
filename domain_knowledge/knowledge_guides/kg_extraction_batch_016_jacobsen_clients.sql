-- ============================================================================
-- RLC Knowledge Graph: Extraction Batch 016 (Jacobsen Client Projects)
-- Source: Consulting project files from Jacobsen/Fastmarkets client engagements
-- Folder: C:/Users/torem/RLC Dropbox/Tore Alden/Jacobsen/Projects/
-- Extracted: 2026-04-16
-- Scope:  PBF Chalmette feedstock availability study (Partners Group),
--         ICF/TEI Oleo-X technical diligence (Pascagoula FOG pretreatment),
--         Refining capacity analysis (AGP/multi-client),
--         Marathon global feedstock pricing model framework,
--         Zenith Joliet terminal feedstock SOW,
--         Warburg-Pincus Montana Renewables feedstock SOW,
--         EcoEngineers Canadian biofuel feedstock study,
--         Unilever liquid oils market intelligence,
--         Multi-client 20-year feedstock outlook,
--         Expert interview frameworks (biofuels trade routes, CI, canola)
-- Clients: Partners Group/PBF, ICF/TEI, AGP, Marathon, Zenith Energy,
--          Warburg-Pincus, EcoEngineers, Unilever, BHP, McKinsey, Shell, Stepan
-- ============================================================================

-- KEY FINDINGS:
--   * PBF Chalmette: 650-mile radius feedstock analysis for Gulf Coast RD plant
--     - SBO refining capacity in draw area: ~3.5B lbs (2023), growing to 4.5B lbs
--     - 650-mile radius shows deficits for all feedstocks; Midwest/barge sourcing essential
--     - Demand elasticity hierarchy: oleochemical > food mfg > RD > biodiesel > bottling > food service
--     - Total US SBO demand (2023): 24.3B lbs vs production 23.1B lbs = structural deficit
--     - Argentine SBO import risk: 19% US tariff, declining Argentine crush capacity, farmer hoarding
--     - UCO had highest margins; tallow margins worst due to record prices
--     - Barge logistics preferred: lower cost, utilization-rate-dependent pricing, Gulf port advantage
--     - RD price = ULSD forward curve + RD-ULSD spread + credit values
--   * ICF/TEI Oleo-X: Feedstock pretreatment plant technical diligence (Pascagoula)
--     - US BBD feedstock demand: 18B lbs (2021) -> 26B+ lbs (2026)
--     - SBO refining margins: historically 2.5-3 cpb, peaked 15+ cpb (2021-2022)
--     - Refining capacity shortage drove record margins; expansion to normalize 6-8 cpb
--     - Available FOG supply: 58B lbs (2024) -> 60B+ lbs (2026)
--     - Tolling arrangement advantage over spot market during high-volatility periods
--     - HPAI disease risk: avian flu can eliminate 3%+ of poultry fat supply rapidly
--   * Refining Capacity: Soybean oil refining shortage driving industry restructuring
--     - US crush capacity: 2.2B bushels; refining only covers portion
--     - RD plants adding front-end refining: ~5.6B lbs potential capacity
--     - Biofuel share of SBO demand: <50% (2020/21) -> 50%+ (2021/22+)
--     - Expansion cycle: crush -> refining -> RD capacity -> feedstock shortage -> repeat
--   * Marathon: Feedstock pricing staircase model -- each incremental 10-20 MBPD demand
--     creates new price equilibrium. 100+ MBPD additional RD capacity expected in 5 years.
--   * Multi-Client Study: 20-year feedstock forecast for 11 feedstocks, 3 scenarios
--   * EcoEngineers: Canadian CFS impact on cross-border feedstock trade flows


-- ============================================================================
-- 1. NODES: Facilities, Models, Concepts
-- ============================================================================

-- Facility: PBF Chalmette Renewable Diesel Plant
INSERT INTO core.kg_node (node_type, node_key, label, properties) VALUES
('facility', 'pbf_chalmette_rd', 'PBF Chalmette Renewable Diesel Plant',
 '{"location": "Chalmette, Louisiana (Gulf Coast)", "owner": "PBF Energy (investor: Partners Group)", "type": "Renewable diesel production", "region": "Gulf Coast", "study_date": "2022-09", "draw_radius_miles": 650, "competitive_position": "4th largest renewable feedstock buyer in Gulf market", "logistics_advantage": "Close to one of largest energy/agricultural ports in US. Barge access via Mississippi River. Multiple rail lines.", "feedstock_strategy": "SBO primary feedstock, supplemented by DCO, tallow, UCO. Midwest/barge sourcing needed beyond 650-mile draw area due to local deficits.", "barge_logistics": {"30000_barrel_barges": true, "shipments_per_month": 2, "cost_advantage": "Barge operators price based on utilization rates -- backhaul on ag/energy port guarantees competitive rates", "risk": "Low water Nov-Jan, longer transit vs rail"}, "all_feedstocks_profitable_all_years": true, "100pct_ca_shipment_assumed": true}'::jsonb)
ON CONFLICT (node_key) DO UPDATE SET properties = EXCLUDED.properties;

-- Facility: Oleo-X Pascagoula Pretreatment Plant
INSERT INTO core.kg_node (node_type, node_key, label, properties) VALUES
('facility', 'oleox_pascagoula', 'Oleo-X Pascagoula FOG Pretreatment Plant',
 '{"location": "Pascagoula, Mississippi", "owner": "TEI Renewable Energy (Time Equities)", "type": "Oilseed processing / FOG pretreatment for renewable diesel", "technology": "Oleo-X proprietary process", "offtake_partner": "BP Products North America Inc.", "study_date": "2022-05", "purpose": "Pretreat fats, oils, and greases to meet RD feedstock specifications. Tolling arrangement with BP.", "product_spec": "Meets renewable diesel producer specifications", "financial_model_reviewed": "Pascagoula Financial Model - BP Only April 22 2022", "key_risk": "Offtake term sheet one-sided toward BP; performance/underperformance terms lacking; delivery point clarification needed"}'::jsonb)
ON CONFLICT (node_key) DO UPDATE SET properties = EXCLUDED.properties;

-- Facility: Zenith Joliet Terminal
INSERT INTO core.kg_node (node_type, node_key, label, properties) VALUES
('facility', 'zenith_joliet_terminal', 'Zenith Joliet Renewables Terminal',
 '{"location": "1035 W. Laraway Road, Joliet, Illinois", "owner": "Zenith Energy Renewables", "type": "Renewable fuel feedstock terminal/storage", "study_date": "2021-09", "draw_radius_miles": 300, "feedstocks_studied": ["DCO from ethanol plants", "CWG and tallow from renderers", "UCO", "SBO (crude and refined)"], "deliverables": "Feedstock supply/demand balance sheets 2021-2024, plant maps with rail lines, regional S&D projections", "fee": "$100,000 (50/50 signing/completion)", "study_duration_weeks": 6}'::jsonb)
ON CONFLICT (node_key) DO UPDATE SET properties = EXCLUDED.properties;

-- Facility: Montana Renewables (Warburg-Pincus)
INSERT INTO core.kg_node (node_type, node_key, label, properties) VALUES
('facility', 'montana_renewables', 'Montana Renewables (Great Falls)',
 '{"location": "Great Falls, Montana", "investor": "Warburg-Pincus", "type": "Renewable diesel production", "study_date": "2022", "draw_radius_miles": 750, "feedstocks_studied": ["DCO from ethanol plants", "CWG and tallow (beef) from renderers", "UCO and brown grease", "SBO (crude and refined)", "Canola/camelina oil"], "study_scope": ["Feedstock supply/availability within 750-mile radius", "Plant maps with capacity/rail info", "S&D balance sheets 2021-2025", "Benchmark pricing with logistics costs", "Market upgrading costs by feed type", "Market $/mile transportation figures"]}'::jsonb)
ON CONFLICT (node_key) DO UPDATE SET properties = EXCLUDED.properties;

-- Model: Feedstock Availability Radius Analysis
INSERT INTO core.kg_node (node_type, node_key, label, properties) VALUES
('model', 'feedstock_radius_analysis_model', 'Feedstock Availability Radius Analysis Model',
 '{"description": "Consulting methodology for assessing renewable fuel feedstock availability within a defined radius of a proposed or existing facility. Used across 6+ client engagements (PBF, Zenith, Montana Renewables, Warburg-Pincus, generic SOW template).", "methodology": {"step_1": "Identify all producers of primary RD feedstocks within draw radius (SBO crushers, ethanol plants, renderers, UCO aggregators)", "step_2": "Estimate production capacity per plant based on proprietary data and industry knowledge", "step_3": "For imports: (plant capacity / national capacity) * national import projection", "step_4": "For UCO: US production estimate * (population in radius / US population)", "step_5": "Demand split into biofuel (facility-by-facility estimates + BD historical mix) and non-biofuel (same population methodology)", "step_6": "Project BD rationalization as RD capacity grows"}, "typical_radii_miles": [300, 650, 750], "expansion_trigger": "When initial radius shows deficit, expand to Midwest or 75% of US population", "key_finding_pattern": "Nearly all studies show initial radius deficits -- facilities must source beyond local area. Midwest/barge access is critical differentiator.", "logistics_framework": {"rail_cost_cpb": 2.5, "barge_cost_advantage": "Lower than rail, utilization-rate-dependent pricing", "barge_risk": "Low water Nov-Jan, longer transit"}, "source": "pbf_study_2022, zenith_proposal_2021, warburg_pincus_sow_2022"}'::jsonb)
ON CONFLICT (node_key) DO UPDATE SET properties = EXCLUDED.properties;

-- Model: Feedstock Demand Elasticity Hierarchy
INSERT INTO core.kg_node (node_type, node_key, label, properties) VALUES
('model', 'feedstock_demand_elasticity_model', 'Feedstock Demand Elasticity Hierarchy Model',
 '{"description": "Framework for ranking feedstock end-use categories by price elasticity of demand. Lower elasticity = more resistant to being priced out. Critical for assessing which demand segments will lose access to feedstock as BBD demand grows.", "hierarchy_lowest_to_highest_elasticity": ["1. Oleochemical (industrial chemicals -- feedstock is small share of end product cost, very inelastic)", "2. Food manufacturers / consumer products (brand margins absorb price increases)", "3. Renewable diesel (credit revenue stack provides margin buffer)", "4. Biodiesel (lower credit revenue, more price sensitive than RD)", "5. Food manufacturing / vegetable oil bottling (commodity margins, price competitive)", "6. Food service (restaurants, institutional -- most price sensitive)"], "total_us_sbo_demand_2023_blbs": 24.273, "us_sbo_production_2023_blbs": 23.119, "implication": "Production < total demand = structural deficit. Lowest-elasticity users (oleochemical, food CPG) insulated. Highest-elasticity users (food service) squeezed first. RD sits in middle -- credit stack determines competitive position vs food manufacturers.", "key_insight": "As RD capacity expands, food service and vegetable oil bottlers lose access to SBO first. Oleochemical demand is essentially inelastic and sets a floor on available supply being redirected to BBD.", "source": "pbf_study_2022"}'::jsonb)
ON CONFLICT (node_key) DO UPDATE SET properties = EXCLUDED.properties;

-- Model: Feedstock Pricing Staircase Model (Marathon)
INSERT INTO core.kg_node (node_type, node_key, label, properties) VALUES
('model', 'feedstock_pricing_staircase_model', 'Feedstock Pricing Staircase Model',
 '{"description": "Framework for projecting how each incremental 10-20 MBPD of new RD demand creates a new feedstock price equilibrium. Commissioned by Marathon Petroleum.", "methodology": {"step_1": "Quantify worldwide FOG production by category (SBO, canola, corn oil, animal fats, UCO, CWG)", "step_2": "Subtract food/consumer demand to get BBD-available supply", "step_3": "Model incremental demand in 10-20 MBPD tranches", "step_4": "For each tranche, determine new price equilibrium for each feedstock", "step_5": "Identify when LCFS credit price becomes dominant investment driver", "step_6": "Identify when RIN price has nominal impact on economics"}, "commissioned_by": "Marathon Petroleum", "study_date": "2022", "demand_growth_assumption": "100+ MBPD additional RD capacity over next 5 years", "additional_feedstock_sources_modeled": ["Additional soybean/canola crush capacity buildout (5 and 10 year)", "Productivity gains for corn/soybean/canola yields", "Cover crops as new feedstock source", "Currently uncollected UCO"], "scenarios": "Multiple with clearly defined assumptions for each", "key_question": "At what demand level does the LCFS credit become the marginal driver for capacity investment vs RIN?", "source": "marathon_sow_2022"}'::jsonb)
ON CONFLICT (node_key) DO UPDATE SET properties = EXCLUDED.properties;

-- Model: SBO Refining Capacity Shortage Model
INSERT INTO core.kg_node (node_type, node_key, label, properties) VALUES
('model', 'sbo_refining_capacity_shortage_model', 'US SBO Refining Capacity Shortage Model',
 '{"description": "Framework for analyzing the structural mismatch between US soybean crushing capacity and vegetable oil refining capacity, driven by RD demand growth.", "key_data_2021": {"us_crush_capacity_bushels_b": 2.2, "implied_sbo_production_capacity_blbs": 25.5, "sbo_refined_by_crushers_1920_blbs": 18.4, "sbo_total_production_1920_blbs": 25.0, "rd_refining_capacity_blbs": 5.6, "rd_annual_production_capacity_jan2021_mgy": 894, "rd_monthly_production_mgy": 62}, "expansion_announcements_2021": [{"company": "Cargill", "action": "Doubling soybean processing in Sidney OH, +10% Cedar Rapids IA"}, {"company": "ADM", "action": "Efficiency improvements at existing plants; new 52.5M bu crush plant in Spiritwood ND by 2023"}, {"company": "Bunge", "action": "Increased tank storage, refinery efficiency improvements"}, {"company": "CHS", "action": "+30% crush capacity Fairmont MN (55M to 71.5M bu), late 2021"}, {"company": "Shell Rock Soy Processing", "action": "New 38.5M bu plant, 2022. Offtake agreement with Phillips 66 for 4,000 tpd SBO"}, {"company": "ADM Spiritwood ND", "action": "52.5M bu/yr, completion before 2023 harvest, includes refining"}], "demand_structure_shift": {"biofuel_share_2020_pct": "less_than_50", "biofuel_share_2022_pct": "50_plus", "turning_point": "2020/21 likely last year biofuel < 50% of SBO demand"}, "refining_margin_history_cpb": {"historical_range": [2.5, 3.0], "peak_2021_cpb": 15, "forecast_normalization_cpb": [6, 8]}, "cycle_dynamics": "Expansion follows cycle: RD demand -> refining shortage -> margin spike -> crush/refine investment -> capacity addition -> margin normalization -> next RD expansion wave. Periods of overshooting and undershooting.", "canola_west_coast": "Canola crushing may expand on West Coast NA to serve Pacific RD market -- Canadian Prairie canola + existing infrastructure.", "source": "refining_capacity_report_2021, agp_deliverables_2022"}'::jsonb)
ON CONFLICT (node_key) DO UPDATE SET properties = EXCLUDED.properties;

-- Model: Feedstock Hedging Limitation Model
INSERT INTO core.kg_node (node_type, node_key, label, properties) VALUES
('model', 'feedstock_hedging_limitation_model', 'Feedstock Hedging Limitation Framework',
 '{"description": "Framework for understanding the structural limitations of hedging renewable diesel feedstock price risk.", "limitations": {"limited_instruments": "Effective hedging tools for fats and grease prices are limited. Some vendors write OTC swaps/options but markets are thin.", "volatility_cost": "Historically high volatility makes hedging expensive -- option premiums elevated.", "counterparty_limits": "Many institutions writing OTC products will limit risk by capping notional volume per counterparty.", "basis_risk": "Cash-to-futures basis for fats/greases is poorly correlated with exchange-traded instruments (SBO futures)", "correlation_breakdown": "SBO-to-fat/grease price correlations can break during extreme moves"}, "emerging_tools": "Growing participation of major refinery companies (Marathon, Phillips 66, Valero) expected to force development of better hedging instruments.", "implication_for_margin_analysis": "Margin projections assume spot market execution. Actual margins will differ based on hedging program effectiveness and cost. Tolling arrangements can reduce feedstock price exposure but introduce counterparty risk.", "source": "pbf_study_2022, icf_oleox_2022"}'::jsonb)
ON CONFLICT (node_key) DO UPDATE SET properties = EXCLUDED.properties;

-- Concept: Food vs Fuel Policy Risk
INSERT INTO core.kg_node (node_type, node_key, label, properties) VALUES
('concept', 'food_vs_fuel_policy_risk', 'Food vs Fuel Policy Risk for BBD',
 '{"description": "Framework for assessing the risk that food-versus-fuel policy changes reduce vegetable oil demand from BBD industry.", "us_assessment": "As long as EPA-mandated volumes do not decline substantially, insufficient alternative feedstock volumes exist to significantly impact SBO demand. Policy would only redirect a small share of demand from SBO to fats/greases -- but fats/greases supply ceiling is 7-9B lbs below what would be needed to replace SBO share.", "california_assessment": "CARB considering lipid cap (similar to EU 7% crop-based cap). Impact limited because SBO already <20% of CA BBD feedstock mix. Losing LCFS credits for SBO-derived BBD could slow RD capacity expansion but not reverse it.", "eu_assessment": "REDII caps crop-based biofuels at 2020 levels (max 7%). SBO may be designated high-risk feedstock by 2030. Impact redirects SBO from EU biofuel to food/export market.", "ethanol_precedent": "Ethanol industry survived robust food-vs-fuel debate. Despite political pressure, RFS mandates remained. Same pattern likely for BBD but with longer transition period due to fewer feedstock alternatives.", "food_price_trigger": "Renewed debate intensifies when food price inflation accelerates (2021-2022 was trigger event).", "source": "pbf_study_2022, icf_oleox_2022, expert_interviews_2022"}'::jsonb)
ON CONFLICT (node_key) DO UPDATE SET properties = EXCLUDED.properties;

-- Concept: Canadian CFS Feedstock Impact
INSERT INTO core.kg_node (node_type, node_key, label, properties) VALUES
('concept', 'canada_cfs_feedstock_impact', 'Canada Clean Fuel Standard Impact on Feedstock Markets',
 '{"description": "Framework for analyzing how Canadas Clean Fuel Standard (CFS) reshapes North American feedstock trade flows.", "study_outline": {"scope": "Canadian feedstock market overview, historical use, CI focus, lessons from CA LCFS, CFS sufficiency question", "feedstocks_covered": ["Canola oil (supply/demand/economics)", "Tallow (supply constraints, demand, economics)", "CWG (estimating supply, growth potential)", "UCO (estimating supply, growth potential)", "Advanced feedstocks (camelina, others)"], "cross_border_analysis": {"canadian_to_us": ["Canola oil", "Tallow", "UCO"], "us_to_canadian": ["UCO", "Tallow", "CWG", "SBO"]}, "key_questions": ["Will Canada have sufficient low-CI feedstock for CFS?", "How does CI scoring drive feedstock preference?", "What are comparative economics across borders?"]}, "canola_focus": {"acreage_expansion_potential": true, "yield_potential": true, "crushing_capacity_growth": true, "rd_production_economics": true}, "policy_context": ["RFS (US)", "LCFS (CA/OR/BC)", "CFS (Canada national)", "Emerging state LCFS (WA, potential NY/Midwest)"], "source": "ecoengineers_outline_2022"}'::jsonb)
ON CONFLICT (node_key) DO UPDATE SET properties = EXCLUDED.properties;

-- Model: Multi-Client 20-Year Feedstock Outlook Framework
INSERT INTO core.kg_node (node_type, node_key, label, properties) VALUES
('model', 'multiclient_20yr_feedstock_outlook', 'Multi-Client 20-Year Feedstock Outlook Framework',
 '{"description": "Comprehensive 20-year feedstock supply/demand forecast methodology used for multi-client consulting product.", "scope": {"north_american_feedstocks": ["Tallow", "DCO", "Yellow grease / UCO", "CWG", "Poultry fat", "SBO", "Canola oil"], "global_feedstocks": ["Palm oil", "SBO (global)", "Rapeseed oil", "Tropical oils"]}, "deliverables_per_feedstock": ["Production forecasts", "BBD availability (3 scenarios over 20 years)", "Current availability and pricing assessment", "20-year price forecast ranges (3 scenarios)", "Key pricing formulas for contracts", "Historical pricing mechanisms and prospective outlook", "Hedging availability and correlations", "Significant market inputs (USDA correlations, reports watched)", "General supply terms and conditions", "Top suppliers/locations with asset descriptions", "Yields based on general feed"], "biofuel_deliverables": ["Additional by-products", "New market developments (tall oil, fish oil)", "Biodiesel market outlook", "Renewable diesel market outlook", "Co-processing market outlook"], "format": {"narrative_report": "50 pages including graphs/charts (management primer)", "presentation": "250-300 slides with detailed 20-year S&D forecasts", "conference_call": "2-hour review with Jacobsen team"}, "pricing": {"multi_client": "$35,000", "team_visit": "$15,000 additional"}, "source": "multiclient_sell_sheet_2019"}'::jsonb)
ON CONFLICT (node_key) DO UPDATE SET properties = EXCLUDED.properties;

-- Concept: RD Price Projection Methodology
INSERT INTO core.kg_node (node_type, node_key, label, properties) VALUES
('concept', 'rd_price_projection_methodology', 'Renewable Diesel Price Projection Methodology',
 '{"description": "Framework for projecting renewable diesel prices, used across multiple consulting engagements.", "components": {"base_price": "ULSD forward curve", "spread_adjustment": "RD-to-ULSD historical spread (renewable premium)", "energy_price_sensitivity": "Adjusted for projected energy price changes"}, "credit_value_categories": {"variable_daily": ["LCFS credits", "RIN prices (D4, D6)"], "stable": ["BTC ($1/gal)", "Cap-and-Trade credits"]}, "rin_pricing_model": {"inputs": ["D6 credit value projection", "SBO-to-heating oil spread", "Feedstock prices"], "mechanism": "RINs function as margin stabilizers -- rise when production falls short of mandates, fall when production exceeds RVOs"}, "lcfs_pricing_model": {"inputs": ["Credit generation forecast", "Deficit generation forecast", "CARB policy change scenarios"], "carb_changes_considered_2022": ["CI reduction target increase (20% to 25-30%)", "Lipid feedstock volume cap", "Crop-based biofuel limitations"]}, "ira_clean_fuel_credit": {"years": "2025-2027", "threshold": "40% GHG reduction vs petroleum diesel", "key_change": "Excludes imported biofuels (unlike BTC)"}, "margin_analysis": "Two perspectives: (1) production margin ex-credits, (2) production margin with credits. Spread = policy risk exposure.", "source": "pbf_study_2022, icf_oleox_2022"}'::jsonb)
ON CONFLICT (node_key) DO UPDATE SET properties = EXCLUDED.properties;

-- Concept: Expert Interview Frameworks (Biofuels Trade Routes)
INSERT INTO core.kg_node (node_type, node_key, label, properties) VALUES
('concept', 'biofuel_trade_route_shifts_2022', 'Biofuel Feedstock Trade Route Shifts (2022 Expert View)',
 '{"description": "Expert analytical framework for major shifts in biofuels feedstock trade routes, circa mid-2022.", "trade_route_shifts": {"palm_sbo_substitution": "Palm-SBO spread above long-term average, driving smaller developing countries in Western Hemisphere to substitute palm for SBO.", "us_sbo_exports_decline": "US SBO shipments continue declining, creating opportunities for Argentine exporters. But Argentine crush margins under pressure from sharp rise in US soybean meal exports.", "us_record_imports": "US needs more SBO than domestic production -> record imports projected unless Congress acts to remove import tariffs or BBD production rationalizes."}, "russia_ukraine_impact": {"global_veg_oil_trade": "Disruption to sunflower oil exports from Black Sea region", "us_export_impact": "US export volumes affected by redirection of global trade flows"}, "regulation_favorites": {"vegetable_oils_ci_scores": {"sbo_avg": 55, "canola_avg": 53, "fats_greases_avg": 30, "yellow_grease": 25, "uco_avg": 20}, "canola_rin_status_2022": "Does not yet produce RINs for RD under RFS. Pathway approval expected by end of 2022.", "sbo_ci_improvement": "Efforts to reduce SBO CI score likely to have minimal impact -- fats/greases continue favored under LCFS."}, "crop_forecasts_2022": {"us_soy": "Soil moisture favorable but developing shortages need timely rains. Outlook turning hotter/drier for pollination window.", "canola": "Conditions greatly improved from 2021 drought. Low carryout stocks limit crushing volumes. Canadian crush likely near record 10.4 MMT from improved production.", "sbo_price": "RBD peaked ~$1/lb in late April 2022. Prices dropped on recession fears but drove biodiesel margins to record levels above $4/gal."}, "source": "expert_interviews_july_2022"}'::jsonb)
ON CONFLICT (node_key) DO UPDATE SET properties = EXCLUDED.properties;

-- Concept: Unilever Liquid Oils Market Intelligence Scope
INSERT INTO core.kg_node (node_type, node_key, label, properties) VALUES
('concept', 'liquid_oils_market_intelligence', 'Liquid Oils Market Intelligence Framework (CPG Procurement)',
 '{"description": "Framework for providing market intelligence to consumer products companies (e.g., Unilever) on liquid oils procurement.", "us_scope": ["Crush and refining capacities (current + planned)", "Historical and projected crush margins (cash and board)", "Expected crush rates", "Physical SBO pricing in IA, IL markets", "Physical canola pricing in IL", "Key demand drivers: biodiesel, RD, RINs, food use", "Spot and forward bids/offers for SBO and canola"], "eu_scope": ["Main liquid oils refineries in Europe (capacity + actual production)", "Africa refineries and import flows to EU", "Non-GMO SBO market: supply/demand, European production vs Brazil imports", "Soybean basis for Europe (Brazil-linked)", "Ukraine suppliers overview", "RSO/SFO/SBO market drivers in EU", "GMO vs non-GMO price correlation", "CBOT/basis vs EU market correlation", "Sustainable/deforestation-free premium analysis"], "key_insight": "CPG companies are both competitors and customers in feedstock markets. They need forward visibility on (1) BBD industry absorbing increasing share of SBO, (2) basis/premium shifts between regions, (3) non-GMO/sustainability premiums affecting procurement cost.", "focus_markets": ["Iowa (IA)", "Illinois (IL)", "Export", "Toronto (canola)"], "source": "unilever_study_2022"}'::jsonb)
ON CONFLICT (node_key) DO UPDATE SET properties = EXCLUDED.properties;

-- Concept: UCO Market Research Framework
INSERT INTO core.kg_node (node_type, node_key, label, properties) VALUES
('concept', 'uco_global_market_research_framework', 'UCO Global Market Research Framework',
 '{"description": "Framework for comprehensive UCO market research commissioned by Unilever. UCO is a by-product of restaurant/food service industry, making it uniquely sensitive to economic activity levels.", "production_drivers": ["GDP growth rate (primary)", "Restaurant industry activity", "Pandemic impacts on food service", "Collection infrastructure expansion"], "risk_factors": ["Economic recession reduces restaurant activity -> UCO supply drops", "Quality variability across collection sources", "Contamination risk from mixed waste oil sources"], "supply_estimation_methodology": "US UCO production = f(GDP growth, population, food service spending). Draw area estimation uses population share methodology.", "uco_market_characteristics": {"lowest_ci_commonly_available": true, "ci_score_avg": 20, "highest_bbd_margin_feedstock": true, "quality_variability": "High -- varies by collection source, mixed oils", "contamination_risk": "Quality issues have driven some RD producers away from UCO despite margin advantage"}, "source": "unilever_uco_brief_2022, pbf_study_2022"}'::jsonb)
ON CONFLICT (node_key) DO UPDATE SET properties = EXCLUDED.properties;


-- ============================================================================
-- 2. EDGES: Causal relationships, supply chain links, competition
-- ============================================================================

-- PBF Chalmette competes for Gulf feedstock
INSERT INTO core.kg_edge (source_node_id, target_node_id, edge_type, weight, properties, created_by, confidence) VALUES
((SELECT id FROM core.kg_node WHERE node_key = 'pbf_chalmette_rd'),
 (SELECT id FROM core.kg_node WHERE node_key = 'diamond_green_diesel'),
 'COMPETES_WITH', 0.9,
 '{"mechanism": "PBF Chalmette is 4th largest renewable feedstock buyer in Gulf market. Competes with Diamond Green Diesel (Norco, LA -- 400 MGY expansion 2022) and other Gulf Coast RD plants for SBO, tallow, and UCO supplies within same draw area. Gulf region estimated 1.72B gal/yr RD capacity.", "competitive_advantage": "Location near major agricultural port. Barge market access. Ability to leverage large feedstock volumes for strategic partnerships.", "source": "pbf_study_2022"}'::jsonb,
 'extracted', 0.90);

-- Feedstock demand elasticity drives SBO price dynamics
INSERT INTO core.kg_edge (source_node_id, target_node_id, edge_type, weight, properties, created_by, confidence) VALUES
((SELECT id FROM core.kg_node WHERE node_key = 'feedstock_demand_elasticity_model'),
 (SELECT id FROM core.kg_node WHERE node_key = 'soybean_oil'),
 'PREDICTS', 0.85,
 '{"mechanism": "Demand elasticity hierarchy determines which end users get priced out as BBD demand grows. Oleochemical and food CPG are most inelastic (insulated). Food service most elastic (squeezed first). RD sits in middle -- credit revenue stack determines competitive position. With total SBO demand (24.3B lbs) exceeding production (23.1B lbs), the elasticity ranking directly predicts which users lose access.", "direction": "price_allocation_by_elasticity", "source": "pbf_study_2022"}'::jsonb,
 'extracted', 0.85);

-- SBO refining capacity shortage drives crush expansion
INSERT INTO core.kg_edge (source_node_id, target_node_id, edge_type, weight, properties, created_by, confidence) VALUES
((SELECT id FROM core.kg_node WHERE node_key = 'sbo_refining_capacity_shortage_model'),
 (SELECT id FROM core.kg_node WHERE node_key = 'renewable_diesel'),
 'CAUSES', 0.9,
 '{"mechanism": "Structural mismatch between US crush capacity (2.2B bu) and refining capacity drives refining margin from 2.5-3 cpb (historical) to 15+ cpb (2021-2022). This margin spike triggers crush/refining expansion wave. RD producers respond by adding front-end refining (5.6B lbs potential), competing with traditional crushers for crude SBO. Biofuel share of SBO demand crosses 50% threshold in 2021/22.", "cycle": "RD demand -> refining shortage -> margin spike -> investment -> capacity addition -> normalization -> next wave", "source": "refining_capacity_2021, agp_deliverables_2022"}'::jsonb,
 'extracted', 0.90);

-- Marathon staircase model links demand growth to price levels
INSERT INTO core.kg_edge (source_node_id, target_node_id, edge_type, weight, properties, created_by, confidence) VALUES
((SELECT id FROM core.kg_node WHERE node_key = 'feedstock_pricing_staircase_model'),
 (SELECT id FROM core.kg_node WHERE node_key = 'feedstock_supply_chain_model'),
 'ENRICHES', 0.8,
 '{"mechanism": "Staircase model adds demand-growth-to-price quantification to feedstock supply chain framework. Each incremental 10-20 MBPD of RD demand creates new price equilibrium. Identifies the critical threshold where LCFS credit value (not RIN) becomes dominant investment driver. With 100+ MBPD expected in 5 years, the staircase provides scenario-specific price paths for all FOG categories.", "source": "marathon_sow_2022"}'::jsonb,
 'extracted', 0.80);

-- Canada CFS impacts cross-border feedstock trade
INSERT INTO core.kg_edge (source_node_id, target_node_id, edge_type, weight, properties, created_by, confidence) VALUES
((SELECT id FROM core.kg_node WHERE node_key = 'canada_cfs_feedstock_impact'),
 (SELECT id FROM core.kg_node WHERE node_key = 'canola_oil'),
 'CAUSES', 0.8,
 '{"mechanism": "Canada CFS creates additional demand pull on canola oil, tallow, and UCO. Cross-border feedstock trade flows: Canadian canola/tallow/UCO to US market vs US SBO/UCO/tallow/CWG to Canadian market. CFS CI requirements favor low-CI feedstocks similar to LCFS. Key question: sufficient Canadian low-CI supply for CFS, or will imports from US be needed?", "direction": "demand_increase_for_canadian_feedstocks", "source": "ecoengineers_outline_2022"}'::jsonb,
 'extracted', 0.80);

-- Oleo-X pretreatment competes in FOG market
INSERT INTO core.kg_edge (source_node_id, target_node_id, edge_type, weight, properties, created_by, confidence) VALUES
((SELECT id FROM core.kg_node WHERE node_key = 'oleox_pascagoula'),
 (SELECT id FROM core.kg_node WHERE node_key = 'feedstock_supply_chain_model'),
 'ENRICHES', 0.75,
 '{"mechanism": "Oleo-X introduces independent pretreatment/tolling model into feedstock supply chain. Instead of integrated RD plants handling own pretreatment, standalone pretreatment facilities process FOGs to RD-spec and sell to producers under tolling arrangements. This model can capture refining margin arbitrage during periods of high volatility by locking in tolling fees. BP as offtake partner provides demand certainty.", "source": "icf_oleox_2022"}'::jsonb,
 'extracted', 0.75);

-- HPAI disease risk impacts poultry fat supply
INSERT INTO core.kg_edge (source_node_id, target_node_id, edge_type, weight, properties, created_by, confidence) VALUES
((SELECT id FROM core.kg_node WHERE node_key = 'feedstock_supply_chain_model'),
 (SELECT id FROM core.kg_node WHERE node_key = 'tallow'),
 'RISK_FACTOR', 0.7,
 '{"mechanism": "Highly pathogenic avian influenza (HPAI) poses acute supply risk to poultry fat and by extension to all animal fat markets. Feb 2022 detected in Indiana turkey flock, then spread rapidly. HPAI spreads via wild migratory birds making containment difficult. Can eliminate 3%+ of poultry fat supply in weeks. Diseases in hogs/cattle are less common and more manageable. This is primary disease risk for fats/greases supply -- makes poultry fat a riskier component of feedstock mix than tallow or CWG.", "severity": "acute_supply_disruption", "frequency": "periodic_outbreaks", "source": "icf_oleox_2022, pbf_study_2022"}'::jsonb,
 'extracted', 0.80);

-- Radius analysis model links to feedstock supply chain
INSERT INTO core.kg_edge (source_node_id, target_node_id, edge_type, weight, properties, created_by, confidence) VALUES
((SELECT id FROM core.kg_node WHERE node_key = 'feedstock_radius_analysis_model'),
 (SELECT id FROM core.kg_node WHERE node_key = 'feedstock_supply_chain_model'),
 'ENRICHES', 0.85,
 '{"mechanism": "Radius analysis adds geographic/logistic layer to feedstock supply chain model. Key finding across 6+ studies: nearly all facilities show feedstock deficits within initial draw area. Midwest and barge-connected supply is critical. Rail cost ~2.5 cpb baseline. Barge cost lower but subject to seasonal river conditions. Multiple logistics options (rail + barge) essential for risk mitigation.", "source": "pbf_study_2022, zenith_proposal_2021, warburg_pincus_sow_2022"}'::jsonb,
 'extracted', 0.85);

-- Liquid oils framework links CPG demand to SBO
INSERT INTO core.kg_edge (source_node_id, target_node_id, edge_type, weight, properties, created_by, confidence) VALUES
((SELECT id FROM core.kg_node WHERE node_key = 'liquid_oils_market_intelligence'),
 (SELECT id FROM core.kg_node WHERE node_key = 'soybean_oil'),
 'ENRICHES', 0.7,
 '{"mechanism": "CPG procurement intelligence adds food industry demand perspective to SBO analysis. Key dimensions: crush/refining capacity, physical basis in IA/IL, non-GMO premium, EU sustainable/deforestation-free premium. CPG companies (Unilever, P&G) are both competitors for feedstock and customers needing forward market intelligence. Non-GMO SBO market in Europe adds basis dimension (Brazil-linked vs North American).", "source": "unilever_study_2022"}'::jsonb,
 'extracted', 0.70);


-- ============================================================================
-- 3. CONTEXTS: Expert rules, risk thresholds, analytical frameworks
-- ============================================================================

-- PBF Feedstock Draw Area Analysis
INSERT INTO core.kg_context (node_id, context_type, context_key, context_value, applicable_when, source) VALUES
((SELECT id FROM core.kg_node WHERE node_key = 'pbf_chalmette_rd'),
 'expert_rule', 'pbf_chalmette_feedstock_analysis',
 '{"rule": "Gulf Coast RD facilities face structural feedstock deficits within local draw areas. PBF Chalmette 650-mile analysis shows: SBO refining capacity ~3.5B lbs (2023) growing to 4.5B, but demand exceeds supply in all feedstock categories within radius. Must source from Midwest via barge/rail.", "sbo_analysis": {"refining_capacity_draw_area_blbs": 3.5, "refining_capacity_growing_to_blbs": 4.5, "midwest_barge_surplus_likely": true, "crude_degummed_basis_risk": "Basis may rise more than anticipated"}, "dco_analysis": {"ethanol_plants_in_draw_area": 7, "dco_yield_lbs_per_bu": 0.8, "yield_doubling_technology_available": true, "margins_below_average": true, "distance_challenge": "Many Midwest plants >1000 miles, not on waterways"}, "tallow_analysis": {"renderers_in_draw_area": 2, "combined_capacity_mlbs": 112, "not_on_waterways": true, "record_prices_compress_margins": true, "midwest_expansion_avg_distance_miles": 995}, "cwg_analysis": {"renderers_in_draw_area": 1, "limited_capacity": true, "not_on_rail_or_waterway": true, "margins_second_worst": true}, "uco_analysis": {"large_aggregator_presence": true, "highest_margins_all_feedstocks": true, "quality_variability_risk": true, "75pct_population_still_deficit": true}, "source": "pbf_study_2022"}'::jsonb,
 'always', 'extracted');

-- Feedstock Availability Growth Projections (ICF)
INSERT INTO core.kg_context (node_id, context_type, context_key, context_value, applicable_when, source) VALUES
((SELECT id FROM core.kg_node WHERE node_key = 'feedstock_supply_chain_model'),
 'expert_rule', 'us_feedstock_availability_projections_2022',
 '{"rule": "US BBD feedstock demand grows from 18B lbs (2021) to 26+ B lbs by 2026. Available FOG supply grows from 58B lbs (2024) to 60+ B lbs (2026), but competition from multiple industries constrains effective availability. Growth is primarily from soybean oil production increases (yield improvements, acreage, new crush capacity).", "demand_growth_driver": "RD capacity expansion. Biomass-based diesel demand structure shift from biodiesel to renewable diesel continues.", "supply_growth_constraints": {"fats_greases": "By-products of other processes -- cannot be expanded independently. Growth limited by meat production and food service activity.", "sbo": "More optimistic -- yield improvements + acreage + crush capacity expansions. But droughts limit upside.", "dco": "Technology can double yields per bushel but total supply still small fraction of demand.", "uco": "Sensitive to GDP growth. Pandemic showed vulnerability. 3% real GDP growth assumed."}, "exogenous_risks": ["HPAI avian influenza (poultry fat)", "Drought (SBO/canola production)", "Economic recession (UCO/YG)", "Policy changes (food vs fuel)", "Russia-Ukraine (sunflower oil supply)"], "price_forecast_methodology": "Combination of fundamental S&D, price models, and AI/ML algorithms. Refining margin projections based on veg oil refining capacity forecast.", "source": "icf_oleox_2022"}'::jsonb,
 'always', 'extracted');

-- SBO Refining Margin Normalization Framework
INSERT INTO core.kg_context (node_id, context_type, context_key, context_value, applicable_when, source) VALUES
((SELECT id FROM core.kg_node WHERE node_key = 'sbo_refining_capacity_shortage_model'),
 'expert_rule', 'sbo_refining_margin_normalization',
 '{"rule": "SBO refining margins spiked from historical 2.5-3 cpb to 15+ cpb (2021-2022) due to structural mismatch. Normalization to 6-8 cpb expected as (1) crush expansions come online (Cargill, ADM, CHS, Shell Rock), (2) RD plants add front-end refining capacity (~5.6B lbs), (3) demand growth moderates. However, full RBD is NOT required for RD production -- crude degummed or neutralized meets spec.", "expansion_timeline_2021": {"chs_fairmont_mn": "Late 2021, +30% (55M to 71.5M bu)", "shell_rock_ia": "2022, 38.5M bu (new entrant), Phillips 66 offtake", "adm_spiritwood_nd": "Before 2023 harvest, 52.5M bu, includes refining", "cargill_sidney_oh": "Doubling capacity", "cargill_cedar_rapids_ia": "+10%"}, "stocks_outlook": "Both crude and refined SBO stocks expected to fall to pipeline levels -- lowest possible given volumes moving through value chain.", "export_decline": "SBO exports decline sharply as domestic BBD demand absorbs supply. USDA May projections confirmed this expectation.", "geographic_mismatch": "Crush concentrated in Midwest, RD concentrated in Gulf/West Coast. Creates basis/logistics premium for Gulf-delivered SBO. Canola crush may expand to West Coast to serve Pacific RD market.", "source": "refining_capacity_2021, agp_outline_2022"}'::jsonb,
 'always', 'extracted');

-- Tolling Arrangement Advantage
INSERT INTO core.kg_context (node_id, context_type, context_key, context_value, applicable_when, source) VALUES
((SELECT id FROM core.kg_node WHERE node_key = 'oleox_pascagoula'),
 'expert_rule', 'tolling_vs_spot_feedstock_economics',
 '{"rule": "During periods of elevated feedstock price volatility, tolling arrangements (processing feedstock for a fixed fee rather than buying inputs/selling outputs) provide margin stability advantage. The 30-day standard deviation for RBD SBO and refining margins showed sharp volatility increase accompanying the 2021-2022 margin rally.", "icf_assessment": {"crude_degummed_sbo_assumption_cpb": 0.54, "finding": "Below lower end of projection range for forecast period", "rbd_sbo_assumption_cpb": 0.65, "finding_rbd": "Below range for 2025-2026", "tallow_assumption_cpb": 0.72, "finding_tallow": "Above upper end of projection range", "yg_assumption": "Generally reasonable, within projection range", "refining_margin_11cpb": "Upper end of range, above median for each forecast year. But includes deodorizing which may not be required for RD."}, "risk_assessment": "Offtake term sheet (BP) has one-sided terms, limited performance provisions, unclear ownership/delivery points. Needs clarification before execution.", "source": "icf_oleox_2022"}'::jsonb,
 'always', 'extracted');

-- Feedstock Radius Pattern: All Studies Show Deficits
INSERT INTO core.kg_context (node_id, context_type, context_key, context_value, applicable_when, source) VALUES
((SELECT id FROM core.kg_node WHERE node_key = 'feedstock_radius_analysis_model'),
 'expert_rule', 'radius_analysis_deficit_pattern',
 '{"rule": "Across 6+ consulting engagements (PBF 650-mi, Zenith 300-mi, Montana 750-mi, generic template), nearly all initial radius analyses show feedstock deficits. This is a structural feature of the BBD industry: demand is concentrated at large facilities while feedstock production is dispersed across agricultural regions.", "mitigation_strategies": ["Expand draw area to Midwest (where feedstock surplus exists)", "Access barge market (lower cost, higher volume per shipment)", "Develop strategic partnerships with producers (volume commitments)", "Explore Argentine/international SBO imports (tariff risk)", "Source UCO from 75%+ of US population area"], "barge_advantage": "Facilities on waterways have structural logistics advantage. Barge cost < rail. Mississippi River access connects Gulf plants to Midwest surplus.", "rail_benchmark_cpb": 2.5, "strategic_partnership_value": "As 4th-largest (PBF) or similar-scale buyer, leverage volume for preferential pricing and guaranteed supply", "source": "pbf_study_2022, zenith_proposal_2021, warburg_pincus_sow_2022, generic_sow_template"}'::jsonb,
 'always', 'extracted');

-- SBO Demand Structure Crossing 50% Biofuel Threshold
INSERT INTO core.kg_context (node_id, context_type, context_key, context_value, applicable_when, source) VALUES
((SELECT id FROM core.kg_node WHERE node_key = 'soybean_oil'),
 'expert_rule', 'sbo_biofuel_demand_50pct_threshold',
 '{"rule": "2020/21 is likely the last marketing year where biofuel production represents less than 50% of total US SBO demand. This threshold crossing fundamentally changes SBO pricing dynamics -- energy markets (ULSD, RINs, LCFS) become primary price drivers rather than food market fundamentals.", "pre_threshold": "SBO priced primarily by food demand, crush economics, export competitiveness. Heating oil spread was secondary.", "post_threshold": "SBO priced by BBD margin stack: ULSD price + credit values - production costs. Food demand becomes price taker rather than price setter. July SBO - heating oil spread becomes key market relationship.", "evidence": "Spread between July SBO and heating oil futures became primary market signal. SBO prices hit highest since 2008 RFS passage.", "implication_for_crush": "Crush margin increasingly driven by oil value rather than meal value. Historical meal-driven crush economics inverting to oil-driven.", "similarity_to_2008": "Current rally similar to 2008 when initial RFS passage drove SBO to records. Difference: 2021+ is sustained structural demand growth, not speculative spike.", "source": "refining_capacity_2021"}'::jsonb,
 'always', 'extracted');

-- Expert View: Canola RIN Pathway and CI Positioning
INSERT INTO core.kg_context (node_id, context_type, context_key, context_value, applicable_when, source) VALUES
((SELECT id FROM core.kg_node WHERE node_key = 'canola_oil'),
 'expert_rule', 'canola_rin_pathway_ci_positioning_2022',
 '{"rule": "Canola oil CI score (avg 53) only marginally better than SBO (avg 55). Both far worse than fats/greases (avg 30s) or UCO (avg 20). Canola RIN pathway for RD not yet approved as of mid-2022 but expected by year-end. Approval would open significant new RD feedstock supply but at minimal CI advantage over SBO.", "ci_comparison": {"sbo_avg": 55, "canola_avg": 53, "fats_greases_avg": 30, "yellow_grease": 25, "uco_avg": 20}, "canola_advantages_vs_sbo": "Canada CFS creating additional pull. West Coast crushing infrastructure. Lower deforestation concern vs palm oil.", "canola_disadvantages": "CI only marginally better than SBO. Low carryout stocks (2022) limit near-term crushing volumes. Competition for supply between food and BBD similar to SBO.", "canadian_crush_context_2022": "Record 10.4 MMT crush achievable from improved production vs 2021 drought year. But stocks-to-use tight.", "source": "expert_interviews_2022"}'::jsonb,
 'always', 'extracted');

-- Argentine SBO Import Risk to US
INSERT INTO core.kg_context (node_id, context_type, context_key, context_value, applicable_when, source) VALUES
((SELECT id FROM core.kg_node WHERE node_key = 'soybean_oil'),
 'expert_rule', 'argentine_sbo_us_import_challenges',
 '{"rule": "US facilities considering Argentine SBO imports face multiple structural challenges: (1) 19% US import tariff on SBO, (2) declining Argentine crush capacity from margin compression, (3) farmer hoarding behavior, (4) EPA certification premium, (5) dollar strength driving Argentine inflation and farmer retention.", "tariff_barrier": "19% tariff on US SBO imports. Politically powerful agricultural lobby will oppose reduction. Dollar strength makes imported SBO relatively more expensive.", "crush_capacity_outlook": "Argentine crush margins expected to decline as excess processing capacity in US crushes meal export margins. This could shrink Argentine crush over time.", "farmer_hoarding_2022": "Dollar strength increased Argentine inflation -> farmers sold fewer soybeans than normal. Government introduced soy dollar program to incentivize sales -- incentive significantly larger than EPA premium.", "parana_river_risk": "Multi-year La Nina drought in Brazil reduced Parana water levels to 77-year low. Draft restrictions reduce cargo sizes but operations continue.", "practical_assessment": "Argentine SBO can supplement but not anchor a US facility feedstock strategy. Tariff + supply variability + logistics create too much uncertainty for primary sourcing.", "source": "pbf_study_2022"}'::jsonb,
 'always', 'extracted');

-- BD Rationalization Framework
INSERT INTO core.kg_context (node_id, context_type, context_key, context_value, applicable_when, source) VALUES
((SELECT id FROM core.kg_node WHERE node_key = 'bbd_balance_sheet_model'),
 'expert_rule', 'biodiesel_rationalization_by_rd',
 '{"rule": "Growth in RD production capacity will ultimately rationalize (close) biodiesel production capacity. BD plants operating on lower credit revenue, smaller scale, and weaker feedstock logistics lose market share to RD. PBF study models BD capacity declining in draw area as RD expands.", "mechanism": "RD advantages over BD: (1) drop-in fuel (no blend limit), (2) higher credit revenue per gallon, (3) refinery-scale economics, (4) better cold-weather properties. BD disadvantages: blend wall, smaller scale, limited CI improvement potential.", "historical_pattern": "US BD production peaked at 1.8B gal. BD share of BBD declining as RD capacity ramps. BD survives in niche markets (mandated blending states, specialty applications) but aggregate production declines.", "modeling_approach": "PBF study reduced BD feedstock demand projections to reflect rationalization potential within draw area. Remaining BD demand used historical EIA feedstock mix (average of most recent years).", "timeline": "Gradual -- BD plants close as leases expire and equipment ages. Some convert to RD or repurpose. Full rationalization over 5-10 years from 2022.", "source": "pbf_study_2022"}'::jsonb,
 'always', 'extracted');

-- Feedstock Price Volatility and Hedging Limitations
INSERT INTO core.kg_context (node_id, context_type, context_key, context_value, applicable_when, source) VALUES
((SELECT id FROM core.kg_node WHERE node_key = 'feedstock_hedging_limitation_model'),
 'expert_rule', 'feedstock_hedging_structural_limitations',
 '{"rule": "Effective hedging of BBD feedstock price risk faces structural limitations: (1) limited OTC instruments for fats/greases, (2) historically high volatility makes options expensive, (3) counterparty limits cap notional volumes, (4) SBO futures have basis risk to physical delivery points, (5) correlation breakdowns during extreme moves.", "market_development": "Growing participation of major refinery companies (Marathon, Phillips 66, Valero, HF Sinclair) should force development of better hedging infrastructure. But timeline uncertain.", "practical_implication": "Margin projections in feasibility studies assume spot market execution. Actual margins differ based on hedging program. Facilities with active hedging programs may sacrifice upside for downside protection.", "tolling_alternative": "Tolling arrangements (like Oleo-X/BP) transfer feedstock price risk to offtake partner. Toll fee provides margin certainty but typically at lower average margin than spot.", "source": "pbf_study_2022, icf_oleox_2022"}'::jsonb,
 'always', 'extracted');


-- ============================================================================
-- 4. SOURCE REGISTRY: Register all processed documents
-- ============================================================================

-- PBF Feedstock Availability Study (Partners Group)
INSERT INTO core.kg_source (
    source_key, source_type, title, location_uri, document_date, document_type,
    commodities, topics, status, first_processed, last_processed,
    nodes_extracted, edges_extracted, contexts_extracted
) VALUES
('pbf_feedstock_availability_study_final_2022', 'local_file',
 'PBF - Feedstock Availability Study - Final Draft (Partners Group)',
 'C:/Users/torem/RLC Dropbox/Tore Alden/Jacobsen/Projects/Partners Group/PBF - Feedstock Availability Study - Final Draft.docx',
 '2022-09-08', 'consulting_report',
 '{soybean_oil,tallow,used_cooking_oil,distillers_corn_oil,choice_white_grease,renewable_diesel}',
 '{feedstock_availability,radius_analysis,demand_elasticity,gulf_coast,barge_logistics,refining_capacity,food_vs_fuel,hedging_limitations,bd_rationalization,argentine_sbo_import}',
 'completed', NOW(), NOW(), 5, 4, 5)
ON CONFLICT (source_key) DO UPDATE SET
    status = 'completed', last_processed = NOW(),
    nodes_extracted = EXCLUDED.nodes_extracted,
    edges_extracted = EXCLUDED.edges_extracted,
    contexts_extracted = EXCLUDED.contexts_extracted;

-- ICF/TEI Oleo-X Technical Diligence
INSERT INTO core.kg_source (
    source_key, source_type, title, location_uri, document_date, document_type,
    commodities, topics, status, first_processed, last_processed,
    nodes_extracted, edges_extracted, contexts_extracted
) VALUES
('icf_oleox_technical_diligence_2022', 'local_file',
 'Oleo-X Limited Technical Diligence (ICF for TEI Renewable Energy)',
 'C:/Users/torem/RLC Dropbox/Tore Alden/Jacobsen/Projects/ICF/TimeRenewable_Feedstock_ICF_050322_TC.docx',
 '2022-05-03', 'consulting_report',
 '{soybean_oil,tallow,yellow_grease,used_cooking_oil,poultry_fat,renewable_diesel}',
 '{feedstock_availability,price_forecast,refining_margins,tolling_arrangement,offtake_review,fog_pretreatment,hpai_disease_risk}',
 'completed', NOW(), NOW(), 2, 2, 2)
ON CONFLICT (source_key) DO UPDATE SET
    status = 'completed', last_processed = NOW(),
    nodes_extracted = EXCLUDED.nodes_extracted,
    edges_extracted = EXCLUDED.edges_extracted,
    contexts_extracted = EXCLUDED.contexts_extracted;

-- Refining Capacity Report (May 2021)
INSERT INTO core.kg_source (
    source_key, source_type, title, location_uri, document_date, document_type,
    commodities, topics, status, first_processed, last_processed,
    nodes_extracted, edges_extracted, contexts_extracted
) VALUES
('refining_capacity_report_may_2021', 'local_file',
 'The Impact of Growing Renewable Diesel Capacity on Soybean Oil Refining Capacity',
 'C:/Users/torem/RLC Dropbox/Tore Alden/Jacobsen/Projects/Refining Capacity Update/Refining Capacity Report - 05132021.docx',
 '2021-05-13', 'consulting_report',
 '{soybean_oil,canola_oil,renewable_diesel}',
 '{refining_capacity,crush_expansion,refining_margins,demand_structure_shift,biofuel_threshold,canola_west_coast}',
 'completed', NOW(), NOW(), 1, 1, 2)
ON CONFLICT (source_key) DO UPDATE SET
    status = 'completed', last_processed = NOW(),
    nodes_extracted = EXCLUDED.nodes_extracted,
    edges_extracted = EXCLUDED.edges_extracted,
    contexts_extracted = EXCLUDED.contexts_extracted;

-- AGP Project Deliverables (Vegetable Oil Refining Analysis)
INSERT INTO core.kg_source (
    source_key, source_type, title, location_uri, document_date, document_type,
    commodities, topics, status, first_processed, last_processed,
    nodes_extracted, edges_extracted, contexts_extracted
) VALUES
('agp_deliverables_2022', 'local_file',
 'AGP Project Deliverables - Vegetable Oil Refining Capacity Analysis',
 'C:/Users/torem/RLC Dropbox/Tore Alden/Jacobsen/Projects/AGP/AGP Project Deliverables - 04122022.docx',
 '2022-04-12', 'consulting_report',
 '{soybean_oil,canola_oil,renewable_diesel}',
 '{refining_capacity,crush_expansion,crude_vs_refined_balance_sheet,refining_margin_cycle,basis_analysis}',
 'completed', NOW(), NOW(), 0, 0, 0)
ON CONFLICT (source_key) DO UPDATE SET
    status = 'completed', last_processed = NOW(),
    nodes_extracted = EXCLUDED.nodes_extracted,
    edges_extracted = EXCLUDED.edges_extracted,
    contexts_extracted = EXCLUDED.contexts_extracted;

-- AGP Report First Draft
INSERT INTO core.kg_source (
    source_key, source_type, title, location_uri, document_date, document_type,
    commodities, topics, status, first_processed, last_processed,
    nodes_extracted, edges_extracted, contexts_extracted
) VALUES
('agp_report_first_draft_2022', 'local_file',
 'AGP Report - First Draft (RD Capacity Impact on Feedstock Markets)',
 'C:/Users/torem/RLC Dropbox/Tore Alden/Jacobsen/Projects/AGP/AGP Report - First Draft.docx',
 '2022-06-11', 'consulting_report',
 '{soybean_oil,renewable_diesel}',
 '{rd_capacity_expansion,feedstock_demand,fats_greases,vegetable_oil_demand}',
 'completed', NOW(), NOW(), 0, 0, 0)
ON CONFLICT (source_key) DO UPDATE SET
    status = 'completed', last_processed = NOW(),
    nodes_extracted = EXCLUDED.nodes_extracted,
    edges_extracted = EXCLUDED.edges_extracted,
    contexts_extracted = EXCLUDED.contexts_extracted;

-- Zenith Joliet Renewables Proposal
INSERT INTO core.kg_source (
    source_key, source_type, title, location_uri, document_date, document_type,
    commodities, topics, status, first_processed, last_processed,
    nodes_extracted, edges_extracted, contexts_extracted
) VALUES
('zenith_joliet_proposal_final_2021', 'local_file',
 'Zenith Joliet Renewables Proposal - Final',
 'C:/Users/torem/RLC Dropbox/Tore Alden/Jacobsen/Projects/Zenith Energy/Zenith Joliet Renewables Proposal - Final.docx',
 '2021-09-15', 'consulting_proposal',
 '{soybean_oil,tallow,distillers_corn_oil,used_cooking_oil,choice_white_grease}',
 '{feedstock_availability,radius_analysis,plant_mapping,logistics,sd_balance_sheets}',
 'completed', NOW(), NOW(), 1, 0, 0)
ON CONFLICT (source_key) DO UPDATE SET
    status = 'completed', last_processed = NOW(),
    nodes_extracted = EXCLUDED.nodes_extracted,
    edges_extracted = EXCLUDED.edges_extracted,
    contexts_extracted = EXCLUDED.contexts_extracted;

-- Marathon SOW
INSERT INTO core.kg_source (
    source_key, source_type, title, location_uri, document_date, document_type,
    commodities, topics, status, first_processed, last_processed,
    nodes_extracted, edges_extracted, contexts_extracted
) VALUES
('marathon_sow_2022', 'local_file',
 'Marathon Petroleum - Feedstock Pricing Staircase SOW',
 'C:/Users/torem/RLC Dropbox/Tore Alden/Jacobsen/Projects/Marathon/Marathon SOW Tore mark up.docx',
 '2022-01-01', 'scope_of_work',
 '{soybean_oil,canola_oil,tallow,used_cooking_oil,choice_white_grease,poultry_fat,distillers_corn_oil}',
 '{pricing_staircase,lcfs_economics,rin_economics,feedstock_supply,crush_capacity,cover_crops}',
 'completed', NOW(), NOW(), 1, 1, 0)
ON CONFLICT (source_key) DO UPDATE SET
    status = 'completed', last_processed = NOW(),
    nodes_extracted = EXCLUDED.nodes_extracted,
    edges_extracted = EXCLUDED.edges_extracted,
    contexts_extracted = EXCLUDED.contexts_extracted;

-- Warburg-Pincus Montana Renewables SOW
INSERT INTO core.kg_source (
    source_key, source_type, title, location_uri, document_date, document_type,
    commodities, topics, status, first_processed, last_processed,
    nodes_extracted, edges_extracted, contexts_extracted
) VALUES
('warburg_pincus_montana_renewables_sow_2022', 'local_file',
 'Warburg-Pincus Scope of Work - Montana Renewables',
 'C:/Users/torem/RLC Dropbox/Tore Alden/Jacobsen/Projects/Warburg-Pincus/Warburg-Pincus Scope of Work - Montana Renewables.docx',
 '2022-01-01', 'scope_of_work',
 '{soybean_oil,canola_oil,tallow,used_cooking_oil,choice_white_grease,distillers_corn_oil}',
 '{feedstock_availability,radius_analysis,plant_mapping,logistics,benchmark_pricing}',
 'completed', NOW(), NOW(), 1, 0, 0)
ON CONFLICT (source_key) DO UPDATE SET
    status = 'completed', last_processed = NOW(),
    nodes_extracted = EXCLUDED.nodes_extracted,
    edges_extracted = EXCLUDED.edges_extracted,
    contexts_extracted = EXCLUDED.contexts_extracted;

-- EcoEngineers Canadian Biofuel Feedstocks Outline
INSERT INTO core.kg_source (
    source_key, source_type, title, location_uri, document_date, document_type,
    commodities, topics, status, first_processed, last_processed,
    nodes_extracted, edges_extracted, contexts_extracted
) VALUES
('ecoengineers_canadian_feedstocks_outline_2022', 'local_file',
 'Multi-Client Outline - Canadian Biofuel Feedstocks',
 'C:/Users/torem/RLC Dropbox/Tore Alden/Jacobsen/Projects/EcoEngineers/Multi-Client Outline - Canadian Biofuel Feedstocks.docx',
 '2022-01-01', 'study_outline',
 '{canola_oil,tallow,used_cooking_oil,choice_white_grease,soybean_oil}',
 '{canada_cfs,lcfs_lessons,cross_border_trade,ci_scoring,advanced_feedstocks,camelina}',
 'completed', NOW(), NOW(), 1, 1, 0)
ON CONFLICT (source_key) DO UPDATE SET
    status = 'completed', last_processed = NOW(),
    nodes_extracted = EXCLUDED.nodes_extracted,
    edges_extracted = EXCLUDED.edges_extracted,
    contexts_extracted = EXCLUDED.contexts_extracted;

-- Unilever Liquid Oils Market Intelligence Study
INSERT INTO core.kg_source (
    source_key, source_type, title, location_uri, document_date, document_type,
    commodities, topics, status, first_processed, last_processed,
    nodes_extracted, edges_extracted, contexts_extracted
) VALUES
('unilever_liquid_oils_study_2022', 'local_file',
 'Unilever Liquid Oils - Market Intelligence Study',
 'C:/Users/torem/RLC Dropbox/Tore Alden/Jacobsen/Projects/Unilever/Liquid Oils - Market Intelligence Study - 01-10-2022.docx',
 '2022-01-10', 'scope_of_work',
 '{soybean_oil,canola_oil}',
 '{cpg_procurement,crush_margins,physical_pricing,non_gmo,eu_market,deforestation_free}',
 'completed', NOW(), NOW(), 1, 1, 0)
ON CONFLICT (source_key) DO UPDATE SET
    status = 'completed', last_processed = NOW(),
    nodes_extracted = EXCLUDED.nodes_extracted,
    edges_extracted = EXCLUDED.edges_extracted,
    contexts_extracted = EXCLUDED.contexts_extracted;

-- Unilever UCO Study Brief
INSERT INTO core.kg_source (
    source_key, source_type, title, location_uri, document_date, document_type,
    commodities, topics, status, first_processed, last_processed,
    nodes_extracted, edges_extracted, contexts_extracted
) VALUES
('unilever_uco_study_brief_2022', 'local_file',
 'Unilever UCO Market Research Study Brief',
 'C:/Users/torem/RLC Dropbox/Tore Alden/Jacobsen/Projects/Unilever/UCO Project/Study brief for Market Research_UCO.docx',
 '2022-01-01', 'scope_of_work',
 '{used_cooking_oil}',
 '{uco_global_market,collection_infrastructure,quality_variability}',
 'completed', NOW(), NOW(), 1, 0, 0)
ON CONFLICT (source_key) DO UPDATE SET
    status = 'completed', last_processed = NOW(),
    nodes_extracted = EXCLUDED.nodes_extracted,
    edges_extracted = EXCLUDED.edges_extracted,
    contexts_extracted = EXCLUDED.contexts_extracted;

-- Expert Interview Questions (Biofuels July 2022)
INSERT INTO core.kg_source (
    source_key, source_type, title, location_uri, document_date, document_type,
    commodities, topics, status, first_processed, last_processed,
    nodes_extracted, edges_extracted, contexts_extracted
) VALUES
('expert_interview_biofuels_july_2022', 'local_file',
 'Biofuels Expert Interview Questions and Answers - July 2022',
 'C:/Users/torem/RLC Dropbox/Tore Alden/Jacobsen/Projects/Expert Interviews/Biofuels expert interview questions - July 2022.docx',
 '2022-07-01', 'expert_interview',
 '{soybean_oil,canola_oil,used_cooking_oil,renewable_diesel}',
 '{trade_routes,ci_scoring,canola_rin_pathway,crop_forecasts,russia_ukraine,palm_sbo_substitution}',
 'completed', NOW(), NOW(), 1, 0, 1)
ON CONFLICT (source_key) DO UPDATE SET
    status = 'completed', last_processed = NOW(),
    nodes_extracted = EXCLUDED.nodes_extracted,
    edges_extracted = EXCLUDED.edges_extracted,
    contexts_extracted = EXCLUDED.contexts_extracted;

-- Multi-Client Sell Sheet (20-Year Feedstock Outlook)
INSERT INTO core.kg_source (
    source_key, source_type, title, location_uri, document_date, document_type,
    commodities, topics, status, first_processed, last_processed,
    nodes_extracted, edges_extracted, contexts_extracted
) VALUES
('multiclient_feedstock_sellsheet_2019', 'local_file',
 'Multi-Client Sell Sheet - 20-Year Feedstock Outlook',
 'C:/Users/torem/RLC Dropbox/Tore Alden/Jacobsen/Projects/Multi-Client Sell Sheet.docx',
 '2019-01-31', 'consulting_proposal',
 '{tallow,distillers_corn_oil,yellow_grease,used_cooking_oil,choice_white_grease,poultry_fat,soybean_oil,canola_oil,palm_oil,rapeseed_oil}',
 '{20yr_forecast,three_scenarios,pricing_formulas,hedging_correlations,biodiesel_outlook,renewable_diesel_outlook,coprocessing,tall_oil,fish_oil}',
 'completed', NOW(), NOW(), 1, 0, 0)
ON CONFLICT (source_key) DO UPDATE SET
    status = 'completed', last_processed = NOW(),
    nodes_extracted = EXCLUDED.nodes_extracted,
    edges_extracted = EXCLUDED.edges_extracted,
    contexts_extracted = EXCLUDED.contexts_extracted;

-- BHP SOW (Renewable Diesel Data)
INSERT INTO core.kg_source (
    source_key, source_type, title, location_uri, document_date, document_type,
    commodities, topics, status, first_processed, last_processed,
    nodes_extracted, edges_extracted, contexts_extracted
) VALUES
('bhp_renewable_diesel_data_sow', 'local_file',
 'BHP - Scope of Work (Renewable Diesel Data Consultants)',
 'C:/Users/torem/RLC Dropbox/Tore Alden/Jacobsen/Projects/BHP/BHP - Scope of Work.docx',
 '2022-01-01', 'scope_of_work',
 '{renewable_diesel}',
 '{rd_pricing,historical_time_series,fame_correlation}',
 'completed', NOW(), NOW(), 0, 0, 0)
ON CONFLICT (source_key) DO UPDATE SET
    status = 'completed', last_processed = NOW(),
    nodes_extracted = EXCLUDED.nodes_extracted,
    edges_extracted = EXCLUDED.edges_extracted,
    contexts_extracted = EXCLUDED.contexts_extracted;

-- Generic Feedstock Availability SOW Template
INSERT INTO core.kg_source (
    source_key, source_type, title, location_uri, document_date, document_type,
    commodities, topics, status, first_processed, last_processed,
    nodes_extracted, edges_extracted, contexts_extracted
) VALUES
('generic_feedstock_availability_sow_template', 'local_file',
 'Scope of Work - Feedstock Availability (Template)',
 'C:/Users/torem/RLC Dropbox/Tore Alden/Jacobsen/Projects/Scope of Work - Feedstock Availability.docx',
 '2021-01-01', 'scope_of_work',
 '{soybean_oil,tallow,distillers_corn_oil,used_cooking_oil,choice_white_grease}',
 '{feedstock_availability,radius_analysis,plant_mapping,sd_balance_sheets}',
 'completed', NOW(), NOW(), 0, 0, 0)
ON CONFLICT (source_key) DO UPDATE SET
    status = 'completed', last_processed = NOW(),
    nodes_extracted = EXCLUDED.nodes_extracted,
    edges_extracted = EXCLUDED.edges_extracted,
    contexts_extracted = EXCLUDED.contexts_extracted;

-- ICF Statement of Work
INSERT INTO core.kg_source (
    source_key, source_type, title, location_uri, document_date, document_type,
    commodities, topics, status, first_processed, last_processed,
    nodes_extracted, edges_extracted, contexts_extracted
) VALUES
('icf_oleox_sow_2022', 'local_file',
 'ICF - Statement of Work - Oleo-X Technical Diligence',
 'C:/Users/torem/RLC Dropbox/Tore Alden/Jacobsen/Projects/ICF/ICF - Statement of Work - 04262022.docx',
 '2022-04-26', 'scope_of_work',
 '{soybean_oil,tallow,yellow_grease,used_cooking_oil}',
 '{fog_pretreatment,technical_diligence,offtake_review}',
 'completed', NOW(), NOW(), 0, 0, 0)
ON CONFLICT (source_key) DO UPDATE SET
    status = 'completed', last_processed = NOW(),
    nodes_extracted = EXCLUDED.nodes_extracted,
    edges_extracted = EXCLUDED.edges_extracted,
    contexts_extracted = EXCLUDED.contexts_extracted;

-- EcoEngineers Draft ToC for RD Feedstocks
INSERT INTO core.kg_source (
    source_key, source_type, title, location_uri, document_date, document_type,
    commodities, topics, status, first_processed, last_processed,
    nodes_extracted, edges_extracted, contexts_extracted
) VALUES
('ecoengineers_rd_feedstocks_toc', 'local_file',
 'Draft ToC for RD Feedstocks Multi-Client Study',
 'C:/Users/torem/RLC Dropbox/Tore Alden/Jacobsen/Projects/EcoEngineers/Draft ToC for RD Feedstocks draft v1.docx',
 '2022-01-01', 'study_outline',
 '{renewable_diesel}',
 '{rd_feedstock_overview,policy_mandates,seed_oil_vs_waste_oil,ci_differentiation,red2}',
 'completed', NOW(), NOW(), 0, 0, 0)
ON CONFLICT (source_key) DO UPDATE SET
    status = 'completed', last_processed = NOW(),
    nodes_extracted = EXCLUDED.nodes_extracted,
    edges_extracted = EXCLUDED.edges_extracted,
    contexts_extracted = EXCLUDED.contexts_extracted;

-- AGP Outline
INSERT INTO core.kg_source (
    source_key, source_type, title, location_uri, document_date, document_type,
    commodities, topics, status, first_processed, last_processed,
    nodes_extracted, edges_extracted, contexts_extracted
) VALUES
('agp_outline_2022', 'local_file',
 'AGP Outline - Vegetable Oil Refining Capacity Impact',
 'C:/Users/torem/RLC Dropbox/Tore Alden/Jacobsen/Projects/AGP/Outline - 06112022.docx',
 '2022-06-11', 'study_outline',
 '{soybean_oil,canola_oil,renewable_diesel}',
 '{refining_capacity,crude_vs_refined,crush_margins,basis_analysis,refining_margin_cycle}',
 'completed', NOW(), NOW(), 0, 0, 0)
ON CONFLICT (source_key) DO UPDATE SET
    status = 'completed', last_processed = NOW(),
    nodes_extracted = EXCLUDED.nodes_extracted,
    edges_extracted = EXCLUDED.edges_extracted,
    contexts_extracted = EXCLUDED.contexts_extracted;

-- Partners Group NDA (legal/contracts - registered but no extraction)
INSERT INTO core.kg_source (
    source_key, source_type, title, location_uri, document_date, document_type,
    commodities, topics, status, first_processed, last_processed,
    nodes_extracted, edges_extracted, contexts_extracted
) VALUES
('partners_group_nda_2022', 'local_file',
 'Partners Group - Fastmarkets NDA',
 'C:/Users/torem/RLC Dropbox/Tore Alden/Jacobsen/Projects/Partners Group/Partners Group - Fastmarkets NDA - 08262022.docx',
 '2022-08-26', 'legal',
 '{}', '{nda}',
 'skipped_legal', NOW(), NOW(), 0, 0, 0)
ON CONFLICT (source_key) DO UPDATE SET
    status = 'skipped_legal', last_processed = NOW();

-- Andrew Li Project (NDA/legal + charts only)
INSERT INTO core.kg_source (
    source_key, source_type, title, location_uri, document_date, document_type,
    commodities, topics, status, first_processed, last_processed,
    nodes_extracted, edges_extracted, contexts_extracted
) VALUES
('andrew_li_ectp_2021', 'local_file',
 'Andrew Li / ECTP Project (NDA + SBO Charts)',
 'C:/Users/torem/RLC Dropbox/Tore Alden/Jacobsen/Projects/Andrew Li Project/SBO Days of Cover and Soy Crush Charts - 03282021.docx',
 '2021-03-28', 'charts_only',
 '{soybean_oil}', '{sbo_days_of_cover,soy_crush}',
 'skipped_charts_only', NOW(), NOW(), 0, 0, 0)
ON CONFLICT (source_key) DO UPDATE SET
    status = 'skipped_charts_only', last_processed = NOW();

-- AI-ML Agenda (internal meeting document)
INSERT INTO core.kg_source (
    source_key, source_type, title, location_uri, document_date, document_type,
    commodities, topics, status, first_processed, last_processed,
    nodes_extracted, edges_extracted, contexts_extracted
) VALUES
('aiml_sentiment_agenda', 'local_file',
 'Agenda for AI Sentiment Product Meeting',
 'C:/Users/torem/RLC Dropbox/Tore Alden/Jacobsen/Projects/AI-ML/Agenda for AI Sentiment Product Meeting.docx',
 '2022-01-01', 'internal_meeting',
 '{}', '{ai_ml,sentiment_analysis}',
 'skipped_internal', NOW(), NOW(), 0, 0, 0)
ON CONFLICT (source_key) DO UPDATE SET
    status = 'skipped_internal', last_processed = NOW();


-- ============================================================================
-- SUMMARY STATISTICS
-- ============================================================================
-- Nodes created/updated: 14
--   Facilities: 4 (PBF Chalmette, Oleo-X Pascagoula, Zenith Joliet, Montana Renewables)
--   Models: 5 (radius analysis, demand elasticity, pricing staircase, refining shortage, hedging limitations)
--   Concepts: 5 (food vs fuel, Canada CFS, multi-client outlook, RD pricing, biofuel trade shifts, liquid oils intel, UCO research)
-- Edges created: 10
-- Contexts created: 10
-- Sources registered: 23 (15 with extractable content, 3 skipped as legal/charts/internal, 5 SOW-only)
-- Client folders processed: 14 (BHP, EcoEngineers, ICF, Marathon, McKinsey, Partners Group,
--   Shell, Stepan, Unilever, Warburg-Pincus, Zenith Energy, AGP, Expert Interviews,
--   Refining Capacity Update + root-level docs)
-- Client folders with NO docx: McKinsey (empty), Shell (empty), Stepan (empty)
