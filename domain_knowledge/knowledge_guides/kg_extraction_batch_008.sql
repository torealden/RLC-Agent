-- ============================================================================
-- RLC Knowledge Graph: Extraction Batch 008
-- Source: Fastmarkets Q2 2023 quarterlies, SAF coverage, OIC consulting, biodiesel summaries
-- Focus: BBD balance sheets, feedstock supply chains, credit markets, consulting methodology
-- Extracted: 2026-02-14
-- ============================================================================

-- NEW NODES
INSERT INTO core.kg_node (node_type, node_key, label, properties) VALUES

-- BIOFUEL MARKET STRUCTURE
('analytical_model', 'bbd_balance_sheet_model', 'Biomass-Based Diesel Balance Sheet Model', '{"context": "Complete supply/demand framework for biodiesel + renewable diesel combined market. Supply: domestic production (EIA monthly data, split BD/RD) + imports. Demand: obligated party blending (RFS mandate) + voluntary blending (LCFS/CFP/CFS credits) + exports. Key metric: production vs implied mandate. When production exceeds combined RVO (BBD + UAB), excess supply pressures D4 RIN values. Q2 2023: record 1.2B gal quarterly output, first time >1B. 2023 forecast: 4.6B gal (+30% YoY). Combined capacity 6.59B gal = <70% utilization. Critical: RD taking share from BD — RD passed 50% of BBD production in 2023 for first time."}'),

('analytical_model', 'bbd_margin_model', 'Biomass-Based Diesel Margin Analysis Framework', '{"context": "Revenue components: fuel price + RIN value + LCFS credit value (if California/OR/WA). Cost components: feedstock cost (varies by mix) + processing + logistics. Margin = revenue stack - feedstock cost. Feedstock mixes vary: BD typically 75% SBO + 25% YG. RD typically 50% BFT + 25% DCO + 25% UCO. LCFS credit value varies by CI score — UCO/DCO get 7-8c/gal more than SBO per $1 LCFS price change. When BD margins fall below zero, capacity rationalization begins. Integrated BD producers (650M gal capacity) are swing producers — sell SBO directly when refining margins exceed BD margins."}'),

('analytical_model', 'feedstock_supply_chain_model', 'Fat/Grease/Vegoil Feedstock Supply Chain Model', '{"context": "Complete supply/demand framework for BBD feedstocks. Supply: domestic production (USDA/EIA) + imports (Census Bureau trade data). Demand: BBD feedstock use + non-biofuel use + exports. Key dynamics: (1) UCO imports exploded from 188M lbs Q2 2022 to 608M lbs Q2 2023 — traceability concerns emerging. (2) China becoming primary UCO source (31% in 2023, projected >50% long-term). (3) US shifted from net exporter to net importer of fats/greases in 2022 (first time). (4) Import CAGR 24% since 2012. (5) Tallow sourcing diversifying from Canada (100% in 2014/15 to 27% in Q2 2023) to Brazil (16%), Uruguay (11%), Argentina (8%). Long-term forecast: import growth slows to 2% CAGR."}'),

('analytical_model', 'rin_oversupply_model', 'RIN Oversupply / Nesting Mechanics Model', '{"context": "RFS nesting allows D3/D4/D5 RINs to satisfy advanced biofuel mandate. Undifferentiated Advanced Biofuel (UAB) = advanced mandate minus D3/D4/D5 generation. Implied BBD mandate = BBD mandate + UAB. When BBD production exceeds implied mandate, D4 RIN oversupply pressures prices. 2023: BBD RIN generation exceeded mandate by 75% (vs 31% in 2022, 5yr avg 33%). D4/D6 spread narrowing: <3c in Q2 2023 (vs 24c in 2022) because ethanol blend wall means D4 RINs substitute for D6 compliance. Key: when D4/D6 spread approaches zero, it signals maximum oversupply condition and BD capacity rationalization is imminent."}'),

('commodity', 'used_cooking_oil', 'Used Cooking Oil (UCO)', '{"context": "Lowest CI feedstock widely available. CI advantage creates substantial LCFS premium for RD producers. Import supply chain: China 31% (growing to >50%), Canada 31% (declining from 85%), Australia 15%, NZ 8%, Chile 6%. Traceability concerns: some cargos rejected at US ports due to origin uncertainty. Industry response: new traceability startups. Long-term: billions of pounds of global UCO supply potential once traceability established. Fastest-growing feedstock: 5% CAGR forecast over 10 years."}'),

('commodity', 'distillers_corn_oil', 'Distillers Corn Oil (DCO)', '{"context": "Byproduct of ethanol production. Supply directly tied to ethanol grind — when corn grind falls, DCO supply falls. CI score between SBO and UCO. RD demand for DCO growing fastest in percentage terms: 59% YoY increase in 2022-2023. BD demand declining as RD takes share. Price correlation: DCO prices rose from $4.89/gal (Mar 2023) to $6.06/gal (Jun 2023). Profitability declined 80% in Q2 2023 — largest decline of any feedstock."}'),

('commodity', 'bleachable_fancy_tallow', 'Bleachable Fancy Tallow (BFT)', '{"context": "Animal fat from beef processing. Supply relatively inelastic — tied to cattle slaughter. US sourcing diversifying internationally. Price rose from $5.24/gal (Apr 2023) to $6.35/gal (Jun 2023). Used primarily in RD production (50% of typical feedstock mix). Profitability declined 72% in Q2 2023. Key sensitivity: cattle cycle affects supply — when herd liquidation peaks, tallow supply peaks then declines."}'),

-- SAF MARKET
('commodity', 'sustainable_aviation_fuel', 'Sustainable Aviation Fuel (SAF)', '{"context": "Emerging market: drop-in replacement for conventional jet fuel. Production pathways: HEFA (hydroprocessed esters and fatty acids — same process as RD), Fischer-Tropsch, alcohol-to-jet. Key players: DGD (Valero/Darling), Montana Renewables, World Energy. SAF Grand Challenge: 3B gal by 2030, 35B gal by 2050. Federal tax credit: $1.25-$1.75/gal (50% GHG reduction minimum). SAF competes with RD for same feedstocks and capacity — when SAF margins exceed RD margins, producers switch. This creates feedstock demand competition that supports fat/oil prices."}'),

('policy', 'lcfs_credit_framework', 'LCFS/CFP/CFS Clean Fuel Credit Framework', '{"context": "California LCFS, Oregon CFP, Washington CFS — all CI-based credit systems. SAF can opt-in voluntarily. Credit value varies by CI score: UCO/DCO get 7-8c/gal more per $1 LCFS price than SBO. Q2 2023: LCFS credits rose from 69c to 77c. BD/RD spread in CA narrowed from 62c/gal (2020) to 4.9c/gal (2022) — CAGR -57%. When LCFS credits rise, low-CI feedstocks (UCO/DCO) benefit disproportionately. EV credit generation growing — competes with BBD for LCFS credit demand. CARB proposed reducing benchmark CI."}'),

-- CONSULTING METHODOLOGY
('analytical_model', 'crusher_feasibility_model', 'Soybean Crusher Feasibility Assessment Framework', '{"context": "RLC Consulting engagement methodology for evaluating soybean crushing facility investments. Key analytical components: (1) Regional supply analysis — county-level soybean production in catchment area, competing crushers, origination radius economics. (2) Crush margin forecasting — historical margins, basis levels for beans/meal/oil, sensitivity to input changes. (3) Demand analysis — meal by livestock sector, oil by food/industrial/RD, specialty products (non-GMO premium). (4) Competitive landscape — existing capacity, announced expansions (Alta, Norfolk, David City), market share impacts. (5) Logistics — highway access, rail, export terminals. (6) Financial — ROI projections (example: 24.7% pre-tax DCF-ROI for 1,000 ton/day). (7) Red Flag Report — identify insurmountable issues before full analysis. Timeline: 30-45 working days."}'),

('data_series', 'eia.monthly_biofuel_production', 'EIA Monthly Biofuel Production & Capacity', '{"context": "Primary data source for BBD production tracking. Reports biodiesel and renewable diesel separately. Capacity reported quarterly. Key series: operable capacity, production, feedstock consumption by type. Seasonal pattern: Q1 lowest, Q2 +20%, Q3 flat, Q4 +7% to peak. Used to calculate capacity utilization and market share shifts (RD vs BD)."}'),

('data_series', 'census.fat_grease_trade', 'Census Bureau Fat/Grease/Oil Trade Data', '{"context": "Monthly import/export data for fats, greases, and vegetable oils by country and commodity. Key for tracking: UCO import surge (3x in one year), tallow sourcing diversification, US shift from net exporter to net importer. Combines with USDA domestic production for total supply estimates."}')

ON CONFLICT (node_key) DO UPDATE SET properties = EXCLUDED.properties;

-- EDGES
INSERT INTO core.kg_edge (source_node_id, target_node_id, edge_type, weight, properties, created_by, confidence) VALUES

-- BBD balance sheet → RIN oversupply
((SELECT id FROM core.kg_node WHERE node_key = 'bbd_balance_sheet_model'),
 (SELECT id FROM core.kg_node WHERE node_key = 'rin_oversupply_model'),
 'PREDICTS', 0.95,
 '{"mechanism": "BBD production forecast vs implied mandate determines RIN oversupply/deficit. 2023: 4.6B gal production vs 4.09B implied mandate = 500M gal excess = D4 RIN price pressure. Combined capacity 6.59B gal vs 4.6B production = <70% utilization = structural overcapacity. When utilization <75%, BD capacity rationalization accelerates. Monitor: EIA monthly production vs EPA mandate, capacity additions/retirements, BD share of total."}',
 'extracted', 0.95),

-- Feedstock supply chain → BBD margins
((SELECT id FROM core.kg_node WHERE node_key = 'feedstock_supply_chain_model'),
 (SELECT id FROM core.kg_node WHERE node_key = 'bbd_margin_model'),
 'PREDICTS', 0.95,
 '{"mechanism": "Feedstock costs = 70-85% of BBD production cost. When feedstock prices rise faster than fuel prices, margins compress. Q2 2023: SBO rose 15%, BFT rose 21%, DCO rose 24% — all outpaced fuel price recovery. Key price transmission: South American oilseed production → SBO prices → all feedstock prices (SBO is marginal barrel). UCO/tallow follow SBO with 2-4 week lag. Brazil/Argentina record crop 215M tonnes → pressure on SBO → pressure on all feedstocks in late 2023/early 2024."}',
 'extracted', 0.95),

-- UCO → LCFS credit value
((SELECT id FROM core.kg_node WHERE node_key = 'used_cooking_oil'),
 (SELECT id FROM core.kg_node WHERE node_key = 'lcfs_credit_framework'),
 'INFLUENCES', 0.90,
 '{"mechanism": "UCO has lowest CI of major feedstocks. Per $1 LCFS credit price change: UCO gets 7.25c/gal, DCO gets 7.63c/gal, BFT gets 6.25c, SBO gets only 0.75c. This CI differential creates massive incentive to source UCO even at premium. Drives import surge from China and creates traceability problem. When LCFS credit prices rise, UCO premium over other feedstocks widens. When LCFS falls, UCO premium narrows and SBO becomes more competitive."}',
 'extracted', 0.90),

-- SAF competes with RD for feedstock
((SELECT id FROM core.kg_node WHERE node_key = 'sustainable_aviation_fuel'),
 (SELECT id FROM core.kg_node WHERE node_key = 'feedstock_supply_chain_model'),
 'COMPETES', 0.90,
 '{"mechanism": "SAF uses same HEFA process and feedstocks as RD. When SAF margins exceed RD margins (due to $1.25-1.75/gal tax credit + airline offtake premiums), producers shift capacity from RD to SAF. DGD SAF plant announcement and Montana Renewables Max SAF Case (16,000 bpd) demonstrate this shift. Impact: same feedstock demand but diverted from RD → RD supply decreases → RD margins improve → but feedstock demand unchanged → feedstock prices supported. SAF Grand Challenge 3B gal by 2030 = massive additional feedstock demand if achieved."}',
 'extracted', 0.90),

-- BBD margin model → soybean oil demand
((SELECT id FROM core.kg_node WHERE node_key = 'bbd_margin_model'),
 (SELECT id FROM core.kg_node WHERE node_key = 'soybean_oil'),
 'INFLUENCES', 0.90,
 '{"mechanism": "When BBD margins are positive, SBO demand for biofuel supported. Integrated BD producers (650M gal capacity) are swing producers — when BD margin > SBO margin, they produce BD. When SBO margin > BD margin, they sell SBO directly. This creates a floor under SBO prices at approximately the breakeven BD production cost. Q2 2023 BD margins ranged from +84c (May high) to -39c (Jun low). Negative margins trigger BD production cuts → SBO demand falls → SBO prices fall → margins recover. Cycle time: 4-8 weeks."}',
 'extracted', 0.90),

-- Crusher feasibility → soybean meal
((SELECT id FROM core.kg_node WHERE node_key = 'crusher_feasibility_model'),
 (SELECT id FROM core.kg_node WHERE node_key = 'soybeans'),
 'ANALYZES', 0.85,
 '{"mechanism": "Crusher feasibility drives new capacity decisions that affect soybean basis, meal supply, and oil supply regionally. OIC Merrill facility analysis: 1,000 ton/day, NW Iowa catchment, competing with Alta/Norfolk/David City announced plants. New crush capacity = additional soybean demand (tighter local basis) + additional meal supply (weaker local meal basis) + additional oil supply (into RD market). When crush margins support 24.7% pre-tax DCF-ROI, investment greenlit → changes regional supply/demand for 20+ years."}',
 'extracted', 0.85),

-- EIA data → BBD balance sheet
((SELECT id FROM core.kg_node WHERE node_key = 'eia.monthly_biofuel_production'),
 (SELECT id FROM core.kg_node WHERE node_key = 'bbd_balance_sheet_model'),
 'FEEDS', 0.95,
 '{"mechanism": "EIA provides monthly production, capacity, and feedstock consumption data. Key monthly tracking: (1) RD vs BD production split — RD crossed 50% in 2023. (2) Capacity utilization — below 70% in 2023 signals overcapacity. (3) Feedstock consumption by type — 3.04B lbs record in Aug 2023. (4) Capacity additions/retirements — track announced vs actual. (5) Seasonal patterns for quarterly forecasting. Data lag: ~2 months."}',
 'extracted', 0.95),

-- Census trade data → feedstock supply
((SELECT id FROM core.kg_node WHERE node_key = 'census.fat_grease_trade'),
 (SELECT id FROM core.kg_node WHERE node_key = 'feedstock_supply_chain_model'),
 'FEEDS', 0.90,
 '{"mechanism": "Census trade data tracks monthly imports/exports by commodity and country. Critical for: (1) UCO import surge monitoring — 3x increase in one year. (2) Tallow source diversification — Canada declining, Brazil/Uruguay/Argentina growing. (3) Total fat/grease trade balance — US became net importer in 2022. (4) Vegetable oil import competition. Data lag: ~6 weeks. Combine with USDA domestic production for total supply."}',
 'extracted', 0.90);

-- CONTEXTS
INSERT INTO core.kg_context (node_id, context_type, context_key, context_value, applicable_when, source) VALUES

-- BBD market assessment framework
((SELECT id FROM core.kg_node WHERE node_key = 'bbd_balance_sheet_model'),
 'expert_rule', 'bbd_market_assessment_methodology',
 '{"quarterly_workflow": {
   "step1_production": "EIA monthly data: total BBD, RD share, BD share. Annualize quarterly rate. Compare to capacity for utilization rate.",
   "step2_capacity": "Track additions/retirements. Announced vs actual — discount announced by 20-30% for delays. Combined capacity was 6.59B gal end of 2023.",
   "step3_mandate_math": "EPA BBD mandate + UAB (advanced minus D3/D4/D5 generation) = implied mandate. Compare production to implied mandate for oversupply/deficit.",
   "step4_margins": "Calculate BD margin (75% SBO + 25% YG feedstock mix) and RD margin (50% BFT + 25% DCO + 25% UCO). Add LCFS credit by CI score. When BD margin negative for >8 weeks, expect capacity cuts.",
   "step5_rin_forecast": "Oversupply ratio (production/implied mandate) predicts D4 RIN direction. >1.3x = strong downward pressure. 1.0-1.3x = range-bound. <1.0x = rally.",
   "step6_feedstock": "Census imports + USDA domestic = total supply. EIA consumption = demand. Residual = non-biofuel use + stock change. Track UCO imports monthly for surge/decline signals."
 },
 "key_thresholds": {
   "capacity_utilization_crisis": "<65% = multiple facility closures expected",
   "capacity_utilization_tight": ">85% = feedstock competition intensifies",
   "bd_margin_shutdown": "< -$0.20/gal for >4 weeks = production cuts begin",
   "d4_d6_spread_collapse": "<$0.05 = maximum oversupply, D4 loses premium value",
   "uco_import_share": ">40% from single country = concentration risk"
 }}',
 'always', 'extracted'),

-- Feedstock price transmission chain
((SELECT id FROM core.kg_node WHERE node_key = 'feedstock_supply_chain_model'),
 'expert_rule', 'feedstock_price_transmission',
 '{"chain": "South American oilseed production → CBOT SBO futures → US cash SBO → yellow grease/UCO (2-4 week lag) → tallow (4-6 week lag) → DCO (tied to ethanol grind economics). SBO is the marginal barrel — sets the floor. CI-adjusted value determines premium over SBO: UCO premium = LCFS credit differential × CI advantage. When LCFS credits rise, UCO premium widens. When SBO falls (South American harvest), all feedstocks follow but UCO/tallow lag creates temporary margin improvement for RD producers using fats over vegoils.",
  "seasonal_pattern": "Q1: weakest (post-SA harvest, low driving demand). Q2: rising (summer driving, pre-US growing season weather premium). Q3: volatile (US crop weather, peak driving). Q4: strongest (SA planting uncertainty, holiday driving demand, year-end RIN compliance buying)."}',
 'always', 'extracted'),

-- SAF market development framework
((SELECT id FROM core.kg_node WHERE node_key = 'sustainable_aviation_fuel'),
 'expert_rule', 'saf_market_framework',
 '{"current_state_2024": {
   "production": "Minimal — <100M gal. Primary producers: World Energy (Paramount, CA), DGD (Port Arthur, TX), Montana Renewables.",
   "capacity_potential": "Significant HEFA capacity could switch from RD to SAF when margins favor. Montana Renewables Max SAF Case = 16,000 bpd.",
   "demand_drivers": "Airline offtake agreements, EU ReFuelEU mandate (2% in 2025, 6% in 2030), federal Grand Challenge (3B gal by 2030)",
   "price": "Premium over conventional jet fuel. Tax credit $1.25-1.75/gal bridges gap.",
   "feedstock_competition": "Same HEFA feedstocks as RD. Additional SAF demand = additional feedstock demand = price support for fats/oils."
 },
 "policy_stack": {
   "federal": "SAF tax credit ($1.25-1.75/gal), RFS D4 RIN, potential 45Z clean fuel credit",
   "california": "LCFS opt-in for aviation",
   "oregon": "CFP opt-in",
   "washington": "CFS opt-in",
   "illinois": "$1.50/gal SAF credit (10yr duration)",
   "eu": "ReFuelEU 2% mandate 2025, 6% 2030, 20% 2035, 70% 2050"
 },
 "analytical_approach": "Monitor: (1) HEFA capacity switching RD→SAF. (2) Airline offtake volume commitments vs actual purchases. (3) Tax credit legislation status. (4) EU mandate implementation timeline. (5) Feedstock CI scoring for SAF pathways. (6) Emerging non-HEFA pathways (alcohol-to-jet, Fischer-Tropsch, power-to-liquid)."}',
 'always', 'extracted'),

-- Soybean crusher feasibility methodology
((SELECT id FROM core.kg_node WHERE node_key = 'crusher_feasibility_model'),
 'expert_rule', 'crusher_feasibility_methodology',
 '{"red_flag_assessment": {
   "supply_risks": ["Insufficient soybean production in catchment radius", "Too many competing crushers announced in region", "Basis widening trend suggesting excess capacity"],
   "demand_risks": ["Meal demand declining (livestock herd contraction)", "Oil demand dependent on single policy (RFS)", "Non-GMO premium not sustainable"],
   "margin_risks": ["Crush margins below breakeven at current basis levels", "New capacity flooding market before facility operational", "Energy cost exposure"],
   "regulatory_risks": ["RFS changes reducing soy oil demand", "LCFS credit collapse", "Trade policy affecting meal exports"]
 },
 "key_metrics": {
   "origination_radius": "Typical 75-150 mile radius. Must cover 100% of daily capacity from this radius.",
   "crush_margin_breakeven": "Approximately $0.80-1.00/bu for 1,000 ton/day facility",
   "roi_threshold": "Minimum 15% pre-tax DCF-ROI for investment greenlight",
   "capacity_utilization_target": ">90% in year 3",
   "basis_impact": "New 1,000 ton/day facility tightens local basis ~$0.05-0.15/bu"
 }}',
 'always', 'extracted');

-- SOURCE REGISTRY
INSERT INTO core.kg_source (source_key, source_type, title, location_uri, document_date, document_type, commodities, topics, status, first_processed, last_processed, nodes_extracted, edges_extracted, contexts_extracted) VALUES
('gdoc_1-Zn3rWgYYaUUP4SveJfYrZUy-MzjsasW88zqjfrb7Aw', 'gdrive_doc', 'Biofuel Summary - Q2 2023 (Tore)', 'https://docs.google.com/document/d/1-Zn3rWgYYaUUP4SveJfYrZUy-MzjsasW88zqjfrb7Aw/edit', '2023-09-01', 'quarterly_report', '{biodiesel,renewable_diesel,soybean_oil}', '{bbd_balance_sheet,capacity,margins,rin_oversupply,feedstock_costs,lcfs}', 'completed', NOW(), NOW(), 0, 0, 0),
('gdoc_1zFbK0bRjDpBe4YRthVn-zyGq7wCfWnXhpwLUxOJZF_s', 'gdrive_doc', 'Fat and Grease Summary - Q2 2023', 'https://docs.google.com/document/d/1zFbK0bRjDpBe4YRthVn-zyGq7wCfWnXhpwLUxOJZF_s/edit', '2023-09-01', 'quarterly_report', '{tallow,uco,dco,yellow_grease}', '{feedstock_supply,imports,trade_flows,uco_traceability,margin_analysis}', 'completed', NOW(), NOW(), 0, 0, 0),
('gdoc_1gfKJkUaGzxLGE-KzYezUGAe9gk89scyZyWBOH_tx6do', 'gdrive_doc', 'Biodiesel Summary - Nov 2023', 'https://docs.google.com/document/d/1gfKJkUaGzxLGE-KzYezUGAe9gk89scyZyWBOH_tx6do/edit', '2023-11-01', 'market_analysis', '{biodiesel,renewable_diesel,saf}', '{lcfs_ev_interplay,rin_pressure,montana_renewables,dgd_saf,camelina,feedstock_consumption}', 'completed', NOW(), NOW(), 0, 0, 0),
('gdoc_1-x6SlsV4ZFP7OZLZ2DKjAb2sjD03kcpeQIMQy6L_dAQ', 'gdrive_doc', 'SAF Short-Term Forecast Report Outline - Sep 2024', 'https://docs.google.com/document/d/1-x6SlsV4ZFP7OZLZ2DKjAb2sjD03kcpeQIMQy6L_dAQ/edit', '2024-09-05', 'report_outline', '{saf}', '{production_forecast,feedstock_analysis,demand_forecast,price_forecast,investment}', 'completed', NOW(), NOW(), 0, 0, 0),
('gdoc_1bsplaM6W96knRLOhpbEZvo4NNjh-rP6sdfJ9IGKDYU4', 'gdrive_doc', 'US SAF Policy Support List - Federal and State Level', 'https://docs.google.com/document/d/1bsplaM6W96knRLOhpbEZvo4NNjh-rP6sdfJ9IGKDYU4/edit', '2024-09-10', 'policy_reference', '{saf}', '{rfs,lcfs,cfp,cfs,saf_tax_credit,refueleu,illinois_credit}', 'completed', NOW(), NOW(), 0, 0, 0),
('gdoc_1945oOBC7Kmj06f6mp26EXTBHqj0o4TUO5LJN0ATJxKI', 'gdrive_doc', 'OIC - Project Working Document (Soybean Crusher Feasibility)', 'https://docs.google.com/document/d/1945oOBC7Kmj06f6mp26EXTBHqj0o4TUO5LJN0ATJxKI/edit', '2024-06-14', 'consulting_engagement', '{soybeans,soybean_meal,soybean_oil}', '{crusher_feasibility,origination,crush_margins,non_gmo,regional_capacity,logistics}', 'completed', NOW(), NOW(), 0, 0, 0),
('gdoc_1ycw7dLNC6eFXfRy7Ka8bGjXyvuG7kjgdK770UNwlrtg', 'gdrive_doc', 'EU-SAF Policy Summary - Aug 2024', 'https://docs.google.com/document/d/1ycw7dLNC6eFXfRy7Ka8bGjXyvuG7kjgdK770UNwlrtg/edit', '2024-08-01', 'policy_reference', '{saf}', '{refueleu,eu_mandate,synthetic_fuels,biofuel_cap}', 'completed', NOW(), NOW(), 0, 0, 0),
('gdoc_1HTUXUqYtofVbHmC02kufbth7kkGDBbi-QVMLHL5voNc', 'gdrive_doc', 'Vegoil Summary - Q2 2023 (Tore)', 'https://docs.google.com/document/d/1HTUXUqYtofVbHmC02kufbth7kkGDBbi-QVMLHL5voNc/edit', '2023-09-01', 'quarterly_report', '{soybean_oil,canola_oil,palm_oil}', '{vegoil_balance,brazil_production,feedstock_demand,price_forecast}', 'completed', NOW(), NOW(), 0, 0, 0)
ON CONFLICT (source_key) DO UPDATE SET status = 'completed', last_processed = NOW();
