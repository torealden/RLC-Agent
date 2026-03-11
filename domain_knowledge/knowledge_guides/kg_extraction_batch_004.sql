-- ============================================================================
-- RLC Knowledge Graph: Extraction Batch 004
-- Source: 6 HB Weekly Text reports (Nov 2020 - Dec 2021, Nov 2022)
-- Extracted: 2026-02-14
-- ============================================================================

-- NEW NODES
INSERT INTO core.kg_node (node_type, node_key, label, properties) VALUES
('data_series', 'usda.wasde.revision_pattern', 'USDA WASDE Yield Revision Patterns', '{"context": "Corn avg drops 0.7 bpa Oct-Nov over 20yr. Soy avg rises <0.1 bpa Nov-Jan. Oct raise then Nov cut in soybeans only 3/20 years."}'),
('data_series', 'hb.initial_forecast', 'HB Initial Next-Year Forecast (December)', '{"context": "Published first week of Dec. Establishes acreage/production/demand/price forecasts for upcoming MY."}'),
('data_series', 'brazil.conab', 'CONAB Monthly Crop Estimates', '{"source": "Companhia Nacional de Abastecimento", "frequency": "monthly"}'),
('data_series', 'brazil.imea', 'IMEA Mato Grosso Crop Tracking', '{"source": "Instituto Mato-Grossense", "frequency": "weekly", "context": "Most granular MT planting/harvest data. Critical for safrinha timing."}'),
('data_series', 'brazil.stu', 'Brazil Soybean Stocks-to-Use', '{"context": "20yr avg 3.9% vs US avg 8.9%. Brazil runs 2x tighter pipeline."}'),
('data_series', 'sre.compliance_deficit', 'SRE Compliance Deficit Exposure', '{"context": "Dec 2021 EPA denial of all 2019-2020 SREs created ~$2.3B in outstanding requirements."}'),
('seasonal_event', 'wheat_winter_kill', 'Winter Wheat Freeze/Kill Risk', '{"context": "Southern Plains HRW vulnerable to extended freeze without snow cover. KC more upside risk than Chicago."}'),
('market_participant', 'brazil.crushers', 'Brazilian Soybean Crushers', '{"context": "When Argentine UpRiver product prices weak, Brazil gains share. 2022: Brazil crush +8% while Argentina fell."}')
ON CONFLICT (node_key) DO UPDATE SET properties = EXCLUDED.properties;

-- EDGES
INSERT INTO core.kg_edge (source_node_id, target_node_id, edge_type, weight, properties, created_by, confidence) VALUES

-- RFS mandate mechanics → ethanol/soy oil
((SELECT id FROM core.kg_node WHERE node_key = 'rvo'),
 (SELECT id FROM core.kg_node WHERE node_key = 'ethanol'),
 'CAUSES', 0.90,
 '{"mechanism": "EPA sets total renewable, advanced, D3, D4 mandates. Ethanol implied. Dec 2021: 2020=17.13B, 2021=18.52B, 2022=20.77B gal. First general exemption use (pandemic). SRE denial created ~$2.3B compliance exposure. D6 rose on supplemental mandate expectations. Corn grind +50M bu, soy oil biofuel use adjusted -2B lbs but still 900M above USDA."}',
 'extracted', 0.95),

-- Soybean oil price architecture from RD capacity
((SELECT id FROM core.kg_node WHERE node_key = 'soybean_oil'),
 (SELECT id FROM core.kg_node WHERE node_key = 'soybean_oil'),
 'SELF_REINFORCING', 0.95,
 '{"mechanism": "RD expansion creates price bands. Floor: non-integrated biodiesel breakeven 49c futures (67c RBD). +10c with LCFS. RD breakeven 25c higher. Ceiling: summer 2021 high. Combined capacity ~5B gal vs 2.76B mandate = overcapacity drives feedstock bidding. Below 1.5B lb stocks → non-biofuel cuts. Canadian drought reduced veg oil availability."}',
 'extracted', 0.95),

-- Acreage economics model
((SELECT id FROM core.kg_node WHERE node_key = 'hb.initial_forecast'),
 (SELECT id FROM core.kg_node WHERE node_key = 'corn'),
 'CAUSES', 0.90,
 '{"mechanism": "Dec forecast: carryout decline + price rally → acreage expansion. Dec 2020: +10.6M acres to 229M (decade high). Soy +6.9M (bulk), corn +1.5M, wheat +2.2M. Inputs: rotation, insurance, input costs, futures. Even with soy expansion, carryout only 144M bu (pipeline). Ethanol grind 4.95B→5.69B bu raises basis → reduces export competitiveness."}',
 'extracted', 0.90),

-- Chinese sourcing tracking
((SELECT id FROM core.kg_node WHERE node_key = 'china'),
 (SELECT id FROM core.kg_node WHERE node_key = 'soybeans'),
 'CAUSES', 0.90,
 '{"mechanism": "Track via: weekly sales (commitments vs shipments), daily flash sales (China vs unknown), FOB spreads (US vs Brazil), timing gaps (>7 day gap = satiation signal). Nov 2020: 27.6M MT committed (+19.7M YoY). US cheaper through Jan, Brazil from Feb. China can draw 2-7M MT without buying. Election transition → Phase 1 uncertainty but need trumps agreement. HB cut US-China from 40M→36M MT on supply not demand."}',
 'extracted', 0.90),

-- WASDE yield revision patterns
((SELECT id FROM core.kg_node WHERE node_key = 'usda.wasde.revision_pattern'),
 (SELECT id FROM core.kg_node WHERE node_key = 'soybeans'),
 'LEADS', 0.85,
 '{"mechanism": "Statistical edge: corn avg -0.7 bpa Nov→Jan. Soy avg +0.1 bpa Nov→Jan. Oct raise then Nov cut in soy only 3/20yr (Nov 2021 was one → strong surprise, +55c rally). When Nov at extreme of analyst range AND historical revision direction aligns, position for Jan revision. 2022: +0.4 Nov suggests -0.5 Jan, but fast harvest may improve accuracy."}',
 'extracted', 0.90),

-- Brazil crop size → world balance
((SELECT id FROM core.kg_node WHERE node_key = 'brazil.conab'),
 (SELECT id FROM core.kg_node WHERE node_key = 'soybeans'),
 'CAUSES', 0.95,
 '{"mechanism": "Brazil is swing factor. Framework: (1) IMEA weekly MT planting (96%+ by mid-Nov = on-schedule). (2) CONAB vs HB vs trendline yield (HB uses trendline, discounts CONAB early-season optimism). (3) STU (3.9% avg = tight pipeline). (4) Chinese import share. (5) Argentine offset capacity. Brazil corn margin of error > soy. Early planting → good safrinha window. Argentina drought → Brazil must deliver. If all three risks align (production miss + strong China + Argentina drought) → record prices."}',
 'extracted', 0.95),

-- RD vs biodiesel capacity competition
((SELECT id FROM core.kg_node WHERE node_key = 'soybean_oil'),
 (SELECT id FROM core.kg_node WHERE node_key = 'ethanol'),
 'COMPETES_WITH', 0.85,
 '{"mechanism": "RD more profitable than biodiesel. Expanding RD capacity forces feedstock prices to level where RD profitable but biodiesel contracts. Creates natural price band. Integrated biodiesel producers survive at higher costs. Connects to canola oil CI advantage from Batch 001."}',
 'extracted', 0.90),

-- Wheat → corn price linkage
((SELECT id FROM core.kg_node WHERE node_key = 'wheat_srw'),
 (SELECT id FROM core.kg_node WHERE node_key = 'corn'),
 'CAUSES', 0.75,
 '{"mechanism": "Wheat in feed ~2% of corn fed but marginal linkage. When wheat >$8/bu (first since 2012, testing $9.50), corn cant decline even on bearish corn fundamentals. 2021: world wheat stocks lowest since 2016/17."}',
 'extracted', 0.80);

-- CONTEXTS
INSERT INTO core.kg_context (node_id, context_type, context_key, context_value, applicable_when, source) VALUES
((SELECT id FROM core.kg_node WHERE node_key = 'soybean_oil'),
 'risk_threshold', 'soybean_oil_price_architecture',
 '{"floor_biodiesel": "49c/lb futures (67c RBD)", "floor_lcfs": "~59c/lb", "ceiling": "above summer 2021 high", "stocks_trigger": "below 1.5B lb → non-biofuel cuts", "capacity_overcapacity": "5B gal combined vs 2.76B mandate", "written": "December 2021"}',
 'always', 'extracted'),

((SELECT id FROM core.kg_node WHERE node_key = 'hb.initial_forecast'),
 'expert_rule', 'initial_forecast_methodology',
 '{"steps": "carryin → acreage response to price → trendline yield → demand by category → price from STU. Dec publication. Compare with USDA Feb Outlook. Divergences = trades. 2020 example: +10.6M acres, saw acreage battle as 2021 defining theme."}',
 'year_end', 'extracted'),

((SELECT id FROM core.kg_node WHERE node_key = 'usda.wasde.revision_pattern'),
 'expert_rule', 'wasde_interpretation_framework',
 '{"pre_report": "Build independent BS, compare to consensus, identify possible changes this month, position for divergence. Report day: compare to own AND consensus, calculate implied rest-of-world changes, check if reaction proportional. Post: update selectively, use historical patterns for next report, identify wrong USDA assumptions. Dec special: no US production update, South America + demand focus."}',
 'pre_report', 'extracted'),

((SELECT id FROM core.kg_node WHERE node_key = 'china'),
 'expert_rule', 'chinese_export_tracking',
 '{"streams": "Weekly sales (commitments vs shipments), daily flash (China vs unknown, >7 day gap = satiation), FOB spreads (US vs Brazil, seasonal switch Feb), inventory draw analysis (months can cover without buying), commitment pace vs USDA trajectory."}',
 'always', 'extracted'),

((SELECT id FROM core.kg_node WHERE node_key = 'brazil.conab'),
 'expert_rule', 'brazil_crop_framework',
 '{"planting": "IMEA weekly MT, Deral weekly PR, CONAB monthly national. MT 96%+ by mid-Nov = on-schedule. Yield: trendline base, discount CONAB early optimism. STU: 3.9% avg (2x tighter than US 8.9%). Crush competition: Argentine meal -$10/ton → Brazil gains share, crush can add 1-2M MT. Corn: margin of error larger, safrinha timing from soy harvest."}',
 'growing_season', 'extracted');

-- SOURCE REGISTRY
INSERT INTO core.kg_source (source_key, source_type, title, location_uri, document_date, document_type, commodities, topics, status, first_processed, last_processed, nodes_extracted, edges_extracted, contexts_extracted) VALUES
('gdoc_1Xkpjh2h6cMdLhfRTriiE55PfKWFCawb97mhCYofRkZ8', 'gdrive_doc', 'HB Weekly Text - 12162021 (EPA Biofuel Mandates)', 'https://docs.google.com/document/d/1Xkpjh2h6cMdLhfRTriiE55PfKWFCawb97mhCYofRkZ8/edit', '2021-12-16', 'weekly_text', '{corn,soybeans,soybean_oil,ethanol}', '{rfs_mandates,sre_denial,renewable_diesel,biodiesel_breakeven,soybean_oil_architecture}', 'completed', NOW(), NOW(), 0, 0, 0),
('gdoc_1MCvUbqs6w46_IZiKaX9kT45s-cabfHzY1QiqCU3qH2Q', 'gdrive_doc', 'HB Weekly Text - 12022020 (Initial 2021/22 Forecasts)', 'https://docs.google.com/document/d/1MCvUbqs6w46_IZiKaX9kT45s-cabfHzY1QiqCU3qH2Q/edit', '2020-12-02', 'weekly_text', '{corn,soybeans,wheat}', '{initial_forecast,acreage_economics,ethanol_mandate}', 'completed', NOW(), NOW(), 0, 0, 0),
('gdoc_17PwDpneKnsK7SFG_ifXyrlGqvsPphZmPTfP0cy3FVRM', 'gdrive_doc', 'HB Weekly Text - 11182020 (China Phase 1 Update)', 'https://docs.google.com/document/d/17PwDpneKnsK7SFG_ifXyrlGqvsPphZmPTfP0cy3FVRM/edit', '2020-11-18', 'weekly_text', '{corn,soybeans,wheat}', '{china_demand,phase1_trade,export_tracking,election_impact}', 'completed', NOW(), NOW(), 0, 0, 0),
('gdoc_15xvX2iCVpI0JnnrFPlHxSkK_wkOX4U7QPKVyMEkqlqQ', 'gdrive_doc', 'HB Weekly Text - 11172022 (Brazil S&D Preview)', 'https://docs.google.com/document/d/15xvX2iCVpI0JnnrFPlHxSkK_wkOX4U7QPKVyMEkqlqQ/edit', '2022-11-17', 'weekly_text', '{soybeans,corn}', '{brazil_production,conab,safrinha,crush_competition,argentina_drought}', 'completed', NOW(), NOW(), 0, 0, 0),
('gdoc_1i7ysKuw27Hl4CtfP49PONoj41ge39As0pz_jzKO_0-c', 'gdrive_doc', 'HB Weekly Text - 11112021 (Nov WASDE)', 'https://docs.google.com/document/d/1i7ysKuw27Hl4CtfP49PONoj41ge39As0pz_jzKO_0-c/edit', '2021-11-11', 'weekly_text', '{corn,soybeans,wheat,soybean_oil}', '{wasde_november,yield_revision,renewable_diesel,wheat_rally}', 'completed', NOW(), NOW(), 0, 0, 0),
('gdoc_1Nj_KaAy49p0-eDhvwUG-CR5RLeSN3r9UHFLXHUHspMM', 'gdrive_doc', 'HB Weekly Text - 11102022 (Nov WASDE 2022)', 'https://docs.google.com/document/d/1Nj_KaAy49p0-eDhvwUG-CR5RLeSN3r9UHFLXHUHspMM/edit', '2022-11-10', 'weekly_text', '{corn,soybeans,wheat}', '{wasde_november,yield_pattern,argentina,china_stocks}', 'completed', NOW(), NOW(), 0, 0, 0)
ON CONFLICT (source_key) DO UPDATE SET status = 'completed', last_processed = NOW();
