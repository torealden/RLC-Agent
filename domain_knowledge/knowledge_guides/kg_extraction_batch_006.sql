-- ============================================================================
-- RLC Knowledge Graph: Extraction Batch 006
-- Source: 6 HB Weekly Text reports (Jun 2021, Jun 2022, Jun 2023, Jul 2021)
-- Focus: Spring planting, June 30 reports, acreage/stocks methodology
-- Extracted: 2026-02-14
-- ============================================================================

-- NEW NODES
INSERT INTO core.kg_node (node_type, node_key, label, properties) VALUES
('seasonal_event', 'usda_june30_acreage', 'USDA June 30 Acreage Report', '{"context": "One of most volatile trading days. Updates planted area from March intentions. Market adjusts production = revised area × yield estimate."}'),
('seasonal_event', 'usda_june30_stocks', 'USDA June 30 Quarterly Grain Stocks', '{"context": "June 1 inventory levels. Wheat = ending stocks. Corn/soy = third quarter usage implied. Residual use is critical unknown."}'),
('analytical_model', 'planting_pace_acreage_model', 'Planting Pace → Acreage Prediction Model', '{"context": "When farmers finish corn early, area typically rises from March intentions. Since 2000, USDA only reduced corn in 4/15 years when planting on time or early. Soybean area declines 7/9 early-finish years (counterintuitive)."}'),
('analytical_model', 'quarterly_residual_model', 'Quarterly Stocks Residual Use Model', '{"context": "Residual = crops in transit. Compare export inspections first 2 weeks of quarter start vs end. Soy residual cleaner than corn because feed not bundled."}'),
('analytical_model', 'acreage_rules_of_thumb', 'Acreage Allocation Rules of Thumb', '{"context": "Rule 1: Farmers prefer corn (above estimate 7/10yr). Rule 2: Late corn → soy cut disproportionate. Rule 3: Soy>corn area only 3 times ever. Rule 4: Modern tech = faster planting, higher threshold for prevent-plant."}'),
('data_series', 'usda.grain_stocks.feed_residual', 'USDA Corn Feed & Residual Use', '{"context": "Combined feed+residual makes corn stocks estimation complex. Feed via GCAUs + price + supply. FSI components only reported quarterly after stocks report."}'),
('data_series', 'rin.d6_d4_price', 'RIN Prices (D6/D4)', '{"context": "D6/D4 reached ~$2/gal Jun 2021. Delta built $350M compliance deficit. Record prices triggered lobbying for SRE/mandate reduction."}'),
('policy', 'scotus_sre_ruling_2021', 'Supreme Court SRE Ruling Jun 2021', '{"context": "Overturned 10th Circuit SRE requirement. 50+ pending applications. Reduced corn grind forecast 250M bu. Biden admin ultimately denied all SREs Dec 2021."}'),
('analytical_model', 'north_dakota_planting_switch', 'ND Late Planting Crop Switch Model', '{"context": "ND planting delays → corn to soy/canola switch. 2022: corn 3.6M→3.2M, soy 7M→7.4M record, canola 1.76M→1.89M record."}')
ON CONFLICT (node_key) DO UPDATE SET properties = EXCLUDED.properties;

-- EDGES
INSERT INTO core.kg_edge (source_node_id, target_node_id, edge_type, weight, properties, created_by, confidence) VALUES

-- Planting pace → acreage prediction
((SELECT id FROM core.kg_node WHERE node_key = 'planting_pace_acreage_model'),
 (SELECT id FROM core.kg_node WHERE node_key = 'corn'),
 'PREDICTS', 0.95,
 '{"mechanism": "Early finish → avg +911K corn (raised 7/9 years since 2000). Same-week → avg -171K but wide range. Late → soy loses disproportionately. USDA hasnt lowered corn when planting on time or early since 2015. Soybean asymmetry: in 9 early-finish years, soy area DECLINED avg 550K (fell 7/9). Market misprices by using narrative (fertilizer, RD demand) rather than statistical relationships."}',
 'extracted', 0.95),

-- Acreage rules → market structure
((SELECT id FROM core.kg_node WHERE node_key = 'acreage_rules_of_thumb'),
 (SELECT id FROM core.kg_node WHERE node_key = 'soybeans'),
 'CAUSES', 0.90,
 '{"mechanism": "Rule 1: Farmers prefer corn — even with record soy oil prices + high fertilizer, 2022 June report showed corn above March prediction. Rule 2: Late corn kills soy — 2022 soy -2.63M from March (below analyst range). Rule 3: Technology effect — 50% corn in 2 weeks, 54% soy in 3 weeks (2022). Prevents planting must be MORE extreme than historical. Rule 4: Resurvey risk — USDA resurveys delayed states (ND/MN/SD) in July, reports in Aug Crop Production."}',
 'extracted', 0.90),

-- Quarterly residual model → stocks
((SELECT id FROM core.kg_node WHERE node_key = 'quarterly_residual_model'),
 (SELECT id FROM core.kg_node WHERE node_key = 'soybeans'),
 'PREDICTS', 0.90,
 '{"mechanism": "Export inspections first 2wk of quarter start vs end predicts residual direction. Soy 2023: Mar 43M, Jun 13M → 30M decline → implied -20M residual. HB adjusted to -13M → stocks 800M (mkt 805M). Soy residual clean. Corn complex: must estimate feed (GCAUs) + FSI (ethanol from EIA) separately. Feed estimates off 50-100M bu in any quarter = main error source. Record residual quarters rare but unpredictable (2021 Q3: 94M soy = record)."}',
 'extracted', 0.90),

-- June 30 report dynamics
((SELECT id FROM core.kg_node WHERE node_key = 'usda_june30_acreage'),
 (SELECT id FROM core.kg_node WHERE node_key = 'corn'),
 'CAUSES', 0.95,
 '{"mechanism": "Most volatile trading day. Two reports simultaneously = compounding surprises. 2021: corn limit-up, soy +90c. When BOTH acreage AND stocks surprise same direction, move amplified. 2022 contra: bullish soy acreage cut but soy fell 17.75c on recession fears. Macro can override even strong fundamental surprise. Framework: pre-position when own estimate diverges from consensus, assess proportionality of reaction on report day."}',
 'extracted', 0.95),

-- SCOTUS SRE → demand shock
((SELECT id FROM core.kg_node WHERE node_key = 'scotus_sre_ruling_2021'),
 (SELECT id FROM core.kg_node WHERE node_key = 'ethanol'),
 'CAUSES', 0.90,
 '{"mechanism": "Overturned 10th Circuit → 50+ pending SRE applications. Potential reduction: ethanol 520M-1.1B gal (185-400M bu corn), biodiesel 70-190M gal (525M-1.43B lbs feedstock). HB cut corn grind 250M bu. Admin options: mandate cut (Obama 2014 precedent: -270M gal), SRE grants, E15. Delta $350M compliance deficit betting on relief. Biden ultimately denied all SREs Dec 2021 (Batch 004). Jun-Dec uncertainty demonstrated policy risk dominating fundamentals for 6 months."}',
 'extracted', 0.90),

-- ND crop switch → canola
((SELECT id FROM core.kg_node WHERE node_key = 'north_dakota_planting_switch'),
 (SELECT id FROM core.kg_node WHERE node_key = 'canola_oil'),
 'CAUSES', 0.80,
 '{"mechanism": "ND marginal-acre bellwether. Late planting closes corn window → soy and canola gain. 2022: corn 3.6M→3.2M, soy 7M→7.4M, canola 1.76M→1.89M (all records for latter two). Canola CI advantage for biofuel + shorter season = preferred switch crop. US canola expansion partially offsets Canadian drought risk (Batch 001)."}',
 'extracted', 0.80),

-- Crop conditions early season context
((SELECT id FROM core.kg_node WHERE node_key = 'usda.crop_progress.development'),
 (SELECT id FROM core.kg_node WHERE node_key = 'soybeans'),
 'LEADS', 0.85,
 '{"mechanism": "Early season: initial condition decline from high starting point is NORMAL and does not imply yield loss. Jun 2021: soy G/E dropped from 67% to 62% in 2 weeks (lowest for week since 2012 but above 2019 initial 54%). N Plains worst: ND 24%, SD 45%. Only deviation from seasonal average matters per Batch 005 crop condition model. Rainfall can rapidly improve ratings in Jun-early Jul before development stages become more fixed."}',
 'extracted', 0.85),

-- RIN price as policy leading indicator
((SELECT id FROM core.kg_node WHERE node_key = 'rin.d6_d4_price'),
 (SELECT id FROM core.kg_node WHERE node_key = 'soybean_oil'),
 'LEADS', 0.85,
 '{"mechanism": "When RIN prices spike to ~$2/gal: (1) obligated parties build compliance deficits, (2) lobbying intensifies, (3) policy response within 3-6 months historically. When RINs collapse after policy response, biofuel demand reprices. Monitor: SRE application count, refiner lobbying, Congressional letters to EPA as leading indicators. RIN price sets implicit floor for feedstock demand — above mandate, demand is economic; below mandate, demand is regulatory."}',
 'extracted', 0.85);

-- CONTEXTS
INSERT INTO core.kg_context (node_id, context_type, context_key, context_value, applicable_when, source) VALUES

((SELECT id FROM core.kg_node WHERE node_key = 'usda_june30_acreage'),
 'expert_rule', 'june30_report_methodology',
 '{"acreage": "Step 1: planting completion date vs avg. Step 2: apply planting pace model. Step 3: state-level delayed states (ND/MN/SD) crop switch analysis. Step 4: compare to analyst range (corn above 7/10). Step 5: check resurvey risk. Stocks: Step 1: residual from export inspection comparison. Step 2: soy = known demand + residual. Step 3: corn = FSI (ethanol from EIA) + feed (GCAUs) + residual. Step 4: wheat = supply - known demand. When both acreage AND stocks surprise same direction, position aggressively."}',
 'pre_report', 'extracted'),

((SELECT id FROM core.kg_node WHERE node_key = 'acreage_rules_of_thumb'),
 'expert_rule', 'acreage_decision_tree',
 '{"early_corn": "Corn up from March, soy DOWN (counterintuitive). Check ND canola switch.", "on_time": "Corn unchanged/slightly up. Soy variable. Check price ratio.", "late_corn": "Corn may hold (tech effect) but soy DOWN significantly. Check prevent-plant threshold.", "overrides": "Extreme flooding overrides all. Soy/corn ratio >2.5 overrides pace. High fertilizer favors soy on margin but insufficient to override pace historically."}',
 'pre_planting', 'extracted'),

((SELECT id FROM core.kg_node WHERE node_key = 'rin.d6_d4_price'),
 'expert_rule', 'rfs_compliance_framework',
 '{"responses_to_rin_spike": "1: Mandate reduction (Obama 2014: -270M gal, targets ethanol, corn -120M bu). 2: SRE grants (Obama era 50% approval, Biden denied all). 3: E15 adoption (most bullish, unlikely). Compliance deficit = signal of political pressure building. Monitor SRE applications + refiner lobbying as leading indicators."}',
 'always', 'extracted'),

((SELECT id FROM core.kg_node WHERE node_key = 'quarterly_residual_model'),
 'expert_rule', 'stocks_estimation_cookbook',
 '{"soybeans": "Known demand (NOPA crush + Census exports + seed) + residual (inspections delta). Record residual possible (2021 Q3: 94M). Error range ±30M.", "corn": "FSI (ethanol from EIA + seasonal patterns) + feed (GCAUs × price × supply) + residual. Feed error 50-100M bu = main error source. USDA reports FSI only quarterly.", "wheat": "Supply - exports - food - seed - feed/residual. Feed main unknown. When corn price high, wheat feeding underestimated by USDA models."}',
 'pre_report', 'extracted');

-- SOURCE REGISTRY
INSERT INTO core.kg_source (source_key, source_type, title, location_uri, document_date, document_type, commodities, topics, status, first_processed, last_processed, nodes_extracted, edges_extracted, contexts_extracted) VALUES
('gdoc_1x7Hvf9K_Vs2m44HRYH4DYlVFNrAGx3f6gdADN2sivnA', 'gdrive_doc', 'HB Weekly Text - 06302022 (Acreage Report Reaction)', 'https://docs.google.com/document/d/1x7Hvf9K_Vs2m44HRYH4DYlVFNrAGx3f6gdADN2sivnA/edit', '2022-06-30', 'weekly_text', '{corn,soybeans,wheat}', '{acreage_report,soybean_area_cut,corn_preference,resurvey}', 'completed', NOW(), NOW(), 0, 0, 0),
('gdoc_1eR9VLiTO_4x4p138JOnS6tdF2S8AgiAErpU8QadaZZY', 'gdrive_doc', 'HB Weekly Text - 07012021 (Acreage+Stocks Reaction)', 'https://docs.google.com/document/d/1eR9VLiTO_4x4p138JOnS6tdF2S8AgiAErpU8QadaZZY/edit', '2021-07-01', 'weekly_text', '{corn,soybeans,wheat,soybean_oil}', '{acreage_report,grain_stocks,residual_use,sre_scotus,corn_limit_up}', 'completed', NOW(), NOW(), 0, 0, 0),
('gdoc_1957P49BNXuVilRZNJQlmOlBKxptw21pBtzVhgN62z9o', 'gdrive_doc', 'HB Weekly Text - 06242021 (Acreage+Stocks Preview)', 'https://docs.google.com/document/d/1957P49BNXuVilRZNJQlmOlBKxptw21pBtzVhgN62z9o/edit', '2021-06-24', 'weekly_text', '{corn,soybeans,wheat}', '{acreage_preview,stocks_methodology,residual,planting_pace}', 'completed', NOW(), NOW(), 0, 0, 0),
('gdoc_1L0xpfKfoao7Px-T92FdnQQ1eoGFxbgPqFRR6Jkf6Gmg', 'gdrive_doc', 'HB Weekly Text - 06292023 (Acreage+Stocks Preview 2023)', 'https://docs.google.com/document/d/1L0xpfKfoao7Px-T92FdnQQ1eoGFxbgPqFRR6Jkf6Gmg/edit', '2023-06-29', 'weekly_text', '{corn,soybeans,wheat}', '{planting_pace_model,acreage_statistics,residual_model}', 'completed', NOW(), NOW(), 0, 0, 0),
('gdoc_1TdzqlTlD4vAQEbRo2vBfXwT9hNht7_XF5F5S-4z0m7o', 'gdrive_doc', 'HB Weekly Text - 06172021 (RIN Crisis + Biofuel Policy)', 'https://docs.google.com/document/d/1TdzqlTlD4vAQEbRo2vBfXwT9hNht7_XF5F5S-4z0m7o/edit', '2021-06-17', 'weekly_text', '{corn,soybeans,ethanol,soybean_oil}', '{rin_prices,sre_policy,mandate_reduction,scotus,e15}', 'completed', NOW(), NOW(), 0, 0, 0),
('gdoc_17Az6aJWCwmw0HGnECi4TjfrCPcnusplUAqRfCJxZ6AI', 'gdrive_doc', 'HB Weekly Text - 06232022 (Acreage+Stocks Preview 2022)', 'https://docs.google.com/document/d/17Az6aJWCwmw0HGnECi4TjfrCPcnusplUAqRfCJxZ6AI/edit', '2022-06-23', 'weekly_text', '{corn,soybeans,wheat,canola_oil}', '{north_dakota_switch,canola_expansion,stocks_methodology}', 'completed', NOW(), NOW(), 0, 0, 0)
ON CONFLICT (source_key) DO UPDATE SET status = 'completed', last_processed = NOW();
