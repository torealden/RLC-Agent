-- ============================================================
-- Knowledge Graph Extraction: Batch 009e
-- Source: HOBO Study - Section 6: Strategic Assessment (SWOT)
-- Date: 2026-02-14
-- ============================================================

INSERT INTO kg_source (source_key, source_type, title, url, report_date, category, commodities, themes, status, first_seen, last_processed, node_count, edge_count, context_count)
VALUES
('gdoc_hobo_section6', 'gdrive_doc', 'HOBO Study - Section 6: Strategic Assessment (SWOT)', 'https://docs.google.com/document/d/1TWHAEpSlElgX58QvPimSgVG3X3IgenEXUPknuKsBLVQ/edit', '2025-06-19', 'consulting_report', '{renewable_diesel,saf,uco,tallow,dcor,soybean_oil,yellow_grease}', '{swot,competitive_strategy,feedstock_security,credit_arbitrage,policy_risk}', 'completed', NOW(), NOW(), 4, 6, 6)
ON CONFLICT (source_key) DO UPDATE SET status = 'completed', last_processed = NOW();

-- NODES
INSERT INTO kg_node (node_key, node_type, label, description, metadata)
VALUES
('hobo_h2_recycling', 'technology', 'HOBO H2 Off-Gas Recycling',
 '75% of HOBO hydrogen feedstock produced from recycling process off-gases from the renewable feedstock itself. This CI advantage yields CI in low 20s gCO2/MJ vs industry average upper 30s. More valuable product via additional credits.',
 '{"recycling_pct": 75, "ci_result": "low_20s", "industry_avg": "upper_30s"}'),
('credit_arbitrage_strategy', 'strategy', 'RD/SAF Credit Arbitrage Strategy',
 'HOBO strategy of toggling between RD and SAF output to maximize credit arbitrage. Increase SAF for SAF-specific tax credits, shift to RD if diesel RIN/LCFS credits make that more profitable. Dynamic optimization could enhance revenues vs single-product peers.',
 '{"mechanism": "product_slate_flexibility", "optimization": "continuous"}'),
('yellow_grease_price', 'metric', 'Yellow Grease (UCO) Price Volatility',
 'US yellow grease prices spiked to ~60cts/lb in 2022 from under 25cts two years prior, fell back to mid-30cts by 2024. Illustrates how quickly feedstock economics can change and importance of price risk management.',
 '{"price_2020_per_lb": "<0.25", "price_2022_peak_per_lb": 0.60, "price_2024_per_lb": "mid_0.30s"}'),
('atj_competitive_threat', 'technology', 'ATJ as Competitive Threat to HEFA SAF',
 'Alcohol-to-Jet technology may commercialize in 2030s. Whether threat or opportunity for HEFA depends on ATJ conversion efficiency and ethanol feedstock cost. If higher cost than HEFA, ATJ becomes the price setter and HEFA retains margin advantage.',
 '{"timeline": "2030s", "key_variable": "ethanol_cost", "dual_outcome": "threat_or_price_setter"}')
ON CONFLICT (node_key) DO UPDATE SET description = EXCLUDED.description, metadata = EXCLUDED.metadata;

-- EDGES
INSERT INTO kg_edge (source_node, target_node, edge_type, label, weight, metadata)
VALUES
('hobo_h2_recycling', 'hobo_ci_advantage', 'DRIVES', '75% H2 recycling from off-gases is primary driver of HOBO CI advantage (low 20s vs upper 30s)', 0.90,
 '{"mechanism": "reduced_fossil_h2_input", "confidence": "high"}'),
('credit_arbitrage_strategy', 'hobo_renewables', 'ADVANTAGES', 'Product slate flexibility enables credit arbitrage — maximize whichever product (RD/SAF) yields highest value at any given time', 0.85,
 '{"mechanism": "dynamic_optimization", "confidence": "high"}'),
('cfpc_45z', 'hobo_renewables', 'THREATENS', '45Z expiry/change is primary policy risk. ILUC removal from CI calculation would eliminate differentiation between waste and crop feedstocks, changing HOBO economics', 0.85,
 '{"risk": "iluc_removal_reduces_waste_oil_advantage", "confidence": "high"}'),
('yellow_grease_price', 'hobo_renewables', 'THREATENS', 'Feedstock price volatility (UCO 25cts→60cts→35cts in 4 years) directly impacts margins. Competitors bidding up regional feedstock a major risk', 0.85,
 '{"mechanism": "feedstock_cost_volatility", "confidence": "high"}'),
('atj_competitive_threat', 'hefa_technology', 'COMPETES_WITH', 'ATJ may commercialize in 2030s as alternative SAF pathway. Could cap HEFA SAF demand OR become higher-cost price setter that protects HEFA margins', 0.70,
 '{"timeline": "2030s", "confidence": "moderate"}'),
('hobo_renewables', 'canada_cfr', 'TARGETS', 'HOBO targets Canadian markets (Calgary, Winnipeg, Alberta aviation) as relatively undersupplied for RD/SAF. Canada prices at CA-plus-transport to attract barrels, where HOBO has lowest delivered cost', 0.80,
 '{"mechanism": "proximity_cost_advantage", "confidence": "high"}')
ON CONFLICT DO NOTHING;

-- CONTEXTS
INSERT INTO kg_context (context_key, node_or_edge_ref, context_type, content, confidence, metadata)
VALUES
('hobo_swot_strengths', 'hobo_renewables', 'strategic_assessment',
 'STRENGTHS: (1) Iowa location in feedstock-rich region with low-cost waste oils; (2) H2 off-gas recycling delivers CI in low 20s vs industry upper 30s = more valuable product; (3) Multi-modal logistics (rail, barge, highway) reaching coastal markets and Canada; (4) Flexible RD/SAF product slate enabling credit arbitrage; (5) Access to Midwest, Mountain, Northeast US + Central/Maritime Canada markets.',
 0.90, '{"source": "hobo_section6", "type": "swot_s"}'),

('hobo_swot_weaknesses', 'hobo_renewables', 'strategic_assessment',
 'WEAKNESSES: (1) Greenfield startup requiring several hundred million $ without oil major backing; (2) No existing feedstock collection network or downstream distribution assets; (3) Market timing risk — coming online late 2020s potentially as incentives expire or market oversupplies; (4) Lack of vertical integration exposes to higher input costs and third-party dependency.',
 0.85, '{"source": "hobo_section6", "type": "swot_w"}'),

('hobo_swot_opportunities', 'hobo_renewables', 'strategic_assessment',
 'OPPORTUNITIES: (1) RD/SAF credit arbitrage via flexible production; (2) Canadian low-carbon fuel markets relatively undersupplied; (3) IRA 45Z incentives could significantly boost early-year profits if operational before phase-out; (4) Strategic partnerships (airline SAF offtake, logistics companies, feedstock providers); (5) Niche positioning as one of few Midwest SAF producers.',
 0.85, '{"source": "hobo_section6", "type": "swot_o"}'),

('hobo_swot_threats', 'hobo_renewables', 'strategic_assessment',
 'THREATS: (1) Policy uncertainty — 45Z expiry/change, ILUC removal would reshape economics; (2) Feedstock price volatility (yellow grease 25cts→60cts→35cts in 4 years); (3) Competitor feedstock lock-up in region; (4) ATJ/e-fuels commercializing in 2030s; (5) Potential oversupply as multiple HEFA projects come online simultaneously.',
 0.85, '{"source": "hobo_section6", "type": "swot_t"}'),

('iluc_removal_impact', 'cfpc_45z', 'policy_scenario',
 'CRITICAL SCENARIO: If ILUC is removed from 45Z CI calculation, the CI differentiation between soybean oil and waste feedstocks (UCO, DCO, tallow) would narrow substantially. This would: (1) Directionally increase soybean oil competitiveness for 45Z; (2) Reduce the premium for waste oil access that HOBO relies on; (3) Directionally incentivize RD into California (where LCFS still includes ILUC) relative to SAF (more US-homogeneous pricing). HOBO must monitor and adapt feedstock/market strategy accordingly.',
 0.90, '{"source": "hobo_section6", "critical": true}'),

('canadian_market_underserved', 'canada_cfr', 'market_assessment',
 'Canada CFR + provincial mandates creating growing pull for RD/SAF imports, especially Western Canada. Market prices at California-plus-transport to attract barrels. HOBO has lowest delivered cost due to Midwest proximity. Key outlets: Calgary, Winnipeg, BC, Ontario aviation. By exporting to Canada, HOBO may tap additional credit premiums under CFR/provincial CI regimes while avoiding California market saturation.',
 0.85, '{"source": "hobo_section6", "strategy": "avoid_ca_saturation"}')
ON CONFLICT (context_key) DO UPDATE SET content = EXCLUDED.content, confidence = EXCLUDED.confidence;
