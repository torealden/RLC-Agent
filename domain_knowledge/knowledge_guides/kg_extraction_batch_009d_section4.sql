-- ============================================================
-- Knowledge Graph Extraction: Batch 009d
-- Source: HOBO Study - Section 4: Project Overview
-- Date: 2026-02-14
-- ============================================================

INSERT INTO kg_source (source_key, source_type, title, url, report_date, category, commodities, themes, status, first_seen, last_processed, node_count, edge_count, context_count)
VALUES
('gdoc_hobo_section4', 'gdrive_doc', 'HOBO Study - Section 4: Project Overview - Clinton County Facility', 'https://docs.google.com/document/d/1qc-BOvut86TdqCXi7BL_gHUEBBE_gClCXiszCvONwYE/edit', '2025-06-19', 'consulting_report', '{renewable_diesel,saf}', '{facility_design,clinton_county,logistics,capacity,hobo_spread}', 'completed', NOW(), NOW(), 4, 4, 3)
ON CONFLICT (source_key) DO UPDATE SET status = 'completed', last_processed = NOW();

-- NODES
INSERT INTO kg_node (node_key, node_type, label, description, metadata)
VALUES
('clinton_county_site', 'location', 'Clinton County, Iowa Facility Site',
 'Proposed HOBO facility location. In heart of agricultural center with proximity to abundant feedstock (SBO, corn oil, animal fats). Multi-modal transport access: Mississippi River barges, rail lines with spur, highways. Strategic for both inbound feedstock and outbound distribution.',
 '{"state": "Iowa", "county": "Clinton", "transport": ["barge", "rail", "highway"], "river": "Mississippi"}'),
('hobo_facility_specs', 'asset', 'HOBO HEFA Facility Specifications',
 'Output exceeding 125 MMgy of RD and SAF. ~9,300 bpd capacity — among the larger standalone RD plants in the US. Flexible product slate (can swing between SAF and RD). Built-for-purpose greenfield design vs repurposed refineries.',
 '{"capacity_mgy": 125, "capacity_bpd": 9300, "design": "greenfield_built_for_purpose", "flexibility": "RD_SAF_swing"}'),
('hobo_spread_concept', 'metric', 'HOBO Spread (Heating Oil minus Bean Oil)',
 'The company name HOBO reflects the Heating Oil-to-Bean Oil price spread — the core economics of converting soybean/bio oils into diesel-range fuels. This spread is the fundamental profitability indicator for RD production.',
 '{"formula": "heating_oil_price - soybean_oil_price", "significance": "core_RD_economics"}'),
('mississippi_river_access', 'infrastructure', 'Mississippi River Barge Access',
 'HOBO site offers Mississippi River barge access for optional bulk transport. Combined with rail spur and highway access creates multi-modal logistics flexibility for feedstock inbound and product outbound.',
 '{"mode": "barge", "benefit": "bulk_transport_flexibility"}')
ON CONFLICT (node_key) DO UPDATE SET description = EXCLUDED.description, metadata = EXCLUDED.metadata;

-- EDGES
INSERT INTO kg_edge (source_node, target_node, edge_type, label, weight, metadata)
VALUES
('clinton_county_site', 'hobo_renewables', 'HOSTS', 'Clinton County site provides multi-modal logistics and feedstock proximity', 0.95,
 '{"confidence": "very_high"}'),
('hobo_spread_concept', 'rd_market', 'MEASURES', 'HOBO spread (HO-BO) is the fundamental profitability measure for converting bio-oils to diesel', 0.95,
 '{"confidence": "very_high", "cross_ref": "batch_001_hobo_spread"}'),
('mississippi_river_access', 'clinton_county_site', 'CONNECTS', 'Mississippi River provides barge access for bulk feedstock/product transport', 0.85,
 '{"confidence": "high"}'),
('hobo_facility_specs', 'hobo_renewables', 'DEFINES', 'HOBO facility designed at 125+ MMgy with RD/SAF swing capability — among largest standalone US RD plants', 0.95,
 '{"confidence": "very_high"}')
ON CONFLICT DO NOTHING;

-- CONTEXTS
INSERT INTO kg_context (context_key, node_or_edge_ref, context_type, content, confidence, metadata)
VALUES
('hobo_name_etymology', 'hobo_spread_concept', 'expert_rule',
 'HOBO stands for Heating Oil minus Bean Oil — the fundamental spread that determines RD production economics. This is the same HOBO spread identified in Batch 001 as the "master lead indicator" for D4 RIN direction and biodiesel production economics from the RIN forecast reports. The company literally named itself after the spread that defines its profitability.',
 0.95, '{"source": "hobo_section4", "cross_ref": "batch_001_hobo_as_lead_indicator"}'),

('greenfield_vs_repurposed', 'hobo_facility_specs', 'competitive_advantage',
 'Most recent RD/SAF projects repurposed existing oil refineries for quick execution and existing logistics. But this creates disadvantages: located 500-1500mi from primary feedstock basin, not built-for-purpose (limits feedstock flexibility and CI), pre-existing infrastructure constrains feedstock types processable. HOBO as greenfield has short supply chains, full design optimization, and broader feedstock flexibility — turning the apparent disadvantage of no existing infrastructure into a key strength.',
 0.90, '{"source": "hobo_section4", "strategy": "greenfield_advantage"}'),

('outbound_market_reach', 'clinton_county_site', 'strategic_assessment',
 'Clinton County outbound logistics advantage: proximal to Midwest, Mountain, and Northeast US markets (45% of US population centers). Rail access to West Coast/California. Mississippi barge to Gulf. Also advantaged for Central, Rockies, Central and Maritime Canada. Can optimize placement of RD vs SAF into respective highest-priced markets (e.g., RD into Canada, SAF into Illinois).',
 0.90, '{"source": "hobo_section4", "population_reach_pct": 45}')
ON CONFLICT (context_key) DO UPDATE SET content = EXCLUDED.content, confidence = EXCLUDED.confidence;
