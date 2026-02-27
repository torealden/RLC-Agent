-- ============================================================================
-- RLC-Agent CNS: Knowledge Graph (Analyst Brain)
-- ============================================================================
-- File: 020_knowledge_graph.sql
-- Purpose: The analyst's mental model stored as a graph. Nodes are entities
--          (commodities, data series, reports, regions, policies). Edges are
--          relationships (CAUSES, COMPETES_WITH, SUBSTITUTES, LEADS).
--          Contexts are enrichment (seasonal norms, risk thresholds, expert rules).
--          This is what makes the Desktop LLM think like a commodity analyst.
-- Depends: 01_schemas.sql (core schema)
-- Source:  WIRING_PLAN.md Part 3
-- ============================================================================

-- ---------------------------------------------------------------------------
-- 1. KG_NODE: Entities in the commodity analytical universe
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS core.kg_node (
    id          SERIAL PRIMARY KEY,
    node_type   TEXT NOT NULL,          -- 'data_series', 'commodity', 'region',
                                       -- 'report', 'policy', 'seasonal_event',
                                       -- 'market_participant', 'balance_sheet_line',
                                       -- 'price_level'
    node_key    TEXT NOT NULL UNIQUE,   -- 'cftc.corn.managed_money_net'
    label       TEXT NOT NULL,          -- 'CFTC Corn Managed Money Net Position'
    properties  JSONB DEFAULT '{}'      -- Flexible metadata
);

CREATE INDEX IF NOT EXISTS idx_kg_node_type ON core.kg_node(node_type);
CREATE INDEX IF NOT EXISTS idx_kg_node_properties ON core.kg_node USING GIN(properties);

COMMENT ON TABLE core.kg_node IS
    'Knowledge Graph nodes: commodities, data series, reports, regions, policies, '
    'seasonal events, market participants, balance sheet lines, price levels.';


-- ---------------------------------------------------------------------------
-- 2. KG_EDGE: Relationships — the analyst's mental model
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS core.kg_edge (
    id              SERIAL PRIMARY KEY,
    source_node_id  INTEGER REFERENCES core.kg_node(id),
    target_node_id  INTEGER REFERENCES core.kg_node(id),
    edge_type       TEXT NOT NULL,      -- 'CAUSES', 'COMPETES_WITH', 'SUBSTITUTES',
                                       -- 'LEADS', 'SEASONAL_PATTERN', 'RISK_THRESHOLD',
                                       -- 'CROSS_MARKET', 'TRIGGERS', 'SUPPLIES',
                                       -- 'CONSUMES'
    weight          NUMERIC DEFAULT 1.0,-- Strength of relationship (0-1)
    properties      JSONB DEFAULT '{}', -- Flexible: mechanism, lag, direction, evidence
    created_by      TEXT DEFAULT 'manual', -- manual | extracted | derived | learned
    confidence      NUMERIC DEFAULT 1.0   -- 1.0 = certain, <1.0 = inferred
);

CREATE INDEX IF NOT EXISTS idx_kg_edge_source ON core.kg_edge(source_node_id);
CREATE INDEX IF NOT EXISTS idx_kg_edge_target ON core.kg_edge(target_node_id);
CREATE INDEX IF NOT EXISTS idx_kg_edge_type ON core.kg_edge(edge_type);

COMMENT ON TABLE core.kg_edge IS
    'Knowledge Graph edges: relationships between nodes. This is the core of the '
    'analyst brain — causal links, cross-market dynamics, substitution hierarchies.';


-- ---------------------------------------------------------------------------
-- 3. KG_CONTEXT: Enrichment — the "so what" for each node
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS core.kg_context (
    id              SERIAL PRIMARY KEY,
    node_id         INTEGER REFERENCES core.kg_node(id),
    context_type    TEXT NOT NULL,       -- 'seasonal_norm', 'percentile_range',
                                        -- 'risk_threshold', 'historical_analog',
                                        -- 'expert_rule', 'causal_chain'
    context_key     TEXT NOT NULL,       -- 'feb_normal_range', '5yr_percentile'
    context_value   JSONB NOT NULL,      -- The actual context data
    applicable_when TEXT,                -- SQL-like condition or 'always'
    source          TEXT DEFAULT 'analyst', -- analyst | extracted | computed | learned
    last_updated    TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_kg_context_node ON core.kg_context(node_id);
CREATE INDEX IF NOT EXISTS idx_kg_context_type ON core.kg_context(context_type);

COMMENT ON TABLE core.kg_context IS
    'Knowledge Graph context: enrichment data for nodes. Seasonal norms, risk '
    'thresholds, expert rules, historical analogs, causal chains.';


-- ---------------------------------------------------------------------------
-- Verification
-- ---------------------------------------------------------------------------
DO $$
BEGIN
    RAISE NOTICE '020_knowledge_graph.sql executed successfully:';
    RAISE NOTICE '  - core.kg_node: table created';
    RAISE NOTICE '  - core.kg_edge: table created';
    RAISE NOTICE '  - core.kg_context: table created';
    RAISE NOTICE '  - Indexes created on all KG tables';
END $$;
