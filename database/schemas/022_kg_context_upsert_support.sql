-- ============================================================================
-- Migration 022: Add unique constraint for idempotent context upserts
-- Required by Phase 5 automated calculators (seasonal_calculator, etc.)
-- ============================================================================

-- The combination of node_id + context_type + context_key uniquely identifies
-- a context entry. This enables ON CONFLICT DO UPDATE for computed contexts.
-- Example: node 'corn' + type 'seasonal_norm' + key 'cftc_mm_net_monthly'
ALTER TABLE core.kg_context
    ADD CONSTRAINT uq_kg_context_node_type_key
    UNIQUE (node_id, context_type, context_key);
