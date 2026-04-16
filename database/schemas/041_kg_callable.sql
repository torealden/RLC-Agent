-- =============================================================================
-- Migration 041: kg_callable — executable models attached to KG nodes
-- =============================================================================
-- Purpose
-- -------
-- The knowledge graph stores narrative frameworks (expert rules, seasonal
-- norms, causal chains) as kg_context JSONB. Those tell an LLM *what* matters.
-- kg_callable layers on a parallel table that lets the LLM *compute* rather
-- than just cite: each row is a typed, testable function with a signature,
-- implementation, defaults, test cases, and a pointer back to the narrative
-- context that documents its logic.
--
-- Modes of use:
--   1. User scenario — caller supplies all inputs, gets a number
--   2. Self-exploration — caller asks the callable to sweep its own ranges
--      and report sensitivities, breakpoints, secondary effects. The sweep
--      spec lives in self_exploration JSONB.
--
-- See project_symbiotic_forecasting.md for the broader endpoint this feeds.
-- =============================================================================

CREATE TABLE IF NOT EXISTS core.kg_callable (
    id                  SERIAL PRIMARY KEY,

    -- Identity
    callable_key        TEXT NOT NULL UNIQUE,
    node_id             INTEGER NOT NULL REFERENCES core.kg_node(id) ON DELETE CASCADE,
    label               TEXT NOT NULL,
    description         TEXT,

    -- Classification
    callable_type       TEXT NOT NULL CHECK (callable_type IN (
        'formula',       -- pure math, no I/O (e.g., stack = ulsd + rin + lcfs)
        'sql',           -- parameterized SELECT against gold/silver
        'python',        -- Python function (module.func) - for complex logic
        'sensitivity',   -- linear coefficient (e.g., $0.05/lb = $0.35-0.40/gal)
        'composite'      -- wraps multiple callables
    )),

    -- Contract
    signature           JSONB NOT NULL,
        -- { "inputs":  { "<name>": {"type": "float|int|enum|date|str",
        --                            "range": [lo, hi] | "values": [...],
        --                            "units": "<unit>",
        --                            "required": bool,
        --                            "source": "<data_series node_key>" optional } },
        --   "output":  { "name": "...", "type": "float|dict", "units": "<unit>" } }

    implementation      TEXT NOT NULL,
        -- For formula: a Python expression string. Whitelisted namespace.
        -- For sql:     a parameterized SELECT (e.g., "SELECT ... WHERE commodity=%(c)s")
        -- For python:  dotted path to a module-level function (e.g., "src.kg.callables.weather_yield.run")
        -- For sensitivity: JSON with coefficient + baseline
        -- For composite: JSON with sub-callable keys + composition rule

    defaults            JSONB,           -- fallback input values
    units               JSONB,           -- {input_name → unit, "output" → unit}
    test_cases          JSONB,           -- [{inputs:{...}, expected:{...}, tolerance:0.05}, ...]

    -- Self-exploration (Mode 2)
    self_exploration    JSONB,
        -- { "sweep_params": ["param1", "param2"],
        --   "ranges": { "param1": {"min": x, "max": y, "step": z | "values": [...]} },
        --   "baseline": { <param>: <value> },
        --   "downstream_nodes": ["node_key1", ...],   -- re-evaluate on threshold cross
        --   "threshold_rules": [{ "when": "<expr>", "surface": "<message>" }],
        --   "report_format": "sensitivity_table | breakpoint_scan | scenario_grid" }

    -- Provenance
    source_context_id   INTEGER REFERENCES core.kg_context(id) ON DELETE SET NULL,
    source_note         TEXT,            -- e.g., "HB Sep 2022 WASDE preview"
    confidence          NUMERIC(3,2),    -- 0.00-1.00

    -- Validation
    last_validated_at   TIMESTAMPTZ,
    last_validated_pass BOOLEAN,
    last_validated_note TEXT,

    -- Lifecycle
    status              TEXT NOT NULL DEFAULT 'draft' CHECK (status IN (
        'draft', 'active', 'deprecated', 'retired'
    )),
    created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_kg_callable_node ON core.kg_callable(node_id);
CREATE INDEX IF NOT EXISTS idx_kg_callable_status ON core.kg_callable(status);
CREATE INDEX IF NOT EXISTS idx_kg_callable_type ON core.kg_callable(callable_type);

-- =============================================================================
-- Invocation log — every call the LLM (or anyone) makes to a kg_callable.
-- Lets us audit, replay, and measure callable quality over time.
-- =============================================================================

CREATE TABLE IF NOT EXISTS core.kg_callable_invocation (
    id               BIGSERIAL PRIMARY KEY,
    callable_id      INTEGER NOT NULL REFERENCES core.kg_callable(id) ON DELETE CASCADE,
    invoked_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    invoked_by       TEXT,                    -- 'mcp', 'dispatcher', 'test', etc.
    mode             TEXT NOT NULL CHECK (mode IN ('scenario', 'self_exploration')),
    inputs           JSONB NOT NULL,
    output           JSONB,
    warnings         JSONB,
    error_message    TEXT,
    duration_ms      INTEGER,
    citations        JSONB                    -- resolved source docs / context ids
);

CREATE INDEX IF NOT EXISTS idx_kg_callable_inv_callable ON core.kg_callable_invocation(callable_id, invoked_at DESC);

-- =============================================================================
-- Helper view: callables with their parent node + source context
-- =============================================================================

CREATE OR REPLACE VIEW core.kg_callable_detail AS
SELECT
    c.id,
    c.callable_key,
    c.label,
    c.callable_type,
    c.status,
    n.node_key        AS node_key,
    n.node_type       AS node_type,
    n.label           AS node_label,
    c.signature,
    c.defaults,
    c.units,
    c.self_exploration IS NOT NULL AS has_self_exploration,
    c.test_cases      IS NOT NULL AS has_tests,
    c.confidence,
    c.last_validated_at,
    c.last_validated_pass,
    ctx.context_type  AS source_context_type,
    ctx.context_key   AS source_context_key
FROM core.kg_callable c
JOIN core.kg_node n   ON n.id = c.node_id
LEFT JOIN core.kg_context ctx ON ctx.id = c.source_context_id;

COMMENT ON TABLE core.kg_callable IS 'Executable models attached to KG nodes. LLM invokes these rather than reasoning through the math from scratch.';
COMMENT ON TABLE core.kg_callable_invocation IS 'Audit trail of every kg_callable invocation, for replay and quality measurement.';
