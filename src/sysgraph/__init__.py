"""System graph extraction.

Static extraction of lineage across three substrates -- Python/VBA/SQL code, the PostgreSQL
catalog, and the Excel estate -- into the `sys` schema.

Design: docs/specs/system_knowledge_graph_design_v1.md
Migration: database/migrations/146_sys_system_graph.sql

Ruling (design section 3): v1 is static extraction, not runtime instrumentation. The prior
model, `audit.lineage_edge`, required every script to voluntarily call add_lineage_edge();
across 636 Python files adoption was ~0. Rebuild the graph from scratch on demand instead.
"""

EXTRACTOR_VERSION = "1.0.0"
