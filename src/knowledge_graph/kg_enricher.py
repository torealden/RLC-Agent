"""
Knowledge Graph Enricher

Produces analyst-style context for data collection events by querying
the KG for relevant nodes, edges, and contexts. Used by collector_runner
to enrich event_log entries so the LLM briefing contains actionable
intelligence rather than raw row counts.

Usage:
    from src.knowledge_graph import KGEnricher

    enricher = KGEnricher()
    result = enricher.enrich_collection_event('cftc_cot', 312, 'week_ending_2026-02-07')
    # Returns dict with kg_context, kg_relationships, enriched_summary
"""

import logging
from typing import Dict, List, Optional, Any

from src.knowledge_graph.kg_manager import KGManager

logger = logging.getLogger(__name__)

# Maps collector schedule keys to KG node_keys that provide relevant context.
# When a collector finishes, we look up these nodes to enrich the event summary.
COLLECTOR_KG_MAP: Dict[str, List[str]] = {
    'cftc_cot': ['cftc.cot', 'managed_money', 'corn', 'soybeans'],
    'usda_nass_crop_progress': ['usda.crop_condition_rating', 'usda.crop_progress.development', 'crop_condition_yield_model'],
    'usda_fas_psd': ['usda.wasde.revision_pattern', 'usda.wasde.aug_yield_change'],
    'eia_ethanol': ['eia.ethanol', 'ethanol'],
    'eia_petroleum': ['crude_oil', 'heating_oil'],
    'conab_production': ['brazil.conab', 'brazil.imea', 'brazil.stu'],
    'usda_fas_export_sales': ['usda.export_sales', 'usda.flash_sales'],
    'usda_fgis_inspections': ['usda.fgis'],
    'epa_emts': ['epa.emts', 'rfs2', 'rvo'],
    'nopa_crush': ['nopa.crush', 'crush_margin.board', 'oil_share'],
    'nass_processing': ['usda.fats_oils', 'nopa.crush'],
    'census_trade': ['usda.export_sales'],
    'usda_nass_acreage': ['fsa.acreage', 'planting_pace_acreage_model', 'acreage_rules_of_thumb'],
    'usda_grain_stocks': ['usda.grain_stocks', 'quarterly_residual_model', 'usda.grain_stocks.feed_residual'],
    'drought_monitor': ['peak_weather_sensitivity', 'corn_pollination_window', 'soybean_pod_fill_aug'],
}


class KGEnricher:
    """
    Enriches collection events with Knowledge Graph context.

    When a collector finishes, this class looks up related KG nodes
    and produces a summary of analyst-relevant context that gets
    written into the event_log details JSONB.
    """

    def __init__(self):
        self.kg = KGManager()

    def enrich_collection_event(
        self,
        collector_name: str,
        rows_collected: int = 0,
        data_period: Optional[str] = None,
    ) -> Optional[Dict[str, Any]]:
        """
        Look up KG context relevant to a collector and return enrichment data.

        Args:
            collector_name: The collector schedule key (e.g., 'cftc_cot')
            rows_collected: Number of rows collected
            data_period: Data period string (e.g., 'week_ending_2026-02-07')

        Returns:
            Dict with keys:
              - 'related_nodes': list of node summaries
              - 'key_contexts': list of relevant expert rules/thresholds
              - 'key_relationships': list of relevant edge summaries
              - 'enriched_summary': one-line analyst context string
            Or None if no relevant KG data found.
        """
        node_keys = COLLECTOR_KG_MAP.get(collector_name)
        if not node_keys:
            return None

        related_nodes = []
        key_contexts = []
        key_relationships = []

        for node_key in node_keys:
            try:
                enriched = self.kg.get_enriched_context(node_key)
                if enriched is None:
                    continue

                node = enriched['node']
                related_nodes.append({
                    'node_key': node['node_key'],
                    'label': node['label'],
                    'node_type': node['node_type'],
                })

                # Extract the most useful context entries
                for ctx in enriched.get('contexts', []):
                    if ctx.get('context_type') in ('expert_rule', 'risk_threshold'):
                        key_contexts.append({
                            'node_key': node['node_key'],
                            'context_type': ctx['context_type'],
                            'context_key': ctx['context_key'],
                            'summary': _summarize_context(ctx),
                        })

                # Extract outgoing causal/predictive edges
                for edge in enriched.get('edges', []):
                    if edge.get('edge_type') in ('CAUSES', 'PREDICTS', 'LEADS', 'COMPETES_WITH'):
                        key_relationships.append({
                            'edge_type': edge['edge_type'],
                            'source': edge.get('source_label', edge.get('source_key', '')),
                            'target': edge.get('target_label', edge.get('target_key', '')),
                            'summary': _summarize_edge(edge),
                        })

            except Exception as e:
                logger.debug(f"KG lookup failed for {node_key}: {e}")
                continue

        if not related_nodes:
            return None

        # Build a concise enrichment summary
        enriched_summary = _build_summary(
            collector_name, related_nodes, key_contexts, key_relationships
        )

        return {
            'related_nodes': related_nodes[:10],  # Cap at 10
            'key_contexts': key_contexts[:5],      # Cap at 5 most relevant
            'key_relationships': key_relationships[:5],
            'enriched_summary': enriched_summary,
        }

    def get_collector_kg_map(self) -> Dict[str, List[str]]:
        """Return the collector → KG node mapping for inspection."""
        return dict(COLLECTOR_KG_MAP)


def _summarize_context(ctx: Dict) -> str:
    """Extract a short summary from a context entry."""
    cv = ctx.get('context_value', {})
    if isinstance(cv, dict):
        # Look for common summary keys
        for key in ('rule', 'framework', 'model', 'methodology'):
            if key in cv:
                text = str(cv[key])
                return text[:200] + '...' if len(text) > 200 else text
        # Fall back to context_key
        return ctx.get('context_key', '')
    return str(cv)[:200]


def _summarize_edge(edge: Dict) -> str:
    """Extract a short summary from an edge."""
    props = edge.get('properties', {})
    if isinstance(props, dict) and 'mechanism' in props:
        text = str(props['mechanism'])
        return text[:150] + '...' if len(text) > 150 else text
    return f"{edge.get('source_key', '?')} {edge.get('edge_type', '?')} {edge.get('target_key', '?')}"


def _build_summary(
    collector_name: str,
    nodes: List[Dict],
    contexts: List[Dict],
    relationships: List[Dict],
) -> str:
    """Build a concise one-line enrichment summary."""
    parts = []

    node_labels = [n['label'] for n in nodes[:3]]
    if node_labels:
        parts.append(f"Related: {', '.join(node_labels)}")

    if contexts:
        parts.append(f"{len(contexts)} expert rule(s)")

    if relationships:
        edge_types = sorted(set(r['edge_type'] for r in relationships))
        parts.append(f"Links: {', '.join(edge_types)}")

    return ' | '.join(parts) if parts else ''
