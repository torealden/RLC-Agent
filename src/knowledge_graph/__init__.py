"""
RLC-Agent Knowledge Graph Package

Provides programmatic access to the analyst knowledge graph
stored in core.kg_node, core.kg_edge, and core.kg_context.
"""

from src.knowledge_graph.kg_manager import KGManager
from src.knowledge_graph.kg_enricher import KGEnricher
from src.knowledge_graph.seasonal_calculator import SeasonalCalculator
from src.knowledge_graph.pace_calculator import PaceCalculator

__all__ = ['KGManager', 'KGEnricher', 'SeasonalCalculator', 'PaceCalculator']
