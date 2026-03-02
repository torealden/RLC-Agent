"""
Base Analysis Template

Abstract base class for all analysis templates in the autonomous pipeline.
Phase 2 templates (e.g., WASDEAnalysisTemplate) subclass this and implement
the abstract methods. Follows KGManager pattern for lazy DB/KG connections.

Usage:
    class WASDEAnalysisTemplate(BaseAnalysisTemplate):
        template_id = 'wasde_monthly'
        report_type = 'wasde'
        prompt_template_id = 'wasde_analysis'
        required_collectors = ['usda_wasde']
        trigger_mode = 'event'
        trigger_collector = 'usda_wasde'
        kg_node_keys = ['corn', 'soybeans', 'wheat', 'usda.wasde.revision_pattern']

        def check_data_ready(self): ...
        def gather_data(self): ...
        def compute_analysis(self, data): ...
"""

import logging
from abc import ABC, abstractmethod
from typing import Dict, List, Optional

logger = logging.getLogger(__name__)


class BaseAnalysisTemplate(ABC):
    """
    Abstract base for analysis templates.

    Subclasses define what data they need, how to gather it, and how to
    compute derived analysis. The build_prompt_context() method orchestrates
    the full flow: check_data_ready -> gather_data -> compute_analysis ->
    get_kg_context -> combine into a prompt context dict.
    """

    # --- Subclass must set these ---
    template_id: str = ''
    report_type: str = ''
    prompt_template_id: str = ''       # Links to PromptRegistry template
    required_collectors: List[str] = []
    trigger_mode: str = 'event'        # 'event', 'time', or 'dependency'
    trigger_collector: Optional[str] = None
    kg_node_keys: List[str] = []

    # --- Lazy-init singletons ---
    _connection_manager = None
    _kg_manager = None

    def _get_connection(self):
        """Lazy import of get_connection (follows KGManager pattern)."""
        from src.services.database.db_config import get_connection
        return get_connection()

    def _get_kg_manager(self):
        """Lazy singleton for KGManager."""
        if self._kg_manager is None:
            from src.knowledge_graph.kg_manager import KGManager
            self._kg_manager = KGManager()
        return self._kg_manager

    # ------------------------------------------------------------------
    # Abstract methods (Phase 2 templates implement these)
    # ------------------------------------------------------------------

    @abstractmethod
    def check_data_ready(self) -> bool:
        """
        Check if all required data is fresh enough to run this analysis.

        Returns True if all required_collectors have delivered data within
        their expected freshness window.
        """
        ...

    @abstractmethod
    def gather_data(self) -> Dict:
        """
        Query the database for all data needed by this analysis.

        Returns a dict of named data sets (DataFrames, dicts, or lists)
        that compute_analysis() will consume.
        """
        ...

    @abstractmethod
    def compute_analysis(self, data: Dict) -> Dict:
        """
        Compute derived analysis from the gathered data.

        Performs calculations like YoY changes, stocks-to-use ratios,
        and other analytics. Returns a dict ready for prompt rendering.
        """
        ...

    # ------------------------------------------------------------------
    # Default implementations
    # ------------------------------------------------------------------

    def get_kg_context(self) -> Dict:
        """
        Fetch enriched KG context for all kg_node_keys.

        Returns a dict of {node_key: enriched_context} for all nodes
        that exist in the knowledge graph.
        """
        kg = self._get_kg_manager()
        contexts = {}
        for key in self.kg_node_keys:
            ctx = kg.get_enriched_context(key)
            if ctx:
                contexts[key] = ctx
            else:
                logger.debug("KG node '%s' not found, skipping", key)
        return contexts

    def build_prompt_context(self) -> Dict:
        """
        Main entry point: gather data, compute analysis, add KG context.

        Returns a combined dict suitable for passing to a prompt template's
        render() method.

        Raises:
            RuntimeError: If check_data_ready() returns False
        """
        if not self.check_data_ready():
            raise RuntimeError(
                f"Data not ready for template '{self.template_id}'. "
                f"Required collectors: {self.required_collectors}"
            )

        data = self.gather_data()
        analysis = self.compute_analysis(data)
        kg_context = self.get_kg_context()

        return {
            'template_id': self.template_id,
            'report_type': self.report_type,
            'data': data,
            'analysis': analysis,
            'kg_context': kg_context,
            'kg_node_keys': self.kg_node_keys,
        }
