"""
Prompt Registry

Central registry of all prompt templates. Phase 1 supports manual
registration only. Auto-discovery from a templates directory is Phase 2+.

Usage:
    from src.prompts.registry import PromptRegistry
    from my_templates import WASDEAnalysis

    registry = PromptRegistry()
    registry.register(WASDEAnalysis())

    rendered = registry.render('wasde_analysis', {'commodity': 'corn', 'data': '...'})
"""

import logging
from typing import Dict, List, Optional

from src.prompts.base_template import BasePromptTemplate, RenderedPrompt

logger = logging.getLogger(__name__)


class PromptRegistry:
    """
    Registry of prompt templates, keyed by TEMPLATE_ID.

    Thread-safe for reads (dict lookup). Write collisions are acceptable
    because registration happens at startup, not at runtime.
    """

    def __init__(self):
        self._templates: Dict[str, BasePromptTemplate] = {}

    def register(self, template: BasePromptTemplate) -> None:
        """
        Register a template instance. Overwrites if same ID already exists.

        Args:
            template: An instance of a BasePromptTemplate subclass
        """
        tid = template.TEMPLATE_ID
        if not tid:
            raise ValueError("Template must have a non-empty TEMPLATE_ID")
        if tid in self._templates:
            logger.info("Overwriting template '%s' (v%s -> v%s)",
                        tid, self._templates[tid].TEMPLATE_VERSION,
                        template.TEMPLATE_VERSION)
        self._templates[tid] = template
        logger.debug("Registered template '%s' v%s", tid, template.TEMPLATE_VERSION)

    def get(self, template_id: str) -> BasePromptTemplate:
        """
        Retrieve a template by ID.

        Raises:
            KeyError: If template_id is not registered
        """
        if template_id not in self._templates:
            raise KeyError(
                f"Template '{template_id}' not registered. "
                f"Available: {list(self._templates.keys())}"
            )
        return self._templates[template_id]

    def render(
        self,
        template_id: str,
        variables: Dict,
        context_keys: Optional[List[str]] = None,
    ) -> RenderedPrompt:
        """Shortcut: get template and render it in one call."""
        template = self.get(template_id)
        return template.render(variables, context_keys=context_keys)

    def list_templates(self) -> List[Dict]:
        """Return summary info for all registered templates."""
        return [
            {
                'template_id': t.TEMPLATE_ID,
                'version': t.TEMPLATE_VERSION,
                'task_type': t.TASK_TYPE,
                'sensitivity': t.DEFAULT_SENSITIVITY,
                'required_variables': t.REQUIRED_VARIABLES,
            }
            for t in self._templates.values()
        ]
