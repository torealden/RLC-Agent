"""
Model Router

Selects the right LLM model for a given task based on sensitivity level,
task type, and complexity. Enforces the sensitivity -> cloud/local gate
as a hard safety constraint.

Usage:
    from src.services.llm.model_router import ModelRouter

    router = ModelRouter()
    model = router.route('analysis', sensitivity=0)
    print(model.model_id)  # 'claude-sonnet-4-20250514'

    model = router.route('analysis', sensitivity=2)
    print(model.model_id)  # 'llama3.1:32b'
"""

import logging
import os
from dataclasses import dataclass
from typing import Dict, Optional

logger = logging.getLogger(__name__)


@dataclass
class ModelConfig:
    """Configuration for a single LLM model selection."""
    model_id: str           # e.g. 'claude-sonnet-4-20250514' or 'llama3.1:70b'
    provider: str           # 'anthropic' or 'ollama'
    tier: str               # 'cloud' or 'local'
    cost_per_1k_in: float
    cost_per_1k_out: float
    max_tokens: int = 4096
    temperature: float = 0.3


class ModelRouter:
    """
    Routes task_type + sensitivity -> ModelConfig.

    Routing logic:
      - sensitivity 0-1: cloud models allowed (pick from cloud column)
      - sensitivity 2+: local only (pick from local column)
      - complexity 'high' upgrades one model tier within the column
      - complexity 'low' downgrades one model tier within the column
    """

    # task_type -> {cloud: model_id, local: model_id}
    ROUTING_MATRIX = {
        'narrative':    {'cloud': 'claude-sonnet-4-20250514',  'local': 'llama3.1:70b'},
        'analysis':     {'cloud': 'claude-sonnet-4-20250514',  'local': 'llama3.1:32b'},
        'summary':      {'cloud': 'claude-haiku-4-5-20251001', 'local': 'llama3.1:8b'},
        'chart_config': {'cloud': 'claude-haiku-4-5-20251001', 'local': 'llama3.1:8b'},
        'synthesis':    {'cloud': 'claude-opus-4-20250514',    'local': 'llama3.1:70b'},
    }

    # Ordered tiers for upgrade/downgrade within a column
    _CLOUD_TIER = ['claude-haiku-4-5-20251001', 'claude-sonnet-4-20250514', 'claude-opus-4-20250514']
    _LOCAL_TIER = ['llama3.1:8b', 'llama3.1:32b', 'llama3.1:70b']

    # Fallback chain: each model -> next fallback
    _FALLBACK_CHAIN = {
        'claude-opus-4-20250514':    'claude-sonnet-4-20250514',
        'claude-sonnet-4-20250514':  'claude-haiku-4-5-20251001',
        'claude-haiku-4-5-20251001': 'llama3.1:70b',
        'llama3.1:70b':              'llama3.1:32b',
        'llama3.1:32b':              'llama3.1:8b',
        'llama3.1:8b':               None,
    }

    def __init__(self, sensitivity_config=None):
        if sensitivity_config is None:
            from src.config.sensitivity import SensitivityConfig
            sensitivity_config = SensitivityConfig()
        self._config = sensitivity_config

    def route(
        self,
        task_type: str,
        sensitivity: int = 0,
        complexity: str = 'medium',
    ) -> ModelConfig:
        """
        Select the best model for a task.

        Args:
            task_type: One of 'narrative', 'analysis', 'summary', 'chart_config', 'synthesis'
            sensitivity: 0-4 (0=public, 4=restricted)
            complexity: 'low', 'medium', or 'high'

        Returns:
            ModelConfig with the selected model

        Raises:
            ValueError: If task_type is unknown
        """
        if task_type not in self.ROUTING_MATRIX:
            raise ValueError(
                f"Unknown task_type '{task_type}'. "
                f"Valid: {list(self.ROUTING_MATRIX.keys())}"
            )

        cloud_allowed = self._config.is_cloud_allowed(sensitivity)
        column = 'cloud' if cloud_allowed else 'local'
        model_id = self.ROUTING_MATRIX[task_type][column]

        # Apply complexity adjustment
        model_id = self._adjust_for_complexity(model_id, column, complexity)

        # Pre-flight safety check: block cloud for high sensitivity
        model_id = self._preflight_check(model_id, sensitivity)

        return self._build_config(model_id)

    def health_check(self) -> Dict[str, bool]:
        """Check availability of Anthropic API and Ollama server."""
        return {
            'anthropic': self._check_anthropic(),
            'ollama': self._check_ollama(),
        }

    def get_fallback(self, model_id: str) -> Optional[ModelConfig]:
        """Return the next fallback model, or None if no fallback exists."""
        next_id = self._FALLBACK_CHAIN.get(model_id)
        if next_id is None:
            return None
        return self._build_config(next_id)

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _adjust_for_complexity(self, model_id: str, column: str, complexity: str) -> str:
        """Shift model up/down within its tier based on complexity."""
        if complexity == 'medium':
            return model_id

        tier = self._CLOUD_TIER if column == 'cloud' else self._LOCAL_TIER
        if model_id not in tier:
            return model_id

        idx = tier.index(model_id)
        if complexity == 'high' and idx < len(tier) - 1:
            return tier[idx + 1]
        elif complexity == 'low' and idx > 0:
            return tier[idx - 1]
        return model_id

    def _preflight_check(self, model_id: str, sensitivity: int) -> str:
        """Hard gate: if sensitivity >= 2, force local model."""
        if sensitivity >= 2 and self._config.get_model_tier(model_id) == 'cloud':
            logger.warning(
                "Preflight blocked cloud model %s for sensitivity=%d, falling back to local",
                model_id, sensitivity,
            )
            # Walk fallback chain until we find a local model
            current = model_id
            while current and self._config.get_model_tier(current) == 'cloud':
                current = self._FALLBACK_CHAIN.get(current)
            if current is None:
                raise RuntimeError("No local model available in fallback chain")
            return current
        return model_id

    def _build_config(self, model_id: str) -> ModelConfig:
        """Build a ModelConfig from the sensitivity config's model metadata."""
        costs = self._config.get_model_cost(model_id)
        tier = self._config.get_model_tier(model_id)
        provider = 'anthropic' if tier == 'cloud' else 'ollama'
        model_meta = self._config.models.get(model_id, {})

        return ModelConfig(
            model_id=model_id,
            provider=provider,
            tier=tier,
            cost_per_1k_in=costs['cost_per_1k_in'],
            cost_per_1k_out=costs['cost_per_1k_out'],
            max_tokens=model_meta.get('max_tokens', 4096),
        )

    @staticmethod
    def _check_anthropic() -> bool:
        """Check if Anthropic API key is set and SDK is importable."""
        if not os.environ.get("ANTHROPIC_API_KEY"):
            return False
        try:
            import anthropic  # noqa: F401
            return True
        except ImportError:
            return False

    @staticmethod
    def _check_ollama() -> bool:
        """Check if Ollama server responds."""
        try:
            import requests
            response = requests.get(
                f"{os.environ.get('OLLAMA_URL', 'http://localhost:11434')}/api/tags",
                timeout=2,
            )
            return response.status_code == 200
        except Exception:
            return False
