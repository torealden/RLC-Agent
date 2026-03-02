"""
Sensitivity Configuration Loader

Reads config/sensitivity.yaml and provides methods for sensitivity-based
routing decisions: cloud vs local model selection, cost lookups, and
per-data-source sensitivity levels.

Usage:
    from src.config.sensitivity import SensitivityConfig

    config = SensitivityConfig()
    print(config.is_cloud_allowed(0))        # True
    print(config.is_cloud_allowed(2))        # False
    print(config.get_data_source_sensitivity('cftc_cot'))  # 0
"""

import logging
from pathlib import Path
from typing import Dict, Optional

logger = logging.getLogger(__name__)

# Project root -> config/ directory
CONFIG_DIR = Path(__file__).parent.parent.parent / "config"


class SensitivityConfig:
    """
    Loads sensitivity.yaml and exposes lookup methods for model routing.

    Attributes:
        levels: Dict mapping sensitivity int -> level metadata
        data_source_defaults: Dict mapping collector_key -> sensitivity int
        models: Dict mapping model_id -> model metadata (provider, tier, cost)
    """

    def __init__(self, config_path: Optional[Path] = None):
        import yaml

        path = config_path or (CONFIG_DIR / "sensitivity.yaml")
        if not path.exists():
            raise FileNotFoundError(f"Sensitivity config not found: {path}")

        with open(path, 'r') as f:
            raw = yaml.safe_load(f)

        self.levels: Dict[int, dict] = {
            int(k): v for k, v in raw.get('sensitivity_levels', {}).items()
        }
        self.data_source_defaults: Dict[str, int] = raw.get('data_source_defaults', {})
        self.models: Dict[str, dict] = raw.get('models', {})

        logger.debug(
            "SensitivityConfig loaded: %d levels, %d sources, %d models",
            len(self.levels), len(self.data_source_defaults), len(self.models),
        )

    def get_data_source_sensitivity(self, collector_key: str) -> int:
        """Return the default sensitivity level for a data source (0 if unknown)."""
        return self.data_source_defaults.get(collector_key, 0)

    def is_cloud_allowed(self, sensitivity: int) -> bool:
        """Return True if cloud models are permitted at this sensitivity level."""
        level = self.levels.get(sensitivity)
        if level is None:
            # Unknown level -> deny cloud access as safety default
            return False
        return level.get('cloud_allowed', False)

    def get_model_cost(self, model_id: str) -> Dict[str, float]:
        """Return {cost_per_1k_in, cost_per_1k_out} for a model."""
        model = self.models.get(model_id, {})
        return {
            'cost_per_1k_in': model.get('cost_per_1k_in', 0.0),
            'cost_per_1k_out': model.get('cost_per_1k_out', 0.0),
        }

    def get_model_tier(self, model_id: str) -> str:
        """Return 'cloud' or 'local' for a model. Defaults to 'local'."""
        model = self.models.get(model_id, {})
        return model.get('tier', 'local')
