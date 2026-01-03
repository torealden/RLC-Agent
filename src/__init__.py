"""
RLC-Agent Source Package

A comprehensive agricultural commodity market analysis system.
Designed for integration with desktop LLM applications and Windows wrappers.

Components:
- agents/: Specialized agents for data collection, analysis, and reporting
- orchestrators/: Workflow coordination
- schedulers/: Task scheduling
- services/: API clients, database, and document services
- tools/: LLM-callable tools
- utils/: Shared utilities
"""

__version__ = "2.0.0"
__author__ = "Round Lakes Commodities"

from pathlib import Path

# Package root directory
PACKAGE_ROOT = Path(__file__).parent
PROJECT_ROOT = PACKAGE_ROOT.parent

# Standard paths
DATA_DIR = PROJECT_ROOT / "data"
CONFIG_DIR = PROJECT_ROOT / "config"
MODELS_DIR = PROJECT_ROOT / "Models"
EXPORTS_DIR = PROJECT_ROOT / "exports"
DOCS_DIR = PROJECT_ROOT / "docs"

__all__ = [
    "__version__",
    "PACKAGE_ROOT",
    "PROJECT_ROOT",
    "DATA_DIR",
    "CONFIG_DIR",
    "MODELS_DIR",
    "EXPORTS_DIR",
    "DOCS_DIR",
]
