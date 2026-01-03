"""
RLC Orchestrator - Built-in Agents
===================================
These agents come pre-installed with the orchestrator.

Available agents:
- health_check: System health monitoring
"""

from . import health_check

__all__ = ["health_check"]
