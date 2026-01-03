"""
RLC-Agent Agents Package

Specialized agents for data collection, analysis, and reporting.

Subpackages:
- base/: Base classes for agents and collectors
- collectors/: Data collection agents organized by region
    - us/: USDA, CFTC, EIA, Census, CME
    - canada/: CGC, StatsCan
    - south_america/: Brazil, Argentina, etc.
    - asia/: MPOB, etc.
    - europe/: Eurostat
    - global/: FAO
    - market/: CME, IBKR, TradeStation
- analysis/: Price forecasting, fundamental analysis
- reporting/: Report generation agents
- integration/: Email, Calendar, Notion integration
- core/: Master agent, memory, approvals
"""

__all__ = [
    "base",
    "collectors",
    "analysis",
    "reporting",
    "integration",
    "core",
]
