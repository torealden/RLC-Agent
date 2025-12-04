"""
HB Weekly Report Writer Agent

Autonomous agent for generating the HigbyBarrett (HB) agricultural market report
every Tuesday. Coordinates with data pipelines and services to compile a comprehensive
market analysis document.

Key Features:
- Automated commodity market research (corn, wheat, soybeans, soybean meal, soybean oil)
- Integration with internal spreadsheet data (Dropbox) and future database support
- External market price fetching via API Manager (USDA AMS)
- Analysis of bullish/bearish factors and swing catalysts
- Word document generation with tables and charts
- Weekly scheduling with Tuesday execution
- Error handling with escalation to shared inbox

Author: RLC Master Agent System
Version: 1.0.0
"""

from .services.orchestrator import HBReportOrchestrator
from .config.settings import HBWeeklyReportConfig

__version__ = "1.0.0"
__all__ = [
    "HBReportOrchestrator",
    "HBWeeklyReportConfig",
]
