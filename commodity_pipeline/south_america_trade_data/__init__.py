"""
South America Trade Data Pipeline

A comprehensive data pipeline for collecting, harmonizing, and analyzing
trade flow data from South American countries:

Monthly Trade Flow Data:
- Argentina (INDEC)
- Brazil (Comex Stat / MDIC/SECEX)
- Colombia (DANE)
- Uruguay (DNA)
- Paraguay (DNA / WITS fallback)

Weekly Port Lineup Data:
- Brazil (ANEC - Brazilian Grain Exporters Association)
- Argentina (NABSA - planned)

Features:
- Automated data collection from official government sources
- HS code harmonization to 6-digit level for cross-country comparison
- Quantity/value normalization (metric tons, USD)
- Reporter-partner balance matrix for trade reconciliation
- Quality validation and outlier detection
- Scheduling based on official release calendars
- Port lineup data for near-term export visibility

Usage:
    # Trade flow data
    from south_america_trade_data import TradeDataOrchestrator
    orchestrator = TradeDataOrchestrator()
    result = orchestrator.run_monthly_pipeline(2024, 8)

    # Port lineup data
    from south_america_trade_data import LineupDataOrchestrator
    lineup_orchestrator = LineupDataOrchestrator()
    result = lineup_orchestrator.run_weekly_pipeline()
"""

__version__ = "1.0.0"
__author__ = "Round Lakes Commodities"

from .config.settings import (
    SouthAmericaTradeConfig,
    BrazilLineupConfig,
    ArgentinaLineupConfig,
    default_config,
)

from .services.orchestrator import TradeDataOrchestrator, LineupDataOrchestrator
from .services.scheduler import TradeDataScheduler, LineupScheduler

from .agents.argentina_agent import ArgentinaINDECAgent
from .agents.brazil_agent import BrazilComexStatAgent
from .agents.colombia_agent import ColombiaDANEAgent
from .agents.uruguay_agent import UruguayDNAAgent
from .agents.paraguay_agent import ParaguayAgent
from .agents.brazil_lineup_agent import BrazilANECLineupAgent

from .utils.harmonization import TradeDataHarmonizer, BalanceMatrixBuilder
from .utils.quality import QualityValidator, OutlierDetector

__all__ = [
    # Configuration
    'SouthAmericaTradeConfig',
    'BrazilLineupConfig',
    'ArgentinaLineupConfig',
    'default_config',

    # Services
    'TradeDataOrchestrator',
    'TradeDataScheduler',
    'LineupDataOrchestrator',
    'LineupScheduler',

    # Trade Agents
    'ArgentinaINDECAgent',
    'BrazilComexStatAgent',
    'ColombiaDANEAgent',
    'UruguayDNAAgent',
    'ParaguayAgent',

    # Lineup Agents
    'BrazilANECLineupAgent',

    # Utilities
    'TradeDataHarmonizer',
    'BalanceMatrixBuilder',
    'QualityValidator',
    'OutlierDetector',
]
