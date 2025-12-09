"""
South America Trade Data Database Module

Trade Flow Tables:
- TradeFlowRecord: Raw trade flow data
- TradeFlowHarmonized: Harmonized trade data at HS6 level
- TradeBalanceMatrix: Reporter-partner balance matrix

Port Lineup Tables:
- PortLineupRecord: Port lineup/vessel data
- LineupAggregate: Aggregated lineup totals
- LineupSourceLog: Lineup ingestion logs

Reference Tables:
- HSCodeReference: HS code hierarchy and descriptions
- CountryReference: Country codes and mappings
"""

from .models import (
    Base,
    TradeFlowRecord,
    TradeFlowHarmonized,
    TradeBalanceMatrix,
    HSCodeReference,
    CountryReference,
    DataSourceLog,
    QualityAlert,
    MonthlyAggregate,
    PortLineupRecord,
    LineupAggregate,
    LineupSourceLog,
    init_database,
    create_all_tables,
    get_session_factory,
)

__all__ = [
    # Base
    'Base',

    # Trade flow tables
    'TradeFlowRecord',
    'TradeFlowHarmonized',
    'TradeBalanceMatrix',
    'MonthlyAggregate',

    # Lineup tables
    'PortLineupRecord',
    'LineupAggregate',
    'LineupSourceLog',

    # Reference tables
    'HSCodeReference',
    'CountryReference',

    # Logging
    'DataSourceLog',
    'QualityAlert',

    # Utilities
    'init_database',
    'create_all_tables',
    'get_session_factory',
]
