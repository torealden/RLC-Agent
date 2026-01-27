"""
South America Data Collectors

Collectors for agricultural data from South American sources:
- Brazil: CONAB, Comex Stat, IBGE, ABIOVE, IMEA
- Argentina: MAGYP, Buenos Aires Grain Exchange
- Paraguay, Uruguay, Colombia: Trade data
"""

from .brazil_agent import BrazilComexStatAgent
from .conab_collector import CONABCollector, CONABConfig
from .conab_soybean_agent import (
    CONABSoybeanAgent,
    CONABSoybeanConfig,
    CollectionResult
)

__all__ = [
    # Brazil
    'BrazilComexStatAgent',
    'CONABCollector',
    'CONABConfig',
    'CONABSoybeanAgent',
    'CONABSoybeanConfig',
    'CollectionResult',
]
