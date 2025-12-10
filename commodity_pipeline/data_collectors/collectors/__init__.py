"""
Data Collectors Package

Contains all data source collectors organized by region/type.

NORTH AMERICA:
- CFTC COT: Commitments of Traders positioning data
- USDA FAS: Export sales and PSD data
- EIA Ethanol: Ethanol production and stocks
- Drought Monitor: US drought conditions

SOUTH AMERICA:
- (Planned) CONAB: Brazil crop estimates
- (Planned) ABIOVE: Brazil soy crush
- (Planned) BCBA: Argentina grain exchange

ASIA PACIFIC:
- MPOB: Malaysian palm oil data

EUROPE:
- (Planned) Eurostat: EU production/trade data
"""

from .base_collector import BaseCollector, CollectorResult, CollectorConfig

# North America collectors
from .cftc_cot_collector import CFTCCOTCollector, CFTCCOTConfig
from .usda_fas_collector import USDATFASCollector, USDATFASConfig
from .eia_ethanol_collector import EIAEthanolCollector, EIAEthanolConfig
from .drought_collector import DroughtCollector, DroughtConfig

# Asia Pacific collectors
from .mpob_collector import MPOBCollector, MPOBConfig

__all__ = [
    # Base classes
    'BaseCollector',
    'CollectorResult',
    'CollectorConfig',

    # North America
    'CFTCCOTCollector',
    'CFTCCOTConfig',
    'USDATFASCollector',
    'USDATFASConfig',
    'EIAEthanolCollector',
    'EIAEthanolConfig',
    'DroughtCollector',
    'DroughtConfig',

    # Asia Pacific
    'MPOBCollector',
    'MPOBConfig',
]


# Registry of all available collectors by region
COLLECTOR_REGISTRY = {
    'north_america': {
        'cftc_cot': {
            'class': CFTCCOTCollector,
            'config_class': CFTCCOTConfig,
            'description': 'CFTC Commitments of Traders',
            'auth_required': False,
            'status': 'implemented',
        },
        'usda_fas': {
            'class': USDATFASCollector,
            'config_class': USDATFASConfig,
            'description': 'USDA FAS Export Sales & PSD',
            'auth_required': False,
            'status': 'implemented',
        },
        'eia_ethanol': {
            'class': EIAEthanolCollector,
            'config_class': EIAEthanolConfig,
            'description': 'EIA Ethanol Production & Stocks',
            'auth_required': True,
            'env_var': 'EIA_API_KEY',
            'status': 'implemented',
        },
        'drought': {
            'class': DroughtCollector,
            'config_class': DroughtConfig,
            'description': 'US Drought Monitor',
            'auth_required': False,
            'status': 'implemented',
        },
        'usda_nass': {
            'class': None,
            'description': 'USDA NASS Crop Progress',
            'auth_required': True,
            'env_var': 'NASS_API_KEY',
            'status': 'planned',
        },
    },
    'south_america': {
        'conab': {
            'class': None,
            'description': 'CONAB Brazil Crop Estimates',
            'auth_required': False,
            'status': 'planned',
        },
        'abiove': {
            'class': None,
            'description': 'ABIOVE Brazil Soy Crush',
            'auth_required': False,
            'status': 'planned',
        },
        'bcba': {
            'class': None,
            'description': 'Buenos Aires Grain Exchange',
            'auth_required': False,
            'status': 'planned',
        },
        'rosario': {
            'class': None,
            'description': 'Rosario Board of Trade',
            'auth_required': False,
            'status': 'planned',
        },
    },
    'asia_pacific': {
        'mpob': {
            'class': MPOBCollector,
            'config_class': MPOBConfig,
            'description': 'Malaysian Palm Oil Board',
            'auth_required': False,
            'status': 'implemented',
        },
        'dce': {
            'class': None,
            'description': 'Dalian Commodity Exchange',
            'auth_required': False,
            'status': 'planned',
        },
        'bursa': {
            'class': None,
            'description': 'Bursa Malaysia Derivatives',
            'auth_required': False,
            'status': 'planned',
        },
    },
    'europe': {
        'eurostat': {
            'class': None,
            'description': 'Eurostat Agriculture Data',
            'auth_required': True,
            'env_var': 'EUROSTAT_API_KEY',
            'status': 'planned',
        },
        'ec_agri': {
            'class': None,
            'description': 'European Commission AGRI',
            'auth_required': True,
            'status': 'planned',
        },
    },
    'global': {
        'fao_amis': {
            'class': None,
            'description': 'FAO AMIS Global Data',
            'auth_required': False,
            'status': 'planned',
        },
        'igc': {
            'class': None,
            'description': 'International Grains Council',
            'auth_required': True,
            'status': 'planned',
        },
    },
}


def get_available_collectors() -> dict:
    """Get all implemented collectors"""
    available = {}
    for region, collectors in COLLECTOR_REGISTRY.items():
        for name, info in collectors.items():
            if info.get('status') == 'implemented' and info.get('class'):
                available[name] = {
                    'region': region,
                    'class': info['class'],
                    'description': info['description'],
                    'auth_required': info.get('auth_required', False),
                }
    return available


def get_collector(name: str):
    """Get a collector class by name"""
    for region, collectors in COLLECTOR_REGISTRY.items():
        if name in collectors:
            info = collectors[name]
            if info.get('class'):
                return info['class']
    return None


def list_collectors_by_region(region: str = None) -> dict:
    """List collectors, optionally filtered by region"""
    if region:
        return COLLECTOR_REGISTRY.get(region, {})
    return COLLECTOR_REGISTRY
