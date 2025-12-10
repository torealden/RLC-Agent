"""
Data Collectors Package

Contains all data source collectors organized by region/type.

NORTH AMERICA:
- CFTC COT: Commitments of Traders positioning data
- USDA FAS: Export sales and PSD data
- USDA ERS: Feed grains, oilseeds, wheat databases
- USDA NASS: Crop progress, production, stocks
- USDA AMS: Market news - tallow, DDGS, feedstocks
- EIA Ethanol: Ethanol production and stocks
- EIA Petroleum: Crude, products, natural gas
- EPA RFS: RIN generation data
- Drought Monitor: US drought conditions
- Census Trade: US import/export trade data
- Canada CGC: Canadian Grain Commission
- Canada StatsCan: Statistics Canada agricultural data
- CME Settlements: Futures settlement prices

ASIA PACIFIC:
- MPOB: Malaysian palm oil data

SOUTH AMERICA:
- (Planned) CONAB: Brazil crop estimates
- (Planned) ABIOVE: Brazil soy crush
- (Planned) BCBA: Argentina grain exchange

EUROPE:
- (Planned) Eurostat: EU production/trade data
"""

from .base_collector import BaseCollector, CollectorResult, CollectorConfig, DataFrequency, AuthType

# North America collectors
from .cftc_cot_collector import CFTCCOTCollector, CFTCCOTConfig
from .usda_fas_collector import USDATFASCollector, USDATFASConfig
from .eia_ethanol_collector import EIAEthanolCollector, EIAEthanolConfig
from .drought_collector import DroughtCollector, DroughtConfig

# USDA ERS collectors
from .usda_ers_collector import (
    FeedGrainsCollector,
    FeedGrainsConfig,
    OilCropsCollector,
    OilCropsConfig,
    WheatDataCollector,
    WheatDataConfig,
)

# USDA NASS collector
from .usda_nass_collector import NASSCollector, NASSConfig

# EIA Petroleum collector
from .eia_petroleum_collector import EIAPetroleumCollector, EIAPetroleumConfig

# EPA RFS collector
from .epa_rfs_collector import EPARFSCollector, EPARFSConfig

# USDA AMS collectors
from .usda_ams_collector import (
    TallowProteinCollector,
    TallowProteinConfig,
    GrainCoProductsCollector,
    GrainCoProductsConfig,
)

# Census Trade collector
from .census_trade_collector import CensusTradeCollector, CensusTradeConfig

# Canadian collectors
from .canada_cgc_collector import CGCCollector, CGCConfig
from .canada_statscan_collector import (
    StatsCanCollector,
    StatsCanConfig,
    CanolaCouncilCollector,
    CanolaConcilConfig,
)

# CME settlements collector
from .cme_settlements_collector import CMESettlementsCollector, CMESettlementsConfig

# Asia Pacific collectors
from .mpob_collector import MPOBCollector, MPOBConfig

__all__ = [
    # Base classes
    'BaseCollector',
    'CollectorResult',
    'CollectorConfig',
    'DataFrequency',
    'AuthType',

    # North America - Core
    'CFTCCOTCollector',
    'CFTCCOTConfig',
    'USDATFASCollector',
    'USDATFASConfig',
    'EIAEthanolCollector',
    'EIAEthanolConfig',
    'DroughtCollector',
    'DroughtConfig',

    # North America - USDA ERS
    'FeedGrainsCollector',
    'FeedGrainsConfig',
    'OilCropsCollector',
    'OilCropsConfig',
    'WheatDataCollector',
    'WheatDataConfig',

    # North America - USDA NASS
    'NASSCollector',
    'NASSConfig',

    # North America - EIA Petroleum
    'EIAPetroleumCollector',
    'EIAPetroleumConfig',

    # North America - EPA RFS
    'EPARFSCollector',
    'EPARFSConfig',

    # North America - USDA AMS
    'TallowProteinCollector',
    'TallowProteinConfig',
    'GrainCoProductsCollector',
    'GrainCoProductsConfig',

    # North America - Census Trade
    'CensusTradeCollector',
    'CensusTradeConfig',

    # North America - Canada
    'CGCCollector',
    'CGCConfig',
    'StatsCanCollector',
    'StatsCanConfig',
    'CanolaCouncilCollector',
    'CanolaConcilConfig',

    # North America - CME
    'CMESettlementsCollector',
    'CMESettlementsConfig',

    # Asia Pacific
    'MPOBCollector',
    'MPOBConfig',
]


# Registry of all available collectors by region
COLLECTOR_REGISTRY = {
    'north_america': {
        # Core collectors
        'cftc_cot': {
            'class': CFTCCOTCollector,
            'config_class': CFTCCOTConfig,
            'description': 'CFTC Commitments of Traders',
            'auth_required': False,
            'status': 'implemented',
            'release_schedule': 'Friday 3:30 PM ET',
        },
        'usda_fas': {
            'class': USDATFASCollector,
            'config_class': USDATFASConfig,
            'description': 'USDA FAS Export Sales & PSD',
            'auth_required': False,
            'status': 'implemented',
            'release_schedule': 'Thursday 8:30 AM ET (export sales)',
        },
        'eia_ethanol': {
            'class': EIAEthanolCollector,
            'config_class': EIAEthanolConfig,
            'description': 'EIA Ethanol Production & Stocks',
            'auth_required': True,
            'env_var': 'EIA_API_KEY',
            'status': 'implemented',
            'release_schedule': 'Wednesday 10:30 AM ET',
        },
        'drought': {
            'class': DroughtCollector,
            'config_class': DroughtConfig,
            'description': 'US Drought Monitor',
            'auth_required': False,
            'status': 'implemented',
            'release_schedule': 'Thursday 8:30 AM ET',
        },
        # USDA ERS collectors
        'usda_ers_feed_grains': {
            'class': FeedGrainsCollector,
            'config_class': FeedGrainsConfig,
            'description': 'USDA ERS Feed Grains Database',
            'auth_required': False,
            'status': 'implemented',
            'commodities': ['corn', 'sorghum', 'barley', 'oats'],
            'release_schedule': 'Updated periodically',
        },
        'usda_ers_oil_crops': {
            'class': OilCropsCollector,
            'config_class': OilCropsConfig,
            'description': 'USDA ERS Oil Crops Yearbook',
            'auth_required': False,
            'status': 'implemented',
            'commodities': ['soybeans', 'soybean_oil', 'soybean_meal', 'canola', 'sunflower'],
            'release_schedule': 'Updated periodically',
        },
        'usda_ers_wheat': {
            'class': WheatDataCollector,
            'config_class': WheatDataConfig,
            'description': 'USDA ERS Wheat Data',
            'auth_required': False,
            'status': 'implemented',
            'commodities': ['wheat_hrw', 'wheat_hrs', 'wheat_srw', 'wheat_durum', 'wheat_white'],
            'release_schedule': 'Updated periodically',
        },
        # USDA NASS
        'usda_nass': {
            'class': NASSCollector,
            'config_class': NASSConfig,
            'description': 'USDA NASS Quick Stats',
            'auth_required': True,
            'env_var': 'NASS_API_KEY',
            'status': 'implemented',
            'release_schedule': 'Monday 4:00 PM ET (crop progress)',
        },
        # EIA Petroleum
        'eia_petroleum': {
            'class': EIAPetroleumCollector,
            'config_class': EIAPetroleumConfig,
            'description': 'EIA Petroleum & Energy Data',
            'auth_required': True,
            'env_var': 'EIA_API_KEY',
            'status': 'implemented',
            'commodities': ['crude_oil', 'gasoline', 'diesel', 'jet_fuel', 'natural_gas'],
            'release_schedule': 'Wednesday 10:30 AM ET',
        },
        # EPA RFS
        'epa_rfs': {
            'class': EPARFSCollector,
            'config_class': EPARFSConfig,
            'description': 'EPA RFS RIN Data',
            'auth_required': False,
            'status': 'implemented',
            'release_schedule': 'Monthly',
        },
        # USDA AMS
        'usda_ams_tallow': {
            'class': TallowProteinCollector,
            'config_class': TallowProteinConfig,
            'description': 'USDA AMS Tallow & Grease Prices',
            'auth_required': False,
            'status': 'implemented',
            'commodities': ['yellow_grease', 'cwg', 'tallow', 'lard', 'poultry_fat'],
            'release_schedule': 'Weekly',
        },
        'usda_ams_ddgs': {
            'class': GrainCoProductsCollector,
            'config_class': GrainCoProductsConfig,
            'description': 'USDA AMS Grain Co-Products',
            'auth_required': False,
            'status': 'implemented',
            'commodities': ['ddgs', 'wdg', 'dco', 'corn_gluten'],
            'release_schedule': 'Weekly',
        },
        # Census Trade
        'census_trade': {
            'class': CensusTradeCollector,
            'config_class': CensusTradeConfig,
            'description': 'US Census Trade Data',
            'auth_required': False,
            'env_var': 'CENSUS_API_KEY',
            'status': 'implemented',
            'release_schedule': 'Monthly (~6 week lag)',
        },
        # Canada
        'canada_cgc': {
            'class': CGCCollector,
            'config_class': CGCConfig,
            'description': 'Canadian Grain Commission',
            'auth_required': False,
            'status': 'implemented',
            'commodities': ['wheat', 'canola', 'barley', 'oats'],
            'release_schedule': 'Thursday',
        },
        'canada_statscan': {
            'class': StatsCanCollector,
            'config_class': StatsCanConfig,
            'description': 'Statistics Canada Agriculture',
            'auth_required': False,
            'status': 'implemented',
            'commodities': ['wheat', 'canola', 'barley', 'oats', 'corn', 'soybeans'],
            'release_schedule': 'Varies by report',
        },
        'canola_council': {
            'class': CanolaCouncilCollector,
            'config_class': CanolaConcilConfig,
            'description': 'Canola Council of Canada',
            'auth_required': False,
            'status': 'implemented',
            'commodities': ['canola'],
            'release_schedule': 'Varies',
        },
        # CME
        'cme_settlements': {
            'class': CMESettlementsCollector,
            'config_class': CMESettlementsConfig,
            'description': 'CME Futures Settlements',
            'auth_required': False,
            'status': 'implemented',
            'commodities': ['corn', 'wheat', 'soybeans', 'soybean_oil', 'soybean_meal',
                           'crude_oil', 'gasoline', 'diesel', 'natural_gas', 'ethanol'],
            'release_schedule': 'Daily after close',
        },
        # Planned
        'usda_wasde': {
            'class': None,
            'description': 'USDA WASDE Reports',
            'auth_required': False,
            'status': 'planned',
            'release_schedule': '~12th of month, 12:00 PM ET',
        },
        'nopa_crush': {
            'class': None,
            'description': 'NOPA Monthly Crush',
            'auth_required': False,
            'status': 'planned',
            'release_schedule': '~15th of month',
        },
    },
    'south_america': {
        'conab': {
            'class': None,
            'description': 'CONAB Brazil Crop Estimates',
            'auth_required': False,
            'status': 'planned',
            'release_schedule': 'Monthly (8th-12th)',
        },
        'abiove': {
            'class': None,
            'description': 'ABIOVE Brazil Soy Crush',
            'auth_required': False,
            'status': 'planned',
            'release_schedule': 'Monthly',
        },
        'bcba': {
            'class': None,
            'description': 'Buenos Aires Grain Exchange',
            'auth_required': False,
            'status': 'planned',
            'release_schedule': 'Weekly',
        },
        'rosario': {
            'class': None,
            'description': 'Rosario Board of Trade',
            'auth_required': False,
            'status': 'planned',
            'release_schedule': 'Weekly',
        },
    },
    'asia_pacific': {
        'mpob': {
            'class': MPOBCollector,
            'config_class': MPOBConfig,
            'description': 'Malaysian Palm Oil Board',
            'auth_required': False,
            'status': 'implemented',
            'commodities': ['palm_oil', 'palm_kernel_oil'],
            'release_schedule': '~10th of month',
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
        'china_customs': {
            'class': None,
            'description': 'China Customs Trade Data',
            'auth_required': False,
            'status': 'planned',
            'release_schedule': 'Monthly',
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
        'un_comtrade': {
            'class': None,
            'description': 'UN Comtrade Trade Database',
            'auth_required': False,
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
                    'config_class': info.get('config_class'),
                    'description': info['description'],
                    'auth_required': info.get('auth_required', False),
                    'env_var': info.get('env_var'),
                    'commodities': info.get('commodities', []),
                    'release_schedule': info.get('release_schedule'),
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


def get_collector_config(name: str):
    """Get a collector config class by name"""
    for region, collectors in COLLECTOR_REGISTRY.items():
        if name in collectors:
            info = collectors[name]
            return info.get('config_class')
    return None


def list_collectors_by_region(region: str = None) -> dict:
    """List collectors, optionally filtered by region"""
    if region:
        return COLLECTOR_REGISTRY.get(region, {})
    return COLLECTOR_REGISTRY


def list_collectors_by_commodity(commodity: str) -> dict:
    """List collectors that provide data for a specific commodity"""
    matching = {}
    for region, collectors in COLLECTOR_REGISTRY.items():
        for name, info in collectors.items():
            commodities = info.get('commodities', [])
            if commodity.lower() in [c.lower() for c in commodities]:
                matching[name] = {
                    'region': region,
                    **info
                }
    return matching


def get_release_schedule() -> dict:
    """Get release schedule for all implemented collectors"""
    schedule = {
        'daily': [],
        'weekly': {},
        'monthly': [],
        'other': [],
    }

    for region, collectors in COLLECTOR_REGISTRY.items():
        for name, info in collectors.items():
            if info.get('status') != 'implemented':
                continue

            release = info.get('release_schedule', '')
            entry = {
                'name': name,
                'description': info['description'],
                'schedule': release,
            }

            if 'daily' in release.lower():
                schedule['daily'].append(entry)
            elif any(day in release.lower() for day in ['monday', 'tuesday', 'wednesday', 'thursday', 'friday']):
                # Weekly release
                for day in ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday']:
                    if day.lower() in release.lower():
                        if day not in schedule['weekly']:
                            schedule['weekly'][day] = []
                        schedule['weekly'][day].append(entry)
                        break
            elif 'month' in release.lower():
                schedule['monthly'].append(entry)
            elif release:
                schedule['other'].append(entry)

    return schedule
