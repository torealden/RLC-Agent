"""
Data Collectors Module

Comprehensive framework for collecting agricultural commodity data
from various public and private data sources worldwide.

## Implemented Collectors (Ready to Use)

### North America - United States
- **CFTC COT**: Weekly positioning data (FREE, no auth)
- **USDA FAS**: Export sales & PSD data (FREE, no auth)
- **USDA ERS**: Feed grains, oil crops, wheat databases (FREE, no auth)
- **USDA NASS**: Crop progress, production, stocks (FREE, requires API key)
- **USDA AMS**: Tallow, grease, DDGS prices (FREE, no auth)
- **EIA Ethanol**: Ethanol production & stocks (FREE, requires API key)
- **EIA Petroleum**: Crude, products, natural gas (FREE, requires API key)
- **EPA RFS**: RIN generation data (FREE, no auth)
- **Drought Monitor**: US drought conditions (FREE, no auth)
- **Census Trade**: US import/export trade (FREE, optional API key)
- **CME Settlements**: Futures settlement prices (FREE, no auth)

### North America - Canada
- **CGC**: Canadian Grain Commission weekly data (FREE, no auth)
- **Statistics Canada**: Production, stocks, crush (FREE, no auth)
- **Canola Council**: Canola industry statistics (FREE, no auth)

### Asia Pacific
- **MPOB**: Malaysian palm oil data (FREE, web scraping)

## Planned Collectors

### South America
- CONAB: Brazil crop estimates
- ABIOVE: Brazil soy crush statistics
- BCBA/Rosario: Argentina grain exchanges

### Europe
- Eurostat: EU production/trade data
- EC AGRI: European Commission agriculture

### Global
- FAO-AMIS: Global food security data
- IGC: International Grains Council
- UN Comtrade: Global trade database

## Quick Start

```python
from commodity_pipeline.data_collectors import (
    # US Core
    CFTCCOTCollector,
    USDATFASCollector,
    EIAEthanolCollector,
    DroughtCollector,

    # US Extended
    FeedGrainsCollector,
    OilCropsCollector,
    NASSCollector,
    EIAPetroleumCollector,
    TallowProteinCollector,
    CensusTradeCollector,
    CMESettlementsCollector,

    # Canada
    CGCCollector,
    StatsCanCollector,

    # Asia Pacific
    MPOBCollector,
)

# CFTC COT - no auth needed
cot = CFTCCOTCollector()
result = cot.collect()

# USDA FAS - no auth needed
fas = USDATFASCollector()
result = fas.collect(data_type="export_sales")

# EIA - requires API key
import os
os.environ['EIA_API_KEY'] = 'your_key'
eia = EIAEthanolCollector()
result = eia.collect()
```

## Registry Functions

```python
from commodity_pipeline.data_collectors.collectors import (
    get_available_collectors,
    get_collector,
    list_collectors_by_region,
    list_collectors_by_commodity,
    get_release_schedule,
    COLLECTOR_REGISTRY,
)

# List all implemented collectors
available = get_available_collectors()

# Get collector by name
CollectorClass = get_collector('cftc_cot')

# List by region
north_america = list_collectors_by_region('north_america')

# List by commodity
corn_collectors = list_collectors_by_commodity('corn')

# Get release schedule
schedule = get_release_schedule()
```
"""

from .collectors import (
    # Base classes
    BaseCollector,
    CollectorResult,
    CollectorConfig,
    DataFrequency,
    AuthType,

    # North America - Core
    CFTCCOTCollector,
    CFTCCOTConfig,
    USDATFASCollector,
    USDATFASConfig,
    EIAEthanolCollector,
    EIAEthanolConfig,
    DroughtCollector,
    DroughtConfig,

    # North America - USDA ERS
    FeedGrainsCollector,
    FeedGrainsConfig,
    OilCropsCollector,
    OilCropsConfig,
    WheatDataCollector,
    WheatDataConfig,

    # North America - USDA NASS
    NASSCollector,
    NASSConfig,

    # North America - EIA Petroleum
    EIAPetroleumCollector,
    EIAPetroleumConfig,

    # North America - EPA RFS
    EPARFSCollector,
    EPARFSConfig,

    # North America - USDA AMS
    TallowProteinCollector,
    TallowProteinConfig,
    GrainCoProductsCollector,
    GrainCoProductsConfig,

    # North America - Census Trade
    CensusTradeCollector,
    CensusTradeConfig,

    # North America - Canada
    CGCCollector,
    CGCConfig,
    StatsCanCollector,
    StatsCanConfig,
    CanolaCouncilCollector,
    CanolaConcilConfig,

    # North America - CME
    CMESettlementsCollector,
    CMESettlementsConfig,

    # Asia Pacific
    MPOBCollector,
    MPOBConfig,

    # Registry functions
    get_available_collectors,
    get_collector,
    get_collector_config,
    list_collectors_by_region,
    list_collectors_by_commodity,
    get_release_schedule,
    COLLECTOR_REGISTRY,
)

__version__ = '2.0.0'

__all__ = [
    # Base
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

    # Registry
    'get_available_collectors',
    'get_collector',
    'get_collector_config',
    'list_collectors_by_region',
    'list_collectors_by_commodity',
    'get_release_schedule',
    'COLLECTOR_REGISTRY',
]
