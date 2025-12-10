"""
Data Collectors Module

Comprehensive framework for collecting agricultural commodity data
from various public and private data sources worldwide.

## Implemented Collectors (Ready to Use)

### North America
- **CFTC COT**: Weekly positioning data (FREE, no auth)
- **USDA FAS**: Export sales & PSD data (FREE, no auth)
- **EIA Ethanol**: Ethanol production & stocks (FREE, requires API key)
- **Drought Monitor**: US drought conditions (FREE, no auth)

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

## Quick Start

```python
from commodity_pipeline.data_collectors import (
    CFTCCOTCollector,
    USDATFASCollector,
    EIAEthanolCollector,
    DroughtCollector,
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
    COLLECTOR_REGISTRY,
)

# List all implemented collectors
available = get_available_collectors()

# Get collector by name
CollectorClass = get_collector('cftc_cot')

# List by region
south_america = list_collectors_by_region('south_america')
```
"""

from .collectors import (
    # Base classes
    BaseCollector,
    CollectorResult,
    CollectorConfig,

    # North America
    CFTCCOTCollector,
    CFTCCOTConfig,
    USDATFASCollector,
    USDATFASConfig,
    EIAEthanolCollector,
    EIAEthanolConfig,
    DroughtCollector,
    DroughtConfig,

    # Asia Pacific
    MPOBCollector,
    MPOBConfig,

    # Registry functions
    get_available_collectors,
    get_collector,
    list_collectors_by_region,
    COLLECTOR_REGISTRY,
)

__version__ = '1.0.0'

__all__ = [
    # Base
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

    # Registry
    'get_available_collectors',
    'get_collector',
    'list_collectors_by_region',
    'COLLECTOR_REGISTRY',
]
