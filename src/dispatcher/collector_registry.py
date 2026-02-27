"""
Collector Registry (COLLECTOR_MAP)

Maps collector schedule names (from RELEASE_SCHEDULES in master_scheduler.py)
to the actual collector classes. Uses lazy imports to avoid loading all 30+
modules at startup.

Usage:
    registry = CollectorRegistry()
    collector = registry.get_collector('cftc_cot')
    collector.collect()
"""

import logging
from typing import Dict, Optional, Callable, Any, List

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Registry: schedule_key -> (module_path, class_name)
# ---------------------------------------------------------------------------
# Maps the keys in RELEASE_SCHEDULES to their actual import locations.
# Only the module path and class name are stored — actual imports are lazy.

COLLECTOR_MAP: Dict[str, Dict[str, str]] = {
    # === CORE 5 (Phase 1 priority) ===
    'cftc_cot': {
        'module': 'src.agents.collectors.us.cftc_cot_collector',
        'class': 'CFTCCOTCollector',
    },
    'usda_fas_export_sales': {
        'module': 'src.agents.collectors.us.usda_fas_collector',
        'class': 'USDATFASCollector',
    },
    'usda_wasde': {
        'module': 'src.agents.collectors.us.usda_wasde_collector',
        'class': 'USDAWASPECollector',
    },
    'cme_settlements': {
        'module': 'src.agents.collectors.market.cme_settlements_collector',
        'class': 'CMESettlementsCollector',
    },
    'eia_ethanol': {
        'module': 'src.agents.collectors.us.eia_ethanol_collector',
        'class': 'EIAEthanolCollector',
    },

    # === Weekly ===
    'usda_nass_crop_progress': {
        'module': 'src.agents.collectors.us.usda_nass_collector',
        'class': 'NASSCollector',
    },
    'eia_petroleum': {
        'module': 'src.agents.collectors.us.eia_petroleum_collector',
        'class': 'EIAPetroleumCollector',
    },
    'drought_monitor': {
        'module': 'src.agents.collectors.us.drought_collector',
        'class': 'DroughtCollector',
    },
    'canada_cgc': {
        'module': 'src.agents.collectors.canada.canada_cgc_collector',
        'class': 'CGCCollector',
    },
    'usda_ams_feedstocks': {
        'module': 'src.agents.collectors.us.usda_ams_collector',
        'class': 'TallowProteinCollector',
    },
    'usda_ams_ddgs': {
        'module': 'src.agents.collectors.us.usda_ams_collector',
        'class': 'GrainCoProductsCollector',
    },

    # === Monthly ===
    'mpob': {
        'module': 'src.agents.collectors.asia.mpob_collector',
        'class': 'MPOBCollector',
    },
    'census_trade': {
        'module': 'src.agents.collectors.us.census_trade_collector',
        'class': 'CensusTradeCollector',
    },
    'epa_rfs': {
        'module': 'src.agents.collectors.us.epa_rfs_collector',
        'class': 'EPARFSCollector',
    },
    'canada_statscan': {
        'module': 'src.agents.collectors.canada.canada_statscan_collector',
        'class': 'StatsCanCollector',
    },

    # === Quarterly ===
    'usda_nass_stocks': {
        'module': 'src.agents.collectors.us.usda_nass_collector',
        'class': 'NASSCollector',
    },
    'canada_statscan_stocks': {
        'module': 'src.agents.collectors.canada.canada_statscan_collector',
        'class': 'StatsCanCollector',
    },

    # === On-Demand / ERS ===
    'usda_ers_feed_grains': {
        'module': 'src.agents.collectors.us.usda_ers_collector',
        'class': 'FeedGrainsCollector',
    },
    'usda_ers_oil_crops': {
        'module': 'src.agents.collectors.us.usda_ers_collector',
        'class': 'OilCropsCollector',
    },
    'usda_ers_wheat': {
        'module': 'src.agents.collectors.us.usda_ers_collector',
        'class': 'WheatDataCollector',
    },

    # === South America (not yet in RELEASE_SCHEDULES but available) ===
    'conab': {
        'module': 'src.agents.collectors.south_america.conab_collector',
        'class': 'CONABCollector',
    },
    'anec': {
        'module': 'src.agents.collectors.south_america.anec_collector',
        'class': 'ANECCollector',
    },
}


class CollectorRegistry:
    """
    Registry of available data collectors.

    Provides lazy loading — collector modules are only imported when
    get_collector() is called, avoiding import-time side effects.
    """

    def __init__(self):
        self._cache: Dict[str, type] = {}

    def _import_class(self, module_path: str, class_name: str) -> type:
        """Dynamically import a collector class."""
        import importlib
        module = importlib.import_module(module_path)
        return getattr(module, class_name)

    def get_collector_class(self, name: str) -> Optional[type]:
        """
        Get the collector class for a schedule key.

        Args:
            name: Collector schedule key (e.g., 'cftc_cot')

        Returns:
            The collector class, or None if not registered
        """
        if name not in COLLECTOR_MAP:
            logger.warning(f"Collector '{name}' not in registry")
            return None

        if name not in self._cache:
            entry = COLLECTOR_MAP[name]
            try:
                cls = self._import_class(entry['module'], entry['class'])
                self._cache[name] = cls
                logger.debug(f"Loaded collector class: {entry['class']}")
            except (ImportError, AttributeError) as e:
                logger.error(f"Failed to import collector '{name}': {e}")
                return None

        return self._cache[name]

    def get_collector(self, name: str, **kwargs) -> Optional[Any]:
        """
        Get an instantiated collector.

        Args:
            name: Collector schedule key
            **kwargs: Passed to collector constructor

        Returns:
            Instantiated collector, or None if not available
        """
        cls = self.get_collector_class(name)
        if cls is None:
            return None

        try:
            return cls(**kwargs) if kwargs else cls()
        except Exception as e:
            logger.error(f"Failed to instantiate collector '{name}': {e}")
            return None

    def list_collectors(self) -> List[Dict[str, str]]:
        """List all registered collectors with their module/class info."""
        result = []
        for name, entry in COLLECTOR_MAP.items():
            result.append({
                'name': name,
                'module': entry['module'],
                'class': entry['class'],
                'loaded': name in self._cache,
            })
        return result

    def is_registered(self, name: str) -> bool:
        """Check if a collector name is registered."""
        return name in COLLECTOR_MAP
