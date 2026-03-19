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
    # === Daily Futures ===
    'yfinance_futures': {
        'module': 'src.agents.collectors.market.yfinance_daily_collector',
        'class': 'YFinanceDailyCollector',
    },
    'futures_overnight': {
        'module': 'src.agents.collectors.market.futures_data_collector',
        'class': 'FuturesDataCollector',
    },
    'futures_us_session': {
        'module': 'src.agents.collectors.market.futures_data_collector',
        'class': 'FuturesDataCollector',
    },
    'futures_settlement': {
        'module': 'src.agents.collectors.market.futures_data_collector',
        'class': 'FuturesDataCollector',
    },

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
        # NOTE: WASDE data comes via FAS PSD API — this collector
        # is a dedicated PSD puller. Class name typo is original.
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

    # === Cash Prices (AMS structured) ===
    'usda_ams_cash_prices': {
        'module': 'src.agents.collectors.us.ams_cash_price_collector',
        'class': 'AMSCashPriceCollector',
    },

    # === South America — Tier 2 (newly wired) ===
    'argentina_magyp': {
        'module': 'src.agents.collectors.south_america.magyp_collector',
        'class': 'MAGyPCollector',
    },
    'brazil_ibge_sidra': {
        'module': 'src.agents.collectors.south_america.ibge_sidra_collector',
        'class': 'IBGESIDRACollector',
    },
    'brazil_imea': {
        'module': 'src.agents.collectors.south_america.imea_collector',
        'class': 'IMEACollector',
    },

    # === Global — Tier 2 (newly wired) ===
    'faostat': {
        'module': 'src.agents.collectors.global.faostat_collector',
        'class': 'FAOSTATCollector',
    },

    # === South America (existing) ===
    'conab': {
        'module': 'src.agents.collectors.south_america.conab_collector',
        'class': 'CONABCollector',
    },
    'anec': {
        'module': 'src.agents.collectors.south_america.anec_collector',
        'class': 'ANECCollector',
    },

    # === Yield Model ===
    'yield_forecast': {
        'module': 'src.agents.collectors.us.yield_forecast_collector',
        'class': 'YieldForecastCollector',
    },

    # === Weather Intelligence ===
    'weather_daily_summary': {
        'module': 'src.agents.collectors.us.weather_summary_collector',
        'class': 'WeatherSummaryCollector',
    },

    # === Tier 1 New Collectors ===
    'fgis_inspections': {
        'module': 'src.agents.collectors.us.fgis_inspections_collector',
        'class': 'FGISInspectionsCollector',
    },
    'brazil_comexstat': {
        'module': 'src.agents.collectors.south_america.comexstat_collector',
        'class': 'ComexStatCollector',
    },
    'argentina_indec': {
        'module': 'src.agents.collectors.south_america.indec_collector',
        'class': 'INDECCollector',
    },

    # === EPA ECHO Facility Intelligence ===
    'epa_echo_oilseed': {
        'module': 'src.agents.collectors.us.epa_echo_facility_collector',
        'class': 'EPAEchoOilseedCollector',
    },
    'epa_echo_ethanol': {
        'module': 'src.agents.collectors.us.epa_echo_facility_collector',
        'class': 'EPAEchoEthanolCollector',
    },
    'epa_echo_biodiesel': {
        'module': 'src.agents.collectors.us.epa_echo_facility_collector',
        'class': 'EPAEchoBiodieselCollector',
    },
    'epa_echo_milling': {
        'module': 'src.agents.collectors.us.epa_echo_facility_collector',
        'class': 'EPAEchoMillingCollector',
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
