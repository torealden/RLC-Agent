#!/usr/bin/env python3
"""
Test All Collectors Script

Tests each collector to verify it can connect and fetch data.
Reports success/failure status for each.

Usage:
    python scripts/test_all_collectors.py
    python scripts/test_all_collectors.py --verbose
    python scripts/test_all_collectors.py --collector cftc_cot
"""

import argparse
import logging
import os
import sys
from pathlib import Path
from datetime import date, timedelta

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from dotenv import load_dotenv

# Load credentials from centralized config
credentials_path = project_root / "config" / "credentials.env"
if credentials_path.exists():
    load_dotenv(credentials_path)
    print(f"Loaded credentials from {credentials_path}")
else:
    load_dotenv()  # Fall back to default .env
    print("Using default .env file")

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


# Test configurations for each collector
TEST_CONFIGS = {
    'cftc_cot': {
        'class': 'CFTCCOTCollector',
        'params': {'commodities': ['corn']},
        'auth_required': False,
    },
    'usda_fas': {
        'class': 'USDATFASCollector',
        'params': {'data_type': 'export_sales', 'commodities': ['corn']},
        'auth_required': False,
    },
    'drought': {
        'class': 'DroughtCollector',
        'params': {},
        'auth_required': False,
    },
    'feed_grains': {
        'class': 'FeedGrainsCollector',
        'params': {'tables': ['corn']},
        'auth_required': False,
    },
    'oil_crops': {
        'class': 'OilCropsCollector',
        'params': {'tables': ['soybeans']},
        'auth_required': False,
    },
    'wheat_data': {
        'class': 'WheatDataCollector',
        'params': {},
        'auth_required': False,
    },
    'tallow_protein': {
        'class': 'TallowProteinCollector',
        'params': {},
        'auth_required': False,
    },
    'grain_coproducts': {
        'class': 'GrainCoProductsCollector',
        'params': {},
        'auth_required': False,
    },
    'epa_rfs': {
        'class': 'EPARFSCollector',
        'params': {},
        'auth_required': False,
    },
    'census_trade': {
        'class': 'CensusTradeCollector',
        'params': {'commodities': ['corn'], 'flow_type': 'exports'},
        'auth_required': False,
    },
    'canada_cgc': {
        'class': 'CGCCollector',
        'params': {'report_types': ['visible_supply']},
        'auth_required': False,
    },
    'canada_statscan': {
        'class': 'StatsCanCollector',
        'params': {'tables': ['grain_stocks']},
        'auth_required': False,
    },
    'cme_settlements': {
        'class': 'CMESettlementsCollector',
        'params': {'contracts': ['ZC']},
        'auth_required': False,
    },
    'mpob': {
        'class': 'MPOBCollector',
        'params': {},
        'auth_required': False,
    },
    'eia_ethanol': {
        'class': 'EIAEthanolCollector',
        'params': {},
        'auth_required': True,
        'env_var': 'EIA_API_KEY',
    },
    'eia_petroleum': {
        'class': 'EIAPetroleumCollector',
        'params': {'series': ['wti_spot', 'rbob_spot']},
        'auth_required': True,
        'env_var': 'EIA_API_KEY',
    },
    'nass': {
        'class': 'NASSCollector',
        'params': {'data_type': 'crop_progress', 'commodities': ['corn']},
        'auth_required': True,
        'env_var': 'QUICK_STATS_API_KEY',
    },
}


def load_collector(class_name: str):
    """Load collector class by name"""
    from commodity_pipeline.data_collectors import (
        CFTCCOTCollector,
        USDATFASCollector,
        DroughtCollector,
        FeedGrainsCollector,
        OilCropsCollector,
        WheatDataCollector,
        TallowProteinCollector,
        GrainCoProductsCollector,
        EPARFSCollector,
        CensusTradeCollector,
        CGCCollector,
        StatsCanCollector,
        CMESettlementsCollector,
        MPOBCollector,
        EIAEthanolCollector,
        EIAPetroleumCollector,
        NASSCollector,
    )

    collectors = {
        'CFTCCOTCollector': CFTCCOTCollector,
        'USDATFASCollector': USDATFASCollector,
        'DroughtCollector': DroughtCollector,
        'FeedGrainsCollector': FeedGrainsCollector,
        'OilCropsCollector': OilCropsCollector,
        'WheatDataCollector': WheatDataCollector,
        'TallowProteinCollector': TallowProteinCollector,
        'GrainCoProductsCollector': GrainCoProductsCollector,
        'EPARFSCollector': EPARFSCollector,
        'CensusTradeCollector': CensusTradeCollector,
        'CGCCollector': CGCCollector,
        'StatsCanCollector': StatsCanCollector,
        'CMESettlementsCollector': CMESettlementsCollector,
        'MPOBCollector': MPOBCollector,
        'EIAEthanolCollector': EIAEthanolCollector,
        'EIAPetroleumCollector': EIAPetroleumCollector,
        'NASSCollector': NASSCollector,
    }

    return collectors.get(class_name)


def test_collector(name: str, config: dict, verbose: bool = False) -> dict:
    """Test a single collector"""
    result = {
        'name': name,
        'status': 'unknown',
        'records': 0,
        'error': None,
        'warnings': [],
    }

    # Check auth
    if config.get('auth_required'):
        env_var = config.get('env_var')
        if not os.getenv(env_var):
            result['status'] = 'skipped'
            result['error'] = f'Missing {env_var}'
            return result

    try:
        # Load collector
        CollectorClass = load_collector(config['class'])
        if not CollectorClass:
            result['status'] = 'error'
            result['error'] = f'Class not found: {config["class"]}'
            return result

        collector = CollectorClass()

        # Test connection first
        if hasattr(collector, 'test_connection'):
            success, message = collector.test_connection()
            if verbose:
                logger.info(f"  Connection test: {message}")

        # Run collection with date range
        end_date = date.today()
        start_date = end_date - timedelta(days=30)

        params = config.get('params', {}).copy()
        params['start_date'] = start_date
        params['end_date'] = end_date

        collection_result = collector.collect(**params)

        result['status'] = 'success' if collection_result.success else 'failed'
        result['records'] = collection_result.records_fetched or 0
        result['warnings'] = collection_result.warnings or []

        if not collection_result.success:
            result['error'] = collection_result.error_message

    except Exception as e:
        result['status'] = 'error'
        result['error'] = str(e)
        if verbose:
            import traceback
            traceback.print_exc()

    return result


def run_tests(collectors: list = None, verbose: bool = False):
    """Run tests on all or specified collectors"""
    collectors = collectors or list(TEST_CONFIGS.keys())

    print("\n" + "="*70)
    print("COLLECTOR TEST SUITE")
    print("="*70 + "\n")

    results = []

    # Separate by auth requirement
    no_auth = [(n, c) for n, c in TEST_CONFIGS.items()
               if n in collectors and not c.get('auth_required')]
    auth_req = [(n, c) for n, c in TEST_CONFIGS.items()
                if n in collectors and c.get('auth_required')]

    print("--- Testing collectors (no API key required) ---\n")
    for name, config in no_auth:
        print(f"Testing {name}...", end=' ', flush=True)
        result = test_collector(name, config, verbose)
        results.append(result)

        if result['status'] == 'success':
            print(f"✓ ({result['records']} records)")
        elif result['status'] == 'skipped':
            print(f"○ Skipped: {result['error']}")
        else:
            print(f"✗ {result['error']}")

        if verbose and result['warnings']:
            for w in result['warnings']:
                print(f"  Warning: {w}")

    print("\n--- Testing collectors (API key required) ---\n")
    for name, config in auth_req:
        print(f"Testing {name}...", end=' ', flush=True)
        result = test_collector(name, config, verbose)
        results.append(result)

        if result['status'] == 'success':
            print(f"✓ ({result['records']} records)")
        elif result['status'] == 'skipped':
            print(f"○ Skipped: {result['error']}")
        else:
            print(f"✗ {result['error']}")

        if verbose and result['warnings']:
            for w in result['warnings']:
                print(f"  Warning: {w}")

    # Summary
    print("\n" + "="*70)
    print("TEST SUMMARY")
    print("="*70 + "\n")

    success = [r for r in results if r['status'] == 'success']
    failed = [r for r in results if r['status'] in ('failed', 'error')]
    skipped = [r for r in results if r['status'] == 'skipped']

    print(f"Total: {len(results)}")
    print(f"  ✓ Passed:  {len(success)}")
    print(f"  ✗ Failed:  {len(failed)}")
    print(f"  ○ Skipped: {len(skipped)}")

    if failed:
        print("\nFailed collectors:")
        for r in failed:
            print(f"  - {r['name']}: {r['error']}")

    total_records = sum(r['records'] for r in results)
    print(f"\nTotal records fetched: {total_records:,}")

    return results


def main():
    parser = argparse.ArgumentParser(description='Test commodity data collectors')
    parser.add_argument('--collector', '-c', nargs='+',
                       help='Specific collectors to test')
    parser.add_argument('--verbose', '-v', action='store_true',
                       help='Verbose output')
    parser.add_argument('--list', '-l', action='store_true',
                       help='List available collectors')

    args = parser.parse_args()

    if args.list:
        print("\nAvailable collectors:")
        for name, config in sorted(TEST_CONFIGS.items()):
            auth = f"(requires {config.get('env_var')})" if config.get('auth_required') else ""
            print(f"  {name:20} {auth}")
        return

    run_tests(
        collectors=args.collector,
        verbose=args.verbose
    )


if __name__ == '__main__':
    main()
