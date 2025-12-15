#!/usr/bin/env python3
"""
Commodity Data Pull Script

Comprehensive script to test and pull data from all implemented collectors.
Generates data exports suitable for Power BI and reporting.

Usage:
    python pull_commodity_data.py --test-only          # Test connectivity only
    python pull_commodity_data.py --region north_america   # Pull specific region
    python pull_commodity_data.py --export-format csv  # Export format
    python pull_commodity_data.py                      # Full data pull

Environment Variables Required:
    EIA_API_KEY - For EIA ethanol and petroleum data
    NASS_API_KEY - For USDA NASS data
    CENSUS_API_KEY - (Optional) For Census trade data
"""

import os
import sys
import argparse
import logging
from datetime import datetime, date, timedelta
from pathlib import Path
import json
from typing import Dict, List, Any, Optional, Tuple
from dataclasses import dataclass, field
import time

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

try:
    import pandas as pd
    PANDAS_AVAILABLE = True
except ImportError:
    PANDAS_AVAILABLE = False
    print("Warning: pandas not available. Some features disabled.")

from data_collectors.collectors import (
    COLLECTOR_REGISTRY,
    get_available_collectors,
    CollectorResult,
)


# ============================================================================
# CONFIGURATION
# ============================================================================

@dataclass
class PullConfig:
    """Configuration for data pull"""
    output_dir: Path = field(default_factory=lambda: Path("./data/exports"))
    cache_dir: Path = field(default_factory=lambda: Path("./data/cache"))
    export_format: str = "csv"  # csv, json, parquet
    test_only: bool = False
    regions: List[str] = field(default_factory=lambda: [
        "north_america", "south_america", "asia_pacific", "global"
    ])
    use_cache: bool = True
    parallel: bool = False
    verbose: bool = False

    # Date ranges
    lookback_days: int = 365
    end_date: date = field(default_factory=date.today)

    def __post_init__(self):
        self.start_date = self.end_date - timedelta(days=self.lookback_days)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.cache_dir.mkdir(parents=True, exist_ok=True)


@dataclass
class CollectorTestResult:
    """Result of testing a single collector"""
    name: str
    region: str
    success: bool
    message: str
    response_time_ms: int = 0
    records: int = 0
    warnings: List[str] = field(default_factory=list)


# ============================================================================
# LOGGING SETUP
# ============================================================================

def setup_logging(verbose: bool = False) -> logging.Logger:
    """Configure logging"""
    level = logging.DEBUG if verbose else logging.INFO

    logging.basicConfig(
        level=level,
        format='%(asctime)s | %(levelname)-8s | %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )

    return logging.getLogger(__name__)


# ============================================================================
# API KEY MANAGEMENT
# ============================================================================

API_KEYS = {
    'EIA_API_KEY': os.environ.get('EIA_API_KEY'),
    'NASS_API_KEY': os.environ.get('NASS_API_KEY'),
    'CENSUS_API_KEY': os.environ.get('CENSUS_API_KEY'),
}


def check_api_keys(logger: logging.Logger) -> Dict[str, bool]:
    """Check which API keys are configured"""
    status = {}
    for key, value in API_KEYS.items():
        status[key] = value is not None and len(value) > 0
        if status[key]:
            logger.info(f"  {key}: Configured")
        else:
            logger.warning(f"  {key}: NOT SET")
    return status


def get_collectors_needing_keys() -> Dict[str, str]:
    """Get map of collectors to their required API key env vars"""
    needs_key = {}
    for region, collectors in COLLECTOR_REGISTRY.items():
        for name, info in collectors.items():
            if info.get('auth_required') and info.get('env_var'):
                needs_key[name] = info['env_var']
    return needs_key


# ============================================================================
# COLLECTOR TESTING
# ============================================================================

def test_collector(
    name: str,
    info: Dict,
    logger: logging.Logger
) -> CollectorTestResult:
    """Test a single collector"""
    region = info.get('region', 'unknown')

    # Check if implemented
    collector_class = info.get('class')
    if not collector_class:
        return CollectorTestResult(
            name=name,
            region=region,
            success=False,
            message="Not implemented (planned)"
        )

    # Check API key if required
    if info.get('auth_required'):
        env_var = info.get('env_var')
        if env_var and not os.environ.get(env_var):
            return CollectorTestResult(
                name=name,
                region=region,
                success=False,
                message=f"Missing API key: {env_var}",
                warnings=[f"Set {env_var} environment variable"]
            )

    # Instantiate and test
    try:
        config_class = info.get('config_class')
        if config_class:
            # Build config with API key if needed
            config_kwargs = {}
            if info.get('auth_required') and info.get('env_var'):
                api_key = os.environ.get(info['env_var'])
                if api_key:
                    config_kwargs['api_key'] = api_key

            config = config_class(**config_kwargs)
            collector = collector_class(config)
        else:
            collector = collector_class()

        start = time.time()
        success, message = collector.test_connection()
        elapsed = int((time.time() - start) * 1000)

        return CollectorTestResult(
            name=name,
            region=region,
            success=success,
            message=message,
            response_time_ms=elapsed
        )

    except Exception as e:
        return CollectorTestResult(
            name=name,
            region=region,
            success=False,
            message=f"Error: {str(e)}"
        )


def run_connectivity_tests(
    config: PullConfig,
    logger: logging.Logger
) -> List[CollectorTestResult]:
    """Test connectivity to all collectors"""
    results = []
    available = get_available_collectors()

    logger.info("=" * 60)
    logger.info("CONNECTIVITY TESTS")
    logger.info("=" * 60)

    for name, info in available.items():
        if info['region'] not in config.regions:
            continue

        logger.info(f"Testing {name}...")
        result = test_collector(name, info, logger)
        results.append(result)

        status = "OK" if result.success else "FAIL"
        logger.info(f"  [{status}] {result.message} ({result.response_time_ms}ms)")

        for warning in result.warnings:
            logger.warning(f"  ! {warning}")

    # Summary
    successful = sum(1 for r in results if r.success)
    logger.info("-" * 60)
    logger.info(f"Results: {successful}/{len(results)} collectors online")

    return results


# ============================================================================
# DATA COLLECTION
# ============================================================================

def collect_from_source(
    name: str,
    info: Dict,
    config: PullConfig,
    logger: logging.Logger
) -> Tuple[Optional[CollectorResult], List[str]]:
    """Collect data from a single source"""
    warnings = []

    collector_class = info.get('class')
    config_class = info.get('config_class')

    if not collector_class:
        return None, ["Not implemented"]

    # Check API key
    if info.get('auth_required'):
        env_var = info.get('env_var')
        if env_var and not os.environ.get(env_var):
            return None, [f"Missing {env_var}"]

    try:
        # Build config
        config_kwargs = {
            'cache_directory': config.cache_dir,
            'cache_enabled': config.use_cache,
        }

        if info.get('auth_required') and info.get('env_var'):
            api_key = os.environ.get(info['env_var'])
            if api_key:
                config_kwargs['api_key'] = api_key

        if config_class:
            collector_config = config_class(**config_kwargs)
            collector = collector_class(collector_config)
        else:
            collector = collector_class()

        # Collect data
        result = collector.collect(
            start_date=config.start_date,
            end_date=config.end_date,
            use_cache=config.use_cache
        )

        if result.warnings:
            warnings.extend(result.warnings)

        return result, warnings

    except Exception as e:
        logger.error(f"Error collecting from {name}: {e}")
        return None, [str(e)]


def pull_all_data(
    config: PullConfig,
    logger: logging.Logger
) -> Dict[str, CollectorResult]:
    """Pull data from all configured collectors"""
    results = {}
    available = get_available_collectors()

    logger.info("=" * 60)
    logger.info("DATA COLLECTION")
    logger.info("=" * 60)
    logger.info(f"Date range: {config.start_date} to {config.end_date}")
    logger.info(f"Regions: {', '.join(config.regions)}")
    logger.info("-" * 60)

    for name, info in available.items():
        if info['region'] not in config.regions:
            continue

        logger.info(f"Collecting: {name} ({info['description']})")

        result, warnings = collect_from_source(name, info, config, logger)

        if result and result.success:
            records = result.records_fetched
            cache_note = " (cached)" if result.from_cache else ""
            logger.info(f"  SUCCESS: {records} records{cache_note}")
            results[name] = result
        else:
            reason = warnings[0] if warnings else "Unknown error"
            logger.warning(f"  SKIPPED: {reason}")

        for w in warnings:
            if w not in ["Not implemented"]:
                logger.warning(f"  ! {w}")

    # Summary
    logger.info("-" * 60)
    total_records = sum(r.records_fetched for r in results.values())
    logger.info(f"Collected from {len(results)} sources, {total_records} total records")

    return results


# ============================================================================
# DATA EXPORT
# ============================================================================

def export_data(
    results: Dict[str, CollectorResult],
    config: PullConfig,
    logger: logging.Logger
) -> List[Path]:
    """Export collected data to files"""
    exported_files = []
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    logger.info("=" * 60)
    logger.info("DATA EXPORT")
    logger.info("=" * 60)
    logger.info(f"Format: {config.export_format}")
    logger.info(f"Output: {config.output_dir}")
    logger.info("-" * 60)

    for name, result in results.items():
        if not result.data:
            continue

        # Determine filename
        filename = f"{name}_{timestamp}.{config.export_format}"
        filepath = config.output_dir / filename

        try:
            data = result.data

            if config.export_format == "csv":
                if PANDAS_AVAILABLE:
                    if isinstance(data, pd.DataFrame):
                        df = data
                    elif isinstance(data, list):
                        df = pd.DataFrame(data)
                    elif isinstance(data, dict):
                        df = pd.DataFrame([data] if not isinstance(list(data.values())[0], list) else data)
                    else:
                        logger.warning(f"  {name}: Cannot export type {type(data)}")
                        continue

                    df.to_csv(filepath, index=False)
                else:
                    # Fallback: write as JSON
                    filepath = filepath.with_suffix('.json')
                    with open(filepath, 'w') as f:
                        json.dump(data, f, indent=2, default=str)

            elif config.export_format == "json":
                if PANDAS_AVAILABLE and isinstance(data, pd.DataFrame):
                    data = data.to_dict(orient='records')
                with open(filepath, 'w') as f:
                    json.dump(data, f, indent=2, default=str)

            elif config.export_format == "parquet":
                if PANDAS_AVAILABLE:
                    if isinstance(data, pd.DataFrame):
                        df = data
                    else:
                        df = pd.DataFrame(data)
                    df.to_parquet(filepath)
                else:
                    logger.warning(f"  {name}: Parquet requires pandas")
                    continue

            exported_files.append(filepath)
            logger.info(f"  Exported: {filename}")

        except Exception as e:
            logger.error(f"  Error exporting {name}: {e}")

    logger.info("-" * 60)
    logger.info(f"Exported {len(exported_files)} files")

    return exported_files


def create_summary_report(
    test_results: List[CollectorTestResult],
    data_results: Dict[str, CollectorResult],
    config: PullConfig,
    logger: logging.Logger
) -> Path:
    """Create summary report for the data pull"""
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    report_path = config.output_dir / f"data_pull_report_{timestamp}.md"

    with open(report_path, 'w') as f:
        f.write("# Commodity Data Pull Report\n\n")
        f.write(f"**Generated:** {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n")
        f.write(f"**Date Range:** {config.start_date} to {config.end_date}\n\n")
        f.write(f"**Regions:** {', '.join(config.regions)}\n\n")

        # Connectivity Summary
        f.write("## Connectivity Status\n\n")
        f.write("| Collector | Region | Status | Response Time |\n")
        f.write("|-----------|--------|--------|---------------|\n")

        for result in test_results:
            status = "OK" if result.success else "FAIL"
            f.write(f"| {result.name} | {result.region} | {status} | {result.response_time_ms}ms |\n")

        successful = sum(1 for r in test_results if r.success)
        f.write(f"\n**Summary:** {successful}/{len(test_results)} collectors online\n\n")

        # Data Collection Summary
        if data_results:
            f.write("## Data Collection Summary\n\n")
            f.write("| Source | Records | Cached | Period |\n")
            f.write("|--------|---------|--------|--------|\n")

            for name, result in data_results.items():
                cached = "Yes" if result.from_cache else "No"
                period = f"{result.period_start or '-'} to {result.period_end or '-'}"
                f.write(f"| {name} | {result.records_fetched} | {cached} | {period} |\n")

            total_records = sum(r.records_fetched for r in data_results.values())
            f.write(f"\n**Total:** {total_records} records from {len(data_results)} sources\n\n")

        # API Key Status
        f.write("## API Key Status\n\n")
        for key, value in API_KEYS.items():
            status = "Configured" if value else "NOT SET"
            f.write(f"- **{key}:** {status}\n")

        # Notes for Tuesday Report
        f.write("\n## Notes for Report\n\n")
        f.write("### Key Data Sources Active:\n\n")

        by_region = {}
        for name, result in data_results.items():
            info = get_available_collectors().get(name, {})
            region = info.get('region', 'unknown')
            if region not in by_region:
                by_region[region] = []
            by_region[region].append(name)

        for region, sources in by_region.items():
            f.write(f"**{region.replace('_', ' ').title()}:**\n")
            for source in sources:
                f.write(f"- {source}\n")
            f.write("\n")

        # Missing/Unavailable
        f.write("### Sources Not Available:\n\n")
        failed = [r for r in test_results if not r.success]
        if failed:
            for result in failed:
                f.write(f"- {result.name}: {result.message}\n")
        else:
            f.write("All sources available!\n")

    logger.info(f"Report saved: {report_path}")
    return report_path


# ============================================================================
# MAIN
# ============================================================================

def parse_args() -> argparse.Namespace:
    """Parse command line arguments"""
    parser = argparse.ArgumentParser(
        description="Pull commodity data from all configured collectors"
    )

    parser.add_argument(
        "--test-only",
        action="store_true",
        help="Only test connectivity, don't pull data"
    )

    parser.add_argument(
        "--region",
        choices=["north_america", "south_america", "asia_pacific", "europe", "global", "all"],
        default="all",
        help="Region to pull data from"
    )

    parser.add_argument(
        "--export-format",
        choices=["csv", "json", "parquet"],
        default="csv",
        help="Export format for data"
    )

    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("./data/exports"),
        help="Output directory for exports"
    )

    parser.add_argument(
        "--lookback-days",
        type=int,
        default=365,
        help="Days of historical data to fetch"
    )

    parser.add_argument(
        "--no-cache",
        action="store_true",
        help="Disable cache, fetch fresh data"
    )

    parser.add_argument(
        "-v", "--verbose",
        action="store_true",
        help="Verbose output"
    )

    return parser.parse_args()


def main():
    """Main entry point"""
    args = parse_args()
    logger = setup_logging(args.verbose)

    logger.info("=" * 60)
    logger.info("COMMODITY DATA PULL")
    logger.info("=" * 60)
    logger.info(f"Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    # Determine regions
    if args.region == "all":
        regions = ["north_america", "south_america", "asia_pacific", "global"]
    else:
        regions = [args.region]

    # Build config
    config = PullConfig(
        output_dir=args.output_dir,
        export_format=args.export_format,
        test_only=args.test_only,
        regions=regions,
        use_cache=not args.no_cache,
        verbose=args.verbose,
        lookback_days=args.lookback_days,
    )

    # Check API keys
    logger.info("\nChecking API Keys:")
    check_api_keys(logger)

    # Run connectivity tests
    test_results = run_connectivity_tests(config, logger)

    # Pull data if not test-only
    data_results = {}
    if not config.test_only:
        data_results = pull_all_data(config, logger)

        # Export data
        if data_results:
            export_data(data_results, config, logger)

    # Create summary report
    report_path = create_summary_report(test_results, data_results, config, logger)

    logger.info("=" * 60)
    logger.info("COMPLETE")
    logger.info("=" * 60)
    logger.info(f"Report: {report_path}")

    # Return exit code
    all_ok = all(r.success for r in test_results)
    return 0 if all_ok else 1


if __name__ == "__main__":
    sys.exit(main())
