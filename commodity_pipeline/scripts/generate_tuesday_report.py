#!/usr/bin/env python3
"""
Tuesday Report Data Generator

Pulls key commodity market data for the weekly Tuesday report.
Focuses on the most important metrics for each region.

Usage:
    python generate_tuesday_report.py
    python generate_tuesday_report.py --output-dir ./reports
"""

import os
import sys
import argparse
import logging
from datetime import datetime, date, timedelta
from pathlib import Path
import json
from typing import Dict, List, Any, Optional
from dataclasses import dataclass

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

try:
    import pandas as pd
    PANDAS_AVAILABLE = True
except ImportError:
    PANDAS_AVAILABLE = False
    print("Warning: pandas not available")

from data_collectors.collectors import (
    get_available_collectors,
    get_collector,
    get_collector_config,
    COLLECTOR_REGISTRY,
)


# ============================================================================
# REPORT CONFIGURATION
# ============================================================================

# Key data sources by priority for the Tuesday report
TUESDAY_REPORT_SOURCES = {
    'north_america': {
        'high_priority': [
            'cftc_cot',          # Positioning - released Friday
            'usda_fas',          # Export sales - released Thursday
            'eia_ethanol',       # Ethanol production - released Wednesday
            'drought',           # Drought conditions - released Thursday
            'usda_nass',         # Crop progress - released Monday
            'cme_settlements',   # Futures prices
        ],
        'medium_priority': [
            'usda_ers_feed_grains',
            'usda_ers_oil_crops',
            'usda_ers_wheat',
            'eia_petroleum',
            'epa_rfs',
        ],
        'low_priority': [
            'usda_ams_tallow',
            'usda_ams_ddgs',
            'census_trade',
            'canada_cgc',
            'canada_statscan',
        ],
    },
    'south_america': {
        'high_priority': [
            'conab',             # Brazil crop estimates
            'abiove',            # Brazil soy crush
            'imea',              # Mato Grosso state data
            'magyp',             # Argentina production
        ],
        'medium_priority': [
            'ibge_sidra',        # Brazil municipal statistics
        ],
    },
    'asia_pacific': {
        'high_priority': [
            'mpob',              # Malaysian palm oil
        ],
    },
    'global': {
        'medium_priority': [
            'faostat',           # FAO historical data
        ],
    },
}


# ============================================================================
# LOGGING
# ============================================================================

def setup_logging() -> logging.Logger:
    """Configure logging"""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s | %(levelname)-8s | %(message)s',
        datefmt='%H:%M:%S'
    )
    return logging.getLogger(__name__)


# ============================================================================
# DATA COLLECTION HELPERS
# ============================================================================

def get_api_key(env_var: str) -> Optional[str]:
    """Get API key from environment"""
    return os.environ.get(env_var)


def collect_source_data(
    source_name: str,
    logger: logging.Logger,
    lookback_days: int = 30
) -> Optional[Dict[str, Any]]:
    """Collect data from a single source"""
    available = get_available_collectors()

    if source_name not in available:
        logger.warning(f"Source {source_name} not available")
        return None

    info = available[source_name]

    # Check API key requirement
    if info.get('auth_required'):
        env_var = info.get('env_var')
        if env_var and not get_api_key(env_var):
            logger.warning(f"Skipping {source_name}: Missing {env_var}")
            return None

    try:
        collector_class = info['class']
        config_class = info.get('config_class')

        # Build config
        config_kwargs = {}
        if info.get('auth_required') and info.get('env_var'):
            api_key = get_api_key(info['env_var'])
            if api_key:
                config_kwargs['api_key'] = api_key

        if config_class:
            config = config_class(**config_kwargs)
            collector = collector_class(config)
        else:
            return None

        # Collect data
        end_date = date.today()
        start_date = end_date - timedelta(days=lookback_days)

        result = collector.collect(
            start_date=start_date,
            end_date=end_date
        )

        if result.success:
            return {
                'source': source_name,
                'description': info['description'],
                'records': result.records_fetched,
                'data': result.data,
                'from_cache': result.from_cache,
                'collected_at': str(result.collected_at),
            }

        logger.warning(f"Failed to collect from {source_name}: {result.error_message}")
        return None

    except Exception as e:
        logger.error(f"Error collecting from {source_name}: {e}")
        return None


# ============================================================================
# REPORT GENERATION
# ============================================================================

def generate_market_summary(
    data: Dict[str, Any],
    logger: logging.Logger
) -> str:
    """Generate market summary section"""
    summary_lines = []

    # COT Positioning
    if 'cftc_cot' in data and data['cftc_cot']:
        summary_lines.append("### CFTC Commitments of Traders")
        summary_lines.append("*Managed Money Positions (Net)*")
        cot_data = data['cftc_cot'].get('data', {})
        if cot_data:
            summary_lines.append(f"- Data collected: {data['cftc_cot'].get('records', 0)} records")
        summary_lines.append("")

    # Export Sales
    if 'usda_fas' in data and data['usda_fas']:
        summary_lines.append("### USDA Export Sales")
        fas_data = data['usda_fas'].get('data', {})
        if fas_data:
            summary_lines.append(f"- Data collected: {data['usda_fas'].get('records', 0)} records")
        summary_lines.append("")

    # Ethanol
    if 'eia_ethanol' in data and data['eia_ethanol']:
        summary_lines.append("### Ethanol Market")
        summary_lines.append(f"- Data collected: {data['eia_ethanol'].get('records', 0)} records")
        summary_lines.append("")

    # Drought
    if 'drought' in data and data['drought']:
        summary_lines.append("### Drought Monitor")
        summary_lines.append(f"- Data collected: {data['drought'].get('records', 0)} records")
        summary_lines.append("")

    return "\n".join(summary_lines)


def generate_south_america_summary(
    data: Dict[str, Any],
    logger: logging.Logger
) -> str:
    """Generate South America summary section"""
    summary_lines = []

    # Brazil CONAB
    if 'conab' in data and data['conab']:
        summary_lines.append("### Brazil - CONAB")
        summary_lines.append(f"- Data collected: {data['conab'].get('records', 0)} records")
        summary_lines.append("")

    # Brazil ABIOVE (soy crush)
    if 'abiove' in data and data['abiove']:
        summary_lines.append("### Brazil - ABIOVE Soy Crush")
        summary_lines.append(f"- Data collected: {data['abiove'].get('records', 0)} records")
        summary_lines.append("")

    # Mato Grosso
    if 'imea' in data and data['imea']:
        summary_lines.append("### Mato Grosso - IMEA")
        summary_lines.append(f"- Data collected: {data['imea'].get('records', 0)} records")
        summary_lines.append("")

    # Argentina
    if 'magyp' in data and data['magyp']:
        summary_lines.append("### Argentina - MAGyP")
        summary_lines.append(f"- Data collected: {data['magyp'].get('records', 0)} records")
        summary_lines.append("")

    return "\n".join(summary_lines)


def create_tuesday_report(
    collected_data: Dict[str, Any],
    output_dir: Path,
    logger: logging.Logger
) -> Path:
    """Create the Tuesday report document"""
    timestamp = datetime.now()
    report_date = timestamp.strftime("%Y-%m-%d")
    report_path = output_dir / f"tuesday_report_{report_date}.md"

    with open(report_path, 'w') as f:
        # Header
        f.write(f"# Weekly Commodity Market Report\n\n")
        f.write(f"**Report Date:** {timestamp.strftime('%A, %B %d, %Y')}\n\n")
        f.write(f"**Generated:** {timestamp.strftime('%Y-%m-%d %H:%M:%S')}\n\n")

        # Executive Summary placeholder
        f.write("## Executive Summary\n\n")
        f.write("*[Add executive summary here after reviewing data]*\n\n")
        f.write("---\n\n")

        # North America
        f.write("## North America\n\n")
        na_summary = generate_market_summary(collected_data, logger)
        f.write(na_summary if na_summary else "*No North America data collected*\n\n")
        f.write("---\n\n")

        # South America
        f.write("## South America\n\n")
        sa_summary = generate_south_america_summary(collected_data, logger)
        f.write(sa_summary if sa_summary else "*No South America data collected*\n\n")
        f.write("---\n\n")

        # Asia Pacific
        f.write("## Asia Pacific\n\n")
        if 'mpob' in collected_data and collected_data['mpob']:
            f.write("### Malaysia - MPOB Palm Oil\n")
            f.write(f"- Data collected: {collected_data['mpob'].get('records', 0)} records\n\n")
        else:
            f.write("*No Asia Pacific data collected*\n\n")
        f.write("---\n\n")

        # Data Sources Summary
        f.write("## Data Sources Summary\n\n")
        f.write("| Source | Region | Records | Status |\n")
        f.write("|--------|--------|---------|--------|\n")

        for source, data in collected_data.items():
            if data:
                status = "Cached" if data.get('from_cache') else "Fresh"
                f.write(f"| {source} | - | {data.get('records', 0)} | {status} |\n")

        total_sources = len([d for d in collected_data.values() if d])
        total_records = sum(d.get('records', 0) for d in collected_data.values() if d)
        f.write(f"\n**Total:** {total_sources} sources, {total_records} records\n\n")

        # Key Dates
        f.write("## Key Dates This Week\n\n")
        f.write("- **Monday:** USDA Crop Progress (4:00 PM ET)\n")
        f.write("- **Wednesday:** EIA Petroleum Status (10:30 AM ET)\n")
        f.write("- **Thursday:** USDA Export Sales (8:30 AM ET)\n")
        f.write("- **Thursday:** US Drought Monitor (8:30 AM ET)\n")
        f.write("- **Friday:** CFTC COT Report (3:30 PM ET)\n")

    logger.info(f"Report created: {report_path}")
    return report_path


def export_raw_data(
    collected_data: Dict[str, Any],
    output_dir: Path,
    logger: logging.Logger
) -> List[Path]:
    """Export raw data to CSV files for Power BI"""
    exported = []
    timestamp = datetime.now().strftime("%Y%m%d")

    for source, data in collected_data.items():
        if not data or not data.get('data'):
            continue

        try:
            raw_data = data['data']
            filepath = output_dir / f"{source}_{timestamp}.csv"

            if PANDAS_AVAILABLE:
                if isinstance(raw_data, pd.DataFrame):
                    df = raw_data
                elif isinstance(raw_data, list):
                    df = pd.DataFrame(raw_data)
                elif isinstance(raw_data, dict):
                    if all(isinstance(v, list) for v in raw_data.values()):
                        df = pd.DataFrame(raw_data)
                    else:
                        df = pd.DataFrame([raw_data])
                else:
                    logger.warning(f"Cannot export {source}: unsupported type")
                    continue

                df.to_csv(filepath, index=False)
            else:
                # JSON fallback
                filepath = filepath.with_suffix('.json')
                with open(filepath, 'w') as f:
                    json.dump(raw_data, f, indent=2, default=str)

            exported.append(filepath)
            logger.info(f"Exported: {filepath.name}")

        except Exception as e:
            logger.error(f"Error exporting {source}: {e}")

    return exported


# ============================================================================
# MAIN
# ============================================================================

def main():
    parser = argparse.ArgumentParser(
        description="Generate Tuesday report data"
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("./data/reports"),
        help="Output directory"
    )
    parser.add_argument(
        "--high-priority-only",
        action="store_true",
        help="Only collect high priority sources"
    )
    parser.add_argument(
        "--lookback-days",
        type=int,
        default=30,
        help="Days of historical data"
    )

    args = parser.parse_args()
    logger = setup_logging()

    # Create output directory
    args.output_dir.mkdir(parents=True, exist_ok=True)

    logger.info("=" * 60)
    logger.info("TUESDAY REPORT DATA GENERATION")
    logger.info("=" * 60)
    logger.info(f"Output: {args.output_dir}")
    logger.info("")

    # Check API keys
    logger.info("API Key Status:")
    api_keys = {
        'EIA_API_KEY': bool(get_api_key('EIA_API_KEY')),
        'NASS_API_KEY': bool(get_api_key('NASS_API_KEY')),
    }
    for key, available in api_keys.items():
        status = "OK" if available else "MISSING"
        logger.info(f"  {key}: {status}")
    logger.info("")

    # Collect data from each source
    collected_data = {}

    for region, priorities in TUESDAY_REPORT_SOURCES.items():
        logger.info(f"Region: {region.upper()}")

        sources = priorities.get('high_priority', [])
        if not args.high_priority_only:
            sources.extend(priorities.get('medium_priority', []))
            sources.extend(priorities.get('low_priority', []))

        for source in sources:
            logger.info(f"  Collecting: {source}...")
            data = collect_source_data(source, logger, args.lookback_days)
            collected_data[source] = data

            if data:
                logger.info(f"    OK - {data.get('records', 0)} records")
            else:
                logger.info(f"    SKIPPED")

        logger.info("")

    # Generate report
    logger.info("Generating report...")
    report_path = create_tuesday_report(collected_data, args.output_dir, logger)

    # Export raw data
    logger.info("Exporting raw data...")
    exported = export_raw_data(collected_data, args.output_dir, logger)

    logger.info("")
    logger.info("=" * 60)
    logger.info("COMPLETE")
    logger.info("=" * 60)
    logger.info(f"Report: {report_path}")
    logger.info(f"Data files: {len(exported)}")

    return 0


if __name__ == "__main__":
    sys.exit(main())
