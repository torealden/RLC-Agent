#!/usr/bin/env python3
"""
Brazil Soybean Weekly Report - Data Pack Generator

This script creates the weekly_data_pack.json that contains all data
needed for the LLM to write the report. No hallucinations - only data
that's been validated and loaded.

Usage:
    # Generate data pack for current week
    python weekly_data_pack.py

    # Generate for specific date
    python weekly_data_pack.py --date 2026-01-22

Output:
    data/brazil_soy/output/weekly_data_pack_YYYYMMDD.json
"""

import argparse
import json
import logging
import sys
from pathlib import Path
from datetime import date, datetime, timedelta
from typing import Dict, List, Optional, Any

PROJECT_ROOT = Path(__file__).parent.parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from scripts.brazil_soy_report.brazil_soy_config import (
    DATA_SOURCES,
    PROCESSED_DIR,
    OUTPUT_DIR,
    get_report_week_dates,
    REPORT_CONFIG,
)

try:
    import pandas as pd
    PANDAS_AVAILABLE = True
except ImportError:
    PANDAS_AVAILABLE = False

logging.basicConfig(level=logging.INFO, format='%(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


# =============================================================================
# DATA LOADING FROM PROCESSED FILES
# =============================================================================

def load_processed_data(source: str, report_date: date) -> Optional[pd.DataFrame]:
    """Load processed CSV for a source."""
    if not PANDAS_AVAILABLE:
        return None

    # Find matching file
    pattern = f"{source}_{report_date.strftime('%Y%m%d')}.csv"
    filepath = PROCESSED_DIR / pattern

    if not filepath.exists():
        # Try finding any recent file for this source
        files = list(PROCESSED_DIR.glob(f"{source}_*.csv"))
        if files:
            files.sort(key=lambda f: f.stat().st_mtime, reverse=True)
            filepath = files[0]
            logger.warning(f"Using older file: {filepath.name}")
        else:
            return None

    try:
        df = pd.read_csv(filepath)
        if 'date' in df.columns:
            df['date'] = pd.to_datetime(df['date'])
        return df
    except Exception as e:
        logger.error(f"Error loading {source}: {e}")
        return None


# =============================================================================
# WEEKLY CALCULATIONS
# =============================================================================

def calculate_weekly_change(
    df: pd.DataFrame,
    price_col: str,
    start_date: date,
    end_date: date
) -> Dict[str, Any]:
    """
    Calculate Wed-to-Wed price change.

    Returns dict with start_value, end_value, change, change_pct
    """
    if df is None or 'date' not in df.columns:
        return {'status': 'no_data'}

    # Filter to date range
    df = df.copy()
    df['date'] = pd.to_datetime(df['date']).dt.date

    # Get values closest to start and end Wednesday
    def get_closest_value(target_date: date, max_days: int = 3):
        """Get value closest to target date, within max_days tolerance."""
        for offset in range(max_days + 1):
            for direction in [0, -1, 1]:
                check_date = target_date + timedelta(days=offset * direction)
                match = df[df['date'] == check_date]
                if len(match) > 0:
                    return {
                        'value': float(match[price_col].iloc[-1]),
                        'date': str(check_date),
                        'offset_days': offset * direction
                    }
        return None

    start_data = get_closest_value(start_date)
    end_data = get_closest_value(end_date)

    if not start_data or not end_data:
        return {
            'status': 'incomplete',
            'start': start_data,
            'end': end_data,
            'note': 'Missing data for one or both Wednesdays'
        }

    change = end_data['value'] - start_data['value']
    change_pct = (change / start_data['value']) * 100 if start_data['value'] != 0 else 0

    return {
        'status': 'complete',
        'start_date': start_data['date'],
        'start_value': round(start_data['value'], 2),
        'start_offset_days': start_data['offset_days'],
        'end_date': end_data['date'],
        'end_value': round(end_data['value'], 2),
        'end_offset_days': end_data['offset_days'],
        'change': round(change, 2),
        'change_pct': round(change_pct, 2),
        'direction': 'up' if change > 0 else ('down' if change < 0 else 'unchanged'),
    }


def get_period_stats(
    df: pd.DataFrame,
    price_col: str,
    start_date: date,
    end_date: date
) -> Dict[str, Any]:
    """Get statistics for the reporting period."""
    if df is None or 'date' not in df.columns:
        return {'status': 'no_data'}

    df = df.copy()
    df['date'] = pd.to_datetime(df['date']).dt.date

    # Filter to period
    mask = (df['date'] >= start_date) & (df['date'] <= end_date)
    period_df = df[mask]

    if len(period_df) == 0:
        return {'status': 'no_data_in_period'}

    prices = period_df[price_col].dropna()

    return {
        'status': 'complete',
        'count': len(prices),
        'min': round(float(prices.min()), 2),
        'max': round(float(prices.max()), 2),
        'mean': round(float(prices.mean()), 2),
        'latest': round(float(prices.iloc[-1]), 2),
    }


# =============================================================================
# BUILD DATA PACK SECTIONS
# =============================================================================

def build_price_snapshot(week: Dict, processed_data: Dict) -> Dict[str, Any]:
    """
    Build price snapshot section.

    Shows current levels and Wed-to-Wed changes for:
    - CEPEA Paranagua (BRL)
    - IMEA MT (BRL)
    - CBOT Nearby (USD)
    """
    snapshot = {
        'section': 'price_snapshot',
        'report_week': week['week_label'],
        'prices': {}
    }

    # CEPEA Paranagua
    cepea_df = processed_data.get('cepea_paranagua')
    if cepea_df is not None:
        weekly = calculate_weekly_change(
            cepea_df, 'price_brl',
            week['start_wed'], week['end_wed']
        )
        stats = get_period_stats(
            cepea_df, 'price_brl',
            week['start_wed'], week['end_wed']
        )
        snapshot['prices']['cepea_paranagua'] = {
            'description': 'CEPEA Soy Paranagua Cash Price',
            'unit': 'BRL/60kg',
            'weekly_change': weekly,
            'period_stats': stats,
        }

    # IMEA MT
    imea_df = processed_data.get('imea_mt')
    if imea_df is not None:
        weekly = calculate_weekly_change(
            imea_df, 'price_brl',
            week['start_wed'], week['end_wed']
        )
        stats = get_period_stats(
            imea_df, 'price_brl',
            week['start_wed'], week['end_wed']
        )
        snapshot['prices']['imea_mt'] = {
            'description': 'IMEA Mato Grosso Soy Spot Price',
            'unit': 'BRL/sc (60kg)',
            'weekly_change': weekly,
            'period_stats': stats,
        }

    # CBOT Futures
    cbot_df = processed_data.get('cbot_futures')
    if cbot_df is not None:
        weekly = calculate_weekly_change(
            cbot_df, 'price_usc',
            week['start_wed'], week['end_wed']
        )
        stats = get_period_stats(
            cbot_df, 'price_usc',
            week['start_wed'], week['end_wed']
        )
        snapshot['prices']['cbot_soy'] = {
            'description': 'CBOT Soybean Futures (nearby)',
            'unit': 'USc/bu',
            'weekly_change': weekly,
            'period_stats': stats,
        }

    return snapshot


def build_fx_context(week: Dict, processed_data: Dict) -> Dict[str, Any]:
    """Build FX context section (USD/BRL)."""
    section = {
        'section': 'fx_context',
        'report_week': week['week_label'],
        'fx': None,
        'note': 'Monthly reference rate for context'
    }

    usdbrl_df = processed_data.get('cepea_usdbrl')
    if usdbrl_df is not None and len(usdbrl_df) > 0:
        # Get latest month's rate
        latest = usdbrl_df.iloc[-1]
        section['fx'] = {
            'rate': round(float(latest.get('price_brl', latest.get('value', 0))), 4),
            'period': str(latest.get('date', latest.get('month', 'latest'))),
            'source': 'CEPEA'
        }

    return section


def build_exports_ytd(week: Dict, processed_data: Dict) -> Dict[str, Any]:
    """Build exports YTD comparison section from ANEC data."""
    section = {
        'section': 'exports_ytd',
        'report_week': week['week_label'],
        'exports': None,
        'note': 'ANEC weekly shipment forecasts - manually maintained'
    }

    anec_df = processed_data.get('anec_exports')
    if anec_df is not None and len(anec_df) > 0:
        # Get latest week's data
        latest = anec_df.iloc[-1].to_dict()
        section['exports'] = {
            'week': str(latest.get('week', 'latest')),
            'soy_mmt': latest.get('soy_mmt'),
            'meal_mmt': latest.get('meal_mmt'),
            'ytd_2025': latest.get('ytd_2025'),
            'ytd_2026': latest.get('ytd_2026'),
            'source': 'ANEC'
        }

        # Calculate YoY change if both years present
        if latest.get('ytd_2025') and latest.get('ytd_2026'):
            ytd_25 = float(latest['ytd_2025'])
            ytd_26 = float(latest['ytd_2026'])
            if ytd_25 > 0:
                section['exports']['yoy_change_pct'] = round(
                    ((ytd_26 - ytd_25) / ytd_25) * 100, 1
                )

    return section


def build_weather_signal(week: Dict, processed_data: Dict) -> Dict[str, Any]:
    """Build weather signal section from NOAA data."""
    section = {
        'section': 'weather_signal',
        'report_week': week['week_label'],
        'signal': None,
        'note': 'NOAA 7-day precipitation outlook for Brazil'
    }

    noaa_df = processed_data.get('noaa_weather')
    if noaa_df is not None and len(noaa_df) > 0:
        latest = noaa_df.iloc[-1].to_dict()
        section['signal'] = {
            'week': str(latest.get('week', 'latest')),
            'condition': latest.get('signal', 'unknown'),
            'notes': latest.get('notes', ''),
            'source': 'NOAA'
        }

    return section


def build_llm_prompt(week: Dict, data_pack: Dict) -> str:
    """
    Build a structured prompt for the LLM to write the report narrative.

    The LLM should only use data from the data_pack - no hallucinations.
    """
    prompt = f"""You are writing the Brazil Soybean Weekly Report for {week['week_label']}.

IMPORTANT RULES:
1. Only use the data provided below. Do not make up numbers.
2. If data is missing, say "Data not available" - do not estimate.
3. Keep the tone professional but accessible.
4. Focus on week-over-week changes and what they mean for the market.

DATA PROVIDED:
"""

    # Add price snapshot
    if 'price_snapshot' in data_pack:
        prompt += "\n## PRICES\n"
        for name, data in data_pack['price_snapshot'].get('prices', {}).items():
            wc = data.get('weekly_change', {})
            if wc.get('status') == 'complete':
                prompt += f"- {data['description']}: {wc['end_value']} {data['unit']} "
                prompt += f"({wc['direction']} {abs(wc['change_pct']):.1f}% WoW)\n"
            else:
                prompt += f"- {data['description']}: [Data incomplete]\n"

    # Add FX
    if 'fx_context' in data_pack and data_pack['fx_context'].get('fx'):
        fx = data_pack['fx_context']['fx']
        prompt += f"\n## FX CONTEXT\n- USD/BRL: {fx['rate']} (monthly avg)\n"

    # Add exports
    if 'exports_ytd' in data_pack and data_pack['exports_ytd'].get('exports'):
        exp = data_pack['exports_ytd']['exports']
        prompt += f"\n## EXPORTS (ANEC)\n"
        if exp.get('soy_mmt'):
            prompt += f"- Weekly soy shipments: {exp['soy_mmt']} MMT\n"
        if exp.get('ytd_2026') and exp.get('ytd_2025'):
            prompt += f"- YTD 2026: {exp['ytd_2026']} MMT vs YTD 2025: {exp['ytd_2025']} MMT\n"
            if exp.get('yoy_change_pct'):
                prompt += f"- YoY change: {exp['yoy_change_pct']:+.1f}%\n"

    # Add weather
    if 'weather_signal' in data_pack and data_pack['weather_signal'].get('signal'):
        sig = data_pack['weather_signal']['signal']
        prompt += f"\n## WEATHER\n- 7-day outlook: {sig['condition']}\n"
        if sig.get('notes'):
            prompt += f"- Notes: {sig['notes']}\n"

    prompt += """
REQUESTED OUTPUT:
Write a 2-3 paragraph market summary covering:
1. Price action (led by CEPEA Paranagua, compare to CBOT)
2. Export pace and comparison to last year
3. Weather impact on crop conditions (if signal is notable)
4. Overall market sentiment (bullish/bearish/neutral)

Keep it under 300 words. End with a brief outlook sentence.
"""

    return prompt


# =============================================================================
# MAIN DATA PACK GENERATOR
# =============================================================================

def generate_data_pack(report_date: date = None) -> Dict[str, Any]:
    """
    Generate the complete weekly data pack.

    Returns a dict that can be serialized to JSON for the LLM.
    """
    week = get_report_week_dates(report_date)
    logger.info(f"Generating data pack for: {week['week_label']}")

    # Load all processed data
    processed_data = {}
    for source in DATA_SOURCES:
        df = load_processed_data(source, week['end_wed'])
        if df is not None:
            processed_data[source] = df
            logger.info(f"  Loaded {source}: {len(df)} rows")
        else:
            logger.warning(f"  Missing: {source}")

    # Build data pack
    data_pack = {
        'metadata': {
            'report_type': 'Brazil Soybean Weekly Report',
            'report_week': week['week_label'],
            'start_date': str(week['start_wed']),
            'end_date': str(week['end_wed']),
            'generated_at': datetime.now().isoformat(),
            'sources_loaded': list(processed_data.keys()),
            'sources_missing': [s for s in DATA_SOURCES if s not in processed_data],
        },
        'price_snapshot': build_price_snapshot(week, processed_data),
        'fx_context': build_fx_context(week, processed_data),
        'exports_ytd': build_exports_ytd(week, processed_data),
        'weather_signal': build_weather_signal(week, processed_data),
    }

    # Add LLM prompt
    data_pack['llm_prompt'] = build_llm_prompt(week, data_pack)

    # Save to output
    output_file = OUTPUT_DIR / f"weekly_data_pack_{week['end_wed'].strftime('%Y%m%d')}.json"
    with open(output_file, 'w') as f:
        json.dump(data_pack, f, indent=2, default=str)

    logger.info(f"Data pack saved to: {output_file}")

    # Also save just the prompt for easy access
    prompt_file = OUTPUT_DIR / f"llm_prompt_{week['end_wed'].strftime('%Y%m%d')}.txt"
    with open(prompt_file, 'w') as f:
        f.write(data_pack['llm_prompt'])

    logger.info(f"LLM prompt saved to: {prompt_file}")

    return data_pack


# =============================================================================
# CLI
# =============================================================================

def main():
    parser = argparse.ArgumentParser(
        description='Generate weekly data pack for Brazil Soybean Report'
    )
    parser.add_argument(
        '--date',
        type=str,
        help='Report date (YYYY-MM-DD). Default: latest Wednesday'
    )
    parser.add_argument(
        '--show-prompt',
        action='store_true',
        help='Print the LLM prompt to console'
    )

    args = parser.parse_args()

    report_date = None
    if args.date:
        report_date = datetime.strptime(args.date, "%Y-%m-%d").date()

    data_pack = generate_data_pack(report_date)

    # Print summary
    print("\n" + "=" * 60)
    print("WEEKLY DATA PACK SUMMARY")
    print("=" * 60)
    print(f"Report Week: {data_pack['metadata']['report_week']}")
    print(f"Sources Loaded: {', '.join(data_pack['metadata']['sources_loaded']) or 'None'}")
    print(f"Sources Missing: {', '.join(data_pack['metadata']['sources_missing']) or 'None'}")
    print()

    # Print price changes
    if data_pack['price_snapshot']['prices']:
        print("PRICE CHANGES (Wed-to-Wed):")
        for name, data in data_pack['price_snapshot']['prices'].items():
            wc = data.get('weekly_change', {})
            if wc.get('status') == 'complete':
                arrow = '↑' if wc['change'] > 0 else ('↓' if wc['change'] < 0 else '→')
                print(f"  {name}: {wc['end_value']} {data['unit']} "
                      f"({arrow} {wc['change']:+.2f}, {wc['change_pct']:+.1f}%)")
            else:
                print(f"  {name}: [Incomplete data]")
    else:
        print("PRICE CHANGES: No price data available")

    print()

    if args.show_prompt:
        print("=" * 60)
        print("LLM PROMPT:")
        print("=" * 60)
        print(data_pack['llm_prompt'])


if __name__ == '__main__':
    main()
