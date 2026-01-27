#!/usr/bin/env python3
"""
Brazil Soybean Weekly Report - Chart Generator

Creates PNG visualizations for the weekly report:
1. CEPEA Paranagua 7-day price chart
2. CBOT vs CEPEA comparison (dual-axis)
3. Exports YTD comparison bar chart

Usage:
    python brazil_soy_charts.py
    python brazil_soy_charts.py --date 2026-01-22
"""

import argparse
import logging
import sys
from pathlib import Path
from datetime import date, datetime, timedelta
from typing import Dict, Optional

PROJECT_ROOT = Path(__file__).parent.parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from scripts.brazil_soy_report.brazil_soy_config import (
    PROCESSED_DIR,
    OUTPUT_DIR,
    get_report_week_dates,
)

# Check for visualization libraries
try:
    import matplotlib
    matplotlib.use('Agg')  # Non-interactive backend for server
    import matplotlib.pyplot as plt
    import matplotlib.dates as mdates
    MATPLOTLIB_AVAILABLE = True
except ImportError:
    MATPLOTLIB_AVAILABLE = False
    print("WARNING: matplotlib not available. Install with: pip install matplotlib")

try:
    import pandas as pd
    PANDAS_AVAILABLE = True
except ImportError:
    PANDAS_AVAILABLE = False

logging.basicConfig(level=logging.INFO, format='%(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


# =============================================================================
# CHART STYLING
# =============================================================================

# RLC Brand colors (professional commodity report style)
COLORS = {
    'primary': '#1a5f7a',      # Dark teal
    'secondary': '#57c5b6',    # Light teal
    'accent': '#159895',       # Medium teal
    'warning': '#ffc93c',      # Yellow
    'danger': '#e74c3c',       # Red
    'success': '#27ae60',      # Green
    'neutral': '#95a5a6',      # Gray
    'background': '#ffffff',
    'grid': '#ecf0f1',
    'text': '#2c3e50',
}

def setup_style():
    """Configure matplotlib style for professional reports."""
    if not MATPLOTLIB_AVAILABLE:
        return

    plt.rcParams.update({
        'figure.facecolor': COLORS['background'],
        'axes.facecolor': COLORS['background'],
        'axes.edgecolor': COLORS['neutral'],
        'axes.labelcolor': COLORS['text'],
        'axes.titlecolor': COLORS['text'],
        'xtick.color': COLORS['text'],
        'ytick.color': COLORS['text'],
        'grid.color': COLORS['grid'],
        'grid.alpha': 0.7,
        'font.family': 'sans-serif',
        'font.size': 10,
        'axes.titlesize': 12,
        'axes.labelsize': 10,
        'legend.fontsize': 9,
        'figure.titlesize': 14,
    })


# =============================================================================
# DATA LOADING
# =============================================================================

def load_data(source: str, report_date: date) -> Optional[pd.DataFrame]:
    """Load processed data file."""
    if not PANDAS_AVAILABLE:
        return None

    # Find matching file
    files = list(PROCESSED_DIR.glob(f"{source}_*.csv"))
    if not files:
        return None

    # Use most recent
    files.sort(key=lambda f: f.stat().st_mtime, reverse=True)
    filepath = files[0]

    try:
        df = pd.read_csv(filepath)
        if 'date' in df.columns:
            df['date'] = pd.to_datetime(df['date'])
        return df
    except Exception as e:
        logger.error(f"Error loading {source}: {e}")
        return None


# =============================================================================
# CHART GENERATORS
# =============================================================================

def create_cepea_chart(week: Dict, output_dir: Path) -> Optional[Path]:
    """
    Create 7-day CEPEA Paranagua price chart.

    Shows daily prices with the Wed-to-Wed change highlighted.
    """
    if not MATPLOTLIB_AVAILABLE or not PANDAS_AVAILABLE:
        logger.error("matplotlib and pandas required for charts")
        return None

    df = load_data('cepea_paranagua', week['end_wed'])
    if df is None or len(df) == 0:
        logger.warning("No CEPEA data available for chart")
        return None

    # Filter to report week (with 2 extra days for context)
    start = week['start_wed'] - timedelta(days=2)
    end = week['end_wed'] + timedelta(days=1)
    df = df[(df['date'].dt.date >= start) & (df['date'].dt.date <= end)]

    if len(df) < 2:
        logger.warning("Insufficient CEPEA data for chart")
        return None

    setup_style()
    fig, ax = plt.subplots(figsize=(10, 5))

    # Plot price line
    ax.plot(df['date'], df['price_brl'],
            color=COLORS['primary'], linewidth=2, marker='o', markersize=4)

    # Highlight Wednesdays
    wed_dates = [week['start_wed'], week['end_wed']]
    for wed in wed_dates:
        ax.axvline(pd.Timestamp(wed), color=COLORS['accent'],
                   linestyle='--', alpha=0.5, linewidth=1)

    # Add Wed-to-Wed annotation
    start_val = df[df['date'].dt.date == week['start_wed']]['price_brl']
    end_val = df[df['date'].dt.date == week['end_wed']]['price_brl']

    if len(start_val) > 0 and len(end_val) > 0:
        start_v = start_val.iloc[0]
        end_v = end_val.iloc[0]
        change = end_v - start_v
        change_pct = (change / start_v) * 100

        # Annotation box
        color = COLORS['success'] if change >= 0 else COLORS['danger']
        arrow = '↑' if change > 0 else ('↓' if change < 0 else '→')

        ax.annotate(
            f'{arrow} {change:+.2f} BRL ({change_pct:+.1f}%)',
            xy=(df['date'].iloc[-1], end_v),
            xytext=(10, 10), textcoords='offset points',
            fontsize=11, fontweight='bold', color=color,
            bbox=dict(boxstyle='round,pad=0.3', facecolor='white', edgecolor=color, alpha=0.9)
        )

    # Formatting
    ax.set_title(f'CEPEA Soy Paranagua Cash Price\n{week["week_label"]}',
                 fontweight='bold', pad=10)
    ax.set_ylabel('Price (BRL/60kg)')
    ax.set_xlabel('')

    ax.xaxis.set_major_formatter(mdates.DateFormatter('%b %d'))
    ax.xaxis.set_major_locator(mdates.DayLocator())
    plt.xticks(rotation=45, ha='right')

    ax.grid(True, alpha=0.3)
    ax.set_ylim(bottom=df['price_brl'].min() * 0.98, top=df['price_brl'].max() * 1.02)

    # Add source note
    fig.text(0.99, 0.01, 'Source: CEPEA/ESALQ-USP',
             fontsize=8, color=COLORS['neutral'], ha='right', style='italic')

    plt.tight_layout()

    # Save
    output_path = output_dir / f"cepea_paranagua_{week['end_wed'].strftime('%Y%m%d')}.png"
    plt.savefig(output_path, dpi=150, bbox_inches='tight', facecolor='white')
    plt.close()

    logger.info(f"Saved: {output_path}")
    return output_path


def create_cbot_cepea_comparison(week: Dict, output_dir: Path) -> Optional[Path]:
    """
    Create dual-axis chart comparing CBOT (USD) and CEPEA (BRL) prices.

    Helps visualize basis and convergence/divergence.
    """
    if not MATPLOTLIB_AVAILABLE or not PANDAS_AVAILABLE:
        return None

    cepea_df = load_data('cepea_paranagua', week['end_wed'])
    cbot_df = load_data('cbot_futures', week['end_wed'])

    if cepea_df is None or cbot_df is None:
        logger.warning("Insufficient data for CBOT/CEPEA comparison")
        return None

    # Filter to week
    start = week['start_wed'] - timedelta(days=2)
    end = week['end_wed'] + timedelta(days=1)

    cepea_df = cepea_df[(cepea_df['date'].dt.date >= start) & (cepea_df['date'].dt.date <= end)]
    cbot_df = cbot_df[(cbot_df['date'].dt.date >= start) & (cbot_df['date'].dt.date <= end)]

    if len(cepea_df) < 2 or len(cbot_df) < 2:
        logger.warning("Insufficient data for comparison chart")
        return None

    setup_style()
    fig, ax1 = plt.subplots(figsize=(10, 5))

    # CEPEA on left axis (BRL)
    color_cepea = COLORS['primary']
    ax1.set_xlabel('')
    ax1.set_ylabel('CEPEA (BRL/60kg)', color=color_cepea)
    line1 = ax1.plot(cepea_df['date'], cepea_df['price_brl'],
                     color=color_cepea, linewidth=2, marker='o', markersize=3,
                     label='CEPEA Paranagua')
    ax1.tick_params(axis='y', labelcolor=color_cepea)

    # CBOT on right axis (USc)
    ax2 = ax1.twinx()
    color_cbot = COLORS['accent']
    ax2.set_ylabel('CBOT (USc/bu)', color=color_cbot)
    line2 = ax2.plot(cbot_df['date'], cbot_df['price_usc'],
                     color=color_cbot, linewidth=2, marker='s', markersize=3,
                     linestyle='--', label='CBOT Soy')
    ax2.tick_params(axis='y', labelcolor=color_cbot)

    # Title and legend
    ax1.set_title(f'CBOT vs CEPEA Soybean Prices\n{week["week_label"]}',
                  fontweight='bold', pad=10)

    # Combine legends
    lines = line1 + line2
    labels = [l.get_label() for l in lines]
    ax1.legend(lines, labels, loc='upper left', framealpha=0.9)

    # Format x-axis
    ax1.xaxis.set_major_formatter(mdates.DateFormatter('%b %d'))
    ax1.xaxis.set_major_locator(mdates.DayLocator())
    plt.xticks(rotation=45, ha='right')

    ax1.grid(True, alpha=0.3)

    # Source note
    fig.text(0.99, 0.01, 'Sources: CEPEA, Barchart',
             fontsize=8, color=COLORS['neutral'], ha='right', style='italic')

    plt.tight_layout()

    output_path = output_dir / f"cbot_vs_cepea_{week['end_wed'].strftime('%Y%m%d')}.png"
    plt.savefig(output_path, dpi=150, bbox_inches='tight', facecolor='white')
    plt.close()

    logger.info(f"Saved: {output_path}")
    return output_path


def create_exports_ytd_chart(week: Dict, output_dir: Path) -> Optional[Path]:
    """
    Create YTD exports comparison bar chart (2025 vs 2026).
    """
    if not MATPLOTLIB_AVAILABLE or not PANDAS_AVAILABLE:
        return None

    df = load_data('anec_exports', week['end_wed'])
    if df is None or len(df) == 0:
        logger.warning("No ANEC export data available")
        return None

    # Get latest row with YTD data
    latest = df.iloc[-1]

    ytd_2025 = latest.get('ytd_2025')
    ytd_2026 = latest.get('ytd_2026')

    if ytd_2025 is None or ytd_2026 is None:
        logger.warning("Missing YTD columns in ANEC data")
        return None

    setup_style()
    fig, ax = plt.subplots(figsize=(8, 5))

    # Bar chart
    categories = ['2025', '2026']
    values = [float(ytd_2025), float(ytd_2026)]
    colors = [COLORS['neutral'], COLORS['primary']]

    bars = ax.bar(categories, values, color=colors, width=0.6, edgecolor='white')

    # Add value labels on bars
    for bar, val in zip(bars, values):
        ax.text(bar.get_x() + bar.get_width()/2, bar.get_height() + 0.2,
                f'{val:.1f}', ha='center', va='bottom', fontweight='bold', fontsize=12)

    # YoY change annotation
    if ytd_2025 > 0:
        yoy = ((ytd_2026 - ytd_2025) / ytd_2025) * 100
        color = COLORS['success'] if yoy > 0 else COLORS['danger']
        ax.text(0.5, 0.95, f'YoY: {yoy:+.1f}%',
                transform=ax.transAxes, ha='center', va='top',
                fontsize=14, fontweight='bold', color=color,
                bbox=dict(boxstyle='round', facecolor='white', edgecolor=color, alpha=0.9))

    ax.set_title(f'Brazil Soy Exports YTD (MMT)\n{week["week_label"]}',
                 fontweight='bold', pad=10)
    ax.set_ylabel('Million Metric Tons')
    ax.set_ylim(0, max(values) * 1.2)

    ax.grid(True, axis='y', alpha=0.3)

    # Source
    fig.text(0.99, 0.01, 'Source: ANEC',
             fontsize=8, color=COLORS['neutral'], ha='right', style='italic')

    plt.tight_layout()

    output_path = output_dir / f"exports_ytd_{week['end_wed'].strftime('%Y%m%d')}.png"
    plt.savefig(output_path, dpi=150, bbox_inches='tight', facecolor='white')
    plt.close()

    logger.info(f"Saved: {output_path}")
    return output_path


# =============================================================================
# MAIN
# =============================================================================

def generate_all_charts(report_date: date = None) -> Dict[str, Optional[Path]]:
    """Generate all charts for the report."""
    week = get_report_week_dates(report_date)
    logger.info(f"Generating charts for: {week['week_label']}")

    charts = {}

    charts['cepea_paranagua'] = create_cepea_chart(week, OUTPUT_DIR)
    charts['cbot_vs_cepea'] = create_cbot_cepea_comparison(week, OUTPUT_DIR)
    charts['exports_ytd'] = create_exports_ytd_chart(week, OUTPUT_DIR)

    # Summary
    created = [k for k, v in charts.items() if v is not None]
    failed = [k for k, v in charts.items() if v is None]

    print(f"\nCharts created: {len(created)}")
    for name in created:
        print(f"  [OK] {name}")
    if failed:
        print(f"\nCharts failed: {len(failed)}")
        for name in failed:
            print(f"  [FAIL] {name}")

    return charts


def main():
    parser = argparse.ArgumentParser(
        description='Generate charts for Brazil Soybean Weekly Report'
    )
    parser.add_argument(
        '--date',
        type=str,
        help='Report date (YYYY-MM-DD). Default: latest Wednesday'
    )
    parser.add_argument(
        '--chart',
        choices=['cepea', 'comparison', 'exports', 'all'],
        default='all',
        help='Which chart to generate'
    )

    args = parser.parse_args()

    report_date = None
    if args.date:
        report_date = datetime.strptime(args.date, "%Y-%m-%d").date()

    if not MATPLOTLIB_AVAILABLE:
        print("ERROR: matplotlib is required. Install with:")
        print("  pip install matplotlib")
        sys.exit(1)

    if args.chart == 'all':
        generate_all_charts(report_date)
    else:
        week = get_report_week_dates(report_date)
        if args.chart == 'cepea':
            create_cepea_chart(week, OUTPUT_DIR)
        elif args.chart == 'comparison':
            create_cbot_cepea_comparison(week, OUTPUT_DIR)
        elif args.chart == 'exports':
            create_exports_ytd_chart(week, OUTPUT_DIR)


if __name__ == '__main__':
    main()
