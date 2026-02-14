#!/usr/bin/env python3
"""
Graphics Generator Agent

Generates custom visualizations from RLC data for reports and analysis.

Supported chart types:
- Time series (prices, production, stocks)
- Bar charts (comparisons, rankings)
- YoY comparison charts
- CFTC positioning charts
- Balance sheet tables
- Regional comparison maps

Output locations:
- data/generated_graphics/charts/
- data/generated_graphics/maps/
- data/generated_graphics/tables/
"""

import os
import json
import logging
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Any, Union
from dataclasses import dataclass, field

# Visualization libraries
try:
    import matplotlib.pyplot as plt
    import matplotlib.dates as mdates
    from matplotlib.figure import Figure
    import numpy as np
    MATPLOTLIB_AVAILABLE = True
except ImportError:
    MATPLOTLIB_AVAILABLE = False

try:
    import pandas as pd
    PANDAS_AVAILABLE = True
except ImportError:
    PANDAS_AVAILABLE = False

try:
    import psycopg2
    from psycopg2.extras import RealDictCursor
    DB_AVAILABLE = True
except ImportError:
    DB_AVAILABLE = False

# Setup paths
PROJECT_ROOT = Path(__file__).parent.parent.parent
DATA_DIR = PROJECT_ROOT / "data"
GRAPHICS_DIR = DATA_DIR / "generated_graphics"
CHARTS_DIR = GRAPHICS_DIR / "charts"
MAPS_DIR = GRAPHICS_DIR / "maps"
TABLES_DIR = GRAPHICS_DIR / "tables"

# Create directories
for d in [CHARTS_DIR, MAPS_DIR, TABLES_DIR]:
    d.mkdir(parents=True, exist_ok=True)

logger = logging.getLogger(__name__)


@dataclass
class ChartConfig:
    """Configuration for chart generation"""
    title: str
    chart_type: str  # 'line', 'bar', 'area', 'scatter', 'table'
    data_source: str  # SQL query or data key
    x_column: str = 'date'
    y_columns: List[str] = field(default_factory=list)
    y_labels: Dict[str, str] = field(default_factory=dict)
    colors: Dict[str, str] = field(default_factory=dict)
    figsize: tuple = (12, 6)
    grid: bool = True
    legend: bool = True
    x_label: str = ''
    y_label: str = ''
    style: str = 'seaborn-v0_8-whitegrid'
    add_average: bool = False
    average_years: int = 5
    output_format: str = 'png'
    dpi: int = 150


class GraphicsGeneratorAgent:
    """Agent for generating custom visualizations from data."""

    # RLC brand colors
    COLORS = {
        'primary': '#1f4e79',      # Navy blue
        'secondary': '#2e75b6',    # Light blue
        'accent': '#70ad47',       # Green
        'warning': '#ed7d31',      # Orange
        'neutral': '#7f7f7f',      # Gray
        'corn': '#ffc000',         # Corn yellow
        'soybeans': '#70ad47',     # Soybean green
        'wheat': '#c09c6b',        # Wheat tan
        'ethanol': '#2e75b6',      # Ethanol blue
        'positive': '#548235',     # Positive green
        'negative': '#c00000',     # Negative red
    }

    def __init__(self, db_config: Dict[str, Any] = None):
        """Initialize the graphics generator."""
        self.db_config = db_config or {
            'host': os.getenv('DB_HOST', 'localhost'),
            'port': int(os.getenv('DB_PORT', 5432)),
            'database': os.getenv('DB_NAME', 'rlc_commodities'),
            'user': os.getenv('DB_USER', 'postgres'),
            'password': os.getenv('DB_PASSWORD', 'SoupBoss1')
        }

        if not MATPLOTLIB_AVAILABLE:
            logger.warning("matplotlib not available - chart generation disabled")

        if not PANDAS_AVAILABLE:
            logger.warning("pandas not available - data processing limited")

    def _get_db_connection(self):
        """Get database connection."""
        if not DB_AVAILABLE:
            return None
        try:
            return psycopg2.connect(**self.db_config)
        except Exception as e:
            logger.error(f"Database connection failed: {e}")
            return None

    def _query_to_dataframe(self, sql: str, params: tuple = None) -> Optional[pd.DataFrame]:
        """Execute SQL query and return as DataFrame."""
        if not PANDAS_AVAILABLE:
            return None

        conn = self._get_db_connection()
        if not conn:
            return None

        try:
            df = pd.read_sql(sql, conn, params=params)
            return df
        except Exception as e:
            logger.error(f"Query failed: {e}")
            return None
        finally:
            conn.close()

    def _setup_style(self, config: ChartConfig):
        """Setup matplotlib style."""
        try:
            plt.style.use(config.style)
        except OSError:
            plt.style.use('default')

        # Set font sizes
        plt.rcParams['font.size'] = 10
        plt.rcParams['axes.titlesize'] = 14
        plt.rcParams['axes.labelsize'] = 12
        plt.rcParams['legend.fontsize'] = 10
        plt.rcParams['xtick.labelsize'] = 10
        plt.rcParams['ytick.labelsize'] = 10

    def generate_time_series(
        self,
        config: ChartConfig,
        df: pd.DataFrame = None
    ) -> Optional[str]:
        """
        Generate a time series line chart.

        Args:
            config: Chart configuration
            df: DataFrame with data (optional, will query if not provided)

        Returns:
            Path to saved chart or None
        """
        if not MATPLOTLIB_AVAILABLE:
            return None

        # Get data
        if df is None and config.data_source:
            df = self._query_to_dataframe(config.data_source)

        if df is None or df.empty:
            logger.warning(f"No data for chart: {config.title}")
            return None

        self._setup_style(config)

        fig, ax = plt.subplots(figsize=config.figsize)

        # Ensure x column is datetime
        if config.x_column in df.columns:
            df[config.x_column] = pd.to_datetime(df[config.x_column])
            df = df.sort_values(config.x_column)

        # Plot each y column
        for y_col in config.y_columns:
            if y_col in df.columns:
                label = config.y_labels.get(y_col, y_col)
                color = config.colors.get(y_col, self.COLORS['primary'])
                ax.plot(
                    df[config.x_column],
                    df[y_col],
                    label=label,
                    color=color,
                    linewidth=2
                )

        # Formatting
        ax.set_title(config.title, fontweight='bold', pad=15)
        ax.set_xlabel(config.x_label)
        ax.set_ylabel(config.y_label)

        if config.grid:
            ax.grid(True, alpha=0.3)

        if config.legend and len(config.y_columns) > 1:
            ax.legend(loc='best')

        # Format x-axis dates
        ax.xaxis.set_major_formatter(mdates.DateFormatter('%b %Y'))
        ax.xaxis.set_major_locator(mdates.AutoDateLocator())
        fig.autofmt_xdate()

        # Add footer
        fig.text(
            0.99, 0.01,
            f'Generated: {datetime.now().strftime("%Y-%m-%d %H:%M")} | RLC Analytics',
            ha='right', va='bottom', fontsize=8, color='gray'
        )

        plt.tight_layout()

        # Save
        filename = f"{config.title.replace(' ', '_').lower()}_{datetime.now().strftime('%Y%m%d')}.{config.output_format}"
        filepath = CHARTS_DIR / filename

        fig.savefig(filepath, dpi=config.dpi, bbox_inches='tight')
        plt.close(fig)

        logger.info(f"Generated chart: {filepath}")
        return str(filepath)

    def generate_bar_chart(
        self,
        config: ChartConfig,
        df: pd.DataFrame = None
    ) -> Optional[str]:
        """
        Generate a bar chart.

        Args:
            config: Chart configuration
            df: DataFrame with data

        Returns:
            Path to saved chart or None
        """
        if not MATPLOTLIB_AVAILABLE:
            return None

        if df is None and config.data_source:
            df = self._query_to_dataframe(config.data_source)

        if df is None or df.empty:
            return None

        self._setup_style(config)

        fig, ax = plt.subplots(figsize=config.figsize)

        x = range(len(df))
        width = 0.8 / len(config.y_columns) if len(config.y_columns) > 1 else 0.6

        for i, y_col in enumerate(config.y_columns):
            if y_col in df.columns:
                offset = (i - len(config.y_columns) / 2 + 0.5) * width
                label = config.y_labels.get(y_col, y_col)
                color = config.colors.get(y_col, self.COLORS['primary'])

                bars = ax.bar(
                    [xi + offset for xi in x],
                    df[y_col],
                    width,
                    label=label,
                    color=color
                )

        ax.set_title(config.title, fontweight='bold', pad=15)
        ax.set_xlabel(config.x_label)
        ax.set_ylabel(config.y_label)
        ax.set_xticks(x)
        ax.set_xticklabels(df[config.x_column], rotation=45, ha='right')

        if config.grid:
            ax.grid(True, alpha=0.3, axis='y')

        if config.legend and len(config.y_columns) > 1:
            ax.legend(loc='best')

        fig.text(
            0.99, 0.01,
            f'Generated: {datetime.now().strftime("%Y-%m-%d %H:%M")} | RLC Analytics',
            ha='right', va='bottom', fontsize=8, color='gray'
        )

        plt.tight_layout()

        filename = f"{config.title.replace(' ', '_').lower()}_{datetime.now().strftime('%Y%m%d')}.{config.output_format}"
        filepath = CHARTS_DIR / filename

        fig.savefig(filepath, dpi=config.dpi, bbox_inches='tight')
        plt.close(fig)

        logger.info(f"Generated chart: {filepath}")
        return str(filepath)

    def generate_yoy_comparison(
        self,
        title: str,
        sql: str,
        value_col: str,
        date_col: str = 'date',
        compare_years: int = 5
    ) -> Optional[str]:
        """
        Generate a year-over-year comparison chart.

        Args:
            title: Chart title
            sql: SQL query for data
            value_col: Column with values to compare
            date_col: Column with dates
            compare_years: Number of years to compare

        Returns:
            Path to saved chart
        """
        if not MATPLOTLIB_AVAILABLE or not PANDAS_AVAILABLE:
            return None

        df = self._query_to_dataframe(sql)
        if df is None or df.empty:
            return None

        df[date_col] = pd.to_datetime(df[date_col])
        df['year'] = df[date_col].dt.year
        df['day_of_year'] = df[date_col].dt.dayofyear

        current_year = df['year'].max()
        years_to_show = range(current_year - compare_years + 1, current_year + 1)

        self._setup_style(ChartConfig(title=title, chart_type='line', data_source=''))

        fig, ax = plt.subplots(figsize=(14, 7))

        colors = plt.cm.Blues(np.linspace(0.3, 1, compare_years))

        for i, year in enumerate(years_to_show):
            year_data = df[df['year'] == year].sort_values('day_of_year')
            if not year_data.empty:
                linewidth = 3 if year == current_year else 1.5
                alpha = 1 if year == current_year else 0.7
                ax.plot(
                    year_data['day_of_year'],
                    year_data[value_col],
                    label=str(year),
                    color=colors[i],
                    linewidth=linewidth,
                    alpha=alpha
                )

        ax.set_title(title, fontweight='bold', pad=15)
        ax.set_xlabel('Day of Year')
        ax.set_ylabel(value_col.replace('_', ' ').title())
        ax.legend(title='Year', loc='best')
        ax.grid(True, alpha=0.3)

        # Format x-axis as months
        month_starts = [1, 32, 60, 91, 121, 152, 182, 213, 244, 274, 305, 335]
        month_names = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun',
                       'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec']
        ax.set_xticks(month_starts)
        ax.set_xticklabels(month_names)

        fig.text(
            0.99, 0.01,
            f'Generated: {datetime.now().strftime("%Y-%m-%d %H:%M")} | RLC Analytics',
            ha='right', va='bottom', fontsize=8, color='gray'
        )

        plt.tight_layout()

        filename = f"yoy_{title.replace(' ', '_').lower()}_{datetime.now().strftime('%Y%m%d')}.png"
        filepath = CHARTS_DIR / filename

        fig.savefig(filepath, dpi=150, bbox_inches='tight')
        plt.close(fig)

        return str(filepath)

    def generate_cftc_positioning(self) -> Optional[str]:
        """
        Generate CFTC managed money positioning chart.

        Returns:
            Path to saved chart
        """
        if not MATPLOTLIB_AVAILABLE:
            return None

        sql = """
            SELECT
                commodity,
                report_date,
                mm_net_position,
                mm_net_position_pct_oi
            FROM gold.cftc_latest_positions
            ORDER BY report_date DESC
        """

        df = self._query_to_dataframe(sql)
        if df is None or df.empty:
            return None

        self._setup_style(ChartConfig(title='', chart_type='bar', data_source=''))

        fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(14, 6))

        # Net positions bar chart
        colors = [self.COLORS['positive'] if x > 0 else self.COLORS['negative']
                  for x in df['mm_net_position']]

        ax1.barh(df['commodity'], df['mm_net_position'], color=colors)
        ax1.set_title('Managed Money Net Positions', fontweight='bold')
        ax1.set_xlabel('Contracts')
        ax1.axvline(x=0, color='black', linewidth=0.5)
        ax1.grid(True, alpha=0.3, axis='x')

        # Percent of OI
        colors2 = [self.COLORS['positive'] if x > 0 else self.COLORS['negative']
                   for x in df['mm_net_position_pct_oi']]

        ax2.barh(df['commodity'], df['mm_net_position_pct_oi'], color=colors2)
        ax2.set_title('Net Position % of Open Interest', fontweight='bold')
        ax2.set_xlabel('Percent')
        ax2.axvline(x=0, color='black', linewidth=0.5)
        ax2.grid(True, alpha=0.3, axis='x')

        # Report date in title
        report_date = df['report_date'].iloc[0] if not df.empty else 'Unknown'
        fig.suptitle(f'CFTC Commitments of Traders - {report_date}',
                     fontsize=14, fontweight='bold', y=1.02)

        fig.text(
            0.99, 0.01,
            f'Generated: {datetime.now().strftime("%Y-%m-%d %H:%M")} | RLC Analytics',
            ha='right', va='bottom', fontsize=8, color='gray'
        )

        plt.tight_layout()

        filename = f"cftc_positioning_{datetime.now().strftime('%Y%m%d')}.png"
        filepath = CHARTS_DIR / filename

        fig.savefig(filepath, dpi=150, bbox_inches='tight')
        plt.close(fig)

        return str(filepath)

    def generate_ethanol_dashboard(self) -> Optional[str]:
        """
        Generate EIA ethanol production/stocks dashboard.

        Returns:
            Path to saved chart
        """
        if not MATPLOTLIB_AVAILABLE:
            return None

        sql = """
            SELECT
                week_ending,
                production_mbpd,
                stocks_mb,
                implied_corn_grind_mbu
            FROM gold.eia_ethanol_weekly
            ORDER BY week_ending DESC
            LIMIT 52
        """

        df = self._query_to_dataframe(sql)
        if df is None or df.empty:
            return None

        df = df.sort_values('week_ending')

        self._setup_style(ChartConfig(title='', chart_type='line', data_source=''))

        fig, axes = plt.subplots(2, 2, figsize=(14, 10))

        # Production
        ax1 = axes[0, 0]
        ax1.plot(df['week_ending'], df['production_mbpd'],
                 color=self.COLORS['ethanol'], linewidth=2)
        ax1.set_title('Weekly Ethanol Production', fontweight='bold')
        ax1.set_ylabel('Thousand Barrels/Day')
        ax1.grid(True, alpha=0.3)

        # Stocks
        ax2 = axes[0, 1]
        ax2.fill_between(df['week_ending'], df['stocks_mb'],
                         color=self.COLORS['secondary'], alpha=0.5)
        ax2.plot(df['week_ending'], df['stocks_mb'],
                 color=self.COLORS['primary'], linewidth=2)
        ax2.set_title('Ethanol Stocks', fontweight='bold')
        ax2.set_ylabel('Thousand Barrels')
        ax2.grid(True, alpha=0.3)

        # Implied corn grind
        ax3 = axes[1, 0]
        ax3.bar(df['week_ending'], df['implied_corn_grind_mbu'],
                color=self.COLORS['corn'], alpha=0.8)
        ax3.set_title('Implied Weekly Corn Grind', fontweight='bold')
        ax3.set_ylabel('Million Bushels')
        ax3.grid(True, alpha=0.3, axis='y')

        # Summary stats
        ax4 = axes[1, 1]
        ax4.axis('off')

        latest = df.iloc[-1]
        prev = df.iloc[-2] if len(df) > 1 else latest

        stats_text = f"""
        LATEST DATA ({latest['week_ending'].strftime('%Y-%m-%d')})

        Production:     {latest['production_mbpd']:.1f} mb/d
        Week/Week:      {latest['production_mbpd'] - prev['production_mbpd']:+.1f} mb/d

        Stocks:         {latest['stocks_mb']:.0f} mb
        Week/Week:      {latest['stocks_mb'] - prev['stocks_mb']:+.0f} mb

        Corn Grind:     {latest['implied_corn_grind_mbu']:.1f} mbu/week
        Week/Week:      {latest['implied_corn_grind_mbu'] - prev['implied_corn_grind_mbu']:+.1f} mbu
        """

        ax4.text(0.1, 0.9, stats_text, transform=ax4.transAxes,
                 fontsize=12, verticalalignment='top', fontfamily='monospace',
                 bbox=dict(boxstyle='round', facecolor='white', alpha=0.8))

        fig.suptitle('EIA Weekly Ethanol Report', fontsize=16, fontweight='bold', y=1.02)

        for ax in [ax1, ax2, ax3]:
            ax.xaxis.set_major_formatter(mdates.DateFormatter('%b'))
            ax.xaxis.set_major_locator(mdates.MonthLocator())

        fig.text(
            0.99, 0.01,
            f'Generated: {datetime.now().strftime("%Y-%m-%d %H:%M")} | RLC Analytics',
            ha='right', va='bottom', fontsize=8, color='gray'
        )

        plt.tight_layout()

        filename = f"ethanol_dashboard_{datetime.now().strftime('%Y%m%d')}.png"
        filepath = CHARTS_DIR / filename

        fig.savefig(filepath, dpi=150, bbox_inches='tight')
        plt.close(fig)

        return str(filepath)

    def generate_crop_condition_chart(self, commodity: str = 'corn') -> Optional[str]:
        """
        Generate crop condition comparison chart.

        Args:
            commodity: Commodity to chart

        Returns:
            Path to saved chart
        """
        if not MATPLOTLIB_AVAILABLE:
            return None

        sql = """
            SELECT
                week_ending,
                good_excellent_pct,
                poor_very_poor_pct,
                condition_index
            FROM gold.nass_condition_yoy
            WHERE commodity = %s
            ORDER BY week_ending
        """

        df = self._query_to_dataframe(sql, (commodity,))
        if df is None or df.empty:
            return None

        self._setup_style(ChartConfig(title='', chart_type='line', data_source=''))

        fig, ax = plt.subplots(figsize=(12, 6))

        ax.fill_between(df['week_ending'], df['good_excellent_pct'],
                        color=self.COLORS['positive'], alpha=0.3, label='Good/Excellent')
        ax.fill_between(df['week_ending'], df['poor_very_poor_pct'],
                        color=self.COLORS['negative'], alpha=0.3, label='Poor/V.Poor')

        ax.plot(df['week_ending'], df['good_excellent_pct'],
                color=self.COLORS['positive'], linewidth=2)
        ax.plot(df['week_ending'], df['poor_very_poor_pct'],
                color=self.COLORS['negative'], linewidth=2)

        ax.set_title(f'{commodity.title()} Crop Condition', fontweight='bold', pad=15)
        ax.set_ylabel('Percent of Crop')
        ax.legend(loc='best')
        ax.grid(True, alpha=0.3)
        ax.set_ylim(0, 100)

        ax.xaxis.set_major_formatter(mdates.DateFormatter('%b %d'))
        fig.autofmt_xdate()

        fig.text(
            0.99, 0.01,
            f'Generated: {datetime.now().strftime("%Y-%m-%d %H:%M")} | RLC Analytics',
            ha='right', va='bottom', fontsize=8, color='gray'
        )

        plt.tight_layout()

        filename = f"{commodity}_condition_{datetime.now().strftime('%Y%m%d')}.png"
        filepath = CHARTS_DIR / filename

        fig.savefig(filepath, dpi=150, bbox_inches='tight')
        plt.close(fig)

        return str(filepath)

    def list_available_graphics(self) -> Dict[str, List[str]]:
        """List all available generated graphics."""
        result = {
            'charts': [],
            'maps': [],
            'tables': []
        }

        for name, directory in [('charts', CHARTS_DIR), ('maps', MAPS_DIR), ('tables', TABLES_DIR)]:
            if directory.exists():
                result[name] = [f.name for f in directory.glob('*.png')]

        return result


# =============================================================================
# CLI INTERFACE
# =============================================================================

def main():
    """Command-line interface for graphics generator."""
    import argparse

    logging.basicConfig(level=logging.INFO)

    parser = argparse.ArgumentParser(description='RLC Graphics Generator')

    parser.add_argument(
        'command',
        choices=['ethanol', 'cftc', 'condition', 'list', 'test'],
        help='Command to execute'
    )

    parser.add_argument(
        '--commodity',
        default='corn',
        help='Commodity for condition chart'
    )

    args = parser.parse_args()

    generator = GraphicsGeneratorAgent()

    if args.command == 'test':
        print("Testing graphics generator...")
        print(f"matplotlib available: {MATPLOTLIB_AVAILABLE}")
        print(f"pandas available: {PANDAS_AVAILABLE}")
        print(f"Database available: {DB_AVAILABLE}")
        print(f"Charts directory: {CHARTS_DIR}")
        return

    if args.command == 'list':
        graphics = generator.list_available_graphics()
        print("\nAvailable Graphics:")
        for category, files in graphics.items():
            print(f"\n{category.upper()}:")
            for f in files[:10]:
                print(f"  - {f}")
            if len(files) > 10:
                print(f"  ... and {len(files) - 10} more")
        return

    if args.command == 'ethanol':
        path = generator.generate_ethanol_dashboard()
        print(f"Generated: {path}" if path else "Failed to generate")
        return

    if args.command == 'cftc':
        path = generator.generate_cftc_positioning()
        print(f"Generated: {path}" if path else "Failed to generate")
        return

    if args.command == 'condition':
        path = generator.generate_crop_condition_chart(args.commodity)
        print(f"Generated: {path}" if path else "Failed to generate")
        return


if __name__ == '__main__':
    main()
