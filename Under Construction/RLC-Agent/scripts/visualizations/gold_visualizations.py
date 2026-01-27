#!/usr/bin/env python3
"""
Gold Layer Visualizations

Creates analytical views and visualizations from silver layer data.
Generates charts, dashboards, and summary reports for US Corn.

Usage:
    python scripts/visualizations/gold_visualizations.py --all
    python scripts/visualizations/gold_visualizations.py --chart prices
    python scripts/visualizations/gold_visualizations.py --report
"""

import argparse
import json
import logging
import sqlite3
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger('gold_visualizations')

PROJECT_ROOT = Path(__file__).parent.parent.parent
DB_PATH = PROJECT_ROOT / "data" / "rlc_commodities.db"
OUTPUT_DIR = PROJECT_ROOT / "output" / "visualizations"

# Try to import visualization libraries
try:
    import matplotlib.pyplot as plt
    import matplotlib.dates as mdates
    HAS_MATPLOTLIB = True
except ImportError:
    HAS_MATPLOTLIB = False
    logger.warning("matplotlib not available - chart generation disabled")


class GoldVisualization:
    """Creates gold layer visualizations and reports."""

    def __init__(self, conn: sqlite3.Connection):
        self.conn = conn
        self.conn.row_factory = sqlite3.Row
        OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    def create_all_views(self):
        """Create all gold layer SQL views."""
        logger.info("Creating gold layer views...")

        self._create_corn_price_summary_view()
        self._create_corn_balance_sheet_summary()
        self._create_price_seasonality_view()
        self._create_stocks_to_use_history()
        self._create_industrial_use_trends()

        logger.info("Gold views created")

    def _create_corn_price_summary_view(self):
        """Create a comprehensive corn price summary view."""
        self.conn.execute("DROP VIEW IF EXISTS gold_corn_price_summary")
        self.conn.execute("""
            CREATE VIEW gold_corn_price_summary AS
            SELECT
                marketing_year,
                -- Farm prices
                AVG(CASE WHEN price_type = 'FARM' AND is_annual_average = 1
                    THEN price_usd_bu END) AS farm_price_annual_avg,
                MIN(CASE WHEN price_type = 'FARM' AND is_annual_average = 0
                    THEN price_usd_bu END) AS farm_price_low,
                MAX(CASE WHEN price_type = 'FARM' AND is_annual_average = 0
                    THEN price_usd_bu END) AS farm_price_high,
                -- Cash prices (Central Illinois)
                AVG(CASE WHEN price_type = 'CASH' AND market_location = 'Central Illinois'
                    THEN price_usd_bu END) AS cash_price_cil_avg,
                -- Price range
                MAX(CASE WHEN price_type = 'FARM' THEN price_usd_bu END) -
                MIN(CASE WHEN price_type = 'FARM' THEN price_usd_bu END) AS price_range,
                -- Count of observations
                COUNT(CASE WHEN price_type = 'FARM' AND is_annual_average = 0
                    THEN 1 END) AS farm_price_obs,
                COUNT(CASE WHEN price_type = 'CASH' THEN 1 END) AS cash_price_obs
            FROM silver_price
            WHERE commodity_code = 'CORN'
            GROUP BY marketing_year
            ORDER BY marketing_year DESC
        """)
        self.conn.commit()

    def _create_corn_balance_sheet_summary(self):
        """Create corn balance sheet summary view."""
        self.conn.execute("DROP VIEW IF EXISTS gold_corn_balance_sheet")
        self.conn.execute("""
            CREATE VIEW gold_corn_balance_sheet AS
            SELECT
                marketing_year,
                -- Supply (million bushels)
                beginning_stocks,
                production,
                imports,
                total_supply,
                -- Demand
                food_seed_industrial AS fsi,
                feed_residual,
                exports,
                total_use,
                ending_stocks,
                -- Key ratios
                ROUND(stocks_to_use_ratio * 100, 1) AS stocks_use_pct,
                ROUND(export_share, 1) AS export_pct,
                ROUND(feed_share, 1) AS feed_pct,
                ROUND(fsi_share, 1) AS fsi_pct,
                -- YoY changes
                ROUND(production_yoy_change, 1) AS production_yoy_pct,
                ROUND(ending_stocks_yoy_change, 1) AS stocks_yoy_pct,
                -- Metric tons
                ROUND(production_mmt, 2) AS production_mmt,
                ROUND(exports_mmt, 2) AS exports_mmt,
                ROUND(ending_stocks_mmt, 2) AS ending_stocks_mmt
            FROM silver_balance_sheet
            WHERE commodity_code = 'CORN' AND location_code = 'US'
            ORDER BY marketing_year DESC
        """)
        self.conn.commit()

    def _create_price_seasonality_view(self):
        """Create view for analyzing price seasonality."""
        self.conn.execute("DROP VIEW IF EXISTS gold_corn_price_seasonality")
        self.conn.execute("""
            CREATE VIEW gold_corn_price_seasonality AS
            SELECT
                price_month,
                CASE price_month
                    WHEN 1 THEN 'Sep' WHEN 2 THEN 'Oct' WHEN 3 THEN 'Nov'
                    WHEN 4 THEN 'Dec' WHEN 5 THEN 'Jan' WHEN 6 THEN 'Feb'
                    WHEN 7 THEN 'Mar' WHEN 8 THEN 'Apr' WHEN 9 THEN 'May'
                    WHEN 10 THEN 'Jun' WHEN 11 THEN 'Jul' WHEN 12 THEN 'Aug'
                END AS month_name,
                -- Historical averages (all years)
                AVG(price_usd_bu) AS avg_price,
                MIN(price_usd_bu) AS min_price,
                MAX(price_usd_bu) AS max_price,
                -- Recent 5-year average
                AVG(CASE WHEN CAST(SUBSTR(marketing_year, 1, 4) AS INTEGER) >= 2019
                    THEN price_usd_bu END) AS recent_5yr_avg,
                -- Count
                COUNT(*) AS obs_count
            FROM silver_price
            WHERE commodity_code = 'CORN'
              AND price_type = 'FARM'
              AND price_month IS NOT NULL
              AND is_annual_average = 0
            GROUP BY price_month
            ORDER BY price_month
        """)
        self.conn.commit()

    def _create_stocks_to_use_history(self):
        """Create stocks-to-use ratio history view."""
        self.conn.execute("DROP VIEW IF EXISTS gold_corn_stocks_use_history")
        self.conn.execute("""
            CREATE VIEW gold_corn_stocks_use_history AS
            SELECT
                sb.marketing_year,
                ROUND(sb.stocks_to_use_ratio * 100, 1) AS stocks_use_pct,
                sb.ending_stocks,
                sb.total_use,
                -- Get corresponding price
                (SELECT AVG(price_usd_bu)
                 FROM silver_price sp
                 WHERE sp.commodity_code = 'CORN'
                   AND sp.marketing_year = sb.marketing_year
                   AND sp.price_type = 'FARM'
                   AND sp.is_annual_average = 1) AS avg_farm_price
            FROM silver_balance_sheet sb
            WHERE sb.commodity_code = 'CORN' AND sb.location_code = 'US'
            ORDER BY sb.marketing_year DESC
        """)
        self.conn.commit()

    def _create_industrial_use_trends(self):
        """Create industrial use trends view."""
        self.conn.execute("DROP VIEW IF EXISTS gold_corn_industrial_use")
        self.conn.execute("""
            CREATE VIEW gold_corn_industrial_use AS
            SELECT
                marketing_year,
                SUM(CASE WHEN use_category = 'FUEL_ALCOHOL' THEN quantity END) AS ethanol,
                SUM(CASE WHEN use_category = 'HFCS' THEN quantity END) AS hfcs,
                SUM(CASE WHEN use_category = 'GLUCOSE_DEXTROSE' THEN quantity END) AS glucose,
                SUM(CASE WHEN use_category = 'STARCH' THEN quantity END) AS starch,
                SUM(CASE WHEN use_category = 'BEVERAGE_ALCOHOL' THEN quantity END) AS beverages,
                SUM(CASE WHEN use_category = 'CEREALS_OTHER' THEN quantity END) AS cereals,
                SUM(CASE WHEN use_category = 'SEED' THEN quantity END) AS seed,
                SUM(CASE WHEN use_category = 'TOTAL_FSI' THEN quantity END) AS total_fsi
            FROM industrial_use
            WHERE commodity_code = 'CORN'
            GROUP BY marketing_year
            ORDER BY marketing_year DESC
        """)
        self.conn.commit()

    def generate_price_chart(self, years: int = 10) -> Optional[str]:
        """Generate corn price history chart."""
        if not HAS_MATPLOTLIB:
            logger.warning("Cannot generate chart - matplotlib not available")
            return None

        logger.info(f"Generating corn price chart (last {years} years)...")

        # Get price data
        cursor = self.conn.execute("""
            SELECT price_date, price_usd_bu, price_type, market_location
            FROM silver_price
            WHERE commodity_code = 'CORN'
              AND price_date IS NOT NULL
              AND is_annual_average = 0
              AND CAST(SUBSTR(price_date, 1, 4) AS INTEGER) >= ?
            ORDER BY price_date
        """, (datetime.now().year - years,))

        data = cursor.fetchall()
        if not data:
            logger.warning("No price data found")
            return None

        # Separate farm and cash prices
        farm_dates, farm_prices = [], []
        cash_dates, cash_prices = [], []

        for row in data:
            date = datetime.strptime(row['price_date'], '%Y-%m-%d')
            if row['price_type'] == 'FARM':
                farm_dates.append(date)
                farm_prices.append(row['price_usd_bu'])
            elif row['price_type'] == 'CASH' and row['market_location'] == 'Central Illinois':
                cash_dates.append(date)
                cash_prices.append(row['price_usd_bu'])

        # Create chart
        fig, ax = plt.subplots(figsize=(12, 6))

        if farm_dates:
            ax.plot(farm_dates, farm_prices, 'b-', label='Farm Price', linewidth=1.5)
        if cash_dates:
            ax.plot(cash_dates, cash_prices, 'g-', label='Cash (Central IL)', linewidth=1.5, alpha=0.8)

        ax.set_xlabel('Date')
        ax.set_ylabel('Price ($/bushel)')
        ax.set_title('US Corn Prices')
        ax.legend(loc='upper left')
        ax.grid(True, alpha=0.3)

        # Format x-axis
        ax.xaxis.set_major_formatter(mdates.DateFormatter('%Y'))
        ax.xaxis.set_major_locator(mdates.YearLocator())
        plt.xticks(rotation=45)

        # Save chart
        chart_path = OUTPUT_DIR / f"corn_prices_{datetime.now().strftime('%Y%m%d')}.png"
        plt.tight_layout()
        plt.savefig(chart_path, dpi=150)
        plt.close()

        logger.info(f"Chart saved: {chart_path}")
        return str(chart_path)

    def generate_balance_sheet_chart(self) -> Optional[str]:
        """Generate corn balance sheet components chart."""
        if not HAS_MATPLOTLIB:
            return None

        logger.info("Generating corn balance sheet chart...")

        cursor = self.conn.execute("""
            SELECT marketing_year, production, exports, feed_residual,
                   food_seed_industrial, ending_stocks
            FROM silver_balance_sheet
            WHERE commodity_code = 'CORN' AND location_code = 'US'
              AND CAST(SUBSTR(marketing_year, 1, 4) AS INTEGER) >= 2010
            ORDER BY marketing_year
        """)

        data = cursor.fetchall()
        if not data:
            return None

        years = [row['marketing_year'] for row in data]
        production = [row['production'] / 1000 for row in data]  # Convert to billion bushels
        exports = [row['exports'] / 1000 for row in data]
        feed = [row['feed_residual'] / 1000 for row in data]
        fsi = [row['food_seed_industrial'] / 1000 for row in data]
        stocks = [row['ending_stocks'] / 1000 for row in data]

        fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(12, 10))

        # Supply/Production chart
        ax1.bar(years, production, color='green', alpha=0.7, label='Production')
        ax1.plot(years, stocks, 'ro-', label='Ending Stocks', markersize=6)
        ax1.set_ylabel('Billion Bushels')
        ax1.set_title('US Corn Supply')
        ax1.legend(loc='upper left')
        ax1.grid(True, alpha=0.3)
        plt.setp(ax1.xaxis.get_majorticklabels(), rotation=45)

        # Demand breakdown (stacked bar)
        width = 0.8
        ax2.bar(years, feed, width, label='Feed & Residual', color='orange', alpha=0.7)
        ax2.bar(years, fsi, width, bottom=feed, label='Food/Seed/Industrial', color='purple', alpha=0.7)
        ax2.bar(years, exports, width, bottom=[f+i for f, i in zip(feed, fsi)],
                label='Exports', color='blue', alpha=0.7)
        ax2.set_xlabel('Marketing Year')
        ax2.set_ylabel('Billion Bushels')
        ax2.set_title('US Corn Demand Breakdown')
        ax2.legend(loc='upper left')
        ax2.grid(True, alpha=0.3)
        plt.setp(ax2.xaxis.get_majorticklabels(), rotation=45)

        chart_path = OUTPUT_DIR / f"corn_balance_sheet_{datetime.now().strftime('%Y%m%d')}.png"
        plt.tight_layout()
        plt.savefig(chart_path, dpi=150)
        plt.close()

        logger.info(f"Chart saved: {chart_path}")
        return str(chart_path)

    def generate_stocks_price_chart(self) -> Optional[str]:
        """Generate stocks-to-use vs price chart."""
        if not HAS_MATPLOTLIB:
            return None

        logger.info("Generating stocks-to-use vs price chart...")

        cursor = self.conn.execute("""
            SELECT marketing_year, stocks_use_pct, avg_farm_price
            FROM gold_corn_stocks_use_history
            WHERE stocks_use_pct IS NOT NULL AND avg_farm_price IS NOT NULL
            ORDER BY marketing_year
        """)

        data = cursor.fetchall()
        if not data:
            return None

        stocks_use = [row['stocks_use_pct'] for row in data]
        prices = [row['avg_farm_price'] for row in data]
        years = [row['marketing_year'] for row in data]

        fig, ax = plt.subplots(figsize=(10, 8))

        # Scatter plot with year labels
        scatter = ax.scatter(stocks_use, prices, c=range(len(stocks_use)),
                            cmap='viridis', s=100, alpha=0.7)

        # Add year labels to recent points
        for i, year in enumerate(years[-10:]):  # Last 10 years
            idx = len(years) - 10 + i
            ax.annotate(year, (stocks_use[idx], prices[idx]),
                       textcoords="offset points", xytext=(5, 5), fontsize=8)

        ax.set_xlabel('Stocks-to-Use Ratio (%)')
        ax.set_ylabel('Average Farm Price ($/bu)')
        ax.set_title('US Corn: Stocks-to-Use vs Price Relationship')
        ax.grid(True, alpha=0.3)

        # Add trend line
        import numpy as np
        z = np.polyfit(stocks_use, prices, 1)
        p = np.poly1d(z)
        x_line = np.linspace(min(stocks_use), max(stocks_use), 100)
        ax.plot(x_line, p(x_line), 'r--', alpha=0.5, label='Trend')
        ax.legend()

        chart_path = OUTPUT_DIR / f"corn_stocks_price_{datetime.now().strftime('%Y%m%d')}.png"
        plt.tight_layout()
        plt.savefig(chart_path, dpi=150)
        plt.close()

        logger.info(f"Chart saved: {chart_path}")
        return str(chart_path)

    def generate_report(self) -> str:
        """Generate a comprehensive corn market report."""
        logger.info("Generating corn market report...")

        report = []
        report.append("=" * 60)
        report.append("US CORN MARKET REPORT")
        report.append(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M')}")
        report.append("=" * 60)

        # Current marketing year balance sheet
        report.append("\n## BALANCE SHEET SUMMARY\n")
        cursor = self.conn.execute("""
            SELECT * FROM gold_corn_balance_sheet
            ORDER BY marketing_year DESC
            LIMIT 3
        """)
        for row in cursor.fetchall():
            report.append(f"### {row['marketing_year']}")
            report.append(f"  Production: {row['production']:,.0f} MB ({row['production_mmt']:.1f} MMT)")
            report.append(f"  Total Use:  {row['total_use']:,.0f} MB")
            report.append(f"  Exports:    {row['exports']:,.0f} MB ({row['export_pct']:.1f}%)")
            report.append(f"  Feed:       {row['feed_residual']:,.0f} MB ({row['feed_pct']:.1f}%)")
            report.append(f"  FSI:        {row['fsi']:,.0f} MB ({row['fsi_pct']:.1f}%)")
            report.append(f"  End Stocks: {row['ending_stocks']:,.0f} MB")
            report.append(f"  Stocks/Use: {row['stocks_use_pct']:.1f}%")
            report.append("")

        # Price summary
        report.append("\n## PRICE SUMMARY\n")
        cursor = self.conn.execute("""
            SELECT * FROM gold_corn_price_summary
            WHERE marketing_year >= '2020/21'
            ORDER BY marketing_year DESC
        """)
        for row in cursor.fetchall():
            report.append(f"### {row['marketing_year']}")
            if row['farm_price_annual_avg']:
                report.append(f"  Farm Price Avg: ${row['farm_price_annual_avg']:.2f}/bu")
            if row['farm_price_low'] and row['farm_price_high']:
                report.append(f"  Price Range:    ${row['farm_price_low']:.2f} - ${row['farm_price_high']:.2f}")
            if row['cash_price_cil_avg']:
                report.append(f"  Cash (CIL) Avg: ${row['cash_price_cil_avg']:.2f}/bu")
            report.append("")

        # Industrial use breakdown
        report.append("\n## INDUSTRIAL USE (Latest Year)\n")
        cursor = self.conn.execute("""
            SELECT * FROM gold_corn_industrial_use
            ORDER BY marketing_year DESC
            LIMIT 1
        """)
        row = cursor.fetchone()
        if row:
            report.append(f"Marketing Year: {row['marketing_year']}")
            report.append(f"  Ethanol:    {row['ethanol']:,.0f} MB")
            report.append(f"  HFCS:       {row['hfcs']:,.0f} MB")
            report.append(f"  Starch:     {row['starch']:,.0f} MB")
            report.append(f"  Glucose:    {row['glucose']:,.0f} MB")
            report.append(f"  Beverages:  {row['beverages']:,.0f} MB")
            report.append(f"  Total FSI:  {row['total_fsi']:,.0f} MB")

        # Seasonality
        report.append("\n## PRICE SEASONALITY (5-Year Average)\n")
        cursor = self.conn.execute("""
            SELECT month_name, recent_5yr_avg, min_price, max_price
            FROM gold_corn_price_seasonality
            ORDER BY price_month
        """)
        report.append("  Month    5yr Avg    Low      High")
        report.append("  " + "-" * 40)
        for row in cursor.fetchall():
            if row['recent_5yr_avg']:
                report.append(f"  {row['month_name']:8} ${row['recent_5yr_avg']:.2f}    "
                            f"${row['min_price']:.2f}   ${row['max_price']:.2f}")

        report_text = "\n".join(report)

        # Save report
        report_path = OUTPUT_DIR / f"corn_report_{datetime.now().strftime('%Y%m%d')}.txt"
        with open(report_path, 'w') as f:
            f.write(report_text)

        logger.info(f"Report saved: {report_path}")
        return report_text


def main():
    parser = argparse.ArgumentParser(description='Generate gold layer visualizations')
    parser.add_argument('--all', action='store_true', help='Generate all views, charts, and reports')
    parser.add_argument('--views', action='store_true', help='Create SQL views only')
    parser.add_argument('--charts', action='store_true', help='Generate charts')
    parser.add_argument('--report', action='store_true', help='Generate text report')
    args = parser.parse_args()

    conn = sqlite3.connect(str(DB_PATH))
    viz = GoldVisualization(conn)

    if args.all or args.views:
        viz.create_all_views()

    if args.all or args.charts:
        viz.generate_price_chart()
        viz.generate_balance_sheet_chart()
        viz.generate_stocks_price_chart()

    if args.all or args.report:
        report = viz.generate_report()
        print(report)

    if not any([args.all, args.views, args.charts, args.report]):
        # Default: run everything
        viz.create_all_views()
        viz.generate_price_chart()
        viz.generate_balance_sheet_chart()
        viz.generate_stocks_price_chart()
        report = viz.generate_report()
        print(report)

    conn.close()


if __name__ == '__main__':
    main()
