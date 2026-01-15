#!/usr/bin/env python3
"""
Wheat Gold Layer Visualizations

Creates analytical views and visualizations for wheat.
Generates charts, dashboards, and summary reports.

Usage:
    python scripts/visualizations/wheat_visualizations.py --all
    python scripts/visualizations/wheat_visualizations.py --report
"""

import argparse
import logging
import sqlite3
from datetime import datetime
from pathlib import Path

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger('wheat_viz')

PROJECT_ROOT = Path(__file__).parent.parent.parent
DB_PATH = PROJECT_ROOT / "data" / "rlc_commodities.db"
OUTPUT_DIR = PROJECT_ROOT / "output" / "visualizations"

try:
    import matplotlib.pyplot as plt
    HAS_MATPLOTLIB = True
except ImportError:
    HAS_MATPLOTLIB = False


class WheatVisualization:
    """Creates wheat gold layer visualizations."""

    def __init__(self, conn: sqlite3.Connection):
        self.conn = conn
        self.conn.row_factory = sqlite3.Row
        OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    def create_all_views(self):
        """Create all wheat gold layer SQL views."""
        logger.info("Creating wheat gold views...")

        self._create_wheat_balance_sheet_view()
        self._create_wheat_price_summary()
        self._create_world_wheat_production_view()
        self._create_wheat_export_destinations()
        self._create_wheat_class_production()

        logger.info("Wheat gold views created")

    def _create_wheat_balance_sheet_view(self):
        """Create US wheat balance sheet summary view."""
        self.conn.execute("DROP VIEW IF EXISTS gold_wheat_balance_sheet")
        self.conn.execute("""
            CREATE VIEW gold_wheat_balance_sheet AS
            SELECT
                marketing_year,
                beginning_stocks,
                production,
                imports,
                total_supply,
                food_use,
                seed_use,
                feed_residual,
                exports,
                total_use,
                ending_stocks,
                ROUND(production_mmt, 2) AS production_mmt,
                ROUND(exports_mmt, 2) AS exports_mmt,
                ROUND(ending_stocks_mmt, 2) AS ending_stocks_mmt,
                ROUND(stocks_to_use_ratio * 100, 1) AS stocks_use_pct,
                ROUND(food_use_pct, 1) AS food_use_pct,
                ROUND(export_pct, 1) AS export_pct,
                ROUND(feed_pct, 1) AS feed_pct,
                ROUND(production_yoy_pct, 1) AS production_yoy_pct,
                ROUND(ending_stocks_yoy_pct, 1) AS stocks_yoy_pct,
                ROUND(exports_yoy_pct, 1) AS exports_yoy_pct
            FROM silver_wheat_balance_sheet
            WHERE commodity_code = 'WHEAT' AND location_code = 'US'
            ORDER BY marketing_year DESC
        """)
        self.conn.commit()

    def _create_wheat_price_summary(self):
        """Create wheat price summary by class from production table."""
        self.conn.execute("DROP VIEW IF EXISTS gold_wheat_prices")
        self.conn.execute("""
            CREATE VIEW gold_wheat_prices AS
            SELECT
                marketing_year,
                MAX(CASE WHEN commodity_code = 'WHEAT' THEN farm_price_usd_bu END) AS all_wheat_price,
                MAX(CASE WHEN commodity_code = 'WHEAT_HRW' THEN farm_price_usd_bu END) AS hrw_price,
                MAX(CASE WHEN commodity_code = 'WHEAT_HRS' THEN farm_price_usd_bu END) AS hrs_price,
                MAX(CASE WHEN commodity_code = 'WHEAT_SRW' THEN farm_price_usd_bu END) AS srw_price,
                MAX(CASE WHEN commodity_code = 'WHEAT_WHITE' THEN farm_price_usd_bu END) AS white_price,
                MAX(CASE WHEN commodity_code = 'WHEAT_DURUM' THEN farm_price_usd_bu END) AS durum_price
            FROM silver_wheat_production
            WHERE location_code = 'US'
            GROUP BY marketing_year
            HAVING all_wheat_price IS NOT NULL OR hrw_price IS NOT NULL
            ORDER BY marketing_year DESC
        """)
        self.conn.commit()

    def _create_world_wheat_production_view(self):
        """Create world wheat production view."""
        self.conn.execute("DROP VIEW IF EXISTS gold_world_wheat_production")
        self.conn.execute("""
            CREATE VIEW gold_world_wheat_production AS
            SELECT
                marketing_year,
                location_code,
                ROUND(production_mmt, 2) AS production_mmt,
                ROUND(exports_mmt, 2) AS exports_mmt,
                ROUND(stocks_to_use_ratio * 100, 1) AS stocks_use_pct,
                ROUND(production_yoy_pct, 1) AS production_yoy_pct
            FROM silver_wheat_balance_sheet
            WHERE commodity_code = 'WHEAT'
              AND location_code IN ('US', 'WORLD', 'EU', 'RU', 'CA', 'AU', 'UA', 'AR', 'CN', 'IN')
              AND production IS NOT NULL
            ORDER BY marketing_year DESC, production DESC
        """)
        self.conn.commit()

    def _create_wheat_export_destinations(self):
        """Create top wheat export destinations view."""
        self.conn.execute("DROP VIEW IF EXISTS gold_wheat_exports_by_destination")
        self.conn.execute("""
            CREATE VIEW gold_wheat_exports_by_destination AS
            SELECT
                marketing_year,
                partner_code,
                partner_name,
                ROUND(quantity_mbu, 1) AS quantity_mbu,
                ROUND(quantity_mmt, 3) AS quantity_mmt,
                ROUND(market_share_pct, 1) AS market_share_pct,
                ROUND(yoy_change_pct, 1) AS yoy_change_pct
            FROM silver_wheat_trade
            WHERE commodity_code IN ('WHEAT', 'WHEAT_HRW', 'WHEAT_HRS', 'WHEAT_SRW')
              AND flow_direction = 'EXPORT'
              AND quantity_mbu > 0
            ORDER BY marketing_year DESC, quantity_mbu DESC
        """)
        self.conn.commit()

    def _create_wheat_class_production(self):
        """Create wheat production by class view."""
        self.conn.execute("DROP VIEW IF EXISTS gold_wheat_by_class")
        self.conn.execute("""
            CREATE VIEW gold_wheat_by_class AS
            SELECT
                marketing_year,
                commodity_code,
                ROUND(area_planted, 1) AS area_planted,
                ROUND(area_harvested, 1) AS area_harvested,
                ROUND(yield_bu_acre, 1) AS yield_bu_acre,
                ROUND(production_mbu, 1) AS production_mbu,
                ROUND(production_mmt, 3) AS production_mmt,
                ROUND(farm_price_usd_bu, 2) AS farm_price_bu,
                ROUND(harvest_ratio * 100, 1) AS harvest_pct,
                ROUND(production_yoy_pct, 1) AS production_yoy_pct,
                ROUND(yield_yoy_pct, 1) AS yield_yoy_pct
            FROM silver_wheat_production
            WHERE location_code = 'US'
              AND commodity_code IN ('WHEAT', 'WHEAT_HRW', 'WHEAT_HRS', 'WHEAT_SRW', 'WHEAT_WHITE', 'WHEAT_DURUM')
            ORDER BY marketing_year DESC, production_mbu DESC
        """)
        self.conn.commit()

    def generate_wheat_price_chart(self, years: int = 20) -> str:
        """Generate wheat price history chart by class."""
        if not HAS_MATPLOTLIB:
            return None

        logger.info("Generating wheat price chart...")

        cursor = self.conn.execute("""
            SELECT marketing_year, all_wheat_price, hrw_price, hrs_price, srw_price
            FROM gold_wheat_prices
            WHERE CAST(SUBSTR(marketing_year, 1, 4) AS INTEGER) >= ?
              AND all_wheat_price IS NOT NULL
            ORDER BY marketing_year
        """, (datetime.now().year - years,))

        data = cursor.fetchall()
        if not data:
            logger.warning("No wheat price data found")
            return None

        years_list = [row['marketing_year'] for row in data]
        all_wheat = [row['all_wheat_price'] for row in data]
        hrw = [row['hrw_price'] for row in data]
        hrs = [row['hrs_price'] for row in data]
        srw = [row['srw_price'] for row in data]

        fig, ax = plt.subplots(figsize=(14, 7))

        ax.plot(years_list, all_wheat, 'ko-', label='All Wheat', linewidth=2, markersize=6)
        ax.plot(years_list, hrw, 'rs--', label='HRW', alpha=0.7)
        ax.plot(years_list, hrs, 'b^--', label='HRS', alpha=0.7)
        ax.plot(years_list, srw, 'gv--', label='SRW', alpha=0.7)

        ax.set_xlabel('Marketing Year')
        ax.set_ylabel('$/bushel')
        ax.set_title('US Wheat Farm Prices by Class')
        ax.legend(loc='upper left')
        ax.tick_params(axis='x', rotation=45)
        ax.grid(True, alpha=0.3)

        chart_path = OUTPUT_DIR / f"wheat_prices_{datetime.now().strftime('%Y%m%d')}.png"
        plt.tight_layout()
        plt.savefig(chart_path, dpi=150)
        plt.close()

        logger.info(f"Chart saved: {chart_path}")
        return str(chart_path)

    def generate_balance_sheet_chart(self) -> str:
        """Generate wheat balance sheet chart."""
        if not HAS_MATPLOTLIB:
            return None

        logger.info("Generating wheat balance sheet chart...")

        cursor = self.conn.execute("""
            SELECT marketing_year, production, food_use, feed_residual,
                   exports, ending_stocks, stocks_use_pct
            FROM gold_wheat_balance_sheet
            WHERE CAST(SUBSTR(marketing_year, 1, 4) AS INTEGER) >= 2000
            ORDER BY marketing_year
        """)

        data = cursor.fetchall()
        if not data:
            logger.warning("No wheat balance sheet data found")
            return None

        years = [row['marketing_year'] for row in data]
        production = [row['production'] if row['production'] else 0 for row in data]
        food = [row['food_use'] if row['food_use'] else 0 for row in data]
        feed = [row['feed_residual'] if row['feed_residual'] else 0 for row in data]
        exports = [row['exports'] if row['exports'] else 0 for row in data]
        stocks = [row['ending_stocks'] if row['ending_stocks'] else 0 for row in data]
        stocks_use = [row['stocks_use_pct'] if row['stocks_use_pct'] else 0 for row in data]

        fig, axes = plt.subplots(2, 2, figsize=(14, 10))

        # Production vs Stocks
        ax1 = axes[0, 0]
        ax1.bar(years, production, color='goldenrod', alpha=0.7, label='Production')
        ax1.plot(years, stocks, 'ro-', label='Ending Stocks', markersize=4)
        ax1.set_ylabel('Million Bushels')
        ax1.set_title('US Wheat Production & Stocks')
        ax1.legend(loc='upper left')
        ax1.tick_params(axis='x', rotation=45)
        ax1.grid(True, alpha=0.3)

        # Demand breakdown
        ax2 = axes[0, 1]
        ax2.bar(years, food, label='Food', color='wheat', alpha=0.9)
        ax2.bar(years, feed, bottom=food, label='Feed & Residual', color='tan', alpha=0.7)
        bottom2 = [f + fd for f, fd in zip(food, feed)]
        ax2.bar(years, exports, bottom=bottom2, label='Exports', color='blue', alpha=0.6)
        ax2.set_ylabel('Million Bushels')
        ax2.set_title('US Wheat Use Breakdown')
        ax2.legend(loc='upper left')
        ax2.tick_params(axis='x', rotation=45)
        ax2.grid(True, alpha=0.3)

        # Stocks-to-Use ratio
        ax3 = axes[1, 0]
        ax3.bar(years, stocks_use, color='darkgoldenrod', alpha=0.7)
        ax3.axhline(y=30, color='red', linestyle='--', label='30% threshold')
        ax3.set_xlabel('Marketing Year')
        ax3.set_ylabel('Percent')
        ax3.set_title('US Wheat Stocks-to-Use Ratio')
        ax3.legend(loc='upper right')
        ax3.tick_params(axis='x', rotation=45)
        ax3.grid(True, alpha=0.3)

        # Use shares
        ax4 = axes[1, 1]
        food_share = []
        exp_share = []
        feed_share = []
        for row in data:
            total = (row['food_use'] or 0) + (row['feed_residual'] or 0) + (row['exports'] or 0)
            if total > 0:
                food_share.append((row['food_use'] or 0) / total * 100)
                exp_share.append((row['exports'] or 0) / total * 100)
                feed_share.append((row['feed_residual'] or 0) / total * 100)
            else:
                food_share.append(0)
                exp_share.append(0)
                feed_share.append(0)

        ax4.plot(years, food_share, 'g-o', label='Food %', markersize=4)
        ax4.plot(years, exp_share, 'b-s', label='Export %', markersize=4)
        ax4.plot(years, feed_share, 'r-^', label='Feed %', markersize=4)
        ax4.set_xlabel('Marketing Year')
        ax4.set_ylabel('Percent of Total Use')
        ax4.set_title('US Wheat Use Shares')
        ax4.legend(loc='right')
        ax4.tick_params(axis='x', rotation=45)
        ax4.grid(True, alpha=0.3)

        chart_path = OUTPUT_DIR / f"wheat_balance_sheet_{datetime.now().strftime('%Y%m%d')}.png"
        plt.tight_layout()
        plt.savefig(chart_path, dpi=150)
        plt.close()

        logger.info(f"Chart saved: {chart_path}")
        return str(chart_path)

    def generate_export_destinations_chart(self) -> str:
        """Generate wheat export destinations chart."""
        if not HAS_MATPLOTLIB:
            return None

        logger.info("Generating wheat export destinations chart...")

        # Get latest year's top destinations
        cursor = self.conn.execute("""
            SELECT partner_name, SUM(quantity_mbu) as total_qty
            FROM gold_wheat_exports_by_destination
            WHERE CAST(SUBSTR(marketing_year, 1, 4) AS INTEGER) >= 2018
            GROUP BY partner_code
            ORDER BY total_qty DESC
            LIMIT 10
        """)

        data = cursor.fetchall()
        if not data:
            logger.warning("No wheat export data found")
            return None

        partners = [row['partner_name'] or 'Other' for row in data]
        quantities = [row['total_qty'] for row in data]

        fig, ax = plt.subplots(figsize=(12, 6))

        colors = plt.cm.Blues([0.3 + i * 0.07 for i in range(len(partners))])
        ax.barh(partners[::-1], quantities[::-1], color=colors[::-1])
        ax.set_xlabel('Million Bushels (5-year total)')
        ax.set_title('Top US Wheat Export Destinations (2018-present)')
        ax.grid(True, alpha=0.3, axis='x')

        for i, (p, q) in enumerate(zip(partners[::-1], quantities[::-1])):
            ax.text(q + max(quantities) * 0.01, i, f'{q:.0f}', va='center', fontsize=9)

        chart_path = OUTPUT_DIR / f"wheat_export_destinations_{datetime.now().strftime('%Y%m%d')}.png"
        plt.tight_layout()
        plt.savefig(chart_path, dpi=150)
        plt.close()

        logger.info(f"Chart saved: {chart_path}")
        return str(chart_path)

    def generate_report(self) -> str:
        """Generate comprehensive wheat market report."""
        logger.info("Generating wheat market report...")

        report = []
        report.append("=" * 60)
        report.append("US WHEAT MARKET REPORT")
        report.append(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M')}")
        report.append("=" * 60)

        # Balance sheet summary
        report.append("\n## US WHEAT BALANCE SHEET (Million Bushels)\n")
        cursor = self.conn.execute("""
            SELECT * FROM gold_wheat_balance_sheet
            ORDER BY marketing_year DESC
            LIMIT 5
        """)
        rows = cursor.fetchall()
        if rows:
            report.append("  Year      Production    Exports    Food    End Stks  Stks/Use")
            report.append("  " + "-" * 65)
            for row in rows:
                prod = f"{row['production']:,.0f}" if row['production'] else "    -"
                exp = f"{row['exports']:,.0f}" if row['exports'] else "   -"
                food = f"{row['food_use']:,.0f}" if row['food_use'] else "   -"
                stks = f"{row['ending_stocks']:,.0f}" if row['ending_stocks'] else "   -"
                stks_use = f"{row['stocks_use_pct']:.1f}%" if row['stocks_use_pct'] else "  -"
                report.append(f"  {row['marketing_year']:8} {prod:>12} {exp:>10} {food:>8} {stks:>10} {stks_use:>8}")

        # Latest year details
        report.append("\n## LATEST YEAR DETAILS\n")
        if rows:
            row = rows[0]
            report.append(f"Marketing Year: {row['marketing_year']}")
            report.append(f"\nSUPPLY:")
            if row['beginning_stocks']:
                report.append(f"  Beginning Stocks: {row['beginning_stocks']:,.0f}")
            if row['production']:
                report.append(f"  Production:       {row['production']:,.0f} ({row['production_mmt']:.1f} MMT)")
            if row['imports']:
                report.append(f"  Imports:          {row['imports']:,.0f}")
            if row['total_supply']:
                report.append(f"  Total Supply:     {row['total_supply']:,.0f}")
            report.append(f"\nDEMAND:")
            if row['food_use']:
                report.append(f"  Food Use:         {row['food_use']:,.0f} ({row['food_use_pct']:.1f}%)")
            if row['seed_use']:
                report.append(f"  Seed Use:         {row['seed_use']:,.0f}")
            if row['feed_residual']:
                report.append(f"  Feed & Residual:  {row['feed_residual']:,.0f} ({row['feed_pct']:.1f}%)")
            if row['exports']:
                report.append(f"  Exports:          {row['exports']:,.0f} ({row['export_pct']:.1f}%)")
            if row['total_use']:
                report.append(f"  Total Use:        {row['total_use']:,.0f}")
            report.append(f"\nSTOCKS:")
            if row['ending_stocks']:
                report.append(f"  Ending Stocks:    {row['ending_stocks']:,.0f} ({row['ending_stocks_mmt']:.1f} MMT)")
            if row['stocks_use_pct']:
                report.append(f"  Stocks/Use:       {row['stocks_use_pct']:.1f}%")

        # Price summary by class
        report.append("\n## WHEAT PRICES BY CLASS ($/bushel)\n")
        cursor = self.conn.execute("""
            SELECT * FROM gold_wheat_prices
            ORDER BY marketing_year DESC
            LIMIT 5
        """)
        rows = cursor.fetchall()
        if rows:
            report.append("  Year       All    HRW    HRS    SRW   White  Durum")
            report.append("  " + "-" * 55)
            for row in rows:
                all_p = f"${row['all_wheat_price']:.2f}" if row['all_wheat_price'] else "  -  "
                hrw = f"${row['hrw_price']:.2f}" if row['hrw_price'] else "  -  "
                hrs = f"${row['hrs_price']:.2f}" if row['hrs_price'] else "  -  "
                srw = f"${row['srw_price']:.2f}" if row['srw_price'] else "  -  "
                white = f"${row['white_price']:.2f}" if row['white_price'] else "  -  "
                durum = f"${row['durum_price']:.2f}" if row['durum_price'] else "  -  "
                report.append(f"  {row['marketing_year']:8} {all_p:>6} {hrw:>6} {hrs:>6} {srw:>6} {white:>6} {durum:>6}")

        # Production by class
        report.append("\n## US WHEAT PRODUCTION BY CLASS (Latest Year)\n")
        cursor = self.conn.execute("""
            SELECT commodity_code, production_mbu, production_mmt, yield_bu_acre, farm_price_bu
            FROM gold_wheat_by_class
            WHERE CAST(SUBSTR(marketing_year, 1, 4) AS INTEGER) = (
                SELECT MAX(CAST(SUBSTR(marketing_year, 1, 4) AS INTEGER))
                FROM gold_wheat_by_class
                WHERE production_mbu IS NOT NULL
            )
            ORDER BY production_mbu DESC
        """)
        rows = cursor.fetchall()
        if rows:
            report.append("  Class           Prod (mbu)     MMT    Yield   Price")
            report.append("  " + "-" * 55)
            for row in rows:
                cls = row['commodity_code'].replace('WHEAT_', '').replace('WHEAT', 'ALL')
                prod = f"{row['production_mbu']:,.0f}" if row['production_mbu'] else "   -"
                mmt = f"{row['production_mmt']:.2f}" if row['production_mmt'] else " -"
                yld = f"{row['yield_bu_acre']:.1f}" if row['yield_bu_acre'] else " -"
                price = f"${row['farm_price_bu']:.2f}" if row['farm_price_bu'] else "  -"
                report.append(f"  {cls:14} {prod:>12} {mmt:>7} {yld:>7} {price:>7}")

        # Top export destinations
        report.append("\n## TOP WHEAT EXPORT DESTINATIONS (Latest Year)\n")
        cursor = self.conn.execute("""
            SELECT partner_name, SUM(quantity_mbu) as qty, AVG(market_share_pct) as share
            FROM gold_wheat_exports_by_destination
            WHERE CAST(SUBSTR(marketing_year, 1, 4) AS INTEGER) = (
                SELECT MAX(CAST(SUBSTR(marketing_year, 1, 4) AS INTEGER))
                FROM gold_wheat_exports_by_destination
            )
            GROUP BY partner_code
            ORDER BY qty DESC
            LIMIT 10
        """)
        rows = cursor.fetchall()
        if rows:
            report.append("  Destination          Quantity (mbu)   Share")
            report.append("  " + "-" * 45)
            for row in rows:
                name = row['partner_name'] or 'Unknown'
                qty = f"{row['qty']:,.0f}" if row['qty'] else "   -"
                share = f"{row['share']:.1f}%" if row['share'] else " -"
                report.append(f"  {name:20} {qty:>15} {share:>8}")

        # World production comparison
        report.append("\n## WORLD WHEAT PRODUCTION (Latest Year, MMT)\n")
        cursor = self.conn.execute("""
            SELECT location_code, production_mmt, exports_mmt, stocks_use_pct
            FROM gold_world_wheat_production
            WHERE CAST(SUBSTR(marketing_year, 1, 4) AS INTEGER) = (
                SELECT MAX(CAST(SUBSTR(marketing_year, 1, 4) AS INTEGER))
                FROM gold_world_wheat_production
                WHERE production_mmt IS NOT NULL
            )
            ORDER BY production_mmt DESC
        """)
        rows = cursor.fetchall()
        if rows:
            report.append("  Country         Production    Exports  Stks/Use")
            report.append("  " + "-" * 50)
            for row in rows:
                loc = row['location_code']
                prod = f"{row['production_mmt']:.1f}" if row['production_mmt'] else "  -"
                exp = f"{row['exports_mmt']:.1f}" if row['exports_mmt'] else " -"
                stk = f"{row['stocks_use_pct']:.1f}%" if row['stocks_use_pct'] else " -"
                report.append(f"  {loc:16} {prod:>10} {exp:>10} {stk:>10}")

        report_text = "\n".join(report)

        report_path = OUTPUT_DIR / f"wheat_report_{datetime.now().strftime('%Y%m%d')}.txt"
        with open(report_path, 'w') as f:
            f.write(report_text)

        logger.info(f"Report saved: {report_path}")
        return report_text


def main():
    parser = argparse.ArgumentParser(description='Generate wheat visualizations')
    parser.add_argument('--all', action='store_true', help='Generate all views, charts, and reports')
    parser.add_argument('--views', action='store_true', help='Create SQL views only')
    parser.add_argument('--charts', action='store_true', help='Generate charts')
    parser.add_argument('--report', action='store_true', help='Generate text report')
    args = parser.parse_args()

    conn = sqlite3.connect(str(DB_PATH))
    viz = WheatVisualization(conn)

    if args.all or args.views:
        viz.create_all_views()

    if args.all or args.charts:
        viz.generate_wheat_price_chart()
        viz.generate_balance_sheet_chart()
        viz.generate_export_destinations_chart()

    if args.all or args.report:
        report = viz.generate_report()
        print(report)

    if not any([args.all, args.views, args.charts, args.report]):
        viz.create_all_views()
        viz.generate_wheat_price_chart()
        viz.generate_balance_sheet_chart()
        viz.generate_export_destinations_chart()
        report = viz.generate_report()
        print(report)

    conn.close()


if __name__ == '__main__':
    main()
