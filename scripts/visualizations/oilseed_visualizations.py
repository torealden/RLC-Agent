#!/usr/bin/env python3
"""
Oilseed Gold Layer Visualizations

Creates analytical views and visualizations for soybeans and oilseeds.
Generates charts, dashboards, and summary reports.

Usage:
    python scripts/visualizations/oilseed_visualizations.py --all
    python scripts/visualizations/oilseed_visualizations.py --report
"""

import argparse
import logging
import sqlite3
from datetime import datetime
from pathlib import Path

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger('oilseed_viz')

PROJECT_ROOT = Path(__file__).parent.parent.parent
DB_PATH = PROJECT_ROOT / "data" / "rlc_commodities.db"
OUTPUT_DIR = PROJECT_ROOT / "output" / "visualizations"

try:
    import matplotlib.pyplot as plt
    HAS_MATPLOTLIB = True
except ImportError:
    HAS_MATPLOTLIB = False


class OilseedVisualization:
    """Creates oilseed gold layer visualizations."""

    def __init__(self, conn: sqlite3.Connection):
        self.conn = conn
        self.conn.row_factory = sqlite3.Row
        OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    def create_all_views(self):
        """Create all oilseed gold layer SQL views."""
        logger.info("Creating oilseed gold views...")

        self._create_soybean_balance_sheet_view()
        self._create_soybean_price_summary()
        self._create_crush_margin_view()
        self._create_world_production_view()
        self._create_soybean_complex_summary()

        logger.info("Oilseed gold views created")

    def _create_soybean_balance_sheet_view(self):
        """Create US soybean balance sheet summary view."""
        self.conn.execute("DROP VIEW IF EXISTS gold_soybean_balance_sheet")
        self.conn.execute("""
            CREATE VIEW gold_soybean_balance_sheet AS
            SELECT
                marketing_year,
                beginning_stocks,
                production,
                imports,
                total_supply,
                crush,
                exports,
                seed_feed_residual,
                total_use,
                ending_stocks,
                original_unit,
                ROUND(stocks_to_use_ratio * 100, 1) AS stocks_use_pct,
                ROUND(crush_pct, 1) AS crush_pct,
                ROUND(export_pct, 1) AS export_pct,
                ROUND(production_yoy_pct, 1) AS production_yoy_pct,
                ROUND(ending_stocks_yoy_pct, 1) AS stocks_yoy_pct
            FROM silver_oilseed_balance_sheet
            WHERE commodity_code = 'SOYBEANS' AND location_code = 'US'
            ORDER BY marketing_year DESC
        """)
        self.conn.commit()

    def _create_soybean_price_summary(self):
        """Create soybean price summary view."""
        self.conn.execute("DROP VIEW IF EXISTS gold_soybean_prices")
        self.conn.execute("""
            CREATE VIEW gold_soybean_prices AS
            SELECT
                marketing_year,
                MAX(CASE WHEN commodity_code = 'SOYBEANS' AND price_type IN ('FARM', 'SEASON_AVG')
                    THEN price_usd_bu END) AS soybean_price_bu,
                MAX(CASE WHEN commodity_code = 'SOYBEAN_MEAL' AND price_type = 'WHOLESALE'
                    THEN price_usd_st END) AS meal_price_st,
                MAX(CASE WHEN commodity_code = 'SOYBEAN_OIL' AND price_type = 'WHOLESALE'
                    THEN price_cents_lb END) AS oil_price_cents_lb
            FROM silver_oilseed_price
            WHERE location_code = 'US'
            GROUP BY marketing_year
            HAVING soybean_price_bu IS NOT NULL
            ORDER BY marketing_year DESC
        """)
        self.conn.commit()

    def _create_crush_margin_view(self):
        """Create crush margin history view."""
        self.conn.execute("DROP VIEW IF EXISTS gold_crush_margins")
        self.conn.execute("""
            CREATE VIEW gold_crush_margins AS
            SELECT
                marketing_year,
                ROUND(soybean_price_bu, 2) AS bean_price,
                ROUND(meal_price_st, 2) AS meal_price,
                ROUND(oil_price_lb, 2) AS oil_price,
                ROUND(meal_value_bu, 2) AS meal_value_per_bu,
                ROUND(oil_value_bu, 2) AS oil_value_per_bu,
                ROUND(gross_margin_bu, 2) AS gross_margin
            FROM silver_crush_margin
            WHERE location_code = 'US'
            ORDER BY marketing_year DESC
        """)
        self.conn.commit()

    def _create_world_production_view(self):
        """Create world soybean production view."""
        self.conn.execute("DROP VIEW IF EXISTS gold_world_soybean_production")
        self.conn.execute("""
            CREATE VIEW gold_world_soybean_production AS
            SELECT
                marketing_year,
                location_code,
                ROUND(production, 1) AS production,
                original_unit,
                ROUND(production_yoy_pct, 1) AS yoy_pct
            FROM silver_oilseed_balance_sheet
            WHERE commodity_code = 'SOYBEANS'
              AND location_code IN ('US', 'BR', 'AR', 'WORLD', 'CN')
              AND production IS NOT NULL
            ORDER BY marketing_year DESC, production DESC
        """)
        self.conn.commit()

    def _create_soybean_complex_summary(self):
        """Create soybean complex summary (beans + meal + oil)."""
        self.conn.execute("DROP VIEW IF EXISTS gold_soybean_complex")
        self.conn.execute("""
            CREATE VIEW gold_soybean_complex AS
            SELECT
                b.marketing_year,
                -- Soybeans
                b.production AS bean_production,
                b.crush AS bean_crush,
                b.exports AS bean_exports,
                b.ending_stocks AS bean_stocks,
                ROUND(b.stocks_to_use_ratio * 100, 1) AS bean_stocks_use_pct,
                -- Meal
                m.production AS meal_production,
                m.exports AS meal_exports,
                -- Oil
                o.production AS oil_production,
                o.exports AS oil_exports,
                o.biofuel_use AS oil_biofuel
            FROM silver_oilseed_balance_sheet b
            LEFT JOIN silver_oilseed_balance_sheet m
                ON b.marketing_year = m.marketing_year
                AND m.commodity_code = 'SOYBEAN_MEAL'
                AND m.location_code = 'US'
            LEFT JOIN silver_oilseed_balance_sheet o
                ON b.marketing_year = o.marketing_year
                AND o.commodity_code = 'SOYBEAN_OIL'
                AND o.location_code = 'US'
            WHERE b.commodity_code = 'SOYBEANS' AND b.location_code = 'US'
            ORDER BY b.marketing_year DESC
        """)
        self.conn.commit()

    def generate_soybean_price_chart(self, years: int = 15) -> str:
        """Generate soybean price history chart."""
        if not HAS_MATPLOTLIB:
            return None

        logger.info("Generating soybean price chart...")

        cursor = self.conn.execute("""
            SELECT marketing_year, soybean_price_bu, meal_price_st, oil_price_cents_lb
            FROM gold_soybean_prices
            WHERE CAST(SUBSTR(marketing_year, 1, 4) AS INTEGER) >= ?
            ORDER BY marketing_year
        """, (datetime.now().year - years,))

        data = cursor.fetchall()
        if not data:
            return None

        years_list = [row['marketing_year'] for row in data]
        bean_prices = [row['soybean_price_bu'] for row in data]
        meal_prices = [row['meal_price_st'] for row in data if row['meal_price_st']]
        oil_prices = [row['oil_price_cents_lb'] for row in data if row['oil_price_cents_lb']]

        fig, (ax1, ax2, ax3) = plt.subplots(3, 1, figsize=(12, 10))

        # Soybean price
        ax1.bar(years_list, bean_prices, color='green', alpha=0.7)
        ax1.set_ylabel('$/bushel')
        ax1.set_title('US Soybean Price (Farm)')
        ax1.tick_params(axis='x', rotation=45)
        ax1.grid(True, alpha=0.3)

        # Meal price
        meal_years = [row['marketing_year'] for row in data if row['meal_price_st']]
        if meal_years:
            ax2.bar(meal_years, meal_prices, color='orange', alpha=0.7)
            ax2.set_ylabel('$/short ton')
            ax2.set_title('US Soybean Meal Price (Wholesale)')
            ax2.tick_params(axis='x', rotation=45)
            ax2.grid(True, alpha=0.3)

        # Oil price
        oil_years = [row['marketing_year'] for row in data if row['oil_price_cents_lb']]
        if oil_years:
            ax3.bar(oil_years, oil_prices, color='gold', alpha=0.7)
            ax3.set_ylabel('cents/lb')
            ax3.set_title('US Soybean Oil Price (Wholesale)')
            ax3.tick_params(axis='x', rotation=45)
            ax3.grid(True, alpha=0.3)

        chart_path = OUTPUT_DIR / f"soybean_prices_{datetime.now().strftime('%Y%m%d')}.png"
        plt.tight_layout()
        plt.savefig(chart_path, dpi=150)
        plt.close()

        logger.info(f"Chart saved: {chart_path}")
        return str(chart_path)

    def generate_crush_margin_chart(self) -> str:
        """Generate crush margin chart."""
        if not HAS_MATPLOTLIB:
            return None

        logger.info("Generating crush margin chart...")

        cursor = self.conn.execute("""
            SELECT marketing_year, bean_price, gross_margin
            FROM gold_crush_margins
            WHERE gross_margin IS NOT NULL
            ORDER BY marketing_year
        """)

        data = cursor.fetchall()
        if not data:
            return None

        years = [row['marketing_year'] for row in data]
        margins = [row['gross_margin'] for row in data]

        fig, ax = plt.subplots(figsize=(12, 6))

        colors = ['green' if m > 0 else 'red' for m in margins]
        ax.bar(years, margins, color=colors, alpha=0.7)
        ax.axhline(y=0, color='black', linestyle='-', linewidth=0.5)
        ax.set_xlabel('Marketing Year')
        ax.set_ylabel('$/bushel')
        ax.set_title('US Soybean Gross Crush Margin')
        ax.tick_params(axis='x', rotation=45)
        ax.grid(True, alpha=0.3)

        chart_path = OUTPUT_DIR / f"soybean_crush_margin_{datetime.now().strftime('%Y%m%d')}.png"
        plt.tight_layout()
        plt.savefig(chart_path, dpi=150)
        plt.close()

        logger.info(f"Chart saved: {chart_path}")
        return str(chart_path)

    def generate_balance_sheet_chart(self) -> str:
        """Generate soybean balance sheet chart."""
        if not HAS_MATPLOTLIB:
            return None

        logger.info("Generating soybean balance sheet chart...")

        cursor = self.conn.execute("""
            SELECT marketing_year, production, crush, exports, ending_stocks
            FROM gold_soybean_balance_sheet
            WHERE CAST(SUBSTR(marketing_year, 1, 4) AS INTEGER) >= 2005
            ORDER BY marketing_year
        """)

        data = cursor.fetchall()
        if not data:
            return None

        years = [row['marketing_year'] for row in data]
        production = [row['production'] / 1000 if row['production'] else 0 for row in data]  # Billion bushels
        crush = [row['crush'] / 1000 if row['crush'] else 0 for row in data]
        exports = [row['exports'] / 1000 if row['exports'] else 0 for row in data]
        stocks = [row['ending_stocks'] / 1000 if row['ending_stocks'] else 0 for row in data]

        fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(12, 10))

        # Supply/Production
        ax1.bar(years, production, color='green', alpha=0.7, label='Production')
        ax1.plot(years, stocks, 'ro-', label='Ending Stocks', markersize=6)
        ax1.set_ylabel('Billion Bushels')
        ax1.set_title('US Soybean Supply')
        ax1.legend(loc='upper left')
        ax1.tick_params(axis='x', rotation=45)
        ax1.grid(True, alpha=0.3)

        # Demand breakdown
        ax2.bar(years, crush, label='Crush', color='purple', alpha=0.7)
        ax2.bar(years, exports, bottom=crush, label='Exports', color='blue', alpha=0.7)
        ax2.set_xlabel('Marketing Year')
        ax2.set_ylabel('Billion Bushels')
        ax2.set_title('US Soybean Demand Breakdown')
        ax2.legend(loc='upper left')
        ax2.tick_params(axis='x', rotation=45)
        ax2.grid(True, alpha=0.3)

        chart_path = OUTPUT_DIR / f"soybean_balance_sheet_{datetime.now().strftime('%Y%m%d')}.png"
        plt.tight_layout()
        plt.savefig(chart_path, dpi=150)
        plt.close()

        logger.info(f"Chart saved: {chart_path}")
        return str(chart_path)

    def generate_report(self) -> str:
        """Generate comprehensive soybean market report."""
        logger.info("Generating soybean market report...")

        report = []
        report.append("=" * 60)
        report.append("US SOYBEAN MARKET REPORT")
        report.append(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M')}")
        report.append("=" * 60)

        # Balance sheet summary
        report.append("\n## SOYBEAN BALANCE SHEET\n")
        cursor = self.conn.execute("""
            SELECT * FROM gold_soybean_balance_sheet
            ORDER BY marketing_year DESC
            LIMIT 5
        """)
        for row in cursor.fetchall():
            report.append(f"### {row['marketing_year']}")
            if row['production']:
                report.append(f"  Production:    {row['production']:,.0f} {row['original_unit']}")
            if row['crush']:
                report.append(f"  Crush:         {row['crush']:,.0f} ({row['crush_pct']:.1f}%)")
            if row['exports']:
                report.append(f"  Exports:       {row['exports']:,.0f} ({row['export_pct']:.1f}%)")
            if row['ending_stocks']:
                report.append(f"  Ending Stocks: {row['ending_stocks']:,.0f}")
            if row['stocks_use_pct']:
                report.append(f"  Stocks/Use:    {row['stocks_use_pct']:.1f}%")
            report.append("")

        # Price summary
        report.append("\n## SOYBEAN COMPLEX PRICES\n")
        cursor = self.conn.execute("""
            SELECT * FROM gold_soybean_prices
            ORDER BY marketing_year DESC
            LIMIT 5
        """)
        report.append("  Year        Bean($/bu)  Meal($/ST)  Oil(c/lb)")
        report.append("  " + "-" * 50)
        for row in cursor.fetchall():
            bean = f"${row['soybean_price_bu']:.2f}" if row['soybean_price_bu'] else "   -  "
            meal = f"${row['meal_price_st']:.0f}" if row['meal_price_st'] else "  -  "
            oil = f"{row['oil_price_cents_lb']:.1f}" if row['oil_price_cents_lb'] else " - "
            report.append(f"  {row['marketing_year']:10} {bean:>10}  {meal:>10}  {oil:>8}")

        # Crush margins
        report.append("\n## CRUSH MARGINS\n")
        cursor = self.conn.execute("""
            SELECT * FROM gold_crush_margins
            ORDER BY marketing_year DESC
            LIMIT 5
        """)
        report.append("  Year        Gross Margin ($/bu)")
        report.append("  " + "-" * 35)
        for row in cursor.fetchall():
            margin = row['gross_margin']
            if margin is not None:
                sign = "+" if margin > 0 else ""
                report.append(f"  {row['marketing_year']:10} {sign}${margin:.2f}")

        # Soybean complex summary
        report.append("\n## SOYBEAN COMPLEX SUMMARY (Latest Year)\n")
        cursor = self.conn.execute("""
            SELECT * FROM gold_soybean_complex
            ORDER BY marketing_year DESC
            LIMIT 1
        """)
        row = cursor.fetchone()
        if row:
            report.append(f"Marketing Year: {row['marketing_year']}")
            report.append(f"\nSOYBEANS:")
            if row['bean_production']:
                report.append(f"  Production: {row['bean_production']:,.0f}")
            if row['bean_crush']:
                report.append(f"  Crush:      {row['bean_crush']:,.0f}")
            if row['bean_exports']:
                report.append(f"  Exports:    {row['bean_exports']:,.0f}")
            if row['bean_stocks']:
                report.append(f"  End Stocks: {row['bean_stocks']:,.0f}")
            if row['bean_stocks_use_pct']:
                report.append(f"  Stocks/Use: {row['bean_stocks_use_pct']:.1f}%")

            report.append(f"\nSOYBEAN MEAL:")
            if row['meal_production']:
                report.append(f"  Production: {row['meal_production']:,.0f}")
            if row['meal_exports']:
                report.append(f"  Exports:    {row['meal_exports']:,.0f}")

            report.append(f"\nSOYBEAN OIL:")
            if row['oil_production']:
                report.append(f"  Production: {row['oil_production']:,.0f}")
            if row['oil_exports']:
                report.append(f"  Exports:    {row['oil_exports']:,.0f}")
            if row['oil_biofuel']:
                report.append(f"  Biofuel:    {row['oil_biofuel']:,.0f}")

        report_text = "\n".join(report)

        report_path = OUTPUT_DIR / f"soybean_report_{datetime.now().strftime('%Y%m%d')}.txt"
        with open(report_path, 'w') as f:
            f.write(report_text)

        logger.info(f"Report saved: {report_path}")
        return report_text


def main():
    parser = argparse.ArgumentParser(description='Generate oilseed visualizations')
    parser.add_argument('--all', action='store_true', help='Generate all views, charts, and reports')
    parser.add_argument('--views', action='store_true', help='Create SQL views only')
    parser.add_argument('--charts', action='store_true', help='Generate charts')
    parser.add_argument('--report', action='store_true', help='Generate text report')
    args = parser.parse_args()

    conn = sqlite3.connect(str(DB_PATH))
    viz = OilseedVisualization(conn)

    if args.all or args.views:
        viz.create_all_views()

    if args.all or args.charts:
        viz.generate_soybean_price_chart()
        viz.generate_crush_margin_chart()
        viz.generate_balance_sheet_chart()

    if args.all or args.report:
        report = viz.generate_report()
        print(report)

    if not any([args.all, args.views, args.charts, args.report]):
        viz.create_all_views()
        viz.generate_soybean_price_chart()
        viz.generate_crush_margin_chart()
        viz.generate_balance_sheet_chart()
        report = viz.generate_report()
        print(report)

    conn.close()


if __name__ == '__main__':
    main()
