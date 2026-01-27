"""
Inventory balance sheet data in PostgreSQL to understand what we have
for building traditional S&D balance sheets.

Traditional Balance Sheet:
  Beginning Stocks + Production + Imports = Total Supply
  Domestic Use + Exports = Total Demand
  Total Supply - Total Demand = Ending Stocks
  Beginning Stocks (current year) = Ending Stocks (prior year)
"""

import psycopg2
from collections import defaultdict

# Database configuration
PG_HOST = "localhost"
PG_PORT = "5432"
PG_DATABASE = "rlc_commodities"
PG_USER = "postgres"
PG_PASSWORD = "SoupBoss1"


def main():
    """Inventory the balance sheet data."""

    print("=" * 70)
    print("BALANCE SHEET DATA INVENTORY")
    print("=" * 70)

    try:
        conn = psycopg2.connect(
            host=PG_HOST,
            port=PG_PORT,
            database=PG_DATABASE,
            user=PG_USER,
            password=PG_PASSWORD
        )
        cursor = conn.cursor()
        print(f"Connected to {PG_DATABASE}\n")
    except Exception as e:
        print(f"Connection failed: {e}")
        return

    # 1. Get all unique commodities
    print("-" * 70)
    print("COMMODITIES")
    print("-" * 70)
    cursor.execute("""
        SELECT DISTINCT commodity, COUNT(*) as records
        FROM bronze.sqlite_commodity_balance_sheets
        GROUP BY commodity
        ORDER BY records DESC
    """)
    commodities = cursor.fetchall()
    for comm, count in commodities:
        print(f"  {comm}: {count:,} records")

    # 2. Get all unique countries
    print("\n" + "-" * 70)
    print("COUNTRIES")
    print("-" * 70)
    cursor.execute("""
        SELECT DISTINCT country, COUNT(*) as records
        FROM bronze.sqlite_commodity_balance_sheets
        GROUP BY country
        ORDER BY records DESC
        LIMIT 20
    """)
    countries = cursor.fetchall()
    for country, count in countries:
        print(f"  {country}: {count:,} records")

    # 3. Get all unique metrics (these are the balance sheet line items)
    print("\n" + "-" * 70)
    print("METRICS (Balance Sheet Line Items)")
    print("-" * 70)
    cursor.execute("""
        SELECT DISTINCT metric, COUNT(*) as records
        FROM bronze.sqlite_commodity_balance_sheets
        GROUP BY metric
        ORDER BY records DESC
    """)
    metrics = cursor.fetchall()
    for metric, count in metrics:
        print(f"  {metric}: {count:,} records")

    # 4. Get all unique sections
    print("\n" + "-" * 70)
    print("SECTIONS")
    print("-" * 70)
    cursor.execute("""
        SELECT DISTINCT section, COUNT(*) as records
        FROM bronze.sqlite_commodity_balance_sheets
        GROUP BY section
        ORDER BY records DESC
    """)
    sections = cursor.fetchall()
    for section, count in sections:
        print(f"  {section}: {count:,} records")

    # 5. Get marketing years
    print("\n" + "-" * 70)
    print("MARKETING YEARS")
    print("-" * 70)
    cursor.execute("""
        SELECT DISTINCT marketing_year
        FROM bronze.sqlite_commodity_balance_sheets
        WHERE marketing_year IS NOT NULL
        ORDER BY marketing_year DESC
        LIMIT 20
    """)
    years = cursor.fetchall()
    for (year,) in years:
        print(f"  {year}")

    # 6. Check for traditional balance sheet components for US Soybeans
    print("\n" + "-" * 70)
    print("US SOYBEANS - Balance Sheet Components Check")
    print("-" * 70)

    balance_sheet_keywords = {
        'Beginning Stocks': ['beginning', 'beg stocks', 'beginning stocks', 'carry-in', 'carryin'],
        'Production': ['production', 'output', 'crop'],
        'Imports': ['import'],
        'Total Supply': ['total supply', 'supply total'],
        'Domestic Use': ['domestic', 'crush', 'food', 'feed', 'seed', 'residual'],
        'Exports': ['export'],
        'Ending Stocks': ['ending', 'end stocks', 'ending stocks', 'carry-out', 'carryout'],
    }

    cursor.execute("""
        SELECT DISTINCT metric
        FROM bronze.sqlite_commodity_balance_sheets
        WHERE LOWER(commodity) LIKE '%soy%'
          AND LOWER(country) LIKE '%united states%'
    """)
    soy_metrics = [m[0] for m in cursor.fetchall()]

    print("\nMetrics found for US Soybeans:")
    for m in sorted(soy_metrics):
        print(f"  - {m}")

    # 7. Sample data for US Soybeans
    print("\n" + "-" * 70)
    print("SAMPLE: US Soybeans 2024/25 (if available)")
    print("-" * 70)
    cursor.execute("""
        SELECT metric, marketing_year, value, unit
        FROM bronze.sqlite_commodity_balance_sheets
        WHERE LOWER(commodity) LIKE '%soy%'
          AND LOWER(country) LIKE '%united states%'
          AND marketing_year LIKE '%2024%'
        ORDER BY metric
        LIMIT 30
    """)
    rows = cursor.fetchall()
    if rows:
        for metric, my, value, unit in rows:
            print(f"  {metric}: {value} {unit or ''} ({my})")
    else:
        print("  No 2024 data found, trying latest year...")
        cursor.execute("""
            SELECT metric, marketing_year, value, unit
            FROM bronze.sqlite_commodity_balance_sheets
            WHERE LOWER(commodity) LIKE '%soy%'
              AND LOWER(country) LIKE '%united states%'
            ORDER BY marketing_year DESC, metric
            LIMIT 30
        """)
        rows = cursor.fetchall()
        for metric, my, value, unit in rows:
            print(f"  {metric}: {value} {unit or ''} ({my})")

    # 8. Check data for other key commodities
    print("\n" + "-" * 70)
    print("DATA COVERAGE BY COMMODITY + COUNTRY")
    print("-" * 70)
    cursor.execute("""
        SELECT
            commodity,
            country,
            COUNT(DISTINCT metric) as unique_metrics,
            COUNT(DISTINCT marketing_year) as year_coverage,
            MIN(marketing_year) as earliest,
            MAX(marketing_year) as latest,
            COUNT(*) as total_records
        FROM bronze.sqlite_commodity_balance_sheets
        GROUP BY commodity, country
        ORDER BY total_records DESC
        LIMIT 30
    """)
    coverage = cursor.fetchall()
    print(f"\n{'Commodity':<20} {'Country':<25} {'Metrics':>8} {'Years':>6} {'Range':<15} {'Records':>10}")
    print("-" * 90)
    for comm, country, metrics, years, earliest, latest, records in coverage:
        comm_short = comm[:18] if len(comm) > 18 else comm
        country_short = country[:23] if len(country) > 23 else country
        year_range = f"{earliest}-{latest}" if earliest and latest else "N/A"
        print(f"{comm_short:<20} {country_short:<25} {metrics:>8} {years:>6} {year_range:<15} {records:>10,}")

    conn.close()
    print("\n" + "=" * 70)
    print("INVENTORY COMPLETE")
    print("=" * 70)


if __name__ == "__main__":
    main()
