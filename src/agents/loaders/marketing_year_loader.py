"""
Load marketing year reference data from JSON into PostgreSQL.

Loads the comprehensive USDA PSD marketing year definitions from
domain_knowledge/data_dictionaries/usda_psd_marketing_years_reference.json
"""

import json
import psycopg2
from pathlib import Path

# Paths
PROJECT_ROOT = Path(__file__).parent.parent.parent.parent
JSON_PATH = PROJECT_ROOT / 'domain_knowledge' / 'data_dictionaries' / 'usda_psd_marketing_years_reference.json'

# Database connection
DB_CONFIG = {
    'host': 'localhost',
    'port': 5432,
    'database': 'rlc_commodities',
    'user': 'postgres',
    'password': 'SoupBoss1'
}


def load_marketing_years():
    """Load marketing year reference data into silver layer."""

    # Load JSON
    print(f"Loading from {JSON_PATH}...")
    with open(JSON_PATH, 'r', encoding='utf-8') as f:
        data = json.load(f)

    # Connect to database
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()

    # Load trade year conventions
    print("\nLoading trade year conventions...")
    conventions = data.get('trade_year_conventions', [])

    for conv in conventions:
        cur.execute("""
            INSERT INTO silver.trade_year_convention
            (commodity_group, ty_begin_month, ty_end_month, period_description)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (commodity_group) DO UPDATE SET
                ty_begin_month = EXCLUDED.ty_begin_month,
                ty_end_month = EXCLUDED.ty_end_month,
                period_description = EXCLUDED.period_description
        """, (
            conv['commodity_group'],
            conv['ty_begin_month'],
            conv['ty_end_month'],
            conv.get('period', '')
        ))

    print(f"  Loaded {len(conventions)} trade year conventions")

    # Load marketing year definitions
    print("\nLoading marketing year definitions...")
    marketing_years = data.get('marketing_years', [])
    loaded = 0
    skipped = 0

    for my in marketing_years:
        try:
            cur.execute("""
                INSERT INTO silver.marketing_year_reference
                (country, country_code, commodity, commodity_group,
                 my_begin_month, my_end_month, my_label_format,
                 ty_begin_month, ty_end_month, southern_hemisphere, notes)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (country_code, commodity) DO UPDATE SET
                    country = EXCLUDED.country,
                    commodity_group = EXCLUDED.commodity_group,
                    my_begin_month = EXCLUDED.my_begin_month,
                    my_end_month = EXCLUDED.my_end_month,
                    my_label_format = EXCLUDED.my_label_format,
                    ty_begin_month = EXCLUDED.ty_begin_month,
                    ty_end_month = EXCLUDED.ty_end_month,
                    southern_hemisphere = EXCLUDED.southern_hemisphere,
                    notes = EXCLUDED.notes
            """, (
                my['country'],
                my['country_code'],
                my['commodity'],
                my['commodity_group'],
                my['my_begin_month'],
                my['my_end_month'],
                my.get('my_label_format', ''),
                my['ty_begin_month'],
                my['ty_end_month'],
                my.get('southern_hemisphere', False),
                my.get('notes', '')
            ))
            loaded += 1
        except Exception as e:
            print(f"  Error loading {my.get('country_code')}/{my.get('commodity')}: {e}")
            skipped += 1

    conn.commit()

    print(f"  Loaded {loaded} marketing year definitions")
    if skipped:
        print(f"  Skipped {skipped} due to errors")

    # Summary
    cur.execute("SELECT COUNT(*) FROM silver.marketing_year_reference")
    total = cur.fetchone()[0]

    cur.execute("SELECT COUNT(DISTINCT country_code) FROM silver.marketing_year_reference")
    countries = cur.fetchone()[0]

    cur.execute("SELECT COUNT(DISTINCT commodity) FROM silver.marketing_year_reference")
    commodities = cur.fetchone()[0]

    print(f"\nDatabase now has:")
    print(f"  {total} marketing year definitions")
    print(f"  {countries} countries")
    print(f"  {commodities} commodities")

    conn.close()
    print("\nDone!")


if __name__ == '__main__':
    load_marketing_years()
