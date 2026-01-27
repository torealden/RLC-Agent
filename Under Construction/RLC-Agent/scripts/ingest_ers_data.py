"""
ERS Data Ingestion Script
Loads clean, structured ERS data from CSV files into PostgreSQL.

Data sources:
- OilCropsAllTables.csv - Comprehensive oilseed/oils data
- Wheat/*.csv - Wheat supply, demand, trade data

Usage:
    python scripts/ingest_ers_data.py --preview   # Preview data
    python scripts/ingest_ers_data.py --load      # Load to database
"""

import os
import csv
import argparse
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Optional
import psycopg2
from psycopg2.extras import execute_values

# ============================================================================
# CONFIGURATION
# ============================================================================

SCRIPT_DIR = Path(__file__).parent
PROJECT_ROOT = SCRIPT_DIR.parent
DATA_DIR = PROJECT_ROOT / "Models" / "Data"

# PostgreSQL configuration
PG_CONFIG = {
    "host": "localhost",
    "port": "5432",
    "database": "rlc_commodities",
    "user": "postgres",
    "password": "SoupBoss1"
}

# ============================================================================
# SCHEMA DEFINITIONS
# ============================================================================

CREATE_ERS_OILCROPS_TABLE = """
CREATE TABLE IF NOT EXISTS bronze.ers_oilcrops_raw (
    id SERIAL PRIMARY KEY,
    timeperiod_desc VARCHAR(50),
    marketing_year VARCHAR(20),
    my_definition VARCHAR(50),
    commodity_group VARCHAR(50),
    commodity VARCHAR(100),
    commodity_desc2 VARCHAR(100),
    attribute_desc VARCHAR(200),
    attribute_desc2 VARCHAR(200),
    geography_desc VARCHAR(100),
    geography_desc2 VARCHAR(100),
    amount NUMERIC(20, 6),
    unit_desc VARCHAR(100),
    table_number VARCHAR(20),
    table_name TEXT,
    source_file VARCHAR(200),
    created_at TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE(marketing_year, commodity, attribute_desc, geography_desc, timeperiod_desc, table_number)
);

CREATE INDEX IF NOT EXISTS idx_ers_oilcrops_commodity ON bronze.ers_oilcrops_raw(commodity);
CREATE INDEX IF NOT EXISTS idx_ers_oilcrops_my ON bronze.ers_oilcrops_raw(marketing_year);
CREATE INDEX IF NOT EXISTS idx_ers_oilcrops_attr ON bronze.ers_oilcrops_raw(attribute_desc);
"""

CREATE_ERS_WHEAT_TABLE = """
CREATE TABLE IF NOT EXISTS bronze.ers_wheat_raw (
    id SERIAL PRIMARY KEY,
    commodity_desc VARCHAR(100),
    commodity_desc2 VARCHAR(100),
    attribute_desc VARCHAR(200),
    geography_desc VARCHAR(100),
    unit_desc VARCHAR(100),
    marketing_year VARCHAR(20),
    timeperiod_desc VARCHAR(50),
    amount NUMERIC(20, 6),
    source_file VARCHAR(200),
    created_at TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE(marketing_year, commodity_desc, commodity_desc2, attribute_desc, geography_desc, timeperiod_desc)
);

CREATE INDEX IF NOT EXISTS idx_ers_wheat_commodity ON bronze.ers_wheat_raw(commodity_desc);
CREATE INDEX IF NOT EXISTS idx_ers_wheat_my ON bronze.ers_wheat_raw(marketing_year);
CREATE INDEX IF NOT EXISTS idx_ers_wheat_attr ON bronze.ers_wheat_raw(attribute_desc);
"""

# ============================================================================
# DATA LOADING FUNCTIONS
# ============================================================================

def parse_amount(value: str) -> Optional[float]:
    """Parse amount value, handling various formats."""
    if not value or value.strip() == '':
        return None
    try:
        # Remove commas and whitespace
        clean = value.replace(',', '').strip()
        return float(clean)
    except (ValueError, TypeError):
        return None


def load_oilcrops_csv(filepath: Path) -> List[Dict]:
    """Load OilCropsAllTables.csv and return records."""
    records = []

    with open(filepath, 'r', encoding='utf-8-sig') as f:
        reader = csv.DictReader(f)
        for row in reader:
            amount = parse_amount(row.get('Amount', ''))
            if amount is None:
                continue  # Skip rows without valid amounts

            records.append({
                'timeperiod_desc': row.get('Timeperiod_Desc', '').strip(),
                'marketing_year': row.get('Marketing_Year', '').strip(),
                'my_definition': row.get('MY_Definition', '').strip(),
                'commodity_group': row.get('Commodity_Group', '').strip(),
                'commodity': row.get('Commodity', '').strip(),
                'commodity_desc2': row.get('Commodity_Desc2', '').strip(),
                'attribute_desc': row.get('Attribute_Desc', '').strip(),
                'attribute_desc2': row.get('Attribute_Desc2', '').strip(),
                'geography_desc': row.get('Geography_Desc', '').strip(),
                'geography_desc2': row.get('Geography_Desc2', '').strip(),
                'amount': amount,
                'unit_desc': row.get('Unit_Desc', '').strip(),
                'table_number': row.get('Table_number', '').strip(),
                'table_name': row.get('Table_name', '').strip(),
                'source_file': filepath.name
            })

    return records


def load_wheat_csv(filepath: Path) -> List[Dict]:
    """Load a wheat CSV file and return records."""
    records = []

    with open(filepath, 'r', encoding='utf-8-sig') as f:
        reader = csv.DictReader(f)
        for row in reader:
            amount = parse_amount(row.get('Amount', ''))
            if amount is None:
                continue

            records.append({
                'commodity_desc': row.get('Commodity_Desc', '').strip(),
                'commodity_desc2': row.get('Commodity_Desc2', '').strip(),
                'attribute_desc': row.get('Attribute_Desc', '').strip(),
                'geography_desc': row.get('Geography_Desc', '').strip(),
                'unit_desc': row.get('Unit_Desc', '').strip(),
                'marketing_year': row.get('Marketing_Year', '').strip(),
                'timeperiod_desc': row.get('Timeperiod_Desc', '').strip(),
                'amount': amount,
                'source_file': filepath.name
            })

    return records


# ============================================================================
# DATABASE OPERATIONS
# ============================================================================

def get_connection():
    """Get PostgreSQL connection."""
    return psycopg2.connect(**PG_CONFIG)


def setup_tables(conn):
    """Create tables if they don't exist."""
    with conn.cursor() as cur:
        cur.execute(CREATE_ERS_OILCROPS_TABLE)
        cur.execute(CREATE_ERS_WHEAT_TABLE)
    conn.commit()
    print("Tables created/verified")


def insert_oilcrops_records(conn, records: List[Dict]) -> int:
    """Insert oilcrops records with upsert."""
    if not records:
        return 0

    # Deduplicate by key - keep last occurrence (most recent value)
    seen = {}
    for r in records:
        key = (r['marketing_year'], r['commodity'], r['attribute_desc'],
               r['geography_desc'], r['timeperiod_desc'], r['table_number'])
        seen[key] = r

    unique_records = list(seen.values())
    print(f"    (Deduplicated: {len(records)} -> {len(unique_records)} unique records)")

    sql = """
        INSERT INTO bronze.ers_oilcrops_raw
        (timeperiod_desc, marketing_year, my_definition, commodity_group,
         commodity, commodity_desc2, attribute_desc, attribute_desc2,
         geography_desc, geography_desc2, amount, unit_desc,
         table_number, table_name, source_file)
        VALUES %s
        ON CONFLICT (marketing_year, commodity, attribute_desc, geography_desc, timeperiod_desc, table_number)
        DO UPDATE SET
            amount = EXCLUDED.amount,
            created_at = NOW()
    """

    values = [
        (r['timeperiod_desc'], r['marketing_year'], r['my_definition'], r['commodity_group'],
         r['commodity'], r['commodity_desc2'], r['attribute_desc'], r['attribute_desc2'],
         r['geography_desc'], r['geography_desc2'], r['amount'], r['unit_desc'],
         r['table_number'], r['table_name'], r['source_file'])
        for r in unique_records
    ]

    with conn.cursor() as cur:
        execute_values(cur, sql, values)
    conn.commit()

    return len(unique_records)


def insert_wheat_records(conn, records: List[Dict]) -> int:
    """Insert wheat records with upsert."""
    if not records:
        return 0

    # Deduplicate by key - keep last occurrence (most recent value)
    seen = {}
    for r in records:
        key = (r['marketing_year'], r['commodity_desc'], r['commodity_desc2'],
               r['attribute_desc'], r['geography_desc'], r['timeperiod_desc'])
        seen[key] = r

    unique_records = list(seen.values())
    if len(records) != len(unique_records):
        print(f"    (Deduplicated: {len(records)} -> {len(unique_records)} unique records)")

    sql = """
        INSERT INTO bronze.ers_wheat_raw
        (commodity_desc, commodity_desc2, attribute_desc, geography_desc,
         unit_desc, marketing_year, timeperiod_desc, amount, source_file)
        VALUES %s
        ON CONFLICT (marketing_year, commodity_desc, commodity_desc2, attribute_desc, geography_desc, timeperiod_desc)
        DO UPDATE SET
            amount = EXCLUDED.amount,
            created_at = NOW()
    """

    values = [
        (r['commodity_desc'], r['commodity_desc2'], r['attribute_desc'], r['geography_desc'],
         r['unit_desc'], r['marketing_year'], r['timeperiod_desc'], r['amount'], r['source_file'])
        for r in unique_records
    ]

    with conn.cursor() as cur:
        execute_values(cur, sql, values)
    conn.commit()

    return len(unique_records)


# ============================================================================
# VALIDATION FUNCTIONS
# ============================================================================

def validate_soybeans(conn):
    """Validate soybean data against known WASDE values."""
    print("\n" + "=" * 60)
    print("VALIDATION: US Soybeans 2024/25 (vs Jan 2026 WASDE)")
    print("=" * 60)

    # Expected values from January 2026 WASDE (million bushels)
    expected = {
        'Production': 4461,      # 4.461 billion bushels
        'Beginning stocks': 342,
        'Exports': 1825,
        'Crush': 2420,
        'Ending stocks': 380
    }

    with conn.cursor() as cur:
        # Query actual loaded values
        cur.execute("""
            SELECT attribute_desc, amount, unit_desc
            FROM bronze.ers_oilcrops_raw
            WHERE commodity = 'Soybeans'
              AND marketing_year = '2024/25'
              AND geography_desc = 'United States'
              AND attribute_desc IN ('Production', 'Beginning stocks', 'Exports', 'Crush', 'Ending stocks')
            ORDER BY attribute_desc
        """)

        results = cur.fetchall()

        if not results:
            print("No 2024/25 soybean data found!")
            return

        print(f"\n{'Attribute':<20} {'Loaded':>15} {'Expected':>15} {'Unit':>20} {'Status':>10}")
        print("-" * 80)

        for attr, amount, unit in results:
            exp = expected.get(attr)
            # Convert if needed (thousand bushels to million)
            if 'Thousand' in unit:
                amount_mil = amount / 1000
            elif 'Million' in unit:
                amount_mil = amount
            else:
                amount_mil = amount

            status = "OK" if exp and abs(amount_mil - exp) / exp < 0.05 else "CHECK"
            exp_str = f"{exp:,.0f}" if exp else "N/A"
            print(f"{attr:<20} {amount_mil:>15,.0f} {exp_str:>15} {unit:>20} {status:>10}")


def show_summary(conn):
    """Show summary of loaded data."""
    print("\n" + "=" * 60)
    print("DATA SUMMARY")
    print("=" * 60)

    with conn.cursor() as cur:
        # Oilcrops summary
        cur.execute("""
            SELECT commodity, COUNT(*) as records,
                   MIN(marketing_year) as earliest, MAX(marketing_year) as latest
            FROM bronze.ers_oilcrops_raw
            GROUP BY commodity
            ORDER BY records DESC
            LIMIT 15
        """)

        print("\nOilcrops Data (top 15 commodities):")
        print(f"{'Commodity':<25} {'Records':>10} {'Earliest':>12} {'Latest':>12}")
        print("-" * 60)
        for row in cur.fetchall():
            print(f"{row[0]:<25} {row[1]:>10,} {row[2]:>12} {row[3]:>12}")

        # Wheat summary
        cur.execute("""
            SELECT commodity_desc2, COUNT(*) as records,
                   MIN(marketing_year) as earliest, MAX(marketing_year) as latest
            FROM bronze.ers_wheat_raw
            GROUP BY commodity_desc2
            ORDER BY records DESC
            LIMIT 10
        """)

        print("\nWheat Data:")
        print(f"{'Commodity':<25} {'Records':>10} {'Earliest':>12} {'Latest':>12}")
        print("-" * 60)
        for row in cur.fetchall():
            print(f"{row[0]:<25} {row[1]:>10,} {row[2]:>12} {row[3]:>12}")


# ============================================================================
# MAIN FUNCTIONS
# ============================================================================

def preview_data():
    """Preview data without loading to database."""
    print("=" * 60)
    print("ERS DATA PREVIEW")
    print("=" * 60)

    # Preview OilCrops
    oilcrops_file = DATA_DIR / "OilCropsAllTables.csv"
    if oilcrops_file.exists():
        records = load_oilcrops_csv(oilcrops_file)
        print(f"\nOilCropsAllTables.csv: {len(records):,} records")

        # Show sample
        soybeans = [r for r in records if r['commodity'] == 'Soybeans' and r['marketing_year'] == '2024/25']
        print(f"  Soybeans 2024/25: {len(soybeans)} records")
        for r in soybeans[:5]:
            print(f"    {r['attribute_desc']}: {r['amount']:,.0f} {r['unit_desc']}")

    # Preview Wheat
    wheat_dir = DATA_DIR / "Wheat"
    if wheat_dir.exists():
        total_wheat = 0
        for csv_file in sorted(wheat_dir.glob("*.csv")):
            records = load_wheat_csv(csv_file)
            total_wheat += len(records)
            print(f"\n{csv_file.name}: {len(records):,} records")
        print(f"\nTotal wheat records: {total_wheat:,}")


def load_data():
    """Load data to database."""
    print("=" * 60)
    print("ERS DATA INGESTION")
    print("=" * 60)

    conn = get_connection()
    print(f"Connected to {PG_CONFIG['database']}")

    # Setup tables
    setup_tables(conn)

    # Load OilCrops
    oilcrops_file = DATA_DIR / "OilCropsAllTables.csv"
    if oilcrops_file.exists():
        print(f"\nLoading {oilcrops_file.name}...")
        records = load_oilcrops_csv(oilcrops_file)
        inserted = insert_oilcrops_records(conn, records)
        print(f"  Loaded {inserted:,} oilcrops records")

    # Load Wheat CSVs
    wheat_dir = DATA_DIR / "Wheat"
    if wheat_dir.exists():
        total_wheat = 0
        for csv_file in sorted(wheat_dir.glob("*.csv")):
            print(f"\nLoading {csv_file.name}...")
            records = load_wheat_csv(csv_file)
            inserted = insert_wheat_records(conn, records)
            total_wheat += inserted
            print(f"  Loaded {inserted:,} records")
        print(f"\nTotal wheat records loaded: {total_wheat:,}")

    # Validate and summarize
    validate_soybeans(conn)
    show_summary(conn)

    conn.close()
    print("\n" + "=" * 60)
    print("INGESTION COMPLETE")
    print("=" * 60)


def main():
    parser = argparse.ArgumentParser(description="Load ERS data to PostgreSQL")
    parser.add_argument("--preview", action="store_true", help="Preview data without loading")
    parser.add_argument("--load", action="store_true", help="Load data to database")

    args = parser.parse_args()

    if args.preview:
        preview_data()
    elif args.load:
        load_data()
    else:
        print("Usage:")
        print("  python scripts/ingest_ers_data.py --preview  # Preview data")
        print("  python scripts/ingest_ers_data.py --load     # Load to database")


if __name__ == "__main__":
    main()
