#!/usr/bin/env python3
"""
Ingest ALL wheat data files into bronze.wheat_all table.
This creates a comprehensive wheat database with all ERS data.
"""

import os
import csv
import psycopg2
from psycopg2.extras import execute_values
from pathlib import Path

# Database connection
DB_CONFIG = {
    'host': 'localhost',
    'database': 'rlc_commodities',
    'user': 'postgres',
    'password': 'postgres'
}

WHEAT_DIR = Path('Models/Data/Wheat')

# Create table SQL - comprehensive structure to handle all file types
CREATE_TABLE_SQL = """
DROP TABLE IF EXISTS bronze.wheat_all CASCADE;

CREATE TABLE bronze.wheat_all (
    id SERIAL PRIMARY KEY,
    source_file TEXT,
    commodity_desc TEXT,
    commodity_desc2 TEXT,
    attribute_desc TEXT,
    attribute_desc2 TEXT,
    geography_desc TEXT,
    unit_desc TEXT,
    marketing_year TEXT,
    fiscal_year TEXT,
    calendar_year TEXT,
    timeperiod_desc TEXT,
    amount NUMERIC,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(source_file, commodity_desc, commodity_desc2, attribute_desc, attribute_desc2,
           geography_desc, unit_desc, marketing_year, fiscal_year, calendar_year, timeperiod_desc)
);

CREATE INDEX idx_wheat_all_commodity ON bronze.wheat_all(commodity_desc);
CREATE INDEX idx_wheat_all_attribute ON bronze.wheat_all(attribute_desc);
CREATE INDEX idx_wheat_all_marketing_year ON bronze.wheat_all(marketing_year);
CREATE INDEX idx_wheat_all_geography ON bronze.wheat_all(geography_desc);
"""

def normalize_header(header):
    """Normalize column header names."""
    header = header.strip().lower().replace(' ', '_').replace('-', '_')
    # Fix known issues
    if header == '26':  # Bad header in file 26
        return 'commodity_desc'
    return header

def read_csv_file(filepath):
    """Read a CSV file and return normalized records."""
    records = []
    source_file = filepath.name

    with open(filepath, 'r', encoding='utf-8-sig') as f:
        reader = csv.DictReader(f)
        # Normalize headers
        reader.fieldnames = [normalize_header(h) for h in reader.fieldnames]

        for row in reader:
            record = {
                'source_file': source_file,
                'commodity_desc': row.get('commodity_desc', '').strip('"'),
                'commodity_desc2': row.get('commodity_desc2', '').strip('"') if row.get('commodity_desc2') else None,
                'attribute_desc': row.get('attribute_desc', '').strip('"') if row.get('attribute_desc') else None,
                'attribute_desc2': row.get('attribute_desc2', '').strip('"') if row.get('attribute_desc2') else None,
                'geography_desc': row.get('geography_desc', '').strip('"') if row.get('geography_desc') else None,
                'unit_desc': row.get('unit_desc', '').strip('"') if row.get('unit_desc') else None,
                'marketing_year': row.get('marketing_year', '').strip('"') if row.get('marketing_year') else None,
                'fiscal_year': row.get('fiscal_year', '').strip('"') if row.get('fiscal_year') else None,
                'calendar_year': row.get('calendar_year', '').strip('"') if row.get('calendar_year') else None,
                'timeperiod_desc': row.get('timeperiod_desc', '').strip('"') if row.get('timeperiod_desc') else None,
                'amount': None
            }

            # Parse amount
            amount_str = row.get('amount', '').strip('"').replace(',', '')
            if amount_str and amount_str not in ('', 'NA', 'N/A', '--', '.'):
                try:
                    record['amount'] = float(amount_str)
                except ValueError:
                    record['amount'] = None

            records.append(record)

    return records

def insert_records(conn, records):
    """Insert records into database with deduplication."""
    if not records:
        return 0

    # Deduplicate by key
    seen = {}
    for r in records:
        key = (r['source_file'], r['commodity_desc'], r['commodity_desc2'], r['attribute_desc'],
               r['attribute_desc2'], r['geography_desc'], r['unit_desc'], r['marketing_year'],
               r['fiscal_year'], r['calendar_year'], r['timeperiod_desc'])
        seen[key] = r
    unique_records = list(seen.values())

    insert_sql = """
        INSERT INTO bronze.wheat_all
        (source_file, commodity_desc, commodity_desc2, attribute_desc, attribute_desc2,
         geography_desc, unit_desc, marketing_year, fiscal_year, calendar_year, timeperiod_desc, amount)
        VALUES %s
        ON CONFLICT (source_file, commodity_desc, commodity_desc2, attribute_desc, attribute_desc2,
                     geography_desc, unit_desc, marketing_year, fiscal_year, calendar_year, timeperiod_desc)
        DO UPDATE SET amount = EXCLUDED.amount
    """

    values = [
        (r['source_file'], r['commodity_desc'], r['commodity_desc2'], r['attribute_desc'],
         r['attribute_desc2'], r['geography_desc'], r['unit_desc'], r['marketing_year'],
         r['fiscal_year'], r['calendar_year'], r['timeperiod_desc'], r['amount'])
        for r in unique_records
    ]

    with conn.cursor() as cur:
        execute_values(cur, insert_sql, values, page_size=1000)

    conn.commit()
    return len(unique_records)

def main():
    print("=" * 60)
    print("WHEAT DATA INGESTION - ALL FILES")
    print("=" * 60)

    conn = psycopg2.connect(**DB_CONFIG)

    # Create table
    print("\nCreating bronze.wheat_all table...")
    with conn.cursor() as cur:
        cur.execute(CREATE_TABLE_SQL)
    conn.commit()
    print("Table created.")

    # Process each CSV file
    total_records = 0
    csv_files = sorted(WHEAT_DIR.glob('*.csv'))

    for filepath in csv_files:
        print(f"\nProcessing: {filepath.name}")
        try:
            records = read_csv_file(filepath)
            count = insert_records(conn, records)
            print(f"  Inserted: {count:,} records")
            total_records += count
        except Exception as e:
            print(f"  ERROR: {e}")

    print("\n" + "=" * 60)
    print(f"TOTAL RECORDS INSERTED: {total_records:,}")
    print("=" * 60)

    # Show summary by file
    print("\nSummary by source file:")
    with conn.cursor() as cur:
        cur.execute("""
            SELECT source_file, COUNT(*) as records
            FROM bronze.wheat_all
            GROUP BY source_file
            ORDER BY source_file
        """)
        for row in cur.fetchall():
            print(f"  {row[0]}: {row[1]:,}")

    # Show sample data - acreage
    print("\nSample: US Wheat Acreage 2024/25")
    with conn.cursor() as cur:
        cur.execute("""
            SELECT attribute_desc, attribute_desc2, amount, unit_desc
            FROM bronze.wheat_all
            WHERE commodity_desc = 'Wheat'
              AND commodity_desc2 = 'All wheat'
              AND marketing_year = '2024/25'
              AND geography_desc = 'United States'
              AND attribute_desc LIKE 'Crops%'
            ORDER BY attribute_desc
        """)
        for row in cur.fetchall():
            print(f"  {row[0]} - {row[1]}: {row[2]} {row[3]}")

    conn.close()
    print("\nDone!")

if __name__ == '__main__':
    main()
