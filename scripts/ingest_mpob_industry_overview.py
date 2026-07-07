"""
Ingest MPOB Industry Overview (2016-2024) into bronze.mpob_industry_overview.

Each MPOB report has two tables:
  Table 0: planted area, production, stocks, exports, revenue, imports
  Table 1: prices, OER, FFB yield

Each table shows current_year and prior_year values.
We extract every data point with: year, category, indicator, region, value, unit.
"""

import os
import re
import psycopg2
from docx import Document
from dotenv import load_dotenv

load_dotenv()

# ---------- configuration ----------
YEARS = list(range(2016, 2025))
DOC_PATH = "G:/My Drive/google_docs_to_add/MPOB_Overview_of_Industry_{year}.docx"

DB_LOCAL = dict(host="localhost", port=5432, dbname="rlc_commodities",
                user="postgres", password="")
DB_RDS   = dict(host="rlc-commodities.c16c6wm826t7.us-east-2.rds.amazonaws.com",
                port=5432, dbname="rlc_commodities", user="postgres",
                password=os.getenv("RLC_PG_PASSWORD"), sslmode="require")

# ---------- schema DDL ----------
DDL_TABLE = """
CREATE TABLE IF NOT EXISTS bronze.mpob_industry_overview (
    id              SERIAL PRIMARY KEY,
    data_year       INTEGER      NOT NULL,
    source_year     INTEGER      NOT NULL,
    category        TEXT         NOT NULL,
    indicator       TEXT         NOT NULL,
    region          TEXT,
    value           NUMERIC,
    unit            TEXT         NOT NULL,
    source_file     TEXT         NOT NULL,
    ingested_at     TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

-- useful indexes
CREATE INDEX IF NOT EXISTS idx_mpob_ind_year
    ON bronze.mpob_industry_overview (data_year, category);
CREATE INDEX IF NOT EXISTS idx_mpob_ind_indicator
    ON bronze.mpob_industry_overview (indicator);
"""

DDL_GOLD_VIEW = """
CREATE OR REPLACE VIEW gold.mpob_industry_summary AS
WITH base AS (
    SELECT data_year, category, indicator, region, value, unit
    FROM bronze.mpob_industry_overview
)
SELECT
    data_year,
    category,
    indicator,
    region,
    value,
    unit,
    LAG(value) OVER (
        PARTITION BY category, indicator, region
        ORDER BY data_year
    ) AS prior_year_value,
    CASE
        WHEN LAG(value) OVER (
            PARTITION BY category, indicator, region
            ORDER BY data_year
        ) IS NOT NULL
         AND LAG(value) OVER (
            PARTITION BY category, indicator, region
            ORDER BY data_year
        ) <> 0
        THEN ROUND(
            (value - LAG(value) OVER (
                PARTITION BY category, indicator, region
                ORDER BY data_year
            )) / ABS(LAG(value) OVER (
                PARTITION BY category, indicator, region
                ORDER BY data_year
            )) * 100, 1
        )
    END AS yoy_change_pct
FROM base
ORDER BY data_year, category, indicator, region;
"""

# ---------- helpers ----------

def clean_number(s: str) -> float | None:
    """Parse a number like '5,612,852' or '(39,717)' or '(0.7)' or '2.8 folds'."""
    if not s or s.strip() in ('', '-', 'N/A', 'n/a'):
        return None
    s = s.strip()
    # Remove "folds" etc
    s = re.sub(r'\s*folds?\s*', '', s, flags=re.I)
    negative = False
    if s.startswith('(') and s.endswith(')'):
        negative = True
        s = s[1:-1]
    s = s.replace(',', '').strip()
    try:
        val = float(s)
        return -val if negative else val
    except ValueError:
        return None


def normalize_indicator(raw: str) -> str:
    """Lowercase, collapse whitespace, strip asterisks and footnote refs."""
    s = raw.strip().upper()
    s = re.sub(r'[*]+$', '', s)
    s = re.sub(r'\s*\(\d+\)\s*$', '', s)  # trailing (1) footnotes
    s = re.sub(r'\s*\(P\)\s*', '', s, flags=re.I)  # (P) for preliminary
    s = re.sub(r'\s+', ' ', s).strip()
    return s


def detect_category(header_text: str) -> tuple[str, str]:
    """Return (category, unit) from a section-header row."""
    h = header_text.upper().strip()
    h = re.sub(r'[*]+', '', h).strip()
    h = re.sub(r'\s*\(P\)\s*', '', h, flags=re.I)
    h = re.sub(r'\s+', ' ', h).strip()

    if 'PLANTED AREA' in h:
        return ('planted_area', 'hectares')
    if 'CPO PRODUCTION' in h:
        return ('cpo_production', 'tonnes')
    if 'CLOSING STOCK' in h:
        return ('closing_stocks', 'tonnes')
    if 'TOTAL EXPORT' in h:
        # This is a total row, not a section header
        return (None, None)
    if 'EXPORT REVENUE' in h:
        return ('exports_revenue', 'rm_million')
    if h.startswith('EXPORT') and 'TONNES' in h:
        return ('exports_volume', 'tonnes')
    if 'TOTAL REVENUE' in h:
        return (None, None)
    if 'IMPORT' in h:
        return ('imports', 'tonnes')
    if 'PRICE' in h and 'RM' in h:
        return ('prices', 'rm_per_tonne')
    if 'OER' in h:
        return ('oer', 'percent')
    if 'FFB YIELD' in h:
        return ('ffb_yield', 'tonnes_per_hectare')
    return (None, None)


# Regions for planted area / production / OER / FFB yield
REGIONS = {'MALAYSIA', 'PENINSULAR MALAYSIA', 'SABAH', 'SARAWAK'}

# Normalize indicator names across years (label differences)
INDICATOR_MAP = {
    'OLEOCHEMICALS': 'PALM-BASED OLEOCHEMICALS',
    'FINISHED PRODUCTS': 'PALM-BASED FINISHED PRODUCTS',
    'PALM OIL': 'PALM OIL',
    'PALM KERNEL OIL': 'PALM KERNEL OIL',
    'PALM KERNEL CAKE': 'PALM KERNEL CAKE',
    'PALM-BASED OLEOCHEMICAL': 'PALM-BASED OLEOCHEMICALS',
    'OTHER PALM-BASED PRODUCTS': 'OTHER PALM PRODUCTS',
    'OTHER PALM-BASED PRODUCTS (1)': 'OTHER PALM PRODUCTS',
    'PALM-BASED OLEOCHEMICALS': 'PALM-BASED OLEOCHEMICALS',
    'BIODIESEL': 'BIODIESEL',
    'PALM-BASED FINISHED PRODUCTS': 'PALM-BASED FINISHED PRODUCTS',
}


def parse_docx(year: int) -> list[dict]:
    """Parse one MPOB overview docx and return list of data-point dicts."""
    path = DOC_PATH.format(year=year)
    doc = Document(path)
    source_file = f"MPOB_Overview_of_Industry_{year}.docx"
    records = []

    for table in doc.tables:
        rows = table.rows
        # Detect the two data-year columns from header row
        header_cells = [c.text.strip() for c in rows[0].cells]
        # Columns 1 and 2 hold the year numbers
        current_year = int(header_cells[1])
        prior_year = int(header_cells[2])

        current_category = None
        current_unit = None

        for row in rows[2:]:  # skip 2 header rows
            cells = [c.text.strip() for c in row.cells]
            indicator_raw = cells[0]
            indicator = normalize_indicator(indicator_raw)

            # Check if this row is a section header (all cells identical = merged row)
            if all(c.strip() == cells[0].strip() for c in cells) or \
               (cells[1] == '' and cells[2] == '' and cells[3] == '' and cells[4] == ''):
                cat, unit = detect_category(indicator)
                if cat:
                    current_category = cat
                    current_unit = unit
                continue

            # Skip "Source:" rows
            if indicator.startswith('SOURCE'):
                continue

            if current_category is None:
                continue

            # Determine if this indicator has a region
            region = None
            final_indicator = indicator

            if current_category in ('planted_area', 'cpo_production', 'oer', 'ffb_yield'):
                if indicator in REGIONS:
                    region = indicator
                    # The indicator IS the category name
                    final_indicator = current_category.upper()
                else:
                    final_indicator = indicator

            # For exports/stocks/revenue/imports, normalize labels
            mapped = INDICATOR_MAP.get(indicator)
            if mapped:
                final_indicator = mapped
            elif indicator.startswith('PALM') or indicator.startswith('OTHER'):
                final_indicator = indicator

            # Handle TOTAL rows
            if 'TOTAL EXPORT' in indicator:
                final_indicator = 'TOTAL EXPORTS'
                current_category_save = current_category
            elif 'TOTAL REVENUE' in indicator:
                final_indicator = 'TOTAL REVENUE'
                current_category_save = current_category
            else:
                current_category_save = current_category

            # Parse values for both years
            val_current = clean_number(cells[1])
            val_prior = clean_number(cells[2])

            if val_current is not None:
                records.append(dict(
                    data_year=current_year,
                    source_year=year,
                    category=current_category_save,
                    indicator=final_indicator,
                    region=region,
                    value=val_current,
                    unit=current_unit,
                    source_file=source_file,
                ))
            if val_prior is not None:
                records.append(dict(
                    data_year=prior_year,
                    source_year=year,
                    category=current_category_save,
                    indicator=final_indicator,
                    region=region,
                    value=val_prior,
                    unit=current_unit,
                    source_file=source_file,
                ))

    return records


def deduplicate(all_records: list[dict]) -> list[dict]:
    """
    When the same data point appears in two files (e.g. 2023 data appears in
    both the 2023 and 2024 reports), keep the one from the report whose
    source_year == data_year (the "primary" report). If neither matches,
    keep the latest source_year.
    """
    best = {}
    for r in all_records:
        key = (r['data_year'], r['category'], r['indicator'], r['region'])
        if key not in best:
            best[key] = r
        else:
            existing = best[key]
            # Prefer source_year == data_year
            r_is_primary = r['source_year'] == r['data_year']
            e_is_primary = existing['source_year'] == existing['data_year']
            if r_is_primary and not e_is_primary:
                best[key] = r
            elif not r_is_primary and not e_is_primary:
                if r['source_year'] > existing['source_year']:
                    best[key] = r
    return list(best.values())


def setup_db(conn):
    """Create table and gold view."""
    with conn.cursor() as cur:
        cur.execute("CREATE SCHEMA IF NOT EXISTS bronze;")
        cur.execute("CREATE SCHEMA IF NOT EXISTS gold;")
        cur.execute(DDL_TABLE)
        cur.execute(DDL_GOLD_VIEW)
    conn.commit()


def insert_records(conn, records: list[dict]):
    """Truncate + bulk insert."""
    with conn.cursor() as cur:
        cur.execute("TRUNCATE TABLE bronze.mpob_industry_overview;")
        insert_sql = """
            INSERT INTO bronze.mpob_industry_overview
                (data_year, source_year, category, indicator, region, value, unit, source_file)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """
        for r in records:
            cur.execute(insert_sql, (
                r['data_year'], r['source_year'], r['category'], r['indicator'],
                r['region'], r['value'], r['unit'], r['source_file']
            ))
    conn.commit()


def verify(conn):
    """Print summary stats."""
    with conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM bronze.mpob_industry_overview;")
        total = cur.fetchone()[0]

        cur.execute("""
            SELECT data_year, COUNT(*)
            FROM bronze.mpob_industry_overview
            GROUP BY data_year ORDER BY data_year;
        """)
        by_year = cur.fetchall()

        cur.execute("""
            SELECT category, COUNT(*)
            FROM bronze.mpob_industry_overview
            GROUP BY category ORDER BY category;
        """)
        by_cat = cur.fetchall()

        cur.execute("""
            SELECT data_year, category, indicator, region, value, unit
            FROM gold.mpob_industry_summary
            WHERE data_year = 2024
            ORDER BY category, indicator, region
            LIMIT 15;
        """)
        gold_sample = cur.fetchall()

    print(f"\n  Total rows: {total}")
    print(f"\n  By data_year:")
    for yr, cnt in by_year:
        print(f"    {yr}: {cnt}")
    print(f"\n  By category:")
    for cat, cnt in by_cat:
        print(f"    {cat}: {cnt}")
    print(f"\n  Gold view sample (2024):")
    for row in gold_sample:
        print(f"    {row}")


def main():
    # 1) Parse all files
    print("Parsing MPOB docx files...")
    all_records = []
    for year in YEARS:
        recs = parse_docx(year)
        print(f"  {year}: {len(recs)} raw records")
        all_records.extend(recs)
    print(f"Total raw records: {len(all_records)}")

    # 2) Deduplicate (prefer primary source year)
    deduped = deduplicate(all_records)
    print(f"After deduplication: {len(deduped)} records")

    # 3) Load into localhost
    print("\n--- Loading into LOCALHOST ---")
    conn_local = psycopg2.connect(**DB_LOCAL)
    try:
        setup_db(conn_local)
        insert_records(conn_local, deduped)
        verify(conn_local)
    finally:
        conn_local.close()

    # 4) Load into RDS
    print("\n--- Loading into RDS ---")
    conn_rds = psycopg2.connect(**DB_RDS)
    try:
        setup_db(conn_rds)
        insert_records(conn_rds, deduped)
        verify(conn_rds)
    finally:
        conn_rds.close()

    print("\nDone!")


if __name__ == "__main__":
    main()
