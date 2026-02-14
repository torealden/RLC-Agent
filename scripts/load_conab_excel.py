#!/usr/bin/env python3
"""
CONAB Excel Data Loader
========================
Loads historical CONAB data from Excel files into the database.

Handles two types of files:
1. Historical production series (Nov 25 files): Área, Produtividade, Produção sheets
   - Rows are states/regions
   - Columns are crop years (1976/77 to 2025/26)

2. Supply/Demand files (Jan 2026): Suprimento sheets
   - Rows are commodities with S&D items
   - Columns are crop years

Usage:
    python scripts/load_conab_excel.py                    # Load all files
    python scripts/load_conab_excel.py --commodity soy    # Load only soy
    python scripts/load_conab_excel.py --file "CONAB - Jan 2026.xlsx"  # Specific file
"""

import os
import sys
import logging
import uuid
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Tuple, Any

# Setup paths
PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))
sys.path.insert(0, str(PROJECT_ROOT / "src"))

from dotenv import load_dotenv
load_dotenv()

import pandas as pd
import psycopg2
from psycopg2.extras import execute_values

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('CONABLoader')

# Database connection
DB_CONFIG = {
    'host': os.getenv('DB_HOST', 'localhost'),
    'port': int(os.getenv('DB_PORT', '5432')),
    'database': os.getenv('DB_NAME', 'rlc_commodities'),
    'user': os.getenv('DB_USER', 'postgres'),
    'password': os.getenv('DB_PASSWORD', '')
}

# CONAB Excel files configuration
DATA_DIR = PROJECT_ROOT / "data" / "raw"

CONAB_FILES = {
    'soybeans': {
        'file': 'CONAB - Soy - Nov 25.xlsx',
        'commodity': 'soybeans',
        'commodity_pt': 'soja',
        'crop_type': ''
    },
    'corn_total': {
        'file': 'CONAB - Corn Totals - Nov 25.xlsx',
        'commodity': 'corn',
        'commodity_pt': 'milho',
        'crop_type': ''
    },
    'corn_1st': {
        'file': 'CONAB - Corn 1st Crop - Nov 25.xlsx',
        'commodity': 'corn_first_crop',
        'commodity_pt': 'milho_1_safra',
        'crop_type': 'first_crop'
    },
    'corn_2nd': {
        'file': 'CONAB - Corn 2nd Crop - Nov 25.xlsx',
        'commodity': 'corn_safrinha',
        'commodity_pt': 'milho_2_safra',
        'crop_type': 'safrinha'
    },
    'corn_3rd': {
        'file': 'CONAB - Corn 3rd Crop - Nov 25.xlsx',
        'commodity': 'corn_third_crop',
        'commodity_pt': 'milho_3_safra',
        'crop_type': 'third_crop'
    },
}

# State code mapping (Brazilian states)
STATE_CODES = {
    'RR': 'Roraima', 'RO': 'Rondônia', 'AC': 'Acre', 'AM': 'Amazonas',
    'PA': 'Pará', 'AP': 'Amapá', 'TO': 'Tocantins',
    'MA': 'Maranhão', 'PI': 'Piauí', 'CE': 'Ceará', 'RN': 'Rio Grande do Norte',
    'PB': 'Paraíba', 'PE': 'Pernambuco', 'AL': 'Alagoas', 'SE': 'Sergipe', 'BA': 'Bahia',
    'MG': 'Minas Gerais', 'ES': 'Espírito Santo', 'RJ': 'Rio de Janeiro', 'SP': 'São Paulo',
    'PR': 'Paraná', 'SC': 'Santa Catarina', 'RS': 'Rio Grande do Sul',
    'MT': 'Mato Grosso', 'MS': 'Mato Grosso do Sul', 'GO': 'Goiás', 'DF': 'Distrito Federal',
}

# Region names in Portuguese/English
REGIONS = {
    'NORTE': 'Norte',
    'NORDESTE': 'Nordeste',
    'CENTRO-OESTE': 'Centro-Oeste',
    'SUDESTE': 'Sudeste',
    'SUL': 'Sul',
    'BRASIL': 'BRASIL',
    'NORTH': 'Norte',
    'NORTHEAST': 'Nordeste',
    'CENTER-WEST': 'Centro-Oeste',
    'SOUTHEAST': 'Sudeste',
    'SOUTH': 'Sul',
}


def normalize_crop_year(value: str) -> str:
    """Normalize crop year format to YYYY/YY."""
    if not value:
        return None
    value = str(value).strip()

    # Already in YYYY/YY format
    if '/' in value and len(value) == 7:
        return value

    # Handle variations like 2024/2025 -> 2024/25
    if '/' in value and len(value) == 9:
        parts = value.split('/')
        return f"{parts[0]}/{parts[1][2:]}"

    return value


def normalize_state(value: str) -> str:
    """Normalize state/region name."""
    if not value:
        return None
    value = str(value).strip().upper()

    # Check if it's a state code
    if value in STATE_CODES:
        return value

    # Check if it's a region
    if value in REGIONS:
        return REGIONS[value]

    # Check if it matches a state name
    for code, name in STATE_CODES.items():
        if name.upper() == value or name.upper().replace('Ã', 'A').replace('Í', 'I') == value:
            return code

    # Return as-is for BRASIL or unknown
    if 'BRASIL' in value or 'BRAZIL' in value:
        return 'BRASIL'

    return value


def parse_number(value: Any) -> Optional[float]:
    """Parse a number, handling Brazilian format and text values."""
    if value is None or pd.isna(value):
        return None

    if isinstance(value, (int, float)):
        return float(value) if not pd.isna(value) else None

    value = str(value).strip()
    if value in ['', '-', '...', 'nd', 'n.d.', 'n/a']:
        return None

    # Handle Brazilian number format (1.234,56 -> 1234.56)
    # First remove thousand separators, then replace decimal
    try:
        # Check if it uses comma as decimal separator
        if ',' in value and '.' in value:
            # Brazilian format: 1.234,56
            value = value.replace('.', '').replace(',', '.')
        elif ',' in value:
            # Just comma as decimal
            value = value.replace(',', '.')

        return float(value)
    except ValueError:
        logger.warning(f"Could not parse number: {value}")
        return None


def get_db_connection():
    """Get database connection."""
    return psycopg2.connect(**DB_CONFIG)


def load_production_file(
    file_path: Path,
    commodity: str,
    commodity_pt: str,
    crop_type: str = '',
    ingest_run_id: str = None
) -> Dict[str, int]:
    """
    Load a CONAB historical production Excel file.

    These files have:
    - Sheet 'Área' (planted area in 1000 ha)
    - Sheet 'Produtividade' (yield in kg/ha)
    - Sheet 'Produção' (production in 1000 t)

    Rows are states/regions, columns are crop years.
    """
    if not file_path.exists():
        logger.warning(f"File not found: {file_path}")
        return {'inserted': 0, 'updated': 0, 'skipped': 0}

    logger.info(f"Loading {file_path.name} for {commodity}")

    ingest_run_id = ingest_run_id or str(uuid.uuid4())

    # Read all sheets
    try:
        xl = pd.ExcelFile(file_path)
        sheets = xl.sheet_names
        logger.info(f"  Sheets found: {sheets}")
    except Exception as e:
        logger.error(f"Error reading Excel file: {e}")
        return {'inserted': 0, 'updated': 0, 'skipped': 0, 'error': str(e)}

    # Map sheet names to data types
    sheet_map = {}
    for sheet in sheets:
        sheet_lower = sheet.lower()
        if 'área' in sheet_lower or 'area' in sheet_lower:
            sheet_map['area'] = sheet
        elif 'produtividade' in sheet_lower or 'yield' in sheet_lower:
            sheet_map['yield'] = sheet
        elif 'produção' in sheet_lower or 'producao' in sheet_lower or 'production' in sheet_lower:
            sheet_map['production'] = sheet

    if not sheet_map:
        logger.warning(f"No recognized sheets in {file_path.name}")
        return {'inserted': 0, 'updated': 0, 'skipped': 0}

    # Parse each data sheet
    data_by_state_year = {}  # {(state, crop_year): {area, yield, production}}

    for data_type, sheet_name in sheet_map.items():
        logger.info(f"  Processing sheet: {sheet_name} ({data_type})")

        try:
            # Read sheet without header first to find the right row
            df_raw = pd.read_excel(file_path, sheet_name=sheet_name, header=None, nrows=10)

            # Find the header row (contains REGIÃO/UF or similar)
            header_row = 5  # Default
            for i in range(10):
                first_cell = str(df_raw.iloc[i, 0]).upper()
                if 'REGI' in first_cell or 'UF' in first_cell:
                    header_row = i
                    break

            # Re-read with correct header row
            df = pd.read_excel(file_path, sheet_name=sheet_name, header=header_row)

            # First column is state/region
            orig_cols = list(df.columns)
            df.columns = ['state'] + list(df.columns[1:])

            # Identify crop year columns (format like 1976/77 or columns containing /)
            crop_year_cols = []
            for col in df.columns[1:]:
                col_str = str(col).strip()
                # Match crop year format: YYYY/YY
                if '/' in col_str and len(col_str) >= 6:
                    try:
                        # Validate it looks like a crop year
                        parts = col_str.split('/')
                        if len(parts) == 2 and parts[0].isdigit() and parts[1][:2].isdigit():
                            crop_year_cols.append(col)
                    except:
                        pass

            logger.info(f"    Found {len(crop_year_cols)} crop year columns")

            # Process each row
            for idx, row in df.iterrows():
                state_raw = row.get('state', '')
                state = normalize_state(state_raw)

                if not state or state in ['nan', 'None', '']:
                    continue

                # Skip header-like rows
                if 'REGIÃO' in str(state).upper() or 'UF' in str(state).upper():
                    continue

                for col in crop_year_cols:
                    crop_year = normalize_crop_year(str(col))
                    if not crop_year:
                        continue

                    value = parse_number(row.get(col))
                    if value is None:
                        continue

                    key = (state, crop_year)
                    if key not in data_by_state_year:
                        data_by_state_year[key] = {}

                    if data_type == 'area':
                        data_by_state_year[key]['planted_area_1000ha'] = value
                    elif data_type == 'yield':
                        data_by_state_year[key]['yield_kg_ha'] = value
                    elif data_type == 'production':
                        data_by_state_year[key]['production_1000t'] = value

        except Exception as e:
            logger.error(f"Error processing sheet {sheet_name}: {e}")
            continue

    # Insert data into database
    if not data_by_state_year:
        logger.warning(f"No data parsed from {file_path.name}")
        return {'inserted': 0, 'updated': 0, 'skipped': 0}

    conn = get_db_connection()
    cur = conn.cursor()

    inserted = 0
    updated = 0
    skipped = 0

    try:
        for (state, crop_year), values in data_by_state_year.items():
            if not values:
                skipped += 1
                continue

            # Upsert into bronze.conab_production
            sql = """
                INSERT INTO bronze.conab_production (
                    crop_year, state, commodity, commodity_pt, crop_type,
                    planted_area_1000ha, production_1000t, yield_kg_ha,
                    source, ingest_run_id, created_at, updated_at
                ) VALUES (
                    %s, %s, %s, %s, %s,
                    %s, %s, %s,
                    'CONAB', %s, NOW(), NOW()
                )
                ON CONFLICT ON CONSTRAINT conab_production_natural_key
                DO UPDATE SET
                    planted_area_1000ha = COALESCE(EXCLUDED.planted_area_1000ha, bronze.conab_production.planted_area_1000ha),
                    production_1000t = COALESCE(EXCLUDED.production_1000t, bronze.conab_production.production_1000t),
                    yield_kg_ha = COALESCE(EXCLUDED.yield_kg_ha, bronze.conab_production.yield_kg_ha),
                    ingest_run_id = EXCLUDED.ingest_run_id,
                    updated_at = NOW()
                RETURNING (xmax = 0) as inserted
            """

            cur.execute(sql, (
                crop_year,
                state,
                commodity,
                commodity_pt,
                crop_type,
                values.get('planted_area_1000ha'),
                values.get('production_1000t'),
                values.get('yield_kg_ha'),
                ingest_run_id
            ))

            result = cur.fetchone()
            if result and result[0]:
                inserted += 1
            else:
                updated += 1

        conn.commit()
        logger.info(f"  Loaded {inserted} new, {updated} updated, {skipped} skipped records")

    except Exception as e:
        conn.rollback()
        logger.error(f"Database error: {e}")
        return {'inserted': inserted, 'updated': updated, 'skipped': skipped, 'error': str(e)}
    finally:
        cur.close()
        conn.close()

    return {'inserted': inserted, 'updated': updated, 'skipped': skipped}


def load_supply_demand_file(
    file_path: Path,
    ingest_run_id: str = None
) -> Dict[str, int]:
    """
    Load CONAB Supply/Demand Excel file (Jan 2026 format).

    These files have:
    - Sheet 'Suprimento' with overall S&D
    - Sheet 'Suprimento - Soja' with soy-specific S&D

    Format:
    - Row 4: Headers (PRODUTO, SAFRA, ?, ESTOQUE INICIAL, PRODUÇÃO, ...)
    - Rows 5+: Data with commodity in first column (only for first row of group)
    """
    if not file_path.exists():
        logger.warning(f"File not found: {file_path}")
        return {'inserted': 0, 'updated': 0, 'skipped': 0}

    logger.info(f"Loading supply/demand from {file_path.name}")

    ingest_run_id = ingest_run_id or str(uuid.uuid4())

    # Read Excel file
    try:
        xl = pd.ExcelFile(file_path)
        sheets = xl.sheet_names
        logger.info(f"  Sheets found: {sheets}")
    except Exception as e:
        logger.error(f"Error reading Excel file: {e}")
        return {'inserted': 0, 'updated': 0, 'skipped': 0, 'error': str(e)}

    # Commodity mapping (Portuguese to English)
    COMMODITY_MAP = {
        'SOJA': 'soybeans',
        'MILHO': 'corn',
        'MILHO TOTAL': 'corn',
        'TRIGO': 'wheat',
        'ARROZ': 'rice',
        'ARROZ EM CASCA': 'rice',
        'ALGODAO': 'cotton',
        'ALGODÃO': 'cotton',
        'FEIJAO': 'beans',
        'FEIJÃO': 'beans',
        'SORGO': 'sorghum',
    }

    all_records = []

    for sheet_name in sheets:
        if 'suprimento' not in sheet_name.lower():
            continue

        logger.info(f"  Processing sheet: {sheet_name}")

        try:
            # Read the sheet with header on row 4 (0-indexed)
            df = pd.read_excel(file_path, sheet_name=sheet_name, header=4)

            # Get column names and normalize them
            cols = list(df.columns)
            logger.info(f"    Columns: {cols[:10]}...")

            # Find the column indices for each S&D item
            col_map = {}
            for i, col in enumerate(cols):
                col_str = str(col).upper().replace('\n', ' ').strip()
                if 'ESTOQUE' in col_str and 'INICIAL' in col_str:
                    col_map['beginning_stocks'] = i
                elif 'PRODU' in col_str and 'ESTOQUE' not in col_str:
                    col_map['production'] = i
                elif 'IMPORT' in col_str:
                    col_map['imports'] = i
                elif 'SUPRIMENTO' in col_str:
                    col_map['total_supply'] = i
                elif 'CONSUMO' in col_str:
                    col_map['consumption'] = i
                elif 'EXPORT' in col_str:
                    col_map['exports'] = i
                elif 'DEMANDA' in col_str:
                    col_map['total_demand'] = i
                elif 'ESTOQUE' in col_str and 'FINAL' in col_str:
                    col_map['ending_stocks'] = i

            logger.info(f"    Mapped columns: {col_map}")

            current_commodity = None

            for idx, row in df.iterrows():
                # Get commodity from first column
                produto_raw = row.iloc[0] if not pd.isna(row.iloc[0]) else ''
                produto = str(produto_raw).upper().replace('\n', ' ').strip()

                # Check if this row has a commodity name
                for pt_name, en_name in COMMODITY_MAP.items():
                    if pt_name in produto:
                        current_commodity = en_name
                        break

                # For soja-specific sheet, default to soybeans
                if not current_commodity and 'SOJA' in sheet_name.upper():
                    current_commodity = 'soybeans'

                if not current_commodity:
                    continue

                # Get crop year from second column (SAFRA)
                safra = str(row.iloc[1]).strip() if len(row) > 1 and not pd.isna(row.iloc[1]) else ''
                crop_year = normalize_crop_year(safra)

                if not crop_year:
                    continue

                # Extract S&D values
                for item_type, col_idx in col_map.items():
                    try:
                        value = parse_number(row.iloc[col_idx])
                        if value is not None:
                            all_records.append({
                                'crop_year': crop_year,
                                'commodity': current_commodity,
                                'item_type': item_type,
                                'value_1000t': value,
                                'raw_item_name': str(cols[col_idx]),
                            })
                    except (IndexError, KeyError):
                        pass

        except Exception as e:
            logger.error(f"Error processing sheet {sheet_name}: {e}")
            import traceback
            traceback.print_exc()
            continue

    if not all_records:
        logger.warning(f"No S&D data parsed from {file_path.name}")
        return {'inserted': 0, 'updated': 0, 'skipped': 0}

    # Insert into database
    conn = get_db_connection()
    cur = conn.cursor()

    inserted = 0
    updated = 0

    try:
        for record in all_records:
            sql = """
                INSERT INTO bronze.conab_supply_demand (
                    crop_year, commodity, item_type, value_1000t,
                    raw_item_name, source, ingest_run_id, created_at, updated_at
                ) VALUES (
                    %s, %s, %s, %s, %s, 'CONAB', %s, NOW(), NOW()
                )
                ON CONFLICT (crop_year, commodity, item_type)
                DO UPDATE SET
                    value_1000t = EXCLUDED.value_1000t,
                    raw_item_name = EXCLUDED.raw_item_name,
                    ingest_run_id = EXCLUDED.ingest_run_id,
                    updated_at = NOW()
                RETURNING (xmax = 0) as inserted
            """

            cur.execute(sql, (
                record['crop_year'],
                record['commodity'],
                record['item_type'],
                record['value_1000t'],
                record['raw_item_name'],
                ingest_run_id
            ))

            result = cur.fetchone()
            if result and result[0]:
                inserted += 1
            else:
                updated += 1

        conn.commit()
        logger.info(f"  Loaded {inserted} new, {updated} updated S&D records")

    except Exception as e:
        conn.rollback()
        logger.error(f"Database error: {e}")
        return {'inserted': inserted, 'updated': updated, 'error': str(e)}
    finally:
        cur.close()
        conn.close()

    return {'inserted': inserted, 'updated': updated}


def transform_to_silver():
    """Transform bronze CONAB data to silver layer."""
    logger.info("Transforming CONAB data to silver layer...")

    conn = get_db_connection()
    cur = conn.cursor()

    try:
        # Transform production data
        sql = """
            INSERT INTO silver.conab_production (
                crop_year, state, commodity, crop_type,
                planted_area_ha, harvested_area_ha, production_mt, production_mmt,
                yield_kg_ha, yield_mt_ha,
                quality_flag, data_source, created_at, updated_at
            )
            SELECT
                crop_year,
                state,
                commodity,
                crop_type,
                planted_area_1000ha * 1000 as planted_area_ha,
                harvested_area_1000ha * 1000 as harvested_area_ha,
                production_1000t * 1000 as production_mt,
                production_1000t / 1000 as production_mmt,
                yield_kg_ha,
                yield_kg_ha / 1000 as yield_mt_ha,
                'OK' as quality_flag,
                'CONAB' as data_source,
                NOW() as created_at,
                NOW() as updated_at
            FROM bronze.conab_production
            WHERE production_1000t IS NOT NULL
               OR planted_area_1000ha IS NOT NULL
            ON CONFLICT ON CONSTRAINT silver_conab_production_natural_key
            DO UPDATE SET
                planted_area_ha = EXCLUDED.planted_area_ha,
                harvested_area_ha = EXCLUDED.harvested_area_ha,
                production_mt = EXCLUDED.production_mt,
                production_mmt = EXCLUDED.production_mmt,
                yield_kg_ha = EXCLUDED.yield_kg_ha,
                yield_mt_ha = EXCLUDED.yield_mt_ha,
                updated_at = NOW()
        """
        cur.execute(sql)
        production_count = cur.rowcount
        logger.info(f"  Silver production: {production_count} records updated")

        # Transform S&D to balance sheet (pivot from item_type rows to columns)
        sql = """
            INSERT INTO silver.conab_balance_sheet (
                crop_year, commodity,
                beginning_stocks_mt, production_mt, imports_mt, total_supply_mt,
                domestic_consumption_mt, exports_mt, total_use_mt,
                ending_stocks_mt,
                quality_flag, data_source, created_at, updated_at
            )
            SELECT
                crop_year,
                commodity,
                MAX(CASE WHEN item_type = 'beginning_stocks' THEN value_1000t * 1000 END) as beginning_stocks_mt,
                MAX(CASE WHEN item_type = 'production' THEN value_1000t * 1000 END) as production_mt,
                MAX(CASE WHEN item_type = 'imports' THEN value_1000t * 1000 END) as imports_mt,
                MAX(CASE WHEN item_type = 'total_supply' THEN value_1000t * 1000 END) as total_supply_mt,
                MAX(CASE WHEN item_type = 'consumption' THEN value_1000t * 1000 END) as domestic_consumption_mt,
                MAX(CASE WHEN item_type = 'exports' THEN value_1000t * 1000 END) as exports_mt,
                MAX(CASE WHEN item_type = 'total_demand' THEN value_1000t * 1000 END) as total_use_mt,
                MAX(CASE WHEN item_type = 'ending_stocks' THEN value_1000t * 1000 END) as ending_stocks_mt,
                'OK' as quality_flag,
                'CONAB' as data_source,
                NOW() as created_at,
                NOW() as updated_at
            FROM bronze.conab_supply_demand
            GROUP BY crop_year, commodity
            ON CONFLICT (crop_year, commodity)
            DO UPDATE SET
                beginning_stocks_mt = EXCLUDED.beginning_stocks_mt,
                production_mt = EXCLUDED.production_mt,
                imports_mt = EXCLUDED.imports_mt,
                total_supply_mt = EXCLUDED.total_supply_mt,
                domestic_consumption_mt = EXCLUDED.domestic_consumption_mt,
                exports_mt = EXCLUDED.exports_mt,
                total_use_mt = EXCLUDED.total_use_mt,
                ending_stocks_mt = EXCLUDED.ending_stocks_mt,
                updated_at = NOW()
        """
        cur.execute(sql)
        balance_count = cur.rowcount
        logger.info(f"  Silver balance sheet: {balance_count} records updated")

        conn.commit()

    except Exception as e:
        conn.rollback()
        logger.error(f"Silver transformation error: {e}")
        import traceback
        traceback.print_exc()
    finally:
        cur.close()
        conn.close()


def main():
    """Main entry point."""
    import argparse

    parser = argparse.ArgumentParser(description='Load CONAB Excel data')
    parser.add_argument('--commodity', choices=['soy', 'corn', 'all'], default='all',
                        help='Which commodity to load')
    parser.add_argument('--file', help='Specific file to load')
    parser.add_argument('--skip-historical', action='store_true',
                        help='Skip historical production files')
    parser.add_argument('--skip-sd', action='store_true',
                        help='Skip supply/demand files')
    parser.add_argument('--skip-silver', action='store_true',
                        help='Skip silver layer transformation')

    args = parser.parse_args()

    ingest_run_id = str(uuid.uuid4())
    logger.info(f"Starting CONAB data load (run_id: {ingest_run_id})")

    results = {}

    # Load historical production files
    if not args.skip_historical:
        if args.commodity in ['soy', 'all']:
            config = CONAB_FILES['soybeans']
            file_path = DATA_DIR / config['file']
            results['soybeans'] = load_production_file(
                file_path, config['commodity'], config['commodity_pt'],
                config['crop_type'], ingest_run_id
            )

        if args.commodity in ['corn', 'all']:
            for key in ['corn_total', 'corn_1st', 'corn_2nd', 'corn_3rd']:
                config = CONAB_FILES[key]
                file_path = DATA_DIR / config['file']
                results[key] = load_production_file(
                    file_path, config['commodity'], config['commodity_pt'],
                    config['crop_type'], ingest_run_id
                )

    # Load supply/demand file
    if not args.skip_sd:
        sd_file = DATA_DIR / 'CONAB - Jan 2026.xlsx'
        if sd_file.exists():
            results['supply_demand'] = load_supply_demand_file(sd_file, ingest_run_id)

    # Transform to silver
    if not args.skip_silver:
        transform_to_silver()

    # Summary
    logger.info("=" * 60)
    logger.info("CONAB Data Load Summary")
    logger.info("=" * 60)

    total_inserted = 0
    total_updated = 0

    for name, result in results.items():
        inserted = result.get('inserted', 0)
        updated = result.get('updated', 0)
        total_inserted += inserted
        total_updated += updated
        logger.info(f"  {name}: {inserted} inserted, {updated} updated")

    logger.info(f"Total: {total_inserted} inserted, {total_updated} updated")
    logger.info("=" * 60)


if __name__ == '__main__':
    main()
