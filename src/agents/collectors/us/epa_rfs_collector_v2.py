"""
EPA RFS (Renewable Fuel Standard) Data Collector V2
====================================================
Enhanced collector that loads RIN data from EPA CSV files.

Data sources:
- RIN Generation Breakout: generationbreakout_[mon][year].csv
- RIN Separation: separation data files
- RIN Retirement: retirement data files
- Available RINs: quarterly snapshots

Note: EPA does not provide a REST API. Data must be downloaded as CSV files
from: https://www.epa.gov/fuels-registration-reporting-and-compliance-help/public-data-renewable-fuel-standard

URL Pattern for generation files:
    https://www.epa.gov/system/files/other-files/YYYY-MM/generationbreakout_monYYYY.csv
    Example: https://www.epa.gov/system/files/other-files/2026-01/generationbreakout_dec2025.csv
"""

import csv
import json
import logging
import os
import re
from dataclasses import dataclass, field
from datetime import datetime, date
from pathlib import Path
from typing import Dict, List, Optional, Any, Tuple

import requests
from dotenv import load_dotenv

# Load environment
PROJECT_ROOT = Path(__file__).parent.parent.parent.parent.parent
load_dotenv(PROJECT_ROOT / '.env')

try:
    import pandas as pd
    PANDAS_AVAILABLE = True
except ImportError:
    PANDAS_AVAILABLE = False

logger = logging.getLogger(__name__)


# =============================================================================
# D-CODE REFERENCE
# =============================================================================

D_CODE_INFO = {
    '3': {
        'name': 'Cellulosic Biofuel',
        'ghg_threshold': '>=60%',
        'equivalence_value': 1.0,
        'satisfies': ['cellulosic', 'advanced', 'total'],
    },
    '4': {
        'name': 'Biomass-Based Diesel',
        'ghg_threshold': '>=50%',
        'equivalence_value': 1.6,  # Average of biodiesel (1.5) and RD (1.7)
        'satisfies': ['bbd', 'advanced', 'total'],
    },
    '5': {
        'name': 'Advanced Biofuel',
        'ghg_threshold': '>=50%',
        'equivalence_value': 1.0,
        'satisfies': ['advanced', 'total'],
    },
    '6': {
        'name': 'Renewable Fuel (Conventional)',
        'ghg_threshold': '>=20%',
        'equivalence_value': 1.0,
        'satisfies': ['total'],
    },
    '7': {
        'name': 'Cellulosic Diesel',
        'ghg_threshold': '>=60%',
        'equivalence_value': 1.7,
        'satisfies': ['cellulosic_or_bbd', 'advanced', 'total'],
    },
}


# =============================================================================
# FILE TYPE DEFINITIONS
# =============================================================================

FILE_TYPES = {
    'generation_breakout': {
        'pattern': r'generationbreakout_(\w+)(\d{4})\.csv',
        'url_template': 'https://www.epa.gov/system/files/other-files/{year}-{month:02d}/generationbreakout_{mon}{data_year}.csv',
        'columns': ['RIN_YEAR', 'Fuel Code', 'DOMESTIC', 'IMPORTER', 'Foreign Generation', 'Total RINs'],
        'table': 'bronze.epa_rfs_generation',
    },
    'generation_volume': {
        'pattern': r'generationvolume.*\.csv',
        'table': 'bronze.epa_rfs_generation_monthly',
    },
    'separation': {
        'pattern': r'separation.*\.csv',
        'table': 'bronze.epa_rfs_separation',
    },
    'retirement': {
        'pattern': r'retirement.*\.csv',
        'table': 'bronze.epa_rfs_retirement',
    },
    'available': {
        'pattern': r'available.*\.csv',
        'table': 'bronze.epa_rfs_available',
    },
}


# =============================================================================
# CONFIGURATION
# =============================================================================

@dataclass
class EPARFSConfig:
    """EPA RFS collector configuration."""
    source_name: str = "EPA RFS"
    source_url: str = "https://www.epa.gov/fuels-registration-reporting-and-compliance-help/public-data-renewable-fuel-standard"

    # Local data directory
    data_dir: Path = field(
        default_factory=lambda: PROJECT_ROOT / 'data' / 'raw' / 'rfs_data'
    )

    # Database
    db_host: str = field(default_factory=lambda: os.environ.get('DB_HOST', 'localhost'))
    db_port: int = field(default_factory=lambda: int(os.environ.get('DB_PORT', '5432')))
    db_name: str = field(default_factory=lambda: os.environ.get('DB_NAME', 'rlc_commodities'))
    db_user: str = field(default_factory=lambda: os.environ.get('DB_USER', 'postgres'))
    db_password: str = field(default_factory=lambda: os.environ.get('DB_PASSWORD', ''))


# =============================================================================
# COLLECTOR CLASS
# =============================================================================

class EPARFSCollectorV2:
    """
    Enhanced collector for EPA RFS data.

    Loads data from local CSV files downloaded from EPA.
    Supports multiple file types: generation, separation, retirement, available.
    """

    def __init__(self, config: EPARFSConfig = None):
        self.config = config or EPARFSConfig()

    # =========================================================================
    # FILE LOADING
    # =========================================================================

    def load_generation_breakout(self, file_path: Path) -> List[Dict]:
        """
        Load RIN generation breakout from CSV file.

        Expected columns: RIN_YEAR, Fuel Code, DOMESTIC, IMPORTER, Foreign Generation, Total RINs

        Args:
            file_path: Path to CSV file

        Returns:
            List of records with parsed data
        """
        records = []
        source_file = file_path.name

        # Extract file month from filename (e.g., 'dec2025' from 'generationbreakout_dec2025.csv')
        match = re.search(r'generationbreakout_(\w+)\.csv', source_file, re.IGNORECASE)
        file_month = match.group(1) if match else None

        with open(file_path, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)

            for row in reader:
                try:
                    # Parse D-code (stored as '3', '4', '5', '6', '7')
                    d_code = str(row.get('Fuel Code', '')).strip()
                    if not d_code or d_code not in D_CODE_INFO:
                        continue

                    record = {
                        'rin_year': int(row.get('RIN_YEAR', 0)),
                        'd_code': d_code,
                        'domestic_rins': self._parse_int(row.get('DOMESTIC')),
                        'importer_rins': self._parse_int(row.get('IMPORTER')),
                        'foreign_generation_rins': self._parse_int(row.get('Foreign Generation')),
                        'total_rins': self._parse_int(row.get('Total RINs')),
                        'source_file': source_file,
                        'file_month': file_month,
                    }

                    records.append(record)

                except (ValueError, KeyError) as e:
                    logger.warning(f"Error parsing row: {e}")
                    continue

        logger.info(f"Loaded {len(records)} records from {source_file}")
        return records

    def load_available_rins(self, file_path: Path) -> List[Dict]:
        """
        Load available RINs data from CSV file.

        Expected columns: RIN_YEAR, FUEL_CD, ASSIGNMENT, Total Generated,
                         Total Retired, Total Locked, Total Available

        Args:
            file_path: Path to CSV file

        Returns:
            List of records with parsed data
        """
        records = []
        source_file = file_path.name

        with open(file_path, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)

            for row in reader:
                try:
                    d_code = str(row.get('FUEL_CD', '')).strip()
                    if not d_code or d_code not in D_CODE_INFO:
                        continue

                    record = {
                        'rin_year': int(row.get('RIN_YEAR', 0)),
                        'd_code': d_code,
                        'assignment': row.get('ASSIGNMENT', '').strip(),
                        'total_generated': self._parse_int(row.get('Total Generated')),
                        'total_retired': self._parse_int(row.get('Total Retired')),
                        'total_locked': self._parse_int(row.get('Total Locked')),
                        'total_available': self._parse_int(row.get('Total Available')),
                        'source_file': source_file,
                    }

                    records.append(record)

                except (ValueError, KeyError) as e:
                    logger.warning(f"Error parsing row: {e}")
                    continue

        logger.info(f"Loaded {len(records)} available RIN records from {source_file}")
        return records

    def load_rin_data_monthly(self, file_path: Path) -> List[Dict]:
        """
        Load monthly RIN generation data from rindata CSV.

        Columns: FUEL_CODE, RIN_YEAR, Production Month, RIN_QUANTITY, BATCH_VOLUME

        This provides monthly granularity for time series analysis.
        """
        records = []
        source_file = file_path.name

        with open(file_path, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)

            for row in reader:
                try:
                    d_code = str(row.get('FUEL_CODE', '')).strip()
                    if not d_code or d_code not in D_CODE_INFO:
                        continue

                    record = {
                        'rin_year': int(row.get('RIN_YEAR', 0)),
                        'month': int(row.get('Production Month', 0)),
                        'd_code': d_code,
                        'rin_quantity': self._parse_int(row.get('RIN_QUANTITY')),
                        'batch_volume': self._parse_int(row.get('BATCH_VOLUME')),
                        'source_file': source_file,
                    }
                    records.append(record)

                except (ValueError, KeyError) as e:
                    logger.warning(f"Error parsing row: {e}")
                    continue

        logger.info(f"Loaded {len(records)} monthly RIN records from {source_file}")
        return records

    def load_fuel_production(self, file_path: Path) -> List[Dict]:
        """
        Load fuel production by fuel type from fuelproduction CSV.

        Columns: RIN Year, Fuel, Fuel Code, Fuel Category, Fuel Category Code,
                 RIN Quantity, Batch Volume

        Provides breakdown by specific fuel type (Naphtha, LPG, biodiesel, etc.)
        """
        records = []
        source_file = file_path.name

        with open(file_path, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)

            for row in reader:
                try:
                    d_code = str(row.get('Fuel Code', '')).strip()
                    if not d_code or d_code not in D_CODE_INFO:
                        continue

                    record = {
                        'rin_year': int(row.get('RIN Year', 0)),
                        'd_code': d_code,
                        'fuel_name': row.get('Fuel', '').strip(),
                        'fuel_category': row.get('Fuel Category', '').strip(),
                        'fuel_category_code': row.get('Fuel Category Code', '').strip(),
                        'rin_quantity': self._parse_int(row.get('RIN Quantity')),
                        'batch_volume': self._parse_int(row.get('Batch Volume')),
                        'source_file': source_file,
                    }
                    records.append(record)

                except (ValueError, KeyError) as e:
                    logger.warning(f"Error parsing row: {e}")
                    continue

        logger.info(f"Loaded {len(records)} fuel production records from {source_file}")
        return records

    def load_retirement_transactions(self, file_path: Path) -> List[Dict]:
        """
        Load RIN retirement data by reason from retiretransaction CSV.

        Columns: RIN_YEAR, FUEL_CD, FUEL_CD_DESCRIPTION, RETIRE_REASON_CD,
                 RETIRE_REASON_CD_DESCRIPTION, RIN_QUANTITY

        Shows compliance retirements, invalid RINs, enforcement, etc.
        """
        records = []
        source_file = file_path.name

        with open(file_path, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)

            for row in reader:
                try:
                    d_code = str(row.get('FUEL_CD', '')).strip()
                    if not d_code or d_code not in D_CODE_INFO:
                        continue

                    record = {
                        'rin_year': int(row.get('RIN_YEAR', 0)),
                        'd_code': d_code,
                        'd_code_description': row.get('FUEL_CD_DESCRIPTION', '').strip(),
                        'retire_reason_code': row.get('RETIRE_REASON_CD', '').strip(),
                        'retire_reason': row.get('RETIRE_REASON_CD_DESCRIPTION', '').strip(),
                        'rin_quantity': self._parse_int(row.get('RIN_QUANTITY')),
                        'source_file': source_file,
                    }
                    records.append(record)

                except (ValueError, KeyError) as e:
                    logger.warning(f"Error parsing row: {e}")
                    continue

        logger.info(f"Loaded {len(records)} retirement records from {source_file}")
        return records

    def load_separation_transactions(self, file_path: Path) -> List[Dict]:
        """
        Load RIN separation data by reason from separatetransaction CSV.

        Columns: RIN Year, Fuel Cd, Fuel Cd Description, Sep Reason Cd,
                 Separate Reason Cd Description, RIN Quantity

        Shows separation by obligated party receipt, blending, export, etc.
        """
        records = []
        source_file = file_path.name

        with open(file_path, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)

            for row in reader:
                try:
                    d_code = str(row.get('Fuel Cd', '')).strip()
                    if not d_code or d_code not in D_CODE_INFO:
                        continue

                    record = {
                        'rin_year': int(row.get('RIN Year', 0)),
                        'd_code': d_code,
                        'd_code_description': row.get('Fuel Cd Description', '').strip(),
                        'separation_reason_code': row.get('Sep Reason Cd', '').strip(),
                        'separation_reason': row.get('Separate Reason Cd Description', '').strip(),
                        'rin_quantity': self._parse_int(row.get('RIN Quantity')),
                        'source_file': source_file,
                    }
                    records.append(record)

                except (ValueError, KeyError) as e:
                    logger.warning(f"Error parsing row: {e}")
                    continue

        logger.info(f"Loaded {len(records)} separation records from {source_file}")
        return records

    def load_all_files(self, data_dir: Path = None) -> Dict[str, List[Dict]]:
        """
        Load all EPA RFS files from a directory.

        Args:
            data_dir: Directory containing CSV files (default: config.data_dir)

        Returns:
            Dict mapping file type to list of records
        """
        data_dir = data_dir or self.config.data_dir

        if not data_dir.exists():
            logger.warning(f"Data directory not found: {data_dir}")
            return {}

        results = {}

        for csv_file in data_dir.glob('*.csv'):
            file_name = csv_file.name.lower()

            # Identify file type
            if 'generationbreakout' in file_name:
                records = self.load_generation_breakout(csv_file)
                results.setdefault('generation_breakout', []).extend(records)

            elif 'availablerins' in file_name:
                records = self.load_available_rins(csv_file)
                results.setdefault('available_rins', []).extend(records)

            elif 'rindata' in file_name:
                records = self.load_rin_data_monthly(csv_file)
                results.setdefault('rin_data_monthly', []).extend(records)

            elif 'fuelproduction' in file_name:
                records = self.load_fuel_production(csv_file)
                results.setdefault('fuel_production', []).extend(records)

            elif 'retiretransaction' in file_name:
                records = self.load_retirement_transactions(csv_file)
                results.setdefault('retirement', []).extend(records)

            elif 'separatetransaction' in file_name:
                records = self.load_separation_transactions(csv_file)
                results.setdefault('separation', []).extend(records)

        return results

    # =========================================================================
    # URL CONSTRUCTION
    # =========================================================================

    def build_generation_url(self, data_year: int, data_month: int) -> str:
        """
        Build URL for generation breakout file.

        The URL format is:
        https://www.epa.gov/system/files/other-files/YYYY-MM/generationbreakout_monYYYY.csv

        Where YYYY-MM is the publication date (typically data_month + 1 or +2)
        and monYYYY is the data month in lowercase 3-letter format + year.

        Args:
            data_year: Year of the data (e.g., 2025)
            data_month: Month of the data (1-12)

        Returns:
            Full URL to the CSV file
        """
        months_abbrev = ['jan', 'feb', 'mar', 'apr', 'may', 'jun',
                        'jul', 'aug', 'sep', 'oct', 'nov', 'dec']

        mon = months_abbrev[data_month - 1]

        # Publication is typically 1-2 months after data month
        # December 2025 data released in January 2026
        if data_month == 12:
            pub_year = data_year + 1
            pub_month = 1
        else:
            pub_year = data_year
            pub_month = data_month + 1

        url = f"https://www.epa.gov/system/files/other-files/{pub_year}-{pub_month:02d}/generationbreakout_{mon}{data_year}.csv"

        return url

    def download_generation_file(
        self,
        data_year: int,
        data_month: int,
        save_dir: Path = None
    ) -> Optional[Path]:
        """
        Download generation breakout file from EPA.

        Args:
            data_year: Year of the data
            data_month: Month of the data
            save_dir: Directory to save file (default: config.data_dir)

        Returns:
            Path to saved file or None if download failed
        """
        save_dir = save_dir or self.config.data_dir
        save_dir.mkdir(parents=True, exist_ok=True)

        url = self.build_generation_url(data_year, data_month)
        logger.info(f"Downloading from: {url}")

        try:
            response = requests.get(url, timeout=30)
            response.raise_for_status()

            # Extract filename from URL
            filename = url.split('/')[-1]
            save_path = save_dir / filename

            with open(save_path, 'wb') as f:
                f.write(response.content)

            logger.info(f"Saved to: {save_path}")
            return save_path

        except requests.exceptions.RequestException as e:
            logger.error(f"Download failed: {e}")
            return None

    # =========================================================================
    # DATABASE METHODS
    # =========================================================================

    def save_generation_to_bronze(
        self,
        records: List[Dict],
        conn=None
    ) -> Dict[str, int]:
        """
        Save generation records to bronze.epa_rfs_generation.

        Uses upsert to handle updates to existing records.

        Args:
            records: List of generation record dicts
            conn: Optional database connection

        Returns:
            Dict with insert/update counts
        """
        import psycopg2

        close_conn = False
        if conn is None:
            conn = psycopg2.connect(
                host=self.config.db_host,
                port=self.config.db_port,
                database=self.config.db_name,
                user=self.config.db_user,
                password=self.config.db_password
            )
            close_conn = True

        cursor = conn.cursor()
        counts = {'inserted': 0, 'updated': 0, 'errors': 0}

        for record in records:
            try:
                cursor.execute("""
                    INSERT INTO bronze.epa_rfs_generation
                    (rin_year, d_code, domestic_rins, importer_rins,
                     foreign_generation_rins, total_rins, source_file, file_month, collected_at)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, NOW())
                    ON CONFLICT (rin_year, d_code)
                    DO UPDATE SET
                        domestic_rins = EXCLUDED.domestic_rins,
                        importer_rins = EXCLUDED.importer_rins,
                        foreign_generation_rins = EXCLUDED.foreign_generation_rins,
                        total_rins = EXCLUDED.total_rins,
                        source_file = EXCLUDED.source_file,
                        file_month = EXCLUDED.file_month,
                        collected_at = NOW()
                    RETURNING (xmax = 0) as inserted
                """, (
                    record.get('rin_year'),
                    record.get('d_code'),
                    record.get('domestic_rins'),
                    record.get('importer_rins'),
                    record.get('foreign_generation_rins'),
                    record.get('total_rins'),
                    record.get('source_file'),
                    record.get('file_month'),
                ))

                result = cursor.fetchone()
                if result and result[0]:
                    counts['inserted'] += 1
                else:
                    counts['updated'] += 1

            except Exception as e:
                logger.error(f"Error saving record: {e}")
                counts['errors'] += 1
                conn.rollback()

        conn.commit()

        if close_conn:
            cursor.close()
            conn.close()

        logger.info(f"Saved to bronze.epa_rfs_generation: {counts}")
        return counts

    def transform_to_silver(self, conn=None) -> int:
        """
        Transform bronze generation data to silver layer.

        Adds D-code names, calculates equivalent gallons, and YoY changes.

        Returns:
            Number of records transformed
        """
        import psycopg2

        close_conn = False
        if conn is None:
            conn = psycopg2.connect(
                host=self.config.db_host,
                port=self.config.db_port,
                database=self.config.db_name,
                user=self.config.db_user,
                password=self.config.db_password
            )
            close_conn = True

        cursor = conn.cursor()

        # Get D-code equivalence values
        ev_map = {k: v['equivalence_value'] for k, v in D_CODE_INFO.items()}
        name_map = {k: v['name'] for k, v in D_CODE_INFO.items()}

        cursor.execute("""
            SELECT id, rin_year, d_code, domestic_rins, importer_rins,
                   foreign_generation_rins, total_rins
            FROM bronze.epa_rfs_generation
            ORDER BY d_code, rin_year
        """)

        rows = cursor.fetchall()
        count = 0

        # Track previous year for YoY calculation
        prev_by_dcode = {}

        for row in rows:
            bronze_id, year, d_code, domestic, importer, foreign, total = row

            # Get equivalence value and name
            ev = ev_map.get(d_code, 1.0)
            name = name_map.get(d_code, f'D{d_code}')

            # Calculate equivalent gallons
            equiv_gallons = total / ev if total else None

            # YoY change
            prev = prev_by_dcode.get(d_code)
            yoy_rins = None
            yoy_pct = None
            if prev and prev.get('year') == year - 1:
                prev_total = prev.get('total', 0)
                if prev_total:
                    yoy_rins = total - prev_total
                    yoy_pct = 100.0 * yoy_rins / prev_total

            prev_by_dcode[d_code] = {'year': year, 'total': total}

            try:
                cursor.execute("""
                    INSERT INTO silver.epa_rfs_generation
                    (year, d_code, d_code_name, total_rins, domestic_rins, import_rins,
                     foreign_rins, equivalent_gallons, equivalence_value,
                     yoy_change_rins, yoy_change_pct, source_bronze_id, transformed_at)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())
                    ON CONFLICT (year, d_code)
                    DO UPDATE SET
                        d_code_name = EXCLUDED.d_code_name,
                        total_rins = EXCLUDED.total_rins,
                        domestic_rins = EXCLUDED.domestic_rins,
                        import_rins = EXCLUDED.import_rins,
                        foreign_rins = EXCLUDED.foreign_rins,
                        equivalent_gallons = EXCLUDED.equivalent_gallons,
                        equivalence_value = EXCLUDED.equivalence_value,
                        yoy_change_rins = EXCLUDED.yoy_change_rins,
                        yoy_change_pct = EXCLUDED.yoy_change_pct,
                        source_bronze_id = EXCLUDED.source_bronze_id,
                        transformed_at = NOW()
                """, (
                    year, d_code, name, total, domestic, importer, foreign,
                    equiv_gallons, ev, yoy_rins, yoy_pct, bronze_id
                ))
                count += 1

            except Exception as e:
                logger.error(f"Error transforming record: {e}")
                conn.rollback()

        conn.commit()

        if close_conn:
            cursor.close()
            conn.close()

        logger.info(f"Transformed {count} records to silver.epa_rfs_generation")
        return count

    def save_monthly_to_bronze(self, records: List[Dict], conn=None) -> Dict[str, int]:
        """Save monthly RIN data to bronze.epa_rfs_rin_monthly."""
        import psycopg2

        close_conn = False
        if conn is None:
            conn = psycopg2.connect(
                host=self.config.db_host, port=self.config.db_port,
                database=self.config.db_name, user=self.config.db_user,
                password=self.config.db_password
            )
            close_conn = True

        cursor = conn.cursor()
        counts = {'inserted': 0, 'updated': 0, 'errors': 0}

        for record in records:
            try:
                cursor.execute("""
                    INSERT INTO bronze.epa_rfs_rin_monthly
                    (rin_year, production_month, d_code, rin_quantity, batch_volume,
                     source_file, collected_at)
                    VALUES (%s, %s, %s, %s, %s, %s, NOW())
                    ON CONFLICT (rin_year, production_month, d_code)
                    DO UPDATE SET
                        rin_quantity = EXCLUDED.rin_quantity,
                        batch_volume = EXCLUDED.batch_volume,
                        source_file = EXCLUDED.source_file,
                        collected_at = NOW()
                    RETURNING (xmax = 0) as inserted
                """, (
                    record.get('rin_year'),
                    record.get('month'),
                    record.get('d_code'),
                    record.get('rin_quantity'),
                    record.get('batch_volume'),
                    record.get('source_file'),
                ))
                result = cursor.fetchone()
                counts['inserted' if result and result[0] else 'updated'] += 1
            except Exception as e:
                logger.error(f"Error saving monthly record: {e}")
                counts['errors'] += 1
                conn.rollback()

        conn.commit()
        if close_conn:
            cursor.close()
            conn.close()

        logger.info(f"Saved to bronze.epa_rfs_rin_monthly: {counts}")
        return counts

    def save_fuel_production_to_bronze(self, records: List[Dict], conn=None) -> Dict[str, int]:
        """Save fuel production data to bronze.epa_rfs_fuel_production."""
        import psycopg2

        close_conn = False
        if conn is None:
            conn = psycopg2.connect(
                host=self.config.db_host, port=self.config.db_port,
                database=self.config.db_name, user=self.config.db_user,
                password=self.config.db_password
            )
            close_conn = True

        cursor = conn.cursor()
        counts = {'inserted': 0, 'updated': 0, 'errors': 0}

        for record in records:
            try:
                cursor.execute("""
                    INSERT INTO bronze.epa_rfs_fuel_production
                    (rin_year, d_code, fuel_category_code, fuel_name, fuel_category,
                     rin_quantity, batch_volume, source_file, collected_at)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, NOW())
                    ON CONFLICT (rin_year, d_code, fuel_category_code)
                    DO UPDATE SET
                        fuel_name = EXCLUDED.fuel_name,
                        fuel_category = EXCLUDED.fuel_category,
                        rin_quantity = EXCLUDED.rin_quantity,
                        batch_volume = EXCLUDED.batch_volume,
                        source_file = EXCLUDED.source_file,
                        collected_at = NOW()
                    RETURNING (xmax = 0) as inserted
                """, (
                    record.get('rin_year'),
                    record.get('d_code'),
                    record.get('fuel_category_code'),
                    record.get('fuel_name'),
                    record.get('fuel_category'),
                    record.get('rin_quantity'),
                    record.get('batch_volume'),
                    record.get('source_file'),
                ))
                result = cursor.fetchone()
                counts['inserted' if result and result[0] else 'updated'] += 1
            except Exception as e:
                logger.error(f"Error saving fuel production record: {e}")
                counts['errors'] += 1
                conn.rollback()

        conn.commit()
        if close_conn:
            cursor.close()
            conn.close()

        logger.info(f"Saved to bronze.epa_rfs_fuel_production: {counts}")
        return counts

    def save_available_to_bronze(self, records: List[Dict], conn=None) -> Dict[str, int]:
        """Save available RINs data to bronze.epa_rfs_available."""
        import psycopg2

        close_conn = False
        if conn is None:
            conn = psycopg2.connect(
                host=self.config.db_host, port=self.config.db_port,
                database=self.config.db_name, user=self.config.db_user,
                password=self.config.db_password
            )
            close_conn = True

        cursor = conn.cursor()
        counts = {'inserted': 0, 'updated': 0, 'errors': 0}

        for record in records:
            try:
                cursor.execute("""
                    INSERT INTO bronze.epa_rfs_available
                    (rin_year, d_code, assignment, total_generated, total_retired,
                     total_locked, total_available, source_file, collected_at)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, NOW())
                    ON CONFLICT (rin_year, d_code, assignment)
                    DO UPDATE SET
                        total_generated = EXCLUDED.total_generated,
                        total_retired = EXCLUDED.total_retired,
                        total_locked = EXCLUDED.total_locked,
                        total_available = EXCLUDED.total_available,
                        source_file = EXCLUDED.source_file,
                        collected_at = NOW()
                    RETURNING (xmax = 0) as inserted
                """, (
                    record.get('rin_year'),
                    record.get('d_code'),
                    record.get('assignment'),
                    record.get('total_generated'),
                    record.get('total_retired'),
                    record.get('total_locked'),
                    record.get('total_available'),
                    record.get('source_file'),
                ))
                result = cursor.fetchone()
                counts['inserted' if result and result[0] else 'updated'] += 1
            except Exception as e:
                logger.error(f"Error saving available record: {e}")
                counts['errors'] += 1
                conn.rollback()

        conn.commit()
        if close_conn:
            cursor.close()
            conn.close()

        logger.info(f"Saved to bronze.epa_rfs_available: {counts}")
        return counts

    def save_retirement_to_bronze(self, records: List[Dict], conn=None) -> Dict[str, int]:
        """Save retirement transaction data to bronze.epa_rfs_retirement."""
        import psycopg2

        close_conn = False
        if conn is None:
            conn = psycopg2.connect(
                host=self.config.db_host, port=self.config.db_port,
                database=self.config.db_name, user=self.config.db_user,
                password=self.config.db_password
            )
            close_conn = True

        cursor = conn.cursor()
        counts = {'inserted': 0, 'updated': 0, 'errors': 0}

        for record in records:
            try:
                cursor.execute("""
                    INSERT INTO bronze.epa_rfs_retirement
                    (rin_year, d_code, retire_reason_code, d_code_description,
                     retire_reason_description, rin_quantity, source_file, collected_at)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, NOW())
                    ON CONFLICT (rin_year, d_code, retire_reason_code)
                    DO UPDATE SET
                        d_code_description = EXCLUDED.d_code_description,
                        retire_reason_description = EXCLUDED.retire_reason_description,
                        rin_quantity = EXCLUDED.rin_quantity,
                        source_file = EXCLUDED.source_file,
                        collected_at = NOW()
                    RETURNING (xmax = 0) as inserted
                """, (
                    record.get('rin_year'),
                    record.get('d_code'),
                    record.get('retire_reason_code'),
                    record.get('d_code_description'),
                    record.get('retire_reason'),
                    record.get('rin_quantity'),
                    record.get('source_file'),
                ))
                result = cursor.fetchone()
                counts['inserted' if result and result[0] else 'updated'] += 1
            except Exception as e:
                logger.error(f"Error saving retirement record: {e}")
                counts['errors'] += 1
                conn.rollback()

        conn.commit()
        if close_conn:
            cursor.close()
            conn.close()

        logger.info(f"Saved to bronze.epa_rfs_retirement: {counts}")
        return counts

    def save_separation_to_bronze(self, records: List[Dict], conn=None) -> Dict[str, int]:
        """Save separation transaction data to bronze.epa_rfs_separation."""
        import psycopg2

        close_conn = False
        if conn is None:
            conn = psycopg2.connect(
                host=self.config.db_host, port=self.config.db_port,
                database=self.config.db_name, user=self.config.db_user,
                password=self.config.db_password
            )
            close_conn = True

        cursor = conn.cursor()
        counts = {'inserted': 0, 'updated': 0, 'errors': 0}

        for record in records:
            try:
                cursor.execute("""
                    INSERT INTO bronze.epa_rfs_separation
                    (rin_year, d_code, separation_reason_code, d_code_description,
                     separation_reason_description, rin_quantity, source_file, collected_at)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, NOW())
                    ON CONFLICT (rin_year, d_code, separation_reason_code)
                    DO UPDATE SET
                        d_code_description = EXCLUDED.d_code_description,
                        separation_reason_description = EXCLUDED.separation_reason_description,
                        rin_quantity = EXCLUDED.rin_quantity,
                        source_file = EXCLUDED.source_file,
                        collected_at = NOW()
                    RETURNING (xmax = 0) as inserted
                """, (
                    record.get('rin_year'),
                    record.get('d_code'),
                    record.get('separation_reason_code'),
                    record.get('d_code_description'),
                    record.get('separation_reason'),
                    record.get('rin_quantity'),
                    record.get('source_file'),
                ))
                result = cursor.fetchone()
                counts['inserted' if result and result[0] else 'updated'] += 1
            except Exception as e:
                logger.error(f"Error saving separation record: {e}")
                counts['errors'] += 1
                conn.rollback()

        conn.commit()
        if close_conn:
            cursor.close()
            conn.close()

        logger.info(f"Saved to bronze.epa_rfs_separation: {counts}")
        return counts

    def save_all_to_bronze(self, data: Dict[str, List[Dict]], conn=None) -> Dict[str, Dict[str, int]]:
        """
        Save all loaded data to bronze layer.

        Args:
            data: Dict from load_all_files()
            conn: Optional shared database connection

        Returns:
            Dict mapping file type to save counts
        """
        results = {}

        if 'generation_breakout' in data:
            results['generation_breakout'] = self.save_generation_to_bronze(data['generation_breakout'], conn)

        if 'rin_data_monthly' in data:
            results['rin_data_monthly'] = self.save_monthly_to_bronze(data['rin_data_monthly'], conn)

        if 'fuel_production' in data:
            results['fuel_production'] = self.save_fuel_production_to_bronze(data['fuel_production'], conn)

        if 'available_rins' in data:
            results['available_rins'] = self.save_available_to_bronze(data['available_rins'], conn)

        if 'retirement' in data:
            results['retirement'] = self.save_retirement_to_bronze(data['retirement'], conn)

        if 'separation' in data:
            results['separation'] = self.save_separation_to_bronze(data['separation'], conn)

        return results

    # =========================================================================
    # UTILITY METHODS
    # =========================================================================

    def _parse_int(self, value: Any) -> Optional[int]:
        """Safely parse integer value."""
        if value is None or value == '':
            return None
        try:
            return int(str(value).replace(',', ''))
        except (ValueError, TypeError):
            return None

    def get_summary(self, conn=None) -> Dict:
        """
        Get summary of loaded EPA RFS data.

        Returns:
            Dict with record counts and date ranges
        """
        import psycopg2

        close_conn = False
        if conn is None:
            try:
                conn = psycopg2.connect(
                    host=self.config.db_host,
                    port=self.config.db_port,
                    database=self.config.db_name,
                    user=self.config.db_user,
                    password=self.config.db_password
                )
                close_conn = True
            except Exception as e:
                logger.error(f"Database connection failed: {e}")
                return {'error': str(e)}

        cursor = conn.cursor()
        summary = {}

        try:
            cursor.execute("""
                SELECT
                    COUNT(*) as records,
                    MIN(rin_year) as min_year,
                    MAX(rin_year) as max_year,
                    array_agg(DISTINCT d_code ORDER BY d_code) as d_codes
                FROM bronze.epa_rfs_generation
            """)
            row = cursor.fetchone()
            summary['bronze_generation'] = {
                'records': row[0],
                'year_range': f"{row[1]}-{row[2]}" if row[1] else None,
                'd_codes': row[3] if row[3] else []
            }

        except Exception as e:
            summary['bronze_generation'] = {'error': str(e)}

        if close_conn:
            cursor.close()
            conn.close()

        return summary


# =============================================================================
# CLI INTERFACE
# =============================================================================

def main():
    """CLI for EPA RFS collector V2."""
    import argparse

    parser = argparse.ArgumentParser(description='EPA RFS Data Collector V2')

    parser.add_argument(
        'command',
        choices=['load', 'download', 'transform', 'summary', 'dcodes'],
        help='Command to execute'
    )

    parser.add_argument(
        '--file',
        help='Specific CSV file to load'
    )

    parser.add_argument(
        '--data-dir',
        help='Directory containing CSV files'
    )

    parser.add_argument(
        '--year',
        type=int,
        help='Data year for download'
    )

    parser.add_argument(
        '--month',
        type=int,
        help='Data month for download (1-12)'
    )

    parser.add_argument(
        '--save-db',
        action='store_true',
        help='Save data to PostgreSQL'
    )

    args = parser.parse_args()

    # Setup logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )

    collector = EPARFSCollectorV2()

    if args.command == 'dcodes':
        print("\nEPA RFS D-Code Reference:")
        print("-" * 60)
        for code, info in D_CODE_INFO.items():
            print(f"  D{code}: {info['name']}")
            print(f"       GHG: {info['ghg_threshold']}, EV: {info['equivalence_value']}")
            print(f"       Satisfies: {', '.join(info['satisfies'])}")
        return

    if args.command == 'summary':
        summary = collector.get_summary()
        print("\nEPA RFS Data Summary:")
        print("-" * 40)
        for key, value in summary.items():
            print(f"  {key}: {value}")
        return

    if args.command == 'download':
        if not args.year or not args.month:
            print("Error: --year and --month required for download")
            return

        result = collector.download_generation_file(args.year, args.month)
        if result:
            print(f"Downloaded: {result}")
        else:
            print("Download failed")
        return

    if args.command == 'load':
        if args.file:
            file_path = Path(args.file)
            records = collector.load_generation_breakout(file_path)
        else:
            data_dir = Path(args.data_dir) if args.data_dir else collector.config.data_dir
            all_data = collector.load_all_files(data_dir)
            records = all_data.get('generation_breakout', [])

        print(f"\nLoaded {len(records)} generation records")

        if records:
            print("\nSample records:")
            for r in records[:3]:
                print(f"  {r['rin_year']} D{r['d_code']}: {r['total_rins']:,} total RINs")

        if args.save_db and records:
            db_result = collector.save_generation_to_bronze(records)
            print(f"\nDatabase: {db_result}")
        return

    if args.command == 'transform':
        count = collector.transform_to_silver()
        print(f"\nTransformed {count} records to silver layer")
        return


if __name__ == '__main__':
    main()
