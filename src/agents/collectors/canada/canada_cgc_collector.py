"""
Canadian Grain Commission (CGC) Data Collector

Collects grain statistics from Canada via two structured CSV endpoints:
1. Grain Statistics Weekly (GSW) — deliveries, stocks, shipments, exports by week/grain/region
2. Exports from Licensed Facilities — monthly exports by grain, grade, destination

Data source: https://www.grainscanada.gc.ca/en/grain-research/statistics/
Free - No authentication required.

CGC crop year runs August 1 to July 31 (e.g., 2025-26 = Aug 2025 - Jul 2026).
"""

import logging
from dataclasses import dataclass, field
from datetime import datetime, date, timedelta
from typing import Dict, List, Optional, Any
from io import StringIO

from .base_collector import (
    BaseCollector,
    CollectorConfig,
    CollectorResult,
    DataFrequency,
    AuthType,
)

from src.services.database.db_config import get_connection as get_db_connection

try:
    import pandas as pd
    PANDAS_AVAILABLE = True
except ImportError:
    PANDAS_AVAILABLE = False

logger = logging.getLogger(__name__)


# ======================================================================
# CGC URL templates
# ======================================================================

# Grain Statistics Weekly — full crop-year CSV (~7 MB, updated weekly on Thursday)
GSW_CSV_URL = (
    "https://www.grainscanada.gc.ca/en/grain-research/statistics/"
    "grain-statistics-weekly/{crop_year}/gsw-shg-en.csv"
)

# Exports from Licensed Facilities — cumulative CSV (Aug 2014 to present, ~1.3 MB)
EXPORTS_CSV_URL = (
    "https://www.grainscanada.gc.ca/en/grain-research/statistics/"
    "exports-grain-licensed-facilities/csv/exports.csv"
)

# Normalize CGC grain names to our standard commodity codes
GRAIN_MAP = {
    'wheat': 'wheat',
    'amber durum': 'wheat_durum',
    'canola': 'canola',
    'barley': 'barley',
    'oats': 'oats',
    'corn': 'corn',
    'soybeans': 'soybeans',
    'flaxseed': 'flaxseed',
    'peas': 'peas',
    'lentils': 'lentils',
    'chick peas': 'chickpeas',
    'rye': 'rye',
    'mustard seed': 'mustard',
    'sunflower': 'sunflower',
    'canaryseed': 'canaryseed',
    'beans': 'beans',
}

# Months ordinal for sorting
MONTH_ORDER = {
    'january': 1, 'february': 2, 'march': 3, 'april': 4,
    'may': 5, 'june': 6, 'july': 7, 'august': 8,
    'september': 9, 'october': 10, 'november': 11, 'december': 12,
}


def _current_crop_year_label() -> str:
    """Return current CGC crop year label like '2025-26'. CGC year starts Aug 1."""
    today = date.today()
    if today.month >= 8:
        start = today.year
    else:
        start = today.year - 1
    end_short = str(start + 1)[-2:]
    return f"{start}-{end_short}"


def _normalize_grain(raw: str) -> Optional[str]:
    """Normalize a CGC grain name to standard commodity code."""
    if not raw:
        return None
    key = raw.strip().lower()
    # Direct lookup
    if key in GRAIN_MAP:
        return GRAIN_MAP[key]
    # Partial match for imported grains like 'U.S. Corn', 'Canadian and Imported Origin Corn'
    for pattern, code in GRAIN_MAP.items():
        if pattern in key:
            return code
    return key.replace(' ', '_')


# ======================================================================
# Config & Collector
# ======================================================================

@dataclass
class CGCConfig(CollectorConfig):
    """Canadian Grain Commission configuration"""
    source_name: str = "Canadian Grain Commission"
    source_url: str = "https://www.grainscanada.gc.ca"
    auth_type: AuthType = AuthType.NONE
    frequency: DataFrequency = DataFrequency.WEEKLY

    # Which datasets to fetch
    fetch_gsw: bool = True
    fetch_exports: bool = True

    # Crop year override (None = current)
    crop_year: Optional[str] = None

    # Request settings
    timeout: int = 90
    retry_attempts: int = 3


class CGCCollector(BaseCollector):
    """
    Collector for Canadian Grain Commission data.

    Fetches two structured CSV files:
    1. Grain Statistics Weekly — weekly deliveries, stocks, shipments, exports
       by grain, grade, and region across all Canadian provinces/terminals.
    2. Exports from Licensed Facilities — monthly exports by grain, grade,
       and destination country.

    Data released every Thursday during the crop year.
    """

    def __init__(self, config: CGCConfig = None):
        config = config or CGCConfig()
        super().__init__(config)
        self.config: CGCConfig = config

        # CGC is a public .gc.ca website — standard browser headers
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
            'Accept': 'text/csv,text/html,application/xhtml+xml,*/*;q=0.8',
            'Accept-Language': 'en-CA,en;q=0.5',
        })

    def get_table_name(self) -> str:
        return "canada_cgc"

    # ------------------------------------------------------------------
    # Main collect override — persist to bronze after fetch
    # ------------------------------------------------------------------

    def collect(self, start_date=None, end_date=None, use_cache=True, **kwargs):
        """Override collect to save results to bronze after fetching."""
        result = super().collect(start_date, end_date, use_cache, **kwargs)
        if result.success and result.data is not None and not getattr(result, 'from_cache', False):
            try:
                gsw_records = result.data.get('gsw', [])
                export_records = result.data.get('exports', [])
                saved = 0
                if gsw_records:
                    saved += self._save_gsw_to_bronze(gsw_records)
                if export_records:
                    saved += self._save_exports_to_bronze(export_records)
                result.records_fetched = saved
                self.logger.info(f"Saved {saved} total records to bronze")
            except Exception as e:
                self.logger.error(f"Bronze save failed (data still returned): {e}", exc_info=True)
        return result

    # ------------------------------------------------------------------
    # fetch_data — the core abstract method implementation
    # ------------------------------------------------------------------

    def fetch_data(
        self,
        start_date: date = None,
        end_date: date = None,
        **kwargs,
    ) -> CollectorResult:
        """
        Fetch CGC grain statistics CSVs.

        Returns CollectorResult with data={'gsw': [...], 'exports': [...]}.
        """
        if not PANDAS_AVAILABLE:
            return CollectorResult(
                success=False,
                source=self.config.source_name,
                error_message="pandas is required for CGC CSV parsing",
            )

        all_records: Dict[str, list] = {'gsw': [], 'exports': []}
        warnings: List[str] = []
        total = 0

        # --- 1. Grain Statistics Weekly ---
        if self.config.fetch_gsw:
            crop_year = self.config.crop_year or _current_crop_year_label()
            gsw_url = GSW_CSV_URL.format(crop_year=crop_year)
            self.logger.info(f"Fetching GSW CSV for crop year {crop_year}")

            try:
                records = self._fetch_gsw_csv(gsw_url, crop_year)
                all_records['gsw'] = records
                total += len(records)
                self.logger.info(f"GSW: {len(records)} records")
            except Exception as e:
                warnings.append(f"GSW: {e}")
                self.logger.error(f"Error fetching GSW: {e}", exc_info=True)

        # --- 2. Exports from Licensed Facilities ---
        if self.config.fetch_exports:
            self.logger.info("Fetching Exports CSV")

            try:
                records = self._fetch_exports_csv(EXPORTS_CSV_URL, start_date, end_date)
                all_records['exports'] = records
                total += len(records)
                self.logger.info(f"Exports: {len(records)} records")
            except Exception as e:
                warnings.append(f"Exports: {e}")
                self.logger.error(f"Error fetching Exports: {e}", exc_info=True)

        if total == 0:
            return CollectorResult(
                success=False,
                source=self.config.source_name,
                error_message="No data retrieved from any CGC CSV endpoint",
                warnings=warnings,
            )

        return CollectorResult(
            success=True,
            source=self.config.source_name,
            records_fetched=total,
            data=all_records,
            period_start=str(start_date) if start_date else None,
            period_end=str(end_date) if end_date else None,
            warnings=warnings,
        )

    # ------------------------------------------------------------------
    # CSV fetchers
    # ------------------------------------------------------------------

    def _fetch_gsw_csv(self, url: str, crop_year: str) -> List[Dict]:
        """Download and parse Grain Statistics Weekly CSV."""
        response, error = self._make_request(url, timeout=self.config.timeout)
        if error:
            raise RuntimeError(f"GSW request failed: {error}")
        if response.status_code != 200:
            raise RuntimeError(f"GSW HTTP {response.status_code}")

        df = pd.read_csv(StringIO(response.text))

        # Expected columns: Crop Year, Grain Week, Week Ending Date,
        #   worksheet, metric, period, grain, grade, Region, Ktonnes
        required = {'Crop Year', 'Grain Week', 'Week Ending Date', 'metric', 'grain', 'Ktonnes'}
        missing = required - set(df.columns)
        if missing:
            raise RuntimeError(f"GSW CSV missing columns: {missing}")

        # Parse week-ending date (CGC uses DD/MM/YYYY format)
        df['week_ending'] = pd.to_datetime(df['Week Ending Date'], format='%d/%m/%Y', errors='coerce')

        records = []
        for _, row in df.iterrows():
            we = row['week_ending']
            grain_raw = str(row.get('grain', '')).strip()
            records.append({
                'crop_year': str(row.get('Crop Year', crop_year)),
                'grain_week': int(row['Grain Week']) if pd.notna(row['Grain Week']) else None,
                'week_ending': we.strftime('%Y-%m-%d') if pd.notna(we) else None,
                'worksheet': str(row.get('worksheet', '')).strip(),
                'metric': str(row.get('metric', '')).strip(),
                'period': str(row.get('period', '')).strip(),
                'grain': grain_raw,
                'grade': str(row.get('grade', '')).strip() if pd.notna(row.get('grade')) else '',
                'region': str(row.get('Region', '')).strip() if pd.notna(row.get('Region')) else '',
                'ktonnes': float(row['Ktonnes']) if pd.notna(row['Ktonnes']) else None,
                'commodity': _normalize_grain(grain_raw),
            })

        return records

    def _fetch_exports_csv(
        self, url: str, start_date: Optional[date], end_date: Optional[date]
    ) -> List[Dict]:
        """Download and parse Exports from Licensed Facilities CSV."""
        response, error = self._make_request(url, timeout=self.config.timeout)
        if error:
            raise RuntimeError(f"Exports request failed: {error}")
        if response.status_code != 200:
            raise RuntimeError(f"Exports HTTP {response.status_code}")

        df = pd.read_csv(StringIO(response.text))

        # Expected: Year, Month, Grain, Grade, Ktonnes, Elevator, Region, Global_region, Destination
        required = {'Year', 'Month', 'Grain', 'Ktonnes'}
        missing = required - set(df.columns)
        if missing:
            raise RuntimeError(f"Exports CSV missing columns: {missing}")

        # Optional date filter: only keep recent data
        if start_date:
            start_year = start_date.year
            df = df[df['Year'] >= start_year]
        if end_date:
            df = df[df['Year'] <= end_date.year]

        records = []
        for _, row in df.iterrows():
            grain_raw = str(row.get('Grain', '')).strip()
            records.append({
                'year': int(row['Year']) if pd.notna(row['Year']) else None,
                'month': str(row.get('Month', '')).strip(),
                'grain': grain_raw,
                'grade': str(row.get('Grade', '')).strip() if pd.notna(row.get('Grade')) else '',
                'ktonnes': float(row['Ktonnes']) if pd.notna(row['Ktonnes']) else None,
                'elevator': str(row.get('Elevator', '')).strip() if pd.notna(row.get('Elevator')) else '',
                'region': str(row.get('Region', '')).strip() if pd.notna(row.get('Region')) else '',
                'global_region': str(row.get('Global_region', '')).strip() if pd.notna(row.get('Global_region')) else '',
                'destination': str(row.get('Destination', '')).strip() if pd.notna(row.get('Destination')) else '',
                'commodity': _normalize_grain(grain_raw),
            })

        return records

    # ------------------------------------------------------------------
    # Bronze persistence
    # ------------------------------------------------------------------

    def _save_gsw_to_bronze(self, records: List[Dict]) -> int:
        """Upsert GSW records to bronze.canada_cgc_weekly."""
        if not records:
            return 0

        with get_db_connection() as conn:
            cur = conn.cursor()
            count = 0
            for rec in records:
                if not rec.get('week_ending'):
                    continue
                cur.execute("""
                    INSERT INTO bronze.canada_cgc_weekly
                        (crop_year, grain_week, week_ending, worksheet, metric,
                         period, grain, grade, region, ktonnes, commodity,
                         source, collected_at)
                    VALUES
                        (%(crop_year)s, %(grain_week)s, %(week_ending)s,
                         %(worksheet)s, %(metric)s, %(period)s,
                         %(grain)s, %(grade)s, %(region)s, %(ktonnes)s,
                         %(commodity)s, 'CGC_GSW', NOW())
                    ON CONFLICT (crop_year, grain_week, worksheet, metric, period, grain, grade, region)
                    DO UPDATE SET
                        ktonnes = EXCLUDED.ktonnes,
                        commodity = EXCLUDED.commodity,
                        collected_at = NOW()
                """, rec)
                count += 1
            conn.commit()
            self.logger.info(f"Saved {count} records to bronze.canada_cgc_weekly")
            return count

    def _save_exports_to_bronze(self, records: List[Dict]) -> int:
        """Upsert export records to bronze.canada_cgc_exports."""
        if not records:
            return 0

        with get_db_connection() as conn:
            cur = conn.cursor()
            count = 0
            for rec in records:
                if not rec.get('year'):
                    continue
                cur.execute("""
                    INSERT INTO bronze.canada_cgc_exports
                        (year, month, grain, grade, ktonnes, elevator,
                         region, global_region, destination, commodity,
                         source, collected_at)
                    VALUES
                        (%(year)s, %(month)s, %(grain)s, %(grade)s,
                         %(ktonnes)s, %(elevator)s, %(region)s,
                         %(global_region)s, %(destination)s, %(commodity)s,
                         'CGC_EXPORTS', NOW())
                    ON CONFLICT (year, month, grain, grade, elevator, region, destination)
                    DO UPDATE SET
                        ktonnes = EXCLUDED.ktonnes,
                        commodity = EXCLUDED.commodity,
                        global_region = EXCLUDED.global_region,
                        collected_at = NOW()
                """, rec)
                count += 1
            conn.commit()
            self.logger.info(f"Saved {count} records to bronze.canada_cgc_exports")
            return count

    # ------------------------------------------------------------------
    # Required abstract method
    # ------------------------------------------------------------------

    def parse_response(self, response_data: Any) -> Any:
        return response_data

    # ------------------------------------------------------------------
    # Convenience methods
    # ------------------------------------------------------------------

    def get_visible_supply(self, commodity: str = 'wheat') -> Optional[Any]:
        """
        Get latest visible supply (stocks in licensed elevators) for a commodity.

        Filters GSW data for metric='Stocks' and period='Current Week'.
        """
        result = self.collect(use_cache=True)
        if not result.success or result.data is None:
            return None

        gsw = result.data.get('gsw', [])
        if not gsw or not PANDAS_AVAILABLE:
            return None

        df = pd.DataFrame(gsw)
        mask = (
            (df['metric'] == 'Stocks')
            & (df['period'] == 'Current Week')
            & (df['commodity'] == _normalize_grain(commodity))
        )
        return df[mask] if mask.any() else None

    def get_weekly_exports(self, commodity: str = 'wheat') -> Optional[Any]:
        """Get weekly export data for a commodity from GSW."""
        result = self.collect(use_cache=True)
        if not result.success or result.data is None:
            return None

        gsw = result.data.get('gsw', [])
        if not gsw or not PANDAS_AVAILABLE:
            return None

        df = pd.DataFrame(gsw)
        norm = _normalize_grain(commodity)
        mask = (
            (df['metric'] == 'Exports')
            & (df['commodity'] == norm)
        )
        return df[mask] if mask.any() else None

    def get_canola_movement(self) -> Optional[Any]:
        """Get canola movement data"""
        return self.get_weekly_exports('canola')

    def get_wheat_movement(self) -> Optional[Any]:
        """Get all wheat movement data"""
        return self.get_weekly_exports('wheat')


# =============================================================================
# CLI INTERFACE
# =============================================================================

def main():
    """CLI for CGC collector"""
    import argparse

    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s %(name)s %(levelname)s: %(message)s',
    )

    parser = argparse.ArgumentParser(description='Canadian Grain Commission Data Collector')
    parser.add_argument(
        'command',
        choices=['fetch', 'visible', 'exports', 'test'],
        help='Command to execute',
    )
    parser.add_argument('--crop-year', help='Crop year label (e.g., 2025-26)')
    parser.add_argument('--no-gsw', action='store_true', help='Skip GSW fetch')
    parser.add_argument('--no-exports', action='store_true', help='Skip exports fetch')

    args = parser.parse_args()

    config = CGCConfig(
        crop_year=args.crop_year,
        fetch_gsw=not args.no_gsw,
        fetch_exports=not args.no_exports,
    )
    collector = CGCCollector(config)

    if args.command == 'test':
        success, message = collector.test_connection()
        print(f"Connection test: {'PASS' if success else 'FAIL'} - {message}")
        return

    if args.command == 'visible':
        data = collector.get_visible_supply()
        if data is not None and PANDAS_AVAILABLE:
            print(data.to_string())
        else:
            print("No visible supply data available")
        return

    if args.command == 'exports':
        data = collector.get_weekly_exports()
        if data is not None and PANDAS_AVAILABLE:
            print(data.to_string())
        else:
            print("No export data available")
        return

    if args.command == 'fetch':
        result = collector.collect(use_cache=False)
        print(f"Success: {result.success}")
        print(f"Records: {result.records_fetched}")
        if result.data:
            gsw = result.data.get('gsw', [])
            exp = result.data.get('exports', [])
            print(f"  GSW records: {len(gsw)}")
            print(f"  Export records: {len(exp)}")
        if result.warnings:
            print(f"Warnings: {result.warnings}")
        if result.error_message:
            print(f"Error: {result.error_message}")


if __name__ == '__main__':
    main()
