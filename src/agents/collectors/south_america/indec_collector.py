"""
INDEC Argentina Trade Collector

Collects Argentine foreign trade statistics from INDEC
(Instituto Nacional de Estadística y Censos).

Data source:
- https://www.indec.gob.ar/Nivel4/Tema/3/2/40
- Monthly ICA (Intercambio Comercial Argentino) database ZIP

No API key required. Data via monthly ZIP download containing CSVs.
Monthly data with ~6-8 week lag.
"""

import logging
import re
import zipfile
from dataclasses import dataclass, field
from datetime import datetime, date
from io import BytesIO
from typing import Dict, List, Optional, Any

from .base_collector import (
    BaseCollector,
    CollectorConfig,
    CollectorResult,
    DataFrequency,
    AuthType
)

from src.services.database.db_config import get_connection as get_db_connection

try:
    import pandas as pd
    PANDAS_AVAILABLE = True
except ImportError:
    PANDAS_AVAILABLE = False

logger = logging.getLogger(__name__)

# NCM codes for agricultural commodities
AG_NCM_CODES = {
    '12019000': 'soybeans',        # Soybeans (whole, excl. seed)
    '12011000': 'soybeans',        # Soybean seed
    '10059010': 'corn',            # Corn grain (excl. seed)
    '10051000': 'corn',            # Corn seed
    '10019900': 'wheat',           # Wheat (excl. durum, excl. seed)
    '10011100': 'wheat',           # Durum wheat for sowing
    '10019100': 'wheat',           # Other wheat for sowing
    '10079000': 'sorghum',         # Sorghum grain
    '23040010': 'soybean_meal',    # Soybean meal/pellets
    '23040090': 'soybean_meal',    # Soybean residue
    '15071000': 'soybean_oil',     # Soybean oil, crude
    '15079011': 'soybean_oil',     # Soybean oil, refined (<=5L)
    '15079019': 'soybean_oil',     # Soybean oil, refined (>5L)
    '15079090': 'soybean_oil',     # Soybean oil, other
    '12060090': 'sunflower_seed',  # Sunflower seeds
    '15121110': 'sunflower_oil',   # Sunflower oil, crude
    '15121919': 'sunflower_oil',   # Sunflower oil, refined
    '23063010': 'sunflower_meal',  # Sunflower meal
    '10030090': 'barley',          # Barley grain
    '10063021': 'rice',            # Rice, semi/wholly milled
}


@dataclass
class INDECConfig(CollectorConfig):
    """INDEC specific configuration"""
    source_name: str = "INDEC Argentina"
    source_url: str = "https://www.indec.gob.ar"
    auth_type: AuthType = AuthType.NONE
    frequency: DataFrequency = DataFrequency.MONTHLY

    # Discovery page for latest ICA report
    ica_page: str = "https://www.indec.gob.ar/Nivel4/Tema/3/2/40"

    # Rate limiting
    rate_limit_per_minute: int = 10
    timeout: int = 120


class INDECCollector(BaseCollector):
    """
    Collector for INDEC Argentine trade statistics.

    Downloads the monthly ICA database ZIP, extracts CSV files, and
    parses agricultural commodity trade data.

    No API key required — public FTP download.
    """

    def __init__(self, config: INDECConfig = None):
        config = config or INDECConfig()
        super().__init__(config)
        self.config: INDECConfig = config

    def get_table_name(self) -> str:
        return "indec_trade"

    def collect(self, start_date=None, end_date=None, use_cache=True, **kwargs):
        """Override collect to save results to bronze after fetching."""
        result = super().collect(start_date, end_date, use_cache, **kwargs)
        if result.success and result.data is not None and not getattr(result, 'from_cache', False):
            try:
                records = result.data.to_dict('records') if hasattr(result.data, 'to_dict') else result.data
                if records:
                    saved = self.save_to_bronze(records)
                    result.records_fetched = saved
            except Exception as e:
                self.logger.error(f"Bronze save failed (data still returned): {e}")
        return result

    def fetch_data(
        self,
        start_date: date = None,
        end_date: date = None,
        flow: str = "both",
        **kwargs
    ) -> CollectorResult:
        """
        Fetch trade data from INDEC ICA monthly database.

        Args:
            start_date: Not used (ZIP contains full period)
            end_date: Not used
            flow: 'export', 'import', or 'both'

        Returns:
            CollectorResult with parsed trade data
        """
        all_records = []
        warnings = []

        # Step 1: Discover latest ICA report ID
        ica_id = self._discover_latest_ica_id()
        if not ica_id:
            return CollectorResult(
                success=False,
                source=self.config.source_name,
                error_message="Could not discover latest ICA report ID"
            )

        self.logger.info(f"Found ICA report: {ica_id}")

        # Step 2: Download the ZIP
        zip_url = f"{self.config.source_url}/ftp/ica_digital/{ica_id}/data/bases_de_datos.zip"
        response, error = self._make_request(zip_url, timeout=120)

        if error:
            return CollectorResult(
                success=False,
                source=self.config.source_name,
                error_message=f"ZIP download failed: {error}"
            )

        if response.status_code != 200:
            return CollectorResult(
                success=False,
                source=self.config.source_name,
                error_message=f"ZIP download HTTP {response.status_code}"
            )

        # Step 3: Extract and parse CSVs
        try:
            zf = zipfile.ZipFile(BytesIO(response.content))
            file_list = zf.namelist()
            self.logger.info(f"ZIP contains: {file_list}")

            if flow in ('export', 'both'):
                expo_files = [f for f in file_list if re.match(r'exponm\d{2}\.csv', f, re.I)]
                for fname in expo_files:
                    records = self._parse_trade_csv(zf, fname, 'export', ica_id)
                    all_records.extend(records)
                    self.logger.info(f"Parsed {len(records)} export records from {fname}")

            if flow in ('import', 'both'):
                impo_files = [f for f in file_list if re.match(r'impom\d{2}\.csv', f, re.I)]
                for fname in impo_files:
                    records = self._parse_trade_csv(zf, fname, 'import', ica_id)
                    all_records.extend(records)
                    self.logger.info(f"Parsed {len(records)} import records from {fname}")

            if not all_records:
                warnings.append(f"No ag commodity records found in {len(file_list)} files")

        except zipfile.BadZipFile:
            return CollectorResult(
                success=False,
                source=self.config.source_name,
                error_message="Downloaded file is not a valid ZIP"
            )
        except Exception as e:
            return CollectorResult(
                success=False,
                source=self.config.source_name,
                error_message=f"ZIP parse error: {str(e)}"
            )

        if PANDAS_AVAILABLE and all_records:
            result_df = pd.DataFrame(all_records)
        else:
            result_df = all_records

        return CollectorResult(
            success=len(all_records) > 0,
            source=self.config.source_name,
            records_fetched=len(all_records),
            data=result_df,
            warnings=warnings
        )

    def _discover_latest_ica_id(self) -> Optional[str]:
        """Discover the latest ICA report identifier from the INDEC page."""
        response, error = self._make_request(self.config.ica_page)

        if error or response.status_code != 200:
            self.logger.warning(f"ICA discovery failed: {error or response.status_code}")
            return None

        # Look for ICA digital identifiers in the page
        matches = re.findall(
            r'/ftp/ica_digital/(ica_d_\d{2}_[0-9A-Fa-f]+)/',
            response.text
        )

        if matches:
            # Return the first (most recent) match
            return matches[0]

        self.logger.warning("No ICA identifier found in page")
        return None

    def _parse_trade_csv(
        self,
        zf: zipfile.ZipFile,
        filename: str,
        flow: str,
        ica_id: str
    ) -> List[Dict]:
        """Parse a trade CSV file from the ICA ZIP."""
        records = []

        try:
            with zf.open(filename) as f:
                content = f.read()

            # Try Latin-1 first (INDEC standard), then UTF-8
            for encoding in ['latin-1', 'utf-8', 'iso-8859-1']:
                try:
                    text = content.decode(encoding)
                    break
                except UnicodeDecodeError:
                    continue
            else:
                logger.warning(f"Could not decode {filename}")
                return records

            lines = text.strip().split('\n')
            if len(lines) < 2:
                return records

            # Skip header row
            for line in lines[1:]:
                parts = line.strip().split(';')
                if len(parts) < 8:
                    continue

                ncm = parts[2].strip().replace('"', '')
                if ncm not in AG_NCM_CODES:
                    continue

                commodity = AG_NCM_CODES[ncm]

                try:
                    year = int(parts[0].strip().replace('"', ''))
                    month = int(parts[1].strip().replace('"', ''))
                except (ValueError, IndexError):
                    continue

                ncm_desc = parts[3].strip().replace('"', '')
                country_code = parts[4].strip().replace('"', '')
                country_name = parts[5].strip().replace('"', '')

                weight_raw = parts[6].strip().replace('"', '').replace(',', '.')
                fob_raw = parts[7].strip().replace('"', '').replace(',', '.')

                # Skip confidential values (s1-s10)
                weight_kg = self._safe_numeric(weight_raw)
                fob_usd = self._safe_numeric(fob_raw)

                record = {
                    'flow': flow,
                    'year': year,
                    'month': month,
                    'ncm_code': ncm,
                    'ncm_description': ncm_desc,
                    'commodity': commodity,
                    'country_code': country_code,
                    'country_name': country_name,
                    'weight_kg': weight_kg,
                    'fob_usd': fob_usd,
                    'ica_report_id': ica_id,
                    'source': 'INDEC',
                }

                # Import-specific fields
                if flow == 'import' and len(parts) >= 11:
                    record['freight_usd'] = self._safe_numeric(
                        parts[8].strip().replace('"', '').replace(',', '.')
                    )
                    record['insurance_usd'] = self._safe_numeric(
                        parts[9].strip().replace('"', '').replace(',', '.')
                    )
                    record['cif_usd'] = self._safe_numeric(
                        parts[10].strip().replace('"', '').replace(',', '.')
                    )

                records.append(record)

        except Exception as e:
            logger.warning(f"Error parsing {filename}: {e}")

        return records

    def _safe_numeric(self, value: str) -> Optional[float]:
        """Convert string to numeric, handling confidential codes."""
        if not value or value.startswith('s') or value in ('-', '...', ''):
            return None
        try:
            return float(value)
        except (ValueError, TypeError):
            return None

    def parse_response(self, response_data: Any) -> Any:
        return response_data

    def save_to_bronze(self, records: list) -> int:
        """Upsert records to bronze.indec_trade."""
        if not records:
            return 0
        with get_db_connection() as conn:
            cur = conn.cursor()
            count = 0
            for rec in records:
                cur.execute("""
                    INSERT INTO bronze.indec_trade
                        (flow, year, month, ncm_code, ncm_description,
                         commodity, country_code, country_name,
                         weight_kg, fob_usd, cif_usd, freight_usd,
                         insurance_usd, ica_report_id, source, collected_at)
                    VALUES
                        (%(flow)s, %(year)s, %(month)s, %(ncm_code)s,
                         %(ncm_description)s, %(commodity)s,
                         %(country_code)s, %(country_name)s,
                         %(weight_kg)s, %(fob_usd)s, %(cif_usd)s,
                         %(freight_usd)s, %(insurance_usd)s,
                         %(ica_report_id)s, %(source)s, NOW())
                    ON CONFLICT (flow, year, month, ncm_code, country_code)
                    DO UPDATE SET
                        weight_kg = EXCLUDED.weight_kg,
                        fob_usd = EXCLUDED.fob_usd,
                        cif_usd = EXCLUDED.cif_usd,
                        freight_usd = EXCLUDED.freight_usd,
                        insurance_usd = EXCLUDED.insurance_usd,
                        ica_report_id = EXCLUDED.ica_report_id,
                        collected_at = NOW()
                """, {
                    'flow': rec.get('flow'),
                    'year': rec.get('year'),
                    'month': rec.get('month'),
                    'ncm_code': rec.get('ncm_code', ''),
                    'ncm_description': rec.get('ncm_description', ''),
                    'commodity': rec.get('commodity', ''),
                    'country_code': rec.get('country_code', ''),
                    'country_name': rec.get('country_name', ''),
                    'weight_kg': rec.get('weight_kg'),
                    'fob_usd': rec.get('fob_usd'),
                    'cif_usd': rec.get('cif_usd'),
                    'freight_usd': rec.get('freight_usd'),
                    'insurance_usd': rec.get('insurance_usd'),
                    'ica_report_id': rec.get('ica_report_id', ''),
                    'source': rec.get('source', 'INDEC'),
                })
                count += 1
            conn.commit()
            self.logger.info(f"Saved {count} records to bronze.indec_trade")
            return count


def main():
    """CLI for INDEC collector"""
    import argparse
    logging.basicConfig(level=logging.INFO)

    parser = argparse.ArgumentParser(description='INDEC Argentina Trade Collector')
    parser.add_argument('command', choices=['exports', 'imports', 'both', 'test'])
    args = parser.parse_args()

    collector = INDECCollector()

    if args.command == 'test':
        success, msg = collector.test_connection()
        print(f"Test: {'PASS' if success else 'FAIL'} - {msg}")
        return

    flow = 'both' if args.command == 'both' else args.command.rstrip('s')
    result = collector.collect(flow=flow, use_cache=False)
    print(f"Success: {result.success}, Records: {result.records_fetched}")
    if result.warnings:
        print(f"Warnings: {result.warnings}")


if __name__ == '__main__':
    main()
