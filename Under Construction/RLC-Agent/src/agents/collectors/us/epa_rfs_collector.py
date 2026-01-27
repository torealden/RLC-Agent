"""
EPA RFS (Renewable Fuel Standard) Data Collector

Collects RIN (Renewable Identification Number) data from EPA EMTS:
- RIN generation by D-code
- RIN retirements
- Biofuel volumes by pathway

Data source: https://www.epa.gov/fuels-registration-reporting-and-compliance-help/public-data-renewable-fuel-standard
"""

import logging
import io
import re
from dataclasses import dataclass, field
from datetime import datetime, date, timedelta
from typing import Dict, List, Optional, Any

from .base_collector import (
    BaseCollector,
    CollectorConfig,
    CollectorResult,
    DataFrequency,
    AuthType
)

try:
    import pandas as pd
    PANDAS_AVAILABLE = True
except ImportError:
    PANDAS_AVAILABLE = False

logger = logging.getLogger(__name__)


# RIN D-Code definitions
RIN_DCODES = {
    'D3': {
        'name': 'Cellulosic Biofuel',
        'description': 'Cellulosic ethanol, cellulosic diesel',
        'ghg_reduction': '60%',
    },
    'D4': {
        'name': 'Biomass-Based Diesel',
        'description': 'Biodiesel, renewable diesel from qualified feedstocks',
        'ghg_reduction': '50%',
    },
    'D5': {
        'name': 'Advanced Biofuel',
        'description': 'Sugarcane ethanol, other advanced biofuels',
        'ghg_reduction': '50%',
    },
    'D6': {
        'name': 'Renewable Fuel',
        'description': 'Corn ethanol (conventional)',
        'ghg_reduction': '20%',
    },
    'D7': {
        'name': 'Cellulosic Diesel',
        'description': 'Diesel from cellulosic feedstocks',
        'ghg_reduction': '60%',
    },
}


@dataclass
class EPARFSConfig(CollectorConfig):
    """EPA RFS configuration"""
    source_name: str = "EPA RFS"
    source_url: str = "https://www.epa.gov/fuels-registration-reporting-and-compliance-help/public-data-renewable-fuel-standard"
    auth_type: AuthType = AuthType.NONE
    frequency: DataFrequency = DataFrequency.MONTHLY

    # Direct data URLs (these may need periodic updates)
    rin_generation_url: str = "https://www.epa.gov/sites/default/files/documents/rin-generation-and-retirement-monthly.xls"

    # D-codes to track
    dcodes: List[str] = field(default_factory=lambda: ['D3', 'D4', 'D5', 'D6', 'D7'])


class EPARFSCollector(BaseCollector):
    """
    Collector for EPA Renewable Fuel Standard data.

    Provides:
    - RIN generation volumes by D-code
    - RIN retirement volumes
    - Biofuel production pathways
    - Feedstock usage

    Data is typically updated monthly, with a ~2 month lag.
    """

    def __init__(self, config: EPARFSConfig = None):
        config = config or EPARFSConfig()
        super().__init__(config)
        self.config: EPARFSConfig = config

    def get_table_name(self) -> str:
        return "epa_rfs_rins"

    def fetch_data(
        self,
        start_date: date = None,
        end_date: date = None,
        dcodes: List[str] = None,
        **kwargs
    ) -> CollectorResult:
        """
        Fetch RFS data from EPA.

        Note: EPA provides data via Excel downloads that are updated monthly.
        This collector attempts to parse the publicly available data files.
        """
        if not PANDAS_AVAILABLE:
            return CollectorResult(
                success=False,
                source=self.config.source_name,
                error_message="pandas required for Excel parsing"
            )

        dcodes = dcodes or self.config.dcodes
        all_records = []
        warnings = []

        # Try to fetch the RIN generation/retirement data
        response, error = self._make_request(
            self.config.rin_generation_url,
            timeout=60
        )

        if error:
            # Try alternative URL format
            warnings.append(f"Primary URL failed: {error}")
            return self._try_alternative_fetch(dcodes, warnings)

        if response.status_code != 200:
            warnings.append(f"HTTP {response.status_code}")
            return self._try_alternative_fetch(dcodes, warnings)

        try:
            # Try to parse as Excel
            content_type = response.headers.get('content-type', '')

            if 'excel' in content_type.lower() or 'spreadsheet' in content_type.lower():
                excel_data = pd.ExcelFile(io.BytesIO(response.content))

                for sheet_name in excel_data.sheet_names:
                    try:
                        df = pd.read_excel(excel_data, sheet_name=sheet_name)
                        records = self._parse_rin_sheet(df, sheet_name, dcodes)
                        all_records.extend(records)
                    except Exception as e:
                        warnings.append(f"Sheet {sheet_name}: {e}")
            else:
                # May be HTML or other format
                return self._try_alternative_fetch(dcodes, warnings)

        except Exception as e:
            warnings.append(f"Parse error: {e}")
            return self._try_alternative_fetch(dcodes, warnings)

        if not all_records:
            return CollectorResult(
                success=False,
                source=self.config.source_name,
                error_message="No data parsed from EPA files",
                warnings=warnings
            )

        result_df = pd.DataFrame(all_records)

        return CollectorResult(
            success=True,
            source=self.config.source_name,
            records_fetched=len(all_records),
            data=result_df,
            warnings=warnings
        )

    def _try_alternative_fetch(
        self,
        dcodes: List[str],
        warnings: List[str]
    ) -> CollectorResult:
        """
        Alternative approach: Try to scrape or use known data patterns.
        """
        # For now, return a structured empty result with guidance
        return CollectorResult(
            success=False,
            source=self.config.source_name,
            error_message="EPA data files not accessible. Manual download may be required.",
            warnings=warnings + [
                "EPA RFS data requires manual download from:",
                "https://www.epa.gov/fuels-registration-reporting-and-compliance-help/public-data-renewable-fuel-standard"
            ]
        )

    def _parse_rin_sheet(
        self,
        df: pd.DataFrame,
        sheet_name: str,
        dcodes: List[str]
    ) -> List[Dict]:
        """Parse a RIN data sheet"""
        records = []

        # Determine if this is generation or retirement data
        is_generation = 'gen' in sheet_name.lower()
        data_type = 'generation' if is_generation else 'retirement'

        # Try to find header row
        header_row = 0
        for i in range(min(10, len(df))):
            row = df.iloc[i]
            if any('year' in str(v).lower() or 'month' in str(v).lower()
                   for v in row.values if pd.notna(v)):
                header_row = i
                break

        if header_row > 0:
            df.columns = df.iloc[header_row]
            df = df.iloc[header_row + 1:]

        # Process rows
        for _, row in df.iterrows():
            try:
                # Try to extract year/month
                year = None
                month = None

                for col in df.columns:
                    col_str = str(col).lower()
                    if 'year' in col_str:
                        year = row[col]
                    elif 'month' in col_str:
                        month = row[col]

                # Look for D-code columns
                for col in df.columns:
                    col_str = str(col).upper()
                    for dcode in dcodes:
                        if dcode in col_str:
                            value = row[col]
                            if pd.notna(value) and isinstance(value, (int, float)):
                                records.append({
                                    'year': int(year) if year else None,
                                    'month': int(month) if month else None,
                                    'dcode': dcode,
                                    'dcode_name': RIN_DCODES.get(dcode, {}).get('name', ''),
                                    'data_type': data_type,
                                    'volume_gallons': float(value),
                                    'source': 'EPA_RFS',
                                })
                            break

            except Exception:
                continue

        return records

    def parse_response(self, response_data: Any) -> Any:
        return response_data

    # =========================================================================
    # MANUAL DATA ENTRY SUPPORT
    # =========================================================================

    def load_from_file(self, file_path: str) -> CollectorResult:
        """
        Load RIN data from a manually downloaded file.

        Args:
            file_path: Path to Excel/CSV file downloaded from EPA

        Returns:
            CollectorResult with parsed data
        """
        if not PANDAS_AVAILABLE:
            return CollectorResult(
                success=False,
                source=self.config.source_name,
                error_message="pandas required"
            )

        try:
            if file_path.endswith('.xlsx') or file_path.endswith('.xls'):
                excel_data = pd.ExcelFile(file_path)
                all_records = []

                for sheet_name in excel_data.sheet_names:
                    df = pd.read_excel(excel_data, sheet_name=sheet_name)
                    records = self._parse_rin_sheet(df, sheet_name, self.config.dcodes)
                    all_records.extend(records)

                result_df = pd.DataFrame(all_records)

            elif file_path.endswith('.csv'):
                result_df = pd.read_csv(file_path)

            else:
                return CollectorResult(
                    success=False,
                    source=self.config.source_name,
                    error_message="Unsupported file format"
                )

            return CollectorResult(
                success=True,
                source=self.config.source_name,
                records_fetched=len(result_df),
                data=result_df
            )

        except Exception as e:
            return CollectorResult(
                success=False,
                source=self.config.source_name,
                error_message=str(e)
            )

    def get_dcode_summary(self) -> Dict[str, Dict]:
        """Get summary of D-code definitions"""
        return RIN_DCODES


# =============================================================================
# CLI INTERFACE
# =============================================================================

def main():
    """CLI for EPA RFS collector"""
    import argparse

    parser = argparse.ArgumentParser(description='EPA RFS Data Collector')

    parser.add_argument(
        'command',
        choices=['fetch', 'load', 'dcodes'],
        help='Command to execute'
    )

    parser.add_argument(
        '--file',
        help='Local file to load (for load command)'
    )

    parser.add_argument(
        '--output', '-o',
        help='Output file'
    )

    args = parser.parse_args()

    collector = EPARFSCollector()

    if args.command == 'dcodes':
        import json
        print(json.dumps(collector.get_dcode_summary(), indent=2))
        return

    if args.command == 'load':
        if not args.file:
            print("Error: --file required for load command")
            return

        result = collector.load_from_file(args.file)

    else:
        result = collector.collect()

    print(f"Success: {result.success}")
    print(f"Records: {result.records_fetched}")

    if result.warnings:
        print(f"Warnings: {result.warnings}")

    if result.error_message:
        print(f"Error: {result.error_message}")

    if args.output and result.data is not None:
        if args.output.endswith('.csv') and PANDAS_AVAILABLE:
            result.data.to_csv(args.output, index=False)
        print(f"Saved to: {args.output}")


if __name__ == '__main__':
    main()
