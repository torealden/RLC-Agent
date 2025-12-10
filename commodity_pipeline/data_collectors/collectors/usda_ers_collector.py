"""
USDA ERS Database Collectors

Collectors for USDA Economic Research Service data products:
- Feed Grains Database (corn, sorghum, barley, oats)
- Oil Crops Yearbook (soybeans, canola, sunflower, etc.)
- Wheat Data (all wheat classes)

Data is typically available as Excel/CSV downloads that are updated monthly.
"""

import logging
import io
import re
from dataclasses import dataclass, field
from datetime import datetime, date, timedelta
from typing import Dict, List, Optional, Any
from pathlib import Path

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


# =============================================================================
# FEED GRAINS DATABASE COLLECTOR
# =============================================================================

@dataclass
class FeedGrainsConfig(CollectorConfig):
    """USDA ERS Feed Grains Database configuration"""
    source_name: str = "USDA ERS Feed Grains"
    source_url: str = "https://www.ers.usda.gov/data-products/feed-grains-database/"
    auth_type: AuthType = AuthType.NONE
    frequency: DataFrequency = DataFrequency.MONTHLY

    # Direct download URLs for yearbook tables
    yearbook_url: str = "https://www.ers.usda.gov/webdocs/DataFiles/50048/FeedGrainsYearbookTables2024.xlsx"

    # Commodities covered
    commodities: List[str] = field(default_factory=lambda: [
        'corn', 'sorghum', 'barley', 'oats'
    ])


class FeedGrainsCollector(BaseCollector):
    """
    Collector for USDA ERS Feed Grains Database.

    Provides:
    - Supply and use data for corn, sorghum, barley, oats
    - Prices (farm, wholesale)
    - Trade data
    - Historical data back to 1866

    Data is updated monthly with the Feed Outlook report.
    """

    def __init__(self, config: FeedGrainsConfig = None):
        config = config or FeedGrainsConfig()
        super().__init__(config)
        self.config: FeedGrainsConfig = config

    def get_table_name(self) -> str:
        return "usda_ers_feed_grains"

    def fetch_data(
        self,
        start_date: date = None,
        end_date: date = None,
        tables: List[str] = None,
        **kwargs
    ) -> CollectorResult:
        """
        Fetch Feed Grains data from USDA ERS.

        Args:
            start_date: Start of marketing year range
            end_date: End of marketing year range
            tables: Specific tables to fetch (default: all)
        """
        if not PANDAS_AVAILABLE:
            return CollectorResult(
                success=False,
                source=self.config.source_name,
                error_message="pandas required for Excel parsing"
            )

        # Try to download the yearbook Excel file
        response, error = self._make_request(
            self.config.yearbook_url,
            timeout=60
        )

        if error:
            return CollectorResult(
                success=False,
                source=self.config.source_name,
                error_message=error
            )

        if response.status_code != 200:
            return CollectorResult(
                success=False,
                source=self.config.source_name,
                error_message=f"HTTP {response.status_code}"
            )

        try:
            # Parse Excel file
            excel_data = pd.ExcelFile(io.BytesIO(response.content))

            all_records = []
            warnings = []

            # Process each sheet
            for sheet_name in excel_data.sheet_names:
                try:
                    df = pd.read_excel(excel_data, sheet_name=sheet_name)
                    records = self._parse_sheet(df, sheet_name)
                    all_records.extend(records)
                except Exception as e:
                    warnings.append(f"Sheet {sheet_name}: {e}")

            if not all_records:
                return CollectorResult(
                    success=False,
                    source=self.config.source_name,
                    error_message="No data parsed from Excel file",
                    warnings=warnings
                )

            # Convert to DataFrame
            result_df = pd.DataFrame(all_records)

            return CollectorResult(
                success=True,
                source=self.config.source_name,
                records_fetched=len(all_records),
                data=result_df,
                warnings=warnings
            )

        except Exception as e:
            return CollectorResult(
                success=False,
                source=self.config.source_name,
                error_message=f"Excel parsing error: {e}"
            )

    def _parse_sheet(self, df: pd.DataFrame, sheet_name: str) -> List[Dict]:
        """Parse a single Excel sheet into records"""
        records = []

        # Identify commodity from sheet name
        commodity = None
        for c in self.config.commodities:
            if c.lower() in sheet_name.lower():
                commodity = c
                break

        if not commodity:
            commodity = 'unknown'

        # Try to find header row (usually contains 'Year' or marketing year)
        header_row = 0
        for i, row in df.iterrows():
            if any('year' in str(v).lower() for v in row.values if pd.notna(v)):
                header_row = i
                break

        # Re-read with correct header
        if header_row > 0:
            df.columns = df.iloc[header_row]
            df = df.iloc[header_row + 1:]

        # Process rows
        for _, row in df.iterrows():
            try:
                # Try to extract year/marketing year
                year_val = None
                for col in df.columns:
                    if 'year' in str(col).lower():
                        year_val = row[col]
                        break

                if year_val is None:
                    year_val = row.iloc[0] if len(row) > 0 else None

                # Extract numeric values
                for col in df.columns:
                    if col is None or 'year' in str(col).lower():
                        continue

                    value = row[col]
                    if pd.notna(value) and isinstance(value, (int, float)):
                        records.append({
                            'commodity': commodity,
                            'sheet': sheet_name,
                            'year': str(year_val) if year_val else None,
                            'metric': str(col),
                            'value': float(value),
                            'source': 'USDA_ERS_FEED_GRAINS',
                        })
            except Exception:
                continue

        return records

    def parse_response(self, response_data: Any) -> Any:
        return response_data


# =============================================================================
# OIL CROPS YEARBOOK COLLECTOR
# =============================================================================

@dataclass
class OilCropsConfig(CollectorConfig):
    """USDA ERS Oil Crops Yearbook configuration"""
    source_name: str = "USDA ERS Oil Crops"
    source_url: str = "https://www.ers.usda.gov/data-products/oil-crops-yearbook/"
    auth_type: AuthType = AuthType.NONE
    frequency: DataFrequency = DataFrequency.MONTHLY

    # Direct download URLs
    base_download_url: str = "https://www.ers.usda.gov/webdocs/DataFiles/52218/"

    # Available datasets
    datasets: Dict[str, str] = field(default_factory=lambda: {
        'soybeans': 'SoybeansOilCrops.xlsx',
        'sunflower': 'Sunflower.xlsx',
        'canola': 'Canola.xlsx',
        'cottonseed': 'Cottonseed.xlsx',
        'peanuts': 'Peanuts.xlsx',
        'flaxseed': 'Flaxseed.xlsx',
    })


class OilCropsCollector(BaseCollector):
    """
    Collector for USDA ERS Oil Crops Yearbook.

    Provides supply, use, stocks, and price data for:
    - Soybeans, soybean meal, soybean oil
    - Sunflower seeds, sunflower meal, sunflower oil
    - Canola/rapeseed, canola meal, canola oil
    - Cottonseed, peanuts, flaxseed
    """

    def __init__(self, config: OilCropsConfig = None):
        config = config or OilCropsConfig()
        super().__init__(config)
        self.config: OilCropsConfig = config

    def get_table_name(self) -> str:
        return "usda_ers_oil_crops"

    def fetch_data(
        self,
        start_date: date = None,
        end_date: date = None,
        commodities: List[str] = None,
        **kwargs
    ) -> CollectorResult:
        """
        Fetch Oil Crops data from USDA ERS.
        """
        if not PANDAS_AVAILABLE:
            return CollectorResult(
                success=False,
                source=self.config.source_name,
                error_message="pandas required for Excel parsing"
            )

        commodities = commodities or list(self.config.datasets.keys())

        all_records = []
        warnings = []

        for commodity in commodities:
            if commodity not in self.config.datasets:
                warnings.append(f"Unknown commodity: {commodity}")
                continue

            filename = self.config.datasets[commodity]
            url = f"{self.config.base_download_url}{filename}"

            response, error = self._make_request(url, timeout=60)

            if error:
                warnings.append(f"{commodity}: {error}")
                continue

            if response.status_code != 200:
                warnings.append(f"{commodity}: HTTP {response.status_code}")
                continue

            try:
                excel_data = pd.ExcelFile(io.BytesIO(response.content))

                for sheet_name in excel_data.sheet_names:
                    try:
                        df = pd.read_excel(excel_data, sheet_name=sheet_name)
                        records = self._parse_oil_crops_sheet(df, commodity, sheet_name)
                        all_records.extend(records)
                    except Exception as e:
                        warnings.append(f"{commodity}/{sheet_name}: {e}")

            except Exception as e:
                warnings.append(f"{commodity}: Excel error - {e}")

        if not all_records:
            return CollectorResult(
                success=False,
                source=self.config.source_name,
                error_message="No data retrieved",
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

    def _parse_oil_crops_sheet(
        self,
        df: pd.DataFrame,
        commodity: str,
        sheet_name: str
    ) -> List[Dict]:
        """Parse an oil crops Excel sheet"""
        records = []

        # Determine sub-commodity (seed, meal, oil)
        sub_commodity = commodity
        sheet_lower = sheet_name.lower()
        if 'meal' in sheet_lower:
            sub_commodity = f"{commodity}_meal"
        elif 'oil' in sheet_lower:
            sub_commodity = f"{commodity}_oil"

        # Find header row
        header_row = 0
        for i in range(min(10, len(df))):
            row = df.iloc[i]
            if any('year' in str(v).lower() for v in row.values if pd.notna(v)):
                header_row = i
                break

        if header_row > 0:
            df.columns = df.iloc[header_row]
            df = df.iloc[header_row + 1:]

        # Process data
        for _, row in df.iterrows():
            try:
                year_val = row.iloc[0] if len(row) > 0 else None

                for col in df.columns[1:]:
                    if col is None:
                        continue
                    value = row[col]
                    if pd.notna(value) and isinstance(value, (int, float)):
                        records.append({
                            'commodity': sub_commodity,
                            'sheet': sheet_name,
                            'year': str(year_val) if year_val else None,
                            'metric': str(col),
                            'value': float(value),
                            'source': 'USDA_ERS_OIL_CROPS',
                        })
            except Exception:
                continue

        return records

    def parse_response(self, response_data: Any) -> Any:
        return response_data


# =============================================================================
# WHEAT DATA COLLECTOR
# =============================================================================

@dataclass
class WheatDataConfig(CollectorConfig):
    """USDA ERS Wheat Data configuration"""
    source_name: str = "USDA ERS Wheat"
    source_url: str = "https://www.ers.usda.gov/data-products/wheat-data/"
    auth_type: AuthType = AuthType.NONE
    frequency: DataFrequency = DataFrequency.MONTHLY

    # Download URL
    yearbook_url: str = "https://www.ers.usda.gov/webdocs/DataFiles/54096/Wheat-Data-Yearbook-Tables.xlsx"

    # Wheat classes
    wheat_classes: List[str] = field(default_factory=lambda: [
        'all_wheat', 'hard_red_winter', 'hard_red_spring',
        'soft_red_winter', 'white', 'durum'
    ])


class WheatDataCollector(BaseCollector):
    """
    Collector for USDA ERS Wheat Data.

    Provides supply, use, trade, and price data for all wheat classes:
    - Hard Red Winter (HRW)
    - Hard Red Spring (HRS)
    - Soft Red Winter (SRW)
    - White
    - Durum
    """

    def __init__(self, config: WheatDataConfig = None):
        config = config or WheatDataConfig()
        super().__init__(config)
        self.config: WheatDataConfig = config

    def get_table_name(self) -> str:
        return "usda_ers_wheat"

    def fetch_data(
        self,
        start_date: date = None,
        end_date: date = None,
        **kwargs
    ) -> CollectorResult:
        """Fetch Wheat data from USDA ERS."""
        if not PANDAS_AVAILABLE:
            return CollectorResult(
                success=False,
                source=self.config.source_name,
                error_message="pandas required for Excel parsing"
            )

        response, error = self._make_request(
            self.config.yearbook_url,
            timeout=60
        )

        if error:
            return CollectorResult(
                success=False,
                source=self.config.source_name,
                error_message=error
            )

        if response.status_code != 200:
            return CollectorResult(
                success=False,
                source=self.config.source_name,
                error_message=f"HTTP {response.status_code}"
            )

        try:
            excel_data = pd.ExcelFile(io.BytesIO(response.content))

            all_records = []
            warnings = []

            for sheet_name in excel_data.sheet_names:
                try:
                    df = pd.read_excel(excel_data, sheet_name=sheet_name)
                    records = self._parse_wheat_sheet(df, sheet_name)
                    all_records.extend(records)
                except Exception as e:
                    warnings.append(f"Sheet {sheet_name}: {e}")

            if not all_records:
                return CollectorResult(
                    success=False,
                    source=self.config.source_name,
                    error_message="No data parsed",
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

        except Exception as e:
            return CollectorResult(
                success=False,
                source=self.config.source_name,
                error_message=f"Excel parsing error: {e}"
            )

    def _parse_wheat_sheet(self, df: pd.DataFrame, sheet_name: str) -> List[Dict]:
        """Parse a wheat data Excel sheet"""
        records = []

        # Identify wheat class from sheet name
        wheat_class = 'all_wheat'
        sheet_lower = sheet_name.lower()

        if 'hrw' in sheet_lower or 'hard red winter' in sheet_lower:
            wheat_class = 'hard_red_winter'
        elif 'hrs' in sheet_lower or 'hard red spring' in sheet_lower:
            wheat_class = 'hard_red_spring'
        elif 'srw' in sheet_lower or 'soft red winter' in sheet_lower:
            wheat_class = 'soft_red_winter'
        elif 'white' in sheet_lower:
            wheat_class = 'white'
        elif 'durum' in sheet_lower:
            wheat_class = 'durum'

        # Find header row
        header_row = 0
        for i in range(min(10, len(df))):
            row = df.iloc[i]
            if any('year' in str(v).lower() for v in row.values if pd.notna(v)):
                header_row = i
                break

        if header_row > 0:
            df.columns = df.iloc[header_row]
            df = df.iloc[header_row + 1:]

        # Process data
        for _, row in df.iterrows():
            try:
                year_val = row.iloc[0] if len(row) > 0 else None

                for col in df.columns[1:]:
                    if col is None:
                        continue
                    value = row[col]
                    if pd.notna(value) and isinstance(value, (int, float)):
                        records.append({
                            'commodity': 'wheat',
                            'wheat_class': wheat_class,
                            'sheet': sheet_name,
                            'year': str(year_val) if year_val else None,
                            'metric': str(col),
                            'value': float(value),
                            'source': 'USDA_ERS_WHEAT',
                        })
            except Exception:
                continue

        return records

    def parse_response(self, response_data: Any) -> Any:
        return response_data


# =============================================================================
# CLI INTERFACE
# =============================================================================

def main():
    """CLI for USDA ERS collectors"""
    import argparse
    import json

    parser = argparse.ArgumentParser(description='USDA ERS Data Collectors')

    parser.add_argument(
        'collector',
        choices=['feed_grains', 'oil_crops', 'wheat'],
        help='Collector to run'
    )

    parser.add_argument(
        '--output', '-o',
        help='Output file (JSON or CSV)'
    )

    args = parser.parse_args()

    # Select collector
    if args.collector == 'feed_grains':
        collector = FeedGrainsCollector()
    elif args.collector == 'oil_crops':
        collector = OilCropsCollector()
    else:
        collector = WheatDataCollector()

    # Run collection
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
        else:
            if PANDAS_AVAILABLE and hasattr(result.data, 'to_json'):
                result.data.to_json(args.output, orient='records')
        print(f"Saved to: {args.output}")


if __name__ == '__main__':
    main()
