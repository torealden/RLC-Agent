"""
Canadian Grain Commission (CGC) Data Collector

Collects grain handling and inspection data from Canada:
- Weekly Visible Supply
- Weekly Grain Movement
- Grain Inspections
- Licensed Elevator Receipts

Data source: https://www.grainscanada.gc.ca/en/grain-research/statistics/
Free - No authentication required
"""

import logging
import re
from dataclasses import dataclass, field
from datetime import datetime, date, timedelta
from typing import Dict, List, Optional, Any
from bs4 import BeautifulSoup

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


# CGC Report Types and URLs
CGC_REPORTS = {
    'visible_supply': {
        'name': 'Visible Supply of Canadian Grain',
        'url': 'https://www.grainscanada.gc.ca/en/grain-research/statistics/visible-supply.html',
        'frequency': 'weekly',
        'release_day': 'Thursday',
        'description': 'Weekly stocks in licensed elevators',
    },
    'grain_movement': {
        'name': 'Weekly Grain Movement',
        'url': 'https://www.grainscanada.gc.ca/en/grain-research/statistics/movement.html',
        'frequency': 'weekly',
        'release_day': 'Thursday',
        'description': 'Weekly receipts, shipments, and exports',
    },
    'inspections': {
        'name': 'Grain Inspections',
        'url': 'https://www.grainscanada.gc.ca/en/grain-research/statistics/inspections.html',
        'frequency': 'weekly',
        'release_day': 'Thursday',
        'description': 'Export inspections by grade',
    },
    'producer_deliveries': {
        'name': 'Producer Deliveries',
        'url': 'https://www.grainscanada.gc.ca/en/grain-research/statistics/producer-deliveries.html',
        'frequency': 'weekly',
        'release_day': 'Thursday',
        'description': 'Farm deliveries to elevators',
    },
}

# CGC commodity codes
CGC_COMMODITIES = {
    'wheat_all': 'All Wheat',
    'wheat_cwrs': 'Canada Western Red Spring',
    'wheat_cwad': 'Canada Western Amber Durum',
    'wheat_cwsws': 'Canada Western Soft White Spring',
    'wheat_cwes': 'Canada Western Extra Strong',
    'wheat_cps': 'Canada Prairie Spring',
    'canola': 'Canola',
    'barley': 'Barley',
    'oats': 'Oats',
    'flaxseed': 'Flaxseed',
    'corn': 'Corn',
    'soybeans': 'Soybeans',
    'peas': 'Peas',
    'lentils': 'Lentils',
    'chickpeas': 'Chickpeas',
    'mustard': 'Mustard Seed',
    'sunflower': 'Sunflower Seed',
}


@dataclass
class CGCConfig(CollectorConfig):
    """Canadian Grain Commission configuration"""
    source_name: str = "Canadian Grain Commission"
    source_url: str = "https://www.grainscanada.gc.ca"
    auth_type: AuthType = AuthType.NONE
    frequency: DataFrequency = DataFrequency.WEEKLY

    # Report types to fetch
    report_types: List[str] = field(default_factory=lambda: [
        'visible_supply', 'grain_movement'
    ])

    # Commodities of interest
    commodities: List[str] = field(default_factory=lambda: [
        'wheat_cwrs', 'wheat_cwad', 'canola', 'barley', 'oats'
    ])

    # Request settings
    timeout: int = 30
    retry_attempts: int = 3


class CGCCollector(BaseCollector):
    """
    Collector for Canadian Grain Commission data.

    Provides:
    - Weekly visible supply (elevator stocks)
    - Weekly grain movement (receipts, exports)
    - Export inspections
    - Producer deliveries

    Data released every Thursday.
    """

    def __init__(self, config: CGCConfig = None):
        config = config or CGCConfig()
        super().__init__(config)
        self.config: CGCConfig = config

        # Update headers for web scraping
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
            'Accept-Language': 'en-CA,en;q=0.5',
        })

    def get_table_name(self) -> str:
        return "canada_cgc"

    def fetch_data(
        self,
        start_date: date = None,
        end_date: date = None,
        report_types: List[str] = None,
        commodities: List[str] = None,
        **kwargs
    ) -> CollectorResult:
        """
        Fetch CGC grain statistics.

        Args:
            report_types: List of report types ('visible_supply', 'grain_movement', etc.)
            commodities: List of commodities to filter

        Returns:
            CollectorResult with grain data
        """
        report_types = report_types or self.config.report_types
        commodities = commodities or self.config.commodities
        end_date = end_date or date.today()
        start_date = start_date or date(end_date.year - 1, 1, 1)

        all_records = []
        warnings = []

        for report_type in report_types:
            if report_type not in CGC_REPORTS:
                warnings.append(f"Unknown report type: {report_type}")
                continue

            report_info = CGC_REPORTS[report_type]
            self.logger.info(f"Fetching CGC {report_info['name']}")

            try:
                records = self._fetch_report(report_type, report_info, start_date, end_date)

                # Filter by commodity if specified
                if commodities:
                    commodity_names = [CGC_COMMODITIES.get(c, c) for c in commodities]
                    records = [r for r in records if any(
                        cn.lower() in r.get('commodity', '').lower()
                        for cn in commodity_names
                    )]

                all_records.extend(records)
                self.logger.info(f"Retrieved {len(records)} records from {report_type}")

            except Exception as e:
                warnings.append(f"{report_type}: {e}")
                self.logger.error(f"Error fetching {report_type}: {e}", exc_info=True)

        if not all_records:
            return CollectorResult(
                success=False,
                source=self.config.source_name,
                error_message="No data retrieved",
                warnings=warnings
            )

        # Convert to DataFrame
        if PANDAS_AVAILABLE:
            df = pd.DataFrame(all_records)
            df = df.sort_values(['report_type', 'report_date', 'commodity'])
        else:
            df = all_records

        return CollectorResult(
            success=True,
            source=self.config.source_name,
            records_fetched=len(all_records),
            data=df,
            period_start=start_date.isoformat(),
            period_end=end_date.isoformat(),
            warnings=warnings
        )

    def _fetch_report(
        self,
        report_type: str,
        report_info: Dict,
        start_date: date,
        end_date: date
    ) -> List[Dict]:
        """Fetch a specific CGC report"""
        records = []

        # CGC provides Excel downloads - look for download links
        url = report_info['url']
        response, error = self._make_request(url)

        if error:
            raise Exception(f"Request failed: {error}")

        if response.status_code != 200:
            raise Exception(f"HTTP {response.status_code}")

        # Parse HTML to find Excel download links
        soup = BeautifulSoup(response.text, 'html.parser')

        # Look for Excel file links
        excel_links = []
        for link in soup.find_all('a', href=True):
            href = link['href']
            if '.xlsx' in href.lower() or '.xls' in href.lower():
                full_url = href if href.startswith('http') else f"{self.config.source_url}{href}"
                excel_links.append({
                    'url': full_url,
                    'text': link.get_text(strip=True)
                })

        # Also look for data tables on the page
        tables = soup.find_all('table')
        for table in tables:
            table_records = self._parse_html_table(table, report_type)
            records.extend(table_records)

        # If Excel links found, attempt to download and parse
        if excel_links and PANDAS_AVAILABLE:
            for link_info in excel_links[:3]:  # Limit to 3 files
                try:
                    excel_records = self._fetch_excel(link_info['url'], report_type)
                    records.extend(excel_records)
                except Exception as e:
                    self.logger.warning(f"Failed to fetch Excel {link_info['url']}: {e}")

        return records

    def _fetch_excel(self, url: str, report_type: str) -> List[Dict]:
        """Fetch and parse Excel file"""
        records = []

        if not PANDAS_AVAILABLE:
            return records

        response, error = self._make_request(url)
        if error or response.status_code != 200:
            return records

        try:
            # Read Excel file
            df = pd.read_excel(response.content)

            # Find date columns and data
            for col in df.columns:
                if 'date' in str(col).lower() or 'week' in str(col).lower():
                    df['report_date'] = pd.to_datetime(df[col], errors='coerce')
                    break

            # Convert to records
            for _, row in df.iterrows():
                record = {
                    'report_type': report_type,
                    'source': 'CGC',
                }
                for col in df.columns:
                    record[str(col).lower().replace(' ', '_')] = row[col]
                records.append(record)

        except Exception as e:
            self.logger.warning(f"Error parsing Excel: {e}")

        return records

    def _parse_html_table(self, table, report_type: str) -> List[Dict]:
        """Parse HTML table from CGC page"""
        records = []

        try:
            rows = table.find_all('tr')
            if len(rows) < 2:
                return records

            # Get headers
            header_row = rows[0]
            headers = [th.get_text(strip=True) for th in header_row.find_all(['th', 'td'])]

            if not headers:
                return records

            # Parse data rows
            for row in rows[1:]:
                cells = row.find_all(['td', 'th'])
                if len(cells) < 2:
                    continue

                values = [cell.get_text(strip=True) for cell in cells]

                record = {
                    'report_type': report_type,
                    'source': 'CGC',
                }

                for i, (header, value) in enumerate(zip(headers, values)):
                    clean_header = re.sub(r'[^\w\s]', '', header).strip().lower().replace(' ', '_')
                    if clean_header:
                        # Try to parse numbers
                        numeric_val = self._parse_numeric(value)
                        record[clean_header] = numeric_val if numeric_val is not None else value

                # Try to identify commodity from first column
                if values:
                    record['commodity'] = values[0]

                records.append(record)

        except Exception as e:
            self.logger.warning(f"Error parsing table: {e}")

        return records

    def _parse_numeric(self, text: str) -> Optional[float]:
        """Parse numeric value from text"""
        if not text:
            return None

        # Remove commas, whitespace
        cleaned = re.sub(r'[,\s]', '', text)

        # Extract number
        match = re.search(r'-?[\d.]+', cleaned)
        if match:
            try:
                return float(match.group())
            except ValueError:
                pass

        return None

    def parse_response(self, response_data: Any) -> Any:
        return response_data

    # =========================================================================
    # CONVENIENCE METHODS
    # =========================================================================

    def get_visible_supply(self, commodity: str = 'wheat_cwrs') -> Optional[Any]:
        """
        Get latest visible supply for a commodity.

        Args:
            commodity: Commodity code

        Returns:
            DataFrame with visible supply data
        """
        result = self.collect(
            report_types=['visible_supply'],
            commodities=[commodity]
        )

        return result.data if result.success else None

    def get_weekly_exports(self, commodity: str = 'wheat_cwrs') -> Optional[Any]:
        """
        Get weekly export data for a commodity.

        Args:
            commodity: Commodity code

        Returns:
            DataFrame with export data
        """
        result = self.collect(
            report_types=['grain_movement'],
            commodities=[commodity]
        )

        return result.data if result.success else None

    def get_canola_movement(self) -> Optional[Any]:
        """Get canola movement data"""
        return self.get_weekly_exports('canola')

    def get_wheat_movement(self) -> Optional[Any]:
        """Get all wheat movement data"""
        return self.get_weekly_exports('wheat_all')


# =============================================================================
# CLI INTERFACE
# =============================================================================

def main():
    """CLI for CGC collector"""
    import argparse
    import json

    parser = argparse.ArgumentParser(description='Canadian Grain Commission Data Collector')

    parser.add_argument(
        'command',
        choices=['fetch', 'visible', 'exports', 'test'],
        help='Command to execute'
    )

    parser.add_argument(
        '--report-types',
        nargs='+',
        default=['visible_supply', 'grain_movement'],
        help='Report types to fetch'
    )

    parser.add_argument(
        '--commodities',
        nargs='+',
        default=['wheat_cwrs', 'canola'],
        help='Commodities to fetch'
    )

    parser.add_argument(
        '--output', '-o',
        help='Output file (CSV or JSON)'
    )

    args = parser.parse_args()

    collector = CGCCollector()

    if args.command == 'test':
        success, message = collector.test_connection()
        print(f"Connection test: {'PASS' if success else 'FAIL'} - {message}")
        return

    if args.command == 'visible':
        data = collector.get_visible_supply()
        if data is not None and PANDAS_AVAILABLE:
            print(data.to_string())
        return

    if args.command == 'exports':
        data = collector.get_weekly_exports()
        if data is not None and PANDAS_AVAILABLE:
            print(data.to_string())
        return

    if args.command == 'fetch':
        result = collector.collect(
            report_types=args.report_types,
            commodities=args.commodities
        )

        print(f"Success: {result.success}")
        print(f"Records: {result.records_fetched}")

        if result.warnings:
            print(f"Warnings: {result.warnings}")

        if args.output and result.data is not None:
            if args.output.endswith('.csv') and PANDAS_AVAILABLE:
                result.data.to_csv(args.output, index=False)
            else:
                with open(args.output, 'w') as f:
                    if PANDAS_AVAILABLE and hasattr(result.data, 'to_dict'):
                        json.dump(result.data.to_dict('records'), f, indent=2, default=str)
                    else:
                        json.dump(result.data, f, indent=2, default=str)
            print(f"Saved to: {args.output}")


if __name__ == '__main__':
    main()
