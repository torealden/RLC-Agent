"""
MPOB - Malaysian Palm Oil Board Data Collector

Collects palm oil production, stocks, and export data from MPOB.
Monthly data released around the 10th of the following month.

Data source: http://bepi.mpob.gov.my/

No API available - requires web scraping.
"""

import logging
import re
from dataclasses import dataclass, field
from datetime import datetime, date, timedelta
from typing import Dict, List, Optional, Any, Tuple
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


# MPOB Data Categories
MPOB_CATEGORIES = {
    'production': {
        'url_path': '/index.php/en/statistics/production.html',
        'description': 'Palm Oil Production',
        'unit': 'tonnes',
    },
    'stocks': {
        'url_path': '/index.php/en/statistics/stock.html',
        'description': 'Palm Oil Stocks',
        'unit': 'tonnes',
    },
    'exports': {
        'url_path': '/index.php/en/statistics/export.html',
        'description': 'Palm Oil Exports',
        'unit': 'tonnes',
    },
    'imports': {
        'url_path': '/index.php/en/statistics/import.html',
        'description': 'Palm Oil Imports',
        'unit': 'tonnes',
    },
    'prices': {
        'url_path': '/index.php/en/statistics/price.html',
        'description': 'Palm Oil Prices',
        'unit': 'MYR/tonne',
    },
}


@dataclass
class MPOBConfig(CollectorConfig):
    """MPOB specific configuration"""
    source_name: str = "MPOB Malaysia"
    source_url: str = "http://bepi.mpob.gov.my"
    auth_type: AuthType = AuthType.NONE
    frequency: DataFrequency = DataFrequency.MONTHLY

    # Categories to fetch
    categories: List[str] = field(default_factory=lambda: [
        'production', 'stocks', 'exports'
    ])

    # Request settings for scraping
    timeout: int = 30
    retry_attempts: int = 3


class MPOBCollector(BaseCollector):
    """
    Collector for Malaysian Palm Oil Board data.

    Scrapes monthly statistics from MPOB website:
    - CPO (Crude Palm Oil) production
    - Palm oil stocks
    - Export volumes by destination
    - Local prices

    Note: MPOB website structure may change; scraping logic
    may need updates.
    """

    def __init__(self, config: MPOBConfig = None):
        config = config or MPOBConfig()
        super().__init__(config)
        self.config: MPOBConfig = config

        # Update headers for scraping
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.5',
        })

    def get_table_name(self) -> str:
        return "mpob_palm_oil"

    def fetch_data(
        self,
        start_date: date = None,
        end_date: date = None,
        categories: List[str] = None,
        **kwargs
    ) -> CollectorResult:
        """
        Fetch palm oil data from MPOB website.

        Args:
            start_date: Start date (used for filtering parsed data)
            end_date: End date
            categories: List of categories to fetch

        Returns:
            CollectorResult with palm oil data
        """
        categories = categories or self.config.categories
        end_date = end_date or date.today()
        start_date = start_date or date(end_date.year - 1, 1, 1)

        all_records = []
        warnings = []

        for category in categories:
            if category not in MPOB_CATEGORIES:
                warnings.append(f"Unknown category: {category}")
                continue

            cat_info = MPOB_CATEGORIES[category]
            url = f"{self.config.source_url}{cat_info['url_path']}"

            self.logger.info(f"Fetching MPOB {category} data from {url}")

            response, error = self._make_request(url)

            if error:
                warnings.append(f"{category}: {error}")
                continue

            if response.status_code != 200:
                warnings.append(f"{category}: HTTP {response.status_code}")
                continue

            try:
                records = self._parse_html_tables(
                    response.text, category, cat_info
                )

                # Filter by date range
                for record in records:
                    record_date = self._parse_period_to_date(record.get('period'))
                    if record_date and start_date <= record_date <= end_date:
                        all_records.append(record)

            except Exception as e:
                warnings.append(f"{category}: Parse error - {e}")
                self.logger.error(f"Error parsing {category}: {e}", exc_info=True)

        if not all_records:
            return CollectorResult(
                success=len(warnings) == 0,
                source=self.config.source_name,
                error_message="No data retrieved" if not warnings else None,
                warnings=warnings
            )

        # Convert to DataFrame
        if PANDAS_AVAILABLE:
            df = pd.DataFrame(all_records)
            if 'period' in df.columns:
                df['date'] = df['period'].apply(self._parse_period_to_date)
                df = df.sort_values(['category', 'date'])
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

    def _parse_html_tables(
        self,
        html_content: str,
        category: str,
        cat_info: Dict
    ) -> List[Dict]:
        """Parse HTML tables from MPOB page"""
        records = []

        try:
            soup = BeautifulSoup(html_content, 'html.parser')

            # Find all tables
            tables = soup.find_all('table')

            for table in tables:
                # Try to parse each table
                table_records = self._parse_table(table, category, cat_info)
                records.extend(table_records)

        except Exception as e:
            self.logger.warning(f"Error parsing HTML: {e}")

        return records

    def _parse_table(
        self,
        table,
        category: str,
        cat_info: Dict
    ) -> List[Dict]:
        """Parse a single HTML table"""
        records = []

        try:
            rows = table.find_all('tr')
            if len(rows) < 2:
                return records

            # Get headers from first row
            header_row = rows[0]
            headers = [th.get_text(strip=True) for th in header_row.find_all(['th', 'td'])]

            # Process data rows
            for row in rows[1:]:
                cells = row.find_all(['td', 'th'])
                if len(cells) < 2:
                    continue

                values = [cell.get_text(strip=True) for cell in cells]

                # Try to identify period column
                period = None
                for i, val in enumerate(values):
                    if self._looks_like_period(val):
                        period = val
                        break

                if not period and values:
                    period = values[0]  # Assume first column is period

                # Parse numeric values
                for i, (header, value) in enumerate(zip(headers, values)):
                    if i == 0:  # Skip period column
                        continue

                    numeric_value = self._parse_numeric(value)
                    if numeric_value is not None:
                        records.append({
                            'category': category,
                            'period': period,
                            'metric': header,
                            'value': numeric_value,
                            'unit': cat_info['unit'],
                            'source': 'MPOB',
                        })

        except Exception as e:
            self.logger.warning(f"Error parsing table: {e}")

        return records

    def _looks_like_period(self, text: str) -> bool:
        """Check if text looks like a period (month/year)"""
        if not text:
            return False

        # Check for patterns like "Jan 2024", "January 2024", "2024-01", etc.
        patterns = [
            r'\b(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\w*\s+\d{4}\b',
            r'\b\d{4}[-/]\d{1,2}\b',
            r'\b\d{1,2}[-/]\d{4}\b',
        ]

        for pattern in patterns:
            if re.search(pattern, text, re.IGNORECASE):
                return True

        return False

    def _parse_period_to_date(self, period: str) -> Optional[date]:
        """Convert period string to date"""
        if not period:
            return None

        # Try various formats
        formats = [
            '%b %Y',      # Jan 2024
            '%B %Y',      # January 2024
            '%Y-%m',      # 2024-01
            '%m/%Y',      # 01/2024
            '%Y/%m',      # 2024/01
        ]

        for fmt in formats:
            try:
                return datetime.strptime(period.strip(), fmt).date()
            except ValueError:
                continue

        # Try to extract year and month from text
        match = re.search(r'(\w+)\s+(\d{4})', period)
        if match:
            month_str, year_str = match.groups()
            months = {
                'jan': 1, 'feb': 2, 'mar': 3, 'apr': 4,
                'may': 5, 'jun': 6, 'jul': 7, 'aug': 8,
                'sep': 9, 'oct': 10, 'nov': 11, 'dec': 12
            }
            month = months.get(month_str[:3].lower())
            if month:
                return date(int(year_str), month, 1)

        return None

    def _parse_numeric(self, text: str) -> Optional[float]:
        """Parse numeric value from text"""
        if not text:
            return None

        # Remove commas and whitespace
        cleaned = re.sub(r'[,\s]', '', text)

        # Try to extract number
        match = re.search(r'-?[\d.]+', cleaned)
        if match:
            try:
                return float(match.group())
            except ValueError:
                pass

        return None

    def parse_response(self, response_data: Any) -> Any:
        """Parse response data"""
        return response_data

    # =========================================================================
    # CONVENIENCE METHODS
    # =========================================================================

    def get_monthly_summary(self, year: int = None, month: int = None) -> Optional[Dict]:
        """
        Get monthly palm oil summary.

        Args:
            year: Year (default: current)
            month: Month (default: previous month)

        Returns:
            Dict with production, stocks, exports
        """
        if year is None:
            year = date.today().year
        if month is None:
            month = date.today().month - 1
            if month == 0:
                month = 12
                year -= 1

        target_date = date(year, month, 1)

        result = self.collect(
            start_date=target_date,
            end_date=target_date + timedelta(days=31)
        )

        if not result.success or result.data is None:
            return None

        summary = {
            'period': f"{year}-{month:02d}",
            'production': None,
            'stocks': None,
            'exports': None,
        }

        if PANDAS_AVAILABLE and hasattr(result.data, 'groupby'):
            for category in ['production', 'stocks', 'exports']:
                cat_data = result.data[result.data['category'] == category]
                if not cat_data.empty:
                    # Get total or main value
                    total_row = cat_data[cat_data['metric'].str.contains('total', case=False, na=False)]
                    if not total_row.empty:
                        summary[category] = total_row['value'].iloc[0]
                    else:
                        summary[category] = cat_data['value'].sum()

        return summary

    def get_production_trend(self, months: int = 12) -> Optional[Any]:
        """
        Get production trend for recent months.

        Args:
            months: Number of months

        Returns:
            DataFrame with monthly production
        """
        end_date = date.today()
        start_date = date(end_date.year - (months // 12), end_date.month - (months % 12), 1)

        if start_date.month <= 0:
            start_date = date(start_date.year - 1, start_date.month + 12, 1)

        result = self.collect(
            start_date=start_date,
            end_date=end_date,
            categories=['production']
        )

        return result.data if result.success else None


# =============================================================================
# CLI INTERFACE
# =============================================================================

def main():
    """Command-line interface for MPOB collector"""
    import argparse
    import json

    parser = argparse.ArgumentParser(description='MPOB Palm Oil Data Collector')

    parser.add_argument(
        'command',
        choices=['fetch', 'summary', 'test'],
        help='Command to execute'
    )

    parser.add_argument(
        '--categories',
        nargs='+',
        default=['production', 'stocks', 'exports'],
        help='Categories to fetch'
    )

    parser.add_argument(
        '--year',
        type=int,
        help='Year for summary'
    )

    parser.add_argument(
        '--month',
        type=int,
        help='Month for summary'
    )

    parser.add_argument(
        '--output',
        '-o',
        help='Output file (JSON or CSV)'
    )

    args = parser.parse_args()

    # Create collector
    config = MPOBConfig(categories=args.categories)
    collector = MPOBCollector(config)

    if args.command == 'test':
        success, message = collector.test_connection()
        print(f"Connection test: {'PASS' if success else 'FAIL'} - {message}")
        return

    if args.command == 'summary':
        summary = collector.get_monthly_summary(args.year, args.month)
        if summary:
            print(json.dumps(summary, indent=2, default=str))
        else:
            print("Failed to get monthly summary")
        return

    if args.command == 'fetch':
        result = collector.collect(categories=args.categories)

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
                    result.data.to_json(args.output, orient='records', date_format='iso')
                else:
                    with open(args.output, 'w') as f:
                        json.dump(result.data, f, default=str)
            print(f"Saved to: {args.output}")


if __name__ == '__main__':
    main()
