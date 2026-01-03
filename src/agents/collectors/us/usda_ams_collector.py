"""
USDA AMS Market News Collectors

Collects data from USDA Agricultural Marketing Service:
- Tallow and Protein Report (animal fats, greases)
- Grain Co-Products Report (DDGS, corn oil)
- National Weekly Ethanol Report

Data is free and updated weekly.
"""

import logging
import re
from dataclasses import dataclass, field
from datetime import datetime, date, timedelta
from typing import Dict, List, Optional, Any, Tuple

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
# TALLOW AND PROTEIN REPORT COLLECTOR
# =============================================================================

@dataclass
class TallowProteinConfig(CollectorConfig):
    """USDA AMS Tallow and Protein Report configuration"""
    source_name: str = "USDA AMS Tallow"
    source_url: str = "https://www.ams.usda.gov/mnreports/nw_ls442.txt"
    auth_type: AuthType = AuthType.NONE
    frequency: DataFrequency = DataFrequency.WEEKLY

    # Report URLs
    central_us_url: str = "https://www.ams.usda.gov/mnreports/nw_ls442.txt"


class TallowProteinCollector(BaseCollector):
    """
    Collector for USDA AMS Tallow and Protein Report.

    Provides FOB Central US prices for:
    - Choice White Grease (CWG)
    - Yellow Grease
    - Bleachable Fancy Tallow (BFT)
    - Packer/Renderer Tallow
    - Edible Tallow
    - Lard
    - Poultry Fat (indirect)

    Updated weekly on Friday.
    """

    def __init__(self, config: TallowProteinConfig = None):
        config = config or TallowProteinConfig()
        super().__init__(config)
        self.config: TallowProteinConfig = config

    def get_table_name(self) -> str:
        return "usda_ams_tallow"

    def fetch_data(
        self,
        start_date: date = None,
        end_date: date = None,
        **kwargs
    ) -> CollectorResult:
        """Fetch Tallow and Protein Report data"""
        response, error = self._make_request(
            self.config.central_us_url,
            timeout=30
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
            text = response.text
            records = self._parse_tallow_report(text)

            if not records:
                return CollectorResult(
                    success=False,
                    source=self.config.source_name,
                    error_message="No data parsed from report"
                )

            if PANDAS_AVAILABLE:
                df = pd.DataFrame(records)
            else:
                df = records

            return CollectorResult(
                success=True,
                source=self.config.source_name,
                records_fetched=len(records),
                data=df
            )

        except Exception as e:
            return CollectorResult(
                success=False,
                source=self.config.source_name,
                error_message=f"Parse error: {e}"
            )

    def _parse_tallow_report(self, text: str) -> List[Dict]:
        """Parse the text-format tallow report"""
        records = []
        lines = text.split('\n')

        # Extract report date
        report_date = None
        for line in lines[:20]:
            if 'Report' in line and any(month in line for month in
                                        ['January', 'February', 'March', 'April', 'May', 'June',
                                         'July', 'August', 'September', 'October', 'November', 'December']):
                # Try to parse date
                date_match = re.search(
                    r'(January|February|March|April|May|June|July|August|September|October|November|December)\s+\d+,\s+\d{4}',
                    line
                )
                if date_match:
                    try:
                        report_date = datetime.strptime(date_match.group(), '%B %d, %Y').date()
                    except ValueError:
                        pass
                break

        # Product patterns and their normalized names
        product_patterns = {
            'choice white grease': 'choice_white_grease',
            'yellow grease': 'yellow_grease',
            'yellow grease ecb': 'yellow_grease_ecb',
            'packer bleachable': 'packer_bleachable_tallow',
            'renderer bleachable': 'renderer_bleachable_tallow',
            'edible tallow': 'edible_tallow',
            'fancy bleach': 'bleachable_fancy_tallow',
            'poultry fat': 'poultry_fat',
            'lard': 'lard',
        }

        # Location patterns
        locations = {
            'chicago': 'CAF_Chicago',
            'gulf': 'CAF_Gulf',
            'central': 'Central_US',
        }

        current_location = None

        for line in lines:
            line_lower = line.lower()

            # Detect location headers
            for loc_key, loc_name in locations.items():
                if loc_key in line_lower and ('caf' in line_lower or 'fob' in line_lower):
                    current_location = loc_name
                    break

            # Look for price data
            for product_key, product_name in product_patterns.items():
                if product_key in line_lower:
                    # Try to extract price range
                    prices = self._extract_prices(line)
                    if prices:
                        low, high, avg = prices
                        records.append({
                            'report_date': str(report_date) if report_date else None,
                            'product': product_name,
                            'location': current_location or 'Central_US',
                            'price_low': low,
                            'price_high': high,
                            'price_avg': avg,
                            'unit': 'cents/lb',
                            'source': 'USDA_AMS_TALLOW',
                        })
                    break

        return records

    def _extract_prices(self, line: str) -> Optional[Tuple[float, float, float]]:
        """Extract price range from a line"""
        # Look for patterns like "45.00-50.00" or "45.00 - 50.00"
        range_match = re.search(r'(\d+\.?\d*)\s*[-–]\s*(\d+\.?\d*)', line)

        if range_match:
            low = float(range_match.group(1))
            high = float(range_match.group(2))
            avg = (low + high) / 2
            return low, high, avg

        # Look for single price
        single_match = re.search(r'(\d+\.?\d*)\s*(?:cents|per)', line)
        if single_match:
            price = float(single_match.group(1))
            return price, price, price

        return None

    def parse_response(self, response_data: Any) -> Any:
        return response_data


# =============================================================================
# GRAIN CO-PRODUCTS (DDGS) COLLECTOR
# =============================================================================

@dataclass
class GrainCoProductsConfig(CollectorConfig):
    """USDA AMS Grain Co-Products configuration"""
    source_name: str = "USDA AMS DDGS"
    source_url: str = "https://www.ams.usda.gov/mnreports/ams_3618.pdf"
    auth_type: AuthType = AuthType.NONE
    frequency: DataFrequency = DataFrequency.WEEKLY

    # Alternative text report
    ethanol_report_url: str = "https://www.ams.usda.gov/mnreports/ams_3616.pdf"
    sd_ethanol_url: str = "https://www.ams.usda.gov/mnreports/sf_gr112.txt"


class GrainCoProductsCollector(BaseCollector):
    """
    Collector for USDA AMS Grain Co-Products data.

    Provides prices for:
    - Dried Distillers Grains (DDG)
    - Wet Distillers Grains (WDG)
    - Modified Wet Distillers Grains (MWDG)
    - Distillers Corn Oil (DCO)

    Updated weekly.
    """

    def __init__(self, config: GrainCoProductsConfig = None):
        config = config or GrainCoProductsConfig()
        super().__init__(config)
        self.config: GrainCoProductsConfig = config

    def get_table_name(self) -> str:
        return "usda_ams_ddgs"

    def fetch_data(
        self,
        start_date: date = None,
        end_date: date = None,
        **kwargs
    ) -> CollectorResult:
        """Fetch grain co-products data"""
        # Try the text-based South Dakota report first (more parseable)
        response, error = self._make_request(
            self.config.sd_ethanol_url,
            timeout=30
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
            text = response.text
            records = self._parse_coproducts_report(text)

            if not records:
                return CollectorResult(
                    success=False,
                    source=self.config.source_name,
                    error_message="No data parsed from report"
                )

            if PANDAS_AVAILABLE:
                df = pd.DataFrame(records)
            else:
                df = records

            return CollectorResult(
                success=True,
                source=self.config.source_name,
                records_fetched=len(records),
                data=df
            )

        except Exception as e:
            return CollectorResult(
                success=False,
                source=self.config.source_name,
                error_message=f"Parse error: {e}"
            )

    def _parse_coproducts_report(self, text: str) -> List[Dict]:
        """Parse grain co-products report"""
        records = []
        lines = text.split('\n')

        # Extract report date
        report_date = None
        for line in lines[:20]:
            date_match = re.search(
                r'(January|February|March|April|May|June|July|August|September|October|November|December)\s+\d+,\s+\d{4}',
                line
            )
            if date_match:
                try:
                    report_date = datetime.strptime(date_match.group(), '%B %d, %Y').date()
                except ValueError:
                    pass
                break

        # Product patterns
        products = {
            'dried distillers': 'DDG',
            'wet distillers': 'WDG',
            'modified wet': 'MWDG',
            'distillers corn oil': 'DCO',
            'corn oil': 'DCO',
        }

        for line in lines:
            line_lower = line.lower()

            for product_key, product_name in products.items():
                if product_key in line_lower:
                    # Extract prices
                    prices = re.findall(r'\$?(\d+\.?\d*)', line)

                    if prices:
                        # Usually format is: low, high, or just one price
                        try:
                            if len(prices) >= 2:
                                low = float(prices[0])
                                high = float(prices[1])
                                avg = (low + high) / 2
                            else:
                                avg = float(prices[0])
                                low = high = avg

                            # Determine unit
                            unit = '$/ton'
                            if 'lb' in line_lower or 'pound' in line_lower:
                                unit = '$/lb'
                            elif 'cwt' in line_lower:
                                unit = '$/cwt'

                            records.append({
                                'report_date': str(report_date) if report_date else None,
                                'product': product_name,
                                'price_low': low,
                                'price_high': high,
                                'price_avg': avg,
                                'unit': unit,
                                'source': 'USDA_AMS_COPRODUCTS',
                            })
                        except ValueError:
                            pass
                    break

        return records

    def parse_response(self, response_data: Any) -> Any:
        return response_data


# =============================================================================
# COMBINED USDA AMS COLLECTOR
# =============================================================================

class USDAMarketNewsCollector:
    """
    Combined collector for multiple USDA AMS reports.

    Provides unified interface to:
    - Tallow and Protein Report
    - Grain Co-Products Report
    """

    def __init__(self):
        self.tallow_collector = TallowProteinCollector()
        self.coproducts_collector = GrainCoProductsCollector()

    def collect_all(self) -> Dict[str, CollectorResult]:
        """Collect from all AMS reports"""
        return {
            'tallow': self.tallow_collector.collect(),
            'coproducts': self.coproducts_collector.collect(),
        }

    def get_biofuel_feedstock_prices(self) -> Optional[Dict]:
        """Get latest prices for biofuel feedstocks"""
        tallow_result = self.tallow_collector.collect()

        if not tallow_result.success:
            return None

        if PANDAS_AVAILABLE and hasattr(tallow_result.data, 'to_dict'):
            # Pivot to get latest prices by product
            df = tallow_result.data
            latest = df.groupby('product').last().reset_index()
            return latest.set_index('product')['price_avg'].to_dict()

        return None


# =============================================================================
# CLI INTERFACE
# =============================================================================

def main():
    """CLI for USDA AMS collectors"""
    import argparse
    import json

    parser = argparse.ArgumentParser(description='USDA AMS Market News Collectors')

    parser.add_argument(
        'report',
        choices=['tallow', 'ddgs', 'all'],
        help='Report to fetch'
    )

    parser.add_argument(
        '--output', '-o',
        help='Output file'
    )

    args = parser.parse_args()

    if args.report == 'tallow':
        collector = TallowProteinCollector()
        result = collector.collect()
    elif args.report == 'ddgs':
        collector = GrainCoProductsCollector()
        result = collector.collect()
    else:
        combined = USDAMarketNewsCollector()
        results = combined.collect_all()
        print(json.dumps({k: v.success for k, v in results.items()}, indent=2))
        return

    print(f"Success: {result.success}")
    print(f"Records: {result.records_fetched}")

    if result.error_message:
        print(f"Error: {result.error_message}")

    if result.data is not None and PANDAS_AVAILABLE:
        print(result.data.to_string())

    if args.output and result.data is not None:
        if args.output.endswith('.csv') and PANDAS_AVAILABLE:
            result.data.to_csv(args.output, index=False)
        print(f"Saved to: {args.output}")


if __name__ == '__main__':
    main()
