"""
CME Group Settlement Prices Collector

Collects daily settlement prices from CME Group:
- Grain futures (corn, wheat, soybeans, oats)
- Oilseed products (soybean oil, soybean meal)
- Livestock (live cattle, feeder cattle, lean hogs)
- Energy (crude oil, natural gas, RBOB, heating oil)
- Biofuel-related (ethanol)

Data sources:
- CME Daily Bulletin (PDF) - Free, delayed
- CME DataMine API - Paid subscription
- Settlement files - Free, available after close

Note: Real-time data requires paid subscription.
Daily settlements available for free with delay.
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


# CME contract specifications
CME_CONTRACTS = {
    # Grains - CBOT
    'ZC': {
        'name': 'Corn',
        'exchange': 'CBOT',
        'tick_size': 0.25,
        'contract_size': 5000,
        'unit': 'bushels',
        'quote_unit': 'cents/bushel',
        'months': ['H', 'K', 'N', 'U', 'Z'],  # Mar, May, Jul, Sep, Dec
    },
    'ZW': {
        'name': 'Chicago SRW Wheat',
        'exchange': 'CBOT',
        'tick_size': 0.25,
        'contract_size': 5000,
        'unit': 'bushels',
        'quote_unit': 'cents/bushel',
        'months': ['H', 'K', 'N', 'U', 'Z'],
    },
    'KE': {
        'name': 'KC HRW Wheat',
        'exchange': 'CBOT',
        'tick_size': 0.25,
        'contract_size': 5000,
        'unit': 'bushels',
        'quote_unit': 'cents/bushel',
        'months': ['H', 'K', 'N', 'U', 'Z'],
    },
    'MWE': {
        'name': 'Minneapolis HRS Wheat',
        'exchange': 'MGEX',
        'tick_size': 0.25,
        'contract_size': 5000,
        'unit': 'bushels',
        'quote_unit': 'cents/bushel',
        'months': ['H', 'K', 'N', 'U', 'Z'],
    },
    'ZS': {
        'name': 'Soybeans',
        'exchange': 'CBOT',
        'tick_size': 0.25,
        'contract_size': 5000,
        'unit': 'bushels',
        'quote_unit': 'cents/bushel',
        'months': ['F', 'H', 'K', 'N', 'Q', 'U', 'X'],  # Jan, Mar, May, Jul, Aug, Sep, Nov
    },
    'ZM': {
        'name': 'Soybean Meal',
        'exchange': 'CBOT',
        'tick_size': 0.10,
        'contract_size': 100,
        'unit': 'short tons',
        'quote_unit': '$/short ton',
        'months': ['F', 'H', 'K', 'N', 'Q', 'U', 'V', 'Z'],
    },
    'ZL': {
        'name': 'Soybean Oil',
        'exchange': 'CBOT',
        'tick_size': 0.01,
        'contract_size': 60000,
        'unit': 'pounds',
        'quote_unit': 'cents/pound',
        'months': ['F', 'H', 'K', 'N', 'Q', 'U', 'V', 'Z'],
    },
    'ZO': {
        'name': 'Oats',
        'exchange': 'CBOT',
        'tick_size': 0.25,
        'contract_size': 5000,
        'unit': 'bushels',
        'quote_unit': 'cents/bushel',
        'months': ['H', 'K', 'N', 'U', 'Z'],
    },
    # Energy - NYMEX
    'CL': {
        'name': 'WTI Crude Oil',
        'exchange': 'NYMEX',
        'tick_size': 0.01,
        'contract_size': 1000,
        'unit': 'barrels',
        'quote_unit': '$/barrel',
        'months': list('FGHJKMNQUVXZ'),  # All months
    },
    'RB': {
        'name': 'RBOB Gasoline',
        'exchange': 'NYMEX',
        'tick_size': 0.0001,
        'contract_size': 42000,
        'unit': 'gallons',
        'quote_unit': '$/gallon',
        'months': list('FGHJKMNQUVXZ'),
    },
    'HO': {
        'name': 'Heating Oil/ULSD',
        'exchange': 'NYMEX',
        'tick_size': 0.0001,
        'contract_size': 42000,
        'unit': 'gallons',
        'quote_unit': '$/gallon',
        'months': list('FGHJKMNQUVXZ'),
    },
    'NG': {
        'name': 'Natural Gas',
        'exchange': 'NYMEX',
        'tick_size': 0.001,
        'contract_size': 10000,
        'unit': 'MMBtu',
        'quote_unit': '$/MMBtu',
        'months': list('FGHJKMNQUVXZ'),
    },
    # Ethanol
    'EH': {
        'name': 'Chicago Ethanol (Platts)',
        'exchange': 'CBOT',
        'tick_size': 0.001,
        'contract_size': 29000,
        'unit': 'gallons',
        'quote_unit': '$/gallon',
        'months': list('FGHJKMNQUVXZ'),
    },
    # Livestock - CME
    'LE': {
        'name': 'Live Cattle',
        'exchange': 'CME',
        'tick_size': 0.025,
        'contract_size': 40000,
        'unit': 'pounds',
        'quote_unit': 'cents/pound',
        'months': ['G', 'J', 'M', 'Q', 'V', 'Z'],  # Feb, Apr, Jun, Aug, Oct, Dec
    },
    'GF': {
        'name': 'Feeder Cattle',
        'exchange': 'CME',
        'tick_size': 0.025,
        'contract_size': 50000,
        'unit': 'pounds',
        'quote_unit': 'cents/pound',
        'months': ['F', 'H', 'J', 'K', 'Q', 'U', 'V', 'X'],
    },
    'HE': {
        'name': 'Lean Hogs',
        'exchange': 'CME',
        'tick_size': 0.025,
        'contract_size': 40000,
        'unit': 'pounds',
        'quote_unit': 'cents/pound',
        'months': ['G', 'J', 'K', 'M', 'N', 'Q', 'V', 'Z'],
    },
    # Palm Oil
    'CPO': {
        'name': 'USD Malaysian Crude Palm Oil',
        'exchange': 'CME',
        'tick_size': 0.25,
        'contract_size': 25,
        'unit': 'metric tons',
        'quote_unit': '$/metric ton',
        'months': list('FGHJKMNQUVXZ'),
    },
}

# Month codes
MONTH_CODES = {
    'F': 1, 'G': 2, 'H': 3, 'J': 4, 'K': 5, 'M': 6,
    'N': 7, 'Q': 8, 'U': 9, 'V': 10, 'X': 11, 'Z': 12
}


@dataclass
class CMESettlementsConfig(CollectorConfig):
    """CME Settlements configuration"""
    source_name: str = "CME Group"
    source_url: str = "https://www.cmegroup.com"
    auth_type: AuthType = AuthType.NONE  # Free for delayed data
    frequency: DataFrequency = DataFrequency.DAILY

    # Contracts to track
    contracts: List[str] = field(default_factory=lambda: [
        'ZC', 'ZW', 'KE', 'ZS', 'ZM', 'ZL', 'CL', 'RB', 'HO', 'NG', 'EH'
    ])

    # Number of forward months to track
    forward_months: int = 6

    # Request settings
    timeout: int = 30
    retry_attempts: int = 3


class CMESettlementsCollector(BaseCollector):
    """
    Collector for CME Group settlement prices.

    Provides daily settlement prices for:
    - Agricultural futures (grains, oilseeds, livestock)
    - Energy futures (crude, products, natural gas)
    - Ethanol futures

    Note: This collector uses publicly available data.
    For real-time data, CME DataMine subscription required.
    """

    def __init__(self, config: CMESettlementsConfig = None):
        config = config or CMESettlementsConfig()
        super().__init__(config)
        self.config: CMESettlementsConfig = config

        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
            'Accept': 'application/json, text/html, */*',
        })

    def get_table_name(self) -> str:
        return "cme_settlements"

    def fetch_data(
        self,
        start_date: date = None,
        end_date: date = None,
        contracts: List[str] = None,
        **kwargs
    ) -> CollectorResult:
        """
        Fetch settlement prices from CME.

        Args:
            contracts: List of contract codes (e.g., ['ZC', 'ZS'])
            start_date: Start date
            end_date: End date

        Returns:
            CollectorResult with settlement prices
        """
        contracts = contracts or self.config.contracts
        end_date = end_date or date.today()
        start_date = start_date or end_date - timedelta(days=30)

        all_records = []
        warnings = []

        for contract in contracts:
            if contract not in CME_CONTRACTS:
                warnings.append(f"Unknown contract: {contract}")
                continue

            contract_info = CME_CONTRACTS[contract]
            self.logger.info(f"Fetching {contract_info['name']} settlements")

            try:
                records = self._fetch_contract_settlements(
                    contract, contract_info, start_date, end_date
                )
                all_records.extend(records)
                self.logger.info(f"Retrieved {len(records)} records for {contract}")

            except Exception as e:
                warnings.append(f"{contract}: {e}")
                self.logger.error(f"Error fetching {contract}: {e}", exc_info=True)

        if not all_records:
            # Return helpful message about data availability
            return CollectorResult(
                success=False,
                source=self.config.source_name,
                error_message=(
                    "No settlement data retrieved. "
                    "CME free data may have access limitations. "
                    "Consider using Quandl, Barchart, or CME DataMine for reliable futures data."
                ),
                warnings=warnings
            )

        # Convert to DataFrame
        if PANDAS_AVAILABLE:
            df = pd.DataFrame(all_records)
            df = df.sort_values(['contract', 'trade_date', 'contract_month'])
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

    def _fetch_contract_settlements(
        self,
        contract: str,
        contract_info: Dict,
        start_date: date,
        end_date: date
    ) -> List[Dict]:
        """Fetch settlements for a specific contract"""
        records = []

        # CME provides settlement data via their website
        # The exact endpoint depends on the product
        exchange = contract_info['exchange']

        # Try CME Globex endpoint for quotes
        # Note: This may require JavaScript rendering in production
        quotes_url = f"https://www.cmegroup.com/markets/{self._get_product_path(contract)}.quotes.html"

        response, error = self._make_request(quotes_url)

        if error:
            self.logger.warning(f"Request failed for {contract}: {error}")
            # Try alternative: Settlements page
            return self._fetch_from_settlements_page(contract, contract_info)

        if response.status_code != 200:
            return self._fetch_from_settlements_page(contract, contract_info)

        # Parse HTML for settlement data
        try:
            soup = BeautifulSoup(response.text, 'html.parser')

            # Look for settlement table or JSON data
            # CME uses JavaScript rendering, so we look for embedded data
            scripts = soup.find_all('script')

            for script in scripts:
                if script.string and 'quotesList' in str(script.string):
                    # Found embedded quote data
                    records = self._parse_embedded_quotes(
                        script.string, contract, contract_info
                    )
                    if records:
                        break

            # Also try parsing visible tables
            if not records:
                tables = soup.find_all('table')
                for table in tables:
                    if 'settle' in table.get_text().lower():
                        table_records = self._parse_settlement_table(
                            table, contract, contract_info
                        )
                        records.extend(table_records)

        except Exception as e:
            self.logger.warning(f"Error parsing response: {e}")

        return records

    def _fetch_from_settlements_page(
        self,
        contract: str,
        contract_info: Dict
    ) -> List[Dict]:
        """Fetch from CME settlements/final page"""
        records = []

        # CME daily bulletin contains all settlements
        # Available as PDF: https://www.cmegroup.com/market-data/daily-bulletin.html

        self.logger.info(f"Settlement data for {contract} may require manual download from CME Daily Bulletin")

        # Return placeholder with contract info
        today = date.today()
        for i, month_code in enumerate(contract_info['months'][:self.config.forward_months]):
            month_num = MONTH_CODES[month_code]
            year = today.year if month_num >= today.month else today.year + 1

            records.append({
                'contract': contract,
                'contract_name': contract_info['name'],
                'exchange': contract_info['exchange'],
                'contract_month': f"{month_code}{year % 100:02d}",
                'contract_month_date': date(year, month_num, 1).isoformat(),
                'trade_date': today.isoformat(),
                'settlement': None,  # Would need actual data
                'change': None,
                'volume': None,
                'open_interest': None,
                'unit': contract_info['quote_unit'],
                'source': 'CME_PLACEHOLDER',
                'note': 'Actual data requires CME DataMine or alternative source',
            })

        return records

    def _get_product_path(self, contract: str) -> str:
        """Get CME website product path"""
        paths = {
            'ZC': 'agriculture/grains/corn',
            'ZW': 'agriculture/grains/wheat',
            'KE': 'agriculture/grains/kc-wheat',
            'ZS': 'agriculture/oilseeds/soybean',
            'ZM': 'agriculture/oilseeds/soybean-meal',
            'ZL': 'agriculture/oilseeds/soybean-oil',
            'ZO': 'agriculture/grains/oats',
            'CL': 'energy/crude-oil/light-sweet-crude',
            'RB': 'energy/refined-products/rbob-gasoline',
            'HO': 'energy/refined-products/heating-oil',
            'NG': 'energy/natural-gas/natural-gas',
            'EH': 'agriculture/ethanol/chicago-ethanol',
            'LE': 'agriculture/livestock/live-cattle',
            'GF': 'agriculture/livestock/feeder-cattle',
            'HE': 'agriculture/livestock/lean-hogs',
        }
        return paths.get(contract, f'agriculture/{contract.lower()}')

    def _parse_embedded_quotes(
        self,
        script_content: str,
        contract: str,
        contract_info: Dict
    ) -> List[Dict]:
        """Parse embedded JSON quote data from CME page"""
        records = []

        import json

        # Look for JSON data pattern
        match = re.search(r'quotesList\s*:\s*(\[.*?\])', script_content, re.DOTALL)
        if match:
            try:
                data = json.loads(match.group(1))
                for quote in data:
                    records.append({
                        'contract': contract,
                        'contract_name': contract_info['name'],
                        'exchange': contract_info['exchange'],
                        'contract_month': quote.get('expirationMonth', ''),
                        'trade_date': date.today().isoformat(),
                        'settlement': self._parse_price(quote.get('settle')),
                        'change': self._parse_price(quote.get('change')),
                        'volume': self._parse_int(quote.get('volume')),
                        'open_interest': self._parse_int(quote.get('openInterest')),
                        'high': self._parse_price(quote.get('high')),
                        'low': self._parse_price(quote.get('low')),
                        'unit': contract_info['quote_unit'],
                        'source': 'CME',
                    })
            except json.JSONDecodeError:
                pass

        return records

    def _parse_settlement_table(
        self,
        table,
        contract: str,
        contract_info: Dict
    ) -> List[Dict]:
        """Parse HTML settlement table"""
        records = []

        try:
            rows = table.find_all('tr')
            if len(rows) < 2:
                return records

            # Get headers
            headers = [th.get_text(strip=True).lower() for th in rows[0].find_all(['th', 'td'])]

            for row in rows[1:]:
                cells = row.find_all(['td', 'th'])
                values = [cell.get_text(strip=True) for cell in cells]

                if len(values) < 2:
                    continue

                # Map to record
                record = {
                    'contract': contract,
                    'contract_name': contract_info['name'],
                    'exchange': contract_info['exchange'],
                    'trade_date': date.today().isoformat(),
                    'unit': contract_info['quote_unit'],
                    'source': 'CME',
                }

                for i, (header, value) in enumerate(zip(headers, values)):
                    if 'month' in header:
                        record['contract_month'] = value
                    elif 'settle' in header:
                        record['settlement'] = self._parse_price(value)
                    elif 'change' in header:
                        record['change'] = self._parse_price(value)
                    elif 'volume' in header:
                        record['volume'] = self._parse_int(value)
                    elif 'interest' in header:
                        record['open_interest'] = self._parse_int(value)
                    elif 'high' in header:
                        record['high'] = self._parse_price(value)
                    elif 'low' in header:
                        record['low'] = self._parse_price(value)

                if record.get('settlement') is not None:
                    records.append(record)

        except Exception as e:
            self.logger.warning(f"Error parsing table: {e}")

        return records

    def _parse_price(self, value: Any) -> Optional[float]:
        """Parse price value"""
        if value is None:
            return None

        try:
            cleaned = re.sub(r'[^\d.\-+]', '', str(value))
            if cleaned:
                return float(cleaned)
        except (ValueError, TypeError):
            pass

        return None

    def _parse_int(self, value: Any) -> Optional[int]:
        """Parse integer value"""
        if value is None:
            return None

        try:
            cleaned = re.sub(r'[^\d]', '', str(value))
            if cleaned:
                return int(cleaned)
        except (ValueError, TypeError):
            pass

        return None

    def parse_response(self, response_data: Any) -> Any:
        return response_data

    # =========================================================================
    # CONVENIENCE METHODS
    # =========================================================================

    def get_grain_settlements(self) -> Optional[Any]:
        """Get settlements for grain contracts"""
        grains = ['ZC', 'ZW', 'KE', 'ZS', 'ZM', 'ZL', 'ZO']
        result = self.collect(contracts=grains)
        return result.data if result.success else None

    def get_energy_settlements(self) -> Optional[Any]:
        """Get settlements for energy contracts"""
        energy = ['CL', 'RB', 'HO', 'NG']
        result = self.collect(contracts=energy)
        return result.data if result.success else None

    def get_ethanol_settlements(self) -> Optional[Any]:
        """Get settlements for ethanol"""
        result = self.collect(contracts=['EH'])
        return result.data if result.success else None

    def get_livestock_settlements(self) -> Optional[Any]:
        """Get settlements for livestock contracts"""
        livestock = ['LE', 'GF', 'HE']
        result = self.collect(contracts=livestock)
        return result.data if result.success else None

    def get_front_month(self, contract: str) -> Optional[Dict]:
        """
        Get front month settlement for a contract.

        Args:
            contract: Contract code (e.g., 'ZC')

        Returns:
            Dict with front month data
        """
        if contract not in CME_CONTRACTS:
            return None

        result = self.collect(contracts=[contract])

        if not result.success or result.data is None:
            return None

        if PANDAS_AVAILABLE and hasattr(result.data, 'iloc'):
            if not result.data.empty:
                return result.data.iloc[0].to_dict()

        return None

    def calculate_spread(
        self,
        contract: str,
        front_month: str,
        back_month: str
    ) -> Optional[float]:
        """
        Calculate calendar spread between two months.

        Args:
            contract: Contract code
            front_month: Front month code (e.g., 'H24')
            back_month: Back month code (e.g., 'K24')

        Returns:
            Spread value
        """
        result = self.collect(contracts=[contract])

        if not result.success or result.data is None:
            return None

        if PANDAS_AVAILABLE and hasattr(result.data, 'query'):
            df = result.data
            front = df[df['contract_month'] == front_month]
            back = df[df['contract_month'] == back_month]

            if not front.empty and not back.empty:
                front_settle = front.iloc[0]['settlement']
                back_settle = back.iloc[0]['settlement']

                if front_settle is not None and back_settle is not None:
                    return front_settle - back_settle

        return None


# =============================================================================
# CLI INTERFACE
# =============================================================================

def main():
    """CLI for CME settlements collector"""
    import argparse
    import json

    parser = argparse.ArgumentParser(description='CME Settlement Prices Collector')

    parser.add_argument(
        'command',
        choices=['fetch', 'grains', 'energy', 'ethanol', 'test'],
        help='Command to execute'
    )

    parser.add_argument(
        '--contracts',
        nargs='+',
        help='Contract codes to fetch'
    )

    parser.add_argument(
        '--output', '-o',
        help='Output file'
    )

    args = parser.parse_args()

    collector = CMESettlementsCollector()

    if args.command == 'test':
        success, message = collector.test_connection()
        print(f"Connection test: {'PASS' if success else 'FAIL'} - {message}")
        return

    if args.command == 'grains':
        data = collector.get_grain_settlements()
        if data is not None:
            if PANDAS_AVAILABLE and hasattr(data, 'to_string'):
                print(data.to_string())
            else:
                print(json.dumps(data, indent=2, default=str))
        return

    if args.command == 'energy':
        data = collector.get_energy_settlements()
        if data is not None:
            if PANDAS_AVAILABLE and hasattr(data, 'to_string'):
                print(data.to_string())
            else:
                print(json.dumps(data, indent=2, default=str))
        return

    if args.command == 'ethanol':
        data = collector.get_ethanol_settlements()
        if data is not None:
            if PANDAS_AVAILABLE and hasattr(data, 'to_string'):
                print(data.to_string())
            else:
                print(json.dumps(data, indent=2, default=str))
        return

    if args.command == 'fetch':
        result = collector.collect(contracts=args.contracts)

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
                with open(args.output, 'w') as f:
                    if PANDAS_AVAILABLE and hasattr(result.data, 'to_dict'):
                        json.dump(result.data.to_dict('records'), f, indent=2, default=str)
                    else:
                        json.dump(result.data, f, indent=2, default=str)
            print(f"Saved to: {args.output}")


if __name__ == '__main__':
    main()
