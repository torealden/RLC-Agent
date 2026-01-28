"""
CFTC Commitments of Traders (COT) Collector

Collects weekly positioning data from the CFTC, including:
- Managed Money (MM) positions
- Commercial (Producer/Merchant/Processor/User) positions
- Swap Dealer positions
- Open Interest

Data released every Friday at 3:30 PM ET for positions as of Tuesday.

Sources:
- CFTC Public Reporting API (Socrata)
- cot_reports Python package (optional)
"""

import logging
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


# CFTC Contract Codes for Agricultural Commodities
CFTC_AG_CONTRACTS = {
    'corn': {
        'code': '002602',
        'exchange': 'CBOT',
        'name': 'CORN - CHICAGO BOARD OF TRADE',
        'multiplier': 5000,  # bushels per contract
    },
    'soybeans': {
        'code': '005602',
        'exchange': 'CBOT',
        'name': 'SOYBEANS - CHICAGO BOARD OF TRADE',
        'multiplier': 5000,
    },
    'wheat_srw': {
        'code': '001602',
        'exchange': 'CBOT',
        'name': 'WHEAT-SRW - CHICAGO BOARD OF TRADE',
        'multiplier': 5000,
    },
    'wheat_hrw': {
        'code': '001612',
        'exchange': 'KCBT',
        'name': 'WHEAT-HRW - KANSAS CITY BOARD OF TRADE',
        'multiplier': 5000,
    },
    'soybean_oil': {
        'code': '007601',
        'exchange': 'CBOT',
        'name': 'SOYBEAN OIL - CHICAGO BOARD OF TRADE',
        'multiplier': 60000,  # pounds per contract
    },
    'soybean_meal': {
        'code': '026603',
        'exchange': 'CBOT',
        'name': 'SOYBEAN MEAL - CHICAGO BOARD OF TRADE',
        'multiplier': 100,  # short tons per contract
    },
    'cotton': {
        'code': '033661',
        'exchange': 'ICE',
        'name': 'COTTON NO. 2 - ICE FUTURES U.S.',
        'multiplier': 50000,  # pounds per contract
    },
    'sugar': {
        'code': '080732',
        'exchange': 'ICE',
        'name': 'SUGAR NO. 11 - ICE FUTURES U.S.',
        'multiplier': 112000,  # pounds per contract
    },
    'coffee': {
        'code': '083731',
        'exchange': 'ICE',
        'name': 'COFFEE C - ICE FUTURES U.S.',
        'multiplier': 37500,  # pounds per contract
    },
    'cocoa': {
        'code': '073732',
        'exchange': 'ICE',
        'name': 'COCOA - ICE FUTURES U.S.',
        'multiplier': 10,  # metric tons per contract
    },
}


@dataclass
class CFTCCOTConfig(CollectorConfig):
    """CFTC COT specific configuration"""
    source_name: str = "CFTC COT"
    source_url: str = "https://publicreporting.cftc.gov/resource/6dca-aqww.json"
    auth_type: AuthType = AuthType.NONE
    frequency: DataFrequency = DataFrequency.WEEKLY

    # CFTC-specific settings
    report_type: str = "legacy"  # legacy is most reliable; disaggregated or tff also available
    contracts: List[str] = field(default_factory=lambda: [
        'corn', 'soybeans', 'wheat_srw', 'wheat_hrw',
        'soybean_oil', 'soybean_meal'
    ])

    # API endpoints for different report types (Socrata resource IDs)
    # Find datasets at: https://publicreporting.cftc.gov/browse
    endpoints: Dict[str, str] = field(default_factory=lambda: {
        'legacy': 'https://publicreporting.cftc.gov/resource/6dca-aqww.json',
        'disaggregated': 'https://publicreporting.cftc.gov/resource/72hh-3qpy.json',
        'tff': 'https://publicreporting.cftc.gov/resource/gpe5-46if.json',
    })


class CFTCCOTCollector(BaseCollector):
    """
    Collector for CFTC Commitments of Traders data.

    Uses the CFTC Socrata API for public data access.

    Features:
    - Disaggregated report (MM, Swap, Producer, Other)
    - Legacy report (Commercials, Non-Commercials, Nonreportable)
    - Historical data back to 2006
    - Net positions calculation
    - Week-over-week change calculation
    """

    def __init__(self, config: CFTCCOTConfig = None):
        config = config or CFTCCOTConfig()
        super().__init__(config)
        self.config: CFTCCOTConfig = config

    def get_table_name(self) -> str:
        return "cftc_cot"

    def _build_query(
        self,
        contract_code: str,
        start_date: date = None,
        end_date: date = None,
        limit: int = 100
    ) -> Dict[str, str]:
        """Build Socrata API query parameters"""
        params = {
            '$limit': str(limit),
            '$order': 'report_date_as_yyyy_mm_dd DESC',
        }

        # Filter by contract code
        where_clauses = [f"cftc_contract_market_code='{contract_code}'"]

        if start_date:
            where_clauses.append(
                f"report_date_as_yyyy_mm_dd>='{start_date.isoformat()}'"
            )

        if end_date:
            where_clauses.append(
                f"report_date_as_yyyy_mm_dd<='{end_date.isoformat()}'"
            )

        if where_clauses:
            params['$where'] = ' AND '.join(where_clauses)

        return params

    def fetch_data(
        self,
        start_date: date = None,
        end_date: date = None,
        contracts: List[str] = None,
        limit_per_contract: int = 52,
        **kwargs
    ) -> CollectorResult:
        """
        Fetch COT data from CFTC API.

        Args:
            start_date: Start date for historical data
            end_date: End date (default: latest available)
            contracts: List of contracts to fetch (default: all configured)
            limit_per_contract: Max records per contract

        Returns:
            CollectorResult with positioning data
        """
        contracts = contracts or self.config.contracts
        end_date = end_date or date.today()

        # Default to 1 year of history
        if start_date is None:
            start_date = end_date - timedelta(days=365)

        api_url = self.config.endpoints.get(
            self.config.report_type,
            self.config.source_url
        )

        all_records = []
        warnings = []

        for contract in contracts:
            if contract not in CFTC_AG_CONTRACTS:
                warnings.append(f"Unknown contract: {contract}")
                continue

            contract_info = CFTC_AG_CONTRACTS[contract]

            params = self._build_query(
                contract_info['code'],
                start_date,
                end_date,
                limit_per_contract
            )

            response, error = self._make_request(api_url, params=params)

            if error:
                warnings.append(f"{contract}: {error}")
                continue

            if response.status_code != 200:
                warnings.append(f"{contract}: HTTP {response.status_code}")
                continue

            try:
                data = response.json()

                for record in data:
                    parsed = self._parse_record(record, contract, contract_info)
                    if parsed:
                        all_records.append(parsed)

            except Exception as e:
                warnings.append(f"{contract}: Parse error - {e}")

        if not all_records:
            return CollectorResult(
                success=False,
                source=self.config.source_name,
                error_message="No data retrieved",
                warnings=warnings
            )

        # Convert to DataFrame if pandas available
        if PANDAS_AVAILABLE:
            df = pd.DataFrame(all_records)
            df['report_date'] = pd.to_datetime(df['report_date'])
            df = df.sort_values(['commodity', 'report_date'])
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

    def _parse_record(
        self,
        record: Dict,
        commodity: str,
        contract_info: Dict
    ) -> Optional[Dict]:
        """Parse a single COT record into standardized format"""
        try:
            # Extract date
            report_date = record.get('report_date_as_yyyy_mm_dd', '')
            as_of_date = record.get('as_of_date_in_form_yymmdd', '')

            # Open Interest
            open_interest = self._safe_int(record.get('open_interest_all'))

            # Handle LEGACY report format (noncomm/comm)
            if self.config.report_type == 'legacy':
                # Non-Commercial (Speculators)
                noncomm_long = self._safe_int(record.get('noncomm_positions_long_all'))
                noncomm_short = self._safe_int(record.get('noncomm_positions_short_all'))
                noncomm_spread = self._safe_int(record.get('noncomm_positions_spread_all'))

                # Commercial (Hedgers)
                comm_long = self._safe_int(record.get('comm_positions_long_all'))
                comm_short = self._safe_int(record.get('comm_positions_short_all'))

                # Non-reportables
                nonrept_long = self._safe_int(record.get('nonrept_positions_long_all'))
                nonrept_short = self._safe_int(record.get('nonrept_positions_short_all'))

                # Changes
                noncomm_long_chg = self._safe_int(record.get('change_in_noncomm_long_all'))
                noncomm_short_chg = self._safe_int(record.get('change_in_noncomm_short_all'))

                # Calculate nets
                noncomm_net = (noncomm_long or 0) - (noncomm_short or 0)
                comm_net = (comm_long or 0) - (comm_short or 0)
                noncomm_net_change = (noncomm_long_chg or 0) - (noncomm_short_chg or 0)

                return {
                    'report_date': report_date,
                    'as_of_date': as_of_date,
                    'commodity': commodity,
                    'exchange': contract_info['exchange'],
                    'contract_code': contract_info['code'],

                    # Non-Commercial (map to mm_ fields for consistency)
                    'mm_long': noncomm_long,
                    'mm_short': noncomm_short,
                    'mm_spread': noncomm_spread,
                    'mm_net': noncomm_net,
                    'mm_net_change': noncomm_net_change,

                    # Commercial (map to prod_ fields)
                    'prod_long': comm_long,
                    'prod_short': comm_short,
                    'prod_net': comm_net,

                    # Legacy doesn't have swap breakdown
                    'swap_long': None,
                    'swap_short': None,
                    'swap_spread': None,
                    'swap_net': None,

                    # Other (not in legacy)
                    'other_long': None,
                    'other_short': None,

                    # Non-reportables
                    'nonrept_long': nonrept_long,
                    'nonrept_short': nonrept_short,

                    # Open Interest
                    'open_interest': open_interest,

                    # Metadata
                    'source': 'CFTC',
                    'report_type': self.config.report_type,
                }

            # Handle DISAGGREGATED report format
            else:
                # Managed Money positions
                mm_long = self._safe_int(record.get('m_money_positions_long_all'))
                mm_short = self._safe_int(record.get('m_money_positions_short_all'))
                mm_spread = self._safe_int(record.get('m_money_positions_spread_all'))

                # Commercial/Producer positions
                prod_long = self._safe_int(record.get('prod_merc_positions_long_all'))
                prod_short = self._safe_int(record.get('prod_merc_positions_short_all'))

                # Swap Dealer positions
                swap_long = self._safe_int(record.get('swap_positions_long_all'))
                swap_short = self._safe_int(record.get('swap_positions_short_all'))
                swap_spread = self._safe_int(record.get('swap__positions_spread_all'))

                # Other Reportables
                other_long = self._safe_int(record.get('other_rept_positions_long_all'))
                other_short = self._safe_int(record.get('other_rept_positions_short_all'))

                # Non-reportables
                nonrept_long = self._safe_int(record.get('nonrept_positions_long_all'))
                nonrept_short = self._safe_int(record.get('nonrept_positions_short_all'))

                # Calculate net positions
                mm_net = (mm_long or 0) - (mm_short or 0)
                prod_net = (prod_long or 0) - (prod_short or 0)
                swap_net = (swap_long or 0) - (swap_short or 0)

                # Week-over-week changes
                mm_long_change = self._safe_int(record.get('change_in_m_money_long_all'))
                mm_short_change = self._safe_int(record.get('change_in_m_money_short_all'))
                mm_net_change = (mm_long_change or 0) - (mm_short_change or 0)

                return {
                    'report_date': report_date,
                    'as_of_date': as_of_date,
                    'commodity': commodity,
                    'exchange': contract_info['exchange'],
                    'contract_code': contract_info['code'],

                    # Managed Money
                    'mm_long': mm_long,
                    'mm_short': mm_short,
                    'mm_spread': mm_spread,
                    'mm_net': mm_net,
                    'mm_net_change': mm_net_change,

                    # Producer/Merchant/Processor
                    'prod_long': prod_long,
                    'prod_short': prod_short,
                    'prod_net': prod_net,

                    # Swap Dealers
                    'swap_long': swap_long,
                    'swap_short': swap_short,
                    'swap_spread': swap_spread,
                    'swap_net': swap_net,

                    # Other Reportables
                    'other_long': other_long,
                    'other_short': other_short,

                    # Non-reportables
                    'nonrept_long': nonrept_long,
                    'nonrept_short': nonrept_short,

                    # Open Interest
                    'open_interest': open_interest,

                    # Metadata
                    'source': 'CFTC',
                    'report_type': self.config.report_type,
                }

        except Exception as e:
            self.logger.warning(f"Error parsing record: {e}")
            return None

    def _safe_int(self, value: Any) -> Optional[int]:
        """Safely convert value to int"""
        if value is None or value == '':
            return None
        try:
            return int(float(value))
        except (ValueError, TypeError):
            return None

    def parse_response(self, response_data: Any) -> Any:
        """Parse API response - implemented in _parse_record"""
        return response_data

    def get_latest_report_date(self) -> Optional[date]:
        """Get the date of the most recent COT report"""
        result = self.fetch_data(
            start_date=date.today() - timedelta(days=14),
            end_date=date.today(),
            contracts=['corn'],
            limit_per_contract=1
        )

        if result.success and result.data is not None:
            if PANDAS_AVAILABLE and hasattr(result.data, 'iloc'):
                return result.data['report_date'].max().date()
            elif isinstance(result.data, list) and result.data:
                return self.parse_date(result.data[0].get('report_date'))

        return None

    def get_net_positions_summary(
        self,
        as_of_date: date = None
    ) -> Dict[str, Dict]:
        """
        Get summary of net positions for all commodities.

        Args:
            as_of_date: Report date (default: latest)

        Returns:
            Dict with commodity -> position summary
        """
        if as_of_date is None:
            as_of_date = self.get_latest_report_date() or date.today()

        result = self.fetch_data(
            start_date=as_of_date - timedelta(days=7),
            end_date=as_of_date,
            limit_per_contract=2
        )

        if not result.success:
            return {}

        summary = {}

        if PANDAS_AVAILABLE and hasattr(result.data, 'groupby'):
            for commodity, group in result.data.groupby('commodity'):
                latest = group.sort_values('report_date').iloc[-1]

                summary[commodity] = {
                    'report_date': str(latest['report_date']),
                    'mm_net': int(latest['mm_net']) if latest['mm_net'] else 0,
                    'mm_net_change': int(latest['mm_net_change']) if latest['mm_net_change'] else 0,
                    'prod_net': int(latest['prod_net']) if latest['prod_net'] else 0,
                    'swap_net': int(latest['swap_net']) if latest['swap_net'] else 0,
                    'open_interest': int(latest['open_interest']) if latest['open_interest'] else 0,
                }
        else:
            # Handle non-pandas case
            for record in result.data:
                commodity = record['commodity']
                if commodity not in summary:
                    summary[commodity] = record

        return summary

    # =========================================================================
    # DATABASE METHODS
    # =========================================================================

    def save_to_bronze(
        self,
        contracts: List[str] = None,
        weeks: int = 52,
        conn=None
    ) -> Dict[str, int]:
        """
        Collect and save COT data to bronze layer.

        Args:
            contracts: List of contracts to fetch
            weeks: Number of weeks of history
            conn: Optional database connection

        Returns:
            Dict with counts of records saved
        """
        import os
        import psycopg2
        from pathlib import Path
        from dotenv import load_dotenv

        # Load .env from project root
        project_root = Path(__file__).parent.parent.parent.parent.parent
        load_dotenv(project_root / '.env')

        close_conn = False
        if conn is None:
            password = (os.environ.get('RLC_PG_PASSWORD') or
                       os.environ.get('DATABASE_PASSWORD') or
                       os.environ.get('DB_PASSWORD'))
            conn = psycopg2.connect(
                host=os.environ.get('DATABASE_HOST', 'localhost'),
                port=os.environ.get('DATABASE_PORT', '5432'),
                database=os.environ.get('DATABASE_NAME', 'rlc_commodities'),
                user=os.environ.get('DATABASE_USER', 'postgres'),
                password=password
            )
            close_conn = True

        cursor = conn.cursor()
        counts = {'inserted': 0, 'updated': 0, 'errors': 0}

        # Fetch data
        end_date = date.today()
        start_date = end_date - timedelta(weeks=weeks)

        result = self.collect(
            start_date=start_date,
            end_date=end_date,
            contracts=contracts
        )

        if not result.success or result.data is None:
            self.logger.error(f"Failed to fetch COT data: {result.error_message}")
            return counts

        # Convert to list of dicts if DataFrame
        if hasattr(result.data, 'to_dict'):
            records = result.data.to_dict('records')
        else:
            records = result.data

        for record in records:
            try:
                # Convert report_date to proper format
                report_date = record.get('report_date')
                if hasattr(report_date, 'strftime'):
                    report_date = report_date.strftime('%Y-%m-%d')

                cursor.execute("""
                    INSERT INTO bronze.cftc_cot
                    (report_date, as_of_date, commodity, exchange, contract_code,
                     mm_long, mm_short, mm_spread, mm_net, mm_net_change,
                     prod_long, prod_short, prod_net,
                     swap_long, swap_short, swap_spread, swap_net,
                     other_long, other_short,
                     nonrept_long, nonrept_short,
                     open_interest, report_type, source, collected_at)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())
                    ON CONFLICT (report_date, commodity, report_type)
                    DO UPDATE SET
                        mm_net = EXCLUDED.mm_net,
                        mm_net_change = EXCLUDED.mm_net_change,
                        prod_net = EXCLUDED.prod_net,
                        open_interest = EXCLUDED.open_interest,
                        collected_at = NOW()
                """, (
                    report_date,
                    record.get('as_of_date'),
                    record.get('commodity'),
                    record.get('exchange'),
                    record.get('contract_code'),
                    record.get('mm_long'),
                    record.get('mm_short'),
                    record.get('mm_spread'),
                    record.get('mm_net'),
                    record.get('mm_net_change'),
                    record.get('prod_long'),
                    record.get('prod_short'),
                    record.get('prod_net'),
                    record.get('swap_long'),
                    record.get('swap_short'),
                    record.get('swap_spread'),
                    record.get('swap_net'),
                    record.get('other_long'),
                    record.get('other_short'),
                    record.get('nonrept_long'),
                    record.get('nonrept_short'),
                    record.get('open_interest'),
                    record.get('report_type'),
                    record.get('source', 'CFTC')
                ))

                if cursor.rowcount > 0:
                    counts['inserted'] += 1

            except Exception as e:
                self.logger.error(f"Error saving record: {e}")
                counts['errors'] += 1

        conn.commit()

        if close_conn:
            cursor.close()
            conn.close()

        self.logger.info(f"Saved {counts['inserted']} records to bronze.cftc_cot")
        return counts


# =============================================================================
# ALTERNATIVE: Using cot_reports package
# =============================================================================

def fetch_with_cot_reports(
    contracts: List[str] = None,
    report_type: str = 'legacy_fut'
) -> Optional[Any]:
    """
    Alternative fetch using the cot_reports package.

    Install with: pip install cot_reports

    Args:
        contracts: List of contract names
        report_type: 'legacy_fut', 'disagg_fut', or 'traders_in_financial_futures_fut'

    Returns:
        DataFrame with COT data
    """
    try:
        import cot_reports as cot

        df = cot.cot_year(
            year=datetime.now().year,
            report_type=report_type
        )

        # Filter for agricultural contracts
        if contracts:
            ag_codes = [
                CFTC_AG_CONTRACTS[c]['code']
                for c in contracts
                if c in CFTC_AG_CONTRACTS
            ]
            df = df[df['CFTC Contract Market Code'].isin(ag_codes)]

        return df

    except ImportError:
        logger.error("cot_reports package not installed. Run: pip install cot_reports")
        return None
    except Exception as e:
        logger.error(f"Error fetching with cot_reports: {e}")
        return None


# =============================================================================
# CLI INTERFACE
# =============================================================================

def main():
    """Command-line interface for CFTC COT collector"""
    import argparse
    import json

    parser = argparse.ArgumentParser(description='CFTC COT Data Collector')

    parser.add_argument(
        'command',
        choices=['fetch', 'summary', 'test'],
        help='Command to execute'
    )

    parser.add_argument(
        '--contracts',
        nargs='+',
        default=['corn', 'soybeans', 'wheat_srw'],
        help='Contracts to fetch'
    )

    parser.add_argument(
        '--weeks',
        type=int,
        default=4,
        help='Number of weeks to fetch'
    )

    parser.add_argument(
        '--output',
        '-o',
        help='Output file (JSON)'
    )

    parser.add_argument(
        '--save-db',
        action='store_true',
        help='Save data to PostgreSQL bronze layer'
    )

    args = parser.parse_args()

    # Create collector
    config = CFTCCOTConfig(contracts=args.contracts)
    collector = CFTCCOTCollector(config)

    if args.command == 'test':
        success, message = collector.test_connection()
        print(f"Connection test: {'PASS' if success else 'FAIL'} - {message}")
        return

    if args.command == 'summary':
        summary = collector.get_net_positions_summary()
        print(json.dumps(summary, indent=2, default=str))
        return

    if args.command == 'fetch':
        if args.save_db:
            counts = collector.save_to_bronze(
                contracts=args.contracts,
                weeks=args.weeks
            )
            print(f"Saved to database: {counts}")
            return

        end_date = date.today()
        start_date = end_date - timedelta(weeks=args.weeks)

        result = collector.collect(
            start_date=start_date,
            end_date=end_date,
            contracts=args.contracts
        )

        print(f"Success: {result.success}")
        print(f"Records: {result.records_fetched}")

        if result.warnings:
            print(f"Warnings: {result.warnings}")

        if args.output and result.data is not None:
            if PANDAS_AVAILABLE and hasattr(result.data, 'to_json'):
                result.data.to_json(args.output, orient='records', date_format='iso')
            else:
                with open(args.output, 'w') as f:
                    json.dump(result.data, f, default=str)
            print(f"Saved to: {args.output}")


if __name__ == '__main__':
    main()
