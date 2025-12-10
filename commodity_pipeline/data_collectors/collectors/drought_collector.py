"""
US Drought Monitor Data Collector

Collects drought condition data from the US Drought Monitor.
Updated every Thursday for conditions as of Tuesday.

Data source: https://droughtmonitor.unl.edu/

No API key required - public data download.
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


# Drought category definitions
DROUGHT_CATEGORIES = {
    'NONE': {'code': -1, 'description': 'None', 'color': '#FFFFFF'},
    'D0': {'code': 0, 'description': 'Abnormally Dry', 'color': '#FFFF00'},
    'D1': {'code': 1, 'description': 'Moderate Drought', 'color': '#FCD37F'},
    'D2': {'code': 2, 'description': 'Severe Drought', 'color': '#FFAA00'},
    'D3': {'code': 3, 'description': 'Extreme Drought', 'color': '#E60000'},
    'D4': {'code': 4, 'description': 'Exceptional Drought', 'color': '#730000'},
}

# Key agricultural states
AG_STATES = [
    'IA',  # Iowa - corn, soybeans
    'IL',  # Illinois - corn, soybeans
    'NE',  # Nebraska - corn
    'MN',  # Minnesota - corn, soybeans
    'IN',  # Indiana - corn, soybeans
    'KS',  # Kansas - wheat, corn
    'ND',  # North Dakota - wheat, soybeans
    'SD',  # South Dakota - corn, soybeans
    'OH',  # Ohio - corn, soybeans
    'MO',  # Missouri - corn, soybeans
    'TX',  # Texas - cotton, wheat
    'OK',  # Oklahoma - wheat
    'MT',  # Montana - wheat
    'WA',  # Washington - wheat
    'ID',  # Idaho - wheat
]


@dataclass
class DroughtConfig(CollectorConfig):
    """Drought Monitor specific configuration"""
    source_name: str = "US Drought Monitor"
    source_url: str = "https://droughtmonitor.unl.edu/DmData/DataDownload/ComprehensiveStatistics.aspx"
    auth_type: AuthType = AuthType.NONE
    frequency: DataFrequency = DataFrequency.WEEKLY

    # States to track
    states: List[str] = field(default_factory=lambda: AG_STATES)

    # Data endpoints
    csv_endpoint: str = "https://droughtmonitor.unl.edu/DmData/DataDownload/ComprehensiveStatistics.aspx"
    geojson_endpoint: str = "https://droughtmonitor.unl.edu/DmData/GeoJSON/"


class DroughtCollector(BaseCollector):
    """
    Collector for US Drought Monitor data.

    Provides:
    - State-level drought statistics
    - Percentage of area in each drought category
    - Weekly comparisons
    - Agricultural impact assessment
    """

    def __init__(self, config: DroughtConfig = None):
        config = config or DroughtConfig()
        super().__init__(config)
        self.config: DroughtConfig = config

    def get_table_name(self) -> str:
        return "drought_conditions"

    def fetch_data(
        self,
        start_date: date = None,
        end_date: date = None,
        states: List[str] = None,
        area_type: str = "state",
        **kwargs
    ) -> CollectorResult:
        """
        Fetch drought data from US Drought Monitor.

        Args:
            start_date: Start date for data range
            end_date: End date (default: today)
            states: List of state codes to fetch
            area_type: 'state', 'county', or 'national'

        Returns:
            CollectorResult with drought data
        """
        states = states or self.config.states
        end_date = end_date or date.today()
        start_date = start_date or (end_date - timedelta(days=365))

        # Build URL for CSV download
        url = self._build_download_url(
            start_date, end_date, states, area_type
        )

        response, error = self._make_request(url, timeout=60)

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

        # Parse CSV response
        try:
            records = self._parse_csv_response(response.text, states)

            if not records:
                return CollectorResult(
                    success=False,
                    source=self.config.source_name,
                    error_message="No data in response"
                )

            # Convert to DataFrame
            if PANDAS_AVAILABLE:
                df = pd.DataFrame(records)
                df['date'] = pd.to_datetime(df['date'])
                df = df.sort_values(['state', 'date'])
            else:
                df = records

            return CollectorResult(
                success=True,
                source=self.config.source_name,
                records_fetched=len(records),
                data=df,
                period_start=start_date.isoformat(),
                period_end=end_date.isoformat()
            )

        except Exception as e:
            return CollectorResult(
                success=False,
                source=self.config.source_name,
                error_message=f"Parse error: {e}"
            )

    def _build_download_url(
        self,
        start_date: date,
        end_date: date,
        states: List[str],
        area_type: str
    ) -> str:
        """Build download URL for drought data"""
        # The Drought Monitor has a specific URL format for data download
        base_url = "https://droughtmonitor.unl.edu/DmData/DataDownload/WebServiceCall.aspx"

        params = {
            'aession': 'D',  # Drought statistics
            'stateabbr': ','.join(states) if states else 'US',
            'startdate': start_date.strftime('%m/%d/%Y'),
            'enddate': end_date.strftime('%m/%d/%Y'),
            'format': 'json',
        }

        param_str = '&'.join(f"{k}={v}" for k, v in params.items())
        return f"{base_url}?{param_str}"

    def _parse_csv_response(
        self,
        content: str,
        states: List[str]
    ) -> List[Dict]:
        """Parse CSV/JSON response from Drought Monitor"""
        records = []

        try:
            import json
            data = json.loads(content)

            for item in data:
                state = item.get('State', item.get('state', ''))

                if states and state not in states:
                    continue

                record = {
                    'date': item.get('MapDate', item.get('date')),
                    'state': state,
                    'fips': item.get('FIPS'),

                    # Percentage of area in each drought category
                    'none_pct': self._safe_float(item.get('None')),
                    'd0_pct': self._safe_float(item.get('D0')),
                    'd1_pct': self._safe_float(item.get('D1')),
                    'd2_pct': self._safe_float(item.get('D2')),
                    'd3_pct': self._safe_float(item.get('D3')),
                    'd4_pct': self._safe_float(item.get('D4')),

                    # Aggregated metrics
                    'drought_pct': None,  # Will calculate
                    'severe_drought_pct': None,  # D2+D3+D4

                    'source': 'USDM',
                }

                # Calculate aggregates
                d0 = record['d0_pct'] or 0
                d1 = record['d1_pct'] or 0
                d2 = record['d2_pct'] or 0
                d3 = record['d3_pct'] or 0
                d4 = record['d4_pct'] or 0

                record['drought_pct'] = d0 + d1 + d2 + d3 + d4
                record['severe_drought_pct'] = d2 + d3 + d4

                records.append(record)

        except json.JSONDecodeError:
            # Try parsing as CSV
            lines = content.strip().split('\n')
            if len(lines) < 2:
                return records

            headers = lines[0].split(',')

            for line in lines[1:]:
                values = line.split(',')
                if len(values) != len(headers):
                    continue

                row = dict(zip(headers, values))
                state = row.get('State', '')

                if states and state not in states:
                    continue

                record = {
                    'date': row.get('MapDate', row.get('Date')),
                    'state': state,
                    'd0_pct': self._safe_float(row.get('D0')),
                    'd1_pct': self._safe_float(row.get('D1')),
                    'd2_pct': self._safe_float(row.get('D2')),
                    'd3_pct': self._safe_float(row.get('D3')),
                    'd4_pct': self._safe_float(row.get('D4')),
                    'source': 'USDM',
                }
                records.append(record)

        return records

    def _safe_float(self, value: Any) -> Optional[float]:
        """Safely convert value to float"""
        if value is None or value == '':
            return None
        try:
            return float(value)
        except (ValueError, TypeError):
            return None

    def parse_response(self, response_data: Any) -> Any:
        """Parse response data"""
        return response_data

    # =========================================================================
    # CONVENIENCE METHODS
    # =========================================================================

    def get_corn_belt_drought(self) -> Optional[Dict]:
        """
        Get current drought conditions in the Corn Belt.

        Returns:
            Dict with aggregated drought metrics for corn states
        """
        corn_states = ['IA', 'IL', 'NE', 'MN', 'IN', 'OH', 'SD']

        result = self.collect(
            start_date=date.today() - timedelta(days=14),
            end_date=date.today(),
            states=corn_states
        )

        if not result.success or result.data is None:
            return None

        if PANDAS_AVAILABLE and hasattr(result.data, 'groupby'):
            df = result.data
            latest_date = df['date'].max()
            latest = df[df['date'] == latest_date]

            return {
                'date': str(latest_date),
                'states_included': corn_states,
                'avg_drought_pct': latest['drought_pct'].mean(),
                'avg_severe_drought_pct': latest['severe_drought_pct'].mean(),
                'states_with_severe': (latest['severe_drought_pct'] > 10).sum(),
                'by_state': latest[['state', 'drought_pct', 'severe_drought_pct']].to_dict('records'),
            }

        return None

    def get_wheat_belt_drought(self) -> Optional[Dict]:
        """
        Get current drought conditions in the Wheat Belt.

        Returns:
            Dict with aggregated drought metrics for wheat states
        """
        wheat_states = ['KS', 'OK', 'TX', 'ND', 'MT', 'WA', 'ID', 'NE', 'CO']

        result = self.collect(
            start_date=date.today() - timedelta(days=14),
            end_date=date.today(),
            states=wheat_states
        )

        if not result.success or result.data is None:
            return None

        if PANDAS_AVAILABLE and hasattr(result.data, 'groupby'):
            df = result.data
            latest_date = df['date'].max()
            latest = df[df['date'] == latest_date]

            return {
                'date': str(latest_date),
                'states_included': wheat_states,
                'avg_drought_pct': latest['drought_pct'].mean(),
                'avg_severe_drought_pct': latest['severe_drought_pct'].mean(),
                'by_state': latest[['state', 'drought_pct', 'severe_drought_pct']].to_dict('records'),
            }

        return None

    def get_weekly_comparison(
        self,
        state: str,
        weeks: int = 4
    ) -> Optional[Any]:
        """
        Get weekly drought comparison for a state.

        Args:
            state: State code (e.g., 'IA')
            weeks: Number of weeks to compare

        Returns:
            DataFrame with weekly drought progression
        """
        result = self.collect(
            start_date=date.today() - timedelta(weeks=weeks),
            end_date=date.today(),
            states=[state]
        )

        return result.data if result.success else None


# =============================================================================
# CLI INTERFACE
# =============================================================================

def main():
    """Command-line interface for Drought collector"""
    import argparse
    import json

    parser = argparse.ArgumentParser(description='US Drought Monitor Data Collector')

    parser.add_argument(
        'command',
        choices=['fetch', 'corn_belt', 'wheat_belt', 'test'],
        help='Command to execute'
    )

    parser.add_argument(
        '--states',
        nargs='+',
        help='State codes to fetch (e.g., IA IL NE)'
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
        help='Output file (JSON or CSV)'
    )

    args = parser.parse_args()

    # Create collector
    config = DroughtConfig(states=args.states or AG_STATES)
    collector = DroughtCollector(config)

    if args.command == 'test':
        success, message = collector.test_connection()
        print(f"Connection test: {'PASS' if success else 'FAIL'} - {message}")
        return

    if args.command == 'corn_belt':
        data = collector.get_corn_belt_drought()
        if data:
            print(json.dumps(data, indent=2, default=str))
        else:
            print("Failed to get Corn Belt drought data")
        return

    if args.command == 'wheat_belt':
        data = collector.get_wheat_belt_drought()
        if data:
            print(json.dumps(data, indent=2, default=str))
        else:
            print("Failed to get Wheat Belt drought data")
        return

    if args.command == 'fetch':
        end_date = date.today()
        start_date = end_date - timedelta(weeks=args.weeks)

        result = collector.collect(
            start_date=start_date,
            end_date=end_date,
            states=args.states
        )

        print(f"Success: {result.success}")
        print(f"Records: {result.records_fetched}")

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
