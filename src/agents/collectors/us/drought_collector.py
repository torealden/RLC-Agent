"""
US Drought Monitor Data Collector

Collects drought condition data from the US Drought Monitor.
Updated every Thursday for conditions as of Tuesday.

Data source: https://droughtmonitor.unl.edu/
API docs: https://droughtmonitor.unl.edu/DmData/DataDownload/WebServiceInfo.aspx

No API key required - public data download.

IMPORTANT: The USDM API requires two-digit FIPS codes for state-level
queries (e.g., '19' for Iowa), NOT state abbreviations. The response
is CSV format (not JSON).
"""

import csv
import io
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

from src.services.database.db_config import get_connection as get_db_connection

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

# State abbreviation -> two-digit FIPS code mapping
# The USDM API requires FIPS codes, not abbreviations
STATE_FIPS = {
    'AL': '01', 'AK': '02', 'AZ': '04', 'AR': '05', 'CA': '06',
    'CO': '08', 'CT': '09', 'DE': '10', 'FL': '12', 'GA': '13',
    'HI': '15', 'ID': '16', 'IL': '17', 'IN': '18', 'IA': '19',
    'KS': '20', 'KY': '21', 'LA': '22', 'ME': '23', 'MD': '24',
    'MA': '25', 'MI': '26', 'MN': '27', 'MS': '28', 'MO': '29',
    'MT': '30', 'NE': '31', 'NV': '32', 'NH': '33', 'NJ': '34',
    'NM': '35', 'NY': '36', 'NC': '37', 'ND': '38', 'OH': '39',
    'OK': '40', 'OR': '41', 'PA': '42', 'RI': '44', 'SC': '45',
    'SD': '46', 'TN': '47', 'TX': '48', 'UT': '49', 'VT': '50',
    'VA': '51', 'WA': '53', 'WV': '54', 'WI': '55', 'WY': '56',
}

# Reverse mapping for response parsing
FIPS_STATE = {v: k for k, v in STATE_FIPS.items()}

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
    source_url: str = "https://usdmdataservices.unl.edu/api"
    auth_type: AuthType = AuthType.NONE
    frequency: DataFrequency = DataFrequency.WEEKLY

    # States to track
    states: List[str] = field(default_factory=lambda: AG_STATES)

    # REST API endpoints
    # Docs: https://droughtmonitor.unl.edu/DmData/DataDownload/WebServiceInfo.aspx
    state_stats_endpoint: str = "/StateStatistics/GetDroughtSeverityStatisticsByAreaPercent"
    us_stats_endpoint: str = "/USStatistics/GetDroughtSeverityStatisticsByAreaPercent"
    county_stats_endpoint: str = "/CountyStatistics/GetDroughtSeverityStatisticsByAreaPercent"


class DroughtCollector(BaseCollector):
    """
    Collector for US Drought Monitor data.

    Provides:
    - State-level drought statistics (% area in each category)
    - Percentage of area in each drought category (D0-D4)
    - Weekly comparisons
    - Agricultural impact assessment

    The USDM API returns CSV data and requires state FIPS codes
    (not abbreviations) for the aoi parameter.
    """

    def __init__(self, config: DroughtConfig = None):
        config = config or DroughtConfig()
        super().__init__(config)
        self.config: DroughtConfig = config

    def get_table_name(self) -> str:
        return "drought_conditions"

    def _state_to_fips(self, state_abbr: str) -> Optional[str]:
        """Convert state abbreviation to two-digit FIPS code."""
        return STATE_FIPS.get(state_abbr.upper())

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
            states: List of state abbreviation codes to fetch
            area_type: 'state', 'county', or 'national'

        Returns:
            CollectorResult with drought data
        """
        states = states or self.config.states
        end_date = end_date or date.today()
        start_date = start_date or (end_date - timedelta(days=365))

        all_records = []
        warnings = []

        if area_type == 'national':
            # Single request for national data
            url = self._build_download_url(start_date, end_date, None, 'national')
            response, error = self._make_request(url, timeout=60)

            if error:
                warnings.append(f"US national: {error}")
            elif response.status_code != 200:
                warnings.append(f"US national: HTTP {response.status_code}")
            else:
                try:
                    records = self._parse_csv_response(response.text)
                    all_records.extend(records)
                except Exception as e:
                    warnings.append(f"US national: Parse error - {e}")
        else:
            # Fetch each state separately (API requires per-state requests)
            for state in states:
                fips = self._state_to_fips(state)
                if not fips:
                    warnings.append(f"{state}: Unknown state, no FIPS code")
                    continue

                url = self._build_download_url(
                    start_date, end_date, fips, area_type
                )

                response, error = self._make_request(url, timeout=60)

                if error:
                    warnings.append(f"{state}: {error}")
                    continue

                if response.status_code != 200:
                    warnings.append(f"{state}: HTTP {response.status_code}")
                    continue

                try:
                    records = self._parse_csv_response(response.text)
                    all_records.extend(records)
                except Exception as e:
                    warnings.append(f"{state}: Parse error - {e}")

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
            df['date'] = pd.to_datetime(df['date'])
            df = df.sort_values(['state', 'date'])
        else:
            df = all_records

        return CollectorResult(
            success=True,
            source=self.config.source_name,
            records_fetched=len(all_records),
            data=df,
            period_start=start_date.isoformat(),
            period_end=end_date.isoformat(),
            warnings=warnings if warnings else None
        )

    def _build_download_url(
        self,
        start_date: date,
        end_date: date,
        aoi: Optional[str],
        area_type: str
    ) -> str:
        """Build download URL for drought data.

        Args:
            start_date: Start of date range
            end_date: End of date range
            aoi: Area of interest - FIPS code for states, 'us' for national
            area_type: 'state', 'county', or 'national'

        Returns:
            Full API URL with query parameters
        """
        base_url = self.config.source_url

        if area_type == 'national':
            endpoint = self.config.us_stats_endpoint
            aoi_param = 'us'
        elif area_type == 'county':
            endpoint = self.config.county_stats_endpoint
            aoi_param = aoi or '19'
        else:
            endpoint = self.config.state_stats_endpoint
            aoi_param = aoi or '19'

        # statisticsType=2 returns area percent (1 returns absolute area sq mi)
        params = {
            'aoi': aoi_param,
            'startdate': start_date.strftime('%m/%d/%Y'),
            'enddate': end_date.strftime('%m/%d/%Y'),
            'statisticsType': '2',
        }

        param_str = '&'.join(f"{k}={v}" for k, v in params.items())
        return f"{base_url}{endpoint}?{param_str}"

    def _parse_csv_response(self, content: str) -> List[Dict]:
        """Parse CSV response from Drought Monitor API.

        The API returns CSV with columns:
        MapDate, StateAbbreviation (or AreaOfInterest for US),
        None, D0, D1, D2, D3, D4, ValidStart, ValidEnd, StatisticFormatID

        Values are percentages when statisticsType=2.
        """
        records = []

        if not content or not content.strip():
            return records

        reader = csv.DictReader(io.StringIO(content))

        for row in reader:
            # Get state abbreviation - field name varies by endpoint
            state = (row.get('StateAbbreviation') or
                     row.get('AreaOfInterest') or
                     row.get('CountyName') or '')

            # For national data, AreaOfInterest = "CONUS"
            if state == 'CONUS':
                state = 'US'

            # Parse date from MapDate (format: YYYYMMDD)
            map_date_str = row.get('MapDate', '')
            if map_date_str:
                try:
                    report_date = datetime.strptime(str(map_date_str), '%Y%m%d').strftime('%Y-%m-%d')
                except ValueError:
                    report_date = map_date_str
            else:
                report_date = row.get('ValidStart', '')

            # Parse drought percentages
            # CSV column 'None' is the non-drought percentage
            none_pct = self._safe_float(row.get('None'))
            d0 = self._safe_float(row.get('D0')) or 0
            d1 = self._safe_float(row.get('D1')) or 0
            d2 = self._safe_float(row.get('D2')) or 0
            d3 = self._safe_float(row.get('D3')) or 0
            d4 = self._safe_float(row.get('D4')) or 0

            record = {
                'date': report_date,
                'state': state.upper().strip() if state else '',
                'valid_start': row.get('ValidStart', ''),
                'valid_end': row.get('ValidEnd', ''),
                'none_pct': none_pct,
                'd0_pct': d0,
                'd1_pct': d1,
                'd2_pct': d2,
                'd3_pct': d3,
                'd4_pct': d4,
                'drought_pct': round(d0 + d1 + d2 + d3 + d4, 2),
                'severe_drought_pct': round(d2 + d3 + d4, 2),
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

    def save_to_bronze(self, records: list) -> int:
        """Upsert drought records to bronze.drought_conditions."""
        if not records:
            return 0

        with get_db_connection() as conn:
            cur = conn.cursor()
            count = 0
            for rec in records:
                cur.execute("""
                    INSERT INTO bronze.drought_conditions
                        (map_date, state, valid_start, valid_end,
                         none_pct, d0_pct, d1_pct, d2_pct, d3_pct, d4_pct,
                         drought_pct, severe_drought_pct,
                         source, collected_at)
                    VALUES
                        (%(date)s, %(state)s, %(valid_start)s, %(valid_end)s,
                         %(none_pct)s, %(d0_pct)s, %(d1_pct)s, %(d2_pct)s,
                         %(d3_pct)s, %(d4_pct)s,
                         %(drought_pct)s, %(severe_drought_pct)s,
                         %(source)s, NOW())
                    ON CONFLICT (map_date, state)
                    DO UPDATE SET
                        none_pct = EXCLUDED.none_pct,
                        d0_pct = EXCLUDED.d0_pct,
                        d1_pct = EXCLUDED.d1_pct,
                        d2_pct = EXCLUDED.d2_pct,
                        d3_pct = EXCLUDED.d3_pct,
                        d4_pct = EXCLUDED.d4_pct,
                        drought_pct = EXCLUDED.drought_pct,
                        severe_drought_pct = EXCLUDED.severe_drought_pct,
                        collected_at = NOW()
                """, rec)
                count += 1
            conn.commit()
            self.logger.info(f"Saved {count} records to bronze.drought_conditions")
            return count

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
                'states_with_severe': int((latest['severe_drought_pct'] > 10).sum()),
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
        choices=['fetch', 'corn_belt', 'wheat_belt', 'test', 'save'],
        help='Command to execute (save = fetch + save_to_bronze)'
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

    logging.basicConfig(level=logging.INFO)

    # Create collector
    config = DroughtConfig(states=args.states or AG_STATES)
    collector = DroughtCollector(config)

    if args.command == 'test':
        # Test with a simple single-state fetch
        test_result = collector.collect(
            start_date=date.today() - timedelta(days=14),
            end_date=date.today(),
            states=['IA'],
            use_cache=False
        )
        if test_result.success:
            print(f"Connection test: PASS - {test_result.records_fetched} records")
        else:
            print(f"Connection test: FAIL - {test_result.error_message}")
            if test_result.warnings:
                for w in test_result.warnings:
                    print(f"  Warning: {w}")
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

    if args.command in ('fetch', 'save'):
        end_date = date.today()
        start_date = end_date - timedelta(weeks=args.weeks)

        result = collector.collect(
            start_date=start_date,
            end_date=end_date,
            states=args.states,
            use_cache=False
        )

        print(f"Success: {result.success}")
        print(f"Records: {result.records_fetched}")

        if result.error_message:
            print(f"Error: {result.error_message}")
        if result.warnings:
            for w in result.warnings:
                print(f"  Warning: {w}")

        # Save to database if requested
        if args.command == 'save' and result.success and result.data is not None:
            if PANDAS_AVAILABLE and hasattr(result.data, 'to_dict'):
                records = result.data.to_dict('records')
                # Convert Timestamp back to string for DB insert
                for r in records:
                    if hasattr(r.get('date'), 'strftime'):
                        r['date'] = r['date'].strftime('%Y-%m-%d')
            else:
                records = result.data
            saved = collector.save_to_bronze(records)
            print(f"Saved to bronze: {saved} records")

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
