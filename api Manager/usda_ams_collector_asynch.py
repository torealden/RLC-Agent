"""
USDA AMS Market News Data Collector - Complete Implementation
Specialized agent for USDA AMS Market News API
Handles authentication, rate limiting, and data extraction
Round Lakes Commodities

Features:
- Loads API credentials from .env file
- Loads report configurations from Excel file
- Automatic retry with exponential backoff
- Standardized data parsing for grain and ethanol reports
- Uses asyncio for concurrent API requests
- Historical data collection with date range support
- Raw data preservation for debugging and database building
"""
# Added imports for asyncio and aiohttp
import asyncio
import aiohttp
from aiohttp import ClientSession, BasicAuth
import json
import logging
import os
import argparse
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any, Tuple
import pandas as pd
from pathlib import Path
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class USDACollector:
    """
    Specialized agent for USDA AMS Market News API
    Handles authentication, rate limiting, and data extraction
    Complete implementation with all helper functions

    Supports:
    - Daily price collection
    - Historical data collection with date ranges
    - Raw data preservation for database building
    """
    def __init__(self, api_key: str = None, output_dir: str = './data', config_path: str = 'report_config.xlsx',
                 save_raw: bool = True):
        """
        Initialize the USDA collector.

        Args:
            api_key: Your USDA AMS API key. If None, reads from USDA_AMS_API_KEY env var.
            output_dir: Directory to save collected data.
            config_path: Path to the Excel file containing report configurations.
            save_raw: Whether to save raw API responses for debugging.
        """
        # Load API key from environment if not provided
        if api_key is None:
            api_key = os.getenv('USDA_AMS_API_KEY')
            if api_key:
                logger.info("Loaded USDA API key from environment")
            else:
                logger.warning("No USDA API key provided or found in environment")
        self.api_key = api_key
        self.base_url = 'https://marsapi.ams.usda.gov/services/v1.2'
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.today = datetime.now().strftime('%m/%d/%Y')
        self.save_raw = save_raw
        # Removed requests session setup; using aiohttp for asynchronous requests
        # Load report configurations from Excel (or defaults)
        self.report_configs = self._load_report_configs(config_path)
    
    def _load_report_configs(self, config_path: str) -> List[Dict]:
        """
        Load report configurations from Excel file or use defaults.
        
        Args:
            config_path: Path to Excel configuration file
        Returns:
            List of report configuration dictionaries
        """
        default_configs = [
            {'id': '2849', 'name': 'Daily Grain Review', 'type': 'grain'},
            {'id': '3617', 'name': 'Daily Ethanol Report', 'type': 'ethanol'},
            {'id': '2852', 'name': 'Iowa Daily Grain Bids', 'type': 'grain'}
        ]
        try:
            if Path(config_path).exists():
                df_config = pd.read_excel(config_path)
                # Rename columns if needed to match expected keys
                column_mapping = {'report_id': 'id', 'report_name': 'name', 'report_type': 'type'}
                df_config = df_config.rename(columns={k: v for k, v in column_mapping.items() if k in df_config.columns})
                if 'id' in df_config.columns:
                    df_config['id'] = df_config['id'].astype(str)
                configs = df_config.to_dict(orient='records')
                logger.info(f"Loaded {len(configs)} report configurations from {config_path}")
                return configs
            else:
                logger.info(f"Config file {config_path} not found, using default configurations")
                return default_configs
        except Exception as e:
            logger.warning(f"Failed to load report configurations from {config_path}: {e}")
            logger.info("Using default report configurations")
            return default_configs
    
    # _create_session_with_retry removed since requests Session is no longer used
    
    async def fetch_with_retry(self, session: ClientSession, url: str, params: Dict = None, auth: Any = None,
                                max_retries: int = 3, timeout: int = 30) -> Optional[Any]:
        """
        Fetch data from URL with retry logic and error handling (asynchronous).
        
        Args:
            session: aiohttp ClientSession to use for the request.
            url: The URL to fetch.
            params: Query parameters.
            auth: Authentication object (BasicAuth).
            max_retries: Maximum number of retry attempts.
            timeout: Request timeout in seconds.
        Returns:
            Parsed JSON data if successful, None otherwise.
        """
        # This method is now asynchronous, using aiohttp for HTTP requests
        if auth is None:
            auth = BasicAuth(self.api_key or "", "")
        timeout_cfg = aiohttp.ClientTimeout(total=timeout)
        for attempt in range(max_retries):
            try:
                logger.info(f"Fetching {url} (attempt {attempt+1}/{max_retries})")
                async with session.get(url, params=params, auth=auth, timeout=timeout_cfg) as response:
                    if response.status == 200:
                        try:
                            data = await response.json()
                        except Exception as e:
                            logger.error(f"Failed to decode JSON from {url}: {e}")
                            return None
                        logger.info(f"Successfully fetched data from {url}")
                        return data
                    elif response.status == 401:
                        logger.error("Authentication failed. Check your API key.")
                        return None
                    elif response.status == 404:
                        logger.error(f"Resource not found: {url}")
                        return None
                    elif response.status == 429:
                        # Rate limited - exponential backoff
                        wait_time = 2 ** attempt
                        logger.warning(f"Rate limited. Waiting {wait_time} seconds.")
                        await asyncio.sleep(wait_time)
                        # Continue to retry
                    else:
                        logger.warning(f"Received status code {response.status}")
                        # Will retry on next loop iteration
            except asyncio.TimeoutError:
                logger.error(f"Request timed out (attempt {attempt+1})")
            except aiohttp.ClientError as e:
                logger.error(f"Network error: {e} (attempt {attempt+1})")
            except Exception as e:
                logger.error(f"Unexpected error: {e} (attempt {attempt+1})")
            # Wait before next retry attempt (exponential backoff for general errors)
            if attempt < max_retries - 1:
                await asyncio.sleep(2 ** attempt)
        logger.error(f"Failed to fetch {url} after {max_retries} attempts")
        return None
    
    def parse_usda_response(self, response_data: Any, report_type: str = 'grain',
                             report_name: str = '', report_id: str = '') -> Tuple[List[Dict], Dict]:
        """
        Parse USDA API response and standardize the data structure.

        Args:
            response_data: JSON response from USDA API
            report_type: Type of report being parsed (grain, ethanol, etc.)
            report_name: Name of the report for logging
            report_id: ID of the report

        Returns:
            Tuple of (list of standardized data records, raw response metadata)
        """
        parsed_records = []
        raw_metadata = {}

        try:
            if isinstance(response_data, dict):
                # Extract report-level metadata
                raw_metadata = {
                    'report_title': response_data.get('report_title', ''),
                    'slug_id': response_data.get('slug_id', report_id),
                    'slug_name': response_data.get('slug_name', ''),
                    'report_date': response_data.get('report_date', ''),
                    'published_date': response_data.get('published_date', ''),
                    'office_name': response_data.get('office_name', ''),
                    'market_location_name': response_data.get('market_location_name', ''),
                }

                # The actual data is in 'results' array
                if 'results' in response_data:
                    data_items = response_data['results']
                    logger.info(f"Found {len(data_items)} results in response for {report_name}")
                elif 'data' in response_data:
                    data_items = response_data['data']
                    logger.info(f"Found {len(data_items)} data items in response for {report_name}")
                else:
                    # The response itself might be a list or the data might be at root level
                    # Log available keys to help debug
                    logger.warning(f"No 'results' or 'data' key found in response for {report_name}")
                    logger.info(f"Available keys: {list(response_data.keys())}")
                    data_items = [response_data]
            elif isinstance(response_data, list):
                data_items = response_data
                logger.info(f"Response is a list with {len(data_items)} items")
            else:
                logger.error(f"Unexpected response format: {type(response_data)}")
                return [], raw_metadata

            for item in data_items:
                parsed_record = self._parse_single_record(item, report_type, report_name, report_id)
                if parsed_record:
                    parsed_records.append(parsed_record)

            logger.info(f"Parsed {len(parsed_records)} records from {report_name}")

        except Exception as e:
            logger.error(f"Error parsing USDA response for {report_name}: {e}")
            logger.debug(f"Response data: {json.dumps(response_data, indent=2)[:2000]}")

        return parsed_records, raw_metadata

    def _parse_single_record(self, item: Dict, report_type: str,
                              report_name: str = '', report_id: str = '') -> Optional[Dict]:
        """
        Parse a single record from USDA response.
        Extracts all available price-related fields and preserves raw data.
        IMPORTANT: Preserves ALL original fields from API response to ensure price data isn't lost.

        Args:
            item: Single data item from response
            report_type: Type of report being parsed
            report_name: Name of the report
            report_id: Report ID

        Returns:
            Standardized record dictionary with all available data
        """
        try:
            # Start with common metadata
            record = {
                'source': 'USDA_AMS',
                'source_report': report_name,
                'source_report_id': report_id,
                'report_type': report_type,
                'fetch_timestamp': datetime.now().isoformat()
            }

            # IMPORTANT: First, copy ALL original fields from the API response
            # This ensures we don't lose any price data due to field name mismatches
            for key, value in item.items():
                if key not in record:
                    # Convert to lowercase key for consistency but preserve value
                    record[f'api_{key}'] = value

            # Extract report date - try multiple possible field names
            report_date = (
                item.get('report_date') or
                item.get('published_date') or
                item.get('date') or
                item.get('report_begin_date') or
                item.get('report_end_date') or
                ''
            )
            record['report_date'] = report_date

            # Extract commodity information - try multiple field names
            commodity = (
                item.get('commodity') or
                item.get('commodity_name') or
                item.get('commodity_lov_id') or
                item.get('class') or
                item.get('item') or
                item.get('class_name') or
                item.get('product') or
                ''
            )
            record['commodity'] = str(commodity).lower() if commodity else ''

            # Extract location information
            location = (
                item.get('location') or
                item.get('market_location') or
                item.get('market_location_name') or
                item.get('region') or
                item.get('region_name') or
                item.get('city') or
                item.get('office_city') or
                item.get('plant_location') or
                item.get('office_name') or
                ''
            )
            record['location'] = location

            # Comprehensive price field mapping - USDA uses MANY different naming conventions
            # These are the actual field names used across different USDA AMS reports
            price_fields = {
                'price': [
                    'price', 'current_price', 'price_point', 'wtd_avg', 'weighted_avg',
                    'avg_price', 'average', 'mean', 'price_avg', 'avg'
                ],
                'price_low': [
                    'price_low', 'low_price', 'low', 'price_min', 'low_range',
                    'price_range_low', 'min_price', 'minimum', 'lo', 'low_price_range'
                ],
                'price_high': [
                    'price_high', 'high_price', 'high', 'price_max', 'high_range',
                    'price_range_high', 'max_price', 'maximum', 'hi', 'high_price_range'
                ],
                'price_avg': [
                    'price_avg', 'avg_price', 'average_price', 'avg', 'weighted_avg',
                    'wtd_avg', 'weighted_average', 'mean_price', 'average'
                ],
                'price_mostly': [
                    'price_mostly', 'mostly', 'mostly_price', 'mode', 'most_common'
                ],
            }

            for target_field, source_fields in price_fields.items():
                for src in source_fields:
                    if src in item and item[src] is not None and item[src] != '':
                        parsed = self._parse_price(item[src])
                        if parsed is not None:
                            record[target_field] = parsed
                            break

            # Extract basis fields for grain reports
            basis_fields = {
                'basis': ['basis', 'basis_level', 'basis_price', 'cash_basis'],
                'basis_low': ['basis_low', 'low_basis', 'basis_range_low'],
                'basis_high': ['basis_high', 'high_basis', 'basis_range_high'],
                'basis_change': ['basis_change', 'change', 'basis_chg', 'chg'],
            }

            for target_field, source_fields in basis_fields.items():
                for src in source_fields:
                    if src in item and item[src] is not None and item[src] != '':
                        parsed = self._parse_price(item[src])
                        if parsed is not None:
                            record[target_field] = parsed
                            break

            # Extract unit of measure
            unit = (
                item.get('unit') or
                item.get('unit_of_measure') or
                item.get('commodity_unit') or
                item.get('price_unit') or
                item.get('uom') or
                ''
            )
            record['unit'] = unit

            # Extract additional context fields
            record['grade'] = item.get('grade') or item.get('quality') or item.get('grade_name') or ''
            record['delivery_period'] = item.get('delivery_period') or item.get('delivery_month') or item.get('del_period') or ''
            record['delivery_point'] = item.get('delivery_point') or item.get('delivery_location') or ''
            record['transaction_type'] = item.get('transaction_type') or item.get('type') or item.get('trans_type') or ''
            record['product_type'] = item.get('product_type') or item.get('product') or ''

            # Volume/quantity fields
            volume = item.get('volume') or item.get('quantity') or item.get('head_count') or item.get('qty')
            if volume is not None:
                record['volume'] = volume

            # Weight fields (common in livestock reports)
            weight_avg = item.get('weight_avg') or item.get('avg_weight') or item.get('average_weight')
            weight_low = item.get('weight_low') or item.get('low_weight') or item.get('min_weight')
            weight_high = item.get('weight_high') or item.get('high_weight') or item.get('max_weight')
            if weight_avg:
                record['weight_avg'] = self._parse_price(weight_avg)
            if weight_low:
                record['weight_low'] = self._parse_price(weight_low)
            if weight_high:
                record['weight_high'] = self._parse_price(weight_high)

            # Store raw data for database building and debugging - ALWAYS include this
            record['raw_data'] = json.dumps(item)

            # Remove empty string fields (but keep None, 0 values, and raw_data)
            record = {k: v for k, v in record.items() if v != '' or k == 'raw_data'}

            return record

        except Exception as e:
            logger.error(f"Error parsing single record: {e}")
            logger.debug(f"Item data: {json.dumps(item, indent=2)[:1000]}")
            return None
    
    def _parse_price(self, price_value: Any) -> Optional[float]:
        """
        Parse price value from various formats.
        Helper function to standardize price data.
        
        Args:
            price_value: Price value in various formats
        Returns:
            Parsed price as float, or None if unparseable
        """
        if price_value is None or price_value == '':
            return None
        try:
            if isinstance(price_value, str):
                price_value = price_value.replace('$', '').replace(',', '').strip()
            return float(price_value)
        except (ValueError, TypeError):
            logger.warning(f"Could not parse price value: {price_value}")
            return None
    
    async def collect_daily_prices(self, report_date: str = None) -> List[Dict]:
        """
        Main collection method - collects daily prices from all configured reports concurrently.

        Args:
            report_date: Date in MM/DD/YYYY format (default: today).

        Returns:
            List of all collected and standardized data records.
        """
        if report_date is None:
            report_date = self.today

        auth = BasicAuth(self.api_key or "", "")
        collected_data: List[Dict] = []
        raw_responses: Dict[str, Any] = {}

        # Use a single aiohttp session for all concurrent requests
        async with aiohttp.ClientSession() as session:
            tasks = []
            for report in self.report_configs:
                logger.info(f"Collecting {report['name']} (ID: {report['id']})")
                url = f"{self.base_url}/reports/{report['id']}"
                # Use proper query format per USDA API docs
                params = {'q': f'report_begin_date={report_date}', 'allSections': 'true'}
                # Prepare and schedule fetch tasks for each report (concurrent execution)
                tasks.append(self.fetch_with_retry(session, url, params=params, auth=auth))
            # Run all fetch tasks concurrently and wait for all to complete
            results = await asyncio.gather(*tasks)

        # Process each result after concurrent fetches
        for idx, result in enumerate(results):
            report = self.report_configs[idx]
            if result:
                try:
                    # Save raw response for debugging
                    if self.save_raw:
                        raw_responses[report['name']] = result

                    standardized, metadata = self.parse_usda_response(
                        result,
                        report_type=report.get('type', 'generic'),
                        report_name=report['name'],
                        report_id=report['id']
                    )
                    collected_data.extend(standardized)
                    self._save_report_data(standardized, report['name'], report_date, raw_response=result)
                except Exception as e:
                    logger.error(f"Error processing data for {report['name']}: {e}")
            else:
                logger.error(f"Failed to fetch data for {report['name']} (ID: {report['id']})")

        logger.info(f"Total records collected: {len(collected_data)}")
        self._save_combined_data(collected_data, report_date)

        # Save all raw responses to a single file for analysis
        if self.save_raw and raw_responses:
            self._save_raw_responses(raw_responses, report_date)

        return collected_data

    async def collect_historical_data(self, start_date: str, end_date: str = None,
                                       report_ids: List[str] = None,
                                       commodity_filter: str = None,
                                       location_filter: str = None) -> List[Dict]:
        """
        Collect historical price data for a date range.

        Args:
            start_date: Start date in MM/DD/YYYY format
            end_date: End date in MM/DD/YYYY format (default: today)
            report_ids: List of specific report IDs to fetch (default: all configured reports)
            commodity_filter: Filter results to specific commodity (case-insensitive partial match)
            location_filter: Filter results to specific location (case-insensitive partial match)

        Returns:
            List of all collected data records for the date range
        """
        if end_date is None:
            end_date = self.today

        logger.info(f"Collecting historical data from {start_date} to {end_date}")
        if commodity_filter:
            logger.info(f"Filtering by commodity: {commodity_filter}")
        if location_filter:
            logger.info(f"Filtering by location: {location_filter}")

        # Determine which reports to collect
        if report_ids:
            reports = [r for r in self.report_configs if r['id'] in report_ids]
        else:
            reports = self.report_configs

        auth = BasicAuth(self.api_key or "", "")
        collected_data: List[Dict] = []
        raw_responses: Dict[str, Any] = {}

        async with aiohttp.ClientSession() as session:
            tasks = []
            for report in reports:
                logger.info(f"Collecting historical data for {report['name']} (ID: {report['id']})")
                url = f"{self.base_url}/reports/{report['id']}"
                # Use date range format: start_date:end_date
                params = {
                    'q': f'report_begin_date={start_date}:{end_date}',
                    'allSections': 'true'
                }
                tasks.append(self.fetch_with_retry(session, url, params=params, auth=auth))

            results = await asyncio.gather(*tasks)

        # Process results
        for idx, result in enumerate(results):
            report = reports[idx]
            if result:
                try:
                    if self.save_raw:
                        raw_responses[report['name']] = result

                    standardized, metadata = self.parse_usda_response(
                        result,
                        report_type=report.get('type', 'generic'),
                        report_name=report['name'],
                        report_id=report['id']
                    )

                    # Apply filters if specified
                    if commodity_filter or location_filter:
                        filtered = []
                        for record in standardized:
                            commodity_match = True
                            location_match = True

                            if commodity_filter:
                                commodity = str(record.get('commodity', '')).lower()
                                commodity_match = commodity_filter.lower() in commodity

                            if location_filter:
                                location = str(record.get('location', '')).lower()
                                location_match = location_filter.lower() in location

                            if commodity_match and location_match:
                                filtered.append(record)

                        standardized = filtered
                        logger.info(f"After filtering: {len(standardized)} records for {report['name']}")

                    collected_data.extend(standardized)
                    logger.info(f"Collected {len(standardized)} records for {report['name']}")
                except Exception as e:
                    logger.error(f"Error processing historical data for {report['name']}: {e}")
            else:
                logger.error(f"Failed to fetch historical data for {report['name']} (ID: {report['id']})")

        logger.info(f"Total historical records collected: {len(collected_data)}")

        # Save all collected data
        date_str = f"{datetime.strptime(start_date, '%m/%d/%Y').strftime('%Y%m%d')}_to_{datetime.strptime(end_date, '%m/%d/%Y').strftime('%Y%m%d')}"

        # Add filter info to filename if filters were used
        if commodity_filter:
            date_str += f"_{commodity_filter.replace(' ', '_')}"
        if location_filter:
            date_str += f"_{location_filter.replace(' ', '_')}"

        self._save_historical_data(collected_data, date_str)

        if self.save_raw and raw_responses:
            self._save_raw_responses(raw_responses, f"historical_{date_str}")

        return collected_data

    async def collect_single_report_historical(self, report_id: str, start_date: str,
                                                end_date: str = None,
                                                commodity_filter: str = None,
                                                location_filter: str = None) -> List[Dict]:
        """
        Collect historical data for a SINGLE report. Useful for targeted data collection
        without fetching all configured reports.

        Args:
            report_id: The USDA report ID (e.g., '3617' for Daily Ethanol Report)
            start_date: Start date in MM/DD/YYYY format
            end_date: End date in MM/DD/YYYY format (default: today)
            commodity_filter: Filter results to specific commodity (case-insensitive partial match)
            location_filter: Filter results to specific location (case-insensitive partial match)

        Returns:
            List of collected data records for the single report
        """
        if end_date is None:
            end_date = self.today

        # Find the report config or create a temporary one
        report_config = None
        for r in self.report_configs:
            if r['id'] == report_id:
                report_config = r
                break

        if not report_config:
            # Create a temporary config for this report
            report_config = {'id': report_id, 'name': f'Report_{report_id}', 'type': 'generic'}
            logger.info(f"Report {report_id} not in configuration, using generic parsing")

        logger.info(f"Collecting historical data for {report_config['name']} (ID: {report_id})")
        logger.info(f"Date range: {start_date} to {end_date}")

        auth = BasicAuth(self.api_key or "", "")
        collected_data: List[Dict] = []

        async with aiohttp.ClientSession() as session:
            url = f"{self.base_url}/reports/{report_id}"
            params = {
                'q': f'report_begin_date={start_date}:{end_date}',
                'allSections': 'true'
            }
            result = await self.fetch_with_retry(session, url, params=params, auth=auth)

        if result:
            try:
                # Save raw response
                if self.save_raw:
                    date_str = f"{datetime.strptime(start_date, '%m/%d/%Y').strftime('%Y%m%d')}_to_{datetime.strptime(end_date, '%m/%d/%Y').strftime('%Y%m%d')}"
                    self._save_raw_responses({report_config['name']: result}, f"single_report_{report_id}_{date_str}")

                standardized, metadata = self.parse_usda_response(
                    result,
                    report_type=report_config.get('type', 'generic'),
                    report_name=report_config['name'],
                    report_id=report_id
                )

                # Apply filters if specified
                if commodity_filter or location_filter:
                    filtered = []
                    for record in standardized:
                        commodity_match = True
                        location_match = True

                        if commodity_filter:
                            commodity = str(record.get('commodity', '')).lower()
                            commodity_match = commodity_filter.lower() in commodity

                        if location_filter:
                            location = str(record.get('location', '')).lower()
                            location_match = location_filter.lower() in location

                        if commodity_match and location_match:
                            filtered.append(record)

                    standardized = filtered
                    logger.info(f"After filtering: {len(standardized)} records")

                collected_data.extend(standardized)
                logger.info(f"Collected {len(standardized)} records for {report_config['name']}")

                # Save the data
                date_str = f"{datetime.strptime(start_date, '%m/%d/%Y').strftime('%Y%m%d')}_to_{datetime.strptime(end_date, '%m/%d/%Y').strftime('%Y%m%d')}"
                if commodity_filter:
                    date_str += f"_{commodity_filter.replace(' ', '_')}"
                if location_filter:
                    date_str += f"_{location_filter.replace(' ', '_')}"

                self._save_historical_data(collected_data, f"single_{report_id}_{date_str}")

            except Exception as e:
                logger.error(f"Error processing data for {report_config['name']}: {e}")
        else:
            logger.error(f"Failed to fetch data for report {report_id}")

        return collected_data

    def _save_raw_responses(self, raw_responses: Dict[str, Any], date_str: str):
        """Save raw API responses for debugging and analysis."""
        try:
            if '/' in date_str:
                date_str = datetime.strptime(date_str, '%m/%d/%Y').strftime('%Y%m%d')

            raw_dir = self.output_dir / 'raw_responses'
            raw_dir.mkdir(parents=True, exist_ok=True)
            raw_file = raw_dir / f"raw_responses_{date_str}.json"

            with open(raw_file, 'w') as f:
                json.dump(raw_responses, f, indent=2, default=str)
            logger.info(f"Saved raw responses to {raw_file}")
        except Exception as e:
            logger.warning(f"Could not save raw responses: {e}")

    def _save_historical_data(self, data: List[Dict], date_str: str):
        """Save historical data collection results."""
        if not data:
            logger.warning("No historical data to save")
            return

        historical_dir = self.output_dir / 'historical'
        historical_dir.mkdir(parents=True, exist_ok=True)

        # Save JSON
        json_file = historical_dir / f"historical_{date_str}.json"
        try:
            with open(json_file, 'w') as f:
                json.dump(data, f, indent=2, default=str)
            logger.info(f"Saved {len(data)} historical records to {json_file}")
        except Exception as e:
            logger.warning(f"Could not save historical JSON: {e}")

        # Save CSV
        try:
            df = pd.DataFrame(data)
            # Drop raw_data column for CSV (too large)
            if 'raw_data' in df.columns:
                df_csv = df.drop(columns=['raw_data'])
            else:
                df_csv = df
            csv_file = historical_dir / f"historical_{date_str}.csv"
            df_csv.to_csv(csv_file, index=False)
            logger.info(f"Saved historical CSV to {csv_file}")
        except Exception as e:
            logger.warning(f"Could not save historical CSV: {e}")
    
    def _save_report_data(self, data: List[Dict], report_name: str, report_date: str,
                           raw_response: Any = None):
        """
        Save individual report data to file.

        Args:
            data: Parsed data records
            report_name: Name of the report
            report_date: Date of the report
            raw_response: Raw API response (optional, for debugging)
        """
        if not data:
            logger.warning(f"No data to save for {report_name}")
            return

        date_str = datetime.strptime(report_date, '%m/%d/%Y').strftime('%Y%m%d')
        date_dir = self.output_dir / date_str
        date_dir.mkdir(parents=True, exist_ok=True)
        safe_name = report_name.replace(' ', '_').replace('/', '_').replace(':', '_')

        # Save parsed data as JSON
        json_file = date_dir / f"{safe_name}_{date_str}.json"
        try:
            with open(json_file, 'w') as f:
                json.dump(data, f, indent=2, default=str)
            logger.info(f"Saved {len(data)} records to {json_file}")
        except Exception as e:
            logger.warning(f"Could not save JSON for {report_name}: {e}")

        # Save as CSV (without raw_data column for readability)
        try:
            df = pd.DataFrame(data)
            # Drop raw_data column for CSV
            if 'raw_data' in df.columns:
                df_csv = df.drop(columns=['raw_data'])
            else:
                df_csv = df
            csv_file = date_dir / f"{safe_name}_{date_str}.csv"
            df_csv.to_csv(csv_file, index=False)
            logger.info(f"Saved CSV to {csv_file}")
        except Exception as e:
            logger.warning(f"Could not save CSV: {e}")

        # Save raw response if enabled and provided
        if self.save_raw and raw_response:
            raw_file = date_dir / f"{safe_name}_{date_str}_raw.json"
            try:
                with open(raw_file, 'w') as f:
                    json.dump(raw_response, f, indent=2, default=str)
                logger.debug(f"Saved raw response to {raw_file}")
            except Exception as e:
                logger.warning(f"Could not save raw response: {e}")
    
    def _save_combined_data(self, data: List[Dict], report_date: str):
        """
        Save combined data from all reports.
        
        Args:
            data: All collected data
            report_date: Date of collection
        """
        if not data:
            return
        date_str = datetime.strptime(report_date, '%m/%d/%Y').strftime('%Y%m%d')
        combined_file = self.output_dir / f"combined_{date_str}.json"
        try:
            with open(combined_file, 'w') as f:
                json.dump(data, f, indent=2, default=str)
            logger.info(f"Saved combined data to {combined_file}")
        except Exception as e:
            logger.warning(f"Could not save combined JSON: {e}")
    
    async def get_available_reports(self) -> List[Dict]:
        """
        Get list of all available reports from USDA AMS (asynchronous).
        Useful for discovering new report IDs.
        
        Returns:
            List of available reports with their IDs and names.
        """
        url = f"{self.base_url}/reports"
        # Use a temporary session to fetch the reports list
        async with aiohttp.ClientSession() as session:
            auth = BasicAuth(self.api_key or "", "")
            data = await self.fetch_with_retry(session, url, auth=auth)
        if data:
            if isinstance(data, list):
                logger.info(f"Found {len(data)} available reports")
            else:
                logger.info("Fetched available reports (non-list response)")
            return data if isinstance(data, list) else []
        else:
            return []
    
    def add_report(self, report_id: str, report_name: str, report_type: str = 'generic'):
        """
        Dynamically add a report configuration at runtime.
        
        Args:
            report_id: USDA report ID.
            report_name: Descriptive name for the report.
            report_type: Type of report for parsing (grain, ethanol, generic).
        """
        new_config = {'id': str(report_id), 'name': report_name, 'type': report_type}
        self.report_configs.append(new_config)
        logger.info(f"Added report configuration: {report_name} (ID: {report_id})")
    
    def list_configured_reports(self) -> List[Dict]:
        """
        Return the currently configured reports.
        
        Returns:
            List of configured report dictionaries.
        """
        return self.report_configs.copy()

def parse_args():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description='USDA AMS Market News Data Collector - Round Lakes Commodities',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Collect today's data
  python usda_ams_collector_asynch.py

  # Collect data for a specific date
  python usda_ams_collector_asynch.py --date 12/01/2025

  # Collect historical data for a date range
  python usda_ams_collector_asynch.py --historical --start 11/01/2025 --end 12/01/2025

  # List available reports
  python usda_ams_collector_asynch.py --list-reports

  # Collect historical data for specific reports
  python usda_ams_collector_asynch.py --historical --start 11/01/2025 --reports 3617,3618

  # Collect historical data for a SINGLE report (faster, targeted)
  python usda_ams_collector_asynch.py --single-report 3617 --start 01/01/2025 --end 12/01/2025

  # Filter historical data by commodity
  python usda_ams_collector_asynch.py --historical --start 11/01/2025 --commodity ethanol

  # Filter historical data by location
  python usda_ams_collector_asynch.py --historical --start 11/01/2025 --location Iowa

  # Combine filters for targeted data collection
  python usda_ams_collector_asynch.py --single-report 2849 --start 01/01/2025 --commodity corn --location Iowa
        """
    )

    parser.add_argument('--date', '-d', type=str, default=None,
                        help='Specific date to collect (MM/DD/YYYY format)')
    parser.add_argument('--historical', '-H', action='store_true',
                        help='Collect historical data for a date range')
    parser.add_argument('--single-report', '-S', type=str, default=None,
                        help='Collect historical data for a SINGLE report ID (faster than --historical)')
    parser.add_argument('--start', '-s', type=str,
                        help='Start date for historical collection (MM/DD/YYYY)')
    parser.add_argument('--end', '-e', type=str,
                        help='End date for historical collection (MM/DD/YYYY, default: today)')
    parser.add_argument('--reports', '-r', type=str,
                        help='Comma-separated list of report IDs to collect')
    parser.add_argument('--commodity', type=str, default=None,
                        help='Filter results by commodity (partial match, case-insensitive)')
    parser.add_argument('--location', type=str, default=None,
                        help='Filter results by location (partial match, case-insensitive)')
    parser.add_argument('--output', '-o', type=str, default='./usda_data',
                        help='Output directory for collected data')
    parser.add_argument('--config', '-c', type=str, default='report_config.xlsx',
                        help='Path to report configuration Excel file')
    parser.add_argument('--list-reports', '-l', action='store_true',
                        help='List available USDA reports and exit')
    parser.add_argument('--no-raw', action='store_true',
                        help='Do not save raw API responses')
    parser.add_argument('--verbose', '-v', action='store_true',
                        help='Enable verbose logging')

    return parser.parse_args()


async def main():
    """
    Main entry point for the USDA AMS Data Collector.
    Supports daily collection, historical data collection, and report listing.
    """
    args = parse_args()

    # Set logging level
    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)

    # Initialize the collector
    collector = USDACollector(
        output_dir=args.output,
        config_path=args.config,
        save_raw=not args.no_raw
    )

    # Check if API key was loaded
    if not collector.api_key:
        print("ERROR: No API key found!")
        print("Please ensure USDA_AMS_API_KEY is set in your .env file")
        return

    # List available reports mode
    if args.list_reports:
        print("\nFetching available USDA AMS reports...")
        reports = await collector.get_available_reports()
        if reports:
            print(f"\nFound {len(reports)} reports available:\n")
            for report in (reports if isinstance(reports, list) else []):
                if isinstance(report, Dict):
                    print(f"  {report.get('slug_id', 'N/A'):6} | {report.get('report_title', report.get('slug_name', 'N/A'))}")
        return

    # Show configured reports
    print("Configured reports:")
    for report in collector.list_configured_reports():
        print(f"  - {report['name']} (ID: {report['id']}, Type: {report.get('type', 'generic')})")

    data = []

    # Single report historical data collection mode
    if args.single_report:
        if not args.start:
            print("ERROR: --start date is required for single report historical collection")
            return

        print(f"\nCollecting historical data for report {args.single_report}")
        print(f"Date range: {args.start} to {args.end or 'today'}")
        if args.commodity:
            print(f"Commodity filter: {args.commodity}")
        if args.location:
            print(f"Location filter: {args.location}")

        data = await collector.collect_single_report_historical(
            report_id=args.single_report,
            start_date=args.start,
            end_date=args.end,
            commodity_filter=args.commodity,
            location_filter=args.location
        )
        print(f"\nCollected {len(data)} total records for report {args.single_report}")

    # Historical data collection mode (multiple reports)
    elif args.historical:
        if not args.start:
            print("ERROR: --start date is required for historical collection")
            return

        report_ids = args.reports.split(',') if args.reports else None
        print(f"\nCollecting historical data from {args.start} to {args.end or 'today'}...")
        if args.commodity:
            print(f"Commodity filter: {args.commodity}")
        if args.location:
            print(f"Location filter: {args.location}")

        data = await collector.collect_historical_data(
            start_date=args.start,
            end_date=args.end,
            report_ids=report_ids,
            commodity_filter=args.commodity,
            location_filter=args.location
        )
        print(f"\nCollected {len(data)} total historical records")

    else:
        # Daily collection mode
        report_date = args.date
        date_display = report_date or 'today'
        print(f"\nCollecting data for {date_display}...")

        data = await collector.collect_daily_prices(report_date=report_date)
        print(f"\nCollected {len(data)} total records")

    # Show sample record if any data was collected
    if data:
        print("\nSample record (showing first record with price data):")
        # Find a record with price data to display
        sample = None
        for record in data:
            if any(k in record for k in ['price', 'price_avg', 'price_low', 'price_high']):
                sample = record
                break
        if sample is None:
            sample = data[0]

        # Display sample without raw_data (too verbose for console)
        display_sample = {k: v for k, v in sample.items() if k != 'raw_data' and not k.startswith('api_')}
        print(json.dumps(display_sample, indent=2, default=str))

        # Show summary of ALL fields found (including api_ prefixed ones)
        all_fields = set()
        for record in data:
            all_fields.update(record.keys())

        # Show price/basis fields
        price_fields = [f for f in all_fields if 'price' in f.lower() or 'basis' in f.lower()]
        if price_fields:
            print(f"\nPrice/basis fields found in data: {', '.join(sorted(price_fields))}")

        # Show API fields (original fields from USDA response)
        api_fields = [f for f in all_fields if f.startswith('api_')]
        if api_fields:
            print(f"\nOriginal API fields preserved (api_* prefix): {', '.join(sorted(api_fields))}")
            print("Note: These fields contain the raw values from the USDA API response.")

        # Check if raw_data is present
        if any('raw_data' in record for record in data):
            print("\nraw_data field: Present (contains full JSON from API for database building)")
        else:
            print("\nWARNING: raw_data field not found in output!")


if __name__ == "__main__":
    print("USDA AMS Data Collector - Round Lakes Commodities")
    print("=" * 50)
    print("\nThis script includes:")
    print("  ✓ Automatic API key loading from .env file")
    print("  ✓ Excel-based report configuration")
    print("  ✓ Asynchronous fetch_with_retry() with exponential backoff")
    print("  ✓ parse_usda_response() - Intelligent response parsing")
    print("  ✓ collect_daily_prices() - Daily data collection")
    print("  ✓ collect_historical_data() - Historical data with date ranges")
    print("  ✓ Raw response saving for database building")
    print("\nEnsure your .env file contains USDA_AMS_API_KEY")
    print("\nUsage: python usda_ams_collector_asynch.py --help for options")
    print()
    asyncio.run(main())
