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
"""

import json
import time
import logging
import os
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any
import requests
from requests.auth import HTTPBasicAuth
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
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
    """
    
    def __init__(self, api_key: str = None, output_dir: str = './data', config_path: str = 'report_config.xlsx'):
        """
        Initialize the USDA collector.
        
        Args:
            api_key: Your USDA AMS API key. If None, reads from USDA_AMS_API_KEY env var.
            output_dir: Directory to save collected data.
            config_path: Path to the Excel file containing report configurations.
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
        
        # Configure session with retry logic
        self.session = self._create_session_with_retry()
        
        # Load report configurations from Excel if available, otherwise use defaults
        self.report_configs = self._load_report_configs(config_path)
    
    def _load_report_configs(self, config_path: str) -> List[Dict]:
        """
        Load report configurations from Excel file or use defaults.
        
        Args:
            config_path: Path to Excel configuration file
            
        Returns:
            List of report configuration dictionaries
        """
        # Default report configurations (fallback)
        default_configs = [
            {
                'id': '2849',
                'name': 'Daily Grain Review',
                'type': 'grain'
            },
            {
                'id': '3617',
                'name': 'Daily Ethanol Report',
                'type': 'ethanol'
            },
            {
                'id': '2852',
                'name': 'Iowa Daily Grain Bids',
                'type': 'grain'
            }
        ]
        
        # Try to load from Excel file
        try:
            if Path(config_path).exists():
                df_config = pd.read_excel(config_path)
                
                # Rename columns if needed to match expected keys
                column_mapping = {
                    'report_id': 'id',
                    'report_name': 'name',
                    'report_type': 'type'
                }
                df_config = df_config.rename(columns={k: v for k, v in column_mapping.items() if k in df_config.columns})
                
                # Ensure IDs are strings (if numeric IDs were read)
                if 'id' in df_config.columns:
                    df_config['id'] = df_config['id'].astype(str)
                
                # Convert DataFrame to list of dicts
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
    
    def _create_session_with_retry(self) -> requests.Session:
        """
        Create a requests session with automatic retry logic.
        
        Returns:
            Configured requests Session object
        """
        session = requests.Session()
        
        # Configure retry strategy
        retry_strategy = Retry(
            total=3,  # Total number of retries
            status_forcelist=[429, 500, 502, 503, 504],  # HTTP status codes to retry
            allowed_methods=["GET"],  # Only retry GET requests
            backoff_factor=1  # Wait 1, 2, 4 seconds between retries
        )
        
        adapter = HTTPAdapter(max_retries=retry_strategy)
        session.mount("http://", adapter)
        session.mount("https://", adapter)
        
        # Set default timeout for all requests
        session.timeout = 30
        
        return session
    
    def fetch_with_retry(self, url: str, params: Dict = None, auth: Any = None,
                        max_retries: int = 3, timeout: int = 30) -> Optional[requests.Response]:
        """
        Fetch data from URL with retry logic and error handling.
        
        Args:
            url: The URL to fetch
            params: Query parameters
            auth: Authentication object (HTTPBasicAuth)
            max_retries: Maximum number of retry attempts
            timeout: Request timeout in seconds
            
        Returns:
            Response object if successful, None otherwise
        """
        # Use provided auth or create default
        if auth is None:
            auth = HTTPBasicAuth(self.api_key, '')
        
        for attempt in range(max_retries):
            try:
                logger.info(f"Fetching {url} (attempt {attempt + 1}/{max_retries})")
                
                response = self.session.get(
                    url,
                    params=params,
                    auth=auth,
                    timeout=timeout
                )
                
                # Check for successful response
                if response.status_code == 200:
                    logger.info(f"Successfully fetched data from {url}")
                    return response
                
                # Handle specific error codes
                elif response.status_code == 401:
                    logger.error("Authentication failed. Check your API key.")
                    return None
                
                elif response.status_code == 404:
                    logger.error(f"Resource not found: {url}")
                    return None
                
                elif response.status_code == 429:
                    # Rate limited - wait before retry
                    wait_time = 2 ** attempt  # Exponential backoff
                    logger.warning(f"Rate limited. Waiting {wait_time} seconds...")
                    time.sleep(wait_time)
                
                else:
                    logger.warning(f"Received status code {response.status_code}")
                    
            except requests.exceptions.Timeout:
                logger.error(f"Request timed out (attempt {attempt + 1})")
                
            except requests.exceptions.ConnectionError:
                logger.error(f"Connection error (attempt {attempt + 1})")
                
            except Exception as e:
                logger.error(f"Unexpected error: {str(e)}")
            
            # Wait before next retry
            if attempt < max_retries - 1:
                time.sleep(2 ** attempt)
        
        logger.error(f"Failed to fetch {url} after {max_retries} attempts")
        return None
    
    def parse_usda_response(self, response_data: Any, report_type: str = 'grain') -> List[Dict]:
        """
        Parse USDA API response and standardize the data structure.
        
        Args:
            response_data: JSON response from USDA API
            report_type: Type of report being parsed (grain, ethanol, etc.)
            
        Returns:
            List of standardized data records
        """
        parsed_records = []
        
        try:
            # Handle different response formats
            if isinstance(response_data, dict):
                # Check if response contains results array
                if 'results' in response_data:
                    data_items = response_data['results']
                elif 'data' in response_data:
                    data_items = response_data['data']
                else:
                    data_items = [response_data]
            elif isinstance(response_data, list):
                data_items = response_data
            else:
                logger.error(f"Unexpected response format: {type(response_data)}")
                return []
            
            # Parse each data item
            for item in data_items:
                parsed_record = self._parse_single_record(item, report_type)
                if parsed_record:
                    parsed_records.append(parsed_record)
            
            logger.info(f"Parsed {len(parsed_records)} records from response")
            
        except Exception as e:
            logger.error(f"Error parsing USDA response: {str(e)}")
            logger.debug(f"Response data: {json.dumps(response_data, indent=2)}")
        
        return parsed_records
    
    def _parse_single_record(self, item: Dict, report_type: str) -> Optional[Dict]:
        """
        Parse a single record from USDA response.
        Helper function for parse_usda_response.
        
        Args:
            item: Single data item from response
            report_type: Type of report being parsed
            
        Returns:
            Standardized record dictionary
        """
        try:
            # Common fields across different report types
            record = {
                'source': 'USDA_AMS',
                'report_type': report_type,
                'fetch_timestamp': datetime.now().isoformat()
            }
            
            if report_type == 'grain':
                # Parse grain price data
                record.update({
                    'commodity': item.get('commodity_name', '').lower(),
                    'location': item.get('location', item.get('market_location', '')),
                    'price_type': item.get('price_type', 'spot'),
                    'report_date': item.get('report_date', item.get('published_date', '')),
                    'price_low': self._parse_price(item.get('price_low', item.get('low_price'))),
                    'price_high': self._parse_price(item.get('price_high', item.get('high_price'))),
                    'price_avg': self._parse_price(item.get('price_avg', item.get('average_price'))),
                    'basis_low': self._parse_price(item.get('basis_low')),
                    'basis_high': self._parse_price(item.get('basis_high')),
                    'unit': item.get('unit', '$/bushel'),
                    'delivery_period': item.get('delivery_period', 'spot'),
                    'grade': item.get('grade', '#2 Yellow'),
                    'additional_info': item.get('comments', '')
                })
                
            elif report_type == 'ethanol':
                # Parse ethanol report data
                record.update({
                    'commodity': item.get('commodity', 'ethanol'),
                    'location': item.get('plant_location', item.get('location', '')),
                    'report_date': item.get('report_date', ''),
                    'price': self._parse_price(item.get('price')),
                    'price_low': self._parse_price(item.get('low')),
                    'price_high': self._parse_price(item.get('high')),
                    'unit': item.get('unit', '$/gallon'),
                    'product_type': item.get('product_type', 'ethanol'),
                    'fob_type': item.get('fob_type', '')
                })
                
            else:
                # Generic parsing for other report types
                record.update({
                    'commodity': item.get('commodity', item.get('commodity_name', '')),
                    'location': item.get('location', ''),
                    'report_date': item.get('report_date', item.get('date', '')),
                    'value': item.get('value', item.get('price', '')),
                    'unit': item.get('unit', ''),
                    'raw_data': item  # Store raw data for debugging
                })
            
            # Clean up empty values
            record = {k: v for k, v in record.items() if v not in ['', None]}
            
            return record
            
        except Exception as e:
            logger.error(f"Error parsing single record: {str(e)}")
            logger.debug(f"Item data: {json.dumps(item, indent=2)}")
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
            # Handle string prices with currency symbols or commas
            if isinstance(price_value, str):
                price_value = price_value.replace('$', '').replace(',', '').strip()
            
            return float(price_value)
            
        except (ValueError, TypeError):
            logger.warning(f"Could not parse price value: {price_value}")
            return None
    
    def collect_daily_prices(self, report_date: str = None) -> List[Dict]:
        """
        Main collection method - collects daily prices from all configured reports.
        
        Args:
            report_date: Date in MM/DD/YYYY format (default: today)
            
        Returns:
            List of all collected and standardized data records
        """
        if report_date is None:
            report_date = self.today
        
        # Authenticate with API key
        auth = HTTPBasicAuth(self.api_key, '')
        
        collected_data = []
        
        for report in self.report_configs:
            logger.info(f"Collecting {report['name']} (ID: {report['id']})")
            
            # Build query with date filters
            params = {
                'q': f'report_begin_date={report_date}',
                'allSections': 'true'
            }
            
            # Fetch with automatic retry
            response = self.fetch_with_retry(
                f'{self.base_url}/reports/{report["id"]}',
                params=params,
                auth=auth
            )
            
            if response:
                try:
                    # Parse and standardize
                    standardized = self.parse_usda_response(
                        response.json(),
                        report_type=report.get('type', 'generic')
                    )
                    collected_data.extend(standardized)
                    
                    # Save individual report data
                    self._save_report_data(standardized, report['name'], report_date)
                    
                except json.JSONDecodeError as e:
                    logger.error(f"Failed to parse JSON for {report['name']}: {e}")
            else:
                logger.error(f"Failed to fetch {report['name']}")
        
        logger.info(f"Total records collected: {len(collected_data)}")
        
        # Save combined data
        self._save_combined_data(collected_data, report_date)
        
        return collected_data
    
    def _save_report_data(self, data: List[Dict], report_name: str, report_date: str):
        """
        Save individual report data to file.
        
        Args:
            data: Parsed data records
            report_name: Name of the report
            report_date: Date of the report
        """
        if not data:
            return
        
        # Create date-based subdirectory
        date_str = datetime.strptime(report_date, '%m/%d/%Y').strftime('%Y%m%d')
        date_dir = self.output_dir / date_str
        date_dir.mkdir(parents=True, exist_ok=True)
        
        # Create safe filename
        safe_name = report_name.replace(' ', '_').replace('/', '_')
        
        # Save as JSON
        json_file = date_dir / f"{safe_name}_{date_str}.json"
        with open(json_file, 'w') as f:
            json.dump(data, f, indent=2, default=str)
        logger.info(f"Saved {len(data)} records to {json_file}")
        
        # Save as CSV if pandas is available
        try:
            df = pd.DataFrame(data)
            csv_file = date_dir / f"{safe_name}_{date_str}.csv"
            df.to_csv(csv_file, index=False)
            logger.info(f"Saved CSV to {csv_file}")
        except Exception as e:
            logger.warning(f"Could not save CSV: {e}")
    
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
        
        # Save combined JSON
        combined_file = self.output_dir / f"combined_{date_str}.json"
        with open(combined_file, 'w') as f:
            json.dump(data, f, indent=2, default=str)
        
        logger.info(f"Saved combined data to {combined_file}")
    
    def get_available_reports(self) -> List[Dict]:
        """
        Get list of all available reports from USDA AMS.
        Useful for discovering new report IDs.
        
        Returns:
            List of available reports with their IDs and names
        """
        url = f"{self.base_url}/reports"
        response = self.fetch_with_retry(url)
        
        if response:
            try:
                reports = response.json()
                logger.info(f"Found {len(reports)} available reports")
                return reports
            except json.JSONDecodeError:
                logger.error("Failed to parse reports list")
                return []
        
        return []
    
    def add_report(self, report_id: str, report_name: str, report_type: str = 'generic'):
        """
        Dynamically add a report configuration at runtime.
        
        Args:
            report_id: USDA report ID
            report_name: Descriptive name for the report
            report_type: Type of report for parsing (grain, ethanol, generic)
        """
        new_config = {
            'id': str(report_id),
            'name': report_name,
            'type': report_type
        }
        self.report_configs.append(new_config)
        logger.info(f"Added report configuration: {report_name} (ID: {report_id})")
    
    def list_configured_reports(self) -> List[Dict]:
        """
        Return the currently configured reports.
        
        Returns:
            List of configured report dictionaries
        """
        return self.report_configs.copy()


# Example usage and testing
def main():
    """
    Example of how to use the USDACollector class.
    API key is loaded automatically from .env file.
    """
    # Initialize the collector (API key loads from .env)
    collector = USDACollector(output_dir='./usda_data')
    
    # Check if API key was loaded
    if not collector.api_key:
        print("ERROR: No API key found!")
        print("Please ensure USDA_AMS_API_KEY is set in your .env file")
        return
    
    # Show configured reports
    print("Configured reports:")
    for report in collector.list_configured_reports():
        print(f"  - {report['name']} (ID: {report['id']}, Type: {report.get('type', 'generic')})")
    
    # Collect today's data
    print("\nCollecting today's data...")
    today_data = collector.collect_daily_prices()
    
    print(f"\nCollected {len(today_data)} total records")
    
    # Show sample record if any data was collected
    if today_data:
        print("\nSample record:")
        print(json.dumps(today_data[0], indent=2, default=str))
    
    # Get list of available reports (optional)
    print("\nFetching available reports...")
    reports = collector.get_available_reports()
    if reports:
        print(f"Found {len(reports)} reports available")
        # Show first 5 as example
        for report in reports[:5]:
            if isinstance(report, dict):
                print(f"  - {report.get('slug_id', 'N/A')}: {report.get('report_title', 'N/A')}")


if __name__ == "__main__":
    print("USDA AMS Data Collector - Round Lakes Commodities")
    print("=" * 50)
    print("\nThis script includes:")
    print("  ✓ Automatic API key loading from .env file")
    print("  ✓ Excel-based report configuration")
    print("  ✓ fetch_with_retry() - Automatic retry with exponential backoff")
    print("  ✓ parse_usda_response() - Intelligent response parsing")
    print("  ✓ collect_daily_prices() - Main collection method")
    print("\nEnsure your .env file contains USDA_AMS_API_KEY")
    print()
    
    main()