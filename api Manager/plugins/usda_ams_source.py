"""
USDA Agricultural Marketing Service (AMS) Market News API Plugin
Updated to read commodities and locations from Excel configuration
"""
import os
from typing import Dict, Any, List, Optional
import pandas as pd
import requests
from datetime import datetime, timedelta
import logging
import json
from core.base_api import BaseAPISource

logger = logging.getLogger(__name__)

class USDAMSSource(BaseAPISource):
    """
    USDA AMS Market News data source
    Dynamically configured via Excel file
    """
    
    def __init__(self, config: Dict[str, Any], db_manager, commodity_config):
        super().__init__(config, db_manager)
        
        # Commodity configuration
        self.commodity_config = commodity_config
        
        # API credentials
        self.api_key = os.getenv('USDA_AMS_API_KEY')
        self.username = os.getenv('USDA_AMS_USERNAME')
        
        # API endpoints
        self.base_url = config.get('base_url', 'https://marsapi.ams.usda.gov/services/v1.2/reports')
        
        # Date range for data pull
        self.lookback_days = config.get('lookback_days', 7)
        
    def authenticate(self) -> bool:
        """Authenticate with USDA AMS API"""
        try:
            if not self.api_key:
                logger.error("USDA AMS API key not found in environment variables")
                return False
            
            self.session.headers.update({
                'Accept': 'application/json',
                'API_KEY': self.api_key
            })
            
            # Test authentication
            test_url = f"{self.base_url}/search"
            response = self.session.get(
                test_url,
                params={'q': 'soybeans', 'pagesize': 1},
                timeout=self.timeout
            )
            
            if response.status_code == 200:
                logger.info("USDA AMS authentication successful")
                return True
            else:
                logger.error(f"USDA AMS authentication failed: {response.status_code}")
                return False
                
        except Exception as e:
            logger.error(f"USDA AMS authentication error: {e}")
            return False
    
    def fetch_data(self) -> Dict[str, pd.DataFrame]:
        """
        Fetch data from USDA AMS based on Excel configuration
        """
        try:
            end_date = datetime.now()
            start_date = end_date - timedelta(days=self.lookback_days)
            
            all_prices = []
            all_reports = []
            
            # Get price locations from Excel config
            price_locations = self.commodity_config.get_price_locations()
            
            # Group by commodity and data source
            for location_config in price_locations:
                if location_config['data_source'] != 'usda_ams':
                    continue
                
                commodity_code = location_config['commodity_code']
                location = location_config['location']
                report_type = location_config['report_type']
                
                logger.info(f"Fetching {commodity_code} data for {location}...")
                
                # Get commodity info for search query
                commodity_info = self.commodity_config.get_commodity_info(commodity_code)
                if not commodity_info:
                    logger.warning(f"Commodity info not found for {commodity_code}")
                    continue
                
                search_term = commodity_info['commodity_name']
                
                # Search for reports
                reports = self.search_reports(
                    search_term, 
                    report_type, 
                    start_date, 
                    end_date
                )
                
                # Process each report
                for report_meta in reports:
                    slug_id = report_meta.get('slug_id')
                    
                    # Fetch full report
                    report = self.fetch_report(slug_id)
                    if not report:
                        continue
                    
                    # Parse report
                    prices_df = self.parse_report(report, commodity_code)
                    
                    # Filter by location
                    if not prices_df.empty:
                        prices_df = prices_df[prices_df['location'].str.contains(location, case=False, na=False)]
                        
                        if not prices_df.empty:
                            all_prices.append(prices_df)
                            
                            # Store report metadata
                            report_record = {
                                'report_id': slug_id,
                                'slug_id': slug_id,
                                'report_title': report.get('report_title', ''),
                                'report_date': prices_df['report_date'].iloc[0] if not prices_df.empty else None,
                                'publish_date': datetime.now(),
                                'office': report.get('office', ''),
                                'report_type': report_type,
                                'raw_data': json.dumps(report),
                                'fetched_at': datetime.now()
                            }
                            all_reports.append(report_record)
            
            # Combine all data
            prices_df = pd.concat(all_prices, ignore_index=True) if all_prices else pd.DataFrame()
            reports_df = pd.DataFrame(all_reports) if all_reports else pd.DataFrame()
            
            return {
                'usda_ams_prices': prices_df,
                'usda_ams_reports': reports_df
            }
            
        except Exception as e:
            logger.error(f"Error fetching USDA AMS data: {e}")
            return {}
    
    def search_reports(self, commodity: str, report_type: str, 
                       start_date: datetime, end_date: datetime) -> List[Dict]:
        """Search for reports"""
        try:
            self._respect_rate_limit()
            
            search_url = f"{self.base_url}/search"
            params = {
                'q': commodity,
                'reportType': report_type,
                'startDate': start_date.strftime('%Y-%m-%d'),
                'endDate': end_date.strftime('%Y-%m-%d'),
                'pagesize': 100
            }
            
            response = self.session.get(search_url, params=params, timeout=self.timeout)
            response.raise_for_status()
            
            data = response.json()
            reports = data.get('results', [])
            
            logger.info(f"Found {len(reports)} {commodity} reports")
            return reports
            
        except Exception as e:
            logger.error(f"Error searching reports: {e}")
            return []
    
    def fetch_report(self, slug_id: str) -> Optional[Dict]:
        """Fetch specific report"""
        try:
            self._respect_rate_limit()
            report_url = f"{self.base_url}/{slug_id}"
            response = self.session.get(report_url, timeout=self.timeout)
            response.raise_for_status()
            return response.json()
        except Exception as e:
            logger.error(f"Error fetching report {slug_id}: {e}")
            return None
    
    def parse_report(self, report: Dict, commodity_code: str) -> pd.DataFrame:
        """Parse report into DataFrame"""
        try:
            records = []
            
            report_date = datetime.strptime(
                report.get('published_date', ''), 
                '%Y-%m-%dT%H:%M:%S'
            ).date()
            
            report_id = report.get('slug_id', '')
            
            # Parse report sections
            # NOTE: This structure will vary based on actual USDA API response
            # You'll need to adjust this based on real data
            sections = report.get('results', [])
            for section in sections:
                location = section.get('location', 'Unknown')
                prices = section.get('data', {})
                
                record = {
                    'report_date': report_date,
                    'report_id': report_id,
                    'commodity': commodity_code,
                    'location': location,
                    'price': prices.get('price'),
                    'price_unit': prices.get('price_unit', 'USD/bu'),
                    'basis': prices.get('basis'),
                    'basis_month': prices.get('basis_month'),
                    'volume': prices.get('volume'),
                }
                records.append(record)
            
            return pd.DataFrame(records)
            
        except Exception as e:
            logger.error(f"Error parsing report: {e}")
            return pd.DataFrame()
    
    def validate_data(self, data: Dict[str, pd.DataFrame]) -> bool:
        """Validate data using rules from Excel config"""
        try:
            prices_df = data.get('usda_ams_prices', pd.DataFrame())
            
            if prices_df.empty:
                return True
            
            # Get validation rules from config
            for _, row in prices_df.iterrows():
                commodity_code = row['commodity']
                rules = self.commodity_config.get_validation_rules(commodity_code)
                
                for rule in rules:
                    if rule['validation_type'] == 'price_range':
                        if row['price'] < rule['min_value'] or row['price'] > rule['max_value']:
                            logger.error(f"Price validation failed for {commodity_code}: {row['price']}")
                            return False
            
            logger.info("Data validation passed")
            return True
            
        except Exception as e:
            logger.error(f"Validation error: {e}")
            return False
    
    def transform_data(self, data: Dict[str, pd.DataFrame]) -> Dict[str, pd.DataFrame]:
        """Transform data for database"""
        try:
            prices_df = data.get('usda_ams_prices', pd.DataFrame())
            
            if not prices_df.empty:
                prices_df['report_date'] = pd.to_datetime(prices_df['report_date'])
                prices_df['location'] = prices_df['location'].str.strip().str.title()
                prices_df['price'] = prices_df['price'].round(4)
                prices_df['updated_at'] = datetime.now()
                
                # Remove duplicates
                prices_df = prices_df.drop_duplicates(
                    subset=['report_date', 'report_id', 'commodity', 'location'],
                    keep='last'
                )
            
            return {
                'usda_ams_prices': prices_df,
                'usda_ams_reports': data.get('usda_ams_reports', pd.DataFrame())
            }
            
        except Exception as e:
            logger.error(f"Transformation error: {e}")
            return data