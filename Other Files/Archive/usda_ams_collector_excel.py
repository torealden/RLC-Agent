import pandas as pd
from pathlib import Path
import logging

# ... [other imports remain unchanged] ...

class USDACollector:
    """
    Specialized agent for USDA AMS Market News API.
    Handles authentication, rate limiting, and data extraction.
    """
    def __init__(self, api_key: str, output_dir: str = './data', config_path: str = 'report_config.xlsx'):
        """
        Initialize the USDA collector.
        
        Args:
            api_key: Your USDA AMS API key.
            output_dir: Directory to save collected data.
            config_path: Path to the Excel file containing report configurations.
        """
        self.api_key = api_key
        self.base_url = 'https://marsapi.ams.usda.gov/services/v1.2'
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.today = datetime.now().strftime('%m/%d/%Y')
        self.session = self._create_session_with_retry()
        
        # Load report configurations from Excel instead of hardcoding
        self.report_configs = []
        try:
            df_config = pd.read_excel(config_path)
            # Rename columns if needed to match expected keys
            if 'report_id' in df_config.columns and 'id' not in df_config.columns:
                df_config = df_config.rename(columns={'report_id': 'id'})
            if 'report_name' in df_config.columns and 'name' not in df_config.columns:
                df_config = df_config.rename(columns={'report_name': 'name'})
            # Ensure IDs are strings (if numeric IDs were read)
            df_config['id'] = df_config['id'].astype(str)
            # Convert DataFrame to list of dicts
            self.report_configs = df_config.to_dict(orient='records')
            logging.info(f"Loaded {len(self.report_configs)} report configurations from {config_path}")
        except Exception as e:
            logging.error(f"Failed to load report configurations from {config_path}: {e}")
            # If loading fails, proceed with an empty list (no reports)
            self.report_configs = []
    
    # ... [other methods like _create_session_with_retry, fetch_with_retry, etc.] ...
    
    def collect_daily_prices(self, report_date: str = None) -> List[Dict]:
        """
        Collect daily prices from all configured reports for the given date.
        """
        if report_date is None:
            report_date = self.today
        auth = HTTPBasicAuth(self.api_key, '')
        collected_data = []
        for report in self.report_configs:
            logger.info(f"Collecting {report['name']} (ID: {report['id']})")
            params = {
                'q': f'report_begin_date={report_date}',
                'allSections': 'true'
            }
            response = self.fetch_with_retry(f"{self.base_url}/reports/{report['id']}", params=params, auth=auth)
            if not response:
                logger.error(f"Failed to fetch report ID {report['id']} – {report['name']}")
                continue
            try:
                # Parse and standardize the response based on report type
                data = self.parse_usda_response(response.json(), report_type=report.get('type'))
                collected_data.extend(data)
                # Save individual report data to file
                self._save_report_data(data, report['name'], report_date)
            except json.JSONDecodeError as e:
                logger.error(f"JSON parsing error for {report['name']} (ID {report['id']}): {e}")
        logger.info(f"Total records collected for {report_date}: {len(collected_data)}")
        # Save combined data from all reports for the date
        self._save_combined_data(collected_data, report_date)
        return collected_data

    # ... [remaining methods unchanged: parse_usda_response, _save_report_data, etc.] ...
