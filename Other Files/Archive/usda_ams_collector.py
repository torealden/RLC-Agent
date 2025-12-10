class USDACollector:
    """
    Specialized agent for USDA AMS Market News API
    Handles authentication, rate limiting, and data extraction
    """
    
    def collect_daily_prices(self):
        # Authenticate with API key
        auth = HTTPBasicAuth(self.api_key, '')
        
        collected_data = []
        for report in self.report_configs:
            # Build query with date filters
            params = {
                'q': f'report_begin_date={self.today}',
                'allSections': 'true'
            }
            
            # Fetch with automatic retry
            response = self.fetch_with_retry(
                f'{self.base_url}/reports/{report["id"]}',
                params=params,
                auth=auth
            )
            
            # Parse and standardize
            standardized = self.parse_usda_response(response.json())
            collected_data.extend(standardized)
        
        return collected_data