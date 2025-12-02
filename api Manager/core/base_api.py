"""
Base class for all API data sources
"""
from abc import ABC, abstractmethod
from typing import Dict, Any, Optional, List
import pandas as pd
import requests
from datetime import datetime, timedelta
import time
import logging
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

logger = logging.getLogger(__name__)

class BaseAPISource(ABC):
    """
    Abstract base class that all API sources must inherit from
    """
    
    def __init__(self, config: Dict[str, Any], db_manager):
        """
        Initialize the API source
        
        Args:
            config: Configuration dictionary for this API source
            db_manager: DatabaseManager instance
        """
        self.config = config
        self.db_manager = db_manager
        self.name = config.get('name', self.__class__.__name__)
        self.enabled = config.get('enabled', True)
        
        # Rate limiting
        self.rate_limit = config.get('rate_limit_per_minute', 60)
        self.last_request_time = None
        
        # Retry configuration
        self.retry_attempts = config.get('retry_attempts', 3)
        self.timeout = config.get('timeout', 30)
        
        # Session with retry logic
        self.session = self._create_session()
        
        # Tracking
        self.last_run = None
        self.last_success = None
        self.consecutive_failures = 0
        
    def _create_session(self) -> requests.Session:
        """Create a requests session with retry logic"""
        session = requests.Session()
        retry_strategy = Retry(
            total=self.retry_attempts,
            backoff_factor=1,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["HEAD", "GET", "POST", "PUT", "DELETE", "OPTIONS", "TRACE"]
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)
        session.mount("http://", adapter)
        session.mount("https://", adapter)
        return session
    
    def _respect_rate_limit(self):
        """Enforce rate limiting between requests"""
        if self.last_request_time and self.rate_limit:
            min_interval = 60.0 / self.rate_limit  # seconds between requests
            elapsed = time.time() - self.last_request_time
            if elapsed < min_interval:
                time.sleep(min_interval - elapsed)
        self.last_request_time = time.time()
    
    @abstractmethod
    def authenticate(self) -> bool:
        """
        Handle authentication for the API source
        
        Returns:
            bool: True if authentication successful, False otherwise
        """
        pass
    
    @abstractmethod
    def fetch_data(self) -> Dict[str, pd.DataFrame]:
        """
        Fetch data from the API source
        
        Returns:
            Dict[str, pd.DataFrame]: Dictionary of DataFrames keyed by table name
        """
        pass
    
    @abstractmethod
    def validate_data(self, data: Dict[str, pd.DataFrame]) -> bool:
        """
        Validate the fetched data
        
        Args:
            data: Dictionary of DataFrames to validate
            
        Returns:
            bool: True if data is valid, False otherwise
        """
        pass
    
    @abstractmethod
    def transform_data(self, data: Dict[str, pd.DataFrame]) -> Dict[str, pd.DataFrame]:
        """
        Transform data into standardized format for database
        
        Args:
            data: Raw data from API
            
        Returns:
            Dict[str, pd.DataFrame]: Transformed data ready for database
        """
        pass
    
    def save_data(self, data: Dict[str, pd.DataFrame]) -> bool:
        """
        Save data to database
        
        Args:
            data: Dictionary of DataFrames to save, keyed by table name
            
        Returns:
            bool: True if save successful, False otherwise
        """
        try:
            for table_name, df in data.items():
                if df.empty:
                    logger.warning(f"{self.name}: Empty DataFrame for table '{table_name}'")
                    continue
                
                # Determine unique columns for upsert (default to 'date' if present)
                unique_cols = ['date'] if 'date' in df.columns else list(df.columns[:2])
                
                success = self.db_manager.upsert_dataframe(
                    df, 
                    table_name, 
                    unique_columns=unique_cols
                )
                
                if not success:
                    return False
                    
            return True
            
        except Exception as e:
            logger.error(f"{self.name}: Failed to save data: {e}")
            return False
    
    def run(self) -> bool:
        """
        Execute the full data pipeline for this source
        
        Returns:
            bool: True if successful, False otherwise
        """
        self.last_run = datetime.now()
        
        try:
            logger.info(f"Starting data pull for {self.name}")
            
            # Step 1: Authenticate
            if not self.authenticate():
                raise Exception(f"Authentication failed for {self.name}")
            
            # Step 2: Fetch raw data
            raw_data = self.fetch_data()
            
            if not raw_data:
                raise Exception(f"No data fetched for {self.name}")
            
            # Step 3: Validate data
            if not self.validate_data(raw_data):
                raise Exception(f"Data validation failed for {self.name}")
            
            # Step 4: Transform data
            transformed_data = self.transform_data(raw_data)
            
            # Step 5: Save to database
            if not self.save_data(transformed_data):
                raise Exception(f"Failed to save data for {self.name}")
            
            # Success!
            self.last_success = datetime.now()
            self.consecutive_failures = 0
            logger.info(f"Successfully completed data pull for {self.name}")
            return True
            
        except Exception as e:
            self.consecutive_failures += 1
            logger.error(f"{self.name} failed (attempt {self.consecutive_failures}): {str(e)}")
            self.handle_error(e)
            return False
    
    def handle_error(self, error: Exception):
        """
        Handle errors during execution
        
        Args:
            error: The exception that occurred
        """
        # Log the error
        logger.error(f"{self.name} error: {str(error)}", exc_info=True)
        
        # Could send notifications here if consecutive_failures exceeds threshold
        if self.consecutive_failures >= 3:
            logger.critical(f"{self.name} has failed {self.consecutive_failures} times consecutively!")
            # TODO: Send email/Slack notification
    
    def get_status(self) -> Dict[str, Any]:
        """Get current status of this API source"""
        return {
            'name': self.name,
            'enabled': self.enabled,
            'last_run': self.last_run,
            'last_success': self.last_success,
            'consecutive_failures': self.consecutive_failures,
            'is_healthy': self.consecutive_failures < 3
        }