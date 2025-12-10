from abc import ABC, abstractmethod
from typing import Dict, Any, Optional
import pandas as pd

class BaseAPISource(ABC):
    def __init__(self, config: Dict[str, Any], db_config: Dict[str, str]):
        self.config = config
        self.db_config = db_config
        self.name = self.config.get('name', self.__class__.__name__)
        
    @abstractmethod
    def authenticate(self) -> bool:
        """Handle authentication for the API source"""
        pass
    
    @abstractmethod
    def fetch_data(self) -> Dict[str, pd.DataFrame]:
        """Fetch and return data from the API source"""
        pass
    
    @abstractmethod
    def validate_data(self, data: Dict[str, pd.DataFrame]) -> bool:
        """Validate the fetched data"""
        pass
    
    @abstractmethod
    def transform_data(self, data: Dict[str, pd.DataFrame]) -> Dict[str, pd.DataFrame]:
        """Transform data into standardized format"""
        pass
    
    def update_database(self, data: Dict[str, pd.DataFrame]) -> bool:
        """Standard database update method (can be overridden)"""
        # Common database update logic here
        pass
    
    def run(self) -> bool:
        """Standard execution flow"""
        try:
            if not self.authenticate():
                raise Exception(f"Authentication failed for {self.name}")
            
            raw_data = self.fetch_data()
            
            if not self.validate_data(raw_data):
                raise Exception(f"Data validation failed for {self.name}")
            
            transformed_data = self.transform_data(raw_data)
            
            return self.update_database(transformed_data)
            
        except Exception as e:
            self.handle_error(e)
            return False
    
    def handle_error(self, error: Exception):
        """Standard error handling"""
        print(f"Error in {self.name}: {str(error)}")