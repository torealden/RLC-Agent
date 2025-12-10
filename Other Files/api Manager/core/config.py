"""
Configuration management for the data pipeline
Now reads from Excel configuration files for dynamic commodity/country setup
"""
import os
from pathlib import Path
from typing import Dict, Any, Optional, List
from dotenv import load_dotenv
import json
import pandas as pd
from pydantic import BaseModel, Field
import logging

logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()

class DatabaseConfig(BaseModel):
    """Database configuration with support for SQLite and PostgreSQL"""
    db_type: str = Field(default_factory=lambda: os.getenv('DB_TYPE', 'sqlite'))
    
    # SQLite settings
    db_path: str = Field(default_factory=lambda: os.getenv('DB_PATH', 'data/local_database.db'))
    
    # PostgreSQL settings
    host: Optional[str] = Field(default_factory=lambda: os.getenv('DB_HOST'))
    port: Optional[int] = Field(default_factory=lambda: int(os.getenv('DB_PORT', '5432')) if os.getenv('DB_PORT') else None)
    database: Optional[str] = Field(default_factory=lambda: os.getenv('DB_NAME'))
    username: Optional[str] = Field(default_factory=lambda: os.getenv('DB_USER'))
    password: Optional[str] = Field(default_factory=lambda: os.getenv('DB_PASSWORD'))
    
    @property
    def connection_string(self) -> str:
        """Generate connection string based on database type"""
        if self.db_type == 'sqlite':
            # Ensure directory exists
            db_path = Path(self.db_path)
            db_path.parent.mkdir(parents=True, exist_ok=True)
            return f"sqlite:///{self.db_path}"
        elif self.db_type == 'postgresql':
            return f"postgresql://{self.username}:{self.password}@{self.host}:{self.port}/{self.database}"
        else:
            raise ValueError(f"Unsupported database type: {self.db_type}")

class NotificationConfig(BaseModel):
    email: str = Field(default_factory=lambda: os.getenv('ALERT_EMAIL', ''))
    smtp_server: str = Field(default_factory=lambda: os.getenv('SMTP_SERVER', 'smtp.gmail.com'))
    smtp_port: int = Field(default_factory=lambda: int(os.getenv('SMTP_PORT', '587')))
    smtp_username: str = Field(default_factory=lambda: os.getenv('SMTP_USERNAME', ''))
    smtp_password: str = Field(default_factory=lambda: os.getenv('SMTP_PASSWORD', ''))

class CommodityConfig:
    """
    Manages commodity configuration from Excel file
    """
    def __init__(self, excel_path: str = None):
        self.excel_path = excel_path or os.getenv('COMMODITIES_CONFIG_PATH', 'config/commodities_config.xlsx')
        self.commodities_df: Optional[pd.DataFrame] = None
        self.trade_countries_df: Optional[pd.DataFrame] = None
        self.price_locations_df: Optional[pd.DataFrame] = None
        self.validation_rules_df: Optional[pd.DataFrame] = None
        
        self._load_config()
    
    def _load_config(self):
        """Load configuration from Excel file"""
        try:
            excel_file = pd.ExcelFile(self.excel_path)
            
            # Load each sheet
            if 'Commodities' in excel_file.sheet_names:
                self.commodities_df = pd.read_excel(excel_file, sheet_name='Commodities')
                self.commodities_df = self.commodities_df[self.commodities_df['active'] == True]
            
            if 'Trade_Countries' in excel_file.sheet_names:
                self.trade_countries_df = pd.read_excel(excel_file, sheet_name='Trade_Countries')
                self.trade_countries_df = self.trade_countries_df[self.trade_countries_df['active'] == True]
            
            if 'Price_Locations' in excel_file.sheet_names:
                self.price_locations_df = pd.read_excel(excel_file, sheet_name='Price_Locations')
                self.price_locations_df = self.price_locations_df[self.price_locations_df['active'] == True]
            
            if 'Data_Validation_Rules' in excel_file.sheet_names:
                self.validation_rules_df = pd.read_excel(excel_file, sheet_name='Data_Validation_Rules')
            
            logger.info(f"Loaded commodity configuration from {self.excel_path}")
            
        except FileNotFoundError:
            logger.warning(f"Commodity config file not found: {self.excel_path}")
            logger.warning("Creating default configuration...")
            self._create_default_config()
        except Exception as e:
            logger.error(f"Error loading commodity config: {e}")
    
    def _create_default_config(self):
        """Create a default configuration Excel file"""
        # Create default DataFrames
        self.commodities_df = pd.DataFrame({
            'commodity_code': ['SOYBEAN', 'CORN', 'SOYOIL'],
            'commodity_name': ['Soybeans', 'Corn', 'Soybean Oil'],
            'category': ['agriculture', 'agriculture', 'agriculture'],
            'unit_preference': ['bushel', 'bushel', 'pound'],
            'metric_ton_conversion': [36.744, 39.368, 2204.62],
            'active': [True, True, True]
        })
        
        self.trade_countries_df = pd.DataFrame({
            'country_code': ['US', 'BR', 'AR'],
            'country_name': ['United States', 'Brazil', 'Argentina'],
            'region': ['North America', 'South America', 'South America'],
            'trade_data_source': ['census_bureau', 'mdic', 'indec'],
            'active': [True, True, True]
        })
        
        self.price_locations_df = pd.DataFrame({
            'commodity_code': ['SOYBEAN', 'SOYBEAN', 'SOYBEAN'],
            'location': ['Central Illinois', 'Decatur, IL', 'Chicago, IL'],
            'data_source': ['usda_ams', 'usda_ams', 'usda_ams'],
            'report_type': ['grain-cash-bids', 'grain-cash-bids', 'grain-cash-bids'],
            'active': [True, True, True]
        })
        
        self.validation_rules_df = pd.DataFrame({
            'rule_name': ['price_positive', 'volume_reasonable'],
            'commodity_code': ['ALL', 'SOYBEAN'],
            'validation_type': ['price_range', 'volume_range'],
            'min_value': [0, 0],
            'max_value': [10000, 1000000],
            'alert_threshold': [None, 10000]
        })
        
        # Save to Excel
        try:
            Path(self.excel_path).parent.mkdir(parents=True, exist_ok=True)
            with pd.ExcelWriter(self.excel_path, engine='openpyxl') as writer:
                self.commodities_df.to_excel(writer, sheet_name='Commodities', index=False)
                self.trade_countries_df.to_excel(writer, sheet_name='Trade_Countries', index=False)
                self.price_locations_df.to_excel(writer, sheet_name='Price_Locations', index=False)
                self.validation_rules_df.to_excel(writer, sheet_name='Data_Validation_Rules', index=False)
            logger.info(f"Created default commodity config at {self.excel_path}")
        except Exception as e:
            logger.error(f"Failed to create default config file: {e}")
    
    def get_active_commodities(self) -> List[Dict[str, Any]]:
        """Get list of active commodities"""
        if self.commodities_df is None or self.commodities_df.empty:
            return []
        return self.commodities_df.to_dict('records')
    
    def get_commodity_info(self, commodity_code: str) -> Optional[Dict[str, Any]]:
        """Get information for a specific commodity"""
        if self.commodities_df is None:
            return None
        result = self.commodities_df[self.commodities_df['commodity_code'] == commodity_code]
        if result.empty:
            return None
        return result.iloc[0].to_dict()
    
    def get_price_locations(self, commodity_code: str = None) -> List[Dict[str, Any]]:
        """Get price locations, optionally filtered by commodity"""
        if self.price_locations_df is None or self.price_locations_df.empty:
            return []
        
        df = self.price_locations_df
        if commodity_code:
            df = df[df['commodity_code'] == commodity_code]
        
        return df.to_dict('records')
    
    def get_trade_countries(self) -> List[Dict[str, Any]]:
        """Get list of countries for trade data"""
        if self.trade_countries_df is None or self.trade_countries_df.empty:
            return []
        return self.trade_countries_df.to_dict('records')
    
    def get_validation_rules(self, commodity_code: str = None) -> List[Dict[str, Any]]:
        """Get validation rules, optionally filtered by commodity"""
        if self.validation_rules_df is None or self.validation_rules_df.empty:
            return []
        
        df = self.validation_rules_df
        if commodity_code:
            df = df[(df['commodity_code'] == commodity_code) | (df['commodity_code'] == 'ALL')]
        
        return df.to_dict('records')
    
    def reload(self):
        """Reload configuration from Excel file"""
        self._load_config()

class AppConfig:
    def __init__(self, config_file: str = "config/api_sources.json"):
        self.config_file = config_file
        self.db_config = DatabaseConfig()
        self.notification_config = NotificationConfig()
        self.commodity_config = CommodityConfig()
        self.api_configs = self._load_api_configs()
        
    def _load_api_configs(self) -> Dict[str, Any]:
        """Load API source configurations from JSON file"""
        config_path = Path(self.config_file)
        if config_path.exists():
            with open(config_path, 'r') as f:
                return json.load(f)
        return {}
    
    def get_api_config(self, api_name: str) -> Optional[Dict[str, Any]]:
        """Get configuration for a specific API source"""
        return self.api_configs.get(api_name)
    
    def is_enabled(self, api_name: str) -> bool:
        """Check if an API source is enabled"""
        config = self.get_api_config(api_name)
        return config.get('enabled', False) if config else False