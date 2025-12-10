"""
Configuration Manager for Commodity Data Pipeline
Centralized settings management for database, API, and pipeline configuration
Round Lakes Commodities
"""

import os
from pathlib import Path
from typing import Dict, Any, Optional
from dataclasses import dataclass, field
from dotenv import load_dotenv

# Load environment variables
load_dotenv()


@dataclass
class DatabaseConfig:
    """Database connection configuration"""
    db_type: str = "sqlite"  # 'sqlite' or 'mysql' or 'postgresql'
    host: str = "localhost"
    port: int = 3306
    username: str = ""
    password: str = ""
    database: str = "commodities_db"
    sqlite_path: str = "./data/rlc_commodities.db"
    
    @classmethod
    def from_env(cls) -> 'DatabaseConfig':
        """Create DatabaseConfig from environment variables"""
        return cls(
            db_type=os.getenv('DB_TYPE', 'sqlite'),
            host=os.getenv('DB_HOST', 'localhost'),
            port=int(os.getenv('DB_PORT', '3306')),
            username=os.getenv('DB_USER', os.getenv('DB_USERNAME', '')),
            password=os.getenv('DB_PASSWORD', ''),
            database=os.getenv('DB_NAME', os.getenv('DB_DATABASE', 'commodities_db')),
            sqlite_path=os.getenv('SQLITE_PATH', './data/rlc_commodities.db')
        )
    
    def get_connection_string(self) -> str:
        """Generate database connection string"""
        if self.db_type == 'sqlite':
            return f"sqlite:///{self.sqlite_path}"
        elif self.db_type == 'mysql':
            return f"mysql+mysqlconnector://{self.username}:{self.password}@{self.host}:{self.port}/{self.database}"
        elif self.db_type == 'postgresql':
            return f"postgresql://{self.username}:{self.password}@{self.host}:{self.port}/{self.database}"
        else:
            raise ValueError(f"Unsupported database type: {self.db_type}")


@dataclass
class APIConfig:
    """API configuration for data sources"""
    usda_ams_api_key: str = ""
    usda_ams_base_url: str = "https://marsapi.ams.usda.gov/services/v1.2"
    request_timeout: int = 30
    max_retries: int = 3
    retry_backoff: float = 2.0
    
    @classmethod
    def from_env(cls) -> 'APIConfig':
        """Create APIConfig from environment variables"""
        return cls(
            usda_ams_api_key=os.getenv('USDA_AMS_API_KEY', ''),
            usda_ams_base_url=os.getenv('USDA_AMS_BASE_URL', 'https://marsapi.ams.usda.gov/services/v1.2'),
            request_timeout=int(os.getenv('REQUEST_TIMEOUT', '30')),
            max_retries=int(os.getenv('MAX_RETRIES', '3')),
            retry_backoff=float(os.getenv('RETRY_BACKOFF', '2.0'))
        )


@dataclass
class PipelineConfig:
    """Pipeline execution configuration"""
    output_dir: str = "./data"
    log_dir: str = "./logs"
    report_config_path: str = "report_config.xlsx"
    enable_file_output: bool = True
    enable_database_output: bool = True
    verification_enabled: bool = True
    verification_sample_size: int = 5
    historical_start_date: str = "01/01/2000"
    historical_end_date: str = "12/31/2025"
    
    @classmethod
    def from_env(cls) -> 'PipelineConfig':
        """Create PipelineConfig from environment variables"""
        return cls(
            output_dir=os.getenv('OUTPUT_DIR', './data'),
            log_dir=os.getenv('LOG_DIR', './logs'),
            report_config_path=os.getenv('REPORT_CONFIG_PATH', 'report_config.xlsx'),
            enable_file_output=os.getenv('ENABLE_FILE_OUTPUT', 'true').lower() == 'true',
            enable_database_output=os.getenv('ENABLE_DATABASE_OUTPUT', 'true').lower() == 'true',
            verification_enabled=os.getenv('VERIFICATION_ENABLED', 'true').lower() == 'true',
            verification_sample_size=int(os.getenv('VERIFICATION_SAMPLE_SIZE', '5')),
            historical_start_date=os.getenv('HISTORICAL_START_DATE', '01/01/2000'),
            historical_end_date=os.getenv('HISTORICAL_END_DATE', '12/31/2025')
        )


@dataclass 
class Settings:
    """Master configuration container"""
    database: DatabaseConfig = field(default_factory=DatabaseConfig.from_env)
    api: APIConfig = field(default_factory=APIConfig.from_env)
    pipeline: PipelineConfig = field(default_factory=PipelineConfig.from_env)
    
    def __post_init__(self):
        """Ensure directories exist"""
        Path(self.pipeline.output_dir).mkdir(parents=True, exist_ok=True)
        Path(self.pipeline.log_dir).mkdir(parents=True, exist_ok=True)
        if self.database.db_type == 'sqlite':
            Path(self.database.sqlite_path).parent.mkdir(parents=True, exist_ok=True)
    
    @classmethod
    def load(cls) -> 'Settings':
        """Load all settings from environment"""
        return cls()
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert settings to dictionary (for logging, excluding secrets)"""
        return {
            'database': {
                'db_type': self.database.db_type,
                'host': self.database.host,
                'port': self.database.port,
                'database': self.database.database,
                'sqlite_path': self.database.sqlite_path if self.database.db_type == 'sqlite' else None
            },
            'api': {
                'usda_ams_base_url': self.api.usda_ams_base_url,
                'request_timeout': self.api.request_timeout,
                'max_retries': self.api.max_retries,
                'has_api_key': bool(self.api.usda_ams_api_key)
            },
            'pipeline': {
                'output_dir': self.pipeline.output_dir,
                'enable_file_output': self.pipeline.enable_file_output,
                'enable_database_output': self.pipeline.enable_database_output,
                'verification_enabled': self.pipeline.verification_enabled
            }
        }


# Global settings instance
settings = Settings.load()


def get_settings() -> Settings:
    """Get the global settings instance"""
    return settings


def reload_settings() -> Settings:
    """Reload settings from environment"""
    global settings
    load_dotenv(override=True)
    settings = Settings.load()
    return settings