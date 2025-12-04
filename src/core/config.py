"""
Centralized Configuration Management for RLC Agent System.

Loads configuration from environment variables and config files.
Provides typed access to all system settings.
"""

import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, List, Optional, Any
from dotenv import load_dotenv
import json
import logging

logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()


@dataclass
class DatabaseConfig:
    """Database connection configuration"""
    db_type: str = "sqlite"
    sqlite_path: str = "./data/rlc_commodities.db"
    host: str = "localhost"
    port: int = 5432
    database: str = "commodities_db"
    username: str = ""
    password: str = ""

    @classmethod
    def from_env(cls) -> 'DatabaseConfig':
        return cls(
            db_type=os.getenv('DB_TYPE', 'sqlite'),
            sqlite_path=os.getenv('SQLITE_PATH', './data/rlc_commodities.db'),
            host=os.getenv('DB_HOST', 'localhost'),
            port=int(os.getenv('DB_PORT', '5432')),
            database=os.getenv('DB_NAME', 'commodities_db'),
            username=os.getenv('DB_USER', ''),
            password=os.getenv('DB_PASSWORD', '')
        )


@dataclass
class APIConfig:
    """API credentials configuration"""
    usda_ams_api_key: str = ""
    openai_api_key: str = ""
    anthropic_api_key: str = ""

    @classmethod
    def from_env(cls) -> 'APIConfig':
        return cls(
            usda_ams_api_key=os.getenv('USDA_AMS_API_KEY', ''),
            openai_api_key=os.getenv('OPENAI_API_KEY', ''),
            anthropic_api_key=os.getenv('ANTHROPIC_API_KEY', '')
        )


@dataclass
class SchedulerConfig:
    """Scheduler configuration"""
    enabled: bool = True
    check_interval_seconds: int = 60
    timezone: str = "America/Chicago"
    max_concurrent_tasks: int = 5
    retry_failed_tasks: bool = True
    max_retries: int = 3

    @classmethod
    def from_env(cls) -> 'SchedulerConfig':
        return cls(
            enabled=os.getenv('SCHEDULER_ENABLED', 'true').lower() == 'true',
            check_interval_seconds=int(os.getenv('SCHEDULER_INTERVAL', '60')),
            timezone=os.getenv('SCHEDULER_TIMEZONE', 'America/Chicago'),
            max_concurrent_tasks=int(os.getenv('MAX_CONCURRENT_TASKS', '5')),
            retry_failed_tasks=os.getenv('RETRY_FAILED_TASKS', 'true').lower() == 'true',
            max_retries=int(os.getenv('MAX_RETRIES', '3'))
        )


@dataclass
class DataSourceSchedule:
    """Schedule for a data source"""
    source_id: str
    source_name: str
    frequency: str  # 'daily', 'weekly', 'monthly'
    day_of_week: Optional[str] = None  # For weekly
    day_of_month: Optional[int] = None  # For monthly
    time: str = "06:00"
    timezone: str = "America/Chicago"
    enabled: bool = True
    reports: List[str] = field(default_factory=list)


@dataclass
class PipelineConfig:
    """Data pipeline configuration"""
    output_dir: str = "./data"
    save_raw_responses: bool = True
    enable_database_output: bool = True
    enable_file_output: bool = True
    verification_enabled: bool = True
    verification_sample_size: int = 10

    @classmethod
    def from_env(cls) -> 'PipelineConfig':
        return cls(
            output_dir=os.getenv('OUTPUT_DIR', './data'),
            save_raw_responses=os.getenv('SAVE_RAW', 'true').lower() == 'true',
            enable_database_output=os.getenv('DB_OUTPUT', 'true').lower() == 'true',
            enable_file_output=os.getenv('FILE_OUTPUT', 'true').lower() == 'true',
            verification_enabled=os.getenv('VERIFICATION_ENABLED', 'true').lower() == 'true',
            verification_sample_size=int(os.getenv('VERIFICATION_SAMPLE_SIZE', '10'))
        )


@dataclass
class Settings:
    """Main settings container for the entire system"""
    database: DatabaseConfig = field(default_factory=DatabaseConfig.from_env)
    api: APIConfig = field(default_factory=APIConfig.from_env)
    scheduler: SchedulerConfig = field(default_factory=SchedulerConfig.from_env)
    pipeline: PipelineConfig = field(default_factory=PipelineConfig.from_env)

    # Data source schedules
    data_sources: Dict[str, DataSourceSchedule] = field(default_factory=dict)

    # Project paths
    project_root: Path = field(default_factory=lambda: Path(__file__).parent.parent.parent)

    def __post_init__(self):
        """Initialize default data source schedules"""
        if not self.data_sources:
            self.data_sources = self._default_data_sources()

    def _default_data_sources(self) -> Dict[str, DataSourceSchedule]:
        """Default data source schedule configuration"""
        return {
            'usda_ams_daily': DataSourceSchedule(
                source_id='usda_ams_daily',
                source_name='USDA AMS Daily Reports',
                frequency='daily',
                time='06:00',
                timezone='America/Chicago',
                reports=['3617', '2849', '3192', '3225']  # Ethanol, grain bids
            ),
            'usda_ams_weekly': DataSourceSchedule(
                source_id='usda_ams_weekly',
                source_name='USDA AMS Weekly Reports',
                frequency='weekly',
                day_of_week='thursday',
                time='15:00',
                timezone='America/Chicago',
                reports=['weekly_ethanol', 'weekly_grain_coproducts']
            ),
            'fgis_export_inspections': DataSourceSchedule(
                source_id='fgis_export',
                source_name='FGIS Export Inspections',
                frequency='weekly',
                day_of_week='friday',
                time='11:00',
                timezone='America/New_York',
                reports=['export_inspections']
            ),
            'south_america_trade': DataSourceSchedule(
                source_id='south_america_trade',
                source_name='South America Trade Data',
                frequency='monthly',
                day_of_month=15,
                time='09:00',
                timezone='America/Chicago',
                reports=['argentina', 'brazil', 'colombia', 'paraguay', 'uruguay']
            )
        }

    @classmethod
    def from_file(cls, config_path: str) -> 'Settings':
        """Load settings from a JSON configuration file"""
        settings = cls()

        if Path(config_path).exists():
            with open(config_path) as f:
                config_data = json.load(f)

            # Override with file settings
            if 'data_sources' in config_data:
                for source_id, source_config in config_data['data_sources'].items():
                    settings.data_sources[source_id] = DataSourceSchedule(**source_config)

            logger.info(f"Loaded configuration from {config_path}")

        return settings

    def to_dict(self) -> Dict[str, Any]:
        """Convert settings to dictionary"""
        return {
            'database': {
                'type': self.database.db_type,
                'path': self.database.sqlite_path if self.database.db_type == 'sqlite' else None,
                'host': self.database.host,
                'port': self.database.port,
            },
            'scheduler': {
                'enabled': self.scheduler.enabled,
                'timezone': self.scheduler.timezone,
                'interval': self.scheduler.check_interval_seconds
            },
            'pipeline': {
                'output_dir': self.pipeline.output_dir,
                'verification_enabled': self.pipeline.verification_enabled
            },
            'data_sources': {
                k: {
                    'name': v.source_name,
                    'frequency': v.frequency,
                    'enabled': v.enabled
                }
                for k, v in self.data_sources.items()
            }
        }


# Global settings instance
_settings: Optional[Settings] = None


def get_settings() -> Settings:
    """Get the global settings instance"""
    global _settings
    if _settings is None:
        _settings = Settings()
    return _settings


def reload_settings(config_path: str = None) -> Settings:
    """Reload settings from environment/file"""
    global _settings
    if config_path:
        _settings = Settings.from_file(config_path)
    else:
        _settings = Settings()
    return _settings
