"""
Configuration Settings for RLC Master Agent System
Centralized settings management for all agent components
Round Lakes Commodities
"""

import os
import json
import logging
from pathlib import Path
from typing import Dict, Any, Optional, List
from dataclasses import dataclass, field
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Base paths
BASE_DIR = Path(__file__).parent.parent
PROJECT_ROOT = BASE_DIR.parent


@dataclass
class LLMConfig:
    """LLM (Language Model) configuration"""
    provider: str = "ollama"  # 'ollama' or 'openai'
    ollama_model: str = "llama2:13b"
    ollama_base_url: str = "http://localhost:11434"
    openai_api_key: str = ""
    openai_model: str = "gpt-4"
    temperature: float = 0.7
    max_tokens: int = 2000

    @classmethod
    def from_env(cls) -> 'LLMConfig':
        """Create LLMConfig from environment variables"""
        return cls(
            provider=os.getenv('LLM_PROVIDER', 'ollama'),
            ollama_model=os.getenv('OLLAMA_MODEL', 'llama2:13b'),
            ollama_base_url=os.getenv('OLLAMA_BASE_URL', 'http://localhost:11434'),
            openai_api_key=os.getenv('OPENAI_API_KEY', ''),
            openai_model=os.getenv('OPENAI_MODEL', 'gpt-4'),
            temperature=float(os.getenv('LLM_TEMPERATURE', '0.7')),
            max_tokens=int(os.getenv('LLM_MAX_TOKENS', '2000'))
        )


@dataclass
class NotionConfig:
    """Notion integration configuration"""
    api_key: str = ""
    tasks_db_id: str = ""
    memory_db_id: str = ""
    interactions_db_id: str = ""
    wiki_db_id: str = ""

    @classmethod
    def from_env(cls) -> 'NotionConfig':
        """Create NotionConfig from environment variables"""
        return cls(
            api_key=os.getenv('NOTION_API_KEY', ''),
            tasks_db_id=os.getenv('NOTION_TASKS_DB', ''),
            memory_db_id=os.getenv('NOTION_MEMORY_DB', ''),
            interactions_db_id=os.getenv('NOTION_INTERACTIONS_DB', ''),
            wiki_db_id=os.getenv('NOTION_WIKI_DB', '')
        )

    def is_configured(self) -> bool:
        """Check if Notion is properly configured"""
        return bool(self.api_key and self.tasks_db_id)


@dataclass
class GoogleConfig:
    """Google API configuration"""
    credentials_dir: str = ""
    gmail_work_credentials: str = "gmail_work_credentials.json"
    gmail_personal_credentials: str = "gmail_personal_credentials.json"
    calendar_credentials: str = "calendar_credentials.json"
    token_dir: str = ""
    scopes_gmail: List[str] = field(default_factory=lambda: [
        'https://www.googleapis.com/auth/gmail.readonly',
        'https://www.googleapis.com/auth/gmail.send',
        'https://www.googleapis.com/auth/gmail.modify'
    ])
    scopes_calendar: List[str] = field(default_factory=lambda: [
        'https://www.googleapis.com/auth/calendar',
        'https://www.googleapis.com/auth/calendar.events'
    ])

    @classmethod
    def from_env(cls) -> 'GoogleConfig':
        """Create GoogleConfig from environment variables"""
        credentials_dir = os.getenv('GOOGLE_CREDENTIALS_DIR', str(BASE_DIR / 'config'))
        return cls(
            credentials_dir=credentials_dir,
            gmail_work_credentials=os.getenv('GMAIL_WORK_CREDENTIALS', 'gmail_work_credentials.json'),
            gmail_personal_credentials=os.getenv('GMAIL_PERSONAL_CREDENTIALS', 'gmail_personal_credentials.json'),
            calendar_credentials=os.getenv('CALENDAR_CREDENTIALS', 'calendar_credentials.json'),
            token_dir=os.getenv('GOOGLE_TOKEN_DIR', str(BASE_DIR / 'config' / 'tokens'))
        )

    def get_credentials_path(self, service: str) -> Path:
        """Get full path to credentials file"""
        if service == 'gmail_work':
            return Path(self.credentials_dir) / self.gmail_work_credentials
        elif service == 'gmail_personal':
            return Path(self.credentials_dir) / self.gmail_personal_credentials
        elif service == 'calendar':
            return Path(self.credentials_dir) / self.calendar_credentials
        else:
            raise ValueError(f"Unknown service: {service}")


@dataclass
class APIConfig:
    """External API configuration"""
    usda_api_key: str = ""
    usda_base_url: str = "https://marsapi.ams.usda.gov/services/v1.2"
    census_api_key: str = ""
    census_base_url: str = "https://api.census.gov/data"
    weather_api_key: str = ""
    weather_base_url: str = "https://api.openweathermap.org/data/2.5"
    request_timeout: int = 30
    max_retries: int = 3
    retry_backoff: float = 2.0

    @classmethod
    def from_env(cls) -> 'APIConfig':
        """Create APIConfig from environment variables"""
        return cls(
            usda_api_key=os.getenv('USDA_API_KEY', os.getenv('USDA_AMS_API_KEY', '')),
            usda_base_url=os.getenv('USDA_BASE_URL', 'https://marsapi.ams.usda.gov/services/v1.2'),
            census_api_key=os.getenv('CENSUS_API_KEY', ''),
            census_base_url=os.getenv('CENSUS_BASE_URL', 'https://api.census.gov/data'),
            weather_api_key=os.getenv('WEATHER_API_KEY', os.getenv('OPENWEATHER_API_KEY', '')),
            weather_base_url=os.getenv('WEATHER_BASE_URL', 'https://api.openweathermap.org/data/2.5'),
            request_timeout=int(os.getenv('REQUEST_TIMEOUT', '30')),
            max_retries=int(os.getenv('MAX_RETRIES', '3')),
            retry_backoff=float(os.getenv('RETRY_BACKOFF', '2.0'))
        )


@dataclass
class UserConfig:
    """User-specific configuration"""
    name: str = ""
    business_name: str = "Round Lakes Commodities"
    primary_email: str = ""
    timezone: str = "America/Chicago"
    working_hours_start: int = 8
    working_hours_end: int = 18
    preferred_meeting_duration: int = 30  # minutes

    @classmethod
    def from_env(cls) -> 'UserConfig':
        """Create UserConfig from environment variables"""
        return cls(
            name=os.getenv('USER_NAME', ''),
            business_name=os.getenv('BUSINESS_NAME', 'Round Lakes Commodities'),
            primary_email=os.getenv('USER_EMAIL', ''),
            timezone=os.getenv('USER_TIMEZONE', 'America/Chicago'),
            working_hours_start=int(os.getenv('WORKING_HOURS_START', '8')),
            working_hours_end=int(os.getenv('WORKING_HOURS_END', '18')),
            preferred_meeting_duration=int(os.getenv('PREFERRED_MEETING_DURATION', '30'))
        )


@dataclass
class Settings:
    """Master configuration container for RLC Master Agent"""
    llm: LLMConfig = field(default_factory=LLMConfig.from_env)
    notion: NotionConfig = field(default_factory=NotionConfig.from_env)
    google: GoogleConfig = field(default_factory=GoogleConfig.from_env)
    api: APIConfig = field(default_factory=APIConfig.from_env)
    user: UserConfig = field(default_factory=UserConfig.from_env)

    # Agent behavior settings
    autonomy_level: int = 1  # 1=supervised, 2=partial, 3=autonomous
    require_calendar_approval: bool = False
    require_email_approval: bool = True
    log_level: str = "INFO"

    # Paths
    base_dir: Path = field(default_factory=lambda: BASE_DIR)
    logs_dir: Path = field(default_factory=lambda: BASE_DIR / 'logs')
    data_dir: Path = field(default_factory=lambda: BASE_DIR / 'data')

    def __post_init__(self):
        """Initialize directories and load additional settings"""
        self.autonomy_level = int(os.getenv('AUTONOMY_LEVEL', '1'))
        self.require_calendar_approval = os.getenv('REQUIRE_CALENDAR_APPROVAL', 'false').lower() == 'true'
        self.require_email_approval = os.getenv('REQUIRE_EMAIL_APPROVAL', 'true').lower() == 'true'
        self.log_level = os.getenv('LOG_LEVEL', 'INFO')

        # Ensure directories exist
        self.create_directories()

    def create_directories(self):
        """Create required directories"""
        directories = [
            self.logs_dir,
            self.data_dir,
            Path(self.google.token_dir) if self.google.token_dir else None
        ]
        for directory in directories:
            if directory:
                directory.mkdir(parents=True, exist_ok=True)

    def validate(self) -> Dict[str, List[str]]:
        """Validate configuration and return any issues"""
        issues = {
            'errors': [],
            'warnings': []
        }

        # Check LLM config
        if self.llm.provider == 'openai' and not self.llm.openai_api_key:
            issues['errors'].append("OpenAI provider selected but OPENAI_API_KEY not set")

        # Check Notion config
        if not self.notion.api_key:
            issues['warnings'].append("NOTION_API_KEY not set - Notion features will be disabled")
        elif not all([self.notion.tasks_db_id, self.notion.memory_db_id]):
            issues['warnings'].append("Not all Notion database IDs are configured")

        # Check API keys
        if not self.api.usda_api_key:
            issues['warnings'].append("USDA_API_KEY not set - USDA data features will be limited")
        if not self.api.census_api_key:
            issues['warnings'].append("CENSUS_API_KEY not set - Census data features will be limited")

        return issues

    def to_dict(self) -> Dict[str, Any]:
        """Convert settings to dictionary (excluding secrets)"""
        return {
            'llm': {
                'provider': self.llm.provider,
                'model': self.llm.ollama_model if self.llm.provider == 'ollama' else self.llm.openai_model,
                'temperature': self.llm.temperature
            },
            'notion': {
                'configured': self.notion.is_configured(),
                'has_tasks_db': bool(self.notion.tasks_db_id),
                'has_memory_db': bool(self.notion.memory_db_id),
                'has_interactions_db': bool(self.notion.interactions_db_id),
                'has_wiki_db': bool(self.notion.wiki_db_id)
            },
            'api': {
                'has_usda_key': bool(self.api.usda_api_key),
                'has_census_key': bool(self.api.census_api_key),
                'has_weather_key': bool(self.api.weather_api_key)
            },
            'user': {
                'name': self.user.name,
                'business': self.user.business_name,
                'timezone': self.user.timezone
            },
            'autonomy_level': self.autonomy_level,
            'log_level': self.log_level
        }

    @classmethod
    def load(cls) -> 'Settings':
        """Load settings from environment"""
        return cls()


# Global settings instance
_settings: Optional[Settings] = None


def get_settings() -> Settings:
    """Get the global settings instance (lazy initialization)"""
    global _settings
    if _settings is None:
        _settings = Settings.load()
    return _settings


def reload_settings() -> Settings:
    """Reload settings from environment"""
    global _settings
    load_dotenv(override=True)
    _settings = Settings.load()
    return _settings


def setup_logging(settings: Optional[Settings] = None) -> logging.Logger:
    """Set up logging for the application"""
    if settings is None:
        settings = get_settings()

    log_level = getattr(logging, settings.log_level.upper(), logging.INFO)
    log_file = settings.logs_dir / 'master_agent.log'

    # Create formatter
    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )

    # File handler
    file_handler = logging.FileHandler(log_file)
    file_handler.setLevel(log_level)
    file_handler.setFormatter(formatter)

    # Console handler
    console_handler = logging.StreamHandler()
    console_handler.setLevel(log_level)
    console_handler.setFormatter(formatter)

    # Root logger
    logger = logging.getLogger('rlc_master_agent')
    logger.setLevel(log_level)
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)

    return logger
