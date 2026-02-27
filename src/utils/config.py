"""
RLC-Agent Configuration Management

Centralized configuration for the entire system.
Supports environment variables, config files, and defaults.
"""

import os
import json
import logging
from pathlib import Path
from typing import Optional, Dict, Any
from dataclasses import dataclass, field
from dotenv import load_dotenv

logger = logging.getLogger(__name__)

# Load environment variables from .env file
PROJECT_ROOT = Path(__file__).parent.parent.parent
load_dotenv(PROJECT_ROOT / ".env")


@dataclass
class DatabaseConfig:
    """Database configuration."""
    # PostgreSQL (production)
    pg_host: str = field(default_factory=lambda: os.getenv("PG_HOST", "localhost"))
    pg_port: int = field(default_factory=lambda: int(os.getenv("PG_PORT", "5432")))
    pg_database: str = field(default_factory=lambda: os.getenv("PG_DATABASE", "rlc_commodities"))
    pg_user: str = field(default_factory=lambda: os.getenv("PG_USER", "rlc_admin"))
    pg_password: str = field(default_factory=lambda: os.getenv("PG_PASSWORD", ""))

    # SQLite (development/backup)
    sqlite_path: Path = field(default_factory=lambda: PROJECT_ROOT / "data" / "rlc_commodities.db")

    # Which database to use
    use_postgres: bool = field(default_factory=lambda: os.getenv("USE_POSTGRES", "false").lower() == "true")

    @property
    def connection_string(self) -> str:
        """Get the appropriate connection string."""
        if self.use_postgres:
            return f"postgresql://{self.pg_user}:{self.pg_password}@{self.pg_host}:{self.pg_port}/{self.pg_database}"
        return f"sqlite:///{self.sqlite_path}"


@dataclass
class APIConfig:
    """API credentials configuration."""
    # USDA
    usda_ams_api_key: str = field(default_factory=lambda: os.getenv("USDA_AMS_API_KEY", ""))
    usda_nass_api_key: str = field(default_factory=lambda: os.getenv("USDA_NASS_API_KEY", ""))
    usda_fas_api_key: str = field(default_factory=lambda: os.getenv("USDA_FAS_API_KEY", ""))

    # EIA
    eia_api_key: str = field(default_factory=lambda: os.getenv("EIA_API_KEY", ""))

    # Census Bureau
    census_api_key: str = field(default_factory=lambda: os.getenv("CENSUS_API_KEY", ""))

    # Google APIs
    google_credentials_path: Path = field(
        default_factory=lambda: Path(os.getenv("GOOGLE_CREDENTIALS_PATH", str(PROJECT_ROOT / "config" / "credentials.json")))
    )

    # Anthropic/OpenAI for LLM
    anthropic_api_key: str = field(default_factory=lambda: os.getenv("ANTHROPIC_API_KEY", ""))
    openai_api_key: str = field(default_factory=lambda: os.getenv("OPENAI_API_KEY", ""))

    # Ollama (local LLM)
    ollama_url: str = field(default_factory=lambda: os.getenv("OLLAMA_URL", "http://localhost:11434"))
    ollama_model: str = field(default_factory=lambda: os.getenv("OLLAMA_MODEL", "llama3"))


@dataclass
class PathConfig:
    """Path configuration."""
    project_root: Path = field(default_factory=lambda: PROJECT_ROOT)
    src_root: Path = field(default_factory=lambda: PROJECT_ROOT / "src")
    data_dir: Path = field(default_factory=lambda: PROJECT_ROOT / "data")
    config_dir: Path = field(default_factory=lambda: PROJECT_ROOT / "config")
    models_dir: Path = field(default_factory=lambda: PROJECT_ROOT / "Models")
    exports_dir: Path = field(default_factory=lambda: PROJECT_ROOT / "exports")
    docs_dir: Path = field(default_factory=lambda: PROJECT_ROOT / "docs")
    logs_dir: Path = field(default_factory=lambda: PROJECT_ROOT / "data" / "logs")
    cache_dir: Path = field(default_factory=lambda: PROJECT_ROOT / "data" / "cache")

    def ensure_directories(self):
        """Ensure all required directories exist."""
        for dir_path in [self.data_dir, self.logs_dir, self.cache_dir, self.exports_dir]:
            dir_path.mkdir(parents=True, exist_ok=True)


@dataclass
class AgentConfig:
    """Agent behavior configuration."""
    # Autonomy level (1-5, higher = more autonomous)
    autonomy_level: int = field(default_factory=lambda: int(os.getenv("AUTONOMY_LEVEL", "3")))

    # Default commodities to track
    default_commodities: list = field(default_factory=lambda: [
        "soybeans", "corn", "wheat", "soy_oil", "soy_meal",
        "canola", "palm_oil", "ethanol"
    ])

    # Default regions
    default_regions: list = field(default_factory=lambda: [
        "us", "brazil", "argentina", "canada", "eu"
    ])

    # Cache TTL (hours)
    cache_ttl_hours: int = field(default_factory=lambda: int(os.getenv("CACHE_TTL_HOURS", "24")))

    # Rate limiting
    rate_limit_per_minute: int = field(default_factory=lambda: int(os.getenv("RATE_LIMIT_PER_MINUTE", "60")))


@dataclass
class Settings:
    """Main settings class combining all configurations."""
    database: DatabaseConfig = field(default_factory=DatabaseConfig)
    api: APIConfig = field(default_factory=APIConfig)
    paths: PathConfig = field(default_factory=PathConfig)
    agent: AgentConfig = field(default_factory=AgentConfig)

    def __post_init__(self):
        """Ensure directories exist after initialization."""
        self.paths.ensure_directories()

    @classmethod
    def from_file(cls, config_path: Path) -> "Settings":
        """Load settings from a JSON file."""
        if config_path.exists():
            with open(config_path) as f:
                data = json.load(f)
            # TODO: Parse and apply config file settings
            logger.info(f"Loaded configuration from {config_path}")
        return cls()

    def to_dict(self) -> Dict[str, Any]:
        """Convert settings to dictionary (excludes sensitive data)."""
        return {
            "database": {
                "use_postgres": self.database.use_postgres,
                "pg_host": self.database.pg_host,
                "pg_database": self.database.pg_database,
            },
            "paths": {
                "project_root": str(self.paths.project_root),
                "data_dir": str(self.paths.data_dir),
                "models_dir": str(self.paths.models_dir),
            },
            "agent": {
                "autonomy_level": self.agent.autonomy_level,
                "default_commodities": self.agent.default_commodities,
                "default_regions": self.agent.default_regions,
            }
        }


# Global settings instance
_settings: Optional[Settings] = None


def get_settings() -> Settings:
    """Get the global settings instance."""
    global _settings
    if _settings is None:
        _settings = Settings()
    return _settings


def reload_settings() -> Settings:
    """Reload settings from environment/files."""
    global _settings
    _settings = Settings()
    return _settings
