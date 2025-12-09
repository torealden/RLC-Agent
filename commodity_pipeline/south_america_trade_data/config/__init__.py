"""
South America Trade Data Configuration Module

Trade Flow Configurations:
- SouthAmericaTradeConfig: Main configuration class
- CountryConfig: Base configuration for country sources
- ArgentinaConfig, BrazilConfig, etc.: Country-specific configs

Lineup Configurations:
- LineupConfig: Base configuration for port lineup sources
- BrazilLineupConfig: Brazil ANEC lineup configuration
- ArgentinaLineupConfig: Argentina NABSA lineup configuration
"""

from .settings import (
    SouthAmericaTradeConfig,
    CountryConfig,
    ArgentinaConfig,
    BrazilConfig,
    ColombiaConfig,
    UruguayConfig,
    ParaguayConfig,
    DatabaseConfig,
    LineupConfig,
    BrazilLineupConfig,
    ArgentinaLineupConfig,
    default_config,
)

__all__ = [
    # Main config
    'SouthAmericaTradeConfig',
    'default_config',

    # Trade configs
    'CountryConfig',
    'ArgentinaConfig',
    'BrazilConfig',
    'ColombiaConfig',
    'UruguayConfig',
    'ParaguayConfig',

    # Lineup configs
    'LineupConfig',
    'BrazilLineupConfig',
    'ArgentinaLineupConfig',

    # Database
    'DatabaseConfig',
]
