"""
Export Inspections Agent Configuration Settings
Round Lakes Commodities - Data Pipeline Configuration
"""

import os
from pathlib import Path
from dataclasses import dataclass, field
from typing import Dict, List, Optional
from enum import Enum


class DatabaseType(Enum):
    """Supported database types"""
    MYSQL = "mysql"
    POSTGRESQL = "postgresql"
    SQLITE = "sqlite"


@dataclass
class DatabaseConfig:
    """Database connection configuration"""
    db_type: DatabaseType = DatabaseType.MYSQL
    host: str = "localhost"
    port: int = 3306
    database: str = "export_inspections"
    username: str = ""
    password: str = ""
    
    # SQLite specific
    sqlite_path: Optional[Path] = None
    
    # Connection pool settings
    pool_size: int = 5
    max_overflow: int = 10
    pool_timeout: int = 30
    
    def get_connection_string(self) -> str:
        """Generate database connection string"""
        if self.db_type == DatabaseType.SQLITE:
            path = self.sqlite_path or Path("./data/export_inspections.db")
            return f"sqlite:///{path}"
        elif self.db_type == DatabaseType.MYSQL:
            return f"mysql+pymysql://{self.username}:{self.password}@{self.host}:{self.port}/{self.database}"
        elif self.db_type == DatabaseType.POSTGRESQL:
            return f"postgresql+psycopg2://{self.username}:{self.password}@{self.host}:{self.port}/{self.database}"
        else:
            raise ValueError(f"Unsupported database type: {self.db_type}")


@dataclass
class FGISDataSourceConfig:
    """FGIS Export Grain Report data source configuration"""
    # Base URL for the FGIS export grain reports
    base_url: str = "https://fgisonline.ams.usda.gov/exportgrainreport"
    
    # File naming pattern: CY{YEAR}.csv (e.g., CY2025.csv)
    file_pattern: str = "CY{year}.csv"
    
    # Local storage paths
    data_directory: Path = field(default_factory=lambda: Path("./data/raw"))
    processed_directory: Path = field(default_factory=lambda: Path("./data/processed"))
    
    # Download settings
    download_timeout: int = 300  # 5 minutes
    retry_attempts: int = 3
    retry_delay: int = 10  # seconds
    
    # Update schedule (report typically released Monday for week ending previous Thursday)
    update_day: str = "Monday"
    expected_release_hour: int = 11  # 11 AM Eastern
    
    def get_download_url(self, year: int) -> str:
        """Generate download URL for a specific year"""
        filename = self.file_pattern.format(year=year)
        return f"{self.base_url}/{filename}"


@dataclass 
class CommodityConfig:
    """Commodity-specific configuration"""
    # Standard bushel weights (lbs per bushel)
    bushel_weights: Dict[str, float] = field(default_factory=lambda: {
        "SOYBEANS": 60.0,
        "CORN": 56.0,
        "WHEAT": 60.0,  # All wheat classes
        "SORGHUM": 56.0,
        "BARLEY": 48.0,
        "OATS": 32.0,
        "RYE": 56.0,
        "FLAXSEED": 56.0,
        "SUNFLOWER": 28.0,  # Oil type
    })
    
    # Marketing year start months (1-indexed)
    marketing_year_starts: Dict[str, int] = field(default_factory=lambda: {
        "SOYBEANS": 9,   # September
        "CORN": 9,       # September
        "WHEAT": 6,      # June (all classes)
        "SORGHUM": 9,    # September
        "BARLEY": 6,     # June
        "OATS": 6,       # June
        "RYE": 6,        # June
        "FLAXSEED": 9,   # September
        "SUNFLOWER": 10, # October
    })
    
    # Wheat classes for detailed tracking
    wheat_classes: List[str] = field(default_factory=lambda: [
        "HRW",   # Hard Red Winter
        "HRS",   # Hard Red Spring
        "SRW",   # Soft Red Winter
        "SWW",   # Soft White Winter
        "SWH",   # Soft White (general)
        "HWW",   # Hard White Winter
        "DU",    # Durum
        "DNS",   # Dark Northern Spring
    ])
    
    def get_bushel_weight(self, commodity: str) -> float:
        """Get bushel weight for commodity, default to 60 if unknown"""
        return self.bushel_weights.get(commodity.upper(), 60.0)
    
    def get_marketing_year(self, commodity: str, date) -> int:
        """Calculate marketing year for a given commodity and date"""
        start_month = self.marketing_year_starts.get(commodity.upper(), 9)
        if date.month >= start_month:
            return date.year
        else:
            return date.year - 1


@dataclass
class RegionMappingConfig:
    """Port and destination region mapping configuration"""
    # US Port regions based on FGIS definitions
    port_region_mapping: Dict[str, str] = field(default_factory=lambda: {
        # Gulf ports
        "MISSISSIPPI R.": "GULF",
        "S. TEXAS": "GULF",
        "E. GULF": "GULF",
        "EAST GULF": "GULF",
        "WEST GULF": "GULF",
        "TEXAS GULF": "GULF",
        
        # Pacific ports
        "COLUMBIA R.": "PACIFIC",
        "PUGET SOUND": "PACIFIC",
        "CALIFORNIA": "PACIFIC",
        "WEST COAST": "PACIFIC",
        
        # Atlantic ports
        "N. ATLANTIC": "ATLANTIC",
        "S. ATLANTIC": "ATLANTIC",
        "EAST COAST": "ATLANTIC",
        
        # Great Lakes
        "GREAT LAKES": "GREAT_LAKES",
        "LAKE SUPERIOR": "GREAT_LAKES",
        "LAKE ONTARIO": "GREAT_LAKES",
        "DULUTH": "GREAT_LAKES",
        
        # Interior/Land crossings
        "INTERIOR": "INTERIOR",
        "CANADA": "INTERIOR",
        "MEXICO": "INTERIOR",
    })
    
    # Destination country to region mapping
    destination_regions: Dict[str, str] = field(default_factory=lambda: {
        # European Union
        "AUSTRIA": "EU",
        "BELGIUM": "EU",
        "BULGARIA": "EU",
        "CROATIA": "EU",
        "CYPRUS": "EU",
        "CZECH REPUBLIC": "EU",
        "DENMARK": "EU",
        "ESTONIA": "EU",
        "FINLAND": "EU",
        "FRANCE": "EU",
        "GERMANY": "EU",
        "GREECE": "EU",
        "HUNGARY": "EU",
        "IRELAND": "EU",
        "ITALY": "EU",
        "LATVIA": "EU",
        "LITHUANIA": "EU",
        "LUXEMBOURG": "EU",
        "MALTA": "EU",
        "NETHERLANDS": "EU",
        "POLAND": "EU",
        "PORTUGAL": "EU",
        "ROMANIA": "EU",
        "SLOVAKIA": "EU",
        "SLOVENIA": "EU",
        "SPAIN": "EU",
        "SWEDEN": "EU",
        
        # Former UK (post-Brexit)
        "UNITED KINGDOM": "OTHER_EUROPE",
        "UN KINGDOM": "OTHER_EUROPE",
        
        # Other Europe
        "SWITZERLAND": "OTHER_EUROPE",
        "NORWAY": "OTHER_EUROPE",
        "TURKEY": "OTHER_EUROPE",
        
        # FSU (Former Soviet Union)
        "RUSSIA": "FSU",
        "UKRAINE": "FSU",
        "BELARUS": "FSU",
        "KAZAKHSTAN": "FSU",
        "UZBEKISTAN": "FSU",
        "AZERBAIJAN": "FSU",
        "GEORGIA": "FSU",
        "ARMENIA": "FSU",
        "MOLDOVA": "FSU",
        "TURKMENISTAN": "FSU",
        "TAJIKISTAN": "FSU",
        "KYRGYZSTAN": "FSU",
        
        # Asia & Oceania
        "CHINA": "ASIA_OCEANIA",
        "JAPAN": "ASIA_OCEANIA",
        "SOUTH KOREA": "ASIA_OCEANIA",
        "KOREA, SOUTH": "ASIA_OCEANIA",
        "TAIWAN": "ASIA_OCEANIA",
        "VIETNAM": "ASIA_OCEANIA",
        "THAILAND": "ASIA_OCEANIA",
        "INDONESIA": "ASIA_OCEANIA",
        "PHILIPPINES": "ASIA_OCEANIA",
        "MALAYSIA": "ASIA_OCEANIA",
        "SINGAPORE": "ASIA_OCEANIA",
        "INDIA": "ASIA_OCEANIA",
        "PAKISTAN": "ASIA_OCEANIA",
        "BANGLADESH": "ASIA_OCEANIA",
        "AUSTRALIA": "ASIA_OCEANIA",
        "NEW ZEALAND": "ASIA_OCEANIA",
        "NEPAL": "ASIA_OCEANIA",
        "SRI LANKA": "ASIA_OCEANIA",
        
        # Middle East
        "SAUDI ARABIA": "MIDDLE_EAST",
        "UNITED ARAB EMIRATES": "MIDDLE_EAST",
        "UAE": "MIDDLE_EAST",
        "ISRAEL": "MIDDLE_EAST",
        "JORDAN": "MIDDLE_EAST",
        "IRAQ": "MIDDLE_EAST",
        "IRAN": "MIDDLE_EAST",
        "KUWAIT": "MIDDLE_EAST",
        "OMAN": "MIDDLE_EAST",
        "QATAR": "MIDDLE_EAST",
        "BAHRAIN": "MIDDLE_EAST",
        "YEMEN": "MIDDLE_EAST",
        "LEBANON": "MIDDLE_EAST",
        "SYRIA": "MIDDLE_EAST",
        
        # Africa
        "EGYPT": "AFRICA",
        "MOROCCO": "AFRICA",
        "ALGERIA": "AFRICA",
        "TUNISIA": "AFRICA",
        "LIBYA": "AFRICA",
        "NIGERIA": "AFRICA",
        "SOUTH AFRICA": "AFRICA",
        "KENYA": "AFRICA",
        "ETHIOPIA": "AFRICA",
        "SUDAN": "AFRICA",
        
        # Western Hemisphere
        "CANADA": "WESTERN_HEMISPHERE",
        "MEXICO": "WESTERN_HEMISPHERE",
        "BRAZIL": "WESTERN_HEMISPHERE",
        "ARGENTINA": "WESTERN_HEMISPHERE",
        "COLOMBIA": "WESTERN_HEMISPHERE",
        "PERU": "WESTERN_HEMISPHERE",
        "CHILE": "WESTERN_HEMISPHERE",
        "VENEZUELA": "WESTERN_HEMISPHERE",
        "ECUADOR": "WESTERN_HEMISPHERE",
        "GUATEMALA": "WESTERN_HEMISPHERE",
        "COSTA RICA": "WESTERN_HEMISPHERE",
        "PANAMA": "WESTERN_HEMISPHERE",
        "DOMINICAN REPUBLIC": "WESTERN_HEMISPHERE",
        "DOMINICN REP": "WESTERN_HEMISPHERE",
        "JAMAICA": "WESTERN_HEMISPHERE",
        "HONDURAS": "WESTERN_HEMISPHERE",
        "EL SALVADOR": "WESTERN_HEMISPHERE",
        "NICARAGUA": "WESTERN_HEMISPHERE",
        "CUBA": "WESTERN_HEMISPHERE",
        "HAITI": "WESTERN_HEMISPHERE",
        "TRINIDAD": "WESTERN_HEMISPHERE",
        "TRINIDAD AND TOBAGO": "WESTERN_HEMISPHERE",
    })
    
    def get_port_region(self, port: str) -> str:
        """Get standardized port region"""
        if not port:
            return "UNKNOWN"
        port_upper = port.upper().strip()
        return self.port_region_mapping.get(port_upper, "OTHER")
    
    def get_destination_region(self, country: str) -> str:
        """Get standardized destination region"""
        if not country:
            return "UNKNOWN"
        country_upper = country.upper().strip()
        return self.destination_regions.get(country_upper, "OTHER")


@dataclass
class AgentConfig:
    """Main agent configuration"""
    # Agent identification
    agent_name: str = "ExportInspectionsAgent"
    agent_version: str = "1.0.0"
    
    # Logging
    log_level: str = "INFO"
    log_directory: Path = field(default_factory=lambda: Path("./logs"))
    
    # Data source configuration
    data_source: FGISDataSourceConfig = field(default_factory=FGISDataSourceConfig)
    
    # Database configuration
    database: DatabaseConfig = field(default_factory=DatabaseConfig)
    
    # Commodity configuration
    commodities: CommodityConfig = field(default_factory=CommodityConfig)
    
    # Region mapping
    regions: RegionMappingConfig = field(default_factory=RegionMappingConfig)
    
    # Processing settings
    batch_size: int = 1000
    parallel_workers: int = 4
    
    # Data validation
    validate_on_insert: bool = True
    reject_duplicates: bool = True
    
    @classmethod
    def from_environment(cls) -> "AgentConfig":
        """Create configuration from environment variables"""
        config = cls()
        
        # Database settings from environment
        db_type = os.getenv("DB_TYPE", "mysql")
        config.database.db_type = DatabaseType(db_type.lower())
        config.database.host = os.getenv("DB_HOST", "localhost")
        config.database.port = int(os.getenv("DB_PORT", "3306"))
        config.database.database = os.getenv("DB_NAME", "export_inspections")
        config.database.username = os.getenv("DB_USER", "")
        config.database.password = os.getenv("DB_PASSWORD", "")
        
        # Logging from environment
        config.log_level = os.getenv("LOG_LEVEL", "INFO")
        
        return config


# Default configuration instance
default_config = AgentConfig()
