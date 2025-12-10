"""
Tests for Export Inspections Agent
"""

import pytest
import sys
import os
from datetime import date, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from unittest.mock import Mock, patch, MagicMock
import tempfile
import csv

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from config.settings import (
    AgentConfig, DatabaseConfig, DatabaseType, 
    CommodityConfig, RegionMappingConfig
)
from database.models import (
    Base, InspectionRecord, WeeklyCommoditySummary,
    create_db_engine, init_database
)
from utils.csv_parser import FGISCSVParser, ParsedRecord, CSVColumnMapping
from utils.download_manager import FGISDownloadManager, DownloadResult


class TestDatabaseConfig:
    """Tests for database configuration"""
    
    def test_sqlite_connection_string(self):
        config = DatabaseConfig(
            db_type=DatabaseType.SQLITE,
            sqlite_path=Path("/tmp/test.db")
        )
        conn_str = config.get_connection_string()
        assert "sqlite:///" in conn_str
        assert "test.db" in conn_str
    
    def test_mysql_connection_string(self):
        config = DatabaseConfig(
            db_type=DatabaseType.MYSQL,
            host="localhost",
            port=3306,
            database="testdb",
            username="user",
            password="pass"
        )
        conn_str = config.get_connection_string()
        assert "mysql+pymysql://" in conn_str
        assert "user:pass" in conn_str
        assert "localhost:3306" in conn_str
    
    def test_postgresql_connection_string(self):
        config = DatabaseConfig(
            db_type=DatabaseType.POSTGRESQL,
            host="pghost",
            port=5432,
            database="testdb",
            username="pguser",
            password="pgpass"
        )
        conn_str = config.get_connection_string()
        assert "postgresql+psycopg2://" in conn_str


class TestCommodityConfig:
    """Tests for commodity configuration"""
    
    def test_bushel_weights(self):
        config = CommodityConfig()
        
        assert config.get_bushel_weight("SOYBEANS") == 60.0
        assert config.get_bushel_weight("CORN") == 56.0
        assert config.get_bushel_weight("WHEAT") == 60.0
        assert config.get_bushel_weight("UNKNOWN") == 60.0  # Default
    
    def test_marketing_year_calculation(self):
        config = CommodityConfig()
        
        # Soybeans: MY starts September
        assert config.get_marketing_year("SOYBEANS", date(2024, 10, 1)) == 2024
        assert config.get_marketing_year("SOYBEANS", date(2024, 8, 1)) == 2023
        
        # Wheat: MY starts June
        assert config.get_marketing_year("WHEAT", date(2024, 7, 1)) == 2024
        assert config.get_marketing_year("WHEAT", date(2024, 5, 1)) == 2023


class TestRegionMapping:
    """Tests for region mapping"""
    
    def test_port_region_mapping(self):
        config = RegionMappingConfig()
        
        assert config.get_port_region("MISSISSIPPI R.") == "GULF"
        assert config.get_port_region("COLUMBIA R.") == "PACIFIC"
        assert config.get_port_region("GREAT LAKES") == "GREAT_LAKES"
        assert config.get_port_region("UNKNOWN PORT") == "OTHER"
    
    def test_destination_region_mapping(self):
        config = RegionMappingConfig()
        
        assert config.get_destination_region("CHINA") == "ASIA_OCEANIA"
        assert config.get_destination_region("MEXICO") == "WESTERN_HEMISPHERE"
        assert config.get_destination_region("GERMANY") == "EU"
        assert config.get_destination_region("UNKNOWN COUNTRY") == "OTHER"


class TestCSVParser:
    """Tests for CSV parser"""
    
    def test_parse_date_formats(self):
        parser = FGISCSVParser()
        
        assert parser.parse_date("01/15/2024") == date(2024, 1, 15)
        assert parser.parse_date("2024-01-15") == date(2024, 1, 15)
        assert parser.parse_date("") is None
        assert parser.parse_date("invalid") is None
    
    def test_parse_integer(self):
        parser = FGISCSVParser()
        
        assert parser.parse_integer("1234") == 1234
        assert parser.parse_integer("1,234,567") == 1234567
        assert parser.parse_integer("1234.0") == 1234
        assert parser.parse_integer("") is None
        assert parser.parse_integer("abc") is None
    
    def test_parse_decimal(self):
        parser = FGISCSVParser()
        
        assert parser.parse_decimal("12.345") == Decimal("12.345")
        assert parser.parse_decimal("1,234.56") == Decimal("1234.56")
        assert parser.parse_decimal("") is None
    
    def test_parse_boolean(self):
        parser = FGISCSVParser()
        
        assert parser.parse_boolean("Y") is True
        assert parser.parse_boolean("YES") is True
        assert parser.parse_boolean("N") is False
        assert parser.parse_boolean("NO") is False
        assert parser.parse_boolean("") is None
    
    def test_parse_row(self):
        parser = FGISCSVParser()
        
        row = {
            'Thursday': '01/16/2025',
            'Serial No.': 'ABC123',
            'Grain': 'SOYBEANS',
            'Destination': 'CHINA',
            'Pounds': '1000000',
            'Metric Ton': '453.59',
            'TW': '55.2',
            'M AVG': '12.5'
        }
        
        record = parser.parse_row(row, row_number=1)
        
        assert record.is_valid
        assert record.data['week_ending_date'] == date(2025, 1, 16)
        assert record.data['serial_number'] == 'ABC123'
        assert record.data['grain'] == 'SOYBEANS'
        assert record.data['destination'] == 'CHINA'
        assert record.data['pounds'] == 1000000
    
    def test_parse_row_missing_required(self):
        parser = FGISCSVParser()
        
        row = {
            'Thursday': '01/16/2025',
            'Serial No.': 'ABC123',
            # Missing: Grain, Destination, Pounds
        }
        
        record = parser.parse_row(row, row_number=1)
        
        assert not record.is_valid
        assert len(record.errors) > 0


class TestCSVParserWithFile:
    """Tests for parsing actual CSV files"""
    
    @pytest.fixture
    def sample_csv(self, tmp_path):
        """Create a sample CSV file for testing"""
        csv_file = tmp_path / "CY2025.csv"
        
        headers = [
            'Thursday', 'Serial No.', 'Type Shipm', 'Type Serv', 
            'Type Carrier', 'Field Office', 'Port', 'Grain', 'Class',
            'Grade', 'Destination', 'Pounds', 'Metric Ton', 'TW', 'M AVG'
        ]
        
        rows = [
            ['01/16/2025', 'SN001', 'Export', 'Original', 'Vessel',
             'Gulf', 'MISSISSIPPI R.', 'SOYBEANS', '', 'US NO. 2',
             'CHINA', '1000000', '453.59', '55.2', '12.5'],
            ['01/16/2025', 'SN002', 'Export', 'Original', 'Vessel',
             'Pacific', 'COLUMBIA R.', 'CORN', '', 'US NO. 2',
             'JAPAN', '2000000', '907.18', '54.0', '14.0'],
        ]
        
        with open(csv_file, 'w', newline='') as f:
            writer = csv.writer(f)
            writer.writerow(headers)
            writer.writerows(rows)
        
        return csv_file
    
    def test_parse_file(self, sample_csv):
        parser = FGISCSVParser()
        
        records = list(parser.parse_file(sample_csv))
        
        assert len(records) == 2
        assert all(r.is_valid for r in records)
        
        # Check first record
        assert records[0].data['grain'] == 'SOYBEANS'
        assert records[0].data['destination'] == 'CHINA'
        
        # Check second record
        assert records[1].data['grain'] == 'CORN'
        assert records[1].data['destination'] == 'JAPAN'
    
    def test_validate_file_structure(self, sample_csv):
        parser = FGISCSVParser()
        
        is_valid, issues = parser.validate_file_structure(sample_csv)
        
        assert is_valid
        assert len(issues) == 0
    
    def test_get_column_headers(self, sample_csv):
        parser = FGISCSVParser()
        
        headers = parser.get_column_headers(sample_csv)
        
        assert 'Thursday' in headers
        assert 'Serial No.' in headers
        assert 'Grain' in headers


class TestDownloadManager:
    """Tests for download manager"""
    
    def test_get_download_url(self):
        manager = FGISDownloadManager()
        
        # Test URL generation
        # We can't test actual downloads without mocking
        local_files = manager.get_local_files()
        assert isinstance(local_files, dict)
    
    def test_download_result_string(self):
        result = DownloadResult(
            success=True,
            file_path=Path("/tmp/test.csv"),
            url="http://example.com/test.csv",
            status_code=200,
            file_size=1000,
            download_time=1.5
        )
        
        assert "test.csv" in str(result)
        assert "1,000" in str(result)


class TestDatabaseModels:
    """Tests for database models"""
    
    @pytest.fixture
    def db_session(self):
        """Create an in-memory SQLite database for testing"""
        session_factory, engine = init_database("sqlite:///:memory:")
        session = session_factory()
        yield session
        session.close()
    
    def test_create_inspection_record(self, db_session):
        record = InspectionRecord(
            week_ending_date=date(2025, 1, 16),
            serial_number='TEST001',
            grain='SOYBEANS',
            destination='CHINA',
            pounds=1000000,
            calendar_year=2025
        )
        
        db_session.add(record)
        db_session.commit()
        
        # Query back
        result = db_session.query(InspectionRecord).filter_by(
            serial_number='TEST001'
        ).first()
        
        assert result is not None
        assert result.grain == 'SOYBEANS'
        assert result.pounds == 1000000
    
    def test_create_weekly_summary(self, db_session):
        summary = WeeklyCommoditySummary(
            week_ending_date=date(2025, 1, 16),
            calendar_year=2025,
            marketing_year=2024,
            commodity='SOYBEANS',
            total_pounds=50000000,
            total_metric_tons=Decimal('22679.6'),
            certificate_count=100
        )
        
        db_session.add(summary)
        db_session.commit()
        
        # Query back
        result = db_session.query(WeeklyCommoditySummary).filter_by(
            commodity='SOYBEANS'
        ).first()
        
        assert result is not None
        assert result.total_pounds == 50000000


class TestIntegration:
    """Integration tests for the full workflow"""
    
    @pytest.fixture
    def agent_with_temp_db(self, tmp_path):
        """Create agent with temporary database"""
        config = AgentConfig()
        config.database.db_type = DatabaseType.SQLITE
        config.database.sqlite_path = tmp_path / "test.db"
        config.data_source.data_directory = tmp_path / "data"
        
        from agents.export_inspections_agent import ExportInspectionsAgent
        agent = ExportInspectionsAgent(config)
        
        return agent
    
    def test_initialize_lookup_tables(self, agent_with_temp_db):
        agent = agent_with_temp_db
        
        # Should not raise
        agent.initialize_lookup_tables()
        
        # Verify commodities were created
        session = agent.get_session()
        from database.models import Commodity
        commodities = session.query(Commodity).all()
        
        assert len(commodities) > 0
        assert any(c.code == 'SOYBEANS' for c in commodities)
        
        session.close()
    
    def test_get_status(self, agent_with_temp_db):
        agent = agent_with_temp_db
        
        status = agent.get_status()
        
        assert 'agent' in status
        assert 'version' in status
        assert 'total_records' in status


# Run tests if executed directly
if __name__ == '__main__':
    pytest.main([__file__, '-v'])
