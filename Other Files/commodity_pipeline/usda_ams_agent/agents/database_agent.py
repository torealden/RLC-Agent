"""
Database Agent for Commodity Data Pipeline
Handles database connection, table management, and data insertion
Supports SQLite (local development) and MySQL/PostgreSQL (production)
Round Lakes Commodities
"""

import logging
import sqlite3
from datetime import datetime
from typing import Dict, List, Optional, Any, Tuple, Union
from dataclasses import dataclass
from contextlib import contextmanager
from decimal import Decimal
import json

logger = logging.getLogger(__name__)


@dataclass
class InsertResult:
    """Result of a batch insert operation"""
    total_attempted: int
    inserted: int
    skipped: int  # duplicates
    errors: int
    error_messages: List[str]
    
    @property
    def success_rate(self) -> float:
        if self.total_attempted == 0:
            return 1.0
        return (self.inserted + self.skipped) / self.total_attempted


class DatabaseAgent:
    """
    Database agent for commodity price data storage.
    Handles connection management, schema creation, and CRUD operations.
    """
    
    # Schema definitions matching the plan document
    PRICE_DATA_SCHEMA = """
    CREATE TABLE IF NOT EXISTS price_data (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        report_date DATE NOT NULL,
        commodity VARCHAR(100) NOT NULL,
        location VARCHAR(150),
        price DECIMAL(12,4),
        price_low DECIMAL(12,4),
        price_high DECIMAL(12,4),
        price_avg DECIMAL(12,4),
        basis DECIMAL(12,4),
        basis_low DECIMAL(12,4),
        basis_high DECIMAL(12,4),
        unit VARCHAR(50),
        source_report VARCHAR(150) NOT NULL,
        report_type VARCHAR(50),
        grade VARCHAR(50),
        delivery_period VARCHAR(50),
        fetch_timestamp DATETIME,
        raw_data TEXT,
        created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
        UNIQUE(report_date, commodity, location, source_report)
    )
    """
    
    COMMODITIES_SCHEMA = """
    CREATE TABLE IF NOT EXISTS commodities (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        name VARCHAR(100) NOT NULL UNIQUE,
        category VARCHAR(50),
        default_unit VARCHAR(50),
        frequency VARCHAR(20),
        is_active BOOLEAN DEFAULT 1,
        created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
        updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
    )
    """
    
    DATA_QUALITY_LOG_SCHEMA = """
    CREATE TABLE IF NOT EXISTS data_quality_log (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        run_timestamp DATETIME NOT NULL,
        source_report VARCHAR(150),
        records_fetched INTEGER,
        records_inserted INTEGER,
        records_skipped INTEGER,
        records_failed INTEGER,
        verification_status VARCHAR(20),
        error_messages TEXT,
        created_at DATETIME DEFAULT CURRENT_TIMESTAMP
    )
    """
    
    PIPELINE_RUNS_SCHEMA = """
    CREATE TABLE IF NOT EXISTS pipeline_runs (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        run_id VARCHAR(50) NOT NULL UNIQUE,
        start_time DATETIME NOT NULL,
        end_time DATETIME,
        status VARCHAR(20) DEFAULT 'running',
        reports_processed INTEGER DEFAULT 0,
        total_records_inserted INTEGER DEFAULT 0,
        error_count INTEGER DEFAULT 0,
        notes TEXT,
        created_at DATETIME DEFAULT CURRENT_TIMESTAMP
    )
    """
    
    # Indexes for performance
    INDEXES = [
        "CREATE INDEX IF NOT EXISTS idx_price_data_date ON price_data(report_date)",
        "CREATE INDEX IF NOT EXISTS idx_price_data_commodity ON price_data(commodity)",
        "CREATE INDEX IF NOT EXISTS idx_price_data_source ON price_data(source_report)",
        "CREATE INDEX IF NOT EXISTS idx_price_data_date_commodity ON price_data(report_date, commodity)",
        "CREATE INDEX IF NOT EXISTS idx_quality_log_timestamp ON data_quality_log(run_timestamp)"
    ]
    
    def __init__(self, db_type: str = 'sqlite', connection_params: Dict = None):
        """
        Initialize the database agent.
        
        Args:
            db_type: Database type ('sqlite', 'mysql', 'postgresql')
            connection_params: Connection parameters dictionary
        """
        self.db_type = db_type.lower()
        self.connection_params = connection_params or {}
        self._connection = None
        
        # Set default SQLite path if not specified
        if self.db_type == 'sqlite' and 'database' not in self.connection_params:
            self.connection_params['database'] = './data/rlc_commodities.db'
        
        logger.info(f"DatabaseAgent initialized with {self.db_type} backend")
    
    @contextmanager
    def get_connection(self):
        """
        Context manager for database connections.
        Ensures proper connection handling and cleanup.
        """
        conn = None
        try:
            if self.db_type == 'sqlite':
                import sqlite3
                db_path = self.connection_params.get('database', './data/rlc_commodities.db')
                # Ensure directory exists
                import os
                os.makedirs(os.path.dirname(db_path) if os.path.dirname(db_path) else '.', exist_ok=True)
                conn = sqlite3.connect(db_path)
                conn.row_factory = sqlite3.Row  # Enable column access by name
            elif self.db_type == 'mysql':
                import mysql.connector
                conn = mysql.connector.connect(
                    host=self.connection_params.get('host', 'localhost'),
                    port=self.connection_params.get('port', 3306),
                    user=self.connection_params.get('username', 'root'),
                    password=self.connection_params.get('password', ''),
                    database=self.connection_params.get('database', 'commodities_db')
                )
            elif self.db_type == 'postgresql':
                import psycopg2
                import psycopg2.extras
                conn = psycopg2.connect(
                    host=self.connection_params.get('host', 'localhost'),
                    port=self.connection_params.get('port', 5432),
                    user=self.connection_params.get('username', 'postgres'),
                    password=self.connection_params.get('password', ''),
                    database=self.connection_params.get('database', 'commodities_db')
                )
            else:
                raise ValueError(f"Unsupported database type: {self.db_type}")
            
            yield conn
            conn.commit()
        except Exception as e:
            if conn:
                conn.rollback()
            logger.error(f"Database connection error: {e}")
            raise
        finally:
            if conn:
                conn.close()
    
    def initialize_schema(self) -> bool:
        """
        Create all required database tables and indexes.
        
        Returns:
            True if successful, False otherwise
        """
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                
                # Adjust schema for different database types
                schemas = [
                    self.PRICE_DATA_SCHEMA,
                    self.COMMODITIES_SCHEMA,
                    self.DATA_QUALITY_LOG_SCHEMA,
                    self.PIPELINE_RUNS_SCHEMA
                ]
                
                for schema in schemas:
                    # Adjust for MySQL/PostgreSQL if needed
                    adjusted_schema = self._adjust_schema_for_db(schema)
                    cursor.execute(adjusted_schema)
                
                # Create indexes
                for index_sql in self.INDEXES:
                    try:
                        cursor.execute(index_sql)
                    except Exception as e:
                        logger.warning(f"Index creation warning: {e}")
                
                conn.commit()
                logger.info("Database schema initialized successfully")
                return True
                
        except Exception as e:
            logger.error(f"Failed to initialize database schema: {e}")
            return False
    
    def _adjust_schema_for_db(self, schema: str) -> str:
        """Adjust SQL schema for different database engines"""
        if self.db_type == 'mysql':
            # MySQL adjustments
            schema = schema.replace('AUTOINCREMENT', 'AUTO_INCREMENT')
            schema = schema.replace('BOOLEAN', 'TINYINT(1)')
        elif self.db_type == 'postgresql':
            # PostgreSQL adjustments
            schema = schema.replace('INTEGER PRIMARY KEY AUTOINCREMENT', 'SERIAL PRIMARY KEY')
            schema = schema.replace('DATETIME', 'TIMESTAMP')
        return schema
    
    def insert_price_records(self, records: List[Dict]) -> InsertResult:
        """
        Insert multiple price records into the database.
        Uses INSERT OR IGNORE (SQLite) or INSERT IGNORE (MySQL) for duplicates.
        
        Args:
            records: List of record dictionaries from the parser
            
        Returns:
            InsertResult with counts and any error messages
        """
        if not records:
            return InsertResult(0, 0, 0, 0, [])
        
        inserted = 0
        skipped = 0
        errors = 0
        error_messages = []
        
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                
                for record in records:
                    try:
                        # Prepare the insert data
                        insert_data = self._prepare_record_for_insert(record)
                        
                        # Build insert statement based on DB type
                        if self.db_type == 'sqlite':
                            sql = """
                            INSERT OR IGNORE INTO price_data 
                            (report_date, commodity, location, price, price_low, price_high, 
                             price_avg, basis, basis_low, basis_high, unit, source_report, 
                             report_type, grade, delivery_period, fetch_timestamp, raw_data)
                            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                            """
                        elif self.db_type == 'mysql':
                            sql = """
                            INSERT IGNORE INTO price_data 
                            (report_date, commodity, location, price, price_low, price_high,
                             price_avg, basis, basis_low, basis_high, unit, source_report,
                             report_type, grade, delivery_period, fetch_timestamp, raw_data)
                            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                            """
                        else:  # postgresql
                            sql = """
                            INSERT INTO price_data 
                            (report_date, commodity, location, price, price_low, price_high,
                             price_avg, basis, basis_low, basis_high, unit, source_report,
                             report_type, grade, delivery_period, fetch_timestamp, raw_data)
                            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                            ON CONFLICT (report_date, commodity, location, source_report) DO NOTHING
                            """
                        
                        cursor.execute(sql, insert_data)
                        
                        # Check if row was actually inserted
                        if cursor.rowcount > 0:
                            inserted += 1
                        else:
                            skipped += 1
                            
                    except Exception as e:
                        errors += 1
                        error_msg = f"Error inserting record: {e}"
                        error_messages.append(error_msg)
                        logger.warning(error_msg)
                
                conn.commit()
                
        except Exception as e:
            error_msg = f"Batch insert failed: {e}"
            error_messages.append(error_msg)
            logger.error(error_msg)
            errors = len(records)
        
        result = InsertResult(
            total_attempted=len(records),
            inserted=inserted,
            skipped=skipped,
            errors=errors,
            error_messages=error_messages
        )
        
        logger.info(f"Insert complete: {inserted} inserted, {skipped} skipped, {errors} errors")
        return result
    
    def _prepare_record_for_insert(self, record: Dict) -> Tuple:
        """
        Prepare a record dictionary for database insertion.
        
        Args:
            record: Raw record from parser
            
        Returns:
            Tuple of values in correct order for INSERT
        """
        # Parse the date
        report_date = record.get('report_date', '')
        if isinstance(report_date, str) and report_date:
            # Try to parse common date formats
            for fmt in ['%m/%d/%Y', '%Y-%m-%d', '%Y%m%d']:
                try:
                    parsed = datetime.strptime(report_date, fmt)
                    report_date = parsed.strftime('%Y-%m-%d')
                    break
                except ValueError:
                    continue
        
        # Get commodity - handle various field names
        commodity = (record.get('commodity') or 
                    record.get('commodity_name') or 
                    record.get('product_type') or
                    'Unknown')
        
        # Get location
        location = (record.get('location') or 
                   record.get('market_location') or 
                   record.get('plant_location') or 
                   'National')
        
        # Get source report name
        source_report = (record.get('source_report') or 
                        record.get('report_name') or 
                        record.get('source') or 
                        'USDA_AMS')
        
        # Store raw data as JSON for debugging
        raw_data = json.dumps(record.get('raw_data', {})) if record.get('raw_data') else None
        
        return (
            report_date,
            str(commodity)[:100],  # Truncate to field length
            str(location)[:150] if location else None,
            record.get('price'),
            record.get('price_low'),
            record.get('price_high'),
            record.get('price_avg'),
            record.get('basis'),
            record.get('basis_low'),
            record.get('basis_high'),
            record.get('unit', ''),
            str(source_report)[:150],
            record.get('report_type', ''),
            record.get('grade', ''),
            record.get('delivery_period', ''),
            record.get('fetch_timestamp', datetime.now().isoformat()),
            raw_data
        )
    
    def get_record_count(self, source_report: str = None, 
                         commodity: str = None,
                         start_date: str = None,
                         end_date: str = None) -> int:
        """
        Get count of records matching the criteria.
        
        Args:
            source_report: Filter by source report name
            commodity: Filter by commodity name
            start_date: Start date (YYYY-MM-DD)
            end_date: End date (YYYY-MM-DD)
            
        Returns:
            Count of matching records
        """
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                
                sql = "SELECT COUNT(*) FROM price_data WHERE 1=1"
                params = []
                
                if source_report:
                    sql += " AND source_report = ?"
                    params.append(source_report)
                if commodity:
                    sql += " AND commodity = ?"
                    params.append(commodity)
                if start_date:
                    sql += " AND report_date >= ?"
                    params.append(start_date)
                if end_date:
                    sql += " AND report_date <= ?"
                    params.append(end_date)
                
                # Adjust placeholders for MySQL/PostgreSQL
                if self.db_type in ['mysql', 'postgresql']:
                    sql = sql.replace('?', '%s')
                
                cursor.execute(sql, params)
                result = cursor.fetchone()
                return result[0] if result else 0
                
        except Exception as e:
            logger.error(f"Error getting record count: {e}")
            return 0
    
    def get_records(self, source_report: str = None,
                    commodity: str = None,
                    start_date: str = None,
                    end_date: str = None,
                    limit: int = 100) -> List[Dict]:
        """
        Retrieve records matching the criteria.
        
        Args:
            source_report: Filter by source report name
            commodity: Filter by commodity name
            start_date: Start date (YYYY-MM-DD)
            end_date: End date (YYYY-MM-DD)
            limit: Maximum records to return
            
        Returns:
            List of record dictionaries
        """
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                
                sql = """
                SELECT id, report_date, commodity, location, price, price_low, price_high,
                       price_avg, basis, unit, source_report, report_type, grade, 
                       delivery_period, fetch_timestamp, created_at
                FROM price_data WHERE 1=1
                """
                params = []
                
                if source_report:
                    sql += " AND source_report = ?"
                    params.append(source_report)
                if commodity:
                    sql += " AND commodity = ?"
                    params.append(commodity)
                if start_date:
                    sql += " AND report_date >= ?"
                    params.append(start_date)
                if end_date:
                    sql += " AND report_date <= ?"
                    params.append(end_date)
                
                sql += " ORDER BY report_date DESC LIMIT ?"
                params.append(limit)
                
                # Adjust for MySQL/PostgreSQL
                if self.db_type in ['mysql', 'postgresql']:
                    sql = sql.replace('?', '%s')
                
                cursor.execute(sql, params)
                rows = cursor.fetchall()
                
                # Convert to dictionaries
                columns = ['id', 'report_date', 'commodity', 'location', 'price', 
                          'price_low', 'price_high', 'price_avg', 'basis', 'unit',
                          'source_report', 'report_type', 'grade', 'delivery_period',
                          'fetch_timestamp', 'created_at']
                
                return [dict(zip(columns, row)) for row in rows]
                
        except Exception as e:
            logger.error(f"Error retrieving records: {e}")
            return []
    
    def get_unique_commodities(self) -> List[str]:
        """Get list of unique commodity names in the database"""
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute("SELECT DISTINCT commodity FROM price_data ORDER BY commodity")
                return [row[0] for row in cursor.fetchall()]
        except Exception as e:
            logger.error(f"Error getting commodities: {e}")
            return []
    
    def get_unique_sources(self) -> List[str]:
        """Get list of unique source report names in the database"""
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute("SELECT DISTINCT source_report FROM price_data ORDER BY source_report")
                return [row[0] for row in cursor.fetchall()]
        except Exception as e:
            logger.error(f"Error getting sources: {e}")
            return []
    
    def get_date_range(self, source_report: str = None) -> Tuple[Optional[str], Optional[str]]:
        """
        Get the min and max dates in the database.
        
        Returns:
            Tuple of (min_date, max_date) or (None, None) if empty
        """
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                
                sql = "SELECT MIN(report_date), MAX(report_date) FROM price_data"
                params = []
                
                if source_report:
                    sql += " WHERE source_report = ?"
                    params.append(source_report)
                    if self.db_type in ['mysql', 'postgresql']:
                        sql = sql.replace('?', '%s')
                
                cursor.execute(sql, params if params else None)
                result = cursor.fetchone()
                
                if result and result[0]:
                    return (str(result[0]), str(result[1]))
                return (None, None)
                
        except Exception as e:
            logger.error(f"Error getting date range: {e}")
            return (None, None)
    
    def log_quality_result(self, source_report: str, records_fetched: int,
                          records_inserted: int, records_skipped: int,
                          records_failed: int, verification_status: str,
                          error_messages: List[str] = None) -> bool:
        """
        Log data quality metrics for a pipeline run.
        """
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                
                sql = """
                INSERT INTO data_quality_log 
                (run_timestamp, source_report, records_fetched, records_inserted,
                 records_skipped, records_failed, verification_status, error_messages)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """
                
                if self.db_type in ['mysql', 'postgresql']:
                    sql = sql.replace('?', '%s')
                
                cursor.execute(sql, (
                    datetime.now().isoformat(),
                    source_report,
                    records_fetched,
                    records_inserted,
                    records_skipped,
                    records_failed,
                    verification_status,
                    json.dumps(error_messages) if error_messages else None
                ))
                
                conn.commit()
                return True
                
        except Exception as e:
            logger.error(f"Error logging quality result: {e}")
            return False
    
    def get_statistics(self) -> Dict[str, Any]:
        """
        Get database statistics summary.
        
        Returns:
            Dictionary with database statistics
        """
        stats = {
            'total_records': 0,
            'unique_commodities': 0,
            'unique_sources': 0,
            'date_range': (None, None),
            'commodities': [],
            'sources': []
        }
        
        try:
            stats['total_records'] = self.get_record_count()
            stats['commodities'] = self.get_unique_commodities()
            stats['unique_commodities'] = len(stats['commodities'])
            stats['sources'] = self.get_unique_sources()
            stats['unique_sources'] = len(stats['sources'])
            stats['date_range'] = self.get_date_range()
        except Exception as e:
            logger.error(f"Error getting statistics: {e}")
        
        return stats


# Factory function for creating database agent from config
def create_database_agent(settings=None) -> DatabaseAgent:
    """
    Factory function to create a DatabaseAgent from settings.
    
    Args:
        settings: Settings object (uses global if None)
        
    Returns:
        Configured DatabaseAgent instance
    """
    if settings is None:
        from config.settings import get_settings
        settings = get_settings()
    
    connection_params = {
        'database': settings.database.sqlite_path if settings.database.db_type == 'sqlite' else settings.database.database,
        'host': settings.database.host,
        'port': settings.database.port,
        'username': settings.database.username,
        'password': settings.database.password
    }
    
    return DatabaseAgent(
        db_type=settings.database.db_type,
        connection_params=connection_params
    )