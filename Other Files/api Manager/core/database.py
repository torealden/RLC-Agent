"""
Database management with automatic schema creation and migration
Supports both SQLite (local) and PostgreSQL (server/cloud)
"""
from sqlalchemy import create_engine, MetaData, Table, Column, Integer, String, Float, Date, DateTime, Text, Boolean
from sqlalchemy.orm import sessionmaker, declarative_base
from sqlalchemy.pool import NullPool, StaticPool
from sqlalchemy import text, inspect
from datetime import datetime
from typing import Dict, Any, List, Optional
import pandas as pd
from contextlib import contextmanager
import logging

logger = logging.getLogger(__name__)

Base = declarative_base()

class DatabaseManager:
    """
    Manages database connections, schema creation, and CRUD operations
    Works with SQLite for local development and PostgreSQL for production
    """
    
    def __init__(self, connection_string: str, db_type: str = 'sqlite'):
        self.connection_string = connection_string
        self.db_type = db_type
        self.engine = None
        self.metadata = MetaData()
        self.Session = None
        
    def connect(self):
        """Establish database connection"""
        try:
            if self.db_type == 'sqlite':
                self.engine = create_engine(
                    self.connection_string,
                    connect_args={'check_same_thread': False},  # Allow multi-threading
                    poolclass=StaticPool,
                    echo=False
                )
            else:  # PostgreSQL
                self.engine = create_engine(
                    self.connection_string,
                    poolclass=NullPool,
                    echo=False
                )
            
            self.Session = sessionmaker(bind=self.engine)
            
            # Test connection
            with self.engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            
            logger.info(f"Database connection established ({self.db_type})")
            return True
            
        except Exception as e:
            logger.error(f"Database connection failed: {e}")
            return False
    
    @contextmanager
    def get_session(self):
        """Context manager for database sessions"""
        session = self.Session()
        try:
            yield session
            session.commit()
        except Exception as e:
            session.rollback()
            logger.error(f"Database session error: {e}")
            raise
        finally:
            session.close()
    
    def table_exists(self, table_name: str) -> bool:
        """Check if a table exists in the database"""
        inspector = inspect(self.engine)
        return table_name in inspector.get_table_names()
    
    def create_table(self, table_name: str, columns: Dict[str, Any], 
                     primary_key: Optional[List[str]] = None,
                     indexes: Optional[List[str]] = None):
        """
        Dynamically create a table with specified columns
        
        Args:
            table_name: Name of the table
            columns: Dict of {column_name: column_type}
            primary_key: List of column names to use as primary key
            indexes: List of column names to index
        """
        try:
            if self.table_exists(table_name):
                logger.info(f"Table '{table_name}' already exists")
                return True
            
            # Build column list
            col_objects = []
            for col_name, col_def in columns.items():
                if isinstance(col_def, Column):
                    col_objects.append(col_def)
                else:
                    col_objects.append(Column(col_name, col_def))
            
            # Add ID column if no primary key specified
            if not primary_key:
                col_objects.insert(0, Column('id', Integer, primary_key=True, autoincrement=True))
            
            # Create table
            table = Table(table_name, self.metadata, *col_objects)
            table.create(self.engine)
            
            logger.info(f"Table '{table_name}' created successfully")
            return True
            
        except Exception as e:
            logger.error(f"Failed to create table '{table_name}': {e}")
            return False
    
    def upsert_dataframe(self, df: pd.DataFrame, table_name: str, 
                         unique_columns: List[str], 
                         update_timestamp: bool = True):
        """
        Insert or update DataFrame into database table
        Handles both SQLite and PostgreSQL syntax
        
        Args:
            df: DataFrame to upsert
            table_name: Target table name
            unique_columns: Columns that define uniqueness (for conflict resolution)
            update_timestamp: Whether to add/update an 'updated_at' timestamp
        """
        try:
            if df.empty:
                logger.warning(f"Empty DataFrame provided for table '{table_name}'")
                return False
            
            # Add timestamp if requested
            if update_timestamp and 'updated_at' not in df.columns:
                df['updated_at'] = datetime.now()
            
            # Convert DataFrame to list of dicts
            records = df.to_dict('records')
            
            with self.engine.connect() as conn:
                for record in records:
                    columns = list(record.keys())
                    values_placeholder = ', '.join([f":{k}" for k in columns])
                    
                    if self.db_type == 'sqlite':
                        # SQLite uses INSERT OR REPLACE
                        stmt = text(f"""
                            INSERT OR REPLACE INTO {table_name} ({', '.join(columns)})
                            VALUES ({values_placeholder})
                        """)
                    else:  # PostgreSQL
                        # PostgreSQL uses ON CONFLICT
                        update_clause = ', '.join([f"{k}=EXCLUDED.{k}" for k in columns if k not in unique_columns])
                        stmt = text(f"""
                            INSERT INTO {table_name} ({', '.join(columns)})
                            VALUES ({values_placeholder})
                            ON CONFLICT ({', '.join(unique_columns)})
                            DO UPDATE SET {update_clause}
                        """)
                    
                    conn.execute(stmt, record)
                conn.commit()
            
            logger.info(f"Upserted {len(records)} records into '{table_name}'")
            return True
            
        except Exception as e:
            logger.error(f"Failed to upsert data into '{table_name}': {e}")
            return False
    
    def execute_query(self, query: str, params: Optional[Dict] = None) -> pd.DataFrame:
        """Execute a SQL query and return results as DataFrame"""
        try:
            with self.engine.connect() as conn:
                result = pd.read_sql(text(query), conn, params=params)
            return result
        except Exception as e:
            logger.error(f"Query execution failed: {e}")
            return pd.DataFrame()
    
    def initialize_usda_tables(self):
        """Initialize tables for USDA AMS data"""
        
        # Main prices table
        self.create_table(
            'usda_ams_prices',
            {
                'report_date': Date,
                'report_id': String(100),
                'commodity': String(100),
                'location': String(200),
                'price': Float,
                'price_unit': String(50),
                'basis': Float,
                'basis_month': String(20),
                'volume': Float,
                'updated_at': DateTime,
            }
        )
        
        # Reports metadata table
        self.create_table(
            'usda_ams_reports',
            {
                'report_id': String(100),
                'slug_id': String(100),
                'report_title': Text,
                'report_date': Date,
                'publish_date': DateTime,
                'office': String(200),
                'report_type': String(100),
                'raw_data': Text,
                'fetched_at': DateTime,
            }
        )
    
    def initialize_trade_tables(self):
        """Initialize tables for trade flow data"""
        
        self.create_table(
            'trade_flows',
            {
                'trade_date': Date,
                'country_code': String(10),
                'commodity_code': String(50),
                'trade_type': String(20),  # 'import' or 'export'
                'partner_country': String(10),
                'quantity': Float,
                'quantity_unit': String(20),
                'value_usd': Float,
                'data_source': String(50),
                'updated_at': DateTime,
            }
        )
    
    def initialize_all_tables(self):
        """Initialize all required tables"""
        logger.info("Initializing database tables...")
        self.initialize_usda_tables()
        self.initialize_trade_tables()
        # Add other table initialization methods as needed
        logger.info("Database tables initialized")
    
    def migrate_to_postgres(self, postgres_connection_string: str):
        """
        Migrate data from SQLite to PostgreSQL
        
        Args:
            postgres_connection_string: Connection string for target PostgreSQL database
        """
        if self.db_type != 'sqlite':
            logger.error("Can only migrate FROM SQLite")
            return False
        
        try:
            # Create new PostgreSQL database manager
            pg_manager = DatabaseManager(postgres_connection_string, 'postgresql')
            if not pg_manager.connect():
                return False
            
            # Get all table names from SQLite
            inspector = inspect(self.engine)
            tables = inspector.get_table_names()
            
            logger.info(f"Migrating {len(tables)} tables from SQLite to PostgreSQL...")
            
            for table_name in tables:
                # Read data from SQLite
                df = self.execute_query(f"SELECT * FROM {table_name}")
                
                if df.empty:
                    logger.info(f"Table '{table_name}' is empty, skipping")
                    continue
                
                # Write to PostgreSQL
                df.to_sql(table_name, pg_manager.engine, if_exists='replace', index=False)
                logger.info(f"Migrated table '{table_name}' ({len(df)} rows)")
            
            logger.info("Migration completed successfully")
            return True
            
        except Exception as e:
            logger.error(f"Migration failed: {e}")
            return False
    
    def close(self):
        """Close database connection"""
        if self.engine:
            self.engine.dispose()
            logger.info("Database connection closed")