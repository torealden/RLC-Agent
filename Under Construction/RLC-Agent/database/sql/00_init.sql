-- ============================================================================
-- Round Lakes Commodities - Database Initialization
-- ============================================================================
-- File: 00_init.sql
-- Purpose: Initialize database with required extensions and base settings
-- Execute: First, as superuser
-- ============================================================================

-- ----------------------------------------------------------------------------
-- Extensions
-- ----------------------------------------------------------------------------

-- UUID generation for potential future use
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- Cryptographic functions (for checksums if needed)
CREATE EXTENSION IF NOT EXISTS "pgcrypto";

-- Statistical aggregates (useful for time-series analysis)
CREATE EXTENSION IF NOT EXISTS "pg_stat_statements";

-- Optional: PostGIS for geographic data (uncomment if needed)
-- CREATE EXTENSION IF NOT EXISTS "postgis";

-- Optional: TimescaleDB for time-series optimization (uncomment if using)
-- CREATE EXTENSION IF NOT EXISTS "timescaledb";

-- ----------------------------------------------------------------------------
-- Database-level settings (apply via ALTER DATABASE or postgresql.conf)
-- ----------------------------------------------------------------------------

-- These are recommendations; apply as appropriate for your environment:

-- Memory settings for ~32GB RAM server
-- shared_buffers = '8GB'
-- effective_cache_size = '24GB'
-- work_mem = '256MB'
-- maintenance_work_mem = '2GB'

-- WAL settings for durability
-- wal_level = 'replica'  -- Enables point-in-time recovery
-- archive_mode = 'on'
-- archive_command = 'cp %p /var/lib/postgresql/wal_archive/%f'
-- max_wal_size = '4GB'
-- min_wal_size = '1GB'

-- Connection settings
-- max_connections = 50  -- Low because we use pgBouncer
-- idle_in_transaction_session_timeout = '5min'

-- Autovacuum tuning for mixed workload
-- autovacuum_vacuum_scale_factor = 0.05
-- autovacuum_analyze_scale_factor = 0.02
-- autovacuum_vacuum_cost_limit = 1000

-- Logging for debugging
-- log_min_duration_statement = 1000  -- Log queries over 1 second
-- log_checkpoints = on
-- log_lock_waits = on

-- ----------------------------------------------------------------------------
-- Helpful comment
-- ----------------------------------------------------------------------------
COMMENT ON DATABASE current_database() IS
    'Round Lakes Commodities - Multi-agent commodity analytics platform';
