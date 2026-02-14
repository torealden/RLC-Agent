-- =============================================================================
-- RLC Commodities Database - Operational Guidance
-- Version: 1.0.0
-- =============================================================================
--
-- This file contains configuration recommendations, monitoring queries,
-- and operational procedures for running PostgreSQL in production.
--
-- =============================================================================

-- =============================================================================
-- POSTGRESQL.CONF RECOMMENDATIONS
-- =============================================================================

-- Add these settings to postgresql.conf for optimal performance:

/*
# =============================================================================
# MEMORY SETTINGS
# =============================================================================
# Adjust based on available RAM (assuming 16GB system, ~4GB for PostgreSQL)

shared_buffers = 1GB                    # 25% of available RAM for PG
effective_cache_size = 3GB              # 75% of available RAM
work_mem = 64MB                         # Per-operation memory (careful with concurrent queries)
maintenance_work_mem = 256MB            # For VACUUM, CREATE INDEX

# =============================================================================
# WRITE-AHEAD LOG (WAL)
# =============================================================================

wal_level = replica                     # Enables point-in-time recovery
max_wal_size = 2GB                      # Before checkpoint
min_wal_size = 256MB
checkpoint_completion_target = 0.9

# =============================================================================
# CONNECTIONS
# =============================================================================

max_connections = 50                    # Plenty for ~10 concurrent agents
# Consider pgBouncer for connection pooling in production

# =============================================================================
# AUTOVACUUM
# =============================================================================

autovacuum = on
autovacuum_max_workers = 3
autovacuum_naptime = 1min
autovacuum_vacuum_threshold = 50
autovacuum_analyze_threshold = 50
autovacuum_vacuum_scale_factor = 0.1    # Vacuum after 10% of table changes
autovacuum_analyze_scale_factor = 0.05  # Analyze after 5% changes

# =============================================================================
# LOGGING
# =============================================================================

log_destination = 'stderr'
logging_collector = on
log_directory = 'log'
log_filename = 'postgresql-%Y-%m-%d.log'
log_rotation_age = 1d
log_rotation_size = 100MB

log_min_duration_statement = 1000       # Log queries taking > 1 second
log_checkpoints = on
log_connections = on
log_disconnections = on
log_lock_waits = on
log_temp_files = 0

# =============================================================================
# SECURITY
# =============================================================================

ssl = on                                # Require SSL for remote connections
password_encryption = scram-sha-256

*/

-- =============================================================================
-- PGBOUNCER CONFIGURATION (Recommended)
-- =============================================================================

-- pgBouncer provides connection pooling, essential for multiple agents.
-- Install: sudo apt install pgbouncer (Linux) or use installer (Windows)

/*
# /etc/pgbouncer/pgbouncer.ini

[databases]
rlc_commodities = host=127.0.0.1 port=5432 dbname=rlc_commodities

[pgbouncer]
listen_addr = 127.0.0.1
listen_port = 6432
auth_type = scram-sha-256
auth_file = /etc/pgbouncer/userlist.txt

# Pool settings
pool_mode = transaction          # Best for short queries
default_pool_size = 20
max_client_conn = 100
max_db_connections = 30

# Timeouts
query_timeout = 300              # 5 minutes max query time
client_idle_timeout = 600        # 10 minutes idle disconnect

# Logging
log_connections = 1
log_disconnections = 1
log_pooler_errors = 1
stats_period = 60

# /etc/pgbouncer/userlist.txt
# Format: "username" "password_hash"
# Generate hash: SELECT rolname, rolpassword FROM pg_authid WHERE rolname = 'ingest_writer';
*/

-- =============================================================================
-- BACKUP STRATEGY
-- =============================================================================

-- Daily base backup + WAL archiving for point-in-time recovery

/*
# 1. Enable WAL archiving in postgresql.conf:

archive_mode = on
archive_command = 'cp %p /var/lib/postgresql/wal_archive/%f'

# 2. Create backup script (/opt/scripts/pg_backup.sh):

#!/bin/bash
BACKUP_DIR="/var/backups/postgresql"
DATE=$(date +%Y%m%d_%H%M%S)
DB_NAME="rlc_commodities"

# Base backup
pg_dump -Fc -f "$BACKUP_DIR/${DB_NAME}_${DATE}.dump" $DB_NAME

# Keep only last 7 days
find $BACKUP_DIR -name "*.dump" -mtime +7 -delete

# 3. Schedule with cron (run at 2 AM daily):
# 0 2 * * * /opt/scripts/pg_backup.sh

# 4. Restore procedure:
pg_restore -d rlc_commodities_restored /var/backups/postgresql/rlc_commodities_20250101_020000.dump
*/

-- =============================================================================
-- MONITORING QUERIES
-- =============================================================================

-- -----------------------------------------------------------------------------
-- Active connections by role
-- -----------------------------------------------------------------------------
-- SELECT
--     usename,
--     application_name,
--     client_addr,
--     state,
--     query_start,
--     NOW() - query_start AS query_duration,
--     LEFT(query, 100) AS query_preview
-- FROM pg_stat_activity
-- WHERE datname = 'rlc_commodities'
-- ORDER BY query_start;

-- -----------------------------------------------------------------------------
-- Table sizes
-- -----------------------------------------------------------------------------
-- SELECT
--     schemaname,
--     tablename,
--     pg_size_pretty(pg_total_relation_size(schemaname || '.' || tablename)) AS total_size,
--     pg_size_pretty(pg_relation_size(schemaname || '.' || tablename)) AS table_size,
--     pg_size_pretty(pg_indexes_size(schemaname || '.' || tablename)) AS index_size
-- FROM pg_tables
-- WHERE schemaname IN ('bronze', 'silver', 'gold', 'audit')
-- ORDER BY pg_total_relation_size(schemaname || '.' || tablename) DESC;

-- -----------------------------------------------------------------------------
-- Slow queries (requires pg_stat_statements extension)
-- -----------------------------------------------------------------------------
-- CREATE EXTENSION IF NOT EXISTS pg_stat_statements;
--
-- SELECT
--     LEFT(query, 100) AS query_preview,
--     calls,
--     ROUND(total_exec_time::NUMERIC, 2) AS total_ms,
--     ROUND(mean_exec_time::NUMERIC, 2) AS mean_ms,
--     rows
-- FROM pg_stat_statements
-- ORDER BY total_exec_time DESC
-- LIMIT 20;

-- -----------------------------------------------------------------------------
-- Index usage (find unused indexes)
-- -----------------------------------------------------------------------------
-- SELECT
--     schemaname,
--     tablename,
--     indexname,
--     idx_scan AS times_used,
--     pg_size_pretty(pg_relation_size(indexrelid)) AS index_size
-- FROM pg_stat_user_indexes
-- WHERE idx_scan = 0
-- ORDER BY pg_relation_size(indexrelid) DESC;

-- -----------------------------------------------------------------------------
-- Bloat estimation (tables needing VACUUM)
-- -----------------------------------------------------------------------------
-- SELECT
--     schemaname,
--     relname,
--     n_dead_tup,
--     n_live_tup,
--     ROUND(100 * n_dead_tup::NUMERIC / NULLIF(n_live_tup + n_dead_tup, 0), 2) AS dead_pct,
--     last_vacuum,
--     last_autovacuum
-- FROM pg_stat_user_tables
-- WHERE n_dead_tup > 1000
-- ORDER BY n_dead_tup DESC;

-- =============================================================================
-- MAINTENANCE PROCEDURES
-- =============================================================================

-- Weekly maintenance (run during low-activity periods):
--
-- -- Reclaim space and update statistics
-- VACUUM ANALYZE bronze.census_trade_raw;
-- VACUUM ANALYZE bronze.fgis_inspection_raw;
-- VACUUM ANALYZE silver.observation;
--
-- -- Rebuild bloated indexes (if needed)
-- REINDEX TABLE CONCURRENTLY silver.observation;

-- =============================================================================
-- MIGRATION TO CLOUD
-- =============================================================================

-- This schema is designed for easy migration to:
-- - AWS RDS PostgreSQL
-- - Azure Database for PostgreSQL
-- - Google Cloud SQL
-- - Any managed PostgreSQL service

-- Migration steps:
-- 1. Create cloud database instance (PostgreSQL 14+)
-- 2. Export schema: pg_dump -s -f schema.sql rlc_commodities
-- 3. Export data: pg_dump -a -Fc -f data.dump rlc_commodities
-- 4. Apply schema to cloud: psql -f schema.sql cloud_db_url
-- 5. Restore data: pg_restore -d cloud_db_url data.dump
-- 6. Update connection strings in .env files
-- 7. Validate with reconciliation views

-- Cloud-specific considerations:
-- - RDS: Use IAM authentication instead of passwords
-- - Azure: Consider Azure Active Directory integration
-- - All: Enable SSL, configure VPC/firewall rules

-- =============================================================================
-- DISASTER RECOVERY
-- =============================================================================

-- Recovery Time Objective (RTO): 1 hour (restore from backup)
-- Recovery Point Objective (RPO): 24 hours (daily backups)

-- For lower RPO, implement:
-- 1. WAL shipping to standby server
-- 2. Logical replication to secondary
-- 3. Cloud-native backups with PITR (point-in-time recovery)

-- =============================================================================
-- HEALTH CHECK VIEW
-- =============================================================================

CREATE OR REPLACE VIEW gold.system_health AS
SELECT
    'Database Size' AS metric,
    pg_size_pretty(pg_database_size('rlc_commodities')) AS value
UNION ALL
SELECT
    'Active Connections',
    COUNT(*)::TEXT
FROM pg_stat_activity
WHERE datname = 'rlc_commodities'
UNION ALL
SELECT
    'Bronze Tables',
    COUNT(*)::TEXT
FROM information_schema.tables
WHERE table_schema = 'bronze'
UNION ALL
SELECT
    'Total Observations',
    TO_CHAR(COUNT(*), 'FM999,999,999')
FROM silver.observation
UNION ALL
SELECT
    'Latest Ingest',
    COALESCE(MAX(started_at)::TEXT, 'Never')
FROM audit.ingest_run;

COMMENT ON VIEW gold.system_health IS 'Quick health check for the database';

-- =============================================================================
-- END OF OPERATIONAL GUIDANCE
-- =============================================================================
