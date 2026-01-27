-- ============================================================================
-- Round Lakes Commodities - Operational Guidance & Server Configuration
-- ============================================================================
-- File: 99_operational.sql
-- Purpose: Server settings, pgBouncer config, backup scripts, monitoring queries
-- This file contains reference configurations - not for direct execution
-- ============================================================================

-- ============================================================================
-- POSTGRESQL SERVER CONFIGURATION (postgresql.conf)
-- ============================================================================
-- Recommended settings for a local server with 32GB RAM running ~10 agents

/*
# Memory Settings
shared_buffers = '8GB'              # 25% of RAM
effective_cache_size = '24GB'       # 75% of RAM
work_mem = '256MB'                  # Per-operation memory
maintenance_work_mem = '2GB'        # For VACUUM, CREATE INDEX
huge_pages = 'try'                  # Better memory management

# Write-Ahead Log (WAL)
wal_level = 'replica'               # Enables point-in-time recovery
archive_mode = 'on'
archive_command = 'cp %p /var/lib/postgresql/wal_archive/%f'
max_wal_size = '4GB'
min_wal_size = '1GB'
wal_compression = 'on'

# Checkpoints
checkpoint_completion_target = 0.9
checkpoint_timeout = '15min'

# Connection Settings
max_connections = 50                # Low because pgBouncer handles pooling
superuser_reserved_connections = 3

# Timeouts
idle_in_transaction_session_timeout = '5min'
statement_timeout = '5min'          # Default; override per role

# Autovacuum (tuned for mixed workload)
autovacuum = on
autovacuum_max_workers = 4
autovacuum_naptime = '30s'
autovacuum_vacuum_threshold = 50
autovacuum_vacuum_scale_factor = 0.05   # More aggressive
autovacuum_analyze_threshold = 50
autovacuum_analyze_scale_factor = 0.02
autovacuum_vacuum_cost_limit = 1000

# Logging
log_destination = 'csvlog'
logging_collector = on
log_directory = '/var/log/postgresql'
log_filename = 'postgresql-%Y-%m-%d.log'
log_min_duration_statement = 1000   # Log queries > 1 second
log_checkpoints = on
log_connections = on
log_disconnections = on
log_lock_waits = on
log_temp_files = 0

# Statistics
track_io_timing = on
track_functions = 'all'

# Parallelism
max_parallel_workers_per_gather = 4
max_parallel_workers = 8
max_parallel_maintenance_workers = 4
*/


-- ============================================================================
-- PGBOUNCER CONFIGURATION (pgbouncer.ini)
-- ============================================================================
-- Transaction pooling for ~10 concurrent agents

/*
[databases]
rlc = host=127.0.0.1 port=5432 dbname=rlc

[pgbouncer]
listen_addr = 127.0.0.1
listen_port = 6432
auth_type = md5
auth_file = /etc/pgbouncer/userlist.txt

# Pool settings
pool_mode = transaction              # Release connection after each transaction
max_client_conn = 100                # Max client connections to pgBouncer
default_pool_size = 20               # Connections per database/user pair
min_pool_size = 5                    # Keep some connections warm
reserve_pool_size = 5                # Extra connections for burst

# Timeouts
server_idle_timeout = 300            # Close idle server connections after 5 min
client_idle_timeout = 0              # Don't timeout idle clients
query_timeout = 300                  # 5 min query timeout

# Limits
max_db_connections = 30              # Max connections to actual database
max_user_connections = 30

# Logging
log_connections = 1
log_disconnections = 1
log_pooler_errors = 1
stats_period = 60

# Admin
admin_users = rlc_admin
stats_users = rlc_admin
*/

-- pgBouncer userlist.txt format:
-- "username" "md5_password_hash"
-- To generate: SELECT 'md5' || md5(password || username);


-- ============================================================================
-- BACKUP STRATEGY
-- ============================================================================

-- Daily base backup script (run via cron at 2 AM):
/*
#!/bin/bash
# /opt/rlc/scripts/backup_daily.sh

BACKUP_DIR=/var/backups/postgresql
DATE=$(date +%Y%m%d)
PGHOST=127.0.0.1
PGPORT=5432
PGDATABASE=rlc
PGUSER=rlc_admin

# Create daily backup directory
mkdir -p $BACKUP_DIR/daily

# Full base backup
pg_basebackup -h $PGHOST -p $PGPORT -U $PGUSER -D $BACKUP_DIR/daily/base_$DATE \
    -Ft -z -P --checkpoint=fast

# Logical backup for portability
pg_dump -h $PGHOST -p $PGPORT -U $PGUSER -d $PGDATABASE \
    -Fc -f $BACKUP_DIR/daily/rlc_$DATE.dump

# Cleanup backups older than 7 days
find $BACKUP_DIR/daily -mtime +7 -delete

# Log success
echo "$(date): Backup completed successfully" >> /var/log/rlc_backup.log
*/

-- Weekly logical backup by schema (for selective restore):
/*
#!/bin/bash
# /opt/rlc/scripts/backup_weekly.sh

BACKUP_DIR=/var/backups/postgresql/weekly
DATE=$(date +%Y%m%d)

for SCHEMA in core audit bronze silver gold; do
    pg_dump -h 127.0.0.1 -U rlc_admin -d rlc -n $SCHEMA \
        -Fc -f $BACKUP_DIR/${SCHEMA}_$DATE.dump
done

# Cleanup backups older than 30 days
find $BACKUP_DIR -mtime +30 -delete
*/

-- Restore procedures:
/*
# Restore from base backup:
pg_restore -h 127.0.0.1 -U rlc_admin -d rlc -c /path/to/backup.dump

# Restore single schema:
pg_restore -h 127.0.0.1 -U rlc_admin -d rlc -n silver /path/to/silver_backup.dump

# Point-in-time recovery:
# 1. Stop PostgreSQL
# 2. Replace data directory with base backup
# 3. Create recovery.signal file
# 4. Set recovery_target_time in postgresql.conf
# 5. Start PostgreSQL
*/


-- ============================================================================
-- MONITORING QUERIES
-- ============================================================================

-- 1. Connection status
CREATE OR REPLACE VIEW gold.monitor_connections AS
SELECT
    datname AS database,
    usename AS username,
    application_name,
    client_addr,
    state,
    COUNT(*) AS connection_count,
    MAX(NOW() - backend_start) AS oldest_connection
FROM pg_stat_activity
WHERE datname = current_database()
GROUP BY datname, usename, application_name, client_addr, state
ORDER BY connection_count DESC;

COMMENT ON VIEW gold.monitor_connections IS
    'Current connection status by user and state.';


-- 2. Long-running queries
CREATE OR REPLACE VIEW gold.monitor_slow_queries AS
SELECT
    pid,
    usename AS username,
    application_name,
    client_addr,
    state,
    EXTRACT(EPOCH FROM NOW() - query_start)::INTEGER AS duration_seconds,
    LEFT(query, 200) AS query_preview,
    wait_event_type,
    wait_event
FROM pg_stat_activity
WHERE datname = current_database()
  AND state != 'idle'
  AND query_start < NOW() - INTERVAL '10 seconds'
ORDER BY duration_seconds DESC;

COMMENT ON VIEW gold.monitor_slow_queries IS
    'Queries running longer than 10 seconds.';


-- 3. Table sizes and bloat
CREATE OR REPLACE VIEW gold.monitor_table_sizes AS
SELECT
    schemaname AS schema,
    tablename AS table_name,
    pg_size_pretty(pg_total_relation_size(schemaname || '.' || tablename)) AS total_size,
    pg_size_pretty(pg_table_size(schemaname || '.' || tablename)) AS table_size,
    pg_size_pretty(pg_indexes_size(schemaname || '.' || tablename)) AS index_size,
    n_live_tup AS live_rows,
    n_dead_tup AS dead_rows,
    CASE WHEN n_live_tup > 0
        THEN ROUND(100.0 * n_dead_tup / n_live_tup, 2)
        ELSE 0
    END AS dead_ratio_pct,
    last_vacuum,
    last_autovacuum,
    last_analyze
FROM pg_stat_user_tables
ORDER BY pg_total_relation_size(schemaname || '.' || tablename) DESC;

COMMENT ON VIEW gold.monitor_table_sizes IS
    'Table sizes with vacuum/analyze stats. High dead_ratio indicates bloat.';


-- 4. Index usage
CREATE OR REPLACE VIEW gold.monitor_index_usage AS
SELECT
    schemaname AS schema,
    tablename AS table_name,
    indexname AS index_name,
    idx_scan AS scans,
    idx_tup_read AS tuples_read,
    idx_tup_fetch AS tuples_fetched,
    pg_size_pretty(pg_relation_size(indexrelid)) AS index_size,
    CASE WHEN idx_scan = 0 THEN 'UNUSED' ELSE 'USED' END AS status
FROM pg_stat_user_indexes
ORDER BY idx_scan DESC;

COMMENT ON VIEW gold.monitor_index_usage IS
    'Index usage statistics. UNUSED indexes may be candidates for removal.';


-- 5. WAL and replication status
CREATE OR REPLACE VIEW gold.monitor_wal_status AS
SELECT
    pg_current_wal_lsn() AS current_lsn,
    pg_walfile_name(pg_current_wal_lsn()) AS current_wal_file,
    pg_size_pretty(pg_wal_lsn_diff(pg_current_wal_lsn(), '0/0')) AS total_wal_size,
    (SELECT COUNT(*) FROM pg_ls_waldir()) AS wal_file_count
;

COMMENT ON VIEW gold.monitor_wal_status IS
    'Current WAL position and file count.';


-- 6. Disk space
CREATE OR REPLACE VIEW gold.monitor_disk_space AS
SELECT
    pg_size_pretty(pg_database_size(current_database())) AS database_size,
    pg_size_pretty(SUM(pg_total_relation_size(schemaname || '.' || tablename)))
        AS total_table_size
FROM pg_stat_user_tables;

COMMENT ON VIEW gold.monitor_disk_space IS
    'Current database disk usage.';


-- 7. Ingest run status
CREATE OR REPLACE VIEW gold.monitor_ingest_runs AS
SELECT
    ds.code AS data_source,
    ir.job_name,
    ir.status,
    ir.started_at,
    ir.completed_at,
    ir.duration_seconds,
    ir.records_fetched,
    ir.records_inserted,
    ir.records_failed,
    ir.error_message
FROM audit.ingest_run ir
JOIN core.data_source ds ON ir.data_source_id = ds.id
WHERE ir.started_at > NOW() - INTERVAL '24 hours'
ORDER BY ir.started_at DESC;

COMMENT ON VIEW gold.monitor_ingest_runs IS
    'Ingest runs from the last 24 hours.';


-- 8. Validation status summary
CREATE OR REPLACE VIEW gold.monitor_validation_status AS
SELECT
    ds.code AS data_source,
    vs.entity_type,
    vs.status,
    COUNT(*) AS count,
    MAX(vs.validated_at) AS last_validated
FROM audit.validation_status vs
JOIN core.data_source ds ON vs.data_source_id = ds.id
GROUP BY ds.code, vs.entity_type, vs.status
ORDER BY ds.code, vs.entity_type, vs.status;

COMMENT ON VIEW gold.monitor_validation_status IS
    'Summary of validation status by source and entity type.';


-- 9. Agent health
CREATE OR REPLACE VIEW gold.monitor_agent_health AS
SELECT
    agent_id,
    agent_type,
    status,
    current_task,
    last_heartbeat,
    EXTRACT(EPOCH FROM NOW() - last_heartbeat) / 60 AS minutes_since_heartbeat,
    CASE
        WHEN last_heartbeat > NOW() - INTERVAL '2 minutes' THEN 'HEALTHY'
        WHEN last_heartbeat > NOW() - INTERVAL '10 minutes' THEN 'WARNING'
        ELSE 'STALE'
    END AS health_status
FROM audit.agent_heartbeat
ORDER BY last_heartbeat DESC;

COMMENT ON VIEW gold.monitor_agent_health IS
    'Agent health status based on heartbeat recency.';


-- ============================================================================
-- MAINTENANCE PROCEDURES
-- ============================================================================

-- Scheduled maintenance (run weekly):
CREATE OR REPLACE PROCEDURE maintenance_weekly()
LANGUAGE plpgsql
AS $$
BEGIN
    -- Refresh series statistics
    PERFORM silver.refresh_series_stats();

    -- Analyze key tables
    ANALYZE core.series;
    ANALYZE silver.observation;
    ANALYZE bronze.wasde_cell;
    ANALYZE audit.ingest_run;

    RAISE NOTICE 'Weekly maintenance completed at %', NOW();
END;
$$;


-- Clean up old audit records (run monthly):
CREATE OR REPLACE PROCEDURE maintenance_cleanup_audit(
    p_retention_days INTEGER DEFAULT 90
)
LANGUAGE plpgsql
AS $$
DECLARE
    v_cutoff TIMESTAMPTZ := NOW() - (p_retention_days || ' days')::INTERVAL;
    v_deleted INTEGER;
BEGIN
    -- Delete old ingest errors
    DELETE FROM audit.ingest_error
    WHERE error_time < v_cutoff AND is_resolved = TRUE;
    GET DIAGNOSTICS v_deleted = ROW_COUNT;
    RAISE NOTICE 'Deleted % resolved ingest errors older than % days', v_deleted, p_retention_days;

    -- Delete old raw payloads (keeping reference in ingest_run)
    DELETE FROM audit.raw_payload
    WHERE created_at < v_cutoff;
    GET DIAGNOSTICS v_deleted = ROW_COUNT;
    RAISE NOTICE 'Deleted % raw payloads older than % days', v_deleted, p_retention_days;

    -- Delete old agent heartbeats
    DELETE FROM audit.agent_heartbeat
    WHERE last_heartbeat < v_cutoff;
    GET DIAGNOSTICS v_deleted = ROW_COUNT;
    RAISE NOTICE 'Deleted % stale agent heartbeats', v_deleted;
END;
$$;


-- ============================================================================
-- CLOUD MIGRATION CHECKLIST
-- ============================================================================

/*
MIGRATION TO CLOUD (AWS RDS / GCP Cloud SQL / Azure Database)

Pre-Migration:
1. [ ] Inventory all extensions used (uuid-ossp, pgcrypto, pg_stat_statements)
2. [ ] Verify cloud provider supports required extensions
3. [ ] Document current postgresql.conf settings
4. [ ] Export current roles and grants
5. [ ] Create logical backup with pg_dump

Migration Steps:
1. [ ] Create cloud database instance with matching PostgreSQL version
2. [ ] Configure VPC/security groups for agent access
3. [ ] Import schema using pg_dump/pg_restore
4. [ ] Create roles (07_roles_grants.sql) - passwords will differ
5. [ ] Import data
6. [ ] Verify row counts match
7. [ ] Update connection strings in all agents
8. [ ] Test with read-only workload first
9. [ ] Switch write workload
10. [ ] Monitor for 24 hours

Post-Migration:
1. [ ] Enable automated backups
2. [ ] Configure monitoring (CloudWatch, Stackdriver, etc.)
3. [ ] Set up alerts for disk, CPU, connections
4. [ ] Review and tune cloud-specific parameters
5. [ ] Decommission local database

Connection String Changes:
- Local: postgresql://user:pass@localhost:6432/rlc
- Cloud: postgresql://user:pass@your-instance.region.rds.amazonaws.com:5432/rlc

Note: pgBouncer may be replaced by cloud-native pooling (RDS Proxy, etc.)
*/


-- ============================================================================
-- CRON SCHEDULE REFERENCE
-- ============================================================================

/*
# Example crontab entries for RLC database maintenance

# Daily backup at 2:00 AM
0 2 * * * /opt/rlc/scripts/backup_daily.sh >> /var/log/rlc_backup.log 2>&1

# Weekly backup at 3:00 AM Sunday
0 3 * * 0 /opt/rlc/scripts/backup_weekly.sh >> /var/log/rlc_backup.log 2>&1

# Weekly maintenance at 4:00 AM Sunday
0 4 * * 0 psql -U rlc_admin -d rlc -c "CALL maintenance_weekly();"

# Monthly cleanup on 1st at 5:00 AM
0 5 1 * * psql -U rlc_admin -d rlc -c "CALL maintenance_cleanup_audit(90);"

# Hourly stats refresh
0 * * * * psql -U rlc_admin -d rlc -c "SELECT silver.refresh_series_stats();"
*/


-- ============================================================================
-- ALERTING THRESHOLDS
-- ============================================================================

-- Create a function to check system health and return alerts
CREATE OR REPLACE FUNCTION gold.check_system_health()
RETURNS TABLE (
    alert_level VARCHAR(10),
    alert_type VARCHAR(50),
    message TEXT
) AS $$
BEGIN
    -- Check connection count
    IF (SELECT COUNT(*) FROM pg_stat_activity WHERE datname = current_database()) > 40 THEN
        alert_level := 'WARNING';
        alert_type := 'connections';
        message := 'High connection count: ' ||
            (SELECT COUNT(*) FROM pg_stat_activity WHERE datname = current_database());
        RETURN NEXT;
    END IF;

    -- Check for long-running queries
    IF EXISTS (
        SELECT 1 FROM pg_stat_activity
        WHERE datname = current_database()
          AND state != 'idle'
          AND query_start < NOW() - INTERVAL '5 minutes'
    ) THEN
        alert_level := 'WARNING';
        alert_type := 'long_query';
        message := 'Queries running longer than 5 minutes detected';
        RETURN NEXT;
    END IF;

    -- Check for failed ingest runs in last hour
    IF (
        SELECT COUNT(*) FROM audit.ingest_run
        WHERE status = 'failed' AND started_at > NOW() - INTERVAL '1 hour'
    ) > 0 THEN
        alert_level := 'ERROR';
        alert_type := 'ingest_failure';
        message := 'Failed ingest runs in the last hour: ' ||
            (SELECT COUNT(*) FROM audit.ingest_run WHERE status = 'failed' AND started_at > NOW() - INTERVAL '1 hour');
        RETURN NEXT;
    END IF;

    -- Check for stale agents
    IF EXISTS (
        SELECT 1 FROM audit.agent_heartbeat
        WHERE last_heartbeat < NOW() - INTERVAL '10 minutes'
          AND status = 'alive'
    ) THEN
        alert_level := 'WARNING';
        alert_type := 'stale_agent';
        message := 'Agents with stale heartbeats detected';
        RETURN NEXT;
    END IF;

    -- Check for unvalidated releases older than 24 hours
    IF EXISTS (
        SELECT 1 FROM bronze.wasde_release
        WHERE is_validated = FALSE
          AND created_at < NOW() - INTERVAL '24 hours'
    ) THEN
        alert_level := 'WARNING';
        alert_type := 'pending_validation';
        message := 'WASDE releases pending validation for more than 24 hours';
        RETURN NEXT;
    END IF;

    RETURN;
END;
$$ LANGUAGE plpgsql;

COMMENT ON FUNCTION gold.check_system_health IS
    'Check system health and return any active alerts.';


-- ============================================================================
-- Verification
-- ============================================================================
DO $$
BEGIN
    RAISE NOTICE 'Operational guidance created:';
    RAISE NOTICE '  - PostgreSQL configuration recommendations';
    RAISE NOTICE '  - pgBouncer configuration template';
    RAISE NOTICE '  - Backup scripts and procedures';
    RAISE NOTICE '  - Monitoring views: gold.monitor_*';
    RAISE NOTICE '  - Maintenance procedures';
    RAISE NOTICE '  - Cloud migration checklist';
    RAISE NOTICE '  - System health check function';
END $$;
