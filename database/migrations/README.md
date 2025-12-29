# Database Migrations

This directory contains versioned migration scripts for the RLC database.

## Naming Convention

```
YYYYMMDD_HHMMSS_description.sql
```

Example:
```
20240115_120000_add_weather_tables.sql
```

## Migration Best Practices

1. **Idempotent**: Migrations should be safe to run multiple times
2. **Reversible**: Include rollback statements where possible
3. **Tested**: Test on a copy of production data first
4. **Small**: Keep migrations focused on a single change
5. **Documented**: Include comments explaining the change

## Template

```sql
-- ============================================================================
-- Migration: [DESCRIPTION]
-- Date: [YYYY-MM-DD]
-- Author: [NAME]
-- ============================================================================
-- Description:
--   [What this migration does]
--
-- Rollback:
--   [How to undo this migration]
-- ============================================================================

-- Check preconditions (optional)
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM core.series WHERE series_key = 'example') THEN
        RAISE EXCEPTION 'Precondition failed: expected series not found';
    END IF;
END $$;

-- Forward migration
ALTER TABLE core.series ADD COLUMN IF NOT EXISTS new_column VARCHAR(100);

-- Update data
UPDATE core.series SET new_column = 'default' WHERE new_column IS NULL;

-- Rollback (commented out, for reference)
-- ALTER TABLE core.series DROP COLUMN IF EXISTS new_column;

-- Verification
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = 'core' AND table_name = 'series' AND column_name = 'new_column'
    ) THEN
        RAISE EXCEPTION 'Migration verification failed: new_column not found';
    END IF;
    RAISE NOTICE 'Migration completed successfully';
END $$;
```

## Running Migrations

```bash
# Run a specific migration
psql -U postgres -d rlc -f migrations/20240115_120000_add_weather_tables.sql

# Run all migrations in order
for f in migrations/*.sql; do
    echo "Running $f..."
    psql -U postgres -d rlc -f "$f"
done
```

## Migration Tracking

Consider using a migration tracking table:

```sql
CREATE TABLE IF NOT EXISTS audit.schema_migrations (
    id SERIAL PRIMARY KEY,
    filename VARCHAR(255) NOT NULL UNIQUE,
    applied_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    applied_by VARCHAR(100) DEFAULT current_user
);

-- Before running a migration:
-- 1. Check if already applied
-- 2. Run migration
-- 3. Record in schema_migrations
```
