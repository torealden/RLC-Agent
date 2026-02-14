#!/bin/bash
# ============================================================================
# Round Lakes Commodities - Database Installation Script
# ============================================================================
# Usage: ./install.sh [database_name] [postgres_user]
#
# Examples:
#   ./install.sh                    # Uses defaults: rlc, postgres
#   ./install.sh rlc_dev            # Custom database name
#   ./install.sh rlc_dev myuser     # Custom database and user
# ============================================================================

set -e  # Exit on error

# Configuration
DB_NAME="${1:-rlc}"
PG_USER="${2:-postgres}"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SQL_DIR="$SCRIPT_DIR/sql"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

echo "=============================================="
echo "Round Lakes Commodities Database Installation"
echo "=============================================="
echo ""
echo "Database: $DB_NAME"
echo "User: $PG_USER"
echo "SQL Directory: $SQL_DIR"
echo ""

# Check if psql is available
if ! command -v psql &> /dev/null; then
    echo -e "${RED}Error: psql command not found. Please install PostgreSQL client.${NC}"
    exit 1
fi

# Check if SQL directory exists
if [ ! -d "$SQL_DIR" ]; then
    echo -e "${RED}Error: SQL directory not found at $SQL_DIR${NC}"
    exit 1
fi

# Function to run a SQL file
run_sql_file() {
    local file="$1"
    local filename=$(basename "$file")

    if [ -f "$file" ]; then
        echo -n "  Running $filename... "
        if psql -U "$PG_USER" -d "$DB_NAME" -f "$file" -q 2>&1 | grep -q "ERROR"; then
            echo -e "${RED}FAILED${NC}"
            return 1
        else
            echo -e "${GREEN}OK${NC}"
            return 0
        fi
    else
        echo -e "${YELLOW}  Skipping $filename (not found)${NC}"
        return 0
    fi
}

# Check if database exists
echo "Checking database..."
if psql -U "$PG_USER" -lqt | cut -d \| -f 1 | grep -qw "$DB_NAME"; then
    echo -e "${YELLOW}Database '$DB_NAME' already exists.${NC}"
    read -p "Drop and recreate? (y/N): " confirm
    if [ "$confirm" = "y" ] || [ "$confirm" = "Y" ]; then
        echo "Dropping database..."
        dropdb -U "$PG_USER" "$DB_NAME"
        echo "Creating database..."
        createdb -U "$PG_USER" "$DB_NAME"
    fi
else
    echo "Creating database '$DB_NAME'..."
    createdb -U "$PG_USER" "$DB_NAME"
fi

echo ""
echo "Installing database schema..."
echo ""

# Run SQL files in order
files=(
    "00_init.sql"
    "01_schemas.sql"
    "02_core_dimensions.sql"
    "03_audit_tables.sql"
    "04_bronze_wasde.sql"
    "05_silver_observation.sql"
    "06_gold_views.sql"
    "07_roles_grants.sql"
    "08_functions.sql"
)

errors=0
for file in "${files[@]}"; do
    if ! run_sql_file "$SQL_DIR/$file"; then
        ((errors++))
    fi
done

echo ""

# Optional: Run sample DML
read -p "Run sample DML patterns (09_sample_dml.sql)? (y/N): " run_samples
if [ "$run_samples" = "y" ] || [ "$run_samples" = "Y" ]; then
    run_sql_file "$SQL_DIR/09_sample_dml.sql"
fi

echo ""

# Summary
if [ $errors -eq 0 ]; then
    echo -e "${GREEN}=============================================="
    echo "Installation completed successfully!"
    echo "==============================================${NC}"
    echo ""
    echo "Next steps:"
    echo "  1. Create user accounts (see 07_roles_grants.sql)"
    echo "  2. Configure pgBouncer (see 99_operational.sql)"
    echo "  3. Set up backup scripts (see 99_operational.sql)"
    echo ""
    echo "Quick verification:"
    echo "  psql -U $PG_USER -d $DB_NAME -c '\\dn'"
    echo ""
else
    echo -e "${RED}=============================================="
    echo "Installation completed with $errors error(s)"
    echo "==============================================${NC}"
    echo ""
    echo "Please check the error messages above and fix any issues."
    exit 1
fi
