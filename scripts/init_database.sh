#!/bin/bash
#
# Initialize MLB Data Platform Database
# Creates schemas, tables, indexes, and materialized views
#
# Usage:
#   ./scripts/init_database.sh [environment]
#
# Examples:
#   ./scripts/init_database.sh           # Local (docker compose)
#   ./scripts/init_database.sh dev       # Dev K8s cluster
#   ./scripts/init_database.sh prod      # Prod K8s cluster

set -e  # Exit on error
set -u  # Exit on undefined variable

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Configuration
ENVIRONMENT="${1:-local}"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
SQL_DIR="${PROJECT_ROOT}/sql/migrations"
POSTGRES_VERSION="15"  # Match K3d cluster version

echo -e "${BLUE}========================================${NC}"
echo -e "${BLUE}MLB Data Platform - Database Initialization${NC}"
echo -e "${BLUE}========================================${NC}"
echo ""
echo -e "Environment: ${GREEN}${ENVIRONMENT}${NC}"
echo -e "PostgreSQL Client: ${GREEN}${POSTGRES_VERSION}-alpine (Docker)${NC}"
echo ""

# Helper function to run psql in Docker container
psql_docker() {
    docker run --rm -i \
        -e PGHOST="${PGHOST}" \
        -e PGPORT="${PGPORT}" \
        -e PGUSER="${PGUSER}" \
        -e PGPASSWORD="${PGPASSWORD}" \
        -e PGDATABASE="${PGDATABASE}" \
        --network host \
        postgres:${POSTGRES_VERSION}-alpine \
        psql "$@"
}

# Helper function to run psql with file input
psql_docker_file() {
    local file="$1"
    docker run --rm -i \
        -e PGHOST="${PGHOST}" \
        -e PGPORT="${PGPORT}" \
        -e PGUSER="${PGUSER}" \
        -e PGPASSWORD="${PGPASSWORD}" \
        -e PGDATABASE="${PGDATABASE}" \
        --network host \
        -v "${file}:${file}:ro" \
        postgres:${POSTGRES_VERSION}-alpine \
        psql -f "${file}"
}

# Set PostgreSQL connection based on environment
case "${ENVIRONMENT}" in
    local)
        PGHOST="localhost"
        PGPORT="65254"
        PGUSER="mlb_admin"
        PGPASSWORD="mlb_admin_password"
        PGDATABASE="mlb_games"
        echo -e "Target: ${YELLOW}Local K3d Cluster (port 65254)${NC}"
        ;;
    dev)
        PGHOST="postgresql.mlb-data-platform.svc.cluster.local"
        PGPORT="5432"
        PGUSER="mlb_admin"
        PGPASSWORD="${MLB_POSTGRES_PASSWORD:-}"
        PGDATABASE="mlb_games"
        echo -e "Target: ${YELLOW}Dev Kubernetes Cluster${NC}"
        ;;
    prod)
        PGHOST="postgresql.mlb-data-platform.svc.cluster.local"
        PGPORT="5432"
        PGUSER="mlb_admin"
        PGPASSWORD="${MLB_POSTGRES_PASSWORD:-}"
        PGDATABASE="mlb_games"
        echo -e "Target: ${YELLOW}Prod Kubernetes Cluster${NC}"
        ;;
    *)
        echo -e "${RED}ERROR: Unknown environment: ${ENVIRONMENT}${NC}"
        echo "Valid environments: local, dev, prod"
        exit 1
        ;;
esac

# Check for password
if [ -z "${PGPASSWORD}" ]; then
    echo -e "${RED}ERROR: PGPASSWORD not set${NC}"
    echo "For dev/prod, set MLB_POSTGRES_PASSWORD environment variable"
    exit 1
fi

# Export for psql
export PGHOST PGPORT PGUSER PGPASSWORD PGDATABASE

# Test connection
echo -e "${BLUE}Testing database connection...${NC}"
if ! psql_docker -c "SELECT version();" > /dev/null 2>&1; then
    echo -e "${RED}ERROR: Cannot connect to PostgreSQL${NC}"
    echo "Host: ${PGHOST}:${PGPORT}"
    echo "Database: ${PGDATABASE}"
    echo "User: ${PGUSER}"
    exit 1
fi
echo -e "${GREEN}✓ Connected successfully${NC}"
echo ""

# Check if schema already exists
echo -e "${BLUE}Checking existing schema...${NC}"
EXISTING_SCHEMAS=$(psql_docker -t -c "SELECT schema_name FROM information_schema.schemata WHERE schema_name IN ('metadata', 'season', 'schedule', 'game');")

if [ -n "${EXISTING_SCHEMAS}" ]; then
    echo -e "${YELLOW}WARNING: Found existing schemas:${NC}"
    echo "${EXISTING_SCHEMAS}"
    echo ""
    read -p "Drop and recreate? This will DELETE ALL DATA. (yes/no): " confirm
    if [ "${confirm}" != "yes" ]; then
        echo -e "${YELLOW}Aborted.${NC}"
        exit 0
    fi

    echo -e "${YELLOW}Dropping existing schemas...${NC}"
    psql <<EOF
        DROP SCHEMA IF EXISTS game CASCADE;
        DROP SCHEMA IF EXISTS schedule CASCADE;
        DROP SCHEMA IF EXISTS season CASCADE;
        DROP SCHEMA IF EXISTS metadata CASCADE;
EOF
    echo -e "${GREEN}✓ Existing schemas dropped${NC}"
fi

# Run migrations
echo ""
echo -e "${BLUE}Running migrations...${NC}"

# Get list of migration files
MIGRATIONS=$(find "${SQL_DIR}" -name "V*.sql" | sort)

if [ -z "${MIGRATIONS}" ]; then
    echo -e "${RED}ERROR: No migration files found in ${SQL_DIR}${NC}"
    exit 1
fi

# Run each migration
for migration in ${MIGRATIONS}; do
    filename=$(basename "${migration}")
    echo -e "Running: ${YELLOW}${filename}${NC}"

    if psql_docker_file "${migration}" > /dev/null 2>&1; then
        echo -e "${GREEN}  ✓ Success${NC}"
    else
        echo -e "${RED}  ✗ Failed${NC}"
        echo ""
        echo -e "${RED}ERROR: Migration failed: ${filename}${NC}"
        echo "Check logs above for details"
        exit 1
    fi
done

echo ""
echo -e "${GREEN}✓ All migrations completed successfully${NC}"

# Verify schema
echo ""
echo -e "${BLUE}Verifying schema...${NC}"

# Count tables
TABLE_COUNT=$(psql_docker -t -c "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema IN ('metadata', 'season', 'schedule', 'game') AND table_type = 'BASE TABLE';")
VIEW_COUNT=$(psql_docker -t -c "SELECT COUNT(*) FROM information_schema.views WHERE table_schema IN ('metadata', 'season', 'schedule', 'game');")

echo -e "Tables created: ${GREEN}${TABLE_COUNT}${NC}"
echo -e "Views created: ${GREEN}${VIEW_COUNT}${NC}"

# List schemas
echo ""
echo -e "${BLUE}Schemas:${NC}"
psql_docker -c "SELECT schema_name FROM information_schema.schemata WHERE schema_name IN ('metadata', 'season', 'schedule', 'game') ORDER BY schema_name;"

# List tables
echo ""
echo -e "${BLUE}Tables:${NC}"
psql_docker -c "SELECT table_schema, table_name FROM information_schema.tables WHERE table_schema IN ('metadata', 'season', 'schedule', 'game') AND table_type = 'BASE TABLE' ORDER BY table_schema, table_name;"

# Summary
echo ""
echo -e "${BLUE}========================================${NC}"
echo -e "${GREEN}Database initialization complete!${NC}"
echo -e "${BLUE}========================================${NC}"
echo ""
echo -e "${YELLOW}Next steps:${NC}"
echo "  1. Run ingestion job:"
echo "     ${GREEN}uv run mlb-etl ingest --job config/jobs/season_daily.yaml${NC}"
echo ""
echo "  2. Query data:"
echo "     ${GREEN}psql -h ${PGHOST} -U ${PGUSER} -d ${PGDATABASE}${NC}"
echo ""
echo "  3. Refresh materialized views:"
echo "     ${GREEN}psql -c 'SELECT metadata.refresh_all_views();'${NC}"
echo ""

# Save connection info
cat > "${PROJECT_ROOT}/.db_connection_info" <<EOF
# Database connection info for ${ENVIRONMENT}
# Source this file: source .db_connection_info

export PGHOST="${PGHOST}"
export PGPORT="${PGPORT}"
export PGUSER="${PGUSER}"
export PGDATABASE="${PGDATABASE}"
# export PGPASSWORD="<set this manually>"

# Quick connect:
# psql
EOF

echo -e "${BLUE}Connection info saved to:${NC} ${PROJECT_ROOT}/.db_connection_info"
echo ""
