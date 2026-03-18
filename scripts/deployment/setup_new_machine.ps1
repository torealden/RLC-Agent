# =============================================================================
# RLC-Agent New Machine Setup Script
# =============================================================================
# Run this script in PowerShell (as Administrator) to set up a fresh machine
# for RLC-Agent development and analysis.
#
# Usage:
#   1. Open PowerShell as Administrator
#   2. Set-ExecutionPolicy -Scope CurrentUser -ExecutionPolicy RemoteSigned
#   3. .\setup_new_machine.ps1
#
# What this script does:
#   - Installs Git, Python 3.11, Node.js (via winget)
#   - Clones the RLC-Agent repo from GitHub
#   - Installs Python dependencies
#   - Creates the .env file from template
#   - Configures the MCP server for Claude Code
#   - Installs pgAdmin 4 and PostgreSQL ODBC driver
# =============================================================================

$ErrorActionPreference = "Stop"

# Configuration
$REPO_URL = "https://github.com/torealden/RLC-Agent.git"
$DEV_DIR = "C:\dev"
$PROJECT_DIR = "$DEV_DIR\rlc-agent"

Write-Host "`n========================================" -ForegroundColor Cyan
Write-Host "  RLC-Agent New Machine Setup" -ForegroundColor Cyan
Write-Host "========================================`n" -ForegroundColor Cyan

# -------------------------------------------------------------------------
# Step 1: Check/Install Prerequisites
# -------------------------------------------------------------------------
Write-Host "[Step 1/7] Checking prerequisites..." -ForegroundColor Yellow

# Check winget
if (-not (Get-Command winget -ErrorAction SilentlyContinue)) {
    Write-Host "  ERROR: winget not found. Please install App Installer from the Microsoft Store." -ForegroundColor Red
    exit 1
}

# Git
if (-not (Get-Command git -ErrorAction SilentlyContinue)) {
    Write-Host "  Installing Git..." -ForegroundColor Gray
    winget install --id Git.Git -e --accept-source-agreements --accept-package-agreements
    $env:Path += ";C:\Program Files\Git\bin"
    Write-Host "  Git installed." -ForegroundColor Green
} else {
    Write-Host "  Git: $(git --version)" -ForegroundColor Green
}

# Python 3.11
$pythonCmd = Get-Command python -ErrorAction SilentlyContinue
if (-not $pythonCmd -or -not ((python --version 2>&1) -match "3\.11")) {
    Write-Host "  Installing Python 3.11..." -ForegroundColor Gray
    winget install --id Python.Python.3.11 -e --accept-source-agreements --accept-package-agreements
    Write-Host "  Python 3.11 installed. You may need to restart PowerShell." -ForegroundColor Green
} else {
    Write-Host "  Python: $(python --version)" -ForegroundColor Green
}

# Node.js (for Claude Code and MCP)
if (-not (Get-Command node -ErrorAction SilentlyContinue)) {
    Write-Host "  Installing Node.js LTS..." -ForegroundColor Gray
    winget install --id OpenJS.NodeJS.LTS -e --accept-source-agreements --accept-package-agreements
    Write-Host "  Node.js installed." -ForegroundColor Green
} else {
    Write-Host "  Node.js: $(node --version)" -ForegroundColor Green
}

Write-Host ""

# -------------------------------------------------------------------------
# Step 2: Clone Repository
# -------------------------------------------------------------------------
Write-Host "[Step 2/7] Cloning repository..." -ForegroundColor Yellow

if (-not (Test-Path $DEV_DIR)) {
    New-Item -ItemType Directory -Path $DEV_DIR | Out-Null
    Write-Host "  Created $DEV_DIR" -ForegroundColor Gray
}

if (Test-Path "$PROJECT_DIR\.git") {
    Write-Host "  Repository already exists. Pulling latest..." -ForegroundColor Gray
    Push-Location $PROJECT_DIR
    git pull origin main
    Pop-Location
} elseif (Test-Path $PROJECT_DIR) {
    Write-Host "  WARNING: $PROJECT_DIR exists but is not a git repo." -ForegroundColor Red
    Write-Host "  Rename it and re-run, or delete it manually:" -ForegroundColor Red
    Write-Host "    Rename-Item $PROJECT_DIR rlc-agent-backup" -ForegroundColor Gray
    exit 1
} else {
    git clone $REPO_URL $PROJECT_DIR
    Write-Host "  Cloned to $PROJECT_DIR" -ForegroundColor Green
}

Push-Location $PROJECT_DIR
Write-Host ""

# -------------------------------------------------------------------------
# Step 3: Install Python Dependencies
# -------------------------------------------------------------------------
Write-Host "[Step 3/7] Installing Python dependencies..." -ForegroundColor Yellow

python -m pip install --upgrade pip --quiet
python -m pip install -r requirements.txt --quiet
Write-Host "  Dependencies installed." -ForegroundColor Green
Write-Host ""

# -------------------------------------------------------------------------
# Step 4: Create .env file
# -------------------------------------------------------------------------
Write-Host "[Step 4/7] Setting up environment..." -ForegroundColor Yellow

if (Test-Path ".env") {
    Write-Host "  .env already exists. Skipping." -ForegroundColor Gray
} else {
    if (Test-Path ".env.template") {
        Copy-Item ".env.template" ".env"
        Write-Host "  Created .env from template. Edit it with your API keys." -ForegroundColor Green
    } else {
        # Create minimal .env
        @"
# =============================================================================
# RLC-Agent Environment Variables
# =============================================================================

# Database - AWS RDS (shared team database)
DB_TYPE=postgresql
RLC_PG_HOST=rlc-commodities.c16c6wm826t7.us-east-2.rds.amazonaws.com
RLC_PG_PORT=5432
RLC_PG_DATABASE=rlc_commodities
RLC_PG_USER=postgres
RLC_PG_PASSWORD=SoupBoss1
RLC_PG_SSLMODE=require

# Legacy aliases (some scripts use these)
DB_HOST=localhost
DB_PORT=5432
DB_NAME=rlc_commodities
DB_USER=postgres
DATABASE_URL=postgresql://postgres:SoupBoss1@rlc-commodities.c16c6wm826t7.us-east-2.rds.amazonaws.com:5432/rlc_commodities

# USDA APIs (get keys from Tore)
# USDA_FAS_API_KEY=
# USDA_NASS_API_KEY=

# EIA (get key from https://www.eia.gov/opendata/register.php)
# EIA_API_KEY=

# Census Bureau (get key from https://api.census.gov/data/key_signup.html)
# CENSUS_API_KEY=
"@ | Set-Content ".env" -Encoding UTF8
        Write-Host "  Created minimal .env. Ask Tore for API keys." -ForegroundColor Yellow
    }
}

Write-Host ""

# -------------------------------------------------------------------------
# Step 5: Configure Claude Code MCP Server
# -------------------------------------------------------------------------
Write-Host "[Step 5/7] Configuring Claude Code MCP server..." -ForegroundColor Yellow

$pythonPath = (Get-Command python).Source
$mcpServerPath = "$PROJECT_DIR\src\mcp\commodities_db_server.py"

# Create .mcp.json (project-level MCP config)
$mcpConfig = @{
    mcpServers = @{
        "commodities-db" = @{
            command = $pythonPath
            args = @($mcpServerPath)
            env = @{
                "RLC_PG_HOST" = "rlc-commodities.c16c6wm826t7.us-east-2.rds.amazonaws.com"
                "RLC_PG_PORT" = "5432"
                "RLC_PG_DATABASE" = "rlc_commodities"
                "RLC_PG_USER" = "postgres"
                "RLC_PG_PASSWORD" = "SoupBoss1"
                "RLC_PG_SSLMODE" = "require"
            }
        }
    }
}

# Write to a user-local file (not committed to git)
$mcpLocalPath = "$PROJECT_DIR\.mcp.json"
$mcpConfig | ConvertTo-Json -Depth 4 | Set-Content $mcpLocalPath -Encoding UTF8
Write-Host "  MCP config written to .mcp.json" -ForegroundColor Green
Write-Host "  Python path: $pythonPath" -ForegroundColor Gray
Write-Host ""

# -------------------------------------------------------------------------
# Step 6: Install Claude Code
# -------------------------------------------------------------------------
Write-Host "[Step 6/7] Installing Claude Code..." -ForegroundColor Yellow

if (-not (Get-Command claude -ErrorAction SilentlyContinue)) {
    npm install -g @anthropic-ai/claude-code
    Write-Host "  Claude Code installed." -ForegroundColor Green
    Write-Host "  Run 'claude' in the project directory to start." -ForegroundColor Gray
} else {
    Write-Host "  Claude Code already installed: $(claude --version 2>&1)" -ForegroundColor Green
}

Write-Host ""

# -------------------------------------------------------------------------
# Step 7: Install Database Tools (optional)
# -------------------------------------------------------------------------
Write-Host "[Step 7/7] Database tools (optional)..." -ForegroundColor Yellow

# pgAdmin
if (-not (Get-Command pgAdmin4 -ErrorAction SilentlyContinue)) {
    $installPgAdmin = Read-Host "  Install pgAdmin 4 for database browsing? (y/n)"
    if ($installPgAdmin -eq 'y') {
        winget install --id PostgreSQL.pgAdmin -e --accept-source-agreements --accept-package-agreements
        Write-Host "  pgAdmin 4 installed." -ForegroundColor Green
    }
} else {
    Write-Host "  pgAdmin 4: already installed" -ForegroundColor Green
}

# PostgreSQL ODBC (for Excel VBA macros)
$odbcInstalled = Get-OdbcDriver -Name "*PostgreSQL*" -ErrorAction SilentlyContinue
if (-not $odbcInstalled) {
    $installOdbc = Read-Host "  Install PostgreSQL ODBC driver for Excel? (y/n)"
    if ($installOdbc -eq 'y') {
        Write-Host "  Download from: https://ftp.postgresql.org/pub/odbc/releases/" -ForegroundColor Gray
        Write-Host "  Install the 64-bit .msi (psqlodbc_16_00_0005-x64.zip or similar)" -ForegroundColor Gray
        Start-Process "https://ftp.postgresql.org/pub/odbc/releases/"
    }
} else {
    Write-Host "  PostgreSQL ODBC: already installed" -ForegroundColor Green
}

Pop-Location

# -------------------------------------------------------------------------
# Done
# -------------------------------------------------------------------------
Write-Host "`n========================================" -ForegroundColor Cyan
Write-Host "  Setup Complete!" -ForegroundColor Green
Write-Host "========================================" -ForegroundColor Cyan
Write-Host ""
Write-Host "Next steps:" -ForegroundColor Yellow
Write-Host "  1. Edit .env with any missing API keys (ask Tore)" -ForegroundColor White
Write-Host "  2. Test database connection:" -ForegroundColor White
Write-Host "     cd $PROJECT_DIR" -ForegroundColor Gray
Write-Host "     python -c `"from src.services.database.db_config import get_connection; c=get_connection(); print('Connected!'); c.close()`"" -ForegroundColor Gray
Write-Host "  3. Start Claude Code:" -ForegroundColor White
Write-Host "     cd $PROJECT_DIR" -ForegroundColor Gray
Write-Host "     claude" -ForegroundColor Gray
Write-Host "  4. In Claude Code, try:" -ForegroundColor White
Write-Host "     'What is the latest CFTC positioning for corn?'" -ForegroundColor Gray
Write-Host ""
Write-Host "For help: docs\analyst_database_setup_guide.docx" -ForegroundColor Gray
Write-Host ""
