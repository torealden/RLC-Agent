# RLC-Agent Setup Script for RLC-SERVER
# Run this script on RLC-SERVER (Windows 11) as Administrator
#
# Prerequisites:
# - RLC-SERVER setup complete (Ollama running, folder structure in place)
# - Git installed
# - Python 3.11+ via uv installed
#
# Usage:
#   Right-click PowerShell -> Run as Administrator
#   cd C:\RLC\projects
#   .\setup_rlc_server.ps1

$ErrorActionPreference = "Stop"

# Configuration
$RLC_ROOT = "C:\RLC"
$PROJECTS_DIR = "$RLC_ROOT\projects"
$AGENT_DIR = "$PROJECTS_DIR\rlc-agent"
$REPO_URL = "https://github.com/torealden/RLC-Agent.git"
$BRANCH = "main"  # or "claude/organize-rlc-scripts-EQDsh" for development

Write-Host ""
Write-Host "============================================================" -ForegroundColor Cyan
Write-Host "        RLC-Agent Setup for RLC-SERVER" -ForegroundColor Cyan
Write-Host "============================================================" -ForegroundColor Cyan
Write-Host ""

# 1. Verify Ollama is running
Write-Host "[1/7] Checking Ollama service..." -ForegroundColor Yellow
try {
    $response = Invoke-RestMethod -Uri "http://localhost:11434/api/tags" -TimeoutSec 5
    $modelCount = $response.models.Count
    Write-Host "  OK - Ollama running with $modelCount models" -ForegroundColor Green
} catch {
    Write-Host "  ERROR - Ollama not responding. Start OllamaLLM service first." -ForegroundColor Red
    Write-Host "  Run: nssm start OllamaLLM" -ForegroundColor Yellow
    exit 1
}

# 2. Create projects directory
Write-Host "[2/7] Creating directory structure..." -ForegroundColor Yellow
if (-not (Test-Path $PROJECTS_DIR)) {
    New-Item -ItemType Directory -Force -Path $PROJECTS_DIR | Out-Null
}
Write-Host "  OK - $PROJECTS_DIR" -ForegroundColor Green

# 3. Clone or update repository
Write-Host "[3/7] Setting up RLC-Agent repository..." -ForegroundColor Yellow
if (Test-Path "$AGENT_DIR\.git") {
    Write-Host "  Repository exists, pulling latest..." -ForegroundColor White
    Push-Location $AGENT_DIR
    git fetch origin
    git checkout $BRANCH
    git pull origin $BRANCH
    Pop-Location
} else {
    Write-Host "  Cloning repository..." -ForegroundColor White
    git clone $REPO_URL $AGENT_DIR
    Push-Location $AGENT_DIR
    git checkout $BRANCH
    Pop-Location
}
Write-Host "  OK - Repository ready at $AGENT_DIR" -ForegroundColor Green

# 4. Setup Python environment
Write-Host "[4/7] Setting up Python environment..." -ForegroundColor Yellow
Push-Location $AGENT_DIR

# Check if uv is available
$uvPath = Get-Command uv -ErrorAction SilentlyContinue
if (-not $uvPath) {
    Write-Host "  Installing uv package manager..." -ForegroundColor White
    powershell -ExecutionPolicy ByPass -c "irm https://astral.sh/uv/install.ps1 | iex"
    $env:Path = [System.Environment]::GetEnvironmentVariable("Path","Machine") + ";" + [System.Environment]::GetEnvironmentVariable("Path","User")
}

# Create virtual environment
if (-not (Test-Path ".venv")) {
    uv venv
}

# Activate and install dependencies
.\.venv\Scripts\Activate.ps1

# Create requirements.txt if it doesn't exist
if (-not (Test-Path "requirements.txt")) {
    @"
# Core dependencies
aiohttp>=3.9.0
httpx>=0.27.0
pandas>=2.0.0
numpy>=1.24.0
python-dotenv>=1.0.0

# Database
sqlalchemy>=2.0.0
psycopg2-binary>=2.9.0  # PostgreSQL
pymysql>=1.1.0          # MySQL

# API clients
requests>=2.31.0
beautifulsoup4>=4.12.0
lxml>=5.0.0

# Scheduling
apscheduler>=3.10.0

# Office document generation
python-docx>=1.0.0

# Cloud integrations (optional)
dropbox>=11.0.0

# Development
pytest>=7.0.0
pytest-asyncio>=0.21.0
"@ | Out-File -FilePath "requirements.txt" -Encoding UTF8
}

Write-Host "  Installing dependencies..." -ForegroundColor White
uv pip install -r requirements.txt

Pop-Location
Write-Host "  OK - Python environment ready" -ForegroundColor Green

# 5. Create .env file from template
Write-Host "[5/7] Configuring environment..." -ForegroundColor Yellow
$envFile = "$AGENT_DIR\.env"
$envExample = "$AGENT_DIR\.env.example"

if (-not (Test-Path $envFile)) {
    if (Test-Path $envExample) {
        Copy-Item $envExample $envFile
        Write-Host "  Created .env from template" -ForegroundColor White
        Write-Host "  IMPORTANT: Edit $envFile with your API keys" -ForegroundColor Yellow
    } else {
        @"
# RLC-Agent Environment Configuration
# Copy this file to .env and fill in your values

# Local LLM (Ollama)
OLLAMA_URL=http://localhost:11434
OLLAMA_MODEL=qwen2.5:7b

# Database (SQLite default, PostgreSQL recommended for production)
DATABASE_URL=sqlite:///./data/rlc_commodities.db
# DATABASE_URL=postgresql://rlc_user:password@localhost:5432/rlc_commodities

# USDA API Keys
NASS_API_KEY=your_nass_api_key_here
USDA_AMS_API_KEY=your_ams_api_key_here

# Energy Information Administration
EIA_API_KEY=your_eia_api_key_here

# Optional - Cloud integrations
# DROPBOX_ACCESS_TOKEN=
# DROPBOX_REFRESH_TOKEN=
# DROPBOX_APP_KEY=
# DROPBOX_APP_SECRET=
"@ | Out-File -FilePath $envFile -Encoding UTF8
        Write-Host "  Created default .env file" -ForegroundColor White
        Write-Host "  IMPORTANT: Edit $envFile with your API keys" -ForegroundColor Yellow
    }
} else {
    Write-Host "  .env already exists" -ForegroundColor Green
}

# 6. Initialize database
Write-Host "[6/7] Initializing database..." -ForegroundColor Yellow
Push-Location $AGENT_DIR

# Create data directory
New-Item -ItemType Directory -Force -Path "data" | Out-Null

# Run database initialization if script exists
$dbInitScript = "scripts/init_database.py"
if (Test-Path $dbInitScript) {
    .\.venv\Scripts\python.exe $dbInitScript
    Write-Host "  OK - Database initialized" -ForegroundColor Green
} else {
    Write-Host "  SKIP - Database init script not found" -ForegroundColor Yellow
}
Pop-Location

# 7. Create Windows service for the Master Agent (optional)
Write-Host "[7/7] Setting up Master Agent service..." -ForegroundColor Yellow

$serviceName = "RLCMasterAgent"
$existingService = Get-Service -Name $serviceName -ErrorAction SilentlyContinue

if ($existingService) {
    Write-Host "  Service already exists" -ForegroundColor Green
} else {
    Write-Host "  Creating scheduled task for Master Agent..." -ForegroundColor White

    # Create a startup script
    $startupScript = "$AGENT_DIR\run_agent.ps1"
    @"
# RLC Master Agent Startup Script
Set-Location "$AGENT_DIR"
& ".\.venv\Scripts\Activate.ps1"
python deployment/start_agent.py 2>&1 | Tee-Object -FilePath "C:\RLC\logs\master-agent.log" -Append
"@ | Out-File -FilePath $startupScript -Encoding UTF8

    Write-Host "  Created startup script: $startupScript" -ForegroundColor Green
    Write-Host "  To run the agent manually: $startupScript" -ForegroundColor White
}

# Summary
Write-Host ""
Write-Host "============================================================" -ForegroundColor Cyan
Write-Host "        Setup Complete!" -ForegroundColor Green
Write-Host "============================================================" -ForegroundColor Cyan
Write-Host ""
Write-Host "NEXT STEPS:" -ForegroundColor Yellow
Write-Host ""
Write-Host "1. Configure API keys:" -ForegroundColor White
Write-Host "   notepad $envFile" -ForegroundColor Gray
Write-Host ""
Write-Host "2. Test the system:" -ForegroundColor White
Write-Host "   cd $AGENT_DIR" -ForegroundColor Gray
Write-Host "   .\.venv\Scripts\Activate.ps1" -ForegroundColor Gray
Write-Host "   python deployment/start_agent.py" -ForegroundColor Gray
Write-Host ""
Write-Host "3. Test data collection:" -ForegroundColor White
Write-Host "   python scripts/collect.py --source usda-ams" -ForegroundColor Gray
Write-Host ""
Write-Host "4. View agent logs:" -ForegroundColor White
Write-Host "   Get-Content C:\RLC\logs\master-agent.log -Tail 50 -Wait" -ForegroundColor Gray
Write-Host ""
Write-Host "============================================================" -ForegroundColor Cyan
