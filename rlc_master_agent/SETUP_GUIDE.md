# RLC Master Agent - Setup Guide

## Complete Setup and Usage Guide for Round Lakes Commodities AI Business Partner

---

## Table of Contents

1. [Overview](#overview)
2. [Prerequisites](#prerequisites)
3. [Installation](#installation)
4. [Configuration](#configuration)
5. [Notion Setup](#notion-setup)
6. [Google API Setup](#google-api-setup)
7. [Running the Agent](#running-the-agent)
8. [Usage Guide](#usage-guide)
9. [Troubleshooting](#troubleshooting)
10. [Architecture Reference](#architecture-reference)

---

## Overview

The RLC Master Agent is an AI-powered business assistant designed for Round Lakes Commodities. It functions across all branches (RLC Analytics, RLC Meats, and RLC Trading), acting as an AI business partner focused on profitability and efficiency.

### Key Features

- **Email Management**: Triage, summarize, and respond to emails via Gmail
- **Calendar Scheduling**: Manage Google Calendar, find free slots, schedule meetings
- **Market Data**: Fetch commodity prices from USDA, trade data from Census Bureau
- **Weather Integration**: Ranch and agricultural weather conditions
- **Process Automation**: Execute documented business processes
- **Memory & Learning**: Persistent memory via Notion integration
- **Gradual Autonomy**: Three levels of automation with approval controls

---

## Prerequisites

### Required Software

1. **Python 3.8+** - [Download Python](https://www.python.org/downloads/)
2. **Git** - [Download Git](https://git-scm.com/downloads)
3. **Ollama** (for local LLM) - [Download Ollama](https://ollama.ai/)

### Required Accounts

1. **Google Account** - For Gmail and Calendar integration
2. **Notion Account** - For memory and documentation (free tier works)
3. **USDA Market News Account** - For commodity data (free)
4. **Census Bureau API Key** - For trade data (free)
5. **OpenWeatherMap Account** - For weather data (free tier)

---

## Installation

### Step 1: Clone the Repository

```bash
git clone <repository-url>
cd RLC-Agent/rlc_master_agent
```

### Step 2: Create Virtual Environment

```bash
python -m venv venv

# On Linux/Mac:
source venv/bin/activate

# On Windows:
venv\Scripts\activate
```

### Step 3: Install Dependencies

```bash
pip install -r requirements.txt
```

### Step 4: Install Ollama and Download Model

```bash
# Install Ollama (Linux/Mac)
curl -fsSL https://ollama.ai/install.sh | sh

# Download a model
ollama pull llama2:13b

# Start Ollama server
ollama serve
```

---

## Configuration

### Step 1: Create Environment File

```bash
cp .env.example .env
```

### Step 2: Edit .env File

Open `.env` in your editor and fill in the values:

```bash
# LLM Configuration
LLM_PROVIDER=ollama
OLLAMA_MODEL=llama2:13b
OLLAMA_BASE_URL=http://localhost:11434

# API Keys (get these from the respective services)
USDA_API_KEY=your_usda_key_here
CENSUS_API_KEY=your_census_key_here
WEATHER_API_KEY=your_openweather_key_here

# Notion (see Notion Setup section below)
NOTION_API_KEY=secret_xxx
NOTION_TASKS_DB=xxx
NOTION_MEMORY_DB=xxx
NOTION_INTERACTIONS_DB=xxx
NOTION_WIKI_DB=xxx

# User Configuration
USER_NAME=Your Name
BUSINESS_NAME=Round Lakes Commodities
USER_EMAIL=your@email.com
USER_TIMEZONE=America/Chicago
```

### Getting API Keys

1. **USDA API Key**:
   - Go to [USDA QuickStats API](https://quickstats.nass.usda.gov/api)
   - Sign up for a free API key

2. **Census Bureau API Key**:
   - Go to [Census API Key Signup](https://api.census.gov/data/key_signup.html)
   - Request a free key

3. **OpenWeatherMap API Key**:
   - Go to [OpenWeatherMap](https://openweathermap.org/api)
   - Create account and get free API key

---

## Notion Setup

### Step 1: Create Notion Integration

1. Go to [Notion Integrations](https://www.notion.so/my-integrations)
2. Click "New integration"
3. Name it "RLC Assistant"
4. Select your workspace
5. Grant read, update, and insert permissions
6. Copy the "Internal Integration Token" (starts with `secret_`)

### Step 2: Create Databases

In your Notion workspace, create these databases:

#### Tasks Database
- **Properties**:
  - Name (Title)
  - Status (Select: Not Started, In Progress, Completed, Blocked)
  - Priority (Select: High, Medium, Low)
  - Due Date (Date)
  - Category (Select: Email, Calendar, Data, etc.)
  - AI Notes (Text)

#### Assistant Memory Database
- **Properties**:
  - Name (Title)
  - Type (Select: Preference, Fact, Decision, Observation)
  - Category (Select: Trading, Ranch, Analytics, Communications)
  - Content (Text)
  - Source (Text)
  - Confidence (Number)
  - Created (Date)

#### Interaction History Database
- **Properties**:
  - Name (Title)
  - Date (Date)
  - User Input (Text)
  - Agent Response (Text)
  - Actions (Multi-select)
  - Tools Used (Multi-select)
  - Approval Required (Checkbox)
  - Success (Checkbox)
  - Feedback (Text)

#### Process Wiki Database
- **Properties**:
  - Name (Title)
  - Category (Select: Market Reports, Ranch Operations, Trading)
  - Frequency (Select: Daily, Weekly, Monthly, Ad-hoc)
  - Steps (Text)
  - Automation Status (Select: Manual, Partial, Full)
  - Last Run (Date)
  - Owner (Select: Human, AI)

### Step 3: Share with Integration

For each database:
1. Open the database page
2. Click "..." menu → "Add connections"
3. Select "RLC Assistant" integration

### Step 4: Get Database IDs

For each database:
1. Open as full page
2. Copy the URL
3. The ID is the 32-character string before the `?`

Example URL: `https://notion.so/workspace/Tasks-abc123...?v=...`
Database ID: `abc123...` (the 32 characters)

Add these IDs to your `.env` file.

---

## Google API Setup

### Step 1: Create Google Cloud Project

1. Go to [Google Cloud Console](https://console.cloud.google.com/)
2. Create a new project (e.g., "RLC Assistant")
3. Enable these APIs:
   - Gmail API
   - Google Calendar API

### Step 2: Create OAuth Credentials

1. Go to "APIs & Services" → "Credentials"
2. Click "Create Credentials" → "OAuth client ID"
3. Choose "Desktop app"
4. Download the JSON file

### Step 3: Save Credentials

Save the downloaded files in the `config/` directory:
- `gmail_work_credentials.json` - For work Gmail
- `calendar_credentials.json` - For Calendar

### Step 4: Run OAuth Setup

```bash
python setup_auth.py
```

This will open browser windows to authorize Gmail and Calendar access.

---

## Running the Agent

### Verify Setup

First, run the initialization script to verify everything is configured:

```bash
python initialize_system.py
```

This checks:
- Python version
- Required packages
- Environment configuration
- Ollama connection
- Google credentials
- Notion connectivity
- Data service APIs

### Start the Agent

```bash
python launch.py
```

### Command Line Options

```bash
# Interactive mode (default)
python launch.py

# Check system status
python launch.py --status

# Run health check
python launch.py --health

# Run daily workflow
python launch.py --daily

# Debug mode
python launch.py --debug

# Test mode (simulated actions)
python launch.py --test
```

---

## Usage Guide

### Basic Commands

Once the agent is running, you can interact naturally:

#### Email Commands
```
check inbox
show emails
summarize emails
draft reply to John
```

#### Calendar Commands
```
what's on my schedule today
show today's events
find free time
schedule meeting with John tomorrow
next event
```

#### Market Data Commands
```
corn price
soybean price
market overview
daily briefing
export data for corn
```

#### Weather Commands
```
weather
ranch weather
weather forecast
```

#### System Commands
```
help
status
```

### Autonomy Levels

The agent operates at three autonomy levels:

1. **Level 1 - Supervised**: Asks approval for all significant actions
2. **Level 2 - Partial**: Auto-handles routine tasks, asks for important ones
3. **Level 3 - Autonomous**: Handles most tasks, only escalates exceptions

Start at Level 1 and gradually increase as trust builds.

---

## Troubleshooting

### Common Issues

#### "Ollama not running"
```bash
# Start Ollama server
ollama serve

# In another terminal, verify it's running
curl http://localhost:11434/api/tags
```

#### "Gmail authentication failed"
```bash
# Delete existing tokens and re-authenticate
rm config/tokens/gmail_*.pickle
python setup_auth.py
```

#### "Notion connection failed"
- Verify your API key is correct in `.env`
- Check that databases are shared with the integration
- Verify database IDs are correct

#### "USDA/Census API errors"
- Verify API keys in `.env`
- Check API service status at their respective websites
- Some endpoints have rate limits - wait and retry

### Debug Mode

Run with debug logging for more details:

```bash
python launch.py --debug
```

Check logs in the `logs/` directory:
- `master_agent.log` - Main agent logs
- `approval_stats.json` - Approval history

---

## Architecture Reference

### Directory Structure

```
rlc_master_agent/
├── config/
│   ├── __init__.py
│   ├── settings.py           # Configuration management
│   ├── email_preferences.json # Email handling rules
│   ├── tokens/               # OAuth tokens (created at runtime)
│   ├── gmail_work_credentials.json
│   └── calendar_credentials.json
├── services/
│   ├── __init__.py
│   ├── usda_api.py           # USDA data service
│   ├── census_api.py         # Census Bureau service
│   └── weather_api.py        # Weather service
├── logs/                     # Log files (created at runtime)
├── data/                     # Data files (created at runtime)
├── master_agent.py           # Main orchestrator
├── email_agent.py            # Email management
├── calendar_agent.py         # Calendar management
├── data_agent.py             # Data retrieval
├── verification_agent.py     # Quality checking
├── approval_manager.py       # Autonomy control
├── memory_manager.py         # Notion memory
├── launch.py                 # Startup script
├── setup_auth.py             # OAuth setup
├── initialize_system.py      # System verification
├── requirements.txt          # Python dependencies
├── .env.example             # Environment template
└── SETUP_GUIDE.md           # This file
```

### Component Responsibilities

| Component | Purpose |
|-----------|---------|
| Master Agent | Central orchestrator, request routing, LLM integration |
| Email Agent | Gmail integration, email triage, drafting |
| Calendar Agent | Google Calendar, scheduling, availability |
| Data Agent | Market data, exports, weather |
| Verification Agent | Quality checks, validation |
| Approval Manager | Autonomy levels, human-in-the-loop |
| Memory Manager | Notion integration, persistent memory |

---

## Next Steps

After completing setup:

1. **Test basic commands** - Try email, calendar, and data commands
2. **Document processes** - Add your business processes to the Notion wiki
3. **Set preferences** - Tell the agent your preferences (e.g., "I prefer morning meetings")
4. **Monitor and adjust** - Review logs and adjust autonomy as trust builds
5. **Extend capabilities** - Add new data sources or processes as needed

---

## Support

For issues or questions:
- Check the logs in `logs/` directory
- Run `python initialize_system.py` to verify configuration
- Review this guide's troubleshooting section

---

*RLC Master Agent - Round Lakes Commodities*
*AI Business Partner System*
