#!/bin/bash
# RLC Master Agent System - Complete Setup Script
# This script creates the entire project structure and all files

echo "Creating RLC Master Agent System..."
echo "=================================="

# Create project directory
mkdir -p rlc-assistant
cd rlc-assistant

# Create directory structure
echo "Creating directory structure..."
mkdir -p agents services config data logs data/memory

# Create requirements.txt
echo "Creating requirements.txt..."
cat > requirements.txt << 'EOL'
# RLC Master Agent System Requirements

# Core LLM Frameworks
langchain>=0.1.0
langchain-community>=0.0.10
langchain-openai>=0.0.5
ollama>=0.1.7

# Google APIs
google-auth>=2.25.0
google-auth-oauthlib>=1.1.0
google-auth-httplib2>=0.1.0
google-api-python-client>=2.100.0

# Data Processing
pandas>=2.0.0
numpy>=1.24.0
yfinance>=0.2.30
requests>=2.31.0
python-dotenv>=1.0.0

# Notion Integration
notion-client>=2.0.0

# Logging and Utils
python-dateutil>=2.8.2
pytz>=2023.3

# Optional for enhanced features
openai>=1.0.0  # If using OpenAI
chromadb>=0.4.0  # For vector storage (optional)
faiss-cpu>=1.7.4  # Alternative vector storage (optional)
EOL

# Create .env.example
echo "Creating .env.example..."
cat > .env.example << 'EOL'
# RLC Master Agent Environment Configuration
# Copy this to .env and fill in your actual values

# LLM Configuration
LLM_PROVIDER=ollama
OLLAMA_MODEL=llama2
OLLAMA_BASE_URL=http://localhost:11434

# Notion Configuration
NOTION_API_KEY=secret_your_notion_integration_token
NOTION_MEMORY_DB_ID=your_memory_database_id_here
NOTION_TASKS_DB_ID=your_tasks_database_id_here
NOTION_PROCESSES_DB_ID=your_processes_database_id_here
NOTION_WIKI_ID=your_wiki_page_id_here

# Data API Keys
USDA_API_KEY=your_usda_api_key_here
CENSUS_API_KEY=your_census_api_key_here

# Business Configuration
BUSINESS_NAME="Round Lakes Commodities"
BUSINESS_TIMEZONE="America/New_York"
INITIAL_AUTONOMY_LEVEL=1
AUTO_APPROVE_CALENDAR_CREATION=true
EOL

# Copy .env.example to .env
cp .env.example .env

echo ""
echo "✅ Project structure created!"
echo ""
echo "Next steps:"
echo "1. cd rlc-assistant"
echo "2. Create virtual environment: python3 -m venv venv"
echo "3. Activate it: source venv/bin/activate"
echo "4. Install dependencies: pip install -r requirements.txt"
echo "5. Edit .env file with your API keys"
echo "6. Add your Google credential JSON files to config/"
echo "7. Run setup: python setup_auth.py"
echo "8. Launch: python launch.py"
echo ""
echo "For the complete code files, please see the individual Python files provided separately."
echo "Place them in the appropriate directories as shown in the structure above."
