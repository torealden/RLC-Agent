# RLC Orchestrator

A persistent AI-powered business automation system for Round Lakes Companies.

## Overview

The RLC Orchestrator is a hybrid AI system that combines:
- **Local persistence**: A lightweight orchestrator running 24/7 on your server
- **Cloud intelligence**: API calls to frontier AI models (Claude, GPT-4) for complex reasoning
- **Human oversight**: Email-based approval workflows for all important decisions

The system is designed to build itself progressively, with human approval at each step.

## Quick Start

```bash
# 1. Clone/copy this directory to your server
# 2. Create a virtual environment
python -m venv .venv
source .venv/bin/activate

# 3. Install dependencies
pip install -r requirements.txt

# 4. Initialize the database
python main.py --init-only --with-samples

# 5. Run the orchestrator
python main.py
```

## Project Structure

```
rlc-orchestrator/
├── main.py              # Entry point
├── requirements.txt     # Python dependencies
├── core/                # Core orchestrator components
│   ├── database.py      # SQLAlchemy models
│   ├── queue.py         # Task queue management
│   ├── executor.py      # Task execution engine
│   └── security.py      # Security guard
├── agents/              # Agent implementations
│   └── builtin/         # Built-in agents
├── integrations/        # External service integrations
│   ├── email_client.py  # Email (Phase 2)
│   └── ai_gateway.py    # AI APIs (Phase 3)
├── data/                # Database and data files
├── logs/                # Log files
└── scripts/             # Setup and utility scripts
```

## Development Phases

- **Phase 0**: Foundation (server, database, basic structure) ✓
- **Phase 1**: Heartbeat (task queue, executor loop)
- **Phase 2**: Email Integration (send/receive, approval workflows)
- **Phase 3**: AI Integration (Claude/GPT-4 for reasoning)
- **Phase 4**: Calendar Integration (daily briefings, scheduling)
- **Phase 5**: Self-Building (generate and deploy new agents)
- **Phase 6+**: Data pipeline expansion, analysis, reporting

## Configuration

Copy `.env.example` to `.env` and configure:

```bash
# Database
DB_PATH=data/orchestrator.db

# Logging
LOG_LEVEL=INFO
LOG_FILE=logs/orchestrator.log

# Executor
POLL_INTERVAL=5

# Email (Phase 2)
# EMAIL_HOST=smtp.gmail.com
# EMAIL_PORT=587
# EMAIL_USER=your-email@gmail.com
# EMAIL_PASSWORD=your-app-password
# EMAIL_RECIPIENT=tore.alden@roundlakescommodities.com

# AI APIs (Phase 3)
# ANTHROPIC_API_KEY=sk-...
# OPENAI_API_KEY=sk-...
```

## Running as a Service

To run the orchestrator as a systemd service (starts on boot, restarts on crash):

```bash
sudo bash scripts/setup_service.sh
sudo systemctl start rlc-orchestrator
```

## Security

The system includes multiple security layers:
- **Blocklist**: Dangerous operations are never allowed
- **Allowlist**: Sensitive operations require explicit permission
- **Sandboxing**: New code runs in isolation before deployment
- **Human approval**: Critical actions require email confirmation

See `core/security.py` for the complete security model.

## License

Proprietary - Round Lakes Companies
