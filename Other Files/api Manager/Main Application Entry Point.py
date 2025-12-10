# main.py
from core.orchestrator import DataOrchestrator
import os

def main():
    # Database configuration
    db_config = {
        'host': os.getenv('DB_HOST', 'localhost'),
        'username': os.getenv('DB_USERNAME'),
        'password': os.getenv('DB_PASSWORD'),
        'database': os.getenv('DB_NAME', 'commodities')
    }
    
    # Initialize orchestrator
    orchestrator = DataOrchestrator(db_config=db_config)
    orchestrator.initialize()
    
    # For manual execution
    # orchestrator.run_all_plugins()
    
    # For scheduled execution
    # orchestrator.start_scheduler()

if __name__ == "__main__":
    main()
```

## Directory Structure

Your new project structure should look like:
```
your_project/
├── main.py
├── config/
│   ├── api_sources.json
│   └── database.json
├── core/
│   ├── __init__.py
│   ├── base_api.py
│   ├── plugin_manager.py
│   └── orchestrator.py
├── plugins/
│   ├── __init__.py
│   ├── eia_source.py
│   ├── gtt_source.py
│   └── lcfs_source.py
└── utils/
    ├── __init__.py
    ├── database.py
    └── helpers.py