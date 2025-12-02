#!/usr/bin/env python3
"""
RLC Master Agent - System Initialization Script
Validates configuration and creates necessary directories
Round Lakes Commodities
"""

import sys
import os
import json
from pathlib import Path
from datetime import datetime

# Ensure the package is in the path
sys.path.insert(0, str(Path(__file__).parent))


def print_header(text: str):
    """Print a section header"""
    print(f"\n{'='*60}")
    print(text)
    print('='*60)


def print_check(name: str, status: bool, message: str = ""):
    """Print a check result"""
    icon = "[OK]" if status else "[!!]"
    print(f"  {icon} {name}")
    if message:
        print(f"      {message}")


def check_python_version():
    """Check Python version"""
    print("\nChecking Python version...")
    version = sys.version_info
    if version.major >= 3 and version.minor >= 8:
        print_check("Python version", True, f"Python {version.major}.{version.minor}.{version.micro}")
        return True
    else:
        print_check("Python version", False, f"Python 3.8+ required, found {version.major}.{version.minor}")
        return False


def check_dependencies():
    """Check required Python packages"""
    print("\nChecking dependencies...")

    required = [
        ('dotenv', 'python-dotenv'),
        ('requests', 'requests'),
        ('pandas', 'pandas'),
    ]

    optional = [
        ('google.oauth2', 'google-auth'),
        ('googleapiclient', 'google-api-python-client'),
        ('notion_client', 'notion-client'),
        ('langchain', 'langchain'),
    ]

    all_ok = True

    print("\n  Required packages:")
    for module_name, package_name in required:
        try:
            __import__(module_name)
            print_check(package_name, True)
        except ImportError:
            print_check(package_name, False, f"Install with: pip install {package_name}")
            all_ok = False

    print("\n  Optional packages:")
    for module_name, package_name in optional:
        try:
            __import__(module_name)
            print_check(package_name, True)
        except ImportError:
            print_check(package_name, False, f"Install with: pip install {package_name}")

    return all_ok


def check_env_file():
    """Check .env file exists and has required values"""
    print("\nChecking environment configuration...")

    env_path = Path(__file__).parent / '.env'
    example_path = Path(__file__).parent / '.env.example'

    if not env_path.exists():
        if example_path.exists():
            print_check(".env file", False, "Not found. Creating from .env.example...")
            import shutil
            shutil.copy(example_path, env_path)
            print(f"      Created {env_path}")
            print("      Please edit this file with your configuration.")
            return False
        else:
            print_check(".env file", False, "Not found and no template available.")
            return False

    print_check(".env file", True, str(env_path))

    # Check for key values
    from dotenv import dotenv_values
    config = dotenv_values(env_path)

    print("\n  Configuration status:")

    # LLM config
    llm_provider = config.get('LLM_PROVIDER', 'ollama')
    if llm_provider == 'openai' and not config.get('OPENAI_API_KEY'):
        print_check("OpenAI API Key", False, "Required when LLM_PROVIDER=openai")
    else:
        print_check(f"LLM Provider", True, llm_provider)

    # Notion
    if config.get('NOTION_API_KEY'):
        print_check("Notion API Key", True, "Configured")
    else:
        print_check("Notion API Key", False, "Not configured (memory will use local storage)")

    # APIs
    if config.get('USDA_API_KEY'):
        print_check("USDA API Key", True, "Configured")
    else:
        print_check("USDA API Key", False, "Not configured")

    if config.get('CENSUS_API_KEY'):
        print_check("Census API Key", True, "Configured")
    else:
        print_check("Census API Key", False, "Not configured")

    return True


def check_directories():
    """Create required directories"""
    print("\nChecking directories...")

    base_dir = Path(__file__).parent
    directories = [
        base_dir / 'logs',
        base_dir / 'data',
        base_dir / 'config' / 'tokens',
    ]

    for directory in directories:
        directory.mkdir(parents=True, exist_ok=True)
        print_check(str(directory.relative_to(base_dir)), True, "Created/verified")

    return True


def check_ollama():
    """Check if Ollama is running"""
    print("\nChecking Ollama (local LLM)...")

    try:
        import requests
        from config.settings import get_settings

        settings = get_settings()
        if settings.llm.provider != 'ollama':
            print_check("Ollama", True, f"Not required (using {settings.llm.provider})")
            return True

        base_url = settings.llm.ollama_base_url
        response = requests.get(f"{base_url}/api/tags", timeout=5)

        if response.ok:
            models = response.json().get('models', [])
            model_names = [m.get('name', '') for m in models]
            print_check("Ollama server", True, f"Running at {base_url}")

            # Check for the configured model
            target_model = settings.llm.ollama_model
            if any(target_model in m for m in model_names):
                print_check(f"Model '{target_model}'", True, "Available")
            else:
                print_check(f"Model '{target_model}'", False,
                           f"Not found. Run: ollama pull {target_model}")
                if model_names:
                    print(f"      Available models: {', '.join(model_names[:5])}")
            return True
        else:
            print_check("Ollama server", False, "Not responding")
            return False

    except requests.exceptions.ConnectionError:
        print_check("Ollama server", False, "Not running. Start with: ollama serve")
        return False
    except Exception as e:
        print_check("Ollama server", False, f"Error: {e}")
        return False


def check_google_credentials():
    """Check Google OAuth credentials"""
    print("\nChecking Google credentials...")

    try:
        from config.settings import get_settings
        settings = get_settings()

        # Check credential files
        gmail_work = settings.google.get_credentials_path('gmail_work')
        gmail_personal = settings.google.get_credentials_path('gmail_personal')
        calendar = settings.google.get_credentials_path('calendar')

        print_check("Gmail (work) credentials", gmail_work.exists(),
                   str(gmail_work) if gmail_work.exists() else "Not found")
        print_check("Gmail (personal) credentials", gmail_personal.exists(),
                   str(gmail_personal) if gmail_personal.exists() else "Optional - not found")
        print_check("Calendar credentials", calendar.exists(),
                   str(calendar) if calendar.exists() else "Not found")

        # Check tokens
        token_dir = Path(settings.google.token_dir)
        gmail_token = token_dir / 'gmail_work_token.pickle'
        calendar_token = token_dir / 'calendar_token.pickle'

        print("\n  OAuth tokens:")
        print_check("Gmail token", gmail_token.exists(),
                   "Authenticated" if gmail_token.exists() else "Run setup_auth.py")
        print_check("Calendar token", calendar_token.exists(),
                   "Authenticated" if calendar_token.exists() else "Run setup_auth.py")

        return True

    except Exception as e:
        print_check("Google credentials", False, f"Error: {e}")
        return False


def check_notion():
    """Check Notion connectivity"""
    print("\nChecking Notion integration...")

    try:
        from config.settings import get_settings
        settings = get_settings()

        if not settings.notion.api_key:
            print_check("Notion API", False, "API key not configured")
            return False

        try:
            from notion_client import Client
            client = Client(auth=settings.notion.api_key)
            user = client.users.me()
            print_check("Notion API", True, f"Connected as: {user.get('name', 'Unknown')}")

            # Check databases
            print("\n  Databases:")
            for db_name, db_id in [
                ('Tasks', settings.notion.tasks_db_id),
                ('Memory', settings.notion.memory_db_id),
                ('Interactions', settings.notion.interactions_db_id),
                ('Wiki', settings.notion.wiki_db_id)
            ]:
                if db_id:
                    try:
                        client.databases.retrieve(db_id)
                        print_check(f"{db_name} DB", True, f"ID: {db_id[:8]}...")
                    except Exception:
                        print_check(f"{db_name} DB", False, "ID configured but not accessible")
                else:
                    print_check(f"{db_name} DB", False, "Not configured")

            return True

        except ImportError:
            print_check("Notion API", False, "notion-client not installed")
            return False

    except Exception as e:
        print_check("Notion API", False, f"Error: {e}")
        return False


def check_data_services():
    """Check data service connectivity"""
    print("\nChecking data services...")

    try:
        from config.settings import get_settings
        from services.usda_api import USDAService
        from services.census_api import CensusService
        from services.weather_api import WeatherService

        settings = get_settings()

        # USDA
        usda = USDAService(
            api_key=settings.api.usda_api_key,
            base_url=settings.api.usda_base_url
        )
        usda_health = usda.health_check()
        print_check("USDA API", usda_health.get('status') == 'healthy',
                   usda_health.get('status', 'unknown'))

        # Census
        census = CensusService(
            api_key=settings.api.census_api_key,
            base_url=settings.api.census_base_url
        )
        census_health = census.health_check()
        print_check("Census API", census_health.get('status') == 'healthy',
                   census_health.get('status', 'unknown'))

        # Weather
        weather = WeatherService(
            api_key=settings.api.weather_api_key,
            base_url=settings.api.weather_base_url
        )
        weather_health = weather.health_check()
        print_check("Weather API", weather_health.get('status') == 'healthy',
                   weather_health.get('status', 'unknown'))

        return True

    except Exception as e:
        print_check("Data services", False, f"Error: {e}")
        return False


def run_initialization():
    """Run the full initialization check"""
    print_header("RLC Master Agent - System Initialization")
    print(f"Date: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    results = {}

    # Run all checks
    results['python'] = check_python_version()
    results['dependencies'] = check_dependencies()
    results['env'] = check_env_file()
    results['directories'] = check_directories()
    results['ollama'] = check_ollama()
    results['google'] = check_google_credentials()
    results['notion'] = check_notion()
    results['data'] = check_data_services()

    # Summary
    print_header("Initialization Summary")

    all_critical_ok = results['python'] and results['dependencies'] and results['directories']
    all_ok = all(results.values())

    if all_ok:
        print("\n  All checks passed!")
        print("\n  You can now run: python launch.py")
    elif all_critical_ok:
        print("\n  Core requirements met. Some optional features may not work.")
        print("\n  You can still run: python launch.py")
        print("  Review the warnings above to enable all features.")
    else:
        print("\n  Some critical checks failed. Please fix the issues above.")
        print("\n  Common solutions:")
        print("  - Install missing packages: pip install -r requirements.txt")
        print("  - Copy and edit config: cp .env.example .env")
        print("  - Set up Google OAuth: python setup_auth.py")
        print("  - Start Ollama: ollama serve")

    print("\n" + "=" * 60)
    print("Initialization complete!")
    print("=" * 60 + "\n")

    return 0 if all_critical_ok else 1


def main():
    """Main entry point"""
    return run_initialization()


if __name__ == '__main__':
    sys.exit(main())
