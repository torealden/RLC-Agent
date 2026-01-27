"""
RLC Commodity Analysis LLM Assistant
Interfaces with local Ollama instance for commodity market analysis and project assistance.

Usage:
    python scripts/llm_assistant.py                    # Interactive chat mode
    python scripts/llm_assistant.py --review           # Review project structure
    python scripts/llm_assistant.py --analyze-data     # Analyze database contents
    python scripts/llm_assistant.py --query "question" # Single question mode
"""

import os
import sys
import json
import argparse
import requests
from pathlib import Path
from typing import Optional, List, Dict, Generator

# ============================================================================
# CONFIGURATION
# ============================================================================

OLLAMA_HOST = "http://localhost:11434"
DEFAULT_MODEL = "llama3.1"

# Project paths
SCRIPT_DIR = Path(__file__).parent
PROJECT_ROOT = SCRIPT_DIR.parent

# PostgreSQL configuration (matches other scripts)
PG_CONFIG = {
    "host": "localhost",
    "port": "5432",
    "database": "rlc_commodities",
    "user": "postgres",
    "password": "SoupBoss1"
}

# ============================================================================
# SYSTEM PROMPTS
# ============================================================================

COMMODITY_ANALYST_PROMPT = """You are an expert commodity market analyst specializing in oilseeds, grains, and biofuels. You have deep knowledge of:

**Commodities:**
- Soybeans, soybean meal, soybean oil
- Corn, wheat, and other feed grains
- Rapeseed/canola, sunflower, palm oil
- Ethanol, biodiesel, and renewable fuels

**Market Knowledge:**
- USDA WASDE reports and balance sheet analysis
- Global trade flows and export/import patterns
- Marketing year conventions (Sep-Aug for soybeans, Oct-Sep for products)
- Crush margins, basis, and spreads
- Weather impacts on crop production
- Policy impacts (RFS, EPA mandates, trade policy)

**Data Analysis:**
- Historical trend analysis
- Supply/demand balance sheets
- Trade flow matrices
- Seasonal patterns

You are helping with the RLC-Agent project, which:
1. Collects commodity market data from multiple sources (USDA, EIA, Census, etc.)
2. Stores data in a PostgreSQL database with bronze/silver/gold medallion architecture
3. Uses data through 2019/20 marketing year for LLM training
4. Reserves 2021-2025 data for testing predictions

Be concise, accurate, and focus on actionable insights. When analyzing data, explain your reasoning and highlight key patterns or anomalies."""

PROJECT_REVIEW_PROMPT = """You are reviewing the RLC-Agent project structure. This is a commodity market analysis system that:

1. **Data Collection**: Pulls data from USDA, EIA, Census Bureau, Canadian CGC, Brazilian CONAB, and other sources
2. **Database**: PostgreSQL with bronze (raw), silver (cleaned), gold (analytics-ready) layers
3. **Analysis**: Historical commodity balance sheets and trade flows
4. **Goal**: Train an LLM on historical data (through 2019/20) to forecast commodity markets

Review the provided information and give insights about:
- Project organization and architecture
- Data pipeline completeness
- Potential improvements
- How the LLM training data is structured"""

# ============================================================================
# OLLAMA API FUNCTIONS
# ============================================================================

def check_ollama_running() -> bool:
    """Check if Ollama server is running."""
    try:
        response = requests.get(f"{OLLAMA_HOST}/api/tags", timeout=5)
        return response.status_code == 200
    except requests.exceptions.ConnectionError:
        return False

def list_models() -> List[str]:
    """List available Ollama models."""
    try:
        response = requests.get(f"{OLLAMA_HOST}/api/tags", timeout=10)
        if response.status_code == 200:
            data = response.json()
            return [model["name"] for model in data.get("models", [])]
    except Exception as e:
        print(f"Error listing models: {e}")
    return []

def chat(
    messages: List[Dict[str, str]],
    model: str = DEFAULT_MODEL,
    stream: bool = True
) -> Generator[str, None, None]:
    """
    Send chat messages to Ollama and stream response.

    Args:
        messages: List of {"role": "user/assistant/system", "content": "..."}
        model: Model name to use
        stream: Whether to stream the response

    Yields:
        Response text chunks
    """
    payload = {
        "model": model,
        "messages": messages,
        "stream": stream
    }

    try:
        response = requests.post(
            f"{OLLAMA_HOST}/api/chat",
            json=payload,
            stream=stream,
            timeout=300
        )

        if response.status_code != 200:
            yield f"Error: {response.status_code} - {response.text}"
            return

        if stream:
            for line in response.iter_lines():
                if line:
                    data = json.loads(line)
                    if "message" in data and "content" in data["message"]:
                        yield data["message"]["content"]
        else:
            data = response.json()
            if "message" in data and "content" in data["message"]:
                yield data["message"]["content"]

    except requests.exceptions.Timeout:
        yield "Error: Request timed out. The model may be loading or processing a large request."
    except Exception as e:
        yield f"Error: {e}"

def simple_query(prompt: str, system_prompt: str = COMMODITY_ANALYST_PROMPT, model: str = DEFAULT_MODEL) -> str:
    """Send a simple query and return the full response."""
    messages = [
        {"role": "system", "content": system_prompt},
        {"role": "user", "content": prompt}
    ]

    response_text = ""
    for chunk in chat(messages, model=model, stream=False):
        response_text += chunk

    return response_text

# ============================================================================
# PROJECT ANALYSIS FUNCTIONS
# ============================================================================

def get_project_structure() -> str:
    """Get a summary of the project structure."""
    structure = []

    # Key directories
    key_dirs = ["scripts", "database", "src/agents", "deployment", "Models"]

    for dir_name in key_dirs:
        dir_path = PROJECT_ROOT / dir_name
        if dir_path.exists():
            structure.append(f"\n## {dir_name}/")
            if dir_path.is_dir():
                for item in sorted(dir_path.rglob("*"))[:20]:  # Limit to 20 items
                    if item.is_file() and not item.name.startswith("."):
                        rel_path = item.relative_to(PROJECT_ROOT)
                        structure.append(f"  - {rel_path}")

    return "\n".join(structure)

def get_database_summary() -> str:
    """Get summary of database tables and row counts."""
    try:
        import psycopg2

        conn = psycopg2.connect(**PG_CONFIG)
        cur = conn.cursor()

        summary = ["## Database Summary\n"]

        # Get schemas
        cur.execute("""
            SELECT schema_name FROM information_schema.schemata
            WHERE schema_name IN ('bronze', 'silver', 'gold')
        """)
        schemas = [row[0] for row in cur.fetchall()]

        for schema in schemas:
            summary.append(f"\n### {schema.upper()} Layer")

            # Get tables in schema
            cur.execute(f"""
                SELECT table_name FROM information_schema.tables
                WHERE table_schema = '{schema}' AND table_type = 'BASE TABLE'
            """)
            tables = [row[0] for row in cur.fetchall()]

            for table in tables:
                try:
                    cur.execute(f"SELECT COUNT(*) FROM {schema}.{table}")
                    count = cur.fetchone()[0]
                    summary.append(f"  - {table}: {count:,} rows")
                except:
                    summary.append(f"  - {table}: (error reading)")

            # Get views in schema
            cur.execute(f"""
                SELECT table_name FROM information_schema.views
                WHERE table_schema = '{schema}'
            """)
            views = [row[0] for row in cur.fetchall()]

            if views:
                summary.append(f"\n  Views:")
                for view in views:
                    summary.append(f"    - {view}")

        conn.close()
        return "\n".join(summary)

    except ImportError:
        return "psycopg2 not installed - cannot query database"
    except Exception as e:
        return f"Database connection error: {e}"

def get_sample_data() -> str:
    """Get sample data from key tables."""
    try:
        import psycopg2
        import pandas as pd

        conn = psycopg2.connect(**PG_CONFIG)

        samples = ["## Sample Data\n"]

        # Sample from trade data
        try:
            df = pd.read_sql("""
                SELECT commodity, partner_country, flow_type, marketing_year, quantity
                FROM bronze.trade_data_raw
                LIMIT 5
            """, conn)
            samples.append("\n### Trade Data Sample:")
            samples.append(df.to_string())
        except Exception as e:
            samples.append(f"\nTrade data: {e}")

        # Sample from balance sheets
        try:
            df = pd.read_sql("""
                SELECT * FROM bronze.sqlite_commodity_balance_sheets
                LIMIT 5
            """, conn)
            samples.append("\n\n### Balance Sheet Sample:")
            samples.append(df.to_string())
        except Exception as e:
            samples.append(f"\nBalance sheets: {e}")

        conn.close()
        return "\n".join(samples)

    except Exception as e:
        return f"Error getting sample data: {e}"

# ============================================================================
# INTERACTIVE MODES
# ============================================================================

def interactive_chat(model: str = DEFAULT_MODEL):
    """Run interactive chat session."""
    print(f"\n{'='*60}")
    print(f"RLC Commodity Analysis Assistant")
    print(f"Model: {model}")
    print(f"{'='*60}")
    print("Type 'quit' to exit, 'clear' to reset conversation")
    print("Special commands: /db (database info), /project (project info)")
    print(f"{'='*60}\n")

    messages = [{"role": "system", "content": COMMODITY_ANALYST_PROMPT}]

    while True:
        try:
            user_input = input("\nYou: ").strip()

            if not user_input:
                continue

            if user_input.lower() == 'quit':
                print("Goodbye!")
                break

            if user_input.lower() == 'clear':
                messages = [{"role": "system", "content": COMMODITY_ANALYST_PROMPT}]
                print("Conversation cleared.")
                continue

            # Special commands
            if user_input.lower() == '/db':
                db_info = get_database_summary()
                print(f"\n{db_info}")
                continue

            if user_input.lower() == '/project':
                proj_info = get_project_structure()
                print(f"\n{proj_info}")
                continue

            # Add context commands
            if user_input.lower().startswith('/analyze'):
                # Add database context to the query
                db_summary = get_database_summary()
                sample_data = get_sample_data()
                context = f"Here is the current database state:\n{db_summary}\n\n{sample_data}\n\nUser question: {user_input[8:]}"
                user_input = context

            messages.append({"role": "user", "content": user_input})

            print("\nAssistant: ", end="", flush=True)

            full_response = ""
            for chunk in chat(messages, model=model):
                print(chunk, end="", flush=True)
                full_response += chunk

            print()  # New line after response

            messages.append({"role": "assistant", "content": full_response})

        except KeyboardInterrupt:
            print("\n\nInterrupted. Type 'quit' to exit.")
        except EOFError:
            break

def review_project(model: str = DEFAULT_MODEL):
    """Have the LLM review the project structure."""
    print(f"\n{'='*60}")
    print("Project Review")
    print(f"{'='*60}\n")

    # Gather project information
    print("Gathering project information...")
    project_structure = get_project_structure()
    db_summary = get_database_summary()

    prompt = f"""Please review this RLC-Agent project:

{project_structure}

{db_summary}

Provide:
1. Overview of the project architecture
2. Assessment of the data pipeline
3. Strengths and potential improvements
4. Recommendations for the LLM training approach"""

    print("\nAnalyzing...\n")

    messages = [
        {"role": "system", "content": PROJECT_REVIEW_PROMPT},
        {"role": "user", "content": prompt}
    ]

    for chunk in chat(messages, model=model):
        print(chunk, end="", flush=True)
    print()

def analyze_data(model: str = DEFAULT_MODEL):
    """Have the LLM analyze the database contents."""
    print(f"\n{'='*60}")
    print("Data Analysis")
    print(f"{'='*60}\n")

    print("Gathering database information...")
    db_summary = get_database_summary()
    sample_data = get_sample_data()

    prompt = f"""Analyze this commodity database:

{db_summary}

{sample_data}

Provide insights on:
1. Data completeness and coverage
2. Key commodities and trade partners represented
3. Time period coverage
4. Data quality observations
5. Suggestions for analysis or visualization"""

    print("\nAnalyzing...\n")

    messages = [
        {"role": "system", "content": COMMODITY_ANALYST_PROMPT},
        {"role": "user", "content": prompt}
    ]

    for chunk in chat(messages, model=model):
        print(chunk, end="", flush=True)
    print()

# ============================================================================
# MAIN
# ============================================================================

def main():
    parser = argparse.ArgumentParser(description="RLC Commodity Analysis LLM Assistant")
    parser.add_argument("--model", default=DEFAULT_MODEL, help="Ollama model to use")
    parser.add_argument("--review", action="store_true", help="Review project structure")
    parser.add_argument("--analyze-data", action="store_true", help="Analyze database contents")
    parser.add_argument("--query", type=str, help="Single question mode")
    parser.add_argument("--list-models", action="store_true", help="List available models")

    args = parser.parse_args()

    # Check Ollama is running
    if not check_ollama_running():
        print("Error: Ollama is not running. Start it with: ollama serve")
        sys.exit(1)

    if args.list_models:
        models = list_models()
        print("Available models:")
        for model in models:
            print(f"  - {model}")
        return

    if args.review:
        review_project(model=args.model)
    elif args.analyze_data:
        analyze_data(model=args.model)
    elif args.query:
        response = simple_query(args.query, model=args.model)
        print(response)
    else:
        interactive_chat(model=args.model)

if __name__ == "__main__":
    main()
