# RLC Commodities MCP Server

This MCP (Model Context Protocol) server provides Claude with direct access to the agricultural commodities database, enabling autonomous data exploration and analysis.

## Setup

### 1. Install Dependencies

```bash
pip install mcp psycopg2-binary
```

### 2. Configure Claude Code

Add to your Claude Code MCP settings (`~/.config/claude-code/settings.json` or via the UI):

```json
{
  "mcpServers": {
    "commodities-db": {
      "command": "python",
      "args": ["C:/Users/torem/RLC Dropbox/RLC Team Folder/RLC-Agent/src/mcp/commodities_db_server.py"]
    }
  }
}
```

### 3. Verify Connection

Restart Claude Code and verify the server is connected. You should see the commodities-db tools available.

## Available Tools

| Tool | Description |
|------|-------------|
| `query_database` | Execute custom SQL SELECT queries |
| `list_tables` | List all tables in bronze/silver/gold schemas |
| `describe_table` | Get column information for a table |
| `get_balance_sheet` | Get S&D balance sheet for commodity/country |
| `get_production_ranking` | Get global production rankings |
| `get_stocks_to_use` | Calculate stocks-to-use ratios |
| `get_commodity_summary` | Get summary of all commodities in database |
| `analyze_supply_demand` | Comprehensive S&D analysis with YoY changes |
| `get_brazil_production` | Brazil state-level production from CONAB |

## CLI Mode

The server also works as a command-line tool:

```bash
# Get commodity summary
python src/mcp/commodities_db_server.py --summary

# Get US corn balance sheet
python src/mcp/commodities_db_server.py --balance-sheet corn US

# Get global soybean production ranking
python src/mcp/commodities_db_server.py --ranking soybeans

# Run S&D analysis
python src/mcp/commodities_db_server.py --analyze corn US

# Custom query
python src/mcp/commodities_db_server.py --query "SELECT * FROM bronze.fas_psd WHERE commodity='wheat' LIMIT 5"
```

## Example Prompts for Claude

With the MCP server connected, you can ask Claude:

- "What's the current US corn stocks-to-use ratio compared to last year?"
- "Show me the top 10 soybean producers globally"
- "Analyze Brazil's corn production trends by state"
- "Compare US and Brazil soybean export projections"
- "What commodities have the tightest stocks situation?"

## Database Schema

See `domain_knowledge/LLM_DATABASE_CONTEXT.md` for complete database documentation including:
- Table schemas and columns
- Example queries
- Unit conversions
- Marketing year definitions
- Country and commodity codes
