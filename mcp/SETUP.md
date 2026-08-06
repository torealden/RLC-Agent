# RLC Local LLM — Setup

Goal: qwen3-coder:30b (Ollama) querying the RLC-Agent Postgres and reading
balance-sheet workbooks through a read-only MCP server.

Architecture: `mcphost` (bridge) → `rlc_mcp_server.py` (this server) → Postgres + workbook files.

## 1. Create the read-only database role (do this first)

Run once in psql as your admin user. This is the real safety layer — the
server's SQL validation is backup, not the primary defense.

```sql
CREATE ROLE rlc_readonly LOGIN PASSWORD '<standard automated-process password — RLC_READONLY_PG_PASSWORD in .env>';
GRANT CONNECT ON DATABASE your_db_name TO rlc_readonly;
GRANT USAGE ON SCHEMA bronze, silver, gold, reports TO rlc_readonly;
GRANT SELECT ON ALL TABLES IN SCHEMA bronze, silver, gold, reports TO rlc_readonly;
-- future tables get SELECT automatically:
ALTER DEFAULT PRIVILEGES IN SCHEMA bronze, silver, gold, reports
  GRANT SELECT ON TABLES TO rlc_readonly;
```

## 2. Install Python dependencies

```powershell
pip install mcp psycopg2-binary openpyxl
```

## 3. Place files and fill in config

Put `rlc_mcp_server.py` and `mcphost-config.json` in a folder, e.g.
`C:\dev\RLC-Agent\mcp\`. Edit `mcphost-config.json`:

- `RLC_PG_DSN` — the rlc_readonly connection string with your password and DB name
- `RLC_WORKBOOK_ROOT` — the directory containing your balance-sheet workbooks
- The `args` path — wherever you put the .py file

## 4. Smoke-test the server standalone (before involving the LLM)

```powershell
$env:RLC_PG_DSN = "postgresql://rlc_readonly:...@localhost:5432/your_db"
$env:RLC_WORKBOOK_ROOT = "C:\path\to\workbooks"
python C:\dev\RLC-Agent\mcp\rlc_mcp_server.py --selftest
```

You should see your table list and workbook files. Fix any errors here —
they'll be config/permissions issues, and they're much easier to diagnose
now than through the LLM.

## 5. Install and run mcphost

mcphost is a Go binary. Easiest install:

```powershell
winget install GoLang.Go        # if you don't have Go
go install github.com/mark3labs/mcphost@latest
```

The binary lands in `%USERPROFILE%\go\bin\mcphost.exe` (add that folder to
PATH, or call it by full path). Then:

```powershell
mcphost -m ollama:qwen3-coder:30b --provider-url http://127.0.0.1:11434 --config C:\dev\RLC-Agent\mcp\mcphost-config.json
```

Note: mcphost's config format has evolved across versions — if it rejects the
JSON, run `mcphost --help` and check the README at
github.com/mark3labs/mcphost for the current schema (newer versions also
accept YAML). The mcpServers block contents stay the same.

### Ollama URL quirk (this machine)

`--provider-url http://127.0.0.1:11434` is required: OLLAMA_HOST is set to the
scheme-less server-bind form `0.0.0.0:11434` and mcphost would otherwise reuse it
as the client base URL and fail to parse it. Easiest: run `mcpun_local_llm.ps1`.

## 6. First conversation — verify the loop

Good first prompts, in order:

1. "List the tables you can see." → should call rlc_list_tables
2. "Describe gold.curve_term." → should call rlc_describe_table
3. "How many rows are in silver.price_mark, grouped by source?" → real query

If the model guesses column names instead of describing the table first,
tell it: "Always call rlc_describe_table before writing SQL." Small models
need that reminder more than Claude does.

## Caps and knobs (environment variables, all optional)

| Variable | Default | Meaning |
|---|---|---|
| RLC_MAX_ROWS | 200 | Max rows returned per query |
| RLC_STMT_TIMEOUT_MS | 15000 | Query timeout |

Raise RLC_MAX_ROWS cautiously — every row eats the model's 16k context.

## Alternative host: Open WebUI

If you'd rather have a browser chat UI than mcphost's terminal, Open WebUI +
its `mcpo` proxy also bridges Ollama to MCP servers. More setup, nicer
interface. Start with mcphost; switch later if the terminal grates.
