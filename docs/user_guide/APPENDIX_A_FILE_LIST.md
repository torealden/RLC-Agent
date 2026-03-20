# Appendix A: File List for New Users

[← Back to Table of Contents](00_COVER_AND_TOC.md)

---

This appendix lists all files that need to be transferred to a new user's machine that are **not** stored in Git.

## Files to Obtain

These files contain credentials or are generated locally. Obtain them from an existing team member.

### Required Files

| File | Location | Description | Source |
|------|----------|-------------|--------|
| `.env` | `C:\dev\RLC-Agent\` | Main configuration (DB, API keys) | Copy from team member, update credentials |
| `.env` | `C:\dev\RLC-Agent\dashboards\ops\` | Dashboard DB config | Same as above, can be subset |

### Conditional Files (If Using These Features)

| File | Location | Description | When Needed |
|------|----------|-------------|-------------|
| `credentials.json` | `C:\dev\RLC-Agent\` | Google OAuth credentials | Gmail/Calendar integration |
| `token.json` | `C:\dev\RLC-Agent\` | Google OAuth token | Auto-generated after first auth |
| `dropbox_token.txt` | `C:\dev\RLC-Agent\config\` | Dropbox API token | Report distribution |

---

## Main Configuration File (.env)

Create this file at `C:\dev\RLC-Agent\.env`:

```ini
# =============================================================================
# RLC COMMODITIES PLATFORM - CONFIGURATION
# =============================================================================
# Copy this template and fill in your values.
# DO NOT commit this file to Git.
# =============================================================================

# -----------------------------------------------------------------------------
# DATABASE CONNECTION
# -----------------------------------------------------------------------------
# Primary connection (required)
RLC_PG_HOST=<database-host>
RLC_PG_PORT=5432
RLC_PG_DATABASE=rlc_commodities
RLC_PG_USER=<your-username>
RLC_PG_PASSWORD=<your-password>

# Alternate format (some scripts use this)
DATABASE_URL=postgresql://<user>:<password>@<host>:5432/rlc_commodities

# -----------------------------------------------------------------------------
# API KEYS - REQUIRED FOR DATA COLLECTION
# -----------------------------------------------------------------------------

# USDA NASS QuickStats API
# Register: https://quickstats.nass.usda.gov/api
NASS_API_KEY=<your-nass-key>

# US Energy Information Administration
# Register: https://www.eia.gov/opendata/register.php
EIA_API_KEY=<your-eia-key>

# US Census Bureau
# Register: https://api.census.gov/data/key_signup.html
CENSUS_API_KEY=<your-census-key>

# -----------------------------------------------------------------------------
# API KEYS - OPTIONAL
# -----------------------------------------------------------------------------

# Tavily Web Search (for LLM research)
# Register: https://tavily.com
TAVILY_API_KEY=<your-tavily-key>

# Notion API (knowledge base integration)
# Register: https://www.notion.so/my-integrations
NOTION_API_KEY=<your-notion-key>

# USDA FAS API (if separate key required)
FAS_API_KEY=<your-fas-key>

# OpenWeather API (weather data)
OPENWEATHER_API_KEY=<your-openweather-key>

# -----------------------------------------------------------------------------
# OPTIONAL SERVICES
# -----------------------------------------------------------------------------

# Dropbox (report distribution)
DROPBOX_ACCESS_TOKEN=<your-dropbox-token>
DROPBOX_REFRESH_TOKEN=<your-refresh-token>

# Email (SMTP for notifications)
SMTP_HOST=smtp.gmail.com
SMTP_PORT=587
SMTP_USER=<your-email>
SMTP_PASSWORD=<your-app-password>

# -----------------------------------------------------------------------------
# LOCAL PATHS (usually leave as default)
# -----------------------------------------------------------------------------
DATA_CACHE_DIR=./data/cache
REPORTS_OUTPUT_DIR=./reports
LOGS_DIR=./logs
```

---

## Dashboard Configuration File

Create this file at `C:\dev\RLC-Agent\dashboards\ops\.env`:

```ini
# Dashboard Database Connection
# (Same credentials as main .env)
RLC_PG_HOST=<database-host>
RLC_PG_PORT=5432
RLC_PG_DATABASE=rlc_commodities
RLC_PG_USER=<your-username>
RLC_PG_PASSWORD=<your-password>
```

---

## Google API Credentials (Optional)

If you need Gmail/Calendar integration:

### Step 1: Get credentials.json

1. Go to [Google Cloud Console](https://console.cloud.google.com)
2. Create a project (or select existing)
3. Enable Gmail API and Calendar API
4. Create OAuth 2.0 credentials (Desktop app)
5. Download JSON → rename to `credentials.json`
6. Place in `C:\dev\RLC-Agent\`

### Step 2: Generate token.json

1. Run the auth script:
   ```bash
   python scripts/auth_google.py
   ```
2. Browser opens → sign in with your Google account
3. `token.json` is created automatically

---

## Verification Checklist

After setting up files, verify each component:

```bash
# 1. Check main .env is readable
python -c "from dotenv import load_dotenv; load_dotenv(); import os; print('HOST:', os.getenv('RLC_PG_HOST', 'NOT SET'))"

# 2. Check database connection
python -c "from dashboards.ops.db import get_connection; c=get_connection(); print('DB OK')"

# 3. Check API keys are set
python -c "import os; from dotenv import load_dotenv; load_dotenv(); print('NASS:', 'SET' if os.getenv('NASS_API_KEY') else 'MISSING')"

# 4. Launch dashboard
streamlit run dashboards/ops/app.py
```

---

## File Security

⚠️ **Critical Security Notes:**

| DO | DON'T |
|----|-------|
| ✅ Keep `.env` files private | ❌ Commit `.env` to Git |
| ✅ Use strong passwords | ❌ Share credentials via email |
| ✅ Rotate API keys periodically | ❌ Store credentials in code |
| ✅ Use separate keys per user | ❌ Use production keys for testing |

### If Credentials Are Compromised

1. **Immediately** revoke/regenerate the affected key
2. Update `.env` with new key
3. Check logs for unauthorized access
4. Notify team members

---

## Quick Setup Script

For convenience, here's a script to verify your setup:

Save as `scripts/verify_setup.py`:

```python
#!/usr/bin/env python
"""Verify RLC platform setup."""

import os
import sys
from pathlib import Path

def check_file(path, name):
    """Check if file exists."""
    if Path(path).exists():
        print(f"  ✓ {name}")
        return True
    else:
        print(f"  ✗ {name} - MISSING")
        return False

def check_env_var(var_name, required=True):
    """Check if environment variable is set."""
    from dotenv import load_dotenv
    load_dotenv()

    value = os.getenv(var_name)
    if value:
        # Don't print actual values for security
        print(f"  ✓ {var_name}")
        return True
    else:
        status = "MISSING (required)" if required else "not set (optional)"
        print(f"  {'✗' if required else '○'} {var_name} - {status}")
        return not required

def main():
    print("\n=== RLC Platform Setup Verification ===\n")

    all_ok = True

    # Check files
    print("Files:")
    all_ok &= check_file(".env", "Main .env")
    all_ok &= check_file("dashboards/ops/.env", "Dashboard .env")
    check_file("credentials.json", "Google credentials (optional)")

    # Check required env vars
    print("\nRequired Environment Variables:")
    all_ok &= check_env_var("RLC_PG_HOST")
    all_ok &= check_env_var("RLC_PG_DATABASE")
    all_ok &= check_env_var("RLC_PG_USER")
    all_ok &= check_env_var("RLC_PG_PASSWORD")

    # Check API keys
    print("\nAPI Keys (for data collection):")
    all_ok &= check_env_var("NASS_API_KEY")
    all_ok &= check_env_var("EIA_API_KEY")
    all_ok &= check_env_var("CENSUS_API_KEY")

    # Check optional
    print("\nOptional Services:")
    check_env_var("TAVILY_API_KEY", required=False)
    check_env_var("NOTION_API_KEY", required=False)
    check_env_var("DROPBOX_ACCESS_TOKEN", required=False)

    # Database connection test
    print("\nDatabase Connection:")
    try:
        from dashboards.ops.db import get_connection
        with get_connection() as conn:
            cur = conn.cursor()
            cur.execute("SELECT 1")
            print("  ✓ Database connection successful")
    except Exception as e:
        print(f"  ✗ Database connection failed: {e}")
        all_ok = False

    # Summary
    print("\n" + "=" * 40)
    if all_ok:
        print("Setup complete! All required components verified.")
    else:
        print("Setup incomplete. Please address the issues above.")
        sys.exit(1)

if __name__ == "__main__":
    main()
```

Run with:
```bash
python scripts/verify_setup.py
```

---

[← Back to Table of Contents](00_COVER_AND_TOC.md) | [Next: API Key Registration →](APPENDIX_B_API_KEYS.md)
