#!/usr/bin/env python
"""
Verify RLC platform setup.

This script checks that all required files, environment variables,
and connections are properly configured.

Usage:
    python scripts/verify_setup.py
"""

import os
import sys
from pathlib import Path


def check_file(path, name, required=True):
    """Check if file exists."""
    exists = Path(path).exists()
    if exists:
        print(f"  [OK] {name}")
        return True
    else:
        marker = "[MISSING]" if required else "[optional]"
        print(f"  {marker} {name}")
        return not required


def check_env_var(var_name, required=True):
    """Check if environment variable is set."""
    try:
        from dotenv import load_dotenv
        load_dotenv()
    except ImportError:
        print("  [WARN] python-dotenv not installed, checking system env only")

    value = os.getenv(var_name)
    if value:
        # Don't print actual values for security
        print(f"  [OK] {var_name}")
        return True
    else:
        status = "[MISSING]" if required else "[optional]"
        print(f"  {status} {var_name}")
        return not required


def check_database():
    """Test database connection."""
    try:
        # Try to import and connect
        sys.path.insert(0, str(Path(__file__).parent.parent))
        from dashboards.ops.db import get_connection

        with get_connection() as conn:
            cur = conn.cursor()
            cur.execute("SELECT 1")
            print("  [OK] Database connection successful")
            return True
    except ImportError as e:
        print(f"  [MISSING] Database module not found: {e}")
        return False
    except Exception as e:
        print(f"  [FAILED] Database connection: {e}")
        return False


def check_packages():
    """Check required Python packages."""
    required = ['pandas', 'sqlalchemy', 'psycopg2', 'streamlit', 'dotenv']
    all_ok = True

    for pkg in required:
        try:
            if pkg == 'dotenv':
                __import__('dotenv')
            elif pkg == 'psycopg2':
                __import__('psycopg2')
            else:
                __import__(pkg)
            print(f"  [OK] {pkg}")
        except ImportError:
            print(f"  [MISSING] {pkg}")
            all_ok = False

    return all_ok


def main():
    print("\n" + "=" * 50)
    print("RLC Platform Setup Verification")
    print("=" * 50)

    all_ok = True

    # Get repo root (assuming script is in scripts/)
    repo_root = Path(__file__).parent.parent

    # Check files
    print("\n[1/5] Configuration Files:")
    all_ok &= check_file(repo_root / ".env", "Main .env file")
    all_ok &= check_file(repo_root / "dashboards" / "ops" / ".env", "Dashboard .env file")
    check_file(repo_root / "credentials.json", "Google credentials", required=False)

    # Check Python packages
    print("\n[2/5] Python Packages:")
    all_ok &= check_packages()

    # Check required env vars
    print("\n[3/5] Required Environment Variables:")
    all_ok &= check_env_var("RLC_PG_HOST")
    all_ok &= check_env_var("RLC_PG_DATABASE")
    all_ok &= check_env_var("RLC_PG_USER")
    all_ok &= check_env_var("RLC_PG_PASSWORD")

    # Check API keys
    print("\n[4/5] API Keys (for data collection):")
    all_ok &= check_env_var("NASS_API_KEY")
    all_ok &= check_env_var("EIA_API_KEY")
    all_ok &= check_env_var("CENSUS_API_KEY")

    # Check optional services
    print("\n[4b/5] Optional Services:")
    check_env_var("TAVILY_API_KEY", required=False)
    check_env_var("NOTION_API_KEY", required=False)
    check_env_var("DROPBOX_ACCESS_TOKEN", required=False)

    # Database connection test
    print("\n[5/5] Database Connection:")
    all_ok &= check_database()

    # Summary
    print("\n" + "=" * 50)
    if all_ok:
        print("SUCCESS: All required components verified!")
        print("=" * 50)
        print("\nNext steps:")
        print("  - Launch dashboard: streamlit run dashboards/ops/app.py")
        print("  - Run a collector: python scripts/collect.py --collector cftc_cot")
        sys.exit(0)
    else:
        print("INCOMPLETE: Please address the [MISSING] items above.")
        print("=" * 50)
        print("\nSee docs/user_guide/APPENDIX_A_FILE_LIST.md for setup instructions.")
        sys.exit(1)


if __name__ == "__main__":
    main()
