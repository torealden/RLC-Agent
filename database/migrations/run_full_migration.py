"""
Full Migration Script - Bronze/Silver/Gold Database Setup
Runs all migration steps in order.

Usage:
    python run_full_migration.py

Steps:
    1. Inventory existing PostgreSQL data
    2. Deploy medallion schema (core, audit, bronze, silver, gold)
    3. Migrate existing PostgreSQL data to Bronze
    4. Migrate SQLite data to Bronze
"""

import sys
from pathlib import Path

# Add migrations directory to path
sys.path.insert(0, str(Path(__file__).parent))

from importlib import import_module


def run_migration():
    print("=" * 70)
    print("RLC COMMODITIES - FULL DATABASE MIGRATION")
    print("Bronze / Silver / Gold Medallion Architecture")
    print("=" * 70)

    steps = [
        ("01_inventory_postgres", "Inventory PostgreSQL Database"),
        ("02_deploy_medallion_schema", "Deploy Medallion Schema"),
        ("03_migrate_existing_to_bronze", "Migrate Existing Data to Bronze"),
        ("04_migrate_sqlite_to_bronze", "Migrate SQLite Data to Bronze"),
    ]

    results = []

    for module_name, description in steps:
        print(f"\n{'='*70}")
        print(f"STEP: {description}")
        print(f"{'='*70}")

        response = input(f"\nRun this step? (Y/n): ").strip().lower()
        if response == 'n':
            print("Skipped.")
            results.append((description, "SKIPPED"))
            continue

        try:
            module = import_module(module_name)

            # Find and run main function
            if hasattr(module, 'inventory_database'):
                success = module.inventory_database() is not None
            elif hasattr(module, 'deploy_schema'):
                success = module.deploy_schema()
            elif hasattr(module, 'migrate_to_bronze'):
                success = module.migrate_to_bronze()
            elif hasattr(module, 'migrate_sqlite_to_bronze'):
                success = module.migrate_sqlite_to_bronze()
            else:
                print(f"No entry point found in {module_name}")
                success = False

            results.append((description, "SUCCESS" if success else "FAILED"))

        except Exception as e:
            print(f"Error running {module_name}: {e}")
            results.append((description, f"ERROR: {e}"))

    # Final summary
    print("\n" + "=" * 70)
    print("MIGRATION SUMMARY")
    print("=" * 70)
    for step, status in results:
        print(f"  {step}: {status}")

    print("\n" + "=" * 70)
    print("NEXT STEPS")
    print("=" * 70)
    print("1. Connect Power BI to the new schema tables:")
    print("   - bronze.* for raw data")
    print("   - silver.observation for time-series")
    print("   - gold.* for business views")
    print("")
    print("2. Update collectors to write to bronze schema")
    print("3. Configure silver layer transformations")
    print("4. Verify data in Power BI dashboards")


if __name__ == "__main__":
    run_migration()
