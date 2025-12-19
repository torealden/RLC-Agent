#!/usr/bin/env python3
"""
RLC-Agent Deployment Script for RLC-SERVER

This script deploys the RLC-Agent codebase to the C:\RLC\projects folder
on the RLC-SERVER Windows machine.

Usage:
    From the RLC-Agent directory on your development machine:
    python deployment/deploy_to_rlc_server.py --target \\RLC-SERVER\c$\RLC\projects

    Or if running on RLC-SERVER directly:
    python deployment/deploy_to_rlc_server.py --target C:\RLC\projects
"""

import shutil
import os
import sys
from pathlib import Path
import argparse
import json
from datetime import datetime


# Deployment configuration
DEPLOY_STRUCTURE = {
    # Core agent code
    "src/agents": [
        "commodity_pipeline/usda_ams_agent",
        "commodity_pipeline/south_america_trade_data",
        "commodity_pipeline/export_inspections_agent",
        "commodity_pipeline/hb_weekly_report_writer",
        "commodity_pipeline/data_collectors",
    ],

    # Entry points and orchestration
    "src/orchestrators": [
        "rlc_master_agent",
    ],

    # Shared utilities and tools
    "src/core": [
        "commodity_pipeline/common",
    ],

    # Configuration and models
    "config": [
        "Models",
        ".env.example",
    ],

    # Database schemas
    "db/migrations": [
        "docs/migrations",
    ],

    # Scripts and entry points
    "scripts": [
        "scripts",
    ],

    # Documentation
    "docs": [
        "docs",
        "ARCHITECTURE_PLAN.md",
        "RLC-Server Set Up Guide.md",
    ],
}

# Files/folders to exclude from deployment
EXCLUDE_PATTERNS = [
    "__pycache__",
    "*.pyc",
    ".git",
    ".env",  # Never deploy actual .env files
    "*.log",
    "*.db",
    "node_modules",
    ".venv",
    "venv",
    "Other Files",  # Archive/legacy
    "api Manager",  # Legacy prototype with security issues
    "exports",  # Data exports
    "*.pbix",  # Power BI files (large binaries)
]


def should_exclude(path: Path) -> bool:
    """Check if a path should be excluded from deployment."""
    name = path.name
    for pattern in EXCLUDE_PATTERNS:
        if pattern.startswith("*"):
            if name.endswith(pattern[1:]):
                return True
        elif name == pattern:
            return True
    return False


def copy_item(src: Path, dest: Path, dry_run: bool = False) -> int:
    """Copy a file or directory, respecting exclusions."""
    copied = 0

    if should_exclude(src):
        return 0

    if src.is_file():
        if not dry_run:
            dest.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(src, dest)
        print(f"  {'[DRY] ' if dry_run else ''}COPY: {src.name}")
        return 1

    elif src.is_dir():
        for item in src.iterdir():
            if not should_exclude(item):
                copied += copy_item(item, dest / item.name, dry_run)

    return copied


def deploy(source_root: Path, target_root: Path, dry_run: bool = False):
    """Deploy RLC-Agent to target location."""

    print(f"\n{'=' * 60}")
    print(f"RLC-Agent Deployment")
    print(f"{'=' * 60}")
    print(f"Source: {source_root}")
    print(f"Target: {target_root}")
    print(f"Mode:   {'DRY RUN' if dry_run else 'LIVE DEPLOYMENT'}")
    print(f"{'=' * 60}\n")

    total_copied = 0

    for target_subdir, source_paths in DEPLOY_STRUCTURE.items():
        print(f"\n[{target_subdir}]")
        target_dir = target_root / target_subdir

        for source_path in source_paths:
            src = source_root / source_path
            if src.exists():
                # Determine destination name
                if src.is_file():
                    dest = target_dir / src.name
                else:
                    dest = target_dir / src.name

                copied = copy_item(src, dest, dry_run)
                total_copied += copied
            else:
                print(f"  [SKIP] Not found: {source_path}")

    print(f"\n{'=' * 60}")
    print(f"Deployment {'would copy' if dry_run else 'copied'} {total_copied} items")
    print(f"{'=' * 60}\n")

    return total_copied


def create_deployment_manifest(source_root: Path, target_root: Path):
    """Create a manifest file documenting the deployment."""
    manifest = {
        "deployment_time": datetime.now().isoformat(),
        "source": str(source_root),
        "target": str(target_root),
        "structure": DEPLOY_STRUCTURE,
        "excluded": EXCLUDE_PATTERNS,
    }

    manifest_path = target_root / "deployment_manifest.json"
    with open(manifest_path, "w") as f:
        json.dump(manifest, f, indent=2)

    print(f"Created deployment manifest: {manifest_path}")


def main():
    parser = argparse.ArgumentParser(description="Deploy RLC-Agent to RLC-SERVER")
    parser.add_argument(
        "--source",
        type=Path,
        default=Path(__file__).parent.parent,
        help="Source RLC-Agent directory (default: parent of this script)"
    )
    parser.add_argument(
        "--target",
        type=Path,
        required=True,
        help="Target deployment directory (e.g., C:\\RLC\\projects\\rlc-agent)"
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would be deployed without actually copying"
    )
    parser.add_argument(
        "--clean",
        action="store_true",
        help="Remove existing deployment before copying"
    )

    args = parser.parse_args()

    source_root = args.source.resolve()
    target_root = args.target.resolve()

    # Validate source
    if not (source_root / "commodity_pipeline").exists():
        print(f"ERROR: Source doesn't look like RLC-Agent: {source_root}")
        sys.exit(1)

    # Clean if requested
    if args.clean and target_root.exists() and not args.dry_run:
        print(f"Cleaning existing deployment: {target_root}")
        shutil.rmtree(target_root)

    # Create target directory
    if not args.dry_run:
        target_root.mkdir(parents=True, exist_ok=True)

    # Deploy
    deploy(source_root, target_root, args.dry_run)

    # Create manifest
    if not args.dry_run:
        create_deployment_manifest(source_root, target_root)

    print("\nNext steps:")
    print(f"  1. Copy .env.example to .env and configure API keys")
    print(f"  2. Install dependencies: cd {target_root} && uv pip install -r requirements.txt")
    print(f"  3. Initialize database: python scripts/init_database.py")
    print(f"  4. Start the master agent: python start_agent.py")


if __name__ == "__main__":
    main()
