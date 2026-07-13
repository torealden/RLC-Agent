"""Regenerate ALL feedstock balance-sheet flat files in one shot.

The flat files Desktop links are fully derived from the DB (rake output + NASS/Census/slaughter).
Historically they were three separate writers you had to remember to run individually, and the
runbook's step 8 named only the fats writer — so the oils and slaughter files silently went stale.
This is the single entry point: run it and every feedstock flat file is rebuilt from current data.

It is invoked two ways:
  1. Automatically at the tail of scripts/rake_feedstock_vintage_aware.py (so a rake can't leave the
     flat files behind). Failures there are non-fatal — the rake has already committed.
  2. Manually as runbook step 8:  python scripts/refresh_feedstock_flat_files.py

Exit 0 iff every writer succeeded; exit 1 if any failed (so a dispatcher/CI step can gate on it).
"""
import subprocess
import sys
from pathlib import Path

ROOT = Path(r"C:/dev/RLC-Agent")

# Each writer is self-contained (reads DB, writes its own xlsx under models/). Order is
# independent — they don't depend on each other — but keep it stable for readable logs.
WRITERS = [
    ("fats & greases (6 commodities)", "scripts/write_fats_supply_flat_files.py"),
    ("vegetable oils (SBO + canola)", "scripts/write_oils_supply_flat_files.py"),
    ("livestock slaughter (yield base)", "scripts/write_slaughter_flat_file.py"),
]


def main():
    print("=== refresh feedstock flat files ===")
    failures = []
    for label, rel in WRITERS:
        print(f"\n--- {label}  [{rel}] ---")
        r = subprocess.run([sys.executable, str(ROOT / rel)], cwd=str(ROOT),
                           capture_output=True, text=True)
        # echo the writer's own summary lines (row counts / series) so this stays diagnosable
        for line in (r.stdout or "").splitlines():
            print("   " + line)
        if r.returncode != 0:
            failures.append(label)
            print(f"   *** FAILED (exit {r.returncode}) ***")
            for line in (r.stderr or "").splitlines()[-15:]:
                print("   ! " + line)

    print("\n=== summary ===")
    if failures:
        print(f"{len(failures)}/{len(WRITERS)} writers FAILED: {', '.join(failures)}")
        return 1
    print(f"all {len(WRITERS)} flat-file writers OK")
    return 0


if __name__ == "__main__":
    sys.exit(main())
