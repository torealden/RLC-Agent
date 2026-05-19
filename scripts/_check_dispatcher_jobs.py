"""Read APScheduler in-memory jobs from the running dispatcher's heartbeat."""
import json
from pathlib import Path

hb = Path("scripts/deployment/dispatcher_heartbeat.json")
if hb.exists():
    data = json.loads(hb.read_text())
    print(json.dumps(data, indent=2, default=str)[:5000])
else:
    print(f"No heartbeat file at {hb}")
