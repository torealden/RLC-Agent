"""Categorize all active alerts (priority<=2, unacknowledged) into:
  📥 Tore: manual download / external action (the actionable list)
  🔴 Code fix needed
  🟢 Resolved / noise (safe to bulk-acknowledge)

Writes docs/specs/ops_active_alerts_punchlist.md with the triage and a
bulk-acknowledge SQL for the noise.
"""
import os
import sys
from datetime import datetime
from pathlib import Path

sys.stdout.reconfigure(encoding="utf-8", line_buffering=True)
from dotenv import load_dotenv
load_dotenv()
import psycopg2
from psycopg2.extras import RealDictCursor

PROJECT_ROOT = Path(__file__).resolve().parent.parent
OUT = PROJECT_ROOT / "docs" / "specs" / "ops_active_alerts_punchlist.md"

# Source -> (category, action_note)
# Categories: 'manual', 'code', 'noise'
CLASSIFICATION = {
    'epa_rfs':                  ('manual', 'EPA data files. Their public download moved — likely need to grab from https://www.epa.gov/fuels-registration-reporting-and-compliance-help/public-data-renewable-fuel-standard manually.'),
    'faostat':                  ('manual', 'FAOSTAT bulk CSV download getting 403. Options: register for API key at fao.org/faostat OR download bulk zips manually from https://www.fao.org/faostat/en/#data.'),
    'mpob':                     ('manual', 'Malaysian Palm Oil Board: all endpoints 404. MPOB restructured their public data site — go to https://bepi.mpob.gov.my/ and download monthly Excel files.'),
    'conab':                    ('manual', 'CONAB Brazil: 404 on automated download. Grab the latest Levantamento de Safra PDF/xlsx from https://www.conab.gov.br/info-agro/safras manually.'),
    'usda_ers_oil_crops':       ('manual', 'USDA ERS Oil Crops Yearbook: no data retrieved. Download the latest from https://www.ers.usda.gov/data-products/oil-crops-yearbook/'),
    'usda_ers_feed_grains':     ('manual', 'USDA ERS Feed Grains: HTTP 404. Download from https://www.ers.usda.gov/data-products/feed-grains-database/'),
    'usda_ers_wheat':           ('manual', 'USDA ERS Wheat Data: HTTP 404. Download from https://www.ers.usda.gov/data-products/wheat-data/'),

    # Code fixes — we can address
    'gefs_ensemble':            ('code', 'GEFS ensemble collector returning None error. Code/parser bug.'),
    'ndvi_charts':              ('code', 'NDVICollector.download_all_charts signature mismatch on `commodity` kwarg.'),
    'yield_forecast':           ('code', 'Collector not found in registry — needs registration in collector_registry.py.'),
    'weather_daily_summary':    ('code', 'Same — not in registry. Class file exists, just not registered.'),
    'nass_processing':          ('code', 'NASSProcessingCollector has no .collect() override. Same bug as AMS pre-patch.'),
    'usda_ams_feedstocks':      ('code', 'AMS Tallow/Protein parser: "No data parsed from report". Slug ID may have changed or report format shifted.'),
    'usda_ams_ddgs':            ('code', 'AMS Grain Co-Products parser: "No data parsed from report". Same family of issue as feedstocks.'),
    'eia_ethanol':              ('code', 'Reports needing EIA_API_KEY but the key IS set. Code likely missing the env-var lookup.'),
    'cme_settlements':          ('code', 'Listed as OVERDUE but daily run is operating normally — alert logic needs a tighter threshold.'),

    # Pre-fix events from this week — these will stop with next runs
    'usda_fas_export_sales':    ('noise', 'Pre-fix failures from earlier today (before v2 collector landed). Tomorrow run is on the v2 path; alerts will stop.'),

    # Already-fixed from earlier sessions, alerts linger
    'canada_cgc':               ('noise', 'Fixed in March 2026 (rewrite to CSV bulk download). Old alerts pre-fix.'),
    'canada_statscan':          ('noise', 'Fixed in March 2026. Old alerts pre-fix.'),
    'drought_monitor':          ('noise', 'Fixed in March 2026 (FIPS codes + CSV parsing). Old alerts pre-fix.'),
    'census_trade':             ('noise', 'Old March connection errors; collector is healthy now (470k rows).'),
    'cftc_cot':                 ('noise', 'Single old isoformat error from March. Collector healthy now.'),

    # Success notifications / informational that bled into alerts at priority<=2
    'dispatcher_watchdog':      ('noise', 'Healthy operational events (watchdog restarts). Acknowledge to clear UI.'),
    'wasde_monthly':            ('noise', 'These are `report_generated` success events at priority=1. Acknowledge.'),
    'knowledge_graph':          ('noise', 'KG batch-load success notifications.'),
    'kg_extraction':            ('noise', 'KG extraction success notifications.'),
    'DOC':                      ('noise', 'Daily ops anomaly counts. Review but not blocking.'),
    'epa_echo_biodiesel':       ('noise', 'May 1 transient "No facilities found" from EPA ECHO. Resolved — recent runs succeed.'),
    'epa_echo_ethanol':         ('noise', 'Same May 1 transient. Resolved.'),
    'epa_echo_milling':         ('noise', 'Same May 1 transient. Resolved.'),
}


conn = psycopg2.connect(
    host=os.getenv("RLC_PG_HOST"), port=os.getenv("RLC_PG_PORT", "5432"),
    dbname=os.getenv("RLC_PG_DB", "rlc_commodities"),
    user=os.getenv("RLC_PG_USER"), password=os.getenv("RLC_PG_PASSWORD"),
    sslmode="require",
)
cur = conn.cursor(cursor_factory=RealDictCursor)

# Pull all active alerts grouped by source
cur.execute("""
    SELECT source,
           COUNT(*) AS n,
           MIN(event_time) AS first_seen,
           MAX(event_time) AS last_seen,
           STRING_AGG(DISTINCT LEFT(COALESCE(summary,''), 200), ' || ') AS sample_summaries,
           ARRAY_AGG(id ORDER BY id) AS alert_ids
    FROM core.event_log
    WHERE priority <= 2 AND acknowledged = FALSE
    GROUP BY source
""")
by_source = {r['source']: r for r in cur.fetchall()}

# Group sources into the three buckets
manual_sources, code_sources, noise_sources = [], [], []
unknown_sources = []
for src, data in by_source.items():
    cat_info = CLASSIFICATION.get(src)
    if not cat_info:
        unknown_sources.append((src, data))
        continue
    cat, note = cat_info
    item = (src, data, note)
    if cat == 'manual':   manual_sources.append(item)
    elif cat == 'code':   code_sources.append(item)
    else:                 noise_sources.append(item)

manual_sources.sort(key=lambda x: x[1]['n'], reverse=True)
code_sources.sort(key=lambda x: x[1]['n'], reverse=True)
noise_sources.sort(key=lambda x: x[1]['n'], reverse=True)

noise_ids = []
for src, data, note in noise_sources:
    noise_ids.extend(data['alert_ids'])

lines = []
P = lines.append
P("# Ops dashboard — Active Alerts triage")
P("")
P(f"*Generated {datetime.utcnow():%Y-%m-%d %H:%M UTC}. "
  f"Total active: {sum(d['n'] for d in by_source.values())} alerts across {len(by_source)} sources.*")
P("")
P("Three buckets:")
P("- 📥 **Manual** — Tore action needed (download a file, register for an API key, etc.)")
P("- 🔴 **Code** — Claude-UI action (collector bug or registration issue)")
P("- 🟢 **Noise** — already resolved or informational; safe to bulk-acknowledge")
P("")
P("---")
P("")

# Manual bucket
P(f"## 📥 Manual — {len(manual_sources)} sources, {sum(d[1]['n'] for d in manual_sources)} alerts total")
P("")
P("**These are the spreadsheets/downloads you asked about.** Each one is a "
  "publicly-accessible USDA / EPA / global-government data file whose URL the "
  "collector can't reach automatically (auth change, URL change, server 403).")
P("")
P("| Source | Count | Last seen | What to download |")
P("|--------|------:|-----------|------------------|")
for src, data, note in manual_sources:
    P(f"| `{src}` | {data['n']} | {str(data['last_seen'])[:16]} | {note} |")
P("")

# Code bucket
P(f"## 🔴 Code — {len(code_sources)} sources, {sum(d[1]['n'] for d in code_sources)} alerts total")
P("")
P("Real collector bugs. Tasks created or referenced where applicable.")
P("")
P("| Source | Count | Last seen | Diagnosis |")
P("|--------|------:|-----------|-----------|")
for src, data, note in code_sources:
    P(f"| `{src}` | {data['n']} | {str(data['last_seen'])[:16]} | {note} |")
P("")

# Noise bucket
P(f"## 🟢 Noise — {len(noise_sources)} sources, {sum(d[1]['n'] for d in noise_sources)} alerts total")
P("")
P("Already-resolved failures still showing in the UI because nobody acknowledged "
  "them. Safe to bulk-clear with the SQL below.")
P("")
P("| Source | Count | Last seen | Why noise |")
P("|--------|------:|-----------|-----------|")
for src, data, note in noise_sources:
    P(f"| `{src}` | {data['n']} | {str(data['last_seen'])[:16]} | {note} |")
P("")

P("### Bulk-acknowledge SQL (clears the noise bucket)")
P("")
P("```sql")
P(f"UPDATE core.event_log SET acknowledged = TRUE")
P(f"WHERE id = ANY(ARRAY[{','.join(str(i) for i in noise_ids)}]);")
P("```")
P("")
P(f"*(That clears {len(noise_ids)} rows.)*")
P("")

if unknown_sources:
    P("")
    P(f"## ❓ Unclassified — {len(unknown_sources)} sources")
    P("")
    P("These didn't match my classification table; re-run with a triage update.")
    for src, data in unknown_sources:
        P(f"- `{src}` (n={data['n']})")
    P("")

OUT.parent.mkdir(parents=True, exist_ok=True)
OUT.write_text("\n".join(lines), encoding="utf-8")
print(f"Wrote: {OUT}")
print()
print(f"Summary:")
print(f"  📥 Manual:  {sum(d[1]['n'] for d in manual_sources)} alerts ({len(manual_sources)} sources)")
print(f"  🔴 Code:    {sum(d[1]['n'] for d in code_sources)} alerts ({len(code_sources)} sources)")
print(f"  🟢 Noise:   {sum(d[1]['n'] for d in noise_sources)} alerts ({len(noise_sources)} sources)")
if unknown_sources:
    print(f"  ❓ Unclassified: {sum(d[1]['n'] for d in unknown_sources)} alerts")
conn.close()
