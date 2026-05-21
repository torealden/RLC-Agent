"""
Pull a comprehensive snapshot of US soybean crush data for Tore's June 4
conference presentation. Produces a markdown file at
`docs/specs/crush_outlook_data_snapshot.md` that summarizes:

  - Most recent monthly NASS crush + YoY
  - Pace vs USDA WASDE projection (current marketing year)
  - Monthly crush trend (last 12 months)
  - Crush margin / IFV-adjacent indicators
  - Capacity context (silver.facility_map oilseed crush)
  - Exports demand context (FAS ESR soybeans)
  - Key open questions the data raises

Read it once before drafting the presentation. Numbers are pulled live —
re-run anytime for fresher cuts.
"""
import os
import sys
from datetime import date, datetime
from pathlib import Path

sys.stdout.reconfigure(encoding="utf-8", line_buffering=True)
from dotenv import load_dotenv
load_dotenv()
import psycopg2
from psycopg2.extras import RealDictCursor

PROJECT_ROOT = Path(__file__).resolve().parent.parent
OUT = PROJECT_ROOT / "docs" / "specs" / "crush_outlook_data_snapshot.md"

conn = psycopg2.connect(
    host=os.getenv("RLC_PG_HOST"),
    port=os.getenv("RLC_PG_PORT", "5432"),
    dbname=os.getenv("RLC_PG_DB", "rlc_commodities"),
    user=os.getenv("RLC_PG_USER"),
    password=os.getenv("RLC_PG_PASSWORD"),
    sslmode="require",
)
cur = conn.cursor(cursor_factory=RealDictCursor)

lines: list[str] = []
P = lines.append

P(f"# US Soybean Crush — Outlook Data Snapshot")
P(f"")
P(f"*Generated {datetime.utcnow().strftime('%Y-%m-%d %H:%M UTC')} for June 4 conference prep.*")
P(f"")
P(f"---")
P(f"")

# ----------------------------------------------------------------------
# Section 1 — Monthly crush trend (NASS Grain Crushings — corn for ethanol)
# and Oil Crushings (soybeans crushed for meal+oil)
# ----------------------------------------------------------------------
P(f"## 1. Monthly soybean crush — last 12 months")
P(f"")

try:
    cur.execute("""
        SELECT calendar_year, month, realized_value
        FROM silver.monthly_realized
        WHERE commodity = 'soybeans' AND attribute = 'crush'
          AND source LIKE 'NASS%'
        ORDER BY calendar_year DESC, month DESC
        LIMIT 13
    """)
    rows = cur.fetchall()
    if rows:
        P(f"| Year | Month | Crush (mil bu)* | YoY Δ |")
        P(f"|------|-------|-----------------|-------|")
        # Build YoY lookup by (year-1, month)
        by_ym = {(r['calendar_year'], r['month']): r['realized_value'] for r in rows}
        # Pull prior 12 months for YoY denominators
        cur.execute("""
            SELECT calendar_year, month, realized_value
            FROM silver.monthly_realized
            WHERE commodity='soybeans' AND attribute='crush'
              AND source LIKE 'NASS%' AND calendar_year >= 2023
            ORDER BY calendar_year DESC, month DESC
            LIMIT 36
        """)
        all_rows = cur.fetchall()
        all_by = {(r['calendar_year'], r['month']): r['realized_value'] for r in all_rows}
        for r in rows[:12]:
            y, m, v = r['calendar_year'], r['month'], r['realized_value']
            prev = all_by.get((y - 1, m))
            yoy = f"{((v - prev) / prev * 100):+.1f}%" if prev else "—"
            v_mil = (float(v) / 1_000_000) if v else 0
            P(f"| {y} | {m:02d} | {v_mil:>14,.1f} | {yoy} |")
        P(f"")
        P(f"*Units: monthly crush in mil bu. Source: NASS Oil Crushings monthly report.*")
    else:
        P(f"_No NASS soybean crush data found in silver.monthly_realized._")
except Exception as e:
    P(f"_Query error: {e}_")
    conn.rollback()

P(f"")

# ----------------------------------------------------------------------
# Section 2 — Pace vs USDA WASDE projection (current MY)
# ----------------------------------------------------------------------
P(f"## 2. Crush pace vs USDA projection — current marketing year")
P(f"")
try:
    # Check the pace tracking computed contexts in kg_context
    cur.execute("""
        SELECT n.node_key, c.context_key, c.context_value, c.last_updated
        FROM core.kg_context c
        JOIN core.kg_node n ON c.node_id = n.id
        WHERE n.node_key IN ('nopa.crush', 'soybean_crush', 'soybeans')
          AND c.context_type = 'pace_tracking'
        ORDER BY c.last_updated DESC
        LIMIT 5
    """)
    rows = cur.fetchall()
    if rows:
        for r in rows:
            P(f"**{r['node_key']} / {r['context_key']}** (updated {r['last_updated']}):")
            P(f"")
            cv = r['context_value']
            if isinstance(cv, dict):
                for k, v in list(cv.items())[:8]:
                    P(f"- {k}: {v}")
            else:
                P(f"`{str(cv)[:600]}`")
            P(f"")
    else:
        # Fallback: compute pace inline
        P(f"_No precomputed pace_tracking contexts found. Inline computation:_")
        P(f"")
        cur.execute("""
            SELECT marketing_year, SUM(realized_value) / 1e9 AS cumulative_bbu
            FROM silver.monthly_realized
            WHERE commodity='soybeans' AND attribute='crush'
              AND source LIKE 'NASS%' AND marketing_year >= 2023
            GROUP BY 1 ORDER BY 1 DESC LIMIT 4
        """)
        for r in cur.fetchall():
            P(f"- MY {r['marketing_year']}/{(r['marketing_year']+1)%100:02d}: cumulative {r['cumulative_bbu']:.3f} bil bu")
except Exception as e:
    P(f"_Query error: {e}_")
    conn.rollback()

P(f"")

# ----------------------------------------------------------------------
# Section 3 — US soybean S&D snapshot (latest WASDE / PSD)
# ----------------------------------------------------------------------
P(f"## 3. US soybean S&D — most recent 3 marketing years")
P(f"")
try:
    # PSD has multiple records per MY (different USDA monthly releases).
    # Pick the latest collected for each MY.
    cur.execute("""
        SELECT DISTINCT ON (marketing_year)
               marketing_year, production, imports, beginning_stocks,
               crush, exports, ending_stocks, domestic_consumption
        FROM bronze.fas_psd
        WHERE commodity='soybeans' AND country_code='US'
          AND marketing_year >= 2022
        ORDER BY marketing_year DESC, collected_at DESC
        LIMIT 5
    """)
    rows = cur.fetchall()
    if rows:
        P(f"| MY | Production | Imports | Beg Stocks | Crush | Exports | End Stocks | Stocks/Use |")
        P(f"|----|-----------:|--------:|-----------:|------:|--------:|-----------:|-----------:|")
        for r in rows:
            stu = ""
            if r['ending_stocks'] and (r['crush'] or 0) + (r['exports'] or 0):
                stu = f"{r['ending_stocks'] / ((r['crush'] or 0) + (r['exports'] or 0)) * 100:.1f}%"
            P(f"| {r['marketing_year']} | {r['production']:>10,.0f} | {r['imports']:>7,.0f} | {r['beginning_stocks']:>10,.0f} | {r['crush'] or 0:>5,.0f} | {r['exports']:>7,.0f} | {r['ending_stocks']:>10,.0f} | {stu:>10s} |")
        P(f"")
        P(f"*Units: 1000 MT.*")
    else:
        P(f"_No PSD data found for US soybeans._")
except Exception as e:
    P(f"_Query error: {e}_")
    conn.rollback()

P(f"")

# ----------------------------------------------------------------------
# Section 4 — Soybean oil + meal monthly production (downstream of crush)
# ----------------------------------------------------------------------
P(f"## 4. Soybean oil production — last 12 months")
P(f"")
try:
    # Two sources report oil production (NASS_FATS_OILS, NASS_SOY_CRUSH).
    # Prefer NASS_FATS_OILS as the canonical source; dedupe on (year, month).
    cur.execute("""
        SELECT DISTINCT ON (calendar_year, month)
               calendar_year, month, realized_value, source
        FROM silver.monthly_realized
        WHERE commodity='soybeans' AND attribute='oil_production_crude'
          AND source = 'NASS_FATS_OILS'
        ORDER BY calendar_year DESC, month DESC, source
        LIMIT 13
    """)
    rows = cur.fetchall()
    if rows:
        cur.execute("""
            SELECT DISTINCT ON (calendar_year, month)
                   calendar_year, month, realized_value
            FROM silver.monthly_realized
            WHERE commodity='soybeans' AND attribute='oil_production_crude'
              AND source = 'NASS_FATS_OILS' AND calendar_year >= 2023
            ORDER BY calendar_year DESC, month DESC, source
            LIMIT 36
        """)
        all_rows = cur.fetchall()
        full_by = {(r['calendar_year'], r['month']): r['realized_value'] for r in all_rows}

        P(f"| Year | Month | Oil produced (bil lbs) | YoY Δ |")
        P(f"|------|-------|-----------------------:|-------|")
        for r in rows[:12]:
            y, m, v = r['calendar_year'], r['month'], r['realized_value']
            prev = full_by.get((y - 1, m))
            yoy = f"{((v - prev) / prev * 100):+.1f}%" if prev else "—"
            P(f"| {y} | {m:02d} | {(float(v)/1e9):>20,.3f} | {yoy} |")
        P(f"")
        P(f"*Source: NASS Fats & Oils monthly report. crude oil production only.*")
    else:
        P(f"_No NASS soybean oil production data._")
except Exception as e:
    P(f"_Query error: {e}_")
    conn.rollback()

P(f"")

# ----------------------------------------------------------------------
# Section 5 — Soybean exports (FAS ESR — should be fresh after backfill)
# ----------------------------------------------------------------------
P(f"## 5. Soybean exports — current MY 2025/26 (weeks 2025-09-04 onward)")
P(f"")
try:
    cur.execute("""
        SELECT week_ending,
               SUM(weekly_exports) AS weekly,
               SUM(accumulated_exports) FILTER (WHERE country_code = '0') AS world_accumulated,
               COUNT(DISTINCT country_code) AS countries_active
        FROM bronze.fas_export_sales
        WHERE commodity='soybeans' AND marketing_year = 2025
          AND week_ending >= '2026-01-01'
        GROUP BY week_ending ORDER BY week_ending DESC LIMIT 12
    """)
    rows = cur.fetchall()
    if rows:
        P(f"| Week ending | Weekly exports (MT) | Cumulative MY (MT) | Active buyers |")
        P(f"|-------------|---------------------:|--------------------:|--------------:|")
        for r in rows:
            wk = float(r['weekly']) if r['weekly'] else 0
            cum = float(r['world_accumulated']) if r['world_accumulated'] else 0
            P(f"| {r['week_ending']} | {wk:>18,.0f} | {cum:>18,.0f} | {r['countries_active']:>12} |")
        P(f"")
    else:
        P(f"_FAS ESR data not yet refreshed for soybeans MY 2025/26._")
except Exception as e:
    P(f"_Query error: {e}_")
    conn.rollback()

P(f"")

# ----------------------------------------------------------------------
# Section 6 — Top destinations YoY (current MY vs prior MY same period)
# ----------------------------------------------------------------------
P(f"## 6. Soybean exports — top 10 destinations, MY 2025/26 vs 2024/25")
P(f"")
try:
    cur.execute("""
        WITH cur_my AS (
            SELECT country_code, SUM(weekly_exports) AS total
            FROM bronze.fas_export_sales
            WHERE commodity='soybeans' AND marketing_year=2025
            GROUP BY country_code
        ),
        prev_my AS (
            SELECT country_code, SUM(weekly_exports) AS total
            FROM bronze.fas_export_sales
            WHERE commodity='soybeans' AND marketing_year=2024
            GROUP BY country_code
        )
        SELECT cur_my.country_code, cur_my.total AS current_total,
               prev_my.total AS prior_total
        FROM cur_my LEFT JOIN prev_my USING (country_code)
        WHERE cur_my.country_code != '0' AND cur_my.total > 0
        ORDER BY cur_my.total DESC LIMIT 10
    """)
    rows = cur.fetchall()
    if rows:
        P(f"| Country code | Current MY (MT) | Prior MY (MT) | YoY Δ |")
        P(f"|-------------|----------------:|--------------:|------:|")
        for r in rows:
            cur_t = float(r['current_total']) if r['current_total'] else 0
            prev_t = float(r['prior_total']) if r['prior_total'] else 0
            yoy = f"{((cur_t - prev_t) / prev_t * 100):+.1f}%" if prev_t else "n/a"
            P(f"| {r['country_code']} | {cur_t:>15,.0f} | {prev_t:>13,.0f} | {yoy:>5s} |")
        P(f"")
        P(f"*Country codes are FAS numeric. Top 10 buyers ranked by current MY total.*")
    else:
        P(f"_FAS ESR data not yet refreshed for soybeans._")
except Exception as e:
    P(f"_Query error: {e}_")
    conn.rollback()

P(f"")

# ----------------------------------------------------------------------
# Section 7 — Crusher capacity context
# ----------------------------------------------------------------------
P(f"## 7. US oilseed crush capacity — facility footprint")
P(f"")
try:
    cur.execute("""
        SELECT state, COUNT(*) AS facilities,
               SUM(nameplate_mmbu_yr) AS total_mmbu_yr
        FROM silver.facility_map
        WHERE industry_code IN ('oilseed_crush', 'oilseed_crush_other')
          AND country = 'US' AND status_normalized = 'operating'
        GROUP BY state ORDER BY 3 DESC NULLS LAST LIMIT 15
    """)
    rows = cur.fetchall()
    if rows:
        P(f"| State | Crushers | Total mil bu/yr |")
        P(f"|-------|---------:|----------------:|")
        for r in rows:
            cap = f"{float(r['total_mmbu_yr']):>14,.0f}" if r['total_mmbu_yr'] else "—"
            P(f"| {r['state']} | {r['facilities']:>8} | {cap} |")
        P(f"")
        cur.execute("""
            SELECT COUNT(*) AS n, SUM(nameplate_mmbu_yr) AS cap
            FROM silver.facility_map
            WHERE industry_code IN ('oilseed_crush','oilseed_crush_other')
              AND country='US' AND status_normalized='operating'
        """)
        r = cur.fetchone()
        cap = float(r['cap']) if r['cap'] else 0
        P(f"**US total operating oilseed-crush facilities: {r['n']}.**")
        if cap:
            P(f"**Reported nameplate where available: {cap:,.0f} mil bu/yr.** "
              f"(Subset of facilities have capacity in our database; coverage gap is the curated capacity workstream.)")
    else:
        P(f"_silver.facility_map oilseed crush returned no rows._")
except Exception as e:
    P(f"_Query error: {e}_")
    conn.rollback()

P(f"")

# ----------------------------------------------------------------------
# Section 8 — IFV / margin context
# ----------------------------------------------------------------------
P(f"## 8. BBD feedstock value pull on crush — context")
P(f"")
P(f"The crush outlook can't be told without the BBD/RD demand pull. Key data points:")
P(f"")
try:
    cur.execute("""
        SELECT MAX(date::date) AS latest_date,
               COUNT(*) AS rows
        FROM bronze.feedstock_prices
        WHERE commodity = 'sbo'
    """)
    r = cur.fetchone()
    if r:
        P(f"- **bronze.feedstock_prices** for SBO: latest {r['latest_date']}, {r['rows']:,} rows total.")
except Exception:
    conn.rollback()

try:
    cur.execute("""
        SELECT MAX(period::date) AS latest, COUNT(*) AS rows
        FROM bronze.eia_biofuels_monthly
        WHERE LOWER(series_name) LIKE '%soybean oil%' OR LOWER(series_name) LIKE '%sbo%'
    """)
    r = cur.fetchone()
    if r:
        P(f"- **EIA monthly biofuel SBO consumption series**: latest {r['latest']}, {r['rows']:,} rows.")
except Exception:
    conn.rollback()

try:
    cur.execute("""
        SELECT MAX(week_ending::date) AS latest, COUNT(*) AS rows
        FROM bronze.epa_rfs_rin_generation
        WHERE rin_type = 'D4'
    """)
    r = cur.fetchone()
    if r:
        P(f"- **EPA D4 RIN generation** (biomass-based diesel pool): latest {r['latest']}, {r['rows']:,} rows.")
except Exception:
    conn.rollback()

P(f"")

# ----------------------------------------------------------------------
# Section 9 — Open data questions worth answering before the talk
# ----------------------------------------------------------------------
P(f"## 9. Open questions / things to check before the talk")
P(f"")
P(f"- Is the WASDE projection for MY 2025/26 crush still in line with NASS month-over-month?")
P(f"- What's the spread between board crush margin and our implied feedstock value?")
P(f"- BBD demand share of total domestic SBO use in current MY vs trailing 5-yr average.")
P(f"- China share of current-MY exports vs prior MY — is the rebalancing into other markets durable?")
P(f"- Domestic crush capacity additions for 2026/27 — facility-level (who, when, how many bushels).")
P(f"- 45Z policy scenario sensitivity on RD demand for SBO (use the IFV kg_callable for this).")

P(f"")
P(f"---")
P(f"")
P(f"*Re-run `python scripts/_crush_outlook_data_snapshot.py` anytime for refreshed numbers.*")

OUT.parent.mkdir(parents=True, exist_ok=True)
OUT.write_text("\n".join(lines), encoding="utf-8")
print(f"Wrote: {OUT}")
print(f"Lines: {len(lines)}")
conn.close()
