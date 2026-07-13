"""Estimate poultry-fat (PF) biofuel supply for months EIA withholds it.

EIA suppresses poultry-fat biofuel feedstock use from Sep 2025 on (is_withheld), so
populate_silver_feedstock_supply (which only loads non-withheld EIA) leaves PF with no supply past
Aug 2025 — the allocator then zero-allocates PF and gold.bbd_feedstock_raked goes stale at Nov 2024.

PF is a small, EIA-suppressed feedstock: biofuel use ~14-25 mil lb/mo (~7-13% of NASS poultry-fat
production; the bulk goes to pet food / feed / oleochem, i.e. non-bio). So the fill is a trailing-12mo
average of the last non-withheld EIA PF biofuel use — NOT NASS production, which would overstate it
~10x. Regional split copies the last non-withheld month's shares. Flagged source='PF_EIA_ESTIMATE'
(traceable, reversible, auto-retires when EIA resumes reporting PF). Re-run the allocator after this.
"""
import sys
from pathlib import Path

ROOT = Path(r"C:/dev/RLC-Agent"); sys.path.insert(0, str(ROOT))
from dotenv import load_dotenv; load_dotenv(ROOT / ".env")
from src.services.database.db_config import get_connection

with get_connection() as conn:
    cur = conn.cursor()
    # 1. national trailing-12mo avg of non-withheld EIA PF biofuel use
    cur.execute("""SELECT avg(quantity_mil_lbs) a FROM (
                     SELECT quantity_mil_lbs FROM bronze.eia_feedstock_monthly
                     WHERE plant_type='total' AND feedstock_name ILIKE '%poultry%'
                       AND NOT is_withheld AND quantity_mil_lbs IS NOT NULL
                     ORDER BY year DESC, month DESC LIMIT 12) t""")
    est_national = float(cur.fetchone()['a'])

    # 2. last non-withheld PF month's regional shares (from feedstock_supply)
    cur.execute("SELECT max(period) mx FROM silver.feedstock_supply WHERE feedstock_code='PF'")
    last_pf = cur.fetchone()['mx']
    cur.execute("""SELECT region, net_available_biofuel v FROM silver.feedstock_supply
                   WHERE feedstock_code='PF' AND period=%s""", (last_pf,))
    reg = {r['region']: float(r['v'] or 0) for r in cur.fetchall()}
    tot = sum(reg.values()) or 1.0
    shares = {k: v / tot for k, v in reg.items()}

    # 3. price ladder: reuse last known PF price per region
    cur.execute("""SELECT DISTINCT ON (region) region, avg_price_per_lb p FROM silver.feedstock_supply
                   WHERE feedstock_code='PF' AND avg_price_per_lb IS NOT NULL
                   ORDER BY region, period DESC""")
    price = {r['region']: r['p'] for r in cur.fetchall()}

    # 4. target months: withheld EIA PF months from first-withheld through the EIA feedstock frontier
    cur.execute("""SELECT DISTINCT make_date(year,month,1) p FROM bronze.eia_feedstock_monthly
                   WHERE plant_type='total' AND feedstock_name ILIKE '%%poultry%%' AND is_withheld
                     AND make_date(year,month,1) > %s ORDER BY 1""", (last_pf,))
    months = [r['p'] for r in cur.fetchall()]

    # 5. write (idempotent: clear prior estimate rows first)
    cur.execute("DELETE FROM silver.feedstock_supply WHERE feedstock_code='PF' AND source='PF_EIA_ESTIMATE'")
    n = 0
    for p in months:
        for region, share in shares.items():
            v = est_national * share
            cur.execute("""INSERT INTO silver.feedstock_supply
                (period,feedstock_code,region,net_available_biofuel,avg_price_per_lb,domestic_production,imports,total_available,source)
                VALUES (%s,'PF',%s,%s,%s,%s,0,%s,'PF_EIA_ESTIMATE')
                ON CONFLICT (period,feedstock_code,region) DO UPDATE
                  SET net_available_biofuel=EXCLUDED.net_available_biofuel,
                      domestic_production=EXCLUDED.domestic_production,
                      total_available=EXCLUDED.total_available, source='PF_EIA_ESTIMATE'""",
                (p, region, v, price.get(region), v, v))
            n += 1
    conn.commit()
    print(f"PF trailing estimate = {est_national:.1f} mil lb/mo (national), split by {last_pf} regional shares")
    print(f"wrote {n} PF rows (source PF_EIA_ESTIMATE) for {len(months)} withheld months "
          f"{str(months[0])[:7] if months else '-'}..{str(months[-1])[:7] if months else '-'}")
