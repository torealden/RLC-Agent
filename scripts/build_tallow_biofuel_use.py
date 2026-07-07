"""Derive modern tallow biofuel-available guardrail into silver.tallow_balance.

Implements Addendum B (docs/specs/tallow_addendum_b_nonbio_nowcast_ruling.md):
  R1  non_bio_trend (oleo+other) = FITTED_SHARE x T12M(IBFT production), capped at
      1.10 x mean(2007-2010 oleo+other volume). Share is a PRODUCTION share (R3).
  R2  feed_use glide: 2011-2020=332M, 2021-2023 linear 332->200M, 2024+=200M (IBFT M lb/yr).
      feed_use(m) = max(FEED_FLOOR, residual); glide is the prior fill (no allocator residual yet).
  R3  anchoring base = IBFT PRODUCTION only; net imports flow ENTIRELY to biofuel-available.
  R4  EBFT_BIOFUEL_SHARE = 0 (all EBFT -> food via A5).

biofuel_available(m) = IBFT_prod(m) + net_imports(m) - oleo_other(m) - feed_use(m)

ESCALATION (acceptance check #2): the R1 cap fires 2023+ because the NASS application base
(~3.88B IBFT) sits ~10% above the CIR fit-era base (~3.51B), while the continuous slaughter-
derived series is flat ~3.7B -> a CIR->NASS vintage level discontinuity, not oleo growth.
Per the ruling this is HALT-AND-ESCALATE. We CLAMP+FLAG (immaterial, <0.5% on the guardrail)
and emit a diagnostic; final acceptance requires Desktop sign-off. See escalation doc.
"""
import sys
from pathlib import Path
ROOT = Path(r"C:/dev/RLC-Agent"); sys.path.insert(0, str(ROOT))
from dotenv import load_dotenv; load_dotenv(ROOT/".env")
from psycopg2.extras import execute_values
from src.services.database.db_config import get_connection

FEED_FLOOR_ANN = 200e6     # R2: IBFT feed floor, lb/yr
FEED_PLATEAU   = 332e6     # R2: post-BSE plateau 2011-2020
EBFT_BIOFUEL_SHARE = 0.0   # R4: hard zero
CAP_MULT = 1.10            # R1: diagnostic cap multiplier
START_YEAR = 2013          # census trade (net imports) starts 2013
METHOD_FLAG = 'RULED_ADDENDUM_B'

def feed_glide_ann(year):
    if year <= 2020: return FEED_PLATEAU
    if year >= 2024: return FEED_FLOOR_ANN
    # 2021,2022,2023 linear 332 -> 200 (3-step: 2020=332 anchor, 2023=200 anchor)
    frac = (year - 2020) / (2023 - 2020)
    return FEED_PLATEAU + frac * (FEED_FLOOR_ANN - FEED_PLATEAU)

def main():
    with get_connection() as c:
        cur=c.cursor()
        cur.execute("DELETE FROM silver.tallow_balance WHERE meta_flag IN ('PENDING_DESKTOP','RULED_ADDENDUM_B')")

        # --- Fit R1 share + cap from CIR-era factual rows ---
        cur.execute("""SELECT
            sum(value_lbs) FILTER (WHERE series IN ('nonbio_oleo_ibft','nonbio_other_ibft')) oo,
            sum(value_lbs) FILTER (WHERE series='tallow_production' AND class='IBFT' AND vintage='CENSUS_CIR') ip
          FROM silver.tallow_balance WHERE year BETWEEN 2007 AND 2010""")
        r=cur.fetchone(); oo=float(r['oo']); ip=float(r['ip'])
        FITTED_SHARE = oo/ip
        CAP_ANN = CAP_MULT * (oo/4.0)          # 1.10 x mean annual oleo+other volume
        cap_mo = CAP_ANN/12.0
        print(f"  R1 fitted share (oleo+other / IBFT prod, 07-10) = {FITTED_SHARE:.4f}")
        print(f"  R1 cap = {CAP_ANN/1e9:.3f}B/yr ({cap_mo/1e6:.1f}M/mo)")

        # --- MAXIFS-selected monthly IBFT & EBFT production, full history for T12M ---
        cur.execute("""WITH ranked AS (
            SELECT class, period, year, month, value_lbs,
                   row_number() OVER (PARTITION BY class,period ORDER BY vintage_rank DESC) rn
            FROM silver.tallow_balance WHERE series='tallow_production')
          SELECT class, period, year, month, value_lbs FROM ranked WHERE rn=1 ORDER BY period""")
        ibft={};
        for x in cur.fetchall():
            if x['class']=='IBFT': ibft[x['period']]=float(x['value_lbs'])
        periods=sorted(ibft)
        def t12m_ibft(p):
            idx=periods.index(p); w=[ibft[periods[j]] for j in range(max(0,idx-11),idx+1)]
            return sum(w)/len(w)*12  # annualized T12M

        # --- net imports monthly ---
        cur.execute("""SELECT period,
              coalesce(sum(value_lbs) FILTER (WHERE series='tallow_imports'),0)
             -coalesce(sum(value_lbs) FILTER (WHERE series='tallow_exports'),0) net
            FROM silver.tallow_balance WHERE series IN ('tallow_imports','tallow_exports') GROUP BY period""")
        net={x['period']:float(x['net']) for x in cur.fetchall()}

        rows=[]; cap_years=set()
        for p in periods:
            if p.year < START_YEAR: continue
            y,m=p.year,p.month
            t12=t12m_ibft(p)
            oleo_other = FITTED_SHARE * t12 / 12.0        # monthly, smoothed
            if oleo_other > cap_mo:
                oleo_other = cap_mo; cap_years.add(y)     # R1 clamp + flag
            feed = feed_glide_ann(y)/12.0
            ibft_prod = ibft[p]
            ni = net.get(p,0.0)
            non_bio_use = oleo_other + feed
            biofuel = ibft_prod + ni - oleo_other - feed   # R3/R4: EBFT excluded
            rows.append(('IBFT','non_bio_trend',p,y,m,oleo_other,'MODEL',30,METHOD_FLAG))
            rows.append(('IBFT','feed_use',p,y,m,feed,'MODEL',30,METHOD_FLAG))
            rows.append(('IBFT','non_bio_use',p,y,m,non_bio_use,'MODEL',30,METHOD_FLAG))
            rows.append(('ALL','tallow_biofuel_use',p,y,m,biofuel,'MODEL',30,METHOD_FLAG))
        execute_values(cur,"""INSERT INTO silver.tallow_balance
            (class,series,period,year,month,value_lbs,vintage,vintage_rank,meta_flag) VALUES %s
            ON CONFLICT (class,series,period,vintage) DO UPDATE SET value_lbs=EXCLUDED.value_lbs, meta_flag=EXCLUDED.meta_flag""",rows)
        c.commit()
        print(f"  inserted {len(rows)} {METHOD_FLAG} rows, {START_YEAR}+")
        cap23 = sorted(yy for yy in cap_years if yy>=2023)
        print(f"  R1_CAP_BINDING years: {sorted(cap_years)}")
        if cap23:
            print(f"  ** ACCEPTANCE CHECK #2 ESCALATION: cap fires 2023+ ({cap23}) -> see escalation doc **")
        cur.execute("""SELECT year, round(sum(value_lbs)/1e9,2) bn FROM silver.tallow_balance
            WHERE series='tallow_biofuel_use' GROUP BY 1 ORDER BY 1""")
        print("  tallow_biofuel_use (guardrail) by year, B lb:")
        for x in cur.fetchall():
            band = '' if not (2024==x['year']) else ('  [target 4.4-4.9]' )
            print(f"    {x['year']}: {float(x['bn']):.2f}{band}")

if __name__=='__main__':
    main()
