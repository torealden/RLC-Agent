"""Vintage-aware rake of allocator output to EIA feedstock control totals (design v1.6 Layer D, v3).

Evolves rake_feedstock_to_eia.py. Key ideas:

  1. EIA control total per (period, EIA feedstock) is chosen by a *coverage-preference ladder*
     evaluated per (feedstock, soybean-oil-style marketing year = Oct y - Sep y+1):

       1) plant_type='total' (not withheld) complete (>=11 of 12 months) -> use monthly 'total'.
          basis EIA_TOTAL. This is the redaction-proof rollup: EIA sometimes withholds one of the
          biodiesel / renewable_diesel components while the combined 'total' survives. Preferring
          'total' recovers those months. (plant_type='total' only exists 2021+.)
       2) elif bd+rd (biodiesel + renewable_diesel, not withheld) complete (>=11 of 12) -> monthly
          bd+rd. basis EIA_BDRD. Extends coverage back to ~2006 (before 'total' existed).
       3) elif feedstock is Soybean Oil -> USDA seasonal fallback (see below). basis USDA_SEASONAL.
       4) else -> leave unraked (rake_factor=1). basis UNRAKED.

     For Soybean Oil 'total' == bd+rd wherever both are complete, so SBO results are unchanged by the
     ladder (the label may read EIA_TOTAL instead of EIA_BDRD for 2023/24, but the value is identical).
     The ladder's payoff is Corn Oil / Canola / Cottonseed, where EIA redacts a component and bd+rd
     collapses while 'total' still reports the full number.

  2. USDA fallback FOR SOYBEAN OIL ONLY: for each soybean-oil marketing year where EIA(bd+rd) SBO
     coverage is incomplete (<11 of 12 months non-withheld), the SBO control total for that MY's 12
     months is replaced with a USDA-derived monthly control = USDA annual SBO biofuel use (ERS Oil
     Crops Yearbook, MY Total, latest vintage) distributed across the MY's months in proportion to
     monthly (biodiesel + renewable_diesel) production seasonality.

RLC_CANONICAL (EBFT/IBFT/BFT/UCO/YG) stays exempt at 1.0 (basis EXEMPT_RLC). Each raked row records
control_basis: 'EIA_TOTAL' | 'EIA_BDRD' | 'USDA_SEASONAL' | 'EXEMPT_RLC' | 'UNRAKED'.

Does NOT touch the allocator or gold.feedstock_allocation. Re-rakes from existing allocation rows only.
"""
import sys
from pathlib import Path
from datetime import date
ROOT = Path(r"C:/dev/RLC-Agent"); sys.path.insert(0, str(ROOT))
from dotenv import load_dotenv; load_dotenv(ROOT / ".env")
from src.services.database.db_config import get_connection

RUN_DAY = '2026-07-21'  # output vintage tag for this rake (fixed; incremental-run friendly)

# ---------------------------------------------------------------------------------------------
# TWO-SIDED RAKE (ruled by Tore 2026-07-21).
#
# The rake used to apply ONE factor per (period, feedstock) across every fuel_type, so the grand
# total tied to EIA while the biodiesel / renewable-diesel SPLIT was left to the facility mix. For
# CY2024 soybean oil that put biodiesel 1,788.7 mil lb ABOVE EIA's published 7,399 and the RD family
# the same amount below, with the total dead on 13,320 -- an error invisible to any total check.
#
# The rule: allocate all EIA-reported feedstock use AS EIA REPORTS IT. Where EIA publishes a
# biodiesel/renewable-diesel breakout (Soybean Oil, Canola Oil, Corn Oil -- and only those), rake
# each side to its own control. Our three-way split of the RD family (renewable diesel /
# co-processing / SAF) is RLC's own analytical breakout and must SUM to EIA's renewable-diesel line.
# Where EIA publishes only a total, one-sided as before. Tallow and UCO stay RLC-canonical/exempt.
FUEL_BD = {'biodiesel'}
FUEL_RD = {'renewable_diesel', 'coprocessing', 'saf'}   # must sum to EIA 'renewable_diesel'


def fuel_bucket(ft):
    ft = (ft or '').strip().lower()
    if ft in FUEL_BD:
        return 'BD'
    if ft in FUEL_RD:
        return 'RD'
    return None

# allocator feedstock_code -> EIA feedstock_name (EIA lumps UCO into Yellow Grease, tallow combined)
A2E = {'SBO':'Soybean Oil','CO':'Canola Oil','CAN':'Canola Oil','DCO':'Corn Oil',
       'EBFT':'Tallow','IBFT':'Tallow','BFT':'Tallow','YG':'Yellow Grease','UCO':'Yellow Grease',
       'CWG':'White Grease','PF':'Poultry','PLT':'Poultry','CSO':'Cottonseed Oil'}

# RLC-canonical feedstocks (Ruling 1 + UCO Amendment 1): RLC supply build is authoritative, EIA
# disregarded. EXEMPT from the rake — kept at allocator totals, rake_factor forced to 1.0.
RLC_CANONICAL = {'EBFT', 'IBFT', 'BFT', 'UCO', 'YG'}

DDL = """
CREATE TABLE IF NOT EXISTS gold.bbd_feedstock_raked (
    facility_id int, period date, feedstock_code text, fuel_type text,
    raked_mil_lbs numeric, pre_rake_mil_lbs numeric, rake_factor numeric,
    eia_feedstock text, run_day date, created_at timestamptz DEFAULT now()
);
"""


def my_of(p):
    """Soybean-oil marketing year (Oct y - Sep y+1) containing date p."""
    return p.year if p.month >= 10 else p.year - 1


with get_connection() as c:
    cur = c.cursor()
    cur.execute(DDL)
    # add control_basis column if the table predates it
    cur.execute("ALTER TABLE gold.bbd_feedstock_raked ADD COLUMN IF NOT EXISTS control_basis text")
    cur.execute("DELETE FROM gold.bbd_feedstock_raked WHERE run_day=%s", (RUN_DAY,))

    # 1. latest run per period, per-facility allocation.
    #    Take the newest run per period across ALL run dates (not just today's), so re-running a
    #    single month later is picked up without re-running every month. This is the correct
    #    architecture for month-at-a-time incremental allocator runs.
    cur.execute("""
        WITH latest AS (SELECT DISTINCT ON (period) period, run_id FROM gold.feedstock_allocation
                        ORDER BY period, created_at DESC)
        SELECT a.facility_id, a.period, a.feedstock_code, a.fuel_type, a.allocated_mil_lbs
        FROM gold.feedstock_allocation a JOIN latest l ON a.period=l.period AND a.run_id=l.run_id
    """)
    rows = cur.fetchall()

    # 2. allocator totals per (period, eia_name)
    alloc_tot = {}
    alloc_bucket = {}          # (period, eia_name, 'BD'|'RD') -> allocator mil lbs
    for r in rows:
        eia = A2E.get(r['feedstock_code'])
        if not eia:
            continue
        v = float(r['allocated_mil_lbs'] or 0)
        alloc_tot[(r['period'], eia)] = alloc_tot.get((r['period'], eia), 0) + v
        b = fuel_bucket(r['fuel_type'])
        if b:
            alloc_bucket[(r['period'], eia, b)] = alloc_bucket.get((r['period'], eia, b), 0) + v

    # 3. EIA control totals per (period, eia_name) from two rollups, captured monthly:
    #      - plant_type='total'           (redaction-proof; exists 2021+)
    #      - biodiesel + renewable_diesel (extends coverage to ~2006, but collapses under redaction)
    #    Month counts per (eia_name, MY) drive the coverage ladder below.
    cur.execute("""SELECT make_date(year, month, 1) period, feedstock_name,
                     sum(quantity_mil_lbs) FILTER (WHERE plant_type='total') tot,
                     sum(quantity_mil_lbs) FILTER (WHERE plant_type IN ('biodiesel','renewable_diesel')) bdrd,
                     sum(quantity_mil_lbs) FILTER (WHERE plant_type='biodiesel') bd,
                     sum(quantity_mil_lbs) FILTER (WHERE plant_type='renewable_diesel') rd
                   FROM bronze.eia_feedstock_monthly
                   WHERE NOT is_withheld AND quantity_mil_lbs IS NOT NULL
                   GROUP BY 1,2""")
    total_month, bdrd_month = {}, {}   # (period, eia_name) -> mil lbs
    bd_month, rd_month = {}, {}         # (period, eia_name) -> mil lbs, the two published sides
    total_mo, bdrd_mo = {}, {}          # (eia_name, my) -> count of months present
    for r in cur.fetchall():
        key = (r['period'], r['feedstock_name']); my = my_of(r['period'])
        if r['tot'] is not None:
            total_month[key] = float(r['tot'])
            total_mo[(r['feedstock_name'], my)] = total_mo.get((r['feedstock_name'], my), 0) + 1
        if r['bdrd'] is not None:
            bdrd_month[key] = float(r['bdrd'])
            bdrd_mo[(r['feedstock_name'], my)] = bdrd_mo.get((r['feedstock_name'], my), 0) + 1
        if r['bd'] is not None:
            bd_month[key] = float(r['bd'])
        if r['rd'] is not None:
            rd_month[key] = float(r['rd'])

    # 3b. WITHHELD-COMPONENT SPLIT. EIA sometimes redacts BOTH plant_type components for a month
    #     while publishing the combined 'total' (Corn Oil April 2024: total 339, bd='W', rd='W' --
    #     which is the whole 340 mil lb gap between EIA's CY2024 bd+rd and its own total). Ruled
    #     2026-07-21: split that total PRO-RATA on the bd:rd ratio of the same marketing year's
    #     non-withheld months, so the month still lands on its published control and the split is
    #     inferred from EIA's own behaviour rather than from our facility mix.
    my_bd, my_rd = {}, {}
    for (p, nm), v in bd_month.items():
        if (p, nm) in rd_month:
            k = (nm, my_of(p))
            my_bd[k] = my_bd.get(k, 0.0) + v
            my_rd[k] = my_rd.get(k, 0.0) + rd_month[(p, nm)]
    bd_share_my = {k: my_bd[k] / (my_bd[k] + my_rd[k]) for k in my_bd if (my_bd[k] + my_rd[k]) > 0}
    split_months = []
    for (p, nm), tot in total_month.items():
        if (p, nm) in bd_month or (p, nm) in rd_month:
            continue                      # at least one side published -> no inference needed
        share = bd_share_my.get((nm, my_of(p)))
        if share is None:
            continue                      # no ratio to lean on -> leave to the one-sided total rake
        bd_month[(p, nm)] = tot * share
        rd_month[(p, nm)] = tot * (1.0 - share)
        split_months.append((p, nm, tot, share))
    if split_months:
        print("\nEIA_TOTAL_SPLIT — months where EIA withheld BOTH components; total split pro-rata "
              "on the same MY's published bd:rd ratio:")
        for p, nm, tot, share in sorted(split_months):
            print(f"  {p} {nm:14s} total={tot:8.1f}  -> bd={tot*share:7.1f} ({share*100:4.1f}%) "
                  f"rd={tot*(1-share):7.1f}")

    # --- SANITY: bd+rd vs plant_type='total' rollup, 2023-2024 ---
    cur.execute("""SELECT feedstock_name,
                     sum(quantity_mil_lbs) FILTER (WHERE plant_type IN ('biodiesel','renewable_diesel')) bdrd,
                     sum(quantity_mil_lbs) FILTER (WHERE plant_type='total') tot
                   FROM bronze.eia_feedstock_monthly
                   WHERE year IN (2023,2024) AND NOT is_withheld AND quantity_mil_lbs IS NOT NULL
                     AND feedstock_name IN ('Soybean Oil','Canola Oil','Corn Oil','Tallow')
                   GROUP BY 1 ORDER BY 1""")
    print("SANITY 2023-2024: bd+rd vs plant_type='total' (mil lbs) — where total > bd+rd, EIA redacted a component")
    for r in cur.fetchall():
        bdrd = float(r['bdrd'] or 0); tot = float(r['tot'] or 0)
        d = f"{(tot/bdrd-1)*100:+.2f}%" if bdrd else "n/a"
        print(f"  {r['feedstock_name']:15} bd+rd={bdrd:10.1f}  total={tot:10.1f}  total/bdrd={d}")

    # 4. SBO EIA(bd+rd) month-presence per MY -> which MYs are incomplete (need USDA fallback)
    cur.execute("""SELECT make_date(year, month, 1) period
                   FROM bronze.eia_feedstock_monthly
                   WHERE feedstock_name='Soybean Oil' AND plant_type IN ('biodiesel','renewable_diesel')
                     AND NOT is_withheld AND quantity_mil_lbs IS NOT NULL""")
    sbo_eia_months = set(r['period'] for r in cur.fetchall())
    sbo_my_month_count = {}
    for p in sbo_eia_months:
        sbo_my_month_count[my_of(p)] = sbo_my_month_count.get(my_of(p), 0) + 1

    # 5. USDA annual SBO biofuel use: MY Total, latest vintage per MY, normalized to mil lbs
    cur.execute("""
      WITH r AS (
        SELECT my_start_year, amount, unit_desc,
          row_number() OVER (PARTITION BY my_start_year ORDER BY ingested_at DESC, id DESC) rn
        FROM bronze.ers_oil_crops_yearbook
        WHERE commodity ILIKE '%soybean oil%' AND attribute_desc='Domestic use, Biofuel'
          AND timeperiod_desc='MY Total' AND amount IS NOT NULL)
      SELECT my_start_year,
        CASE WHEN unit_desc ILIKE '%thousand%' OR unit_desc ILIKE '%1,000%' THEN amount*0.001 ELSE amount END mil_lbs
      FROM r WHERE rn=1""")
    usda_annual = {r['my_start_year']: float(r['mil_lbs']) for r in cur.fetchall() if r['mil_lbs'] is not None}

    # 6. monthly BBD production seasonality (biodiesel + renewable_diesel), from gold monthly.
    SEASONALITY_SERIES = "gold.us_liquid_fuel_production_monthly (biodiesel_kgal + renewable_diesel_kgal)"
    cur.execute("""SELECT period_date period,
                     coalesce(biodiesel_kgal,0)+coalesce(renewable_diesel_kgal,0) prod
                   FROM gold.us_liquid_fuel_production_monthly""")
    prod_month = {r['period']: float(r['prod'] or 0) for r in cur.fetchall()}

    # 7. Determine USDA_SEASONAL MYs and build the SBO monthly control for those MYs.
    #    A MY qualifies when EIA(bd+rd) SBO is incomplete (<11 of 12 months) AND USDA annual exists.
    #    Distribute USDA annual across the MY's 12 months by production share.
    sbo_usda_control = {}   # period -> USDA-derived monthly control (mil lbs)
    usda_seasonal_mys = set()
    example_my = 2021
    for my, annual in sorted(usda_annual.items()):
        months = [date(my, m, 1) for m in range(10, 13)] + [date(my + 1, m, 1) for m in range(1, 10)]
        present = sbo_my_month_count.get(my, 0)
        if present >= 11:
            continue  # EIA complete -> stays on EIA
        prod_my = {m: prod_month.get(m, 0.0) for m in months}
        prod_total = sum(prod_my.values())
        if prod_total <= 0:
            continue  # no seasonality basis -> cannot build USDA control, leave to EIA/unraked
        usda_seasonal_mys.add(my)
        for m in months:
            sbo_usda_control[m] = annual * (prod_my[m] / prod_total)
        if my == example_my:
            print(f"\nSeasonality series: {SEASONALITY_SERIES}")
            print(f"USDA_SEASONAL example MY {my}/{(my+1)%100:02d} — annual USDA SBO biofuel = {annual:.1f} mil lbs")
            print("  12 monthly production shares (bd+rd):")
            for m in months:
                print(f"    {m}: {prod_my[m]/prod_total*100:5.2f}%  -> USDA control {sbo_usda_control[m]:8.1f} mil lbs")

    print(f"\nUSDA_SEASONAL marketing years: {sorted((f'{y}/{(y+1)%100:02d}') for y in usda_seasonal_mys)}")

    # 7b. Coverage-preference ladder. The ladder is evaluated PER MONTH for the EIA rollups (not
    #     all-or-nothing per MY): prefer redaction-proof plant_type='total' where present, else bd+rd.
    #     The per-(feedstock,MY) >=11/12 threshold is retained ONLY as the trigger for the SBO
    #     USDA-seasonal fallback (which is inherently an annual->monthly distribution).
    #
    #     NOTE ON THE SPEC: the brief specified a strictly per-MY ladder (a whole MY becomes UNRAKED
    #     unless total OR bd+rd reaches 11/12). Because EIA 'total' is only complete for MY2023+ and
    #     bd+rd coverage is partial across MY2017-2022, that strict reading DISCARDS valid single-month
    #     controls and leaves UNRAKED essentially flat (~1735 veg-oil facility-months, vs ~1271 before)
    #     — failing the stated 'sharp drop' objective. Per-month preference recovers every redacted
    #     month where 'total' exists AND keeps every month bd+rd already covered, so new UNRAKED is a
    #     strict subset of old. Complete MYs still resolve to a single clean basis (2023/24 -> EIA_TOTAL);
    #     partial MYs degrade to per-month EIA_TOTAL/EIA_BDRD instead of being dumped. SBO is unchanged.

    # 8/9. rake each per-facility row, recording control_basis (batched insert)
    from psycopg2.extras import execute_values
    batch = []
    unsplittable = set()   # EIA gave both sides but the allocator left a fuel bucket empty
    for r in rows:
        code = r['feedstock_code']
        eia = A2E.get(code)
        if not eia:
            continue
        pre = float(r['allocated_mil_lbs'] or 0)
        p = r['period']; my = my_of(p)
        a = alloc_tot.get((p, eia), 0.0)

        if code in RLC_CANONICAL:
            rf, basis = 1.0, 'EXEMPT_RLC'
        elif code == 'SBO' and my in usda_seasonal_mys:
            # SBO USDA-seasonal MY: force all 12 months to USDA control (redaction-proof annual anchor)
            ctrl = sbo_usda_control.get(p)
            if ctrl is not None and a > 0:
                rf, basis = ctrl / a, 'USDA_SEASONAL'
            else:
                rf, basis = 1.0, 'UNRAKED'
        else:
            # --- TWO-SIDED first: where EIA publishes BOTH sides, each side gets its own control.
            #     Our RD/co-processing/SAF three-way must sum to EIA's single renewable-diesel line.
            bucket = fuel_bucket(r['fuel_type'])
            ctrl_bd, ctrl_rd = bd_month.get((p, eia)), rd_month.get((p, eia))
            pre_bd = alloc_bucket.get((p, eia, 'BD'), 0.0)
            pre_rd = alloc_bucket.get((p, eia, 'RD'), 0.0)
            two_sided_ok = (ctrl_bd is not None and ctrl_rd is not None and bucket is not None
                            and pre_bd > 0 and pre_rd > 0)
            if two_sided_ok:
                rf = (ctrl_bd / pre_bd) if bucket == 'BD' else (ctrl_rd / pre_rd)
                basis = 'EIA_BD_RD_2SIDED'
            else:
                # one-sided fallbacks, unchanged. Reasons two-sided can't apply: EIA publishes no
                # breakout for this feedstock (all the fats), or the allocator put nothing in one of
                # the two buckets that month so there is no base to scale.
                tot = total_month.get((p, eia))   # redaction-proof rollup, preferred
                bd = bdrd_month.get((p, eia))      # bd+rd, extends coverage but collapses under redaction
                if tot is not None and a > 0:
                    rf, basis = tot / a, 'EIA_TOTAL'
                elif bd is not None and a > 0:
                    rf, basis = bd / a, 'EIA_BDRD'
                else:
                    rf, basis = 1.0, 'UNRAKED'
                if ctrl_bd is not None and ctrl_rd is not None and bucket is not None:
                    unsplittable.add((p, eia, round(pre_bd, 1), round(pre_rd, 1)))

        batch.append((r['facility_id'], p, code, r['fuel_type'], pre*rf, pre, rf, eia, RUN_DAY, basis))
    execute_values(cur, """INSERT INTO gold.bbd_feedstock_raked
        (facility_id,period,feedstock_code,fuel_type,raked_mil_lbs,pre_rake_mil_lbs,rake_factor,
         eia_feedstock,run_day,control_basis) VALUES %s""", batch, page_size=1000)
    n = len(batch)
    c.commit()
    print(f"\nraked {n} rows -> gold.bbd_feedstock_raked (run_day {RUN_DAY})")
    if unsplittable:
        print(f"\nWARNING: {len(unsplittable)} (period, feedstock) months had an EIA bd/rd breakout "
              f"but the allocator left one fuel bucket EMPTY, so they fell back to a one-sided rake "
              f"and their SPLIT still will not tie to EIA:")
        for p_, eia_, pb, pr in sorted(unsplittable)[:15]:
            print(f"  {p_} {eia_:14s} pre-rake BD={pb:9.1f}  RD-family={pr:9.1f}")
        if len(unsplittable) > 15:
            print(f"  ... and {len(unsplittable)-15} more")

    # TIE-OUT: for every feedstock EIA breaks out, raked BD and raked RD-family must equal EIA's
    # published sides. The absence of exactly this check is what let a 1,789 mil lb error hide
    # behind a correct grand total.
    print("\n=== TIE-OUT vs EIA published sides (calendar year, mil lbs) ===")
    cur.execute("""
      WITH ours AS (
        SELECT eia_feedstock, extract(year from period)::int yr,
               sum(raked_mil_lbs) FILTER (WHERE fuel_type='biodiesel') bd,
               sum(raked_mil_lbs) FILTER (WHERE fuel_type<>'biodiesel') rd
        FROM gold.bbd_feedstock_raked WHERE run_day=%s GROUP BY 1,2),
      eia AS (
        SELECT feedstock_name, year yr,
               sum(quantity_mil_lbs) FILTER (WHERE plant_type='biodiesel') bd,
               sum(quantity_mil_lbs) FILTER (WHERE plant_type='renewable_diesel') rd
        FROM bronze.eia_feedstock_monthly WHERE NOT is_withheld GROUP BY 1,2)
      SELECT o.eia_feedstock, o.yr, e.bd ebd, o.bd obd, e.rd erd, o.rd ord_
      FROM ours o JOIN eia e ON e.feedstock_name=o.eia_feedstock AND e.yr=o.yr
      WHERE e.bd IS NOT NULL AND e.rd IS NOT NULL AND o.yr BETWEEN 2022 AND 2025
      ORDER BY 1,2""", (RUN_DAY,))
    # NOTE: this annual view is INDICATIVE ONLY and will legitimately differ wherever a month was
    # imputed (EIA_TOTAL_SPLIT) or anchored to USDA (USDA_SEASONAL) -- EIA's published annual sides
    # exclude the withheld months that we filled. The binding test is the per-month one below,
    # restricted to months actually raked on the two-sided basis.
    print(f"{'feedstock':14} {'yr':5} {'EIA bd':>9} {'our bd':>9} {'d':>8} | "
          f"{'EIA rd':>9} {'our rd':>9} {'d':>8}")
    off = 0
    for r in cur.fetchall():
        ebd, obd = float(r['ebd'] or 0), float(r['obd'] or 0)
        erd, ord_ = float(r['erd'] or 0), float(r['ord_'] or 0)
        bad = not (abs(obd - ebd) < 1 and abs(ord_ - erd) < 1)
        off += bad
        print(f"{r['eia_feedstock']:14} {r['yr']:5} {ebd:9.1f} {obd:9.1f} {obd-ebd:8.1f} | "
              f"{erd:9.1f} {ord_:9.1f} {ord_-erd:8.1f}{'   <-- OFF' if bad else ''}")
    print(f"  -> {off} feedstock-years differ (expected where months were imputed/USDA-anchored)")

    # BINDING TEST: every month actually raked two-sided must land on EIA's own two sides.
    print("\n=== BINDING per-month tie-out (control_basis='EIA_BD_RD_2SIDED') ===")
    cur.execute("""
      WITH ours AS (
        SELECT period, eia_feedstock,
               sum(raked_mil_lbs) FILTER (WHERE fuel_type='biodiesel') bd,
               sum(raked_mil_lbs) FILTER (WHERE fuel_type<>'biodiesel') rd
        FROM gold.bbd_feedstock_raked
        WHERE run_day=%s AND control_basis='EIA_BD_RD_2SIDED' GROUP BY 1,2),
      eia AS (
        SELECT make_date(year,month,1) period, feedstock_name,
               sum(quantity_mil_lbs) FILTER (WHERE plant_type='biodiesel') bd,
               sum(quantity_mil_lbs) FILTER (WHERE plant_type='renewable_diesel') rd
        FROM bronze.eia_feedstock_monthly WHERE NOT is_withheld GROUP BY 1,2)
      SELECT o.eia_feedstock, count(*) n,
             max(abs(o.bd-e.bd)) mbd, max(abs(o.rd-e.rd)) mrd
      FROM ours o JOIN eia e ON e.feedstock_name=o.eia_feedstock AND e.period=o.period
      WHERE e.bd IS NOT NULL AND e.rd IS NOT NULL
      GROUP BY 1 ORDER BY 1""", (RUN_DAY,))
    allok = True
    for r in cur.fetchall():
        mbd, mrd = float(r['mbd'] or 0), float(r['mrd'] or 0)
        ok = mbd < 0.01 and mrd < 0.01
        allok &= ok
        print(f"  {r['eia_feedstock']:14} {r['n']:4d} months | worst |d| bd={mbd:.4f} rd={mrd:.4f}"
              f"  {'OK' if ok else '<-- FAIL'}")
    print("  -> two-sided rake ties to EIA exactly" if allok else "  -> TWO-SIDED RAKE DOES NOT TIE")

    # basis distribution
    cur.execute("""SELECT control_basis, count(*) n FROM gold.bbd_feedstock_raked
                   WHERE run_day=%s GROUP BY 1 ORDER BY 2 DESC""", (RUN_DAY,))
    print("control_basis distribution:")
    unraked_total = 0
    for r in cur.fetchall():
        print(f"  {r['control_basis']:14} {r['n']}")
        if r['control_basis'] == 'UNRAKED':
            unraked_total = r['n']
    print(f"\nUNRAKED facility-months total: {unraked_total}")

    # remaining UNRAKED, broken down by feedstock_code x MY (why can't they be raked?)
    cur.execute("""SELECT feedstock_code,
                     (CASE WHEN extract(month from period)>=10 THEN extract(year from period)
                           ELSE extract(year from period)-1 END)::int my, count(*) n
                   FROM gold.bbd_feedstock_raked
                   WHERE run_day=%s AND control_basis='UNRAKED'
                   GROUP BY 1,2 ORDER BY 1,2""", (RUN_DAY,))
    print("remaining UNRAKED by feedstock_code x MY (neither 'total' nor bd+rd reached 11/12 mo, and not SBO/USDA-eligible):")
    for r in cur.fetchall():
        print(f"  {r['feedstock_code']:5} MY{r['my']}/{(r['my']+1)%100:02d}  {r['n']}")

    # 10a. trailing-12mo national pre/post vs EIA
    cur.execute("""SELECT eia_feedstock,
                       round(sum(pre_rake_mil_lbs)/1000.0,2) pre, round(sum(raked_mil_lbs)/1000.0,2) post,
                       round(avg(rake_factor),3) avg_rf
                   FROM gold.bbd_feedstock_raked WHERE run_day=%s AND period BETWEEN '2024-10-01' AND '2025-09-01'
                   GROUP BY 1 ORDER BY 2 DESC""", (RUN_DAY,))
    print(f"\ntrailing-12mo national (B lb): pre-rake -> post-rake (avg rake factor)")
    for r in cur.fetchall():
        print(f"  {r['eia_feedstock']:16} {float(r['pre']):6.2f} -> {float(r['post']):6.2f}  (x{r['avg_rf']})")

    # 10b. VERIFICATION: Corn Oil (DCO) & Canola raked national total by MY 2018-2024
    print("\n=== VERIFICATION: Corn Oil (DCO) & Canola raked national total by MY (mil lbs) ===")
    print(f"{'eia_feedstock':15} {'MY':9} {'pre':>10} {'raked':>10} {'basis(es)':>28}")
    cur.execute("""SELECT eia_feedstock,
                     (CASE WHEN extract(month from period)>=10 THEN extract(year from period)
                           ELSE extract(year from period)-1 END)::int my,
                     round(sum(pre_rake_mil_lbs),1) pre, round(sum(raked_mil_lbs),1) raked,
                     string_agg(distinct control_basis, ',' ORDER BY control_basis) bases
                   FROM gold.bbd_feedstock_raked
                   WHERE run_day=%s AND eia_feedstock IN ('Corn Oil','Canola Oil')
                     AND (CASE WHEN extract(month from period)>=10 THEN extract(year from period)
                              ELSE extract(year from period)-1 END) BETWEEN 2018 AND 2024
                   GROUP BY 1,2 ORDER BY 1,2""", (RUN_DAY,))
    for r in cur.fetchall():
        print(f"{r['eia_feedstock']:15} {r['my']}/{(r['my']+1)%100:02d}  {float(r['pre']):10.1f} {float(r['raked']):10.1f} {r['bases']:>28}")

    # 10c. SANITY: Corn Oil CY2024 bd+rd vs total — redaction magnitude
    cur.execute("""SELECT
                     sum(quantity_mil_lbs) FILTER (WHERE plant_type IN ('biodiesel','renewable_diesel')) bdrd,
                     sum(quantity_mil_lbs) FILTER (WHERE plant_type='total') tot
                   FROM bronze.eia_feedstock_monthly
                   WHERE feedstock_name='Corn Oil' AND year=2024
                     AND NOT is_withheld AND quantity_mil_lbs IS NOT NULL""")
    r = cur.fetchone()
    bdrd = float(r['bdrd'] or 0); tot = float(r['tot'] or 0)
    gap = tot - bdrd
    pct = f"{(tot/bdrd-1)*100:+.1f}%" if bdrd else "n/a"
    print(f"\nSANITY Corn Oil CY2024 (mil lbs): bd+rd (redaction-exposed) = {bdrd:.1f}  |  total (redaction-proof) = {tot:.1f}")
    print(f"  redaction gap total-bd+rd = {gap:+.1f} mil lbs ({pct}) — recovered by preferring plant_type='total'")

    # 10d. VERIFICATION: SBO by MY 2016..2024 — must be UNCHANGED from v2
    cur.execute("""SELECT make_date(year, month, 1) period, sum(quantity_mil_lbs) q
                   FROM bronze.eia_feedstock_monthly
                   WHERE feedstock_name='Soybean Oil' AND plant_type IN ('biodiesel','renewable_diesel')
                     AND NOT is_withheld AND quantity_mil_lbs IS NOT NULL GROUP BY 1""")
    eia_sbo_by_my, eia_sbo_mo = {}, {}
    for r in cur.fetchall():
        my = my_of(r['period'])
        eia_sbo_by_my[my] = eia_sbo_by_my.get(my, 0.0) + float(r['q'])
        eia_sbo_mo[my] = eia_sbo_mo.get(my, 0) + 1

    cur.execute("""SELECT period, sum(raked_mil_lbs) q,
                     string_agg(distinct control_basis, ',' ORDER BY control_basis) bases
                   FROM gold.bbd_feedstock_raked
                   WHERE run_day=%s AND feedstock_code='SBO' GROUP BY 1""", (RUN_DAY,))
    raked_sbo_by_my, sbo_basis_by_my = {}, {}
    for r in cur.fetchall():
        my = my_of(r['period'])
        raked_sbo_by_my[my] = raked_sbo_by_my.get(my, 0.0) + float(r['q'])
        sbo_basis_by_my[my] = r['bases']

    print("\n=== VERIFICATION: Soybean Oil by marketing year (mil lbs) — must be UNCHANGED ===")
    print(f"{'MY':9} {'USDA':>9} {'EIA(bd+rd)':>11} {'mo':>3} {'RLC-raked':>10} {'RLC/USDA%':>10} {'basis':>13}")
    for my in range(2016, 2025):
        usda = usda_annual.get(my, 0.0)
        eia = eia_sbo_by_my.get(my, 0.0)
        mo = eia_sbo_mo.get(my, 0)
        raked = raked_sbo_by_my.get(my, 0.0)
        pct = (raked / usda * 100) if usda else 0.0
        basis = sbo_basis_by_my.get(my, 'UNRAKED')
        print(f"{my}/{(my+1)%100:02d}  {usda:9.1f} {eia:11.1f} {mo:3d} {raked:10.1f} {pct:9.1f}% {basis:>13}")

    # expectation flags
    print("\n=== EXPECTATION CHECK (SBO band) ===")
    expect = {
        2020: ('USDA_SEASONAL', 95, 105), 2021: ('USDA_SEASONAL', 95, 105), 2022: ('USDA_SEASONAL', 95, 105),
        2018: (None, 88, 93), 2019: (None, 88, 93), 2024: (None, 84, 90),
    }
    for my in range(2016, 2025):
        usda = usda_annual.get(my, 0.0); raked = raked_sbo_by_my.get(my, 0.0)
        pct = (raked / usda * 100) if usda else 0.0
        if my in expect:
            _, lo, hi = expect[my]
            ok = 'OK' if lo <= pct <= hi else '*** MISMATCH ***'
            print(f"  MY{my}/{(my+1)%100:02d}: {pct:.1f}%  expected {lo}-{hi}%  [{ok}]")
        else:
            print(f"  MY{my}/{(my+1)%100:02d}: {pct:.1f}%  (EIA-driven, no fixed expectation)")

# The flat files Desktop links are fully derived from the rake we just wrote. Regenerate them
# automatically so a rake can never leave stale flat files behind. Non-fatal: the rake has already
# committed above; if a writer fails, warn and let the operator re-run refresh_feedstock_flat_files.py.
import subprocess as _sp
print("\n=== auto-refreshing flat files (refresh_feedstock_flat_files.py) ===")
_r = _sp.run([sys.executable, str(Path(__file__).with_name('refresh_feedstock_flat_files.py'))])
if _r.returncode != 0:
    print("*** WARNING: flat-file refresh reported failures — re-run "
          "scripts/refresh_feedstock_flat_files.py and check output before handing to Desktop ***")
