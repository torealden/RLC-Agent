"""
Oil-stocks composition helper (shared by the NASS processing collector and the
scripts/fix_oil_stocks_composition.py backfill tool).

NASS Fats & Oils reports oil stocks split by refinement stage. The collector's row-level
`_map_attribute` cannot produce the correct TOTAL, because:
  * the total is crude + once-refined -- two rows that must be summed, and the silver
    upsert key would collapse them (last writer wins -> once-refined alone, ~5-6x low);
  * the crude grand-total line differs by commodity (ONSITE&OFFSITE for most, bare CRUDE
    for corn, absent for palm), so which row IS the total is a per-commodity decision that
    a stateless row classifier gets wrong (soybean carries BOTH lines).

So oil STOCKS rows are skipped at classify time and this function owns all three
attributes -- `oil_stocks_crude`, `oil_stocks_once_refined`, and the `oil_stocks` total --
recomputed from bronze after ingest. It is idempotent.

  crude_total   = COALESCE("ONSITE & OFFSITE, CRUDE", bare "CRUDE")   # bare = corn only
  refined_total = "ONSITE & OFFSITE, [ONCE] REFINED"
  oil_stocks    = crude_total + refined_total
"""

SOURCES = ('NASS_FATS_OILS', 'NASS_SOY_CRUSH')

COMMODITY_TO_CLASS = {
    'soybeans': 'SOYBEAN', 'canola': 'CANOLA', 'cottonseed': 'COTTONSEED',
    'sunflower': 'SUNFLOWER', 'palm': 'PALM', 'palm_kernel': 'PALM KERNEL',
    'coconut': 'COCONUT', 'safflower': 'SAFFLOWER', 'peanut': 'PEANUT',
    'peanuts': 'PEANUT', 'corn': 'CORN',
}


def build_component_index(cur):
    """(class_desc, year, month) -> {'crude': v|None, 'refined': v|None} from bronze.
    Returns (index, problems). `problems` lists keys where a USED component is
    multi-valued (a data anomaly worth failing on)."""
    cur.execute("""
        SELECT class_desc, year, month, short_desc, value
        FROM bronze.nass_processing
        WHERE statisticcat_desc='STOCKS' AND commodity_desc='OIL' AND value IS NOT NULL
    """)
    raw = {}
    for r in cur.fetchall():
        k = (r['class_desc'], r['year'], r['month'])
        sd = r['short_desc'].upper()
        v = float(r['value'])
        d = raw.setdefault(k, {'oo_crude': set(), 'plain_crude': set(), 'oo_refined': set()})
        is_refined = 'REFINED' in sd
        if 'ONSITE & OFFSITE' in sd and 'CRUDE' in sd and not is_refined:
            d['oo_crude'].add(v)
        elif 'ONSITE & OFFSITE' in sd and is_refined:
            d['oo_refined'].add(v)
        elif 'CRUDE' in sd and 'ONSITE' not in sd and 'OFFSITE' not in sd:
            d['plain_crude'].add(v)

    idx, problems = {}, []

    def one(s, k, label):
        if not s:
            return None
        if len(s) > 1:
            problems.append(f"{k} {label} multi-valued: {sorted(s)}")
        return max(s)

    for k, d in raw.items():
        oo_crude = one(d['oo_crude'], k, 'oo_crude')
        oo_refined = one(d['oo_refined'], k, 'oo_refined')
        if oo_crude is not None:
            crude = oo_crude
        elif len(d['plain_crude']) == 1:
            crude = next(iter(d['plain_crude']))
        elif len(d['plain_crude']) > 1:
            problems.append(f"{k} plain_crude multi-valued and needed: {sorted(d['plain_crude'])}")
            crude = None
        else:
            crude = None
        idx[k] = {'crude': crude, 'refined': oo_refined}
    return idx, problems


def recompute_oil_stocks(conn, apply=True, verbose=False):
    """Recompute oil_stocks (+ crude/once_refined components) in silver.monthly_realized
    from bronze, in place, for source in SOURCES. Returns a stats dict.
    Raises ValueError if a used bronze component is multi-valued."""
    cur = conn.cursor()
    idx, problems = build_component_index(cur)
    if problems:
        raise ValueError("oil-stocks component validation failed:\n  " + "\n  ".join(problems))

    cur.execute("""
        SELECT commodity, country, marketing_year, month, calendar_year,
               realized_value, unit, source
        FROM silver.monthly_realized
        WHERE attribute='oil_stocks' AND source = ANY(%s)
    """, (list(SOURCES),))
    rows = cur.fetchall()

    stats = {'scanned': len(rows), 'updated': 0, 'same': 0, 'no_bronze': 0, 'components': 0}
    for r in rows:
        cls = COMMODITY_TO_CLASS.get(r['commodity'])
        if cls is None:
            continue
        comp = idx.get((cls, r['calendar_year'], r['month']))
        if comp is None:
            stats['no_bronze'] += 1
            continue
        total = (comp['crude'] or 0.0) + (comp['refined'] or 0.0)
        cur_val = float(r['realized_value']) if r['realized_value'] is not None else None
        if cur_val is None or abs(cur_val - total) > 0.5:
            stats['updated'] += 1
            if apply:
                cur.execute("""
                    UPDATE silver.monthly_realized SET realized_value=%s, collected_at=NOW()
                    WHERE commodity=%s AND country=%s AND marketing_year=%s AND month=%s
                      AND attribute='oil_stocks' AND source=%s
                """, (total, r['commodity'], r['country'], r['marketing_year'], r['month'], r['source']))
        else:
            stats['same'] += 1
        for attr, val in (('oil_stocks_crude', comp['crude']), ('oil_stocks_once_refined', comp['refined'])):
            if val is None:
                continue
            stats['components'] += 1
            if apply:
                cur.execute("""
                    INSERT INTO silver.monthly_realized
                      (commodity, country, marketing_year, month, calendar_year,
                       attribute, realized_value, unit, source, report_date, collected_at)
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,CURRENT_DATE,NOW())
                    ON CONFLICT (commodity, country, marketing_year, month, attribute, source)
                    DO UPDATE SET realized_value=EXCLUDED.realized_value, collected_at=NOW()
                """, (r['commodity'], r['country'], r['marketing_year'], r['month'],
                      r['calendar_year'], attr, val, r['unit'], r['source']))
    if apply:
        conn.commit()
    if verbose:
        print(f"recompute_oil_stocks: {stats}")
    return stats
