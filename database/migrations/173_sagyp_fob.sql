-- 173: Argentine official FOB prices (SAGyP) — bronze landing + curated silver map
--
-- Source: https://www.magyp.gob.ar/sitio/areas/ss_mercados_agropecuarios/ws/ssma/precios_fob.php?Fecha=dd/mm/yyyy
-- Published ART business days; JSON body behind a text/html Content-Type; history to 1993-01-04.
-- Rows repeat per posicion with different mesDesde-mesHasta ranges: FORWARD SHIPMENT BANDS —
-- the core value of the series. Bronze stores every row returned, bands preserved.
--
-- Silver mapping is by EXACT posicion code, not prefix: variants inside one HS prefix carry
-- DIFFERENT prices (bulk vs bagged, e.g. wheat 275 vs 295 on 2026-08-04; refined SBO bulk 1229
-- vs packaged 1339) and the digit that encodes bulk/bagged moves position between products
-- (wheat: 2nd digit of the pair; corn: 1st) — a prefix rule would nondeterministically collapse
-- variants into one series_key. Posicion codes are stable 1993->2026 (verified by live probe:
-- identical codes on 1993-01-04, 1995-01-05, 2020-08-03, 2026-08-04), so exact-code curation is
-- durable. Unmapped variants stay bronze-only; extend the map to add series.
--
-- price_mark tenor: the spec's tenor_type='shipment_band' would violate price_mark_tenor_type_ck
-- (SPOT/CONTRACT/WINDOW/NEARBY). WINDOW is this estate's vocabulary for a shipment window —
-- used with tenor 'YYYY-MM:YYYY-MM' built from the desde/hasta band. Band boundaries are data,
-- never hardcoded (deferred band extended through Jul 2027 on 2026-07-31).

BEGIN;

CREATE TABLE IF NOT EXISTS bronze.sagyp_fob_raw (
    fecha        date        NOT NULL,
    circular     text,                      -- circular number; changes on same-day revision
    posicion     text        NOT NULL,      -- Argentine tariff position, 12-char, stable since 1993
    precio       numeric,                   -- USD/t
    mes_desde    int         NOT NULL,
    anio_desde   int         NOT NULL,
    mes_hasta    int,
    anio_hasta   int,
    collected_at timestamptz DEFAULT now(),
    PRIMARY KEY (fecha, posicion, mes_desde, anio_desde)
);

COMMENT ON TABLE bronze.sagyp_fob_raw IS
    'SAGyP official FOB price circulars (precios_fob.php), all posiciones, shipment bands preserved. USD/t.';

CREATE TABLE IF NOT EXISTS reference.sagyp_position_map (
    posicion    text PRIMARY KEY,
    series_key  text NOT NULL,
    description text,
    is_active   boolean NOT NULL DEFAULT true
);

COMMENT ON TABLE reference.sagyp_position_map IS
    'Curated SAGyP posicion -> silver.price_mark series_key. Exact codes only (one canonical '
    'bulk/standard variant per series); unmapped posiciones remain bronze-only.';

INSERT INTO reference.sagyp_position_map (posicion, series_key, description) VALUES
    ('15121110310E', 'SAGYP_SUNOIL_CRUDE',   'Crude sunflower oil, bulk (a granel)'),
    ('15071000100Q', 'SAGYP_SBO_CRUDE',      'Crude soybean oil, bulk'),
    ('15079019100G', 'SAGYP_SBO_REFINED',    'Refined soybean oil, bulk'),
    ('15121919110H', 'SAGYP_SUNOIL_REFINED', 'Refined sunflower oil, bulk'),
    ('10019900110W', 'SAGYP_WHEAT',          'Bread wheat (trigo pan 1001.99), bulk — the AR export benchmark'),
    ('10011900110H', 'SAGYP_WHEAT_DURUM',    'Durum/candeal wheat (1001.19), bulk'),
    ('10059010190Y', 'SAGYP_CORN',           'Corn, bulk, standard (los demas)'),
    ('12019000190C', 'SAGYP_SOYBEANS',       'Soybeans, bulk'),
    ('23040010100B', 'SAGYP_SOYMEAL',        'Soybean meal, pellets'),
    ('12060090910Y', 'SAGYP_SUNSEED',        'Sunflower seed, bulk'),
    ('23063010100F', 'SAGYP_SUNMEAL',        'Sunflower meal, pellets'),
    ('11010010190D', 'SAGYP_WHEAT_FLOUR',    'Wheat flour (all four flour posiciones price identically)')
ON CONFLICT (posicion) DO UPDATE SET
    series_key = EXCLUDED.series_key,
    description = EXCLUDED.description;

-- Freshness registration: daily weekday-aware expected-by rule (migs 171/172).
-- SAGyP publishes during the ART business day; our pull runs 18:00 ET with a T-1 revision
-- re-pull, so expect data by 18:30 ET (= 19:30 ART). data_source carries no ART column —
-- expected_release_time_et is ET by contract; the source-local timezone is recorded below.
INSERT INTO data_source (code, name, description, base_url, api_type, update_frequency,
                         timezone, is_active, category, expected_frequency,
                         expected_release_time_et, collector_key)
SELECT 'SAGYP_FOB',
       'Argentina SAGyP Official FOB Prices',
       'Official FOB price circulars (grains, oilseeds, products) with forward shipment bands. '
       'Published ART business days; empty weekday response = Argentine holiday, not a failure.',
       'https://www.magyp.gob.ar/sitio/areas/ss_mercados_agropecuarios/ws/ssma/precios_fob.php',
       'json', 'daily', 'America/Argentina/Buenos_Aires', true, 'grains',
       'daily', '18:30', 'sagyp_fob_oficial'
WHERE NOT EXISTS (SELECT 1 FROM data_source WHERE collector_key = 'sagyp_fob_oficial');

COMMIT;
