# Biodiesel ↔ Renewable Diesel Trade Flow Split — Design

**Status:** Approved 2026-05-13. Implementation in progress.
**Date:** 2026-05-12 (drafted), 2026-05-13 (approved with answers below).

## v1 scope
- BD vs RD only. Co-processing carve-out is deferred to v2.
- Quarterly refresh cadence with EMTS D4 RIN-cancellation-for-export as the
  primary cross-check for tuning the rules over time.

## Problem statement

US Census trade data lumps biodiesel (BD) and renewable diesel (RD) together
under HS 3826 (biodiesel chapter). On the EXPORT side it's a single 10-digit
Schedule B code (3826000000). On the IMPORT side there are 3 HTSUS sub-codes
(3826.00.10/30/90) but all are "biodiesel mixtures" — RD comes in under these
or under HS 2710.20.x with negligible reported volumes.

Result: the BD vs RD split cannot be read directly from HS codes. We need a
heuristic allocation, analogous to DCO/corn-oil split.

## Approach: dual anchor

**Anchor 1 — Origin/destination country profile**
Origin country is the strongest signal because each foreign producer has a
known technology mix.

| Origin | BD% | RD% | Source |
|---|---:|---:|---|
| Singapore | 5% | 95% | Neste Singapore = HEFA RD plant, ~1.7 Bgy RD nameplate |
| Finland | 5% | 95% | Neste Porvoo + Naantali = RD |
| Netherlands | 25% | 75% | Neste Rotterdam RD (large) + small BD |
| Canada (BC) | 10% | 90% | Tidewater Prince George + Braya Come By Chance RD |
| Argentina | 95% | 5% | Renova/Vicentin/Cresta = soy-meth-ester BD |
| Indonesia | 95% | 5% | Wilmar/SinarMas palm BD |
| Malaysia | 90% | 10% | Mostly palm BD; some Petronas RD |
| Brazil | 95% | 5% | BE8/Petrobras = soy/tallow BD |
| South Korea | 95% | 5% | Dansuk/JC/Eco = UCO BD |
| Spain | 90% | 10% | Biocom + others = UCO BD |
| Germany | 70% | 30% | Mixed |
| France | 85% | 15% | Mostly rapeseed BD |
| Hong Kong | 100% | 0% | ASB UCO BD only |
| India | 100% | 0% | Universal Biofuels UCO BD |
| Australia | 100% | 0% | Just Biodiesel only |
| Default (other) | 90% | 10% | Conservative BD default |

**Anchor 2 — Time period weight**
The RD industry was tiny pre-2018 (DGD only). Geismar expansion + Neste
Rotterdam + Marathon ND came online 2020-2023. Singapore Neste 2010s. So:

| Period | RD industry capacity | Adjustment |
|---|---|---|
| 2013-2017 | ~250 Bgy global | All entries × 0.3 RD weight |
| 2018-2020 | ~700 Bgy | Use base table |
| 2021-2023 | ~1500 Bgy | Use base table |
| 2024+ | ~3000 Bgy | All entries × 1.2 RD weight (capped at 100%) |

**Anchor 3 — EMTS D4 production share (US side only, sanity check)**
`bronze.epa_emts_monthly` has US-domestic D4 RIN generation split by
fuel_category. The ratio (BD-RIN / (BD-RIN + RD-RIN)) gives the domestic
production share. For US EXPORTS we expect ~95% BD because US is a net RD
importer — domestic RD production stays domestic, BD surplus gets exported.
This is a cross-check, not the primary allocator.

## Schema

```sql
-- reference.biofuel_trade_split — manual heuristic mapping
CREATE TABLE reference.biofuel_trade_split (
    rule_id        SERIAL PRIMARY KEY,
    hs_code        TEXT NOT NULL,           -- '3826000000' etc.
    flow           TEXT NOT NULL,           -- 'imports' / 'exports'
    origin         TEXT,                    -- ISO2 or NULL (matches any)
    year_from      INT NOT NULL,
    year_to        INT NOT NULL,
    bd_share       NUMERIC NOT NULL CHECK (bd_share BETWEEN 0 AND 1),
    rd_share       NUMERIC NOT NULL CHECK (rd_share BETWEEN 0 AND 1),
    confidence     TEXT NOT NULL,           -- 'high' | 'medium' | 'low'
    source         TEXT,                    -- citation
    notes          TEXT,
    CHECK (bd_share + rd_share = 1.0)
);
```

```sql
-- gold view: emit two rows per source row (BD + RD components)
CREATE VIEW gold.biofuel_trade_split AS
WITH base AS (
    SELECT ct.*,
           split.bd_share, split.rd_share, split.confidence
    FROM bronze.census_trade ct
    LEFT JOIN reference.biofuel_trade_split split
      ON ct.hs_code = split.hs_code
     AND ct.flow = split.flow
     AND (split.origin IS NULL OR ct.country_code = split.origin)
     AND ct.year BETWEEN split.year_from AND split.year_to
    WHERE ct.hs_code LIKE '3826%'
)
SELECT year, month, flow, country_code, country_name,
       'BIODIESEL'::TEXT AS commodity_split,
       quantity * COALESCE(bd_share, 1.0) AS quantity_split,
       value_usd * COALESCE(bd_share, 1.0) AS value_usd_split,
       confidence
FROM base
UNION ALL
SELECT year, month, flow, country_code, country_name,
       'RENEWABLE_DIESEL'::TEXT,
       quantity * COALESCE(rd_share, 0.0),
       value_usd * COALESCE(rd_share, 0.0),
       confidence
FROM base
WHERE COALESCE(rd_share, 0.0) > 0;
```

## Validation plan

1. **National-level sanity check**: aggregate split exports/imports by year,
   compare to known industry total (e.g., EIA monthly RD imports). If our
   split says 2024 RD imports = 800 Bgy and EIA says 900 Bgy, the heuristic
   is roughly correct.
2. **Origin-country anomalies**: any origin where our split disagrees with
   the known operator at that origin gets surfaced for manual review.
3. **Confidence flagging**: trades with no matching rule get `bd_share=1.0,
   confidence='low'` (defaults to BD) so nothing is silently dropped.

## Decisions (resolved 2026-05-13)

1. **Country profile table** — adopt as-is for v1. Iterate based on what
   numbers reveal after validation.
2. **US RD export share** — start with spec default (5% RD on exports);
   Tore's prior is higher RD particularly to EU. EMTS RIN-cancellation-for-
   export cross-check (see validation) will adjust on first quarterly refresh.
   If EMTS shows export-cancelled D4 RINs implying e.g. 40% RD share, revise.
3. **Confidence levels** — high/med/low enum is fine. Don't over-engineer.
4. **Refresh cadence** — quarterly. Cross-check against EMTS RIN cancellations
   filed for export (these distinguish D4=BBD from D6=ethanol). Co-processing
   split deferred to v2 — BD vs RD is sufficient for current need.

## Effort

- Schema migration: ~15 min
- Initial rule population: ~30 min (with your guidance on table values)
- Gold view + integration: ~30 min
- Validation pass: ~1 hr
- Total: ~2-3 hours once table values are locked.
