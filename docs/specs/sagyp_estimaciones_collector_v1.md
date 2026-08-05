# sagyp_estimaciones collector — queued spec (v1, 2026-08-05)

**Status: QUEUED, not built.** Feeds the scenario engine and the WAPR accuracy paper, not a
weekly report. Build in its own session.

## What it is
Department-level Estimaciones Agrícolas — the dataset behind the Ministry's Tableau boards.
Columns (verified by live probe 2026-08-05): `cultivo, anio, campania, provincia, provincia_id,
departamento, departamento_id, superficie_sembrada_ha, superficie_cosechada_ha, produccion_tm,
rendimiento_kgxha`. 1969/70 → present, ~162k rows, full-refresh file that updates with campaign
progress.

## Canonical endpoint (verified live 2026-08-05)
- CKAN dataset: `estimaciones-agricolas` on datos.gob.ar / datos.magyp.gob.ar
  (package id `9e1e77ba-267e-4eaa-a59f-3296e86b5f36`, main resource
  `95d066e6-8a0f-4a80-b59d-6f28f88eacd5`).
- Current download URL ends `estimaciones-agricolas-2026-03.csv` — **the filename is versioned**,
  so resolve the resource URL at runtime via CKAN `package_show`, never hardcode the filename.
- Tableau/visor front-ends (`datosestimaciones.magyp.gob.ar`) are display layers; the CKAN CSV
  is canonical.

## Parsing notes
- The 2026-03 vintage is **comma-delimited, UTF-8, fully quoted** — the earlier Desktop note
  (latin-1, semicolon-delimited, inconsistent quote-wrapping with doubled inner quotes)
  describes older vintages. Sniff delimiter + encoding per download instead of assuming either;
  normalize doubled inner quotes before parsing if the semicolon variant returns.
- Numeric fields arrive unquoted; blanks occur (unplanted/no-harvest cells).

## Landing plan
- `bronze.sagyp_estimaciones` — full refresh each pull (~162k rows), keep
  `(cultivo, campania, provincia_id, departamento_id)` natural key + all source columns +
  `collected_at`. Full-refresh = truncate-and-load inside one transaction, or upsert + delete
  missing keys; decide in build session (revisions do occur mid-campaign).
- Silver: filter to girasol / soja (1ra, 2da, total) / maíz / trigo / cebada / sorgo.
- Weekly refresh (updates with campaign progress); register with the weekly freshness rule.
- Collector: `src/agents/collectors/south_america/` alongside `sagyp_fob_collector.py`
  (same contract: CollectorResult, no self-logging).
