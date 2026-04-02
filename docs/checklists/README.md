# RLC Operations Checklists

Inspired by NASA's Artemis mission checklists. Every data release, every model run,
every forecast update follows a defined procedure with verification steps.

## Checklist Types

### Daily Operations (DOC)
- `daily_checklist.md` — Morning routine + end-of-day verification

### Event-Driven (Report Days)
- `wasde_day.md` — Monthly WASDE release procedure
- `fats_oils_report.md` — NASS Fats & Oils monthly release
- `crop_progress.md` — Weekly crop progress (Apr-Nov)
- `export_sales.md` — Weekly FAS export sales
- `cftc_friday.md` — Weekly CFTC COT release
- `eia_wednesday.md` — Weekly EIA petroleum status
- `prospective_plantings.md` — Annual March 31 release
- `quarterly_stocks.md` — Quarterly grain stocks
- `nopa_monthly.md` — Monthly NOPA crush data

### Weekly Production
- `weekly_report.md` — Weekly report production checklist

### Monthly Review
- `monthly_review.md` — End-of-month forecast reconciliation

## Checklist Format

Each checklist follows this structure:
```
[ ] VERIFY — Confirm data/inputs are available
[ ] INGEST — System pulls and stores the data
[ ] COMPARE — Compare new data against prior estimates
[ ] UPDATE — Update spreadsheets and models
[ ] RECONCILE — Compare human vs system projections
[ ] PUBLISH — Generate client-facing output
```
