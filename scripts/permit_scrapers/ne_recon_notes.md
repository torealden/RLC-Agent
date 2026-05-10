# NE NDEE Permit Portal — Recon Notes

> **Status:** Recon-only, no working scraper yet.
> **Last touched:** 2026-05-10

## The portal

NDEE (Nebraska Department of Environment and Energy) publishes air
permits via the **OBPA** (OpenText Business Process Automation /
Process Suite) portal:

- Public entry: https://ecmp.nebraska.gov/PublicAccess/index.html
- Angular SPA (no Radware bot wall, but JS-rendered)
- API endpoints under `/PublicAccess/api/`

## API endpoints discovered

| Endpoint | Method | Purpose |
|---|---|---|
| `/api/CustomQuery` | GET | Returns list of available named queries |
| `/api/Keywords` | POST `{QueryID: N}` | Returns the keyword fields for a query |
| `/api/Search` (TBD) | POST | Executes a search; format not yet captured |
| `/obpa-config.json` | GET | App configuration |

## CustomQuery candidates for air permits

| ID | Name | Keywords |
|---|---|---|
| **759** | DEQ Compendium Public Query | **DEQ Facility Name** + Facility# + Program + Title/Subject + Reference + Decision Date + Compendium Industry + Compendium Category |
| 340 | DEQ IIS Facility Program Viewing | Facility# + Program ID + Program + Document ID |
| 335 | DEQ Program and ID viewing | Program + Program ID |
| 180 | DEQ GIS - DEQ IIS Facility Program | Facility# + Program |
| 197 | DEQ Public Notice Viewing | Facility# + Program |
| 425 | DEQ AWIN City Search | DEQ City |
| 496 | DEQ Air GP Public Access | DEQ Air GP Application # |

**Best bet:** QID=759 (Compendium Public Query) — has the richest
keyword set including Facility Name (case-insensitive substring).

## What's not yet captured

1. **Search execution endpoint** — likely `/api/Search` or `/api/QueryExecute`
   but format unknown. Need to Playwright-drive a real search and
   capture the POST body.

2. **Document download URL** — once we have search results, we need
   to figure out the per-document URL pattern. Likely `/api/Document?id=N`
   or similar.

3. **Program codes** — keyword 115 "DEQ Program" probably wants codes
   like AIR, AOP (Air Operating Permit), etc. Need to enumerate.

4. **Facility number** — keyword 114 "DEQ Facility Number" is NDEE's
   internal facility ID. AGP Hastings = ?, AGP David City = ?. Need
   to look these up via the GIS layer or by an initial broad search.

## Reasonable path forward (~1 day)

1. **Playwright session: drive a search by facility name** — open the
   portal, select QID=759, type "Ag Processing", submit. Capture the
   POST body shape and the response document list.
2. **Capture document download URL pattern** — click a result and
   watch the network tab.
3. **Build `scripts/permit_scrapers/ne.py`** mirroring `mn.py` (uses
   Playwright) but with NE-specific endpoints.
4. **Test against AGP Hastings + AGP David City** — capture latest
   Title V Class I permit PDFs.

## Alternative path (if NDEE makes this hard)

NDEE Air Quality Program contact: **dwee.airquality@nebraska.gov**
or **(402) 471-2186**. Information request route, similar to MN
MPCA fallback — days-to-weeks turnaround.

The known facility:
- AGP Inc. Hastings Soy South Plant, **2801 E. 7th St., Hastings,
  Adams County, NE** — there's a recent draft Title V Class I
  modification proposed for "replacement of existing desolventizer
  toaster decks" (per NDEE public notice).

## Why this is paused

The MO scraper (just-shipped) gives us the AGP St. Joseph permits
without further work. The recommended next step is to run the MO
scraper for other operators (ADM Kansas City, ADM Deerfield, ADM
Mexico, Cargill MO sites) before investing the full day on NE.
