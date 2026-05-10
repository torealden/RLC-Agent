# Company Reports — Convention

Centralized per-operator document store. One folder per operator; the
folder name is the **NYSE/NASDAQ ticker symbol in uppercase** for
public companies, or the **uppercase short code** for private/
cooperative entities (AGP, CARGILL, etc.).

## Per-company subfolder layout

```
domain_knowledge/company_reports/
├── README.md                    (this file)
├── public_company_tickers.xlsx  (master ticker list)
├── company_tickers_cache.json   (SEC EDGAR cache)
├── ADM/                          NYSE: ADM
│   ├── books/                    Annotated bibliographies, histories
│   │   ├── soyinfo_adm_andreas_1884_2020.pdf
│   │   ├── soyinfo_adm_entries.json
│   │   └── soyinfo_adm_full_text.txt
│   ├── extracts/                 Our curated markdown / structured extracts
│   │   └── adm_deep_history.md
│   ├── public_reports/           Annual reports, sustainability, SEC filings
│   │   └── sec_filings/          SEC EDGAR — 10-K, 8-K, DEF 14A, 10-Q
│   │       └── (one folder per filing accession#)
│   ├── climate/                  Climate / sustainability / ESG documents
│   ├── news/                     Press releases, media articles
│   ├── permits/                  Air, water, hazwaste permits (per facility)
│   ├── manifest.csv              Inventory of pulled SEC filings
│   └── extractions_manifest.csv  Inventory of LLM extraction.json outputs
├── AGP/                          Private cooperative
│   └── (same structure)
├── CARGILL/                      Private — no ticker
│   └── (same structure)
├── BG/                           NYSE: BG (Bunge)
├── TSN/                          NYSE: TSN (Tyson)
├── INGR/                         NYSE: INGR (Ingredion)
├── HRL/                          NYSE: HRL (Hormel)
├── (etc — one per ticker we track)
```

## Folder naming rules

- **Public companies:** folder = NYSE/NASDAQ ticker, UPPERCASE.
  Example: `ADM`, `BG`, `TSN`. Don't use the long company name.
- **Private companies / cooperatives:** folder = uppercase short
  code that's recognizable to a domain expert. Example: `AGP`
  (not "Ag Processing Inc"), `CARGILL`, `KOCH`, `LANDOLAKES`.
- **Subsidiaries:** if a subsidiary has its own document trail and
  is large enough, give it its own folder. Otherwise file under the
  parent. Example: Continental Grain's grain merchandising went to
  Cargill in 1999 — those docs live under `CARGILL/`.

## Subfolder semantics

| Subfolder | Contents |
|---|---|
| `books/` | Long-form annotated bibliographies (SoyInfo Center volumes), academic histories, primary-source company histories. Both PDF and extracted text. |
| `sec_filings/` | SEC EDGAR pulls — 10-K, 10-Q, 8-K, DEF 14A. One subfolder per accession number. (Existing legacy structure; new pulls follow same.) |
| `extracts/` | OUR curated markdown / structured extracts derived from the books and filings. The deep-history docs live here. |
| `public_reports/` | Annual reports (the company's own glossy publication, not 10-K), sustainability reports, investor decks, fact books. |
| `climate/` | Climate change policy, sustainability/ESG reports, GHG inventories, climate-disclosure documents (CDP, TCFD). |
| `news/` | Press releases, news articles, media coverage. Date in filename. |
| `permits/` | Air, water, hazwaste permits per facility. Mirrored from `data/raw/state_air_permits/` for fast company-level access. |

## When to add a new company folder

When we start tracking an operator that's NOT in this directory:
1. Determine the ticker (or short code if private).
2. Create the standard subfolder structure (run the snippet below).
3. Add the company to `public_company_tickers.xlsx` if public.
4. Drop documents into the right subfolder.

```bash
cd domain_knowledge/company_reports
TICKER=NEWCO
for sub in books extracts climate public_reports news permits sec_filings; do
  mkdir -p "$TICKER/$sub"
done
```

## Where do incoming docs go?

Quick lookup if someone hands us a document:

| Document type | Location |
|---|---|
| 10-K / 10-Q / 8-K / DEF 14A | `<TICKER>/public_reports/sec_filings/<accession#>/` |
| Annual glossy report | `<TICKER>/public_reports/` |
| Sustainability / ESG / TCFD / GHG | `<TICKER>/climate/` |
| Climate Change policy doc | `<TICKER>/climate/` |
| Industry analyst report | `<TICKER>/public_reports/` (or `domain_knowledge/sample_reports/` if cross-company) |
| SoyInfo Center book | `<TICKER>/books/` |
| Historical academic monograph | `<TICKER>/books/` |
| Title V air permit | `<TICKER>/permits/` AND `data/raw/state_air_permits/<state>/<facility_id>/` |
| Press release / news article | `<TICKER>/news/` |
| Our curated markdown summary | `<TICKER>/extracts/` |
| SEC filings inventory CSV | `<TICKER>/manifest.csv` (kept at ticker root for fast scan) |

## Currently populated

As of 2026-05-10:

| Ticker | Status | Notes |
|---|---|---|
| ADM | Books + SEC + extract | SoyInfo 1884-2020 mined (1572 entries); deep history extracted |
| AGP | Books + extract | SoyInfo 1923-2021 mined (652 entries); deep history extracted |
| CARGILL | Book downloaded | SoyInfo 1940-2020 PDF downloaded; mining queued |
| AGCO, BG, CALM, CF, CTVA, DAR, GPRE, HRL, INGR, IPI, JBS, MOS, NTR, TSN | SEC filings only | From scripts/sec_edgar_puller.py — extraction pipeline next |
