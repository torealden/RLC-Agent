# RLC Website — Staged Buildout Specification

**Status:** Draft v0.1 (2026-06-02). Companion to
`RLC_Website_Build_Specification.docx` (v1.0).

**Purpose:** The existing DOCX is a comprehensive single-launch spec.
Real delivery happens in three stages because (a) the report-rendering
infrastructure isn't finished, (b) the IFV playground depends on the
calibrated allocator that's still being verified, and (c) the facility
model substrate is mid-build. This document slices the existing spec
into three deployable releases — **1.0**, **1.1**, **2.0** — each shipped
on its own, each capable of standing alone if subsequent stages slip.

**Authority:** Brand, baseline tech, security, performance, and
accessibility requirements from the existing DOCX apply to every stage
unchanged. This document specifies *what changes per stage*; assume the
DOCX standards are still in force where this document is silent.

---

## At-a-glance

| Stage | Purpose                                          | Auth     | Backend depth                         | Target ship |
|-------|--------------------------------------------------|----------|---------------------------------------|-------------|
| 1.0   | Credibility site + lead capture                  | None     | CMS-only, static-ish public content   | ~ 6 weeks   |
| 1.1   | Deliver The Feedstock Report via browser; IFV playground | Magic-link / SSO | Subscriber portal + read-only data layer | ~ 10–14 weeks after 1.0 |
| 2.0   | Modeling access — IFV per facility + facility model results | Tiered | Read-only API onto silver/gold data | ~ 12–16 weeks after 1.1 |

Each stage carries cumulative scope; **nothing built in an earlier stage
should need to be redone in a later one**. The vendor's architectural
recommendation must satisfy this.

---

## Stage 1.0 — Credibility site

### Purpose
Be findable, be vet-able, capture inbound. The site has to make RLC
look like a serious firm to a procurement lead at Bunge or an analyst
at USDA who Googles "Round Lakes Companies" after seeing Tore quoted in
S&P Global Platts.

### In scope
All pages from DOCX §5.1 — **except** the Subscriber Portal:

- Home
- Firm (About, Leadership, Approach)
- Consulting (Engagement models, Representative work, Inquiry form)
- The Feedstock Report (Product overview, Sample issue PDF download,
  Pricing & subscribe — checkout link out to whatever subscription
  tool we're using at the time, e.g., Substack/Beehiiv/Memberful)
- Insights (article archive, single-article template)
- Speaking (talks list, booking form)
- Contact (form + email addresses)
- Legal (Privacy, Terms, Disclosures, Cookie Policy)

### Out of scope (1.0)
- Subscriber portal (Stage 1.1)
- In-browser Feedstock Report rendering (Stage 1.1)
- IFV playground (Stage 1.1)
- Facility model browsing (Stage 2.0)
- Any authenticated experience

### Functional requirements
Same as DOCX §7 except:
- §7.2 Subscriptions & payments — **handled externally** in 1.0. The
  Feedstock Report subscribe button leads off-site to the existing
  Substack/Beehiiv checkout. The website does not handle billing yet.
- §7.4 Search — **public-content only** (Insights + Speaking + Newsroom).
- §7.6 Multi-language — English only (per DOCX).

### Acceptance gate
All DOCX §13.2 criteria except those that depend on the portal. Plus:
- A `subscribe` button on every page that goes through a UTM-tagged
  link to the external subscription tool, so all conversions are
  attributed back to the page they came from.
- A `talk-to-RLC` button on every page that fires a server-side
  conversion event to GA4.

### Cross-stage hooks (must not block 1.1/2.0)
- CMS choice must support adding an authenticated zone in 1.1 without
  re-platforming. *If Webflow, that means committing to Memberstack or
  similar in 1.1; if WordPress, MemberPress / Restrict Content Pro;
  if headless, the auth provider must be chosen in 1.0 even if not used.*
- Domain + DNS + email auth (SPF/DKIM/DMARC) configured for the
  primary domain AND a `subscribers.roundlakescompanies.com` (or
  similar) subdomain reserved for 1.1 portal.
- UTM convention defined and documented in 1.0; report pages in 1.1
  will inherit it.
- Brand tokens (color, type, spacing) checked into the CMS as
  reusable components so 1.1 portal styling can reuse them.

---

## Stage 1.1 — Subscriber reports + IFV playground

### Purpose
Move the Feedstock Report delivery from email-only to
**email + browser**. The browser version is the primary value-add for
subscribers: searchable archive, deep linking, instrumented usage. The
IFV playground is the first interactive deliverable — subscribers can
tweak a handful of inputs and see the calibrated implied feedstock
value update.

### In scope (delta vs 1.0)
- **Authenticated subscriber portal** at
  `subscribers.roundlakescompanies.com` (or `/subscribers/` path).
- **In-browser Feedstock Report rendering** — every issue published as
  a full HTML page (not a PDF download). PDF remains downloadable for
  offline / print, but the canonical version is the HTML page.
- **Past issues archive** — searchable, filterable by date and section.
- **IFV playground** — a single-page interactive tool, see spec below.
- **UTM tracking** of subscriber traffic — each issue carries its
  campaign / source / medium tags so we can see which channels (email,
  LinkedIn, partner referrals) actually drive engagement.
- **Subscription billing** moves on-platform — Stripe (Memberful or
  Outseta as the wrapper). The external Substack/Beehiiv subscriber
  list is migrated; current subscribers get continuity of access.
- **Email magic-link login** is the floor; Google SSO if it doesn't
  add disproportionate cost. No enterprise SSO yet (that's a 2.0
  decision when government clients show up).

### IFV playground spec
- Single page; 5–8 input controls; updates a result panel live.
- Inputs to expose in 1.1:
  - Feedstock type (dropdown: SBO / canola / DCO / tallow / CWG / YG / UCO)
  - Region (dropdown: a small set of canonical regions per feedstock)
  - Biofuel pathway (BD or RD)
  - D4 RIN ($/RIN) slider
  - LCFS credit ($/credit) slider — only shown if region is California
  - Diesel reference price ($/gal) — defaults to current ULSD Gulf
  - 45Z credit ($/gal-equivalent) — defaults to current model value
- Output panel shows:
  - Implied feedstock value ($/lb)
  - Decomposition: diesel value + RIN + LCFS + 45Z + BTC (if active)
  - "Currently producing" plug: what real-world feedstock cash price
    is right now in that region, and the IFV-vs-cash spread
- Default state is the current-market scenario — subscriber lands and
  sees a sensible number without touching anything.
- "Reset to defaults" button. "Share scenario" button copies a URL
  that reproduces the current input state (so analysts can DM each
  other a specific configuration).
- **The model itself runs server-side**, not in JavaScript. The page
  is a thin client. This is non-negotiable because (a) the calibrated
  allocator code must not be exposed, and (b) we want to log every
  scenario for product analytics.

### Out of scope (1.1)
- Per-facility IFV (Stage 2.0)
- Facility model browsing (Stage 2.0)
- Custom price overrides beyond the exposed sliders (Stage 2.0 if at all)
- Subscriber-uploaded data (never; out of scope permanently)

### Functional requirements added in 1.1
- DOCX §7.1 CMS — extended to support **report templates** alongside
  Insights articles. A report template has fixed section structure
  (executive read / price dashboard / credit stack / production tracker
  / IFV / S&D / policy / trade / market signals / news / calls register /
  week ahead) and the editor fills sections, not free-form blocks.
- DOCX §7.2 Subscriptions & payments — on-platform via Stripe wrapper.
- DOCX §7.3 Forms — extend lead-capture to gated-content gating
  (subscriber-only).
- DOCX §7.4 Search — extends to authenticated archive search.

### Acceptance gate
- The current week's Feedstock Report is delivered to subscribers by
  email (a deep link to the HTML version) AND visible in the portal.
- Past 4 issues migrated to the portal as HTML.
- IFV playground produces numerically-correct results matching the
  calibrated model (sanity-checked against `kg_callable` output for
  the same inputs).
- Every page in the portal carries the UTM and analytics hooks.
- Cancellation, refund, seat management work end-to-end.
- Existing subscriber migration: zero double-charges, zero lost
  access (verified by reconciliation against the prior platform's
  subscriber list).

### Cross-stage hooks (must not block 2.0)
- Auth tier structure must already support "premium" or
  "model-access" tier even if no feature uses it yet.
- Data layer (the server-side service that backs the IFV
  playground) must be designed as **a read-only API onto the silver
  and gold layers** of the RLC data warehouse — *not* a direct
  database connection from the website. This is what 2.0 will scale.

---

## Stage 2.0 — Modeling access

### Purpose
Differentiate via tooling that buyers can't get from a research email.
The facility model — the per-facility production / feedstock-mix /
margin / IFV layer being built across the
`project_iowa_multi_industry_expansion` and feedstock-allocator work
— becomes a subscriber-visible product. Read-only. They see what
the model says, they don't get to run their own scenarios on it
(that's a paid consulting engagement).

### In scope (delta vs 1.1)
- **Facility browser** — search a facility (operator, location,
  EPA / EIA / CARB ID), see modeled outputs: estimated production,
  feedstock mix, margins, IFV, recent news/permit/CARB pathway
  changes that the system has detected.
- **Insights drill-down** — when a major price moves (e.g., D4 RIN +5¢)
  the model output identifies the top N facilities most affected;
  these "insights" surface in the subscriber portal as auto-generated
  briefings.
- **Tiered access** — basic subscriber sees the existing 1.1
  experience; premium subscriber sees the facility browser + insights.
- **Audit logging** of premium-tier model access. Government clients
  will demand this; bake it in from launch.
- **Optional: API access** for the highest enterprise tier. Read-only
  endpoint returning IFV per facility per scenario. Rate-limited.

### Out of scope (2.0)
- User-uploaded data (still never).
- User-defined scenarios beyond the sliders. The facility model runs
  the canonical scenarios; analysts can request bespoke scenarios as
  a consulting engagement.
- Real-time data (model results land on a daily cadence at most;
  facility data updates run weekly).

### Functional requirements added in 2.0
- **Read-only data API** onto silver / gold layers. Pre-aggregated.
  Cached aggressively. Built on the same data pipeline as the IFV
  playground but exposes per-facility rows.
- **Search** — entity-level search across the facility taxonomy.
  Algolia or comparable; must support faceting (commodity, region,
  operator, technology).
- **Audit + compliance** — every facility lookup logged with
  subscriber ID + timestamp + facility ID. Retained 24 months.
  Available for export on government-client subpoena.

### Acceptance gate
- A premium subscriber can search for "Chevron REG Geismar" and see
  modeled production, feedstock mix (with confidence bounds), recent
  CARB pathway changes, and IFV vs cash spread.
- A premium subscriber can see at least one insight per week generated
  from the facility model.
- Audit log review: pull last 30 days of premium-tier accesses,
  confirm structure matches government RFP requirements.
- Performance: facility page LCP < 2.5s on 4G (per DOCX §8.3).

### Cross-stage hooks (forward)
- Multi-language support hinted in DOCX §7.6 may activate here for
  LATAM expansion.
- Eventual model-update visualization (showing how a forecast
  changed week-over-week) is a 2.x release, not a 2.0 requirement.

---

## Cross-stage decisions Tore needs to lock before Stage 1.0 RFP

The vendor needs answers to these to scope correctly:

1. **CMS commitment.** The DOCX names Webflow OR WordPress (with
   headless as a vendor-justified upgrade). Lock one *before*
   1.0 — switching at 1.1 means re-platforming the portal too.
2. **Subscription wrapper.** Memberful, Outseta, or Stripe + custom
   build. Same logic — choose before 1.0 even though it isn't *used*
   until 1.1, so the brand/styling/single-sign-on flow is consistent.
3. **Subscriber-list migration timing.** When does the Substack /
   Beehiiv (or whatever current channel) handoff happen? It should
   land WITHIN 1.1, not at the boundary of 1.0 launch.
4. **IFV playground default scenario.** What does a subscriber see
   when they first land on `/ifv-playground` with no inputs touched?
   Recommendation: current-market scenario (live cash prices,
   current RIN, current LCFS, BD pathway) — but Tore should approve
   the specific defaults.
5. **2.0 tier pricing strategy.** Does premium-tier facility access
   cost the same as basic Feedstock Report, more, or much more? This
   affects checkout UI in 1.1.
6. **Government / institutional channel.** If sovereigns / multilaterals
   are part of the 2.0 client mix, audit log retention and data-residency
   commitments may need to be promoted from "forward-looking" to
   "mandatory" earlier than 2.0. Worth confirming before the vendor
   scopes 1.1 infra.

---

## Risks the vendor needs to scope against

- **Auth model lock-in.** A wrong call on magic-link vs SSO vs full
  enterprise auth in 1.1 forces re-architecture at 2.0. The vendor
  should propose an auth provider that supports tiered roles from day 1,
  even if 1.1 only uses two tiers (free / paid).
- **Data API leakage.** The IFV playground and the facility browser
  must NEVER expose the underlying calibration parameters or the
  proprietary allocator weights. Server-side rendering + careful
  payload design. Per `feedback_fastmarkets_keep_dont_show` we also
  must never expose any fastmarkets-sourced rows.
- **Brand drift.** If 1.0, 1.1, and 2.0 are built by different vendors
  (or different teams at the same vendor), the Figma design system
  delivered with 1.0 must be the single source of truth.
- **Performance regression at 2.0.** Facility-level pages with maps,
  charts, and entity search will be heavier than the 1.0 content
  pages. Performance budget (DOCX §8.3) needs to be re-validated at
  the 2.0 launch.
- **Subscriber migration disaster.** Moving billing from Substack/
  Beehiiv to Stripe in 1.1 is the highest-risk single event in the
  whole roadmap. Vendor must propose a parallel-run / dual-tracking
  period (minimum 2 billing cycles) to catch silent failures.

---

## What I'd hand the vendor

Three documents:

1. **`RLC_Website_Build_Specification.docx`** (v1.0) — the existing
   master spec. Brand, performance, security, integrations, etc.
2. **This document** — staged scope and stage-gate acceptance criteria.
3. **A short RFI** — ask the vendor to recommend the CMS / auth /
   subscription wrapper combination given the 1.0 → 1.1 → 2.0 trajectory.
   Two pages max. Use their answer to refine the master spec before
   committing.

---

## Open questions for Tore

- Stage 1.0 timeline: is 6 weeks aggressive enough to be useful, or
  should we treat 1.0 as a 3-week "launch the placeholder, get a
  domain, get GA4 instrumented" sprint and absorb the heavier pages
  (Approach, Consulting, Insights) into 1.1?
- Stage 1.1 IFV playground: do you want the sliders to control real
  inputs to the calibrated allocator (which would make the page heavy
  but conceptually clean), or pre-computed scenarios on a coarse grid
  (which is faster but loses some expressiveness)?
- Stage 2.0 facility detail: do you want facility-level results
  organized around the facility (one page per facility) or around the
  question (one page per scenario / market move, showing affected
  facilities)? Both are valuable; one ships first.
