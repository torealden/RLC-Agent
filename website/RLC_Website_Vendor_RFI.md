# RLC Website Build — Request for Information

**To:** Web build vendors under consideration
**From:** Felipe (Project Lead), Round Lakes Companies
**Date:** June 2026
**Response deadline:** Two weeks from receipt
**Format:** PDF or DOCX, max 8 pages, plus separately-priced exhibits

---

## 1. About the engagement

Round Lakes Companies (RLC) is a commodity market intelligence firm
serving biomass-based diesel, feedstock, oilseed, and broader
agricultural markets through Consulting, Trading, and Meats divisions
and publishes a paid weekly research product, *The Feedstock Report*.

We are scoping a new website to be delivered in **three sequential
stages** over ~24 months total. We will issue a competitive RFP after
this RFI; this RFI is the input to the RFP, not the contract.

Two documents accompany this RFI and are mandatory reading before you
respond:

- `RLC_Website_Build_Specification.docx` (v1.0) — the comprehensive
  single-launch specification covering brand, content, performance,
  accessibility, security, and integrations.
- `RLC_Website_Staged_Buildout_Spec.md` — the staged delivery
  specification (1.0 / 1.1 / 2.0). This document layers progressive
  functionality on top of the v1.0 spec.

If the two documents conflict, the **staged spec wins** for *scope and
sequencing*; the v1.0 DOCX wins for *standards and acceptance criteria*.

---

## 2. What we want from you in this RFI

A six-page recommendation, structured per Section 3 below, that lets us
decide:

1. **Stack:** which CMS, auth provider, and subscription wrapper you
   recommend given the 1.0 → 1.1 → 2.0 trajectory, and why.
2. **Sequence risk:** which transitions between stages carry the
   highest risk, and how your recommended stack mitigates them.
3. **Rough scope of effort:** order-of-magnitude estimate of hours and
   calendar weeks per stage. Not a fixed bid — a directional number.
4. **Team:** the people who would actually do the work, not the
   account team.
5. **Subprocessors:** every third-party service that would touch any
   RLC data, and whether each has a current SOC 2 Type II or ISO 27001
   certification (per the security baseline).

We will use your responses to write the RFP. The shortlist will be
two or three vendors. We expect the chosen vendor to win on technical
recommendation and team quality, not lowest bid.

---

## 3. Required structure of your response

Use these section headers. Brevity wins. Bullet points are fine. We
will reject responses that are marketing decks repackaged.

### 3.1 Stack recommendation (max 2 pages)
- Which CMS for the public site and why. Webflow, WordPress, headless
  (Sanity / Contentful / Payload / etc.) are all acceptable; we have
  not pre-selected.
- Which auth provider and why. Magic-link minimum in 1.1; tiered roles
  in 2.0; potential government / institutional client requirements in
  2.0+ that may push toward enterprise SSO.
- Which subscription wrapper and why (Memberful / Outseta /
  Stripe + custom / other).
- Which hosting + CDN + WAF combination.
- How your recommended stack handles the 1.1 → 2.0 transition without
  re-platforming the portal or migrating subscriber data twice.

### 3.2 Stage-by-stage scope and effort (max 1 page)
- 1.0: rough hours / calendar weeks / cost band ($X to $Y).
- 1.1: same.
- 2.0: same.
- Identify any portions you would *decline* to build (e.g., the
  IFV playground server-side service if you'd want RLC's data team
  to deliver it as a hosted API).

### 3.3 Highest-risk transitions and mitigations (max 1 page)
- The subscriber-list migration in 1.1 (Substack/Beehiiv → Stripe).
  Propose a parallel-run protocol. Minimum 2 billing cycles.
- The auth-tier expansion at 2.0 (basic → basic + premium).
- The data-API exposure for facility browsing at 2.0. How do you
  ensure model calibration parameters never leak client-side?

### 3.4 Team (max 1 page)
- Named people who would build this. Title, years on this kind of
  work, prior projects they shipped at this stage maturity.
- Account / project management overhead as a percentage of total
  hours.

### 3.5 Subprocessors and compliance (max 1 page)
- Full list of third-party services your recommended stack uses.
- For each: current SOC 2 Type II or ISO 27001 status, data residency
  (US / EU / UK only — no other regions accepted), and the data
  it would process for RLC.

### 3.6 Concerns and questions (max 1 page)
- What you'd want to clarify with us before committing to a fixed
  bid in the RFP phase.
- What constraints in the spec you think are over-specified
  (we'll consider relaxing them) or under-specified (we'll consider
  adding to them).
- What you'd recommend we cut from each stage to ship faster.

---

## 4. What we will NOT use this RFI to evaluate

- Visual design portfolios. Send links if you want — we won't grade
  them at this stage.
- Marketing performance promises. Out of scope.
- Discounts or "if we sign in 30 days" pricing. Not applicable.
- Vendor case studies unrelated to financial-services or
  paid-research-publishing builds.

---

## 5. Logistics

- Submit to felipe@roundlakescompanies.com (placeholder — confirm
  before sending).
- One round of clarifying questions in writing within the first week
  after issue. All questions and answers shared with all RFI
  participants.
- We will short-list 2–3 vendors for the RFP within four weeks of
  the RFI deadline.

---

## 6. About the project context (so you can scope sensibly)

- RLC's data infrastructure (PostgreSQL warehouse with bronze /
  silver / gold layers, ~150 tables) is built and operational. The
  website's server-side data layer will sit on top of this — you do
  not need to build a data warehouse.
- RLC publishes *The Feedstock Report* weekly; the report-rendering
  infrastructure to convert that data layer into a clean HTML issue
  is in progress and will be ready before 1.1 starts.
- The IFV (Implied Feedstock Value) calculation is a calibrated
  proprietary model. The 1.1 playground exposes 5–8 input sliders
  against it; you do not need to build the model. You need to build
  the page that calls it and renders the result.
- The facility model is similarly proprietary and lives in the data
  warehouse. The 2.0 facility browser is a read-only display of
  outputs the warehouse already produces.
- Branding is locked. Tore is the deciding voice on any design
  question. Felipe owns vendor management and day-to-day delivery.

---

## 7. Why we're doing this

Because the current state — no proper website — is costing RLC
inbound that we know is happening (event organizers, press, Fortune
500 procurement leads, sovereign analysts). We are willing to spend
to get this right. We are not willing to spend to get this elegant.
The goal is operational credibility and product delivery, not a
design award.
