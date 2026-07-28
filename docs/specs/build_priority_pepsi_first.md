# Build Priority — Pepsi / Helios First

**Purpose:** the order to work the country × complex builds, Pepsi (Helios pilot) ahead of everything
else. Pairs with the progress trackers (`models/_Progress_*.xlsx`) — each row names the group workbook
and tab it lives on. "State" = where it sits in the 10-step process today (2026-07-28).

**The governing logic:** Pepsi's deliverable is the veg-oil **substitution / price** answer across palm,
soy oil, rapeseed, sunflower, and corn oil. That needs the price-setting **exporters** first (they set
the reference series we quote), then the swing **importers** (the demand/substitution side), then the
world rollups and scenario stubs. Fundamentals before prices; prices are the final pass on each.

Legend — State: **TEMPLATE** built · **annual✓** annual-closed & tied out, monthly pending ·
**—** not started.

---

## P0 — the reference templates (already built)

| # | Complex | Country | Group · tab | State |
|---|---|---|---|---|
| 1 | Soybean | United States | Oilseeds · US Soybean | **TEMPLATE** (the structural reference) |
| 2 | Corn Oil | United States | Oilseeds · US Corn Oil | **TEMPLATE** (DCO / feedstock layer) |

## P1 — Pepsi Tier-A exporters (price-setting; monthly build) — THE priority

These 9 are annual-closed and tied out; the work is the monthly block per the oilseed spec. This is the
supply reference for the substitution answer.

| # | Complex | Country | Group · tab | State |
|---|---|---|---|---|
| 3 | Palm | Malaysia | Oilseeds · Malaysia Palm | annual✓ |
| 4 | Palm | Indonesia | Oilseeds · Indonesia Palm | annual✓ |
| 5 | Soybean | Brazil | Oilseeds · Brazil Soybean | annual✓ (monthly block partly staged) |
| 6 | Soybean | Argentina | Oilseeds · Argentina Soybean | annual✓ |
| 7 | Sunflower | Ukraine | Oilseeds · Ukraine Sun | annual✓ |
| 8 | Sunflower | Russia | Oilseeds · Russia Sun | annual✓ |
| 9 | Sunflower | Argentina | Oilseeds · Argentina Sun | annual✓ |
| 10 | Rapeseed / Canola | Europe | Oilseeds · EU Canola | annual✓ |
| 11 | Rapeseed / Canola | Canada | Oilseeds · Canada Canola | annual✓ |
| 12 | Rapeseed / Canola | Australia | Oilseeds · Australia Canola | annual✓ |
| 13 | Rapeseed / Canola | Russia | Oilseeds · Russia Canola | annual✓ |
| 14 | Corn Oil | Brazil | Oilseeds · Brazil Corn Oil | — (no PSD; needs derived source) |

## P2 — Pepsi Tier-B importers (demand / substitution side)

Shared importer workbook per country (tab per oil + an allocation tab splitting veg-oil demand across
palm/sun/rape/soy on relative price — this is the substitution scenario promised to Helios).

| # | Complex(es) | Country | Group · tab | State |
|---|---|---|---|---|
| 15 | Palm/Soy/Rape/Sun | China | Oilseeds · China {Palm,Soybean,Canola,Sun} | — |
| 16 | Palm/Soy/Rape/Sun | India | Oilseeds · India {Palm,Soybean,Canola,Sun} | — |
| 17 | Palm/Soy/Rape/Sun | Europe | Oilseeds · EU {Palm,Soybean,Sun} | — (EU rape is Tier-A above) |
| 18 | Rape/Sun | Turkey | Oilseeds · Turkey {Canola,Sun} | — |

## P3 — World rollups (automated from `bronze.fas_psd`)
All five complexes — no manual build; wire the auto-rollup. (Not carried as tabs; tracked via the C-tier
of the coverage matrix.)

## P4 — Pepsi Tier-D scenario stubs (production + trade + shock coefficient, no full sheet)
Palm: Colombia, Guatemala, Mexico · Rapeseed: Brazil, Mexico · Sunflower: Colombia, Mexico ·
Soybean: Mexico · Corn Oil: Mexico.

---

## P5+ — after Pepsi (the other groups; scaffolded, prune first)

These trackers exist (`_Progress_FeedGrains/FoodGrains/Energy.xlsx`) with a starter tab set to confirm.
Rough order once Pepsi is underway:

1. **Feed Grains — Corn** (US, Brazil, Argentina, Ukraine): feeds the ethanol/DDGS and corn-oil lines
   that already touch the oilseed/biofuel work — highest cross-leverage.
2. **Energy** (US ethanol, biodiesel, RD, SAF): the BBD demand pull under the veg-oil complexes.
3. **Food Grains — Wheat, then Rice**: standalone; lowest coupling to the Pepsi deliverable.

Sequencing within each is TBD when scope firms — the starter tabs are a proposal, not a commitment.

---

*Re-rank as cells close. When a P1 cell's monthly block verifies (oilseed spec guards 1–5), it graduates
to `done`/green in the coverage matrix and drops off the critical path.*
