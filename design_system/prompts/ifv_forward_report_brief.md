# Claude Design brief — RLC Implied Feedstock Value (IFV) Forward Report

Paste into Claude Design. **Attach the icon SVGs** from `design_system/icons/svg/` (soybean, canola,
oil-droplet, refinery, barrel, renewable, drumstick, steak, tanker-truck, …). Single-color line icons
on `currentColor`.

> **This is a first outline — expect to iterate. Build it, we react, we refine.**

---

**Build a report template for Round Lakes Commodities: the Implied Feedstock Value (IFV) Forward Report.**

The IFV is a **transparent, fundamentals-derived value for biofuel feedstocks** — built up from the
market's own fuel and policy prices. This report presents current and forward IFV values for the
relevant feedstocks across the relevant regions, plus a weekly analysis section.

## Positioning — read carefully, it drives the whole design
- The report is a **feedstock-value reference**. It states IFV values with the calm authority of a
  reference series — the reader should feel they're looking at *the* number, derived in the open.
- Establish validity through **methodology and transparency** — show the build-up, cite the inputs.
  The math being visible IS the credibility argument.
- **Never mention price reporting agencies, PRAs, benchmarks-to-replace, or any desire to become one.**
  Do not compare, do not aspire. Simply present IFV as a sound, self-evident reference. The absence of
  that framing is deliberate.

## Brand — "Lake, Field & Grain"
Palette (rule: *Lake asserts, Field signals, Wheat lights*): Lake `#1B2A4A` anchor · Field `#3C7D22`
accent/positive · Wheat `#C8A951` light/hairline · Paper `#F7F3EB` ground · Sage `#B7CCA4` bands/fills
· Clay `#96492A` negative (not red) · Slate `#8A8F98` captions/axes.

Typography: **display/headlines = Georgia** (`font-family: Georgia, "Times New Roman", serif`);
**body + tables = Calibri** with `font-variant-numeric: tabular-nums`. Theme-aware (light on Paper,
dark grounded on Lake).

**Horizon band** (signature divider): thin Wheat hairline on top, 2px gap, 4px Field band below.

## Structure

### Masthead
"Implied Feedstock Value — Forward Report", edition/date, a horizon band. Immediately below, a
**summary strip** of headline IFV numbers — the relevant feedstock × region cells, each showing the
current IFV (¢/lb) and a small forward arrow (Field up / Clay down). This strip says "here are the
values" before any prose.

### The stack — the centerpiece, one per region × commodity
For each region/commodity, present the IFV as a **waterfall that builds the value, starting from the
predominant fuel in that market** and stepping through the policy stack down to the feedstock:

```
  Predominant fuel value (region base)        e.g. Renewable diesel off ULSD
    + D4 RIN                                   biofuel credit
    + LCFS credit                              (CA / OR / WA; CI-dependent)
    + 45Z production credit                    (CI-dependent)
    − conversion / operating cost
    = value available to feedstock, per gallon
    ÷ yield  (~7.5 lb feedstock / gal)
    = IMPLIED FEEDSTOCK VALUE  (¢/lb)
```

Render this as a **labeled waterfall chart** (bars stepping up in Field for additions, down in Clay
for the cost, landing on a bold Wheat-highlighted IFV bar), beside the resulting **IFV figure** and a
small **forward curve** (history in Lake, forward in Field, Sage confidence band). Header each block
with the commodity icon + name + region.

Template these markets (design 2–3, they repeat):
- **US Gulf — renewable diesel**, feedstock soybean oil
- **California — renewable diesel** (LCFS in the stack), feedstock used cooking oil (low CI)
- **US — SAF**, feedstock tallow

### Weekly analysis
A section of short analytical callouts on the week's data and what it did to IFV — e.g. a recent
**NOPA crush** print (vs expectations, crush-margin read), the latest **EMTS / RIN generation** data,
credit-market moves (D4 RIN, LCFS). Each callout: a Field/Clay directional chip, a one-line claim, a
sentence of reasoning, and its IFV implication.

### Footer
Methodology reference line (Slate), edition, horizon band.

## Constraints
Self-contained HTML, inline CSS, theme-aware. Real, plausible feedstock/biofuel copy — no lorem
(ULSD levels, D4 RIN cents, CA LCFS credit, 45Z, CI scores, crush margins, SBO/UCO/tallow prices).
Running text near 65 characters wide; tabular figures everywhere numbers align.
