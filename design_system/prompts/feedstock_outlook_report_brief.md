# Claude Design brief — RLC "Feedstock Price Outlook" report template

Paste this into Claude Design. **Attach the icon SVGs** from `design_system/icons/svg/`
(soybean, canola, oil-droplet, wheat, corn, barrel, refinery, grain-silo, tanker-vessel,
tanker-truck, rail-tanker, renewable, steak, bacon, drumstick). They are single-color line
icons on `currentColor` — recolor them by setting `color`.

---

**Build a two-page report template for Round Lakes Commodities: a "Feedstock Price Outlook."**
It delivers rolling price forecasts for **soybean oil, canola oil, and sunflower oil** and — more
importantly — *the reasons behind the price* (the explanation is the product). Editorial, restrained,
data-forward. Design it to work in the viewer's light and dark theme.

## Brand system — "Lake, Field & Grain" (use exactly)
Palette (semantic rule: *Lake asserts, Field signals, Wheat lights*):
- **Lake `#1B2A4A`** — anchor: body text, mastheads, dark surfaces, table headers, chart base
- **Field `#3C7D22`** — accent: rules, positive values, forecast lines, the horizon
- **Wheat `#C8A951`** — light: highlights, hairlines, callout borders (never meaning alone)
- **Paper `#F7F3EB`** — page/fill ground
- **Sage `#B7CCA4`** — tints, confidence bands, chart fills
- **Clay `#96492A`** — counter: negatives / bearish (replaces red)
- **Slate `#8A8F98`** — captions, axes, secondary text

Typography: **Georgia** for display/headings, **Calibri** for body and all tables
(`font-variant-numeric: tabular-nums` on every figure).

**Horizon band** (the signature motif — use it as the section divider, never a plain rule):
a thin **Wheat** hairline on top, a 2px gap, then a 4px **Field** band below (grounded — light
above, land below). Print equivalent: 1.2pt Wheat / 2pt gap / 4pt Field.

## Page 1 — Cover
- Eyebrow: "Round Lakes Commodities · Feedstock Price Outlook"
- Georgia title, e.g. "Vegetable Oils — Monthly Outlook", with the issue month + a horizon band beneath.
- A row of the three commodity icons (soybean, canola, plus **sunflower — pending; use the oil-droplet
  as a stand-in and label it**) in Field, each with its oil name and a one-line directional call
  (e.g. "Soybean oil · bullish" with a Field up-chip; "Canola oil · bearish" with a Clay down-chip).
- Footer: source line in Slate + a horizon band.

## Page 2 — Per-commodity outlook (design one, it repeats for each oil)
- Header: the commodity's icon + name (Georgia), a Field/Clay directional chip, and the forecast
  figure (e.g. "48.5¢/lb, +6% 3-mo") in tabular figures.
- **Price forecast chart**: history line in **Lake**, the forecast segment in **Field** (dashed or
  solid, clearly distinct), a **Sage** confidence band around the forecast, faint grid, an emphasized
  endpoint. Axes/labels in Slate.
- **"What changed & why"** — the hero of the page. A stack of 3–4 short callouts, each a driver:
  a small Field or Clay chip (bullish/bearish), a one-line claim, and a sentence of reasoning.
  This is the deliverable's real value; give it visual weight.
- A compact **S&D snapshot** table (production / crush / exports / ending stocks / stocks-to-use),
  Lake header row with white bold Calibri, Wheat highlight on the key row, Clay for negative deltas,
  Field for positive.

## Constraints
Self-contained HTML, inline CSS, theme-aware (light on Paper, dark grounded on Lake). Real content
throughout — no lorem; invent plausible vegetable-oil market copy (China demand, Black Sea sunflower
supply, biofuel pull, crush margins, palm substitution). Keep running text near 65 characters wide.
