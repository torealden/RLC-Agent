# Claude Design brief — RLC "Implied Feedstock Value" Weekly

**Handoff note:** the *content model is locked*. A working sample (v5) demonstrates the exact
structure — **attach a screenshot of it** alongside this brief. Your job is the **look**, not the
content: make it beautiful and, above all, **consistent — every weekly edition must look identical**,
because the editions are mass-produced by code from this one template.

**This project has the "Lake, Field & Grain" design system synced** — the brand tokens and the
24-icon library. **Use the project's canonical icons** — single-bean `soybean`, the Erlenmeyer-flask
vegetable oils (`soybean-oil`, `canola-oil`, `sunflower-oil`, `corn-oil`, `cottonseed-oil`), the
meat-cut livestock (`steak`, `bacon`, `drumstick`), and the `uco` / `crude` droplet pair — **do not
redraw them.** The palette/type/horizon below match the synced tokens.

---

**RLC Implied Feedstock Value — a weekly feedstock-value reference.** $100/month, weekly cadence. It
must read as more credible and more comprehensive than a monthly Argus/Fastmarkets/Informa report,
and it wins on **transparency** — every value is *built in the open* via its fuel-and-policy stack.

## Positioning — drives everything
- A **reference**, stated with calm authority. Values first; the reader sees the numbers before prose.
- Validity comes from **showing the math** (the stack) — that visible build-up *is* the credibility.
- **Never mention PRAs, benchmarks-to-replace, or any aspiration to be one.** Just present IFV as
  self-evidently sound. The absence of that framing is deliberate.

## Brand — "Lake, Field & Grain"
Palette (rule: *Lake asserts, Field signals, Wheat lights*): Lake `#1B2A4A` anchor · Field `#3C7D22`
accent/positive · Wheat `#C8A951` light/hairline · Paper `#F7F3EB` ground · Sage `#B7CCA4` bands ·
Clay `#96492A` negative (not red) · Slate `#8A8F98` captions. Sequential **data ramp** for the
heatmap (low→high): `#C3DCA9 · #8FBF6B · #5F9E3B · #3C7D22 · #24500F`.
Type: **Georgia** display/headlines, **Calibri** body + tables (`tabular-nums`). Theme-aware.
**Horizon band** divider (signature): thin Wheat hairline on top, 2px gap, 4px Field band below.

## Structure — five parts, in order (see the sample)

1. **Masthead** — "Implied Feedstock Value", edition/date, horizon band, then a **values-first
   summary strip** of the headline IFVs (feedstock · region, ¢/lb, w/w arrow).

2. **The Feature** — the weekly analysis article (an editor's-letter reimagined as a *real article*,
   not a letter). Georgia headline + italic dek + 3–4 tight paragraphs of original argument, beside
   a small supporting chart. This is what proves original thinking; give it editorial weight.

3. **The IFV Board** — a **heatmap matrix**: feedstock rows (SBO, canola, UCO, tallow, DCO, CWG) ×
   region-fuel columns (US Gulf RD, Midwest RD, California RD, US SAF, Canada CFR). Each cell = IFV
   ¢/lb + w/w change, **shaded by magnitude** using the data ramp (dark = high). Lake header row.
   A color legend. This is the "compare every value across commodities and regions at a glance."

4. **Featured stacks** — 2–3 full build-ups, the transparency showcase. Each: icon + name + region
   header with the big IFV figure; a **price-context strip** (52-week range bar with a current marker
   that turns Wheat-with-a-Clay-ring when the weekly move ≥ 2σ, plus realized vol %); the **stack as
   a waterfall** — ULSD (or Jet) → −Weighted RIN → −Density adj → **=Implied fuel value** → +45Z →
   +D4 RIN → +LCFS/CFR → +Market premium → **=Total implied BBD value** → −Transport → **=Net to
   offtaker** → −Conversion → **÷ yield → Implied Feedstock Value (¢/lb)**, with running subtotals
   (bars step up in Field / down in Clay / subtotals in Slate / final IFV in Wheat); and a small
   **forward curve** (history Lake, forward dashed Field, Sage confidence band).

5. **Data that moved the value** — 3–4 callouts (NOPA crush, D4 RIN, EMTS generation, LCFS), each a
   Field/Clay chip + one-line claim + a sentence + its IFV impact.

Footer: methodology line + "Weekly · $100/month" positioning + horizon band.

## Constraints
Self-contained HTML, inline CSS, theme-aware (light on Paper, dark grounded on Lake). Real,
plausible biofuel copy — no lorem. Tabular figures everywhere numbers align; wide content
(the board) scrolls in its own container so the page never scrolls sideways. Consistency over
novelty: this template is reused every week.
