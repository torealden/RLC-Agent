# Helios WAPR — current readings + 4-week trend (actuals through 2026-08-04)

Source: Helios Enterprise API `api.helios.sc`, pulled live 2026-08-05. WAPR = Weighted Average
Percent Risk, 0–100. "vs norm" = composite minus same-day historical average (positive = riskier
than normal). Factor columns are current-day readings; the API publishes a historical norm for the
composite only, not per factor. Trend column = composite at −28d → −21d → −14d → −7d → now.

**Coverage caveats (Helios structural, not fixable on our side):**
- Country-level only. "Corn Belt", "N Plains", "Prairies" rows are US/CA national aggregates.
- **No sunflower coverage at all** — Black Sea and Argentina sunflower cannot be served from Helios.
- **No Russia rapeseed pair.** Canola=rapeseed slug exists but `ru` is not in its country list.
- Wheat is NOT split spring/winter (durum is separate) — US row is all-wheat, already Post-Harvest
  dominated by winter wheat; it is a weak proxy for N Plains spring wheat.

| Region / crop | Phase | WAPR | vs norm | Hot | Cold | Wet | Dry | 4-wk composite trend |
|---|---|---:|---:|---:|---:|---:|---:|---|
| US corn (natl) | Grain/Fruit Fill | 27.9 | −7.5 | 13.5 | 4.6 | 5.2 | 8.3 | 31.9→25.8→33.2→33.0→27.9 (flat, below norm) |
| US soybeans (natl) | Reproductive Dev | 26.5 | −5.8 | 13.0 | 4.8 | 4.0 | 8.6 | 18.5→15.0→19.1→19.6→26.5 (**rising**) |
| US wheat ALL (natl) | Post-Harvest | 21.2 | +0.2 | 13.0 | 3.8 | 4.3 | 5.4 | 19.1→18.5→17.5→20.3→21.2 (drifting up, at norm) |
| Canada canola (natl) | Harvest | 33.8 | −0.2 | 10.6 | 10.1 | 1.9 | 20.1 | 37.2→42.8→37.9→35.2→33.8 (easing; **Mid severity**) |
| Ukraine wheat | Peak Harvest | 16.4 | −4.0 | 8.6 | 1.3 | 9.6 | 0.0 | 13.3→10.6→14.6→10.3→16.4 (up on wet-harvest risk) |
| Ukraine rapeseed | Planting | 18.4 | −7.5 | 17.1 | 0.5 | 0.2 | 1.3 | 9.9→7.0→5.9→3.4→18.4 (**sharp 1-wk heat jump**) |
| Russia wheat ALL | Peak Harvest | 17.3 | −2.0 | 6.2 | 1.2 | 12.1 | 0.0 | 17.2→14.7→16.3→15.8→17.3 (flat; wet is the factor) |
| Russia rapeseed | — | — | — | — | — | — | — | NOT COVERED by Helios |
| France rapeseed | Planting | 25.6 | **+11.7** | 16.7 | 1.9 | 0.0 | 14.1 | 22.1→33.1→20.0→15.9→25.6 (choppy, hot+dry planting) |
| Germany rapeseed | Harvest | 15.0 | +0.8 | 9.2 | 3.7 | 2.6 | 1.9 | 10.7→8.9→11.0→11.5→15.0 (mild rise, near norm) |
| Poland rapeseed | Planting | 17.9 | +4.8 | 16.0 | 2.1 | 0.0 | 0.0 | 7.4→4.4→8.5→16.0→17.9 (**rising, heat at planting**) |
| Malaysia oil palm | Moderate Prod | 17.8 | −2.5 | 11.2 | 0.0 | 9.0 | 0.3 | 17.1→18.8→17.4→13.4→17.8 (flat, benign) |
| Indonesia oil palm | Moderate Prod | 23.0 | **+7.4** | 18.7 | 0.1 | 2.1 | 6.6 | 17.9→17.8→21.4→16.8→23.0 (rising, heat-led) |
| Argentina corn | Post-Harvest | 4.4 | −14.0 | 2.6 | 0.7 | 0.3 | 1.1 | 17.2→11.0→8.6→5.1→4.4 (off-season wind-down) |
| Argentina wheat | Planting | 6.6 | **−20.4** | 1.7 | 0.9 | 0.1 | 4.1 | 22.6→13.6→12.0→9.5→6.6 (falling; benign planting) |

**Movers worth a sentence in the report:**
- **US soybeans**: composite up ~8 pts over 4 weeks into Reproductive Development (heat 13.0 + dry
  8.6 both building), though still ~6 pts below the historical norm for this date.
- **Canada canola**: only Mid-severity row on the board. Risk mix rotated from heat to **dry (20.1)**
  going into harvest; composite easing but running right at its (high) seasonal norm.
- **Northern-hemisphere rapeseed planting is the hot spot**: France +11.7 vs norm (heat+dry),
  Poland +4.8 (heat), Ukraine jumped 3.4→18.4 in one week on heat. Germany (still harvesting) benign.
- **Indonesia palm** +7.4 vs norm, heat-led and rising; Malaysia normal.
- **Argentina** planting season strikingly benign — wheat 20 pts below normal risk.
- All Black Sea readings remain Low severity in absolute terms.
