# Yield Model Validation Report — Cotton

**Generated:** 2026-02-12 19:09
**Test Years:** 2018–2024

## 1. Accuracy by Forecast Week

| Week | N | RMSE | MAE | Mean Error | Dir Accuracy | Target RMSE | Pass? |
|:---:|:---:|:---:|:---:|:---:|:---:|:---:|:---:|
| 18 | 28 | 74.4 | 61.8 | +14.3 | 96% | 15.0 | No |
| 22 | 28 | 87.5 | 75.7 | +9.0 | 93% | 12.0 | No |
| 26 | 28 | 84.5 | 73.5 | +7.5 | 86% | 10.0 | No |
| 30 | 28 | 83.4 | 72.9 | +5.4 | 89% | 7.0 | No |
| 34 | 28 | 85.7 | 75.1 | +6.6 | 86% | 5.0 | No |
| 38 | 28 | 84.5 | 73.3 | +9.8 | 89% | 4.0 | No |

## 2. Skill Scores vs Benchmarks

| Week | Skill vs Trend | Skill vs Last Year | Skill vs 5yr Avg |
|:---:|:---:|:---:|:---:|
| 18 | — | — | — |
| 22 | — | — | — |
| 26 | — | — | — |
| 30 | — | — | — |
| 34 | — | — | — |
| 38 | — | — | — |

> Positive skill = model outperforms benchmark. Target: > 0.200 by week 26.

## 3. Bias Analysis

No forecast/actual data available for bias analysis.


## 4. Worst-Case Analysis

### Week 26 — Largest Errors

| Year | State | Predicted | Actual | Error |
|:---:|:---:|:---:|:---:|:---:|
| 2024 | KS | 871.7 | 720.0 | +151.7 |
| 2023 | OK | 553.0 | 403.0 | +150.0 |
| 2022 | OK | 525.0 | 387.0 | +138.0 |
| 2021 | KS | 935.1 | 1069.0 | -133.9 |
| 2020 | OK | 811.0 | 939.0 | -128.0 |
### Week 30 — Largest Errors

| Year | State | Predicted | Actual | Error |
|:---:|:---:|:---:|:---:|:---:|
| 2024 | KS | 862.5 | 720.0 | +142.5 |
| 2023 | KS | 834.0 | 693.0 | +141.0 |
| 2020 | OK | 800.3 | 939.0 | -138.7 |
| 2022 | OK | 518.5 | 387.0 | +131.5 |
| 2023 | MO | 1249.3 | 1120.0 | +129.3 |

## 5. Recommendations

- **Bias:** Within acceptable range
- **Week 26:** RMSE 84.5 exceeds target 10.0. Consider adding more features or adjusting ensemble weights.
- **Week 30:** RMSE 83.4 exceeds target 7.0. Consider adding more features or adjusting ensemble weights.
