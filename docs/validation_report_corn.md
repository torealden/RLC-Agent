# Yield Model Validation Report — Corn

**Generated:** 2026-02-12 19:09
**Test Years:** 2018–2024

## 1. Accuracy by Forecast Week

| Week | N | RMSE | MAE | Mean Error | Dir Accuracy | Target RMSE | Pass? |
|:---:|:---:|:---:|:---:|:---:|:---:|:---:|:---:|
| 18 | 91 | 8.2 | 6.4 | +1.0 | 92% | 15.0 | Yes |
| 22 | 91 | 8.1 | 6.5 | +0.7 | 93% | 12.0 | Yes |
| 26 | 91 | 8.4 | 6.8 | +0.6 | 92% | 10.0 | Yes |
| 30 | 91 | 8.5 | 7.0 | -0.6 | 91% | 7.0 | No |
| 34 | 91 | 8.7 | 7.1 | -0.7 | 92% | 5.0 | No |
| 38 | 91 | 8.8 | 7.0 | -0.9 | 92% | 4.0 | No |

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
| 2021 | SD | 154.7 | 133.0 | +21.7 |
| 2018 | SD | 153.3 | 173.0 | -19.7 |
| 2019 | IN | 178.9 | 161.0 | +17.9 |
| 2021 | ND | 125.7 | 108.0 | +17.7 |
| 2019 | IL | 196.4 | 180.0 | +16.4 |
### Week 30 — Largest Errors

| Year | State | Predicted | Actual | Error |
|:---:|:---:|:---:|:---:|:---:|
| 2018 | SD | 150.7 | 173.0 | -22.3 |
| 2021 | ND | 125.9 | 108.0 | +17.9 |
| 2019 | OH | 175.3 | 158.0 | +17.3 |
| 2021 | SD | 149.8 | 133.0 | +16.8 |
| 2019 | IN | 177.5 | 161.0 | +16.5 |

## 5. Recommendations

- **Bias:** Within acceptable range
- **Week 30:** RMSE 8.5 exceeds target 7.0. Consider adding more features or adjusting ensemble weights.
