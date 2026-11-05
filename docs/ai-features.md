# AI features

DPO uses AI in four places. The **rules decide** what gets flagged and how it is scored; AI **explains**,
**forecasts** and **answers questions**. Every AI output is labelled, and a non-AI fallback keeps it
working when the AI service is unavailable.

| Feature | Databricks capability | Output | Fallback |
|---|---|---|---|
| 1. AI narratives | `ai_query` on a Databricks-hosted LLM | `gold.recommendation_narratives` | fixed template |
| 2. Spend forecast and budget outlook | `ai_forecast` on a SQL warehouse | `gold.cost_forecast`, `gold.budget_outlook` | weekday-average baseline |
| 3. Genie spaces | AI/BI Genie (question to SQL) | answers in Genie | dashboards |
| 4. Dashboards (AI/BI) | Lakeview | 2 dashboards | — |

This page covers 1 and 2. Genie: `genie-guide.md`. Dashboards: `dashboard-guide.md`.

---

## 1. AI narratives

### Problem
The rules produce precise but terse findings: `R01_COMPUTE_IDLE on cluster:abc. avg_cpu_pct = 6.20 against
a threshold of 10.00`. The workload owner still has to work out why it happened, what exactly to change, how
to prove the change worked, and what could go wrong. Without that, findings sit in the backlog. A senior
architect writing this for every finding does not scale.

### What it does
Each daily run, the top `ai_max_rows` findings (default 25, by score) are sent to a Databricks-hosted model,
which writes four labelled lines:

```
Root cause:  why this metric is high for this kind of workload
Fix:         the concrete Databricks change, building on the rule's recommended action
Validation:  what to measure before and after, and for how long
Risk:        what could go wrong and how to roll back
```

The text appears in the **Why flagged** column of the Recommendations page and in Genie answers, prefixed
`[AI-generated, review before acting]`. Findings below the top N keep the template.

### How it works
1. **Minimal input.** Per finding the model gets the rule, category, severity, workload type, priority and
   score, metric, observed value, threshold, confidence, feasibility, estimated saving and recommended
   action. It never gets workload IDs, cost centres, job names, users, SQL text or evidence JSON.
2. **Injection-resistant prompt.** The data is wrapped in `<data>` tags and declared untrusted; the model
   must not invent numbers, names, savings or SQL.
3. **Output validation.** A reply is used only if it contains all four labels, is at most 1,200 characters
   and contains no links or echoed tags. Anything else is discarded and the template is kept
   (`ai_rejected` in the run log).
4. **Cache.** Accepted narratives are stored in `gold.ai_narrative_cache`, keyed by a SHA-256 hash of
   (endpoint, prompt version, input). An unchanged finding is not sent again, so the text stays the same
   from day to day and repeat runs cost no tokens. Entries expire after 90 days; changing the prompt
   (`PROMPT_VERSION`) or the endpoint regenerates them.
5. **Fallback.** If the endpoint fails, cached narratives are still used and the rest keep the template.
   The pipeline never fails because of AI.

The run log (`ops.run_log`, stage `narrate`) records `ai_generated`, `ai_cached` and `ai_rejected` for each run.

### Configure (profile)
| Setting | Default | Meaning |
|---|---|---|
| `ai_enabled` | `true` in every profile | turn narratives on or off |
| `ai_endpoint` | `databricks-meta-llama-3-3-70b-instruct` | must start with `databricks-` or `system.ai.` (Databricks-hosted) unless `governance.ai_allow_external` is approved |
| `ai_max_rows` | `25` | findings narrated per run (1–200) |

Before go-live: check the endpoint exists in the workspace region (**Serving** page), and give
`dpo-operators` **CAN_QUERY** on it. The install access gate reports the endpoint's state as OK or WARN.
Cost: at most 25 short calls per day, fewer after the first run thanks to the cache.

---

## 2. Spend forecast and budget outlook

### Problem
Databricks spend is only known after it is incurred, and the monthly invoice arrives after the month has
closed. A cost centre that will overrun its budget usually finds out when it is too late to act. The run
rate in the dashboard (window cost × 30 / days) ignores weekly patterns (weekday batch load, quiet weekends)
and recent trends, so it is a poor predictor.

### What it does
Every run forecasts daily spend **per cost centre** for the rest of the current month, with a prediction
interval (default 80%), and compares it with the monthly budget set in the profile:

```
projected month total = spent so far this month + forecast for the remaining days
```

| Status | Meaning | Action |
|---|---|---|
| `OVER_BUDGET` | spent so far is already above budget | stop or defer non-critical workloads now |
| `AT_RISK` | projected total is above budget | act this week: apply the P1 findings for that cost centre |
| `WATCH` | only the upper bound of the projection is above budget | monitor; no action yet |
| `ON_TRACK` | even the upper bound is within budget | — |
| `NO_BUDGET` | no budget set for this cost centre | set one in the profile |

Where to see it:
- Dashboard **DPO · Cost optimization → Budget & forecast**: counters (cost centres at risk, projected month,
  budget, projected overrun), the outlook table worst-first, and a daily actual vs forecast chart with the range.
- Genie: "Which cost centres are at risk of going over budget this month?"
- Tables: `reporting.budget_outlook`, `reporting.cost_forecast`. They are row-filtered like every other
  reporting view, so a cost owner sees only their own cost centres.

### How it works
1. The pipeline builds the daily cost per cost centre over the look-back window, with 0 for days without
   spend (`ai_forecast` does not fill gaps itself). Untagged spend is its own series, `(untagged)`.
2. `ai_forecast` is a Databricks SQL function (it needs a Pro or serverless SQL warehouse), so the pipeline
   writes that series to `gold.forecast_input` and runs
   `CREATE OR REPLACE TABLE gold.forecast_raw AS SELECT * FROM ai_forecast(TABLE(gold.forecast_input), ...)`
   on the job's SQL warehouse through the Statement Execution API, **as the pipeline identity**. Only
   validated table names, a date and numbers go into that statement.
3. Forecasts are clipped at 0 and saved with `method = 'ai_forecast'`.
4. If there is no warehouse, the function is not enabled, or the call fails, a **baseline** is used instead:
   the cost centre's average for that weekday, scaled by the last 7 days' trend (limited to 0.5–2×), with an
   interval from the spread of past daily values. It is recorded as `method = 'baseline'` and the reason goes
   in the run log, so a baseline never passes for AI.
5. The outlook adds the month-to-date actuals, sums the daily forecast and its bounds, and sets the status.
   Summing daily bounds gives a deliberately wide (conservative) monthly range.
6. Fewer than 14 days of history means no forecast (too little data for weekly patterns).

The outlook month is the month of the day after the latest usage date. On the last day of a month it
switches to the next month.

### Configure (profile)
| Setting | Default | Meaning |
|---|---|---|
| `budgets` | `{}` | monthly budget per cost centre, e.g. `{"cc-finance": 6000, "cc-sales": 9000}` (currency of the profile) |
| `forecast_enabled` | `true` | turn the stage on or off |
| `forecast_method` | `ai_forecast` | or `baseline` to skip AI |
| `forecast_interval` | `0.8` | prediction interval width (0.5–0.99) |
| `forecast_version` | `""` | `""` = Databricks default; `"2"` = newer model (needs the *Predictive AI Functions* preview) |
| `lookback_days` | `30` | longer history (60–90) gives better forecasts |

The SQL warehouse comes from the bundle (`var.warehouse_id`, passed to the pipeline job as `warehouse_id`);
without the bundle, the first serverless warehouse visible to the job identity is used. Give `dpo-operators`
**CAN_USE** on it. The install access gate reports whether a Pro or serverless warehouse is visible.

---

## Governance summary

| Concern | Control |
|---|---|
| Data sent to the model | metrics and rule metadata only; no IDs, names, cost centres, users or SQL |
| Where the model runs | Databricks-hosted endpoints only (`databricks-*`, `system.ai.*`) unless approved |
| Prompt injection | input declared untrusted; output must match the four-label format; no links; length cap |
| Wrong AI output | labelled "review before acting"; the rule's metric, threshold and action are always shown next to it |
| Forecast data | runs as the pipeline identity on your warehouse; outputs filtered by cost centre for viewers |
| Hidden degradation | `narrative_source` and `method` columns plus run-log notes show when AI was or was not used |

Tests: `tests/test_ai_features.py` (stand-ins for both AI functions; everything around them runs for real).
