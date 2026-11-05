# Genie guide

The Genie spaces (Genie agents) are **code**, like the dashboards. `src/dpo/genie.py` generates one copy per
profile with that profile's table names, and the bundle deploys them.

```
src/dpo/genie.py ──python -m dpo.genie──► genie/<profile>/{cost_optimization,engineering}.geniespace.json
                                                   │
                  databricks bundle deploy -t <target>   (resources.genie_spaces in databricks.yml, CLI >= 1.3.0)
                  or notebooks/00_install (Git folder, no CLI; widget genie = yes)
```

## What you get

### DPO · Ask cost optimization (viewers, analysts, operators)

Tables: `reporting.recommendations`, `reporting.recommendation_narratives`, `reporting.workload_summary`,
`reporting.job_scorecard`, `reporting.cost_by_dimension`. Each person sees only their cost centres.

Sample questions: top 10 P1 recommendations · spend per cost centre · most expensive workloads · estimated
saving by category · jobs that fail most · daily cost by product.

### DPO · Ask engineering (analysts, operators)

Tables: gold `recommendations`, `recommendation_narratives`, `workload_summary`, `workload_cost_daily`,
`job_scorecard`, `run_cost`, `query_fingerprint_stats`, `cost_by_dimension`; ops `run_log`, `source_status`,
`dq_report`, `rule_status`; DPO's own cost: gold `dpo_cost_summary`, `dpo_cost_daily`, `dpo_run_cost`.

Sample questions: query shapes scanning most data without pruning · highest cost per successful run · did the
latest pipeline run succeed · missing sources · direct vs allocated vs idle spend · findings per rule · what
DPO itself costs and whether it pays for itself.

### In both spaces

| Part | Content |
|---|---|
| Instructions | currency and window; the score formula and P1/P2/P3 thresholds; savings are *estimated* until validated; `workload_key` format; `(untagged)` for NULL cost centre; R01–R17 meanings; SELECT only. Cost space: which columns may be NULL for you and how to compute shares over visible rows. Engineering space: join keys and "latest run". |
| Example SQL | six question → query pairs (each one executed against real pipeline output in `tests/test_genie.py`) |
| Measures | estimated monthly savings, P1 findings, total cost (+ job failure rate in engineering) |
| Benchmarks | the six example questions with their expected SQL |

## Use it

1. Deploy (`databricks bundle deploy -t <target>`) after `dpo_install` has succeeded once, so the tables exist.
2. Workspace sidebar → **Genie** → *DPO · Ask cost optimization* (or *… (free_edition)* on Free Edition).
3. Ask a question or click a sample question. Check the generated SQL with **Show code** before you act on a number.
4. After each deploy, an admin opens **Benchmarks → Run all**: answers should match the expected SQL.

Readers need **CAN_RUN** on the space (granted by the bundle to groups) and SELECT on its tables (granted by
`00_setup`). They do not need warehouse permission for Genie: compute runs on the author's credentials,
data access is always the reader's.

## Change it

Edit `src/dpo/genie.py` (instructions, sample questions, examples, measures, tables), then:

```bash
cd src && python -m dpo.genie          # regenerates genie/<profile>/*.geniespace.json
cd .. && pytest -q tests/test_genie.py  # rules, layers, no value sampling on secured views, SQL runs
databricks bundle deploy -t <target>
```

Edits made in the Genie UI are overwritten by the next deploy. To try an idea in the UI first, make it in a
copy of the space, then port it to `genie.py`.

Rules the generator keeps (the API rejects or silently breaks spaces otherwise): `version: 2`; every `id` 32
lowercase hex (stable hashes here, so a regenerate gives an identical file); tables sorted by identifier with no
`id`; column configs sorted by name; every list sorted by `id`; questions, SQL and instruction text as lists of
strings; benchmark SQL inside `answer[].content`; at most 50 benchmarks.

## Do not

- Switch on **format assistance / entity matching** for the cost space: values are sampled with the author's
  rights and shown to every user, which leaks other cost centres (see GOVERNANCE.md, "Genie spaces").
- Add gold, ops, silver or `security`/audit tables to the cost space, or share the engineering space with viewers.
- Give anyone but admins CAN_EDIT or CAN_MANAGE.
