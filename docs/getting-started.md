# Getting started: Free Edition, enterprise dev, enterprise prod

Three walkthroughs, one per environment. Each one runs from an empty workspace to a working DPO with
dashboards and Genie spaces, and ends with a checklist to confirm it worked.

| | Free Edition | Enterprise dev / UAT | Enterprise prod |
|---|---|---|---|
| Profile | `free_edition` | `enterprise_dev` | `enterprise_prod` |
| Bundle target | `dev` | `enterprise_dev` | `enterprise_prod` |
| Data | synthetic only | real system tables | real system tables |
| Catalog | `workspace` (`dpo_*` schemas) | `dpo_dev` | `dpo` |
| Access control | none (single user) | enforced, drift reported | enforced, drift revoked |
| Runs as | you | you (the deployer) | `sp-dpo` (pipeline), `sp-dpo-admin` (install, governance) |
| Schedules | paused | paused | daily |
| Deploy from | Git folder or your laptop | your laptop or CI | CI from `main` (reviewed) |
| Time needed | 20 minutes | half a day (mostly waiting for platform admins) | a day plus approvals |

The install is the same everywhere: an **access gate** checks everything first and stops with one message
listing every problem and its fix. Nothing is changed until the gate passes, and every step can be re-run.

---

## A. Free Edition (your sandbox, synthetic data)

> Free Edition is non-commercial and has no access control. **Never load client data into it.** DPO uses
> synthetic telemetry there, which contains a realistic case for each of the 17 rules.

### Option 1: in the browser only (no tools to install)

1. **Sign in** to your Free Edition workspace.
2. **Add the repo.** Left sidebar → **Workspace** → your user folder → **Create → Git folder** → paste the
   repository URL → **Create Git folder**.
3. **Bind the profile to your workspace** (once). Copy your workspace host from the browser address bar, for
   example `dbc-1234abcd-5678.cloud.databricks.com`. Put it in `config/environments/free_edition.json` →
   `"workspace_hosts": ["<your host>"]`, commit and push from the Git folder (**Git** button), or edit it on GitHub
   and **Pull**. With the host listed, every notebook picks `free_edition` automatically.
4. **Install.** Open `notebooks/00_install` → leave the widgets as they are (`mode = install`, `schedule = paused`
   if you do not want a daily job) → **Run all**. It takes about 10–15 minutes on serverless. Results:
   - the **access gate** table (it should show no BLOCKER on Free Edition);
   - the step table: governance base, synthetic data, pipeline, viewer layer, lab, regression tests, verify,
     jobs, dashboards, Genie;
   - the top 25 recommendations.
5. **Open the dashboards.** Sidebar → **Dashboards** → *DPO · Cost optimization (free_edition)*, which has three
   pages: Overview, Recommendations, Budget & forecast. Then *DPO · Engineering (free_edition)*.
6. **Ask Genie.** Sidebar → **Genie** → *DPO · Ask cost optimization (free_edition)* → click a sample question
   such as "Which cost centres are at risk of going over budget this month?" → **Show code** to see the SQL.
7. **Try the budgets.** Edit `budgets` in `free_edition.json` (for example lower `cc-sales` to 7000) → run
   `notebooks/02_run_pipeline` → the Budget & forecast page changes status.

### Option 2: with the Databricks CLI and asset bundle

1. Install the Databricks CLI **1.3.0 or newer** (`databricks --version`).
2. Log in once: `databricks auth login --host https://<your-free-edition-host> --profile Optimization`.
   The `dev` target uses the CLI profile named `Optimization`; change `workspace.host` and `profile` in
   `databricks.yml` if yours differ.
3. From the repo folder:
   ```bash
   databricks bundle validate -t dev
   databricks bundle deploy   -t dev        # jobs, dashboards, Genie spaces (prefixed "[dev <you>]")
   databricks bundle run dpo_install -t dev # gate → setup → synthetic data → pipeline → views → verify
   ```
4. Steps A5–A7 as above. Re-run the daily analysis with `databricks bundle run dpo_pipeline -t dev`.

### Check that it worked

- [ ] `00_install` finished with "DPO installed" and no FAILED required step.
- [ ] `notebooks/04_regression_tests` → every check `passed = true`.
- [ ] Recommendations page lists P1/P2/P3 findings; Budget & forecast shows OVER_BUDGET / AT_RISK / WATCH / ON_TRACK.
- [ ] Genie answers the sample questions.
- [ ] `02_run_pipeline` output: the `forecast` row says `ai_forecast` or `fallback to baseline: <reason>`, and the
      `narrate` row shows `ai_generated` / `ai_cached`. Both AI features are optional; the fallbacks are normal on
      Free Edition if AI functions are not available.

---

## B. Enterprise dev / UAT (real telemetry, governed)

### B1. One-time platform set-up (account and workspace admins)

Send this list to your platform team. The install gate checks every item and tells you which one is missing.

1. **Groups** (account console → User management → Groups), assigned to the workspace:
   `dpo-admins`, `dpo-operators`, `dpo-analysts`, `dpo-viewers`, plus one group per cost-centre audience
   (for example `finance-cost-owners`). Add yourself to `dpo-admins`.
2. **System tables**: a metastore admin runs
   ```sql
   GRANT USE CATALOG ON CATALOG system TO `dpo-operators`;
   GRANT USE SCHEMA, SELECT ON SCHEMA system.billing  TO `dpo-operators`;
   GRANT USE SCHEMA, SELECT ON SCHEMA system.lakeflow TO `dpo-operators`;
   GRANT USE SCHEMA, SELECT ON SCHEMA system.query    TO `dpo-operators`;
   GRANT USE SCHEMA, SELECT ON SCHEMA system.compute  TO `dpo-operators`;
   GRANT USE SCHEMA, SELECT ON SCHEMA system.access   TO `dpo-admins`;
   GRANT CREATE CATALOG ON METASTORE TO `dpo-admins`;      -- or pre-create catalog dpo_dev for dpo-admins
   ```
   If a system schema is not visible, an account admin enables it (Account console → system tables).
3. **SQL warehouse**: a serverless (or Pro) SQL warehouse named `dpo-sql`. Give **CAN_USE** to all four DPO groups.
   The dashboards, Genie and `ai_forecast` run on it.
4. **AI model** (optional, for AI narratives): on the **Serving** page, check that `databricks-meta-llama-3-3-70b-instruct`
   (or another Databricks-hosted chat model) exists in your region. Give `dpo-operators` **CAN_QUERY**, and
   confirm the model is approved for your data. Use a different model by setting `ai_endpoint` in the profile.
5. **Serverless jobs** enabled (or see the classic-compute fallback at the end of `resources/dpo_jobs.yml`).

### B2. Configure (in a private fork of the repo)

> The public repository must never hold a client's configuration. Fork it privately per client.

Edit `config/environments/enterprise_dev.json`:

| Key | Set to |
|---|---|
| `workspace_hosts` | `["adb-<id>.<n>.azuredatabricks.net"]`: the dev workspace host. Required: runs elsewhere are refused |
| `catalog` | `dpo_dev`, or your naming |
| `workspace_ids` | the workspace IDs to analyse (`[]` = all in the account) |
| `governance.groups` | your group names, if they differ |
| `governance.cost_center_groups` | `{"finance-cost-owners": ["cc-finance"], ...}`: which group sees which cost centres |
| `budgets` | monthly budget per cost centre, e.g. `{"cc-finance": 6000}` |
| `contract_prices` / `price_multiplier` | your negotiated prices, or a discount (0.8 = 20% off list) |
| `cost_center_tag` | the tag key your clusters and jobs use (default `cost_center`) |

If your warehouse is not called `dpo-sql`, rename the `warehouse_id` lookup in `databricks.yml` (target `enterprise_dev`).

### B3. Deploy and install

```bash
databricks auth login --host https://<dev-workspace-host> --profile client-dev
databricks bundle validate -t enterprise_dev --profile client-dev
databricks bundle deploy   -t enterprise_dev --profile client-dev
databricks bundle run dpo_install -t enterprise_dev --profile client-dev
```

`dpo_install` runs the access gate first. If it is blocked, the job output lists every gap with its fix. Fix them
and run it again. Without the CLI, an admin can open `notebooks/00_install` in a Git folder and **Run all** (fill
`pipeline_run_as` / `governance_run_as` only if you use service principals in dev).

### B4. Give people access

1. Add users to the groups: platform engineers → `dpo-analysts`, cost owners → their cost-centre group **and**
   `dpo-viewers`.
2. Individual exceptions (an admin runs this in a SQL editor):
   `INSERT INTO dpo_dev.security.access_map VALUES ('lead@corp.com', 'cc-sales', current_user(), current_timestamp());`
3. Run `notebooks/00_setup` with `apply_governance = apply` after changing groups or cost-centre mappings.

### B5. Check that it worked

- [ ] Gate: no BLOCKER; warnings understood (for example "AI endpoint not ready" means narratives use templates).
- [ ] `[dev <you>] dpo-tests-enterprise_dev` job → regression and stress tasks green.
- [ ] As a viewer test user: the cost dashboard and the Genie cost space show only their cost centres, user names
      appear as `user-xxxxxxxxxxxx`, and the Engineering dashboard and Genie engineering space are not available.
- [ ] `notebooks/06_governance_audit` → section 1 shows only DPO groups; section 3 shows your runs.
- [ ] Recommendations look plausible to a platform engineer (sample 5 P1s and check the evidence).

---

## C. Enterprise production

### C1. Everything from B1, plus

1. **Two service principals** (Settings → Identity and access → Service principals):
   `sp-dpo` → member of **`dpo-operators` only** (never `dpo-admins`); `sp-dpo-admin` → member of `dpo-admins`.
   The gate refuses an operator SP that is also an admin.
2. **A deploy service principal** for CI with OAuth M2M credentials (client ID and secret), with workspace access
   and the **Service principal: User** role on `sp-dpo` and `sp-dpo-admin`.
3. **GitHub, in the private fork:**
   - environments `enterprise_dev` and `enterprise_prod`, each with secrets `DATABRICKS_HOST`,
     `DATABRICKS_CLIENT_ID`, `DATABRICKS_CLIENT_SECRET`;
   - `enterprise_prod`: **required reviewers** and deployment branch **`main` only**;
   - branch protection on `main` with "Require review from Code Owners", and the `.github/CODEOWNERS` team set
     to your DPO admins.

### C2. Configure

Edit `config/environments/enterprise_prod.json` like B2, with the production host, catalog `dpo`, the production
`budgets` and `cost_center_groups`, and a `managed_location` if your metastore needs one for new catalogs. Rename the
SP lookups (`sp-dpo`, `sp-dpo-admin`) and the warehouse lookup in `databricks.yml` if your names differ. Open a pull
request; a code owner reviews and merges it.

### C3. Deploy and install

1. GitHub → **Actions → dpo-deploy → Run workflow** → branch `main`, target `enterprise_prod`, `run_install` ticked.
   A reviewer approves the environment. The workflow validates the bundle, deploys it, then runs `dpo_install` as
   `sp-dpo-admin`.
2. Read the `dpo_install` run output. If it is blocked, fix the listed items (usually a missing grant), then re-run
   the workflow with `run_install` ticked, or run the `dpo-install-enterprise_prod` job from the Jobs page (admins only).
3. Schedules are active in production: `dpo-pipeline-enterprise_prod` runs daily at 06:30 IST and
   `dpo-governance-enterprise_prod` at 08:00 IST (re-applies grants and revokes drift).

### C4. Go-live checklist

- [ ] Gate passed with the two service principals verified (`pipeline run-as` and `governance run-as` OK).
- [ ] First pipeline run succeeded; `ops.source_status` shows every required source `available`.
- [ ] Viewer test as in B5 (row filter, hashed identities, no engineering access).
- [ ] Dashboards: **Share** shows group permissions only, no embedded credentials. Genie: admins CAN_MANAGE,
      everyone else CAN_RUN.
- [ ] AI: the endpoint is approved for your data; the `narrate` and `forecast` rows in the run log show the expected method.
- [ ] Engineering dashboard → **DPO's own cost** shows DPO's jobs and SQL after a day of billing; the
      savings-to-cost ratio is what you report to management.
- [ ] Budgets agreed with finance; cost owners know what OVER_BUDGET / AT_RISK / WATCH mean (see `docs/ai-features.md`).
- [ ] Failure e-mails: uncomment `email_notifications` in `resources/dpo_jobs.yml` (DPO admins' distribution list).
- [ ] Monthly review booked: `06_governance_audit` (leavers, drift, DENIED and BLOCKER_ACCEPTED rows, access-map
      re-certification), Genie **Monitoring** and **Benchmarks**.

### C5. Day-to-day operation

| Task | How |
|---|---|
| Change rules, thresholds, budgets, cost-centre mapping | pull request to the profile or `config/rules.json` → review → dpo-deploy (no `run_install` needed for rules/budgets; with `run_install` ticked after group or cost-centre changes) |
| Re-run a failed day | Jobs → `dpo-pipeline-enterprise_prod` → **Run now** (full recompute; safe) |
| Add a viewer | add them to their cost-centre group and `dpo-viewers` (no code change) |
| Act on a finding | create the work item (ADO), reference the rule id, measure before/after (`03_optimization_lab` for the fix type) |
| Roll back | redeploy the previous commit with dpo-deploy; data is a full recompute, so the next run is consistent |
| Remove DPO | `databricks bundle destroy -t enterprise_prod`, then an admin drops catalog `dpo` after exporting what audit needs |

---

## Troubleshooting

| Message | Cause | Fix |
|---|---|---|
| `DPO install BLOCKED ... [workspace binding]` | profile bound to another workspace | use the right profile, or add this host to `workspace_hosts` |
| `refusing to read real system tables into an ungoverned schema` | `free_edition` (or `source_mode=system` with governance off) in a real workspace | use `enterprise_dev` / `enterprise_prod` |
| `catalog '...' is not the profile's catalog` | job or widget overrode `catalog` on an enforced profile | remove the override, or add the catalog to `governance.allowed_catalogs` |
| `[operators_system_billing] BLOCKER` | operators cannot read a system schema | step B1.2 |
| `not an application ID (UUID)` | a name was typed instead of the SP application ID | copy the Application ID from the service principal's page |
| `forecast: fallback to baseline: ...` | no Pro/serverless warehouse, `ai_forecast` not available, or no CAN_USE | step B1.3; the baseline forecast is used meanwhile |
| `narrate: fallback: ...` or `ai_rejected > 0` | endpoint missing, no CAN_QUERY, or replies in the wrong format | step B1.4; template narratives are used meanwhile |
| Dashboard "Can't extract a value from source" | an old v1 dashboard reading `workspace.dpo.ai_recommendations` | delete the old dashboard; use the bundle-deployed ones |
| Genie answers "no data" for a viewer | no cost-centre mapping for that person | `cost_center_groups` in the profile or `security.access_map` |
