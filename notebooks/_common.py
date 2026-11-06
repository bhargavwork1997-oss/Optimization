# Databricks notebook source
# MAGIC %md
# MAGIC # _common · Shared bootstrap (do not run on its own)
# MAGIC
# MAGIC Every DPO notebook starts with `%run ./_common`. It prepares the same environment everywhere, so a notebook
# MAGIC behaves identically in a Git folder, in a bundle deployment and in a scheduled job.
# MAGIC
# MAGIC | | |
# MAGIC |---|---|
# MAGIC | **What it does** | finds the project folder, imports `src/dpo`, detects which profile fits this workspace, creates the shared widgets, builds and validates the configuration (`cfg`) |
# MAGIC | **Defines** | `REPO_ROOT`, `HOST`, `cfg`, `CFG_ERROR`, `build_cfg(**overrides)`, `forecast_warehouse(cfg)` |
# MAGIC | **Reads** | `config/environments/<profile>.json`, `config/rules.json` |
# MAGIC | **Writes** | nothing |
# MAGIC
# MAGIC **Profile selection.** The profile in `config/environments/` whose `workspace_hosts` lists this workspace is
# MAGIC chosen automatically. A job parameter, the `profile` widget or the `DPO_PROFILE` environment variable can override
# MAGIC it; the pipeline then re-checks on every run that the profile is allowed in this workspace (runtime guard).
# MAGIC
# MAGIC **Widgets.** Leave them blank to use the profile's values. Values typed here are validated (allow-listed
# MAGIC characters, ranges, date format) before any SQL is built from them.

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1 · Locate the project and import the DPO package
# MAGIC Works from a Git folder and from a bundle deployment (`<root>/files/...`). `src/` is put on the Python path, and
# MAGIC the modules are reloaded so edits in `src/` take effect without detaching the notebook.

# COMMAND ----------

import os
import sys


def _project_root():
    """Folder holding src/ and config/. Works in a Git folder and in a bundle deployment
    (<root_path>/files/...): first the notebook's working directory, then the notebook path."""
    candidates = [os.path.abspath(os.path.join(os.getcwd(), ".."))]
    try:
        nb = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
        nb = nb if nb.startswith("/Workspace") else "/Workspace" + nb
        candidates.append(os.path.dirname(os.path.dirname(nb)))
    except Exception:
        pass
    for c in candidates:
        if os.path.isdir(os.path.join(c, "src", "dpo")) and os.path.isdir(os.path.join(c, "config")):
            return c
    raise RuntimeError(
        f"Cannot find the DPO project (src/dpo and config/) from {candidates}. "
        "Run notebooks from the repo's notebooks/ folder or a bundle deployment."
    )


REPO_ROOT = _project_root()
SRC = os.path.join(REPO_ROOT, "src")
if SRC not in sys.path:
    sys.path.insert(0, SRC)

# Reload so edits in src/ are picked up without detaching the notebook
import importlib

import dpo
import dpo.config
import dpo.forecast
import dpo.governance
import dpo.installer
import dpo.pipeline
import dpo.recommend
import dpo.rules
import dpo.selfcost
import dpo.synth
import dpo.transforms
import dpo.validation

for m in (
    dpo.config,
    dpo.transforms,
    dpo.synth,
    dpo.pipeline,
    dpo.rules,
    dpo.recommend,
    dpo.validation,
    dpo.governance,
    dpo.installer,
    dpo.forecast,
    dpo.selfcost,
):
    importlib.reload(m)

from dpo.config import PROFILE_DIR, DPOConfig, detect_profile

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2 · Detect this workspace and pick the profile
# MAGIC The host comes from Spark, the notebook context or the SDK, in that order. `detect_profile` matches it against each
# MAGIC profile's `workspace_hosts`; no match falls back to `free_edition`.

# COMMAND ----------


def workspace_host():
    """This workspace's host name, or None. Tries Spark conf, then the notebook context."""
    try:
        h = spark.conf.get("spark.databricks.workspaceUrl", None)
        if h:
            return h
    except Exception:
        pass
    try:
        ctx = dbutils.notebook.entry_point.getDbutils().notebook().getContext()
        return ctx.browserHostName().get()
    except Exception:
        pass
    try:
        from databricks.sdk import WorkspaceClient

        return WorkspaceClient().config.host
    except Exception:
        return None


HOST = workspace_host()
PROFILES = sorted(f[:-5] for f in os.listdir(PROFILE_DIR) if f.endswith(".json"))
AUTO_PROFILE, AUTO_REASON = detect_profile(HOST)
if AUTO_PROFILE not in PROFILES:
    AUTO_PROFILE = "free_edition"

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3 · Shared widgets
# MAGIC `profile` is pre-selected from step 2. All others are optional overrides of the profile. `warehouse_id` is the SQL
# MAGIC warehouse `ai_forecast` runs on; the scheduled job passes the bundle's warehouse here.

# COMMAND ----------

dbutils.widgets.dropdown("profile", AUTO_PROFILE, PROFILES, "Profile (auto-detected)")
dbutils.widgets.text("catalog", "", "Catalog (blank = profile)")
dbutils.widgets.text("schema_prefix", "", "Schema prefix (blank = profile)")
dbutils.widgets.dropdown("source_mode", "", ["", "synthetic", "system"], "Source mode (blank = profile)")
dbutils.widgets.text("lookback_days", "", "Look-back days (blank = profile)")
dbutils.widgets.text("end_date", "", "End date YYYY-MM-DD (blank = today)")
dbutils.widgets.text("inventory_catalogs", "", "Catalogs to scan for layout (comma list)")
dbutils.widgets.text("warehouse_id", "", "SQL warehouse id for ai_forecast (blank = first serverless)")


def forecast_warehouse(cfg) -> str:
    """ai_forecast runs only on a Pro/serverless SQL warehouse: the job passes one (bundle var.warehouse_id),
    otherwise the first serverless warehouse visible to this identity. "" = baseline forecast."""
    wh = dbutils.widgets.get("warehouse_id").strip()
    if wh or not (cfg.forecast_enabled and cfg.forecast_method == "ai_forecast"):
        return wh
    try:
        from databricks.sdk import WorkspaceClient

        from dpo.dashboard import find_warehouse

        return find_warehouse(WorkspaceClient())
    except Exception as exc:
        print(f"No SQL warehouse for ai_forecast ({type(exc).__name__}); the baseline forecast will be used.")
        return ""


# COMMAND ----------

# MAGIC %md
# MAGIC ### 4 · Build and validate the configuration
# MAGIC `build_cfg()` merges profile + non-empty widgets (+ explicit overrides used by the test notebooks), then
# MAGIC `validate()` rejects anything unsafe: illegal identifiers, unbound hosts, external AI endpoints, bad dates, test
# MAGIC hooks against a real metastore. A notebook that sets `DPO_DEFER_CFG_ERRORS = True` (the installer) gets the error
# MAGIC in `CFG_ERROR` to show it nicely instead of a stack trace.

# COMMAND ----------


def build_cfg(**overrides) -> DPOConfig:
    w = dbutils.widgets.get
    widget_vals = {
        "catalog": w("catalog"),
        "schema_prefix": w("schema_prefix"),
        "source_mode": w("source_mode"),
        "lookback_days": int(w("lookback_days")) if w("lookback_days") else None,
        "end_date": w("end_date"),
        "inventory_catalogs": [c.strip() for c in w("inventory_catalogs").split(",") if c.strip()],
    }
    cfg = DPOConfig.from_profile(w("profile"), {**widget_vals, **overrides})
    cfg.table_format = "delta"
    cfg.rules_path = os.path.join(REPO_ROOT, "config", "rules.json")
    if "schema_prefix" in overrides:  # an explicit "" must win over the profile
        cfg.schema_prefix = overrides["schema_prefix"]
    cfg.forecast_warehouse_id = forecast_warehouse(cfg)
    cfg.validate()
    return cfg


CFG_ERROR = None
try:
    cfg = build_cfg()
except ValueError as e:
    cfg, CFG_ERROR = None, e
    if not globals().get("DPO_DEFER_CFG_ERRORS"):
        raise ValueError(f"Profile '{dbutils.widgets.get('profile')}' is invalid: {e}") from None

if cfg is not None:
    g = cfg.governance
    print(
        f"DPO {dpo.__version__} | workspace={HOST} | profile={cfg.profile} ({AUTO_REASON if cfg.profile == AUTO_PROFILE else 'chosen in widget'}) | "
        f"catalog={cfg.catalog} prefix='{cfg.schema_prefix}' mode={cfg.source_mode} | governance enforce={g['enforce']}"
    )
