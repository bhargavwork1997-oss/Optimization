# Databricks notebook source
# MAGIC %md
# MAGIC ### DPO shared bootstrap
# MAGIC Run by every other notebook with `%run ./_common`.
# MAGIC
# MAGIC **The profile is picked automatically**: the profile in `config/environments/` whose `workspace_hosts`
# MAGIC lists this workspace wins (job parameter or the `DPO_PROFILE` env var override it). You only touch the
# MAGIC `profile` widget to deliberately run another profile. Blank widgets take the profile's values.

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
    raise RuntimeError(f"Cannot find the DPO project (src/dpo and config/) from {candidates}. "
                       "Run notebooks from the repo's notebooks/ folder or a bundle deployment.")


REPO_ROOT = _project_root()
SRC = os.path.join(REPO_ROOT, "src")
if SRC not in sys.path:
    sys.path.insert(0, SRC)

# Reload so edits in src/ are picked up without detaching the notebook
import importlib
import dpo, dpo.config, dpo.transforms, dpo.synth, dpo.pipeline, dpo.rules, dpo.recommend, dpo.validation, \
    dpo.governance, dpo.installer
for m in (dpo.config, dpo.transforms, dpo.synth, dpo.pipeline, dpo.rules, dpo.recommend, dpo.validation,
          dpo.governance, dpo.installer):
    importlib.reload(m)

from dpo.config import DPOConfig, PROFILE_DIR, detect_profile


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

dbutils.widgets.dropdown("profile", AUTO_PROFILE, PROFILES, "Profile (auto-detected)")
dbutils.widgets.text("catalog", "", "Catalog (blank = profile)")
dbutils.widgets.text("schema_prefix", "", "Schema prefix (blank = profile)")
dbutils.widgets.dropdown("source_mode", "", ["", "synthetic", "system"], "Source mode (blank = profile)")
dbutils.widgets.text("lookback_days", "", "Look-back days (blank = profile)")
dbutils.widgets.text("end_date", "", "End date YYYY-MM-DD (blank = today)")
dbutils.widgets.text("inventory_catalogs", "", "Catalogs to scan for layout (comma list)")


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
    if "schema_prefix" in overrides:            # an explicit "" must win over the profile
        cfg.schema_prefix = overrides["schema_prefix"]
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
    print(f"DPO {dpo.__version__} | workspace={HOST} | profile={cfg.profile} ({AUTO_REASON if cfg.profile == AUTO_PROFILE else 'chosen in widget'}) | "
          f"catalog={cfg.catalog} prefix='{cfg.schema_prefix}' mode={cfg.source_mode} | governance enforce={g['enforce']}")
