# Databricks notebook source
# MAGIC %md
# MAGIC ### DPO shared bootstrap
# MAGIC Run by every other notebook with `%run ./_common`.
# MAGIC
# MAGIC **Pick a profile, nothing else.** `free_edition`, `enterprise_dev` or `enterprise_prod`
# MAGIC (files in `config/environments/`). Any widget left blank takes the profile's value;
# MAGIC fill one in only to override it for this run. Jobs deployed by the bundle pass the profile for you.

# COMMAND ----------

import os
import sys

REPO_ROOT = os.path.abspath(os.path.join(os.getcwd(), ".."))
SRC = os.path.join(REPO_ROOT, "src")
if SRC not in sys.path:
    sys.path.insert(0, SRC)

# Reload so edits in src/ are picked up without detaching the notebook
import importlib
import dpo, dpo.config, dpo.transforms, dpo.synth, dpo.pipeline, dpo.rules, dpo.recommend, dpo.validation, dpo.governance
for m in (dpo.config, dpo.transforms, dpo.synth, dpo.pipeline, dpo.rules, dpo.recommend, dpo.validation,
          dpo.governance):
    importlib.reload(m)

from dpo.config import DPOConfig, PROFILE_DIR

PROFILES = sorted(f[:-5] for f in os.listdir(PROFILE_DIR) if f.endswith(".json"))
DEFAULT_PROFILE = os.environ.get("DPO_PROFILE", "free_edition")

dbutils.widgets.dropdown("profile", DEFAULT_PROFILE if DEFAULT_PROFILE in PROFILES else PROFILES[0],
                         PROFILES, "Profile")
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
    # an explicit override that sets prefix to "" must win over the profile
    if "schema_prefix" in overrides:
        cfg.schema_prefix = overrides["schema_prefix"]
    cfg.validate()
    return cfg


cfg = build_cfg()
g = cfg.governance
print(f"DPO {dpo.__version__} | profile={cfg.profile} | catalog={cfg.catalog} prefix='{cfg.schema_prefix}' "
      f"mode={cfg.source_mode} window={cfg.lookback_days}d end={cfg.end_date or 'today'} | "
      f"governance enforce={g['enforce']} grants={g['apply_grants']}")
