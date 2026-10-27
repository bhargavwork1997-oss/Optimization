"""Central configuration for the DPO accelerator.

One config object drives every stage. Environment differences live in
profiles (config/environments/*.json), so moving from Free Edition to an
enterprise account is a profile switch, not a code change:

  free_edition     catalog=workspace, prefix dpo_, governance not enforced
  enterprise_dev   catalog=dpo_dev,   system tables, governance enforced
  enterprise_prod  catalog=dpo,       system tables, governance enforced + drift revoke
  local (tests)    catalog=None, parquet tables
"""
from __future__ import annotations

import copy
import json
import os
import re
from dataclasses import dataclass, field, fields
from typing import Optional

# Logical source name -> Databricks system table.  table_inventory has no single
# system table; it is collected from information_schema + DESCRIBE DETAIL.
SYSTEM_SOURCES = {
    "billing_usage": "system.billing.usage",
    "list_prices": "system.billing.list_prices",
    "jobs": "system.lakeflow.jobs",
    "job_run_timeline": "system.lakeflow.job_run_timeline",
    "job_task_run_timeline": "system.lakeflow.job_task_run_timeline",
    "query_history": "system.query.history",
    "clusters": "system.compute.clusters",
    "node_timeline": "system.compute.node_timeline",
    "table_inventory": None,
}

# Data layers are written by the pipeline (operators).
DATA_LAYERS = ("sim", "bronze", "silver", "gold", "ops")
# reporting = secured views for viewers (admin-owned); security = access map + salt (admin-only)
LAYERS = DATA_LAYERS + ("reporting", "security")

_IDENT = re.compile(r"^[A-Za-z0-9_]+$")
_GROUP = re.compile(r"^[A-Za-z0-9_.@ +-]+$")
_ENDPOINT = re.compile(r"^[A-Za-z0-9_.-]+$")
_CC_VALUE = re.compile(r"^[A-Za-z0-9_.()@ -]+$")
_SKU_LIKE = re.compile(r"^[A-Za-z0-9_%]+$")
_LOCATION = re.compile(r"^(abfss|s3|gs|r2)://[A-Za-z0-9@._/:-]+$")
_HOST = re.compile(r"^[a-z0-9.-]+$")


def normalize_host(host: Optional[str]) -> Optional[str]:
    """'https://adb-1.2.azuredatabricks.net/?o=1' -> 'adb-1.2.azuredatabricks.net'"""
    if not host:
        return None
    h = host.strip().lower()
    h = re.sub(r"^https?://", "", h)
    return h.split("/")[0].split("?")[0] or None


def detect_profile(host: Optional[str], profile_dir: Optional[str] = None,
                   default: str = "free_edition") -> tuple[str, str]:
    """Pick the profile whose `workspace_hosts` contains this workspace. Returns (profile, reason)."""
    profile_dir = profile_dir or PROFILE_DIR
    env = os.environ.get("DPO_PROFILE")
    if env:
        return env, "DPO_PROFILE environment variable"
    h = normalize_host(host)
    if h:
        for f in sorted(os.listdir(profile_dir)):
            if f.endswith(".json"):
                with open(os.path.join(profile_dir, f)) as fh:
                    hosts = [normalize_host(x) for x in json.load(fh).get("workspace_hosts", [])]
                if h in hosts:
                    return f[:-5], f"workspace {h} is listed in {f}"
    return default, f"no profile lists workspace {h or '(unknown)'}; defaulting to {default}"

_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
_DEFAULT_RULES = os.path.join(_ROOT, "config", "rules.json")
PROFILE_DIR = os.path.join(_ROOT, "config", "environments")


def default_governance() -> dict:
    return {
        "enforce": False,              # authorization check + row/identity filtering in reporting views
        "apply_grants": False,         # run GRANT / OWNER / TAG statements each run
        "manage_ownership": False,     # ALTER ... OWNER TO admins
        "grant_system_access": False,  # GRANT on system.* (needs metastore admin)
        "revoke_drift": False,         # REVOKE privileges from principals outside the role model
        "fail_on_error": False,        # a failed governance statement fails the run
        "groups": {
            "admins": "dpo-admins",        # own the catalog, read everything, manage grants
            "operators": "dpo-operators",  # run the pipeline (job service principal belongs here)
            "analysts": "dpo-analysts",    # read silver, gold, ops health; see identities
            "viewers": "dpo-viewers",      # read reporting views only; filtered by cost centre
        },
        "authorized_principals": [],       # extra users/SPs allowed to run (prefer groups)
        "cost_center_groups": {},          # {"finance-leads": ["cc-finance"]} -> row visibility
        "mask_identities_for_viewers": True,
        "classification": "confidential",
        "membership_function": "is_account_group_member",
        "operators_can_create_schemas": False,  # dev only: lets test suites create dpo_test_* schemas
        "retain_raw_sql": False,           # False: bronze keeps literal- and comment-stripped SQL only
        "ai_allow_external": False,        # False: ai_query only to Databricks-hosted endpoints
        "simulate_membership": None,       # local tests only (refused when a catalog is set)
        "simulate_user": None,             # local tests only (refused when a catalog is set)
    }


@dataclass
class DPOConfig:
    profile: str = "custom"
    catalog: Optional[str] = "workspace"      # None = local Spark (no Unity Catalog)
    schema_prefix: str = "dpo_"               # workspace.dpo_bronze, workspace.dpo_silver ...
    managed_location: Optional[str] = None    # enterprise: abfss://... for CREATE CATALOG
    source_mode: str = "synthetic"            # "synthetic" | "system"
    lookback_days: int = 30
    end_date: Optional[str] = None            # "YYYY-MM-DD"; None = today (UTC)
    table_format: str = "delta"               # "parquet" for local tests
    rules_path: str = _DEFAULT_RULES
    weights: dict = field(default_factory=lambda: {"V": 0.35, "C": 0.25, "F": 0.20, "R": 0.20})
    cost_center_tag: str = "cost_center"
    currency: str = "USD"
    price_multiplier: float = 1.0             # contract discount: 0.8 = 20% off list
    contract_prices: dict = field(default_factory=dict)  # {"%ALL_PURPOSE%": 0.40} USD/DBU, SKU LIKE pattern
    workspace_ids: list = field(default_factory=list)  # scope account-wide system tables; [] = all
    workspace_hosts: list = field(default_factory=list)  # workspaces this profile may run in (auto-detect + guard)
    ai_enabled: bool = False
    ai_endpoint: str = "databricks-meta-llama-3-3-70b-instruct"
    ai_max_rows: int = 25
    inventory_catalogs: list = field(default_factory=list)  # system mode: catalogs to scan for table layout
    inventory_max_tables: int = 500
    log_retention_days: int = 365
    governance: dict = field(default_factory=default_governance)

    # ---- profiles -------------------------------------------------------------
    @classmethod
    def from_profile(cls, name: str, overrides: Optional[dict] = None,
                     profile_dir: str = PROFILE_DIR) -> "DPOConfig":
        """Load config/environments/<name>.json, then apply non-empty overrides."""
        path = os.path.join(profile_dir, f"{name}.json")
        if not os.path.exists(path):
            avail = sorted(f[:-5] for f in os.listdir(profile_dir) if f.endswith(".json"))
            raise ValueError(f"unknown profile '{name}'. Available: {avail}")
        with open(path) as fh:
            data = json.load(fh)
        data["profile"] = name
        for k, v in (overrides or {}).items():
            if v not in (None, "", []):
                data[k] = v
        return cls.from_dict(data)

    @classmethod
    def from_dict(cls, data: dict) -> "DPOConfig":
        known = {f.name for f in fields(cls)}
        unknown = set(data) - known - {"description"}
        if unknown:
            raise ValueError(f"unknown config keys: {sorted(unknown)}")
        data = {k: v for k, v in data.items() if k in known}
        gov = default_governance()
        for k, v in (data.pop("governance", None) or {}).items():
            if k == "groups":
                gov["groups"] = {**gov["groups"], **v}
            else:
                gov[k] = v
        return cls(governance=gov, **data)

    def to_dict(self) -> dict:
        return {f.name: copy.deepcopy(getattr(self, f.name)) for f in fields(self)}

    # ---- naming -------------------------------------------------------------
    def schema(self, layer: str) -> str:
        name = f"{self.schema_prefix}{layer}"
        return f"{self.catalog}.{name}" if self.catalog else name

    def t(self, layer: str, name: str) -> str:
        return f"{self.schema(layer)}.{name}"

    def source(self, name: str) -> Optional[str]:
        if self.source_mode == "system":
            return SYSTEM_SOURCES[name]
        return self.t("sim", name)

    def group(self, role: str) -> str:
        return self.governance["groups"][role]

    # ---- rules ----------------------------------------------------------------
    def rules(self) -> dict:
        with open(os.path.abspath(self.rules_path)) as fh:
            data = json.load(fh)
        return {r["rule_id"]: r for r in data["rules"]}

    def validate(self) -> None:
        if self.source_mode not in ("synthetic", "system"):
            raise ValueError(f"source_mode must be synthetic|system, got {self.source_mode}")
        total = round(sum(self.weights.values()), 6)
        if total != 1.0:
            raise ValueError(f"score weights must sum to 1.0, got {total}")
        if self.lookback_days < 7:
            raise ValueError("lookback_days must be >= 7 (baselines need history)")
        if not 0 < float(self.price_multiplier) <= 1.5:
            raise ValueError("price_multiplier must be in (0, 1.5]")
        g = self.governance
        missing = {"admins", "operators", "analysts", "viewers"} - set(g["groups"])
        if missing:
            raise ValueError(f"governance.groups missing roles: {sorted(missing)}")
        if g["enforce"] and not self.catalog and g.get("simulate_membership") is None:
            raise ValueError("governance.enforce needs Unity Catalog (catalog must be set)")
        if g["revoke_drift"] and not g["apply_grants"]:
            raise ValueError("revoke_drift requires apply_grants")
        # test hooks must never bypass authorization against a real metastore
        if self.catalog and (g.get("simulate_membership") is not None or g.get("simulate_user")):
            raise ValueError("simulate_membership / simulate_user are test-only and refused when catalog is set")
        # everything below is interpolated into SQL: allow-list the characters
        for label, val in (("catalog", self.catalog), ("schema_prefix", self.schema_prefix or "x"),
                           ("cost_center_tag", self.cost_center_tag)):
            if val is not None and not _IDENT.match(val):
                raise ValueError(f"illegal characters in {label}: {val!r}")
        for c in self.inventory_catalogs:
            if not _IDENT.match(c):
                raise ValueError(f"illegal characters in inventory catalog: {c!r}")
        for h in self.workspace_hosts:
            if not normalize_host(h) or not _HOST.match(normalize_host(h)):
                raise ValueError(f"bad workspace host: {h!r}")
        if self.managed_location and not _LOCATION.match(self.managed_location):
            raise ValueError(f"managed_location must be a cloud URI without quotes: {self.managed_location!r}")
        for name in list(g["groups"].values()) + list(g["cost_center_groups"]) + list(g["authorized_principals"]):
            if not _GROUP.match(name):
                raise ValueError(f"illegal character in group name: {name}")
        for centres in g["cost_center_groups"].values():
            for c in centres:
                if not _CC_VALUE.match(c):
                    raise ValueError(f"illegal character in cost centre: {c!r}")
        for pat, price in self.contract_prices.items():
            if not _SKU_LIKE.match(pat) or not 0 < float(price) < 100:
                raise ValueError(f"bad contract price {pat!r}: {price!r}")
        if not _ENDPOINT.match(self.ai_endpoint):
            raise ValueError(f"illegal ai_endpoint: {self.ai_endpoint!r}")
        if self.ai_enabled and not g["ai_allow_external"] and not self.ai_endpoint.startswith("databricks-"):
            raise ValueError("ai_endpoint must be a Databricks-hosted model (databricks-*) unless "
                             "governance.ai_allow_external is approved")
        if g["membership_function"] not in ("is_account_group_member", "is_member"):
            raise ValueError("membership_function must be is_account_group_member or is_member")
