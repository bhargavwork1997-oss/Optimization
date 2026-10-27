"""Asset Bundle compliance checks (no workspace needed).

CI also exports the CLI's own schema (`databricks bundle schema`) and points DPO_BUNDLE_SCHEMA at it,
so the YAML is validated against the exact schema the CLI uses.
"""
import copy
import fnmatch
import json
import os
import re

import pytest
import yaml

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
BUNDLE = os.path.join(ROOT, "databricks.yml")


def _load(path):
    with open(path) as fh:
        return yaml.safe_load(fh)


def _files():
    root = _load(BUNDLE)
    files = [BUNDLE]
    for pattern in root.get("include", []):
        d = os.path.join(ROOT, os.path.dirname(pattern))
        files += sorted(os.path.join(d, f) for f in os.listdir(d) if fnmatch.fnmatch(f, os.path.basename(pattern)))
    return root, files


def _merge(a, b):
    out = copy.deepcopy(a)
    for k, v in (b or {}).items():
        out[k] = _merge(out[k], v) if isinstance(v, dict) and isinstance(out.get(k), dict) else copy.deepcopy(v)
    return out


def _jobs_for(target):
    """Global jobs merged with the target's overrides, each tagged with the file that defines its tasks."""
    root, files = _files()
    jobs, origin = {}, {}
    for f in files:
        for k, j in ((_load(f).get("resources") or {}).get("jobs") or {}).items():
            jobs[k] = _merge(jobs.get(k, {}), j)
            if "tasks" in j:
                origin[k] = f
    for k, j in ((root["targets"][target].get("resources") or {}).get("jobs") or {}).items():
        jobs[k] = _merge(jobs.get(k, {}), j)
        if "tasks" in j:
            origin[k] = BUNDLE
    top = root["targets"][target].get("permissions", root.get("permissions", []))
    for j in jobs.values():                     # the CLI applies bundle-level permissions to every job
        j["permissions"] = list(top) + list(j.get("permissions", []))
    return jobs, origin


TARGETS = list(_load(BUNDLE)["targets"])


def test_no_yaml_anchors_or_aliases():
    for f in _files()[1]:
        text = open(f).read()
        code = "\n".join(l.split("#", 1)[0] for l in text.splitlines())
        assert not re.search(r":\s*&\w|:\s*\*\w|^\s*-\s*\*\w", code, re.M), f"anchor/alias in {f}"


@pytest.mark.parametrize("target", TARGETS)
def test_every_notebook_path_exists(target):
    jobs, origin = _jobs_for(target)
    for k, j in jobs.items():
        for t in j["tasks"]:
            p = os.path.normpath(os.path.join(os.path.dirname(origin[k]), t["notebook_task"]["notebook_path"]))
            assert os.path.isfile(p), f"{target}/{k}/{t['task_key']}: {p}"
            assert open(p).readline().startswith("# Databricks notebook source"), p


@pytest.mark.parametrize("target", TARGETS)
def test_jobs_are_parameterised_tagged_and_serverless(target):
    jobs, _ = _jobs_for(target)
    for k, j in jobs.items():
        assert any(p["name"] == "profile" for p in j.get("parameters", [])), f"{k} lacks profile parameter"
        assert j.get("tags", {}).get("app") == "dpo-accelerator" and "dpo_profile" in j["tags"], k
        assert "${var.profile}" in j["name"], f"{k}: name must carry the profile"
        assert not j.get("job_clusters") and not any("new_cluster" in t or "existing_cluster_id" in t
                                                     for t in j["tasks"]), f"{k} must stay serverless"


def test_each_target_points_at_an_existing_profile():
    root = _load(BUNDLE)
    default = root["variables"]["profile"]["default"]
    for t, cfg in root["targets"].items():
        prof = (cfg.get("variables") or {}).get("profile", default)
        assert os.path.isfile(os.path.join(ROOT, "config", "environments", f"{prof}.json")), f"{t}: {prof}"


def test_production_target_follows_bundle_production_rules():
    prod = _load(BUNDLE)["targets"]["enterprise_prod"]
    assert prod["mode"] == "production"
    assert prod["run_as"]["service_principal_name"] == "${var.service_principal}"
    assert not prod["workspace"]["root_path"].startswith("/Workspace/Shared"), "Shared is writable by all users"
    for v in ("service_principal", "admin_service_principal"):
        assert "lookup" in prod["variables"][v], f"{v} must be resolved, not left empty"
    # bundle-wide permissions: admins manage, run-as SPs read the code; never operators or analysts
    for p in prod.get("permissions", []):
        assert (p["level"], p.get("group_name")) == ("CAN_MANAGE", "${var.admins_group}") or (
            p["level"] == "CAN_VIEW" and p.get("service_principal_name") in (
                "${var.service_principal}", "${var.admin_service_principal}")), p


@pytest.mark.parametrize("target", ["enterprise_dev", "enterprise_prod"])
def test_admin_identity_jobs_are_admins_only(target):
    jobs, _ = _jobs_for(target)
    for k in ("dpo_install", "dpo_governance"):
        groups = {(p["level"], p.get("group_name")) for p in jobs[k].get("permissions", []) if p.get("group_name")}
        assert groups == {("CAN_MANAGE", "${var.admins_group}")}, f"{target}/{k}: {groups}"
    pipe = {p.get("group_name"): p["level"] for p in jobs["dpo_pipeline"]["permissions"]}
    assert pipe["${var.operators_group}"] == "CAN_MANAGE_RUN"
    if target == "enterprise_prod":
        assert jobs["dpo_install"]["run_as"]["service_principal_name"] == "${var.admin_service_principal}"
        assert jobs["dpo_governance"]["run_as"]["service_principal_name"] == "${var.admin_service_principal}"
        assert "run_as" not in jobs["dpo_pipeline"]          # inherits the operator SP


def test_bundle_install_job_never_creates_jobs_itself():
    jobs, _ = _jobs_for("enterprise_prod")
    params = jobs["dpo_install"]["tasks"][0]["notebook_task"]["base_parameters"]
    assert params["schedule"] == "none"
    assert params["pipeline_run_as"] == "${var.service_principal}"


def test_installer_and_bundle_agree_on_job_names():
    from dpo.config import DPOConfig
    from dpo.installer import job_specs
    for target in ("enterprise_prod",):
        jobs, _ = _jobs_for(target)
        cfg = DPOConfig.from_profile("enterprise_prod")
        bundle_names = {j["name"].replace("${var.profile}", cfg.profile) for j in jobs.values()}
        assert {s["name"] for s in job_specs(cfg, "/x")} <= bundle_names


def test_sync_keeps_runtime_files():
    excl = _load(BUNDLE)["sync"]["exclude"]
    for needed in ("src/dpo/pipeline.py", "config/rules.json", "config/environments/enterprise_prod.json",
                   "tests/golden/small_seed42.json", "notebooks/_common.py"):
        assert not any(fnmatch.fnmatch(needed, e) for e in excl), needed


def test_against_cli_schema():
    path = os.environ.get("DPO_BUNDLE_SCHEMA")
    if not path:
        pytest.skip("set DPO_BUNDLE_SCHEMA to the output of `databricks bundle schema` (done in CI)")
    jsonschema = pytest.importorskip("jsonschema")
    raw = open(path).read().replace("\\\\p{L}", "[^\\\\W\\\\d_]").replace("\\\\p{N}", "\\\\d")  # Go -> Python regex
    v = jsonschema.Draft7Validator(json.loads(raw))
    for f in _files()[1]:
        errs = [jsonschema.exceptions.best_match([e]) for e in v.iter_errors(_load(f))]
        assert not errs, [(f, "/".join(map(str, e.absolute_path)), e.message[:200]) for e in errs]
