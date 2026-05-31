"""
common.config
-------------
Loads the declarative operator + canonical configs and resolves ADLS Gen2 paths.

Storage layout (ADLS Gen2, one container per zone is also fine; here we use one
container `recon` with zone prefixes so lifecycle policies can differ per zone):

  abfss://recon@<storage>.dfs.core.windows.net/
      landing/operator/<operator_code>/<file_arrival_date>/...      (raw files, immutable)
      landing/oltp/<table>/<load_date>/...                          (CDC parquet)
      bronze/<table>/                                               (Delta, append, raw+meta)
      silver/<table>/                                               (Delta, conformed canonical)
      gold/<table>/                                                 (Delta, marts)
      _control/recon_period_control/                                (Delta, open/closed months)
      _control/recon_run_log/                                       (Delta, restatement audit)
"""
from __future__ import annotations
import os
import yaml

# In Databricks these come from the job's widgets / cluster env, defaulted for local runs.
STORAGE_ACCOUNT = os.environ.get("RECON_STORAGE", "tapmadrecon")
CONTAINER = os.environ.get("RECON_CONTAINER", "recon")
_ABFSS = f"abfss://{CONTAINER}@{STORAGE_ACCOUNT}.dfs.core.windows.net"

CONFIG_DIR = os.environ.get("RECON_CONFIG_DIR", "/dbfs/FileStore/recon/config")


def _abfss(*parts: str) -> str:
    return "/".join([_ABFSS, *[p.strip("/") for p in parts]])


# zone roots --------------------------------------------------------------
def landing_operator(operator_code: str, file_arrival_date: str) -> str:
    return _abfss("landing", "operator", operator_code, file_arrival_date)


def landing_oltp(table: str, load_date: str) -> str:
    return _abfss("landing", "oltp", table, load_date)


def bronze(table: str) -> str:
    return _abfss("bronze", table)


def silver(table: str) -> str:
    return _abfss("silver", table)


def gold(table: str) -> str:
    return _abfss("gold", table)


def control(table: str) -> str:
    return _abfss("_control", table)


# config loaders ----------------------------------------------------------
def load_operators(path: str | None = None) -> dict:
    path = path or os.path.join(CONFIG_DIR, "operators.yaml")
    with open(path) as f:
        cfg = yaml.safe_load(f)
    # merge defaults into each operator block
    defaults = cfg.get("defaults", {})
    for code, spec in cfg["operators"].items():
        merged = {**defaults, **spec}
        merged["operator_code"] = code
        cfg["operators"][code] = merged
    return cfg


def load_canonical(path: str | None = None) -> dict:
    path = path or os.path.join(CONFIG_DIR, "canonical_schema.yaml")
    with open(path) as f:
        return yaml.safe_load(f)


def enabled_operators(cfg: dict) -> list[str]:
    return [c for c, s in cfg["operators"].items() if s.get("enabled", False)]
