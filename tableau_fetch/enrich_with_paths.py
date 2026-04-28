"""
Post-processes tableau_lineage.json by resolving each datasource's
storage path via the Databricks Unity Catalog REST API.

Usage:
    python enrich_with_paths.py [input.json] [output.json]

Defaults: input=tableau_lineage.json, output=tableau_lineage_enriched.json
"""

import json
import os
import re
import sys
from pathlib import Path
from typing import Dict, Optional

import requests
from dotenv import load_dotenv

for _env_path in [Path.cwd() / ".env", Path(__file__).resolve().parent.parent / ".env"]:
    if _env_path.exists():
        load_dotenv(_env_path)
        break

DATABRICKS_HOST = os.getenv("DATABRICKS_HOST", "").rstrip("/")
DATABRICKS_PAT = os.getenv("DATABRICKS_PAT_SECRET") or os.getenv("DATABRICKS_PAT")
DATABRICKS_WAREHOUSE_ID = os.getenv("DATABRICKS_WAREHOUSE_ID")


def pick_warehouse() -> str:
    """Auto-select a warehouse: prefer RUNNING (no start latency), else first available."""
    resp = requests.get(
        f"{DATABRICKS_HOST}/api/2.0/sql/warehouses",
        headers={"Authorization": f"Bearer {DATABRICKS_PAT}"},
        timeout=30,
    )
    resp.raise_for_status()
    warehouses = resp.json().get("warehouses", [])
    if not warehouses:
        sys.exit("ERROR: no SQL warehouses visible to this PAT")

    running = [w for w in warehouses if w.get("state") == "RUNNING"]
    chosen = running[0] if running else warehouses[0]
    print(f"Auto-selected warehouse: {chosen['name']} ({chosen['id']}, state={chosen['state']})")
    return chosen["id"]

# Matches the "(catalog.schema.table)" portion in a tableau_datasource_name like:
#   "dcrpl_order_report (hive_metastore.fivebelow.dcrpl_order_report) (fivebelow)"
FULL_NAME_RE = re.compile(r"\(([a-zA-Z0-9_]+\.[a-zA-Z0-9_]+\.[a-zA-Z0-9_]+)\)")


def extract_full_name(datasource_name: str):
    match = FULL_NAME_RE.search(datasource_name or "")
    return match.group(1) if match else None


def fetch_storage_path(full_name: str) -> Optional[str]:
    """Run DESCRIBE DETAIL via the SQL Statement Execution API to get the table's location."""
    resp = requests.post(
        f"{DATABRICKS_HOST}/api/2.0/sql/statements",
        headers={"Authorization": f"Bearer {DATABRICKS_PAT}"},
        json={
            "warehouse_id": DATABRICKS_WAREHOUSE_ID,
            "statement": f"DESCRIBE DETAIL {full_name}",
            "wait_timeout": "30s",
            "on_wait_timeout": "CANCEL",
        },
        timeout=60,
    )
    if not resp.ok:
        raise RuntimeError(f"{resp.status_code} {resp.reason} for {full_name}\n{resp.text}")

    body = resp.json()
    state = (body.get("status") or {}).get("state")
    if state != "SUCCEEDED":
        err = (body.get("status") or {}).get("error") or {}
        print(f"  [{state}] {err.get('message', '')}")
        return None

    result = body.get("result") or {}
    columns = [c["name"] for c in (body.get("manifest") or {}).get("schema", {}).get("columns", [])]
    rows = result.get("data_array") or []
    if not rows or "location" not in columns:
        return None
    return rows[0][columns.index("location")]


def main(in_path: str, out_path: str):
    global DATABRICKS_WAREHOUSE_ID

    if not DATABRICKS_HOST:
        sys.exit("ERROR: DATABRICKS_HOST missing from .env")
    if not DATABRICKS_PAT:
        sys.exit("ERROR: DATABRICKS_PAT_SECRET (or DATABRICKS_PAT) missing from .env")
    if not DATABRICKS_WAREHOUSE_ID:
        DATABRICKS_WAREHOUSE_ID = pick_warehouse()

    with open(in_path) as f:
        data = json.load(f)

    cache: Dict[str, Optional[str]] = {}

    for entry in data:
        ds = entry.get("datasource") or {}
        name = ds.get("tableau_datasource_name")
        full_name = extract_full_name(name)
        if not full_name:
            ds["storage_path"] = None
            continue

        if full_name not in cache:
            print(f"Resolving {full_name}...")
            try:
                cache[full_name] = fetch_storage_path(full_name)
            except Exception as e:
                print(f"  failed: {e}")
                cache[full_name] = None

        ds["storage_path"] = cache[full_name]

    with open(out_path, "w") as f:
        json.dump(data, f, indent=2)

    resolved = sum(1 for v in cache.values() if v)
    print(f"\nDone. Resolved {resolved}/{len(cache)} unique tables → {out_path}")


if __name__ == "__main__":
    in_path = sys.argv[1] if len(sys.argv) > 1 else "tableau_lineage.json"
    out_path = sys.argv[2] if len(sys.argv) > 2 else "tableau_lineage_enriched.json"
    main(in_path, out_path)
