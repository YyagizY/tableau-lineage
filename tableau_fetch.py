#!/usr/bin/env python3
"""
Fetches Tableau report metadata, resolves the underlying delta table path,
and writes metadata.json to:

    ~/Desktop/tableau_fetch/{report}/{sheet}/metadata.json
"""

import argparse
import json
import os
import re
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path

# Source ~/.zshrc so env vars set there are available regardless of how this is invoked
for _line in subprocess.check_output(["zsh", "-c", "source ~/.zshrc && env"], text=True).splitlines():
    if "=" in _line:
        _k, _, _v = _line.partition("=")
        os.environ.setdefault(_k, _v)

from tableau_fetch.tableau import fetch_sheet_metadata, FieldInfo
from tableau_fetch.databricks import resolve_delta_table


def _build_repo_name(client: str) -> str:
    slug = re.sub(r"[^a-z0-9]+", "-", client.lower()).strip("-")
    if not slug:
        raise ValueError(f"Invalid client name: {client!r} — produces empty repo slug")
    return f"customer-pipeline-{slug}"


def _map_column(f: FieldInfo) -> dict:
    if f.field_type == "calculated":
        return {
            "displayed_name": f.display_name,
            "original_column": None,
            "is_calculated": True,
            "formula": f.formula,
            "referenced_columns": re.findall(r"\[([^\]]+)\]", f.formula or ""),
        }
    return {
        "displayed_name": f.display_name,
        "original_column": f.name,
        "is_calculated": False,
    }


def _log(msg: str) -> None:
    print(f"  {msg}", flush=True)


def run(url: str, client: str) -> None:
    # Step 1: Tableau metadata
    _log("Connecting to Tableau Cloud...")
    sheet_meta = fetch_sheet_metadata(url)
    ds = sheet_meta.datasource
    n_calculated = sum(1 for f in sheet_meta.fields if f.field_type == "calculated")
    n_direct = len(sheet_meta.fields) - n_calculated
    _log(f"Retrieved {len(sheet_meta.fields)} fields ({n_direct} direct, {n_calculated} calculated)")

    # Step 2: Databricks delta table path
    _log("Resolving delta table via Databricks Unity Catalog...")
    delta = resolve_delta_table(ds.database, ds.schema, ds.table)
    _log(f"Resolved delta table: {delta.full_name}")
    _log(f"Table path: {delta.storage_path}")

    # Step 3: Resolve repository
    repo_name = _build_repo_name(client)
    repo_path = Path.home() / "Desktop" / "repos" / "clients" / repo_name
    _log(f"Looking for pipeline repo at {repo_path}...")
    if not repo_path.exists():
        raise FileNotFoundError(f"Repository not found at {repo_path}")
    if not os.access(repo_path, os.R_OK):
        raise PermissionError(f"Permission denied reading repository at {repo_path}")
    _log(f"Found pipeline repo: {repo_name}")

    # Step 4: Assemble output
    output = {
        "report": sheet_meta.workbook,
        "sheet": sheet_meta.sheet,
        "extracted_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "delta_table": {
            "full_name": delta.full_name,
            "storage_path": delta.storage_path,
        },
        "columns": [_map_column(f) for f in sheet_meta.fields],
        "repo_name": repo_name,
        "repo_path": str(repo_path),
    }

    # Step 5: Write output
    out_dir = Path.home() / "Desktop" / "tableau_fetch" / sheet_meta.workbook / sheet_meta.sheet
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / "metadata.json"
    out_path.write_text(json.dumps(output, indent=2))

    _log(f"Written to {out_path}")
    print()
    print(f"  Output path : {out_path}")
    print(f"  Repo name   : {repo_name}")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Fetch Tableau report metadata and resolve the underlying delta table."
    )
    parser.add_argument("--url", required=True, help="Tableau Cloud report URL (sheet name is derived from the URL)")
    parser.add_argument(
        "--client", required=True,
        help="Client name (used to locate the pipeline repo: customer-pipeline-{client})",
    )
    args = parser.parse_args()

    try:
        run(url=args.url, client=args.client)
    except (ValueError, PermissionError, EnvironmentError, FileNotFoundError) as exc:
        print(f"\nError: {exc}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
