#!/usr/bin/env python3
"""
Fetches Tableau report metadata, resolves the underlying delta table path,
and writes metadata.json to:

    ~/Desktop/tableau_fetch/{report}/{sheet}/metadata.json
"""

import argparse
import json
import re
import sys
from datetime import datetime, timezone
from pathlib import Path

from tableau_fetch.tableau import fetch_sheet_metadata, FieldInfo
from tableau_fetch.databricks import resolve_delta_table


def _derive_repo_slug(workbook: str) -> str:
    slug = re.sub(r"[^a-z0-9]+", "-", workbook.lower()).strip("-")
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
        "original_column": f.name if f.name != f.display_name else None,
        "is_calculated": False,
    }


def _log(msg: str) -> None:
    print(f"  {msg}", flush=True)


def run(url: str, sheet: str, repo_override: str | None = None) -> None:
    # Step 1: Tableau metadata
    _log("Connecting to Tableau Cloud...")
    sheet_meta = fetch_sheet_metadata(url, sheet)
    ds = sheet_meta.datasource
    n_calculated = sum(1 for f in sheet_meta.fields if f.field_type == "calculated")
    n_direct = len(sheet_meta.fields) - n_calculated
    _log(f"Retrieved {len(sheet_meta.fields)} fields ({n_direct} direct, {n_calculated} calculated)")

    # Step 2: Databricks delta table path
    _log("Resolving delta table via Databricks Unity Catalog...")
    delta = resolve_delta_table(ds.database, ds.schema, ds.table)
    _log(f"Resolved delta table: {delta.full_name}")
    _log(f"Table path: {delta.storage_path}")

    # Step 3: Assemble output
    repo_name = repo_override or _derive_repo_slug(sheet_meta.workbook)
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
    }

    # Step 4: Write output
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
    parser.add_argument("--url", required=True, help="Tableau Cloud report URL")
    parser.add_argument("--sheet", required=True, help="Sheet / view name")
    parser.add_argument(
        "--repo",
        default=None,
        help="Override the derived customer pipeline repo name (default: auto-derived from workbook name)",
    )
    args = parser.parse_args()

    try:
        run(url=args.url, sheet=args.sheet, repo_override=args.repo)
    except (ValueError, PermissionError, EnvironmentError) as exc:
        print(f"\nError: {exc}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
