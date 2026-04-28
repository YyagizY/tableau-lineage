#!/usr/bin/env python3
"""
Orchestrator: Tableau URL → enriched lineage JSON.

Steps:
    1. Download the workbook as .twb  (tableau_fetch.download_workbook)
    2. Extract lineage to JSON        (tableau_fetch.twbx_lineage)
    3. Enrich with Databricks paths   (tableau_fetch.enrich_with_paths)

Only the final enriched JSON is kept; intermediates live in a temp dir.

Usage:
    python3 pipeline.py <tableau_url> [-o output.json]
"""

import argparse
import json
import re
import subprocess
import sys
import tempfile
from pathlib import Path
from typing import List


def run_step(label: str, cmd: List[str]) -> None:
    print(f"\n=== {label} ===")
    result = subprocess.run(cmd)
    if result.returncode != 0:
        sys.exit(f"\n[pipeline] step failed: {label}")


def build_repo_name(customer: str) -> str:
    slug = re.sub(r"[^a-z0-9]+", "-", customer.lower()).strip("-")
    if not slug:
        sys.exit(f"Invalid customer name: {customer!r}")
    return f"customer-pipeline-{slug}"


def main() -> None:
    parser = argparse.ArgumentParser(description="End-to-end Tableau → Databricks lineage.")
    parser.add_argument("url", nargs="?", help="Tableau Cloud workbook URL (omit if --twb is given)")
    parser.add_argument("--customer", required=True, help="Customer name, e.g. 'fivebelow'")
    parser.add_argument(
        "--twb",
        help="Path to a local .twb/.twbx file. If provided, skips step 1 (download).",
    )
    parser.add_argument(
        "-o", "--output",
        default="lineage_enriched.json",
        help="Final output JSON (default: lineage_enriched.json)",
    )
    args = parser.parse_args()

    if not args.url and not args.twb:
        parser.error("either a Tableau URL or --twb <path> is required")

    final_output = Path(args.output).resolve()
    repo_name = build_repo_name(args.customer)

    with tempfile.TemporaryDirectory() as tmpdir:
        tmp = Path(tmpdir)
        twb_path = tmp / "workbook.twb"
        raw_json = tmp / "lineage.json"
        enriched_json = tmp / "enriched.json"

        if args.twb:
            twb_path = Path(args.twb).resolve()
            if not twb_path.exists():
                sys.exit(f"[pipeline] --twb file not found: {twb_path}")
            print(f"\n=== 1/3 Skipped (using local file: {twb_path.name}) ===")
        else:
            run_step(
                "1/3 Download workbook",
                [sys.executable, "-m", "tableau_fetch.download_workbook", args.url, str(twb_path)],
            )

        run_step(
            "2/3 Extract lineage",
            [sys.executable, "-m", "tableau_fetch.twbx_lineage", str(twb_path), "-o", str(raw_json)],
        )

        run_step(
            "3/3 Enrich with Databricks paths",
            [sys.executable, "-m", "tableau_fetch.enrich_with_paths", str(raw_json), str(enriched_json)],
        )

        with open(enriched_json) as f:
            sheets = json.load(f)

        final = {"customer-name": repo_name, "sheets": sheets}
        with open(final_output, "w") as f:
            json.dump(final, f, indent=2)

    print(f"\nDone. Final output: {final_output}")


if __name__ == "__main__":
    main()
