#!/usr/bin/env python3
"""
Orchestrator: Tableau URL → enriched lineage JSON.

Steps:
    1. Fetch lineage to JSON           (tableau_fetch.metadata_lineage — Metadata API, no download)
    2. Enrich with Databricks paths    (tableau_fetch.enrich_with_paths)

Lineage is read straight from the Tableau Metadata API (GraphQL); the workbook
is never downloaded. A local .twb/.twbx can still be parsed offline via --twb,
which routes through the legacy XML extractor (tableau_fetch.twbx_lineage).

Only the final enriched JSON is kept; intermediates live in a temp dir.

Usage:
    python3 pipeline.py <tableau_url> --customer <name> [-o output.json]
    python3 pipeline.py --twb <local.twb> --customer <name> [-o output.json]
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
        help="Path to a local .twb/.twbx file. Parses it offline via the legacy "
             "XML extractor instead of calling the Metadata API.",
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
        raw_json = tmp / "lineage.json"
        enriched_json = tmp / "enriched.json"

        if args.twb:
            twb_path = Path(args.twb).resolve()
            if not twb_path.exists():
                sys.exit(f"[pipeline] --twb file not found: {twb_path}")
            run_step(
                "1/2 Extract lineage (local .twb, offline)",
                [sys.executable, "-m", "tableau_fetch.twbx_lineage", str(twb_path), "-o", str(raw_json)],
            )
        else:
            run_step(
                "1/2 Fetch lineage (Metadata API, no download)",
                [sys.executable, "-m", "tableau_fetch.metadata_lineage", args.url, "-o", str(raw_json)],
            )

        run_step(
            "2/2 Enrich with Databricks paths",
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
