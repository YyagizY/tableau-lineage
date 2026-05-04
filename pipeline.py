#!/usr/bin/env python3
"""
Orchestrator: Tableau URL → enriched lineage JSON.

Steps:
    1. Download the workbook as .twb  (tableau_fetch.download_workbook)
    2. Extract lineage to JSON        (tableau_fetch.twbx_lineage)
    3. Enrich with Databricks paths   (tableau_fetch.enrich_with_paths)

Only the final enriched JSON is kept; intermediates live in a temp dir.
Output: ~/Desktop/tableau_lineage/<customer>/<report>/lineage_enriched.json

Usage:
    python3 pipeline.py <tableau_url> --customer <name>
"""

import argparse
import json
import re
import subprocess
import sys
import tempfile
from pathlib import Path
from tableau_fetch.download_workbook import parse_tableau_url


def run_step(label: str, cmd: list[str]) -> None:
    print(f"\n=== {label} ===")
    result = subprocess.run(cmd)
    if result.returncode != 0:
        sys.exit(f"\n[pipeline] step failed: {label}")


def slugify(name: str) -> str:
    slug = re.sub(r"[^a-z0-9]+", "-", name.lower()).strip("-")
    if not slug:
        sys.exit(f"Invalid customer name: {name!r}")
    return slug


def build_repo_name(customer: str) -> str:
    return f"customer-pipeline-{slugify(customer)}"


def main() -> None:
    parser = argparse.ArgumentParser(description="End-to-end Tableau → Databricks lineage.")
    parser.add_argument("url", help="Tableau Cloud workbook URL")
    parser.add_argument("--customer", required=True, help="Customer name, e.g. 'fivebelow'")
    args = parser.parse_args()

    _, _, slug = parse_tableau_url(args.url)
    client_slug = slugify(args.customer)
    repo_name = build_repo_name(args.customer)
    output_dir = Path.home() / "Desktop" / "tableau_lineage" / client_slug / slug
    output_dir.mkdir(parents=True, exist_ok=True)
    final_output = output_dir / "lineage_enriched.json"

    with tempfile.TemporaryDirectory() as tmpdir:
        tmp = Path(tmpdir)
        # Name the temp .twb after the workbook slug so the `workbook`
        # field in the lineage output reflects the real name (twbx.py
        # derives it from the file stem).
        twb_path = tmp / f"{slug}.twb"
        raw_json = tmp / "lineage.json"
        enriched_json = tmp / "enriched.json"

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

    print(f"\nDone. You can find the JSON at: {final_output}")


if __name__ == "__main__":
    main()
