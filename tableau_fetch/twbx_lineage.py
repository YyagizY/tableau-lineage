#!/usr/bin/env python3
"""
CLI: parse a local .twbx file and write tableau_lineage.json.

Usage:
    python twbx_lineage.py <workbook.twbx> [-o tableau_lineage.json]
"""

import argparse
import json
import sys
from pathlib import Path

from tableau_fetch.twbx import load_twbx, to_json_payload


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Extract lineage from a local .twbx workbook."
    )
    parser.add_argument("twbx_path", help="Path to the .twbx (or .twb) file")
    parser.add_argument(
        "-o", "--output", default="tableau_lineage.json",
        help="Output JSON path (default: tableau_lineage.json)",
    )
    args = parser.parse_args()

    src = Path(args.twbx_path)
    if not src.exists():
        print(f"Error: file not found: {src}", file=sys.stderr)
        sys.exit(1)

    sheets = load_twbx(src)
    if not sheets:
        print(f"Error: no worksheets found in {src}", file=sys.stderr)
        sys.exit(1)

    out_path = Path(args.output)
    out_path.write_text(json.dumps(to_json_payload(sheets), indent=2))

    workbook = sheets[0].workbook
    delta = sheets[0].datasource.delta_table or "<none>"
    print(f"Loaded: {workbook} | {len(sheets)} sheets | Delta table: {delta}")
    print(f"Written: {out_path}")


if __name__ == "__main__":
    main()
