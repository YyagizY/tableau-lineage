#!/usr/bin/env python3
"""
Fetch Tableau lineage via the Metadata API (GraphQL) — no workbook download.

Replaces the download_workbook + twbx XML-parse front end. Uses the
*datasource-level* path

    workbooksConnection -> embeddedDatasourcesConnection
                        -> fieldsConnection -> upstreamColumns

which reads column references straight from the embedded datasource model,
instead of the sheet-level path

    workbooksConnection -> sheetsConnection
                        -> sheetFieldInstancesConnection -> upstreamColumns

whose upstreamColumns are resolved through Tableau's lineage/catalog layer and
come back empty for Hive Metastore connections.

Emits the exact same per-sheet JSON shape as tableau_fetch.twbx, so
enrich_with_paths.py and all downstream consumers are unchanged.

Usage:
    python3 -m tableau_fetch.metadata_lineage <tableau_url> [-o lineage.json]
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path

import requests
from dotenv import load_dotenv

from tableau_fetch.twbx import (
    TwbxField,
    TwbxDatasource,
    TwbxSheet,
    to_json_payload,
    _parse_custom_sql_table,
)
from tableau_fetch.download_workbook import parse_tableau_url

for _env_path in [Path.cwd() / ".env", Path(__file__).resolve().parent.parent / ".env"]:
    if _env_path.exists():
        load_dotenv(_env_path)
        break

PAT_NAME = os.getenv("TABLEAU_PAT_NAME")
PAT_SECRET = os.getenv("TABLEAU_PAT_SECRET")

API_VERSION = "3.21"


def _check(resp):
    if not resp.ok:
        raise RuntimeError(f"{resp.status_code} {resp.reason} at {resp.url}\nBody: {resp.text}")
    return resp


def signin(server, site):
    if not PAT_NAME or not PAT_SECRET:
        raise RuntimeError("TABLEAU_PAT_NAME / TABLEAU_PAT_SECRET missing from .env")
    resp = _check(requests.post(
        f"{server}/api/{API_VERSION}/auth/signin",
        headers={"Accept": "application/json", "Content-Type": "application/json"},
        json={"credentials": {
            "personalAccessTokenName": PAT_NAME,
            "personalAccessTokenSecret": PAT_SECRET,
            "site": {"contentUrl": site},
        }},
        timeout=30,
    ))
    c = resp.json()["credentials"]
    return c["token"], c["site"]["id"]


def signout(server, token):
    requests.post(f"{server}/api/{API_VERSION}/auth/signout", headers={"x-tableau-auth": token})


def resolve_workbook_luid(server, site_id, token, workbook_slug):
    """Map the URL contentUrl slug to the workbook's exact name + luid via REST.

    Filtering the Metadata API by name alone can match several workbooks that
    share a display name; resolving the luid first keeps the GraphQL query exact.
    """
    resp = _check(requests.get(
        f"{server}/api/{API_VERSION}/sites/{site_id}/workbooks",
        headers={"x-tableau-auth": token, "Accept": "application/json"},
        params={"filter": f"contentUrl:eq:{workbook_slug}"},
    ))
    wbs = resp.json().get("workbooks", {}).get("workbook", [])
    if not wbs:
        raise ValueError(f"Workbook with contentUrl '{workbook_slug}' not found on site")
    return wbs[0]["name"], wbs[0]["id"]


_DS_QUERY = """
{
  workbooks(filter: { luid: "%s" }) {
    name
    embeddedDatasources {
      name
      downstreamSheets { name }
      upstreamTables {
        name
        schema
        database { name connectionType }
      }
      fields {
        name
        __typename
        ... on ColumnField {
          dataType
          upstreamColumns {
            name
            table {
              __typename
              ... on DatabaseTable { name schema database { name } }
              ... on CustomSQLTable { name query }
            }
          }
        }
        ... on CalculatedField { dataType formula }
      }
    }
  }
}
"""


def _graphql(server, token, query):
    resp = _check(requests.post(
        f"{server}/api/metadata/graphql",
        headers={"X-Tableau-Auth": token, "Accept": "application/json"},
        json={"query": query},
        timeout=120,
    ))
    result = resp.json()
    errors = [
        e for e in result.get("errors", [])
        if e.get("extensions", {}).get("severity", "ERROR") != "WARNING"
    ]
    if errors:
        raise RuntimeError(f"Metadata API errors: {json.dumps(errors)[:1000]}")
    return result["data"]


def _delta_from_database_table(tbl: dict) -> str | None:
    """Reconstruct catalog.schema.table from a DatabaseTable node.

    fullName is unreliable (sometimes [cat].[schema].[table], sometimes bare
    schema.table), so build it from the component parts instead.
    """
    name = tbl.get("name")
    schema = tbl.get("schema")
    db = (tbl.get("database") or {}).get("name")
    if db and schema and name:
        return f"{db}.{schema}.{name}"
    if schema and name:
        return f"{schema}.{name}"
    return name or None


def _delta_for_datasource(ds: dict) -> str | None:
    """Best underlying table for a datasource.

    Prefer the datasource-level upstreamTables (already resolves custom SQL to
    the real physical table). Fall back to a CustomSQLTable's query text parsed
    with the same regex the XML parser used.
    """
    tables = ds.get("upstreamTables") or []
    if tables:
        # V1: if a datasource joins multiple tables, take the first.
        return _delta_from_database_table(tables[0])

    # Fallback: dig a CustomSQLTable out of the fields' upstreamColumns.
    for f in ds.get("fields", []):
        for col in f.get("upstreamColumns") or []:
            tbl = col.get("table") or {}
            if tbl.get("__typename") == "CustomSQLTable":
                ref = _parse_custom_sql_table(tbl.get("query") or "")
                if ref:
                    return ".".join(ref)
    return None


def _parse_field(node: dict) -> TwbxField | None:
    typename = node.get("__typename")
    display = node.get("name", "")
    dtype = (node.get("dataType") or "STRING").upper()

    if typename == "CalculatedField":
        return TwbxField(display, None, dtype, True, node.get("formula"))

    if typename == "ColumnField":
        upstream = node.get("upstreamColumns") or []
        # dataType=TABLE with no upstream columns is the datasource pseudo-field.
        if not upstream and dtype == "TABLE":
            return None
        original = upstream[0]["name"] if upstream else display
        return TwbxField(display, original, dtype, False, None)

    return None


def fetch_lineage(tableau_url: str) -> list[TwbxSheet]:
    server, site, workbook_slug = parse_tableau_url(tableau_url)
    token, site_id = signin(server, site)
    try:
        wb_name, luid = resolve_workbook_luid(server, site_id, token, workbook_slug)
        data = _graphql(server, token, _DS_QUERY % luid)
    finally:
        signout(server, token)

    wbs = data.get("workbooks") or []
    if not wbs:
        raise ValueError(f"Workbook {workbook_slug!r} returned no metadata")
    wb = wbs[0]
    workbook_name = wb.get("name") or wb_name

    sheets: list[TwbxSheet] = []
    for ds in wb.get("embeddedDatasources", []):
        caption = ds.get("name", "")
        delta = _delta_for_datasource(ds)
        if delta is None:
            print(f"Warning: no delta path for datasource {caption!r}", file=sys.stderr)

        fields = [f for f in (_parse_field(n) for n in ds.get("fields", [])) if f]
        ds_info = TwbxDatasource(tableau_datasource_name=caption, delta_table=delta)

        for sheet in ds.get("downstreamSheets", []):
            sheets.append(TwbxSheet(
                workbook=workbook_name,
                sheet=sheet.get("name", ""),
                datasource=ds_info,
                fields=list(fields),
            ))
    return sheets


def main() -> None:
    parser = argparse.ArgumentParser(description="Fetch Tableau lineage via Metadata API.")
    parser.add_argument("url", help="Tableau Cloud workbook/view URL")
    parser.add_argument("-o", "--output", default="tableau_lineage.json",
                        help="Output JSON path (default: tableau_lineage.json)")
    args = parser.parse_args()

    sheets = fetch_lineage(args.url)
    if not sheets:
        print("Error: no sheets/datasources found", file=sys.stderr)
        sys.exit(1)

    Path(args.output).write_text(json.dumps(to_json_payload(sheets), indent=2))
    delta = sheets[0].datasource.delta_table or "<none>"
    print(f"Loaded: {sheets[0].workbook} | {len(sheets)} sheet-records | first delta: {delta}")
    print(f"Written: {args.output}")


if __name__ == "__main__":
    main()
