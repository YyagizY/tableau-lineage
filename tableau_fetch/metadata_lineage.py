#!/usr/bin/env python3
"""
Fetch Tableau lineage via the Metadata API (GraphQL) — no workbook download.

Reads workbook structure, datasources, and per-field lineage straight from the
Metadata API and emits a workbook-centric JSON document:

    {
      "workbook-name": ...,
      "number-of-dashboards": N, "number-of-active-dashboards": N,
      "number-of-sheets": N, "number-of-active-sheets": N,
      "number-of-data-sources": N,
      "datasources": [
        { "datasource_name", "delta_table", "storage_path" (filled by enrich),
          "connection_type", "is_used_actively", "sheets_using_the_datasource",
          "fields": [ { "displayed_name", "original_column", "data_type",
                        "is_calculated", "formula", "is_stale" } ] } ]
    }

Uses the *datasource-level* path (embeddedDatasources → fields → upstreamColumns)
rather than the sheet-level path, whose upstreamColumns resolve through Tableau's
catalog/lineage layer and come back empty for Hive Metastore connections.

"active sheet"      = a worksheet surfaced in >= 1 dashboard (containedInDashboards)
"is_used_actively"  = a datasource with >= 1 active downstream sheet
"is_stale" (field)  = the field is used by no active sheet, directly OR transitively
                      (a field referenced by a used field is not stale)

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

from tableau_fetch.twbx import _parse_custom_sql_table
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


_WB_QUERY = """
{
  workbooks(filter: { luid: "%s" }) {
    name
    dashboards { name }
    sheets {
      name
      containedInDashboards { name }
      sheetFieldInstances { name }
    }
    embeddedDatasources {
      name
      hasExtracts
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
        ... on CalculatedField {
          dataType
          formula
          fields { name }
        }
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

    for f in ds.get("fields", []):
        for col in f.get("upstreamColumns") or []:
            tbl = col.get("table") or {}
            if tbl.get("__typename") == "CustomSQLTable":
                ref = _parse_custom_sql_table(tbl.get("query") or "")
                if ref:
                    return ".".join(ref)
    return None


def _field_record(node: dict) -> dict | None:
    """Map a GraphQL field node to the output field dict (is_stale filled later)."""
    typename = node.get("__typename")
    display = node.get("name", "")
    dtype = (node.get("dataType") or "STRING").upper()

    if typename == "CalculatedField":
        return {
            "displayed_name": display,
            "original_column": None,
            "data_type": dtype,
            "is_calculated": True,
            "formula": node.get("formula"),
            "is_stale": False,
        }

    if typename == "ColumnField":
        upstream = node.get("upstreamColumns") or []
        if not upstream and dtype == "TABLE":
            return None  # datasource pseudo-field
        return {
            "displayed_name": display,
            "original_column": upstream[0]["name"] if upstream else display,
            "data_type": dtype,
            "is_calculated": False,
            "formula": None,
            "is_stale": False,
        }

    return None


def _stale_names(ds: dict, active_sheet_names: set, sheet_fields: dict) -> set:
    """Names of this datasource's fields that no active sheet uses (transitively).

    directly-used = field names on active sheets that use this datasource.
    used-closure  = directly-used + every field reachable by following
                    calc-field reference edges from them.
    stale         = datasource fields not in the closure.
    """
    ds_field_names = {f.get("name") for f in ds.get("fields", [])}

    # references graph: field name -> set of field names it references (calc fields)
    refs = {}
    for f in ds.get("fields", []):
        if f.get("__typename") == "CalculatedField":
            refs[f.get("name")] = {r.get("name") for r in (f.get("fields") or [])}

    active_ds_sheets = [s.get("name") for s in ds.get("downstreamSheets", [])
                        if s.get("name") in active_sheet_names]

    directly_used = set()
    for sheet in active_ds_sheets:
        directly_used |= (sheet_fields.get(sheet, set()) & ds_field_names)

    # forward closure: if U is used and U references F, then F is used too
    used = set(directly_used)
    stack = list(directly_used)
    while stack:
        cur = stack.pop()
        for ref in refs.get(cur, set()):
            if ref in ds_field_names and ref not in used:
                used.add(ref)
                stack.append(ref)

    return ds_field_names - used


def fetch_lineage(tableau_url: str) -> dict:
    server, site, workbook_slug = parse_tableau_url(tableau_url)
    token, site_id = signin(server, site)
    try:
        wb_name, luid = resolve_workbook_luid(server, site_id, token, workbook_slug)
        data = _graphql(server, token, _WB_QUERY % luid)
    finally:
        signout(server, token)

    wbs = data.get("workbooks") or []
    if not wbs:
        raise ValueError(f"Workbook {workbook_slug!r} returned no metadata")
    wb = wbs[0]
    workbook_name = wb.get("name") or wb_name

    dashboards = wb.get("dashboards") or []
    sheets = wb.get("sheets") or []

    # active sheet = surfaced in >= 1 dashboard; active dashboard = contains such a sheet
    active_sheet_names = set()
    active_dashboard_names = set()
    sheet_fields = {}
    for s in sheets:
        name = s.get("name")
        containing = [d.get("name") for d in (s.get("containedInDashboards") or [])]
        if containing:
            active_sheet_names.add(name)
            active_dashboard_names.update(containing)
        sheet_fields[name] = {fi.get("name") for fi in (s.get("sheetFieldInstances") or [])}

    datasources = []
    for ds in wb.get("embeddedDatasources", []):
        delta = _delta_for_datasource(ds)
        if delta is None:
            print(f"Warning: no delta path for datasource {ds.get('name')!r}", file=sys.stderr)

        downstream = [s.get("name") for s in (ds.get("downstreamSheets") or [])]
        stale = _stale_names(ds, active_sheet_names, sheet_fields)

        fields = []
        for node in ds.get("fields", []):
            rec = _field_record(node)
            if rec is None:
                continue
            rec["is_stale"] = rec["displayed_name"] in stale
            fields.append(rec)

        datasources.append({
            "datasource_name": ds.get("name", ""),
            "delta_table": delta,
            "storage_path": None,  # filled by enrich_with_paths
            "connection_type": "extract" if ds.get("hasExtracts") else "live",
            "is_used_actively": any(s in active_sheet_names for s in downstream),
            "sheets_using_the_datasource": downstream,
            "fields": fields,
        })

    return {
        "workbook-name": workbook_name,
        "number-of-dashboards": len(dashboards),
        "number-of-active-dashboards": len(active_dashboard_names),
        "number-of-sheets": len(sheets),
        "number-of-active-sheets": len(active_sheet_names),
        "number-of-data-sources": len(wb.get("embeddedDatasources", [])),
        "datasources": datasources,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Fetch Tableau lineage via Metadata API.")
    parser.add_argument("url", help="Tableau Cloud workbook/view URL")
    parser.add_argument("-o", "--output", default="tableau_lineage.json",
                        help="Output JSON path (default: tableau_lineage.json)")
    args = parser.parse_args()

    doc = fetch_lineage(args.url)
    if not doc["datasources"]:
        print("Error: no datasources found", file=sys.stderr)
        sys.exit(1)

    Path(args.output).write_text(json.dumps(doc, indent=2))
    print(f"Loaded: {doc['workbook-name']} | "
          f"{doc['number-of-data-sources']} datasources | "
          f"{doc['number-of-active-sheets']}/{doc['number-of-sheets']} active sheets")
    print(f"Written: {args.output}")


if __name__ == "__main__":
    main()
