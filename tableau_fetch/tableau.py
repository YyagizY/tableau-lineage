"""
Story 1 — Tableau Cloud Metadata Extraction Module

Connects to Tableau Cloud, authenticates via PAT, and extracts all metadata
for a given workbook/sheet using the Metadata API (GraphQL).
"""

import os
import re
import requests
from dataclasses import dataclass, field


TABLEAU_API_VERSION = "3.21"


@dataclass
class TableauAuth:
    site: str
    token: str
    site_id: str
    base_url: str


@dataclass
class ParsedUrl:
    pod: str
    site: str
    workbook: str
    view: str


@dataclass
class FieldInfo:
    name: str
    display_name: str
    field_type: str          # "dimension" | "measure" | "calculated"
    data_type: str
    formula: str | None = None
    referenced_columns: list[str] = field(default_factory=list)


@dataclass
class DatasourceInfo:
    name: str
    database: str
    schema: str
    table: str


@dataclass
class SheetMetadata:
    workbook: str
    sheet: str
    fields: list[FieldInfo]
    datasource: DatasourceInfo


URL_PATTERN = re.compile(
    r"https://(?P<pod>[^.]+)\.online\.tableau\.com/#/site/(?P<site>[^/]+)/views/(?P<workbook>[^/]+)/(?P<view>.+)"
)


def parse_url(url: str) -> ParsedUrl:
    m = URL_PATTERN.match(url)
    if not m:
        raise ValueError(
            f"Invalid Tableau Cloud URL: {url!r}\n"
            "Expected format: https://<pod>.online.tableau.com/#/site/<site>/views/<workbook>/<view>"
        )
    return ParsedUrl(**m.groupdict())


def authenticate(parsed: ParsedUrl) -> TableauAuth:
    pat_name = os.environ.get("TABLEAU_PAT_NAME")
    pat_secret = os.environ.get("TABLEAU_PAT_SECRET")
    if not pat_name or not pat_secret:
        raise EnvironmentError(
            "Missing required environment variables: TABLEAU_PAT_NAME, TABLEAU_PAT_SECRET"
        )

    base_url = f"https://{parsed.pod}.online.tableau.com/api/{TABLEAU_API_VERSION}"
    payload = {
        "credentials": {
            "personalAccessTokenName": pat_name,
            "personalAccessTokenSecret": pat_secret,
            "site": {"contentUrl": parsed.site},
        }
    }
    resp = requests.post(
        f"{base_url}/auth/signin",
        json=payload,
        headers={"Accept": "application/json"},
        timeout=30,
    )
    if resp.status_code == 401:
        raise PermissionError("Tableau PAT authentication failed — check TABLEAU_PAT_NAME and TABLEAU_PAT_SECRET.")
    resp.raise_for_status()

    data = resp.json()
    return TableauAuth(
        site=parsed.site,
        token=data["credentials"]["token"],
        site_id=data["credentials"]["site"]["id"],
        base_url=base_url,
    )


def _graphql(auth: TableauAuth, query: str) -> dict:
    metadata_url = auth.base_url.replace(f"/api/{TABLEAU_API_VERSION}", "/api/metadata/graphql")
    resp = requests.post(
        metadata_url,
        json={"query": query},
        headers={
            "X-Tableau-Auth": auth.token,
            "Accept": "application/json",
        },
        timeout=30,
    )
    resp.raise_for_status()
    result = resp.json()
    if "errors" in result:
        actual_errors = [
            e for e in result["errors"]
            if e.get("extensions", {}).get("severity", "ERROR") != "WARNING"
        ]
        if actual_errors:
            raise RuntimeError(f"Metadata API errors: {actual_errors}")
    return result["data"]


def _build_fields_query(workbook_name: str) -> str:
    safe = workbook_name.replace("\\", "\\\\").replace('"', '\\"')
    field_fragment = """
              name
              ... on ColumnField {
                __typename
                dataType
                role
                upstreamColumns {
                  name
                  table {
                    ... on DatabaseTable {
                      name
                      schema
                      fullName
                      database { name }
                    }
                  }
                }
              }
              ... on CalculatedField {
                __typename
                dataType
                formula
              }
"""
    return f"""
{{
  workbooksConnection(filter: {{ name: "{safe}" }}) {{
    nodes {{
      name
      sheetsConnection {{
        nodes {{
          name
          sheetFieldInstancesConnection {{
            nodes {{{field_fragment}
            }}
          }}
        }}
      }}
      dashboardsConnection {{
        nodes {{
          name
          sheetsConnection {{
            nodes {{
              sheetFieldInstancesConnection {{
                nodes {{{field_fragment}
                }}
              }}
            }}
          }}
        }}
      }}
    }}
  }}
}}
"""

def _resolve_workbook_name(auth: TableauAuth, workbook_slug: str) -> str:
    """Resolve the exact workbook name from its URL slug using the REST API."""
    resp = requests.get(
        f"{auth.base_url}/sites/{auth.site_id}/workbooks",
        params={"filter": f"contentUrl:eq:{workbook_slug}"},
        headers={"X-Tableau-Auth": auth.token, "Accept": "application/json"},
        timeout=30,
    )
    resp.raise_for_status()
    workbooks = resp.json().get("workbooks", {}).get("workbook", [])
    if not workbooks:
        raise ValueError(f"Workbook not found for slug {workbook_slug!r}.")
    return workbooks[0]["name"]


def resolve_sheet_name(auth: TableauAuth, workbook_slug: str, view_slug: str) -> str:
    """Resolve the actual sheet name from the URL slugs using the REST API."""
    content_url = f"{workbook_slug}/sheets/{view_slug}"
    resp = requests.get(
        f"{auth.base_url}/sites/{auth.site_id}/views",
        params={"filter": f"contentUrl:eq:{content_url}"},
        headers={"X-Tableau-Auth": auth.token, "Accept": "application/json"},
        timeout=30,
    )
    resp.raise_for_status()
    views = resp.json().get("views", {}).get("view", [])
    if not views:
        raise ValueError(
            f"View not found for workbook={workbook_slug!r} view={view_slug!r}. "
            "Check the URL is correct and the PAT has tableau:views:read scope."
        )
    return views[0]["name"]


def _parse_fields(nodes: list[dict]) -> tuple[list[FieldInfo], DatasourceInfo | None]:
    fields = []
    datasource: DatasourceInfo | None = None

    for node in nodes:
        typename = node.get("__typename", "")
        display_name = node.get("name", "")
        data_type = node.get("dataType", "unknown")

        if typename == "CalculatedField":
            formula = node.get("formula")
            fi = FieldInfo(
                name=display_name,
                display_name=display_name,
                field_type="calculated",
                data_type=data_type,
                formula=formula,
            )
            fields.append(fi)

        elif typename == "ColumnField":
            upstream = node.get("upstreamColumns", [])
            original_cols = [c["name"] for c in upstream if c.get("name")]
            original_col = original_cols[0] if original_cols else display_name
            field_type = "measure" if node.get("role", "").upper() == "MEASURE" else "dimension"

            # V1 limitation: only the first table encountered is captured as the datasource.
            # Views that join multiple tables will silently lose all but the first.
            if datasource is None and upstream:
                for col in upstream:
                    tbl = col.get("table") or {}
                    if tbl:
                        db = (tbl.get("database") or {}).get("name", "")
                        datasource = DatasourceInfo(
                            name=tbl.get("fullName", ""),
                            database=db,
                            schema=tbl.get("schema", ""),
                            table=tbl.get("name", ""),
                        )
                        break

            fi = FieldInfo(
                name=original_col,
                display_name=display_name,
                field_type=field_type,
                data_type=data_type,
                referenced_columns=original_cols,
            )
            fields.append(fi)

        # DatasourceField (Tableau-generated fields like Number of Records,
        # Measure Names/Values, groups, sets) have no upstream Databricks column — skip them.

    return fields, datasource


def fetch_sheet_metadata(url: str) -> SheetMetadata:
    parsed_url = parse_url(url)
    auth = authenticate(parsed_url)

    workbook_name = _resolve_workbook_name(auth, parsed_url.workbook)

    # The Metadata API only supports filtering by workbook name, not by sheet/view name.
    # We fetch the full workbook and filter down to the target sheet.
    data = _graphql(auth, _build_fields_query(workbook_name))

    all_workbooks = data.get("workbooksConnection", {}).get("nodes", [])
    if not all_workbooks:
        raise ValueError(f"Workbook {parsed_url.workbook!r} not found on site {parsed_url.site!r}.")

    wb_node = all_workbooks[0]
    all_views = (
        wb_node.get("sheetsConnection", {}).get("nodes", [])
        + wb_node.get("dashboardsConnection", {}).get("nodes", [])
    )

    sheet_name = resolve_sheet_name(auth, parsed_url.workbook, parsed_url.view)
    matched = [v for v in all_views if v["name"] == sheet_name]
    if not matched:
        raise ValueError(f"View {sheet_name!r} not found in workbook {workbook_name!r}.")

    view = matched[0]
    if "sheetFieldInstancesConnection" in view:
        # It's a worksheet — field data is directly available
        field_nodes = view["sheetFieldInstancesConnection"].get("nodes", [])
    else:
        # It's a dashboard — collect fields from all inner sheets and deduplicate by name
        seen = set()
        field_nodes = []
        for sheet in view.get("sheetsConnection", {}).get("nodes", []):
            for node in sheet.get("sheetFieldInstancesConnection", {}).get("nodes", []):
                name = node.get("name")
                if name not in seen:
                    seen.add(name)
                    field_nodes.append(node)
    fields, datasource = _parse_fields(field_nodes)

    if datasource is None:
        datasource = DatasourceInfo(name="unknown", database="", schema="", table="")

    return SheetMetadata(
        workbook=workbook_name,
        sheet=sheet_name,
        fields=fields,
        datasource=datasource,
    )
