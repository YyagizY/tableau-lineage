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


def _graphql(auth: TableauAuth, query: str, variables: dict) -> dict:
    metadata_url = auth.base_url.replace(f"/api/{TABLEAU_API_VERSION}", "/api/metadata/graphql")
    resp = requests.post(
        metadata_url,
        json={"query": query, "variables": variables},
        headers={
            "X-Tableau-Auth": auth.token,
            "Accept": "application/json",
        },
        timeout=30,
    )
    resp.raise_for_status()
    result = resp.json()
    if "errors" in result:
        raise RuntimeError(f"Metadata API errors: {result['errors']}")
    return result["data"]


FIELDS_QUERY = """
query SheetFields($workbookName: String!, $sheetName: String!) {
  workbooksConnection(filter: { name: $workbookName }) {
    nodes {
      name
      sheetsConnection(filter: { name: $sheetName }) {
        nodes {
          name
          fieldsConnection {
            nodes {
              name
              ... on ColumnField {
                __typename
                dataType
                upstreamColumns {
                  name
                  table {
                    ... on DatabaseTable {
                      name
                      schema
                      fullName
                      database {
                        name
                      }
                    }
                  }
                }
              }
              ... on CalculatedField {
                __typename
                dataType
                formula
              }
              ... on DatasourceField {
                __typename
                dataType
              }
            }
          }
        }
      }
    }
  }
}
"""


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

            # Extract datasource info from the first upstream column that has table info
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
                field_type="dimension",
                data_type=data_type,
                referenced_columns=original_cols,
            )
            fields.append(fi)

        else:
            fi = FieldInfo(
                name=display_name,
                display_name=display_name,
                field_type="dimension",
                data_type=data_type,
            )
            fields.append(fi)

    return fields, datasource


def fetch_sheet_metadata(url: str) -> SheetMetadata:
    parsed = parse_url(url)
    auth = authenticate(parsed)

    data = _graphql(
        auth,
        FIELDS_QUERY,
        {"workbookName": parsed.workbook, "sheetName": parsed.view},
    )

    workbooks = data.get("workbooksConnection", {}).get("nodes", [])
    if not workbooks:
        raise ValueError(f"Workbook {parsed.workbook!r} not found on site {parsed.site!r}.")

    sheets = workbooks[0].get("sheetsConnection", {}).get("nodes", [])
    if not sheets:
        raise ValueError(f"Sheet {parsed.view!r} not found in workbook {parsed.workbook!r}.")

    field_nodes = sheets[0].get("fieldsConnection", {}).get("nodes", [])
    fields, datasource = _parse_fields(field_nodes)

    if datasource is None:
        datasource = DatasourceInfo(name="unknown", database="", schema="", table="")

    return SheetMetadata(
        workbook=parsed.workbook,
        sheet=parsed.view,
        fields=fields,
        datasource=datasource,
    )
