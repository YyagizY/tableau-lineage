"""
Local .twbx lineage extractor.

Parses a Tableau workbook (.twbx = zipped .twb XML) and produces per-sheet
lineage records. No Tableau Server or Databricks API calls — everything comes
from the local file.
"""

from __future__ import annotations

import re
import sys
import zipfile
import xml.etree.ElementTree as ET
from dataclasses import dataclass, asdict, field
from pathlib import Path


@dataclass
class TwbxField:
    displayed_name: str
    original_column: str | None
    data_type: str
    is_calculated: bool
    formula: str | None


@dataclass
class TwbxDatasource:
    tableau_datasource_name: str
    delta_table: str | None


@dataclass
class TwbxSheet:
    workbook: str
    sheet: str
    datasource: TwbxDatasource
    fields: list[TwbxField] = field(default_factory=list)


_DATATYPE_MAP = {
    "string": "STRING",
    "integer": "INTEGER",
    "real": "REAL",
    "boolean": "BOOLEAN",
    "date": "DATE",
    "datetime": "DATETIME",
}


def _map_datatype(raw: str | None) -> str:
    return _DATATYPE_MAP.get((raw or "").lower(), "STRING")


def _strip_brackets(name: str) -> str:
    s = name or ""
    if len(s) >= 2 and s.startswith("[") and s.endswith("]"):
        s = s[1:-1]
    return s


# Captures table refs like FROM/JOIN schema.table or catalog.schema.table.
# Strips optional backticks/brackets. Stops at whitespace/comma/end.
_SQL_TABLE_REF_RE = re.compile(
    r"\b(?:FROM|JOIN)\s+"
    r"[`\[]?([a-zA-Z_][a-zA-Z0-9_]*)[`\]]?"
    r"\."
    r"[`\[]?([a-zA-Z_][a-zA-Z0-9_]*)[`\]]?"
    r"(?:\.[`\[]?([a-zA-Z_][a-zA-Z0-9_]*)[`\]]?)?",
    re.IGNORECASE,
)


def _parse_custom_sql_table(sql: str) -> tuple[str, ...] | None:
    refs = {tuple(p for p in m.groups() if p) for m in _SQL_TABLE_REF_RE.finditer(sql or "")}
    return refs.pop() if len(refs) == 1 else None


def _catalog_from_connections(ds_elem: ET.Element) -> str | None:
    for conn in ds_elem.iter("connection"):
        if conn.get("class") in ("hyper", "federated"):
            continue
        dbname = conn.get("dbname")
        if dbname and "/" not in dbname and not dbname.startswith("dbfs:"):
            return dbname
    return None


def _extract_delta_path(ds_elem: ET.Element) -> str | None:
    catalog = _catalog_from_connections(ds_elem)

    # 1. <relation type='table' table='[...]'> — the most reliable signal.
    #    3 parts → use as-is. 2 parts → combine with the catalog from the
    #    Databricks connection. Skip the local "[Extract].[Extract]" cache.
    for rel in ds_elem.iter("relation"):
        t = rel.get("table") or ""
        parts = [_strip_brackets(p) for p in t.split(".") if p]
        if not parts or parts[0].lower() == "extract":
            continue
        if len(parts) == 3:
            return ".".join(parts)
        if len(parts) == 2 and catalog:
            return f"{catalog}.{parts[0]}.{parts[1]}"

    # 2. Custom SQL Query (<relation type='text'>). If the SQL references a
    #    single table, combine with the connection's catalog when needed.
    for rel in ds_elem.iter("relation"):
        if rel.get("type") != "text" or not rel.text:
            continue
        ref = _parse_custom_sql_table(rel.text)
        if ref is None:
            continue
        if len(ref) == 3:
            return ".".join(ref)
        if len(ref) == 2 and catalog:
            return f"{catalog}.{ref[0]}.{ref[1]}"

    real_conns = [
        c for c in ds_elem.iter("connection")
        if c.get("class") not in ("hyper", "federated")
    ]

    # 3. Connection with dbname + schema + table.
    for conn in real_conns:
        dbname = conn.get("dbname")
        schema = conn.get("schema")
        table = conn.get("table")
        if dbname and schema and table:
            return f"{dbname}.{schema}.{table}"

    # 4. Path-style dbname (Delta Lake location URL).
    for conn in real_conns:
        dbname = conn.get("dbname") or ""
        if "/" in dbname or dbname.startswith("dbfs:"):
            return dbname

    return None


def _parse_column(col_elem: ET.Element) -> TwbxField | None:
    name_attr = col_elem.get("name")
    if not name_attr:
        return None

    original = _strip_brackets(name_attr)
    caption = col_elem.get("caption") or original
    datatype = _map_datatype(col_elem.get("datatype"))

    calc = col_elem.find("calculation")
    if calc is not None:
        return TwbxField(
            displayed_name=caption,
            original_column=None,
            data_type=datatype,
            is_calculated=True,
            formula=calc.get("formula"),
        )

    return TwbxField(
        displayed_name=caption,
        original_column=original,
        data_type=datatype,
        is_calculated=False,
        formula=None,
    )


def _parse_datasource(ds_elem: ET.Element) -> tuple[TwbxDatasource, list[TwbxField]]:
    ds_caption = ds_elem.get("caption") or ds_elem.get("name", "")
    delta = _extract_delta_path(ds_elem)
    if delta is None:
        print(
            f"Warning: no delta path found for datasource {ds_caption!r}",
            file=sys.stderr,
        )

    fields: list[TwbxField] = []
    for col in ds_elem.findall("column"):
        f = _parse_column(col)
        if f is not None:
            fields.append(f)

    return TwbxDatasource(tableau_datasource_name=ds_caption, delta_table=delta), fields


def _find_worksheet_datasource_ref(ws_elem: ET.Element) -> str | None:
    for ds_ref in ws_elem.iter("datasource"):
        name = ds_ref.get("name")
        if name and name != "Parameters":
            return name
    return None


def _read_twb_xml(path: Path) -> bytes:
    data = path.read_bytes()
    # Tableau's REST download endpoint sometimes returns a zipped .twbx even
    # when called with includeExtract=false, so dispatch on magic bytes.
    if data[:4] == b"PK\x03\x04":
        with zipfile.ZipFile(path, "r") as zf:
            twb_names = [n for n in zf.namelist() if n.lower().endswith(".twb")]
            if not twb_names:
                raise ValueError(f"No .twb file found inside {path}")
            return zf.read(twb_names[0])
    return data


def load_twbx(path: str | Path) -> list[TwbxSheet]:
    path = Path(path)
    xml_bytes = _read_twb_xml(path)
    root = ET.fromstring(xml_bytes)
    workbook_name = path.stem

    datasources: dict[str, tuple[TwbxDatasource, list[TwbxField]]] = {}
    for ds_elem in root.findall(".//datasources/datasource"):
        ds_name = ds_elem.get("name", "")
        if not ds_name or ds_name == "Parameters":
            continue
        if ds_elem.find(".//connection") is None:
            continue
        datasources[ds_name] = _parse_datasource(ds_elem)

    sheets: list[TwbxSheet] = []
    for ws in root.findall(".//worksheets/worksheet"):
        sheet_name = ws.get("name", "")
        ref = _find_worksheet_datasource_ref(ws)
        if ref not in datasources:
            if len(datasources) == 1:
                ref = next(iter(datasources))
            else:
                print(
                    f"Warning: worksheet {sheet_name!r} has no resolvable datasource",
                    file=sys.stderr,
                )
                continue
        ds_info, fields = datasources[ref]
        sheets.append(
            TwbxSheet(
                workbook=workbook_name,
                sheet=sheet_name,
                datasource=ds_info,
                fields=list(fields),
            )
        )

    return sheets


def to_json_payload(sheets: list[TwbxSheet]):
    dicts = [asdict(s) for s in sheets]
    if len(dicts) == 1:
        return dicts[0]
    return dicts
