"""
Story 2 — Delta Table Resolution Module

Takes datasource connection details from the Tableau extraction and resolves
the exact delta table name and storage path via the Databricks Unity Catalog
REST API (hive_metastore catalog).
"""

import os
import requests
from dataclasses import dataclass


@dataclass
class DeltaTable:
    full_name: str          # hive_metastore.<schema>.<table>
    storage_path: str
    catalog: str
    schema: str
    table: str


def _databricks_host() -> str:
    host = os.environ.get("DATABRICKS_HOST", "")
    if not host:
        raise EnvironmentError("Missing required environment variable: DATABRICKS_HOST")
    return host.rstrip("/")


def _databricks_pat() -> str:
    pat = os.environ.get("DATABRICKS_PAT", "")
    if not pat:
        raise EnvironmentError("Missing required environment variable: DATABRICKS_PAT")
    return pat


def _get(path: str) -> dict:
    host = _databricks_host()
    pat = _databricks_pat()
    resp = requests.get(
        f"{host}{path}",
        headers={"Authorization": f"Bearer {pat}"},
        timeout=30,
    )
    if resp.status_code == 404:
        return {}
    resp.raise_for_status()
    return resp.json()


def _resolve_full_name(database: str, schema: str, table: str) -> str:
    """
    Build the hive_metastore full name. Tableau may give us a database name
    that maps to the hive schema, so we normalise here.
    """
    # If database already looks like a full catalog.schema, leave it
    parts = [p for p in [database, schema, table] if p]
    if len(parts) == 3:
        return ".".join(parts)
    # Prefix with hive_metastore as the default catalog
    if len(parts) == 2:
        return f"hive_metastore.{'.'.join(parts)}"
    return f"hive_metastore.{parts[0]}"


def resolve_delta_table(database: str, schema: str, table: str) -> DeltaTable:
    full_name = _resolve_full_name(database, schema, table)
    encoded = full_name.replace(".", "%2E")  # Unity Catalog path uses dots
    data = _get(f"/api/2.1/unity-catalog/tables/{full_name}")

    if not data:
        raise ValueError(
            f"Delta table {full_name!r} not found in Unity Catalog.\n"
            "Check that DATABRICKS_HOST is correct and the PAT has metastore read access."
        )

    storage_path = data.get("storage_location", "")
    catalog_name = data.get("catalog_name", "hive_metastore")
    schema_name = data.get("schema_name", schema)
    table_name = data.get("name", table)

    return DeltaTable(
        full_name=f"{catalog_name}.{schema_name}.{table_name}",
        storage_path=storage_path,
        catalog=catalog_name,
        schema=schema_name,
        table=table_name,
    )
