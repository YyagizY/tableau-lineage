# tableau-lineage

Two-step pipeline that reads a Tableau Cloud workbook's lineage from the
Metadata API (GraphQL — no download), then enriches each datasource with the
Databricks storage path resolved via the SQL Statement Execution API. The
final JSON grounds Claude Code in data lineage for manager questions.

## Repo layout

```
pipeline.py                       # Orchestrator — runs the two steps end-to-end
tableau_fetch/
  __init__.py
  metadata_lineage.py             # Step 1: Tableau URL → lineage JSON via Metadata API (no download)
  enrich_with_paths.py            # Step 2: lineage JSON → enriched JSON (adds storage_path)
  download_workbook.py            # Legacy/offline: Tableau URL → .twb via REST API + PAT
  twbx_lineage.py                 # Legacy/offline: .twb/.twbx → lineage JSON (used by --twb)
  twbx.py                         # .twbx/.twb XML parser; also supplies the shared dataclasses
requirements.txt
.env.example
```

The default path (`metadata_lineage.py`) never downloads the workbook, so the
PAT only needs `tableau:metadata:read` — **Download Workbook permission is no
longer required**. The `--twb` flag still parses a local `.twb/.twbx` offline
through the legacy XML extractor as an escape hatch.

`tests/` is gitignored — kept locally for development only.

## Jira epic

Source of truth for stories and acceptance criteria: CXNAPB-112

## Running locally

```bash
# Install deps
pip install -r requirements.txt

# Copy .env.example → .env and fill in:
#   TABLEAU_PAT_NAME, TABLEAU_PAT_SECRET
#   DATABRICKS_HOST, DATABRICKS_PAT_SECRET
#   DATABRICKS_WAREHOUSE_ID (optional — auto-picked if omitted)

# End-to-end run
python3 pipeline.py \
  "https://us-east-1.online.tableau.com/#/site/invent-us/views/DCNeedReport/SummaryView" \
  --customer fivebelow \
  -o lineage_enriched.json
```

Each step is also runnable standalone as a module from the repo root:

```bash
python3 -m tableau_fetch.metadata_lineage   <url> [-o lineage.json]   # default: no download
python3 -m tableau_fetch.enrich_with_paths  [input.json] [output.json]

# Legacy / offline (only when parsing a local file via --twb):
python3 -m tableau_fetch.download_workbook  <url> [output.twb]
python3 -m tableau_fetch.twbx_lineage       <workbook.twb> [-o lineage.json]
```

## Output shape

Workbook-centric. Counts summarize the workbook; each datasource carries its
resolved storage path, connection type, active-usage flag, the sheets that use
it, and its fields (each flagged `is_stale`).

```json
{
  "customer-name": "customer-pipeline-fivebelow",
  "workbook-name": "DC Need Report",
  "number-of-dashboards": 13,
  "number-of-active-dashboards": 13,
  "number-of-sheets": 17,
  "number-of-active-sheets": 17,
  "number-of-data-sources": 8,
  "number-of-parameters": 3,
  "parameters": ["Review Dimension DC", "Measure Selector", "..."],
  "datasources": [
    {
      "datasource_name": "...",
      "delta_table": "hive_metastore.fivebelow.dcrpl_order_report",
      "storage_path": "dbfs:/mnt/.../dc_rpl/reporting/order_report",
      "connection_type": "live",
      "is_used_actively": true,
      "sheets_using_the_datasource": ["sheet1", "sheet2"],
      "fields": [
        {
          "displayed_name": "Review Dimension",
          "original_column": null,
          "data_type": "STRING",
          "is_calculated": true,
          "formula": "IF [Parameters].[Parameter 1]=... END",
          "is_stale": false
        }
      ]
    }
  ]
}
```

Definitions:
- **active sheet** = worksheet surfaced in ≥1 dashboard (`containedInDashboards`).
- **`connection_type`** = `extract` if the datasource has a materialized extract
  (`hasExtracts`), else `live`.
- **`is_used_actively`** = the datasource has ≥1 active downstream sheet.
- **`is_stale`** (field) = the field is used by no active sheet — directly or
  transitively. A field referenced by a used calc field is *not* stale; the
  reference graph comes from `CalculatedField.fields`.
- **`delta_table`** is retained alongside `storage_path` because
  `enrich_with_paths.py` needs it and `rules/lineage-tracing.md` keys off it.
- **`parameters`** lists workbook parameter *names* only. The Metadata API's
  `Parameter` type exposes nothing else (no datatype / current value / allowed
  values); that richer detail exists only in the downloaded `.twb` XML.

## Environment variables

See `.env.example`. Never commit `.env` or real PAT values.

## Key design decisions

- `tableau_fetch/metadata_lineage.py` queries the Metadata API along the
  *datasource* path (`embeddedDatasources → fields → upstreamColumns`) rather
  than the *sheet* path (`sheetFieldInstances → upstreamColumns`). The sheet
  path resolves columns through Tableau's catalog/lineage layer, which returns
  empty for `hive_metastore` connectors — the original reason the project was
  rewritten to download and parse XML. The datasource path reads column refs
  straight from the embedded datasource model and is not gated by the catalog.
- `delta_table` is reconstructed from a `DatabaseTable`'s component parts
  (`database.name` + `schema` + `name`), not its `fullName`, because `fullName`
  is inconsistently bracketed/qualified across datasources.
- Calculated-field formulas from the Metadata API are caption-resolved
  (`SUM([Sales Revenue])`) rather than the raw internal identifiers the XML
  carried (`SUM([sales_revenue])`, `[Calculation_...]`) — same logic, friendlier
  names.
- `tableau_fetch/enrich_with_paths.py` uses the Databricks SQL Statement
  Execution API (`DESCRIBE DETAIL`) rather than the Unity Catalog tables
  endpoint, because UC doesn't reliably surface `storage_location` for
  `hive_metastore` tables.
- Warehouse auto-selection prefers a RUNNING warehouse to avoid start latency
  and permission issues with stopped warehouses.
- `pipeline.py` uses a tempdir for intermediates so only the final enriched
  JSON remains on disk.
- `tableau_fetch/twbx.py:_read_twb_xml` dispatches on the file's magic bytes
  (PK header → unzip; otherwise → raw XML) rather than the suffix, because
  Tableau's REST download endpoint sometimes returns a zipped `.twbx` even
  when called with `includeExtract=false`.
