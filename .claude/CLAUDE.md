# tableau-lineage

Three-step pipeline that downloads a Tableau Cloud workbook, extracts its
lineage to JSON, and enriches each datasource with the Databricks storage
path resolved via the SQL Statement Execution API. The final JSON grounds
Claude Code in data lineage for manager questions.

## Repo layout

```
pipeline.py                       # Orchestrator — runs the three steps end-to-end
tableau_fetch/
  __init__.py
  download_workbook.py            # Step 1: Tableau URL → .twb via REST API + PAT
  twbx_lineage.py                 # Step 2: .twb/.twbx → lineage JSON
  enrich_with_paths.py            # Step 3: lineage JSON → enriched JSON (adds storage_path)
  twbx.py                         # .twbx/.twb XML parser used by twbx_lineage
requirements.txt
.env.example
```

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
python3 -m tableau_fetch.download_workbook  <url> [output.twb]
python3 -m tableau_fetch.twbx_lineage       <workbook.twb> [-o lineage.json]
python3 -m tableau_fetch.enrich_with_paths  [input.json] [output.json]
```

## Output shape

```json
{
  "customer-name": "customer-pipeline-fivebelow",
  "sheets": [
    {
      "workbook": "...",
      "sheet": "...",
      "datasource": {
        "tableau_datasource_name": "...",
        "delta_table": "hive_metastore",
        "storage_path": "/mnt/.../reporting/..."
      },
      "fields": [...]
    }
  ]
}
```

## Environment variables

See `.env.example`. Never commit `.env` or real PAT values.

## Key design decisions

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
