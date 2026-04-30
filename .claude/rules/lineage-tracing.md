# Tracing a Tableau report back to its source code

The whole point of `lineage_enriched.json` is to let you answer "where does
this dashboard's data come from?" without spelunking. Two fields collapse
that question to one or two `gh` calls — use them directly instead of
searching for the company or the table name.

## `customer-name` is the GitHub repo slug, verbatim

`customer-name: customer-pipeline-fivebelow` →
`inventanalytics/customer-pipeline-fivebelow`. The matching analysis repo
follows the same convention: `inventanalytics/customer-analysis-<customer>`.

Never run `gh search repos "<customer>"` to find these — you'll get unrelated
public repos. Construct the slug from `customer-name` and go straight to the
repo.

## `storage_path` pins down the producing notebook in one search

A path like `dbfs:/mnt/invent-fivebelow-datastore/allocation/pg_data/weekly_pg`
appears in exactly two kinds of files: the notebook that *writes* the table
(the producer) and any notebooks that *read* it (downstream consumers). One
query nails it:

```bash
gh search code "<storage_path_fragment>" --owner inventanalytics
```

Do **not** search for the bare `delta_table` name (e.g. `weekly_pg`). It will
match unrelated repos that happen to use a similar name (alfamart, fastpoc,
tbretail, etc.). Always search the storage path.

### Fallback: trailing path components for `os.path.join`-built paths

A literal-path search will miss producers that assemble the path from
variables, which is common in Spark code:

```python
.save(os.path.join(root_path_dc_order_results, 'reporting', 'order_report'))
```

The full string `dc_rpl/reporting/order_report` never appears in the code —
only the trailing `'reporting'` and `'order_report'` literals do. When the
literal-path search returns one or zero results, fall back to:

1. **Search the trailing two components** as a path fragment, e.g.
   `gh search code "reporting/order_report" --owner inventanalytics`. May
   match config files or readers; useful for narrowing the producing repo.
2. **Walk the repo's directory tree** via `gh api repos/<org>/<repo>/contents/<path>`
   once you know which repo to look in. The directory layout typically mirrors
   the storage layout (e.g. `rocks_extension/dc_replenishment/dc_order_pipeline/`
   produces the `dc_rpl/reporting/*` tables).
3. **Read the orchestrator file** (often named `*_reporting.py` or similar)
   first — it usually contains the literal `'reporting'` / `'<table_name>'`
   string components and shows how the upstream parquet outputs map to the
   final Delta tables.

## Sanity-check, don't search

The `fields[]` column names (`inventory_f`, `sales_quantity`, …) are useful
for confirming you read the right file, not for finding it.

## Caveat: caption ≠ underlying table for Custom SQL datasources

`tableau_datasource_name` is the user-chosen label in Tableau, while
`delta_table` is the actual underlying table parsed from the SQL. For example,
DCKPIReport's `lost_sales_agg` datasource queries
`hive_metastore.fivebelow.kpi_reporting` (with a filter applied in SQL).
Trust `delta_table` / `storage_path`, not the caption.
