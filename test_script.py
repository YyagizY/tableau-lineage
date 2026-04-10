# %% [markdown]
# # Tableau Lineage — Interactive Exploration
# Run cells one by one to debug each step.
# Set your credentials in the cell below before running.

# %% Config
import os
import subprocess
import sys

sys.path.insert(0, "/Users/omer/.claude/tableau-lineage")

# Pull env vars from the login shell (picks up ~/.zshrc exports)
_shell_env = subprocess.check_output(["zsh", "-c", "source ~/.zshrc && env"], text=True)
for _line in _shell_env.splitlines():
    if "=" in _line:
        _k, _, _v = _line.partition("=")
        os.environ.setdefault(_k, _v)

TABLEAU_URL = "https://us-east-1.online.tableau.com/#/site/invent-us/views/DCNeedReport/WeeklyView"

# Verify credentials are present
print(f"TABLEAU_PAT_NAME   : {os.environ.get('TABLEAU_PAT_NAME', 'NOT SET')}")
print(f"TABLEAU_PAT_SECRET : {'*' * 8 if os.environ.get('TABLEAU_PAT_SECRET') else 'NOT SET'}")

# %% Step 1 — Parse URL
from tableau_fetch.tableau import parse_url

parsed = parse_url(TABLEAU_URL)
print(f"pod      : {parsed.pod}")
print(f"site     : {parsed.site}")
print(f"workbook : {parsed.workbook}")
print(f"view     : {parsed.view}")

# %% Step 2 — Authenticate with Tableau Cloud
from tableau_fetch.tableau import authenticate

auth = authenticate(parsed)
print(f"token    : {auth.token[:10]}...")
print(f"site_id  : {auth.site_id}")
print(f"base_url : {auth.base_url}")

# %% Step 3 — Resolve names and fetch fields
from tableau_fetch.tableau import _graphql, _resolve_workbook_name, resolve_sheet_name, _build_fields_query

workbook_name = _resolve_workbook_name(auth, parsed.workbook)
sheet_name = resolve_sheet_name(auth, parsed.workbook, parsed.view)
print(f"Workbook : {workbook_name!r}")
print(f"Sheet    : {sheet_name!r}")

raw = _graphql(auth, _build_fields_query(workbook_name), {})
workbooks = raw.get("workbooksConnection", {}).get("nodes", [])
wb_node = workbooks[0]
all_views = (
    wb_node.get("sheetsConnection", {}).get("nodes", [])
    + wb_node.get("dashboardsConnection", {}).get("nodes", [])
)
matched = next((v for v in all_views if v["name"] == sheet_name), None)
field_nodes = (matched.get("fieldsConnection") or {}).get("nodes", [])
print(f"Field nodes : {len(field_nodes)}")
for n in field_nodes:
    print(f"  {n.get('__typename', '?'):20s}  {n.get('name')}")

#%%
sample = next((n for n in field_nodes if n.get("__typename") == "ColumnField"), None)
print(sample) 

#%%
import json                                                                                                                                                            
print(json.dumps(matched, indent=2))  

# %% Step 4 — Parse fields into structured FieldInfo + DatasourceInfo
from tableau_fetch.tableau import _parse_fields

fields, datasource = _parse_fields(field_nodes)
print(f"Datasource: {datasource}")
print(f"\nFields ({len(fields)} total):")
for f in fields:
    print(f"  [{f.field_type:12s}]  display={f.display_name!r:30s}  col={f.name!r}")





# %% Step 5 — Full fetch_sheet_metadata (combines all steps above)
from tableau_fetch.tableau import fetch_sheet_metadata

meta = fetch_sheet_metadata(TABLEAU_URL)
print(f"workbook   : {meta.workbook}")
print(f"sheet      : {meta.sheet}")
print(f"datasource : {meta.datasource.database}.{meta.datasource.schema}.{meta.datasource.table}")
print(f"fields     : {len(meta.fields)}")

# %% Step 6 — Preview the JSON output (Databricks skipped)
import json
from datetime import datetime, timezone

output = {
    "report": meta.workbook,
    "sheet": meta.sheet,
    "extracted_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
    "delta_table": "(skipped — no Databricks PAT)",
    "columns": [
        {
            "displayed_name": f.display_name,
            "original_column": f.name if f.name != f.display_name else None,
            "is_calculated": f.field_type == "calculated",
            **({"formula": f.formula} if f.field_type == "calculated" else {}),
        }
        for f in meta.fields
    ],
}
print(json.dumps(output, indent=2))

# %%