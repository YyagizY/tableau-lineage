# tableau-lineage

Lightweight utility that extracts Tableau Cloud report metadata, resolves the
underlying Databricks delta table via Unity Catalog, and produces a structured
`metadata.json` that Claude Code uses to answer manager questions about data lineage.

## Repo layout

```
tableau_fetch.py          # CLI entry point
analyze_report.sh         # Claude Code wrapper
tableau_fetch/
  __init__.py
  tableau.py              # Tableau Cloud Metadata Extraction + Column-Level Mapping
  databricks.py           # Delta Table Resolution
requirements.txt
.env.example
```

## Jira epic

Source of truth for stories and acceptance criteria: CXNAPB-112

## Running locally

```bash
# Install deps
pip install -r requirements.txt

# Set credentials (copy .env.example → .env and fill in values, then source it)
source .env

# Fetch metadata for a report/sheet
python tableau_fetch.py \
  --url "https://prod-uk.online.tableau.com/#/site/mysite/views/CustomerDashboard/Revenue" \
  --sheet "Revenue"

# Launch Claude Code analysis session
./analyze_report.sh CustomerDashboard Revenue
```

## Environment variables

See `.env.example` for required variables and required PAT scopes.
Never commit `.env` or real PAT values.

## Key design decisions

- Only dependency is `requests` (V1 constraint).
- All credentials come from env vars only — never written to JSON output.
- Column mapping uses normalised (lowercase, whitespace/underscore stripped) fuzzy matching
  as a fallback when exact match fails.
- `analyze_report.sh` launches `claude --print` with an inline system prompt grounding
  Claude in the metadata file and local repo path.
