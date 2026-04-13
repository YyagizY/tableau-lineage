#!/usr/bin/env bash
# Story 5 — Claude Code Wrapper Script
#
# Usage:
#   ./analyze_report.sh <report_name> <sheet_name>
#
# Example:
#   ./analyze_report.sh CustomerDashboard Revenue

set -euo pipefail

REPORT="${1:-}"
SHEET="${2:-}"

if [[ -z "$REPORT" || -z "$SHEET" ]]; then
  echo "Usage: $0 <report_name> <sheet_name>"
  exit 1
fi

METADATA="$HOME/Desktop/tableau_fetch/$REPORT/$SHEET/metadata.json"

if [[ ! -f "$METADATA" ]]; then
  echo "Error: metadata not found at $METADATA"
  echo "Run first: python tableau_fetch.py --url \"<url>\" --client \"<client>\""
  exit 1
fi

# Read repo path from metadata (validated during fetch)
REPO_PATH=$(python3 -c "import json; d=json.load(open('$METADATA')); print(d.get('repo_path', ''))")

if [[ -z "$REPO_PATH" || ! -d "$REPO_PATH" ]]; then
  echo "Warning: pipeline repo not found — Claude will work from metadata only."
  REPO_PATH=""
fi

SYSTEM_PROMPT="$(cat <<EOF
You are a data lineage assistant helping non-technical managers understand Tableau reports.

Your context for this session:
- Report: $REPORT
- Sheet: $SHEET
- Metadata file: $METADATA
$(if [[ -n "$REPO_PATH" ]]; then echo "- Pipeline repo: $REPO_PATH"; fi)

Instructions:
1. Start by reading $METADATA and summarising the sheet structure in plain language.
2. Use the pipeline repo at $REPO_PATH to trace how the delta table is built. Reference specific files and line numbers.
3. Answer all questions in plain language suitable for non-technical managers.
4. When discussing pipeline logic, always cite the file and line number.
5. Flag any ambiguities or gaps in your confidence clearly.
6. Never display or echo PAT values or secrets — if you see them, redact immediately.
7. If asked something you cannot determine from the available files, say so clearly.

Begin with an automatic summary:
"I've loaded the metadata for $REPORT / $SHEET. Here's what I see: [X fields, Y calculated, delta table Z]. Ask me anything."
EOF
)"

echo "Launching Claude Code for $REPORT / $SHEET..."
echo "Metadata: $METADATA"
if [[ -n "$REPO_PATH" ]]; then
  echo "Repo:     $REPO_PATH"
fi
echo ""

claude --print "$SYSTEM_PROMPT"
