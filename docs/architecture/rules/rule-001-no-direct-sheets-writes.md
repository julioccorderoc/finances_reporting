# Rule 001 — No Direct Google Sheets Writes

**Source ADR:** [ADR-001](../../ADR/ADR-001-sqlite-source-of-truth.md)
**Scope:** All Python modules under `finances/`.

**Constraint:** Only `finances/reports/sheets_sync.py` may import `gspread` or any other Google Sheets client. No other module may write to Google Sheets. The Sheets workbook is a read-only mirror generated from SQL views; it is never the source of truth.

**Lint check (suggested):** `grep -rn "import gspread\|from gspread" finances/ | grep -v "^finances/reports/sheets_sync.py:"` must return empty.
