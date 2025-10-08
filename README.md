# Carbox Inventory Monitor

This repo scrapes **https://www.carboxautosales.com/inventory/** daily with Playwright, captures all vehicle detail pages (JS-rendered), extracts **Year / Make / Model / VIN**, and diffs vs the previous day.

## Outputs
- `reports/inventory_YYYY-MM-DD.csv` — full snapshot (Year, Make, Model, VIN, URL)
- `reports/added_by_group_YYYY-MM-DD.csv` — **added** counts by Year/Make/Model with VINs
- `reports/removed_by_group_YYYY-MM-DD.csv` — **removed** counts by Year/Make/Model with VINs
- `reports/delta_YYYY-MM-DD.csv` — record-level adds/removes with URLs

## Quick start
1. Create a new GitHub repo and upload these files.
2. In the repo, go to **Actions** tab and enable workflows if prompted.
3. The workflow runs daily at **7:30 AM America/New_York** (11:30 UTC). You can also run it immediately via **Actions → Daily Carbox Inventory → Run workflow**.

## Local test
```bash
python -m venv .venv && . .venv/bin/activate
pip install -r requirements.txt
python -m playwright install --with-deps chromium
python scrape_carbox.py
ls reports/
```
Generated on 2025-10-08T14:15:49.427013 UTC.
