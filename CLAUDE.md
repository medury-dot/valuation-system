# Valuation System - Project Instructions

## Data Guidelines
- **Trailing 12 Months (TTM)**: When recent half-yearly data for balancesheet items (such a debt) or cashflows ia  is available, use it to compute TTM figures rather than relying solely on older annual data. For example for cashflows, h2_2025 + h1_2026 gives a more current TTM (Oct 2024 - Sep 2025) than the yearly FY2025 figure (Apr 2024 - Mar 2025). For quarterly items, use latest last four quarters for TTM such as sales, pbidt, pat.

- **Indian Fiscal Year**: Year numbers in yearly and half-yearly metrics are Indian fiscal years ending March. FY2025 = Apr 2024 - Mar 2025. h1 = Apr-Sep, h2 = Oct-Mar.
- **Column Name Case Sensitivity**: Yearly `trade_payables` (lowercase), `acc_dep` (no 'r'). Half-yearly `Trade_payables` (Title case), `acc_depr` (with 'r'). Always verify exact column names.
- **3-Tier Fallback**: ACTUAL data first, then DERIVED estimates, then DEFAULT values. Log source with [ACTUAL]/[DERIVED]/[DEFAULT] tags.
- **Core CSV**: ~4000 columns, ~9000 companies. Use `low_memory=False` when loading with pandas.

## Sector Taxonomy
- **Use `valuation_group` and `valuation_subgroup` columns** for all sector/industry classification. Ignore `cd_sector` and `cd_industry1` columns — they are legacy Screener.in fields, not our taxonomy.

## Architecture
- Valuation blend: DCF 60%, Relative 30%, Monte Carlo 10%
- Terminal growth: ROCE-linked (g = Reinvestment Rate x ROCE), capped 2%-5%
- Two-tier peer selection: tight (same industry, 2x weight) + broad (same sector, 1x weight)

## Excel Formula Parity
- **All valuation math in Python MUST have a corresponding change in Excel formulas.** The Excel workbook is the auditable artifact — if Python computes it, Excel must show it as a formula, not a hardcoded value.
- Terminal Value section uses NOPAT-based FCFF (5 formula rows: Revenue → EBITDA → Dep → NOPAT → FCFF)
- Margin improvement is dampened: full → 0 over projection period (prevents unrealistic linear expansion)

## Valuation Pipeline
- **All valuation runs MUST go through `utils/batch_valuation.py`** — never call DCF/financial_processor directly from ad-hoc scripts. The batch pipeline handles: database saves, Google Sheets updates (Recent Activity tab), correct valuation_subgroup passing, and Excel report generation. Ad-hoc scripts bypass these integrations and cause silent data gaps.

## External API / Rate Limit Policy
- **Never hit external APIs unnecessarily** during batch runs. Use local data files first (macro CSVs, cached JSONs). External APIs (yfinance, FRED, etc.) should only be called for initial data population scripts, not during per-company valuation loops.
- **Risk-free rate**: Read from `market_indicators.csv` (MACRO_DATA_PATH in .env), not from yfinance API. The 10Y bond yield from FRED is updated monthly and stored locally.
- **Google Sheets writes**: Always batch in chunks of 100 rows with a 1-second pause between batches to avoid rate limits. Never write 1000+ rows in a single API call.

## Batch Run Error Logging
- **Every batch job MUST produce an issues CSV** at `valuation_system/logs/batch_issues_YYYYMMDD_HHMM.csv` capturing all WARNING and ERROR level logs.
- **CSV columns**: `timestamp, symbol, company_name, valuation_group, valuation_subgroup, level, logger, message, traceback`
- The issues CSV path and summary counts must be printed at the end of every batch run.
- This enables post-run analysis of which companies have data issues, missing prices, negative valuations, etc.

## Driver Weights
- **Weights within each grouping MUST sum to 100%** and be displayed as percentage format (e.g., "12.0%" not 0.12).
- Tab 1 (Macro): All 23 MACRO drivers sum to 100%
- Tab 2 (Group): Drivers within each `valuation_group` sum to 100%
- Tab 3 (Subgroup): Drivers within each `valuation_subgroup` sum to 100%
- Tab 4 (Company): Drivers within each `company_id` sum to 100%
- When adding new drivers or changing weights, normalize so the group total remains 100%.

## Config
- All settings in `.env` file — no hardcoding
- MySQL: root@localhost:3306/rag
- ChromaDB: localhost:8001

❯ for sales figure use the sales_* columns (dont see <year>_sales column); use them to calculate TTM sales. same for pbidt, pat also.

## Input Data Column Inventory
- **Column Tracking CSV**: See `valuation_system/docs/input_data_columns.csv` for the complete list of all available data columns, their frequency, naming patterns, source (core CSV / fullstats / proposed), usage status, and driver mappings.
- **Data Source Tracking**: `financial_processor.py` tracks which key inputs use actual vs estimated data via `_data_sources` dict (keys: nwc, shares, tax, roce, reinvest). Values: ACTUAL_CF, ACTUAL_COLUMN, ACTUAL_CE, ACTUAL_PAYOUT, DERIVED_BS, DERIVED_MCAP, DERIVED_ACCRUAL, DERIVED_NWDEBT, DERIVED_CAPEX, DEFAULT. These are saved to `vs_valuation_snapshots` and shown in Excel Assumptions sheet column D.
- **TIER 1 New Columns** (6 from Accord, to be added to fullstats): cf_wc_change, shares_outstanding, capital_employed, dividend_payout_ratio, cf_tax_paid, rd_pct_of_sales. Code is ready — columns loaded in core_loader.py, fallback logic in financial_processor.py. Will activate when data appears in fullstats.
