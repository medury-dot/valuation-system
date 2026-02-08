# Valuation System - Project Instructions

## Data Guidelines
- **Trailing 12 Months (TTM)**: When recent half-yearly data for balancesheet items (such a debt) or cashflows ia  is available, use it to compute TTM figures rather than relying solely on older annual data. For example for cashflows, h2_2025 + h1_2026 gives a more current TTM (Oct 2024 - Sep 2025) than the yearly FY2025 figure (Apr 2024 - Mar 2025). For quarterly items, use latest last four quarters for TTM such as sales, pbidt, pat.

- **Indian Fiscal Year**: Year numbers in yearly and half-yearly metrics are Indian fiscal years ending March. FY2025 = Apr 2024 - Mar 2025. h1 = Apr-Sep, h2 = Oct-Mar.
- **Column Name Case Sensitivity**: Yearly `trade_payables` (lowercase), `acc_dep` (no 'r'). Half-yearly `Trade_payables` (Title case), `acc_depr` (with 'r'). Always verify exact column names.
- **3-Tier Fallback**: ACTUAL data first, then DERIVED estimates, then DEFAULT values. Log source with [ACTUAL]/[DERIVED]/[DEFAULT] tags.
- **Core CSV**: ~4000 columns, ~9000 companies. Use `low_memory=False` when loading with pandas.

## Architecture
- Valuation blend: DCF 60%, Relative 30%, Monte Carlo 10%
- Terminal growth: ROCE-linked (g = Reinvestment Rate x ROCE), capped 2%-5%
- Two-tier peer selection: tight (same industry, 2x weight) + broad (same sector, 1x weight)

## Config
- All settings in `.env` file — no hardcoding
- MySQL: root@localhost:3306/rag
- ChromaDB: localhost:8001

❯ for sales figure use the sales_* columns (dont see <year>_sales column); use them to calculate TTM sales. same for pbidt, pat also.               
