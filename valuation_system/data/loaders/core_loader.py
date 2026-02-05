"""
Core Data Loader - Extract financial data from core-all-input CSV.
Maps index-based, year-based, and half-yearly columns to structured company financials.

Column conventions in core CSV:
  1. Index-based (QUARTERLY): sales_128, pbidt_132, op_profit_151, etc.
     - Formula: FY = 1989 + (index-1)//4, Q = (index-1)%4 + 1
     - Q1=Apr-Jun, Q2=Jul-Sep, Q3=Oct-Dec, Q4=Jan-Mar
     - 128=FY2020Q4(Mar2020), 132=FY2021Q4, 148=FY2025Q4, 151=FY2026Q3
  2. Year-based (ANNUAL): 2024_sales, 2024_debt, 2024_roce, etc.
     - Year = Indian fiscal year ending March (FY2024 = Apr 2023 - Mar 2024)
  3. Half-yearly: h1_2024_cash_and_bank, h2_2024_acc_depr, etc.
     - h1 = H1 of fiscal year (Apr-Sep), h2 = H2 of fiscal year (Oct-Mar)
     - h2 corresponds to March year-end balance sheet date

Some metrics exist ONLY as quarterly (empcost, opex, genadminexp, totalexp, pbdt).
Some exist as BOTH quarterly and annual (sales, pbidt, pat, interest, pbt_excp, op_profit).
Some exist ONLY as annual (debt, networth, roce, roe, pur_of_fixed_assets, acc_dep).
Some exist ONLY as half-yearly (cash_and_bank).
"""

import os
import logging
from typing import Optional, Tuple

import numpy as np
import pandas as pd
from dotenv import load_dotenv

logger = logging.getLogger(__name__)

load_dotenv(os.path.join(os.path.dirname(__file__), '..', '..', 'config', '.env'))


class CoreDataLoader:
    """
    Load and process the core-all-input CSV for valuation.
    Handles three column naming conventions:
      1. Index-based (QUARTERLY): sales_128 → quarterly data
      2. Year-based (ANNUAL): 2024_debt → annual fiscal year data
      3. Half-yearly: h1_2024_cash_and_bank, h2_2024_acc_depr

    All year numbers are Indian fiscal years ending March.
    FY2024 = April 2023 to March 2024.

    Quarter index formula:
      FY = 1989 + (index - 1) // 4
      Q  = (index - 1) % 4 + 1
      Q1=Apr-Jun, Q2=Jul-Sep, Q3=Oct-Dec, Q4=Jan-Mar
    """

    def __init__(self, csv_path: str = None):
        self.csv_path = csv_path or self._resolve_csv_path()
        if not self.csv_path:
            raise ValueError("CORE_CSV_PATH not set in .env and no CSV found in CORE_CSV_DIR")

        self._df = None
        self._column_index_cache = {}  # {prefix: max_index}
        logger.info(f"CoreDataLoader initialized with: {self.csv_path}")

    @staticmethod
    def _resolve_csv_path() -> Optional[str]:
        """Resolve CSV path: use CORE_CSV_PATH if set, else find latest in CORE_CSV_DIR."""
        explicit_path = os.getenv('CORE_CSV_PATH', '').strip()
        if explicit_path and os.path.isfile(explicit_path):
            return explicit_path

        # Auto-detect latest CSV from directory
        csv_dir = os.getenv('CORE_CSV_DIR', '').strip()
        if not csv_dir:
            # Derive directory from CORE_CSV_PATH if it's a file path
            if explicit_path:
                csv_dir = os.path.dirname(explicit_path)
            else:
                return None

        if not os.path.isdir(csv_dir):
            return None

        import glob
        csv_files = glob.glob(os.path.join(csv_dir, 'core-all-input-*-latest-final.csv'))
        if not csv_files:
            logger.warning(f"No core CSV files found in {csv_dir}")
            return None

        # Sort by modification time, pick latest
        latest = max(csv_files, key=os.path.getmtime)
        logger.info(f"Auto-detected latest core CSV: {os.path.basename(latest)}")
        return latest

    # =========================================================================
    # QUARTER INDEX HELPERS
    # =========================================================================

    @staticmethod
    def index_to_quarter(idx: int) -> Tuple[int, int]:
        """Convert period index to (fiscal_year, quarter_number).
        Q1=Apr-Jun, Q2=Jul-Sep, Q3=Oct-Dec, Q4=Jan-Mar."""
        fy = 1989 + (idx - 1) // 4
        q = (idx - 1) % 4 + 1
        return (fy, q)

    @staticmethod
    def quarter_to_index(fy: int, q: int) -> int:
        """Convert (fiscal_year, quarter) to period index."""
        return 1 + (fy - 1989) * 4 + (q - 1)

    @staticmethod
    def index_to_fiscal_year(idx: int) -> int:
        """Get fiscal year for a quarter index."""
        return 1989 + (idx - 1) // 4

    @staticmethod
    def index_to_label(idx: int) -> str:
        """Human-readable label: 'FY2024Q4 (Mar 2024)'."""
        fy, q = CoreDataLoader.index_to_quarter(idx)
        q_months = {1: 'Jun', 2: 'Sep', 3: 'Dec', 4: 'Mar'}
        cal_year = fy if q == 4 else fy - 1
        return f"FY{fy}Q{q} ({q_months[q]} {cal_year})"

    # =========================================================================
    # CSV LOADING
    # =========================================================================

    @property
    def df(self) -> pd.DataFrame:
        """Lazy load the CSV."""
        if self._df is None:
            logger.info(f"Loading core CSV: {self.csv_path}")
            self._df = pd.read_csv(self.csv_path, low_memory=False)
            logger.info(f"Loaded {len(self._df)} companies, {len(self._df.columns)} columns")
        return self._df

    def _find_max_index(self, prefix: str) -> Optional[int]:
        """Find the highest period index for a column prefix (e.g., 'sales' → 150)."""
        if prefix in self._column_index_cache:
            return self._column_index_cache[prefix]

        max_idx = None
        for col in self.df.columns:
            if col.startswith(f'{prefix}_'):
                suffix = col[len(prefix) + 1:]
                if suffix.isdigit():
                    idx = int(suffix)
                    if max_idx is None or idx > max_idx:
                        max_idx = idx

        self._column_index_cache[prefix] = max_idx
        return max_idx

    # =========================================================================
    # DATA EXTRACTION
    # =========================================================================

    def get_company_financials(self, company_name: str) -> dict:
        """
        Extract all financial data for a company.
        Returns structured dict with time-series and point-in-time data.
        """
        matches = self.df[self.df['Company Name'] == company_name]
        if matches.empty:
            matches = self.df[self.df['Company Name'].str.contains(
                company_name, case=False, na=False)]

        if matches.empty:
            raise ValueError(f"Company not found: {company_name}")

        if len(matches) > 1:
            logger.warning(f"Multiple matches for '{company_name}', "
                           f"using first: {matches.iloc[0]['Company Name']}")

        row = matches.iloc[0]

        return {
            # Identification
            'company_name': row['Company Name'],
            'sector': row.get('CD_Sector', ''),
            'industry': row.get('CD_Industry1', ''),
            'nse_symbol': row.get('CD_NSE Symbol1', ''),
            'bse_code': str(row.get('CD_BSE Code', '')),
            'isin': row.get('CD_ISIN No', ''),
            'chairman': row.get('CD_Chairman', ''),
            'auditor': row.get('CD_Auditor', ''),

            # Income Statement — ANNUAL (year-based, preferred for DCF)
            'sales_annual': self._extract_year_series(row, 'sales'),
            'pbidt_annual': self._extract_year_series(row, 'pbidt'),
            'pat_annual': self._extract_year_series(row, 'pat'),
            'total_income': self._extract_year_series(row, 'totalincome'),
            'net_profit': self._extract_year_series(row, 'netprofit'),

            # Income Statement — QUARTERLY (index-based)
            'sales_quarterly': self._extract_quarterly_series(row, 'sales'),
            'pbidt_quarterly': self._extract_quarterly_series(row, 'pbidt'),
            'pat_quarterly': self._extract_quarterly_series(row, 'pat'),
            'op_profit_quarterly': self._extract_quarterly_series(row, 'op_profit'),

            # P&L Detail — QUARTERLY only (no year-based columns exist)
            'employee_cost_quarterly': self._extract_quarterly_series(row, 'empcost'),
            'operating_expenses_quarterly': self._extract_quarterly_series(row, 'opex'),
            'gen_admin_expenses_quarterly': self._extract_quarterly_series(row, 'genadminexp'),
            'total_expenditure_quarterly': self._extract_quarterly_series(row, 'totalexp'),
            'interest_quarterly': self._extract_quarterly_series(row, 'interest'),
            'pbdt_quarterly': self._extract_quarterly_series(row, 'pbdt'),
            'pbt_excp_quarterly': self._extract_quarterly_series(row, 'pbt_excp'),

            # Balance Sheet (year-based annual)
            'debt': self._extract_year_series(row, 'debt'),
            'networth': self._extract_year_series(row, 'networth'),
            'total_assets': self._extract_year_series(row, 'totalassets'),
            'gross_block': self._extract_year_series(row, 'grsblk'),
            'net_block': self._extract_year_series(row, 'netblk'),
            'cwip': self._extract_year_series(row, 'cwip'),
            'lt_borrowings': self._extract_year_series(row, 'LT_borrow'),
            'trade_payables': self._extract_year_series(row, 'trade_payables'),
            'accumulated_depreciation': self._extract_year_series(row, 'acc_dep'),
            'inventories': self._extract_year_series(row, 'inventories'),
            'sundry_debtors': self._extract_year_series(row, 'sundrydebtors'),
            'share_capital': self._extract_year_series(row, 'share_capital'),
            'total_reserves': self._extract_year_series(row, 'total_reserves'),

            # Actual Capex & Asset Sales (year-based annual)
            'pur_of_fixed_assets': self._extract_year_series(row, 'pur_of_fixed_assets'),
            'sale_of_fixed_assets': self._extract_year_series(row, 'sale_of_fixed_assets'),

            # Actual Accumulated Depreciation — yearly column is 'acc_dep' (no 'r')
            'acc_dep_yearly': self._extract_year_series(row, 'acc_dep'),

            # Cash Flow — QUARTERLY (index-based)
            'cfo_quarterly': self._extract_quarterly_series(row, 'cashflow_ops'),
            'cfi_quarterly': self._extract_quarterly_series(row, 'cashflow_investing'),
            'capex_quarterly': self._extract_quarterly_series(
                row, 'cashflow_purchase_fixedassets'),
            'asset_sales_quarterly': self._extract_quarterly_series(
                row, 'cashflow_sale_fixedassets'),

            # Cash Flow — YEARLY (actual annual totals, not derived from quarterly)
            'cashflow_ops_yearly': self._extract_year_series(row, 'cashflow_ops'),
            'cashflow_investing_yearly': self._extract_year_series(row, 'cashflow_investing'),
            'cashflow_financing_yearly': self._extract_year_series(row, 'cashflow_financing'),

            # Actual P&L Items — YEARLY
            'pbt_excp_yearly': self._extract_year_series(row, 'pbt_excp'),
            'interest_yearly': self._extract_year_series(row, 'interest'),
            'op_profit_yearly': self._extract_year_series(row, 'op_profit'),

            # FCF per share (yearly, limited: recent years only)
            'fcf_per_share_yearly': self._extract_year_series(row, 'fcf_per_share'),

            # Half-yearly data (h1=Apr-Sep, h2=Oct-Mar of fiscal year)
            'cash_and_bank_hy': self._extract_halfyearly_series(row, 'cash_and_bank'),
            'inventories_hy': self._extract_halfyearly_series(row, 'inventories'),
            'fixed_assets_hy': self._extract_halfyearly_series(row, 'fixed_assets'),
            'tot_assets_hy': self._extract_halfyearly_series(row, 'tot_assets'),

            # Profitability Ratios (year-based annual)
            'roe': self._extract_year_series(row, 'roe'),
            'roce': self._extract_year_series(row, 'roce'),
            'roa': self._extract_year_series(row, 'roa'),
            'gpm': self._extract_year_series(row, 'gpm'),
            'ebidtm': self._extract_year_series(row, 'ebidtm'),
            'pbidtm': self._extract_year_series(row, 'pbidtm'),
            'patm': self._extract_year_series(row, 'patm'),
            'ptm': self._extract_year_series(row, 'ptm'),

            # Efficiency Ratios (year-based annual)
            'inventory_turnover': self._extract_year_series(row, 'inv_tr'),
            'receivable_days': self._extract_year_series(row, 'recv_days'),
            'inventory_days': self._extract_year_series(row, 'inv_days'),
            'payable_days': self._extract_year_series(row, 'paybl_days'),
            'debtor_turnover': self._extract_year_series(row, 'debtor_tr'),
            'cash_conversion_cycle': self._extract_year_series(row, 'ccc'),

            # Leverage
            'debt_mcap_ratio': self._extract_year_series(row, 'debt_mcap_ratio'),

            # Market Cap — QUARTERLY (point-in-time snapshots)
            'market_cap_quarterly': self._extract_quarterly_series(
                row, 'actualmcap_in_crores'),

            # Promoter Holding — QUARTERLY (point-in-time)
            'promoter_holding_quarterly': self._extract_quarterly_series(row, 'promoter'),
            'promoter_pledged_quarterly': self._extract_quarterly_series(
                row, 'promoter', suffix='_pledged'),

            # Cashflow ratios (year-based)
            'sales_cashflow_ratio': self._safe_get_year(row, 'sales_cashflow_ratio', 2024),

            # Banking specific (year-based)
            'gnpa': self._extract_year_series(row, 'gnpa'),
            'nnpa': self._extract_year_series(row, 'nnpa'),

            # === BACKWARD COMPAT ALIASES (use annual where available) ===
            'sales': self._extract_year_series(row, 'sales'),
            'pbidt': self._extract_year_series(row, 'pbidt'),
            'pat': self._extract_year_series(row, 'pat'),
            'op_profit': self._extract_quarterly_series(row, 'op_profit'),
            'cfo': self._annualize_quarterly(
                self._extract_quarterly_series(row, 'cashflow_ops')),
            'cfi': self._annualize_quarterly(
                self._extract_quarterly_series(row, 'cashflow_investing')),
            'capex': self._annualize_quarterly(
                self._extract_quarterly_series(row, 'cashflow_purchase_fixedassets')),
            'asset_sales': self._annualize_quarterly(
                self._extract_quarterly_series(row, 'cashflow_sale_fixedassets')),
            'interest_expense': self._annualize_quarterly(
                self._extract_quarterly_series(row, 'interest')),
            'employee_cost': self._annualize_quarterly(
                self._extract_quarterly_series(row, 'empcost')),
            'operating_expenses': self._annualize_quarterly(
                self._extract_quarterly_series(row, 'opex')),
            'pbt_excl_exceptional': self._annualize_quarterly(
                self._extract_quarterly_series(row, 'pbt_excp')),
            'market_cap': self._extract_quarterly_series(row, 'actualmcap_in_crores'),
            'promoter_holding': self._extract_quarterly_series(row, 'promoter'),
            'promoter_pledged': self._extract_quarterly_series(
                row, 'promoter', suffix='_pledged'),
        }

    def _extract_quarterly_series(self, row: pd.Series, prefix: str,
                                   suffix: str = '',
                                   num_quarters: int = 12) -> dict:
        """
        Extract recent quarterly data from index-based columns.
        Returns {index: value} for the most recent num_quarters.
        """
        max_idx = self._find_max_index(prefix + suffix if suffix else prefix)
        if max_idx is None:
            # Try scanning for this specific prefix
            max_idx_found = None
            for col in self.df.columns:
                full_prefix = f'{prefix}_'
                if suffix:
                    # Pattern: prefix_NNN_suffix
                    if not col.startswith(full_prefix):
                        continue
                    rest = col[len(full_prefix):]
                    if rest.endswith(suffix):
                        num_part = rest[:-len(suffix)]
                        if num_part.isdigit():
                            idx = int(num_part)
                            if max_idx_found is None or idx > max_idx_found:
                                max_idx_found = idx
                else:
                    if col.startswith(full_prefix):
                        num_part = col[len(full_prefix):]
                        if num_part.isdigit():
                            idx = int(num_part)
                            if max_idx_found is None or idx > max_idx_found:
                                max_idx_found = idx
            max_idx = max_idx_found

        if max_idx is None:
            return {}

        result = {}
        start_idx = max(max_idx - num_quarters + 1, 1)
        for idx in range(start_idx, max_idx + 1):
            col = f"{prefix}_{idx}{suffix}"
            if col in row.index:
                val = row[col]
                if pd.notna(val):
                    result[idx] = float(val)

        return result

    def _annualize_quarterly(self, quarterly_series: dict) -> dict:
        """
        Convert quarterly index-based series to annual fiscal-year-keyed dict.
        Groups by fiscal year and sums 4 quarters.
        Only includes complete fiscal years (all 4 quarters present).

        Returns: {fiscal_year: annual_sum}
        """
        if not quarterly_series:
            return {}

        # Group by fiscal year
        fy_groups = {}
        for idx, val in quarterly_series.items():
            fy = self.index_to_fiscal_year(idx)
            if fy not in fy_groups:
                fy_groups[fy] = {}
            _, q = self.index_to_quarter(idx)
            fy_groups[fy][q] = val

        # Sum complete fiscal years
        result = {}
        for fy, quarters in sorted(fy_groups.items()):
            if len(quarters) == 4:
                result[fy] = round(sum(quarters.values()), 2)

        return result

    def _extract_year_series(self, row: pd.Series, metric: str,
                              years: range = None) -> dict:
        """Extract time series from year-based columns (e.g., 2024_debt)."""
        if years is None:
            years = range(2015, 2027)

        result = {}
        for year in years:
            col = f"{year}_{metric}"
            if col in row.index:
                val = row[col]
                if pd.notna(val):
                    result[year] = float(val)
        return result

    def _extract_halfyearly_series(self, row: pd.Series, metric: str,
                                      years: range = None) -> dict:
        """
        Extract half-yearly data from h1_{year}_{metric} and h2_{year}_{metric} columns.
        Returns: {(year, 1): value, (year, 2): value, ...}
        where 1=H1 (Apr-Sep) and 2=H2 (Oct-Mar) of the Indian fiscal year.
        """
        if years is None:
            years = range(2010, 2027)

        result = {}
        for year in years:
            for half in (1, 2):
                col = f"h{half}_{year}_{metric}"
                if col in row.index:
                    val = row[col]
                    if pd.notna(val):
                        result[(year, half)] = float(val)
        return result

    def get_latest_halfyearly(self, hy_series: dict) -> Optional[float]:
        """Get most recent value from a half-yearly series keyed by (year, half)."""
        if not hy_series:
            return None
        latest_key = max(hy_series.keys())
        return hy_series[latest_key]

    def _safe_get_year(self, row: pd.Series, metric: str, year: int):
        """Safely get a year-based metric."""
        col = f"{year}_{metric}"
        if col in row.index:
            val = row[col]
            return float(val) if pd.notna(val) else None
        return None

    # =========================================================================
    # QUARTERLY AGGREGATION HELPERS
    # =========================================================================

    def get_ttm(self, quarterly_series: dict) -> Optional[float]:
        """Sum the last 4 quarters for Trailing Twelve Months."""
        if not quarterly_series:
            return None
        sorted_indices = sorted(quarterly_series.keys())
        if len(sorted_indices) < 4:
            return None
        last_4 = sorted_indices[-4:]
        return round(sum(quarterly_series[idx] for idx in last_4), 2)

    def get_latest_quarterly(self, quarterly_series: dict) -> Optional[float]:
        """Get most recent quarterly value."""
        if not quarterly_series:
            return None
        latest_idx = max(quarterly_series.keys())
        return quarterly_series[latest_idx]

    # =========================================================================
    # SECTOR & COMPUTED HELPERS
    # =========================================================================

    def get_company_name_by_symbol(self, nse_symbol: str) -> Optional[str]:
        """Look up Company Name in core CSV by NSE symbol."""
        matches = self.df[self.df['CD_NSE Symbol1'] == nse_symbol]
        if matches.empty:
            return None
        return matches.iloc[0]['Company Name']

    def get_financials_by_symbol(self, nse_symbol: str) -> Optional[dict]:
        """Get company financials by NSE symbol (for peer lookup)."""
        name = self.get_company_name_by_symbol(nse_symbol)
        if not name:
            return None
        try:
            return self.get_company_financials(name)
        except ValueError:
            return None

    def get_sector_peers(self, sector: str, top_n: int = 15) -> list:
        """Get top companies in sector by market cap."""
        sector_df = self.df[self.df['CD_Sector'] == sector].copy()

        # Find latest available market cap column
        max_mcap_idx = self._find_max_index('actualmcap_in_crores')
        mcap_col = f'actualmcap_in_crores_{max_mcap_idx}' if max_mcap_idx else None

        if mcap_col and mcap_col in sector_df.columns:
            sector_df = sector_df.dropna(subset=[mcap_col])
            sector_df = sector_df.sort_values(mcap_col, ascending=False)

        return sector_df.head(top_n)['Company Name'].tolist()

    def calculate_cagr(self, series: dict, years: int = 5) -> Optional[float]:
        """Calculate CAGR from a time-series dict (year-keyed or index-keyed)."""
        sorted_keys = sorted(series.keys())
        if len(sorted_keys) < 2:
            return None

        end_key = sorted_keys[-1]
        start_key = max(sorted_keys[0], end_key - years)

        start_val = series.get(start_key)
        end_val = series.get(end_key)

        if not start_val or not end_val or start_val <= 0 or end_val <= 0:
            return None

        n = end_key - start_key
        if n <= 0:
            return None

        return (end_val / start_val) ** (1 / n) - 1

    def calculate_average(self, series: dict, years: int = 5) -> Optional[float]:
        """Calculate average of last N entries from a time-series dict."""
        sorted_keys = sorted(series.keys())
        recent = sorted_keys[-years:] if len(sorted_keys) >= years else sorted_keys

        values = [series[k] for k in recent if series.get(k) is not None]
        if not values:
            return None

        return np.mean(values)

    def get_latest_value(self, series: dict) -> Optional[float]:
        """Get most recent value from a time-series dict."""
        if not series:
            return None
        latest_key = max(series.keys())
        return series[latest_key]
