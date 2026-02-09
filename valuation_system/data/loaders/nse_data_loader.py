"""
NSE Data Loader

Provides access to NSE quarterly filing data as a supplementary data source.
Used by financial_processor to get latest quarters when core CSV is stale.

Priority: NSE data is checked FIRST for latest 2-3 quarters, then falls back to core CSV.
"""

import os
import logging
from typing import Optional, Dict
from datetime import datetime, timedelta

import pandas as pd
from dotenv import load_dotenv

logger = logging.getLogger(__name__)

load_dotenv(os.path.join(os.path.dirname(__file__), '..', '..', 'config', '.env'))

NSE_CSV_PATH = os.getenv(
    'NSE_QUARTERLY_CSV_PATH',
    os.path.join(os.path.dirname(__file__), '..', '..', 'nse_results_prototype', 'cache', 'nse_quarterly_data.csv')
)


class NSEDataLoader:
    """
    Loads NSE quarterly filing data from nse_quarterly_data.csv.
    Provides unified interface matching CoreDataLoader patterns.
    """

    def __init__(self):
        self.df = None
        self.csv_path = NSE_CSV_PATH
        self.loaded_at = None

        if os.path.exists(self.csv_path):
            self._load()
        else:
            logger.debug(f"NSE CSV not found at {self.csv_path} - NSE data unavailable")

    def _load(self):
        """Load NSE CSV."""
        try:
            self.df = pd.read_csv(self.csv_path, low_memory=False)
            self.loaded_at = datetime.now()
            logger.info(f"NSE data loaded: {len(self.df)} companies, {len(self.df.columns)} columns")
        except Exception as e:
            logger.error(f"Failed to load NSE CSV: {e}")
            self.df = None

    def is_available(self) -> bool:
        """Check if NSE data is available."""
        return self.df is not None and not self.df.empty

    def get_company_data(self, nse_symbol: str) -> Optional[pd.Series]:
        """
        Get all NSE data for a company by NSE symbol.

        Returns:
            pandas Series with all columns, or None if not found
        """
        if not self.is_available():
            return None

        matches = self.df[self.df['nse_symbol'] == nse_symbol]
        if len(matches) == 0:
            return None

        return matches.iloc[0]

    def get_metric_dict(self, company_data: pd.Series, metric: str) -> Dict[int, float]:
        """
        Extract a metric from NSE company data into quarter_idx → value dict.

        Args:
            company_data: pandas Series from get_company_data()
            metric: metric name (e.g., 'sales', 'pat', 'pbidt')

        Returns:
            Dict[quarter_idx, value] — e.g., {147: 1234.5, 148: 1456.7}
        """
        if company_data is None:
            return {}

        result = {}
        for col in company_data.index:
            if col.startswith(f'{metric}_') and col.split('_')[-1].isdigit():
                q_idx = int(col.split('_')[-1])
                val = company_data[col]
                if pd.notna(val):
                    result[q_idx] = float(val)

        return result

    def get_latest_quarter_idx(self, nse_symbol: str) -> Optional[int]:
        """
        Get the latest quarter index available for a company in NSE data.

        Returns:
            Quarter index (e.g., 151) or None
        """
        company_data = self.get_company_data(nse_symbol)
        if company_data is None:
            return None

        # Find all quarter indices in the data
        quarter_indices = []
        for col in company_data.index:
            if '_' in col and col.split('_')[-1].isdigit():
                parts = col.split('_')
                if len(parts) >= 2:
                    try:
                        q_idx = int(parts[-1])
                        quarter_indices.append(q_idx)
                    except ValueError:
                        continue

        return max(quarter_indices) if quarter_indices else None

    def has_newer_data_than_core(self, nse_symbol: str, core_latest_idx: int) -> bool:
        """
        Check if NSE has newer quarters than core CSV.

        Args:
            nse_symbol: NSE symbol
            core_latest_idx: Latest quarter index from core CSV

        Returns:
            True if NSE has data for quarters > core_latest_idx
        """
        nse_latest = self.get_latest_quarter_idx(nse_symbol)
        if nse_latest is None:
            return False

        return nse_latest > core_latest_idx

    def get_ttm(self, metric_dict: Dict[int, float]) -> Optional[float]:
        """
        Calculate TTM (trailing 12 months) from quarterly data.
        Sums the last 4 quarters.

        Args:
            metric_dict: Dict[quarter_idx, value] from get_metric_dict()

        Returns:
            TTM sum or None if < 4 quarters available
        """
        if not metric_dict or len(metric_dict) < 4:
            return None

        # Get last 4 quarters
        sorted_quarters = sorted(metric_dict.keys(), reverse=True)[:4]
        ttm = sum(metric_dict[q] for q in sorted_quarters)

        return ttm if ttm > 0 else None

    def get_latest_value(self, metric_dict: Dict[int, float]) -> Optional[float]:
        """
        Get the latest value from a metric dict.

        Args:
            metric_dict: Dict[quarter_idx, value]

        Returns:
            Latest value or None
        """
        if not metric_dict:
            return None

        latest_idx = max(metric_dict.keys())
        return metric_dict[latest_idx]


# =============================================================================
# HELPER: Merge NSE data into core financials dict
# =============================================================================

def merge_nse_into_financials(financials: Dict, nse_symbol: str, nse_loader: NSEDataLoader) -> Dict:
    """
    Merge NSE quarterly data into financials dict from CoreDataLoader.

    Strategy:
    - For each metric (sales, pat, pbidt, etc.), check if NSE has newer quarters
    - If yes, merge NSE quarters into the metric dict with [NSE_FILING] tag
    - Core CSV data is preserved, NSE data is additive

    Args:
        financials: Dict from CoreDataLoader.get_company_financials()
        nse_symbol: NSE symbol for the company
        nse_loader: NSEDataLoader instance

    Returns:
        Updated financials dict with NSE data merged in
    """
    if not nse_loader.is_available():
        return financials

    company_data = nse_loader.get_company_data(nse_symbol)
    if company_data is None:
        return financials

    # Metrics to merge
    METRICS = [
        'sales', 'pat', 'pbidt', 'interest', 'pbt_excp', 'other_income',
        'depreciation', 'empcost', 'rawmat', 'other_exp', 'total_exp', 'tax',
        'exceptional', 'paid_up_equity', 'basic_eps', 'diluted_eps'
    ]

    for metric in METRICS:
        # Get NSE data for this metric
        nse_data = nse_loader.get_metric_dict(company_data, metric)
        if not nse_data:
            continue

        # Find corresponding key in financials
        # Try metric_quarterly first, then metric, then metric_annual
        fin_key = None
        if f'{metric}_quarterly' in financials:
            fin_key = f'{metric}_quarterly'
        elif metric in financials:
            fin_key = metric
        elif f'{metric}_annual' in financials:
            fin_key = f'{metric}_annual'

        if fin_key is None:
            # Create new key
            fin_key = f'{metric}_quarterly'
            financials[fin_key] = {}

        # Merge: add NSE quarters that are newer than core CSV
        core_data = financials[fin_key]
        if not isinstance(core_data, dict):
            continue

        core_latest_idx = max(core_data.keys()) if core_data else 0

        for q_idx, value in nse_data.items():
            if q_idx > core_latest_idx:
                # NSE has newer data - add it
                core_data[q_idx] = value
                logger.debug(f"[NSE_FILING] {metric} Q{q_idx} = {value:.2f} (newer than core)")

    return financials
