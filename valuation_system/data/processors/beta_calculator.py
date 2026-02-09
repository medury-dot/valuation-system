"""
Subgroup Beta Calculator
Computes unlevered betas per valuation_subgroup from Indian market data.

Methodology (adapted from Damodaran):
1. Load prices for all active companies + market proxy
   - Weekly mode: NIFTY index from combined_weekly_prices.csv (261 weeks, Feb 2021-Feb 2026)
   - Monthly mode: NIFTYBEES ETF from combined_monthly_prices.csv (290+ months)
2. Compute returns (weekly or monthly) for each company and market
3. Compute TWO regression betas per company:
   a. 2-year window — captures recent risk profile
   b. 5-year window — captures longer-term structural risk
4. Blend: company_beta = (2/3) × 2yr_beta + (1/3) × 5yr_beta
   - Matches Damodaran's exact methodology (weekly returns, dual window)
5. De-lever using company D/E ratio: beta_unlev = beta_lev / (1 + (1-t) × D/E)
6. Aggregate SIMPLE AVERAGE unlevered beta per valuation_subgroup
   - Damodaran uses simple average (not median) across firms
7. Cache results to JSON (30-day TTL), separate files per frequency

All data from actual sources — no synthetic/fabricated values.
"""

import os
import json
import logging
from datetime import datetime, timedelta
from typing import Optional

import numpy as np
import pandas as pd
from dotenv import load_dotenv

logger = logging.getLogger(__name__)

load_dotenv(os.path.join(os.path.dirname(__file__), '..', '..', 'config', '.env'))

# --- Window parameters by frequency ---
# Weekly: 2yr=104 weeks, 5yr=260 weeks
# Monthly: 2yr=24 months, 5yr=60 months
WINDOW_PARAMS = {
    'weekly': {
        'preferred_short': 104,   # 2 years of weeks
        'preferred_long': 260,    # 5 years of weeks
        'min_short': 78,          # ~1.5 years minimum for 2yr window
        'min_long': 156,          # ~3 years minimum for 5yr window
        'label': 'weekly',
    },
    'monthly': {
        'preferred_short': 24,    # 2 years of months
        'preferred_long': 60,     # 5 years of months
        'min_short': 18,          # ~1.5 years minimum
        'min_long': 36,           # 3 years minimum
        'label': 'monthly',
    },
}

SHORT_WEIGHT = 2/3       # Weight for 2-year beta (recent relevance)
LONG_WEIGHT = 1/3        # Weight for 5-year beta (structural stability)
MIN_COMPANIES = 3        # Minimum companies per subgroup for reliable average
CACHE_MAX_AGE_DAYS = 30


class SubgroupBetaCalculator:
    """
    Compute unlevered betas per valuation_subgroup from Indian market data.

    frequency='weekly' (default): Uses NIFTY index from combined_weekly_prices.csv
        - Matches Damodaran's exact methodology (weekly returns)
        - 261 weeks available (Feb 2021 - Feb 2026)
    frequency='monthly': Uses NIFTYBEES ETF from combined_monthly_prices.csv
        - Legacy mode, 290+ months available
    """

    def __init__(self, price_loader, core_loader, mysql_client, frequency='weekly'):
        self.prices = price_loader
        self.core = core_loader
        self.mysql = mysql_client
        self.frequency = frequency

        if frequency not in WINDOW_PARAMS:
            raise ValueError(f"frequency must be 'weekly' or 'monthly', got '{frequency}'")

        self.params = WINDOW_PARAMS[frequency]

        # Cache path includes frequency suffix
        cache_dir = os.path.join(os.path.dirname(__file__), '..', '..', 'data', 'cache')
        self.cache_path = os.path.join(cache_dir, f'subgroup_betas_{frequency}.json')
        self._cache = None

        # Lazy-loaded weekly DataFrame
        self._weekly_df = None

    # =========================================================================
    # Cache management
    # =========================================================================

    def _load_cache(self) -> dict:
        """Load cached subgroup betas from disk."""
        if self._cache is not None:
            return self._cache

        if os.path.exists(self.cache_path):
            try:
                with open(self.cache_path, 'r') as f:
                    self._cache = json.load(f)
                    logger.info(f"Loaded subgroup beta cache ({self.frequency}): "
                                f"{len(self._cache.get('subgroups', {}))} subgroups")
                    return self._cache
            except Exception as e:
                logger.warning(f"Failed to load beta cache: {e}")

        self._cache = {}
        return self._cache

    def _save_cache(self, data: dict):
        """Save subgroup betas to disk cache."""
        os.makedirs(os.path.dirname(self.cache_path), exist_ok=True)
        try:
            with open(self.cache_path, 'w') as f:
                json.dump(data, f, indent=2, default=str)
            self._cache = data
            logger.info(f"Saved subgroup beta cache ({self.frequency}): "
                        f"{len(data.get('subgroups', {}))} subgroups")
        except Exception as e:
            logger.error(f"Failed to save beta cache: {e}", exc_info=True)

    def _is_cache_fresh(self) -> bool:
        """Check if cached betas are still valid."""
        cache = self._load_cache()
        computed_at = cache.get('computed_at')
        if not computed_at:
            return False
        try:
            computed_dt = datetime.fromisoformat(computed_at)
            return (datetime.now() - computed_dt) < timedelta(days=CACHE_MAX_AGE_DAYS)
        except (ValueError, TypeError):
            return False

    # =========================================================================
    # Weekly data loading (from combined_weekly_prices.csv)
    # =========================================================================

    def _load_weekly_prices(self) -> pd.DataFrame:
        """
        Lazy-load combined_weekly_prices.csv.
        Cols: nse_symbol, weekly_date, close, year_week, Company Name, ...
        """
        if self._weekly_df is not None:
            return self._weekly_df

        weekly_path = os.getenv('WEEKLY_PRICES_PATH')
        if not weekly_path:
            raise ValueError("WEEKLY_PRICES_PATH not set in .env — required for weekly beta calculation")

        if not os.path.exists(weekly_path):
            raise FileNotFoundError(f"Weekly prices file not found: {weekly_path}")

        logger.info(f"Loading weekly prices: {weekly_path}")
        self._weekly_df = pd.read_csv(weekly_path, low_memory=False,
                                       usecols=['nse_symbol', 'weekly_date', 'close', 'year_week'])
        self._weekly_df['weekly_date'] = pd.to_datetime(self._weekly_df['weekly_date'])
        logger.info(f"Loaded {len(self._weekly_df)} weekly price records, "
                    f"date range: {self._weekly_df['weekly_date'].min()} to {self._weekly_df['weekly_date'].max()}, "
                    f"symbols: {self._weekly_df['nse_symbol'].nunique()}")
        return self._weekly_df

    def _get_market_weekly_returns(self) -> pd.Series:
        """
        Get NIFTY index weekly returns from combined_weekly_prices.csv.
        NIFTY = actual Nifty 50 index (not ETF proxy), 261 weeks.
        Returns Series indexed by year_week with weekly return values.
        """
        df = self._load_weekly_prices()
        nifty = df[df['nse_symbol'] == 'NIFTY'][['weekly_date', 'close', 'year_week']].copy()

        if nifty.empty:
            raise ValueError("NIFTY index not found in weekly price CSV (nse_symbol='NIFTY')")

        # One row per year_week (latest date if duplicates)
        nifty = nifty.sort_values('weekly_date').drop_duplicates('year_week', keep='last')
        nifty = nifty.set_index('year_week').sort_index()

        # Compute weekly returns
        nifty['return'] = nifty['close'].pct_change()
        market_returns = nifty['return'].dropna()

        logger.info(f"Market (NIFTY) weekly returns: {len(market_returns)} weeks "
                    f"({market_returns.index.min()} to {market_returns.index.max()})")
        return market_returns

    def _get_company_weekly_returns(self, nse_symbol: str) -> pd.Series:
        """
        Get weekly returns for a single company from combined_weekly_prices.csv.
        Returns Series indexed by year_week.
        """
        df = self._load_weekly_prices()
        company = df[df['nse_symbol'] == nse_symbol][['weekly_date', 'close', 'year_week']].copy()

        if company.empty or len(company) < self.params['min_short']:
            return pd.Series(dtype=float)

        # One row per year_week (latest date if duplicates)
        company = company.sort_values('weekly_date').drop_duplicates('year_week', keep='last')
        company = company.set_index('year_week').sort_index()

        # Compute weekly returns
        company['return'] = company['close'].pct_change()
        return company['return'].dropna()

    # =========================================================================
    # Monthly data loading (from combined_monthly_prices.csv via price_loader)
    # =========================================================================

    def _get_market_monthly_returns(self) -> pd.Series:
        """
        Get market (NIFTYBEES) monthly returns from combined_monthly_prices.csv.
        NIFTYBEES = Nippon India Nifty 50 ETF, 290+ months from 2002.
        Returns Series indexed by year_month with monthly return values.
        """
        df = self.prices.df
        nifty = df[df['nse_symbol'] == 'NIFTYBEES'][['daily_date', 'close', 'year_month']].copy()

        if nifty.empty:
            raise ValueError("Market proxy not found in price CSV (nse_symbol='NIFTYBEES')")

        # Take one row per year_month (latest date in each month)
        nifty = nifty.sort_values('daily_date').drop_duplicates('year_month', keep='last')
        nifty = nifty.set_index('year_month').sort_index()

        # Compute monthly returns
        nifty['return'] = nifty['close'].pct_change()
        market_returns = nifty['return'].dropna()

        logger.info(f"Market (NIFTYBEES) monthly returns: {len(market_returns)} months "
                    f"({market_returns.index.min()} to {market_returns.index.max()})")
        return market_returns

    def _get_company_monthly_returns(self, nse_symbol: str) -> pd.Series:
        """
        Get monthly returns for a single company.
        Returns Series indexed by year_month.
        """
        df = self.prices.df
        company = df[df['nse_symbol'] == nse_symbol][['daily_date', 'close', 'year_month']].copy()

        if company.empty or len(company) < self.params['min_short']:
            return pd.Series(dtype=float)

        # Take one row per year_month (latest date in each month)
        company = company.sort_values('daily_date').drop_duplicates('year_month', keep='last')
        company = company.set_index('year_month').sort_index()

        # Compute monthly returns
        company['return'] = company['close'].pct_change()
        return company['return'].dropna()

    # =========================================================================
    # Dispatch: get returns based on frequency
    # =========================================================================

    def _get_market_returns(self) -> pd.Series:
        """Dispatch to weekly or monthly market returns based on self.frequency."""
        if self.frequency == 'weekly':
            return self._get_market_weekly_returns()
        else:
            return self._get_market_monthly_returns()

    def _get_company_returns(self, nse_symbol: str) -> pd.Series:
        """Dispatch to weekly or monthly company returns based on self.frequency."""
        if self.frequency == 'weekly':
            return self._get_company_weekly_returns(nse_symbol)
        else:
            return self._get_company_monthly_returns(nse_symbol)

    # =========================================================================
    # Regression
    # =========================================================================

    def _regress_beta(self, company_returns: pd.Series, market_returns: pd.Series,
                      n_periods: int) -> Optional[dict]:
        """
        OLS regression of company returns vs market returns over last n_periods.
        n_periods = weeks (weekly mode) or months (monthly mode).
        Returns dict with beta, R², n_periods actually used, or None.
        """
        # Align by index (year_week or year_month inner join) and take last n_periods
        common_periods = company_returns.index.intersection(market_returns.index)

        if len(common_periods) > n_periods:
            common_periods = common_periods[-n_periods:]

        # Determine minimum based on window size
        if n_periods <= self.params['preferred_short']:
            min_required = self.params['min_short']
        else:
            min_required = self.params['min_long']

        if len(common_periods) < min_required:
            return None

        x = market_returns.loc[common_periods].values
        y = company_returns.loc[common_periods].values

        # Remove NaN/inf
        mask = np.isfinite(x) & np.isfinite(y)
        x, y = x[mask], y[mask]

        if len(x) < min_required:
            return None

        # OLS: y = alpha + beta * x
        x_mean = np.mean(x)
        y_mean = np.mean(y)
        cov_xy = np.mean((x - x_mean) * (y - y_mean))
        var_x = np.mean((x - x_mean) ** 2)

        if var_x < 1e-10:
            return None

        beta = cov_xy / var_x
        alpha = y_mean - beta * x_mean

        # R²
        y_pred = alpha + beta * x
        ss_res = np.sum((y - y_pred) ** 2)
        ss_tot = np.sum((y - y_mean) ** 2)
        r_squared = 1 - (ss_res / ss_tot) if ss_tot > 0 else 0

        return {
            'beta': round(beta, 4),
            'alpha': round(alpha, 6),
            'r_squared': round(r_squared, 4),
            'n_periods': len(x),
        }

    def compute_company_beta(self, nse_symbol: str, market_returns: pd.Series) -> Optional[dict]:
        """
        Compute blended beta for a company using dual-window approach.

        Blend = (2/3) × 2yr_beta + (1/3) × 5yr_beta
        Falls back to single window if only one is available.

        Returns dict with blended_beta, beta_2yr, beta_5yr, or None.
        """
        company_returns = self._get_company_returns(nse_symbol)

        if company_returns.empty:
            return None

        # Compute both windows
        result_2yr = self._regress_beta(company_returns, market_returns, self.params['preferred_short'])
        result_5yr = self._regress_beta(company_returns, market_returns, self.params['preferred_long'])

        if result_2yr is None and result_5yr is None:
            return None

        # Blend betas
        if result_2yr is not None and result_5yr is not None:
            blended = SHORT_WEIGHT * result_2yr['beta'] + LONG_WEIGHT * result_5yr['beta']
            method = '2yr+5yr_blended'
        elif result_2yr is not None:
            blended = result_2yr['beta']
            method = '2yr_only'
        else:
            blended = result_5yr['beta']
            method = '5yr_only'

        # Sanity check: beta should be in reasonable range
        if blended < -1.0 or blended > 4.0:
            logger.warning(f"  {nse_symbol}: extreme blended beta {blended:.3f}, excluding")
            return None

        return {
            'levered_beta': round(blended, 4),
            'beta_2yr': result_2yr['beta'] if result_2yr else None,
            'beta_5yr': result_5yr['beta'] if result_5yr else None,
            'r_squared_2yr': result_2yr['r_squared'] if result_2yr else None,
            'r_squared_5yr': result_5yr['r_squared'] if result_5yr else None,
            'n_periods_2yr': result_2yr['n_periods'] if result_2yr else None,
            'n_periods_5yr': result_5yr['n_periods'] if result_5yr else None,
            'method': method,
            'frequency': self.frequency,
        }

    def compute_all_subgroup_betas(self, force: bool = False) -> dict:
        """
        Compute simple average unlevered beta for each valuation_subgroup.
        Uses Damodaran methodology: 2/3 × 2yr + 1/3 × 5yr, simple average.

        Returns: {subgroup: {unlevered_beta, n_companies, avg_levered, min, max, companies: [...]}}
        """
        if not force and self._is_cache_fresh():
            cache = self._load_cache()
            logger.info(f"Using cached subgroup betas ({self.frequency}, still fresh)")
            return cache.get('subgroups', {})

        freq_label = self.frequency
        market_proxy = 'NIFTY' if self.frequency == 'weekly' else 'NIFTYBEES'
        logger.info(f"Computing subgroup betas from Indian market data ({freq_label})...")
        logger.info(f"Methodology: ({SHORT_WEIGHT:.0%} × 2yr + {LONG_WEIGHT:.0%} × 5yr) {freq_label}, "
                    f"simple average per subgroup, market={market_proxy}")

        # 1. Get market returns (weekly or monthly)
        market_returns = self._get_market_returns()

        # 2. Get all active companies with subgroups
        companies = self.mysql.query("""
            SELECT nse_symbol, company_name, valuation_subgroup
            FROM vs_active_companies
            WHERE is_active = 1 AND nse_symbol IS NOT NULL AND nse_symbol != ''
            AND valuation_subgroup IS NOT NULL AND valuation_subgroup != ''
        """)

        if not companies:
            logger.error("No active companies found in database")
            return {}

        logger.info(f"Processing {len(companies)} active companies for {freq_label} beta calculation...")

        # 3. For each company: compute blended levered beta, then de-lever
        subgroup_betas = {}    # {subgroup: [list of unlevered betas]}
        subgroup_details = {}  # {subgroup: [list of company detail dicts]}

        processed = 0
        skipped = 0
        for company in companies:
            symbol = company['nse_symbol']
            subgroup = company['valuation_subgroup']

            # Compute blended levered beta
            beta_result = self.compute_company_beta(symbol, market_returns)
            if beta_result is None:
                skipped += 1
                continue

            levered_beta = beta_result['levered_beta']

            # Get D/E ratio and tax rate for de-levering
            de_ratio = 0.0
            tax_rate = 0.25
            try:
                company_name = company['company_name']
                financials = self.core.get_company_financials(company_name)
                debt = self.core.get_latest_value(financials.get('debt', {}))
                networth = self.core.get_latest_value(financials.get('networth', {}))
                if debt and networth and networth > 0:
                    de_ratio = debt / networth

                # Get effective tax rate
                pat = financials.get('pat_annual', financials.get('pat', {}))
                pbt = financials.get('pbt_excp_yearly', {})
                if pat and pbt:
                    latest_yr = max(pbt.keys()) if pbt else None
                    if latest_yr and latest_yr in pat and pbt[latest_yr] and pbt[latest_yr] > 0:
                        computed_tax = 1 - (pat[latest_yr] / pbt[latest_yr])
                        if 0 < computed_tax < 0.50:
                            tax_rate = computed_tax
            except Exception:
                pass  # Use defaults

            # De-lever: beta_unlev = beta_lev / (1 + (1-t) × D/E)
            denominator = 1 + (1 - tax_rate) * de_ratio
            unlevered_beta = levered_beta / denominator if denominator > 0 else levered_beta

            # Sanity: unlevered beta should be positive and < 3
            if unlevered_beta <= 0 or unlevered_beta > 3.0:
                skipped += 1
                continue

            if subgroup not in subgroup_betas:
                subgroup_betas[subgroup] = []
                subgroup_details[subgroup] = []

            subgroup_betas[subgroup].append(unlevered_beta)
            subgroup_details[subgroup].append({
                'symbol': symbol,
                'levered_beta': levered_beta,
                'unlevered_beta': round(unlevered_beta, 4),
                'de_ratio': round(de_ratio, 4),
                'tax_rate': round(tax_rate, 4),
                'beta_2yr': beta_result.get('beta_2yr'),
                'beta_5yr': beta_result.get('beta_5yr'),
                'r_squared_2yr': beta_result.get('r_squared_2yr'),
                'r_squared_5yr': beta_result.get('r_squared_5yr'),
                'method': beta_result.get('method'),
            })
            processed += 1

        logger.info(f"Processed {processed} companies, skipped {skipped} "
                    f"(insufficient data or extreme values)")

        # 4. Aggregate by subgroup — SIMPLE AVERAGE (Damodaran uses average, not median)
        result = {}
        for subgroup, betas in sorted(subgroup_betas.items()):
            betas_arr = np.array(betas)
            n = len(betas_arr)
            avg_beta = float(np.mean(betas_arr))

            # Also compute average levered for reference
            levered_betas = [d['levered_beta'] for d in subgroup_details[subgroup]]

            entry = {
                'unlevered_beta': round(avg_beta, 4),
                'n_companies': n,
                'avg_levered': round(float(np.mean(levered_betas)), 4),
                'median_unlevered': round(float(np.median(betas_arr)), 4),
                'min': round(float(np.min(betas_arr)), 4),
                'max': round(float(np.max(betas_arr)), 4),
                'std': round(float(np.std(betas_arr)), 4),
                'companies': subgroup_details[subgroup],
            }

            flag = ""
            if n < MIN_COMPANIES:
                flag = " [LOW N - REVIEW]"
                entry['low_n_flag'] = True

            logger.info(f"  {subgroup}: β_u={avg_beta:.3f} "
                        f"(n={n}, range=[{np.min(betas_arr):.2f}, {np.max(betas_arr):.2f}], "
                        f"std={np.std(betas_arr):.2f}){flag}")
            result[subgroup] = entry

        # 5. Cache
        cache_data = {
            'computed_at': datetime.now().isoformat(),
            'frequency': self.frequency,
            'methodology': f'{SHORT_WEIGHT:.0%} × 2yr_{freq_label} + {LONG_WEIGHT:.0%} × 5yr_{freq_label}, '
                           f'simple average per subgroup, de-levered',
            'market_proxy': market_proxy,
            'window_params': self.params,
            'total_companies': processed,
            'total_subgroups': len(result),
            'subgroups': result,
        }
        self._save_cache(cache_data)

        return result

    def get_subgroup_beta(self, valuation_subgroup: str) -> Optional[dict]:
        """
        Look up cached subgroup beta. Recompute if stale.
        Returns dict with unlevered_beta, n_companies, etc. or None if not found.
        """
        cache = self._load_cache()
        subgroups = cache.get('subgroups', {})

        if valuation_subgroup in subgroups:
            entry = subgroups[valuation_subgroup]
            logger.debug(f"Subgroup beta ({self.frequency}) for {valuation_subgroup}: "
                         f"β_u={entry['unlevered_beta']:.3f} (n={entry['n_companies']})")
            return entry

        logger.warning(f"No cached {self.frequency} beta for subgroup '{valuation_subgroup}'")
        return None
