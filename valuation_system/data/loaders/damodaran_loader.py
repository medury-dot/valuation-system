"""
Damodaran Data Loader
Fetch Equity Risk Premium, industry betas, and cost of capital
from Damodaran's data pages at NYU Stern.
Cached locally to avoid repeated scraping.
"""

import os
import json
import logging
from datetime import datetime, timedelta
from typing import Optional

import pandas as pd
import requests
from dotenv import load_dotenv

logger = logging.getLogger(__name__)

load_dotenv(os.path.join(os.path.dirname(__file__), '..', '..', 'config', '.env'))


class DamodaranLoader:
    """
    Fetch and cache Damodaran's publicly available data:
    - Country risk premiums (India ERP)
    - Industry betas (unlevered)
    - Cost of capital by industry

    Data is cached locally and refreshed monthly.
    """

    DAMODARAN_URLS = {
        'erp': 'https://pages.stern.nyu.edu/~adamodar/New_Home_Page/datafile/ctryprem.html',
        'betas_emerging': 'https://pages.stern.nyu.edu/~adamodar/New_Home_Page/datafile/Betas.html',
        'wacc': 'https://pages.stern.nyu.edu/~adamodar/New_Home_Page/datafile/wacc.html',
        'cost_of_equity': 'https://pages.stern.nyu.edu/~adamodar/New_Home_Page/datafile/ctryprem.html',
    }

    # Map our sector names to Damodaran's industry categories
    SECTOR_TO_DAMODARAN = {
        'Chemicals': 'Chemical (Specialty)',
        'Automobile & Ancillaries': 'Auto & Truck',
        'Finance': 'Financial Svcs. (Non-bank & Insurance)',
        'IT': 'Software (System & Application)',
        'Healthcare': 'Drugs (Pharmaceutical)',
        'FMCG': 'Food Processing',
    }

    # Default values (Jan 2026 Damodaran data)
    # Updated monthly from Damodaran's pages
    DEFAULT_INDIA_PARAMS = {
        'mature_market_erp': 0.046,     # US ERP
        'india_country_risk_premium': 0.019,  # India-specific
        'india_total_erp': 0.065,       # mature + country
        'india_default_spread': 0.0114,
        'india_rating': 'Baa3',
        'risk_free_rate_india': 0.071,  # 10Y G-Sec yield
        'last_updated': '2026-01-01',
    }

    # Industry betas (unlevered) - Damodaran Jan 2026
    DEFAULT_INDUSTRY_BETAS = {
        'Chemical (Specialty)': {'unlevered_beta': 0.82, 'de_ratio': 0.25, 'tax_rate': 0.12},
        'Chemical (Basic)': {'unlevered_beta': 0.77, 'de_ratio': 0.30, 'tax_rate': 0.11},
        'Auto & Truck': {'unlevered_beta': 0.88, 'de_ratio': 0.15, 'tax_rate': 0.10},
        'Auto Parts': {'unlevered_beta': 0.92, 'de_ratio': 0.20, 'tax_rate': 0.12},
        'Financial Svcs. (Non-bank & Insurance)': {'unlevered_beta': 0.60, 'de_ratio': 2.00, 'tax_rate': 0.08},
        'Banks (Regional)': {'unlevered_beta': 0.45, 'de_ratio': 3.50, 'tax_rate': 0.10},
        'Software (System & Application)': {'unlevered_beta': 1.05, 'de_ratio': 0.05, 'tax_rate': 0.07},
        'Drugs (Pharmaceutical)': {'unlevered_beta': 0.91, 'de_ratio': 0.10, 'tax_rate': 0.05},
        'Food Processing': {'unlevered_beta': 0.63, 'de_ratio': 0.20, 'tax_rate': 0.10},
        'Healthcare Products': {'unlevered_beta': 0.90, 'de_ratio': 0.10, 'tax_rate': 0.06},
    }

    def __init__(self, cache_dir: str = None):
        self.cache_dir = cache_dir or os.path.join(
            os.path.dirname(__file__), '..', '..', 'data', 'cache'
        )
        os.makedirs(self.cache_dir, exist_ok=True)
        self._cache = {}
        self._load_cache()

    def _load_cache(self):
        """Load cached data from disk."""
        cache_file = os.path.join(self.cache_dir, 'damodaran_cache.json')
        if os.path.exists(cache_file):
            try:
                with open(cache_file, 'r') as f:
                    self._cache = json.load(f)
                logger.info(f"Loaded Damodaran cache from {cache_file}")
            except Exception as e:
                logger.warning(f"Failed to load Damodaran cache: {e}")
                self._cache = {}

    def _save_cache(self):
        """Save cache to disk."""
        cache_file = os.path.join(self.cache_dir, 'damodaran_cache.json')
        try:
            with open(cache_file, 'w') as f:
                json.dump(self._cache, f, indent=2, default=str)
        except Exception as e:
            logger.error(f"Failed to save Damodaran cache: {e}", exc_info=True)

    def _is_cache_fresh(self, key: str, max_age_days: int = 30) -> bool:
        """Check if cached data is still fresh."""
        if key not in self._cache:
            return False
        cached_date = self._cache[key].get('cached_at')
        if not cached_date:
            return False
        try:
            cached_dt = datetime.fromisoformat(cached_date)
            return (datetime.now() - cached_dt) < timedelta(days=max_age_days)
        except (ValueError, TypeError):
            return False

    def get_india_erp(self) -> dict:
        """
        Get India's equity risk premium.

        Total ERP = Mature Market ERP + Country Risk Premium
        Typically ~6.5% (4.6% base + 1.9% India premium)

        Returns dict with components.
        """
        if self._is_cache_fresh('india_erp'):
            return self._cache['india_erp']['data']

        # Try to scrape Damodaran's page
        try:
            erp_data = self._scrape_country_erp()
            if erp_data:
                self._cache['india_erp'] = {
                    'data': erp_data,
                    'cached_at': datetime.now().isoformat()
                }
                self._save_cache()
                return erp_data
        except Exception as e:
            logger.warning(f"Failed to scrape Damodaran ERP: {e}, using defaults")

        return self.DEFAULT_INDIA_PARAMS

    def get_industry_beta(self, sector: str,
                          company_de_ratio: float = None,
                          company_tax_rate: float = 0.25) -> dict:
        """
        Get beta for a sector/industry.

        Uses Damodaran's unlevered beta, then re-levers for company's
        capital structure.

        Levered Beta = Unlevered Beta * (1 + (1-t) * D/E)
        """
        damodaran_industry = self.SECTOR_TO_DAMODARAN.get(sector, sector)
        industry_data = self.DEFAULT_INDUSTRY_BETAS.get(damodaran_industry)

        if not industry_data:
            logger.warning(f"No Damodaran beta for '{damodaran_industry}', using market beta=1.0")
            industry_data = {'unlevered_beta': 1.0, 'de_ratio': 0.20, 'tax_rate': 0.10}

        unlevered_beta = industry_data['unlevered_beta']

        # Re-lever for company's capital structure
        de_ratio = company_de_ratio if company_de_ratio is not None else industry_data['de_ratio']
        levered_beta = unlevered_beta * (1 + (1 - company_tax_rate) * de_ratio)

        result = {
            'damodaran_industry': damodaran_industry,
            'unlevered_beta': unlevered_beta,
            'industry_de_ratio': industry_data['de_ratio'],
            'company_de_ratio_used': round(de_ratio, 4),
            'tax_rate_used': company_tax_rate,
            'levered_beta': round(levered_beta, 4),
        }

        logger.debug(f"Beta for {sector}: unlevered={unlevered_beta}, "
                      f"levered={levered_beta:.4f} (D/E={de_ratio:.2f})")

        return result

    def get_risk_free_rate(self) -> float:
        """
        Get India 10Y G-Sec yield (risk-free rate for DCF).

        Primary: Try Yahoo Finance for ^TNX equivalent
        Fallback: Use cached/default value
        """
        try:
            import yfinance
            # India 10Y G-Sec - use I10Y.IN or fallback
            ticker = yfinance.Ticker("^TNX")
            hist = ticker.history(period='5d')
            if not hist.empty:
                us_10y = float(hist['Close'].iloc[-1]) / 100
                # India premium ~0.5-1% over US 10Y typically
                india_10y = us_10y + 0.025  # Approximate India-US spread
                logger.info(f"Risk-free rate: US 10Y={us_10y:.4f}, India 10Y (est)={india_10y:.4f}")
                return india_10y
        except Exception as e:
            logger.warning(f"Failed to get live risk-free rate: {e}")

        return self.DEFAULT_INDIA_PARAMS['risk_free_rate_india']

    def get_wacc_inputs(self, sector: str,
                        company_de_ratio: float = None,
                        company_tax_rate: float = 0.25) -> dict:
        """
        Get all WACC inputs for a sector:
        - Risk-free rate
        - Equity risk premium
        - Beta (levered)
        - Cost of equity (CAPM)
        """
        erp_data = self.get_india_erp()
        beta_data = self.get_industry_beta(sector, company_de_ratio, company_tax_rate)
        rf = self.get_risk_free_rate()
        erp = erp_data.get('india_total_erp', self.DEFAULT_INDIA_PARAMS['india_total_erp'])

        cost_of_equity = rf + beta_data['levered_beta'] * erp

        return {
            'risk_free_rate': rf,
            'equity_risk_premium': erp,
            'mature_market_erp': erp_data.get('mature_market_erp', 0.046),
            'country_risk_premium': erp_data.get('india_country_risk_premium', 0.019),
            'beta': beta_data['levered_beta'],
            'unlevered_beta': beta_data['unlevered_beta'],
            'cost_of_equity': round(cost_of_equity, 4),
            'damodaran_industry': beta_data['damodaran_industry'],
        }

    def _scrape_country_erp(self) -> Optional[dict]:
        """
        Scrape Damodaran's country risk premium page for India data.

        Damodaran's ctryprem.html table columns (typical structure):
          Country | Moody's Rating | Adj. Default Spread | Equity Risk Premium | Country Risk Premium

        'Equity Risk Premium' = Total ERP (mature + country) — use directly
        'Country Risk Premium' = country-specific add-on over mature market

        We identify columns by header text to avoid wrong-column bugs.
        """
        try:
            headers = {'User-Agent': 'Mozilla/5.0 (equity-research-tool)'}
            resp = requests.get(self.DAMODARAN_URLS['erp'], headers=headers, timeout=30)
            resp.raise_for_status()

            tables = pd.read_html(resp.text)

            for table in tables:
                # Find the column that contains country names
                country_col = None
                for col in table.columns:
                    if table[col].astype(str).str.contains('India', case=False, na=False).any():
                        country_col = col
                        break

                if country_col is None:
                    continue

                india_mask = table[country_col].astype(str).str.contains('India', case=False, na=False)
                if not india_mask.any():
                    continue

                india_row = table[india_mask].iloc[0]
                col_headers = [str(c).lower() for c in table.columns]

                logger.info(f"Damodaran ERP table columns: {list(table.columns)}")
                logger.info(f"India row values: {list(india_row.values)}")

                # Try to identify columns by header text
                total_erp = None
                country_rp = None

                for i, header in enumerate(col_headers):
                    val = india_row.iloc[i]
                    try:
                        num_val = float(str(val).replace('%', '').strip())
                    except (ValueError, TypeError):
                        continue

                    # Convert to decimal if in percentage form
                    if num_val > 1:
                        num_val = num_val / 100

                    if 'equity risk' in header and 'country' not in header:
                        total_erp = num_val
                        logger.info(f"  Parsed Total ERP from column '{table.columns[i]}': {num_val:.4f}")
                    elif 'country risk' in header or 'country' in header and 'premium' in header:
                        country_rp = num_val
                        logger.info(f"  Parsed Country RP from column '{table.columns[i]}': {num_val:.4f}")

                # If we couldn't identify by headers, use heuristic matching:
                # - Total ERP for India is typically 5-10% (mature ERP + country RP)
                # - Country RP is typically 1-5%
                # - Default spread is typically 0.5-3%
                # We look for values in these ranges among all numeric values
                if total_erp is None and country_rp is None:
                    numeric_vals = []
                    for v in india_row.values:
                        try:
                            nv = float(str(v).replace('%', '').strip())
                            if nv > 1:
                                nv = nv / 100
                            numeric_vals.append(nv)
                        except (ValueError, TypeError):
                            continue

                    logger.info(f"  All numeric values: {[f'{v:.4f}' for v in numeric_vals]}")

                    # Find Total ERP: value in 0.04-0.12 range, closest to default 0.065
                    erp_candidates = [v for v in numeric_vals if 0.04 <= v <= 0.12]
                    if erp_candidates:
                        total_erp = min(erp_candidates, key=lambda x: abs(x - 0.065))
                        logger.info(f"  Heuristic: Total ERP = {total_erp:.4f} "
                                    f"(from candidates {[f'{v:.4f}' for v in erp_candidates]})")

                    # Find Country RP: value in 0.01-0.06 range, should be < Total ERP
                    crp_candidates = [v for v in numeric_vals
                                      if 0.01 <= v <= 0.06 and (total_erp is None or v < total_erp)]
                    if crp_candidates:
                        country_rp = min(crp_candidates, key=lambda x: abs(x - 0.019))
                        logger.info(f"  Heuristic: Country RP = {country_rp:.4f} "
                                    f"(from candidates {[f'{v:.4f}' for v in crp_candidates]})")

                # Build result with validation
                result = {
                    'mature_market_erp': 0.046,
                    'source': 'damodaran_scraped',
                    'last_updated': datetime.now().isoformat(),
                }

                if total_erp is not None and 0.04 <= total_erp <= 0.15:
                    result['india_total_erp'] = total_erp
                    result['india_country_risk_premium'] = total_erp - 0.046
                    logger.info(f"  Final: Total ERP={total_erp:.4f}, Country RP={total_erp - 0.046:.4f}")
                elif country_rp is not None and 0.005 <= country_rp <= 0.08:
                    result['india_country_risk_premium'] = country_rp
                    result['india_total_erp'] = 0.046 + country_rp
                    logger.info(f"  Final from Country RP: Total ERP={0.046 + country_rp:.4f}")
                else:
                    logger.warning(f"  Scraped ERP values out of bounds (total={total_erp}, country={country_rp}), "
                                   f"using defaults: total_erp=0.065, country_rp=0.019")
                    result['india_country_risk_premium'] = 0.019
                    result['india_total_erp'] = 0.065

                return result

            logger.warning("Could not find India row in Damodaran ERP table")
            return None

        except Exception as e:
            logger.error(f"Damodaran scrape failed: {e}", exc_info=True)
            return None

    def get_all_params(self, sector: str,
                       company_de_ratio: float = None,
                       company_tax_rate: float = 0.25) -> dict:
        """
        Convenience method: get all Damodaran parameters needed for valuation.
        """
        wacc_inputs = self.get_wacc_inputs(sector, company_de_ratio, company_tax_rate)
        erp_data = self.get_india_erp()

        return {
            **wacc_inputs,
            'india_rating': erp_data.get('india_rating', 'Baa3'),
            'india_default_spread': erp_data.get('india_default_spread', 0.0114),
            'data_source': 'damodaran',
        }
