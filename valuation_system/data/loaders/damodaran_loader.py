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

    # Map our CSV sector names to Damodaran's industry categories (legacy fallback)
    SECTOR_TO_DAMODARAN = {
        'Chemicals': 'Chemical (Specialty)',
        'Automobile & Ancillaries': 'Auto & Truck',
        'Finance': 'Financial Svcs. (Non-bank & Insurance)',
        'IT': 'Software (System & Application)',
        'Healthcare': 'Drugs (Pharmaceutical)',
        'FMCG': 'Food Processing',
    }

    # Map our 55 valuation_subgroups → Damodaran India industry names
    # Used for fallback when computed betas are unavailable
    SUBGROUP_TO_DAMODARAN_INDIA = {
        'AUTO_ANCILLARY_BATTERIES': 'Auto Parts',
        'AUTO_ANCILLARY_COMPONENTS': 'Auto Parts',
        'AUTO_ANCILLARY_TIRES': 'Rubber& Tires',
        'AUTO_OEM': 'Auto & Truck',
        'CHEMICALS_COMMODITY': 'Chemical (Basic)',
        'CHEMICALS_PAINTS_COATINGS': 'Chemical (Specialty)',
        'CHEMICALS_SPECIALTY': 'Chemical (Specialty)',
        'CONSUMER_AGRI': 'Farming/Agriculture',
        'CONSUMER_DURABLES_BROWN_GOODS': 'Electronics (Consumer & Office)',
        'CONSUMER_DURABLES_SMALL_APPLIANCES': 'Household Products',
        'CONSUMER_DURABLES_WHITE_GOODS': 'Household Products',
        'CONSUMER_FMCG_HPC': 'Household Products',
        'CONSUMER_FMCG_PACKAGED_FOOD': 'Food Processing',
        'CONSUMER_FMCG_STAPLES': 'Food Processing',
        'CONSUMER_FOOD_BEVERAGE': 'Beverage (Soft)',
        'CONSUMER_RETAIL_OFFLINE': 'Retail (Special Lines)',
        'CONSUMER_TEXTILE': 'Apparel',
        'ENERGY_DOWNSTREAM': 'Oil/Gas Distribution',
        'ENERGY_POWER_DISTRIBUTION': 'Power',
        'ENERGY_POWER_GENERATION': 'Power',
        'ENERGY_UPSTREAM': 'Oil/Gas (Production and Exploration)',
        'FINANCIALS_ASSET_MGMT': 'Investments & Asset Management',
        'FINANCIALS_BANKING_PRIVATE': 'Bank (Money Center)',
        'FINANCIALS_BANKING_PSU': 'Bank (Money Center)',
        'FINANCIALS_EXCHANGES_DEPOSITORIES': 'Brokerage & Investment Banking',
        'FINANCIALS_INSURANCE_GENERAL': 'Insurance (General)',
        'FINANCIALS_INSURANCE_HEALTH': 'Insurance (Prop/Cas.)',
        'FINANCIALS_INSURANCE_LIFE': 'Insurance (Life)',
        'FINANCIALS_NBFC_DIVERSIFIED': 'Financial Svcs. (Non-bank & Insurance)',
        'FINANCIALS_NBFC_HOUSING': 'Financial Svcs. (Non-bank & Insurance)',
        'FINANCIALS_NBFC_VEHICLE': 'Financial Svcs. (Non-bank & Insurance)',
        'FINANCIALS_RATINGS': 'Information Services',
        'HEALTHCARE_DIAGNOSTICS': 'Healthcare Support Services',
        'HEALTHCARE_HOSPITALS': 'Hospitals/Healthcare Facilities',
        'HEALTHCARE_MEDICAL_EQUIPMENT': 'Healthcare Products',
        'HEALTHCARE_PHARMA_CRO_CDMO': 'Drugs (Pharmaceutical)',
        'HEALTHCARE_PHARMA_MFG': 'Drugs (Pharmaceutical)',
        'INDUSTRIALS_CAPITAL_GOODS': 'Machinery',
        'INDUSTRIALS_DEFENSE': 'Aerospace/Defense',
        'INDUSTRIALS_ELECTRICALS': 'Electrical Equipment',
        'INDUSTRIALS_ENGINEERING': 'Engineering/Construction',
        'INDUSTRIALS_TELECOM_EQUIPMENT': 'Telecom. Equipment',
        'INFRA_CONSTRUCTION': 'Engineering/Construction',
        'INFRA_LOGISTICS_PORTS': 'Transportation',
        'METALS_COPPER_ZINC': 'Metals & Mining',
        'METALS_STEEL': 'Steel',
        'REALTY_RESIDENTIAL': 'Real Estate (Development)',
        'SERVICES_HOSPITALITY': 'Hotel/Gaming',
        'SERVICES_MEDIA_BROADCASTING': 'Broadcasting',
        'SERVICES_TELECOM_OPERATORS': 'Telecom. Services',
        'SERVICES_TELECOM_TOWERS': 'Telecom. Equipment',
        'TECHNOLOGY_BPO_ITES': 'Computer Services',
        'TECHNOLOGY_DIGITAL_INFRA': 'Information Services',
        'TECHNOLOGY_IT_SERVICES': 'Software (System & Application)',
        'TECHNOLOGY_PRODUCT_SAAS': 'Software (System & Application)',
        # New subgroups from NOT_CLASSIFIED reclassification (Feb 2026)
        'CONSUMER_ALCOHOLIC_BEVERAGES': 'Beverage (Alcoholic)',
        'CONSUMER_JEWELLERY': 'Retail (Special Lines)',
        'CONSUMER_RETAIL_ONLINE': 'Retail (General)',
        'ENERGY_INDUSTRIAL_GAS': 'Oil/Gas Distribution',
        'INDUSTRIALS_ENVIRONMENTAL': 'Environmental & Waste Services',
        'INFRA_LOGISTICS': 'Transportation',
        'INFRA_SHIPPING': 'Shipbuilding & Marine',
        'MATERIALS_MINING': 'Metals & Mining',
        'MATERIALS_PAPER': 'Paper/Forest Products',
        'MATERIALS_PLASTICS': 'Packaging & Container',
        'SERVICES_AVIATION': 'Air Transport',
        'SERVICES_BPO': 'Business & Consumer Services',
        'SERVICES_EDUCATION': 'Education',
        'SERVICES_PROFESSIONAL': 'Business & Consumer Services',
        'SERVICES_TRADING': 'Retail (Distributors)',
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
                          company_tax_rate: float = 0.25,
                          valuation_subgroup: str = None) -> dict:
        """
        Get beta for a company.

        Priority chain:
        1. Indian subgroup beta (computed from NIFTYBEES regression, dual-window)
        2. Damodaran India beta (from betaIndia.xls, 93 industries)
        3. Damodaran US sector beta — legacy fallback
        4. Market beta = 1.0

        No Blume adjustment — the dual-window (2/3 × 2yr + 1/3 × 5yr) methodology
        already handles noise via the 5yr component pulling toward structural beta.

        Re-levers unlevered beta for company's own capital structure:
        Levered Beta = Unlevered Beta * (1 + (1-t) * D/E)
        """
        beta_source = 'default'
        unlevered_beta = None
        subgroup_data = None

        # Priority 1: Indian subgroup beta from computed cache
        if valuation_subgroup:
            subgroup_data = self._get_subgroup_beta(valuation_subgroup)
            if subgroup_data:
                unlevered_beta = subgroup_data['unlevered_beta']
                beta_source = f'indian_subgroup:{valuation_subgroup}'
                logger.info(f"Beta from Indian subgroup '{valuation_subgroup}': "
                           f"β_u={unlevered_beta:.3f} (n={subgroup_data['n_companies']})")

        # Priority 2: Damodaran India beta (from betaIndia.xls)
        if unlevered_beta is None and valuation_subgroup:
            india_beta = self._get_damodaran_india_beta(valuation_subgroup)
            if india_beta is not None:
                unlevered_beta = india_beta['unlevered_beta']
                beta_source = f'damodaran_india:{india_beta["industry"]}'
                logger.info(f"Beta from Damodaran India '{india_beta['industry']}': "
                           f"β_u={unlevered_beta:.3f} (n={india_beta.get('n_firms', '?')})")

        # Priority 3: Damodaran US sector mapping (legacy)
        if unlevered_beta is None:
            damodaran_industry = self.SECTOR_TO_DAMODARAN.get(sector, sector)
            industry_data = self.DEFAULT_INDUSTRY_BETAS.get(damodaran_industry)
            if industry_data:
                unlevered_beta = industry_data['unlevered_beta']
                beta_source = f'damodaran_us:{damodaran_industry}'
                logger.info(f"Beta from Damodaran US '{damodaran_industry}': β_u={unlevered_beta:.3f}")

        # Priority 4: Market beta default
        if unlevered_beta is None:
            unlevered_beta = 1.0
            beta_source = 'default_market'
            logger.warning(f"No beta for sector='{sector}' subgroup='{valuation_subgroup}', using market beta=1.0")

        # Re-lever for company's capital structure
        de_ratio = company_de_ratio if company_de_ratio is not None else 0.20
        levered_beta = unlevered_beta * (1 + (1 - company_tax_rate) * de_ratio)

        result = {
            'beta_source': beta_source,
            'unlevered_beta': round(unlevered_beta, 4),
            'company_de_ratio_used': round(de_ratio, 4),
            'tax_rate_used': company_tax_rate,
            'levered_beta': round(levered_beta, 4),
        }

        if subgroup_data:
            result['subgroup_n_companies'] = subgroup_data['n_companies']
            result['subgroup_range'] = f"[{subgroup_data['min']:.2f}, {subgroup_data['max']:.2f}]"

        logger.info(f"Beta for {sector}/{valuation_subgroup}: unlevered={unlevered_beta:.4f}, "
                    f"levered={levered_beta:.4f} (D/E={de_ratio:.2f}, source={beta_source})")

        return result

    def get_all_beta_scenarios(self, valuation_group: str, valuation_subgroup: str,
                                company_symbol: str, de_ratio: float,
                                tax_rate: float = 0.25) -> dict:
        """
        Compute 3 beta scenarios for multi-scenario DCF valuation.

        Scenario A: Individual company beta (from weekly cache, company-specific)
        Scenario B: Damodaran India industry beta (professional estimate)
        Scenario C: Subgroup aggregate beta (current default, peer average)

        Returns dict with keys: 'individual_weekly', 'damodaran_india', 'subgroup_aggregate'
        Each scenario has: levered_beta, unlevered_beta, source, wacc (optional)
        """
        scenarios = {}

        # Scenario A: Individual company beta from weekly cache
        weekly_cache_file = os.path.join(self.cache_dir, 'subgroup_betas_weekly.json')
        if os.path.exists(weekly_cache_file):
            try:
                with open(weekly_cache_file, 'r') as f:
                    weekly_cache = json.load(f)

                subgroups = weekly_cache.get('subgroups', {})
                lookup_key = valuation_subgroup.upper()

                if lookup_key in subgroups:
                    subgroup_data = subgroups[lookup_key]
                    companies = subgroup_data.get('companies', [])

                    # Search for this specific company
                    for comp in companies:
                        if comp.get('symbol') == company_symbol:
                            scenarios['individual_weekly'] = {
                                'levered_beta': comp['levered_beta'],
                                'unlevered_beta': comp['unlevered_beta'],
                                'source': f'individual_weekly:{company_symbol}',
                                'de_ratio': comp.get('de_ratio', de_ratio),
                                'tax_rate': comp.get('tax_rate', tax_rate)
                            }
                            logger.info(f"Scenario A (Individual): β_lev={comp['levered_beta']:.3f} "
                                       f"for {company_symbol} from weekly cache")
                            break
            except Exception as e:
                logger.warning(f"Failed to load individual weekly beta: {e}")

        # Scenario B: Damodaran India industry beta
        india_beta = self._get_damodaran_india_beta(valuation_subgroup)
        if india_beta:
            ub = india_beta['unlevered_beta']
            lb = self._relever_beta(ub, de_ratio, tax_rate)
            scenarios['damodaran_india'] = {
                'unlevered_beta': ub,
                'levered_beta': lb,
                'source': f'damodaran_india:{india_beta["industry"]}',
                'industry': india_beta['industry'],  # Explicit industry field for GSheet display
                'subgroup_mapped': valuation_subgroup,  # Which subgroup was mapped to this industry
                'n_firms': india_beta.get('n_firms'),
                'de_ratio': de_ratio,
                'tax_rate': tax_rate
            }
            logger.info(f"Scenario B (Damodaran India): β_u={ub:.3f} → β_lev={lb:.3f} "
                       f"from {india_beta['industry']} (mapped from {valuation_subgroup})")

        # Scenario C: Subgroup aggregate beta (current logic)
        subgroup_beta = self._get_subgroup_beta(valuation_subgroup)
        if subgroup_beta:
            ub = subgroup_beta['unlevered_beta']
            lb = self._relever_beta(ub, de_ratio, tax_rate)
            scenarios['subgroup_aggregate'] = {
                'unlevered_beta': ub,
                'levered_beta': lb,
                'source': f'subgroup_aggregate:{valuation_subgroup}',
                'n_companies': subgroup_beta['n_companies'],
                'de_ratio': de_ratio,
                'tax_rate': tax_rate
            }
            logger.info(f"Scenario C (Subgroup Aggregate): β_u={ub:.3f} → β_lev={lb:.3f} "
                       f"from {valuation_subgroup} (n={subgroup_beta['n_companies']})")

        # If no scenarios found, add market beta fallback
        if not scenarios:
            scenarios['market_fallback'] = {
                'unlevered_beta': 1.0,
                'levered_beta': self._relever_beta(1.0, de_ratio, tax_rate),
                'source': 'default_market',
                'de_ratio': de_ratio,
                'tax_rate': tax_rate
            }
            logger.warning(f"No beta scenarios found for {company_symbol}, using market beta=1.0")

        return scenarios

    def _relever_beta(self, unlevered_beta: float, de_ratio: float, tax_rate: float) -> float:
        """
        Re-lever unlevered beta for company's capital structure.
        Levered Beta = Unlevered Beta × (1 + (1-t) × D/E)
        """
        return unlevered_beta * (1 + (1 - tax_rate) * de_ratio)

    def _get_subgroup_beta(self, valuation_subgroup: str) -> Optional[dict]:
        """
        Look up Indian subgroup beta from cached JSON files.
        Priority: weekly > monthly > legacy (subgroup_betas.json)
        Returns dict with unlevered_beta, n_companies, min, max or None.
        """
        # Try cache files in priority order: weekly first (matches Damodaran methodology),
        # then monthly, then legacy filename
        cache_files = [
            ('weekly', os.path.join(self.cache_dir, 'subgroup_betas_weekly.json')),
            ('monthly', os.path.join(self.cache_dir, 'subgroup_betas_monthly.json')),
            ('legacy', os.path.join(self.cache_dir, 'subgroup_betas.json')),
        ]

        for freq_label, cache_file in cache_files:
            if not os.path.exists(cache_file):
                continue

            try:
                with open(cache_file, 'r') as f:
                    cache = json.load(f)

                subgroups = cache.get('subgroups', {})
                # Case-insensitive lookup: cache keys are UPPERCASE, input may be lowercase
                lookup_key = valuation_subgroup.upper()
                if lookup_key in subgroups:
                    entry = subgroups[lookup_key]
                    logger.debug(f"Subgroup beta for '{lookup_key}' from {freq_label} cache: "
                                f"β_u={entry['unlevered_beta']:.3f} (n={entry['n_companies']})")
                    return entry
            except Exception as e:
                logger.warning(f"Failed to read {freq_label} subgroup beta cache ({cache_file}): {e}")
                continue

        logger.debug(f"Subgroup '{valuation_subgroup}' (tried '{valuation_subgroup.upper()}') not found in any beta cache")
        return None

    def _get_damodaran_india_beta(self, valuation_subgroup: str) -> Optional[dict]:
        """
        Look up Damodaran India beta (from betaIndia.xls) for a valuation_subgroup.
        Uses SUBGROUP_TO_DAMODARAN_INDIA mapping.
        Returns dict with unlevered_beta, industry name, n_firms, or None.
        """
        # Case-insensitive lookup: mapping keys are UPPERCASE
        damodaran_industry = self.SUBGROUP_TO_DAMODARAN_INDIA.get(valuation_subgroup.upper())
        if not damodaran_industry:
            logger.debug(f"No Damodaran India mapping for subgroup '{valuation_subgroup}' (tried '{valuation_subgroup.upper()}')")
            return None

        cache_file = os.path.join(self.cache_dir, 'damodaran_india_betas.json')
        if not os.path.exists(cache_file):
            logger.debug(f"No Damodaran India beta cache at {cache_file}")
            return None

        try:
            with open(cache_file, 'r') as f:
                india_data = json.load(f)

            industries = india_data.get('industries', {})
            if damodaran_industry in industries:
                entry = industries[damodaran_industry]
                ub = entry.get('unlevered_beta')
                if ub is not None and ub > 0:
                    return {
                        'unlevered_beta': ub,
                        'industry': damodaran_industry,
                        'n_firms': entry.get('n_firms'),
                        'levered_beta': entry.get('levered_beta'),
                        'de_ratio': entry.get('de_ratio'),
                        'effective_tax_rate': entry.get('effective_tax_rate'),
                    }

            logger.debug(f"Damodaran India industry '{damodaran_industry}' not found in cache")
            return None
        except Exception as e:
            logger.warning(f"Failed to read Damodaran India cache: {e}")
            return None

    def get_risk_free_rate(self) -> float:
        """
        Get India 10Y G-Sec yield (risk-free rate for DCF).

        Primary: Read from local market_indicators.csv (FRED 10Y bond yield)
        Fallback: Use cached/default value (0.071)
        """
        try:
            macro_path = os.getenv('MACRO_DATA_PATH', '')
            indicators_file = os.path.join(macro_path, 'market_indicators.csv')

            if os.path.exists(indicators_file):
                df = pd.read_csv(indicators_file, comment='#')
                # Filter for 10Y bond yield from FRED
                bond_df = df[df['series_name'].str.contains('10 year bond yield', case=False, na=False)]
                if not bond_df.empty:
                    bond_df = bond_df.sort_values('date', ascending=False)
                    latest_val = float(bond_df.iloc[0]['value'])
                    latest_date = bond_df.iloc[0]['date']
                    # FRED stores as raw number (6.62 = 6.62%), convert to decimal
                    rf = latest_val / 100.0
                    logger.info(f"Risk-free rate: India 10Y={rf:.4f} ({latest_val:.2f}%) from {latest_date} [ACTUAL: market_indicators.csv]")
                    return rf
                else:
                    logger.warning("No '10 year bond yield' series found in market_indicators.csv")
            else:
                logger.warning(f"market_indicators.csv not found at {indicators_file}")
        except Exception as e:
            logger.warning(f"Failed to read risk-free rate from macro data: {e}")

        default_rf = self.DEFAULT_INDIA_PARAMS['risk_free_rate_india']
        logger.info(f"Risk-free rate: {default_rf:.4f} [DEFAULT]")
        return default_rf

    def get_wacc_inputs(self, sector: str,
                        company_de_ratio: float = None,
                        company_tax_rate: float = 0.25,
                        valuation_subgroup: str = None) -> dict:
        """
        Get all WACC inputs for a company:
        - Risk-free rate
        - Equity risk premium
        - Beta (levered) — from Indian subgroup or Damodaran fallback
        - Cost of equity (CAPM)
        """
        erp_data = self.get_india_erp()
        beta_data = self.get_industry_beta(sector, company_de_ratio, company_tax_rate,
                                           valuation_subgroup=valuation_subgroup)
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
            'beta_source': beta_data['beta_source'],
            'cost_of_equity': round(cost_of_equity, 4),
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
                       company_tax_rate: float = 0.25,
                       valuation_subgroup: str = None) -> dict:
        """
        Convenience method: get all parameters needed for valuation.
        Uses Indian subgroup beta when available, Damodaran as fallback.
        """
        wacc_inputs = self.get_wacc_inputs(sector, company_de_ratio, company_tax_rate,
                                           valuation_subgroup=valuation_subgroup)
        erp_data = self.get_india_erp()

        return {
            **wacc_inputs,
            'india_rating': erp_data.get('india_rating', 'Baa3'),
            'india_default_spread': erp_data.get('india_default_spread', 0.0114),
        }
