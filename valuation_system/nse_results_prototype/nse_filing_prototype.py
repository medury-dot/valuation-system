"""
NSE Filing Data Prototype — Probe NSE APIs and compare with core CSV.

This script:
1. Establishes an NSE session (cookies from homepage)
2. Fetches quarterly/annual results for 5 pilot companies
3. Dumps raw JSON to cache/ for inspection
4. Compares key figures (sales, PBIDT, PAT) against core CSV
5. Generates a field inventory report

Usage:
    python -m valuation_system.nse_results_prototype.nse_filing_prototype > nse_prototype.log 2>&1

Output:
    - Raw JSON dumps in nse_results_prototype/cache/<symbol>/
    - Comparison CSV: nse_results_prototype/cache/nse_vs_core_comparison.csv
    - Field inventory: nse_results_prototype/cache/field_inventory.txt
"""

import os
import sys
import json
import time
import logging
import traceback
from datetime import datetime
from typing import Dict, List, Optional, Any, Tuple

import requests
import pandas as pd
from dotenv import load_dotenv

# --- Path setup for valuation_system imports ---
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))
load_dotenv(os.path.join(os.path.dirname(__file__), '..', 'config', '.env'))

from valuation_system.data.loaders.core_loader import CoreDataLoader

# --- Logging ---
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s [%(levelname)s] %(name)s: %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger('nse_prototype')

# --- Config from .env ---
CACHE_DIR = os.getenv('NSE_CACHE_DIR',
                       os.path.join(os.path.dirname(__file__), 'cache'))

# Pilot companies: (NSE symbol, company name for logging)
PILOT_COMPANIES = [
    ('EICHERMOT', 'Eicher Motors'),
    ('BEL', 'Bharat Electronics'),
    ('VBL', 'Varun Beverages'),
    ('AETHER', 'Aether Industries'),
    ('CRISIL', 'CRISIL'),
]

# Rate limiting: pause between API calls (seconds)
API_PAUSE = 1.5  # conservative — NSE tolerates ~3 req/sec but be polite

# NSE base URL
NSE_BASE = 'https://www.nseindia.com'

# Endpoints to probe per company
COMPANY_ENDPOINTS = {
    'results': '/api/results-comparision?symbol={symbol}',
    'quote': '/api/quote-equity?symbol={symbol}',
    'announcements': '/api/corporate-announcements?index=equities&symbol={symbol}',
}

# Global discovery endpoints (not per-company)
GLOBAL_ENDPOINTS = {
    'recent_filings_quarterly': '/api/corporates-financial-results?index=equities&period=Quarterly',
    'recent_filings_annual': '/api/corporates-financial-results?index=equities&period=Annual',
    'event_calendar': '/api/event-calendar',
}

# NSE field → our name mapping for results
# NSE quarterly results use `re_*` prefixed keys
NSE_FIELD_MAP = {
    # Revenue
    're_net_sale': 'revenue_from_operations',
    're_oth_inc_new': 'other_income',
    're_total_inc': 'total_income_alt',  # sometimes null
    # Expenses
    're_rawmat_consump': 'raw_material_cost',
    're_pur_trd_goods': 'purchase_of_traded_goods',
    're_inc_dre_sttr': 'change_in_inventories',
    're_staff_cost': 'employee_cost',
    're_oth_exp': 'other_expenses',
    're_oth_tot_exp': 'total_expenses',
    # Profitability
    're_depr_und_exp': 'depreciation',
    're_int_new': 'finance_cost',
    're_pro_loss_bef_tax': 'pbt',
    're_excepn_items_new': 'exceptional_items',
    're_tax': 'tax_expense_total',
    're_curr_tax': 'current_tax',
    're_deff_tax': 'deferred_tax',
    're_net_profit': 'pat',
    're_con_pro_loss': 'consolidated_pat',
    're_proloss_ord_act': 'profit_from_ordinary',
    # EPS
    're_basic_eps_for_cont_dic_opr': 'basic_eps_continuing',
    're_dilut_eps_for_cont_dic_opr': 'diluted_eps_continuing',
    're_basic_eps': 'basic_eps',
    're_diluted_eps': 'diluted_eps',
    # Share data
    're_pdup': 'paid_up_equity',
    're_face_val': 'face_value',
    # Discontinued operations
    're_pro_los_frm_dis_opr': 'pbt_discontinued',
    're_prolos_dis_opr_aftr_tax': 'pat_discontinued',
    're_tax_expens_of_dis_opr': 'tax_discontinued',
    # Associate / minority
    're_share_associate': 'share_of_associate',
    're_minority_int': 'minority_interest',
    # Banking-specific (null for non-banks)
    're_grs_npa': 'gross_npa',
    're_grs_npa_per': 'gross_npa_pct',
    're_int_expd': 'interest_expended',
    're_int_earned': 'interest_earned',
    're_ret_asset': 'return_on_assets',
    're_cap_ade_rat': 'capital_adequacy_ratio',
    're_debt_eqt_rat': 'debt_equity_ratio',
    're_int_ser_cov': 'interest_service_coverage',
    're_debt_ser_cov': 'debt_service_coverage',
    # Meta
    're_to_dt': 'period_end_date',
    're_from_dt': 'period_start_date',
    're_create_dt': 'filing_date',
    're_res_type': 'result_type',  # U=unaudited, A=audited
    're_seq_num': 'seq_number',
    're_remarks': 'remarks',
    're_notes_to_ac': 'notes',
    're_desc_note_seg': 'segment_notes',
    're_desc_note_fin': 'finance_notes',
    're_seg_remarks': 'segment_remarks',
    # Other operational
    're_oper_exp': 'operating_expense',
    're_oth_oper_exp': 'other_operating_expense',
    're_extraord_items': 'extraordinary_items',
    're_oth_pro_cont': 'other_provision_contingency',
    're_oth_inc': 'other_income_alt',
    're_oth': 'other_unclassified',
    're_tot_exp_exc_pro_cont': 'total_exp_excl_provisions',
    're_oper_exp_bef_pro_cont': 'operating_exp_before_provisions',
    # Debt
    're_paid_debt': 'paid_up_debt',
    're_face_value_debt': 'face_value_debt',
    're_debt_rdmption': 'debt_redemption',
    # Government / revaluation
    're_goi_per_shhd': 'govt_holding_pct',
    're_res_reval': 'revaluation_reserve',
    # Before exceptional
    're_pro_aft_int_bef_excep': 'profit_after_int_before_exceptional',
    're_bsc_eps_bfr_exi': 'basic_eps_before_extraordinary',
    're_dil_eps_bfr_exi': 'diluted_eps_before_extraordinary',
    # Banking
    're_bal_rbi_oth_bnk_funds': 'balance_rbi_other_bank_funds',
    're_cet_1_ret': 'cet1_ratio',
    're_per_grs_npa': 'percentage_gross_npa',
    're_amt_grs_np_asst': 'amount_gross_npa',
    're_prov_emp_pay': 'provision_employee_payable',
    # Income
    're_income_inv': 'income_from_investments',
    're_int_dis_adv_bills': 'interest_discounts_advances_bills',
    # Summary
    're_pro_loss_bef_tax_sum': 'pbt_summary',
}

# Map NSE period end date → core CSV quarter index
# Core CSV: index = (FY-1989)*4 + Q, where FY2025Q4=148, FY2026Q3=151
# NSE dates like "31-DEC-2024" = Q3 of FY2025 (Oct-Dec 2024)
MONTH_TO_QUARTER = {
    3: 4,   # Mar = Q4 (Jan-Mar)
    6: 1,   # Jun = Q1 (Apr-Jun)
    9: 2,   # Sep = Q2 (Jul-Sep)
    12: 3,  # Dec = Q3 (Oct-Dec)
}


def _nse_date_to_quarter_index(date_str: str) -> Optional[Tuple[int, str]]:
    """
    Convert NSE date string like '31-DEC-2024' to core CSV quarter index.
    Returns (quarter_index, quarter_label) or None.

    Indian fiscal year: FY2025 = Apr 2024 - Mar 2025
    Quarter index formula: (FY - 1989) * 4 + Q
    """
    try:
        dt = datetime.strptime(date_str, '%d-%b-%Y')
    except (ValueError, TypeError):
        return None

    month = dt.month
    year = dt.year
    q = MONTH_TO_QUARTER.get(month)
    if q is None:
        logger.warning(f"Unexpected month {month} in NSE date {date_str}")
        return None

    # Determine fiscal year
    if month <= 3:
        fy = year  # Jan-Mar belongs to FY ending that year
    else:
        fy = year + 1  # Apr-Dec belongs to FY ending next year

    idx = (fy - 1989) * 4 + q
    label = f"FY{fy}Q{q}"
    return idx, label


class NSESession:
    """
    Manages an NSE session.
    NSE homepage returns 403 but API endpoints work without cookies.
    We attempt cookie refresh but don't fail if it doesn't work.
    """

    HEADERS = {
        'User-Agent': ('Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) '
                       'AppleWebKit/537.36 (KHTML, like Gecko) '
                       'Chrome/120.0.0.0 Safari/537.36'),
        'Accept': 'application/json, text/plain, */*',
        'Accept-Language': 'en-US,en;q=0.9',
        'Accept-Encoding': 'gzip, deflate, br',
        'Referer': 'https://www.nseindia.com/',
        'Connection': 'keep-alive',
    }

    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update(self.HEADERS)
        self._cookie_refreshed_at = None

    def _refresh_cookies(self):
        """Try to get session cookies from NSE homepage (best-effort)."""
        logger.debug("Attempting NSE session cookie refresh...")
        try:
            resp = self.session.get(NSE_BASE, timeout=15)
            if resp.status_code == 200:
                self._cookie_refreshed_at = datetime.now()
                cookies = dict(self.session.cookies)
                logger.info(f"NSE cookies obtained: {list(cookies.keys())}")
            else:
                logger.debug(f"NSE homepage returned {resp.status_code} — "
                             f"proceeding without cookies (APIs may still work)")
                self._cookie_refreshed_at = datetime.now()  # don't retry constantly
        except Exception as e:
            logger.debug(f"Cookie refresh failed (non-fatal): {e}")
            self._cookie_refreshed_at = datetime.now()

    def _ensure_cookies(self):
        """Refresh cookies if stale (>4 min) or missing."""
        if (self._cookie_refreshed_at is None or
                (datetime.now() - self._cookie_refreshed_at).seconds > 240):
            self._refresh_cookies()

    def get(self, url: str, max_retries: int = 3) -> Optional[Dict]:
        """
        GET a URL with session cookies, retrying on failure.
        Returns parsed JSON or None on failure.
        """
        self._ensure_cookies()

        for attempt in range(1, max_retries + 1):
            try:
                logger.debug(f"GET (attempt {attempt}): {url}")
                resp = self.session.get(url, timeout=20)

                if resp.status_code == 401 or resp.status_code == 403:
                    logger.warning(f"Auth error ({resp.status_code}) for API, refreshing cookies...")
                    self._refresh_cookies()
                    time.sleep(API_PAUSE)
                    continue

                if resp.status_code == 429:
                    wait = API_PAUSE * (2 ** attempt)
                    logger.warning(f"Rate limited (429), waiting {wait}s...")
                    time.sleep(wait)
                    continue

                resp.raise_for_status()

                if not resp.text.strip():
                    logger.warning(f"Empty response body for {url}")
                    return {}

                data = resp.json()
                logger.debug(f"Response: {type(data).__name__}, "
                             f"{'len=' + str(len(data)) if isinstance(data, (list, dict)) else ''}")
                return data

            except requests.exceptions.JSONDecodeError:
                logger.warning(f"Non-JSON response for {url}: {resp.text[:200]}")
                return None
            except Exception as e:
                logger.warning(f"Attempt {attempt} failed for {url}: {e}")
                if attempt < max_retries:
                    time.sleep(API_PAUSE * attempt)
                else:
                    logger.error(f"All {max_retries} attempts failed for {url}\n{traceback.format_exc()}")
                    return None

        return None


def save_json(data: Any, filepath: str):
    """Save data as formatted JSON."""
    os.makedirs(os.path.dirname(filepath), exist_ok=True)
    with open(filepath, 'w') as f:
        json.dump(data, f, indent=2, default=str)
    logger.info(f"Saved: {filepath} ({os.path.getsize(filepath)} bytes)")


def fetch_company_data(nse: NSESession, symbol: str) -> Dict[str, Any]:
    """Fetch all available data for a single company from NSE APIs."""
    company_data = {}
    company_cache = os.path.join(CACHE_DIR, symbol)

    for endpoint_name, url_template in COMPANY_ENDPOINTS.items():
        url = NSE_BASE + url_template.format(symbol=symbol)
        logger.info(f"[{symbol}] Fetching {endpoint_name}...")

        data = nse.get(url)
        company_data[endpoint_name] = data

        filepath = os.path.join(company_cache, f'{endpoint_name}.json')
        save_json(data, filepath)

        time.sleep(API_PAUSE)

    return company_data


def fetch_global_data(nse: NSESession) -> Dict[str, Any]:
    """Fetch global/discovery endpoints (not per-company)."""
    global_data = {}
    global_cache = os.path.join(CACHE_DIR, '_global')

    for endpoint_name, url_path in GLOBAL_ENDPOINTS.items():
        url = NSE_BASE + url_path
        logger.info(f"[GLOBAL] Fetching {endpoint_name}...")

        data = nse.get(url)
        global_data[endpoint_name] = data

        filepath = os.path.join(global_cache, f'{endpoint_name}.json')
        save_json(data, filepath)

        time.sleep(API_PAUSE)

    return global_data


def inventory_fields(all_data: Dict[str, Dict]) -> str:
    """
    Build a field inventory from all fetched data.
    Returns a human-readable report string.
    """
    lines = []
    lines.append("=" * 80)
    lines.append("NSE API FIELD INVENTORY REPORT")
    lines.append(f"Generated: {datetime.now().isoformat()}")
    lines.append("=" * 80)

    # Collect all result fields across companies
    all_result_fields = {}

    for symbol, endpoints in all_data.items():
        lines.append(f"\n{'─' * 60}")
        lines.append(f"COMPANY: {symbol}")
        lines.append(f"{'─' * 60}")

        for endpoint_name, data in endpoints.items():
            lines.append(f"\n  Endpoint: {endpoint_name}")

            if data is None:
                lines.append("    [NO DATA] API returned None")
                continue

            if isinstance(data, dict):
                lines.append(f"    Type: dict, Keys: {len(data)}")
                for key, value in data.items():
                    if isinstance(value, list):
                        val_desc = f"list[{len(value)}]"
                        if value and isinstance(value[0], dict):
                            val_desc += f" of dicts with keys: {list(value[0].keys())[:10]}"
                            # Track result fields
                            if endpoint_name == 'results':
                                for item in value:
                                    for k, v in item.items():
                                        if k not in all_result_fields:
                                            all_result_fields[k] = {'non_null': 0, 'total': 0, 'sample': None}
                                        all_result_fields[k]['total'] += 1
                                        if v is not None and v != '' and v != '-':
                                            all_result_fields[k]['non_null'] += 1
                                            if all_result_fields[k]['sample'] is None:
                                                all_result_fields[k]['sample'] = v
                    elif isinstance(value, dict):
                        val_desc = f"dict with keys: {list(value.keys())[:10]}"
                    elif isinstance(value, str) and len(value) > 80:
                        val_desc = f"str({len(value)} chars): {value[:60]}..."
                    else:
                        val_desc = f"{type(value).__name__}: {str(value)[:80]}"
                    lines.append(f"    - {key}: {val_desc}")

            elif isinstance(data, list):
                lines.append(f"    Type: list[{len(data)}]")
                if data and isinstance(data[0], dict):
                    lines.append(f"    Item keys: {list(data[0].keys())}")
                    lines.append(f"    Sample item[0]:")
                    for k, v in data[0].items():
                        lines.append(f"      {k}: {str(v)[:80]}")
            else:
                lines.append(f"    Type: {type(data).__name__}, Value: {str(data)[:200]}")

    # Results field summary
    if all_result_fields:
        lines.append(f"\n{'=' * 80}")
        lines.append(f"RESULTS FIELD SUMMARY (across all companies)")
        lines.append(f"{'=' * 80}")
        lines.append(f"{'Field':<45} {'Non-null':>8} {'Total':>6} {'Our Name':<30} Sample")
        lines.append(f"{'─' * 130}")
        for field in sorted(all_result_fields.keys()):
            info = all_result_fields[field]
            our_name = NSE_FIELD_MAP.get(field, '???')
            sample = str(info['sample'])[:30] if info['sample'] is not None else '-'
            lines.append(f"  {field:<43} {info['non_null']:>8}/{info['total']:<6} {our_name:<30} {sample}")

    return '\n'.join(lines)


def _extract_results_quarters(results_data: Any) -> List[Dict]:
    """
    Extract quarterly/annual result records from the results-comparision endpoint.
    NSE returns: {"resCmpData": [...], "bankNonBnking": "N"}
    """
    if results_data is None:
        return []

    if isinstance(results_data, dict):
        # Primary structure: resCmpData list
        res_list = results_data.get('resCmpData')
        if isinstance(res_list, list):
            return res_list
        if res_list is None:
            logger.debug("resCmpData is null — company may not have results on NSE")
            return []

    if isinstance(results_data, list):
        return results_data

    return []


def compare_with_core_csv(all_data: Dict[str, Dict], core_loader: CoreDataLoader) -> pd.DataFrame:
    """
    Compare NSE API figures against core CSV for pilot companies.
    Returns a DataFrame with the comparison.
    """
    rows = []
    core_df = core_loader.df

    # Core CSV symbol column
    sym_col = 'CD_NSE Symbol1'
    if sym_col not in core_df.columns:
        logger.error(f"Core CSV missing '{sym_col}' column. Available: {[c for c in core_df.columns if 'symbol' in c.lower()]}")
        return pd.DataFrame()

    for symbol, endpoints in all_data.items():
        results_data = endpoints.get('results')
        if not results_data:
            logger.warning(f"[{symbol}] No results data for comparison")
            continue

        quarters = _extract_results_quarters(results_data)
        if not quarters:
            logger.warning(f"[{symbol}] Could not extract quarter records")
            continue

        logger.info(f"[{symbol}] Extracted {len(quarters)} quarter records for comparison")

        # Find company in core CSV by NSE symbol
        matches = core_df[core_df[sym_col].astype(str).str.strip().str.upper() == symbol.upper()]
        if len(matches) == 0:
            # Fallback: name search
            name_map = {
                'EICHERMOT': 'eicher',
                'BEL': 'bharat electr',
                'VBL': 'varun bev',
                'AETHER': 'aether',
                'CRISIL': 'crisil',
            }
            search_term = name_map.get(symbol, symbol.lower())
            matches = core_df[core_df['company_name'].astype(str).str.lower().str.contains(search_term, na=False)]

        if len(matches) == 0:
            logger.warning(f"[{symbol}] NOT FOUND in core CSV — skipping comparison")
            continue

        company_row = matches.iloc[0]
        logger.info(f"[{symbol}] Found in core CSV: {company_row.get('company_name', 'N/A')}")

        # Compare each NSE quarter
        for q_record in quarters:
            row = _build_comparison_row(symbol, q_record, company_row)
            if row:
                rows.append(row)

    if not rows:
        logger.warning("No comparison rows generated")
        return pd.DataFrame()

    return pd.DataFrame(rows)


def _build_comparison_row(symbol: str, nse_record: Dict, company_row: pd.Series) -> Optional[Dict]:
    """
    Build one comparison row: NSE values vs core CSV for a single quarter.
    """
    # Parse period from NSE
    period_end = nse_record.get('re_to_dt', '')
    period_start = nse_record.get('re_from_dt', '')
    result_type = nse_record.get('re_res_type', '')  # U=unaudited, A=audited
    filing_date = nse_record.get('re_create_dt', '')

    # Map to core CSV quarter index
    qi = _nse_date_to_quarter_index(period_end)
    q_idx = qi[0] if qi else None
    q_label = qi[1] if qi else 'UNKNOWN'

    # Extract NSE financials (values in LAKHS from NSE)
    # Core CSV is in CRORES, so we convert: lakhs / 100 = crores
    LAKHS_TO_CR = 100.0

    nse_sales_lakhs = _safe_float(nse_record.get('re_net_sale'))
    nse_other_income_lakhs = _safe_float(nse_record.get('re_oth_inc_new'))
    nse_total_income_lakhs = _safe_float(nse_record.get('re_total_inc'))
    if nse_total_income_lakhs is None and nse_sales_lakhs is not None:
        nse_total_income_lakhs = nse_sales_lakhs + (nse_other_income_lakhs or 0)
    nse_total_expenses_lakhs = _safe_float(nse_record.get('re_oth_tot_exp'))
    nse_depreciation_lakhs = _safe_float(nse_record.get('re_depr_und_exp'))
    nse_interest_lakhs = _safe_float(nse_record.get('re_int_new'))
    nse_pbt_lakhs = _safe_float(nse_record.get('re_pro_loss_bef_tax'))
    nse_tax_lakhs = _safe_float(nse_record.get('re_tax'))
    nse_pat_lakhs = _safe_float(nse_record.get('re_net_profit'))
    nse_paid_up_lakhs = _safe_float(nse_record.get('re_pdup'))
    nse_exceptional_lakhs = _safe_float(nse_record.get('re_excepn_items_new'))
    nse_rawmat_lakhs = _safe_float(nse_record.get('re_rawmat_consump'))
    nse_staff_cost_lakhs = _safe_float(nse_record.get('re_staff_cost'))
    nse_other_exp_lakhs = _safe_float(nse_record.get('re_oth_exp'))
    nse_curr_tax_lakhs = _safe_float(nse_record.get('re_curr_tax'))
    nse_def_tax_lakhs = _safe_float(nse_record.get('re_deff_tax'))
    # EPS is per-share, not in lakhs — no conversion needed
    nse_eps_basic = _safe_float(nse_record.get('re_basic_eps_for_cont_dic_opr'))
    nse_eps_diluted = _safe_float(nse_record.get('re_dilut_eps_for_cont_dic_opr'))

    # Convert to crores for comparison with core CSV
    nse_sales = nse_sales_lakhs / LAKHS_TO_CR if nse_sales_lakhs is not None else None
    nse_other_income = nse_other_income_lakhs / LAKHS_TO_CR if nse_other_income_lakhs is not None else None
    nse_total_income = nse_total_income_lakhs / LAKHS_TO_CR if nse_total_income_lakhs is not None else None
    nse_total_expenses = nse_total_expenses_lakhs / LAKHS_TO_CR if nse_total_expenses_lakhs is not None else None
    nse_depreciation = nse_depreciation_lakhs / LAKHS_TO_CR if nse_depreciation_lakhs is not None else None
    nse_interest = nse_interest_lakhs / LAKHS_TO_CR if nse_interest_lakhs is not None else None
    nse_pbt = nse_pbt_lakhs / LAKHS_TO_CR if nse_pbt_lakhs is not None else None
    nse_tax = nse_tax_lakhs / LAKHS_TO_CR if nse_tax_lakhs is not None else None
    nse_pat = nse_pat_lakhs / LAKHS_TO_CR if nse_pat_lakhs is not None else None
    nse_paid_up = nse_paid_up_lakhs / LAKHS_TO_CR if nse_paid_up_lakhs is not None else None
    nse_exceptional = nse_exceptional_lakhs / LAKHS_TO_CR if nse_exceptional_lakhs is not None else None

    # Compute PBIDT in crores: PBT + Depreciation + Interest
    nse_pbidt = None
    if nse_pbt is not None and nse_depreciation is not None:
        nse_pbidt = nse_pbt + nse_depreciation + (nse_interest or 0)

    # Core CSV quarterly values (in CRORES — same unit after conversion)
    core_sales = None
    core_pat = None
    core_pbidt = None
    core_interest = None
    if q_idx is not None:
        core_sales = _safe_float(company_row.get(f'sales_{q_idx}'))
        core_pat = _safe_float(company_row.get(f'pat_{q_idx}'))
        core_pbidt = _safe_float(company_row.get(f'pbidt_{q_idx}'))
        core_interest = _safe_float(company_row.get(f'interest_{q_idx}'))

    # Compute match percentages
    sales_diff_pct = _pct_diff(nse_sales, core_sales)
    pat_diff_pct = _pct_diff(nse_pat, core_pat)
    pbidt_diff_pct = _pct_diff(nse_pbidt, core_pbidt)

    row = {
        'symbol': symbol,
        'period_end': period_end,
        'period_start': period_start,
        'quarter_label': q_label,
        'quarter_index': q_idx,
        'result_type': result_type,
        'filing_date': filing_date,
        # NSE figures (lakhs)
        'nse_sales': nse_sales,
        'nse_other_income': nse_other_income,
        'nse_total_income': nse_total_income,
        'nse_total_expenses': nse_total_expenses,
        'nse_pbidt': nse_pbidt,
        'nse_depreciation': nse_depreciation,
        'nse_interest': nse_interest,
        'nse_pbt': nse_pbt,
        'nse_exceptional': nse_exceptional,
        'nse_tax': nse_tax,
        'nse_pat': nse_pat,
        'nse_eps_basic': nse_eps_basic,
        'nse_eps_diluted': nse_eps_diluted,
        'nse_paid_up_equity': nse_paid_up,
        # Core CSV figures (lakhs)
        'core_sales': core_sales,
        'core_pat': core_pat,
        'core_pbidt': core_pbidt,
        'core_interest': core_interest,
        # Comparison
        'sales_diff_pct': sales_diff_pct,
        'pat_diff_pct': pat_diff_pct,
        'pbidt_diff_pct': pbidt_diff_pct,
        'sales_match': 'YES' if sales_diff_pct is not None and abs(sales_diff_pct) < 1.0 else
                        ('CLOSE' if sales_diff_pct is not None and abs(sales_diff_pct) < 5.0 else
                         ('NO' if sales_diff_pct is not None else 'N/A')),
    }

    # Also dump all raw NSE fields for this quarter
    for k, v in nse_record.items():
        row[f'nse_raw_{k}'] = v

    return row


def _safe_float(val) -> Optional[float]:
    """Convert to float, returning None for null/empty/dash."""
    if val is None or val == '' or val == '-':
        return None
    try:
        return float(val)
    except (ValueError, TypeError):
        return None


def _pct_diff(nse_val: Optional[float], core_val: Optional[float]) -> Optional[float]:
    """Percentage difference: (NSE - Core) / Core * 100. None if either missing."""
    if nse_val is None or core_val is None or core_val == 0:
        return None
    return round((nse_val - core_val) / abs(core_val) * 100, 2)


def generate_comparison_report(comparison_df: pd.DataFrame) -> str:
    """Generate a human-readable comparison summary."""
    lines = []
    lines.append("\n" + "=" * 80)
    lines.append("NSE vs CORE CSV COMPARISON REPORT")
    lines.append("=" * 80)

    if comparison_df.empty:
        lines.append("No comparison data available.")
        return '\n'.join(lines)

    for symbol in comparison_df['symbol'].unique():
        sym_df = comparison_df[comparison_df['symbol'] == symbol].sort_values('quarter_index', ascending=False)
        lines.append(f"\n{'─' * 70}")
        lines.append(f"SYMBOL: {symbol} — {len(sym_df)} quarters")
        lines.append(f"{'─' * 70}")
        lines.append(f"  {'Quarter':<12} {'NSE Sales':>12} {'Core Sales':>12} {'Diff%':>8}  "
                      f"{'NSE PAT':>12} {'Core PAT':>12} {'Diff%':>8}  Match")
        lines.append(f"  {'─' * 68}")

        for _, row in sym_df.iterrows():
            q = row.get('quarter_label', 'N/A')
            ns = f"{row['nse_sales']:,.0f}" if pd.notna(row.get('nse_sales')) else 'N/A'
            cs = f"{row['core_sales']:,.0f}" if pd.notna(row.get('core_sales')) else 'N/A'
            sd = f"{row['sales_diff_pct']:+.1f}%" if pd.notna(row.get('sales_diff_pct')) else 'N/A'
            np_ = f"{row['nse_pat']:,.0f}" if pd.notna(row.get('nse_pat')) else 'N/A'
            cp = f"{row['core_pat']:,.0f}" if pd.notna(row.get('core_pat')) else 'N/A'
            pd_ = f"{row['pat_diff_pct']:+.1f}%" if pd.notna(row.get('pat_diff_pct')) else 'N/A'
            match = row.get('sales_match', 'N/A')
            lines.append(f"  {q:<12} {ns:>12} {cs:>12} {sd:>8}  {np_:>12} {cp:>12} {pd_:>8}  {match}")

        # Additional detail: EPS, depreciation, etc.
        lines.append(f"\n  Additional fields (latest quarter):")
        latest = sym_df.iloc[0]
        for field in ['nse_eps_basic', 'nse_eps_diluted', 'nse_depreciation',
                       'nse_interest', 'nse_exceptional', 'nse_paid_up_equity']:
            val = latest.get(field)
            if pd.notna(val):
                label = field.replace('nse_', '').replace('_', ' ').title()
                lines.append(f"    {label}: {val:,.2f}")

    # Summary stats
    lines.append(f"\n{'=' * 70}")
    lines.append("SUMMARY")
    lines.append(f"{'=' * 70}")
    total = len(comparison_df)
    matched = len(comparison_df[comparison_df['sales_match'] == 'YES'])
    close = len(comparison_df[comparison_df['sales_match'] == 'CLOSE'])
    no_match = len(comparison_df[comparison_df['sales_match'] == 'NO'])
    na = len(comparison_df[comparison_df['sales_match'] == 'N/A'])
    lines.append(f"  Total quarters compared: {total}")
    lines.append(f"  Sales match (< 1% diff): {matched}")
    lines.append(f"  Sales close (< 5% diff): {close}")
    lines.append(f"  Sales mismatch (>= 5%):  {no_match}")
    lines.append(f"  No data for comparison:  {na}")

    # List all NSE fields observed
    nse_raw_cols = sorted([c for c in comparison_df.columns if c.startswith('nse_raw_')])
    if nse_raw_cols:
        lines.append(f"\n{'─' * 70}")
        lines.append(f"ALL NSE RESULT FIELDS ({len(nse_raw_cols)} unique)")
        lines.append(f"{'─' * 70}")
        for col in nse_raw_cols:
            field = col.replace('nse_raw_', '')
            non_null = comparison_df[col].apply(lambda x: x is not None and x != '' and x != '-').sum()
            our_name = NSE_FIELD_MAP.get(field, '???')
            sample = comparison_df[col].dropna().iloc[0] if non_null > 0 else 'N/A'
            lines.append(f"  {field:<40} {non_null:>3}/{total} non-null  -> {our_name}")

    return '\n'.join(lines)


def generate_global_data_report(global_data: Dict[str, Any]) -> str:
    """Generate a report on global/discovery endpoints."""
    lines = []
    lines.append("\n" + "=" * 80)
    lines.append("GLOBAL / DISCOVERY ENDPOINTS REPORT")
    lines.append("=" * 80)

    # Recent filings
    for period in ['recent_filings_quarterly', 'recent_filings_annual']:
        data = global_data.get(period, [])
        if isinstance(data, list):
            lines.append(f"\n  {period}: {len(data)} filings")
            if data:
                lines.append(f"    Fields per filing: {list(data[0].keys())}")
                # Show most recent 5
                lines.append(f"    Most recent 5:")
                for item in data[:5]:
                    lines.append(f"      {item.get('symbol','?')}: {item.get('companyName','?')} "
                                 f"[{item.get('relatingTo','?')}] filed {item.get('filingDate','?')}")

    # Event calendar
    events = global_data.get('event_calendar', [])
    if isinstance(events, list):
        lines.append(f"\n  event_calendar: {len(events)} upcoming events")
        if events:
            lines.append(f"    Fields: {list(events[0].keys())}")
            # Count by purpose
            purposes = {}
            for e in events:
                p = e.get('purpose', 'Unknown')
                purposes[p] = purposes.get(p, 0) + 1
            lines.append(f"    By purpose:")
            for p, count in sorted(purposes.items(), key=lambda x: -x[1]):
                lines.append(f"      {p}: {count}")
            # Show next 5 financial results
            results_events = [e for e in events if 'result' in str(e.get('purpose', '')).lower()]
            if results_events:
                lines.append(f"    Next 5 financial results board meetings:")
                for e in results_events[:5]:
                    lines.append(f"      {e.get('date','?')} — {e.get('symbol','?')} ({e.get('company','?')})")

    return '\n'.join(lines)


def generate_core_format_csv(all_data: Dict[str, Dict], core_loader: CoreDataLoader) -> str:
    """
    Generate a CSV in core-CSV-like format: one row per company, quarterly columns.
    Columns: company_name, nse_symbol, sales_149, sales_150, sales_151, pat_149, etc.
    Values in CRORES (NSE lakhs / 100) to match core CSV units.

    Also includes fields NOT in core CSV (employee_cost, raw_material, depreciation, etc.)
    as bonus columns.

    Returns the output file path.
    """
    LAKHS_TO_CR = 100.0
    core_df = core_loader.df
    sym_col = 'CD_NSE Symbol1'

    # Metrics to extract and their NSE source fields
    METRICS = {
        'sales': 're_net_sale',
        'pat': 're_net_profit',
        'pbidt': None,           # derived: pbt + dep + interest
        'interest': 're_int_new',
        'pbt_excp': 're_pro_loss_bef_tax',
        'totalincome': None,     # derived: net_sale + other_income
        # --- Bonus fields not in core CSV quarterly columns ---
        'other_income': 're_oth_inc_new',
        'depreciation': 're_depr_und_exp',
        'empcost': 're_staff_cost',
        'rawmat': 're_rawmat_consump',
        'other_exp': 're_oth_exp',
        'total_exp': 're_oth_tot_exp',
        'tax': 're_tax',
        'curr_tax': 're_curr_tax',
        'def_tax': 're_deff_tax',
        'exceptional': 're_excepn_items_new',
        'paid_up_equity': 're_pdup',
        'basic_eps': 're_basic_eps_for_cont_dic_opr',
        'diluted_eps': 're_dilut_eps_for_cont_dic_opr',
    }

    # EPS fields are per-share (no lakhs→crores conversion)
    NO_CONVERT = {'basic_eps', 'diluted_eps'}

    rows = []

    for symbol, endpoints in all_data.items():
        quarters = _extract_results_quarters(endpoints.get('results'))
        if not quarters:
            continue

        # Find company name from core CSV
        company_name = symbol
        if sym_col in core_df.columns:
            matches = core_df[core_df[sym_col].astype(str).str.strip().str.upper() == symbol.upper()]
            if len(matches) > 0:
                company_name = matches.iloc[0].get('company_name', symbol)

        # Get quote data for extra info
        quote = endpoints.get('quote', {})
        isin = ''
        industry = ''
        if isinstance(quote, dict) and 'info' in quote:
            isin = quote['info'].get('isin', '')
            industry = quote['info'].get('industry', '')

        row = {
            'company_name': company_name,
            'nse_symbol': symbol,
            'isin': isin,
            'nse_industry': industry,
            'data_source': 'NSE_FILING',
            'fetch_date': datetime.now().strftime('%Y-%m-%d'),
        }

        # Process each quarter
        for q_record in quarters:
            period_end = q_record.get('re_to_dt', '')
            qi = _nse_date_to_quarter_index(period_end)
            if qi is None:
                continue
            q_idx, q_label = qi

            for metric_name, nse_field in METRICS.items():
                if nse_field is not None:
                    raw_val = _safe_float(q_record.get(nse_field))
                    if raw_val is not None:
                        if metric_name in NO_CONVERT:
                            row[f'{metric_name}_{q_idx}'] = round(raw_val, 2)
                        else:
                            row[f'{metric_name}_{q_idx}'] = round(raw_val / LAKHS_TO_CR, 2)
                elif metric_name == 'pbidt':
                    # Derived: PBT + Depreciation + Interest
                    pbt = _safe_float(q_record.get('re_pro_loss_bef_tax'))
                    dep = _safe_float(q_record.get('re_depr_und_exp'))
                    intr = _safe_float(q_record.get('re_int_new'))
                    if pbt is not None and dep is not None:
                        row[f'pbidt_{q_idx}'] = round((pbt + dep + (intr or 0)) / LAKHS_TO_CR, 2)
                elif metric_name == 'totalincome':
                    # Derived: net_sale + other_income
                    ns = _safe_float(q_record.get('re_net_sale'))
                    oi = _safe_float(q_record.get('re_oth_inc_new'))
                    if ns is not None:
                        row[f'totalincome_{q_idx}'] = round((ns + (oi or 0)) / LAKHS_TO_CR, 2)

            # Also store filing metadata per quarter
            row[f'filing_date_{q_idx}'] = q_record.get('re_create_dt', '')
            row[f'result_type_{q_idx}'] = q_record.get('re_res_type', '')

        rows.append(row)

    if not rows:
        logger.warning("No data for core-format CSV")
        return ''

    df = pd.DataFrame(rows)

    # Sort columns: fixed cols first, then metric columns sorted by index
    fixed_cols = ['company_name', 'nse_symbol', 'isin', 'nse_industry', 'data_source', 'fetch_date']
    metric_cols = sorted([c for c in df.columns if c not in fixed_cols],
                         key=lambda x: (x.rsplit('_', 1)[0], int(x.rsplit('_', 1)[1]) if x.rsplit('_', 1)[1].isdigit() else 0))
    df = df[fixed_cols + metric_cols]

    output_path = os.path.join(CACHE_DIR, 'nse_quarterly_data.csv')
    df.to_csv(output_path, index=False)
    logger.info(f"Core-format CSV saved: {output_path} ({len(df)} companies, {len(df.columns)} columns)")

    # Print summary
    q_indices = set()
    for col in metric_cols:
        parts = col.rsplit('_', 1)
        if len(parts) == 2 and parts[1].isdigit():
            q_indices.add(int(parts[1]))
    q_labels = []
    for qi in sorted(q_indices):
        fy = 1989 + (qi - 1) // 4
        q = (qi - 1) % 4 + 1
        q_labels.append(f"FY{fy}Q{q}(idx={qi})")
    logger.info(f"  Quarters covered: {', '.join(q_labels)}")
    logger.info(f"  Metrics per quarter: {sorted(set(c.rsplit('_', 1)[0] for c in metric_cols if c.rsplit('_', 1)[1].isdigit()))}")

    return output_path


def main():
    """Run the NSE filing prototype: fetch, cache, compare, report."""
    logger.info("=" * 80)
    logger.info("NSE FILING DATA PROTOTYPE — Starting")
    logger.info(f"Cache dir: {CACHE_DIR}")
    logger.info(f"Pilot companies: {[s for s, _ in PILOT_COMPANIES]}")
    logger.info("=" * 80)

    os.makedirs(CACHE_DIR, exist_ok=True)

    # Check for cached data from a previous run
    use_cache = True
    for symbol, _ in PILOT_COMPANIES:
        results_file = os.path.join(CACHE_DIR, symbol, 'results.json')
        if not os.path.exists(results_file):
            use_cache = False
            break

    nse = NSESession()

    if use_cache:
        logger.info("Found cached data from previous run — loading from disk (skip API calls)")
        logger.info("Delete cache/ to force a fresh fetch")

        # Load cached global data
        global_data = {}
        global_cache = os.path.join(CACHE_DIR, '_global')
        for endpoint_name in GLOBAL_ENDPOINTS:
            filepath = os.path.join(global_cache, f'{endpoint_name}.json')
            if os.path.exists(filepath):
                with open(filepath) as f:
                    global_data[endpoint_name] = json.load(f)
                logger.info(f"[GLOBAL] Loaded cached: {endpoint_name}")

        # Load cached company data
        all_company_data = {}
        for symbol, _ in PILOT_COMPANIES:
            company_data = {}
            for endpoint_name in COMPANY_ENDPOINTS:
                filepath = os.path.join(CACHE_DIR, symbol, f'{endpoint_name}.json')
                if os.path.exists(filepath):
                    with open(filepath) as f:
                        company_data[endpoint_name] = json.load(f)
            all_company_data[symbol] = company_data
            logger.info(f"[{symbol}] Loaded cached data: {list(company_data.keys())}")
    else:
        # --- Step 1: Fetch global discovery endpoints ---
        logger.info("\n>>> STEP 1: Global discovery endpoints")
        global_data = fetch_global_data(nse)

        # --- Step 2: Fetch per-company data ---
        logger.info("\n>>> STEP 2: Per-company data")
        all_company_data = {}
        for symbol, company_name in PILOT_COMPANIES:
            logger.info(f"\n--- Fetching: {symbol} ({company_name}) ---")
            try:
                company_data = fetch_company_data(nse, symbol)
                all_company_data[symbol] = company_data
            except Exception as e:
                logger.error(f"Failed to fetch {symbol}: {e}\n{traceback.format_exc()}")
                all_company_data[symbol] = {}

    # --- Step 3: Generate field inventory ---
    logger.info("\n>>> STEP 3: Field inventory")
    inventory = inventory_fields(all_company_data)
    inventory_path = os.path.join(CACHE_DIR, 'field_inventory.txt')
    with open(inventory_path, 'w') as f:
        f.write(inventory)
    logger.info(f"Field inventory saved: {inventory_path}")
    print(inventory)

    # --- Step 4: Global data report ---
    logger.info("\n>>> STEP 4: Global data report")
    global_report = generate_global_data_report(global_data)
    print(global_report)
    global_report_path = os.path.join(CACHE_DIR, 'global_data_report.txt')
    with open(global_report_path, 'w') as f:
        f.write(global_report)

    # --- Step 5: Compare with core CSV ---
    logger.info("\n>>> STEP 5: Comparing with core CSV")
    try:
        core_loader = CoreDataLoader()
        comparison_df = compare_with_core_csv(all_company_data, core_loader)

        if not comparison_df.empty:
            # Save comparison CSV (main columns only)
            main_cols = [c for c in comparison_df.columns if not c.startswith('nse_raw_')]
            raw_cols = ['symbol', 'period_end', 'quarter_label'] + \
                       [c for c in comparison_df.columns if c.startswith('nse_raw_')]

            comp_path = os.path.join(CACHE_DIR, 'nse_vs_core_comparison.csv')
            comparison_df[main_cols].to_csv(comp_path, index=False)
            logger.info(f"Comparison CSV saved: {comp_path}")

            raw_path = os.path.join(CACHE_DIR, 'nse_raw_fields_all.csv')
            comparison_df[raw_cols].to_csv(raw_path, index=False)
            logger.info(f"Raw fields CSV saved: {raw_path}")

            # Print report
            report = generate_comparison_report(comparison_df)
            print(report)

            report_path = os.path.join(CACHE_DIR, 'comparison_report.txt')
            with open(report_path, 'w') as f:
                f.write(report)
            logger.info(f"Comparison report saved: {report_path}")
        else:
            logger.warning("Comparison DataFrame is empty — no matching data found")
    except Exception as e:
        logger.error(f"Comparison step failed: {e}\n{traceback.format_exc()}")

    # --- Step 6: Generate core-format CSV ---
    logger.info("\n>>> STEP 6: Generating core-CSV-format output")
    try:
        try:
            core_loader  # check if already created in step 5
        except NameError:
            core_loader = CoreDataLoader()
        core_format_path = generate_core_format_csv(all_company_data, core_loader)
    except Exception as e:
        logger.error(f"Core-format CSV generation failed: {e}\n{traceback.format_exc()}")
        core_format_path = ''

    # --- Step 7: Summary ---
    logger.info("\n" + "=" * 80)
    logger.info("NSE FILING PROTOTYPE — Complete")
    logger.info(f"Cache dir: {CACHE_DIR}")
    logger.info(f"Companies fetched: {len(all_company_data)}")
    for symbol in all_company_data:
        endpoints_ok = sum(1 for v in all_company_data[symbol].values() if v is not None)
        endpoints_total = len(all_company_data[symbol])
        quarters = len(_extract_results_quarters(all_company_data[symbol].get('results')))
        logger.info(f"  {symbol}: {endpoints_ok}/{endpoints_total} endpoints, {quarters} quarters of results")
    logger.info("=" * 80)
    logger.info(f"\nOutput files:")
    logger.info(f"  Field inventory:    {os.path.join(CACHE_DIR, 'field_inventory.txt')}")
    logger.info(f"  Global data report: {os.path.join(CACHE_DIR, 'global_data_report.txt')}")
    logger.info(f"  Comparison CSV:     {os.path.join(CACHE_DIR, 'nse_vs_core_comparison.csv')}")
    logger.info(f"  Comparison report:  {os.path.join(CACHE_DIR, 'comparison_report.txt')}")
    logger.info(f"  Raw NSE fields:     {os.path.join(CACHE_DIR, 'nse_raw_fields_all.csv')}")
    logger.info(f"  Core-format CSV:    {os.path.join(CACHE_DIR, 'nse_quarterly_data.csv')}")


if __name__ == '__main__':
    main()
