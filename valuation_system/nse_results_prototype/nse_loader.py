"""
NSE Filing Data — Production Loader

Event-driven fetching: discover new filings from global endpoints, fetch only
companies with new data, update tracker state, and merge into CSV.

Modes:
  --mode daily     Event-driven: discover today's filings, fetch only new ones (default)
  --mode sweep     Full sweep: fetch ALL tracked companies (quarterly safety net)
  --mode seed      Initial seeding: register all active companies in tracker, then sweep
  --mode symbol    Fetch a single symbol: --symbol EICHERMOT

Usage:
  python -m valuation_system.nse_results_prototype.nse_loader --mode daily
  python -m valuation_system.nse_results_prototype.nse_loader --mode sweep > /tmp/nse_sweep.log 2>&1
  python -m valuation_system.nse_results_prototype.nse_loader --mode seed > /tmp/nse_seed.log 2>&1
  python -m valuation_system.nse_results_prototype.nse_loader --mode symbol --symbol EICHERMOT
"""

import os
import sys
import json
import time
import hashlib
import logging
import argparse
import traceback
import csv
from datetime import datetime, date, timedelta
from typing import Dict, List, Optional, Any, Tuple, Set

import pandas as pd
from dotenv import load_dotenv

# --- Path setup ---
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))
load_dotenv(os.path.join(os.path.dirname(__file__), '..', 'config', '.env'))

from valuation_system.nse_results_prototype.nse_filing_prototype import (
    NSESession, NSE_BASE, NSE_FIELD_MAP, MONTH_TO_QUARTER,
    _nse_date_to_quarter_index, _safe_float, _extract_results_quarters,
    GLOBAL_ENDPOINTS, COMPANY_ENDPOINTS,
)
from valuation_system.storage.mysql_client import get_mysql_client

# --- Logging ---
LOG_DIR = os.getenv('LOG_DIR', os.path.join(os.path.dirname(__file__), '..', 'logs'))
os.makedirs(LOG_DIR, exist_ok=True)

logging.basicConfig(
    level=os.getenv('LOG_LEVEL', 'DEBUG'),
    format='%(asctime)s [%(levelname)s] %(name)s: %(message)s',
    handlers=[
        logging.FileHandler(os.path.join(LOG_DIR, f"nse_loader_{datetime.now().strftime('%Y-%m-%d')}.log")),
        logging.StreamHandler(sys.stdout),
    ]
)
logger = logging.getLogger('valuation_system.nse_loader')

# --- Config from .env ---
NSE_QUARTERLY_CSV_PATH = os.getenv(
    'NSE_QUARTERLY_CSV_PATH',
    os.path.join(os.path.dirname(__file__), 'cache', 'nse_quarterly_data.csv')
)
NSE_CACHE_DIR = os.getenv(
    'NSE_CACHE_DIR',
    os.path.join(os.path.dirname(__file__), 'cache')
)
BATCH_SIZE = int(os.getenv('NSE_FETCH_BATCH_SIZE', '50'))
API_PAUSE = float(os.getenv('NSE_RATE_LIMIT_PAUSE', '1.5'))
LAKHS_TO_CR = 100.0

# Metrics to extract from NSE results → CSV columns
METRICS = {
    'sales': 're_net_sale',
    'pat': 're_net_profit',
    'pbidt': None,           # derived: pbt + dep + interest
    'interest': 're_int_new',
    'pbt_excp': 're_pro_loss_bef_tax',
    'totalincome': None,     # derived: net_sale + other_income
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
NO_CONVERT = {'basic_eps', 'diluted_eps'}  # per-share, not in lakhs


class NSELoader:
    """
    Production NSE filing data loader.

    Lifecycle:
    1. discover()  — Find companies with new filings (event-driven)
    2. fetch()     — Fetch results for discovered companies
    3. store()     — Update MySQL tracker + merge into CSV
    4. validate()  — Compare against core CSV where overlap exists

    State is tracked in vs_nse_fetch_tracker (MySQL).
    """

    def __init__(self):
        self.mysql = get_mysql_client()
        self.nse = NSESession()
        self.issues: List[Dict] = []  # Batch issues log
        self._start_time = datetime.now()

    # =========================================================================
    # DISCOVERY: Find companies with new filings
    # =========================================================================

    def discover_new_filings(self) -> Dict[str, Any]:
        """
        Call global NSE endpoints to find companies that filed today/recently.
        Returns dict with 'board_meetings' and 'recent_filings' symbol sets.
        """
        logger.info("=== DISCOVER: Checking for new filings ===")
        result = {'board_meetings': set(), 'recent_filings': {}, 'api_calls': 0}

        # 1. Event calendar — board meetings (often result announcements)
        url = NSE_BASE + GLOBAL_ENDPOINTS['event_calendar']
        events = self.nse.get(url)
        result['api_calls'] += 1
        time.sleep(API_PAUSE)

        if isinstance(events, list):
            today_str = date.today().strftime('%d-%b-%Y')
            for ev in events:
                purpose = str(ev.get('purpose', '')).lower()
                ev_date = str(ev.get('date', ''))
                symbol = ev.get('symbol', '')
                if symbol and 'result' in purpose:
                    # Board meeting for financial results
                    result['board_meetings'].add(symbol)
                    if ev_date == today_str:
                        logger.info(f"  [BOARD TODAY] {symbol}: {ev.get('company', '')} — {ev.get('purpose', '')}")
            logger.info(f"  Event calendar: {len(events)} events, {len(result['board_meetings'])} result-related")
        else:
            logger.warning(f"  Event calendar returned unexpected type: {type(events)}")

        # 2. Global filings list — all recent quarterly filings
        url = NSE_BASE + GLOBAL_ENDPOINTS['recent_filings_quarterly']
        filings = self.nse.get(url)
        result['api_calls'] += 1
        time.sleep(API_PAUSE)

        if isinstance(filings, list):
            for f in filings:
                symbol = f.get('symbol', '')
                filing_date_str = f.get('filingDate', '')
                relating_to = f.get('relatingTo', '')
                xbrl_url = f.get('xbrl', '')
                if symbol:
                    result['recent_filings'][symbol] = {
                        'filing_date': filing_date_str,
                        'relating_to': relating_to,
                        'xbrl_url': xbrl_url,
                        'company_name': f.get('companyName', ''),
                    }
            logger.info(f"  Global filings: {len(filings)} quarterly, {len(result['recent_filings'])} unique symbols")
        else:
            logger.warning(f"  Global filings returned unexpected type: {type(filings)}")

        return result

    def decide_fetch_list(self, discovery: Dict, mode: str = 'daily',
                          symbols: List[str] = None, min_mcap_cr: float = 2500.0) -> List[Dict]:
        """
        Decide which companies to fetch based on mode and discovery.

        Args:
            discovery: Dict from discover_new_filings()
            mode: 'daily' | 'sweep' | 'seed' | 'symbol'
            symbols: list of symbols for 'symbol' mode
            min_mcap_cr: minimum market cap in crores for seed mode (default: 2500)

        Returns list of dicts: [{'nse_symbol': ..., 'company_id': ..., 'reason': ...}, ...]
        """
        logger.info(f"=== DECIDE: mode={mode} ===")

        if mode == 'symbol' and symbols:
            # Single symbol mode
            fetch_list = []
            for sym in symbols:
                company = self.mysql.get_company_by_symbol(sym)
                if company:
                    fetch_list.append({
                        'nse_symbol': sym,
                        'company_id': company['id'],
                        'company_name': company.get('company_name', sym),
                        'reason': 'manual',
                    })
                else:
                    logger.warning(f"  Symbol {sym} not found in marketscrip — will fetch without company_id")
                    fetch_list.append({
                        'nse_symbol': sym,
                        'company_id': None,
                        'company_name': sym,
                        'reason': 'manual',
                    })
            logger.info(f"  Symbol mode: {len(fetch_list)} companies")
            return fetch_list

        # Get all tracked symbols from MySQL
        tracked = {}
        tracker_rows = self.mysql.query(
            "SELECT nse_symbol, company_id, latest_quarter_end, last_fetch_date, data_hash "
            "FROM vs_nse_fetch_tracker"
        )
        for row in tracker_rows:
            tracked[row['nse_symbol']] = row

        if mode == 'sweep':
            # Full sweep: all tracked companies
            fetch_list = []
            for sym, row in tracked.items():
                fetch_list.append({
                    'nse_symbol': sym,
                    'company_id': row['company_id'],
                    'company_name': sym,
                    'reason': 'sweep',
                })
            logger.info(f"  Sweep mode: {len(fetch_list)} companies")
            return fetch_list

        if mode == 'seed':
            # Seed: register all active companies (filtered by mcap), then sweep
            return self._seed_and_list(min_mcap_cr=min_mcap_cr)

        # Daily mode: event-driven
        fetch_list = []
        board_meetings = discovery.get('board_meetings', set())
        recent_filings = discovery.get('recent_filings', {})

        for sym in board_meetings | set(recent_filings.keys()):
            if sym not in tracked:
                # Not tracked yet — skip (must be seeded first)
                continue

            tracker_row = tracked[sym]
            reason = []

            # Check if company had a board meeting for results
            if sym in board_meetings:
                reason.append('board_meeting')

            # Check if filing is newer than what we have
            if sym in recent_filings:
                filing_info = recent_filings[sym]
                filing_date_str = filing_info.get('filing_date', '')
                try:
                    filing_dt = datetime.strptime(filing_date_str, '%d-%b-%Y').date()
                    if tracker_row['latest_quarter_end'] is None or filing_dt > tracker_row['latest_quarter_end']:
                        reason.append('newer_filing')
                except (ValueError, TypeError):
                    pass

            if reason:
                fetch_list.append({
                    'nse_symbol': sym,
                    'company_id': tracker_row['company_id'],
                    'company_name': sym,
                    'reason': '+'.join(reason),
                    'xbrl_url': recent_filings.get(sym, {}).get('xbrl_url', ''),
                })

        logger.info(f"  Daily mode: {len(fetch_list)} companies to fetch "
                     f"(board: {len(board_meetings)}, filings: {len(recent_filings)})")
        return fetch_list

    def _seed_and_list(self, min_mcap_cr: float = 2500.0) -> List[Dict]:
        """
        Register all active companies with NSE symbols in the tracker table.
        Filters by market cap > min_mcap_cr to avoid seeding small/illiquid companies.
        Returns list for sweep.

        Args:
            min_mcap_cr: Minimum market cap in crores (default: 2500)
        """
        logger.info(f"  Seeding tracker from vs_active_companies (mcap > {min_mcap_cr} Cr)...")

        # Get all active companies with NSE symbols
        companies = self.mysql.query(
            "SELECT a.company_id, a.nse_symbol, a.company_name, a.valuation_group "
            "FROM vs_active_companies a "
            "WHERE a.is_active = 1 AND a.nse_symbol IS NOT NULL AND a.nse_symbol != '' "
            "ORDER BY a.priority ASC, a.company_id ASC"
        )
        logger.info(f"  Found {len(companies)} active companies with NSE symbols")

        # Load monthly prices to get latest market cap
        prices_path = os.getenv('MONTHLY_PRICES_PATH')
        if not prices_path or not os.path.exists(prices_path):
            logger.warning(f"  Prices CSV not found at {prices_path}, proceeding without mcap filter")
            filtered_companies = companies
        else:
            try:
                logger.info(f"  Loading prices from {prices_path} to filter by mcap...")
                prices_df = pd.read_csv(prices_path, low_memory=False)

                # Get latest mcap per nse_symbol (prices are sorted by date descending)
                # Group by nse_symbol and take first row (most recent)
                latest_mcaps = {}
                for symbol in prices_df['nse_symbol'].dropna().unique():
                    symbol_df = prices_df[prices_df['nse_symbol'] == symbol]
                    if not symbol_df.empty:
                        latest_row = symbol_df.iloc[0]
                        mcap = latest_row.get('mcap', 0)
                        if pd.notna(mcap):
                            latest_mcaps[symbol] = float(mcap)

                logger.info(f"  Loaded mcap data for {len(latest_mcaps)} symbols")

                # Filter companies by mcap
                filtered_companies = []
                below_threshold = 0
                no_price_data = 0

                for comp in companies:
                    sym = comp['nse_symbol']
                    if sym in latest_mcaps:
                        mcap = latest_mcaps[sym]
                        if mcap >= min_mcap_cr:
                            filtered_companies.append(comp)
                        else:
                            below_threshold += 1
                    else:
                        # No price data - could be newly listed or delisted, skip
                        no_price_data += 1

                logger.info(f"  Filtered to {len(filtered_companies)} companies with mcap >= {min_mcap_cr} Cr")
                logger.info(f"  Excluded: {below_threshold} below threshold, {no_price_data} no price data")

            except Exception as e:
                logger.warning(f"  Failed to filter by mcap: {e}, proceeding with all companies")
                filtered_companies = companies

        companies = filtered_companies

        # Get existing tracker entries
        existing = set()
        rows = self.mysql.query("SELECT nse_symbol FROM vs_nse_fetch_tracker")
        for row in rows:
            existing.add(row['nse_symbol'])

        # Insert missing entries
        new_count = 0
        for comp in companies:
            sym = comp['nse_symbol']
            if sym not in existing:
                try:
                    self.mysql.execute(
                        "INSERT INTO vs_nse_fetch_tracker (company_id, nse_symbol) "
                        "VALUES (%s, %s)",
                        (comp['company_id'], sym)
                    )
                    new_count += 1
                except Exception as e:
                    if 'Duplicate' not in str(e):
                        logger.warning(f"  Failed to seed {sym}: {e}")

        logger.info(f"  Seeded {new_count} new entries (total tracked: {len(existing) + new_count})")

        # Return full list for sweep
        fetch_list = []
        for comp in companies:
            fetch_list.append({
                'nse_symbol': comp['nse_symbol'],
                'company_id': comp['company_id'],
                'company_name': comp.get('company_name', comp['nse_symbol']),
                'reason': 'seed',
            })
        return fetch_list

    # =========================================================================
    # FETCH: Get results from NSE API
    # =========================================================================

    def fetch_results(self, fetch_list: List[Dict]) -> Dict[str, Dict]:
        """
        Fetch quarterly results for each company in fetch_list.
        Returns {symbol: {results_data, quote_data, xbrl_url, ...}}.
        Rate-limited with configurable pause.
        """
        total = len(fetch_list)
        logger.info(f"=== FETCH: {total} companies, batch_size={BATCH_SIZE}, pause={API_PAUSE}s ===")

        all_results = {}
        success = 0
        failed = 0
        no_data = 0

        for i, company in enumerate(fetch_list):
            symbol = company['nse_symbol']
            company_id = company.get('company_id')

            if (i + 1) % 100 == 0:
                elapsed = (datetime.now() - self._start_time).total_seconds()
                rate = (i + 1) / elapsed * 60 if elapsed > 0 else 0
                logger.info(f"  Progress: {i+1}/{total} ({rate:.0f}/min), "
                           f"success={success}, failed={failed}, no_data={no_data}")

            # Fetch results-comparision endpoint
            url = NSE_BASE + COMPANY_ENDPOINTS['results'].format(symbol=symbol)
            data = self.nse.get(url)
            time.sleep(API_PAUSE)

            if data is None:
                failed += 1
                self._log_issue(symbol, company.get('company_name', ''),
                                'ERROR', f"API returned None for {symbol}")
                self._update_tracker(symbol, company_id, status='FAILED')
                continue

            quarters = _extract_results_quarters(data)
            if not quarters:
                no_data += 1
                self._log_issue(symbol, company.get('company_name', ''),
                                'WARNING', f"No quarter records for {symbol}")
                self._update_tracker(symbol, company_id, status='NO_DATA')
                continue

            # Compute data hash to detect changes
            data_hash = hashlib.md5(
                json.dumps(quarters, sort_keys=True, default=str).encode()
            ).hexdigest()

            all_results[symbol] = {
                'quarters': quarters,
                'data_hash': data_hash,
                'company_id': company_id,
                'company_name': company.get('company_name', symbol),
                'xbrl_url': company.get('xbrl_url', ''),
                'reason': company.get('reason', ''),
            }
            success += 1

            logger.debug(f"  [{i+1}/{total}] {symbol}: {len(quarters)} quarters fetched")

        logger.info(f"  FETCH complete: {success} success, {failed} failed, {no_data} no_data")
        return all_results

    # =========================================================================
    # STORE: Update tracker + merge into CSV
    # =========================================================================

    def store_results(self, all_results: Dict[str, Dict]) -> str:
        """
        1. Update vs_nse_fetch_tracker for each company
        2. Build/merge nse_quarterly_data.csv

        Returns path to the CSV file.
        """
        logger.info(f"=== STORE: {len(all_results)} companies ===")

        # Load existing CSV if it exists
        csv_path = NSE_QUARTERLY_CSV_PATH
        existing_df = None
        if os.path.exists(csv_path):
            existing_df = pd.read_csv(csv_path, low_memory=False)
            logger.info(f"  Loaded existing CSV: {len(existing_df)} rows, {len(existing_df.columns)} cols")

        # Build new rows
        new_rows = []
        tracker_updates = 0

        for symbol, result_data in all_results.items():
            quarters = result_data['quarters']
            company_id = result_data.get('company_id')
            company_name = result_data.get('company_name', symbol)
            data_hash = result_data.get('data_hash', '')
            xbrl_url = result_data.get('xbrl_url', '')

            # Parse quarters to find the latest
            latest_q_end = None
            latest_q_idx = None
            latest_result_type = None
            latest_filing_date = None

            row = {
                'company_name': company_name,
                'nse_symbol': symbol,
                'data_source': 'NSE_FILING',
                'fetch_date': datetime.now().strftime('%Y-%m-%d'),
            }

            for q_record in quarters:
                period_end = q_record.get('re_to_dt', '')
                qi = _nse_date_to_quarter_index(period_end)
                if qi is None:
                    continue
                q_idx, q_label = qi

                # Track latest quarter
                try:
                    q_end_dt = datetime.strptime(period_end, '%d-%b-%Y').date()
                    if latest_q_end is None or q_end_dt > latest_q_end:
                        latest_q_end = q_end_dt
                        latest_q_idx = q_idx
                        latest_result_type = q_record.get('re_res_type', '')
                        latest_filing_date = q_record.get('re_create_dt', '')
                except (ValueError, TypeError):
                    pass

                # Extract metrics
                for metric_name, nse_field in METRICS.items():
                    if nse_field is not None:
                        raw_val = _safe_float(q_record.get(nse_field))
                        if raw_val is not None:
                            if metric_name in NO_CONVERT:
                                row[f'{metric_name}_{q_idx}'] = round(raw_val, 2)
                            else:
                                row[f'{metric_name}_{q_idx}'] = round(raw_val / LAKHS_TO_CR, 2)
                    elif metric_name == 'pbidt':
                        pbt = _safe_float(q_record.get('re_pro_loss_bef_tax'))
                        dep = _safe_float(q_record.get('re_depr_und_exp'))
                        intr = _safe_float(q_record.get('re_int_new'))
                        if pbt is not None and dep is not None:
                            row[f'pbidt_{q_idx}'] = round((pbt + dep + (intr or 0)) / LAKHS_TO_CR, 2)
                    elif metric_name == 'totalincome':
                        ns = _safe_float(q_record.get('re_net_sale'))
                        oi = _safe_float(q_record.get('re_oth_inc_new'))
                        if ns is not None:
                            row[f'totalincome_{q_idx}'] = round((ns + (oi or 0)) / LAKHS_TO_CR, 2)

                # Filing metadata per quarter
                row[f'filing_date_{q_idx}'] = q_record.get('re_create_dt', '')
                row[f'result_type_{q_idx}'] = q_record.get('re_res_type', '')

            new_rows.append(row)

            # Update tracker in MySQL
            filing_date_parsed = None
            if latest_filing_date:
                try:
                    filing_date_parsed = datetime.strptime(latest_filing_date, '%d-%b-%Y').date()
                except (ValueError, TypeError):
                    pass

            self._update_tracker(
                symbol, company_id,
                status='SUCCESS',
                latest_quarter_end=latest_q_end,
                latest_quarter_idx=latest_q_idx,
                result_type=latest_result_type,
                filing_date=filing_date_parsed,
                quarters_available=len(quarters),
                data_hash=data_hash,
                xbrl_url=xbrl_url,
            )
            tracker_updates += 1

        logger.info(f"  Updated {tracker_updates} tracker rows in MySQL")

        # Merge with existing CSV
        if new_rows:
            new_df = pd.DataFrame(new_rows)

            if existing_df is not None and not existing_df.empty:
                # Remove existing rows for symbols we're updating
                updating_symbols = set(new_df['nse_symbol'].values)
                kept_df = existing_df[~existing_df['nse_symbol'].isin(updating_symbols)]
                merged_df = pd.concat([kept_df, new_df], ignore_index=True)
                logger.info(f"  Merged: {len(kept_df)} kept + {len(new_df)} new/updated = {len(merged_df)} total")
            else:
                merged_df = new_df
                logger.info(f"  Created new CSV with {len(merged_df)} rows")

            # Sort columns: fixed cols first, then metric columns sorted
            fixed_cols = ['company_name', 'nse_symbol', 'data_source', 'fetch_date']
            metric_cols = sorted(
                [c for c in merged_df.columns if c not in fixed_cols],
                key=lambda x: (x.rsplit('_', 1)[0],
                               int(x.rsplit('_', 1)[1]) if len(x.rsplit('_', 1)) == 2 and x.rsplit('_', 1)[1].isdigit() else 0)
            )
            merged_df = merged_df[[c for c in fixed_cols if c in merged_df.columns] + metric_cols]

            # Write CSV
            os.makedirs(os.path.dirname(csv_path), exist_ok=True)
            merged_df.to_csv(csv_path, index=False)
            logger.info(f"  CSV saved: {csv_path} ({len(merged_df)} companies, {len(merged_df.columns)} columns)")

            # Log quarters covered
            q_indices = set()
            for col in metric_cols:
                parts = col.rsplit('_', 1)
                if len(parts) == 2 and parts[1].isdigit():
                    q_indices.add(int(parts[1]))
            if q_indices:
                q_labels = []
                for qi in sorted(q_indices):
                    fy = 1989 + (qi - 1) // 4
                    q = (qi - 1) % 4 + 1
                    q_labels.append(f"FY{fy}Q{q}({qi})")
                logger.info(f"  Quarters covered: {', '.join(q_labels)}")

            return csv_path
        else:
            logger.info("  No new data to store")
            return csv_path

    def _update_tracker(self, symbol: str, company_id: Optional[int],
                        status: str = 'SUCCESS', **kwargs):
        """Update or insert a tracker row in vs_nse_fetch_tracker."""
        try:
            existing = self.mysql.query_one(
                "SELECT id FROM vs_nse_fetch_tracker WHERE nse_symbol = %s",
                (symbol,)
            )

            if existing:
                # Update
                set_parts = ["last_fetch_date = NOW()", f"last_fetch_status = '{status}'"]
                params = []
                for key, val in kwargs.items():
                    if val is not None:
                        set_parts.append(f"{key} = %s")
                        params.append(val)
                params.append(symbol)
                self.mysql.execute(
                    f"UPDATE vs_nse_fetch_tracker SET {', '.join(set_parts)} "
                    f"WHERE nse_symbol = %s",
                    tuple(params)
                )
            else:
                # Insert
                insert_data = {
                    'nse_symbol': symbol,
                    'company_id': company_id or 0,
                    'last_fetch_date': datetime.now(),
                    'last_fetch_status': status,
                }
                for key, val in kwargs.items():
                    if val is not None:
                        insert_data[key] = val
                self.mysql.insert('vs_nse_fetch_tracker', insert_data)

        except Exception as e:
            logger.error(f"  Failed to update tracker for {symbol}: {e}\n{traceback.format_exc()}")

    # =========================================================================
    # VALIDATE: Compare against core CSV
    # =========================================================================

    def validate_against_core(self, all_results: Dict[str, Dict],
                              sample_size: int = 50) -> Dict:
        """
        Compare NSE data against core CSV for a random sample.
        Returns validation summary.
        """
        logger.info(f"=== VALIDATE: Comparing {sample_size} random companies against core CSV ===")

        try:
            from valuation_system.data.loaders.core_loader import CoreDataLoader
            core_loader = CoreDataLoader()
            core_df = core_loader.df
        except Exception as e:
            logger.error(f"  Cannot load core CSV: {e}")
            return {'error': str(e)}

        sym_col = 'CD_NSE Symbol1'
        if sym_col not in core_df.columns:
            logger.error(f"  Core CSV missing '{sym_col}' column")
            return {'error': f'missing {sym_col}'}

        # Sample companies
        import random
        symbols = list(all_results.keys())
        sample = random.sample(symbols, min(sample_size, len(symbols)))

        matches = 0
        close = 0
        mismatches = 0
        no_core = 0
        details = []

        for symbol in sample:
            result_data = all_results[symbol]
            quarters = result_data['quarters']

            # Find in core CSV
            core_matches = core_df[core_df[sym_col].astype(str).str.strip().str.upper() == symbol.upper()]
            if len(core_matches) == 0:
                no_core += 1
                continue

            company_row = core_matches.iloc[0]

            # Compare latest quarter
            for q_record in quarters[:1]:  # just latest
                period_end = q_record.get('re_to_dt', '')
                qi = _nse_date_to_quarter_index(period_end)
                if qi is None:
                    continue
                q_idx, q_label = qi

                nse_sales_lakhs = _safe_float(q_record.get('re_net_sale'))
                nse_sales = nse_sales_lakhs / LAKHS_TO_CR if nse_sales_lakhs else None
                core_sales = _safe_float(company_row.get(f'sales_{q_idx}'))

                if nse_sales is not None and core_sales is not None and core_sales != 0:
                    diff_pct = abs(nse_sales - core_sales) / abs(core_sales) * 100
                    if diff_pct < 1.0:
                        matches += 1
                    elif diff_pct < 5.0:
                        close += 1
                    else:
                        mismatches += 1
                        details.append({
                            'symbol': symbol,
                            'quarter': q_label,
                            'nse_sales': nse_sales,
                            'core_sales': core_sales,
                            'diff_pct': round(diff_pct, 2),
                        })
                else:
                    no_core += 1

        summary = {
            'sample_size': len(sample),
            'exact_match_lt1pct': matches,
            'close_lt5pct': close,
            'mismatch_gt5pct': mismatches,
            'no_core_data': no_core,
            'mismatches': details[:10],  # Top 10 mismatches
        }

        logger.info(f"  Validation: {matches} match (<1%), {close} close (<5%), "
                     f"{mismatches} mismatch (>5%), {no_core} no core data")
        if details:
            for d in details[:5]:
                logger.warning(f"    MISMATCH: {d['symbol']} {d['quarter']}: "
                              f"NSE={d['nse_sales']:.0f} vs Core={d['core_sales']:.0f} ({d['diff_pct']:.1f}%)")

        return summary

    # =========================================================================
    # ISSUES LOG
    # =========================================================================

    def _log_issue(self, symbol: str, company_name: str, level: str, message: str):
        """Log a batch issue for the issues CSV."""
        self.issues.append({
            'timestamp': datetime.now().isoformat(),
            'symbol': symbol,
            'company_name': company_name,
            'level': level,
            'logger': 'nse_loader',
            'message': message,
            'traceback': '',
        })

    def write_issues_csv(self) -> str:
        """Write batch issues to CSV. Returns path."""
        if not self.issues:
            logger.info("  No issues to report")
            return ''

        issues_path = os.path.join(
            LOG_DIR,
            f"nse_issues_{datetime.now().strftime('%Y%m%d_%H%M')}.csv"
        )
        with open(issues_path, 'w', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=[
                'timestamp', 'symbol', 'company_name', 'level', 'logger', 'message', 'traceback'
            ])
            writer.writeheader()
            writer.writerows(self.issues)

        logger.info(f"  Issues CSV: {issues_path} ({len(self.issues)} issues)")
        return issues_path

    # =========================================================================
    # MAIN ORCHESTRATION
    # =========================================================================

    def run(self, mode: str = 'daily', symbols: List[str] = None, min_mcap_cr: float = 2500.0) -> Dict:
        """
        Run the full discover → fetch → store → validate cycle.

        Args:
            mode: 'daily' | 'sweep' | 'seed' | 'symbol'
            symbols: list of symbols for 'symbol' mode
            min_mcap_cr: minimum market cap in crores for seed mode (default: 2500)

        Returns: summary dict
        """
        logger.info("=" * 80)
        logger.info(f"NSE LOADER — mode={mode}, started at {self._start_time.isoformat()}")
        logger.info("=" * 80)

        summary = {
            'mode': mode,
            'started_at': self._start_time.isoformat(),
            'api_calls': 0,
            'companies_fetched': 0,
            'companies_failed': 0,
            'csv_path': '',
            'issues_path': '',
        }

        try:
            # Step 1: Discover
            if mode in ('daily',):
                discovery = self.discover_new_filings()
                summary['api_calls'] += discovery.get('api_calls', 0)
            else:
                discovery = {}

            # Step 2: Decide what to fetch
            fetch_list = self.decide_fetch_list(discovery, mode=mode, symbols=symbols, min_mcap_cr=min_mcap_cr)
            summary['companies_to_fetch'] = len(fetch_list)

            if not fetch_list:
                logger.info("  No companies to fetch — nothing to do")
                summary['status'] = 'NO_WORK'
                return summary

            # Step 3: Fetch
            all_results = self.fetch_results(fetch_list)
            summary['companies_fetched'] = len(all_results)
            summary['companies_failed'] = len(fetch_list) - len(all_results)
            summary['api_calls'] += len(fetch_list)

            # Step 4: Store
            csv_path = self.store_results(all_results)
            summary['csv_path'] = csv_path

            # Step 5: Validate (sample)
            if all_results and mode in ('sweep', 'seed'):
                validation = self.validate_against_core(all_results, sample_size=50)
                summary['validation'] = validation

            # Step 6: Issues CSV
            issues_path = self.write_issues_csv()
            summary['issues_path'] = issues_path
            summary['issues_count'] = len(self.issues)

            elapsed = (datetime.now() - self._start_time).total_seconds()
            summary['elapsed_seconds'] = round(elapsed, 1)
            summary['status'] = 'SUCCESS'

            logger.info("=" * 80)
            logger.info(f"NSE LOADER — COMPLETE in {elapsed:.0f}s")
            logger.info(f"  Fetched: {summary['companies_fetched']}/{summary.get('companies_to_fetch', 0)}")
            logger.info(f"  Failed: {summary['companies_failed']}")
            logger.info(f"  API calls: {summary['api_calls']}")
            logger.info(f"  CSV: {csv_path}")
            if issues_path:
                logger.info(f"  Issues: {issues_path} ({len(self.issues)} issues)")
            logger.info("=" * 80)

        except Exception as e:
            logger.error(f"NSE Loader failed: {e}\n{traceback.format_exc()}")
            summary['status'] = 'FAILED'
            summary['error'] = str(e)
            self._log_issue('SYSTEM', '', 'ERROR', f"Loader failed: {e}\n{traceback.format_exc()}")
            self.write_issues_csv()

        return summary


def main():
    parser = argparse.ArgumentParser(description='NSE Filing Data Loader')
    parser.add_argument('--mode', choices=['daily', 'sweep', 'seed', 'symbol'],
                        default='daily', help='Fetch mode')
    parser.add_argument('--symbol', type=str, help='NSE symbol (for --mode symbol)')
    parser.add_argument('--symbols', type=str, help='Comma-separated symbols (for --mode symbol)')
    args = parser.parse_args()

    symbols = None
    if args.mode == 'symbol':
        if args.symbol:
            symbols = [args.symbol]
        elif args.symbols:
            symbols = [s.strip() for s in args.symbols.split(',')]
        else:
            print("ERROR: --symbol or --symbols required with --mode symbol")
            sys.exit(1)

    loader = NSELoader()
    result = loader.run(mode=args.mode, symbols=symbols)

    print(f"\n{'='*60}")
    print(f"  NSE Loader Summary")
    print(f"{'='*60}")
    print(f"  Status:    {result.get('status', 'UNKNOWN')}")
    print(f"  Mode:      {result.get('mode')}")
    print(f"  Fetched:   {result.get('companies_fetched', 0)}/{result.get('companies_to_fetch', 0)}")
    print(f"  Failed:    {result.get('companies_failed', 0)}")
    print(f"  API calls: {result.get('api_calls', 0)}")
    print(f"  Elapsed:   {result.get('elapsed_seconds', 0)}s")
    print(f"  CSV:       {result.get('csv_path', '')}")
    if result.get('issues_path'):
        print(f"  Issues:    {result.get('issues_path')} ({result.get('issues_count', 0)})")
    if result.get('validation'):
        v = result['validation']
        print(f"  Validation: {v.get('exact_match_lt1pct', 0)} match, "
              f"{v.get('close_lt5pct', 0)} close, {v.get('mismatch_gt5pct', 0)} mismatch")
    print(f"{'='*60}")


if __name__ == '__main__':
    main()
