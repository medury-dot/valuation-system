#!/usr/bin/env python3
"""
Batch Valuation Runner
Runs valuations for multiple companies with options for detailed Excel reports.

Usage:
  # Option A: Database + Sheets only (fast)
  python3 utils/batch_valuation.py --source gsheet --mode quick

  # Option B: Database + Sheets + Excel reports (full audit trail)
  python3 utils/batch_valuation.py --source gsheet --mode full

  # Run specific companies
  python3 utils/batch_valuation.py --symbols BEL,KEI,ACUTAAS --mode full
"""

import sys
import os
import argparse
import csv
import json
import logging
import time
import traceback
import pandas as pd
from pathlib import Path
from datetime import datetime

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))

import gspread
from google.oauth2.service_account import Credentials
from dotenv import load_dotenv

from valuation_system.data.loaders.core_loader import CoreDataLoader
from valuation_system.data.loaders.price_loader import PriceLoader
from valuation_system.data.loaders.damodaran_loader import DamodaranLoader
from valuation_system.data.processors.financial_processor import FinancialProcessor
from valuation_system.models.dcf_model import FCFFValuation, DCFInputs
from valuation_system.models.relative_valuation import RelativeValuation
from valuation_system.storage.mysql_client import ValuationMySQLClient
from valuation_system.utils.config_loader import get_active_companies

# Optional: Import Excel generation if in full mode
try:
    from valuation_system.agents.valuator import ValuatorAgent
    from valuation_system.agents.group_analyst import GroupAnalystAgent
    from valuation_system.utils.excel_report import generate_valuation_excel
    EXCEL_AVAILABLE = True
except ImportError:
    EXCEL_AVAILABLE = False

load_dotenv(Path(__file__).parent.parent / 'config' / '.env')

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        logging.FileHandler(Path(__file__).parent.parent / 'logs' / 'batch_valuation.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Suppress noisy loggers
for name in ['yfinance', 'urllib3', 'httpx']:
    logging.getLogger(name).setLevel(logging.WARNING)


class _BatchLogHandler(logging.Handler):
    """Captures WARNING+ log records and appends them to the batch's issue list."""

    def __init__(self, batch_valuator):
        super().__init__(level=logging.WARNING)
        self.batch = batch_valuator

    def emit(self, record):
        try:
            msg = record.getMessage()
            tb = self.format_traceback(record)
            # If traceback was embedded in the message (our pattern: "error\nTraceback..."), split it
            if not tb and '\nTraceback' in msg:
                parts = msg.split('\nTraceback', 1)
                msg = parts[0]
                tb = 'Traceback' + parts[1]

            self.batch._issue_rows.append({
                'timestamp': datetime.fromtimestamp(record.created).strftime('%Y-%m-%d %H:%M:%S'),
                'symbol': self.batch._current_symbol or '',
                'company_name': self.batch._current_csv_name or '',
                'valuation_group': self.batch._current_group or '',
                'valuation_subgroup': self.batch._current_subgroup or '',
                'level': record.levelname,
                'logger': record.name,
                'message': msg.strip(),
                'traceback': tb.strip(),
            })
        except Exception:
            pass  # Never let logging errors break the batch

    @staticmethod
    def format_traceback(record):
        if record.exc_info and record.exc_info[1]:
            return ''.join(traceback.format_exception(*record.exc_info)).strip()
        return ''


class BatchValuator:
    def __init__(self, mode='quick'):
        self.mode = mode  # 'quick' or 'full'
        self.mysql = ValuationMySQLClient.get_instance()
        self.core_loader = CoreDataLoader()
        self.price_loader = PriceLoader()
        self.damodaran_loader = DamodaranLoader()

        # Track results
        self.already_done = set()  # symbols already valued today (for --resume)
        self.results = {
            'success': [],
            'failed': [],
            'skipped': []
        }
        # Error/warning CSV log
        self._current_symbol = None
        self._current_csv_name = None
        self._current_group = None
        self._current_subgroup = None
        self._issue_rows = []  # list of dicts for CSV output
        self._log_handler = _BatchLogHandler(self)
        # Attach to valuation_system root logger to capture all WARNING+ from sub-modules
        vs_logger = logging.getLogger('valuation_system')
        vs_logger.addHandler(self._log_handler)
        # Also capture from __main__ (this module's logger)
        logger.addHandler(self._log_handler)

    def write_issues_csv(self):
        """Write all captured warnings/errors to a CSV file."""
        if not self._issue_rows:
            logger.info("No warnings or errors to write to CSV")
            return None

        log_dir = Path(__file__).parent.parent / 'logs'
        log_dir.mkdir(exist_ok=True)
        csv_path = log_dir / f'batch_issues_{datetime.now().strftime("%Y%m%d_%H%M")}.csv'

        fieldnames = ['timestamp', 'symbol', 'company_name', 'valuation_group',
                      'valuation_subgroup', 'level', 'logger', 'message', 'traceback']

        with open(csv_path, 'w', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(self._issue_rows)

        # Summary counts
        warn_count = sum(1 for r in self._issue_rows if r['level'] == 'WARNING')
        err_count = sum(1 for r in self._issue_rows if r['level'] == 'ERROR')
        crit_count = sum(1 for r in self._issue_rows if r['level'] == 'CRITICAL')
        symbols_with_issues = len(set(r['symbol'] for r in self._issue_rows if r['symbol']))

        logger.info(f"Issues CSV: {csv_path}")
        logger.info(f"  {len(self._issue_rows)} total issues across {symbols_with_issues} companies "
                     f"({warn_count} warnings, {err_count} errors, {crit_count} critical)")
        return csv_path

    def load_already_valued_today(self):
        """Load symbols that already have a valuation for today (for --resume)."""
        rows = self.mysql.query('''
            SELECT DISTINCT m.symbol
            FROM vs_valuations v
            JOIN mssdb.kbapp_marketscrip m ON v.company_id = m.marketscrip_id
            WHERE v.valuation_date = CURDATE()
        ''')
        self.already_done = {r['symbol'] for r in rows if r.get('symbol')}
        logger.info(f"Resume mode: {len(self.already_done)} companies already valued today, will skip them")

    def get_companies_from_gsheet(self):
        """Load companies from Google Sheets 'Active Companies' tab."""
        auth_path = os.getenv('GSHEET_AUTH_PATH')
        spreadsheet_id = os.getenv('GSHEET_DRIVERS_ID')

        scopes = [
            'https://www.googleapis.com/auth/spreadsheets',
            'https://www.googleapis.com/auth/drive'
        ]

        creds = Credentials.from_service_account_file(auth_path, scopes=scopes)
        gc = gspread.authorize(creds)

        sheet = gc.open_by_key(spreadsheet_id)
        ws = sheet.worksheet('6. Active Companies')

        # Get all rows (skip header)
        data = ws.get_all_records()

        # Build symbol-to-csv_name mapping from database
        symbol_mapping = {}
        db_companies = self.mysql.query('''
            SELECT nse_symbol, csv_name, valuation_group, valuation_subgroup
            FROM vs_active_companies
            WHERE is_active = 1
        ''')
        for comp in db_companies:
            symbol_mapping[comp['nse_symbol']] = {
                'csv_name': comp['csv_name'],
                'valuation_group': comp['valuation_group'],
                'valuation_subgroup': comp.get('valuation_subgroup', '')
            }

        # Filter active companies and map to correct csv_name
        companies = []
        for row in data:
            if row.get('is_active') in ['TRUE', 'True', True, 1, '1']:
                symbol = row['nse_symbol']
                # Look up csv_name from database, fallback to company_name
                mapping = symbol_mapping.get(symbol, {})
                companies.append({
                    'symbol': symbol,
                    'name': row['company_name'],
                    'valuation_group': mapping.get('valuation_group', row.get('valuation_group', '')),
                    'valuation_subgroup': mapping.get('valuation_subgroup', row.get('valuation_subgroup', '')),
                    'csv_name': mapping.get('csv_name', row['company_name'])
                })

        return companies

    def get_companies_from_database(self):
        """Load companies from MySQL database."""
        companies_dict = get_active_companies(mysql_client=self.mysql)

        companies = []
        for symbol, company in companies_dict.items():
            companies.append({
                'symbol': symbol,
                'name': company.get('company_name', company.get('csv_name')),
                'valuation_group': company.get('valuation_group', ''),
                'valuation_subgroup': company.get('valuation_subgroup', ''),
                'csv_name': company.get('csv_name')
            })

        return companies

    def run_quick_valuation(self, company):
        """Run quick DCF valuation (MySQL + Sheets only)."""
        symbol = company['symbol']
        csv_name = company['csv_name']
        valuation_group = company.get('valuation_group', '')
        valuation_subgroup = company.get('valuation_subgroup', '')

        # Set context for CSV error logging
        self._current_symbol = symbol
        self._current_csv_name = csv_name
        self._current_group = valuation_group
        self._current_subgroup = valuation_subgroup

        logger.info(f"\n{'='*80}")
        logger.info(f"Valuing {symbol}: {csv_name}")
        logger.info(f"Group: {valuation_group}, Subgroup: {valuation_subgroup}")
        logger.info(f"{'='*80}")

        try:
            # Build DCF inputs
            processor = FinancialProcessor(
                self.core_loader,
                self.price_loader,
                self.damodaran_loader
            )
            dcf_dict = processor.build_dcf_inputs(
                csv_name, valuation_group,
                valuation_subgroup=valuation_subgroup
            )

            if not dcf_dict:
                raise ValueError(f"Failed to build DCF inputs for {csv_name}")

            # Load GROUP + SUBGROUP outlook and apply driver adjustments
            company_adjustment = None
            if EXCEL_AVAILABLE and valuation_group:
                try:
                    group_analyst = GroupAnalystAgent(
                        valuation_group=valuation_group,
                        valuation_subgroup=valuation_subgroup,
                        mysql_client=self.mysql
                    )
                    sector_outlook = group_analyst.calculate_outlook()

                    # Apply group/subgroup adjustments (growth + margin)
                    growth_adj = sector_outlook.get('growth_adjustment', 0)
                    margin_adj = sector_outlook.get('margin_adjustment', 0)
                    if growth_adj != 0:
                        dcf_dict['revenue_growth_rates'] = [
                            round(max(0.02, r * (1 + growth_adj)), 4)
                            for r in dcf_dict.get('revenue_growth_rates', [])
                        ]
                    if margin_adj != 0:
                        dcf_dict['margin_improvement'] = dcf_dict.get('margin_improvement', 0) + margin_adj

                    # Load company-level adjustment
                    company_id_row = self.mysql.query_one(
                        "SELECT marketscrip_id FROM mssdb.kbapp_marketscrip WHERE symbol = %s",
                        (symbol,)
                    )
                    if company_id_row:
                        company_adjustment = group_analyst.calculate_company_adjustment(
                            company_id_row['marketscrip_id'])

                        # Apply company auto-driver adjustments (growth + margin)
                        c_growth = company_adjustment.get('growth_adj', 0)
                        c_margin = company_adjustment.get('margin_adj', 0)
                        if c_growth != 0:
                            dcf_dict['revenue_growth_rates'] = [
                                round(max(0.02, r * (1 + c_growth)), 4)
                                for r in dcf_dict.get('revenue_growth_rates', [])
                            ]
                        if c_margin != 0:
                            dcf_dict['margin_improvement'] = dcf_dict.get('margin_improvement', 0) + c_margin

                        # Apply company terminal adjustments (from PM qualitative drivers)
                        roce_adj = company_adjustment.get('terminal_roce_adj', 0)
                        reinv_adj = company_adjustment.get('terminal_reinv_adj', 0)
                        if roce_adj != 0:
                            dcf_dict['terminal_roce'] = dcf_dict.get('terminal_roce', 0.15) + roce_adj
                        if reinv_adj != 0:
                            dcf_dict['terminal_reinvestment_rate'] = (
                                dcf_dict.get('terminal_reinvestment_rate', 0.30) + reinv_adj)

                    logger.info(f"  Driver adjustments: group={growth_adj:+.2%}/{margin_adj:+.2%}, "
                                f"company={c_growth:+.2%}/{c_margin:+.2%}" if company_adjustment else
                                f"  Driver adjustments: group={growth_adj:+.2%}/{margin_adj:+.2%}")
                except Exception as e:
                    logger.warning(f"Driver adjustment failed (proceeding without): {e}")

            # Convert to DCFInputs object
            dcf_inputs = DCFInputs(**{
                k: v for k, v in dcf_dict.items()
                if k in DCFInputs.__dataclass_fields__
            })

            # Run DCF
            dcf_model = FCFFValuation()
            dcf_result = dcf_model.calculate_intrinsic_value(dcf_inputs)

            # Look up company in master (needed for price fallback + DB save)
            company_id = self.mysql.query_one(
                "SELECT marketscrip_id, scrip_code FROM mssdb.kbapp_marketscrip WHERE symbol = %s",
                (symbol,)
            )
            bse_code = company_id.get('scrip_code') if company_id else None

            # Get current price (try NSE symbol -> BSE code -> company name -> Yahoo)
            price_data = self.price_loader.get_latest_data(symbol, bse_code=bse_code, company_name=csv_name)
            cmp = (price_data.get('cmp') or 0) if price_data else 0
            cmp = float(cmp) if cmp else 0

            intrinsic = float(dcf_result.get('intrinsic_per_share') or 0)
            upside = ((intrinsic / cmp) - 1) * 100 if cmp > 0 else 0
            # Cap upside to ±9999% to avoid MySQL DECIMAL overflow
            upside = max(-9999.0, min(9999.0, upside))

            logger.info(f"  Intrinsic: ₹{intrinsic:,.2f}")
            logger.info(f"  CMP: ₹{cmp:,.2f}" if cmp > 0 else "  CMP: N/A (no price data)")
            logger.info(f"  Upside: {upside:+.1f}%")

            if company_id:
                assumptions = dcf_result.get('assumptions', {})
                key_assumptions = {
                    # WACC components
                    'wacc': dcf_result.get('wacc', 0),
                    'cost_of_equity': dcf_result.get('cost_of_equity', 0),
                    'cost_of_debt_at': dcf_result.get('cost_of_debt_at', 0),
                    'beta': assumptions.get('beta', 0),
                    'risk_free_rate': assumptions.get('risk_free_rate', 0),
                    'erp': assumptions.get('erp', 0),
                    'debt_ratio': assumptions.get('debt_ratio', 0),
                    # Growth & margins
                    'revenue_growth': assumptions.get('growth_rates', []),
                    'ebitda_margin': assumptions.get('ebitda_margin', 0),
                    # Operating ratios
                    'capex_to_sales': assumptions.get('capex_to_sales', 0),
                    'tax_rate': assumptions.get('tax_rate', 0),
                    # Terminal assumptions
                    'terminal_growth': dcf_result.get('terminal_growth', 0),
                    'terminal_roce': assumptions.get('terminal_roce', 0),
                    'terminal_reinvestment': assumptions.get('terminal_reinvestment', 0),
                    # Value breakdown
                    'terminal_value_pct': dcf_result.get('terminal_value_pct', 0),
                    'firm_value': dcf_result.get('firm_value', 0),
                    'equity_value': dcf_result.get('equity_value', 0),
                    'net_debt': assumptions.get('net_debt', 0),
                    'shares_outstanding': assumptions.get('shares_outstanding', 0),
                }

                valuation_data = {
                    'company_id': company_id['marketscrip_id'],
                    'valuation_date': datetime.now().date(),
                    'method': 'DCF',
                    'scenario': 'BASE',
                    'intrinsic_value': intrinsic,
                    'cmp': cmp if cmp > 0 else None,
                    'upside_pct': upside if cmp > 0 else None,
                    'key_assumptions': key_assumptions,
                    'created_by': 'AGENT'
                }

                self.mysql.insert('vs_valuations', valuation_data)
                logger.info(f"✓ Saved to database")

            self.results['success'].append({
                'symbol': symbol,
                'intrinsic': intrinsic,
                'cmp': cmp,
                'upside': upside
            })

            return True

        except Exception as e:
            tb = traceback.format_exc()
            logger.error(f"✗ Failed: {e}\n{tb}")
            self.results['failed'].append({
                'symbol': symbol,
                'error': str(e),
                'traceback': tb
            })
            return False

    def run_full_valuation(self, company):
        """Run full valuation with Excel report generation."""
        if not EXCEL_AVAILABLE:
            logger.warning("Excel generation not available, falling back to quick mode")
            return self.run_quick_valuation(company)

        symbol = company['symbol']

        logger.info(f"\n{'='*80}")
        logger.info(f"Full Valuation: {symbol}")
        logger.info(f"{'='*80}")

        try:
            # First run quick valuation to save to DB
            success = self.run_quick_valuation(company)
            if not success:
                return False

            # Then generate detailed Excel report
            logger.info(f"Generating Excel report for {symbol}...")

            # Get company config from database
            companies = get_active_companies(mysql_client=self.mysql)
            company_cfg = companies.get(symbol)

            if not company_cfg:
                logger.warning(f"No config found for {symbol}, skipping Excel")
                return True

            valuation_group = company_cfg.get('valuation_group', 'INDUSTRIALS')
            valuation_subgroup = company_cfg.get('valuation_subgroup', '')

            # Create group analyst (4-level hierarchy)
            group_analyst = GroupAnalystAgent(
                valuation_group=valuation_group,
                valuation_subgroup=valuation_subgroup,
                mysql_client=self.mysql
            )
            sector_outlook = group_analyst.calculate_outlook()

            # Load company-level adjustment
            company_adjustment = None
            company_id_row = self.mysql.query_one(
                "SELECT marketscrip_id FROM mssdb.kbapp_marketscrip WHERE symbol = %s",
                (symbol,)
            )
            if company_id_row:
                company_adjustment = group_analyst.calculate_company_adjustment(
                    company_id_row['marketscrip_id'])

            # Create valuator
            valuator = ValuatorAgent(
                self.core_loader,
                self.price_loader,
                self.damodaran_loader,
                self.mysql
            )

            # Run full valuation
            result = valuator.run_full_valuation(
                company_config=company_cfg,
                sector_outlook=sector_outlook,
                company_adjustment=company_adjustment,
            )

            if 'error' not in result:
                # Generate Excel
                excel_path = generate_valuation_excel(result)
                logger.info(f"✓ Excel report: {excel_path}")

            return True

        except Exception as e:
            import traceback
            logger.error(f"✗ Excel generation failed: {e}\n{traceback.format_exc()}")
            # Don't mark as failed if quick valuation succeeded
            return True

    @staticmethod
    def _fmt_pct(val):
        """Safely format a value as percentage string. Returns '' if missing/0/None."""
        if val is None or val == 0 or val == '':
            return ''
        try:
            return f"{float(val):.1%}"
        except (TypeError, ValueError):
            return ''

    @staticmethod
    def _fmt_pct2(val):
        """Format percentage with 2 decimal places (for beta-like precision)."""
        if val is None or val == 0 or val == '':
            return ''
        try:
            return f"{float(val):.2f}"
        except (TypeError, ValueError):
            return ''

    @staticmethod
    def _fmt_cr(val):
        """Format value in Crores (whole number)."""
        if val is None or val == 0 or val == '':
            return ''
        try:
            return f"{float(val):,.0f}"
        except (TypeError, ValueError):
            return ''

    def _get_sector_industry(self, symbol, sector_lookup):
        """Return (sector, industry) from core CSV (CD_Sector, CD_Industry1)."""
        if symbol and symbol in sector_lookup:
            return sector_lookup[symbol]
        return ('', '')

    def _get_pe_pb_bv_mcap(self, symbol, cmp, price_lookup):
        """Return (P/E, P/B, Book Value, MCap Cr) formatted strings for GSheet row."""
        pe_str, pb_str, bv_str, mcap_str = '', '', '', ''
        if symbol and symbol in price_lookup:
            pe_val, pb_val, mcap_val = price_lookup[symbol]
            if pe_val is not None and pe_val > 0:
                pe_str = f"{pe_val:.1f}"
            if pb_val is not None and pb_val > 0:
                pb_str = f"{pb_val:.2f}"
                # Book value per share = CMP / P/B
                try:
                    cmp_f = float(cmp) if cmp else 0
                    if cmp_f > 0:
                        bv_str = f"{cmp_f / pb_val:.2f}"
                except (TypeError, ValueError):
                    pass
            if mcap_val is not None and mcap_val > 0:
                mcap_str = f"{mcap_val:,.0f}"
        return (pe_str, pb_str, bv_str, mcap_str)

    def update_gsheet_results(self, only_current_run=True):
        """Update Google Sheets with batch results including enriched DCF assumptions.

        Args:
            only_current_run: If True (default), only write THIS run's successful valuations.
                              If False (--gsheet-all), write latest 100 from DB across all runs.
        """
        # Skip if current-run mode and nothing succeeded
        if only_current_run and not self.results['success']:
            logger.info("No successful valuations in this run — skipping GSheet update")
            return

        try:
            auth_path = os.getenv('GSHEET_AUTH_PATH')
            spreadsheet_id = os.getenv('GSHEET_DRIVERS_ID')

            scopes = [
                'https://www.googleapis.com/auth/spreadsheets',
                'https://www.googleapis.com/auth/drive'
            ]

            creds = Credentials.from_service_account_file(auth_path, scopes=scopes)
            gc = gspread.authorize(creds)

            sheet = gc.open_by_key(spreadsheet_id)
            ws = sheet.worksheet('5. Recent Activity')

            if only_current_run:
                # Only fetch this run's successful symbols from today's valuations
                success_symbols = [r['symbol'] for r in self.results['success'] if r.get('symbol')]
                if not success_symbols:
                    logger.info("No successful symbols to write to GSheet")
                    return
                placeholders = ','.join(['%s'] * len(success_symbols))
                valuations = self.mysql.query(f'''
                    SELECT
                        v.id,
                        m.symbol as nse_symbol,
                        m.name as company_name,
                        v.valuation_date,
                        v.method,
                        v.scenario,
                        v.intrinsic_value,
                        v.cmp,
                        v.upside_pct,
                        v.key_assumptions,
                        v.created_at,
                        v.created_by,
                        a.valuation_group, a.valuation_subgroup
                    FROM vs_valuations v
                    JOIN mssdb.kbapp_marketscrip m ON v.company_id = m.marketscrip_id
                    LEFT JOIN vs_active_companies a ON v.company_id = a.company_id
                    WHERE v.valuation_date = CURDATE()
                      AND m.symbol IN ({placeholders})
                    ORDER BY m.symbol
                ''', tuple(success_symbols))
                logger.info(f"GSheet: writing {len(valuations)} valuations from this run")
            else:
                # Override: write ALL valuations from DB across all runs
                valuations = self.mysql.query('''
                    SELECT
                        v.id,
                        m.symbol as nse_symbol,
                        m.name as company_name,
                        v.valuation_date,
                        v.method,
                        v.scenario,
                        v.intrinsic_value,
                        v.cmp,
                        v.upside_pct,
                        v.key_assumptions,
                        v.created_at,
                        v.created_by,
                        a.valuation_group, a.valuation_subgroup
                    FROM vs_valuations v
                    JOIN mssdb.kbapp_marketscrip m ON v.company_id = m.marketscrip_id
                    LEFT JOIN vs_active_companies a ON v.company_id = a.company_id
                    ORDER BY v.created_at DESC
                ''')
                logger.info(f"GSheet: writing {len(valuations)} valuations from DB (--gsheet-all)")

            # 33 columns: A through AG
            headers = [
                'ID', 'Symbol', 'Company', 'Sector', 'Industry', 'Val Group', 'Val Subgroup',
                'Val Date', 'Method', 'Scenario',
                'Intrinsic', 'CMP', 'Upside %',
                'WACC', 'Beta', 'Ke', 'Terminal g', 'Terminal ROCE', 'Terminal Reinvest', 'TV%',
                'EBITDA Margin', 'Capex/Sales', 'Tax Rate',
                'Firm Value Cr', 'Equity Value Cr', 'Net Debt Cr', 'Shares Cr',
                'P/E', 'P/B', 'Book Value', 'MCap Cr',
                'Created At', 'Created By'
            ]

            # Build latest P/E, P/B, MCap lookup from monthly prices (one-time, fast)
            price_df = self.price_loader.df
            price_latest = price_df.sort_values('daily_date', ascending=False).drop_duplicates(subset='nse_symbol', keep='first')
            price_lookup = {}
            for _, prow in price_latest.iterrows():
                sym = prow.get('nse_symbol')
                if sym and str(sym) != 'nan':
                    pe_val = prow.get('pe')
                    pb_val = prow.get('pb')
                    mcap_val = prow.get('mcap')
                    pe_f = float(pe_val) if pd.notna(pe_val) and pe_val != 0 else None
                    pb_f = float(pb_val) if pd.notna(pb_val) and pb_val != 0 else None
                    mcap_f = float(mcap_val) if pd.notna(mcap_val) and mcap_val != 0 else None
                    price_lookup[str(sym)] = (pe_f, pb_f, mcap_f)

            # Build sector/industry lookup from core CSV (CD_Sector, CD_Industry1)
            core_df = self.core_loader.df
            sector_lookup = {}
            for _, crow in core_df[['CD_NSE Symbol1', 'CD_Sector', 'CD_Industry1']].dropna(subset=['CD_NSE Symbol1']).iterrows():
                sym = str(crow['CD_NSE Symbol1']).strip()
                sec = str(crow['CD_Sector']) if pd.notna(crow['CD_Sector']) else ''
                ind = str(crow['CD_Industry1']) if pd.notna(crow['CD_Industry1']) else ''
                sector_lookup[sym] = (sec, ind)

            rows = [headers]
            for val in valuations:
                # Parse key_assumptions JSON (handle None for old rows)
                ka = val.get('key_assumptions')
                if ka is None:
                    ka = {}
                elif isinstance(ka, str):
                    try:
                        ka = json.loads(ka)
                    except (json.JSONDecodeError, TypeError):
                        ka = {}

                rows.append([
                    str(val['id']),
                    val['nse_symbol'],
                    val['company_name'],
                    *self._get_sector_industry(val['nse_symbol'], sector_lookup),
                    val.get('valuation_group') or '',
                    val.get('valuation_subgroup') or '',
                    str(val['valuation_date']),
                    val['method'],
                    val['scenario'] or 'BASE',
                    f"{val['intrinsic_value']:.2f}" if val['intrinsic_value'] else '',
                    f"{val['cmp']:.2f}" if val['cmp'] else '',
                    f"{val['upside_pct']:.1f}%" if val['upside_pct'] else '',
                    # WACC components
                    self._fmt_pct(ka.get('wacc')),
                    self._fmt_pct2(ka.get('beta')),
                    self._fmt_pct(ka.get('cost_of_equity')),
                    # Terminal assumptions
                    self._fmt_pct(ka.get('terminal_growth')),
                    self._fmt_pct(ka.get('terminal_roce')),
                    self._fmt_pct(ka.get('terminal_reinvestment')),
                    self._fmt_pct(ka.get('terminal_value_pct')),
                    # Operating ratios
                    self._fmt_pct(ka.get('ebitda_margin')),
                    self._fmt_pct(ka.get('capex_to_sales')),
                    self._fmt_pct(ka.get('tax_rate')),
                    # Value breakdown (Cr)
                    self._fmt_cr(ka.get('firm_value')),
                    self._fmt_cr(ka.get('equity_value')),
                    self._fmt_cr(ka.get('net_debt')),
                    self._fmt_pct2(ka.get('shares_outstanding')),
                    # Market multiples from monthly prices
                    *self._get_pe_pb_bv_mcap(val['nse_symbol'], val['cmp'], price_lookup),
                    # Metadata
                    str(val['created_at']),
                    val['created_by']
                ])

            # Append new rows after existing data (never clear — preserve history)
            existing_data = ws.get_all_values()
            # Detect truly empty sheet (no data or only empty rows)
            has_data = any(any(cell.strip() for cell in row) for row in existing_data) if existing_data else False
            if not has_data:
                # Empty sheet — write header first
                ws.update(values=[headers], range_name='A1')
                ws.format('A1:AG1', {
                    'textFormat': {'bold': True},
                    'backgroundColor': {'red': 0.2, 'green': 0.6, 'blue': 0.8}
                })
                append_start = 2
                existing_data = []  # reset for dedup logic below
                logger.info("  Sheet was empty — wrote header row")
            else:
                append_start = len(existing_data) + 1
                # Ensure header row exists and matches (check col A)
                if not existing_data[0] or existing_data[0][0] != 'ID':
                    # Header is missing or wrong — insert header at row 1
                    ws.update(values=[headers], range_name='A1')
                    ws.format('A1:AG1', {
                        'textFormat': {'bold': True},
                        'backgroundColor': {'red': 0.2, 'green': 0.6, 'blue': 0.8}
                    })
                    append_start = len(existing_data) + 1

            # Deduplicate: skip valuation IDs already in the sheet
            existing_ids = set()
            for erow in existing_data[1:]:  # skip header
                if erow and erow[0]:
                    existing_ids.add(erow[0].strip())

            data_rows = rows[1:]  # skip header from rows (already in sheet)
            new_rows = [r for r in data_rows if r[0] not in existing_ids]

            if not new_rows:
                logger.info("  All valuations already in GSheet — nothing to append")
                return

            # Expand sheet if needed (GSheet default may be too small)
            total_rows_needed = append_start + len(new_rows)
            cols_needed = len(headers)
            if total_rows_needed > ws.row_count or cols_needed > ws.col_count:
                new_row_count = max(ws.row_count, total_rows_needed + 500)
                new_col_count = max(ws.col_count, cols_needed)
                ws.resize(rows=new_row_count, cols=new_col_count)
                logger.info(f"  Expanded sheet to {new_row_count} rows x {new_col_count} cols")

            # Append in batches of 100
            BATCH_SIZE = 100
            for batch_start in range(0, len(new_rows), BATCH_SIZE):
                batch_end = min(batch_start + BATCH_SIZE, len(new_rows))
                batch = new_rows[batch_start:batch_end]
                target_row = append_start + batch_start
                ws.update(values=batch, range_name=f'A{target_row}')
                if batch_end < len(new_rows):
                    logger.info(f"  Appended rows {batch_start+1}-{batch_end} of {len(new_rows)}")
                    time.sleep(1)  # Rate limit courtesy pause between batches

            logger.info(f"✓ Appended {len(new_rows)} new valuations to GSheet (total rows now: {append_start + len(new_rows) - 1})")

        except Exception as e:
            logger.error(f"Failed to update Google Sheets: {e}")
            logger.error(traceback.format_exc())

    def print_summary(self):
        """Print batch execution summary."""
        print(f"\n{'='*80}")
        print(f"BATCH VALUATION SUMMARY")
        print(f"{'='*80}")
        print(f"Mode: {self.mode.upper()}")
        skipped_resume = [s for s in self.results['skipped'] if s.get('reason') == 'already valued today']
        skipped_other = [s for s in self.results['skipped'] if s.get('reason') != 'already valued today']
        print(f"Successful: {len(self.results['success'])}")
        print(f"Failed: {len(self.results['failed'])}")
        if skipped_resume:
            print(f"Skipped (already valued today): {len(skipped_resume)}")
        if skipped_other:
            print(f"Skipped (other): {len(skipped_other)}")
        print(f"{'='*80}\n")

        if self.results['success']:
            print("Successful Valuations:")
            for r in self.results['success']:
                sym = r.get('symbol') or '???'
                print(f"  ✓ {sym:12s} | Intrinsic: ₹{r['intrinsic']:>8,.2f} | CMP: ₹{r['cmp']:>8,.2f} | {r['upside']:>6.1f}%")

        if self.results['failed']:
            print("\nFailed Valuations:")
            for r in self.results['failed']:
                sym = r.get('symbol') or '???'
                print(f"  ✗ {sym:12s} | Error: {r.get('error', 'unknown')}")


def main():
    parser = argparse.ArgumentParser(description='Batch Valuation Runner')
    parser.add_argument('--source', choices=['gsheet', 'database'], default='gsheet',
                        help='Source of company list (default: gsheet)')
    parser.add_argument('--mode', choices=['quick', 'full'], default='quick',
                        help='quick=DB+Sheets only, full=DB+Sheets+Excel (default: quick)')
    parser.add_argument('--symbols', type=str,
                        help='Comma-separated list of symbols (overrides source)')
    parser.add_argument('--limit', type=int,
                        help='Limit number of companies to process')
    parser.add_argument('--resume', action='store_true',
                        help='Skip companies already valued today (resume interrupted batch)')
    parser.add_argument('--gsheet-all', action='store_true',
                        help='Write latest 100 valuations from DB to GSheet (default: only this run)')

    args = parser.parse_args()

    print(f"\n{'#'*80}")
    print(f"# BATCH VALUATION - MODE: {args.mode.upper()}")
    print(f"# Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"{'#'*80}\n")

    # Initialize batch valuator
    batch = BatchValuator(mode=args.mode)

    # Get companies
    if args.symbols:
        # Parse symbols from command line
        symbols = [s.strip() for s in args.symbols.split(',')]
        companies_dict = get_active_companies(mysql_client=batch.mysql)
        companies = []
        for symbol in symbols:
            if symbol in companies_dict:
                comp = companies_dict[symbol]
                companies.append({
                    'symbol': symbol,
                    'name': comp.get('company_name', comp.get('csv_name')),
                    'valuation_group': comp.get('valuation_group', ''),
                    'valuation_subgroup': comp.get('valuation_subgroup', ''),
                    'csv_name': comp.get('csv_name')
                })
            else:
                # Fallback: look up symbol directly in kbapp_marketscrip
                logger.info(f"Symbol {symbol} not in vs_active_companies, looking up in kbapp_marketscrip...")
                scrip = batch.mysql.query_one(
                    "SELECT marketscrip_id, symbol, name, sector, industry "
                    "FROM mssdb.kbapp_marketscrip WHERE symbol = %s AND scrip_type IN ('', 'EQS') LIMIT 1",
                    (symbol,)
                )
                if scrip:
                    # Use the name from kbapp_marketscrip as csv_name (matches core CSV 'Company Name')
                    csv_name = scrip['name']
                    logger.info(f"Found {symbol} in kbapp_marketscrip: {csv_name} (id={scrip['marketscrip_id']})")
                    companies.append({
                        'symbol': symbol,
                        'name': csv_name,
                        'valuation_group': scrip.get('sector', ''),
                        'valuation_subgroup': scrip.get('industry', ''),
                        'csv_name': csv_name
                    })
                else:
                    logger.warning(f"Symbol {symbol} not found in kbapp_marketscrip either — skipping")
    elif args.source == 'gsheet':
        logger.info("Loading companies from Google Sheets...")
        companies = batch.get_companies_from_gsheet()
    else:
        logger.info("Loading companies from database...")
        companies = batch.get_companies_from_database()

    # Apply limit
    if args.limit:
        companies = companies[:args.limit]

    logger.info(f"Found {len(companies)} companies to value")

    # Resume mode: load already-completed symbols
    if args.resume:
        batch.load_already_valued_today()

    # Run valuations
    start_time = time.time()

    for i, company in enumerate(companies, 1):
        symbol = company.get('symbol')
        if not symbol or not company.get('csv_name'):
            logger.warning(f"[{i}/{len(companies)}] Skipping company with missing symbol or csv_name: {company}")
            batch.results['skipped'].append({'symbol': symbol or '???', 'reason': 'missing symbol or csv_name'})
            continue

        if args.resume and symbol in batch.already_done:
            batch.results['skipped'].append({'symbol': symbol, 'reason': 'already valued today'})
            continue

        logger.info(f"\n[{i}/{len(companies)}] Processing {symbol}...")

        try:
            if args.mode == 'full':
                batch.run_full_valuation(company)
            else:
                batch.run_quick_valuation(company)

        except KeyboardInterrupt:
            logger.warning("\nBatch interrupted by user")
            break
        except Exception as e:
            tb = traceback.format_exc()
            logger.error(f"Unexpected error: {e}\n{tb}")
            batch.results['failed'].append({
                'symbol': company.get('symbol', '???'),
                'error': str(e),
                'traceback': tb
            })

    elapsed = time.time() - start_time

    # Update Google Sheets
    logger.info("\nUpdating Google Sheets...")
    batch.update_gsheet_results(only_current_run=not args.gsheet_all)

    # Write issues CSV
    issues_csv = batch.write_issues_csv()

    # Print summary
    batch.print_summary()

    print(f"\nTotal Time: {elapsed/60:.1f} minutes")
    if len(companies) > 0:
        print(f"Average: {elapsed/len(companies):.1f} seconds per company")
    else:
        logger.warning("No companies were processed — nothing to value")
        print("No companies were processed.")

    if issues_csv:
        print(f"\nIssues CSV: {issues_csv}")
    print(f"Batch log: {Path(__file__).parent.parent / 'logs' / 'batch_valuation.log'}\n")

    return 0 if not batch.results['failed'] else 1


if __name__ == '__main__':
    sys.exit(main())
