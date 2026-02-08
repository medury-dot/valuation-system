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
import logging
import time
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


class BatchValuator:
    def __init__(self, mode='quick'):
        self.mode = mode  # 'quick' or 'full'
        self.mysql = ValuationMySQLClient.get_instance()
        self.core_loader = CoreDataLoader()
        self.price_loader = PriceLoader()
        self.damodaran_loader = DamodaranLoader()

        # Track results
        self.results = {
            'success': [],
            'failed': [],
            'skipped': []
        }

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
        ws = sheet.worksheet('5. Active Companies')

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
            dcf_dict = processor.build_dcf_inputs(csv_name, valuation_group)

            if not dcf_dict:
                raise ValueError(f"Failed to build DCF inputs for {csv_name}")

            # Convert to DCFInputs object
            dcf_inputs = DCFInputs(**{
                k: v for k, v in dcf_dict.items()
                if k in DCFInputs.__dataclass_fields__
            })

            # Run DCF
            dcf_model = FCFFValuation()
            dcf_result = dcf_model.calculate_intrinsic_value(dcf_inputs)

            # Get current price
            price_data = self.price_loader.get_latest_data(symbol)
            cmp = price_data.get('cmp', 0) if price_data else 0

            intrinsic = dcf_result.get('intrinsic_per_share', 0)
            upside = ((intrinsic / cmp) - 1) * 100 if cmp > 0 else 0

            logger.info(f"  Intrinsic: ₹{intrinsic:,.2f}")
            logger.info(f"  CMP: ₹{cmp:,.2f}")
            logger.info(f"  Upside: {upside:+.1f}%")

            # Save to database
            company_id = self.mysql.query_one(
                "SELECT marketscrip_id FROM mssdb.kbapp_marketscrip WHERE symbol = %s",
                (symbol,)
            )

            if company_id:
                key_assumptions = {
                    'wacc': dcf_result.get('wacc', 0),
                    'terminal_growth': dcf_result.get('terminal_growth', 0),
                    'revenue_growth': dcf_result.get('assumptions', {}).get('growth_rates', []),
                    'ebitda_margin': dcf_result.get('assumptions', {}).get('ebitda_margin', 0),
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
            logger.error(f"✗ Failed: {e}")
            self.results['failed'].append({
                'symbol': symbol,
                'error': str(e)
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
                sector_outlook=sector_outlook
            )

            if 'error' not in result:
                # Generate Excel
                excel_path = generate_valuation_excel(result)
                logger.info(f"✓ Excel report: {excel_path}")

            return True

        except Exception as e:
            logger.error(f"✗ Excel generation failed: {e}")
            # Don't mark as failed if quick valuation succeeded
            return True

    def update_gsheet_results(self):
        """Update Google Sheets with batch results."""
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
            ws = sheet.worksheet('4. Recent Activity')

            # Get latest valuations from MySQL
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
                    v.created_at,
                    v.created_by
                FROM vs_valuations v
                JOIN mssdb.kbapp_marketscrip m ON v.company_id = m.marketscrip_id
                ORDER BY v.created_at DESC
                LIMIT 100
            ''')

            # Prepare data
            headers = ['ID', 'Symbol', 'Company', 'Val Date', 'Method', 'Scenario',
                       'Intrinsic', 'CMP', 'Upside %', 'Created At', 'Created By']

            rows = [headers]
            for val in valuations:
                rows.append([
                    str(val['id']),
                    val['nse_symbol'],
                    val['company_name'],
                    str(val['valuation_date']),
                    val['method'],
                    val['scenario'] or 'BASE',
                    f"{val['intrinsic_value']:.2f}" if val['intrinsic_value'] else '',
                    f"{val['cmp']:.2f}" if val['cmp'] else '',
                    f"{val['upside_pct']:.1f}%" if val['upside_pct'] else '',
                    str(val['created_at']),
                    val['created_by']
                ])

            # Update sheet
            ws.clear()
            ws.update(values=rows, range_name='A1')

            # Format header
            ws.format('A1:K1', {
                'textFormat': {'bold': True},
                'backgroundColor': {'red': 0.2, 'green': 0.6, 'blue': 0.8}
            })

            logger.info(f"✓ Updated Google Sheets with {len(valuations)} valuations")

        except Exception as e:
            logger.error(f"Failed to update Google Sheets: {e}")

    def print_summary(self):
        """Print batch execution summary."""
        print(f"\n{'='*80}")
        print(f"BATCH VALUATION SUMMARY")
        print(f"{'='*80}")
        print(f"Mode: {self.mode.upper()}")
        print(f"Successful: {len(self.results['success'])}")
        print(f"Failed: {len(self.results['failed'])}")
        print(f"Skipped: {len(self.results['skipped'])}")
        print(f"{'='*80}\n")

        if self.results['success']:
            print("Successful Valuations:")
            for r in self.results['success']:
                print(f"  ✓ {r['symbol']:12s} | Intrinsic: ₹{r['intrinsic']:>8,.2f} | CMP: ₹{r['cmp']:>8,.2f} | {r['upside']:>6.1f}%")

        if self.results['failed']:
            print("\nFailed Valuations:")
            for r in self.results['failed']:
                print(f"  ✗ {r['symbol']:12s} | Error: {r['error']}")


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

    # Run valuations
    start_time = time.time()

    for i, company in enumerate(companies, 1):
        logger.info(f"\n[{i}/{len(companies)}] Processing {company['symbol']}...")

        try:
            if args.mode == 'full':
                batch.run_full_valuation(company)
            else:
                batch.run_quick_valuation(company)

        except KeyboardInterrupt:
            logger.warning("\nBatch interrupted by user")
            break
        except Exception as e:
            logger.error(f"Unexpected error: {e}")
            batch.results['failed'].append({
                'symbol': company['symbol'],
                'error': str(e)
            })

    elapsed = time.time() - start_time

    # Update Google Sheets
    logger.info("\nUpdating Google Sheets...")
    batch.update_gsheet_results()

    # Print summary
    batch.print_summary()

    print(f"\nTotal Time: {elapsed/60:.1f} minutes")
    if len(companies) > 0:
        print(f"Average: {elapsed/len(companies):.1f} seconds per company\n")
    else:
        logger.warning("No companies were processed — nothing to value")
        print("No companies were processed.\n")

    return 0 if not batch.results['failed'] else 1


if __name__ == '__main__':
    sys.exit(main())
