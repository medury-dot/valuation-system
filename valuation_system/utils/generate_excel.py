"""
Generate Excel valuation report for a company.
Usage: python -m valuation_system.utils.generate_excel --symbol EICHERMOT
"""

import sys
import os
import logging
import argparse

# Add parent to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))

from valuation_system.data.loaders.core_loader import CoreDataLoader
from valuation_system.data.loaders.price_loader import PriceLoader
from valuation_system.data.loaders.damodaran_loader import DamodaranLoader
from valuation_system.storage.mysql_client import ValuationMySQLClient
from valuation_system.agents.valuator import ValuatorAgent
from valuation_system.agents.sector_analyst import SectorAnalystAgent
from valuation_system.utils.config_loader import load_sectors_config, load_companies_config
from valuation_system.utils.excel_report import generate_valuation_excel


def main():
    parser = argparse.ArgumentParser(description='Generate valuation Excel report')
    parser.add_argument('--symbol', required=True, help='NSE symbol (e.g., EICHERMOT)')
    parser.add_argument('--output', help='Output path for Excel file')
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s [%(levelname)s] %(name)s: %(message)s'
    )
    # Suppress noisy loggers during Excel generation
    for name in ['yfinance', 'urllib3', 'httpx', 'peewee']:
        logging.getLogger(name).setLevel(logging.WARNING)

    # Initialize components
    core = CoreDataLoader()
    prices = PriceLoader()
    damodaran = DamodaranLoader()
    mysql = ValuationMySQLClient()

    # Find company config
    raw_config = load_companies_config()
    companies = raw_config.get('companies', {})

    # Find company by symbol
    company_key = None
    company_cfg = None
    for key, cfg in companies.items():
        if cfg.get('nse_symbol') == args.symbol:
            company_key = key
            company_cfg = cfg
            break

    if not company_cfg:
        print(f"Company with symbol {args.symbol} not found in companies.yaml")
        sys.exit(1)

    sector_key = company_cfg['sector']

    # Create sector analyst and get outlook
    sector_analyst = SectorAnalystAgent(sector_key, mysql)
    sector_outlook = sector_analyst.calculate_sector_outlook()

    # Create valuator (it builds its own internal components)
    valuator = ValuatorAgent(core, prices, damodaran, mysql)

    # Run valuation
    company_name = company_cfg.get('csv_name', '')
    logging.info(f"Running valuation for {company_name} ({args.symbol})...")
    result = valuator.run_full_valuation(
        company_config=company_cfg,
        sector_outlook=sector_outlook,
    )

    if 'error' in result:
        print(f"Valuation failed: {result['error']}")
        sys.exit(1)

    # Generate Excel
    output_path = args.output or None
    excel_path = generate_valuation_excel(result, output_path)
    print(f"\nExcel report generated: {excel_path}")
    print(f"  Company: {company_name} ({args.symbol})")
    print(f"  CMP: Rs {result.get('cmp', 0):,.2f}")
    print(f"  Blended Value: Rs {result.get('intrinsic_value_blended', 0):,.2f}")
    upside = result.get('upside_pct')
    if upside is not None:
        print(f"  Upside: {upside:+.1f}%")


if __name__ == '__main__':
    main()
