#!/usr/bin/env python3
"""
Run valuation for a single company using database config
"""

import sys
import os
import argparse
import logging
import json
from pathlib import Path
from datetime import datetime

sys.path.insert(0, str(Path(__file__).parent.parent))

from data.loaders.core_loader import CoreDataLoader
from data.loaders.price_loader import PriceLoader
from data.loaders.damodaran_loader import DamodaranLoader
from data.processors.financial_processor import FinancialProcessor
from models.dcf_model import FCFFValuation, DCFInputs
from models.relative_valuation import RelativeValuation
from storage.mysql_client import ValuationMySQLClient
from utils.config_loader import get_active_companies

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(name)s: %(message)s')
logger = logging.getLogger(__name__)


def run_valuation(symbol: str):
    """Run full valuation for a company."""

    # Initialize loaders
    core_loader = CoreDataLoader()
    price_loader = PriceLoader()
    damodaran_loader = DamodaranLoader()
    mysql_client = ValuationMySQLClient.get_instance()

    # Load companies from database
    companies = get_active_companies(mysql_client=mysql_client)

    if symbol not in companies:
        logger.error(f"Company {symbol} not found in database")
        logger.info(f"Available: {list(companies.keys())[:10]}...")
        return None

    company = companies[symbol]
    logger.info(f"\n{'='*80}")
    logger.info(f"Valuation for {symbol}: {company.get('csv_name', symbol)}")
    logger.info(f"Group: {company.get('valuation_group')}")
    logger.info(f"Subgroup: {company.get('valuation_subgroup')}")
    logger.info(f"{'='*80}\n")

    # Get company data
    csv_name = company.get('csv_name', symbol)

    # Load financial data
    processor = FinancialProcessor(core_loader, price_loader, damodaran_loader)
    sector = company.get('valuation_group', 'INDUSTRIALS')
    dcf_dict = processor.build_dcf_inputs(csv_name, sector)

    if not dcf_dict:
        logger.error(f"Failed to build DCF inputs for {csv_name}")
        return None

    # Convert dict to DCFInputs object
    dcf_inputs = DCFInputs(**{
        k: v for k, v in dcf_dict.items()
        if k in DCFInputs.__dataclass_fields__
    })

    # Run DCF valuation
    dcf_model = FCFFValuation()
    dcf_result = dcf_model.calculate_intrinsic_value(dcf_inputs)

    logger.info(f"\n{'='*80}")
    logger.info(f"DCF VALUATION RESULTS - {symbol}")
    logger.info(f"{'='*80}")
    logger.info(f"Intrinsic Value: ₹{dcf_result.get('intrinsic_per_share', 0):.2f}")
    logger.info(f"WACC: {dcf_result.get('wacc', 0)*100:.2f}%")
    logger.info(f"Terminal Value: ₹{dcf_result.get('terminal_value', 0)/1e7:.2f} Cr")
    logger.info(f"Terminal Value %: {dcf_result.get('terminal_value_pct', 0):.1f}%")
    logger.info(f"Firm Value: ₹{dcf_result.get('firm_value', 0)/1e7:.2f} Cr")
    logger.info(f"Equity Value: ₹{dcf_result.get('equity_value', 0)/1e7:.2f} Cr")
    logger.info(f"{'='*80}\n")

    # Get current price
    price_data = price_loader.get_latest_data(symbol)
    current_price = 0
    upside = 0
    if price_data:
        current_price = price_data.get('cmp', 0)
        intrinsic = dcf_result.get('intrinsic_per_share', 0)
        upside = ((intrinsic / current_price) - 1) * 100 if current_price > 0 else 0

        logger.info(f"{'='*80}")
        logger.info(f"MARKET COMPARISON")
        logger.info(f"{'='*80}")
        logger.info(f"Current Price: ₹{current_price:.2f}")
        logger.info(f"Intrinsic Value: ₹{intrinsic:.2f}")
        logger.info(f"Upside/Downside: {upside:+.1f}%")
        logger.info(f"{'='*80}\n")

    # Try relative valuation
    try:
        rel_val = RelativeValuation(core_loader, price_loader, mysql_client)
        rel_result = rel_val.value_company(
            csv_name,
            company.get('valuation_group'),
            peer_count=15
        )

        if rel_result.get('success'):
            logger.info(f"{'='*80}")
            logger.info(f"RELATIVE VALUATION")
            logger.info(f"{'='*80}")
            logger.info(f"Fair Value: ₹{rel_result.get('fair_value_per_share', 0):.2f}")
            logger.info(f"Peers: {rel_result.get('peers_used', 0)}")
            logger.info(f"{'='*80}\n")
    except Exception as e:
        logger.warning(f"Relative valuation failed: {e}")

    # Save to database
    try:
        company_id = company.get('company_id')
        if company_id:
            # Prepare key_assumptions JSON
            key_assumptions = {
                'wacc': dcf_result.get('wacc', 0),
                'terminal_growth': dcf_result.get('terminal_growth', 0),
                'terminal_roce': dcf_result.get('assumptions', {}).get('terminal_roce', 0),
                'revenue_growth': dcf_result.get('assumptions', {}).get('growth_rates', []),
                'ebitda_margin': dcf_result.get('assumptions', {}).get('ebitda_margin', 0),
            }

            valuation_data = {
                'company_id': company_id,
                'valuation_date': datetime.now().date(),
                'method': 'DCF',
                'scenario': 'BASE',
                'intrinsic_value': dcf_result.get('intrinsic_per_share', 0),
                'cmp': current_price if current_price > 0 else None,
                'upside_pct': upside if current_price > 0 else None,
                'key_assumptions': key_assumptions,
                'created_by': 'AGENT'
            }

            mysql_client.insert('vs_valuations', valuation_data)
            logger.info(f"✓ Valuation saved to database")
    except Exception as e:
        logger.warning(f"Failed to save to database: {e}")

    return dcf_result


def main():
    parser = argparse.ArgumentParser(description='Run valuation for a company')
    parser.add_argument('--symbol', required=True, help='NSE symbol (e.g., BEL)')

    args = parser.parse_args()

    result = run_valuation(args.symbol)

    if result:
        print(f"\n✓ Valuation complete for {args.symbol}")
        return 0
    else:
        print(f"\n✗ Valuation failed for {args.symbol}")
        return 1


if __name__ == '__main__':
    sys.exit(main())
