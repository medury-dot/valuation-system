#!/usr/bin/env python3
"""
Populate Peer Statistics Cache
Pre-compute peer statistics for all valuation subgroups to enable fast quality score calculations.

Usage:
    # Populate all subgroups
    python -m valuation_system.utils.populate_peer_stats

    # Populate specific subgroup
    python -m valuation_system.utils.populate_peer_stats --subgroup BANKING_PRIVATE

    # Force refresh (ignore last_updated)
    python -m valuation_system.utils.populate_peer_stats --force
"""

import os
import sys
import logging
import argparse
from datetime import datetime, timedelta

# Add project root to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))

from valuation_system.data.loaders.core_loader import CoreDataLoader
from valuation_system.storage.mysql_client import ValuationMySQLClient

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def populate_peer_stats(subgroup_filter=None, force_refresh=False):
    """
    Pre-compute peer statistics for all valuation subgroups.

    Args:
        subgroup_filter: Optional specific subgroup to refresh (e.g., 'BANKING_PRIVATE')
        force_refresh: If True, refresh all; if False, skip recently updated (<7 days)
    """
    core = CoreDataLoader()
    mysql = ValuationMySQLClient()

    # Get all unique subgroups
    query = '''
        SELECT DISTINCT valuation_subgroup, COUNT(*) as count
        FROM vs_active_companies
        WHERE is_active = 1
    '''
    params = []

    if subgroup_filter:
        query += ' AND valuation_subgroup = %s'
        params.append(subgroup_filter)

    query += ' GROUP BY valuation_subgroup ORDER BY valuation_subgroup'

    subgroups = mysql.query(query, tuple(params) if params else ())

    logger.info(f"Found {len(subgroups)} valuation subgroups to process")

    total_updated = 0
    total_skipped = 0

    for sg in subgroups:
        subgroup = sg['valuation_subgroup']
        company_count = sg['count']

        # Check if recently updated (skip if < 7 days and not forcing)
        if not force_refresh:
            existing = mysql.query_one(
                'SELECT last_updated FROM vs_subgroup_peer_stats WHERE valuation_subgroup = %s',
                (subgroup,)
            )
            if existing and existing['last_updated']:
                days_old = (datetime.now() - existing['last_updated']).days
                if days_old < 7:
                    logger.info(f"  {subgroup}: Skipping (updated {days_old} days ago)")
                    total_skipped += 1
                    continue

        logger.info(f"  {subgroup}: Processing {company_count} companies...")

        # Get all companies in this subgroup
        companies = mysql.query('''
            SELECT DISTINCT a.company_id, m.symbol, m.name
            FROM vs_active_companies a
            JOIN mssdb.kbapp_marketscrip m ON a.company_id = m.marketscrip_id
            WHERE a.valuation_subgroup = %s AND a.is_active = 1
        ''', (subgroup,))

        roce_list = []
        growth_list = []
        de_list = []
        pledge_list = []

        for comp in companies:
            try:
                # Try to get financials by company name (most reliable for core CSV)
                financials = core.get_company_financials(comp['name'])

                if financials:
                    # Extract ROCE
                    roce_series = financials.get('roce', {})
                    if roce_series:
                        latest_roce = core.get_latest_value(roce_series)
                        if latest_roce and latest_roce > 0:
                            # Normalize to decimal (0.15 = 15%)
                            roce_normalized = latest_roce / 100 if latest_roce > 1 else latest_roce
                            roce_list.append(roce_normalized)

                    # Extract 5yr revenue CAGR
                    sales = financials.get('sales_annual', {})
                    if sales:
                        cagr = core.calculate_cagr(sales, years=5)
                        if cagr is not None:
                            growth_list.append(cagr)

                    # Extract D/E ratio
                    debt_series = financials.get('debt', {})
                    nw_series = financials.get('networth', {})

                    debt = core.get_latest_value(debt_series) or 0
                    nw = core.get_latest_value(nw_series) or 1

                    if nw > 0:
                        de_ratio = debt / nw
                        de_list.append(de_ratio)

                    # Extract promoter pledge (from fullstats quarterly data)
                    pledge_series = financials.get('pledgebypromoter_quarterly', {})
                    if pledge_series:
                        latest_pledge = core.get_latest_value(pledge_series)
                        if latest_pledge is not None:
                            pledge_list.append(latest_pledge)

            except Exception as e:
                logger.debug(f"    Skipping {comp['symbol']}: {e}")
                continue

        # Calculate medians
        import numpy as np

        median_roce = float(np.median(roce_list)) if roce_list else None
        median_cagr = float(np.median(growth_list)) if growth_list else None
        median_de = float(np.median(de_list)) if de_list else None
        median_pledge = float(np.median(pledge_list)) if pledge_list else None

        # Upsert to database
        mysql.execute('''
            INSERT INTO vs_subgroup_peer_stats (
                valuation_subgroup, peer_count,
                median_roce, median_revenue_cagr, median_de_ratio, median_promoter_pledge
            )
            VALUES (%s, %s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
                peer_count = VALUES(peer_count),
                median_roce = VALUES(median_roce),
                median_revenue_cagr = VALUES(median_revenue_cagr),
                median_de_ratio = VALUES(median_de_ratio),
                median_promoter_pledge = VALUES(median_promoter_pledge),
                last_updated = CURRENT_TIMESTAMP
        ''', (
            subgroup,
            len(companies),
            median_roce,
            median_cagr,
            median_de,
            median_pledge
        ))

        logger.info(f"    ✓ Updated: {len(roce_list)} ROCE, {len(growth_list)} Growth, "
                   f"{len(de_list)} D/E, {len(pledge_list)} Pledge")
        total_updated += 1

    logger.info(f"\nCompleted: {total_updated} updated, {total_skipped} skipped")
    return total_updated


def refresh_single_subgroup(valuation_subgroup):
    """
    Refresh peer stats for a single valuation subgroup.
    Called async from batch_valuation after each company valuation.

    Args:
        valuation_subgroup: The subgroup to refresh (e.g., 'BANKING_PRIVATE')
    """
    try:
        updated = populate_peer_stats(subgroup_filter=valuation_subgroup, force_refresh=True)
        logger.debug(f"Refreshed peer stats for {valuation_subgroup}")
        return updated
    except Exception as e:
        logger.debug(f"Failed to refresh peer stats for {valuation_subgroup}: {e}")
        return 0


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Populate peer statistics cache')
    parser.add_argument('--subgroup', type=str, help='Specific subgroup to refresh')
    parser.add_argument('--force', action='store_true', help='Force refresh all (ignore last_updated)')

    args = parser.parse_args()

    populate_peer_stats(subgroup_filter=args.subgroup, force_refresh=args.force)
