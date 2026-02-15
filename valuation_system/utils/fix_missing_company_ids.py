"""
Fix existing events with scope=COMPANY but company_id=NULL
Backfills company_id by matching headline against marketscrip.
"""

import os
import sys
import logging
import re

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))

from valuation_system.storage.mysql_client import MySQLClient

logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')
logger = logging.getLogger(__name__)


def extract_company_from_headline(mysql, headline: str) -> int:
    """Extract company_id by matching headline against marketscrip."""
    if not headline:
        return None

    try:
        companies = mysql.query("""
            SELECT marketscrip_id, symbol, name
            FROM mssdb.kbapp_marketscrip
            WHERE scrip_type IN ('', 'EQS')
              AND symbol IS NOT NULL
              AND symbol != ''
              AND name IS NOT NULL
            ORDER BY LENGTH(name) DESC
            LIMIT 1000
        """)

        headline_upper = headline.upper()

        # Try exact symbol match first
        for company in companies:
            symbol = company.get('symbol', '')
            if symbol and symbol.upper() in headline_upper:
                pattern = r'\b' + re.escape(symbol.upper()) + r'\b'
                if re.search(pattern, headline_upper):
                    return company['marketscrip_id']

        # Try company name match
        for company in companies:
            name = company.get('name', '')
            if not name or len(name) < 5:
                continue

            name_parts = name.replace(' Limited', '').replace(' Ltd', '').replace(' Pvt', '')
            name_clean = name_parts.split()[0] if name_parts else name

            if len(name_clean) >= 5 and name_clean.upper() in headline_upper:
                return company['marketscrip_id']

    except Exception as e:
        logger.error(f"Failed to extract company: {e}")

    return None


def fix_missing_company_ids():
    """Find and fix all events with scope=COMPANY but company_id=NULL."""
    mysql = MySQLClient()

    # Find broken events
    broken_events = mysql.query("""
        SELECT id, headline, source
        FROM vs_event_timeline
        WHERE scope = 'COMPANY' AND company_id IS NULL
        ORDER BY id
    """)

    logger.info(f"Found {len(broken_events)} events with scope=COMPANY but no company_id")

    fixed = 0
    not_found = 0

    for event in broken_events:
        event_id = event['id']
        headline = event['headline']

        logger.info(f"\nEvent {event_id}: {headline[:80]}")

        company_id = extract_company_from_headline(mysql, headline)

        if company_id:
            # Get company details for confirmation
            company = mysql.query("""
                SELECT symbol, name
                FROM mssdb.kbapp_marketscrip
                WHERE marketscrip_id = %s
            """, (company_id,))

            if company:
                comp = company[0]
                logger.info(f"  → Matched: {comp['symbol']} - {comp['name']}")
                logger.info(f"  → Updating company_id={company_id}")

                # Update the record
                mysql.execute("""
                    UPDATE vs_event_timeline
                    SET company_id = %s
                    WHERE id = %s
                """, (company_id, event_id))

                fixed += 1
            else:
                logger.warning(f"  → Company ID {company_id} not found in marketscrip!")
                not_found += 1
        else:
            logger.warning(f"  → Could not match company from headline")
            not_found += 1

    logger.info(f"\n{'='*60}")
    logger.info(f"Summary: {fixed} fixed, {not_found} not found")
    logger.info(f"{'='*60}")


if __name__ == '__main__':
    fix_missing_company_ids()
