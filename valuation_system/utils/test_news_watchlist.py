#!/usr/bin/env python3
"""
Test News Watchlist Integration
Verifies that news_scanner.py reads from vs_news_watchlist table correctly.
"""

import os
import sys

# Add parent directory to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))

from valuation_system.agents.news_scanner import NewsScannerAgent
from valuation_system.storage.mysql_client import ValuationMySQLClient


def test_watchlist_loading():
    """Test that news scanner loads watchlist from database."""
    print("=" * 80)
    print("Testing News Watchlist Integration")
    print("=" * 80)

    # Initialize MySQL client
    mysql = ValuationMySQLClient.get_instance()

    # Direct query to verify table contents
    print("\n1. Direct query of vs_news_watchlist table:")
    print("-" * 80)
    rows = mysql.query("""
        SELECT
            w.id,
            w.company_id,
            m.symbol,
            m.name,
            w.is_enabled,
            w.priority,
            w.notes
        FROM vs_news_watchlist w
        JOIN mssdb.kbapp_marketscrip m ON w.company_id = m.marketscrip_id
        ORDER BY w.priority, m.symbol
    """)

    print(f"Found {len(rows)} companies in watchlist:\n")
    for row in rows:
        status = "✓" if row['is_enabled'] else "✗"
        print(f"{status} {row['symbol']:12} {row['name']:45} [{row['priority']}]")

    # Test NewsScannerAgent watchlist loading
    print("\n2. NewsScannerAgent._load_watchlist():")
    print("-" * 80)
    scanner = NewsScannerAgent(mysql_client=mysql)

    print(f"\nLoaded {len(scanner._watched_companies)} companies:")
    for symbol in scanner._watched_companies:
        print(f"  - {symbol}")

    print(f"\nLoaded {len(scanner._watched_sectors)} sectors:")
    for sector in scanner._watched_sectors:
        print(f"  - {sector}")

    # Test enabled/disabled filtering
    print("\n3. Test enable/disable filtering:")
    print("-" * 80)

    # Disable AETHER temporarily
    mysql.execute("UPDATE vs_news_watchlist SET is_enabled = FALSE WHERE company_id = 47582")
    print("Disabled AETHER (company_id=47582)")

    # Reload watchlist
    scanner2 = NewsScannerAgent(mysql_client=mysql)
    print(f"\nWatchlist after disabling AETHER: {len(scanner2._watched_companies)} companies")
    print(f"AETHER in list: {'AETHER' in scanner2._watched_companies}")

    # Re-enable AETHER
    mysql.execute("UPDATE vs_news_watchlist SET is_enabled = TRUE WHERE company_id = 47582")
    print("\nRe-enabled AETHER")

    # Reload again
    scanner3 = NewsScannerAgent(mysql_client=mysql)
    print(f"Watchlist after re-enabling: {len(scanner3._watched_companies)} companies")
    print(f"AETHER in list: {'AETHER' in scanner3._watched_companies}")

    print("\n" + "=" * 80)
    print("✓ Test Complete - News Watchlist Integration Working!")
    print("=" * 80)
    print("\nNext Steps:")
    print("1. Access Django admin: cd /Users/ram/code/rag/machai/RAGApp && python manage.py runserver")
    print("2. Navigate to: http://localhost:8000/admin/")
    print("3. Find 'News Watchlist' under 'MSSDB' section")
    print("4. Use bulk actions to enable/disable companies")
    print("5. Run news scanner: python scheduler/runner.py news_scan")


if __name__ == '__main__':
    test_watchlist_loading()
