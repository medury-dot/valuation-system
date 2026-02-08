#!/usr/bin/env python3
"""
Sync Driver Definitions to Google Sheet
- Syncs GROUP drivers to Tab 2 "Valuation Group Drivers"
- Syncs SUBGROUP drivers to Tab 3 "Valuation Subgroup Drivers"
"""

import os
import sys
import mysql.connector
import gspread
from google.oauth2.service_account import Credentials
from datetime import datetime

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))

from dotenv import load_dotenv
load_dotenv('/Users/ram/code/research/valuation_system/config/.env')


def get_drivers_from_mysql(driver_level):
    """Get drivers from MySQL for a given level"""
    conn = mysql.connector.connect(
        host=os.getenv('MYSQL_HOST', 'localhost'),
        port=int(os.getenv('MYSQL_PORT', 3306)),
        user=os.getenv('MYSQL_USER', 'root'),
        password=os.getenv('MYSQL_PASSWORD', ''),
        database=os.getenv('MYSQL_DATABASE', 'rag')
    )
    cursor = conn.cursor(dictionary=True)

    cursor.execute("""
        SELECT
            valuation_group,
            valuation_subgroup,
            driver_category,
            driver_name,
            weight,
            impact_direction,
            trend,
            current_value,
            last_updated
        FROM vs_drivers
        WHERE driver_level = %s
        ORDER BY valuation_group, valuation_subgroup, driver_category, driver_name
    """, (driver_level,))

    drivers = cursor.fetchall()
    cursor.close()
    conn.close()
    return drivers


def sync_group_drivers(gc, sheet_id):
    """Sync GROUP drivers to Tab 2"""
    sh = gc.open_by_key(sheet_id)
    ws = sh.worksheet("2. Valuation Group Drivers")

    # Get drivers from MySQL
    drivers = get_drivers_from_mysql('GROUP')
    print(f"  Found {len(drivers)} GROUP drivers in MySQL")

    # Prepare data with headers
    headers = ['Valuation Group', 'Category', 'Driver Name', 'Weight', 'Impact', 'Trend', 'Current Value', 'Last Updated']
    rows = [headers]

    for d in drivers:
        rows.append([
            d['valuation_group'] or '',
            d['driver_category'] or '',
            d['driver_name'] or '',
            float(d['weight']) if d['weight'] else 0.0,
            d['impact_direction'] or 'NEUTRAL',
            d['trend'] or 'STABLE',
            d['current_value'] or '',
            str(d['last_updated']) if d['last_updated'] else ''
        ])

    # Clear and update
    ws.clear()
    ws.update(range_name='A1', values=rows)
    print(f"  Synced {len(rows)-1} GROUP drivers to Tab 2")
    return len(rows) - 1


def sync_subgroup_drivers(gc, sheet_id):
    """Sync SUBGROUP drivers to Tab 3"""
    sh = gc.open_by_key(sheet_id)
    ws = sh.worksheet("3. Valuation Subgroup Drivers")

    # Get drivers from MySQL
    drivers = get_drivers_from_mysql('SUBGROUP')
    print(f"  Found {len(drivers)} SUBGROUP drivers in MySQL")

    # Prepare data with headers
    headers = ['Valuation Group', 'Valuation Subgroup', 'Category', 'Driver Name', 'Weight', 'Impact', 'Trend', 'Current Value', 'Last Updated']
    rows = [headers]

    for d in drivers:
        rows.append([
            d['valuation_group'] or '',
            d['valuation_subgroup'] or '',
            d['driver_category'] or '',
            d['driver_name'] or '',
            float(d['weight']) if d['weight'] else 0.0,
            d['impact_direction'] or 'NEUTRAL',
            d['trend'] or 'STABLE',
            d['current_value'] or '',
            str(d['last_updated']) if d['last_updated'] else ''
        ])

    # Clear and update
    ws.clear()
    ws.update(range_name='A1', values=rows)
    print(f"  Synced {len(rows)-1} SUBGROUP drivers to Tab 3")
    return len(rows) - 1


def sync_active_companies(gc, sheet_id):
    """Sync active companies with subgroups to Tab 6"""
    sh = gc.open_by_key(sheet_id)
    ws = sh.worksheet("6. Active Companies")

    # Get companies from MySQL
    conn = mysql.connector.connect(
        host=os.getenv('MYSQL_HOST', 'localhost'),
        port=int(os.getenv('MYSQL_PORT', 3306)),
        user=os.getenv('MYSQL_USER', 'root'),
        password=os.getenv('MYSQL_PASSWORD', ''),
        database=os.getenv('MYSQL_DATABASE', 'rag')
    )
    cursor = conn.cursor(dictionary=True)

    cursor.execute("""
        SELECT
            company_id,
            nse_symbol,
            company_name,
            valuation_group,
            valuation_subgroup,
            cd_sector,
            cd_industry
        FROM vs_active_companies
        WHERE is_active = 1 OR is_active = 0
        ORDER BY valuation_group, valuation_subgroup, company_name
    """)

    companies = cursor.fetchall()
    cursor.close()
    conn.close()

    print(f"  Found {len(companies)} active companies in MySQL")

    # Prepare data with headers
    headers = ['Company ID', 'NSE Symbol', 'Company Name', 'Valuation Group', 'Valuation Subgroup', 'CD Sector', 'CD Industry']
    rows = [headers]

    for c in companies:
        rows.append([
            c['company_id'] or '',
            c['nse_symbol'] or '',
            c['company_name'] or '',
            c['valuation_group'] or '',
            c['valuation_subgroup'] or '',
            c['cd_sector'] or '',
            c['cd_industry'] or ''
        ])

    # Clear and update
    ws.clear()
    ws.update(range_name='A1', values=rows)
    print(f"  Synced {len(rows)-1} companies to Tab 6")
    return len(rows) - 1


def main():
    print("=" * 70)
    print("SYNC DRIVERS TO GOOGLE SHEET")
    print("=" * 70)

    # Setup Google Sheets
    scopes = ['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive']
    creds = Credentials.from_service_account_file(os.getenv('GSHEET_AUTH_PATH'), scopes=scopes)
    gc = gspread.authorize(creds)

    sheet_id = os.getenv('GSHEET_DRIVERS_ID')
    print(f"\n[1] Connected to GSheet: {sheet_id}")

    # Sync GROUP drivers
    print("\n[2] Syncing GROUP drivers to Tab 2...")
    group_count = sync_group_drivers(gc, sheet_id)

    # Sync SUBGROUP drivers
    print("\n[3] Syncing SUBGROUP drivers to Tab 3...")
    subgroup_count = sync_subgroup_drivers(gc, sheet_id)

    # Sync active companies
    print("\n[4] Syncing active companies to Tab 6...")
    company_count = sync_active_companies(gc, sheet_id)

    print("\n" + "=" * 70)
    print("SUMMARY")
    print("=" * 70)
    print(f"GROUP drivers synced: {group_count}")
    print(f"SUBGROUP drivers synced: {subgroup_count}")
    print(f"Active companies synced: {company_count}")
    print(f"\nGSheet URL: https://docs.google.com/spreadsheets/d/{sheet_id}")

    return group_count, subgroup_count, company_count


if __name__ == '__main__':
    main()
