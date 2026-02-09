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
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta

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
            is_active,
            source,
            linked_macro_driver,
            link_direction,
            company_id,
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
    headers = ['Valuation Group', 'Category', 'Driver Name', 'Weight', 'Impact', 'Trend',
               'Current Value', 'Is Active', 'Source', 'Linked Macro', 'Link Direction', 'Last Updated']
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
            'TRUE' if d.get('is_active', 1) else 'FALSE',
            d.get('source', 'SEED') or 'SEED',
            d.get('linked_macro_driver') or '',
            d.get('link_direction') or '',
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
    headers = ['Valuation Group', 'Valuation Subgroup', 'Category', 'Driver Name', 'Weight', 'Impact', 'Trend',
               'Current Value', 'Is Active', 'Source', 'Linked Macro', 'Link Direction', 'Last Updated']
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
            'TRUE' if d.get('is_active', 1) else 'FALSE',
            d.get('source', 'SEED') or 'SEED',
            d.get('linked_macro_driver') or '',
            d.get('link_direction') or '',
            str(d['last_updated']) if d['last_updated'] else ''
        ])

    # Clear and update
    ws.clear()
    ws.update(range_name='A1', values=rows)
    print(f"  Synced {len(rows)-1} SUBGROUP drivers to Tab 3")
    return len(rows) - 1


def sync_macro_drivers(gc, sheet_id):
    """Sync MACRO drivers to Tab 1 '1. Macro Drivers' with metadata."""
    sh = gc.open_by_key(sheet_id)
    ws = sh.worksheet("1. Macro Drivers")

    # Get MACRO drivers from MySQL
    drivers = get_drivers_from_mysql('MACRO')
    print(f"  Found {len(drivers)} MACRO drivers in MySQL")

    # Metadata for display: category, unit, source, frequency, description, weight, scenarios, lag_months
    # lag_months = typical delay from reference period end until data is published
    #   market_indicators: 0 (real-time/daily), CPI: 1mo, IIP: 2mo, WPI: 1mo, GDP/GVA: 3mo, PLFS: 3mo
    #   FRED bond yield: 0 (monthly, near real-time), RBI repo: 0 (event-driven)
    MACRO_DRIVER_METADATA = {
        'gdp_growth':             {'category': 'Economy',   'unit': '%',     'source': 'MOSPI',       'frequency': 'Quarterly', 'description': 'India GDP growth rate',                    'weight': 0.08, 'bull': '7.5%', 'base': '6.5%', 'bear': '5.0%', 'lag_months': 3},
        'pmi_composite':          {'category': 'Economy',   'unit': 'Index', 'source': 'S&P Global',  'frequency': 'Monthly',   'description': 'Composite PMI (>50 = expansion)',          'weight': 0.06, 'bull': '58',   'base': '54',   'bear': '48',   'lag_months': 0},
        'pmi_manufacturing':      {'category': 'Economy',   'unit': 'Index', 'source': 'S&P Global',  'frequency': 'Monthly',   'description': 'Manufacturing PMI',                        'weight': 0.05, 'bull': '57',   'base': '53',   'bear': '48',   'lag_months': 0},
        'pmi_services':           {'category': 'Economy',   'unit': 'Index', 'source': 'S&P Global',  'frequency': 'Monthly',   'description': 'Services PMI',                             'weight': 0.05, 'bull': '60',   'base': '55',   'bear': '49',   'lag_months': 0},
        'interest_rate_10y':      {'category': 'WACC',      'unit': '%',     'source': 'FRED',        'frequency': 'Monthly',   'description': 'India 10Y bond yield (risk-free rate)',     'weight': 0.12, 'bull': '6.0%', 'base': '7.0%', 'bear': '8.0%', 'lag_months': 0},
        'repo_rate':              {'category': 'WACC',      'unit': '%',     'source': 'RBI',         'frequency': 'As needed', 'description': 'RBI repo rate',                            'weight': 0.08, 'bull': '5.5%', 'base': '6.25%','bear': '7.0%', 'lag_months': 0},
        'usd_inr':                {'category': 'Trade',     'unit': 'INR',   'source': 'RBI/Market',  'frequency': 'Daily',     'description': 'USD/INR exchange rate',                    'weight': 0.06, 'bull': '82',   'base': '85',   'bear': '88',   'lag_months': 0},
        'balance_of_trade':       {'category': 'Trade',     'unit': 'USD Bn','source': 'RBI',         'frequency': 'Monthly',   'description': 'Monthly trade balance (exports-imports)',   'weight': 0.04, 'bull': '-15',  'base': '-20',  'bear': '-28',  'lag_months': 0},
        'fii_flows':              {'category': 'Flows',     'unit': 'INR Cr','source': 'NSDL/CDSL',   'frequency': 'Daily',     'description': 'Foreign Institutional Investor flows',     'weight': 0.04, 'bull': '+5000','base': '0',    'bear': '-5000','lag_months': 0},
        'dii_flows':              {'category': 'Flows',     'unit': 'INR Cr','source': 'NSDL/CDSL',   'frequency': 'Daily',     'description': 'Domestic Institutional Investor flows',    'weight': 0.03, 'bull': '+3000','base': '+1000','bear': '-500', 'lag_months': 0},
        'cpi_headline':           {'category': 'Inflation', 'unit': '% YoY', 'source': 'MOSPI',      'frequency': 'Monthly',   'description': 'CPI headline inflation (General Index)',   'weight': 0.06, 'bull': '3.5%', 'base': '5.0%', 'bear': '7.0%', 'lag_months': 1},
        'cpi_food_inflation':     {'category': 'Inflation', 'unit': '% YoY', 'source': 'MOSPI',      'frequency': 'Monthly',   'description': 'CPI food & beverages inflation',           'weight': 0.04, 'bull': '3.0%', 'base': '6.0%', 'bear': '10.0%','lag_months': 1},
        'wpi_inflation':          {'category': 'Inflation', 'unit': '% YoY', 'source': 'MOSPI',      'frequency': 'Monthly',   'description': 'WPI overall wholesale inflation',          'weight': 0.03, 'bull': '1.0%', 'base': '3.0%', 'bear': '6.0%', 'lag_months': 1},
        'wpi_fuel_power':         {'category': 'Inflation', 'unit': '% YoY', 'source': 'MOSPI',      'frequency': 'Monthly',   'description': 'WPI fuel & power inflation',               'weight': 0.04, 'bull': '-2.0%','base': '3.0%', 'bear': '10.0%','lag_months': 1},
        'wpi_manufactured':       {'category': 'Inflation', 'unit': '% YoY', 'source': 'MOSPI',      'frequency': 'Monthly',   'description': 'WPI manufactured products inflation',      'weight': 0.04, 'bull': '0.5%', 'base': '2.5%', 'bear': '5.0%', 'lag_months': 1},
        'wpi_primary_articles':   {'category': 'Inflation', 'unit': '% YoY', 'source': 'MOSPI',      'frequency': 'Monthly',   'description': 'WPI primary articles (food, minerals)',    'weight': 0.03, 'bull': '2.0%', 'base': '5.0%', 'bear': '9.0%', 'lag_months': 1},
        'gva_manufacturing_real': {'category': 'GDP',       'unit': '% YoY', 'source': 'MOSPI',      'frequency': 'Quarterly', 'description': 'GVA Manufacturing real growth',            'weight': 0.03, 'bull': '10.0%','base': '6.0%', 'bear': '2.0%', 'lag_months': 3},
        'gva_construction_real':  {'category': 'GDP',       'unit': '% YoY', 'source': 'MOSPI',      'frequency': 'Quarterly', 'description': 'GVA Construction real growth',             'weight': 0.02, 'bull': '12.0%','base': '7.0%', 'bear': '3.0%', 'lag_months': 3},
        'gva_financial_real':     {'category': 'GDP',       'unit': '% YoY', 'source': 'MOSPI',      'frequency': 'Quarterly', 'description': 'GVA Financial/RE/Prof Services growth',    'weight': 0.02, 'bull': '9.0%', 'base': '6.0%', 'bear': '3.0%', 'lag_months': 3},
        'gva_agriculture_real':   {'category': 'GDP',       'unit': '% YoY', 'source': 'MOSPI',      'frequency': 'Quarterly', 'description': 'GVA Agriculture real growth',              'weight': 0.02, 'bull': '5.0%', 'base': '3.0%', 'bear': '0.5%', 'lag_months': 3},
        'iip_manufacturing':      {'category': 'Industry',  'unit': 'Index', 'source': 'MOSPI',      'frequency': 'Monthly',   'description': 'IIP Manufacturing index',                  'weight': 0.04, 'bull': '140',  'base': '130',  'bear': '115',  'lag_months': 2},
        'unemployment_rate':      {'category': 'Labour',    'unit': '%',     'source': 'MOSPI PLFS',  'frequency': 'Quarterly', 'description': 'Unemployment rate (15+ age)',              'weight': 0.03, 'bull': '5.0%', 'base': '7.0%', 'bear': '9.0%', 'lag_months': 3},
        'lfpr_total':             {'category': 'Labour',    'unit': '%',     'source': 'MOSPI PLFS',  'frequency': 'Quarterly', 'description': 'Labour force participation rate (15+)',    'weight': 0.02, 'bull': '60.0%','base': '55.0%','bear': '50.0%','lag_months': 3},
    }

    # Headers matching existing Tab 1 format + Next Expected Update
    headers = [
        'Driver Name', 'Category', 'Unit', 'Current Value', 'Trend',
        'Impact Direction', 'Weight', 'Source', 'Frequency',
        'Description', 'Bull Scenario', 'Base Scenario', 'Bear Scenario',
        'Last Updated', 'Next Expected Update'
    ]
    rows = [headers]

    for d in drivers:
        name = d['driver_name'] or ''
        meta = MACRO_DRIVER_METADATA.get(name, {})

        # Compute next expected update from last_updated + frequency + lag
        next_update_str = ''
        if d['last_updated'] and meta:
            try:
                last_dt = d['last_updated'] if isinstance(d['last_updated'], datetime) else datetime.strptime(str(d['last_updated'])[:10], '%Y-%m-%d')
                freq = meta.get('frequency', '')
                lag = meta.get('lag_months', 0)

                if freq == 'Daily':
                    # Daily data: next business day
                    next_dt = last_dt + timedelta(days=1)
                elif freq == 'Monthly':
                    # Next month's data arrives after lag_months
                    next_dt = last_dt + relativedelta(months=1 + lag)
                elif freq == 'Quarterly':
                    # Next quarter's data arrives after lag_months
                    next_dt = last_dt + relativedelta(months=3 + lag)
                elif freq == 'As needed':
                    next_dt = None  # Event-driven (RBI policy)
                else:
                    next_dt = None

                if next_dt:
                    next_update_str = next_dt.strftime('%Y-%m-%d')
                else:
                    next_update_str = 'Event-driven'
            except Exception:
                next_update_str = ''

        rows.append([
            name,
            meta.get('category', ''),
            meta.get('unit', ''),
            d['current_value'] or '',
            d['trend'] or 'STABLE',
            d['impact_direction'] or 'NEUTRAL',
            float(meta.get('weight', d['weight'] or 0)),
            meta.get('source', ''),
            meta.get('frequency', ''),
            meta.get('description', ''),
            meta.get('bull', ''),
            meta.get('base', ''),
            meta.get('bear', ''),
            str(d['last_updated']) if d['last_updated'] else '',
            next_update_str
        ])

    # Clear and update
    ws.clear()
    ws.update(range_name='A1', values=rows)
    print(f"  Synced {len(rows)-1} MACRO drivers to Tab 1")
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


def sync_company_drivers(gc, sheet_id):
    """Sync COMPANY drivers to Tab 4 '4. Company Drivers'.
    Handles large row counts (~29K) by writing in batches of 100 with 1s pause."""
    import time

    sh = gc.open_by_key(sheet_id)

    # Create tab if it doesn't exist
    try:
        ws = sh.worksheet("4. Company Drivers")
    except gspread.exceptions.WorksheetNotFound:
        ws = sh.add_worksheet("4. Company Drivers", rows=35000, cols=16)
        print("  Created new tab '4. Company Drivers'")

    # Get COMPANY drivers from MySQL with company name lookup
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
            d.company_id,
            COALESCE(a.nse_symbol, m.symbol, '') AS nse_symbol,
            COALESCE(a.company_name, m.name, '') AS company_name,
            d.valuation_group,
            d.valuation_subgroup,
            d.driver_category,
            d.driver_name,
            d.current_value,
            d.impact_direction,
            d.trend,
            d.weight,
            d.source,
            d.is_active,
            d.last_updated
        FROM vs_drivers d
        LEFT JOIN vs_active_companies a ON d.company_id = a.company_id
        LEFT JOIN mssdb.kbapp_marketscrip m ON d.company_id = m.marketscrip_id
        WHERE d.driver_level = 'COMPANY'
        ORDER BY d.valuation_subgroup, a.company_name, d.driver_category, d.driver_name
    """)

    drivers = cursor.fetchall()
    cursor.close()
    conn.close()

    print(f"  Found {len(drivers)} COMPANY drivers in MySQL")

    if not drivers:
        print("  No company drivers to sync")
        return 0

    headers = [
        'Company ID', 'NSE Symbol', 'Company Name', 'Valuation Group',
        'Valuation Subgroup', 'Category', 'Driver Name', 'Current Value',
        'Direction', 'Trend', 'Weight', 'Source', 'Is Active', 'Last Updated'
    ]

    # Build all rows
    data_rows = []
    for d in drivers:
        data_rows.append([
            d['company_id'] or '',
            d['nse_symbol'] or '',
            d['company_name'] or '',
            d['valuation_group'] or '',
            d['valuation_subgroup'] or '',
            d['driver_category'] or '',
            d['driver_name'] or '',
            d['current_value'] or '',
            d['impact_direction'] or 'NEUTRAL',
            d['trend'] or 'STABLE',
            float(d['weight']) if d['weight'] else 0.0,
            d.get('source', 'SEED') or 'SEED',
            'TRUE' if d.get('is_active', 1) else 'FALSE',
            str(d['last_updated']) if d['last_updated'] else ''
        ])

    # Ensure sheet is large enough
    total_rows_needed = len(data_rows) + 2  # header + data + buffer
    if total_rows_needed > ws.row_count:
        ws.resize(rows=total_rows_needed + 500, cols=len(headers))
        print(f"  Expanded sheet to {total_rows_needed + 500} rows")

    # Clear and write header
    ws.clear()
    ws.update(range_name='A1', values=[headers])

    # Write data in batches of 100
    BATCH_SIZE = 100
    for batch_start in range(0, len(data_rows), BATCH_SIZE):
        batch_end = min(batch_start + BATCH_SIZE, len(data_rows))
        batch = data_rows[batch_start:batch_end]
        target_row = batch_start + 2  # +2 for 1-indexed + header row
        ws.update(range_name=f'A{target_row}', values=batch)

        if batch_end < len(data_rows):
            if batch_start % 1000 == 0:
                print(f"  Written {batch_end}/{len(data_rows)} rows...")
            time.sleep(1)  # Rate limit courtesy pause

    print(f"  Synced {len(data_rows)} COMPANY drivers to Tab 4")
    return len(data_rows)


def sync_discovered_drivers(gc, sheet_id):
    """Sync discovered drivers (agent suggestions) to Tab 7 '7. Discovered Drivers'."""
    sh = gc.open_by_key(sheet_id)

    # Create tab if it doesn't exist
    try:
        ws = sh.worksheet("7. Discovered Drivers")
    except gspread.exceptions.WorksheetNotFound:
        ws = sh.add_worksheet("7. Discovered Drivers", rows=500, cols=15)
        print("  Created new tab '7. Discovered Drivers'")

    # Get discovered drivers from MySQL
    conn = mysql.connector.connect(
        host=os.getenv('MYSQL_HOST', 'localhost'),
        port=int(os.getenv('MYSQL_PORT', 3306)),
        user=os.getenv('MYSQL_USER', 'root'),
        password=os.getenv('MYSQL_PASSWORD', ''),
        database=os.getenv('MYSQL_DATABASE', 'rag')
    )
    cursor = conn.cursor(dictionary=True)

    cursor.execute("""
        SELECT id, driver_level, valuation_group, valuation_subgroup,
               driver_category, driver_name, suggested_weight, reasoning,
               source_headline, confidence, status, discovered_at, reviewed_by
        FROM vs_discovered_drivers
        ORDER BY status ASC, discovered_at DESC
    """)

    discoveries = cursor.fetchall()
    cursor.close()
    conn.close()

    print(f"  Found {len(discoveries)} discovered drivers in MySQL")

    headers = [
        'ID', 'Level', 'Valuation Group', 'Valuation Subgroup', 'Category',
        'Driver Name', 'Weight', 'Reasoning', 'Source Headline',
        'Confidence', 'Status', 'Discovered At', 'Reviewed By'
    ]
    rows = [headers]

    for d in discoveries:
        rows.append([
            d['id'],
            d['driver_level'] or '',
            d['valuation_group'] or '',
            d['valuation_subgroup'] or '',
            d['driver_category'] or '',
            d['driver_name'] or '',
            float(d['suggested_weight']) if d['suggested_weight'] else 0.0,
            d['reasoning'] or '',
            d['source_headline'] or '',
            d['confidence'] or 'MEDIUM',
            d['status'] or 'PENDING',
            str(d['discovered_at']) if d['discovered_at'] else '',
            d['reviewed_by'] or ''
        ])

    ws.clear()
    ws.update(range_name='A1', values=rows)
    print(f"  Synced {len(rows)-1} discovered drivers to Tab 7")
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

    # Sync MACRO drivers
    print("\n[2] Syncing MACRO drivers to Tab 1...")
    macro_count = sync_macro_drivers(gc, sheet_id)

    # Sync GROUP drivers
    print("\n[3] Syncing GROUP drivers to Tab 2...")
    group_count = sync_group_drivers(gc, sheet_id)

    # Sync SUBGROUP drivers
    print("\n[4] Syncing SUBGROUP drivers to Tab 3...")
    subgroup_count = sync_subgroup_drivers(gc, sheet_id)

    # Sync company drivers
    print("\n[5] Syncing COMPANY drivers to Tab 4...")
    company_driver_count = sync_company_drivers(gc, sheet_id)

    # Sync active companies
    print("\n[6] Syncing active companies to Tab 6...")
    company_count = sync_active_companies(gc, sheet_id)

    # Sync discovered drivers
    print("\n[7] Syncing discovered drivers to Tab 7...")
    discovered_count = sync_discovered_drivers(gc, sheet_id)

    print("\n" + "=" * 70)
    print("SUMMARY")
    print("=" * 70)
    print(f"MACRO drivers synced: {macro_count}")
    print(f"GROUP drivers synced: {group_count}")
    print(f"SUBGROUP drivers synced: {subgroup_count}")
    print(f"COMPANY drivers synced: {company_driver_count}")
    print(f"Active companies synced: {company_count}")
    print(f"Discovered drivers synced: {discovered_count}")
    print(f"\nGSheet URL: https://docs.google.com/spreadsheets/d/{sheet_id}")

    return macro_count, group_count, subgroup_count, company_driver_count, company_count, discovered_count


if __name__ == '__main__':
    main()
