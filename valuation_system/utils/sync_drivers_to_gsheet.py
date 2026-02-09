#!/usr/bin/env python3
"""
Sync Driver Definitions to Google Sheet
- Syncs GROUP drivers to Tab 2 "Valuation Group Drivers"
- Syncs SUBGROUP drivers to Tab 3 "Valuation Subgroup Drivers"
"""

import os
import sys
import json
import time
import logging
import mysql.connector
import gspread
import pandas as pd
from google.oauth2.service_account import Credentials
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta

logger = logging.getLogger(__name__)

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))

from collections import defaultdict
from dotenv import load_dotenv
load_dotenv('/Users/ram/code/research/valuation_system/config/.env')


def _compute_weight_sums(drivers, group_key):
    """Compute sum of weights per group for normalization to 100%."""
    sums = defaultdict(float)
    for d in drivers:
        key = d.get(group_key, '') or ''
        sums[key] += float(d.get('weight') or 0)
    return sums


def _fmt_weight_pct(raw_weight, group_sum):
    """Normalize a weight to percentage string within its group."""
    if group_sum <= 0:
        return '0.0%'
    pct = (float(raw_weight or 0) / group_sum) * 100
    return f'{pct:.1f}%'


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

    # Normalize weights per valuation_group to sum to 100%
    group_sums = _compute_weight_sums(drivers, 'valuation_group')

    # Prepare data with headers
    headers = ['Valuation Group', 'Category', 'Driver Name', 'Weight', 'Impact', 'Trend',
               'Current Value', 'Is Active', 'Source', 'Linked Macro', 'Link Direction', 'Last Updated']
    rows = [headers]

    for d in drivers:
        grp = d['valuation_group'] or ''
        rows.append([
            grp,
            d['driver_category'] or '',
            d['driver_name'] or '',
            _fmt_weight_pct(d['weight'], group_sums.get(grp, 0)),
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
    print(f"  Synced {len(rows)-1} GROUP drivers to Tab 2 ({len(group_sums)} groups, weights normalized to 100%)")
    return len(rows) - 1


def sync_subgroup_drivers(gc, sheet_id):
    """Sync SUBGROUP drivers to Tab 3"""
    sh = gc.open_by_key(sheet_id)
    ws = sh.worksheet("3. Valuation Subgroup Drivers")

    # Get drivers from MySQL
    drivers = get_drivers_from_mysql('SUBGROUP')
    print(f"  Found {len(drivers)} SUBGROUP drivers in MySQL")

    # Normalize weights per valuation_subgroup to sum to 100%
    subgroup_sums = _compute_weight_sums(drivers, 'valuation_subgroup')

    # Prepare data with headers
    headers = ['Valuation Group', 'Valuation Subgroup', 'Category', 'Driver Name', 'Weight', 'Impact', 'Trend',
               'Current Value', 'Is Active', 'Source', 'Linked Macro', 'Link Direction', 'Last Updated']
    rows = [headers]

    for d in drivers:
        sg = d['valuation_subgroup'] or ''
        rows.append([
            d['valuation_group'] or '',
            sg,
            d['driver_category'] or '',
            d['driver_name'] or '',
            _fmt_weight_pct(d['weight'], subgroup_sums.get(sg, 0)),
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
    print(f"  Synced {len(rows)-1} SUBGROUP drivers to Tab 3 ({len(subgroup_sums)} subgroups, weights normalized to 100%)")
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

    # Compute total macro weight for normalization to 100%
    macro_total_weight = sum(
        float(MACRO_DRIVER_METADATA.get(d['driver_name'] or '', {}).get('weight', d['weight'] or 0))
        for d in drivers
    )

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
            _fmt_weight_pct(meta.get('weight', d['weight'] or 0), macro_total_weight),
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


def _load_mcap_company_ids(min_mcap_cr):
    """Load set of company_ids with latest MCap >= min_mcap_cr from monthly prices CSV.
    Maps accode/bse_code back to company_id via vs_active_companies."""
    prices_path = os.getenv('MONTHLY_PRICES_PATH', '')
    if not prices_path or not os.path.exists(prices_path):
        logger.warning("MONTHLY_PRICES_PATH not found, skipping MCap filter")
        return None

    # Load prices, get latest MCap per accode
    prices_df = pd.read_csv(prices_path, usecols=['accode', 'bse_code', 'mcap', 'year_month'],
                            dtype={'accode': str, 'bse_code': str}, low_memory=False)
    prices_df = prices_df.dropna(subset=['mcap'])
    prices_df = prices_df.sort_values('year_month', ascending=False)

    # Get latest MCap per accode (prefer NSE)
    # Strip '.0' suffix from CSV float-strings to match DB integer-strings
    def _clean_code(val):
        s = str(val).strip()
        if s.endswith('.0'):
            s = s[:-2]
        return s if s and s != 'nan' else ''

    latest_mcap = {}
    for _, row in prices_df.iterrows():
        ac = _clean_code(row.get('accode', ''))
        bse = _clean_code(row.get('bse_code', ''))
        mcap = float(row['mcap'])
        key = ac if ac else bse
        if key and key not in latest_mcap:
            latest_mcap[key] = mcap

    # Map to company_ids via vs_active_companies
    conn = mysql.connector.connect(
        host=os.getenv('MYSQL_HOST', 'localhost'),
        port=int(os.getenv('MYSQL_PORT', 3306)),
        user=os.getenv('MYSQL_USER', 'root'),
        password=os.getenv('MYSQL_PASSWORD', ''),
        database=os.getenv('MYSQL_DATABASE', 'rag')
    )
    cursor = conn.cursor(dictionary=True)
    cursor.execute("SELECT company_id, accord_code, bse_code FROM vs_active_companies")
    companies = cursor.fetchall()
    cursor.close()
    conn.close()

    qualifying_ids = set()
    for comp in companies:
        cid = comp['company_id']
        ac = str(comp.get('accord_code') or '').strip()
        bse = str(comp.get('bse_code') or '').strip()
        mcap = latest_mcap.get(ac) or latest_mcap.get(bse) or 0
        if mcap >= min_mcap_cr:
            qualifying_ids.add(cid)

    return qualifying_ids


def sync_company_drivers(gc, sheet_id):
    """Sync COMPANY drivers to Tab 4 '4. Company Drivers'.
    Only syncs companies with MCap >= MIN_GSHEET_MCAP_CR (default 1000 Cr).
    Handles large row counts by writing in batches of 100 with 1s pause."""

    min_mcap = float(os.getenv('MIN_GSHEET_MCAP_CR', 1000))
    print(f"  Filtering to companies with MCap >= {min_mcap:.0f} Cr...")
    mcap_company_ids = _load_mcap_company_ids(min_mcap)
    if mcap_company_ids is not None:
        print(f"  Found {len(mcap_company_ids)} companies with MCap >= {min_mcap:.0f} Cr")
    else:
        print("  WARNING: Could not load MCap data, syncing all companies")

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

    all_drivers = cursor.fetchall()
    cursor.close()
    conn.close()

    # Filter by MCap if available
    if mcap_company_ids is not None:
        drivers = [d for d in all_drivers if d['company_id'] in mcap_company_ids]
        print(f"  Filtered: {len(drivers)} COMPANY drivers (from {len(all_drivers)} total) for {len(mcap_company_ids)} companies with MCap >= {min_mcap:.0f} Cr")
    else:
        drivers = all_drivers
        print(f"  Found {len(drivers)} COMPANY drivers in MySQL (no MCap filter)")

    if not drivers:
        print("  No company drivers to sync")
        return 0

    # Normalize weights per company_id to sum to 100%
    company_sums = _compute_weight_sums(drivers, 'company_id')

    headers = [
        'Company ID', 'NSE Symbol', 'Company Name', 'Valuation Group',
        'Valuation Subgroup', 'Category', 'Driver Name', 'Current Value',
        'Direction', 'Trend', 'Weight', 'Source', 'Is Active', 'Last Updated'
    ]

    # Build all rows
    data_rows = []
    for d in drivers:
        cid = d['company_id'] or ''
        data_rows.append([
            cid,
            d['nse_symbol'] or '',
            d['company_name'] or '',
            d['valuation_group'] or '',
            d['valuation_subgroup'] or '',
            d['driver_category'] or '',
            d['driver_name'] or '',
            d['current_value'] or '',
            d['impact_direction'] or 'NEUTRAL',
            d['trend'] or 'STABLE',
            _fmt_weight_pct(d['weight'], company_sums.get(cid, 0)),
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
    """Sync discovered drivers (agent suggestions) to Tab 7 '7. Discovered Drivers'.
    PM edits Status column to APPROVED/REJECTED to approve/reject suggestions."""
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
               source_headline, confidence, status, discovered_at, reviewed_by, pm_notes
        FROM vs_discovered_drivers
        ORDER BY status ASC, discovered_at DESC
        LIMIT 500
    """)

    discoveries = cursor.fetchall()
    cursor.close()
    conn.close()

    print(f"  Found {len(discoveries)} discovered drivers in MySQL")

    headers = [
        'ID', 'Discovered At', 'Level', 'Valuation Group', 'Valuation Subgroup', 'Category',
        'Driver Name', 'Weight', 'Reasoning', 'Source Headline',
        'Confidence', 'Status', 'Reviewed By', 'PM Notes'
    ]
    rows = [headers]

    for d in discoveries:
        rows.append([
            d['id'],
            str(d['discovered_at']) if d['discovered_at'] else '',
            d['driver_level'] or '',
            d['valuation_group'] or '',
            d['valuation_subgroup'] or '',
            d['driver_category'] or '',
            d['driver_name'] or '',
            f"{float(d['suggested_weight'])*100:.1f}%" if d['suggested_weight'] else '0.0%',
            d['reasoning'] or '',
            d['source_headline'] or '',
            d['confidence'] or 'MEDIUM',
            d['status'] or 'PENDING',
            d['reviewed_by'] or '',
            d.get('pm_notes', '') or ''
        ])

    ws.clear()
    ws.update(range_name='A1', values=rows)

    # Add data validation for Status column (column L, row 2 onwards)
    try:
        from gspread_formatting import DataValidationRule, BooleanCondition, set_data_validation_for_cell_range
        validation_rule = DataValidationRule(
            BooleanCondition('ONE_OF_LIST', ['PENDING', 'APPROVED', 'REJECTED']),
            showCustomUi=True,
            strict=True
        )
        set_data_validation_for_cell_range(ws, 'L2:L500', validation_rule)
    except ImportError:
        print("  Warning: gspread_formatting not installed, skipping data validation")
    except Exception as e:
        print(f"  Warning: Could not add data validation to Status column: {e}")

    print(f"  Synced {len(rows)-1} discovered drivers to Tab 7")
    return len(rows) - 1


def _format_drivers_affected(drivers_json):
    """Format drivers_affected JSON for GSheet display.
    Old format: ["revenue_growth", "stock_price"] → "revenue_growth, stock_price"
    New format: [{"driver":"revenue_growth","level":"GROUP","impact_pct":-3}]
                → "revenue_growth(GROUP:-3.0%)"
    """
    if not drivers_json:
        return ''
    if isinstance(drivers_json, str):
        try:
            drivers_json = json.loads(drivers_json)
        except json.JSONDecodeError:
            return drivers_json  # Return as-is if not valid JSON

    parts = []
    for item in drivers_json:
        if isinstance(item, str):
            parts.append(item)
        elif isinstance(item, dict):
            name = item.get('driver', '?')
            level = item.get('level', '')
            pct = item.get('impact_pct')
            if level and pct is not None:
                parts.append(f"{name}({level}:{pct:+.1f}%)")
            elif level:
                parts.append(f"{name}({level})")
            else:
                parts.append(name)
    return ', '.join(parts)


def sync_news_events(gc, sheet_id):
    """Sync recent news events to Tab 8 '8. News Events' with expanded columns.
    Shows vs_event_timeline data from last 7 days for PM review.
    17 columns: Scraped At, Published At, Severity, Scope, Valuation Subgroup,
    Headline, Summary, Drivers Affected, Valuation Impact %, Source, URL,
    Company, Related, LLM, Tokens, PM Reviewed, PM Notes"""
    sh = gc.open_by_key(sheet_id)

    # Create tab if it doesn't exist
    try:
        ws = sh.worksheet("8. News Events")
    except gspread.exceptions.WorksheetNotFound:
        ws = sh.add_worksheet("8. News Events", rows=500, cols=17)
        print("  Created new tab '8. News Events'")

    # Get news events from MySQL (last 7 days) - only primary events (no dupes)
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
            e.event_timestamp,
            e.published_at,
            e.severity,
            e.scope,
            COALESCE(ac.valuation_subgroup, '') as valuation_subgroup,
            e.headline,
            COALESCE(e.grok_synopsis, e.summary, '') as summary,
            e.drivers_affected,
            e.valuation_impact_pct,
            e.source,
            e.source_url,
            COALESCE(ac.nse_symbol, '') as company_symbol,
            e.semantic_group_id,
            e.id,
            e.llm_model,
            e.llm_tokens,
            e.pm_reviewed,
            e.pm_notes,
            (SELECT COUNT(*) FROM vs_event_timeline e2
             WHERE e2.semantic_group_id = e.id AND e2.id != e.id) as related_count
        FROM vs_event_timeline e
        LEFT JOIN vs_active_companies ac ON e.company_id = ac.company_id
        WHERE e.event_date >= DATE_SUB(CURDATE(), INTERVAL 7 DAY)
          AND e.semantic_group_id = e.id
        ORDER BY
            FIELD(e.severity, 'CRITICAL', 'HIGH', 'MEDIUM', 'LOW'),
            e.event_timestamp DESC
        LIMIT 200
    """)

    events = cursor.fetchall()
    cursor.close()
    conn.close()

    print(f"  Found {len(events)} news events (last 7 days, primary events only)")

    headers = [
        'Scraped At', 'Published At', 'Severity', 'Scope', 'Valuation Subgroup',
        'Headline', 'Summary', 'Drivers Affected', 'Valuation Impact %',
        'Source', 'URL', 'Company', 'Related', 'LLM', 'Tokens',
        'PM Reviewed', 'PM Notes'
    ]
    rows = [headers]

    for ev in events:
        # Format timestamps with HH:MM:SS
        scraped = ev.get('event_timestamp')
        scraped_str = scraped.strftime('%Y-%m-%d %H:%M:%S') if scraped else ''

        published = ev.get('published_at')
        published_str = published.strftime('%Y-%m-%d %H:%M:%S') if published else ''

        # Format drivers_affected
        drivers_str = _format_drivers_affected(ev.get('drivers_affected'))

        # Valuation impact
        impact = ev.get('valuation_impact_pct')
        impact_str = f"{impact:.1f}%" if impact is not None else ''

        # Related count
        related_count = ev.get('related_count', 0)
        related_str = f"(+{related_count})" if related_count > 0 else ''

        # LLM tokens formatted with commas
        tokens = ev.get('llm_tokens')
        tokens_str = f"{tokens:,}" if tokens else ''

        rows.append([
            scraped_str,
            published_str,
            ev.get('severity', ''),
            ev.get('scope', ''),
            ev.get('valuation_subgroup', ''),
            ev.get('headline', ''),
            (ev.get('summary', '') or '')[:300],  # Truncate long summaries
            drivers_str,
            impact_str,
            ev.get('source', ''),
            ev.get('source_url', ''),
            ev.get('company_symbol', ''),
            related_str,
            ev.get('llm_model', ''),
            tokens_str,
            'YES' if ev.get('pm_reviewed') else '',
            ev.get('pm_notes', '') or '',
        ])

    ws.clear()

    # Batch write in chunks to avoid rate limits
    chunk_size = 100
    for i in range(0, len(rows), chunk_size):
        chunk = rows[i:i + chunk_size]
        start_row = i + 1
        ws.update(range_name=f'A{start_row}', values=chunk)
        if i + chunk_size < len(rows):
            time.sleep(1)

    # Apply conditional formatting for severity (updated range to Q for 17 cols)
    try:
        from gspread_formatting import ConditionalFormatRule, BooleanRule, BooleanCondition, CellFormat, Color, GridRange
        from gspread_formatting import set_conditional_format_rules

        rules = []
        severity_colors = {
            'CRITICAL': Color(1.0, 0.8, 0.8),
            'HIGH':     Color(1.0, 0.9, 0.7),
            'MEDIUM':   Color(1.0, 1.0, 0.8),
        }

        for severity, color in severity_colors.items():
            rules.append(ConditionalFormatRule(
                ranges=[GridRange.from_a1_range('A2:Q500', ws)],
                booleanRule=BooleanRule(
                    condition=BooleanCondition('CUSTOM_FORMULA', [f'=$C2="{severity}"']),
                    format=CellFormat(backgroundColor=color)
                )
            ))

        set_conditional_format_rules(ws, rules)
    except ImportError:
        print("  Warning: gspread_formatting not installed, skipping color coding")
    except Exception as e:
        print(f"  Warning: Could not apply color coding: {e}")

    print(f"  Synced {len(rows)-1} news events to Tab 8")
    return len(rows) - 1


def sync_materiality_dashboard(gc, sheet_id):
    """Sync materiality alerts to Tab 9 '9. Materiality Dashboard'.
    Read-only dashboard showing critical alerts from last 7 days, color-coded by severity."""
    sh = gc.open_by_key(sheet_id)

    # Create tab if it doesn't exist
    try:
        ws = sh.worksheet("9. Materiality Dashboard")
    except gspread.exceptions.WorksheetNotFound:
        ws = sh.add_worksheet("9. Materiality Dashboard", rows=1000, cols=13)
        print("  Created new tab '9. Materiality Dashboard'")

    # Get materiality alerts from MySQL (last 7 days)
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
            ma.alert_type,
            ma.severity,
            COALESCE(ac.nse_symbol, ma.valuation_group, ma.valuation_subgroup, 'MACRO') as scope_name,
            ma.driver_affected,
            ma.current_value,
            ma.baseline_value,
            ma.deviation_pct,
            ma.suggested_action,
            COALESCE(ma.reasoning, ma.signal_description) as reasoning,
            ma.created_at,
            ma.pm_reviewed,
            ma.pm_notes
        FROM vs_materiality_alerts ma
        LEFT JOIN vs_active_companies ac ON ma.company_id = ac.company_id
        WHERE ma.created_at >= DATE_SUB(NOW(), INTERVAL 7 DAY)
        ORDER BY
            FIELD(ma.severity, 'CRITICAL', 'HIGH', 'MEDIUM', 'LOW'),
            ma.created_at DESC
    """)

    alerts = cursor.fetchall()
    cursor.close()
    conn.close()

    print(f"  Found {len(alerts)} materiality alerts (last 7 days)")

    headers = [
        'Alert Type', 'Severity', 'Company/Sector', 'Driver Affected',
        'Current Value', 'Baseline', 'Change %', 'Suggested Action',
        'Reasoning', 'Created At', 'PM Reviewed', 'PM Notes', 'Review Link'
    ]
    rows = [headers]

    for a in alerts:
        change_pct = a.get('deviation_pct')
        change_str = f"{change_pct:.1f}%" if change_pct is not None else 'N/A'

        rows.append([
            a['alert_type'] or '',
            a['severity'] or 'MEDIUM',
            a['scope_name'] or '',
            a['driver_affected'] or '',
            str(a['current_value']) if a['current_value'] is not None else '',
            str(a['baseline_value']) if a['baseline_value'] is not None else '',
            change_str,
            a['suggested_action'] or '',
            a['reasoning'] or '',
            str(a['created_at']) if a['created_at'] else '',
            'YES' if a.get('pm_reviewed') else 'NO',
            a.get('pm_notes', '') or '',
            ''  # Review Link (can be populated with GSheet formula later)
        ])

    ws.clear()
    ws.update(range_name='A1', values=rows)

    # Apply conditional formatting rules (4 rules, 1 API call batch — not per-row)
    try:
        from gspread_formatting import ConditionalFormatRule, BooleanRule, BooleanCondition, CellFormat, Color, GridRange

        ws_id = ws.id
        rules = []
        severity_colors = {
            'CRITICAL': Color(1.0, 0.8, 0.8),   # Light red
            'HIGH':     Color(1.0, 0.9, 0.7),    # Light orange
            'MEDIUM':   Color(1.0, 1.0, 0.8),    # Light yellow
            'LOW':      Color(0.9, 1.0, 0.9),    # Light green
        }

        for severity, color in severity_colors.items():
            rules.append(ConditionalFormatRule(
                ranges=[GridRange.from_a1_range(f'A2:M1000', ws)],
                booleanRule=BooleanRule(
                    condition=BooleanCondition('CUSTOM_FORMULA', [f'=$B2="{severity}"']),
                    format=CellFormat(backgroundColor=color)
                )
            ))

        from gspread_formatting import set_conditional_format_rules
        set_conditional_format_rules(ws, rules)
    except ImportError:
        # gspread_formatting not installed, skip color coding
        print("  Warning: gspread_formatting not installed, skipping color coding")
    except Exception as e:
        print(f"  Warning: Could not apply color coding: {e}")

    print(f"  Synced {len(rows)-1} materiality alerts to Tab 9")
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

    # Sync news events
    print("\n[8] Syncing news events to Tab 8...")
    news_count = sync_news_events(gc, sheet_id)

    # Sync materiality dashboard
    print("\n[9] Syncing materiality dashboard to Tab 9...")
    materiality_count = sync_materiality_dashboard(gc, sheet_id)

    print("\n" + "=" * 70)
    print("SUMMARY")
    print("=" * 70)
    print(f"MACRO drivers synced: {macro_count}")
    print(f"GROUP drivers synced: {group_count}")
    print(f"SUBGROUP drivers synced: {subgroup_count}")
    print(f"COMPANY drivers synced: {company_driver_count}")
    print(f"Active companies synced: {company_count}")
    print(f"Discovered drivers synced: {discovered_count}")
    print(f"Materiality alerts synced: {materiality_count}")
    print(f"\nGSheet URL: https://docs.google.com/spreadsheets/d/{sheet_id}")

    return macro_count, group_count, subgroup_count, company_driver_count, company_count, discovered_count, materiality_count


if __name__ == '__main__':
    main()
