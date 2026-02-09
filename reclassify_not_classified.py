#!/usr/bin/env python3
"""
Reclassify NOT_CLASSIFIED companies in the valuation taxonomy.

Updates:
  - Excel (source of truth): valuation_group-valuation_subgroup-feb2026.xlsx
  - MySQL: vs_active_companies (new investable companies),
           vs_valuation_group_configs (NON_OPERATING),
           vs_valuation_subgroup_configs (15 new subgroups)

Generates:
  - Changes CSV log at valuation_system/logs/reclassify_changes_YYYYMMDD_HHMM.csv
  - Detailed log at reclassify_not_classified.log

Usage:
  python3 reclassify_not_classified.py --dry-run    # Preview changes only
  python3 reclassify_not_classified.py               # Apply changes
"""

import os
import sys
import csv
import shutil
import argparse
import traceback
from datetime import datetime

import pandas as pd
import mysql.connector

# === PATHS ===
EXCEL_PATH = "/Users/ram/code/research/valuation_group-valuation_subgroup-feb2026.xlsx"
LOG_PATH = "/Users/ram/code/research/reclassify_not_classified.log"
CHANGES_CSV_DIR = "/Users/ram/code/research/valuation_system/logs"

BATCH_SIZE = 100
INVESTABLE_MIN_SALES_CR = 100  # Companies with sales > 100 Cr become investable

# === CD_Industry1 → (valuation_group, valuation_subgroup) MAPPING ===
# NOTE: Exact string matching — whitespace quirks (double spaces) are intentional
CD_INDUSTRY_TO_TAXONOMY = {
    # --- NON_OPERATING ---
    'ETF': ('NON_OPERATING', 'NON_OPERATING_ETF'),
    'Other': ('NON_OPERATING', 'NON_OPERATING_SHELL'),
    'Index': ('NON_OPERATING', 'NON_OPERATING_INDEX'),
    'Cash and Cash Equivalents': ('NON_OPERATING', 'NON_OPERATING_CASH'),

    # --- MAP TO EXISTING SUBGROUPS ---
    'Textile - Spinning': ('CONSUMER_DISCRETIONARY', 'CONSUMER_TEXTILE'),
    'Textile - Weaving': ('CONSUMER_DISCRETIONARY', 'CONSUMER_TEXTILE'),
    'Textile - Manmade  Fibres': ('CONSUMER_DISCRETIONARY', 'CONSUMER_TEXTILE'),
    'Rubber  Products': ('AUTO', 'AUTO_ANCILLARY_TIRES'),
    'Tea/Coffee': ('CONSUMER_STAPLES', 'CONSUMER_FOOD_BEVERAGE'),
    'Solvent  Extraction': ('CONSUMER_STAPLES', 'CONSUMER_AGRI'),
    'Film Production, Distribution & Entertainment': ('SERVICES', 'SERVICES_MEDIA_BROADCASTING'),
    'Advertising & Media': ('SERVICES', 'SERVICES_MEDIA_BROADCASTING'),
    'Aquaculture': ('CONSUMER_STAPLES', 'CONSUMER_AGRI'),
    'Electronics - Components': ('INDUSTRIALS', 'INDUSTRIALS_ELECTRICALS'),
    'Ferro & Silica Manganese': ('MATERIALS_METALS', 'METALS_STEEL'),
    'Telecom-Infrastructure': ('SERVICES', 'SERVICES_TELECOM_TOWERS'),
    'Textile - Machinery': ('INDUSTRIALS', 'INDUSTRIALS_CAPITAL_GOODS'),
    'Courier  Services': ('REAL_ESTATE_INFRA', 'INFRA_LOGISTICS_PORTS'),
    'Abrasives': ('INDUSTRIALS', 'INDUSTRIALS_CAPITAL_GOODS'),
    'Floriculture': ('CONSUMER_STAPLES', 'CONSUMER_AGRI'),
    'Photographic Products': ('INDUSTRIALS', 'INDUSTRIALS_CAPITAL_GOODS'),

    # --- NEW SUBGROUPS ---
    'Trading': ('SERVICES', 'SERVICES_TRADING'),
    'Plastic Products': ('MATERIALS_CHEMICALS', 'MATERIALS_PLASTICS'),
    'Logistics': ('REAL_ESTATE_INFRA', 'INFRA_LOGISTICS'),
    'Diamond  &  Jewellery': ('CONSUMER_DISCRETIONARY', 'CONSUMER_JEWELLERY'),
    'Business Support': ('SERVICES', 'SERVICES_BPO'),
    'Paper & Paper Products': ('MATERIALS_CHEMICALS', 'MATERIALS_PAPER'),
    'Professional Services': ('SERVICES', 'SERVICES_PROFESSIONAL'),
    'Educational Institutions': ('SERVICES', 'SERVICES_EDUCATION'),
    'Breweries & Distilleries': ('CONSUMER_STAPLES', 'CONSUMER_ALCOHOLIC_BEVERAGES'),
    'Shipping': ('REAL_ESTATE_INFRA', 'INFRA_SHIPPING'),
    'e-Commerce': ('CONSUMER_DISCRETIONARY', 'CONSUMER_RETAIL_ONLINE'),
    'Industrial  Gases & Fuels': ('ENERGY_UTILITIES', 'ENERGY_INDUSTRIAL_GAS'),
    'Mining & Minerals': ('MATERIALS_METALS', 'MATERIALS_MINING'),
    'Environmental Services': ('INDUSTRIALS', 'INDUSTRIALS_ENVIRONMENTAL'),
    'Airlines': ('SERVICES', 'SERVICES_AVIATION'),
}

# New subgroup configs: subgroup → (parent_group, display_name)
NEW_SUBGROUP_CONFIGS = {
    'SERVICES_TRADING':             ('SERVICES',              'Trading & Distribution'),
    'MATERIALS_PLASTICS':           ('MATERIALS_CHEMICALS',   'Plastics & Packaging'),
    'INFRA_LOGISTICS':              ('REAL_ESTATE_INFRA',     'Logistics'),
    'CONSUMER_JEWELLERY':           ('CONSUMER_DISCRETIONARY','Diamond & Jewellery'),
    'SERVICES_BPO':                 ('SERVICES',              'Business Process Services'),
    'MATERIALS_PAPER':              ('MATERIALS_CHEMICALS',   'Paper & Forest Products'),
    'SERVICES_PROFESSIONAL':        ('SERVICES',              'Professional Services'),
    'SERVICES_EDUCATION':           ('SERVICES',              'Education'),
    'CONSUMER_ALCOHOLIC_BEVERAGES': ('CONSUMER_STAPLES',      'Alcoholic Beverages'),
    'INFRA_SHIPPING':               ('REAL_ESTATE_INFRA',     'Shipping & Marine'),
    'CONSUMER_RETAIL_ONLINE':       ('CONSUMER_DISCRETIONARY','e-Commerce & Online Retail'),
    'ENERGY_INDUSTRIAL_GAS':        ('ENERGY_UTILITIES',      'Industrial Gases & Fuels'),
    'MATERIALS_MINING':             ('MATERIALS_METALS',      'Mining & Minerals'),
    'INDUSTRIALS_ENVIRONMENTAL':    ('INDUSTRIALS',           'Environmental Services'),
    'SERVICES_AVIATION':            ('SERVICES',              'Airlines & Aviation'),
}

# Damodaran India mappings for new subgroups
NEW_SUBGROUP_TO_DAMODARAN = {
    'SERVICES_TRADING': 'Retail (Distributors)',
    'MATERIALS_PLASTICS': 'Packaging & Container',
    'INFRA_LOGISTICS': 'Transportation',
    'CONSUMER_JEWELLERY': 'Retail (Special Lines)',
    'SERVICES_BPO': 'Business & Consumer Services',
    'MATERIALS_PAPER': 'Paper/Forest Products',
    'SERVICES_PROFESSIONAL': 'Business & Consumer Services',
    'SERVICES_EDUCATION': 'Education',
    'CONSUMER_ALCOHOLIC_BEVERAGES': 'Beverage (Alcoholic)',
    'INFRA_SHIPPING': 'Shipbuilding & Marine',
    'CONSUMER_RETAIL_ONLINE': 'Retail (General)',
    'ENERGY_INDUSTRIAL_GAS': 'Oil/Gas Distribution',
    'MATERIALS_MINING': 'Metals & Mining',
    'INDUSTRIALS_ENVIRONMENTAL': 'Environmental & Waste Services',
    'SERVICES_AVIATION': 'Air Transport',
}


def log(msg, logf, also_stdout=False):
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
    line = f"[{ts}] {msg}"
    logf.write(line + "\n")
    logf.flush()
    if also_stdout:
        print(msg)


def main():
    parser = argparse.ArgumentParser(description='Reclassify NOT_CLASSIFIED companies')
    parser.add_argument('--dry-run', action='store_true',
                        help='Preview changes without writing Excel or MySQL')
    args = parser.parse_args()

    with open(LOG_PATH, "w") as logf:
        log("=" * 80, logf)
        log(f"RECLASSIFY NOT_CLASSIFIED -- {'DRY RUN' if args.dry_run else 'APPLY MODE'}", logf, also_stdout=True)
        log("=" * 80, logf)

        # --- Step 1: Load Excel ---
        log(f"Loading Excel: {EXCEL_PATH}", logf, also_stdout=True)
        df = pd.read_excel(EXCEL_PATH)
        log(f"  Total rows: {len(df)}", logf, also_stdout=True)

        nc_mask = df['valuation_group'] == 'NOT_CLASSIFIED'
        log(f"  NOT_CLASSIFIED rows: {nc_mask.sum()}", logf, also_stdout=True)

        # --- Step 2: Apply mappings ---
        log("\nApplying CD_Industry1 → taxonomy mappings...", logf, also_stdout=True)
        changes = []
        counters = {
            'reclassified': 0,
            'non_operating': 0,
            'non_operating_blank': 0,
            'newly_investable': 0,
            'unchanged': 0,
        }

        for idx in df[nc_mask].index:
            row = df.loc[idx]
            company_name = str(row['Company Name']).strip() if pd.notna(row['Company Name']) else ''
            cd_industry = str(row['CD_Industry1']).strip() if pd.notna(row['CD_Industry1']) else ''
            accord_code = str(int(row['Accord Code'])) if pd.notna(row['Accord Code']) else ''

            old_group = 'NOT_CLASSIFIED'
            old_subgroup = str(row['valuation_subgroup']).strip() if pd.notna(row['valuation_subgroup']) else ''
            old_investable = bool(row.get('investable', False))

            # Handle blank rows (no Company Name or no CD_Industry1)
            if not company_name or company_name == 'nan':
                new_group = 'NON_OPERATING'
                new_subgroup = 'NON_OPERATING_UNKNOWN'
                action = 'NON_OPERATING_BLANK'
                counters['non_operating_blank'] += 1
            elif cd_industry in ('', 'nan'):
                # Has company name but no industry
                new_group = 'NON_OPERATING'
                new_subgroup = 'NON_OPERATING_UNKNOWN'
                action = 'NON_OPERATING_BLANK'
                counters['non_operating_blank'] += 1
            else:
                mapping = CD_INDUSTRY_TO_TAXONOMY.get(cd_industry)
                if mapping:
                    new_group, new_subgroup = mapping
                    if new_group == 'NON_OPERATING':
                        action = 'NON_OPERATING'
                        counters['non_operating'] += 1
                    else:
                        action = 'RECLASSIFIED'
                        counters['reclassified'] += 1
                else:
                    # No mapping found — stays NOT_CLASSIFIED
                    new_group = 'NOT_CLASSIFIED'
                    new_subgroup = old_subgroup
                    action = 'UNCHANGED'
                    counters['unchanged'] += 1

            # Update Excel DataFrame
            df.at[idx, 'valuation_group'] = new_group
            df.at[idx, 'valuation_subgroup'] = new_subgroup

            # Determine investability for reclassified companies
            new_investable = False
            if action == 'RECLASSIFIED':
                sales_2025 = float(row.get('2025_sales', 0) or 0)
                sales_2024 = float(row.get('2024_sales', 0) or 0)
                best_sales = max(sales_2025, sales_2024)
                # Either NSE symbol or BSE code is sufficient for price data
                has_nse = pd.notna(row.get('CD_NSE Symbol1')) and str(row.get('CD_NSE Symbol1', '')).strip() not in ('', 'nan')
                has_bse = pd.notna(row.get('CD_Bse Scrip ID')) and str(row.get('CD_Bse Scrip ID', '')).strip() not in ('', 'nan', '0')
                if best_sales > INVESTABLE_MIN_SALES_CR and (has_nse or has_bse):
                    new_investable = True
                    counters['newly_investable'] += 1

            df.at[idx, 'investable'] = new_investable

            # Log change
            change = {
                'accord_code': accord_code,
                'company_name': company_name,
                'cd_industry1': cd_industry,
                'old_group': old_group,
                'old_subgroup': old_subgroup,
                'new_group': new_group,
                'new_subgroup': new_subgroup,
                'investable_before': old_investable,
                'investable_after': new_investable,
                'action': action,
            }
            changes.append(change)

            if action != 'UNCHANGED':
                log(f"  {action}: {company_name[:40]:<40} "
                    f"{cd_industry[:30]:<30} → {new_group}/{new_subgroup}"
                    f"{' [INVESTABLE]' if new_investable else ''}", logf)

        # --- Summary ---
        log("\n" + "=" * 80, logf)
        log("MAPPING SUMMARY", logf, also_stdout=True)
        log("=" * 80, logf)
        for key, count in counters.items():
            log(f"  {key:<25} {count:>6}", logf, also_stdout=True)
        # newly_investable is a subset of reclassified, don't double-count
        total_distinct = counters['reclassified'] + counters['non_operating'] + counters['non_operating_blank'] + counters['unchanged']
        log(f"  {'TOTAL PROCESSED':<25} {total_distinct:>6}", logf, also_stdout=True)
        log(f"  (newly_investable is a subset of reclassified)", logf, also_stdout=True)

        # Verify group distribution after changes
        log("\nPost-reclassification valuation_group distribution:", logf, also_stdout=True)
        group_counts = df['valuation_group'].value_counts().sort_values(ascending=False)
        for grp, cnt in group_counts.items():
            log(f"  {grp:<40} {cnt:>6}", logf, also_stdout=True)

        # New subgroup distribution
        log("\nNew subgroup distribution (newly created only):", logf, also_stdout=True)
        for subgrp in sorted(NEW_SUBGROUP_CONFIGS.keys()):
            cnt = len(df[df['valuation_subgroup'] == subgrp])
            if cnt > 0:
                log(f"  {subgrp:<45} {cnt:>6}", logf, also_stdout=True)

        # --- Step 3: Write changes CSV ---
        os.makedirs(CHANGES_CSV_DIR, exist_ok=True)
        csv_path = os.path.join(CHANGES_CSV_DIR,
                                f"reclassify_changes_{datetime.now().strftime('%Y%m%d_%H%M')}.csv")
        fieldnames = ['accord_code', 'company_name', 'cd_industry1',
                       'old_group', 'old_subgroup', 'new_group', 'new_subgroup',
                       'investable_before', 'investable_after', 'action']
        with open(csv_path, 'w', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(changes)
        log(f"\nChanges CSV: {csv_path} ({len(changes)} rows)", logf, also_stdout=True)

        if args.dry_run:
            log("\n*** DRY RUN — no Excel or MySQL changes written ***", logf, also_stdout=True)
            log(f"Log: {LOG_PATH}", logf, also_stdout=True)

            # Print Damodaran mapping additions for reference
            log("\n=== DAMODARAN MAPPING ADDITIONS (paste into damodaran_loader.py) ===", logf, also_stdout=True)
            for subgrp, dam_ind in sorted(NEW_SUBGROUP_TO_DAMODARAN.items()):
                log(f"    '{subgrp}': '{dam_ind}',", logf, also_stdout=True)

            return

        # --- Step 4: Backup and save Excel ---
        backup_path = EXCEL_PATH.replace('.xlsx', '.backup.xlsx')
        log(f"\nBacking up Excel to: {backup_path}", logf, also_stdout=True)
        shutil.copy2(EXCEL_PATH, backup_path)

        log(f"Saving updated Excel: {EXCEL_PATH}", logf, also_stdout=True)
        df.to_excel(EXCEL_PATH, index=False)
        log("  Excel saved.", logf, also_stdout=True)

        # --- Step 5: MySQL updates ---
        log("\nConnecting to MySQL (root@localhost:3306/rag)...", logf, also_stdout=True)
        conn = mysql.connector.connect(
            host="localhost",
            port=3306,
            user="root",
            password="",
            database="rag",
            autocommit=False,
        )
        cur = conn.cursor(dictionary=True)

        try:
            # 5a: Insert NON_OPERATING group config (if not exists)
            cur.execute("SELECT id FROM vs_valuation_group_configs WHERE valuation_group = 'NON_OPERATING'")
            if not cur.fetchone():
                cur.execute(
                    "INSERT INTO vs_valuation_group_configs (valuation_group, is_active) "
                    "VALUES ('NON_OPERATING', 0)"
                )
                log("  Inserted NON_OPERATING into vs_valuation_group_configs", logf, also_stdout=True)
            else:
                log("  NON_OPERATING already exists in vs_valuation_group_configs", logf, also_stdout=True)

            # 5b: Insert new subgroup configs
            for subgrp, (parent_grp, display_name) in NEW_SUBGROUP_CONFIGS.items():
                cur.execute(
                    "SELECT id FROM vs_valuation_subgroup_configs WHERE valuation_subgroup = %s",
                    (subgrp,)
                )
                if not cur.fetchone():
                    cur.execute(
                        "INSERT INTO vs_valuation_subgroup_configs "
                        "(valuation_subgroup, parent_valuation_group, display_name, is_active) "
                        "VALUES (%s, %s, %s, 1)",
                        (subgrp, parent_grp, display_name)
                    )
                    log(f"  Inserted subgroup config: {subgrp} → {parent_grp}", logf)
                else:
                    log(f"  Subgroup config already exists: {subgrp}", logf)

            # 5c: Insert newly investable companies into vs_active_companies
            # First, get existing accord_codes and company_ids
            cur.execute("SELECT accord_code, company_id FROM vs_active_companies")
            existing_rows = cur.fetchall()
            existing_accord_codes = set()
            existing_company_ids = set()
            for r in existing_rows:
                if r['accord_code']:
                    existing_accord_codes.add(str(r['accord_code']).strip())
                if r['company_id']:
                    existing_company_ids.add(r['company_id'])
            log(f"  Existing vs_active_companies: {len(existing_rows)}", logf, also_stdout=True)

            # Preload marketscrip lookup
            cur.execute(
                "SELECT marketscrip_id, accord_code, name, symbol, scrip_code "
                "FROM mssdb.kbapp_marketscrip "
                "WHERE scrip_type IN ('', 'EQS') AND accord_code IS NOT NULL AND accord_code != ''"
            )
            ms_rows = cur.fetchall()
            ms_lookup = {}
            for r in ms_rows:
                raw_ac = str(r['accord_code']).strip()
                clean_ac = raw_ac[:-2] if raw_ac.endswith('.0') else raw_ac
                if clean_ac and clean_ac not in ms_lookup:
                    ms_lookup[clean_ac] = r
            log(f"  Loaded {len(ms_lookup)} marketscrip accord_code mappings", logf)

            # Build NSE symbol lookup from ALL equities (not just those with accord_codes)
            # Some companies like Titan have accord_code=None in marketscrip
            cur.execute(
                "SELECT marketscrip_id, accord_code, name, symbol, scrip_code "
                "FROM mssdb.kbapp_marketscrip "
                "WHERE scrip_type IN ('', 'EQS') AND symbol IS NOT NULL AND symbol != ''"
            )
            all_sym_rows = cur.fetchall()
            ms_symbol_lookup = {}
            for r in all_sym_rows:
                sym = (r.get('symbol') or '').strip().upper()
                if sym and sym not in ms_symbol_lookup:
                    ms_symbol_lookup[sym] = r
            log(f"  Loaded {len(ms_symbol_lookup)} marketscrip NSE symbol mappings (fallback)", logf)

            # Build BSE code lookup in marketscrip
            cur.execute(
                "SELECT marketscrip_id, accord_code, name, symbol, scrip_code "
                "FROM mssdb.kbapp_marketscrip "
                "WHERE scrip_type IN ('', 'EQS') AND scrip_code IS NOT NULL AND scrip_code != ''"
            )
            all_bse_rows = cur.fetchall()
            ms_bse_lookup = {}
            for r in all_bse_rows:
                bse = str(r.get('scrip_code', '')).strip()
                if bse and bse not in ms_bse_lookup:
                    ms_bse_lookup[bse] = r
            log(f"  Loaded {len(ms_bse_lookup)} marketscrip BSE code mappings (fallback)", logf)

            # Build marketscrip name lookup (uppercase, stripped) for fuzzy-ish matching
            cur.execute(
                "SELECT marketscrip_id, accord_code, name, symbol, scrip_code "
                "FROM mssdb.kbapp_marketscrip "
                "WHERE scrip_type IN ('', 'EQS') AND name IS NOT NULL AND name != ''"
            )
            all_name_rows = cur.fetchall()
            ms_name_lookup = {}
            for r in all_name_rows:
                nm = (r.get('name') or '').strip().upper()
                if nm and nm not in ms_name_lookup:
                    ms_name_lookup[nm] = r
            log(f"  Loaded {len(ms_name_lookup)} marketscrip name mappings (fallback)", logf)

            # Precompute Excel accord_code → row index lookup (avoid slow lambda filter)
            excel_ac_lookup = {}
            for eidx, erow in df.iterrows():
                if pd.notna(erow.get('Accord Code')):
                    ac = str(int(erow['Accord Code']))
                    excel_ac_lookup[ac] = eidx

            # Load monthly prices for additional accord_code → NSE/BSE lookup
            log("  Loading monthly prices for accord_code fallback...", logf)
            prices_path = os.getenv('MONTHLY_PRICES_PATH',
                '/Users/ram/code/investment_strategies/data/prices/combined_monthly_prices.csv')
            prices_ac_lookup = {}  # accord_code → {nse_symbol, bse_code, name}
            try:
                prices_df = pd.read_csv(prices_path, usecols=['Company Name', 'accode', 'nse_symbol', 'bse_code'],
                                         low_memory=False)
                # Deduplicate by accode (take first occurrence)
                for _, pr in prices_df.drop_duplicates(subset='accode').iterrows():
                    ac = str(int(pr['accode'])) if pd.notna(pr.get('accode')) else ''
                    if ac:
                        prices_ac_lookup[ac] = {
                            'nse_symbol': str(pr.get('nse_symbol', '')).strip() if pd.notna(pr.get('nse_symbol')) else '',
                            'bse_code': str(int(pr.get('bse_code'))) if pd.notna(pr.get('bse_code')) and pr.get('bse_code') else '',
                            'name': str(pr.get('Company Name', '')).strip() if pd.notna(pr.get('Company Name')) else '',
                        }
                log(f"  Loaded {len(prices_ac_lookup)} monthly prices accord_code mappings", logf)
            except Exception as e:
                log(f"  WARNING: Could not load monthly prices: {e}", logf, also_stdout=True)

            # Get newly investable companies from changes
            newly_investable = [c for c in changes if c['investable_after'] and c['action'] == 'RECLASSIFIED']
            log(f"\n  Newly investable companies to insert: {len(newly_investable)}", logf, also_stdout=True)

            INSERT_SQL = (
                "INSERT INTO vs_active_companies "
                "(company_id, company_name, nse_symbol, bse_code, accord_code, "
                " sector, industry, valuation_group, valuation_subgroup, "
                " cd_sector, cd_industry, csv_name, "
                " valuation_frequency, priority, is_active, added_date, added_by) "
                "VALUES "
                "(%(company_id)s, %(company_name)s, %(nse_symbol)s, %(bse_code)s, %(accord_code)s, "
                " %(sector)s, %(industry)s, %(valuation_group)s, %(valuation_subgroup)s, "
                " %(cd_sector)s, %(cd_industry)s, %(csv_name)s, "
                " %(valuation_frequency)s, %(priority)s, %(is_active)s, CURDATE(), %(added_by)s)"
            )

            inserted = 0
            skipped_existing = 0
            skipped_no_ms = 0
            insert_errors = []
            insert_batch = []
            fallback_stats = {'accord': 0, 'nse_symbol': 0, 'bse_code': 0, 'prices_nse': 0, 'prices_bse': 0, 'name': 0}

            for change in newly_investable:
                accord_code = change['accord_code']

                if accord_code in existing_accord_codes:
                    skipped_existing += 1
                    log(f"  SKIP (already exists): accord={accord_code} ({change['company_name'][:40]})", logf)
                    continue

                # === Multi-tier marketscrip lookup ===
                ms = None
                match_method = ''

                # Tier 1: accord_code in marketscrip
                ms = ms_lookup.get(accord_code)
                if ms:
                    match_method = 'accord'

                # Tier 2: NSE symbol from Excel → marketscrip
                if ms is None:
                    eidx = excel_ac_lookup.get(accord_code)
                    if eidx is not None:
                        nse_sym = str(df.at[eidx, 'CD_NSE Symbol1']).strip().upper() if pd.notna(df.at[eidx, 'CD_NSE Symbol1']) else ''
                        if nse_sym and nse_sym != 'NAN':
                            ms = ms_symbol_lookup.get(nse_sym)
                            if ms:
                                match_method = 'nse_symbol'

                # Tier 3: BSE code from Excel → marketscrip (handles numeric and string BSE codes)
                if ms is None:
                    eidx = excel_ac_lookup.get(accord_code)
                    if eidx is not None:
                        bse_raw = df.at[eidx, 'CD_Bse Scrip ID'] if 'CD_Bse Scrip ID' in df.columns else None
                        if pd.notna(bse_raw):
                            try:
                                bse_code = str(int(float(bse_raw)))
                            except (ValueError, TypeError):
                                bse_code = str(bse_raw).strip()
                            if bse_code and bse_code not in ('0', 'nan'):
                                ms = ms_bse_lookup.get(bse_code)
                                if not ms:
                                    # BSE code might be a symbol string (e.g. 'TITAN')
                                    ms = ms_symbol_lookup.get(bse_code.upper())
                                if ms:
                                    match_method = 'bse_code'

                # Tier 4: Monthly prices accord_code → get NSE/BSE → marketscrip
                if ms is None and accord_code in prices_ac_lookup:
                    pinfo = prices_ac_lookup[accord_code]
                    if pinfo['nse_symbol'] and pinfo['nse_symbol'] not in ('', 'nan'):
                        ms = ms_symbol_lookup.get(pinfo['nse_symbol'].upper())
                        if ms:
                            match_method = 'prices_nse'
                    if ms is None and pinfo['bse_code'] and pinfo['bse_code'] not in ('', '0', 'nan'):
                        ms = ms_bse_lookup.get(pinfo['bse_code'])
                        if ms:
                            match_method = 'prices_bse'

                # Tier 5: Company name exact match in marketscrip
                if ms is None:
                    name_upper = change['company_name'].strip().upper()
                    ms = ms_name_lookup.get(name_upper)
                    if ms:
                        match_method = 'name'

                if ms:
                    fallback_stats[match_method] += 1
                    if match_method != 'accord':
                        log(f"  Found via {match_method}: {change['company_name'][:40]} → marketscrip_id={ms['marketscrip_id']}", logf)

                if ms is None:
                    skipped_no_ms += 1
                    log(f"  ERROR: No marketscrip for accord={accord_code} ({change['company_name'][:40]})", logf)
                    insert_errors.append({
                        'accord_code': accord_code,
                        'company_name': change['company_name'],
                        'error_reason': 'No marketscrip match (all 5 tiers failed)',
                    })
                    continue

                company_id = ms['marketscrip_id']
                if company_id in existing_company_ids:
                    skipped_existing += 1
                    log(f"  SKIP (dup company_id={company_id}): {change['company_name'][:40]}", logf)
                    continue

                # Get Excel row for full details
                eidx = excel_ac_lookup.get(accord_code)
                cd_sector = ''
                cd_industry = ''
                if eidx is not None:
                    cd_sector = str(df.at[eidx, 'CD_Sector']).strip() if pd.notna(df.at[eidx, 'CD_Sector']) else ''
                    cd_industry = str(df.at[eidx, 'CD_Industry1']).strip() if pd.notna(df.at[eidx, 'CD_Industry1']) else ''
                    if cd_sector == 'nan':
                        cd_sector = ''
                    if cd_industry == 'nan':
                        cd_industry = ''

                params = {
                    'company_id': company_id,
                    'company_name': ms['name'] or '',
                    'nse_symbol': ms['symbol'] if ms['symbol'] else None,
                    'bse_code': ms['scrip_code'] if ms['scrip_code'] else None,
                    'accord_code': accord_code,
                    'sector': change['new_group'],
                    'industry': change['new_subgroup'],
                    'valuation_group': change['new_group'],
                    'valuation_subgroup': change['new_subgroup'],
                    'cd_sector': cd_sector,
                    'cd_industry': cd_industry,
                    'csv_name': change['company_name'],
                    'valuation_frequency': 'WEEKLY',
                    'priority': 5,
                    'is_active': 1,
                    'added_by': 'reclassify_feb2026',
                }

                insert_batch.append(params)
                existing_company_ids.add(company_id)
                existing_accord_codes.add(accord_code)

                log(f"  QUEUE: id={company_id}, accord={accord_code}, "
                    f"sym={ms['symbol']}, {change['new_group']}/{change['new_subgroup']}", logf)

                # Batch insert
                if len(insert_batch) >= BATCH_SIZE:
                    try:
                        cur.executemany(INSERT_SQL, insert_batch)
                        conn.commit()
                        inserted += len(insert_batch)
                        log(f"  COMMITTED batch of {len(insert_batch)} (total: {inserted})", logf, also_stdout=True)
                    except Exception as e:
                        conn.rollback()
                        log(f"  BATCH INSERT ERROR: {e}\n{traceback.format_exc()}", logf, also_stdout=True)
                        for p in insert_batch:
                            insert_errors.append({
                                'accord_code': p['accord_code'],
                                'company_name': p['csv_name'],
                                'error_reason': f'Batch insert failed: {str(e)}',
                            })
                    insert_batch = []

            # Final batch
            if insert_batch:
                try:
                    cur.executemany(INSERT_SQL, insert_batch)
                    conn.commit()
                    inserted += len(insert_batch)
                    log(f"  COMMITTED final batch of {len(insert_batch)} (total: {inserted})", logf, also_stdout=True)
                except Exception as e:
                    conn.rollback()
                    log(f"  FINAL BATCH INSERT ERROR: {e}\n{traceback.format_exc()}", logf, also_stdout=True)
                    for p in insert_batch:
                        insert_errors.append({
                            'accord_code': p['accord_code'],
                            'company_name': p['csv_name'],
                            'error_reason': f'Batch insert failed: {str(e)}',
                        })

            # --- Verification ---
            log("\n" + "=" * 80, logf)
            log("MYSQL VERIFICATION: vs_active_companies by valuation_group", logf, also_stdout=True)
            log("=" * 80, logf)
            cur.execute(
                "SELECT valuation_group, COUNT(*) AS cnt "
                "FROM vs_active_companies "
                "GROUP BY valuation_group "
                "ORDER BY cnt DESC"
            )
            verification = cur.fetchall()
            total_final = 0
            log(f"  {'valuation_group':<45} {'count':>6}", logf, also_stdout=True)
            log(f"  {'-' * 53}", logf, also_stdout=True)
            for v in verification:
                grp = v['valuation_group'] or '(NULL)'
                cnt = v['cnt']
                total_final += cnt
                log(f"  {grp:<45} {cnt:>6}", logf, also_stdout=True)
            log(f"  {'-' * 53}", logf, also_stdout=True)
            log(f"  {'TOTAL':<45} {total_final:>6}", logf, also_stdout=True)

            # New subgroup verification
            log("\n  New subgroups in vs_active_companies:", logf, also_stdout=True)
            for subgrp in sorted(NEW_SUBGROUP_CONFIGS.keys()):
                cur.execute(
                    "SELECT COUNT(*) AS cnt FROM vs_active_companies WHERE valuation_subgroup = %s",
                    (subgrp,)
                )
                cnt = cur.fetchone()['cnt']
                if cnt > 0:
                    log(f"    {subgrp:<45} {cnt:>4}", logf, also_stdout=True)

        except Exception as e:
            conn.rollback()
            log(f"\nMYSQL ERROR — ROLLED BACK: {e}\n{traceback.format_exc()}", logf, also_stdout=True)
            raise
        finally:
            cur.close()
            conn.close()

        # --- Final Summary ---
        log("\n" + "=" * 80, logf)
        log("FINAL SUMMARY", logf, also_stdout=True)
        log("=" * 80, logf)
        summary = [
            f"  Excel changes applied:     YES",
            f"  Excel backup:              {backup_path}",
            f"  Reclassified:              {counters['reclassified']}",
            f"  Non-operating:             {counters['non_operating']}",
            f"  Non-operating (blank):     {counters['non_operating_blank']}",
            f"  Unchanged (Misc/Div):      {counters['unchanged']}",
            f"  Newly investable:          {counters['newly_investable']}",
            f"  Inserted to MySQL:         {inserted}",
            f"  Match methods:             accord={fallback_stats['accord']}, nse_symbol={fallback_stats['nse_symbol']}, "
            f"bse_code={fallback_stats['bse_code']}, prices_nse={fallback_stats['prices_nse']}, "
            f"prices_bse={fallback_stats['prices_bse']}, name={fallback_stats['name']}",
            f"  Skipped (already in DB):   {skipped_existing}",
            f"  Skipped (no marketscrip):  {skipped_no_ms}",
            f"  Insert errors:             {len(insert_errors)}",
            f"  Changes CSV:               {csv_path}",
            f"  Log:                       {LOG_PATH}",
        ]
        for line in summary:
            log(line, logf, also_stdout=True)

        # Print Damodaran mapping reminder
        log("\n=== NEXT STEPS ===", logf, also_stdout=True)
        log("1. Add these to SUBGROUP_TO_DAMODARAN_INDIA in damodaran_loader.py:", logf, also_stdout=True)
        for subgrp, dam_ind in sorted(NEW_SUBGROUP_TO_DAMODARAN.items()):
            log(f"    '{subgrp}': '{dam_ind}',", logf, also_stdout=True)
        log("2. Re-run beta calculator:", logf, also_stdout=True)
        log("   python3 valuation_system/data/processors/beta_calculator.py --frequency weekly", logf, also_stdout=True)
        log("3. Test a newly-classified company:", logf, also_stdout=True)
        log("   python3 valuation_system/utils/batch_valuation.py --symbols TITAN --mode quick", logf, also_stdout=True)

        log("\nDONE.", logf, also_stdout=True)


if __name__ == '__main__':
    try:
        main()
    except Exception as e:
        print(f"FATAL ERROR: {e}")
        traceback.print_exc()
        with open(LOG_PATH, 'a') as logf:
            logf.write(f"\nFATAL ERROR: {e}\n")
            logf.write(traceback.format_exc())
        sys.exit(1)
