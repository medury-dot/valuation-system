#!/usr/bin/env python3
"""
Round 2: Match 913 investable companies that failed accord_code matching.
Strategy:
  1. Match by NSE symbol (Excel CD_NSE Symbol1 -> marketscrip symbol)
  2. Match by BSE code (Excel CD_Bse Scrip ID -> marketscrip symbol)
  3. Match by alternate_symbol
  4. Very strict fuzzy match by company name (threshold >= 95 only)
  5. For each match, insert into vs_active_companies if not already present
"""

import pandas as pd
import mysql.connector
import logging
import traceback
import re
import sys
from datetime import datetime
from rapidfuzz import fuzz, process

# Setup logging
LOG_FILE = '/Users/ram/code/research/expand_active_companies_round2.log'
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        logging.FileHandler(LOG_FILE, mode='w'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

def get_db_connection():
    return mysql.connector.connect(
        host='localhost',
        port=3306,
        user='root',
        password='',
        database='rag'
    )

def load_marketscrip_equities(conn):
    """Load all equity records from marketscrip with valid symbols."""
    query = """
        SELECT marketscrip_id, symbol, scrip_code, name, alternate_symbol, alternate_name
        FROM mssdb.kbapp_marketscrip 
        WHERE scrip_type IN ('', 'EQS')
        AND symbol IS NOT NULL 
        AND symbol != ''
        AND symbol != 'nan'
    """
    df = pd.read_sql(query, conn)
    logger.info(f"Loaded {len(df)} equity records from marketscrip (with valid symbol)")
    return df

def get_existing_company_ids(conn):
    """Get set of company_ids already in vs_active_companies."""
    cursor = conn.cursor()
    cursor.execute("SELECT company_id FROM vs_active_companies")
    ids = set(row[0] for row in cursor.fetchall())
    cursor.close()
    logger.info(f"Found {len(ids)} existing company_ids in vs_active_companies")
    return ids

def normalize_name_for_fuzzy(name):
    """Normalize company name for fuzzy matching."""
    if pd.isna(name):
        return ''
    name = str(name).lower().strip()
    # Remove common suffixes
    for suffix in [' limited', ' ltd.', ' ltd', ' pvt.', ' pvt', ' private',
                   ' public', ' corporation', ' corp.', ' corp',
                   ' - (rights entitlements (res))']:
        name = name.replace(suffix, '')
    # Remove special chars but keep spaces
    name = re.sub(r'[^a-z0-9\s&]', ' ', name)
    name = re.sub(r'\s+', ' ', name).strip()
    return name


def match_by_nse_symbol(error_df, excel_df, ms_df):
    """Match by NSE symbol."""
    matches = {}
    symbol_lookup = {}
    for _, row in ms_df.iterrows():
        sym = str(row['symbol']).strip().upper()
        if sym and sym != 'NAN' and sym not in symbol_lookup:
            symbol_lookup[sym] = row

    for _, erow in error_df.iterrows():
        accord_code = erow['accord_code']
        excel_row = excel_df[excel_df['Accord Code'] == accord_code]
        if excel_row.empty:
            continue
        excel_row = excel_row.iloc[0]
        nse_sym = excel_row.get('CD_NSE Symbol1')
        if pd.notna(nse_sym):
            nse_sym = str(nse_sym).strip().upper()
            if nse_sym in symbol_lookup:
                ms_row = symbol_lookup[nse_sym]
                matches[accord_code] = {
                    'marketscrip_id': int(ms_row['marketscrip_id']),
                    'symbol': ms_row['symbol'],
                    'scrip_code': ms_row['scrip_code'],
                    'ms_name': ms_row['name'],
                    'excel_name': excel_row['Company Name'],
                    'method': 'nse_symbol'
                }
    logger.info(f"NSE symbol matching: {len(matches)} matches")
    return matches

def match_by_bse_code(error_df, excel_df, ms_df, already_matched):
    """Match by BSE code (Excel has symbol-like BSE codes) against marketscrip symbol."""
    matches = {}
    symbol_lookup = {}
    for _, row in ms_df.iterrows():
        sym = str(row['symbol']).strip().upper()
        if sym and sym != 'NAN' and sym not in symbol_lookup:
            symbol_lookup[sym] = row

    for _, erow in error_df.iterrows():
        accord_code = erow['accord_code']
        if accord_code in already_matched:
            continue
        excel_row = excel_df[excel_df['Accord Code'] == accord_code]
        if excel_row.empty:
            continue
        excel_row = excel_row.iloc[0]
        bse_code = excel_row.get('CD_Bse Scrip ID')
        if pd.notna(bse_code):
            bse_code = str(bse_code).strip().upper()
            if bse_code in symbol_lookup:
                ms_row = symbol_lookup[bse_code]
                matches[accord_code] = {
                    'marketscrip_id': int(ms_row['marketscrip_id']),
                    'symbol': ms_row['symbol'],
                    'scrip_code': ms_row['scrip_code'],
                    'ms_name': ms_row['name'],
                    'excel_name': excel_row['Company Name'],
                    'method': 'bse_code_as_symbol'
                }
    logger.info(f"BSE code (as symbol) matching: {len(matches)} matches")
    return matches

def match_by_alternate_symbol(error_df, excel_df, ms_df, already_matched):
    """Match by alternate_symbol."""
    matches = {}
    alt_sym_lookup = {}
    for _, row in ms_df.iterrows():
        alt_sym = row.get('alternate_symbol')
        if pd.notna(alt_sym):
            alt_sym = str(alt_sym).strip().upper()
            if alt_sym and alt_sym != 'NAN' and alt_sym not in alt_sym_lookup:
                alt_sym_lookup[alt_sym] = row

    for _, erow in error_df.iterrows():
        accord_code = erow['accord_code']
        if accord_code in already_matched:
            continue
        excel_row = excel_df[excel_df['Accord Code'] == accord_code]
        if excel_row.empty:
            continue
        excel_row = excel_row.iloc[0]
        
        for sym_col in ['CD_NSE Symbol1', 'CD_Bse Scrip ID']:
            sym = excel_row.get(sym_col)
            if pd.notna(sym):
                sym = str(sym).strip().upper()
                if sym in alt_sym_lookup:
                    ms_row = alt_sym_lookup[sym]
                    matches[accord_code] = {
                        'marketscrip_id': int(ms_row['marketscrip_id']),
                        'symbol': ms_row['symbol'],
                        'scrip_code': ms_row['scrip_code'],
                        'ms_name': ms_row['name'],
                        'excel_name': excel_row['Company Name'],
                        'method': 'alternate_symbol'
                    }
                    break
    logger.info(f"Alternate symbol matching: {len(matches)} matches")
    return matches

def match_by_name_very_strict(error_df, excel_df, ms_df, already_matched):
    """
    Very strict fuzzy name matching:
    - Only accept score >= 95 (near-exact after normalization)
    - Also exclude Rights Entitlements matches
    """
    matches = {}
    
    # Build normalized name -> ms_row lookup, excluding REs
    ms_names = {}
    for _, row in ms_df.iterrows():
        name = str(row['name']).strip()
        if name and name.lower() != 'nan':
            # Skip Rights Entitlements entries
            if 'rights entitlements' in name.lower() or '(res)' in name.lower():
                continue
            norm = normalize_name_for_fuzzy(name)
            if norm and norm not in ms_names:
                ms_names[norm] = row
    
    ms_name_list = list(ms_names.keys())
    
    THRESHOLD = 95  # Very high threshold - near exact matches only
    
    for _, erow in error_df.iterrows():
        accord_code = erow['accord_code']
        if accord_code in already_matched:
            continue
        
        company_name = erow['company_name']
        norm_name = normalize_name_for_fuzzy(company_name)
        if not norm_name or len(norm_name) < 3:
            continue
        
        # Use process.extract to get top match
        results = process.extract(
            norm_name, 
            ms_name_list, 
            scorer=fuzz.token_sort_ratio, 
            limit=2
        )
        
        if not results:
            continue
        
        best_name, best_score, best_idx = results[0]
        
        if best_score >= THRESHOLD:
            ms_row = ms_names[best_name]
            matches[accord_code] = {
                'marketscrip_id': int(ms_row['marketscrip_id']),
                'symbol': ms_row['symbol'],
                'scrip_code': ms_row['scrip_code'],
                'ms_name': ms_row['name'],
                'excel_name': company_name,
                'method': f'name_fuzzy_score{int(best_score)}',
                'score': best_score
            }
            logger.debug(f"  FUZZY [{int(best_score)}]: '{company_name}' -> '{ms_row['name']}' (id={ms_row['marketscrip_id']})")
        elif best_score >= 80:
            logger.debug(f"  NEAR MISS [{int(best_score)}]: '{company_name}' -> '{ms_names[best_name]['name']}'")
    
    logger.info(f"Very strict fuzzy name matching (>=95): {len(matches)} matches")
    return matches

def insert_matched_companies(conn, all_matches, excel_df, existing_ids):
    """Insert matched companies into vs_active_companies."""
    inserted = 0
    skipped_existing = 0
    skipped_duplicate_ms = 0
    errors = 0
    
    cursor = conn.cursor()
    
    # Get current marketscrip_ids in the table
    cursor.execute("SELECT company_id FROM vs_active_companies")
    existing_ms_ids = set(row[0] for row in cursor.fetchall())
    
    insert_sql = """
        INSERT INTO vs_active_companies 
        (company_id, nse_symbol, company_name, sector, industry, 
         valuation_frequency, priority, is_active, added_date, added_by,
         csv_name, accord_code, bse_code, valuation_group, valuation_subgroup,
         cd_sector, cd_industry)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """
    
    for accord_code, match in all_matches.items():
        ms_id = match['marketscrip_id']
        
        if ms_id in existing_ms_ids:
            logger.debug(f"  SKIP (exists): company_id={ms_id}, {match.get('excel_name','')}")
            skipped_existing += 1
            continue
        
        # Get Excel row
        excel_row = excel_df[excel_df['Accord Code'] == accord_code]
        if excel_row.empty:
            logger.warning(f"  No Excel row for accord_code={accord_code}")
            errors += 1
            continue
        excel_row = excel_row.iloc[0]
        
        company_name = excel_row['Company Name']
        nse_symbol = match.get('symbol', '')
        bse_code = str(excel_row.get('CD_Bse Scrip ID', '')) if pd.notna(excel_row.get('CD_Bse Scrip ID')) else ''
        valuation_group = excel_row.get('valuation_group', '') if pd.notna(excel_row.get('valuation_group')) else ''
        valuation_subgroup = excel_row.get('valuation_subgroup', '') if pd.notna(excel_row.get('valuation_subgroup')) else ''
        cd_sector = excel_row.get('CD_Sector', '') if pd.notna(excel_row.get('CD_Sector')) else ''
        cd_industry = excel_row.get('CD_Industry1', '') if pd.notna(excel_row.get('CD_Industry1')) else ''
        
        try:
            cursor.execute(insert_sql, (
                ms_id, nse_symbol, company_name, valuation_group, valuation_subgroup,
                'DAILY', 5, 1, datetime.now().strftime('%Y-%m-%d'), 'round2_migration',
                company_name, str(accord_code), bse_code, valuation_group, valuation_subgroup,
                cd_sector, cd_industry
            ))
            existing_ms_ids.add(ms_id)
            inserted += 1
            logger.debug(f"  INSERTED: {company_name} (accord={accord_code}, ms_id={ms_id}, method={match['method']})")
        except mysql.connector.IntegrityError as e:
            if 'Duplicate' in str(e):
                skipped_duplicate_ms += 1
                logger.debug(f"  DUPLICATE: company_id={ms_id} for {company_name}")
            else:
                errors += 1
                logger.error(f"  ERROR inserting {company_name}: {e}")
        except Exception as e:
            errors += 1
            logger.error(f"  ERROR inserting {company_name}: {e}\n{traceback.format_exc()}")
    
    conn.commit()
    cursor.close()
    
    return inserted, skipped_existing, skipped_duplicate_ms, errors


def main():
    logger.info("=" * 80)
    logger.info("ROUND 2: Matching 913 unmatched investable companies")
    logger.info("=" * 80)
    
    # Load data
    errors_df = pd.read_csv('/Users/ram/code/research/expand_active_errors.csv')
    logger.info(f"Loaded {len(errors_df)} error companies")
    
    excel_df = pd.read_excel('/Users/ram/code/research/valuation_group-valuation_subgroup-feb2026.xlsx')
    logger.info(f"Loaded Excel with {len(excel_df)} companies")
    
    conn = get_db_connection()
    ms_df = load_marketscrip_equities(conn)
    existing_ids = get_existing_company_ids(conn)
    
    # --- Method 1: NSE Symbol ---
    logger.info("-" * 60)
    logger.info("METHOD 1: NSE Symbol")
    nse_matches = match_by_nse_symbol(errors_df, excel_df, ms_df)
    for ac, m in sorted(nse_matches.items(), key=lambda x: x[1]['excel_name']):
        logger.debug(f"  NSE: {m['excel_name']} -> {m['ms_name']} (sym={m['symbol']}, id={m['marketscrip_id']})")
    
    # --- Method 2: BSE Code ---
    logger.info("-" * 60)
    logger.info("METHOD 2: BSE Code (as symbol)")
    bse_matches = match_by_bse_code(errors_df, excel_df, ms_df, set(nse_matches.keys()))
    for ac, m in sorted(bse_matches.items(), key=lambda x: x[1]['excel_name']):
        logger.debug(f"  BSE: {m['excel_name']} -> {m['ms_name']} (sym={m['symbol']}, id={m['marketscrip_id']})")
    
    # --- Method 3: Alternate Symbol ---
    logger.info("-" * 60)
    logger.info("METHOD 3: Alternate Symbol")
    already_matched = set(nse_matches.keys()) | set(bse_matches.keys())
    alt_matches = match_by_alternate_symbol(errors_df, excel_df, ms_df, already_matched)
    for ac, m in sorted(alt_matches.items(), key=lambda x: x[1]['excel_name']):
        logger.debug(f"  ALT: {m['excel_name']} -> {m['ms_name']} (sym={m['symbol']}, id={m['marketscrip_id']})")
    
    # --- Method 4: Very Strict Fuzzy Name ---
    logger.info("-" * 60)
    logger.info("METHOD 4: Very strict fuzzy name (>=95)")
    already_matched = already_matched | set(alt_matches.keys())
    name_matches = match_by_name_very_strict(errors_df, excel_df, ms_df, already_matched)
    
    # Combine all matches
    all_matches = {}
    all_matches.update(nse_matches)
    all_matches.update(bse_matches)
    all_matches.update(alt_matches)
    all_matches.update(name_matches)
    
    logger.info("=" * 60)
    logger.info(f"TOTAL MATCHES: {len(all_matches)}")
    logger.info(f"  NSE Symbol:         {len(nse_matches)}")
    logger.info(f"  BSE Code (symbol):  {len(bse_matches)}")
    logger.info(f"  Alternate Symbol:   {len(alt_matches)}")
    logger.info(f"  Name Fuzzy (>=95):  {len(name_matches)}")
    
    # --- Insert ---
    logger.info("-" * 60)
    logger.info("INSERTING into vs_active_companies...")
    inserted, skipped_existing, skipped_dup, errors = insert_matched_companies(
        conn, all_matches, excel_df, existing_ids
    )
    
    logger.info(f"INSERT RESULTS:")
    logger.info(f"  Inserted:           {inserted}")
    logger.info(f"  Skipped (existed):  {skipped_existing}")
    logger.info(f"  Skipped (dup ms):   {skipped_dup}")
    logger.info(f"  Errors:             {errors}")
    
    # --- Unmatched ---
    matched_codes = set(all_matches.keys())
    unmatched_df = errors_df[~errors_df['accord_code'].isin(matched_codes)].copy()
    
    # Enrich with Excel data
    unmatched_enriched = unmatched_df.merge(
        excel_df[['Accord Code', 'Company Name', 'CD_NSE Symbol1', 'CD_Bse Scrip ID', 
                  'CD_Sector', 'CD_Industry1', 'valuation_group', 'valuation_subgroup']],
        left_on='accord_code', right_on='Accord Code', how='left'
    )
    if 'Accord Code' in unmatched_enriched.columns:
        unmatched_enriched = unmatched_enriched.drop(columns=['Accord Code'], errors='ignore')
    
    unmatched_enriched.to_csv('/Users/ram/code/research/expand_active_errors_round2.csv', index=False)
    logger.info(f"Saved {len(unmatched_enriched)} unmatched to expand_active_errors_round2.csv")
    
    # Final count
    cursor = conn.cursor()
    cursor.execute("SELECT COUNT(*) FROM vs_active_companies")
    total = cursor.fetchone()[0]
    cursor.close()
    
    logger.info("=" * 60)
    logger.info("FINAL SUMMARY")
    logger.info(f"  Total in vs_active_companies: {total}")
    logger.info(f"  Matched this round:           {len(all_matches)}")
    logger.info(f"  Inserted this round:          {inserted}")
    logger.info(f"  Still unmatched:              {len(unmatched_enriched)}")
    logger.info("=" * 60)
    
    conn.close()

if __name__ == '__main__':
    main()
