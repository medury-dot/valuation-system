#!/usr/bin/env python3
"""
Refine Subgroup Classification Script
- Adds granularity to existing valuation_subgroup classifications
- Updates Excel, MySQL, and GSheet with refined subgroups and drivers
"""

import os
import sys
import pandas as pd
import mysql.connector
from datetime import datetime

# Add parent directory to path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))

from dotenv import load_dotenv
load_dotenv('/Users/ram/code/research/valuation_system/config/.env')

# ============================================================================
# REFINEMENT RULES - Map existing subgroups to refined subgroups
# ============================================================================

def refine_auto_ancillary(row):
    """Split AUTO_ANCILLARY into TIRES/BATTERIES/COMPONENTS"""
    industry = str(row.get('CD_Industry1', '')).lower()
    if 'tyre' in industry or 'tire' in industry:
        return 'AUTO_ANCILLARY_TIRES'
    elif 'batter' in industry:
        return 'AUTO_ANCILLARY_BATTERIES'
    else:
        return 'AUTO_ANCILLARY_COMPONENTS'

def refine_consumer_durables(row):
    """Split CONSUMER_DURABLES into WHITE_GOODS/BROWN_GOODS/SMALL_APPLIANCES"""
    industry = str(row.get('CD_Industry1', '')).lower()
    if 'air condition' in industry or 'domestic appliance' in industry:
        return 'CONSUMER_DURABLES_WHITE_GOODS'
    elif 'electronic' in industry or 'hardware' in industry:
        return 'CONSUMER_DURABLES_BROWN_GOODS'
    else:
        return 'CONSUMER_DURABLES_SMALL_APPLIANCES'

def refine_consumer_retail(row):
    """Split CONSUMER_RETAIL into ONLINE/OFFLINE"""
    company = str(row.get('Company Name', '')).lower()
    # Known e-commerce/online retailers
    online_keywords = ['flipkart', 'amazon', 'nykaa', 'zomato', 'swiggy', 'meesho',
                       'paytm mall', 'myntra', 'ajio', 'snapdeal', 'shopclues',
                       'firstcry', 'lenskart', 'pepperfry', 'urbanladder', 'bigbasket']
    for kw in online_keywords:
        if kw in company:
            return 'CONSUMER_RETAIL_ONLINE'
    return 'CONSUMER_RETAIL_OFFLINE'

def refine_consumer_fmcg(row):
    """Split CONSUMER_FMCG into HPC/PACKAGED_FOOD/STAPLES"""
    industry = str(row.get('CD_Industry1', '')).lower()
    company = str(row.get('Company Name', '')).lower()

    # HPC (Home & Personal Care)
    hpc_keywords = ['personal', 'household', 'detergent', 'soap', 'cosmetic', 'beauty']
    for kw in hpc_keywords:
        if kw in industry:
            return 'CONSUMER_FMCG_HPC'

    # Staples (commodity-linked)
    staples_keywords = ['edible oil', 'cigarette', 'tobacco', 'sugar']
    for kw in staples_keywords:
        if kw in industry:
            return 'CONSUMER_FMCG_STAPLES'

    # Known HPC companies
    hpc_companies = ['hindustan unilever', 'marico', 'dabur', 'colgate', 'godrej consumer',
                     'emami', 'jyothy', 'bajaj consumer']
    for kw in hpc_companies:
        if kw in company:
            return 'CONSUMER_FMCG_HPC'

    # Known staples companies
    staples_companies = ['itc', 'adani wilmar', 'patanjali foods', 'godfrey phillips']
    for kw in staples_companies:
        if kw in company:
            return 'CONSUMER_FMCG_STAPLES'

    # Default to packaged food (Nestle, Britannia, etc.)
    return 'CONSUMER_FMCG_PACKAGED_FOOD'

def refine_energy_oil_gas(row):
    """Split ENERGY_OIL_GAS into UPSTREAM/MIDSTREAM/DOWNSTREAM"""
    industry = str(row.get('CD_Industry1', '')).lower()

    if 'exploration' in industry:
        return 'ENERGY_UPSTREAM'
    elif 'transmission' in industry or 'marketing' in industry:
        return 'ENERGY_MIDSTREAM'
    elif 'refiner' in industry or 'petrochemical' in industry:
        return 'ENERGY_DOWNSTREAM'
    else:
        return 'ENERGY_DOWNSTREAM'  # Default

def refine_financials_other(row):
    """Split FINANCIALS_OTHER into RATINGS/EXCHANGES_DEPOSITORIES/BROKING"""
    industry = str(row.get('CD_Industry1', '')).lower()

    if 'rating' in industry:
        return 'FINANCIALS_RATINGS'
    elif 'depository' in industry:
        return 'FINANCIALS_EXCHANGES_DEPOSITORIES'
    elif 'broking' in industry:
        return 'FINANCIALS_BROKING'
    else:
        return 'FINANCIALS_BROKING'  # Default

def refine_financials_asset_mgmt(row):
    """Split FINANCIALS_ASSET_MGMT - move Stock Broking to BROKING"""
    industry = str(row.get('CD_Industry1', '')).lower()

    if 'broking' in industry:
        return 'FINANCIALS_BROKING'
    elif 'depository' in industry:
        return 'FINANCIALS_EXCHANGES_DEPOSITORIES'
    else:
        return 'FINANCIALS_ASSET_MGMT'

def classify_insurance(row):
    """Classify Insurance companies from NOT_CLASSIFIED"""
    company = str(row.get('Company Name', '')).lower()

    if 'health' in company:
        return 'FINANCIALS_INSURANCE_HEALTH'
    elif 'life' in company:
        return 'FINANCIALS_INSURANCE_LIFE'
    elif 'general' in company or 'lombard' in company:
        return 'FINANCIALS_INSURANCE_GENERAL'
    else:
        # Default based on company patterns
        if any(x in company for x in ['lic ', 'sbi life', 'hdfc life', 'icici pru', 'max life']):
            return 'FINANCIALS_INSURANCE_LIFE'
        return 'FINANCIALS_INSURANCE_GENERAL'

def refine_healthcare_pharma(row):
    """Split HEALTHCARE_PHARMA_EXPORT into MFG/CRO_CDMO"""
    company = str(row.get('Company Name', '')).lower()

    # Known CRO/CDMO companies
    cro_cdmo = ['syngene', 'divi', 'laurus', 'jubilant', 'piramal', 'suven', 'dishman',
                'aragen', 'neuland', 'hikal', 'aarti pharma', 'pi industries']
    for kw in cro_cdmo:
        if kw in company:
            return 'HEALTHCARE_PHARMA_CRO_CDMO'

    return 'HEALTHCARE_PHARMA_MFG'

def refine_healthcare_hospitals(row):
    """Split HEALTHCARE_HOSPITALS_EQUIPMENT into HOSPITALS/DIAGNOSTICS/EQUIPMENT/AYUSH"""
    industry = str(row.get('CD_Industry1', '')).lower()
    company = str(row.get('Company Name', '')).lower()

    # Diagnostics
    diagnostics = ['lal path', 'thyrocare', 'metropolis', 'srl', 'suburban', 'krsnaa']
    for kw in diagnostics:
        if kw in company:
            return 'HEALTHCARE_DIAGNOSTICS'

    # AYUSH (Ayurvedic)
    ayush = ['patanjali', 'dabur', 'himalaya', 'baidyanath', 'hamdard', 'zandu']
    for kw in ayush:
        if kw in company:
            return 'HEALTHCARE_AYUSH'

    if 'equipment' in industry or 'supplies' in industry:
        return 'HEALTHCARE_MEDICAL_EQUIPMENT'

    return 'HEALTHCARE_HOSPITALS'

def refine_metals_non_ferrous(row):
    """Split METALS_NON_FERROUS into ALUMINUM/COPPER_ZINC"""
    industry = str(row.get('CD_Industry1', '')).lower()

    if 'alumin' in industry:
        return 'METALS_ALUMINUM'
    else:
        return 'METALS_COPPER_ZINC'

def refine_services_media(row):
    """Split SERVICES_MEDIA_ENTERTAINMENT into BROADCASTING/OTT/PRINT/ADVERTISING"""
    industry = str(row.get('CD_Industry1', '')).lower()
    company = str(row.get('Company Name', '')).lower()

    # OTT platforms
    ott = ['netflix', 'hotstar', 'zee5', 'sonyliv', 'voot', 'jiocinema', 'prime video']
    for kw in ott:
        if kw in company:
            return 'SERVICES_MEDIA_OTT'

    # Print media
    print_media = ['times', 'hindustan times', 'indian express', 'hindu', 'jagran',
                   'dainik', 'sakal', 'lokmat', 'deccan', 'telegraph', 'tribune']
    for kw in print_media:
        if kw in company:
            return 'SERVICES_MEDIA_PRINT'

    # Advertising
    if 'advertising' in industry or 'media' in industry:
        # Check if it's an ad agency
        ad_agencies = ['wpp', 'omnicom', 'publicis', 'interpublic', 'dentsu', 'ogilvy']
        for kw in ad_agencies:
            if kw in company:
                return 'SERVICES_MEDIA_ADVERTISING'

    # Default to broadcasting (TV channels)
    return 'SERVICES_MEDIA_BROADCASTING'

def refine_services_telecom(row):
    """Split SERVICES_TELECOM into OPERATORS/TOWERS"""
    company = str(row.get('Company Name', '')).lower()

    # Tower companies
    towers = ['indus tower', 'bharti infratel', 'tower', 'infratel']
    for kw in towers:
        if kw in company:
            return 'SERVICES_TELECOM_TOWERS'

    return 'SERVICES_TELECOM_OPERATORS'

def classify_telecom_equipment(row):
    """Move Telecom Equipment from NOT_CLASSIFIED to INDUSTRIALS"""
    return 'INDUSTRIALS_TELECOM_EQUIPMENT'


# ============================================================================
# MAIN REFINEMENT FUNCTION
# ============================================================================

def refine_subgroups(df):
    """Apply all refinement rules to the dataframe"""

    # Create a copy of valuation_subgroup for comparison
    df['original_subgroup'] = df['valuation_subgroup'].copy()

    changes = []

    for idx, row in df.iterrows():
        original = row['valuation_subgroup']
        industry = str(row.get('CD_Industry1', ''))
        new_subgroup = original
        new_group = row.get('valuation_group', '')

        # Apply refinement rules based on current subgroup
        if original == 'AUTO_ANCILLARY':
            new_subgroup = refine_auto_ancillary(row)

        elif original == 'CONSUMER_DURABLES':
            new_subgroup = refine_consumer_durables(row)

        elif original == 'CONSUMER_RETAIL':
            new_subgroup = refine_consumer_retail(row)

        elif original == 'CONSUMER_FMCG':
            new_subgroup = refine_consumer_fmcg(row)

        elif original == 'ENERGY_OIL_GAS':
            new_subgroup = refine_energy_oil_gas(row)

        elif original == 'FINANCIALS_OTHER':
            new_subgroup = refine_financials_other(row)

        elif original == 'FINANCIALS_ASSET_MGMT':
            new_subgroup = refine_financials_asset_mgmt(row)

        elif original == 'HEALTHCARE_PHARMA_EXPORT':
            new_subgroup = refine_healthcare_pharma(row)

        elif original == 'HEALTHCARE_HOSPITALS_EQUIPMENT':
            new_subgroup = refine_healthcare_hospitals(row)

        elif original == 'METALS_NON_FERROUS':
            new_subgroup = refine_metals_non_ferrous(row)

        elif original == 'SERVICES_MEDIA_ENTERTAINMENT':
            new_subgroup = refine_services_media(row)

        elif original == 'SERVICES_TELECOM':
            new_subgroup = refine_services_telecom(row)

        # Handle NOT_CLASSIFIED - Insurance and Telecom Equipment
        elif original == 'NOT_CLASSIFIED':
            if industry == 'Insurance':
                new_subgroup = classify_insurance(row)
                new_group = 'FINANCIALS'
            elif industry == 'Telecommunication - Equipment':
                new_subgroup = classify_telecom_equipment(row)
                new_group = 'INDUSTRIALS'

        # Update if changed
        if new_subgroup != original:
            df.at[idx, 'valuation_subgroup'] = new_subgroup
            if new_group and new_group != row.get('valuation_group', ''):
                df.at[idx, 'valuation_group'] = new_group
            changes.append({
                'company': row['Company Name'],
                'original': original,
                'refined': new_subgroup
            })

    return df, changes


def update_mysql(df):
    """Update vs_active_companies with refined subgroups"""
    conn = mysql.connector.connect(
        host=os.getenv('MYSQL_HOST', 'localhost'),
        port=int(os.getenv('MYSQL_PORT', 3306)),
        user=os.getenv('MYSQL_USER', 'root'),
        password=os.getenv('MYSQL_PASSWORD', ''),
        database=os.getenv('MYSQL_DATABASE', 'rag')
    )
    cursor = conn.cursor()

    # Update each company
    updated = 0
    for _, row in df.iterrows():
        accord_code = str(row.get('Accord Code'))
        subgroup = row.get('valuation_subgroup')
        group = row.get('valuation_group')

        if accord_code and subgroup:
            cursor.execute("""
                UPDATE vs_active_companies
                SET valuation_subgroup = %s, valuation_group = %s, last_synced = NOW()
                WHERE accord_code = %s
            """, (subgroup, group, accord_code))
            if cursor.rowcount > 0:
                updated += 1

    conn.commit()
    print(f"[MySQL] Updated {updated} companies in vs_active_companies")

    cursor.close()
    conn.close()
    return updated


def get_subgroup_counts(df):
    """Get counts of companies per subgroup"""
    counts = df['valuation_subgroup'].value_counts().sort_index()
    return counts


def main():
    print("=" * 70)
    print("REFINE SUBGROUP CLASSIFICATION")
    print("=" * 70)

    excel_path = '/Users/ram/code/research/valuation_group-valuation_subgroup-feb2026.xlsx'

    # Step 1: Read Excel
    print(f"\n[1] Reading Excel: {excel_path}")
    df = pd.read_excel(excel_path)
    print(f"    Loaded {len(df)} companies")

    # Get original counts
    print("\n[2] Original subgroup counts:")
    original_counts = get_subgroup_counts(df)
    print(f"    {len(original_counts)} unique subgroups")

    # Step 3: Apply refinements
    print("\n[3] Applying refinement rules...")
    df, changes = refine_subgroups(df)
    print(f"    {len(changes)} companies reclassified")

    # Show sample changes
    if changes:
        print("\n    Sample changes (first 20):")
        for c in changes[:20]:
            print(f"      {c['company'][:40]:<40} {c['original']:<35} -> {c['refined']}")

    # Get new counts
    print("\n[4] New subgroup counts:")
    new_counts = get_subgroup_counts(df)
    print(f"    {len(new_counts)} unique subgroups")

    # Show new subgroups
    new_subgroups = set(new_counts.index) - set(original_counts.index)
    if new_subgroups:
        print(f"\n    New subgroups added ({len(new_subgroups)}):")
        for sg in sorted(new_subgroups):
            print(f"      {sg}: {new_counts[sg]} companies")

    # Step 4: Save Excel
    print(f"\n[5] Saving updated Excel...")
    # Remove temporary column
    df = df.drop(columns=['original_subgroup'], errors='ignore')
    df.to_excel(excel_path, index=False)
    print(f"    Saved to {excel_path}")

    # Step 5: Update MySQL
    print("\n[6] Updating MySQL...")
    update_mysql(df)

    # Final summary
    print("\n" + "=" * 70)
    print("SUMMARY")
    print("=" * 70)
    print(f"Total companies: {len(df)}")
    print(f"Subgroups: {len(original_counts)} -> {len(new_counts)}")
    print(f"Companies reclassified: {len(changes)}")

    # Print all subgroups with counts
    print("\n[ALL SUBGROUPS]")
    for sg, count in new_counts.items():
        print(f"  {sg:<45} {count:>5}")

    return df, changes


if __name__ == '__main__':
    main()
