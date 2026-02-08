#!/usr/bin/env python3
"""
Company Migration Script - Load companies from gem classification file
Migrates up to 1000 investable companies with balanced sector distribution
"""

import sys
import os
import pandas as pd
import yaml
import json
import argparse
from datetime import datetime, date
from pathlib import Path
from typing import Dict, List, Tuple

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from storage.mysql_client import ValuationMySQLClient
from dotenv import load_dotenv

load_dotenv(Path(__file__).parent.parent / 'config' / '.env')


class CompanyMigrator:
    """Handles migration of companies from gem file to database."""

    def __init__(self, mysql_client: ValuationMySQLClient):
        self.mysql = mysql_client
        self.gem_file = Path(__file__).parent.parent / 'data' / 'reference' / 'sector-industry-vertical-feb2026.xlsx'
        self.companies_yaml = Path(__file__).parent.parent / 'config' / 'companies.yaml'
        self.prices_file = Path(os.getenv('MONTHLY_PRICES_PATH', ''))

    def load_gem_classification(self) -> pd.DataFrame:
        """Load gem classification Excel file."""
        print(f"Loading gem classification from: {self.gem_file}")
        df = pd.read_excel(self.gem_file)
        print(f"Loaded {len(df)} companies from gem file")
        print(f"Columns: {list(df.columns)}")
        return df

    def load_prices_data(self) -> pd.DataFrame:
        """Load monthly prices CSV for market cap data."""
        if not self.prices_file.exists():
            print(f"Warning: Prices file not found: {self.prices_file}")
            return pd.DataFrame()

        print(f"Loading prices data from: {self.prices_file}")
        df = pd.read_csv(self.prices_file)

        # Get latest market cap by symbol
        if 'date' in df.columns:
            df['date'] = pd.to_datetime(df['date'])
            latest = df.sort_values('date').groupby('symbol').tail(1)
            return latest[['symbol', 'mcap']].rename(columns={'symbol': 'CD_NSE Symbol1', 'mcap': 'latest_mcap'})
        return pd.DataFrame()

    def rank_companies_for_migration(self, df: pd.DataFrame, target_count: int = 1000) -> pd.DataFrame:
        """
        Rank and select companies for migration.

        Strategy:
        1. Filter investable=True (2,160 candidates)
        2. Merge with market cap data
        3. Ensure balanced valuation_group distribution (50+ per major group)
        4. Prioritize by: market cap, data quality, NSE listing
        5. Select top N
        """
        # Filter investable
        investable = df[df['investable'] == True].copy()
        print(f"\n{len(investable)} investable companies")

        # Merge with prices for market cap
        prices = self.load_prices_data()
        if not prices.empty:
            investable = investable.merge(prices, on='CD_NSE Symbol1', how='left')
            print(f"Market cap data available for {investable['latest_mcap'].notna().sum()} companies")
        else:
            investable['latest_mcap'] = 0

        # Calculate data quality score
        investable['data_quality'] = 0
        investable['data_quality'] += investable['CD_NSE Symbol1'].notna().astype(int) * 10  # NSE listing
        if '2025_sales' in investable.columns:
            investable['data_quality'] += investable['2025_sales'].notna().astype(int) * 5  # Recent sales
        if '2025_pat' in investable.columns:
            investable['data_quality'] += investable['2025_pat'].notna().astype(int) * 3  # Recent profit
        if 'latest_mcap' in investable.columns:
            investable['data_quality'] += investable['latest_mcap'].gt(0).astype(int) * 5  # Market cap available

        # Balanced selection by valuation_group
        selected = []

        # Get group counts (exclude NOT_CLASSIFIED)
        groups = investable[investable['valuation_group'] != 'NOT_CLASSIFIED']['valuation_group'].value_counts()
        print(f"\nValuation groups: {len(groups)}")

        # Calculate target per group (with minimum 30 for small groups)
        min_per_group = 30
        remaining = target_count - (len(groups) * min_per_group)

        for group, count in groups.items():
            group_df = investable[investable['valuation_group'] == group].copy()

            # Calculate allocation: minimum + proportional share
            proportional = int(remaining * (count / investable['valuation_group'].value_counts().sum()))
            target_for_group = min_per_group + proportional
            target_for_group = min(target_for_group, len(group_df))  # Can't exceed available

            # Rank within group by mcap and data quality
            group_df['rank_score'] = (
                group_df['latest_mcap'].fillna(0) / 1e9 +  # Normalize mcap (in billions)
                group_df['data_quality'] * 100  # Weight data quality highly
            )

            top_n = group_df.nlargest(target_for_group, 'rank_score')
            selected.append(top_n)
            print(f"{group:30s}: {target_for_group:4d} selected (from {count} available)")

        result = pd.concat(selected, ignore_index=True)
        result = result.nlargest(target_count, 'rank_score')  # Trim to exact target

        print(f"\nTotal selected: {len(result)}")
        return result

    def get_company_id_from_mssdb(self, nse_symbol: str = None, accord_code: str = None) -> int:
        """Get marketscrip_id from mssdb by NSE symbol or accord code."""
        if nse_symbol:
            # Try symbol column first
            result = self.mysql.query_one(
                "SELECT marketscrip_id FROM mssdb.kbapp_marketscrip WHERE symbol = %s LIMIT 1",
                (nse_symbol,)
            )
            if result:
                return result['marketscrip_id']

            # Try alternate_symbol
            result = self.mysql.query_one(
                "SELECT marketscrip_id FROM mssdb.kbapp_marketscrip WHERE alternate_symbol = %s LIMIT 1",
                (nse_symbol,)
            )
            if result:
                return result['marketscrip_id']

        if accord_code:
            result = self.mysql.query_one(
                "SELECT marketscrip_id FROM mssdb.kbapp_marketscrip WHERE accord_code = %s LIMIT 1",
                (accord_code,)
            )
            if result:
                return result['marketscrip_id']

        return None

    def migrate_from_gem(self, target_count: int = 1000, dry_run: bool = True) -> Dict:
        """
        Migrate companies from gem classification file.

        Returns: Migration summary dict
        """
        print(f"\n{'='*80}")
        print(f"Company Migration from GEM Classification")
        print(f"Target: {target_count} companies")
        print(f"Mode: {'DRY RUN' if dry_run else 'EXECUTE'}")
        print(f"{'='*80}\n")

        # Load and rank
        gem_df = self.load_gem_classification()
        selected = self.rank_companies_for_migration(gem_df, target_count)

        # Assign valuation_frequency by market cap
        selected['valuation_frequency'] = 'MONTHLY'  # Default
        selected.loc[selected['rank_score'].nlargest(100).index, 'valuation_frequency'] = 'DAILY'
        selected.loc[selected['rank_score'].nlargest(500).index[100:], 'valuation_frequency'] = 'WEEKLY'

        print(f"\nValuation frequency distribution:")
        print(selected['valuation_frequency'].value_counts())

        # Migration summary
        summary = {
            'total_selected': len(selected),
            'dry_run': dry_run,
            'timestamp': datetime.now().isoformat(),
            'by_group': {},
            'by_frequency': selected['valuation_frequency'].value_counts().to_dict(),
            'inserted': 0,
            'updated': 0,
            'skipped': 0,
            'errors': []
        }

        if dry_run:
            print(f"\nDRY RUN: Would insert {len(selected)} companies")
            print("\nSample (first 5):")
            print(selected[['Company Name', 'CD_NSE Symbol1', 'valuation_group', 'valuation_subgroup', 'valuation_frequency']].head())
            return summary

        # Execute migration
        print(f"\nExecuting migration...")
        for idx, row in selected.iterrows():
            try:
                # Get company_id from mssdb
                company_id = self.get_company_id_from_mssdb(
                    nse_symbol=row.get('CD_NSE Symbol1'),
                    accord_code=str(row.get('Accord Code', ''))
                )

                if not company_id:
                    summary['skipped'] += 1
                    summary['errors'].append(f"No company_id for {row.get('Company Name')}")
                    continue

                # Check if already exists
                existing = self.mysql.query_one(
                    "SELECT id FROM vs_active_companies WHERE company_id = %s",
                    (company_id,)
                )

                data = {
                    'company_id': company_id,
                    'nse_symbol': row.get('CD_NSE Symbol1'),
                    'company_name': row.get('Company Name'),
                    'csv_name': row.get('Company Name'),  # Will be verified later
                    'bse_code': str(row.get('CD_Bse Scrip ID', '')),
                    'accord_code': str(row.get('Accord Code', '')),
                    'valuation_group': row.get('valuation_group'),
                    'valuation_subgroup': row.get('valuation_subgroup'),
                    'cd_sector': row.get('CD_Sector'),
                    'cd_industry': row.get('CD_Industry1'),
                    'sector': row.get('valuation_group'),  # Legacy field
                    'industry': row.get('valuation_subgroup'),  # Legacy field
                    'valuation_frequency': row.get('valuation_frequency'),
                    'priority': 5,  # Default
                    'is_active': 1,
                    'added_date': date.today(),
                    'added_by': 'gem_migration'
                }

                if existing:
                    # Update
                    self.mysql.execute(
                        """UPDATE vs_active_companies SET
                           nse_symbol = %s, company_name = %s, csv_name = %s,
                           bse_code = %s, accord_code = %s,
                           valuation_group = %s, valuation_subgroup = %s,
                           cd_sector = %s, cd_industry = %s,
                           sector = %s, industry = %s,
                           valuation_frequency = %s, is_active = 1,
                           last_synced = NOW()
                           WHERE company_id = %s""",
                        (data['nse_symbol'], data['company_name'], data['csv_name'],
                         data['bse_code'], data['accord_code'],
                         data['valuation_group'], data['valuation_subgroup'],
                         data['cd_sector'], data['cd_industry'],
                         data['sector'], data['industry'],
                         data['valuation_frequency'], company_id)
                    )
                    summary['updated'] += 1
                else:
                    # Insert
                    self.mysql.insert('vs_active_companies', data)
                    summary['inserted'] += 1

                # Track by group
                group = data['valuation_group']
                summary['by_group'][group] = summary['by_group'].get(group, 0) + 1

                if (summary['inserted'] + summary['updated']) % 100 == 0:
                    print(f"  Processed {summary['inserted'] + summary['updated']} companies...")

            except Exception as e:
                summary['errors'].append(f"Error for {row.get('Company Name')}: {str(e)}")
                summary['skipped'] += 1

        print(f"\n{'='*80}")
        print(f"Migration Summary:")
        print(f"  Inserted: {summary['inserted']}")
        print(f"  Updated:  {summary['updated']}")
        print(f"  Skipped:  {summary['skipped']}")
        print(f"  Errors:   {len(summary['errors'])}")
        print(f"{'='*80}\n")

        if summary['errors']:
            print(f"First 10 errors:")
            for err in summary['errors'][:10]:
                print(f"  - {err}")

        return summary

    def migrate_pilot_alpha_configs(self) -> Dict:
        """Migrate Aether + Eicher alpha configs from companies.yaml."""
        print(f"\n{'='*80}")
        print(f"Migrating Pilot Alpha Configs from YAML")
        print(f"{'='*80}\n")

        if not self.companies_yaml.exists():
            print(f"Warning: {self.companies_yaml} not found")
            return {'error': 'YAML file not found'}

        with open(self.companies_yaml, 'r') as f:
            config = yaml.safe_load(f)

        companies_config = config.get('companies', {})
        summary = {'migrated': 0, 'errors': []}

        for key, company_data in companies_config.items():
            try:
                nse_symbol = company_data.get('nse_symbol')
                if not nse_symbol:
                    continue

                # Get company_id
                result = self.mysql.query_one(
                    "SELECT company_id FROM vs_active_companies WHERE nse_symbol = %s",
                    (nse_symbol,)
                )

                if not result:
                    summary['errors'].append(f"{nse_symbol} not found in vs_active_companies")
                    continue

                company_id = result['company_id']

                # Extract alpha data
                alpha_thesis = company_data.get('alpha_thesis', {})
                alpha_drivers = company_data.get('alpha_drivers', {})
                sector_overrides = company_data.get('sector_specific_overrides', {})

                # Insert or update
                existing = self.mysql.query_one(
                    "SELECT id FROM vs_company_alpha_configs WHERE company_id = %s",
                    (company_id,)
                )

                data = {
                    'company_id': company_id,
                    'thesis_bull': alpha_thesis.get('bull', ''),
                    'thesis_bear': alpha_thesis.get('bear', ''),
                    'thesis_key_moat': alpha_thesis.get('key_moat', ''),
                    'alpha_drivers': json.dumps(alpha_drivers),
                    'sector_overrides': json.dumps(sector_overrides),
                    'created_by': 'yaml_migration',
                    'notes': f'Migrated from companies.yaml on {datetime.now().date()}'
                }

                if existing:
                    # Update
                    config_id = existing['id']
                    self.mysql.execute(
                        """UPDATE vs_company_alpha_configs SET
                           thesis_bull = %s, thesis_bear = %s, thesis_key_moat = %s,
                           alpha_drivers = %s, sector_overrides = %s,
                           updated_at = NOW()
                           WHERE company_id = %s""",
                        (data['thesis_bull'], data['thesis_bear'], data['thesis_key_moat'],
                         data['alpha_drivers'], data['sector_overrides'], company_id)
                    )
                else:
                    config_id = self.mysql.insert('vs_company_alpha_configs', data)

                # Link to vs_active_companies
                self.mysql.execute(
                    "UPDATE vs_active_companies SET alpha_config_id = %s WHERE company_id = %s",
                    (config_id, company_id)
                )

                summary['migrated'] += 1
                print(f"✓ Migrated alpha config for {nse_symbol}")

            except Exception as e:
                summary['errors'].append(f"Error for {key}: {str(e)}")

        print(f"\nSummary: {summary['migrated']} configs migrated")
        if summary['errors']:
            print(f"Errors: {len(summary['errors'])}")
            for err in summary['errors']:
                print(f"  - {err}")

        return summary

    def validate_migration(self) -> Dict:
        """Validate migration results."""
        print(f"\n{'='*80}")
        print(f"Migration Validation")
        print(f"{'='*80}\n")

        checks = {}

        # Check 1: Total companies
        total = self.mysql.query_one("SELECT COUNT(*) as cnt FROM vs_active_companies WHERE is_active = 1")
        checks['total_active'] = total['cnt']
        print(f"✓ Total active companies: {total['cnt']}")

        # Check 2: Group distribution
        groups = self.mysql.query("""
            SELECT valuation_group, COUNT(*) as cnt
            FROM vs_active_companies
            WHERE is_active = 1
            GROUP BY valuation_group
            ORDER BY valuation_group
        """)
        checks['group_distribution'] = {g['valuation_group']: g['cnt'] for g in groups}
        print(f"\n✓ Group distribution:")
        for g in groups:
            print(f"  {g['valuation_group']:30s}: {g['cnt']:4d}")

        # Check 3: Alpha configs
        alpha = self.mysql.query_one("SELECT COUNT(*) as cnt FROM vs_company_alpha_configs")
        checks['alpha_configs'] = alpha['cnt']
        print(f"\n✓ Alpha configs: {alpha['cnt']}")

        # Check 4: Valuation group configs
        group_configs = self.mysql.query_one(
            "SELECT COUNT(*) as total, SUM(is_active) as active FROM vs_valuation_group_configs"
        )
        checks['group_configs'] = {
            'total': group_configs['total'],
            'active': int(group_configs['active'])
        }
        print(f"\n✓ Valuation group configs: {group_configs['total']} total, {group_configs['active']} active")

        # Check 5: Data quality
        missing_nse = self.mysql.query_one(
            "SELECT COUNT(*) as cnt FROM vs_active_companies WHERE is_active = 1 AND (nse_symbol IS NULL OR nse_symbol = '')"
        )
        checks['missing_nse_symbol'] = missing_nse['cnt']
        print(f"\n✓ Missing NSE symbols: {missing_nse['cnt']}")

        missing_group = self.mysql.query_one(
            "SELECT COUNT(*) as cnt FROM vs_active_companies WHERE is_active = 1 AND valuation_group IS NULL"
        )
        checks['missing_valuation_group'] = missing_group['cnt']
        print(f"✓ Missing valuation_group: {missing_group['cnt']}")

        # Check 6: Frequency distribution
        freq = self.mysql.query("""
            SELECT valuation_frequency, COUNT(*) as cnt
            FROM vs_active_companies
            WHERE is_active = 1
            GROUP BY valuation_frequency
        """)
        checks['frequency_distribution'] = {f['valuation_frequency']: f['cnt'] for f in freq}
        print(f"\n✓ Frequency distribution:")
        for f in freq:
            print(f"  {f['valuation_frequency']:10s}: {f['cnt']:4d}")

        print(f"\n{'='*80}")

        # Summary assessment
        passed = True
        if checks['total_active'] < 900:
            print(f"⚠️  Warning: Only {checks['total_active']} companies (target 1000)")
            passed = False
        if checks['group_configs']['active'] < 12:
            print(f"⚠️  Warning: Only {checks['group_configs']['active']} active groups (expected 12)")
            passed = False
        if checks['missing_nse_symbol'] > 50:
            print(f"⚠️  Warning: {checks['missing_nse_symbol']} companies missing NSE symbol")
            passed = False

        if passed:
            print(f"✅ All validation checks passed!")

        return checks


def main():
    parser = argparse.ArgumentParser(description='Migrate companies from gem classification file')
    parser.add_argument('--source', default='gem', choices=['gem'], help='Source of company data')
    parser.add_argument('--target', type=int, default=1000, help='Target number of companies')
    parser.add_argument('--dry-run', action='store_true', help='Dry run (no database changes)')
    parser.add_argument('--execute', action='store_true', help='Execute migration (write to database)')
    parser.add_argument('--pilot-alpha-only', action='store_true', help='Only migrate pilot alpha configs')
    parser.add_argument('--validate', action='store_true', help='Validate migration results')

    args = parser.parse_args()

    # Get MySQL client
    mysql_client = ValuationMySQLClient.get_instance()
    migrator = CompanyMigrator(mysql_client)

    if args.pilot_alpha_only:
        migrator.migrate_pilot_alpha_configs()
    elif args.validate:
        migrator.validate_migration()
    else:
        migrator.migrate_from_gem(
            target_count=args.target,
            dry_run=not args.execute
        )


if __name__ == '__main__':
    main()
