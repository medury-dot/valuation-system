"""
Google Sheets Unified Client for 1000+ Companies
Redesigned for scalability - 6 unified sheets instead of per-company sheets

Sheet Structure (1.5M cells total, 15% of 10M limit):
1. Macro Drivers (10 rows)
2. Valuation Group Drivers (221 rows for 13 groups)
3. Valuation Subgroup Drivers (1,276 rows for 52 subgroups)
4. Company Drivers (22,001 rows for 1000 companies × 22 drivers)
5. Valuation History (30,001 rows, 30-day rolling window)
6. Driver History (60,001 rows, change audit trail)
7. Event Log (30,001 rows, material news)
"""

import os
import logging
from datetime import datetime, timedelta
from typing import List, Dict, Optional
import time

import gspread
from gspread.exceptions import APIError
from dotenv import load_dotenv

logger = logging.getLogger(__name__)

load_dotenv(os.path.join(os.path.dirname(__file__), '..', 'config', '.env'))


class GSheetUnifiedClient:
    """
    Unified Google Sheets client for 1000+ companies.
    Uses batch operations for performance.
    """

    SHEET_CONFIGS = {
        'macro': {
            'title': '1. Macro Drivers',
            'rows': 15,
            'cols': 12,
            'headers': ['driver_id', 'category', 'driver_name', 'current_value', 'bull_value', 'base_value',
                       'bear_value', 'weight', 'metric', 'impact', 'trend', 'last_updated']
        },
        'group': {
            'title': '2. Valuation Group Drivers',
            'rows': 250,
            'cols': 10,
            'headers': ['valuation_group', 'category', 'driver_name', 'current_value', 'weight',
                       'metric', 'impact', 'trend', 'last_updated', 'source']
        },
        'subgroup': {
            'title': '3. Valuation Subgroup Drivers',
            'rows': 1300,
            'cols': 14,
            'headers': ['valuation_subgroup', 'valuation_group', 'category', 'driver_name', 'current', 'bull', 'base', 'bear',
                       'weight', 'metric', 'impact', 'trend', 'last_updated', 'source']
        },
        'company': {
            'title': '4. Company Drivers',
            'rows': 23000,
            'cols': 16,
            'headers': ['driver_id', 'company_id', 'company_name', 'nse_symbol', 'sector', 'category',
                       'driver_name', 'current', 'bull', 'base', 'bear',
                       'weight', 'metric', 'vs_peers', 'trend', 'last_updated']
        },
        'activity': {
            'title': '5. Recent Activity',
            'rows': 31000,
            'cols': 11,
            'headers': ['ID', 'Symbol', 'Company', 'Val Date', 'Method', 'Scenario',
                       'Intrinsic', 'CMP', 'Upside %', 'Created At', 'Created By']
        },
        'active_companies': {
            'title': '6. Active Companies',
            'rows': 5000,
            'cols': 10,
            'headers': ['company_id', 'nse_symbol', 'company_name', 'sector', 'industry',
                       'valuation_group', 'valuation_subgroup', 'is_active', 'priority', 'notes']
        }
    }

    def __init__(self, spreadsheet_id: str = None):
        self.spreadsheet_id = spreadsheet_id or os.getenv('GSHEET_DRIVERS_ID')
        self.auth_path = os.getenv('GSHEET_AUTH_PATH', '')
        self._client = None
        self._spreadsheet = None

    @property
    def client(self):
        """Lazy init gspread client."""
        if self._client is None:
            try:
                self._client = gspread.service_account(filename=self.auth_path)
                logger.info("Google Sheets unified client authenticated")
            except Exception as e:
                logger.error(f"Failed to auth Google Sheets: {e}")
                raise
        return self._client

    @property
    def spreadsheet(self):
        """Lazy open spreadsheet."""
        if self._spreadsheet is None:
            if not self.spreadsheet_id:
                raise ValueError("GSHEET_DRIVERS_ID not set in .env")
            self._spreadsheet = self.client.open_by_key(self.spreadsheet_id)
            logger.info(f"Opened spreadsheet: {self._spreadsheet.title}")
        return self._spreadsheet

    def init_6sheet_structure(self) -> Dict[str, str]:
        """
        Initialize 6 unified sheets with proper headers and formatting.

        Returns:
            Dict mapping sheet keys to GIDs
        """
        logger.info("Initializing 6-sheet structure...")

        existing_sheets = {ws.title: ws for ws in self.spreadsheet.worksheets()}
        result = {}

        for key, config in self.SHEET_CONFIGS.items():
            title = config['title']

            if title in existing_sheets:
                ws = existing_sheets[title]
                logger.info(f"Sheet exists: {title}, resizing...")
                ws.resize(rows=config['rows'], cols=config['cols'])
            else:
                logger.info(f"Creating sheet: {title}")
                ws = self.spreadsheet.add_worksheet(
                    title=title,
                    rows=config['rows'],
                    cols=config['cols']
                )

            # Set headers
            ws.update('A1', [config['headers']], value_input_option='RAW')

            # Format headers (bold, freeze row 1)
            ws.format('A1:Z1', {
                'textFormat': {'bold': True},
                'backgroundColor': {'red': 0.9, 'green': 0.9, 'blue': 0.9}
            })
            ws.freeze(rows=1)

            result[key] = ws.id
            logger.info(f"✓ {title}: {config['rows']} rows × {config['cols']} cols")

            # Rate limit protection
            time.sleep(1)

        total_cells = sum(cfg['rows'] * cfg['cols'] for cfg in self.SHEET_CONFIGS.values())
        logger.info(f"Total cells: {total_cells:,} ({total_cells/10_000_000*100:.1f}% of 10M limit)")

        return result

    def batch_update_company_drivers(self, drivers: List[Dict], mysql_client=None) -> int:
        """
        Batch update company drivers sheet.

        Args:
            drivers: List of driver dicts with keys matching headers
            mysql_client: Optional MySQL client for parallel DB update

        Returns:
            Number of rows updated
        """
        if not drivers:
            return 0

        ws = self._get_sheet('company')

        # Build rows
        header = self.SHEET_CONFIGS['company']['headers']
        rows = []

        for driver in drivers:
            row = [driver.get(col, '') for col in header]
            rows.append(row)

        # Clear existing data (keep headers)
        ws.clear()
        ws.update('A1', [header] + rows, value_input_option='RAW')

        logger.info(f"Batch updated {len(rows)} company driver rows")

        # Parallel MySQL sync if client provided
        if mysql_client:
            self._sync_to_mysql(drivers, mysql_client, table='vs_drivers')

        return len(rows)

    def batch_update_group_drivers(self, drivers: List[Dict]) -> int:
        """Batch update valuation group drivers."""
        if not drivers:
            return 0

        ws = self._get_sheet('group')
        header = self.SHEET_CONFIGS['group']['headers']

        rows = [[driver.get(col, '') for col in header] for driver in drivers]

        ws.clear()
        ws.update('A1', [header] + rows, value_input_option='RAW')

        logger.info(f"Batch updated {len(rows)} group driver rows")
        return len(rows)

    def sync_drivers_from_mysql(self, mysql_client, full_refresh: bool = False):
        """
        Daily MySQL → GSheet sync (6 AM IST).

        Args:
            mysql_client: ValuationMySQLClient instance
            full_refresh: If True, sync all drivers; else only changed today
        """
        logger.info(f"Syncing drivers from MySQL (full_refresh={full_refresh})")

        # Sync macro drivers
        macro = mysql_client.query("SELECT * FROM vs_drivers WHERE driver_type = 'macro'")
        if macro:
            self._update_macro_drivers(macro)

        # Sync group drivers
        groups = mysql_client.query("""
            SELECT d.*, vgc.valuation_group
            FROM vs_drivers d
            JOIN vs_valuation_group_configs vgc ON d.sector_key = vgc.valuation_group
            WHERE d.driver_type = 'sector'
        """)
        if groups:
            self.batch_update_group_drivers(groups)

        # Sync company drivers
        where_clause = "" if full_refresh else "WHERE DATE(d.updated_at) = CURDATE()"
        companies = mysql_client.query(f"""
            SELECT d.*, ac.nse_symbol, ac.company_name, ac.valuation_group, ac.valuation_subgroup
            FROM vs_drivers d
            JOIN vs_active_companies ac ON d.company_id = ac.company_id
            WHERE d.driver_type = 'company' {where_clause}
        """)
        if companies:
            self.batch_update_company_drivers(companies)

        logger.info("Sync complete")

    def sync_drivers_from_gsheet(self, mysql_client) -> Dict[str, int]:
        """
        GSheet → MySQL sync (GSheet is source of truth for drivers).
        Called after PM edits drivers in GSheet.

        Returns:
            Dict with counts: {macro: N, group: N, subgroup: N, company: N}
        """
        logger.info("Syncing drivers from GSheet to MySQL (GSheet is source of truth)")
        counts = {'macro': 0, 'group': 0, 'subgroup': 0, 'company': 0}

        # 1. Sync MACRO drivers from Sheet 1
        try:
            ws = self._get_sheet('macro')
            rows = ws.get_all_records()
            for row in rows:
                if not row.get('driver_name'):
                    continue
                # Handle both 'current' and 'current_value' column names
                current = row.get('current_value') or row.get('current', '')
                self._upsert_driver(mysql_client, {
                    'driver_level': 'MACRO',
                    'driver_category': row.get('category', ''),
                    'driver_name': row.get('driver_name'),
                    'current_value': str(current),
                    'weight': self._parse_float(row.get('weight')),
                    'impact_direction': self._normalize_direction(row.get('impact', row.get('trend'))),
                    'trend': self._normalize_trend(row.get('trend')),
                    'updated_by': 'GSHEET',
                })
                counts['macro'] += 1
        except Exception as e:
            logger.error(f"Failed to sync macro drivers: {e}")

        # 2. Sync GROUP drivers from Sheet 2
        try:
            ws = self._get_sheet('group')
            rows = ws.get_all_records()
            for row in rows:
                if not row.get('driver_name') or not row.get('valuation_group'):
                    continue
                # Handle both 'current' and 'current_value' column names
                current = row.get('current_value') or row.get('current', '')
                self._upsert_driver(mysql_client, {
                    'driver_level': 'GROUP',
                    'driver_category': row.get('category', ''),
                    'driver_name': row.get('driver_name'),
                    'valuation_group': row.get('valuation_group'),
                    'sector': row.get('valuation_group'),  # For backward compat
                    'current_value': str(current),
                    'weight': self._parse_float(row.get('weight')),
                    'impact_direction': self._normalize_direction(row.get('impact', row.get('trend'))),
                    'trend': self._normalize_trend(row.get('trend')),
                    'updated_by': 'GSHEET',
                })
                counts['group'] += 1
        except Exception as e:
            logger.error(f"Failed to sync group drivers: {e}")

        # 3. Sync SUBGROUP drivers from Sheet 3
        try:
            ws = self._get_sheet('subgroup')
            rows = ws.get_all_records()
            for row in rows:
                if not row.get('driver_name') or not row.get('valuation_subgroup'):
                    continue
                # Handle both 'current' and 'current_value' column names
                current = row.get('current_value') or row.get('current', '')
                self._upsert_driver(mysql_client, {
                    'driver_level': 'SUBGROUP',
                    'driver_category': row.get('category', ''),
                    'driver_name': row.get('driver_name'),
                    'valuation_group': row.get('valuation_group', ''),  # Parent group for traceability
                    'valuation_subgroup': row.get('valuation_subgroup'),
                    'current_value': str(current),
                    'weight': self._parse_float(row.get('weight')),
                    'impact_direction': self._normalize_direction(row.get('impact', row.get('trend'))),
                    'trend': self._normalize_trend(row.get('trend')),
                    'updated_by': 'GSHEET',
                })
                counts['subgroup'] += 1
        except Exception as e:
            logger.error(f"Failed to sync subgroup drivers: {e}")

        # 4. Sync COMPANY drivers from Sheet 4
        try:
            ws = self._get_sheet('company')
            rows = ws.get_all_records()
            for row in rows:
                company_id = row.get('company_id')
                if not row.get('driver_name') or not company_id:
                    continue
                self._upsert_driver(mysql_client, {
                    'driver_level': 'COMPANY',
                    'driver_category': row.get('category', ''),
                    'driver_name': row.get('driver_name'),
                    'company_id': company_id,
                    'valuation_group': row.get('valuation_group'),
                    'valuation_subgroup': row.get('valuation_subgroup'),
                    'current_value': str(row.get('current', '')),
                    'weight': self._parse_float(row.get('weight')),
                    'impact_direction': self._normalize_direction(row.get('trend')),
                    'trend': self._normalize_trend(row.get('trend')),
                    'updated_by': 'GSHEET',
                })
                counts['company'] += 1
        except Exception as e:
            logger.error(f"Failed to sync company drivers: {e}")

        logger.info(f"GSheet → MySQL sync complete: {counts}")
        return counts

    def _upsert_driver(self, mysql_client, driver: Dict):
        """Insert or update a driver in vs_drivers."""
        level = driver.get('driver_level')
        name = driver.get('driver_name')
        group = driver.get('valuation_group')
        subgroup = driver.get('valuation_subgroup')
        company_id = driver.get('company_id')

        # Build WHERE clause for checking existence
        if level == 'MACRO':
            existing = mysql_client.query_one(
                "SELECT id FROM vs_drivers WHERE driver_level='MACRO' AND driver_name=%s",
                (name,)
            )
        elif level == 'GROUP':
            existing = mysql_client.query_one(
                "SELECT id FROM vs_drivers WHERE driver_level='GROUP' AND driver_name=%s AND valuation_group=%s",
                (name, group)
            )
        elif level == 'SUBGROUP':
            existing = mysql_client.query_one(
                "SELECT id FROM vs_drivers WHERE driver_level='SUBGROUP' AND driver_name=%s AND valuation_subgroup=%s",
                (name, subgroup)
            )
        else:  # COMPANY
            existing = mysql_client.query_one(
                "SELECT id FROM vs_drivers WHERE driver_level='COMPANY' AND driver_name=%s AND company_id=%s",
                (name, company_id)
            )

        if existing:
            # Update
            mysql_client.execute(
                """UPDATE vs_drivers SET
                   current_value=%s, weight=%s, impact_direction=%s, trend=%s,
                   updated_by=%s, last_updated=NOW()
                   WHERE id=%s""",
                (driver.get('current_value'), driver.get('weight'),
                 driver.get('impact_direction'), driver.get('trend'),
                 driver.get('updated_by', 'GSHEET'), existing['id'])
            )
        else:
            # Insert
            mysql_client.insert('vs_drivers', driver)

    def _parse_float(self, val) -> Optional[float]:
        """Parse float from GSheet cell, handling empty/string values."""
        if val is None or val == '':
            return None
        try:
            return float(val)
        except (ValueError, TypeError):
            return None

    def _normalize_direction(self, val) -> Optional[str]:
        """Normalize impact direction to POSITIVE/NEGATIVE/NEUTRAL."""
        if not val:
            return 'NEUTRAL'
        val_upper = str(val).upper().strip()
        if val_upper in ('POSITIVE', 'UP', '+', 'BULLISH'):
            return 'POSITIVE'
        elif val_upper in ('NEGATIVE', 'DOWN', '-', 'BEARISH'):
            return 'NEGATIVE'
        return 'NEUTRAL'

    def _normalize_trend(self, val) -> Optional[str]:
        """Normalize trend to UP/DOWN/STABLE."""
        if not val:
            return 'STABLE'
        val_upper = str(val).upper().strip()
        if val_upper in ('UP', 'RISING', 'POSITIVE', '+'):
            return 'UP'
        elif val_upper in ('DOWN', 'FALLING', 'NEGATIVE', '-'):
            return 'DOWN'
        return 'STABLE'

    def detect_pm_edits(self, mysql_client) -> List[Dict]:
        """
        Poll GSheet for PM edits across ALL driver sheets, sync to MySQL (every 5 mins).
        Checks: Macro, Group, Subgroup, and Company driver sheets.

        Returns:
            List of detected changes
        """
        changes = []

        # Check MACRO drivers (Sheet 1)
        changes.extend(self._detect_edits_for_level(mysql_client, 'macro', 'MACRO'))

        # Check GROUP drivers (Sheet 2)
        changes.extend(self._detect_edits_for_level(mysql_client, 'group', 'GROUP'))

        # Check SUBGROUP drivers (Sheet 3)
        changes.extend(self._detect_edits_for_level(mysql_client, 'subgroup', 'SUBGROUP'))

        # Check COMPANY drivers (Sheet 4)
        changes.extend(self._detect_edits_for_level(mysql_client, 'company', 'COMPANY'))

        if changes:
            logger.info(f"Detected {len(changes)} PM edits across all driver sheets, synced to MySQL")

        return changes

    def _detect_edits_for_level(self, mysql_client, sheet_key: str, driver_level: str) -> List[Dict]:
        """Detect PM edits for a specific driver level sheet."""
        changes = []

        try:
            ws = self._get_sheet(sheet_key)
            rows = ws.get_all_records()
        except Exception as e:
            logger.warning(f"Failed to read {sheet_key} sheet: {e}")
            return changes

        for row in rows:
            driver_name = row.get('driver_name')
            current_value = str(row.get('current', ''))

            if not driver_name:
                continue

            # Build lookup query based on level
            if driver_level == 'MACRO':
                db_row = mysql_client.query_one(
                    "SELECT id, current_value FROM vs_drivers WHERE driver_level='MACRO' AND driver_name=%s",
                    (driver_name,)
                )
            elif driver_level == 'GROUP':
                valuation_group = row.get('valuation_group')
                if not valuation_group:
                    continue
                db_row = mysql_client.query_one(
                    "SELECT id, current_value FROM vs_drivers WHERE driver_level='GROUP' AND driver_name=%s AND valuation_group=%s",
                    (driver_name, valuation_group)
                )
            elif driver_level == 'SUBGROUP':
                valuation_subgroup = row.get('valuation_subgroup')
                if not valuation_subgroup:
                    continue
                db_row = mysql_client.query_one(
                    "SELECT id, current_value FROM vs_drivers WHERE driver_level='SUBGROUP' AND driver_name=%s AND valuation_subgroup=%s",
                    (driver_name, valuation_subgroup)
                )
            else:  # COMPANY
                company_id = row.get('company_id')
                if not company_id:
                    continue
                db_row = mysql_client.query_one(
                    "SELECT id, current_value FROM vs_drivers WHERE driver_level='COMPANY' AND driver_name=%s AND company_id=%s",
                    (driver_name, company_id)
                )

            if db_row and str(db_row.get('current_value', '')) != current_value:
                # PM edited this value
                change = {
                    'driver_level': driver_level,
                    'driver_name': driver_name,
                    'old_value': db_row['current_value'],
                    'new_value': current_value,
                    'changed_by': 'PM',
                    'timestamp': datetime.now(),
                }

                # Add context based on level
                if driver_level == 'GROUP':
                    change['valuation_group'] = row.get('valuation_group')
                elif driver_level == 'SUBGROUP':
                    change['valuation_subgroup'] = row.get('valuation_subgroup')
                elif driver_level == 'COMPANY':
                    change['company_id'] = row.get('company_id')

                changes.append(change)

                # Update MySQL
                mysql_client.execute(
                    """UPDATE vs_drivers SET
                       current_value = %s,
                       last_updated = NOW(),
                       updated_by = 'PM'
                       WHERE id = %s""",
                    (current_value, db_row['id'])
                )

                # Log to driver_changelog
                changelog_entry = {
                    'driver_level': driver_level,
                    'driver_name': driver_name,
                    'old_value': db_row['current_value'],
                    'new_value': current_value,
                    'triggered_by': 'PM_OVERRIDE',
                    'change_reason': 'Manual edit via GSheet',
                    'change_timestamp': datetime.now(),
                }
                if driver_level == 'GROUP':
                    changelog_entry['valuation_group'] = row.get('valuation_group')
                elif driver_level == 'SUBGROUP':
                    changelog_entry['valuation_subgroup'] = row.get('valuation_subgroup')
                elif driver_level == 'COMPANY':
                    changelog_entry['company_id'] = row.get('company_id')

                mysql_client.insert('vs_driver_changelog', changelog_entry)

        return changes

    def archive_history_to_mysql(self, mysql_client, days_to_keep: int = 30):
        """
        Archive rows >30 days from History sheets to MySQL, delete from GSheet.

        Args:
            mysql_client: MySQL client
            days_to_keep: Keep only last N days in GSheet
        """
        cutoff = datetime.now() - timedelta(days=days_to_keep)

        # Archive valuation history
        ws = self._get_sheet('history')
        rows = ws.get_all_records()

        to_archive = [r for r in rows if datetime.fromisoformat(r['date']) < cutoff]

        if to_archive:
            # Bulk insert to MySQL
            for row in to_archive:
                mysql_client.insert('vs_valuation_history_archive', row)

            # Delete from GSheet (keep recent only)
            recent = [r for r in rows if datetime.fromisoformat(r['date']) >= cutoff]
            ws.clear()
            ws.update('A1', [self.SHEET_CONFIGS['history']['headers']] + recent)

            logger.info(f"Archived {len(to_archive)} valuation history rows to MySQL")

        # Similar for driver_changes and events
        # ... (implementation omitted for brevity)

    def _get_sheet(self, key: str):
        """Get worksheet by config key."""
        title = self.SHEET_CONFIGS[key]['title']
        return self.spreadsheet.worksheet(title)

    def _update_macro_drivers(self, drivers: List[Dict]):
        """Update macro drivers sheet."""
        ws = self._get_sheet('macro')
        header = self.SHEET_CONFIGS['macro']['headers']
        rows = [[d.get(col, '') for col in header] for d in drivers]

        ws.clear()
        ws.update('A1', [header] + rows, value_input_option='RAW')

        logger.info(f"Updated {len(rows)} macro drivers")

    def _sync_to_mysql(self, rows: List[Dict], mysql_client, table: str):
        """Parallel sync to MySQL (background operation)."""
        try:
            for row in rows:
                mysql_client.insert(table, row, on_duplicate='update')
        except Exception as e:
            logger.error(f"MySQL sync failed: {e}")

    def get_cell_count(self) -> Dict:
        """Calculate total cell usage."""
        total = 0
        by_sheet = {}

        for key, config in self.SHEET_CONFIGS.items():
            cells = config['rows'] * config['cols']
            total += cells
            by_sheet[config['title']] = cells

        return {
            'total_cells': total,
            'by_sheet': by_sheet,
            'percent_of_limit': total / 10_000_000 * 100,
            'limit': 10_000_000
        }


def main():
    """CLI for sheet operations."""
    import argparse
    import sys
    sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))

    parser = argparse.ArgumentParser(description='Google Sheets Unified Client')
    parser.add_argument('--init', action='store_true', help='Initialize 7-sheet structure')
    parser.add_argument('--validate', action='store_true', help='Validate cell count')
    parser.add_argument('--sync-from-gsheet', action='store_true',
                        help='Sync drivers from GSheet to MySQL (GSheet is source of truth)')
    parser.add_argument('--detect-edits', action='store_true',
                        help='Detect PM edits across all driver sheets')

    args = parser.parse_args()

    client = GSheetUnifiedClient()

    if args.init:
        client.init_6sheet_structure()
    elif args.validate:
        stats = client.get_cell_count()
        print(f"\nCell Usage:")
        print(f"  Total: {stats['total_cells']:,}")
        print(f"  Limit: {stats['limit']:,}")
        print(f"  Usage: {stats['percent_of_limit']:.1f}%")
        print(f"\nBy Sheet:")
        for title, count in stats['by_sheet'].items():
            print(f"  {title:30s}: {count:,}")
    elif args.sync_from_gsheet or args.detect_edits:
        # Need MySQL client for these operations
        from valuation_system.storage.mysql_client import ValuationMySQLClient
        mysql = ValuationMySQLClient.get_instance()

        if args.sync_from_gsheet:
            counts = client.sync_drivers_from_gsheet(mysql)
            print(f"\nSynced drivers from GSheet to MySQL:")
            print(f"  MACRO:    {counts['macro']}")
            print(f"  GROUP:    {counts['group']}")
            print(f"  SUBGROUP: {counts['subgroup']}")
            print(f"  COMPANY:  {counts['company']}")
        elif args.detect_edits:
            changes = client.detect_pm_edits(mysql)
            print(f"\nDetected {len(changes)} PM edits:")
            for c in changes[:10]:  # Show first 10
                print(f"  {c['driver_level']:10s} | {c['driver_name']:25s} | {c['old_value']} → {c['new_value']}")
            if len(changes) > 10:
                print(f"  ... and {len(changes) - 10} more")


if __name__ == '__main__':
    main()
