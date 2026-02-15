"""
Google Sheets Client for Driver Tracking (4-Level Hierarchy)
Reads/writes driver values, weights, and history to Google Sheets.
Provides collaborative interface for PM to view and override drivers.

Sheets Structure (10 tabs):
  Sheet 1: Macro Drivers (15% weight)
  Sheet 2: Valuation Group Drivers (20% weight)
  Sheet 3: Valuation Subgroup Drivers (35% weight)
  Sheet 4: Company Drivers (30% weight)
  Sheet 5: Recent Activity
  Sheet 6: Active Companies
  Sheet 7: Discovered Drivers (PM approval workflow)
  Sheet 8: News Events
  Sheet 9: Materiality Dashboard (read-only alerts)
  Sheet 10: Social Posts (Twitter + LinkedIn drafts for PM approval)
"""

import os
import logging
from datetime import datetime
from typing import Optional

import gspread
from dotenv import load_dotenv

from valuation_system.utils.resilience import retry_with_backoff, check_internet

logger = logging.getLogger(__name__)

load_dotenv(os.path.join(os.path.dirname(__file__), '..', 'config', '.env'))


class GSheetClient:
    """
    Google Sheets integration for driver tracking.
    Uses gspread with service account authentication.

    All driver values are maintained in Google Sheets as the
    source of truth for PM-editable parameters. The system
    reads from sheets before each valuation run and writes
    back updates from agent analysis.
    """

    SHEET_NAMES = {
        'macro_drivers': '1. Macro Drivers',
        'group_drivers': '2. Valuation Group Drivers',  # 4-level hierarchy: GROUP level
        'subgroup_drivers': '3. Valuation Subgroup Drivers',  # 4-level hierarchy: SUBGROUP level
        'company_drivers': '4. Company Drivers',  # 4-level hierarchy: COMPANY level
        'recent_activity': '5. Recent Activity',  # 7-day summary
        'active_companies': '6. Active Companies',
        'discovered_drivers': '7. Discovered Drivers',  # PM approval workflow
        'news_events': '8. News Events',  # Recent news intelligence from all sources
        'materiality_dashboard': '9. Materiality Dashboard',  # Read-only alerts dashboard
        'social_posts': '10. Social Posts',  # Twitter + LinkedIn drafts for PM approval
        # Legacy aliases (for backward compatibility):
        'sector_drivers': '2. Valuation Group Drivers',
        'sector_chemicals': '2. Valuation Group Drivers',
        'sector_automobiles': '2. Valuation Group Drivers',
        'company_aether': '4. Company Drivers',
        'company_eicher': '4. Company Drivers',
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
                logger.info("Google Sheets client authenticated")
            except Exception as e:
                logger.error(f"Failed to auth Google Sheets: {e}", exc_info=True)
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

    def create_driver_sheets(self):
        """
        Create all required sheets if they don't exist.
        Called during initial setup.
        """
        if not check_internet():
            logger.warning("No internet, cannot create Google Sheets")
            return

        existing = [ws.title for ws in self.spreadsheet.worksheets()]

        for key, title in self.SHEET_NAMES.items():
            if title not in existing:
                try:
                    ws = self.spreadsheet.add_worksheet(title=title, rows=100, cols=20)
                    self._init_sheet_headers(key, ws)
                    logger.info(f"Created sheet: {title}")
                except Exception as e:
                    logger.error(f"Failed to create sheet {title}: {e}", exc_info=True)

    def _init_sheet_headers(self, sheet_key: str, worksheet):
        """Initialize column headers for a sheet."""
        headers = {
            'macro_drivers': [
                'Category', 'Driver', 'Current Value', 'Bull', 'Base', 'Bear',
                'Source', 'Update Freq', 'Last Updated', 'Trend', 'Weight',
                'Valuation Impact'
            ],
            'sector_chemicals': [
                'Category', 'Driver', 'Metric', 'Current', 'Bull', 'Base', 'Bear',
                'Weight', 'Impact', 'Trend', 'Last Updated', 'Source'
            ],
            'sector_automobiles': [
                'Category', 'Driver', 'Metric', 'Current', 'Bull', 'Base', 'Bear',
                'Weight', 'Impact', 'Trend', 'Last Updated', 'Source'
            ],
            'company_aether': [
                'Category', 'Driver', 'Metric', 'Current', 'vs Peers',
                'Weight', 'Alpha Impact', 'Last Updated', 'Source'
            ],
            'company_eicher': [
                'Category', 'Driver', 'Metric', 'Current', 'vs Peers',
                'Weight', 'Alpha Impact', 'Last Updated', 'Source'
            ],
        }

        if sheet_key in headers:
            worksheet.update('A1', [headers[sheet_key]])

    @retry_with_backoff(max_retries=2, base_delay=2.0)
    def get_sector_drivers(self, sector_sheet_key: str) -> list:
        """
        Read current driver values from sector sheet.
        Returns list of driver dicts.
        """
        ws = self.spreadsheet.worksheet(self.SHEET_NAMES.get(sector_sheet_key, ''))
        records = ws.get_all_records()

        drivers = []
        for row in records:
            if row.get('Driver'):
                drivers.append({
                    'category': row.get('Category', ''),
                    'name': row.get('Driver', ''),
                    'metric': row.get('Metric', ''),
                    'current_value': row.get('Current', ''),
                    'bull': row.get('Bull', ''),
                    'base': row.get('Base', ''),
                    'bear': row.get('Bear', ''),
                    'weight': self._safe_float(row.get('Weight', 0)),
                    'impact': row.get('Impact', ''),
                    'trend': row.get('Trend', ''),
                })

        return drivers

    @retry_with_backoff(max_retries=2, base_delay=2.0)
    def update_driver_value(self, sheet_key: str, driver_name: str,
                             new_value: str, column: str = 'Current'):
        """
        Update a specific driver value in the sheet.
        Finds the row by driver name, updates the specified column.
        """
        ws = self.spreadsheet.worksheet(self.SHEET_NAMES.get(sheet_key, ''))

        # Find driver_name column (could be column B or C depending on sheet structure)
        headers = ws.row_values(1)
        driver_name_col = None
        for i, h in enumerate(headers):
            if h == 'driver_name':
                driver_name_col = i + 1
                break

        if not driver_name_col:
            # Fallback to column 2 for old structure
            driver_name_col = 2

        cell = ws.find(driver_name, in_column=driver_name_col)

        if not cell:
            logger.warning(f"Driver '{driver_name}' not found in {sheet_key}")
            return

        # Find column index for the target column
        col_idx = None
        for i, h in enumerate(headers):
            if h == column:
                col_idx = i + 1
                break

        if col_idx:
            ws.update_cell(cell.row, col_idx, new_value)
            # Also update Last Updated column
            for i, h in enumerate(headers):
                if h in ['Last Updated', 'last_updated']:
                    ws.update_cell(cell.row, i + 1, datetime.now().strftime('%Y-%m-%d %H:%M'))
                    break

            logger.info(f"Updated {driver_name}.{column} = {new_value} in {sheet_key}")

    def seed_macro_drivers(self):
        """
        Seed the Macro Drivers sheet with initial values.
        Called during setup only.
        """
        drivers = [
            ['Growth', 'Real GDP Growth', '6.8%', '7.5%', '6.5%', '5.5%', 'RBI/MOSPI', 'Quarterly', '', 'STABLE', '0.05', 'Revenue ceiling'],
            ['Growth', 'GDP Cycle Phase', 'EXPANSION', '-', '-', '-', 'Internal', 'Quarterly', '', 'STABLE', '0.03', 'Scenario framing'],
            ['Inflation', 'CPI Inflation', '5.2%', '4.5%', '5.0%', '6.5%', 'MOSPI', 'Monthly', '', 'STABLE', '0.03', 'Pricing power'],
            ['Rates', 'Repo Rate', '6.5%', '6.0%', '6.5%', '7.0%', 'RBI', 'As announced', '', 'STABLE', '0.04', 'WACC input'],
            ['Rates', '10Y G-Sec Yield', '7.1%', '6.8%', '7.2%', '7.8%', 'RBI', 'Daily', '', 'STABLE', '0.04', 'Risk-free rate'],
            ['Currency', 'INR/USD', '83.5', '82.0', '84.0', '87.0', 'Yahoo', 'Daily', '', 'STABLE', '0.03', 'Export/import'],
            ['Fiscal', 'Govt Capex Growth', '25%', '30%', '22%', '15%', 'Budget', 'Annual', '', 'UP', '0.03', 'Infrastructure'],
            ['Liquidity', 'FII/FPI Flows', 'NEUTRAL', 'INFLOW', 'NEUTRAL', 'OUTFLOW', 'NSDL', 'Weekly', '', 'STABLE', '0.03', 'Multiple'],
            ['Liquidity', 'Credit Growth', '14%', '16%', '13%', '10%', 'RBI', 'Monthly', '', 'STABLE', '0.02', 'Demand proxy'],
        ]

        try:
            ws = self.spreadsheet.worksheet(self.SHEET_NAMES['macro_drivers'])
            for i, row in enumerate(drivers, start=2):
                ws.update(f'A{i}', [row])
            logger.info(f"Seeded {len(drivers)} macro drivers")
        except Exception as e:
            logger.error(f"Failed to seed macro drivers: {e}", exc_info=True)

    # =========================================================================
    # SOCIAL MEDIA POST QUEUEING (to 'Social Posts' tab on main drivers GSheet)
    # =========================================================================

    SOCIAL_POSTS_HEADERS = [
        'Date', 'Time IST', 'Category', 'Headline',
        'Twitter Draft', 'LinkedIn Draft',
        'Approval', 'posted_x_at', 'posted_linkedin_at'
    ]

    def _get_social_sheet(self):
        """
        Get or create the 'Social Posts' tab on the main drivers GSheet.
        Auto-creates with headers if it doesn't exist.
        """
        tab_name = self.SHEET_NAMES['social_posts']
        try:
            ws = self.spreadsheet.worksheet(tab_name)
        except gspread.exceptions.WorksheetNotFound:
            logger.info(f"Creating '{tab_name}' tab on main GSheet...")
            ws = self.spreadsheet.add_worksheet(title=tab_name, rows=500, cols=len(self.SOCIAL_POSTS_HEADERS))
            ws.update('A1', [self.SOCIAL_POSTS_HEADERS])
            logger.info(f"Created '{tab_name}' tab with headers")
        return ws

    @retry_with_backoff(max_retries=2, base_delay=2.0)
    def queue_social_post(self, twitter_text: str = '', linkedin_text: str = '',
                          category: str = '', headline: str = '',
                          scheduled_date: str = None, scheduled_time: str = None) -> bool:
        """
        Queue a social media post to 'Social Posts' tab on main drivers GSheet.
        PM will review and set Approval=YES before social_poster.py publishes.

        GSheet columns:
        Date | Time IST | Category | Headline | Twitter Draft | LinkedIn Draft |
        Approval | posted_x_at | posted_linkedin_at

        Args:
            twitter_text: Tweet text (max 280 chars)
            linkedin_text: LinkedIn post text (150-500 words)
            category: Post category (sector_insight, company_highlight, etc.)
            headline: Short description of what this post is about
            scheduled_date: Date string (YYYY-MM-DD), defaults to today
            scheduled_time: Time string (HH:MM), defaults to 08:00
        Returns:
            True if successfully queued
        """
        now = datetime.now()
        date_str = scheduled_date or now.strftime('%Y-%m-%d')
        time_str = scheduled_time or '08:00'

        row = [
            date_str,                # Date
            time_str,                # Time IST
            category,                # Category
            headline,                # Headline
            twitter_text,            # Twitter Draft
            linkedin_text,           # LinkedIn Draft
            '',                      # Approval (PM fills in YES/NO)
            '',                      # posted_x_at (social_poster fills in)
            '',                      # posted_linkedin_at (social_poster fills in)
        ]

        try:
            ws = self._get_social_sheet()
            ws.append_row(row, value_input_option='USER_ENTERED')
            logger.info(f"Queued social post: {headline or twitter_text[:50]}... [category={category}]")
            return True
        except Exception as e:
            logger.error(f"Failed to queue social post to GSheet: {e}", exc_info=True)
            raise

    def get_approved_social_posts(self) -> list:
        """
        Read approved but unposted social posts from GSheet.
        Returns list of dicts with row_index for updating posted_at timestamps.
        """
        try:
            ws = self._get_social_sheet()
            records = ws.get_all_records()

            approved = []
            for i, row in enumerate(records):
                approval = str(row.get('Approval', '')).strip().upper()
                if approval in ('YES', 'Y'):
                    posted_x = str(row.get('posted_x_at', '')).strip()
                    posted_li = str(row.get('posted_linkedin_at', '')).strip()

                    # Only include if at least one platform hasn't been posted
                    if not posted_x or not posted_li:
                        approved.append({
                            'row_index': i + 2,  # +2 for header row + 0-based index
                            'date': row.get('Date', ''),
                            'time': row.get('Time IST', ''),
                            'category': row.get('Category', ''),
                            'headline': row.get('Headline', ''),
                            'twitter': row.get('Twitter Draft', ''),
                            'linkedin': row.get('LinkedIn Draft', ''),
                            'posted_x_at': posted_x,
                            'posted_linkedin_at': posted_li,
                        })

            logger.info(f"Found {len(approved)} approved unposted social posts")
            return approved
        except Exception as e:
            logger.error(f"Failed to read approved social posts: {e}", exc_info=True)
            return []

    def mark_social_post_posted(self, row_index: int, platform: str):
        """
        Update posted_at timestamp in GSheet after successful posting.

        Args:
            row_index: The GSheet row number (1-based, including header)
            platform: 'twitter' or 'linkedin'
        """
        try:
            ws = self._get_social_sheet()
            headers = ws.row_values(1)
            timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

            col_name = 'posted_x_at' if platform == 'twitter' else 'posted_linkedin_at'
            for i, h in enumerate(headers):
                if h == col_name:
                    ws.update_cell(row_index, i + 1, timestamp)
                    logger.info(f"Marked row {row_index} as posted on {platform} at {timestamp}")
                    return
            logger.warning(f"Column '{col_name}' not found in Social Posts headers")
        except Exception as e:
            logger.error(f"Failed to mark post as posted: {e}", exc_info=True)

    @staticmethod
    def _safe_float(val) -> float:
        try:
            return float(val)
        except (ValueError, TypeError):
            return 0.0
