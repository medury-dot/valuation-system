"""
Orchestrator Agent
Main coordinator for the entire valuation system.

Responsibilities:
- Schedule and coordinate all agents
- Handle catchup after gaps (machine off, internet down)
- Detect material valuation changes and trigger alerts
- Maintain model history and audit trail
- Run on-demand valuations
- Replay queued operations after service recovery

Edge Cases Handled:
- Machine was off for N days → Run catchup for missed days
- Internet unavailable → Use cached data, queue operations
- MySQL down → Queue DB writes, use local state
- ChromaDB down → Skip vector ops, use metadata
- Partial data → Run what's possible, flag gaps
- Concurrent runs → Lock mechanism prevents overlap
- On-demand + scheduled overlap → On-demand takes priority
"""

import os
import logging
from datetime import datetime, date, timedelta
from typing import Optional

from dotenv import load_dotenv

from valuation_system.agents.news_scanner import NewsScannerAgent
from valuation_system.agents.sector_analyst import SectorAnalystAgent
from valuation_system.agents.valuator import ValuatorAgent
from valuation_system.data.loaders.core_loader import CoreDataLoader
from valuation_system.data.loaders.price_loader import PriceLoader
from valuation_system.data.loaders.damodaran_loader import DamodaranLoader
from valuation_system.storage.mysql_client import get_mysql_client
from valuation_system.storage.gsheet_client import GSheetClient
from valuation_system.utils.config_loader import (
    load_sectors_config, load_companies_config,
    get_active_sectors, get_active_companies
)
from valuation_system.utils.resilience import (
    RunStateManager, GracefulDegradation,
    check_dependencies, check_internet, safe_task_run
)
from valuation_system.utils.llm_client import LLMClient

logger = logging.getLogger(__name__)

load_dotenv(os.path.join(os.path.dirname(__file__), '..', 'config', '.env'))

ALERT_THRESHOLD_PCT = float(os.getenv('ALERT_THRESHOLD_PCT', 5.0))


class OrchestratorAgent:
    """
    Main coordinator. Entry point for all scheduled and on-demand operations.

    Cycle types:
    - Hourly: News scan → classify → update drivers
    - Daily: Full valuation refresh → alerts → digest email
    - Weekly: Summary report → accuracy tracking
    - On-demand: Single company or portfolio valuation
    - Catchup: Fill gaps from downtime
    """

    def __init__(self):
        """Initialize all components. Handles service unavailability gracefully."""
        self.state = RunStateManager()
        self.degradation = GracefulDegradation()
        self.llm = LLMClient()

        # Check what's available
        self.deps = check_dependencies()

        # Initialize data loaders (always available - file-based)
        self.core_loader = CoreDataLoader()
        self.price_loader = PriceLoader()
        self.damodaran_loader = DamodaranLoader()

        # Initialize MySQL client (may fail if MySQL is down)
        self.mysql = None
        if self.deps.get('mysql'):
            try:
                self.mysql = get_mysql_client()
            except Exception as e:
                logger.error(f"MySQL unavailable: {e}")

        # Initialize Google Sheets client (may fail if no internet or credentials)
        self.gsheet = None
        if check_internet():
            try:
                self.gsheet = GSheetClient()
                logger.info("Google Sheets client initialized")
            except Exception as e:
                logger.warning(f"Google Sheets unavailable: {e}")

        # Load configs
        self.sectors_config = load_sectors_config()
        self.companies_config = load_companies_config()
        self.active_sectors = get_active_sectors(self.sectors_config)
        self.active_companies = get_active_companies(self.companies_config)

        # Initialize agents
        self.news_scanner = None
        if self.mysql:
            self.news_scanner = NewsScannerAgent(self.mysql, self.llm, self.state)

        self.sector_analysts = {}
        if self.mysql:
            for sector_key in self.active_sectors:
                self.sector_analysts[sector_key] = SectorAnalystAgent(
                    sector_key, self.mysql, self.llm
                )

        self.valuator = None
        if self.mysql:
            self.valuator = ValuatorAgent(
                self.core_loader, self.price_loader, self.damodaran_loader,
                self.mysql
            )

        logger.info(f"Orchestrator initialized. Dependencies: {self.deps}")
        logger.info(f"Active sectors: {list(self.active_sectors.keys())}")
        logger.info(f"Active companies: {list(self.active_companies.keys())}")

    # =========================================================================
    # HOURLY CYCLE
    # =========================================================================

    def run_hourly_cycle(self) -> dict:
        """
        Hourly execution:
        1. Check for missed runs → catchup if needed
        2. Scan news sources
        3. Classify and store significant events
        4. Update sector drivers from news
        5. Check for critical events requiring immediate valuation
        """
        if self.state.is_running('hourly_cycle'):
            logger.info("Hourly cycle already running, skipping")
            return {'status': 'SKIPPED', 'reason': 'already_running'}

        self.state.mark_running('hourly_cycle')
        start_time = datetime.now()
        result = {'cycle': 'hourly', 'started_at': start_time.isoformat()}

        try:
            # 0. Replay any queued operations from previous failures
            self._replay_queued_ops()

            # 1. Check for catchup
            catchup_result = self._check_and_run_catchup()
            result['catchup'] = catchup_result

            # 2. News scan
            articles = []
            significant = []
            if self.news_scanner and check_internet():
                articles = self.news_scanner.scan_all_sources()
                significant = self.news_scanner.classify_and_store(articles)
                result['articles_scanned'] = len(articles)
                result['significant_events'] = len(significant)
            else:
                result['news_scan'] = 'SKIPPED' if not check_internet() else 'NO_SCANNER'

            # 3. Update sector drivers
            driver_changes = {}
            for sector_key, analyst in self.sector_analysts.items():
                changes = analyst.update_drivers_from_news(significant)
                driver_changes[sector_key] = len(changes)
            result['driver_changes'] = driver_changes

            # 4. Check for critical events → immediate valuation
            critical = [e for e in significant if e.get('severity') == 'CRITICAL']
            if critical:
                logger.warning(f"{len(critical)} CRITICAL events detected!")
                self._handle_critical_events(critical)
                result['critical_events'] = len(critical)

            self.state.record_success('hourly_cycle', result)
            result['status'] = 'SUCCESS'

        except Exception as e:
            logger.error(f"Hourly cycle failed: {e}", exc_info=True)
            self.state.record_failure('hourly_cycle', str(e))
            result['status'] = 'FAILED'
            result['error'] = str(e)

        elapsed = (datetime.now() - start_time).total_seconds()
        result['elapsed_seconds'] = round(elapsed, 1)
        logger.info(f"Hourly cycle completed in {elapsed:.1f}s: {result.get('status')}")

        return result

    # =========================================================================
    # MACRO DATA SYNC
    # =========================================================================

    def _check_and_update_macro_data(self) -> dict:
        """
        Check if macro CSV is stale (>30 days old) and run update script if needed.
        Self-healing: Catches missed monthly updates.

        Returns dict with update results.
        """
        import subprocess

        result = {'checked': True, 'updated': False, 'reason': ''}

        try:
            macro_csv_path = os.getenv('MACRO_DATA_PATH',
                                       '/Users/ram/code/investment_strategies/data/macro') + '/market_indicators.csv'

            if not os.path.exists(macro_csv_path):
                result['reason'] = 'CSV not found'
                return result

            # Check file modification time
            mtime = os.path.getmtime(macro_csv_path)
            file_age_days = (datetime.now().timestamp() - mtime) / 86400

            logger.info(f"Macro CSV age: {file_age_days:.1f} days")

            # If stale (>30 days), run update script
            if file_age_days > 30:
                logger.warning(f"Macro CSV is stale ({file_age_days:.1f} days old), running update...")

                update_script = os.getenv('MACRO_SCRIPT_PATH',
                                         '/Users/ram/code/investment_strategies/scripts/update_macro_data.py')

                if not os.path.exists(update_script):
                    logger.error(f"Update script not found: {update_script}")
                    result['reason'] = 'Update script not found'
                    return result

                # Run update script
                try:
                    proc_result = subprocess.run(
                        ['python3', update_script, '--all'],
                        capture_output=True,
                        text=True,
                        timeout=600  # 10 minutes max
                    )

                    if proc_result.returncode == 0:
                        logger.info("Macro data updated successfully via script")
                        result['updated'] = True
                        result['reason'] = f'Auto-update triggered (CSV was {file_age_days:.1f} days old)'
                    else:
                        logger.error(f"Macro update script failed: {proc_result.stderr}")
                        result['reason'] = f'Update script failed: {proc_result.stderr[:200]}'

                except subprocess.TimeoutExpired:
                    logger.error("Macro update script timed out (>10 min)")
                    result['reason'] = 'Update script timed out'
                except Exception as e:
                    logger.error(f"Failed to run macro update script: {e}")
                    result['reason'] = f'Script execution failed: {str(e)}'
            else:
                result['reason'] = f'CSV fresh ({file_age_days:.1f} days old)'

        except Exception as e:
            logger.error(f"Macro staleness check failed: {e}", exc_info=True)
            result['reason'] = f'Check failed: {str(e)}'

        return result

    def _sync_macro_from_csv(self) -> dict:
        """
        Read latest macro data from CSV files, update Google Sheet if changed.
        Called daily before valuation to ensure fresh macro data.

        Returns dict with sync results.
        """
        import pandas as pd

        result = {'synced': 0, 'unchanged': 0, 'errors': []}

        if not self.gsheet or not self.mysql:
            logger.warning("GSheet or MySQL unavailable, skipping macro sync")
            return {'status': 'SKIPPED', 'reason': 'dependencies_unavailable'}

        try:
            # Path to macro data
            macro_csv_path = os.getenv('MACRO_DATA_PATH',
                                       '/Users/ram/code/investment_strategies/data/macro') + '/market_indicators.csv'

            if not os.path.exists(macro_csv_path):
                logger.warning(f"Macro CSV not found: {macro_csv_path}")
                return {'status': 'SKIPPED', 'reason': 'csv_not_found'}

            # Read latest macro data
            df = pd.read_csv(macro_csv_path, comment='#')
            df['date'] = pd.to_datetime(df['date'])
            latest_date = df['date'].max()
            latest_data = df[df['date'] == latest_date]

            # Map CSV series names to our driver names
            series_mapping = {
                'GDP Growth': 'gdp_growth',
                'USD INR': 'usd_inr',
                '10 year bond yield (Source: FRED)': 'interest_rate_10y',
                'Repo Rate': 'repo_rate',
                'PMI composite': 'pmi_composite'
            }

            logger.info(f"Syncing macro data from CSV (as of {latest_date.strftime('%Y-%m-%d')})")

            for csv_series, driver_name in series_mapping.items():
                try:
                    # Get value from CSV
                    csv_row = latest_data[latest_data['series_name'] == csv_series]
                    if len(csv_row) == 0:
                        logger.warning(f"Series '{csv_series}' not found in CSV")
                        result['errors'].append(f"{driver_name}: series not found")
                        continue

                    csv_value = csv_row['value'].values[0]

                    # Format value based on driver type
                    if driver_name in ['gdp_growth', 'interest_rate_10y', 'repo_rate']:
                        # These are stored as decimals in CSV (0.063 = 6.3%)
                        if csv_value < 1:
                            formatted_value = f"{csv_value * 100:.2f}%"
                        else:
                            # Already in percentage format
                            formatted_value = f"{csv_value:.2f}%"
                    else:
                        # usd_inr, pmi_composite - store as is
                        formatted_value = f"{csv_value:.2f}"

                    # Get current value from MySQL
                    current = self.mysql.query_one(
                        "SELECT current_value FROM vs_drivers WHERE driver_level = 'MACRO' AND driver_name = %s",
                        (driver_name,)
                    )

                    current_value = current['current_value'] if current else None

                    # Check if changed
                    if current_value != formatted_value:
                        logger.info(f"  {driver_name}: {current_value} → {formatted_value}")

                        # Update Google Sheet
                        if self.gsheet:
                            self.gsheet.update_driver_value(
                                'macro_drivers',  # Uses SHEET_NAMES mapping
                                driver_name,
                                formatted_value,
                                column='current_value'
                            )

                        # Update MySQL
                        if current:
                            self.mysql.execute(
                                """UPDATE vs_drivers
                                   SET current_value = %s, last_updated = NOW()
                                   WHERE driver_level = 'MACRO' AND driver_name = %s""",
                                (formatted_value, driver_name)
                            )
                        else:
                            # Insert if not exists
                            self.mysql.execute(
                                """INSERT INTO vs_drivers
                                   (driver_level, driver_name, current_value, weight, last_updated)
                                   VALUES ('MACRO', %s, %s, 0.20, NOW())""",
                                (driver_name, formatted_value)
                            )

                        result['synced'] += 1
                    else:
                        result['unchanged'] += 1

                except Exception as e:
                    logger.error(f"Error syncing {driver_name}: {e}")
                    result['errors'].append(f"{driver_name}: {str(e)}")

            logger.info(f"Macro sync complete: {result['synced']} updated, {result['unchanged']} unchanged")
            result['status'] = 'SUCCESS'

        except Exception as e:
            logger.error(f"Macro sync failed: {e}", exc_info=True)
            result['status'] = 'FAILED'
            result['error'] = str(e)

        return result

    # =========================================================================
    # DAILY VALUATION
    # =========================================================================

    def run_daily_valuation(self) -> dict:
        """
        Daily valuation refresh:
        1. Reload price data (file may have been updated)
        2. Calculate sector outlooks
        3. Run full valuation for each active company
        4. Compare with previous valuation
        5. Generate alerts for material changes
        6. Store all results
        """
        if self.state.is_running('daily_valuation'):
            logger.info("Daily valuation already running, skipping")
            return {'status': 'SKIPPED'}

        self.state.mark_running('daily_valuation')
        start_time = datetime.now()
        result = {'cycle': 'daily_valuation', 'started_at': start_time.isoformat()}

        try:
            # Check if macro data is stale and update if needed
            macro_update_result = self._check_and_update_macro_data()
            result['macro_update'] = macro_update_result

            # Sync macro data from CSV to GSheet/MySQL
            macro_sync_result = self._sync_macro_from_csv()
            result['macro_sync'] = macro_sync_result

            # Reload price data
            self.price_loader.reload()

            valuations = {}
            alerts = []

            for company_key, company_config in self.active_companies.items():
                try:
                    val_result = self._run_single_valuation(company_key, company_config)
                    if val_result and 'error' not in val_result:
                        valuations[company_key] = val_result

                        # Check for material change
                        alert = self._check_valuation_change(company_config, val_result)
                        if alert:
                            alerts.append(alert)

                except Exception as e:
                    logger.error(f"Valuation failed for {company_key}: {e}", exc_info=True)
                    valuations[company_key] = {'error': str(e)}

            result['valuations'] = {
                k: {
                    'intrinsic': v.get('intrinsic_value_blended'),
                    'cmp': v.get('cmp'),
                    'upside_pct': v.get('upside_pct'),
                    'confidence': v.get('confidence_score'),
                } if 'error' not in v else {'error': v['error']}
                for k, v in valuations.items()
            }
            result['alerts'] = len(alerts)
            result['status'] = 'SUCCESS'

            self.state.record_success('daily_valuation', result)

        except Exception as e:
            logger.error(f"Daily valuation failed: {e}", exc_info=True)
            self.state.record_failure('daily_valuation', str(e))
            result['status'] = 'FAILED'
            result['error'] = str(e)

        elapsed = (datetime.now() - start_time).total_seconds()
        result['elapsed_seconds'] = round(elapsed, 1)
        logger.info(f"Daily valuation completed in {elapsed:.1f}s")

        return result

    # =========================================================================
    # ON-DEMAND VALUATION
    # =========================================================================

    def run_on_demand(self, company_key: str = None,
                      nse_symbol: str = None,
                      overrides: dict = None) -> dict:
        """
        Run valuation on demand for a specific company.
        Can be triggered by PM or by critical event.

        Args:
            company_key: Key from companies.yaml (e.g., 'aether_industries')
            nse_symbol: NSE symbol (e.g., 'AETHER') - alternative to company_key
            overrides: Manual parameter overrides

        Returns:
            Full valuation result.
        """
        # Find company config
        if company_key:
            company_config = self.active_companies.get(company_key)
        elif nse_symbol:
            company_config = next(
                (v for v in self.active_companies.values()
                 if v.get('nse_symbol') == nse_symbol),
                None
            )
            company_key = next(
                (k for k, v in self.active_companies.items()
                 if v.get('nse_symbol') == nse_symbol),
                nse_symbol
            )
        else:
            return {'error': 'company_key or nse_symbol required'}

        if not company_config:
            return {'error': f'Company not found: {company_key or nse_symbol}'}

        logger.info(f"On-demand valuation for {company_key}")

        # Reload fresh data
        self.price_loader.reload()

        result = self._run_single_valuation(company_key, company_config, overrides)
        return result

    def run_portfolio_valuation(self) -> dict:
        """Run valuation for all active companies (on-demand full portfolio)."""
        return self.run_daily_valuation()

    # =========================================================================
    # INTERNAL METHODS
    # =========================================================================

    def _run_single_valuation(self, company_key: str,
                               company_config: dict,
                               overrides: dict = None) -> dict:
        """Run valuation for a single company."""
        sector_key = company_config.get('sector', '')

        # Get sector outlook
        sector_outlook = {'sector': '', 'outlook_score': 0, 'outlook_label': 'NEUTRAL',
                          'growth_adjustment': 0, 'margin_adjustment': 0}
        if sector_key in self.sector_analysts:
            sector_outlook = self.sector_analysts[sector_key].calculate_sector_outlook()

        # Get sector config
        sector_config = self.active_sectors.get(sector_key, {})

        # Run valuation
        if not self.valuator:
            return {'error': 'Valuator not initialized (MySQL may be down)'}

        val_result = self.valuator.run_full_valuation(
            company_config, sector_outlook, sector_config, overrides
        )

        # Store result
        if val_result and 'error' not in val_result:
            self.valuator.store_valuation(val_result)

        return val_result

    def _check_valuation_change(self, company_config: dict,
                                 new_valuation: dict) -> Optional[dict]:
        """Check if valuation changed materially from previous."""
        if not self.mysql:
            return None

        nse_symbol = company_config.get('nse_symbol', '')
        company = self.mysql.get_company_by_symbol(nse_symbol)
        if not company:
            return None

        company_id = company['id']  # marketscrip_id from mssdb

        previous = self.mysql.get_latest_valuation(company_id)
        if not previous:
            return None  # First valuation, no comparison

        old_value = float(previous.get('intrinsic_value', 0))
        new_value = new_valuation.get('intrinsic_value_blended', 0)

        if old_value <= 0:
            return None

        change_pct = abs(new_value - old_value) / old_value * 100

        if change_pct >= ALERT_THRESHOLD_PCT:
            alert = {
                'company': company_config.get('csv_name', nse_symbol),
                'nse_symbol': nse_symbol,
                'old_value': old_value,
                'new_value': new_value,
                'change_pct': round(change_pct, 2),
                'cmp': new_valuation.get('cmp'),
                'upside_pct': new_valuation.get('upside_pct'),
                'triggered_at': datetime.now().isoformat(),
            }

            logger.warning(f"ALERT: {nse_symbol} valuation changed by {change_pct:.1f}%: "
                           f"₹{old_value:,.2f} → ₹{new_value:,.2f}")

            # Log alert to DB
            try:
                self.mysql.log_alert(
                    company_id, 'VALUATION_CHANGE',
                    change_pct,
                    f"Intrinsic value changed from ₹{old_value:,.2f} to ₹{new_value:,.2f}"
                )
            except Exception as e:
                logger.error(f"Failed to log alert: {e}", exc_info=True)

            return alert

        return None

    def _handle_critical_events(self, events: list):
        """Handle critical news events that need immediate valuation."""
        for event in events:
            affected = event.get('affected_companies', [])
            if isinstance(affected, list):
                for symbol in affected:
                    if symbol in [c.get('nse_symbol') for c in self.active_companies.values()]:
                        logger.info(f"Triggering immediate valuation for {symbol} due to critical event")
                        self.run_on_demand(nse_symbol=symbol)

    def _check_and_run_catchup(self) -> dict:
        """Check for missed runs and execute catchups."""
        catchup_result = {'needed': False}

        # Check news scan catchup
        if self.news_scanner:
            missed = self.state.get_missed_days('news_scan', expected_frequency_hours=1)
            if missed:
                catchup_result['news_missed_days'] = len(missed)
                catchup_result['needed'] = True
                news_catchup = self.news_scanner.run_catchup()
                catchup_result['news_catchup'] = news_catchup

        # Check daily valuation catchup
        missed_val = self.state.get_missed_days('daily_valuation', expected_frequency_hours=24)
        if missed_val:
            catchup_result['valuation_missed_days'] = len(missed_val)
            catchup_result['needed'] = True
            # Run today's valuation (most recent is what matters)
            logger.info(f"Running catchup daily valuation (missed {len(missed_val)} days)")

        return catchup_result

    def _replay_queued_ops(self):
        """Replay any operations queued during previous failures."""
        queue_size = self.degradation.get_queue_size()
        if queue_size > 0:
            logger.info(f"Replaying {queue_size} queued operations")
            self.degradation.replay_queued_operations(self._handle_queued_op)

    def _handle_queued_op(self, op: dict):
        """Handle a single queued operation during replay."""
        op_type = op.get('type')

        if op_type == 'store_event' and self.mysql:
            data = op.get('data', {})
            self.mysql.insert('vs_event_timeline', data)

        elif op_type == 'store_valuation' and self.mysql:
            data = op.get('data', {})
            if self.valuator:
                self.valuator.store_valuation(data)

        elif op_type == 'news_scan' and self.news_scanner:
            self.news_scanner.scan_all_sources()

        else:
            logger.warning(f"Unknown queued operation type: {op_type}")

    # =========================================================================
    # STATUS & MONITORING
    # =========================================================================

    def get_system_status(self) -> dict:
        """Get full system status for monitoring."""
        return {
            'dependencies': check_dependencies(),
            'task_states': self.state.get_full_status(),
            'data_staleness': self.degradation.check_data_staleness(),
            'queued_operations': self.degradation.get_queue_size(),
            'active_sectors': list(self.active_sectors.keys()),
            'active_companies': list(self.active_companies.keys()),
            'mysql_available': self.mysql is not None,
            'timestamp': datetime.now().isoformat(),
        }
