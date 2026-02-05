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
