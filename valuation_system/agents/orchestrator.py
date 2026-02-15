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
from valuation_system.agents.group_analyst import GroupAnalystAgent
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
from valuation_system.utils.structured_logger import StructuredLogger

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

        # MySQL client initialized later - will update slog after mysql is available
        self.mysql = None
        self.slog = None  # Will be set after MySQL init

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

        # Initialize StructuredLogger (with or without MySQL)
        self.slog = StructuredLogger('OrchestratorAgent', logger, self.mysql)

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
        self.companies_config = load_companies_config()  # Keep for fallback
        self.active_sectors = get_active_sectors(self.sectors_config)

        # Load companies from database (with YAML fallback)
        self.active_companies = get_active_companies(
            mysql_client=self.mysql,
            use_yaml_fallback=True  # Safe during transition
        )
        logger.info(f"Loaded {len(self.active_companies)} active companies from {'database' if self.mysql else 'YAML'}")

        # Initialize agents
        self.news_scanner = None
        if self.mysql:
            self.news_scanner = NewsScannerAgent(self.mysql, self.llm, self.state)

        self.group_analysts = {}
        if self.mysql:
            for sector_key in self.active_sectors:
                self.group_analysts[sector_key] = GroupAnalystAgent(
                    valuation_group=sector_key,
                    mysql_client=self.mysql,
                    llm_client=self.llm
                )

        self.valuator = None
        if self.mysql:
            self.valuator = ValuatorAgent(
                self.core_loader, self.price_loader, self.damodaran_loader,
                self.mysql
            )

        # Initialize email sender (for daily digest)
        self.email_sender = None
        try:
            from valuation_system.notifications.email_sender import EmailSender
            self.email_sender = EmailSender()
        except Exception as e:
            logger.warning(f"EmailSender unavailable: {e}")

        # Initialize content agent (for social media posts)
        self.content_agent = None
        if self.mysql:
            try:
                from valuation_system.agents.content_agent import ContentAgent
                self.content_agent = ContentAgent(self.mysql, self.gsheet, self.llm)
            except Exception as e:
                logger.warning(f"ContentAgent unavailable: {e}")

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

        # Structured logging: cycle start
        self.slog.log_cycle_start('hourly')

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

            # 3. Update group drivers
            driver_changes = {}
            for sector_key, analyst in self.group_analysts.items():
                changes = analyst.update_drivers_from_news(significant)
                driver_changes[sector_key] = len(changes)
            result['driver_changes'] = driver_changes

            # 4. Check for critical events → immediate valuation
            critical = [e for e in significant if e.get('severity') == 'CRITICAL']
            if critical:
                logger.warning(f"{len(critical)} CRITICAL events detected!")
                self._handle_critical_events(critical)
                result['critical_events'] = len(critical)

                # Send critical alert emails
                if self.email_sender and self.email_sender.enabled:
                    for event in critical:
                        try:
                            self.email_sender.send_critical_alert(event)
                        except Exception as e:
                            logger.error(f"Failed to send critical alert email: {e}")

            # 5. Detect PM edits in GSheet (Tab 1-4 drivers + Tab 7 discovered drivers)
            if self.gsheet and self.mysql:
                try:
                    from valuation_system.storage.gsheet_unified import GSheetUnifiedClient
                    unified = GSheetUnifiedClient()
                    pm_edits = unified.detect_pm_edits(self.mysql)
                    result['pm_edits_detected'] = len(pm_edits)
                except Exception as e:
                    logger.error(f"PM edit detection failed: {e}", exc_info=True)
                    result['pm_edits_detected'] = 0

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

        # Structured logging: cycle complete
        self.slog.log_cycle_complete(
            cycle_type='hourly',
            elapsed_ms=elapsed * 1000,
            metrics={
                'articles_scanned': result.get('articles_scanned', 0),
                'significant_events': result.get('significant_events', 0),
                'critical_events': result.get('critical_events', 0),
                'pm_edits_detected': result.get('pm_edits_detected', 0),
                'driver_changes_total': sum(result.get('driver_changes', {}).values())
            },
            status=result.get('status', 'UNKNOWN')
        )

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
        Read latest macro data from all CSV files, compute trends, update GSheet + MySQL.
        Called daily before valuation to ensure fresh macro data.

        Handles 23 MACRO drivers from 5 sources:
        - market_indicators.csv: gdp_growth, usd_inr, interest_rate_10y, repo_rate,
          pmi_composite, pmi_manufacturing, pmi_services, balance_of_trade, fii_flows, dii_flows
        - iip_monthly.csv: iip_manufacturing
        - cpi_monthly.csv: cpi_food_inflation, cpi_headline
        - wpi_monthly.csv: wpi_inflation, wpi_fuel_power, wpi_manufactured, wpi_primary_articles
        - gdp_quarterly.csv: gva_manufacturing_real, gva_construction_real,
          gva_financial_real, gva_agriculture_real
        - plfs_quarterly.csv: unemployment_rate, lfpr_total

        Also syncs IIP sector indices → GROUP drivers and CPI categories → GROUP signals.

        Returns dict with sync results.
        """
        import pandas as pd

        result = {'synced': 0, 'unchanged': 0, 'errors': [], 'group_synced': 0}

        if not self.mysql:
            logger.warning("MySQL unavailable, skipping macro sync")
            return {'status': 'SKIPPED', 'reason': 'dependencies_unavailable'}

        macro_dir = os.getenv('MACRO_DATA_PATH',
                              '/Users/ram/code/investment_strategies/data/macro')

        try:
            # ---------------------------------------------------------------
            # 1. MARKET INDICATORS (10 drivers — expanded from 5)
            # ---------------------------------------------------------------
            market_csv = os.path.join(macro_dir, 'market_indicators.csv')
            if os.path.exists(market_csv):
                df_market = pd.read_csv(market_csv, comment='#')
                df_market['date'] = pd.to_datetime(df_market['date'])
                latest_date = df_market['date'].max()
                latest_market = df_market[df_market['date'] == latest_date]

                market_mapping = {
                    'GDP Growth': ('gdp_growth', 'pct'),
                    'USD INR': ('usd_inr', 'value'),
                    '10 year bond yield (Source: FRED)': ('interest_rate_10y', 'pct'),
                    'Repo Rate': ('repo_rate', 'pct'),
                    'PMI composite': ('pmi_composite', 'value'),
                    'PMI Manufacturing': ('pmi_manufacturing', 'value'),
                    'PMI Services': ('pmi_services', 'value'),
                    'Balance of trade': ('balance_of_trade', 'value'),
                    'Total FII': ('fii_flows', 'value'),
                    'Total DII': ('dii_flows', 'value'),
                }

                logger.info(f"Syncing market indicators (as of {latest_date.strftime('%Y-%m-%d')})")
                for csv_series, (driver_name, fmt) in market_mapping.items():
                    self._sync_single_macro_driver(
                        latest_market, csv_series, driver_name, fmt,
                        df_market, result)

            # ---------------------------------------------------------------
            # 2. IIP — iip_manufacturing driver + GROUP sector signals
            # ---------------------------------------------------------------
            iip_csv = os.path.join(macro_dir, 'iip_monthly.csv')
            if os.path.exists(iip_csv):
                df_iip = pd.read_csv(iip_csv, comment='#')
                df_iip['date'] = pd.to_datetime(df_iip['date'])
                latest_iip_date = df_iip['date'].max()
                latest_iip = df_iip[df_iip['date'] == latest_iip_date]

                logger.info(f"Syncing IIP data (as of {latest_iip_date.strftime('%Y-%m-%d')})")

                # MACRO driver: iip_manufacturing
                mfg = latest_iip[latest_iip['series_name'] == 'Manufacturing']
                if not mfg.empty:
                    val = float(mfg['value'].values[0])
                    trend = self._compute_trend(df_iip, 'Manufacturing', months=3)
                    self._update_driver('MACRO', 'iip_manufacturing', f"{val:.1f}",
                                       trend=trend, result=result)

                # IIP → GROUP driver mapping (data-driven from macro_metadata.csv)
                iip_group_map = self._load_macro_to_group_mapping('IIP')
                for iip_series, meta in iip_group_map.items():
                    vg = meta['valuation_group']
                    row = latest_iip[latest_iip['series_name'] == iip_series]
                    if not row.empty:
                        val = float(row['value'].values[0])
                        trend = self._compute_trend(df_iip, iip_series, months=3)
                        driver_name = f"iip_{vg.lower()}"
                        self._update_group_driver(vg, driver_name, f"{val:.1f}",
                                                  trend=trend, result=result,
                                                  driver_category='MACRO_SIGNAL')

            # ---------------------------------------------------------------
            # 3. CPI — cpi_food_inflation driver + GROUP signals
            # ---------------------------------------------------------------
            cpi_csv = os.path.join(macro_dir, 'cpi_monthly.csv')
            if os.path.exists(cpi_csv):
                df_cpi = pd.read_csv(cpi_csv, comment='#')
                df_cpi['date'] = pd.to_datetime(df_cpi['date'])
                latest_cpi_date = df_cpi['date'].max()
                # Use Combined only for MACRO driver
                df_cpi_combined = df_cpi[df_cpi['category'] == 'CPI_Combined']
                latest_cpi = df_cpi_combined[df_cpi_combined['date'] == latest_cpi_date]

                logger.info(f"Syncing CPI data (as of {latest_cpi_date.strftime('%Y-%m-%d')})")

                # MACRO driver: cpi_food_inflation (YoY from Food and beverages index)
                food = latest_cpi[latest_cpi['series_name'] == 'Food and beverages']
                if not food.empty:
                    current_idx = float(food['value'].values[0])
                    # YoY: compare to same month previous year
                    yoy_date = latest_cpi_date - pd.DateOffset(years=1)
                    yoy_row = df_cpi_combined[
                        (df_cpi_combined['series_name'] == 'Food and beverages') &
                        (df_cpi_combined['date'].dt.month == yoy_date.month) &
                        (df_cpi_combined['date'].dt.year == yoy_date.year)
                    ]
                    if not yoy_row.empty:
                        prev_idx = float(yoy_row['value'].values[0])
                        yoy_pct = ((current_idx / prev_idx) - 1) * 100
                        trend = self._compute_trend(df_cpi_combined, 'Food and beverages', months=3)
                        self._update_driver('MACRO', 'cpi_food_inflation',
                                           f"{yoy_pct:.2f}%", trend=trend, result=result)

                # MACRO driver: cpi_headline (YoY from General Index)
                general = latest_cpi[latest_cpi['series_name'] == 'General Index (All Groups)']
                if not general.empty:
                    current_idx = float(general['value'].values[0])
                    yoy_date = latest_cpi_date - pd.DateOffset(years=1)
                    yoy_row = df_cpi_combined[
                        (df_cpi_combined['series_name'] == 'General Index (All Groups)') &
                        (df_cpi_combined['date'].dt.month == yoy_date.month) &
                        (df_cpi_combined['date'].dt.year == yoy_date.year)
                    ]
                    if not yoy_row.empty:
                        prev_idx = float(yoy_row['value'].values[0])
                        yoy_pct = ((current_idx / prev_idx) - 1) * 100
                        trend = self._compute_trend(df_cpi_combined, 'General Index (All Groups)', months=3)
                        self._update_driver('MACRO', 'cpi_headline',
                                           f"{yoy_pct:.2f}%", trend=trend, result=result)

                # CPI → GROUP margin signal mapping (data-driven from macro_metadata.csv)
                cpi_group_map = self._load_macro_to_group_mapping('CPI')
                for cpi_series, meta in cpi_group_map.items():
                    vg = meta['valuation_group']
                    row = latest_cpi[latest_cpi['series_name'] == cpi_series]
                    if not row.empty:
                        val = float(row['value'].values[0])
                        trend = self._compute_trend(df_cpi_combined, cpi_series, months=3)
                        driver_name = f"cpi_{vg.lower()}"
                        self._update_group_driver(vg, driver_name, f"{val:.1f}",
                                                  trend=trend, result=result,
                                                  driver_category='MACRO_SIGNAL')

            # ---------------------------------------------------------------
            # 4. WPI — wpi_inflation MACRO driver
            # ---------------------------------------------------------------
            wpi_csv = os.path.join(macro_dir, 'wpi_monthly.csv')
            if os.path.exists(wpi_csv):
                df_wpi = pd.read_csv(wpi_csv, comment='#')
                df_wpi['date'] = pd.to_datetime(df_wpi['date'])
                latest_wpi_date = df_wpi['date'].max()
                latest_wpi = df_wpi[df_wpi['date'] == latest_wpi_date]

                logger.info(f"Syncing WPI data (as of {latest_wpi_date.strftime('%Y-%m-%d')})")

                # WPI overall YoY
                overall = latest_wpi[latest_wpi['series_name'] == 'Wholesale Price Index']
                if not overall.empty:
                    current_idx = float(overall['value'].values[0])
                    yoy_date = latest_wpi_date - pd.DateOffset(years=1)
                    yoy_row = df_wpi[
                        (df_wpi['series_name'] == 'Wholesale Price Index') &
                        (df_wpi['date'].dt.month == yoy_date.month) &
                        (df_wpi['date'].dt.year == yoy_date.year)
                    ]
                    if not yoy_row.empty:
                        prev_idx = float(yoy_row['value'].values[0])
                        yoy_pct = ((current_idx / prev_idx) - 1) * 100
                        trend = self._compute_trend(df_wpi, 'Wholesale Price Index', months=3)
                        self._update_driver('MACRO', 'wpi_inflation',
                                           f"{yoy_pct:.2f}%", trend=trend, result=result)

                # WPI MajorGroup YoY drivers
                wpi_macro_map = {
                    'Fuel & power': 'wpi_fuel_power',
                    'Manufactured products': 'wpi_manufactured',
                    'Primary articles': 'wpi_primary_articles',
                }
                for wpi_series, driver_name in wpi_macro_map.items():
                    row = latest_wpi[latest_wpi['series_name'] == wpi_series]
                    if not row.empty:
                        current_idx = float(row['value'].values[0])
                        yoy_date = latest_wpi_date - pd.DateOffset(years=1)
                        yoy_row = df_wpi[
                            (df_wpi['series_name'] == wpi_series) &
                            (df_wpi['date'].dt.month == yoy_date.month) &
                            (df_wpi['date'].dt.year == yoy_date.year)
                        ]
                        if not yoy_row.empty:
                            prev_idx = float(yoy_row['value'].values[0])
                            yoy_pct = ((current_idx / prev_idx) - 1) * 100
                            trend = self._compute_trend(df_wpi, wpi_series, months=3)
                            self._update_driver('MACRO', driver_name,
                                               f"{yoy_pct:.2f}%", trend=trend, result=result)
                        else:
                            # No YoY data yet — store raw index with trend
                            trend = self._compute_trend(df_wpi, wpi_series, months=3)
                            self._update_driver('MACRO', driver_name,
                                               f"{current_idx:.1f} (idx)", trend=trend, result=result)
                            logger.info(f"  {driver_name}: YoY not available, storing index={current_idx:.1f}")

                # WPI → GROUP driver mapping (data-driven from macro_metadata.csv)
                wpi_group_map = self._load_macro_to_group_mapping('WPI')
                for wpi_series, meta in wpi_group_map.items():
                    vg = meta['valuation_group']
                    row = latest_wpi[latest_wpi['series_name'] == wpi_series]
                    if not row.empty:
                        val = float(row['value'].values[0])
                        trend = self._compute_trend(df_wpi, wpi_series, months=3)
                        driver_name = f"wpi_{vg.lower()}"
                        self._update_group_driver(vg, driver_name, f"{val:.1f}",
                                                  trend=trend, result=result,
                                                  driver_category='MACRO_SIGNAL')

            # ---------------------------------------------------------------
            # 5. PLFS — unemployment_rate + lfpr_total MACRO drivers
            # ---------------------------------------------------------------
            plfs_csv = os.path.join(macro_dir, 'plfs_quarterly.csv')
            if os.path.exists(plfs_csv):
                df_plfs = pd.read_csv(plfs_csv, comment='#')
                df_plfs['date'] = pd.to_datetime(df_plfs['date'])
                latest_plfs_date = df_plfs['date'].max()

                logger.info(f"Syncing PLFS data (as of {latest_plfs_date.strftime('%Y-%m-%d')})")

                # Helper: sync a PLFS series as MACRO driver
                def _sync_plfs_driver(series_filter_fn, driver_name, label):
                    matching = [s for s in df_plfs['series_name'].unique() if series_filter_fn(s)]
                    if not matching:
                        logger.warning(f"PLFS series not found for {driver_name}")
                        return
                    series_name = matching[0]
                    latest_row = df_plfs[
                        (df_plfs['series_name'] == series_name) &
                        (df_plfs['date'] == latest_plfs_date)
                    ]
                    if latest_row.empty:
                        return
                    val = float(latest_row['value'].values[0])
                    # Trend: compare last 2 quarters
                    dates = sorted(df_plfs[df_plfs['series_name'] == series_name]['date'].unique())
                    trend = 'STABLE'
                    if len(dates) >= 2:
                        prev = df_plfs[
                            (df_plfs['series_name'] == series_name) &
                            (df_plfs['date'] == dates[-2])
                        ]
                        if not prev.empty:
                            prev_val = float(prev['value'].values[0])
                            if val > prev_val + 0.3:
                                trend = 'UP'
                            elif val < prev_val - 0.3:
                                trend = 'DOWN'
                    self._update_driver('MACRO', driver_name,
                                       f"{val:.1f}%", trend=trend, result=result)

                # Unemployment rate (15+ age, person, rural+urban, all)
                _sync_plfs_driver(
                    lambda s: 'Unemployment Rate' in s and '- person -' in s
                              and 'rural + urban' in s and '15-29' not in s and '(all)' in s,
                    'unemployment_rate', 'UR'
                )

                # LFPR total (person, rural+urban, all)
                _sync_plfs_driver(
                    lambda s: 'LFPR' in s and '- person -' in s
                              and 'rural + urban' in s and '(all)' in s,
                    'lfpr_total', 'LFPR'
                )

            # ---------------------------------------------------------------
            # 6. GDP/GVA — 4 sectoral GVA Real YoY drivers
            # ---------------------------------------------------------------
            gdp_csv = os.path.join(macro_dir, 'gdp_quarterly.csv')
            if os.path.exists(gdp_csv):
                df_gdp = pd.read_csv(gdp_csv, comment='#')
                df_gdp['date'] = pd.to_datetime(df_gdp['date'])
                latest_gdp_date = df_gdp['date'].max()

                logger.info(f"Syncing GDP/GVA data (as of {latest_gdp_date.strftime('%Y-%m-%d')})")

                gva_macro_map = {
                    'Gross Value Added - Manufacturing (Real)': 'gva_manufacturing_real',
                    'Gross Value Added - Construction (Real)': 'gva_construction_real',
                    'Gross Value Added - Financial, Real Estate & Professional Services (Real)': 'gva_financial_real',
                    'Gross Value Added - Agriculture, Livestock, Forestry and Fishing (Real)': 'gva_agriculture_real',
                }
                for gva_series, driver_name in gva_macro_map.items():
                    latest_row = df_gdp[
                        (df_gdp['series_name'] == gva_series) &
                        (df_gdp['date'] == latest_gdp_date)
                    ]
                    if latest_row.empty:
                        logger.debug(f"GVA series '{gva_series}' not found at latest date")
                        continue
                    current_val = float(latest_row['value'].values[0])

                    # YoY: same quarter previous year
                    yoy_date = latest_gdp_date - pd.DateOffset(years=1)
                    yoy_row = df_gdp[
                        (df_gdp['series_name'] == gva_series) &
                        (df_gdp['date'].dt.month == yoy_date.month) &
                        (df_gdp['date'].dt.year == yoy_date.year)
                    ]
                    if not yoy_row.empty:
                        prev_val = float(yoy_row['value'].values[0])
                        if prev_val > 0:
                            yoy_pct = ((current_val / prev_val) - 1) * 100
                            trend = self._compute_trend(df_gdp, gva_series, months=4)
                            self._update_driver('MACRO', driver_name,
                                               f"{yoy_pct:.2f}%", trend=trend, result=result)
                        else:
                            logger.warning(f"GVA {driver_name}: previous value is zero, skipping YoY")
                    else:
                        logger.debug(f"GVA {driver_name}: no YoY data for {yoy_date}")

                # GVA → GROUP driver mapping (data-driven from macro_metadata.csv)
                gva_group_map = self._load_macro_to_group_mapping('NAS')
                for gva_series_meta, meta in gva_group_map.items():
                    vg = meta['valuation_group']
                    # Find the matching series in GDP data
                    row = df_gdp[
                        (df_gdp['series_name'] == gva_series_meta) &
                        (df_gdp['date'] == latest_gdp_date)
                    ]
                    if not row.empty:
                        val = float(row['value'].values[0])
                        trend = self._compute_trend(df_gdp, gva_series_meta, months=4)
                        driver_name = f"gva_{vg.lower()}"
                        self._update_group_driver(vg, driver_name, f"{val:.1f}",
                                                  trend=trend, result=result,
                                                  driver_category='MACRO_SIGNAL')

            logger.info(f"Macro sync complete: {result['synced']} MACRO updated, "
                        f"{result['group_synced']} GROUP updated, "
                        f"{result['unchanged']} unchanged")
            result['status'] = 'SUCCESS'

        except Exception as e:
            logger.error(f"Macro sync failed: {e}", exc_info=True)
            result['status'] = 'FAILED'
            result['error'] = str(e)

        return result

    def _compute_trend(self, df: 'pd.DataFrame', series_name: str, months: int = 3) -> str:
        """
        Compute trend (UP/DOWN/STABLE) from last N months of data.
        Uses simple linear slope of the time series.
        """
        series_data = df[df['series_name'] == series_name].sort_values('date')
        if len(series_data) < 2:
            return 'STABLE'

        # Take last N data points
        recent = series_data.tail(months)
        values = recent['value'].astype(float).tolist()

        if len(values) < 2:
            return 'STABLE'

        # Simple: compare first and last
        first_val = values[0]
        last_val = values[-1]

        if first_val == 0:
            return 'STABLE'

        pct_change = ((last_val - first_val) / abs(first_val)) * 100

        if pct_change > 2:
            return 'UP'
        elif pct_change < -2:
            return 'DOWN'
        else:
            return 'STABLE'

    def _load_macro_to_group_mapping(self, source_type: str) -> dict:
        """
        Load macro-to-group mapping from macro_metadata.csv (data-driven, not hardcoded).

        Args:
            source_type: Filter by metric_type ('IIP', 'CPI', 'WPI', 'NAS', 'PLFS')

        Returns:
            Dict of {series_name: {'valuation_group': ..., 'valuation_subgroup': ..., 'source': ...}}
            Deduplicates CPI (uses Combined only). Excludes rows with empty valuation_group.
        """
        import pandas as pd

        if not hasattr(self, '_macro_metadata_cache'):
            self._macro_metadata_cache = {}

        if source_type in self._macro_metadata_cache:
            return self._macro_metadata_cache[source_type]

        macro_dir = os.getenv('MACRO_DATA_PATH',
                              '/Users/ram/code/investment_strategies/data/macro')
        metadata_csv = os.path.join(macro_dir, 'macro_metadata.csv')

        if not os.path.exists(metadata_csv):
            logger.warning(f"macro_metadata.csv not found at {metadata_csv}")
            return {}

        df = pd.read_csv(metadata_csv)

        # Filter by source type
        df_filtered = df[df['metric_type'].str.contains(source_type, case=False, na=False)]

        # For CPI, use only Combined to avoid duplicates
        if source_type == 'CPI':
            df_filtered = df_filtered[df_filtered['metric_type'] == 'Combined CPI']

        # Exclude rows with empty valuation_group
        df_filtered = df_filtered[df_filtered['valuation_group'].notna() & (df_filtered['valuation_group'] != '')]

        # Build mapping (deduplicate by series_name — take first occurrence)
        mapping = {}
        for _, row in df_filtered.iterrows():
            sn = row['series_name']
            if sn not in mapping:
                mapping[sn] = {
                    'valuation_group': row['valuation_group'],
                    'valuation_subgroup': row.get('valuation_subgroup', '') if pd.notna(row.get('valuation_subgroup')) else '',
                    'source': source_type,
                }

        self._macro_metadata_cache[source_type] = mapping
        logger.info(f"Loaded {len(mapping)} {source_type} → GROUP mappings from macro_metadata.csv")
        return mapping

    def _sync_single_macro_driver(self, latest_data, csv_series: str,
                                  driver_name: str, fmt: str,
                                  full_df: 'pd.DataFrame', result: dict):
        """Sync a single macro driver from market_indicators.csv."""
        try:
            csv_row = latest_data[latest_data['series_name'] == csv_series]
            if len(csv_row) == 0:
                logger.warning(f"Series '{csv_series}' not found in CSV")
                result['errors'].append(f"{driver_name}: series not found")
                return

            csv_value = csv_row['value'].values[0]

            if fmt == 'pct':
                if csv_value < 1:
                    formatted_value = f"{csv_value * 100:.2f}%"
                else:
                    formatted_value = f"{csv_value:.2f}%"
            else:
                formatted_value = f"{csv_value:.2f}"

            trend = self._compute_trend(full_df, csv_series, months=3)
            self._update_driver('MACRO', driver_name, formatted_value,
                               trend=trend, result=result)

        except Exception as e:
            logger.error(f"Error syncing {driver_name}: {e}")
            result['errors'].append(f"{driver_name}: {str(e)}")

    def _update_driver(self, level: str, driver_name: str, value: str,
                       trend: str = 'STABLE', result: dict = None):
        """Update a single driver in MySQL and optionally GSheet."""
        try:
            current = self.mysql.query_one(
                "SELECT current_value, trend FROM vs_drivers WHERE driver_level = %s AND driver_name = %s",
                (level, driver_name)
            )

            current_value = current['current_value'] if current else None

            if current_value != value or (current and current.get('trend') != trend):
                logger.info(f"  {driver_name}: {current_value} → {value} (trend={trend})")

                if self.gsheet and level == 'MACRO':
                    try:
                        self.gsheet.update_driver_value(
                            'macro_drivers', driver_name, value, column='current_value')
                    except Exception as e:
                        logger.warning(f"GSheet update failed for {driver_name}: {e}")

                if current:
                    self.mysql.execute(
                        """UPDATE vs_drivers
                           SET current_value = %s, trend = %s, last_updated = NOW()
                           WHERE driver_level = %s AND driver_name = %s""",
                        (value, trend, level, driver_name)
                    )
                else:
                    self.mysql.execute(
                        """INSERT INTO vs_drivers
                           (driver_level, driver_name, current_value, trend, weight, last_updated)
                           VALUES (%s, %s, %s, %s, 0.0222, NOW())""",
                        (level, driver_name, value, trend)
                    )

                if result:
                    result['synced'] += 1
            else:
                if result:
                    result['unchanged'] += 1

        except Exception as e:
            logger.error(f"Error updating driver {driver_name}: {e}")
            if result:
                result['errors'].append(f"{driver_name}: {str(e)}")

    def _update_group_driver(self, valuation_group: str, driver_name: str,
                             value: str, trend: str = 'STABLE', result: dict = None,
                             driver_category: str = None):
        """Update or create a GROUP-level driver for sector-mapped macro data."""
        try:
            current = self.mysql.query_one(
                """SELECT current_value FROM vs_drivers
                   WHERE driver_level = 'GROUP' AND driver_name = %s
                   AND valuation_group = %s""",
                (driver_name, valuation_group)
            )

            current_value = current['current_value'] if current else None

            if current_value != value:
                logger.debug(f"  GROUP {valuation_group}/{driver_name}: {current_value} → {value}")

                if current:
                    self.mysql.execute(
                        """UPDATE vs_drivers
                           SET current_value = %s, trend = %s, last_updated = NOW()
                           WHERE driver_level = 'GROUP' AND driver_name = %s
                           AND valuation_group = %s""",
                        (value, trend, driver_name, valuation_group)
                    )
                else:
                    cat = driver_category or 'MACRO_SIGNAL'
                    self.mysql.execute(
                        """INSERT INTO vs_drivers
                           (driver_level, driver_category, driver_name, valuation_group, current_value,
                            trend, weight, impact_direction, updated_by, last_updated)
                           VALUES ('GROUP', %s, %s, %s, %s, %s, 0.10, 'POSITIVE', 'mospi_sync', NOW())""",
                        (cat, driver_name, valuation_group, value, trend)
                    )

                if result:
                    result['group_synced'] += 1
            else:
                if result:
                    result['unchanged'] += 1

        except Exception as e:
            logger.error(f"Error updating group driver {valuation_group}/{driver_name}: {e}")
            if result:
                result['errors'].append(f"{valuation_group}/{driver_name}: {str(e)}")

    def _cascade_macro_to_linked_drivers(self) -> dict:
        """
        After macro sync, cascade MACRO driver states to linked GROUP/SUBGROUP drivers.
        Each linked driver has a linked_macro_driver (name) and link_direction (SAME/INVERSE).

        Cascade logic:
        - SAME: Macro trend/direction maps directly (e.g., PMI up → demand POSITIVE/UP)
        - INVERSE: Direction flips (e.g., WPI up → cost NEGATIVE/UP, trend stays)

        Skips PM_OVERRIDE drivers (PM has manually set them).
        """
        result = {'cascaded': 0, 'skipped_pm': 0, 'errors': []}

        if not self.mysql:
            return result

        try:
            # Get all drivers that have a macro link
            linked_drivers = self.mysql.query(
                """SELECT d.id, d.driver_level, d.driver_name, d.valuation_group,
                          d.valuation_subgroup, d.linked_macro_driver, d.link_direction,
                          d.source, d.impact_direction AS current_direction,
                          d.trend AS current_trend
                   FROM vs_drivers d
                   WHERE d.linked_macro_driver IS NOT NULL
                     AND d.is_active = 1"""
            )

            if not linked_drivers:
                logger.debug("No linked drivers to cascade")
                return result

            # Load current MACRO driver states once
            macro_states = {}
            macro_drivers = self.mysql.query(
                """SELECT driver_name, impact_direction, trend, current_value
                   FROM vs_drivers
                   WHERE driver_level = 'MACRO' AND is_active = 1"""
            )
            for m in (macro_drivers or []):
                macro_states[m['driver_name']] = m

            # Cascade each linked driver
            for ld in linked_drivers:
                macro_name = ld['linked_macro_driver']
                macro_state = macro_states.get(macro_name)

                if not macro_state:
                    logger.debug(f"Linked macro driver '{macro_name}' not found for "
                                 f"{ld['driver_name']}")
                    continue

                # Skip PM overrides
                if ld.get('source') == 'PM_OVERRIDE':
                    result['skipped_pm'] += 1
                    continue

                macro_direction = macro_state.get('impact_direction', 'NEUTRAL')
                macro_trend = macro_state.get('trend', 'STABLE')
                link_dir = ld.get('link_direction', 'SAME')

                if link_dir == 'SAME':
                    new_direction = macro_direction
                    new_trend = macro_trend
                else:  # INVERSE
                    if macro_direction == 'POSITIVE':
                        new_direction = 'NEGATIVE'
                    elif macro_direction == 'NEGATIVE':
                        new_direction = 'POSITIVE'
                    else:
                        new_direction = 'NEUTRAL'
                    new_trend = macro_trend  # Trend direction stays (UP means the macro metric is rising)

                # Only update if changed
                if (new_direction != ld.get('current_direction') or
                        new_trend != ld.get('current_trend')):
                    try:
                        self.mysql.execute(
                            """UPDATE vs_drivers
                               SET impact_direction = %s, trend = %s,
                                   updated_by = 'MACRO_CASCADE', last_updated = NOW()
                               WHERE id = %s""",
                            (new_direction, new_trend, ld['id'])
                        )

                        # Log changelog
                        self.mysql.execute(
                            """INSERT INTO vs_driver_changelog
                               (driver_level, driver_name, sector,
                                old_value, new_value, change_reason, triggered_by)
                               VALUES (%s, %s, %s, %s, %s, %s, 'MACRO_UPDATE')""",
                            (
                                ld['driver_level'],
                                ld['driver_name'],
                                ld.get('valuation_group', ''),
                                f"{ld.get('current_direction')}/{ld.get('current_trend')}",
                                f"{new_direction}/{new_trend}",
                                f"Cascaded from MACRO {macro_name} ({link_dir}): "
                                f"{macro_direction}/{macro_trend}",
                            )
                        )

                        result['cascaded'] += 1
                        logger.debug(f"Cascaded {macro_name} → {ld['driver_name']} "
                                     f"({ld['driver_level']}): {new_direction}/{new_trend}")

                    except Exception as e:
                        logger.error(f"Failed to cascade {macro_name} → {ld['driver_name']}: {e}")
                        result['errors'].append(f"{ld['driver_name']}: {str(e)}")

            logger.info(f"Macro cascade: {result['cascaded']} updated, "
                        f"{result['skipped_pm']} PM overrides skipped")

        except Exception as e:
            logger.error(f"Macro cascade failed: {e}", exc_info=True)
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

        # Structured logging: cycle start
        self.slog.log_cycle_start('daily_valuation')

        try:
            # Check if macro data is stale and update if needed
            macro_update_result = self._check_and_update_macro_data()
            result['macro_update'] = macro_update_result

            # Sync macro data from CSV to GSheet/MySQL
            macro_sync_result = self._sync_macro_from_csv()
            result['macro_sync'] = macro_sync_result

            # Cascade macro updates to linked GROUP/SUBGROUP drivers
            cascade_result = self._cascade_macro_to_linked_drivers()
            result['macro_cascade'] = cascade_result

            # Assess materiality (macro divergences, driver momentum, valuation gaps)
            materiality_result = self._assess_materiality()
            result['materiality'] = materiality_result

            # Process approved driver discoveries
            discovery_result = self._process_approved_discoveries()
            result['discoveries_promoted'] = discovery_result

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

            # --- News Intelligence & Driver Discovery ---

            # Detect price trend anomalies (PE/PB/EV-EBITDA/PS percentiles)
            result['price_trends'] = self._detect_price_trends()

            # Auto-fill empty qualitative (SEED) drivers via LLM + news
            result['qualitative_drivers'] = self._populate_qualitative_drivers()

            # Run trend detection across all group analysts
            result['trend_detection'] = self._run_trend_detection()

            # Sync Tab 7 (Discovered Drivers) and Tab 9 (Materiality Dashboard)
            result['dashboard_sync'] = self._sync_dashboard_tabs()

            # Generate social media draft posts
            result['social_content'] = self._generate_social_content()

            # Send daily intelligence digest email
            result['daily_digest'] = self._send_daily_digest()

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

        # Structured logging: cycle complete
        self.slog.log_cycle_complete(
            cycle_type='daily_valuation',
            elapsed_ms=elapsed * 1000,
            metrics={
                'companies_valued': len(result.get('valuations', {})),
                'alerts_created': result.get('alerts', 0),
                'macro_drivers_synced': result.get('macro_sync', {}).get('synced', 0),
                'price_trend_alerts': result.get('price_trends', {}).get('alerts_created', 0),
                'qualitative_drivers_filled': result.get('qualitative_drivers', {}).get('drivers_filled', 0),
                'social_posts_generated': result.get('social_content', {}).get('posts_generated', 0)
            },
            status=result.get('status', 'UNKNOWN')
        )

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
        if sector_key in self.group_analysts:
            sector_outlook = self.group_analysts[sector_key].calculate_outlook()

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

    def _assess_materiality(self) -> dict:
        """
        Assess materiality across all macro and group drivers.
        Generates alerts when significant divergences or momentum shifts are detected.

        Signal types:
        1. MACRO_DIVERGENCE: Macro driver value >2σ from 12-month mean
        2. DRIVER_MOMENTUM: >3 drivers for a group shift in same direction within 7 days
        3. VALUATION_GAP: CMP moves >10% from last intrinsic value
        4. CROSS_SIGNAL: Macro + sector driver alignment (e.g., rate cut + housing down)

        Returns dict with alert counts.
        """
        import pandas as pd
        from decimal import Decimal

        result = {'alerts_created': 0, 'signals_checked': 0}

        if not self.mysql:
            return result

        today = date.today()

        try:
            # ----------------------------------------------------------
            # 1. MACRO_DIVERGENCE: Check macro drivers for outlier values
            # ----------------------------------------------------------
            macro_dir = os.getenv('MACRO_DATA_PATH',
                                  '/Users/ram/code/investment_strategies/data/macro')
            market_csv = os.path.join(macro_dir, 'market_indicators.csv')

            if os.path.exists(market_csv):
                df = pd.read_csv(market_csv, comment='#')
                df['date'] = pd.to_datetime(df['date'])
                df['value'] = pd.to_numeric(df['value'], errors='coerce')

                for series in df['series_name'].unique():
                    series_data = df[df['series_name'] == series].sort_values('date')
                    if len(series_data) < 6:
                        continue

                    values = series_data['value'].dropna()
                    if len(values) < 6:
                        continue

                    mean_12m = values.tail(12).mean()
                    std_12m = values.tail(12).std()
                    latest = values.iloc[-1]

                    result['signals_checked'] += 1

                    if std_12m > 0 and abs(latest - mean_12m) > 2 * std_12m:
                        direction = 'above' if latest > mean_12m else 'below'
                        sigma = (latest - mean_12m) / std_12m

                        severity = 'HIGH' if abs(sigma) > 3 else 'MEDIUM'
                        action = 'REVALUE_NOW' if severity == 'HIGH' else 'WATCH'

                        try:
                            self.mysql.execute(
                                """INSERT INTO vs_materiality_alerts
                                   (alert_date, alert_type, signal_description,
                                    suggested_action, severity)
                                   VALUES (%s, 'MACRO_DIVERGENCE', %s, %s, %s)""",
                                (
                                    today,
                                    f"{series}: {latest:.2f} is {abs(sigma):.1f}σ {direction} "
                                    f"12-month mean ({mean_12m:.2f})",
                                    action,
                                    severity,
                                )
                            )
                            result['alerts_created'] += 1
                        except Exception as e:
                            logger.error(f"Failed to create macro divergence alert: {e}")

            # ----------------------------------------------------------
            # 2. DRIVER_MOMENTUM: Groups with multiple driver shifts
            # ----------------------------------------------------------
            momentum_sql = """
                SELECT valuation_group, COUNT(*) as changes
                FROM vs_driver_changelog
                WHERE change_timestamp >= DATE_SUB(NOW(), INTERVAL 7 DAY)
                  AND driver_level IN ('GROUP', 'SUBGROUP')
                GROUP BY valuation_group
                HAVING changes >= 3
            """
            momentum_groups = self.mysql.query(momentum_sql)
            for mg in (momentum_groups or []):
                vg = mg['valuation_group']
                changes = mg['changes']

                # Get company count for this group
                company_count = self.mysql.query_one(
                    """SELECT COUNT(*) as cnt FROM vs_active_companies
                       WHERE valuation_group = %s""",
                    (vg,)
                )
                affected = company_count['cnt'] if company_count else 0

                try:
                    self.mysql.execute(
                        """INSERT INTO vs_materiality_alerts
                           (alert_date, alert_type, valuation_group, signal_description,
                            affected_companies, suggested_action, severity)
                           VALUES (%s, 'DRIVER_MOMENTUM', %s, %s, %s, 'WATCH', 'MEDIUM')""",
                        (
                            today,
                            vg,
                            f"{changes} driver changes in 7 days for {vg}",
                            affected,
                        )
                    )
                    result['alerts_created'] += 1
                except Exception as e:
                    logger.error(f"Failed to create momentum alert: {e}")

            # ----------------------------------------------------------
            # 3. VALUATION_GAP: CMP diverging from intrinsic value
            # ----------------------------------------------------------
            gap_sql = """
                SELECT v.company_id, v.intrinsic_value, v.cmp, v.upside_pct,
                       ac.valuation_group, ac.valuation_subgroup, ac.nse_symbol
                FROM vs_valuations v
                JOIN vs_active_companies ac ON v.company_id = ac.company_id
                WHERE v.id IN (
                    SELECT MAX(id) FROM vs_valuations
                    WHERE method = 'BLENDED'
                    GROUP BY company_id
                )
                AND ABS(v.upside_pct) > 10
            """
            gaps = self.mysql.query(gap_sql)
            # Group by valuation_group for summary alerts
            gap_by_group = {}
            for g in (gaps or []):
                vg = g.get('valuation_group', 'UNKNOWN')
                if vg not in gap_by_group:
                    gap_by_group[vg] = []
                gap_by_group[vg].append(g)

            for vg, companies in gap_by_group.items():
                if len(companies) >= 3:  # Only alert if 3+ companies in a group gap
                    avg_upside = sum(float(c.get('upside_pct', 0) or 0) for c in companies) / len(companies)
                    action = 'REVALUE_NOW' if abs(avg_upside) > 20 else 'WATCH'
                    severity = 'HIGH' if abs(avg_upside) > 30 else 'MEDIUM'

                    try:
                        self.mysql.execute(
                            """INSERT INTO vs_materiality_alerts
                               (alert_date, alert_type, valuation_group, signal_description,
                                affected_companies, suggested_action, severity)
                               VALUES (%s, 'VALUATION_GAP', %s, %s, %s, %s, %s)""",
                            (
                                today,
                                vg,
                                f"{len(companies)} companies in {vg} have avg {avg_upside:+.1f}% gap",
                                len(companies),
                                action,
                                severity,
                            )
                        )
                        result['alerts_created'] += 1
                    except Exception as e:
                        logger.error(f"Failed to create valuation gap alert: {e}")

            logger.info(f"Materiality assessment: {result['signals_checked']} signals checked, "
                        f"{result['alerts_created']} alerts created")

        except Exception as e:
            logger.error(f"Materiality assessment failed: {e}", exc_info=True)
            result['error'] = str(e)

        return result

    def _process_approved_discoveries(self) -> dict:
        """
        Promote APPROVED discovered drivers into vs_drivers and log the change.
        Called at start of daily valuation to pick up PM-approved suggestions.
        """
        result = {'promoted': 0, 'errors': []}

        if not self.mysql:
            return result

        try:
            approved = self.mysql.query(
                """SELECT * FROM vs_discovered_drivers WHERE status = 'APPROVED'"""
            )

            if not approved:
                logger.debug("No approved driver discoveries to process")
                return result

            for disc in approved:
                try:
                    # Insert into vs_drivers
                    self.mysql.execute(
                        """INSERT INTO vs_drivers
                           (driver_level, driver_category, driver_name, valuation_group,
                            valuation_subgroup, weight, impact_direction, trend,
                            updated_by, last_updated)
                           VALUES (%s, %s, %s, %s, %s, %s, 'NEUTRAL', 'STABLE',
                                   'AGENT_DISCOVERY', NOW())
                           ON DUPLICATE KEY UPDATE weight = VALUES(weight),
                                                    updated_by = 'AGENT_DISCOVERY'""",
                        (
                            disc['driver_level'],
                            disc.get('driver_category', 'DEMAND'),
                            disc['driver_name'],
                            disc.get('valuation_group'),
                            disc.get('valuation_subgroup'),
                            float(disc.get('suggested_weight', 0.05)),
                        )
                    )

                    # Log to changelog
                    self.mysql.execute(
                        """INSERT INTO vs_driver_changelog
                           (driver_level, driver_category, driver_name, sector,
                            new_value, new_weight, change_reason, triggered_by)
                           VALUES (%s, %s, %s, %s, 'NEW', %s, %s, 'AGENT_ANALYSIS')""",
                        (
                            disc['driver_level'],
                            disc.get('driver_category', 'DEMAND'),
                            disc['driver_name'],
                            disc.get('valuation_group', ''),
                            float(disc.get('suggested_weight', 0.05)),
                            f"Promoted from discovery #{disc['id']}: {disc.get('reasoning', '')}",
                        )
                    )

                    # Mark as processed (update status to prevent re-processing)
                    self.mysql.execute(
                        """UPDATE vs_discovered_drivers
                           SET status = 'APPROVED', reviewed_at = NOW()
                           WHERE id = %s""",
                        (disc['id'],)
                    )

                    result['promoted'] += 1
                    logger.info(f"Promoted discovered driver: {disc['driver_name']} "
                                f"({disc['driver_level']}) → vs_drivers")

                except Exception as e:
                    logger.error(f"Failed to promote discovery #{disc['id']}: {e}", exc_info=True)
                    result['errors'].append(f"#{disc['id']}: {str(e)}")

        except Exception as e:
            logger.error(f"Discovery processing failed: {e}", exc_info=True)
            result['errors'].append(str(e))

        if result['promoted'] > 0:
            logger.info(f"Promoted {result['promoted']} discovered drivers into vs_drivers")

        return result

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
    # NEWS INTELLIGENCE & DRIVER DISCOVERY
    # =========================================================================

    def _detect_price_trends(self) -> dict:
        """Detect monthly PE/PB/evebidta/PS anomalies from price data."""
        result = {'alerts_created': 0, 'companies_analyzed': 0, 'status': 'SKIPPED'}

        if not self.mysql:
            return result

        try:
            from valuation_system.data.processors.price_trend_analyzer import PriceTrendAnalyzer

            analyzer = PriceTrendAnalyzer()
            run_result = analyzer.run_full_analysis(self.mysql)
            result.update(run_result)
            result['status'] = 'SUCCESS'
            logger.info(f"Price trend detection: {run_result.get('self_relative_alerts', 0)} self-relative + "
                        f"{run_result.get('sector_relative_alerts', 0)} sector-relative alerts")

        except Exception as e:
            logger.error(f"Price trend detection failed: {e}", exc_info=True)
            result['status'] = 'FAILED'
            result['error'] = str(e)

        return result

    def _populate_qualitative_drivers(self) -> dict:
        """Auto-fill empty SEED drivers using LLM + recent news analysis."""
        result = {'companies_processed': 0, 'drivers_filled': 0, 'status': 'SKIPPED'}

        if not self.mysql or not self.llm:
            return result

        try:
            from valuation_system.agents.qualitative_driver_agent import QualitativeDriverAgent

            agent = QualitativeDriverAgent(self.mysql, self.llm)
            batch_result = agent.run_batch(max_companies=60)
            result.update(batch_result)
            result['status'] = 'SUCCESS'
            logger.info(f"Qualitative driver auto-fill: {batch_result.get('drivers_filled', 0)} drivers "
                        f"filled across {batch_result.get('companies_processed', 0)} companies")

        except Exception as e:
            logger.error(f"Qualitative driver auto-fill failed: {e}", exc_info=True)
            result['status'] = 'FAILED'
            result['error'] = str(e)

        return result

    def _send_daily_digest(self) -> dict:
        """Generate and send daily intelligence email to PM."""
        result = {'status': 'SKIPPED', 'sent': False}

        if not self.mysql:
            return result

        try:
            from valuation_system.agents.daily_digest_generator import DailyDigestGenerator
            from valuation_system.notifications.email_sender import EmailSender

            email = self.email_sender or EmailSender()
            generator = DailyDigestGenerator(self.mysql, email)
            sent = generator.send_digest()
            result['sent'] = sent
            result['status'] = 'SUCCESS' if sent else 'EMAIL_DISABLED'

        except Exception as e:
            logger.error(f"Daily digest failed: {e}", exc_info=True)
            result['status'] = 'FAILED'
            result['error'] = str(e)

        return result

    def _generate_social_content(self) -> dict:
        """Generate social media draft posts from today's insights."""
        result = {'posts_generated': 0, 'posts_queued': 0, 'status': 'SKIPPED'}

        if not self.content_agent:
            return result

        try:
            posts = self.content_agent.generate_daily_posts()
            result['posts_generated'] = len(posts)

            if posts:
                queued = self.content_agent.publish_posts(posts)
                result['posts_queued'] = sum(1 for p in queued if p.get('queued'))

            result['status'] = 'SUCCESS'
            logger.info(f"Social content: {result['posts_generated']} generated, "
                        f"{result['posts_queued']} queued for PM approval")

        except Exception as e:
            logger.error(f"Social content generation failed: {e}", exc_info=True)
            result['status'] = 'FAILED'
            result['error'] = str(e)

        return result

    def _sync_dashboard_tabs(self) -> dict:
        """Sync Tab 7 (Discovered Drivers), Tab 8 (News Events), Tab 9 (Materiality Dashboard)."""
        result = {'tab7': 0, 'tab8': 0, 'tab9': 0, 'status': 'SKIPPED'}

        if not self.gsheet:
            return result

        try:
            from valuation_system.utils.sync_drivers_to_gsheet import (
                sync_discovered_drivers, sync_news_events, sync_materiality_dashboard
            )

            sheet_id = self.gsheet.spreadsheet_id
            gc = self.gsheet.client

            result['tab7'] = sync_discovered_drivers(gc, sheet_id)
            result['tab8'] = sync_news_events(gc, sheet_id)
            result['tab9'] = sync_materiality_dashboard(gc, sheet_id)
            result['status'] = 'SUCCESS'

        except Exception as e:
            logger.error(f"Dashboard tab sync failed: {e}", exc_info=True)
            result['status'] = 'FAILED'
            result['error'] = str(e)

        return result

    def _run_trend_detection(self) -> dict:
        """Run detect_trend_developments() on all active group analysts."""
        result = {'groups_analyzed': 0, 'trends_found': 0}

        for sector_key, analyst in self.group_analysts.items():
            try:
                trends = analyst.detect_trend_developments()
                if trends:
                    result['trends_found'] += len(trends)
                result['groups_analyzed'] += 1
            except Exception as e:
                logger.error(f"Trend detection failed for {sector_key}: {e}", exc_info=True)

        logger.info(f"Trend detection: {result['trends_found']} trends across "
                    f"{result['groups_analyzed']} groups")
        return result

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
