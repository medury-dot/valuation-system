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
        Read latest macro data from all CSV files, compute trends, update GSheet + MySQL.
        Called daily before valuation to ensure fresh macro data.

        Handles 9 MACRO drivers from 3 sources:
        - market_indicators.csv: gdp_growth, usd_inr, interest_rate_10y, repo_rate, pmi_composite
        - iip_monthly.csv: iip_manufacturing (latest IIP Manufacturing index)
        - cpi_monthly.csv: cpi_food_inflation (latest CPI food inflation YoY)
        - wpi_monthly.csv: wpi_inflation (latest WPI YoY)
        - plfs_quarterly.csv: unemployment_rate (latest PLFS UR)

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
            # 1. MARKET INDICATORS (existing 5 drivers)
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

                # IIP → GROUP driver mapping
                iip_group_map = {
                    'Manufacture of motor vehicles, trailers and semi-trailers': 'AUTO',
                    'Manufacture of pharmaceuticals, medicinal chemical and botanical products': 'HEALTHCARE',
                    'Manufacture of basic metals': 'MATERIALS_METALS',
                    'Manufacture of chemicals and chemical products': 'MATERIALS_CHEMICALS',
                    'Manufacture of food products': 'CONSUMER_STAPLES',
                    'Manufacture of textiles': 'CONSUMER_DISCRETIONARY',
                    'Manufacture of electrical equipment': 'INDUSTRIALS',
                    'Electricity': 'ENERGY_UTILITIES',
                    'Manufacture of computer, electronic and optical products': 'TECHNOLOGY',
                    'Mining': 'MATERIALS_METALS',
                }
                for iip_series, vg in iip_group_map.items():
                    row = latest_iip[latest_iip['series_name'] == iip_series]
                    if not row.empty:
                        val = float(row['value'].values[0])
                        trend = self._compute_trend(df_iip, iip_series, months=3)
                        driver_name = f"iip_{vg.lower()}"
                        self._update_group_driver(vg, driver_name, f"{val:.1f}",
                                                  trend=trend, result=result)

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

                # CPI → GROUP margin signal mapping
                cpi_group_map = {
                    'Food and beverages': 'CONSUMER_STAPLES',
                    'Fuel and light': 'ENERGY_UTILITIES',
                    'Clothing and footwear': 'CONSUMER_DISCRETIONARY',
                    'Housing': 'REAL_ESTATE_INFRA',
                    'Health': 'HEALTHCARE',
                    'Transport and communication': 'AUTO',
                }
                for cpi_series, vg in cpi_group_map.items():
                    row = latest_cpi[latest_cpi['series_name'] == cpi_series]
                    if not row.empty:
                        val = float(row['value'].values[0])
                        trend = self._compute_trend(df_cpi_combined, cpi_series, months=3)
                        driver_name = f"cpi_{vg.lower()}"
                        self._update_group_driver(vg, driver_name, f"{val:.1f}",
                                                  trend=trend, result=result)

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

            # ---------------------------------------------------------------
            # 5. PLFS — unemployment_rate MACRO driver
            # ---------------------------------------------------------------
            plfs_csv = os.path.join(macro_dir, 'plfs_quarterly.csv')
            if os.path.exists(plfs_csv):
                df_plfs = pd.read_csv(plfs_csv, comment='#')
                df_plfs['date'] = pd.to_datetime(df_plfs['date'])
                latest_plfs_date = df_plfs['date'].max()

                logger.info(f"Syncing PLFS data (as of {latest_plfs_date.strftime('%Y-%m-%d')})")

                # Find overall UR for 15+ age group, total sector
                ur_series = [s for s in df_plfs['series_name'].unique()
                             if 'Unemployment Rate' in s and '- total' in s.lower()
                             and '15-29' not in s]
                if ur_series:
                    ur_name = ur_series[0]
                    ur_latest = df_plfs[
                        (df_plfs['series_name'] == ur_name) &
                        (df_plfs['date'] == latest_plfs_date)
                    ]
                    if not ur_latest.empty:
                        val = float(ur_latest['value'].values[0])
                        # Simple trend: compare last 2 quarters
                        dates = sorted(df_plfs[df_plfs['series_name'] == ur_name]['date'].unique())
                        if len(dates) >= 2:
                            prev = df_plfs[
                                (df_plfs['series_name'] == ur_name) &
                                (df_plfs['date'] == dates[-2])
                            ]
                            if not prev.empty:
                                prev_val = float(prev['value'].values[0])
                                if val > prev_val + 0.3:
                                    trend = 'UP'
                                elif val < prev_val - 0.3:
                                    trend = 'DOWN'
                                else:
                                    trend = 'STABLE'
                            else:
                                trend = 'STABLE'
                        else:
                            trend = 'STABLE'
                        self._update_driver('MACRO', 'unemployment_rate',
                                           f"{val:.1f}%", trend=trend, result=result)

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
                             value: str, trend: str = 'STABLE', result: dict = None):
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
                    self.mysql.execute(
                        """INSERT INTO vs_drivers
                           (driver_level, driver_name, valuation_group, current_value,
                            trend, weight, impact_direction, updated_by, last_updated)
                           VALUES ('GROUP', %s, %s, %s, %s, 0.10, 'POSITIVE', 'mospi_sync', NOW())""",
                        (driver_name, valuation_group, value, trend)
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
