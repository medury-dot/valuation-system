"""
Price Trend Analyzer
Detects valuation anomalies using percentile-based analysis on monthly price data.

Methodology:
1. For each company, compute its own 3-year (36-month) percentile distribution
   of PE, PB, EV/EBITDA, P/S ratios from monthly snapshots.
2. Flag current month as anomaly if ratio is below 10th percentile (potential value)
   or above 90th percentile (potential overvaluation).
3. Compute sector-relative comparison: is the company's current ratio cheap/expensive
   vs its valuation_subgroup peers' current medians?

Data source: combined_monthly_prices.csv (~910K rows, monthly snapshots since 1997)
  Columns: accode, bse_code, nse_symbol, Company Name, daily_date, close, pe, pb,
           evebidta (NOT ev_ebitda), ps, mcap, exchange, year_month

Integration:
  - Reads company list from MySQL vs_active_companies
  - Writes alerts to MySQL vs_materiality_alerts

All data from actual sources -- no synthetic/fabricated values.
"""

import os
import sys
import logging
import traceback
from datetime import datetime, date
from typing import Optional

import numpy as np
import pandas as pd
from dotenv import load_dotenv

# Ensure valuation_system package is importable
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', '..'))

logger = logging.getLogger(__name__)

# Load environment from valuation_system config
load_dotenv(os.path.join(os.path.dirname(__file__), '..', '..', 'config', '.env'))

# Valuation ratios to analyze (column names as they appear in the CSV)
RATIO_COLUMNS = ['pe', 'pb', 'evebidta', 'ps']

# Percentile thresholds for self-relative anomaly detection
# Tighter thresholds to surface only truly extreme anomalies for PM
ANOMALY_LOW_PCTILE = 5     # Below 5th pctile = WATCH (potential deep value)
ANOMALY_HIGH_PCTILE = 95   # Above 95th pctile = WATCH (potential overvaluation)
URGENT_LOW_PCTILE = 3      # Below 3rd pctile = REVALUE_NOW
URGENT_HIGH_PCTILE = 97    # Above 97th pctile = REVALUE_NOW

# Lookback window for self-relative percentile computation (months)
LOOKBACK_MONTHS = 36

# Minimum data points required for reliable percentile computation
MIN_DATA_POINTS = 12

# Sector-relative thresholds (deviation from subgroup median)
# Wider thresholds to reduce noise — only flag meaningful divergences
SECTOR_REL_CHEAP_PCT = -50     # 50% below subgroup median = cheap
SECTOR_REL_EXPENSIVE_PCT = 50  # 50% above subgroup median = expensive
SECTOR_REL_URGENT_PCT = 70     # 70% deviation = REVALUE_NOW

# Minimum peers required for sector-relative comparison
MIN_PEERS_FOR_SECTOR_REL = 3

# Minimum market cap (in Crores) to filter out illiquid micro-caps
# Micro-caps have erratic ratios that create noise, not signal
MIN_MCAP_CR = 500

# Maximum sane ratio values — anything beyond is data quality issue, not signal
# PE > 200 = loss-making or near-zero earnings; PB > 50 = accounting anomaly
MAX_SANE_RATIOS = {'pe': 200, 'pb': 50, 'evebidta': 100, 'ps': 50}


class PriceTrendAnalyzer:
    """
    Percentile-based price trend analyzer for equity valuation system.

    Uses monthly price snapshots to detect:
    1. Self-relative anomalies: company trading at extreme percentiles of its own history
    2. Sector-relative anomalies: company trading far from its valuation_subgroup peers

    Prefers NSE data when a company appears on both exchanges.
    """

    def __init__(self, prices_csv_path: Optional[str] = None):
        """
        Initialize with monthly prices CSV.

        Args:
            prices_csv_path: Path to combined_monthly_prices.csv.
                             If None, reads MONTHLY_PRICES_PATH from .env.
        """
        self.prices_csv_path = prices_csv_path or os.getenv('MONTHLY_PRICES_PATH')
        if not self.prices_csv_path:
            raise ValueError(
                "No prices CSV path provided and MONTHLY_PRICES_PATH not set in .env. "
                "Cannot initialize PriceTrendAnalyzer."
            )
        if not os.path.exists(self.prices_csv_path):
            raise FileNotFoundError(
                f"Monthly prices CSV not found at: {self.prices_csv_path}"
            )

        logger.info(f"Loading monthly prices from: {self.prices_csv_path}")
        load_start = datetime.now()
        self.prices_df = pd.read_csv(self.prices_csv_path, low_memory=False)
        load_elapsed = (datetime.now() - load_start).total_seconds()
        logger.info(
            f"Loaded {len(self.prices_df)} rows, {len(self.prices_df.columns)} columns "
            f"in {load_elapsed:.1f}s"
        )

        # Parse daily_date to datetime for date-based filtering
        self.prices_df['daily_date'] = pd.to_datetime(
            self.prices_df['daily_date'], errors='coerce'
        )

        # Convert accode to string for consistent matching (CSV has float like 124622.0)
        self.prices_df['accode'] = self.prices_df['accode'].apply(
            lambda x: str(int(float(x))) if pd.notna(x) and str(x).replace('.', '').replace('-', '').isdigit() else None
        )
        # Convert bse_code similarly
        self.prices_df['bse_code'] = self.prices_df['bse_code'].apply(
            lambda x: str(int(float(x))) if pd.notna(x) and str(x).replace('.', '').replace('-', '').isdigit() else None
        )

        # Deduplicate: prefer NSE rows when company appears on both exchanges
        self._deduplicate_exchanges()

        # Convert ratio columns to numeric (coerce non-numeric to NaN)
        for col in RATIO_COLUMNS:
            if col in self.prices_df.columns:
                self.prices_df[col] = pd.to_numeric(self.prices_df[col], errors='coerce')

        logger.info(
            f"After dedup: {len(self.prices_df)} rows. "
            f"Date range: {self.prices_df['daily_date'].min()} to {self.prices_df['daily_date'].max()}"
        )

    def _deduplicate_exchanges(self):
        """
        When a company appears on both BSE and NSE in the same month,
        keep only the NSE row (more liquid, more reliable pricing).
        """
        initial_count = len(self.prices_df)

        # For companies with accode (most NSE-listed), group by accode + year_month
        # and prefer exchange='nse'
        has_accode = self.prices_df['accode'].notna()
        df_with_accode = self.prices_df[has_accode].copy()
        df_without_accode = self.prices_df[~has_accode].copy()

        if not df_with_accode.empty:
            # Sort so NSE comes first (nse < bse alphabetically, but let's be explicit)
            df_with_accode['_exchange_priority'] = df_with_accode['exchange'].map(
                {'nse': 0, 'bse': 1}
            ).fillna(2)
            df_with_accode = df_with_accode.sort_values('_exchange_priority')
            df_with_accode = df_with_accode.drop_duplicates(
                subset=['accode', 'year_month'], keep='first'
            )
            df_with_accode = df_with_accode.drop(columns=['_exchange_priority'])

        self.prices_df = pd.concat([df_with_accode, df_without_accode], ignore_index=True)
        dedup_removed = initial_count - len(self.prices_df)
        if dedup_removed > 0:
            logger.info(
                f"Exchange dedup: removed {dedup_removed} duplicate BSE rows "
                f"(kept NSE where both exist)"
            )

    def _find_company_rows(self, accord_code: str, bse_code: str, nse_symbol: str) -> pd.DataFrame:
        """
        Find price rows for a company using accord_code (primary) or bse_code (fallback).

        Args:
            accord_code: Company's accord_code (maps to 'accode' in CSV)
            bse_code: Company's BSE code (fallback identifier)
            nse_symbol: Company's NSE symbol (not used for lookup but logged)

        Returns:
            DataFrame of matching rows sorted by daily_date ascending
        """
        mask = pd.Series(False, index=self.prices_df.index)

        # Primary: match by accord_code -> accode
        if accord_code and str(accord_code).strip():
            ac_str = str(accord_code).strip()
            mask = self.prices_df['accode'] == ac_str

        # Fallback: match by bse_code
        if mask.sum() == 0 and bse_code and str(bse_code).strip():
            bse_str = str(bse_code).strip()
            mask = self.prices_df['bse_code'] == bse_str

        matched = self.prices_df[mask].copy()
        if matched.empty:
            logger.debug(
                f"No price rows found for accord_code={accord_code}, "
                f"bse_code={bse_code}, nse_symbol={nse_symbol}"
            )
            return matched

        matched = matched.sort_values('daily_date')
        logger.debug(
            f"Found {len(matched)} price rows for {nse_symbol or accord_code}: "
            f"{matched['daily_date'].min().date()} to {matched['daily_date'].max().date()}"
        )
        return matched

    def _get_latest_and_history(self, company_rows: pd.DataFrame):
        """
        Extract latest month's data and the lookback history.

        Returns:
            (latest_row: pd.Series, history_df: pd.DataFrame) or (None, None)
        """
        if company_rows.empty:
            return None, None

        # Latest row is the most recent month
        latest_row = company_rows.iloc[-1]
        latest_date = latest_row['daily_date']

        # History: last LOOKBACK_MONTHS months (excluding the latest)
        cutoff_date = latest_date - pd.DateOffset(months=LOOKBACK_MONTHS)
        history = company_rows[
            (company_rows['daily_date'] >= cutoff_date) &
            (company_rows['daily_date'] < latest_date)
        ]

        return latest_row, history

    def detect_anomalies(self, company_lookups: list) -> list:
        """
        Detect self-relative anomalies: company trading at extreme percentiles
        of its own 3-year history.

        Args:
            company_lookups: list of dicts with keys:
                company_id, accord_code, bse_code, nse_symbol,
                valuation_group, valuation_subgroup

        Returns:
            list of alert dicts ready for vs_materiality_alerts insertion
        """
        alerts = []
        processed = 0
        skipped_no_data = 0
        skipped_insufficient = 0

        logger.info(
            f"Starting self-relative anomaly detection for {len(company_lookups)} companies"
        )

        for lookup in company_lookups:
            company_id = lookup.get('company_id')
            accord_code = lookup.get('accord_code')
            bse_code = lookup.get('bse_code')
            nse_symbol = lookup.get('nse_symbol')
            valuation_group = lookup.get('valuation_group')
            valuation_subgroup = lookup.get('valuation_subgroup')
            company_name = lookup.get('company_name', nse_symbol or accord_code)

            try:
                rows = self._find_company_rows(accord_code, bse_code, nse_symbol)
                if rows.empty:
                    skipped_no_data += 1
                    continue

                latest, history = self._get_latest_and_history(rows)
                if latest is None or history is None or len(history) < MIN_DATA_POINTS:
                    skipped_insufficient += 1
                    logger.debug(
                        f"Insufficient history for {company_name}: "
                        f"{len(history) if history is not None else 0} months "
                        f"(need {MIN_DATA_POINTS})"
                    )
                    continue

                for ratio_col in RATIO_COLUMNS:
                    current_val = latest.get(ratio_col)
                    if pd.isna(current_val) or current_val is None:
                        continue

                    current_val = float(current_val)

                    # Filter out non-positive ratios (meaningless for PE/PB etc.)
                    hist_values = history[ratio_col].dropna()
                    hist_values = hist_values[hist_values > 0]

                    if len(hist_values) < MIN_DATA_POINTS:
                        logger.debug(
                            f"{company_name} {ratio_col}: only {len(hist_values)} "
                            f"valid data points (need {MIN_DATA_POINTS}), skipping"
                        )
                        continue

                    if current_val <= 0:
                        # Negative PE/PB etc. means losses -- not useful for percentile
                        continue

                    # Skip insane ratio values (data quality issue, not real signal)
                    max_sane = MAX_SANE_RATIOS.get(ratio_col, 200)
                    if current_val > max_sane:
                        logger.debug(f"{company_name} {ratio_col}={current_val:.1f} exceeds max sane {max_sane}, skipping")
                        continue

                    # Compute percentile of current value within 3-year history
                    percentile = float(
                        (hist_values < current_val).sum() / len(hist_values) * 100
                    )
                    median_val = float(hist_values.median())
                    p10 = float(hist_values.quantile(0.10))
                    p90 = float(hist_values.quantile(0.90))
                    p5 = float(hist_values.quantile(0.05))
                    p95 = float(hist_values.quantile(0.95))

                    # Deviation from 3-year median
                    deviation_pct = (
                        ((current_val - median_val) / median_val) * 100
                        if median_val != 0 else 0.0
                    )

                    logger.debug(
                        f"SELF-REL {company_name} {ratio_col}: "
                        f"current={current_val:.2f}, median={median_val:.2f}, "
                        f"pctile={percentile:.1f}%, p5={p5:.2f}, p10={p10:.2f}, "
                        f"p90={p90:.2f}, p95={p95:.2f}, dev={deviation_pct:+.1f}%"
                    )

                    # Check for anomaly
                    is_anomaly = False
                    severity = None
                    suggested_action = None
                    reasoning = None

                    if percentile <= URGENT_LOW_PCTILE:
                        is_anomaly = True
                        severity = 'HIGH'
                        suggested_action = 'REVALUE_NOW'
                        reasoning = (
                            f"{company_name} {ratio_col.upper()}={current_val:.2f} is at "
                            f"{percentile:.0f}th percentile of its 3yr history "
                            f"(below 5th pctile={p5:.2f}). 3yr median={median_val:.2f}. "
                            f"Deviation {deviation_pct:+.1f}% from median. "
                            f"Potential deep value based on {len(hist_values)} months of data."
                        )
                    elif percentile <= ANOMALY_LOW_PCTILE:
                        is_anomaly = True
                        severity = 'MEDIUM'
                        suggested_action = 'WATCH'
                        reasoning = (
                            f"{company_name} {ratio_col.upper()}={current_val:.2f} is at "
                            f"{percentile:.0f}th percentile of its 3yr history "
                            f"(below 10th pctile={p10:.2f}). 3yr median={median_val:.2f}. "
                            f"Deviation {deviation_pct:+.1f}% from median. "
                            f"Potential value based on {len(hist_values)} months of data."
                        )
                    elif percentile >= URGENT_HIGH_PCTILE:
                        is_anomaly = True
                        severity = 'HIGH'
                        suggested_action = 'REVALUE_NOW'
                        reasoning = (
                            f"{company_name} {ratio_col.upper()}={current_val:.2f} is at "
                            f"{percentile:.0f}th percentile of its 3yr history "
                            f"(above 95th pctile={p95:.2f}). 3yr median={median_val:.2f}. "
                            f"Deviation {deviation_pct:+.1f}% from median. "
                            f"Potential overvaluation based on {len(hist_values)} months of data."
                        )
                    elif percentile >= ANOMALY_HIGH_PCTILE:
                        is_anomaly = True
                        severity = 'MEDIUM'
                        suggested_action = 'WATCH'
                        reasoning = (
                            f"{company_name} {ratio_col.upper()}={current_val:.2f} is at "
                            f"{percentile:.0f}th percentile of its 3yr history "
                            f"(above 90th pctile={p90:.2f}). 3yr median={median_val:.2f}. "
                            f"Deviation {deviation_pct:+.1f}% from median. "
                            f"Potential overvaluation based on {len(hist_values)} months of data."
                        )

                    if is_anomaly:
                        alert = {
                            'alert_date': date.today().isoformat(),
                            'alert_type': 'VALUATION_GAP',
                            'severity': severity,
                            'scope': 'COMPANY',
                            'company_id': company_id,
                            'valuation_group': valuation_group,
                            'valuation_subgroup': valuation_subgroup,
                            'driver_affected': ratio_col,
                            'current_value': str(round(current_val, 4)),
                            'baseline_value': str(round(median_val, 4)),
                            'deviation_pct': round(deviation_pct, 2),
                            'suggested_action': suggested_action,
                            'signal_description': (
                                f"Self-relative: {ratio_col.upper()} at "
                                f"{percentile:.0f}th pctile of 3yr history"
                            ),
                            'reasoning': reasoning,
                        }
                        alerts.append(alert)
                        logger.info(
                            f"ALERT: {company_name} {ratio_col.upper()} "
                            f"self-relative anomaly: pctile={percentile:.0f}%, "
                            f"severity={severity}, action={suggested_action}"
                        )

                processed += 1

            except Exception as e:
                logger.error(
                    f"Error processing {company_name} (company_id={company_id}): {e}\n"
                    f"{traceback.format_exc()}"
                )
                continue

        logger.info(
            f"Self-relative anomaly detection complete: "
            f"processed={processed}, skipped_no_data={skipped_no_data}, "
            f"skipped_insufficient={skipped_insufficient}, alerts={len(alerts)}"
        )
        return alerts

    def detect_sector_relative_anomalies(self, company_lookups: list) -> list:
        """
        Detect sector-relative anomalies: company's current ratios vs its
        valuation_subgroup peers' current medians.

        For each valuation_subgroup, computes the median of the latest-month
        ratio across all companies in that subgroup. Then flags companies
        whose current ratio deviates significantly from the subgroup median.

        Args:
            company_lookups: list of dicts with keys:
                company_id, accord_code, bse_code, nse_symbol,
                valuation_group, valuation_subgroup

        Returns:
            list of alert dicts ready for vs_materiality_alerts insertion
        """
        alerts = []

        logger.info(
            f"Starting sector-relative anomaly detection for "
            f"{len(company_lookups)} companies"
        )

        # Group companies by valuation_subgroup
        subgroup_map = {}
        for lookup in company_lookups:
            subgroup = lookup.get('valuation_subgroup')
            if not subgroup:
                continue
            if subgroup not in subgroup_map:
                subgroup_map[subgroup] = []
            subgroup_map[subgroup].append(lookup)

        logger.info(f"Found {len(subgroup_map)} unique valuation_subgroups")

        for subgroup, members in subgroup_map.items():
            if len(members) < MIN_PEERS_FOR_SECTOR_REL:
                logger.debug(
                    f"Subgroup {subgroup}: only {len(members)} members "
                    f"(need {MIN_PEERS_FOR_SECTOR_REL}), skipping sector-relative"
                )
                continue

            # Collect current ratios for all members in this subgroup
            member_current_ratios = {}  # {ratio_col: [(company_lookup, value), ...]}
            for ratio_col in RATIO_COLUMNS:
                member_current_ratios[ratio_col] = []

            for lookup in members:
                accord_code = lookup.get('accord_code')
                bse_code = lookup.get('bse_code')
                nse_symbol = lookup.get('nse_symbol')

                rows = self._find_company_rows(accord_code, bse_code, nse_symbol)
                if rows.empty:
                    continue

                latest = rows.iloc[-1]
                for ratio_col in RATIO_COLUMNS:
                    val = latest.get(ratio_col)
                    if pd.notna(val) and float(val) > 0:
                        member_current_ratios[ratio_col].append(
                            (lookup, float(val))
                        )

            # Compute subgroup medians and flag deviations
            for ratio_col in RATIO_COLUMNS:
                ratio_data = member_current_ratios[ratio_col]
                if len(ratio_data) < MIN_PEERS_FOR_SECTOR_REL:
                    logger.debug(
                        f"Subgroup {subgroup} {ratio_col}: only {len(ratio_data)} "
                        f"valid values (need {MIN_PEERS_FOR_SECTOR_REL}), skipping"
                    )
                    continue

                values = [v for _, v in ratio_data]
                subgroup_median = float(np.median(values))
                subgroup_p25 = float(np.percentile(values, 25))
                subgroup_p75 = float(np.percentile(values, 75))

                logger.debug(
                    f"SECTOR-REL {subgroup} {ratio_col}: "
                    f"median={subgroup_median:.2f}, p25={subgroup_p25:.2f}, "
                    f"p75={subgroup_p75:.2f}, n_companies={len(ratio_data)}"
                )

                for lookup, current_val in ratio_data:
                    company_id = lookup.get('company_id')
                    nse_symbol = lookup.get('nse_symbol')
                    company_name = lookup.get('company_name', nse_symbol or str(company_id))
                    valuation_group = lookup.get('valuation_group')

                    if subgroup_median == 0:
                        continue

                    # Skip insane ratio values
                    max_sane = MAX_SANE_RATIOS.get(ratio_col, 200)
                    if current_val > max_sane:
                        continue

                    deviation_pct = (
                        ((current_val - subgroup_median) / subgroup_median) * 100
                    )

                    logger.debug(
                        f"SECTOR-REL {company_name} {ratio_col}: "
                        f"current={current_val:.2f}, subgroup_median={subgroup_median:.2f}, "
                        f"dev={deviation_pct:+.1f}%"
                    )

                    # Check for sector-relative anomaly
                    is_anomaly = False
                    severity = None
                    suggested_action = None
                    reasoning = None

                    if deviation_pct <= -abs(SECTOR_REL_URGENT_PCT):
                        is_anomaly = True
                        severity = 'HIGH'
                        suggested_action = 'REVALUE_NOW'
                        reasoning = (
                            f"{company_name} {ratio_col.upper()}={current_val:.2f} is "
                            f"{deviation_pct:+.1f}% below subgroup {subgroup} median "
                            f"({subgroup_median:.2f}). Subgroup range: p25={subgroup_p25:.2f}, "
                            f"p75={subgroup_p75:.2f} across {len(ratio_data)} peers. "
                            f"Significant relative undervaluation."
                        )
                    elif deviation_pct <= SECTOR_REL_CHEAP_PCT:
                        is_anomaly = True
                        severity = 'MEDIUM'
                        suggested_action = 'WATCH'
                        reasoning = (
                            f"{company_name} {ratio_col.upper()}={current_val:.2f} is "
                            f"{deviation_pct:+.1f}% below subgroup {subgroup} median "
                            f"({subgroup_median:.2f}). Subgroup range: p25={subgroup_p25:.2f}, "
                            f"p75={subgroup_p75:.2f} across {len(ratio_data)} peers. "
                            f"Relatively cheap within sector."
                        )
                    elif deviation_pct >= abs(SECTOR_REL_URGENT_PCT):
                        is_anomaly = True
                        severity = 'HIGH'
                        suggested_action = 'REVALUE_NOW'
                        reasoning = (
                            f"{company_name} {ratio_col.upper()}={current_val:.2f} is "
                            f"{deviation_pct:+.1f}% above subgroup {subgroup} median "
                            f"({subgroup_median:.2f}). Subgroup range: p25={subgroup_p25:.2f}, "
                            f"p75={subgroup_p75:.2f} across {len(ratio_data)} peers. "
                            f"Significant relative overvaluation."
                        )
                    elif deviation_pct >= SECTOR_REL_EXPENSIVE_PCT:
                        is_anomaly = True
                        severity = 'MEDIUM'
                        suggested_action = 'WATCH'
                        reasoning = (
                            f"{company_name} {ratio_col.upper()}={current_val:.2f} is "
                            f"{deviation_pct:+.1f}% above subgroup {subgroup} median "
                            f"({subgroup_median:.2f}). Subgroup range: p25={subgroup_p25:.2f}, "
                            f"p75={subgroup_p75:.2f} across {len(ratio_data)} peers. "
                            f"Relatively expensive within sector."
                        )

                    if is_anomaly:
                        alert = {
                            'alert_date': date.today().isoformat(),
                            'alert_type': 'VALUATION_GAP',
                            'severity': severity,
                            'scope': 'COMPANY',
                            'company_id': company_id,
                            'valuation_group': valuation_group,
                            'valuation_subgroup': subgroup,
                            'driver_affected': ratio_col,
                            'current_value': str(round(current_val, 4)),
                            'baseline_value': str(round(subgroup_median, 4)),
                            'deviation_pct': round(deviation_pct, 2),
                            'suggested_action': suggested_action,
                            'signal_description': (
                                f"Sector-relative: {ratio_col.upper()} "
                                f"{deviation_pct:+.0f}% vs {subgroup} median"
                            ),
                            'reasoning': reasoning,
                        }
                        alerts.append(alert)
                        logger.info(
                            f"ALERT: {company_name} {ratio_col.upper()} "
                            f"sector-relative anomaly vs {subgroup}: "
                            f"dev={deviation_pct:+.1f}%, severity={severity}"
                        )

        logger.info(
            f"Sector-relative anomaly detection complete: "
            f"subgroups_analyzed={len(subgroup_map)}, alerts={len(alerts)}"
        )
        return alerts

    def upsert_alerts_to_db(self, alerts: list, mysql_client) -> int:
        """
        Batch insert alerts into vs_materiality_alerts.

        Deduplicates by (alert_date, company_id, driver_affected, signal_description)
        to avoid flooding the table with repeated daily runs.

        Args:
            alerts: list of alert dicts (from detect_anomalies or detect_sector_relative_anomalies)
            mysql_client: ValuationMySQLClient instance

        Returns:
            Number of alerts inserted
        """
        if not alerts:
            logger.info("No alerts to insert")
            return 0

        logger.info(f"Upserting {len(alerts)} alerts to vs_materiality_alerts")

        # Delete existing VALUATION_GAP alerts for today from this analyzer
        # to avoid duplicates on re-runs
        today = date.today().isoformat()
        try:
            deleted = mysql_client.execute(
                "DELETE FROM vs_materiality_alerts "
                "WHERE alert_date = %s AND alert_type = 'VALUATION_GAP' "
                "AND scope = 'COMPANY' AND driver_affected IN ('pe', 'pb', 'evebidta', 'ps')",
                (today,)
            )
            logger.info(
                f"Cleared {deleted} existing VALUATION_GAP alerts for {today} "
                f"before re-inserting"
            )
        except Exception as e:
            logger.error(
                f"Error clearing old alerts: {e}\n{traceback.format_exc()}"
            )
            raise

        # Batch insert in chunks of 100 to respect rate limits
        chunk_size = 100
        total_inserted = 0

        for i in range(0, len(alerts), chunk_size):
            chunk = alerts[i:i + chunk_size]
            try:
                inserted = mysql_client.insert_batch('vs_materiality_alerts', chunk)
                total_inserted += inserted
                logger.debug(
                    f"Inserted chunk {i // chunk_size + 1}: "
                    f"{inserted} alerts (cumulative: {total_inserted})"
                )
            except Exception as e:
                logger.error(
                    f"Error inserting alert chunk starting at index {i}: {e}\n"
                    f"{traceback.format_exc()}"
                )
                # Log individual alerts in the failed chunk for debugging
                for j, alert in enumerate(chunk):
                    logger.error(
                        f"  Failed alert [{i + j}]: company_id={alert.get('company_id')}, "
                        f"driver={alert.get('driver_affected')}, "
                        f"current={alert.get('current_value')}, "
                        f"baseline={alert.get('baseline_value')}"
                    )
                raise

        logger.info(
            f"Upsert complete: {total_inserted} alerts inserted into vs_materiality_alerts"
        )
        return total_inserted

    def run_full_analysis(self, mysql_client) -> dict:
        """
        Run complete price trend analysis:
        1. Load active companies from vs_active_companies
        2. Detect self-relative anomalies
        3. Detect sector-relative anomalies
        4. Upsert all alerts to database

        Args:
            mysql_client: ValuationMySQLClient instance

        Returns:
            dict with summary stats:
                companies_analyzed, self_alerts, sector_alerts, total_inserted
        """
        logger.info("=" * 70)
        logger.info("PRICE TREND ANALYZER - Full Analysis Run")
        logger.info("=" * 70)
        run_start = datetime.now()

        # Step 1: Load active companies from vs_active_companies
        logger.info("Step 1: Loading active companies from vs_active_companies")
        company_rows = mysql_client.query(
            "SELECT company_id, accord_code, bse_code, nse_symbol, "
            "company_name, valuation_group, valuation_subgroup "
            "FROM vs_active_companies "
            "WHERE is_active = 1 "
            "AND valuation_group IS NOT NULL "
            "AND valuation_group != 'NOT_CLASSIFIED' "
            "AND valuation_group != 'NON_OPERATING'"
        )
        logger.info(f"Loaded {len(company_rows)} active companies for analysis")

        if not company_rows:
            logger.warning("No active companies found in vs_active_companies. Aborting.")
            return {
                'companies_analyzed': 0,
                'self_alerts': 0,
                'sector_alerts': 0,
                'total_inserted': 0,
            }

        # Step 1b: Filter by market cap to remove illiquid micro-caps
        # Their erratic ratios create noise, not investable signals
        if MIN_MCAP_CR > 0:
            before_count = len(company_rows)
            # Build latest mcap lookup from prices data (most recent month per accode)
            latest_prices = self.prices_df.sort_values('daily_date', ascending=False)
            latest_mcap_by_accode = latest_prices.drop_duplicates(subset='accode', keep='first').set_index('accode')['mcap']
            latest_mcap_by_bse = latest_prices.drop_duplicates(subset='bse_code', keep='first').set_index('bse_code')['mcap']

            filtered_rows = []
            for row in company_rows:
                accode = str(row.get('accord_code', '')).strip()
                bse_code = str(row.get('bse_code', '')).strip()
                mcap = None
                if accode and accode in latest_mcap_by_accode.index:
                    mcap = pd.to_numeric(latest_mcap_by_accode.get(accode), errors='coerce')
                elif bse_code and bse_code in latest_mcap_by_bse.index:
                    mcap = pd.to_numeric(latest_mcap_by_bse.get(bse_code), errors='coerce')
                if mcap and mcap >= MIN_MCAP_CR:
                    filtered_rows.append(row)
            company_rows = filtered_rows
            logger.info(f"Market cap filter (>={MIN_MCAP_CR} Cr): {before_count} → {len(company_rows)} companies")

        # Step 2: Self-relative anomaly detection
        logger.info("Step 2: Self-relative anomaly detection")
        self_alerts = self.detect_anomalies(company_rows)

        # Step 3: Sector-relative anomaly detection
        logger.info("Step 3: Sector-relative anomaly detection")
        sector_alerts = self.detect_sector_relative_anomalies(company_rows)

        # Combine alerts
        all_alerts = self_alerts + sector_alerts
        logger.info(
            f"Total alerts: {len(all_alerts)} "
            f"(self-relative={len(self_alerts)}, sector-relative={len(sector_alerts)})"
        )

        # Step 4: Upsert to database
        logger.info("Step 4: Upserting alerts to vs_materiality_alerts")
        total_inserted = self.upsert_alerts_to_db(all_alerts, mysql_client)

        run_elapsed = (datetime.now() - run_start).total_seconds()

        # Summary
        summary = {
            'companies_analyzed': len(company_rows),
            'self_alerts': len(self_alerts),
            'sector_alerts': len(sector_alerts),
            'total_inserted': total_inserted,
            'run_time_seconds': round(run_elapsed, 1),
        }

        logger.info("=" * 70)
        logger.info(f"PRICE TREND ANALYZER - Run Complete in {run_elapsed:.1f}s")
        logger.info(f"  Companies analyzed: {summary['companies_analyzed']}")
        logger.info(f"  Self-relative alerts: {summary['self_alerts']}")
        logger.info(f"  Sector-relative alerts: {summary['sector_alerts']}")
        logger.info(f"  Total alerts inserted: {summary['total_inserted']}")
        logger.info("=" * 70)

        return summary


# =============================================================================
# CLI entry point for standalone runs
# =============================================================================
if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser(
        description='Price Trend Analyzer - Detect valuation anomalies from monthly prices'
    )
    parser.add_argument(
        '--dry-run', action='store_true',
        help='Run analysis but do not write to database'
    )
    parser.add_argument(
        '--symbol', type=str, default=None,
        help='Analyze a single NSE symbol (for debugging)'
    )
    parser.add_argument(
        '--log-level', type=str, default='INFO',
        choices=['DEBUG', 'INFO', 'WARNING', 'ERROR'],
        help='Logging level (default: INFO)'
    )
    args = parser.parse_args()

    # Configure logging
    log_dir = os.getenv('LOG_DIR', os.path.join(os.path.dirname(__file__), '..', '..', 'logs'))
    os.makedirs(log_dir, exist_ok=True)
    log_file = os.path.join(
        log_dir,
        f"price_trend_{datetime.now().strftime('%Y%m%d_%H%M')}.log"
    )

    logging.basicConfig(
        level=getattr(logging, args.log_level),
        format='%(asctime)s [%(name)s] %(levelname)s: %(message)s',
        handlers=[
            logging.StreamHandler(),
            logging.FileHandler(log_file),
        ]
    )

    logger.info(f"Log file: {log_file}")

    from valuation_system.storage.mysql_client import ValuationMySQLClient

    mysql_client = ValuationMySQLClient.get_instance()
    analyzer = PriceTrendAnalyzer()

    if args.symbol:
        # Single-symbol debug mode
        logger.info(f"Single-symbol mode: {args.symbol}")
        company = mysql_client.query_one(
            "SELECT company_id, accord_code, bse_code, nse_symbol, "
            "company_name, valuation_group, valuation_subgroup "
            "FROM vs_active_companies "
            "WHERE nse_symbol = %s",
            (args.symbol,)
        )
        if not company:
            logger.error(f"Symbol {args.symbol} not found in vs_active_companies")
            sys.exit(1)

        lookups = [company]
        self_alerts = analyzer.detect_anomalies(lookups)
        sector_alerts = analyzer.detect_sector_relative_anomalies(lookups)

        print(f"\n--- Self-relative alerts for {args.symbol} ---")
        for a in self_alerts:
            print(
                f"  {a['driver_affected'].upper()}: current={a['current_value']}, "
                f"median={a['baseline_value']}, dev={a['deviation_pct']:+.1f}%, "
                f"severity={a['severity']}, action={a['suggested_action']}"
            )
        print(f"\n--- Sector-relative alerts for {args.symbol} ---")
        for a in sector_alerts:
            print(
                f"  {a['driver_affected'].upper()}: current={a['current_value']}, "
                f"baseline={a['baseline_value']}, dev={a['deviation_pct']:+.1f}%, "
                f"severity={a['severity']}, action={a['suggested_action']}"
            )

        if not args.dry_run and (self_alerts or sector_alerts):
            all_alerts = self_alerts + sector_alerts
            inserted = analyzer.upsert_alerts_to_db(all_alerts, mysql_client)
            print(f"\nInserted {inserted} alerts to vs_materiality_alerts")
        elif args.dry_run:
            print("\n[DRY RUN] No alerts written to database")
    else:
        # Full analysis mode
        if args.dry_run:
            logger.info("[DRY RUN] Will not write to database")
            company_rows = mysql_client.query(
                "SELECT company_id, accord_code, bse_code, nse_symbol, "
                "company_name, valuation_group, valuation_subgroup "
                "FROM vs_active_companies "
                "WHERE is_active = 1 "
                "AND valuation_group IS NOT NULL "
                "AND valuation_group != 'NOT_CLASSIFIED' "
                "AND valuation_group != 'NON_OPERATING'"
            )
            self_alerts = analyzer.detect_anomalies(company_rows)
            sector_alerts = analyzer.detect_sector_relative_anomalies(company_rows)
            print(f"\n[DRY RUN] Would insert {len(self_alerts) + len(sector_alerts)} alerts")
            print(f"  Self-relative: {len(self_alerts)}")
            print(f"  Sector-relative: {len(sector_alerts)}")

            # Log severity breakdown
            severity_counts = {}
            for a in self_alerts + sector_alerts:
                sev = a['severity']
                severity_counts[sev] = severity_counts.get(sev, 0) + 1
            for sev, cnt in sorted(severity_counts.items()):
                print(f"  {sev}: {cnt}")
        else:
            summary = analyzer.run_full_analysis(mysql_client)
            print(f"\nAnalysis complete: {summary}")

    logger.info("Price trend analysis finished")
