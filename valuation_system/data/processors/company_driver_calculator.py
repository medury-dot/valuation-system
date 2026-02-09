"""
Company Driver Calculator
Auto-computes 8 universal quantitative drivers from core CSV + prices data.

Each driver returns: {driver_name, current_value, impact_direction, trend}
- current_value: The actual number (e.g., "18.5%")
- impact_direction: POSITIVE if above subgroup median, NEGATIVE if below, NEUTRAL if within ±1σ
- trend: UP/DOWN/STABLE based on 3-year direction

Respects PM overrides: before writing, check source column. If PM_OVERRIDE, skip.
"""

import os
import logging
import math
import numpy as np
import mysql.connector
from typing import Optional
from collections import defaultdict

from dotenv import load_dotenv

load_dotenv(os.path.join(os.path.dirname(__file__), '..', '..', 'config', '.env'))

logger = logging.getLogger(__name__)


class CompanyDriverCalculator:
    """Computes quantitative company drivers from core CSV + prices data."""

    def __init__(self, core_loader, price_loader):
        self.core = core_loader
        self.prices = price_loader
        # Subgroup medians cache: {subgroup: {metric: {median, std}}}
        self._subgroup_stats = {}

    def precompute_subgroup_medians(self, companies: list):
        """
        Pre-compute subgroup-level medians for peer comparison drivers.
        Call once before batch valuation loop.

        Args:
            companies: list of dicts with 'csv_name', 'valuation_subgroup', 'symbol' (NSE accord_code),
                       'bse_code' (BSE scrip_code) — both used for price lookup fallback
        """
        logger.info("Pre-computing subgroup medians for company drivers...")

        # Group companies by subgroup
        by_subgroup = defaultdict(list)
        for comp in companies:
            sg = comp.get('valuation_subgroup', '')
            if sg and sg not in ('NON_OPERATING', 'NOT_CLASSIFIED'):
                by_subgroup[sg].append(comp)

        for subgroup, sg_companies in by_subgroup.items():
            margins = []
            pe_values = []
            fcf_yields = []
            revenue_cagrs = []

            for comp in sg_companies:
                csv_name = comp.get('csv_name', '')
                symbol = comp.get('symbol', '')  # NSE accord_code
                bse_code = comp.get('bse_code', '')  # BSE scrip_code
                if not csv_name:
                    continue

                try:
                    financials = self.core.get_company_financials(csv_name)
                    if not financials:
                        continue

                    # EBITDA margin (TTM PBIDT / TTM Sales)
                    sales_q = financials.get('sales_quarterly', {})
                    pbidt_q = financials.get('pbidt_quarterly', {})
                    ttm_sales = self.core.get_ttm(sales_q)
                    ttm_pbidt = self.core.get_ttm(pbidt_q)
                    if ttm_sales and ttm_sales > 0 and ttm_pbidt:
                        margins.append(ttm_pbidt / ttm_sales)

                    # Revenue CAGR 3yr
                    sales_annual = financials.get('sales_annual', {})
                    cagr = self.core.calculate_cagr(sales_annual, years=3)
                    if cagr is not None:
                        revenue_cagrs.append(cagr)

                    # P/E from prices (with BSE code fallback)
                    if symbol or bse_code:
                        price_data = self.prices.get_latest_data(symbol, bse_code=bse_code, company_name=csv_name)
                        pe = price_data.get('pe') if price_data else None
                        if pe and pe > 0:
                            pe_values.append(pe)

                        mcap = price_data.get('mcap_cr') if price_data else None
                        # FCF yield: TTM FCF / MCap
                        cfo_annual = financials.get('cashflow_ops_yearly', {})
                        capex_annual = financials.get('pur_of_fixed_assets', {})
                        latest_cfo = self.core.get_latest_value(cfo_annual)
                        latest_capex = self.core.get_latest_value(capex_annual)
                        if latest_cfo is not None and latest_capex is not None and mcap and mcap > 0:
                            fcf = latest_cfo - abs(latest_capex)
                            fcf_yields.append(fcf / mcap)

                except Exception as e:
                    logger.debug(f"Skipping {csv_name} for subgroup stats: {e}")

            # Store stats
            self._subgroup_stats[subgroup] = {
                'ebitda_margin': _compute_stats(margins),
                'pe': _compute_stats(pe_values),
                'fcf_yield': _compute_stats(fcf_yields),
                'revenue_cagr_3yr': _compute_stats(revenue_cagrs),
            }

        logger.info(f"Pre-computed medians for {len(self._subgroup_stats)} subgroups")

    def compute_all_drivers(self, csv_name: str, valuation_subgroup: str,
                            company_id: int, symbol: str = '') -> list:
        """
        Compute 8 universal quantitative drivers for one company.
        Returns list of dicts ready for vs_drivers upsert.
        """
        results = []
        financials = self.core.get_company_financials(csv_name)
        if not financials:
            logger.warning(f"No financials for {csv_name}, skipping driver computation")
            return results

        sg_stats = self._subgroup_stats.get(valuation_subgroup, {})

        # Get price data once
        price_data = None
        if symbol:
            price_data = self.prices.get_latest_data(symbol)

        # 1. revenue_cagr_3yr
        d = self._compute_revenue_cagr_3yr(financials, sg_stats)
        if d:
            results.append(d)

        # 2. ebitda_margin_vs_peers
        d = self._compute_ebitda_margin_vs_peers(financials, sg_stats)
        if d:
            results.append(d)

        # 3. roce_trend
        d = self._compute_roce_trend(financials)
        if d:
            results.append(d)

        # 4. debt_equity_change
        d = self._compute_debt_equity_change(financials)
        if d:
            results.append(d)

        # 5. promoter_holding_trend
        d = self._compute_promoter_holding_trend(financials)
        if d:
            results.append(d)

        # 6. fcf_yield
        d = self._compute_fcf_yield(financials, price_data, sg_stats)
        if d:
            results.append(d)

        # 7. earnings_momentum
        d = self._compute_earnings_momentum(financials)
        if d:
            results.append(d)

        # 8. relative_valuation_gap
        d = self._compute_relative_valuation_gap(price_data, sg_stats)
        if d:
            results.append(d)

        return results

    def _compute_revenue_cagr_3yr(self, financials: dict, sg_stats: dict) -> Optional[dict]:
        """3-year quarterly sales CAGR vs subgroup median."""
        sales_annual = financials.get('sales_annual', {})
        cagr = self.core.calculate_cagr(sales_annual, years=3)
        if cagr is None:
            return None

        stats = sg_stats.get('revenue_cagr_3yr', {})
        direction = _classify_vs_median(cagr, stats)

        # Trend: compare 3yr CAGR vs 5yr CAGR (accelerating or decelerating)
        cagr_5yr = self.core.calculate_cagr(sales_annual, years=5)
        if cagr_5yr is not None:
            if cagr > cagr_5yr + 0.02:
                trend = 'UP'
            elif cagr < cagr_5yr - 0.02:
                trend = 'DOWN'
            else:
                trend = 'STABLE'
        else:
            trend = 'STABLE'

        return {
            'driver_name': 'revenue_cagr_3yr',
            'current_value': f"{cagr:.1%}",
            'impact_direction': direction,
            'trend': trend,
        }

    def _compute_ebitda_margin_vs_peers(self, financials: dict, sg_stats: dict) -> Optional[dict]:
        """TTM EBITDA margin percentile within subgroup."""
        sales_q = financials.get('sales_quarterly', {})
        pbidt_q = financials.get('pbidt_quarterly', {})
        ttm_sales = self.core.get_ttm(sales_q)
        ttm_pbidt = self.core.get_ttm(pbidt_q)

        if not ttm_sales or ttm_sales <= 0 or ttm_pbidt is None:
            return None

        margin = ttm_pbidt / ttm_sales
        stats = sg_stats.get('ebitda_margin', {})
        direction = _classify_vs_median(margin, stats)

        # Trend: compare TTM margin vs prior year margin
        # Approximate via annualized quarterly
        sales_annual = financials.get('sales_annual', {})
        pbidt_annual = financials.get('pbidt_annual', {})
        if sales_annual and pbidt_annual:
            years_sorted = sorted(sales_annual.keys())
            if len(years_sorted) >= 2:
                prev_year = years_sorted[-2]
                prev_sales = sales_annual.get(prev_year, 0)
                prev_pbidt = pbidt_annual.get(prev_year, 0)
                if prev_sales and prev_sales > 0:
                    prev_margin = prev_pbidt / prev_sales
                    if margin > prev_margin + 0.01:
                        trend = 'UP'
                    elif margin < prev_margin - 0.01:
                        trend = 'DOWN'
                    else:
                        trend = 'STABLE'
                else:
                    trend = 'STABLE'
            else:
                trend = 'STABLE'
        else:
            trend = 'STABLE'

        return {
            'driver_name': 'ebitda_margin_vs_peers',
            'current_value': f"{margin:.1%}",
            'impact_direction': direction,
            'trend': trend,
        }

    def _compute_roce_trend(self, financials: dict) -> Optional[dict]:
        """3-year ROCE direction from annual ROCE values."""
        roce_series = financials.get('roce', {})
        if not roce_series or len(roce_series) < 2:
            return None

        years_sorted = sorted(roce_series.keys())
        recent_years = years_sorted[-3:] if len(years_sorted) >= 3 else years_sorted

        values = [roce_series[y] for y in recent_years if roce_series.get(y) is not None]
        if len(values) < 2:
            return None

        latest = values[-1]
        first = values[0]

        if latest > first + 2:
            direction = 'POSITIVE'
            trend = 'UP'
        elif latest < first - 2:
            direction = 'NEGATIVE'
            trend = 'DOWN'
        else:
            direction = 'NEUTRAL'
            trend = 'STABLE'

        return {
            'driver_name': 'roce_trend',
            'current_value': f"{latest:.1f}%",
            'impact_direction': direction,
            'trend': trend,
        }

    def _compute_debt_equity_change(self, financials: dict) -> Optional[dict]:
        """D/E ratio trend from yearly LT_borrow and NW."""
        debt_series = financials.get('debt', {})
        nw_series = financials.get('networth', {})

        if not debt_series or not nw_series:
            return None

        years_sorted = sorted(set(debt_series.keys()) & set(nw_series.keys()))
        if len(years_sorted) < 2:
            return None

        recent_years = years_sorted[-3:] if len(years_sorted) >= 3 else years_sorted
        de_ratios = []
        for y in recent_years:
            d = debt_series.get(y, 0) or 0
            nw = nw_series.get(y, 1) or 1
            de_ratios.append(d / nw if nw > 0 else 0)

        if not de_ratios:
            return None

        latest = de_ratios[-1]
        first = de_ratios[0]

        # Lower D/E is better → direction inverted
        if latest < first - 0.1:
            direction = 'POSITIVE'  # Deleveraging
            trend = 'DOWN'
        elif latest > first + 0.1:
            direction = 'NEGATIVE'  # Leveraging up
            trend = 'UP'
        else:
            direction = 'NEUTRAL'
            trend = 'STABLE'

        return {
            'driver_name': 'debt_equity_change',
            'current_value': f"{latest:.2f}x",
            'impact_direction': direction,
            'trend': trend,
        }

    def _compute_promoter_holding_trend(self, financials: dict) -> Optional[dict]:
        """Promoter stake change from promoter_* quarterly columns."""
        promoter = financials.get('promoter_holding_quarterly', {})
        if not promoter or len(promoter) < 2:
            return None

        indices_sorted = sorted(promoter.keys())
        recent = indices_sorted[-4:] if len(indices_sorted) >= 4 else indices_sorted
        values = [promoter[i] for i in recent if promoter.get(i) is not None]

        if len(values) < 2:
            return None

        latest = values[-1]
        earliest = values[0]
        change = latest - earliest

        if change > 0.5:
            direction = 'POSITIVE'
            trend = 'UP'
        elif change < -1.0:
            direction = 'NEGATIVE'
            trend = 'DOWN'
        else:
            direction = 'NEUTRAL'
            trend = 'STABLE'

        return {
            'driver_name': 'promoter_holding_trend',
            'current_value': f"{latest:.1f}% ({change:+.1f}pp)",
            'impact_direction': direction,
            'trend': trend,
        }

    def _compute_fcf_yield(self, financials: dict, price_data: dict,
                           sg_stats: dict) -> Optional[dict]:
        """TTM FCF / MCap vs subgroup median."""
        cfo_annual = financials.get('cashflow_ops_yearly', {})
        capex_annual = financials.get('pur_of_fixed_assets', {})

        latest_cfo = self.core.get_latest_value(cfo_annual)
        latest_capex = self.core.get_latest_value(capex_annual)

        if latest_cfo is None or latest_capex is None:
            return None

        fcf = latest_cfo - abs(latest_capex)
        mcap = (price_data.get('mcap_cr') or 0) if price_data else 0

        if mcap <= 0:
            return None

        fcf_yield = fcf / mcap
        stats = sg_stats.get('fcf_yield', {})
        direction = _classify_vs_median(fcf_yield, stats)

        # Trend: hard to compute without history; use STABLE as default
        trend = 'STABLE'

        return {
            'driver_name': 'fcf_yield',
            'current_value': f"{fcf_yield:.1%}",
            'impact_direction': direction,
            'trend': trend,
        }

    def _compute_earnings_momentum(self, financials: dict) -> Optional[dict]:
        """PAT growth acceleration from quarterly pat_* columns."""
        pat_q = financials.get('pat_quarterly', {})
        if not pat_q or len(pat_q) < 8:
            return None

        indices_sorted = sorted(pat_q.keys())
        if len(indices_sorted) < 8:
            return None

        # YoY growth for last 2 periods (each = 4 quarters summed)
        recent_4 = indices_sorted[-4:]
        prev_4 = indices_sorted[-8:-4]

        recent_sum = sum(pat_q.get(i, 0) or 0 for i in recent_4)
        prev_sum = sum(pat_q.get(i, 0) or 0 for i in prev_4)

        if prev_sum == 0:
            return None

        yoy_growth = (recent_sum / prev_sum) - 1

        # Check if growth is accelerating (compare with prior period)
        if len(indices_sorted) >= 12:
            older_4 = indices_sorted[-12:-8]
            older_sum = sum(pat_q.get(i, 0) or 0 for i in older_4)
            if older_sum != 0:
                prior_yoy = (prev_sum / older_sum) - 1
                acceleration = yoy_growth - prior_yoy
            else:
                acceleration = 0
        else:
            acceleration = 0

        if yoy_growth > 0.15:
            direction = 'POSITIVE'
        elif yoy_growth < -0.05:
            direction = 'NEGATIVE'
        else:
            direction = 'NEUTRAL'

        if acceleration > 0.05:
            trend = 'UP'
        elif acceleration < -0.05:
            trend = 'DOWN'
        else:
            trend = 'STABLE'

        return {
            'driver_name': 'earnings_momentum',
            'current_value': f"{yoy_growth:.1%} YoY",
            'impact_direction': direction,
            'trend': trend,
        }

    def _compute_relative_valuation_gap(self, price_data: dict,
                                         sg_stats: dict) -> Optional[dict]:
        """P/E vs subgroup median from prices CSV."""
        if not price_data:
            return None

        pe = price_data.get('pe')
        if not pe or pe <= 0:
            return None

        stats = sg_stats.get('pe', {})
        median_pe = stats.get('median')
        if not median_pe or median_pe <= 0:
            return None

        premium = (pe / median_pe) - 1

        # Higher P/E = more expensive = NEGATIVE for valuation driver
        if premium > 0.20:
            direction = 'NEGATIVE'  # Trading at premium
        elif premium < -0.15:
            direction = 'POSITIVE'  # Trading at discount
        else:
            direction = 'NEUTRAL'

        trend = 'STABLE'  # Would need historical P/E series for trend

        return {
            'driver_name': 'relative_valuation_gap',
            'current_value': f"P/E {pe:.1f}x vs {median_pe:.1f}x ({premium:+.0%})",
            'impact_direction': direction,
            'trend': trend,
        }

    def upsert_drivers_to_db(self, company_id: int, valuation_group: str,
                              valuation_subgroup: str, drivers: list):
        """
        Upsert auto-computed drivers to vs_drivers.
        Skips rows where source='PM_OVERRIDE' (PM has manually set them).
        """
        conn = mysql.connector.connect(
            host=os.getenv('MYSQL_HOST', 'localhost'),
            port=int(os.getenv('MYSQL_PORT', 3306)),
            user=os.getenv('MYSQL_USER', 'root'),
            password=os.getenv('MYSQL_PASSWORD', ''),
            database=os.getenv('MYSQL_DATABASE', 'rag')
        )
        cursor = conn.cursor(dictionary=True)

        updated = 0
        skipped_pm = 0

        for d in drivers:
            driver_name = d['driver_name']

            # Check if PM has overridden this driver
            cursor.execute("""
                SELECT source FROM vs_drivers
                WHERE driver_level = 'COMPANY' AND driver_name = %s AND company_id = %s
            """, (driver_name, company_id))
            existing = cursor.fetchone()

            if existing and existing.get('source') == 'PM_OVERRIDE':
                skipped_pm += 1
                logger.debug(f"Skipping PM-overridden driver {driver_name} for company_id={company_id}")
                continue

            # Upsert
            cursor.execute("""
                UPDATE vs_drivers
                SET current_value = %s, impact_direction = %s, trend = %s,
                    source = 'AUTO', last_updated = NOW()
                WHERE driver_level = 'COMPANY' AND driver_name = %s AND company_id = %s
            """, (d['current_value'], d['impact_direction'], d['trend'],
                  driver_name, company_id))

            if cursor.rowcount > 0:
                updated += 1

        conn.commit()
        cursor.close()
        conn.close()

        if updated > 0 or skipped_pm > 0:
            logger.debug(f"Company {company_id}: updated {updated} drivers, skipped {skipped_pm} PM overrides")

        return updated, skipped_pm


def _compute_stats(values: list) -> dict:
    """Compute median, mean, std for a list of values."""
    if not values:
        return {}
    arr = np.array([v for v in values if v is not None and not math.isnan(v)])
    if len(arr) == 0:
        return {}
    return {
        'median': float(np.median(arr)),
        'mean': float(np.mean(arr)),
        'std': float(np.std(arr)) if len(arr) > 1 else 0.0,
        'count': len(arr),
    }


def _classify_vs_median(value: float, stats: dict) -> str:
    """Classify value vs subgroup median: POSITIVE/NEGATIVE/NEUTRAL."""
    if not stats or 'median' not in stats:
        return 'NEUTRAL'

    median = stats['median']
    std = stats.get('std', 0)

    if std == 0:
        # No dispersion data — use simple above/below
        if value > median * 1.1:
            return 'POSITIVE'
        elif value < median * 0.9:
            return 'NEGATIVE'
        return 'NEUTRAL'

    # Within ±1σ of median = NEUTRAL
    z_score = (value - median) / std if std > 0 else 0
    if z_score > 1.0:
        return 'POSITIVE'
    elif z_score < -1.0:
        return 'NEGATIVE'
    return 'NEUTRAL'
