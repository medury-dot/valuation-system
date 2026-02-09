"""
Company Driver Calculator
Auto-computes 31 universal quantitative drivers from core CSV + prices data.

Original 8 drivers (1-8):
  revenue_cagr_3yr, ebitda_margin_vs_peers, roce_trend, debt_equity_change,
  promoter_holding_trend, fcf_yield, earnings_momentum, relative_valuation_gap

New market share + growth drivers (9-11):
  market_share_by_revenue, market_share_by_profit, growth_vs_gdp

Exposed DCF metrics (12-16):
  capex_to_sales_trend, nwc_to_sales_trend, effective_tax_rate, cost_of_debt,
  promoter_pledge_pct

New ratio drivers Tier 1 (17-21):
  interest_coverage, operating_leverage, fcf_margin, earnings_quality, capex_phase

New ratio drivers Tier 2 (22-26):
  roe_trend_3y, gross_margin_trend, cash_conversion_cycle, earnings_volatility,
  asset_turnover_trend

Composite quality scores (27-30):
  operational_excellence, financial_health, growth_efficiency, earnings_sustainability

Employee productivity (31):
  employee_productivity

Each driver returns: {driver_name, current_value, impact_direction, trend}
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
        # Aggregate sales/PAT by subgroup and group for market share
        self._subgroup_totals = {}  # {subgroup: {sales: X, pat: X}}
        self._group_totals = {}  # {group: {sales: X, pat: X}}
        # Company-level data for market share: {csv_name: {sales, pat, subgroup, group}}
        self._company_aggregates = {}
        # Capex/sales by subgroup: {subgroup: [ratio1, ratio2, ...]}
        self._subgroup_capex_sales = defaultdict(list)
        # GDP growth rate (from vs_drivers MACRO)
        self._gdp_growth = None
        # Stats for composite scores: {subgroup: {metric: [values]}}
        self._subgroup_metric_lists = defaultdict(lambda: defaultdict(list))
        # Prior year sales for market share trend
        self._subgroup_totals_prior = {}
        self._group_totals_prior = {}
        self._company_aggregates_prior = {}

    def precompute_subgroup_medians(self, companies: list):
        """
        Pre-compute subgroup-level medians for peer comparison drivers.
        Also pre-aggregates sales/PAT totals for market share computation.
        Call once before batch valuation loop.

        Args:
            companies: list of dicts with 'csv_name', 'valuation_subgroup', 'valuation_group',
                       'symbol' (NSE accord_code), 'bse_code' (BSE scrip_code)
        """
        logger.info("Pre-computing subgroup medians and aggregates for %d companies...", len(companies))

        # Group companies by subgroup and group
        by_subgroup = defaultdict(list)
        by_group = defaultdict(list)
        for comp in companies:
            sg = comp.get('valuation_subgroup', '')
            grp = comp.get('valuation_group', '')
            if sg and sg not in ('NON_OPERATING', 'NOT_CLASSIFIED'):
                by_subgroup[sg].append(comp)
            if grp and grp not in ('NON_OPERATING', 'NOT_CLASSIFIED'):
                by_group[grp].append(comp)

        # Initialize aggregate accumulators
        subgroup_sales = defaultdict(float)
        subgroup_pat = defaultdict(float)
        group_sales = defaultdict(float)
        group_pat = defaultdict(float)
        subgroup_sales_prior = defaultdict(float)
        subgroup_pat_prior = defaultdict(float)
        group_sales_prior = defaultdict(float)
        group_pat_prior = defaultdict(float)

        for subgroup, sg_companies in by_subgroup.items():
            margins = []
            pe_values = []
            fcf_yields = []
            revenue_cagrs = []
            capex_sales_list = []
            # For composite scores
            roce_vals = []
            opm_vals = []
            de_vals = []
            ic_vals = []

            for comp in sg_companies:
                csv_name = comp.get('csv_name', '')
                symbol = comp.get('symbol', '')
                bse_code = comp.get('bse_code', '')
                group = comp.get('valuation_group', '')
                if not csv_name:
                    continue

                try:
                    financials = self.core.get_company_financials(csv_name)
                    if not financials:
                        continue

                    # TTM Sales for market share
                    sales_q = financials.get('sales_quarterly', {})
                    pbidt_q = financials.get('pbidt_quarterly', {})
                    pat_q = financials.get('pat_quarterly', {})
                    ttm_sales = self.core.get_ttm(sales_q)
                    ttm_pbidt = self.core.get_ttm(pbidt_q)
                    ttm_pat = self.core.get_ttm(pat_q)

                    if ttm_sales and ttm_sales > 0:
                        subgroup_sales[subgroup] += ttm_sales
                        group_sales[group] += ttm_sales
                        self._company_aggregates[csv_name] = {
                            'ttm_sales': ttm_sales,
                            'ttm_pat': ttm_pat if ttm_pat and ttm_pat > 0 else 0,
                            'subgroup': subgroup,
                            'group': group,
                        }

                        # Prior year sales (annual, 2nd most recent year)
                        sales_annual = financials.get('sales_annual', {})
                        pat_annual = financials.get('pat_annual', {})
                        if sales_annual:
                            ys = sorted(sales_annual.keys())
                            if len(ys) >= 2:
                                prior_sales = sales_annual.get(ys[-2], 0) or 0
                                if prior_sales > 0:
                                    subgroup_sales_prior[subgroup] += prior_sales
                                    group_sales_prior[group] += prior_sales
                                    self._company_aggregates_prior[csv_name] = {
                                        'sales': prior_sales,
                                    }
                                    if pat_annual:
                                        prior_pat = pat_annual.get(ys[-2], 0) or 0
                                        if prior_pat > 0:
                                            subgroup_pat_prior[subgroup] += prior_pat
                                            group_pat_prior[group] += prior_pat
                                            self._company_aggregates_prior[csv_name]['pat'] = prior_pat

                    if ttm_pat and ttm_pat > 0:
                        subgroup_pat[subgroup] += ttm_pat
                        group_pat[group] += ttm_pat

                    # EBITDA margin
                    if ttm_sales and ttm_sales > 0 and ttm_pbidt:
                        margin = ttm_pbidt / ttm_sales
                        margins.append(margin)
                        opm_vals.append(margin)

                    # Revenue CAGR 3yr
                    sales_annual = financials.get('sales_annual', {})
                    cagr = self.core.calculate_cagr(sales_annual, years=3)
                    if cagr is not None:
                        revenue_cagrs.append(cagr)

                    # ROCE
                    roce = financials.get('roce', {})
                    if roce:
                        latest_roce = self.core.get_latest_value(roce)
                        if latest_roce is not None:
                            roce_vals.append(latest_roce)

                    # D/E
                    debt_s = financials.get('debt', {})
                    nw_s = financials.get('networth', {})
                    if debt_s and nw_s:
                        lat_d = self.core.get_latest_value(debt_s) or 0
                        lat_nw = self.core.get_latest_value(nw_s) or 1
                        if lat_nw > 0:
                            de_vals.append(lat_d / lat_nw)

                    # Capex/Sales
                    capex_annual = financials.get('pur_of_fixed_assets', {})
                    latest_capex = self.core.get_latest_value(capex_annual)
                    if latest_capex is not None and ttm_sales and ttm_sales > 0:
                        cs_ratio = abs(latest_capex) / ttm_sales
                        capex_sales_list.append(cs_ratio)
                        self._subgroup_capex_sales[subgroup].append(cs_ratio)

                    # Interest coverage for subgroup stats
                    interest_q = financials.get('interest_quarterly', {})
                    ttm_interest = self.core.get_ttm(interest_q)
                    if ttm_pbidt and ttm_interest and ttm_interest > 0:
                        ic_vals.append(ttm_pbidt / ttm_interest)

                    # P/E from prices
                    if symbol or bse_code:
                        price_data = self.prices.get_latest_data(symbol, bse_code=bse_code, company_name=csv_name)
                        pe = price_data.get('pe') if price_data else None
                        if pe and pe > 0:
                            pe_values.append(pe)

                        mcap = price_data.get('mcap_cr') if price_data else None
                        cfo_annual = financials.get('cashflow_ops_yearly', {})
                        latest_cfo = self.core.get_latest_value(cfo_annual)
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
                'capex_sales': _compute_stats(capex_sales_list),
                'interest_coverage': _compute_stats(ic_vals),
            }

            # Store metric lists for composite score ranking
            self._subgroup_metric_lists[subgroup]['roce'] = roce_vals
            self._subgroup_metric_lists[subgroup]['opm'] = opm_vals
            self._subgroup_metric_lists[subgroup]['capex_sales'] = capex_sales_list
            self._subgroup_metric_lists[subgroup]['de'] = de_vals
            self._subgroup_metric_lists[subgroup]['ic'] = ic_vals

        # Store totals
        self._subgroup_totals = {sg: {'sales': s, 'pat': subgroup_pat.get(sg, 0)} for sg, s in subgroup_sales.items()}
        self._group_totals = {g: {'sales': s, 'pat': group_pat.get(g, 0)} for g, s in group_sales.items()}
        self._subgroup_totals_prior = {sg: {'sales': s, 'pat': subgroup_pat_prior.get(sg, 0)} for sg, s in subgroup_sales_prior.items()}
        self._group_totals_prior = {g: {'sales': s, 'pat': group_pat_prior.get(g, 0)} for g, s in group_sales_prior.items()}

        # Load GDP growth from vs_drivers (MACRO level)
        self._load_gdp_growth()

        logger.info(f"Pre-computed medians for {len(self._subgroup_stats)} subgroups, "
                     f"market share data for {len(self._company_aggregates)} companies")

    def _load_gdp_growth(self):
        """Load GDP growth rate from vs_drivers MACRO level."""
        try:
            conn = mysql.connector.connect(
                host=os.getenv('MYSQL_HOST', 'localhost'),
                port=int(os.getenv('MYSQL_PORT', 3306)),
                user=os.getenv('MYSQL_USER', 'root'),
                password=os.getenv('MYSQL_PASSWORD', ''),
                database=os.getenv('MYSQL_DATABASE', 'rag')
            )
            cursor = conn.cursor(dictionary=True)
            cursor.execute("""
                SELECT current_value FROM vs_drivers
                WHERE driver_name = 'gdp_growth' AND driver_level = 'MACRO'
                ORDER BY last_updated DESC LIMIT 1
            """)
            row = cursor.fetchone()
            if row and row.get('current_value'):
                try:
                    val = str(row['current_value']).replace('%', '').strip()
                    self._gdp_growth = float(val)
                    logger.info(f"GDP growth rate loaded: {self._gdp_growth}%")
                except (ValueError, TypeError):
                    self._gdp_growth = 6.5  # Fallback India GDP estimate
                    logger.warning("Could not parse GDP growth, using fallback 6.5%%")
            else:
                self._gdp_growth = 6.5
                logger.warning("No GDP growth driver found, using fallback 6.5%%")
            cursor.close()
            conn.close()
        except Exception as e:
            self._gdp_growth = 6.5
            logger.warning(f"Failed to load GDP growth: {e}, using fallback 6.5%%")

    def compute_all_drivers(self, csv_name: str, valuation_subgroup: str,
                            company_id: int, symbol: str = '',
                            valuation_group: str = '', bse_code: str = '') -> list:
        """
        Compute all universal quantitative drivers for one company.
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
        if symbol or bse_code:
            price_data = self.prices.get_latest_data(symbol, bse_code=bse_code, company_name=csv_name)

        # === ORIGINAL 8 DRIVERS ===

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

        # === MARKET SHARE + GROWTH DRIVERS (9-11) ===

        # 9. market_share_by_revenue
        d = self._compute_market_share_by_revenue(csv_name, financials, valuation_subgroup, valuation_group)
        if d:
            results.append(d)

        # 10. market_share_by_profit
        d = self._compute_market_share_by_profit(csv_name, financials, valuation_subgroup, valuation_group)
        if d:
            results.append(d)

        # 11. growth_vs_gdp
        d = self._compute_growth_vs_gdp(financials)
        if d:
            results.append(d)

        # === EXPOSED DCF METRICS (12-16) ===

        # 12. capex_to_sales_trend
        d = self._compute_capex_to_sales_trend(financials)
        if d:
            results.append(d)

        # 13. nwc_to_sales_trend
        d = self._compute_nwc_to_sales_trend(financials)
        if d:
            results.append(d)

        # 14. effective_tax_rate
        d = self._compute_effective_tax_rate(financials)
        if d:
            results.append(d)

        # 15. cost_of_debt
        d = self._compute_cost_of_debt(financials)
        if d:
            results.append(d)

        # 16. promoter_pledge_pct
        d = self._compute_promoter_pledge_pct(financials)
        if d:
            results.append(d)

        # === RATIO DRIVERS TIER 1 (17-21) ===

        # 17. interest_coverage
        d = self._compute_interest_coverage(financials)
        if d:
            results.append(d)

        # 18. operating_leverage
        d = self._compute_operating_leverage(financials)
        if d:
            results.append(d)

        # 19. fcf_margin
        d = self._compute_fcf_margin(financials)
        if d:
            results.append(d)

        # 20. earnings_quality
        d = self._compute_earnings_quality(financials)
        if d:
            results.append(d)

        # 21. capex_phase
        d = self._compute_capex_phase(financials)
        if d:
            results.append(d)

        # === RATIO DRIVERS TIER 2 (22-26) ===

        # 22. roe_trend_3y
        d = self._compute_roe_trend_3y(financials)
        if d:
            results.append(d)

        # 23. gross_margin_trend
        d = self._compute_gross_margin_trend(financials)
        if d:
            results.append(d)

        # 24. cash_conversion_cycle
        d = self._compute_cash_conversion_cycle(financials)
        if d:
            results.append(d)

        # 25. earnings_volatility
        d = self._compute_earnings_volatility(financials)
        if d:
            results.append(d)

        # 26. asset_turnover_trend
        d = self._compute_asset_turnover_trend(financials)
        if d:
            results.append(d)

        # === COMPOSITE QUALITY SCORES (27-30) ===

        # 27. operational_excellence
        d = self._compute_operational_excellence(financials, valuation_subgroup)
        if d:
            results.append(d)

        # 28. financial_health
        d = self._compute_financial_health(financials)
        if d:
            results.append(d)

        # 29. growth_efficiency
        d = self._compute_growth_efficiency(financials)
        if d:
            results.append(d)

        # 30. earnings_sustainability
        d = self._compute_earnings_sustainability(financials)
        if d:
            results.append(d)

        # === EMPLOYEE PRODUCTIVITY (31) ===

        # 31. employee_productivity
        d = self._compute_employee_productivity(financials, valuation_subgroup)
        if d:
            results.append(d)

        return results

    # =========================================================================
    # ORIGINAL 8 DRIVERS (unchanged)
    # =========================================================================

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

        if latest < first - 0.1:
            direction = 'POSITIVE'
            trend = 'DOWN'
        elif latest > first + 0.1:
            direction = 'NEGATIVE'
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

        recent_4 = indices_sorted[-4:]
        prev_4 = indices_sorted[-8:-4]

        recent_sum = sum(pat_q.get(i, 0) or 0 for i in recent_4)
        prev_sum = sum(pat_q.get(i, 0) or 0 for i in prev_4)

        if prev_sum == 0:
            return None

        yoy_growth = (recent_sum / prev_sum) - 1

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

        if premium > 0.20:
            direction = 'NEGATIVE'
        elif premium < -0.15:
            direction = 'POSITIVE'
        else:
            direction = 'NEUTRAL'

        trend = 'STABLE'

        return {
            'driver_name': 'relative_valuation_gap',
            'current_value': f"P/E {pe:.1f}x vs {median_pe:.1f}x ({premium:+.0%})",
            'impact_direction': direction,
            'trend': trend,
        }

    # =========================================================================
    # MARKET SHARE + GROWTH DRIVERS (9-11)
    # =========================================================================

    def _compute_market_share_by_revenue(self, csv_name: str, financials: dict,
                                          valuation_subgroup: str, valuation_group: str) -> Optional[dict]:
        """Dual-level market share by revenue with capex context."""
        agg = self._company_aggregates.get(csv_name)
        if not agg or agg['ttm_sales'] <= 0:
            return None

        ttm_sales = agg['ttm_sales']

        # Subgroup share
        sg_total = self._subgroup_totals.get(valuation_subgroup, {}).get('sales', 0)
        sg_share = (ttm_sales / sg_total * 100) if sg_total > 0 else 0

        # Group share
        grp_total = self._group_totals.get(valuation_group, {}).get('sales', 0)
        grp_share = (ttm_sales / grp_total * 100) if grp_total > 0 else 0

        # Year-over-year change (subgroup share)
        prior_agg = self._company_aggregates_prior.get(csv_name, {})
        prior_sales = prior_agg.get('sales', 0)
        sg_total_prior = self._subgroup_totals_prior.get(valuation_subgroup, {}).get('sales', 0)
        sg_share_prior = (prior_sales / sg_total_prior * 100) if sg_total_prior > 0 and prior_sales > 0 else 0
        sg_change = sg_share - sg_share_prior if sg_share_prior > 0 else 0

        # Direction based on subgroup share change
        if sg_change > 0.5:
            direction = 'POSITIVE'
            trend = 'UP'
        elif sg_change < -0.5:
            direction = 'NEGATIVE'
            trend = 'DOWN'
        else:
            direction = 'NEUTRAL'
            trend = 'STABLE'

        # Capex context
        capex_annual = financials.get('pur_of_fixed_assets', {})
        latest_capex = self.core.get_latest_value(capex_annual)
        capex_context = ''
        if latest_capex is not None and ttm_sales > 0:
            cs_ratio = abs(latest_capex) / ttm_sales * 100
            sg_capex_stats = self._subgroup_stats.get(valuation_subgroup, {}).get('capex_sales', {})
            sg_median_cs = (sg_capex_stats.get('median', 0) or 0) * 100

            if sg_change < -0.5 and cs_ratio > sg_median_cs:
                capex_context = f" | Investing to gain share (capex {cs_ratio:.1f}% vs peer {sg_median_cs:.1f}%)"
            elif sg_change < -0.5 and cs_ratio <= sg_median_cs:
                capex_context = f" | Losing share without investing"
            elif sg_change > 0.5 and cs_ratio > sg_median_cs:
                capex_context = f" | Capex-driven share gain (capex {cs_ratio:.1f}% vs peer {sg_median_cs:.1f}%)"
            elif sg_change > 0.5 and cs_ratio <= sg_median_cs:
                capex_context = f" | Organic share gain (brand/distribution)"

        value = f"Subgroup: {sg_share:.1f}% ({sg_change:+.1f}pp) | Group: {grp_share:.1f}%{capex_context}"

        return {
            'driver_name': 'market_share_by_revenue',
            'current_value': value,
            'impact_direction': direction,
            'trend': trend,
        }

    def _compute_market_share_by_profit(self, csv_name: str, financials: dict,
                                         valuation_subgroup: str, valuation_group: str) -> Optional[dict]:
        """Dual-level market share by PAT (profitable companies only)."""
        agg = self._company_aggregates.get(csv_name)
        if not agg or agg['ttm_pat'] <= 0:
            return None

        ttm_pat = agg['ttm_pat']

        sg_total = self._subgroup_totals.get(valuation_subgroup, {}).get('pat', 0)
        sg_share = (ttm_pat / sg_total * 100) if sg_total > 0 else 0

        grp_total = self._group_totals.get(valuation_group, {}).get('pat', 0)
        grp_share = (ttm_pat / grp_total * 100) if grp_total > 0 else 0

        # Prior year PAT share
        prior_agg = self._company_aggregates_prior.get(csv_name, {})
        prior_pat = prior_agg.get('pat', 0)
        sg_total_prior = self._subgroup_totals_prior.get(valuation_subgroup, {}).get('pat', 0)
        sg_share_prior = (prior_pat / sg_total_prior * 100) if sg_total_prior > 0 and prior_pat > 0 else 0
        sg_change = sg_share - sg_share_prior if sg_share_prior > 0 else 0

        if sg_change > 0.5:
            direction = 'POSITIVE'
            trend = 'UP'
        elif sg_change < -0.5:
            direction = 'NEGATIVE'
            trend = 'DOWN'
        else:
            direction = 'NEUTRAL'
            trend = 'STABLE'

        value = f"Subgroup: {sg_share:.1f}% ({sg_change:+.1f}pp) | Group: {grp_share:.1f}%"

        return {
            'driver_name': 'market_share_by_profit',
            'current_value': value,
            'impact_direction': direction,
            'trend': trend,
        }

    def _compute_growth_vs_gdp(self, financials: dict) -> Optional[dict]:
        """Company 3Y revenue CAGR vs GDP growth rate."""
        sales_annual = financials.get('sales_annual', {})
        cagr = self.core.calculate_cagr(sales_annual, years=3)
        if cagr is None or self._gdp_growth is None:
            return None

        cagr_pct = cagr * 100  # Convert from decimal to percentage
        gdp = self._gdp_growth
        ratio = cagr_pct / gdp if gdp > 0 else 0

        if ratio > 2.0:
            direction = 'POSITIVE'  # Structural outperformer
        elif ratio < 0.8:
            direction = 'NEGATIVE'  # Underperformer
        else:
            direction = 'NEUTRAL'  # In-line

        # Trend: check if ratio is improving
        cagr_5yr = self.core.calculate_cagr(sales_annual, years=5)
        if cagr_5yr is not None:
            ratio_5yr = (cagr_5yr * 100) / gdp if gdp > 0 else 0
            if ratio > ratio_5yr + 0.3:
                trend = 'UP'
            elif ratio < ratio_5yr - 0.3:
                trend = 'DOWN'
            else:
                trend = 'STABLE'
        else:
            trend = 'STABLE'

        return {
            'driver_name': 'growth_vs_gdp',
            'current_value': f"CAGR {cagr_pct:.1f}% vs GDP {gdp:.1f}% = {ratio:.1f}x",
            'impact_direction': direction,
            'trend': trend,
        }

    # =========================================================================
    # EXPOSED DCF METRICS (12-16)
    # =========================================================================

    def _compute_capex_to_sales_trend(self, financials: dict) -> Optional[dict]:
        """Capex/Sales ratio trend from financial_processor data."""
        capex_annual = financials.get('pur_of_fixed_assets', {})
        sales_annual = financials.get('sales_annual', {})
        if not capex_annual or not sales_annual:
            return None

        common_years = sorted(set(capex_annual.keys()) & set(sales_annual.keys()))
        if len(common_years) < 2:
            return None

        recent = common_years[-3:] if len(common_years) >= 3 else common_years
        ratios = []
        for y in recent:
            s = sales_annual.get(y, 0)
            c = capex_annual.get(y, 0)
            if s and s > 0:
                ratios.append(abs(c or 0) / s)

        if not ratios:
            return None

        latest = ratios[-1]
        first = ratios[0]

        # Declining capex/sales can be POSITIVE (maturing) or context-dependent
        if latest < first - 0.02:
            direction = 'POSITIVE'  # Capex efficiency improving
            trend = 'DOWN'
        elif latest > first + 0.02:
            direction = 'NEUTRAL'  # Could be expansion investment
            trend = 'UP'
        else:
            direction = 'NEUTRAL'
            trend = 'STABLE'

        return {
            'driver_name': 'capex_to_sales_trend',
            'current_value': f"{latest:.1%}",
            'impact_direction': direction,
            'trend': trend,
        }

    def _compute_nwc_to_sales_trend(self, financials: dict) -> Optional[dict]:
        """NWC/Sales trend. Uses (Inv + Debtors) - (TotLiab - LTBorrow)."""
        inv_hy = financials.get('inventories_hy', {})
        dr_hy = financials.get('sundry_debtors_hy', {})
        tl_hy = financials.get('tot_liab_hy', {})
        ltb_hy = financials.get('LT_borrow_hy', {})
        sales_annual = financials.get('sales_annual', {})

        if not sales_annual:
            return None

        latest_sales = self.core.get_latest_value(sales_annual)
        if not latest_sales or latest_sales <= 0:
            return None

        # Try half-yearly first
        inv = self.core.get_latest_value(inv_hy) if inv_hy else None
        dr = self.core.get_latest_value(dr_hy) if dr_hy else None
        tl = self.core.get_latest_value(tl_hy) if tl_hy else None
        ltb = self.core.get_latest_value(ltb_hy) if ltb_hy else None

        # Fallback to annual
        if inv is None:
            inv = self.core.get_latest_value(financials.get('inventories', {}))
        if dr is None:
            dr = self.core.get_latest_value(financials.get('sundry_debtors', {}))
        if tl is None:
            tl = self.core.get_latest_value(financials.get('tot_liab', {}))
        if ltb is None:
            ltb = self.core.get_latest_value(financials.get('LT_borrow', financials.get('lt_borrowings', {})))

        if None in (inv, dr, tl, ltb):
            return None

        nwc = (inv + dr) - (tl - ltb)
        nwc_pct = nwc / latest_sales

        # Declining NWC/Sales = improving efficiency = POSITIVE
        if nwc_pct < -0.05:
            direction = 'POSITIVE'  # Negative NWC = cash release on growth
        elif nwc_pct > 0.20:
            direction = 'NEGATIVE'  # High working capital needs
        else:
            direction = 'NEUTRAL'

        return {
            'driver_name': 'nwc_to_sales_trend',
            'current_value': f"{nwc_pct:.1%}",
            'impact_direction': direction,
            'trend': 'STABLE',
        }

    def _compute_effective_tax_rate(self, financials: dict) -> Optional[dict]:
        """Effective tax rate from PAT/PBT."""
        pat_annual = financials.get('pat_annual', {})
        pbt_annual = financials.get('pbt_excp_yearly', {})
        if not pat_annual or not pbt_annual:
            return None

        common = sorted(set(pat_annual.keys()) & set(pbt_annual.keys()))
        if not common:
            return None

        latest_year = common[-1]
        pat = pat_annual.get(latest_year, 0)
        pbt = pbt_annual.get(latest_year, 0)

        if not pbt or pbt <= 0:
            return None

        tax_rate = 1 - (pat / pbt)
        if tax_rate < 0:
            tax_rate = 0

        if tax_rate < 0.25:
            direction = 'POSITIVE'  # Tax shields active
        elif tax_rate > 0.30:
            direction = 'NEGATIVE'  # High effective rate
        else:
            direction = 'NEUTRAL'

        return {
            'driver_name': 'effective_tax_rate',
            'current_value': f"{tax_rate:.1%}",
            'impact_direction': direction,
            'trend': 'STABLE',
        }

    def _compute_cost_of_debt(self, financials: dict) -> Optional[dict]:
        """Cost of debt = Interest / Total Debt."""
        interest_yearly = financials.get('interest_yearly', {})
        debt = financials.get('debt', {})
        if not interest_yearly or not debt:
            return None

        common = sorted(set(interest_yearly.keys()) & set(debt.keys()))
        if not common:
            return None

        latest_year = common[-1]
        interest = interest_yearly.get(latest_year, 0) or 0
        total_debt = debt.get(latest_year, 0) or 0

        if total_debt <= 0:
            return None

        cod = interest / total_debt

        if cod < 0.08:
            direction = 'POSITIVE'
        elif cod > 0.12:
            direction = 'NEGATIVE'
        else:
            direction = 'NEUTRAL'

        return {
            'driver_name': 'cost_of_debt',
            'current_value': f"{cod:.1%}",
            'impact_direction': direction,
            'trend': 'STABLE',
        }

    def _compute_promoter_pledge_pct(self, financials: dict) -> Optional[dict]:
        """Promoter pledge percentage from quarterly data."""
        pledged = financials.get('promoter_pledged_quarterly', {})
        if not pledged:
            return None

        latest = self.core.get_latest_value(pledged)
        if latest is None:
            return None

        if latest <= 0:
            direction = 'POSITIVE'
        elif latest > 20:
            direction = 'NEGATIVE'  # High pledge risk
        elif latest > 5:
            direction = 'NEUTRAL'
        else:
            direction = 'POSITIVE'

        return {
            'driver_name': 'promoter_pledge_pct',
            'current_value': f"{latest:.1f}%",
            'impact_direction': direction,
            'trend': 'STABLE',
        }

    # =========================================================================
    # RATIO DRIVERS TIER 1 (17-21)
    # =========================================================================

    def _compute_interest_coverage(self, financials: dict) -> Optional[dict]:
        """TTM PBIDT / TTM Interest."""
        pbidt_q = financials.get('pbidt_quarterly', {})
        interest_q = financials.get('interest_quarterly', {})

        ttm_pbidt = self.core.get_ttm(pbidt_q)
        ttm_interest = self.core.get_ttm(interest_q)

        if ttm_pbidt is None or not ttm_interest or ttm_interest <= 0:
            return None

        ic = ttm_pbidt / ttm_interest

        if ic > 5:
            direction = 'POSITIVE'
        elif ic < 2:
            direction = 'NEGATIVE'
        else:
            direction = 'NEUTRAL'

        return {
            'driver_name': 'interest_coverage',
            'current_value': f"{ic:.1f}x",
            'impact_direction': direction,
            'trend': 'STABLE',
        }

    def _compute_operating_leverage(self, financials: dict) -> Optional[dict]:
        """%ΔPBIDT / %ΔSales (YoY)."""
        sales_annual = financials.get('sales_annual', {})
        pbidt_annual = financials.get('pbidt_annual', {})
        if not sales_annual or not pbidt_annual:
            return None

        common = sorted(set(sales_annual.keys()) & set(pbidt_annual.keys()))
        if len(common) < 2:
            return None

        curr_year = common[-1]
        prev_year = common[-2]
        s_curr = sales_annual.get(curr_year, 0) or 0
        s_prev = sales_annual.get(prev_year, 0) or 0
        p_curr = pbidt_annual.get(curr_year, 0) or 0
        p_prev = pbidt_annual.get(prev_year, 0) or 0

        if s_prev == 0 or p_prev == 0:
            return None

        sales_growth = (s_curr / s_prev) - 1
        pbidt_growth = (p_curr / p_prev) - 1

        if abs(sales_growth) < 0.01:
            return None

        op_leverage = pbidt_growth / sales_growth

        if op_leverage > 1.5:
            direction = 'POSITIVE'
        elif op_leverage < 0.5:
            direction = 'NEGATIVE'
        else:
            direction = 'NEUTRAL'

        trend = 'STABLE'
        if op_leverage > 3:
            trend = 'UP'  # Very high operating leverage — flag
        elif op_leverage < 0:
            trend = 'DOWN'  # Negative leverage

        return {
            'driver_name': 'operating_leverage',
            'current_value': f"{op_leverage:.2f}x",
            'impact_direction': direction,
            'trend': trend,
        }

    def _compute_fcf_margin(self, financials: dict) -> Optional[dict]:
        """(CFO - Capex) / Sales."""
        cfo_annual = financials.get('cashflow_ops_yearly', {})
        capex_annual = financials.get('pur_of_fixed_assets', {})
        sales_annual = financials.get('sales_annual', {})
        if not cfo_annual or not capex_annual or not sales_annual:
            return None

        latest_cfo = self.core.get_latest_value(cfo_annual)
        latest_capex = self.core.get_latest_value(capex_annual)
        latest_sales = self.core.get_latest_value(sales_annual)

        if latest_cfo is None or latest_capex is None or not latest_sales or latest_sales <= 0:
            return None

        fcf = latest_cfo - abs(latest_capex)
        fcf_margin = fcf / latest_sales

        if fcf_margin > 0.10:
            direction = 'POSITIVE'
        elif fcf_margin < 0:
            direction = 'NEGATIVE'
        else:
            direction = 'NEUTRAL'

        return {
            'driver_name': 'fcf_margin',
            'current_value': f"{fcf_margin:.1%}",
            'impact_direction': direction,
            'trend': 'STABLE',
        }

    def _compute_earnings_quality(self, financials: dict) -> Optional[dict]:
        """Operating CF / PAT (>1.0 = good cash conversion)."""
        cfo_annual = financials.get('cashflow_ops_yearly', {})
        pat_annual = financials.get('pat_annual', {})
        if not cfo_annual or not pat_annual:
            return None

        latest_cfo = self.core.get_latest_value(cfo_annual)
        latest_pat = self.core.get_latest_value(pat_annual)

        if latest_cfo is None or not latest_pat or latest_pat <= 0:
            return None

        eq = latest_cfo / latest_pat

        if eq > 0.8:
            direction = 'POSITIVE'
        elif eq < 0.5:
            direction = 'NEGATIVE'
        else:
            direction = 'NEUTRAL'

        return {
            'driver_name': 'earnings_quality',
            'current_value': f"{eq:.2f}x",
            'impact_direction': direction,
            'trend': 'STABLE',
        }

    def _compute_capex_phase(self, financials: dict) -> Optional[dict]:
        """Latest 2Y capex avg / 3Y avg. >1.2=expansion, <0.8=harvesting."""
        capex_annual = financials.get('pur_of_fixed_assets', {})
        if not capex_annual or len(capex_annual) < 3:
            return None

        years = sorted(capex_annual.keys())
        if len(years) < 3:
            return None

        recent_3 = years[-3:]
        recent_2 = years[-2:]

        avg_3y = np.mean([abs(capex_annual.get(y, 0) or 0) for y in recent_3])
        avg_2y = np.mean([abs(capex_annual.get(y, 0) or 0) for y in recent_2])

        if avg_3y <= 0:
            return None

        ratio = avg_2y / avg_3y

        if ratio > 1.2:
            direction = 'NEUTRAL'  # Expansion — could be positive or negative
            if ratio > 1.5:
                trend = 'UP'
            else:
                trend = 'STABLE'
        elif ratio < 0.8:
            direction = 'NEUTRAL'  # Harvesting
            if ratio < 0.6:
                trend = 'DOWN'
            else:
                trend = 'STABLE'
        else:
            direction = 'NEUTRAL'  # Maintenance
            trend = 'STABLE'

        label = 'Expansion' if ratio > 1.2 else ('Harvesting' if ratio < 0.8 else 'Maintenance')

        return {
            'driver_name': 'capex_phase',
            'current_value': f"{ratio:.2f}x ({label})",
            'impact_direction': direction,
            'trend': trend,
        }

    # =========================================================================
    # RATIO DRIVERS TIER 2 (22-26) — Pre-computed CSV ratios + derived
    # =========================================================================

    def _compute_roe_trend_3y(self, financials: dict) -> Optional[dict]:
        """Direction of ROE over 3 years from pre-computed CSV ratios."""
        roe_series = financials.get('roe', {})
        if not roe_series or len(roe_series) < 2:
            return None

        years_sorted = sorted(roe_series.keys())
        recent = years_sorted[-3:] if len(years_sorted) >= 3 else years_sorted
        values = [roe_series[y] for y in recent if roe_series.get(y) is not None]
        if len(values) < 2:
            return None

        latest = values[-1]
        first = values[0]
        change = latest - first

        if change > 3:
            direction = 'POSITIVE'
            trend = 'UP'
        elif change < -3:
            direction = 'NEGATIVE'
            trend = 'DOWN'
        else:
            direction = 'NEUTRAL'
            trend = 'STABLE'

        return {
            'driver_name': 'roe_trend_3y',
            'current_value': f"{latest:.1f}% ({change:+.1f}pp over 3Y)",
            'impact_direction': direction,
            'trend': trend,
        }

    def _compute_gross_margin_trend(self, financials: dict) -> Optional[dict]:
        """Direction of GPM over 3 years from CSV gpm column."""
        gpm_series = financials.get('gpm', {})
        if not gpm_series or len(gpm_series) < 2:
            return None

        years_sorted = sorted(gpm_series.keys())
        recent = years_sorted[-3:] if len(years_sorted) >= 3 else years_sorted
        values = [gpm_series[y] for y in recent if gpm_series.get(y) is not None]
        if len(values) < 2:
            return None

        latest = values[-1]
        first = values[0]
        change = latest - first

        if change > 2:
            direction = 'POSITIVE'
            trend = 'UP'
        elif change < -2:
            direction = 'NEGATIVE'
            trend = 'DOWN'
        else:
            direction = 'NEUTRAL'
            trend = 'STABLE'

        return {
            'driver_name': 'gross_margin_trend',
            'current_value': f"{latest:.1f}% ({change:+.1f}pp over 3Y)",
            'impact_direction': direction,
            'trend': trend,
        }

    def _compute_cash_conversion_cycle(self, financials: dict) -> Optional[dict]:
        """CCC in days + trend direction from CSV ccc column."""
        ccc_series = financials.get('cash_conversion_cycle', {})
        if not ccc_series or len(ccc_series) < 2:
            return None

        years_sorted = sorted(ccc_series.keys())
        recent = years_sorted[-3:] if len(years_sorted) >= 3 else years_sorted
        values = [ccc_series[y] for y in recent if ccc_series.get(y) is not None]
        if len(values) < 2:
            return None

        latest = values[-1]
        first = values[0]
        change = latest - first

        # Lower CCC is better
        if change < -10:
            direction = 'POSITIVE'
            trend = 'DOWN'
        elif change > 10:
            direction = 'NEGATIVE'
            trend = 'UP'
        else:
            direction = 'NEUTRAL'
            trend = 'STABLE'

        return {
            'driver_name': 'cash_conversion_cycle',
            'current_value': f"{latest:.0f} days ({change:+.0f}d over 3Y)",
            'impact_direction': direction,
            'trend': trend,
        }

    def _compute_earnings_volatility(self, financials: dict) -> Optional[dict]:
        """Std dev of quarterly PAT YoY growth (last 12 quarters)."""
        pat_q = financials.get('pat_quarterly', {})
        if not pat_q or len(pat_q) < 12:
            return None

        indices = sorted(pat_q.keys())
        if len(indices) < 12:
            return None

        # Compute YoY growth for each of last 8 quarters (need 4 prior quarters for each)
        recent_12 = indices[-12:]
        yoy_growths = []
        for i in range(4, len(recent_12)):
            curr = pat_q.get(recent_12[i], 0) or 0
            prior = pat_q.get(recent_12[i - 4], 0) or 0
            if prior != 0:
                yoy_growths.append((curr / prior) - 1)

        if len(yoy_growths) < 3:
            return None

        vol = float(np.std(yoy_growths)) * 100  # As percentage

        if vol < 15:
            direction = 'POSITIVE'  # Stable earnings
        elif vol > 40:
            direction = 'NEGATIVE'  # Highly volatile
        else:
            direction = 'NEUTRAL'

        return {
            'driver_name': 'earnings_volatility',
            'current_value': f"Std dev {vol:.0f}%",
            'impact_direction': direction,
            'trend': 'STABLE',
        }

    def _compute_asset_turnover_trend(self, financials: dict) -> Optional[dict]:
        """Sales/TotalAssets trend over 3 years."""
        sales_annual = financials.get('sales_annual', {})
        ta = financials.get('total_assets', {})
        if not sales_annual or not ta:
            return None

        common = sorted(set(sales_annual.keys()) & set(ta.keys()))
        if len(common) < 2:
            return None

        recent = common[-3:] if len(common) >= 3 else common
        ratios = []
        for y in recent:
            s = sales_annual.get(y, 0) or 0
            a = ta.get(y, 0) or 0
            if a > 0:
                ratios.append(s / a)

        if len(ratios) < 2:
            return None

        latest = ratios[-1]
        first = ratios[0]
        change = latest - first

        if change > 0.1:
            direction = 'POSITIVE'
            trend = 'UP'
        elif change < -0.1:
            direction = 'NEGATIVE'
            trend = 'DOWN'
        else:
            direction = 'NEUTRAL'
            trend = 'STABLE'

        return {
            'driver_name': 'asset_turnover_trend',
            'current_value': f"{latest:.2f}x ({change:+.2f} over 3Y)",
            'impact_direction': direction,
            'trend': trend,
        }

    # =========================================================================
    # COMPOSITE QUALITY SCORES (27-30)
    # =========================================================================

    def _compute_operational_excellence(self, financials: dict, valuation_subgroup: str) -> Optional[dict]:
        """Weighted avg: 40% ROCE rank + 30% OPM rank + 30% (1-capex/sales) rank within subgroup."""
        roce = financials.get('roce', {})
        latest_roce = self.core.get_latest_value(roce) if roce else None
        if latest_roce is None:
            return None

        # Get TTM margin
        sales_q = financials.get('sales_quarterly', {})
        pbidt_q = financials.get('pbidt_quarterly', {})
        ttm_sales = self.core.get_ttm(sales_q)
        ttm_pbidt = self.core.get_ttm(pbidt_q)
        if not ttm_sales or ttm_sales <= 0 or ttm_pbidt is None:
            return None
        opm = ttm_pbidt / ttm_sales

        # Get capex/sales
        capex_annual = financials.get('pur_of_fixed_assets', {})
        latest_capex = self.core.get_latest_value(capex_annual)
        cs = abs(latest_capex) / ttm_sales if latest_capex is not None and ttm_sales > 0 else None

        # Rank within subgroup
        sg_lists = self._subgroup_metric_lists.get(valuation_subgroup, {})

        roce_rank = _percentile_rank(latest_roce, sg_lists.get('roce', []))
        opm_rank = _percentile_rank(opm, sg_lists.get('opm', []))
        cs_rank = _percentile_rank(1 - cs, sg_lists.get('capex_sales', [])) if cs is not None else 0.5

        score = 0.4 * roce_rank + 0.3 * opm_rank + 0.3 * cs_rank

        if score > 0.70:
            direction = 'POSITIVE'
        elif score < 0.30:
            direction = 'NEGATIVE'
        else:
            direction = 'NEUTRAL'

        return {
            'driver_name': 'operational_excellence',
            'current_value': f"Score {score:.0%} (ROCE:{latest_roce:.0f}%, OPM:{opm:.0%})",
            'impact_direction': direction,
            'trend': 'STABLE',
        }

    def _compute_financial_health(self, financials: dict) -> Optional[dict]:
        """Blend of D/E + Interest Coverage + Cash position."""
        debt_s = financials.get('debt', {})
        nw_s = financials.get('networth', {})
        lat_d = self.core.get_latest_value(debt_s) if debt_s else None
        lat_nw = self.core.get_latest_value(nw_s) if nw_s else None

        if lat_d is None or lat_nw is None or lat_nw <= 0:
            return None

        de = lat_d / lat_nw

        # Interest coverage
        pbidt_q = financials.get('pbidt_quarterly', {})
        interest_q = financials.get('interest_quarterly', {})
        ttm_pbidt = self.core.get_ttm(pbidt_q)
        ttm_interest = self.core.get_ttm(interest_q)
        ic = ttm_pbidt / ttm_interest if ttm_pbidt and ttm_interest and ttm_interest > 0 else 0

        # Cash from half-yearly
        cash_hy = financials.get('cash_and_bank_hy', {})
        cash = self.core.get_latest_value(cash_hy) if cash_hy else 0
        cash = cash or 0
        cash_to_debt = cash / lat_d if lat_d > 0 else 1.0

        # Simple scoring: lower D/E is better, higher IC is better, higher cash/debt is better
        de_score = max(0, min(1, 1 - de / 2))  # D/E=0 → 1.0, D/E=2 → 0
        ic_score = max(0, min(1, ic / 10))  # IC=10+ → 1.0, IC=0 → 0
        cash_score = max(0, min(1, cash_to_debt))  # Cash >= Debt → 1.0

        score = 0.4 * de_score + 0.35 * ic_score + 0.25 * cash_score

        if score > 0.70:
            direction = 'POSITIVE'
        elif score < 0.30:
            direction = 'NEGATIVE'
        else:
            direction = 'NEUTRAL'

        return {
            'driver_name': 'financial_health',
            'current_value': f"Score {score:.0%} (D/E:{de:.1f}x, IC:{ic:.1f}x, Cash/Debt:{cash_to_debt:.0%})",
            'impact_direction': direction,
            'trend': 'STABLE',
        }

    def _compute_growth_efficiency(self, financials: dict) -> Optional[dict]:
        """Revenue CAGR / Capital intensity (capex/sales + nwc/sales)."""
        sales_annual = financials.get('sales_annual', {})
        cagr = self.core.calculate_cagr(sales_annual, years=3)
        if cagr is None:
            return None

        capex_annual = financials.get('pur_of_fixed_assets', {})
        latest_capex = self.core.get_latest_value(capex_annual)
        latest_sales = self.core.get_latest_value(sales_annual)

        if latest_capex is None or not latest_sales or latest_sales <= 0:
            return None

        cs = abs(latest_capex) / latest_sales

        # Simple capital intensity
        capital_intensity = max(cs, 0.01)  # Floor to avoid division by zero
        efficiency = (cagr * 100) / (capital_intensity * 100)  # Growth per unit of capital

        if efficiency > 3:
            direction = 'POSITIVE'
        elif efficiency < 1:
            direction = 'NEGATIVE'
        else:
            direction = 'NEUTRAL'

        return {
            'driver_name': 'growth_efficiency',
            'current_value': f"CAGR/CapIntensity = {efficiency:.1f}x (CAGR:{cagr:.0%}, Capex/Sales:{cs:.0%})",
            'impact_direction': direction,
            'trend': 'STABLE',
        }

    def _compute_earnings_sustainability(self, financials: dict) -> Optional[dict]:
        """OCF/PAT + 1/(1+earnings_volatility). Higher = more sustainable."""
        cfo_annual = financials.get('cashflow_ops_yearly', {})
        pat_annual = financials.get('pat_annual', {})

        latest_cfo = self.core.get_latest_value(cfo_annual) if cfo_annual else None
        latest_pat = self.core.get_latest_value(pat_annual) if pat_annual else None

        if latest_cfo is None or not latest_pat or latest_pat <= 0:
            return None

        ocf_pat = latest_cfo / latest_pat

        # Earnings volatility
        pat_q = financials.get('pat_quarterly', {})
        vol_score = 0.5  # Default if can't compute
        if pat_q and len(pat_q) >= 12:
            indices = sorted(pat_q.keys())
            recent_12 = indices[-12:]
            yoy_growths = []
            for i in range(4, len(recent_12)):
                curr = pat_q.get(recent_12[i], 0) or 0
                prior = pat_q.get(recent_12[i - 4], 0) or 0
                if prior != 0:
                    yoy_growths.append((curr / prior) - 1)
            if len(yoy_growths) >= 3:
                vol = float(np.std(yoy_growths))
                vol_score = 1 / (1 + vol)

        # Blend: cash conversion quality + stability
        score = 0.6 * min(ocf_pat, 1.5) / 1.5 + 0.4 * vol_score

        if score > 0.70:
            direction = 'POSITIVE'
        elif score < 0.30:
            direction = 'NEGATIVE'
        else:
            direction = 'NEUTRAL'

        return {
            'driver_name': 'earnings_sustainability',
            'current_value': f"Score {score:.0%} (OCF/PAT:{ocf_pat:.1f}x, Stability:{vol_score:.0%})",
            'impact_direction': direction,
            'trend': 'STABLE',
        }

    # =========================================================================
    # EMPLOYEE PRODUCTIVITY (31)
    # =========================================================================

    def _compute_employee_productivity(self, financials: dict, valuation_subgroup: str) -> Optional[dict]:
        """TTM sales / TTM empcost — how much revenue per rupee of employee cost."""
        sales_q = financials.get('sales_quarterly', {})
        empcost_q = financials.get('employee_cost_quarterly', {})

        ttm_sales = self.core.get_ttm(sales_q)
        ttm_empcost = self.core.get_ttm(empcost_q)

        if not ttm_sales or ttm_sales <= 0 or not ttm_empcost or ttm_empcost <= 0:
            return None

        productivity = ttm_sales / ttm_empcost

        # Compare to subgroup (would need more data, use simple thresholds)
        if productivity > 8:
            direction = 'POSITIVE'
        elif productivity < 3:
            direction = 'NEGATIVE'
        else:
            direction = 'NEUTRAL'

        return {
            'driver_name': 'employee_productivity',
            'current_value': f"{productivity:.1f}x (sales/empcost)",
            'impact_direction': direction,
            'trend': 'STABLE',
        }

    # =========================================================================
    # DATABASE UPSERT
    # =========================================================================

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


# =============================================================================
# MODULE-LEVEL HELPER FUNCTIONS
# =============================================================================

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
        if value > median * 1.1:
            return 'POSITIVE'
        elif value < median * 0.9:
            return 'NEGATIVE'
        return 'NEUTRAL'

    z_score = (value - median) / std if std > 0 else 0
    if z_score > 1.0:
        return 'POSITIVE'
    elif z_score < -1.0:
        return 'NEGATIVE'
    return 'NEUTRAL'


def _percentile_rank(value: float, values_list: list) -> float:
    """Compute percentile rank of value within a list (0.0 to 1.0)."""
    if not values_list or value is None:
        return 0.5
    clean = [v for v in values_list if v is not None and not math.isnan(v)]
    if not clean:
        return 0.5
    below = sum(1 for v in clean if v < value)
    return below / len(clean)
