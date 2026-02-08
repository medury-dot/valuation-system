"""
Financial Processor
Bridges raw CSV data to valuation model inputs.
Extracts and normalizes financial metrics from CoreDataLoader output
into structured inputs for DCF, Relative, and Monte Carlo models.

Key principle: ACTUAL → DERIVED → DEFAULT (3-tier fallback)
- Use actual yearly/half-yearly data from core CSV when available
- Fall back to derived estimates only when actual data is missing
- Log source tag [ACTUAL], [DERIVED], or [DEFAULT] for every metric
"""

import os
import logging
from typing import Optional

import numpy as np
from dotenv import load_dotenv

logger = logging.getLogger(__name__)

load_dotenv(os.path.join(os.path.dirname(__file__), '..', '..', 'config', '.env'))


class FinancialProcessor:
    """
    Process raw company financials from CoreDataLoader into model-ready inputs.

    Responsibilities:
    - Calculate FCFF from actual cash flow data
    - Estimate working capital changes
    - Calculate reinvestment rates
    - Derive growth rates and margins
    - Build DCFInputs dataclass from raw data
    """

    def __init__(self, core_loader, price_loader, damodaran_loader):
        self.core = core_loader
        self.prices = price_loader
        self.damodaran = damodaran_loader

    def _prefer_ttm(self, financials: dict, annual_key: str,
                     quarterly_key: str) -> tuple:
        """
        Prefer TTM from quarterly data over latest annual.
        Returns (value, source_tag).
        """
        quarterly = financials.get(quarterly_key, {})
        ttm = self.core.get_ttm(quarterly)
        annual_dict = financials.get(annual_key, financials.get(
            annual_key.replace('_annual', ''), {}))
        latest_annual = self.core.get_latest_value(annual_dict)

        if ttm and ttm > 0:
            return ttm, f'TTM quarterly (sum last 4Q = {ttm:,.1f})'
        elif latest_annual and latest_annual > 0:
            return latest_annual, f'FY annual ({latest_annual:,.1f})'
        return 0, 'no data'

    def _collect_ratio_components(self, financials: dict) -> dict:
        """
        Collect numerator/denominator detail for all ratios.
        Used for Excel traceability (e.g., "avg(949/18148=5.2%, 850/16500=5.2%)").
        """
        sales = financials.get('sales_annual', financials.get('sales', {}))
        components = {}

        # Capex/Sales components
        pur_fa = financials.get('pur_of_fixed_assets', {})
        if pur_fa and sales:
            capex_comp = []
            for year in sorted(pur_fa.keys(), reverse=True)[:3]:
                if year in sales and sales[year] and sales[year] > 0 and pur_fa.get(year):
                    capex_comp.append({
                        'year': year, 'numerator': abs(pur_fa[year]),
                        'denominator': sales[year],
                        'ratio': abs(pur_fa[year]) / sales[year]
                    })
            components['capex'] = capex_comp

        # Depreciation/Sales components
        acc_dep = financials.get('acc_dep_yearly', {})
        if acc_dep and len(acc_dep) >= 2 and sales:
            depr_comp = []
            sorted_years = sorted(acc_dep.keys())
            for i in range(max(0, len(sorted_years) - 3), len(sorted_years)):
                if i == 0:
                    continue
                year = sorted_years[i]
                prev = sorted_years[i - 1]
                annual_depr = acc_dep[year] - acc_dep[prev]
                if annual_depr > 0 and year in sales and sales[year] > 0:
                    depr_comp.append({
                        'year': year,
                        'numerator': round(annual_depr, 1),
                        'denominator': sales[year],
                        'ratio': annual_depr / sales[year],
                        'label': f'Δ({acc_dep[year]:.0f}-{acc_dep[prev]:.0f})'
                    })
            components['depreciation'] = depr_comp

        # NWC/Sales components
        inventories = financials.get('inventories', {})
        debtors = financials.get('sundry_debtors', {})
        payables = financials.get('trade_payables', {})
        if inventories and debtors and sales:
            nwc_comp = []
            common_years = set(inventories.keys()) & set(debtors.keys()) & set(sales.keys())
            if payables:
                common_years = common_years & set(payables.keys())
            for year in sorted(common_years, reverse=True)[:3]:
                inv = inventories.get(year, 0)
                dr = debtors.get(year, 0)
                pay = payables.get(year, 0)
                nwc = inv + dr - pay
                if sales[year] and sales[year] > 0:
                    nwc_comp.append({
                        'year': year, 'inv': inv, 'debtors': dr,
                        'payables': pay, 'nwc': nwc,
                        'denominator': sales[year],
                        'ratio': nwc / sales[year]
                    })
            components['nwc'] = nwc_comp

        # Tax Rate components
        pat = financials.get('pat_annual', financials.get('pat', {}))
        pbt_yearly = financials.get('pbt_excp_yearly', {})
        if pat and pbt_yearly:
            tax_comp = []
            for year in sorted(pbt_yearly.keys(), reverse=True)[:3]:
                if year in pat and pbt_yearly[year] and pbt_yearly[year] > 0:
                    tax = 1 - (pat[year] / pbt_yearly[year])
                    if 0 < tax < 0.50:
                        tax_comp.append({
                            'year': year, 'pat': pat[year],
                            'pbt': pbt_yearly[year], 'rate': tax
                        })
            components['tax'] = tax_comp

        # Cash source detail
        cash_hy = financials.get('cash_and_bank_hy', {})
        if cash_hy:
            latest_key = max(cash_hy.keys())
            latest_val = cash_hy[latest_key]
            if latest_val is not None:
                components['cash_source'] = (
                    f'h{latest_key[1]}_{latest_key[0]}_cash_and_bank = {latest_val:,.2f} Cr'
                )

        # Cost of debt components
        interest_yearly = financials.get('interest_yearly', {})
        debt_series = financials.get('debt', {})
        if interest_yearly and debt_series:
            cod_comp = []
            for year in sorted(interest_yearly.keys(), reverse=True)[:3]:
                if year in debt_series and debt_series[year] and debt_series[year] > 0:
                    rate = interest_yearly[year] / debt_series[year]
                    if 0.01 < rate < 0.25:
                        cod_comp.append({
                            'year': year, 'interest': interest_yearly[year],
                            'debt': debt_series[year], 'rate': rate
                        })
            components['cost_of_debt'] = cod_comp

        return components

    def _collect_yoy_growth(self, sales_annual: dict) -> list:
        """Collect individual year-over-year revenue growth rates (last 3 years)."""
        yoy = []
        years = sorted(sales_annual.keys(), reverse=True)
        for i in range(min(3, len(years) - 1)):
            curr = sales_annual.get(years[i])
            prev = sales_annual.get(years[i + 1])
            if curr and prev and prev > 0:
                yoy.append({
                    'from_year': years[i + 1], 'to_year': years[i],
                    'from_value': prev, 'to_value': curr,
                    'growth': (curr / prev) - 1
                })
        return yoy

    def build_dcf_inputs(self, company_name: str, sector: str,
                          sector_config: dict = None,
                          overrides: dict = None) -> dict:
        """
        Build complete DCF inputs from raw data.

        Args:
            company_name: Exact company name in core CSV
            sector: CSV sector name (e.g. 'Chemicals')
            sector_config: Sector config from sectors.yaml
            overrides: Manual overrides for any parameter

        Returns:
            Dict compatible with DCFInputs dataclass.
        """
        financials = self.core.get_company_financials(company_name)
        if not financials:
            raise ValueError(f"No financial data for {company_name}")

        nse_symbol = financials.get('nse_symbol', '')
        price_data = self.prices.get_latest_data(nse_symbol)

        # Get Damodaran parameters
        de_ratio = self._calculate_de_ratio(financials)
        tax_rate = self._estimate_effective_tax_rate(financials)
        damodaran_params = self.damodaran.get_all_params(
            sector, company_de_ratio=de_ratio, company_tax_rate=tax_rate
        )

        # Revenue base — prefer TTM from quarterly, fall back to annual
        latest_sales, revenue_source = self._prefer_ttm(
            financials, 'sales_annual', 'sales_quarterly')

        # Also get TTM PAT and PBIDT for traceability
        latest_pat_ttm, pat_source = self._prefer_ttm(
            financials, 'pat_annual', 'pat_quarterly')
        latest_pbidt_ttm, pbidt_source = self._prefer_ttm(
            financials, 'pbidt_annual', 'pbidt_quarterly')

        # Growth rates from annual data (CAGR always from annual, YoY for context)
        sales_annual = financials.get('sales_annual', financials.get('sales', {}))
        revenue_cagr_5y = self.core.calculate_cagr(sales_annual, years=5)
        revenue_cagr_3y = self.core.calculate_cagr(sales_annual, years=3)
        revenue_yoy = self._collect_yoy_growth(sales_annual)

        # Ratio components for Excel traceability
        ratio_components = self._collect_ratio_components(financials)

        # Growth rate trajectory (decay towards terminal)
        growth_rates = self._build_growth_trajectory(
            revenue_cagr_5y, revenue_cagr_3y, sector_config
        )

        # Margin analysis (uses annual data)
        ebitda_margin = self._calculate_ebitda_margin(financials)
        margin_improvement = self._estimate_margin_trend(financials)

        # Capital requirements (uses annualized quarterly data)
        capex_ratio = self._calculate_capex_to_sales(financials)
        depreciation_ratio = self._calculate_depreciation_to_sales(financials)
        nwc_ratio = self._calculate_nwc_to_sales(financials)

        # Terminal value params from sector config
        terminal_assumptions = {}
        if sector_config:
            terminal_assumptions = sector_config.get('terminal_assumptions', {})

        terminal_roce = self._estimate_terminal_roce(financials, terminal_assumptions)
        terminal_reinvestment = self._estimate_terminal_reinvestment(
            financials, terminal_assumptions
        )

        # Shares outstanding from market cap
        shares = self._estimate_shares_outstanding(financials, price_data)

        # Net debt
        net_debt = self._calculate_net_debt(financials)
        cash = self._get_cash_equivalents(financials)

        # Latest CFO/Capex — prefer yearly actual, fall back to quarterly TTM
        actual_cfo = self.core.get_latest_value(financials.get('cashflow_ops_yearly', {}))
        cfo_source = 'ACTUAL: cashflow_ops_yearly'
        if not actual_cfo:
            actual_cfo = self.core.get_ttm(financials.get('cfo_quarterly', {}))
            cfo_source = 'DERIVED: quarterly TTM' if actual_cfo else None

        actual_capex = self.core.get_latest_value(financials.get('pur_of_fixed_assets', {}))
        capex_source = 'ACTUAL: pur_of_fixed_assets'
        if not actual_capex:
            actual_capex = self.core.get_ttm(financials.get('capex_quarterly', {}))
            capex_source = 'DERIVED: quarterly TTM' if actual_capex else None

        dcf_inputs = {
            'company_name': company_name,
            'nse_symbol': nse_symbol,
            'base_revenue': latest_sales or 0,
            'revenue_growth_rates': growth_rates,
            'ebitda_margin': ebitda_margin or 0.15,
            'margin_improvement': margin_improvement,
            'capex_to_sales': capex_ratio or 0.08,
            'depreciation_to_sales': depreciation_ratio or 0.04,
            'nwc_to_sales': nwc_ratio or 0.15,
            'tax_rate': tax_rate,
            'risk_free_rate': damodaran_params['risk_free_rate'],
            'equity_risk_premium': damodaran_params['equity_risk_premium'],
            'beta': damodaran_params['beta'],
            'cost_of_debt': self._estimate_cost_of_debt(financials),
            'debt_ratio': self._calculate_debt_ratio(financials, price_data),
            'terminal_roce': terminal_roce,
            'terminal_reinvestment': terminal_reinvestment,
            'shares_outstanding': shares,
            'net_debt': net_debt,
            'cash_and_equivalents': cash,
            'actual_cfo': actual_cfo,
            'actual_capex': actual_capex,
            # Traceability fields for Excel report
            'revenue_cagr_3y': revenue_cagr_3y,
            'revenue_cagr_5y': revenue_cagr_5y,
            'revenue_yoy_growth': revenue_yoy,
            'revenue_source': revenue_source,
            'ttm_pat': latest_pat_ttm,
            'pat_source': pat_source,
            'ttm_pbidt': latest_pbidt_ttm,
            'pbidt_source': pbidt_source,
            'capex_components': ratio_components.get('capex', []),
            'depr_components': ratio_components.get('depreciation', []),
            'nwc_components': ratio_components.get('nwc', []),
            'tax_components': ratio_components.get('tax', []),
            'cash_source_detail': ratio_components.get('cash_source', ''),
            'cost_of_debt_components': ratio_components.get('cost_of_debt', []),
        }

        # Apply overrides
        if overrides:
            for key, value in overrides.items():
                if key in dcf_inputs:
                    logger.info(f"Override: {key} = {value} (was {dcf_inputs[key]})")
                    dcf_inputs[key] = value

        # === TRACEABILITY: Log every DCF input with source tags ===
        logger.info(f"Built DCF inputs for {company_name}:")
        logger.info(f"  Revenue base:        Rs {dcf_inputs['base_revenue']:,.1f} Cr  [{revenue_source}]")
        logger.info(f"  TTM PAT:             Rs {latest_pat_ttm:,.1f} Cr  [{pat_source}]")
        logger.info(f"  TTM PBIDT:           Rs {latest_pbidt_ttm:,.1f} Cr  [{pbidt_source}]")
        if revenue_yoy:
            for yoy in revenue_yoy:
                logger.info(f"  YoY Growth FY{yoy['to_year']}: ({yoy['to_value']:,.0f}/{yoy['from_value']:,.0f})-1 = {yoy['growth']:.1%}")
        logger.info(f"  Growth rates:        {[f'{g:.1%}' for g in dcf_inputs['revenue_growth_rates']]}")
        logger.info(f"  EBITDA margin:       {dcf_inputs['ebitda_margin']:.2%}"
                     f"{'  [DEFAULT]' if ebitda_margin is None else ''}")
        logger.info(f"  Margin improvement:  {dcf_inputs['margin_improvement']:.4f}")
        logger.info(f"  Capex/Sales:         {dcf_inputs['capex_to_sales']:.4f}"
                     f"{'  [DEFAULT=0.08]' if capex_ratio is None else ''}")
        logger.info(f"  Depreciation/Sales:  {dcf_inputs['depreciation_to_sales']:.4f}"
                     f"{'  [DEFAULT=0.04]' if depreciation_ratio is None else ''}")
        logger.info(f"  NWC/Sales:           {dcf_inputs['nwc_to_sales']:.4f}"
                     f"{'  [DEFAULT=0.15]' if nwc_ratio is None else ''}")
        logger.info(f"  Tax rate:            {dcf_inputs['tax_rate']:.2%}")
        logger.info(f"  Risk-free rate:      {dcf_inputs['risk_free_rate']:.4f}")
        logger.info(f"  Equity Risk Premium: {dcf_inputs['equity_risk_premium']:.4f}")
        logger.info(f"  Beta (levered):      {dcf_inputs['beta']:.4f}")
        ke = dcf_inputs['risk_free_rate'] + dcf_inputs['beta'] * dcf_inputs['equity_risk_premium']
        logger.info(f"  Cost of Equity (Ke): {ke:.4f}")
        logger.info(f"  Cost of Debt (pre):  {dcf_inputs['cost_of_debt']:.4f}")
        logger.info(f"  Debt ratio (D/V):    {dcf_inputs['debt_ratio']:.4f}")
        logger.info(f"  Terminal ROCE:        {dcf_inputs['terminal_roce']:.4f}")
        logger.info(f"  Terminal reinvest:    {dcf_inputs['terminal_reinvestment']:.4f}")
        logger.info(f"  Shares outstanding:  {dcf_inputs['shares_outstanding']:.2f} Cr")
        logger.info(f"  Net debt:            Rs {dcf_inputs['net_debt']:,.1f} Cr")
        logger.info(f"  Cash & equivalents:  Rs {dcf_inputs['cash_and_equivalents']:,.1f} Cr")
        if actual_cfo:
            logger.info(f"  Actual CFO:          Rs {actual_cfo:,.1f} Cr  [{cfo_source}]")
        if actual_capex:
            logger.info(f"  Actual Capex:        Rs {actual_capex:,.1f} Cr  [{capex_source}]")

        return dcf_inputs

    def build_relative_inputs(self, company_name: str) -> dict:
        """Build inputs needed for relative valuation."""
        financials = self.core.get_company_financials(company_name)

        # Prefer TTM from quarterly, fall back to annual
        latest_sales, _ = self._prefer_ttm(financials, 'sales_annual', 'sales_quarterly')
        latest_pat, _ = self._prefer_ttm(financials, 'pat_annual', 'pat_quarterly')
        latest_pbidt, _ = self._prefer_ttm(financials, 'pbidt_annual', 'pbidt_quarterly')

        networth = financials.get('networth', {})
        debt = financials.get('debt', {})
        latest_networth = self.core.get_latest_value(networth)
        latest_debt = self.core.get_latest_value(debt)

        sales_annual = financials.get('sales_annual', financials.get('sales', {}))

        nse_symbol = financials.get('nse_symbol', '')
        price_data = self.prices.get_latest_data(nse_symbol)
        shares = self._estimate_shares_outstanding(financials, price_data)

        return {
            'company_name': company_name,
            'nse_symbol': nse_symbol,
            'shares_outstanding': shares,
            'latest_sales': latest_sales,
            'latest_pat': latest_pat,
            'latest_ebitda': latest_pbidt,
            'latest_pbidt': latest_pbidt,
            'latest_networth': latest_networth,
            'net_debt': self._calculate_net_debt(financials),
            'latest_roce': self.core.get_latest_value(financials.get('roce', {})),
            'latest_roe': self.core.get_latest_value(financials.get('roe', {})),
            'revenue_cagr_5y': self.core.calculate_cagr(sales_annual, years=5),
            'revenue_cagr_3y': self.core.calculate_cagr(sales_annual, years=3),
            'promoter_pledge_pct': self._get_promoter_pledge(financials),
            'debt_to_equity': (latest_debt / latest_networth)
                if latest_debt and latest_networth and latest_networth > 0 else 0,
        }

    def _build_growth_trajectory(self, cagr_5y: Optional[float],
                                  cagr_3y: Optional[float],
                                  sector_config: dict = None) -> list:
        """
        Build 5-year growth trajectory that decays toward terminal.

        Logic:
        - Year 1: Near-term growth (average of 3Y and 5Y CAGR)
        - Years 2-4: Gradual decay
        - Year 5: Approaches terminal growth zone (5-7%)
        """
        if not cagr_5y and not cagr_3y:
            logger.warning("No CAGR data, using default 10% growth")
            return [0.10, 0.09, 0.08, 0.07, 0.06]

        near_term = cagr_3y or cagr_5y or 0.10
        medium_term = cagr_5y or cagr_3y or 0.08

        # Blend for starting growth
        starting_growth = (near_term * 0.6 + medium_term * 0.4)
        starting_growth = max(0.03, min(starting_growth, 0.30))

        # Terminal zone
        terminal_zone = 0.06

        # Decay linearly from starting to terminal
        rates = []
        for i in range(5):
            rate = starting_growth - (starting_growth - terminal_zone) * (i / 4)
            rates.append(round(max(0.03, rate), 4))

        logger.debug(f"Growth trajectory: {rates} (from CAGR 3Y={cagr_3y}, 5Y={cagr_5y})")
        return rates

    def _calculate_ebitda_margin(self, financials: dict) -> Optional[float]:
        """Calculate average EBITDA margin from recent annual data."""
        pbidt = financials.get('pbidt_annual', financials.get('pbidt', {}))
        sales = financials.get('sales_annual', financials.get('sales', {}))

        if not pbidt or not sales:
            return None

        margins = []
        for year in sorted(pbidt.keys(), reverse=True)[:3]:
            if year in sales and sales[year] and sales[year] > 0:
                margins.append(pbidt[year] / sales[year])

        if margins:
            return round(np.mean(margins), 4)
        return None

    def _estimate_margin_trend(self, financials: dict) -> float:
        """
        Estimate annual margin improvement from historical trend.
        Positive = expanding, negative = compressing.
        """
        ebidtm = financials.get('ebidtm', financials.get('pbidtm', {}))
        if not ebidtm or len(ebidtm) < 3:
            return 0.0

        sorted_years = sorted(ebidtm.keys())
        values = [ebidtm[y] / 100 for y in sorted_years]  # Convert from % to decimal

        if len(values) < 2:
            return 0.0

        # Simple linear regression for trend
        n = len(values)
        x = np.arange(n)
        slope = np.polyfit(x, values, 1)[0]

        # Cap at reasonable annual change
        return round(max(-0.02, min(0.02, slope)), 4)

    def _calculate_capex_to_sales(self, financials: dict) -> Optional[float]:
        """
        Calculate capex as % of sales.
        ACTUAL → DERIVED → DEFAULT fallback chain.

        For high-growth companies (CAGR > 30%), normalize to maintenance capex
        to avoid treating temporary expansion capex as permanent.
        """
        sales = financials.get('sales_annual', financials.get('sales', {}))
        if not sales:
            return None

        # Detect high-growth phase (check both CAGR and recent YoY)
        sales_annual = financials.get('sales_annual', financials.get('sales', {}))
        recent_cagr_3y = self.core.calculate_cagr(sales_annual, years=3)

        # Also check most recent YoY growth
        recent_yoy = 0
        if sales_annual and len(sales_annual) >= 2:
            years = sorted(sales_annual.keys(), reverse=True)
            if len(years) >= 2:
                curr = sales_annual.get(years[0], 0)
                prev = sales_annual.get(years[1], 0)
                if curr and prev and prev > 0:
                    recent_yoy = (curr / prev) - 1

        # High-growth if EITHER condition: CAGR >20% OR recent YoY >50%
        is_high_growth = (recent_cagr_3y and recent_cagr_3y > 0.20) or recent_yoy > 0.50

        # Method 1 [ACTUAL]: Use pur_of_fixed_assets (yearly, direct from cash flow statement)
        pur_fa = financials.get('pur_of_fixed_assets', {})
        if pur_fa:
            ratios = []
            for year in sorted(pur_fa.keys(), reverse=True)[:3]:
                if year in sales and sales[year] and sales[year] > 0 and pur_fa[year]:
                    cap = abs(pur_fa[year])
                    ratio = cap / sales[year]
                    ratios.append(ratio)
                    logger.debug(f"  Capex/Sales {year}: {cap:.1f}/{sales[year]:.1f} "
                                 f"= {ratio:.4f} [ACTUAL: pur_of_fixed_assets]")
            if ratios:
                historical_avg = round(np.mean(ratios), 4)

                # HIGH-GROWTH NORMALIZATION: Use maintenance capex, not expansion capex
                if is_high_growth and historical_avg > 0.15:  # >15% indicates expansion phase
                    # For specialty chemicals, maintenance capex is typically 5-8%
                    normalized_capex = 0.065  # 6.5% for chemicals
                    logger.info(f"  Capex/Sales: {normalized_capex:.4f} [NORMALIZED for high-growth: "
                                f"historical {historical_avg:.1%} includes expansion, using maintenance capex. "
                                f"3Y CAGR: {recent_cagr_3y:.1%}]")
                    return normalized_capex
                else:
                    logger.info(f"  Capex/Sales: {historical_avg:.4f} [ACTUAL: pur_of_fixed_assets avg {len(ratios)}yr]")
                    return historical_avg

        # Method 2 [DERIVED]: Derive from gross block + CWIP changes
        gross_block = financials.get('gross_block', {})
        cwip = financials.get('cwip', {})

        if gross_block and len(gross_block) >= 2:
            sorted_years = sorted(gross_block.keys())
            ratios = []
            for i in range(1, len(sorted_years)):
                year = sorted_years[i]
                prev = sorted_years[i - 1]
                gb_delta = gross_block[year] - gross_block[prev]
                cwip_delta = cwip.get(year, 0) - cwip.get(prev, 0)
                capex_derived = gb_delta + cwip_delta
                if capex_derived > 0 and year in sales and sales[year] > 0:
                    ratios.append(capex_derived / sales[year])
                    logger.debug(f"  Capex/Sales {year}: {capex_derived:.1f}/{sales[year]:.1f} "
                                 f"= {capex_derived/sales[year]:.4f} "
                                 f"(GrsBlk delta={gb_delta:.1f}, CWIP delta={cwip_delta:.1f})")

            if ratios:
                result = round(np.mean(ratios[-3:]), 4)
                logger.info(f"  Capex/Sales: {result:.4f} [DERIVED: Δ(gross_block+CWIP) avg {len(ratios[-3:])}yr]")
                return result

        # Method 3 [DERIVED]: Annualized quarterly cashflow capex
        capex = financials.get('capex', {})
        if capex:
            ratios = []
            for year in sorted(capex.keys(), reverse=True)[:3]:
                if year in sales and sales[year] and sales[year] > 0:
                    cap = abs(capex[year])
                    ratios.append(cap / sales[year])
            if ratios:
                result = round(np.mean(ratios), 4)
                logger.info(f"  Capex/Sales: {result:.4f} [DERIVED: annualized quarterly capex]")
                return result

        logger.warning("  No capex data available — falling back to default [DEFAULT=0.08]")
        return None

    def _calculate_depreciation_to_sales(self, financials: dict) -> Optional[float]:
        """
        Calculate D&A as % of sales.
        ACTUAL → DERIVED → DEFAULT fallback chain.
        """
        sales = financials.get('sales_annual', financials.get('sales', {}))
        if not sales:
            return None

        # Method 1 [ACTUAL]: Use acc_dep_yearly (actual accumulated depreciation, Δ = annual charge)
        acc_dep = financials.get('acc_dep_yearly', {})
        if acc_dep and len(acc_dep) >= 2:
            sorted_years = sorted(acc_dep.keys())
            depr_ratios = []
            for i in range(1, len(sorted_years)):
                year = sorted_years[i]
                prev = sorted_years[i - 1]
                annual_depr = acc_dep[year] - acc_dep[prev]
                if annual_depr > 0 and year in sales and sales[year] > 0:
                    ratio = annual_depr / sales[year]
                    depr_ratios.append(ratio)
                    logger.debug(f"  Depreciation/Sales {year}: {annual_depr:.1f}/{sales[year]:.1f} "
                                 f"= {ratio:.4f} [ACTUAL: Δ(acc_dep)]")
            if depr_ratios:
                result = round(np.mean(depr_ratios[-3:]), 4)
                logger.info(f"  Depreciation/Sales: {result:.4f} [ACTUAL: Δ(acc_dep) avg {len(depr_ratios[-3:])}yr]")
                return result

        # Method 2 [ACTUAL]: accumulated_depreciation from acc_dep column (same data, compat key)
        acc_depr = financials.get('accumulated_depreciation', {})
        if acc_depr and len(acc_depr) >= 2:
            sorted_years = sorted(acc_depr.keys())
            depr_amounts = []
            for i in range(1, len(sorted_years)):
                year = sorted_years[i]
                prev_year = sorted_years[i - 1]
                depr = acc_depr[year] - acc_depr[prev_year]
                if depr > 0 and year in sales and sales[year] > 0:
                    depr_amounts.append(depr / sales[year])
            if depr_amounts:
                result = round(np.mean(depr_amounts[-3:]), 4)
                logger.info(f"  Depreciation/Sales: {result:.4f} [ACTUAL: Δ(accumulated_depreciation)]")
                return result

        # Method 3 [DERIVED]: Derive from gross_block - net_block
        gross_block = financials.get('gross_block', {})
        net_block = financials.get('net_block', {})

        if gross_block and net_block and len(gross_block) >= 2:
            acc_depr_derived = {}
            for year in gross_block:
                if year in net_block:
                    acc_depr_derived[year] = gross_block[year] - net_block[year]

            if len(acc_depr_derived) >= 2:
                sorted_years = sorted(acc_depr_derived.keys())
                depr_ratios = []
                for i in range(1, len(sorted_years)):
                    year = sorted_years[i]
                    prev = sorted_years[i - 1]
                    annual_depr = acc_depr_derived[year] - acc_depr_derived[prev]
                    if annual_depr > 0 and year in sales and sales[year] > 0:
                        ratio = annual_depr / sales[year]
                        depr_ratios.append(ratio)

                if depr_ratios:
                    result = round(np.mean(depr_ratios[-3:]), 4)
                    logger.info(f"  Depreciation/Sales: {result:.4f} [DERIVED: Δ(gross_block - net_block)]")
                    return result

        logger.warning("  No depreciation data available — falling back to default [DEFAULT=0.04]")
        return None

    def _calculate_nwc_to_sales(self, financials: dict) -> Optional[float]:
        """
        Calculate Net Working Capital / Sales.
        ACTUAL → DERIVED → DEFAULT fallback chain.

        NWC = (Inventories + Sundry Debtors) - ALL Current Liabilities

        Where Current Liabilities = Total Liabilities - Long-term Borrowings
        This captures trade payables, customer advances, provisions, other CL.

        For high-growth companies, NWC can be abnormally high due to inventory
        build-up and extended receivables. Normalize to industry standards.
        """
        sales = financials.get('sales_annual', financials.get('sales', {}))

        # Detect high-growth phase (check both CAGR and recent YoY)
        sales_annual = financials.get('sales_annual', financials.get('sales', {}))
        recent_cagr_3y = self.core.calculate_cagr(sales_annual, years=3)

        # Also check most recent YoY growth
        recent_yoy = 0
        if sales_annual and len(sales_annual) >= 2:
            years = sorted(sales_annual.keys(), reverse=True)
            if len(years) >= 2:
                curr = sales_annual.get(years[0], 0)
                prev = sales_annual.get(years[1], 0)
                if curr and prev and prev > 0:
                    recent_yoy = (curr / prev) - 1

        # High-growth if EITHER condition: CAGR >20% OR recent YoY >50%
        is_high_growth = (recent_cagr_3y and recent_cagr_3y > 0.20) or recent_yoy > 0.50

        # Method 1A [ACTUAL-HALFYEARLY]: Try half-yearly first (most recent data)
        inv_hy = financials.get('inventories_hy', {})
        debtors_hy = financials.get('sundry_debtors_hy', {})
        tot_liab_hy = financials.get('tot_liab_hy', {})
        lt_borrow_hy = financials.get('LT_borrow_hy', {})

        # For half-yearly, we need TTM sales to calculate ratio
        sales_qtr = financials.get('sales_quarterly', {})
        if sales_qtr and inv_hy and debtors_hy and tot_liab_hy:
            # Get latest TTM sales
            qtr_indices = sorted(sales_qtr.keys(), reverse=True)
            if len(qtr_indices) >= 4:
                ttm_sales = sum(sales_qtr[qtr_indices[i]] for i in range(4))

                # Get most recent half-year
                half_years = sorted(tot_liab_hy.keys(), reverse=True)
                if half_years and ttm_sales > 0:
                    latest_hy = half_years[0]
                    inv = inv_hy.get(latest_hy, 0)
                    debtors = debtors_hy.get(latest_hy, 0)
                    tot_liab = tot_liab_hy.get(latest_hy, 0)
                    lt_borrow = lt_borrow_hy.get(latest_hy, 0)

                    if inv > 0 and debtors > 0 and tot_liab > 0:
                        current_liab = tot_liab - lt_borrow
                        nwc = inv + debtors - current_liab
                        ratio = nwc / ttm_sales

                        logger.info(f"  NWC/Sales: {ratio:.4f} [ACTUAL: (inv+debtors-ALL_CL)/sales from {latest_hy}]")
                        logger.info(f"    Operating CA: Rs {inv + debtors:,.0f} Cr")
                        logger.info(f"    Current Liab: Rs {current_liab:,.0f} Cr (Total={tot_liab:.0f} - LT={lt_borrow:.0f})")
                        logger.info(f"    Net WC:       Rs {nwc:,.0f} Cr")

                        # HIGH-GROWTH NORMALIZATION (only if NWC is very positive)
                        if is_high_growth and ratio > 0.60:
                            normalized_nwc = 0.30
                            logger.info(f"  ** NORMALIZED to {normalized_nwc:.4f} for high-growth (3Y CAGR: {recent_cagr_3y:.1%})")
                            return normalized_nwc

                        return round(ratio, 4)

        # Method 1B [ACTUAL-ANNUAL]: Use annual data (3-year average)
        inventories = financials.get('inventories', {})
        debtors = financials.get('sundry_debtors', {})
        tot_liab = financials.get('tot_liab', {})
        lt_borrow = financials.get('LT_borrow', {})

        if not sales:
            pass  # Will fall through to default
        elif inventories and debtors and tot_liab:
            # Use intersection of available years
            common_years = set(inventories.keys()) & set(debtors.keys()) & set(tot_liab.keys()) & set(sales.keys())
            if common_years:
                ratios = []
                for year in sorted(common_years, reverse=True)[:3]:
                    # Current Liabilities = Total Liabilities - Long-term Borrowings
                    current_liab = tot_liab.get(year, 0) - lt_borrow.get(year, 0)
                    nwc = (inventories.get(year, 0) + debtors.get(year, 0) - current_liab)

                    if sales[year] and sales[year] > 0:
                        ratio = nwc / sales[year]
                        ratios.append(ratio)
                        logger.debug(f"  NWC/Sales {year}: Inv={inventories.get(year, 0):.1f} + "
                                     f"Debtors={debtors.get(year, 0):.1f} - "
                                     f"CL={current_liab:.1f} = "
                                     f"NWC={nwc:.1f} / Sales={sales[year]:.1f} = {ratio:.4f}")
                if ratios:
                    historical_avg = round(np.mean(ratios), 4)

                    # HIGH-GROWTH NORMALIZATION: Cap NWC at reasonable industry levels
                    if is_high_growth and historical_avg > 0.60:  # >60% indicates growth phase buildup
                        # For specialty chemicals, steady-state NWC is typically 25-35%
                        normalized_nwc = 0.30  # 30% for chemicals
                        logger.info(f"  NWC/Sales: {normalized_nwc:.4f} [NORMALIZED for high-growth: "
                                    f"historical {historical_avg:.1%} includes growth-phase buildup, "
                                    f"using steady-state norm. 3Y CAGR: {recent_cagr_3y:.1%}]")
                        return normalized_nwc
                    else:
                        logger.info(f"  NWC/Sales: {historical_avg:.4f} [ACTUAL: (inv+debtors-ALL_CL)/sales avg {len(ratios)}yr]")
                        return historical_avg

        # Method 1B [FALLBACK]: If tot_liab not available, use trade payables only (old method)
        payables = financials.get('trade_payables', {})
        if inventories and debtors and payables:
            common_years = set(inventories.keys()) & set(debtors.keys()) & set(payables.keys()) & set(sales.keys())
            if common_years:
                ratios = []
                for year in sorted(common_years, reverse=True)[:3]:
                    nwc = (inventories.get(year, 0) + debtors.get(year, 0)
                           - payables.get(year, 0))
                    if sales[year] and sales[year] > 0:
                        ratio = nwc / sales[year]
                        ratios.append(ratio)
                        logger.debug(f"  NWC/Sales {year}: Inv={inventories.get(year, 0):.1f} + "
                                     f"Debtors={debtors.get(year, 0):.1f} - "
                                     f"Payables={payables.get(year, 0):.1f} = "
                                     f"NWC={nwc:.1f} / Sales={sales[year]:.1f} = {ratio:.4f}")
                if ratios:
                    historical_avg = round(np.mean(ratios), 4)

                    # HIGH-GROWTH NORMALIZATION: Cap NWC at reasonable industry levels
                    if is_high_growth and historical_avg > 0.60:  # >60% indicates growth phase buildup
                        # For specialty chemicals, steady-state NWC is typically 25-35%
                        normalized_nwc = 0.30  # 30% for chemicals
                        logger.warning(f"  NWC/Sales: {normalized_nwc:.4f} [NORMALIZED for high-growth: "
                                       f"historical {historical_avg:.1%} using OLD METHOD (payables only), "
                                       f"using steady-state norm. 3Y CAGR: {recent_cagr_3y:.1%}]")
                        return normalized_nwc
                    else:
                        logger.warning(f"  NWC/Sales: {historical_avg:.4f} [ACTUAL: (inv+debtors-payables)/sales avg {len(ratios)}yr - OLD METHOD, may overstate NWC]")
                        return historical_avg

        # Method 2 [DERIVED]: Cash Conversion Cycle / 365
        ccc = financials.get('cash_conversion_cycle', {})
        if ccc:
            recent_ccc = [v for v in sorted(ccc.items(), reverse=True)[:3] if v[1] is not None]
            if recent_ccc:
                avg_ccc = np.mean([v[1] for v in recent_ccc])
                result = round(avg_ccc / 365, 4)
                logger.info(f"  NWC/Sales: {result:.4f} [DERIVED: CCC={avg_ccc:.1f}d / 365]")
                return result

        # Method 3 [DERIVED]: From individual days ratios
        inv_days = financials.get('inventory_days', {})
        recv_days = financials.get('receivable_days', {})
        paybl_days = financials.get('payable_days', {})

        if inv_days or recv_days or paybl_days:
            all_years = set(list(inv_days.keys()) + list(recv_days.keys()) + list(paybl_days.keys()))
            ratios = []
            for year in sorted(all_years, reverse=True)[:3]:
                inv = inv_days.get(year, 0) or 0
                recv = recv_days.get(year, 0) or 0
                paybl = paybl_days.get(year, 0) or 0
                computed_ccc = inv + recv - paybl
                ratios.append(computed_ccc / 365)

            if ratios:
                result = round(np.mean(ratios), 4)
                logger.info(f"  NWC/Sales: {result:.4f} [DERIVED: days ratios]")
                return result

        logger.warning("  No NWC data available — falling back to default [DEFAULT=0.15]")
        return None

    def _estimate_effective_tax_rate(self, financials: dict) -> float:
        """
        Estimate effective tax rate. Tax = 1 - PAT/PBT.
        ACTUAL → DERIVED → DEFAULT fallback chain.
        """
        pat = financials.get('pat_annual', financials.get('pat', {}))
        if not pat:
            logger.info(f"  Tax rate: 0.2500 [DEFAULT: no PAT data]")
            return 0.25

        # Method 1 [ACTUAL]: Use pbt_excp_yearly (actual yearly PBT excl exceptional items)
        pbt_yearly = financials.get('pbt_excp_yearly', {})
        if pbt_yearly:
            tax_rates = []
            for year in sorted(pbt_yearly.keys(), reverse=True)[:3]:
                if year in pat and pbt_yearly[year] and pbt_yearly[year] > 0:
                    tax = 1 - (pat[year] / pbt_yearly[year])
                    if 0 < tax < 0.50:
                        tax_rates.append(tax)
                        logger.debug(f"  Tax rate {year}: 1 - {pat[year]:.1f}/{pbt_yearly[year]:.1f} "
                                     f"= {tax:.4f} [ACTUAL: pbt_excp_yearly]")
            if tax_rates:
                result = round(np.mean(tax_rates), 4)
                logger.info(f"  Tax rate: {result:.4f} [ACTUAL: 1 - PAT/pbt_excp avg {len(tax_rates)}yr]")
                return result

        # Method 2 [DERIVED]: PBT from annualized quarterly pbt_excp
        pbt_qtr = financials.get('pbt_excl_exceptional', {})
        if pbt_qtr:
            tax_rates = []
            for year in sorted(pbt_qtr.keys(), reverse=True)[:3]:
                if year in pat and pbt_qtr[year] and pbt_qtr[year] > 0:
                    tax = 1 - (pat[year] / pbt_qtr[year])
                    if 0 < tax < 0.50:
                        tax_rates.append(tax)
            if tax_rates:
                result = round(np.mean(tax_rates), 4)
                logger.info(f"  Tax rate: {result:.4f} [DERIVED: annualized quarterly PBT]")
                return result

        # Method 3 [DERIVED]: Approximate PBT from PBIDT - Interest - Depreciation
        pbidt = financials.get('pbidt_annual', financials.get('pbidt', {}))
        if pbidt and pat:
            interest = financials.get('interest_yearly', financials.get('interest_expense', {}))
            acc_dep = financials.get('acc_dep_yearly', {})
            gross_block = financials.get('gross_block', {})
            net_block = financials.get('net_block', {})

            tax_rates = []
            for year in sorted(pat.keys(), reverse=True)[:3]:
                if year not in pbidt or not pbidt[year]:
                    continue
                int_exp = interest.get(year, 0) or 0
                # Annual depreciation: prefer actual acc_dep Δ, else block diff
                depr = 0
                if acc_dep and year in acc_dep:
                    prev_year = year - 1
                    if prev_year in acc_dep:
                        depr = max(0, acc_dep[year] - acc_dep[prev_year])
                elif gross_block and net_block and year in gross_block and year in net_block:
                    acc_depr_curr = gross_block[year] - net_block[year]
                    prev_year = year - 1
                    if prev_year in gross_block and prev_year in net_block:
                        acc_depr_prev = gross_block[prev_year] - net_block[prev_year]
                        depr = max(0, acc_depr_curr - acc_depr_prev)

                pbt_est = pbidt[year] - int_exp - depr
                if pbt_est > 0 and pat[year]:
                    tax = 1 - (pat[year] / pbt_est)
                    if 0 < tax < 0.50:
                        tax_rates.append(tax)

            if tax_rates:
                result = round(np.mean(tax_rates), 4)
                logger.info(f"  Tax rate: {result:.4f} [DERIVED: PBIDT - Int - Depr]")
                return result

        logger.info(f"  Tax rate: 0.2500 [DEFAULT]")
        return 0.25

    def _estimate_cost_of_debt(self, financials: dict) -> float:
        """
        Estimate pre-tax cost of debt from interest/debt ratio.
        ACTUAL → DERIVED → DEFAULT fallback chain.
        """
        debt = financials.get('debt', {})
        if not debt:
            logger.info(f"  Cost of debt: 0.0900 [DEFAULT: no debt data]")
            return 0.09

        # Method 1 [ACTUAL]: Use interest_yearly (actual annual interest from P&L)
        interest_yearly = financials.get('interest_yearly', {})
        if interest_yearly:
            rates = []
            for year in sorted(interest_yearly.keys(), reverse=True)[:3]:
                if year in debt and debt[year] and debt[year] > 0:
                    rate = interest_yearly[year] / debt[year]
                    if 0.01 < rate < 0.25:
                        rates.append(rate)
            if rates:
                result = round(np.mean(rates), 4)
                logger.info(f"  Cost of debt: {result:.4f} [ACTUAL: interest_yearly/debt avg {len(rates)}yr]")
                return result

        # Method 2 [DERIVED]: Use annualized quarterly interest
        interest_qtr = financials.get('interest_expense', {})
        if interest_qtr:
            rates = []
            for year in sorted(interest_qtr.keys(), reverse=True)[:3]:
                if year in debt and debt[year] and debt[year] > 0:
                    rate = interest_qtr[year] / debt[year]
                    if 0.01 < rate < 0.25:
                        rates.append(rate)
            if rates:
                result = round(np.mean(rates), 4)
                logger.info(f"  Cost of debt: {result:.4f} [DERIVED: annualized quarterly interest/debt]")
                return result

        logger.info(f"  Cost of debt: 0.0900 [DEFAULT]")
        return 0.09

    def _calculate_de_ratio(self, financials: dict) -> float:
        """Calculate Debt/Equity ratio."""
        debt = financials.get('debt', {})
        networth = financials.get('networth', {})

        latest_debt = self.core.get_latest_value(debt)
        latest_networth = self.core.get_latest_value(networth)

        if latest_debt is not None and latest_networth and latest_networth > 0:
            return round(latest_debt / latest_networth, 4)
        return 0.20

    def _calculate_debt_ratio(self, financials: dict, price_data: dict) -> float:
        """Calculate D/(D+E) for WACC."""
        debt = self.core.get_latest_value(financials.get('debt', {}))
        mcap = price_data.get('mcap_cr')

        if debt is not None and mcap and (debt + mcap) > 0:
            return round(debt / (debt + mcap), 4)

        # Fallback to book values
        networth = self.core.get_latest_value(financials.get('networth', {}))
        if debt is not None and networth and (debt + networth) > 0:
            return round(debt / (debt + networth), 4)

        return 0.10

    def _calculate_net_debt(self, financials: dict) -> float:
        """
        Return GROSS debt (not net of cash).
        Cash is handled separately as cash_and_equivalents in DCF inputs.
        DCF model: Equity = Enterprise Value - Debt + Cash
        """
        debt = self.core.get_latest_value(financials.get('debt', {}))
        logger.debug(f"  Gross debt: Rs {debt or 0:,.1f} Cr")
        return debt or 0

    def _get_cash_equivalents(self, financials: dict) -> float:
        """
        Get cash & equivalents.
        ACTUAL → DERIVED → DEFAULT fallback chain.
        """
        # Method 1 [ACTUAL]: Use cash_and_bank from half-yearly data
        # Prefer h2 (Oct-Mar, March year-end balance sheet date)
        cash_hy = financials.get('cash_and_bank_hy', {})
        if cash_hy:
            latest_cash = self.core.get_latest_halfyearly(cash_hy)
            if latest_cash is not None and latest_cash >= 0:
                latest_key = max(cash_hy.keys())
                logger.info(f"  Cash & equivalents: Rs {latest_cash:,.1f} Cr "
                            f"[ACTUAL: h{latest_key[1]}_{latest_key[0]}_cash_and_bank]")
                return round(latest_cash, 2)

        # Method 2 [DERIVED]: Conservative estimate from balance sheet
        total_assets = self.core.get_latest_value(financials.get('total_assets', {}))
        net_block = self.core.get_latest_value(financials.get('net_block', {}))
        cwip_val = self.core.get_latest_value(financials.get('cwip', {}))
        networth = self.core.get_latest_value(financials.get('networth', {}))

        if total_assets and net_block:
            fixed_assets = (net_block or 0) + (cwip_val or 0)
            non_fixed = total_assets - fixed_assets

            cash_est = non_fixed * 0.35

            if networth and networth > 0:
                cash_cap = networth * 0.50
                cash_est = min(cash_est, cash_cap)

            if cash_est > 0:
                logger.info(f"  Cash & equivalents: Rs {cash_est:,.1f} Cr "
                            f"[DERIVED: 35% of non-fixed assets={non_fixed:,.1f}]")
                return round(cash_est, 2)

        logger.warning("  Cash & equivalents: Rs 0.0 Cr [DEFAULT: no data]")
        return 0.0

    def _estimate_shares_outstanding(self, financials: dict,
                                      price_data: dict) -> float:
        """
        Estimate shares from MCap / CMP.
        MCap (Rs Cr) = N_shares * CMP / 1e7
        N_shares_cr = MCap_Cr / CMP
        """
        mcap = price_data.get('mcap_cr')
        cmp = price_data.get('cmp')

        if mcap and cmp and cmp > 0:
            shares_in_crores = mcap / cmp
            return round(shares_in_crores, 4)

        # Fallback: use latest market cap from quarterly data
        mcap_quarterly = financials.get('market_cap_quarterly',
                                         financials.get('market_cap', {}))
        core_mcap = self.core.get_latest_quarterly(mcap_quarterly) if hasattr(
            self.core, 'get_latest_quarterly') else self.core.get_latest_value(mcap_quarterly)
        if core_mcap and cmp and cmp > 0:
            return round(core_mcap / cmp, 4)

        logger.warning(f"Cannot estimate shares for {financials.get('company_name')}")
        return 1.0

    def _estimate_terminal_roce(self, financials: dict,
                                 terminal_assumptions: dict) -> float:
        """Estimate sustainable ROCE for terminal value."""
        roce_series = financials.get('roce', {})
        if not roce_series:
            return terminal_assumptions.get('roce_convergence', 0.15)

        avg_roce = self.core.calculate_average(roce_series, years=5)
        if avg_roce:
            # Convert from % to decimal if needed
            if avg_roce > 1:
                avg_roce = avg_roce / 100

            convergence = terminal_assumptions.get('roce_convergence', 0.15)
            # Blend historical with sector convergence
            terminal = avg_roce * 0.6 + convergence * 0.4
            return round(max(0.08, min(terminal, 0.30)), 4)

        return terminal_assumptions.get('roce_convergence', 0.15)

    def _estimate_terminal_reinvestment(self, financials: dict,
                                         terminal_assumptions: dict) -> float:
        """
        Estimate terminal reinvestment rate.
        Reinvestment Rate = Capex / NOPAT
        ACTUAL → DERIVED → DEFAULT fallback chain.

        For high-growth companies, recent capex is inflated by expansion.
        Use sector default for terminal (steady-state) reinvestment.
        """
        tax_rate = self._estimate_effective_tax_rate(financials)

        # Detect high-growth phase (check both CAGR and recent YoY)
        sales_annual = financials.get('sales_annual', financials.get('sales', {}))
        recent_cagr_3y = self.core.calculate_cagr(sales_annual, years=3)

        # Also check most recent YoY growth
        recent_yoy = 0
        if sales_annual and len(sales_annual) >= 2:
            years = sorted(sales_annual.keys(), reverse=True)
            if len(years) >= 2:
                curr = sales_annual.get(years[0], 0)
                prev = sales_annual.get(years[1], 0)
                if curr and prev and prev > 0:
                    recent_yoy = (curr / prev) - 1

        # High-growth if EITHER condition: CAGR >20% OR recent YoY >50%
        is_high_growth = (recent_cagr_3y and recent_cagr_3y > 0.20) or recent_yoy > 0.50

        # Method 1 [ACTUAL]: Use op_profit_yearly + pur_of_fixed_assets
        op_profit_yearly = financials.get('op_profit_yearly', {})
        pur_fa = financials.get('pur_of_fixed_assets', {})
        if op_profit_yearly and pur_fa:
            latest_ebit = self.core.get_latest_value(op_profit_yearly)
            latest_capex = self.core.get_latest_value(pur_fa)
            if latest_ebit and latest_ebit > 0 and latest_capex:
                nopat = latest_ebit * (1 - tax_rate)
                if nopat > 0:
                    historical_reinv = abs(latest_capex) / nopat

                    # HIGH-GROWTH ADJUSTMENT: Use sector default for terminal, not current high capex
                    if is_high_growth and historical_reinv > 0.50:  # >50% indicates expansion phase
                        sector_default = terminal_assumptions.get('reinvestment_rate', 0.30)
                        logger.info(f"  Terminal reinvestment: {sector_default:.4f} "
                                    f"[NORMALIZED for high-growth: historical {historical_reinv:.1%} "
                                    f"includes expansion capex, using sector steady-state. "
                                    f"3Y CAGR: {recent_cagr_3y:.1%}]")
                        return sector_default
                    else:
                        result = round(max(0.10, min(historical_reinv, 0.60)), 4)
                        logger.info(f"  Terminal reinvestment: {result:.4f} "
                                    f"[ACTUAL: pur_of_fixed_assets/NOPAT = {abs(latest_capex):.1f}/{nopat:.1f}]")
                        return result

        # Method 2 [DERIVED]: TTM quarterly op_profit + annualized capex
        op_profit_quarterly = financials.get('op_profit_quarterly',
                                              financials.get('op_profit', {}))
        capex = financials.get('capex', {})

        if op_profit_quarterly:
            ttm_op_profit = self.core.get_ttm(op_profit_quarterly)
            if ttm_op_profit and ttm_op_profit > 0:
                nopat = ttm_op_profit * (1 - tax_rate)

                latest_capex = self.core.get_latest_value(capex) if capex else None
                if latest_capex is None:
                    capex_quarterly = financials.get('capex_quarterly', {})
                    latest_capex = self.core.get_ttm(capex_quarterly)

                if latest_capex and nopat > 0:
                    cap = abs(latest_capex)
                    reinv = cap / nopat
                    result = round(max(0.10, min(reinv, 0.60)), 4)
                    logger.info(f"  Terminal reinvestment: {result:.4f} [DERIVED: quarterly TTM]")
                    return result

        default = terminal_assumptions.get('reinvestment_rate', 0.30)
        logger.info(f"  Terminal reinvestment: {default:.4f} [DEFAULT]")
        return default

    def _get_promoter_pledge(self, financials: dict) -> float:
        """Get promoter pledge percentage."""
        pledged = financials.get('promoter_pledged_quarterly',
                                  financials.get('promoter_pledged', {}))
        if pledged:
            latest = self.core.get_latest_value(pledged)
            return latest if latest else 0
        return 0

    def calculate_fcff_from_actuals(self, company_name: str) -> dict:
        """
        Calculate historical FCFF using actual cash flow statement data.

        FCFF = Cash from Operations + Interest*(1-tax) - Capex

        Prefers yearly actuals, falls back to annualized quarterly.
        """
        financials = self.core.get_company_financials(company_name)
        tax_rate = self._estimate_effective_tax_rate(financials)

        # Prefer yearly actuals for all three components
        cfo = financials.get('cashflow_ops_yearly', {})
        capex = financials.get('pur_of_fixed_assets', {})
        interest = financials.get('interest_yearly', {})
        source = 'yearly_actuals'

        # Fall back to annualized quarterly if yearly data is sparse
        if not cfo or len(cfo) < 3:
            cfo = financials.get('cfo', {})
            capex = financials.get('capex', {})
            interest = financials.get('interest_expense', {})
            source = 'annualized_quarterly'

        fcff_history = {}
        for year in sorted(set(cfo.keys()) & set(capex.keys())):
            cf_ops = cfo.get(year, 0)
            cap = abs(capex.get(year, 0))
            int_exp = interest.get(year, 0) or 0

            fcff = cf_ops + int_exp * (1 - tax_rate) - cap
            fcff_history[year] = round(fcff, 2)
            logger.debug(f"  FCFF {year}: CFO={cf_ops:.1f} + Int*(1-t)={int_exp*(1-tax_rate):.1f} "
                         f"- Capex={cap:.1f} = {fcff:.1f}")

        logger.info(f"  Historical FCFF ({source}): {len(fcff_history)} years, "
                     f"latest={fcff_history.get(max(fcff_history.keys()), 'N/A') if fcff_history else 'N/A'}")

        return {
            'company': company_name,
            'fcff_history': fcff_history,
            'tax_rate_used': tax_rate,
            'source': source,
        }
