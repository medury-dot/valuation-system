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

    def __init__(self, core_loader, price_loader, damodaran_loader, nse_loader=None):
        self.core = core_loader
        self.prices = price_loader
        self.damodaran = damodaran_loader
        self.nse = nse_loader
        # Track data source for each key metric (ACTUAL_CF, DERIVED_BS, DEFAULT, etc.)
        # Reset per company in build_dcf_inputs()
        self._data_sources = {}

        # Lazy-load NSE data if not provided
        if self.nse is None:
            try:
                from valuation_system.data.loaders.nse_data_loader import NSEDataLoader
                self.nse = NSEDataLoader()
                if self.nse.is_available():
                    logger.info("NSE data loader initialized and available")
            except Exception as e:
                logger.debug(f"NSE data unavailable: {e}")
                self.nse = None

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

    def _compute_bank_metrics_ttm(self, financials: dict, valuation_group: str) -> dict:
        """
        Compute 13 banking-specific metrics from Core CSV bank_* columns.
        Uses TTM for flow items (P&L), latest/avg for stock items (balance sheet).

        Args:
            financials: Company financials dict from core_loader
            valuation_group: Valuation group (e.g., 'FINANCIALS')

        Returns:
            dict: 13 banking metrics or None for each if data unavailable
        """
        import pandas as pd

        # Only compute for FINANCIALS companies
        if valuation_group != 'FINANCIALS':
            return {}

        metrics = {}

        # Determine latest quarter index dynamically from available bank_* columns
        # Look for bank_advances_XXX columns
        advances_cols = [k for k in financials.keys() if k.startswith('bank_advances_')]
        if not advances_cols:
            logger.debug("No bank_advances columns found - not a banking company")
            return {}

        # Extract indices and find latest
        indices = [int(col.split('_')[-1]) for col in advances_cols if col.split('_')[-1].isdigit()]
        if not indices:
            logger.debug("No valid bank_advances indices found")
            return {}

        latest_idx = max(indices)
        logger.debug(f"[ACTUAL] Latest bank data quarter index: {latest_idx}")

        # Quarter indices for TTM (last 4 quarters)
        ttm_indices = [latest_idx - 3, latest_idx - 2, latest_idx - 1, latest_idx]
        yoy_idx = latest_idx - 4  # Same quarter last year

        # Helper: Get TTM sum for flow items
        def get_ttm(prefix):
            total = 0
            for i in ttm_indices:
                val = financials.get(f'{prefix}_{i}')
                if val and pd.notna(val):
                    total += val
            return total if total > 0 else None

        # Helper: Get average of last 4 quarters for stock items
        def get_avg4q(prefix):
            vals = []
            for i in ttm_indices:
                val = financials.get(f'{prefix}_{i}')
                if val and pd.notna(val):
                    vals.append(val)
            return sum(vals) / len(vals) if vals else None

        # 1-2: GNPA/NNPA % (already percentages in yearly columns)
        gnpa_pct = financials.get('2024_gnpa') or financials.get('2025_gnpa')
        nnpa_pct = financials.get('2024_nnpa') or financials.get('2025_nnpa')

        if gnpa_pct and pd.notna(gnpa_pct):
            metrics['gross_npa_pct'] = gnpa_pct
            logger.debug(f"[ACTUAL] GNPA %: {gnpa_pct:.2f}%")
        else:
            metrics['gross_npa_pct'] = None
            logger.debug("[MISSING] GNPA % not available")

        if nnpa_pct and pd.notna(nnpa_pct):
            metrics['net_npa_pct'] = nnpa_pct
            logger.debug(f"[ACTUAL] NNPA %: {nnpa_pct:.2f}%")
        else:
            metrics['net_npa_pct'] = None
            logger.debug("[MISSING] NNPA % not available")

        # 3: PCR (Provision Coverage Ratio)
        provisions_ttm = get_ttm('bank_provisions')
        advances_latest = financials.get(f'bank_advances_{latest_idx}')

        if provisions_ttm and gnpa_pct and advances_latest:
            abs_gnpa = (gnpa_pct / 100) * advances_latest
            if abs_gnpa > 0:
                pcr = (provisions_ttm / abs_gnpa) * 100
                metrics['provision_coverage'] = pcr
                logger.debug(f"[DERIVED] PCR: {pcr:.2f}% (prov_ttm={provisions_ttm:.1f}, abs_gnpa={abs_gnpa:.1f})")
            else:
                metrics['provision_coverage'] = None
        else:
            metrics['provision_coverage'] = None
            logger.debug("[MISSING] PCR - insufficient data")

        # 4: Credit Cost Trend
        advances_avg = get_avg4q('bank_advances')
        if provisions_ttm and advances_avg and advances_avg > 0:
            credit_cost = (provisions_ttm / advances_avg) * 100
            metrics['credit_cost_trend'] = credit_cost
            logger.debug(f"[DERIVED] Credit Cost: {credit_cost:.2f}% (prov_ttm={provisions_ttm:.1f}, adv_avg={advances_avg:.1f})")
        else:
            metrics['credit_cost_trend'] = None
            logger.debug("[MISSING] Credit Cost - insufficient data")

        # 5: Deposit Growth YoY
        deposits_current = financials.get(f'bank_deposits_{latest_idx}')
        deposits_yoy = financials.get(f'bank_deposits_{yoy_idx}')
        if deposits_current and deposits_yoy and deposits_yoy > 0:
            dep_growth = ((deposits_current - deposits_yoy) / deposits_yoy) * 100
            metrics['deposit_growth_yoy'] = dep_growth
            logger.debug(f"[DERIVED] Deposit Growth: {dep_growth:.2f}% ({deposits_current:.1f} vs {deposits_yoy:.1f})")
        else:
            metrics['deposit_growth_yoy'] = None
            logger.debug("[MISSING] Deposit Growth - insufficient data")

        # 6: Loan Growth YoY
        advances_current = financials.get(f'bank_advances_{latest_idx}')
        advances_yoy = financials.get(f'bank_advances_{yoy_idx}')
        if advances_current and advances_yoy and advances_yoy > 0:
            loan_growth = ((advances_current - advances_yoy) / advances_yoy) * 100
            metrics['loan_growth_yoy'] = loan_growth
            logger.debug(f"[DERIVED] Loan Growth: {loan_growth:.2f}% ({advances_current:.1f} vs {advances_yoy:.1f})")
        else:
            metrics['loan_growth_yoy'] = None
            logger.debug("[MISSING] Loan Growth - insufficient data")

        # 7: Loan-to-Deposit Ratio
        if advances_current and deposits_current and deposits_current > 0:
            ld_ratio = (advances_current / deposits_current) * 100
            metrics['loan_to_deposit_ratio'] = ld_ratio
            logger.debug(f"[DERIVED] L/D Ratio: {ld_ratio:.2f}% ({advances_current:.1f} / {deposits_current:.1f})")
        else:
            metrics['loan_to_deposit_ratio'] = None
            logger.debug("[MISSING] L/D Ratio - insufficient data")

        # 8: ROA (already computed in yearly columns)
        roa = financials.get('2025_roa') or financials.get('2024_roa')
        if roa and pd.notna(roa):
            metrics['return_on_assets'] = roa
            logger.debug(f"[ACTUAL] ROA: {roa:.2f}%")
        else:
            metrics['return_on_assets'] = None
            logger.debug("[MISSING] ROA not available")

        # 9: NIM (Net Interest Margin)
        int_earned_ttm = get_ttm('bank_interest_earned')
        int_expend_ttm = get_ttm('bank_interest_expended')
        tot_assets_avg = get_avg4q('tot_assets')

        if int_earned_ttm and int_expend_ttm and tot_assets_avg and tot_assets_avg > 0:
            nii_ttm = int_earned_ttm - int_expend_ttm
            nim = (nii_ttm / tot_assets_avg) * 100
            metrics['net_interest_margin'] = nim
            logger.debug(f"[DERIVED] NIM: {nim:.2f}% (NII_ttm={nii_ttm:.1f}, assets_avg={tot_assets_avg:.1f})")
        else:
            metrics['net_interest_margin'] = None
            logger.debug("[MISSING] NIM - insufficient interest/asset data")

        # 10: Non-Interest Income %
        other_inc_ttm = get_ttm('bank_other_income')
        total_inc_ttm = get_ttm('bank_total_income')

        if other_inc_ttm and total_inc_ttm and total_inc_ttm > 0:
            non_int_pct = (other_inc_ttm / total_inc_ttm) * 100
            metrics['non_interest_income_pct'] = non_int_pct
            logger.debug(f"[DERIVED] Non-Int Income %: {non_int_pct:.2f}% ({other_inc_ttm:.1f} / {total_inc_ttm:.1f})")
        else:
            metrics['non_interest_income_pct'] = None
            logger.debug("[MISSING] Non-Int Income % - insufficient data")

        # 11: Cost to Income Ratio
        opex_ttm = get_ttm('bank_operating_expenses')
        if opex_ttm and total_inc_ttm and total_inc_ttm > 0:
            cost_income = (opex_ttm / total_inc_ttm) * 100
            metrics['cost_to_income_ratio_co'] = cost_income
            logger.debug(f"[DERIVED] Cost/Income: {cost_income:.2f}% ({opex_ttm:.1f} / {total_inc_ttm:.1f})")
        else:
            metrics['cost_to_income_ratio_co'] = None
            logger.debug("[MISSING] Cost/Income - insufficient data")

        # 12: PPOPM (Pre-Provision Operating Profit Margin)
        if int_earned_ttm and int_expend_ttm and other_inc_ttm and opex_ttm and total_inc_ttm and total_inc_ttm > 0:
            ppop = int_earned_ttm - int_expend_ttm + other_inc_ttm - opex_ttm
            ppopm = (ppop / total_inc_ttm) * 100
            metrics['ppop_margin'] = ppopm
            logger.debug(f"[DERIVED] PPOPM: {ppopm:.2f}% (PPOP={ppop:.1f} / total_inc={total_inc_ttm:.1f})")
        else:
            metrics['ppop_margin'] = None
            logger.debug("[MISSING] PPOPM - insufficient data")

        # 13: CASA Ratio (not available yet - user will provide later)
        metrics['casa_ratio_co'] = None
        logger.debug("[MISSING] CASA Ratio - requires deposit breakdown (future XBRL)")

        return metrics

    def build_dcf_inputs(self, company_name: str, sector: str,
                          sector_config: dict = None,
                          overrides: dict = None,
                          valuation_subgroup: str = None) -> dict:
        """
        Build complete DCF inputs from raw data.

        Args:
            company_name: Exact company name in core CSV
            sector: CSV sector name (e.g. 'Chemicals')
            sector_config: Sector config from sectors.yaml
            overrides: Manual overrides for any parameter
            valuation_subgroup: Subgroup for Indian beta lookup (e.g. 'INDUSTRIALS_DEFENSE')

        Returns:
            Dict compatible with DCFInputs dataclass.
        """
        # Reset data source tracking for this company
        self._data_sources = {}

        financials = self.core.get_company_financials(company_name)
        if not financials:
            raise ValueError(f"No financial data for {company_name}")

        nse_symbol = financials.get('nse_symbol', '')
        bse_code = financials.get('bse_code')

        # Merge NSE data if available and has newer quarters
        if self.nse and self.nse.is_available() and nse_symbol:
            from valuation_system.data.loaders.nse_data_loader import merge_nse_into_financials
            financials = merge_nse_into_financials(financials, nse_symbol, self.nse)

        # Compute banking-specific metrics for FINANCIALS companies
        valuation_group = financials.get('valuation_group', sector)
        bank_metrics = self._compute_bank_metrics_ttm(financials, valuation_group)
        if bank_metrics:
            # Merge banking metrics into financials dict for downstream use
            financials.update(bank_metrics)
            logger.debug(f"Added {len(bank_metrics)} banking metrics to financials")

        # Compute banking drivers from fullstats columns (CASA, NIM, C/I, credit growth)
        banking_drivers_fullstats = self._compute_banking_drivers_from_fullstats(financials)
        if banking_drivers_fullstats:
            logger.info(f"Computed {len(banking_drivers_fullstats)} banking drivers from fullstats columns")

        price_data = self.prices.get_latest_data(nse_symbol, bse_code=bse_code, company_name=company_name)

        # Get WACC parameters — uses Indian subgroup beta when available
        de_ratio = self._calculate_de_ratio(financials)
        tax_rate = self._estimate_effective_tax_rate(financials)
        damodaran_params = self.damodaran.get_all_params(
            sector, company_de_ratio=de_ratio, company_tax_rate=tax_rate,
            valuation_subgroup=valuation_subgroup
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
            revenue_cagr_5y, revenue_cagr_3y, sector_config,
            sales_annual=sales_annual
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

        # WEEK 2 FIX: Pass valuation_subgroup for dynamic ROCE blending
        terminal_roce = self._estimate_terminal_roce(financials, terminal_assumptions, valuation_subgroup)
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
            # Banking-specific metrics (for FINANCIALS companies)
            'bank_metrics': bank_metrics,
            # Banking drivers from fullstats (CASA, NIM, C/I ratio, credit growth)
            'banking_drivers_fullstats': banking_drivers_fullstats,
            # R&D data for applicable subgroups
            'rd_pct_of_sales': self._get_rd_to_sales(financials, valuation_subgroup),
            # Data source tracking for Excel audit trail
            'data_sources': dict(self._data_sources),
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
        logger.info(f"  Beta (levered):      {dcf_inputs['beta']:.4f}  [{damodaran_params.get('beta_source', 'unknown')}]")
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

    def build_roe_model_inputs(self, company_name: str, sector_config: dict = None,
                                valuation_subgroup: str = None, overrides: dict = None) -> dict:
        """
        WEEK 3 FIX: Build inputs for ROE-based residual income valuation (for banks/financials).

        Formula: Equity Value = Book Value × (ROE - g) / (Ke - g)

        This is more appropriate for banks than FCFF DCF because:
        - Banks don't have traditional capex/depreciation
        - Deposits are business model, not debt
        - ROE directly measures return on equity capital
        - Book value is the capital base generating returns
        """
        financials = self.core.get_company_financials(company_name)
        if not financials:
            raise ValueError(f"No financial data for {company_name}")

        nse_symbol = financials.get('nse_symbol', '')
        price_data = self.prices.get_latest_data(nse_symbol)

        # Get banking-specific metrics
        bank_metrics = financials.get('banking_metrics_ttm', {})
        if not bank_metrics:
            # Compute if not already in financials
            valuation_group = financials.get('valuation_group', '')
            bank_metrics = self._compute_bank_metrics_ttm(financials, valuation_group)

        # Extract banking quality metrics early (needed for terminal ROE logic)
        npa_pct = bank_metrics.get('npa_pct', 0)
        pcr = bank_metrics.get('provision_coverage_ratio', 0)

        # Book value (networth/shareholders equity)
        networth_series = financials.get('networth', {})
        latest_networth = self.core.get_latest_value(networth_series)

        # ROE (5-year average for terminal)
        avg_roe_5yr = self._get_avg_roe_5yr(financials)
        if not avg_roe_5yr or avg_roe_5yr <= 0:
            # Fall back to simple ROE calculation
            roe_series = financials.get('roe', {})
            avg_roe_5yr = self.core.calculate_average(roe_series, years=5)
            if avg_roe_5yr and avg_roe_5yr > 1:
                avg_roe_5yr = avg_roe_5yr / 100

        # Terminal ROE - use actual 5Y average WITHOUT blending for quality banks
        # Quality banks sustain high ROE, no need for mean reversion
        terminal_assumptions = sector_config.get('terminal_assumptions', {}) if sector_config else {}

        # WEEK 3 FIX: For quality banks (ROE >15%, NPA <2%), use actual ROE
        # For average/weak banks, blend with sector target
        sector_roe_target = terminal_assumptions.get('roe_target', 0.15)  # 15% default

        if avg_roe_5yr and avg_roe_5yr > 0.15 and npa_pct < 0.02:
            # Quality bank - use 90% actual ROE, 10% sector (minimal reversion)
            hist_weight, conv_weight = 0.9, 0.1
            terminal_roe = avg_roe_5yr * hist_weight + sector_roe_target * conv_weight
            logger.info(f"  Quality bank: Using 90/10 blend (ROE={avg_roe_5yr:.2%}, NPA={npa_pct:.2%})")
        else:
            # Average bank - use dynamic blend
            hist_weight, conv_weight = self._get_dynamic_blend_ratio(financials, valuation_subgroup or '')
            terminal_roe = avg_roe_5yr * hist_weight + sector_roe_target * conv_weight if avg_roe_5yr else sector_roe_target

        # Adjust for asset quality (NPA, PCR) - already extracted above
        roe_adjustment = 0.0
        if npa_pct > 0.03:  # NPA > 3%
            roe_adjustment = -0.02  # Reduce ROE by 2pp
            logger.info(f"  ROE adjustment: -2pp (NPA={npa_pct:.2%} > 3%)")
        elif npa_pct < 0.02 and pcr > 0.70:  # Low NPA + strong PCR
            roe_adjustment = +0.01  # Increase ROE by 1pp
            logger.info(f"  ROE adjustment: +1pp (NPA={npa_pct:.2%} < 2%, PCR={pcr:.2%} > 70%)")

        terminal_roe = max(0.08, min(terminal_roe + roe_adjustment, 0.25))

        # Terminal growth - higher for quality banks with strong metrics
        # Quality banks can sustain higher growth (loan book expansion)
        base_growth = terminal_assumptions.get('growth_rate', 0.04)  # 4% default

        if avg_roe_5yr and avg_roe_5yr > 0.18 and npa_pct < 0.015:
            # High-quality bank (ROE >18%, NPA <1.5%): 6% terminal growth
            terminal_growth = 0.06
            logger.info(f"  Quality bank growth: 6% (ROE={avg_roe_5yr:.2%}, NPA={npa_pct:.2%})")
        elif avg_roe_5yr and avg_roe_5yr > 0.15 and npa_pct < 0.02:
            # Good bank (ROE >15%, NPA <2%): 5% terminal growth
            terminal_growth = 0.05
        else:
            # Average bank: 4% terminal growth
            terminal_growth = base_growth

        terminal_growth = min(terminal_growth, 0.07)  # Cap at 7%

        # Cost of equity for banks - use ROE-based approach
        # For banks, Ke should be close to ROE (investors expect returns close to what bank earns)
        # CAPM often gives wrong Ke for banks due to beta issues
        de_ratio = self._calculate_de_ratio(financials)
        tax_rate = self._estimate_effective_tax_rate(financials)
        damodaran_params = self.damodaran.get_all_params(
            sector='FINANCIALS',
            company_de_ratio=de_ratio,
            company_tax_rate=tax_rate,
            valuation_subgroup=valuation_subgroup
        )
        cost_of_equity_capm = damodaran_params['cost_of_equity']

        # WEEK 3 FIX: For banks, use CAPM Ke from beta scenarios
        # For value creation: ROE > Ke → P/B > 1x
        # Use the best beta scenario to get reasonable Ke
        beta_scenarios = self.damodaran.get_all_beta_scenarios(
            valuation_group='FINANCIALS',
            valuation_subgroup=valuation_subgroup or '',
            company_symbol=financials.get('nse_symbol', ''),
            de_ratio=de_ratio,
            tax_rate=tax_rate
        )

        # Prefer individual beta scenario for Ke (most accurate)
        if 'individual_weekly' in beta_scenarios:
            cost_of_equity = beta_scenarios['individual_weekly'].get('cost_of_equity', cost_of_equity_capm)
            logger.info(f"  Banking Ke: Using individual beta Ke={cost_of_equity:.2%}")
        else:
            cost_of_equity = cost_of_equity_capm
            logger.info(f"  Banking Ke: Using CAPM Ke={cost_of_equity:.2%}")

        # Shares outstanding
        shares = self._estimate_shares_outstanding(financials, price_data)

        logger.info(f"ROE Model Inputs for {company_name}:")
        logger.info(f"  Book Value:          Rs {latest_networth:,.1f} Cr")
        logger.info(f"  5Y Avg ROE:          {avg_roe_5yr:.2%}")
        logger.info(f"  Terminal ROE:        {terminal_roe:.2%} (blended {hist_weight:.0%}/{conv_weight:.0%})")
        logger.info(f"  Terminal Growth:     {terminal_growth:.2%}")
        logger.info(f"  Cost of Equity:      {cost_of_equity:.2%}")
        logger.info(f"  Shares Outstanding:  {shares:.2f} Cr")
        logger.info(f"  Banking NIM:         {bank_metrics.get('net_interest_margin', 0):.2%}")
        logger.info(f"  Banking NPA:         {npa_pct:.2%}")

        return {
            'method': 'ROE_RESIDUAL_INCOME',
            'company_name': company_name,
            'nse_symbol': nse_symbol,
            'book_value': latest_networth,
            'avg_roe_5yr': avg_roe_5yr,
            'terminal_roe': terminal_roe,
            'terminal_growth': terminal_growth,
            'cost_of_equity': cost_of_equity,
            'shares_outstanding': shares,
            'banking_metrics': bank_metrics,
            'damodaran_params': damodaran_params,
            'blend_ratio': (hist_weight, conv_weight),
        }

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
        bse_code = financials.get('bse_code')
        price_data = self.prices.get_latest_data(nse_symbol, bse_code=bse_code, company_name=company_name)
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
                                  sector_config: dict = None,
                                  sales_annual: dict = None) -> list:
        """
        Build 5-year growth trajectory that decays toward terminal.

        Logic:
        - Year 1: Near-term growth (average of 3Y and 5Y CAGR)
        - Years 2-4: Gradual decay
        - Year 5: Approaches terminal growth zone (5-7%)
        - Fallback: If no CAGR available, use YoY from most recent 2 years
        """
        if not cagr_5y and not cagr_3y:
            # Try YoY from most recent 2 years of actual data
            if sales_annual and len(sales_annual) >= 2:
                years = sorted(sales_annual.keys())
                prev_val = sales_annual[years[-2]]
                curr_val = sales_annual[years[-1]]
                if prev_val and prev_val > 0 and curr_val and curr_val > 0:
                    yoy = (curr_val / prev_val) - 1
                    # Dampen YoY by 20% (single year can be noisy) and cap
                    starting_growth = max(0.05, min(yoy * 0.80, 0.30))
                    terminal_zone = 0.06
                    rates = []
                    for i in range(5):
                        rate = starting_growth - (starting_growth - terminal_zone) * (i / 4)
                        rates.append(round(max(0.03, rate), 4))
                    logger.info(f"  Growth trajectory from YoY: {[f'{r:.1%}' for r in rates]} "
                                f"(raw YoY={yoy:.1%}, dampened start={starting_growth:.1%})")
                    return rates

            logger.warning("No CAGR or YoY data, using default 10% growth")
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
                if is_high_growth and historical_avg > 0.12:  # >12% indicates expansion phase
                    normalized_capex = 0.065  # 6.5% maintenance capex
                    cagr_str = f"{recent_cagr_3y:.1%}" if recent_cagr_3y is not None else "N/A"
                    logger.info(f"  Capex/Sales: {normalized_capex:.4f} [NORMALIZED for high-growth: "
                                f"historical {historical_avg:.1%} includes expansion, using maintenance capex. "
                                f"3Y CAGR: {cagr_str}]")
                    return normalized_capex

                # TREND-AWARE WEIGHTING: If capex/sales is declining (company exiting
                # expansion phase), weight recent year heavily instead of simple average.
                # ratios[0] = most recent year (sorted reverse above)
                if len(ratios) >= 2 and ratios[0] < ratios[-1] * 0.75:
                    # Latest is 25%+ lower than oldest → declining capex cycle
                    if len(ratios) == 3:
                        weighted = ratios[0] * 0.60 + ratios[1] * 0.25 + ratios[2] * 0.15
                    else:
                        weighted = ratios[0] * 0.70 + ratios[1] * 0.30
                    weighted = round(weighted, 4)
                    logger.info(f"  Capex/Sales: {weighted:.4f} [ACTUAL: declining trend detected, "
                                f"latest={ratios[0]:.1%} vs oldest={ratios[-1]:.1%}, "
                                f"weighted avg. Simple avg was {historical_avg:.1%}]")
                    return weighted
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
                recent = depr_ratios[-3:]  # last 3 years
                historical_avg = round(np.mean(recent), 4)

                # SINGLE-YEAR CROSS-CHECK: When only 1yr of acc_dep data, cross-check
                # with Method 3 (gross_block - net_block) which may have more years.
                # If Method 3 has more data AND gives a lower ratio, blend 50/50.
                if len(recent) == 1:
                    method3_avg = self._dep_sales_from_block_diff(financials, sales)
                    if method3_avg is not None and method3_avg < historical_avg * 0.85:
                        blended = round((historical_avg + method3_avg) / 2, 4)
                        logger.info(f"  Depreciation/Sales: {blended:.4f} [BLENDED: Δ(acc_dep) 1yr={historical_avg:.4f} "
                                   f"+ block_diff={method3_avg:.4f}, avg of both for stability]")
                        return blended

                # TREND-AWARE WEIGHTING: If dep/sales is declining (post-expansion,
                # assets depreciating at lower rate relative to growing sales),
                # weight recent year heavily.
                # recent[-1] = most recent year, recent[0] = oldest of the 3
                if len(recent) >= 2 and recent[-1] < recent[0] * 0.75:
                    # Latest is 25%+ lower than oldest → declining trend
                    if len(recent) == 3:
                        weighted = recent[-1] * 0.60 + recent[-2] * 0.25 + recent[-3] * 0.15
                    else:
                        weighted = recent[-1] * 0.70 + recent[-2] * 0.30
                    weighted = round(weighted, 4)
                    logger.info(f"  Depreciation/Sales: {weighted:.4f} [ACTUAL: declining trend, "
                                f"latest={recent[-1]:.1%} vs oldest={recent[0]:.1%}, "
                                f"weighted avg. Simple avg was {historical_avg:.1%}]")
                    return weighted
                else:
                    logger.info(f"  Depreciation/Sales: {historical_avg:.4f} [ACTUAL: Δ(acc_dep) avg {len(recent)}yr]")
                    return historical_avg

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

    def _dep_sales_from_block_diff(self, financials: dict, sales: dict) -> Optional[float]:
        """
        Helper: Calculate dep/sales from gross_block - net_block difference.
        Returns average ratio or None if insufficient data.
        """
        gross_block = financials.get('gross_block', {})
        net_block = financials.get('net_block', {})
        if not gross_block or not net_block or len(gross_block) < 2:
            return None

        acc_depr_derived = {}
        for year in gross_block:
            if year in net_block:
                acc_depr_derived[year] = gross_block[year] - net_block[year]

        if len(acc_depr_derived) < 2:
            return None

        sorted_years = sorted(acc_depr_derived.keys())
        depr_ratios = []
        for i in range(1, len(sorted_years)):
            year = sorted_years[i]
            prev = sorted_years[i - 1]
            annual_depr = acc_depr_derived[year] - acc_depr_derived[prev]
            if annual_depr > 0 and year in sales and sales[year] > 0:
                depr_ratios.append(annual_depr / sales[year])

        if depr_ratios:
            return round(np.mean(depr_ratios[-3:]), 4)
        return None

    def _calculate_nwc_to_sales(self, financials: dict) -> Optional[float]:
        """
        Calculate Net Working Capital / Sales.
        ACTUAL_CF → ACTUAL_BS → DERIVED → DEFAULT fallback chain.

        Priority 1: Actual CF WC change from cash flow statement (cf_wc_change)
        Priority 2: BS estimation: NWC = (Inv + Dr) - (TotLiab - LT_Borrow)

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

        # Method 0 [ACTUAL_CF]: Use actual CF WC change from cash flow statement
        # This is the gold standard — actual cash impact of working capital changes
        cf_wc_change = financials.get('cf_wc_change_yearly', {})
        if cf_wc_change and sales:
            # Average last 3 years of cf_wc_change / sales
            common_years = sorted(set(cf_wc_change.keys()) & set(sales.keys()), reverse=True)[:3]
            if common_years:
                ratios = []
                for year in common_years:
                    if sales[year] and sales[year] > 0:
                        # CF WC change is typically negative when WC increases (cash outflow)
                        # NWC/Sales ratio should be positive when WC grows with revenue
                        ratio = abs(cf_wc_change[year]) / sales[year]
                        # Preserve sign: positive cf_wc_change = cash inflow (WC decreased)
                        if cf_wc_change[year] > 0:
                            ratio = -ratio  # WC decreased → negative NWC/Sales
                        ratios.append(ratio)
                        logger.debug(f"  CF WC/Sales {year}: cf_wc={cf_wc_change[year]:,.1f} / "
                                     f"sales={sales[year]:,.1f} = {ratio:.4f}")
                if ratios:
                    avg_ratio = round(np.mean(ratios), 4)
                    self._data_sources['nwc'] = 'ACTUAL_CF'
                    logger.info(f"  NWC/Sales: {avg_ratio:.4f} [ACTUAL_CF: cash flow WC change / sales avg {len(ratios)}yr]")
                    if is_high_growth and avg_ratio > 0.60:
                        logger.info(f"  ** NORMALIZED to 0.30 for high-growth (actual CF WC)")
                        return 0.30
                    return avg_ratio

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

                        logger.info(f"  NWC/Sales: {ratio:.4f} [DERIVED_BS: (inv+debtors-ALL_CL)/sales from {latest_hy}]")
                        logger.info(f"    Operating CA: Rs {inv + debtors:,.0f} Cr")
                        logger.info(f"    Current Liab: Rs {current_liab:,.0f} Cr (Total={tot_liab:.0f} - LT={lt_borrow:.0f})")
                        logger.info(f"    Net WC:       Rs {nwc:,.0f} Cr")
                        self._data_sources.setdefault('nwc', 'DERIVED_BS')

                        # HIGH-GROWTH NORMALIZATION (only if NWC is very positive)
                        if is_high_growth and ratio > 0.60:
                            normalized_nwc = 0.30
                            cagr_str = f"{recent_cagr_3y:.1%}" if recent_cagr_3y is not None else "N/A"
                            logger.info(f"  ** NORMALIZED to {normalized_nwc:.4f} for high-growth (3Y CAGR: {cagr_str})")
                            return normalized_nwc

                        return round(ratio, 4)

        # Method 1B [ACTUAL-ANNUAL]: Use annual data (3-year average)
        inventories = financials.get('inventories', {})
        debtors = financials.get('sundry_debtors', {})
        tot_liab = financials.get('tot_liab', {})
        lt_borrow = financials.get('LT_borrow', {})

        # If tot_liab not available, derive from balance sheet identity:
        # Total Liabilities = Total Assets - Networth
        if not tot_liab:
            total_assets = financials.get('total_assets', financials.get('totalassets', {}))
            networth = financials.get('networth', {})
            if total_assets and networth:
                derived_tot_liab = {}
                for year in total_assets:
                    if year in networth and total_assets[year] and networth[year]:
                        derived_val = total_assets[year] - networth[year]
                        # Validate: tot_liab must be positive and totalassets > networth
                        if derived_val > 0 and total_assets[year] > networth[year]:
                            derived_tot_liab[year] = derived_val
                        else:
                            logger.debug(f"    Skipping year {year}: totalassets={total_assets[year]:.1f} "
                                        f"< networth={networth[year]:.1f} (data error)")
                if derived_tot_liab:
                    tot_liab = derived_tot_liab
                    logger.info(f"    Derived tot_liab from totalassets - networth ({len(derived_tot_liab)} years)")

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
                        normalized_nwc = 0.30
                        cagr_str = f"{recent_cagr_3y:.1%}" if recent_cagr_3y is not None else "N/A"
                        logger.info(f"  NWC/Sales: {normalized_nwc:.4f} [NORMALIZED for high-growth: "
                                    f"historical {historical_avg:.1%} includes growth-phase buildup, "
                                    f"using steady-state norm. 3Y CAGR: {cagr_str}]")
                        return normalized_nwc
                    else:
                        logger.info(f"  NWC/Sales: {historical_avg:.4f} [ACTUAL: (inv+debtors-ALL_CL)/sales avg {len(ratios)}yr]")
                        return historical_avg

            # Balance sheet years don't overlap with sales_annual — use latest BS data with TTM sales
            bs_years = set(inventories.keys()) & set(debtors.keys()) & set(tot_liab.keys())
            sales_qtr = financials.get('sales_quarterly', {})
            if bs_years and sales_qtr:
                qtr_keys = sorted(sales_qtr.keys(), reverse=True)
                if len(qtr_keys) >= 4:
                    ttm_sales = sum(sales_qtr[qtr_keys[i]] for i in range(4))
                    latest_bs_year = max(bs_years)
                    current_liab = tot_liab.get(latest_bs_year, 0) - lt_borrow.get(latest_bs_year, 0)
                    nwc = (inventories.get(latest_bs_year, 0) + debtors.get(latest_bs_year, 0) - current_liab)
                    if ttm_sales > 0:
                        ratio = round(nwc / ttm_sales, 4)
                        logger.info(f"  NWC/Sales: {ratio:.4f} [DERIVED: BS year {latest_bs_year} with TTM sales]")
                        logger.info(f"    Operating CA: Rs {inventories.get(latest_bs_year, 0) + debtors.get(latest_bs_year, 0):,.0f} Cr")
                        logger.info(f"    Current Liab: Rs {current_liab:,.0f} Cr "
                                   f"(Total={tot_liab.get(latest_bs_year, 0):.0f} - LT={lt_borrow.get(latest_bs_year, 0):.0f})")
                        logger.info(f"    Net WC:       Rs {nwc:,.0f} Cr")
                        if is_high_growth and ratio > 0.60:
                            logger.info(f"  ** NORMALIZED to 0.30 for high-growth")
                            return 0.30
                        return ratio

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
                        cagr_str = f"{recent_cagr_3y:.1%}" if recent_cagr_3y is not None else "N/A"
                        logger.warning(f"  NWC/Sales: {normalized_nwc:.4f} [NORMALIZED for high-growth: "
                                       f"historical {historical_avg:.1%} using OLD METHOD (payables only), "
                                       f"using steady-state norm. 3Y CAGR: {cagr_str}]")
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
        Estimate effective tax rate.
        ACTUAL_CF (cash tax paid) → DERIVED_ACCRUAL (1-PAT/PBT) → DEFAULT fallback chain.
        """
        pat = financials.get('pat_annual', financials.get('pat', {}))
        if not pat:
            self._data_sources['tax'] = 'DEFAULT'
            logger.info(f"  Tax rate: 0.2500 [DEFAULT: no PAT data]")
            return 0.25

        # Method 0 [ACTUAL_CF]: Use actual cash tax paid from CF statement
        cf_tax_paid = financials.get('cf_tax_paid_yearly', {})
        pbt_for_cash_tax = financials.get('pbt_excp_yearly', {})
        if cf_tax_paid and pbt_for_cash_tax:
            cash_tax_rates = []
            for year in sorted(cf_tax_paid.keys(), reverse=True)[:3]:
                if year in pbt_for_cash_tax and pbt_for_cash_tax[year] and pbt_for_cash_tax[year] > 0:
                    # cf_tax_paid is typically negative (cash outflow) — use absolute value
                    tax = abs(cf_tax_paid[year]) / pbt_for_cash_tax[year]
                    if 0 < tax < 0.50:
                        cash_tax_rates.append(tax)
                        logger.debug(f"  Cash tax rate {year}: |{cf_tax_paid[year]:.1f}|/{pbt_for_cash_tax[year]:.1f} "
                                     f"= {tax:.4f} [ACTUAL_CF: cf_tax_paid]")
            if cash_tax_rates:
                result = round(np.mean(cash_tax_rates), 4)
                self._data_sources['tax'] = 'ACTUAL_CF'
                logger.info(f"  Tax rate: {result:.4f} [ACTUAL_CF: cash tax paid / PBT avg {len(cash_tax_rates)}yr]")
                return result

        # Method 1 [DERIVED_ACCRUAL]: Use pbt_excp_yearly (actual yearly PBT excl exceptional items)
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
                method1_result = round(np.mean(tax_rates), 4)

                # Cross-check: If only 1 year of data AND rate seems anomalous (>30%),
                # also compute via Method 3 (PBIDT - Int - Dep) which may have more years.
                # If Method 3 gives a substantially different rate (>5pp), blend both.
                if len(tax_rates) == 1 and method1_result > 0.30:
                    method3_rate = self._tax_rate_method3(financials, pat)
                    if method3_rate is not None and abs(method3_rate - method1_result) > 0.05:
                        blended = round((method1_result + method3_rate) / 2, 4)
                        logger.info(f"  Tax rate: {blended:.4f} [BLENDED: Method1={method1_result:.4f} (1yr pbt_excp) "
                                   f"+ Method3={method3_rate:.4f} (PBIDT-Int-Dep), avg of both]")
                        return blended

                self._data_sources.setdefault('tax', 'DERIVED_ACCRUAL')
                logger.info(f"  Tax rate: {method1_result:.4f} [DERIVED_ACCRUAL: 1 - PAT/pbt_excp avg {len(tax_rates)}yr]")
                return method1_result

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
        method3_rate = self._tax_rate_method3(financials, pat)
        if method3_rate is not None:
            logger.info(f"  Tax rate: {method3_rate:.4f} [DERIVED: PBIDT - Int - Depr]")
            return method3_rate

        logger.info(f"  Tax rate: 0.2500 [DEFAULT]")
        return 0.25

    def _tax_rate_method3(self, financials: dict, pat: dict) -> Optional[float]:
        """
        Estimate tax rate from PBIDT - Interest - Depreciation.
        Returns average tax rate or None if insufficient data.
        """
        pbidt = financials.get('pbidt_annual', financials.get('pbidt', {}))
        if not pbidt or not pat:
            return None

        interest = financials.get('interest_yearly', financials.get('interest_expense', {}))
        acc_dep = financials.get('acc_dep_yearly', {})
        gross_block = financials.get('gross_block', {})
        net_block = financials.get('net_block', {})

        tax_rates = []
        for year in sorted(pat.keys(), reverse=True)[:3]:
            if year not in pbidt or not pbidt[year]:
                continue
            int_exp = interest.get(year, 0) or 0
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
            return round(np.mean(tax_rates), 4)
        return None

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

        # Method 2 [DERIVED]: Balance sheet residual using actual CSV columns only.
        # From balance sheet identity (Assets = Liabilities + Equity):
        #   Cash + Net Block + CWIP + Inventories + Debtors + OtherAssets
        #     = Networth + Debt + Trade Payables + OtherLiabilities
        # Assuming OtherLiabilities ≈ OtherAssets (provisions, advances, etc.):
        #   Cash ≈ (Networth + Debt + Trade Payables) - (Net Block + CWIP + Inventories + Debtors)
        # All values from actual core CSV columns — no arbitrary multipliers.
        networth = self.core.get_latest_value(financials.get('networth', {}))
        debt_val = self.core.get_latest_value(financials.get('debt', {}))
        trade_pay = self.core.get_latest_value(financials.get('trade_payables', {}))
        net_block = self.core.get_latest_value(financials.get('net_block', {}))
        cwip_val = self.core.get_latest_value(financials.get('cwip', {}))
        inv_val = self.core.get_latest_value(financials.get('inventories', {}))
        debtors_val = self.core.get_latest_value(financials.get('sundry_debtors', {}))

        if networth and net_block:
            liab_side = (networth or 0) + (debt_val or 0) + (trade_pay or 0)
            asset_side = (net_block or 0) + (cwip_val or 0) + (inv_val or 0) + (debtors_val or 0)
            cash_residual = liab_side - asset_side

            logger.info(f"  Cash residual calc: (NW={networth:,.0f} + Debt={debt_val or 0:,.0f} "
                        f"+ TP={trade_pay or 0:,.0f}) - (NB={net_block:,.0f} + CWIP={cwip_val or 0:,.0f} "
                        f"+ Inv={inv_val or 0:,.0f} + Dr={debtors_val or 0:,.0f}) = {cash_residual:,.0f}")

            if cash_residual > 0:
                logger.info(f"  Cash & equivalents: Rs {cash_residual:,.1f} Cr "
                            f"[DERIVED: balance sheet residual from actual CSV columns]")
                return round(cash_residual, 2)
            else:
                logger.info(f"  Cash residual is {cash_residual:,.0f} (negative/zero), setting cash=0")

        logger.warning("  Cash & equivalents: Rs 0.0 Cr [DEFAULT: no cash data in core CSV]")
        return 0.0

    def _estimate_shares_outstanding(self, financials: dict,
                                      price_data: dict) -> float:
        """
        Estimate shares outstanding.
        ACTUAL_COLUMN (paid-up shares) → DERIVED_MCAP (MCap/CMP) → DEFAULT fallback.

        Cross-validates ACTUAL against MCap/CMP. If material divergence (>20%),
        uses MCap/CMP — which reflects post-bonus/split reality from market price.
        """
        # Compute MCap/CMP reference first (used for cross-validation)
        derived_shares_cr = None
        mcap = price_data.get('mcap_cr') if price_data else None
        cmp = price_data.get('cmp') if price_data else None
        if mcap and cmp and cmp > 0:
            derived_shares_cr = mcap / cmp

        # Method 0 [ACTUAL_COLUMN]: Use actual shares_outstanding from balance sheet
        shares_yearly = financials.get('shares_outstanding_yearly', {})
        if shares_yearly:
            latest_shares = self.core.get_latest_value(shares_yearly)
            if latest_shares and latest_shares > 0:
                # shares_outstanding is in absolute numbers, convert to crores
                # BS_Number of Equity Shares Paid Up is in actual count
                if latest_shares > 1e6:
                    shares_cr = latest_shares / 1e7  # Actual count → crores
                elif latest_shares > 100:
                    shares_cr = latest_shares / 100   # Lakhs → crores
                else:
                    shares_cr = latest_shares          # Already in crores

                # Cross-validate against MCap/CMP — the market price already
                # reflects any bonus/split, so MCap/CMP is the ground truth
                if derived_shares_cr and derived_shares_cr > 0 and shares_cr > 0:
                    divergence = abs(shares_cr - derived_shares_cr) / derived_shares_cr
                    if divergence > 0.20:
                        logger.warning(f"  Shares divergence: ACTUAL={shares_cr:.4f} Cr vs "
                                       f"MCap/CMP={derived_shares_cr:.4f} Cr ({divergence:.0%} off). "
                                       f"Using MCap/CMP (market price reflects bonus/split reality)")
                        self._data_sources['shares'] = 'DERIVED_MCAP'
                        return round(derived_shares_cr, 4)
                    else:
                        logger.debug(f"  Shares cross-check OK: ACTUAL={shares_cr:.4f} vs "
                                     f"MCap/CMP={derived_shares_cr:.4f} ({divergence:.0%} divergence)")

                self._data_sources['shares'] = 'ACTUAL_COLUMN'
                logger.info(f"  Shares outstanding: {shares_cr:.4f} Cr [ACTUAL_COLUMN: paid-up shares = {latest_shares:,.0f}]")
                return round(shares_cr, 4)

        # Method 1 [DERIVED_MCAP]: MCap / CMP
        if derived_shares_cr and derived_shares_cr > 0:
            self._data_sources.setdefault('shares', 'DERIVED_MCAP')
            return round(derived_shares_cr, 4)

        # Fallback: use latest market cap from quarterly data
        mcap_quarterly = financials.get('market_cap_quarterly',
                                         financials.get('market_cap', {}))
        core_mcap = self.core.get_latest_quarterly(mcap_quarterly) if hasattr(
            self.core, 'get_latest_quarterly') else self.core.get_latest_value(mcap_quarterly)
        if core_mcap and cmp and cmp > 0:
            self._data_sources.setdefault('shares', 'DERIVED_MCAP')
            return round(core_mcap / cmp, 4)

        self._data_sources['shares'] = 'DEFAULT'
        logger.warning(f"Cannot estimate shares for {financials.get('company_name')}")
        return 1.0

    def _get_avg_roe_5yr(self, financials: dict) -> float:
        """Helper to get 5-year average ROE from fullstats or annual data."""
        # Try fullstats 5Y avg ROE (quarterly series, latest value = rolling 5Y average)
        avg_roe_5yr_q = financials.get('avg_roe_5yr_quarterly', {})
        if avg_roe_5yr_q:
            sorted_keys = sorted(avg_roe_5yr_q.keys())
            avg_roe_5yr = avg_roe_5yr_q[sorted_keys[-1]]
            # Convert from % to decimal if needed
            if avg_roe_5yr is not None and avg_roe_5yr > 1:
                avg_roe_5yr = avg_roe_5yr / 100
            return avg_roe_5yr

        # Fall back to annual ROE series average
        roe_series = financials.get('roe', {})
        if roe_series:
            avg_roe = self.core.calculate_average(roe_series, years=5)
            if avg_roe and avg_roe > 1:
                avg_roe = avg_roe / 100
            return avg_roe

        return None

    def _get_dynamic_blend_ratio(self, financials: dict, valuation_subgroup: str) -> tuple:
        """
        WEEK 2 FIX: Determine dynamic blend ratio based on ROE stability.

        Returns: (historical_weight, convergence_weight)

        Logic:
        - Network/regulated moat sectors: 90/10 (minimal mean reversion)
        - Stable ROE (σ < 3pp): 80/20 (trust historical more)
        - Moderate volatility: 60/40 (default)
        - Declining trend (>2pp/yr): 50/50 (sector convergence important)
        - High volatility (σ > 5pp): 60/40 (moderate reversion)
        """
        # Default: 60/40 (moderate volatility)
        hist_weight, conv_weight = 0.6, 0.4

        roe_series = financials.get('roe', {})
        if not roe_series or len(roe_series) < 3:
            return (hist_weight, conv_weight)

        # Get last 5 years of ROE
        sorted_years = sorted(roe_series.keys(), reverse=True)[:5]
        roe_values = [roe_series[y] for y in sorted_years if roe_series.get(y) is not None]

        if len(roe_values) < 3:
            return (hist_weight, conv_weight)

        # Convert from % to decimal if needed
        roe_values = [v / 100 if v > 1 else v for v in roe_values]

        # Calculate volatility (standard deviation)
        roe_std = np.std(roe_values)

        # Calculate trend (linear slope)
        years_numeric = list(range(len(roe_values)))
        slope, _ = np.polyfit(years_numeric, roe_values, 1) if len(roe_values) >= 2 else (0, 0)

        # Check for network/regulated moat sectors
        moat_subgroups = [
            'TELECOM', 'UTILITIES', 'INFRA_POWER', 'INFRA_ROADS',
            'INFRA_PORTS', 'INFRA_AIRPORTS', 'ENERGY_TRANSMISSION'
        ]
        is_moat_sector = any(mg in valuation_subgroup.upper() for mg in moat_subgroups)

        # Determine blend ratio
        if is_moat_sector:
            hist_weight, conv_weight = 0.9, 0.1
            reason = "regulated moat"
        elif roe_std < 0.03:  # σ < 3pp
            hist_weight, conv_weight = 0.8, 0.2
            reason = f"stable (σ={roe_std:.1%})"
        elif slope < -0.02:  # Declining >2pp/yr
            hist_weight, conv_weight = 0.5, 0.5
            reason = f"declining ({slope:.1%}/yr)"
        elif roe_std > 0.05:  # σ > 5pp
            hist_weight, conv_weight = 0.6, 0.4
            reason = f"volatile (σ={roe_std:.1%})"
        else:
            # Moderate (default)
            reason = "moderate"

        logger.info(f"  Dynamic ROCE blend: {hist_weight:.0%}/{conv_weight:.0%} ({reason})")
        return (hist_weight, conv_weight)

    def _estimate_terminal_roce(self, financials: dict,
                                 terminal_assumptions: dict,
                                 valuation_subgroup: str = None) -> float:
        """
        Estimate sustainable ROCE for terminal value.
        ACTUAL_CE (capital employed) → DERIVED_NWDEBT (NW+Debt proxy) → DEFAULT.

        WEEK 2 FIX: Uses dynamic blend ratio based on ROE stability and
        company-specific convergence when ROE-ROCE divergence >5pp.
        """
        # Method 0 [ACTUAL_CE]: Use actual capital_employed for ROCE calculation
        capital_employed_yearly = financials.get('capital_employed_yearly', {})
        op_profit_yearly = financials.get('op_profit_yearly', {})
        if capital_employed_yearly and op_profit_yearly:
            # --- SANITY CHECK: Cross-validate CE against NW+Debt proxy ---
            # If CE diverges >50% from (NW+Debt) for the same year, CE data is suspect
            debt_series = financials.get('debt', {})
            nw_series = financials.get('networth', {})
            ce_validated = {}
            for year in capital_employed_yearly:
                ce = capital_employed_yearly[year]
                if not ce or ce <= 0:
                    continue
                nw = nw_series.get(year)
                debt = debt_series.get(year, 0) or 0
                if nw and nw > 0:
                    nw_debt_proxy = nw + debt
                    divergence = abs(ce - nw_debt_proxy) / nw_debt_proxy if nw_debt_proxy > 0 else 0
                    if divergence > 0.50:
                        logger.warning(f"  CE sanity check {year}: CE={ce:.1f} vs NW+Debt={nw_debt_proxy:.1f} "
                                       f"({divergence:.0%} divergence > 50% threshold). Using NW+Debt as substitute.")
                        ce_validated[year] = nw_debt_proxy
                    else:
                        ce_validated[year] = ce
                else:
                    ce_validated[year] = ce

            if not ce_validated:
                logger.warning(f"  All CE values failed sanity check vs NW+Debt. "
                               f"Falling back to DERIVED_NWDEBT method.")
            else:
                tax_rate = self._estimate_effective_tax_rate(financials)
                roce_from_ce = []
                for year in sorted(ce_validated.keys(), reverse=True)[:5]:
                    ce = ce_validated[year]
                    ebit = op_profit_yearly.get(year)
                    if ce and ce > 0 and ebit and ebit > 0:
                        nopat = ebit * (1 - tax_rate)
                        roce = nopat / ce
                        roce_from_ce.append(roce)
                        logger.debug(f"  ROCE {year}: NOPAT={nopat:.1f} / CE={ce:.1f} = {roce:.4f} [ACTUAL_CE]")
                if roce_from_ce:
                    avg_roce = np.mean(roce_from_ce)

                    # WEEK 2 FIX: Dynamic blend ratio + company-specific convergence
                    hist_weight, conv_weight = self._get_dynamic_blend_ratio(financials, valuation_subgroup or '')
                    convergence = terminal_assumptions.get('roce_convergence', 0.15)

                    # WEEK 2 FIX: Use company-specific convergence if ROE-ROCE divergence >5pp
                    avg_roe_5yr = self._get_avg_roe_5yr(financials)
                    if avg_roe_5yr and avg_roe_5yr > 0:
                        divergence_pp = abs(avg_roe_5yr - avg_roce) * 100
                        if divergence_pp > 5:
                            company_convergence = avg_roce * 0.7 + avg_roe_5yr * 0.3
                            logger.info(f"  Company-specific convergence: {company_convergence:.2%} "
                                       f"(ROCE={avg_roce:.2%}, ROE={avg_roe_5yr:.2%}, div={divergence_pp:.1f}pp)")
                            convergence = company_convergence

                    terminal = avg_roce * hist_weight + convergence * conv_weight
                    result = round(max(0.08, min(terminal, 0.30)), 4)
                    self._data_sources['roce'] = 'ACTUAL_CE'
                    logger.info(f"  Terminal ROCE: {result:.4f} [ACTUAL_CE: avg {len(roce_from_ce)}yr NOPAT/CapitalEmployed "
                                f"blended {hist_weight:.0%}/{conv_weight:.0%} with convergence={convergence:.2%}]")
                    # Cross-check with fullstats 5Y avg ROE if available
                    self._crosscheck_terminal_roce_with_roe(financials, result)
                    return result

        # Method 1 [DERIVED_NWDEBT]: Use roce from CSV (based on NW+Debt proxy)
        roce_series = financials.get('roce', {})
        if not roce_series:
            self._data_sources['roce'] = 'DEFAULT'
            return terminal_assumptions.get('roce_convergence', 0.15)

        avg_roce = self.core.calculate_average(roce_series, years=5)
        if avg_roce:
            # Convert from % to decimal if needed
            if avg_roce > 1:
                avg_roce = avg_roce / 100

            # WEEK 2 FIX: Dynamic blend ratio + company-specific convergence
            hist_weight, conv_weight = self._get_dynamic_blend_ratio(financials, valuation_subgroup or '')
            convergence = terminal_assumptions.get('roce_convergence', 0.15)

            # WEEK 2 FIX: Use company-specific convergence if ROE-ROCE divergence >5pp
            avg_roe_5yr = self._get_avg_roe_5yr(financials)
            if avg_roe_5yr and avg_roe_5yr > 0:
                divergence_pp = abs(avg_roe_5yr - avg_roce) * 100
                if divergence_pp > 5:
                    company_convergence = avg_roce * 0.7 + avg_roe_5yr * 0.3
                    logger.info(f"  Company-specific convergence: {company_convergence:.2%} "
                               f"(ROCE={avg_roce:.2%}, ROE={avg_roe_5yr:.2%}, div={divergence_pp:.1f}pp)")
                    convergence = company_convergence

            # Blend historical with sector convergence
            terminal = avg_roce * hist_weight + convergence * conv_weight
            terminal_result = round(max(0.08, min(terminal, 0.30)), 4)
            self._data_sources.setdefault('roce', 'DERIVED_NWDEBT')
            logger.info(f"  Terminal ROCE: {terminal_result:.4f} [DERIVED_NWDEBT: "
                       f"blended {hist_weight:.0%}/{conv_weight:.0%} with convergence={convergence:.2%}]")
            # Cross-check with fullstats 5Y avg ROE if available
            self._crosscheck_terminal_roce_with_roe(financials, terminal_result)
            return terminal_result

        self._data_sources.setdefault('roce', 'DEFAULT')
        return terminal_assumptions.get('roce_convergence', 0.15)

    def _crosscheck_terminal_roce_with_roe(self, financials: dict, terminal_roce: float):
        """
        Cross-check computed terminal ROCE against fullstats 5Y avg ROE.
        If divergence > 5pp, log WARNING and average both for a more robust estimate.
        This is informational — does not override the terminal ROCE value.
        """
        # Try fullstats 5Y avg ROE (quarterly series, latest value = rolling 5Y average)
        avg_roe_5yr_q = financials.get('avg_roe_5yr_quarterly', {})
        avg_roe_5yr = None
        if avg_roe_5yr_q:
            sorted_keys = sorted(avg_roe_5yr_q.keys())
            avg_roe_5yr = avg_roe_5yr_q[sorted_keys[-1]]
            # Convert from % to decimal if needed
            if avg_roe_5yr is not None and avg_roe_5yr > 1:
                avg_roe_5yr = avg_roe_5yr / 100

        # Fall back to annual ROE series average
        if avg_roe_5yr is None:
            roe_series = financials.get('roe', {})
            if roe_series:
                avg_roe_raw = self.core.calculate_average(roe_series, years=5)
                if avg_roe_raw is not None:
                    avg_roe_5yr = avg_roe_raw / 100 if avg_roe_raw > 1 else avg_roe_raw

        if avg_roe_5yr is None:
            return

        divergence_pp = abs(terminal_roce - avg_roe_5yr) * 100

        if divergence_pp > 5:
            logger.warning(f"  Terminal ROCE cross-check: ROCE={terminal_roce:.2%} vs "
                           f"5Y avg ROE={avg_roe_5yr:.2%} (divergence={divergence_pp:.1f}pp > 5pp threshold). "
                           f"Blended average={(terminal_roce + avg_roe_5yr) / 2:.2%}")
        else:
            logger.info(f"  Terminal ROCE cross-check: ROCE={terminal_roce:.2%} vs "
                        f"5Y avg ROE={avg_roe_5yr:.2%} (divergence={divergence_pp:.1f}pp — within 5pp)")

    def _estimate_terminal_reinvestment(self, financials: dict,
                                         terminal_assumptions: dict) -> float:
        """
        Estimate terminal reinvestment rate.
        ACTUAL_PAYOUT (1 - dividend payout) → DERIVED_CAPEX (capex/NOPAT) → DEFAULT.

        When both methods available, use weighted average: 60% capex method, 40% payout method.
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

        # Check dividend payout ratio for cross-check / alternative method
        payout_reinvestment = None
        dividend_payout = financials.get('dividend_payout_ratio_yearly', {})
        if dividend_payout:
            recent_payouts = sorted(dividend_payout.keys(), reverse=True)[:3]
            payout_vals = [dividend_payout[y] for y in recent_payouts
                           if dividend_payout[y] is not None and 0 < dividend_payout[y] < 100]
            if payout_vals:
                avg_payout = np.mean(payout_vals)
                # Payout is in %, convert to decimal
                if avg_payout > 1:
                    avg_payout = avg_payout / 100
                payout_reinvestment = round(max(0.10, min(1 - avg_payout, 0.60)), 4)
                logger.info(f"  Payout-based reinvestment: {payout_reinvestment:.4f} "
                            f"[ACTUAL_PAYOUT: 1 - avg_payout({avg_payout:.1%}) = {1-avg_payout:.1%}]")

        # Method 1 [DERIVED_CAPEX]: Use op_profit_yearly + pur_of_fixed_assets
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
                        sector_default = terminal_assumptions.get('reinvestment_rate') or 0.30
                        cagr_str = f"{recent_cagr_3y:.1%}" if recent_cagr_3y is not None else "N/A"
                        logger.info(f"  Terminal reinvestment: {sector_default:.4f} "
                                    f"[NORMALIZED for high-growth: historical {historical_reinv:.1%} "
                                    f"includes expansion capex, using sector steady-state. "
                                    f"3Y CAGR: {cagr_str}]")
                        self._data_sources['reinvest'] = 'DERIVED_CAPEX'
                        return sector_default
                    else:
                        capex_result = round(max(0.10, min(historical_reinv, 0.60)), 4)
                        # Blend with payout method if both available (60% capex, 40% payout)
                        if payout_reinvestment is not None:
                            blended = round(capex_result * 0.60 + payout_reinvestment * 0.40, 4)
                            self._data_sources['reinvest'] = 'ACTUAL_PAYOUT'
                            logger.info(f"  Terminal reinvestment: {blended:.4f} "
                                        f"[BLENDED: capex_method={capex_result:.4f}*0.6 + "
                                        f"payout_method={payout_reinvestment:.4f}*0.4]")
                            return blended
                        self._data_sources['reinvest'] = 'DERIVED_CAPEX'
                        logger.info(f"  Terminal reinvestment: {capex_result:.4f} "
                                    f"[DERIVED_CAPEX: pur_of_fixed_assets/NOPAT = {abs(latest_capex):.1f}/{nopat:.1f}]")
                        return capex_result

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
                    # Blend with payout if available
                    if payout_reinvestment is not None:
                        blended = round(result * 0.60 + payout_reinvestment * 0.40, 4)
                        self._data_sources['reinvest'] = 'ACTUAL_PAYOUT'
                        logger.info(f"  Terminal reinvestment: {blended:.4f} "
                                    f"[BLENDED: qtr_capex={result:.4f}*0.6 + payout={payout_reinvestment:.4f}*0.4]")
                        return blended
                    self._data_sources.setdefault('reinvest', 'DERIVED_CAPEX')
                    logger.info(f"  Terminal reinvestment: {result:.4f} [DERIVED_CAPEX: quarterly TTM]")
                    return result

        # If only payout-based available (no capex data)
        if payout_reinvestment is not None:
            self._data_sources['reinvest'] = 'ACTUAL_PAYOUT'
            logger.info(f"  Terminal reinvestment: {payout_reinvestment:.4f} [ACTUAL_PAYOUT: 1 - dividend_payout]")
            return payout_reinvestment

        default = terminal_assumptions.get('reinvestment_rate', 0.30)
        self._data_sources['reinvest'] = 'DEFAULT'
        logger.info(f"  Terminal reinvestment: {default:.4f} [DEFAULT]")
        return default

    def _get_rd_to_sales(self, financials: dict, valuation_subgroup: str = None) -> Optional[float]:
        """
        Get R&D as % of sales for applicable subgroups.
        Only relevant for PHARMA_MFG, DEFENSE, MEDICAL_EQUIPMENT, PRODUCT_SAAS.
        For these subgroups, R&D should be treated as growth capex in DCF.
        """
        RD_SUBGROUPS = {
            'HEALTHCARE_PHARMA_MFG', 'INDUSTRIALS_DEFENSE',
            'HEALTHCARE_MEDICAL_EQUIPMENT', 'TECHNOLOGY_PRODUCT_SAAS',
        }
        if valuation_subgroup and valuation_subgroup not in RD_SUBGROUPS:
            return None

        rd_series = financials.get('rd_pct_of_sales_yearly', {})
        if not rd_series:
            return None

        # Average last 3 years
        recent = sorted(rd_series.keys(), reverse=True)[:3]
        vals = [rd_series[y] for y in recent if rd_series[y] is not None and rd_series[y] > 0]
        if not vals:
            return None

        avg_rd = round(np.mean(vals), 4)
        # Convert from % to decimal if needed
        if avg_rd > 1:
            avg_rd = avg_rd / 100

        logger.info(f"  R&D/Sales: {avg_rd:.4f} [ACTUAL: rd_pct_of_sales avg {len(vals)}yr]")
        return avg_rd

    def _compute_banking_drivers_from_fullstats(self, financials: dict) -> dict:
        """
        Compute banking-specific driver values from fullstats YYYY_metric columns.
        Returns dict mapping driver_name → {value, trend, source}.
        """
        drivers = {}

        # CASA ratio (direct value)
        casa = financials.get('casa_yearly', {})
        if casa:
            recent = sorted(casa.keys(), reverse=True)[:3]
            vals = [casa[y] for y in recent if casa[y] is not None and casa[y] > 0]
            if vals:
                drivers['casa_ratio'] = {
                    'value': round(vals[0], 2),
                    'trend': 'UP' if len(vals) >= 2 and vals[0] > vals[1] else
                             ('DOWN' if len(vals) >= 2 and vals[0] < vals[1] else 'STABLE'),
                    'source': 'COMPUTED',
                }
                logger.debug(f"  CASA ratio: {vals[0]:.2f}% [COMPUTED from fullstats]")

        # Cost-to-income ratio (direct value, lower is better)
        ci = financials.get('cost_income_ratio_yearly', {})
        if ci:
            recent = sorted(ci.keys(), reverse=True)[:3]
            vals = [ci[y] for y in recent if ci[y] is not None and ci[y] > 0]
            if vals:
                drivers['cost_to_income_ratio'] = {
                    'value': round(vals[0], 2),
                    'trend': 'DOWN' if len(vals) >= 2 and vals[0] < vals[1] else
                             ('UP' if len(vals) >= 2 and vals[0] > vals[1] else 'STABLE'),
                    'source': 'COMPUTED',
                }
                logger.debug(f"  Cost/Income: {vals[0]:.2f}% [COMPUTED from fullstats]")

        # Credit-deposit ratio (for credit_growth driver — YoY change)
        cd = financials.get('credit_deposits_yearly', {})
        if cd and len(cd) >= 2:
            recent = sorted(cd.keys(), reverse=True)[:2]
            curr = cd.get(recent[0])
            prev = cd.get(recent[1])
            if curr and prev and prev > 0:
                yoy_change = (curr - prev) / prev * 100
                drivers['credit_growth'] = {
                    'value': round(yoy_change, 2),
                    'trend': 'UP' if yoy_change > 0 else ('DOWN' if yoy_change < 0 else 'STABLE'),
                    'source': 'COMPUTED',
                }
                logger.debug(f"  Credit growth: {yoy_change:.2f}% YoY [COMPUTED from credit_deposits]")

        # NIM (direct value from fullstats)
        nim = financials.get('nim_yearly', {})
        if nim:
            recent = sorted(nim.keys(), reverse=True)[:3]
            vals = [nim[y] for y in recent if nim[y] is not None and nim[y] > 0]
            if vals:
                drivers['net_interest_margin'] = {
                    'value': round(vals[0], 2),
                    'trend': 'UP' if len(vals) >= 2 and vals[0] > vals[1] else
                             ('DOWN' if len(vals) >= 2 and vals[0] < vals[1] else 'STABLE'),
                    'source': 'COMPUTED',
                }
                logger.debug(f"  NIM: {vals[0]:.2f}% [COMPUTED from fullstats]")

        return drivers

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
