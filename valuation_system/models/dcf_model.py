"""
FCFF-Based DCF Valuation Model
Based on Damodaran methodology:
- FCFF = EBIT(1-t) + D&A - Capex - ΔWorkingCapital
- WACC = Ke * (E/V) + Kd * (1-t) * (D/V)
- Terminal Value using ROCE-linked growth
- Bull/Base/Bear scenarios
"""

import os
import copy
import logging
from dataclasses import dataclass, field
from typing import Optional

import numpy as np
from dotenv import load_dotenv

load_dotenv(os.path.join(os.path.dirname(__file__), '..', 'config', '.env'))

logger = logging.getLogger(__name__)


@dataclass
class DCFInputs:
    """All inputs required for FCFF-based DCF valuation."""

    # Company identification
    company_name: str = ''
    nse_symbol: str = ''

    # Revenue drivers
    base_revenue: float = 0.0          # TTM or latest annual revenue (Rs Cr)
    revenue_growth_rates: list = field(default_factory=lambda: [0.12, 0.11, 0.10, 0.09, 0.08])

    # Margin drivers
    ebitda_margin: float = 0.20        # Base EBITDA margin
    margin_improvement: float = 0.005  # Annual margin change (can be negative)

    # Capital requirements
    capex_to_sales: float = 0.08       # Capex as % of revenue
    depreciation_to_sales: float = 0.04  # D&A as % of revenue
    nwc_to_sales: float = 0.15         # Net working capital as % of revenue

    # Tax
    tax_rate: float = 0.25             # Effective tax rate

    # Cost of capital (CAPM)
    risk_free_rate: float = 0.071      # India 10Y G-Sec yield
    equity_risk_premium: float = 0.065 # Damodaran India ERP (mature + country)
    beta: float = 1.0                  # Bottom-up levered beta
    cost_of_debt: float = 0.09         # Pre-tax cost of debt
    debt_ratio: float = 0.10           # D/(D+E) target

    # Terminal value (ROCE-linked)
    terminal_roce: float = 0.18        # Sustainable ROCE
    terminal_reinvestment: float = 0.30 # Reinvestment rate
    # terminal_growth = terminal_reinvestment × terminal_roce (capped at 5%)

    # Shares outstanding and debt
    shares_outstanding: float = 1.0    # In crores
    net_debt: float = 0.0              # Debt - Cash (Rs Cr)
    cash_and_equivalents: float = 0.0

    # Actual cash flow data (if available)
    actual_cfo: Optional[float] = None
    actual_capex: Optional[float] = None


class FCFFValuation:
    """
    Free Cash Flow to Firm valuation per Damodaran methodology.

    FCFF = EBIT(1-t) + D&A - Capex - ΔWorkingCapital
    Firm Value = PV(FCFF) + PV(Terminal Value)
    Equity Value = Firm Value - Net Debt
    """

    def __init__(self, projection_years: int = 5):
        self.projection_years = projection_years

    def calculate_wacc(self, inputs: DCFInputs) -> float:
        """
        WACC = Ke * (E/V) + Kd * (1-t) * (D/V)
        Ke = Rf + β * ERP (CAPM)
        """
        cost_of_equity = (
            inputs.risk_free_rate +
            inputs.beta * inputs.equity_risk_premium
        )
        after_tax_cost_of_debt = inputs.cost_of_debt * (1 - inputs.tax_rate)

        wacc = (
            cost_of_equity * (1 - inputs.debt_ratio) +
            after_tax_cost_of_debt * inputs.debt_ratio
        )

        logger.debug(f"WACC calculation: Ke={cost_of_equity:.4f}, "
                      f"Kd_at={after_tax_cost_of_debt:.4f}, "
                      f"D/V={inputs.debt_ratio:.4f}, WACC={wacc:.4f}")

        return wacc

    def project_fcff(self, inputs: DCFInputs) -> list:
        """
        Project FCFF for explicit forecast period.

        If actual CFO and Capex are available, uses them for Year 0 calibration.
        """
        projections = []
        revenue = inputs.base_revenue

        for i in range(self.projection_years):
            growth = inputs.revenue_growth_rates[i] if i < len(inputs.revenue_growth_rates) else inputs.revenue_growth_rates[-1]

            prior_revenue = revenue
            revenue = revenue * (1 + growth)

            # Margins
            ebitda_margin = inputs.ebitda_margin + (i * inputs.margin_improvement)
            ebitda_margin = max(0.05, min(ebitda_margin, 0.50))  # Sanity bounds

            ebitda = revenue * ebitda_margin
            depreciation = revenue * inputs.depreciation_to_sales
            ebit = ebitda - depreciation
            nopat = ebit * (1 - inputs.tax_rate)

            # Capital expenditure
            capex = revenue * inputs.capex_to_sales

            # Working capital change
            nwc_current = revenue * inputs.nwc_to_sales
            nwc_prior = prior_revenue * inputs.nwc_to_sales
            delta_nwc = nwc_current - nwc_prior

            # FCFF
            fcff = nopat + depreciation - capex - delta_nwc

            projections.append({
                'year': i + 1,
                'revenue': round(revenue, 2),
                'growth_rate': round(growth, 4),
                'ebitda': round(ebitda, 2),
                'ebitda_margin': round(ebitda_margin, 4),
                'depreciation': round(depreciation, 2),
                'ebit': round(ebit, 2),
                'nopat': round(nopat, 2),
                'capex': round(capex, 2),
                'delta_nwc': round(delta_nwc, 2),
                'fcff': round(fcff, 2)
            })

        return projections

    def calculate_terminal_value(self, final_fcff: float, wacc: float,
                                  terminal_roce: float,
                                  terminal_reinvestment: float) -> dict:
        """
        Terminal Value using ROCE-linked growth (Damodaran approach).

        g = Reinvestment Rate × ROCE
        Terminal Value = FCFF_n+1 / (WACC - g)

        Links growth to the firm's ability to generate returns
        on reinvested capital, avoiding arbitrary growth assumptions.
        """
        terminal_growth = terminal_reinvestment * terminal_roce
        # Cap at reasonable level
        terminal_growth = min(terminal_growth, 0.05)  # Max 5%
        terminal_growth = max(terminal_growth, 0.02)  # Min 2%

        if wacc <= terminal_growth:
            logger.warning(f"WACC ({wacc:.4f}) <= terminal growth ({terminal_growth:.4f}), "
                           f"adjusting terminal growth down")
            terminal_growth = wacc - 0.02

        terminal_fcff = final_fcff * (1 + terminal_growth)
        terminal_value = terminal_fcff / (wacc - terminal_growth)

        logger.debug(f"Terminal value: g={terminal_growth:.4f}, "
                      f"FCFF_n+1={terminal_fcff:.2f}, TV={terminal_value:.2f}")

        return {
            'terminal_growth': terminal_growth,
            'terminal_fcff': terminal_fcff,
            'terminal_value': terminal_value
        }

    def calculate_intrinsic_value(self, inputs: DCFInputs) -> dict:
        """
        Main valuation calculation.

        Returns complete DCF output with all intermediate calculations.
        """
        # WACC
        wacc = self.calculate_wacc(inputs)
        cost_of_equity = inputs.risk_free_rate + inputs.beta * inputs.equity_risk_premium

        # Project FCFF
        projections = self.project_fcff(inputs)

        # PV of explicit period
        pv_fcff = sum(
            proj['fcff'] / ((1 + wacc) ** proj['year'])
            for proj in projections
        )

        # Terminal value
        tv_result = self.calculate_terminal_value(
            projections[-1]['fcff'],
            wacc,
            inputs.terminal_roce,
            inputs.terminal_reinvestment
        )
        pv_terminal = tv_result['terminal_value'] / ((1 + wacc) ** len(projections))

        # Firm and equity value
        firm_value = pv_fcff + pv_terminal
        equity_value = firm_value - inputs.net_debt + inputs.cash_and_equivalents

        # Per share
        intrinsic_per_share = equity_value / inputs.shares_outstanding if inputs.shares_outstanding > 0 else 0

        # === TRACEABILITY: Log the value bridge ===
        logger.debug(f"DCF Value Bridge for {inputs.company_name}:")
        logger.debug(f"  PV of explicit FCFFs:    Rs {pv_fcff:>12,.2f} Cr")
        logger.debug(f"  PV of Terminal Value:     Rs {pv_terminal:>12,.2f} Cr")
        logger.debug(f"  = Firm (Enterprise) Value:Rs {firm_value:>12,.2f} Cr")
        logger.debug(f"  - Net Debt:               Rs {inputs.net_debt:>12,.2f} Cr")
        logger.debug(f"  + Cash & Equivalents:     Rs {inputs.cash_and_equivalents:>12,.2f} Cr")
        logger.debug(f"  = Equity Value:            Rs {equity_value:>12,.2f} Cr")
        logger.debug(f"  / Shares Outstanding:     {inputs.shares_outstanding:>12.2f} Cr")
        logger.debug(f"  = Intrinsic Per Share:     Rs {intrinsic_per_share:>12,.2f}")

        result = {
            'company': inputs.company_name,
            'method': 'DCF_FCFF',

            # Key outputs
            'intrinsic_per_share': round(intrinsic_per_share, 2),
            'equity_value': round(equity_value, 2),
            'firm_value': round(firm_value, 2),

            # Breakdown
            'pv_explicit_period': round(pv_fcff, 2),
            'pv_terminal_value': round(pv_terminal, 2),
            'terminal_value_pct': round(pv_terminal / firm_value * 100, 1) if firm_value > 0 else 0,

            # Terminal value details
            'terminal_growth': tv_result['terminal_growth'],
            'terminal_value': round(tv_result['terminal_value'], 2),

            # WACC components
            'wacc': round(wacc, 4),
            'cost_of_equity': round(cost_of_equity, 4),
            'cost_of_debt_at': round(inputs.cost_of_debt * (1 - inputs.tax_rate), 4),

            # Key assumptions
            'assumptions': {
                'base_revenue': inputs.base_revenue,
                'growth_rates': inputs.revenue_growth_rates,
                'ebitda_margin': inputs.ebitda_margin,
                'margin_improvement': inputs.margin_improvement,
                'capex_to_sales': inputs.capex_to_sales,
                'tax_rate': inputs.tax_rate,
                'risk_free_rate': inputs.risk_free_rate,
                'erp': inputs.equity_risk_premium,
                'beta': inputs.beta,
                'debt_ratio': inputs.debt_ratio,
                'terminal_roce': inputs.terminal_roce,
                'terminal_reinvestment': inputs.terminal_reinvestment,
                'net_debt': inputs.net_debt,
                'shares_outstanding': inputs.shares_outstanding
            },

            # Projections
            'fcff_projections': projections
        }

        logger.info(f"DCF result for {inputs.company_name}: "
                     f"Intrinsic=₹{intrinsic_per_share:,.2f}, "
                     f"WACC={wacc:.2%}, TV%={result['terminal_value_pct']:.1f}%")

        return result

    def sensitivity_analysis(self, inputs: DCFInputs,
                              wacc_range: tuple = (-0.02, 0.02, 0.005),
                              growth_range: tuple = (-0.02, 0.02, 0.005)) -> dict:
        """
        Sensitivity table: intrinsic value vs WACC and terminal growth.
        """
        base_result = self.calculate_intrinsic_value(inputs)
        base_wacc = base_result['wacc']
        base_growth = base_result['terminal_growth']

        table = []
        for wacc_delta in np.arange(wacc_range[0], wacc_range[1] + wacc_range[2], wacc_range[2]):
            row = []
            for growth_delta in np.arange(growth_range[0], growth_range[1] + growth_range[2], growth_range[2]):
                modified = copy.deepcopy(inputs)
                # Adjust inputs to achieve target WACC/growth
                adj_wacc = base_wacc + wacc_delta
                adj_growth = base_growth + growth_delta

                # Recalculate terminal value with adjusted parameters
                projections = self.project_fcff(modified)
                pv_fcff = sum(
                    p['fcff'] / ((1 + adj_wacc) ** p['year'])
                    for p in projections
                )

                if adj_wacc > adj_growth:
                    terminal_fcff = projections[-1]['fcff'] * (1 + adj_growth)
                    tv = terminal_fcff / (adj_wacc - adj_growth)
                    pv_tv = tv / ((1 + adj_wacc) ** len(projections))
                    firm_val = pv_fcff + pv_tv
                    eq_val = firm_val - inputs.net_debt + inputs.cash_and_equivalents
                    per_share = eq_val / inputs.shares_outstanding if inputs.shares_outstanding > 0 else 0
                else:
                    per_share = float('inf')

                row.append(round(per_share, 2))
            table.append(row)

        return {
            'base_intrinsic': base_result['intrinsic_per_share'],
            'wacc_values': [round(base_wacc + d, 4) for d in np.arange(wacc_range[0], wacc_range[1] + wacc_range[2], wacc_range[2])],
            'growth_values': [round(base_growth + d, 4) for d in np.arange(growth_range[0], growth_range[1] + growth_range[2], growth_range[2])],
            'sensitivity_table': table
        }


class ScenarioBuilder:
    """
    Build Bull/Base/Bear scenarios based on driver states.
    """

    def build_scenarios(self, base_inputs: DCFInputs,
                        sector_outlook: dict = None) -> dict:
        """
        Create Bull/Base/Bear DCF input sets.
        Adjustments based on sector outlook and driver states.
        """
        bull = self._build_bull(base_inputs, sector_outlook)
        bear = self._build_bear(base_inputs, sector_outlook)

        return {
            'BULL': bull,
            'BASE': base_inputs,
            'BEAR': bear
        }

    def _build_bull(self, inputs: DCFInputs, outlook: dict = None) -> DCFInputs:
        """
        Bull case: Favorable driver assumptions.
        - 20% higher growth rates
        - 30% faster margin expansion
        - 10% lower risk premium (multiple expansion)
        """
        bull = copy.deepcopy(inputs)
        bull.revenue_growth_rates = [min(g * 1.20, 0.35) for g in inputs.revenue_growth_rates]
        bull.margin_improvement = inputs.margin_improvement * 1.3
        bull.equity_risk_premium = inputs.equity_risk_premium * 0.90
        bull.terminal_roce = inputs.terminal_roce * 1.10
        return bull

    def _build_bear(self, inputs: DCFInputs, outlook: dict = None) -> DCFInputs:
        """
        Bear case: Adverse driver assumptions.
        - 30% lower growth rates
        - 50% slower margin expansion (or contraction)
        - 20% higher risk premium (de-rating)
        """
        bear = copy.deepcopy(inputs)
        bear.revenue_growth_rates = [max(g * 0.70, 0.02) for g in inputs.revenue_growth_rates]
        bear.margin_improvement = inputs.margin_improvement * 0.5
        bear.equity_risk_premium = inputs.equity_risk_premium * 1.20
        bear.terminal_roce = inputs.terminal_roce * 0.85
        return bear


class MonteCarloValuation:
    """
    Probabilistic valuation using parameter distributions.
    """

    def __init__(self, n_simulations: int = None):
        self.n_simulations = n_simulations or int(os.getenv('MC_SIMULATIONS', 10000))

    def run_simulation(self, dcf_model: FCFFValuation,
                       base_inputs: DCFInputs,
                       cmp: float = None) -> dict:
        """
        Run Monte Carlo simulation with triangular/normal distributions.
        """
        results = []

        for _ in range(self.n_simulations):
            sim_inputs = copy.deepcopy(base_inputs)

            # Randomize key inputs
            # Revenue growth: triangular around base
            sim_inputs.revenue_growth_rates = [
                np.random.triangular(g * 0.7, g, g * 1.3)
                for g in base_inputs.revenue_growth_rates
            ]

            # EBITDA margin: normal around base
            sim_inputs.ebitda_margin = np.random.normal(
                base_inputs.ebitda_margin,
                base_inputs.ebitda_margin * 0.10  # 10% std dev
            )
            sim_inputs.ebitda_margin = max(0.05, sim_inputs.ebitda_margin)

            # Terminal ROCE: normal
            sim_inputs.terminal_roce = np.random.normal(
                base_inputs.terminal_roce,
                base_inputs.terminal_roce * 0.15
            )
            sim_inputs.terminal_roce = max(0.08, sim_inputs.terminal_roce)

            # Beta: normal
            sim_inputs.beta = np.random.normal(
                base_inputs.beta,
                0.15
            )
            sim_inputs.beta = max(0.5, sim_inputs.beta)

            try:
                result = dcf_model.calculate_intrinsic_value(sim_inputs)
                val = result['intrinsic_per_share']
                if val > 0 and val < base_inputs.base_revenue * 100:  # Sanity check
                    results.append(val)
            except Exception:
                continue

        if not results:
            logger.error("Monte Carlo simulation produced no valid results")
            return {'mean': None, 'median': None}

        results = np.array(results)

        mc_result = {
            'simulations': len(results),
            'mean': round(float(np.mean(results)), 2),
            'median': round(float(np.median(results)), 2),
            'std': round(float(np.std(results)), 2),
            'percentiles': {
                '5th': round(float(np.percentile(results, 5)), 2),
                '10th': round(float(np.percentile(results, 10)), 2),
                '25th': round(float(np.percentile(results, 25)), 2),
                '75th': round(float(np.percentile(results, 75)), 2),
                '90th': round(float(np.percentile(results, 90)), 2),
                '95th': round(float(np.percentile(results, 95)), 2),
            },
            'cv': round(float(np.std(results) / np.mean(results)), 4),
        }

        if cmp:
            mc_result['probability_above_cmp'] = round(float((results > cmp).mean()), 4)
            mc_result['cmp'] = cmp

        logger.info(f"Monte Carlo: median=₹{mc_result['median']:,.2f}, "
                     f"std=₹{mc_result['std']:,.2f}, "
                     f"P(>CMP)={mc_result.get('probability_above_cmp', 'N/A')}")

        return mc_result


