"""
Relative Valuation Model
Peer comparison using P/E, P/B, EV/EBITDA, P/S multiples.
Primary data source: combined_monthly_prices.csv (updated daily).
"""

import os
import logging
from typing import Optional

import numpy as np
from dotenv import load_dotenv

logger = logging.getLogger(__name__)

load_dotenv(os.path.join(os.path.dirname(__file__), '..', 'config', '.env'))


class RelativeValuation:
    """
    Relative valuation using peer multiples from combined_monthly_prices.csv.

    Sector-appropriate methods:
      Chemicals: EV/EBITDA primary, P/E secondary
      Automobiles: P/E primary, EV/EBITDA secondary
      Finance: P/B primary, P/E secondary
      IT: P/E primary, EV/EBITDA secondary
    """

    # Multiple weights by sector (how much weight each multiple gets)
    SECTOR_MULTIPLE_WEIGHTS = {
        'Chemicals': {
            'ev_ebitda': 0.45,
            'pe': 0.30,
            'ps': 0.15,
            'pb': 0.10,
        },
        'Automobile & Ancillaries': {
            'pe': 0.40,
            'ev_ebitda': 0.35,
            'pb': 0.15,
            'ps': 0.10,
        },
        'Finance': {
            'pb': 0.50,
            'pe': 0.40,
            'ev_ebitda': 0.00,
            'ps': 0.10,
        },
        'IT': {
            'pe': 0.35,
            'ev_ebitda': 0.35,
            'ps': 0.20,
            'pb': 0.10,
        },
        'Healthcare': {
            'ev_ebitda': 0.40,
            'pe': 0.30,
            'ps': 0.20,
            'pb': 0.10,
        },
        'FMCG': {
            'pe': 0.45,
            'ev_ebitda': 0.35,
            'ps': 0.10,
            'pb': 0.10,
        },
    }

    # Default if sector not mapped
    DEFAULT_MULTIPLE_WEIGHTS = {
        'ev_ebitda': 0.35,
        'pe': 0.35,
        'ps': 0.15,
        'pb': 0.15,
    }

    def __init__(self, price_loader):
        """
        Initialize with a PriceLoader instance (shared data source).
        """
        self.price_loader = price_loader

    def calculate_relative_value(self, company_financials: dict,
                                  peer_multiples: dict,
                                  sector: str,
                                  adjustment_factors: dict = None) -> dict:
        """
        Calculate relative valuation using peer multiples.

        Args:
            company_financials: Dict with pat, pbidt/ebitda, sales, networth,
                                shares_outstanding, net_debt
            peer_multiples: Output from PriceLoader.get_peer_multiples()
            sector: CSV sector name for selecting appropriate weights
            adjustment_factors: Quality adjustments (ROCE premium, growth premium, etc.)

        Returns:
            Dict with implied valuations per multiple and weighted average.
        """
        shares = company_financials.get('shares_outstanding', 1)
        if shares <= 0:
            logger.error("shares_outstanding must be > 0")
            return {'relative_value_per_share': None, 'error': 'invalid shares_outstanding'}

        multiple_weights = self.SECTOR_MULTIPLE_WEIGHTS.get(sector, self.DEFAULT_MULTIPLE_WEIGHTS)

        implied_values = {}
        total_weight = 0.0
        weighted_sum = 0.0

        # P/E based valuation
        pe_value = self._value_from_pe(
            company_financials, peer_multiples, shares
        )
        if pe_value and multiple_weights.get('pe', 0) > 0:
            implied_values['pe'] = pe_value
            w = multiple_weights['pe']
            weighted_sum += pe_value['implied_per_share'] * w
            total_weight += w

        # P/B based valuation
        pb_value = self._value_from_pb(
            company_financials, peer_multiples, shares
        )
        if pb_value and multiple_weights.get('pb', 0) > 0:
            implied_values['pb'] = pb_value
            w = multiple_weights['pb']
            weighted_sum += pb_value['implied_per_share'] * w
            total_weight += w

        # EV/EBITDA based valuation
        ev_ebitda_value = self._value_from_ev_ebitda(
            company_financials, peer_multiples, shares
        )
        if ev_ebitda_value and multiple_weights.get('ev_ebitda', 0) > 0:
            implied_values['ev_ebitda'] = ev_ebitda_value
            w = multiple_weights['ev_ebitda']
            weighted_sum += ev_ebitda_value['implied_per_share'] * w
            total_weight += w

        # P/S based valuation
        ps_value = self._value_from_ps(
            company_financials, peer_multiples, shares
        )
        if ps_value and multiple_weights.get('ps', 0) > 0:
            implied_values['ps'] = ps_value
            w = multiple_weights['ps']
            weighted_sum += ps_value['implied_per_share'] * w
            total_weight += w

        if total_weight == 0:
            logger.warning("No valid multiples for relative valuation")
            return {'relative_value_per_share': None, 'error': 'no valid multiples'}

        base_relative_value = weighted_sum / total_weight

        # Apply quality adjustments
        total_adjustment = 0.0
        if adjustment_factors:
            total_adjustment = sum(adjustment_factors.values())
            logger.debug(f"Quality adjustment: {total_adjustment:+.2%} "
                         f"({adjustment_factors})")

        adjusted_value = base_relative_value * (1 + total_adjustment)

        result = {
            'method': 'RELATIVE',
            'sector': sector,
            'relative_value_per_share': round(adjusted_value, 2),
            'base_value_before_adjustment': round(base_relative_value, 2),
            'quality_adjustment_pct': round(total_adjustment * 100, 2),
            'adjustment_factors': adjustment_factors or {},
            'multiple_weights_used': multiple_weights,
            'implied_values': implied_values,
            'peer_data': {
                'as_of_date': peer_multiples.get('as_of_date'),
                'peer_count': peer_multiples.get('peer_count'),
            }
        }

        logger.info(f"Relative valuation: ₹{adjusted_value:,.2f}/share "
                     f"(base ₹{base_relative_value:,.2f}, adj {total_adjustment:+.1%})")

        return result

    def _value_from_pe(self, financials: dict, peers: dict,
                       shares: float) -> Optional[dict]:
        """Implied equity value from P/E multiple."""
        pe_stats = peers.get('pe', {})
        median_pe = pe_stats.get('median')
        if not median_pe or median_pe <= 0:
            return None

        # Use latest PAT
        pat = financials.get('latest_pat')
        if not pat or pat <= 0:
            return None

        implied_mcap = pat * median_pe
        implied_per_share = implied_mcap / shares

        return {
            'multiple_name': 'P/E',
            'peer_median': round(median_pe, 2),
            'peer_mean': round(pe_stats.get('mean', 0), 2),
            'company_metric': round(pat, 2),
            'metric_label': 'PAT (Rs Cr)',
            'implied_mcap': round(implied_mcap, 2),
            'implied_per_share': round(implied_per_share, 2),
        }

    def _value_from_pb(self, financials: dict, peers: dict,
                       shares: float) -> Optional[dict]:
        """Implied equity value from P/B multiple."""
        pb_stats = peers.get('pb', {})
        median_pb = pb_stats.get('median')
        if not median_pb or median_pb <= 0:
            return None

        networth = financials.get('latest_networth')
        if not networth or networth <= 0:
            return None

        implied_mcap = networth * median_pb
        implied_per_share = implied_mcap / shares

        return {
            'multiple_name': 'P/B',
            'peer_median': round(median_pb, 2),
            'peer_mean': round(pb_stats.get('mean', 0), 2),
            'company_metric': round(networth, 2),
            'metric_label': 'Networth (Rs Cr)',
            'implied_mcap': round(implied_mcap, 2),
            'implied_per_share': round(implied_per_share, 2),
        }

    def _value_from_ev_ebitda(self, financials: dict, peers: dict,
                               shares: float) -> Optional[dict]:
        """Implied equity value from EV/EBITDA multiple."""
        ev_stats = peers.get('ev_ebitda', {})
        median_ev_ebitda = ev_stats.get('median')
        if not median_ev_ebitda or median_ev_ebitda <= 0:
            return None

        ebitda = financials.get('latest_ebitda') or financials.get('latest_pbidt')
        if not ebitda or ebitda <= 0:
            return None

        net_debt = financials.get('net_debt', 0)

        implied_ev = ebitda * median_ev_ebitda
        implied_equity = implied_ev - net_debt
        implied_per_share = implied_equity / shares

        return {
            'multiple_name': 'EV/EBITDA',
            'peer_median': round(median_ev_ebitda, 2),
            'peer_mean': round(ev_stats.get('mean', 0), 2),
            'company_metric': round(ebitda, 2),
            'metric_label': 'EBITDA (Rs Cr)',
            'net_debt_used': round(net_debt, 2),
            'implied_ev': round(implied_ev, 2),
            'implied_equity': round(implied_equity, 2),
            'implied_per_share': round(implied_per_share, 2),
        }

    def _value_from_ps(self, financials: dict, peers: dict,
                       shares: float) -> Optional[dict]:
        """Implied equity value from P/S multiple."""
        ps_stats = peers.get('ps', {})
        median_ps = ps_stats.get('median')
        if not median_ps or median_ps <= 0:
            return None

        sales = financials.get('latest_sales')
        if not sales or sales <= 0:
            return None

        implied_mcap = sales * median_ps
        implied_per_share = implied_mcap / shares

        return {
            'multiple_name': 'P/S',
            'peer_median': round(median_ps, 2),
            'peer_mean': round(ps_stats.get('mean', 0), 2),
            'company_metric': round(sales, 2),
            'metric_label': 'Sales (Rs Cr)',
            'implied_mcap': round(implied_mcap, 2),
            'implied_per_share': round(implied_per_share, 2),
        }

    def calculate_quality_adjustments(self, company_financials: dict,
                                       peer_averages: dict) -> dict:
        """
        Calculate quality premium/discount based on company vs peer metrics.

        Returns adjustment factors that can be passed to calculate_relative_value().
        Each factor is a fraction (e.g., 0.10 = +10% premium, -0.05 = -5% discount).
        """
        adjustments = {}

        # ROCE premium/discount
        company_roce = company_financials.get('latest_roce')
        peer_roce = peer_averages.get('median_roce')
        if company_roce and peer_roce and peer_roce > 0:
            roce_diff = (company_roce - peer_roce) / peer_roce
            # Cap adjustment at ±15%
            adjustments['roce_premium'] = round(max(-0.15, min(0.15, roce_diff * 0.5)), 4)

        # Growth premium/discount (revenue CAGR vs peers)
        company_growth = company_financials.get('revenue_cagr_5y')
        peer_growth = peer_averages.get('median_revenue_cagr')
        if company_growth is not None and peer_growth is not None and peer_growth > 0:
            growth_diff = (company_growth - peer_growth) / max(peer_growth, 0.01)
            adjustments['growth_premium'] = round(max(-0.10, min(0.10, growth_diff * 0.3)), 4)

        # Governance discount (promoter pledge, related party)
        pledge_pct = company_financials.get('promoter_pledge_pct', 0)
        if pledge_pct and pledge_pct > 5:
            adjustments['governance_discount'] = round(-min(pledge_pct / 100, 0.10), 4)

        # Balance sheet premium (low debt)
        debt_to_equity = company_financials.get('debt_to_equity', 0)
        if debt_to_equity is not None and debt_to_equity < 0.3:
            adjustments['balance_sheet_premium'] = 0.03
        elif debt_to_equity is not None and debt_to_equity > 1.5:
            adjustments['balance_sheet_discount'] = -0.05

        logger.debug(f"Quality adjustments: {adjustments}")
        return adjustments

    def get_historical_band(self, symbol: str, periods: int = 60) -> dict:
        """
        Calculate historical valuation bands (P/E, P/B, EV/EBITDA).
        Useful for mean-reversion based relative valuation.

        Returns current multiple position relative to historical range.
        """
        hist_df = self.price_loader.get_historical_multiples(symbol, periods)

        if hist_df.empty:
            logger.warning(f"No historical multiples for {symbol}")
            return {}

        bands = {}
        for col, label in [('pe', 'P/E'), ('pb', 'P/B'), ('evebidta', 'EV/EBITDA'), ('ps', 'P/S')]:
            if col not in hist_df.columns:
                continue
            series = hist_df[col].dropna()
            positive = series[series > 0]
            if positive.empty:
                continue

            current = float(positive.iloc[0]) if len(positive) > 0 else None
            bands[label] = {
                'current': round(current, 2) if current else None,
                'median': round(float(positive.median()), 2),
                'mean': round(float(positive.mean()), 2),
                'min': round(float(positive.min()), 2),
                'max': round(float(positive.max()), 2),
                'p25': round(float(positive.quantile(0.25)), 2),
                'p75': round(float(positive.quantile(0.75)), 2),
                'std': round(float(positive.std()), 2),
                'data_points': len(positive),
            }

            if current and bands[label]['median']:
                # Position relative to historical range
                range_width = bands[label]['max'] - bands[label]['min']
                if range_width > 0:
                    percentile = (current - bands[label]['min']) / range_width
                    bands[label]['current_percentile'] = round(percentile * 100, 1)

                # Premium/discount to historical median
                bands[label]['premium_to_median_pct'] = round(
                    (current / bands[label]['median'] - 1) * 100, 1
                )

        return bands

    def generate_relative_report(self, symbol: str, company_financials: dict,
                                  sector: str, adjustment_factors: dict = None) -> dict:
        """
        Full relative valuation report combining:
        1. Current peer multiples comparison
        2. Historical valuation bands
        3. Quality-adjusted fair value
        """
        csv_sector = sector

        # Get peer multiples
        peer_multiples = self.price_loader.get_peer_multiples(csv_sector)
        if not peer_multiples:
            return {'error': f'No peer multiples for sector {csv_sector}'}

        # Get historical bands
        bands = self.get_historical_band(symbol)

        # Calculate relative value
        rel_value = self.calculate_relative_value(
            company_financials, peer_multiples, csv_sector, adjustment_factors
        )

        # Get company's own current multiples
        company_data = self.price_loader.get_latest_data(symbol)

        return {
            'symbol': symbol,
            'sector': csv_sector,
            'relative_valuation': rel_value,
            'historical_bands': bands,
            'company_current_multiples': {
                'pe': company_data.get('pe'),
                'pb': company_data.get('pb'),
                'ev_ebitda': company_data.get('ev_ebitda'),
                'ps': company_data.get('ps'),
                'mcap_cr': company_data.get('mcap_cr'),
            },
            'peer_summary': {
                'as_of_date': peer_multiples.get('as_of_date'),
                'peer_count': peer_multiples.get('peer_count'),
                'pe_median': peer_multiples.get('pe', {}).get('median'),
                'pb_median': peer_multiples.get('pb', {}).get('median'),
                'ev_ebitda_median': peer_multiples.get('ev_ebitda', {}).get('median'),
                'ps_median': peer_multiples.get('ps', {}).get('median'),
                'peer_list': peer_multiples.get('peer_list', []),
            },
        }
