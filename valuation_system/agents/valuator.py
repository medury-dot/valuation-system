"""
Valuator Agent
Main valuation engine combining DCF, Relative, and Monte Carlo.
Produces Bull/Base/Bear intrinsic values with full audit trail.

Edge Cases:
- Missing financial data → Use available data, flag gaps, lower confidence
- No peer multiples → Fall back to historical bands
- Monte Carlo timeout → Reduce simulations, still produce result
- Stale price data → Flag staleness, use with warning
"""

import os
import math
import logging
from datetime import datetime, date, timedelta
from typing import Optional

import numpy as np
from dotenv import load_dotenv

from valuation_system.models.dcf_model import DCFInputs, FCFFValuation, ScenarioBuilder, MonteCarloValuation
from valuation_system.models.relative_valuation import RelativeValuation
from valuation_system.data.processors.financial_processor import FinancialProcessor
from valuation_system.utils.config_loader import get_blend_weights, load_sectors_config
from valuation_system.utils.resilience import GracefulDegradation

logger = logging.getLogger(__name__)

load_dotenv(os.path.join(os.path.dirname(__file__), '..', 'config', '.env'))


class ListHandler(logging.Handler):
    """Logging handler that stores log records in a list for later retrieval.
    Used to capture computation logs for the Excel Computation Log tab."""

    def __init__(self):
        super().__init__()
        self.records = []

    def emit(self, record):
        self.records.append(record)

    def get_records(self):
        return list(self.records)

    def clear(self):
        self.records.clear()


class ValuatorAgent:
    """
    Combines all valuation methodologies:
    1. FCFF DCF with WACC (primary)
    2. Relative valuation (peer multiples)
    3. Monte Carlo simulation (probability)
    4. Bull/Base/Bear scenarios

    Blended output: 60% DCF + 30% Relative + 10% Monte Carlo median
    """

    def __init__(self, core_loader, price_loader, damodaran_loader,
                 mysql_client):
        self.core = core_loader
        self.prices = price_loader
        self.damodaran = damodaran_loader
        self.mysql = mysql_client

        self.financial_processor = FinancialProcessor(
            core_loader, price_loader, damodaran_loader
        )
        self.dcf_model = FCFFValuation(
            projection_years=int(os.getenv('DCF_PROJECTION_YEARS', 5))
        )
        self.relative_model = RelativeValuation(price_loader)
        self.scenario_builder = ScenarioBuilder()
        self.monte_carlo = MonteCarloValuation()
        self.degradation = GracefulDegradation()

        self.blend_weights = get_blend_weights()

    def run_full_valuation(self, company_config: dict,
                           sector_outlook: dict,
                           sector_config: dict = None,
                           overrides: dict = None,
                           company_adjustment: dict = None) -> dict:
        """
        Complete valuation pipeline for a company.

        Args:
            company_config: Company config from companies.yaml
            sector_outlook: Output from GroupAnalystAgent.calculate_outlook()
            sector_config: Sector config from sectors.yaml
            overrides: Manual parameter overrides

        Returns:
            Complete valuation result with all methods, scenarios, and audit trail.
        """
        company_name = company_config['csv_name']
        nse_symbol = company_config['nse_symbol']
        csv_sector = sector_outlook.get('sector', '')

        # Load sector config for driver tabs in Excel
        sectors_cfg = load_sectors_config()
        sector_key = company_config.get('sector', '')
        if not sector_config and sector_key:
            sector_config = sectors_cfg.get('sectors', {}).get(sector_key, {})

        logger.info(f"Starting full valuation for {company_name} ({nse_symbol})")

        # Attach ListHandler to capture all computation logs for Excel tab
        log_handler = ListHandler()
        log_handler.setLevel(logging.DEBUG)
        log_handler.setFormatter(logging.Formatter(
            '%(asctime)s | %(name)s | %(levelname)s | %(message)s'
        ))
        root_logger = logging.getLogger('valuation_system')
        root_logger.addHandler(log_handler)

        # Track data quality issues
        warnings = []
        confidence_adjustments = []

        # 1. Get current market price
        price_data = self.prices.get_latest_data(nse_symbol)
        cmp = price_data.get('cmp')
        if not cmp:
            warnings.append("No current market price available")
            cmp = 0

        # Check price staleness
        staleness = self.degradation.check_data_staleness()
        if staleness.get('prices_file', {}).get('is_stale'):
            warnings.append(f"Price data is stale: {staleness['prices_file']['age_hours']:.0f}h old")
            confidence_adjustments.append(-0.10)

        # 2. Build DCF inputs (pass valuation_subgroup for Indian beta lookup)
        valuation_subgroup = company_config.get('valuation_subgroup', '')
        try:
            dcf_input_dict = self.financial_processor.build_dcf_inputs(
                company_name, csv_sector, sector_config, overrides,
                valuation_subgroup=valuation_subgroup
            )
        except ValueError as e:
            logger.error(f"Cannot build DCF inputs for {company_name}: {e}")
            return {'error': str(e), 'company': company_name}

        # Apply sector outlook adjustments
        dcf_input_dict = self._apply_sector_adjustments(dcf_input_dict, sector_outlook)

        # Apply company-level driver adjustments
        if company_adjustment:
            dcf_input_dict = self._apply_company_adjustments(dcf_input_dict, company_adjustment)

        # Convert to DCFInputs dataclass
        dcf_inputs = DCFInputs(**{
            k: v for k, v in dcf_input_dict.items()
            if k in DCFInputs.__dataclass_fields__
        })

        # 3. Run DCF with scenarios
        scenarios = self.scenario_builder.build_scenarios(dcf_inputs, sector_outlook)
        dcf_results = {}
        for scenario_name, scenario_inputs in scenarios.items():
            try:
                dcf_results[scenario_name] = self.dcf_model.calculate_intrinsic_value(scenario_inputs)
            except Exception as e:
                logger.error(f"DCF {scenario_name} failed for {company_name}: {e}", exc_info=True)
                warnings.append(f"DCF {scenario_name} calculation failed: {e}")

        if 'BASE' not in dcf_results:
            logger.error(f"DCF BASE case failed for {company_name}")
            return {'error': 'DCF BASE case failed', 'company': company_name, 'warnings': warnings}

        dcf_base_value = dcf_results['BASE']['intrinsic_per_share']

        # 4. Relative valuation (two-tier peer selection from mssdb)
        relative_value = None
        relative_result = {}
        try:
            relative_inputs = self.financial_processor.build_relative_inputs(company_name)

            # Get sector/industry from mssdb (authoritative classification)
            company_classification = self.mysql.get_company_classification(nse_symbol)
            mssdb_sector = (company_classification or {}).get('sector', '') or csv_sector
            mssdb_industry = (company_classification or {}).get('industry', '')

            # Build two-tier peer group
            peer_group = self._build_peer_group(nse_symbol, mssdb_sector, mssdb_industry)

            if peer_group:
                # Get multiples from prices CSV using peer symbols
                peer_symbols = [p['peer_symbol'] for p in peer_group]
                peer_weights = {
                    p['peer_symbol']: (2.0 if p['tier'] == 'tight' else 1.0)
                    for p in peer_group
                }
                peer_multiples = self.prices.get_peer_multiples_by_symbols(
                    peer_symbols, peer_weights
                )

                # Compute peer averages for quality adjustments (from core CSV)
                peer_averages = self._compute_peer_averages(peer_group)

                if peer_multiples:
                    quality_adj = self.relative_model.calculate_quality_adjustments(
                        relative_inputs, peer_averages
                    )
                    relative_result = self.relative_model.calculate_relative_value(
                        relative_inputs, peer_multiples, mssdb_sector, quality_adj
                    )
                    relative_value = relative_result.get('relative_value_per_share')

                    # Store per-peer multiples for Excel peer table
                    # Enrich with tier, taxonomy, and CD sector/industry
                    raw_peer_list = peer_multiples.get('peer_list', [])
                    peer_tier_map = {
                        p['peer_symbol']: p for p in peer_group
                    }
                    # Batch-lookup taxonomy for all peer symbols
                    peer_symbols_all = [pl.get('nse_symbol', '') for pl in raw_peer_list]
                    taxonomy_map = self._get_peer_taxonomy(peer_symbols_all)

                    for pl in raw_peer_list:
                        sym = pl.get('nse_symbol', '')
                        pg = peer_tier_map.get(sym, {})
                        pl['tier'] = pg.get('tier', 'broad')
                        pl['similarity'] = pg.get('similarity_score', 0)
                        tax = taxonomy_map.get(sym, {})
                        pl['valuation_group'] = tax.get('valuation_group', '')
                        pl['valuation_subgroup'] = tax.get('valuation_subgroup', '')
                        pl['cd_sector'] = tax.get('cd_sector', '')
                        pl['cd_industry'] = tax.get('cd_industry', '')

                    relative_result['peer_multiples_list'] = raw_peer_list

                    # Store peer group for traceability
                    relative_result['peer_group'] = [
                        {'symbol': p['peer_symbol'], 'name': p.get('peer_name', ''),
                         'tier': p['tier'], 'similarity': p.get('similarity_score')}
                        for p in peer_group
                    ]
            else:
                logger.warning(f"No peer group found for {company_name} "
                               f"(sector={mssdb_sector}, industry={mssdb_industry})")
                warnings.append("No peers found for relative valuation")
        except Exception as e:
            logger.error(f"Relative valuation failed for {company_name}: {e}", exc_info=True)
            warnings.append(f"Relative valuation failed: {e}")

        # 5. Monte Carlo
        mc_result = {}
        try:
            mc_result = self.monte_carlo.run_simulation(
                self.dcf_model, dcf_inputs, cmp=cmp
            )
        except Exception as e:
            logger.error(f"Monte Carlo failed for {company_name}: {e}", exc_info=True)
            warnings.append(f"Monte Carlo failed: {e}")

        mc_median = mc_result.get('median')

        # 6. Blend results
        blended_value = self._blend_valuations(dcf_base_value, relative_value, mc_median)

        # 7. Calculate confidence score
        confidence = self._calculate_confidence(
            dcf_results, relative_result, mc_result, warnings, confidence_adjustments
        )

        # 8. Upside/downside
        upside_pct = ((blended_value / cmp) - 1) * 100 if cmp and cmp > 0 else None

        # 9. Sensitivity analysis
        try:
            sensitivity = self.dcf_model.sensitivity_analysis(
                dcf_inputs,
                wacc_range=(-0.04, 0.04, 0.005),
                growth_range=(-0.03, 0.03, 0.005),
            )
        except Exception:
            sensitivity = {}

        result = {
            'company_name': company_name,
            'nse_symbol': nse_symbol,
            'valuation_date': date.today().isoformat(),
            'sector': csv_sector,

            # Market data
            'cmp': cmp,
            'mcap_cr': price_data.get('mcap_cr'),
            'price_date': str(price_data.get('date', '')),
            'price_source': price_data.get('source', ''),

            # Blended result
            'intrinsic_value_blended': round(blended_value, 2),
            'upside_pct': round(upside_pct, 2) if upside_pct is not None else None,

            # DCF results by scenario
            'dcf_bull': dcf_results.get('BULL', {}).get('intrinsic_per_share'),
            'dcf_base': dcf_base_value,
            'dcf_bear': dcf_results.get('BEAR', {}).get('intrinsic_per_share'),
            'dcf_details': dcf_results.get('BASE', {}),

            # Relative
            'relative_value': relative_value,
            'relative_details': relative_result,

            # Monte Carlo
            'mc_median': mc_median,
            'mc_mean': mc_result.get('mean'),
            'mc_probability_above_cmp': mc_result.get('probability_above_cmp'),
            'mc_percentiles': mc_result.get('percentiles', {}),

            # Blending
            'blend_weights': self.blend_weights,

            # Sector context
            'sector_outlook': sector_outlook,

            # Quality
            'confidence_score': confidence,
            'warnings': warnings,

            # Sensitivity
            'sensitivity': sensitivity,

            # Key assumptions (for audit)
            'dcf_assumptions': dcf_results.get('BASE', {}).get('assumptions', {}),

            # Model version
            'model_version': 'v1.0.0',

            # Traceability for Excel report
            'revenue_cagr_3y': dcf_input_dict.get('revenue_cagr_3y'),
            'revenue_cagr_5y': dcf_input_dict.get('revenue_cagr_5y'),
            'revenue_yoy_growth': dcf_input_dict.get('revenue_yoy_growth', []),
            'revenue_source': dcf_input_dict.get('revenue_source', ''),
            'ttm_pat': dcf_input_dict.get('ttm_pat'),
            'pat_source': dcf_input_dict.get('pat_source', ''),
            'ttm_pbidt': dcf_input_dict.get('ttm_pbidt'),
            'pbidt_source': dcf_input_dict.get('pbidt_source', ''),
            'capex_components': dcf_input_dict.get('capex_components', []),
            'depr_components': dcf_input_dict.get('depr_components', []),
            'nwc_components': dcf_input_dict.get('nwc_components', []),
            'tax_components': dcf_input_dict.get('tax_components', []),
            'cash_source_detail': dcf_input_dict.get('cash_source_detail', ''),
            'cost_of_debt_components': dcf_input_dict.get('cost_of_debt_components', []),

            # Driver configs for Excel driver tabs
            'sector_drivers_config': sector_config or {},
            'company_alpha_config': company_config,
            'driver_hierarchy': sectors_cfg.get('driver_hierarchy', {}),

            # 4-level hierarchy fields
            'valuation_group': company_config.get('valuation_group', ''),
            'valuation_subgroup': company_config.get('valuation_subgroup', ''),
            'group_outlook': sector_outlook.get('group_score', 0),
            'subgroup_outlook': sector_outlook.get('subgroup_score', 0),
            'group_drivers': sector_outlook.get('group_drivers', {}),
            'subgroup_drivers': sector_outlook.get('subgroup_drivers', {}),

            # Company-level driver adjustments
            'company_adjustment': company_adjustment or {},
            'company_drivers': (company_adjustment or {}).get('company_drivers', {}),
        }

        logger.info(f"Valuation complete for {company_name}: "
                     f"Blended=₹{blended_value:,.2f}, CMP=₹{cmp:,.2f}, "
                     f"Upside={upside_pct:+.1f}%, Confidence={confidence:.2f}" if upside_pct else
                     f"Valuation complete for {company_name}: Blended=₹{blended_value:,.2f}")

        # Detach log handler and store captured records for Excel Computation Log tab
        root_logger.removeHandler(log_handler)
        result['_computation_logs'] = log_handler.get_records()

        return result

    def _apply_sector_adjustments(self, dcf_inputs: dict,
                                   sector_outlook: dict) -> dict:
        """Adjust DCF inputs based on sector outlook."""
        growth_adj = sector_outlook.get('growth_adjustment', 0)
        margin_adj = sector_outlook.get('margin_adjustment', 0)

        if growth_adj != 0:
            adjusted_rates = [
                round(max(0.02, r * (1 + growth_adj)), 4)
                for r in dcf_inputs.get('revenue_growth_rates', [])
            ]
            dcf_inputs['revenue_growth_rates'] = adjusted_rates
            logger.debug(f"Growth adjusted by {growth_adj:+.2%}: {adjusted_rates}")

        if margin_adj != 0:
            dcf_inputs['margin_improvement'] = dcf_inputs.get('margin_improvement', 0) + margin_adj

        return dcf_inputs

    def _apply_company_adjustments(self, dcf_inputs: dict,
                                     company_adj: dict) -> dict:
        """Adjust DCF inputs based on company-level driver scoring.

        Auto-computed drivers → growth + margin adjustments (projection period).
        PM-curated qualitative drivers → terminal ROCE + reinvestment adjustments.
        """
        # Growth adjustment from auto drivers
        c_growth = company_adj.get('growth_adj', 0)
        if c_growth != 0:
            adjusted_rates = [
                round(max(0.02, r * (1 + c_growth)), 4)
                for r in dcf_inputs.get('revenue_growth_rates', [])
            ]
            dcf_inputs['revenue_growth_rates'] = adjusted_rates
            logger.debug(f"Company growth adjusted by {c_growth:+.2%}: {adjusted_rates}")

        # Margin adjustment from auto drivers
        c_margin = company_adj.get('margin_adj', 0)
        if c_margin != 0:
            dcf_inputs['margin_improvement'] = dcf_inputs.get('margin_improvement', 0) + c_margin

        # Terminal ROCE adjustment from PM qualitative drivers
        roce_adj = company_adj.get('terminal_roce_adj', 0)
        if roce_adj != 0:
            dcf_inputs['terminal_roce'] = dcf_inputs.get('terminal_roce', 0.15) + roce_adj
            logger.debug(f"Terminal ROCE adjusted by {roce_adj:+.2%}")

        # Terminal reinvestment adjustment from PM qualitative drivers
        reinv_adj = company_adj.get('terminal_reinv_adj', 0)
        if reinv_adj != 0:
            dcf_inputs['terminal_reinvestment_rate'] = (
                dcf_inputs.get('terminal_reinvestment_rate', 0.30) + reinv_adj)
            logger.debug(f"Terminal reinvestment adjusted by {reinv_adj:+.2%}")

        return dcf_inputs

    def _blend_valuations(self, dcf_value: float,
                          relative_value: Optional[float],
                          mc_median: Optional[float]) -> float:
        """
        Blend valuation methods using configured weights.
        If a method failed, redistribute its weight proportionally.
        """
        values = {}
        weights = {}

        if dcf_value and dcf_value > 0:
            values['dcf'] = dcf_value
            weights['dcf'] = self.blend_weights['dcf']

        if relative_value and relative_value > 0:
            values['relative'] = relative_value
            weights['relative'] = self.blend_weights['relative']

        if mc_median and mc_median > 0:
            values['monte_carlo'] = mc_median
            weights['monte_carlo'] = self.blend_weights['monte_carlo']

        if not values:
            logger.error("No valid valuations to blend")
            return 0.0

        # Normalize weights to sum to 1.0
        total_weight = sum(weights.values())
        normalized = {k: v / total_weight for k, v in weights.items()}

        blended = sum(values[k] * normalized[k] for k in values)

        logger.debug(f"Blended valuation: {values} with weights {normalized} = {blended:.2f}")
        return blended

    def _calculate_confidence(self, dcf_results: dict, relative_result: dict,
                              mc_result: dict, warnings: list,
                              adjustments: list) -> float:
        """
        Calculate confidence score (0.0 to 1.0).

        Higher confidence when:
        - DCF, Relative, and MC are close to each other
        - Low coefficient of variation in MC
        - Few data quality warnings
        - All three methods produced results
        """
        score = 0.70  # Base score

        # Bonus: all methods produced results
        methods_available = sum([
            bool(dcf_results.get('BASE')),
            bool(relative_result.get('relative_value_per_share')),
            bool(mc_result.get('median')),
        ])
        score += methods_available * 0.05  # Up to +0.15

        # Bonus: methods agree (low dispersion)
        values = [
            v for v in [
                dcf_results.get('BASE', {}).get('intrinsic_per_share'),
                relative_result.get('relative_value_per_share'),
                mc_result.get('median'),
            ] if v and v > 0
        ]
        if len(values) >= 2:
            import numpy as np
            cv = np.std(values) / np.mean(values) if np.mean(values) > 0 else 1.0
            if cv < 0.15:
                score += 0.10
            elif cv < 0.30:
                score += 0.05
            else:
                score -= 0.05

        # Bonus: MC has low CV
        mc_cv = mc_result.get('cv', 1.0)
        if mc_cv and mc_cv < 0.20:
            score += 0.05

        # Penalty: warnings
        score -= len(warnings) * 0.03

        # Apply custom adjustments
        for adj in adjustments:
            score += adj

        return round(max(0.10, min(1.00, score)), 2)

    def _get_peer_taxonomy(self, symbols: list) -> dict:
        """Batch-lookup valuation_group, valuation_subgroup, cd_sector, cd_industry for peer symbols."""
        if not symbols:
            return {}
        result = {}
        # Query vs_active_companies + core CSV sector/industry
        placeholders = ','.join(['%s'] * len(symbols))
        rows = self.mysql.query(
            f"""SELECT ms.symbol, ac.valuation_group, ac.valuation_subgroup,
                       ms.sector as cd_sector, ms.industry as cd_industry
                FROM {self.mysql.MARKETSCRIP_TABLE} ms
                LEFT JOIN vs_active_companies ac ON ac.company_id = ms.marketscrip_id
                WHERE ms.symbol IN ({placeholders})
                  AND ms.scrip_type IN {self.mysql.EQUITY_SCRIP_TYPES}""",
            symbols
        )
        for row in (rows or []):
            sym = row.get('symbol', '')
            result[sym] = {
                'valuation_group': row.get('valuation_group') or '',
                'valuation_subgroup': row.get('valuation_subgroup') or '',
                'cd_sector': row.get('cd_sector') or '',
                'cd_industry': row.get('cd_industry') or '',
            }
        return result

    # =========================================================================
    # PEER GROUP SELECTION
    # =========================================================================

    def _build_peer_group(self, nse_symbol: str, sector: str, industry: str) -> list:
        """
        Two-tier algorithmic peer selection:
          Tier 1 (tight): Same industry from mssdb — weight 2x in relative valuation
          Tier 2 (broad): Same sector, different industry, mcap 0.3x-3x — weight 1x

        Cached in vs_peer_groups, refreshed monthly.
        Returns list of dicts with: peer_company_id, peer_symbol, peer_name, tier,
                                     similarity_score, mcap_ratio, valid_until
        """
        # 1. Check cache
        company = self.mysql.get_company_by_symbol(nse_symbol)
        if not company:
            logger.error(f"Company not found in mssdb: {nse_symbol}")
            return []

        company_id = company['id']
        cached = self.mysql.get_cached_peer_group(company_id)
        if cached:
            logger.info(f"Using cached peer group for {nse_symbol}: {len(cached)} peers")
            return cached

        logger.info(f"Building peer group for {nse_symbol} "
                     f"(sector={sector}, industry={industry})")

        # 2. Get target company's MCap from prices CSV
        target_mcap_dict = self.prices.get_mcap_for_symbols([nse_symbol])
        target_mcap = target_mcap_dict.get(nse_symbol, 0)

        # Get target company's financials from core CSV for similarity scoring
        target_financials = self.core.get_financials_by_symbol(nse_symbol)
        target_roe = None
        target_de = None
        if target_financials:
            roe_series = target_financials.get('roe', {})
            target_roe = self.core.get_latest_value(roe_series)
            debt_series = target_financials.get('debt', {})
            nw_series = target_financials.get('networth', {})
            latest_debt = self.core.get_latest_value(debt_series) or 0
            latest_nw = self.core.get_latest_value(nw_series) or 1
            target_de = latest_debt / latest_nw if latest_nw > 0 else 0

        # 3. Tight peers: same industry
        tight_candidates = []
        if industry:
            tight_raw = self.mysql.get_companies_by_industry(industry, exclude_symbol=nse_symbol)
            tight_symbols = [c['nse_symbol'] for c in tight_raw if c.get('nse_symbol')]

            # Get MCaps from prices CSV
            tight_mcaps = self.prices.get_mcap_for_symbols(tight_symbols) if tight_symbols else {}

            for c in tight_raw:
                sym = c.get('nse_symbol')
                if not sym:
                    continue
                peer_mcap = tight_mcaps.get(sym, 0)
                if peer_mcap <= 0:
                    continue  # No price data

                mcap_ratio = peer_mcap / target_mcap if target_mcap > 0 else 0

                # Compute similarity score
                sim = self._compute_similarity(
                    sym, target_mcap, target_roe, target_de, peer_mcap
                )

                tight_candidates.append({
                    'peer_company_id': c['id'],
                    'peer_symbol': sym,
                    'peer_name': c.get('company_name', ''),
                    'tier': 'tight',
                    'similarity_score': round(sim, 4),
                    'mcap_ratio': round(mcap_ratio, 4),
                    'valid_until': (date.today() + timedelta(days=30)).isoformat(),
                })

        # Sort tight peers by similarity, take top 10
        tight_candidates.sort(key=lambda x: x['similarity_score'], reverse=True)
        tight_peers = tight_candidates[:10]

        # 4. Broad peers: same sector, different industry, mcap 0.3x-3x
        broad_candidates = []
        if sector:
            broad_raw = self.mysql.get_companies_by_sector(sector, exclude_industry=industry)
            broad_symbols = [c['nse_symbol'] for c in broad_raw if c.get('nse_symbol')]

            # Get MCaps from prices CSV
            broad_mcaps = self.prices.get_mcap_for_symbols(broad_symbols) if broad_symbols else {}

            for c in broad_raw:
                sym = c.get('nse_symbol')
                if not sym:
                    continue
                peer_mcap = broad_mcaps.get(sym, 0)
                if peer_mcap <= 0:
                    continue

                mcap_ratio = peer_mcap / target_mcap if target_mcap > 0 else 0

                # MCap filter: 0.3x to 3x of target
                if target_mcap > 0 and (mcap_ratio < 0.3 or mcap_ratio > 3.0):
                    continue

                sim = self._compute_similarity(
                    sym, target_mcap, target_roe, target_de, peer_mcap
                )

                broad_candidates.append({
                    'peer_company_id': c['id'],
                    'peer_symbol': sym,
                    'peer_name': c.get('company_name', ''),
                    'tier': 'broad',
                    'similarity_score': round(sim, 4),
                    'mcap_ratio': round(mcap_ratio, 4),
                    'valid_until': (date.today() + timedelta(days=30)).isoformat(),
                })

        # Sort broad peers by similarity, take top 10
        broad_candidates.sort(key=lambda x: x['similarity_score'], reverse=True)
        broad_peers = broad_candidates[:10]

        all_peers = tight_peers + broad_peers

        # 5. Log peer group for traceability
        logger.info(f"Peer group for {nse_symbol} ({industry}):")
        if tight_peers:
            logger.info(f"  Tight peers (same industry, weight=2x): {len(tight_peers)}")
            for p in tight_peers:
                logger.info(f"    {p['peer_symbol']:<15s} MCap ratio={p['mcap_ratio']:.2f}  "
                             f"Sim={p['similarity_score']:.4f}  ({p['peer_name']})")
        if broad_peers:
            logger.info(f"  Broad peers ({sector}, weight=1x): {len(broad_peers)}")
            for p in broad_peers:
                logger.info(f"    {p['peer_symbol']:<15s} MCap ratio={p['mcap_ratio']:.2f}  "
                             f"Sim={p['similarity_score']:.4f}  ({p['peer_name']})")

        if not all_peers:
            logger.warning(f"No peers found for {nse_symbol}")
            return []

        # 6. Cache to vs_peer_groups
        try:
            self.mysql.save_peer_group(company_id, all_peers)
        except Exception as e:
            logger.warning(f"Failed to cache peer group: {e}")

        return all_peers

    def _compute_similarity(self, peer_symbol: str,
                             target_mcap: float, target_roe: float,
                             target_de: float, peer_mcap: float) -> float:
        """
        Compute similarity score (0-1) between target and a peer.
        Uses MCap from prices CSV, ROE/D/E from core CSV.

        Weighted: MCap proximity 40%, ROE proximity 30%, D/E proximity 30%.
        """
        scores = []
        weights = []

        # MCap proximity (40%)
        if target_mcap and target_mcap > 0 and peer_mcap and peer_mcap > 0:
            log_ratio = abs(math.log10(peer_mcap / target_mcap))
            mcap_score = max(0, 1 - log_ratio)  # 1.0 = same size, 0.0 = 10x different
            scores.append(mcap_score)
            weights.append(0.4)

        # ROE proximity (30%) — from core CSV
        peer_financials = self.core.get_financials_by_symbol(peer_symbol)
        peer_roe = None
        peer_de = None
        if peer_financials:
            roe_series = peer_financials.get('roe', {})
            peer_roe = self.core.get_latest_value(roe_series)
            debt_series = peer_financials.get('debt', {})
            nw_series = peer_financials.get('networth', {})
            p_debt = self.core.get_latest_value(debt_series) or 0
            p_nw = self.core.get_latest_value(nw_series) or 1
            peer_de = p_debt / p_nw if p_nw > 0 else 0

        if target_roe is not None and peer_roe is not None:
            roe_diff = abs(peer_roe - target_roe) / max(abs(target_roe), 10)
            roe_score = max(0, 1 - roe_diff)
            scores.append(roe_score)
            weights.append(0.3)

        # D/E proximity (30%) — from core CSV
        if target_de is not None and peer_de is not None:
            de_diff = abs(peer_de - target_de) / max(target_de, 1)
            de_score = max(0, 1 - de_diff)
            scores.append(de_score)
            weights.append(0.3)

        if not scores:
            return 0.5  # Default if no data available

        total_weight = sum(weights)
        return sum(s * w for s, w in zip(scores, weights)) / total_weight

    def _compute_peer_averages(self, peer_group: list) -> dict:
        """
        Compute weighted average metrics from peer group for quality adjustments.
        All financial metrics from core CSV.
        Tight peers weighted 2x, broad peers 1x.
        """
        roce_values = []
        roce_weights = []
        growth_values = []
        growth_weights = []

        for peer in peer_group:
            w = 2.0 if peer['tier'] == 'tight' else 1.0
            financials = self.core.get_financials_by_symbol(peer['peer_symbol'])
            if not financials:
                continue

            # ROCE from core CSV
            roce_series = financials.get('roce', {})
            roce = self.core.get_latest_value(roce_series)
            if roce is not None:
                roce_values.append(roce)
                roce_weights.append(w)

            # Revenue CAGR from core CSV
            sales_annual = financials.get('sales_annual', {})
            cagr = self.core.calculate_cagr(sales_annual, years=5)
            if cagr is not None:
                growth_values.append(cagr)
                growth_weights.append(w)

        result = {}
        if roce_values:
            # Weighted median approximation: use weighted average
            total_w = sum(roce_weights)
            result['median_roce'] = sum(v * w for v, w in zip(roce_values, roce_weights)) / total_w
        if growth_values:
            total_w = sum(growth_weights)
            result['median_revenue_cagr'] = sum(
                v * w for v, w in zip(growth_values, growth_weights)
            ) / total_w

        logger.info(f"Peer averages: ROCE={result.get('median_roce', 'N/A')}, "
                     f"Revenue CAGR={result.get('median_revenue_cagr', 'N/A')}")
        return result

    def store_valuation(self, result: dict) -> Optional[int]:
        """Store valuation result in MySQL.
        company_id = marketscrip_id from mssdb.kbapp_marketscrip."""
        try:
            # Get company from mssdb.kbapp_marketscrip
            company = self.mysql.get_company_by_symbol(result['nse_symbol'])
            if not company:
                logger.error(f"Company not found in mssdb.kbapp_marketscrip: {result['nse_symbol']}")
                return None

            company_id = company['id']  # marketscrip_id

            # Store individual valuation
            val_id = self.mysql.insert('vs_valuations', {
                'company_id': company_id,
                'valuation_date': result['valuation_date'],
                'method': 'BLENDED',
                'scenario': 'BASE',
                'intrinsic_value': result['intrinsic_value_blended'],
                'cmp': result.get('cmp'),
                'upside_pct': result.get('upside_pct'),
                'confidence_score': result.get('confidence_score'),
                'key_assumptions': result.get('dcf_assumptions'),
                'driver_snapshot': result.get('sector_outlook'),
                'model_version': result.get('model_version', 'v1.0.0'),
                'created_by': 'AGENT',
            })

            # Store full snapshot
            self.mysql.store_valuation_snapshot({
                'company_id': company_id,
                'snapshot_date': result['valuation_date'],
                'snapshot_type': 'DAILY',
                'intrinsic_value_dcf': result.get('dcf_base'),
                'intrinsic_value_relative': result.get('relative_value'),
                'intrinsic_value_blended': result['intrinsic_value_blended'],
                'cmp': result.get('cmp'),
                'upside_pct': result.get('upside_pct'),
                'dcf_bull': result.get('dcf_bull'),
                'dcf_base': result.get('dcf_base'),
                'dcf_bear': result.get('dcf_bear'),
                'dcf_assumptions': result.get('dcf_assumptions'),
                'sector_drivers_snapshot': result.get('sector_outlook'),
                'sector_outlook_score': result.get('sector_outlook', {}).get('outlook_score'),
                'sector_outlook_label': result.get('sector_outlook', {}).get('outlook_label'),
                'confidence_score': result.get('confidence_score'),
                'peer_multiples_snapshot': result.get('relative_details', {}).get('peer_data'),
                'model_version': result.get('model_version', 'v1.0.0'),
            })

            logger.info(f"Stored valuation for {result['company_name']}, id={val_id}")
            return val_id

        except Exception as e:
            logger.error(f"Failed to store valuation: {e}", exc_info=True)
            self.degradation.queue_operation({
                'type': 'store_valuation',
                'data': result,
            })
            return None
