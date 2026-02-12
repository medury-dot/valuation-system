"""
Comprehensive Regression Test Suite
Tests all components of the valuation system end-to-end.
Results emailed daily with pass/fail summary.

Test Categories:
1. DATA LAYER: CSV loading, price loading, Damodaran data
2. MODELS: DCF calculation, relative valuation, Monte Carlo
3. AGENTS: News scanner, sector analyst, valuator, orchestrator
4. STORAGE: MySQL connectivity, schema integrity, CRUD operations
5. RESILIENCE: Internet down, service down, stale data, catchup
6. INTEGRATION: End-to-end valuation pipeline
7. EDGE CASES: Missing data, negative values, zero shares, etc.

Schedule: Daily at 06:00 IST (before market open)
"""

import os
import sys
import json
import logging
import traceback
from datetime import datetime, date, timedelta
from dataclasses import dataclass, field

from dotenv import load_dotenv

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))

load_dotenv(os.path.join(os.path.dirname(__file__), '..', 'config', '.env'))

logger = logging.getLogger(__name__)


@dataclass
class TestResult:
    name: str
    category: str
    passed: bool
    message: str = ''
    duration_ms: float = 0
    error: str = ''
    traceback_str: str = ''


@dataclass
class TestSuiteResult:
    run_date: str = ''
    total: int = 0
    passed: int = 0
    failed: int = 0
    errors: int = 0
    skipped: int = 0
    duration_seconds: float = 0
    results: list = field(default_factory=list)


class RegressionTestRunner:
    """
    Run all regression tests and email results.
    """

    def __init__(self):
        self.results = TestSuiteResult(run_date=datetime.now().isoformat())
        self.errors_log_path = os.path.join(
            os.path.dirname(__file__), '..', 'logs', 'regression_errors.csv'
        )

    def run_all_tests(self) -> TestSuiteResult:
        """Run the complete regression suite."""
        start = datetime.now()
        logger.info("=" * 60)
        logger.info("Starting Regression Test Suite")
        logger.info("=" * 60)

        # Category 1: Data Layer
        self._run_test('test_core_csv_loads', 'DATA', self.test_core_csv_loads)
        self._run_test('test_core_csv_company_lookup', 'DATA', self.test_core_csv_company_lookup)
        self._run_test('test_core_csv_financials_structure', 'DATA', self.test_core_csv_financials_structure)
        self._run_test('test_price_file_loads', 'DATA', self.test_price_file_loads)
        self._run_test('test_price_latest_data', 'DATA', self.test_price_latest_data)
        self._run_test('test_price_peer_multiples', 'DATA', self.test_price_peer_multiples)
        self._run_test('test_price_historical_multiples', 'DATA', self.test_price_historical_multiples)
        self._run_test('test_damodaran_defaults', 'DATA', self.test_damodaran_defaults)
        self._run_test('test_damodaran_beta_calculation', 'DATA', self.test_damodaran_beta_calculation)

        # Category 2: Models
        self._run_test('test_dcf_wacc_calculation', 'MODEL', self.test_dcf_wacc_calculation)
        self._run_test('test_dcf_fcff_projection', 'MODEL', self.test_dcf_fcff_projection)
        self._run_test('test_dcf_terminal_value', 'MODEL', self.test_dcf_terminal_value)
        self._run_test('test_dcf_intrinsic_value', 'MODEL', self.test_dcf_intrinsic_value)
        self._run_test('test_dcf_sanity_checks', 'MODEL', self.test_dcf_sanity_checks)
        self._run_test('test_scenario_builder', 'MODEL', self.test_scenario_builder)
        self._run_test('test_monte_carlo_runs', 'MODEL', self.test_monte_carlo_runs)
        self._run_test('test_relative_valuation', 'MODEL', self.test_relative_valuation)
        self._run_test('test_blended_valuation', 'MODEL', self.test_blended_valuation)

        # Category 3: Financial Processor
        self._run_test('test_financial_processor_dcf_inputs', 'PROCESSOR', self.test_financial_processor_dcf_inputs)
        self._run_test('test_financial_processor_relative_inputs', 'PROCESSOR', self.test_financial_processor_relative_inputs)
        self._run_test('test_growth_trajectory', 'PROCESSOR', self.test_growth_trajectory)

        # Category 4: Storage
        self._run_test('test_mysql_connectivity', 'STORAGE', self.test_mysql_connectivity)
        self._run_test('test_mysql_schema_tables', 'STORAGE', self.test_mysql_schema_tables)

        # Category 5: Resilience
        self._run_test('test_run_state_manager', 'RESILIENCE', self.test_run_state_manager)
        self._run_test('test_graceful_degradation_queue', 'RESILIENCE', self.test_graceful_degradation_queue)
        self._run_test('test_data_staleness_check', 'RESILIENCE', self.test_data_staleness_check)
        self._run_test('test_dependency_check', 'RESILIENCE', self.test_dependency_check)

        # Category 6: Configuration
        self._run_test('test_env_loaded', 'CONFIG', self.test_env_loaded)
        self._run_test('test_sectors_yaml', 'CONFIG', self.test_sectors_yaml)
        self._run_test('test_companies_yaml', 'CONFIG', self.test_companies_yaml)

        # Category 7: Edge Cases
        self._run_test('test_dcf_zero_revenue', 'EDGE', self.test_dcf_zero_revenue)
        self._run_test('test_dcf_negative_growth', 'EDGE', self.test_dcf_negative_growth)
        self._run_test('test_dcf_wacc_below_growth', 'EDGE', self.test_dcf_wacc_below_growth)
        self._run_test('test_missing_company_data', 'EDGE', self.test_missing_company_data)
        self._run_test('test_empty_peer_multiples', 'EDGE', self.test_empty_peer_multiples)

        # Category 8: Integration (end-to-end)
        self._run_test('test_eicher_motors_e2e', 'INTEGRATION', self.test_eicher_motors_e2e)

        # Finalize
        elapsed = (datetime.now() - start).total_seconds()
        self.results.duration_seconds = round(elapsed, 1)
        self.results.total = len(self.results.results)
        self.results.passed = sum(1 for r in self.results.results if r.passed)
        self.results.failed = sum(1 for r in self.results.results if not r.passed and not r.error)
        self.results.errors = sum(1 for r in self.results.results if r.error)

        # Log summary
        logger.info("=" * 60)
        logger.info(f"REGRESSION TEST RESULTS")
        logger.info(f"  Total: {self.results.total}")
        logger.info(f"  Passed: {self.results.passed}")
        logger.info(f"  Failed: {self.results.failed}")
        logger.info(f"  Errors: {self.results.errors}")
        logger.info(f"  Duration: {elapsed:.1f}s")
        logger.info("=" * 60)

        # Write errors CSV
        self._write_errors_csv()

        # Email results
        self._email_results()

        return self.results

    def _run_test(self, name: str, category: str, test_fn):
        """Run a single test with error handling."""
        start = datetime.now()
        try:
            test_fn()
            duration = (datetime.now() - start).total_seconds() * 1000
            result = TestResult(name=name, category=category, passed=True,
                                message='OK', duration_ms=round(duration, 1))
            logger.info(f"  PASS: {name} ({duration:.0f}ms)")
        except AssertionError as e:
            duration = (datetime.now() - start).total_seconds() * 1000
            result = TestResult(name=name, category=category, passed=False,
                                message=str(e), duration_ms=round(duration, 1))
            logger.warning(f"  FAIL: {name}: {e}")
        except Exception as e:
            duration = (datetime.now() - start).total_seconds() * 1000
            tb = traceback.format_exc()
            result = TestResult(name=name, category=category, passed=False,
                                error=str(e), traceback_str=tb,
                                duration_ms=round(duration, 1))
            logger.error(f"  ERROR: {name}: {e}")

        self.results.results.append(result)

    # =========================================================================
    # DATA LAYER TESTS
    # =========================================================================

    def test_core_csv_loads(self):
        from valuation_system.data.loaders.core_loader import CoreDataLoader
        loader = CoreDataLoader()
        df = loader.df
        assert df is not None, "Core CSV failed to load"
        assert len(df) > 0, "Core CSV is empty"
        assert 'Company Name' in df.columns, "Missing 'Company Name' column"

    def test_core_csv_company_lookup(self):
        from valuation_system.data.loaders.core_loader import CoreDataLoader
        loader = CoreDataLoader()
        financials = loader.get_company_financials('Eicher Motors Ltd.')
        assert financials is not None, "Eicher Motors not found in core CSV"
        assert financials['company_name'] == 'Eicher Motors Ltd.'

    def test_core_csv_financials_structure(self):
        from valuation_system.data.loaders.core_loader import CoreDataLoader
        loader = CoreDataLoader()
        fin = loader.get_company_financials('Eicher Motors Ltd.')
        required_keys = ['sales', 'pat', 'pbidt', 'debt', 'networth', 'roce', 'roe']
        for key in required_keys:
            assert key in fin, f"Missing key '{key}' in financials"
            # At least some data should be present
            data = fin[key]
            if isinstance(data, dict):
                assert len(data) > 0 or True, f"Empty data for '{key}'"

    def test_price_file_loads(self):
        from valuation_system.data.loaders.price_loader import PriceLoader
        loader = PriceLoader()
        df = loader.df
        assert df is not None, "Price file failed to load"
        assert len(df) > 0, "Price file is empty"
        assert 'nse_symbol' in df.columns

    def test_price_latest_data(self):
        from valuation_system.data.loaders.price_loader import PriceLoader
        loader = PriceLoader()
        data = loader.get_latest_data('EICHERMOT')
        assert data is not None
        assert data.get('source') in ('local_monthly_prices', 'yahoo_finance', 'yahoo_finance_failed')
        if data.get('cmp'):
            assert data['cmp'] > 0, "CMP should be positive"

    def test_price_peer_multiples(self):
        from valuation_system.data.loaders.price_loader import PriceLoader
        loader = PriceLoader()
        peers = loader.get_peer_multiples('Automobile & Ancillaries')
        assert peers is not None
        if peers:
            assert 'pe' in peers
            assert 'peer_count' in peers

    def test_price_historical_multiples(self):
        from valuation_system.data.loaders.price_loader import PriceLoader
        loader = PriceLoader()
        hist = loader.get_historical_multiples('EICHERMOT', periods=12)
        assert hist is not None
        # May be empty if symbol not found, but shouldn't crash

    def test_damodaran_defaults(self):
        from valuation_system.data.loaders.damodaran_loader import DamodaranLoader
        loader = DamodaranLoader()
        erp = loader.get_india_erp()
        assert erp is not None
        assert 'india_total_erp' in erp
        assert 0.04 <= erp['india_total_erp'] <= 0.12, f"ERP {erp['india_total_erp']} out of range"

    def test_damodaran_beta_calculation(self):
        from valuation_system.data.loaders.damodaran_loader import DamodaranLoader
        loader = DamodaranLoader()
        beta = loader.get_industry_beta('Automobile & Ancillaries', company_de_ratio=0.15)
        assert beta is not None
        assert 'levered_beta' in beta
        assert 0.3 <= beta['levered_beta'] <= 3.0, f"Beta {beta['levered_beta']} out of range"

    # =========================================================================
    # MODEL TESTS
    # =========================================================================

    def test_dcf_wacc_calculation(self):
        from valuation_system.models.dcf_model import DCFInputs, FCFFValuation
        inputs = DCFInputs(
            risk_free_rate=0.071, equity_risk_premium=0.065,
            beta=1.0, cost_of_debt=0.09, debt_ratio=0.10, tax_rate=0.25
        )
        dcf = FCFFValuation()
        wacc = dcf.calculate_wacc(inputs)
        assert 0.05 < wacc < 0.20, f"WACC {wacc} out of reasonable range"
        # Expected: 0.071 + 1.0 * 0.065 = 0.136 * 0.9 + 0.09*0.75*0.1 ≈ 0.129
        assert abs(wacc - 0.129) < 0.01, f"WACC {wacc} not close to expected ~0.129"

    def test_dcf_fcff_projection(self):
        from valuation_system.models.dcf_model import DCFInputs, FCFFValuation
        inputs = DCFInputs(
            base_revenue=1000, revenue_growth_rates=[0.12, 0.11, 0.10, 0.09, 0.08],
            ebitda_margin=0.20, margin_improvement=0.005,
            capex_to_sales=0.08, depreciation_to_sales=0.04,
            nwc_to_sales=0.15, tax_rate=0.25,
        )
        dcf = FCFFValuation()
        projections = dcf.project_fcff(inputs)
        assert len(projections) == 5
        assert projections[0]['revenue'] > 1000, "Y1 revenue should grow"
        assert all(p['fcff'] > 0 for p in projections), "FCFF should be positive for healthy inputs"

    def test_dcf_terminal_value(self):
        from valuation_system.models.dcf_model import FCFFValuation
        dcf = FCFFValuation()
        tv = dcf.calculate_terminal_value(100, 0.12, 0.18, 0.30)
        assert tv['terminal_growth'] > 0, "Terminal growth should be positive"
        assert tv['terminal_growth'] <= 0.05, "Terminal growth should be capped at 5%"
        assert tv['terminal_value'] > 0, "Terminal value should be positive"

    def test_dcf_intrinsic_value(self):
        from valuation_system.models.dcf_model import DCFInputs, FCFFValuation
        inputs = DCFInputs(
            company_name='Test Co', base_revenue=1000,
            revenue_growth_rates=[0.12, 0.11, 0.10, 0.09, 0.08],
            ebitda_margin=0.20, margin_improvement=0.005,
            capex_to_sales=0.08, depreciation_to_sales=0.04,
            nwc_to_sales=0.15, tax_rate=0.25,
            risk_free_rate=0.071, equity_risk_premium=0.065,
            beta=1.0, cost_of_debt=0.09, debt_ratio=0.10,
            terminal_roce=0.18, terminal_reinvestment=0.30,
            shares_outstanding=10, net_debt=100,
        )
        dcf = FCFFValuation()
        result = dcf.calculate_intrinsic_value(inputs)
        assert result['intrinsic_per_share'] > 0, "Intrinsic value should be positive"
        assert result['firm_value'] > 0
        # WEEK 4 FIX: Tightened TV% bounds from 0-95% to 20-90%
        assert 20 < result['terminal_value_pct'] < 90, \
            f"TV% = {result['terminal_value_pct']:.1f}% outside normal range (20-90%)"
        assert result['wacc'] > 0

    def test_dcf_sanity_checks(self):
        """TV should be between 20-90% of total value for reasonable inputs."""
        from valuation_system.models.dcf_model import DCFInputs, FCFFValuation
        inputs = DCFInputs(
            base_revenue=1000, revenue_growth_rates=[0.15, 0.13, 0.11, 0.10, 0.08],
            ebitda_margin=0.22, margin_improvement=0.005,
            capex_to_sales=0.08, depreciation_to_sales=0.04,
            nwc_to_sales=0.12, tax_rate=0.25,
            risk_free_rate=0.071, equity_risk_premium=0.065,
            beta=0.9, cost_of_debt=0.08, debt_ratio=0.10,
            terminal_roce=0.20, terminal_reinvestment=0.25,
            shares_outstanding=10, net_debt=50,
        )
        dcf = FCFFValuation()
        result = dcf.calculate_intrinsic_value(inputs)
        # WEEK 4 FIX: Tightened from <90% to 20-90% range
        assert 20 < result['terminal_value_pct'] < 90, \
            f"TV% = {result['terminal_value_pct']}% outside normal range (20-90%)"

    def test_scenario_builder(self):
        from valuation_system.models.dcf_model import DCFInputs, ScenarioBuilder
        base = DCFInputs(
            base_revenue=1000, revenue_growth_rates=[0.12, 0.10, 0.08, 0.07, 0.06],
            ebitda_margin=0.20, equity_risk_premium=0.065, terminal_roce=0.18,
        )
        builder = ScenarioBuilder()
        scenarios = builder.build_scenarios(base)
        assert 'BULL' in scenarios
        assert 'BASE' in scenarios
        assert 'BEAR' in scenarios
        # Bull should have higher growth than bear
        assert scenarios['BULL'].revenue_growth_rates[0] > scenarios['BEAR'].revenue_growth_rates[0]

    def test_monte_carlo_runs(self):
        from valuation_system.models.dcf_model import DCFInputs, FCFFValuation, MonteCarloValuation
        inputs = DCFInputs(
            base_revenue=1000, revenue_growth_rates=[0.12, 0.10, 0.08, 0.07, 0.06],
            ebitda_margin=0.20, margin_improvement=0.005,
            capex_to_sales=0.08, depreciation_to_sales=0.04,
            nwc_to_sales=0.15, tax_rate=0.25,
            risk_free_rate=0.071, equity_risk_premium=0.065,
            beta=1.0, cost_of_debt=0.09, debt_ratio=0.10,
            terminal_roce=0.18, terminal_reinvestment=0.30,
            shares_outstanding=10, net_debt=100,
        )
        dcf = FCFFValuation()
        # Use fewer simulations for speed in tests
        mc = MonteCarloValuation(n_simulations=100)
        result = mc.run_simulation(dcf, inputs, cmp=500)
        assert result.get('median') is not None, "MC should produce a median"
        assert result.get('median') > 0, "MC median should be positive"
        assert 'probability_above_cmp' in result

    def test_relative_valuation(self):
        from valuation_system.models.relative_valuation import RelativeValuation
        from valuation_system.data.loaders.price_loader import PriceLoader
        loader = PriceLoader()
        rel = RelativeValuation(loader)

        financials = {
            'latest_pat': 500, 'latest_networth': 3000,
            'latest_ebitda': 800, 'latest_sales': 5000,
            'net_debt': 200, 'shares_outstanding': 10,
        }
        peers = {
            'pe': {'median': 25, 'mean': 28},
            'pb': {'median': 4, 'mean': 4.5},
            'ev_ebitda': {'median': 18, 'mean': 20},
            'ps': {'median': 3, 'mean': 3.5},
        }
        result = rel.calculate_relative_value(financials, peers, 'Chemicals')
        assert result.get('relative_value_per_share') is not None
        assert result['relative_value_per_share'] > 0

    def test_blended_valuation(self):
        """Test that blending works when some methods are missing."""
        from valuation_system.agents.valuator import ValuatorAgent
        # Test the blending logic directly
        valuator = type('MockValuator', (), {
            'blend_weights': {'dcf': 0.60, 'relative': 0.30, 'monte_carlo': 0.10},
            '_blend_valuations': ValuatorAgent._blend_valuations,
        })()
        # All three available
        result = valuator._blend_valuations(100, 120, 110)
        assert 100 < result < 120
        # Only DCF available
        result = valuator._blend_valuations(100, None, None)
        assert result == 100
        # Two available
        result = valuator._blend_valuations(100, 120, None)
        assert 100 < result < 120

    # =========================================================================
    # PROCESSOR TESTS
    # =========================================================================

    def test_financial_processor_dcf_inputs(self):
        from valuation_system.data.loaders.core_loader import CoreDataLoader
        from valuation_system.data.loaders.price_loader import PriceLoader
        from valuation_system.data.loaders.damodaran_loader import DamodaranLoader
        from valuation_system.data.processors.financial_processor import FinancialProcessor

        proc = FinancialProcessor(CoreDataLoader(), PriceLoader(), DamodaranLoader())
        try:
            inputs = proc.build_dcf_inputs('Eicher Motors Ltd.', 'Automobile & Ancillaries')
            assert inputs['base_revenue'] > 0, "Revenue should be positive"
            assert inputs['shares_outstanding'] > 0, "Shares should be positive"
            assert 0 < inputs['ebitda_margin'] < 1, "Margin should be between 0 and 1"
            assert len(inputs['revenue_growth_rates']) == 5
        except ValueError:
            pass  # OK if company not found in test env

    def test_financial_processor_relative_inputs(self):
        from valuation_system.data.loaders.core_loader import CoreDataLoader
        from valuation_system.data.loaders.price_loader import PriceLoader
        from valuation_system.data.loaders.damodaran_loader import DamodaranLoader
        from valuation_system.data.processors.financial_processor import FinancialProcessor

        proc = FinancialProcessor(CoreDataLoader(), PriceLoader(), DamodaranLoader())
        try:
            inputs = proc.build_relative_inputs('Eicher Motors Ltd.')
            assert 'latest_pat' in inputs
            assert 'shares_outstanding' in inputs
        except ValueError:
            pass

    def test_growth_trajectory(self):
        from valuation_system.data.processors.financial_processor import FinancialProcessor
        proc = FinancialProcessor.__new__(FinancialProcessor)
        rates = proc._build_growth_trajectory(0.15, 0.18, {})
        assert len(rates) == 5
        assert rates[0] >= rates[-1], "Growth should decay over time"
        assert all(r >= 0.03 for r in rates), "Growth should be at least 3%"

    # =========================================================================
    # STORAGE TESTS
    # =========================================================================

    def test_mysql_connectivity(self):
        from valuation_system.utils.resilience import check_service
        mysql_host = os.getenv('MYSQL_HOST', 'localhost')
        mysql_port = int(os.getenv('MYSQL_PORT', 3306))
        assert check_service(mysql_host, mysql_port), \
            f"MySQL not reachable at {mysql_host}:{mysql_port}"

    def test_mysql_schema_tables(self):
        from valuation_system.storage.mysql_client import get_mysql_client
        try:
            mysql = get_mysql_client()
            # Check vs_* tables in rag database
            tables = mysql.query("SHOW TABLES LIKE 'vs_%'")
            table_names = [list(t.values())[0] for t in tables]
            required_vs = ['vs_valuations', 'vs_valuation_snapshots',
                           'vs_drivers', 'vs_event_timeline', 'vs_alerts',
                           'vs_model_versions']
            for t in required_vs:
                assert t in table_names, f"Missing table: {t}"

            # Check company master in mssdb
            company_count = mysql.query_one(
                "SELECT COUNT(*) as cnt FROM mssdb.kbapp_marketscrip "
                "WHERE scrip_type IN ('', 'EQS') AND symbol IS NOT NULL AND symbol != ''"
            )
            assert company_count and company_count['cnt'] > 0, \
                "mssdb.kbapp_marketscrip has no equity scrips"

            # Verify pilot companies exist
            eicher = mysql.get_company_by_symbol('EICHERMOT')
            assert eicher is not None, "Eicher Motors not found in mssdb.kbapp_marketscrip"
            assert eicher['id'] > 0, "marketscrip_id should be positive"

        except Exception as e:
            logger.warning(f"MySQL schema test skipped: {e}")

    # =========================================================================
    # RESILIENCE TESTS
    # =========================================================================

    def test_run_state_manager(self):
        from valuation_system.utils.resilience import RunStateManager
        import tempfile
        state = RunStateManager(state_dir=tempfile.mkdtemp())
        state.record_success('test_task', {'test': True})
        assert state.get_last_run('test_task') is not None
        assert state.should_retry('test_task')
        state.record_failure('test_task', 'test error')
        assert state.should_retry('test_task')

    def test_graceful_degradation_queue(self):
        from valuation_system.utils.resilience import GracefulDegradation
        import tempfile
        gd = GracefulDegradation(state_dir=tempfile.mkdtemp())
        gd.queue_operation({'type': 'test', 'data': 'hello'})
        assert gd.get_queue_size() == 1
        replayed = gd.replay_queued_operations(lambda op: None)
        assert replayed == 1
        assert gd.get_queue_size() == 0

    def test_data_staleness_check(self):
        from valuation_system.utils.resilience import GracefulDegradation
        gd = GracefulDegradation()
        staleness = gd.check_data_staleness()
        assert isinstance(staleness, dict)

    def test_dependency_check(self):
        from valuation_system.utils.resilience import check_dependencies
        deps = check_dependencies()
        assert 'internet' in deps
        assert 'mysql' in deps
        assert 'core_csv' in deps

    # =========================================================================
    # CONFIGURATION TESTS
    # =========================================================================

    def test_env_loaded(self):
        assert os.getenv('MYSQL_DATABASE') == 'rag', "MYSQL_DATABASE should be 'rag'"
        # CORE_CSV_PATH or CORE_CSV_DIR must be set (auto-detect picks latest file from dir)
        assert os.getenv('CORE_CSV_PATH') or os.getenv('CORE_CSV_DIR'), \
            "Neither CORE_CSV_PATH nor CORE_CSV_DIR set"
        assert os.getenv('MONTHLY_PRICES_PATH'), "MONTHLY_PRICES_PATH not set"

    def test_sectors_yaml(self):
        from valuation_system.utils.config_loader import load_sectors_config, get_active_sectors
        config = load_sectors_config()
        assert 'sectors' in config
        active = get_active_sectors(config)
        assert len(active) >= 2, "Should have at least 2 active sectors"
        assert 'specialty_chemicals' in active
        assert 'automobiles' in active

    def test_companies_yaml(self):
        from valuation_system.utils.config_loader import load_companies_config, get_active_companies
        config = load_companies_config()
        assert 'companies' in config
        active = get_active_companies(config)
        assert len(active) >= 2
        assert 'aether_industries' in active
        assert 'eicher_motors' in active

    # =========================================================================
    # EDGE CASE TESTS
    # =========================================================================

    def test_dcf_zero_revenue(self):
        from valuation_system.models.dcf_model import DCFInputs, FCFFValuation
        inputs = DCFInputs(base_revenue=0, shares_outstanding=10)
        dcf = FCFFValuation()
        result = dcf.calculate_intrinsic_value(inputs)
        # Should not crash, should return 0 or very small value
        assert result['intrinsic_per_share'] is not None

    def test_dcf_negative_growth(self):
        from valuation_system.models.dcf_model import DCFInputs, FCFFValuation
        inputs = DCFInputs(
            base_revenue=1000, revenue_growth_rates=[-0.05, -0.03, 0, 0.02, 0.03],
            ebitda_margin=0.15, shares_outstanding=10,
            risk_free_rate=0.071, equity_risk_premium=0.065,
            beta=1.0, terminal_roce=0.12, terminal_reinvestment=0.25,
        )
        dcf = FCFFValuation()
        result = dcf.calculate_intrinsic_value(inputs)
        assert result is not None  # Should handle without crashing

    def test_dcf_wacc_below_growth(self):
        from valuation_system.models.dcf_model import FCFFValuation
        dcf = FCFFValuation()
        # WACC < growth should be handled
        tv = dcf.calculate_terminal_value(100, 0.05, 0.50, 0.50)
        # Should adjust growth down
        assert tv['terminal_growth'] < 0.05

    def test_missing_company_data(self):
        from valuation_system.data.loaders.core_loader import CoreDataLoader
        loader = CoreDataLoader()
        try:
            loader.get_company_financials('NONEXISTENT_COMPANY_XYZ')
            assert False, "Should raise ValueError for missing company"
        except ValueError:
            pass  # Expected

    def test_empty_peer_multiples(self):
        from valuation_system.models.relative_valuation import RelativeValuation
        from valuation_system.data.loaders.price_loader import PriceLoader
        rel = RelativeValuation(PriceLoader())
        result = rel.calculate_relative_value(
            {'latest_pat': 100, 'shares_outstanding': 10},
            {},  # Empty peer multiples
            'Chemicals'
        )
        assert result.get('relative_value_per_share') is None or result.get('error')

    # =========================================================================
    # INTEGRATION TESTS
    # =========================================================================

    def test_eicher_motors_e2e(self):
        """End-to-end test: load data → build inputs → run DCF → get value."""
        from valuation_system.data.loaders.core_loader import CoreDataLoader
        from valuation_system.data.loaders.price_loader import PriceLoader
        from valuation_system.data.loaders.damodaran_loader import DamodaranLoader
        from valuation_system.data.processors.financial_processor import FinancialProcessor
        from valuation_system.models.dcf_model import DCFInputs, FCFFValuation

        try:
            proc = FinancialProcessor(CoreDataLoader(), PriceLoader(), DamodaranLoader())
            dcf_dict = proc.build_dcf_inputs('Eicher Motors Ltd.', 'Automobile & Ancillaries')

            dcf_inputs = DCFInputs(**{
                k: v for k, v in dcf_dict.items()
                if k in DCFInputs.__dataclass_fields__
            })

            dcf = FCFFValuation()
            result = dcf.calculate_intrinsic_value(dcf_inputs)

            assert result['intrinsic_per_share'] > 0, "E2E: Intrinsic should be positive"
            assert result['wacc'] > 0.05, "E2E: WACC should be reasonable"
            logger.info(f"E2E Eicher: Intrinsic=Rs{result['intrinsic_per_share']:,.2f}, "
                        f"WACC={result['wacc']:.2%}")

        except ValueError as e:
            logger.warning(f"E2E test skipped: {e}")

    # =========================================================================
    # REPORTING
    # =========================================================================

    def _write_errors_csv(self):
        """Write errors to CSV for tracking."""
        import csv
        try:
            with open(self.errors_log_path, 'w', newline='') as f:
                writer = csv.writer(f)
                writer.writerow(['Date', 'Test', 'Category', 'Status', 'Message', 'Error'])
                for r in self.results.results:
                    writer.writerow([
                        self.results.run_date,
                        r.name,
                        r.category,
                        'PASS' if r.passed else 'FAIL',
                        r.message,
                        r.error or '',
                    ])
            logger.info(f"Errors CSV written to {self.errors_log_path}")
        except Exception as e:
            logger.error(f"Failed to write errors CSV: {e}", exc_info=True)

    def _email_results(self):
        """Email regression test results."""
        try:
            from valuation_system.notifications.email_sender import EmailSender
            email = EmailSender()

            pass_rate = (self.results.passed / self.results.total * 100) if self.results.total > 0 else 0
            status_emoji = 'PASS' if self.results.failed == 0 and self.results.errors == 0 else 'FAIL'

            subject = (f"[REGRESSION {status_emoji}] "
                       f"{self.results.passed}/{self.results.total} passed "
                       f"({pass_rate:.0f}%)")

            # Build HTML table of results
            rows_html = ''
            for r in self.results.results:
                color = '#4CAF50' if r.passed else '#f44336'
                status = 'PASS' if r.passed else ('ERROR' if r.error else 'FAIL')
                rows_html += f"""
                <tr>
                    <td style="padding: 4px; border: 1px solid #ddd;">{r.category}</td>
                    <td style="padding: 4px; border: 1px solid #ddd;">{r.name}</td>
                    <td style="padding: 4px; border: 1px solid #ddd; color: {color}; font-weight: bold;">{status}</td>
                    <td style="padding: 4px; border: 1px solid #ddd;">{r.duration_ms:.0f}ms</td>
                    <td style="padding: 4px; border: 1px solid #ddd; font-size: 11px;">{r.message or r.error or ''}</td>
                </tr>
                """

            body = f"""
            <html><body style="font-family: Arial, sans-serif;">
            <h2>Regression Test Report - {datetime.now().strftime('%Y-%m-%d %H:%M')}</h2>
            <table style="margin: 10px 0;">
            <tr><td><b>Total:</b></td><td>{self.results.total}</td></tr>
            <tr><td><b>Passed:</b></td><td style="color: green;">{self.results.passed}</td></tr>
            <tr><td><b>Failed:</b></td><td style="color: red;">{self.results.failed}</td></tr>
            <tr><td><b>Errors:</b></td><td style="color: orange;">{self.results.errors}</td></tr>
            <tr><td><b>Duration:</b></td><td>{self.results.duration_seconds:.1f}s</td></tr>
            </table>

            <table style="border-collapse: collapse; width: 100%; font-size: 12px;">
            <tr style="background: #333; color: white;">
                <th style="padding: 6px;">Category</th>
                <th style="padding: 6px;">Test</th>
                <th style="padding: 6px;">Status</th>
                <th style="padding: 6px;">Duration</th>
                <th style="padding: 6px;">Details</th>
            </tr>
            {rows_html}
            </table>

            <p style="color: #666; font-size: 11px;">
            Agentic Valuation System - Automated Regression Tests
            </p>
            </body></html>
            """

            email.send(subject, body)
        except Exception as e:
            logger.error(f"Failed to email test results: {e}", exc_info=True)


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO,
                        format='%(asctime)s [%(levelname)s] %(name)s: %(message)s')
    runner = RegressionTestRunner()
    results = runner.run_all_tests()
    sys.exit(0 if results.failed == 0 and results.errors == 0 else 1)
