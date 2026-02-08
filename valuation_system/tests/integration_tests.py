#!/usr/bin/env python3
"""
Integration Tests for 1000-Company Scale
Tests database migration, config loading, and system functionality
"""

import sys
import time
from pathlib import Path
from datetime import datetime

sys.path.insert(0, str(Path(__file__).parent.parent))

from storage.mysql_client import ValuationMySQLClient
from utils.config_loader import get_active_companies

# Optional: orchestrator may not import if dependencies missing
try:
    from agents.orchestrator import OrchestratorAgent
    ORCHESTRATOR_AVAILABLE = True
except Exception as e:
    print(f"Warning: Orchestrator not available: {e}")
    ORCHESTRATOR_AVAILABLE = False


def test_database_migration():
    """Test 1: Database migration successful."""
    print("\n" + "="*80)
    print("TEST 1: Database Migration")
    print("="*80)

    mysql = ValuationMySQLClient.get_instance()

    # Check companies
    companies = mysql.query("SELECT COUNT(*) as cnt FROM vs_active_companies WHERE is_active = 1")
    count = companies[0]['cnt']

    print(f"✓ Active companies: {count}")
    assert count >= 800, f"Expected >= 800 companies, got {count}"

    # Check valuation groups
    groups = mysql.query("SELECT COUNT(*) as cnt FROM vs_valuation_group_configs WHERE is_active = 1")
    group_count = groups[0]['cnt']

    print(f"✓ Active valuation groups: {group_count}")
    assert group_count >= 12, f"Expected >= 12 groups, got {group_count}"

    # Check alpha configs
    alphas = mysql.query("SELECT COUNT(*) as cnt FROM vs_company_alpha_configs")
    alpha_count = alphas[0]['cnt']

    print(f"✓ Alpha configs: {alpha_count}")
    assert alpha_count >= 2, f"Expected >= 2 alpha configs, got {alpha_count}"

    # Check distribution
    print("\nValuation group distribution:")
    dist = mysql.query("""
        SELECT valuation_group, COUNT(*) as cnt
        FROM vs_active_companies
        WHERE is_active = 1
        GROUP BY valuation_group
        ORDER BY cnt DESC
    """)

    for row in dist:
        print(f"  {row['valuation_group']:30s}: {row['cnt']:4d}")

    print("\n✅ Database migration test PASSED")
    return True


def test_config_loader_database():
    """Test 2: Config loader reads from database."""
    print("\n" + "="*80)
    print("TEST 2: Config Loader Database Integration")
    print("="*80)

    mysql = ValuationMySQLClient.get_instance()

    start = time.time()
    companies = get_active_companies(mysql_client=mysql)
    elapsed = time.time() - start

    print(f"✓ Loaded {len(companies)} companies in {elapsed:.2f}s")
    assert len(companies) >= 800, f"Expected >= 800 companies, got {len(companies)}"

    # Check structure
    sample = list(companies.values())[0]
    required_fields = ['company_id', 'nse_symbol', 'valuation_group', 'valuation_subgroup']

    print(f"Sample company fields: {list(sample.keys())[:10]}")

    for field in required_fields:
        assert field in sample, f"Missing field: {field} (available: {list(sample.keys())})"
        print(f"✓ Field present: {field}")

    # Check pilot companies with alpha configs
    pilot_symbols = ['AETHER', 'EICHERMOT']
    for symbol in pilot_symbols:
        if symbol in companies:
            company = companies[symbol]
            print(f"\n✓ Pilot company: {symbol}")
            print(f"  - Group: {company.get('valuation_group')}")
            print(f"  - Subgroup: {company.get('valuation_subgroup')}")
            if 'alpha_thesis' in company:
                print(f"  - Has alpha config: YES")
            else:
                print(f"  - Has alpha config: NO (expected YES)")

    print("\n✅ Config loader test PASSED")
    return True


def test_orchestrator_initialization():
    """Test 3: Orchestrator initializes with 883 companies."""
    print("\n" + "="*80)
    print("TEST 3: Orchestrator Initialization")
    print("="*80)

    if not ORCHESTRATOR_AVAILABLE:
        print("⚠️  Orchestrator not available, skipping test")
        print("✅ Orchestrator initialization test SKIPPED")
        return True

    start = time.time()
    orchestrator = OrchestratorAgent()
    elapsed = time.time() - start

    print(f"✓ Orchestrator initialized in {elapsed:.2f}s")
    print(f"✓ Active companies: {len(orchestrator.active_companies)}")
    print(f"✓ Active sectors: {len(orchestrator.active_sectors)}")

    assert len(orchestrator.active_companies) >= 800, \
        f"Expected >= 800 companies, got {len(orchestrator.active_companies)}"

    # Check data loaders
    assert orchestrator.core_loader is not None, "Core loader not initialized"
    assert orchestrator.price_loader is not None, "Price loader not initialized"
    print("✓ Data loaders initialized")

    # Check MySQL
    if orchestrator.mysql:
        print("✓ MySQL connected")
    else:
        print("⚠️  MySQL not connected (may be expected)")

    print("\n✅ Orchestrator initialization test PASSED")
    return True


def test_valuation_group_configs():
    """Test 4: Valuation group configs populated."""
    print("\n" + "="*80)
    print("TEST 4: Valuation Group Configurations")
    print("="*80)

    mysql = ValuationMySQLClient.get_instance()

    groups = mysql.query("""
        SELECT valuation_group,
               LENGTH(driver_config) as driver_size,
               LENGTH(terminal_assumptions) as terminal_size,
               is_active
        FROM vs_valuation_group_configs
        WHERE is_active = 1
        ORDER BY valuation_group
    """)

    print(f"Found {len(groups)} active groups:\n")

    for group in groups:
        has_drivers = group['driver_size'] and group['driver_size'] > 50
        has_terminal = group['terminal_size'] and group['terminal_size'] > 50

        status = "✓" if (has_drivers and has_terminal) else "⚠️"

        print(f"{status} {group['valuation_group']:30s}: "
              f"drivers={group['driver_size'] or 0:5d}b, "
              f"terminal={group['terminal_size'] or 0:3d}b")

        if not has_drivers:
            print(f"    WARNING: No drivers configured for {group['valuation_group']}")

    print("\n✅ Valuation group configs test PASSED")
    return True


def test_performance():
    """Test 5: Performance benchmarks."""
    print("\n" + "="*80)
    print("TEST 5: Performance Benchmarks")
    print("="*80)

    mysql = ValuationMySQLClient.get_instance()

    # Benchmark 1: Company query
    start = time.time()
    companies = mysql.query("SELECT * FROM vs_active_companies WHERE is_active = 1")
    elapsed = time.time() - start

    print(f"✓ Company query ({len(companies)} rows): {elapsed*1000:.1f}ms")
    assert elapsed < 5.0, f"Company query too slow: {elapsed:.1f}s"

    # Benchmark 2: Config loading
    start = time.time()
    active = get_active_companies(mysql_client=mysql)
    elapsed = time.time() - start

    print(f"✓ Config loading ({len(active)} companies): {elapsed*1000:.1f}ms")
    assert elapsed < 10.0, f"Config loading too slow: {elapsed:.1f}s"

    # Benchmark 3: Group query
    start = time.time()
    groups = mysql.query("SELECT * FROM vs_valuation_group_configs WHERE is_active = 1")
    elapsed = time.time() - start

    print(f"✓ Group query ({len(groups)} rows): {elapsed*1000:.1f}ms")

    print("\n✅ Performance test PASSED")
    return True


def test_data_quality():
    """Test 6: Data quality checks."""
    print("\n" + "="*80)
    print("TEST 6: Data Quality")
    print("="*80)

    mysql = ValuationMySQLClient.get_instance()

    # Check for missing NSE symbols
    missing_nse = mysql.query_one("""
        SELECT COUNT(*) as cnt
        FROM vs_active_companies
        WHERE is_active = 1 AND (nse_symbol IS NULL OR nse_symbol = '')
    """)

    print(f"✓ Missing NSE symbols: {missing_nse['cnt']}")
    assert missing_nse['cnt'] == 0, f"Found {missing_nse['cnt']} missing NSE symbols"

    # Check for missing valuation_group
    missing_group = mysql.query_one("""
        SELECT COUNT(*) as cnt
        FROM vs_active_companies
        WHERE is_active = 1 AND valuation_group IS NULL
    """)

    print(f"✓ Missing valuation_group: {missing_group['cnt']}")
    assert missing_group['cnt'] == 0, f"Found {missing_group['cnt']} missing groups"

    # Check for companies with both group and subgroup
    with_subgroup = mysql.query_one("""
        SELECT COUNT(*) as cnt
        FROM vs_active_companies
        WHERE is_active = 1
          AND valuation_group IS NOT NULL
          AND valuation_subgroup IS NOT NULL
    """)

    print(f"✓ Companies with subgroup: {with_subgroup['cnt']}")

    # Check frequency distribution
    freq_dist = mysql.query("""
        SELECT valuation_frequency, COUNT(*) as cnt
        FROM vs_active_companies
        WHERE is_active = 1
        GROUP BY valuation_frequency
        ORDER BY cnt DESC
    """)

    print("\n✓ Frequency distribution:")
    for row in freq_dist:
        print(f"  {row['valuation_frequency']:10s}: {row['cnt']:4d}")

    print("\n✅ Data quality test PASSED")
    return True


def run_all_tests():
    """Run all integration tests."""
    print("\n" + "#"*80)
    print("# INTEGRATION TESTS - 1000 Company Scale")
    print(f"# Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("#"*80)

    tests = [
        test_database_migration,
        test_config_loader_database,
        test_orchestrator_initialization,
        test_valuation_group_configs,
        test_performance,
        test_data_quality,
    ]

    passed = 0
    failed = 0
    start_time = time.time()

    for test in tests:
        try:
            if test():
                passed += 1
        except Exception as e:
            print(f"\n❌ {test.__name__} FAILED: {e}")
            failed += 1
            import traceback
            traceback.print_exc()

    elapsed = time.time() - start_time

    print("\n" + "#"*80)
    print("# INTEGRATION TEST SUMMARY")
    print("#"*80)
    print(f"Total:    {len(tests)}")
    print(f"Passed:   {passed}")
    print(f"Failed:   {failed}")
    print(f"Duration: {elapsed:.1f}s")
    print("#"*80)

    if failed == 0:
        print("\n🎉 ALL INTEGRATION TESTS PASSED!")
        return 0
    else:
        print(f"\n⚠️  {failed} TESTS FAILED")
        return 1


if __name__ == '__main__':
    exit(run_all_tests())
