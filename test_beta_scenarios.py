#!/usr/bin/env python3
"""
Test script to demonstrate Week 1 beta scenario implementation.
Shows how LEMONTREE's valuation improves with correct beta selection.
"""

import sys
import os
sys.path.insert(0, os.path.dirname(__file__))

from valuation_system.data.loaders.core_loader import CoreDataLoader
from valuation_system.data.loaders.damodaran_loader import DamodaranLoader

def test_beta_scenarios():
    """Test beta scenarios for LEMONTREE and other problem companies."""

    print("=" * 80)
    print("WEEK 1 BETA SCENARIO IMPLEMENTATION TEST")
    print("=" * 80)
    print()

    # Initialize loaders
    dam = DamodaranLoader()

    # Test companies with their parameters
    test_companies = [
        {
            'symbol': 'LEMONTREE',
            'name': 'Lemon Tree Hotels Ltd',
            'group': 'SERVICES',
            'subgroup': 'SERVICES_HOSPITALITY',
            'de_ratio': 1.46,
            'tax_rate': 0.151,
            'current_dcf': 13.19,
            'cmp': 126.21,
            'issue': 'Beta 2.42 too high (subgroup aggregate with individual D/E)'
        },
        {
            'symbol': 'DIXON',
            'name': 'Dixon Technologies India Ltd',
            'group': 'CONSUMER_DISCRETIONARY',
            'subgroup': 'CONSUMER_DURABLES_CONSUMER_ELECTRONICS',
            'de_ratio': 0.05,
            'tax_rate': 0.25,
            'current_dcf': 3775,
            'cmp': 11502,
            'issue': 'Terminal ROCE suppressed + extreme TV%'
        },
        {
            'symbol': 'ICICIBANK',
            'name': 'ICICI Bank Ltd',
            'group': 'FINANCIALS',
            'subgroup': 'FINANCIALS_BANKING_PRIVATE',
            'de_ratio': 4.20,
            'tax_rate': 0.25,
            'current_dcf': 818,
            'cmp': 1406,
            'issue': 'Banking methodology gap + beta too high'
        },
        {
            'symbol': 'SBIN',
            'name': 'State Bank of India',
            'group': 'FINANCIALS',
            'subgroup': 'FINANCIALS_BANKING_PSU',
            'de_ratio': 5.10,
            'tax_rate': 0.25,
            'current_dcf': 1179,
            'cmp': 1066,
            'issue': 'Terminal ROCE 60/40 blend too aggressive'
        }
    ]

    for comp in test_companies:
        print(f"\n{'=' * 80}")
        print(f"Company: {comp['name']} ({comp['symbol']})")
        print(f"Subgroup: {comp['subgroup']}")
        print(f"Current Issue: {comp['issue']}")
        print(f"Current DCF: ₹{comp['current_dcf']:.2f} vs CMP: ₹{comp['cmp']:.2f} "
              f"({((comp['current_dcf']/comp['cmp'])-1)*100:+.1f}%)")
        print(f"{'=' * 80}")

        # Get beta scenarios
        scenarios = dam.get_all_beta_scenarios(
            valuation_group=comp['group'],
            valuation_subgroup=comp['subgroup'],
            company_symbol=comp['symbol'],
            de_ratio=comp['de_ratio'],
            tax_rate=comp['tax_rate']
        )

        if not scenarios:
            print(f"  ⚠️  No beta scenarios found")
            continue

        # Calculate estimated intrinsic value for each scenario
        # Using simplified WACC impact: Intrinsic ∝ 1/WACC (rough approximation)
        rf = 0.0662  # 6.62% India 10Y
        erp = 0.0708  # 7.08% equity risk premium

        print(f"\nBeta Scenarios:")
        print(f"{'Scenario':<25} {'Beta':<8} {'Ke':<8} {'WACC':<8} {'Est. Impact':<15}")
        print(f"{'-'*80}")

        base_wacc = None
        for key, data in scenarios.items():
            beta = data['levered_beta']
            ke = rf + beta * erp

            # Simplified WACC (assuming cost of debt ~11%, tax ~15-25%)
            e_weight = 1 / (1 + comp['de_ratio'])
            d_weight = comp['de_ratio'] / (1 + comp['de_ratio'])
            kd = 0.11
            wacc = e_weight * ke + d_weight * kd * (1 - comp['tax_rate'])

            # Store base WACC for comparison
            if key == 'subgroup_aggregate':
                base_wacc = wacc

            # Estimate impact on intrinsic value
            if base_wacc:
                # Intrinsic value inversely proportional to WACC
                wacc_impact = (base_wacc / wacc - 1) * 100 if wacc > 0 else 0
                impact_str = f"{wacc_impact:+.1f}%"
            else:
                impact_str = "baseline"

            scenario_name = {
                'individual_weekly': 'A: Individual (Weekly)',
                'damodaran_india': 'B: Damodaran India',
                'subgroup_aggregate': 'C: Subgroup Aggregate'
            }.get(key, key)

            print(f"{scenario_name:<25} {beta:<8.3f} {ke*100:<7.2f}% {wacc*100:<7.2f}% {impact_str:<15}")
            print(f"  Source: {data['source']}")

        # Recommendation
        print(f"\n📊 Recommendation:")
        if 'individual_weekly' in scenarios:
            print(f"  ✓ Use Scenario A (Individual Weekly) - most accurate, company-specific")
            beta_a = scenarios['individual_weekly']['levered_beta']
            wacc_a = e_weight * (rf + beta_a * erp) + d_weight * kd * (1 - comp['tax_rate'])
            improvement = (base_wacc / wacc_a - 1) * 100
            new_dcf = comp['current_dcf'] * (1 + improvement/100)
            print(f"  📈 Estimated new DCF: ₹{new_dcf:.2f} "
                  f"({((new_dcf/comp['cmp'])-1)*100:+.1f}% vs CMP)")
        elif 'damodaran_india' in scenarios:
            print(f"  ✓ Use Scenario B (Damodaran India) - professional industry estimate")
        else:
            print(f"  ⚠️  Only Scenario C available - use with caution (peer average)")

    print(f"\n{'=' * 80}")
    print("Summary:")
    print("  • Scenario A (Individual) most accurate when available")
    print("  • Scenario B (Damodaran India) for newly listed or volatile companies")
    print("  • Scenario C (Subgroup Aggregate) only as fallback")
    print(f"{'=' * 80}\n")

if __name__ == '__main__':
    test_beta_scenarios()
