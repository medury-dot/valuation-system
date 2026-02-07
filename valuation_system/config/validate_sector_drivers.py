#!/usr/bin/env python3
"""
Validate sector driver configurations
Checks that all driver weights sum to 100% (1.00) for each sector
"""

import yaml
from pathlib import Path


def validate_sector_drivers(yaml_file):
    """Validate that driver weights sum to 100% for each sector"""

    with open(yaml_file, 'r') as f:
        config = yaml.safe_load(f)

    sectors = config.get('sectors', {})
    validation_results = []

    for sector_key, sector_config in sectors.items():
        if not sector_config or sector_config.get('is_active') == False:
            continue

        print(f"\n{'='*80}")
        print(f"Validating: {sector_key.upper()}")
        print(f"{'='*80}")

        # Sum up all driver weights
        total_weight = 0.0
        driver_categories = ['demand_drivers', 'cost_drivers', 'regulatory_drivers']

        category_weights = {}
        for category in driver_categories:
            drivers = sector_config.get(category, [])
            cat_weight = sum(d.get('weight', 0) for d in drivers)
            category_weights[category] = cat_weight
            total_weight += cat_weight

            print(f"\n{category.replace('_', ' ').title()}:")
            for driver in drivers:
                print(f"  - {driver['name']:35s} : {driver['weight']:.2f}")
            print(f"  {'SUBTOTAL':35s} : {cat_weight:.2f}")

        print(f"\n{'TOTAL WEIGHT':37s} : {total_weight:.2f}")

        # Check if total is 100% (allowing 1% tolerance for rounding)
        is_valid = abs(total_weight - 1.0) < 0.01
        status = "✓ VALID" if is_valid else "✗ INVALID"

        if not is_valid:
            diff = total_weight - 1.0
            print(f"\n{'STATUS':37s} : {status} (off by {diff:+.2f})")
        else:
            print(f"\n{'STATUS':37s} : {status}")

        validation_results.append({
            'sector': sector_key,
            'total_weight': total_weight,
            'is_valid': is_valid,
            'category_weights': category_weights
        })

    # Summary
    print(f"\n\n{'='*80}")
    print("VALIDATION SUMMARY")
    print(f"{'='*80}\n")

    for result in validation_results:
        status = "✓" if result['is_valid'] else "✗"
        print(f"{status} {result['sector']:30s} : {result['total_weight']:.2f}")

    all_valid = all(r['is_valid'] for r in validation_results)

    if all_valid:
        print(f"\n✓ All sectors validated successfully!\n")
    else:
        print(f"\n✗ Some sectors have invalid driver weights!\n")

    return all_valid


if __name__ == "__main__":
    script_dir = Path(__file__).parent
    yaml_file = script_dir / "sectors_new_additions.yaml"

    print(f"Validating: {yaml_file}\n")
    validate_sector_drivers(yaml_file)
