#!/usr/bin/env python3
"""
Sync Group Configurations and Seed Drivers to Database

Functions:
- sync_valuation_groups(): Sync group configs to vs_valuation_group_configs
- seed_group_drivers(): Seed GROUP-level drivers into vs_drivers table
"""

import sys
import yaml
import json
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from storage.mysql_client import ValuationMySQLClient


def load_sectors_yaml():
    """Load sectors.yaml configuration."""
    yaml_path = Path(__file__).parent.parent / 'config' / 'sectors.yaml'
    with open(yaml_path, 'r') as f:
        config = yaml.safe_load(f)
    return config


def sync_valuation_groups(mysql_client: ValuationMySQLClient, dry_run: bool = False):
    """
    Sync valuation_groups from YAML to vs_valuation_group_configs table.

    Args:
        mysql_client: MySQL client instance
        dry_run: If True, print what would be synced without modifying DB
    """
    config = load_sectors_yaml()
    valuation_groups = config.get('valuation_groups', {})
    sectors = config.get('sectors', {})
    sectors_extended = config.get('sectors_extended', {})

    # Merge sectors and sectors_extended
    all_sectors = {**sectors, **sectors_extended}

    print(f"\n{'='*80}")
    print(f"Syncing Valuation Group Configurations")
    print(f"Mode: {'DRY RUN' if dry_run else 'EXECUTE'}")
    print(f"{'='*80}\n")

    synced = 0
    errors = []

    for group_name, group_config in valuation_groups.items():
        try:
            # Get primary sector config
            primary_sector_key = group_config.get('primary_sector')
            primary_sector = all_sectors.get(primary_sector_key, {})

            # Build driver_config by merging demand, cost, regulatory drivers
            driver_config = {
                'demand_drivers': primary_sector.get('demand_drivers', []),
                'cost_drivers': primary_sector.get('cost_drivers', []),
                'regulatory_drivers': primary_sector.get('regulatory_drivers', []),
            }

            # Porter's forces (if available in primary sector)
            porter_forces = primary_sector.get('porter_forces', {})

            # Terminal assumptions from group config (overrides sector defaults)
            terminal_assumptions = group_config.get('terminal_assumptions',
                                                   primary_sector.get('terminal_assumptions', {}))

            # Valuation methods
            valuation_methods = primary_sector.get('valuation_methods', {})

            # Build complete config
            full_config = {
                'description': group_config.get('description', ''),
                'primary_sector': primary_sector_key,
                'subgroups': group_config.get('subgroups', {}),
                'valuation_methods': valuation_methods,
                'key_metrics': primary_sector.get('key_metrics', []),
            }

            data = {
                'valuation_group': group_name,
                'driver_config': json.dumps(driver_config),
                'porter_forces': json.dumps(porter_forces) if porter_forces else None,
                'terminal_assumptions': json.dumps(terminal_assumptions),
                'is_active': True,
                'notes': json.dumps(full_config)
            }

            if dry_run:
                print(f"Would sync: {group_name}")
                print(f"  Primary sector: {primary_sector_key}")
                print(f"  Subgroups: {len(group_config.get('subgroups', {}))}")
                print(f"  Demand drivers: {len(driver_config.get('demand_drivers', []))}")
                print(f"  Terminal margin: {terminal_assumptions.get('margin_range', 'N/A')}")
                print()
            else:
                # Update existing or skip (already seeded in schema_updates.sql)
                mysql_client.execute(
                    """UPDATE vs_valuation_group_configs SET
                       driver_config = %s,
                       porter_forces = %s,
                       terminal_assumptions = %s,
                       notes = %s,
                       updated_at = NOW()
                       WHERE valuation_group = %s""",
                    (data['driver_config'], data['porter_forces'],
                     data['terminal_assumptions'], data['notes'], group_name)
                )
                synced += 1
                print(f"✓ Synced {group_name}")

        except Exception as e:
            errors.append(f"Error syncing {group_name}: {e}")
            print(f"✗ Error: {group_name} - {e}")

    print(f"\n{'='*80}")
    print(f"Summary: {synced} groups synced")
    if errors:
        print(f"Errors: {len(errors)}")
        for err in errors:
            print(f"  - {err}")
    print(f"{'='*80}\n")

    return {'synced': synced, 'errors': errors}


def seed_subgroup_drivers(mysql_client: ValuationMySQLClient, dry_run: bool = False):
    """
    Seed SUBGROUP-level drivers into vs_drivers table from sectors.yaml.

    Sector configs (industrials_defense, specialty_chemicals, etc.) contain
    SUBGROUP-specific drivers. These map to valuation_subgroup level.
    """
    config = load_sectors_yaml()
    sectors = config.get('sectors', {})

    print(f"\n{'='*80}")
    print(f"Seeding SUBGROUP-Level Drivers to vs_drivers")
    print(f"Mode: {'DRY RUN' if dry_run else 'EXECUTE'}")
    print(f"{'='*80}\n")

    seeded = 0
    skipped = 0
    errors = []

    # Map YAML config keys to (valuation_group, valuation_subgroup)
    # MUST match actual values in vs_active_companies table!
    config_key_to_subgroup = {
        # INDUSTRIALS
        'industrials_defense': ('INDUSTRIALS', 'INDUSTRIALS_DEFENSE'),

        # AUTO (not AUTOMOBILES)
        'automobiles': ('AUTO', 'AUTO_OEM'),
        'auto_ancillary': ('AUTO', 'AUTO_ANCILLARY'),

        # CONSUMER_STAPLES
        'fmcg': ('CONSUMER_STAPLES', 'CONSUMER_FMCG'),
        'consumer_food_beverage': ('CONSUMER_STAPLES', 'CONSUMER_FOOD_BEVERAGE'),

        # CONSUMER_DISCRETIONARY
        'consumer_durables': ('CONSUMER_DISCRETIONARY', 'CONSUMER_DURABLES'),

        # HEALTHCARE - note: PHARMA_EXPORT not just PHARMA
        'pharma': ('HEALTHCARE', 'HEALTHCARE_PHARMA_EXPORT'),
        'healthcare_hospitals': ('HEALTHCARE', 'HEALTHCARE_HOSPITALS_EQUIPMENT'),

        # TECHNOLOGY (not IT_SERVICES)
        'it_services': ('TECHNOLOGY', 'TECHNOLOGY_IT_SERVICES'),

        # MATERIALS_CHEMICALS
        'specialty_chemicals': ('MATERIALS_CHEMICALS', 'CHEMICALS_SPECIALTY'),

        # MATERIALS_METALS
        'metals_steel': ('MATERIALS_METALS', 'METALS_STEEL'),
        'metals_non_ferrous': ('MATERIALS_METALS', 'METALS_NON_FERROUS'),

        # FINANCIALS - match exact subgroup names from DB
        'financials_banking_private': ('FINANCIALS', 'FINANCIALS_BANKING_PRIVATE'),
        'financials_banking_psu': ('FINANCIALS', 'FINANCIALS_BANKING_PSU'),
        'financials_nbfc_diversified': ('FINANCIALS', 'FINANCIALS_NBFC_DIVERSIFIED'),
        'financials_nbfc_housing': ('FINANCIALS', 'FINANCIALS_NBFC_HOUSING'),
        'financials_nbfc_vehicle': ('FINANCIALS', 'FINANCIALS_NBFC_VEHICLE'),
        'financials_asset_mgmt': ('FINANCIALS', 'FINANCIALS_ASSET_MGMT'),

        # REAL_ESTATE_INFRA (not REAL_ESTATE or INFRASTRUCTURE)
        'realty_residential': ('REAL_ESTATE_INFRA', 'REALTY_RESIDENTIAL'),
        'infra_logistics': ('REAL_ESTATE_INFRA', 'INFRA_LOGISTICS_PORTS'),
        'infra_construction': ('REAL_ESTATE_INFRA', 'INFRA_CONSTRUCTION'),

        # ENERGY_UTILITIES
        'energy_power': ('ENERGY_UTILITIES', 'ENERGY_POWER_GENERATION'),
        'energy_oil_gas': ('ENERGY_UTILITIES', 'ENERGY_OIL_GAS'),

        # SERVICES
        'services_hospitality': ('SERVICES', 'SERVICES_HOSPITALITY'),
        'services_media': ('SERVICES', 'SERVICES_MEDIA_ENTERTAINMENT'),
        'services_telecom': ('SERVICES', 'SERVICES_TELECOM'),
    }

    for config_key, group_config in sectors.items():
        if not group_config.get('is_active', True):
            continue

        mapping = config_key_to_subgroup.get(config_key)
        if not mapping:
            continue

        valuation_group, valuation_subgroup = mapping

        for category in ['demand_drivers', 'cost_drivers', 'regulatory_drivers',
                         'group_specific_drivers']:
            drivers = group_config.get(category, [])

            for driver in drivers:
                driver_name = driver.get('name', '')
                weight = driver.get('weight', 0)

                if not driver_name:
                    continue

                try:
                    existing = mysql_client.query_one(
                        """SELECT id FROM vs_drivers
                           WHERE driver_level = 'SUBGROUP'
                           AND driver_name = %s
                           AND valuation_subgroup = %s""",
                        (driver_name, valuation_subgroup)
                    )

                    if existing:
                        skipped += 1
                        continue

                    if dry_run:
                        print(f"  Would seed: {valuation_subgroup}/{driver_name} (weight={weight})")
                    else:
                        mysql_client.insert('vs_drivers', {
                            'driver_level': 'SUBGROUP',
                            'driver_category': category.replace('_drivers', '').upper(),
                            'driver_name': driver_name,
                            'valuation_group': valuation_group,
                            'valuation_subgroup': valuation_subgroup,
                            'current_value': 'NEUTRAL',
                            'weight': weight,
                            'impact_direction': 'NEUTRAL',
                            'trend': 'STABLE',
                            'updated_by': 'SEED_SUBGROUP'
                        })
                        seeded += 1

                except Exception as e:
                    errors.append(f"Error seeding {driver_name}: {e}")

    if not dry_run:
        print(f"\n✓ Seeded {seeded} new SUBGROUP drivers")
        print(f"  Skipped {skipped} existing drivers")
    else:
        print(f"\nWould seed drivers (new ones only)")

    if errors:
        print(f"\nErrors: {len(errors)}")
        for err in errors[:5]:
            print(f"  - {err}")

    return {'seeded': seeded, 'skipped': skipped, 'errors': errors}


def seed_group_drivers(mysql_client: ValuationMySQLClient, dry_run: bool = False):
    """
    Seed GROUP-level drivers - broader drivers that apply to entire valuation_group.

    These are macro-like drivers for the sector, not subgroup-specific.
    Example: For INDUSTRIALS group - manufacturing_pmi, capex_cycle, infrastructure_spend
    """
    print(f"\n{'='*80}")
    print(f"Seeding GROUP-Level Drivers (broad sector drivers)")
    print(f"Mode: {'DRY RUN' if dry_run else 'EXECUTE'}")
    print(f"{'='*80}\n")

    # GROUP-level drivers are broader and apply to all subgroups
    # MUST match actual valuation_group values from vs_active_companies!
    group_drivers = {
        'INDUSTRIALS': [
            {'name': 'manufacturing_pmi', 'weight': 0.15, 'category': 'DEMAND'},
            {'name': 'infrastructure_investment', 'weight': 0.12, 'category': 'DEMAND'},
            {'name': 'capex_cycle', 'weight': 0.10, 'category': 'DEMAND'},
            {'name': 'industrial_credit_growth', 'weight': 0.08, 'category': 'DEMAND'},
            {'name': 'commodity_prices_index', 'weight': 0.10, 'category': 'COST'},
            {'name': 'power_tariff', 'weight': 0.05, 'category': 'COST'},
            {'name': 'labor_cost_inflation', 'weight': 0.05, 'category': 'COST'},
            {'name': 'ease_of_doing_business', 'weight': 0.05, 'category': 'REGULATORY'},
        ],
        'FINANCIALS': [
            {'name': 'credit_growth', 'weight': 0.15, 'category': 'DEMAND'},
            {'name': 'interest_rate_cycle', 'weight': 0.12, 'category': 'DEMAND'},
            {'name': 'gdp_growth', 'weight': 0.10, 'category': 'DEMAND'},
            {'name': 'credit_cost_cycle', 'weight': 0.10, 'category': 'COST'},
            {'name': 'npa_cycle', 'weight': 0.08, 'category': 'COST'},
            {'name': 'regulatory_capital_norms', 'weight': 0.08, 'category': 'REGULATORY'},
            {'name': 'digital_adoption', 'weight': 0.05, 'category': 'DEMAND'},
        ],
        'MATERIALS_CHEMICALS': [
            {'name': 'global_chemical_demand', 'weight': 0.12, 'category': 'DEMAND'},
            {'name': 'china_plus_one', 'weight': 0.10, 'category': 'DEMAND'},
            {'name': 'crude_oil_prices', 'weight': 0.12, 'category': 'COST'},
            {'name': 'feedstock_availability', 'weight': 0.08, 'category': 'COST'},
            {'name': 'environmental_compliance', 'weight': 0.08, 'category': 'REGULATORY'},
            {'name': 'capacity_additions', 'weight': 0.08, 'category': 'DEMAND'},
        ],
        'MATERIALS_METALS': [
            {'name': 'global_steel_demand', 'weight': 0.12, 'category': 'DEMAND'},
            {'name': 'china_steel_production', 'weight': 0.10, 'category': 'DEMAND'},
            {'name': 'iron_ore_coking_coal', 'weight': 0.12, 'category': 'COST'},
            {'name': 'power_cost', 'weight': 0.08, 'category': 'COST'},
            {'name': 'anti_dumping_duties', 'weight': 0.08, 'category': 'REGULATORY'},
            {'name': 'infrastructure_capex', 'weight': 0.10, 'category': 'DEMAND'},
        ],
        'HEALTHCARE': [
            {'name': 'healthcare_spending', 'weight': 0.12, 'category': 'DEMAND'},
            {'name': 'patent_cliff_opportunity', 'weight': 0.10, 'category': 'DEMAND'},
            {'name': 'us_generics_pricing', 'weight': 0.10, 'category': 'COST'},
            {'name': 'api_prices', 'weight': 0.08, 'category': 'COST'},
            {'name': 'usfda_compliance', 'weight': 0.10, 'category': 'REGULATORY'},
            {'name': 'price_control_nlem', 'weight': 0.08, 'category': 'REGULATORY'},
        ],
        'TECHNOLOGY': [  # was IT_SERVICES - renamed to match actual group
            {'name': 'global_it_spending', 'weight': 0.15, 'category': 'DEMAND'},
            {'name': 'digital_transformation', 'weight': 0.12, 'category': 'DEMAND'},
            {'name': 'usdinr_movement', 'weight': 0.10, 'category': 'COST'},
            {'name': 'talent_availability', 'weight': 0.08, 'category': 'COST'},
            {'name': 'attrition_rate', 'weight': 0.08, 'category': 'COST'},
            {'name': 'ai_automation_impact', 'weight': 0.10, 'category': 'DEMAND'},
        ],
        'CONSUMER_STAPLES': [
            {'name': 'rural_demand', 'weight': 0.12, 'category': 'DEMAND'},
            {'name': 'urban_consumption', 'weight': 0.10, 'category': 'DEMAND'},
            {'name': 'input_cost_inflation', 'weight': 0.10, 'category': 'COST'},
            {'name': 'distribution_reach', 'weight': 0.08, 'category': 'DEMAND'},
            {'name': 'competitive_intensity', 'weight': 0.08, 'category': 'COST'},
        ],
        'CONSUMER_DISCRETIONARY': [
            {'name': 'urban_consumption', 'weight': 0.12, 'category': 'DEMAND'},
            {'name': 'consumer_sentiment', 'weight': 0.10, 'category': 'DEMAND'},
            {'name': 'discretionary_spending', 'weight': 0.10, 'category': 'DEMAND'},
            {'name': 'interest_rates_retail', 'weight': 0.08, 'category': 'DEMAND'},
            {'name': 'raw_material_prices', 'weight': 0.08, 'category': 'COST'},
            {'name': 'competitive_intensity', 'weight': 0.08, 'category': 'COST'},
        ],
        'AUTO': [  # was AUTOMOBILES - renamed to match actual group
            {'name': 'vehicle_demand_cycle', 'weight': 0.12, 'category': 'DEMAND'},
            {'name': 'ev_transition', 'weight': 0.10, 'category': 'DEMAND'},
            {'name': 'commodity_prices', 'weight': 0.10, 'category': 'COST'},
            {'name': 'interest_rates_retail', 'weight': 0.08, 'category': 'DEMAND'},
            {'name': 'emission_norms', 'weight': 0.08, 'category': 'REGULATORY'},
        ],
        'ENERGY_UTILITIES': [
            {'name': 'power_demand_growth', 'weight': 0.12, 'category': 'DEMAND'},
            {'name': 'oil_gas_prices', 'weight': 0.12, 'category': 'COST'},
            {'name': 'renewable_transition', 'weight': 0.10, 'category': 'DEMAND'},
            {'name': 'fuel_cost', 'weight': 0.08, 'category': 'COST'},
            {'name': 'tariff_regulation', 'weight': 0.10, 'category': 'REGULATORY'},
            {'name': 'green_energy_mandate', 'weight': 0.08, 'category': 'REGULATORY'},
        ],
        'REAL_ESTATE_INFRA': [
            {'name': 'real_estate_demand', 'weight': 0.12, 'category': 'DEMAND'},
            {'name': 'interest_rates', 'weight': 0.10, 'category': 'DEMAND'},
            {'name': 'infra_capex', 'weight': 0.12, 'category': 'DEMAND'},
            {'name': 'construction_material_cost', 'weight': 0.10, 'category': 'COST'},
            {'name': 'regulatory_approvals', 'weight': 0.08, 'category': 'REGULATORY'},
            {'name': 'rera_compliance', 'weight': 0.08, 'category': 'REGULATORY'},
        ],
        'SERVICES': [
            {'name': 'gdp_growth', 'weight': 0.12, 'category': 'DEMAND'},
            {'name': 'consumer_spending', 'weight': 0.10, 'category': 'DEMAND'},
            {'name': 'tourism_demand', 'weight': 0.10, 'category': 'DEMAND'},
            {'name': 'digital_adoption', 'weight': 0.08, 'category': 'DEMAND'},
            {'name': 'labor_cost', 'weight': 0.08, 'category': 'COST'},
            {'name': 'regulatory_environment', 'weight': 0.08, 'category': 'REGULATORY'},
        ],
    }

    seeded = 0
    skipped = 0

    for valuation_group, drivers in group_drivers.items():
        for driver in drivers:
            try:
                existing = mysql_client.query_one(
                    """SELECT id FROM vs_drivers
                       WHERE driver_level = 'GROUP'
                       AND driver_name = %s
                       AND valuation_group = %s""",
                    (driver['name'], valuation_group)
                )

                if existing:
                    skipped += 1
                    continue

                if dry_run:
                    print(f"  Would seed: GROUP/{valuation_group}/{driver['name']}")
                else:
                    mysql_client.insert('vs_drivers', {
                        'driver_level': 'GROUP',
                        'driver_category': driver['category'],
                        'driver_name': driver['name'],
                        'valuation_group': valuation_group,
                        'valuation_subgroup': None,
                        'current_value': 'NEUTRAL',
                        'weight': driver['weight'],
                        'impact_direction': 'NEUTRAL',
                        'trend': 'STABLE',
                        'updated_by': 'SEED_GROUP'
                    })
                    seeded += 1
            except Exception as e:
                print(f"  Error: {e}")

    if not dry_run:
        print(f"\n✓ Seeded {seeded} new GROUP drivers")
        print(f"  Skipped {skipped} existing drivers")

    return {'seeded': seeded, 'skipped': skipped}


if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser(description='Sync group configs and seed drivers')
    parser.add_argument('--dry-run', action='store_true', help='Dry run without DB changes')
    parser.add_argument('--seed-group-drivers', action='store_true',
                        help='Seed GROUP-level drivers (broad sector drivers)')
    parser.add_argument('--seed-subgroup-drivers', action='store_true',
                        help='Seed SUBGROUP-level drivers from sector configs')
    parser.add_argument('--seed-all', action='store_true',
                        help='Seed both GROUP and SUBGROUP drivers')
    args = parser.parse_args()

    mysql_client = ValuationMySQLClient.get_instance()

    if args.seed_all:
        seed_group_drivers(mysql_client, dry_run=args.dry_run)
        seed_subgroup_drivers(mysql_client, dry_run=args.dry_run)
    elif args.seed_group_drivers:
        seed_group_drivers(mysql_client, dry_run=args.dry_run)
    elif args.seed_subgroup_drivers:
        seed_subgroup_drivers(mysql_client, dry_run=args.dry_run)
    else:
        sync_valuation_groups(mysql_client, dry_run=args.dry_run)
