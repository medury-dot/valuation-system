#!/usr/bin/env python3
"""
Populate Driver Definitions for All Subgroups
- Creates DEMAND, COST, REGULATORY drivers for each subgroup
- Inserts into vs_drivers MySQL table
- Syncs to Google Sheet
"""

import os
import sys
import mysql.connector
from datetime import datetime

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))

from dotenv import load_dotenv
load_dotenv('/Users/ram/code/research/valuation_system/config/.env')

# ============================================================================
# DRIVER DEFINITIONS BY SUBGROUP
# Each subgroup has: DEMAND (volume/pricing), COST (inputs), REGULATORY drivers
# ============================================================================

SUBGROUP_DRIVERS = {
    # =========== AUTO ===========
    'AUTO_OEM': [
        ('DEMAND', 'industry_volume', 0.12, 'Monthly vehicle sales volume'),
        ('DEMAND', 'rural_demand', 0.08, 'Rural India demand sentiment'),
        ('DEMAND', 'urban_demand', 0.06, 'Urban India demand'),
        ('DEMAND', 'export_growth', 0.06, 'Export market growth'),
        ('DEMAND', 'pricing_power', 0.08, 'Price increase absorption'),
        ('DEMAND', 'premium_mix', 0.06, 'Mix shift to higher variants'),
        ('COST', 'steel_prices', 0.08, 'Steel commodity prices'),
        ('COST', 'aluminum_prices', 0.04, 'Aluminum commodity prices'),
        ('COST', 'semiconductor_supply', 0.04, 'Chip availability'),
        ('COST', 'battery_costs', 0.06, 'EV battery costs'),
        ('REGULATORY', 'emission_norms', 0.04, 'BS-VI and emission compliance'),
        ('REGULATORY', 'fame_subsidies', 0.03, 'EV incentive schemes'),
        ('REGULATORY', 'scrappage_policy', 0.02, 'Vehicle scrappage impact'),
        ('REGULATORY', 'interest_rates', 0.05, 'Auto loan rates'),
    ],
    'AUTO_ANCILLARY_TIRES': [
        ('DEMAND', 'oem_volume', 0.10, 'OEM tire fitment demand'),
        ('DEMAND', 'replacement_demand', 0.15, 'Aftermarket replacement cycle'),
        ('DEMAND', 'export_demand', 0.08, 'Export market volume'),
        ('DEMAND', 'truck_radial_penetration', 0.06, 'TBR adoption rate'),
        ('COST', 'natural_rubber_prices', 0.15, 'Natural rubber commodity'),
        ('COST', 'synthetic_rubber_prices', 0.08, 'Synthetic rubber costs'),
        ('COST', 'crude_oil_prices', 0.06, 'Oil-linked inputs'),
        ('COST', 'carbon_black_prices', 0.04, 'Carbon black costs'),
        ('REGULATORY', 'import_duties', 0.03, 'Anti-dumping duties on imports'),
        ('REGULATORY', 'quality_standards', 0.02, 'BIS certification'),
    ],
    'AUTO_ANCILLARY_BATTERIES': [
        ('DEMAND', 'vehicle_production', 0.12, 'OEM battery fitment'),
        ('DEMAND', 'replacement_cycle', 0.15, 'Aftermarket replacement'),
        ('DEMAND', 'ev_adoption', 0.10, 'EV battery demand'),
        ('DEMAND', 'inverter_demand', 0.06, 'Industrial battery demand'),
        ('COST', 'lead_prices', 0.15, 'Lead commodity prices'),
        ('COST', 'lithium_prices', 0.08, 'Li-ion battery materials'),
        ('COST', 'sulfuric_acid', 0.03, 'Acid prices'),
        ('REGULATORY', 'battery_waste_rules', 0.03, 'EPR compliance'),
        ('REGULATORY', 'localization_norms', 0.04, 'PLI incentives'),
    ],
    'AUTO_ANCILLARY_COMPONENTS': [
        ('DEMAND', 'oem_production', 0.15, 'OEM vehicle production'),
        ('DEMAND', 'content_per_vehicle', 0.10, 'Value per vehicle'),
        ('DEMAND', 'localization', 0.08, 'Import substitution'),
        ('DEMAND', 'export_orders', 0.08, 'Global OEM orders'),
        ('COST', 'steel_prices', 0.10, 'Steel input costs'),
        ('COST', 'aluminum_prices', 0.06, 'Aluminum costs'),
        ('COST', 'labor_costs', 0.04, 'Wage inflation'),
        ('COST', 'freight_costs', 0.03, 'Logistics costs'),
        ('REGULATORY', 'emission_norms', 0.04, 'BS-VI component demand'),
        ('REGULATORY', 'safety_standards', 0.03, 'Crash test requirements'),
    ],

    # =========== CONSUMER DISCRETIONARY ===========
    'CONSUMER_DURABLES_WHITE_GOODS': [
        ('DEMAND', 'housing_completions', 0.12, 'New home completions'),
        ('DEMAND', 'replacement_cycle', 0.10, 'Product replacement'),
        ('DEMAND', 'summer_demand', 0.08, 'AC seasonality'),
        ('DEMAND', 'rural_penetration', 0.08, 'Rural electrification'),
        ('DEMAND', 'premium_mix', 0.06, 'Inverter AC adoption'),
        ('COST', 'steel_prices', 0.08, 'Steel costs'),
        ('COST', 'copper_prices', 0.06, 'Copper for compressors'),
        ('COST', 'refrigerant_costs', 0.04, 'R32 refrigerant'),
        ('REGULATORY', 'energy_ratings', 0.04, 'BEE star ratings'),
        ('REGULATORY', 'import_duties', 0.03, 'Tariffs on components'),
    ],
    'CONSUMER_DURABLES_BROWN_GOODS': [
        ('DEMAND', 'consumer_sentiment', 0.12, 'Discretionary spending'),
        ('DEMAND', 'festival_demand', 0.10, 'Diwali/festive sales'),
        ('DEMAND', 'replacement_cycle', 0.08, 'TV/audio replacement'),
        ('DEMAND', 'content_consumption', 0.06, 'OTT/streaming growth'),
        ('COST', 'display_panel_prices', 0.12, 'LCD/LED panel costs'),
        ('COST', 'semiconductor_prices', 0.08, 'Chip costs'),
        ('COST', 'logistics_costs', 0.04, 'Freight costs'),
        ('REGULATORY', 'import_duties', 0.04, 'Component tariffs'),
        ('REGULATORY', 'pli_incentives', 0.03, 'Manufacturing subsidies'),
    ],
    'CONSUMER_DURABLES_SMALL_APPLIANCES': [
        ('DEMAND', 'kitchen_appliance_penetration', 0.12, 'Mixers/grinders adoption'),
        ('DEMAND', 'replacement_cycle', 0.10, 'Product replacement'),
        ('DEMAND', 'ecommerce_penetration', 0.08, 'Online sales'),
        ('DEMAND', 'rural_electrification', 0.06, 'Power access'),
        ('COST', 'motor_costs', 0.08, 'Electric motor prices'),
        ('COST', 'plastics_prices', 0.06, 'Polymer costs'),
        ('COST', 'copper_prices', 0.05, 'Copper winding costs'),
        ('REGULATORY', 'quality_standards', 0.03, 'BIS compliance'),
    ],
    'CONSUMER_RETAIL_ONLINE': [
        ('DEMAND', 'gmv_growth', 0.15, 'Gross merchandise value'),
        ('DEMAND', 'user_acquisition', 0.10, 'New customer adds'),
        ('DEMAND', 'order_frequency', 0.08, 'Repeat purchase rate'),
        ('DEMAND', 'average_order_value', 0.06, 'Basket size'),
        ('COST', 'customer_acquisition_cost', 0.12, 'CAC trend'),
        ('COST', 'logistics_cost', 0.10, 'Fulfillment costs'),
        ('COST', 'payment_costs', 0.04, 'UPI/card charges'),
        ('REGULATORY', 'fdi_norms', 0.04, 'E-commerce FDI rules'),
        ('REGULATORY', 'data_privacy', 0.02, 'DPDP compliance'),
    ],
    'CONSUMER_RETAIL_OFFLINE': [
        ('DEMAND', 'same_store_sales_growth', 0.15, 'SSSG'),
        ('DEMAND', 'footfall_growth', 0.10, 'Store traffic'),
        ('DEMAND', 'store_expansion', 0.08, 'New store adds'),
        ('DEMAND', 'basket_size', 0.06, 'Average ticket'),
        ('COST', 'rental_costs', 0.10, 'Real estate rentals'),
        ('COST', 'staff_costs', 0.08, 'Employee wages'),
        ('COST', 'shrinkage', 0.04, 'Inventory loss'),
        ('REGULATORY', 'fssai_compliance', 0.03, 'Food safety'),
        ('REGULATORY', 'gst_rates', 0.02, 'Tax rates'),
    ],
    'CONSUMER_TEXTILE': [
        ('DEMAND', 'domestic_demand', 0.12, 'India consumption'),
        ('DEMAND', 'export_orders', 0.10, 'Global brands orders'),
        ('DEMAND', 'fashion_cycles', 0.06, 'Trend changes'),
        ('COST', 'cotton_prices', 0.15, 'Cotton commodity'),
        ('COST', 'yarn_prices', 0.08, 'Yarn costs'),
        ('COST', 'labor_costs', 0.08, 'Wage inflation'),
        ('COST', 'power_costs', 0.05, 'Electricity'),
        ('REGULATORY', 'msp_cotton', 0.04, 'Cotton MSP'),
        ('REGULATORY', 'export_incentives', 0.03, 'RoSCTL/RoDTEP'),
    ],

    # =========== CONSUMER STAPLES ===========
    'CONSUMER_AGRI': [
        ('DEMAND', 'food_inflation', 0.10, 'Food price index'),
        ('DEMAND', 'rural_consumption', 0.12, 'Rural demand'),
        ('DEMAND', 'export_demand', 0.08, 'Agri exports'),
        ('COST', 'input_costs', 0.08, 'Seeds/fertilizer'),
        ('COST', 'labor_costs', 0.06, 'Farm wages'),
        ('COST', 'logistics_costs', 0.05, 'Transportation'),
        ('REGULATORY', 'msp_hikes', 0.08, 'Minimum support prices'),
        ('REGULATORY', 'export_restrictions', 0.05, 'Export bans'),
        ('REGULATORY', 'apmc_reforms', 0.03, 'Market reforms'),
    ],
    'CONSUMER_FMCG_HPC': [
        ('DEMAND', 'urban_consumption', 0.10, 'Urban demand'),
        ('DEMAND', 'rural_penetration', 0.10, 'Rural adoption'),
        ('DEMAND', 'premiumization', 0.08, 'Trade-up trend'),
        ('DEMAND', 'distribution_reach', 0.06, 'Outlet coverage'),
        ('COST', 'palm_oil_prices', 0.08, 'PFAD prices'),
        ('COST', 'packaging_costs', 0.06, 'Plastic/carton'),
        ('COST', 'ad_spend', 0.06, 'A&P costs'),
        ('REGULATORY', 'gst_rates', 0.03, 'Tax slabs'),
        ('REGULATORY', 'labeling_norms', 0.02, 'Packaging rules'),
    ],
    'CONSUMER_FMCG_PACKAGED_FOOD': [
        ('DEMAND', 'snacking_growth', 0.10, 'Packaged snacks'),
        ('DEMAND', 'health_consciousness', 0.08, 'Health foods'),
        ('DEMAND', 'convenience_trend', 0.08, 'Ready-to-eat'),
        ('DEMAND', 'distribution_expansion', 0.06, 'GT/MT reach'),
        ('COST', 'wheat_prices', 0.08, 'Wheat commodity'),
        ('COST', 'milk_prices', 0.08, 'Dairy input'),
        ('COST', 'packaging_costs', 0.06, 'Packaging'),
        ('REGULATORY', 'fssai_standards', 0.04, 'Food safety'),
        ('REGULATORY', 'labeling_front_of_pack', 0.03, 'FOP labeling'),
    ],
    'CONSUMER_FMCG_STAPLES': [
        ('DEMAND', 'volume_growth', 0.12, 'Category volumes'),
        ('DEMAND', 'rural_demand', 0.10, 'Rural consumption'),
        ('DEMAND', 'value_growth', 0.06, 'Pricing trends'),
        ('COST', 'commodity_prices', 0.15, 'Raw material'),
        ('COST', 'packaging_costs', 0.06, 'Packaging'),
        ('COST', 'logistics_costs', 0.05, 'Distribution'),
        ('REGULATORY', 'tax_rates', 0.06, 'GST/excise'),
        ('REGULATORY', 'export_duties', 0.04, 'Export taxes'),
    ],
    'CONSUMER_FOOD_BEVERAGE': [
        ('DEMAND', 'eating_out_trend', 0.10, 'QSR growth'),
        ('DEMAND', 'beverage_consumption', 0.10, 'Soft drinks/juice'),
        ('DEMAND', 'health_trends', 0.06, 'Healthy beverages'),
        ('DEMAND', 'seasonal_demand', 0.06, 'Summer peak'),
        ('COST', 'sugar_prices', 0.10, 'Sugar commodity'),
        ('COST', 'fruit_pulp_prices', 0.06, 'Juice inputs'),
        ('COST', 'packaging_costs', 0.06, 'PET/glass'),
        ('REGULATORY', 'sugar_content_norms', 0.04, 'Health warnings'),
        ('REGULATORY', 'plastic_ban', 0.03, 'Single-use plastic'),
    ],

    # =========== ENERGY UTILITIES ===========
    'ENERGY_UPSTREAM': [
        ('DEMAND', 'crude_oil_prices', 0.20, 'Brent/WTI prices'),
        ('DEMAND', 'natural_gas_prices', 0.12, 'Henry Hub/JKM'),
        ('DEMAND', 'production_volume', 0.10, 'Output levels'),
        ('COST', 'drilling_costs', 0.08, 'CAPEX intensity'),
        ('COST', 'operating_costs', 0.06, 'Lifting costs'),
        ('REGULATORY', 'cess_rates', 0.06, 'Oil cess'),
        ('REGULATORY', 'exploration_policy', 0.04, 'OALP/HELP'),
        ('REGULATORY', 'gas_pricing', 0.04, 'APM gas prices'),
    ],
    'ENERGY_MIDSTREAM': [
        ('DEMAND', 'pipeline_throughput', 0.15, 'Transmission volumes'),
        ('DEMAND', 'city_gas_distribution', 0.12, 'CGD expansion'),
        ('DEMAND', 'industrial_demand', 0.08, 'Industrial PNG'),
        ('COST', 'pipeline_tariffs', 0.10, 'Transmission rates'),
        ('COST', 'maintenance_costs', 0.06, 'OPEX'),
        ('REGULATORY', 'pngrb_tariffs', 0.08, 'Regulated tariffs'),
        ('REGULATORY', 'ga_bidding', 0.04, 'Geographic area rounds'),
    ],
    'ENERGY_DOWNSTREAM': [
        ('DEMAND', 'fuel_demand', 0.12, 'Petrol/diesel consumption'),
        ('DEMAND', 'petrochemical_demand', 0.10, 'Polymer demand'),
        ('DEMAND', 'grm_levels', 0.15, 'Gross refining margin'),
        ('COST', 'crude_oil_prices', 0.15, 'Input crude costs'),
        ('COST', 'refinery_opex', 0.06, 'Operating costs'),
        ('REGULATORY', 'fuel_pricing', 0.08, 'Retail price control'),
        ('REGULATORY', 'biofuel_blending', 0.04, 'Ethanol mandate'),
    ],
    'ENERGY_POWER_DISTRIBUTION': [
        ('DEMAND', 'power_demand_growth', 0.12, 'Electricity demand'),
        ('DEMAND', 'industrial_demand', 0.08, 'Industrial consumption'),
        ('DEMAND', 'agricultural_demand', 0.06, 'Farm pump sets'),
        ('COST', 'power_purchase_cost', 0.15, 'Generation cost'),
        ('COST', 'transmission_losses', 0.08, 'AT&C losses'),
        ('REGULATORY', 'tariff_revisions', 0.12, 'SERC tariff orders'),
        ('REGULATORY', 'subsidy_payments', 0.06, 'State subsidies'),
    ],
    'ENERGY_POWER_GENERATION': [
        ('DEMAND', 'power_demand', 0.12, 'Electricity demand'),
        ('DEMAND', 'plf_levels', 0.10, 'Plant load factor'),
        ('DEMAND', 'merchant_prices', 0.08, 'Spot power prices'),
        ('COST', 'coal_prices', 0.15, 'Coal costs'),
        ('COST', 'gas_prices', 0.06, 'Gas-based plants'),
        ('COST', 'renewable_costs', 0.05, 'Solar/wind LCOE'),
        ('REGULATORY', 'ppa_rates', 0.08, 'Power purchase agreements'),
        ('REGULATORY', 'rpo_compliance', 0.04, 'Renewable obligations'),
    ],

    # =========== FINANCIALS ===========
    'FINANCIALS_BANKING_PRIVATE': [
        ('DEMAND', 'credit_growth', 0.15, 'Loan book growth'),
        ('DEMAND', 'casa_ratio', 0.10, 'Low-cost deposits'),
        ('DEMAND', 'fee_income', 0.08, 'Non-interest income'),
        ('COST', 'cost_of_funds', 0.10, 'Deposit costs'),
        ('COST', 'credit_costs', 0.12, 'Provisioning'),
        ('COST', 'operating_costs', 0.06, 'Cost-to-income'),
        ('REGULATORY', 'rbi_rates', 0.08, 'Repo rate'),
        ('REGULATORY', 'capital_norms', 0.04, 'Basel III'),
    ],
    'FINANCIALS_BANKING_PSU': [
        ('DEMAND', 'credit_growth', 0.12, 'Loan growth'),
        ('DEMAND', 'government_business', 0.10, 'Govt deposits/advances'),
        ('DEMAND', 'msme_lending', 0.08, 'Priority sector'),
        ('COST', 'cost_of_funds', 0.08, 'Deposit costs'),
        ('COST', 'credit_costs', 0.12, 'NPA provisions'),
        ('COST', 'operating_costs', 0.06, 'Branch costs'),
        ('REGULATORY', 'rbi_rates', 0.08, 'Monetary policy'),
        ('REGULATORY', 'capital_infusion', 0.06, 'Govt recapitalization'),
    ],
    'FINANCIALS_NBFC_DIVERSIFIED': [
        ('DEMAND', 'aum_growth', 0.15, 'Assets under management'),
        ('DEMAND', 'disbursement_growth', 0.10, 'New loans'),
        ('DEMAND', 'yield_on_advances', 0.08, 'Lending rates'),
        ('COST', 'cost_of_borrowing', 0.12, 'Funding costs'),
        ('COST', 'credit_costs', 0.10, 'Provisions'),
        ('COST', 'operating_expenses', 0.06, 'OPEX ratio'),
        ('REGULATORY', 'scale_based_regulation', 0.06, 'RBI norms'),
        ('REGULATORY', 'capital_adequacy', 0.04, 'CAR requirements'),
    ],
    'FINANCIALS_NBFC_HOUSING': [
        ('DEMAND', 'housing_demand', 0.12, 'Home sales'),
        ('DEMAND', 'loan_growth', 0.12, 'Disbursements'),
        ('DEMAND', 'average_ticket_size', 0.06, 'Loan size trend'),
        ('COST', 'cost_of_funds', 0.12, 'Borrowing costs'),
        ('COST', 'credit_costs', 0.08, 'NPA provisions'),
        ('REGULATORY', 'pmay_subsidy', 0.06, 'CLSS scheme'),
        ('REGULATORY', 'npa_recognition', 0.05, 'RBI/NHB norms'),
    ],
    'FINANCIALS_NBFC_VEHICLE': [
        ('DEMAND', 'vehicle_sales', 0.15, 'Auto sales volume'),
        ('DEMAND', 'loan_disbursements', 0.12, 'New loans'),
        ('DEMAND', 'used_vehicle_financing', 0.08, 'Pre-owned segment'),
        ('COST', 'cost_of_funds', 0.12, 'Borrowing costs'),
        ('COST', 'credit_costs', 0.10, 'Provisions'),
        ('REGULATORY', 'rbi_norms', 0.05, 'Lending guidelines'),
        ('REGULATORY', 'ltv_limits', 0.04, 'LTV caps'),
    ],
    'FINANCIALS_ASSET_MGMT': [
        ('DEMAND', 'aum_growth', 0.18, 'Assets under management'),
        ('DEMAND', 'sip_flows', 0.12, 'Systematic investment'),
        ('DEMAND', 'equity_market_returns', 0.10, 'Market performance'),
        ('COST', 'expense_ratio', 0.08, 'TER trends'),
        ('COST', 'distribution_costs', 0.06, 'Commission payouts'),
        ('REGULATORY', 'sebi_ter_norms', 0.06, 'Expense caps'),
        ('REGULATORY', 'disclosure_norms', 0.04, 'Transparency'),
    ],
    'FINANCIALS_BROKING': [
        ('DEMAND', 'trading_volumes', 0.18, 'Market turnover'),
        ('DEMAND', 'new_demat_accounts', 0.12, 'Account additions'),
        ('DEMAND', 'active_clients', 0.10, 'Monthly active'),
        ('COST', 'technology_costs', 0.08, 'IT infrastructure'),
        ('COST', 'compliance_costs', 0.05, 'Regulatory compliance'),
        ('REGULATORY', 'sebi_norms', 0.06, 'Broker regulations'),
        ('REGULATORY', 'transaction_charges', 0.04, 'Exchange fees'),
    ],
    'FINANCIALS_EXCHANGES_DEPOSITORIES': [
        ('DEMAND', 'trading_volumes', 0.20, 'Exchange turnover'),
        ('DEMAND', 'new_issuances', 0.10, 'IPO/rights'),
        ('DEMAND', 'demat_accounts', 0.10, 'Account growth'),
        ('COST', 'technology_costs', 0.08, 'Systems investment'),
        ('COST', 'regulatory_costs', 0.04, 'Compliance'),
        ('REGULATORY', 'sebi_fees', 0.06, 'Transaction levies'),
        ('REGULATORY', 'listing_norms', 0.04, 'Listing requirements'),
    ],
    'FINANCIALS_RATINGS': [
        ('DEMAND', 'debt_issuances', 0.20, 'Bond market activity'),
        ('DEMAND', 'bank_credit_growth', 0.10, 'Bank rating demand'),
        ('DEMAND', 'sme_ratings', 0.08, 'SME assessments'),
        ('COST', 'analyst_costs', 0.08, 'Human resources'),
        ('COST', 'technology_investments', 0.05, 'Digital platforms'),
        ('REGULATORY', 'sebi_cra_norms', 0.08, 'Rating agency rules'),
        ('REGULATORY', 'disclosure_requirements', 0.04, 'Transparency'),
    ],
    'FINANCIALS_INSURANCE_LIFE': [
        ('DEMAND', 'new_business_premium', 0.15, 'NBP growth'),
        ('DEMAND', 'persistency_ratio', 0.10, '13th month persistency'),
        ('DEMAND', 'apev_growth', 0.08, 'Value of new business'),
        ('COST', 'mortality_experience', 0.08, 'Claims ratio'),
        ('COST', 'commission_costs', 0.08, 'Distribution costs'),
        ('COST', 'opex_ratio', 0.06, 'Operating expenses'),
        ('REGULATORY', 'irdai_solvency', 0.06, 'Solvency margins'),
        ('REGULATORY', 'product_regulations', 0.04, 'Product approvals'),
    ],
    'FINANCIALS_INSURANCE_GENERAL': [
        ('DEMAND', 'gwp_growth', 0.15, 'Gross written premium'),
        ('DEMAND', 'motor_insurance', 0.10, 'Motor OD/TP'),
        ('DEMAND', 'health_insurance', 0.08, 'Health segment'),
        ('COST', 'combined_ratio', 0.15, 'Loss + expense ratio'),
        ('COST', 'claims_ratio', 0.10, 'Loss experience'),
        ('REGULATORY', 'irdai_solvency', 0.06, 'Capital requirements'),
        ('REGULATORY', 'motor_tp_rates', 0.04, 'Mandated rates'),
    ],
    'FINANCIALS_INSURANCE_HEALTH': [
        ('DEMAND', 'health_gwp_growth', 0.18, 'Premium growth'),
        ('DEMAND', 'retail_penetration', 0.10, 'Individual policies'),
        ('DEMAND', 'group_business', 0.08, 'Corporate segment'),
        ('COST', 'claims_ratio', 0.15, 'Medical claims'),
        ('COST', 'network_costs', 0.06, 'Hospital empanelment'),
        ('COST', 'fraud_losses', 0.04, 'Claims fraud'),
        ('REGULATORY', 'irdai_norms', 0.06, 'Health insurance rules'),
        ('REGULATORY', 'portability_rules', 0.03, 'Policy portability'),
    ],

    # =========== HEALTHCARE ===========
    'HEALTHCARE_PHARMA_MFG': [
        ('DEMAND', 'domestic_formulations', 0.12, 'India pharma market'),
        ('DEMAND', 'us_generics', 0.12, 'US market sales'),
        ('DEMAND', 'emerging_markets', 0.08, 'ROW sales'),
        ('DEMAND', 'anda_approvals', 0.06, 'New product launches'),
        ('COST', 'api_prices', 0.08, 'Active ingredient costs'),
        ('COST', 'rd_spend', 0.06, 'R&D investment'),
        ('REGULATORY', 'usfda_compliance', 0.10, 'FDA inspections'),
        ('REGULATORY', 'price_control', 0.05, 'DPCO/NPPA'),
    ],
    'HEALTHCARE_PHARMA_CRO_CDMO': [
        ('DEMAND', 'global_outsourcing', 0.15, 'Pharma outsourcing trend'),
        ('DEMAND', 'order_book', 0.12, 'Contract backlog'),
        ('DEMAND', 'new_molecule_pipeline', 0.10, 'Innovation pipeline'),
        ('COST', 'scientist_costs', 0.08, 'R&D talent'),
        ('COST', 'capex_intensity', 0.06, 'Capacity investments'),
        ('REGULATORY', 'usfda_audits', 0.08, 'Regulatory inspections'),
        ('REGULATORY', 'ehs_compliance', 0.04, 'Environmental norms'),
    ],
    'HEALTHCARE_HOSPITALS': [
        ('DEMAND', 'arpob', 0.15, 'Average revenue per occupied bed'),
        ('DEMAND', 'occupancy_rate', 0.12, 'Bed occupancy'),
        ('DEMAND', 'medical_tourism', 0.06, 'International patients'),
        ('COST', 'doctor_costs', 0.10, 'Physician salaries'),
        ('COST', 'consumables', 0.06, 'Medical supplies'),
        ('COST', 'real_estate', 0.05, 'Expansion capex'),
        ('REGULATORY', 'nabh_accreditation', 0.04, 'Quality standards'),
        ('REGULATORY', 'pricing_caps', 0.05, 'Stent/implant caps'),
    ],
    'HEALTHCARE_DIAGNOSTICS': [
        ('DEMAND', 'test_volumes', 0.15, 'Tests performed'),
        ('DEMAND', 'revenue_per_test', 0.10, 'Realization'),
        ('DEMAND', 'network_expansion', 0.08, 'Collection centers'),
        ('DEMAND', 'wellness_testing', 0.06, 'Preventive health'),
        ('COST', 'reagent_costs', 0.08, 'Testing consumables'),
        ('COST', 'collection_costs', 0.06, 'Sample logistics'),
        ('REGULATORY', 'nabl_accreditation', 0.04, 'Lab standards'),
        ('REGULATORY', 'price_controls', 0.04, 'Test price caps'),
    ],
    'HEALTHCARE_MEDICAL_EQUIPMENT': [
        ('DEMAND', 'hospital_capex', 0.15, 'Hospital equipment spend'),
        ('DEMAND', 'import_substitution', 0.10, 'Make in India'),
        ('DEMAND', 'diagnostics_growth', 0.08, 'Imaging/lab equipment'),
        ('COST', 'component_costs', 0.08, 'Electronics/parts'),
        ('COST', 'rd_investment', 0.06, 'Product development'),
        ('REGULATORY', 'cdsco_approvals', 0.06, 'Device registration'),
        ('REGULATORY', 'mdr_compliance', 0.04, 'Medical device rules'),
    ],
    'HEALTHCARE_AYUSH': [
        ('DEMAND', 'ayurvedic_market', 0.15, 'Ayurveda growth'),
        ('DEMAND', 'wellness_trend', 0.10, 'Natural products'),
        ('DEMAND', 'export_demand', 0.08, 'Global herbal'),
        ('COST', 'herb_procurement', 0.10, 'Raw material'),
        ('COST', 'formulation_costs', 0.06, 'Manufacturing'),
        ('REGULATORY', 'ayush_licensing', 0.06, 'Product approvals'),
        ('REGULATORY', 'gmp_compliance', 0.04, 'Manufacturing standards'),
    ],

    # =========== INDUSTRIALS ===========
    'INDUSTRIALS_DEFENSE': [
        ('DEMAND', 'defense_budget', 0.18, 'MoD capital budget'),
        ('DEMAND', 'order_book', 0.15, 'Contract backlog'),
        ('DEMAND', 'indigenization', 0.10, 'Make in India defense'),
        ('DEMAND', 'exports', 0.06, 'Defense exports'),
        ('COST', 'raw_material', 0.06, 'Steel/aluminum'),
        ('COST', 'rd_spend', 0.05, 'Development costs'),
        ('REGULATORY', 'dag_policy', 0.06, 'Acquisition guidelines'),
        ('REGULATORY', 'offset_requirements', 0.04, 'Offset obligations'),
    ],
    'INDUSTRIALS_CAPITAL_GOODS': [
        ('DEMAND', 'capex_cycle', 0.18, 'Industrial capex'),
        ('DEMAND', 'order_inflows', 0.12, 'New orders'),
        ('DEMAND', 'government_infra', 0.10, 'Govt infrastructure'),
        ('COST', 'steel_prices', 0.08, 'Steel costs'),
        ('COST', 'component_costs', 0.06, 'Bought-out parts'),
        ('COST', 'labor_costs', 0.05, 'Wage inflation'),
        ('REGULATORY', 'make_in_india', 0.04, 'Localization policy'),
        ('REGULATORY', 'pli_schemes', 0.04, 'PLI incentives'),
    ],
    'INDUSTRIALS_ELECTRICALS': [
        ('DEMAND', 'construction_activity', 0.12, 'Real estate/infra'),
        ('DEMAND', 'power_distribution', 0.12, 'Discom capex'),
        ('DEMAND', 'industrial_expansion', 0.08, 'Factory electrification'),
        ('COST', 'copper_prices', 0.12, 'Copper commodity'),
        ('COST', 'aluminum_prices', 0.08, 'Aluminum costs'),
        ('COST', 'pvc_prices', 0.04, 'Insulation materials'),
        ('REGULATORY', 'safety_standards', 0.04, 'BIS norms'),
        ('REGULATORY', 'energy_efficiency', 0.03, 'Efficiency ratings'),
    ],
    'INDUSTRIALS_ENGINEERING': [
        ('DEMAND', 'industrial_production', 0.15, 'IIP growth'),
        ('DEMAND', 'export_orders', 0.10, 'Global demand'),
        ('DEMAND', 'domestic_capex', 0.10, 'India investment'),
        ('COST', 'raw_material', 0.10, 'Steel/metals'),
        ('COST', 'labor_costs', 0.06, 'Wage costs'),
        ('REGULATORY', 'quality_certifications', 0.04, 'ISO standards'),
        ('REGULATORY', 'export_incentives', 0.03, 'RoDTEP'),
    ],
    'INDUSTRIALS_TELECOM_EQUIPMENT': [
        ('DEMAND', 'telecom_capex', 0.18, 'Operator network spend'),
        ('DEMAND', '5g_rollout', 0.12, '5G deployment'),
        ('DEMAND', 'fiber_expansion', 0.10, 'FTTH growth'),
        ('DEMAND', 'government_orders', 0.08, 'BharatNet/defense'),
        ('COST', 'component_costs', 0.08, 'Electronics parts'),
        ('COST', 'rd_investment', 0.06, 'R&D spend'),
        ('REGULATORY', 'pli_telecom', 0.06, 'PLI incentives'),
        ('REGULATORY', 'trusted_sources', 0.04, 'Security clearances'),
    ],

    # =========== MATERIALS CHEMICALS ===========
    'CHEMICALS_SPECIALTY': [
        ('DEMAND', 'agrochemical_demand', 0.12, 'Agchem volumes'),
        ('DEMAND', 'pharma_intermediates', 0.10, 'Pharma API demand'),
        ('DEMAND', 'china_plus_one', 0.10, 'Supply chain shift'),
        ('COST', 'crude_derivatives', 0.10, 'Petrochemical inputs'),
        ('COST', 'power_costs', 0.06, 'Energy costs'),
        ('COST', 'logistics', 0.04, 'Freight costs'),
        ('REGULATORY', 'environmental_norms', 0.06, 'Pollution control'),
        ('REGULATORY', 'reach_compliance', 0.04, 'EU REACH'),
    ],
    'CHEMICALS_COMMODITY': [
        ('DEMAND', 'industrial_demand', 0.12, 'Industrial consumption'),
        ('DEMAND', 'agricultural_demand', 0.10, 'Fertilizer/agchem'),
        ('DEMAND', 'export_markets', 0.08, 'Global demand'),
        ('COST', 'feedstock_prices', 0.15, 'Raw material'),
        ('COST', 'energy_costs', 0.10, 'Power/gas'),
        ('REGULATORY', 'pollution_control', 0.06, 'CPCB norms'),
        ('REGULATORY', 'import_duties', 0.04, 'Trade protection'),
    ],
    'CHEMICALS_PAINTS_COATINGS': [
        ('DEMAND', 'decorative_demand', 0.15, 'Housing/renovation'),
        ('DEMAND', 'industrial_coatings', 0.10, 'Auto/industrial'),
        ('DEMAND', 'distribution_reach', 0.08, 'Dealer network'),
        ('COST', 'titanium_dioxide', 0.12, 'TiO2 prices'),
        ('COST', 'crude_derivatives', 0.08, 'Solvents/resins'),
        ('COST', 'packaging_costs', 0.04, 'Tin/plastic'),
        ('REGULATORY', 'voc_norms', 0.04, 'VOC limits'),
        ('REGULATORY', 'lead_content', 0.02, 'Lead-free standards'),
    ],

    # =========== MATERIALS METALS ===========
    'METALS_STEEL': [
        ('DEMAND', 'construction_demand', 0.12, 'Real estate/infra'),
        ('DEMAND', 'auto_demand', 0.10, 'Automotive steel'),
        ('DEMAND', 'export_markets', 0.08, 'Export volumes'),
        ('COST', 'iron_ore_prices', 0.15, 'Iron ore costs'),
        ('COST', 'coking_coal_prices', 0.12, 'Met coal costs'),
        ('COST', 'power_costs', 0.06, 'Energy costs'),
        ('REGULATORY', 'import_duties', 0.05, 'Trade protection'),
        ('REGULATORY', 'export_duties', 0.04, 'Export taxes'),
    ],
    'METALS_ALUMINUM': [
        ('DEMAND', 'construction_demand', 0.10, 'Building products'),
        ('DEMAND', 'auto_lightweighting', 0.10, 'EV aluminum'),
        ('DEMAND', 'packaging_demand', 0.08, 'Foil/cans'),
        ('COST', 'alumina_prices', 0.12, 'Alumina costs'),
        ('COST', 'power_costs', 0.15, 'Electricity intensive'),
        ('COST', 'carbon_anode', 0.05, 'Anode costs'),
        ('REGULATORY', 'lme_prices', 0.10, 'LME aluminum'),
        ('REGULATORY', 'export_duties', 0.03, 'Trade policy'),
    ],
    'METALS_COPPER_ZINC': [
        ('DEMAND', 'electrical_demand', 0.12, 'Power/electronics'),
        ('DEMAND', 'construction_demand', 0.10, 'Building products'),
        ('DEMAND', 'auto_demand', 0.08, 'EV copper content'),
        ('COST', 'lme_copper_prices', 0.15, 'LME copper'),
        ('COST', 'lme_zinc_prices', 0.10, 'LME zinc'),
        ('COST', 'concentrate_supply', 0.06, 'Ore availability'),
        ('REGULATORY', 'mining_policy', 0.04, 'Mining leases'),
        ('REGULATORY', 'environmental_norms', 0.04, 'Smelter pollution'),
    ],

    # =========== REAL ESTATE INFRA ===========
    'REALTY_RESIDENTIAL': [
        ('DEMAND', 'housing_demand', 0.15, 'Home sales'),
        ('DEMAND', 'affordability_index', 0.10, 'Price-to-income'),
        ('DEMAND', 'inventory_levels', 0.08, 'Unsold stock'),
        ('COST', 'construction_costs', 0.10, 'Building costs'),
        ('COST', 'land_prices', 0.08, 'Land acquisition'),
        ('COST', 'financing_costs', 0.06, 'Project funding'),
        ('REGULATORY', 'rera_compliance', 0.06, 'RERA norms'),
        ('REGULATORY', 'stamp_duty', 0.04, 'Transaction costs'),
    ],
    'INFRA_CONSTRUCTION': [
        ('DEMAND', 'government_capex', 0.18, 'Govt infra spend'),
        ('DEMAND', 'order_book', 0.12, 'Contract backlog'),
        ('DEMAND', 'private_capex', 0.08, 'Private investment'),
        ('COST', 'steel_prices', 0.10, 'Steel costs'),
        ('COST', 'cement_prices', 0.08, 'Cement costs'),
        ('COST', 'labor_costs', 0.06, 'Wage inflation'),
        ('REGULATORY', 'land_acquisition', 0.05, 'Right of way'),
        ('REGULATORY', 'environmental_clearances', 0.04, 'EC delays'),
    ],
    'INFRA_LOGISTICS_PORTS': [
        ('DEMAND', 'exim_trade', 0.15, 'Trade volumes'),
        ('DEMAND', 'container_volumes', 0.12, 'TEU throughput'),
        ('DEMAND', 'coastal_shipping', 0.06, 'Cabotage'),
        ('COST', 'fuel_costs', 0.08, 'Bunker costs'),
        ('COST', 'equipment_costs', 0.06, 'Crane/handling'),
        ('REGULATORY', 'port_tariffs', 0.08, 'TAMP rates'),
        ('REGULATORY', 'cabotage_rules', 0.04, 'Coastal shipping'),
    ],

    # =========== SERVICES ===========
    'SERVICES_HOSPITALITY': [
        ('DEMAND', 'occupancy_rates', 0.15, 'Room occupancy'),
        ('DEMAND', 'arr_growth', 0.12, 'Average room rate'),
        ('DEMAND', 'revpar', 0.10, 'Revenue per available room'),
        ('DEMAND', 'corporate_travel', 0.08, 'Business demand'),
        ('COST', 'staff_costs', 0.08, 'Wage inflation'),
        ('COST', 'food_costs', 0.06, 'F&B costs'),
        ('REGULATORY', 'tourism_policy', 0.04, 'Visa/incentives'),
        ('REGULATORY', 'liquor_licenses', 0.03, 'State policies'),
    ],
    'SERVICES_MEDIA_BROADCASTING': [
        ('DEMAND', 'ad_revenue', 0.18, 'TV advertising'),
        ('DEMAND', 'viewership_share', 0.12, 'TRP ratings'),
        ('DEMAND', 'subscription_revenue', 0.08, 'Carriage fees'),
        ('COST', 'content_costs', 0.12, 'Programming'),
        ('COST', 'distribution_costs', 0.06, 'Carriage/DTH'),
        ('REGULATORY', 'nto_tariffs', 0.06, 'TRAI regulations'),
        ('REGULATORY', 'content_guidelines', 0.04, 'I&B ministry'),
    ],
    'SERVICES_MEDIA_OTT': [
        ('DEMAND', 'subscriber_growth', 0.18, 'Paid subscribers'),
        ('DEMAND', 'arpu', 0.12, 'Revenue per user'),
        ('DEMAND', 'watch_time', 0.08, 'Engagement'),
        ('COST', 'content_acquisition', 0.15, 'Licensing/originals'),
        ('COST', 'technology_costs', 0.08, 'Streaming infrastructure'),
        ('REGULATORY', 'it_rules', 0.04, 'Content regulations'),
        ('REGULATORY', 'data_localization', 0.03, 'Data storage'),
    ],
    'SERVICES_MEDIA_PRINT': [
        ('DEMAND', 'circulation', 0.12, 'Copy sales'),
        ('DEMAND', 'ad_revenue', 0.15, 'Print advertising'),
        ('DEMAND', 'digital_transition', 0.08, 'Online subscriptions'),
        ('COST', 'newsprint_prices', 0.15, 'Paper costs'),
        ('COST', 'distribution_costs', 0.08, 'Delivery network'),
        ('REGULATORY', 'newsprint_import', 0.04, 'Import duties'),
    ],
    'SERVICES_MEDIA_ADVERTISING': [
        ('DEMAND', 'adex_growth', 0.18, 'Ad expenditure'),
        ('DEMAND', 'digital_share', 0.12, 'Digital advertising'),
        ('DEMAND', 'client_budgets', 0.10, 'Corporate ad spend'),
        ('COST', 'talent_costs', 0.10, 'Creative talent'),
        ('COST', 'media_buying', 0.06, 'Media costs'),
        ('REGULATORY', 'asci_guidelines', 0.03, 'Ad standards'),
    ],
    'SERVICES_TELECOM_OPERATORS': [
        ('DEMAND', 'subscriber_growth', 0.10, 'Mobile subscribers'),
        ('DEMAND', 'arpu', 0.15, 'Revenue per user'),
        ('DEMAND', 'data_consumption', 0.10, 'GB per user'),
        ('COST', 'spectrum_costs', 0.12, 'Spectrum payments'),
        ('COST', 'network_opex', 0.08, 'Tower/maintenance'),
        ('COST', 'content_costs', 0.04, 'Bundled content'),
        ('REGULATORY', 'agr_dues', 0.06, 'Regulatory levies'),
        ('REGULATORY', 'tariff_floors', 0.04, 'Minimum pricing'),
    ],
    'SERVICES_TELECOM_TOWERS': [
        ('DEMAND', 'tenancy_ratio', 0.18, 'Tenants per tower'),
        ('DEMAND', 'new_tower_additions', 0.12, 'Tower rollout'),
        ('DEMAND', 'colocations', 0.10, 'Sharing growth'),
        ('COST', 'power_costs', 0.12, 'Diesel/electricity'),
        ('COST', 'site_rentals', 0.08, 'Land lease'),
        ('REGULATORY', 'emf_norms', 0.04, 'Radiation limits'),
        ('REGULATORY', 'row_permissions', 0.04, 'Right of way'),
    ],

    # =========== TECHNOLOGY ===========
    'TECHNOLOGY_IT_SERVICES': [
        ('DEMAND', 'deal_wins', 0.15, 'Contract signings'),
        ('DEMAND', 'client_spending', 0.12, 'IT budgets'),
        ('DEMAND', 'digital_revenue', 0.10, 'Digital/cloud mix'),
        ('COST', 'employee_costs', 0.15, 'Wage inflation'),
        ('COST', 'attrition', 0.08, 'Employee turnover'),
        ('COST', 'subcontracting', 0.04, 'Third-party costs'),
        ('REGULATORY', 'visa_policies', 0.06, 'H1B/immigration'),
        ('REGULATORY', 'data_privacy', 0.03, 'GDPR/compliance'),
    ],
    'TECHNOLOGY_BPO_ITES': [
        ('DEMAND', 'outsourcing_trend', 0.15, 'BPO market growth'),
        ('DEMAND', 'deal_pipeline', 0.10, 'New contracts'),
        ('DEMAND', 'voice_vs_nonvoice', 0.06, 'Mix shift'),
        ('COST', 'employee_costs', 0.15, 'Wage inflation'),
        ('COST', 'real_estate', 0.08, 'Office costs'),
        ('COST', 'technology', 0.06, 'Automation investment'),
        ('REGULATORY', 'labor_laws', 0.04, 'Employment norms'),
    ],
    'TECHNOLOGY_DIGITAL_INFRA': [
        ('DEMAND', 'data_center_demand', 0.18, 'Rack capacity'),
        ('DEMAND', 'cloud_adoption', 0.12, 'Enterprise cloud'),
        ('DEMAND', 'data_localization', 0.08, 'Local storage mandate'),
        ('COST', 'power_costs', 0.12, 'Electricity'),
        ('COST', 'real_estate', 0.08, 'Land/building'),
        ('COST', 'cooling_costs', 0.06, 'HVAC systems'),
        ('REGULATORY', 'data_protection', 0.06, 'DPDP compliance'),
        ('REGULATORY', 'power_approvals', 0.03, 'Electricity connections'),
    ],
    'TECHNOLOGY_PRODUCT_SAAS': [
        ('DEMAND', 'arr_growth', 0.18, 'Annual recurring revenue'),
        ('DEMAND', 'net_retention', 0.12, 'NDR/NRR'),
        ('DEMAND', 'new_customer_adds', 0.10, 'Logo additions'),
        ('COST', 'rd_investment', 0.10, 'Product development'),
        ('COST', 'sales_marketing', 0.10, 'GTM costs'),
        ('COST', 'cloud_hosting', 0.06, 'Infrastructure'),
        ('REGULATORY', 'data_privacy', 0.04, 'GDPR/DPDP'),
    ],
}


def get_all_subgroups():
    """Get all unique subgroups from MySQL"""
    conn = mysql.connector.connect(
        host=os.getenv('MYSQL_HOST', 'localhost'),
        port=int(os.getenv('MYSQL_PORT', 3306)),
        user=os.getenv('MYSQL_USER', 'root'),
        password=os.getenv('MYSQL_PASSWORD', ''),
        database=os.getenv('MYSQL_DATABASE', 'rag')
    )
    cursor = conn.cursor()
    cursor.execute("""
        SELECT DISTINCT valuation_subgroup
        FROM vs_active_companies
        WHERE valuation_subgroup IS NOT NULL AND valuation_subgroup != ''
        ORDER BY valuation_subgroup
    """)
    subgroups = [row[0] for row in cursor.fetchall()]
    cursor.close()
    conn.close()
    return subgroups


def get_existing_subgroup_drivers():
    """Get subgroups that already have drivers"""
    conn = mysql.connector.connect(
        host=os.getenv('MYSQL_HOST', 'localhost'),
        port=int(os.getenv('MYSQL_PORT', 3306)),
        user=os.getenv('MYSQL_USER', 'root'),
        password=os.getenv('MYSQL_PASSWORD', ''),
        database=os.getenv('MYSQL_DATABASE', 'rag')
    )
    cursor = conn.cursor()
    cursor.execute("""
        SELECT DISTINCT valuation_subgroup
        FROM vs_drivers
        WHERE driver_level = 'SUBGROUP' AND valuation_subgroup IS NOT NULL
    """)
    existing = [row[0] for row in cursor.fetchall()]
    cursor.close()
    conn.close()
    return existing


def get_subgroup_to_group_mapping():
    """Get mapping of subgroup to group"""
    conn = mysql.connector.connect(
        host=os.getenv('MYSQL_HOST', 'localhost'),
        port=int(os.getenv('MYSQL_PORT', 3306)),
        user=os.getenv('MYSQL_USER', 'root'),
        password=os.getenv('MYSQL_PASSWORD', ''),
        database=os.getenv('MYSQL_DATABASE', 'rag')
    )
    cursor = conn.cursor()
    cursor.execute("""
        SELECT DISTINCT valuation_subgroup, valuation_group
        FROM vs_active_companies
        WHERE valuation_subgroup IS NOT NULL AND valuation_group IS NOT NULL
    """)
    mapping = {row[0]: row[1] for row in cursor.fetchall()}
    cursor.close()
    conn.close()
    return mapping


def insert_drivers(subgroup, group, drivers, cursor):
    """Insert drivers for a subgroup"""
    inserted = 0
    for category, name, weight, description in drivers:
        try:
            cursor.execute("""
                INSERT INTO vs_drivers
                (driver_level, driver_category, driver_name, valuation_group, valuation_subgroup,
                 weight, impact_direction, trend, updated_by)
                VALUES ('SUBGROUP', %s, %s, %s, %s, %s, 'NEUTRAL', 'STABLE', 'system')
            """, (category, name, group, subgroup, weight))
            inserted += 1
        except Exception as e:
            print(f"      Error inserting {name}: {e}")
    return inserted


def main():
    print("=" * 70)
    print("POPULATE DRIVER DEFINITIONS")
    print("=" * 70)

    # Get all subgroups from MySQL
    all_subgroups = get_all_subgroups()
    print(f"\n[1] Found {len(all_subgroups)} subgroups in vs_active_companies")

    # Get existing drivers
    existing = get_existing_subgroup_drivers()
    print(f"[2] Found {len(existing)} subgroups with existing drivers")

    # Get subgroup to group mapping
    sg_to_group = get_subgroup_to_group_mapping()

    # Connect to MySQL
    conn = mysql.connector.connect(
        host=os.getenv('MYSQL_HOST', 'localhost'),
        port=int(os.getenv('MYSQL_PORT', 3306)),
        user=os.getenv('MYSQL_USER', 'root'),
        password=os.getenv('MYSQL_PASSWORD', ''),
        database=os.getenv('MYSQL_DATABASE', 'rag')
    )
    cursor = conn.cursor()

    # Insert drivers for each subgroup
    print(f"\n[3] Inserting drivers for subgroups...")
    total_inserted = 0
    subgroups_updated = 0

    for subgroup in all_subgroups:
        if subgroup in SUBGROUP_DRIVERS:
            drivers = SUBGROUP_DRIVERS[subgroup]
            group = sg_to_group.get(subgroup, '')

            # Delete existing drivers for this subgroup (to avoid duplicates)
            cursor.execute("""
                DELETE FROM vs_drivers
                WHERE driver_level = 'SUBGROUP' AND valuation_subgroup = %s
            """, (subgroup,))

            # Insert new drivers
            inserted = insert_drivers(subgroup, group, drivers, cursor)
            total_inserted += inserted
            subgroups_updated += 1
            print(f"    {subgroup}: {inserted} drivers")
        else:
            if subgroup not in existing:
                print(f"    {subgroup}: NO DRIVER DEFINITION (needs manual creation)")

    conn.commit()
    cursor.close()
    conn.close()

    print(f"\n[4] Summary:")
    print(f"    Subgroups updated: {subgroups_updated}")
    print(f"    Total drivers inserted: {total_inserted}")

    # List subgroups without drivers
    missing = [sg for sg in all_subgroups if sg not in SUBGROUP_DRIVERS]
    if missing:
        print(f"\n[!] Subgroups without driver definitions ({len(missing)}):")
        for sg in missing:
            print(f"    - {sg}")

    return subgroups_updated, total_inserted


if __name__ == '__main__':
    main()
