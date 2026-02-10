-- ============================================================================
-- Normalize GROUP Driver Weights to 100% for All Valuation Groups
-- ============================================================================
-- Adds new sector-specific drivers per group, then normalizes all weights
-- so each group sums to exactly 100%.
--
-- Pattern: INSERT new drivers with raw weights, then UPDATE all weights
-- by dividing each by the group total (proportional normalization).
-- ============================================================================

-- ============================================================================
-- AUTO (68% → 100%, add 5 new drivers → 12 total)
-- ============================================================================
INSERT INTO vs_drivers
  (driver_level, driver_category, driver_name, valuation_group, weight,
   impact_direction, trend, updated_by, source)
VALUES
  ('GROUP', 'MACRO_SIGNAL', 'rural_demand_index',              'AUTO', 0.07, 'NEUTRAL', 'STABLE', 'system', 'SEED_GROUP'),
  ('GROUP', 'REGULATORY',   'scrappage_policy_impact',         'AUTO', 0.06, 'NEUTRAL', 'STABLE', 'system', 'SEED_GROUP'),
  ('GROUP', 'REGULATORY',   'auto_component_pli',              'AUTO', 0.06, 'NEUTRAL', 'STABLE', 'system', 'SEED_GROUP'),
  ('GROUP', 'COST',         'steel_price_cycle',               'AUTO', 0.07, 'NEUTRAL', 'STABLE', 'system', 'SEED_GROUP'),
  ('GROUP', 'DEMAND',       'consumer_financing_availability', 'AUTO', 0.06, 'NEUTRAL', 'STABLE', 'system', 'SEED_GROUP');

-- Normalize AUTO: raw total = 0.68 + 0.32 = 1.00 (happens to be exact)
-- But let's still normalize to handle any rounding
UPDATE vs_drivers d
JOIN (
  SELECT SUM(weight) as total_weight
  FROM vs_drivers
  WHERE driver_level = 'GROUP' AND valuation_group = 'AUTO'
) t ON 1=1
SET d.weight = ROUND(d.weight / t.total_weight, 3)
WHERE d.driver_level = 'GROUP' AND d.valuation_group = 'AUTO';


-- ============================================================================
-- TECHNOLOGY (73% → 100%, add 4 new drivers → 11 total)
-- ============================================================================
INSERT INTO vs_drivers
  (driver_level, driver_category, driver_name, valuation_group, weight,
   impact_direction, trend, updated_by, source)
VALUES
  ('GROUP', 'DEMAND',       'cloud_infrastructure_spend',  'TECHNOLOGY', 0.08, 'NEUTRAL', 'STABLE', 'system', 'SEED_GROUP'),
  ('GROUP', 'REGULATORY',   'visa_policy_impact',          'TECHNOLOGY', 0.06, 'NEUTRAL', 'STABLE', 'system', 'SEED_GROUP'),
  ('GROUP', 'MACRO_SIGNAL', 'global_recession_risk',       'TECHNOLOGY', 0.07, 'NEUTRAL', 'STABLE', 'system', 'SEED_GROUP'),
  ('GROUP', 'MACRO_SIGNAL', 'rupee_depreciation_trend',    'TECHNOLOGY', 0.06, 'NEUTRAL', 'STABLE', 'system', 'SEED_GROUP');

-- Normalize TECHNOLOGY: raw total = 0.73 + 0.27 = 1.00
UPDATE vs_drivers d
JOIN (
  SELECT SUM(weight) as total_weight
  FROM vs_drivers
  WHERE driver_level = 'GROUP' AND valuation_group = 'TECHNOLOGY'
) t ON 1=1
SET d.weight = ROUND(d.weight / t.total_weight, 3)
WHERE d.driver_level = 'GROUP' AND d.valuation_group = 'TECHNOLOGY';


-- ============================================================================
-- CONSUMER_DISCRETIONARY (76% → 100%, add 4 new drivers → 12 total)
-- ============================================================================
INSERT INTO vs_drivers
  (driver_level, driver_category, driver_name, valuation_group, weight,
   impact_direction, trend, updated_by, source)
VALUES
  ('GROUP', 'DEMAND',       'housing_starts_trend',      'CONSUMER_DISCRETIONARY', 0.06, 'NEUTRAL', 'STABLE', 'system', 'SEED_GROUP'),
  ('GROUP', 'COST',         'gold_price_cycle',          'CONSUMER_DISCRETIONARY', 0.06, 'NEUTRAL', 'STABLE', 'system', 'SEED_GROUP'),
  ('GROUP', 'COMPETITIVE',  'e_commerce_penetration',    'CONSUMER_DISCRETIONARY', 0.06, 'NEUTRAL', 'STABLE', 'system', 'SEED_GROUP'),
  ('GROUP', 'MACRO_SIGNAL', 'per_capita_income_growth',  'CONSUMER_DISCRETIONARY', 0.06, 'NEUTRAL', 'STABLE', 'system', 'SEED_GROUP');

-- Normalize CONSUMER_DISCRETIONARY: raw total = 0.76 + 0.24 = 1.00
UPDATE vs_drivers d
JOIN (
  SELECT SUM(weight) as total_weight
  FROM vs_drivers
  WHERE driver_level = 'GROUP' AND valuation_group = 'CONSUMER_DISCRETIONARY'
) t ON 1=1
SET d.weight = ROUND(d.weight / t.total_weight, 3)
WHERE d.driver_level = 'GROUP' AND d.valuation_group = 'CONSUMER_DISCRETIONARY';


-- ============================================================================
-- SERVICES (76% → 100%, add 4 new drivers → 12 total)
-- ============================================================================
INSERT INTO vs_drivers
  (driver_level, driver_category, driver_name, valuation_group, weight,
   impact_direction, trend, updated_by, source)
VALUES
  ('GROUP', 'DEMAND',       'aviation_traffic_growth',  'SERVICES', 0.06, 'NEUTRAL', 'STABLE', 'system', 'SEED_GROUP'),
  ('GROUP', 'REGULATORY',   'spectrum_auction_cycle',   'SERVICES', 0.06, 'NEUTRAL', 'STABLE', 'system', 'SEED_GROUP'),
  ('GROUP', 'DEMAND',       'hotel_occupancy_rates',    'SERVICES', 0.06, 'NEUTRAL', 'STABLE', 'system', 'SEED_GROUP'),
  ('GROUP', 'COMPETITIVE',  'gig_economy_growth',       'SERVICES', 0.06, 'NEUTRAL', 'STABLE', 'system', 'SEED_GROUP');

-- Normalize SERVICES: raw total = 0.76 + 0.24 = 1.00
UPDATE vs_drivers d
JOIN (
  SELECT SUM(weight) as total_weight
  FROM vs_drivers
  WHERE driver_level = 'GROUP' AND valuation_group = 'SERVICES'
) t ON 1=1
SET d.weight = ROUND(d.weight / t.total_weight, 3)
WHERE d.driver_level = 'GROUP' AND d.valuation_group = 'SERVICES';


-- ============================================================================
-- HEALTHCARE (78% → 100%, add 4 new drivers → 12 total)
-- ============================================================================
INSERT INTO vs_drivers
  (driver_level, driver_category, driver_name, valuation_group, weight,
   impact_direction, trend, updated_by, source)
VALUES
  ('GROUP', 'DEMAND',       'biosimilar_opportunity',      'HEALTHCARE', 0.06, 'NEUTRAL', 'STABLE', 'system', 'SEED_GROUP'),
  ('GROUP', 'COST',         'rd_spending_intensity',       'HEALTHCARE', 0.06, 'NEUTRAL', 'STABLE', 'system', 'SEED_GROUP'),
  ('GROUP', 'REGULATORY',   'ayushman_bharat_expansion',   'HEALTHCARE', 0.05, 'NEUTRAL', 'STABLE', 'system', 'SEED_GROUP'),
  ('GROUP', 'DEMAND',       'medical_tourism_trend',       'HEALTHCARE', 0.05, 'NEUTRAL', 'STABLE', 'system', 'SEED_GROUP');

-- Normalize HEALTHCARE: raw total = 0.78 + 0.22 = 1.00
UPDATE vs_drivers d
JOIN (
  SELECT SUM(weight) as total_weight
  FROM vs_drivers
  WHERE driver_level = 'GROUP' AND valuation_group = 'HEALTHCARE'
) t ON 1=1
SET d.weight = ROUND(d.weight / t.total_weight, 3)
WHERE d.driver_level = 'GROUP' AND d.valuation_group = 'HEALTHCARE';


-- ============================================================================
-- MATERIALS_CHEMICALS (68% → 100%, add 5 new drivers → 12 total)
-- ============================================================================
INSERT INTO vs_drivers
  (driver_level, driver_category, driver_name, valuation_group, weight,
   impact_direction, trend, updated_by, source)
VALUES
  ('GROUP', 'COST',        'polymer_naphtha_cycle',       'MATERIALS_CHEMICALS', 0.07, 'NEUTRAL', 'STABLE', 'system', 'SEED_GROUP'),
  ('GROUP', 'DEMAND',      'agrochemical_demand_cycle',   'MATERIALS_CHEMICALS', 0.06, 'NEUTRAL', 'STABLE', 'system', 'SEED_GROUP'),
  ('GROUP', 'REGULATORY',  'anti_dumping_protection',     'MATERIALS_CHEMICALS', 0.07, 'NEUTRAL', 'STABLE', 'system', 'SEED_GROUP'),
  ('GROUP', 'DEMAND',      'specialty_premium_index',     'MATERIALS_CHEMICALS', 0.06, 'NEUTRAL', 'STABLE', 'system', 'SEED_GROUP'),
  ('GROUP', 'REGULATORY',  'pli_chemicals',               'MATERIALS_CHEMICALS', 0.06, 'NEUTRAL', 'STABLE', 'system', 'SEED_GROUP');

-- Normalize MATERIALS_CHEMICALS: raw total = 0.68 + 0.32 = 1.00
UPDATE vs_drivers d
JOIN (
  SELECT SUM(weight) as total_weight
  FROM vs_drivers
  WHERE driver_level = 'GROUP' AND valuation_group = 'MATERIALS_CHEMICALS'
) t ON 1=1
SET d.weight = ROUND(d.weight / t.total_weight, 3)
WHERE d.driver_level = 'GROUP' AND d.valuation_group = 'MATERIALS_CHEMICALS';


-- ============================================================================
-- MATERIALS_METALS (80% → 100%, add 3 new drivers → 11 total)
-- ============================================================================
INSERT INTO vs_drivers
  (driver_level, driver_category, driver_name, valuation_group, weight,
   impact_direction, trend, updated_by, source)
VALUES
  ('GROUP', 'COST',        'scrap_recycling_adoption',   'MATERIALS_METALS', 0.07, 'NEUTRAL', 'STABLE', 'system', 'SEED_GROUP'),
  ('GROUP', 'REGULATORY',  'carbon_border_adjustment',   'MATERIALS_METALS', 0.07, 'NEUTRAL', 'STABLE', 'system', 'SEED_GROUP'),
  ('GROUP', 'DEMAND',      'domestic_consumption_share',  'MATERIALS_METALS', 0.06, 'NEUTRAL', 'STABLE', 'system', 'SEED_GROUP');

-- Normalize MATERIALS_METALS: raw total = 0.80 + 0.20 = 1.00
UPDATE vs_drivers d
JOIN (
  SELECT SUM(weight) as total_weight
  FROM vs_drivers
  WHERE driver_level = 'GROUP' AND valuation_group = 'MATERIALS_METALS'
) t ON 1=1
SET d.weight = ROUND(d.weight / t.total_weight, 3)
WHERE d.driver_level = 'GROUP' AND d.valuation_group = 'MATERIALS_METALS';


-- ============================================================================
-- ENERGY_UTILITIES (80% → 100%, add 3 new drivers → 11 total)
-- ============================================================================
INSERT INTO vs_drivers
  (driver_level, driver_category, driver_name, valuation_group, weight,
   impact_direction, trend, updated_by, source)
VALUES
  ('GROUP', 'COST',        'grm_cycle',                   'ENERGY_UTILITIES', 0.07, 'NEUTRAL', 'STABLE', 'system', 'SEED_GROUP'),
  ('GROUP', 'DEMAND',      'plf_national_average',        'ENERGY_UTILITIES', 0.07, 'NEUTRAL', 'STABLE', 'system', 'SEED_GROUP'),
  ('GROUP', 'DEMAND',      'ev_charging_infrastructure',  'ENERGY_UTILITIES', 0.06, 'NEUTRAL', 'STABLE', 'system', 'SEED_GROUP');

-- Normalize ENERGY_UTILITIES: raw total = 0.80 + 0.20 = 1.00
UPDATE vs_drivers d
JOIN (
  SELECT SUM(weight) as total_weight
  FROM vs_drivers
  WHERE driver_level = 'GROUP' AND valuation_group = 'ENERGY_UTILITIES'
) t ON 1=1
SET d.weight = ROUND(d.weight / t.total_weight, 3)
WHERE d.driver_level = 'GROUP' AND d.valuation_group = 'ENERGY_UTILITIES';


-- ============================================================================
-- CONSUMER_STAPLES (88% → 100%, add 2 new drivers → 11 total)
-- ============================================================================
INSERT INTO vs_drivers
  (driver_level, driver_category, driver_name, valuation_group, weight,
   impact_direction, trend, updated_by, source)
VALUES
  ('GROUP', 'REGULATORY',  'msp_hike_trend',         'CONSUMER_STAPLES', 0.06, 'NEUTRAL', 'STABLE', 'system', 'SEED_GROUP'),
  ('GROUP', 'DEMAND',      'premiumization_trend',    'CONSUMER_STAPLES', 0.06, 'NEUTRAL', 'STABLE', 'system', 'SEED_GROUP');

-- Normalize CONSUMER_STAPLES: raw total = 0.88 + 0.12 = 1.00
UPDATE vs_drivers d
JOIN (
  SELECT SUM(weight) as total_weight
  FROM vs_drivers
  WHERE driver_level = 'GROUP' AND valuation_group = 'CONSUMER_STAPLES'
) t ON 1=1
SET d.weight = ROUND(d.weight / t.total_weight, 3)
WHERE d.driver_level = 'GROUP' AND d.valuation_group = 'CONSUMER_STAPLES';


-- ============================================================================
-- INDUSTRIALS (90% → 100%, add 2 new drivers → 12 total)
-- ============================================================================
INSERT INTO vs_drivers
  (driver_level, driver_category, driver_name, valuation_group, weight,
   impact_direction, trend, updated_by, source)
VALUES
  ('GROUP', 'DEMAND',      'order_book_pipeline',      'INDUSTRIALS', 0.05, 'NEUTRAL', 'STABLE', 'system', 'SEED_GROUP'),
  ('GROUP', 'REGULATORY',  'make_in_india_progress',   'INDUSTRIALS', 0.05, 'NEUTRAL', 'STABLE', 'system', 'SEED_GROUP');

-- Normalize INDUSTRIALS: raw total = 0.90 + 0.10 = 1.00
UPDATE vs_drivers d
JOIN (
  SELECT SUM(weight) as total_weight
  FROM vs_drivers
  WHERE driver_level = 'GROUP' AND valuation_group = 'INDUSTRIALS'
) t ON 1=1
SET d.weight = ROUND(d.weight / t.total_weight, 3)
WHERE d.driver_level = 'GROUP' AND d.valuation_group = 'INDUSTRIALS';


-- ============================================================================
-- REAL_ESTATE_INFRA (90% → 100%, add 2 new drivers → 11 total)
-- ============================================================================
INSERT INTO vs_drivers
  (driver_level, driver_category, driver_name, valuation_group, weight,
   impact_direction, trend, updated_by, source)
VALUES
  ('GROUP', 'DEMAND',      'affordable_housing_demand',  'REAL_ESTATE_INFRA', 0.05, 'NEUTRAL', 'STABLE', 'system', 'SEED_GROUP'),
  ('GROUP', 'DEMAND',      'toll_revenue_growth',        'REAL_ESTATE_INFRA', 0.05, 'NEUTRAL', 'STABLE', 'system', 'SEED_GROUP');

-- Normalize REAL_ESTATE_INFRA: raw total = 0.90 + 0.10 = 1.00
UPDATE vs_drivers d
JOIN (
  SELECT SUM(weight) as total_weight
  FROM vs_drivers
  WHERE driver_level = 'GROUP' AND valuation_group = 'REAL_ESTATE_INFRA'
) t ON 1=1
SET d.weight = ROUND(d.weight / t.total_weight, 3)
WHERE d.driver_level = 'GROUP' AND d.valuation_group = 'REAL_ESTATE_INFRA';


-- ============================================================================
-- Also re-normalize FINANCIALS to fix 100.1% → exactly 100%
-- ============================================================================
UPDATE vs_drivers d
JOIN (
  SELECT SUM(weight) as total_weight
  FROM vs_drivers
  WHERE driver_level = 'GROUP' AND valuation_group = 'FINANCIALS'
) t ON 1=1
SET d.weight = ROUND(d.weight / t.total_weight, 3)
WHERE d.driver_level = 'GROUP' AND d.valuation_group = 'FINANCIALS';


-- ============================================================================
-- VERIFICATION
-- ============================================================================
SELECT
  valuation_group,
  COUNT(*) as driver_count,
  ROUND(SUM(weight) * 100, 1) as total_weight_pct
FROM vs_drivers
WHERE driver_level = 'GROUP'
GROUP BY valuation_group
ORDER BY valuation_group;
