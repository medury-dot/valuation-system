-- FINANCIALS GROUP Driver Enhancement
-- Add 7 new GROUP-level drivers and rebalance weights to sum to 100%
--
-- Strategy: Apply proportional reduction factor 0.877 (100/114) to all weights
-- Existing 7 drivers: 68% → 60%
-- New 7 drivers: 46% → 40%
-- Total: 100%

-- Step 1: Update existing driver weights (multiply by 0.877)
UPDATE vs_drivers
SET weight = ROUND(weight * 0.877, 3)
WHERE driver_level = 'GROUP'
  AND valuation_group = 'FINANCIALS'
  AND driver_name IN (
    'credit_growth',
    'interest_rate_cycle',
    'credit_cost_cycle',
    'gdp_growth',
    'npa_cycle',
    'regulatory_capital_norms',
    'digital_adoption'
  );

-- Step 2: Insert 7 new GROUP drivers with adjusted weights
INSERT INTO vs_drivers
  (driver_level, driver_category, driver_name, valuation_group, weight,
   impact_direction, trend, updated_by, source)
VALUES
  -- Credit to GDP ratio - lending opportunity indicator
  ('GROUP', 'MACRO_SIGNAL', 'credit_to_gdp_ratio', 'FINANCIALS', 0.070,
   'NEUTRAL', 'STABLE', 'system', 'SEED_GROUP'),

  -- System liquidity - CRR/SLR tracking
  ('GROUP', 'MACRO_SIGNAL', 'system_liquidity', 'FINANCIALS', 0.061,
   'NEUTRAL', 'STABLE', 'system', 'SEED_GROUP'),

  -- Per capita income - banking penetration driver
  ('GROUP', 'MACRO_SIGNAL', 'per_capita_income', 'FINANCIALS', 0.070,
   'NEUTRAL', 'STABLE', 'system', 'SEED_GROUP'),

  -- Rural income growth - financial inclusion
  ('GROUP', 'MACRO_SIGNAL', 'rural_income_growth', 'FINANCIALS', 0.053,
   'NEUTRAL', 'STABLE', 'system', 'SEED_GROUP'),

  -- Fintech disruption - competitive threat
  ('GROUP', 'COMPETITIVE', 'fintech_disruption', 'FINANCIALS', 0.053,
   'NEUTRAL', 'STABLE', 'system', 'SEED_GROUP'),

  -- NPA recognition norms - regulatory stringency
  ('GROUP', 'REGULATORY', 'npa_recognition_norms', 'FINANCIALS', 0.044,
   'NEUTRAL', 'STABLE', 'system', 'SEED_GROUP'),

  -- Loan to deposit ratio - system-wide liquidity metric
  ('GROUP', 'DEMAND', 'loan_to_deposit_ratio', 'FINANCIALS', 0.053,
   'NEUTRAL', 'STABLE', 'system', 'SEED_GROUP');

-- Step 3: Verify weight sum (should be ~1.00 or 100%)
SELECT
  'Weight verification' as check_name,
  SUM(weight) as total_weight,
  COUNT(*) as driver_count
FROM vs_drivers
WHERE driver_level = 'GROUP' AND valuation_group = 'FINANCIALS';

-- Step 4: Show all FINANCIALS GROUP drivers
SELECT
  driver_category,
  driver_name,
  weight,
  CONCAT(ROUND(weight * 100, 1), '%') as weight_pct
FROM vs_drivers
WHERE driver_level = 'GROUP' AND valuation_group = 'FINANCIALS'
ORDER BY weight DESC;
