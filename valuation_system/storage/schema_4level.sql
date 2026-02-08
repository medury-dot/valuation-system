-- =============================================================================
-- 4-Level Driver Hierarchy Migration
-- Transforms from 3-level (MACRO/SECTOR/COMPANY) to 4-level:
--   MACRO (15%) -> GROUP (20%) -> SUBGROUP (35%) -> COMPANY (30%)
--
-- Run this migration script to update existing tables.
-- =============================================================================

-- 1. Update vs_drivers enum: MACRO, GROUP, SUBGROUP, COMPANY
-- Note: Need to preserve existing SECTOR data by mapping to GROUP first
ALTER TABLE vs_drivers
  MODIFY driver_level ENUM('MACRO', 'SECTOR', 'GROUP', 'SUBGROUP', 'COMPANY') NOT NULL;

-- Map existing SECTOR to GROUP
UPDATE vs_drivers SET driver_level = 'GROUP' WHERE driver_level = 'SECTOR';

-- Now restrict to only new enum values
ALTER TABLE vs_drivers
  MODIFY driver_level ENUM('MACRO', 'GROUP', 'SUBGROUP', 'COMPANY') NOT NULL;

-- Add new columns for valuation_group and valuation_subgroup
ALTER TABLE vs_drivers
  ADD COLUMN valuation_group VARCHAR(100) AFTER sector,
  ADD COLUMN valuation_subgroup VARCHAR(100) AFTER valuation_group;

-- Populate valuation_group from sector column for existing rows
UPDATE vs_drivers SET valuation_group = sector WHERE valuation_group IS NULL AND sector IS NOT NULL;

-- 2. Update vs_driver_changelog enum
ALTER TABLE vs_driver_changelog
  MODIFY driver_level ENUM('MACRO', 'SECTOR', 'GROUP', 'SUBGROUP', 'COMPANY');

UPDATE vs_driver_changelog SET driver_level = 'GROUP' WHERE driver_level = 'SECTOR';

ALTER TABLE vs_driver_changelog
  MODIFY driver_level ENUM('MACRO', 'GROUP', 'SUBGROUP', 'COMPANY');

ALTER TABLE vs_driver_changelog
  ADD COLUMN valuation_group VARCHAR(100) AFTER sector,
  ADD COLUMN valuation_subgroup VARCHAR(100) AFTER valuation_group;

UPDATE vs_driver_changelog SET valuation_group = sector WHERE valuation_group IS NULL AND sector IS NOT NULL;

-- 3. Update vs_weight_history enum
ALTER TABLE vs_weight_history
  MODIFY driver_level ENUM('MACRO', 'SECTOR', 'GROUP', 'SUBGROUP', 'COMPANY');

UPDATE vs_weight_history SET driver_level = 'GROUP' WHERE driver_level = 'SECTOR';

ALTER TABLE vs_weight_history
  MODIFY driver_level ENUM('MACRO', 'GROUP', 'SUBGROUP', 'COMPANY');

ALTER TABLE vs_weight_history
  ADD COLUMN valuation_group VARCHAR(100) AFTER sector,
  ADD COLUMN valuation_subgroup VARCHAR(100) AFTER valuation_group;

UPDATE vs_weight_history SET valuation_group = sector WHERE valuation_group IS NULL AND sector IS NOT NULL;

-- 4. Update vs_event_timeline scope enum
ALTER TABLE vs_event_timeline
  MODIFY scope ENUM('MACRO', 'SECTOR', 'GROUP', 'SUBGROUP', 'COMPANY');

UPDATE vs_event_timeline SET scope = 'GROUP' WHERE scope = 'SECTOR';

ALTER TABLE vs_event_timeline
  MODIFY scope ENUM('MACRO', 'GROUP', 'SUBGROUP', 'COMPANY');

-- 5. Add indices for new columns on vs_drivers
ALTER TABLE vs_drivers ADD INDEX idx_valuation_group (valuation_group);
ALTER TABLE vs_drivers ADD INDEX idx_valuation_subgroup (valuation_subgroup);

-- Update unique key to include new columns
ALTER TABLE vs_drivers DROP INDEX uk_driver;
ALTER TABLE vs_drivers ADD UNIQUE KEY uk_driver (driver_level, driver_name, valuation_group, valuation_subgroup, company_id);

-- 6. Create valuation_subgroup config table (mirrors vs_valuation_group_configs)
CREATE TABLE IF NOT EXISTS vs_valuation_subgroup_configs (
    id INT AUTO_INCREMENT PRIMARY KEY,
    valuation_subgroup VARCHAR(100) NOT NULL UNIQUE,
    parent_valuation_group VARCHAR(100) NOT NULL,
    display_name VARCHAR(150),
    driver_config JSON COMMENT 'Subgroup-specific driver definitions',
    terminal_assumptions JSON COMMENT 'Terminal margin/ROCE ranges',
    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    INDEX idx_parent (parent_valuation_group),
    INDEX idx_active (is_active)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- 7. Create valuation_group config table if not exists
CREATE TABLE IF NOT EXISTS vs_valuation_group_configs (
    id INT AUTO_INCREMENT PRIMARY KEY,
    valuation_group VARCHAR(100) NOT NULL UNIQUE,
    display_name VARCHAR(150),
    driver_config JSON COMMENT 'Group-level driver definitions',
    terminal_assumptions JSON COMMENT 'Terminal margin/ROCE ranges',
    porter_forces JSON COMMENT 'Porter five forces config',
    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    INDEX idx_active (is_active)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- 8. Update vs_valuation_snapshots to include subgroup drivers
ALTER TABLE vs_valuation_snapshots
  ADD COLUMN subgroup_drivers_snapshot JSON AFTER sector_drivers_snapshot;

-- =============================================================================
-- Verification queries (run after migration)
-- =============================================================================
-- Check enum updated:
-- SHOW COLUMNS FROM vs_drivers LIKE 'driver_level';
-- Expected: ENUM('MACRO','GROUP','SUBGROUP','COMPANY')

-- Check new columns exist:
-- DESCRIBE vs_drivers;
-- Should show valuation_group, valuation_subgroup columns

-- Check new table created:
-- SHOW TABLES LIKE 'vs_valuation_subgroup_configs';

-- Check data migrated:
-- SELECT driver_level, COUNT(*) FROM vs_drivers GROUP BY driver_level;
-- Should show GROUP instead of SECTOR
