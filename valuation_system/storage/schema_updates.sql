-- =============================================================================
-- Schema Updates for Scaling to 1000 Companies
-- Phase 1: Database Migration
-- =============================================================================

-- Table 1: Update vs_active_companies (align with gem classification)
-- Add columns for gem integration, valuation grouping, and alpha configs
-- Note: Run each ALTER separately; ignore duplicate column/index errors on re-run

ALTER TABLE vs_active_companies
ADD COLUMN alpha_config_id INT DEFAULT NULL COMMENT 'FK to vs_company_alpha_configs',
ADD COLUMN csv_name VARCHAR(255) DEFAULT NULL COMMENT 'Name as appears in core CSV',
ADD COLUMN accord_code VARCHAR(50) DEFAULT NULL COMMENT 'Link to gem file and mssdb',
ADD COLUMN bse_code VARCHAR(50) DEFAULT NULL COMMENT 'BSE scrip code',
ADD COLUMN valuation_group VARCHAR(100) DEFAULT NULL COMMENT 'Top-level sector (13 groups)',
ADD COLUMN valuation_subgroup VARCHAR(100) DEFAULT NULL COMMENT 'Industry subgroup (40+ subgroups)',
ADD COLUMN cd_sector VARCHAR(100) DEFAULT NULL COMMENT 'Original CD_Sector from gem',
ADD COLUMN cd_industry VARCHAR(100) DEFAULT NULL COMMENT 'Original CD_Industry1 from gem';

ALTER TABLE vs_active_companies ADD INDEX idx_valuation_group (valuation_group);
ALTER TABLE vs_active_companies ADD INDEX idx_valuation_subgroup (valuation_subgroup);
ALTER TABLE vs_active_companies ADD INDEX idx_accord_code (accord_code);
ALTER TABLE vs_active_companies ADD INDEX idx_alpha_config (alpha_config_id);

-- Table 2: Company-specific alpha configurations (optional overrides)
CREATE TABLE IF NOT EXISTS vs_company_alpha_configs (
    id INT AUTO_INCREMENT PRIMARY KEY,
    company_id INT NOT NULL COMMENT 'marketscrip_id from mssdb',
    thesis_bull TEXT COMMENT 'Bull case thesis',
    thesis_bear TEXT COMMENT 'Bear case thesis',
    thesis_key_moat TEXT COMMENT 'Key competitive moat',
    alpha_drivers JSON COMMENT 'Company-specific driver overrides',
    sector_overrides JSON COMMENT 'Company-specific toggles',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    created_by VARCHAR(100) DEFAULT 'system',
    notes TEXT,
    UNIQUE KEY uk_company (company_id),
    INDEX idx_company (company_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
COMMENT 'Optional company-specific configurations that override sector defaults';

-- Table 3: Valuation group driver configs (replaces per-sector YAML)
CREATE TABLE IF NOT EXISTS vs_valuation_group_configs (
    id INT AUTO_INCREMENT PRIMARY KEY,
    valuation_group VARCHAR(100) NOT NULL UNIQUE COMMENT '13 groups: MATERIALS_CHEMICALS, AUTO, etc.',
    driver_config JSON COMMENT 'Demand, cost, regulatory drivers with weights',
    porter_forces JSON COMMENT 'Entry barriers, supplier power, substitutes, etc.',
    terminal_assumptions JSON COMMENT 'Margin range, ROCE, reinvestment rate for DCF',
    is_active BOOLEAN DEFAULT TRUE COMMENT 'Enable/disable entire group',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    notes TEXT COMMENT 'Sector-specific notes',
    INDEX idx_active (is_active),
    INDEX idx_group (valuation_group)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
COMMENT 'Driver configurations for 13 top-level valuation groups';

-- Seed valuation_group_configs with 13 groups from gem data
-- investable=True counts from sector-industry-vertical-feb2026.xlsx
INSERT INTO vs_valuation_group_configs (valuation_group, is_active, notes)
VALUES
    ('MATERIALS_CHEMICALS', TRUE, '201 investable companies, 267 specialty + 83 commodity'),
    ('AUTO', TRUE, '185 investable, 274 ancillary + 23 OEM'),
    ('TECHNOLOGY', TRUE, '143 investable, 361 IT services + 49 BPO + 31 digital infra'),
    ('FINANCIALS', TRUE, '227 investable, diverse NBFC/banking/asset mgmt'),
    ('HEALTHCARE', TRUE, '165 investable, 257 pharma export + 110 hospitals/equipment'),
    ('INDUSTRIALS', TRUE, '256 investable, 282 capital goods + 166 electricals'),
    ('CONSUMER_STAPLES', TRUE, '221 investable, 186 food/beverage + 138 FMCG + 137 agri'),
    ('CONSUMER_DISCRETIONARY', TRUE, '204 investable, 310 textile + 84 durables + 81 retail'),
    ('REAL_ESTATE_INFRA', TRUE, '265 investable, 406 construction + 236 residential'),
    ('MATERIALS_METALS', TRUE, '156 investable, 237 steel + 51 non-ferrous'),
    ('SERVICES', TRUE, '79 investable, 137 hospitality + 39 media + 19 telecom'),
    ('ENERGY_UTILITIES', TRUE, '58 investable, 59 power + 41 oil/gas'),
    ('NOT_CLASSIFIED', FALSE, '0 investable (excluded from analysis)')
ON DUPLICATE KEY UPDATE
    is_active = VALUES(is_active),
    notes = VALUES(notes),
    updated_at = CURRENT_TIMESTAMP;

-- Verification queries
SELECT 'Schema updates completed. Verification:' AS status;
SELECT COUNT(*) AS total_groups, SUM(is_active) AS active_groups
FROM vs_valuation_group_configs;

-- Show table structures
SELECT 'vs_active_companies new columns:' AS info;
DESCRIBE vs_active_companies;

SELECT 'vs_company_alpha_configs structure:' AS info;
DESCRIBE vs_company_alpha_configs;

SELECT 'vs_valuation_group_configs structure:' AS info;
DESCRIBE vs_valuation_group_configs;
