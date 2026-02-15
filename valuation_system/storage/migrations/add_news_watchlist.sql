-- =============================================================================
-- Migration: Add News Watchlist Table
-- Purpose: Control which companies to scan for news (start with 10 for testing)
-- Date: 2026-02-14
-- =============================================================================

-- 1. Create the table
CREATE TABLE IF NOT EXISTS vs_news_watchlist (
    id INT AUTO_INCREMENT PRIMARY KEY,
    company_id INT NOT NULL COMMENT 'mssdb.kbapp_marketscrip.marketscrip_id',
    is_enabled BOOLEAN DEFAULT TRUE,
    priority ENUM('HIGH', 'MEDIUM', 'LOW') DEFAULT 'MEDIUM',
    scan_sources JSON COMMENT 'Optional override: specific sources for this company',
    added_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    added_by VARCHAR(100) DEFAULT 'django_admin',
    notes TEXT COMMENT 'Why this company is in the watchlist',

    UNIQUE INDEX idx_company_id (company_id),
    INDEX idx_enabled (is_enabled),
    INDEX idx_priority (priority)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
COMMENT='News scanning watchlist - controls which companies to monitor for news';

-- 2. Populate with 10 test companies (high market cap, diverse sectors)
-- Get company_ids from marketscrip first:
INSERT INTO vs_news_watchlist (company_id, is_enabled, priority, added_by, notes)
SELECT
    m.marketscrip_id,
    TRUE as is_enabled,
    'HIGH' as priority,
    'migration_2026-02-14' as added_by,
    CONCAT('Initial test batch - ', m.sector) as notes
FROM mssdb.kbapp_marketscrip m
WHERE m.symbol IN (
    'EICHERMOT',   -- Autos (Pilot company 1)
    'AETHER',      -- Chemicals (Pilot company 2)
    'SUNPHARMA',   -- Pharma
    'TCS',         -- IT Services
    'RELIANCE',    -- Oil & Gas / Retail
    'HDFCBANK',    -- Banking
    'INFY',        -- IT Services
    'ITC',         -- FMCG
    'TITAN',       -- Consumer Discretionary
    'BAJFINANCE'   -- NBFC
)
AND m.scrip_type IN ('', 'EQS')
ON DUPLICATE KEY UPDATE
    is_enabled = TRUE,
    notes = CONCAT(notes, ' | Re-enabled on 2026-02-14');

-- 3. Verify insertion
SELECT
    w.id,
    w.company_id,
    m.symbol,
    m.name,
    w.is_enabled,
    w.priority,
    w.notes
FROM vs_news_watchlist w
JOIN mssdb.kbapp_marketscrip m ON w.company_id = m.marketscrip_id
ORDER BY w.priority, m.symbol;
