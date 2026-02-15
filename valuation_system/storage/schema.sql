-- =============================================================================
-- Agentic Equity Valuation System - MySQL Schema
-- Database: rag (shared with RAGApp)
-- All tables prefixed with vs_ (valuation system)
--
-- COMPANY MASTER: Uses mssdb.kbapp_marketscrip (single source of truth)
--   - company_id in all vs_* tables = marketscrip_id from mssdb.kbapp_marketscrip
--   - No FOREIGN KEY constraints (cross-database FKs not supported in MySQL)
--   - Company lookups: SELECT * FROM mssdb.kbapp_marketscrip WHERE symbol = 'EICHERMOT'
-- =============================================================================

-- 2. VALUATIONS (Individual valuation runs)
-- company_id = mssdb.kbapp_marketscrip.marketscrip_id
CREATE TABLE IF NOT EXISTS vs_valuations (
    id INT AUTO_INCREMENT PRIMARY KEY,
    company_id INT NOT NULL COMMENT 'mssdb.kbapp_marketscrip.marketscrip_id',
    valuation_date DATE NOT NULL,
    method ENUM('DCF', 'RELATIVE', 'BLENDED') NOT NULL,
    scenario ENUM('BULL', 'BASE', 'BEAR'),
    intrinsic_value DECIMAL(15,2),
    cmp DECIMAL(15,2),
    upside_pct DECIMAL(8,2),
    confidence_score DECIMAL(5,2),
    key_assumptions JSON,
    driver_snapshot JSON,
    model_version VARCHAR(50),
    created_by ENUM('AGENT', 'PM_OVERRIDE') DEFAULT 'AGENT',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_company_date (company_id, valuation_date)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- 3. VALUATION SNAPSHOTS (Point-in-time complete state)
CREATE TABLE IF NOT EXISTS vs_valuation_snapshots (
    id INT AUTO_INCREMENT PRIMARY KEY,
    company_id INT NOT NULL COMMENT 'mssdb.kbapp_marketscrip.marketscrip_id',
    snapshot_date DATE NOT NULL,
    snapshot_type ENUM('DAILY', 'WEEKLY', 'EVENT_TRIGGERED', 'PM_OVERRIDE') DEFAULT 'DAILY',

    -- Valuation outputs
    intrinsic_value_dcf DECIMAL(15,2),
    intrinsic_value_relative DECIMAL(15,2),
    intrinsic_value_blended DECIMAL(15,2),
    cmp DECIMAL(15,2),
    upside_pct DECIMAL(8,2),

    -- Scenario values
    dcf_bull DECIMAL(15,2),
    dcf_base DECIMAL(15,2),
    dcf_bear DECIMAL(15,2),

    -- Key assumptions (JSON)
    dcf_assumptions JSON,
    macro_drivers_snapshot JSON,
    sector_drivers_snapshot JSON,
    company_drivers_snapshot JSON,

    -- Context
    sector_outlook_score DECIMAL(5,2),
    sector_outlook_label VARCHAR(20),
    confidence_score DECIMAL(5,2),
    peer_multiples_snapshot JSON,

    -- Data source tracking (which inputs used actual vs estimated data)
    nwc_source ENUM('ACTUAL_CF','DERIVED_BS','DEFAULT') DEFAULT NULL COMMENT 'NWC: actual CF WC change vs BS estimation',
    shares_source ENUM('ACTUAL_COLUMN','DERIVED_MCAP','DEFAULT') DEFAULT NULL COMMENT 'Shares: actual paid-up vs MCap/CMP',
    tax_source ENUM('ACTUAL_CF','DERIVED_ACCRUAL','DEFAULT') DEFAULT NULL COMMENT 'Tax: actual cash tax vs 1-PAT/PBT',
    roce_source ENUM('ACTUAL_CE','DERIVED_NWDEBT','DEFAULT') DEFAULT NULL COMMENT 'ROCE: actual capital employed vs NW+Debt',
    reinvest_source ENUM('ACTUAL_PAYOUT','DERIVED_CAPEX','DEFAULT') DEFAULT NULL COMMENT 'Reinvestment: payout ratio vs capex/NOPAT',

    model_version VARCHAR(50),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    INDEX idx_company_date (company_id, snapshot_date)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- 4. DRIVERS (Current state)
CREATE TABLE IF NOT EXISTS vs_drivers (
    id INT AUTO_INCREMENT PRIMARY KEY,
    driver_level ENUM('MACRO', 'SECTOR', 'COMPANY') NOT NULL,
    driver_category VARCHAR(50),
    driver_name VARCHAR(100) NOT NULL,
    sector VARCHAR(100),
    company_id INT COMMENT 'mssdb.kbapp_marketscrip.marketscrip_id',
    current_value TEXT,
    weight DECIMAL(5,4),
    impact_direction ENUM('POSITIVE', 'NEGATIVE', 'NEUTRAL'),
    trend ENUM('UP', 'DOWN', 'STABLE'),
    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    updated_by VARCHAR(50),
    is_active TINYINT(1) NOT NULL DEFAULT 1,
    source VARCHAR(30) DEFAULT 'SEED',
    linked_macro_driver VARCHAR(100) DEFAULT NULL,
    link_direction ENUM('SAME','INVERSE') DEFAULT NULL,
    INDEX idx_level_sector (driver_level, sector),
    UNIQUE KEY uk_driver (driver_level, driver_name, sector, company_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- 5. DRIVER CHANGELOG (Every change tracked)
CREATE TABLE IF NOT EXISTS vs_driver_changelog (
    id INT AUTO_INCREMENT PRIMARY KEY,
    change_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    driver_level ENUM('MACRO', 'SECTOR', 'COMPANY'),
    driver_category VARCHAR(50),
    driver_name VARCHAR(100),
    sector VARCHAR(100),
    company_id INT COMMENT 'mssdb.kbapp_marketscrip.marketscrip_id',
    old_value TEXT,
    new_value TEXT,
    old_weight DECIMAL(5,4),
    new_weight DECIMAL(5,4),
    change_reason TEXT,
    triggered_by ENUM('NEWS_EVENT', 'MACRO_UPDATE', 'PM_OVERRIDE', 'SCHEDULED_REFRESH', 'AGENT_ANALYSIS'),
    source_event_id INT,
    is_active TINYINT(1) DEFAULT NULL,
    source VARCHAR(30) DEFAULT NULL,
    estimated_valuation_impact_pct DECIMAL(8,2),
    INDEX idx_timestamp (change_timestamp),
    INDEX idx_driver (driver_level, driver_name)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- 6. NEWS EVENTS (Classified news)
CREATE TABLE IF NOT EXISTS vs_news_events (
    id INT AUTO_INCREMENT PRIMARY KEY,
    company_id INT COMMENT 'mssdb.kbapp_marketscrip.marketscrip_id',
    sector VARCHAR(100),
    event_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    source VARCHAR(100),
    headline VARCHAR(500),
    summary TEXT,
    category ENUM('REGULATORY', 'MANAGEMENT', 'PRODUCT', 'MA', 'MACRO', 'COMPETITOR', 'GOVERNANCE'),
    severity ENUM('CRITICAL', 'HIGH', 'MEDIUM', 'LOW'),
    valuation_impact_pct DECIMAL(8,2),
    chromadb_doc_id VARCHAR(100),
    processed BOOLEAN DEFAULT FALSE,
    INDEX idx_severity_date (severity, event_date),
    INDEX idx_company (company_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- 7. EVENT TIMELINE (All events with document links)
CREATE TABLE IF NOT EXISTS vs_event_timeline (
    id INT AUTO_INCREMENT PRIMARY KEY,
    event_date DATE,
    event_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    event_type ENUM('NEWS', 'EARNINGS', 'MANAGEMENT_CHANGE', 'REGULATORY',
                    'POLICY', 'MACRO', 'SECTOR_DEVELOPMENT', 'COMPETITOR', 'VALUATION_UPDATE'),
    scope ENUM('MACRO', 'SECTOR', 'COMPANY'),
    sector VARCHAR(100),
    company_id INT COMMENT 'mssdb.kbapp_marketscrip.marketscrip_id',
    headline VARCHAR(500),
    summary TEXT,
    severity ENUM('CRITICAL', 'HIGH', 'MEDIUM', 'LOW'),

    -- Impact linkage
    drivers_affected JSON,
    valuation_impact_pct DECIMAL(8,2),
    valuation_before DECIMAL(15,2),
    valuation_after DECIMAL(15,2),

    -- Source & document links
    source VARCHAR(100),
    source_url TEXT,
    search_query VARCHAR(200) COMMENT 'Search term/keyword that captured this news (for dedup debugging)',
    chromadb_doc_id VARCHAR(100),
    s3_document_path TEXT,
    grok_synopsis TEXT,
    key_quotes TEXT,
    filing_type VARCHAR(50),
    filing_reference VARCHAR(100),

    -- Status
    processed BOOLEAN DEFAULT FALSE,
    pm_reviewed BOOLEAN DEFAULT FALSE,
    pm_notes TEXT,

    INDEX idx_date_scope (event_date, scope),
    INDEX idx_company_date (company_id, event_date),
    INDEX idx_chromadb (chromadb_doc_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- 8. DOCUMENTS (Metadata registry - content in ChromaDB)
CREATE TABLE IF NOT EXISTS vs_documents (
    id INT AUTO_INCREMENT PRIMARY KEY,
    doc_type ENUM('NEWS_ARTICLE', 'BSE_FILING', 'NSE_FILING', 'ANNUAL_REPORT',
                  'INVESTOR_PRESENTATION', 'CON_CALL_TRANSCRIPT', 'ANALYST_REPORT',
                  'RESEARCH_NOTE', 'REGULATORY_FILING', 'CREDIT_RATING'),
    chromadb_collection VARCHAR(100) DEFAULT 'RAG_GPT',
    chromadb_doc_id VARCHAR(100) UNIQUE,
    s3_path TEXT,
    title VARCHAR(500),
    source VARCHAR(100),
    source_url TEXT,
    publish_date DATE,
    grok_summary TEXT,
    key_metrics_extracted JSON,
    company_id INT COMMENT 'mssdb.kbapp_marketscrip.marketscrip_id',
    sector VARCHAR(100),
    valuation_relevant BOOLEAN DEFAULT TRUE,
    relevance_score DECIMAL(3,2),
    indexed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    last_accessed TIMESTAMP,
    INDEX idx_company_type (company_id, doc_type),
    INDEX idx_chromadb (chromadb_doc_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- 9. ALERTS
CREATE TABLE IF NOT EXISTS vs_alerts (
    id INT AUTO_INCREMENT PRIMARY KEY,
    company_id INT NOT NULL COMMENT 'mssdb.kbapp_marketscrip.marketscrip_id',
    alert_type ENUM('VALUATION_CHANGE', 'NEWS_EVENT', 'DRIVER_CHANGE') NOT NULL,
    trigger_reason TEXT,
    old_value DECIMAL(15,2),
    new_value DECIMAL(15,2),
    change_pct DECIMAL(8,2),
    sent_at TIMESTAMP,
    acknowledged_at TIMESTAMP,
    pm_action VARCHAR(50),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_company_date (company_id, created_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- 10. MODEL HISTORY (Audit trail)
CREATE TABLE IF NOT EXISTS vs_model_history (
    id INT AUTO_INCREMENT PRIMARY KEY,
    company_id INT COMMENT 'mssdb.kbapp_marketscrip.marketscrip_id',
    change_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    change_type VARCHAR(50),
    field_changed VARCHAR(100),
    old_value TEXT,
    new_value TEXT,
    changed_by VARCHAR(50),
    reason TEXT,
    INDEX idx_company_date (company_id, change_date)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- 11. WEIGHT HISTORY
CREATE TABLE IF NOT EXISTS vs_weight_history (
    id INT AUTO_INCREMENT PRIMARY KEY,
    effective_date DATE,
    driver_level ENUM('MACRO', 'SECTOR', 'COMPANY'),
    driver_name VARCHAR(100),
    sector VARCHAR(100),
    weight DECIMAL(5,4),
    weight_rationale TEXT,
    set_by ENUM('INITIAL', 'LEARNED', 'PM_OVERRIDE'),
    correlation_with_returns DECIMAL(5,4),
    sample_period_start DATE,
    sample_period_end DATE,
    INDEX idx_date_driver (effective_date, driver_name)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- 12. MODEL VERSIONS
CREATE TABLE IF NOT EXISTS vs_model_versions (
    id INT AUTO_INCREMENT PRIMARY KEY,
    version VARCHAR(50) UNIQUE,
    release_date DATE,
    description TEXT,
    changes_from_previous TEXT,
    dcf_methodology TEXT,
    relative_valuation_rules TEXT,
    blending_weights JSON,
    backtest_accuracy DECIMAL(5,2),
    live_accuracy DECIMAL(5,2),
    is_active BOOLEAN DEFAULT TRUE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- 13. PM FEEDBACK
CREATE TABLE IF NOT EXISTS vs_pm_feedback (
    id INT AUTO_INCREMENT PRIMARY KEY,
    valuation_id INT,
    pm_decision ENUM('AGREE', 'OVERRIDE', 'REJECT'),
    pm_valuation DECIMAL(15,2),
    pm_notes TEXT,
    actual_outcome DECIMAL(15,2),
    outcome_date DATE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- 14. PM INTERACTIONS (All PM decisions for learning)
CREATE TABLE IF NOT EXISTS vs_pm_interactions (
    id INT AUTO_INCREMENT PRIMARY KEY,
    interaction_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    company_id INT COMMENT 'mssdb.kbapp_marketscrip.marketscrip_id',
    sector VARCHAR(100),
    interaction_type ENUM('VALUATION_OVERRIDE', 'DRIVER_OVERRIDE', 'WEIGHT_CHANGE',
                          'ALERT_ACKNOWLEDGE', 'ALERT_DISMISS', 'FEEDBACK'),
    agent_recommendation TEXT,
    pm_decision TEXT,
    pm_reasoning TEXT,
    outcome_tracked BOOLEAN DEFAULT FALSE,
    outcome_date DATE,
    outcome_result TEXT,
    agent_was_correct BOOLEAN
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- 15. SOCIAL MEDIA POSTS (Tracking)
CREATE TABLE IF NOT EXISTS vs_social_posts (
    id INT AUTO_INCREMENT PRIMARY KEY,
    platform ENUM('twitter', 'linkedin') DEFAULT 'twitter',
    content TEXT NOT NULL,
    category VARCHAR(50),
    status ENUM('queued', 'approved', 'posted', 'rejected') DEFAULT 'queued',
    source_event_id INT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    posted_at TIMESTAMP NULL,
    engagement_likes INT DEFAULT 0,
    engagement_retweets INT DEFAULT 0,
    engagement_replies INT DEFAULT 0,
    INDEX idx_status (status),
    INDEX idx_created (created_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- 16. PEER GROUPS (Two-tier algorithmic peer selection for relative valuation)
-- company_id and peer_company_id = mssdb.kbapp_marketscrip.marketscrip_id
-- Sector/industry from mssdb; financial metrics from core CSV + prices CSV
CREATE TABLE IF NOT EXISTS vs_peer_groups (
    id INT AUTO_INCREMENT PRIMARY KEY,
    company_id INT NOT NULL COMMENT 'target company - mssdb marketscrip_id',
    peer_company_id INT NOT NULL COMMENT 'peer company - mssdb marketscrip_id',
    peer_symbol VARCHAR(50) NOT NULL,
    peer_name VARCHAR(255),
    tier ENUM('tight', 'broad') NOT NULL COMMENT 'tight=same industry, broad=same sector',
    similarity_score DECIMAL(5,4) COMMENT '0-1, higher=more similar',
    mcap_ratio DECIMAL(8,4) COMMENT 'peer_mcap / target_mcap',
    computed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    valid_until DATE NOT NULL COMMENT 'cache expiry, typically +30 days',
    is_pm_override BOOLEAN DEFAULT FALSE,
    pm_notes TEXT,
    UNIQUE KEY uq_pair (company_id, peer_company_id),
    INDEX idx_company_valid (company_id, valid_until),
    INDEX idx_tier (company_id, tier)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- 17. SUBGROUP PEER STATS (Cached peer statistics for quality score calculations)
-- Pre-computed medians by valuation_subgroup to avoid on-the-fly peer loading
-- Refreshed async after each valuation + weekly batch job
CREATE TABLE IF NOT EXISTS vs_subgroup_peer_stats (
    id INT AUTO_INCREMENT PRIMARY KEY,
    valuation_subgroup VARCHAR(100) NOT NULL,
    peer_count INT DEFAULT 0 COMMENT 'Number of active companies in this subgroup',
    median_roce DECIMAL(6,4) COMMENT 'Median ROCE across peers (0.15 = 15%)',
    median_revenue_cagr DECIMAL(6,4) COMMENT 'Median 5yr revenue CAGR',
    median_de_ratio DECIMAL(6,4) COMMENT 'Median debt/equity ratio',
    median_promoter_pledge DECIMAL(6,4) COMMENT 'Median promoter pledge %',
    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    UNIQUE KEY idx_subgroup (valuation_subgroup),
    INDEX idx_updated (last_updated)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- 19. DISCOVERED DRIVERS (Agent-suggested new drivers, pending PM approval)
CREATE TABLE IF NOT EXISTS vs_discovered_drivers (
    id INT AUTO_INCREMENT PRIMARY KEY,
    discovered_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    driver_level ENUM('MACRO','GROUP','SUBGROUP') NOT NULL,
    valuation_group VARCHAR(100),
    valuation_subgroup VARCHAR(100),
    driver_category VARCHAR(50),
    driver_name VARCHAR(100) NOT NULL,
    suggested_weight DECIMAL(5,4),
    reasoning TEXT,
    source_event_id INT,
    source_headline TEXT,
    confidence ENUM('HIGH','MEDIUM','LOW') DEFAULT 'MEDIUM',
    status ENUM('PENDING','APPROVED','REJECTED') DEFAULT 'PENDING',
    reviewed_by VARCHAR(50),
    reviewed_at TIMESTAMP NULL,
    INDEX idx_status (status),
    INDEX idx_group (valuation_group, valuation_subgroup)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- 20. MATERIALITY ALERTS (Opportunity/risk signals from driver analysis)
CREATE TABLE IF NOT EXISTS vs_materiality_alerts (
    id INT AUTO_INCREMENT PRIMARY KEY,
    alert_date DATE,
    alert_type ENUM('MACRO_DIVERGENCE','DRIVER_MOMENTUM','VALUATION_GAP','CROSS_SIGNAL') NOT NULL,
    valuation_group VARCHAR(100),
    valuation_subgroup VARCHAR(100),
    signal_description TEXT,
    affected_companies INT,
    suggested_action ENUM('REVALUE_NOW','WATCH','REDUCE_EXPOSURE'),
    severity ENUM('HIGH','MEDIUM','LOW'),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_date_type (alert_date, alert_type),
    INDEX idx_group (valuation_group)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- 19. NSE FETCH TRACKER (State tracking for NSE filing data fetches)
CREATE TABLE IF NOT EXISTS vs_nse_fetch_tracker (
    id INT AUTO_INCREMENT PRIMARY KEY,
    company_id INT NOT NULL COMMENT 'mssdb.kbapp_marketscrip.marketscrip_id',
    nse_symbol VARCHAR(20) NOT NULL,

    -- Latest data we have
    latest_quarter_end DATE COMMENT 'e.g., 2024-12-31 = FY2025Q3',
    latest_quarter_idx INT COMMENT 'e.g., 147 (matches core CSV convention)',
    result_type CHAR(1) COMMENT 'U=unaudited, A=audited',
    filing_date DATE COMMENT 'when company filed to NSE',

    -- Fetch metadata
    last_fetch_date DATETIME COMMENT 'when we last called the API',
    last_fetch_status ENUM('SUCCESS','FAILED','NO_DATA') DEFAULT 'SUCCESS',
    quarters_available INT DEFAULT 0 COMMENT 'how many quarters returned',
    data_hash VARCHAR(32) COMMENT 'MD5 of results JSON (detect changes)',

    -- XBRL tracking
    xbrl_url VARCHAR(512) COMMENT 'URL for segment/balance sheet parsing',
    xbrl_parsed TINYINT DEFAULT 0 COMMENT '0=not parsed, 1=parsed',
    has_segments TINYINT DEFAULT 0 COMMENT '1 if multi-segment company',

    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,

    UNIQUE KEY uq_symbol (nse_symbol),
    INDEX idx_quarter (latest_quarter_end),
    INDEX idx_fetch_date (last_fetch_date),
    INDEX idx_status (last_fetch_status),
    INDEX idx_company (company_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='NSE filing fetch state tracking for event-driven updates';

-- 20. COMPANY SEGMENTS (Canonical segment names per company)
CREATE TABLE IF NOT EXISTS vs_company_segments (
    id INT AUTO_INCREMENT PRIMARY KEY,
    company_id INT NOT NULL COMMENT 'mssdb.kbapp_marketscrip.marketscrip_id',
    segment_name VARCHAR(100) NOT NULL COMMENT 'e.g., "Royal Enfield", "VECV"',
    segment_type ENUM('BUSINESS','GEOGRAPHIC') DEFAULT 'BUSINESS',
    mapped_subgroup VARCHAR(50) COMMENT 'valuation_subgroup for this segment',
    revenue_share_pct DECIMAL(5,2) COMMENT 'latest known % of total revenue',
    is_active TINYINT DEFAULT 1,
    source ENUM('XBRL','MANUAL','LLM_SUGGESTED') DEFAULT 'XBRL',
    pm_approved TINYINT DEFAULT 0 COMMENT 'PM must approve segment→subgroup mapping',
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,

    UNIQUE KEY uq_company_segment (company_id, segment_name),
    INDEX idx_subgroup (mapped_subgroup),
    INDEX idx_company (company_id),
    INDEX idx_approval (pm_approved)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='Segment master for SOTP valuation';

-- 21. SEGMENT FINANCIALS (Quarterly segment financial data)
CREATE TABLE IF NOT EXISTS vs_segment_financials (
    id INT AUTO_INCREMENT PRIMARY KEY,
    company_id INT NOT NULL COMMENT 'mssdb.kbapp_marketscrip.marketscrip_id',
    segment_id INT NOT NULL COMMENT 'FK to vs_company_segments.id',
    quarter_idx INT NOT NULL COMMENT 'same convention as core CSV (147=FY2025Q3)',

    revenue_cr DECIMAL(12,2) COMMENT 'segment revenue in crores',
    profit_cr DECIMAL(12,2) COMMENT 'segment profit (EBIT or PBT)',
    assets_cr DECIMAL(12,2) COMMENT 'segment assets',
    liabilities_cr DECIMAL(12,2) COMMENT 'segment liabilities',
    capex_cr DECIMAL(12,2) COMMENT 'segment capex',
    depreciation_cr DECIMAL(12,2),

    data_source ENUM('XBRL','MANUAL') DEFAULT 'XBRL',
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,

    UNIQUE KEY uq_seg_quarter (company_id, segment_id, quarter_idx),
    INDEX idx_quarter (quarter_idx),
    INDEX idx_segment (segment_id),
    INDEX idx_company (company_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='Quarterly segment financials from XBRL';

-- 19. NSE FETCH TRACKER (State tracking for incremental NSE API fetching)
-- Tracks which companies have been fetched, when, and what data we have.
-- Enables event-driven fetching: only re-fetch when new filings are detected.
CREATE TABLE IF NOT EXISTS vs_nse_fetch_tracker (
    id INT AUTO_INCREMENT PRIMARY KEY,
    company_id INT NOT NULL COMMENT 'mssdb.kbapp_marketscrip.marketscrip_id',
    nse_symbol VARCHAR(20) NOT NULL,

    -- Latest data we have
    latest_quarter_end DATE COMMENT 'e.g., 2024-12-31 = FY2025Q3',
    latest_quarter_idx INT COMMENT 'e.g., 147 (matches core CSV convention)',
    result_type CHAR(1) COMMENT 'U=unaudited, A=audited',
    filing_date DATE COMMENT 'when company filed to NSE',

    -- Fetch metadata
    last_fetch_date DATETIME COMMENT 'when we last called the API',
    last_fetch_status ENUM('SUCCESS','FAILED','NO_DATA') DEFAULT 'SUCCESS',
    quarters_available INT DEFAULT 0 COMMENT 'how many quarters returned',
    data_hash VARCHAR(32) COMMENT 'MD5 of results JSON (detect changes)',

    -- XBRL tracking (Phase 2)
    xbrl_url VARCHAR(512) COMMENT 'URL for segment/balance sheet parsing',
    xbrl_parsed TINYINT DEFAULT 0 COMMENT '0=not parsed, 1=parsed',
    has_segments TINYINT DEFAULT 0 COMMENT '1 if multi-segment company',

    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,

    UNIQUE KEY uq_symbol (nse_symbol),
    INDEX idx_company (company_id),
    INDEX idx_quarter (latest_quarter_end),
    INDEX idx_fetch_date (last_fetch_date),
    INDEX idx_status (last_fetch_status)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- 20. COMPANY SEGMENTS (Canonical segment names per company, from XBRL)
-- Phase 2: Segment data for SOTP valuation of conglomerates.
CREATE TABLE IF NOT EXISTS vs_company_segments (
    id INT AUTO_INCREMENT PRIMARY KEY,
    company_id INT NOT NULL COMMENT 'mssdb.kbapp_marketscrip.marketscrip_id',
    segment_name VARCHAR(100) NOT NULL COMMENT 'e.g., Royal Enfield, VECV',
    segment_type ENUM('BUSINESS','GEOGRAPHIC') DEFAULT 'BUSINESS',
    mapped_subgroup VARCHAR(50) COMMENT 'our valuation_subgroup for this segment',
    revenue_share_pct DECIMAL(5,2) COMMENT 'latest known pct of total revenue',
    is_active TINYINT DEFAULT 1,
    source ENUM('XBRL','MANUAL','LLM_SUGGESTED') DEFAULT 'XBRL',
    pm_approved TINYINT DEFAULT 0 COMMENT 'PM must approve segment->subgroup mapping',
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,

    UNIQUE KEY uq_company_segment (company_id, segment_name),
    INDEX idx_subgroup (mapped_subgroup)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- 21. SEGMENT FINANCIALS (Quarterly segment P&L from XBRL)
CREATE TABLE IF NOT EXISTS vs_segment_financials (
    id INT AUTO_INCREMENT PRIMARY KEY,
    company_id INT NOT NULL COMMENT 'mssdb.kbapp_marketscrip.marketscrip_id',
    segment_id INT NOT NULL COMMENT 'FK to vs_company_segments.id',
    quarter_idx INT NOT NULL COMMENT 'same convention as core CSV (147=FY2025Q3)',

    revenue_cr DECIMAL(12,2) COMMENT 'segment revenue in crores',
    profit_cr DECIMAL(12,2) COMMENT 'segment profit (EBIT or PBT)',
    assets_cr DECIMAL(12,2) COMMENT 'segment assets',
    liabilities_cr DECIMAL(12,2) COMMENT 'segment liabilities',
    capex_cr DECIMAL(12,2) COMMENT 'segment capex',
    depreciation_cr DECIMAL(12,2),

    data_source ENUM('XBRL','MANUAL') DEFAULT 'XBRL',
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,

    UNIQUE KEY uq_seg_quarter (company_id, segment_id, quarter_idx),
    INDEX idx_quarter (quarter_idx)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- =============================================================================
-- AGENT ACTIVITY LOG (Operational monitoring)
-- =============================================================================
-- Tracks agent execution cycles with structured metrics
-- Replaces GSheet Tab 10 - designed for high-frequency logging (hourly + daily cycles)
-- View via Django admin or direct SQL queries
CREATE TABLE IF NOT EXISTS vs_agent_activity_log (
    id INT AUTO_INCREMENT PRIMARY KEY,
    timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
    agent_name VARCHAR(50) NOT NULL COMMENT 'NewsScannerAgent, OrchestratorAgent, etc.',
    cycle_type VARCHAR(50) COMMENT 'hourly, daily, weekly, news_scan, etc.',
    action VARCHAR(100) NOT NULL COMMENT 'cycle_start, cycle_complete, source_scanned, etc.',

    -- Structured metrics (JSON for flexibility)
    metrics JSON COMMENT 'Agent-specific metrics: articles_scanned, companies_valued, etc.',

    elapsed_ms INT COMMENT 'Execution time in milliseconds',
    status ENUM('SUCCESS','FAILED','SKIPPED','IN_PROGRESS') DEFAULT 'SUCCESS',
    error_message TEXT COMMENT 'Error details if status=FAILED',

    -- Fast lookups
    INDEX idx_timestamp (timestamp),
    INDEX idx_agent_status (agent_name, status),
    INDEX idx_cycle (cycle_type, timestamp)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
COMMENT='Agent execution activity log - high-frequency operational data';

-- 20. NEWS WATCHLIST (Controls which companies to scan for news)
-- Managed via Django admin with bulk actions
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

-- =============================================================================
-- SEED DATA: Initial model version
-- =============================================================================
INSERT IGNORE INTO vs_model_versions (version, release_date, description, blending_weights, is_active)
VALUES (
    'v1.0.0',
    CURDATE(),
    'Initial release: FCFF DCF + Relative Valuation + Monte Carlo',
    '{"dcf": 0.60, "relative": 0.30, "monte_carlo": 0.10}',
    TRUE
);
