# News Watchlist Setup Guide

## Overview
The news pipeline now uses a dedicated `vs_news_watchlist` table to control which companies to scan for news. This allows you to:
- Start with a small test batch (currently 10 companies)
- Enable/disable companies via Django admin
- Set priority levels (HIGH/MEDIUM/LOW)
- Scale up gradually as the pipeline stabilizes

## Current Setup (10 Test Companies)

| Symbol | Company Name | Sector | Priority |
|--------|-------------|---------|----------|
| AETHER | Aether Industries | Chemicals | HIGH |
| BAJFINANCE | Bajaj Finance | Finance | HIGH |
| EICHERMOT | Eicher Motors | Automobile | HIGH |
| HDFCBANK | HDFC Bank | Banking | HIGH |
| INFY | Infosys | IT Services | HIGH |
| ITC | ITC Limited | FMCG | HIGH |
| RELIANCE | Reliance Industries | Oil & Gas | HIGH |
| SUNPHARMA | Sun Pharma | Healthcare | HIGH |
| TCS | Tata Consultancy Services | IT Services | HIGH |
| TITAN | Titan Company | Jewellery | HIGH |

## Using Django Admin

### 1. Start Django Server
```bash
cd /Users/ram/code/rag/machai/RAGApp
python manage.py runserver
```

### 2. Access Admin Interface
- URL: http://localhost:8000/admin/
- Section: **MSSDB > News Watchlist**

### 3. Available Bulk Actions

#### Enable/Disable Companies
- Select companies using checkboxes
- Choose action: "✓ Enable selected companies" or "✗ Disable selected companies"
- Click "Go"

#### Set Priority Levels
- Select companies
- Choose action: "Set priority: HIGH/MEDIUM/LOW"
- Click "Go"

#### Add More Companies
- Use action: "Add companies from Active Companies list"
- This adds top 10 by market cap that aren't already in the watchlist

### 4. Individual Company Management
Click on any company to:
- Toggle `is_enabled` checkbox
- Change `priority` dropdown
- Add `notes` (why this company is in the watchlist)
- Configure `scan_sources` (JSON list of specific sources, optional)

## How News Scanner Uses Watchlist

### Loading Process
1. News scanner queries `vs_news_watchlist` on startup
2. Filters to `is_enabled = TRUE` companies only
3. Orders by priority (HIGH → MEDIUM → LOW)
4. Loads corresponding sectors for broader news

### Example Query
```sql
SELECT m.symbol, m.name, w.priority
FROM vs_news_watchlist w
JOIN mssdb.kbapp_marketscrip m ON w.company_id = m.marketscrip_id
WHERE w.is_enabled = TRUE
ORDER BY
    CASE w.priority
        WHEN 'HIGH' THEN 1
        WHEN 'MEDIUM' THEN 2
        WHEN 'LOW' THEN 3
    END,
    m.symbol;
```

## Adding New Companies

### Method 1: Via SQL
```sql
-- Find company_id first
SELECT marketscrip_id, symbol, name
FROM mssdb.kbapp_marketscrip
WHERE symbol = 'YOURCOMPANY'
AND scrip_type IN ('', 'EQS');

-- Add to watchlist
INSERT INTO vs_news_watchlist (company_id, is_enabled, priority, added_by, notes)
VALUES (12345, TRUE, 'MEDIUM', 'pm_manual', 'Added for Q4 results tracking');
```

### Method 2: Via Django Admin
1. Go to **MSSDB > Market Scrips**
2. Search for company by name/symbol
3. Select company
4. Use action: "Add to Valuation System + Enable GSheet Sync"
5. Then go to **News Watchlist** and enable it there

### Method 3: Bulk Import from Active Companies
1. Go to **News Watchlist**
2. Select any existing entry (just to enable the action dropdown)
3. Choose action: "Add companies from Active Companies list"
4. This adds top 10 by market cap that aren't already in watchlist

## Testing the Integration

### Run Test Script
```bash
cd /Users/ram/code/research
python valuation_system/utils/test_news_watchlist.py
```

**Expected Output:**
- Lists all companies in watchlist
- Shows NewsScannerAgent loaded companies
- Tests enable/disable filtering

### Run News Scanner
```bash
cd /Users/ram/code/research
python valuation_system/scheduler/runner.py news_scan
```

**What Happens:**
1. Scanner loads 10 companies from watchlist
2. Builds search terms (symbols + sectors)
3. Scans configured news sources
4. Stores in `vs_event_timeline` table
5. Logs to `vs_agent_activity_log`

## GSheet Sync

**Important:** The `vs_news_watchlist` table is **NOT synced to GSheet**. This is intentional:
- Prevents accidental overwrites during GSheet sync
- Keeps control in Django admin (single source of truth)
- Avoids sync conflicts

If you need a GSheet view:
- Consider a read-only tab that queries the DB
- Or export a snapshot periodically

## Scaling Up

### Gradual Expansion Plan
1. **Week 1:** 10 companies (current) - validate data quality
2. **Week 2:** Add 20 more (30 total) - monitor performance
3. **Week 3:** Add 50 more (80 total) - check for rate limits
4. **Week 4:** Expand to 200+ - full production

### Performance Considerations
- News sources have rate limits (1.5s between calls)
- More companies = longer scan time
- HIGH priority companies scanned first
- Consider splitting into multiple hourly batches

## Troubleshooting

### No Companies Loaded
```python
# Check table contents
mysql -u root rag -e "SELECT COUNT(*) FROM vs_news_watchlist WHERE is_enabled=TRUE;"
```

### Scanner Uses Fallback Companies
If scanner logs "Using fallback companies [AETHER, EICHERMOT]":
- Check database connection
- Verify table exists
- Check `is_enabled` flags

### Django Admin Not Showing News Watchlist
1. Check model is imported in `mssdb/admin.py`
2. Restart Django server
3. Clear browser cache

## File Locations

| File | Purpose |
|------|---------|
| `valuation_system/storage/schema.sql` | Table definition (line 564+) |
| `valuation_system/storage/migrations/add_news_watchlist.sql` | Migration script |
| `valuation_system/agents/news_scanner.py` | Scanner implementation |
| `/Users/ram/code/rag/machai/RAGApp/rag/models.py` | Django model (line 808+) |
| `/Users/ram/code/rag/machai/RAGApp/mssdb/admin.py` | Django admin (line 186+) |
| `valuation_system/utils/test_news_watchlist.py` | Test script |

## Next Steps

1. ✅ Table created and populated with 10 companies
2. ✅ Django admin configured with bulk actions
3. ✅ News scanner updated to query watchlist
4. ✅ Integration tested successfully
5. ⏳ **YOU ARE HERE:** Run news pipeline with 10 companies
6. ⏳ Monitor results in `vs_event_timeline`
7. ⏳ Adjust watchlist based on data quality
8. ⏳ Gradually scale up to more companies
