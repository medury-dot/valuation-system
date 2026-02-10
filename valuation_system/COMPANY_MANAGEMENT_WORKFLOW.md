# Company Management Workflow - Valuation System

## Overview

The valuation system uses a two-tier company management approach:
1. **Market Scrips** (mssdb) - Read-only master reference (16,961 companies)
2. **Active Companies** (rag) - Working set with full control (2,912 companies)

---

## 📋 STEP 1: Browse Market Scrips (Read-Only Master Data)

**Location:** http://localhost:8000/admin/mssdb/kbappmarketscrip/

**Database:** `mssdb.kbapp_marketscrip` (external, read-only)

**Total Records:** 16,961 companies

### Features

- ✅ **Read-only view** - Cannot edit, add, or delete records (external master data)
- ✅ **Rich filtering:**
  - Cap Class (Large Cap, Mid Cap, Small Cap, etc.)
  - MCap Range (>10K Cr, 5K-10K, 2.5K-5K, 1K-2.5K, 500-1K, <500 Cr)
  - Sector
  - Industry
  - Scrip Type
- ✅ **Search:** Name, Symbol, Alternate Symbol, Scrip Code, ISIN Number, Accord Code
- ✅ **"In VS" column** - Green checkmark shows if company already exists in valuation system

### Bulk Actions Available

#### 1. Add to Valuation System + Enable GSheet Sync
Creates record in `vs_active_companies` with:
```
is_active = 1
is_gsheet_sync = 1
added_by = 'django_admin'
added_date = CURDATE()
```
**Use when:** You want the company in batch valuations AND visible in Google Sheets

#### 2. Add to Valuation System (Local Only)
Creates record in `vs_active_companies` with:
```
is_active = 1
is_gsheet_sync = 0
added_by = 'django_admin'
added_date = CURDATE()
```
**Use when:** You want the company in batch valuations but NOT in Google Sheets (keeps GSheet clean)

### Smart Duplicate Handling

- If company **already exists** and you use "Add with GSheet Sync":
  - Updates existing record: `is_gsheet_sync = 1`
  - Shows message: "Added 0 companies (1 already existed)"

- If company **already exists** and you use "Add Local Only":
  - Skips update (no change)
  - Shows message: "Added 0 companies (1 already existed)"

---

## 🔧 STEP 2: Manage Active Companies (Full CRUD)

**Location:** http://localhost:8000/admin/valuation_system/vsactivecompanies/

**Database:** `rag.vs_active_companies` (valuation system)

**Current Records:** 2,912 companies (962 with `is_gsheet_sync=1`)

### Features

- ✅ **Full edit/update permissions** - Modify any field
- ✅ **Inline editing** - Click company to edit all details
- ✅ **Rich filtering:**
  - Active status (`is_active`)
  - GSheet Sync status (`is_gsheet_sync`)
  - Valuation Group
  - Valuation Subgroup
  - Sector
  - Industry
  - Priority (1-10)
- ✅ **Search:** Company Name, NSE Symbol, BSE Code, Accord Code, CSV Name, Company ID

### Bulk Actions Available

#### 1. Enable GSheet Sync
```sql
UPDATE vs_active_companies SET is_gsheet_sync = 1 WHERE id IN (...)
```
**Use when:** You want to start syncing selected companies to Google Sheets

#### 2. Disable GSheet Sync
```sql
UPDATE vs_active_companies SET is_gsheet_sync = 0 WHERE id IN (...)
```
**Use when:** You want to stop syncing selected companies to Google Sheets (e.g., small cap cleanup)

#### 3. Activate Companies
```sql
UPDATE vs_active_companies SET is_active = 1 WHERE id IN (...)
```
**Use when:** Reactivating previously deactivated companies

#### 4. Deactivate Companies (Soft Delete)
```sql
UPDATE vs_active_companies SET is_active = 0 WHERE id IN (...)
```
**Use when:** Temporarily removing companies from batch valuation
- **Preserves all child data:** drivers, valuations, events, alerts
- **No cascade delete:** All historical data intact
- **Reversible:** Can reactivate anytime with bulk action

---

## 🔄 DATA FLOW

```
┌─────────────────────────────────────────────────┐
│ mssdb.kbapp_marketscrip                        │
│ (16,961 companies - Read Only)                 │
│                                                 │
│ Filters: Cap Class, MCap, Sector, Industry     │
└─────────────────────┬───────────────────────────┘
                      │
                      │ [SELECT + Bulk Action]
                      ↓
┌─────────────────────────────────────────────────┐
│ rag.vs_active_companies                        │
│ (2,912 companies - Full Control)               │
│                                                 │
│ • is_active = 1/0                              │
│ • is_gsheet_sync = 1/0                         │
│ • valuation_group, valuation_subgroup          │
│ • sector, industry, priority                   │
└─────────────────────┬───────────────────────────┘
                      │
                      │ [is_active = 1]
                      ↓
┌─────────────────────────────────────────────────┐
│ Batch Valuation Pipeline                       │
│ (2,912 companies processed)                    │
│                                                 │
│ • Financial data processing                    │
│ • DCF, Relative, Monte Carlo valuation         │
│ • Excel report generation                      │
│ • Database saves (vs_valuations)               │
└─────────────────────┬───────────────────────────┘
                      │
                      │ [is_gsheet_sync = 1]
                      ↓
┌─────────────────────────────────────────────────┐
│ Google Sheets Sync                              │
│ (962 companies synced)                          │
│                                                 │
│ • Tab 4: Company Drivers                       │
│ • Tab 6: Active Companies (all 2,912 shown)    │
│ • Tab 8: News Events (GSheet companies only)   │
│ • Tab 9: Materiality Alerts (GSheet co's only) │
└─────────────────────────────────────────────────┘
```

---

## 📊 GOOGLE SHEETS SYNC BEHAVIOR

### Tab 4: Company Drivers
**Filter:** `is_gsheet_sync = 1`

Shows COMPANY-level drivers only for companies with `is_gsheet_sync = 1`

**Why:** Keeps GSheet manageable (962 companies vs 2,912)

**Query:**
```sql
SELECT d.*, a.nse_symbol, a.company_name
FROM vs_drivers d
LEFT JOIN vs_active_companies a ON d.company_id = a.company_id
WHERE d.driver_level = 'COMPANY'
  AND a.is_gsheet_sync = 1
```

### Tab 6: Active Companies
**Filter:** Shows ALL active companies

Displays all 2,912 companies with status columns:
- `Active`: YES/NO (`is_active`)
- `GSheet Sync`: YES/NO (`is_gsheet_sync`)

**Why:** PM needs visibility of entire working set

**Query:**
```sql
SELECT company_id, nse_symbol, company_name,
       valuation_group, valuation_subgroup,
       cd_sector, cd_industry,
       is_active, is_gsheet_sync
FROM vs_active_companies
ORDER BY valuation_group, valuation_subgroup, company_name
```

### Tab 8: News Events
**Filter:** Company-level events for `is_gsheet_sync = 1` ONLY

Shows:
- ALL MACRO-level events (no company_id)
- Company-level events ONLY for `is_gsheet_sync = 1` companies

**Why:** Prevents GSheet clutter from thousands of small-cap news items

**Query:**
```sql
SELECT e.*, ac.nse_symbol, ac.valuation_subgroup
FROM vs_event_timeline e
LEFT JOIN vs_active_companies ac ON e.company_id = ac.company_id
WHERE e.event_date >= DATE_SUB(CURDATE(), INTERVAL 7 DAY)
  AND e.semantic_group_id = e.id
  AND (e.company_id IS NULL OR ac.is_gsheet_sync = 1)
ORDER BY FIELD(e.severity, 'CRITICAL', 'HIGH', 'MEDIUM', 'LOW'),
         e.event_timestamp DESC
LIMIT 200
```

### Tab 9: Materiality Dashboard
**Filter:** Company-level alerts for `is_gsheet_sync = 1` ONLY

Shows:
- ALL MACRO/GROUP/SUBGROUP alerts
- Company-level alerts ONLY for `is_gsheet_sync = 1` companies

**Why:** PM focuses on material alerts for tracked companies

**Query:**
```sql
SELECT ma.*, ac.nse_symbol
FROM vs_materiality_alerts ma
LEFT JOIN vs_active_companies ac ON ma.company_id = ac.company_id
WHERE ma.created_at >= DATE_SUB(NOW(), INTERVAL 7 DAY)
  AND (ma.company_id IS NULL OR ac.is_gsheet_sync = 1)
ORDER BY FIELD(ma.severity, 'CRITICAL', 'HIGH', 'MEDIUM', 'LOW'),
         ma.created_at DESC
```

---

## 💡 KEY DESIGN DECISIONS

### 1. Two-Tier Architecture
**Market Scrips (mssdb)** = External master data
**Active Companies (rag)** = Valuation system working set

**Why:**
- Market scrips is shared across multiple applications
- Valuation system needs its own taxonomy (valuation_group/subgroup)
- Cannot pollute external master with valuation-specific fields

### 2. is_active Flag (Batch Valuation Control)
Controls whether company is included in batch valuation runs

**Why:**
- Soft delete preserves all historical data (drivers, valuations, events)
- Can reactivate without losing context
- No foreign key cascade issues (cross-DB design)

### 3. is_gsheet_sync Flag (GSheet Clutter Prevention)
Controls whether company appears in Google Sheets Tabs 4, 8, 9

**Why:**
- 2,912 companies × 8 drivers/company = 23,296 rows in Tab 4
- PM cannot review 23K rows in GSheet
- 962 companies (MCap >= 2,500 Cr) × 8 drivers = 7,696 rows (manageable)
- Can adjust threshold dynamically via Django admin bulk actions

### 4. Separate Control for Valuation vs GSheet
`is_active` and `is_gsheet_sync` are independent flags

**Use cases:**
- `is_active=1, is_gsheet_sync=1` → Normal tracked company (962 companies)
- `is_active=1, is_gsheet_sync=0` → Valuation only, no GSheet (1,950 companies)
- `is_active=0, is_gsheet_sync=0` → Deactivated (0 companies currently)

### 5. PM-Controlled, Not Code-Controlled
No hardcoded MCap thresholds in sync scripts

**Why:**
- PM decides which companies to sync via Django admin
- Flexibility: Can sync small-cap if high-conviction idea
- Can remove large-cap if temporarily not tracking
- Business logic in UI, not code

---

## 🎯 COMMON WORKFLOWS

### Adding a New Company to Valuation System

1. Go to **Market Scrips** admin
2. Filter by cap_class = "Large Cap" (or search by name/symbol)
3. Check "In VS" column - if green checkmark, already exists
4. Select company (checkbox)
5. Choose action:
   - "Add to Valuation System + Enable GSheet Sync" (recommended for MCap > 2,500 Cr)
   - "Add to Valuation System (Local Only)" (for smaller companies)
6. Company now appears in **Active Companies** admin
7. Set `valuation_group` and `valuation_subgroup` manually (if not auto-detected)
8. Run batch valuation to populate vs_valuations
9. If `is_gsheet_sync = 1`, next GSheet sync will include it

### Cleaning Up GSheet (Too Many Companies)

1. Go to **Active Companies** admin
2. Filter by `is_gsheet_sync = Yes`
3. Identify candidates for removal (e.g., MCap < 1,000 Cr, low priority)
4. Select companies (checkboxes)
5. Bulk action: "Disable GSheet Sync"
6. Companies remain in batch valuation but disappear from GSheet
7. Next GSheet sync will reflect changes

### Temporarily Removing a Company (e.g., Delisted)

1. Go to **Active Companies** admin
2. Search for company by name/symbol
3. Select company (checkbox)
4. Bulk action: "Deactivate companies"
5. Company removed from batch valuation
6. All child data preserved (drivers, valuations, events, alerts)
7. To reactivate: Bulk action "Activate companies"

### Promoting Small-Cap to GSheet (High Conviction Idea)

1. Go to **Active Companies** admin
2. Filter by `is_gsheet_sync = No`
3. Search for company (e.g., small-cap with good fundamentals)
4. Select company (checkbox)
5. Bulk action: "Enable GSheet Sync"
6. Company now syncs to GSheet Tabs 4, 8, 9
7. PM can track drivers/alerts in GSheet

---

## 🔍 FILTERING IN ADMIN PAGES

All admin pages with company references now support filtering by:
- **Valuation Group** (e.g., CONSUMER, FINANCIALS, INDUSTRIALS)
- **Valuation Subgroup** (e.g., CONSUMER_FMCG, FINANCIALS_BANKS)
- **Sector** (legacy Screener.in field)
- **Industry** (legacy Screener.in field)

### Pages with Filters

1. **VsActiveCompanies** - Direct filters (valuation_group/subgroup are model fields)
2. **VsEventTimeline** - Join filters via company_id → vs_active_companies
3. **VsMaterialityAlerts** - Direct valuation_group/subgroup, join for sector/industry
4. **VsDrivers** - Direct valuation_group/subgroup, join for sector/industry
5. **VsCompanySegments** - Join filters via company_id
6. **VsNseFetchTracker** - Join filters via company_id

**Why join filters:** Some tables only have `company_id`, not full taxonomy fields

---

## 📈 CURRENT STATE (as of implementation)

| Metric | Value |
|--------|-------|
| Total Market Scrips (mssdb) | 16,961 |
| Active Companies (rag) | 2,912 |
| is_active = 1 | 2,912 |
| is_gsheet_sync = 1 | 962 |
| GSheet-synced companies | 962 |
| Initialized via MCap filter | >= 2,500 Cr |

---

## 🚀 NEXT STEPS

### For PM
1. Review the 962 companies with `is_gsheet_sync = 1`
2. Adjust threshold (enable/disable GSheet sync via bulk actions)
3. Ensure valuation_group/subgroup are set for all active companies
4. Set priority field (1-10) to control processing order in batch runs

### For System
1. Batch valuation runs daily for `is_active = 1` companies (2,912)
2. GSheet sync runs after batch valuation, syncs `is_gsheet_sync = 1` companies (962)
3. News scanner picks up events for all active companies
4. Materiality alerts generated for all active companies
5. GSheet shows subset for PM review

---

## 📝 TECHNICAL NOTES

### Database Schema

```sql
-- Added column to vs_active_companies
ALTER TABLE vs_active_companies
  ADD COLUMN is_gsheet_sync TINYINT(1) NOT NULL DEFAULT 0
  COMMENT 'Sync to GSheet Tabs 4-9'
  AFTER is_active;

ALTER TABLE vs_active_companies
  ADD INDEX idx_gsheet_sync (is_gsheet_sync);
```

### Django Models

- `KbappMarketscrip` - Unmanaged model, `app_label = 'mssdb'`, routes to mssdb database
- `VsActiveCompanies` - Unmanaged model, `app_label = 'valuation_system'`, has `is_gsheet_sync` field

### Database Router

```python
class MultiDBRouter:
    def db_for_read(self, model, **hints):
        if model._meta.model_name == 'kbappmarketscrip':
            return 'mssdb'
        return 'default'
```

### GSheet Sync Changes

- Removed `_load_mcap_company_ids()` function
- Added `_load_gsheet_sync_company_ids()` function
- Replaced MCap filter with `WHERE is_gsheet_sync = 1` in Tabs 4, 8, 9
- Tab 6 shows all companies with status columns

---

## ⚠️ IMPORTANT REMINDERS

1. **Never modify mssdb.kbapp_marketscrip** - It's a shared master across applications
2. **Use bulk actions, not manual SQL** - Django admin provides audit trail
3. **is_active = 0 preserves data** - Soft delete, not hard delete
4. **GSheet sync is selective** - Not all active companies need to be in GSheet
5. **Valuation taxonomy supersedes legacy** - Use valuation_group/subgroup, ignore cd_sector/cd_industry

---

## 📚 REFERENCES

- Django Admin: http://localhost:8000/admin/
- Market Scrips: http://localhost:8000/admin/mssdb/kbappmarketscrip/
- Active Companies: http://localhost:8000/admin/valuation_system/vsactivecompanies/
- GSheet Sync Script: `/Users/ram/code/research/valuation_system/utils/sync_drivers_to_gsheet.py`
- Plan: `/Users/ram/.claude/plans/parallel-squishing-sifakis.md`
