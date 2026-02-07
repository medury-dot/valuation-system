# Architecture Decision: Google Sheet vs MySQL for Driver Storage
**Key Requirement**: Multi-analyst collaboration on driver weights and values

---

## Executive Summary

**Recommendation**: **Hybrid Approach** (Google Sheet primary + MySQL cache)

**Rationale**:
- ✅ Google Sheet: Best for **collaboration** (multiple analysts, real-time)
- ✅ MySQL: Best for **performance** (fast queries, no API limits)
- ✅ Hybrid: Gets benefits of both, minimizes drawbacks

**For 1000 companies**: Scalable with 90-day rolling window (3.4M cells, 34% of limit)

---

## Detailed Comparison

### Option 1: Google Sheet as Source of Truth (Current Design)

#### ✅ ADVANTAGES

**1. Collaboration (⭐⭐⭐⭐⭐)**:
```
Multiple analysts can:
├─ Edit simultaneously (real-time sync)
├─ See who changed what (version history)
├─ Add comments to discuss values
├─ Use @mentions to notify colleagues
├─ See edit history with timestamps
└─ Resolve conflicts visually
```

**Real-world scenario**:
```
10:00 AM: Analyst 1 updates crude_oil: 85 → 90
          Comment: "EIA forecast suggests spike"

10:15 AM: Analyst 2 sees change notification
          Reply comment: "Agree, but China demand weak. Use 88?"

10:20 AM: Analyst 1 updates to 88
          Comment: "Good point, adjusted"

RESULT: Collaborative decision in 20 minutes
        (vs email thread taking hours)
```

**2. Low Technical Barrier (⭐⭐⭐⭐⭐)**:
- Any analyst can use (no SQL knowledge needed)
- Familiar spreadsheet interface
- No code deployment needed
- Filter/sort without programming
- Formulas visible (transparency)

**3. Visual Workflow (⭐⭐⭐⭐)**:
```
Conditional Formatting:
├─ Recent changes highlighted in yellow
├─ Outliers (>2σ) highlighted in red
├─ Trending UP = green, DOWN = red arrows
└─ Missing values = orange warning

Data Validation:
├─ Dropdown for qualitative values (HIGH/MODERATE/LOW)
├─ Regex validation for percentages (4.5%)
└─ Range limits for ratios (0-100)
```

**4. Built-in Features (⭐⭐⭐⭐)**:
- Version history (restore to any point)
- Named ranges (reference cells easily)
- Protected ranges (lock certain columns)
- Sharing permissions (Editor, Viewer, Commenter)
- Mobile app (update drivers from phone)

**5. No Infrastructure (⭐⭐⭐⭐⭐)**:
- No server to maintain
- No backups needed (Google handles)
- No downtime
- Auto-saves every edit

#### ❌ DISADVANTAGES

**1. Scale Limits (⭐⭐)**:
```
Google Sheets Limits:
├─ 10 million cells max
├─ 18,278 columns max (not relevant for us)
├─ 200 sheets max (not relevant with new design)
└─ 5 million characters per cell (not relevant)

Our Usage (1000 companies):
├─ ~208,520 rows × 15 cols = 3.4M cells
├─ With 90-day archive: WITHIN LIMIT ✓
└─ Without archive: Would hit limit in ~3 years ⚠️
```

**Mitigation**: 90-day rolling window + MySQL archive

**2. API Performance (⭐⭐⭐)**:
```
Read Performance:
├─ 25,000 rows: 5-10 seconds (Google Sheets API)
├─ vs <100ms in MySQL
└─ Acceptable for daily sync, not for real-time queries

Write Performance:
├─ Update 1 driver: 1-2 seconds
├─ vs <10ms in MySQL
└─ Acceptable for hourly updates

Rate Limits:
├─ 300 read requests per minute
├─ 100 write requests per minute
└─ Enough for our hourly/daily jobs ✓
```

**Mitigation**: MySQL cache layer (read from MySQL, sync from GSheet)

**3. Offline Access (⭐)**:
- Requires internet
- If Google is down, system can't update drivers
- **Mitigation**: Use last known values from MySQL cache

**4. Storage Quota (⭐⭐)**:
- Service account quota exceeded (issue we hit)
- **Mitigation**: Use personal Google account or workspace account

**5. Complex Queries (⭐⭐)**:
```
Hard to do in GSheet:
├─ JOIN across sheets
├─ Aggregations (AVG, STDEV across 1000 companies)
├─ Time-series analysis
└─ Statistical functions

Example: "Find all companies where (r_and_d_intensity > 4%)
         AND (net_debt_ebitda < 1.0)
         AND (promoter_holding > 70%)"

In GSheet: Complex filtering, manual
In MySQL:  Simple WHERE clause, instant
```

**Mitigation**: Use MySQL for analytics, GSheet for data entry

---

### Option 2: MySQL as Source of Truth

#### ✅ ADVANTAGES

**1. Performance (⭐⭐⭐⭐⭐)**:
```
Query Speed:
├─ Read 25,000 rows: <100ms (vs 5-10s in GSheet)
├─ Update 1 driver: <10ms (vs 1-2s in GSheet)
├─ Complex JOIN: <500ms
└─ Aggregations: <200ms

Scale:
├─ Millions of rows: No problem
├─ No cell/row limits
└─ TB of data supported
```

**2. Data Integrity (⭐⭐⭐⭐⭐)**:
```
Features:
├─ Foreign keys (company_id → kbapp_marketscrip)
├─ Constraints (weight between 0-1, NOT NULL)
├─ Transactions (all-or-nothing updates)
├─ Indexes (fast lookups)
└─ Triggers (auto-update last_modified)
```

**3. Powerful Queries (⭐⭐⭐⭐⭐)**:
```sql
-- Complex analytics (impossible in GSheet):

-- Find top 20 high-quality, low-debt companies
SELECT company_name, r_and_d, debt_ratio, promoter
FROM vs_drivers
WHERE driver_name IN ('r_and_d_intensity', 'net_debt_ebitda', 'promoter_holding')
  AND r_and_d > 4
  AND debt_ratio < 1.0
  AND promoter > 70
ORDER BY r_and_d DESC
LIMIT 20;

-- Time-series: How has crude oil changed over time?
SELECT date, old_value, new_value
FROM vs_driver_history
WHERE driver_name = 'crude_oil'
ORDER BY date DESC
LIMIT 30;
```

**4. Offline Operation (⭐⭐⭐⭐⭐)**:
- Works without internet
- No API quotas
- No rate limits
- Always available

**5. No Scale Limits (⭐⭐⭐⭐⭐)**:
- Store unlimited history
- No archiving needed
- Millions of rows perform fine

#### ❌ DISADVANTAGES

**1. Collaboration (⭐⭐ POOR)**:
```
Multi-analyst workflow requires:

Option A: phpMyAdmin/MySQL Workbench
├─ Problem: Only 1 person can edit at a time (no real-time sync)
├─ Problem: Need SQL knowledge
├─ Problem: No built-in commenting
└─ Problem: Version conflicts (who has latest data?)

Option B: Build Custom Web UI
├─ Problem: Dev time (weeks/months)
├─ Problem: Maintenance burden
├─ Problem: User management, auth
└─ Problem: Mobile support needed

Option C: Use Retool/Budibase (Low-code UI)
├─ Cost: $10-50/user/month
├─ Learning curve
└─ Still requires setup
```

**Real-world scenario** (without web UI):
```
10:00 AM: Analyst 1 updates crude_oil via SQL:
          UPDATE vs_drivers SET current_value = 90 WHERE driver_name = 'crude_oil'

10:15 AM: Analyst 2 doesn't know about change
          Updates to 88: UPDATE vs_drivers SET current_value = 88 ...

10:20 AM: Analyst 1's value overwritten
          No notification, no comment thread
          Need to check logs/history to resolve

RESULT: Conflict resolution requires manual coordination
        (vs Google Sheet automatic sync + notifications)
```

**2. No Visual Interface (⭐⭐)**:
- No conditional formatting
- No charts/graphs (need separate BI tool)
- Hard to spot patterns
- Analysts prefer spreadsheets

**3. Deployment Friction (⭐⭐⭐)**:
- Schema changes require migrations
- Need DBA for structural updates
- Analyst can't add new driver type easily (need dev)

**4. Audit Trail Complexity (⭐⭐⭐)**:
- Need triggers or application logic
- Version history not built-in
- Need separate table for history

---

### Option 3: Hybrid (RECOMMENDED ✅)

**Architecture**:
```
┌─────────────────────────────────────────────────────────────┐
│  GOOGLE SHEET (Source of Truth for Current State)          │
│  • PM + Analysts edit here                                  │
│  • Real-time collaboration                                  │
│  • Last 90 days of history                                  │
└───────────────┬─────────────────────────────────────────────┘
                │
                │ Hourly Sync (reads changes)
                │ Daily Full Sync (all drivers)
                ▼
┌─────────────────────────────────────────────────────────────┐
│  MYSQL (Cache + Full History Archive)                       │
│  • System reads from here (fast)                            │
│  • Stores ALL history (unlimited)                           │
│  • Complex analytics queries                                │
└─────────────────────────────────────────────────────────────┘
```

#### How It Works

**For PM/Analysts (Use Google Sheet)**:
1. Open Google Sheet in browser
2. Filter to Aether Industries (company_id = 47582)
3. Update "r_and_d_intensity": 4.5% → 4.8%
4. Add comment: "Q3 results showed increased R&D spend"
5. Colleague sees change immediately, can reply

**For System (Use MySQL)**:
1. Hourly job: Read GSheet API → Check for changes
2. If changes found: Sync to MySQL `vs_drivers` table
3. Daily valuation: Read from MySQL (fast <100ms)
4. Write results back to MySQL
5. Append summary to GSheet "Valuation History"

**Data Flow**:
```
┌─────────────────┐
│ Analyst edits   │
│ Google Sheet    │
│ (Column H)      │
└────────┬────────┘
         │
         │ Hourly sync
         ▼
┌─────────────────┐      ┌──────────────────┐
│ MySQL vs_drivers│◄─────│ Valuation System │
│ (current state) │      │ Reads from MySQL │
└────────┬────────┘      └────────┬─────────┘
         │                        │
         │                        │ Daily
         │                        ▼
         │               ┌──────────────────┐
         │               │ vs_valuations    │
         │               │ (results)        │
         │               └────────┬─────────┘
         │                        │
         │    Summary written     │
         └────────────────────────┘
                  to GSheet
```

#### Benefits of Hybrid

✅ **Best Collaboration** (Google Sheet):
- Multiple analysts edit simultaneously
- Real-time notifications
- Comments for discussions
- Visual interface
- No technical barrier

✅ **Best Performance** (MySQL):
- System reads from MySQL (fast)
- Complex queries in MySQL
- Unlimited history storage
- No API rate limits for queries

✅ **Best Reliability**:
- If Google down: System uses MySQL cache (last known values)
- If MySQL down: System queues updates, replays after recovery
- Dual storage = redundancy

✅ **Best Scalability**:
- Google Sheet: 90-day rolling window (3.4M cells)
- MySQL: Full history (unlimited)
- Archives old GSheet data to MySQL automatically

#### Drawbacks

⚠️ **Sync Complexity**:
- Need sync logic (hourly job)
- Potential lag (up to 1 hour)
- Edge case: Analyst edits during valuation run

**Mitigation**:
- Lock GSheet during valuation (use Apps Script)
- Or: Accept eventual consistency (1 hour lag acceptable)

⚠️ **Duplicate Storage**:
- Same data in 2 places
- Need to keep in sync
- MySQL is ~1 hour behind GSheet

**Mitigation**:
- MySQL is "cache", not independent source
- GSheet is source of truth
- Sync is one-way: GSheet → MySQL

---

## Collaboration Scenarios

### Scenario 1: Research Team Meeting (5 analysts)

**Task**: Update all chemicals sector drivers after industry conference

**With Google Sheet** ✅:
```
11:00 AM Conference ends
11:15 AM Team opens shared Google Sheet on projector
         Each analyst assigned drivers to update

Analyst 1: Updates crude_oil, natural_gas (Cost drivers)
Analyst 2: Updates china_plus_one, pharma_api (Demand drivers)
Analyst 3: Updates PLI_scheme, REACH_compliance (Regulatory)
Analyst 4: Reviews and adds comments
Analyst 5: Updates 10 company-specific drivers for Aether

11:45 AM All updates done, discussed, and committed
12:00 PM Hourly job syncs to MySQL
01:00 PM Next hourly job uses updated drivers

DURATION: 30 minutes collaborative session
```

**With MySQL Only** ❌:
```
11:00 AM Conference ends
11:15 AM Analyst 1 writes SQL updates, emails to team
11:30 AM Analyst 2 replies with different values
11:45 AM Analyst 3 suggests compromise values
12:00 PM Back-and-forth continues via email
12:30 PM Finally agree on values
12:45 PM Analyst 1 runs UPDATE statements
01:00 PM Realizes syntax error, debugs
01:15 PM Finally updated

DURATION: 2 hours with coordination overhead
ISSUES: No real-time collaboration, error-prone
```

---

### Scenario 2: Weekly Driver Review

**Task**: Review all 1000 companies' drivers, update based on news

**With Google Sheet** ✅:
```
Visual Workflow:
1. Open "3. Company Drivers" sheet
2. Use conditional formatting:
   - Green: Updated in last 7 days ✓
   - Yellow: Updated 7-30 days ago ⚠️
   - Red: Not updated in 30+ days ⚠️⚠️
3. Filter red cells → Review stale drivers
4. Update values directly in sheet
5. Add comments explaining changes

Collaboration:
- Analyst 1: Reviews rows 1-10,000 (400 companies)
- Analyst 2: Reviews rows 10,001-20,000 (400 companies)
- Analyst 3: Reviews rows 20,001-25,000 (200 companies)

All work simultaneously, no conflicts
```

**With MySQL Only** ❌:
```
Need to build:
1. Web UI for filtering/sorting
2. Color coding logic
3. User assignment system
4. Conflict resolution

Or use phpMyAdmin:
- Can't work simultaneously (file locking)
- No visual highlighting
- Hard to track who reviewed what
```

---

### Scenario 3: Quick Update from Mobile

**Task**: Urgent news - crude oil spikes to $95, analyst is commuting

**With Google Sheet** ✅:
```
1. Open Google Sheets mobile app
2. Navigate to "2. Sector Drivers"
3. Find crude_oil row (Ctrl+F on mobile)
4. Update: 85 → 95
5. Add comment: "Geopolitical tension, see Reuters"
6. Done in 2 minutes

System picks up in next hourly sync
```

**With MySQL Only** ❌:
```
Need either:
- SSH into server (not mobile-friendly)
- Wait until at desk
- Or build mobile web UI (complex)
```

---

## Analyst Workflow Comparison

### Updating 10 Drivers

| Task | Google Sheet | MySQL (SQL) | MySQL (Web UI) |
|------|--------------|-------------|----------------|
| **Open data** | 1 click (bookmark) | SSH + login | Login to UI |
| **Find driver** | Ctrl+F or filter | Write SELECT query | Search box |
| **Update value** | Click cell, type | Write UPDATE query | Edit form |
| **Add context** | Add comment | Update separate column | Comment box |
| **Save** | Auto-save | COMMIT | Save button |
| **Notify team** | @mention in comment | Send email | Depends on UI |
| **Review changes** | Version history (built-in) | Query history table | Audit log |
| **Total time** | **5 minutes** ⭐ | 15-20 minutes | 8-10 minutes |
| **Error rate** | Low (validation) | High (typos, syntax) | Medium |
| **Collaboration** | Real-time | Sequential | Depends |

**Winner**: Google Sheet (3-4x faster, easier collaboration)

---

## Technical Performance Comparison

### Read Performance (Daily Valuation Run)

**Scenario**: Read all drivers for 1000 companies

| Method | Operation | Time | Network | Limit |
|--------|-----------|------|---------|-------|
| **Google Sheet** | API call: `get_all_records()` | 5-10 seconds | Required | 300 req/min |
| **MySQL** | Query: `SELECT * FROM vs_drivers` | <100ms | Local | None |

**Impact on Daily Job**:
- With GSheet read: 10 seconds overhead
- With MySQL read: <1 second overhead
- **Winner**: MySQL (100x faster)

**Solution**: Daily job reads from MySQL (not GSheet)

---

### Write Performance (Driver Updates)

**Scenario**: Update 50 drivers after news events (hourly job)

| Method | Operation | Time | Network | Limit |
|--------|-----------|------|---------|-------|
| **Google Sheet** | 50 × `update_cell()` | 50-100 seconds | Required | 100 write/min |
| **MySQL** | 50 × `UPDATE` or 1 batch | <500ms | Local | None |

**Impact on Hourly Job**:
- With GSheet write: 1-2 minutes
- With MySQL write: <1 second
- **Winner**: MySQL (100x faster)

**Solution**: Write to MySQL immediately, sync to GSheet in background

---

### History/Archive Storage

**Scenario**: Store 3 years of driver history (1000 companies, daily updates)

| Method | Storage | Query | Cost | Limit |
|--------|---------|-------|------|-------|
| **Google Sheet** | 1M+ rows → **Exceeds limit** ❌ | Slow | Free | 10M cells |
| **MySQL** | 1M+ rows → No problem ✓ | Fast | Free (local) | TB+ |

**Winner**: MySQL (no contest)

**Solution**: GSheet stores last 90 days, MySQL stores forever

---

## Cost Analysis

### Google Workspace (for multi-analyst collaboration)

**Free Tier**:
- 15 GB storage per account
- ⚠️ Service account quota issue (we hit this)

**Google Workspace** ($12/user/month):
- Unlimited storage (per user)
- Better API quotas
- Support

**For 5 analysts**: $60/month = $720/year

---

### MySQL (Local)

**Cost**: $0 (already running locally)

**Backup Storage** (optional):
- AWS RDS: $50-100/month for managed MySQL
- Or: Local backups (free, manual)

---

### Build Web UI (MySQL Front-End)

**Option A: Build Custom**:
- Dev time: 2-4 weeks (full-time)
- Tech stack: React + Node + MySQL
- Features: Auth, CRUD, filtering, audit log
- **Cost**: $8,000-15,000 (outsourced) or 160-320 hours (in-house)

**Option B: Use Retool/Budibase (Low-Code)**:
- Setup time: 2-4 days
- Cost: $10-50/user/month
- For 5 users: $250-2,500/year
- Pros: Fast, professional, maintained
- Cons: Monthly cost, vendor lock-in

**Option C: Use Metabase/Redash (BI Tool)**:
- Free (open source)
- Setup: 1-2 days
- Features: Read-only dashboards, SQL editor
- ❌ **No collaborative editing** (can view, can't easily update)

---

## Recommended Architecture: HYBRID

### Design

```
┌─────────────────────────────────────────────────────────────────┐
│                    TIER 1: GOOGLE SHEET                         │
│                  (Analyst Interface + Recent History)           │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  Sheet 1: Macro Drivers (20 rows, all time)                    │
│  Sheet 2: Sector Drivers (500 rows, all time)                  │
│  Sheet 3: Company Drivers (25,000 rows, current state)         │
│  Sheet 4: Valuation History (last 90 days, ~90K rows)          │
│  Sheet 5: Driver History (last 90 days, ~90K rows)             │
│  Sheet 6: Event Log (last 30 days, ~3K rows)                   │
│                                                                 │
│  Total: 3.4M cells (34% of limit) ✓                            │
│                                                                 │
└────────────────────┬────────────────────────────────────────────┘
                     │
                     │ SYNC (Bidirectional)
                     │ • Hourly: GSheet changes → MySQL (differential)
                     │ • Daily: Full sync (all drivers)
                     │ • On valuation: MySQL results → GSheet history
                     │
┌────────────────────▼────────────────────────────────────────────┐
│                    TIER 2: MYSQL                                │
│                 (System Cache + Full Archive)                   │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  vs_drivers (current state, 25.5K rows)                         │
│  vs_driver_history (full audit trail, unlimited)                │
│  vs_valuations (all valuations, unlimited)                      │
│  vs_news (all news events, unlimited)                           │
│  ... (11 more tables)                                           │
│                                                                 │
│  Total: Millions of rows, no limits ✓                           │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

---

### Roles & Responsibilities

**Google Sheet (Analyst-Facing)**:
- ✅ PM/Analysts update driver values (Column H)
- ✅ Add comments for discussions
- ✅ View recent history (90 days)
- ✅ See valuation summary
- ❌ No complex analytics (use MySQL/BI tool for that)

**MySQL (System-Facing)**:
- ✅ Valuation engine reads from here (fast)
- ✅ News scanner writes here (fast)
- ✅ Stores ALL history (unlimited)
- ✅ Complex queries (analytics, reporting)
- ❌ Analysts don't interact directly (use GSheet instead)

---

### Sync Strategy

**Hourly Job (Lightweight)**:
```python
def sync_gsheet_to_mysql():
    """
    Read only CHANGED drivers from GSheet (check last_updated column).
    Write to MySQL vs_drivers table.
    """
    # 1. Get all drivers with last_updated > last_sync_time
    #    (Only read ~10-50 changed rows, not all 25K)

    # 2. For each changed driver:
    #    - UPDATE vs_drivers SET current_value = new_value
    #    - INSERT into vs_driver_history (audit trail)
    #    - Log change (who, what, when, why)

    # 3. Update sync checkpoint (last_sync_time = now)

# Runtime: 5-10 seconds (minimal overhead)
```

**Daily Full Sync**:
```python
def full_sync_gsheet_to_mysql():
    """
    Full read of all driver current states.
    Ensures MySQL is authoritative copy of GSheet.
    """
    # 1. Read ALL rows from GSheet (25K rows, 5-10 seconds)
    # 2. Batch UPDATE to MySQL (all 25K rows, <1 second)
    # 3. Verify counts match

# Runtime: 10-15 seconds total
```

**Archive Job (Weekly)**:
```python
def archive_old_gsheet_records():
    """
    Move records older than 90 days from GSheet to MySQL.
    Keeps GSheet lean.
    """
    cutoff = datetime.now() - timedelta(days=90)

    # 1. Get old records from GSheet (last_updated < cutoff)
    # 2. Verify they exist in MySQL (should already be there from daily sync)
    # 3. Delete from GSheet
    # 4. Keep GSheet under 3.4M cells

# Runtime: 20-30 seconds weekly
```

---

## Scalability Analysis: Hybrid Approach

### For 1000 Companies

**Google Sheet Storage**:
| Sheet | Rows | Retention | Cells | Notes |
|-------|------|-----------|-------|-------|
| Macro Drivers | 20 | All time | 280 | Never archives |
| Sector Drivers | 500 | All time | 7.5K | Rarely changes |
| Company Drivers | 25,000 | Current state | 325K | Active editing |
| Valuation History | 90,000 | 90 days | 1.7M | Archives weekly |
| Driver History | 90,000 | 90 days | 1.3M | Archives weekly |
| Event Log | 3,000 | 30 days | 42K | Archives daily |
| **TOTAL** | **208,520** | - | **3.4M** | **34% of limit** ✓ |

**MySQL Storage** (full history):
| Table | Rows/Year | Size/Year | Total (5 years) |
|-------|-----------|-----------|-----------------|
| vs_drivers | 25,520 | 2 MB | 10 MB |
| vs_driver_history | 365,000 | 50 MB | 250 MB |
| vs_valuations | 365,000 | 100 MB | 500 MB |
| vs_news | 30,000 | 20 MB | 100 MB |
| **TOTAL** | **785,520/yr** | **172 MB/yr** | **860 MB (5yr)** ✓ |

**Conclusion**: Both scale easily to 1000 companies ✓

---

## Final Recommendation

### 🏆 **Use Hybrid Architecture**

**Google Sheet for**:
- ✅ Analyst data entry (current driver values)
- ✅ Collaboration (real-time editing)
- ✅ Comments/discussions
- ✅ Recent history (90 days)
- ✅ Visual interface

**MySQL for**:
- ✅ System reads (fast valuation runs)
- ✅ Full history (unlimited storage)
- ✅ Complex analytics (SQL queries)
- ✅ Backup/redundancy
- ✅ API independence (no rate limits)

**Sync**:
- Hourly: GSheet changes → MySQL (differential)
- Daily: Full sync (all drivers)
- Weekly: Archive old GSheet data

---

## Implementation Checklist

### Phase 1: Setup (This Week)

- [ ] Create Google Sheet with 6 sheets (using provided CSVs)
- [ ] Share with service account OR use personal account
- [ ] Add GSHEET_DRIVERS_ID to .env
- [ ] Test connection (`python test_gsheet_connection.py`)
- [ ] Verify all 6 sheets readable

### Phase 2: Sync Logic (Next Week)

- [ ] Implement hourly sync: GSheet → MySQL
- [ ] Implement daily full sync
- [ ] Implement weekly archive job
- [ ] Test with 2 companies (Aether, Eicher)
- [ ] Monitor sync logs

### Phase 3: Scale (Following Weeks)

- [ ] Add 10 more companies
- [ ] Test sync performance with 100 rows
- [ ] Add 100 companies
- [ ] Test sync performance with 2,500 rows
- [ ] Gradually scale to full watchlist

### Phase 4: Analytics Layer (Optional)

- [ ] Build Metabase dashboards for analytics
- [ ] Create SQL views for common queries
- [ ] Set up automated reports (driver changes, outliers)

---

## Alternative: If Google Sheet is Unacceptable

**If you must use MySQL only**, recommended approach:

**Build Web UI with Retool** ($250/month for 5 users):
- Setup time: 2-4 days
- Features: CRUD, filtering, audit log, comments
- Collaboration: Good (not real-time like GSheet, but acceptable)
- Learning curve: 1-2 days per analyst

**Pros vs Google Sheet**:
- ✅ Unlimited scale
- ✅ Faster API
- ✅ Better for 10,000+ companies (if you expand beyond stocks)

**Cons vs Google Sheet**:
- ❌ Monthly cost ($3,000/year)
- ❌ Vendor lock-in
- ❌ Setup time (vs GSheet 30 mins)

---

## Conclusion

**For your use case (1000 companies, multiple analysts)**:

**Google Sheet + MySQL Hybrid** is optimal because:
1. ⭐⭐⭐⭐⭐ **Collaboration** (your key requirement)
2. ⭐⭐⭐⭐⭐ **Performance** (MySQL cache layer)
3. ⭐⭐⭐⭐ **Scalability** (90-day rolling window)
4. ⭐⭐⭐⭐⭐ **Low barrier** (analysts love spreadsheets)
5. ⭐⭐⭐⭐ **Cost** ($0 with personal account, or $720/yr for Workspace)

**MySQL-only** would require building a web UI ($3,000-15,000) and wouldn't have the collaboration benefits of real-time spreadsheet editing.

---

**Decision**: Proceed with **Hybrid (Google Sheet + MySQL)** ✅

**Action**: Create Google Sheet using the 6 CSV templates provided.
