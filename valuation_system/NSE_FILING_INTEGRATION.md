# NSE Filing Data Integration — Implementation Summary

**Date:** 2026-02-09
**Status:** Phase 1 Complete (Event-Driven Fetch + State Tracking)
**Phase 2:** XBRL Segment Parsing (Scheduled for Separate Session)

---

## What Was Built

### 1. MySQL Schema (3 New Tables)

**File:** `storage/schema.sql`

#### `vs_nse_fetch_tracker` (Table 19)
- **Purpose**: State tracking for NSE filing data fetches
- **Key Fields**:
  - `company_id` → mssdb.kbapp_marketscrip.marketscrip_id
  - `nse_symbol` → NSE trading symbol
  - `latest_quarter_end`, `latest_quarter_idx` → Latest data we have
  - `filing_date` → When company filed to NSE
  - `last_fetch_date`, `last_fetch_status` → Fetch metadata
  - `data_hash` → MD5 for change detection
  - `xbrl_url`, `xbrl_parsed`, `has_segments` → XBRL tracking (Phase 2)

#### `vs_company_segments` (Table 20)
- **Purpose**: Canonical segment names per company (Phase 2 — SOTP valuation)
- **Key Fields**:
  - `company_id`, `segment_name` (e.g., "Royal Enfield", "VECV")
  - `mapped_subgroup` → Valuation subgroup for segment
  - `revenue_share_pct` → Latest % of total revenue
  - `pm_approved` → PM must approve segment→subgroup mapping

#### `vs_segment_financials` (Table 21)
- **Purpose**: Quarterly segment financial data from XBRL (Phase 2)
- **Key Fields**:
  - `company_id`, `segment_id`, `quarter_idx`
  - `revenue_cr`, `profit_cr`, `assets_cr`, `liabilities_cr`, `capex_cr`, `depreciation_cr`

---

### 2. NSE Loader Production System

**File:** `nse_results_prototype/nse_loader.py` (817 lines)

#### Class: `NSELoader`

**3 Operating Modes:**

1. **Daily Mode (Event-Driven)**
   - Discovers new filings via NSE event calendar + global filings API
   - Fetches only companies with new data (5-100 companies/day)
   - Runtime: 15 sec - 2.5 min during result season
   - API calls: 2 discovery + N fetch calls

2. **Sweep Mode (Quarterly Safety Net)**
   - Fetches all 1,500 active companies with 45-day staleness check
   - Runtime: ~38 min for full sweep
   - Scheduled: 02:00 IST on 15th of Feb/May/Aug/Nov
   - API calls: 2 + 1,500 = 1,502 calls

3. **Seed Mode (Initial Setup)**
   - Register all active companies with mcap > threshold (default: 2500 Cr)
   - Filter applied to avoid seeding small/illiquid companies
   - Runtime: ~20-30 min for ~800-1000 companies (vs 65 min for all 2,655)
   - Run once on initial setup, then use daily mode for incremental updates

4. **Single Mode (On-Demand)**
   - Fetch one company immediately
   - Used for manual testing and urgent updates

**Key Methods:**
- `discover_new_filings()` → Find companies with board meetings or new filings
- `fetch_company_results(symbol)` → Get quarterly data from NSE API
- `update_tracker(symbol, fetch_result)` → Update vs_nse_fetch_tracker
- `update_csv(fetch_result)` → Incrementally merge into nse_quarterly_data.csv
- `validate_against_core(symbol, fetch_result)` → Compare with core CSV

**Reuses from Prototype:**
- `NSESession` class (cookie handling, rate limiting, retries)
- `_nse_date_to_quarter_index()` → Convert NSE date to core CSV quarter index
- `_extract_results_quarters()` → Parse NSE JSON response

---

### 3. Pipeline Integration

**File:** `pipelines/xyops_config.py`

#### Updated `daily_valuation` Pipeline
- **New Step 1:** `nse_fetch` (runs before process_discoveries)
  - Module: `valuation_system.nse_results_prototype.nse_loader`
  - Method: `NSELoader.run_daily()`
  - Timeout: 5 minutes
  - Scheduled: 20:30 IST (30 min before batch valuation at 21:00)

#### New `nse_sweep` Pipeline
- **Schedule:** `0 2 15 2,5,8,11 *` → 02:00 IST on 15th of Feb/May/Aug/Nov
- **Purpose:** Safety net to catch any filings missed by daily event-driven mode
- **Timeout:** 60 minutes

**File:** `scheduler/runner.py`

- Added `run_nse_fetch(mode, symbol)` function
- Added `nse_fetch` command: `python -m valuation_system.scheduler.runner nse_fetch --mode daily`
- Added `nse_results_prototype/cache` to directory creation in `run_init()`

---

### 4. Data Source Integration

**File:** `data/loaders/nse_data_loader.py` (232 lines)

#### Class: `NSEDataLoader`
- Loads `nse_quarterly_data.csv` (incremental file updated by NSELoader)
- Provides unified interface matching `CoreDataLoader` patterns
- **Key Methods:**
  - `get_company_data(nse_symbol)` → Get all NSE data for company
  - `get_metric_dict(company_data, metric)` → Extract quarterly dict (e.g., {147: 1234.5, 148: 1456.7})
  - `get_latest_quarter_idx(nse_symbol)` → Latest quarter available
  - `has_newer_data_than_core(nse_symbol, core_latest_idx)` → Check staleness
  - `get_ttm(metric_dict)` → Calculate TTM from last 4 quarters

#### Helper Function: `merge_nse_into_financials(financials, nse_symbol, nse_loader)`
- Merges NSE quarters into `financials` dict from CoreDataLoader
- Strategy: For each metric, if NSE has quarters > core CSV latest, add them
- Metrics merged: sales, pat, pbidt, interest, pbt_excp, other_income, depreciation, empcost, rawmat, other_exp, total_exp, tax, exceptional, paid_up_equity, basic_eps, diluted_eps

**File:** `data/processors/financial_processor.py`

- **Updated `__init__`**: Lazy-load NSEDataLoader
- **Updated `build_dcf_inputs()`**: Merge NSE data before processing
  ```python
  if self.nse and self.nse.is_available() and nse_symbol:
      financials = merge_nse_into_financials(financials, nse_symbol, self.nse)
  ```

---

### 5. Configuration (.env)

**File:** `config/.env` (Already Present, Lines 59-63)

```bash
# NSE Filing Data
NSE_CACHE_DIR=/Users/ram/code/research/valuation_system/nse_results_prototype/cache
NSE_QUARTERLY_CSV_PATH=/Users/ram/code/research/valuation_system/nse_results_prototype/cache/nse_quarterly_data.csv
NSE_FETCH_BATCH_SIZE=50
NSE_RATE_LIMIT_PAUSE=1.5
```

---

## CSV Format: `nse_quarterly_data.csv`

**Columns:**

### Fixed Columns
- `company_name`, `nse_symbol`, `isin`, `nse_industry`, `data_source`, `fetch_date`

### Quarterly Metrics (Indexed by Quarter)
- Format: `{metric}_{quarter_idx}` (e.g., `sales_147`, `pat_148`)
- **Units:** Crores (NSE lakhs / 100 for consistency with core CSV)

**Metrics:**
1. **Core CSV Overlap:** sales, pat, pbidt, interest, pbt_excp
2. **Bonus Fields (Not in Core CSV):**
   - other_income, depreciation, empcost, rawmat, other_exp, total_exp
   - tax, exceptional, paid_up_equity, basic_eps, diluted_eps

### Filing Metadata (Per Quarter)
- `filing_date_{quarter_idx}` → When filed to NSE
- `result_type_{quarter_idx}` → U=unaudited, A=audited

**Example Columns:**
```
company_name, nse_symbol, isin, nse_industry, data_source, fetch_date,
sales_147, sales_148, sales_149,
pat_147, pat_148, pat_149,
pbidt_147, pbidt_148, pbidt_149,
...
filing_date_147, filing_date_148,
result_type_147, result_type_148
```

---

## How It Works: Event-Driven Fetch Flow

### Daily Cycle (20:30 IST)

1. **DISCOVER** (2 API calls)
   - GET `/api/event-calendar` → Board meetings in next 7 days
   - GET `/api/corporates-financial-results?period=Quarterly` → All recent filings (last 30 days)
   - Compare against `vs_nse_fetch_tracker` → Find NEW filings

2. **FETCH** (N API calls, N = 5-100 companies)
   - For each company with new filing:
     - GET `/api/results-comparision?symbol={symbol}`
     - Parse 5 quarters of P&L data (21 metrics per quarter)
     - Compute MD5 hash for change detection

3. **STORE**
   - Insert/update `vs_nse_fetch_tracker` (latest_quarter_idx, filing_date, data_hash)
   - Merge into `nse_quarterly_data.csv` (read → merge new quarters → write)

4. **VALIDATE**
   - Compare sales/pat/pbidt with core CSV where quarters overlap
   - Log mismatches if diff > 5%
   - Expected match: < 2% difference (proven in prototype)

### Quarterly Sweep (02:00 IST, 15th of Q2/Q3/Q4/Q1)

- Fetch all 1,500 active companies if `last_fetch_date` > 45 days old
- Safety net to catch anything daily mode missed

---

## Data Flow: NSE → Core → Financial Processor → DCF

```
NSELoader.run_daily()
  ↓
nse_quarterly_data.csv (incremental merge)
  ↓
NSEDataLoader.get_company_data(symbol)
  ↓
merge_nse_into_financials(core_financials, nse_symbol, nse_loader)
  → Adds quarters > core CSV latest to financials dict
  ↓
FinancialProcessor.build_dcf_inputs(company_name)
  → Uses merged financials (core + NSE)
  ↓
DCFModel._project_future_financials()
  → Latest TTM from NSE (if available) or core CSV
```

**Logging:**
- Core CSV data: `[ACTUAL]` tag
- NSE data: `[NSE_FILING]` tag
- Derived estimates: `[DERIVED]` tag
- Defaults: `[DEFAULT]` tag

---

## What We Proved (From Prototype)

### Validation Results (5 Pilot Companies)
- **Sales Match:** 0.2% - 1.9% difference vs core CSV
- **PAT Match:** < 2% difference
- **Data Availability:** 5 quarters of P&L per company
- **API Success:** 100% success rate (no auth required)
- **Segment Data:** NOT in JSON API — requires XBRL parsing (Phase 2)

### NSE API Characteristics
- **Rate Limit:** ~3 req/sec tolerated, we use 1.5 sec pause (conservative)
- **No Auth:** Cookies not required, API works without session
- **Retry Logic:** 403/401 trigger cookie refresh, 429 exponential backoff
- **Data Format:** JSON with 21 metrics per quarter, values in LAKHS

### XBRL Finding (Critical)
- `re_desc_note_seg` field in JSON API is just "-" or exemption text
- Segment data is ONLY in XBRL XML files (3,796/3,809 filings have XBRL URLs)
- XBRL parsing requires Ind-AS taxonomy knowledge → Phase 2

---

## Phase 2: XBRL Segment Parsing (Future Session)

### Scope
1. **Build XBRL Parser:** `nse_results_prototype/xbrl_parser.py`
   - Download XBRL files from `vs_nse_fetch_tracker.xbrl_url`
   - Parse Ind-AS taxonomy for segment revenue/profit
   - Insert into `vs_company_segments` + `vs_segment_financials`

2. **Segment → Subgroup Mapping (LLM + PM Approval)**
   - LLM suggests `mapped_subgroup` (e.g., "Royal Enfield" → AUTO_TWO_WHEELERS)
   - PM reviews in GSheet Tab 10 (new "Segments" tab)
   - Set `pm_approved=1` when confirmed

3. **SOTP Valuation Method**
   - Modify `valuator.py` to value each segment separately
   - Apply segment-specific multiples (from `mapped_subgroup`)
   - Sum segment values for total enterprise value

### Why Segments Matter
- **Conglomerates valued incorrectly:** Eicher (bikes + trucks), L&T (EPC + IT + Financial Services), Reliance (O2C + Retail + Jio + Media)
- **Revenue-weighted SOTP:** Each segment gets appropriate sector multiples
- **Example:** Eicher's VECV (trucks) is 30% of revenue but valued at auto OEM multiples (too low) instead of commercial vehicle multiples

---

## Verification Checklist (Before Production)

### ✅ Completed in This Session

1. ✅ MySQL tables created (vs_nse_fetch_tracker, vs_company_segments, vs_segment_financials)
2. ✅ NSELoader with daily/sweep/single modes implemented
3. ✅ Pipeline integration (nse_fetch step before daily_valuation)
4. ✅ Runner command: `python -m valuation_system.scheduler.runner nse_fetch`
5. ✅ NSEDataLoader with merge logic
6. ✅ FinancialProcessor integration (lazy NSE data merge)
7. ✅ .env configuration (already present)

### 🔲 To Run Before First Production Use

1. **Initialize tables:**
   ```bash
   python -m valuation_system.scheduler.runner init
   ```

2. **Test single company fetch:**
   ```bash
   python -m valuation_system.scheduler.runner nse_fetch --mode single --symbol EICHERMOT
   ```

3. **Verify CSV created:**
   ```bash
   ls -lh /Users/ram/code/research/valuation_system/nse_results_prototype/cache/nse_quarterly_data.csv
   ```

4. **Run daily mode (50-100 companies):**
   ```bash
   python -m valuation_system.scheduler.runner nse_fetch --mode daily
   ```

5. **Check tracker state:**
   ```sql
   SELECT nse_symbol, latest_quarter_idx, filing_date, last_fetch_status
   FROM vs_nse_fetch_tracker
   ORDER BY last_fetch_date DESC
   LIMIT 10;
   ```

6. **Run batch valuation with NSE data:**
   ```bash
   python -m valuation_system.utils.batch_valuation --symbols EICHERMOT,BEL,VBL
   ```

7. **Check batch issues CSV for [NSE_FILING] tags:**
   ```bash
   grep NSE_FILING /Users/ram/code/research/valuation_system/logs/batch_issues_*.csv
   ```

8. **(Optional) Full sweep for all 1,500 companies:**
   ```bash
   python -m valuation_system.scheduler.runner nse_fetch --mode sweep
   # Takes ~38 min, run off-hours
   ```

---

## Files Modified/Created

### Created (3 files, 1,279 lines)
1. `nse_results_prototype/nse_loader.py` — 817 lines
2. `data/loaders/nse_data_loader.py` — 232 lines
3. `NSE_FILING_INTEGRATION.md` — This file

### Modified (4 files)
1. `storage/schema.sql` — Added 3 tables (vs_nse_fetch_tracker, vs_company_segments, vs_segment_financials)
2. `pipelines/xyops_config.py` — Added nse_fetch step to daily_valuation, added nse_sweep pipeline
3. `scheduler/runner.py` — Added run_nse_fetch() function, nse_fetch command, directory creation
4. `data/processors/financial_processor.py` — Added nse_loader param, NSE data merge in build_dcf_inputs()

---

## Key Design Decisions

1. **Event-Driven > Brute-Force:** Daily mode only fetches companies with new filings (5-100/day) instead of all 1,500 daily
2. **Incremental CSV:** Read existing, merge new quarters, write back — no full rewrites
3. **State Tracking:** vs_nse_fetch_tracker prevents duplicate fetches and enables smart staleness checks
4. **Merge, Don't Replace:** NSE data is additive to core CSV (newer quarters only), preserves historical data
5. **No Synthetic Data:** All values from actual NSE API fields, units converted (lakhs → crores) for consistency
6. **Segments → Phase 2:** XBRL parsing is complex (Ind-AS taxonomy), separate implementation session
7. **Validation First:** Compare NSE vs core CSV on every fetch, log mismatches for review

---

## Lessons from Prototype → Production

1. **NSE API needs no auth:** Cookies help but aren't required
2. **Rate limiting is lenient:** 3 req/sec tolerated, we use 1.5 sec pause
3. **Quarter index math is critical:** NSE dates → core CSV indices must be exact
4. **Lakhs → Crores conversion:** NSE uses lakhs, core CSV uses crores, Excel uses crores
5. **MD5 hash for change detection:** Prevents redundant CSV writes when data unchanged
6. **Event calendar has false positives:** Board meeting ≠ guaranteed filing, still check global filings list
7. **Segment data is XBRL-only:** JSON API `re_desc_note_seg` field is useless

---

## Next Steps

### Immediate (This Week)
1. Run verification checklist above
2. Monitor first 3 days of daily fetches for errors
3. Check batch issues CSV for NSE data quality

### Phase 2 (Separate Session)
1. Build XBRL parser for segment data
2. Create GSheet Tab 10 for segment approval workflow
3. Implement SOTP valuation method
4. Test on 10 conglomerates (Eicher, L&T, Reliance, Adani, Tata group companies)

### Future Enhancements
- NSE announcements API for event-driven revaluations
- Balance sheet data from XBRL (NSE JSON has P&L only)
- Cashflow statement from XBRL (if needed for validation)
- Automated email alerts on data quality issues

---

## Contact / Questions

- Implementation questions: See code comments in nse_loader.py
- Data format questions: See prototype report at `nse_results_prototype/cache/field_inventory.txt`
- API documentation: NSE doesn't publish official docs, reverse-engineered from browser network tab

**Last Updated:** 2026-02-09 by Claude Opus 4.6
