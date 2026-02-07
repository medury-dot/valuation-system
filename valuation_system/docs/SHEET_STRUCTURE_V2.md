# Google Sheet Structure V2 - Scalable Design
**For 1000+ Companies**

---

## Design Principle

**❌ OLD (Doesn't Scale)**:
- One sheet per company → 1000 companies = 1000+ sheets
- One sheet per sector → 50 sectors = 50+ sheets

**✅ NEW (Scalable)**:
- **6 sheets total** (fixed, regardless of company count)
- Use rows for different entities (companies, sectors)
- Filter by `company_id`, `sector`, `driver_level`

---

## Sheet Structure

### Sheet 1: Macro Drivers (1 sheet, ~10-20 rows)

**Purpose**: Global economic factors affecting all companies

**Columns**:
| Column | Type | Example | Notes |
|--------|------|---------|-------|
| driver_id | INT | 1 | Unique ID |
| category | TEXT | Economy | Demand, Cost, Currency, Commodity, Policy |
| driver_name | TEXT | gdp_growth | Unique name |
| current_value | TEXT | 7.2% | PM-editable |
| bull_value | TEXT | 8.0% | Optimistic scenario |
| base_value | TEXT | 7.2% | Expected scenario |
| bear_value | TEXT | 6.0% | Pessimistic scenario |
| weight | DECIMAL | 0.05 | Impact weight (sum to ~0.20) |
| metric | TEXT | % | Unit of measurement |
| source | TEXT | NSO | Data source |
| update_freq | TEXT | Quarterly | How often updated |
| last_updated | DATETIME | 2026-02-05 10:00 | Auto-updated |
| trend | TEXT | STABLE | UP / DOWN / STABLE |
| impact | TEXT | Baseline growth | How it affects valuation |

**Sample Rows**:
```
1 | Economy | gdp_growth | 7.2% | 8.0% | 7.2% | 6.0% | 0.05 | % | NSO | Quarterly | 2026-01-15 | STABLE | Baseline growth
2 | Currency | usd_inr | 83.5 | 82.0 | 83.5 | 86.0 | 0.03 | Rate | RBI | Daily | 2026-02-05 | STABLE | Export margins
3 | Commodity | crude_oil | 85 | 70 | 85 | 95 | 0.05 | $/bbl | EIA | Weekly | 2026-02-05 | UP | Cost of goods
...
```

**Total Rows**: ~10-20 (fixed, all companies use same macro drivers)

---

### Sheet 2: Sector Drivers (1 sheet, ~500 rows for 10 sectors × 50 drivers)

**Purpose**: Sector-specific drivers (demand, cost, regulatory, Porter's forces)

**Columns**:
| Column | Type | Example | Notes |
|--------|------|---------|-------|
| driver_id | INT | 101 | Unique ID |
| sector | TEXT | Chemicals | From sectors.yaml |
| category | TEXT | Demand | Demand, Cost, Regulatory, Porter |
| driver_name | TEXT | china_plus_one | Unique within sector |
| current_value | TEXT | HIGH | PM-editable |
| bull_value | TEXT | HIGH | Optimistic |
| base_value | TEXT | MODERATE | Expected |
| bear_value | TEXT | LOW | Pessimistic |
| weight | DECIMAL | 0.10 | Driver weight (sum to ~0.55) |
| metric | TEXT | Intensity | Unit |
| impact | TEXT | market_share | What it affects |
| sensitivity | DECIMAL | -0.5 | For cost drivers (crude: -0.5% margin per $1) |
| trend | TEXT | UP | UP / DOWN / STABLE |
| last_updated | DATETIME | 2026-02-05 | Auto-updated |
| source | TEXT | Trade data | Source |

**Sample Rows**:
```
101 | Chemicals | Demand | volume_growth | 8% | 12% | 8% | 5% | 0.12 | YoY % | revenue_growth | NULL | STABLE | 2026-01-20 | Industry
102 | Chemicals | Demand | china_plus_one | HIGH | HIGH | MODERATE | LOW | 0.10 | Intensity | market_share | NULL | UP | 2026-02-05 | Trade data
103 | Chemicals | Cost | crude_oil | 85 | 70 | 85 | 95 | 0.10 | $/bbl | rm_cost | -0.5 | UP | 2026-02-05 | EIA
...
201 | Automobile & Ancillaries | Demand | industry_volume | 5% | 8% | 5% | 2% | 0.12 | YoY % | revenue_growth | NULL | STABLE | 2026-01-20 | SIAM
202 | Automobile & Ancillaries | Cost | steel_prices | 55 | 48 | 55 | 65 | 0.08 | Rs/kg | rm_cost | -0.3 | STABLE | 2026-02-04 | Steel ministry
...
```

**Query Pattern**:
```sql
SELECT * FROM sheet WHERE sector = 'Chemicals'  -- Get all chemicals drivers
SELECT * FROM sheet WHERE driver_name = 'crude_oil'  -- Get crude across sectors
```

**Total Rows**: ~500 (10 sectors × ~50 drivers each, grows linearly with sectors)

---

### Sheet 3: Company Drivers (1 sheet, ~25,000 rows for 1000 companies × 25 drivers)

**Purpose**: Company-specific alpha drivers

**Columns**:
| Column | Type | Example | Notes |
|--------|------|---------|-------|
| driver_id | INT | 10001 | Unique ID |
| company_id | INT | 47582 | From kbapp_marketscrip.marketscrip_id |
| company_name | TEXT | Aether Industries Ltd. | For readability |
| nse_symbol | TEXT | AETHER | For filtering |
| category | TEXT | Market Share | 9 categories |
| driver_name | TEXT | share_trajectory | Unique within company |
| current_value | TEXT | +2.5% | PM-editable |
| vs_peers | TEXT | Above avg | Relative positioning |
| weight | DECIMAL | 0.06 | Driver weight (sum to ~0.25) |
| metric | TEXT | YoY Δ | Unit |
| alpha_impact | TEXT | ROCE premium | How it creates alpha |
| last_updated | DATETIME | 2026-02-05 | Auto-updated |
| source | TEXT | Company filings | Source |

**Sample Rows**:
```
10001 | 47582 | Aether Industries Ltd. | AETHER | Market Share | share_trajectory | +2.5% | Above avg | 0.06 | YoY Δ | ROCE premium | 2026-02-05 | Filings
10002 | 47582 | Aether Industries Ltd. | AETHER | Mgmt Quality | r_and_d_intensity | 4.5% | 3.2% avg | 0.05 | % Sales | ROCE, terminal | 2026-02-05 | Filings
10003 | 47582 | Aether Industries Ltd. | AETHER | Product Mix | crams_pct | 35% | High | 0.05 | % Revenue | Margin, ASP | 2026-02-05 | Filings
...
20001 | 39424 | Eicher Motors Ltd. | EICHERMOT | Market Share | premium_2w_share | 28% | Leader | 0.08 | % | ROCE premium | 2026-02-05 | SIAM
20002 | 39424 | Eicher Motors Ltd. | EICHERMOT | Geographic | export_growth | 25% | Strong | 0.04 | YoY % | Growth driver | 2026-02-05 | Filings
...
```

**Query Pattern**:
```sql
SELECT * FROM sheet WHERE company_id = 47582  -- Get all Aether drivers
SELECT * FROM sheet WHERE nse_symbol = 'AETHER'  -- Same, by symbol
SELECT * FROM sheet WHERE driver_name = 'r_and_d_intensity'  -- Compare R&D across companies
```

**Total Rows**: ~25,000 (1000 companies × 25 drivers avg, grows linearly with companies)

**Google Sheets Limit**: 10 million cells
- 25,000 rows × 13 columns = 325,000 cells ✓ Well within limit

---

### Sheet 4: Valuation History (1 sheet, append-only)

**Purpose**: Timeline of all valuations for all companies

**Columns**:
| Column | Type | Example | Notes |
|--------|------|---------|-------|
| valuation_id | INT | 1 | Auto-increment |
| date | DATE | 2026-02-06 | Valuation date |
| company_id | INT | 47582 | Foreign key |
| company_name | TEXT | Aether Industries | For readability |
| nse_symbol | TEXT | AETHER | For filtering |
| intrinsic_value | DECIMAL | 287.63 | Blended value |
| cmp | DECIMAL | 1006.60 | Market price |
| upside_pct | DECIMAL | -71.4% | (Intrinsic - CMP) / CMP |
| dcf_base | DECIMAL | 209.64 | DCF base case |
| dcf_bull | DECIMAL | 251.68 | DCF bull case |
| dcf_bear | DECIMAL | 154.92 | DCF bear case |
| relative_value | DECIMAL | 469.87 | Relative valuation |
| monte_carlo_median | DECIMAL | 208.88 | MC median |
| confidence_score | DECIMAL | 0.80 | Model confidence |
| key_change | TEXT | Initial valuation | What changed |
| driver_impact | TEXT | High-growth normalization | Driver summary |
| event_ref | TEXT | | Link to event_id if triggered by news |
| doc_link | TEXT | aether_valuation_20260206.xlsx | Excel report link |
| synopsis | TEXT | First valuation with DCF fixes | 1-line summary |

**Sample Rows**:
```
1 | 2026-02-06 | 47582 | Aether Industries | AETHER | 287.63 | 1006.60 | -71.4% | 209.64 | 251.68 | 154.92 | 469.87 | 208.88 | 0.80 | Initial | Normalized | | aether_20260206.xlsx | First valuation
2 | 2026-02-07 | 47582 | Aether Industries | AETHER | 295.50 | 1015.00 | -70.9% | 215.30 | ... | | | +2.7% | china_plus_one UP | 12345 | | Driver update
3 | 2026-02-06 | 39424 | Eicher Motors | EICHERMOT | 3474.10 | 7215.00 | -51.8% | ... | | | Initial | Mature company | | eicher_20260206.xlsx | Baseline
```

**Growth**: Append daily (1000 companies × 365 days = 365,000 rows/year)
**Limit**: Google Sheets can handle millions of rows ✓

---

### Sheet 5: Driver History (1 sheet, append-only audit trail)

**Purpose**: Track every driver update (who changed what, when, why)

**Columns**:
| Column | Type | Example | Notes |
|--------|------|---------|-------|
| history_id | INT | 1 | Auto-increment |
| timestamp | DATETIME | 2026-02-05 10:15 | When changed |
| driver_level | TEXT | SECTOR | MACRO / SECTOR / COMPANY |
| sector | TEXT | Chemicals | If SECTOR level |
| company_id | INT | 47582 | If COMPANY level |
| company_name | TEXT | Aether | For readability |
| driver_name | TEXT | china_plus_one | Which driver |
| old_value | TEXT | MODERATE | Before |
| new_value | TEXT | HIGH | After |
| change_type | TEXT | AUTO | AUTO (LLM) / MANUAL (PM) |
| reason | TEXT | China tariff news | Why changed |
| impact_pct | DECIMAL | 2.5% | Estimated valuation impact |
| source_doc | TEXT | ET article link | Evidence |
| updated_by | TEXT | LLM / PM Name | Who made change |

**Sample Rows**:
```
1 | 2026-02-05 10:15 | SECTOR | Chemicals | NULL | NULL | china_plus_one | MODERATE | HIGH | AUTO | China tariff news | 2.5% | ET link | LLM
2 | 2026-02-05 14:30 | COMPANY | NULL | 47582 | Aether | r_and_d_intensity | 4.2% | 4.5% | MANUAL | Q3 results | 0.2% | Result filing | PM Ram
```

**Growth**: ~1,000 updates/day (10 macro + 100 sector + 900 company changes)
**Annual**: ~365,000 rows/year ✓ Manageable

---

### Sheet 6: Event Log (1 sheet, append-only)

**Purpose**: All news events processed

**Columns**:
| Column | Type | Example | Notes |
|--------|------|---------|-------|
| event_id | INT | 12345 | Auto-increment (matches MySQL) |
| event_date | DATE | 2026-02-05 | When event occurred |
| event_type | TEXT | REGULATORY | Category |
| scope | TEXT | SECTOR | MACRO / SECTOR / COMPANY |
| sector | TEXT | Chemicals | If sector-level |
| company_id | INT | 47582 | If company-level |
| company_name | TEXT | Aether | For readability |
| severity | TEXT | HIGH | CRITICAL / HIGH / MODERATE / LOW |
| headline | TEXT | China imposes tariff... | Short headline |
| synopsis | TEXT | China 10% tariff on chemicals... | 1-2 sentences |
| source_url | TEXT | https://economictimes... | Link to article |
| chromadb_id | TEXT | news_12345 | Link to full text in ChromaDB |
| drivers_affected | TEXT | china_plus_one, crude_oil | Comma-separated |
| processed_at | DATETIME | 2026-02-05 10:15 | When analyzed |

**Sample Rows**:
```
12345 | 2026-02-05 | REGULATORY | SECTOR | Chemicals | NULL | NULL | HIGH | China tariff... | 10% export tariff... | ET link | news_12345 | china_plus_one | 2026-02-05 10:15
12346 | 2026-02-05 | EARNINGS | COMPANY | NULL | 47582 | Aether | MODERATE | Q3 results inline | Revenue 317 Cr... | BSE filing | news_12346 | r_and_d_intensity | 2026-02-05 16:30
```

**Growth**: ~50-100 events/day
**Annual**: ~30,000 rows/year ✓ Manageable

---

## Scalability Analysis

### Storage Requirements (1000 Companies)

| Sheet | Rows | Columns | Cells | % of 10M Limit |
|-------|------|---------|-------|----------------|
| **Macro Drivers** | 20 | 14 | 280 | 0.003% |
| **Sector Drivers** | 500 | 15 | 7,500 | 0.075% |
| **Company Drivers** | 25,000 | 13 | 325,000 | 3.25% |
| **Valuation History** | 365,000/yr | 19 | 6.9M/yr | 69%/yr |
| **Driver History** | 365,000/yr | 14 | 5.1M/yr | 51%/yr |
| **Event Log** | 30,000/yr | 14 | 420K/yr | 4.2%/yr |
| **TOTAL (Year 1)** | ~785,520 | - | **12.7M** | **127%** ⚠️ |

**⚠️ Issue**: History sheets will exceed 10M cells in Year 1!

---

## Solution: Archive Strategy

### Option A: Rolling Window (Recommended)

**Keep last 90 days in Google Sheet, archive rest to MySQL**:

| Sheet | Keep in GSheet | Archive to MySQL |
|-------|----------------|------------------|
| Macro Drivers | All (current state) | History in vs_drivers table |
| Sector Drivers | All (current state) | History in vs_drivers table |
| Company Drivers | All (current state) | History in vs_drivers table |
| **Valuation History** | **Last 90 days** (~90K rows) | **All in vs_valuations** ✓ |
| **Driver History** | **Last 90 days** (~90K rows) | **All in vs_driver_history** ✓ |
| **Event Log** | **Last 30 days** (~3K rows) | **All in vs_news** ✓ |

**Cell Count (with 90-day window)**:
- Valuation History: 90,000 × 19 = 1.7M cells
- Driver History: 90,000 × 14 = 1.3M cells
- Event Log: 3,000 × 14 = 42K cells
- **Total: ~3.4M cells** (34% of limit) ✓ Sustainable

**Archive Process** (automated):
```python
# Daily cleanup job (runs at 02:00)
def archive_old_records():
    cutoff_date = datetime.now() - timedelta(days=90)

    # 1. Copy old records from GSheet to MySQL (if not already there)
    # 2. Delete old records from GSheet
    # 3. Keep GSheet under 3-4M cells
```

---

### Option B: Separate Sheets per Year

**Current Year in GSheet, Previous Years in Archive Sheets**:

Sheets:
- Valuation History 2026 (active, append)
- Valuation History 2025 (read-only archive)
- Valuation History 2024 (read-only archive)
- ...

**Issue**: Still grows unbounded (1 sheet per year per data type)

**Verdict**: Option A (Rolling Window) is better ✓

---

## Revised Sheet Structure (Final)

### 6 Sheets (Fixed Count)

1. **Macro Drivers** (~20 rows, static)
2. **Sector Drivers** (~500 rows, grows slowly with sectors)
3. **Company Drivers** (~25,000 rows for 1000 companies)
4. **Valuation History - Last 90 Days** (~90,000 rows, rolling window)
5. **Driver History - Last 90 Days** (~90,000 rows, rolling window)
6. **Event Log - Last 30 Days** (~3,000 rows, rolling window)

**Total Cells**: ~3.4M (34% of 10M limit) ✓ Sustainable

**Archive Strategy**: MySQL stores all history, GSheet is working memory

---

## Implementation Changes Needed

### Update gsheet_client.py

**Current approach** (doesn't scale):
```python
SHEET_NAMES = {
    'macro_drivers': 'Macro Drivers',
    'sector_chemicals': 'Sector - Specialty Chemicals',  # ❌ One per sector
    'company_aether': 'Company - Aether Industries',     # ❌ One per company
    ...
}
```

**New approach** (scalable):
```python
SHEET_NAMES = {
    'macro_drivers': 'Macro Drivers',
    'sector_drivers': 'Sector Drivers',        # ✓ Single sheet, filter by sector
    'company_drivers': 'Company Drivers',      # ✓ Single sheet, filter by company_id
    'valuation_history': 'Valuation History',
    'driver_history': 'Driver History',
    'event_log': 'Event Log',
}

def get_sector_drivers(self, sector_name: str) -> list:
    """Get drivers for a specific sector from single sheet."""
    ws = self.spreadsheet.worksheet('Sector Drivers')
    all_records = ws.get_all_records()

    # Filter by sector
    sector_drivers = [r for r in all_records if r.get('sector') == sector_name]
    return sector_drivers

def get_company_drivers(self, company_id: int) -> list:
    """Get drivers for a specific company from single sheet."""
    ws = self.spreadsheet.worksheet('Company Drivers')
    all_records = ws.get_all_records()

    # Filter by company_id
    company_drivers = [r for r in all_records if r.get('company_id') == company_id]
    return company_drivers
```

---

## Performance Considerations

### Read Performance

**Challenge**: Reading 25,000 rows on every valuation run

**Solution**: MySQL Cache Layer
```python
# Daily at 06:00: Sync GSheet → MySQL
def sync_drivers_to_mysql():
    """Copy all driver current values from GSheet to MySQL."""
    # Read once per day, cache in MySQL
    # Valuation runs read from MySQL (fast)
    # GSheet is source of truth, MySQL is cache
```

**Query Time**:
- GSheet API: ~5-10 seconds for 25K rows
- MySQL: <100ms for same data ✓

**Strategy**:
- Hourly: Read only **changed** drivers from GSheet (use "last_updated" filter)
- Daily: Full sync GSheet → MySQL
- Valuation: Always read from MySQL cache

---

### Write Performance

**Challenge**: Updating 1 driver in 25,000 rows

**Solution**: Smart Writes
```python
def update_driver_value(self, company_id: int, driver_name: str, new_value: str):
    """Update single driver efficiently."""
    ws = self.spreadsheet.worksheet('Company Drivers')

    # Find row: Filter on company_id + driver_name
    # Use gspread's find() with column search
    cell = ws.find(driver_name, in_column=5)  # driver_name column

    # Verify company_id matches (could be multiple companies with same driver name)
    if cell:
        company_id_col = ws.cell(cell.row, 2).value  # column B = company_id
        if company_id_col == company_id:
            ws.update_cell(cell.row, 4, new_value)  # column D = current_value
            ws.update_cell(cell.row, 12, datetime.now())  # column L = last_updated
```

**Write Time**: ~1-2 seconds per driver update ✓ Acceptable

---

## Migration Plan

### From Current (Per-Company Sheets) → Scalable (Single Sheets)

**Step 1**: Create new 6-sheet structure
**Step 2**: Migrate existing data:
- Aether drivers → Rows in "Company Drivers" (company_id=47582)
- Eicher drivers → Rows in "Company Drivers" (company_id=39424)
- Chemicals sector → Rows in "Sector Drivers" (sector='Chemicals')
- Auto sector → Rows in "Sector Drivers" (sector='Automobile & Ancillaries')

**Step 3**: Update gsheet_client.py with new query patterns
**Step 4**: Test with 2 companies
**Step 5**: Scale to full watchlist

---

## Comparison: Old vs New

| Aspect | Old Design | New Design | Winner |
|--------|-----------|------------|--------|
| **Scalability** | 1000 companies = 1000+ sheets ❌ | 6 sheets total ✓ | ✅ NEW |
| **Google Sheets Limit** | Exceeds 200 sheet limit | Always 6 sheets | ✅ NEW |
| **Cell Count** | ~10M cells Year 1 ⚠️ | ~3.4M cells (with archive) | ✅ NEW |
| **Query Speed** | Fast (direct sheet access) | Slower (row filtering) | ❌ OLD |
| **PM Usability** | Easy (dedicated sheets) | Harder (need filtering) | ❌ OLD |
| **Maintenance** | Hard (1000 sheets to manage) | Easy (6 sheets) | ✅ NEW |
| **Overall** | - | - | ✅ **NEW** |

---

## Recommendation

**Use New Scalable Structure (6 sheets)**:
1. Macro Drivers
2. Sector Drivers
3. Company Drivers
4. Valuation History (90-day rolling)
5. Driver History (90-day rolling)
6. Event Log (30-day rolling)

**With**:
- MySQL as primary storage (full history)
- GSheet as working memory (recent + current state)
- Daily sync: GSheet ↔ MySQL

**Benefits**:
- ✅ Scales to 1000+ companies
- ✅ Stays within Google Sheets limits
- ✅ Fast queries via MySQL
- ✅ PM can still edit drivers in GSheet (filter view)

---

**Shall I create the revised scalable templates?**
