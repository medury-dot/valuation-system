# Company Drivers Sheet - Detailed Structure
**Sheet 3 of 6 - Scalable to 1000+ Companies**

---

## Overview

**Sheet Name**: `3. Company Drivers`

**Purpose**: Store alpha drivers for ALL companies in single sheet

**Scalability**:
- 2 companies = 32 rows ✓
- 100 companies = ~2,500 rows ✓
- 1000 companies = ~25,000 rows ✓
- **Google Sheets limit**: 10 million cells (we'd use 325K) ✓

**Query Pattern**: Filter by `company_id` or `nse_symbol` to get one company's drivers

---

## Column Structure (14 columns)

| # | Column | Type | Width | Example | Purpose |
|---|--------|------|-------|---------|---------|
| A | **driver_id** | INT | 60px | 10000 | Unique ID (auto-increment) |
| B | **company_id** | INT | 80px | 47582 | Foreign key to kbapp_marketscrip.marketscrip_id |
| C | **company_name** | TEXT | 200px | Aether Industries Ltd. | Human-readable (for PM) |
| D | **nse_symbol** | TEXT | 100px | AETHER | For filtering/lookup |
| E | **sector** | TEXT | 150px | Chemicals | For sector grouping |
| F | **category** | TEXT | 120px | Market Share | Groups related drivers (9 categories) |
| G | **driver_name** | TEXT | 180px | share_trajectory | Unique driver identifier |
| H | **current_value** | TEXT | 100px | +2.5% | **PM-EDITABLE** ← Main input field |
| I | **vs_peers** | TEXT | 120px | Above avg | Relative positioning context |
| J | **weight** | DECIMAL | 80px | 0.06 | Impact weight (sum to 0.25 per company) |
| K | **metric** | TEXT | 100px | YoY Δ | Unit of measurement |
| L | **alpha_impact** | TEXT | 200px | ROCE premium in Relative Val | How it affects valuation |
| M | **last_updated** | DATETIME | 140px | 2026-02-05 10:15 | Auto-updated timestamp |
| N | **source** | TEXT | 150px | Company filings | Data source |

**Key Field**: Column H (`current_value`) is what PM edits to update driver

---

## Data Layout Pattern

### For Each Company: 22-25 Rows (Alpha Drivers)

**Driver Categories** (9 categories × 2-3 drivers each):

```
Row Structure for Aether Industries (company_id = 47582):
═══════════════════════════════════════════════════════════════════

Rows 10000-10001: MARKET SHARE (2 drivers, 0.11 total weight)
  ├─ share_trajectory (0.06) - YoY market share change
  └─ segment_leadership (0.05) - Position in niche (e.g., #3 in CRAMS)

Rows 10002-10004: MGMT QUALITY (3 drivers, 0.15 total weight)
  ├─ capital_allocation (0.06) - Track record score 1-10
  ├─ r_and_d_intensity (0.05) - R&D spend as % of sales
  └─ execution_track_record (0.04) - Project delivery rating

Rows 10005-10007: CAPEX (3 drivers, 0.13 total weight)
  ├─ expansion_plans (0.05) - Announced capex in Rs Cr
  ├─ execution_risk (0.04) - ON_TRACK / AT_RISK / DELAYED
  └─ roi_on_past_capex (0.04) - Historical ROI %

Rows 10008-10009: GEOGRAPHIC MIX (2 drivers, 0.07 total weight)
  ├─ export_pct (0.04) - Export as % of revenue
  └─ fx_exposure (0.03) - Net currency impact

Rows 10010-10012: PRODUCT MIX (3 drivers, 0.12 total weight)
  ├─ crams_pct (0.05) - CRAMS revenue %
  ├─ new_molecule_pipeline (0.04) - Count of molecules in development
  └─ customer_stickiness (0.03) - Average customer tenure

Rows 10013-10014: BALANCE SHEET (2 drivers, 0.06 total weight)
  ├─ net_debt_ebitda (0.04) - Leverage ratio
  └─ interest_coverage (0.02) - EBITDA / Interest

Rows 10015-10017: GOVERNANCE (3 drivers, 0.07 total weight)
  ├─ promoter_holding (0.03) - Promoter ownership %
  ├─ pledge_pct (0.02) - Pledged shares %
  └─ related_party_risk (0.02) - RPT risk score

Rows 10018-10019: KEY PERSONNEL (2 drivers, 0.04 total weight)
  ├─ ceo_cfo_stability (0.02) - Leadership tenure
  └─ key_scientist_exits (0.02) - Talent retention (for R&D companies)

Rows 10020-10021: LITIGATION (2 drivers, 0.03 total weight)
  ├─ pending_cases (0.01) - Count of ongoing cases
  └─ regulatory_actions (0.02) - SEBI/FDA/Pollution board issues
```

**Total Weight per Company**: 0.25 (25% in driver hierarchy)

---

## Sample Data (First 10 Rows)

```
driver_id | company_id | company_name           | nse_symbol | sector    | category      | driver_name          | current_value | vs_peers    | weight | metric     | alpha_impact             | last_updated     | source
─────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────
10000     | 47582      | Aether Industries Ltd. | AETHER     | Chemicals | Market Share  | share_trajectory     | +2.5%         | Above avg   | 0.06   | YoY Δ      | ROCE premium             |                  | Company filings
10001     | 47582      | Aether Industries Ltd. | AETHER     | Chemicals | Market Share  | segment_leadership   | #3 in CRAMS   | Mid-tier    | 0.05   | Position   | ROCE premium             |                  | Industry analysis
10002     | 47582      | Aether Industries Ltd. | AETHER     | Chemicals | Mgmt Quality  | capital_allocation   | 8/10          | Above avg   | 0.06   | Score /10  | ROCE, terminal reinvest  |                  | Track record
10003     | 47582      | Aether Industries Ltd. | AETHER     | Chemicals | Mgmt Quality  | r_and_d_intensity    | 4.5%          | 3.2% avg    | 0.05   | % Sales    | ROCE, terminal margin    |                  | Filings
10004     | 47582      | Aether Industries Ltd. | AETHER     | Chemicals | Mgmt Quality  | execution_track_rec  | STRONG        | Good        | 0.04   | Rating     | ROCE, confidence         |                  | Project history
10005     | 47582      | Aether Industries Ltd. | AETHER     | Chemicals | Capex         | expansion_plans      | 300           | High        | 0.05   | Rs Cr      | Revenue growth, Capex    |                  | Guidance
10006     | 47582      | Aether Industries Ltd. | AETHER     | Chemicals | Capex         | execution_risk       | ON_TRACK      | Low risk    | 0.04   | Status     | Revenue growth timeline  |                  | Updates
10007     | 47582      | Aether Industries Ltd. | AETHER     | Chemicals | Capex         | roi_on_past_capex    | 18%           | Above sect  | 0.04   | %          | Capex efficiency, ROCE   |                  | Financial analysis
10008     | 47582      | Aether Industries Ltd. | AETHER     | Chemicals | Geographic    | export_pct           | 60%           | High        | 0.04   | % Revenue  | Diversification, FX      |                  | Filings
10009     | 47582      | Aether Industries Ltd. | AETHER     | Chemicals | Geographic    | fx_exposure          | POSITIVE      | Exporter    | 0.03   | Net Impact | Margin hedge from rupee  |                  | Currency analysis
```

---

## How to Use (PM Workflow)

### Scenario: Update Aether's R&D Intensity After Q3 Results

**Manual Update in Google Sheet**:
1. Open sheet: "3. Company Drivers"
2. Apply filter:
   - Column B (`company_id`) = 47582
   - OR Column D (`nse_symbol`) = "AETHER"
3. Find row: `driver_name` = "r_and_d_intensity"
4. Update Column H (`current_value`): 4.5% → **4.8%**
5. Column M (`last_updated`) auto-updates to current timestamp
6. Add note in Column N (`source`): "Q3 FY2026 results"

**System Behavior**:
- Next hourly job: Syncs change to MySQL `vs_drivers` table
- Next daily job: Uses new 4.8% value in valuation
- Impact: Slight increase in ROCE premium → +0.5% relative valuation

---

### Scenario: Compare R&D Intensity Across All Companies

**Query in Google Sheet**:
1. Apply filter on Column G (`driver_name`) = "r_and_d_intensity"
2. Sort by Column H (`current_value`) descending
3. See which companies invest most in R&D

**Expected Results** (example):
```
Company              | R&D Intensity | vs Peers
──────────────────────────────────────────────────
PI Industries        | 6.2%          | Leader
Aether Industries    | 4.8%          | Above avg
Aarti Industries     | 3.5%          | Average
SRF                  | 2.8%          | Below avg
```

---

## Adding New Companies

### For Each New Company (e.g., Company ID 50000, Symbol XYZ):

**Add 22-25 rows** to sheet "3. Company Drivers":

```python
# Python script to generate rows:
company_id = 50000
company_name = "XYZ Industries Ltd."
symbol = "XYZ"
sector = "Chemicals"
starting_driver_id = 50000

rows = [
    # Market Share
    (starting_driver_id, company_id, company_name, symbol, sector,
     'Market Share', 'share_trajectory', 'TBD', 'TBD', 0.06, 'YoY Δ',
     'ROCE premium', '', 'Company filings'),

    # ... (20 more rows following the template)
]

# Append to Google Sheet
worksheet.append_rows(rows)
```

**Scalability**:
- Company 1 (Aether): Rows 10000-10021 (22 rows)
- Company 2 (Eicher): Rows 20000-20009 (10 rows)
- Company 3 (XYZ): Rows 30000-30024 (25 rows)
- ...
- Company 1000: Rows 1000000-1000024 (25 rows)

**Total for 1000 companies**: ~25,000 rows (avg 25 drivers/company)

---

## Filtering & Querying

### In Google Sheets (Manual)

**Get all drivers for Aether**:
```
1. Click column B header → "Create a filter"
2. Filter: company_id = 47582
3. Result: 22 rows for Aether
```

**Get all companies with high R&D intensity**:
```
1. Filter: driver_name = "r_and_d_intensity"
2. Filter: current_value > 4%
3. Result: All R&D-intensive companies
```

---

### In Code (Programmatic)

**Python (gsheet_client.py)**:
```python
def get_company_drivers(self, company_id: int) -> list:
    """Get all drivers for one company."""
    ws = self.spreadsheet.worksheet('3. Company Drivers')
    all_records = ws.get_all_records()

    # Filter by company_id
    company_drivers = [
        r for r in all_records
        if r.get('company_id') == company_id
    ]

    return company_drivers

# Usage:
aether_drivers = client.get_company_drivers(47582)
# Returns: [{driver_name: 'share_trajectory', current_value: '+2.5%', ...}, ...]
```

**SQL (after sync to MySQL)**:
```sql
-- Get all Aether drivers
SELECT * FROM vs_drivers
WHERE driver_level = 'COMPANY'
  AND company_id = 47582;

-- Get R&D intensity for all companies
SELECT company_name, current_value as r_and_d_pct
FROM vs_drivers
WHERE driver_level = 'COMPANY'
  AND driver_name = 'r_and_d_intensity'
ORDER BY CAST(current_value AS DECIMAL) DESC;

-- Get top 10 companies by promoter holding
SELECT company_name, nse_symbol, current_value as promoter_pct
FROM vs_drivers
WHERE driver_name = 'promoter_holding'
ORDER BY CAST(REPLACE(current_value, '%', '') AS DECIMAL) DESC
LIMIT 10;
```

---

## Full Column Details

### Column A: driver_id (Primary Key)

**Type**: Integer
**Range**: 10000-999999999
**Pattern**:
- Aether: 10000-10099
- Eicher: 20000-20099
- Company 3: 30000-30099
- ...
- Company 1000: 1000000-1000099

**Purpose**: Unique identifier for each driver instance

---

### Column B: company_id (Foreign Key)

**Type**: Integer
**Source**: `mssdb.kbapp_marketscrip.marketscrip_id`
**Examples**:
- Aether: 47582
- Eicher: 39424
- Reliance: 13611
- TCS: 16669

**Purpose**: Links to company master (enables JOIN queries)

**Constraint**: Must exist in kbapp_marketscrip

---

### Column C: company_name

**Type**: Text (max 100 chars)
**Source**: `mssdb.kbapp_marketscrip.name`
**Purpose**: Human-readable (PM doesn't remember IDs)

**Format**: Exact match from company master
- ✅ "Aether Industries Ltd."
- ❌ "Aether" (abbreviated)

---

### Column D: nse_symbol

**Type**: Text (max 20 chars)
**Source**: `mssdb.kbapp_marketscrip.symbol`
**Purpose**: Easier filtering for PM

**Examples**: AETHER, EICHERMOT, RELIANCE, TCS

---

### Column E: sector

**Type**: Text (max 50 chars)
**Source**: `mssdb.kbapp_marketscrip.sector` OR `CD_Sector` from CSV
**Purpose**: For sector-level aggregations

**Values**: Chemicals, Automobile & Ancillaries, IT, Pharma, FMCG, etc.

---

### Column F: category (Driver Category)

**Type**: Text (max 30 chars)
**Values** (9 categories):
1. **Market Share** - Competitive positioning
2. **Mgmt Quality** - Management excellence
3. **Capex** - Capital allocation plans
4. **Geographic Mix** - Regional diversification
5. **Product Mix** - Product/service portfolio
6. **Balance Sheet** - Financial health
7. **Governance** - Corporate governance quality
8. **Key Personnel** - Leadership stability
9. **Litigation** - Legal/regulatory risks

**Custom Categories** (company-specific):
- Eicher: "VECV JV" (joint venture specific)
- Banks: "Asset Quality" (NPAs, credit costs)
- IT: "Client Concentration" (top client risk)

---

### Column G: driver_name (Driver Identifier)

**Type**: Text (max 50 chars)
**Format**: snake_case
**Examples**:
- share_trajectory
- r_and_d_intensity
- crams_pct
- promoter_holding

**Must be unique** within a company (but same driver can exist for multiple companies)

---

### Column H: current_value ⭐ (PM-EDITABLE)

**Type**: Text (stores as text to handle mixed formats)
**Formats**:
- Percentage: "4.5%", "60%"
- Ratio: "0.8x", "12x"
- Absolute: "300" (Rs Cr), "8" (count)
- Qualitative: "STRONG", "HIGH", "ON_TRACK"
- Position: "#3 in CRAMS", "Leader"

**This is the MAIN field PM edits**

**Examples**:
```
Driver                | Type        | Current Value
───────────────────────────────────────────────────
r_and_d_intensity     | Percentage  | 4.5%
net_debt_ebitda       | Ratio       | 0.8x
expansion_plans       | Absolute    | 300
execution_track_record| Qualitative | STRONG
segment_leadership    | Position    | #3 in CRAMS
```

---

### Column I: vs_peers (Peer Comparison)

**Type**: Text (max 50 chars)
**Purpose**: Contextual information for PM

**Values**:
- "Leader", "Above avg", "Average", "Below avg", "Laggard"
- "Best-in-class", "Top quartile", "Mid-tier"
- Specific: "3.2% peer avg" (shows peer benchmark)

**Not used in calculations** - just reference for PM

---

### Column J: weight (Driver Weight)

**Type**: Decimal (0.01 to 0.10)
**Range**: 0.01 to 0.10 per driver
**Sum**: Should total 0.25 (25%) per company

**Distribution** (Aether example):
```
Category         | Drivers | Total Weight
───────────────────────────────────────────
Market Share     | 2       | 0.11
Mgmt Quality     | 3       | 0.15 ← Highest
Capex            | 3       | 0.13
Geographic Mix   | 2       | 0.07
Product Mix      | 3       | 0.12
Balance Sheet    | 2       | 0.06
Governance       | 3       | 0.07
Key Personnel    | 2       | 0.04
Litigation       | 2       | 0.03
───────────────────────────────────────────
TOTAL            | 22      | 0.25 ✓
```

---

### Column K: metric (Unit)

**Type**: Text (max 30 chars)
**Purpose**: Unit of measurement for current_value

**Examples**:
- "%", "YoY Δ", "% Revenue", "% Sales"
- "Ratio", "x" (for multiples)
- "Score /10", "Rating", "Status"
- "Rs Cr", "Count", "Tenure"

---

### Column L: alpha_impact (How Driver Affects Valuation)

**Type**: Text (max 100 chars)
**Purpose**: Explains valuation linkage

**Examples**:
```
Driver                | Alpha Impact
──────────────────────────────────────────────────────────────
r_and_d_intensity     | ROCE, terminal margin (sustains pricing power)
crams_pct             | Margin, ASP growth (high-margin business)
promoter_holding      | Governance quality discount in Relative Val
net_debt_ebitda       | Debt ratio in WACC, cash position
execution_risk        | Revenue growth timeline, confidence score
```

---

### Column M: last_updated (Timestamp)

**Type**: DateTime
**Format**: YYYY-MM-DD HH:MM
**Auto-updated**: When Column H changes

**Workflow**:
1. PM edits current_value
2. System detects change (hourly sync)
3. Updates last_updated timestamp
4. Writes to driver_history sheet (audit trail)

---

### Column N: source (Data Source)

**Type**: Text (max 100 chars)
**Purpose**: Where did this value come from?

**Examples**:
- "Company filings" (annual report, investor presentation)
- "Q3 FY2026 results" (quarterly earnings)
- "Management guidance" (concall)
- "Industry analysis" (3rd party reports)
- "Financial analysis" (our calculation)

---

## Growth Company vs Mature Company

### Aether Industries (High-Growth)

**22 Alpha Drivers** - Emphasis on growth/quality:

**Heavy on**:
- ✅ R&D intensity (0.05) - Innovation
- ✅ Execution track record (0.04) - Growth delivery
- ✅ Expansion plans (0.05) - Future capacity
- ✅ CRAMS % (0.05) - High-margin business
- ✅ Customer stickiness (0.03) - Retention

**Light on**:
- Balance sheet (0.06 total) - Not debt-driven
- Litigation (0.03 total) - Clean record

---

### Eicher Motors (Mature)

**10 Alpha Drivers** - Emphasis on market dominance:

**Heavy on**:
- ✅ Premium 2W share (0.08) - Market leadership
- ✅ Export growth (0.04) - New growth driver
- ✅ Brand building (0.05) - Moat strength
- ✅ VECV JV (0.07 total) - Cyclical opportunity

**Light on**:
- R&D (not a key driver for bikes)
- Product mix (focused business)

**Key Difference**: Eicher-specific category "VECV JV" not in Aether

---

## Scalability Example: 1000 Companies

### Sheet 3 with 1000 Companies

**Row Count**: ~25,000 (1000 companies × 25 drivers avg)

**Sample Layout**:
```
Rows 10000-10024: Aether Industries (25 drivers)
Rows 20000-20009: Eicher Motors (10 drivers)
Rows 30000-30022: Reliance Industries (23 drivers)
Rows 40000-40019: TCS (20 drivers)
...
Rows 1000000-1000024: Company 1000 (25 drivers)
```

**Cell Count**: 25,000 rows × 14 columns = **350,000 cells** (3.5% of 10M limit) ✓

**Query Time** (Google Sheets API):
- Read all 25,000 rows: ~5-10 seconds (acceptable for daily sync)
- Update 1 driver: ~1-2 seconds (acceptable for hourly updates)

**After MySQL Sync** (fast queries):
```sql
-- Get Aether drivers: <100ms
SELECT * FROM vs_drivers WHERE company_id = 47582;

-- Compare all companies' promoter holding: <500ms
SELECT company_name, current_value
FROM vs_drivers
WHERE driver_name = 'promoter_holding'
ORDER BY CAST(current_value AS DECIMAL) DESC;
```

---

## Field Validation Rules

### Recommended Data Validation (in Google Sheets)

**Column H (current_value)** - Set validation based on driver type:

**For percentage drivers** (r_and_d_intensity, export_pct, etc.):
- Validation: Custom formula
- Rule: `=REGEXMATCH(H2, "^\d+(\.\d+)?%$")`
- Error: "Must be in format: 4.5%"

**For ratio drivers** (net_debt_ebitda, interest_coverage):
- Validation: Number
- Min: 0, Max: 100
- Error: "Must be numeric ratio"

**For qualitative drivers** (execution_risk, fx_exposure):
- Validation: List from range
- Values: "STRONG, MODERATE, WEAK" or "HIGH, MODERATE, LOW"
- Error: "Select from dropdown"

---

## Summary: Why This Structure Scales

✅ **Fixed Sheet Count**: Always 6 sheets (not 1000+)
✅ **Efficient Storage**: 350K cells for 1000 companies (3.5% of limit)
✅ **Fast Queries**: MySQL cache layer for sub-second reads
✅ **Easy Updates**: PM edits one cell, system syncs automatically
✅ **Audit Trail**: Every change logged in Sheet 5 (Driver History)
✅ **Flexible**: Add companies by appending rows (no new sheets needed)

---

**Ready to create the Google Sheet with this structure?**

Next: Import these 6 CSV files into Google Sheets as separate sheets.
