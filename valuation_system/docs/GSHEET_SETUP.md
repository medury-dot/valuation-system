# Google Sheet Setup Guide
**For Valuation System Driver Tracking**

---

## Quick Setup (5 minutes)

### Step 1: Create Google Sheet

1. Go to: **https://sheets.google.com**
2. Click **"Blank"** to create new spreadsheet
3. Name it: **"Valuation System - Driver Tracking"**

---

### Step 2: Share with Service Account

1. Click **"Share"** button (top right)
2. Add this email as **Editor**:
   ```
   smstormservice@smstorm.iam.gserviceaccount.com
   ```
3. Click **"Send"**

---

### Step 3: Import CSV Templates (8 sheets)

**For each CSV template** in `/Users/ram/code/research/valuation_system/docs/`:

1. In Google Sheet: **File → Import**
2. Click **"Upload"** tab
3. Drag & drop the CSV file (or browse)
4. **Import location**: Select **"Insert new sheet"**
5. **Separator type**: Comma
6. Click **"Import data"**

**Import these 8 files in order**:

1. ✅ `macro_drivers_template.csv` → Sheet: "Macro Drivers"
2. ✅ `sector_specialty_chemicals_template.csv` → Sheet: "Sector - Specialty Chemicals"
3. ✅ `sector_automobiles_template.csv` → Sheet: "Sector - Automobiles"
4. ✅ `company_aether_template.csv` → Sheet: "Company - Aether Industries"
5. ✅ `company_eicher_template.csv` → Sheet: "Company - Eicher Motors"
6. ✅ `valuation_history_template.csv` → Sheet: "Valuation History"
7. ✅ `driver_history_template.csv` → Sheet: "Driver History"
8. ✅ `event_log_template.csv` → Sheet: "Event Log"

**Tip**: Rename sheets after import to match the names above exactly.

---

### Step 4: Get Spreadsheet ID

1. Look at the URL in your browser:
   ```
   https://docs.google.com/spreadsheets/d/SPREADSHEET_ID_HERE/edit
                                          ^^^^^^^^^^^^^^^^
   ```
2. Copy the **SPREADSHEET_ID** (between `/d/` and `/edit`)

**Example**:
```
URL: https://docs.google.com/spreadsheets/d/1AbC123xyz456/edit
ID:  1AbC123xyz456
```

---

### Step 5: Update .env File

1. Open: `/Users/ram/code/research/valuation_system/config/.env`
2. Find line: `GSHEET_DRIVERS_ID=`
3. Update with your ID:
   ```bash
   GSHEET_DRIVERS_ID=1AbC123xyz456
   ```
4. Save the file

---

### Step 6: Test Connection

Run this command to verify connection:

```bash
cd /Users/ram/code/research
python3 << 'EOF'
from valuation_system.storage.gsheet_client import GSheetClient
import os
from dotenv import load_dotenv

load_dotenv('/Users/ram/code/research/valuation_system/config/.env')

gsheet_id = os.getenv('GSHEET_DRIVERS_ID')
print(f"Testing connection to: {gsheet_id}")

try:
    client = GSheetClient()
    spreadsheet = client.spreadsheet
    print(f"✓ Connected successfully!")
    print(f"  Title: {spreadsheet.title}")
    print(f"  Sheets: {[ws.title for ws in spreadsheet.worksheets()]}")
except Exception as e:
    print(f"✗ Connection failed: {e}")
EOF
```

**Expected output**:
```
✓ Connected successfully!
  Title: Valuation System - Driver Tracking
  Sheets: ['Macro Drivers', 'Sector - Specialty Chemicals', ...]
```

---

## What Each Sheet Contains

### 1. Macro Drivers (20% weight)
- GDP growth, inflation, USD/INR, crude oil, interest rates
- Updated: Hourly (from news)
- Used by: All companies

### 2. Sector - Specialty Chemicals (55% weight)
- Demand: volume_growth, china_plus_one, pharma_api_demand
- Cost: crude_oil, natural_gas, operating_leverage
- Regulatory: PLI scheme, REACH compliance, FDA approvals
- Updated: Hourly (from news)
- Used by: Aether

### 3. Sector - Automobiles (55% weight)
- Demand: industry_volume, rural/urban demand, exports
- Cost: steel, aluminum, semiconductors, battery costs
- Updated: Hourly (from news)
- Used by: Eicher

### 4. Company - Aether Industries (25% weight)
- R&D intensity, CRAMS %, export %, promoter holding
- Market share, capex plans, balance sheet
- Updated: Quarterly (from results)

### 5. Company - Eicher Motors (25% weight)
- Premium 2W share, export growth, VECV JV
- Brand building, cash position
- Updated: Monthly (from SIAM data, results)

### 6. Valuation History
- Timeline of all valuations
- Tracks intrinsic value changes over time
- Links to driver changes

### 7. Driver History
- Audit trail of all driver updates
- Shows old → new values with reasoning
- Links to source documents

### 8. Event Log
- All news events processed
- Severity, synopsis, ChromaDB links

---

## How Drivers Flow Through the System

```
1. NEWS SCAN (Hourly)
   ↓
2. LLM CLASSIFIES event → Identifies affected drivers
   ↓
3. GOOGLE SHEET UPDATED (source of truth)
   ↓
4. MYSQL SYNCED (fast query cache)
   ↓
5. DAILY VALUATION reads from MySQL
   ↓
6. DCF/RELATIVE/MC calculations use driver-adjusted assumptions
```

**PM Can Override**: Edit "Current" column in Google Sheet anytime
- Manual edits take priority over LLM updates
- System reads from sheet before each valuation run

---

## Troubleshooting

**Service account quota error?**
- Use your personal Google account instead
- Share the sheet with service account as Editor
- Service account can still read/write, just can't create

**Sheets not showing in system?**
- Check sheet names match exactly (case-sensitive)
- Verify GSHEET_DRIVERS_ID in .env is correct
- Run test connection script above

**Driver updates not working?**
- Check internet connection
- Verify service account has Editor permission
- Check logs: `tail -f logs/hourly.log`

---

## After Setup

### Test the system:

```bash
# Run hourly cycle (should read from Google Sheet)
python -m valuation_system.scheduler.runner hourly

# Check if drivers were loaded
grep "Loaded.*drivers from DB" logs/hourly.log
```

### Update a driver manually:

1. Open Google Sheet
2. Navigate to "Sector - Specialty Chemicals"
3. Find row with driver "crude_oil"
4. Change "Current" column: 85 → 90
5. Next valuation will use $90

---

## Template Files Reference

All templates are in: `/Users/ram/code/research/valuation_system/docs/`

| File | Import As Sheet | Pre-filled Rows |
|------|-----------------|-----------------|
| `macro_drivers_template.csv` | Macro Drivers | 5 drivers |
| `sector_specialty_chemicals_template.csv` | Sector - Specialty Chemicals | 13 drivers |
| `sector_automobiles_template.csv` | Sector - Automobiles | 10 drivers |
| `company_aether_template.csv` | Company - Aether Industries | 22 alpha drivers |
| `company_eicher_template.csv` | Company - Eicher Motors | 10 alpha drivers |
| `valuation_history_template.csv` | Valuation History | 2 initial entries |
| `driver_history_template.csv` | Driver History | 1 example |
| `event_log_template.csv` | Event Log | Empty (headers only) |

---

**Ready to go!** Once you create the sheet and update .env, the system will automatically sync drivers.
