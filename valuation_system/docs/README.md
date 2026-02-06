# Valuation System Documentation

**Agentic Valuation System - Quick Reference**

---

## 📚 Documentation Files

1. **[VALUATION_PROCESS.md](./VALUATION_PROCESS.md)** ← **START HERE**
   - Complete system architecture
   - Driver hierarchy (Macro 20%, Sector 55%, Company 25%)
   - Agent ecosystem and workflows
   - Scheduled jobs and automation
   - LLM integration points
   - Setup and configuration
   - Operational procedures

---

## 🎯 Quick Start

### For Product Managers

**Daily Workflow**:
```
Morning (08:00):  Review social media drafts in Google Sheets → Approve
Evening (20:30):  Check email for valuation alerts → Review if >5% change
Weekly (Sunday):  Review accuracy report → Adjust drivers if needed
```

**Manual Valuation**:
```bash
python -m valuation_system.scheduler.runner valuation --symbol AETHER
```

**Override Driver Values**:
1. Open Google Sheets (GSHEET_DRIVERS_ID)
2. Edit "Current" column for any driver
3. Manual values take priority over LLM updates

---

### For Developers

**Installation**:
```bash
cd /Users/ram/code/research/valuation_system
pip install -r requirements.txt
python -m valuation_system.scheduler.runner init
```

**Run Tests**:
```bash
python -m valuation_system.scheduler.runner test
# Should show 36/36 passing
```

**Check System Status**:
```bash
python -m valuation_system.scheduler.runner status
```

**Manual Runs**:
```bash
# Hourly cycle (news scan + driver updates)
python -m valuation_system.scheduler.runner hourly

# Daily valuation (full portfolio)
python -m valuation_system.scheduler.runner daily

# Social media drafts
python -m valuation_system.scheduler.runner social

# Single company
python -m valuation_system.scheduler.runner valuation --symbol AETHER

# Catchup after downtime
python -m valuation_system.scheduler.runner catchup
```

---

## 🏗️ System Architecture (High-Level)

```
SCHEDULED JOBS (launchd)
├─ Hourly (Every 60 min)     → News scan → LLM classify → Update drivers
├─ Daily (20:00 IST)         → Full valuation → Alerts → Email digest
├─ Social (08:00 IST)        → Generate tweet drafts → GSheet for approval
└─ Regression (06:00 IST)    → Run 36 tests → Email results

AGENTS
├─ Orchestrator              → Main coordinator, catchup handler
├─ NewsScanner               → Scrape ET/MC/BSE → LLM classify
├─ SectorAnalyst (per sector)→ LLM driver impact analysis
├─ Valuator                  → DCF/Relative/MonteCarlo → Blend (60/30/10)
└─ ContentAgent              → Generate social media insights

DATA STORES
├─ MySQL (rag database)      → vs_valuations, vs_drivers, vs_news (14 tables)
├─ Google Sheets             → Driver values (PM-editable source of truth)
├─ ChromaDB                  → News RAG, document storage
└─ CSV Files                 → Core financials, monthly prices, macro data
```

---

## 🔑 Key Concepts

### Driver Hierarchy

**Total Weight = 100%**

1. **MACRO (20%)**: GDP, interest rates, crude oil, USD/INR
   - Sets baseline for all sectors

2. **SECTOR (55%)**: Industry-specific drivers
   - Specialty Chemicals: china_plus_one, crude_oil, pharma_api_demand, PLI scheme
   - Automobiles: industry_volume, rural_demand, steel_prices, EV transition

3. **COMPANY (25%)**: Alpha drivers
   - Aether: R&D intensity, CRAMS %, export %, promoter holding
   - Eicher: Premium 2W share, export growth, VECV JV profitability

**Principle**: *"Macro sets the ceiling and floor. Sectors determine the value. Companies decide who wins."*

---

### Valuation Methodology

**Blended Intrinsic Value** = 60% DCF + 30% Relative + 10% Monte Carlo

**DCF**:
- 10-year projections with driver-adjusted growth/margins
- Terminal growth = Reinvestment Rate × ROCE (capped 2-5%)
- WACC from Damodaran sector betas

**Relative**:
- 2-tier peer selection (tight + broad)
- Multiples adjusted by sector outlook score

**Monte Carlo**:
- 1,000 simulations with randomized inputs
- Captures uncertainty in driver values

---

### LLM Integration

**5 Touchpoints**:
1. **News Classification**: Categorize articles (MACRO/SECTOR/COMPANY)
2. **Driver Impact**: Analyze which drivers to update
3. **Driver Synthesis**: Convert driver states → valuation parameters
4. **Content Generation**: Draft social media posts
5. **Commentary** (optional): Explain valuation vs market gap

**Fallback Chain**: Grok (primary) → Ollama → OpenAI

---

## 📊 Example: China Tariff Event

**Time: 10:00 AM - Hourly Job Runs**

1. **NewsScannerAgent** scrapes Economic Times
   - Finds: "China imposes 10% export tariff on specialty chemicals"

2. **LLM Classification**:
   ```json
   {
     "category": "REGULATORY",
     "scope": "SECTOR",
     "affected_sector": "specialty_chemicals",
     "severity": "HIGH"
   }
   ```

3. **SectorAnalyst** (Specialty Chemicals) analyzes impact:
   ```json
   {
     "affected_drivers": [
       {
         "driver_name": "china_plus_one",
         "old_state": "MODERATE",
         "new_state": "HIGH",
         "revenue_impact": +2.5%
       }
     ]
   }
   ```

4. **Updates Google Sheets**:
   - Sheet: "Sector - Specialty Chemicals"
   - Row: china_plus_one
   - Current: MODERATE → HIGH

5. **Syncs to MySQL** `vs_drivers` table

**Time: 20:00 - Daily Job Runs**

6. **ValuatorAgent** for Aether:
   - Reads driver: china_plus_one = HIGH
   - LLM synthesis: Revenue growth adjustment +1.5%
   - DCF recalculation: Rs 1,593 per share
   - Relative valuation: Rs 4,554 per share
   - Blended: Rs 2,483 per share (up 7.5% from yesterday)

7. **Alert Triggered** (>5% change):
   - Email sent to medury@gmail.com
   - "Aether intrinsic up 7.5% due to china_plus_one upgrade"

**Next Morning: 08:00 - Social Job**

8. **ContentAgent** generates tweet:
   ```
   "Specialty chemicals seeing structural tailwind as China tariffs
   push global buyers to diversify. Indian companies with REACH
   compliance gaining share at higher margins. The moat deepens.
   When does 'China+1' become 'India First'? #SpecChem #India"
   ```

9. **Writes to Google Sheet** for PM approval

10. **PM Reviews** → Sets Approval="YES" → Posts to Twitter

---

## 🗂️ File Structure

```
valuation_system/
├── agents/
│   ├── orchestrator.py          # Main coordinator
│   ├── news_scanner.py          # News scraping + LLM classify
│   ├── sector_analyst.py        # Driver impact analysis
│   ├── valuator.py              # Valuation engine
│   └── content_agent.py         # Social media drafts
├── models/
│   ├── financial_processor.py   # TTM metrics, 3-tier fallback
│   ├── dcf_model.py             # DCF valuation
│   ├── relative_valuation.py   # Peer multiples
│   └── monte_carlo.py           # Scenario simulation
├── storage/
│   ├── mysql_client.py          # MySQL wrapper
│   ├── gsheet_client.py         # Google Sheets integration
│   └── schema.sql               # 14 table definitions
├── utils/
│   ├── llm_client.py            # LLM fallback chain
│   ├── resilience.py            # Graceful degradation
│   ├── config_loader.py         # YAML parsers
│   └── generate_excel.py        # Excel report generator
├── config/
│   ├── .env                     # Environment variables
│   ├── companies.yaml           # Watchlist + alpha drivers
│   └── sectors.yaml             # Sector drivers + terminal assumptions
├── scheduler/
│   ├── runner.py                # CLI entry points
│   └── *.plist                  # launchd job configs
├── docs/
│   ├── README.md                # This file
│   └── VALUATION_PROCESS.md     # Comprehensive documentation
├── logs/                        # Daily logs (auto-rotated)
├── reports/                     # Excel outputs
└── requirements.txt             # Python dependencies
```

---

## ⚙️ Configuration

### Environment Variables (.env)

**Required**:
```bash
# Database
MYSQL_DATABASE=rag
MYSQL_USER=root

# Data Paths
CORE_CSV_PATH=/Users/ram/code/investment_strategies/data/core-input/core-all-input-2026-01-21 11-02-latest-final.csv
PRICE_CSV_PATH=/Users/ram/code/investment_strategies/data/prices/combined_monthly_prices.csv

# Alert Email
ALERT_EMAIL=medury@gmail.com
```

**TODO** (for full automation):
```bash
GSHEET_DRIVERS_ID=  # Create spreadsheet with 8 sheets
SMTP_USER=          # For email alerts
SMTP_PASSWORD=      # For email alerts
GROK_API_KEY=       # For LLM
```

---

## 🔍 Troubleshooting

**Hourly job not running?**
```bash
launchctl list | grep valuation
tail -f logs/hourly.log
```

**MySQL connection error?**
```bash
# Operations will queue to JSON
# After MySQL is back:
python -m valuation_system.scheduler.runner catchup
```

**Driver values not updating?**
```bash
# Check Google Sheets sync:
# Manual edits in GSheet take priority over LLM updates
# Verify GSHEET_DRIVERS_ID is set in .env
```

**Valuation seems off?**
```bash
# Check data source quality:
python -m valuation_system.scheduler.runner valuation --symbol AETHER
# Review logs for [ACTUAL]/[DERIVED]/[DEFAULT] tags
# Compare TTM values vs screener.in
```

---

## 📧 Contact & Support

- **Email Alerts**: medury@gmail.com
- **Issue Tracker**: GitHub issues (when moved to repo)
- **Documentation Updates**: Edit this file or VALUATION_PROCESS.md

---

## 📖 Further Reading

1. **[VALUATION_PROCESS.md](./VALUATION_PROCESS.md)** - Complete technical documentation
2. Plan file: `/Users/ram/.claude/plans/parallel-squishing-sifakis.md` - Aether validation plan
3. Requirements: `/Users/ram/code/research/valuation_system_requirements.md` - Original specs

---

**Version**: 1.0 | **Last Updated**: 2026-02-06
