# Next Session - Quick Start Guide
**Updated**: 2026-02-09 (Session 5)
**Status**: 31 auto-computed COMPANY drivers (was 8), 3 price signals, headline intelligence in digest

---

## Current System State

### Complete Features
1. **4-Level Driver Hierarchy**: MACRO(23) → GROUP(96) → SUBGROUP(585) → COMPANY(96,391)
2. **Company Driver Templates**: 96,391 rows for 2,912 companies (31 auto + 2-4 qualitative each)
3. **Macro Cascade**: 26 links (all 6 GROUP + 20 SUBGROUP) auto-update from macro changes
4. **Valuation Integration**: Quick + Full both apply all 4 driver levels
5. **Excel Reports**: Real company drivers, 13-col peer table, 17×13 sensitivity, computation logs
6. **GSheet Tabs 1-9**: All synced (Macro, Group, Subgroup, Company, Activity, Companies, Discovered Drivers, News Events, Materiality Dashboard)

### News Intelligence (Tested 2026-02-09)
7. **News Scanner (5 sources)**: Moneycontrol, Economic Times, Business Standard, Google News, Teams Channel
8. **Price Trend Analyzer**: Percentile-based PE/PB/evebidta/PS anomaly detection (self + sector relative). MCap>500Cr filter, sane ratio caps. NEW: Valuation bands (5Y percentile), Equity Risk Premium, Price Momentum signals.
9. **Qualitative Driver Agent**: LLM auto-fills SEED drivers from vs_event_timeline + sector-level news. 84 drivers filled for 60 companies.
10. **Daily Digest Generator**: HTML email with 6 sections (critical alerts, **top headlines by driver impact**, news intelligence, pending discoveries, value buy opps, driver changes)
11. **PM Approval Workflow**: Tab 7 PENDING→APPROVED/REJECTED detection in hourly cycle
12. **Materiality Dashboard**: Tab 9 color-coded alerts synced daily

### Session 5 Additions (2026-02-09)
13. **31 Auto-Computed COMPANY Drivers** (was 8):
    - Market share (dual-level: subgroup + group) with capex context narrative
    - Growth vs GDP comparison
    - Exposed DCF metrics: capex/sales, NWC/sales, tax rate, cost of debt, promoter pledge
    - 5 high-value ratios: interest coverage, operating leverage, FCF margin, earnings quality, capex phase
    - 5 trend ratios: ROE 3Y, gross margin, cash conversion cycle, earnings volatility, asset turnover
    - 4 composite scores: operational excellence, financial health, growth efficiency, earnings sustainability
    - Employee productivity
14. **3 New Price Signals**: Valuation band (5Y PE/PB percentiles), Equity Risk Premium (1/PE - Rf), Price Momentum (6M/12M returns)
15. **Top 10 Headlines by Driver Impact**: New digest section showing headline → driver impact mapping
16. **Tab 8 Bug Fixes**: Semantic group null check, insert guard, backfill legacy rows

### Pipeline Flow (Automated)
```
HOURLY: News scan (5 sources) → Driver updates → PM edit detection → Critical alerts email
DAILY:  Macro sync → Cascade → Valuations → Price trends → Qualitative auto-fill
        → Trend detection → Tab 7/9 sync → Social drafts → Daily digest email
```

---

## Priority Tasks (Next Session)

### 1. Configure SMTP for Email Delivery
```bash
# Gmail App Password (for daily digest emails)
# Go to: https://myaccount.google.com/apppasswords
# Add to .env:
SMTP_USER=your@gmail.com
SMTP_PASSWORD=<app-password>
```

### 2. Twitter API (for social posting) - OPTIONAL
```bash
# Go to: https://developer.twitter.com/en/portal/dashboard
TWITTER_API_KEY=...
TWITTER_API_SECRET=...
```

### 3. Test Full Daily Cycle (includes valuations)
```bash
# WARNING: Runs valuations for 2,655 companies — takes hours
# Run in separate terminal:
python3 -c "
import sys; sys.path.insert(0, '.')
from valuation_system.agents.orchestrator import OrchestratorAgent
orch = OrchestratorAgent()
result = orch.run_daily_valuation()
import json; print(json.dumps(result, indent=2, default=str))
" > /tmp/daily_cycle.log 2>&1 &
tail -f /tmp/daily_cycle.log
```

### 4. Fine-tune Price Trend Thresholds
- Currently 3,951 alerts — might still be noisy for PM
- Consider raising MIN_MCAP_CR from 500 to 1000
- Consider adding a "top N" per sector to limit alerts

---

## Deferred Items (Future Sessions)

### Bucket 1: Data Already Available — Not Yet Surfaced
- **FII/DII holding trend**: Columns `fii_holding_pct`, `dii_holding_pct` expected in next CSV update. When available, add 2 COMPANY drivers.
- **Pledge percentage from quarterly data**: `promoter_{idx}_pledged` columns — verify availability in core CSV and add to quarterly extraction if present.

### Bucket 2: Requires External API Scraping
- **Dividend per share**: BSE corporate actions API (`https://api.bseindia.com/BseIndiaAPI/api/CorporateAction/...`). Scrape dividend history, compute yield, add `dividend_yield` COMPANY driver.
- **Buyback amount**: Same BSE API. Track buyback events, compute buyback yield, add `buyback_yield` driver.
- **Credit rating**: CRISIL/ICRA/CARE websites or BSE filings. Parse rating + outlook, add `credit_rating` GOVERNANCE driver. Direction: POSITIVE if A+/above, NEGATIVE if BBB/below.

### Bucket 3: Requires Annual Report / Filings Data
- **Export revenue %**: Available in segment reporting (annual reports). Requires PDF parsing or structured data source. Add `export_revenue_pct` COMPETITIVE driver.
- **R&D spend %**: Available in notes to accounts. Add `rd_to_sales` STRATEGIC driver for pharma/tech companies.
- **Patent count / IP portfolio**: Needs IPO India or Google Patents API. Mostly relevant for pharma/tech.

### Bucket 4: Employee Data
- **Employee count**: Not in core CSV. Could scrape from annual reports or EPFO data. Add `revenue_per_employee` COMPETITIVE driver (requires absolute headcount, not just empcost).

---

## Files Modified This Session (Session 5)

### Modified Files
- `agents/news_scanner.py` — Added null check in `_store_event()` after mysql.insert()
- `agents/daily_digest_generator.py` — New "Top Headlines by Driver Impact" section with `_get_top_headlines_by_driver_impact()`, `_format_drivers_for_display()`, `_render_section_headline_driver_impact()`
- `data/processors/company_driver_calculator.py` — Expanded from 8 to 31 drivers: market share (dual-level), growth vs GDP, exposed DCF metrics, 10 ratio drivers, 4 composite scores, employee productivity
- `data/processors/price_trend_analyzer.py` — Added valuation bands (5Y percentile), equity risk premium, price momentum signals
- `utils/populate_drivers.py` — Updated UNIVERSAL_COMPANY_DRIVERS from 8 to 31 entries
- `utils/sync_drivers_to_gsheet.py` — Tightened Tab 8 semantic filter, removed redundant import

### Database Changes
- Backfilled 36 legacy `vs_event_timeline` rows with `semantic_group_id = id`
- Populated 66,976 new COMPANY driver rows (23 new × 2,912 companies)
- Total COMPANY drivers now: 96,391

---

## Architecture Reference

### Orchestrator Daily Cycle (run_daily_valuation)
1. `_check_and_update_macro_data()` — Stale CSV check
2. `_sync_macro_from_csv()` — 23 MACRO drivers from 5 CSVs
3. `_cascade_macro_to_linked_drivers()` — SAME/INVERSE cascade
4. `_assess_materiality()` — 4 signal types
5. `_process_approved_discoveries()` — Promote APPROVED→vs_drivers
6. Valuation loop (2,655 active companies)
7. `_detect_price_trends()` — PE/PB/evebidta/PS percentile anomalies + valuation bands + ERP + momentum
8. `_populate_qualitative_drivers()` — LLM fills SEED drivers (60 companies/run)
9. `_run_trend_detection()` — 30/60/90-day persistent trends
10. `_sync_dashboard_tabs()` — Tab 7 + Tab 9
11. `_generate_social_content()` — Draft posts for PM
12. `_send_daily_digest()` — Email with 6 sections

### Orchestrator Hourly Cycle (run_hourly_cycle)
1. Replay queued ops
2. News scan (5 sources: MC, ET, BS, Google News, Teams)
3. Driver updates from news
4. Critical event → immediate valuation + email
5. PM edit detection (Tab 1-4 + Tab 7 discovered drivers)

---

## Known Limitations
1. **SMTP not configured**: Daily digest generates HTML but won't send until SMTP_USER/PASSWORD set in .env
2. **Twitter not configured**: ContentAgent generates drafts but won't post until Twitter API creds set
3. **ChromaDB not used for news**: QualitativeDriverAgent queries vs_event_timeline (MySQL), not ChromaDB
4. **LLM batch assessment**: Grok sometimes returns single-dict instead of array — per-driver fallback handles it
5. **3,951 price alerts**: Still somewhat noisy; digest caps at top 15/25 but underlying data could be further filtered
6. **vs_discovered_drivers empty**: No new drivers have been suggested yet (needs GROUP analyst to run)
7. **31 new drivers untested at scale**: Driver computation for all 2,912 companies not yet run — needs batch_valuation integration
