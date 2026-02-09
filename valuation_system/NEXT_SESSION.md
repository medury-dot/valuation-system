# Next Session - Quick Start Guide
**Updated**: 2026-02-09 (Session 4)
**Status**: Full E2E pipeline tested for Eicher Motors (all 6 steps pass)

---

## Current System State

### Complete Features
1. **4-Level Driver Hierarchy**: MACRO(23) → GROUP(96) → SUBGROUP(585) → COMPANY(29,415)
2. **Company Driver Templates**: 29,415 rows for 2,912 companies (8 auto + 3 qualitative each)
3. **Macro Cascade**: 26 links (all 6 GROUP + 20 SUBGROUP) auto-update from macro changes
4. **Valuation Integration**: Quick + Full both apply all 4 driver levels
5. **Excel Reports**: Real company drivers, 13-col peer table, 17×13 sensitivity, computation logs
6. **GSheet Tabs 1-9**: All synced (Macro, Group, Subgroup, Company, Activity, Companies, Discovered Drivers, News Events, Materiality Dashboard)

### News Intelligence (Tested 2026-02-09)
7. **News Scanner (5 sources)**: Moneycontrol, Economic Times, Business Standard, Google News, Teams Channel
8. **Price Trend Analyzer**: Percentile-based PE/PB/evebidta/PS anomaly detection (self + sector relative). MCap>500Cr filter, sane ratio caps.
9. **Qualitative Driver Agent**: LLM auto-fills SEED drivers from vs_event_timeline + sector-level news. 84 drivers filled for 60 companies.
10. **Daily Digest Generator**: HTML email (76KB) with 5 sections (critical alerts, news intelligence, pending discoveries, value buy opps, driver changes)
11. **PM Approval Workflow**: Tab 7 PENDING→APPROVED/REJECTED detection in hourly cycle
12. **Materiality Dashboard**: Tab 9 color-coded alerts synced daily

### Eicher Motors E2E Test (2026-02-09, Session 4)
| Step | Component | Result |
|------|-----------|--------|
| 1 | News Events | 1 event (India-US trade deal / Harley vs Enfield) |
| 2 | Price Trends | 3 alerts (PB=8.9 HIGH, PS=9.4 HIGH, EV/EBITDA MEDIUM) |
| 3 | Qualitative Drivers | 11/11 filled (market_share=NEGATIVE from trade deal) |
| 4 | Valuation | DCF ₹2,156, Blended ₹3,268, CMP ₹7,178, -54.5% |
| 5 | GSheet Sync | Tab 7:0, Tab 8:36 news, Tab 9:3,951 alerts |
| 6 | Daily Digest | 76KB HTML, 5 sections, no overlap |

### Batch Test Results (Session 3)
- **News Scan**: 110 raw → 83 unique articles, 36 events in vs_event_timeline (9 CRITICAL, 10 HIGH, 17 MEDIUM)
- **Price Trends**: 1,691 companies analyzed, 3,951 alerts (MCap>500Cr, PE<200 filters)
- **Qualitative Drivers**: 60 companies, 84 drivers filled, 25 skipped (no news), 0 errors, 67 LLM calls
- **Daily Digest**: 76KB HTML, 5 sections populated, digest data correctly separated (no overlap between sections)
- **Driver Changelog**: 102 entries with AGENT_ANALYSIS source

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

## Files Created/Modified This Session (Session 3)

### Modified Files
- `agents/qualitative_driver_agent.py` — Fixed GROUP BY SQL, added sector-level news matching (`_VALUATION_GROUP_TO_SECTOR_KEYWORDS`), per-driver fallback for partial batch results
- `agents/daily_digest_generator.py` — Added news events section, LIMIT caps (15/25/30), excluded VALUATION_GAP from critical alerts (dedup), value opps shows "top 25 of N total"
- `agents/news_scanner.py` — Teams as 5th source, persistent DB-backed dedup, O(1) relevance checking, ENUM normalization (`_normalize_scope`, `_normalize_severity`)
- `data/processors/price_trend_analyzer.py` — MCap>500Cr filter, tighter percentile thresholds, MAX_SANE_RATIOS filter
- `config/.env` — LLM_MODEL=grok-3-mini-fast, Azure AD credentials, Teams channel config
- `utils/llm_client.py` — Ollama fallback changed to `os.getenv('OLLAMA_MODEL', 'mistral:7b')`

### New Files
- `integrations/__init__.py` — Package init for Teams integration

### Key Config Changes
- LLM_MODEL: `grok-2-1212` → `grok-3-mini-fast` (old model deprecated)
- Azure AD: AZURE_TENANT_ID, AZURE_CLIENT_ID, AZURE_CLIENT_SECRET configured
- Teams: TEAMS_TEAM_ID, TEAMS_CHANNEL_ID configured

---

## Architecture Reference

### Orchestrator Daily Cycle (run_daily_valuation)
1. `_check_and_update_macro_data()` — Stale CSV check
2. `_sync_macro_from_csv()` — 23 MACRO drivers from 5 CSVs
3. `_cascade_macro_to_linked_drivers()` — SAME/INVERSE cascade
4. `_assess_materiality()` — 4 signal types
5. `_process_approved_discoveries()` — Promote APPROVED→vs_drivers
6. Valuation loop (2,655 active companies)
7. `_detect_price_trends()` — PE/PB/evebidta/PS percentile anomalies
8. `_populate_qualitative_drivers()` — LLM fills SEED drivers (60 companies/run)
9. `_run_trend_detection()` — 30/60/90-day persistent trends
10. `_sync_dashboard_tabs()` — Tab 7 + Tab 9
11. `_generate_social_content()` — Draft posts for PM
12. `_send_daily_digest()` — Email with 5 sections

### Orchestrator Hourly Cycle (run_hourly_cycle)
1. Replay queued ops
2. News scan (5 sources: MC, ET, BS, Google News, Teams)
3. Driver updates from news
4. Critical event → immediate valuation + email
5. PM edit detection (Tab 1-4 + Tab 7 discovered drivers)

---

## Known Limitations
1. **SMTP not configured**: Daily digest generates HTML (76KB) but won't send until SMTP_USER/PASSWORD set in .env
2. **Twitter not configured**: ContentAgent generates drafts but won't post until Twitter API creds set
3. **ChromaDB not used for news**: QualitativeDriverAgent queries vs_event_timeline (MySQL), not ChromaDB
4. **LLM batch assessment**: Grok sometimes returns single-dict instead of array — per-driver fallback handles it
5. **3,951 price alerts**: Still somewhat noisy; digest caps at top 15/25 but underlying data could be further filtered
6. **vs_discovered_drivers empty**: No new drivers have been suggested yet (needs GROUP analyst to run)
