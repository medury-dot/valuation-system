# Driver System Enhancement: Macro, Group & Subgroup Drivers + Dynamic Intelligence

## Context

The valuation system collects rich macro data from MOSPI API (CPI, IIP, WPI, GDP/GVA, PLFS) and market indicators (bond yield, repo rate, PMI, FII/DII, forex). However, only a fraction reaches the GSheet dashboard:

- **Tab 1 "1. Macro Drivers"**: Only 5 rows (gdp_growth, usd_inr, interest_rate_10y, repo_rate, pmi_composite). MySQL has 9. We collect 10 market_indicators series, 27 IIP, 28 CPI, ~80 WPI, 15 GDP/GVA — most not visible.
- **Tab 2 "2. Valuation Group Drivers"**: 73 rows for 12 groups. Orchestrator auto-maps IIP→GROUP (10) + CPI→GROUP (6) to MySQL, but these 16 never appear in GSheet.
- **Tab 3 "3. Valuation Subgroup Drivers"**: 424 rows for 50 subgroups. **17 new subgroups have ZERO drivers** (575 companies uncovered).
- **Dynamic discovery**: `group_analyst.py` can UPDATE existing drivers — cannot CREATE new ones from news/agent analysis.
- **Pipeline management**: XYOps is available but not yet integrated for scheduling/monitoring driver pipelines.

**Goal**: (1) Full macro visibility in Tab 1, (2) data-driven group mapping from `macro_metadata.csv`, (3) populate 17 missing subgroups, (4) agent-driven driver discovery, (5) materiality detection for investment opportunities, (6) XYOps-managed pipeline scheduling.

---

## Phase 1: Populate 17 Missing Subgroup Drivers (unblocks 575 companies)

### File: `valuation_system/utils/populate_drivers.py`

Add entries to `SUBGROUP_DRIVERS` dict for these 17 subgroups (~155 new drivers total):

| Subgroup | Cos | Key Drivers |
|----------|-----|-------------|
| SERVICES_TRADING | 197 | commodity_volumes, working_capital, forex_volatility, import_duties |
| MATERIALS_PLASTICS | 87 | fmcg_packaging, polymer_prices, crude_oil_linkage, single_use_ban |
| INFRA_LOGISTICS | 65 | ecommerce_volumes, fuel_costs, gst_logistics, multimodal_policy |
| MATERIALS_PAPER | 40 | packaging_board, wood_pulp_prices, power_costs, import_duties |
| CONSUMER_JEWELLERY | 39 | gold_price_trend, organized_share, hallmarking, import_duty_gold |
| SERVICES_PROFESSIONAL | 29 | corporate_capex, staffing_demand, talent_costs |
| SERVICES_BPO | 25 | outsourcing_trend, employee_costs, data_privacy |
| CONSUMER_ALCOHOLIC_BEVERAGES | 17 | premiumization, volume_growth, state_excise_policy |
| MATERIALS_MINING | 11 | global_commodity_prices, steel_production, mining_leases |
| INFRA_SHIPPING | 11 | charter_rates, bunker_fuel_costs, imo_regulations |
| SERVICES_EDUCATION | 11 | enrollment_growth, edtech_adoption, nep_2020 |
| ENERGY_INDUSTRIAL_GAS | 11 | industrial_production, power_costs, safety_standards |
| CONSUMER_RETAIL_ONLINE | 10 | gmv_growth, cac, ecommerce_policy |
| INDUSTRIALS_ENVIRONMENTAL | 10 | municipal_waste, pollution_control, epr_compliance |
| SERVICES_AVIATION | 6 | passenger_traffic, atf_prices, airport_charges |
| FINANCIALS_EXCHANGES_DEPOSITORIES | 4 | trading_volumes, ipo_pipeline, sebi_regulations |
| FINANCIALS_RATINGS | 2 | debt_issuances, sebi_cra_norms |

Each subgroup gets 7-10 drivers (DEMAND/COST/REGULATORY), weights sum to ~0.80-1.00.

---

## Phase 2: Expand Tab 1 — Full Macro Dashboard (5 → 23 drivers)

### File: `valuation_system/agents/orchestrator.py` — expand `_sync_macro_from_csv()`

Add 14 new MACRO drivers from data already in local CSVs:

| # | Driver | Source | Category |
|---|--------|--------|----------|
| 10 | pmi_manufacturing | market_indicators.csv / "PMI Manufacturing" | Economy |
| 11 | pmi_services | market_indicators.csv / "PMI Services" | Economy |
| 12 | balance_of_trade | market_indicators.csv / "Balance of trade" | Trade |
| 13 | fii_flows | market_indicators.csv / "Total FII" | Flows |
| 14 | dii_flows | market_indicators.csv / "Total DII" | Flows |
| 15 | cpi_headline | cpi_monthly.csv / "General Index (All Groups)" YoY | Inflation |
| 16 | wpi_fuel_power | wpi_monthly.csv / "Fuel and Power" YoY | Inflation |
| 17 | wpi_manufactured | wpi_monthly.csv / "Manufactured Products" YoY | Inflation |
| 18 | wpi_primary_articles | wpi_monthly.csv / "Primary articles" YoY | Inflation |
| 19 | gva_manufacturing_real | gdp_quarterly.csv / GVA Manufacturing (Real) YoY | GDP |
| 20 | gva_construction_real | gdp_quarterly.csv / GVA Construction (Real) YoY | GDP |
| 21 | gva_financial_real | gdp_quarterly.csv / GVA Financial Services (Real) YoY | GDP |
| 22 | gva_agriculture_real | gdp_quarterly.csv / GVA Agriculture (Real) YoY | GDP |
| 23 | lfpr_total | plfs_quarterly.csv / LFPR total | Labour |

Changes to `_sync_macro_from_csv()`:
1. **Section 1**: Add 5 to `market_mapping` (PMI Mfg/Services, BoT, FII, DII)
2. **Section 3**: Add CPI headline inflation as MACRO driver
3. **Section 4**: Add 3 WPI MajorGroup YoY series as MACRO drivers
4. **New Section 6**: Read `gdp_quarterly.csv`, compute YoY for 4 GVA series
5. **Section 5**: Add LFPR total alongside unemployment_rate

### File: `valuation_system/utils/sync_drivers_to_gsheet.py` — add `sync_macro_drivers()`

Currently syncs Tabs 2, 3, 6 but NOT Tab 1.

New function:
- Query `vs_drivers WHERE driver_level='MACRO'`
- Define `MACRO_DRIVER_METADATA` dict with category labels, metric units, source, update frequency, impact descriptions, and bull/base/bear scenario values
- Write to Tab 1 matching existing 14-column headers
- Add to `main()` as first sync step

**Weight allocation** (sum to 1.0):
- Tier 1 (WACC inputs): interest_rate_10y=0.12, repo_rate=0.08
- Tier 2 (growth): gdp_growth=0.08, pmi_composite=0.06, pmi_mfg=0.05, pmi_services=0.05
- Tier 3 (inflation): cpi_headline=0.06, cpi_food=0.04, wpi_manufactured=0.04, wpi_fuel=0.04, wpi_primary=0.03
- Tier 4 (structural): usd_inr=0.06, balance_of_trade=0.04, fii_flows=0.04, dii_flows=0.03
- Tier 5 (GVA): gva_mfg=0.03, gva_construction=0.02, gva_financial=0.02, gva_agri=0.02
- Tier 6 (labour/industrial): unemployment_rate=0.03, lfpr_total=0.02, iip_manufacturing=0.04

---

## Phase 3: Data-Driven Group Mapping (replaces hardcoded IIP/CPI maps)

### File: `valuation_system/agents/orchestrator.py`

**Problem**: `iip_group_map` (10 entries) and `cpi_group_map` (6 entries) are hardcoded. But `macro_metadata.csv` already maps 117 rows to `valuation_group`.

**Solution**: New method `_load_macro_to_group_mapping()`:
1. Read `macro_metadata.csv` from `MACRO_DATA_PATH`
2. Deduplicate CPI (use Combined only), group by series_name
3. Return `{series_name: {'valuation_group': ..., 'valuation_subgroup': ..., 'source': 'IIP'|'CPI'|'WPI'|'GVA'}}`
4. Replace hardcoded `iip_group_map` / `cpi_group_map` with data-driven loop

**Result**: ~45 auto-mapped GROUP drivers (up from 16) covering all macro sources:
- IIP: ~22 series → 10 groups
- CPI Combined: ~13 series → 7 groups
- WPI: ~5 series → 5 groups (NEW)
- GVA: ~5 series → 5 groups (NEW)

These get `driver_category='MACRO_SIGNAL'` to distinguish from hand-curated DEMAND/COST/REGULATORY.

**Tab 2 total**: 73 existing + ~45 auto-mapped = ~118 GROUP drivers.

---

## Phase 4: Dynamic Driver Discovery (Agent Intelligence)

### 4A. New MySQL table: `vs_discovered_drivers`

```sql
CREATE TABLE IF NOT EXISTS vs_discovered_drivers (
    id INT AUTO_INCREMENT PRIMARY KEY,
    discovered_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    driver_level ENUM('MACRO','GROUP','SUBGROUP') NOT NULL,
    valuation_group VARCHAR(100),
    valuation_subgroup VARCHAR(100),
    driver_category VARCHAR(50),
    driver_name VARCHAR(100) NOT NULL,
    suggested_weight DECIMAL(5,4),
    reasoning TEXT,
    source_event_id INT,
    source_headline TEXT,
    confidence ENUM('HIGH','MEDIUM','LOW') DEFAULT 'MEDIUM',
    status ENUM('PENDING','APPROVED','REJECTED') DEFAULT 'PENDING',
    reviewed_by VARCHAR(50),
    reviewed_at TIMESTAMP NULL
);
```

### 4B. File: `valuation_system/agents/group_analyst.py`

Enhance `DRIVER_IMPACT_PROMPT` to ask:
```
If this event suggests we should track a NEW driver not in the current list:
"new_driver_suggestions": [{"name": "...", "category": "DEMAND|COST|REGULATORY",
 "level": "GROUP|SUBGROUP", "weight": 0.05, "reasoning": "..."}]
```

New method `_handle_new_driver_suggestions(suggestions, event)`:
- Dedup against existing `vs_drivers` and `vs_discovered_drivers`
- Insert with `status='PENDING'`

### 4C. File: `valuation_system/utils/sync_drivers_to_gsheet.py`

Add `sync_discovered_drivers()` for new GSheet tab "7. Discovered Drivers":
- Headers: ID, Level, Group, Subgroup, Category, Driver Name, Weight, Reasoning, Source, Confidence, Status, Discovered At
- PM reviews and sets Status to APPROVED/REJECTED in GSheet

### 4D. File: `valuation_system/agents/orchestrator.py`

In `run_daily_valuation()`, before valuations:
1. Query `vs_discovered_drivers WHERE status='APPROVED'`
2. Insert into `vs_drivers` with `updated_by='AGENT_DISCOVERY'`
3. Log to `vs_driver_changelog` with `triggered_by='AGENT_ANALYSIS'`

---

## Phase 5: Materiality Detection & Opportunity Sensing

### 5A. Materiality scoring in `orchestrator.py`

New method `_assess_materiality()` called after macro sync + news scan:

**Signal types for materiality**:
1. **Macro divergence**: When a macro driver deviates >2σ from 12-month mean (e.g., PMI drops below 50, FII outflows spike)
2. **Driver momentum**: When >3 drivers for a group/subgroup shift in same direction within 7 days
3. **Valuation gap widening**: When CMP moves >10% away from intrinsic value (from last valuation)
4. **Cross-signal**: Macro + sector driver alignment (e.g., repo rate cut + housing CPI down = bullish REAL_ESTATE_INFRA)

**Output**: `vs_materiality_alerts` table (new):
```sql
CREATE TABLE IF NOT EXISTS vs_materiality_alerts (
    id INT AUTO_INCREMENT PRIMARY KEY,
    alert_date DATE,
    alert_type ENUM('MACRO_DIVERGENCE','DRIVER_MOMENTUM','VALUATION_GAP','CROSS_SIGNAL'),
    valuation_group VARCHAR(100),
    valuation_subgroup VARCHAR(100),
    signal_description TEXT,
    affected_companies INT,
    suggested_action ENUM('REVALUE_NOW','WATCH','REDUCE_EXPOSURE'),
    severity ENUM('HIGH','MEDIUM','LOW'),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

### 5B. Trend detection in `group_analyst.py`

New method `_detect_trend_developments()`:
- Track rolling 30/60/90-day driver trajectories per group
- Flag when a trend persists >60 days (structural shift vs noise)
- Flag reversals (driver was UP for 60 days, now DOWN — possible inflection)
- Store in `vs_driver_changelog` with `triggered_by='TREND_DETECTION'`

### 5C. Opportunity scoring

After materiality assessment, rank groups/subgroups by opportunity:
```
opportunity_score = (valuation_upside × driver_momentum × macro_tailwind) / volatility
```
- Groups with top 5 opportunity scores get flagged for priority re-valuation
- ContentAgent generates insight posts for top movers

---

## Phase 6: XYOps Pipeline Integration

### Pipeline definitions (declarative YAML or Python):

**Pipeline 1: `macro_sync` (runs 06:00 IST daily)**
- Step 1: Run MOSPI scraper (`update_macro_data.py`)
- Step 2: Run orchestrator macro sync (`_sync_macro_from_csv()`)
- Step 3: Sync to GSheet Tab 1 (`sync_macro_drivers()`)
- Step 4: Assess materiality (`_assess_materiality()`)
- Alerts on failure; retries 3x with 5-min backoff

**Pipeline 2: `news_scan` (runs every 60 min, 08:00-22:00 IST)**
- Step 1: NewsScanner.scan_all_sources()
- Step 2: Classify & store events
- Step 3: GroupAnalyst.update_drivers_from_news() (parallel per group)
- Step 4: Detect new driver suggestions
- Step 5: Sync driver changes to GSheet Tabs 2, 3
- Monitored: event count, classification latency, driver changes/hour

**Pipeline 3: `daily_valuation` (runs 20:00 IST)**
- Step 1: Process approved driver discoveries
- Step 2: Run batch valuation (all active companies)
- Step 3: Sync results to GSheet Tab 5
- Step 4: Detect valuation gaps (>10% change → alert)
- Step 5: Generate email digest
- SLA: complete within 2 hours for 2,900 companies

**Pipeline 4: `weekly_review` (Sunday 10:00 IST)**
- Step 1: Full GSheet sync (all 7 tabs)
- Step 2: Trend detection (30/60/90-day analysis)
- Step 3: Opportunity scoring & ranking
- Step 4: Generate weekly summary report
- Step 5: ContentAgent creates week-ahead preview posts

**Pipeline 5: `social_posts` (runs 08:00 IST daily)**
- Step 1: Gather signals (events, driver changes, alerts)
- Step 2: ContentAgent generates 3 posts
- Step 3: Queue to GSheet for PM approval

### XYOps integration file: `valuation_system/pipelines/xyops_config.py`

- Register each pipeline with XYOps scheduler
- Define dependencies (macro_sync must complete before daily_valuation)
- Configure alerting (email on failure, Slack if available)
- Dashboard: pipeline run history, success rates, latencies

---

## Files to Modify (Summary)

| File | Phase | Changes |
|------|-------|---------|
| `utils/populate_drivers.py` | 1 | Add 17 subgroups (~155 drivers) to SUBGROUP_DRIVERS |
| `agents/orchestrator.py` | 2,3,5 | Add 14 MACRO drivers, `_load_macro_to_group_mapping()`, `_assess_materiality()`, process approved discoveries |
| `utils/sync_drivers_to_gsheet.py` | 2,4 | Add `sync_macro_drivers()` for Tab 1, `sync_discovered_drivers()` for Tab 7 |
| `agents/group_analyst.py` | 4,5 | Enhance DRIVER_IMPACT_PROMPT, `_handle_new_driver_suggestions()`, `_detect_trend_developments()` |
| `storage/schema.sql` | 4,5 | Add `vs_discovered_drivers`, `vs_materiality_alerts` tables |
| `pipelines/xyops_config.py` | 6 | NEW: XYOps pipeline definitions |

---

## Verification

```bash
# Phase 1: Populate missing subgroup drivers
python3 valuation_system/utils/populate_drivers.py > /tmp/populate_drivers.log 2>&1
# Verify: ~155 new SUBGROUP drivers, Tab 3 → ~579 rows

# Phase 2: Expanded macro sync
python3 -c "
from valuation_system.agents.orchestrator import OrchestratorAgent
o = OrchestratorAgent()
r = o._sync_macro_from_csv()
print(r)
" > /tmp/macro_sync.log 2>&1
# Verify: 23 MACRO + ~45 GROUP drivers

# Phase 3: Full GSheet sync
python3 valuation_system/utils/sync_drivers_to_gsheet.py > /tmp/gsheet_sync.log 2>&1
# Verify: Tab1=23 rows, Tab2=~118 rows, Tab3=~579 rows

# Phase 4: Test discovery with news event
# (manual after implementation)

# MySQL check
mysql -u root rag -e "
  SELECT driver_level, COUNT(*) FROM vs_drivers GROUP BY driver_level;
"
# Expected: MACRO=23, GROUP=~130, SUBGROUP=~579
```

## Implementation Order

1. **Phase 1** — quickest win, unblocks 575 companies
2. **Phase 2** — Tab 1 visibility (all macros shown)
3. **Phase 3** — data-driven group mapping (replaces hardcoded maps)
4. **Phase 4** — dynamic discovery infrastructure
5. **Phase 5** — materiality/opportunity sensing
6. **Phase 6** — XYOps pipeline management
