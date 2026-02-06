# Valuation Process & Driver Management

**Agentic Valuation System - Technical Documentation**
*Version 1.0 | Last Updated: 2026-02-06*

---

## Table of Contents

1. [System Overview](#system-overview)
2. [Driver Storage Architecture](#driver-storage-architecture)
3. [Complete Valuation Workflow](#complete-valuation-workflow)
4. [Agent Ecosystem](#agent-ecosystem)
5. [Scheduled Jobs](#scheduled-jobs)
6. [Driver Hierarchy](#driver-hierarchy)
7. [LLM Integration Points](#llm-integration-points)
8. [Data Flow Diagrams](#data-flow-diagrams)
9. [Configuration & Setup](#configuration--setup)
10. [Operational Procedures](#operational-procedures)

---

## System Overview

The Agentic Valuation System is an autonomous equity research platform that:

- **Continuously monitors** news and market events (hourly)
- **Analyzes impact** on valuation drivers using LLM intelligence
- **Updates valuations** daily with driver-based DCF/Relative/Monte Carlo blend
- **Generates alerts** when intrinsic value changes >5%
- **Creates content** for social media to share insights
- **Maintains full audit trail** of all decisions and assumptions

**Key Principle**: *"Macro sets the ceiling and floor. Sectors determine the value. Companies decide who wins."*

**Design Philosophy**:
- ✅ **Agent Autonomy**: Each agent operates independently
- ✅ **Graceful Degradation**: System functions even when services are down
- ✅ **PM-in-the-Loop**: Human oversight via Google Sheets and approval workflows
- ✅ **LLM as Co-pilot**: AI analyzes and synthesizes, but doesn't make final decisions
- ✅ **Full Traceability**: Every calculation logged with source tags

---

## Driver Storage Architecture

### Two-Tier Storage System

#### **Tier 1: Google Sheets** (Source of Truth for PM Editing)

**Location**: Spreadsheet ID stored in `.env` → `GSHEET_DRIVERS_ID`
**Auth**: Service account JSON → `GSHEET_AUTH_PATH`
**Purpose**: Collaborative interface for PM to view and override driver values

**8 Sheets Structure**:

| Sheet # | Name | Purpose | Columns |
|---------|------|---------|---------|
| 1 | **Macro Drivers** | Global economic factors (20% weight) | Category, Driver, Current Value, Bull, Base, Bear, Source, Update Freq, Last Updated, Trend, Weight, Valuation Impact |
| 2 | **Sector - Specialty Chemicals** | Chemicals industry drivers (55% weight) | Category, Driver, Metric, Current, Bull, Base, Bear, Weight, Impact, Trend, Last Updated, Source |
| 3 | **Sector - Automobiles** | Auto industry drivers (55% weight) | Category, Driver, Metric, Current, Bull, Base, Bear, Weight, Impact, Trend, Last Updated, Source |
| 4 | **Company - Aether Industries** | Aether-specific alpha (25% weight) | Category, Driver, Metric, Current, vs Peers, Weight, Alpha Impact, Last Updated, Source |
| 5 | **Company - Eicher Motors** | Eicher-specific alpha (25% weight) | Category, Driver, Metric, Current, vs Peers, Weight, Alpha Impact, Last Updated, Source |
| 6 | **Valuation History** | Timeline of intrinsic value changes | Date, Company, Intrinsic Value, CMP, Upside%, Key Change, Driver Impact, Event Ref, Doc Link, Synopsis |
| 7 | **Driver History** | Audit trail of driver updates | Date, Level, Driver, Old Value, New Value, Reason, Impact, Source Doc |
| 8 | **Event Log** | News events driving changes | Event ID, Date, Type, Company, Severity, Headline, Synopsis, Source URL, ChromaDB ID |

**Example Row (Sector - Specialty Chemicals)**:
```
Category: Cost Drivers
Driver: crude_oil
Metric: $/bbl
Current: 85
Bull: 70
Base: 80
Bear: 95
Weight: 0.10
Impact: rm_cost
Trend: UP
Last Updated: 2026-02-05 10:15
Source: EIA Weekly Report
```

#### **Tier 2: MySQL Database** (Fast Query Cache)

**Database**: `rag` (shared with RAGApp)
**Table**: `vs_drivers`
**Purpose**: Fast querying during valuation runs, audit trail
**Sync**: Google Sheets → MySQL (after each driver update)

**Schema**:
```sql
CREATE TABLE vs_drivers (
    id INT AUTO_INCREMENT PRIMARY KEY,
    driver_level ENUM('MACRO', 'SECTOR', 'COMPANY') NOT NULL,
    sector VARCHAR(100),
    company_id INT,
    driver_category VARCHAR(100),
    driver_name VARCHAR(100) NOT NULL,
    current_value VARCHAR(255),
    bull_value VARCHAR(255),
    base_value VARCHAR(255),
    bear_value VARCHAR(255),
    weight DECIMAL(5,4),
    impact_direction ENUM('UP', 'DOWN', 'NEUTRAL', 'STABLE') DEFAULT 'NEUTRAL',
    trend ENUM('UP', 'DOWN', 'STABLE') DEFAULT 'STABLE',
    last_updated DATETIME DEFAULT CURRENT_TIMESTAMP,
    source TEXT,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_level_sector (driver_level, sector),
    INDEX idx_company (company_id)
);
```

---

## Complete Valuation Workflow

### Phase 1: Hourly News Monitoring (Every 60 minutes)

```
┌─────────────────────────────────────────────────────────────────┐
│  HOURLY JOB (Triggered by launchd every 60 minutes)            │
└─────────────────────────────────────────────────────────────────┘
                            ▼
                    ┌───────────────┐
                    │ ORCHESTRATOR  │
                    └───────┬───────┘
                            ▼
    ┌───────────────────────────────────────────────────┐
    │  STEP 1: News Scanning (NewsScannerAgent)        │
    └───────────────────────────────────────────────────┘

    Sources:
    • Economic Times (RSS + web scraping)
    • MoneyControl (sector news)
    • BSE/NSE filings (announcements, results)
    • Twitter feeds (breaking news)

    Output: List of articles/events

                            ▼
    ┌───────────────────────────────────────────────────┐
    │  STEP 2: LLM Classification                       │
    └───────────────────────────────────────────────────┘

    LLM Prompt:
    "Analyze this news article:
     Headline: 'China imposes 10% export tariff on specialty chemicals'
     Summary: 'New tariff effective March 1, affects 200+ molecules...'

     Classify:
     1. Category: REGULATORY / MACRO / SECTOR / COMPANY / PRODUCT / MA / GOVERNANCE / EARNINGS / POLICY
     2. Scope: MACRO (all) / SECTOR (which?) / COMPANY (which?)
     3. Severity: CRITICAL / HIGH / MODERATE / LOW
     4. Affected entities
     5. Synopsis (1-2 sentences)"

    LLM Output:
    {
      "category": "REGULATORY",
      "scope": "SECTOR",
      "affected_sector": "specialty_chemicals",
      "severity": "HIGH",
      "synopsis": "China tariff barriers increase, accelerating China+1 shift for Indian specialty chemical companies",
      "confidence": "HIGH"
    }

    Storage:
    • MySQL: vs_news table (metadata)
    • ChromaDB: Full article text (for RAG later)

                            ▼
    ┌───────────────────────────────────────────────────┐
    │  STEP 3: Driver Impact Analysis (SectorAnalyst)   │
    └───────────────────────────────────────────────────┘

    For each SECTOR/COMPANY event:

    LLM Prompt:
    "Given news event: 'China tariff on chemicals'

     Current sector drivers (Specialty Chemicals):
     • volume_growth: 8% (weight 0.12)
     • china_plus_one: MODERATE (weight 0.10)
     • crude_oil: $85 (weight 0.10, sensitivity -0.5)
     • pharma_api_demand: 12% (weight 0.08)
     ...

     Determine:
     1. Which drivers are affected?
     2. Direction: UP / DOWN / STABLE
     3. Magnitude: MINOR / MODERATE / SIGNIFICANT
     4. New suggested value
     5. Impact on revenue growth %, margins %, terminal value
     6. Confidence: HIGH / MEDIUM / LOW
     7. Time horizon: IMMEDIATE / SHORT_TERM_3M / MEDIUM_TERM_1Y / STRUCTURAL"

    LLM Output:
    {
      "affected_drivers": [
        {
          "driver_name": "china_plus_one",
          "old_state": "MODERATE",
          "new_state": "HIGH",
          "direction": "UP",
          "magnitude": "SIGNIFICANT",
          "confidence": "HIGH",
          "time_horizon": "STRUCTURAL",
          "reasoning": "Tariff barriers make India more attractive for global buyers seeking to diversify from China"
        }
      ],
      "revenue_growth_impact_pct": 2.5,
      "margin_impact_pct": 0.5,
      "terminal_value_impact": "MODERATE",
      "overall_assessment": "Positive structural shift for Indian specialty chemicals"
    }

                            ▼
    ┌───────────────────────────────────────────────────┐
    │  STEP 4: Update Google Sheets                     │
    └───────────────────────────────────────────────────┘

    GSheetClient writes:
    • Sheet: "Sector - Specialty Chemicals"
    • Row: Find driver_name = "china_plus_one"
    • Update Column "Current": MODERATE → HIGH
    • Update Column "Trend": STABLE → UP
    • Update Column "Last Updated": 2026-02-05 10:15
    • Update Column "Source": "China tariff news (ET 2026-02-05)"

                            ▼
    ┌───────────────────────────────────────────────────┐
    │  STEP 5: Sync to MySQL                            │
    └───────────────────────────────────────────────────┘

    UPDATE vs_drivers
    SET current_value = 'HIGH',
        trend = 'UP',
        last_updated = '2026-02-05 10:15:00',
        source = 'China tariff news (ET 2026-02-05)'
    WHERE driver_level = 'SECTOR'
      AND sector = 'Specialty Chemicals'
      AND driver_name = 'china_plus_one';

                            ▼
    ┌───────────────────────────────────────────────────┐
    │  STEP 6: Check for CRITICAL Events                │
    └───────────────────────────────────────────────────┘

    IF severity = 'CRITICAL':
      • Trigger immediate on-demand valuation
      • Send email alert to PM
      • Log to vs_alerts table
    ELSE:
      • Changes will be picked up in tonight's daily valuation
```

**Hourly Cycle Duration**: ~2-5 minutes
**Log File**: `/Users/ram/code/research/valuation_system/logs/hourly.log`

---

### Phase 2: Daily Valuation Refresh (20:00 IST)

```
┌─────────────────────────────────────────────────────────────────┐
│  DAILY JOB (Triggered by launchd at 20:00 IST)                 │
└─────────────────────────────────────────────────────────────────┘
                            ▼
                    ┌───────────────┐
                    │ ORCHESTRATOR  │
                    └───────┬───────┘
                            ▼
    ┌───────────────────────────────────────────────────┐
    │  STEP 1: Run Hourly Cycle First (Catchup)        │
    └───────────────────────────────────────────────────┘

    • Ensures latest news/drivers are loaded
    • Fills any gaps if hourly job failed

                            ▼
    ┌───────────────────────────────────────────────────┐
    │  STEP 2: Reload Data Files                        │
    └───────────────────────────────────────────────────┘

    CoreDataLoader:
    • Core CSV: /Users/ram/code/investment_strategies/data/core-input/core-all-input-2026-01-21 11-02-latest-final.csv
      (Financial statements, ~9000 companies, ~4000 columns)

    PriceLoader:
    • Monthly Prices: /Users/ram/code/investment_strategies/data/prices/combined_monthly_prices.csv
      (CMP, PE, PB, PS, EV/EBITDA, MCap - updated daily)

    DamodaranLoader:
    • Risk-free rate, ERP, sector betas
    • From: /Users/ram/code/investment_strategies/data/macro/

                            ▼
    ┌───────────────────────────────────────────────────┐
    │  FOR EACH ACTIVE COMPANY (Aether, Eicher):       │
    └───────────────────────────────────────────────────┘

        ┌─────────────────────────────────────────────┐
        │  Step 2a: Load Drivers from MySQL           │
        └─────────────────────────────────────────────┘

        ValuatorAgent queries:
        • MACRO drivers (20% weight)
        • SECTOR drivers for company's sector (55% weight)
        • COMPANY drivers for specific company (25% weight)

        Example for Aether Industries:

        MACRO (20%):
        • gdp_growth: 7.2% (weight 0.05)
        • inflation: 5.5% (weight 0.04)
        • usd_inr: 83.5 (weight 0.03)
        • crude_oil: $85 (weight 0.05)
        • interest_rate_10y: 7.1% (weight 0.03)

        SECTOR - Specialty Chemicals (55%):
        DEMAND:
        • volume_growth: 8% (weight 0.12)
        • china_plus_one: HIGH (weight 0.10) ← Just updated!
        • pharma_api_demand: 12% (weight 0.08)
        • agrochem_demand: MODERATE (weight 0.06)
        • pricing_power: MODERATE (weight 0.08)

        COST:
        • crude_oil: $85 (weight 0.10, sensitivity -0.5)
        • natural_gas: $3.5/mmbtu (weight 0.06)
        • operating_leverage: 0.6 (weight 0.04)
        • fx_on_costs: NEUTRAL (weight 0.05)

        REGULATORY:
        • pli_scheme: ACTIVE (weight 0.04)
        • eu_reach_compliance: HIGH (weight 0.03)
        • fda_approval_rate: 85% (weight 0.03)
        • environmental_capex: MODERATE (weight 0.03)

        COMPANY - Aether (25%):
        • r_and_d_intensity: 4.5% (weight 0.05)
        • crams_pct: 35% (weight 0.05)
        • export_pct: 60% (weight 0.04)
        • promoter_holding: 72% (weight 0.03)
        • net_debt_ebitda: 0.8x (weight 0.04)
        ...

                ▼
        ┌─────────────────────────────────────────────┐
        │  Step 2b: Financial Data Processing         │
        └─────────────────────────────────────────────┘

        FinancialProcessor (models/financial_processor.py):

        Input: Core CSV row for "Aether Industries Ltd."

        3-Tier Fallback Logic:

        1. TTM Revenue:
           [ATTEMPT 1] Half-yearly: h2_2025 + h1_2026
           → Found: 850 + 920 = Rs 1,770 Cr [ACTUAL]

        2. TTM PAT:
           [ATTEMPT 1] Half-yearly: h2_2025_pat + h1_2026_pat
           → Not available
           [ATTEMPT 2] Quarterly: sum(sales_148, 149, 150, 151) × net_margin
           → Found: Rs 245 Cr [DERIVED]

        3. TTM EBITDA:
           [ATTEMPT 1] Quarterly: sum(pbidt_148, 149, 150, 151)
           → Found: Rs 385 Cr [ACTUAL]

        4. NWC (as % of sales):
           Current Assets = inventories + sundrydebtors + cash_and_bank
           Current Liabilities = trade_payables + ST_borrow
           → Found: 18.5% [ACTUAL]

           IMPORTANT: Use exact column names from CSV!
           • Half-yearly: Trade_payables (Title case), acc_depr (with 'r')
           • Yearly: trade_payables (lowercase), acc_dep (no 'r')

        5. Capex:
           [ATTEMPT 1] Yearly: pur_of_fixed_assets - sale_of_fixed_assets
           → Found: Rs 180 Cr [ACTUAL]

        6. D&A:
           [ATTEMPT 1] Half-yearly: h2_2025_acc_depr + h1_2026_acc_depr
           → Found: Rs 65 Cr [ACTUAL]

        7. Interest:
           [ATTEMPT 1] Yearly: interest
           → Found: Rs 15 Cr [ACTUAL]

        Output: FinancialMetrics object with source tags
        {
          "ttm_revenue": 1770.0,  # [ACTUAL]
          "ttm_pat": 245.0,       # [DERIVED]
          "ttm_ebitda": 385.0,    # [ACTUAL]
          "ebitda_margin": 0.2175,
          "nwc_pct": 0.185,
          "capex_ttm": 180.0,
          "d_and_a": 65.0,
          "revenue_cagr_3y": 0.22,
          "revenue_cagr_5y": 0.19,
          ...
        }

        All logged to: valuation_system/logs/YYYY-MM-DD.log

                ▼
        ┌─────────────────────────────────────────────┐
        │  Step 2c: LLM Driver Synthesis               │
        └─────────────────────────────────────────────┘

        LLM Prompt:
        "You are a valuation analyst for Aether Industries (Specialty Chemicals).

        Given current driver states:
        • china_plus_one: HIGH (up from MODERATE, weight 0.10)
        • crude_oil: $85 (up from $75, weight 0.10, sensitivity -0.5)
        • volume_growth: 8% (stable, weight 0.12)
        • pharma_api_demand: 12% (stable, weight 0.08)
        • r_and_d_intensity: 4.5% (vs peer avg 3.2%, weight 0.05)
        • crams_pct: 35% (high-margin business, weight 0.05)

        Calculate valuation adjustments:
        1. Revenue growth adjustment (vs base case)
        2. EBITDA margin adjustment
        3. Terminal margin override (if needed)
        4. Confidence adjustment
        5. Reasoning"

        LLM Output:
        {
          "revenue_growth_adjustment_pct": 1.5,
          "reasoning_revenue": "china_plus_one upgrade adds 2.5% to growth, offset by crude headwind of -1.0%",

          "margin_adjustment_pct": -0.3,
          "reasoning_margin": "Crude oil pass-through lag creates 30bps margin pressure in near term, but pricing power (MODERATE) allows partial recovery",

          "terminal_margin_override": null,
          "reasoning_terminal": "No override needed. 18-25% range appropriate given R&D intensity and CRAMS mix support long-term pricing power",

          "confidence_adjustment": 0.05,
          "reasoning_confidence": "china_plus_one is structural (HIGH confidence), but crude volatility adds near-term uncertainty",

          "overall_assessment": "Net positive. Structural China+1 tailwind (2-3 year impact) outweighs cyclical crude headwind (6-12 month impact). Maintain positive outlook."
        }

                ▼
        ┌─────────────────────────────────────────────┐
        │  Step 2d: DCF Valuation (3 Scenarios)       │
        └─────────────────────────────────────────────┘

        DCFModel (models/dcf_model.py):

        BASE CASE:
        • Revenue growth: 15% (historical 3Y CAGR) + 1.5% (driver adj) = 16.5%
        • Growth decay: Linear to terminal over 10 years
        • EBITDA margin: 21.75% (TTM) - 0.3% (driver adj) = 21.45%
        • Margin convergence: To 21% (midpoint of 18-25% range)
        • Capex: 10% of revenue (historical avg)
        • NWC: 18.5% of revenue
        • D&A: 3.7% of revenue
        • Tax rate: 25%
        • Terminal growth: Reinvestment Rate × ROCE
          → Reinvest Rate: 35% (from sectors.yaml)
          → Terminal ROCE: 20% (from sectors.yaml)
          → Terminal g: 0.35 × 0.20 = 7.0%
          → Capped at 5.0% (safety)

        WACC Calculation:
        • Risk-free rate: 7.1% (10Y G-Sec)
        • Beta: 1.05 (specialty chemicals sector from Damodaran)
        • Market risk premium: 8.5%
        • Cost of Equity: 7.1% + 1.05 × 8.5% = 16.0%
        • Cost of Debt: 9.0% (current borrowing rate)
        • Tax rate: 25%
        • D/E ratio: 0.15 (target)
        • WACC: 16.0% × (1/1.15) + 9.0% × (1-0.25) × (0.15/1.15) = 14.8%

        CHECK: WACC (14.8%) > Terminal g (5.0%) ✓

        10-Year Projections:
        Year 1: Revenue = 1770 × 1.165 = 2,062 Cr
        Year 2: Revenue = 2,062 × 1.150 = 2,371 Cr
        ...
        Year 10: Revenue = 6,850 Cr (growth decayed to 5.5%)

        Terminal Value:
        FCF_11 = EBITDA_11 × (1 - Tax) - Capex + D&A - Δ NWC
        FCF_11 = 1,439 × 0.75 - 685 + 253 - 85 = 562 Cr
        Terminal Value = 562 / (0.148 - 0.05) = Rs 5,735 Cr

        Enterprise Value = PV(FCF 1-10) + PV(TV)
                        = 1,820 + 1,485 = Rs 3,305 Cr

        Less: Net Debt = Rs 120 Cr
        Equity Value = Rs 3,185 Cr

        Shares Outstanding = MCap / CMP = 2,200 / 1,100 = 2.0 Cr
        Intrinsic Value per Share = 3,185 / 2.0 = Rs 1,593

        BULL CASE: +20% revenue growth, +200bps margin
        → Intrinsic = Rs 1,950

        BEAR CASE: +10% revenue growth, -100bps margin
        → Intrinsic = Rs 1,280

                ▼
        ┌─────────────────────────────────────────────┐
        │  Step 2e: Relative Valuation                │
        └─────────────────────────────────────────────┘

        RelativeValuation (models/relative_valuation.py):

        Peer Selection (2-tier):
        • TIGHT: Same industry (from vs_peer_groups, cached 30 days)
          → Aarti Industries, SRF, PI Industries
          → Weight: 2x

        • BROAD: Same sector (specialty chemicals)
          → Add: Navin Fluorine, Fine Organics, etc.
          → Weight: 1x

        Peer Multiples (from monthly prices CSV):
        Aarti: PE 28x, EV/EBITDA 16x
        SRF: PE 32x, EV/EBITDA 18x
        PI: PE 35x, EV/EBITDA 20x
        Navin: PE 30x, EV/EBITDA 17x

        Weighted Median:
        • PE: 30x (tight peers weighted 2x)
        • EV/EBITDA: 17.5x

        Sector Adjustment (from driver analysis):
        • Sector outlook score: 0.65 (on 0-1 scale)
        • Adjustment factor: (0.65 - 0.50) / 0.50 = +30%

        Adjusted Multiples:
        • PE: 30 × 1.30 = 39x
        • EV/EBITDA: 17.5 × 1.30 = 22.8x

        Valuation:
        • By PE: 39 × (245 / 2.0) = Rs 4,778 per share
        • By EV/EBITDA: (22.8 × 385 - 120) / 2.0 = Rs 4,329 per share

        Relative Intrinsic = Average = Rs 4,554 per share

        NOTE: Relative often higher than DCF for growth companies!

                ▼
        ┌─────────────────────────────────────────────┐
        │  Step 2f: Monte Carlo Simulation            │
        └─────────────────────────────────────────────┘

        MonteCarloModel (models/monte_carlo.py):

        Simulate 1,000 scenarios with randomized inputs:
        • Revenue growth: Normal(16.5%, σ=5%)
        • EBITDA margin: Normal(21.45%, σ=2%)
        • Terminal g: Uniform(3%, 5%)
        • WACC: Normal(14.8%, σ=1.5%)

        Results:
        • P10: Rs 1,150
        • P25: Rs 1,380
        • Median: Rs 1,610
        • P75: Rs 1,890
        • P90: Rs 2,150

                ▼
        ┌─────────────────────────────────────────────┐
        │  Step 2g: Blended Valuation                 │
        └─────────────────────────────────────────────┘

        Blend Weights (from sectors.yaml):
        • DCF: 60%
        • Relative: 30%
        • Monte Carlo: 10%

        Intrinsic Value = (1,593 × 0.60) + (4,554 × 0.30) + (1,610 × 0.10)
                        = 956 + 1,366 + 161
                        = Rs 2,483 per share

        Current Market Price (CMP): Rs 1,100
        Upside: (2,483 - 1,100) / 1,100 = +125.7%

        Confidence Score: 0.72 (based on data quality + driver confidence)

                ▼
        ┌─────────────────────────────────────────────┐
        │  Step 2h: Write to Database                 │
        └─────────────────────────────────────────────┘

        INSERT INTO vs_valuations (
            company_id, valuation_date, cmp,
            intrinsic_value_blended, upside_pct,
            dcf_base, dcf_bull, dcf_bear,
            relative_value, monte_carlo_median,
            confidence_score, alert_triggered
        ) VALUES (
            47582, '2026-02-05', 1100.00,
            2483.00, 125.7,
            1593.00, 1950.00, 1280.00,
            4554.00, 1610.00,
            0.72, 1  -- Alert triggered (>5% change)
        );

        INSERT INTO vs_dcf_details (...);  -- Full 10-year projections
        INSERT INTO vs_relative_valuations (...);  -- Peer analysis

                ▼
        ┌─────────────────────────────────────────────┐
        │  Step 2i: Compare vs Previous Valuation     │
        └─────────────────────────────────────────────┘

        Previous (2026-02-04): Rs 2,310
        Current (2026-02-05): Rs 2,483
        Change: +7.5% (exceeds 5% threshold!)

        Root Cause Analysis:
        • Driver change: china_plus_one MODERATE → HIGH (+2.5% revenue)
        • Driver change: crude_oil $75 → $85 (-0.3% margin)
        • Net impact: +7.5% intrinsic value

        CREATE ALERT:
        INSERT INTO vs_alerts (
            company_id, alert_type, severity,
            message, intrinsic_old, intrinsic_new
        ) VALUES (
            47582, 'VALUATION_CHANGE', 'HIGH',
            'Intrinsic value up 7.5% due to china_plus_one upgrade',
            2310.00, 2483.00
        );

    ┌───────────────────────────────────────────────────┐
    │  STEP 3: Generate Alerts                          │
    └───────────────────────────────────────────────────┘

    EmailSender sends:

    To: medury@gmail.com
    Subject: ALERT: Aether Industries intrinsic value up 7.5%

    Body:
    """
    Aether Industries Ltd. - Valuation Alert

    Intrinsic Value Change:
    • Previous: Rs 2,310
    • Current:  Rs 2,483
    • Change:   +Rs 173 (+7.5%)

    Current Market Price: Rs 1,100
    Upside to Intrinsic: +125.7%

    Root Cause:
    • china_plus_one driver: MODERATE → HIGH
      Impact: +2.5% revenue growth
      Reasoning: China tariff barriers accelerate global buyer shift to India

    • crude_oil: $75 → $85
      Impact: -0.3% EBITDA margin
      Reasoning: RM cost pressure, partial pass-through

    Net Effect: +7.5% intrinsic value

    Recommendation: STRONG BUY (confidence: 72%)

    View full analysis: [Link to Excel report]
    """

    ┌───────────────────────────────────────────────────┐
    │  STEP 4: Send Daily Digest                        │
    └───────────────────────────────────────────────────┘

    EmailSender sends:

    To: medury@gmail.com
    Subject: Daily Valuation Digest - 2026-02-05

    Body:
    """
    Daily Valuation Summary

    Companies Valued: 2 (Aether, Eicher)
    Alerts Triggered: 1 (Aether +7.5%)

    Significant Events Processed: 3
    • China tariff on chemicals (HIGH severity)
    • RBI keeps rates unchanged (MODERATE)
    • Aether Q3 earnings inline (LOW)

    Driver Changes: 4
    • SECTOR china_plus_one: MODERATE → HIGH
    • MACRO crude_oil: $75 → $85
    • COMPANY aether r_and_d_intensity: 4.2% → 4.5%
    • SECTOR pharma_api_demand: 10% → 12%

    Top Movers:
    1. Aether: +7.5% (China+1 upgrade)
    2. Eicher: -1.2% (crude oil headwind)

    System Health:
    • MySQL: ✓ OK
    • ChromaDB: ✓ OK
    • Google Sheets: ✓ OK
    • Internet: ✓ OK

    Next valuation: 2026-02-06 20:00 IST
    """
```

**Daily Cycle Duration**: ~10-15 minutes
**Log File**: `/Users/ram/code/research/valuation_system/logs/daily.log`

---

### Phase 3: Social Media Content (08:00 IST Daily)

```
┌─────────────────────────────────────────────────────────────────┐
│  SOCIAL JOB (Triggered by launchd at 08:00 IST)                │
└─────────────────────────────────────────────────────────────────┘
                            ▼
                    ┌───────────────┐
                    │ ORCHESTRATOR  │
                    └───────┬───────┘
                            ▼
                ┌───────────────────┐
                │  CONTENT AGENT    │
                └───────────────────┘
                            ▼
    ┌───────────────────────────────────────────────────┐
    │  STEP 1: Analyze Recent Changes                   │
    └───────────────────────────────────────────────────┘

    Query vs_valuations for last 7 days:
    • Top movers (>5% valuation change)
    • Driver trends (which drivers are hot?)
    • Sector patterns (any sector-wide shifts?)

    Query vs_drivers for trending drivers:
    • Which drivers changed most frequently?
    • Any structural shifts (MACRO/SECTOR level)?

    Example findings:
    • Specialty chemicals: china_plus_one trending UP
    • 3 companies in sector saw +5-8% valuation increases
    • Crude oil up 13% in 2 weeks

                            ▼
    ┌───────────────────────────────────────────────────┐
    │  STEP 2: LLM Content Generation                   │
    └───────────────────────────────────────────────────┘

    LLM Prompt:
    "You are a professional equity research analyst writing insightful tweets.

    Recent analysis:
    • Specialty chemicals valuations up 5-8% this week
    • Driver: china_plus_one upgraded to HIGH (from MODERATE)
    • Crude oil at $85 (up from $75, but companies have pricing power)
    • 3 companies affected: Aether (+7.5%), SRF (+6.2%), Aarti (+5.8%)

    Style guidelines:
    • Lead with the insight, not the data
    • Use specific numbers when possible
    • End with a question or perspective
    • No stock recommendations or price targets
    • Professional tone, thought-provoking
    • 2-3 sentences max
    • Include relevant hashtags

    Generate 2-3 tweet options for today."

    LLM Output:
    [
      {
        "content": "Specialty chemicals seeing structural tailwind as China tariffs push global buyers to diversify. Indian companies with REACH/FDA compliance gaining share at higher margins. The moat deepens. When does 'China+1' become 'India First'? #SpecChem #India",
        "rationale": "Focuses on structural shift, uses concrete example (REACH/FDA), ends with thought-provoking question",
        "priority": 1
      },
      {
        "content": "Crude at $85 but specialty chemical margins stable at 21%. Why? Pricing power from regulatory barriers + customer stickiness in CRAMS. Not all commodity exposure is created equal. #Chemicals #India",
        "rationale": "Counter-intuitive insight (crude up, margins stable), explains the why",
        "priority": 2
      },
      {
        "content": "Three specialty chemical companies saw valuations up 5-8% this week. Same driver: China+1 momentum. Market starting to price in the structural shift. Are we still early? #SpecChem",
        "rationale": "Pattern recognition across companies, forward-looking question",
        "priority": 3
      }
    ]

                            ▼
    ┌───────────────────────────────────────────────────┐
    │  STEP 3: Write Drafts to Google Sheet             │
    └───────────────────────────────────────────────────┘

    GSheetClient (uses existing workflow from RAGApp):
    • Spreadsheet: "storm posting" (env: GSHEET_SOCIAL_ID)
    • Sheet: "posts"

    Append rows:
    | Date       | Content                                      | Approval | Posted  | Priority |
    |------------|----------------------------------------------|----------|---------|----------|
    | 2026-02-06 | Specialty chemicals seeing structural...     |          |         | 1        |
    | 2026-02-06 | Crude at $85 but specialty chemical...       |          |         | 2        |
    | 2026-02-06 | Three specialty chemical companies...        |          |         | 3        |

                            ▼
    ┌───────────────────────────────────────────────────┐
    │  STEP 4: PM APPROVAL WORKFLOW (Manual)            │
    └───────────────────────────────────────────────────┘

    1. PM opens Google Sheet
    2. Reviews draft tweets
    3. Edits content if needed (directly in sheet)
    4. Sets Approval = "YES" for tweets to post
    5. Runs: python /Users/ram/code/rag/machai/twitter/post_tweets_from_gsheet.py
    6. Script reads sheet, posts approved tweets to Twitter
    7. Updates Posted = "2026-02-06 08:45"

    This ensures PM oversight - no autonomous posting!
```

**Social Cycle Duration**: ~1-2 minutes (drafting only)
**Log File**: `/Users/ram/code/research/valuation_system/logs/social.log`

---

## Agent Ecosystem

### Agent Hierarchy

```
┌────────────────────────────────────────────────────────────────┐
│                       ORCHESTRATOR AGENT                       │
│                     (Main Coordinator)                         │
│                                                                │
│  Responsibilities:                                             │
│  • Schedule and coordinate all agents                          │
│  • Handle catchup after gaps (machine off, internet down)      │
│  • Detect material valuation changes → trigger alerts          │
│  • Maintain model history and audit trail                      │
│  • Replay queued operations after service recovery             │
│                                                                │
│  File: agents/orchestrator.py                                  │
└────────────┬──────────────┬───────────────┬──────────────┬────┘
             │              │               │              │
    ┌────────▼────┐  ┌──────▼──────┐  ┌────▼─────┐  ┌────▼─────┐
    │   NEWS      │  │   SECTOR    │  │ VALUATOR │  │ CONTENT  │
    │  SCANNER    │  │  ANALYST    │  │          │  │  AGENT   │
    └─────────────┘  └─────────────┘  └──────────┘  └──────────┘
```

---

### 1. ORCHESTRATOR AGENT

**File**: `agents/orchestrator.py`
**Singleton**: One instance for entire system

**Key Methods**:

```python
run_hourly_cycle() -> dict
    """
    Hourly execution:
    1. Check for missed runs → catchup if needed
    2. Scan news sources
    3. Classify and store significant events
    4. Update sector drivers from news
    5. Check for critical events requiring immediate valuation

    Returns: {
        'status': 'SUCCESS',
        'articles_scanned': 12,
        'significant_events': 3,
        'driver_changes': {'specialty_chemicals': 2, 'automobiles': 1},
        'critical_events': 0,
        'elapsed_seconds': 245.7
    }
    """

run_daily_valuation() -> dict
    """
    Daily valuation refresh:
    1. Reload price data (file may have been updated)
    2. Calculate sector outlooks
    3. Run full valuation for each active company
    4. Compare with previous valuation
    5. Generate alerts for material changes (>5%)
    6. Store all results

    Returns: {
        'status': 'SUCCESS',
        'valuations': {'aether_industries': {...}, 'eicher_motors': {...}},
        'alerts': 1,
        'elapsed_seconds': 892.3
    }
    """

run_on_demand(company_key: str = None, nse_symbol: str = None) -> dict
    """
    On-demand single company valuation.
    Used for: Manual requests, CRITICAL event response

    Returns: {
        'company_name': 'Aether Industries Ltd.',
        'cmp': 1100.00,
        'intrinsic_value_blended': 2483.00,
        'upside_pct': 125.7,
        'dcf_base': 1593.00,
        'confidence_score': 0.72,
        ...
    }
    """

get_system_status() -> dict
    """
    Health check for all components.

    Returns: {
        'dependencies': {'mysql': True, 'chromadb': True, 'internet': True},
        'task_states': {'hourly_cycle': {...}, 'daily_valuation': {...}},
        'data_staleness': {'core_csv': {...}, 'prices': {...}},
        'queued_operations': 0,
        'active_sectors': ['specialty_chemicals', 'automobiles'],
        'active_companies': ['aether_industries', 'eicher_motors']
    }
    """
```

**Edge Cases Handled**:
- Machine was off for N days → Runs catchup for missed days
- Internet unavailable → Uses cached data, queues operations
- MySQL down → Queues DB writes, uses local state
- ChromaDB down → Skips vector ops, uses metadata only
- Concurrent runs → Lock mechanism prevents overlap (RunStateManager)

---

### 2. NEWS SCANNER AGENT

**File**: `agents/news_scanner.py`
**Singleton**: One instance for entire system

**Responsibilities**:
1. Scrape news from multiple sources
2. Extract: headline, summary, date, source URL
3. **LLM Classification** → Categorize and assess severity
4. Store in MySQL (metadata) + ChromaDB (full text for RAG)

**Data Sources**:
- Economic Times (RSS + web scraping)
- MoneyControl (sector news)
- BSE/NSE announcements (filings)
- Twitter feeds (breaking news) - optional

**LLM Classification Schema**:
```python
{
  "category": "REGULATORY | MANAGEMENT | PRODUCT | MA | MACRO | COMPETITOR | GOVERNANCE | EARNINGS | POLICY",
  "scope": "MACRO | SECTOR | COMPANY",
  "affected_sector": "specialty_chemicals" or null,
  "affected_company": "aether_industries" or null,
  "severity": "CRITICAL | HIGH | MODERATE | LOW",
  "synopsis": "1-2 sentence summary",
  "confidence": "HIGH | MEDIUM | LOW",
  "actionable": true/false
}
```

**Storage**:

MySQL `vs_news` table:
```sql
INSERT INTO vs_news (
    event_type, scope, company_id, sector,
    severity, headline, synopsis, source_url,
    published_at, chromadb_id
) VALUES (...);
```

ChromaDB `RAG_GPT` collection:
```python
collection.add(
    documents=[full_article_text],
    metadatas=[{
        'source': 'Economic Times',
        'event_id': news_id,
        'category': 'REGULATORY',
        ...
    }],
    ids=[f'news_{news_id}']
)
```

**Key Method**:
```python
classify_and_store(articles: list) -> list
    """
    For each article:
    1. Call LLM to classify
    2. Store in MySQL + ChromaDB
    3. Return list of significant events (severity >= MODERATE)

    Returns: [
        {
            'news_id': 12345,
            'category': 'REGULATORY',
            'scope': 'SECTOR',
            'affected_sector': 'specialty_chemicals',
            'severity': 'HIGH',
            'headline': 'China imposes 10% export tariff...',
            ...
        },
        ...
    ]
    """
```

---

### 3. SECTOR ANALYST AGENT

**File**: `agents/sector_analyst.py`
**Instances**: One per sector (Specialty Chemicals, Automobiles, etc.)

**Responsibilities**:
1. Receive news events from Orchestrator
2. **LLM Impact Analysis** → Determine which drivers are affected
3. Update Google Sheets with new driver values
4. Sync to MySQL `vs_drivers` table
5. Calculate sector outlook score (weighted sum of drivers)

**Driver State Management**:
```python
self._driver_states = {
    'SECTOR_crude_oil': {
        'level': 'SECTOR',
        'category': 'cost_drivers',
        'name': 'crude_oil',
        'value': 85,
        'weight': 0.10,
        'impact_direction': 'UP',
        'trend': 'UP',
        'sensitivity': -0.5  # -0.5% margin per $1 increase
    },
    'SECTOR_china_plus_one': {
        'level': 'SECTOR',
        'category': 'demand_drivers',
        'name': 'china_plus_one',
        'value': 'HIGH',
        'weight': 0.10,
        'impact_direction': 'UP',
        'trend': 'UP'
    },
    ...
}
```

**LLM Prompts**:

```python
DRIVER_IMPACT_PROMPT = """You are an equity research sector analyst for {sector}.

Given this news event:
Headline: {headline}
Summary: {summary}
Severity: {severity}

And the current sector drivers:
{current_drivers}

Determine:
1. Which drivers are affected (from the list above)
2. Direction of impact (UP, DOWN, STABLE) for each
3. Magnitude (MINOR: <0.5% valuation impact, MODERATE: 0.5-2%, SIGNIFICANT: >2%)
4. New suggested value/state for each affected driver
5. Estimated impact on revenue growth, margins, and terminal value
6. Confidence level (HIGH, MEDIUM, LOW)
7. Time horizon (IMMEDIATE, SHORT_TERM_3M, MEDIUM_TERM_1Y, STRUCTURAL)

Return as JSON with structure:
{
  "affected_drivers": [
    {
      "driver_name": "...",
      "old_state": "...",
      "new_state": "...",
      "direction": "UP/DOWN/STABLE",
      "magnitude": "MINOR/MODERATE/SIGNIFICANT",
      "confidence": "HIGH/MEDIUM/LOW",
      "time_horizon": "...",
      "reasoning": "..."
    }
  ],
  "revenue_growth_impact_pct": 0.0,
  "margin_impact_pct": 0.0,
  "terminal_value_impact": "NONE/MINOR/MODERATE/SIGNIFICANT",
  "overall_assessment": "..."
}"""
```

**Key Methods**:
```python
update_drivers_from_news(news_events: list) -> list
    """
    For each news event:
    1. Call LLM to analyze impact on drivers
    2. Update self._driver_states
    3. Write to Google Sheets
    4. Sync to MySQL

    Returns: List of driver changes
    [
        {
            'driver_name': 'china_plus_one',
            'old_value': 'MODERATE',
            'new_value': 'HIGH',
            'impact': 2.5,  # % impact on valuation
            'event_id': 12345
        },
        ...
    ]
    """

calculate_sector_outlook() -> float
    """
    Weighted sum of all sector drivers.

    Returns: Score from 0.0 to 1.0
    0.0 = Very bearish
    0.5 = Neutral
    1.0 = Very bullish

    Used to adjust relative valuation multiples.
    """
```

---

### 4. VALUATOR AGENT

**File**: `agents/valuator.py`
**Singleton**: One instance for entire system

**Responsibilities**:
1. Load financial data from CSV
2. Load current prices
3. Load driver states from MySQL
4. **LLM Driver Synthesis** → Convert driver states to valuation parameters
5. Run DCF (Bull/Base/Bear scenarios)
6. Run Relative Valuation
7. Run Monte Carlo simulation
8. Blend valuations (60/30/10)
9. Write results to MySQL

**Dependencies**:
- `models/financial_processor.py` → TTM metrics, ratios
- `models/dcf_model.py` → DCF valuation
- `models/relative_valuation.py` → Peer multiples
- `models/monte_carlo.py` → Scenario simulation

**LLM Synthesis Prompt**:
```python
DRIVER_SYNTHESIS_PROMPT = """You are a valuation analyst for {company_name} ({sector}).

Given current driver states across all levels:

MACRO (20% weight):
{macro_drivers}

SECTOR (55% weight):
{sector_drivers}

COMPANY (25% weight):
{company_drivers}

And historical financials:
• TTM Revenue: {ttm_revenue}
• Revenue CAGR 3Y: {cagr_3y}%
• EBITDA Margin: {ebitda_margin}%
• ROCE: {roce}%

Calculate valuation adjustments:
1. Revenue growth adjustment (% points vs base case)
2. EBITDA margin adjustment (% points)
3. Terminal margin override (if structural change)
4. Confidence adjustment (based on driver certainty)
5. Reasoning for each adjustment

Return as JSON:
{
  "revenue_growth_adjustment_pct": 0.0,
  "reasoning_revenue": "...",
  "margin_adjustment_pct": 0.0,
  "reasoning_margin": "...",
  "terminal_margin_override": null or 0.XX,
  "reasoning_terminal": "...",
  "confidence_adjustment": 0.0,
  "reasoning_confidence": "...",
  "overall_assessment": "..."
}"""
```

**Key Method**:
```python
value_company(company_key: str) -> dict
    """
    Full valuation workflow for one company.

    Steps:
    1. Load company config from companies.yaml
    2. Load financial data from Core CSV
    3. Load current price from monthly CSV
    4. Load all drivers (MACRO + SECTOR + COMPANY)
    5. Call LLM to synthesize driver impacts
    6. Run DCF (Bull/Base/Bear)
    7. Run Relative Valuation
    8. Run Monte Carlo
    9. Blend (60/30/10)
    10. Write to MySQL

    Returns: {
        'company_name': '...',
        'company_id': 47582,
        'cmp': 1100.00,
        'intrinsic_value_blended': 2483.00,
        'upside_pct': 125.7,
        'dcf_base': 1593.00,
        'dcf_bull': 1950.00,
        'dcf_bear': 1280.00,
        'relative_value': 4554.00,
        'monte_carlo_median': 1610.00,
        'confidence_score': 0.72,
        'valuation_date': '2026-02-05',
        ...
    }
    """
```

---

### 5. CONTENT AGENT

**File**: `agents/content_agent.py`
**Singleton**: One instance for entire system

**Responsibilities**:
1. Analyze recent valuation changes + driver trends
2. **LLM Content Generation** → Draft insightful tweets
3. Write drafts to Google Sheet for PM approval
4. (PM manually approves and posts via separate script)

**Content Guidelines** (from companies.yaml):
```yaml
social_media:
  tone: "Professional equity researcher, data-driven, thought-provoking"
  style_guidelines:
    - "Lead with the insight, not the data"
    - "Use specific numbers when possible"
    - "End with a question or perspective"
    - "No stock recommendations or price targets"
    - "Include relevant hashtags"
  max_posts_per_day: 3
  post_time: "08:00"  # IST, before market open
```

**LLM Content Prompt**:
```python
CONTENT_GENERATION_PROMPT = """You are a professional equity research analyst writing insightful tweets for a sophisticated audience.

Recent analysis findings:
{findings}

Example past tweets:
• "Specialty chemicals margins expanding despite crude at $75. Why? China+1 is structural, not cyclical. Companies with REACH compliance are gaining share at higher margins. The moat deepens. #SpecChem #India"
• "Eicher's premium 2W market share hit 28% - but the real story is exports at 25% CAGR. When does international become the bigger growth driver? #EicherMotors #2Wheelers"

Style guidelines:
{style_guidelines}

Generate {num_posts} tweet options for today (ranked by priority).

Return as JSON:
[
  {
    "content": "Tweet text here...",
    "rationale": "Why this insight matters...",
    "priority": 1
  },
  ...
]"""
```

**Key Method**:
```python
generate_daily_posts() -> list
    """
    1. Query vs_valuations for last 7 days → Find top movers
    2. Query vs_drivers for trending drivers
    3. Identify patterns (sector trends, driver themes)
    4. Call LLM to generate 2-3 tweet drafts
    5. Return drafts

    Returns: [
        {
            'content': 'Tweet text...',
            'priority': 1,
            'rationale': '...'
        },
        ...
    ]
    """

publish_posts(posts: list) -> dict
    """
    Write drafts to Google Sheet for PM approval.
    Does NOT post directly to Twitter!

    1. Open Google Sheet: "storm posting" / "posts"
    2. Append rows with drafts
    3. PM reviews manually
    4. PM sets Approval = "YES"
    5. PM runs separate script to post approved tweets

    Returns: {
        'status': 'QUEUED',
        'posts_queued': 3,
        'sheet_url': '...'
    }
    """
```

---

## Scheduled Jobs

### launchd Configuration (macOS)

**4 Scheduled Jobs**:

| Job | File | Trigger | Command | Log |
|-----|------|---------|---------|-----|
| **Hourly** | `com.valuation.hourly.plist` | Every 60 min | `python -m valuation_system.scheduler.runner hourly` | `logs/hourly.log` |
| **Daily** | `com.valuation.daily.plist` | 20:00 IST | `python -m valuation_system.scheduler.runner daily` | `logs/daily.log` |
| **Social** | `com.valuation.social.plist` | 08:00 IST | `python -m valuation_system.scheduler.runner social` | `logs/social.log` |
| **Regression** | `com.valuation.regression.plist` | 06:00 IST | `python -m valuation_system.scheduler.runner test` | `logs/regression.log` |

**Installation**:
```bash
# Copy plist files to launchd directory
cp scheduler/*.plist ~/Library/LaunchAgents/

# Load jobs
launchctl load ~/Library/LaunchAgents/com.valuation.hourly.plist
launchctl load ~/Library/LaunchAgents/com.valuation.daily.plist
launchctl load ~/Library/LaunchAgents/com.valuation.social.plist
launchctl load ~/Library/LaunchAgents/com.valuation.regression.plist

# Check status
launchctl list | grep valuation
```

**Manual Execution** (for testing):
```bash
# Run hourly cycle manually
python -m valuation_system.scheduler.runner hourly

# Run daily valuation manually
python -m valuation_system.scheduler.runner daily

# Run on-demand valuation for Aether
python -m valuation_system.scheduler.runner valuation --symbol AETHER

# Check system status
python -m valuation_system.scheduler.runner status

# Run regression tests
python -m valuation_system.scheduler.runner test
```

---

## Driver Hierarchy

### Hierarchical Driver Framework

**Total Weight**: 100% = Macro (20%) + Sector (55%) + Company (25%)

**Principle**: *"Macro sets the ceiling and floor. Sectors determine the value. Companies decide who wins."*

---

### MACRO LEVEL (20% weight)

**Stored in**: Google Sheet "Macro Drivers" + MySQL `vs_drivers` (driver_level='MACRO')

**Examples**:
| Driver | Metric | Weight | Impact | Current (Example) |
|--------|--------|--------|--------|-------------------|
| gdp_growth | % | 0.05 | Baseline growth | 7.2% |
| inflation | % | 0.04 | Margin pressure | 5.5% |
| usd_inr | Rate | 0.03 | Export/import margins | 83.5 |
| crude_oil | $/bbl | 0.05 | Cost of goods, logistics | $85 |
| interest_rate_10y | % | 0.03 | Risk-free rate for WACC | 7.1% |

**How it's used**:
- Sets risk-free rate for WACC calculation
- Provides baseline growth rate (GDP growth)
- Affects all sectors and companies uniformly
- News with scope='MACRO' updates these drivers

---

### SECTOR LEVEL (55% weight)

#### **Specialty Chemicals Sector**

**Stored in**: Google Sheet "Sector - Specialty Chemicals" + MySQL `vs_drivers` (driver_level='SECTOR', sector='Specialty Chemicals')

**Demand Drivers** (total ~0.44):
| Driver | Weight | Impact | Current (Example) |
|--------|--------|--------|-------------------|
| volume_growth | 0.12 | Revenue growth | 8% |
| china_plus_one | 0.10 | Market share expansion | HIGH |
| pharma_api_demand | 0.08 | End market strength | 12% growth |
| agrochem_demand | 0.06 | Revenue volatility | MODERATE |
| pricing_power | 0.08 | Margin expansion | MODERATE |

**Cost Drivers** (total ~0.25):
| Driver | Weight | Impact | Sensitivity | Current (Example) |
|--------|--------|--------|-------------|-------------------|
| crude_oil | 0.10 | Raw material cost | -0.5 | $85/bbl |
| natural_gas | 0.06 | Power cost | - | $3.5/mmbtu |
| operating_leverage | 0.04 | Margin sensitivity | - | 0.6 |
| fx_on_costs | 0.05 | Margin hedge | - | NEUTRAL |

**Regulatory Drivers** (total ~0.13):
| Driver | Weight | Impact | Current (Example) |
|--------|--------|--------|-------------------|
| pli_scheme | 0.04 | Capex subsidy | ACTIVE |
| eu_reach_compliance | 0.03 | Competitive moat | HIGH |
| fda_approval_rate | 0.03 | Market access | 85% |
| environmental_capex | 0.03 | Reinvestment burden | MODERATE |

**Porter's Forces** (qualitative):
| Force | Level | Drivers | Impact |
|-------|-------|---------|--------|
| Entry Barriers | HIGH | Capex intensity, regulatory barriers, scale | ROCE sustainability |
| Supplier Power | LOW | Input concentration, commodity linkage | Cost stability |
| Buyer Power | MODERATE | Customer concentration, contract type | Pricing pressure |
| Substitutes | LOW | Technology risk, product obsolescence | Terminal value protection |
| Rivalry | CONSOLIDATING | Capacity additions, pricing discipline | Margin expansion |

**Terminal Assumptions**:
- Margin range: 18-25%
- ROCE convergence: 20%
- Reinvestment rate: 35%

---

#### **Automobiles Sector**

**Stored in**: Google Sheet "Sector - Automobiles" + MySQL `vs_drivers` (driver_level='SECTOR', sector='Automobile & Ancillaries')

**Demand Drivers**:
| Driver | Weight | Impact | Current (Example) |
|--------|--------|--------|-------------------|
| industry_volume | 0.12 | Revenue growth | 5% |
| rural_demand | 0.08 | Volume uplift | INDEX 105 |
| urban_demand | 0.06 | Volume | INDEX 110 |
| export_growth | 0.06 | Diversification | 15% |
| pricing_power | 0.08 | ASP/margin | MODERATE |
| premium_mix | 0.06 | ASP/margin | 28% |

**Cost Drivers**:
| Driver | Weight | Impact | Sensitivity | Current (Example) |
|--------|--------|--------|-------------|-------------------|
| steel_prices | 0.08 | RM cost | -0.3 | Rs 55/kg |
| aluminum_prices | 0.04 | RM cost | - | $2,400/MT |
| semiconductor_supply | 0.04 | Production | - | INDEX 95 |
| battery_costs | 0.06 | EV profitability | - | $120/kWh |
| operating_leverage | 0.04 | Margin sensitivity | - | 0.7 |

**Terminal Assumptions**:
- Margin range: 12-18% (lower than chemicals)
- ROCE convergence: 25% (higher than chemicals due to brand moat)
- Reinvestment rate: 30%

---

### COMPANY LEVEL (25% weight)

#### **Aether Industries (Specialty Chemicals)**

**Stored in**: Google Sheet "Company - Aether Industries" + MySQL `vs_drivers` (driver_level='COMPANY', company_id=47582)

**Alpha Drivers**:
| Category | Driver | Weight | Metric | Current (Example) |
|----------|--------|--------|--------|-------------------|
| Market Share | share_trajectory | 0.06 | YoY Δ | +2.5% |
| | segment_leadership | 0.05 | Position | #3 in CRAMS |
| Mgmt Quality | capital_allocation | 0.06 | Score /10 | 8/10 |
| | r_and_d_intensity | 0.05 | % of Sales | 4.5% |
| | execution_track_record | 0.04 | Rating | STRONG |
| Capex | expansion_plans | 0.05 | Rs Cr | 300 (new plant) |
| | execution_risk | 0.04 | Status | ON_TRACK |
| | roi_on_past_capex | 0.04 | % | 18% |
| Geographic | export_pct | 0.04 | % Revenue | 60% |
| | fx_exposure | 0.03 | Net Impact | +ve (exporter) |
| Product Mix | crams_pct | 0.05 | % Revenue | 35% |
| | new_molecule_pipeline | 0.04 | Count | 8 |
| | customer_stickiness | 0.03 | Avg Tenure | 7 years |
| Balance Sheet | net_debt_ebitda | 0.04 | Ratio | 0.8x |
| | interest_coverage | 0.02 | Ratio | 12x |
| Governance | promoter_holding | 0.03 | % | 72% |
| | pledge_pct | 0.02 | % | 0% |
| | related_party_risk | 0.02 | Score | LOW |
| Key Personnel | ceo_cfo_stability | 0.02 | Tenure | 8 years |
| | key_scientist_exits | 0.02 | Count | 0 |
| Litigation | pending_cases | 0.01 | Count | 2 |
| | regulatory_actions | 0.02 | Status | NONE |

**Total Company Weight**: 0.25 (25%)

---

#### **Eicher Motors (Automobiles)**

**Alpha Drivers**:
| Category | Driver | Weight | Metric | Current (Example) |
|----------|--------|--------|--------|-------------------|
| Market Share | premium_2w_share | 0.08 | % | 28% |
| | share_trajectory | 0.04 | YoY Δ | +1.2% |
| Mgmt Quality | capital_allocation | 0.06 | Score /10 | 9/10 |
| | brand_building | 0.05 | Score | EXCELLENT |
| | cost_discipline | 0.04 | OPM Trend | Stable 25% |
| Capex | new_model_pipeline | 0.06 | Count | 3 models |
| | capacity_expansion | 0.03 | Status | Phase 2 done |
| | ev_readiness | 0.03 | Rating | MODERATE |
| Geographic | export_pct | 0.05 | % Revenue | 15% |
| | export_growth | 0.04 | YoY % | 25% |
| | key_markets | 0.03 | Focus | LAM, ASEAN |
| Product Mix | premium_mix | 0.05 | % Units | 82% |
| | accessories_spares_pct | 0.03 | % Revenue | 8% |
| Balance Sheet | cash_position | 0.04 | Rs Cr | 4,200 |
| | dividend_yield | 0.02 | % | 0.8% |
| VECV JV | cv_cycle_position | 0.04 | Stage | UPTURN |
| | jv_profitability | 0.03 | OPM % | 6.5% |

---

## LLM Integration Points

### Where LLMs Are Used

**5 LLM Touchpoints in the System**:

#### 1. **News Classification** (NewsScannerAgent)

**Input**: Raw news article text
**Output**: Structured classification
**Model**: Grok (primary), Ollama (fallback)
**Frequency**: Hourly (per article scraped)

```python
LLM Input:
"Analyze this article:
 Headline: 'India GDP grows 7.2% in Q3FY26'
 Text: 'India's economy expanded 7.2%...'

 Classify: category, scope, severity, synopsis"

LLM Output:
{
  "category": "MACRO",
  "scope": "MACRO",
  "severity": "MODERATE",
  "synopsis": "India GDP exceeds expectations, supports baseline growth assumptions",
  "confidence": "HIGH"
}
```

---

#### 2. **Driver Impact Analysis** (SectorAnalystAgent)

**Input**: News event + current driver states
**Output**: Which drivers to update + new values
**Model**: Grok (primary), Ollama (fallback)
**Frequency**: Hourly (per significant event)

```python
LLM Input:
"News: 'China imposes 10% tariff on chemicals'
 Current drivers: china_plus_one=MODERATE, crude_oil=$85...

 Which drivers affected? New values?"

LLM Output:
{
  "affected_drivers": [
    {
      "driver_name": "china_plus_one",
      "old_state": "MODERATE",
      "new_state": "HIGH",
      "magnitude": "SIGNIFICANT",
      "reasoning": "..."
    }
  ],
  "revenue_growth_impact_pct": 2.5,
  "margin_impact_pct": 0.5
}
```

---

#### 3. **Driver Synthesis** (ValuatorAgent)

**Input**: All driver states (Macro + Sector + Company)
**Output**: Valuation parameter adjustments
**Model**: Grok (primary), Ollama (fallback)
**Frequency**: Daily (per company)

```python
LLM Input:
"Company: Aether Industries
 Drivers: china_plus_one=HIGH, crude_oil=$85, r_and_d_intensity=4.5%...

 Calculate: revenue growth adjustment, margin adjustment"

LLM Output:
{
  "revenue_growth_adjustment_pct": 1.5,
  "margin_adjustment_pct": -0.3,
  "terminal_margin_override": null,
  "confidence_adjustment": 0.05,
  "reasoning": "China+1 tailwind offset by crude headwind..."
}
```

---

#### 4. **Content Generation** (ContentAgent)

**Input**: Recent valuation changes + driver trends
**Output**: Draft social media posts
**Model**: Grok (primary)
**Frequency**: Daily (08:00 IST)

```python
LLM Input:
"Recent: Specialty chemicals up 5-8%, china_plus_one trending UP

 Generate 2-3 insightful tweets (professional tone, thought-provoking)"

LLM Output:
[
  {
    "content": "Specialty chemicals seeing structural tailwind as China tariffs...",
    "priority": 1,
    "rationale": "Focuses on structural shift, ends with question"
  },
  ...
]
```

---

#### 5. **Valuation Commentary** (Optional - future)

**Input**: Final valuation results + comparison to market
**Output**: Written explanation for report
**Model**: Grok
**Frequency**: On-demand (when generating reports)

```python
LLM Input:
"Aether: Intrinsic Rs 2,483, CMP Rs 1,100 (126% upside)
 Market PE 45x, DCF implied PE 17x

 Explain the gap"

LLM Output:
"Market is pricing in aggressive growth expectations (45x PE implies 35%+ earnings CAGR).
 Our DCF is more conservative at 16.5% revenue growth, suggesting potential overvaluation
 OR market has visibility into drivers we're underweighting (possible order book strength)."
```

---

### LLM Fallback Chain

**Primary**: Grok (grok-2-1212) via OpenAI SDK
**Fallback 1**: Ollama (local model)
**Fallback 2**: OpenAI (if both fail)

```python
# utils/llm_client.py
class LLMClient:
    def generate(self, prompt, model='grok'):
        try:
            # Try Grok first
            return self._call_grok(prompt)
        except Exception as e:
            logger.warning(f"Grok failed: {e}, trying Ollama")
            try:
                return self._call_ollama(prompt)
            except Exception as e2:
                logger.error(f"Ollama failed: {e2}, trying OpenAI")
                return self._call_openai(prompt)
```

**Why Grok Primary?**
- Better at financial domain reasoning
- Cheaper than OpenAI for high volume
- Faster response times

---

## Data Flow Diagrams

### Complete System Data Flow

```
┌─────────────────────────────────────────────────────────────────────────┐
│                          DATA SOURCES                                   │
└─────────────────────────────────────────────────────────────────────────┘
            │                    │                    │
            ▼                    ▼                    ▼
    ┌──────────────┐    ┌──────────────┐    ┌──────────────┐
    │  Core CSV    │    │ Monthly      │    │  News        │
    │  (Financial) │    │ Prices       │    │  Sources     │
    │  ~9000 cos   │    │ (CMP, PE..)  │    │  (ET, MC..)  │
    └──────┬───────┘    └──────┬───────┘    └──────┬───────┘
           │                   │                    │
           └───────────────────┼────────────────────┘
                               │
                               ▼
                    ┌──────────────────────┐
                    │   DATA LOADERS       │
                    │  • CoreDataLoader    │
                    │  • PriceLoader       │
                    │  • NewsScannerAgent  │
                    └──────────┬───────────┘
                               │
                               ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                      PROCESSING LAYER                                   │
└─────────────────────────────────────────────────────────────────────────┘
            │                    │                    │
            ▼                    ▼                    ▼
    ┌──────────────┐    ┌──────────────┐    ┌──────────────┐
    │   LLM        │    │  Financial   │    │   Sector     │
    │ Classification│   │  Processor   │    │   Analyst    │
    │              │    │              │    │   (LLM)      │
    └──────┬───────┘    └──────┬───────┘    └──────┬───────┘
           │                   │                    │
           │                   │                    │
           ▼                   ▼                    ▼
    ┌──────────────┐    ┌──────────────┐    ┌──────────────┐
    │   ChromaDB   │    │    MySQL     │    │ Google       │
    │   (RAG)      │    │  vs_news     │    │ Sheets       │
    │              │    │  vs_drivers  │    │ (Drivers)    │
    └──────────────┘    └──────┬───────┘    └──────┬───────┘
                               │                    │
                               └────────┬───────────┘
                                        │
                                        ▼
                            ┌───────────────────────┐
                            │   VALUATOR AGENT      │
                            │  • Reads drivers      │
                            │  • LLM synthesis      │
                            │  • DCF/Relative/MC    │
                            │  • Blended value      │
                            └───────────┬───────────┘
                                        │
                                        ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                      STORAGE LAYER                                      │
└─────────────────────────────────────────────────────────────────────────┘
            │                    │                    │
            ▼                    ▼                    ▼
    ┌──────────────┐    ┌──────────────┐    ┌──────────────┐
    │   MySQL      │    │  Google      │    │   Log        │
    │ vs_valuations│    │  Sheets      │    │   Files      │
    │ vs_dcf_      │    │  Valuation   │    │   (Audit     │
    │ details      │    │  History     │    │   Trail)     │
    └──────┬───────┘    └──────┬───────┘    └──────────────┘
           │                   │
           └────────┬──────────┘
                    │
                    ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                      OUTPUT LAYER                                       │
└─────────────────────────────────────────────────────────────────────────┘
            │                    │                    │
            ▼                    ▼                    ▼
    ┌──────────────┐    ┌──────────────┐    ┌──────────────┐
    │   Email      │    │  Excel       │    │  Social      │
    │   Alerts     │    │  Reports     │    │  Media       │
    │              │    │  (9 sheets)  │    │  (GSheet)    │
    └──────────────┘    └──────────────┘    └──────────────┘
```

---

### Driver Update Flow (Hourly)

```
Economic Times        MoneyControl        BSE/NSE
     │                     │                 │
     └─────────────────────┼─────────────────┘
                           │
                           ▼
                  ┌─────────────────┐
                  │ NewsScannerAgent│
                  │  (scrape)       │
                  └────────┬────────┘
                           │
                           ▼
                     ┌──────────┐
                     │   LLM    │
                     │ Classify │
                     └─────┬────┘
                           │
                  ┌────────┴────────┐
                  │                 │
                  ▼                 ▼
           ┌───────────┐     ┌──────────┐
           │  MySQL    │     │ ChromaDB │
           │ vs_news   │     │  (RAG)   │
           └─────┬─────┘     └──────────┘
                 │
                 ▼
        ┌─────────────────┐
        │ SectorAnalyst   │
        │   (for sector)  │
        └────────┬────────┘
                 │
                 ▼
           ┌──────────┐
           │   LLM    │
           │  Impact  │
           │ Analysis │
           └─────┬────┘
                 │
        ┌────────┴────────┐
        │                 │
        ▼                 ▼
  ┌──────────┐     ┌──────────┐
  │  Google  │     │  MySQL   │
  │  Sheets  │◄────│ vs_drivers│
  │ (update) │     │  (sync)  │
  └──────────┘     └──────────┘
```

---

### Valuation Flow (Daily)

```
┌───────────┐  ┌───────────┐  ┌───────────┐
│ Core CSV  │  │  Prices   │  │  Google   │
│(Financials│  │  (CMP)    │  │  Sheets   │
└─────┬─────┘  └─────┬─────┘  └─────┬─────┘
      │              │              │
      └──────────────┼──────────────┘
                     │
                     ▼
           ┌──────────────────┐
           │ FinancialProcessor│
           │  • TTM metrics   │
           │  • 3-tier fallback│
           └────────┬─────────┘
                    │
                    ▼
           ┌──────────────────┐
           │   ValuatorAgent  │
           │  Load drivers    │
           └────────┬─────────┘
                    │
                    ▼
              ┌──────────┐
              │   LLM    │
              │ Synthesize│
              │ adjustments│
              └─────┬────┘
                    │
          ┌─────────┴─────────┐
          │                   │
          ▼                   ▼
    ┌──────────┐        ┌──────────┐
    │ DCFModel │        │ Relative │
    │ Bull/Base│        │  Peer    │
    │ /Bear    │        │ Multiples│
    └─────┬────┘        └─────┬────┘
          │                   │
          │         ┌─────────┘
          │         │
          │         │    ┌──────────┐
          │         │    │ MonteCarlo│
          │         │    │ Simulation│
          │         │    └─────┬────┘
          │         │          │
          └─────────┼──────────┘
                    │
                    ▼
              ┌──────────┐
              │  BLEND   │
              │ 60/30/10 │
              └─────┬────┘
                    │
          ┌─────────┴─────────┐
          │                   │
          ▼                   ▼
    ┌──────────┐        ┌──────────┐
    │  MySQL   │        │  Alert?  │
    │vs_valuations│     │  (>5%)   │
    └──────────┘        └─────┬────┘
                              │
                              ▼
                        ┌──────────┐
                        │  Email   │
                        │  Sender  │
                        └──────────┘
```

---

## Configuration & Setup

### Environment Variables (.env)

```bash
# Database
MYSQL_HOST=localhost
MYSQL_PORT=3306
MYSQL_USER=root
MYSQL_PASSWORD=
MYSQL_DATABASE=rag

# ChromaDB
CHROMADB_HOST=localhost
CHROMADB_PORT=8001
CHROMADB_USER=jama_user
CHROMADB_COLLECTION=RAG_GPT

# Google Sheets
GSHEET_DRIVERS_ID=  # TODO: Create spreadsheet and add ID
GSHEET_SOCIAL_ID=   # Existing: 'storm posting' spreadsheet
GSHEET_AUTH_PATH=/Users/ram/code/rag/machai/RAGApp/auth.json

# LLM
GROK_API_KEY=your_key_here
OLLAMA_BASE_URL=http://localhost:11434
OPENAI_API_KEY=your_key_here

# Email
SMTP_HOST=smtp.gmail.com
SMTP_PORT=587
SMTP_USER=  # TODO: Add email
SMTP_PASSWORD=  # TODO: Add password
ALERT_EMAIL=medury@gmail.com

# Data Paths
CORE_CSV_PATH=/Users/ram/code/investment_strategies/data/core-input/core-all-input-2026-01-21 11-02-latest-final.csv
PRICE_CSV_PATH=/Users/ram/code/investment_strategies/data/prices/combined_monthly_prices.csv
MACRO_DATA_DIR=/Users/ram/code/investment_strategies/data/macro/

# System
LOG_DIR=/Users/ram/code/research/valuation_system/logs
LOG_LEVEL=DEBUG
ALERT_THRESHOLD_PCT=5.0
```

---

### Initial Setup

```bash
# 1. Install dependencies
cd /Users/ram/code/research/valuation_system
pip install -r requirements.txt

# 2. Initialize database tables
python -m valuation_system.scheduler.runner init
# Creates 14 MySQL tables in 'rag' database

# 3. Create Google Sheets (TODO)
# Manually create spreadsheet with 8 sheets
# Add ID to .env → GSHEET_DRIVERS_ID

# 4. Load launchd jobs
cp scheduler/*.plist ~/Library/LaunchAgents/
launchctl load ~/Library/LaunchAgents/com.valuation.*.plist

# 5. Verify setup
python -m valuation_system.scheduler.runner status

# 6. Run regression tests
python -m valuation_system.scheduler.runner test
# Should show 36/36 tests passing
```

---

## Operational Procedures

### Daily Operations

**Morning (08:00 IST)**:
1. Check email for daily digest from previous night
2. Review Google Sheet "posts" for social media drafts
3. Edit/approve tweets, set Approval="YES"
4. Run: `python /Users/ram/code/rag/machai/twitter/post_tweets_from_gsheet.py`

**Evening (20:30 IST)**:
1. Check email for valuation alerts
2. Review any >5% changes
3. Open Excel reports for detailed analysis (if needed)

**Weekly (Sunday)**:
1. Review weekly summary email
2. Check accuracy: past valuations vs actual price movement
3. Adjust drivers in Google Sheets if systematic bias detected

---

### Manual Valuation

```bash
# On-demand valuation for single company
python -m valuation_system.scheduler.runner valuation --symbol AETHER

# Full portfolio
python -m valuation_system.scheduler.runner portfolio

# Generate Excel report
python -m valuation_system.utils.generate_excel --symbol AETHER
```

---

### Troubleshooting

**If hourly job fails**:
```bash
# Check logs
tail -f /Users/ram/code/research/valuation_system/logs/hourly.log

# Run catchup manually
python -m valuation_system.scheduler.runner catchup

# Check system status
python -m valuation_system.scheduler.runner status
```

**If MySQL is down**:
- System will queue operations to JSON files
- After MySQL recovery, run: `python -m valuation_system.scheduler.runner catchup`
- Queued operations will replay automatically

**If Google Sheets API fails**:
- Drivers will use last known values from MySQL cache
- Updates will queue to JSON
- After recovery, Google Sheets will sync from MySQL

---

### Updating Drivers Manually

**To override LLM driver values**:
1. Open Google Sheet (GSHEET_DRIVERS_ID)
2. Navigate to appropriate sheet (Macro / Sector / Company)
3. Find driver row
4. Update "Current" column
5. System will sync to MySQL on next hourly run
6. Manual edits take priority over LLM updates

**Example**:
```
Sheet: "Sector - Specialty Chemicals"
Row: crude_oil
Column "Current": 85 → 90
Column "Source": "Manual override - EIA forecast"

Next valuation will use $90, not LLM's suggested value.
```

---

### Backup & Restore

**MySQL Backup** (weekly):
```bash
mysqldump -u root rag vs_* > backup_$(date +%Y%m%d).sql
```

**Google Sheets**: Auto-versioned by Google (File → Version history)

**Core CSV**: Keep dated versions, currently using 2026-01-21 snapshot

---

## Appendices

### A. Database Schema

**14 MySQL Tables in `rag` database**:

1. `vs_companies` - DEPRECATED (use mssdb.kbapp_marketscrip)
2. `vs_sectors` - Sector configurations
3. `vs_drivers` - All driver states (MACRO/SECTOR/COMPANY)
4. `vs_valuations` - Daily valuation results
5. `vs_dcf_details` - 10-year DCF projections
6. `vs_relative_valuations` - Peer analysis
7. `vs_monte_carlo_results` - Simulation distribution
8. `vs_peer_groups` - Peer selections (cached 30 days)
9. `vs_news` - News events metadata
10. `vs_event_timeline` - Timeline of events per company
11. `vs_alerts` - Valuation change alerts
12. `vs_run_state` - Scheduler state tracking
13. `vs_data_quality` - Data completeness metrics
14. `vs_audit_log` - Full audit trail

---

### B. Column Naming Conventions (Core CSV)

**CRITICAL**: Case sensitivity matters!

**Quarterly** (index-based):
```
sales_148, sales_149, sales_150, sales_151  # Q148 = FY2025Q4
pbidt_148, pbidt_149, pbidt_150, pbidt_151
```
Index formula: FY = 1989 + (idx-1)//4, Q = (idx-1)%4 + 1

**Annual** (year-prefixed):
```
2024_sales, 2024_debt, 2024_interest  # FY2024 = Apr 2023 - Mar 2024
2025_sales, 2025_debt, 2025_interest  # FY2025 = Apr 2024 - Mar 2025
```

**Half-yearly**:
```
h1_2025_cash_and_bank  # Apr-Sep 2024
h2_2025_acc_depr       # Oct 2024 - Mar 2025
h1_2026_cash_and_bank  # Apr-Sep 2025
h2_2026_acc_depr       # Oct 2025 - Mar 2026
```

**Case differences**:
- Yearly: `trade_payables` (lowercase), `acc_dep` (no 'r')
- Half-yearly: `Trade_payables` (Title case), `acc_depr` (with 'r')

---

### C. Regression Tests

**36 tests across 8 categories**:
1. Data Loading (5 tests)
2. Financial Processing (8 tests)
3. DCF Calculations (6 tests)
4. Driver Synthesis (4 tests)
5. Relative Valuation (4 tests)
6. Database Operations (4 tests)
7. Agent Coordination (3 tests)
8. Edge Cases (2 tests)

Run daily at 06:00 IST, results emailed to PM.

---

### D. Key Files Reference

| File | Purpose |
|------|---------|
| `agents/orchestrator.py` | Main coordinator |
| `agents/news_scanner.py` | News scraping + LLM classification |
| `agents/sector_analyst.py` | Driver impact analysis |
| `agents/valuator.py` | Valuation engine |
| `agents/content_agent.py` | Social media drafts |
| `models/financial_processor.py` | TTM metrics, 3-tier fallback |
| `models/dcf_model.py` | DCF valuation |
| `models/relative_valuation.py` | Peer multiples |
| `models/monte_carlo.py` | Scenario simulation |
| `storage/mysql_client.py` | MySQL wrapper |
| `storage/gsheet_client.py` | Google Sheets integration |
| `utils/llm_client.py` | LLM fallback chain |
| `utils/resilience.py` | Graceful degradation, retry logic |
| `scheduler/runner.py` | CLI entry points |
| `config/companies.yaml` | Company watchlist + alpha drivers |
| `config/sectors.yaml` | Sector drivers + terminal assumptions |
| `config/.env` | Environment variables |

---

---

## Growth Company Handling (Added 2026-02-06)

### Challenge: DCF Fails for High-Growth Companies

**Problem**: Traditional DCF uses 3-year historical averages for capex and working capital. For companies in aggressive expansion phase, this creates catastrophic errors:

```
Example: Aether Industries (159% YoY growth in FY2025)
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Historical Ratios (Expansion Phase):
• Capex/Sales: 46% (building new plants)
• NWC/Sales: 93% (inventory buildup for growth)
• Terminal Reinvest: 60% (recent high capex)

If projected forever:
• Every year consumes 46% of revenue in capex
• Every year ties up 93% of revenue in working capital
• Free Cash Flow = NEGATIVE in all years
• DCF Result = NEGATIVE ❌
```

---

### Solution: Auto-Detection + Normalization

**Implemented in**: `data/processors/financial_processor.py`

#### Detection Logic

```python
# Check BOTH CAGR and recent YoY growth
recent_cagr_3y = calculate_cagr(sales, years=3)
recent_yoy = (sales_latest / sales_previous) - 1

# High-growth if EITHER condition met:
is_high_growth = (recent_cagr_3y > 0.20) OR (recent_yoy > 0.50)
```

**Rationale**:
- CAGR can be distorted by one-time events (IPO, restructuring)
- Recent YoY captures current momentum
- Either signal indicates expansion phase

---

#### Normalization Rules

**Rule 1: Capex Normalization**
```python
if is_high_growth AND historical_capex > 15%:
    capex_normalized = 0.065  # 6.5% maintenance capex for chemicals
    # vs historical 46% (expansion + maintenance)

Logic: Separate expansion capex (temporary) from maintenance capex (permanent)
```

**Rule 2: NWC Normalization**
```python
if is_high_growth AND historical_nwc > 60%:
    nwc_normalized = 0.30  # 30% steady-state for chemicals
    # vs historical 93% (growth-phase buildup)

Logic: Rapid growth requires excess inventory/receivables. Will normalize as growth slows.
```

**Rule 3: Terminal Reinvestment Normalization**
```python
if is_high_growth AND historical_reinvest > 50%:
    terminal_reinvest = sector_config['reinvestment_rate']  # 30-35%
    # vs historical 60% (expansion capex / NOPAT)

Logic: Terminal value assumes steady-state, not expansion phase.
```

---

### Results: Before vs After

#### Aether Industries (High-Growth)

| Assumption | Before Fix | After Fix | Impact |
|------------|------------|-----------|--------|
| Capex/Sales | 46.0% | **6.5%** | +Rs 400 Cr FCF/year |
| NWC/Sales | 92.6% | **30.0%** | +Rs 100 Cr FCF/year |
| Terminal Reinvest | 60.0% | **35.0%** | Terminal value now positive |
| **DCF Base** | **NEGATIVE** ❌ | **Rs 209.64** ✅ | **Fixed!** |
| **Blended** | Rs 470 (Relative only) | **Rs 287.63** (All 3 methods) | ✅ **Reliable** |

#### Eicher Motors (Mature) - No Normalization

| Assumption | Historical | Used | Notes |
|------------|-----------|------|-------|
| Capex/Sales | 4.9% | **4.9%** | Normal, no adjustment |
| NWC/Sales | **-1.7%** | **-1.7%** | Negative WC (collects before paying) ✓ |
| Terminal Reinvest | 19.5% | **19.5%** | Normal, no adjustment |
| **DCF Base** | **Rs 2,789.50** ✅ | Already worked (mature company) | ✅ |

---

### Key Learnings

**1. Not All Companies Are Created Equal**:
- High-growth companies need different treatment than mature ones
- Blind application of historical averages fails for expansion phase
- Auto-detection prevents manual intervention

**2. Expansion vs Maintenance Capex**:
- Expansion capex is temporary (building capacity for future)
- Maintenance capex is permanent (sustaining current operations)
- DCF terminal value should use maintenance capex only

**3. Working Capital Phases**:
- Growth phase: High NWC (stocking up, extending credit to gain share)
- Steady state: Normal NWC (stable turnover ratios)
- Mature/negative NWC: Collect before paying (Eicher example)

**4. Industry-Specific Norms**:
| Industry | Maintenance Capex | Steady-State NWC | Terminal Reinvest |
|----------|-------------------|------------------|-------------------|
| Specialty Chemicals | 5-8% | 25-35% | 30-35% |
| Automobiles | 4-6% | 10-20% (can be negative) | 20-30% |
| Asset-light (IT, FMCG) | 2-4% | 10-15% | 15-25% |
| Capital-intensive (Infra) | 8-12% | 20-30% | 40-50% |

---

### Validation Metrics

**Healthy DCF Should Have**:
- ✅ Positive FCFF in Year 1
- ✅ Terminal Value represents 60-80% of Enterprise Value
- ✅ Terminal growth < WACC (typically 2-5%)
- ✅ Implied PE in reasonable range (10-50x depending on growth)

**Red Flags**:
- ❌ Negative FCF in all projected years
- ❌ Terminal Value = 0% of EV
- ❌ Terminal growth > WACC
- ❌ Implied PE < 5x or > 100x

---

## Version History

| Version | Date | Changes |
|---------|------|---------|
| 1.0 | 2026-02-06 | Initial documentation |
| 1.1 | 2026-02-06 | Added Growth Company Handling section, validated with Aether + Eicher |

---

**END OF DOCUMENT**
