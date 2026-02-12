# CPI Base Year Transition: 2012 → 2024

**Announcement Date:** February 12, 2026, 4:30 PM IST
**Effective From:** January 2024 (retroactive)
**Last Release (Old Series):** December 2025

---

## Summary of Changes

India's Ministry of Statistics & Programme Implementation (MoSPI) released a new Consumer Price Index series with base year 2024, replacing the 2012-base series that had been in use for 12+ years.

### Key Methodology Changes

| Aspect | Old (2012-base) | New (2024-base) | Change |
|--------|-----------------|-----------------|--------|
| **Base Year** | 2012=100 | 2024=100 | Updated |
| **Survey** | HCES 2011-12 | HCES 2023-24 | 12 years newer |
| **Items Tracked** | 299 items | 358 items | +59 items (+20%) |
| **Goods** | 259 | 308 | +49 |
| **Services** | 40 | 50 | +10 |
| **Framework** | 6 Groups | 12 Divisions (COICOP 2018) | International standard |
| **Food Weight** | 45.86% | 36.75% | -9.11pp |

### New Items Added (2024 Series)

**Technology & Digital:**
- Streaming services (Netflix, Prime Video, Disney+)
- Cloud storage subscriptions
- Pen drives and external storage
- Smart home devices
- Electric vehicle charging

**Modern Services:**
- App-based cab services (Uber, Ola)
- Food delivery (Swiggy, Zomato)
- Babysitting and childcare services
- Co-working space rentals

**Updated Food Categories:**
- Value-added dairy products (flavored milk, protein drinks)
- Organic produce
- Ready-to-eat meals
- Rural housing (added for rural CPI)

### Removed Items (Obsolete)

- VCRs and VCDs
- DVD players and discs
- Cassette tapes and players
- Fax machines
- Landline telephone handsets
- Pagers

---

## Impact on Inflation Measurement

### Lower Food Weights

The reduction in food weight from 45.86% to 36.75% reflects:
1. **Rising incomes** - Food share falls as households become wealthier (Engel's Law)
2. **Urbanization** - Urban households spend less on food proportionally
3. **Structural shift** - More spending on services, technology, healthcare

**Impact:** Food price shocks will have LESS impact on headline inflation going forward.

### First Month Comparison (Jan 2026)

| Metric | New Series (2024-base) | Estimated Old Series* |
|--------|------------------------|----------------------|
| **Headline CPI** | 2.75% | ~4.0-4.5% |
| **Food** | 2.13% | ~3.5% |
| **Rural** | 2.73% | ~4.2% |
| **Urban** | 2.77% | ~4.0% |

*Old series estimate based on Dec 2025 trend (1.33%) + typical Jan seasonal uptick

**Observation:** New series shows ~1.5-2.0pp LOWER inflation than old methodology would have shown.

---

## Implications for Valuation System

### 1. **WACC Calculations**

**Risk-Free Rate Component:**
- Old assumption: RBI targets 4% inflation (2012-base)
- New reality: RBI will recalibrate target for 2024-base (likely 3-3.5%)
- **Impact:** Lower inflation → lower nominal risk-free rate → lower WACC

### 2. **Real Growth Adjustments**

**Formula:** Real Growth = Nominal Growth - Inflation

- Old series: 10% revenue growth - 4.5% inflation = 5.5% real growth
- New series: 10% revenue growth - 2.75% inflation = 7.25% real growth

**Impact:** Companies' real growth rates will appear HIGHER under new series.

### 3. **Terminal Growth Rates**

Currently using: `g = Reinvestment Rate × ROCE`, capped at 2-5%

- Old environment: 4-5% nominal terminal growth (3% real + 2% inflation)
- New environment: 5-6% nominal terminal growth (3% real + 2.75% inflation)

**Action:** Review terminal growth caps in `dcf_model.py` - current 5% cap may be too conservative.

### 4. **Sector Impacts**

**Winners (Lower food weight benefits):**
- FMCG companies facing input cost inflation
- Food processors with pricing power
- Agricultural equipment manufacturers

**Losers (Service weight increase):**
- Telecom (streaming displacing cable)
- Traditional retail (e-commerce taking share)
- Cinema chains (OTT competition)

### 5. **Macro Driver Weights**

Current system:
- 23 MACRO drivers (20% of total driver weight)
- CPI-based drivers: `inflation_headline`, `inflation_food`, `inflation_core`

**Action Required:**
1. Recalibrate historical CPI driver values (2012-base → 2024-base)
2. Update inflation alert thresholds (old: >6% warning, new: >4% warning)
3. Adjust sector-specific inflation mappings in `populate_drivers.py`

---

## Data Transition Plan

### Phase 1: API Monitoring (Current)

**Status:** ✅ Code updated, waiting for API data

**Actions:**
- [x] Updated `mospi_scraper.py` to support both base years
- [x] Created `check_new_cpi_series.py` monitoring script
- [ ] Run daily checks until new data appears (ETA: Feb 14-19, 2026)

**Command:**
```bash
python scripts/check_new_cpi_series.py
```

### Phase 2: Historical Backfill (When API Available)

**Goal:** Get 2024-base data for Jan 2024 - Jan 2026 (24 months)

**Actions:**
1. Fetch full 2024-base series from API
2. Verify continuity with old series (overlapping period: Jan 2024 - Dec 2025)
3. Document any index level adjustments

**Command:**
```bash
python scripts/update_macro_data.py --source mospi
```

### Phase 3: Valuation System Integration (Week of Feb 17, 2026)

**Files to Update:**

1. **`valuation_system/utils/populate_drivers.py`**
   - Line ~1200: Update inflation driver calculation
   - Use 2024-base for dates >= Jan 2024
   - Keep 2012-base for historical (pre-Jan 2024)

2. **`valuation_system/agents/dcf_model.py`**
   - Review terminal growth caps (currently 2-5%)
   - Consider raising to 2.5-6% for new lower-inflation environment

3. **`valuation_system/config/macro_metadata.csv`**
   - Add column: `cpi_base_year` (2012 or 2024)
   - Update driver descriptions

4. **Database: `vs_drivers` table**
   - Add new records for 2024-base inflation drivers
   - Mark old drivers as `series=2012_base`, new as `series=2024_base`

### Phase 4: Alert Recalibration (Week of Feb 24, 2026)

**Materiality Alert Thresholds:**

Old (2012-base) → New (2024-base):
- Critical inflation: >6% → >4%
- Warning inflation: >5% → >3.5%
- Deflationary risk: <2% → <1.5%

**Command:**
```bash
# Re-run historical materiality checks with new thresholds
python -m valuation_system.agents.orchestrator --mode=materiality_check --start_date=2024-01-01
```

---

## Data Sources & Documentation

### Official MOSPI Releases

1. **Press Conference:** Feb 12, 2026, 4:30 PM IST
   - Venue: Dr. Ambedkar International Centre, New Delhi

2. **Expert Group Report:**
   - "Comprehensive Updation of Consumer Price Index"
   - Available: https://www.mospi.gov.in/uploads/Marquee/doc-0aee0026-bf32-4213-ae4f-c5308088444e.pdf

3. **Last Old Series Release:**
   - CPI December 2025 (2012-base)
   - Press Release: https://www.pib.gov.in/PressReleasePage.aspx?PRID=2213736

### API Endpoints

**Old Series (2012-base):**
```
GET https://api.mospi.gov.in/api/cpi/getCPIIndex
?base_year=2012&year=2025&month_code=12&state_code=99&sector_code=3
```

**New Series (2024-base):**
```
GET https://api.mospi.gov.in/api/cpi/getCPIIndex
?base_year=2024&year=2026&month_code=1&state_code=99&sector_code=3
```

**Status (as of Feb 12, 2026 6:30 PM):**
- 2012-base: ✅ Available through Dec 2025
- 2024-base: ❌ Not yet in API (0 records)

---

## FAQs

### Q: Can we compare 2012-base and 2024-base inflation rates directly?

**A:** No. The index values are on different scales (2012=100 vs 2024=100). However, percentage changes (inflation rates) can be conceptually compared, though methodology differences mean they measure slightly different consumption baskets.

### Q: What happens to our historical valuations (2020-2023)?

**A:** Keep them as-is. They used the correct CPI series available at that time (2012-base). Only forward valuations (Feb 2026+) should use the new series.

### Q: Should we revalue all companies with the new inflation data?

**A:** No immediate revaluation needed. The system will naturally use new data for:
1. New valuations (companies being valued for first time)
2. Scheduled monthly revaluations
3. Material event-triggered revaluations

Historical valuations remain valid - they used the best available data at that time.

### Q: Will RBI change its inflation target from 4%?

**A:** Likely yes, but not immediately. RBI's Monetary Policy Committee typically reviews the target during the annual policy review (April/May). Expect target to be lowered to 3-3.5% by mid-2026.

### Q: Why is food weight lower in the new series?

**A:** The Household Consumption Expenditure Survey (HCES) 2023-24 found that Indian households are spending a smaller share of their budget on food compared to 2011-12. This reflects:
- Rising per capita incomes (food share falls as income rises)
- Faster growth in non-food spending (healthcare, education, streaming, travel)
- Urbanization (urban households have lower food share)

This is a structural change, not a statistical artifact.

---

## Monitoring Checklist

**Daily (until new data available):**
- [ ] Run `check_new_cpi_series.py`
- [ ] Check MOSPI Twitter/X: @GoIStats
- [ ] Monitor PIB press releases: https://www.pib.gov.in

**Upon New Data Availability:**
- [ ] Fetch full 2024-base series (Jan 2024 - Jan 2026)
- [ ] Verify data quality (no gaps, reasonable index values)
- [ ] Update `vs_drivers` table with new inflation records
- [ ] Recalibrate alert thresholds
- [ ] Document any anomalies or adjustments

**Monthly (ongoing):**
- [ ] Auto-fetch latest CPI from API (2024-base for new months)
- [ ] Cross-check with PIB press releases
- [ ] Monitor for any MOSPI methodology clarifications

---

**Last Updated:** Feb 12, 2026
**Next Review:** Feb 19, 2026 (or when API data becomes available)
