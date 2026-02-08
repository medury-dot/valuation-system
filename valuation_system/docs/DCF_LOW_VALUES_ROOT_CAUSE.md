# DCF Low Values - Root Cause Analysis

**Date**: February 7, 2026
**Status**: 🔴 CRITICAL BUG FOUND
**Impact**: All DCF valuations showing 80-88% downside

---

## Executive Summary

All 3 jewel portfolio valuations showed severe undervaluation:
- BEL: ₹87 vs market ₹439 (-80%)
- KEI: ₹503 vs market ₹4,367 (-88%)
- ACUTAAS: ₹245 vs market ₹1,970 (-88%)

**Root Cause Found**: Working Capital calculation is fundamentally flawed.

---

## Root Cause: NWC Calculation Error

### The Bug

**Current Formula** (in `financial_processor.py`):
```python
NWC = Inventories + Sundry Debtors - Trade Payables
```

**Problem**: This formula **IGNORES** other current liabilities, which for many companies (especially defense, construction, advance-collecting businesses) are the LARGEST component of current liabilities!

###Case Study: BEL (Bharat Electronics)

**Current (Wrong) Calculation**:
```
Inventories:        Rs 10,211 Cr
+ Sundry Debtors:   Rs 10,591 Cr
- Trade Payables:   Rs  2,908 Cr
= Net WC:           Rs 17,894 Cr (67.4% of revenue!)
```

**Correct Calculation** (including ALL current liabilities):
```
Inventories:              Rs 10,211 Cr
+ Sundry Debtors:         Rs 10,591 Cr
- Trade Payables:         Rs  2,908 Cr
- Other Current Liabilities: Rs 19,810 Cr  <-- MISSING!
= Net WC:                 Rs -1,916 Cr (-7.2% of revenue)
```

**Impact on DCF**:
- Wrong NWC/Sales: **+67.4%** (cash is tied up in working capital growth)
- Correct NWC/Sales: **-7.2%** (cash is released from working capital!)
- **Difference: 74.6 percentage points!**

### What Are "Other Current Liabilities"?

For defense contractors like BEL, this Rs 19,810 Cr includes:
1. **Customer Advances** (30-40% of contract value paid upfront by government)
2. **Unearned Revenue** (deferred revenue from long-term contracts)
3. **Provisions** (warranty, employee benefits, etc.)
4. **Other Payables** (non-trade creditors)

These are **operating liabilities** that grow with revenue and should be subtracted from working capital!

---

## Impact on Free Cash Flow

### Year 5 Projection for BEL (Current vs Correct)

**Current (Wrong)**:
```
Revenue Y5:          Rs 43,162 Cr
Revenue Growth Y4→Y5: Rs  2,443 Cr
ΔWorking Capital:    Rs  1,424 Cr  (= 2,443 × 58.3%)  <-- HUGE DRAG
FCFF Y5:             Rs  7,075 Cr  (only 16.4% of revenue)
```

**Corrected** (with -7.2% NWC/Sales):
```
Revenue Y5:          Rs 43,162 Cr
Revenue Growth Y4→Y5: Rs  2,443 Cr
ΔWorking Capital:    Rs   -176 Cr  (= 2,443 × -7.2%)  <-- CASH RELEASE!
FCFF Y5:             Rs  8,675 Cr  (20.1% of revenue)
```

**Impact on Intrinsic Value**:
- Current (wrong): **₹87.40**
- Corrected (estimated): **₹150-180** (rough estimate, needs full recalculation)
- Market price: **₹438.95**

*Note: Still below market, but much more reasonable. Market may be pricing in growth beyond base case.*

---

## Why This Bug Exists

### Historical Context

The financial_processor was likely designed using the **classic textbook formula**:
```
NWC = Current Assets - Current Liabilities
    = (Cash + AR + Inventory) - (AP + Other CL)
```

But the implementation used a **simplified formula**:
```
NWC = (Inventory + AR) - AP
```

This works fine for **normal manufacturing/trading companies** where:
- AP is the dominant current liability (~70-80% of CL)
- Other CL are small (~20-30% of CL)

But it **fails catastrophically** for:
1. **Defense contractors** (customer advances >> trade payables)
2. **Construction companies** (project advances)
3. **Software/SaaS companies** (deferred revenue)
4. **Consumer durable companies** (dealer advances)

### Why It Wasn't Caught in Pilot

The 2 pilot companies were:
1. **Aether Industries** (specialty chemicals) - normal manufacturing, lower advances
2. **Eicher Motors** (automobiles) - dealer advances exist but may be smaller relative to AP

The bug's impact was likely smaller for these companies, so it didn't trigger alarm bells.

---

## Data Availability Check

### Can We Fix This with Available Data?

**Half-yearly data** (14 metrics available):
- h1_2026_inventories ✅
- h1_2026_sundrydebtors ✅
- h1_2026_Trade_payables ✅
- **h1_2026_tot_liab** ✅ (Total Liabilities)
- **h1_2026_LT_borrow** ✅ (Long-term Borrowings)

**Calculation**:
```python
# Current Assets (operating)
current_assets_operating = inventories + sundry_debtors

# Current Liabilities (ALL)
current_liabilities = tot_liab - LT_borrow

# Net Working Capital
nwc = current_assets_operating - current_liabilities
```

**Yearly data** (actuals available):
- 2025_inventories ✅
- 2025_sundrydebtors ✅
- 2025_trade_payables ✅
- **2025_tot_liab** ✅
- **2025_LT_borrow** ✅ (if available)

**Verdict**: ✅ **YES, we have the data to fix this!**

---

## Proposed Fix

### Location
`/Users/ram/code/research/valuation_system/data/processors/financial_processor.py`

### Current Code (Lines ~250-280, estimated)
```python
def _calculate_nwc_metrics(self, company_data: pd.Series, revenue: float) -> dict:
    """Calculate working capital metrics."""

    # Get 3-year average
    nwc_values = []
    for year in [2023, 2024, 2025]:
        inv = company_data.get(f'{year}_inventories', 0)
        debtors = company_data.get(f'{year}_sundrydebtors', 0)
        payables = company_data.get(f'{year}_trade_payables', 0)  # BUG: Only trade payables!

        nwc = (inv + debtors - payables)
        nwc_values.append(nwc)

    avg_nwc = np.mean([v for v in nwc_values if v != 0])
    nwc_to_sales = avg_nwc / revenue if revenue > 0 else 0.15

    return {'nwc_to_sales': nwc_to_sales}
```

### Fixed Code
```python
def _calculate_nwc_metrics(self, company_data: pd.Series, revenue: float) -> dict:
    """Calculate working capital metrics.

    Net Working Capital = (Inventory + Receivables) - ALL Current Liabilities

    Where Current Liabilities = Total Liabilities - Long-term Borrowings
    This captures trade payables, customer advances, provisions, other CL.
    """

    # Try half-yearly first (more recent)
    for half_year in ['h1_2026', 'h2_2025', 'h1_2025']:
        inv = company_data.get(f'{half_year}_inventories', 0)
        debtors = company_data.get(f'{half_year}_sundrydebtors', 0)
        tot_liab = company_data.get(f'{half_year}_tot_liab', 0)
        lt_borrow = company_data.get(f'{half_year}_LT_borrow', 0)

        if inv > 0 and debtors > 0 and tot_liab > 0:
            current_liabilities = tot_liab - lt_borrow
            nwc = (inv + debtors) - current_liabilities
            nwc_to_sales = nwc / revenue if revenue > 0 else 0.15

            logger.info(f"  NWC/Sales: {nwc_to_sales:.4f} [ACTUAL: (inv+debtors-all_CL)/sales from {half_year}]")
            logger.info(f"    Operating CA: Rs {inv + debtors:,.0f} Cr")
            logger.info(f"    Current Liab: Rs {current_liabilities:,.0f} Cr")
            logger.info(f"    Net WC:       Rs {nwc:,.0f} Cr")

            return {'nwc_to_sales': nwc_to_sales}

    # Fallback to yearly data (3-year average)
    nwc_values = []
    for year in [2023, 2024, 2025]:
        inv = company_data.get(f'{year}_inventories', 0)
        debtors = company_data.get(f'{year}_sundrydebtors', 0)
        tot_liab = company_data.get(f'{year}_tot_liab', 0)

        # Try to get LT borrowings (may not exist for all years)
        lt_borrow = company_data.get(f'{year}_LT_borrow', 0)
        if lt_borrow == 0:
            # If not available, estimate as debt (if available)
            lt_borrow = company_data.get(f'{year}_debt', 0)

        if inv > 0 and debtors > 0 and tot_liab > 0:
            current_liabilities = tot_liab - lt_borrow
            nwc = (inv + debtors) - current_liabilities
            nwc_values.append(nwc)

    if nwc_values:
        avg_nwc = np.mean(nwc_values)
        nwc_to_sales = avg_nwc / revenue if revenue > 0 else 0.15

        logger.info(f"  NWC/Sales: {nwc_to_sales:.4f} [ACTUAL: 3-year avg (inv+debtors-all_CL)/sales]")
        return {'nwc_to_sales': nwc_to_sales}

    # Last resort: use default
    logger.warning(f"  NWC/Sales: 0.1500 [DEFAULT: insufficient data for NWC calculation]")
    return {'nwc_to_sales': 0.15}
```

---

## Expected Impact After Fix

### BEL (Bharat Electronics)

**Before Fix**:
- NWC/Sales: +67.4%
- FCFF Y5: Rs 7,075 Cr
- Intrinsic Value: ₹87.40
- vs Market (₹438.95): -80.1%

**After Fix** (estimated):
- NWC/Sales: -7.2%
- FCFF Y5: Rs 8,675 Cr (+22.6%)
- Intrinsic Value: ~₹150-180 (rough estimate)
- vs Market (₹438.95): -60% to -66%

Still below market, but:
1. ✅ More reasonable valuation
2. ✅ Reflects true cash flow dynamics
3. ✅ Market premium can be explained by growth expectations, defense policy tailwinds, quality premium

### KEI Industries

**Before Fix**:
- NWC/Sales: +25.4%
- Intrinsic: ₹502.90
- vs Market (₹4,367): -88.5%

**After Fix** (estimated):
- NWC/Sales: likely +10-15% (still positive, but lower)
- Intrinsic: ~₹700-900 (rough estimate)
- vs Market: Still materially overvalued (~-80%)

KEI has seen a massive rally (8-9x intrinsic value) suggesting momentum-driven pricing.

### ACUTAAS Chemicals

**Before Fix**:
- NWC/Sales: +30.9%
- Intrinsic: ₹245.09
- vs Market (₹1,970): -87.6%

**After Fix** (estimated):
- NWC/Sales: likely +20-25% (specialty chemicals need working capital)
- Intrinsic: ~₹350-450 (rough estimate)
- vs Market: Still overvalued (~-75% to -80%)

Small-cap specialty chemical in high-growth phase, market pricing in sustained growth premium.

---

## Implementation Plan

### Phase 1: Fix NWC Calculation (HIGH PRIORITY) ⚡

**File**: `data/processors/financial_processor.py`

**Steps**:
1. Locate `_calculate_nwc_metrics()` method
2. Replace with fixed version above
3. Add extensive logging to show components
4. Handle missing data gracefully (fallbacks)

**Testing**:
```bash
# Rerun BEL valuation
python3 utils/run_valuation.py --symbol BEL

# Expected log output:
#   NWC/Sales: -0.0722 [ACTUAL: (inv+debtors-all_CL)/sales from h1_2026]
#     Operating CA: Rs 20,802 Cr
#     Current Liab: Rs 22,718 Cr
#     Net WC:       Rs -1,916 Cr

# Expected result:
#   Intrinsic Value: ₹150-180 (vs ₹87.40 before fix)
```

### Phase 2: Validate Across Portfolio (CRITICAL)

**Run for all 883 companies**:
```bash
# Create validation script
for symbol in BEL KEI ACUTAAS AETHER EICHERMOT; do
    echo "Validating $symbol..."
    python3 utils/run_valuation.py --symbol $symbol
done

# Check for:
# - Negative NWC/Sales (should be common for advance-collecting businesses)
# - Extreme values (>100% or <-50%)
# - Companies with missing tot_liab data
```

### Phase 3: Backtest Against Historical Prices

**Retroactive validation**:
1. Run valuations using historical data (FY2023, FY2024, FY2025)
2. Compare intrinsic values vs actual market prices
3. Calculate hit rate: % of companies where intrinsic was within ±50% of market
4. Expected improvement: 20-30% → 50-60% hit rate

### Phase 4: Document and Update Memory

**Files to update**:
1. `MEMORY.md` - Add lesson learned about NWC calculation
2. `IMPLEMENTATION_SUMMARY.md` - Add bug fix notes
3. `CLAUDE.md` - Add data guideline about current liabilities

---

## Lessons Learned

### For Future Projects

1. **Never use simplified formulas** without validating against edge cases
   - Textbook formulas exist for a reason
   - "Quick approximations" can have 100x errors

2. **Test on diverse company types early**
   - Manufacturing (normal working capital)
   - Services (low working capital)
   - Defense/Construction (negative working capital)
   - SaaS (deferred revenue heavy)

3. **Log intermediate calculations extensively**
   - Every component of NWC should be visible
   - Source tags: [ACTUAL]/[DERIVED]/[DEFAULT]
   - Units and scale clearly marked

4. **Sanity check ratios against industry norms**
   - NWC/Sales for defense: typically -10% to +10%
   - NWC/Sales for manufacturing: +15% to +25%
   - NWC/Sales for SaaS: -20% to -5%

5. **When DCF values are systematically low, debug working capital first**
   - ΔWC is often the #1 killer of FCFF
   - Mistakes here compound over 5-10 years
   - Can cause 2-5x valuation errors

---

## Additional Investigation Needed

### 1. Debt Calculation

Check if `net_debt` calculation also has issues:
```python
# Current:
net_debt = debt - cash

# Should it be?
net_debt = (ST_debt + LT_borrow) - (cash + marketable_securities)
```

### 2. Capex Normalization

For high-growth companies (KEI, ACUTAAS), check if capex is being normalized correctly:
- Expansion capex vs maintenance capex
- Should terminal reinvestment rate use maintenance capex only?

### 3. WACC Assumptions

India equity risk premium of 7.08% seems high:
- Damodaran uses mature market ERP (5-5.5%) + country risk premium (1.5-2%)
- But for large-cap PSUs (BEL), country risk may be overstated
- Consider sector-specific ERPs

### 4. Terminal Growth Rate

Current: g = Terminal ROCE × Terminal Reinvestment Rate

Alternative: Consider GDP growth + inflation as floor:
- India nominal GDP growth: ~10-11% (7% real + 4% inflation)
- Terminal growth capped at 5% may be too conservative
- Consider 6-7% terminal growth for high-quality companies

---

## Priority Actions

### Immediate (Today)
1. ✅ Root cause identified
2. ⏳ Fix NWC calculation in financial_processor.py
3. ⏳ Rerun BEL, KEI, ACUTAAS valuations
4. ⏳ Validate results (expect ₹150-180 for BEL)

### Next Week
1. Run regression tests on all 883 companies
2. Compare NWC/Sales before vs after fix
3. Identify outliers (extreme NWC values)
4. Update MEMORY.md with lessons learned

### Next Month
1. Backtest historical valuations (FY2023-2025)
2. Calculate hit rate improvement
3. Tune other DCF parameters (WACC, terminal growth)
4. Add relative valuation for triangulation

---

**Status**: 🔴 Bug identified, fix ready to implement
**Impact**: CRITICAL - affects all 883 company valuations
**Next Step**: Implement NWC fix in financial_processor.py

---

*Analysis completed: February 7, 2026 18:33 IST*
