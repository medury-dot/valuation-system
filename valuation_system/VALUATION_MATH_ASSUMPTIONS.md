# Valuation System — Math, Assumptions & Calculations

All calculations use **only real data from core CSV columns**. No synthetic data, no fabricated multipliers.

---

## 1. Revenue Growth Trajectory

### Data Source
- Quarterly-derived annual sales (`sales_annual` from `_annualize_quarterly(sales_qtr)`)
- NOT `<year>_sales` columns (unreliable for ~818 companies)

### CAGR Calculation (`core_loader.py: calculate_cagr`)
```
CAGR = (end_value / start_value) ^ (1 / n_years) - 1
```
- Tries requested span (5yr, 3yr) first
- Falls back to whatever years are available (minimum 2 data points)
- Example: VBL has only FY2024, FY2025 → uses 1yr span automatically

### Growth Trajectory (`financial_processor.py: _build_growth_trajectory`)

**Priority chain:**
1. **CAGR available**: Blend 3Y CAGR (60%) + 5Y CAGR (40%) → starting growth, linear decay to 6% by Year 5
2. **YoY fallback** (when no CAGR): Use most recent 2 years' YoY growth, dampened by 20%
   - `starting_growth = max(5%, min(YoY * 0.80, 30%))`
   - Linear decay to 6% terminal zone over 5 years
3. **Default**: [10%, 9%, 8%, 7%, 6%] — only if no sales data at all

**Cap/floor**: Year 1 growth capped at 30%, floored at 3%.

---

## 2. EBITDA Margin

### Data Source
- `pbidt_annual` / `sales_annual` (both quarterly-derived)
- Average of last 3 fiscal years

### Formula
```
EBITDA_margin = mean(PBIDT[year] / Sales[year]) for last 3 years
```

### Margin Improvement
- Linear regression on `ebidtm` (EBITDA margin %) series
- Capped at +/- 2% per year
- Default: 0.0 (no improvement assumed)

---

## 3. Capex / Sales

### Data Source
- `pur_of_fixed_assets` (annual, from cash flow statement — actual CSV column)
- `sales_annual` (quarterly-derived)

### Calculation
Per-year ratio: `abs(pur_of_fixed_assets[year]) / sales_annual[year]`

### Trend-Aware Weighting
If capex/sales is **declining** (latest year is 25%+ lower than oldest):
```
If 3 years: weighted = latest * 0.60 + middle * 0.25 + oldest * 0.15
If 2 years: weighted = latest * 0.70 + older * 0.30
```
Rationale: Companies exiting expansion phase (e.g., VBL normalizing from 26% to 9%) should not carry forward peak capex. Weights the trajectory rather than blind averaging.

If **stable** (not declining >25%): Simple average of last 3 years.

### High-Growth Normalization
If company is high-growth (3Y CAGR > 20% OR recent YoY > 50%) AND capex/sales > 12%:
- Force to maintenance capex of 6.5%
- Rationale: Expansion capex is temporary; DCF should use steady-state

### Fallback Chain
1. [ACTUAL] `pur_of_fixed_assets / sales` (yearly)
2. [DERIVED] `Δ(gross_block + CWIP) / sales` (yearly)
3. [DERIVED] Annualized quarterly capex / sales
4. [DEFAULT] 8%

---

## 4. Depreciation / Sales

### Data Source
- `acc_dep_yearly` — accumulated depreciation (actual CSV column, point-in-time)
- Annual depreciation charge = `acc_dep[year] - acc_dep[year-1]`

### Trend-Aware Weighting
Same logic as capex/sales. If declining trend (latest 25%+ below oldest):
```
If 3 years: weighted = latest * 0.60 + middle * 0.25 + oldest * 0.15
If 2 years: weighted = latest * 0.70 + older * 0.30
```
Rationale: Post-expansion, depreciation as % of sales declines as the asset base depreciates while revenue grows faster.

### Fallback Chain
1. [ACTUAL] `Δ(acc_dep_yearly)` — annual change in accumulated depreciation
2. [ACTUAL] `Δ(accumulated_depreciation)` — alternate key
3. [DERIVED] `Δ(gross_block - net_block)` — implied accumulated depreciation
4. [DEFAULT] 4%

---

## 5. Net Working Capital / Sales

### Formula
```
NWC = (Inventories + Sundry Debtors) - Current Liabilities
Current Liabilities = Total Liabilities - Long-term Borrowings
```

### Why `(Tot Liab - LT Borrow)` not just `Trade Payables`?
Current liabilities include: trade payables, customer advances, provisions, other CL.
Using only trade payables **understates** current liabilities, causing:
- Overstatement of NWC
- Overstatement of working capital investment in DCF
- Systematic undervaluation (30-50% for companies with large customer advances)

Companies with **negative NWC** (customer advances): defense (BEL: -7%), auto OEMs (Eicher: -99%), FMCG (ITC: -55%), pharma exporters (SunPharma: -96%).

### Data Sources (preference order)
1. [ACTUAL-HALFYEARLY] Half-yearly inventories, debtors, tot_liab, LT_borrow + TTM sales
2. [ACTUAL-ANNUAL] Annual inventories, debtors, tot_liab, LT_borrow / sales (3yr avg)
3. [FALLBACK] Annual inventories + debtors - trade_payables only (old method, warns)
4. [DERIVED] Cash conversion cycle / 365
5. [DERIVED] Individual days ratios (inv_days + recv_days - paybl_days) / 365
6. [DEFAULT] 15%

---

## 6. Tax Rate

### Formula
```
Effective Tax Rate = 1 - (PAT / PBT excl exceptional items)
```

### Data Sources (preference order)
1. [ACTUAL] `pat_annual / pbt_excp_yearly` — both quarterly-derived, avg last 3 years
2. [DERIVED] `pat_annual / pbt_excl_exceptional` — quarterly annualized PBT
3. [DERIVED] `pat / (PBIDT - Interest - Depreciation)` — estimated PBT
4. [DEFAULT] 25%

### Filters
- Only accepts rates in range (0%, 50%) — rejects anomalous years
- Uses average of up to 3 years to smooth one-off items

---

## 7. Cash & Equivalents

### Principle: Only real CSV data, no fabricated multipliers

### Data Sources (preference order)
1. [ACTUAL] `cash_and_bank` from half-yearly data (e.g., `h1_2026_cash_and_bank`)
   - Available for companies with half-yearly filings in core CSV
2. [DERIVED] Balance sheet residual using actual CSV columns:
   ```
   Cash ≈ (Networth + Debt + Trade Payables) - (Net Block + CWIP + Inventories + Debtors)
   ```
   - From balance sheet identity: Assets = Liabilities + Equity
   - Assumption: Other Liabilities ≈ Other Non-Cash Assets (transparent, logged)
   - All values from actual core CSV columns — no arbitrary multipliers
   - Only used if result > 0; otherwise defaults to 0
3. [DEFAULT] Rs 0 with warning logged

---

## 8. Cost of Debt (Pre-tax)

### Formula
```
Cost of Debt = Interest Expense / Total Debt
```

### Data Sources
1. [ACTUAL] `interest_yearly / debt` — avg last 3 years
2. [DERIVED] Annualized quarterly interest / debt
3. [DEFAULT] 9%

### Filter: Only accepts rates in range (1%, 25%)

---

## 9. WACC

### Formula
```
WACC = Ke * (E/(D+E)) + Kd * (1-t) * (D/(D+E))
```

Where:
- `Ke = Risk-free rate + Beta * Equity Risk Premium`
- Risk-free rate: India 10Y government bond (from Damodaran, ~6.7%)
- ERP: Country equity risk premium (from Damodaran, ~7.1%)
- Beta: Sector-level levered beta (from Damodaran)
- `Kd`: Pre-tax cost of debt (from interest/debt)
- `t`: Effective tax rate
- `D/(D+E)`: Market-value debt ratio (debt / (debt + market_cap))

---

## 10. Terminal Value

### Formula (Perpetuity Growth)
```
Terminal Growth (g) = Reinvestment Rate * Terminal ROCE
g = capped at min(5%, max(2%, computed_g))
Terminal Value = FCFF_last * (1 + g) / (WACC - g)
```

Safety: If WACC <= g, growth is adjusted down to WACC - 1%

### Terminal ROCE
- Blend: 60% historical average ROCE (5yr) + 40% sector convergence rate
- Bounded: [8%, 30%]

### Terminal Reinvestment Rate
- [ACTUAL]: `|pur_of_fixed_assets| / NOPAT` (latest year)
- High-growth normalization: If historical reinvestment > 50% and company is high-growth, use sector steady-state default (30%)
- Bounded: [10%, 60%]

---

## 11. DCF Value Bridge

```
PV of Explicit FCFFs (Years 1-5)
+ PV of Terminal Value
= Firm (Enterprise) Value
- Gross Debt
+ Cash & Equivalents
= Equity Value
/ Shares Outstanding
= Intrinsic Value per Share
```

### FCFF Projection (each year)
```
Revenue = Previous Revenue * (1 + growth_rate)
EBITDA = Revenue * (EBITDA_margin + margin_improvement * year)
Depreciation = Revenue * depreciation_to_sales
EBIT = EBITDA - Depreciation
NOPAT = EBIT * (1 - tax_rate)
Capex = Revenue * capex_to_sales
ΔNWC = (Revenue - Previous Revenue) * nwc_to_sales
FCFF = NOPAT + Depreciation - Capex - ΔNWC
```

---

## 12. Blended Valuation Weights

```
Final Intrinsic = DCF * 60% + Relative * 30% + Monte Carlo Median * 10%
```

If any method fails, redistribute proportionally among available methods.

---

## 13. Known Limitations & Areas for Improvement

1. **VBL Tax Rate**: Computed at 35.6% vs Emkay estimate of 23.2%. Only 1 year of pbt_excp data available.
2. **VBL Dep/Sales**: 8.1% vs normalized ~5%. Only 1 year of acc_dep data — trend weighting requires >= 2 years.
3. **BEL Beta**: Defaults to 1.0 (market beta) because "Capital Goods" not mapped in Damodaran sectors. Defense PSU should be ~0.6-0.7.
4. **Cash estimation**: Balance sheet residual method is approximate. Includes investments and other current assets in the residual. Best fix: source actual cash from company filings or screener data.

---

*Last updated: 2026-02-08*
