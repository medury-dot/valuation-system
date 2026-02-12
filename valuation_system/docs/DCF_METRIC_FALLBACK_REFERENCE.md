# DCF Metric Fallback Reference

Complete reference for all 18 valuation metrics computed by `financial_processor.py → build_dcf_inputs()`.

**Key design principle**: NO metric ever blocks valuation. Every metric has a DEFAULT. Anomalies are logged as WARNING but the pipeline always continues. This ensures batch runs of 2000+ companies never crash on a single company's data gap.

**Source file**: `valuation_system/data/processors/financial_processor.py`

---

## Summary Table

| # | Metric | Method | Fallback Chain | Default | Anomaly Check | Continues? | Log |
|---|--------|--------|---------------|---------|---------------|------------|-----|
| 1 | base_revenue | `_prefer_ttm` | TTM quarterly → annual | 0 | None | YES | INFO |
| 2 | revenue_growth_rates | `_build_growth_trajectory` | CAGR blend → YoY → default | [10-6%] | Cap ±30% | YES | WARN |
| 3 | margin_improvement | `_estimate_margin_trend` | Linear slope → 0 | 0.0 | Cap ±2pp | YES | — |
| 4 | ebitda_margin | `_calculate_ebitda_margin` | PBIDT/Sales 3yr → default | 0.15 | None | YES | INFO |
| 5 | capex_to_sales | `_calculate_capex_to_sales` | PurFA → delta(GB+CWIP) → Qtr → default | 0.08 | High-growth 6.5% | YES | WARN |
| 6 | depreciation_to_sales | `_calculate_depreciation_to_sales` | delta(acc_dep) → delta(acc_depr) → delta(GB-NB) → default | 0.04 | 1yr cross-check | YES | WARN |
| 7 | nwc_to_sales | `_calculate_nwc_to_sales` | CF WC → BS half-yearly → BS annual → days → default | 0.15 | High-growth 30% cap | YES | WARN |
| 8 | tax_rate | `_estimate_effective_tax_rate` | CF tax → accrual → qtr PBT → PBIDT proxy → default | 0.25 | 1yr blend | YES | INFO |
| 9 | beta | `damodaran_loader` | Weekly subgroup → India industry → global → market | 1.0 | None | YES | INFO |
| 10 | risk_free_rate | `damodaran_loader` | market_indicators.csv | 0.07 | None | YES | INFO |
| 11 | cost_of_debt | `_estimate_cost_of_debt` | Interest/Debt → qtr interest → default | 0.09 | Range 1-25% | YES | INFO |
| 12 | debt_ratio | `_calculate_debt_ratio` | D/(D+MCap) → D/(D+NW) → default | 0.10 | None | YES | — |
| 13 | terminal_roce | `_estimate_terminal_roce` | NOPAT/CE → NW+Debt sub → ROCE series → default | 0.15 | CE vs NW+Debt; 5Y ROE | YES | WARN |
| 14 | terminal_reinvestment | `_estimate_terminal_reinvestment` | Payout → Capex/NOPAT → blend → default | 0.30 | High-growth sector | YES | INFO |
| 15 | shares_outstanding | `_estimate_shares_outstanding` | Paid-up shares → MCap/CMP → default | 1.0 Cr | ACTUAL vs MCap >20% | YES | WARN |
| 16 | cash_and_equivalents | `_get_cash_equivalents` | Cash HY → BS residual → 0 | 0.0 | Negative → 0 | YES | WARN |
| 17 | net_debt | `_calculate_net_debt` | Latest debt | 0.0 | None | YES | DEBUG |
| 18 | rd_pct_of_sales | `_get_rd_to_sales` | rd_pct series (4 subgroups) | None | None | YES | INFO |

---

## A. Revenue & Growth Metrics

### 1. base_revenue (Revenue Base — Rs Cr)

| Item | Detail |
|------|--------|
| **Method** | `_prefer_ttm()` |
| **Priority 1** | TTM from quarterly `sales_quarterly` — sum of last 4 quarters |
| **Priority 2** | Latest annual value from `sales_annual` |
| **Default** | 0 |
| **Anomaly detection** | None |
| **Anomaly handling** | N/A |
| **Valuation continues?** | YES — 0 revenue means all projections = 0, DCF = 0, but doesn't crash |
| **Log level** | INFO with source tag (e.g. "TTM quarterly (sum last 4Q = 18,148.0)") |
| **`_data_sources` key** | Not tracked (always available from core CSV) |

### 2. revenue_growth_rates (5-year projection trajectory)

| Item | Detail |
|------|--------|
| **Method** | `_build_growth_trajectory()` |
| **Priority 1** | Blend: 3Y CAGR (60%) + 5Y CAGR (40%), linear decay from starting rate to 6% terminal zone |
| **Priority 2** | YoY from most recent 2 annual sales values — dampened by 20% (single year is noisy), capped at 30% |
| **Default** | `[10%, 9%, 8%, 7%, 6%]` |
| **Anomaly detection** | Starting growth capped at ±30% (floor 3%, ceiling 30%) |
| **Anomaly handling** | Capping applied silently — prevents both unrealistic optimism and excessive pessimism |
| **Valuation continues?** | YES |
| **Log level** | WARNING "No CAGR or YoY data, using default 10% growth" when no data; INFO for YoY fallback; DEBUG for computed |

### 3. margin_improvement (annual EBITDA margin change, pp/year)

| Item | Detail |
|------|--------|
| **Method** | `_estimate_margin_trend()` |
| **Priority 1** | Linear regression slope of `ebidtm` (EBITDA margin %) series, converted to decimal |
| **Default** | 0.0 (no improvement assumed) |
| **Anomaly detection** | Slope capped at ±2pp/year (`max(-0.02, min(0.02, slope))`) |
| **Anomaly handling** | Silent cap — prevents unrealistic linear extrapolation. DCF model applies additional damping (Y1=full, Y5=0) |
| **Valuation continues?** | YES |
| **Log level** | None (silent) |

---

## B. Margin Metrics

### 4. ebitda_margin (EBITDA/Sales)

| Item | Detail |
|------|--------|
| **Method** | `_calculate_ebitda_margin()` |
| **Priority 1** | `pbidt_annual / sales_annual` — average of last 3 years |
| **Default** | 0.15 (15%) |
| **Anomaly detection** | None |
| **Anomaly handling** | N/A |
| **Valuation continues?** | YES |
| **Log level** | INFO with "[DEFAULT]" tag when defaulted |

---

## C. Capital Requirement Metrics

### 5. capex_to_sales (Capex/Sales)

| Item | Detail |
|------|--------|
| **Method** | `_calculate_capex_to_sales()` |
| **Priority 1 [ACTUAL]** | `|pur_of_fixed_assets| / sales_annual` — 3yr average. **Trend-aware**: if latest year < 75% of oldest year, use weighted avg (latest 60%, middle 25%, oldest 15%) instead of simple avg |
| **Priority 2 [DERIVED]** | `delta(gross_block + CWIP) / sales` — change in fixed assets as capex proxy, 3yr avg |
| **Priority 3 [DERIVED]** | Annualized quarterly capex from half-yearly `cashflow_purchase_fixedassets` / sales, 3yr avg |
| **Default** | 0.08 (8%) — returned as `None` then defaulted in `build_dcf_inputs()` |
| **Anomaly detection** | High-growth check: 3Y CAGR > 20% OR recent YoY > 50%. If high-growth AND capex > 12% → expansion phase detected |
| **Anomaly handling** | Normalize to 6.5% maintenance capex (not historical expansion capex). Prevents overestimating permanent capital needs from temporary capacity buildout |
| **Valuation continues?** | YES |
| **Log level** | WARNING "No capex data available" for default; INFO for source + trend weighting details |

### 6. depreciation_to_sales (D&A/Sales)

| Item | Detail |
|------|--------|
| **Method** | `_calculate_depreciation_to_sales()` |
| **Priority 1 [ACTUAL]** | `delta(acc_dep_yearly) / sales` — annual depreciation charge from change in accumulated depreciation, 3yr avg. **Trend-aware** weighting same as capex. |
| **Priority 2 [ACTUAL]** | `delta(accumulated_depreciation) / sales` — compat key (same data, alternate dict key) |
| **Priority 3 [DERIVED]** | `delta(gross_block - net_block) / sales` — net block difference as depreciation proxy |
| **Default** | 0.04 (4%) — returned as `None` then defaulted |
| **Anomaly detection** | **Single-year cross-check**: If only 1 year of acc_dep data, also compute via Method 3 (block diff). If Method 3 has more data AND gives a ratio > 15% lower, blend 50/50 for stability |
| **Anomaly handling** | Blend mitigates risk of single-year outlier (e.g. acquisition year inflating accumulated depreciation) |
| **Valuation continues?** | YES |
| **Log level** | WARNING "No depreciation data available" for default; INFO for source + trend/blend details |

### 7. nwc_to_sales (NWC/Sales) — TIER 1: `cf_wc_change`

| Item | Detail |
|------|--------|
| **Method** | `_calculate_nwc_to_sales()` |
| **Priority 0 [ACTUAL_CF]** | `cf_wc_change_yearly / sales` — 3yr avg of actual cash flow working capital changes. Sign convention: positive cf_wc = cash inflow (WC decreased) → negative NWC/Sales. **Gold standard** — actual cash impact. |
| **Priority 1A [ACTUAL-HY]** | Half-yearly BS: `(Inv + Dr) - (TotLiab - LT_Borrow) / TTM_sales`. Uses most recent half-year (more current than annual). |
| **Priority 1B [ACTUAL-ANN]** | Annual BS: Same formula with yearly data. |
| **Priority 2 [DERIVED]** | Receivable/inventory/payable days ratios from fullstats. |
| **Default** | 0.15 (15%) — returned as `None` then defaulted |
| **Anomaly detection** | High-growth: 3Y CAGR > 20% OR YoY > 50%. If high-growth AND NWC/Sales > 60% → abnormal (inventory build-up / extended receivables during expansion) |
| **Anomaly handling** | Cap at 30% for high-growth companies. **Negative NWC is normal** for: defense (BEL: -7%), auto OEMs (Eicher: -99%), FMCG (ITC: -55%), pharma exporters (SunPharma: -96%), specialty chemicals (Aether: -144%). These collect customer/dealer advances upfront. |
| **Valuation continues?** | YES |
| **Log level** | WARNING "No NWC data available" for default; INFO for source + high-growth normalization |
| **`_data_sources` key** | `nwc`: ACTUAL_CF / ACTUAL_BS / DERIVED / DEFAULT |

---

## D. Tax & WACC Metrics

### 8. tax_rate (Effective Tax Rate) — TIER 1: `cf_tax_paid`

| Item | Detail |
|------|--------|
| **Method** | `_estimate_effective_tax_rate()` |
| **Priority 0 [ACTUAL_CF]** | `|cf_tax_paid| / PBT` — 3yr avg of actual cash taxes paid divided by pre-tax profit. Range filter: 0 < rate < 50%. |
| **Priority 1 [DERIVED_ACCRUAL]** | `1 - PAT / pbt_excp_yearly` — accrual-based from P&L, 3yr avg. Uses PBT excluding exceptional items for stability. |
| **Priority 2 [DERIVED]** | Same formula but with quarterly PBT annualized (when yearly pbt_excp unavailable). |
| **Priority 3 [DERIVED]** | PBT estimated as `PBIDT - Interest - Depreciation`. Fallback when neither pbt_excp nor quarterly PBT available. |
| **Default** | 0.25 (25% — India statutory corporate rate) |
| **Anomaly detection** | **Single-year blend**: If Method 1 has only 1 year of data AND rate > 30%, also compute Method 3. If Methods differ by > 5pp, blend 50/50. Prevents a single anomalous year (tax refund, write-off) from dominating. |
| **Anomaly handling** | Blend produces more stable estimate than single-year outlier |
| **Valuation continues?** | YES |
| **Log level** | INFO for source; DEBUG for per-year breakdown |
| **`_data_sources` key** | `tax`: ACTUAL_CF / DERIVED_ACCRUAL / DEFAULT |

### 9. beta (Levered Beta)

| Item | Detail |
|------|--------|
| **Method** | `damodaran_loader.get_all_params()` |
| **Priority 1** | Weekly subgroup beta from `subgroup_betas_weekly.json` — computed by `beta_calculator.py` from NIFTY index + stock weekly returns. Cache keys are UPPERCASE; lookup is case-insensitive. |
| **Priority 2** | Damodaran India industry beta — mapped from valuation_subgroup to Damodaran industry name (case-insensitive). |
| **Priority 3** | Damodaran global sector beta. |
| **Default** | Market beta 1.0 |
| **Anomaly detection** | None (pre-computed, trusted source) |
| **Anomaly handling** | N/A |
| **Valuation continues?** | YES |
| **Log level** | INFO with `beta_source` tag (e.g. "weekly_subgroup", "damodaran_india", "damodaran_global") |

### 10. risk_free_rate

| Item | Detail |
|------|--------|
| **Method** | `damodaran_loader.get_all_params()` |
| **Source** | `market_indicators.csv` (10Y Indian Government Securities yield from FRED, updated monthly). **NOT** from yfinance API (per external API policy). |
| **Default** | 0.07 (7%) |
| **Anomaly detection** | None |
| **Valuation continues?** | YES |
| **Log level** | INFO |

### 11. cost_of_debt (Pre-tax Kd)

| Item | Detail |
|------|--------|
| **Method** | `_estimate_cost_of_debt()` |
| **Priority 1 [ACTUAL]** | `interest_yearly / debt` — 3yr avg. Range filter: 1% < rate < 25% (filters out anomalies like near-zero interest or restructuring years). |
| **Priority 2 [DERIVED]** | Annualized quarterly interest expense / debt — same range filter. |
| **Default** | 0.09 (9% — typical Indian corporate borrowing rate) |
| **Anomaly detection** | Range filter 1-25% is the anomaly guard |
| **Anomaly handling** | Years outside range silently excluded from average |
| **Valuation continues?** | YES |
| **Log level** | INFO for source |

### 12. debt_ratio (D/(D+E) for WACC)

| Item | Detail |
|------|--------|
| **Method** | `_calculate_debt_ratio()` |
| **Priority 1** | `debt / (debt + MCap)` — market-value weights (preferred for WACC) |
| **Priority 2** | `debt / (debt + networth)` — book-value fallback when MCap unavailable |
| **Default** | 0.10 (10%) |
| **Anomaly detection** | None |
| **Anomaly handling** | N/A |
| **Valuation continues?** | YES |
| **Log level** | None (silent) |

---

## E. Terminal Value Metrics

### 13. terminal_roce (Terminal ROCE) — TIER 1: `capital_employed`

| Item | Detail |
|------|--------|
| **Method** | `_estimate_terminal_roce()` |
| **Priority 0 [ACTUAL_CE]** | `NOPAT / capital_employed` for last 5 years. Blended 60% historical avg + 40% convergence target (default 15%). Clamped to 8-30% range. |
| **CE anomaly detection** | Cross-validate CE against NW+Debt proxy. If divergence > 50%, CE data is suspect (goodwill, revaluations, accounting differences). |
| **CE anomaly handling** | **Use NW+Debt as substitute** for that year (not skip). This preserves data points while using a more reliable denominator. |
| **Priority 1 [DERIVED_NWDEBT]** | `roce` series from core CSV — pre-computed using (PBT+Interest)/(NW+Debt) proxy. Same 5yr avg + convergence blend. |
| **Default** | 0.15 (15%) |
| **Cross-check** | If fullstats `5yr_avg_roe` available, compare against computed terminal ROCE. If divergence > 5pp, log WARNING. Informational only — does not override. |
| **Valuation continues?** | YES — DEFAULT 15% ensures a valid terminal value even with no data |
| **Log level** | WARNING for CE divergence + ROE cross-check; INFO for source |
| **`_data_sources` key** | `roce`: ACTUAL_CE / DERIVED_NWDEBT / DEFAULT |

### 14. terminal_reinvestment — TIER 1: `dividend_payout_ratio`

| Item | Detail |
|------|--------|
| **Method** | `_estimate_terminal_reinvestment()` |
| **Priority 0 [ACTUAL_PAYOUT]** | `1 - avg_dividend_payout` — 3yr avg of payout ratio. Auto-converts % to decimal. Clamped 10-60%. Used for 40% blend weight. |
| **Priority 1 [DERIVED_CAPEX]** | `|pur_of_fixed_assets| / NOPAT` — historical capex-based reinvestment. Clamped 10-60%. Used for 60% blend weight. |
| **Priority 2 [DERIVED]** | TTM quarterly capex / NOPAT — same as Priority 1 but from quarterly data. |
| **Blend rule** | If both payout + capex available: **60% capex + 40% payout** (capex is more direct measure of reinvestment). |
| **Default** | Sector `reinvestment_rate` from terminal_assumptions config (or 0.30 if no config). |
| **Anomaly detection** | High-growth: 3Y CAGR > 20% OR YoY > 50%. If high-growth AND reinvestment > 50% → expansion phase (temporary high capex). |
| **Anomaly handling** | Use sector default instead of historical (prevents treating expansion capex as permanent terminal reinvestment). |
| **Valuation continues?** | YES |
| **Log level** | INFO for source + blend details |
| **`_data_sources` key** | `reinvest`: ACTUAL_PAYOUT / DERIVED_CAPEX / DEFAULT |

---

## F. Balance Sheet Metrics

### 15. shares_outstanding (Crores) — TIER 1

| Item | Detail |
|------|--------|
| **Method** | `_estimate_shares_outstanding()` |
| **Priority 0 [ACTUAL_COLUMN]** | `shares_outstanding_yearly` from core/fullstats CSV. Unit normalization: > 1M → divide by 1e7 (raw count to Cr); > 100 → divide by 100 (lakhs to Cr); else assume already in Cr. |
| **Priority 1 [DERIVED_MCAP]** | `MCap_Cr / CMP` — market-implied shares from market cap and current price. |
| **Default** | 1.0 Cr (makes per-share values meaningless but total enterprise DCF remains valid) |
| **Anomaly detection** | **Cross-validate ACTUAL vs MCap/CMP**. If divergence > 20%, ACTUAL may be stale (pre-bonus/split). Market price already reflects bonus/split reality. |
| **Anomaly handling** | Use MCap/CMP (DERIVED_MCAP) when ACTUAL diverges > 20%. Log WARNING with both values. |
| **Valuation continues?** | YES — even DEFAULT 1.0 allows DCF enterprise value calculation; only per-share is meaningless |
| **Log level** | WARNING for divergence; WARNING for "Cannot estimate shares" |
| **`_data_sources` key** | `shares`: ACTUAL_COLUMN / DERIVED_MCAP / DEFAULT |

### 16. cash_and_equivalents (Rs Cr)

| Item | Detail |
|------|--------|
| **Method** | `_get_cash_equivalents()` |
| **Priority 1 [ACTUAL]** | `cash_and_bank` from half-yearly data (prefer latest h2 = March year-end). This column is **half-yearly ONLY** — no annual version exists. |
| **Priority 2 [DERIVED]** | Balance sheet residual: `(NW + Debt + Trade_Payables) - (Net_Block + CWIP + Inventories + Debtors)`. All actual CSV columns. Assumption: OtherLiabilities approx OtherAssets (provisions, advances, etc. net out). |
| **Default** | 0.0 |
| **Anomaly detection** | Negative residual check — if (Liabilities - Assets) < 0, cash can't be negative |
| **Anomaly handling** | Set cash to 0 when residual is negative. Log the residual breakdown for audit. |
| **Valuation continues?** | YES — 0 cash is conservative but valid (equity = EV - debt + 0) |
| **Log level** | INFO for source + full residual calc breakdown; WARNING "Cash & equivalents: Rs 0.0 Cr [DEFAULT]" when no data |

### 17. net_debt (Gross Debt — Rs Cr)

| Item | Detail |
|------|--------|
| **Method** | `_calculate_net_debt()` |
| **Source** | Latest value from `debt` series in core CSV (annual `YYYY_debt` columns) |
| **Default** | 0.0 |
| **Note** | Despite the name, this returns GROSS debt. Cash is subtracted separately: `Equity = EV - debt + cash`. |
| **Anomaly detection** | None |
| **Valuation continues?** | YES |
| **Log level** | DEBUG |

---

## G. Sector-Specific Metrics

### 18. rd_pct_of_sales (R&D/Sales) — TIER 1

| Item | Detail |
|------|--------|
| **Method** | `_get_rd_to_sales()` |
| **Applies to** | 4 subgroups ONLY: `HEALTHCARE_PHARMA_MFG`, `INDUSTRIALS_DEFENSE`, `HEALTHCARE_MEDICAL_EQUIPMENT`, `TECHNOLOGY_PRODUCT_SAAS`. Returns `None` for all other subgroups. |
| **Priority 0 [ACTUAL]** | `rd_pct_of_sales_yearly` — 3yr avg. Auto-converts from % to decimal if value > 1 (e.g. 5.2 → 0.052). |
| **Default** | None — R&D is excluded from valuation when no data (not blocked, just omitted) |
| **Anomaly detection** | None |
| **Anomaly handling** | N/A |
| **Valuation continues?** | YES — R&D is an additive adjustment, not a required input |
| **Log level** | INFO when R&D data available |
| **Status** | Code ready. Activates when `rd_pct_of_sales` column appears in core/fullstats CSV (TIER 1 pending from Accord). |

---

## Data Source Tracking (`_data_sources`)

The `_data_sources` dict is reset per company in `build_dcf_inputs()` and tracks which tier (ACTUAL/DERIVED/DEFAULT) was used for each key metric. These are:

| Key | Possible Values | Saved To |
|-----|-----------------|----------|
| `nwc` | ACTUAL_CF, ACTUAL_BS, DERIVED, DEFAULT | vs_valuation_snapshots.nwc_source |
| `tax` | ACTUAL_CF, DERIVED_ACCRUAL, DEFAULT | vs_valuation_snapshots.tax_source |
| `shares` | ACTUAL_COLUMN, DERIVED_MCAP, DEFAULT | vs_valuation_snapshots.shares_source |
| `roce` | ACTUAL_CE, DERIVED_NWDEBT, DEFAULT | vs_valuation_snapshots.roce_source |
| `reinvest` | ACTUAL_PAYOUT, DERIVED_CAPEX, DEFAULT | vs_valuation_snapshots.reinvest_source |

These are also displayed in the Excel report's Assumptions sheet, Column D, with color coding:
- Green: ACTUAL_* (actual data from CSV)
- Yellow: DERIVED_* (computed estimate)
- Red: DEFAULT (hardcoded fallback)

---

## Anomaly Handling Philosophy

1. **Never block**: Every metric returns a value. Defaults are conservative but allow DCF to complete.
2. **Cross-validate**: Where two independent sources exist (CE vs NW+Debt, shares vs MCap/CMP), compare them. Use the more reliable source when divergence exceeds threshold.
3. **Normalize for growth**: High-growth companies have temporarily inflated capex, NWC, and reinvestment. Detect growth phase (CAGR > 20% or YoY > 50%) and normalize to steady-state for terminal projections.
4. **Log everything**: Every metric logs its source and method. WARNINGs for anomalies, INFO for source tags, DEBUG for per-year breakdowns. All logs captured in Excel's Computation Log tab.
5. **Blend when uncertain**: When multiple methods give different results (single-year tax, capex+payout for reinvestment), blend them rather than picking one.
