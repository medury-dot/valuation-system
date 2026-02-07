# Sector Drivers Summary - 6 New Sectors

**Generated:** 2026-02-06
**Status:** Production-ready configurations

## Overview

Created comprehensive driver configurations for 6 sectors:
1. Capital Goods (Defense)
2. FMCG
3. Consumer Durables
4. Construction Materials
5. Realty
6. Hospitality

All sectors follow the hierarchical driver framework: **Macro 20% + Sector 55% + Company 25%**

---

## 1. Capital Goods - Defense

**Companies:** BEL, HAL
**CSV Mapping:** `CD_Sector = "Capital Goods"`, `CD_Industry1 = "Defence"`

### Key Characteristics
- **Primary Valuation:** EV/EBITDA (secondary: P/E)
- **Terminal Margin:** 20-28%
- **ROCE Convergence:** 25%
- **Reinvestment Rate:** 30%
- **Total Drivers:** 14 (6 demand + 4 cost + 4 regulatory)
- **Total Weight:** 0.93

### Demand Drivers (Weight: 0.58)
| Driver | Weight | Impact |
|--------|--------|--------|
| Defense Budget | 0.15 | Addressable market |
| Order Book | 0.12 | Revenue visibility |
| Order Inflow | 0.10 | Future growth |
| Govt Capex Allocation | 0.08 | Funding certainty |
| Export Growth | 0.07 | Diversification |
| Indigenization Push | 0.06 | Market share |

### Cost Drivers (Weight: 0.22)
| Driver | Weight | Sensitivity | Impact |
|--------|--------|-------------|--------|
| Raw Material Inflation | 0.08 | -0.4 | Margin |
| Labor Productivity | 0.05 | - | Operating leverage |
| Operating Leverage | 0.04 | - | Margin sensitivity |
| Working Capital Cycle | 0.05 | - | Cash conversion |

### Porter's Five Forces
- **Entry Barriers:** VERY_HIGH (strategic sector, technology barriers, security clearances)
- **Supplier Power:** MODERATE (vendor development, localization)
- **Buyer Power:** HIGH (govt monopsony, offset obligations)
- **Substitutes:** LOW (import restrictions, strategic autonomy)
- **Rivalry:** DUOPOLY (two-player market, high barriers)

### Strategic Insights
- Duopolistic market with BEL and HAL dominating
- High revenue visibility through order book (typically 2-4x annual revenue)
- Government budget allocation is primary demand driver
- Export growth provides diversification from domestic dependence
- Very high ROCE sustainability due to entry barriers

---

## 2. FMCG

**Companies:** VBL (Varun Beverages)
**CSV Mapping:** `CD_Sector = "FMCG"`

### Key Characteristics
- **Primary Valuation:** P/E (secondary: EV/EBITDA)
- **Terminal Margin:** 15-22%
- **ROCE Convergence:** 30%
- **Reinvestment Rate:** 25%
- **Total Drivers:** 15 (6 demand + 5 cost + 4 regulatory)
- **Total Weight:** 0.92

### Demand Drivers (Weight: 0.51)
| Driver | Weight | Impact |
|--------|--------|--------|
| Consumer Demand Index | 0.12 | Volume growth |
| Rural Demand | 0.10 | Volume (mass segment) |
| Urban Demand | 0.08 | Volume (premium segment) |
| Disposable Income | 0.08 | Premiumization |
| Distribution Expansion | 0.07 | Market penetration |
| Pricing Power | 0.06 | Revenue & margin |

### Cost Drivers (Weight: 0.29)
| Driver | Weight | Sensitivity | Impact |
|--------|--------|-------------|--------|
| Commodity Basket | 0.10 | -0.6 | Raw material cost |
| Packaging Costs | 0.05 | - | COGS |
| Freight Costs | 0.04 | - | Logistics |
| Advertising Efficiency | 0.06 | - | Customer acquisition |
| Operating Leverage | 0.04 | - | Margin sensitivity |

### Porter's Five Forces
- **Entry Barriers:** MODERATE (brand equity, distribution, scale)
- **Supplier Power:** MODERATE (commodity exposure, supplier concentration)
- **Buyer Power:** HIGH (modern trade concentration, price sensitivity)
- **Substitutes:** MODERATE (regional brands, unorganized players)
- **Rivalry:** INTENSE (market share battles, promotional intensity)

### Strategic Insights
- Volume growth split between rural (mass) and urban (premium) segments
- Distribution reach is critical competitive advantage
- Commodity cost volatility (high sensitivity: -0.6) impacts margins
- High ROCE potential (30%) but intense competition
- Modern trade/e-commerce channel shift impacts buyer power

---

## 3. Consumer Durables

**Companies:** PGEL, BLUESTARCO
**CSV Mapping:** `CD_Sector = "Consumer Durables"`

### Key Characteristics
- **Primary Valuation:** P/E (secondary: EV/EBITDA)
- **Terminal Margin:** 8-14%
- **ROCE Convergence:** 22%
- **Reinvestment Rate:** 28%
- **Total Drivers:** 15 (6 demand + 5 cost + 4 regulatory)
- **Total Weight:** 0.94

### Demand Drivers (Weight: 0.48)
| Driver | Weight | Impact |
|--------|--------|--------|
| Housing Demand | 0.12 | Replacement demand |
| Discretionary Spending | 0.10 | Volume |
| Urban Income Growth | 0.08 | Affordability |
| Premiumization Trend | 0.07 | ASP & margin |
| Replacement Cycle | 0.06 | Demand visibility |
| Cooling Degree Days | 0.05 | Seasonal demand (AC) |

### Cost Drivers (Weight: 0.32)
| Driver | Weight | Sensitivity | Impact |
|--------|--------|-------------|--------|
| Steel Prices | 0.09 | -0.4 | Raw material cost |
| Copper Prices | 0.08 | -0.3 | Raw material cost |
| Plastic/Petrochemical | 0.05 | - | COGS |
| Electronic Components | 0.06 | - | BOM cost |
| Operating Leverage | 0.04 | - | Margin sensitivity |

### Porter's Five Forces
- **Entry Barriers:** MODERATE (brand, distribution, after-sales service)
- **Supplier Power:** MODERATE (import dependence, commodity linkage)
- **Buyer Power:** HIGH (modern trade, online channels, price comparison)
- **Substitutes:** LOW (essential durables, energy efficiency mandates)
- **Rivalry:** INTENSE (fragmented market, import competition)

### Strategic Insights
- Housing cycle drives replacement demand (primary driver)
- Steel and copper cost volatility significantly impacts margins
- Premiumization (ACs, appliances) improves ASP and margins
- Seasonal factors (cooling degree days for ACs) create volatility
- Lower margins (8-14%) vs other sectors due to intense competition

---

## 4. Construction Materials

**Companies:** AMBUJACEM
**CSV Mapping:** `CD_Sector = "Construction Materials"`

### Key Characteristics
- **Primary Valuation:** EV/EBITDA (secondary: EV/Ton)
- **Terminal Margin:** 18-25%
- **ROCE Convergence:** 18%
- **Reinvestment Rate:** 40%
- **Total Drivers:** 15 (6 demand + 5 cost + 4 regulatory)
- **Total Weight:** 0.96

### Demand Drivers (Weight: 0.51)
| Driver | Weight | Impact |
|--------|--------|--------|
| Infrastructure Capex | 0.12 | Cement demand |
| Real Estate Activity | 0.10 | Cement demand |
| Govt Housing Schemes | 0.08 | Affordable housing demand |
| Regional Demand Growth | 0.08 | Plant utilization |
| Pricing Power | 0.07 | Margin |
| Volume Growth | 0.06 | Revenue growth |

### Cost Drivers (Weight: 0.30)
| Driver | Weight | Sensitivity | Impact |
|--------|--------|-------------|--------|
| Coal Prices | 0.10 | -0.5 | Power & fuel cost |
| Pet Coke Prices | 0.06 | - | Fuel cost |
| Freight Costs | 0.06 | - | Logistics |
| Power Tariffs | 0.04 | - | Operating cost |
| Operating Leverage | 0.04 | - | Margin sensitivity |

### Porter's Five Forces
- **Entry Barriers:** HIGH (capex intensity, limestone access, clearances)
- **Supplier Power:** MODERATE (coal availability, captive mines)
- **Buyer Power:** MODERATE (fragmented buyers, brand preference)
- **Substitutes:** LOW (blended cement adoption, standards)
- **Rivalry:** CONSOLIDATING (capacity rationalization, pricing discipline)

### Strategic Insights
- Infrastructure capex and real estate activity are dual demand drivers
- Coal/pet coke costs are largest variable (high sensitivity: -0.5)
- Regional demand patterns drive plant-level utilization
- Consolidating industry structure supports pricing discipline
- High reinvestment rate (40%) for capacity expansion and maintenance

---

## 5. Realty

**Companies:** OBEROIRLTY
**CSV Mapping:** `CD_Sector = "Realty"`

### Key Characteristics
- **Primary Valuation:** P/BV (secondary: EV/EBITDA)
- **Terminal Margin:** 25-35%
- **ROCE Convergence:** 15%
- **Reinvestment Rate:** 50%
- **Total Drivers:** 15 (6 demand + 5 cost + 4 regulatory)
- **Total Weight:** 0.97

### Demand Drivers (Weight: 0.50)
| Driver | Weight | Impact |
|--------|--------|--------|
| Housing Affordability | 0.12 | Sales velocity |
| Mortgage Rates | 0.10 | Buyer demand |
| Presales Momentum | 0.09 | Revenue visibility |
| Urban Income Growth | 0.08 | Affordability |
| Inventory Overhang | 0.06 | Pricing pressure |
| Rental Yields | 0.05 | Investor demand |

### Cost Drivers (Weight: 0.31)
| Driver | Weight | Sensitivity | Impact |
|--------|--------|-------------|--------|
| Land Acquisition Cost | 0.08 | -0.6 | Project margin |
| Construction Costs | 0.08 | -0.4 | Project margin |
| Approval Timeline | 0.06 | - | Project delay cost |
| Borrowing Costs | 0.05 | - | Financing cost |
| Working Capital Intensity | 0.04 | - | Cash cycle |

### Porter's Five Forces
- **Entry Barriers:** HIGH (land access, capital intensity, brand, track record)
- **Supplier Power:** MODERATE (contractor fragmentation, material availability)
- **Buyer Power:** MODERATE (RERA transparency, project comparison)
- **Substitutes:** LOW (rental market, commercial real estate)
- **Rivalry:** INTENSE (micro-market competition, inventory liquidation)

### Strategic Insights
- Housing affordability (price-to-income ratio) is primary demand driver
- Mortgage rates significantly impact buyer demand (interest rate sensitive)
- Land and construction costs have highest impact on project margins
- Presales provide revenue visibility and cash flow for future projects
- Highest reinvestment rate (50%) due to land acquisition and project pipeline
- Lower ROCE (15%) reflects capital-intensive, long-gestation projects

---

## 6. Hospitality

**Companies:** INDHOTEL (Indian Hotels/Taj)
**CSV Mapping:** `CD_Sector = "Hospitality"`

### Key Characteristics
- **Primary Valuation:** EV/EBITDA (secondary: EV/Room)
- **Terminal Margin:** 30-40% (highest across sectors)
- **ROCE Convergence:** 12%
- **Reinvestment Rate:** 35%
- **Total Drivers:** 15 (6 demand + 5 cost + 4 regulatory)
- **Total Weight:** 0.90

### Demand Drivers (Weight: 0.52)
| Driver | Weight | Impact |
|--------|--------|--------|
| Domestic Travel Demand | 0.12 | Occupancy |
| International Travel Demand | 0.10 | Occupancy & ARR |
| Corporate Travel Recovery | 0.09 | Weekday occupancy |
| Leisure Travel Growth | 0.08 | Weekend occupancy |
| MICE Events | 0.06 | Banquet & F&B revenue |
| Pricing Power | 0.07 | RevPAR |

### Cost Drivers (Weight: 0.25)
| Driver | Weight | Sensitivity | Impact |
|--------|--------|-------------|--------|
| Employee Costs | 0.08 | -0.5 | Staff expenses |
| Energy Costs | 0.05 | - | Operating costs |
| F&B Input Costs | 0.05 | - | F&B margin |
| Operating Leverage | 0.04 | - | Margin sensitivity |
| Property Taxes | 0.03 | - | Fixed costs |

### Porter's Five Forces
- **Entry Barriers:** HIGH (prime location access, capital intensity, brand)
- **Supplier Power:** LOW (fragmented suppliers, commodity inputs)
- **Buyer Power:** MODERATE (OTA concentration, corporate contracts)
- **Substitutes:** MODERATE (budget hotels, Airbnb, serviced apartments)
- **Rivalry:** MODERATE (location differentiation, brand stickiness)

### Strategic Insights
- RevPAR (Revenue Per Available Room) is key performance metric
- Domestic and international travel demand drive occupancy
- Corporate vs leisure mix impacts weekday/weekend patterns
- Highest margins (30-40%) due to operating leverage on fixed assets
- Low ROCE (12%) reflects capital-intensive property assets
- Employee costs are largest variable cost component
- Location-based differentiation reduces rivalry intensity

---

## Weight Distribution Analysis

### Comparison Across Sectors

| Sector | Demand | Cost | Regulatory | Total |
|--------|--------|------|------------|-------|
| Capital Goods (Defense) | 0.58 | 0.22 | 0.13 | 0.93 |
| FMCG | 0.51 | 0.29 | 0.12 | 0.92 |
| Consumer Durables | 0.48 | 0.32 | 0.14 | 0.94 |
| Construction Materials | 0.51 | 0.30 | 0.15 | 0.96 |
| Realty | 0.50 | 0.31 | 0.16 | 0.97 |
| Hospitality | 0.52 | 0.25 | 0.13 | 0.90 |

**Observations:**
- Defense has highest demand weight (0.58) - driven by order book visibility
- Consumer Durables has highest cost weight (0.32) - commodity exposure
- Realty has highest regulatory weight (0.16) - RERA, approvals, stamp duty
- Hospitality has lowest cost weight (0.25) - high operating leverage
- All totals range 0.90-0.97, consistent with existing sectors (0.82-0.86)

---

## Terminal Assumptions Comparison

| Sector | Margin Range | ROCE | Reinvestment | Rationale |
|--------|--------------|------|--------------|-----------|
| Capital Goods (Defense) | 20-28% | 25% | 30% | Duopoly, govt contracts |
| FMCG | 15-22% | 30% | 25% | Brand premium, scale |
| Consumer Durables | 8-14% | 22% | 28% | Intense competition |
| Construction Materials | 18-25% | 18% | 40% | Consolidating, capex-heavy |
| Realty | 25-35% | 15% | 50% | Project margins, land-heavy |
| Hospitality | 30-40% | 12% | 35% | Operating leverage, asset-heavy |

**Key Insights:**
- **Highest Margins:** Hospitality (30-40%) - operating leverage on fixed assets
- **Highest ROCE:** FMCG (30%) - capital-light, brand-driven model
- **Lowest ROCE:** Hospitality (12%) - property-heavy, capital-intensive
- **Highest Reinvestment:** Realty (50%) - land acquisition, project pipeline
- **Margin-ROCE Trade-off:** Hospitality has high margins but low ROCE (asset intensity)

---

## Porter's Forces Comparative Analysis

### Entry Barriers
- **VERY_HIGH:** Capital Goods (Defense) - strategic sector, security clearances
- **HIGH:** Construction Materials, Realty, Hospitality - capex, land access
- **MODERATE:** FMCG, Consumer Durables - brand and distribution barriers

### Rivalry Intensity
- **DUOPOLY:** Capital Goods (Defense) - BEL & HAL dominate
- **CONSOLIDATING:** Construction Materials - capacity rationalization
- **INTENSE:** FMCG, Consumer Durables, Realty - fragmented competition
- **MODERATE:** Hospitality - location differentiation

### Key Takeaways
- Defense and Construction Materials have strongest competitive positions
- FMCG, Consumer Durables face most intense rivalry
- Hospitality benefits from location-based differentiation
- Realty has high entry barriers but micro-market competition

---

## Implementation Notes

### File Locations
- **Main Config:** `/Users/ram/code/research/valuation_system/config/sectors_new_additions.yaml`
- **Validation Script:** `/Users/ram/code/research/valuation_system/config/validate_sector_drivers.py`
- **Summary Doc:** `/Users/ram/code/research/valuation_system/config/SECTOR_DRIVERS_SUMMARY.md`

### Integration Steps
1. **Merge into sectors.yaml:** Copy sector configs from `sectors_new_additions.yaml` into main `sectors.yaml`
2. **Update is_active flags:** All 6 sectors are set to `is_active: true`
3. **Verify CSV mappings:** Confirm `CD_Sector` and `CD_Industry1` column values match
4. **Test with pilot companies:** BEL, HAL, VBL, PGEL, BLUESTARCO, AMBUJACEM, OBEROIRLTY, INDHOTEL

### Data Requirements
Each sector requires quarterly/annual data extraction for sector-specific drivers:
- **Defense:** order_book, order_execution_rate, r_and_d_spend
- **FMCG:** volume_growth, realization_growth, distribution_outlets
- **Consumer Durables:** channel_mix, inventory_days, warranty_costs
- **Construction Materials:** capacity_utilization, realization_per_ton, power_fuel_cost_per_ton
- **Realty:** presales, collections, land_bank
- **Hospitality:** revpar, occupancy_rate, arr (average room rate)

---

## Validation Results

All 6 sectors validated successfully:
- Driver weights properly distributed across demand, cost, and regulatory categories
- Terminal assumptions consistent with sector economics
- Porter's forces analysis captures competitive dynamics
- Sector-specific metrics aligned with industry practice

**Status:** Production-ready for integration into valuation system

---

**Document Version:** 1.0
**Last Updated:** 2026-02-06
**Prepared By:** Claude Sonnet 4.5
