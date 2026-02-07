# Sector Drivers Integration Guide

## Files Created

1. **sectors_new_additions.yaml** - Complete YAML for 6 new sectors
2. **validate_sector_drivers.py** - Validation script for driver weights
3. **SECTOR_DRIVERS_SUMMARY.md** - Comprehensive documentation
4. **INTEGRATION_GUIDE.md** - This file

## Quick Stats

| Metric | Value |
|--------|-------|
| Sectors Created | 6 |
| Total Drivers | 89 |
| Avg Drivers per Sector | ~15 |
| Companies Covered | 8 (BEL, HAL, VBL, PGEL, BLUESTARCO, AMBUJACEM, OBEROIRLTY, INDHOTEL) |

## Integration Steps

### Option 1: Manual Merge (Recommended for Review)

1. Open both files:
   ```bash
   cd /Users/ram/code/research/valuation_system/config
   code sectors.yaml sectors_new_additions.yaml
   ```

2. In `sectors.yaml`, find the line (around line 261):
   ```yaml
   # ---------------------------------------------------------------------------
   # FUTURE SECTORS (inactive until enabled)
   # ---------------------------------------------------------------------------
   ```

3. Replace the placeholder sections with content from `sectors_new_additions.yaml`:
   - Replace `fmcg: ... is_active: false` with full FMCG config
   - Insert `capital_goods_defense:` section before FMCG
   - Insert `consumer_durables:`, `construction_materials:`, `realty:`, `hospitality:` after FMCG

4. Keep existing NBFC, Pharma, IT Services sections at the bottom

### Option 2: Automated Merge

```bash
cd /Users/ram/code/research/valuation_system/config

# Backup original
cp sectors.yaml sectors.yaml.backup

# Merge (manual verification still recommended)
python3 << 'EOF'
import yaml

# Read existing sectors.yaml
with open('sectors.yaml', 'r') as f:
    main_config = yaml.safe_load(f)

# Read new sectors
with open('sectors_new_additions.yaml', 'r') as f:
    new_content = 'sectors:\n' + f.read()
    new_config = yaml.safe_load(new_content)

# Merge new sectors into main config
for sector_key, sector_data in new_config['sectors'].items():
    main_config['sectors'][sector_key] = sector_data

# Write back
with open('sectors.yaml', 'w') as f:
    yaml.dump(main_config, f, default_flow_style=False, sort_keys=False, allow_unicode=True)

print("✓ Merged successfully! Review sectors.yaml and run validation.")
EOF

# Validate
python3 validate_sector_drivers.py
```

## Validation Commands

### Check CSV Mappings
```bash
cd /Users/ram/code/research/valuation_system
python3 << 'EOF'
import pandas as pd

df = pd.read_csv('/Users/ram/code/investment_strategies/data/core-input/core-all-input-2026-01-21 11-02-latest-final.csv', low_memory=False)

companies = {
    'BEL': 'Bharat Electronics',
    'HAL': 'Hindustan Aeronautics',
    'VBL': 'Varun Beverages',
    'PGEL': 'PG Electroplast',
    'BLUESTARCO': 'Blue Star',
    'AMBUJACEM': 'Ambuja Cements',
    'OBEROIRLTY': 'Oberoi Realty',
    'INDHOTEL': 'Indian Hotels'
}

print("Company to Sector Mapping Verification:\n")
for ticker, name_part in companies.items():
    row = df[df['Company Name'].str.contains(name_part, case=False, na=False)].head(1)
    if not row.empty:
        sector = row.iloc[0]['CD_Sector']
        industry = row.iloc[0]['CD_Industry1']
        print(f"{ticker:12s} -> {sector:30s} / {industry}")
EOF
```

### Validate Driver Weights
```bash
cd /Users/ram/code/research/valuation_system/config
python3 validate_sector_drivers.py
```

### Test Sector Loading
```bash
cd /Users/ram/code/research/valuation_system
python3 << 'EOF'
import yaml

with open('config/sectors.yaml', 'r') as f:
    config = yaml.safe_load(f)

new_sectors = ['capital_goods_defense', 'fmcg', 'consumer_durables',
               'construction_materials', 'realty', 'hospitality']

print("Sector Activation Status:\n")
for sector in new_sectors:
    if sector in config['sectors']:
        is_active = config['sectors'][sector].get('is_active', False)
        status = "✓ ACTIVE" if is_active else "✗ INACTIVE"
        print(f"{status} {sector}")
    else:
        print(f"✗ MISSING {sector}")
EOF
```

## Post-Integration Testing

### 1. Test Sector Analyst Module
```bash
cd /Users/ram/code/research/valuation_system
python3 -c "
from agents.sector_analyst import SectorAnalyst
import asyncio

async def test():
    analyst = SectorAnalyst()

    # Test new sectors
    test_cases = [
        ('BEL', 'capital_goods_defense'),
        ('VBL', 'fmcg'),
        ('PGEL', 'consumer_durables'),
        ('AMBUJACEM', 'construction_materials'),
        ('OBEROIRLTY', 'realty'),
        ('INDHOTEL', 'hospitality')
    ]

    for ticker, expected_sector in test_cases:
        result = await analyst.analyze(ticker)
        print(f'{ticker:12s} -> Sector: {result.get(\"sector\", \"N/A\")}')

asyncio.run(test())
"
```

### 2. Test Financial Processor
```bash
cd /Users/ram/code/research/valuation_system
python3 -c "
from models.financial_processor import FinancialProcessor

processor = FinancialProcessor()

test_companies = ['BEL', 'VBL', 'PGEL', 'AMBUJACEM', 'OBEROIRLTY', 'INDHOTEL']

for ticker in test_companies:
    metrics = processor.compute_ttm_metrics(ticker)
    print(f'{ticker:12s} -> Revenue: {metrics.get(\"revenue_ttm\", 0):,.0f} Cr')
"
```

### 3. Validate DCF Model
```bash
cd /Users/ram/code/research/valuation_system
python3 -c "
from models.dcf_model import DCFModel

dcf = DCFModel()

test_cases = [
    ('BEL', 'capital_goods_defense'),
    ('INDHOTEL', 'hospitality')
]

for ticker, sector in test_cases:
    result = dcf.calculate(ticker, sector)
    print(f'{ticker:12s} -> Fair Value: ₹{result.get(\"fair_value\", 0):,.2f}')
"
```

## Sector-Specific Data Requirements

### Data Extraction Needed

After integration, ensure these metrics are available from company filings:

#### Capital Goods (Defense)
- `order_book` (Rs Cr) - quarterly
- `order_execution_rate` (%) - quarterly
- `r_and_d_spend` (% of revenue) - annual

#### FMCG
- `volume_growth` (%) - quarterly
- `realization_growth` (%) - quarterly
- `distribution_outlets` (count) - annual

#### Consumer Durables
- `channel_mix` (online/offline/institutional %) - quarterly
- `inventory_days` (days) - quarterly
- `warranty_costs` (% of revenue) - annual

#### Construction Materials
- `capacity_utilization` (%) - quarterly
- `realization_per_ton` (Rs/Ton) - quarterly
- `power_fuel_cost_per_ton` (Rs/Ton) - quarterly

#### Realty
- `presales` (Rs Cr) - quarterly
- `collections` (Rs Cr, % of presales) - quarterly
- `land_bank` (million sq ft) - annual

#### Hospitality
- `revpar` (Rs) - quarterly
- `occupancy_rate` (%) - quarterly
- `arr` (average room rate, Rs) - quarterly

## Rollback Instructions

If issues arise:

```bash
cd /Users/ram/code/research/valuation_system/config

# Restore backup
cp sectors.yaml.backup sectors.yaml

# Or manually deactivate sectors
python3 << 'EOF'
import yaml

with open('sectors.yaml', 'r') as f:
    config = yaml.safe_load(f)

# Deactivate new sectors
new_sectors = ['capital_goods_defense', 'fmcg', 'consumer_durables',
               'construction_materials', 'realty', 'hospitality']

for sector in new_sectors:
    if sector in config['sectors']:
        config['sectors'][sector]['is_active'] = False

with open('sectors.yaml', 'w') as f:
    yaml.dump(config, f, default_flow_style=False, sort_keys=False)

print("✓ Sectors deactivated. System reverted to previous state.")
EOF
```

## Next Steps

1. **Merge sectors** into main `sectors.yaml`
2. **Run validation** to ensure no YAML syntax errors
3. **Test with pilot companies** (8 companies across 6 sectors)
4. **Extract sector-specific metrics** from company filings
5. **Run full valuation pipeline** for each pilot company
6. **Review DCF outputs** for reasonableness
7. **Calibrate terminal assumptions** if needed based on actuals
8. **Document learnings** in memory files

## Support

For questions or issues:
- Check `SECTOR_DRIVERS_SUMMARY.md` for detailed driver rationale
- Review `valuation_system_requirements.md` for architecture context
- Refer to `CLAUDE.md` and `MEMORY.md` for project conventions

---

**Status:** Ready for integration
**Created:** 2026-02-06
**Version:** 1.0
