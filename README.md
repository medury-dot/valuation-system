# Valuation System

An automated company valuation system that combines DCF, relative valuation, and sector analysis to generate comprehensive valuation reports.

## Features

- **Multi-Method Valuation**: DCF (60%), Relative (30%), Monte Carlo (10%)
- **Driver-Based DCF**: Macro (20%), Sector (55%), Company (25%) weighted drivers
- **Automated Data Processing**: 3-tier fallback (Actual → Derived → Default)
- **News & Sector Analysis**: LLM-powered insights with sentiment analysis
- **Scheduled Execution**: Hourly updates, daily reports, regression testing
- **Social Media Integration**: Automated insight drafts to Google Sheets

## Architecture

```
valuation_system/
├── agents/           # Orchestrator, news scanner, sector analyst, valuator, content agent
├── models/           # DCF model, relative valuation
├── data/            # Loaders (core CSV, prices, Damodaran), financial processor
├── storage/         # MySQL client, Google Sheets integration
├── notifications/   # Email sender, report generator
├── scheduler/       # Launchd jobs and runner
├── tests/           # Regression tests
└── utils/           # Config, LLM client, resilience, Excel generation
```

## Data Sources

- **Core CSV**: ~9000 Indian companies, ~4000 columns (quarterly, annual, half-yearly)
- **Monthly Prices**: Updated daily with PE, PB, PS, EV/EBITDA, market cap
- **Damodaran Data**: Country risk premiums, sector multiples, cost of capital
- **Company Master**: `mssdb.kbapp_marketscrip` (equities only)

## Setup

### Prerequisites

```bash
# Python 3.8+
pip install -r requirements.txt

# MySQL (localhost:3306, database=rag)
# ChromaDB (localhost:8001)
```

### Configuration

Create `.env` file:

```bash
# Database
MYSQL_HOST=localhost
MYSQL_PORT=3306
MYSQL_DATABASE=rag
MYSQL_USER=root
MYSQL_PASSWORD=

# ChromaDB
CHROMADB_HOST=localhost
CHROMADB_PORT=8001
CHROMADB_USER=jama_user

# Data paths
CORE_CSV_PATH=/path/to/core-all-input.csv
PRICES_CSV_PATH=/path/to/combined_monthly_prices.csv
DAMODARAN_CACHE_PATH=/path/to/damodaran_cache.json

# LLM
GROK_API_KEY=your_grok_key
GROK_MODEL=grok-2-1212

# Email
ALERT_EMAIL=your@email.com

# Google Sheets (optional, for social media)
GSHEET_DRIVERS_ID=your_sheet_id
JSONFILELOC=/path/to/auth.json
```

### Initialize Database

```bash
cd valuation_system
python -c "from storage.mysql_client import MySQLClient; MySQLClient().initialize_schema()"
```

### Install Scheduler

```bash
cd valuation_system/scheduler
./install_launchd.sh
```

## Usage

### Run Single Valuation

```python
from valuation_system.agents.orchestrator import Orchestrator

orch = Orchestrator()
result = orch.run_valuation(company_id=39424)  # Eicher Motors
print(f"Fair Value: {result['fair_value_per_share']}")
```

### Run Tests

```bash
python -m valuation_system.tests.regression_tests
```

## Key Metrics & Calculations

### DCF Model
- **Terminal Growth**: ROCE-linked (g = Reinvestment Rate × ROCE), capped 2%-5%
- **WACC**: Must exceed terminal growth (auto-adjusted if not)
- **Valuation Period**: 10 years explicit forecasts

### Relative Valuation
- **Two-Tier Peers**: Same industry (2x weight) + same sector (1x weight)
- **Multiples**: PE, PB, PS, EV/Sales (50% current, 30% median, 20% historical)
- **Caching**: 30-day peer group cache in `vs_peer_groups`

### Financial Processing
- **TTM Calculation**: Uses most recent h2+h1 when available (more current than annual)
- **Indian Fiscal Year**: FY2025 = Apr 2024 - Mar 2025
- **Case Sensitivity**: Yearly `trade_payables` vs Half-yearly `Trade_payables`

## Scheduling

- **Hourly** (0-23): News scanning, sector updates
- **Daily** (08:00 IST): Full valuation run, reports, social media drafts
- **Regression** (01:00): Test suite with email results

## Testing

30+ regression tests across 8 categories:
- Data loading & validation
- Financial calculations
- DCF model logic
- Relative valuation
- Peer selection
- Report generation
- Database operations
- Email notifications

## Database Schema

14 tables in `rag` database:
- `vs_valuations`: Blended fair values, timestamps
- `vs_dcf_outputs`: Full DCF results with drivers
- `vs_relative_valuations`: Peer multiples and methods
- `vs_news`: Scraped articles with sentiment
- `vs_sector_insights`: LLM-generated analysis
- `vs_social_media_posts`: Draft tweets awaiting approval
- `vs_peer_groups`: Cached peer selections
- `vs_run_state`: Execution tracking
- Plus 6 more for prices, metrics, assumptions, reports, errors, degradation

## Company Master

Uses `mssdb.kbapp_marketscrip`:
- Filter: `scrip_type IN ('', 'EQS')` for equities
- `marketscrip_id` = company_id in all `vs_*` tables
- No `is_active` filter (includes inactive for historical analysis)

## Pilot Companies

- **Eicher Motors** (39424): Motorcycle manufacturer
- **Aether Industries** (47582): Specialty chemicals

## Contributing

See [CLAUDE.md](CLAUDE.md) for development guidelines.

## License

MIT

## Contact

medury@gmail.com
