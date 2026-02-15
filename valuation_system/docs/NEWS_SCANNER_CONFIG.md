# News Scanner Configuration Guide

## Lookback Behavior

### First Run (No Previous Run)
- **Lookback period**: TODAY ONLY
- No historical data fetched
- Recommendation: Schedule hourly runs to avoid missing news

### Subsequent Runs
- **Lookback period**: Since last successful run
- **Weekend handling**: Configurable via `.env` (default: INCLUDE weekends)
- Example: If last run was Feb 10, next run on Feb 14 catches up Feb 11-14

## Configuration Settings (.env)

```bash
# News Scanner Settings
NEWS_SCAN_INTERVAL_SECONDS=3600                    # Hourly scans
NEWS_SEMANTIC_DEDUP_THRESHOLD=0.4                  # Similarity threshold for dedup
NEWS_INCLUDE_WEEKENDS=true                         # Include weekends in catchup (default: yes)
```

### Weekend Inclusion Options

| Setting | Behavior | Use Case |
|---------|----------|----------|
| `NEWS_INCLUDE_WEEKENDS=true` | Scans Sat/Sun news | **Recommended** - crypto, global news, weekend events |
| `NEWS_INCLUDE_WEEKENDS=false` | Skips Sat/Sun | Traditional equities only (markets closed) |

**Default: `true`** (includes weekends)

## Source Capabilities

### Current Sources (4)

| Source | Type | Lookback Support | Coverage |
|--------|------|-----------------|----------|
| Moneycontrol | Web scrape | ❌ Current only | Front page headlines |
| Economic Times | Web scrape | ❌ Current only | Front page headlines |
| Business Standard | Web scrape | ❌ Current only | Front page headlines |
| Google News RSS | RSS | 🟡 24-48h | Company/sector specific |

### Recommended Additions

#### Phase 1: Free RSS (Easy - 3 hours total)
1. **Mint RSS** - `https://www.livemint.com/rss/companies`
2. **Financial Express** - `https://www.financialexpress.com/market/stock-market/feed/`
3. **NSE Announcements** - Corporate filings
4. **SEBI Press Releases** - `https://www.sebi.gov.in/sebiweb/other/OtherAction.do?doRssFeeds=yes&type=PRESS_RELEASE`
5. **Reddit r/IndiaInvestments** - `https://www.reddit.com/r/IndiaInvestments/.rss`

**Benefits:**
- Free, no API keys
- 24-48h lookback
- Diverse perspectives

#### Phase 2: Free APIs (Medium - 5 hours total)
6. **NewsAPI.org** - 1 month historical search (free tier: 100 req/day)
7. **Alpha Vantage** - Sentiment analysis (free tier: 25 req/day)
8. **Finnhub** - Company news (free tier: 60 req/min)

**Benefits:**
- Historical search capability
- Metadata (sentiment, categories)
- Company-specific filtering

#### Phase 3: Paid Upgrades (If needed)
9. **NewsAPI.org Pro** - $449/mo - Unlimited historical
10. **Finnhub Premium** - $39/mo - Full company coverage
11. **Benzinga** - $100/mo - Earnings transcripts

## Deduplication

**Window**: Last 7 days of headlines
**Method**: MD5 hash of normalized headline text
**Threshold**: 0.4 Jaccard similarity for semantic dedup

## Catchup Logic Example

### Scenario: System was off Fri-Mon

```python
# Last run: Thursday Feb 13, 8 PM
# Current time: Monday Feb 17, 9 AM
# Setting: NEWS_INCLUDE_WEEKENDS=true

Missed days: [Feb 14 (Fri), Feb 15 (Sat), Feb 16 (Sun), Feb 17 (Mon)]
Catchup hours: 4 days × 24 = 96 hours

# What actually happens per source:
- Teams channel: ✅ Fetches last 96 hours of messages
- Google News RSS: 🟡 Returns ~48h max (RSS limitation)
- Web scraping: ❌ Only current front page (can't go back)
```

### Scenario: Weekends disabled

```python
# Setting: NEWS_INCLUDE_WEEKENDS=false

Missed days: [Feb 14 (Fri), Feb 17 (Mon)]  # Sat/Sun skipped
Catchup hours: 2 days × 24 = 48 hours
```

## Scheduling Options

### Option 1: Hourly (Recommended)
```bash
# Via xyOps or cron
*/1 * * * * /path/to/runner.py hourly
```
**Pros:** Never miss breaking news, minimal catchup needed
**Cons:** Higher API usage

### Option 2: 4x Daily
```bash
# 6 AM, 12 PM, 4 PM, 8 PM
0 6,12,16,20 * * * /path/to/runner.py hourly
```
**Pros:** Good coverage, lower API usage
**Cons:** Up to 6h delay on news

### Option 3: Daily
```bash
# 8 PM daily
0 20 * * * /path/to/runner.py hourly
```
**Pros:** Minimal API usage
**Cons:** 24h delay, large catchup batches

## API Rate Limits

| Source | Free Tier | Rate Limit | Cost to Upgrade |
|--------|-----------|-----------|----------------|
| NewsAPI.org | 100 req/day | 1 req/sec | $449/mo unlimited |
| Alpha Vantage | 25 req/day | 5 req/min | $49/mo 500/day |
| Finnhub | 60 req/min | 60/min | $39/mo unlimited |
| Google News RSS | Unlimited | None | Free |
| RSS feeds | Unlimited | Varies | Free |

## Next Steps

1. **Add 5 free RSS sources** (3 hours) - see NEWS_SOURCES_AVAILABLE.md
2. **Enable hourly scheduling** - max news coverage
3. **Monitor dedup rate** - should be 30-50% with good sources
4. **Upgrade to NewsAPI.org** if need historical search beyond 48h

## Testing

```bash
# Test current watchlist (AETHER only)
python valuation_system/scheduler/runner.py hourly

# Check what was found
mysql -u root rag -e "
SELECT COUNT(*), source, scope
FROM vs_event_timeline
WHERE DATE(event_timestamp) = CURDATE()
GROUP BY source, scope;"

# View latest events
mysql -u root rag -e "
SELECT headline, source, severity
FROM vs_event_timeline
ORDER BY id DESC LIMIT 10;"
```
