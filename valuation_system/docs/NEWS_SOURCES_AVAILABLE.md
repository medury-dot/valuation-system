# Available News Sources for Integration

## Currently Integrated (4 sources)
1. **Moneycontrol** - Web scraping (current front page only)
2. **Economic Times** - Web scraping (current front page only)
3. **Business Standard** - Web scraping (current front page only)
4. **Google News RSS** - RSS feed (24-48h history, company/sector specific)

## Recommended Additions

### Free/Low-Cost Sources

#### Indian Financial News (RSS/API)
1. **Mint (Livemint)** - RSS feeds available
   - URL: `https://www.livemint.com/rss/companies`
   - URL: `https://www.livemint.com/rss/markets`
   - Coverage: 24-48h history
   - Cost: Free

2. **Financial Express** - RSS feeds
   - URL: `https://www.financialexpress.com/market/stock-market/feed/`
   - URL: `https://www.financialexpress.com/industry/feed/`
   - Coverage: 24-48h history
   - Cost: Free

3. **NSE India Press Releases** - RSS/Web scraping
   - URL: `https://www.nseindia.com/companies-listing/corporate-filings-announcements`
   - Coverage: Real-time corporate announcements
   - Cost: Free (already have NSE API access)

4. **BSE Announcements** - API available
   - URL: `https://www.bseindia.com/corporates/ann.html`
   - Coverage: Real-time corporate filings
   - Cost: Free

5. **SEBI Press Releases** - RSS available
   - URL: `https://www.sebi.gov.in/sebiweb/other/OtherAction.do?doRssFeeds=yes&type=PRESS_RELEASE`
   - Coverage: Regulatory announcements
   - Cost: Free

#### International Sources (India coverage)

6. **Reuters India Business** - RSS
   - URL: `https://www.reutersagency.com/feed/?taxonomy=best-topics&post_type=best`
   - Coverage: Major Indian companies, macro
   - Cost: Free (basic RSS)

7. **Bloomberg India** - Web scraping (no free API)
   - URL: `https://www.bloomberg.com/asia`
   - Coverage: Premium content, limited without subscription
   - Cost: Free scraping (rate limited)

8. **NewsAPI.org** - Aggregator API
   - URL: `https://newsapi.org/`
   - Coverage: 1 month history, 100+ sources
   - Cost: Free tier 100 requests/day, $449/mo for production
   - **Best for historical search**

### Premium Sources (Paid)

9. **Alpha Vantage News Sentiment API**
   - URL: `https://www.alphavantage.co/documentation/#news-sentiment`
   - Coverage: Real-time + historical, sentiment scores
   - Cost: Free tier 25 requests/day, $49/mo for 500/day
   - **Includes AI sentiment analysis**

10. **Finnhub News API**
    - URL: `https://finnhub.io/docs/api/company-news`
    - Coverage: Company-specific news, 1 year history
    - Cost: Free tier 60 requests/min, $39/mo unlimited
    - **Good for company-specific historical search**

11. **Benzinga News API**
    - URL: `https://www.benzinga.com/apis/en/news`
    - Coverage: Real-time news + earnings calendars
    - Cost: $100-500/mo depending on volume
    - **Includes earnings call transcripts**

12. **RapidAPI Financial News**
    - URL: `https://rapidapi.com/category/Finance`
    - Multiple providers (Mboum Finance, Yahoo Finance, etc.)
    - Cost: Varies by provider, typically $10-100/mo

### Social Media / Alternative Data

13. **Twitter API v2** - Already have capability via existing code
    - Search for company tickers, cashtags
    - Coverage: Real-time
    - Cost: Free tier limited, $100/mo for production

14. **Reddit Finance Subreddits** - RSS available
    - r/IndiaInvestments, r/StockMarket, r/Indiainvestments
    - URL: `https://www.reddit.com/r/IndiaInvestments/.rss`
    - Coverage: Community sentiment
    - Cost: Free

15. **Seeking Alpha** - Web scraping
    - Company-specific analysis
    - Coverage: Premium analysis, limited free
    - Cost: Free scraping or $239/yr subscription

## Integration Priority (Recommended Order)

### Phase 1: Free RSS (Easy Integration)
1. ✅ Mint RSS feeds - 30 min
2. ✅ Financial Express RSS - 30 min
3. ✅ NSE/BSE announcements - 1 hour
4. ✅ SEBI press releases - 30 min
5. ✅ Reddit finance RSS - 30 min

### Phase 2: Free APIs (Medium Complexity)
6. ✅ NewsAPI.org free tier - 2 hours (best historical search)
7. ✅ Alpha Vantage free tier - 1 hour (sentiment analysis)
8. ✅ Reuters RSS - 30 min

### Phase 3: Paid Upgrades (If Needed)
9. ⏳ Upgrade NewsAPI to production ($449/mo) - unlimited historical
10. ⏳ Finnhub paid tier ($39/mo) - company-specific deep search
11. ⏳ Benzinga ($100/mo) - earnings transcripts

## Technical Notes

### RSS Sources
- Easy to integrate (already have RSS parser)
- 24-48h history typical
- Free, no rate limits
- Limited metadata (no sentiment, categorization)

### API Sources
- Require API keys (add to .env)
- Rate limits vary
- Better metadata (sentiment, topics, entities)
- Historical search capability

### Web Scraping
- No API key needed
- Risk of being blocked if too aggressive
- Requires maintenance (sites change layout)
- No historical data

## Recommended Immediate Additions

**Top 5 to add this week:**
1. **Mint RSS** - Broad Indian market coverage
2. **NewsAPI.org** - Historical search capability (1 month free)
3. **NSE Corporate Announcements** - High-quality, source material
4. **Alpha Vantage** - Free sentiment scores
5. **Reddit IndiaInvestments** - Retail sentiment gauge
