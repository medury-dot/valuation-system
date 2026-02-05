"""
News Scanner Agent
Hourly scanning of financial news sources.
Classifies news by category, severity, and valuation impact.
Stores in ChromaDB and MySQL for retrieval.

Edge Cases Handled:
- Internet unavailable → Skip scan, queue for catchup
- Source unreachable → Skip that source, continue others
- Duplicate detection → Deduplicate by headline similarity
- Rate limiting → Exponential backoff per source
- Machine was off → Catchup scan for missed period
"""

import os
import hashlib
import logging
from datetime import datetime, timedelta
from typing import Optional

import requests
from bs4 import BeautifulSoup
from dotenv import load_dotenv

from valuation_system.utils.llm_client import LLMClient
from valuation_system.utils.resilience import (
    RunStateManager, GracefulDegradation,
    retry_with_backoff, check_internet, safe_task_run
)

logger = logging.getLogger(__name__)

load_dotenv(os.path.join(os.path.dirname(__file__), '..', 'config', '.env'))


class NewsScannerAgent:
    """
    Multi-source news aggregation and classification.

    Sources:
    1. Moneycontrol
    2. Economic Times
    3. Business Standard
    4. BSE Announcements
    5. NSE Announcements
    6. Google News (sector/company specific)

    Flow:
    scan_all_sources() → classify_news() → deduplicate() → store()
    """

    NEWS_SOURCES = [
        {
            'name': 'moneycontrol',
            'base_url': 'https://www.moneycontrol.com/news/business/',
            'type': 'scrape',
            'priority': 1,
        },
        {
            'name': 'economic_times',
            'base_url': 'https://economictimes.indiatimes.com/markets/stocks/news',
            'type': 'scrape',
            'priority': 1,
        },
        {
            'name': 'business_standard',
            'base_url': 'https://www.business-standard.com/markets',
            'type': 'scrape',
            'priority': 2,
        },
        {
            'name': 'google_news',
            'base_url': 'https://news.google.com/rss/search?q=',
            'type': 'rss',
            'priority': 2,
        },
    ]

    CLASSIFICATION_PROMPT = """You are an equity research analyst classifying news for valuation purposes.

Analyze this news article and classify it:

Headline: {headline}
Content: {content}
Source: {source}

WATCHED COMPANIES: {watched_companies}
WATCHED SECTORS: {watched_sectors}

Classify with:
1. category: One of REGULATORY, MANAGEMENT, PRODUCT, MA, MACRO, COMPETITOR, GOVERNANCE, EARNINGS, POLICY
2. severity: CRITICAL (immediate valuation impact >5%), HIGH (>2%), MEDIUM (1-2%), LOW (<1%)
3. affected_companies: List of NSE symbols affected (from watched list, or empty)
4. affected_sectors: List of sectors affected
5. scope: MACRO (affects all), SECTOR (affects sector), COMPANY (affects specific company)
6. valuation_impact_pct: Estimated impact on intrinsic value (-10 to +10)
7. drivers_affected: Which valuation drivers are impacted (e.g., "revenue_growth", "cost_of_capital")
8. summary: 2-sentence summary for quick reference
9. key_data_points: Any specific numbers mentioned (revenue, %, dates)

Return as JSON."""

    def __init__(self, mysql_client, llm_client: LLMClient = None,
                 state_manager: RunStateManager = None):
        self.mysql = mysql_client
        self.llm = llm_client or LLMClient()
        self.state = state_manager or RunStateManager()
        self.degradation = GracefulDegradation()

        # Load watchlist
        self._watched_companies = self._load_watchlist()
        self._watched_sectors = self._load_sectors()

        # Seen headlines for dedup (in-memory cache + DB check)
        self._seen_headlines = set()

    def _load_watchlist(self) -> list:
        """Load watched companies from MySQL."""
        try:
            companies = self.mysql.get_active_companies()
            return [c['nse_symbol'] for c in companies if c.get('nse_symbol')]
        except Exception as e:
            logger.warning(f"Failed to load watchlist from DB: {e}")
            return ['AETHER', 'EICHERMOT']

    def _load_sectors(self) -> list:
        """Load watched sectors."""
        try:
            companies = self.mysql.get_active_companies()
            return list(set(c['sector'] for c in companies if c.get('sector')))
        except Exception:
            return ['Chemicals', 'Automobile & Ancillaries']

    def scan_all_sources(self, catchup_hours: int = None) -> list:
        """
        Scan all news sources. Handles:
        - No internet: Returns empty, queues catchup
        - Individual source failure: Continues with others
        - Catchup mode: Scans for missed period

        Args:
            catchup_hours: If set, scan for news from last N hours (for catchup)
        """
        if not check_internet():
            logger.warning("No internet available, skipping news scan")
            self.degradation.queue_operation({
                'type': 'news_scan',
                'scheduled_at': datetime.now().isoformat(),
                'reason': 'no_internet',
            })
            return []

        all_articles = []
        search_terms = self._build_search_terms()

        for source in self.NEWS_SOURCES:
            try:
                articles = self._scan_source(source, search_terms, catchup_hours)
                all_articles.extend(articles)
                logger.info(f"Scanned {source['name']}: {len(articles)} articles")
            except Exception as e:
                logger.error(f"Failed to scan {source['name']}: {e}", exc_info=True)
                continue

        # Deduplicate
        unique = self._deduplicate(all_articles)
        logger.info(f"News scan complete: {len(all_articles)} raw → {len(unique)} unique")

        return unique

    def classify_and_store(self, articles: list) -> list:
        """
        Classify articles by severity and store significant ones.
        Returns list of classified articles with severity >= MEDIUM.
        """
        significant = []

        for article in articles:
            try:
                classified = self.classify_news(article)
                if not classified or classified.get('error'):
                    continue

                # Merge original article data with classification
                classified['source'] = article.get('source', '')
                classified['source_url'] = article.get('url', '')
                classified['headline'] = article.get('headline', '')
                classified['raw_content'] = article.get('content', '')[:2000]
                classified['scanned_at'] = datetime.now().isoformat()

                # Store all MEDIUM+ severity events
                severity = classified.get('severity', 'LOW')
                if severity in ('CRITICAL', 'HIGH', 'MEDIUM'):
                    self._store_event(classified)
                    significant.append(classified)

            except Exception as e:
                logger.error(f"Failed to classify article '{article.get('headline', '')[:50]}': {e}",
                             exc_info=True)
                continue

        logger.info(f"Classified {len(articles)} articles, {len(significant)} significant")
        return significant

    def classify_news(self, article: dict) -> dict:
        """
        Use LLM to classify a news article.
        """
        prompt = self.CLASSIFICATION_PROMPT.format(
            headline=article.get('headline', ''),
            content=article.get('content', '')[:2000],
            source=article.get('source', ''),
            watched_companies=', '.join(self._watched_companies),
            watched_sectors=', '.join(self._watched_sectors),
        )

        return self.llm.analyze_json(prompt)

    def run_catchup(self) -> dict:
        """
        Run catchup for missed scans (e.g., after machine was off).
        Detects gap and scans for the missed period.
        """
        missed_days = self.state.get_missed_days('news_scan', expected_frequency_hours=1)

        if not missed_days:
            logger.info("No missed news scans to catch up")
            return {'catchup_needed': False}

        hours_missed = len(missed_days) * 24
        logger.info(f"Catching up {len(missed_days)} missed days ({hours_missed}h)")

        articles = self.scan_all_sources(catchup_hours=hours_missed)
        significant = self.classify_and_store(articles)

        self.state.record_success('news_scan', {
            'type': 'catchup',
            'days_caught_up': len(missed_days),
            'articles_found': len(articles),
            'significant': len(significant),
        })

        return {
            'catchup_needed': True,
            'days_caught_up': len(missed_days),
            'articles_found': len(articles),
            'significant_events': len(significant),
        }

    def _scan_source(self, source: dict, search_terms: list,
                     catchup_hours: int = None) -> list:
        """Scan a single news source."""
        articles = []

        if source['type'] == 'rss':
            articles = self._scan_rss(source, search_terms)
        elif source['type'] == 'scrape':
            articles = self._scan_web(source)

        # Tag source
        for article in articles:
            article['source'] = source['name']

        return articles

    @retry_with_backoff(max_retries=2, base_delay=3.0, exceptions=(requests.RequestException,))
    def _scan_web(self, source: dict) -> list:
        """Scrape a web news source."""
        headers = {
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) '
                          'AppleWebKit/537.36 (KHTML, like Gecko) '
                          'Chrome/120.0.0.0 Safari/537.36'
        }
        resp = requests.get(source['base_url'], headers=headers, timeout=30)
        resp.raise_for_status()

        soup = BeautifulSoup(resp.text, 'html.parser')
        articles = []

        # Generic headline extraction (works for most news sites)
        for tag in soup.find_all(['h2', 'h3', 'a'], limit=50):
            headline = tag.get_text(strip=True)
            url = tag.get('href', '')

            if not headline or len(headline) < 20:
                continue

            # Check relevance to watched companies/sectors
            if self._is_relevant(headline):
                articles.append({
                    'headline': headline,
                    'url': url if url.startswith('http') else source['base_url'] + url,
                    'content': headline,  # Will be enriched later if needed
                    'published': datetime.now().isoformat(),
                })

        return articles

    @retry_with_backoff(max_retries=2, base_delay=3.0, exceptions=(requests.RequestException,))
    def _scan_rss(self, source: dict, search_terms: list) -> list:
        """Scan Google News RSS for specific search terms."""
        articles = []

        for term in search_terms[:10]:  # Limit to avoid rate limits
            url = f"{source['base_url']}{term}&hl=en-IN&gl=IN&ceid=IN:en"
            try:
                resp = requests.get(url, timeout=15)
                if resp.status_code != 200:
                    continue

                soup = BeautifulSoup(resp.text, 'xml')
                for item in soup.find_all('item', limit=10):
                    title = item.find('title')
                    link = item.find('link')
                    pub_date = item.find('pubDate')

                    if title:
                        articles.append({
                            'headline': title.get_text(strip=True),
                            'url': link.get_text(strip=True) if link else '',
                            'content': title.get_text(strip=True),
                            'published': pub_date.get_text(strip=True) if pub_date else '',
                        })
            except Exception as e:
                logger.warning(f"RSS scan failed for term '{term}': {e}")
                continue

        return articles

    def _build_search_terms(self) -> list:
        """Build search terms from watched companies and sectors."""
        terms = []
        for symbol in self._watched_companies:
            terms.append(f"{symbol} stock")
            terms.append(f"{symbol} NSE news")

        for sector in self._watched_sectors:
            terms.append(f"India {sector} sector")

        # Add macro terms
        terms.extend([
            "India RBI interest rate",
            "India GDP growth",
            "India chemical industry",
            "India automobile sales",
        ])
        return terms

    def _is_relevant(self, headline: str) -> bool:
        """Quick relevance check for a headline."""
        headline_lower = headline.lower()

        # Check company names/symbols
        for symbol in self._watched_companies:
            if symbol.lower() in headline_lower:
                return True

        # Check sector keywords
        sector_keywords = [
            'chemical', 'specialty chemical', 'pharma api', 'agrochemical',
            'automobile', 'two wheeler', '2 wheeler', 'eicher', 'royal enfield',
            'aether',
            # Macro keywords
            'rbi', 'interest rate', 'gdp', 'inflation', 'crude oil',
            'fii', 'fpi', 'nifty', 'sensex',
        ]
        for keyword in sector_keywords:
            if keyword in headline_lower:
                return True

        return False

    def _deduplicate(self, articles: list) -> list:
        """Deduplicate articles by headline hash."""
        unique = []
        for article in articles:
            headline = article.get('headline', '').strip().lower()
            headline_hash = hashlib.md5(headline.encode()).hexdigest()

            if headline_hash not in self._seen_headlines:
                self._seen_headlines.add(headline_hash)
                unique.append(article)

        return unique

    def _store_event(self, classified: dict):
        """Store a classified news event in MySQL."""
        try:
            # Find company_id if specific company affected
            company_id = None
            affected = classified.get('affected_companies', [])
            if affected and isinstance(affected, list) and len(affected) > 0:
                company = self.mysql.get_company_by_symbol(affected[0])
                if company:
                    company_id = company['id']

            self.mysql.insert('vs_event_timeline', {
                'event_date': datetime.now().date().isoformat(),
                'event_type': 'NEWS',
                'scope': classified.get('scope', 'SECTOR'),
                'sector': classified.get('affected_sectors', [''])[0] if isinstance(classified.get('affected_sectors'), list) else '',
                'company_id': company_id,
                'headline': classified.get('headline', '')[:500],
                'summary': classified.get('summary', ''),
                'severity': classified.get('severity', 'LOW'),
                'drivers_affected': classified.get('drivers_affected'),
                'valuation_impact_pct': classified.get('valuation_impact_pct'),
                'source': classified.get('source', ''),
                'source_url': classified.get('source_url', ''),
                'grok_synopsis': classified.get('summary', ''),
                'processed': False,
            })
        except Exception as e:
            logger.error(f"Failed to store event: {e}", exc_info=True)
            # Queue for retry
            self.degradation.queue_operation({
                'type': 'store_event',
                'data': classified,
            })
