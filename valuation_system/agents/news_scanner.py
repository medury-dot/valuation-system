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
import re
import hashlib
import logging
from datetime import datetime, timedelta
from email.utils import parsedate_to_datetime
from typing import Optional

import requests
from bs4 import BeautifulSoup
from dotenv import load_dotenv

from valuation_system.utils.llm_client import LLMClient
from valuation_system.utils.resilience import (
    RunStateManager, GracefulDegradation,
    retry_with_backoff, check_internet, safe_task_run
)
from valuation_system.utils.structured_logger import StructuredLogger

logger = logging.getLogger(__name__)

load_dotenv(os.path.join(os.path.dirname(__file__), '..', 'config', '.env'))

# ---------------------------------------------------------------------------
# Semantic dedup helpers (module-level)
# ---------------------------------------------------------------------------
_STOPWORDS = {'the', 'a', 'an', 'is', 'are', 'was', 'were', 'in', 'on', 'at', 'to', 'for', 'of',
              'and', 'or', 'but', 'its', 'it', 'has', 'had', 'by', 'as', 'with', 'from', 'this',
              'that', 'will', 'may', 'could', 'should', 'would', 'up', 'after', 'new', 'why', 'how',
              'what', 'stock', 'shares', 'share', 'market', 'india', 'company', 'companies', 'sector',
              'nse', 'bse'}


def _headline_words(headline):
    """Extract meaningful words from headline for Jaccard comparison."""
    return set(w for w in re.sub(r'[^a-z0-9\s]', '', (headline or '').lower()).split()
               if w not in _STOPWORDS and len(w) > 2)


def _jaccard(set_a, set_b):
    """Jaccard similarity between two sets."""
    if not set_a or not set_b:
        return 0.0
    return len(set_a & set_b) / len(set_a | set_b)


class NewsScannerAgent:
    """
    Multi-source news aggregation and classification.

    Sources:
    1. Moneycontrol
    2. Economic Times
    3. Business Standard
    4. Google News (sector/company specific)
    5. Microsoft Teams channel (internal research posts)

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
        # === NEW FREE RSS SOURCES ===
        {
            'name': 'mint_companies',
            'base_url': 'https://www.livemint.com/rss/companies',
            'type': 'rss',
            'priority': 1,
        },
        {
            'name': 'mint_markets',
            'base_url': 'https://www.livemint.com/rss/markets',
            'type': 'rss',
            'priority': 1,
        },
        {
            'name': 'financial_express',
            'base_url': 'https://www.financialexpress.com/market/stock-market/feed/',
            'type': 'rss',
            'priority': 1,
        },
        {
            'name': 'sebi_press_releases',
            'base_url': 'https://www.sebi.gov.in/sebiweb/other/OtherAction.do?doRssFeeds=yes&type=PRESS_RELEASE',
            'type': 'rss',
            'priority': 2,
        },
        {
            'name': 'reddit_indiainvestments',
            'base_url': 'https://www.reddit.com/r/IndiaInvestments/.rss',
            'type': 'rss',
            'priority': 3,
        },
        {
            'name': 'teams_channel',
            'base_url': '',  # Configured via env vars
            'type': 'teams',
            'priority': 3,
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
7. drivers_affected: Array of objects with driver details:
   [{{"driver": "revenue_growth", "level": "GROUP", "impact_pct": -3.0}},
    {{"driver": "cost_of_capital", "level": "MACRO", "impact_pct": +1.5}}]
   level = MACRO/GROUP/SUBGROUP/COMPANY. impact_pct = estimated % impact on that driver.
8. summary: 2-sentence summary for quick reference
9. key_data_points: Any specific numbers mentioned (revenue, %, dates)

Return as JSON."""

    def __init__(self, mysql_client, llm_client: LLMClient = None,
                 state_manager: RunStateManager = None):
        self.mysql = mysql_client
        self.llm = llm_client or LLMClient()
        self.state = state_manager or RunStateManager()
        self.degradation = GracefulDegradation()
        self.slog = StructuredLogger('NewsScannerAgent', logger, mysql_client)

        # Load watchlist
        self._watched_companies = self._load_watchlist()
        self._watched_sectors = self._load_sectors()

        # Build fast lookup set for relevance checking (lowercase symbols)
        self._watched_symbols_lower = set(s.lower() for s in self._watched_companies if s)

        # Seen headlines for dedup: load from DB on init for persistent dedup
        self._seen_headlines = self._load_seen_headlines()

    def _load_seen_headlines(self) -> set:
        """Load headline hashes from recent vs_event_timeline entries for persistent dedup."""
        try:
            # Load headlines from last 7 days to avoid re-processing
            rows = self.mysql.query(
                """SELECT headline FROM vs_event_timeline
                   WHERE event_date >= DATE_SUB(CURDATE(), INTERVAL 7 DAY)"""
            )
            seen = set()
            for r in rows:
                h = (r.get('headline') or '').strip().lower()
                if h:
                    seen.add(hashlib.md5(h.encode()).hexdigest())
            logger.info(f"Loaded {len(seen)} seen headlines from DB for dedup")
            return seen
        except Exception as e:
            logger.warning(f"Failed to load seen headlines from DB: {e}")
            return set()

    def _load_watchlist(self) -> list:
        """Load watched companies from vs_news_watchlist table."""
        try:
            # Query companies enabled for news scanning
            rows = self.mysql.query("""
                SELECT m.symbol as nse_symbol, m.name
                FROM vs_news_watchlist w
                JOIN mssdb.kbapp_marketscrip m ON w.company_id = m.marketscrip_id
                WHERE w.is_enabled = TRUE
                ORDER BY
                    CASE w.priority
                        WHEN 'HIGH' THEN 1
                        WHEN 'MEDIUM' THEN 2
                        WHEN 'LOW' THEN 3
                    END,
                    m.symbol
            """)
            symbols = [r['nse_symbol'] for r in rows if r.get('nse_symbol')]
            logger.info(f"Loaded {len(symbols)} companies from news watchlist")
            return symbols if symbols else ['AETHER', 'EICHERMOT']  # Fallback
        except Exception as e:
            logger.warning(f"Failed to load watchlist from DB: {e}", exc_info=True)
            return ['AETHER', 'EICHERMOT']

    def _load_sectors(self) -> list:
        """Load watched sectors from news watchlist companies."""
        try:
            rows = self.mysql.query("""
                SELECT DISTINCT m.sector
                FROM vs_news_watchlist w
                JOIN mssdb.kbapp_marketscrip m ON w.company_id = m.marketscrip_id
                WHERE w.is_enabled = TRUE AND m.sector IS NOT NULL AND m.sector != ''
            """)
            sectors = [r['sector'] for r in rows if r.get('sector')]
            return sectors if sectors else ['Chemicals', 'Automobile & Ancillaries']
        except Exception as e:
            logger.warning(f"Failed to load sectors from watchlist: {e}")
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
        import time
        cycle_start = time.time()
        self.slog.log_cycle_start('news_scan')

        if not check_internet():
            logger.warning("No internet available, skipping news scan")
            self.degradation.queue_operation({
                'type': 'news_scan',
                'scheduled_at': datetime.now().isoformat(),
                'reason': 'no_internet',
            })
            self.slog.log_error('no_internet', 'Internet unavailable, scan skipped')
            return []

        all_articles = []
        search_terms = self._build_search_terms()

        for source in self.NEWS_SOURCES:
            source_start = time.time()
            try:
                articles = self._scan_source(source, search_terms, catchup_hours)
                all_articles.extend(articles)
                source_elapsed = (time.time() - source_start) * 1000

                # Structured log per-source metrics
                self.slog.log_source_scan(
                    source=source['name'],
                    articles_found=len(articles),
                    significant_events=sum(1 for a in articles if a.get('severity') in ('CRITICAL', 'HIGH')),
                    elapsed_ms=source_elapsed
                )

                logger.info(f"Scanned {source['name']}: {len(articles)} articles in {source_elapsed:.0f}ms")
            except Exception as e:
                logger.error(f"Failed to scan {source['name']}: {e}", exc_info=True)
                self.slog.log_error('source_scan_failed', str(e), source=source['name'])
                continue

        # Deduplicate
        unique = self._deduplicate(all_articles)
        cycle_elapsed = (time.time() - cycle_start) * 1000

        logger.info(f"News scan complete: {len(all_articles)} raw → {len(unique)} unique")

        # Structured log cycle summary
        self.slog.log_cycle_complete(
            cycle_type='news_scan',
            elapsed_ms=cycle_elapsed,
            metrics={
                'sources_scanned': len(self.NEWS_SOURCES),
                'articles_raw': len(all_articles),
                'articles_unique': len(unique),
                'dedup_rate_pct': round((1 - len(unique) / len(all_articles)) * 100, 1) if all_articles else 0
            }
        )

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
                classified['search_query'] = article.get('search_query')  # Track which query captured this
                classified['headline'] = article.get('headline', '')
                classified['raw_content'] = article.get('content', '')[:2000]
                classified['scanned_at'] = datetime.now().isoformat()
                classified['published_at'] = article.get('published')  # RSS pubDate string or None

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
        elif source['type'] == 'teams':
            articles = self._scan_teams(catchup_hours)

        # Tag source
        for article in articles:
            article['source'] = source['name']

        return articles

    def _scan_teams(self, catchup_hours: int = None) -> list:
        """Scan Microsoft Teams channel for news/research posts.
        Uses teams_message_id for persistent dedup — only returns messages
        not already in vs_event_timeline."""
        try:
            from valuation_system.integrations.teams_channel_reader import TeamsChannelReader
        except ImportError as e:
            logger.debug(f"Teams reader import failed (requests/bs4 missing?): {e}")
            return []

        try:
            reader = TeamsChannelReader()
            if not reader.enabled:
                logger.debug("Teams reader not configured, skipping")
                return []

            hours = catchup_hours or 24
            messages = reader.read_recent_messages(hours=hours, max_messages=50)

            # Persistent dedup: skip Teams messages already stored in DB
            # We check source_url which contains the message ID
            if messages and self.mysql:
                try:
                    existing = self.mysql.query(
                        """SELECT source_url FROM vs_event_timeline
                           WHERE source = 'teams_channel'
                             AND event_date >= DATE_SUB(CURDATE(), INTERVAL 7 DAY)"""
                    )
                    existing_urls = set(r.get('source_url', '') for r in existing)
                    before = len(messages)
                    messages = [m for m in messages if m.get('url', '') not in existing_urls]
                    if before > len(messages):
                        logger.info(f"Teams dedup: {before} → {len(messages)} (skipped {before - len(messages)} already processed)")
                except Exception as e:
                    logger.debug(f"Teams dedup check failed, proceeding with all messages: {e}")

            logger.info(f"Teams channel: {len(messages)} new messages in last {hours}h")
            return messages

        except Exception as e:
            logger.error(f"Teams channel scan failed: {e}", exc_info=True)
            return []

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
                    'search_query': None,  # Direct scrape, no search query
                })

        return articles

    @retry_with_backoff(max_retries=2, base_delay=3.0, exceptions=(requests.RequestException,))
    def _scan_rss(self, source: dict, search_terms: list) -> list:
        """
        Scan RSS feeds for news articles.
        Handles both search-based (Google News) and direct feed sources (Mint, FE, etc.).
        """
        articles = []
        source_name = source.get('name', '')

        # Google News uses search terms
        if 'google_news' in source_name:
            return self._scan_google_news_rss(source, search_terms)

        # Direct RSS feeds (Mint, FE, SEBI, Reddit) - fetch entire feed
        return self._scan_direct_rss(source)

    def _scan_google_news_rss(self, source: dict, search_terms: list) -> list:
        """Scan Google News RSS with company/sector search terms."""
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
                        # Parse RSS pubDate to datetime string
                        pub_dt = None
                        if pub_date:
                            try:
                                pub_dt = parsedate_to_datetime(pub_date.get_text(strip=True))
                            except Exception:
                                pub_dt = None

                        articles.append({
                            'headline': title.get_text(strip=True),
                            'url': link.get_text(strip=True) if link else '',
                            'content': title.get_text(strip=True),
                            'published': pub_dt.strftime('%Y-%m-%d %H:%M:%S') if pub_dt else None,
                            'search_query': term,  # Track which search term captured this
                        })
            except Exception as e:
                logger.warning(f"RSS scan failed for term '{term}': {e}")
                continue

        return articles

    def _scan_direct_rss(self, source: dict) -> list:
        """
        Scan direct RSS feeds (Mint, FE, SEBI, Reddit).
        These are not search-based - we get the entire feed and filter locally.
        """
        articles = []

        try:
            response = requests.get(source['base_url'], timeout=15, headers={
                'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36'
            })
            response.raise_for_status()

            # Parse RSS/Atom feed
            soup = BeautifulSoup(response.text, 'xml')

            # Handle both RSS and Atom formats
            items = soup.find_all('item') or soup.find_all('entry')

            for item in items[:30]:  # Get up to 30 recent articles
                # Try RSS format first
                title = item.find('title')
                link = item.find('link')
                pub_date = item.find('pubDate') or item.find('published')
                desc = item.find('description') or item.find('summary')

                if title:
                    headline = title.get_text(strip=True)

                    # Get link text or href attribute
                    if link:
                        url = link.get_text(strip=True) or link.get('href', source['base_url'])
                    else:
                        url = source['base_url']

                    # Get content
                    content = headline
                    if desc:
                        content = desc.get_text(strip=True)[:500]  # First 500 chars

                    # Filter by relevance to watched companies/sectors
                    if self._is_relevant(headline):
                        pub_dt = None
                        if pub_date:
                            try:
                                pub_dt = parsedate_to_datetime(pub_date.get_text(strip=True))
                            except Exception:
                                pub_dt = None

                        articles.append({
                            'headline': headline,
                            'url': url,
                            'content': content,
                            'published': pub_dt.strftime('%Y-%m-%d %H:%M:%S') if pub_dt else None,
                            'search_query': None,  # Direct RSS feed, no search query
                        })

        except Exception as e:
            logger.warning(f"Direct RSS fetch failed for {source['name']}: {e}")

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

    # Keywords that always mark a headline as relevant (macro, sector, market)
    _RELEVANCE_KEYWORDS = {
        'chemical', 'pharma', 'agrochemical',
        'automobile', 'auto', 'eicher', 'enfield',
        'rbi', 'gdp', 'inflation', 'crude',
        'fii', 'fpi', 'nifty', 'sensex',
        'earnings', 'quarterly', 'results', 'guidance',
        'merger', 'acquisition', 'demerger', 'buyback',
        'sebi', 'regulation', 'policy', 'tariff',
        'defence', 'defense', 'infra', 'infrastructure',
        'banking', 'nbfc', 'insurance', 'fintech',
        'it', 'software', 'saas', 'digital',
        'metal', 'steel', 'cement', 'mining',
        'power', 'energy', 'solar', 'renewable',
        'capex', 'investment', 'fund', 'ipo',
    }

    def _is_relevant(self, headline: str) -> bool:
        """Quick relevance check using word-level matching against watched symbols and keywords."""
        headline_lower = headline.lower()
        # Extract words for O(1) set intersection
        words = set(headline_lower.split())

        # Check company symbols (word-level match, O(1) set intersection)
        if words & self._watched_symbols_lower:
            return True

        # Check sector/macro keywords
        if words & self._RELEVANCE_KEYWORDS:
            return True

        # Multi-word keyword checks (can't do set intersection)
        multi_word = [
            'interest rate', 'two wheeler', '2 wheeler',
            'royal enfield', 'rate cut', 'market cap',
        ]
        for kw in multi_word:
            if kw in headline_lower:
                return True

        return False

    def _deduplicate(self, articles: list) -> list:
        """Deduplicate articles by headline hash + semantic similarity.
        Pass 1: Exact MD5 dedup (existing persistent check).
        Pass 2: Semantic dedup within 24h window (not just current batch).
        Pass 3: Company-aware dedup (same company + similar headline = likely duplicate)."""
        # Pass 1: Exact MD5 dedup
        md5_unique = []
        for article in articles:
            headline = article.get('headline', '').strip().lower()
            headline_hash = hashlib.md5(headline.encode()).hexdigest()
            if headline_hash not in self._seen_headlines:
                self._seen_headlines.add(headline_hash)
                md5_unique.append(article)

        # Pass 2: Semantic dedup against last 24h of headlines (stricter threshold)
        threshold = float(os.getenv('NEWS_SEMANTIC_DEDUP_THRESHOLD', '0.25'))  # Lowered from 0.4

        # Load recent headlines from DB (last 24h) for cross-batch dedup
        recent_headlines = self._load_recent_headlines_24h()

        kept = []
        for article in md5_unique:
            words = _headline_words(article.get('headline', ''))
            is_dup = False

            # Check against current batch (fast, in-memory)
            for existing in kept:
                existing_words = _headline_words(existing.get('headline', ''))
                if _jaccard(words, existing_words) > threshold:
                    is_dup = True
                    # Keep the one with longer content
                    if len(article.get('content', '')) > len(existing.get('content', '')):
                        kept.remove(existing)
                        kept.append(article)
                    break

            # Pass 3: Check against last 24h in DB (company-aware dedup)
            if not is_dup:
                is_dup = self._is_duplicate_in_recent(article, words, recent_headlines, threshold)

            if not is_dup:
                kept.append(article)

        if len(md5_unique) != len(kept):
            logger.info(f"Semantic dedup: {len(md5_unique)} → {len(kept)} "
                        f"(removed {len(md5_unique) - len(kept)} similar)")
        return kept

    def _load_recent_headlines_24h(self) -> list:
        """Load headlines from last 24h for cross-batch semantic dedup."""
        try:
            rows = self.mysql.query("""
                SELECT headline, search_query, company_id
                FROM vs_event_timeline
                WHERE event_timestamp >= DATE_SUB(NOW(), INTERVAL 24 HOUR)
                ORDER BY event_timestamp DESC
            """)
            return rows if rows else []
        except Exception as e:
            logger.debug(f"Failed to load recent headlines for dedup: {e}")
            return []

    def _is_duplicate_in_recent(self, article: dict, article_words: set,
                                recent_headlines: list, threshold: float) -> bool:
        """
        Check if article is duplicate of any recent headline.
        Company-aware: if same company mentioned, use stricter threshold (0.20).
        """
        article_hl = article.get('headline', '').lower()

        # Extract company symbols from article headline
        article_companies = set()
        for sym in self._watched_symbols_lower:
            if sym in article_hl.split():
                article_companies.add(sym)

        for recent in recent_headlines:
            recent_words = _headline_words(recent.get('headline', ''))
            if not recent_words:
                continue

            # Company-specific dedup: stricter threshold if same company
            recent_hl = (recent.get('headline') or '').lower()
            recent_companies = set()
            for sym in self._watched_symbols_lower:
                if sym in recent_hl.split():
                    recent_companies.add(sym)

            # Same company + similar headline = likely duplicate
            if article_companies & recent_companies:  # Intersection
                company_threshold = 0.20  # Stricter for same company
                if _jaccard(article_words, recent_words) > company_threshold:
                    logger.debug(f"Company-specific dedup: '{article_hl[:60]}...' similar to "
                                f"'{recent_hl[:60]}...' (companies: {article_companies & recent_companies})")
                    return True

            # General semantic dedup
            if _jaccard(article_words, recent_words) > threshold:
                logger.debug(f"General semantic dedup: '{article_hl[:60]}...' similar to '{recent_hl[:60]}...'")
                return True

        return False

    # Valid ENUM values for vs_event_timeline
    _VALID_SCOPES = {'MACRO', 'GROUP', 'SUBGROUP', 'COMPANY'}
    _VALID_SEVERITIES = {'CRITICAL', 'HIGH', 'MEDIUM', 'LOW'}
    _VALID_EVENT_TYPES = {'NEWS', 'EARNINGS', 'MANAGEMENT_CHANGE', 'REGULATORY', 'POLICY',
                          'MACRO', 'SECTOR_DEVELOPMENT', 'COMPETITOR', 'VALUATION_UPDATE'}
    # Map LLM scope outputs to valid ENUM values
    _SCOPE_MAP = {
        'MACRO': 'MACRO', 'MARKET': 'MACRO', 'ECONOMY': 'MACRO', 'GLOBAL': 'MACRO',
        'SECTOR': 'GROUP', 'GROUP': 'GROUP', 'INDUSTRY': 'GROUP',
        'SUBGROUP': 'SUBGROUP', 'SUB_SECTOR': 'SUBGROUP',
        'COMPANY': 'COMPANY', 'STOCK': 'COMPANY', 'INDIVIDUAL': 'COMPANY',
    }

    def _normalize_scope(self, scope: str) -> str:
        """Normalize LLM-returned scope to valid ENUM value."""
        if not scope:
            return 'GROUP'
        scope_upper = scope.upper().strip()
        return self._SCOPE_MAP.get(scope_upper, 'GROUP')

    def _normalize_severity(self, severity: str) -> str:
        """Normalize LLM-returned severity to valid ENUM value."""
        if not severity:
            return 'LOW'
        sev_upper = severity.upper().strip()
        return sev_upper if sev_upper in self._VALID_SEVERITIES else 'LOW'

    def _find_semantic_group(self, headline, company_id, scope, sector, event_date):
        """Find existing event that's semantically similar (Layer 2: cross-run grouping).
        COMPANY: same company_id + same day + headline overlap
        GROUP/SUBGROUP: same scope-level + similar sector + same day + headline overlap
        MACRO: same scope + same day + headline overlap
        Returns semantic_group_id of the matching primary event, or None."""
        threshold = float(os.getenv('NEWS_SEMANTIC_DEDUP_THRESHOLD', '0.4'))
        new_words = _headline_words(headline)
        if not new_words:
            return None

        try:
            if scope == 'COMPANY' and company_id:
                existing = self.mysql.query(
                    """SELECT id, headline, semantic_group_id FROM vs_event_timeline
                       WHERE event_date = %s AND company_id = %s""",
                    (event_date, company_id))
            elif scope in ('GROUP', 'SUBGROUP') and sector:
                existing = self.mysql.query(
                    """SELECT id, headline, semantic_group_id FROM vs_event_timeline
                       WHERE event_date = %s AND scope IN ('GROUP','SUBGROUP') AND sector = %s""",
                    (event_date, sector))
            elif scope == 'MACRO':
                existing = self.mysql.query(
                    """SELECT id, headline, semantic_group_id FROM vs_event_timeline
                       WHERE event_date = %s AND scope = 'MACRO'""",
                    (event_date,))
            else:
                return None

            for row in existing:
                existing_words = _headline_words(row.get('headline', ''))
                if _jaccard(new_words, existing_words) > threshold:
                    return row.get('semantic_group_id') or row['id']
        except Exception as e:
            logger.debug(f"Semantic group lookup failed: {e}")

        return None

    def _extract_company_from_headline(self, headline: str) -> int:
        """
        Extract company_id from headline by matching company names/symbols.
        Used as fallback when scope=COMPANY but affected_companies is empty.

        Returns:
            company_id (int) if found, None otherwise
        """
        if not headline:
            return None

        try:
            # Get all companies from marketscrip (active companies only)
            companies = self.mysql.query("""
                SELECT marketscrip_id, symbol, name
                FROM mssdb.kbapp_marketscrip
                WHERE scrip_type IN ('', 'EQS')
                  AND symbol IS NOT NULL
                  AND symbol != ''
                  AND name IS NOT NULL
                ORDER BY LENGTH(name) DESC
                LIMIT 1000
            """)

            headline_upper = headline.upper()

            # Try exact symbol match first (faster)
            for company in companies:
                symbol = company.get('symbol', '')
                if symbol and symbol.upper() in headline_upper:
                    # Verify it's a word boundary (not part of another word)
                    import re
                    pattern = r'\b' + re.escape(symbol.upper()) + r'\b'
                    if re.search(pattern, headline_upper):
                        logger.debug(f"Matched company by symbol: {symbol} -> {company['marketscrip_id']}")
                        return company['marketscrip_id']

            # Try company name match (partial match, at least 60% of name)
            for company in companies:
                name = company.get('name', '')
                if not name or len(name) < 5:
                    continue

                # Extract main company name (before "Limited", "Ltd", etc.)
                name_parts = name.replace(' Limited', '').replace(' Ltd', '').replace(' Pvt', '')
                name_clean = name_parts.split()[0] if name_parts else name

                if len(name_clean) >= 5 and name_clean.upper() in headline_upper:
                    logger.debug(f"Matched company by name: {name_clean} -> {company['marketscrip_id']}")
                    return company['marketscrip_id']

        except Exception as e:
            logger.debug(f"Failed to extract company from headline: {e}")

        return None

    def _store_event(self, classified: dict):
        """Store a classified news event in MySQL with LLM metadata and semantic grouping."""
        try:
            # Find company_id if specific company affected
            company_id = None
            affected = classified.get('affected_companies', [])
            if affected and isinstance(affected, list) and len(affected) > 0:
                company = self.mysql.get_company_by_symbol(affected[0])
                if company:
                    company_id = company['id']

            # Normalize LLM outputs to valid ENUM values
            scope = self._normalize_scope(classified.get('scope', ''))

            # FALLBACK: If scope=COMPANY but no company_id, try to extract from headline
            if scope == 'COMPANY' and not company_id:
                company_id = self._extract_company_from_headline(classified.get('headline', ''))
                if company_id:
                    logger.info(f"Extracted company_id={company_id} from headline (fallback)")
                else:
                    logger.warning(f"scope=COMPANY but couldn't find company_id for: {classified.get('headline', '')[:80]}")
            severity = self._normalize_severity(classified.get('severity', ''))
            sectors = classified.get('affected_sectors', [])
            sector = sectors[0] if isinstance(sectors, list) and len(sectors) > 0 else ''
            event_date = datetime.now().date().isoformat()

            # Clamp valuation_impact_pct to valid range
            impact = classified.get('valuation_impact_pct')
            if impact is not None:
                try:
                    impact = max(-99.99, min(99.99, float(impact)))
                except (ValueError, TypeError):
                    impact = None

            new_id = self.mysql.insert('vs_event_timeline', {
                'event_date': event_date,
                'event_type': 'NEWS',
                'scope': scope,
                'sector': sector,
                'company_id': company_id,
                'headline': classified.get('headline', '')[:500],
                'summary': classified.get('summary', ''),
                'severity': severity,
                'drivers_affected': classified.get('drivers_affected'),
                'valuation_impact_pct': impact,
                'source': classified.get('source', ''),
                'source_url': classified.get('source_url', ''),
                'search_query': classified.get('search_query'),  # Track which query captured this
                'grok_synopsis': classified.get('summary', ''),
                'published_at': classified.get('published_at'),
                'llm_model': self.llm.last_call_metadata.get('model', ''),
                'llm_tokens': self.llm.last_call_metadata.get('total_tokens'),
                'processed': False,
            })

            if not new_id:
                logger.error("Failed to insert event into vs_event_timeline, no row ID returned | "
                             "headline=%s | scope=%s | severity=%s",
                             classified.get('headline', '')[:80], scope, severity)
                return

            # Layer 2: Cross-run semantic grouping
            group_id = self._find_semantic_group(
                classified.get('headline', ''), company_id, scope, sector, event_date)
            if group_id:
                self.mysql.execute(
                    "UPDATE vs_event_timeline SET semantic_group_id = %s WHERE id = %s",
                    (group_id, new_id))
                logger.debug(f"Event {new_id} grouped with semantic_group_id={group_id}")
            else:
                # Primary event — points to itself
                self.mysql.execute(
                    "UPDATE vs_event_timeline SET semantic_group_id = %s WHERE id = %s",
                    (new_id, new_id))

        except Exception as e:
            logger.error(f"Failed to store event: {e}", exc_info=True)
            # Queue for retry
            self.degradation.queue_operation({
                'type': 'store_event',
                'data': classified,
            })
