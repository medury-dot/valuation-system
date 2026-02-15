"""
Content Agent
Generates daily social media posts (Twitter + LinkedIn) from key insights.
Posts are queued in Google Sheet ('Social Posts' tab on main drivers GSheet)
for PM approval. The social_poster.py module publishes approved posts.

Voice: Ram Kalyan Medury's personal style loaded from config/prompts/ text files.

Post Types:
- Sector insights (data-driven sector/industry observations)
- Driver changes (contrarian or surprising trends)
- Macro linkages (connecting economic dots)
- Contrarian views (challenging consensus narratives)

NOTE: Avoids company-specific posts (no individual stock earnings/events).
"""

import os
import logging
from datetime import datetime

from dotenv import load_dotenv

from valuation_system.utils.llm_client import LLMClient
from valuation_system.utils.config_loader import load_companies_config
from valuation_system.storage.gsheet_client import GSheetClient

logger = logging.getLogger(__name__)

load_dotenv(os.path.join(os.path.dirname(__file__), '..', 'config', '.env'))

# Prompt file paths
PROMPT_DIR = os.path.join(os.path.dirname(__file__), '..', 'config', 'prompts')
TWITTER_PROMPT_FILE = os.path.join(PROMPT_DIR, 'twitter_news_prompt.txt')
LINKEDIN_PROMPT_FILE = os.path.join(PROMPT_DIR, 'linkedin_news_prompt.txt')


def _load_prompt_file(filepath: str) -> str:
    """Load prompt text from file. Raises if file missing."""
    if not os.path.exists(filepath):
        logger.error(f"Prompt file not found: {filepath}")
        raise FileNotFoundError(f"Prompt file not found: {filepath}")
    with open(filepath, 'r') as f:
        return f.read().strip()


class ContentAgent:
    """
    Generate and publish social media content from valuation insights.

    Uses Grok to craft posts in Ram Kalyan Medury's voice:
    - Data-driven and specific (at least one number per post)
    - Contrarian, pithy, India-first
    - NO stock recommendations or price targets
    - Prompts loaded from editable text files in config/prompts/
    """

    GENERATION_PROMPT = """Today's data points for social media content:

TOP EVENTS (last 24h):
{events}

DRIVER CHANGES (last 24h):
{driver_changes}

VALUATION ALERTS:
{alerts}

SECTOR OUTLOOKS:
{sector_outlooks}

Generate {num_posts} social media posts from the above data.

For EACH post, generate TWO versions:
1. TWITTER: A tweet under 280 characters. Pithy, one insight, one data point.
2. LINKEDIN: A 150-500 word flowing paragraph post. Personal anecdote -> data -> analysis -> closing.

Also classify each post into a category: sector_insight | macro_linkage | driver_change | contrarian_view

CRITICAL RULES:
- Lead with the insight, not the data
- Use specific numbers when possible
- NO stock recommendations or price targets
- AVOID company-specific posts (no "Company X Q3 results" or "Stock Y earnings")
- Focus on SECTOR trends, MACRO themes, MARKET patterns
- If mentioning a company, use it only as an example of a broader trend
- Be thought-provoking, contrarian when appropriate
- Each post should be self-contained

Return as JSON: {{"posts": [{{"twitter": "...", "linkedin": "...", "category": "sector_insight|macro_linkage|driver_change|contrarian_view", "headline": "short description of what this post is about"}}]}}"""

    def __init__(self, mysql_client, gsheet_client: GSheetClient = None,
                 llm_client: LLMClient = None):
        self.mysql = mysql_client
        self.gsheet = gsheet_client or GSheetClient()
        self.llm = llm_client or LLMClient()

        # Load social media config
        companies_config = load_companies_config()
        self.social_config = companies_config.get('social_media', {})
        self.twitter_config = self.social_config.get('platforms', {}).get('twitter', {})
        self.max_posts = self.twitter_config.get('max_posts_per_day', 3)

        # Load voice prompts from files
        self._twitter_voice = self._load_voice_prompt(TWITTER_PROMPT_FILE, 'twitter')
        self._linkedin_voice = self._load_voice_prompt(LINKEDIN_PROMPT_FILE, 'linkedin')

    def _load_voice_prompt(self, filepath: str, platform: str) -> str:
        """Load voice prompt from file, with fallback to empty string."""
        try:
            prompt = _load_prompt_file(filepath)
            logger.info(f"Loaded {platform} voice prompt from {filepath} ({len(prompt)} chars)")
            return prompt
        except FileNotFoundError:
            logger.warning(f"No {platform} voice prompt file found at {filepath}, using minimal prompt")
            return f"You are a financial analyst writing {platform} posts about Indian equity markets."

    def generate_daily_posts(self) -> list:
        """
        Generate posts from last 24h data.
        Returns list of post dicts with 'twitter', 'linkedin', 'category', 'headline'.
        """
        if not self.twitter_config.get('enabled', False):
            logger.info("Twitter posting disabled in config")
            return []

        # Gather data
        events = self._get_recent_events()
        driver_changes = self._get_recent_driver_changes()
        alerts = self._get_recent_alerts()
        sector_outlooks = self._get_sector_outlooks()

        # Build the combined prompt: voice guidelines + data
        system_prompt = (
            f"TWITTER VOICE GUIDELINES:\n{self._twitter_voice}\n\n"
            f"LINKEDIN VOICE GUIDELINES:\n{self._linkedin_voice}"
        )

        user_prompt = self.GENERATION_PROMPT.format(
            events=self._format_events(events),
            driver_changes=self._format_driver_changes(driver_changes),
            alerts=self._format_alerts(alerts),
            sector_outlooks=sector_outlooks,
            num_posts=self.max_posts,
        )

        # Use LLM with system + user prompts
        full_prompt = f"{system_prompt}\n\n{user_prompt}"
        result = self.llm.analyze_json(full_prompt)

        # Handle different response formats from LLM
        posts = []
        if isinstance(result, dict):
            posts = result.get('posts', [])
        elif isinstance(result, list):
            posts = result

        # Validate and normalize posts
        validated = []
        for post in posts[:self.max_posts]:
            twitter_text = post.get('twitter', post.get('text', ''))
            linkedin_text = post.get('linkedin', '')
            category = post.get('category', 'sector_insight')
            headline = post.get('headline', '')

            # Truncate twitter if needed
            if len(twitter_text) > 280:
                twitter_text = twitter_text[:277] + '...'

            if twitter_text:
                validated.append({
                    'twitter': twitter_text,
                    'linkedin': linkedin_text,
                    'category': category,
                    'headline': headline,
                })

        logger.info(f"Generated {len(validated)} dual-platform posts for today")
        return validated

    def publish_posts(self, posts: list) -> list:
        """
        Queue posts in Google Sheet ('Social Posts' tab on main drivers GSheet)
        for PM approval. social_poster.py publishes approved posts.
        Returns list of queued post results.
        """
        if not self.twitter_config.get('enabled', False):
            logger.info("Social posting disabled in config")
            return posts

        queued = []
        for post in posts:
            try:
                # Queue in GSheet for PM approval (both platforms)
                success = self.gsheet.queue_social_post(
                    twitter_text=post.get('twitter', ''),
                    linkedin_text=post.get('linkedin', ''),
                    category=post.get('category', 'sector_insight'),
                    headline=post.get('headline', ''),
                )
                post['queued'] = success
                post['status'] = 'queued' if success else 'queue_failed'

                # Log to MySQL with status=queued (not posted_at)
                self._log_post(post)
                queued.append(post)

            except Exception as e:
                logger.error(f"Failed to queue post: {e}", exc_info=True)
                post['queued'] = False
                post['status'] = 'queue_failed'
                post['error'] = str(e)
                queued.append(post)

        logger.info(f"Queued {sum(1 for p in queued if p.get('queued'))} / {len(queued)} posts to GSheet")
        return queued

    def _log_post(self, post: dict):
        """Log post to MySQL for tracking."""
        try:
            self.mysql.insert('vs_social_posts', {
                'platform': 'both',
                'content': post.get('twitter', ''),
                'linkedin_content': post.get('linkedin', ''),
                'category': post.get('category', ''),
                'status': post.get('status', 'queued'),
            })
        except Exception as e:
            # Table may not have linkedin_content column yet - fallback
            try:
                self.mysql.insert('vs_social_posts', {
                    'platform': 'both',
                    'content': post.get('twitter', ''),
                    'category': post.get('category', ''),
                    'status': post.get('status', 'queued'),
                })
            except Exception as e2:
                logger.error(f"Failed to log post to DB: {e2}", exc_info=True)

    def _get_recent_events(self) -> list:
        """Get significant events from last 24h."""
        try:
            return self.mysql.get_events_by_severity(hours=24, min_severity='MEDIUM')
        except Exception:
            return []

    def _get_recent_driver_changes(self) -> list:
        """Get driver changes from last 24h."""
        try:
            return self.mysql.get_driver_changes(hours=24)
        except Exception:
            return []

    def _get_recent_alerts(self) -> list:
        """Get valuation alerts from last 24h."""
        try:
            return self.mysql.get_recent_alerts(hours=24)
        except Exception:
            return []

    def _get_sector_outlooks(self) -> str:
        """Get current sector outlooks as text."""
        return "Sector outlooks not available"

    def _format_events(self, events: list) -> str:
        if not events:
            return "No significant events in last 24h"
        lines = []
        for e in events[:5]:
            lines.append(f"- [{e.get('severity', '')}] {e.get('headline', '')}")
        return '\n'.join(lines)

    def _format_driver_changes(self, changes: list) -> str:
        if not changes:
            return "No driver changes in last 24h"
        lines = []
        for c in changes[:5]:
            lines.append(
                f"- {c.get('driver_name', '')}: {c.get('old_value', '')} -> {c.get('new_value', '')} "
                f"({c.get('change_reason', '')})"
            )
        return '\n'.join(lines)

    def _format_alerts(self, alerts: list) -> str:
        if not alerts:
            return "No valuation alerts"
        lines = []
        for a in alerts[:3]:
            lines.append(
                f"- {a.get('company_name', '')}: {a.get('change_pct', 0):+.1f}% change"
            )
        return '\n'.join(lines)
