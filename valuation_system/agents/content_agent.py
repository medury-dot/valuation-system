"""
Content Agent
Generates daily social media posts from key insights.
Posts are queued in Google Sheet ('storm posting' / 'posts') for PM approval.
The existing post_tweets_from_gsheet.py script publishes approved posts.

Post Types:
- Sector insights (data-driven observations)
- Company highlights (earnings, events)
- Driver changes (contrarian or surprising)
- Macro linkages (connecting dots)
- Contrarian views (challenging consensus)
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


class ContentAgent:
    """
    Generate and publish social media content from valuation insights.

    Uses Grok to craft posts that are:
    - Data-driven and specific
    - Thought-provoking, occasionally contrarian
    - NO stock recommendations or price targets
    - Professional equity researcher tone
    """

    GENERATION_PROMPT = """You are a professional equity researcher creating Twitter/X posts.
Your goal is to share insights that build thought leadership.

Today's data points:

TOP EVENTS (last 24h):
{events}

DRIVER CHANGES (last 24h):
{driver_changes}

VALUATION ALERTS:
{alerts}

SECTOR OUTLOOKS:
{sector_outlooks}

STYLE GUIDELINES:
{style_guidelines}

EXAMPLE POSTS (for tone reference):
{examples}

Generate {num_posts} tweet-length posts (max 280 chars each).

Rules:
- Lead with the insight, not the data
- Use specific numbers when possible
- End with a question or perspective
- NO stock recommendations or price targets
- Be thought-provoking, contrarian when appropriate
- Include 2-3 relevant hashtags
- Each post should be self-contained

Return as JSON: {{"posts": [{{"text": "...", "category": "sector_insight|company_highlight|driver_change|macro_linkage|contrarian_view"}}]}}"""

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

    def generate_daily_posts(self) -> list:
        """
        Generate posts from last 24h data.
        Returns list of post dicts with 'text' and 'category'.
        """
        if not self.twitter_config.get('enabled', False):
            logger.info("Twitter posting disabled in config")
            return []

        # Gather data
        events = self._get_recent_events()
        driver_changes = self._get_recent_driver_changes()
        alerts = self._get_recent_alerts()
        sector_outlooks = self._get_sector_outlooks()

        # Get style guidelines and examples from config
        guidelines = '\n'.join(self.twitter_config.get('style_guidelines', []))
        examples = '\n'.join(self.twitter_config.get('example_templates', []))

        prompt = self.GENERATION_PROMPT.format(
            events=self._format_events(events),
            driver_changes=self._format_driver_changes(driver_changes),
            alerts=self._format_alerts(alerts),
            sector_outlooks=sector_outlooks,
            style_guidelines=guidelines,
            examples=examples,
            num_posts=self.max_posts,
        )

        result = self.llm.analyze_json(prompt)
        posts = result.get('posts', [])

        # Validate posts
        validated = []
        for post in posts[:self.max_posts]:
            text = post.get('text', '')
            if len(text) > 280:
                text = text[:277] + '...'
            if text:
                validated.append({
                    'text': text,
                    'category': post.get('category', 'sector_insight'),
                })

        logger.info(f"Generated {len(validated)} posts for today")
        return validated

    def publish_posts(self, posts: list) -> list:
        """
        Queue posts in Google Sheet ('storm posting' / 'posts') for PM approval.
        The existing post_tweets_from_gsheet.py script will publish approved posts.
        Returns list of queued post results.
        """
        if not self.twitter_config.get('enabled', False):
            logger.info("Social posting disabled in config")
            return posts  # Return for logging only

        queued = []
        for post in posts:
            try:
                # Queue in GSheet for PM approval
                success = self.gsheet.queue_social_post(
                    post_text=post['text'],
                    category=post.get('category', 'sector_insight'),
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
                'platform': 'twitter',
                'content': post['text'],
                'category': post.get('category', ''),
                'status': post.get('status', 'queued'),
            })
        except Exception as e:
            logger.error(f"Failed to log post to DB: {e}", exc_info=True)

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
        # This will be populated by orchestrator before calling
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
                f"- {c.get('driver_name', '')}: {c.get('old_value', '')} → {c.get('new_value', '')} "
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
