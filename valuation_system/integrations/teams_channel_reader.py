"""
Microsoft Teams Channel Reader
Reads messages from a Teams channel using Microsoft Graph API.
Used as a news source in NewsScannerAgent.

Requires Azure AD app with:
- ChannelMessage.Read.All (application permission)
- Team.ReadBasic.All (application permission)
- Admin consent granted

Environment variables (.env):
- AZURE_TENANT_ID
- AZURE_CLIENT_ID
- AZURE_CLIENT_SECRET
- TEAMS_TEAM_ID (groupId from channel link)
- TEAMS_CHANNEL_ID (channel ID from channel link)
"""

import os
import sys
import logging
import time
import json
import re
from datetime import datetime, timedelta, timezone

import requests

from dotenv import load_dotenv

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))

logger = logging.getLogger(__name__)

load_dotenv(os.path.join(os.path.dirname(__file__), '..', 'config', '.env'))

# Also load rootswings .env for any Teams-specific creds there
_rootswings_env = '/Users/ram/code/investment_strategies/strategies/rootswings/config/.env'
if os.path.exists(_rootswings_env):
    load_dotenv(_rootswings_env, override=False)

GRAPH_API_BASE = 'https://graph.microsoft.com/v1.0'

# State directory for Teams channel state files (one file per channel)
STATE_DIR = os.path.join(
    os.path.dirname(__file__), '..', 'data', 'state'
)


class TeamsChannelReader:
    """
    Reads messages from a Microsoft Teams channel via Graph API.

    Authentication: OAuth2 client credentials flow (app-only, no user login).
    Token is cached and refreshed automatically.
    """

    def __init__(self):
        self.tenant_id = os.getenv('AZURE_TENANT_ID', '')
        self.client_id = os.getenv('AZURE_CLIENT_ID', '')
        self.client_secret = os.getenv('AZURE_CLIENT_SECRET', '')
        self.team_id = os.getenv('TEAMS_TEAM_ID', '')
        self.channel_id = os.getenv('TEAMS_CHANNEL_ID', '')

        self.enabled = bool(
            self.tenant_id and self.client_id and self.client_secret
            and self.team_id and self.channel_id
        )

        self._access_token = None
        self._token_expiry = None

        if not self.enabled:
            logger.info("Teams channel reader disabled: Azure AD credentials not configured")
        else:
            logger.info(f"Teams channel reader configured: team={self.team_id[:8]}..., "
                        f"channel={self.channel_id[:8]}...")

    def is_available(self) -> bool:
        """Check if Teams reading is configured and credentials are valid."""
        if not self.enabled:
            return False

        try:
            self._ensure_token()
            return self._access_token is not None
        except Exception as e:
            logger.warning(f"Teams availability check failed: {e}")
            return False

    def _get_state_file_path(self) -> str:
        """
        Get channel-specific state file path.
        Uses last 8 chars of channel_id for filename uniqueness.
        """
        # Extract last 8 chars of channel ID for readability
        channel_suffix = self.channel_id[-8:] if len(self.channel_id) >= 8 else self.channel_id
        filename = f"teams_last_scan_{channel_suffix}.json"
        return os.path.join(STATE_DIR, filename)

    def _get_last_scan_time(self) -> datetime:
        """Get the last successful scan timestamp from channel-specific state file."""
        state_file = self._get_state_file_path()

        if not os.path.exists(state_file):
            # First run: default to 24 hours ago
            logger.debug(f"No state file for this channel yet: {state_file}")
            return datetime.now(timezone.utc) - timedelta(hours=24)

        try:
            with open(state_file, 'r') as f:
                state = json.load(f)
                last_scan_str = state.get('last_scan_timestamp')
                if last_scan_str:
                    return datetime.fromisoformat(last_scan_str)
        except (json.JSONDecodeError, ValueError, OSError) as e:
            logger.debug(f"Failed to read Teams state file: {e}")

        # Fallback to 24h ago
        return datetime.now(timezone.utc) - timedelta(hours=24)

    def _save_last_scan_time(self, timestamp: datetime):
        """Save the last successful scan timestamp to channel-specific state file."""
        state_file = self._get_state_file_path()

        try:
            os.makedirs(os.path.dirname(state_file), exist_ok=True)
            state = {
                'last_scan_timestamp': timestamp.isoformat(),
                'last_scan_date_human': timestamp.strftime('%Y-%m-%d %H:%M:%S %Z'),
                'channel_id': self.channel_id,
                'team_id': self.team_id,
            }
            with open(state_file, 'w') as f:
                json.dump(state, f, indent=2)
            logger.debug(f"Saved Teams last scan time for channel {self.channel_id[-8:]}: {timestamp.isoformat()}")
        except OSError as e:
            logger.warning(f"Failed to save Teams state file: {e}")

    def _extract_urls(self, text: str) -> list:
        """Extract URLs from text using regex."""
        url_pattern = r'https?://[^\s<>"{}|\\^`\[\]]+'
        return re.findall(url_pattern, text)

    def _fetch_link_content(self, url: str) -> str:
        """Fetch content from a URL (news article, etc.). Returns text or empty string."""
        try:
            response = requests.get(url, timeout=10, headers={
                'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) '
                              'AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
            })
            if response.status_code == 200:
                from bs4 import BeautifulSoup
                soup = BeautifulSoup(response.text, 'html.parser')

                # Remove script and style elements
                for script in soup(["script", "style"]):
                    script.decompose()

                # Get text
                text = soup.get_text(separator=' ', strip=True)
                return text[:5000]  # Limit to 5000 chars
            else:
                logger.debug(f"Failed to fetch {url}: HTTP {response.status_code}")
                return ''
        except Exception as e:
            logger.debug(f"Failed to fetch link content from {url}: {e}")
            return ''

    def read_recent_messages(self, hours: int = 24, max_messages: int = 50) -> list:
        """
        Read recent messages from the configured Teams channel.
        Uses incremental fetching — only processes messages after last scan time.

        Args:
            hours: Maximum look-back window (default 24), but will use last scan time if more recent
            max_messages: Maximum messages to return (default 50)

        Returns:
            List of dicts with keys: headline, content, source, url, published, author
        """
        if not self.enabled:
            logger.debug("Teams reader not enabled, returning empty list")
            return []

        try:
            self._ensure_token()
        except Exception as e:
            logger.error(f"Failed to authenticate with Azure AD: {e}", exc_info=True)
            return []

        articles = []

        # INCREMENTAL: Use last scan time, or fallback to hours parameter
        last_scan = self._get_last_scan_time()
        max_lookback = datetime.now(timezone.utc) - timedelta(hours=hours)
        since = max(last_scan, max_lookback)  # Don't go back further than hours param

        logger.info(f"Teams incremental scan: since {since.strftime('%Y-%m-%d %H:%M:%S UTC')} "
                    f"(last scan: {last_scan.strftime('%Y-%m-%d %H:%M:%S UTC')})")

        try:
            # Graph API: List channel messages
            # https://learn.microsoft.com/en-us/graph/api/channel-list-messages
            url = (f"{GRAPH_API_BASE}/teams/{self.team_id}"
                   f"/channels/{self.channel_id}/messages")

            headers = {
                'Authorization': f'Bearer {self._access_token}',
                'Content-Type': 'application/json',
            }

            # Note: Graph API channel messages does NOT support $orderby or $filter.
            # Messages are returned newest-first by default.
            params = {
                '$top': str(max_messages),
            }

            response = requests.get(url, headers=headers, params=params, timeout=30)

            if response.status_code == 403:
                logger.error("Teams API 403 Forbidden — check that ChannelMessage.Read.All "
                             "permission is granted and admin consented")
                return []

            if response.status_code != 200:
                logger.error(f"Teams API error {response.status_code}: {response.text[:500]}")
                return []

            data = response.json()
            messages = data.get('value', [])

            logger.info(f"Teams API returned {len(messages)} raw messages from channel")

            # Track which messages are NEW (created after last scan)
            # Only fetch replies for NEW messages to avoid redundant API calls
            new_message_ids = set()
            latest_timestamp = since  # Track latest message time for state update

            for msg in messages:
                created_str = msg.get('createdDateTime', '')
                if not created_str:
                    continue

                # Parse ISO datetime
                try:
                    created_dt = datetime.fromisoformat(created_str.replace('Z', '+00:00'))
                except (ValueError, TypeError):
                    continue

                # Filter by time window
                if created_dt < since:
                    continue

                # Track latest timestamp for state update
                if created_dt > latest_timestamp:
                    latest_timestamp = created_dt

                # Mark as new message
                msg_id = msg.get('id', '')
                if msg_id:
                    new_message_ids.add(msg_id)

                # Extract message body
                body = msg.get('body', {})
                content_type = body.get('contentType', 'text')
                raw_content = body.get('content', '')

                # Extract URLs from raw HTML/text BEFORE stripping tags
                urls = self._extract_urls(raw_content)

                # Strip HTML tags if HTML content
                content = raw_content
                if content_type == 'html' and raw_content:
                    from bs4 import BeautifulSoup
                    soup = BeautifulSoup(raw_content, 'html.parser')
                    content = soup.get_text(separator=' ', strip=True)

                if not content or len(content.strip()) < 10:
                    continue

                # FILTER: Only include messages with >20 words OR that have links
                word_count = len(content.split())
                if word_count < 20 and not urls:
                    logger.debug(f"Skipping short message ({word_count} words, no links): {content[:50]}")
                    continue

                # If message has links, fetch the first link's content
                linked_content = ''
                primary_url = ''
                if urls:
                    primary_url = urls[0]  # Use first URL as primary source
                    logger.debug(f"Fetching linked content from {primary_url}")
                    linked_content = self._fetch_link_content(primary_url)
                    if linked_content:
                        # Append linked content to message content
                        content = f"{content}\n\n[Linked article content:]\n{linked_content}"

                # Extract author
                author_name = ''
                from_field = msg.get('from', {})
                if from_field:
                    user = from_field.get('user', {})
                    author_name = user.get('displayName', '')

                # Build headline: first line or first 100 chars
                lines = content.strip().split('\n')
                headline = lines[0].strip()
                if len(headline) > 150:
                    headline = headline[:147] + '...'

                # Build article dict (same format as other news sources)
                # Use linked URL if available, otherwise Teams message link
                article_url = primary_url if primary_url else f"https://teams.microsoft.com/l/message/{self.channel_id}/{msg.get('id', '')}"

                article = {
                    'headline': headline,
                    'content': content[:5000],  # Increased limit to accommodate linked content
                    'source': 'teams_channel',
                    'url': article_url,
                    'published': created_str,
                    'author': author_name,
                    'teams_message_id': msg.get('id', ''),
                }
                articles.append(article)

                logger.debug(f"Teams message: [{author_name}] {headline[:80]} "
                            f"({word_count} words{', +link' if primary_url else ''})")

            # Summary: no reply fetching (user requested to skip replies)
            logger.info(f"Teams channel scan: {len(articles)} NEW messages "
                        f"(since {since.strftime('%Y-%m-%d %H:%M:%S')}), "
                        f"{sum(1 for a in articles if 'Linked article' in a['content'])} with fetched links")

            # Save state: update last scan time to latest message timestamp
            # This ensures next run only fetches messages after this point
            if articles and latest_timestamp > since:
                self._save_last_scan_time(latest_timestamp)
                logger.info(f"Updated Teams last scan time to {latest_timestamp.strftime('%Y-%m-%d %H:%M:%S UTC')}")

        except requests.RequestException as e:
            logger.error(f"Teams API request failed: {e}", exc_info=True)
        except Exception as e:
            logger.error(f"Teams channel read failed: {e}", exc_info=True)

        return articles

    def _ensure_token(self):
        """Get or refresh OAuth2 access token using client credentials flow."""
        now = datetime.now(timezone.utc)

        # Reuse cached token if still valid (with 5-min buffer)
        if (self._access_token and self._token_expiry
                and now < self._token_expiry - timedelta(minutes=5)):
            return

        token_url = f"https://login.microsoftonline.com/{self.tenant_id}/oauth2/v2.0/token"

        data = {
            'client_id': self.client_id,
            'client_secret': self.client_secret,
            'scope': 'https://graph.microsoft.com/.default',
            'grant_type': 'client_credentials',
        }

        response = requests.post(token_url, data=data, timeout=15)

        if response.status_code != 200:
            error_detail = response.json().get('error_description', response.text[:300])
            raise RuntimeError(f"Azure AD token request failed ({response.status_code}): {error_detail}")

        token_data = response.json()
        self._access_token = token_data['access_token']
        expires_in = int(token_data.get('expires_in', 3600))
        self._token_expiry = now + timedelta(seconds=expires_in)

        logger.info(f"Azure AD token acquired, expires in {expires_in}s")


if __name__ == '__main__':
    """Quick test: read recent messages from configured channel."""
    logging.basicConfig(
        level=logging.DEBUG,
        format='%(asctime)s [%(levelname)s] %(name)s: %(message)s'
    )

    reader = TeamsChannelReader()

    if not reader.enabled:
        print("Teams reader not configured. Set these in .env:")
        print("  AZURE_TENANT_ID=...")
        print("  AZURE_CLIENT_ID=...")
        print("  AZURE_CLIENT_SECRET=...")
        print("  TEAMS_TEAM_ID=...")
        print("  TEAMS_CHANNEL_ID=...")
        sys.exit(1)

    print(f"Available: {reader.is_available()}")

    messages = reader.read_recent_messages(hours=48)
    print(f"\nFound {len(messages)} messages in last 48h:")
    for m in messages[:10]:
        print(f"  [{m.get('author', '')}] {m['headline'][:100]}")
        print(f"    Published: {m['published']}")
        print()
