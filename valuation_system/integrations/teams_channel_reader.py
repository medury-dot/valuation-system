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

    def read_recent_messages(self, hours: int = 24, max_messages: int = 50) -> list:
        """
        Read recent messages from the configured Teams channel.

        Args:
            hours: Look back this many hours (default 24)
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
        since = datetime.now(timezone.utc) - timedelta(hours=hours)

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

            logger.info(f"Teams API returned {len(messages)} messages from channel")

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

                # Extract message body
                body = msg.get('body', {})
                content_type = body.get('contentType', 'text')
                content = body.get('content', '')

                # Strip HTML tags if HTML content
                if content_type == 'html' and content:
                    from bs4 import BeautifulSoup
                    soup = BeautifulSoup(content, 'html.parser')
                    content = soup.get_text(separator=' ', strip=True)

                if not content or len(content.strip()) < 10:
                    continue

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
                article = {
                    'headline': headline,
                    'content': content[:2000],
                    'source': 'teams_channel',
                    'url': f"https://teams.microsoft.com/l/message/{self.channel_id}/{msg.get('id', '')}",
                    'published': created_str,
                    'author': author_name,
                    'teams_message_id': msg.get('id', ''),
                }
                articles.append(article)

                logger.debug(f"Teams message: [{author_name}] {headline[:80]}")

            # Also check replies in threads (top-level messages may have important replies)
            # Only check replies for messages that have replies
            for msg in messages:
                if not msg.get('id'):
                    continue

                reply_count = 0
                # Check if message has replies indicator
                if msg.get('replies@odata.count', 0) > 0 or True:
                    # Fetch replies for this message
                    try:
                        replies_url = (f"{GRAPH_API_BASE}/teams/{self.team_id}"
                                       f"/channels/{self.channel_id}"
                                       f"/messages/{msg['id']}/replies")
                        replies_params = {'$top': '10', '$orderby': 'createdDateTime desc'}
                        replies_resp = requests.get(
                            replies_url, headers=headers, params=replies_params, timeout=15
                        )

                        if replies_resp.status_code == 200:
                            replies = replies_resp.json().get('value', [])
                            for reply in replies:
                                r_created = reply.get('createdDateTime', '')
                                try:
                                    r_dt = datetime.fromisoformat(r_created.replace('Z', '+00:00'))
                                except (ValueError, TypeError):
                                    continue

                                if r_dt < since:
                                    continue

                                r_body = reply.get('body', {})
                                r_content = r_body.get('content', '')

                                if r_body.get('contentType') == 'html' and r_content:
                                    from bs4 import BeautifulSoup
                                    soup = BeautifulSoup(r_content, 'html.parser')
                                    r_content = soup.get_text(separator=' ', strip=True)

                                if not r_content or len(r_content.strip()) < 10:
                                    continue

                                r_author = ''
                                r_from = reply.get('from', {})
                                if r_from:
                                    r_author = r_from.get('user', {}).get('displayName', '')

                                r_headline = r_content.strip().split('\n')[0][:150]

                                articles.append({
                                    'headline': r_headline,
                                    'content': r_content[:2000],
                                    'source': 'teams_channel',
                                    'url': f"https://teams.microsoft.com/l/message/{self.channel_id}/{reply.get('id', '')}",
                                    'published': r_created,
                                    'author': r_author,
                                    'teams_message_id': reply.get('id', ''),
                                })
                                reply_count += 1

                        # Rate limit: don't hammer the API for reply fetches
                        time.sleep(0.5)

                    except Exception as e:
                        logger.debug(f"Failed to fetch replies for message {msg['id'][:8]}: {e}")

            logger.info(f"Teams channel scan: {len(articles)} messages/replies in last {hours}h")

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
