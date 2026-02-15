"""
Social Poster
Posts approved social media content from the main GSheet 'Social Posts' tab
to Twitter/X and LinkedIn.

Reuses patterns from /Users/ram/code/rag/machai/twitter/post_tweets_from_gsheet.py
but reads from the main drivers GSheet instead of 'storm posting'.

Workflow:
1. Read GSheet 'Social Posts' tab for rows with Approval=YES
2. For each approved row, post to Twitter and/or LinkedIn (if not already posted)
3. Update posted_x_at / posted_linkedin_at timestamps in GSheet
4. Dry-run mode by default (safety)

Usage:
  python -m valuation_system.scheduler.runner post_social          # dry-run
  python -m valuation_system.scheduler.runner post_social --no-dry-run  # live
"""

import os
import sys
import re
import time
import random
import logging
import traceback

import requests
from dotenv import load_dotenv

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))

from valuation_system.storage.gsheet_client import GSheetClient

logger = logging.getLogger(__name__)

load_dotenv(os.path.join(os.path.dirname(__file__), '..', 'config', '.env'))

MAX_TWEET_LENGTH = 25000 - 1  # X premium allows long posts


class SocialPoster:
    """Post approved social media content to Twitter/X and LinkedIn."""

    def __init__(self, gsheet_client: GSheetClient = None, dry_run: bool = True):
        self.gsheet = gsheet_client or GSheetClient()
        self.dry_run = dry_run

        # Twitter credentials from .env
        self._twitter_api_key = os.getenv('TWITTER_API_KEY', '')
        self._twitter_api_secret = os.getenv('TWITTER_API_SECRET_KEY', '')
        self._twitter_bearer = os.getenv('TWITTER_BEARER_TOKEN', '')
        self._twitter_access_token = os.getenv('TWITTER_ACCESS_TOKEN', '')
        self._twitter_access_secret = os.getenv('TWITTER_ACCESS_TOKEN_SECRET', '')

        # LinkedIn credentials from .env
        self._linkedin_token = os.getenv('LINKEDIN_ACCESS_TOKEN', '')
        self._linkedin_person_urn = os.getenv('LINKEDIN_PERSON_URN', '')

    def post_approved_posts(self) -> dict:
        """
        Read approved posts from GSheet, post to Twitter and LinkedIn.
        Returns summary dict with counts.
        """
        results = {
            'total_approved': 0,
            'twitter_posted': 0,
            'twitter_failed': 0,
            'linkedin_posted': 0,
            'linkedin_failed': 0,
            'dry_run': self.dry_run,
            'details': [],
        }

        approved_posts = self.gsheet.get_approved_social_posts()
        results['total_approved'] = len(approved_posts)

        if not approved_posts:
            logger.info("No approved unposted social posts found")
            return results

        for post in approved_posts:
            detail = {
                'headline': post.get('headline', ''),
                'category': post.get('category', ''),
                'twitter_status': 'skipped',
                'linkedin_status': 'skipped',
            }

            # Post to Twitter if not already posted
            twitter_text = post.get('twitter', '').strip()
            if twitter_text and not post.get('posted_x_at'):
                success = self._post_to_twitter(twitter_text)
                if success:
                    detail['twitter_status'] = 'posted' if not self.dry_run else 'dry_run'
                    results['twitter_posted'] += 1
                    if not self.dry_run:
                        self.gsheet.mark_social_post_posted(post['row_index'], 'twitter')
                else:
                    detail['twitter_status'] = 'failed'
                    results['twitter_failed'] += 1

            # Post to LinkedIn if not already posted
            linkedin_text = post.get('linkedin', '').strip()
            if linkedin_text and not post.get('posted_linkedin_at'):
                success = self._post_to_linkedin(linkedin_text)
                if success:
                    detail['linkedin_status'] = 'posted' if not self.dry_run else 'dry_run'
                    results['linkedin_posted'] += 1
                    if not self.dry_run:
                        self.gsheet.mark_social_post_posted(post['row_index'], 'linkedin')
                else:
                    detail['linkedin_status'] = 'failed'
                    results['linkedin_failed'] += 1

            results['details'].append(detail)

            # Rate limit pause between posts
            if not self.dry_run:
                time.sleep(random.uniform(12, 20))

        logger.info(
            f"Social posting complete: "
            f"Twitter {results['twitter_posted']}/{results['total_approved']}, "
            f"LinkedIn {results['linkedin_posted']}/{results['total_approved']} "
            f"({'DRY RUN' if self.dry_run else 'LIVE'})"
        )
        return results

    def _post_to_twitter(self, text: str) -> bool:
        """
        Post a tweet to Twitter/X using tweepy OAuth1.
        Supports threads (text split by double newline).
        Returns True if successful.
        """
        if self.dry_run:
            logger.info(f"[DRY RUN] Twitter: {text[:100]}...")
            print(f"\n{'*'*20} Twitter Dry Run {'*'*20}")
            print(text)
            return True

        if not all([self._twitter_api_key, self._twitter_api_secret,
                     self._twitter_access_token, self._twitter_access_secret]):
            logger.error("Twitter credentials not configured in .env")
            return False

        try:
            import tweepy

            client = tweepy.Client(
                bearer_token=self._twitter_bearer,
                consumer_key=self._twitter_api_key,
                consumer_secret=self._twitter_api_secret,
                access_token=self._twitter_access_token,
                access_token_secret=self._twitter_access_secret,
            )

            if len(text) > MAX_TWEET_LENGTH:
                logger.error(f"Tweet exceeds max length ({len(text)} chars)")
                return False

            # Check for thread format (tweets separated by ---)
            if '---' in text:
                tweets = [t.strip() for t in text.split('---') if t.strip()]
                return self._post_twitter_thread(client, tweets)
            else:
                response = client.create_tweet(text=text)
                logger.info(f"Posted tweet: {text[:60]}... (id={response.data['id']})")
                return True

        except Exception as e:
            logger.error(f"Twitter posting failed: {e}", exc_info=True)
            return False

    def _post_twitter_thread(self, client, tweets: list) -> bool:
        """Post a thread of tweets. Returns True if all tweets posted."""
        reply_to_id = None
        for i, tweet in enumerate(tweets):
            # Strip thread numbering (e.g., "1/3 " prefix)
            tweet = re.sub(r'^\d+/\d+\s*', '', tweet).strip()
            if not tweet:
                continue

            try:
                if reply_to_id is None:
                    response = client.create_tweet(text=tweet)
                else:
                    time.sleep(random.uniform(12, 20))
                    response = client.create_tweet(text=tweet, in_reply_to_tweet_id=reply_to_id)

                reply_to_id = response.data['id']
                logger.info(f"Posted thread tweet {i+1}/{len(tweets)}: {tweet[:40]}...")

            except Exception as e:
                if hasattr(e, 'response') and hasattr(e.response, 'status_code') and e.response.status_code == 429:
                    logger.warning("Twitter rate limit hit, sleeping 15 minutes...")
                    time.sleep(15 * 60)
                    # Retry once
                    try:
                        if reply_to_id is None:
                            response = client.create_tweet(text=tweet)
                        else:
                            response = client.create_tweet(text=tweet, in_reply_to_tweet_id=reply_to_id)
                        reply_to_id = response.data['id']
                    except Exception as e2:
                        logger.error(f"Twitter thread retry failed: {e2}", exc_info=True)
                        return False
                else:
                    logger.error(f"Twitter thread posting failed at tweet {i+1}: {e}", exc_info=True)
                    return False
        return True

    def _post_to_linkedin(self, text: str) -> bool:
        """
        Post to LinkedIn using UGC API.
        Returns True if successful.
        """
        if self.dry_run:
            logger.info(f"[DRY RUN] LinkedIn: {text[:100]}...")
            print(f"\n{'*'*20} LinkedIn Dry Run {'*'*20}")
            print(text)
            return True

        if not self._linkedin_token or not self._linkedin_person_urn:
            logger.error("LinkedIn credentials not configured in .env")
            return False

        headers = {
            'Authorization': f'Bearer {self._linkedin_token}',
            'Content-Type': 'application/json',
            'X-Restli-Protocol-Version': '2.0.0',
        }

        post_data = {
            "author": self._linkedin_person_urn,
            "lifecycleState": "PUBLISHED",
            "specificContent": {
                "com.linkedin.ugc.ShareContent": {
                    "shareCommentary": {
                        "text": text
                    },
                    "shareMediaCategory": "NONE"
                }
            },
            "visibility": {
                "com.linkedin.ugc.MemberNetworkVisibility": "PUBLIC"
            }
        }

        try:
            response = requests.post(
                'https://api.linkedin.com/v2/ugcPosts',
                headers=headers,
                json=post_data,
                timeout=30,
            )

            if response.status_code == 401:
                logger.error(
                    "LinkedIn auth failed: access token expired. "
                    "Update LINKEDIN_ACCESS_TOKEN in .env"
                )
                return False

            response.raise_for_status()
            logger.info(f"Posted to LinkedIn: {text[:60]}...")
            return True

        except requests.exceptions.HTTPError as e:
            logger.error(
                f"LinkedIn HTTP error: {e}\n"
                f"Status: {response.status_code}\n"
                f"Response: {response.content}",
                exc_info=True,
            )
            return False
        except requests.exceptions.RequestException as e:
            logger.error(f"LinkedIn request failed: {e}", exc_info=True)
            return False
