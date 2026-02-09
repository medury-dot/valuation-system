"""
Qualitative Driver Agent
Auto-fills SEED (qualitative) drivers for companies using LLM analysis of recent news events.

Flow:
1. Find companies with empty SEED drivers (STRATEGIC, COMPETITIVE, ESG categories)
2. For each company, gather recent news from vs_event_timeline
3. Send news + driver list to LLM for assessment
4. Parse structured response: direction, trend, reasoning
5. Update vs_drivers and log to vs_driver_changelog

Data Sources:
- vs_drivers: COMPANY-level SEED drivers with NULL current_value
- vs_event_timeline: Recent news events (matched by company_id or headline)
- vs_active_companies: Company master for batch runs

Edge Cases:
- No news for company -> Skip, leave drivers unfilled (no hallucination)
- LLM returns invalid JSON -> Log error, skip that driver, continue
- LLM call fails -> Log with traceback, continue to next company
- Empty event timeline -> Return early with summary, zero fills
- Driver already filled (race condition) -> Skip, do not overwrite
"""

import os
import sys
import json
import time
import logging
import traceback
from datetime import datetime, timedelta
from typing import Optional

from dotenv import load_dotenv

# Standard import path setup for valuation_system modules
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))

load_dotenv(os.path.join(os.path.dirname(__file__), '..', 'config', '.env'))

logger = logging.getLogger(__name__)

# Maps our valuation_group taxonomy to keyword patterns used in vs_event_timeline.sector
# (legacy Screener.in naming). Used for sector-level news matching.
_VALUATION_GROUP_TO_SECTOR_KEYWORDS = {
    'AUTO': ['Automobile', 'Auto'],
    'TECHNOLOGY': ['IT', 'Software', 'Technology', 'Business Services'],
    'HEALTHCARE': ['Healthcare', 'Pharma'],
    'FINANCIALS': ['Bank', 'Finance', 'Financial', 'Insurance', 'NBFC'],
    'CONSUMER_STAPLES': ['FMCG', 'Consumer', 'Food'],
    'CONSUMER_DISCRETIONARY': ['Consumer', 'Retail', 'Textile', 'Hotel', 'Jewellery'],
    'ENERGY_UTILITIES': ['Energy', 'Power', 'Oil', 'Gas', 'Utilities'],
    'MATERIALS_CHEMICALS': ['Chemical', 'Material'],
    'MATERIALS_METALS': ['Metal', 'Steel', 'Mining', 'Aluminium'],
    'INDUSTRIALS': ['Industrial', 'Engineering', 'Capital Goods', 'Defence', 'Defense'],
    'REAL_ESTATE_INFRA': ['Realty', 'Real Estate', 'Infrastructure', 'Construction'],
    'SERVICES': ['Services', 'Logistics', 'Education'],
    'TELECOM': ['Telecom', 'Telecommunication'],
    'CEMENT': ['Cement'],
    'MEDIA': ['Media', 'Entertainment'],
}


class QualitativeDriverAgent:
    """
    Auto-fills qualitative (SEED) drivers for companies by analyzing recent news events
    through LLM. Only fills drivers where current_value IS NULL -- never overwrites
    existing assessments.

    Qualitative driver categories: STRATEGIC, COMPETITIVE, ESG, REGULATORY, BALANCE_SHEET, GROWTH.

    Rate limits: Max 60 companies per run, 1-second pause between LLM calls.
    All fills logged to vs_driver_changelog for audit trail.
    """

    # System prompt for the LLM when assessing qualitative drivers
    SYSTEM_PROMPT = (
        "You are a senior equity research analyst specializing in Indian listed companies. "
        "You assess qualitative business drivers based on recent news and events. "
        "Be objective, evidence-based, and concise. "
        "If the news does not provide enough evidence for a driver, set direction to NEUTRAL "
        "and reasoning to 'Insufficient evidence from recent news'. "
        "Never fabricate or assume information not present in the provided news."
    )

    # Prompt template for assessing drivers. Placeholders: company_name, driver_list, news_summary
    ASSESSMENT_PROMPT = """Assess the following qualitative drivers for **{company_name}** based on recent news events.

DRIVERS TO ASSESS:
{driver_list}

RECENT NEWS EVENTS (last 30 days):
{news_summary}

For EACH driver, provide your assessment as a JSON array:
[
  {{
    "driver_name": "exact_driver_name_from_list",
    "direction": "POSITIVE" or "NEUTRAL" or "NEGATIVE",
    "trend": "UP" or "STABLE" or "DOWN",
    "reasoning": "1-sentence evidence-based reasoning referencing specific news items"
  }},
  ...
]

Rules:
- Use ONLY information from the provided news events. Do NOT hallucinate.
- If no news is relevant to a specific driver, set direction=NEUTRAL, trend=STABLE, reasoning="No relevant news evidence in the review period".
- Keep reasoning under 200 characters.
- Return valid JSON array only."""

    # Per-driver prompt for individual assessment (fallback if batch fails)
    SINGLE_DRIVER_PROMPT = """Assess the qualitative driver **{driver_name}** (category: {driver_category}) for company **{company_name}**.

Recent news events:
{news_summary}

Provide your assessment as JSON:
{{
  "direction": "POSITIVE" or "NEUTRAL" or "NEGATIVE",
  "trend": "UP" or "STABLE" or "DOWN",
  "reasoning": "1-sentence evidence-based reasoning"
}}

If the news does not relate to this driver, use direction=NEUTRAL, trend=STABLE.
Return valid JSON only."""

    def __init__(self, mysql_client, llm_client):
        """
        Args:
            mysql_client: ValuationMySQLClient instance (query, query_one, execute, insert)
            llm_client: LLMClient instance (analyze_json, analyze)
        """
        self.mysql = mysql_client
        self.llm = llm_client

        # Counters for run summary
        self._llm_calls = 0
        self._drivers_filled = 0
        self._drivers_skipped = 0
        self._companies_processed = 0
        self._companies_skipped_no_news = 0
        self._errors = []

        logger.info("QualitativeDriverAgent initialized")

    def run_batch(self, max_companies: int = 60) -> dict:
        """
        Run qualitative driver auto-fill for up to max_companies that have empty SEED drivers.

        Process:
        1. Find distinct companies with empty SEED drivers
        2. For each (up to max_companies), call populate_qualitative_drivers()
        3. Pause 1 second between LLM calls
        4. Return summary dict

        Args:
            max_companies: Maximum companies to process in one run (rate limiting)

        Returns:
            Summary dict with counts and any errors
        """
        start_time = datetime.now()
        logger.info(f"=== QualitativeDriverAgent batch run START (max_companies={max_companies}) ===")

        # Reset counters
        self._llm_calls = 0
        self._drivers_filled = 0
        self._drivers_skipped = 0
        self._companies_processed = 0
        self._companies_skipped_no_news = 0
        self._errors = []

        # Step 1: Find companies with empty SEED drivers
        try:
            companies = self.mysql.query(
                """SELECT d.company_id, ac.nse_symbol, ac.company_name,
                          MIN(ac.priority) AS priority
                   FROM vs_drivers d
                   JOIN vs_active_companies ac ON d.company_id = ac.company_id
                   WHERE d.driver_level = 'COMPANY'
                     AND d.source = 'SEED'
                     AND (d.current_value IS NULL OR d.current_value = '')
                     AND d.is_active = 1
                     AND ac.is_active = 1
                   GROUP BY d.company_id, ac.nse_symbol, ac.company_name
                   ORDER BY priority ASC, d.company_id ASC
                   LIMIT %s""",
                (max_companies,)
            )
        except Exception as e:
            error_msg = f"Failed to query companies with empty SEED drivers: {e}"
            logger.error(error_msg, exc_info=True)
            return {
                'status': 'ERROR',
                'error': error_msg,
                'traceback': traceback.format_exc(),
                'started_at': start_time.isoformat(),
                'finished_at': datetime.now().isoformat(),
            }

        if not companies:
            logger.info("No companies found with empty SEED drivers. Nothing to do.")
            return {
                'status': 'OK',
                'message': 'No companies with empty SEED drivers found',
                'companies_found': 0,
                'started_at': start_time.isoformat(),
                'finished_at': datetime.now().isoformat(),
            }

        logger.info(f"Found {len(companies)} companies with empty SEED drivers (processing up to {max_companies})")

        # Step 2: Process each company
        for idx, company in enumerate(companies):
            company_id = company['company_id']
            nse_symbol = company.get('nse_symbol') or ''
            company_name = company.get('company_name') or ''

            logger.info(f"[{idx + 1}/{len(companies)}] Processing {company_name} ({nse_symbol}, id={company_id})")

            try:
                filled_count = self.populate_qualitative_drivers(
                    company_id=company_id,
                    nse_symbol=nse_symbol,
                    company_name=company_name
                )
                self._companies_processed += 1
                logger.info(f"  -> Filled {filled_count} drivers for {nse_symbol}")

            except Exception as e:
                error_msg = f"Error processing {company_name} ({nse_symbol}, id={company_id}): {e}"
                logger.error(error_msg, exc_info=True)
                self._errors.append({
                    'company_id': company_id,
                    'nse_symbol': nse_symbol,
                    'company_name': company_name,
                    'error': str(e),
                    'traceback': traceback.format_exc(),
                })
                # Continue to next company -- do not abort batch on single failure
                continue

        # Step 3: Build summary
        elapsed = (datetime.now() - start_time).total_seconds()
        summary = {
            'status': 'OK' if not self._errors else 'PARTIAL',
            'started_at': start_time.isoformat(),
            'finished_at': datetime.now().isoformat(),
            'elapsed_seconds': round(elapsed, 1),
            'companies_found': len(companies),
            'companies_processed': self._companies_processed,
            'companies_skipped_no_news': self._companies_skipped_no_news,
            'drivers_filled': self._drivers_filled,
            'drivers_skipped': self._drivers_skipped,
            'llm_calls_made': self._llm_calls,
            'errors_count': len(self._errors),
            'errors': self._errors[:20],  # Truncate to first 20 for readability
        }

        logger.info(
            f"=== QualitativeDriverAgent batch run COMPLETE ===\n"
            f"  Companies processed: {self._companies_processed}/{len(companies)}\n"
            f"  Companies skipped (no news): {self._companies_skipped_no_news}\n"
            f"  Drivers filled: {self._drivers_filled}\n"
            f"  Drivers skipped: {self._drivers_skipped}\n"
            f"  LLM calls: {self._llm_calls}\n"
            f"  Errors: {len(self._errors)}\n"
            f"  Elapsed: {elapsed:.1f}s"
        )

        return summary

    def populate_qualitative_drivers(self, company_id: int, nse_symbol: str,
                                     company_name: str) -> int:
        """
        Fill empty SEED drivers for one company using LLM analysis of recent news.

        Steps:
        1. Get empty SEED drivers for this company
        2. Get recent news from vs_event_timeline
        3. If no news, skip (no hallucination policy)
        4. Send to LLM for batch assessment
        5. Parse response and update each driver
        6. Log changes to vs_driver_changelog

        Args:
            company_id: marketscrip_id / company_id in vs_drivers
            nse_symbol: NSE trading symbol
            company_name: Company display name

        Returns:
            Count of drivers successfully filled
        """
        logger.debug(f"populate_qualitative_drivers: company_id={company_id}, "
                     f"nse_symbol={nse_symbol}, company_name={company_name}")

        # Step 1: Get empty SEED drivers for this company
        empty_drivers = self._get_empty_seed_drivers(company_id)
        if not empty_drivers:
            logger.debug(f"No empty SEED drivers for company_id={company_id}")
            return 0

        logger.info(f"  Found {len(empty_drivers)} empty SEED drivers for {nse_symbol}")
        for d in empty_drivers:
            logger.debug(f"    Driver: {d['driver_name']} (category={d['driver_category']}, id={d['id']})")

        # Step 2: Get recent news
        news_items = self._get_recent_news(nse_symbol, company_name, days=30)
        if not news_items:
            logger.info(f"  No recent news for {nse_symbol} -- skipping (no-hallucination policy)")
            self._companies_skipped_no_news += 1
            return 0

        logger.info(f"  Found {len(news_items)} news items for {nse_symbol}")
        for n in news_items[:5]:
            logger.debug(f"    News: [{n.get('event_date')}] {n.get('headline', '')[:80]}")

        # Step 3: Build news summary for LLM
        news_summary = self._format_news_for_prompt(news_items)

        # Step 4: Attempt batch LLM assessment (all drivers at once)
        filled_count = 0
        assessments = self._assess_drivers_batch(empty_drivers, company_name, news_summary)

        if assessments:
            # Step 5: Apply assessments from batch
            remaining_drivers = []
            for driver in empty_drivers:
                driver_name = driver['driver_name']
                assessment = assessments.get(driver_name)
                if assessment:
                    success = self._apply_assessment(
                        driver=driver,
                        assessment=assessment,
                        company_id=company_id,
                        company_name=company_name,
                        nse_symbol=nse_symbol,
                        news_count=len(news_items)
                    )
                    if success:
                        filled_count += 1
                        self._drivers_filled += 1
                    else:
                        self._drivers_skipped += 1
                else:
                    remaining_drivers.append(driver)

            # Per-driver fallback for any drivers not covered by batch
            if remaining_drivers:
                logger.info(f"  Batch missed {len(remaining_drivers)} drivers, "
                            f"using per-driver fallback")
                for driver in remaining_drivers:
                    assessment = self._assess_driver(
                        driver_name=driver['driver_name'],
                        driver_category=driver.get('driver_category', ''),
                        company_name=company_name,
                        news_summary=news_summary
                    )
                    if assessment and 'error' not in assessment:
                        success = self._apply_assessment(
                            driver=driver,
                            assessment=assessment,
                            company_id=company_id,
                            company_name=company_name,
                            nse_symbol=nse_symbol,
                            news_count=len(news_items)
                        )
                        if success:
                            filled_count += 1
                            self._drivers_filled += 1
                        else:
                            self._drivers_skipped += 1
                    else:
                        self._drivers_skipped += 1
        else:
            # Batch failed entirely -- fall back to per-driver assessment
            logger.warning(f"  Batch assessment failed for {nse_symbol}, trying per-driver fallback")
            for driver in empty_drivers:
                assessment = self._assess_driver(
                    driver_name=driver['driver_name'],
                    driver_category=driver.get('driver_category', ''),
                    company_name=company_name,
                    news_summary=news_summary
                )
                if assessment and 'error' not in assessment:
                    success = self._apply_assessment(
                        driver=driver,
                        assessment=assessment,
                        company_id=company_id,
                        company_name=company_name,
                        nse_symbol=nse_symbol,
                        news_count=len(news_items)
                    )
                    if success:
                        filled_count += 1
                        self._drivers_filled += 1
                    else:
                        self._drivers_skipped += 1
                else:
                    self._drivers_skipped += 1

        return filled_count

    def _get_empty_seed_drivers(self, company_id: int) -> list:
        """
        Get SEED drivers with NULL/empty current_value for a company.

        Returns:
            List of driver dicts from vs_drivers
        """
        try:
            drivers = self.mysql.query(
                """SELECT id, driver_level, driver_category, driver_name,
                          company_id, current_value, weight, impact_direction,
                          trend, source, valuation_group, valuation_subgroup
                   FROM vs_drivers
                   WHERE driver_level = 'COMPANY'
                     AND source = 'SEED'
                     AND (current_value IS NULL OR current_value = '')
                     AND company_id = %s
                     AND is_active = 1
                   ORDER BY driver_category, driver_name""",
                (company_id,)
            )
            return drivers or []
        except Exception as e:
            logger.error(f"Failed to query empty SEED drivers for company_id={company_id}: {e}",
                         exc_info=True)
            raise

    def _get_recent_news(self, nse_symbol: str, company_name: str,
                         days: int = 30) -> list:
        """
        Get recent news from vs_event_timeline for a company.

        Matching strategy (OR logic):
        1. company_id match via vs_active_companies lookup
        2. headline LIKE '%symbol%' (case-insensitive)
        3. headline LIKE '%company_name%' (case-insensitive, first word of name for safety)
        4. grok_synopsis LIKE '%symbol%'
        5. Sector-level news: scope IN (GROUP,MACRO) and sector matches company's valuation_group

        Args:
            nse_symbol: NSE trading symbol
            company_name: Full company name
            days: How many days back to search

        Returns:
            List of event dicts from vs_event_timeline, sorted by event_date DESC
        """
        cutoff_date = (datetime.now() - timedelta(days=days)).strftime('%Y-%m-%d')

        # Build OR conditions for matching
        conditions = []
        params = []

        # Condition 1: Match by company_id
        company_record = None
        if nse_symbol:
            company_record = self.mysql.query_one(
                """SELECT company_id, valuation_group, valuation_subgroup
                   FROM vs_active_companies
                   WHERE nse_symbol = %s AND is_active = 1 LIMIT 1""",
                (nse_symbol,)
            )
            if company_record:
                conditions.append("e.company_id = %s")
                params.append(company_record['company_id'])

        # Condition 2: Match by NSE symbol in headline
        if nse_symbol:
            conditions.append("e.headline LIKE %s")
            params.append(f"%{nse_symbol}%")

        # Condition 3: Match by company name keyword in headline
        # Use first significant word of company name (skip "The", short words)
        if company_name:
            name_keyword = self._extract_company_keyword(company_name)
            if name_keyword and len(name_keyword) >= 3:
                conditions.append("e.headline LIKE %s")
                params.append(f"%{name_keyword}%")

        # Condition 4: Match by symbol in grok_synopsis (broader search)
        if nse_symbol:
            conditions.append("e.grok_synopsis LIKE %s")
            params.append(f"%{nse_symbol}%")

        # Condition 5: Sector-level news (GROUP/MACRO scope events matching company's sector)
        # Maps valuation_group to legacy sector names in event timeline
        if company_record:
            vg = (company_record.get('valuation_group') or '').upper()
            sector_keywords = _VALUATION_GROUP_TO_SECTOR_KEYWORDS.get(vg, [])
            for keyword in sector_keywords:
                conditions.append("(e.scope IN ('GROUP','MACRO') AND e.sector LIKE %s)")
                params.append(f"%{keyword}%")

        if not conditions:
            logger.debug(f"No search conditions for {nse_symbol}/{company_name}")
            return []

        where_clause = " OR ".join(conditions)

        try:
            events = self.mysql.query(
                f"""SELECT e.id, e.event_date, e.headline, e.summary, e.severity,
                           e.source, e.scope, e.event_type, e.grok_synopsis
                    FROM vs_event_timeline e
                    WHERE e.event_date >= %s
                      AND ({where_clause})
                    ORDER BY e.event_date DESC
                    LIMIT 50""",
                tuple([cutoff_date] + params)
            )
            logger.debug(f"_get_recent_news: {len(events) if events else 0} events for "
                         f"{nse_symbol} (cutoff={cutoff_date}, conditions={len(conditions)})")
            return events or []
        except Exception as e:
            logger.error(f"Failed to query news for {nse_symbol}: {e}", exc_info=True)
            return []

    def _extract_company_keyword(self, company_name: str) -> str:
        """
        Extract the most distinctive keyword from a company name for headline matching.
        Skips common suffixes and prefixes.

        Examples:
            'Eicher Motors Limited' -> 'Eicher'
            'Aether Industries Ltd.' -> 'Aether'
            'The Indian Hotels Company Limited' -> 'Indian Hotels'

        Returns:
            Keyword string (may be empty if name is too generic)
        """
        skip_words = {
            'the', 'ltd', 'ltd.', 'limited', 'pvt', 'private', 'company',
            'corporation', 'corp', 'inc', 'industries', 'india', 'indian',
            'of', 'and', '&', 'co', 'enterprises', 'holdings', 'group',
        }

        words = company_name.strip().split()
        significant_words = [w for w in words if w.lower().strip('.') not in skip_words]

        if not significant_words:
            # Fallback to first word if all words are "common"
            return words[0] if words else ''

        # Return first significant word (usually the brand name)
        return significant_words[0]

    def _format_news_for_prompt(self, news_items: list, max_items: int = 20) -> str:
        """
        Format news items into a concise text block for the LLM prompt.
        Truncates to max_items to stay within token limits.

        Args:
            news_items: List of event dicts from vs_event_timeline
            max_items: Maximum items to include

        Returns:
            Formatted string with numbered news items
        """
        lines = []
        for i, item in enumerate(news_items[:max_items]):
            date_str = str(item.get('event_date', ''))
            headline = item.get('headline', '(no headline)')
            severity = item.get('severity', '')
            summary = item.get('summary', '') or item.get('grok_synopsis', '') or ''

            # Truncate summary to keep prompt manageable
            if summary and len(summary) > 300:
                summary = summary[:297] + '...'

            line = f"{i + 1}. [{date_str}] [{severity}] {headline}"
            if summary:
                line += f"\n   Summary: {summary}"
            lines.append(line)

        if len(news_items) > max_items:
            lines.append(f"\n... and {len(news_items) - max_items} more events (truncated)")

        return "\n".join(lines)

    def _assess_drivers_batch(self, drivers: list, company_name: str,
                              news_summary: str) -> Optional[dict]:
        """
        Use LLM to assess all drivers for a company in a single call (efficient).

        Args:
            drivers: List of driver dicts
            company_name: Company display name
            news_summary: Formatted news text

        Returns:
            Dict of {driver_name: {direction, trend, reasoning}} or None on failure
        """
        # Build driver list for prompt
        driver_lines = []
        for d in drivers:
            driver_lines.append(f"- {d['driver_name']} (category: {d.get('driver_category', 'UNKNOWN')})")
        driver_list_text = "\n".join(driver_lines)

        prompt = self.ASSESSMENT_PROMPT.format(
            company_name=company_name,
            driver_list=driver_list_text,
            news_summary=news_summary
        )

        logger.debug(f"  Batch LLM prompt length: {len(prompt)} chars, {len(drivers)} drivers")

        try:
            self._llm_calls += 1
            response = self.llm.analyze_json(
                prompt,
                system_prompt=self.SYSTEM_PROMPT,
                temperature=0.3
            )

            # Rate limiting pause
            time.sleep(1)

            if not response or 'error' in response:
                logger.warning(f"  LLM batch assessment returned error: {response}")
                return None

            # Parse response -- expect list of assessments
            logger.debug(f"  Batch LLM response type: {type(response).__name__}, "
                         f"keys: {list(response.keys())[:10] if isinstance(response, dict) else 'N/A'}")

            assessments_list = response if isinstance(response, list) else response.get('assessments', response)

            # If we got a dict with driver names as keys directly
            # e.g., {"attrition_vs_peers": {"direction": "NEUTRAL", ...}, ...}
            if isinstance(assessments_list, dict) and 'driver_name' not in assessments_list:
                first_val = next(iter(assessments_list.values()), None) if assessments_list else None
                if isinstance(first_val, dict) and ('direction' in first_val or 'trend' in first_val):
                    logger.info(f"  Batch response: dict-keyed format, {len(assessments_list)} assessments")
                    return assessments_list

            # Single assessment dict: {"driver_name": "x", "direction": "...", ...}
            if isinstance(assessments_list, dict) and 'driver_name' in assessments_list:
                name = assessments_list['driver_name']
                result = {name: {
                    'direction': assessments_list.get('direction', 'NEUTRAL'),
                    'trend': assessments_list.get('trend', 'STABLE'),
                    'reasoning': assessments_list.get('reasoning', ''),
                }}
                logger.info(f"  Batch response: single-dict format, 1 assessment")
                return result

            # Convert list to dict keyed by driver_name
            result = {}
            if isinstance(assessments_list, list):
                for item in assessments_list:
                    if isinstance(item, dict) and 'driver_name' in item:
                        name = item['driver_name']
                        result[name] = {
                            'direction': item.get('direction', 'NEUTRAL'),
                            'trend': item.get('trend', 'STABLE'),
                            'reasoning': item.get('reasoning', ''),
                        }

                logger.info(f"  Batch LLM assessment: {len(result)}/{len(drivers)} drivers assessed")
                return result if result else None
            else:
                logger.warning(f"  Unexpected LLM response format: {type(assessments_list).__name__}, "
                               f"content: {str(assessments_list)[:200]}")
                return None

        except Exception as e:
            logger.error(f"  Batch LLM assessment failed: {e}", exc_info=True)
            return None

    def _assess_driver(self, driver_name: str, driver_category: str,
                       company_name: str, news_summary: str) -> dict:
        """
        Use LLM to assess a single driver (fallback when batch fails).

        Args:
            driver_name: Name of the driver
            driver_category: Category (STRATEGIC, COMPETITIVE, etc.)
            company_name: Company display name
            news_summary: Formatted news text

        Returns:
            Dict {direction, trend, reasoning} or {'error': ...}
        """
        prompt = self.SINGLE_DRIVER_PROMPT.format(
            driver_name=driver_name,
            driver_category=driver_category,
            company_name=company_name,
            news_summary=news_summary
        )

        logger.debug(f"  Single-driver LLM call for '{driver_name}' ({company_name})")

        try:
            self._llm_calls += 1
            response = self.llm.analyze_json(
                prompt,
                system_prompt=self.SYSTEM_PROMPT,
                temperature=0.3
            )

            # Rate limiting pause
            time.sleep(1)

            if not response or 'error' in response:
                logger.warning(f"  Single-driver LLM assessment returned error for "
                               f"'{driver_name}': {response}")
                return response or {'error': 'empty_response'}

            # Validate required fields
            direction = response.get('direction', 'NEUTRAL')
            trend = response.get('trend', 'STABLE')
            reasoning = response.get('reasoning', '')

            return {
                'direction': direction,
                'trend': trend,
                'reasoning': reasoning,
            }

        except Exception as e:
            logger.error(f"  Single-driver LLM assessment failed for '{driver_name}': {e}",
                         exc_info=True)
            return {'error': str(e), 'traceback': traceback.format_exc()}

    def _apply_assessment(self, driver: dict, assessment: dict,
                          company_id: int, company_name: str,
                          nse_symbol: str, news_count: int) -> bool:
        """
        Apply a single driver assessment: update vs_drivers and log to vs_driver_changelog.

        Validates direction/trend enums before writing. Never overwrites non-empty values
        (re-checks current_value in case of race condition).

        Args:
            driver: Driver dict from vs_drivers
            assessment: Dict {direction, trend, reasoning}
            company_id: Company ID
            company_name: Company name for logging
            nse_symbol: NSE symbol for logging
            news_count: Number of news items analyzed (for changelog)

        Returns:
            True if successfully applied, False otherwise
        """
        driver_id = driver['id']
        driver_name = driver['driver_name']

        # Validate direction enum
        direction = assessment.get('direction', 'NEUTRAL')
        if direction not in ('POSITIVE', 'NEGATIVE', 'NEUTRAL'):
            logger.warning(f"  Invalid direction '{direction}' for {driver_name}, defaulting to NEUTRAL")
            direction = 'NEUTRAL'

        # Validate trend enum
        trend = assessment.get('trend', 'STABLE')
        if trend not in ('UP', 'DOWN', 'STABLE'):
            logger.warning(f"  Invalid trend '{trend}' for {driver_name}, defaulting to STABLE")
            trend = 'STABLE'

        reasoning = str(assessment.get('reasoning', ''))[:500]  # Truncate to safe length

        logger.info(f"  Applying: {driver_name} -> direction={direction}, trend={trend}, "
                    f"reasoning='{reasoning[:80]}...'")

        try:
            # Re-check that current_value is still empty (race condition guard)
            current = self.mysql.query_one(
                "SELECT current_value, source FROM vs_drivers WHERE id = %s",
                (driver_id,)
            )
            if current and current.get('current_value') and str(current['current_value']).strip():
                logger.info(f"  Driver {driver_name} (id={driver_id}) already filled "
                            f"(race condition). Skipping.")
                return False

            # Update vs_drivers
            rows_updated = self.mysql.execute(
                """UPDATE vs_drivers
                   SET current_value = %s,
                       impact_direction = %s,
                       trend = %s,
                       source = 'AUTO',
                       updated_by = 'qualitative_driver_agent',
                       last_updated = NOW()
                   WHERE id = %s
                     AND (current_value IS NULL OR current_value = '')""",
                (reasoning, direction, trend, driver_id)
            )

            if rows_updated == 0:
                logger.warning(f"  UPDATE returned 0 rows for driver id={driver_id} "
                               f"({driver_name}). May have been filled concurrently.")
                return False

            logger.debug(f"  Updated vs_drivers id={driver_id}: {rows_updated} row(s)")

            # Log to vs_driver_changelog
            self._log_changelog(
                driver=driver,
                company_id=company_id,
                company_name=company_name,
                nse_symbol=nse_symbol,
                direction=direction,
                trend=trend,
                reasoning=reasoning,
                news_count=news_count
            )

            return True

        except Exception as e:
            logger.error(f"  Failed to apply assessment for {driver_name} (id={driver_id}): {e}",
                         exc_info=True)
            return False

    def _log_changelog(self, driver: dict, company_id: int, company_name: str,
                       nse_symbol: str, direction: str, trend: str,
                       reasoning: str, news_count: int):
        """
        Log the driver change to vs_driver_changelog for audit trail.

        Args:
            driver: Original driver dict
            company_id: Company ID
            company_name: Company name
            nse_symbol: NSE symbol
            direction: New impact_direction
            trend: New trend
            reasoning: LLM reasoning text
            news_count: Number of news items analyzed
        """
        change_reason = (
            f"Auto-filled by QualitativeDriverAgent. "
            f"Analyzed {news_count} news items for {company_name} ({nse_symbol}). "
            f"Assessment: direction={direction}, trend={trend}. "
            f"Reasoning: {reasoning[:200]}"
        )

        try:
            self.mysql.execute(
                """INSERT INTO vs_driver_changelog
                   (driver_level, driver_category, driver_name,
                    valuation_group, valuation_subgroup,
                    company_id, old_value, new_value,
                    change_reason, triggered_by, source)
                   VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)""",
                (
                    'COMPANY',
                    driver.get('driver_category', ''),
                    driver['driver_name'],
                    driver.get('valuation_group', ''),
                    driver.get('valuation_subgroup', ''),
                    company_id,
                    None,  # old_value (was NULL)
                    reasoning,  # new_value
                    change_reason,
                    'AGENT_ANALYSIS',
                    'AUTO',
                )
            )
            logger.debug(f"  Changelog logged for {driver['driver_name']} (company_id={company_id})")

        except Exception as e:
            logger.error(f"  Failed to log changelog for {driver['driver_name']}: {e}",
                         exc_info=True)
            # Do not re-raise -- changelog failure should not prevent the fill


def main():
    """
    CLI entry point for running the qualitative driver agent.
    Usage: python -m valuation_system.agents.qualitative_driver_agent [max_companies]
    """
    import argparse

    # Logging setup
    log_dir = os.path.join(os.path.dirname(__file__), '..', 'logs')
    os.makedirs(log_dir, exist_ok=True)
    log_file = os.path.join(log_dir, f"qualitative_driver_agent_{datetime.now().strftime('%Y%m%d_%H%M')}.log")

    logging.basicConfig(
        level=logging.DEBUG,
        format='%(asctime)s [%(levelname)s] %(name)s: %(message)s',
        handlers=[
            logging.FileHandler(log_file),
            logging.StreamHandler(),
        ]
    )

    parser = argparse.ArgumentParser(description='Qualitative Driver Agent - auto-fill SEED drivers via LLM')
    parser.add_argument('--max-companies', type=int, default=60,
                        help='Maximum number of companies to process (default: 60)')
    args = parser.parse_args()

    logger.info(f"Starting QualitativeDriverAgent CLI (max_companies={args.max_companies})")
    logger.info(f"Log file: {log_file}")

    # Initialize clients
    from valuation_system.storage.mysql_client import get_mysql_client
    from valuation_system.utils.llm_client import LLMClient

    mysql_client = get_mysql_client()
    llm_client = LLMClient()

    agent = QualitativeDriverAgent(mysql_client=mysql_client, llm_client=llm_client)
    summary = agent.run_batch(max_companies=args.max_companies)

    # Print summary
    print("\n" + "=" * 60)
    print("QUALITATIVE DRIVER AGENT - RUN SUMMARY")
    print("=" * 60)
    print(json.dumps(summary, indent=2, default=str))
    print(f"\nLog file: {log_file}")
    print("=" * 60)


if __name__ == '__main__':
    main()
