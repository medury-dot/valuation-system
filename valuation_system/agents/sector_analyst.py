"""
Sector Analyst Agent
Sector-specific analysis and driver tracking.
One instance per sector with specialized knowledge.

Responsibilities:
- Track and update sector drivers from news events
- Calculate sector outlook scores
- Aggregate Porter's forces into modelable parameters
- Feed adjusted growth/margin parameters to Valuator

Edge Cases:
- No news events → Use last known driver state
- Conflicting signals → Weight by severity and recency
- Missing driver data → Fall back to sector defaults
"""

import os
import logging
from datetime import datetime
from typing import Optional

from dotenv import load_dotenv

from valuation_system.utils.llm_client import LLMClient
from valuation_system.utils.config_loader import load_sectors_config, get_driver_hierarchy

logger = logging.getLogger(__name__)

load_dotenv(os.path.join(os.path.dirname(__file__), '..', 'config', '.env'))


class SectorAnalystAgent:
    """
    Sector-specific analysis engine.

    Hierarchical Driver Framework:
    - Macro (20%): Sets ceiling/floor for growth and discount rates
    - Sector (55%): Core valuation drivers (demand, cost, Porter's, regulatory)
    - Company (25%): Alpha layer explaining dispersion

    Each sector instance knows:
    - What drivers matter and their weights
    - Terminal margin/ROCE convergence ranges
    - Porter's forces impact on terminal value
    - Which metrics to track (sector-specific)
    """

    DRIVER_IMPACT_PROMPT = """You are an equity research sector analyst for {sector}.

Given this news event:
Headline: {headline}
Summary: {summary}
Severity: {severity}

And the current sector drivers:
{current_drivers}

Determine:
1. Which drivers are affected (from the list above)
2. Direction of impact (UP, DOWN, STABLE) for each
3. Magnitude (MINOR: <0.5% valuation impact, MODERATE: 0.5-2%, SIGNIFICANT: >2%)
4. New suggested value/state for each affected driver
5. Estimated impact on revenue growth, margins, and terminal value
6. Confidence level (HIGH, MEDIUM, LOW)
7. Time horizon of impact (IMMEDIATE, SHORT_TERM_3M, MEDIUM_TERM_1Y, STRUCTURAL)

Return as JSON with structure:
{{
  "affected_drivers": [
    {{
      "driver_name": "...",
      "old_state": "...",
      "new_state": "...",
      "direction": "UP/DOWN/STABLE",
      "magnitude": "MINOR/MODERATE/SIGNIFICANT",
      "confidence": "HIGH/MEDIUM/LOW",
      "time_horizon": "...",
      "reasoning": "..."
    }}
  ],
  "revenue_growth_impact_pct": 0.0,
  "margin_impact_pct": 0.0,
  "terminal_value_impact": "NONE/MINOR/MODERATE/SIGNIFICANT",
  "overall_assessment": "..."
}}"""

    def __init__(self, sector_key: str, mysql_client,
                 llm_client: LLMClient = None):
        """
        Args:
            sector_key: Key from sectors.yaml (e.g., 'specialty_chemicals')
            mysql_client: MySQL client for DB operations
            llm_client: LLM client for analysis
        """
        self.sector_key = sector_key
        self.mysql = mysql_client
        self.llm = llm_client or LLMClient()

        # Load sector config
        sectors_config = load_sectors_config()
        self.config = sectors_config.get('sectors', {}).get(sector_key, {})
        self.csv_sector_name = self.config.get('csv_sector_name', '')
        self.hierarchy = get_driver_hierarchy(sectors_config)

        # Current driver states (loaded from DB or defaults)
        self._driver_states = {}
        self._load_driver_states()

        logger.info(f"SectorAnalystAgent initialized for '{sector_key}' "
                     f"(CSV: '{self.csv_sector_name}')")

    def _load_driver_states(self):
        """Load current driver states from MySQL or use defaults from config."""
        try:
            db_drivers = self.mysql.query(
                "SELECT * FROM vs_drivers WHERE sector = %s",
                (self.csv_sector_name,)
            )
            if db_drivers:
                for d in db_drivers:
                    key = f"{d['driver_level']}_{d['driver_name']}"
                    self._driver_states[key] = {
                        'level': d['driver_level'],
                        'category': d.get('driver_category', ''),
                        'name': d['driver_name'],
                        'value': d['current_value'],
                        'weight': float(d['weight']) if d.get('weight') else 0,
                        'impact_direction': d.get('impact_direction', 'NEUTRAL'),
                        'trend': d.get('trend', 'STABLE'),
                    }
                logger.info(f"Loaded {len(self._driver_states)} drivers from DB")
                return
        except Exception as e:
            logger.warning(f"Failed to load drivers from DB: {e}")

        # Fall back to config defaults
        self._init_drivers_from_config()

    def _init_drivers_from_config(self):
        """Initialize driver states from sectors.yaml config."""
        for category in ['demand_drivers', 'cost_drivers', 'regulatory_drivers',
                         'sector_specific_drivers']:
            drivers = self.config.get(category, [])
            for driver in drivers:
                key = f"SECTOR_{driver['name']}"
                self._driver_states[key] = {
                    'level': 'SECTOR',
                    'category': category,
                    'name': driver['name'],
                    'value': 'NOT_SET',
                    'weight': driver.get('weight', 0),
                    'impact_direction': 'NEUTRAL',
                    'trend': 'STABLE',
                }

    def update_drivers_from_news(self, news_events: list) -> list:
        """
        Process news events and update driver states.

        Args:
            news_events: List of classified news events (from NewsScannerAgent)

        Returns:
            List of driver changes made.
        """
        changes = []

        # Filter relevant events
        relevant = [
            e for e in news_events
            if self._is_sector_relevant(e)
        ]

        if not relevant:
            logger.debug(f"No relevant news for {self.sector_key}")
            return changes

        for event in relevant:
            try:
                driver_impact = self._analyze_driver_impact(event)
                if driver_impact and 'affected_drivers' in driver_impact:
                    for change in driver_impact['affected_drivers']:
                        self._apply_driver_change(change, event)
                        changes.append(change)
            except Exception as e:
                logger.error(f"Failed to process event for drivers: {e}", exc_info=True)
                continue

        if changes:
            logger.info(f"Updated {len(changes)} drivers from {len(relevant)} events")

        return changes

    def _analyze_driver_impact(self, event: dict) -> Optional[dict]:
        """Use LLM to determine driver impact from a news event."""
        drivers_text = self._format_current_drivers()

        prompt = self.DRIVER_IMPACT_PROMPT.format(
            sector=self.csv_sector_name,
            headline=event.get('headline', ''),
            summary=event.get('summary', ''),
            severity=event.get('severity', 'MEDIUM'),
            current_drivers=drivers_text,
        )

        return self.llm.analyze_json(prompt)

    def _apply_driver_change(self, change: dict, source_event: dict):
        """Apply a driver change to state and log it."""
        driver_name = change.get('driver_name', '')
        key = f"SECTOR_{driver_name}"

        old_value = self._driver_states.get(key, {}).get('value', 'NOT_SET')
        new_value = change.get('new_state', old_value)
        direction = change.get('direction', 'STABLE')

        # Update in-memory state
        if key in self._driver_states:
            self._driver_states[key]['value'] = new_value
            self._driver_states[key]['trend'] = direction
            if direction == 'UP':
                self._driver_states[key]['impact_direction'] = 'POSITIVE'
            elif direction == 'DOWN':
                self._driver_states[key]['impact_direction'] = 'NEGATIVE'

        # Log change to MySQL
        try:
            self.mysql.log_driver_change({
                'driver_level': 'SECTOR',
                'driver_category': self._driver_states.get(key, {}).get('category', ''),
                'driver_name': driver_name,
                'sector': self.csv_sector_name,
                'old_value': str(old_value),
                'new_value': str(new_value),
                'change_reason': change.get('reasoning', ''),
                'triggered_by': 'NEWS_EVENT',
                'estimated_valuation_impact_pct': change.get('valuation_impact_pct'),
            })
        except Exception as e:
            logger.error(f"Failed to log driver change: {e}", exc_info=True)

    def calculate_sector_outlook(self) -> dict:
        """
        Aggregate all driver states into a sector outlook score.

        Score range: -1.0 (very bearish) to +1.0 (very bullish)
        Used to adjust growth and margin assumptions in DCF.
        """
        total_score = 0.0
        total_weight = 0.0
        positives = []
        negatives = []

        for key, driver in self._driver_states.items():
            weight = driver.get('weight', 0)
            direction = driver.get('impact_direction', 'NEUTRAL')
            trend = driver.get('trend', 'STABLE')

            # Score: +1 for positive/up, -1 for negative/down, 0 for neutral
            if direction == 'POSITIVE' or trend == 'UP':
                score = 1.0
                positives.append(driver['name'])
            elif direction == 'NEGATIVE' or trend == 'DOWN':
                score = -1.0
                negatives.append(driver['name'])
            else:
                score = 0.0

            total_score += score * weight
            total_weight += weight

        outlook_score = total_score / total_weight if total_weight > 0 else 0.0
        outlook_score = max(-1.0, min(1.0, outlook_score))

        label = self._score_to_label(outlook_score)
        growth_adj = self._score_to_growth_adjustment(outlook_score)
        margin_adj = self._score_to_margin_adjustment(outlook_score)

        result = {
            'sector': self.csv_sector_name,
            'sector_key': self.sector_key,
            'outlook_score': round(outlook_score, 4),
            'outlook_label': label,
            'growth_adjustment': round(growth_adj, 4),
            'margin_adjustment': round(margin_adj, 4),
            'key_positives': positives,
            'key_negatives': negatives,
            'driver_count': len(self._driver_states),
            'as_of': datetime.now().isoformat(),
        }

        logger.info(f"Sector outlook for {self.csv_sector_name}: "
                     f"{label} ({outlook_score:+.3f}), "
                     f"growth adj={growth_adj:+.2%}, margin adj={margin_adj:+.2%}")

        return result

    def get_terminal_parameters(self) -> dict:
        """
        Get terminal value parameters informed by sector outlook.
        Terminal margin and ROCE convergence ranges from config,
        adjusted by current driver state.
        """
        terminal = self.config.get('terminal_assumptions', {})
        margin_range = terminal.get('margin_range', [0.15, 0.22])
        roce_convergence = terminal.get('roce_convergence', 0.18)
        reinvestment = terminal.get('reinvestment_rate', 0.30)

        outlook = self.calculate_sector_outlook()
        score = outlook['outlook_score']

        # Adjust within range based on outlook
        margin_base = (margin_range[0] + margin_range[1]) / 2
        margin_adj = margin_base + score * (margin_range[1] - margin_range[0]) / 4

        return {
            'terminal_margin': round(max(margin_range[0], min(margin_adj, margin_range[1])), 4),
            'terminal_roce': round(roce_convergence * (1 + score * 0.1), 4),
            'terminal_reinvestment': round(reinvestment * (1 - score * 0.05), 4),
            'margin_range': margin_range,
            'outlook_applied': score,
        }

    def get_driver_snapshot(self) -> dict:
        """Get current state of all drivers (for storage in valuation snapshot)."""
        return {
            'sector': self.csv_sector_name,
            'drivers': {k: {
                'value': v.get('value'),
                'weight': v.get('weight'),
                'direction': v.get('impact_direction'),
                'trend': v.get('trend'),
            } for k, v in self._driver_states.items()},
            'snapshot_time': datetime.now().isoformat(),
        }

    def _is_sector_relevant(self, event: dict) -> bool:
        """Check if a news event is relevant to this sector."""
        # MACRO events affect all sectors
        if event.get('scope') == 'MACRO':
            return True
        if event.get('severity') in ('CRITICAL',):
            return True

        affected_sectors = event.get('affected_sectors', [])
        if isinstance(affected_sectors, list):
            for s in affected_sectors:
                if s and (s.lower() in self.csv_sector_name.lower() or
                          self.csv_sector_name.lower() in s.lower()):
                    return True
        return False

    def _format_current_drivers(self) -> str:
        """Format current driver states for LLM prompt."""
        lines = []
        for key, driver in self._driver_states.items():
            lines.append(
                f"- {driver['name']} (weight={driver['weight']}, "
                f"current={driver['value']}, trend={driver['trend']})"
            )
        return '\n'.join(lines)

    def _score_to_label(self, score: float) -> str:
        if score > 0.3:
            return 'BULLISH'
        elif score > 0.1:
            return 'POSITIVE'
        elif score > -0.1:
            return 'NEUTRAL'
        elif score > -0.3:
            return 'NEGATIVE'
        else:
            return 'BEARISH'

    def _score_to_growth_adjustment(self, score: float) -> float:
        """Convert outlook score to growth rate adjustment."""
        # ±0.3 means ±3% growth adjustment
        return score * 0.03

    def _score_to_margin_adjustment(self, score: float) -> float:
        """Convert outlook score to margin adjustment."""
        # ±0.3 means ±1% margin adjustment
        return score * 0.01
