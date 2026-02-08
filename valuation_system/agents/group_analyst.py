"""
Group/Subgroup Analyst Agent (GroupAnalystAgent)
4-level hierarchical driver tracking and analysis.
One instance per valuation_group + valuation_subgroup combination.

Responsibilities:
- Track and update drivers at GROUP and SUBGROUP levels
- Calculate combined outlook scores
- Aggregate Porter's forces into modelable parameters
- Feed adjusted growth/margin parameters to Valuator

Hierarchical Driver Framework (4-Level):
- Macro (15%): Sets ceiling/floor for growth and discount rates
- Group (20%): Industry cycle, structural trends (valuation_group)
- Subgroup (35%): Key differentiators, specific drivers (valuation_subgroup)
- Company (30%): Alpha layer, execution, catalysts

Edge Cases:
- No news events → Use last known driver state
- Conflicting signals → Weight by severity and recency
- Missing driver data → Fall back to config defaults
- No subgroup defined → All weight shifts to group level
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


class GroupAnalystAgent:
    """
    Group/Subgroup-level analysis engine supporting 4-level driver hierarchy.

    Hierarchical Driver Framework (4-Level):
    - Macro (15%): Sets ceiling/floor for growth and discount rates
    - Group (20%): Industry cycle, structural trends (valuation_group)
    - Subgroup (35%): Key differentiators, specific drivers (valuation_subgroup)
    - Company (30%): Alpha layer, execution, catalysts

    Each instance knows:
    - What drivers matter at group and subgroup levels
    - Terminal margin/ROCE convergence ranges
    - Porter's forces impact on terminal value
    - Which metrics to track (group/subgroup-specific)
    """

    DRIVER_IMPACT_PROMPT = """You are an equity research sector analyst for {group} / {subgroup}.

Given this news event:
Headline: {headline}
Summary: {summary}
Severity: {severity}

And the current drivers:
GROUP DRIVERS ({group}):
{group_drivers}

SUBGROUP DRIVERS ({subgroup}):
{subgroup_drivers}

Determine:
1. Which drivers are affected (from the lists above)
2. Level of driver (GROUP or SUBGROUP)
3. Direction of impact (UP, DOWN, STABLE) for each
4. Magnitude (MINOR: <0.5% valuation impact, MODERATE: 0.5-2%, SIGNIFICANT: >2%)
5. New suggested value/state for each affected driver
6. Estimated impact on revenue growth, margins, and terminal value
7. Confidence level (HIGH, MEDIUM, LOW)
8. Time horizon of impact (IMMEDIATE, SHORT_TERM_3M, MEDIUM_TERM_1Y, STRUCTURAL)

Return as JSON with structure:
{{
  "affected_drivers": [
    {{
      "driver_level": "GROUP/SUBGROUP",
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

    def __init__(self, valuation_group: str, valuation_subgroup: str = '',
                 mysql_client=None, llm_client: LLMClient = None):
        """
        Args:
            valuation_group: Group key from sectors.yaml (e.g., 'INDUSTRIALS')
            valuation_subgroup: Subgroup key (e.g., 'INDUSTRIALS_DEFENSE')
            mysql_client: MySQL client for DB operations
            llm_client: LLM client for analysis
        """
        self.valuation_group = valuation_group
        self.valuation_subgroup = valuation_subgroup or ''
        self.mysql = mysql_client
        self.llm = llm_client or LLMClient()

        # Load group config - try subgroup first, then group, follow primary_group reference
        sectors_config = load_sectors_config()
        self.config = self._resolve_group_config(sectors_config, valuation_group, valuation_subgroup)
        self.csv_group_name = self.config.get('csv_sector_name', valuation_group or '')
        self.hierarchy = get_driver_hierarchy(sectors_config)

        # Current driver states (loaded from DB or defaults)
        self._group_driver_states = {}      # GROUP-level drivers
        self._subgroup_driver_states = {}   # SUBGROUP-level drivers
        self._load_driver_states()

        logger.info(f"GroupAnalystAgent initialized: group={valuation_group}, "
                    f"subgroup={valuation_subgroup}")

    def _resolve_group_config(self, sectors_config: dict, valuation_group: str,
                               valuation_subgroup: str = '') -> dict:
        """
        Resolve group/subgroup config from sectors.yaml.

        Lookup order:
        1. valuation_subgroup (lowercase) - e.g., 'INDUSTRIALS_DEFENSE' -> 'industrials_defense'
        2. valuation_group (lowercase) - e.g., 'INDUSTRIALS' -> 'industrials'
        3. Follow primary_group reference if the config specifies one

        Args:
            sectors_config: Full sectors.yaml config
            valuation_group: The valuation group key (e.g., 'INDUSTRIALS')
            valuation_subgroup: The valuation subgroup key (e.g., 'INDUSTRIALS_DEFENSE')

        Returns:
            The resolved config dict with drivers and terminal assumptions
        """
        sectors = sectors_config.get('sectors', {})

        # 1. First try valuation_subgroup (most specific)
        subgroup_key = valuation_subgroup.lower() if valuation_subgroup else ''
        if subgroup_key and subgroup_key in sectors:
            config = sectors.get(subgroup_key, {})
            logger.debug(f"Config resolved via subgroup: {subgroup_key}")
            return config

        # 2. Try valuation_group
        group_key = valuation_group.lower() if valuation_group else ''
        config = sectors.get(group_key, {})

        # 3. If config has primary_group, follow the reference
        primary_group = config.get('primary_sector') or config.get('primary_group')
        if primary_group and primary_group in sectors:
            logger.debug(f"Following primary_group: {group_key} -> {primary_group}")
            config = sectors.get(primary_group, {})
        elif not config and group_key:
            logger.warning(f"No config found for group={group_key}, subgroup={subgroup_key}")

        return config

    def _load_driver_states(self):
        """Load driver states from MySQL for both GROUP and SUBGROUP levels."""
        if not self.mysql:
            self._init_drivers_from_config()
            return

        try:
            # Load GROUP-level drivers (valuation_group)
            group_drivers = self.mysql.query(
                """SELECT * FROM vs_drivers
                   WHERE driver_level = 'GROUP' AND valuation_group = %s""",
                (self.valuation_group,)
            )
            if group_drivers:
                for d in group_drivers:
                    key = f"GROUP_{d['driver_name']}"
                    self._group_driver_states[key] = self._parse_driver(d)

            # Load SUBGROUP-level drivers (valuation_subgroup)
            if self.valuation_subgroup:
                subgroup_drivers = self.mysql.query(
                    """SELECT * FROM vs_drivers
                       WHERE driver_level = 'SUBGROUP' AND valuation_subgroup = %s""",
                    (self.valuation_subgroup,)
                )
                if subgroup_drivers:
                    for d in subgroup_drivers:
                        key = f"SUBGROUP_{d['driver_name']}"
                        self._subgroup_driver_states[key] = self._parse_driver(d)

            logger.info(f"Loaded {len(self._group_driver_states)} group drivers, "
                        f"{len(self._subgroup_driver_states)} subgroup drivers from DB")

            # If no drivers found in DB, fall back to config
            if not self._group_driver_states:
                self._init_drivers_from_config()

        except Exception as e:
            logger.warning(f"Failed to load drivers from DB: {e}")
            self._init_drivers_from_config()

    def _parse_driver(self, d: dict) -> dict:
        """Parse a driver record from MySQL into internal format."""
        return {
            'level': d['driver_level'],
            'category': d.get('driver_category', ''),
            'name': d['driver_name'],
            'value': d['current_value'],
            'weight': float(d['weight']) if d.get('weight') else 0,
            'impact_direction': d.get('impact_direction', 'NEUTRAL'),
            'trend': d.get('trend', 'STABLE'),
        }

    def _init_drivers_from_config(self):
        """Initialize driver states from sectors.yaml config."""
        for category in ['demand_drivers', 'cost_drivers', 'regulatory_drivers',
                         'group_specific_drivers']:
            drivers = self.config.get(category, [])
            for driver in drivers:
                # All config drivers go to GROUP level by default
                key = f"GROUP_{driver['name']}"
                self._group_driver_states[key] = {
                    'level': 'GROUP',
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
            if self._is_relevant(e)
        ]

        if not relevant:
            logger.debug(f"No relevant news for {self.valuation_group}/{self.valuation_subgroup}")
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
        group_drivers_text = self._format_drivers(self._group_driver_states)
        subgroup_drivers_text = self._format_drivers(self._subgroup_driver_states) or '(none defined)'

        prompt = self.DRIVER_IMPACT_PROMPT.format(
            group=self.valuation_group,
            subgroup=self.valuation_subgroup or '(none)',
            headline=event.get('headline', ''),
            summary=event.get('summary', ''),
            severity=event.get('severity', 'MEDIUM'),
            group_drivers=group_drivers_text,
            subgroup_drivers=subgroup_drivers_text,
        )

        return self.llm.analyze_json(prompt)

    def _apply_driver_change(self, change: dict, source_event: dict):
        """Apply a driver change to state and log it."""
        driver_name = change.get('driver_name', '')
        driver_level = change.get('driver_level', 'GROUP')

        # Determine which state dict to update
        if driver_level == 'SUBGROUP':
            key = f"SUBGROUP_{driver_name}"
            state_dict = self._subgroup_driver_states
        else:
            key = f"GROUP_{driver_name}"
            state_dict = self._group_driver_states

        old_value = state_dict.get(key, {}).get('value', 'NOT_SET')
        new_value = change.get('new_state', old_value)
        direction = change.get('direction', 'STABLE')

        # Update in-memory state
        if key in state_dict:
            state_dict[key]['value'] = new_value
            state_dict[key]['trend'] = direction
            if direction == 'UP':
                state_dict[key]['impact_direction'] = 'POSITIVE'
            elif direction == 'DOWN':
                state_dict[key]['impact_direction'] = 'NEGATIVE'

        # Log change to MySQL
        if self.mysql:
            try:
                self.mysql.log_driver_change({
                    'driver_level': driver_level,
                    'driver_category': state_dict.get(key, {}).get('category', ''),
                    'driver_name': driver_name,
                    'valuation_group': self.valuation_group,
                    'valuation_subgroup': self.valuation_subgroup if driver_level == 'SUBGROUP' else None,
                    'sector': self.csv_group_name,
                    'old_value': str(old_value),
                    'new_value': str(new_value),
                    'change_reason': change.get('reasoning', ''),
                    'triggered_by': 'NEWS_EVENT',
                    'estimated_valuation_impact_pct': change.get('valuation_impact_pct'),
                })
            except Exception as e:
                logger.error(f"Failed to log driver change: {e}", exc_info=True)

    def calculate_outlook(self) -> dict:
        """
        Aggregate all driver states into group + subgroup outlook scores.
        Returns combined adjustments for growth and margins.

        Score range: -1.0 (very bearish) to +1.0 (very bullish)
        """
        # Calculate group-level score
        group_score = self._calculate_level_score(self._group_driver_states)
        group_positives, group_negatives = self._get_positive_negative_drivers(self._group_driver_states)

        # Calculate subgroup-level score
        subgroup_score = self._calculate_level_score(self._subgroup_driver_states)
        subgroup_positives, subgroup_negatives = self._get_positive_negative_drivers(self._subgroup_driver_states)

        # Weighted combination
        group_weight = self.hierarchy.get('group_weight', 0.20)
        subgroup_weight = self.hierarchy.get('subgroup_weight', 0.35)
        total_weight = group_weight + subgroup_weight

        # If no subgroup drivers, all weight goes to group
        if not self._subgroup_driver_states:
            combined_score = group_score
        else:
            combined_score = (
                group_score * group_weight +
                subgroup_score * subgroup_weight
            ) / total_weight if total_weight > 0 else 0

        combined_score = max(-1.0, min(1.0, combined_score))

        label = self._score_to_label(combined_score)
        growth_adj = self._score_to_growth_adjustment(combined_score)
        margin_adj = self._score_to_margin_adjustment(combined_score)

        result = {
            'valuation_group': self.valuation_group,
            'valuation_subgroup': self.valuation_subgroup,
            'sector': self.csv_group_name,  # For backward compatibility
            'sector_key': self.valuation_group.lower() if self.valuation_group else '',
            'group_score': round(group_score, 4),
            'subgroup_score': round(subgroup_score, 4),
            'outlook_score': round(combined_score, 4),  # For backward compatibility
            'outlook_label': label,
            'growth_adjustment': round(growth_adj, 4),
            'margin_adjustment': round(margin_adj, 4),
            'key_positives': group_positives + subgroup_positives,
            'key_negatives': group_negatives + subgroup_negatives,
            'group_driver_count': len(self._group_driver_states),
            'subgroup_driver_count': len(self._subgroup_driver_states),
            'group_drivers': self._group_driver_states.copy(),
            'subgroup_drivers': self._subgroup_driver_states.copy(),
            'as_of': datetime.now().isoformat(),
        }

        logger.info(f"Outlook for {self.valuation_subgroup or self.valuation_group}: "
                    f"{label} ({combined_score:+.3f}), "
                    f"growth adj={growth_adj:+.2%}, margin adj={margin_adj:+.2%}")

        return result

    # Alias for backward compatibility
    def calculate_sector_outlook(self) -> dict:
        """Alias for calculate_outlook() for backward compatibility."""
        return self.calculate_outlook()

    def _calculate_level_score(self, driver_states: dict) -> float:
        """Calculate weighted score for a set of drivers."""
        total_score = 0.0
        total_weight = 0.0

        for key, driver in driver_states.items():
            weight = driver.get('weight', 0)
            direction = driver.get('impact_direction', 'NEUTRAL')
            trend = driver.get('trend', 'STABLE')

            # Score: +1 for positive/up, -1 for negative/down, 0 for neutral
            if direction == 'POSITIVE' or trend == 'UP':
                score = 1.0
            elif direction == 'NEGATIVE' or trend == 'DOWN':
                score = -1.0
            else:
                score = 0.0

            total_score += score * weight
            total_weight += weight

        return total_score / total_weight if total_weight > 0 else 0.0

    def _get_positive_negative_drivers(self, driver_states: dict) -> tuple:
        """Get lists of positive and negative driver names."""
        positives = []
        negatives = []

        for key, driver in driver_states.items():
            direction = driver.get('impact_direction', 'NEUTRAL')
            trend = driver.get('trend', 'STABLE')
            name = driver.get('name', key)

            if direction == 'POSITIVE' or trend == 'UP':
                positives.append(name)
            elif direction == 'NEGATIVE' or trend == 'DOWN':
                negatives.append(name)

        return positives, negatives

    def get_terminal_parameters(self) -> dict:
        """
        Get terminal value parameters informed by outlook.
        Terminal margin and ROCE convergence ranges from config,
        adjusted by current driver state.
        """
        terminal = self.config.get('terminal_assumptions', {})
        margin_range = terminal.get('margin_range', [0.15, 0.22])
        roce_convergence = terminal.get('roce_convergence', 0.18)
        reinvestment = terminal.get('reinvestment_rate', 0.30)

        outlook = self.calculate_outlook()
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
            'valuation_group': self.valuation_group,
            'valuation_subgroup': self.valuation_subgroup,
            'sector': self.csv_group_name,
            'group_drivers': {k: {
                'value': v.get('value'),
                'weight': v.get('weight'),
                'direction': v.get('impact_direction'),
                'trend': v.get('trend'),
            } for k, v in self._group_driver_states.items()},
            'subgroup_drivers': {k: {
                'value': v.get('value'),
                'weight': v.get('weight'),
                'direction': v.get('impact_direction'),
                'trend': v.get('trend'),
            } for k, v in self._subgroup_driver_states.items()},
            'snapshot_time': datetime.now().isoformat(),
        }

    def get_group_drivers(self) -> dict:
        """Get current GROUP-level driver states."""
        return self._group_driver_states.copy()

    def get_subgroup_drivers(self) -> dict:
        """Get current SUBGROUP-level driver states."""
        return self._subgroup_driver_states.copy()

    def _is_relevant(self, event: dict) -> bool:
        """Check if a news event is relevant to this group/subgroup."""
        # MACRO events affect all
        if event.get('scope') == 'MACRO':
            return True
        if event.get('severity') in ('CRITICAL',):
            return True

        affected_sectors = event.get('affected_sectors', [])
        if isinstance(affected_sectors, list):
            for s in affected_sectors:
                if s and (s.lower() in self.csv_group_name.lower() or
                          self.csv_group_name.lower() in s.lower() or
                          s.lower() in self.valuation_group.lower() or
                          (self.valuation_subgroup and s.lower() in self.valuation_subgroup.lower())):
                    return True
        return False

    def _format_drivers(self, driver_states: dict) -> str:
        """Format driver states for LLM prompt."""
        if not driver_states:
            return ''
        lines = []
        for key, driver in driver_states.items():
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


# Backward compatibility alias
SectorAnalystAgent = GroupAnalystAgent
