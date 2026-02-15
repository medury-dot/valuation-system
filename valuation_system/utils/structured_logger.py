"""
Structured Logger - JSON-formatted logging for agent activity tracking.
Enables parsing agent metrics for dashboards and analysis.
"""

import json
import logging
from datetime import datetime
from typing import Any, Dict, Optional


class StructuredLogger:
    """
    JSON-structured logger for agent activity.
    Outputs both human-readable logs and machine-parseable JSON metrics.
    Optionally writes to MySQL vs_agent_activity_log table for persistent tracking.
    """

    def __init__(self, agent_name: str, logger: Optional[logging.Logger] = None,
                 mysql_client=None):
        self.agent_name = agent_name
        self.logger = logger or logging.getLogger(f'valuation_system.{agent_name}')
        self.mysql = mysql_client  # Optional: if provided, logs persist to MySQL

    def log_action(self, action: str, metrics: Dict[str, Any] = None,
                   level: str = 'INFO', **kwargs):
        """
        Log an agent action with structured metrics.
        Writes to both log file and MySQL (if mysql_client provided).

        Args:
            action: What the agent is doing (e.g., "scan_complete", "drivers_updated")
            metrics: Dictionary of metrics (counts, durations, etc.)
            level: Log level (DEBUG/INFO/WARNING/ERROR)
            **kwargs: Additional fields to include in log
        """
        log_entry = {
            'timestamp': datetime.now().isoformat(),
            'agent': self.agent_name,
            'action': action,
            **(metrics or {}),
            **kwargs
        }

        # Human-readable message
        message_parts = [f"{self.agent_name}.{action}"]
        if metrics:
            metric_str = ', '.join(f"{k}={v}" for k, v in metrics.items())
            message_parts.append(f"({metric_str})")

        message = ' '.join(message_parts)

        # Log both message and JSON
        log_func = getattr(self.logger, level.lower(), self.logger.info)
        log_func(f"{message} | JSON: {json.dumps(log_entry)}")

        # Persist to MySQL if available
        self._write_to_mysql(action, metrics, kwargs)

    def log_cycle_start(self, cycle_type: str):
        """Log the start of an agent cycle."""
        self.log_action(
            'cycle_start',
            {'cycle_type': cycle_type},
            level='INFO'
        )

    def log_cycle_complete(self, cycle_type: str, elapsed_ms: float,
                          metrics: Dict[str, Any] = None, status: str = 'success'):
        """Log the completion of an agent cycle."""
        self.log_action(
            'cycle_complete',
            {
                'cycle_type': cycle_type,
                'elapsed_ms': round(elapsed_ms, 2),
                'status': status,
                **(metrics or {})
            },
            level='INFO'
        )

    def log_source_scan(self, source: str, articles_found: int,
                       significant_events: int, elapsed_ms: float):
        """Log results of scanning a single news source."""
        self.log_action(
            'source_scanned',
            {
                'source': source,
                'articles_found': articles_found,
                'significant_events': significant_events,
                'elapsed_ms': round(elapsed_ms, 2)
            },
            level='INFO'
        )

    def log_driver_update(self, driver_name: str, old_value: Any, new_value: Any,
                         change_reason: str):
        """Log a driver value change."""
        self.log_action(
            'driver_updated',
            {
                'driver_name': driver_name,
                'old_value': old_value,
                'new_value': new_value,
                'change_reason': change_reason
            },
            level='INFO'
        )

    def log_llm_call(self, purpose: str, tokens: int, cost_usd: float,
                    elapsed_ms: float):
        """Log an LLM API call."""
        self.log_action(
            'llm_call',
            {
                'purpose': purpose,
                'tokens': tokens,
                'cost_usd': round(cost_usd, 4),
                'elapsed_ms': round(elapsed_ms, 2)
            },
            level='DEBUG'
        )

    def log_error(self, error_type: str, error_message: str, **kwargs):
        """Log an error with context."""
        self.log_action(
            'error',
            {
                'error_type': error_type,
                'error_message': str(error_message),
                **kwargs
            },
            level='ERROR'
        )

    def log_batch_summary(self, action: str, total: int, succeeded: int,
                         failed: int, skipped: int = 0, elapsed_ms: float = 0):
        """Log summary of a batch operation."""
        self.log_action(
            f'{action}_summary',
            {
                'total': total,
                'succeeded': succeeded,
                'failed': failed,
                'skipped': skipped,
                'success_rate_pct': round((succeeded / total * 100) if total > 0 else 0, 1),
                'elapsed_ms': round(elapsed_ms, 2)
            },
            level='INFO'
        )

    def _write_to_mysql(self, action: str, metrics: Dict[str, Any] = None,
                       extra_fields: Dict[str, Any] = None):
        """
        Write log entry to MySQL vs_agent_activity_log table.
        Silently skips if MySQL is unavailable (degrades gracefully).
        """
        if not self.mysql:
            return  # MySQL not available, skip persistence

        try:
            # Extract key fields from metrics and extra_fields
            cycle_type = (metrics or {}).get('cycle_type') or (extra_fields or {}).get('cycle_type')
            elapsed_ms = (metrics or {}).get('elapsed_ms')
            status = (extra_fields or {}).get('status', 'SUCCESS')
            error_message = (extra_fields or {}).get('error_message')

            # Build metrics JSON (exclude fields already in columns)
            metrics_json = {k: v for k, v in (metrics or {}).items()
                          if k not in ('cycle_type', 'elapsed_ms', 'status', 'error_message')}
            if extra_fields:
                metrics_json.update({k: v for k, v in extra_fields.items()
                                   if k not in ('cycle_type', 'elapsed_ms', 'status', 'error_message')})

            self.mysql.execute(
                """INSERT INTO vs_agent_activity_log
                   (agent_name, cycle_type, action, metrics, elapsed_ms, status, error_message)
                   VALUES (%s, %s, %s, %s, %s, %s, %s)""",
                (
                    self.agent_name,
                    cycle_type,
                    action,
                    json.dumps(metrics_json) if metrics_json else None,
                    elapsed_ms,
                    status,
                    error_message
                )
            )
        except Exception as e:
            # Don't fail the agent if MySQL write fails - log and continue
            self.logger.debug(f"Failed to write activity log to MySQL: {e}")
