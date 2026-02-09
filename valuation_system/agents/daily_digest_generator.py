"""
Daily Digest Generator
Produces an HTML intelligence email for the PM summarizing the last 24 hours:
1. Critical Alerts (CRITICAL/HIGH severity materiality alerts)
2. Discovered Drivers Pending PM approval
3. Value Buy Opportunities (VALUATION_GAP alerts)
4. Driver Changes (changelog entries)

Uses existing EmailSender and ValuationMySQLClient.
"""

import os
import sys
import logging
import traceback
from datetime import datetime, timedelta
from html import escape

from dotenv import load_dotenv

# Ensure valuation_system imports work regardless of execution context
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))

load_dotenv(os.path.join(os.path.dirname(__file__), '..', 'config', '.env'))

logger = logging.getLogger(__name__)

# GSheet base URL for the Discovered Drivers tab (Tab 7)
GSHEET_DRIVERS_ID = os.getenv('GSHEET_DRIVERS_ID', '')
GSHEET_DISCOVERED_DRIVERS_URL = (
    f"https://docs.google.com/spreadsheets/d/{GSHEET_DRIVERS_ID}/edit#gid=0"
    if GSHEET_DRIVERS_ID else ''
)

# PM email for fallback display
ALERT_EMAIL_TO = os.getenv('ALERT_EMAIL_TO', 'medury@gmail.com')


class DailyDigestGenerator:
    """
    Generates a daily intelligence digest email for the PM.
    Queries MySQL for last 24 hours of activity across materiality alerts,
    discovered drivers, value opportunities, and driver changes.
    """

    def __init__(self, mysql_client, email_sender):
        """
        Args:
            mysql_client: ValuationMySQLClient instance (has .query() and .query_one())
            email_sender: EmailSender instance (has .send() and .enabled)
        """
        self.mysql = mysql_client
        self.email = email_sender
        logger.info("DailyDigestGenerator initialized | email_enabled=%s", self.email.enabled)

    # =========================================================================
    # DATA QUERIES
    # =========================================================================

    def _get_critical_alerts(self) -> list:
        """
        Fetch CRITICAL and HIGH severity materiality alerts from the last 24 hours.
        Returns list of dicts with alert details.
        """
        sql = """
            SELECT
                ma.id,
                ma.alert_type,
                ma.severity,
                COALESCE(ac.nse_symbol, ma.valuation_group, ma.valuation_subgroup, 'MACRO') AS scope_name,
                ma.driver_affected,
                ma.current_value,
                ma.deviation_pct,
                ma.suggested_action,
                COALESCE(ma.reasoning, ma.signal_description) AS reasoning,
                ma.created_at
            FROM vs_materiality_alerts ma
            LEFT JOIN vs_active_companies ac ON ma.company_id = ac.company_id
            WHERE ma.severity IN ('CRITICAL', 'HIGH')
              AND ma.alert_type != 'VALUATION_GAP'
              AND ma.created_at >= DATE_SUB(NOW(), INTERVAL 24 HOUR)
            ORDER BY
                FIELD(ma.severity, 'CRITICAL', 'HIGH'),
                ABS(ma.deviation_pct) DESC
            LIMIT 15
        """
        try:
            results = self.mysql.query(sql)
            logger.info("Critical alerts fetched: %d rows", len(results))
            for r in results:
                logger.debug("  Alert id=%s type=%s scope=%s severity=%s",
                             r.get('id'), r.get('alert_type'), r.get('scope_name'), r.get('severity'))
            return results
        except Exception as e:
            logger.error("Failed to fetch critical alerts: %s\n%s", e, traceback.format_exc())
            return []

    def _get_pending_discoveries(self) -> list:
        """
        Fetch all PENDING discovered drivers (not time-bounded -- PM needs to see
        the full backlog of items awaiting approval).
        Returns list of dicts.
        """
        sql = """
            SELECT
                id,
                driver_name,
                driver_level,
                valuation_group,
                valuation_subgroup,
                reasoning,
                confidence,
                discovered_at
            FROM vs_discovered_drivers
            WHERE status = 'PENDING'
            ORDER BY
                FIELD(confidence, 'HIGH', 'MEDIUM', 'LOW'),
                discovered_at DESC
        """
        try:
            results = self.mysql.query(sql)
            logger.info("Pending discovered drivers fetched: %d rows", len(results))
            for r in results:
                logger.debug("  Discovery id=%s name=%s level=%s confidence=%s",
                             r.get('id'), r.get('driver_name'), r.get('driver_level'), r.get('confidence'))
            return results
        except Exception as e:
            logger.error("Failed to fetch pending discoveries: %s\n%s", e, traceback.format_exc())
            return []

    def _get_value_opportunities(self) -> tuple:
        """
        Fetch VALUATION_GAP alerts with REVALUE_NOW or WATCH suggested action
        from the last 24 hours, joined with company details.
        Returns (list_of_dicts, total_count).
        """
        # Get total count first
        count_sql = """
            SELECT COUNT(*) as total
            FROM vs_materiality_alerts
            WHERE alert_type = 'VALUATION_GAP'
              AND suggested_action IN ('REVALUE_NOW', 'WATCH')
              AND created_at >= DATE_SUB(NOW(), INTERVAL 24 HOUR)
        """
        sql = """
            SELECT
                ma.id,
                ma.valuation_group,
                ma.valuation_subgroup,
                COALESCE(ac.nse_symbol, '') AS nse_symbol,
                COALESCE(ac.company_name, '') AS company_name,
                ma.signal_description,
                ma.affected_companies,
                ma.suggested_action,
                ma.severity,
                ma.deviation_pct,
                ma.driver_affected,
                ma.current_value,
                COALESCE(ma.reasoning, ma.signal_description) AS reasoning,
                ma.created_at
            FROM vs_materiality_alerts ma
            LEFT JOIN vs_active_companies ac ON ma.company_id = ac.company_id
            WHERE ma.alert_type = 'VALUATION_GAP'
              AND ma.suggested_action IN ('REVALUE_NOW', 'WATCH')
              AND ma.created_at >= DATE_SUB(NOW(), INTERVAL 24 HOUR)
            ORDER BY
                FIELD(ma.suggested_action, 'REVALUE_NOW', 'WATCH'),
                ABS(ma.deviation_pct) DESC
            LIMIT 25
        """
        try:
            total_row = self.mysql.query_one(count_sql)
            total_count = total_row.get('total', 0) if total_row else 0
            results = self.mysql.query(sql)
            logger.info("Value opportunities fetched: %d rows (total: %d)", len(results), total_count)
            for r in results:
                logger.debug("  Opportunity id=%s group=%s action=%s affected=%s",
                             r.get('id'), r.get('valuation_group'),
                             r.get('suggested_action'), r.get('affected_companies'))
            return results, total_count
        except Exception as e:
            logger.error("Failed to fetch value opportunities: %s\n%s", e, traceback.format_exc())
            return [], 0

    def _get_driver_changes(self) -> list:
        """
        Fetch driver changelog entries from the last 24 hours.
        Returns list of dicts.
        """
        sql = """
            SELECT
                id,
                change_timestamp,
                driver_level,
                driver_name,
                valuation_group,
                old_value,
                new_value,
                change_reason,
                triggered_by
            FROM vs_driver_changelog
            WHERE change_timestamp >= DATE_SUB(NOW(), INTERVAL 24 HOUR)
            ORDER BY change_timestamp DESC
            LIMIT 30
        """
        try:
            results = self.mysql.query(sql)
            logger.info("Driver changes fetched: %d rows", len(results))
            for r in results:
                logger.debug("  Change id=%s driver=%s triggered_by=%s",
                             r.get('id'), r.get('driver_name'), r.get('triggered_by'))
            return results
        except Exception as e:
            logger.error("Failed to fetch driver changes: %s\n%s", e, traceback.format_exc())
            return []

    def _get_news_events(self) -> list:
        """
        Fetch news events from the last 24 hours (MEDIUM+ severity).
        Returns list of dicts.
        """
        sql = """
            SELECT
                id,
                event_date,
                event_type,
                scope,
                headline,
                summary,
                severity,
                source,
                source_url,
                valuation_impact_pct
            FROM vs_event_timeline
            WHERE event_date >= DATE_SUB(CURDATE(), INTERVAL 1 DAY)
              AND severity IN ('CRITICAL', 'HIGH', 'MEDIUM')
            ORDER BY
                FIELD(severity, 'CRITICAL', 'HIGH', 'MEDIUM'),
                event_timestamp DESC
            LIMIT 20
        """
        try:
            results = self.mysql.query(sql)
            logger.info("News events fetched: %d rows", len(results))
            return results
        except Exception as e:
            logger.error("Failed to fetch news events: %s\n%s", e, traceback.format_exc())
            return []

    # =========================================================================
    # HTML RENDERING
    # =========================================================================

    def _truncate(self, text, max_len: int) -> str:
        """Safely truncate text to max_len, adding ellipsis if needed."""
        if text is None:
            return ''
        text = str(text)
        if len(text) <= max_len:
            return text
        return text[:max_len - 3] + '...'

    def _severity_color(self, severity: str) -> str:
        """Return inline CSS color for severity level."""
        colors = {
            'CRITICAL': '#d32f2f',
            'HIGH': '#e65100',
            'MEDIUM': '#f9a825',
            'LOW': '#558b2f',
        }
        return colors.get(str(severity).upper(), '#757575')

    def _confidence_color(self, confidence: str) -> str:
        """Return inline CSS color for confidence level."""
        colors = {
            'HIGH': '#2e7d32',
            'MEDIUM': '#f57f17',
            'LOW': '#c62828',
        }
        return colors.get(str(confidence).upper(), '#757575')

    def _action_badge(self, action: str) -> str:
        """Render a suggested_action as an inline badge."""
        colors = {
            'REVALUE_NOW': '#d32f2f',
            'WATCH': '#1565c0',
            'REDUCE_EXPOSURE': '#6a1b9a',
        }
        bg = colors.get(str(action).upper(), '#757575')
        return (
            f'<span style="background:{bg}; color:#fff; padding:2px 8px; '
            f'border-radius:3px; font-size:11px; font-weight:bold;">'
            f'{escape(str(action or ""))}</span>'
        )

    def _format_timestamp(self, ts) -> str:
        """Format a timestamp for display."""
        if ts is None:
            return ''
        if isinstance(ts, datetime):
            return ts.strftime('%Y-%m-%d %H:%M')
        return str(ts)[:16]

    def _format_value(self, val) -> str:
        """Format a numeric or text value for display."""
        if val is None:
            return 'N/A'
        try:
            fval = float(val)
            if abs(fval) >= 1000:
                return f'{fval:,.1f}'
            return f'{fval:.2f}'
        except (ValueError, TypeError):
            return escape(str(val))

    def _render_section_critical_alerts(self, alerts: list) -> str:
        """Render Section 1: Critical Alerts."""
        count = len(alerts)
        header = f'Critical Alerts ({count})'

        if count == 0:
            return self._render_empty_section(header, 'No critical or high-severity alerts in the last 24 hours.')

        rows_html = ''
        for a in alerts:
            severity = str(a.get('severity', '')).upper()
            sev_color = self._severity_color(severity)
            deviation = a.get('deviation_pct')
            dev_str = f'{float(deviation):+.1f}%' if deviation is not None else 'N/A'

            rows_html += f'''
            <tr>
                <td style="padding:6px 10px; border:1px solid #e0e0e0;">{escape(str(a.get('alert_type', '')))}</td>
                <td style="padding:6px 10px; border:1px solid #e0e0e0; color:{sev_color}; font-weight:bold;">{escape(severity)}</td>
                <td style="padding:6px 10px; border:1px solid #e0e0e0; font-weight:bold;">{escape(str(a.get('scope_name', '')))}</td>
                <td style="padding:6px 10px; border:1px solid #e0e0e0;">{escape(str(a.get('driver_affected', '') or ''))}</td>
                <td style="padding:6px 10px; border:1px solid #e0e0e0; text-align:right;">{self._format_value(a.get('current_value'))}</td>
                <td style="padding:6px 10px; border:1px solid #e0e0e0; text-align:right;">{dev_str}</td>
                <td style="padding:6px 10px; border:1px solid #e0e0e0;">{self._action_badge(a.get('suggested_action', ''))}</td>
            </tr>'''

        return f'''
        <div style="margin-bottom:28px;">
            <h2 style="color:#d32f2f; border-bottom:2px solid #d32f2f; padding-bottom:6px; font-size:18px;">
                {header}
            </h2>
            <table style="border-collapse:collapse; width:100%; font-size:13px;">
                <tr style="background:#ffebee;">
                    <th style="padding:8px 10px; border:1px solid #e0e0e0; text-align:left;">Alert Type</th>
                    <th style="padding:8px 10px; border:1px solid #e0e0e0; text-align:left;">Severity</th>
                    <th style="padding:8px 10px; border:1px solid #e0e0e0; text-align:left;">Scope</th>
                    <th style="padding:8px 10px; border:1px solid #e0e0e0; text-align:left;">Driver Affected</th>
                    <th style="padding:8px 10px; border:1px solid #e0e0e0; text-align:right;">Current Value</th>
                    <th style="padding:8px 10px; border:1px solid #e0e0e0; text-align:right;">Deviation</th>
                    <th style="padding:8px 10px; border:1px solid #e0e0e0; text-align:left;">Action</th>
                </tr>
                {rows_html}
            </table>
        </div>'''

    def _render_section_news_events(self, events: list) -> str:
        """Render News Events section — what the system scanned and classified."""
        count = len(events)
        header = f'News Intelligence ({count} events)'

        if count == 0:
            return self._render_empty_section(header, 'No significant news events captured in the last 24 hours.')

        rows_html = ''
        for e in events:
            sev = str(e.get('severity', 'LOW'))
            sev_color = self._severity_color(sev)
            headline = self._truncate(e.get('headline', ''), 120)
            source = str(e.get('source', '')).replace('_', ' ').title()
            summary = self._truncate(e.get('summary', ''), 150)
            scope = str(e.get('scope', '')).title()
            url = e.get('source_url', '')
            headline_html = f'<a href="{escape(url)}" style="color:#1565c0; text-decoration:none;">{escape(headline)}</a>' if url else escape(headline)

            rows_html += f'''
            <tr>
                <td style="padding:6px 10px; border:1px solid #e0e0e0;">
                    <span style="color:{sev_color}; font-weight:bold; font-size:11px;">{sev}</span>
                </td>
                <td style="padding:6px 10px; border:1px solid #e0e0e0;">{headline_html}</td>
                <td style="padding:6px 10px; border:1px solid #e0e0e0; font-size:12px; color:#616161;">{escape(source)}</td>
                <td style="padding:6px 10px; border:1px solid #e0e0e0; font-size:12px; color:#757575;">{escape(summary)}</td>
            </tr>'''

        return f'''
        <div style="margin-bottom:28px;">
            <h2 style="color:#0d47a1; border-bottom:2px solid #0d47a1; padding-bottom:6px; font-size:18px;">
                {header}
            </h2>
            <table style="width:100%; border-collapse:collapse; font-size:13px;">
                <tr style="background:#e3f2fd;">
                    <th style="padding:8px 10px; border:1px solid #e0e0e0; text-align:left; width:70px;">Severity</th>
                    <th style="padding:8px 10px; border:1px solid #e0e0e0; text-align:left;">Headline</th>
                    <th style="padding:8px 10px; border:1px solid #e0e0e0; text-align:left; width:90px;">Source</th>
                    <th style="padding:8px 10px; border:1px solid #e0e0e0; text-align:left;">Summary</th>
                </tr>
                {rows_html}
            </table>
        </div>'''

    def _render_section_pending_discoveries(self, discoveries: list) -> str:
        """Render Section 2: Discovered Drivers Pending Approval."""
        count = len(discoveries)
        header = f'Discovered Drivers Pending Approval ({count})'

        gsheet_link = ''
        if GSHEET_DISCOVERED_DRIVERS_URL:
            gsheet_link = (
                f' &mdash; <a href="{GSHEET_DISCOVERED_DRIVERS_URL}" '
                f'style="color:#1565c0; font-size:13px;">Review in Google Sheet (Tab 7)</a>'
            )

        if count == 0:
            return self._render_empty_section(header + gsheet_link, 'No drivers pending approval.')

        rows_html = ''
        for d in discoveries:
            conf = str(d.get('confidence', 'MEDIUM')).upper()
            conf_color = self._confidence_color(conf)
            reasoning_trunc = self._truncate(d.get('reasoning', ''), 100)
            group_display = str(d.get('valuation_group', '') or '')
            if d.get('valuation_subgroup'):
                group_display += f' / {d["valuation_subgroup"]}'

            rows_html += f'''
            <tr>
                <td style="padding:6px 10px; border:1px solid #e0e0e0; font-weight:bold;">{escape(str(d.get('driver_name', '')))}</td>
                <td style="padding:6px 10px; border:1px solid #e0e0e0;">{escape(str(d.get('driver_level', '')))}</td>
                <td style="padding:6px 10px; border:1px solid #e0e0e0;">{escape(group_display)}</td>
                <td style="padding:6px 10px; border:1px solid #e0e0e0; font-size:12px; color:#555;">{escape(reasoning_trunc)}</td>
                <td style="padding:6px 10px; border:1px solid #e0e0e0; color:{conf_color}; font-weight:bold; text-align:center;">{escape(conf)}</td>
            </tr>'''

        return f'''
        <div style="margin-bottom:28px;">
            <h2 style="color:#1565c0; border-bottom:2px solid #1565c0; padding-bottom:6px; font-size:18px;">
                {header}{gsheet_link}
            </h2>
            <table style="border-collapse:collapse; width:100%; font-size:13px;">
                <tr style="background:#e3f2fd;">
                    <th style="padding:8px 10px; border:1px solid #e0e0e0; text-align:left;">Driver Name</th>
                    <th style="padding:8px 10px; border:1px solid #e0e0e0; text-align:left;">Level</th>
                    <th style="padding:8px 10px; border:1px solid #e0e0e0; text-align:left;">Group / Subgroup</th>
                    <th style="padding:8px 10px; border:1px solid #e0e0e0; text-align:left;">Reasoning</th>
                    <th style="padding:8px 10px; border:1px solid #e0e0e0; text-align:center;">Confidence</th>
                </tr>
                {rows_html}
            </table>
        </div>'''

    def _render_section_value_opportunities(self, opportunities: list, total_count: int = 0) -> str:
        """Render Section 3: Value Buy Opportunities."""
        count = len(opportunities)
        total_label = f' of {total_count}' if total_count > count else ''
        header = f'Value Buy Opportunities (top {count}{total_label})'

        if count == 0:
            return self._render_empty_section(header, 'No valuation gap opportunities in the last 24 hours.')

        rows_html = ''
        for opp in opportunities:
            company_display = str(opp.get('nse_symbol', '') or '')
            if opp.get('company_name'):
                company_display = f"{opp['company_name']} ({company_display})" if company_display else str(opp['company_name'])

            group_display = str(opp.get('valuation_group', '') or '')
            affected = opp.get('affected_companies')
            affected_str = str(affected) if affected is not None else 'N/A'
            deviation = opp.get('deviation_pct')
            dev_str = f'{float(deviation):+.1f}%' if deviation is not None else 'N/A'
            signal = self._truncate(opp.get('signal_description', ''), 120)

            rows_html += f'''
            <tr>
                <td style="padding:6px 10px; border:1px solid #e0e0e0; font-weight:bold;">{escape(company_display) if company_display else escape(group_display)}</td>
                <td style="padding:6px 10px; border:1px solid #e0e0e0;">{escape(group_display)}</td>
                <td style="padding:6px 10px; border:1px solid #e0e0e0; text-align:center;">{escape(affected_str)}</td>
                <td style="padding:6px 10px; border:1px solid #e0e0e0; text-align:right;">{dev_str}</td>
                <td style="padding:6px 10px; border:1px solid #e0e0e0;">{self._action_badge(opp.get('suggested_action', ''))}</td>
                <td style="padding:6px 10px; border:1px solid #e0e0e0; font-size:12px; color:#555;">{escape(signal)}</td>
            </tr>'''

        return f'''
        <div style="margin-bottom:28px;">
            <h2 style="color:#2e7d32; border-bottom:2px solid #2e7d32; padding-bottom:6px; font-size:18px;">
                {header}
            </h2>
            <table style="border-collapse:collapse; width:100%; font-size:13px;">
                <tr style="background:#e8f5e9;">
                    <th style="padding:8px 10px; border:1px solid #e0e0e0; text-align:left;">Company / Group</th>
                    <th style="padding:8px 10px; border:1px solid #e0e0e0; text-align:left;">Valuation Group</th>
                    <th style="padding:8px 10px; border:1px solid #e0e0e0; text-align:center;">Affected Cos.</th>
                    <th style="padding:8px 10px; border:1px solid #e0e0e0; text-align:right;">Gap %</th>
                    <th style="padding:8px 10px; border:1px solid #e0e0e0; text-align:left;">Action</th>
                    <th style="padding:8px 10px; border:1px solid #e0e0e0; text-align:left;">Signal</th>
                </tr>
                {rows_html}
            </table>
        </div>'''

    def _render_section_driver_changes(self, changes: list) -> str:
        """Render Section 4: Driver Changes."""
        count = len(changes)
        header = f'Driver Changes ({count})'

        if count == 0:
            return self._render_empty_section(header, 'No driver changes in the last 24 hours.')

        rows_html = ''
        for c in changes:
            ts = self._format_timestamp(c.get('change_timestamp'))
            reason_trunc = self._truncate(c.get('change_reason', ''), 60)
            triggered = str(c.get('triggered_by', '') or '').replace('_', ' ')

            rows_html += f'''
            <tr>
                <td style="padding:6px 10px; border:1px solid #e0e0e0; white-space:nowrap;">{escape(ts)}</td>
                <td style="padding:6px 10px; border:1px solid #e0e0e0; font-weight:bold;">{escape(str(c.get('driver_name', '')))}</td>
                <td style="padding:6px 10px; border:1px solid #e0e0e0;">{escape(str(c.get('driver_level', '') or ''))}</td>
                <td style="padding:6px 10px; border:1px solid #e0e0e0;">{escape(str(c.get('valuation_group', '') or ''))}</td>
                <td style="padding:6px 10px; border:1px solid #e0e0e0; font-size:12px; color:#555;">{escape(reason_trunc)}</td>
                <td style="padding:6px 10px; border:1px solid #e0e0e0;">{escape(triggered)}</td>
            </tr>'''

        return f'''
        <div style="margin-bottom:28px;">
            <h2 style="color:#6a1b9a; border-bottom:2px solid #6a1b9a; padding-bottom:6px; font-size:18px;">
                {header}
            </h2>
            <table style="border-collapse:collapse; width:100%; font-size:13px;">
                <tr style="background:#f3e5f5;">
                    <th style="padding:8px 10px; border:1px solid #e0e0e0; text-align:left;">Timestamp</th>
                    <th style="padding:8px 10px; border:1px solid #e0e0e0; text-align:left;">Driver Name</th>
                    <th style="padding:8px 10px; border:1px solid #e0e0e0; text-align:left;">Level</th>
                    <th style="padding:8px 10px; border:1px solid #e0e0e0; text-align:left;">Valuation Group</th>
                    <th style="padding:8px 10px; border:1px solid #e0e0e0; text-align:left;">Reason</th>
                    <th style="padding:8px 10px; border:1px solid #e0e0e0; text-align:left;">Triggered By</th>
                </tr>
                {rows_html}
            </table>
        </div>'''

    def _render_empty_section(self, header: str, message: str) -> str:
        """Render an empty section with a muted message."""
        return f'''
        <div style="margin-bottom:28px;">
            <h2 style="color:#757575; border-bottom:1px solid #e0e0e0; padding-bottom:6px; font-size:18px;">
                {header}
            </h2>
            <p style="color:#9e9e9e; font-style:italic; padding:10px 0;">{message}</p>
        </div>'''

    # =========================================================================
    # DIGEST ASSEMBLY
    # =========================================================================

    def generate_digest_html(self) -> str:
        """
        Query MySQL and build a complete HTML email body with all 4 sections.
        Returns the full HTML string.
        """
        logger.info("Generating daily digest HTML...")
        generation_start = datetime.now()

        # Fetch all data
        critical_alerts = self._get_critical_alerts()
        news_events = self._get_news_events()
        pending_discoveries = self._get_pending_discoveries()
        value_opportunities, value_total_count = self._get_value_opportunities()
        driver_changes = self._get_driver_changes()

        # Build summary counts for the header
        total_items = (
            len(critical_alerts) + len(news_events) + len(pending_discoveries)
            + len(value_opportunities) + len(driver_changes)
        )

        today_str = datetime.now().strftime('%A, %B %d, %Y')
        generated_at = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

        # Render each section
        section_alerts = self._render_section_critical_alerts(critical_alerts)
        section_news = self._render_section_news_events(news_events)
        section_discoveries = self._render_section_pending_discoveries(pending_discoveries)
        section_opportunities = self._render_section_value_opportunities(value_opportunities, value_total_count)
        section_changes = self._render_section_driver_changes(driver_changes)

        # Summary bar at the top
        summary_bar = f'''
        <div style="background:#f5f5f5; border-radius:6px; padding:14px 20px; margin-bottom:24px; font-size:13px;">
            <table style="width:100%; border:none;">
                <tr>
                    <td style="text-align:center; padding:4px 12px;">
                        <span style="font-size:22px; font-weight:bold; color:#d32f2f;">{len(critical_alerts)}</span><br/>
                        <span style="color:#757575;">Critical Alerts</span>
                    </td>
                    <td style="text-align:center; padding:4px 12px;">
                        <span style="font-size:22px; font-weight:bold; color:#0d47a1;">{len(news_events)}</span><br/>
                        <span style="color:#757575;">News Events</span>
                    </td>
                    <td style="text-align:center; padding:4px 12px;">
                        <span style="font-size:22px; font-weight:bold; color:#1565c0;">{len(pending_discoveries)}</span><br/>
                        <span style="color:#757575;">Pending Drivers</span>
                    </td>
                    <td style="text-align:center; padding:4px 12px;">
                        <span style="font-size:22px; font-weight:bold; color:#2e7d32;">{len(value_opportunities)}</span><br/>
                        <span style="color:#757575;">Value Opps ({value_total_count} total)</span>
                    </td>
                    <td style="text-align:center; padding:4px 12px;">
                        <span style="font-size:22px; font-weight:bold; color:#6a1b9a;">{len(driver_changes)}</span><br/>
                        <span style="color:#757575;">Driver Changes</span>
                    </td>
                </tr>
            </table>
        </div>'''

        # Assemble full HTML
        html = f'''<!DOCTYPE html>
<html>
<head>
    <meta charset="utf-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
</head>
<body style="font-family:'Segoe UI', Arial, Helvetica, sans-serif; background:#fafafa; margin:0; padding:0;">
    <div style="max-width:860px; margin:20px auto; background:#ffffff; border:1px solid #e0e0e0; border-radius:8px; overflow:hidden;">

        <!-- Header -->
        <div style="background:linear-gradient(135deg, #1a237e, #283593); color:#ffffff; padding:24px 28px;">
            <h1 style="margin:0 0 6px 0; font-size:22px; font-weight:600;">
                Valuation System - Daily Intelligence Digest
            </h1>
            <p style="margin:0; font-size:14px; opacity:0.85;">{today_str}</p>
        </div>

        <!-- Body -->
        <div style="padding:24px 28px;">

            {summary_bar}

            {section_alerts}

            {section_news}

            {section_discoveries}

            {section_opportunities}

            {section_changes}

        </div>

        <!-- Footer -->
        <div style="background:#f5f5f5; padding:16px 28px; border-top:1px solid #e0e0e0; font-size:11px; color:#9e9e9e;">
            <p style="margin:0;">
                Generated at {generated_at} by Agentic Valuation System v1.0.0
                | {total_items} total items
                | Sent to {escape(ALERT_EMAIL_TO)}
            </p>
        </div>

    </div>
</body>
</html>'''

        elapsed_ms = (datetime.now() - generation_start).total_seconds() * 1000
        logger.info("Daily digest HTML generated | sections=4 | total_items=%d | elapsed=%.0fms",
                     total_items, elapsed_ms)

        return html

    # =========================================================================
    # SEND
    # =========================================================================

    def send_digest(self) -> bool:
        """
        Generate and send the daily digest email.

        Returns:
            True if the email was sent successfully.
            False if sending failed or email is disabled (HTML is still generated
            and logged for testing/debugging).
        """
        logger.info("=== Daily Digest: starting generation and send ===")

        try:
            html = self.generate_digest_html()
        except Exception as e:
            logger.error("Failed to generate digest HTML: %s\n%s", e, traceback.format_exc())
            return False

        if not self.email.enabled:
            logger.warning(
                "Email sender is disabled (SMTP not configured). "
                "Digest HTML was generated but cannot be sent. "
                "Set SMTP_USER, SMTP_PASSWORD, and ALERT_EMAIL_TO in .env to enable."
            )
            # Still return the HTML via debug log so it can be captured in tests
            logger.debug("Generated digest HTML length: %d chars", len(html))
            return False

        today_str = datetime.now().strftime('%Y-%m-%d')
        subject = f"[VALUATION DIGEST] Daily Intelligence - {today_str}"

        try:
            sent = self.email.send(
                subject=subject,
                body_html=html,
                body_text=None,
                priority='normal'
            )
            if sent:
                logger.info("Daily digest email sent successfully | subject=%s", subject)
            else:
                logger.warning("EmailSender.send() returned False | subject=%s", subject)
            return sent
        except Exception as e:
            logger.error("Failed to send daily digest email: %s\n%s", e, traceback.format_exc())
            return False


# =============================================================================
# CLI entry point for manual testing / cron
# =============================================================================
if __name__ == '__main__':
    import argparse

    logging.basicConfig(
        level=logging.DEBUG,
        format='%(asctime)s [%(levelname)s] %(name)s: %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )

    parser = argparse.ArgumentParser(description='Generate and optionally send the daily PM digest.')
    parser.add_argument('--send', action='store_true', help='Actually send the email (default: generate HTML only)')
    parser.add_argument('--output', type=str, default=None, help='Write HTML to this file path instead of stdout')
    args = parser.parse_args()

    from valuation_system.storage.mysql_client import ValuationMySQLClient
    from valuation_system.notifications.email_sender import EmailSender

    mysql_client = ValuationMySQLClient.get_instance()
    email_sender = EmailSender()

    generator = DailyDigestGenerator(mysql_client=mysql_client, email_sender=email_sender)

    if args.send:
        success = generator.send_digest()
        logger.info("send_digest() returned %s", success)
    else:
        html = generator.generate_digest_html()
        if args.output:
            with open(args.output, 'w', encoding='utf-8') as f:
                f.write(html)
            logger.info("HTML written to %s (%d bytes)", args.output, len(html))
        else:
            print(html)
