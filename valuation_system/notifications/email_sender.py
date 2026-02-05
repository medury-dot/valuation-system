"""
Email Sender
Send alerts, daily digests, and weekly summaries to PM.
Uses SMTP (Gmail) with graceful fallback if email is not configured.
"""

import os
import logging
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.application import MIMEApplication
from datetime import datetime

from dotenv import load_dotenv

from valuation_system.utils.resilience import retry_with_backoff, check_internet

logger = logging.getLogger(__name__)

load_dotenv(os.path.join(os.path.dirname(__file__), '..', 'config', '.env'))


class EmailSender:
    """
    Email notifications for the valuation system.

    Email types:
    1. Valuation Alert: Material change in intrinsic value
    2. Daily Digest: Summary of all valuations, events, changes
    3. Weekly Summary: Performance tracking, accuracy, outlook
    4. Critical Alert: Immediate notification for critical events
    """

    def __init__(self):
        self.smtp_host = os.getenv('SMTP_HOST', 'smtp.gmail.com')
        self.smtp_port = int(os.getenv('SMTP_PORT', 587))
        self.smtp_user = os.getenv('SMTP_USER', '')
        self.smtp_password = os.getenv('SMTP_PASSWORD', '')
        self.recipient = os.getenv('ALERT_EMAIL_TO', '')
        self.enabled = bool(self.smtp_user and self.smtp_password and self.recipient)

        if not self.enabled:
            logger.warning("Email sending disabled: SMTP credentials not configured")

    @retry_with_backoff(max_retries=2, base_delay=5.0)
    def send(self, subject: str, body_html: str, body_text: str = None,
             priority: str = 'normal', attachments: list = None) -> bool:
        """
        Send an email.

        Args:
            subject: Email subject line
            body_html: HTML body content
            body_text: Plain text fallback (auto-generated from HTML if not provided)
            priority: 'high' or 'normal'
            attachments: List of (filename, bytes) tuples

        Returns:
            True if sent successfully, False otherwise.
        """
        if not self.enabled:
            logger.info(f"Email not sent (disabled): {subject}")
            return False

        if not check_internet():
            logger.warning(f"No internet, cannot send email: {subject}")
            return False

        msg = MIMEMultipart('alternative')
        msg['From'] = self.smtp_user
        msg['To'] = self.recipient
        msg['Subject'] = subject

        if priority == 'high':
            msg['X-Priority'] = '1'
            msg['Importance'] = 'High'

        # Attach text body
        if body_text:
            msg.attach(MIMEText(body_text, 'plain'))

        # Attach HTML body
        msg.attach(MIMEText(body_html, 'html'))

        # Attach files
        if attachments:
            for filename, content in attachments:
                attachment = MIMEApplication(content, Name=filename)
                attachment['Content-Disposition'] = f'attachment; filename="{filename}"'
                msg.attach(attachment)

        try:
            with smtplib.SMTP(self.smtp_host, self.smtp_port) as server:
                server.starttls()
                server.login(self.smtp_user, self.smtp_password)
                server.send_message(msg)
                logger.info(f"Email sent: {subject} → {self.recipient}")
                return True
        except Exception as e:
            logger.error(f"Failed to send email: {e}", exc_info=True)
            raise

    def send_valuation_alert(self, alert: dict) -> bool:
        """Send a valuation change alert."""
        subject = (f"[VALUATION ALERT] {alert.get('company', '')}: "
                   f"{alert.get('change_pct', 0):+.1f}% change")

        body = self._render_alert_template(alert)
        return self.send(subject, body, priority='high')

    def send_daily_digest(self, digest: dict) -> bool:
        """Send daily summary of all valuations and events."""
        today = datetime.now().strftime('%Y-%m-%d')
        subject = f"[VALUATION DIGEST] {today}"

        body = self._render_digest_template(digest)
        return self.send(subject, body)

    def send_weekly_summary(self, summary: dict) -> bool:
        """Send weekly summary report."""
        week = datetime.now().strftime('Week of %Y-%m-%d')
        subject = f"[WEEKLY SUMMARY] {week}"

        body = self._render_weekly_template(summary)
        return self.send(subject, body)

    def send_critical_alert(self, event: dict) -> bool:
        """Send critical event alert immediately."""
        subject = (f"[CRITICAL] {event.get('headline', 'Critical Event')}")

        body = f"""
        <html><body style="font-family: Arial, sans-serif;">
        <h2 style="color: #d32f2f;">CRITICAL EVENT</h2>
        <table style="border-collapse: collapse; width: 100%;">
        <tr><td><b>Event:</b></td><td>{event.get('headline', '')}</td></tr>
        <tr><td><b>Severity:</b></td><td style="color: red;">{event.get('severity', 'CRITICAL')}</td></tr>
        <tr><td><b>Scope:</b></td><td>{event.get('scope', '')}</td></tr>
        <tr><td><b>Company:</b></td><td>{event.get('company', 'Multiple')}</td></tr>
        <tr><td><b>Sector:</b></td><td>{event.get('sector', '')}</td></tr>
        <tr><td><b>Impact:</b></td><td>{event.get('valuation_impact_pct', '')}%</td></tr>
        <tr><td><b>Summary:</b></td><td>{event.get('summary', '')}</td></tr>
        <tr><td><b>Source:</b></td><td><a href="{event.get('source_url', '')}">{event.get('source', '')}</a></td></tr>
        </table>
        <p style="color: #666;">ACTION REQUIRED: Review in system and confirm/override.</p>
        </body></html>
        """
        return self.send(subject, body, priority='high')

    def _render_alert_template(self, alert: dict) -> str:
        """Render valuation alert HTML."""
        return f"""
        <html><body style="font-family: Arial, sans-serif;">
        <h2>VALUATION ALERT: {alert.get('company', '')}</h2>

        <table style="border-collapse: collapse; width: 100%; margin: 10px 0;">
        <tr style="background: #f5f5f5;">
            <td style="padding: 8px; border: 1px solid #ddd;"><b>CMP</b></td>
            <td style="padding: 8px; border: 1px solid #ddd;">\u20b9{alert.get('cmp', 0):,.2f}</td>
        </tr>
        <tr>
            <td style="padding: 8px; border: 1px solid #ddd;"><b>New Intrinsic Value</b></td>
            <td style="padding: 8px; border: 1px solid #ddd;">\u20b9{alert.get('new_value', 0):,.2f}</td>
        </tr>
        <tr style="background: #f5f5f5;">
            <td style="padding: 8px; border: 1px solid #ddd;"><b>Previous Value</b></td>
            <td style="padding: 8px; border: 1px solid #ddd;">\u20b9{alert.get('old_value', 0):,.2f}</td>
        </tr>
        <tr>
            <td style="padding: 8px; border: 1px solid #ddd;"><b>Change</b></td>
            <td style="padding: 8px; border: 1px solid #ddd; color: {'green' if alert.get('change_pct', 0) > 0 else 'red'};">
                {alert.get('change_pct', 0):+.1f}%
            </td>
        </tr>
        <tr style="background: #f5f5f5;">
            <td style="padding: 8px; border: 1px solid #ddd;"><b>Upside/Downside</b></td>
            <td style="padding: 8px; border: 1px solid #ddd;">{alert.get('upside_pct', 0):+.1f}%</td>
        </tr>
        </table>

        <p style="color: #666; margin-top: 20px;">
        <b>Triggered at:</b> {alert.get('triggered_at', '')}
        </p>
        <p style="color: #d32f2f;"><b>ACTION REQUIRED:</b> Review and confirm/override in system.</p>
        </body></html>
        """

    def _render_digest_template(self, digest: dict) -> str:
        """Render daily digest HTML."""
        valuations_html = ''
        for company, val in digest.get('valuations', {}).items():
            if isinstance(val, dict) and 'error' not in val:
                valuations_html += f"""
                <tr>
                    <td style="padding: 6px; border: 1px solid #ddd;">{company}</td>
                    <td style="padding: 6px; border: 1px solid #ddd;">\u20b9{val.get('intrinsic', 0):,.2f}</td>
                    <td style="padding: 6px; border: 1px solid #ddd;">\u20b9{val.get('cmp', 0):,.2f}</td>
                    <td style="padding: 6px; border: 1px solid #ddd;">{val.get('upside_pct', 0):+.1f}%</td>
                    <td style="padding: 6px; border: 1px solid #ddd;">{val.get('confidence', 0):.2f}</td>
                </tr>
                """

        return f"""
        <html><body style="font-family: Arial, sans-serif;">
        <h2>DAILY VALUATION DIGEST - {datetime.now().strftime('%Y-%m-%d')}</h2>

        <h3>Portfolio Summary</h3>
        <table style="border-collapse: collapse; width: 100%;">
        <tr style="background: #4CAF50; color: white;">
            <th style="padding: 8px;">Company</th>
            <th style="padding: 8px;">Intrinsic Value</th>
            <th style="padding: 8px;">CMP</th>
            <th style="padding: 8px;">Upside</th>
            <th style="padding: 8px;">Confidence</th>
        </tr>
        {valuations_html}
        </table>

        <h3>Alerts Today: {digest.get('alerts', 0)}</h3>
        <h3>Events Processed: {digest.get('events_processed', 0)}</h3>
        <h3>Driver Changes: {digest.get('driver_changes', 0)}</h3>

        <p style="color: #666; font-size: 12px;">Generated by Agentic Valuation System v1.0.0</p>
        </body></html>
        """

    def _render_weekly_template(self, summary: dict) -> str:
        """Render weekly summary HTML."""
        return f"""
        <html><body style="font-family: Arial, sans-serif;">
        <h2>WEEKLY VALUATION SUMMARY</h2>
        <p>Period: {summary.get('period', '')}</p>

        <h3>Key Highlights</h3>
        <ul>
        {''.join(f"<li>{h}</li>" for h in summary.get('highlights', []))}
        </ul>

        <h3>Accuracy Tracking</h3>
        <p>Model predictions vs actual price movement will be tracked here.</p>

        <p style="color: #666; font-size: 12px;">Generated by Agentic Valuation System v1.0.0</p>
        </body></html>
        """
