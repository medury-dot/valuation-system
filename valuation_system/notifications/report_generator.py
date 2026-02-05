"""
Report Generator
Generate PDF and text reports for valuations.
Uses reportlab for PDF generation.
"""

import os
import io
import logging
from datetime import datetime

from dotenv import load_dotenv

logger = logging.getLogger(__name__)

load_dotenv(os.path.join(os.path.dirname(__file__), '..', 'config', '.env'))


class ReportGenerator:
    """
    Generate valuation reports in PDF and text formats.

    Report types:
    1. Single company valuation report (1-page PDF)
    2. Portfolio overview (multi-page PDF)
    3. Alert summary (email-friendly text)
    """

    def __init__(self, output_dir: str = None):
        self.output_dir = output_dir or os.path.join(
            os.path.dirname(__file__), '..', 'reports'
        )
        os.makedirs(self.output_dir, exist_ok=True)

    def generate_company_report(self, valuation: dict) -> str:
        """
        Generate a single-company valuation report.
        Returns path to the generated PDF.
        """
        company = valuation.get('company_name', 'Unknown')
        date_str = valuation.get('valuation_date', datetime.now().strftime('%Y-%m-%d'))
        filename = f"{company.replace(' ', '_')}_{date_str}.pdf"
        filepath = os.path.join(self.output_dir, filename)

        try:
            from reportlab.lib import colors
            from reportlab.lib.pagesizes import A4
            from reportlab.lib.units import inch
            from reportlab.platypus import SimpleDocTemplate, Table, TableStyle, Paragraph, Spacer
            from reportlab.lib.styles import getSampleStyleSheet

            doc = SimpleDocTemplate(filepath, pagesize=A4)
            styles = getSampleStyleSheet()
            elements = []

            # Title
            elements.append(Paragraph(
                f"<b>{company} - Equity Valuation Report</b>",
                styles['Title']
            ))
            elements.append(Paragraph(
                f"Date: {date_str} | Model: {valuation.get('model_version', 'v1.0.0')}",
                styles['Normal']
            ))
            elements.append(Spacer(1, 0.3 * inch))

            # Key metrics table
            cmp = valuation.get('cmp', 0)
            intrinsic = valuation.get('intrinsic_value_blended', 0)
            upside = valuation.get('upside_pct', 0)

            key_data = [
                ['Metric', 'Value'],
                ['Current Market Price', f'Rs {cmp:,.2f}'],
                ['Intrinsic Value (Blended)', f'Rs {intrinsic:,.2f}'],
                ['Upside / Downside', f'{upside:+.1f}%'],
                ['DCF - Bull', f'Rs {valuation.get("dcf_bull", 0):,.2f}'],
                ['DCF - Base', f'Rs {valuation.get("dcf_base", 0):,.2f}'],
                ['DCF - Bear', f'Rs {valuation.get("dcf_bear", 0):,.2f}'],
                ['Relative Value', f'Rs {valuation.get("relative_value", 0):,.2f}'],
                ['MC Median', f'Rs {valuation.get("mc_median", 0):,.2f}'],
                ['Confidence Score', f'{valuation.get("confidence_score", 0):.2f}'],
            ]

            table = Table(key_data, colWidths=[3 * inch, 2.5 * inch])
            table.setStyle(TableStyle([
                ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor('#4CAF50')),
                ('TEXTCOLOR', (0, 0), (-1, 0), colors.white),
                ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
                ('ALIGN', (0, 0), (-1, -1), 'LEFT'),
                ('FONTSIZE', (0, 0), (-1, -1), 10),
                ('GRID', (0, 0), (-1, -1), 0.5, colors.grey),
                ('ROWBACKGROUNDS', (0, 1), (-1, -1), [colors.white, colors.HexColor('#f5f5f5')]),
                ('BOTTOMPADDING', (0, 0), (-1, -1), 6),
                ('TOPPADDING', (0, 0), (-1, -1), 6),
            ]))
            elements.append(table)
            elements.append(Spacer(1, 0.3 * inch))

            # Sector outlook
            outlook = valuation.get('sector_outlook', {})
            elements.append(Paragraph(
                f"<b>Sector Outlook:</b> {outlook.get('outlook_label', 'N/A')} "
                f"(Score: {outlook.get('outlook_score', 0):+.3f})",
                styles['Normal']
            ))
            elements.append(Spacer(1, 0.2 * inch))

            # Key assumptions
            assumptions = valuation.get('dcf_assumptions', {})
            if assumptions:
                elements.append(Paragraph("<b>Key DCF Assumptions:</b>", styles['Heading3']))
                assumptions_data = [
                    ['Parameter', 'Value'],
                    ['Revenue Growth (Y1)', f'{assumptions.get("growth_rates", [0])[0]:.1%}'],
                    ['EBITDA Margin', f'{assumptions.get("ebitda_margin", 0):.1%}'],
                    ['WACC', f'{valuation.get("dcf_details", {}).get("wacc", 0):.2%}'],
                    ['Terminal ROCE', f'{assumptions.get("terminal_roce", 0):.1%}'],
                    ['Terminal Growth', f'{valuation.get("dcf_details", {}).get("terminal_growth", 0):.1%}'],
                    ['Beta', f'{assumptions.get("beta", 0):.2f}'],
                    ['Risk-Free Rate', f'{assumptions.get("risk_free_rate", 0):.2%}'],
                    ['ERP', f'{assumptions.get("erp", 0):.2%}'],
                ]
                a_table = Table(assumptions_data, colWidths=[3 * inch, 2.5 * inch])
                a_table.setStyle(TableStyle([
                    ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor('#2196F3')),
                    ('TEXTCOLOR', (0, 0), (-1, 0), colors.white),
                    ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
                    ('FONTSIZE', (0, 0), (-1, -1), 9),
                    ('GRID', (0, 0), (-1, -1), 0.5, colors.grey),
                    ('BOTTOMPADDING', (0, 0), (-1, -1), 4),
                    ('TOPPADDING', (0, 0), (-1, -1), 4),
                ]))
                elements.append(a_table)

            # Warnings
            warnings = valuation.get('warnings', [])
            if warnings:
                elements.append(Spacer(1, 0.2 * inch))
                elements.append(Paragraph("<b>Data Quality Warnings:</b>", styles['Heading3']))
                for w in warnings:
                    elements.append(Paragraph(f"- {w}", styles['Normal']))

            # Footer
            elements.append(Spacer(1, 0.5 * inch))
            elements.append(Paragraph(
                "<i>Generated by Agentic Valuation System v1.0.0. "
                "Not investment advice.</i>",
                styles['Normal']
            ))

            doc.build(elements)
            logger.info(f"Generated report: {filepath}")
            return filepath

        except ImportError:
            logger.warning("reportlab not installed, generating text report instead")
            return self._generate_text_report(valuation)

    def _generate_text_report(self, valuation: dict) -> str:
        """Fallback text report when reportlab is not available."""
        company = valuation.get('company_name', 'Unknown')
        date_str = valuation.get('valuation_date', datetime.now().strftime('%Y-%m-%d'))
        filename = f"{company.replace(' ', '_')}_{date_str}.txt"
        filepath = os.path.join(self.output_dir, filename)

        lines = [
            f"{'=' * 60}",
            f"  {company} - Equity Valuation Report",
            f"  Date: {date_str}",
            f"{'=' * 60}",
            f"",
            f"  CMP:              Rs {valuation.get('cmp', 0):>12,.2f}",
            f"  Intrinsic Value:  Rs {valuation.get('intrinsic_value_blended', 0):>12,.2f}",
            f"  Upside:           {valuation.get('upside_pct', 0):>+12.1f}%",
            f"  Confidence:       {valuation.get('confidence_score', 0):>12.2f}",
            f"",
            f"  DCF Bull:         Rs {valuation.get('dcf_bull', 0):>12,.2f}",
            f"  DCF Base:         Rs {valuation.get('dcf_base', 0):>12,.2f}",
            f"  DCF Bear:         Rs {valuation.get('dcf_bear', 0):>12,.2f}",
            f"  Relative:         Rs {valuation.get('relative_value', 0):>12,.2f}",
            f"  MC Median:        Rs {valuation.get('mc_median', 0):>12,.2f}",
            f"",
            f"{'=' * 60}",
            f"  Generated by Agentic Valuation System v1.0.0",
        ]

        with open(filepath, 'w') as f:
            f.write('\n'.join(lines))

        logger.info(f"Generated text report: {filepath}")
        return filepath

    def generate_portfolio_report(self, valuations: dict) -> str:
        """
        Generate portfolio overview report.
        Returns path to the generated file.
        """
        date_str = datetime.now().strftime('%Y-%m-%d')
        filename = f"Portfolio_Summary_{date_str}.txt"
        filepath = os.path.join(self.output_dir, filename)

        lines = [
            f"{'=' * 80}",
            f"  Portfolio Valuation Summary - {date_str}",
            f"{'=' * 80}",
            f"",
            f"  {'Company':<30} {'Intrinsic':>12} {'CMP':>12} {'Upside':>10} {'Confidence':>10}",
            f"  {'-' * 74}",
        ]

        for key, val in valuations.items():
            if isinstance(val, dict) and 'error' not in val:
                lines.append(
                    f"  {val.get('company_name', key):<30} "
                    f"Rs{val.get('intrinsic_value_blended', 0):>10,.0f} "
                    f"Rs{val.get('cmp', 0):>10,.0f} "
                    f"{val.get('upside_pct', 0):>+9.1f}% "
                    f"{val.get('confidence_score', 0):>9.2f}"
                )

        lines.extend([
            f"",
            f"{'=' * 80}",
            f"  Generated by Agentic Valuation System v1.0.0",
        ])

        with open(filepath, 'w') as f:
            f.write('\n'.join(lines))

        logger.info(f"Generated portfolio report: {filepath}")
        return filepath
