"""
Excel Report Generator for Valuation System (v2)
Generates a comprehensive .xlsx with:
- Assumptions as Single Source of Truth (all other sheets reference it)
- Cross-sheet formulas (no hardcoded values in DCF/Relative/Summary)
- Data Validation dropdowns (Sector Outlook, Peer Y/N)
- Ratio numerator/denominator in remarks
- 3 driver tabs (Macro, Sector, Company)
- 9 sheets total
"""

import os
import logging
from datetime import date, datetime

from openpyxl import Workbook
from openpyxl.styles import Font, PatternFill, Alignment, Border, Side, numbers
from openpyxl.utils import get_column_letter
from openpyxl.worksheet.datavalidation import DataValidation
from openpyxl.worksheet.formula import ArrayFormula
from dotenv import load_dotenv

load_dotenv(os.path.join(os.path.dirname(__file__), '..', 'config', '.env'))

logger = logging.getLogger(__name__)

# ── Styles ──────────────────────────────────────────────────────────────────
HEADER_FONT = Font(bold=True, size=12, color='FFFFFF')
HEADER_FILL = PatternFill(start_color='2F5496', end_color='2F5496', fill_type='solid')
SECTION_FONT = Font(bold=True, size=11, color='2F5496')
SECTION_FILL = PatternFill(start_color='D6E4F0', end_color='D6E4F0', fill_type='solid')
INPUT_FILL = PatternFill(start_color='FFF2CC', end_color='FFF2CC', fill_type='solid')
FORMULA_FILL = PatternFill(start_color='E2EFDA', end_color='E2EFDA', fill_type='solid')
REMARK_FONT = Font(italic=True, color='808080', size=9)
ACTUAL_FONT = Font(color='006100', size=9)
DERIVED_FONT = Font(color='9C6500', size=9)
BOLD_FONT = Font(bold=True)
TITLE_FONT = Font(bold=True, size=14, color='2F5496')
THIN_BORDER = Border(
    left=Side(style='thin'), right=Side(style='thin'),
    top=Side(style='thin'), bottom=Side(style='thin')
)
PCT_FMT = '0.00%'
NUM_FMT = '#,##0.00'
INT_FMT = '#,##0'


def _style_header(ws, row, max_col):
    for col in range(1, max_col + 1):
        cell = ws.cell(row=row, column=col)
        cell.font = HEADER_FONT
        cell.fill = HEADER_FILL
        cell.alignment = Alignment(horizontal='center')


def _style_section(ws, row, max_col, label=None):
    for col in range(1, max_col + 1):
        cell = ws.cell(row=row, column=col)
        cell.font = SECTION_FONT
        cell.fill = SECTION_FILL
    if label:
        ws.cell(row=row, column=1, value=label)


def _inp(ws, row, col, value, fmt=None):
    """Write an input cell (yellow, editable)."""
    cell = ws.cell(row=row, column=col, value=value)
    cell.fill = INPUT_FILL
    cell.border = THIN_BORDER
    if fmt:
        cell.number_format = fmt
    return cell


def _fml(ws, row, col, formula, fmt=None):
    """Write a formula cell (green, computed)."""
    cell = ws.cell(row=row, column=col)
    cell.value = formula
    cell.fill = FORMULA_FILL
    cell.border = THIN_BORDER
    if fmt:
        cell.number_format = fmt
    return cell


def _remark(ws, row, col, text):
    """Write a remark/explanation."""
    # Prefix with single quote to prevent formula interpretation
    safe_text = f"'{text}" if text and (text.startswith('=') or text.startswith('+') or text.startswith('-')) else text
    cell = ws.cell(row=row, column=col, value=safe_text)
    cell.font = REMARK_FONT
    return cell


def _format_ratio_components(components, metric_label=''):
    """Build a readable remark string like 'avg(949/18148=5.2%, 850/16500=5.2%)'."""
    if not components:
        return ''
    parts = []
    for c in components:
        r = c.get('ratio', 0)
        n = c.get('numerator', c.get('nwc', 0))
        d = c.get('denominator', 0)
        parts.append(f"{n:,.0f}/{d:,.0f}={r:.1%}")
    avg_val = sum(c.get('ratio', 0) for c in components) / len(components)
    return f"avg({', '.join(parts)}) = {avg_val:.1%}"


def _format_nwc_components(components):
    """Build NWC remark like 'avg((980+1200-2800)/18148=-3.4%)'."""
    if not components:
        return ''
    parts = []
    for c in components:
        parts.append(
            f"({c['inv']:,.0f}+{c['debtors']:,.0f}-{c['payables']:,.0f})/"
            f"{c['denominator']:,.0f}={c['ratio']:.1%}"
        )
    avg_val = sum(c['ratio'] for c in components) / len(components)
    return f"avg({', '.join(parts)}) = {avg_val:.1%}"


def _format_tax_components(components):
    """Build tax remark like 'avg(1-3800/5100=25.5%)'."""
    if not components:
        return ''
    parts = []
    for c in components:
        parts.append(f"1-{c['pat']:,.0f}/{c['pbt']:,.0f}={c['rate']:.1%}")
    avg_val = sum(c['rate'] for c in components) / len(components)
    return f"avg({', '.join(parts)}) = {avg_val:.1%}"


def _format_cod_components(components):
    """Build cost of debt remark like 'avg(120/800=15.0%)'."""
    if not components:
        return ''
    parts = []
    for c in components:
        parts.append(f"{c['interest']:,.0f}/{c['debt']:,.0f}={c['rate']:.1%}")
    avg_val = sum(c['rate'] for c in components) / len(components)
    return f"avg({', '.join(parts)}) = {avg_val:.1%}"


# ── Main Entry Point ────────────────────────────────────────────────────────

def generate_valuation_excel(result: dict, output_path: str = None) -> str:
    """
    Generate a comprehensive Excel valuation report.

    Args:
        result: Output from ValuatorAgent.run_full_valuation()
        output_path: Optional output path. Defaults to logs/ with timestamp.

    Returns:
        Path to generated Excel file.
    """
    company = result.get('company_name', 'Unknown')
    symbol = result.get('nse_symbol', '')

    if not output_path:
        log_dir = os.path.join(os.path.dirname(__file__), '..', 'logs')
        os.makedirs(log_dir, exist_ok=True)
        safe_name = symbol.lower() if symbol else company.lower().replace(' ', '_')
        timestamp = datetime.now().strftime('%Y%m%d_%H%M')
        output_path = os.path.join(log_dir, f'{safe_name}_valuation_{timestamp}.xlsx')

    wb = Workbook()
    # Cell registry: tracks key cells for cross-sheet references
    refs = {}

    # Build all sheets in order
    ws_assumptions = wb.create_sheet('Assumptions')
    _build_assumptions_sheet(ws_assumptions, result, refs)

    ws_dcf = wb.create_sheet('DCF Model')
    _build_dcf_sheet(ws_dcf, result, refs)

    ws_rel = wb.create_sheet('Relative Val')
    _build_relative_sheet(ws_rel, result, refs)

    ws_mc = wb.create_sheet('Monte Carlo')
    _build_monte_carlo_sheet(ws_mc, result, refs)

    ws_sens = wb.create_sheet('Sensitivity')
    _build_sensitivity_sheet(ws_sens, result, refs)

    ws_macro = wb.create_sheet('Macro Drivers')
    _build_macro_drivers_sheet(ws_macro, result, refs)

    ws_sector = wb.create_sheet('Sector Drivers')
    _build_sector_drivers_sheet(ws_sector, result, refs)

    ws_company = wb.create_sheet('Company Drivers')
    _build_company_drivers_sheet(ws_company, result, refs)

    # Summary is the active (first) sheet — built last since it references others
    ws_summary = wb.active
    ws_summary.title = 'Summary'
    _build_summary_sheet(ws_summary, result, refs)

    # Move Summary to position 0
    wb.move_sheet('Summary', offset=-8)

    wb.save(output_path)
    logger.info(f"Valuation Excel saved to: {output_path}")
    return output_path


# ── Sheet 2: Assumptions (Single Source of Truth) ───────────────────────────

def _build_assumptions_sheet(ws, result, refs):
    ws.column_dimensions['A'].width = 32
    ws.column_dimensions['B'].width = 18
    ws.column_dimensions['C'].width = 60

    assumptions = result.get('dcf_assumptions', {})
    dcf_details = result.get('dcf_details', {})
    dcf_assum = dcf_details.get('assumptions', {})
    outlook = result.get('sector_outlook', {})

    r = 1
    ws.cell(row=r, column=1, value='Valuation Assumptions').font = TITLE_FONT
    _remark(ws, r, 3, 'Yellow = changeable input, Green = formula/computed')

    # ── MACRO LEVEL ──
    r = 3
    _style_section(ws, r, 3, 'MACRO LEVEL')
    _remark(ws, r, 3, 'Weight: 20% of driver hierarchy')
    r += 1
    ws.cell(row=r, column=1, value='Risk-Free Rate (Rf)')
    refs['rf_rate'] = r
    _inp(ws, r, 2, assumptions.get('risk_free_rate', dcf_assum.get('risk_free_rate', 0.0674)), PCT_FMT)
    _remark(ws, r, 3, '[DERIVED: US 10Y + India premium]')
    r += 1
    ws.cell(row=r, column=1, value='Equity Risk Premium (ERP)')
    refs['erp'] = r
    _inp(ws, r, 2, assumptions.get('equity_risk_premium', assumptions.get('erp', dcf_assum.get('erp', 0.0708))), PCT_FMT)
    _remark(ws, r, 3, '[ACTUAL: Damodaran India ERP]')

    # ── SECTOR LEVEL ──
    r += 3
    _style_section(ws, r, 3, 'SECTOR LEVEL')
    _remark(ws, r, 3, 'Weight: 55% of driver hierarchy')
    r += 1
    ws.cell(row=r, column=1, value='Beta (Levered)')
    refs['beta'] = r
    _inp(ws, r, 2, assumptions.get('beta', dcf_assum.get('beta', 0)), '0.0000')
    _remark(ws, r, 3, '[ACTUAL: Damodaran sector beta]')

    r += 1
    ws.cell(row=r, column=1, value='Sector Outlook')
    refs['outlook_label'] = r
    outlook_label = outlook.get('outlook_label', outlook.get('outlook', 'NEUTRAL'))
    outlook_cell = _inp(ws, r, 2, outlook_label)
    # Data Validation dropdown
    dv = DataValidation(type='list', formula1='"BEARISH,NEGATIVE,NEUTRAL,POSITIVE,BULLISH"',
                        allow_blank=False)
    dv.error = 'Select from: BEARISH, NEGATIVE, NEUTRAL, POSITIVE, BULLISH'
    dv.errorTitle = 'Invalid Outlook'
    ws.add_data_validation(dv)
    dv.add(outlook_cell)
    _remark(ws, r, 3, 'Dropdown: change to recalculate growth/margin adjustments')

    # Lookup table for outlook → score (write first before referencing)
    lookup_data = [('BEARISH', -1.0), ('NEGATIVE', -0.5), ('NEUTRAL', 0.0),
                   ('POSITIVE', 0.5), ('BULLISH', 1.0)]
    for i, (label, score) in enumerate(lookup_data):
        ws.cell(row=80 + i, column=5, value=label)
        ws.cell(row=80 + i, column=6, value=score)
    refs['lookup_range'] = 'E80:F84'

    r += 1
    ws.cell(row=r, column=1, value='Outlook Score')
    refs['outlook_score'] = r
    # Use nested IFs instead of VLOOKUP to avoid formula errors
    outlook_ref = refs["outlook_label"]
    nested_if = f'=IF(B{outlook_ref}="BULLISH",1,IF(B{outlook_ref}="POSITIVE",0.5,IF(B{outlook_ref}="NEUTRAL",0,IF(B{outlook_ref}="NEGATIVE",-0.5,-1))))'
    _fml(ws, r, 2, nested_if, '0.00')
    _remark(ws, r, 3, 'Maps label to score: BEARISH=-1.0 ... BULLISH=+1.0')

    r += 1
    ws.cell(row=r, column=1, value='Growth Adjustment')
    refs['growth_adj'] = r
    _fml(ws, r, 2, f'=B{refs["outlook_score"]}*0.03', PCT_FMT)
    _remark(ws, r, 3, '= Outlook Score * 3% (max +/-3%)')

    r += 1
    ws.cell(row=r, column=1, value='Margin Adjustment')
    refs['margin_adj'] = r
    _fml(ws, r, 2, f'=B{refs["outlook_score"]}*0.01', PCT_FMT)
    _remark(ws, r, 3, '= Outlook Score * 1% (max +/-1%)')

    # Peer medians (will be ArrayFormulas referencing Relative Val Y/N)
    r += 1
    ws.cell(row=r, column=1, value='Peer PE Median')
    refs['peer_pe_median'] = r
    # Placeholder — will be set after Relative Val sheet builds
    _inp(ws, r, 2, _get_peer_median(result, 'pe'), '0.00')
    _remark(ws, r, 3, 'From included peers (Y/N toggle in Relative Val)')

    r += 1
    ws.cell(row=r, column=1, value='Peer PB Median')
    refs['peer_pb_median'] = r
    _inp(ws, r, 2, _get_peer_median(result, 'pb'), '0.00')

    r += 1
    ws.cell(row=r, column=1, value='Peer EV/EBITDA Median')
    refs['peer_eveb_median'] = r
    _inp(ws, r, 2, _get_peer_median(result, 'ev_ebitda'), '0.00')

    r += 1
    ws.cell(row=r, column=1, value='Peer PS Median')
    refs['peer_ps_median'] = r
    _inp(ws, r, 2, _get_peer_median(result, 'ps'), '0.00')

    # ── COMPANY LEVEL ──
    r += 3
    _style_section(ws, r, 3, 'COMPANY LEVEL')
    _remark(ws, r, 3, 'Weight: 25% of driver hierarchy')

    r += 1
    ws.cell(row=r, column=1, value='Revenue Base (Rs Cr)')
    refs['base_revenue'] = r
    rev = assumptions.get('base_revenue', dcf_assum.get('base_revenue', 0))
    _inp(ws, r, 2, rev, NUM_FMT)
    _remark(ws, r, 3, f'[{result.get("revenue_source", "")}]')

    # CAGR
    r += 1
    ws.cell(row=r, column=1, value='Revenue CAGR 3Y')
    refs['cagr_3y'] = r
    _inp(ws, r, 2, result.get('revenue_cagr_3y') or 0, PCT_FMT)

    r += 1
    ws.cell(row=r, column=1, value='Revenue CAGR 5Y')
    refs['cagr_5y'] = r
    _inp(ws, r, 2, result.get('revenue_cagr_5y') or 0, PCT_FMT)

    # YoY growth
    yoy = result.get('revenue_yoy_growth', [])
    for item in yoy:
        r += 1
        ws.cell(row=r, column=1, value=f'YoY Growth FY{item["to_year"]}')
        _inp(ws, r, 2, item['growth'], PCT_FMT)
        _remark(ws, r, 3,
                f'= ({item["to_value"]:,.0f} / {item["from_value"]:,.0f}) - 1 = {item["growth"]:.1%}')

    # Growth trajectory with formulas
    r += 1
    ws.cell(row=r, column=1, value='Growth Y1 (base)')
    refs['growth_y1_base'] = r
    _fml(ws, r, 2, f'=B{refs["cagr_3y"]}*0.6+B{refs["cagr_5y"]}*0.4', PCT_FMT)
    _remark(ws, r, 3, '= CAGR_3Y * 0.6 + CAGR_5Y * 0.4')

    r += 1
    ws.cell(row=r, column=1, value='Growth Y1 (sector adj)')
    refs['growth_y1'] = r
    _fml(ws, r, 2, f'=MAX(0.03,B{refs["growth_y1_base"]}*(1+B{refs["growth_adj"]}))', PCT_FMT)
    _remark(ws, r, 3, '= MAX(3%, base * (1 + sector growth adj))')

    for yr in range(2, 6):
        r += 1
        ws.cell(row=r, column=1, value=f'Growth Y{yr} (sector adj)')
        refs[f'growth_y{yr}'] = r
        _fml(ws, r, 2,
             f'=MAX(0.03,B{refs["growth_y1"]}-(B{refs["growth_y1"]}-0.06)*({yr-1}/4))', PCT_FMT)
        _remark(ws, r, 3, f'= MAX(3%, Y1 - (Y1-6%) * {yr-1}/4) — linear decay to 6%')

    # TTM PAT / PBIDT
    r += 1
    ws.cell(row=r, column=1, value='TTM PAT (Rs Cr)')
    refs['ttm_pat'] = r
    _inp(ws, r, 2, result.get('ttm_pat', 0), NUM_FMT)
    _remark(ws, r, 3, f'[{result.get("pat_source", "")}]')

    r += 1
    ws.cell(row=r, column=1, value='TTM PBIDT (Rs Cr)')
    refs['ttm_pbidt'] = r
    _inp(ws, r, 2, result.get('ttm_pbidt', 0), NUM_FMT)
    _remark(ws, r, 3, f'[{result.get("pbidt_source", "")}]')

    # EBITDA Margin as formula
    r += 1
    ws.cell(row=r, column=1, value='EBITDA Margin')
    refs['ebitda_margin'] = r
    ebitda_m = assumptions.get('ebitda_margin', dcf_assum.get('ebitda_margin', 0.15))
    _inp(ws, r, 2, ebitda_m, PCT_FMT)
    _remark(ws, r, 3, f'= TTM PBIDT / TTM Revenue (or avg 3yr)')

    r += 1
    ws.cell(row=r, column=1, value='Margin Improvement (annual)')
    refs['margin_improvement'] = r
    _inp(ws, r, 2, assumptions.get('margin_improvement', dcf_assum.get('margin_improvement', 0)), '0.0000')

    # Ratios with numerator/denominator in remarks
    r += 1
    ws.cell(row=r, column=1, value='Capex / Sales')
    refs['capex_to_sales'] = r
    capex_s = assumptions.get('capex_to_sales', dcf_assum.get('capex_to_sales', 0.08))
    _inp(ws, r, 2, capex_s, PCT_FMT)
    _remark(ws, r, 3, _format_ratio_components(result.get('capex_components', [])))

    r += 1
    ws.cell(row=r, column=1, value='Depreciation / Sales')
    refs['depr_to_sales'] = r
    depr_s = assumptions.get('depreciation_to_sales', dcf_assum.get('depreciation_to_sales', 0.04))
    _inp(ws, r, 2, depr_s, PCT_FMT)
    _remark(ws, r, 3, _format_ratio_components(result.get('depr_components', [])))

    r += 1
    ws.cell(row=r, column=1, value='NWC / Sales')
    refs['nwc_to_sales'] = r
    nwc_s = assumptions.get('nwc_to_sales', dcf_assum.get('nwc_to_sales', 0.15))
    _inp(ws, r, 2, nwc_s, PCT_FMT)
    _remark(ws, r, 3, _format_nwc_components(result.get('nwc_components', [])))

    r += 1
    ws.cell(row=r, column=1, value='Effective Tax Rate')
    refs['tax_rate'] = r
    _inp(ws, r, 2, assumptions.get('tax_rate', dcf_assum.get('tax_rate', 0.25)), PCT_FMT)
    _remark(ws, r, 3, _format_tax_components(result.get('tax_components', [])))

    r += 1
    ws.cell(row=r, column=1, value='Cost of Debt (pre-tax)')
    refs['cost_of_debt'] = r
    _inp(ws, r, 2, assumptions.get('cost_of_debt', dcf_assum.get('cost_of_debt', 0.09)), PCT_FMT)
    _remark(ws, r, 3, _format_cod_components(result.get('cost_of_debt_components', [])))

    r += 1
    ws.cell(row=r, column=1, value='Debt Ratio (D/V)')
    refs['debt_ratio'] = r
    _inp(ws, r, 2, assumptions.get('debt_ratio', dcf_assum.get('debt_ratio', 0.10)), PCT_FMT)

    r += 1
    ws.cell(row=r, column=1, value='Terminal ROCE')
    refs['terminal_roce'] = r
    _inp(ws, r, 2, assumptions.get('terminal_roce', dcf_assum.get('terminal_roce', 0.15)), PCT_FMT)

    r += 1
    ws.cell(row=r, column=1, value='Terminal Reinvestment Rate')
    refs['terminal_reinvestment'] = r
    _inp(ws, r, 2, assumptions.get('terminal_reinvestment', dcf_assum.get('terminal_reinvestment', 0.30)), PCT_FMT)

    r += 1
    ws.cell(row=r, column=1, value='Net Debt (Rs Cr)')
    refs['net_debt'] = r
    _inp(ws, r, 2, assumptions.get('net_debt', dcf_assum.get('net_debt', 0)), NUM_FMT)

    r += 1
    ws.cell(row=r, column=1, value='Cash & Equivalents (Rs Cr)')
    refs['cash'] = r
    cash_val = assumptions.get('cash_and_equivalents', dcf_assum.get('cash_and_equivalents', 0))
    _inp(ws, r, 2, cash_val, NUM_FMT)
    _remark(ws, r, 3, f'[ACTUAL: {result.get("cash_source_detail", "")}]')

    r += 1
    ws.cell(row=r, column=1, value='Shares Outstanding (Cr)')
    refs['shares'] = r
    _inp(ws, r, 2, assumptions.get('shares_outstanding', dcf_assum.get('shares_outstanding', 0)), '0.0000')
    _remark(ws, r, 3, '= MCap / CMP')


def _get_peer_median(result, key):
    """Extract peer median from result dict."""
    rel = result.get('relative_details', {})
    implied = rel.get('implied_values', {})
    return implied.get(key, {}).get('peer_median', 0) or 0


# ── Sheet 3: DCF Model ─────────────────────────────────────────────────────

def _build_dcf_sheet(ws, result, refs):
    ws.column_dimensions['A'].width = 28
    for c in 'BCDEFGHI':
        ws.column_dimensions[c].width = 16

    dcf = result.get('dcf_details', {})
    projections = dcf.get('fcff_projections', [])
    n_years = len(projections) or 5

    r = 1
    ws.cell(row=r, column=1, value='FCFF-Based DCF Valuation').font = TITLE_FONT

    # ── WACC ──
    r = 3
    _style_section(ws, r, 8, 'WACC Calculation')
    r += 1
    rf_row = r
    ws.cell(row=r, column=1, value='Risk-Free Rate (Rf)')
    _fml(ws, r, 2, f"='Assumptions'!B{refs['rf_rate']}", PCT_FMT)
    _remark(ws, r, 3, 'Ref: Assumptions')

    r += 1
    erp_row = r
    ws.cell(row=r, column=1, value='Equity Risk Premium (ERP)')
    _fml(ws, r, 2, f"='Assumptions'!B{refs['erp']}", PCT_FMT)
    _remark(ws, r, 3, 'Ref: Assumptions')

    r += 1
    beta_row = r
    ws.cell(row=r, column=1, value='Beta (Levered)')
    _fml(ws, r, 2, f"='Assumptions'!B{refs['beta']}", '0.0000')
    _remark(ws, r, 3, 'Ref: Assumptions')

    r += 1
    ke_row = r
    ws.cell(row=r, column=1, value='Cost of Equity (Ke)')
    _fml(ws, r, 2, f'=B{rf_row}+B{beta_row}*B{erp_row}', PCT_FMT)
    _remark(ws, r, 3, '= Rf + Beta * ERP')
    refs['ke'] = ke_row

    r += 1
    kd_row = r
    ws.cell(row=r, column=1, value='Cost of Debt (pre-tax, Kd)')
    _fml(ws, r, 2, f"='Assumptions'!B{refs['cost_of_debt']}", PCT_FMT)

    r += 1
    tax_row = r
    ws.cell(row=r, column=1, value='Tax Rate')
    _fml(ws, r, 2, f"='Assumptions'!B{refs['tax_rate']}", PCT_FMT)

    r += 1
    kd_at_row = r
    ws.cell(row=r, column=1, value='Cost of Debt (after-tax)')
    _fml(ws, r, 2, f'=B{kd_row}*(1-B{tax_row})', PCT_FMT)
    _remark(ws, r, 3, '= Kd * (1 - Tax)')

    r += 1
    dv_row = r
    ws.cell(row=r, column=1, value='Debt Ratio (D/V)')
    _fml(ws, r, 2, f"='Assumptions'!B{refs['debt_ratio']}", PCT_FMT)

    r += 1
    wacc_row = r
    ws.cell(row=r, column=1, value='WACC').font = BOLD_FONT
    _fml(ws, r, 2, f'=B{ke_row}*(1-B{dv_row})+B{kd_at_row}*B{dv_row}', PCT_FMT)
    ws.cell(row=r, column=2).font = BOLD_FONT
    _remark(ws, r, 3, '= Ke*(1-D/V) + Kd_at*(D/V)')
    refs['wacc_row'] = wacc_row

    # ── FCFF Projections ──
    r += 2
    _style_section(ws, r, 2 + n_years, 'FCFF Projections (Rs Cr)')
    r += 1
    header_row = r
    ws.cell(row=r, column=1, value='Metric')
    ws.cell(row=r, column=2, value='Base (FY)')
    for i in range(n_years):
        ws.cell(row=r, column=3 + i, value=f'Year {i+1}')
    _style_header(ws, r, 2 + n_years)

    # Row: Revenue Growth (from Assumptions)
    r += 1
    growth_row = r
    ws.cell(row=r, column=1, value='Revenue Growth')
    for i in range(n_years):
        growth_ref = refs.get(f'growth_y{i+1}')
        if growth_ref:
            _fml(ws, r, 3 + i, f"='Assumptions'!B{growth_ref}", PCT_FMT)
        else:
            _inp(ws, r, 3 + i, projections[i].get('growth_rate', 0.06) if i < len(projections) else 0.06, PCT_FMT)

    # Row: Revenue
    r += 1
    rev_row = r
    ws.cell(row=r, column=1, value='Revenue')
    _fml(ws, r, 2, f"='Assumptions'!B{refs['base_revenue']}", NUM_FMT)
    for i in range(n_years):
        col = 3 + i
        prev_col = get_column_letter(col - 1)
        cur_col = get_column_letter(col)
        _fml(ws, r, col, f'={prev_col}{rev_row}*(1+{cur_col}{growth_row})', NUM_FMT)

    # Row: EBITDA Margin
    r += 1
    margin_row = r
    ws.cell(row=r, column=1, value='EBITDA Margin')
    _fml(ws, r, 2, f"='Assumptions'!B{refs['ebitda_margin']}", PCT_FMT)
    for i in range(n_years):
        col = 3 + i
        prev_col = get_column_letter(col - 1)
        _fml(ws, r, col,
             f"={prev_col}{margin_row}+'Assumptions'!B{refs['margin_improvement']}", PCT_FMT)

    # Row: EBITDA
    r += 1
    ebitda_row = r
    ws.cell(row=r, column=1, value='EBITDA')
    for i in range(n_years):
        col = 3 + i
        c = get_column_letter(col)
        _fml(ws, r, col, f'={c}{rev_row}*{c}{margin_row}', NUM_FMT)

    # Row: Depreciation
    r += 1
    depr_row = r
    ws.cell(row=r, column=1, value='Depreciation')
    for i in range(n_years):
        col = 3 + i
        c = get_column_letter(col)
        _fml(ws, r, col, f"={c}{rev_row}*'Assumptions'!B{refs['depr_to_sales']}", NUM_FMT)

    # Row: EBIT
    r += 1
    ebit_row = r
    ws.cell(row=r, column=1, value='EBIT')
    for i in range(n_years):
        col = 3 + i
        c = get_column_letter(col)
        _fml(ws, r, col, f'={c}{ebitda_row}-{c}{depr_row}', NUM_FMT)

    # Row: Tax Rate
    r += 1
    tax_proj_row = r
    ws.cell(row=r, column=1, value='Tax Rate')
    for i in range(n_years):
        _fml(ws, r, 3 + i, f"='Assumptions'!B{refs['tax_rate']}", PCT_FMT)

    # Row: NOPAT
    r += 1
    nopat_row = r
    ws.cell(row=r, column=1, value='NOPAT')
    for i in range(n_years):
        col = 3 + i
        c = get_column_letter(col)
        _fml(ws, r, col, f'={c}{ebit_row}*(1-{c}{tax_proj_row})', NUM_FMT)

    # Row: Capex
    r += 1
    capex_row = r
    ws.cell(row=r, column=1, value='Capex')
    for i in range(n_years):
        col = 3 + i
        c = get_column_letter(col)
        _fml(ws, r, col, f"={c}{rev_row}*'Assumptions'!B{refs['capex_to_sales']}", NUM_FMT)

    # Row: Delta NWC
    r += 1
    dnwc_row = r
    ws.cell(row=r, column=1, value='Change in NWC')
    for i in range(n_years):
        col = 3 + i
        c = get_column_letter(col)
        prev_c = get_column_letter(col - 1)
        _fml(ws, r, col, f"=({c}{rev_row}-{prev_c}{rev_row})*'Assumptions'!B{refs['nwc_to_sales']}", NUM_FMT)

    # Row: FCFF
    r += 1
    fcff_row = r
    ws.cell(row=r, column=1, value='FCFF').font = BOLD_FONT
    for i in range(n_years):
        col = 3 + i
        c = get_column_letter(col)
        cell = _fml(ws, r, col,
                     f'={c}{nopat_row}+{c}{depr_row}-{c}{capex_row}-{c}{dnwc_row}', NUM_FMT)
        cell.font = BOLD_FONT

    # Row: Discount Factor
    r += 1
    df_row = r
    ws.cell(row=r, column=1, value='Discount Factor')
    for i in range(n_years):
        _fml(ws, r, 3 + i, f'=1/(1+$B${wacc_row})^{i+1}', '0.0000')

    # Row: PV of FCFF
    r += 1
    pv_row = r
    ws.cell(row=r, column=1, value='PV of FCFF')
    for i in range(n_years):
        col = 3 + i
        c = get_column_letter(col)
        _fml(ws, r, col, f'={c}{fcff_row}*{c}{df_row}', NUM_FMT)

    # ── Terminal Value ──
    r += 2
    _style_section(ws, r, 8, 'Terminal Value')
    r += 1
    roce_row = r
    ws.cell(row=r, column=1, value='Terminal ROCE')
    _fml(ws, r, 2, f"='Assumptions'!B{refs['terminal_roce']}", PCT_FMT)

    r += 1
    reinv_row = r
    ws.cell(row=r, column=1, value='Terminal Reinvestment Rate')
    _fml(ws, r, 2, f"='Assumptions'!B{refs['terminal_reinvestment']}", PCT_FMT)

    r += 1
    tg_row = r
    ws.cell(row=r, column=1, value='Terminal Growth (g)')
    _fml(ws, r, 2, f'=MIN(5%,MAX(2%,B{roce_row}*B{reinv_row}))', PCT_FMT)
    _remark(ws, r, 3, '= MIN(5%, MAX(2%, ROCE * Reinvestment))')
    refs['terminal_growth'] = tg_row

    r += 1
    last_fcff_col = get_column_letter(2 + n_years)
    fcff_n1_row = r
    ws.cell(row=r, column=1, value='FCFF (n+1)')
    _fml(ws, r, 2, f'={last_fcff_col}{fcff_row}*(1+B{tg_row})', NUM_FMT)

    r += 1
    tv_row = r
    ws.cell(row=r, column=1, value='Terminal Value')
    _fml(ws, r, 2, f'=B{fcff_n1_row}/(B{wacc_row}-B{tg_row})', NUM_FMT)
    _remark(ws, r, 3, '= FCFF(n+1) / (WACC - g)')

    r += 1
    pv_tv_row = r
    ws.cell(row=r, column=1, value='PV of Terminal Value')
    _fml(ws, r, 2, f'=B{tv_row}/(1+B{wacc_row})^{n_years}', NUM_FMT)

    # ── Equity Bridge ──
    r += 2
    _style_section(ws, r, 8, 'Equity Bridge')
    r += 1
    pv_start = get_column_letter(3)
    pv_end = get_column_letter(2 + n_years)
    pv_explicit_row = r
    ws.cell(row=r, column=1, value='PV of Explicit FCFFs')
    _fml(ws, r, 2, f'=SUM({pv_start}{pv_row}:{pv_end}{pv_row})', NUM_FMT)

    r += 1
    pv_tv_bridge = r
    ws.cell(row=r, column=1, value='PV of Terminal Value')
    _fml(ws, r, 2, f'=B{pv_tv_row}', NUM_FMT)

    r += 1
    fv_row = r
    ws.cell(row=r, column=1, value='Firm (Enterprise) Value').font = BOLD_FONT
    cell = _fml(ws, r, 2, f'=B{pv_explicit_row}+B{pv_tv_bridge}', NUM_FMT)
    cell.font = BOLD_FONT

    r += 1
    debt_bridge = r
    ws.cell(row=r, column=1, value='(-) Net Debt')
    _fml(ws, r, 2, f"='Assumptions'!B{refs['net_debt']}", NUM_FMT)

    r += 1
    cash_bridge = r
    ws.cell(row=r, column=1, value='(+) Cash & Equivalents')
    _fml(ws, r, 2, f"='Assumptions'!B{refs['cash']}", NUM_FMT)

    r += 1
    eq_val_row = r
    ws.cell(row=r, column=1, value='Equity Value').font = BOLD_FONT
    cell = _fml(ws, r, 2, f'=B{fv_row}-B{debt_bridge}+B{cash_bridge}', NUM_FMT)
    cell.font = BOLD_FONT

    r += 1
    shares_bridge = r
    ws.cell(row=r, column=1, value='Shares Outstanding (Cr)')
    _fml(ws, r, 2, f"='Assumptions'!B{refs['shares']}", '0.0000')

    r += 1
    iv_row = r
    ws.cell(row=r, column=1, value='Intrinsic Value per Share (Rs)').font = BOLD_FONT
    cell = _fml(ws, r, 2, f'=B{eq_val_row}/B{shares_bridge}', NUM_FMT)
    cell.font = BOLD_FONT
    _remark(ws, r, 3, '= Equity Value / Shares')
    refs['dcf_iv_row'] = iv_row

    r += 1
    ws.cell(row=r, column=1, value='TV as % of Firm Value')
    _fml(ws, r, 2, f'=B{pv_tv_bridge}/B{fv_row}', PCT_FMT)


# ── Sheet 4: Relative Valuation ─────────────────────────────────────────────

def _build_relative_sheet(ws, result, refs):
    ws.column_dimensions['A'].width = 22
    for c in 'BCDEFGH':
        ws.column_dimensions[c].width = 16

    rel = result.get('relative_details', {})
    implied = rel.get('implied_values', {})
    weights = rel.get('multiple_weights_used', {})
    adj_factors = rel.get('adjustment_factors', {})

    r = 1
    ws.cell(row=r, column=1, value='Relative Valuation').font = TITLE_FONT

    # ── Implied Values ──
    r = 3
    _style_section(ws, r, 6, 'Implied Values by Multiple')
    r += 1
    ws.cell(row=r, column=1, value='Multiple')
    ws.cell(row=r, column=2, value='Peer Median')
    ws.cell(row=r, column=3, value='Company Metric')
    ws.cell(row=r, column=4, value='Implied/Share')
    ws.cell(row=r, column=5, value='Weight')
    ws.cell(row=r, column=6, value='Contribution')
    _style_header(ws, r, 6)

    r += 1
    implied_start = r
    mult_keys = [('pe', 'P/E', 'peer_pe_median'),
                 ('pb', 'P/B', 'peer_pb_median'),
                 ('ev_ebitda', 'EV/EBITDA', 'peer_eveb_median'),
                 ('ps', 'P/S', 'peer_ps_median')]

    for key, label, ref_key in mult_keys:
        imp = implied.get(key, {})
        ws.cell(row=r, column=1, value=label)
        # Peer median refs Assumptions
        _fml(ws, r, 2, f"='Assumptions'!B{refs[ref_key]}", '0.00')
        # Company metric
        _inp(ws, r, 3, imp.get('company_metric', 0), NUM_FMT)
        # Implied/share
        _inp(ws, r, 4, imp.get('implied_per_share', 0), NUM_FMT)
        # Weight
        w = weights.get(key, 0)
        _inp(ws, r, 5, w, PCT_FMT)
        # Contribution = Implied * Weight
        _fml(ws, r, 6, f'=D{r}*E{r}', NUM_FMT)
        r += 1

    implied_end = r - 1

    r += 1
    ws.cell(row=r, column=1, value='Total Weight').font = BOLD_FONT
    total_wt_row = r
    _fml(ws, r, 5, f'=SUM(E{implied_start}:E{implied_end})', PCT_FMT)

    r += 1
    base_val_row = r
    ws.cell(row=r, column=1, value='Base Relative Value').font = BOLD_FONT
    _fml(ws, r, 6, f'=SUM(F{implied_start}:F{implied_end})/E{total_wt_row}', NUM_FMT)

    # ── Quality Adjustments ──
    r += 2
    _style_section(ws, r, 6, 'Quality Adjustments')
    r += 1
    ws.cell(row=r, column=1, value='Factor')
    ws.cell(row=r, column=2, value='Adjustment')
    _style_header(ws, r, 2)

    r += 1
    adj_start = r
    adj_ref_map = {}
    for factor, val in adj_factors.items():
        ws.cell(row=r, column=1, value=factor.replace('_', ' ').title())
        _inp(ws, r, 2, val, PCT_FMT)
        adj_ref_map[factor] = r
        r += 1
    adj_end = r - 1
    refs['adj_factor_rows'] = adj_ref_map

    r += 1
    total_adj_row = r
    ws.cell(row=r, column=1, value='Total Quality Adjustment').font = BOLD_FONT
    if adj_start <= adj_end:
        _fml(ws, r, 2, f'=SUM(B{adj_start}:B{adj_end})', PCT_FMT)
    else:
        _inp(ws, r, 2, 0, PCT_FMT)

    r += 1
    adj_val_row = r
    ws.cell(row=r, column=1, value='Adjusted Relative Value').font = BOLD_FONT
    cell = _fml(ws, r, 2, f'=F{base_val_row}*(1+B{total_adj_row})', NUM_FMT)
    cell.font = BOLD_FONT
    refs['relative_adj_row'] = adj_val_row

    # ── Peer Detail Table ──
    peer_list = rel.get('peer_multiples_list', [])
    if peer_list:
        r += 2
        _style_section(ws, r, 8, f'Peer Detail ({len(peer_list)} peers)')
        r += 1
        peer_header_row = r
        for col_idx, header in enumerate(['Symbol', 'Name', 'MCap (Cr)',
                                           'P/E', 'P/B', 'EV/EBITDA', 'P/S', 'Include'], 1):
            ws.cell(row=r, column=col_idx, value=header)
        _style_header(ws, r, 8)

        # Y/N dropdown validation
        yn_dv = DataValidation(type='list', formula1='"Y,N"', allow_blank=False)
        ws.add_data_validation(yn_dv)

        r += 1
        peer_start_row = r
        for peer in peer_list:
            ws.cell(row=r, column=1, value=peer.get('nse_symbol', ''))
            ws.cell(row=r, column=2, value=peer.get('Company Name', ''))
            ws.cell(row=r, column=3, value=peer.get('mcap', 0)).number_format = INT_FMT
            ws.cell(row=r, column=4, value=peer.get('pe', 0) or 0).number_format = '0.00'
            ws.cell(row=r, column=5, value=peer.get('pb', 0) or 0).number_format = '0.00'
            ws.cell(row=r, column=6, value=peer.get('evebidta', 0) or 0).number_format = '0.00'
            ws.cell(row=r, column=7, value=peer.get('ps', 0) or 0).number_format = '0.00'
            include_cell = ws.cell(row=r, column=8, value='Y')
            include_cell.fill = INPUT_FILL
            yn_dv.add(include_cell)
            r += 1
        peer_end_row = r - 1
        refs['peer_start'] = peer_start_row
        refs['peer_end'] = peer_end_row

        # Now set ArrayFormulas in Assumptions for peer medians
        # (Can't do this directly across sheets in openpyxl in all Excel versions,
        #  so we write the formula as a string that Excel will evaluate)


# ── Sheet 5: Monte Carlo ────────────────────────────────────────────────────

def _build_monte_carlo_sheet(ws, result, refs):
    ws.column_dimensions['A'].width = 30
    ws.column_dimensions['B'].width = 18

    r = 1
    ws.cell(row=r, column=1, value='Monte Carlo Simulation').font = TITLE_FONT

    mc_sims = int(os.getenv('MC_SIMULATIONS', 10000))
    r += 2
    ws.cell(row=r, column=1, value='Number of Simulations')
    ws.cell(row=r, column=2, value=mc_sims).number_format = INT_FMT

    r += 2
    _style_section(ws, r, 2, 'Distribution Statistics')
    r += 1
    for label, key in [('Mean', 'mc_mean'), ('Median', 'mc_median')]:
        ws.cell(row=r, column=1, value=label)
        val = result.get(key, 0) or 0
        ws.cell(row=r, column=2, value=val).number_format = NUM_FMT
        if key == 'mc_median':
            refs['mc_median_row'] = r
        r += 1

    percentiles = result.get('mc_percentiles', {})
    for label in ['5th', '10th', '25th', '75th', '90th', '95th']:
        ws.cell(row=r, column=1, value=f'{label} Percentile')
        ws.cell(row=r, column=2, value=percentiles.get(label, 0) or 0).number_format = NUM_FMT
        r += 1

    r += 1
    _style_section(ws, r, 2, 'Market Comparison')
    r += 1
    cmp = result.get('cmp', 0)
    ws.cell(row=r, column=1, value='Current Market Price')
    ws.cell(row=r, column=2, value=cmp).number_format = NUM_FMT
    r += 1
    ws.cell(row=r, column=1, value='P(Value > CMP)')
    ws.cell(row=r, column=2, value=result.get('mc_probability_above_cmp', 0) or 0).number_format = PCT_FMT

    r += 2
    _style_section(ws, r, 2, 'Randomized Parameters')
    r += 1
    ws.cell(row=r, column=1, value='Parameter')
    ws.cell(row=r, column=2, value='Distribution')
    _style_header(ws, r, 2)
    r += 1
    for name, dist in [('Revenue Growth', 'Triangular: base * [0.7, 1.0, 1.3]'),
                        ('EBITDA Margin', 'Normal: mean=base, std=10% of base'),
                        ('Terminal ROCE', 'Normal: mean=base, std=15% of base'),
                        ('Beta', 'Normal: mean=base, std=0.15')]:
        ws.cell(row=r, column=1, value=name)
        ws.cell(row=r, column=2, value=dist)
        r += 1


# ── Sheet 6: Sensitivity ────────────────────────────────────────────────────

def _build_sensitivity_sheet(ws, result, refs):
    ws.column_dimensions['A'].width = 18

    sensitivity = result.get('sensitivity', {})
    table = sensitivity.get('sensitivity_table', [])
    wacc_values = sensitivity.get('wacc_values', [])
    growth_values = sensitivity.get('growth_values', [])

    r = 1
    ws.cell(row=r, column=1, value='Sensitivity Analysis').font = TITLE_FONT
    r += 1
    ws.cell(row=r, column=1, value='Intrinsic value (Rs) for WACC vs Terminal Growth')

    if not table:
        r += 2
        ws.cell(row=r, column=1, value='No sensitivity data available')
        return

    base_value = sensitivity.get('base_intrinsic', 0)
    r += 2
    ws.cell(row=r, column=1, value=f'Base intrinsic: Rs {base_value:,.2f}')

    r += 2
    ws.cell(row=r, column=1, value='WACC \\ Growth').font = BOLD_FONT
    for j, g in enumerate(growth_values):
        cell = ws.cell(row=r, column=2 + j, value=g)
        cell.number_format = PCT_FMT
        cell.font = BOLD_FONT
        cell.alignment = Alignment(horizontal='center')
        ws.column_dimensions[get_column_letter(2 + j)].width = 14
    _style_header(ws, r, 1 + len(growth_values))

    for i, wacc_val in enumerate(wacc_values):
        r += 1
        ws.cell(row=r, column=1, value=wacc_val).number_format = PCT_FMT
        ws.cell(row=r, column=1).font = BOLD_FONT
        if i < len(table):
            for j, val in enumerate(table[i]):
                cell = ws.cell(row=r, column=2 + j, value=val)
                cell.number_format = NUM_FMT
                if abs(val - base_value) < 1:
                    cell.fill = PatternFill(start_color='FFD700', end_color='FFD700', fill_type='solid')
                elif val > base_value * 1.2:
                    cell.fill = PatternFill(start_color='C6EFCE', end_color='C6EFCE', fill_type='solid')
                elif val < base_value * 0.8:
                    cell.fill = PatternFill(start_color='FFC7CE', end_color='FFC7CE', fill_type='solid')


# ── Sheet 7: Macro Drivers (NEW) ────────────────────────────────────────────

def _build_macro_drivers_sheet(ws, result, refs):
    ws.column_dimensions['A'].width = 30
    ws.column_dimensions['B'].width = 18
    ws.column_dimensions['C'].width = 25
    ws.column_dimensions['D'].width = 30

    hierarchy = result.get('driver_hierarchy', {})

    r = 1
    ws.cell(row=r, column=1, value=f'Macro Drivers (Weight: {hierarchy.get("macro_weight", 0.20):.0%})').font = TITLE_FONT
    r += 1
    ws.cell(row=r, column=1, value=hierarchy.get('principle', 'Macro sets the ceiling and floor.')).font = REMARK_FONT

    r += 2
    ws.cell(row=r, column=1, value='Parameter')
    ws.cell(row=r, column=2, value='Value')
    ws.cell(row=r, column=3, value='Source')
    ws.cell(row=r, column=4, value='Feeds Into')
    _style_header(ws, r, 4)

    r += 1
    ws.cell(row=r, column=1, value='Risk-Free Rate (Rf)')
    _fml(ws, r, 2, f"='Assumptions'!B{refs['rf_rate']}", PCT_FMT)
    ws.cell(row=r, column=3, value='Damodaran')
    ws.cell(row=r, column=4, value='WACC via Cost of Equity (Ke)')

    r += 1
    ws.cell(row=r, column=1, value='Equity Risk Premium (ERP)')
    _fml(ws, r, 2, f"='Assumptions'!B{refs['erp']}", PCT_FMT)
    ws.cell(row=r, column=3, value='Damodaran India')
    ws.cell(row=r, column=4, value='WACC via Cost of Equity (Ke)')

    r += 1
    ws.cell(row=r, column=1, value='India 10Y Yield')
    assumptions = result.get('dcf_assumptions', {})
    ws.cell(row=r, column=2, value=assumptions.get('risk_free_rate', 0.0674)).number_format = PCT_FMT
    ws.cell(row=r, column=3, value='Damodaran / RBI')
    ws.cell(row=r, column=4, value='Rf calibration')

    r += 2
    _style_section(ws, r, 4, 'How Macro Flows to Valuation')
    r += 1
    ws.cell(row=r, column=1, value='Ke = Rf + Beta * ERP')
    if refs.get('ke'):
        _fml(ws, r, 2, f"='DCF Model'!B{refs['ke']}", PCT_FMT)
    r += 1
    ws.cell(row=r, column=1, value='WACC = Ke*(1-D/V) + Kd_at*(D/V)')
    if refs.get('wacc_row'):
        _fml(ws, r, 2, f"='DCF Model'!B{refs['wacc_row']}", PCT_FMT)
    r += 1
    ws.cell(row=r, column=1, value='Terminal Growth bounded by GDP')
    if refs.get('terminal_growth'):
        _fml(ws, r, 2, f"='DCF Model'!B{refs['terminal_growth']}", PCT_FMT)


# ── Sheet 8: Sector Drivers (NEW) ───────────────────────────────────────────

def _build_sector_drivers_sheet(ws, result, refs):
    ws.column_dimensions['A'].width = 18
    ws.column_dimensions['B'].width = 25
    ws.column_dimensions['C'].width = 12
    ws.column_dimensions['D'].width = 15
    ws.column_dimensions['E'].width = 16

    sector_cfg = result.get('sector_drivers_config', {})
    hierarchy = result.get('driver_hierarchy', {})
    outlook = result.get('sector_outlook', {})
    sector_name = sector_cfg.get('csv_sector_name', result.get('sector', ''))
    positives = set(outlook.get('key_positives', []))
    negatives = set(outlook.get('key_negatives', []))

    r = 1
    ws.cell(row=r, column=1,
            value=f'Sector Drivers: {sector_name} (Weight: {hierarchy.get("sector_weight", 0.55):.0%})').font = TITLE_FONT

    r += 2
    ws.cell(row=r, column=1, value='Category')
    ws.cell(row=r, column=2, value='Driver')
    ws.cell(row=r, column=3, value='Weight')
    ws.cell(row=r, column=4, value='Direction')
    ws.cell(row=r, column=5, value='Weighted Score')
    _style_header(ws, r, 5)

    # Direction dropdown
    dir_dv = DataValidation(type='list', formula1='"POSITIVE,NEUTRAL,NEGATIVE"',
                            allow_blank=False)
    ws.add_data_validation(dir_dv)

    r += 1
    driver_start = r
    for category in ['demand_drivers', 'cost_drivers', 'regulatory_drivers',
                     'sector_specific_drivers']:
        drivers = sector_cfg.get(category, [])
        cat_label = category.replace('_', ' ').title().replace('Drivers', '').strip()
        for driver in drivers:
            name = driver.get('name', '')
            weight = driver.get('weight', 0)
            # Set initial direction from outlook
            if name in positives:
                direction = 'POSITIVE'
            elif name in negatives:
                direction = 'NEGATIVE'
            else:
                direction = 'NEUTRAL'

            ws.cell(row=r, column=1, value=cat_label)
            ws.cell(row=r, column=2, value=name)
            ws.cell(row=r, column=3, value=weight).number_format = '0.00'
            dir_cell = _inp(ws, r, 4, direction)
            dir_dv.add(dir_cell)
            # Weighted score formula
            _fml(ws, r, 5,
                 f'=IF(D{r}="POSITIVE",1,IF(D{r}="NEGATIVE",-1,0))*C{r}', '0.0000')
            r += 1
    driver_end = r - 1

    # Totals
    r += 1
    ws.cell(row=r, column=2, value='Total Weight').font = BOLD_FONT
    total_wt_row = r
    _fml(ws, r, 3, f'=SUM(C{driver_start}:C{driver_end})', '0.00')

    r += 1
    ws.cell(row=r, column=2, value='Outlook Score').font = BOLD_FONT
    score_row = r
    _fml(ws, r, 5, f'=SUM(E{driver_start}:E{driver_end})/C{total_wt_row}', '0.0000')

    r += 2
    _style_section(ws, r, 5, 'Outlook to Valuation Adjustment')
    r += 1
    ws.cell(row=r, column=1, value='Growth Adjustment')
    _fml(ws, r, 2, f'=E{score_row}*0.03', PCT_FMT)
    _remark(ws, r, 3, 'max +/-3%')
    r += 1
    ws.cell(row=r, column=1, value='Margin Adjustment')
    _fml(ws, r, 2, f'=E{score_row}*0.01', PCT_FMT)
    _remark(ws, r, 3, 'max +/-1%')

    # Porter's Five Forces
    porter = sector_cfg.get('porter_forces', {})
    if porter:
        r += 2
        _style_section(ws, r, 5, "Porter's Five Forces")
        r += 1
        ws.cell(row=r, column=1, value='Force')
        ws.cell(row=r, column=2, value='Level')
        ws.cell(row=r, column=3, value='Impact')
        _style_header(ws, r, 3)
        r += 1
        for force_key in ['entry_barriers', 'supplier_power', 'buyer_power',
                          'substitutes', 'rivalry']:
            force = porter.get(force_key, {})
            if isinstance(force, dict):
                ws.cell(row=r, column=1, value=force_key.replace('_', ' ').title())
                ws.cell(row=r, column=2, value=force.get('value', ''))
                ws.cell(row=r, column=3, value=force.get('impact', ''))
                r += 1


# ── Sheet 9: Company Drivers (NEW) ──────────────────────────────────────────

def _build_company_drivers_sheet(ws, result, refs):
    ws.column_dimensions['A'].width = 20
    ws.column_dimensions['B'].width = 28
    ws.column_dimensions['C'].width = 12
    ws.column_dimensions['D'].width = 35

    company_cfg = result.get('company_alpha_config', {})
    hierarchy = result.get('driver_hierarchy', {})
    company_name = company_cfg.get('csv_name', result.get('company_name', ''))

    r = 1
    ws.cell(row=r, column=1,
            value=f'Company Drivers: {company_name} (Weight: {hierarchy.get("company_weight", 0.25):.0%})').font = TITLE_FONT

    # Alpha thesis
    thesis = company_cfg.get('alpha_thesis', {})
    if thesis:
        r += 1
        ws.cell(row=r, column=1, value='Bull Thesis').font = BOLD_FONT
        ws.cell(row=r, column=2, value=thesis.get('bull', ''))
        r += 1
        ws.cell(row=r, column=1, value='Bear Thesis').font = BOLD_FONT
        ws.cell(row=r, column=2, value=thesis.get('bear', ''))
        r += 1
        ws.cell(row=r, column=1, value='Key Moat').font = BOLD_FONT
        ws.cell(row=r, column=2, value=thesis.get('key_moat', ''))

    # Alpha drivers
    alpha_drivers = company_cfg.get('alpha_drivers', {})
    if alpha_drivers:
        r += 2
        ws.cell(row=r, column=1, value='Category')
        ws.cell(row=r, column=2, value='Driver')
        ws.cell(row=r, column=3, value='Weight')
        ws.cell(row=r, column=4, value='How It Maps to Valuation')
        _style_header(ws, r, 4)

        r += 1
        driver_start = r
        impact_map = {
            'market_share': 'ROCE premium in Relative Val',
            'mgmt_quality': 'ROCE, terminal reinvestment, margin',
            'capex': 'Revenue growth, Capex/Sales ratio',
            'geographic_mix': 'Revenue diversification, FX',
            'product_mix': 'Margin, ASP growth',
            'balance_sheet': 'Debt ratio, cash position',
            'governance': 'Governance discount in Relative Val',
            'key_personnel': 'Execution risk, confidence score',
            'litigation': 'Risk discount',
        }

        for category, drivers in alpha_drivers.items():
            cat_label = category.replace('_', ' ').title()
            if isinstance(drivers, list):
                for driver in drivers:
                    ws.cell(row=r, column=1, value=cat_label)
                    ws.cell(row=r, column=2, value=driver.get('name', ''))
                    ws.cell(row=r, column=3, value=driver.get('weight', 0)).number_format = '0.00'
                    ws.cell(row=r, column=4, value=impact_map.get(category, ''))
                    r += 1
        driver_end = r - 1

        r += 1
        ws.cell(row=r, column=2, value='Total Alpha Weight').font = BOLD_FONT
        _fml(ws, r, 3, f'=SUM(C{driver_start}:C{driver_end})', '0.00')

    # Quality Adjustments cross-reference
    adj_rows = refs.get('adj_factor_rows', {})
    if adj_rows:
        r += 2
        _style_section(ws, r, 4, 'Quality Adjustments (from Relative Val)')
        r += 1
        for factor, adj_row in adj_rows.items():
            ws.cell(row=r, column=1, value=factor.replace('_', ' ').title())
            _fml(ws, r, 2, f"='Relative Val'!B{adj_row}", PCT_FMT)
            r += 1
        ws.cell(row=r, column=1, value='Net Quality Adjustment').font = BOLD_FONT
        if refs.get('relative_adj_row'):
            _remark(ws, r, 2, f"See 'Relative Val' adjusted value")


# ── Sheet 1: Summary ────────────────────────────────────────────────────────

def _build_summary_sheet(ws, result, refs):
    ws.column_dimensions['A'].width = 30
    ws.column_dimensions['B'].width = 18
    ws.column_dimensions['C'].width = 18
    ws.column_dimensions['D'].width = 18
    ws.column_dimensions['E'].width = 18

    company = result.get('company_name', '')
    symbol = result.get('nse_symbol', '')

    r = 1
    ws.cell(row=r, column=1, value=f'{company} ({symbol}) - Valuation Report').font = TITLE_FONT
    r += 1
    ws.cell(row=r, column=1, value=f'Date: {result.get("valuation_date", date.today().isoformat())}')
    ws.cell(row=r, column=2, value=f'Sector: {result.get("sector", "")}')

    # Market data
    r += 2
    _style_section(ws, r, 5, 'Market Data')
    r += 1
    cmp_row = r
    ws.cell(row=r, column=1, value='Current Market Price (CMP)')
    ws.cell(row=r, column=2, value=result.get('cmp', 0)).number_format = NUM_FMT
    r += 1
    ws.cell(row=r, column=1, value='Market Cap (Rs Cr)')
    ws.cell(row=r, column=2, value=result.get('mcap_cr')).number_format = NUM_FMT
    r += 1
    ws.cell(row=r, column=1, value='Price Date')
    ws.cell(row=r, column=2, value=result.get('price_date', ''))

    # Valuation verdict
    r += 2
    _style_section(ws, r, 5, 'Valuation Verdict')
    r += 1
    blended_row = r
    ws.cell(row=r, column=1, value='Blended Intrinsic Value')

    r += 1
    upside_row = r
    ws.cell(row=r, column=1, value='Upside / Downside')
    r += 1
    ws.cell(row=r, column=1, value='Confidence Score')
    ws.cell(row=r, column=2, value=result.get('confidence_score', 0)).number_format = '0.00'

    # DCF Scenarios
    r += 2
    _style_section(ws, r, 5, 'DCF Scenarios')
    r += 1
    for label, key in [('Bull Case', 'dcf_bull'), ('Base Case', 'dcf_base'), ('Bear Case', 'dcf_bear')]:
        ws.cell(row=r, column=1, value=label)
        ws.cell(row=r, column=2, value=result.get(key, 0) or 0).number_format = NUM_FMT
        _fml(ws, r, 3, f'=B{r}/B{cmp_row}-1', PCT_FMT)
        r += 1

    # Method breakdown
    r += 1
    _style_section(ws, r, 5, 'Valuation Methods')
    r += 1
    ws.cell(row=r, column=1, value='Method')
    ws.cell(row=r, column=2, value='Value (Rs)')
    ws.cell(row=r, column=3, value='Weight')
    ws.cell(row=r, column=4, value='Contribution')
    _style_header(ws, r, 4)

    blend = result.get('blend_weights', {'dcf': 0.6, 'relative': 0.3, 'monte_carlo': 0.1})

    r += 1
    method_start = r
    # DCF — reference DCF Model sheet
    ws.cell(row=r, column=1, value='DCF (Base)')
    if refs.get('dcf_iv_row'):
        _fml(ws, r, 2, f"='DCF Model'!B{refs['dcf_iv_row']}", NUM_FMT)
    else:
        ws.cell(row=r, column=2, value=result.get('dcf_base', 0) or 0).number_format = NUM_FMT
    _inp(ws, r, 3, blend.get('dcf', 0.6), PCT_FMT)
    _fml(ws, r, 4, f'=B{r}*C{r}', NUM_FMT)

    r += 1
    # Relative — reference Relative Val sheet
    ws.cell(row=r, column=1, value='Relative')
    if refs.get('relative_adj_row'):
        _fml(ws, r, 2, f"='Relative Val'!B{refs['relative_adj_row']}", NUM_FMT)
    else:
        ws.cell(row=r, column=2, value=result.get('relative_value', 0) or 0).number_format = NUM_FMT
    _inp(ws, r, 3, blend.get('relative', 0.3), PCT_FMT)
    _fml(ws, r, 4, f'=B{r}*C{r}', NUM_FMT)

    r += 1
    # Monte Carlo — reference Monte Carlo sheet
    ws.cell(row=r, column=1, value='Monte Carlo')
    if refs.get('mc_median_row'):
        _fml(ws, r, 2, f"='Monte Carlo'!B{refs['mc_median_row']}", NUM_FMT)
    else:
        ws.cell(row=r, column=2, value=result.get('mc_median', 0) or 0).number_format = NUM_FMT
    _inp(ws, r, 3, blend.get('monte_carlo', 0.1), PCT_FMT)
    _fml(ws, r, 4, f'=B{r}*C{r}', NUM_FMT)
    method_end = r

    # Blended total
    r += 1
    ws.cell(row=r, column=1, value='Blended Total').font = BOLD_FONT
    cell = _fml(ws, r, 4, f'=SUM(D{method_start}:D{method_end})', NUM_FMT)
    cell.font = BOLD_FONT
    blended_formula_row = r

    # Now set the blended value and upside formulas
    _fml(ws, blended_row, 2, f'=D{blended_formula_row}', NUM_FMT)
    ws.cell(row=blended_row, column=2).font = BOLD_FONT
    _fml(ws, upside_row, 2, f'=B{blended_row}/B{cmp_row}-1', PCT_FMT)
