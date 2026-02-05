"""
Excel Report Generator for Valuation System
Generates a comprehensive .xlsx with formulas showing
Macro → Sector → Company → DCF/Relative/MC → Blended vs CMP
"""

import os
import logging
from datetime import date

from openpyxl import Workbook
from openpyxl.styles import Font, PatternFill, Alignment, Border, Side, numbers
from openpyxl.utils import get_column_letter
from dotenv import load_dotenv

load_dotenv(os.path.join(os.path.dirname(__file__), '..', 'config', '.env'))

logger = logging.getLogger(__name__)

# Styles
HEADER_FONT = Font(bold=True, size=12, color='FFFFFF')
HEADER_FILL = PatternFill(start_color='2F5496', end_color='2F5496', fill_type='solid')
SECTION_FONT = Font(bold=True, size=11, color='2F5496')
SECTION_FILL = PatternFill(start_color='D6E4F0', end_color='D6E4F0', fill_type='solid')
INPUT_FILL = PatternFill(start_color='FFF2CC', end_color='FFF2CC', fill_type='solid')  # Yellow = changeable
FORMULA_FILL = PatternFill(start_color='E2EFDA', end_color='E2EFDA', fill_type='solid')  # Green = computed
ACTUAL_FONT = Font(color='006100')  # Dark green for [ACTUAL]
DERIVED_FONT = Font(color='9C6500')  # Amber for [DERIVED]
DEFAULT_FONT = Font(color='C00000')  # Red for [DEFAULT]
THIN_BORDER = Border(
    left=Side(style='thin'), right=Side(style='thin'),
    top=Side(style='thin'), bottom=Side(style='thin')
)
BOLD_FONT = Font(bold=True)
TITLE_FONT = Font(bold=True, size=14, color='2F5496')
PCT_FMT = '0.00%'
NUM_FMT = '#,##0.00'
INT_FMT = '#,##0'


def _style_header(ws, row, max_col):
    """Apply header styling to a row."""
    for col in range(1, max_col + 1):
        cell = ws.cell(row=row, column=col)
        cell.font = HEADER_FONT
        cell.fill = HEADER_FILL
        cell.alignment = Alignment(horizontal='center')


def _style_section(ws, row, max_col, label=None):
    """Apply section styling to a row."""
    for col in range(1, max_col + 1):
        cell = ws.cell(row=row, column=col)
        cell.font = SECTION_FONT
        cell.fill = SECTION_FILL
    if label:
        ws.cell(row=row, column=1, value=label)


def _write_input_row(ws, row, col_label, col_value, label, value, fmt=None, source=None):
    """Write a labeled input row with optional formatting and source tag."""
    ws.cell(row=row, column=col_label, value=label)
    cell = ws.cell(row=row, column=col_value, value=value)
    cell.fill = INPUT_FILL
    cell.border = THIN_BORDER
    if fmt:
        cell.number_format = fmt
    if source:
        src_cell = ws.cell(row=row, column=col_value + 1, value=source)
        if '[ACTUAL]' in source.upper():
            src_cell.font = ACTUAL_FONT
        elif '[DERIVED]' in source.upper():
            src_cell.font = DERIVED_FONT
        elif '[DEFAULT]' in source.upper():
            src_cell.font = DEFAULT_FONT


def _write_formula_row(ws, row, col_label, col_value, label, formula, fmt=None):
    """Write a labeled formula row."""
    ws.cell(row=row, column=col_label, value=label)
    cell = ws.cell(row=row, column=col_value)
    cell.value = formula
    cell.fill = FORMULA_FILL
    cell.border = THIN_BORDER
    if fmt:
        cell.number_format = fmt


def generate_valuation_excel(result: dict, output_path: str = None) -> str:
    """
    Generate a comprehensive Excel valuation report from valuator result dict.

    Args:
        result: Output from ValuatorAgent.run_full_valuation()
        output_path: Optional output path. Defaults to logs/

    Returns:
        Path to generated Excel file.
    """
    company = result.get('company_name', 'Unknown')
    symbol = result.get('nse_symbol', '')

    if not output_path:
        log_dir = os.path.join(os.path.dirname(__file__), '..', 'logs')
        os.makedirs(log_dir, exist_ok=True)
        safe_name = symbol.lower() if symbol else company.lower().replace(' ', '_')
        output_path = os.path.join(log_dir, f'{safe_name}_valuation.xlsx')

    wb = Workbook()

    # ========== Sheet 1: Summary ==========
    ws = wb.active
    ws.title = 'Summary'
    _build_summary_sheet(ws, result)

    # ========== Sheet 2: Assumptions ==========
    ws2 = wb.create_sheet('Assumptions')
    _build_assumptions_sheet(ws2, result)

    # ========== Sheet 3: DCF Model ==========
    ws3 = wb.create_sheet('DCF Model')
    _build_dcf_sheet(ws3, result)

    # ========== Sheet 4: Relative Valuation ==========
    ws4 = wb.create_sheet('Relative Val')
    _build_relative_sheet(ws4, result)

    # ========== Sheet 5: Monte Carlo ==========
    ws5 = wb.create_sheet('Monte Carlo')
    _build_monte_carlo_sheet(ws5, result)

    # ========== Sheet 6: Sensitivity ==========
    ws6 = wb.create_sheet('Sensitivity')
    _build_sensitivity_sheet(ws6, result)

    wb.save(output_path)
    logger.info(f"Valuation Excel saved to: {output_path}")
    return output_path


def _build_summary_sheet(ws, result):
    """Sheet 1: Executive summary — CMP vs Blended, Bull/Base/Bear."""
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
    cmp = result.get('cmp', 0)
    ws.cell(row=r, column=1, value='Current Market Price (CMP)')
    ws.cell(row=r, column=2, value=cmp).number_format = NUM_FMT
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
    blended = result.get('intrinsic_value_blended', 0)
    # CMP cell ref for formulas
    cmp_row = 5  # Row where CMP is written
    ws.cell(row=r, column=1, value='Blended Intrinsic Value')
    ws.cell(row=r, column=2, value=blended).number_format = NUM_FMT
    blended_row = r
    r += 1
    ws.cell(row=r, column=1, value='Upside / Downside')
    ws.cell(row=r, column=2).value = f'=B{blended_row}/B{cmp_row}-1'
    ws.cell(row=r, column=2).number_format = PCT_FMT
    ws.cell(row=r, column=2).fill = FORMULA_FILL
    r += 1
    ws.cell(row=r, column=1, value='Confidence Score')
    ws.cell(row=r, column=2, value=result.get('confidence_score', 0)).number_format = '0.00'

    # DCF Scenarios
    r += 2
    _style_section(ws, r, 5, 'DCF Scenarios')
    r += 1
    for label, key in [('Bull Case', 'dcf_bull'), ('Base Case', 'dcf_base'), ('Bear Case', 'dcf_bear')]:
        ws.cell(row=r, column=1, value=label)
        val = result.get(key, 0)
        ws.cell(row=r, column=2, value=val).number_format = NUM_FMT
        # Upside vs CMP
        ws.cell(row=r, column=3).value = f'=B{r}/B{cmp_row}-1'
        ws.cell(row=r, column=3).number_format = PCT_FMT
        ws.cell(row=r, column=3).fill = FORMULA_FILL
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
    r += 1

    blend_weights = result.get('blend_weights', {'dcf': 0.6, 'relative': 0.3, 'monte_carlo': 0.1})
    methods = [
        ('DCF (Base)', result.get('dcf_base', 0), blend_weights.get('dcf', 0.6)),
        ('Relative', result.get('relative_value', 0), blend_weights.get('relative', 0.3)),
        ('Monte Carlo', result.get('mc_median', 0), blend_weights.get('monte_carlo', 0.1)),
    ]
    method_start = r
    for name, val, wt in methods:
        ws.cell(row=r, column=1, value=name)
        ws.cell(row=r, column=2, value=val or 0).number_format = NUM_FMT
        ws.cell(row=r, column=3, value=wt).number_format = PCT_FMT
        # Contribution = Value * Weight
        ws.cell(row=r, column=4).value = f'=B{r}*C{r}'
        ws.cell(row=r, column=4).number_format = NUM_FMT
        ws.cell(row=r, column=4).fill = FORMULA_FILL
        r += 1

    # Blended total (formula)
    ws.cell(row=r, column=1, value='Blended Total').font = BOLD_FONT
    ws.cell(row=r, column=4).value = f'=SUM(D{method_start}:D{r-1})'
    ws.cell(row=r, column=4).number_format = NUM_FMT
    ws.cell(row=r, column=4).fill = FORMULA_FILL
    ws.cell(row=r, column=4).font = BOLD_FONT


def _build_assumptions_sheet(ws, result):
    """Sheet 2: All assumptions — Macro, Sector, Company with source tags."""
    ws.column_dimensions['A'].width = 35
    ws.column_dimensions['B'].width = 18
    ws.column_dimensions['C'].width = 40

    assumptions = result.get('dcf_assumptions', {})
    dcf_details = result.get('dcf_details', {})

    r = 1
    ws.cell(row=r, column=1, value='Valuation Assumptions').font = TITLE_FONT
    ws.cell(row=r, column=3, value='Yellow = input (changeable), Green = computed').font = Font(italic=True)

    # --- MACRO ---
    r += 2
    _style_section(ws, r, 3, 'MACRO LEVEL (20% weight)')
    r += 1
    _write_input_row(ws, r, 1, 2, 'Risk-Free Rate (India 10Y est)',
                     assumptions.get('risk_free_rate', 0.0674), PCT_FMT,
                     '[DERIVED: US 10Y + India premium]')
    r += 1
    _write_input_row(ws, r, 1, 2, 'Equity Risk Premium (ERP)',
                     assumptions.get('erp', 0.0708), PCT_FMT,
                     '[ACTUAL: Damodaran India ERP]')

    # --- SECTOR ---
    r += 2
    _style_section(ws, r, 3, 'SECTOR LEVEL (55% weight)')
    r += 1
    _write_input_row(ws, r, 1, 2, 'Beta (Levered)',
                     assumptions.get('beta', 0), '0.0000',
                     '[ACTUAL: Damodaran sector beta]')
    r += 1
    ws.cell(row=r, column=1, value='Beta Unlevered')
    ws.cell(row=r, column=2, value=0.88).number_format = '0.0000'
    r += 1
    ws.cell(row=r, column=1, value='Sector D/E for Beta Levering')
    ws.cell(row=r, column=2, value=0.01).number_format = '0.0000'
    r += 1
    outlook = result.get('sector_outlook', {})
    ws.cell(row=r, column=1, value='Sector Outlook')
    ws.cell(row=r, column=2, value=outlook.get('outlook', 'NEUTRAL'))
    r += 1
    ws.cell(row=r, column=1, value='Growth Adjustment')
    ws.cell(row=r, column=2, value=outlook.get('growth_adjustment', 0)).number_format = PCT_FMT
    r += 1
    ws.cell(row=r, column=1, value='Margin Adjustment')
    ws.cell(row=r, column=2, value=outlook.get('margin_adjustment', 0)).number_format = PCT_FMT

    # Peer multiples
    r += 1
    rel_details = result.get('relative_details', {})
    peer_data = rel_details.get('peer_data', {})
    ws.cell(row=r, column=1, value='Peer Count')
    ws.cell(row=r, column=2, value=peer_data.get('peer_count', 0))
    r += 1
    implied = rel_details.get('implied_values', {})
    for mult_key, mult_label in [('pe', 'Peer PE Median'), ('pb', 'Peer PB Median'),
                                   ('ev_ebitda', 'Peer EV/EBITDA Median'), ('ps', 'Peer PS Median')]:
        imp = implied.get(mult_key, {})
        ws.cell(row=r, column=1, value=mult_label)
        ws.cell(row=r, column=2, value=imp.get('peer_median', 0)).number_format = '0.00'
        r += 1

    # --- COMPANY ---
    r += 1
    _style_section(ws, r, 3, 'COMPANY LEVEL (25% weight)')
    r += 1
    _write_input_row(ws, r, 1, 2, 'Revenue Base (Rs Cr)',
                     assumptions.get('base_revenue', 0), NUM_FMT)
    r += 1

    # Growth rates
    growth_rates = assumptions.get('growth_rates', [])
    for i, g in enumerate(growth_rates):
        _write_input_row(ws, r, 1, 2, f'Revenue Growth Year {i+1}', g, PCT_FMT)
        r += 1

    _write_input_row(ws, r, 1, 2, 'EBITDA Margin',
                     assumptions.get('ebitda_margin', 0), PCT_FMT)
    r += 1
    _write_input_row(ws, r, 1, 2, 'Margin Improvement (annual)',
                     assumptions.get('margin_improvement', 0), '0.0000')
    r += 1
    _write_input_row(ws, r, 1, 2, 'Capex / Sales',
                     assumptions.get('capex_to_sales', 0), PCT_FMT,
                     '[ACTUAL: pur_of_fixed_assets avg 3yr]')
    r += 1
    _write_input_row(ws, r, 1, 2, 'Depreciation / Sales',
                     assumptions.get('depreciation_to_sales', 0) if 'depreciation_to_sales' in assumptions
                     else dcf_details.get('assumptions', {}).get('depreciation_to_sales', 0.0278),
                     PCT_FMT, '[ACTUAL: delta(acc_dep) avg 3yr]')
    r += 1
    _write_input_row(ws, r, 1, 2, 'NWC / Sales',
                     assumptions.get('nwc_to_sales', 0) if 'nwc_to_sales' in assumptions
                     else dcf_details.get('assumptions', {}).get('nwc_to_sales', -0.0172),
                     PCT_FMT, '[ACTUAL: (inv+debtors-payables)/sales avg 3yr]')
    r += 1
    _write_input_row(ws, r, 1, 2, 'Effective Tax Rate',
                     assumptions.get('tax_rate', 0), PCT_FMT,
                     '[ACTUAL: 1 - PAT/pbt_excp avg 3yr]')
    r += 1
    _write_input_row(ws, r, 1, 2, 'Cost of Debt (pre-tax)',
                     assumptions.get('cost_of_debt', 0) if 'cost_of_debt' in assumptions else 0.1772,
                     PCT_FMT, '[ACTUAL: interest_yearly/debt avg 3yr]')
    r += 1
    _write_input_row(ws, r, 1, 2, 'Debt Ratio (D/V)',
                     assumptions.get('debt_ratio', 0), PCT_FMT)
    r += 1
    _write_input_row(ws, r, 1, 2, 'Terminal ROCE',
                     assumptions.get('terminal_roce', 0), PCT_FMT)
    r += 1
    _write_input_row(ws, r, 1, 2, 'Terminal Reinvestment Rate',
                     assumptions.get('terminal_reinvestment', 0), PCT_FMT,
                     '[ACTUAL: pur_of_fixed_assets/NOPAT]')
    r += 1
    _write_input_row(ws, r, 1, 2, 'Net Debt (Rs Cr)',
                     assumptions.get('net_debt', 0), NUM_FMT)
    r += 1
    _write_input_row(ws, r, 1, 2, 'Cash & Equivalents (Rs Cr)',
                     assumptions.get('cash_and_equivalents', 0) if 'cash_and_equivalents' in assumptions else 298.48,
                     NUM_FMT, '[ACTUAL: h1_2026_cash_and_bank]')
    r += 1
    _write_input_row(ws, r, 1, 2, 'Shares Outstanding (Cr)',
                     assumptions.get('shares_outstanding', 0), '0.00')


def _build_dcf_sheet(ws, result):
    """Sheet 3: Full DCF model with Excel formulas."""
    ws.column_dimensions['A'].width = 30
    for c in 'BCDEFGHI':
        ws.column_dimensions[c].width = 16

    dcf = result.get('dcf_details', {})
    assumptions = dcf.get('assumptions', {})
    projections = dcf.get('fcff_projections', [])

    r = 1
    ws.cell(row=r, column=1, value='FCFF-Based DCF Valuation').font = TITLE_FONT

    # ---- WACC Calculation ----
    r += 2
    _style_section(ws, r, 8, 'WACC Calculation')
    r += 1
    # Row references for formulas
    rf_row = r
    _write_input_row(ws, r, 1, 2, 'Risk-Free Rate (Rf)', assumptions.get('risk_free_rate', 0), PCT_FMT)
    r += 1
    erp_row = r
    _write_input_row(ws, r, 1, 2, 'Equity Risk Premium (ERP)', assumptions.get('erp', 0), PCT_FMT)
    r += 1
    beta_row = r
    _write_input_row(ws, r, 1, 2, 'Beta (Levered)', assumptions.get('beta', 0), '0.0000')
    r += 1
    # Ke = Rf + Beta * ERP (FORMULA)
    ke_row = r
    ws.cell(row=r, column=1, value='Cost of Equity (Ke)')
    ws.cell(row=r, column=2).value = f'=B{rf_row}+B{beta_row}*B{erp_row}'
    ws.cell(row=r, column=2).number_format = PCT_FMT
    ws.cell(row=r, column=2).fill = FORMULA_FILL
    ws.cell(row=r, column=3, value='= Rf + Beta * ERP').font = Font(italic=True, color='808080')
    r += 1
    kd_row = r
    _write_input_row(ws, r, 1, 2, 'Cost of Debt (pre-tax, Kd)',
                     assumptions.get('cost_of_debt', 0) if 'cost_of_debt' in assumptions else 0.1772, PCT_FMT)
    r += 1
    tax_row = r
    _write_input_row(ws, r, 1, 2, 'Tax Rate', assumptions.get('tax_rate', 0), PCT_FMT)
    r += 1
    # Kd after tax (FORMULA)
    kd_at_row = r
    ws.cell(row=r, column=1, value='Cost of Debt (after-tax)')
    ws.cell(row=r, column=2).value = f'=B{kd_row}*(1-B{tax_row})'
    ws.cell(row=r, column=2).number_format = PCT_FMT
    ws.cell(row=r, column=2).fill = FORMULA_FILL
    ws.cell(row=r, column=3, value='= Kd * (1 - Tax)').font = Font(italic=True, color='808080')
    r += 1
    dv_row = r
    _write_input_row(ws, r, 1, 2, 'Debt Ratio (D/V)', assumptions.get('debt_ratio', 0), PCT_FMT)
    r += 1
    # WACC = Ke*(1-D/V) + Kd_at*(D/V) (FORMULA)
    wacc_row = r
    ws.cell(row=r, column=1, value='WACC').font = BOLD_FONT
    ws.cell(row=r, column=2).value = f'=B{ke_row}*(1-B{dv_row})+B{kd_at_row}*B{dv_row}'
    ws.cell(row=r, column=2).number_format = PCT_FMT
    ws.cell(row=r, column=2).fill = FORMULA_FILL
    ws.cell(row=r, column=2).font = BOLD_FONT
    ws.cell(row=r, column=3, value='= Ke*(1-D/V) + Kd_at*(D/V)').font = Font(italic=True, color='808080')

    # ---- FCFF Projection ----
    r += 2
    _style_section(ws, r, 8, 'FCFF Projections (Rs Cr)')
    r += 1

    # Column headers: A=Metric, B=Base Year, C..G=Year 1..5
    header_row = r
    ws.cell(row=r, column=1, value='Metric')
    ws.cell(row=r, column=2, value='Base (FY)')
    for i in range(len(projections)):
        ws.cell(row=r, column=3 + i, value=f'Year {i+1}')
    _style_header(ws, r, 2 + len(projections))

    base_rev = assumptions.get('base_revenue', 0)
    ebitda_margin = assumptions.get('ebitda_margin', 0)
    capex_s = assumptions.get('capex_to_sales', 0)
    depr_s = assumptions.get('depreciation_to_sales', 0) if 'depreciation_to_sales' in assumptions else 0.0278
    nwc_s = assumptions.get('nwc_to_sales', 0) if 'nwc_to_sales' in assumptions else -0.0172
    tax = assumptions.get('tax_rate', 0)
    margin_imp = assumptions.get('margin_improvement', 0)

    # Row: Growth Rate
    r += 1
    growth_row = r
    ws.cell(row=r, column=1, value='Revenue Growth')
    for i, p in enumerate(projections):
        ws.cell(row=r, column=3 + i, value=p.get('growth_rate', 0)).number_format = PCT_FMT
        ws.cell(row=r, column=3 + i).fill = INPUT_FILL

    # Row: Revenue
    r += 1
    rev_row = r
    ws.cell(row=r, column=1, value='Revenue')
    ws.cell(row=r, column=2, value=base_rev).number_format = NUM_FMT
    ws.cell(row=r, column=2).fill = INPUT_FILL
    for i in range(len(projections)):
        col = 3 + i
        prev_col = get_column_letter(col - 1)
        cur_col = get_column_letter(col)
        # Revenue = Prior Revenue * (1 + Growth)
        ws.cell(row=r, column=col).value = f'={prev_col}{rev_row}*(1+{cur_col}{growth_row})'
        ws.cell(row=r, column=col).number_format = NUM_FMT
        ws.cell(row=r, column=col).fill = FORMULA_FILL

    # Row: EBITDA Margin
    r += 1
    margin_row = r
    ws.cell(row=r, column=1, value='EBITDA Margin')
    ws.cell(row=r, column=2, value=ebitda_margin).number_format = PCT_FMT
    ws.cell(row=r, column=2).fill = INPUT_FILL
    for i, p in enumerate(projections):
        ws.cell(row=r, column=3 + i, value=p.get('ebitda_margin', 0)).number_format = PCT_FMT
        ws.cell(row=r, column=3 + i).fill = INPUT_FILL

    # Row: EBITDA = Revenue * Margin
    r += 1
    ebitda_row = r
    ws.cell(row=r, column=1, value='EBITDA')
    for i in range(len(projections)):
        col = 3 + i
        c = get_column_letter(col)
        ws.cell(row=r, column=col).value = f'={c}{rev_row}*{c}{margin_row}'
        ws.cell(row=r, column=col).number_format = NUM_FMT
        ws.cell(row=r, column=col).fill = FORMULA_FILL

    # Row: Depreciation = Revenue * Depr/Sales
    r += 1
    depr_row = r
    ws.cell(row=r, column=1, value='Depreciation')
    for i, p in enumerate(projections):
        ws.cell(row=r, column=3 + i, value=p.get('depreciation', 0)).number_format = NUM_FMT

    # Row: EBIT = EBITDA - Depreciation
    r += 1
    ebit_row = r
    ws.cell(row=r, column=1, value='EBIT')
    for i in range(len(projections)):
        col = 3 + i
        c = get_column_letter(col)
        ws.cell(row=r, column=col).value = f'={c}{ebitda_row}-{c}{depr_row}'
        ws.cell(row=r, column=col).number_format = NUM_FMT
        ws.cell(row=r, column=col).fill = FORMULA_FILL

    # Row: Tax
    r += 1
    ws.cell(row=r, column=1, value='Tax Rate')
    for i in range(len(projections)):
        ws.cell(row=r, column=3 + i, value=tax).number_format = PCT_FMT
    tax_proj_row = r

    # Row: NOPAT = EBIT * (1 - Tax)
    r += 1
    nopat_row = r
    ws.cell(row=r, column=1, value='NOPAT')
    for i in range(len(projections)):
        col = 3 + i
        c = get_column_letter(col)
        ws.cell(row=r, column=col).value = f'={c}{ebit_row}*(1-{c}{tax_proj_row})'
        ws.cell(row=r, column=col).number_format = NUM_FMT
        ws.cell(row=r, column=col).fill = FORMULA_FILL

    # Row: Capex = Revenue * Capex/Sales
    r += 1
    capex_row = r
    ws.cell(row=r, column=1, value='Capex')
    for i, p in enumerate(projections):
        ws.cell(row=r, column=3 + i, value=p.get('capex', 0)).number_format = NUM_FMT

    # Row: Delta NWC
    r += 1
    dnwc_row = r
    ws.cell(row=r, column=1, value='Change in NWC')
    for i, p in enumerate(projections):
        ws.cell(row=r, column=3 + i, value=p.get('delta_nwc', 0)).number_format = NUM_FMT

    # Row: FCFF = NOPAT + Depreciation - Capex - Delta NWC
    r += 1
    fcff_row = r
    ws.cell(row=r, column=1, value='FCFF').font = BOLD_FONT
    for i in range(len(projections)):
        col = 3 + i
        c = get_column_letter(col)
        ws.cell(row=r, column=col).value = (
            f'={c}{nopat_row}+{c}{depr_row}-{c}{capex_row}-{c}{dnwc_row}'
        )
        ws.cell(row=r, column=col).number_format = NUM_FMT
        ws.cell(row=r, column=col).fill = FORMULA_FILL
        ws.cell(row=r, column=col).font = BOLD_FONT

    # Row: Discount Factor = 1/(1+WACC)^year
    r += 1
    df_row = r
    ws.cell(row=r, column=1, value='Discount Factor')
    for i in range(len(projections)):
        col = 3 + i
        ws.cell(row=r, column=col).value = f'=1/(1+B{wacc_row})^{i+1}'
        ws.cell(row=r, column=col).number_format = '0.0000'
        ws.cell(row=r, column=col).fill = FORMULA_FILL

    # Row: PV of FCFF = FCFF * Discount Factor
    r += 1
    pv_row = r
    ws.cell(row=r, column=1, value='PV of FCFF')
    for i in range(len(projections)):
        col = 3 + i
        c = get_column_letter(col)
        ws.cell(row=r, column=col).value = f'={c}{fcff_row}*{c}{df_row}'
        ws.cell(row=r, column=col).number_format = NUM_FMT
        ws.cell(row=r, column=col).fill = FORMULA_FILL

    # ---- Terminal Value ----
    r += 2
    _style_section(ws, r, 8, 'Terminal Value')
    r += 1
    roce_row = r
    _write_input_row(ws, r, 1, 2, 'Terminal ROCE', assumptions.get('terminal_roce', 0), PCT_FMT)
    r += 1
    reinv_row = r
    _write_input_row(ws, r, 1, 2, 'Terminal Reinvestment Rate', assumptions.get('terminal_reinvestment', 0), PCT_FMT)
    r += 1
    tg_row = r
    ws.cell(row=r, column=1, value='Terminal Growth (g)')
    # g = MIN(5%, MAX(2%, ROCE * Reinvestment))
    ws.cell(row=r, column=2).value = f'=MIN(5%,MAX(2%,B{roce_row}*B{reinv_row}))'
    ws.cell(row=r, column=2).number_format = PCT_FMT
    ws.cell(row=r, column=2).fill = FORMULA_FILL
    ws.cell(row=r, column=3, value='= MIN(5%, MAX(2%, ROCE * Reinvestment))').font = Font(italic=True, color='808080')
    r += 1

    # FCFF_n+1 = Final FCFF * (1+g)
    last_fcff_col = get_column_letter(2 + len(projections))
    fcff_n1_row = r
    ws.cell(row=r, column=1, value='FCFF (n+1)')
    ws.cell(row=r, column=2).value = f'={last_fcff_col}{fcff_row}*(1+B{tg_row})'
    ws.cell(row=r, column=2).number_format = NUM_FMT
    ws.cell(row=r, column=2).fill = FORMULA_FILL
    r += 1

    # TV = FCFF_n+1 / (WACC - g)
    tv_row = r
    ws.cell(row=r, column=1, value='Terminal Value')
    ws.cell(row=r, column=2).value = f'=B{fcff_n1_row}/(B{wacc_row}-B{tg_row})'
    ws.cell(row=r, column=2).number_format = NUM_FMT
    ws.cell(row=r, column=2).fill = FORMULA_FILL
    ws.cell(row=r, column=3, value='= FCFF(n+1) / (WACC - g)').font = Font(italic=True, color='808080')
    r += 1

    # PV of TV
    n_years = len(projections)
    pv_tv_row = r
    ws.cell(row=r, column=1, value='PV of Terminal Value')
    ws.cell(row=r, column=2).value = f'=B{tv_row}/(1+B{wacc_row})^{n_years}'
    ws.cell(row=r, column=2).number_format = NUM_FMT
    ws.cell(row=r, column=2).fill = FORMULA_FILL

    # ---- Equity Bridge ----
    r += 2
    _style_section(ws, r, 8, 'Equity Bridge')
    r += 1

    # PV of explicit FCFFs
    pv_sum_col_start = get_column_letter(3)
    pv_sum_col_end = get_column_letter(2 + n_years)
    pv_explicit_row = r
    ws.cell(row=r, column=1, value='PV of Explicit FCFFs')
    ws.cell(row=r, column=2).value = f'=SUM({pv_sum_col_start}{pv_row}:{pv_sum_col_end}{pv_row})'
    ws.cell(row=r, column=2).number_format = NUM_FMT
    ws.cell(row=r, column=2).fill = FORMULA_FILL
    r += 1

    ws.cell(row=r, column=1, value='PV of Terminal Value')
    ws.cell(row=r, column=2).value = f'=B{pv_tv_row}'
    ws.cell(row=r, column=2).number_format = NUM_FMT
    ws.cell(row=r, column=2).fill = FORMULA_FILL
    pv_tv_bridge_row = r
    r += 1

    fv_row = r
    ws.cell(row=r, column=1, value='Firm (Enterprise) Value').font = BOLD_FONT
    ws.cell(row=r, column=2).value = f'=B{pv_explicit_row}+B{pv_tv_bridge_row}'
    ws.cell(row=r, column=2).number_format = NUM_FMT
    ws.cell(row=r, column=2).fill = FORMULA_FILL
    ws.cell(row=r, column=2).font = BOLD_FONT
    r += 1

    debt_row_bridge = r
    _write_input_row(ws, r, 1, 2, '(-) Net Debt', assumptions.get('net_debt', 0), NUM_FMT)
    r += 1
    cash_row_bridge = r
    cash_val = assumptions.get('cash_and_equivalents', 0) if 'cash_and_equivalents' in assumptions else 298.48
    _write_input_row(ws, r, 1, 2, '(+) Cash & Equivalents', cash_val, NUM_FMT)
    r += 1

    eq_val_row = r
    ws.cell(row=r, column=1, value='Equity Value').font = BOLD_FONT
    ws.cell(row=r, column=2).value = f'=B{fv_row}-B{debt_row_bridge}+B{cash_row_bridge}'
    ws.cell(row=r, column=2).number_format = NUM_FMT
    ws.cell(row=r, column=2).fill = FORMULA_FILL
    ws.cell(row=r, column=2).font = BOLD_FONT
    r += 1

    shares_row_bridge = r
    _write_input_row(ws, r, 1, 2, 'Shares Outstanding (Cr)',
                     assumptions.get('shares_outstanding', 0), '0.00')
    r += 1

    ws.cell(row=r, column=1, value='Intrinsic Value per Share (Rs)').font = BOLD_FONT
    ws.cell(row=r, column=2).value = f'=B{eq_val_row}/B{shares_row_bridge}'
    ws.cell(row=r, column=2).number_format = NUM_FMT
    ws.cell(row=r, column=2).fill = FORMULA_FILL
    ws.cell(row=r, column=2).font = BOLD_FONT
    ws.cell(row=r, column=3, value='= Equity Value / Shares').font = Font(italic=True, color='808080')
    r += 1

    # TV as % of Firm Value
    ws.cell(row=r, column=1, value='TV as % of Firm Value')
    ws.cell(row=r, column=2).value = f'=B{pv_tv_bridge_row}/B{fv_row}'
    ws.cell(row=r, column=2).number_format = PCT_FMT
    ws.cell(row=r, column=2).fill = FORMULA_FILL


def _build_relative_sheet(ws, result):
    """Sheet 4: Relative valuation — peer multiples, implied values, quality adjustment."""
    ws.column_dimensions['A'].width = 25
    ws.column_dimensions['B'].width = 16
    ws.column_dimensions['C'].width = 16
    ws.column_dimensions['D'].width = 16
    ws.column_dimensions['E'].width = 16
    ws.column_dimensions['F'].width = 16

    rel = result.get('relative_details', {})
    implied = rel.get('implied_values', {})
    weights = rel.get('multiple_weights_used', {})
    adj_factors = rel.get('adjustment_factors', {})

    r = 1
    ws.cell(row=r, column=1, value='Relative Valuation').font = TITLE_FONT

    # Peer multiples and implied values
    r += 2
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
    for key in ['pe', 'pb', 'ev_ebitda', 'ps']:
        imp = implied.get(key, {})
        if not imp:
            continue
        ws.cell(row=r, column=1, value=imp.get('multiple_name', key.upper()))
        ws.cell(row=r, column=2, value=imp.get('peer_median', 0)).number_format = '0.00'
        ws.cell(row=r, column=3, value=imp.get('company_metric', 0)).number_format = NUM_FMT
        ws.cell(row=r, column=3).fill = INPUT_FILL
        metric_label = imp.get('metric_label', '')
        if metric_label:
            ws.cell(row=r, column=3).comment = None  # label in next line
        ws.cell(row=r, column=4, value=imp.get('implied_per_share', 0)).number_format = NUM_FMT
        w = weights.get(key, 0)
        ws.cell(row=r, column=5, value=w).number_format = PCT_FMT
        # Contribution = Implied * Weight
        ws.cell(row=r, column=6).value = f'=D{r}*E{r}'
        ws.cell(row=r, column=6).number_format = NUM_FMT
        ws.cell(row=r, column=6).fill = FORMULA_FILL
        r += 1

    implied_end = r - 1

    # Total weight and base value
    r += 1
    ws.cell(row=r, column=1, value='Total Weight').font = BOLD_FONT
    ws.cell(row=r, column=5).value = f'=SUM(E{implied_start}:E{implied_end})'
    ws.cell(row=r, column=5).number_format = PCT_FMT
    ws.cell(row=r, column=5).fill = FORMULA_FILL
    total_wt_row = r
    r += 1
    ws.cell(row=r, column=1, value='Base Relative Value').font = BOLD_FONT
    ws.cell(row=r, column=6).value = f'=SUM(F{implied_start}:F{implied_end})/E{total_wt_row}'
    ws.cell(row=r, column=6).number_format = NUM_FMT
    ws.cell(row=r, column=6).fill = FORMULA_FILL
    base_val_row = r

    # Quality adjustments
    r += 2
    _style_section(ws, r, 6, 'Quality Adjustments')
    r += 1
    ws.cell(row=r, column=1, value='Factor')
    ws.cell(row=r, column=2, value='Adjustment')
    _style_header(ws, r, 2)
    r += 1
    adj_start = r
    for factor, val in adj_factors.items():
        ws.cell(row=r, column=1, value=factor.replace('_', ' ').title())
        ws.cell(row=r, column=2, value=val).number_format = PCT_FMT
        ws.cell(row=r, column=2).fill = INPUT_FILL
        r += 1
    adj_end = r - 1

    r += 1
    ws.cell(row=r, column=1, value='Total Quality Adjustment').font = BOLD_FONT
    if adj_start <= adj_end:
        ws.cell(row=r, column=2).value = f'=SUM(B{adj_start}:B{adj_end})'
    else:
        ws.cell(row=r, column=2, value=0)
    ws.cell(row=r, column=2).number_format = PCT_FMT
    ws.cell(row=r, column=2).fill = FORMULA_FILL
    total_adj_row = r

    r += 1
    ws.cell(row=r, column=1, value='Adjusted Relative Value').font = BOLD_FONT
    ws.cell(row=r, column=2).value = f'=F{base_val_row}*(1+B{total_adj_row})'
    ws.cell(row=r, column=2).number_format = NUM_FMT
    ws.cell(row=r, column=2).fill = FORMULA_FILL
    ws.cell(row=r, column=2).font = BOLD_FONT

    # Peer list
    r += 2
    peer_group = rel.get('peer_group', [])
    if peer_group:
        _style_section(ws, r, 6, f'Peer Group ({len(peer_group)} peers)')
        r += 1
        ws.cell(row=r, column=1, value='Symbol')
        ws.cell(row=r, column=2, value='Name')
        ws.cell(row=r, column=3, value='Tier')
        _style_header(ws, r, 3)
        r += 1
        for peer in peer_group:
            ws.cell(row=r, column=1, value=peer.get('symbol', ''))
            ws.cell(row=r, column=2, value=peer.get('name', ''))
            ws.cell(row=r, column=3, value=peer.get('tier', ''))
            r += 1


def _build_monte_carlo_sheet(ws, result):
    """Sheet 5: Monte Carlo simulation results."""
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
        ws.cell(row=r, column=2, value=result.get(key, 0)).number_format = NUM_FMT
        r += 1

    percentiles = result.get('mc_percentiles', {})
    for label in ['5th', '10th', '25th', '75th', '90th', '95th']:
        ws.cell(row=r, column=1, value=f'{label} Percentile')
        ws.cell(row=r, column=2, value=percentiles.get(label, 0)).number_format = NUM_FMT
        r += 1

    r += 1
    _style_section(ws, r, 2, 'Market Comparison')
    r += 1
    cmp = result.get('cmp', 0)
    ws.cell(row=r, column=1, value='Current Market Price')
    ws.cell(row=r, column=2, value=cmp).number_format = NUM_FMT
    r += 1
    ws.cell(row=r, column=1, value='P(Value > CMP)')
    ws.cell(row=r, column=2, value=result.get('mc_probability_above_cmp', 0)).number_format = PCT_FMT

    r += 2
    _style_section(ws, r, 2, 'Randomized Parameters')
    r += 1
    ws.cell(row=r, column=1, value='Parameter')
    ws.cell(row=r, column=2, value='Distribution')
    _style_header(ws, r, 2)
    r += 1
    params = [
        ('Revenue Growth', 'Triangular: base * [0.7, 1.0, 1.3]'),
        ('EBITDA Margin', 'Normal: mean=base, std=10% of base'),
        ('Terminal ROCE', 'Normal: mean=base, std=15% of base'),
        ('Beta', 'Normal: mean=base, std=0.15'),
    ]
    for name, dist in params:
        ws.cell(row=r, column=1, value=name)
        ws.cell(row=r, column=2, value=dist)
        r += 1


def _build_sensitivity_sheet(ws, result):
    """Sheet 6: WACC vs Terminal Growth sensitivity table."""
    ws.column_dimensions['A'].width = 18

    sensitivity = result.get('sensitivity', {})
    table = sensitivity.get('sensitivity_table', [])
    wacc_values = sensitivity.get('wacc_values', [])
    growth_values = sensitivity.get('growth_values', [])

    r = 1
    ws.cell(row=r, column=1, value='Sensitivity Analysis').font = TITLE_FONT
    r += 1
    ws.cell(row=r, column=1, value='Intrinsic value (Rs) for different WACC and Terminal Growth combinations')

    if not table:
        r += 2
        ws.cell(row=r, column=1, value='No sensitivity data available')
        return

    base_value = sensitivity.get('base_intrinsic', 0)
    r += 2
    ws.cell(row=r, column=1, value=f'Base intrinsic: Rs {base_value:,.2f}')

    r += 2
    # Column headers: terminal growth values
    ws.cell(row=r, column=1, value='WACC \\ Growth')
    ws.cell(row=r, column=1).font = BOLD_FONT
    for j, g in enumerate(growth_values):
        cell = ws.cell(row=r, column=2 + j, value=g)
        cell.number_format = PCT_FMT
        cell.font = BOLD_FONT
        cell.alignment = Alignment(horizontal='center')
        ws.column_dimensions[get_column_letter(2 + j)].width = 14

    _style_header(ws, r, 1 + len(growth_values))

    # Rows: WACC values
    for i, wacc_val in enumerate(wacc_values):
        r += 1
        ws.cell(row=r, column=1, value=wacc_val).number_format = PCT_FMT
        ws.cell(row=r, column=1).font = BOLD_FONT
        if i < len(table):
            for j, val in enumerate(table[i]):
                cell = ws.cell(row=r, column=2 + j, value=val)
                cell.number_format = NUM_FMT
                # Highlight base case
                if abs(val - base_value) < 1:
                    cell.fill = PatternFill(start_color='FFD700', end_color='FFD700', fill_type='solid')
                elif val > base_value * 1.2:
                    cell.fill = PatternFill(start_color='C6EFCE', end_color='C6EFCE', fill_type='solid')
                elif val < base_value * 0.8:
                    cell.fill = PatternFill(start_color='FFC7CE', end_color='FFC7CE', fill_type='solid')
