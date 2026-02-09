# Exchange Filing Data Integration — Plan

## Context

Our valuation system relies on a core CSV that gets refreshed periodically. Companies file quarterly/annual results to BSE and NSE — this is the most authoritative, freshest source of financial data. We want to ingest ALL available information from exchange filings, not just what we currently get from core CSV. This gives us:
- Fresher data (within hours of board meeting vs weeks/months lag in core CSV)
- More data fields (segment revenue, exceptional items, tax breakdowns, EPS variants, auditor qualifications)
- Event-driven triggers (fetch on result announcement, not on a fixed schedule)
- Cross-validation of core CSV figures against official filings

---

## Research Findings

### Available Data from Exchange Filings

**Quarterly Results** (filed within 24-48 hrs of board meeting):
- Revenue from operations, Other income, Total income
- Total expenses breakdown (material costs, employee costs, other expenses)
- EBITDA/PBIDT, Depreciation, Finance/interest costs
- PBT (before and after exceptional items), Exceptional items detail
- Tax expense (current + deferred), PAT
- Other comprehensive income, Total comprehensive income
- EPS (basic + diluted), Paid-up equity share capital, Face value
- Segment-wise revenue and profit (multi-segment companies)
- Previous quarter and year-ago quarter comparisons

**Annual Results** (filed within 60 days of FY-end):
- All quarterly items plus:
- Complete balance sheet (all assets, liabilities, equity line items)
- Cash flow statement (operating, investing, financing with sub-items)
- Key financial ratios
- Related party transactions
- Auditor qualifications/emphasis of matter

**Shareholding Patterns** (filed within 21 days of quarter-end):
- Promoter + promoter group holding (pledged/encumbered detail)
- FII/FPI holding, DII/MF holding, Public holding
- Top 10 shareholders, shareholders > 1%

**Corporate Announcements** (Regulation 30 — filed same day):
- Board meeting outcomes (dividends, buybacks, splits, bonus, rights)
- Mergers/demergers/acquisitions
- Credit rating changes
- Debt issuance/repayment
- Management changes, Related party approvals

### Access Methods

**NSE JSON API (Primary — no XML parsing needed)**:
- `nseindia.com/api/results-comparision?symbol=X` — past quarterly/annual results as JSON
- `nseindia.com/api/corporates-financial-results?index=equities&period=Quarterly` — recent filings list
- `nseindia.com/api/event-calendar` — upcoming board meetings (result dates)
- `nseindia.com/api/corporate-announcements?index=equities` — announcements feed
- Rate limit: ~3 req/sec, anti-bot cookie management required (hit homepage first for session cookie)
- No authentication, but needs proper User-Agent + cookie handling

**XBRL Files (Secondary — for deep balance sheet/cash flow data)**:
- XBRL mandatory from Apr 2025 (Q4 FY2025+)
- Ind-AS taxonomy, parseable with python-xbrl / arelle / brel
- No bulk download API — per-company scraping needed
- More complex but gives full balance sheet detail not in JSON API

### Timing & Triggers

| Event | Typical Timing | Our Action |
|---|---|---|
| Board meeting (results) | After market hours, 6-10 PM IST | Fetch within 1-2 hours |
| XBRL filing | Within 24 hrs of board meeting | Fetch next morning |
| Shareholding pattern | 21 days after quarter-end | Fetch once per quarter |
| Annual results | 60 days after FY-end (May-June) | Fetch when filed |

For event-driven fetching: NSE event calendar API shows board meeting dates. We can monitor this daily and trigger fetches for companies whose board meetings occurred that day.

---

## Plan: Prototype First

### Step 1: Probe the NSE API (understand what we actually get)

Write a prototype script that:

1. **Establishes NSE session**: Hit `nseindia.com` homepage to get cookies, then call APIs
2. **Fetches results for 5 pilot companies**: Eicher (EICHERMOT), BEL (BEL), VBL (VARUNBEV), Aether (AETHER), CRISIL (CRISIL)
3. **For each company, call these endpoints**:
   - `/api/results-comparision?symbol=X` — quarterly/annual results history
   - `/api/quote-equity?symbol=X` — current quote + basic info
4. **Dumps full raw JSON** to files for inspection (no filtering — we want to see everything available)
5. **Also tries these discovery endpoints**:
   - `/api/corporates-financial-results?index=equities&period=Quarterly` — recent filings list
   - `/api/event-calendar` — board meeting dates
   - `/api/corporate-announcements?index=equities&symbol=X` — announcements
6. **Reports**: What fields are returned, data types, how many quarters of history, what's missing vs core CSV

**Output**: Raw JSON dumps + field inventory report. This tells us exactly what we can ingest before building the full loader.

### Step 2: Compare Against Core CSV

For the 5 pilot companies, compare NSE API data against core CSV:
- Which quarters are available in API vs core CSV?
- Do sales/PBIDT/PAT figures match? (should be identical — same source)
- What fields does NSE API have that core CSV doesn't?
- What does core CSV have that NSE API doesn't?

### Step 3: Design Full Loader (based on prototype findings)

After seeing the actual data, design:
- Field mapping (NSE JSON keys → our metric names)
- Storage format (per-company JSON cache? MySQL table? Append to core CSV structure?)
- Event-driven trigger (monitor board meeting calendar → fetch results same day)
- Twice-daily sweep for catching announcements (morning 8 AM + evening 8 PM IST)

---

## Files to Create

| File | Description |
|---|---|
| `valuation_system/data/loaders/nse_filing_prototype.py` | **NEW** — Prototype script for Step 1 + 2 |

Script should dump results to `valuation_system/data/cache/nse_prototype/` for inspection.

---

## Verification

1. Run prototype for 5 companies — confirm API is accessible and returns data
2. Inspect raw JSON — catalog all available fields
3. Cross-check 2-3 quarters of sales/PAT against core CSV
4. Report findings before proceeding to full implementation
