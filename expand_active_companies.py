#!/usr/bin/env python3
"""
Expand vs_active_companies from 883 to all 2,160 investable companies.
Reads from Excel taxonomy file, looks up marketscrip for company_id/nse_symbol/bse_code,
and inserts missing companies into vs_active_companies.

Output: Summary to stdout, detailed log to expand_active_companies.log,
errors CSV to expand_active_errors.csv.
"""

import os
import sys
import csv
import traceback
from datetime import datetime

import pandas as pd
import mysql.connector

# Paths
EXCEL_PATH = "/Users/ram/code/research/valuation_group-valuation_subgroup-feb2026.xlsx"
LOG_PATH = "/Users/ram/code/research/expand_active_companies.log"
ERRORS_CSV_PATH = "/Users/ram/code/research/expand_active_errors.csv"

BATCH_SIZE = 100


def log(msg, logf, also_stdout=False):
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
    line = f"[{ts}] {msg}"
    logf.write(line + "\n")
    logf.flush()
    if also_stdout:
        print(msg)


def main():
    with open(LOG_PATH, "w") as logf:
        log("=" * 80, logf)
        log("EXPAND vs_active_companies -- START", logf, also_stdout=True)
        log("=" * 80, logf)

        # --- 1. Load Excel ---
        log(f"Loading Excel: {EXCEL_PATH}", logf)
        df = pd.read_excel(EXCEL_PATH)
        log(f"  Total rows: {len(df)}, Columns: {list(df.columns)}", logf)

        investable = df[df["investable"] == True].copy()
        log(f"  Investable companies: {len(investable)}", logf, also_stdout=True)

        if len(investable) != 2160:
            log(f"  WARNING: Expected 2160 investable, got {len(investable)}", logf, also_stdout=True)

        # --- 2. Connect to MySQL ---
        log("Connecting to MySQL (root@localhost:3306/rag)...", logf)
        conn = mysql.connector.connect(
            host="localhost",
            port=3306,
            user="root",
            password="",
            database="rag",
            autocommit=False,
        )
        cur = conn.cursor(dictionary=True)

        # --- 3. Get existing accord_codes and company_ids ---
        cur.execute("SELECT accord_code, company_id FROM vs_active_companies")
        existing_rows = cur.fetchall()
        existing_accord_codes = set()
        existing_company_ids = set()
        for row in existing_rows:
            if row["accord_code"]:
                existing_accord_codes.add(str(row["accord_code"]).strip())
            if row["company_id"]:
                existing_company_ids.add(row["company_id"])

        log(f"  Existing vs_active_companies: {len(existing_rows)}", logf, also_stdout=True)
        log(f"  Existing accord_codes: {len(existing_accord_codes)}", logf)
        log(f"  Existing company_ids: {len(existing_company_ids)}", logf)

        # --- 4. Preload marketscrip lookup (equity only) ---
        # IMPORTANT: accord_code in marketscrip has trailing '.0' (e.g. '100325.0')
        # Excel has integer accord_codes (e.g. '100325')
        # We normalize by stripping '.0' suffix for the lookup key
        log("Loading marketscrip equity lookup from mssdb...", logf)
        cur.execute(
            "SELECT marketscrip_id, accord_code, name, symbol, scrip_code "
            "FROM mssdb.kbapp_marketscrip "
            "WHERE scrip_type IN ('', 'EQS') AND accord_code IS NOT NULL AND accord_code != ''"
        )
        ms_rows = cur.fetchall()
        ms_lookup = {}
        for r in ms_rows:
            raw_ac = str(r["accord_code"]).strip()
            # Normalize: strip trailing '.0' if present
            if raw_ac.endswith(".0"):
                clean_ac = raw_ac[:-2]
            else:
                clean_ac = raw_ac
            if clean_ac and clean_ac not in ms_lookup:
                ms_lookup[clean_ac] = r
        log(f"  Loaded {len(ms_lookup)} unique accord_code -> marketscrip mappings", logf)

        # --- 5. Process each investable company ---
        inserted = 0
        skipped_existing = 0
        skipped_dup_company_id = 0
        error_count_no_ms = 0
        errors = []
        insert_batch = []

        INSERT_SQL = (
            "INSERT INTO vs_active_companies "
            "(company_id, company_name, nse_symbol, bse_code, accord_code, "
            " sector, industry, valuation_group, valuation_subgroup, "
            " cd_sector, cd_industry, csv_name, "
            " valuation_frequency, priority, is_active, added_date, added_by) "
            "VALUES "
            "(%(company_id)s, %(company_name)s, %(nse_symbol)s, %(bse_code)s, %(accord_code)s, "
            " %(sector)s, %(industry)s, %(valuation_group)s, %(valuation_subgroup)s, "
            " %(cd_sector)s, %(cd_industry)s, %(csv_name)s, "
            " %(valuation_frequency)s, %(priority)s, %(is_active)s, CURDATE(), %(added_by)s)"
        )

        for idx, row in investable.iterrows():
            accord_code = str(int(row["Accord Code"])) if pd.notna(row["Accord Code"]) else ""
            accord_code = accord_code.strip()
            company_name_excel = str(row["Company Name"]).strip() if pd.notna(row["Company Name"]) else ""
            valuation_group = str(row["valuation_group"]).strip() if pd.notna(row["valuation_group"]) else ""
            valuation_subgroup = str(row["valuation_subgroup"]).strip() if pd.notna(row["valuation_subgroup"]) else ""
            cd_sector = str(row["CD_Sector"]).strip() if pd.notna(row["CD_Sector"]) else ""
            cd_industry = str(row["CD_Industry1"]).strip() if pd.notna(row["CD_Industry1"]) else ""

            # Skip if already in vs_active_companies by accord_code
            if accord_code in existing_accord_codes:
                skipped_existing += 1
                log(f"  SKIP (already exists): accord_code={accord_code} ({company_name_excel})", logf)
                continue

            # Look up in marketscrip (using normalized key)
            ms = ms_lookup.get(accord_code)
            if ms is None:
                err_msg = f"accord_code={accord_code} not found in marketscrip (equities)"
                log(f"  ERROR: {err_msg} -- {company_name_excel}", logf)
                errors.append({
                    "accord_code": accord_code,
                    "company_name": company_name_excel,
                    "error_reason": err_msg,
                })
                error_count_no_ms += 1
                continue

            company_id = ms["marketscrip_id"]
            ms_name = ms["name"] or ""
            ms_symbol = ms["symbol"] or ""
            ms_scrip_code = ms["scrip_code"] or ""

            # Check for duplicate company_id
            if company_id in existing_company_ids:
                err_msg = f"company_id={company_id} already exists in vs_active_companies (dup marketscrip_id)"
                log(f"  SKIP (dup company_id): {err_msg} -- accord={accord_code} ({company_name_excel})", logf)
                errors.append({
                    "accord_code": accord_code,
                    "company_name": company_name_excel,
                    "error_reason": err_msg,
                })
                skipped_dup_company_id += 1
                continue

            # Prepare insert params
            params = {
                "company_id": company_id,
                "company_name": ms_name,
                "nse_symbol": ms_symbol if ms_symbol else None,
                "bse_code": ms_scrip_code if ms_scrip_code else None,
                "accord_code": accord_code,
                "sector": valuation_group,
                "industry": valuation_subgroup,
                "valuation_group": valuation_group,
                "valuation_subgroup": valuation_subgroup,
                "cd_sector": cd_sector,
                "cd_industry": cd_industry,
                "csv_name": company_name_excel,
                "valuation_frequency": "WEEKLY",
                "priority": 5,
                "is_active": 1,
                "added_by": "excel_expansion_feb2026",
            }

            insert_batch.append(params)
            existing_company_ids.add(company_id)
            existing_accord_codes.add(accord_code)

            log(f"  QUEUE INSERT: company_id={company_id}, accord={accord_code}, "
                f"name={ms_name}, symbol={ms_symbol}, group={valuation_group}, "
                f"subgroup={valuation_subgroup}", logf)

            # Batch insert
            if len(insert_batch) >= BATCH_SIZE:
                try:
                    cur.executemany(INSERT_SQL, insert_batch)
                    conn.commit()
                    inserted += len(insert_batch)
                    log(f"  COMMITTED batch of {len(insert_batch)} (total inserted: {inserted})", logf)
                except Exception as e:
                    conn.rollback()
                    log(f"  BATCH INSERT ERROR: {e}", logf)
                    log(f"  TRACEBACK: {traceback.format_exc()}", logf)
                    for p in insert_batch:
                        errors.append({
                            "accord_code": p["accord_code"],
                            "company_name": p["csv_name"],
                            "error_reason": f"Batch insert failed: {str(e)}",
                        })
                insert_batch = []

        # Final batch
        if insert_batch:
            try:
                cur.executemany(INSERT_SQL, insert_batch)
                conn.commit()
                inserted += len(insert_batch)
                log(f"  COMMITTED final batch of {len(insert_batch)} (total inserted: {inserted})", logf)
            except Exception as e:
                conn.rollback()
                log(f"  FINAL BATCH INSERT ERROR: {e}", logf)
                log(f"  TRACEBACK: {traceback.format_exc()}", logf)
                for p in insert_batch:
                    errors.append({
                        "accord_code": p["accord_code"],
                        "company_name": p["csv_name"],
                        "error_reason": f"Batch insert failed: {str(e)}",
                    })

        # --- 6. Write errors CSV ---
        log(f"\nWriting errors CSV to {ERRORS_CSV_PATH} ({len(errors)} errors)", logf)
        with open(ERRORS_CSV_PATH, "w", newline="") as ef:
            writer = csv.DictWriter(ef, fieldnames=["accord_code", "company_name", "error_reason"])
            writer.writeheader()
            for err in errors:
                writer.writerow(err)

        # --- 7. Verification query ---
        log("\n" + "=" * 80, logf)
        log("VERIFICATION: vs_active_companies by valuation_group", logf)
        log("=" * 80, logf)

        cur.execute(
            "SELECT valuation_group, COUNT(*) AS cnt "
            "FROM vs_active_companies "
            "GROUP BY valuation_group "
            "ORDER BY cnt DESC"
        )
        verification = cur.fetchall()
        total_final = 0
        log(f"{'valuation_group':<40} {'count':>6}", logf, also_stdout=True)
        log("-" * 48, logf, also_stdout=True)
        for v in verification:
            grp = v["valuation_group"] or "(NULL)"
            cnt = v["cnt"]
            total_final += cnt
            log(f"{grp:<40} {cnt:>6}", logf, also_stdout=True)
        log("-" * 48, logf, also_stdout=True)
        log(f"{'TOTAL':<40} {total_final:>6}", logf, also_stdout=True)

        # --- 8. Summary ---
        log("\n" + "=" * 80, logf)
        summary_lines = [
            "SUMMARY:",
            f"  Investable in Excel:       {len(investable)}",
            f"  Already existed (skipped):  {skipped_existing}",
            f"  Dup company_id (skipped):   {skipped_dup_company_id}",
            f"  Errors (no marketscrip):    {error_count_no_ms}",
            f"  Total errors in CSV:        {len(errors)}",
            f"  Successfully inserted:      {inserted}",
            f"  Final vs_active_companies:  {total_final}",
            f"  Errors CSV:                 {ERRORS_CSV_PATH}",
            f"  Full log:                   {LOG_PATH}",
        ]
        for line in summary_lines:
            log(line, logf, also_stdout=True)
        log("=" * 80, logf)

        cur.close()
        conn.close()
        log("DONE.", logf, also_stdout=True)


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"FATAL ERROR: {e}")
        traceback.print_exc()
        with open(LOG_PATH, "a") as logf:
            logf.write(f"\nFATAL ERROR: {e}\n")
            logf.write(traceback.format_exc())
        sys.exit(1)
