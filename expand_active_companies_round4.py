#!/usr/bin/env python3
"""
Round 4: Insert ~452 missing companies (from reclassification) into
mssdb.kbapp_marketscrip and rag.vs_active_companies.

These are newly-investable companies from the NOT_CLASSIFIED reclassification
that have no matching marketscrip record (newer IPOs like ETERNAL, PVRINOX,
MEESHO, LENSKART, PhysicsWallah, etc.).

Steps:
1. Load reclassify_missing_round4.csv (companies without marketscrip match)
2. Load Excel for full data (match by Accord Code)
3. Insert into mssdb.kbapp_marketscrip with new marketscrip_ids
4. Insert into rag.vs_active_companies using those new IDs
5. Commit in batches of 100

Usage:
  python3 expand_active_companies_round4.py
"""

import csv
import os
import sys
import traceback
from datetime import datetime

import mysql.connector
import pandas as pd

# Paths
ERRORS_CSV = "/Users/ram/code/research/reclassify_missing_round4.csv"
EXCEL_FILE = "/Users/ram/code/research/valuation_group-valuation_subgroup-feb2026.xlsx"
LOG_FILE = "/Users/ram/code/research/expand_active_companies_round4.log"
ERRORS_OUT = "/Users/ram/code/research/expand_active_errors_round4.csv"

BATCH_SIZE = 100


def log(msg, log_fh):
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    line = f"[{ts}] {msg}"
    print(line)
    log_fh.write(line + "\n")
    log_fh.flush()


def clean_str(val):
    """Return None if val is NaN/empty/None, else stripped string."""
    if val is None:
        return None
    s = str(val).strip()
    if s.lower() == "nan" or s == "":
        return None
    return s


def main():
    log_fh = open(LOG_FILE, "w", encoding="utf-8")
    errors = []

    try:
        # 1. Load input CSV
        log("Loading missing companies CSV...", log_fh)
        input_df = pd.read_csv(ERRORS_CSV, dtype=str)
        log(f"  Loaded {len(input_df)} rows", log_fh)
        log(f"  Columns: {list(input_df.columns)}", log_fh)

        # 2. Load Excel for full data
        log("Loading Excel file...", log_fh)
        excel_df = pd.read_excel(EXCEL_FILE, dtype=str)
        log(f"  Loaded {len(excel_df)} rows from Excel", log_fh)

        # Build lookup by Accord Code
        excel_lookup = {}
        for _, row in excel_df.iterrows():
            ac = clean_str(row.get("Accord Code"))
            if ac:
                # Normalize: strip .0 suffix from float-converted strings
                if ac.endswith(".0"):
                    ac = ac[:-2]
                excel_lookup[ac] = row
        log(f"  Excel lookup built: {len(excel_lookup)} entries", log_fh)

        # 3. Connect to MySQL
        log("Connecting to MySQL...", log_fh)
        conn = mysql.connector.connect(
            host="localhost",
            port=3306,
            user="root",
            password="",
            charset="utf8mb4",
            autocommit=False,
        )
        cursor = conn.cursor(dictionary=True)
        log("  Connected to MySQL", log_fh)

        # 4. Find max marketscrip_id
        cursor.execute("SELECT MAX(marketscrip_id) as max_id FROM mssdb.kbapp_marketscrip")
        result = cursor.fetchone()
        original_max_id = result["max_id"]
        next_id = original_max_id + 1
        log(f"  Current max marketscrip_id: {original_max_id}", log_fh)
        log(f"  New IDs will start from: {next_id}", log_fh)

        # 5. Check for existing accord_codes in marketscrip
        log("Checking for existing accord_codes in marketscrip...", log_fh)
        cursor.execute(
            "SELECT accord_code, marketscrip_id FROM mssdb.kbapp_marketscrip "
            "WHERE accord_code IS NOT NULL AND scrip_type IN ('', 'EQS')"
        )
        existing_ms_map = {}
        for row in cursor.fetchall():
            ac = clean_str(row["accord_code"])
            if ac:
                if ac.endswith(".0"):
                    ac = ac[:-2]
                existing_ms_map[ac] = row["marketscrip_id"]
        log(f"  Existing equity accord_codes in marketscrip: {len(existing_ms_map)}", log_fh)

        # Check vs_active_companies
        cursor.execute(
            "SELECT accord_code FROM rag.vs_active_companies WHERE accord_code IS NOT NULL"
        )
        existing_active_accord = set()
        for row in cursor.fetchall():
            ac = clean_str(row["accord_code"])
            if ac:
                existing_active_accord.add(ac)
        log(f"  Existing accord_codes in vs_active_companies: {len(existing_active_accord)}", log_fh)

        # Also check existing company_ids to avoid duplicates
        cursor.execute("SELECT company_id FROM rag.vs_active_companies")
        existing_company_ids = set()
        for row in cursor.fetchall():
            if row["company_id"]:
                existing_company_ids.add(row["company_id"])

        # 6. Prepare insertions
        now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S.000000")

        marketscrip_insert_sql = """
            INSERT INTO mssdb.kbapp_marketscrip (
                marketscrip_id, name, scrip_type, created, modified,
                symbol, scrip_code, accord_code,
                is_in_NRI_ban_list, is_in_NRI_breach_list
            ) VALUES (
                %s, %s, %s, %s, %s,
                %s, %s, %s,
                %s, %s
            )
        """

        active_insert_sql = """
            INSERT INTO rag.vs_active_companies (
                company_id, company_name, nse_symbol, bse_code, accord_code,
                sector, industry,
                valuation_group, valuation_subgroup,
                cd_sector, cd_industry, csv_name,
                valuation_frequency, priority, is_active,
                added_date, added_by
            ) VALUES (
                %s, %s, %s, %s, %s,
                %s, %s,
                %s, %s,
                %s, %s, %s,
                %s, %s, %s,
                CURDATE(), %s
            )
        """

        inserted_marketscrip = 0
        inserted_active = 0
        skipped_existing_ms = 0
        skipped_existing_active = 0
        skipped_no_data = 0
        batch_ms = []
        batch_active = []
        batch_count = 0

        for idx, row in input_df.iterrows():
            accord_code = clean_str(row["accord_code"])
            if not accord_code:
                errors.append({
                    "accord_code": "",
                    "company_name": "",
                    "error_reason": f"Row {idx}: missing accord_code",
                })
                continue

            # Get data - prefer Excel, fallback to input CSV columns
            excel_row = excel_lookup.get(accord_code)
            if excel_row is not None:
                company_name = clean_str(excel_row.get("Company Name"))
                nse_symbol = clean_str(excel_row.get("CD_NSE Symbol1"))
                bse_code = clean_str(excel_row.get("CD_Bse Scrip ID"))
                cd_sector = clean_str(excel_row.get("CD_Sector"))
                cd_industry = clean_str(excel_row.get("CD_Industry1"))
                val_group = clean_str(excel_row.get("valuation_group"))
                val_subgroup = clean_str(excel_row.get("valuation_subgroup"))
            else:
                company_name = clean_str(row.get("company_name"))
                nse_symbol = clean_str(row.get("nse_symbol"))
                bse_code = clean_str(row.get("bse_code"))
                cd_sector = clean_str(row.get("cd_sector"))
                cd_industry = clean_str(row.get("cd_industry"))
                val_group = clean_str(row.get("valuation_group"))
                val_subgroup = clean_str(row.get("valuation_subgroup"))
                log(f"  WARN {accord_code}: using input CSV data (no Excel match) - {company_name}", log_fh)

            if not company_name:
                errors.append({
                    "accord_code": accord_code,
                    "company_name": "",
                    "error_reason": "No company name found in Excel or input CSV",
                })
                skipped_no_data += 1
                log(f"  SKIP {accord_code}: no company name", log_fh)
                continue

            # Determine marketscrip_id
            if accord_code in existing_ms_map:
                new_id = existing_ms_map[accord_code]
                skipped_existing_ms += 1
                log(f"  EXISTING marketscrip {accord_code} -> ID {new_id} ({company_name})", log_fh)
            else:
                new_id = next_id
                next_id += 1
                batch_ms.append((
                    new_id, company_name, "EQS", now_str, now_str,
                    nse_symbol, bse_code, accord_code,
                    0, 0
                ))

            # Check for dup company_id
            if new_id in existing_company_ids:
                skipped_existing_active += 1
                log(f"  SKIP vs_active: company_id={new_id} already exists ({company_name})", log_fh)
                continue

            # Prepare vs_active insert
            if accord_code in existing_active_accord:
                skipped_existing_active += 1
                log(f"  SKIP vs_active {accord_code}: already exists ({company_name})", log_fh)
            else:
                batch_active.append((
                    new_id, company_name, nse_symbol, bse_code, accord_code,
                    val_group, val_subgroup,
                    val_group, val_subgroup,
                    cd_sector, cd_industry, company_name,
                    "WEEKLY", 5, 1,
                    "reclassify_round4_feb2026"
                ))
                existing_company_ids.add(new_id)
                existing_active_accord.add(accord_code)

            # Flush batches every BATCH_SIZE
            if len(batch_ms) >= BATCH_SIZE or len(batch_active) >= BATCH_SIZE:
                if batch_ms:
                    try:
                        cursor.executemany(marketscrip_insert_sql, batch_ms)
                        inserted_marketscrip += len(batch_ms)
                        log(f"  Inserted {len(batch_ms)} into marketscrip (total: {inserted_marketscrip})", log_fh)
                    except Exception as e:
                        log(f"  ERROR batch marketscrip insert: {e}", log_fh)
                        log(f"  TRACEBACK: {traceback.format_exc()}", log_fh)
                        for item in batch_ms:
                            errors.append({
                                "accord_code": item[7],
                                "company_name": item[1],
                                "error_reason": f"marketscrip insert failed: {e}",
                            })
                    batch_ms = []

                if batch_active:
                    try:
                        cursor.executemany(active_insert_sql, batch_active)
                        inserted_active += len(batch_active)
                        log(f"  Inserted {len(batch_active)} into vs_active_companies (total: {inserted_active})", log_fh)
                    except Exception as e:
                        log(f"  ERROR batch vs_active insert: {e}", log_fh)
                        log(f"  TRACEBACK: {traceback.format_exc()}", log_fh)
                        for item in batch_active:
                            errors.append({
                                "accord_code": item[4],
                                "company_name": item[1],
                                "error_reason": f"vs_active insert failed: {e}",
                            })
                    batch_active = []

                conn.commit()
                batch_count += 1
                log(f"  COMMITTED batch #{batch_count} (ms={inserted_marketscrip}, active={inserted_active})", log_fh)

        # Flush remaining
        if batch_ms:
            try:
                cursor.executemany(marketscrip_insert_sql, batch_ms)
                inserted_marketscrip += len(batch_ms)
                log(f"  Inserted final {len(batch_ms)} into marketscrip (total: {inserted_marketscrip})", log_fh)
            except Exception as e:
                log(f"  ERROR final marketscrip insert: {e}", log_fh)
                log(f"  TRACEBACK: {traceback.format_exc()}", log_fh)
                for item in batch_ms:
                    errors.append({
                        "accord_code": item[7],
                        "company_name": item[1],
                        "error_reason": f"marketscrip insert failed: {e}",
                    })

        if batch_active:
            try:
                cursor.executemany(active_insert_sql, batch_active)
                inserted_active += len(batch_active)
                log(f"  Inserted final {len(batch_active)} into vs_active_companies (total: {inserted_active})", log_fh)
            except Exception as e:
                log(f"  ERROR final vs_active insert: {e}", log_fh)
                log(f"  TRACEBACK: {traceback.format_exc()}", log_fh)
                for item in batch_active:
                    errors.append({
                        "accord_code": item[4],
                        "company_name": item[1],
                        "error_reason": f"vs_active insert failed: {e}",
                    })

        conn.commit()
        log("  FINAL COMMIT done", log_fh)

        # 7. Verification
        log("\n=== VERIFICATION ===", log_fh)

        cursor.execute("SELECT COUNT(*) as cnt FROM rag.vs_active_companies")
        total_active = cursor.fetchone()["cnt"]
        log(f"Total vs_active_companies: {total_active}", log_fh)

        cursor.execute(
            "SELECT valuation_group, COUNT(*) as cnt FROM rag.vs_active_companies "
            "GROUP BY valuation_group ORDER BY cnt DESC"
        )
        log("\nvaluation_group distribution:", log_fh)
        for row in cursor.fetchall():
            log(f"  {row['valuation_group']:<45} {row['cnt']:>5}", log_fh)

        cursor.execute(
            f"SELECT COUNT(*) as cnt FROM mssdb.kbapp_marketscrip WHERE marketscrip_id > {original_max_id}"
        )
        new_ms = cursor.fetchone()["cnt"]
        log(f"\nNew marketscrip entries (id > {original_max_id}): {new_ms}", log_fh)

        cursor.execute(
            "SELECT COUNT(*) as cnt FROM rag.vs_active_companies WHERE added_by = 'reclassify_round4_feb2026'"
        )
        round4_active = cursor.fetchone()["cnt"]
        log(f"Round 4 vs_active entries: {round4_active}", log_fh)

        # Show new subgroups
        log("\nNew subgroups added in Round 4:", log_fh)
        cursor.execute(
            "SELECT valuation_subgroup, COUNT(*) as cnt FROM rag.vs_active_companies "
            "WHERE added_by = 'reclassify_round4_feb2026' "
            "GROUP BY valuation_subgroup ORDER BY cnt DESC"
        )
        for row in cursor.fetchall():
            log(f"  {row['valuation_subgroup']:<45} {row['cnt']:>5}", log_fh)

        # Summary
        log("\n=== SUMMARY ===", log_fh)
        log(f"Input rows: {len(input_df)}", log_fh)
        log(f"Inserted into marketscrip: {inserted_marketscrip}", log_fh)
        log(f"Skipped (already in marketscrip): {skipped_existing_ms}", log_fh)
        log(f"Inserted into vs_active_companies: {inserted_active}", log_fh)
        log(f"Skipped (already in vs_active): {skipped_existing_active}", log_fh)
        log(f"Skipped (no data): {skipped_no_data}", log_fh)
        log(f"Errors: {len(errors)}", log_fh)

        cursor.close()
        conn.close()

    except Exception as e:
        log(f"FATAL ERROR: {e}", log_fh)
        log(f"TRACEBACK: {traceback.format_exc()}", log_fh)
        raise
    finally:
        # Write errors CSV
        with open(ERRORS_OUT, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=["accord_code", "company_name", "error_reason"])
            writer.writeheader()
            if errors:
                writer.writerows(errors)
        log(f"Errors CSV: {ERRORS_OUT} ({len(errors)} rows)", log_fh)
        log_fh.close()
        print(f"\nLog: {LOG_FILE}")
        print(f"Errors: {ERRORS_OUT}")


if __name__ == "__main__":
    main()
