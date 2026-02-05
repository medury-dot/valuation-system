"""
MySQL Client for Valuation System
Adapted from RAGApp MySQLClient - simplified for valuation-specific operations.
"""

import os
import json
import logging
from datetime import datetime, date
from contextlib import contextmanager
from decimal import Decimal

import mysql.connector
from mysql.connector import pooling
from dotenv import load_dotenv

logger = logging.getLogger(__name__)

# Load environment
load_dotenv(os.path.join(os.path.dirname(__file__), '..', 'config', '.env'))


class ValuationMySQLClient:
    """
    MySQL client for the valuation system.
    - vs_* tables are in the 'rag' database (shared with RAGApp)
    - Company master is mssdb.kbapp_marketscrip (cross-database queries)
    - company_id in vs_* tables = marketscrip_id from mssdb.kbapp_marketscrip
    """

    _instance = None

    def __init__(self):
        self.config = {
            'host': os.getenv('MYSQL_HOST', 'localhost'),
            'port': int(os.getenv('MYSQL_PORT', 3306)),
            'user': os.getenv('MYSQL_USER', 'root'),
            'password': os.getenv('MYSQL_PASSWORD', ''),
            'database': os.getenv('MYSQL_DATABASE', 'rag'),
            'charset': 'utf8mb4',
            'collation': 'utf8mb4_unicode_ci',
            'autocommit': True
        }

        # Connection pool
        self.pool = pooling.MySQLConnectionPool(
            pool_name="valuation_pool",
            pool_size=5,
            **self.config
        )
        logger.info(f"MySQL pool created: {self.config['host']}:{self.config['port']}/{self.config['database']}")

    @classmethod
    def get_instance(cls):
        if cls._instance is None:
            cls._instance = cls()
        return cls._instance

    @contextmanager
    def get_connection(self):
        """Context manager for safe connection handling."""
        conn = self.pool.get_connection()
        try:
            yield conn
        except Exception as e:
            conn.rollback()
            logger.error(f"MySQL error: {e}", exc_info=True)
            raise
        finally:
            conn.close()

    # =========================================================================
    # GENERIC CRUD
    # =========================================================================

    def insert(self, table: str, data: dict) -> int:
        """Insert a row and return the ID."""
        # Convert non-serializable types
        clean_data = {}
        for k, v in data.items():
            if isinstance(v, (dict, list)):
                clean_data[k] = json.dumps(v)
            elif isinstance(v, (datetime, date)):
                clean_data[k] = v.isoformat()
            elif isinstance(v, Decimal):
                clean_data[k] = float(v)
            else:
                clean_data[k] = v

        columns = ', '.join(clean_data.keys())
        placeholders = ', '.join(['%s'] * len(clean_data))
        query = f"INSERT INTO {table} ({columns}) VALUES ({placeholders})"

        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(query, list(clean_data.values()))
            conn.commit()
            insert_id = cursor.lastrowid
            logger.debug(f"Inserted into {table}, id={insert_id}")
            return insert_id

    def insert_batch(self, table: str, rows: list) -> int:
        """Batch insert rows."""
        if not rows:
            return 0

        columns = ', '.join(rows[0].keys())
        placeholders = ', '.join(['%s'] * len(rows[0]))
        query = f"INSERT INTO {table} ({columns}) VALUES ({placeholders})"

        with self.get_connection() as conn:
            cursor = conn.cursor()
            values = [list(row.values()) for row in rows]
            cursor.executemany(query, values)
            conn.commit()
            return cursor.rowcount

    def query(self, sql: str, params: tuple = None) -> list:
        """Execute a query and return results as list of dicts."""
        with self.get_connection() as conn:
            cursor = conn.cursor(dictionary=True)
            cursor.execute(sql, params or ())
            return cursor.fetchall()

    def query_one(self, sql: str, params: tuple = None) -> dict:
        """Execute a query and return single result."""
        results = self.query(sql, params)
        return results[0] if results else None

    def execute(self, sql: str, params: tuple = None) -> int:
        """Execute a statement and return affected rows."""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(sql, params or ())
            conn.commit()
            return cursor.rowcount

    # =========================================================================
    # COMPANY OPERATIONS (from mssdb.kbapp_marketscrip)
    # =========================================================================

    MARKETSCRIP_TABLE = 'mssdb.kbapp_marketscrip'

    # Scrip types that represent equities in mssdb.kbapp_marketscrip:
    #   '' (empty) = legacy equities, 'EQS' = equity shares
    EQUITY_SCRIP_TYPES = "('', 'EQS')"

    _COMPANY_COLS = (
        "marketscrip_id as id, name as company_name, symbol as nse_symbol, "
        "scrip_code as bse_code, sector, industry, accord_code, is_active"
    )

    def get_active_companies(self) -> list:
        """Get equity companies from mssdb.kbapp_marketscrip.
        Note: is_active may be 0 for valid companies in mssdb,
        so we also accept companies listed in companies.yaml by symbol."""
        return self.query(
            f"SELECT {self._COMPANY_COLS} "
            f"FROM {self.MARKETSCRIP_TABLE} "
            f"WHERE scrip_type IN {self.EQUITY_SCRIP_TYPES} "
            f"AND symbol IS NOT NULL AND symbol != ''"
        )

    def get_company_by_symbol(self, nse_symbol: str) -> dict:
        """Get company by NSE symbol from mssdb.kbapp_marketscrip."""
        return self.query_one(
            f"SELECT {self._COMPANY_COLS} "
            f"FROM {self.MARKETSCRIP_TABLE} "
            f"WHERE symbol = %s AND scrip_type IN {self.EQUITY_SCRIP_TYPES} LIMIT 1",
            (nse_symbol,)
        )

    def get_company_by_id(self, company_id: int) -> dict:
        """Get company by marketscrip_id from mssdb.kbapp_marketscrip."""
        return self.query_one(
            f"SELECT {self._COMPANY_COLS} "
            f"FROM {self.MARKETSCRIP_TABLE} "
            f"WHERE marketscrip_id = %s",
            (company_id,)
        )

    def get_company_by_accord_code(self, accord_code: str) -> dict:
        """Get company by accord_code for core CSV linking."""
        return self.query_one(
            f"SELECT {self._COMPANY_COLS} "
            f"FROM {self.MARKETSCRIP_TABLE} "
            f"WHERE accord_code = %s AND scrip_type IN {self.EQUITY_SCRIP_TYPES} LIMIT 1",
            (accord_code,)
        )

    def get_companies_by_symbols(self, symbols: list) -> list:
        """Get multiple companies by their NSE symbols."""
        if not symbols:
            return []
        placeholders = ','.join(['%s'] * len(symbols))
        return self.query(
            f"SELECT {self._COMPANY_COLS} "
            f"FROM {self.MARKETSCRIP_TABLE} "
            f"WHERE symbol IN ({placeholders}) AND scrip_type IN {self.EQUITY_SCRIP_TYPES}",
            tuple(symbols)
        )

    # =========================================================================
    # PEER GROUP OPERATIONS
    # =========================================================================

    def get_company_classification(self, nse_symbol: str) -> dict:
        """Get sector + industry from mssdb for a symbol.
        Returns identity + classification only — no financial metrics from mssdb."""
        return self.query_one(
            f"SELECT marketscrip_id as id, name as company_name, symbol as nse_symbol, "
            f"sector, industry "
            f"FROM {self.MARKETSCRIP_TABLE} "
            f"WHERE symbol = %s AND scrip_type IN {self.EQUITY_SCRIP_TYPES} LIMIT 1",
            (nse_symbol,)
        )

    def get_companies_by_industry(self, industry: str, exclude_symbol: str = None) -> list:
        """Get all equity companies in the same industry from mssdb.
        Returns identity + classification only."""
        if not industry:
            return []
        sql = (
            f"SELECT marketscrip_id as id, name as company_name, symbol as nse_symbol, "
            f"sector, industry "
            f"FROM {self.MARKETSCRIP_TABLE} "
            f"WHERE industry = %s AND scrip_type IN {self.EQUITY_SCRIP_TYPES} "
            f"AND symbol IS NOT NULL AND symbol != ''"
        )
        params = [industry]
        if exclude_symbol:
            sql += " AND symbol != %s"
            params.append(exclude_symbol)
        return self.query(sql, tuple(params))

    def get_companies_by_sector(self, sector: str, exclude_industry: str = None) -> list:
        """Get equity companies in same sector, optionally excluding one industry.
        Returns identity + classification only. MCap filtering done via prices CSV."""
        if not sector:
            return []
        sql = (
            f"SELECT marketscrip_id as id, name as company_name, symbol as nse_symbol, "
            f"sector, industry "
            f"FROM {self.MARKETSCRIP_TABLE} "
            f"WHERE sector = %s AND scrip_type IN {self.EQUITY_SCRIP_TYPES} "
            f"AND symbol IS NOT NULL AND symbol != ''"
        )
        params = [sector]
        if exclude_industry:
            sql += " AND (industry != %s OR industry IS NULL)"
            params.append(exclude_industry)
        return self.query(sql, tuple(params))

    def save_peer_group(self, company_id: int, peers: list) -> int:
        """Store computed peer group. Replaces existing non-PM-override peers.
        peers: list of dicts with keys: peer_company_id, peer_symbol, peer_name,
               tier, similarity_score, mcap_ratio, valid_until"""
        if not peers:
            return 0
        # Delete existing auto-computed peers (preserve PM overrides)
        self.execute(
            "DELETE FROM vs_peer_groups WHERE company_id = %s AND is_pm_override = FALSE",
            (company_id,)
        )
        rows = []
        for p in peers:
            rows.append({
                'company_id': company_id,
                'peer_company_id': p['peer_company_id'],
                'peer_symbol': p['peer_symbol'],
                'peer_name': p.get('peer_name', ''),
                'tier': p['tier'],
                'similarity_score': p.get('similarity_score'),
                'mcap_ratio': p.get('mcap_ratio'),
                'valid_until': p['valid_until'],
                'is_pm_override': False,
            })
        count = self.insert_batch('vs_peer_groups', rows)
        logger.info(f"Saved {count} peers for company_id={company_id}")
        return count

    def get_cached_peer_group(self, company_id: int) -> list:
        """Get cached peers if still valid (valid_until >= today).
        Returns list of peer dicts or empty list if expired/missing."""
        return self.query(
            """SELECT peer_company_id, peer_symbol, peer_name, tier,
                      similarity_score, mcap_ratio, is_pm_override
               FROM vs_peer_groups
               WHERE company_id = %s AND valid_until >= CURDATE()
               ORDER BY tier ASC, similarity_score DESC""",
            (company_id,)
        )

    # =========================================================================
    # VALUATION OPERATIONS
    # =========================================================================

    def get_latest_valuation(self, company_id: int) -> dict:
        """Get most recent valuation for a company."""
        return self.query_one(
            """SELECT * FROM vs_valuations
               WHERE company_id = %s
               ORDER BY valuation_date DESC LIMIT 1""",
            (company_id,)
        )

    def get_valuation_history(self, company_id: int, days: int = 365) -> list:
        """Get valuation history for a company."""
        return self.query(
            """SELECT * FROM vs_valuation_snapshots
               WHERE company_id = %s
                 AND snapshot_date >= DATE_SUB(CURDATE(), INTERVAL %s DAY)
               ORDER BY snapshot_date DESC""",
            (company_id, days)
        )

    def store_valuation_snapshot(self, snapshot: dict) -> int:
        """Store a complete valuation snapshot."""
        return self.insert('vs_valuation_snapshots', snapshot)

    # =========================================================================
    # NEWS & EVENTS
    # =========================================================================

    def get_recent_events(self, company_id: int = None, days: int = 30,
                          min_severity: str = None) -> list:
        """Get recent events, optionally filtered by company and severity."""
        conditions = ["event_date >= DATE_SUB(NOW(), INTERVAL %s DAY)"]
        params = [days]

        if company_id:
            conditions.append("(company_id = %s OR scope = 'MACRO')")
            params.append(company_id)

        if min_severity:
            severity_order = {'CRITICAL': 1, 'HIGH': 2, 'MEDIUM': 3, 'LOW': 4}
            min_level = severity_order.get(min_severity, 4)
            valid_severities = [s for s, v in severity_order.items() if v <= min_level]
            placeholders = ','.join(['%s'] * len(valid_severities))
            conditions.append(f"severity IN ({placeholders})")
            params.extend(valid_severities)

        where = ' AND '.join(conditions)
        return self.query(
            f"""SELECT * FROM vs_event_timeline
                WHERE {where}
                ORDER BY event_date DESC""",
            tuple(params)
        )

    def get_events_by_severity(self, hours: int = 24, min_severity: str = 'MEDIUM') -> list:
        """Get events from last N hours above severity threshold."""
        severity_order = {'CRITICAL': 1, 'HIGH': 2, 'MEDIUM': 3, 'LOW': 4}
        min_level = severity_order.get(min_severity, 4)
        valid_severities = [s for s, v in severity_order.items() if v <= min_level]
        placeholders = ','.join(['%s'] * len(valid_severities))

        return self.query(
            f"""SELECT * FROM vs_event_timeline
                WHERE event_timestamp >= DATE_SUB(NOW(), INTERVAL %s HOUR)
                  AND severity IN ({placeholders})
                ORDER BY event_timestamp DESC""",
            tuple([hours] + valid_severities)
        )

    # =========================================================================
    # DRIVER OPERATIONS
    # =========================================================================

    def get_driver_changes(self, hours: int = 24) -> list:
        """Get driver changes from last N hours."""
        return self.query(
            """SELECT * FROM vs_driver_changelog
               WHERE change_timestamp >= DATE_SUB(NOW(), INTERVAL %s HOUR)
               ORDER BY change_timestamp DESC""",
            (hours,)
        )

    def log_driver_change(self, change: dict) -> int:
        """Log a driver value or weight change."""
        return self.insert('vs_driver_changelog', change)

    # =========================================================================
    # ALERTS
    # =========================================================================

    def log_alert(self, company_id: int, alert_type: str,
                  change_pct: float, reason: str = None) -> int:
        """Log an alert."""
        return self.insert('vs_alerts', {
            'company_id': company_id,
            'alert_type': alert_type,
            'change_pct': change_pct,
            'trigger_reason': reason
        })

    def get_recent_alerts(self, hours: int = 24) -> list:
        """Get recent alerts with company info from mssdb."""
        return self.query(
            f"""SELECT a.*, c.name as company_name, c.symbol as nse_symbol
               FROM vs_alerts a
               JOIN {self.MARKETSCRIP_TABLE} c ON a.company_id = c.marketscrip_id
               WHERE a.created_at >= DATE_SUB(NOW(), INTERVAL %s HOUR)
               ORDER BY a.created_at DESC""",
            (hours,)
        )

    # =========================================================================
    # MODEL HISTORY
    # =========================================================================

    def log_model_change(self, company_id: int, change_type: str,
                         field: str, old_val, new_val,
                         changed_by: str, reason: str) -> int:
        """Log a model parameter change."""
        return self.insert('vs_model_history', {
            'company_id': company_id,
            'change_date': datetime.now(),
            'change_type': change_type,
            'field_changed': field,
            'old_value': str(old_val),
            'new_value': str(new_val),
            'changed_by': changed_by,
            'reason': reason
        })

    # =========================================================================
    # PM FEEDBACK
    # =========================================================================

    def log_pm_interaction(self, interaction: dict) -> int:
        """Log a PM interaction (override, feedback, etc.)."""
        return self.insert('vs_pm_interactions', interaction)


def get_mysql_client() -> ValuationMySQLClient:
    """Get singleton MySQL client."""
    return ValuationMySQLClient.get_instance()
