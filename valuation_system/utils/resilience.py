"""
Resilience Utilities
Handles all edge cases: network failures, service unavailability,
gap/catchup days after machine shutdown, graceful degradation.
"""

import os
import json
import time
import logging
import traceback
import functools
from datetime import datetime, date, timedelta
from pathlib import Path
from typing import Callable, Optional

from dotenv import load_dotenv

logger = logging.getLogger(__name__)

load_dotenv(os.path.join(os.path.dirname(__file__), '..', 'config', '.env'))


class RunStateManager:
    """
    Track execution state to handle:
    - Machine was off for N days → catchup runs
    - Last successful run time → avoid duplicate work
    - Failed runs → retry with backoff
    - On-demand runs → skip scheduling conflicts

    State persisted to disk so it survives restarts.
    """

    def __init__(self, state_dir: str = None):
        self.state_dir = state_dir or os.path.join(
            os.path.dirname(__file__), '..', 'data', 'state'
        )
        os.makedirs(self.state_dir, exist_ok=True)
        self._state_file = os.path.join(self.state_dir, 'run_state.json')
        self._state = self._load_state()

    def _load_state(self) -> dict:
        if os.path.exists(self._state_file):
            try:
                with open(self._state_file, 'r') as f:
                    return json.load(f)
            except Exception as e:
                logger.error(f"Failed to load run state: {e}", exc_info=True)
        return {}

    def _save_state(self):
        try:
            with open(self._state_file, 'w') as f:
                json.dump(self._state, f, indent=2, default=str)
        except Exception as e:
            logger.error(f"Failed to save run state: {e}", exc_info=True)

    def get_last_run(self, task_name: str) -> Optional[datetime]:
        """Get timestamp of last successful run for a task."""
        entry = self._state.get(task_name, {})
        last_success = entry.get('last_success')
        if last_success:
            try:
                return datetime.fromisoformat(last_success)
            except (ValueError, TypeError):
                return None
        return None

    def get_missed_days(self, task_name: str, expected_frequency_hours: int = 24) -> list:
        """
        Calculate missed execution dates since last successful run.
        Returns list of dates that need catchup.
        """
        last_run = self.get_last_run(task_name)
        if not last_run:
            # Never run before - just return today
            return [date.today()]

        missed = []
        check_date = last_run.date() + timedelta(days=1)
        today = date.today()

        while check_date <= today:
            # Skip weekends for market-related tasks
            if check_date.weekday() < 5:  # Monday=0, Friday=4
                missed.append(check_date)
            check_date += timedelta(days=1)

        if missed:
            logger.info(f"Task '{task_name}': {len(missed)} missed days "
                        f"since last run on {last_run.date()}")

        return missed

    def record_success(self, task_name: str, details: dict = None):
        """Record a successful task run."""
        if task_name not in self._state:
            self._state[task_name] = {}

        self._state[task_name].update({
            'last_success': datetime.now().isoformat(),
            'last_status': 'SUCCESS',
            'consecutive_failures': 0,
            'last_details': details or {},
        })
        self._save_state()

    def record_failure(self, task_name: str, error: str):
        """Record a failed task run."""
        if task_name not in self._state:
            self._state[task_name] = {}

        failures = self._state[task_name].get('consecutive_failures', 0) + 1
        self._state[task_name].update({
            'last_failure': datetime.now().isoformat(),
            'last_status': 'FAILED',
            'last_error': error,
            'consecutive_failures': failures,
        })
        self._save_state()

    def should_retry(self, task_name: str, max_retries: int = 3) -> bool:
        """Check if task should be retried based on failure count."""
        entry = self._state.get(task_name, {})
        failures = entry.get('consecutive_failures', 0)
        return failures < max_retries

    def get_retry_delay_seconds(self, task_name: str) -> int:
        """Exponential backoff: 60s, 300s, 900s, ..."""
        entry = self._state.get(task_name, {})
        failures = entry.get('consecutive_failures', 0)
        return min(60 * (3 ** failures), 3600)  # Max 1 hour

    def is_running(self, task_name: str) -> bool:
        """Check if task is currently running (prevent overlap)."""
        entry = self._state.get(task_name, {})
        if entry.get('last_status') == 'RUNNING':
            # Check if it's been running too long (stale lock)
            started_at = entry.get('started_at')
            if started_at:
                try:
                    started = datetime.fromisoformat(started_at)
                    if (datetime.now() - started).total_seconds() > 3600:
                        logger.warning(f"Task '{task_name}' appears stale (>1hr), clearing lock")
                        return False
                except (ValueError, TypeError):
                    pass
            return True
        return False

    def mark_running(self, task_name: str):
        """Mark task as currently running."""
        if task_name not in self._state:
            self._state[task_name] = {}
        self._state[task_name].update({
            'last_status': 'RUNNING',
            'started_at': datetime.now().isoformat(),
        })
        self._save_state()

    def get_full_status(self) -> dict:
        """Get status of all tasks for monitoring."""
        return {k: {
            'status': v.get('last_status', 'NEVER_RUN'),
            'last_success': v.get('last_success'),
            'last_failure': v.get('last_failure'),
            'failures': v.get('consecutive_failures', 0),
        } for k, v in self._state.items()}


def retry_with_backoff(max_retries: int = 3, base_delay: float = 1.0,
                       exceptions: tuple = (Exception,)):
    """
    Decorator for retrying functions with exponential backoff.
    Handles transient network errors, API rate limits, etc.
    """
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            last_exception = None
            for attempt in range(max_retries + 1):
                try:
                    return func(*args, **kwargs)
                except exceptions as e:
                    last_exception = e
                    if attempt < max_retries:
                        delay = base_delay * (2 ** attempt)
                        logger.warning(
                            f"{func.__name__} failed (attempt {attempt + 1}/{max_retries + 1}): "
                            f"{e}. Retrying in {delay:.1f}s..."
                        )
                        time.sleep(delay)
                    else:
                        logger.error(
                            f"{func.__name__} failed after {max_retries + 1} attempts: {e}",
                            exc_info=True
                        )
            raise last_exception
        return wrapper
    return decorator


def check_internet(timeout: int = 5) -> bool:
    """Quick check if internet is available."""
    import socket
    try:
        socket.create_connection(("8.8.8.8", 53), timeout=timeout)
        return True
    except OSError:
        return False


def check_service(host: str, port: int, timeout: int = 3) -> bool:
    """Check if a service (MySQL, ChromaDB) is reachable."""
    import socket
    try:
        socket.create_connection((host, port), timeout=timeout)
        return True
    except OSError:
        return False


def check_dependencies() -> dict:
    """
    Check all system dependencies and return status report.
    Call this before any major operation.
    """
    results = {}

    # Internet
    results['internet'] = check_internet()

    # MySQL
    mysql_host = os.getenv('MYSQL_HOST', 'localhost')
    mysql_port = int(os.getenv('MYSQL_PORT', 3306))
    results['mysql'] = check_service(mysql_host, mysql_port)

    # ChromaDB
    chromadb_host = os.getenv('CHROMADB_HOST', 'localhost')
    chromadb_port = int(os.getenv('CHROMADB_PORT', 8001))
    results['chromadb'] = check_service(chromadb_host, chromadb_port)

    # Data files — support both explicit path and auto-detect from directory
    core_csv = os.getenv('CORE_CSV_PATH', '').strip()
    core_csv_dir = os.getenv('CORE_CSV_DIR', '').strip()
    if core_csv and os.path.exists(core_csv):
        results['core_csv'] = True
    elif core_csv_dir and os.path.isdir(core_csv_dir):
        import glob
        results['core_csv'] = bool(glob.glob(os.path.join(core_csv_dir, 'core-all-input-*-latest-final.csv')))
    else:
        results['core_csv'] = False

    prices_csv = os.getenv('MONTHLY_PRICES_PATH', '')
    results['prices_csv'] = os.path.exists(prices_csv)

    if not all(results.values()):
        failed = [k for k, v in results.items() if not v]
        logger.warning(f"Dependency check: FAILED components: {failed}")
    else:
        logger.info("All dependencies available")

    return results


def safe_task_run(task_name: str, state_manager: RunStateManager):
    """
    Decorator that wraps task execution with:
    - Overlap prevention (don't run if already running)
    - State tracking (success/failure/timing)
    - Error capture with full traceback
    - Graceful failure (logs and continues)
    """
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            if state_manager.is_running(task_name):
                logger.info(f"Task '{task_name}' already running, skipping")
                return {'status': 'SKIPPED', 'reason': 'already_running'}

            state_manager.mark_running(task_name)
            start_time = datetime.now()

            try:
                result = func(*args, **kwargs)
                elapsed = (datetime.now() - start_time).total_seconds()
                state_manager.record_success(task_name, {
                    'elapsed_seconds': round(elapsed, 1),
                    'result_summary': str(result)[:200] if result else None,
                })
                logger.info(f"Task '{task_name}' completed in {elapsed:.1f}s")
                return result

            except Exception as e:
                elapsed = (datetime.now() - start_time).total_seconds()
                error_msg = f"{type(e).__name__}: {e}\n{traceback.format_exc()}"
                state_manager.record_failure(task_name, error_msg)
                logger.error(f"Task '{task_name}' failed after {elapsed:.1f}s: {e}",
                             exc_info=True)
                return {'status': 'FAILED', 'error': str(e)}

        return wrapper
    return decorator


class GracefulDegradation:
    """
    When critical services are down, the system degrades gracefully:
    - No internet → Use cached data, skip news scan, log for catchup
    - MySQL down → Queue operations to disk, replay when available
    - ChromaDB down → Skip vector operations, use metadata only
    - Price file stale → Use last available, flag staleness
    """

    def __init__(self, state_dir: str = None):
        self.state_dir = state_dir or os.path.join(
            os.path.dirname(__file__), '..', 'data', 'state'
        )
        os.makedirs(self.state_dir, exist_ok=True)
        self._queue_file = os.path.join(self.state_dir, 'pending_operations.json')

    def queue_operation(self, operation: dict):
        """Queue a failed operation for later replay."""
        queue = self._load_queue()
        operation['queued_at'] = datetime.now().isoformat()
        queue.append(operation)
        self._save_queue(queue)
        logger.info(f"Queued operation: {operation.get('type', 'unknown')}")

    def replay_queued_operations(self, handler_fn: Callable) -> int:
        """Replay queued operations through the handler function."""
        queue = self._load_queue()
        if not queue:
            return 0

        logger.info(f"Replaying {len(queue)} queued operations")
        succeeded = 0
        remaining = []

        for op in queue:
            try:
                handler_fn(op)
                succeeded += 1
            except Exception as e:
                logger.warning(f"Replay failed for operation: {e}")
                remaining.append(op)

        self._save_queue(remaining)
        logger.info(f"Replayed {succeeded}/{len(queue)} operations, "
                     f"{len(remaining)} remaining")
        return succeeded

    def get_queue_size(self) -> int:
        return len(self._load_queue())

    def _load_queue(self) -> list:
        if os.path.exists(self._queue_file):
            try:
                with open(self._queue_file, 'r') as f:
                    return json.load(f)
            except Exception:
                return []
        return []

    def _save_queue(self, queue: list):
        try:
            with open(self._queue_file, 'w') as f:
                json.dump(queue, f, indent=2, default=str)
        except Exception as e:
            logger.error(f"Failed to save operation queue: {e}", exc_info=True)

    def check_data_staleness(self) -> dict:
        """Check if data files are stale and report staleness."""
        staleness = {}

        prices_path = os.getenv('MONTHLY_PRICES_PATH', '')
        if os.path.exists(prices_path):
            mtime = datetime.fromtimestamp(os.path.getmtime(prices_path))
            age_hours = (datetime.now() - mtime).total_seconds() / 3600
            staleness['prices_file'] = {
                'last_modified': mtime.isoformat(),
                'age_hours': round(age_hours, 1),
                'is_stale': age_hours > 48,  # Stale if >2 days old
            }

        core_path = os.getenv('CORE_CSV_PATH', '').strip()
        if not core_path or not os.path.exists(core_path):
            # Try auto-detect from CORE_CSV_DIR
            from valuation_system.data.loaders.core_loader import CoreDataLoader
            core_path = CoreDataLoader._resolve_csv_path() or ''
        if core_path and os.path.exists(core_path):
            mtime = datetime.fromtimestamp(os.path.getmtime(core_path))
            age_days = (datetime.now() - mtime).total_seconds() / 86400
            staleness['core_csv'] = {
                'last_modified': mtime.isoformat(),
                'age_days': round(age_days, 1),
                'is_stale': age_days > 30,  # Stale if >30 days old
            }

        return staleness
