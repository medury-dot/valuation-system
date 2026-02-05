"""
Scheduler Runner
Entry points for all scheduled and on-demand operations.
Designed for launchd (macOS) and manual execution.

Usage:
  # Hourly news scan + driver update
  python -m valuation_system.scheduler.runner hourly

  # Daily valuation refresh (run after market close ~20:00 IST)
  python -m valuation_system.scheduler.runner daily

  # Daily social media posts (run pre-market ~08:00 IST)
  python -m valuation_system.scheduler.runner social

  # Weekly summary (run Sunday ~10:00 IST)
  python -m valuation_system.scheduler.runner weekly

  # On-demand single company valuation
  python -m valuation_system.scheduler.runner valuation --symbol AETHER

  # On-demand portfolio valuation
  python -m valuation_system.scheduler.runner portfolio

  # Catchup after machine was off
  python -m valuation_system.scheduler.runner catchup

  # System status check
  python -m valuation_system.scheduler.runner status

  # Run regression tests
  python -m valuation_system.scheduler.runner test

  # Initialize system (create DB tables, seed data, create GSheets)
  python -m valuation_system.scheduler.runner init

Edge Cases:
- All runs log to LOG_DIR with rotation
- Overlap prevention via RunStateManager locks
- Graceful degradation for each dependency
- On-demand runs bypass scheduling conflicts
"""

import os
import sys
import argparse
import logging
from datetime import datetime

from dotenv import load_dotenv

# Add project root to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))

load_dotenv(os.path.join(os.path.dirname(__file__), '..', 'config', '.env'))

# Configure logging
LOG_DIR = os.getenv('LOG_DIR', os.path.join(os.path.dirname(__file__), '..', 'logs'))
os.makedirs(LOG_DIR, exist_ok=True)

logging.basicConfig(
    level=os.getenv('LOG_LEVEL', 'DEBUG'),
    format='%(asctime)s [%(levelname)s] %(name)s: %(message)s',
    handlers=[
        logging.FileHandler(
            os.path.join(LOG_DIR, f"{datetime.now().strftime('%Y-%m-%d')}.log")
        ),
        logging.StreamHandler(sys.stdout),
    ]
)
logger = logging.getLogger('valuation_system')


def run_hourly():
    """Hourly cycle: news scan → classify → update drivers."""
    from valuation_system.agents.orchestrator import OrchestratorAgent
    orch = OrchestratorAgent()
    result = orch.run_hourly_cycle()
    logger.info(f"Hourly cycle result: {result}")
    return result


def run_daily():
    """Daily cycle: full valuation refresh → alerts → digest."""
    from valuation_system.agents.orchestrator import OrchestratorAgent
    from valuation_system.notifications.email_sender import EmailSender

    orch = OrchestratorAgent()

    # Run hourly first (catchup if needed)
    hourly_result = orch.run_hourly_cycle()

    # Then daily valuation
    daily_result = orch.run_daily_valuation()

    # Send daily digest email
    try:
        email = EmailSender()
        digest = {
            'valuations': daily_result.get('valuations', {}),
            'alerts': daily_result.get('alerts', 0),
            'events_processed': hourly_result.get('significant_events', 0),
            'driver_changes': sum(hourly_result.get('driver_changes', {}).values()),
        }
        email.send_daily_digest(digest)
    except Exception as e:
        logger.error(f"Failed to send daily digest: {e}", exc_info=True)

    return daily_result


def run_social():
    """Generate and queue social media posts in GSheet for PM approval."""
    from valuation_system.agents.content_agent import ContentAgent
    from valuation_system.storage.mysql_client import get_mysql_client
    from valuation_system.storage.gsheet_client import GSheetClient

    try:
        mysql = get_mysql_client()
        gsheet = GSheetClient()
        agent = ContentAgent(mysql, gsheet_client=gsheet)
        posts = agent.generate_daily_posts()
        results = agent.publish_posts(posts)
        logger.info(f"Social posts: {len(results)} queued to GSheet for approval")
        return results
    except Exception as e:
        logger.error(f"Social posting failed: {e}", exc_info=True)
        return {'error': str(e)}


def run_weekly():
    """Weekly summary report."""
    from valuation_system.agents.orchestrator import OrchestratorAgent
    from valuation_system.notifications.email_sender import EmailSender

    orch = OrchestratorAgent()

    # Run daily first
    daily_result = run_daily()

    # Generate weekly summary
    summary = {
        'period': f"Week ending {datetime.now().strftime('%Y-%m-%d')}",
        'highlights': [
            f"Processed daily valuations for {len(daily_result.get('valuations', {}))} companies",
            f"Generated {daily_result.get('alerts', 0)} alerts this week",
        ],
    }

    try:
        email = EmailSender()
        email.send_weekly_summary(summary)
    except Exception as e:
        logger.error(f"Failed to send weekly summary: {e}", exc_info=True)

    return summary


def run_valuation(symbol: str = None, company_key: str = None):
    """On-demand single company valuation."""
    from valuation_system.agents.orchestrator import OrchestratorAgent

    orch = OrchestratorAgent()
    result = orch.run_on_demand(company_key=company_key, nse_symbol=symbol)
    logger.info(f"On-demand valuation: {result.get('company_name', symbol)}")

    # Print summary to stdout
    if 'error' not in result:
        print(f"\n{'='*60}")
        print(f"  {result.get('company_name', '')} - Valuation Summary")
        print(f"{'='*60}")
        print(f"  CMP:             Rs {result.get('cmp', 0):>12,.2f}")
        print(f"  Intrinsic Value: Rs {result.get('intrinsic_value_blended', 0):>12,.2f}")
        print(f"  Upside:          {result.get('upside_pct', 0):>+12.1f}%")
        print(f"  Confidence:      {result.get('confidence_score', 0):>12.2f}")
        print(f"  DCF Bull/Base/Bear: Rs {result.get('dcf_bull', 0):,.0f} / "
              f"Rs {result.get('dcf_base', 0):,.0f} / Rs {result.get('dcf_bear', 0):,.0f}")
        print(f"{'='*60}")
    else:
        print(f"ERROR: {result.get('error')}")

    return result


def run_portfolio():
    """On-demand full portfolio valuation."""
    from valuation_system.agents.orchestrator import OrchestratorAgent
    orch = OrchestratorAgent()
    return orch.run_portfolio_valuation()


def run_catchup():
    """Run catchup for all missed operations."""
    from valuation_system.agents.orchestrator import OrchestratorAgent
    orch = OrchestratorAgent()

    logger.info("Running system catchup...")

    # Hourly catchup (news)
    hourly = orch.run_hourly_cycle()

    # Daily catchup (valuations)
    daily = orch.run_daily_valuation()

    logger.info(f"Catchup complete: hourly={hourly.get('status')}, daily={daily.get('status')}")
    return {'hourly': hourly, 'daily': daily}


def run_status():
    """Print system status."""
    from valuation_system.agents.orchestrator import OrchestratorAgent

    orch = OrchestratorAgent()
    status = orch.get_system_status()

    print(f"\n{'='*60}")
    print("  VALUATION SYSTEM STATUS")
    print(f"{'='*60}")
    print(f"\n  Dependencies:")
    for dep, available in status.get('dependencies', {}).items():
        symbol = 'OK' if available else 'FAIL'
        print(f"    {dep:<20} [{symbol}]")

    print(f"\n  Task States:")
    for task, state in status.get('task_states', {}).items():
        print(f"    {task:<25} {state.get('status', 'UNKNOWN'):<12} "
              f"Last: {state.get('last_success', 'never')}")

    print(f"\n  Data Staleness:")
    for data, info in status.get('data_staleness', {}).items():
        stale = 'STALE' if info.get('is_stale') else 'FRESH'
        print(f"    {data:<20} [{stale}] Modified: {info.get('last_modified', 'unknown')}")

    print(f"\n  Queued Operations: {status.get('queued_operations', 0)}")
    print(f"  Active Sectors: {status.get('active_sectors', [])}")
    print(f"  Active Companies: {status.get('active_companies', [])}")
    print(f"{'='*60}")

    return status


def run_tests():
    """Run regression test suite."""
    from valuation_system.tests.regression_tests import RegressionTestRunner
    runner = RegressionTestRunner()
    return runner.run_all_tests()


def run_init():
    """Initialize the system: create tables, seed data, create sheets."""
    logger.info("Initializing valuation system...")

    # 1. Create MySQL tables
    try:
        from valuation_system.storage.mysql_client import get_mysql_client
        mysql = get_mysql_client()
        schema_path = os.path.join(os.path.dirname(__file__), '..', 'storage', 'schema.sql')
        with open(schema_path, 'r') as f:
            sql_content = f.read()

        # Parse SQL: split on semicolons, filter out comments and empty lines
        statements = []
        for raw_stmt in sql_content.split(';'):
            # Remove comment-only lines
            lines = []
            for line in raw_stmt.split('\n'):
                stripped = line.strip()
                if stripped and not stripped.startswith('--'):
                    lines.append(line)
            stmt = '\n'.join(lines).strip()
            if stmt:
                statements.append(stmt)

        for stmt in statements:
            try:
                mysql.execute(stmt)
                # Log CREATE TABLE name for visibility
                if 'CREATE TABLE' in stmt.upper():
                    table_name = stmt.split('EXISTS')[1].split('(')[0].strip() if 'EXISTS' in stmt else '?'
                    logger.info(f"  Created/verified table: {table_name}")
            except Exception as e:
                if 'already exists' not in str(e).lower() and 'Duplicate' not in str(e):
                    logger.warning(f"SQL warning: {e}")

        logger.info("MySQL tables created/verified")

        # Verify company master is accessible
        company_check = mysql.query_one(
            "SELECT COUNT(*) as cnt FROM mssdb.kbapp_marketscrip "
            "WHERE scrip_type IN ('', 'EQS') AND symbol IS NOT NULL AND symbol != ''"
        )
        if company_check:
            logger.info(f"Company master (mssdb.kbapp_marketscrip): {company_check['cnt']} equity scrips")
        else:
            logger.warning("Cannot access mssdb.kbapp_marketscrip - company lookups will fail")

    except Exception as e:
        logger.error(f"MySQL init failed: {e}", exc_info=True)

    # 2. Create necessary directories
    dirs = ['logs', 'reports', 'data/cache', 'data/state']
    base = os.path.join(os.path.dirname(__file__), '..')
    for d in dirs:
        os.makedirs(os.path.join(base, d), exist_ok=True)

    logger.info("System initialization complete")


def main():
    parser = argparse.ArgumentParser(description='Agentic Valuation System Runner')
    parser.add_argument('command', choices=[
        'hourly', 'daily', 'social', 'weekly',
        'valuation', 'portfolio', 'catchup',
        'status', 'test', 'init'
    ], help='Command to run')
    parser.add_argument('--symbol', type=str, help='NSE symbol for on-demand valuation')
    parser.add_argument('--company', type=str, help='Company key for on-demand valuation')

    args = parser.parse_args()

    logger.info(f"=== Running command: {args.command} ===")
    start = datetime.now()

    try:
        if args.command == 'hourly':
            result = run_hourly()
        elif args.command == 'daily':
            result = run_daily()
        elif args.command == 'social':
            result = run_social()
        elif args.command == 'weekly':
            result = run_weekly()
        elif args.command == 'valuation':
            result = run_valuation(symbol=args.symbol, company_key=args.company)
        elif args.command == 'portfolio':
            result = run_portfolio()
        elif args.command == 'catchup':
            result = run_catchup()
        elif args.command == 'status':
            result = run_status()
        elif args.command == 'test':
            result = run_tests()
        elif args.command == 'init':
            result = run_init()
        else:
            print(f"Unknown command: {args.command}")
            return

        elapsed = (datetime.now() - start).total_seconds()
        logger.info(f"=== Command '{args.command}' completed in {elapsed:.1f}s ===")

    except Exception as e:
        logger.error(f"Command '{args.command}' failed: {e}", exc_info=True)
        sys.exit(1)


if __name__ == '__main__':
    main()
