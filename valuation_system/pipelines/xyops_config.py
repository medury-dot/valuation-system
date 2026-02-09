#!/usr/bin/env python3
"""
XYOps Pipeline Configuration for Valuation System

Defines 5 declarative pipelines with scheduling, dependencies, alerting, and monitoring.
Each pipeline is a sequence of steps that can be executed by the XYOps scheduler.

Pipelines:
1. macro_sync      — 06:00 IST daily: Scrape macro data, sync to MySQL/GSheet, assess materiality
2. news_scan       — Every 60 min (08:00-22:00 IST): Scan news, classify, update drivers
3. nse_fetch       — Standalone: Full NSE sweep for all tracked companies (quarterly safety net)
4. daily_valuation — 20:00 IST daily: NSE fetch + batch valuation + alerts + email digest
5. weekly_review   — Sunday 10:00 IST: Full GSheet sync, trend detection, opportunity scoring
6. social_posts    — 08:00 IST daily: Generate and queue social media posts
"""

import os
import sys
import logging
from datetime import datetime

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))

from dotenv import load_dotenv
load_dotenv(os.path.join(os.path.dirname(__file__), '..', 'config', '.env'))

logger = logging.getLogger(__name__)

# =============================================================================
# PIPELINE DEFINITIONS
# =============================================================================

PIPELINES = {
    'macro_sync': {
        'description': 'Daily macro data refresh: scrape MOSPI → sync MySQL/GSheet → materiality alerts',
        'schedule': '0 6 * * *',  # 06:00 IST daily
        'timezone': 'Asia/Kolkata',
        'timeout_minutes': 30,
        'retries': 3,
        'retry_backoff_minutes': 5,
        'alert_on_failure': True,
        'alert_email': os.getenv('ALERT_EMAIL', 'medury@gmail.com'),
        'dependencies': [],  # No upstream dependencies
        'steps': [
            {
                'name': 'check_macro_staleness',
                'description': 'Check if macro CSV is stale (>30 days) and run update script',
                'module': 'valuation_system.agents.orchestrator',
                'class': 'OrchestratorAgent',
                'method': '_check_and_update_macro_data',
                'timeout_minutes': 15,
            },
            {
                'name': 'sync_macro_to_mysql',
                'description': 'Read macro CSVs, compute trends, update MySQL (23 MACRO + ~45 GROUP drivers)',
                'module': 'valuation_system.agents.orchestrator',
                'class': 'OrchestratorAgent',
                'method': '_sync_macro_from_csv',
                'timeout_minutes': 5,
            },
            {
                'name': 'sync_macro_to_gsheet',
                'description': 'Sync MACRO drivers to GSheet Tab 1',
                'module': 'valuation_system.utils.sync_drivers_to_gsheet',
                'function': 'sync_macro_drivers',
                'timeout_minutes': 3,
            },
            {
                'name': 'assess_materiality',
                'description': 'Check for macro divergences, driver momentum, valuation gaps',
                'module': 'valuation_system.agents.orchestrator',
                'class': 'OrchestratorAgent',
                'method': '_assess_materiality',
                'timeout_minutes': 5,
            },
        ],
        'metrics': ['macro_drivers_synced', 'group_drivers_synced', 'alerts_created'],
    },

    'news_scan': {
        'description': 'Hourly news scan: scan sources → classify → update drivers → discover new drivers',
        'schedule': '0 8-22 * * *',  # Every hour 08:00-22:00 IST
        'timezone': 'Asia/Kolkata',
        'timeout_minutes': 15,
        'retries': 2,
        'retry_backoff_minutes': 3,
        'alert_on_failure': False,  # Hourly failures are tolerable
        'dependencies': [],
        'steps': [
            {
                'name': 'scan_all_sources',
                'description': 'NewsScannerAgent scans RSS, Google News, BSE/NSE feeds',
                'module': 'valuation_system.agents.news_scanner',
                'class': 'NewsScannerAgent',
                'method': 'scan_all_sources',
                'timeout_minutes': 5,
            },
            {
                'name': 'classify_and_store',
                'description': 'Classify events by severity, store in MySQL',
                'module': 'valuation_system.agents.news_scanner',
                'class': 'NewsScannerAgent',
                'method': 'classify_and_store',
                'timeout_minutes': 5,
            },
            {
                'name': 'update_drivers',
                'description': 'GroupAnalyst updates drivers from news (parallel per group)',
                'module': 'valuation_system.agents.orchestrator',
                'class': 'OrchestratorAgent',
                'method': 'run_hourly_cycle',
                'timeout_minutes': 10,
                'note': 'run_hourly_cycle includes steps 1-2 + driver updates + critical event handling',
            },
            {
                'name': 'sync_driver_changes',
                'description': 'Sync driver changes to GSheet Tabs 2 & 3',
                'module': 'valuation_system.utils.sync_drivers_to_gsheet',
                'function': 'main',
                'timeout_minutes': 5,
            },
        ],
        'metrics': ['articles_scanned', 'significant_events', 'driver_changes', 'discoveries_pending'],
    },

    'nse_fetch': {
        'description': 'NSE filing data: full sweep of all tracked companies (quarterly safety net)',
        'schedule': '0 2 15 2,5,8,11 *',  # 02:00 IST on 15th of Feb/May/Aug/Nov
        'timezone': 'Asia/Kolkata',
        'timeout_minutes': 45,
        'retries': 1,
        'retry_backoff_minutes': 10,
        'alert_on_failure': True,
        'alert_email': os.getenv('ALERT_EMAIL', 'medury@gmail.com'),
        'dependencies': [],
        'steps': [
            {
                'name': 'nse_full_sweep',
                'description': 'Fetch NSE filing data for all tracked companies',
                'module': 'valuation_system.nse_results_prototype.nse_loader',
                'class': 'NSELoader',
                'method': 'run',
                'kwargs': {'mode': 'sweep'},
                'timeout_minutes': 40,
            },
        ],
        'metrics': ['companies_fetched', 'companies_failed', 'api_calls'],
    },

    'daily_valuation': {
        'description': 'Full valuation run: NSE fetch → approved discoveries → batch valuation → alerts → email digest',
        'schedule': '0 20 * * *',  # 20:00 IST daily
        'timezone': 'Asia/Kolkata',
        'timeout_minutes': 120,  # 2 hours for ~2,900 companies
        'retries': 1,
        'retry_backoff_minutes': 10,
        'alert_on_failure': True,
        'alert_email': os.getenv('ALERT_EMAIL', 'medury@gmail.com'),
        'dependencies': ['macro_sync'],  # Must run after macro_sync completes
        'sla_minutes': 120,  # SLA: complete within 2 hours
        'steps': [
            {
                'name': 'nse_fetch',
                'description': 'Fetch latest NSE filings (event-driven, ~20-100 companies)',
                'module': 'valuation_system.nse_results_prototype.nse_loader',
                'class': 'NSELoader',
                'method': 'run',
                'kwargs': {'mode': 'daily'},
                'timeout_minutes': 10,
                'note': 'Event-driven: only fetches companies with new filings since last run',
            },
            {
                'name': 'process_discoveries',
                'description': 'Promote APPROVED discovered drivers into vs_drivers',
                'module': 'valuation_system.agents.orchestrator',
                'class': 'OrchestratorAgent',
                'method': '_process_approved_discoveries',
                'timeout_minutes': 2,
            },
            {
                'name': 'batch_valuation',
                'description': 'Run batch valuation for all active companies',
                'module': 'valuation_system.utils.batch_valuation',
                'function': 'main',
                'timeout_minutes': 100,
                'note': 'batch_valuation.py handles DB saves, GSheet updates, Excel reports',
            },
            {
                'name': 'sync_results_gsheet',
                'description': 'Sync valuation results to GSheet Tab 5',
                'module': 'valuation_system.utils.sync_drivers_to_gsheet',
                'function': 'main',
                'timeout_minutes': 10,
            },
            {
                'name': 'detect_valuation_gaps',
                'description': 'Flag companies with >10% valuation change',
                'module': 'valuation_system.agents.orchestrator',
                'class': 'OrchestratorAgent',
                'method': '_assess_materiality',
                'timeout_minutes': 5,
            },
        ],
        'metrics': ['nse_companies_fetched', 'companies_valued', 'valuation_errors', 'alerts_triggered', 'elapsed_minutes'],
    },

    'nse_sweep': {
        'description': 'Quarterly NSE full sweep: fetch all 1,500 active companies (safety net)',
        'schedule': '0 2 15 2,5,8,11 *',  # 02:00 IST on 15th of Feb/May/Aug/Nov
        'timezone': 'Asia/Kolkata',
        'timeout_minutes': 60,
        'retries': 2,
        'retry_backoff_minutes': 10,
        'alert_on_failure': True,
        'alert_email': os.getenv('ALERT_EMAIL', 'medury@gmail.com'),
        'dependencies': [],
        'steps': [
            {
                'name': 'nse_full_sweep',
                'description': 'Fetch all 1,500 active companies from NSE (45-day staleness check)',
                'module': 'valuation_system.nse_results_prototype.nse_loader',
                'class': 'NSELoader',
                'method': 'run_sweep',
                'timeout_minutes': 50,
                'note': 'Safety net: catches any filings missed by daily event-driven mode',
            },
        ],
        'metrics': ['companies_fetched', 'companies_skipped', 'elapsed_minutes'],
    },

    'weekly_review': {
        'description': 'Sunday review: full GSheet sync, trend detection, opportunity scoring',
        'schedule': '0 10 * * 0',  # Sunday 10:00 IST
        'timezone': 'Asia/Kolkata',
        'timeout_minutes': 60,
        'retries': 2,
        'retry_backoff_minutes': 10,
        'alert_on_failure': True,
        'alert_email': os.getenv('ALERT_EMAIL', 'medury@gmail.com'),
        'dependencies': [],
        'steps': [
            {
                'name': 'full_gsheet_sync',
                'description': 'Sync all 7 GSheet tabs (Macro, Group, Subgroup, Companies, Discovered)',
                'module': 'valuation_system.utils.sync_drivers_to_gsheet',
                'function': 'main',
                'timeout_minutes': 15,
            },
            {
                'name': 'trend_detection',
                'description': '30/60/90-day driver trajectory analysis per group',
                'module': 'valuation_system.agents.group_analyst',
                'class': 'GroupAnalystAgent',
                'method': 'detect_trend_developments',
                'parallel_per_group': True,
                'timeout_minutes': 20,
            },
            {
                'name': 'opportunity_scoring',
                'description': 'Rank groups by opportunity score (upside × momentum × tailwind / vol)',
                'module': 'valuation_system.agents.orchestrator',
                'class': 'OrchestratorAgent',
                'method': '_assess_materiality',
                'timeout_minutes': 10,
            },
        ],
        'metrics': ['tabs_synced', 'trends_detected', 'opportunity_scores'],
    },

    'social_posts': {
        'description': 'Daily social media: gather signals → generate posts → queue for PM approval',
        'schedule': '0 8 * * *',  # 08:00 IST daily
        'timezone': 'Asia/Kolkata',
        'timeout_minutes': 15,
        'retries': 1,
        'retry_backoff_minutes': 5,
        'alert_on_failure': False,  # Social posts are non-critical
        'dependencies': ['macro_sync'],
        'steps': [
            {
                'name': 'gather_signals',
                'description': 'Collect recent events, driver changes, materiality alerts',
                'module': 'valuation_system.agents.orchestrator',
                'class': 'OrchestratorAgent',
                'method': 'get_system_status',
                'timeout_minutes': 2,
            },
            {
                'name': 'generate_posts',
                'description': 'ContentAgent generates 3 tweet drafts from signals',
                'module': 'valuation_system.agents.content_agent',
                'class': 'ContentAgent',
                'method': 'generate_daily_posts',
                'timeout_minutes': 5,
            },
            {
                'name': 'queue_to_gsheet',
                'description': 'Queue posts to GSheet for PM approval',
                'module': 'valuation_system.agents.content_agent',
                'class': 'ContentAgent',
                'method': 'queue_posts_for_approval',
                'timeout_minutes': 3,
            },
        ],
        'metrics': ['posts_generated', 'posts_queued'],
    },
}


# =============================================================================
# PIPELINE RUNNER (standalone execution)
# =============================================================================

def run_pipeline(pipeline_name: str) -> dict:
    """
    Execute a named pipeline. Each step is run sequentially.
    Returns dict with step results and overall status.
    """
    if pipeline_name not in PIPELINES:
        raise ValueError(f"Unknown pipeline: {pipeline_name}. Available: {list(PIPELINES.keys())}")

    pipeline = PIPELINES[pipeline_name]
    logger.info(f"Starting pipeline: {pipeline_name} — {pipeline['description']}")

    result = {
        'pipeline': pipeline_name,
        'started_at': datetime.now().isoformat(),
        'steps': {},
        'status': 'RUNNING',
    }

    for step in pipeline['steps']:
        step_name = step['name']
        logger.info(f"  Step: {step_name} — {step['description']}")

        step_result = {'status': 'RUNNING', 'started_at': datetime.now().isoformat()}

        try:
            if 'function' in step:
                # Module-level function call
                import importlib
                mod = importlib.import_module(step['module'])
                fn = getattr(mod, step['function'])
                step_output = fn()
            elif 'class' in step and 'method' in step:
                # Class method call
                import importlib
                mod = importlib.import_module(step['module'])
                cls = getattr(mod, step['class'])
                instance = cls()
                method = getattr(instance, step['method'])
                step_output = method()
            else:
                step_output = {'error': 'No callable defined for step'}

            step_result['output'] = step_output
            step_result['status'] = 'SUCCESS'

        except Exception as e:
            logger.error(f"  Step {step_name} failed: {e}", exc_info=True)
            step_result['status'] = 'FAILED'
            step_result['error'] = str(e)

            # If alert_on_failure is True, the pipeline should stop
            if pipeline.get('alert_on_failure'):
                result['status'] = 'FAILED'
                result['failed_step'] = step_name
                result['error'] = str(e)
                result['steps'][step_name] = step_result
                result['completed_at'] = datetime.now().isoformat()
                return result

        step_result['completed_at'] = datetime.now().isoformat()
        result['steps'][step_name] = step_result

    result['status'] = 'SUCCESS'
    result['completed_at'] = datetime.now().isoformat()
    logger.info(f"Pipeline {pipeline_name} completed: {result['status']}")
    return result


def get_pipeline_status() -> dict:
    """Get summary of all pipeline definitions for monitoring dashboard."""
    return {
        name: {
            'description': p['description'],
            'schedule': p['schedule'],
            'steps': len(p['steps']),
            'timeout_minutes': p['timeout_minutes'],
            'dependencies': p['dependencies'],
            'alert_on_failure': p.get('alert_on_failure', False),
        }
        for name, p in PIPELINES.items()
    }


if __name__ == '__main__':
    import sys
    logging.basicConfig(level=logging.INFO, format='%(name)s - %(levelname)s - %(message)s')

    if len(sys.argv) > 1:
        pipeline_name = sys.argv[1]
        result = run_pipeline(pipeline_name)
        print(f"\nPipeline result: {result['status']}")
        for step_name, step_result in result['steps'].items():
            print(f"  {step_name}: {step_result['status']}")
    else:
        print("Available pipelines:")
        for name, info in get_pipeline_status().items():
            print(f"  {name}: {info['description']}")
            print(f"    Schedule: {info['schedule']}, Steps: {info['steps']}, "
                  f"Timeout: {info['timeout_minutes']}min")
        print(f"\nUsage: python3 {sys.argv[0]} <pipeline_name>")
