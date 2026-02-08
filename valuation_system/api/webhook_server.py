"""
FastAPI Webhook Server for xyOps Integration
Receives HTTP webhooks from xyOps scheduler and executes valuation jobs

Endpoints:
- POST /webhook/valuation/hourly - Hourly news scan
- POST /webhook/valuation/daily - Daily full valuation
- POST /webhook/valuation/on-demand - Single company valuation
- GET /status - Health check
- GET /metrics - System metrics
"""

import os
import sys
import json
import asyncio
import subprocess
from datetime import datetime
from pathlib import Path
from typing import Optional, Dict

from fastapi import FastAPI, HTTPException, Header, BackgroundTasks, Body
from fastapi.responses import JSONResponse
from pydantic import BaseModel
import uvicorn

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from dotenv import load_dotenv

load_dotenv(Path(__file__).parent.parent / 'config' / '.env')

app = FastAPI(
    title="Valuation System Webhook API",
    description="HTTP webhook adapter for xyOps scheduler integration",
    version="1.0.0"
)

# Configuration
WEBHOOK_TOKEN = os.getenv('XYOPS_WEBHOOK_TOKEN', 'default-dev-token-change-me')
RUNNER_PATH = Path(__file__).parent.parent / 'scheduler' / 'runner.py'


# Request Models
class OnDemandRequest(BaseModel):
    """Request for on-demand valuation."""
    symbol: Optional[str] = None
    company_id: Optional[int] = None
    callback_url: Optional[str] = None
    job_id: Optional[str] = None


class JobResponse(BaseModel):
    """Standard job response."""
    status: str
    job_id: str
    message: str
    started_at: str


# Background job tracker
active_jobs = {}


def validate_token(authorization: str):
    """Validate Bearer token from xyOps."""
    if not authorization:
        raise HTTPException(status_code=401, detail="Missing Authorization header")

    try:
        scheme, token = authorization.split()
        if scheme.lower() != 'bearer':
            raise HTTPException(status_code=401, detail="Invalid authentication scheme")
        if token != WEBHOOK_TOKEN:
            raise HTTPException(status_code=401, detail="Invalid token")
    except ValueError:
        raise HTTPException(status_code=401, detail="Invalid Authorization header format")


async def run_job_async(job_type: str, params: Dict = None, job_id: str = None):
    """
    Execute runner.py in background with JSON output.

    Args:
        job_type: hourly, daily, on-demand
        params: Additional parameters (symbol, company_id, etc.)
        job_id: External job ID for tracking
    """
    if not job_id:
        job_id = f"{job_type}_{datetime.now().strftime('%Y%m%d_%H%M%S')}"

    active_jobs[job_id] = {
        'status': 'RUNNING',
        'started_at': datetime.now().isoformat(),
        'job_type': job_type,
        'params': params
    }

    try:
        # Build command
        cmd = [
            sys.executable,  # Use same Python interpreter
            str(RUNNER_PATH),
            job_type,
            '--json',
            '--job-id', job_id
        ]

        # Add parameters
        if params:
            if params.get('symbol'):
                cmd.extend(['--symbol', params['symbol']])
            if params.get('company_id'):
                cmd.extend(['--company-id', str(params['company_id'])])
            if params.get('callback_url'):
                cmd.extend(['--callback-url', params['callback_url']])

        # Execute
        process = await asyncio.create_subprocess_exec(
            *cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE
        )

        stdout, stderr = await process.communicate()

        # Parse JSON result
        try:
            result = json.loads(stdout.decode())
            active_jobs[job_id].update({
                'status': result.get('status', 'COMPLETED'),
                'result': result,
                'completed_at': datetime.now().isoformat()
            })
        except json.JSONDecodeError:
            active_jobs[job_id].update({
                'status': 'FAILED',
                'error': 'Invalid JSON output from runner',
                'stdout': stdout.decode()[:500],
                'stderr': stderr.decode()[:500],
                'completed_at': datetime.now().isoformat()
            })

        # POST to callback URL if provided
        if params and params.get('callback_url'):
            await post_callback(params['callback_url'], active_jobs[job_id])

    except Exception as e:
        active_jobs[job_id].update({
            'status': 'ERROR',
            'error': str(e),
            'completed_at': datetime.now().isoformat()
        })


async def post_callback(url: str, data: Dict):
    """POST results to callback URL."""
    try:
        import httpx
        async with httpx.AsyncClient() as client:
            await client.post(url, json=data, timeout=10.0)
    except Exception as e:
        print(f"Callback failed: {e}")


# Endpoints

@app.post("/webhook/valuation/hourly", response_model=JobResponse)
async def hourly_news_scan(
    background_tasks: BackgroundTasks,
    authorization: str = Header(...)
):
    """
    Hourly news scan → driver updates.

    Triggered by xyOps cron: 0 * * * * (every hour)
    """
    validate_token(authorization)

    job_id = f"hourly_{datetime.now().strftime('%Y%m%d_%H%M%S')}"

    background_tasks.add_task(run_job_async, "hourly", {}, job_id)

    return JobResponse(
        status="ACCEPTED",
        job_id=job_id,
        message="Hourly news scan started",
        started_at=datetime.now().isoformat()
    )


@app.post("/webhook/valuation/daily", response_model=JobResponse)
async def daily_valuation(
    background_tasks: BackgroundTasks,
    authorization: str = Header(...)
):
    """
    Daily full valuation refresh.

    Triggered by xyOps cron: 0 20 * * 1-5 (8 PM IST, Mon-Fri)
    """
    validate_token(authorization)

    job_id = f"daily_{datetime.now().strftime('%Y%m%d_%H%M%S')}"

    background_tasks.add_task(run_job_async, "daily", {}, job_id)

    return JobResponse(
        status="ACCEPTED",
        job_id=job_id,
        message="Daily valuation started",
        started_at=datetime.now().isoformat()
    )


@app.post("/webhook/valuation/on-demand", response_model=JobResponse)
async def on_demand_valuation(
    request: OnDemandRequest,
    background_tasks: BackgroundTasks,
    authorization: str = Header(...)
):
    """
    On-demand company valuation.

    Request body:
        {
            "symbol": "AETHER",  # NSE symbol OR
            "company_id": 47582,  # company_id
            "callback_url": "https://...",  # Optional
            "job_id": "custom-id"  # Optional
        }
    """
    validate_token(authorization)

    if not request.symbol and not request.company_id:
        raise HTTPException(
            status_code=400,
            detail="Either 'symbol' or 'company_id' required"
        )

    job_id = request.job_id or f"ondemand_{datetime.now().strftime('%Y%m%d_%H%M%S')}"

    params = {
        'symbol': request.symbol,
        'company_id': request.company_id,
        'callback_url': request.callback_url
    }

    background_tasks.add_task(run_job_async, "on-demand", params, job_id)

    return JobResponse(
        status="ACCEPTED",
        job_id=job_id,
        message=f"On-demand valuation started for {request.symbol or request.company_id}",
        started_at=datetime.now().isoformat()
    )


@app.post("/webhook/valuation/social", response_model=JobResponse)
async def social_content_generation(
    background_tasks: BackgroundTasks,
    authorization: str = Header(...)
):
    """
    Social media content generation.

    Triggered by xyOps cron: 0 8 * * * (8 AM IST daily)
    """
    validate_token(authorization)

    job_id = f"social_{datetime.now().strftime('%Y%m%d_%H%M%S')}"

    background_tasks.add_task(run_job_async, "social", {}, job_id)

    return JobResponse(
        status="ACCEPTED",
        job_id=job_id,
        message="Social content generation started",
        started_at=datetime.now().isoformat()
    )


@app.get("/status/{job_id}")
async def get_job_status(job_id: str):
    """Get status of a specific job."""
    if job_id not in active_jobs:
        raise HTTPException(status_code=404, detail=f"Job {job_id} not found")

    return active_jobs[job_id]


@app.get("/status")
async def system_status():
    """
    Health check for xyOps monitoring.

    Returns system status, dependencies, data freshness.
    """
    from storage.mysql_client import ValuationMySQLClient

    status = {
        'status': 'healthy',
        'timestamp': datetime.now().isoformat(),
        'dependencies': {},
        'jobs': {
            'active': len([j for j in active_jobs.values() if j['status'] == 'RUNNING']),
            'recent': list(active_jobs.keys())[-10:]
        }
    }

    # Check MySQL
    try:
        mysql = ValuationMySQLClient.get_instance()
        count = mysql.query_one("SELECT COUNT(*) as cnt FROM vs_active_companies WHERE is_active = 1")
        status['dependencies']['mysql'] = {
            'status': 'UP',
            'active_companies': count['cnt']
        }
    except Exception as e:
        status['dependencies']['mysql'] = {
            'status': 'DOWN',
            'error': str(e)
        }
        status['status'] = 'degraded'

    # Check data freshness
    try:
        mysql = ValuationMySQLClient.get_instance()
        latest = mysql.query_one("""
            SELECT MAX(created_at) as latest
            FROM vs_valuations
            WHERE created_at > DATE_SUB(NOW(), INTERVAL 24 HOUR)
        """)
        status['data_freshness'] = {
            'latest_valuation': latest['latest'].isoformat() if latest['latest'] else None,
            'stale': latest['latest'] is None if latest else True
        }
    except Exception as e:
        status['data_freshness'] = {'error': str(e)}

    return status


@app.get("/metrics")
async def system_metrics():
    """
    System metrics for monitoring.

    Returns:
        - Total valuations run today
        - Total companies tracked
        - Average valuation time
        - Error rate
    """
    from storage.mysql_client import ValuationMySQLClient

    mysql = ValuationMySQLClient.get_instance()

    metrics = {}

    # Valuations today
    today = mysql.query_one("""
        SELECT COUNT(*) as cnt
        FROM vs_valuations
        WHERE DATE(created_at) = CURDATE()
    """)
    metrics['valuations_today'] = today['cnt'] if today else 0

    # Total companies
    companies = mysql.query_one("""
        SELECT COUNT(*) as cnt
        FROM vs_active_companies
        WHERE is_active = 1
    """)
    metrics['active_companies'] = companies['cnt'] if companies else 0

    # Job stats
    metrics['jobs'] = {
        'total': len(active_jobs),
        'running': len([j for j in active_jobs.values() if j['status'] == 'RUNNING']),
        'completed': len([j for j in active_jobs.values() if j['status'] in ['COMPLETED', 'SUCCESS']]),
        'failed': len([j for j in active_jobs.values() if j['status'] in ['FAILED', 'ERROR']])
    }

    return metrics


@app.get("/")
async def root():
    """API root."""
    return {
        'service': 'Valuation System Webhook API',
        'version': '1.0.0',
        'status': 'running',
        'endpoints': {
            'hourly': 'POST /webhook/valuation/hourly',
            'daily': 'POST /webhook/valuation/daily',
            'on-demand': 'POST /webhook/valuation/on-demand',
            'social': 'POST /webhook/valuation/social',
            'status': 'GET /status',
            'metrics': 'GET /metrics'
        }
    }


def start_server(host: str = "0.0.0.0", port: int = 8888):
    """Start the webhook server."""
    print(f"Starting webhook server on {host}:{port}")
    print(f"Endpoints:")
    print(f"  POST http://{host}:{port}/webhook/valuation/hourly")
    print(f"  POST http://{host}:{port}/webhook/valuation/daily")
    print(f"  POST http://{host}:{port}/webhook/valuation/on-demand")
    print(f"  GET  http://{host}:{port}/status")
    print(f"  GET  http://{host}:{port}/metrics")

    uvicorn.run(app, host=host, port=port)


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description='Webhook server for xyOps integration')
    parser.add_argument('--host', default='0.0.0.0', help='Host to bind to')
    parser.add_argument('--port', type=int, default=8888, help='Port to bind to')

    args = parser.parse_args()

    start_server(args.host, args.port)
