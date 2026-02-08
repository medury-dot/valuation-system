#!/bin/bash
# Start webhook server for xyOps integration

cd "$(dirname "$0")/.."

echo "Starting Valuation System Webhook Server..."
echo "Port: 8888"
echo "Logs: logs/webhook.log"

# Create logs directory
mkdir -p logs

# Activate virtual environment if exists
if [ -d "venv" ]; then
    source venv/bin/activate
fi

# Start server
python3 -m api.webhook_server \
    --host 0.0.0.0 \
    --port 8888 \
    > logs/webhook.log 2>&1 &

PID=$!
echo "Webhook server started (PID: $PID)"
echo $PID > logs/webhook.pid

echo "Health check: curl http://localhost:8888/status"
