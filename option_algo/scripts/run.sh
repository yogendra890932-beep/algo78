#!/bin/bash
# scripts/run.sh
#
# Runs BOTH the web process (uvicorn) and the worker process
# (worker.py) — required since the worker now owns all live trading
# engine state, communicating with the web process via Redis.
#
# Usage:
#   bash scripts/run.sh dev   (default — web with --reload, worker normal)
#   bash scripts/run.sh prod  (no --reload, single web worker)
#
# For production with systemd, use the two separate unit files
# instead: scripts/algo_bot-web.service and
# scripts/algo_bot-worker.service — each managed/restarted
# independently. This script is for local dev / quick VPS testing.
#
# ⚠️  Requires Redis running and reachable at REDIS_URL (see .env).
#     Install: sudo apt install redis-server (Ubuntu/VPS)
#              or `brew install redis` (macOS)

MODE="${1:-dev}"

if [ -f "venv/bin/activate" ]; then
    source venv/bin/activate
fi

echo "Starting Optiscalper ($MODE mode)..."
echo "Web:    http://localhost:8000"
echo "Worker: trading engines, Telegram, OC monitors"
echo "Press Ctrl+C to stop both"
echo ""

cleanup() {
    echo ""
    echo "Stopping..."
    kill "$WORKER_PID" "$WEB_PID" 2>/dev/null
    wait
}
trap cleanup SIGINT SIGTERM

# Start worker in background
python worker.py &
WORKER_PID=$!

# Start web process
if [ "$MODE" = "prod" ]; then
    uvicorn backend.main:app --host 0.0.0.0 --port 8000 --workers 1 --log-level warning &
else
    uvicorn backend.main:app --host 0.0.0.0 --port 8000 --reload --log-level info &
fi
WEB_PID=$!

wait
