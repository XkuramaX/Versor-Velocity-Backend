#!/bin/sh
set -e

echo "Initialising backend database..."
python -c "from database import create_tables; create_tables(); print('DB tables ready.')"

echo "Starting backend..."
exec uvicorn api.NodesApiComplete:app \
  --host 0.0.0.0 \
  --port 8000 \
  --timeout-keep-alive 120 \
  --limit-concurrency 50
