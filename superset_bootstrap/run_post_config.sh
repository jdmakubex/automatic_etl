#!/usr/bin/env bash
set -euo pipefail

echo "🔧 Installing Python deps (requests)"
python3 -m pip install --user --no-cache-dir requests >/dev/null 2>&1 || pip install --user --no-cache-dir requests

# Ensure PATH includes user base bin (for some images)
export PATH="$PATH:$(python3 -m site --user-base 2>/dev/null)/bin"

echo "📊 Running dataset configuration"
python /bootstrap/configure_datasets.py

echo "⚡ Enabling SQL Lab Run Async by default"
python /bootstrap/enable_sql_lab_async.py || echo "⚠️ Could not enforce Run Async via API; will rely on GLOBAL_ASYNC_QUERIES and UI local setting"

echo "✅ Post-config complete"