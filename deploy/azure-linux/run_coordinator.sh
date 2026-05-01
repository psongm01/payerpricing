#!/usr/bin/env bash
set -euo pipefail

MONTH="${1:?Usage: run_coordinator.sh YYYY-MM [extra coordinator args...]}"
shift || true

REPO_ROOT="${TIC_REPO_ROOT:-$(pwd)}"
PYTHON_BIN="${TIC_PYTHON:-$REPO_ROOT/.venv/bin/python3}"
STORAGE_ROOT="${TIC_STORAGE_ROOT:?Set TIC_STORAGE_ROOT to your Azure container URL or local storage path}"
SHARDS="${TIC_SHARDS:-10}"
SCAN_WORKERS="${TIC_SCAN_WORKERS:-10}"

cd "$REPO_ROOT"

"$PYTHON_BIN" -m cloud.coordinator \
  --month "$MONTH" \
  --storage-root "$STORAGE_ROOT" \
  --repo-root "$REPO_ROOT" \
  --python "$PYTHON_BIN" \
  --shards "$SHARDS" \
  --scan-workers "$SCAN_WORKERS" \
  "$@"
