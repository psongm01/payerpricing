#!/usr/bin/env bash
set -euo pipefail

MONTH="${1:?Usage: run_rust_worker.sh YYYY-MM shard_01 [extra rust worker args...]}"
SHARD="${2:?Usage: run_rust_worker.sh YYYY-MM shard_01 [extra rust worker args...]}"
shift 2 || true

REPO_ROOT="${TIC_REPO_ROOT:-$(pwd)}"
PYTHON_BIN="${TIC_PYTHON:-$REPO_ROOT/.venv/bin/python3}"
STORAGE_ROOT="${TIC_STORAGE_ROOT:?Set TIC_STORAGE_ROOT to your Azure container URL or local storage path}"
WORK_DIR="${TIC_WORK_DIR:-/mnt/resource/tic-refresh}"
PAYER_CODE="${TIC_PAYER_CODE:-UHC}"
STATE="${TIC_STATE:-TX}"
RUST_BIN="${TIC_RUST_STREAM_PRICING:-$REPO_ROOT/rust/stream_pricing_serde/target/release/stream_pricing_serde}"

cd "$REPO_ROOT"

"$PYTHON_BIN" -m cloud.rust_worker \
  --month "$MONTH" \
  --shard "$SHARD" \
  --storage-root "$STORAGE_ROOT" \
  --repo-root "$REPO_ROOT" \
  --python "$PYTHON_BIN" \
  --work-dir "$WORK_DIR" \
  --rust-bin "$RUST_BIN" \
  --payer-code "$PAYER_CODE" \
  --state "$STATE" \
  "$@"
