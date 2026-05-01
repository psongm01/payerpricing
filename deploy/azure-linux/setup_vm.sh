#!/usr/bin/env bash
set -euo pipefail

REPO_ROOT="${TIC_REPO_ROOT:-$(pwd)}"
cd "$REPO_ROOT"

sudo apt-get update
sudo apt-get install -y python3 python3-venv ca-certificates

python3 -m venv .venv
. .venv/bin/activate
python3 -m pip install --upgrade pip
python3 -m pip install -r requirements.txt

mkdir -p "${TIC_WORK_DIR:-/mnt/resource/tic-refresh}"

echo "Setup complete."
echo "Python: $REPO_ROOT/.venv/bin/python3"
