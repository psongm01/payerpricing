#!/usr/bin/env bash
set -euo pipefail

REPO_ROOT="${TIC_REPO_ROOT:-$(pwd)}"
cd "$REPO_ROOT"

sudo apt-get update
sudo apt-get install -y python3 python3-venv ca-certificates build-essential pkg-config curl

python3 -m venv .venv
. .venv/bin/activate
python3 -m pip install --upgrade pip
python3 -m pip install -r requirements.txt

if ! command -v cargo >/dev/null 2>&1; then
  curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y
  . "$HOME/.cargo/env"
fi

if command -v cargo >/dev/null 2>&1; then
  (cd rust/stream_pricing_serde && cargo build --release)
fi

mkdir -p "${TIC_WORK_DIR:-/mnt/resource/tic-refresh}"

echo "Setup complete."
echo "Python: $REPO_ROOT/.venv/bin/python3"
echo "Rust streamer: $REPO_ROOT/rust/stream_pricing_serde/target/release/stream_pricing_serde"
