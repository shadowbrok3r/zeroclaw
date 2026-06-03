#!/usr/bin/env bash
# Rebuild ZeroClaw from the current checkout, reinstall the CLI, and refresh
# the web dashboard under XDG data (Linux default gateway auto-detect path).
#
# Usage:
#   ./dev/rebuild-local.sh
#   RESTART=1 ./dev/rebuild-local.sh   # also run: zeroclaw gateway restart
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

WEB_DATA_DIR="${XDG_DATA_HOME:-$HOME/.local/share}/zeroclaw/web/dist"

echo "==> cargo web build (OpenAPI → api-generated.ts → vite)"
cargo web build

echo "==> cargo install --path . --force"
cargo install --path . --force

echo "==> install web dashboard to $WEB_DATA_DIR"
mkdir -p "$WEB_DATA_DIR"
cp -r web/dist/. "$WEB_DATA_DIR/"

echo "==> installed version"
zeroclaw --version

if [[ "${RESTART:-0}" == "1" ]]; then
  echo "==> zeroclaw gateway restart"
  zeroclaw gateway restart
fi

echo "Done."
