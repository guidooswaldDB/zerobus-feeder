#!/usr/bin/env bash
# Startup script for Linux / macOS.
# Creates a local virtualenv on first run, installs dependencies, and launches
# zerobus_feeder.py forwarding any arguments you pass.
#
# Usage:
#   ./run.sh                         # interactive first-run wizard
#   ./run.sh --config config.yaml    # run with a YAML config
#   ./run.sh --non-interactive       # reuse last-saved values
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

VENV_DIR=".venv"
STAMP="$VENV_DIR/.requirements.stamp"

PYTHON_BIN="${PYTHON:-}"
if [[ -z "$PYTHON_BIN" ]]; then
  for candidate in python3.12 python3.11 python3.10 python3.9 python3 python; do
    if command -v "$candidate" >/dev/null 2>&1; then
      PYTHON_BIN="$candidate"
      break
    fi
  done
fi

if [[ -z "$PYTHON_BIN" ]]; then
  echo "Error: no python3 found on PATH. Install Python 3.9+ and retry." >&2
  exit 1
fi

"$PYTHON_BIN" - <<'PY' >/dev/null || { echo "Error: Python 3.9+ is required." >&2; exit 1; }
import sys
raise SystemExit(0 if sys.version_info >= (3, 9) else 1)
PY

if [[ ! -d "$VENV_DIR" ]]; then
  echo "Creating virtualenv in $VENV_DIR ..."
  "$PYTHON_BIN" -m venv "$VENV_DIR"
fi

# shellcheck disable=SC1091
source "$VENV_DIR/bin/activate"

if [[ ! -f "$STAMP" ]] || [[ requirements.txt -nt "$STAMP" ]]; then
  echo "Installing dependencies from requirements.txt ..."
  python -m pip install --upgrade pip >/dev/null
  python -m pip install -r requirements.txt
  touch "$STAMP"
fi

exec python zerobus_feeder.py "$@"
