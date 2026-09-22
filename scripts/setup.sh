#!/usr/bin/env bash
# scripts/setup.sh — Phase 0: create venv and install all dependencies.
# Run once from the project root: bash scripts/setup.sh

set -e
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"

echo "==> Creating Python virtual environment at $PROJECT_ROOT/venv ..."
python3 -m venv "$PROJECT_ROOT/venv"

echo "==> Activating venv and installing dependencies ..."
source "$PROJECT_ROOT/venv/bin/activate"
pip install --upgrade pip
pip install -r "$PROJECT_ROOT/requirements.txt"

echo ""
echo "==> Setup complete. To activate the venv:"
echo "    source venv/bin/activate"
echo ""
echo "==> To verify Spark works:"
echo "    python test_spark.py"

