#!/usr/bin/env bash
# Run every quality gate, in the order that fails fastest.
#
#   bash scripts/check.sh              everything
#   bash scripts/check.sh --fast       skip the slow, timing-sensitive tests
set -euo pipefail

cd "$(dirname "$0")/.."

PYTEST_ARGS=()
if [[ "${1:-}" == "--fast" ]]; then
    PYTEST_ARGS+=(-m "not slow")
fi

echo "==> ruff format --check"
python -m ruff format --check .

echo "==> ruff check"
python -m ruff check .

echo "==> mypy"
python -m mypy

echo "==> pytest"
python -m pytest -q "${PYTEST_ARGS[@]}"

echo
echo "All checks passed."
