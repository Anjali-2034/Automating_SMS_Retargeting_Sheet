#!/usr/bin/env bash
# SMS / Gurkirat sheet sync
# Delegates to the shared SMS scraper with gurkirat-specific flags.
# Usage: ./run.sh --start 2026-05-01 --end 2026-05-12
#        ./run.sh --start 2026-05-01 --end 2026-05-12 --dry-run

set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

python "$SCRIPT_DIR/../sms_shivam/ct_browser_sync.py" \
  --creator gurkirat@goodeducator.com \
  --worksheet-gid 75277204 \
  "$@"
