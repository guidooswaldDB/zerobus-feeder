#!/usr/bin/env bash
# Removes locally stored connection info and logs so the next run starts clean.
# Targets:
#   .zerobus_feeder_last.yaml   remembered values + client secret
#   feeder_config.yaml          wizard-generated config + client secret
#   zerobus_feeder.log          session log
#
# Usage:
#   ./cleanup.sh
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

TARGETS=(
  ".zerobus_feeder_last.yaml"
  "feeder_config.yaml"
  "zerobus_feeder.log"
)

removed=0
for f in "${TARGETS[@]}"; do
  if [[ -e "$f" ]]; then
    rm -f -- "$f"
    echo "removed $f"
    removed=$((removed + 1))
  fi
done

if [[ "$removed" -eq 0 ]]; then
  echo "nothing to clean"
fi
