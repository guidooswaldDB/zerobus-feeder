#!/usr/bin/env bash
# Runs a one-shot ingest-latency query against the Zerobus target table
# via the Databricks SQL Statement Execution API.
#
# Usage:
#   ./latency_check.sh                                       # uses defaults below
#   PROFILE=foo WAREHOUSE_ID=abc TABLE=cat.sch.tbl ./latency_check.sh
#
# Loop it every 30s:
#   while ./latency_check.sh; do sleep 30; done

set -euo pipefail

PROFILE="${PROFILE:-guido-demo-azure}"
WAREHOUSE_ID="${WAREHOUSE_ID:-4fe305b78fea61ff}"
TABLE="${TABLE:-guido.zerobus.feed2}"
WINDOW="${WINDOW:-15 MINUTES}"

read -r -d '' SQL <<EOF || true
SELECT
  count(*)                                                                              AS total_rows,
  min(event_time)                                                                       AS earliest_event,
  max(event_time)                                                                       AS latest_event,
  round(avg(unix_timestamp(_metadata.file_modification_time) - unix_timestamp(event_time)), 2) AS avg_latency_sec,
  min(unix_timestamp(_metadata.file_modification_time)   - unix_timestamp(event_time))  AS min_latency_sec,
  max(unix_timestamp(_metadata.file_modification_time)   - unix_timestamp(event_time))  AS max_latency_sec,
  percentile_approx(unix_timestamp(_metadata.file_modification_time) - unix_timestamp(event_time), 0.5)  AS p50_latency_sec,
  percentile_approx(unix_timestamp(_metadata.file_modification_time) - unix_timestamp(event_time), 0.95) AS p95_latency_sec,
  percentile_approx(unix_timestamp(_metadata.file_modification_time) - unix_timestamp(event_time), 0.99) AS p99_latency_sec
FROM ${TABLE}
WHERE event_time >= current_timestamp() - INTERVAL ${WINDOW}
EOF

PAYLOAD=$(jq -n \
  --arg wh "$WAREHOUSE_ID" \
  --arg sql "$SQL" \
  '{warehouse_id:$wh, statement:$sql, wait_timeout:"30s", on_wait_timeout:"CANCEL"}')

RESP=$(databricks api post /api/2.0/sql/statements/ --profile "$PROFILE" --json "$PAYLOAD")

STATE=$(jq -r '.status.state' <<<"$RESP")
if [[ "$STATE" != "SUCCEEDED" ]]; then
  echo "[$(date '+%H:%M:%S')] state=$STATE"
  jq '.status.error // .' <<<"$RESP"
  exit 1
fi

# Pretty-print the single result row as label=value pairs
jq -r '
  .manifest.schema.columns as $cols
  | .result.data_array[0] as $row
  | [range(0; $cols|length)] | map("\($cols[.].name)=\($row[.])") | join("  ")
' <<<"$RESP" | awk -v ts="$(date '+%H:%M:%S')" '{print "["ts"] "$0}'
