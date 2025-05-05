#!/usr/bin/env bash
# ---------------------------------------------------------------------------
# End-to-end scenarios from the assignment (statements.txt).
# Run from the project root after `docker compose up` has fully started:
#
#     bash scripts/scenarios.sh                 # run them all
#     bash scripts/scenarios.sh 1 4 5           # run scenarios 1, 4 and 5
# ---------------------------------------------------------------------------
set -euo pipefail

cd "$(dirname "$0")/.."

if ! command -v docker >/dev/null 2>&1; then
  echo "docker is required" >&2
  exit 1
fi

# Use `docker compose` if available, else fall back to `docker-compose`.
if docker compose version >/dev/null 2>&1; then
  DC=(docker compose)
else
  DC=(docker-compose)
fi

PRODUCER=( "${DC[@]}" run --rm producer )
CQL_EXEC=( docker exec -T cassandra-1 cqlsh -e )

banner() {
  echo
  echo "==============================================================="
  echo "  $*"
  echo "==============================================================="
}

cql() {
  "${CQL_EXEC[@]}" "$*"
}

# Allow events processing to settle.
sleep_short() {
  sleep "${SCENARIO_SLEEP:-3}"
}

scenario_1() {
  banner "Scenario 1 — basic warehouse cycle (steps 1-3 of the spec)"
  "${PRODUCER[@]}" received --product SKU-001 --zone ZONE-A --quantity 100
  sleep_short
  cql "SELECT product_id, zone_id, available_quantity, reserved_quantity FROM warehouse.inventory_by_product_zone WHERE product_id='SKU-001' AND zone_id='ZONE-A';"
  cql "SELECT product_id, total_available, total_reserved FROM warehouse.inventory_by_product WHERE product_id='SKU-001';"

  "${PRODUCER[@]}" reserved --product SKU-001 --zone ZONE-A --quantity 30
  sleep_short
  cql "SELECT product_id, zone_id, available_quantity, reserved_quantity FROM warehouse.inventory_by_product_zone WHERE product_id='SKU-001' AND zone_id='ZONE-A';"

  "${PRODUCER[@]}" moved --product SKU-001 --from-zone ZONE-A --to-zone ZONE-B --quantity 20
  sleep_short
  cql "SELECT product_id, zone_id, available_quantity, reserved_quantity FROM warehouse.inventory_by_product_zone WHERE product_id='SKU-001';"

  "${PRODUCER[@]}" shipped --product SKU-001 --zone ZONE-A --quantity 10
  sleep_short
  cql "SELECT product_id, zone_id, available_quantity FROM warehouse.inventory_by_product_zone WHERE product_id='SKU-001' AND zone_id='ZONE-A';"

  "${PRODUCER[@]}" order-created --order ORD-1 --item SKU-001:ZONE-A:15
  sleep_short
  cql "SELECT order_id, status, items FROM warehouse.orders WHERE order_id='ORD-1';"
  cql "SELECT product_id, zone_id, available_quantity, reserved_quantity FROM warehouse.inventory_by_product_zone WHERE product_id='SKU-001' AND zone_id='ZONE-A';"

  "${PRODUCER[@]}" order-completed --order ORD-1
  sleep_short
  cql "SELECT order_id, status FROM warehouse.orders WHERE order_id='ORD-1';"
  cql "SELECT product_id, zone_id, available_quantity, reserved_quantity FROM warehouse.inventory_by_product_zone WHERE product_id='SKU-001' AND zone_id='ZONE-A';"
}

scenario_2() {
  banner "Scenario 2 — idempotency (same event_id sent twice)"
  local EID="evt-idem-002"
  "${PRODUCER[@]}" received --product SKU-002 --zone ZONE-A --quantity 50 --event-id "${EID}"
  sleep_short
  cql "SELECT product_id, zone_id, available_quantity FROM warehouse.inventory_by_product_zone WHERE product_id='SKU-002' AND zone_id='ZONE-A';"
  echo "-- Sending the SAME event_id again..."
  "${PRODUCER[@]}" received --product SKU-002 --zone ZONE-A --quantity 50 --event-id "${EID}"
  sleep_short
  cql "SELECT product_id, zone_id, available_quantity FROM warehouse.inventory_by_product_zone WHERE product_id='SKU-002' AND zone_id='ZONE-A';"
  echo "-- Expect available_quantity = 50 (not 100)."
}

scenario_3() {
  banner "Scenario 3 — consistency between denormalised tables"
  "${PRODUCER[@]}" received --product SKU-003 --zone ZONE-A --quantity 100
  sleep_short
  cql "SELECT product_id, zone_id, available_quantity FROM warehouse.inventory_by_product_zone WHERE product_id='SKU-003' AND zone_id='ZONE-A';"
  cql "SELECT product_id, total_available FROM warehouse.inventory_by_product WHERE product_id='SKU-003';"
  cql "SELECT zone_id, product_id, available_quantity FROM warehouse.inventory_by_zone WHERE zone_id='ZONE-A' AND product_id='SKU-003';"
}

scenario_4() {
  banner "Scenario 4 — out-of-order events"
  "${PRODUCER[@]}" received --product SKU-004 --zone ZONE-A --quantity 100 --timestamp 2026-04-01T12:00:00Z
  sleep_short
  "${PRODUCER[@]}" shipped  --product SKU-004 --zone ZONE-A --quantity 20  --timestamp 2026-04-01T12:05:00Z
  sleep_short
  cql "SELECT product_id, zone_id, available_quantity, last_event_timestamp FROM warehouse.inventory_by_product_zone WHERE product_id='SKU-004' AND zone_id='ZONE-A';"

  echo "-- Sending a stale PRODUCT_RECEIVED with timestamp 12:02 (older than 12:05) -- expected to be ignored."
  "${PRODUCER[@]}" received --product SKU-004 --zone ZONE-A --quantity 50  --timestamp 2026-04-01T12:02:00Z
  sleep_short
  cql "SELECT product_id, zone_id, available_quantity, last_event_timestamp FROM warehouse.inventory_by_product_zone WHERE product_id='SKU-004' AND zone_id='ZONE-A';"
  echo "-- Expect available_quantity = 80 (the stale event was skipped)."
}

scenario_5() {
  banner "Scenario 5 — Dead Letter Queue"
  echo "-- Sending an invalid event (quantity = -5) -- expected to land in DLQ."
  "${PRODUCER[@]}" received --product SKU-005 --zone ZONE-A --quantity -5 || true
  sleep_short
  echo "-- Consumer is still alive — sending a valid event after the bad one."
  "${PRODUCER[@]}" received --product SKU-005 --zone ZONE-A --quantity 10
  sleep_short
  cql "SELECT product_id, zone_id, available_quantity FROM warehouse.inventory_by_product_zone WHERE product_id='SKU-005' AND zone_id='ZONE-A';"

  echo "-- Inspect the DLQ topic:"
  "${DC[@]}" exec -T kafka \
    kafka-console-consumer --bootstrap-server kafka:9092 \
      --topic warehouse-events-dlq \
      --from-beginning --max-messages 5 --timeout-ms 5000 || true
}

scenario_6() {
  banner "Scenario 6 — Cassandra cluster + fault tolerance"
  echo "-- Cluster status:"
  docker exec -T cassandra-1 nodetool status

  echo "-- Writing baseline event..."
  "${PRODUCER[@]}" received --product SKU-006 --zone ZONE-A --quantity 200
  sleep_short
  cql "SELECT product_id, total_available FROM warehouse.inventory_by_product WHERE product_id='SKU-006';"

  echo "-- Stopping cassandra-2 to demonstrate QUORUM tolerance..."
  docker stop cassandra-2 || true
  sleep 5
  echo "-- Writing a SHIPPED event with one node down..."
  "${PRODUCER[@]}" shipped --product SKU-006 --zone ZONE-A --quantity 50
  sleep_short
  cql "SELECT product_id, total_available FROM warehouse.inventory_by_product WHERE product_id='SKU-006';"

  echo "-- Restarting cassandra-2..."
  docker start cassandra-2 || true
  echo "-- Wait for it to rejoin the cluster..."
  sleep 25
  docker exec -T cassandra-1 nodetool status
}

scenario_7() {
  banner "Scenario 7 — monitoring & consumer lag"
  echo "-- /health:"
  curl -fsS http://localhost:8000/health && echo
  echo "-- /metrics (head):"
  curl -fsS http://localhost:8000/metrics | grep -E '^(events_processed_total|consumer_lag|event_processing_duration_seconds|cassandra_write_errors_total)\b' | head -n 20
  echo "-- Producing 10 events of mixed types..."
  for i in $(seq 1 10); do
    "${PRODUCER[@]}" received --product "SKU-MON-$i" --zone ZONE-A --quantity "$((10 * i))"
  done
  sleep_short
  curl -fsS http://localhost:8000/metrics | grep -E '^events_processed_total' | head -n 20
  echo "-- Grafana: http://localhost:3000 (anonymous viewer, dashboard: 'Smart Warehouse — Consumer')."
}

scenario_8() {
  banner "Scenario 8 — Schema Evolution (v1 ↔ v2 with supplier_id)"
  echo "-- Subject versions registered in Schema Registry:"
  curl -fsS http://localhost:8081/subjects/warehouse-events-value/versions
  echo

  "${PRODUCER[@]}" received --product SKU-008 --zone ZONE-A --quantity 100 \
                            --event-id evt-v1-008 \
                            --schema-version v1
  sleep_short
  cql "SELECT product_id, zone_id, available_quantity, supplier_id FROM warehouse.inventory_by_product_zone WHERE product_id='SKU-008';"

  "${PRODUCER[@]}" received --product SKU-008-V2 --zone ZONE-A --quantity 50 \
                            --supplier SUP-001 \
                            --event-id evt-v2-008 \
                            --schema-version v2
  sleep_short
  cql "SELECT product_id, zone_id, available_quantity, supplier_id FROM warehouse.inventory_by_product_zone WHERE product_id='SKU-008-V2';"
  echo "-- Expect supplier_id = SUP-001 for SKU-008-V2 and NULL for SKU-008."
}

run_one() {
  case "$1" in
    1) scenario_1 ;;
    2) scenario_2 ;;
    3) scenario_3 ;;
    4) scenario_4 ;;
    5) scenario_5 ;;
    6) scenario_6 ;;
    7) scenario_7 ;;
    8) scenario_8 ;;
    *) echo "Unknown scenario: $1" >&2; exit 1 ;;
  esac
}

if [ "$#" -eq 0 ]; then
  set -- 1 2 3 4 5 6 7 8
fi

for n in "$@"; do
  run_one "$n"
done
