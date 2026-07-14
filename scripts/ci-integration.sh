#!/usr/bin/env bash
set -euo pipefail

readonly COUNTERPARTY_ID="b7e2a1f0-3b5d-4a1d-8d5a-1d6c8c1a0001"
readonly DELETED_COUNTERPARTY_ID="b7e2a1f0-3b5d-4a1d-8d5a-1d6c8c1a0005"

pg_scalar() {
  local query="$1"
  docker compose exec -T postgres sh -c \
    'psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -tAc "$1"' _ "$query"
}

wait_for_sql() {
  local query="$1"
  local description="$2"
  local output

  for _ in $(seq 1 60); do
    if output="$(pg_scalar "$query" 2>/dev/null)" && \
       [[ "$(printf '%s' "$output" | tr -d '[:space:]')" == "t" ]]; then
      return 0
    fi
    sleep 1
  done

  echo "Timed out waiting for: $description" >&2
  return 1
}

wait_for_health() {
  for _ in $(seq 1 60); do
    if docker compose exec -T integration-service python -c \
      'import httpx; r=httpx.get("http://consumer-service:8081/health", timeout=5); r.raise_for_status(); p=r.json(); assert p["ready"] and p["db_ok"] and p["kafka_ok"] and p["messages_dlq"] == 0' \
      >/dev/null 2>&1; then
      return 0
    fi
    sleep 1
  done

  echo "Consumer health did not become green" >&2
  return 1
}

topic_end_offset() {
  local topic="$1"
  docker compose exec -T kafka /opt/kafka/bin/kafka-get-offsets.sh \
    --bootstrap-server kafka:19092 --topic "$topic" | \
    awk -F: '{total += $3} END {print total + 0}'
}

wait_for_health

docker compose exec -T integration-service python -m integration sync full
wait_for_sql \
  "SELECT (SELECT count(*) FROM ownership_forms) = 4 AND (SELECT count(*) FROM counterparties) = 5" \
  "initial full synchronization"

COUNTS_BEFORE="$(pg_scalar "SELECT (SELECT count(*) FROM ownership_forms) || ':' || (SELECT count(*) FROM counterparties)")"
readonly COUNTS_BEFORE
docker compose exec -T integration-service python -m integration sync full
wait_for_sql \
  "SELECT (SELECT count(*) FROM ownership_forms) || ':' || (SELECT count(*) FROM counterparties) = '$COUNTS_BEFORE'" \
  "idempotent repeated full synchronization"

docker compose exec -T integration-service \
  python -m integration demo touch "$COUNTERPARTY_ID" --name "CI Updated"
docker compose exec -T integration-service python -m integration sync incremental
wait_for_sql \
  "SELECT name = 'CI Updated' FROM counterparties WHERE id = '$COUNTERPARTY_ID'" \
  "incremental counterparty update"

docker compose exec -T integration-service \
  python -m integration demo delete "$DELETED_COUNTERPARTY_ID"
docker compose exec -T integration-service python -m integration sync incremental
wait_for_sql \
  "SELECT deleted FROM counterparties WHERE id = '$DELETED_COUNTERPARTY_ID'" \
  "soft delete propagation"

wait_for_sql \
  "SELECT count(*) = count(DISTINCT id) FROM counterparties" \
  "absence of duplicate counterparties"
wait_for_sql \
  "SELECT count(*) = 0 FROM counterparties c LEFT JOIN ownership_forms o ON o.id = c.ownership_form_id WHERE c.ownership_form_id IS NOT NULL AND o.id IS NULL" \
  "absence of orphan ownership form references"

wait_for_health

[[ "$(topic_end_offset 1c.ownership_forms.v1.dlq)" == "0" ]]
[[ "$(topic_end_offset 1c.counterparties.v1.dlq)" == "0" ]]

echo "Compose integration smoke test passed."
