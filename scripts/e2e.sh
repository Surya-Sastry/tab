#!/usr/bin/env sh
set -eu

: "${POSTGRES_USER:?set POSTGRES_USER}"
: "${POSTGRES_DB:?set POSTGRES_DB}"
: "${ADMIN_REPLAY_KEY:?set ADMIN_REPLAY_KEY}"

api="${TAB_API_URL:-http://127.0.0.1:8080}"
frontend="${TAB_FRONTEND_URL:-http://127.0.0.1:3000}"
work="$(mktemp -d)"
probe_pid=""
cleanup_work() {
  test -z "$probe_pid" || kill "$probe_pid" >/dev/null 2>&1 || true
  rm -rf "$work"
}
trap cleanup_work EXIT

for _ in $(seq 1 60); do
  if curl -fsS "${api}/health/ready" >/dev/null; then
    break
  fi
  sleep 1
done
curl -fsS "${api}/health/ready" >/dev/null
for _ in $(seq 1 30); do
  if curl -fsS "${frontend}/" >"$work/frontend.html" &&
    curl -fsS "${frontend}/health/ready" >/dev/null; then
    break
  fi
  sleep 1
done
rg -q '<title>Tab' "$work/frontend.html"

user_password="$(openssl rand -base64 24 | tr -d '\r\n')"
suffix="$(date +%s)"

curl -fsS -H "Content-Type: application/json" \
  -d "{\"Name\":\"Alice\",\"Email\":\"alice-${suffix}@example.invalid\",\"Password\":\"${user_password}\"}" \
  "${api}/api/v1/auth/register" >"$work/alice.json"
curl -fsS -H "Content-Type: application/json" \
  -d "{\"Name\":\"Bob\",\"Email\":\"bob-${suffix}@example.invalid\",\"Password\":\"${user_password}\"}" \
  "${api}/api/v1/auth/register" >"$work/bob.json"

alice_id="$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))["id"])' "$work/alice.json")"
bob_id="$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))["id"])' "$work/bob.json")"

curl -fsS -H "Content-Type: application/json" \
  -d "{\"Email\":\"alice-${suffix}@example.invalid\",\"Password\":\"${user_password}\"}" \
  "${api}/api/v1/auth/login" >"$work/login.json"
token="$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))["accessToken"])' "$work/login.json")"

curl -fsS -H "Authorization: Bearer ${token}" -H "Content-Type: application/json" \
  -d '{"Name":"Interview Trip","Currency":"INR"}' \
  "${api}/api/v1/groups" >"$work/group.json"
group_id="$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))["id"])' "$work/group.json")"

curl -fsS -X POST -H "Authorization: Bearer ${token}" -H "Content-Type: application/json" \
  -d "{\"userId\":\"${bob_id}\"}" \
  "${api}/api/v1/groups/${group_id}/members" >/dev/null
curl -fsS -H "Authorization: Bearer ${token}" \
  "${api}/api/v1/groups/${group_id}/members" >"$work/members.json"
python3 -c 'import json,sys; assert len(json.load(open(sys.argv[1])))==2' "$work/members.json"

expense_body="{\"payerId\":\"${alice_id}\",\"description\":\"Dinner\",\"amountMinor\":101,\"splitStrategy\":\"EQUAL\",\"participantIds\":[\"${bob_id}\",\"${alice_id}\"]}"
TAB_WS_URL="ws://127.0.0.1:8080/api/v1/groups/${group_id}/ws" \
  TAB_ACCESS_TOKEN="$token" go run ./scripts/ws_probe.go >"$work/ws-event.json" &
probe_pid=$!
sleep 1
curl -fsS -D "$work/first.headers" -H "Authorization: Bearer ${token}" \
  -H "Content-Type: application/json" -H "Idempotency-Key: e2e-${suffix}" \
  -d "$expense_body" "${api}/api/v1/groups/${group_id}/expenses" >"$work/first.json"
wait "$probe_pid"
probe_pid=""
rg -q '"type":"balance.updated"' "$work/ws-event.json"
curl -fsS -D "$work/replay.headers" -H "Authorization: Bearer ${token}" \
  -H "Content-Type: application/json" -H "Idempotency-Key: e2e-${suffix}" \
  -d "$expense_body" "${api}/api/v1/groups/${group_id}/expenses" >"$work/replay.json"

cmp "$work/first.json" "$work/replay.json"
rg -qi '^Idempotent-Replayed: true' "$work/replay.headers"

for _ in $(seq 1 30); do
  curl -fsS -H "Authorization: Bearer ${token}" \
    "${api}/api/v1/groups/${group_id}/balances" >"$work/balances.json"
  if python3 -c 'import json,sys; values=json.load(open(sys.argv[1])); raise SystemExit(0 if any(x["amountMinor"] for x in values) else 1)' "$work/balances.json"; then
    break
  fi
  sleep 1
done
python3 -c 'import json,sys; values=json.load(open(sys.argv[1])); assert sum(x["amountMinor"] for x in values)==0; assert any(x["amountMinor"] for x in values)' "$work/balances.json"

for _ in $(seq 1 30); do
  curl -fsS -H "Authorization: Bearer ${token}" \
    "${api}/api/v1/groups/${group_id}/activity" >"$work/activity.json"
  if python3 -c 'import json,sys; raise SystemExit(0 if len(json.load(open(sys.argv[1])) or [])==1 else 1)' "$work/activity.json"; then
    break
  fi
  sleep 1
done
python3 -c 'import json,sys; assert len(json.load(open(sys.argv[1])) or [])==1' "$work/activity.json"

podman compose stop redpanda >/dev/null
curl -fsS -H "Authorization: Bearer ${token}" -H "Content-Type: application/json" \
  -H "Idempotency-Key: outage-${suffix}" -d "$expense_body" \
  "${api}/api/v1/groups/${group_id}/expenses" >/dev/null
pending="$(podman compose exec -T postgres psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -tAc \
  "SELECT count(*) FROM outbox_events WHERE status IN ('pending','publishing')")"
test "$pending" -ge 1
podman compose start redpanda >/dev/null

for _ in $(seq 1 30); do
  pending="$(podman compose exec -T postgres psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -tAc \
    "SELECT count(*) FROM outbox_events WHERE status IN ('pending','publishing')")"
  test "$pending" -eq 0 && break
  sleep 1
done
test "$pending" -eq 0

for _ in $(seq 1 30); do
  curl -fsS -H "Authorization: Bearer ${token}" \
    "${api}/api/v1/groups/${group_id}/activity" >"$work/activity-after-outage.json"
  if python3 -c 'import json,sys; raise SystemExit(0 if len(json.load(open(sys.argv[1])))==2 else 1)' "$work/activity-after-outage.json"; then
    break
  fi
  sleep 1
done
python3 -c 'import json,sys; assert len(json.load(open(sys.argv[1])))==2' "$work/activity-after-outage.json"
curl -fsS -H "Authorization: Bearer ${token}" \
  "${api}/api/v1/groups/${group_id}/balances" >"$work/before-duplicate.json"
event_json="$(podman compose exec -T postgres psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -tAc \
  "SELECT payload::text FROM outbox_events ORDER BY occurred_at LIMIT 1")"
printf '%s\n%s\n' "$event_json" "$event_json" | podman compose exec -T redpanda \
  rpk topic produce expense.events --brokers redpanda:9092 >/dev/null
sleep 3
curl -fsS -H "Authorization: Bearer ${token}" \
  "${api}/api/v1/groups/${group_id}/balances" >"$work/after-duplicate.json"
cmp "$work/before-duplicate.json" "$work/after-duplicate.json"
curl -fsS -H "Authorization: Bearer ${token}" \
  "${api}/api/v1/groups/${group_id}/activity" >"$work/activity-after-duplicate.json"
python3 -c 'import json,sys; assert len(json.load(open(sys.argv[1])))==2' "$work/activity-after-duplicate.json"

podman compose stop mongodb >/dev/null
curl -fsS -H "Authorization: Bearer ${token}" -H "Content-Type: application/json" \
  -H "Idempotency-Key: mongo-outage-${suffix}" -d "$expense_body" \
  "${api}/api/v1/groups/${group_id}/expenses" >/dev/null
sleep 2
podman compose start mongodb >/dev/null
for _ in $(seq 1 30); do
  curl -fsS -H "Authorization: Bearer ${token}" \
    "${api}/api/v1/groups/${group_id}/activity" >"$work/activity-after-mongo.json" || true
  if python3 -c 'import json,sys; raise SystemExit(0 if len(json.load(open(sys.argv[1])))==3 else 1)' "$work/activity-after-mongo.json"; then
    break
  fi
  sleep 1
done
python3 -c 'import json,sys; assert len(json.load(open(sys.argv[1])))==3' "$work/activity-after-mongo.json"

printf '%s\n' '{invalid-event' | podman compose exec -T redpanda \
  rpk topic produce expense.events --brokers redpanda:9092 >/dev/null
for _ in $(seq 1 30); do
  dlq_count="$(podman compose exec -T postgres psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -tAc \
    "SELECT count(*) FROM dlq_events")"
  test "$dlq_count" -ge 1 && break
  sleep 1
done
test "$dlq_count" -ge 1

podman compose exec -T postgres psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -c \
  "DELETE FROM balance_projections WHERE group_id='${group_id}'" >/dev/null
curl -fsS -X POST -H "X-Admin-Replay-Key: ${ADMIN_REPLAY_KEY}" \
  "${api}/internal/groups/${group_id}/balances/rebuild" >/dev/null
curl -fsS -H "Authorization: Bearer ${token}" \
  "${api}/api/v1/groups/${group_id}/balances" >"$work/rebuilt-balances.json"
python3 -c 'import json,sys; values=json.load(open(sys.argv[1])); assert sum(x["amountMinor"] for x in values)==0; assert any(x["amountMinor"] for x in values)' "$work/rebuilt-balances.json"

status="$(curl -sS -o /dev/null -w '%{http_code}' \
  "${api}/api/v1/groups/${group_id}/ws")"
test "$status" -eq 401

printf 'E2E passed: idempotency, duplicate safety, dependency recovery, DLQ, rebuild, and WebSocket auth\n'
