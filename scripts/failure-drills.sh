#!/usr/bin/env sh
set -eu

: "${TAB_ACCESS_TOKEN:?set TAB_ACCESS_TOKEN}"
: "${TAB_GROUP_ID:?set TAB_GROUP_ID}"
: "${TAB_PAYER_ID:?set TAB_PAYER_ID}"

api="${TAB_API_URL:-http://localhost:8080}"
key="failure-drill-$(date +%s)"
request_file="$(mktemp)"
trap 'rm -f "$request_file"' EXIT

cat >"$request_file" <<EOF
{
  "payerId": "${TAB_PAYER_ID}",
  "description": "broker recovery drill",
  "amountMinor": 100,
  "splitStrategy": "EQUAL",
  "participantIds": ["${TAB_PAYER_ID}"]
}
EOF

echo "Stopping Redpanda and committing an expense to Postgres..."
docker compose stop redpanda
curl --fail-with-body -sS \
  -H "Authorization: Bearer ${TAB_ACCESS_TOKEN}" \
  -H "Idempotency-Key: ${key}" \
  -H "Content-Type: application/json" \
  --data-binary "@${request_file}" \
  "${api}/api/v1/groups/${TAB_GROUP_ID}/expenses"

echo "The outbox backlog should now be non-zero:"
curl --fail-with-body -sS "${api}/metrics" | awk '/tab_outbox_(pending|oldest_age_seconds)/'

echo "Restarting Redpanda; publisher and consumers should catch up..."
docker compose start redpanda
sleep 10
curl --fail-with-body -sS "${api}/metrics" | awk '/tab_outbox_(pending|oldest_age_seconds)/'

echo "Retrying the same HTTP command; the response must identify a replay:"
curl --fail-with-body -i -sS \
  -H "Authorization: Bearer ${TAB_ACCESS_TOKEN}" \
  -H "Idempotency-Key: ${key}" \
  -H "Content-Type: application/json" \
  --data-binary "@${request_file}" \
  "${api}/api/v1/groups/${TAB_GROUP_ID}/expenses"

echo "Compare projection and ledger truth after the consumers catch up:"
curl --fail-with-body -sS \
  -H "Authorization: Bearer ${TAB_ACCESS_TOKEN}" \
  "${api}/api/v1/groups/${TAB_GROUP_ID}/balances"
