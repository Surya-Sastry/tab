# Tab operations and failure runbook

This is an interview aid and a local runbook. PostgreSQL is authoritative.
Redpanda, MongoDB, balance projections, email, and WebSocket messages can lag or
be rebuilt.

## Process readiness

| Process | Live when | Ready when |
|---|---|---|
| API | HTTP loop is running | PostgreSQL is reachable |
| Outbox publisher | process is running | PostgreSQL and Redpanda are reachable |
| Balance consumer | process is running | PostgreSQL and Redpanda are reachable |
| Feed consumer | process is running | Redpanda and MongoDB are reachable |
| Notification consumer | process is running | Redpanda is reachable; SMTP failure is retried |

Liveness must not fail because a dependency is briefly unavailable; that creates
restart storms. Readiness removes a process from traffic while it cannot safely
serve its role.

## Signals

- `tab_http_request_duration_seconds`: latency distribution by route and status.
- `tab_http_requests_total`: rate and error ratio.
- `tab_outbox_pending`: unpublished work.
- `tab_outbox_oldest_age_seconds`: duration of publication failure.
- `tab_producer_failures_total`: failed broker writes.
- `tab_consumer_failures_total`: processing failures before retry/DLQ.
- `tab_dlq_events_total`: quarantined poison events.
- `tab_websocket_connections`: current local connections.
- Kafka consumer lag: broker-side signal, collected by production Kafka tooling.

Suggested symptom alerts:

- Outbox oldest age above five minutes for ten minutes: publisher or broker is unhealthy.
- Consumer lag grows for fifteen minutes: worker is failing or under-capacity.
- Any sustained DLQ growth: event incompatibility or poison payload.
- HTTP 5xx ratio above 2% for five minutes: inspect by route and correlation ID.
- Projection reconciliation differs from ledger sum: stop trusting the read model and rebuild.

Thresholds are starting points, not universal production SLOs.

## Correlation

Follow `correlationId` from the HTTP log to the outbox event. Follow `eventId`
through publisher and consumer logs. Include consumer, partition, offset,
duration, and outcome. Never log authorization headers, JWTs, password material,
connection strings, replay keys, or complete untrusted payloads.

## Failure matrix

### Redpanda unavailable

Expected: expense and outbox commit; HTTP succeeds; pending count and oldest age
rise. After broker recovery, publisher catches up and duplicate-safe consumers
apply the event.

Evidence: expense row, pending outbox row, metrics before/after, event-correlated
consumer logs, projection equal to ledger.

### Publisher killed after Kafka acknowledgement

Expected: the outbox row may be reclaimed and published again. Every consumer
must produce one business effect for the repeated `eventId`.

Evidence: multiple broker records may exist; one `processed_events` row per
consumer/event; one balance effect and one Mongo feed item.

### Balance consumer killed mid-message

Expected: uncommitted work rolls back and Kafka redelivers. The projection change
and processed marker commit together.

### MongoDB unavailable

Expected: API and ledger remain healthy. Feed consumer retries and lag grows.
Feed catches up after MongoDB returns.

### Poison event

Expected: bounded retries, a `dlq_events` row, and committed source offset so the
partition moves. Admin replay validates authorization and republishes with the
original partition key. Consumers remain idempotent.

### Projection drift

Compare projection values with `SUM(ledger_entries)` by group/member. If they
differ, use the rebuild operation from trusted ledger state for local recovery.
At production scale, build into a shadow projection, verify, then swap reads so
live traffic does not see an empty table.

## Backups and recovery order

1. Restore PostgreSQL first: users, membership, command history, ledger, and outbox.
2. Verify every group ledger sums to zero.
3. Start the publisher and allow unpublished outbox rows to drain.
4. Rebuild balance and MongoDB projections.
5. Start notifications only after deciding whether historical email should be suppressed.
6. Re-enable API writes and WebSockets after reconciliation.

Kafka retention supports replay but is not a substitute for PostgreSQL backup.
MongoDB can be discarded. Email cannot be “restored” as financial state.

## Production changes

- Run replicated Kafka with documented durability settings and monitored ISR.
- Size partitions from throughput, consumer parallelism, and key distribution;
  watch for a single hot group.
- Consider CDC/Debezium when polling pressure or publication latency justifies
  the operational complexity.
- Use a managed secret store and rotate JWT/replay keys; source code contains
  names and placeholders only.
- Put PostgreSQL and MongoDB behind authenticated TLS connections and verify
  certificate validity, key strength, signature algorithm, hostname, and trust.
- Use Redis or another non-authoritative pub/sub layer for WebSocket fan-out
  across instances; clients still refetch state.
- Add schema compatibility checks and a registry when producer/consumer release
  independence grows.
- Rate-limit replay, record the administrator identity, and retain an audit trail.

## Interview demonstration order

1. Explain the transaction and source-of-truth boundary.
2. Create an expense and show ledger conservation.
3. Retry the HTTP request and show the same result.
4. Stop Redpanda, create another expense, and show outbox accumulation.
5. Restart Redpanda and show consumers catch up.
6. Deliver a duplicate event and show one projection effect.
7. Submit a poison event, inspect DLQ, fix/replay, and show forward progress.
8. Delete a projection, rebuild, and reconcile with ledger truth.
9. Attempt an unauthorized WebSocket join and show denial.
10. State local limitations and the production migration path.
