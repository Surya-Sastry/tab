# Tab — Shared Expense and Bill Splitting

**Project 3 of 3** · Days 11–15 · Vertical depth B  
**Like:** Splitwise  
**Role:** Learn asynchronous distributed systems: event-driven design, Kafka, transactional outbox, idempotent consumers, ledgers, and real-time projections.

---

## Product summary

Tab lets friends create groups, add shared expenses, calculate balances, simplify who should pay whom, and record settlements.

- **No real money** — settlement records only acknowledge an offline payment.
- **Learning scale** — groups of 5–10 people and hundreds of expenses.
- **Production-shaped behavior** — durable ledger writes, event publishing, replay, duplicate handling, and independently built projections.

The central invariant is:

> Every expense changes the authoritative ledger exactly once, even if requests or events are retried.

## Run the implementation

The repository now contains the Go API, PostgreSQL ledger and migrations,
transactional outbox publisher, Redpanda consumers, MongoDB activity projection,
Mailpit notifications, WebSockets, DLQ/replay, Prometheus metrics, tests, and a
responsive React frontend.

Requirements: Go 1.24+, Node.js 22+, and Docker Compose (or a compatible Compose provider).

```sh
cp .env.example .env
# Replace every placeholder. Generate each secret locally with a CSPRNG, for example:
# openssl rand -base64 32
docker compose up --build
```

Local endpoints:

- React app: `http://localhost:3000`
- API: `http://localhost:8080`
- metrics: `http://localhost:8080/metrics`
- Mailpit: `http://localhost:8025`
- Redpanda external listener: `localhost:19092`

Run fast tests with `go test ./...` and `cd web && npm test`. Set `TEST_DATABASE_URL` to a dedicated
PostgreSQL database to enable the integration tests. Run the guided outage drill
with `sh scripts/failure-drills.sh` after supplying the environment variables
listed by that script.

Implementation map:

```text
cmd/tab-api               REST, health, metrics, WebSocket process
cmd/outbox-publisher      PostgreSQL outbox to Redpanda
cmd/consumer              balance, feed, or notification worker
cmd/migrate               schema bootstrap
internal/domain           deterministic splits, ledger, debt simplification
internal/postgres         commands, transactions, outbox, idempotency, replay
internal/broker           Kafka runtime and projection handlers
internal/httpapi          authenticated HTTP and WebSocket boundary
web                       React/TypeScript app and same-origin Nginx proxy
```

For frontend development, start the backend stack and then run:

```sh
cd web
npm ci
npm run dev
```

Vite serves `http://127.0.0.1:5173` and proxies API requests to the Go process.
The browser JWT is memory-only, so a page refresh intentionally requires login.

---

## Position in the 15-day roadmap

```text
P1 Board (breadth)  →  P2 Showtime (sync depth)  →  P3 Tab (async depth)
     JS monolith         Java microservices            Go + Kafka
```

Showtime explores synchronous service calls and compensation. Tab explores a different combination:

- accept a command through REST;
- commit authoritative data to PostgreSQL;
- reliably publish a domain event;
- let several consumers build independent results;
- tolerate duplicate events and consumer failure;
- update connected users without coupling the HTTP request to every side effect.

---

## Core learning questions

1. Which data is the financial source of truth?
2. How can a database write and Kafka publish avoid getting out of sync?
3. What delivery guarantee does Kafka actually provide?
4. What happens when the same request or event arrives twice?
5. How can consumers fail and later catch up?
6. Which state is authoritative and which state is a projection?
7. How should events be partitioned to preserve per-group ordering?

---

## System architecture

Tab is a **modular Go application plus independently running consumers**. It is not split into unnecessary microservices; Kafka provides the async boundaries being studied.

```text
Client
  │ REST commands / queries
  ▼
Tab API (Go)
  │ single PostgreSQL transaction
  ├──► ledger tables
  └──► outbox table
           │
           ▼
     Outbox Publisher ──► Kafka / Redpanda
                              │ topic: expense.events
                  ┌───────────┼──────────────┐
                  ▼           ▼              ▼
          Balance Projector  Feed Writer   Notification Worker
                  │           │              │
             PostgreSQL    MongoDB        Mailpit/MailHog
                  │
                  └──► WebSocket Hub ──► connected group members

Failed poison events ──► dead-letter topic
```

### Modules/processes

| Component | Responsibility |
|---|---|
| API | Auth, group membership, expense/settlement commands, query endpoints |
| Ledger domain | Validate splits and write authoritative expenses transactionally |
| Outbox publisher | Publish committed events and mark outbox rows delivered |
| Balance projector | Build member balances from expense/settlement events |
| Feed writer | Store human-readable activity in MongoDB |
| Notification worker | Send local email notifications |
| WebSocket hub | Push changed balances to connected group members |
| DLQ/replay tool | Inspect and safely replay failed events |

---

## Technology choices

| Layer | Choice | Why it belongs |
|---|---|---|
| Language | **Go 1.24+** | Learn explicit error handling, interfaces, goroutines, channels, and cancellation |
| HTTP | `net/http` + chi (or Gin) | Keep transport understandable |
| SQL | **PostgreSQL** | Expenses, splits, settlements, and outbox require transactions and constraints |
| SQL access | pgx + sqlc (recommended) or GORM | Learn SQL explicitly; avoid hiding the ledger behind ORM magic |
| Migrations | Goose or Atlas | Version the ledger schema |
| Event broker | **Redpanda/Kafka** | Durable ordered log, consumer groups, replay |
| Document DB | **MongoDB** | Append-oriented activity feed projection |
| Cache/pub-sub | **Redis** | Optional balance cache and WebSocket fan-out |
| Real-time | WebSockets | Members see balance changes without polling |
| Email | Mailpit/MailHog | Local notifications |
| Observability | Structured logs + Prometheus metrics | Measure lag, failures, and outbox backlog |
| Containers | Docker Compose | Run broker, databases, app, and consumers locally |
| Tests | Go testing, Testcontainers | Unit, integration, replay, and failure tests |

---

## Domain model and invariants

### Core entities

- **User** — person using Tab.
- **Group** — membership boundary for expenses.
- **Expense** — amount paid by one member for some members.
- **ExpenseSplit** — how much each participant owes.
- **Settlement** — record that one member paid another offline.
- **LedgerEntry** — optional double-entry representation of obligations.
- **OutboxEvent** — event waiting to be published.
- **BalanceProjection** — derived current balances.
- **ActivityItem** — MongoDB feed document derived from events.

### Invariants

- Expense amount is positive and represented in integer minor units (for example paise/cents), never floating point.
- Payer and all participants belong to the group.
- Split amounts add exactly to the expense total.
- Every expense mutation has an idempotency key unique within its caller/tenant scope.
- A user cannot settle more than the amount currently owed unless the domain explicitly permits credit.
- Ledger and outbox event are committed in the same PostgreSQL transaction.
- Consumers record processed event IDs before/with their side effect.

---

## Supported split strategies

Implement at least two:

1. **Equal:** total divided between selected members; deterministic remainder assignment.
2. **Exact amounts:** supplied amounts must sum exactly to total.

Stretch:

3. **Percentages:** percentages total 100%; deterministic rounding.
4. **Shares:** proportional integer weights.

Use integer minor units and define rounding behavior in tests.

---

## Expense lifecycle

```text
ACTIVE ── edit ──► ACTIVE
   │
   └── delete/void ──► VOIDED
```

Edits and voids should append compensating ledger entries/events rather than silently rewriting historical effects. This preserves auditability.

Settlement lifecycle:

```text
RECORDED ── optional correction ──► REVERSED
```

---

## Event model

Use a common envelope:

```json
{
  "eventId": "uuid",
  "eventType": "expense.created.v1",
  "aggregateId": "group-uuid",
  "aggregateVersion": 42,
  "occurredAt": "RFC3339 timestamp",
  "correlationId": "uuid",
  "causationId": "uuid or null",
  "payload": {}
}
```

Events:

- `expense.created.v1`
- `expense.updated.v1`
- `expense.voided.v1`
- `settlement.recorded.v1`
- `settlement.reversed.v1`

Rules:

- partition by `groupId` so events for one group are ordered;
- event IDs are globally unique;
- event schema changes are additive or versioned;
- never put passwords, tokens, or unnecessary personal data into events;
- consumers treat event payloads as untrusted input and validate them.

---

## Transactional outbox

The API must not perform:

```text
write expense to PostgreSQL
then publish to Kafka
```

Either operation could succeed alone. Instead:

```text
BEGIN
  insert expense + splits/ledger entries
  insert outbox event
COMMIT

outbox publisher later:
  claim pending rows
  publish event to Kafka
  mark published
```

Important implementation questions:

- how publishers claim rows without double-processing (`FOR UPDATE SKIP LOCKED`);
- what happens if publishing succeeds but marking published fails;
- why consumer idempotency is still required;
- how to retry with backoff;
- how to monitor outbox backlog.

The learning target is **at-least-once publication**, not magical exactly-once behavior.

---

## Idempotency

### HTTP command idempotency

`POST /expenses` accepts `Idempotency-Key`.

- First valid request stores key + request fingerprint + response reference.
- Retry with the same key and same request returns the original result.
- Same key with a different request returns conflict.
- Key storage is scoped to authenticated user and endpoint.

### Consumer idempotency

Each consumer has a `processed_events` table/collection with a unique `event_id`.

Process atomically where possible:

```text
BEGIN
  insert processed_event (fails if duplicate)
  apply projection change
COMMIT
```

MongoDB feed writer uses `eventId` as a unique field/upsert key.

---

## API outline

Base path: `/api/v1`

### Auth/users

| Method | Path | Purpose |
|---|---|---|
| POST | `/auth/register` | Register |
| POST | `/auth/login` | Issue JWT |
| GET | `/auth/me` | Current user |

### Groups

| Method | Path | Purpose |
|---|---|---|
| POST | `/groups` | Create group |
| GET | `/groups` | Current user's groups |
| GET | `/groups/{groupId}` | Group details |
| GET | `/groups/{groupId}/members` | Authorized member names |
| POST | `/groups/{groupId}/members` | Add member |
| DELETE | `/groups/{groupId}/members/{userId}` | Remove eligible member |

### Expenses

| Method | Path | Purpose |
|---|---|---|
| POST | `/groups/{groupId}/expenses` | Create expense; requires idempotency key |
| GET | `/groups/{groupId}/expenses` | Paginated history |
| GET | `/expenses/{expenseId}` | Expense details |
| PATCH | `/expenses/{expenseId}` | Correct expense |
| DELETE | `/expenses/{expenseId}` | Void expense |

### Balances and settlements

| Method | Path | Purpose |
|---|---|---|
| GET | `/groups/{groupId}/balances` | Derived member balances |
| GET | `/groups/{groupId}/suggested-settlements` | Debt simplification result |
| POST | `/groups/{groupId}/settlements` | Record offline settlement |
| DELETE | `/settlements/{settlementId}` | Reverse a recorded settlement |

### Activity and operations

| Method | Path | Purpose |
|---|---|---|
| GET | `/groups/{groupId}/activity` | MongoDB feed projection |
| GET | `/health/live` | Process liveness |
| GET | `/health/ready` | Dependencies ready |
| GET | `/metrics` | Prometheus metrics |
| POST | `/internal/dlq/{eventId}/replay` | Local/admin-only replay |
| POST | `/internal/groups/{groupId}/balances/rebuild` | Admin-only projection rebuild from ledger |

### WebSocket

- authenticate on connection;
- authorize group membership before joining `group:{id}`;
- publish `balance.updated` after projection changes;
- never trust client-supplied user identity.

---

## PostgreSQL model

Suggested tables:

- `users`
- `groups`
- `group_members`
- `expenses`
- `expense_splits`
- `settlements`
- `ledger_entries`
- `balance_projections`
- `idempotency_records`
- `outbox_events`
- `processed_events` (per SQL consumer or consumer name included)

Important constraints/indexes:

- unique group membership `(group_id, user_id)`;
- unique idempotency scope `(user_id, endpoint, idempotency_key)`;
- unique event ID;
- nonzero ledger entry amount;
- expense total positive;
- indexes on expense history `(group_id, created_at desc)`;
- outbox scan index on `(status, created_at)`;
- processed event unique `(consumer_name, event_id)`.

---

## MongoDB activity projection

Example document:

```json
{
  "eventId": "uuid",
  "groupId": "uuid",
  "type": "expense.created",
  "actorId": "uuid",
  "summary": "Alice added Dinner",
  "amountMinor": 50000,
  "createdAt": "ISODate"
}
```

Indexes:

- unique `{ eventId: 1 }`;
- feed query `{ groupId: 1, createdAt: -1 }`.

MongoDB is a derived projection. It can be deleted and rebuilt by replaying Kafka events; PostgreSQL remains authoritative.

---

## Debt simplification

Input: net balance per member.

- positive balance → member should receive;
- negative balance → member owes;
- balances must sum to zero.

Output: a smaller set of suggested transfers from debtors to creditors.

Learning goals:

- use a greedy debtor/creditor algorithm;
- prove the sum remains conserved;
- test deterministic tie-breaking;
- distinguish **suggestions** from authoritative ledger state.

---

## Failure scenarios to implement

| Scenario | Expected behavior |
|---|---|
| Client retries expense request | One expense; same response |
| API crashes after DB commit | Outbox row remains and is published later |
| Publisher publishes then crashes before marking row | Event may publish twice; consumers stay idempotent |
| Balance consumer crashes mid-message | Kafka redelivers; projection updates once |
| MongoDB unavailable | Feed consumer retries; ledger/API remain available |
| Poison event always fails validation | Bounded retries then DLQ |
| Events from two groups interleave | Per-group ordering preserved by partition key |
| WebSocket user requests another group | Authorization denies join |
| Projection drifts | Rebuild from event log and compare |

---

## Security requirements

- [x] Validate all REST, WebSocket, and Kafka payloads
- [x] Enforce group membership on every group-scoped query and mutation
- [x] Use generic login errors and rate limiting
- [x] Hash passwords with Argon2id or bcrypt
- [x] Use short-lived JWTs; never store tokens in browser local storage in a real client
- [x] Store secrets only outside source control; commit placeholders, not credentials
- [x] Use integer minor units for amounts
- [x] Use parameterized SQL/generated queries
- [x] Prevent mass assignment: explicit request DTO fields
- [x] Sanitize user text before placing it in logs or email
- [x] Do not put sensitive fields in Kafka events or logs
- [x] Protect DLQ replay as an admin-only/local operation and audit it

---

## Observability requirements

Structured logs include:

- request/correlation ID;
- event ID and event type;
- group ID (non-sensitive identifier);
- consumer name;
- partition/offset;
- processing duration and outcome.

Metrics:

- HTTP latency/error count;
- outbox pending count and oldest-row age;
- producer failures;
- consumer lag;
- consumer processing failures;
- DLQ count;
- WebSocket connections.

Never log JWTs, passwords, raw secrets, or full sensitive payloads.

---

## Testing requirements

### Unit

- [x] Equal split and remainder behavior
- [x] Exact split sum validation
- [x] Balance conservation
- [x] Debt simplification and deterministic output
- [x] Event envelope validation

### Frontend

- [x] Typed API requests, money parsing, and backend error handling
- [x] Authentication mode and memory-only session behavior
- [x] Responsive production build and zero-vulnerability dependency audit
- [x] Same-origin frontend proxy reaches backend readiness

### Integration

- [x] Expense + outbox committed together
- [x] HTTP idempotency behavior
- [x] Outbox claim/publish state handling
- [x] Idempotent balance projection
- [x] Idempotent MongoDB feed upsert

### Failure/replay

- [x] Stop Kafka, create expense, restart Kafka, event eventually publishes
- [x] Process the same event twice; one projection effect
- [x] Feed consumer catches up after MongoDB returns
- [x] Invalid event lands in DLQ
- [x] Delete the balance projection and rebuild it from authoritative ledger entries

### End-to-end

- [x] Create group → add expense → balances update → feed item appears → WebSocket event arrives
- [x] Record settlement → balances and suggestion update

---

## Five-day plan

### Day 11 — Ledger and domain correctness

- [x] Initialize Go modules and layered package structure
- [x] Start PostgreSQL through Docker Compose
- [x] Create users, groups, membership, expenses, splits, settlements
- [x] Implement equal and exact splitting
- [x] Implement transactional expense creation
- [x] Add unit tests for money and balance invariants

**Done when:** an expense produces correct, conserved balances without Kafka.

### Day 12 — Kafka and outbox

- [x] Add Redpanda/Kafka to Compose
- [x] Add event envelope and schema versioning
- [x] Write expense + outbox row in one transaction
- [x] Implement concurrent-safe outbox publisher
- [x] Publish partitioned by group ID
- [x] Add backlog metric/logs

**Done when:** committed expenses eventually appear in the topic after broker recovery.

### Day 13 — Idempotent consumers and projections

- [x] Add balance projector consumer
- [x] Add MongoDB and activity feed consumer
- [x] Add notification consumer + Mailpit/MailHog
- [x] Track processed event IDs per consumer
- [x] Test duplicate delivery for every consumer

**Done when:** replaying one event changes each projection at most once.

### Day 14 — Idempotency, real-time, algorithms

- [x] Add HTTP `Idempotency-Key` handling
- [x] Reject key reuse with a different request
- [x] Add WebSocket group authorization and balance broadcasts
- [x] Implement debt simplification
- [x] Add settlement command and event

**Done when:** retries are harmless and connected members receive a single balance update.

### Day 15 — Failure recovery and explanation

- [x] Add bounded poison-event retries and dead-letter storage
- [x] Build local/admin replay endpoint
- [x] Stop/restart dependencies and record observed behavior
- [x] Rebuild the balance projection from authoritative ledger entries
- [x] Complete integration and E2E tests
- [x] Add CI, structured logs, metrics, and architecture decisions
- [x] Write what changes at production scale

**Done when:** you can demonstrate crash recovery, duplicate handling, DLQ, and projection rebuild.

---

## Learnings map

| Skill | Tab exercise | Target after P3 |
|---|---|---|
| Go | HTTP server, interfaces, explicit errors | ★★☆ |
| Go concurrency | Publisher/consumer lifecycle and cancellation | ★★☆ |
| Domain modeling | Expenses, splits, settlements, invariants | ★★★ |
| PostgreSQL transactions | Ledger + outbox atomic commit | ★★★ |
| Kafka fundamentals | Topics, keys, partitions, consumer groups | ★★★ |
| Event-driven design | Commands separated from projections/side effects | ★★★ |
| Transactional outbox | At-least-once publication | ★★★ |
| Idempotency | HTTP commands and every consumer | ★★★ |
| MongoDB projections | Rebuildable activity feed | ★★☆ |
| WebSockets | Authorized real-time balance updates | ★★☆ |
| DLQ and replay | Isolate and recover failed events | ★★☆ |
| Observability | Lag, backlog, event-correlated logs | ★★☆ |
| Distributed systems reasoning | Failure windows and delivery guarantees | ★★★ |

---

## Scale: build vs understand

| Build locally | Understand theoretically |
|---|---|
| One Redpanda/Kafka broker | Replication, ISR, controller quorum |
| Few topics and consumers | Partition sizing and rebalancing |
| Partition by group ID | Hot partitions and key distribution |
| PostgreSQL outbox poller | CDC/Debezium outbox publishing |
| Small MongoDB projection | Sharding and projection rebuild strategy |
| One WebSocket process | Redis pub/sub fan-out and sticky connections |
| All events retained briefly | Retention, compaction, schema registry |
| Simple retry + DLQ | Retry topics, quarantine, operational replay controls |

---

## Out of scope

- Real bank transfers, cards, UPI, or payment provider integration
- Currency conversion and exchange rates
- Interest, lending, or credit
- Tax calculation
- Microservice decomposition (Showtime already covers it)
- Kubernetes implementation
- Native mobile application
- Production identity provider
- Formal accounting/compliance system

---

## Definition of done

- [x] All services and stores start with Docker Compose
- [x] Amounts use integer minor units and balances conserve value
- [x] Expense + outbox write is one database transaction
- [x] Broker downtime does not lose committed events
- [x] Duplicate HTTP requests and Kafka events have one business effect
- [x] PostgreSQL is demonstrably authoritative; MongoDB is rebuildable/catches up
- [x] Failed poison events reach DLQ storage and can be replayed safely
- [x] WebSocket joins enforce group membership
- [x] Tests cover domain, integration, duplicate, crash, and rebuild behavior
- [x] CI-equivalent test, race, vet, and build commands pass locally
- [x] You can explain at-least-once delivery without claiming exactly-once processing

---

## Docker Compose services

| Service | Purpose |
|---|---|
| `frontend` | React SPA + same-origin Nginx proxy |
| `api` | Go REST + WebSocket server |
| `outbox-publisher` | PostgreSQL → Kafka publisher |
| `balance-consumer` | PostgreSQL balance projection |
| `feed-consumer` | MongoDB activity projection |
| `notification-consumer` | Local email |
| `postgres` | Authoritative ledger + outbox |
| `redpanda` | Kafka-compatible event broker |
| `mongodb` | Activity feed projection |
| `redis` | Optional cache/WebSocket fan-out |
| `mailpit` | Local SMTP and web UI |

Begin with fewer processes and split them into separate Compose services only when their independent failure behavior matters.

---

## Environment template guidance

Commit only names and placeholders:

```env
HTTP_ADDRESS=<local-listen-address>
DATABASE_URL=<local-postgres-url>
MONGODB_URI=<local-mongodb-url>
REDIS_URL=<local-redis-url>
KAFKA_BROKERS=<local-broker-list>
KAFKA_CONSUMER_GROUP=<consumer-group-name>
JWT_SIGNING_KEY=<generate-locally-do-not-commit>
SMTP_HOST=<local-mail-catcher-host>
SMTP_PORT=<local-mail-catcher-port>
```

Never commit actual credentials, tokens, or signing keys.

---

## Mental models to practice

1. **Source of truth:** PostgreSQL ledger is authoritative; balances/feed are projections.
2. **Interfaces are contracts:** REST commands and versioned event envelopes.
3. **State is hard:** mutable current balance vs immutable historical entries.
4. **Failure windows:** enumerate every crash point between DB, broker, and consumer.
5. **Idempotency:** at-least-once delivery requires duplicate-safe effects.
6. **Tradeoffs:** Kafka adds operational cost but allows durable fan-out and replay.
7. **Observability:** lag and outbox age reveal failure before users report it.

---

## Start after Showtime

Before coding, write:

1. group/expense invariants;
2. equal and exact split examples with rounding;
3. event envelope and event list;
4. crash-window table for API, outbox publisher, and consumers;
5. authoritative vs projected data map.

Then build Day 11 without Kafka first. Domain correctness comes before infrastructure.
