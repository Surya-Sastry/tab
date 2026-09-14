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
