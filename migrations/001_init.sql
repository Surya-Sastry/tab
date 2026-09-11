-- +goose Up
CREATE EXTENSION IF NOT EXISTS pgcrypto;

CREATE TABLE users (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    name text NOT NULL CHECK (length(name) BETWEEN 1 AND 120),
    email text NOT NULL UNIQUE CHECK (email = lower(email)),
    password_hash text NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE groups (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    name text NOT NULL CHECK (length(name) BETWEEN 1 AND 120),
    currency char(3) NOT NULL CHECK (currency ~ '^[A-Z]{3}$'),
    created_by uuid NOT NULL REFERENCES users(id),
    created_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE group_members (
    group_id uuid NOT NULL REFERENCES groups(id) ON DELETE CASCADE,
    user_id uuid NOT NULL REFERENCES users(id),
    joined_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (group_id, user_id)
);

CREATE TABLE group_versions (
    group_id uuid PRIMARY KEY REFERENCES groups(id) ON DELETE CASCADE,
    version bigint NOT NULL DEFAULT 0 CHECK (version >= 0)
);

CREATE TYPE expense_status AS ENUM ('ACTIVE', 'VOIDED');
CREATE TABLE expenses (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    group_id uuid NOT NULL REFERENCES groups(id),
    payer_id uuid NOT NULL REFERENCES users(id),
    actor_id uuid NOT NULL REFERENCES users(id),
    description text NOT NULL CHECK (length(description) BETWEEN 1 AND 500),
    amount_minor bigint NOT NULL CHECK (amount_minor > 0),
    currency char(3) NOT NULL CHECK (currency ~ '^[A-Z]{3}$'),
    split_strategy text NOT NULL CHECK (split_strategy IN ('EQUAL', 'EXACT')),
    status expense_status NOT NULL DEFAULT 'ACTIVE',
    revision integer NOT NULL DEFAULT 1 CHECK (revision > 0),
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX expenses_group_history_idx ON expenses (group_id, created_at DESC, id);

CREATE TABLE expense_splits (
    expense_id uuid NOT NULL REFERENCES expenses(id),
    revision integer NOT NULL CHECK (revision > 0),
    user_id uuid NOT NULL REFERENCES users(id),
    amount_minor bigint NOT NULL CHECK (amount_minor >= 0),
    PRIMARY KEY (expense_id, revision, user_id)
);

CREATE TYPE settlement_status AS ENUM ('RECORDED', 'REVERSED');
CREATE TABLE settlements (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    group_id uuid NOT NULL REFERENCES groups(id),
    from_user_id uuid NOT NULL REFERENCES users(id),
    to_user_id uuid NOT NULL REFERENCES users(id),
    actor_id uuid NOT NULL REFERENCES users(id),
    amount_minor bigint NOT NULL CHECK (amount_minor > 0),
    status settlement_status NOT NULL DEFAULT 'RECORDED',
    created_at timestamptz NOT NULL DEFAULT now(),
    reversed_at timestamptz,
    CHECK (from_user_id <> to_user_id)
);

CREATE TABLE ledger_entries (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    group_id uuid NOT NULL REFERENCES groups(id),
    user_id uuid NOT NULL REFERENCES users(id),
    source_type text NOT NULL CHECK (source_type IN ('EXPENSE', 'EXPENSE_COMPENSATION', 'SETTLEMENT', 'SETTLEMENT_REVERSAL')),
    source_id uuid NOT NULL,
    revision integer NOT NULL DEFAULT 1 CHECK (revision > 0),
    amount_minor bigint NOT NULL CHECK (amount_minor <> 0),
    created_at timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX ledger_group_user_idx ON ledger_entries (group_id, user_id);
CREATE INDEX ledger_source_idx ON ledger_entries (source_type, source_id, revision);

CREATE TYPE outbox_status AS ENUM ('pending', 'publishing', 'published', 'dead');
CREATE TABLE outbox_events (
    event_id uuid PRIMARY KEY,
    event_type text NOT NULL,
    aggregate_id uuid NOT NULL REFERENCES groups(id),
    aggregate_version bigint NOT NULL CHECK (aggregate_version > 0),
    occurred_at timestamptz NOT NULL,
    correlation_id uuid NOT NULL,
    causation_id uuid,
    payload jsonb NOT NULL,
    status outbox_status NOT NULL DEFAULT 'pending',
    attempts integer NOT NULL DEFAULT 0,
    available_at timestamptz NOT NULL DEFAULT now(),
    locked_at timestamptz,
    published_at timestamptz,
    last_error text
);
CREATE INDEX outbox_scan_idx ON outbox_events (status, available_at, occurred_at);

CREATE TABLE balance_projections (
    group_id uuid NOT NULL REFERENCES groups(id) ON DELETE CASCADE,
    user_id uuid NOT NULL REFERENCES users(id),
    balance_minor bigint NOT NULL DEFAULT 0,
    aggregate_version bigint NOT NULL DEFAULT 0,
    updated_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (group_id, user_id)
);

CREATE TABLE processed_events (
    consumer_name text NOT NULL,
    event_id uuid NOT NULL,
    processed_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (consumer_name, event_id)
);

CREATE TABLE idempotency_records (
    user_id uuid NOT NULL REFERENCES users(id),
    endpoint text NOT NULL,
    idempotency_key text NOT NULL CHECK (length(idempotency_key) BETWEEN 1 AND 200),
    request_fingerprint char(64) NOT NULL,
    status_code integer,
    response_body jsonb,
    created_at timestamptz NOT NULL DEFAULT now(),
    expires_at timestamptz NOT NULL DEFAULT now() + interval '24 hours',
    PRIMARY KEY (user_id, endpoint, idempotency_key)
);
CREATE INDEX idempotency_expiry_idx ON idempotency_records (expires_at);

CREATE TABLE dlq_events (
    event_id uuid NOT NULL,
    consumer_name text NOT NULL,
    payload jsonb NOT NULL,
    error_message text NOT NULL,
    failed_at timestamptz NOT NULL DEFAULT now(),
    replay_count integer NOT NULL DEFAULT 0,
    status text NOT NULL DEFAULT 'failed' CHECK (status IN ('failed', 'replayed')),
    PRIMARY KEY (consumer_name, event_id)
);

-- +goose Down
DROP TABLE IF EXISTS dlq_events;
DROP TABLE IF EXISTS idempotency_records;
DROP TABLE IF EXISTS processed_events;
DROP TABLE IF EXISTS balance_projections;
DROP TABLE IF EXISTS outbox_events;
DROP TABLE IF EXISTS ledger_entries;
DROP TABLE IF EXISTS settlements;
DROP TYPE IF EXISTS settlement_status;
DROP TABLE IF EXISTS expense_splits;
DROP TABLE IF EXISTS expenses;
DROP TYPE IF EXISTS expense_status;
DROP TABLE IF EXISTS group_versions;
DROP TABLE IF EXISTS group_members;
DROP TABLE IF EXISTS groups;
DROP TABLE IF EXISTS users;
