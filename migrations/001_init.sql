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

-- +goose Down
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
