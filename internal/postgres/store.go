package postgres

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/Surya-Sastry/tab/internal/domain"
	"github.com/Surya-Sastry/tab/internal/event"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type Store struct {
	Pool *pgxpool.Pool
}

type User struct {
	ID    string `json:"id"`
	Name  string `json:"name"`
	Email string `json:"email"`
}

type Group struct {
	ID       string `json:"id"`
	Name     string `json:"name"`
	Currency string `json:"currency"`
}

type GroupMember struct {
	ID   string `json:"id"`
	Name string `json:"name"`
}

type Expense struct {
	ID            string         `json:"id"`
	GroupID       string         `json:"groupId"`
	PayerID       string         `json:"payerId"`
	ActorID       string         `json:"actorId"`
	Description   string         `json:"description"`
	AmountMinor   int64          `json:"amountMinor"`
	Currency      string         `json:"currency"`
	SplitStrategy string         `json:"splitStrategy"`
	Status        string         `json:"status"`
	Revision      int            `json:"revision"`
	Splits        []domain.Split `json:"splits"`
}

type CreateExpenseParams struct {
	GroupID        string
	PayerID        string
	ActorID        string
	Description    string
	AmountMinor    int64
	SplitStrategy  string
	ParticipantIDs []string
	ExactSplits    []domain.Split
	IdempotencyKey string
	Endpoint       string
	CorrelationID  string
}

func Open(ctx context.Context, databaseURL string) (*Store, error) {
	pool, err := pgxpool.New(ctx, databaseURL)
	if err != nil {
		return nil, fmt.Errorf("open postgres: %w", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("ping postgres: %w", err)
	}
	return &Store{Pool: pool}, nil
}

func (s *Store) Close() { s.Pool.Close() }

func (s *Store) Ping(ctx context.Context) error { return s.Pool.Ping(ctx) }

func (s *Store) CreateUser(ctx context.Context, name, email, passwordHash string) (User, error) {
	var user User
	err := s.Pool.QueryRow(ctx, `
		INSERT INTO users (name, email, password_hash)
		VALUES ($1, lower($2), $3)
		RETURNING id, name, email`, cleanText(name), strings.TrimSpace(email), passwordHash).
		Scan(&user.ID, &user.Name, &user.Email)
	if err != nil {
		if isUnique(err) {
			return User{}, fmt.Errorf("%w: email already registered", domain.ErrConflict)
		}
		return User{}, fmt.Errorf("create user: %w", err)
	}
	return user, nil
}

func (s *Store) UserByEmail(ctx context.Context, email string) (User, string, error) {
	var user User
	var hash string
	err := s.Pool.QueryRow(ctx, `
		SELECT id, name, email, password_hash FROM users WHERE email = lower($1)`,
		strings.TrimSpace(email)).Scan(&user.ID, &user.Name, &user.Email, &hash)
	if errors.Is(err, pgx.ErrNoRows) {
		return User{}, "", domain.ErrUnauthenticated
	}
	if err != nil {
		return User{}, "", fmt.Errorf("find user: %w", err)
	}
	return user, hash, nil
}

func (s *Store) UserByID(ctx context.Context, id string) (User, error) {
	var user User
	err := s.Pool.QueryRow(ctx, `SELECT id, name, email FROM users WHERE id=$1`,
		id).Scan(&user.ID, &user.Name, &user.Email)
	if errors.Is(err, pgx.ErrNoRows) {
		return User{}, domain.ErrNotFound
	}
	return user, err
}

func (s *Store) CreateGroup(ctx context.Context, actorID, name, currency string) (Group, error) {
	tx, err := s.Pool.Begin(ctx)
	if err != nil {
		return Group{}, err
	}
	defer tx.Rollback(ctx)
	var group Group
	err = tx.QueryRow(ctx, `
		INSERT INTO groups (name, currency, created_by)
		VALUES ($1, upper($2), $3)
		RETURNING id, name, currency`, cleanText(name), currency, actorID).
		Scan(&group.ID, &group.Name, &group.Currency)
	if err != nil {
		return Group{}, fmt.Errorf("create group: %w", err)
	}
	if _, err = tx.Exec(ctx, `INSERT INTO group_members (group_id,user_id) VALUES ($1,$2)`, group.ID, actorID); err != nil {
		return Group{}, fmt.Errorf("add creator: %w", err)
	}
	if _, err = tx.Exec(ctx, `INSERT INTO group_versions (group_id) VALUES ($1)`, group.ID); err != nil {
		return Group{}, fmt.Errorf("create version: %w", err)
	}
	return group, tx.Commit(ctx)
}

func (s *Store) ListGroups(ctx context.Context, userID string) ([]Group, error) {
	rows, err := s.Pool.Query(ctx, `
		SELECT g.id,g.name,g.currency FROM groups g
		JOIN group_members gm ON gm.group_id=g.id
		WHERE gm.user_id=$1 ORDER BY g.created_at,g.id`, userID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var groups []Group
	for rows.Next() {
		var group Group
		if err := rows.Scan(&group.ID, &group.Name, &group.Currency); err != nil {
			return nil, err
		}
		groups = append(groups, group)
	}
	return groups, rows.Err()
}

func (s *Store) ListGroupMembers(ctx context.Context, groupID, actorID string) ([]GroupMember, error) {
	if err := s.RequireMember(ctx, groupID, actorID); err != nil {
		return nil, err
	}
	rows, err := s.Pool.Query(ctx, `
		SELECT u.id,u.name
		FROM group_members gm
		JOIN users u ON u.id=gm.user_id
		WHERE gm.group_id=$1
		ORDER BY lower(u.name),u.id`, groupID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	members := make([]GroupMember, 0)
	for rows.Next() {
		var member GroupMember
		if err := rows.Scan(&member.ID, &member.Name); err != nil {
			return nil, err
		}
		members = append(members, member)
	}
	return members, rows.Err()
}

func (s *Store) RequireMember(ctx context.Context, groupID, userID string) error {
	var ok bool
	err := s.Pool.QueryRow(ctx, `
		SELECT EXISTS(SELECT 1 FROM group_members WHERE group_id=$1 AND user_id=$2)`,
		groupID, userID).Scan(&ok)
	if err != nil {
		return err
	}
	if !ok {
		return domain.ErrForbidden
	}
	return nil
}

func (s *Store) requireGroupOwner(ctx context.Context, groupID, userID string) error {
	var ok bool
	err := s.Pool.QueryRow(ctx, `
		SELECT EXISTS(SELECT 1 FROM groups WHERE id=$1 AND created_by=$2)`,
		groupID, userID).Scan(&ok)
	if err != nil {
		return err
	}
	if !ok {
		return domain.ErrForbidden
	}
	return nil
}

func (s *Store) AddMember(ctx context.Context, groupID, actorID, userID string) error {
	if err := s.requireGroupOwner(ctx, groupID, actorID); err != nil {
		return err
	}
	_, err := s.Pool.Exec(ctx, `
		INSERT INTO group_members (group_id,user_id) VALUES ($1,$2)
		ON CONFLICT DO NOTHING`, groupID, userID)
	return err
}

func (s *Store) RemoveMember(ctx context.Context, groupID, actorID, userID string) error {
	if err := s.requireGroupOwner(ctx, groupID, actorID); err != nil {
		return err
	}
	tx, err := s.Pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.Serializable})
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	var members int
	if err := tx.QueryRow(ctx, `SELECT count(*) FROM group_members WHERE group_id=$1`, groupID).Scan(&members); err != nil {
		return err
	}
	var balance int64
	if err := tx.QueryRow(ctx, `
		SELECT COALESCE(sum(amount_minor),0) FROM ledger_entries
		WHERE group_id=$1 AND user_id=$2`, groupID, userID).Scan(&balance); err != nil {
		return err
	}
	if members <= 1 || balance != 0 {
		return fmt.Errorf("%w: member has balance or is the final member", domain.ErrConflict)
	}
	tag, err := tx.Exec(ctx, `DELETE FROM group_members WHERE group_id=$1 AND user_id=$2`, groupID, userID)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return domain.ErrNotFound
	}
	return tx.Commit(ctx)
}

func (s *Store) CreateExpense(ctx context.Context, p CreateExpenseParams) (Expense, bool, error) {
	if p.AmountMinor <= 0 || cleanText(p.Description) == "" {
		return Expense{}, false, fmt.Errorf("%w: amount and description are required", domain.ErrInvalid)
	}
	var splits []domain.Split
	var err error
	switch strings.ToUpper(p.SplitStrategy) {
	case "EQUAL":
		splits, err = domain.EqualSplit(p.AmountMinor, p.ParticipantIDs)
	case "EXACT":
		splits, err = domain.ExactSplit(p.AmountMinor, p.ExactSplits)
	default:
		err = fmt.Errorf("%w: splitStrategy must be EQUAL or EXACT", domain.ErrInvalid)
	}
	if err != nil {
		return Expense{}, false, err
	}
	entries, err := domain.ExpenseLedger(p.PayerID, p.AmountMinor, splits)
	if err != nil {
		return Expense{}, false, err
	}
	fingerprint := expenseFingerprint(p, splits)
	tx, err := s.Pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.Serializable})
	if err != nil {
		return Expense{}, false, err
	}
	defer tx.Rollback(ctx)

	if p.IdempotencyKey != "" {
		replayed, body, err := claimCommand(ctx, tx, p.ActorID, p.Endpoint, p.IdempotencyKey, fingerprint)
		if err != nil {
			return Expense{}, false, err
		}
		if replayed {
			var prior Expense
			if err := json.Unmarshal(body, &prior); err != nil {
				return Expense{}, false, fmt.Errorf("decode idempotent response: %w", err)
			}
			return prior, true, tx.Commit(ctx)
		}
	}

	var currency string
	if err := tx.QueryRow(ctx, `SELECT currency FROM groups WHERE id=$1 FOR UPDATE`, p.GroupID).Scan(&currency); errors.Is(err, pgx.ErrNoRows) {
		return Expense{}, false, domain.ErrNotFound
	} else if err != nil {
		return Expense{}, false, err
	}
	memberIDs := append([]string{p.PayerID, p.ActorID}, splitIDs(splits)...)
	if err := requireMembersTx(ctx, tx, p.GroupID, memberIDs); err != nil {
		return Expense{}, false, err
	}
	var version int64
	if err := tx.QueryRow(ctx, `
		UPDATE group_versions SET version=version+1 WHERE group_id=$1 RETURNING version`,
		p.GroupID).Scan(&version); err != nil {
		return Expense{}, false, err
	}
	expense := Expense{
		ID: uuid.NewString(), GroupID: p.GroupID, PayerID: p.PayerID, ActorID: p.ActorID,
		Description: cleanText(p.Description), AmountMinor: p.AmountMinor, Currency: currency,
		SplitStrategy: strings.ToUpper(p.SplitStrategy), Status: "ACTIVE", Revision: 1, Splits: splits,
	}
	if _, err := tx.Exec(ctx, `
		INSERT INTO expenses
		(id,group_id,payer_id,actor_id,description,amount_minor,currency,split_strategy)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8)`,
		expense.ID, expense.GroupID, expense.PayerID, expense.ActorID, expense.Description,
		expense.AmountMinor, expense.Currency, expense.SplitStrategy); err != nil {
		return Expense{}, false, fmt.Errorf("insert expense: %w", err)
	}
	if err := insertSplits(ctx, tx, expense.ID, 1, splits); err != nil {
		return Expense{}, false, err
	}
	if err := insertLedger(ctx, tx, p.GroupID, "EXPENSE", expense.ID, 1, entries); err != nil {
		return Expense{}, false, err
	}
	envelope, err := event.New(event.ExpenseCreatedV1, p.GroupID, p.CorrelationID, version, map[string]any{
		"expenseId": expense.ID, "groupId": p.GroupID, "payerId": p.PayerID,
		"actorId": p.ActorID, "amountMinor": p.AmountMinor, "currency": currency,
		"description": expense.Description, "splitStrategy": expense.SplitStrategy,
		"splits": splits, "ledgerEntries": entries,
	})
	if err != nil {
		return Expense{}, false, err
	}
	if err := insertOutbox(ctx, tx, envelope); err != nil {
		return Expense{}, false, err
	}
	if p.IdempotencyKey != "" {
		body, _ := json.Marshal(expense)
		if err := completeCommand(ctx, tx, p.ActorID, p.Endpoint, p.IdempotencyKey, 201, body); err != nil {
			return Expense{}, false, err
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return Expense{}, false, err
	}
	return expense, false, nil
}

func insertSplits(ctx context.Context, tx pgx.Tx, expenseID string, revision int, splits []domain.Split) error {
	for _, split := range splits {
		if _, err := tx.Exec(ctx, `
			INSERT INTO expense_splits (expense_id,revision,user_id,amount_minor)
			VALUES ($1,$2,$3,$4)`, expenseID, revision, split.UserID, split.AmountMinor); err != nil {
			return fmt.Errorf("insert split: %w", err)
		}
	}
	return nil
}

func insertLedger(ctx context.Context, tx pgx.Tx, groupID, sourceType, sourceID string, revision int, entries []domain.LedgerEntry) error {
	var sum int64
	for _, entry := range entries {
		sum += entry.AmountMinor
		if entry.AmountMinor == 0 {
			continue
		}
		if _, err := tx.Exec(ctx, `
			INSERT INTO ledger_entries (group_id,user_id,source_type,source_id,revision,amount_minor)
			VALUES ($1,$2,$3,$4,$5,$6)`,
			groupID, entry.UserID, sourceType, sourceID, revision, entry.AmountMinor); err != nil {
			return fmt.Errorf("insert ledger: %w", err)
		}
	}
	if sum != 0 {
		return fmt.Errorf("%w: ledger sum %d", domain.ErrInvalid, sum)
	}
	return nil
}

func insertOutbox(ctx context.Context, tx pgx.Tx, envelope event.Envelope) error {
	body, err := json.Marshal(envelope)
	if err != nil {
		return err
	}
	_, err = tx.Exec(ctx, `
		INSERT INTO outbox_events
		(event_id,event_type,aggregate_id,aggregate_version,occurred_at,correlation_id,causation_id,payload)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8)`,
		envelope.EventID, envelope.EventType, envelope.AggregateID, envelope.AggregateVersion,
		envelope.OccurredAt, envelope.CorrelationID, envelope.CausationID, body)
	return err
}

func claimCommand(ctx context.Context, tx pgx.Tx, userID, endpoint, key, fingerprint string) (bool, []byte, error) {
	tag, err := tx.Exec(ctx, `
		INSERT INTO idempotency_records (user_id,endpoint,idempotency_key,request_fingerprint)
		VALUES ($1,$2,$3,$4) ON CONFLICT DO NOTHING`, userID, endpoint, key, fingerprint)
	if err != nil {
		return false, nil, err
	}
	if tag.RowsAffected() == 1 {
		return false, nil, nil
	}
	var existingFingerprint string
	var body []byte
	err = tx.QueryRow(ctx, `
		SELECT request_fingerprint,response_body FROM idempotency_records
		WHERE user_id=$1 AND endpoint=$2 AND idempotency_key=$3 FOR UPDATE`,
		userID, endpoint, key).Scan(&existingFingerprint, &body)
	if err != nil {
		return false, nil, err
	}
	if existingFingerprint != fingerprint {
		return false, nil, fmt.Errorf("%w: idempotency key reused with different request", domain.ErrConflict)
	}
	if len(body) == 0 {
		return false, nil, fmt.Errorf("%w: prior request incomplete", domain.ErrConflict)
	}
	return true, body, nil
}

func completeCommand(ctx context.Context, tx pgx.Tx, userID, endpoint, key string, status int, body []byte) error {
	_, err := tx.Exec(ctx, `
		UPDATE idempotency_records SET status_code=$1,response_body=$2
		WHERE user_id=$3 AND endpoint=$4 AND idempotency_key=$5`,
		status, body, userID, endpoint, key)
	return err
}

func requireMembersTx(ctx context.Context, tx pgx.Tx, groupID string, ids []string) error {
	seen := make(map[string]struct{}, len(ids))
	for _, id := range ids {
		seen[id] = struct{}{}
	}
	for id := range seen {
		var ok bool
		if err := tx.QueryRow(ctx, `
			SELECT EXISTS(SELECT 1 FROM group_members WHERE group_id=$1 AND user_id=$2)`,
			groupID, id).Scan(&ok); err != nil {
			return err
		}
		if !ok {
			return fmt.Errorf("%w: user %s is not a group member", domain.ErrForbidden, id)
		}
	}
	return nil
}

func splitIDs(splits []domain.Split) []string {
	out := make([]string, len(splits))
	for i, split := range splits {
		out[i] = split.UserID
	}
	return out
}

func expenseFingerprint(p CreateExpenseParams, splits []domain.Split) string {
	body, _ := json.Marshal(struct {
		GroupID, PayerID, Description, Strategy string
		Amount                                  int64
		Splits                                  []domain.Split
	}{p.GroupID, p.PayerID, cleanText(p.Description), strings.ToUpper(p.SplitStrategy), p.AmountMinor, splits})
	sum := sha256.Sum256(body)
	return hex.EncodeToString(sum[:])
}

func cleanText(value string) string {
	return strings.Join(strings.Fields(strings.TrimSpace(value)), " ")
}

func isUnique(err error) bool {
	return strings.Contains(err.Error(), "SQLSTATE 23505")
}
