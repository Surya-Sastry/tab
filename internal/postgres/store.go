package postgres

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/Surya-Sastry/tab/internal/domain"
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

func cleanText(value string) string {
	return strings.Join(strings.Fields(strings.TrimSpace(value)), " ")
}

func isUnique(err error) bool {
	return strings.Contains(err.Error(), "SQLSTATE 23505")
}
