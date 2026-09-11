package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/Surya-Sastry/tab/internal/domain"
	"github.com/Surya-Sastry/tab/internal/event"
	"github.com/jackc/pgx/v5"
)

func (s *Store) UpdateExpense(ctx context.Context, actorID, expenseID, description string, amount int64, strategy string, participants []string, exact []domain.Split, correlationID, idempotencyKey string) (Expense, error) {
	if amount <= 0 {
		return Expense{}, fmt.Errorf("%w: positive amount required", domain.ErrInvalid)
	}
	var splits []domain.Split
	var err error
	if strings.EqualFold(strategy, "EQUAL") {
		splits, err = domain.EqualSplit(amount, participants)
	} else {
		splits, err = domain.ExactSplit(amount, exact)
	}
	if err != nil {
		return Expense{}, err
	}
	tx, err := s.Pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.Serializable})
	if err != nil {
		return Expense{}, err
	}
	defer tx.Rollback(ctx)
	endpoint := "PATCH /api/v1/expenses/{expenseID}"
	fingerprint := commandFingerprint(map[string]any{
		"expenseId": expenseID, "description": cleanText(description), "amountMinor": amount,
		"splitStrategy": strings.ToUpper(strategy), "participants": participants, "splits": splits,
	})
	replayed, body, err := claimCommand(ctx, tx, actorID, endpoint, idempotencyKey, fingerprint)
	if err != nil {
		return Expense{}, err
	}
	if replayed {
		var prior Expense
		if err := json.Unmarshal(body, &prior); err != nil {
			return Expense{}, err
		}
		return prior, tx.Commit(ctx)
	}
	var current Expense
	err = tx.QueryRow(ctx, `
		SELECT id,group_id,payer_id,actor_id,description,amount_minor,currency,
		       split_strategy,status,revision
		FROM expenses WHERE id=$1 FOR UPDATE`, expenseID).
		Scan(&current.ID, &current.GroupID, &current.PayerID, &current.ActorID,
			&current.Description, &current.AmountMinor, &current.Currency, &current.SplitStrategy,
			&current.Status, &current.Revision)
	if errors.Is(err, pgx.ErrNoRows) {
		return Expense{}, domain.ErrNotFound
	}
	if err != nil {
		return Expense{}, err
	}
	if current.Status != "ACTIVE" {
		return Expense{}, fmt.Errorf("%w: expense is voided", domain.ErrConflict)
	}
	if err := requireMembersTx(ctx, tx, current.GroupID, append([]string{actorID, current.PayerID}, splitIDs(splits)...)); err != nil {
		return Expense{}, err
	}
	oldRows, err := tx.Query(ctx, `
		SELECT user_id,amount_minor FROM ledger_entries
		WHERE source_type='EXPENSE' AND source_id=$1 AND revision=$2`, expenseID, current.Revision)
	if err != nil {
		return Expense{}, err
	}
	var oldEntries []domain.LedgerEntry
	for oldRows.Next() {
		var entry domain.LedgerEntry
		if err := oldRows.Scan(&entry.UserID, &entry.AmountMinor); err != nil {
			oldRows.Close()
			return Expense{}, err
		}
		oldEntries = append(oldEntries, entry)
	}
	oldRows.Close()
	newEntries, err := domain.ExpenseLedger(current.PayerID, amount, splits)
	if err != nil {
		return Expense{}, err
	}
	revision := current.Revision + 1
	if err := insertLedger(ctx, tx, current.GroupID, "EXPENSE_COMPENSATION", expenseID, revision, domain.Compensate(oldEntries)); err != nil {
		return Expense{}, err
	}
	if err := insertLedger(ctx, tx, current.GroupID, "EXPENSE", expenseID, revision, newEntries); err != nil {
		return Expense{}, err
	}
	if err := insertSplits(ctx, tx, expenseID, revision, splits); err != nil {
		return Expense{}, err
	}
	if _, err := tx.Exec(ctx, `
		UPDATE expenses SET description=$2,amount_minor=$3,split_strategy=$4,
		    revision=$5,updated_at=now() WHERE id=$1`,
		expenseID, cleanText(description), amount, strings.ToUpper(strategy), revision); err != nil {
		return Expense{}, err
	}
	var version int64
	if err := tx.QueryRow(ctx, `
		UPDATE group_versions SET version=version+1 WHERE group_id=$1 RETURNING version`,
		current.GroupID).Scan(&version); err != nil {
		return Expense{}, err
	}
	combined := append(domain.Compensate(oldEntries), newEntries...)
	envelope, err := event.New(event.ExpenseUpdatedV1, current.GroupID, correlationID, version, map[string]any{
		"expenseId": expenseID, "groupId": current.GroupID, "actorId": actorID,
		"amountMinor": amount, "description": cleanText(description), "splits": splits,
		"ledgerEntries": combined,
	})
	if err != nil {
		return Expense{}, err
	}
	if err := insertOutbox(ctx, tx, envelope); err != nil {
		return Expense{}, err
	}
	current.Description, current.AmountMinor, current.SplitStrategy = cleanText(description), amount, strings.ToUpper(strategy)
	current.Revision, current.Splits = revision, splits
	responseBody, _ := json.Marshal(current)
	if err := completeCommand(ctx, tx, actorID, endpoint, idempotencyKey, 200, responseBody); err != nil {
		return Expense{}, err
	}
	return current, tx.Commit(ctx)
}

func (s *Store) VoidExpense(ctx context.Context, actorID, expenseID, correlationID, idempotencyKey string) error {
	tx, err := s.Pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.Serializable})
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	endpoint := "DELETE /api/v1/expenses/{expenseID}"
	fingerprint := commandFingerprint(map[string]string{"expenseId": expenseID})
	replayed, _, err := claimCommand(ctx, tx, actorID, endpoint, idempotencyKey, fingerprint)
	if err != nil {
		return err
	}
	if replayed {
		return tx.Commit(ctx)
	}
	var groupID, status string
	var revision int
	if err := tx.QueryRow(ctx, `
		SELECT group_id,status,revision FROM expenses WHERE id=$1 FOR UPDATE`, expenseID).
		Scan(&groupID, &status, &revision); errors.Is(err, pgx.ErrNoRows) {
		return domain.ErrNotFound
	} else if err != nil {
		return err
	}
	if err := requireMembersTx(ctx, tx, groupID, []string{actorID}); err != nil {
		return err
	}
	if status == "VOIDED" {
		return fmt.Errorf("%w: already voided", domain.ErrConflict)
	}
	rows, err := tx.Query(ctx, `
		SELECT user_id,amount_minor FROM ledger_entries
		WHERE source_type='EXPENSE' AND source_id=$1 AND revision=$2`, expenseID, revision)
	if err != nil {
		return err
	}
	var entries []domain.LedgerEntry
	for rows.Next() {
		var entry domain.LedgerEntry
		if err := rows.Scan(&entry.UserID, &entry.AmountMinor); err != nil {
			rows.Close()
			return err
		}
		entries = append(entries, entry)
	}
	rows.Close()
	compensation := domain.Compensate(entries)
	if err := insertLedger(ctx, tx, groupID, "EXPENSE_COMPENSATION", expenseID, revision+1, compensation); err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, `
		UPDATE expenses SET status='VOIDED',revision=revision+1,updated_at=now() WHERE id=$1`,
		expenseID); err != nil {
		return err
	}
	var version int64
	if err := tx.QueryRow(ctx, `
		UPDATE group_versions SET version=version+1 WHERE group_id=$1 RETURNING version`,
		groupID).Scan(&version); err != nil {
		return err
	}
	envelope, err := event.New(event.ExpenseVoidedV1, groupID, correlationID, version, map[string]any{
		"expenseId": expenseID, "groupId": groupID, "actorId": actorID,
		"ledgerEntries": compensation,
	})
	if err != nil {
		return err
	}
	if err := insertOutbox(ctx, tx, envelope); err != nil {
		return err
	}
	if err := completeCommand(ctx, tx, actorID, endpoint, idempotencyKey, 204, []byte(`{}`)); err != nil {
		return err
	}
	return tx.Commit(ctx)
}
