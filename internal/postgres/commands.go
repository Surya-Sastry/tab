package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/Surya-Sastry/tab/internal/domain"
	"github.com/Surya-Sastry/tab/internal/event"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

type Settlement struct {
	ID          string `json:"id"`
	GroupID     string `json:"groupId"`
	FromUserID  string `json:"fromUserId"`
	ToUserID    string `json:"toUserId"`
	ActorID     string `json:"actorId"`
	AmountMinor int64  `json:"amountMinor"`
	Status      string `json:"status"`
}

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

func (s *Store) RecordSettlement(ctx context.Context, actorID, groupID, fromID, toID string, amount int64, correlationID, idempotencyKey string) (Settlement, error) {
	if amount <= 0 || fromID == toID {
		return Settlement{}, fmt.Errorf("%w: invalid settlement", domain.ErrInvalid)
	}
	tx, err := s.Pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.Serializable})
	if err != nil {
		return Settlement{}, err
	}
	defer tx.Rollback(ctx)
	endpoint := "POST /api/v1/groups/{groupID}/settlements"
	fingerprint := commandFingerprint(map[string]any{
		"groupId": groupID, "fromUserId": fromID, "toUserId": toID, "amountMinor": amount,
	})
	replayed, body, err := claimCommand(ctx, tx, actorID, endpoint, idempotencyKey, fingerprint)
	if err != nil {
		return Settlement{}, err
	}
	if replayed {
		var prior Settlement
		if err := json.Unmarshal(body, &prior); err != nil {
			return Settlement{}, err
		}
		return prior, tx.Commit(ctx)
	}
	if err := requireMembersTx(ctx, tx, groupID, []string{actorID, fromID, toID}); err != nil {
		return Settlement{}, err
	}
	var fromBalance, toBalance int64
	if err := tx.QueryRow(ctx, `
		SELECT COALESCE(sum(amount_minor),0) FROM ledger_entries
		WHERE group_id=$1 AND user_id=$2`, groupID, fromID).Scan(&fromBalance); err != nil {
		return Settlement{}, err
	}
	if err := tx.QueryRow(ctx, `
		SELECT COALESCE(sum(amount_minor),0) FROM ledger_entries
		WHERE group_id=$1 AND user_id=$2`, groupID, toID).Scan(&toBalance); err != nil {
		return Settlement{}, err
	}
	if fromBalance >= 0 || toBalance <= 0 || amount > min(-fromBalance, toBalance) {
		return Settlement{}, fmt.Errorf("%w: settlement exceeds current obligation", domain.ErrConflict)
	}
	settlement := Settlement{
		ID: uuid.NewString(), GroupID: groupID, FromUserID: fromID, ToUserID: toID,
		ActorID: actorID, AmountMinor: amount, Status: "RECORDED",
	}
	if _, err := tx.Exec(ctx, `
		INSERT INTO settlements (id,group_id,from_user_id,to_user_id,actor_id,amount_minor)
		VALUES ($1,$2,$3,$4,$5,$6)`, settlement.ID, groupID, fromID, toID, actorID, amount); err != nil {
		return Settlement{}, err
	}
	entries := []domain.LedgerEntry{
		{UserID: fromID, AmountMinor: amount},
		{UserID: toID, AmountMinor: -amount},
	}
	if err := insertLedger(ctx, tx, groupID, "SETTLEMENT", settlement.ID, 1, entries); err != nil {
		return Settlement{}, err
	}
	var version int64
	if err := tx.QueryRow(ctx, `
		UPDATE group_versions SET version=version+1 WHERE group_id=$1 RETURNING version`,
		groupID).Scan(&version); err != nil {
		return Settlement{}, err
	}
	envelope, err := event.New(event.SettlementRecordedV1, groupID, correlationID, version, map[string]any{
		"settlementId": settlement.ID, "groupId": groupID, "actorId": actorID,
		"fromUserId": fromID, "toUserId": toID, "amountMinor": amount,
		"ledgerEntries": entries,
	})
	if err != nil {
		return Settlement{}, err
	}
	if err := insertOutbox(ctx, tx, envelope); err != nil {
		return Settlement{}, err
	}
	responseBody, _ := json.Marshal(settlement)
	if err := completeCommand(ctx, tx, actorID, endpoint, idempotencyKey, 201, responseBody); err != nil {
		return Settlement{}, err
	}
	return settlement, tx.Commit(ctx)
}

func (s *Store) ReverseSettlement(ctx context.Context, actorID, settlementID, correlationID, idempotencyKey string) error {
	tx, err := s.Pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.Serializable})
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	endpoint := "DELETE /api/v1/settlements/{settlementID}"
	fingerprint := commandFingerprint(map[string]string{"settlementId": settlementID})
	replayed, _, err := claimCommand(ctx, tx, actorID, endpoint, idempotencyKey, fingerprint)
	if err != nil {
		return err
	}
	if replayed {
		return tx.Commit(ctx)
	}
	var groupID, status string
	if err := tx.QueryRow(ctx, `
		SELECT group_id,status FROM settlements WHERE id=$1 FOR UPDATE`, settlementID).
		Scan(&groupID, &status); errors.Is(err, pgx.ErrNoRows) {
		return domain.ErrNotFound
	} else if err != nil {
		return err
	}
	if err := requireMembersTx(ctx, tx, groupID, []string{actorID}); err != nil {
		return err
	}
	if status == "REVERSED" {
		return fmt.Errorf("%w: settlement already reversed", domain.ErrConflict)
	}
	rows, err := tx.Query(ctx, `
		SELECT user_id,amount_minor FROM ledger_entries
		WHERE source_type='SETTLEMENT' AND source_id=$1`, settlementID)
	if err != nil {
		return err
	}
	var original []domain.LedgerEntry
	for rows.Next() {
		var entry domain.LedgerEntry
		if err := rows.Scan(&entry.UserID, &entry.AmountMinor); err != nil {
			rows.Close()
			return err
		}
		original = append(original, entry)
	}
	rows.Close()
	reversal := domain.Compensate(original)
	if err := insertLedger(ctx, tx, groupID, "SETTLEMENT_REVERSAL", settlementID, 2, reversal); err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, `
		UPDATE settlements SET status='REVERSED',reversed_at=now() WHERE id=$1`, settlementID); err != nil {
		return err
	}
	var version int64
	if err := tx.QueryRow(ctx, `
		UPDATE group_versions SET version=version+1 WHERE group_id=$1 RETURNING version`,
		groupID).Scan(&version); err != nil {
		return err
	}
	envelope, err := event.New(event.SettlementReversedV1, groupID, correlationID, version, map[string]any{
		"settlementId": settlementID, "groupId": groupID, "actorId": actorID,
		"ledgerEntries": reversal,
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
