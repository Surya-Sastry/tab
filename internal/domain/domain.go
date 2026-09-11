package domain

import (
	"errors"
	"fmt"
	"sort"
)

var (
	ErrInvalid         = errors.New("invalid input")
	ErrForbidden       = errors.New("forbidden")
	ErrNotFound        = errors.New("not found")
	ErrConflict        = errors.New("conflict")
	ErrUnauthenticated = errors.New("unauthenticated")
)

type Split struct {
	UserID      string `json:"userId"`
	AmountMinor int64  `json:"amountMinor"`
}

type LedgerEntry struct {
	UserID      string `json:"userId"`
	AmountMinor int64  `json:"amountMinor"`
}

type Transfer struct {
	FromUserID  string `json:"fromUserId"`
	ToUserID    string `json:"toUserId"`
	AmountMinor int64  `json:"amountMinor"`
}

func EqualSplit(total int64, participantIDs []string) ([]Split, error) {
	if total <= 0 || len(participantIDs) == 0 {
		return nil, fmt.Errorf("%w: positive total and participants required", ErrInvalid)
	}
	ids := append([]string(nil), participantIDs...)
	sort.Strings(ids)
	for i, id := range ids {
		if id == "" || (i > 0 && id == ids[i-1]) {
			return nil, fmt.Errorf("%w: participants must be unique non-empty IDs", ErrInvalid)
		}
	}
	base, remainder := total/int64(len(ids)), total%int64(len(ids))
	out := make([]Split, len(ids))
	for i, id := range ids {
		amount := base
		if int64(i) < remainder {
			amount++
		}
		out[i] = Split{UserID: id, AmountMinor: amount}
	}
	return out, nil
}

func ExactSplit(total int64, splits []Split) ([]Split, error) {
	if total <= 0 || len(splits) == 0 {
		return nil, fmt.Errorf("%w: positive total and splits required", ErrInvalid)
	}
	out := append([]Split(nil), splits...)
	sort.Slice(out, func(i, j int) bool { return out[i].UserID < out[j].UserID })
	var sum int64
	for i, split := range out {
		if split.UserID == "" || split.AmountMinor < 0 ||
			(i > 0 && split.UserID == out[i-1].UserID) {
			return nil, fmt.Errorf("%w: invalid exact split", ErrInvalid)
		}
		if split.AmountMinor > total-sum {
			return nil, fmt.Errorf("%w: split sum exceeds total", ErrInvalid)
		}
		sum += split.AmountMinor
	}
	if sum != total {
		return nil, fmt.Errorf("%w: split sum %d does not equal total %d", ErrInvalid, sum, total)
	}
	return out, nil
}

func ExpenseLedger(payerID string, total int64, splits []Split) ([]LedgerEntry, error) {
	if payerID == "" || total <= 0 {
		return nil, fmt.Errorf("%w: payer and positive total required", ErrInvalid)
	}
	validated, err := ExactSplit(total, splits)
	if err != nil {
		return nil, err
	}
	byUser := map[string]int64{payerID: total}
	for _, split := range validated {
		byUser[split.UserID] -= split.AmountMinor
	}
	ids := make([]string, 0, len(byUser))
	for id := range byUser {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	entries := make([]LedgerEntry, 0, len(ids))
	var sum int64
	for _, id := range ids {
		if byUser[id] == 0 {
			continue
		}
		entries = append(entries, LedgerEntry{UserID: id, AmountMinor: byUser[id]})
		sum += byUser[id]
	}
	if sum != 0 {
		return nil, fmt.Errorf("%w: ledger does not conserve", ErrInvalid)
	}
	return entries, nil
}

func Compensate(entries []LedgerEntry) []LedgerEntry {
	out := make([]LedgerEntry, len(entries))
	for i, entry := range entries {
		out[i] = LedgerEntry{UserID: entry.UserID, AmountMinor: -entry.AmountMinor}
	}
	return out
}

func SimplifyDebts(balances map[string]int64) ([]Transfer, error) {
	type account struct {
		id     string
		amount int64
	}
	var debtors, creditors []account
	var total int64
	for id, balance := range balances {
		total += balance
		if balance < 0 {
			debtors = append(debtors, account{id: id, amount: -balance})
		} else if balance > 0 {
			creditors = append(creditors, account{id: id, amount: balance})
		}
	}
	if total != 0 {
		return nil, fmt.Errorf("%w: balances do not conserve", ErrInvalid)
	}
	sort.Slice(debtors, func(i, j int) bool {
		if debtors[i].amount == debtors[j].amount {
			return debtors[i].id < debtors[j].id
		}
		return debtors[i].amount > debtors[j].amount
	})
	sort.Slice(creditors, func(i, j int) bool {
		if creditors[i].amount == creditors[j].amount {
			return creditors[i].id < creditors[j].id
		}
		return creditors[i].amount > creditors[j].amount
	})
	var transfers []Transfer
	for di, ci := 0, 0; di < len(debtors) && ci < len(creditors); {
		amount := min(debtors[di].amount, creditors[ci].amount)
		transfers = append(transfers, Transfer{
			FromUserID: debtors[di].id, ToUserID: creditors[ci].id, AmountMinor: amount,
		})
		debtors[di].amount -= amount
		creditors[ci].amount -= amount
		if debtors[di].amount == 0 {
			di++
		}
		if creditors[ci].amount == 0 {
			ci++
		}
	}
	return transfers, nil
}
