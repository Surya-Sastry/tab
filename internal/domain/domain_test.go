package domain

import (
	"errors"
	"reflect"
	"testing"
)

func TestEqualSplitIsDeterministicAndConserved(t *testing.T) {
	got, err := EqualSplit(100, []string{"carol", "alice", "bob"})
	if err != nil {
		t.Fatal(err)
	}
	want := []Split{
		{UserID: "alice", AmountMinor: 34},
		{UserID: "bob", AmountMinor: 33},
		{UserID: "carol", AmountMinor: 33},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got %#v, want %#v", got, want)
	}
}

func TestExactSplitRejectsWrongSum(t *testing.T) {
	_, err := ExactSplit(100, []Split{
		{UserID: "alice", AmountMinor: 70},
		{UserID: "bob", AmountMinor: 20},
	})
	if !errors.Is(err, ErrInvalid) {
		t.Fatalf("got %v, want ErrInvalid", err)
	}
}

func TestExpenseLedgerConserves(t *testing.T) {
	splits, err := EqualSplit(100, []string{"alice", "bob", "carol"})
	if err != nil {
		t.Fatal(err)
	}
	entries, err := ExpenseLedger("alice", 100, splits)
	if err != nil {
		t.Fatal(err)
	}
	var sum int64
	for _, entry := range entries {
		sum += entry.AmountMinor
	}
	if sum != 0 {
		t.Fatalf("ledger sum = %d, want 0", sum)
	}
}

func TestSimplifyDebtsIsDeterministic(t *testing.T) {
	got, err := SimplifyDebts(map[string]int64{
		"alice": 70,
		"bob":   -40,
		"carol": -30,
	})
	if err != nil {
		t.Fatal(err)
	}
	want := []Transfer{
		{FromUserID: "bob", ToUserID: "alice", AmountMinor: 40},
		{FromUserID: "carol", ToUserID: "alice", AmountMinor: 30},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got %#v, want %#v", got, want)
	}
}

func TestSimplifyDebtsRejectsDrift(t *testing.T) {
	_, err := SimplifyDebts(map[string]int64{"alice": 10, "bob": -9})
	if !errors.Is(err, ErrInvalid) {
		t.Fatalf("got %v, want ErrInvalid", err)
	}
}
