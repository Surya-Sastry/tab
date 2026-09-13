package postgres

import (
	"context"
	"os"
	"strings"
	"testing"

	"github.com/Surya-Sastry/tab/internal/domain"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
)

func integrationStore(t *testing.T) *Store {
	t.Helper()
	databaseURL := os.Getenv("TEST_DATABASE_URL")
	if databaseURL == "" {
		t.Skip("TEST_DATABASE_URL is not set")
	}
	ctx := context.Background()
	admin, err := pgxpool.New(ctx, databaseURL)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(admin.Close)
	schema := "tab_test_" + strings.ReplaceAll(uuid.NewString(), "-", "")
	if _, err := admin.Exec(ctx, `CREATE SCHEMA `+schema); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _, _ = admin.Exec(context.Background(), `DROP SCHEMA `+schema+` CASCADE`) })
	body, err := os.ReadFile("../../migrations/001_init.sql")
	if err != nil {
		t.Fatal(err)
	}
	config, err := pgxpool.ParseConfig(databaseURL)
	if err != nil {
		t.Fatal(err)
	}
	config.ConnConfig.RuntimeParams["search_path"] = schema + ",public"
	pool, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	up := strings.SplitN(string(body), "-- +goose Down", 2)[0]
	if _, err := pool.Exec(ctx, up); err != nil {
		t.Fatal(err)
	}
	return &Store{Pool: pool}
}

func TestExpenseOutboxIdempotencyAndProjection(t *testing.T) {
	store := integrationStore(t)
	ctx := context.Background()
	alice, err := store.CreateUser(ctx, "Alice", "alice@example.invalid", uuid.NewString())
	if err != nil {
		t.Fatal(err)
	}
	bob, err := store.CreateUser(ctx, "Bob", "bob@example.invalid", uuid.NewString())
	if err != nil {
		t.Fatal(err)
	}
	group, err := store.CreateGroup(ctx, alice.ID, "Trip", "INR")
	if err != nil {
		t.Fatal(err)
	}
	if err := store.AddMember(ctx, group.ID, alice.ID, bob.ID); err != nil {
		t.Fatal(err)
	}
	members, err := store.ListGroupMembers(ctx, group.ID, alice.ID)
	if err != nil {
		t.Fatal(err)
	}
	if len(members) != 2 || members[0].Name != "Alice" || members[1].Name != "Bob" {
		t.Fatalf("members=%#v, want Alice and Bob", members)
	}
	params := CreateExpenseParams{
		GroupID: group.ID, PayerID: alice.ID, ActorID: alice.ID,
		Description: "Dinner", AmountMinor: 101, SplitStrategy: "EQUAL",
		ParticipantIDs: []string{bob.ID, alice.ID}, IdempotencyKey: "request-1",
		Endpoint: "test/create-expense", CorrelationID: uuid.NewString(),
	}
	first, replayed, err := store.CreateExpense(ctx, params)
	if err != nil || replayed {
		t.Fatalf("first create: replayed=%v err=%v", replayed, err)
	}
	second, replayed, err := store.CreateExpense(ctx, params)
	if err != nil || !replayed || second.ID != first.ID {
		t.Fatalf("retry: second=%#v replayed=%v err=%v", second, replayed, err)
	}
	items, err := store.ClaimOutbox(ctx, 10, 0)
	if err != nil || len(items) != 1 {
		t.Fatalf("outbox items=%d err=%v", len(items), err)
	}
	applied, err := store.ApplyBalanceEvent(ctx, "test-balance", items[0].Envelope)
	if err != nil || !applied {
		t.Fatalf("apply: applied=%v err=%v", applied, err)
	}
	applied, err = store.ApplyBalanceEvent(ctx, "test-balance", items[0].Envelope)
	if err != nil || applied {
		t.Fatalf("duplicate: applied=%v err=%v", applied, err)
	}
	balances, err := store.LedgerBalances(ctx, group.ID, alice.ID)
	if err != nil {
		t.Fatal(err)
	}
	var sum int64
	for _, balance := range balances {
		sum += balance.AmountMinor
	}
	if sum != 0 {
		t.Fatalf("ledger sum=%d", sum)
	}
}

func TestSelfFundedExpenseAppliesAsNoOp(t *testing.T) {
	store := integrationStore(t)
	ctx := context.Background()
	alice, err := store.CreateUser(ctx, "Alice", "alice-solo@example.invalid", uuid.NewString())
	if err != nil {
		t.Fatal(err)
	}
	group, err := store.CreateGroup(ctx, alice.ID, "Solo", "INR")
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err = store.CreateExpense(ctx, CreateExpenseParams{
		GroupID: group.ID, PayerID: alice.ID, ActorID: alice.ID,
		Description: "Coffee", AmountMinor: 10000, SplitStrategy: "EQUAL",
		ParticipantIDs: []string{alice.ID}, CorrelationID: uuid.NewString(),
	}); err != nil {
		t.Fatal(err)
	}
	items, err := store.ClaimOutbox(ctx, 10, 0)
	if err != nil || len(items) != 1 {
		t.Fatalf("outbox items=%d err=%v", len(items), err)
	}
	// The sole participant nets to zero, so the event carries no ledger entries.
	// It must apply cleanly rather than being dead-lettered as a bad payload.
	applied, err := store.ApplyBalanceEvent(ctx, "test-balance", items[0].Envelope)
	if err != nil || !applied {
		t.Fatalf("apply: applied=%v err=%v", applied, err)
	}
	applied, err = store.ApplyBalanceEvent(ctx, "test-balance", items[0].Envelope)
	if err != nil || applied {
		t.Fatalf("duplicate: applied=%v err=%v", applied, err)
	}
	balances, err := store.ProjectionBalances(ctx, group.ID, alice.ID)
	if err != nil {
		t.Fatal(err)
	}
	if len(balances) != 1 || balances[0].AmountMinor != 0 {
		t.Fatalf("balances=%#v, want one zero balance", balances)
	}
}

func TestInvalidExpenseRollsBack(t *testing.T) {
	store := integrationStore(t)
	ctx := context.Background()
	alice, err := store.CreateUser(ctx, "Alice", "alice2@example.invalid", uuid.NewString())
	if err != nil {
		t.Fatal(err)
	}
	group, err := store.CreateGroup(ctx, alice.ID, "Trip", "INR")
	if err != nil {
		t.Fatal(err)
	}
	_, _, err = store.CreateExpense(ctx, CreateExpenseParams{
		GroupID: group.ID, PayerID: alice.ID, ActorID: alice.ID,
		Description: "Invalid", AmountMinor: 100, SplitStrategy: "EXACT",
		ExactSplits:   []domain.Split{{UserID: alice.ID, AmountMinor: 90}},
		CorrelationID: uuid.NewString(),
	})
	if err == nil {
		t.Fatal("expected validation error")
	}
	var count int
	if err := store.Pool.QueryRow(ctx, `SELECT count(*) FROM expenses`).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 0 {
		t.Fatalf("expense count=%d, want 0", count)
	}
}
