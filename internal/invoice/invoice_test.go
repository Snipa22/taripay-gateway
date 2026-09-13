package invoice

import (
	"context"
	"errors"
	"os"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/Snipa22/taripay-gateway/internal/db"
)

// setupStore connects to TARIPAY_TEST_POSTGRES_DSN, migrates a clean schema, and
// returns a Store wired to a fake resolveAddress (never touches a real wallet gRPC
// connection or global walletGRPC package state, per the brief's explicit
// requirement). Skips the calling test if no live Postgres DSN is configured.
func setupStore(t *testing.T, resolveAddress ResolveAddressFunc) *Store {
	t.Helper()
	dsn := os.Getenv("TARIPAY_TEST_POSTGRES_DSN")
	if dsn == "" {
		t.Skip("SKIP: TARIPAY_TEST_POSTGRES_DSN not set, no live Postgres to test against")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatalf("pgxpool.New: %v", err)
	}
	t.Cleanup(pool.Close)

	database := &db.DB{Pool: pool}
	if _, err := pool.Exec(ctx, `DROP TABLE IF EXISTS schema_migrations, webhook_deliveries, invoices CASCADE`); err != nil {
		t.Fatalf("cleanup before test: %v", err)
	}
	if err := database.Migrate(ctx); err != nil {
		t.Fatalf("Migrate() error = %v", err)
	}

	if resolveAddress == nil {
		resolveAddress = func(paymentID string) (string, error) {
			return "fake-address-for-" + paymentID, nil
		}
	}
	return NewStore(pool, resolveAddress)
}

func TestCreate_InsertsRowWithResolvedAddress(t *testing.T) {
	s := setupStore(t, nil)
	ctx := context.Background()

	inv, err := s.Create(ctx, "order-123", 5_000_000, 30*time.Minute)
	if err != nil {
		t.Fatalf("Create() error = %v", err)
	}

	if inv.OrderRef != "order-123" {
		t.Errorf("OrderRef = %q, want %q", inv.OrderRef, "order-123")
	}
	if inv.AmountUTari != 5_000_000 {
		t.Errorf("AmountUTari = %d, want 5000000", inv.AmountUTari)
	}
	if inv.PaymentID != inv.ID.String() {
		t.Errorf("PaymentID = %q, want it to equal ID.String() (%q)", inv.PaymentID, inv.ID.String())
	}
	if inv.Address != "fake-address-for-"+inv.PaymentID {
		t.Errorf("Address = %q, want the resolved fake address", inv.Address)
	}
	if inv.Status != StatusPending {
		t.Errorf("Status = %q, want %q", inv.Status, StatusPending)
	}
	if !inv.ExpiresAt.After(inv.CreatedAt) {
		t.Errorf("ExpiresAt (%v) should be after CreatedAt (%v)", inv.ExpiresAt, inv.CreatedAt)
	}

	// Round-trip through the DB confirms the row was actually inserted.
	got, err := s.GetByID(ctx, inv.ID)
	if err != nil {
		t.Fatalf("GetByID() error = %v", err)
	}
	if got.PaymentID != inv.PaymentID {
		t.Errorf("GetByID().PaymentID = %q, want %q", got.PaymentID, inv.PaymentID)
	}
}

func TestCreate_WalletResolutionFailureInsertsNoRow(t *testing.T) {
	wantErr := errors.New("wallet: connection refused")
	s := setupStore(t, func(paymentID string) (string, error) {
		return "", wantErr
	})
	ctx := context.Background()

	_, err := s.Create(ctx, "order-456", 1000, time.Minute)
	if err == nil {
		t.Fatal("Create() error = nil, want an error when address resolution fails")
	}
	if !errors.Is(err, wantErr) {
		t.Errorf("Create() error = %v, want it to wrap %v", err, wantErr)
	}

	// No orphan row should exist for order-456.
	var count int
	if err := s.db.QueryRow(ctx, `SELECT count(*) FROM invoices WHERE order_ref = $1`, "order-456").Scan(&count); err != nil {
		t.Fatalf("count query: %v", err)
	}
	if count != 0 {
		t.Errorf("invoices with order_ref=order-456 = %d, want 0 (no orphan row on wallet failure)", count)
	}
}

func TestGetByID_NotFound(t *testing.T) {
	s := setupStore(t, nil)
	ctx := context.Background()

	_, err := s.GetByID(ctx, uuid.New())
	if !errors.Is(err, ErrNotFound) {
		t.Errorf("GetByID() error = %v, want ErrNotFound", err)
	}
}

func TestGetByPaymentID_RoundTrip(t *testing.T) {
	s := setupStore(t, nil)
	ctx := context.Background()

	inv, err := s.Create(ctx, "order-789", 42, time.Hour)
	if err != nil {
		t.Fatalf("Create() error = %v", err)
	}

	got, err := s.GetByPaymentID(ctx, inv.PaymentID)
	if err != nil {
		t.Fatalf("GetByPaymentID() error = %v", err)
	}
	if got.ID != inv.ID {
		t.Errorf("GetByPaymentID().ID = %v, want %v", got.ID, inv.ID)
	}

	_, err = s.GetByPaymentID(ctx, "does-not-exist")
	if !errors.Is(err, ErrNotFound) {
		t.Errorf("GetByPaymentID(unknown) error = %v, want ErrNotFound", err)
	}
}

func TestUpdateStatus(t *testing.T) {
	s := setupStore(t, nil)
	ctx := context.Background()

	inv, err := s.Create(ctx, "order-status", 100, time.Hour)
	if err != nil {
		t.Fatalf("Create() error = %v", err)
	}

	confirmedAt := time.Now().UTC().Truncate(time.Second)
	if err := s.UpdateStatus(ctx, inv.ID, StatusConfirmed, &confirmedAt); err != nil {
		t.Fatalf("UpdateStatus() error = %v", err)
	}

	got, err := s.GetByID(ctx, inv.ID)
	if err != nil {
		t.Fatalf("GetByID() error = %v", err)
	}
	if got.Status != StatusConfirmed {
		t.Errorf("Status = %q, want %q", got.Status, StatusConfirmed)
	}
	if got.ConfirmedAt == nil || !got.ConfirmedAt.Equal(confirmedAt) {
		t.Errorf("ConfirmedAt = %v, want %v", got.ConfirmedAt, confirmedAt)
	}

	if err := s.UpdateStatus(ctx, uuid.New(), StatusCancelled, nil); !errors.Is(err, ErrNotFound) {
		t.Errorf("UpdateStatus(unknown id) error = %v, want ErrNotFound", err)
	}
}

// TestAddReceivedAmount covers the C2-fix addition (task brief "fix C2 and add
// auth", part 1): AddReceivedAmount must accumulate correctly across multiple calls
// (simulating multiple transaction events for the same invoice) and return the new
// running total each time, against real Postgres — verifying the atomic
// UPDATE ... RETURNING form actually persists and reflects accumulation, not just a
// mocked/in-memory counter.
func TestAddReceivedAmount(t *testing.T) {
	s := setupStore(t, nil)
	ctx := context.Background()

	inv, err := s.Create(ctx, "order-received", 5000, time.Hour)
	if err != nil {
		t.Fatalf("Create() error = %v", err)
	}
	if inv.AmountReceivedUTari != 0 {
		t.Errorf("AmountReceivedUTari on creation = %d, want 0", inv.AmountReceivedUTari)
	}

	total, err := s.AddReceivedAmount(ctx, inv.ID, 3000)
	if err != nil {
		t.Fatalf("AddReceivedAmount(3000) error = %v", err)
	}
	if total != 3000 {
		t.Errorf("AddReceivedAmount(3000) returned total = %d, want 3000", total)
	}

	total, err = s.AddReceivedAmount(ctx, inv.ID, 2500)
	if err != nil {
		t.Fatalf("AddReceivedAmount(2500) error = %v", err)
	}
	if total != 5500 {
		t.Errorf("AddReceivedAmount(2500) returned total = %d, want 5500 (cumulative)", total)
	}

	// Round-trip through the DB confirms the accumulated total was actually
	// persisted, not just returned in-memory.
	got, err := s.GetByID(ctx, inv.ID)
	if err != nil {
		t.Fatalf("GetByID() error = %v", err)
	}
	if got.AmountReceivedUTari != 5500 {
		t.Errorf("GetByID().AmountReceivedUTari = %d, want 5500", got.AmountReceivedUTari)
	}

	if _, err := s.AddReceivedAmount(ctx, uuid.New(), 100); !errors.Is(err, ErrNotFound) {
		t.Errorf("AddReceivedAmount(unknown id) error = %v, want ErrNotFound", err)
	}
}

func TestExpireStale(t *testing.T) {
	s := setupStore(t, nil)
	ctx := context.Background()

	// A pending invoice already past its expires_at.
	stale, err := s.Create(ctx, "order-stale", 10, time.Hour)
	if err != nil {
		t.Fatalf("Create(stale) error = %v", err)
	}
	if _, err := s.db.Exec(ctx, `UPDATE invoices SET expires_at = now() - interval '1 minute' WHERE id = $1`, stale.ID); err != nil {
		t.Fatalf("backdate expires_at: %v", err)
	}

	// A seen invoice, also stale.
	seenStale, err := s.Create(ctx, "order-seen-stale", 10, time.Hour)
	if err != nil {
		t.Fatalf("Create(seenStale) error = %v", err)
	}
	if err := s.UpdateStatus(ctx, seenStale.ID, StatusSeen, nil); err != nil {
		t.Fatalf("UpdateStatus(seenStale): %v", err)
	}
	if _, err := s.db.Exec(ctx, `UPDATE invoices SET expires_at = now() - interval '1 minute' WHERE id = $1`, seenStale.ID); err != nil {
		t.Fatalf("backdate expires_at: %v", err)
	}

	// A fresh pending invoice, not stale — must not be touched.
	fresh, err := s.Create(ctx, "order-fresh", 10, time.Hour)
	if err != nil {
		t.Fatalf("Create(fresh) error = %v", err)
	}

	// A confirmed invoice past expires_at — must not be touched (only
	// pending/seen are eligible).
	confirmedPast, err := s.Create(ctx, "order-confirmed-past", 10, time.Hour)
	if err != nil {
		t.Fatalf("Create(confirmedPast) error = %v", err)
	}
	now := time.Now().UTC()
	if err := s.UpdateStatus(ctx, confirmedPast.ID, StatusConfirmed, &now); err != nil {
		t.Fatalf("UpdateStatus(confirmedPast): %v", err)
	}
	if _, err := s.db.Exec(ctx, `UPDATE invoices SET expires_at = now() - interval '1 minute' WHERE id = $1`, confirmedPast.ID); err != nil {
		t.Fatalf("backdate expires_at: %v", err)
	}

	count, err := s.ExpireStale(ctx)
	if err != nil {
		t.Fatalf("ExpireStale() error = %v", err)
	}
	if count != 2 {
		t.Errorf("ExpireStale() count = %d, want 2", count)
	}

	for _, tc := range []struct {
		name       string
		id         uuid.UUID
		wantStatus string
	}{
		{"stale pending -> expired", stale.ID, StatusExpired},
		{"stale seen -> expired", seenStale.ID, StatusExpired},
		{"fresh pending untouched", fresh.ID, StatusPending},
		{"confirmed past untouched", confirmedPast.ID, StatusConfirmed},
	} {
		got, err := s.GetByID(ctx, tc.id)
		if err != nil {
			t.Fatalf("%s: GetByID() error = %v", tc.name, err)
		}
		if got.Status != tc.wantStatus {
			t.Errorf("%s: Status = %q, want %q", tc.name, got.Status, tc.wantStatus)
		}
	}
}
